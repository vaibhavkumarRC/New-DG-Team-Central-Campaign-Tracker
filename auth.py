"""Google sign-in gate — only @rapidclaims.ai accounts may use the dashboard.

Wiring: auth.init_app(app, admin_token=ADMIN_TOKEN) from app.py. The gate
is OFF until the GOOGLE_OAUTH_CLIENT_ID env var is set (a Google OAuth
"Web application" client, User type Internal, whose Authorized JavaScript
origins include the Railway URL plus http://localhost:5001, :5002 and
:5056), so local dev and existing deploys keep working until the
credential is configured.

Flow: unauthenticated page requests are answered with the login page —
served with HTTP 200, never a redirect or 401, a DELIBERATE choice:
railway.toml health-checks "/" with no session and only switches traffic
to a new deployment on a 2xx. The page runs Google Identity Services; the
resulting ID token is POSTed to /auth/session and verified server-side
with google-auth's verify_oauth2_token (signature against Google's certs,
expiry, issuer, aud == our client ID) plus an explicit hd/email-domain
check for rapidclaims.ai — the console's "Internal" user type also
restricts sign-in, but that is a console setting that can drift, so the
code enforces the domain itself. On success a 30-day Flask session cookie
is issued. Unauthenticated /api/* calls get 401 JSON.

Headless callers: requests carrying a valid X-Admin-Token header bypass
the login gate — 18 admin routes (/api/sync, campaign CRUD,
/api/zoom-access-check, …) are driven by curl/scripts with no browser
session, and each still enforces its own @require_admin.

Env vars: GOOGLE_OAUTH_CLIENT_ID (required to enable), FLASK_SECRET_KEY
(optional — random per boot otherwise, which just re-prompts sign-in
after a restart; fine with the single gunicorn worker in start.sh),
SESSION_COOKIE_SECURE (optional override — defaults to on when the
Railway /data volume is present, off for local http:// dev).
"""
import os
import secrets
from datetime import timedelta

from flask import jsonify, request, session, render_template_string, redirect

GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_OAUTH_CLIENT_ID', '')
ALLOWED_DOMAIN = os.environ.get('LOGIN_ALLOWED_DOMAIN', 'rapidclaims.ai')

_LOGIN_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sign in — Campaign Command Center</title>
<script src="https://accounts.google.com/gsi/client" async defer></script>
<style>
  body { margin:0; min-height:100vh; display:flex; align-items:center;
         justify-content:center; background:#0f1115; color:#e6e8ee;
         font:14px/1.5 -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
  .card { background:#171a21; border:1px solid #2a2f3a; border-radius:12px;
          padding:40px 44px; text-align:center; max-width:360px; }
  h1 { font-size:18px; margin:0 0 6px; }
  p  { color:#9aa3b2; margin:0 0 24px; }
  .g { display:flex; justify-content:center; }
  .err { color:#e5484d; margin:16px 0 0; min-height:1em; }
</style></head>
<body>
<div class="card">
  <h1>Campaign Command Center</h1>
  <p>Sign in with your @{{ domain }} Google account.</p>
  <div id="g_id_onload" data-client_id="{{ client_id }}"
       data-callback="onCredential" data-auto_select="true"></div>
  <div class="g"><div class="g_id_signin" data-type="standard"
       data-theme="filled_black" data-size="large" data-text="signin_with"></div></div>
  <p class="err" id="err"></p>
</div>
<script>
async function onCredential(resp) {
  const el = document.getElementById('err');
  try {
    const r = await fetch('/auth/session', { method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ credential: resp.credential }) });
    const j = await r.json();
    if (j.ok) location.reload();
    else el.textContent = j.error || 'sign-in failed';
  } catch (e) { el.textContent = 'sign-in failed: ' + (e.message || e); }
}
</script>
</body></html>"""


def _verify_google_token(credential):
    """ID token → verified @ALLOWED_DOMAIN email, or ValueError.
    verify_oauth2_token checks the signature against Google's published
    certs, the expiry, the issuer, and aud == our client ID; the explicit
    domain check below is ours."""
    if not credential:
        raise ValueError('missing credential')
    from google.oauth2 import id_token as g_id_token
    from google.auth.transport.requests import Request as GAuthRequest
    try:
        info = g_id_token.verify_oauth2_token(
            credential, GAuthRequest(), GOOGLE_CLIENT_ID,
            clock_skew_in_seconds=10)
    except Exception as e:
        raise ValueError(f'token verification failed: {e}')
    if info.get('email_verified') not in (True, 'true'):
        raise ValueError('email not verified')
    email = (info.get('email') or '').strip().lower()
    if (info.get('hd') != ALLOWED_DOMAIN
            or not email.endswith('@' + ALLOWED_DOMAIN)):
        raise ValueError(f'only @{ALLOWED_DOMAIN} accounts are allowed')
    return email


def init_app(app, admin_token=''):
    if not GOOGLE_CLIENT_ID:
        print('[auth] GOOGLE_OAUTH_CLIENT_ID not set — login gate DISABLED')
        return
    # Fail at boot, not at first sign-in, if a verification dependency is
    # missing — railway.toml's healthcheck then keeps the previous deployment
    # serving. BOTH imports matter: id_token alone booted clean while
    # google-auth's requests transport was absent, so the gate engaged and
    # every sign-in then died with a 500. Healthy healthcheck, locked-out team.
    from google.oauth2 import id_token as _boot_check          # noqa: F401
    from google.auth.transport.requests import Request as _bc2  # noqa: F401
    app.secret_key = os.environ.get('FLASK_SECRET_KEY') or secrets.token_hex(32)
    # Secure cookies in production only: '/data' is the Railway volume and is
    # the same production signal app.py uses for DATA_DIR. Forcing Secure on
    # local http:// would silently drop the session cookie and loop the login.
    _secure = os.environ.get('SESSION_COOKIE_SECURE')
    _secure = (_secure.lower() in ('1', 'true', 'yes')) if _secure \
        else os.path.isdir('/data')
    app.config.update(SESSION_COOKIE_HTTPONLY=True,
                      SESSION_COOKIE_SAMESITE='Lax',
                      SESSION_COOKIE_SECURE=_secure)
    app.permanent_session_lifetime = timedelta(days=30)
    print(f'[auth] login gate ENABLED — @{ALLOWED_DOMAIN} accounts only')

    @app.route('/auth/session', methods=['POST'])
    def auth_session():
        try:
            email = _verify_google_token(
                (request.get_json(silent=True) or {}).get('credential'))
        except ValueError as e:
            print(f'[auth] rejected sign-in: {e}', flush=True)
            return jsonify({'ok': False, 'error': str(e)}), 403
        except Exception as e:
            # Anything not a ValueError (missing dependency, Google's cert
            # endpoint unreachable, …) would otherwise escape as Flask's HTML
            # 500 page, which the login card tries to parse as JSON and shows
            # as "Unexpected token '<'". Keep the response JSON so the user
            # sees something actionable and the cause reaches the logs.
            print(f'[auth] sign-in ERROR ({type(e).__name__}): {e}', flush=True)
            return jsonify({'ok': False,
                            'error': 'sign-in is temporarily unavailable — '
                                     'please tell the dashboard admin'}), 500
        session.permanent = True
        session['user'] = email
        print(f'[auth] signed in: {email}', flush=True)
        return jsonify({'ok': True, 'user': email})

    @app.route('/auth/whoami')
    def auth_whoami():
        """Who the browser is signed in as — powers the header's Sign out
        link. /auth/* is exempt from the gate, so this is reachable without a
        session and simply answers {"user": null} in that case. It exposes
        nothing an already-signed-in browser doesn't already know."""
        return jsonify({'user': session.get('user')})

    @app.route('/auth/logout')
    def auth_logout():
        session.clear()
        return redirect('/')

    @app.before_request
    def _login_gate():
        if session.get('user'):
            return None
        # Headless admin callers (curl, scripts hitting /api/sync, campaign
        # CRUD, …) authenticate per-request with X-Admin-Token and have no
        # browser session. Let them through; @require_admin on each route
        # still does its own check.
        if admin_token and (request.headers.get('X-Admin-Token')
                            or '').strip() == admin_token:
            return None
        p = request.path
        if (p.startswith('/auth/') or p.startswith('/static/')
                or p == '/favicon.ico'):
            return None
        if p.startswith('/api/'):
            return jsonify({'error': 'login required'}), 401
        return render_template_string(_LOGIN_HTML,
                                      client_id=GOOGLE_CLIENT_ID,
                                      domain=ALLOWED_DOMAIN)
