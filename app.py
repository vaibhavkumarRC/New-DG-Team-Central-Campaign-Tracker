from flask import Flask, jsonify, request, render_template
import subprocess, json, os, threading, time  # v2
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import urllib.request as _urllib_req
import urllib.parse

app = Flask(__name__)

BASE       = os.path.dirname(os.path.abspath(__file__))
# On Railway a persistent volume is mounted at /data — use it if available
DATA_DIR   = '/data' if os.path.isdir('/data') else BASE
CAMPS_FILE    = os.path.join(DATA_DIR, 'campaigns.json')
CACHE_FILE    = os.path.join(DATA_DIR, 'data_cache.json')
SEGMENTS_FILE = os.path.join(DATA_DIR, 'segments.json')
LEDGER_FILE   = os.path.join(DATA_DIR, 'meeting_ledger.json')

# ── Campaign ledger ────────────────────────────────────────────────────────────
# Stores two frozen datasets per campaign so metrics remain correct even after
# leads are reassigned to a different campaign:
#
#   ledger[campaign_id] = {
#     "lead_ids":  ["id1", "id2", ...],          # every Lead ever in this campaign
#     "meetings":  {                               # leads that generated a meeting
#       "lead_id": {
#         "date":    "2026-04-15",
#         "sdr":     "Hursh",
#         "name":    "John Doe",
#         "title":   "CFO",
#         "company": "Banner Health",
#         "sf_url":  "https://...lightning/r/Lead/.../view"
#       }
#     }
#   }
#
# Rules:
#  • lead_ids  — union-only; IDs are added on each sync, never removed.
#  • meetings  — union-only; once attributed to a campaign, never moved.
#  • Calls/emails use lead_ids for the SOQL WhoId filter, so task counts
#    stay accurate regardless of the lead's current Campaign__c value.

_ledger_lock = threading.Lock()

def load_ledger():
    if os.path.exists(LEDGER_FILE):
        try:
            with open(LEDGER_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_ledger(ledger):
    with open(LEDGER_FILE, 'w') as f:
        json.dump(ledger, f)

def _get_or_create_entry(ledger, campaign_id):
    if campaign_id not in ledger:
        ledger[campaign_id] = {'lead_ids': [], 'meetings': {}}
    entry = ledger[campaign_id]
    # Migrate old format (flat dict of lead_id→meeting) to new format
    if 'meetings' not in entry:
        entry = {'lead_ids': [], 'meetings': entry}
        ledger[campaign_id] = entry
    if 'lead_ids' not in entry:
        entry['lead_ids'] = []
    return entry

def merge_leads_into_ledger(campaign_id, lead_ids):
    """Permanently store all Lead IDs that have ever been in this campaign.
    Returns the full frozen set of Lead IDs for this campaign."""
    with _ledger_lock:
        ledger  = load_ledger()
        entry   = _get_or_create_entry(ledger, campaign_id)
        existing = set(entry['lead_ids'])
        new_ids  = [lid for lid in lead_ids if lid not in existing]
        if new_ids:
            entry['lead_ids'] = list(existing) + new_ids
            save_ledger(ledger)
        return entry['lead_ids']

def merge_meetings_into_ledger(campaign_id, sf_records):
    """Add newly discovered meeting leads into the ledger. Never removes entries.
    sf_records: list of SFDC Lead records with Id, Meeting_Generated_on__c,
    Meeting_Generated_by__c, Name, Title, Company fields.
    Returns the meetings dict {lead_id → {date,sdr,name,title,company,sf_url}}.
    """
    with _ledger_lock:
        ledger   = load_ledger()
        entry    = _get_or_create_entry(ledger, campaign_id)
        meetings = entry['meetings']
        changed  = False
        for rec in sf_records:
            lid = rec.get('Id')
            if not lid or lid in meetings:
                continue  # already attributed — never overwrite
            meetings[lid] = {
                'date':    (rec.get('Meeting_Generated_on__c') or '')[:10],
                'sdr':     norm_sdr(rec.get('Meeting_Generated_by__c') or ''),
                'name':    rec.get('Name')    or '—',
                'title':   rec.get('Title')   or '—',
                'company': rec.get('Company') or '—',
                'sf_url':  f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view",
            }
            changed = True
        if changed:
            save_ledger(ledger)
        return meetings

def meetings_from_ledger(meetings, start=None, end=None):
    """Return (total_count, sdr_breakdown_list) from a meetings dict,
    optionally filtered by date range (YYYY-MM-DD strings)."""
    rows = list(meetings.values())
    if start:
        rows = [r for r in rows if r.get('date','') >= start]
    if end:
        rows = [r for r in rows if r.get('date','') <= end]
    total = len(rows)
    sdr_counts = {}
    for r in rows:
        sdr = r.get('sdr') or 'Unknown'
        sdr_counts[sdr] = sdr_counts.get(sdr, 0) + 1
    breakdown = sorted(
        [{'name': k, 'meetings': v} for k, v in sdr_counts.items()],
        key=lambda x: x['meetings'], reverse=True
    )
    return total, breakdown

def meetings_leads_from_ledger(meetings, start=None, end=None):
    """Return list of lead dicts for the meetings modal, optionally date-filtered."""
    rows = [(lid, data) for lid, data in meetings.items()]
    if start:
        rows = [(lid, d) for lid, d in rows if d.get('date','') >= start]
    if end:
        rows = [(lid, d) for lid, d in rows if d.get('date','') <= end]
    rows.sort(key=lambda x: x[1].get('date',''), reverse=True)
    return [
        {
            'name':    d.get('name','—'),
            'title':   d.get('title','—'),
            'company': d.get('company','—'),
            'sdr':     d.get('sdr','—'),
            'date':    d.get('date',''),
            'sf_url':  d.get('sf_url',''),
            'campaign': '',  # will be filled by caller
        }
        for _, d in rows
    ]

# SOQL IN-clause helper — batches large ID lists to stay within query limits
_BATCH_SIZE = 500

def _count_tasks_for_ids(lead_ids, subject_filter, dt_task):
    """Count Tasks matching subject_filter for a list of lead IDs.
    Batches into groups of _BATCH_SIZE to avoid SOQL length limits.
    Uses paginate=False — COUNT queries return 1 row, no pagination needed,
    and avoiding extra API calls prevents Salesforce rate limits."""
    if not lead_ids:
        return 0
    total = 0
    for i in range(0, len(lead_ids), _BATCH_SIZE):
        batch = lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        q = (f"SELECT COUNT(Id) FROM Task "
             f"WHERE ({subject_filter}) AND WhoId IN ({ids_str}){dt_task}")
        total += cnt(soql(q, paginate=False))
    return total

def _count_distinct_who_for_ids(lead_ids, subject_filter, dt_task):
    """Count distinct WhoIds (unique leads contacted) for a list of lead IDs.
    Uses paginate=False — each batch has at most _BATCH_SIZE (500) unique leads
    so the first page of results is enough to find all unique WhoIds per batch."""
    if not lead_ids:
        return 0
    seen = set()
    for i in range(0, len(lead_ids), _BATCH_SIZE):
        batch = lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        q = (f"SELECT WhoId FROM Task "
             f"WHERE ({subject_filter}) AND WhoId IN ({ids_str}){dt_task} "
             f"LIMIT 50000")
        res = soql(q, paginate=False)
        if res and res.get('records'):
            for rec in res['records']:
                wid = rec.get('WhoId')
                if wid:
                    seen.add(wid)
    return len(seen)

def _count_leads_for_ids(lead_ids, extra_filter):
    """Count Leads matching extra_filter from a frozen list of lead IDs.
    Batches into groups of _BATCH_SIZE to avoid SOQL length limits."""
    if not lead_ids:
        return 0
    total = 0
    for i in range(0, len(lead_ids), _BATCH_SIZE):
        batch = lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        q = f"SELECT COUNT(Id) FROM Lead WHERE Id IN ({ids_str}) AND {extra_filter}"
        total += cnt(soql(q))
    return total

def _agg_status_sdr_for_ids(lead_ids, status_filter):
    """Group Leads by Meeting_Generated_by__c + Meeting_Status__c from frozen IDs.
    Returns {'records': [{Meeting_Generated_by__c, Meeting_Status__c, expr0}]}."""
    if not lead_ids:
        return {'records': []}
    from collections import defaultdict
    agg = defaultdict(int)
    for i in range(0, len(lead_ids), _BATCH_SIZE):
        batch = lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        q = (f"SELECT Meeting_Generated_by__c, Meeting_Status__c, COUNT(Id) "
             f"FROM Lead WHERE Id IN ({ids_str}) AND ({status_filter}) "
             f"AND Meeting_Generated_by__c != null "
             f"GROUP BY Meeting_Generated_by__c, Meeting_Status__c")
        res = soql(q)
        if res and res.get('records'):
            for rec in res['records']:
                key = (rec.get('Meeting_Generated_by__c'), rec.get('Meeting_Status__c'))
                agg[key] += int(rec.get('expr0', 0) or 0)
    return {'records': [{'Meeting_Generated_by__c': k[0], 'Meeting_Status__c': k[1], 'expr0': v}
                        for k, v in agg.items()]}

def _agg_sdr_count_for_ids(lead_ids, extra_filter):
    """Group Leads by Meeting_Generated_by__c + COUNT from frozen IDs.
    Returns {'records': [{Meeting_Generated_by__c, expr0}]}."""
    if not lead_ids:
        return {'records': []}
    from collections import defaultdict
    agg = defaultdict(int)
    for i in range(0, len(lead_ids), _BATCH_SIZE):
        batch = lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        q = (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
             f"WHERE Id IN ({ids_str}) AND {extra_filter} "
             f"AND Meeting_Generated_by__c != null "
             f"GROUP BY Meeting_Generated_by__c")
        res = soql(q)
        if res and res.get('records'):
            for rec in res['records']:
                key = rec.get('Meeting_Generated_by__c')
                agg[key] += int(rec.get('expr0', 0) or 0)
    return {'records': [{'Meeting_Generated_by__c': k, 'expr0': v} for k, v in agg.items()]}

DEFAULT_SEGMENTS = ['EPIC Campaign', 'TruBridge Campaign', 'Factors Data', 'Hiring Data', 'High Intent Data']

def load_segments():
    if os.path.exists(SEGMENTS_FILE):
        try:
            with open(SEGMENTS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_segments(segs):
    with open(SEGMENTS_FILE, 'w') as f:
        json.dump(segs, f)
SF_ORG      = os.environ.get('SF_ORG',      'vaibhavkumar@rapidclaims.ai')
SF_BASE_URL = os.environ.get('SF_BASE_URL', 'https://data-page-6243.my.salesforce.com')
NPV_START_DATE = '2026-04-01T00:00:00Z'   # Opportunities created after 31 March 2026

# ── Admin access ──────────────────────────────────────────────────────────────
# Set ADMIN_TOKEN env-var to override the default before sharing the URL.
# Admin URL  →  http://localhost:5001/?admin=<token>
# Viewer URL →  http://localhost:5001/
ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN', 'rc-admin-2026')

def require_admin(f):
    """Decorator: rejects write requests that don't carry the correct admin token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = (request.headers.get('X-Admin-Token') or '').strip()
        if token != ADMIN_TOKEN:
            return jsonify({'error': 'Unauthorized — admin token required'}), 403
        return f(*args, **kwargs)
    return decorated

cache = {
    'campaigns': [], 'sdr_stats': [],
    'totals': {'meeting_done': 0, 'meeting_noshow': 0, 'sql_gen': 0, 's1': 0},
    'last_sync': None, 'is_syncing': False,
    'sync_progress': 0, 'errors': []
}

# ── Period data cache (7d / 30d / qtd) ───────────────────────────────────────
PERIOD_CACHE_TTL = 3600  # 1 hour — period data now comes from main cache, not Salesforce
period_cache = {}        # key: period → {'data': {...}, 'fetched_at': datetime}

# ── Salesforce helpers ────────────────────────────────────────────────────────

# ── Salesforce REST API token cache ──────────────────────────────────────────
_sf_token_cache = {'token': None, 'instance_url': None, 'fetched_at': None}
_sf_token_lock  = threading.Lock()

def _refresh_sf_token():
    """Obtain Salesforce access token + instance URL.

    Strategy order:
    1. sf org display  — works on older CLI (token not yet redacted)
    2. AES decrypt     — SF CLI v2.x encrypts the token in ~/.sfdx/<user>.json
                         using ~/.sfdx/key.json; decrypt with pycryptodome
    3. PTY interaction — use sf org auth show-access-token with a real pseudo-
                         terminal so the CLI will accept the 'y' confirmation
    """
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')
    instance_url = SF_BASE_URL

    def _valid_token(s):
        return (s and len(s) >= 20 and ' ' not in s
                and '[REDACTED]' not in s
                and any(c.isalnum() for c in s[:10]))

    # ── 1. sf org display ────────────────────────────────────────────────────
    try:
        r = subprocess.run(
            ['sf', 'org', 'display', '--target-org', SF_ORG, '--json'],
            capture_output=True, text=True, timeout=30, env=env
        )
        d = json.loads(r.stdout)
        if d.get('status') == 0:
            res = d.get('result', {})
            instance_url = res.get('instanceUrl') or SF_BASE_URL
            tok = res.get('accessToken', '')
            if _valid_token(tok) and '[REDACTED]' not in tok:
                print('[SF-auth] Token from sf org display')
                return tok, instance_url
            print('[SF-auth] sf org display redacted token — trying AES decrypt')
    except Exception as e:
        print(f'[SF-auth] org display exception: {e}')

    # ── 2. AES-256-GCM decrypt — @salesforce/core v1 format ─────────────────
    # CONFIRMED from source: algo=aes-256-gcm, key=buffer.toString('utf8')
    # (= raw ASCII bytes of the hex key string = 32 bytes = AES-256).
    # IV v1: randomBytes(12).toString('hex') used as UTF-8 string (24 ASCII bytes).
    # Format: IV_ASCII(24) + CIPHER_HEX + ':' + TAG_HEX(32)
    try:
        key_file  = os.path.expanduser('~/.sfdx/key.json')
        auth_file = os.path.expanduser(f'~/.sfdx/{SF_ORG}.json')
        if os.path.exists(key_file) and os.path.exists(auth_file):
            with open(key_file)  as f: kd = json.load(f)
            with open(auth_file) as f: ad = json.load(f)
            enc_tok  = ad.get('accessToken', '')
            inst_url = ad.get('instanceUrl') or instance_url
            key_hex  = kd.get('key', '')
            print(f'[SF-auth] AES: key_hex len={len(key_hex)} enc_tok len={len(enc_tok)}')
            if key_hex and enc_tok and len(enc_tok) > 33:
                from Crypto.Cipher import AES
                # v1 format: key = ASCII bytes of hex string, IV = first 24 ASCII chars
                # key.json has 32 hex chars → 32 ASCII bytes = AES-256 key
                key_bytes = key_hex.encode('ascii')   # 32 ASCII bytes = AES-256
                colon_pos = enc_tok.index(':') if ':' in enc_tok else len(enc_tok)
                # Try v1 (IV=24 ASCII chars) and v2 (IV=hex-decoded, 12 or 16 bytes)
                iv_candidates = [
                    (enc_tok[:24].encode('ascii'), enc_tok[24:colon_pos], 'v1-iv24-ascii'),
                    (enc_tok[:32].encode('ascii'), enc_tok[32:colon_pos], 'v1-iv32-ascii'),
                    (bytes.fromhex(enc_tok[:24]),  enc_tok[24:colon_pos], 'v2-iv12-hex'),
                    (bytes.fromhex(enc_tok[:32]),  enc_tok[32:colon_pos], 'v2-iv16-hex'),
                ]
                tag_bytes = bytes.fromhex(enc_tok[colon_pos+1:]) if ':' in enc_tok else b''
                for iv_bytes, cipher_hex, label in iv_candidates:
                    try:
                        cipher = AES.new(key_bytes, AES.MODE_GCM, nonce=iv_bytes)
                        dec = cipher.decrypt_and_verify(
                            bytes.fromhex(cipher_hex), tag_bytes
                        ).decode('utf-8').strip()
                        print(f'[SF-auth] GCM {label} → {repr(dec[:30])}')
                        if _valid_token(dec):
                            print(f'[SF-auth] Token decrypted via AES-256-GCM ({label})')
                            return dec, inst_url
                    except Exception as gcm_err:
                        print(f'[SF-auth] GCM {label} failed: {gcm_err}')
    except ImportError:
        print('[SF-auth] pycryptodome not available for AES decrypt')
    except Exception as e:
        print(f'[SF-auth] AES decrypt error: {e}')

    # ── 3. Node.js AuthInfo.create() — @salesforce/core official decrypt ─────
    # Uses the exact same AuthInfo class that SF CLI uses internally.
    # getFields(true) decrypts the stored accessToken using the key.json secret.
    # This is the most reliable approach — no manual crypto needed.
    try:
        sfcore_path = '/usr/lib/node_modules/@salesforce/cli/node_modules/@salesforce/core'
        node_script = (
            "const p='" + sfcore_path + "';"
            "async function main(){"
            "try{"
            "const {AuthInfo}=require(p);"
            "const ai=await AuthInfo.create({username:'" + SF_ORG + "'});"
            "const f=ai.getFields(true);"
            "const tok=f.accessToken||'';"
            "const inst=f.instanceUrl||'';"
            "if(tok&&tok.length>20&&!tok.includes('[REDACTED]')){"
            "process.stdout.write(JSON.stringify({ok:true,token:tok,instanceUrl:inst}));"
            "}else{"
            "process.stdout.write(JSON.stringify({ok:false,err:'token invalid',tok_preview:String(tok).slice(0,30)}));"
            "}"
            "}catch(e){"
            "process.stdout.write(JSON.stringify({ok:false,err:e.message}));"
            "}"
            "}"
            "main();"
        )
        node_r = subprocess.run(
            ['node', '-e', node_script],
            capture_output=True, text=True, timeout=20, env=env
        )
        out = node_r.stdout.strip()
        if out:
            nd = json.loads(out)
            if nd.get('ok') and nd.get('token'):
                tok = nd['token'].strip()
                inst = nd.get('instanceUrl') or instance_url
                if _valid_token(tok):
                    print(f'[SF-auth] Token via @salesforce/core AuthInfo (len={len(tok)})')
                    return tok, inst
                print(f'[SF-auth] AuthInfo gave invalid token: {repr(tok[:30])}')
            else:
                print(f'[SF-auth] AuthInfo failed: {nd}')
        else:
            print(f'[SF-auth] AuthInfo: empty stdout rc={node_r.returncode} err={node_r.stderr[:200]}')
    except Exception as e:
        print(f'[SF-auth] AuthInfo error: {e}')

    # ── 5. sf org auth show-access-token via subprocess (input='y\n') ──────────
    try:
        import re as _re
        r3 = subprocess.run(
            ['sf', 'org', 'auth', 'show-access-token', '--target-org', SF_ORG],
            capture_output=True, text=True, timeout=30, env=env,
            input='y\n'
        )
        raw = r3.stdout or ''
        # Strip ANSI escape codes
        clean = _re.sub(r'\x1b\[[0-9;?]*[A-Za-z]', '', raw)
        for line in clean.splitlines():
            stripped = line.strip()
            # Token is in a box row: │ Access Token │ <token> │
            # Split on │ and find non-empty segments; if one is "Access Token"
            # the next one is the token value.
            if '│' in stripped:
                parts = [p.strip() for p in stripped.split('│') if p.strip()]
                for i, part in enumerate(parts):
                    if part.lower() in ('access token', 'token') and i + 1 < len(parts):
                        candidate = parts[i + 1]
                        if _valid_token(candidate):
                            print(f'[SF-auth] Token via show-access-token (box row, len={len(candidate)})')
                            return candidate, instance_url
                # Generic fallback: a single valid token in the cell
                if len(parts) == 1 and _valid_token(parts[0]):
                    print(f'[SF-auth] Token via show-access-token (single cell, len={len(parts[0])})')
                    return parts[0], instance_url
        print(f'[SF-auth] show-access-token: no token found. rc={r3.returncode} lines={clean.splitlines()[-5:]}')
    except Exception as e:
        print(f'[SF-auth] show-access-token error: {e}')

    # ── 6. PTY: sf org auth show-access-token with real terminal ─────────────
    try:
        import pty, select as _sel, re as _re2
        master_fd, slave_fd = pty.openpty()
        proc = subprocess.Popen(
            ['sf', 'org', 'auth', 'show-access-token', '--target-org', SF_ORG],
            stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
            env=env, close_fds=True
        )
        os.close(slave_fd)
        buf = b''
        confirmed = False
        deadline  = time.time() + 20
        while time.time() < deadline:
            try:
                ready, _, _ = _sel.select([master_fd], [], [], 0.3)
                if ready:
                    chunk = os.read(master_fd, 4096)
                    buf  += chunk
                    if not confirmed and (b'(y/N)' in chunk or
                                          b'continue?' in chunk.lower()):
                        os.write(master_fd, b'y\r\n')
                        confirmed = True
            except OSError:
                break
            if proc.poll() is not None:
                # drain remaining bytes
                try:
                    while True:
                        r2, _, _ = _sel.select([master_fd], [], [], 0.2)
                        if r2:
                            extra = os.read(master_fd, 4096)
                            if extra: buf += extra
                            else: break
                        else: break
                except OSError:
                    pass
                break
        try: proc.wait(timeout=3)
        except Exception: proc.kill()
        try: os.close(master_fd)
        except Exception: pass
        text = _re2.sub(r'\x1b\[[0-9;?]*[A-Za-z]', '', buf.decode('utf-8', 'ignore'))
        for line in reversed(text.splitlines()):
            stripped = line.strip()
            if '│' in stripped:
                parts = [p.strip() for p in stripped.split('│') if p.strip()]
                for i, part in enumerate(parts):
                    if part.lower() in ('access token', 'token') and i + 1 < len(parts):
                        candidate = parts[i + 1]
                        if _valid_token(candidate):
                            print(f'[SF-auth] Token via PTY (box row, len={len(candidate)})')
                            return candidate, instance_url
                if len(parts) == 1 and _valid_token(parts[0]):
                    print(f'[SF-auth] Token via PTY (single cell, len={len(parts[0])})')
                    return parts[0], instance_url
        print(f'[SF-auth] PTY: no valid token. last lines: {text.splitlines()[-5:]}')
    except Exception as e:
        print(f'[SF-auth] PTY error: {e}')

    print('[SF-auth] All strategies failed — soql() will use CLI fallback')
    return None, instance_url

def _get_sf_token():
    """Return cached (token, instance_url), refreshing every 90 minutes."""
    with _sf_token_lock:
        now = datetime.now()
        age = (now - _sf_token_cache['fetched_at']).total_seconds() if _sf_token_cache['fetched_at'] else 99999
        if not _sf_token_cache['token'] or age > 5400:
            token, instance_url = _refresh_sf_token()
            if token:
                _sf_token_cache['token']        = token
                _sf_token_cache['instance_url'] = instance_url
                _sf_token_cache['fetched_at']   = now
                print('[SF-auth] Token refreshed')
        return _sf_token_cache['token'], _sf_token_cache['instance_url']

_soql_use_cli_fallback = False
_soql_cli_fallback_lock = threading.Lock()

_cli_semaphore = threading.Semaphore(4)  # limit concurrent sf CLI processes

def _soql_via_cli(query):
    """Execute SOQL using 'sf data query' subprocess.
    Used as fallback when REST API auth is broken (token redacted by newer SF CLI).
    Uses a semaphore to cap concurrent Node.js (sf) processes on Railway."""
    import tempfile, os as _os
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')
    # Write query to a temp file to avoid shell argument length limits
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.soql', delete=False) as tf:
            tf.write(query)
            tmp_path = tf.name
        with _cli_semaphore:
            r = subprocess.run(
                ['sf', 'data', 'query',
                 '--file', tmp_path,
                 '--target-org', SF_ORG,
                 '--json'],
                capture_output=True, text=True, timeout=90, env=env
            )
        # SF CLI sometimes emits warnings/preamble before the JSON blob
        raw = r.stdout or ''
        json_start = raw.find('{')
        if json_start > 0:
            raw = raw[json_start:]
        if not raw.strip():
            print(f'[SOQL-CLI] empty stdout (rc={r.returncode}) stderr={r.stderr[:200]}')
            return None
        d = json.loads(raw)
        if d.get('status') == 0:
            result = d.get('result', {})
            return {
                'records':   result.get('records', []),
                'totalSize': result.get('totalSize', 0),
            }
        print(f'[SOQL-CLI] query failed status={d.get("status")}: {d.get("message","")[:200]}')
    except Exception as e:
        print(f'[SOQL-CLI] exception: {e} | query[:80]={query[:80]}')
    finally:
        if tmp_path:
            try:
                _os.unlink(tmp_path)
            except Exception:
                pass
    return None


def soql(query, retries=2, paginate=True):
    """Run a SOQL query via Salesforce REST API — fast, no sf CLI subprocess.
    paginate=True  → follows nextRecordsUrl to retrieve all pages (needed for
                     large Lead ID queries that exceed the 2000-row page size).
    paginate=False → single-page only (use for Task COUNT/WhoId queries to avoid
                     excessive API calls that trigger Salesforce rate limits).

    Falls back to 'sf data query' CLI subprocess automatically when REST auth
    returns a redacted/invalid token (SF CLI v2.x behaviour)."""
    global _soql_use_cli_fallback

    # Fast path: if we already know REST is broken, go straight to CLI
    if _soql_use_cli_fallback:
        return _soql_via_cli(query)

    import urllib.error as _urllib_err
    for attempt in range(retries):
        token, instance_url = _get_sf_token()
        if not token:
            print('[SOQL] No access token — switching to CLI fallback')
            with _soql_cli_fallback_lock:
                _soql_use_cli_fallback = True
            return _soql_via_cli(query)
        try:
            all_records = []
            total_size  = 0
            url = f"{instance_url}/services/data/v59.0/query?q={urllib.parse.quote(query)}"
            while url:
                req = _urllib_req.Request(url, headers={'Authorization': f'Bearer {token}'})
                with _urllib_req.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
                all_records.extend(data.get('records', []))
                if total_size == 0:
                    total_size = data.get('totalSize', 0)
                # Only follow pagination if requested — Task queries use paginate=False
                # to stay within one API call per batch and avoid rate limits
                next_path = data.get('nextRecordsUrl') if paginate else None
                url = f"{instance_url}{next_path}" if next_path else None
            return {'records': all_records, 'totalSize': total_size}
        except _urllib_err.HTTPError as e:
            if e.code in (401, 403):
                print(f'[SOQL] Auth error HTTP {e.code} — switching to CLI fallback')
                with _sf_token_lock:
                    _sf_token_cache['token'] = None
                with _soql_cli_fallback_lock:
                    _soql_use_cli_fallback = True
                return _soql_via_cli(query)
            print(f'[SOQL] HTTP error (attempt {attempt+1}): {e}')
            with _sf_token_lock:
                _sf_token_cache['token'] = None
            if attempt < retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f'[SOQL] error (attempt {attempt+1}): {e}')
            with _sf_token_lock:
                _sf_token_cache['token'] = None  # force token refresh on retry
            if attempt < retries - 1:
                time.sleep(2)
    return None


def setup_sf_auth():
    """Authenticate sf CLI via JWT when running on Railway (cloud).
    Skipped automatically on local dev (env vars not set)."""
    jwt_key   = os.environ.get('SF_JWT_KEY', '').strip()
    client_id = os.environ.get('SF_CLIENT_ID', '').strip()
    if not jwt_key or not client_id:
        print('[sf-auth] No JWT env vars found — assuming local sf CLI session.')
        return
    try:
        key_path = '/tmp/sf_jwt.key'
        with open(key_path, 'w') as f:
            f.write(jwt_key.replace('\\n', '\n'))
        env = os.environ.copy()
        env['PATH'] = '/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')
        r = subprocess.run([
            'sf', 'org', 'login', 'jwt',
            '--username',     SF_ORG,
            '--jwt-key-file', key_path,
            '--client-id',    client_id,
            '--instance-url', SF_BASE_URL,
            '--set-default',
        ], capture_output=True, text=True, env=env)
        os.remove(key_path)
        if r.returncode == 0:
            print('[sf-auth] ✅ JWT login successful')
        else:
            print(f'[sf-auth] ❌ JWT login failed:\n{r.stderr}')
    except Exception as e:
        print(f'[sf-auth] exception: {e}')

# ── SDR extraction ────────────────────────────────────────────────────────────
# ── SFDC name → preferred display name ───────────────────────────────────────
# Exact values returned by Meeting_Generated_by__c / SDR_Owner__c on SFDC
SFDC_NAME_MAP = {
    # ── Active SDRs — canonical display names ─────────────────────────────────
    # SFDC raw value (Meeting_Generated_by__c / SDR_Owner__c) → canonical name
    'Akil Krishna':          'Akil Krishna',
    'Akhilesh':              'Akhilesh Stan',
    'Ananya Rao':            'Ananya Rao',
    'Anurup Bhattacharjee':  'Anurup Bhattacharjee',
    'Anushka HB':            'Anushka HB',
    'Deborah':               'Deborah Deborah',
    'Hreeman Saha':          'Hreeman Saha',
    'Isaac Bartels':         'Isaac Bartels',
    'Michelle B':            'Michelle B',
    'Rithick S':             'Rithick S',
    'Saka':                  'Saka Thapa',
    'Samridhi Dutta':        'Samridhi Dutta',
    'Shahana Abbasi':        'Shahana Abbasi',
    'Soham Saha':            'Soham Saha',
    'Sukhneet':              'Sukhneet Sukhneet',
    # ── Other / alumni SDRs ───────────────────────────────────────────────────
    'Felix':                 'Felix Sam',
    'Felix Sam':             'Felix Sam',
    'Matt Bates':            'Matt',
    'Abhishek Dutta':        'Abhishek',
    'Dushyant':              'Dushyant',
    'Hursh':                 'Hursh',
}

def norm_sdr(name):
    """Normalize a raw SFDC SDR name to the preferred display name."""
    if not name:
        return name
    return SFDC_NAME_MAP.get(name, name)

# Reverse map: display name → SFDC raw name (for querying SFDC by display name)
SFDC_NAME_MAP_REVERSE = {v: k for k, v in SFDC_NAME_MAP.items()}

# Campaign naming convention: "Campaign Name_SDR Name" or "Campaign Name_SDR Name_Date"
KNOWN_SDRS = {
    # lowercase keyword extracted from campaign name → canonical display name
    'akhilesh':  'Akhilesh Stan',
    'akil':      'Akil Krishna',
    'ananya':    'Ananya Rao',
    'anurup':    'Anurup Bhattacharjee',
    'anushka':   'Anushka HB',
    'deborah':   'Deborah Deborah',
    'hreeman':   'Hreeman Saha',
    'issac':     'Isaac Bartels',
    'isaac':     'Isaac Bartels',
    'michelle':  'Michelle B',
    'rithick':   'Rithick S',
    'rithik':    'Rithick S',
    'saka':      'Saka Thapa',
    'samridhi':  'Samridhi Dutta',
    'samrudhi':  'Samridhi Dutta',
    'shahana':   'Shahana Abbasi',
    'soham':     'Soham Saha',
    'sukhneet':  'Sukhneet Sukhneet',
    # Other / alumni
    'felix':     'Felix Sam',
    'hursh':     'Hursh',
    'sheetal':   'Sheetal',
    'dushyant':  'Dushyant',
    'matt':      'Matt',
    'abhishek':  'Abhishek',
    'neil':      'Neil',
    'bianca':    'Bianca',
    'ibtesaam':  'Ibtesaam',
    'lisa':      'Lisa',
}

DATE_WORDS = {'jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec',
              'january','february','march','april','june','july','august','september',
              'october','november','december','st','nd','rd','th'}

def extract_sdr(campaign_name):
    """Smart SDR extraction using 'Campaign_SDRName' convention.
    Checks segments right-to-left, skips date/number segments."""
    parts = campaign_name.replace(' ', '_').split('_')
    for part in reversed(parts):
        token = part.strip().lower()
        # Skip pure date/number segments
        if token.isdigit() or token in DATE_WORDS or any(c.isdigit() for c in token):
            # Keep checking if token looks like "23rd", "11th", "6th" etc.
            core = token.rstrip('stndrh')
            if core.isdigit():
                continue
        if token in KNOWN_SDRS:
            return KNOWN_SDRS[token]
    # Fallback: check if any known SDR name appears as a word in the full name
    lower = campaign_name.lower()
    for k, v in KNOWN_SDRS.items():
        if f' {k}' in lower or f'_{k}' in lower:
            return v
    return ''

def esc(s):
    return s.replace("'", "\\'")

def cnt(res):
    """Extract COUNT(Id) result (returns as expr0)."""
    try:
        return int(res['records'][0].get('expr0', 0) or 0)
    except Exception:
        return 0

# ── Period date helpers ───────────────────────────────────────────────────────

def period_dates(period, custom_start=None, custom_end=None):
    """Return (start_str, end_str) as YYYY-MM-DD for SOQL date filters."""
    today = date.today()
    if period == '7d':
        start = today - timedelta(days=7)
    elif period == '30d':
        start = today - timedelta(days=30)
    elif period == 'qtd':
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        start = date(today.year, q_start_month, 1)
    elif period == 'custom' and custom_start and custom_end:
        return custom_start, custom_end
    else:
        return None, None
    return start.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

# ── Per-campaign metrics ──────────────────────────────────────────────────────

def campaign_metrics(c, start_override=None, end_override=None):
    n     = esc(c['name'])
    start = start_override if start_override is not None else (c.get('start_date') or '').strip()
    end   = end_override   if end_override   is not None else (c.get('end_date')   or '').strip()

    # Build optional date filters
    dt_task = ''
    if start: dt_task += f" AND ActivityDate >= {start}"
    if end:   dt_task += f" AND ActivityDate <= {end}"

    dt_mtg = ''
    if start: dt_mtg += f" AND Meeting_Generated_on__c >= {start}"
    if end:   dt_mtg += f" AND Meeting_Generated_on__c <= {end}"

    # If S1 was manually overridden, skip the S1 Salesforce query entirely
    manual_s1 = c.get('manual_s1')
    has_manual_s1 = manual_s1 is not None and manual_s1 != ''

    results = {}

    # ── Step 1: fetch current Lead IDs for this campaign ─────────────────────
    # We always query the live Campaign__c filter first to discover the current
    # leads, then merge them into the frozen ledger.
    lead_res = soql(f"SELECT Id FROM Lead WHERE Campaign__c = '{n}' LIMIT 10000")
    current_lead_ids = [r['Id'] for r in (lead_res.get('records') or [])] if lead_res else []
    total_leads_check = len(current_lead_ids)

    # Merge current Lead IDs into the frozen ledger so we never lose them
    frozen_lead_ids = merge_leads_into_ledger(c['id'], current_lead_ids)

    # ── Step 2: fetch meeting-lead details (name/title/company) ──────────────
    if current_lead_ids:
        # Only query meeting details for leads currently in this campaign
        mtg_res = soql(
            f"SELECT Id, Meeting_Generated_on__c, Meeting_Generated_by__c, "
            f"Name, Title, Company FROM Lead "
            f"WHERE Campaign__c = '{n}' AND Meeting_Generated_on__c != null LIMIT 2000"
        )
        sf_meeting_records = (mtg_res.get('records') or []) if mtg_res else []
    else:
        sf_meeting_records = []

    # ── Step 3: merge meetings into ledger & compute frozen meeting totals ────
    frozen_meetings = merge_meetings_into_ledger(c['id'], sf_meeting_records)
    total_meetings, sdr_bk = meetings_from_ledger(frozen_meetings, start or None, end or None)

    # ── Step 4: calls / emails / other queries using frozen Lead IDs ─────────
    # Using frozen_lead_ids (not live Campaign__c filter) ensures counts are
    # correct even after leads are reassigned to a different campaign.
    if frozen_lead_ids:
        call_subj  = "Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%'"
        email_subj = "Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%'"

        done_filter   = ("Meeting_Status__c IN ('Meeting Done-Nurture',"
                         "'Meeting Done- Not Interested','Meeting Done-Unqualified')")
        noshow_filter = "Meeting_Status__c = 'Meeting No Show'"
        sql_filter    = "Status = 'SQL'"
        stssdr_filter = (done_filter + " OR " + noshow_filter)

        with ThreadPoolExecutor(max_workers=8) as ex:
            f_calls   = ex.submit(_count_tasks_for_ids,        frozen_lead_ids, call_subj,  dt_task)
            f_emails  = ex.submit(_count_tasks_for_ids,        frozen_lead_ids, email_subj, dt_task)
            f_ucalled = ex.submit(_count_distinct_who_for_ids, frozen_lead_ids, call_subj,  dt_task)
            f_uemailed= ex.submit(_count_distinct_who_for_ids, frozen_lead_ids, email_subj, dt_task)

            # All frozen — use lead IDs from ledger so counts don't drift
            # when a lead is reassigned to a different campaign
            f_done    = ex.submit(_count_leads_for_ids,     frozen_lead_ids, done_filter)
            f_noshow  = ex.submit(_count_leads_for_ids,     frozen_lead_ids, noshow_filter)
            f_sql     = ex.submit(_count_leads_for_ids,     frozen_lead_ids, sql_filter)
            f_stssdr  = ex.submit(_agg_status_sdr_for_ids,  frozen_lead_ids, stssdr_filter)
            f_sqlsdr  = ex.submit(_agg_sdr_count_for_ids,   frozen_lead_ids, sql_filter)

            if not has_manual_s1:
                f_s1 = ex.submit(soql, f"SELECT COUNT(Id) FROM Opportunity "
                                       f"WHERE Id IN (SELECT ConvertedOpportunityId FROM Lead "
                                       f"WHERE Campaign__c = '{n}' AND IsConverted = true)")

        total_calls          = f_calls.result()
        total_emails         = f_emails.result()
        unique_leads_called  = f_ucalled.result()
        unique_leads_emailed = f_uemailed.result()
        results['meeting_done']   = f_done.result()   # int
        results['meeting_noshow'] = f_noshow.result() # int
        results['sql_gen']        = f_sql.result()    # int
        results['status_sdr']     = f_stssdr.result()
        results['sql_sdr']        = f_sqlsdr.result()
        if not has_manual_s1:
            results['s1'] = f_s1.result()
    else:
        total_calls = total_emails = unique_leads_called = unique_leads_emailed = 0

    total_leads = len(frozen_lead_ids)  # use frozen count so it never shrinks

    # Parse per-SDR status breakdown
    status_sdr_bk = {}
    if results.get('status_sdr') and results['status_sdr'].get('records'):
        for rec in results['status_sdr']['records']:
            sdr_name = norm_sdr(rec.get('Meeting_Generated_by__c') or 'Unknown')
            status   = rec.get('Meeting_Status__c') or ''
            count    = int(rec.get('expr0', 0) or 0)
            if sdr_name not in status_sdr_bk:
                status_sdr_bk[sdr_name] = {'meeting_done': 0, 'meeting_noshow': 0, 'sql_gen': 0}
            if status in ('Meeting Done-Nurture', 'Meeting Done- Not Interested', 'Meeting Done-Unqualified'):
                status_sdr_bk[sdr_name]['meeting_done'] += count
            elif status == 'Meeting No Show':
                status_sdr_bk[sdr_name]['meeting_noshow'] += count

    if results.get('sql_sdr') and results['sql_sdr'].get('records'):
        for rec in results['sql_sdr']['records']:
            sdr_name = norm_sdr(rec.get('Meeting_Generated_by__c') or 'Unknown')
            count    = int(rec.get('expr0', 0) or 0)
            if sdr_name not in status_sdr_bk:
                status_sdr_bk[sdr_name] = {'meeting_done': 0, 'meeting_noshow': 0, 'sql_gen': 0}
            status_sdr_bk[sdr_name]['sql_gen'] += count

    calls_per_called_lead   = round(total_calls  / unique_leads_called,  1) if unique_leads_called  > 0 else 0
    emails_per_emailed_lead = round(total_emails / unique_leads_emailed, 1) if unique_leads_emailed > 0 else 0

    return {
        **c,
        'total_leads':              total_leads,
        'total_calls':              total_calls,
        'total_emails':             total_emails,
        'unique_leads_called':      unique_leads_called,
        'unique_leads_emailed':     unique_leads_emailed,
        'calls_per_called_lead':    calls_per_called_lead,
        'emails_per_emailed_lead':  emails_per_emailed_lead,
        'meetings':           total_meetings,
        's1_created':         int(manual_s1) if has_manual_s1 else cnt(results.get('s1')),
        's1_is_manual':       has_manual_s1,
        'meeting_done':       results.get('meeting_done', 0),
        'meeting_noshow':     results.get('meeting_noshow', 0),
        'sql_gen':            results.get('sql_gen', 0),
        'sdr_breakdown':      sdr_bk,
        'status_sdr_breakdown': [{'name': k, **v} for k, v in status_sdr_bk.items()],
        'synced_at':          datetime.now().isoformat()
    }

# ── Opportunity stats fetch (S1 count + NPV) ─────────────────────────────────

def fetch_sdr_opp_stats(start_date=None, end_date=None):
    """Return {display_name: {'s1': count, 'npv': amount}} for Opportunities
    where SDR_Owner__c is filled, within the given date range.
    Defaults to NPV_START_DATE → present."""
    start = f"{start_date}T00:00:00Z" if start_date else NPV_START_DATE
    end_clause = f" AND CreatedDate <= {end_date}T23:59:59Z" if end_date else ""
    q_str = (f"SELECT SDR_Owner__c, COUNT(Id), SUM(Amount) FROM Opportunity "
             f"WHERE CreatedDate >= {start} "
             f"AND SDR_Owner__c != null{end_clause} "
             f"GROUP BY SDR_Owner__c")
    result = soql(q_str)
    stats  = {}
    if result and result.get('records'):
        for rec in result['records']:
            raw    = (rec.get('SDR_Owner__c') or '').strip()
            name   = norm_sdr(raw) if raw else ''
            count  = int(rec.get('expr0') or 0)    # COUNT(Id)  → S1
            amount = float(rec.get('expr1') or 0)  # SUM(Amount) → NPV
            if name:
                prev = stats.get(name, {'s1': 0, 'npv': 0.0})
                stats[name] = {'s1': prev['s1'] + count, 'npv': prev['npv'] + amount}
    return stats

# ── SDR aggregation ───────────────────────────────────────────────────────────

def build_sdr_stats(enriched, sdr_opp_stats=None):
    """Aggregate per-SDR stats.

    Two sources of truth:
    1. campaign.sdr_owner  → owns that campaign's leads/calls/emails
    2. sdr_breakdown       → who actually generated each meeting (Meeting_Generated_by__c)
    """
    sdr = {}

    def ensure(name):
        if name not in sdr:
            sdr[name] = {
                'name':           name,
                'meetings':       0,
                'calls':          0,
                'emails':         0,
                'leads':          0,
                'meeting_done':   0,
                'meeting_noshow': 0,
                'sql_gen':        0,
                's1_count':       0,
                'npv_generated':  0.0,
                'campaigns':      {},   # campaign_name → meetings_generated
            }

    # Pass 1 – assign leads/calls/emails to the campaign SDR owner
    for c in enriched:
        owner = norm_sdr((c.get('sdr_owner') or '').strip())
        if not owner:
            continue
        ensure(owner)
        sdr[owner]['calls']  += c.get('total_calls',  0)
        sdr[owner]['emails'] += c.get('total_emails', 0)
        sdr[owner]['leads']  += c.get('total_leads',  0)
        # Pre-register the campaign with 0 meetings (updated in pass 2 if needed)
        if c['name'] not in sdr[owner]['campaigns']:
            sdr[owner]['campaigns'][c['name']] = 0

    # Pass 2 – assign meetings from Meeting_Generated_by__c breakdown.
    # Meetings are always credited to whoever generated them, but the campaign
    # only appears in that SDR's campaign list if they are the sdr_owner.
    # This prevents campaigns from showing under the wrong SDR card when a
    # different SDR happens to generate a meeting inside another SDR's campaign.
    for c in enriched:
        camp_owner = norm_sdr((c.get('sdr_owner') or '').strip())
        for b in c.get('sdr_breakdown', []):
            name = b['name']
            ensure(name)
            sdr[name]['meetings'] += b['meetings']
            # Only list campaign under this SDR if they own it
            if name == camp_owner:
                prev = sdr[name]['campaigns'].get(c['name'], 0)
                sdr[name]['campaigns'][c['name']] = prev + b['meetings']

    # Pass 3 – assign meeting_done / meeting_noshow / sql_gen from status_sdr_breakdown
    for c in enriched:
        for b in c.get('status_sdr_breakdown', []):
            name = b['name']
            ensure(name)
            sdr[name]['meeting_done']   += b.get('meeting_done',   0)
            sdr[name]['meeting_noshow'] += b.get('meeting_noshow', 0)
            sdr[name]['sql_gen']        += b.get('sql_gen',        0)

    # Pass 4 – attach S1 count + NPV from Opportunity stats (names already normalized)
    if sdr_opp_stats:
        for name, s in sdr.items():
            opp = sdr_opp_stats.get(name, {'s1': 0, 'npv': 0.0})
            s['s1_count']      = int(opp['s1'])
            s['npv_generated'] = float(opp['npv'])

    # Convert campaigns dict → list
    result = []
    for s in sdr.values():
        s['campaigns'] = [{'name': k, 'meetings': v}
                          for k, v in sorted(s['campaigns'].items(),
                                             key=lambda x: x[1], reverse=True)]
        result.append(s)

    return sorted(result, key=lambda x: x['meetings'], reverse=True)

# ── Sync ──────────────────────────────────────────────────────────────────────

def sync():
    """Run the sync and ALWAYS report status to Slack — even on a fatal crash,
    in which case the full traceback (file + line) is sent for debugging."""
    if cache['is_syncing']:
        return
    cache['is_syncing']     = True
    cache['sync_progress']  = 0
    cache['errors']         = []
    cache['fatal_error']    = None
    try:
        _run_sync()
    except Exception:
        import traceback
        tb = traceback.format_exc()
        cache['fatal_error'] = tb
        cache['is_syncing']  = False
        print('[sync] FATAL ERROR:\n' + tb)
    _notify_slack_sync()

def _run_sync():
    # Auto-complete campaigns whose end date has passed before syncing metrics
    auto_complete_campaigns()

    camps = load_campaigns()
    if not camps:
        cache['is_syncing'] = False
        return

    results    = []
    total      = len(camps)
    done_n     = [0]

    # Process 3 campaigns at a time — REST API is lightweight
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(campaign_metrics, c): c for c in camps}
        for f in as_completed(futs):
            try:
                results.append(f.result())
            except Exception as e:
                c = futs[f]
                import traceback as _tb
                tb_lines = _tb.format_exc().strip().splitlines()
                loc = next((l.strip() for l in reversed(tb_lines) if l.strip().startswith('File ')), '')
                cache['errors'].append(f"{c['name']}: {type(e).__name__}: {e} [{loc}]")
                results.append({**c, 'total_leads': 0, 'total_calls': 0,
                                'total_emails': 0, 'meetings': 0,
                                's1_created': 0, 'sdr_breakdown': [],
                                'meeting_done': 0, 'meeting_noshow': 0,
                                'sql_gen': 0, 'status_sdr_breakdown': [],
                                'unique_leads_called': 0, 'unique_leads_emailed': 0,
                                'calls_per_called_lead': 0, 'emails_per_emailed_lead': 0})
            done_n[0] += 1
            cache['sync_progress'] = int(done_n[0] / total * 100)

    # Restore original order
    order = {c['id']: i for i, c in enumerate(camps)}
    results.sort(key=lambda x: order.get(x['id'], 999))

    # Fetch S1 count + NPV from Opportunities (single query, runs after campaign sync)
    sdr_opp_stats = fetch_sdr_opp_stats()

    # Global true totals — avoid double-counting that happens when leads move
    # between campaigns (frozen-ID approach counts them in every campaign they
    # ever belonged to, inflating per-campaign sums).  These single queries
    # count each lead exactly once.
    global_done   = cnt(soql("SELECT COUNT(Id) FROM Lead WHERE Meeting_Status__c IN "
                             "('Meeting Done-Nurture','Meeting Done- Not Interested',"
                             "'Meeting Done-Unqualified')")) or 0
    global_noshow = cnt(soql("SELECT COUNT(Id) FROM Lead WHERE Meeting_Status__c = 'Meeting No Show'")) or 0
    global_sql    = cnt(soql("SELECT COUNT(Id) FROM Lead WHERE Status = 'SQL'")) or 0
    global_s1     = sum(v['s1'] for v in sdr_opp_stats.values()) if sdr_opp_stats else 0

    # ── Protect against wiping good cached data with silent query failures ────
    # If a sync produces 0 calls/emails for a campaign that previously had
    # non-zero values, keep the old numbers (Salesforce rate limit or network
    # hiccup returned None → 0, not a real zero).
    old_by_id = {c['id']: c for c in (cache.get('campaigns') or [])}
    for r in results:
        old = old_by_id.get(r['id'], {})
        if r.get('total_calls', 0) == 0 and old.get('total_calls', 0) > 0:
            r['total_calls']           = old['total_calls']
            r['unique_leads_called']   = old.get('unique_leads_called', 0)
            r['calls_per_called_lead'] = old.get('calls_per_called_lead', 0)
        if r.get('total_emails', 0) == 0 and old.get('total_emails', 0) > 0:
            r['total_emails']             = old['total_emails']
            r['unique_leads_emailed']     = old.get('unique_leads_emailed', 0)
            r['emails_per_emailed_lead']  = old.get('emails_per_emailed_lead', 0)

    # Keep old global totals if new sync returned all zeros (indicates query failure)
    old_totals = cache.get('totals') or {}
    new_totals = {
        'meeting_done':   global_done,
        'meeting_noshow': global_noshow,
        'sql_gen':        global_sql,
        's1':             global_s1,
    }
    if all(v == 0 for v in new_totals.values()) and any(v > 0 for v in old_totals.values()):
        new_totals = old_totals  # keep old totals — new ones look like query failures

    cache['campaigns']     = results
    cache['sdr_stats']     = build_sdr_stats(results, sdr_opp_stats)
    cache['totals']        = new_totals
    cache['last_sync']     = datetime.now().isoformat()
    cache['is_syncing']    = False
    cache['sync_progress'] = 100

    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump({'campaigns': results, 'sdr_stats': cache['sdr_stats'],
                       'totals': cache['totals'], 'last_sync': cache['last_sync']}, f)
    except Exception as e:
        print(f'[cache] save error: {e}')

def _notify_slack_sync():
    """Post a sync-status summary to Slack via an incoming webhook.
    On a fatal crash, sends the full traceback (file + line) for debugging.
    Set SLACK_WEBHOOK_URL on Railway to enable; silently skips otherwise."""
    url = os.environ.get('SLACK_WEBHOOK_URL', '').strip()
    if not url:
        return
    totals = cache.get('totals', {})
    camps  = cache.get('campaigns', [])
    errors = cache.get('errors', [])
    fatal  = cache.get('fatal_error')
    ist    = datetime.utcnow() + timedelta(hours=5, minutes=30)
    ts     = ist.strftime('%d %b %Y, %I:%M %p IST')

    if fatal:
        # Sync crashed entirely — send the full traceback so it can be fixed.
        text = (
            f"*❌ Dashboard sync FAILED*\n"
            f"🕒 {ts}\n"
            f"The sync crashed before finishing. Full error (copy this to Claude):\n"
            f"```{fatal[-2800:]}```"
        )
    else:
        calls  = sum(c.get('total_calls', 0)  or 0 for c in camps)
        emails = sum(c.get('total_emails', 0) or 0 for c in camps)
        status = ('✅ Dashboard sync successful' if not errors
                  else f'⚠️ Dashboard sync completed with {len(errors)} error(s)')
        lines = [
            f"*{status}*",
            f"🕒 {ts}",
            f"📊 Campaigns synced: *{len(camps)}*",
            f"📞 Calls: *{calls:,}*    ✉️ Emails: *{emails:,}*",
            f"🤝 Meetings Done: *{totals.get('meeting_done', 0)}*    🚫 No-Show: *{totals.get('meeting_noshow', 0)}*",
            f"💎 SQL: *{totals.get('sql_gen', 0)}*    🏆 S1: *{totals.get('s1', 0)}*",
        ]
        if errors:
            lines.append(f"*⚠️ {len(errors)} campaign error(s)* (copy to Claude to fix):")
            for e in errors[:8]:
                lines.append(f"• {str(e)[:350]}")
            if len(errors) > 8:
                lines.append(f"…and {len(errors) - 8} more")
        text = '\n'.join(lines)

    try:
        req = _urllib_req.Request(
            url, method='POST',
            data=json.dumps({'text': text}).encode(),
            headers={'Content-Type': 'application/json'})
        _urllib_req.urlopen(req, timeout=15)
    except Exception as e:
        print(f'[slack] webhook error: {e}')

def bg_loop():
    """Auto-sync twice daily: 05:00 IST (23:30 UTC) and 17:00 IST (11:30 UTC)."""
    # Sync times in UTC hours/minutes
    SYNC_TIMES = [(23, 30), (11, 30)]   # 05:00 IST, 17:00 IST
    while True:
        now_utc = datetime.utcnow()
        # Find the next upcoming sync time
        candidates = []
        for h, m in SYNC_TIMES:
            t = now_utc.replace(hour=h, minute=m, second=0, microsecond=0)
            if t <= now_utc:
                t = t.replace(day=t.day + 1)
            candidates.append(t)
        next_sync = min(candidates)
        seconds_until = (next_sync - now_utc).total_seconds()
        time.sleep(seconds_until)
        sync()

# ── Campaign config CRUD ──────────────────────────────────────────────────────

def load_campaigns():
    if os.path.exists(CAMPS_FILE):
        with open(CAMPS_FILE) as f:
            return json.load(f)
    return []

def save_campaigns(data):
    with open(CAMPS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def effective_end_date(c):
    """Return the effective end date for a campaign as a date object.
    If end_date is set, use it. Otherwise use start_date + 14 calendar days.
    Returns None if neither date is available."""
    end = (c.get('end_date') or '').strip()
    if end:
        try:
            return datetime.strptime(end, '%Y-%m-%d').date()
        except ValueError:
            pass
    start = (c.get('start_date') or '').strip()
    if start:
        try:
            return datetime.strptime(start, '%Y-%m-%d').date() + timedelta(days=14)
        except ValueError:
            pass
    return None

def auto_complete_campaigns():
    """Auto-set status to Completed for campaigns whose effective end date has passed.
    Also back-fills missing end_date with start_date + 14 days."""
    camps   = load_campaigns()
    today   = date.today()
    changed = False
    for c in camps:
        eff_end = effective_end_date(c)
        # Back-fill end_date if missing but start_date exists
        if not (c.get('end_date') or '').strip() and (c.get('start_date') or '').strip():
            try:
                computed = (datetime.strptime(c['start_date'], '%Y-%m-%d').date()
                            + timedelta(days=14)).strftime('%Y-%m-%d')
                c['end_date'] = computed
                changed = True
            except ValueError:
                pass
        # Auto-complete if past effective end date and not already Completed
        if eff_end and today > eff_end and c.get('status') != 'Completed':
            c['status'] = 'Completed'
            changed = True
    if changed:
        save_campaigns(camps)
        print(f'[auto-complete] updated campaign statuses on {today}')

# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    return jsonify({
        'campaigns':     cache['campaigns'],
        'sdr_stats':     cache['sdr_stats'],
        'totals':        cache.get('totals', {}),
        'last_sync':     cache['last_sync'],
        'is_syncing':    cache['is_syncing'],
        'sync_progress': cache['sync_progress'],
    })

@app.route('/api/check-admin', methods=['POST'])
def api_check_admin():
    """Validate an admin token sent from the browser."""
    token = (request.json or {}).get('token', '').strip()
    return jsonify({'ok': token == ADMIN_TOKEN})

@app.route('/api/sync', methods=['POST'])
@require_admin
def api_sync():
    if not cache['is_syncing']:
        threading.Thread(target=sync, daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/cache-disk')
@require_admin
def api_cache_disk():
    """Read data_cache.json directly from disk and return summary (bypasses in-memory cache)."""
    try:
        with open(CACHE_FILE) as f:
            d = json.load(f)
        camps = d.get('campaigns', [])
        calls = sum(c.get('total_calls', 0) for c in camps)
        emails = sum(c.get('total_emails', 0) for c in camps)
        return jsonify({
            'source': 'disk',
            'last_sync': d.get('last_sync'),
            'total_calls': calls,
            'total_emails': emails,
            'num_campaigns': len(camps),
            'totals': d.get('totals', {}),
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/debug-campaign-metrics')
@require_admin
def api_debug_campaign_metrics():
    """Run campaign_metrics() for one campaign and return all intermediate values."""
    camps = load_campaigns()
    if not camps:
        return jsonify({'error': 'no campaigns'})
    # Pick a campaign with known leads
    with _ledger_lock:
        ledger = load_ledger()
    target_camp = None
    for c in camps:
        entry = ledger.get(c['id'], {})
        if len(entry.get('lead_ids', [])) > 0:
            target_camp = c
            break
    if not target_camp:
        target_camp = camps[0]
    # Run full campaign_metrics but with debug info
    c = target_camp
    n = esc(c['name'])
    start = (c.get('start_date') or '').strip()
    end = (c.get('end_date') or '').strip()
    dt_task = ''
    if start: dt_task += f" AND ActivityDate >= {start}"
    if end:   dt_task += f" AND ActivityDate <= {end}"
    # Step 1: get current lead IDs via soql
    lead_res = soql(f"SELECT Id FROM Lead WHERE Campaign__c = '{n}' LIMIT 10000")
    current_ids = [r['Id'] for r in (lead_res.get('records') or [])] if lead_res else []
    # Check if Id is actually coming back (might be lowercase or missing)
    raw_record_keys = list((lead_res.get('records') or [{}])[0].keys()) if lead_res and lead_res.get('records') else []
    first_id = current_ids[0] if current_ids else None
    frozen_ids = merge_leads_into_ledger(c['id'], current_ids)
    # Run task count with frozen IDs
    call_subj = "Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%'"
    total_calls = _count_tasks_for_ids(frozen_ids, call_subj, dt_task)
    total_emails = _count_tasks_for_ids(frozen_ids, "Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%'", dt_task)
    # Test with first 5 IDs directly
    sample_ids = frozen_ids[:5]
    sample_ids_str = ','.join(f"'{lid}'" for lid in sample_ids) if sample_ids else "'NONE'"
    sample_count = cnt(soql(f"SELECT COUNT(Id) FROM Task WHERE ({call_subj}) AND WhoId IN ({sample_ids_str}){dt_task}", paginate=False))
    return jsonify({
        'campaign': c['name'],
        'current_lead_count': len(current_ids),
        'frozen_lead_count': len(frozen_ids),
        'first_id': first_id,
        'raw_record_keys': raw_record_keys,
        'soql_use_cli_fallback': _soql_use_cli_fallback,
        'dt_task': dt_task,
        'total_calls': total_calls,
        'total_emails': total_emails,
        'sample_5_ids_call_count': sample_count,
    })


@app.route('/api/debug-tasks')
@require_admin
def api_debug_tasks():
    """Diagnostic: test Task queries for the first campaign that has leads.
    Returns raw counts with and without filters so we can see why calls = 0."""
    camps = load_campaigns()
    if not camps:
        return jsonify({'error': 'no campaigns'})

    # Pick the first campaign that has lead IDs in the ledger
    with _ledger_lock:
        ledger = load_ledger()

    target_camp = None
    target_ids  = []
    for c in camps:
        entry = ledger.get(c['id'], {})
        ids   = entry.get('lead_ids', [])
        if ids:
            target_camp = c
            target_ids  = ids[:50]   # just first 50 leads for a quick test
            break

    if not target_camp:
        return jsonify({'error': 'no leads in ledger yet — run a sync first'})

    ids_str   = ','.join(f"'{lid}'" for lid in target_ids)
    start     = (target_camp.get('start_date') or '').strip()
    end       = (target_camp.get('end_date')   or '').strip()
    dt_task   = ''
    if start: dt_task += f" AND ActivityDate >= {start}"
    if end:   dt_task += f" AND ActivityDate <= {end}"
    dt_created = ''
    if start: dt_created += f" AND CreatedDate >= {start}T00:00:00Z"
    if end:   dt_created += f" AND CreatedDate <= {end}T23:59:59Z"

    call_subj  = "Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%'"
    email_subj = "Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%'"

    results = {}

    # 1. Any tasks at all for these leads (no filters)
    r1 = soql(f"SELECT COUNT(Id) FROM Task WHERE WhoId IN ({ids_str})", paginate=False)
    results['total_tasks_no_filter'] = cnt(r1)

    # 2. Tasks with call subject only
    r2 = soql(f"SELECT COUNT(Id) FROM Task WHERE ({call_subj}) AND WhoId IN ({ids_str})", paginate=False)
    results['call_tasks_no_date'] = cnt(r2)

    # 3. Tasks with email subject only
    r3 = soql(f"SELECT COUNT(Id) FROM Task WHERE ({email_subj}) AND WhoId IN ({ids_str})", paginate=False)
    results['email_tasks_no_date'] = cnt(r3)

    # 4. Call tasks with ActivityDate filter (current logic)
    r4 = soql(f"SELECT COUNT(Id) FROM Task WHERE ({call_subj}) AND WhoId IN ({ids_str}){dt_task}", paginate=False)
    results['call_tasks_activity_date_filter'] = cnt(r4)

    # 5. Call tasks with CreatedDate filter (alternative)
    r5 = soql(f"SELECT COUNT(Id) FROM Task WHERE ({call_subj}) AND WhoId IN ({ids_str}){dt_created}", paginate=False)
    results['call_tasks_created_date_filter'] = cnt(r5)

    # 6. Sample task subjects to see what actually exists
    r6 = soql(f"SELECT Subject, ActivityDate, CreatedDate FROM Task WHERE WhoId IN ({ids_str}) LIMIT 10", paginate=False)
    sample_tasks = []
    if r6 and r6.get('records'):
        for t in r6['records']:
            sample_tasks.append({
                'subject':       t.get('Subject'),
                'activity_date': t.get('ActivityDate'),
                'created_date':  (t.get('CreatedDate') or '')[:10],
            })
    results['sample_task_subjects'] = sample_tasks

    # 7. Check Events object (some dialers log as Events, not Tasks)
    r7 = soql(f"SELECT COUNT(Id) FROM Event WHERE WhoId IN ({ids_str})", paginate=False)
    results['total_events_no_filter'] = cnt(r7)

    # 8. Use FRESH lead IDs from Salesforce (not ledger) for the same campaign
    camp_name = esc(target_camp.get('name', ''))
    live_res = soql(f"SELECT Id FROM Lead WHERE Campaign__c = '{camp_name}' LIMIT 50", paginate=False)
    live_ids = [r['Id'] for r in (live_res.get('records') or [])] if live_res else []
    if live_ids:
        live_ids_str = ','.join(f"'{lid}'" for lid in live_ids)
        r8 = soql(f"SELECT COUNT(Id) FROM Task WHERE WhoId IN ({live_ids_str})", paginate=False)
        results['tasks_for_live_lead_ids'] = cnt(r8)
        # Also check if any of the live IDs differ from ledger IDs
        live_set   = set(live_ids)
        ledger_set = set(target_ids)
        results['live_ids_not_in_ledger']   = len(live_set - ledger_set)
        results['ledger_ids_not_in_live']   = len(ledger_set - live_set)
    else:
        results['tasks_for_live_lead_ids'] = 'could not fetch live IDs'

    # 9. Sample any Tasks from the whole org (not filtered by lead) to confirm Task access
    r9 = soql("SELECT Subject, WhoId FROM Task LIMIT 5", paginate=False)
    sample_org_tasks = []
    if r9 and r9.get('records'):
        for t in r9['records']:
            sample_org_tasks.append({
                'subject': t.get('Subject'),
                'who_id_prefix': (t.get('WhoId') or '')[:3],  # 00Q=Lead, 003=Contact
            })
    results['sample_org_tasks'] = sample_org_tasks
    results['total_org_tasks_sample'] = r9.get('totalSize', 0) if r9 else 'query failed'

    return jsonify({
        'campaign':     target_camp.get('name'),
        'start_date':   start,
        'end_date':     end,
        'leads_tested': len(target_ids),
        'results':      results,
        'note':         '00Q prefix = Lead, 003 prefix = Contact in WhoId',
    })

@app.route('/api/debug-auth')
@require_admin
def api_debug_auth():
    """Diagnostic: show Salesforce auth state and attempt a fresh token fetch."""
    import subprocess

    # 1. Check what the token cache currently holds
    with _sf_token_lock:
        has_token   = bool(_sf_token_cache.get('token'))
        fetched_at  = str(_sf_token_cache.get('fetched_at'))
        token_start = (_sf_token_cache.get('token') or '')[:8] + '...' if has_token else None

    # 2. Try refreshing the token right now
    fresh_token, instance_url = _refresh_sf_token()

    # 3. Run sf org display raw to see what it actually returns
    try:
        env = os.environ.copy()
        env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')
        r = subprocess.run(
            ['sf', 'org', 'display', '--target-org', SF_ORG, '--json'],
            capture_output=True, text=True, timeout=30, env=env
        )
        sf_display_stdout = r.stdout[:500] if r.stdout else '(empty)'
        sf_display_stderr = r.stderr[:500] if r.stderr else '(empty)'
        sf_display_code   = r.returncode
    except Exception as e:
        sf_display_stdout = f'exception: {e}'
        sf_display_stderr = ''
        sf_display_code   = -1

    # 4. Try sf org auth show-access-token directly (with auto-confirm)
    try:
        env2 = os.environ.copy()
        env2['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env2.get('PATH', '')
        r2 = subprocess.run(
            ['sf', 'org', 'auth', 'show-access-token', '--target-org', SF_ORG],
            capture_output=True, text=True, timeout=30, env=env2,
            input='y\n'
        )
        raw_lines = [ln.strip() for ln in (r2.stdout or '').splitlines() if ln.strip()]
        show_token_stdout = str(raw_lines)  # ALL lines for debug
        show_token_stderr = r2.stderr[:300] if r2.stderr else '(empty)'
        show_token_rc     = r2.returncode
        # Also show raw stdout first 800 chars for analysis
        show_token_raw_stdout = repr((r2.stdout or '')[:800])
        # Check if any line looks like a real token — split on │ to handle box table
        import re as _re_dbg
        clean_out = _re_dbg.sub(r'\x1b\[[0-9;?]*[A-Za-z]', '', r2.stdout or '')
        token_line = None
        for ln in reversed(clean_out.splitlines()):
            stripped = ln.strip()
            if '│' in stripped:
                parts = [p.strip() for p in stripped.split('│') if p.strip()]
                for i, part in enumerate(parts):
                    if part.lower() in ('access token', 'token') and i + 1 < len(parts):
                        cand = parts[i + 1]
                        if len(cand) > 20 and ' ' not in cand and '[REDACTED]' not in cand and any(c.isalnum() for c in cand[:10]):
                            token_line = cand
                            break
            if token_line:
                break
        show_token_works  = bool(token_line)
        show_token_preview = (token_line or '')[:20] + '...' if token_line else None
    except Exception as e:
        show_token_stdout = f'exception: {e}'
        show_token_stderr = ''
        show_token_rc     = -1
        show_token_works  = False
        show_token_preview = None

    # 5. Check key.json structure (for encrypted token debugging)
    key_json_info = {}
    try:
        kp = os.path.expanduser('~/.sfdx/key.json')
        if os.path.exists(kp):
            with open(kp) as f:
                kd = json.load(f)
            key_json_info = {k: (v[:8]+'...' if isinstance(v,str) and len(v)>8 else v) for k,v in kd.items()}
    except Exception as ke:
        key_json_info = {'error': str(ke)}
    # Also show first 16 chars of encrypted token in SFDX file
    sfdx_token_preview = None
    try:
        ap = os.path.expanduser(f'~/.sfdx/{SF_ORG}.json')
        if os.path.exists(ap):
            with open(ap) as f:
                ad = json.load(f)
            tok = ad.get('accessToken', '')
            sfdx_token_preview = tok[:16] + '...' if len(tok) > 16 else tok
    except Exception:
        pass

    # 5b. Check auth file locations on disk
    import glob as _glob
    auth_files = {}
    for pattern in [
        os.path.expanduser('~/.sfdx/*.json'),
        os.path.expanduser('~/.sf/orgs/*/*.json'),
        os.path.expanduser('~/.sf/orgs/*/*/*.json'),
    ]:
        for fpath in _glob.glob(pattern)[:3]:
            try:
                with open(fpath) as f:
                    raw = json.load(f)
                tok = raw.get('accessToken', '')
                auth_files[fpath] = {
                    'has_accessToken': bool(tok),
                    'token_redacted': '[REDACTED]' in tok if tok else False,
                    'instanceUrl': raw.get('instanceUrl', ''),
                }
            except Exception as ef:
                auth_files[fpath] = {'error': str(ef)}

    # 6. Test CLI fallback directly
    cli_test = _soql_via_cli("SELECT COUNT(Id) FROM Lead")
    cli_lead_count = cnt(cli_test)

    # 7. Check env vars are set (don't expose values)
    jwt_key_set   = bool(os.environ.get('SF_JWT_KEY', '').strip())
    client_id_set = bool(os.environ.get('SF_CLIENT_ID', '').strip())

    # 8. Force-clear cached token so next soql() uses the fresh token
    with _sf_token_lock:
        _sf_token_cache['token'] = fresh_token
        _sf_token_cache['instance_url'] = instance_url
        _sf_token_cache['fetched_at'] = datetime.now() if fresh_token else None

    # 9. Direct REST call to test token (bypass soql() to see raw error)
    rest_direct_result = None
    rest_direct_error  = None
    try:
        import urllib.error as _ue
        test_url = f"{instance_url}/services/data/v59.0/query?q=SELECT+COUNT(Id)+FROM+Lead"
        test_req = _urllib_req.Request(test_url, headers={'Authorization': f'Bearer {fresh_token}'})
        with _urllib_req.urlopen(test_req, timeout=15) as resp:
            raw = json.loads(resp.read().decode())
            rest_direct_result = raw.get('records', [{}])[0].get('expr0', 'no-expr0')
    except _ue.HTTPError as he:
        body = he.read().decode('utf-8', errors='replace')[:400]
        rest_direct_error = f'HTTP {he.code}: {body}'
    except Exception as ex:
        rest_direct_error = str(ex)[:200]

    # 10. Try REST query via soql() to confirm (may already be in CLI fallback mode)
    global _soql_use_cli_fallback
    was_cli_fallback = _soql_use_cli_fallback
    _soql_use_cli_fallback = False  # force REST attempt
    test_q = soql("SELECT COUNT(Id) FROM Lead", paginate=False)
    test_lead_count = cnt(test_q)
    # If REST worked, great; else the soql() call above will have re-enabled CLI fallback

    return jsonify({
        'sf_org':               SF_ORG,
        'sf_base_url':          SF_BASE_URL,
        'env_SF_JWT_KEY_set':   jwt_key_set,
        'env_SF_CLIENT_ID_set': client_id_set,
        'cached_token_exists':  has_token,
        'cached_token_start':   token_start,
        'cached_fetched_at':    fetched_at,
        'fresh_token_obtained': bool(fresh_token),
        'fresh_token_start':    (fresh_token or '')[:8] + '...' if fresh_token else None,
        'sf_display_returncode': sf_display_code,
        'sf_display_stdout':    sf_display_stdout,
        'sf_display_stderr':    sf_display_stderr,
        'show_access_token_rc':         show_token_rc,
        'show_access_token_works':      show_token_works,
        'show_access_token_token_preview': show_token_preview,
        'show_access_token_lines':      show_token_stdout,
        'show_access_token_stderr':     show_token_stderr,
        'show_access_token_raw_stdout': show_token_raw_stdout,
        'auth_files_on_disk':         auth_files,
        'cli_fallback_lead_count':     cli_lead_count,
        'rest_lead_count':             test_lead_count,
        'cli_fallback_active':         _soql_use_cli_fallback,
        'rest_direct_result':          rest_direct_result,
        'rest_direct_error':           rest_direct_error,
        'key_json_info':               key_json_info,
        'sfdx_encrypted_token_preview': sfdx_token_preview,
    })

@app.route('/api/debug-aes')
@require_admin
def api_debug_aes():
    """Show raw AES decrypt diagnostics — key lengths, formats tried, results."""
    out = {}
    try:
        key_file  = os.path.expanduser('~/.sfdx/key.json')
        auth_file = os.path.expanduser(f'~/.sfdx/{SF_ORG}.json')
        out['key_file_exists']  = os.path.exists(key_file)
        out['auth_file_exists'] = os.path.exists(auth_file)
        if os.path.exists(key_file):
            with open(key_file) as f: kd = json.load(f)
            out['key_json_keys'] = list(kd.keys())
            key_hex = kd.get('key', '')
            out['key_hex_len']   = len(key_hex)
            out['key_hex_start'] = key_hex[:16]
        if os.path.exists(auth_file):
            with open(auth_file) as f: ad = json.load(f)
            enc_tok = ad.get('accessToken', '')
            out['enc_tok_len']   = len(enc_tok)
            out['enc_tok_start'] = enc_tok[:40]
            out['enc_tok_has_colon'] = ':' in enc_tok
            out['enc_tok_colon_pos'] = enc_tok.index(':') if ':' in enc_tok else None
        # Try all decrypt approaches
        results = []
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
        key_bytes = bytes.fromhex(key_hex)
        # Format discovered: IV(32 hex) + CIPHER_HEX + ':' + TAG(32 hex)
        body       = enc_tok[32:]
        cipher_stripped = body.split(':')[0] if ':' in body else body
        candidates = [
            (enc_tok[:32], cipher_stripped,    'iv16+colon-strip'),   # ← correct
            (enc_tok[:32], enc_tok[32:],        'iv16+no-strip'),
            (enc_tok[:32], enc_tok[32:].split(':')[1] if ':' in enc_tok[32:] else '', 'iv16+after-colon'),
            (enc_tok[:16], enc_tok[16:],        'iv8+full'),
        ]
        for iv_hex, ct_hex, fmt in candidates:
            if not ct_hex:
                results.append({'fmt': fmt, 'ok': False, 'error': 'empty ct_hex'})
                continue
            try:
                dec = unpad(
                    AES.new(key_bytes, AES.MODE_CBC, bytes.fromhex(iv_hex))
                       .decrypt(bytes.fromhex(ct_hex)),
                    AES.block_size
                ).decode('utf-8', errors='replace').strip()
                results.append({'fmt': fmt, 'ok': True, 'preview': dec[:40], 'len': len(dec)})
            except Exception as e:
                results.append({'fmt': fmt, 'ok': False, 'error': str(e)})
        out['decrypt_attempts'] = results
    except Exception as e:
        out['error'] = str(e)
    return jsonify(out)

@app.route('/api/debug-node')
@require_admin
def api_debug_node():
    """Run Node.js decrypt inline + show raw output for diagnosis."""
    out = {}
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')

    # Which node?
    try:
        wn = subprocess.run(['which', 'node'], capture_output=True, text=True, timeout=5, env=env)
        out['node_path'] = wn.stdout.strip() or '(not found)'
        wn2 = subprocess.run(['node', '--version'], capture_output=True, text=True, timeout=5, env=env)
        out['node_version'] = wn2.stdout.strip()
    except Exception as e:
        out['node_path'] = f'error: {e}'

    # Read raw files
    try:
        kp = os.path.expanduser('~/.sfdx/key.json')
        ap = os.path.expanduser(f'~/.sfdx/{SF_ORG}.json')
        with open(kp) as f: kd = json.load(f)
        with open(ap) as f: ad = json.load(f)
        out['key_hex_len'] = len(kd.get('key', ''))
        out['key_hex_start'] = kd.get('key', '')[:16]
        enc = ad.get('accessToken', '')
        out['enc_len'] = len(enc)
        out['enc_start'] = enc[:40]
        out['enc_colon_idx'] = enc.index(':') if ':' in enc else None
    except Exception as e:
        out['file_read_error'] = str(e)

    # Run Node.js script — exhaustive: CBC, GCM (with auth tag), stream modes
    node_script = r"""
const fs=require('fs'),crypto=require('crypto'),home=process.env.HOME||'/root';
try{
  const kd=JSON.parse(fs.readFileSync(home+'/.sfdx/key.json','utf8'));
  const ad=JSON.parse(fs.readFileSync(home+'/.sfdx/""" + SF_ORG + r""".json','utf8'));
  const key=Buffer.from(kd.key,'hex');
  const enc=ad.accessToken;
  const inst=ad.instanceUrl||'';
  const colonIdx=enc.indexOf(':');
  const afterColon=colonIdx>=0?enc.slice(colonIdx+1):'';
  const results=[];

  // ── CBC modes ─────────────────────────────────────────────────────────────
  for(const algo of ['aes-'+key.length*8+'-cbc','aes-128-cbc','aes-256-cbc','des3']){
    for(const ivBytes of [8,12,16,24]){
      const ivHex=enc.slice(0,ivBytes*2);
      const ctFull=enc.slice(ivBytes*2);
      const ctHex=ctFull.includes(':')?ctFull.split(':')[0]:ctFull;
      try{
        const d=crypto.createDecipheriv(algo,key,Buffer.from(ivHex,'hex'));
        const dec=Buffer.concat([d.update(Buffer.from(ctHex,'hex')),d.final()]).toString('utf8').trim();
        results.push({algo,ivBytes,mode:'cbc',ok:true,preview:dec.slice(0,30),len:dec.length});
      }catch(e){results.push({algo,ivBytes,mode:'cbc',ok:false,err:e.message.slice(0,60)});}
    }
  }

  // ── GCM modes — try both hex-decoded key AND raw-bytes key ──────────────
  // @salesforce/core v5+ uses aes-256-gcm. The key might be stored as raw
  // bytes in key.json (Buffer.from(key,'utf8') = 32B) not hex-decoded (16B).
  const keyRaw=Buffer.from(kd.key);  // raw UTF-8/ASCII bytes of the key string
  for(const [keyBuf,keyLabel] of [[key,'hex-decoded'],[keyRaw,'raw-bytes']]){
    for(const algo of ['aes-'+keyBuf.length*8+'-gcm']){
      for(const ivBytes of [8,12,16]){
        const ivHex=enc.slice(0,ivBytes*2);
        const ctHex=colonIdx>ivBytes*2?enc.slice(ivBytes*2,colonIdx):enc.slice(ivBytes*2);
        const tagHex=afterColon;
        if(!ctHex||ctHex.length%2!==0) continue;
        try{
          const d=crypto.createDecipheriv(algo,keyBuf,Buffer.from(ivHex,'hex'));
          if(tagHex&&tagHex.length===32) d.setAuthTag(Buffer.from(tagHex,'hex'));
          const dec=Buffer.concat([d.update(Buffer.from(ctHex,'hex')),d.final()]).toString('utf8').trim();
          results.push({algo,ivBytes,mode:'gcm',keyLabel,ok:true,preview:dec.slice(0,30),len:dec.length});
        }catch(e){results.push({algo,ivBytes,mode:'gcm',keyLabel,ok:false,err:e.message.slice(0,60)});}
      }
    }
  }
  // Also try CBC with raw-bytes key (32B = AES-256-CBC)
  for(const ivBytes of [16]){
    const ctFull=enc.slice(ivBytes*2);
    const ctHex=ctFull.includes(':')?ctFull.split(':')[0]:ctFull;
    try{
      const d=crypto.createDecipheriv('aes-256-cbc',keyRaw,Buffer.from(enc.slice(0,ivBytes*2),'hex'));
      const dec=Buffer.concat([d.update(Buffer.from(ctHex,'hex')),d.final()]).toString('utf8').trim();
      results.push({algo:'aes-256-cbc',ivBytes,mode:'cbc',keyLabel:'raw-bytes',ok:true,preview:dec.slice(0,30),len:dec.length});
    }catch(e){results.push({algo:'aes-256-cbc',ivBytes,mode:'cbc',keyLabel:'raw-bytes',ok:false,err:e.message.slice(0,60)});}
  }

  // ── Stream modes (CFB, OFB, CTR) — no padding needed ─────────────────────
  for(const algo of ['aes-'+key.length*8+'-cfb','aes-'+key.length*8+'-ofb','aes-'+key.length*8+'-ctr']){
    for(const ivBytes of [8,16]){
      const ivHex=enc.slice(0,ivBytes*2);
      const ctFull=enc.slice(ivBytes*2);
      const ctHex=ctFull.includes(':')?ctFull.split(':')[0]:ctFull;
      if(!ctHex||ctHex.length%2!==0) continue;
      try{
        const d=crypto.createDecipheriv(algo,key,Buffer.from(ivHex,'hex'));
        const dec=Buffer.concat([d.update(Buffer.from(ctHex,'hex')),d.final()]).toString('utf8').trim();
        results.push({algo,ivBytes,mode:'stream',ok:true,preview:dec.slice(0,30),len:dec.length});
      }catch(e){results.push({algo,ivBytes,mode:'stream',ok:false,err:e.message.slice(0,60)});}
    }
  }

  // ── Read @salesforce/core crypto source to find exact algorithm ──────────
  let coreInfo = {};
  try{
    const corePkg=require.resolve('@salesforce/core/package.json');
    const coreDir=corePkg.replace('/package.json','');
    const pkgData=JSON.parse(fs.readFileSync(corePkg,'utf8'));
    coreInfo.version=pkgData.version;
    // Try to read the crypto implementation file
    const cryptoPaths=['lib/crypto/crypto.js','lib/crypto.js','lib/util/crypto.js'];
    for(const cp of cryptoPaths){
      const fp=coreDir+'/'+cp;
      if(fs.existsSync(fp)){
        const src=fs.readFileSync(fp,'utf8');
        coreInfo.cryptoFile=cp;
        // Extract key lines about the algorithm
        const lines=src.split('\n').filter(l=>l.includes('ALGO')||l.includes('aes')||l.includes('gcm')||l.includes('cbc')||l.includes('IV_BYTES')||l.includes('iv')||l.includes('createCipheriv'));
        coreInfo.algoLines=lines.slice(0,20);
        break;
      }
    }
  }catch(ex){coreInfo.error=ex.message;}

  process.stdout.write(JSON.stringify({ok:true,key_len:key.length,enc_len:enc.length,colon_idx:colonIdx,after_colon_len:afterColon.length,inst,results,coreInfo}));
}catch(e){process.stdout.write(JSON.stringify({ok:false,error:e.message}));}
"""
    try:
        nr = subprocess.run(['node', '-e', node_script],
                            capture_output=True, text=True, timeout=20, env=env)
        out['node_rc']     = nr.returncode
        out['node_stdout'] = nr.stdout[:8000]
        out['node_stderr'] = nr.stderr[:500]
        if nr.stdout.strip():
            try:
                nd = json.loads(nr.stdout.strip())
                out['node_parsed'] = nd
                # Find any successful decrypt with a plausible token
                for r in (nd.get('results') or []):
                    if r.get('ok') and r.get('len', 0) > 20:
                        out['best_candidate'] = r
                        break
            except Exception as je:
                out['json_parse_error'] = str(je)
    except Exception as e:
        out['node_run_error'] = str(e)

    return jsonify(out)

@app.route('/api/debug-sfcore')
@require_admin
def api_debug_sfcore():
    """Find @salesforce/core crypto source and read the encryption algorithm."""
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')
    script = r"""
const fs=require('fs'), path=require('path');
const out={};
// 1. Find @salesforce/core
try{
  const p=require.resolve('@salesforce/core');
  out.core_main=p;
  out.core_dir=path.dirname(p);
}catch(e){out.resolve_error=e.message;}

// 2. Search for crypto files in common SF CLI locations
const searchDirs=[
  '/usr/local/lib/node_modules/@salesforce',
  '/usr/lib/node_modules/@salesforce',
  process.env.HOME+'/.local/share/sf/client/node_modules/@salesforce',
  '/usr/local/lib/node_modules/@salesforce/cli/node_modules/@salesforce/core',
];
out.found_crypto_files=[];
function findCrypto(dir){
  if(!fs.existsSync(dir)) return;
  try{
    const entries=fs.readdirSync(dir,{withFileTypes:true});
    for(const e of entries){
      if(e.isDirectory()&&!e.name.startsWith('.')){
        const sub=dir+'/'+e.name;
        if(e.name==='crypto'||e.name.includes('crypt')){
          out.found_crypto_files.push(sub);
          // Try to read any .js file here
          try{
            const files=fs.readdirSync(sub).filter(f=>f.endsWith('.js'));
            for(const f of files){
              const src=fs.readFileSync(sub+'/'+f,'utf8');
              const algoMatch=src.match(/ALGO\s*=\s*['"]([^'"]+)['"]/);
              const ivMatch=src.match(/IV_BYTES\s*=\s*(\d+)/);
              if(algoMatch||ivMatch){
                out.crypto_algo=algoMatch?algoMatch[1]:null;
                out.iv_bytes=ivMatch?parseInt(ivMatch[1]):null;
                out.crypto_source_file=sub+'/'+f;
                const lines=src.split('\n').filter(l=>l.includes('ALGO')||l.includes('IV_BYTES')||l.includes('createCipheriv')||l.includes('getAuthTag')||l.includes('tag'));
                out.key_lines=lines.slice(0,15);
              }
            }
          }catch(re){}
        }
        if(out.found_crypto_files.length<5) findCrypto(sub);
      }
    }
  }catch(e){}
}
for(const d of searchDirs) findCrypto(d);

// 3. Also try require directly
try{
  const c=require('@salesforce/core/lib/crypto/crypto');
  out.crypto_module_loaded=true;
}catch(e){
  try{
    const c=require('@salesforce/core/lib/crypto');
    out.crypto_module_loaded=true;
  }catch(e2){out.crypto_require_error=e2.message;}
}

process.stdout.write(JSON.stringify(out));
"""
    try:
        nr = subprocess.run(['node', '-e', script],
                            capture_output=True, text=True, timeout=20, env=env)
        result = {}
        if nr.stdout.strip():
            try: result = json.loads(nr.stdout.strip())
            except: result = {'raw': nr.stdout[:1000]}
        result['stderr'] = nr.stderr[:300] if nr.stderr else ''
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/campaigns', methods=['GET'])
def api_camps_get():
    return jsonify(load_campaigns())

@app.route('/api/campaigns', methods=['POST'])
@require_admin
def api_camps_add():
    d = request.json or {}
    name = (d.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Campaign name is required'}), 400
    camps = load_campaigns()
    # Auto-extract SDR if not explicitly provided
    sdr = (d.get('sdr_owner') or '').strip() or extract_sdr(name)
    c = {
        'id':          str(int(time.time() * 1000)),
        'name':        name,
        'sdr_owner':   sdr,
        'email_owner': (d.get('email_owner') or '').strip(),
        'status':      d.get('status', 'Active'),
        'segment':     (d.get('segment') or '').strip(),
        'pod_team':    (d.get('pod_team') or '').strip(),
        'start_date':  d.get('start_date', ''),
        'end_date':    d.get('end_date', ''),
    }
    camps.append(c)
    save_campaigns(camps)
    # Immediately add a stub entry to the in-memory cache so the UI sees it
    # without waiting for a full sync.  Metrics will be 0 until the per-campaign
    # sync completes (called by the frontend right after this request).
    stub = {**c, 'total_leads': 0, 'total_calls': 0, 'total_emails': 0,
            'meetings': 0, 'meeting_done': 0, 'meeting_noshow': 0,
            'sql_gen': 0, 's1_created': 0, 'unique_leads_called': 0,
            'unique_leads_emailed': 0, 'calls_per_called_lead': 0,
            'emails_per_emailed_lead': 0, 'sdr_breakdown': [],
            'synced_at': None}
    cache['campaigns'].append(stub)
    return jsonify(c), 201

@app.route('/api/campaigns/import', methods=['POST'])
@require_admin
def api_camps_import():
    data = request.json
    if not isinstance(data, list):
        return jsonify({'error': 'Expected a JSON array'}), 400
    save_campaigns(data)
    return jsonify({'imported': len(data)}), 200

@app.route('/api/meetings-leads')
def api_meetings_leads():
    """Return leads where Meeting_Generated_on__c is not null. Optional ?campaign= or ?segment= filter.
    When filtering by campaign, serves from the frozen ledger so results are
    correct even after leads have been reassigned to a different campaign.
    Optional ?start=YYYY-MM-DD&end=YYYY-MM-DD overrides campaign date range
    (used when a period filter is active on the dashboard)."""
    camp      = request.args.get('campaign', '').strip()
    segment   = request.args.get('segment',  '').strip()
    pod_team  = request.args.get('pod_team', '').strip()
    # camp_start / camp_end: filter WHICH campaigns are included by their start_date
    # (mirrors the period filter on the dashboard — same campaigns as the KPI card)
    camp_period_start = request.args.get('camp_start', '').strip() or None
    camp_period_end   = request.args.get('camp_end',   '').strip() or None
    if camp:
        # Serve from frozen ledger — immune to lead reassignment
        camps_cfg = load_campaigns()
        camp_cfg  = next((c for c in camps_cfg if c['name'] == camp), {})
        start   = (camp_cfg.get('start_date') or '').strip()
        end     = (camp_cfg.get('end_date')   or '').strip()
        camp_id = camp_cfg.get('id', '')

        if camp_id:
            with _ledger_lock:
                ledger = load_ledger()
                entry  = ledger.get(camp_id, {})
                meetings = entry.get('meetings', entry) if isinstance(entry, dict) and 'meetings' in entry else entry
            leads = meetings_leads_from_ledger(meetings, start or None, end or None)
            for l in leads:
                l['campaign'] = camp
            return jsonify({'leads': leads, 'total': len(leads)})

        # Fallback to live query if campaign not in ledger yet
        dt = ''
        if start: dt += f" AND Meeting_Generated_on__c >= {start}"
        if end:   dt += f" AND Meeting_Generated_on__c <= {end}"
        q = (f"SELECT Id, Name, Title, Company, Campaign__c, "
             f"Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Source__c "
             f"FROM Lead "
             f"WHERE Campaign__c = '{esc(camp)}' "
             f"AND Meeting_Generated_on__c != null{dt} "
             f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 500")
    else:
        # All campaigns view — serve entirely from the frozen ledger (no SOQL).
        # This is instant regardless of how many campaigns exist.
        camps_cfg = load_campaigns()
        if segment:
            camps_cfg = [c for c in camps_cfg if (c.get('segment') or '').strip() == segment]
        if pod_team:
            camps_cfg = [c for c in camps_cfg if (c.get('pod_team') or '').strip() == pod_team]
        # Filter WHICH campaigns are shown by their start_date (mirrors the dashboard
        # period filter so the modal matches the KPI card exactly)
        if camp_period_start:
            camps_cfg = [c for c in camps_cfg if (c.get('start_date') or '') >= camp_period_start]
        if camp_period_end:
            camps_cfg = [c for c in camps_cfg if (c.get('start_date') or '') <= camp_period_end]

        with _ledger_lock:
            ledger = load_ledger()

        all_leads = []
        for camp_cfg in camps_cfg:
            cid   = camp_cfg.get('id', '')
            cname = camp_cfg.get('name', '').strip()
            if not cid or not cname:
                continue
            # Use each campaign's own dates for meeting date filtering
            start = (camp_cfg.get('start_date') or '').strip() or None
            end   = (camp_cfg.get('end_date')   or '').strip() or None
            entry = ledger.get(cid, {})
            meetings = entry.get('meetings', entry) if isinstance(entry, dict) and 'meetings' in entry else entry
            rows = meetings_leads_from_ledger(meetings, start, end)
            for l in rows:
                l['campaign'] = cname
            all_leads.extend(rows)

        all_leads.sort(key=lambda l: l.get('date', ''), reverse=True)
        return jsonify({'leads': all_leads, 'total': len(all_leads)})

    result = soql(q)
    records = result.get('records', []) if result else []
    leads = []
    for r in records:
        lid = r.get('Id') or ''
        leads.append({
            'name':    r.get('Name') or '—',
            'title':   r.get('Title') or '—',
            'company': r.get('Company') or '—',
            'campaign':r.get('Campaign__c') or '—',
            'sdr':     r.get('Meeting_Generated_by__c') or '—',
            'date':    r.get('Meeting_Generated_on__c') or '',
            'source':  r.get('Meeting_Source__c') or '—',
            'sf_url':  f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
        })
    return jsonify({'leads': leads, 'total': len(leads)})

@app.route('/api/status-leads')
def api_status_leads():
    """Return leads filtered by Meeting_Status__c. ?status=done|noshow|sql [&sdr=] [&segment=]

    Uses the same frozen ledger IDs that the dashboard card counts use, so the
    modal always matches the card number exactly.
    """
    status        = request.args.get('status',   '').strip()
    sdr           = request.args.get('sdr',      '').strip()
    segment       = request.args.get('segment',  '').strip()
    pod_team      = request.args.get('pod_team', '').strip()
    camp_period_start = request.args.get('camp_start', '').strip() or None
    camp_period_end   = request.args.get('camp_end',   '').strip() or None

    STATUS_FILTERS = {
        'done':   "Meeting_Status__c IN ('Meeting Done-Nurture', 'Meeting Done- Not Interested', 'Meeting Done-Unqualified')",
        'noshow': "Meeting_Status__c = 'Meeting No Show'",
        'sql':    "Status = 'SQL'",
    }
    if status not in STATUS_FILTERS:
        return jsonify({'leads': [], 'total': 0})

    status_clause = STATUS_FILTERS[status]
    sdr_clause    = ''
    if sdr:
        sfdc_name  = SFDC_NAME_MAP_REVERSE.get(sdr, sdr)
        sdr_clause = f" AND Meeting_Generated_by__c = '{esc(sfdc_name)}'"

    # ── Collect frozen lead IDs from ledger (same source as the card counts) ──
    camps_cfg = load_campaigns()
    if segment:
        camps_cfg = [c for c in camps_cfg if (c.get('segment') or '').strip() == segment]
    if pod_team:
        camps_cfg = [c for c in camps_cfg if (c.get('pod_team') or '').strip() == pod_team]
    if camp_period_start:
        camps_cfg = [c for c in camps_cfg if (c.get('start_date') or '') >= camp_period_start]
    if camp_period_end:
        camps_cfg = [c for c in camps_cfg if (c.get('start_date') or '') <= camp_period_end]

    ledger = load_ledger()
    all_lead_ids = []
    seen = set()
    for camp_cfg in camps_cfg:
        cname = camp_cfg.get('name', '').strip()
        if not cname:
            continue
        entry    = ledger.get(cname, {})
        lead_ids = entry.get('lead_ids', []) if isinstance(entry, dict) else []
        for lid in lead_ids:
            if lid not in seen:
                seen.add(lid)
                all_lead_ids.append(lid)

    if not all_lead_ids:
        return jsonify({'leads': [], 'total': 0})

    # ── Query those IDs with the status filter (batched, no date restriction) ─
    all_records = []
    for i in range(0, len(all_lead_ids), _BATCH_SIZE):
        batch   = all_lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        q = (f"SELECT Id, Name, Title, Company, Campaign__c, "
             f"Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Status__c "
             f"FROM Lead "
             f"WHERE Id IN ({ids_str}) "
             f"AND {status_clause}{sdr_clause} "
             f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 500")
        res = soql(q, paginate=False)
        if res and res.get('records'):
            all_records.extend(res['records'])

    all_records.sort(key=lambda r: r.get('Meeting_Generated_on__c') or '', reverse=True)
    leads = []
    for r in all_records:
        lid = r.get('Id') or ''
        leads.append({
            'name':    r.get('Name') or '—',
            'title':   r.get('Title') or '—',
            'company': r.get('Company') or '—',
            'campaign':r.get('Campaign__c') or '—',
            'sdr':     norm_sdr(r.get('Meeting_Generated_by__c') or '—'),
            'date':    r.get('Meeting_Generated_on__c') or '',
            'status':  r.get('Meeting_Status__c') or '—',
            'sf_url':  f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
        })
    return jsonify({'leads': leads, 'total': len(leads)})


@app.route('/api/sdr-detail')
def api_sdr_detail():
    """Return Opportunities + Meeting Done leads + SQL leads for a given SDR display name."""
    sdr_display = request.args.get('sdr', '').strip()
    if not sdr_display:
        return jsonify({'opportunities': [], 'meeting_done_leads': [], 'sql_leads': []})

    sfdc_name = SFDC_NAME_MAP_REVERSE.get(sdr_display, sdr_display)
    sfdc_esc  = esc(sfdc_name)

    opp_q  = (f"SELECT Id, Name, Amount, StageName, Account.Name, CreatedDate "
              f"FROM Opportunity "
              f"WHERE SDR_Owner__c = '{sfdc_esc}' "
              f"AND CreatedDate >= {NPV_START_DATE} "
              f"ORDER BY Amount DESC NULLS LAST LIMIT 200")

    done_q = (f"SELECT Id, Name, Title, Company, Campaign__c, "
              f"Meeting_Generated_on__c, Meeting_Status__c "
              f"FROM Lead "
              f"WHERE Meeting_Generated_by__c = '{sfdc_esc}' "
              f"AND Meeting_Status__c IN ('Meeting Done-Nurture', "
              f"'Meeting Done- Not Interested', 'Meeting Done-Unqualified') "
              f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 200")

    sql_q  = (f"SELECT Id, Name, Title, Company, Campaign__c, "
              f"Meeting_Generated_on__c "
              f"FROM Lead "
              f"WHERE Meeting_Generated_by__c = '{sfdc_esc}' "
              f"AND Status = 'SQL' "
              f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 200")

    results = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(soql, opp_q): 'opps',
                ex.submit(soql, done_q): 'done',
                ex.submit(soql, sql_q):  'sql'}
        for f in as_completed(futs):
            results[futs[f]] = f.result()

    opps = []
    if results.get('opps') and results['opps'].get('records'):
        for r in results['opps']['records']:
            oid = r.get('Id') or ''
            acc = (r.get('Account') or {})
            opps.append({
                'name':    r.get('Name') or '—',
                'account': acc.get('Name') or '—',
                'stage':   r.get('StageName') or '—',
                'amount':  float(r.get('Amount') or 0),
                'date':    (r.get('CreatedDate') or '')[:10],
                'sf_url':  f"{SF_BASE_URL}/lightning/r/Opportunity/{oid}/view" if oid else '',
            })

    done_leads = []
    if results.get('done') and results['done'].get('records'):
        for r in results['done']['records']:
            lid = r.get('Id') or ''
            done_leads.append({
                'name':    r.get('Name') or '—',
                'title':   r.get('Title') or '—',
                'company': r.get('Company') or '—',
                'campaign':r.get('Campaign__c') or '—',
                'status':  r.get('Meeting_Status__c') or '—',
                'date':    (r.get('Meeting_Generated_on__c') or '')[:10],
                'sf_url':  f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
            })

    sql_leads = []
    if results.get('sql') and results['sql'].get('records'):
        for r in results['sql']['records']:
            lid = r.get('Id') or ''
            sql_leads.append({
                'name':    r.get('Name') or '—',
                'title':   r.get('Title') or '—',
                'company': r.get('Company') or '—',
                'campaign':r.get('Campaign__c') or '—',
                'date':    (r.get('Meeting_Generated_on__c') or '')[:10],
                'sf_url':  f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
            })

    return jsonify({'opportunities': opps,
                    'meeting_done_leads': done_leads,
                    'sql_leads': sql_leads})


@app.route('/api/time-data')
def api_time_data():
    """Return campaigns whose start_date falls within the period, with full metrics.
    ?period=7d|30d|qtd|custom  — custom requires &start=YYYY-MM-DD&end=YYYY-MM-DD
    Results cached for PERIOD_CACHE_TTL seconds (custom dates not cached)."""
    period       = request.args.get('period', '').strip().lower()
    custom_start = request.args.get('start', '').strip()
    custom_end   = request.args.get('end',   '').strip()
    refresh      = request.args.get('refresh', '').strip().lower() == '1'
    start, end   = period_dates(period, custom_start, custom_end)
    if not start:
        return jsonify({'error': 'Invalid period. Use 7d, 30d, qtd, or custom with start/end.'}), 400

    # ── Serve from cache if fresh (custom ranges are never cached) ──────────
    cache_key = period if period != 'custom' else None
    if cache_key:
        cached = period_cache.get(cache_key)
        if cached and not refresh:
            age = (datetime.now() - cached['fetched_at']).total_seconds()
            if age < PERIOD_CACHE_TTL:
                print(f'[period_cache] HIT {period} (age {int(age)}s)')
                return jsonify(cached['data'])
    print(f'[period_cache] MISS {period} — fetching from Salesforce…')

    camps = load_campaigns()
    if not camps:
        return jsonify({'campaigns': [], 'sdr_stats': [], 'period': period,
                        'start_date': start, 'end_date': end})

    # Filter campaigns whose start_date falls within the period window.
    # Campaigns with no start_date are excluded from period views.
    period_ids = {
        c['id'] for c in camps
        if start <= (c.get('start_date') or '') <= end
    }

    # Use already-cached metrics (populated by the last full sync) instead of
    # re-querying Salesforce. This makes period filter switches near-instant.
    # Falls back to the config entry (zero metrics) for campaigns not yet synced.
    cached_by_id = {c['id']: c for c in (cache.get('campaigns') or [])}
    config_by_id = {c['id']: c for c in camps}

    results = []
    for cid in period_ids:
        if cid in cached_by_id:
            results.append(cached_by_id[cid])
        else:
            # Campaign in config but not yet synced — include with zero metrics
            cfg = config_by_id[cid]
            results.append({**cfg, 'total_leads': 0, 'total_calls': 0,
                            'total_emails': 0, 'meetings': 0,
                            's1_created': 0, 'sdr_breakdown': [],
                            'meeting_done': 0, 'meeting_noshow': 0,
                            'sql_gen': 0, 'status_sdr_breakdown': [],
                            'unique_leads_called': 0, 'unique_leads_emailed': 0,
                            'calls_per_called_lead': 0, 'emails_per_emailed_lead': 0})

    # Restore original campaign order
    order = {c['id']: i for i, c in enumerate(camps)}
    results.sort(key=lambda x: order.get(x['id'], 999))

    sdr_opp_stats = fetch_sdr_opp_stats()
    sdr_stats     = build_sdr_stats(results, sdr_opp_stats)

    payload = {
        'campaigns':  results,
        'sdr_stats':  sdr_stats,
        'period':     period,
        'start_date': start,
        'end_date':   end,
    }

    # ── Store in cache (not for custom date ranges) ──────────────────────────
    if cache_key:
        period_cache[cache_key] = {'data': payload, 'fetched_at': datetime.now()}

    return jsonify({
        'campaigns':  results,
        'sdr_stats':  sdr_stats,
        'period':     period,
        'start_date': start,
        'end_date':   end,
    })


@app.route('/api/s1-opportunities')
def api_s1_opportunities():
    """Return Opportunities. Optional ?sdr=DisplayName and/or ?segment=SegmentName.
    When segment is provided, returns opps from converted leads in that segment's campaigns
    (consistent with how s1_created is calculated per campaign).
    Without segment, returns all opps with SDR_Owner__c since NPV_START_DATE."""
    sdr_display = request.args.get('sdr',      '').strip()
    segment     = request.args.get('segment',  '').strip()
    pod_team    = request.args.get('pod_team', '').strip()

    if segment or pod_team:
        camps_cfg = load_campaigns()
        filtered  = camps_cfg
        if segment:
            filtered = [c for c in filtered if (c.get('segment') or '').strip() == segment]
        if pod_team:
            filtered = [c for c in filtered if (c.get('pod_team') or '').strip() == pod_team]
        camp_names_list = [c['name'] for c in filtered]
        if not camp_names_list:
            return jsonify({'opportunities': [], 'total': 0})
        camp_names = ','.join(f"'{esc(n)}'" for n in camp_names_list)
        sdr_clause = ''
        if sdr_display:
            sfdc_name  = SFDC_NAME_MAP_REVERSE.get(sdr_display, sdr_display)
            sdr_clause = f" AND SDR_Owner__c = '{esc(sfdc_name)}'"
        q_str = (f"SELECT Id, Name, Amount, StageName, Account.Name, SDR_Owner__c, CreatedDate "
                 f"FROM Opportunity "
                 f"WHERE Id IN ("
                 f"  SELECT ConvertedOpportunityId FROM Lead "
                 f"  WHERE Campaign__c IN ({camp_names}) AND IsConverted = true"
                 f"){sdr_clause} "
                 f"ORDER BY CreatedDate DESC NULLS LAST LIMIT 500")
    else:
        sdr_clause = ''
        if sdr_display:
            sfdc_name  = SFDC_NAME_MAP_REVERSE.get(sdr_display, sdr_display)
            sdr_clause = f" AND SDR_Owner__c = '{esc(sfdc_name)}'"
        q_str = (f"SELECT Id, Name, Amount, StageName, Account.Name, SDR_Owner__c, CreatedDate "
                 f"FROM Opportunity "
                 f"WHERE CreatedDate >= {NPV_START_DATE} "
                 f"AND SDR_Owner__c != null"
                 f"{sdr_clause} "
                 f"ORDER BY CreatedDate DESC NULLS LAST LIMIT 500")

    result  = soql(q_str)
    records = result.get('records', []) if result else []
    opps    = []
    for r in records:
        oid = r.get('Id') or ''
        acc = (r.get('Account') or {})
        opps.append({
            'name':    r.get('Name') or '—',
            'account': acc.get('Name') or '—',
            'stage':   r.get('StageName') or '—',
            'amount':  float(r.get('Amount') or 0),
            'sdr':     norm_sdr((r.get('SDR_Owner__c') or '').strip()),
            'date':    (r.get('CreatedDate') or '')[:10],
            'sf_url':  f"{SF_BASE_URL}/lightning/r/Opportunity/{oid}/view" if oid else '',
        })
    return jsonify({'opportunities': opps, 'total': len(opps)})


@app.route('/api/nooks-call-detail')
def api_nooks_call_detail():
    """Drill-down: Nooks call stats for a campaign — connected and meetings.
    Uses SFDC Tasks filtered by campaign lead IDs (most reliable source of truth)."""
    campaign = request.args.get('campaign', '').strip()
    if not campaign:
        return jsonify({'error': 'campaign param required'}), 400

    n        = esc(campaign)
    lead_sub = f"SELECT Id FROM Lead WHERE Campaign__c = '{n}'"

    # Apply campaign date range so we only count calls made during the campaign period
    camps     = load_campaigns()
    camp_cfg  = next((c for c in camps if c.get('name') == campaign), {})
    start_date = (camp_cfg.get('start_date') or '').strip()
    end_date   = (camp_cfg.get('end_date')   or '').strip()
    dt_task    = ''
    if start_date: dt_task += f" AND ActivityDate >= {start_date}"
    if end_date:   dt_task += f" AND ActivityDate <= {end_date}"

    MEETING_RESULTS = [
        'Answered - Booked Meeting', 'Meeting',
        'Meeting Generated- Cold', 'Meeting Generated- Conference',
    ]
    # Connected = any disposition where someone actually picked up the phone
    # Includes meeting dispositions (booked meeting = definitely connected)
    CONNECTED_RESULTS = [
        'Answered - Follow Up Required', 'Answered - No Longer with Company',
        'Answered - Wrong Person, No Referral', 'Busy - Call Later', 'Connected',
        'DNC', 'Gatekeeper', 'Not Interested', 'Objection: Already Have Solution',
        'Objection: Asked to Send Info', 'Objection: Not A Priority',
        'Prospect Disconnected', 'Retired', 'Strong Follow up', 'Wrong Number',
    ] + MEETING_RESULTS   # meeting dispositions also mean the call connected

    connected_str = ','.join(f"'{v}'" for v in CONNECTED_RESULTS)
    meeting_str   = ','.join(f"'{v}'" for v in MEETING_RESULTS)

    queries = {
        'total':     f"SELECT COUNT(Id) FROM Task WHERE Subject LIKE '[Nooks Call]%' AND WhoId IN ({lead_sub}){dt_task}",
        'connected': f"SELECT COUNT(Id) FROM Task WHERE Subject LIKE '[Nooks Call]%' AND CallDisposition IN ({connected_str}) AND WhoId IN ({lead_sub}){dt_task}",
        'mtg_tasks': (f"SELECT WhoId, CallDisposition FROM Task "
                      f"WHERE Subject LIKE '[Nooks Call]%' AND CallDisposition IN ({meeting_str}) "
                      f"AND WhoId IN ({lead_sub}){dt_task} LIMIT 500"),
    }

    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(soql, q): k for k, q in queries.items()}
        for fut in as_completed(futs):
            results[futs[fut]] = fut.result()

    total_calls     = cnt(results.get('total'))
    calls_connected = cnt(results.get('connected'))

    # Meeting leads — collect unique WhoIds then get lead names from Salesforce
    mtg_who_result = {}   # who_id → call_disposition
    mtg_res = results.get('mtg_tasks')
    if mtg_res and mtg_res.get('records'):
        for r in mtg_res['records']:
            who_id = r.get('WhoId')
            if who_id and who_id not in mtg_who_result:
                mtg_who_result[who_id] = r.get('CallDisposition', '')

    meeting_leads = []
    if mtg_who_result:
        ids_str  = ','.join(f"'{i}'" for i in list(mtg_who_result.keys())[:200])
        lead_res = soql(f"SELECT Id, Name, Company FROM Lead WHERE Id IN ({ids_str})")
        _, instance_url = _get_sf_token()
        # Normalize keys to lowercase for 15-vs-18 char ID matching
        mtg_lookup = {k.lower(): v for k, v in mtg_who_result.items()}
        if lead_res and lead_res.get('records'):
            for r in lead_res['records']:
                lid = r.get('Id', '')
                disposition = mtg_lookup.get(lid.lower(), '') or mtg_lookup.get(lid[:15].lower(), '')
                meeting_leads.append({
                    'name':        r.get('Name', '—'),
                    'company':     r.get('Company', '—'),
                    'call_result': disposition,
                    'sf_url':      f"{instance_url}/lightning/r/Lead/{lid}/view" if lid else '',
                })

    return jsonify({
        'total_calls':       total_calls,
        'calls_connected':   calls_connected,
        'meetings_generated':len(meeting_leads),
        'meeting_leads':     meeting_leads,
    })


@app.route('/api/campaigns/<cid>', methods=['PUT'])
@require_admin
def api_camps_update(cid):
    d = request.json or {}
    camps = load_campaigns()
    for i, c in enumerate(camps):
        if c['id'] == cid:
            camps[i] = {**c, **d, 'id': cid}
            save_campaigns(camps)
            # Patch in-memory cache too
            for j, cc in enumerate(cache['campaigns']):
                if cc.get('id') == cid:
                    cache['campaigns'][j] = {**cc, **d, 'id': cid}
            return jsonify(camps[i])
    return jsonify({'error': 'Not found'}), 404

@app.route('/api/campaigns/<cid>', methods=['DELETE'])
@require_admin
def api_camps_delete(cid):
    save_campaigns([c for c in load_campaigns() if c['id'] != cid])
    cache['campaigns'] = [c for c in cache['campaigns'] if c.get('id') != cid]
    return jsonify({'ok': True})

@app.route('/api/campaigns/<cid>/sync', methods=['POST'])
@require_admin
def api_camp_sync(cid):
    """Re-sync metrics for a single campaign and patch the cache."""
    camps = load_campaigns()
    camp = next((c for c in camps if c['id'] == cid), None)
    if not camp:
        return jsonify({'error': 'Campaign not found'}), 404
    try:
        updated = campaign_metrics(camp)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    # Patch in-memory cache
    for i, cc in enumerate(cache['campaigns']):
        if cc.get('id') == cid:
            cache['campaigns'][i] = updated
            break
    else:
        cache['campaigns'].append(updated)
    # Invalidate period caches so they reflect the new data
    period_cache.clear()
    return jsonify(updated)

@app.route('/api/campaigns/bulk-status', methods=['POST'])
@require_admin
def api_camps_bulk_status():
    """Update status for multiple campaigns at once.
    Body: { ids: [...], status: 'Active'|'Paused'|'Completed' }"""
    d = request.json or {}
    ids    = set(d.get('ids') or [])
    status = (d.get('status') or '').strip()
    if not ids or status not in ('Active', 'Paused', 'Completed'):
        return jsonify({'error': 'Provide ids[] and a valid status'}), 400
    camps = load_campaigns()
    updated = 0
    for c in camps:
        if c['id'] in ids:
            c['status'] = status
            updated += 1
    save_campaigns(camps)
    # Patch in-memory cache too
    for cc in cache['campaigns']:
        if cc.get('id') in ids:
            cc['status'] = status
    return jsonify({'ok': True, 'updated': updated})

# ── Segments ──────────────────────────────────────────────────────────────────

@app.route('/api/segments', methods=['GET'])
def api_segments_get():
    custom = load_segments()
    all_segs = DEFAULT_SEGMENTS + [s for s in custom if s not in DEFAULT_SEGMENTS]
    return jsonify({'segments': all_segs, 'defaults': DEFAULT_SEGMENTS})

@app.route('/api/segments', methods=['POST'])
@require_admin
def api_segments_add():
    d = request.json or {}
    name = (d.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Segment name required'}), 400
    custom = load_segments()
    if name not in DEFAULT_SEGMENTS and name not in custom:
        custom.append(name)
        save_segments(custom)
    all_segs = DEFAULT_SEGMENTS + [s for s in custom if s not in DEFAULT_SEGMENTS]
    return jsonify({'segments': all_segs, 'defaults': DEFAULT_SEGMENTS})

# ── SDR Reporting 2026 – Meeting Leads ───────────────────────────────────────

@app.route('/api/meeting-leads')
def api_meeting_leads():
    """Return all leads where Meeting_Generated_on__c is set and > 2026-03-31."""
    q = (
        "SELECT Id, FirstName, LastName, Title, Company, "
        "Meeting_Status__c, Meeting_Generated_on__c, Meeting_Scheduled_On__c, "
        "Meeting_Generated_by__c, Meeting_Source__c, Meeting_Channel__c, "
        "Meeting_Type__c, Zoom_Meeting_Link_URL__c, Seller_Name__c "
        "FROM Lead "
        "WHERE Meeting_Generated_on__c != null "
        "AND Meeting_Generated_on__c > 2026-03-31 "
        "ORDER BY Meeting_Generated_on__c DESC NULLS LAST "
        "LIMIT 5000"
    )
    result = soql(q)
    records = result.get('records', []) if result else []
    leads = []
    for r in records:
        lid = r.get('Id') or ''
        first = r.get('FirstName') or ''
        last  = r.get('LastName')  or ''
        name  = f"{first} {last}".strip() or '—'
        leads.append({
            'id':           lid,
            'first_name':   first or '—',
            'last_name':    last  or '—',
            'name':         name,
            'title':        r.get('Title')                    or '—',
            'company':      r.get('Company')                  or '—',
            'status':       r.get('Meeting_Status__c')        or '—',
            'generated_on': (r.get('Meeting_Generated_on__c') or '')[:10],
            'scheduled_on': (r.get('Meeting_Scheduled_On__c') or '')[:10],
            'generated_by': norm_sdr(r.get('Meeting_Generated_by__c') or '—'),
            'source':       r.get('Meeting_Source__c')        or '—',
            'channel':      r.get('Meeting_Channel__c')       or '—',
            'type':         r.get('Meeting_Type__c')          or '—',
            'zoom_url':     r.get('Zoom_Meeting_Link_URL__c') or '',
            'seller':       r.get('Seller_Name__c')           or '—',
            'sf_url':       f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
        })
    return jsonify({'leads': leads, 'total': len(leads)})


# ── Zoom Server-to-Server integration ────────────────────────────────────────
import base64 as _b64
import re as _re_zoom

ZOOM_ACCOUNT_ID    = os.environ.get('ZOOM_ACCOUNT_ID', '')
ZOOM_CLIENT_ID     = os.environ.get('ZOOM_CLIENT_ID', '')
ZOOM_CLIENT_SECRET = os.environ.get('ZOOM_CLIENT_SECRET', '')

_zoom_token_cache   = {'token': None, 'expires_at': 0}
_zoom_token_lock    = threading.Lock()
_zoom_meeting_cache = {}        # meeting_id → {'status': 'correct'|'wrong', 'data': {...}}
_zoom_meeting_lock  = threading.Lock()

def _zoom_configured():
    return bool(ZOOM_ACCOUNT_ID and ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET)

def _zoom_token():
    """Return a cached Zoom S2S access token, refreshing when near expiry."""
    if not _zoom_configured():
        return None
    with _zoom_token_lock:
        now = time.time()
        if _zoom_token_cache['token'] and now < _zoom_token_cache['expires_at'] - 60:
            return _zoom_token_cache['token']
        try:
            auth = _b64.b64encode(f"{ZOOM_CLIENT_ID}:{ZOOM_CLIENT_SECRET}".encode()).decode()
            url  = ("https://zoom.us/oauth/token?grant_type=account_credentials"
                    f"&account_id={ZOOM_ACCOUNT_ID}")
            req  = _urllib_req.Request(url, method='POST',
                       headers={'Authorization': f'Basic {auth}'})
            with _urllib_req.urlopen(req, timeout=15) as resp:
                d = json.loads(resp.read().decode())
            tok = d.get('access_token')
            if tok:
                _zoom_token_cache['token']      = tok
                _zoom_token_cache['expires_at'] = now + int(d.get('expires_in', 3600))
                return tok
            print(f'[Zoom] token response had no access_token: {d}')
        except Exception as e:
            print(f'[Zoom] token error: {e}')
        return None

def _zoom_extract_meeting_id(url):
    """Extract the numeric Zoom meeting ID from a join URL. '' if none found."""
    if not url:
        return ''
    m = _re_zoom.search(r'/(?:j|w|wc/join)/(\d{9,12})', url)
    if m:
        return m.group(1)
    m = _re_zoom.search(r'(\d{9,12})', url.replace(' ', ''))
    return m.group(1) if m else ''

def _zoom_get_meeting(meeting_id):
    """Fetch meeting details by numeric ID.
    Returns (kind, data):
      • ('ok', {...})    — meeting exists
      • ('notfound', None) — 404: meeting ID is genuinely invalid / not in account
      • ('error', None)  — transient (timeout / 429 / 5xx): do NOT cache as wrong
    Retries transient errors a couple of times."""
    import urllib.error as _uerr
    tok = _zoom_token()
    if not tok or not meeting_id:
        return ('error', None)
    for attempt in range(3):
        try:
            req = _urllib_req.Request(
                f"https://api.zoom.us/v2/meetings/{meeting_id}",
                headers={'Authorization': f'Bearer {tok}'})
            with _urllib_req.urlopen(req, timeout=15) as resp:
                return ('ok', json.loads(resp.read().decode()))
        except _uerr.HTTPError as he:
            if he.code == 404:
                return ('notfound', None)        # definitive: invalid meeting
            if he.code in (429, 500, 502, 503, 504):
                time.sleep(0.6 * (attempt + 1))  # transient — back off & retry
                continue
            return ('error', None)               # other HTTP error — treat transient
        except Exception:
            time.sleep(0.4 * (attempt + 1))      # timeout / network — retry
            continue
    return ('error', None)

def _zoom_classify_link(url):
    """Classify a Zoom link → ('empty'|'conference'|'correct'|'wrong'|'pending', data).
      • empty      — field blank
      • conference — field is the literal 'Conference Meeting' (no Zoom link expected)
      • wrong      — filled but no parseable meeting ID, or ID doesn't resolve
      • correct    — meeting ID resolves to a real meeting in the account
      • pending    — Zoom not configured yet (creds missing); can't validate
    Results cached per meeting_id."""
    if not url or not str(url).strip():
        return 'empty', None
    low = str(url).strip().lower()
    # SDRs mark conference bookings by typing "Conference Meeting" in this field.
    if 'conference' in low and 'zoom.us' not in low:
        return 'conference', None
    mid = _zoom_extract_meeting_id(url)
    if not mid:
        return 'wrong', None
    if not _zoom_configured():
        return 'pending', None
    with _zoom_meeting_lock:
        if mid in _zoom_meeting_cache:
            c = _zoom_meeting_cache[mid]
            return c['status'], c['data']
    kind, data = _zoom_get_meeting(mid)
    if kind == 'ok':
        status = 'correct'
    elif kind == 'notfound':
        status = 'wrong'
    else:
        # Transient error — classify as wrong for THIS response but DON'T cache,
        # so the next load retries instead of permanently mislabeling a real meeting.
        return 'wrong', None
    with _zoom_meeting_lock:
        _zoom_meeting_cache[mid] = {'status': status, 'data': data}
    return status, data

# ── Zoom outcome helpers (Done / Upcoming / Did-not-happen + AI summary) ──────
import urllib.error as _uerr2

ZOOM_OUTCOMES_FILE = os.path.join(DATA_DIR, 'zoom_outcomes.json')
_zoom_outcomes_lock = threading.Lock()

def _load_outcomes_cache():
    if os.path.exists(ZOOM_OUTCOMES_FILE):
        try:
            with open(ZOOM_OUTCOMES_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_outcomes_cache(cache):
    try:
        with open(ZOOM_OUTCOMES_FILE, 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        print(f'[Zoom] outcomes cache save error: {e}')

def _zoom_get(url, timeout=15):
    """GET a Zoom API/url with the S2S bearer token. Returns parsed JSON or None."""
    tok = _zoom_token()
    if not tok:
        return None
    try:
        req = _urllib_req.Request(url, headers={'Authorization': f'Bearer {tok}'})
        with _urllib_req.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None

def _zoom_past_meeting(meeting_id):
    """GET /past_meetings/{id} — returns dict if the meeting actually occurred, else None."""
    return _zoom_get(f"https://api.zoom.us/v2/past_meetings/{meeting_id}")

def _zoom_recording_assets(meeting_id):
    """Return {'transcript':bool,'summary_url':..,'next_steps_url':..,'duration':int}."""
    d = _zoom_get(f"https://api.zoom.us/v2/meetings/{meeting_id}/recordings")
    out = {'transcript': False, 'summary_url': None, 'next_steps_url': None,
           'duration': (d or {}).get('duration')}
    for f in (d or {}).get('recording_files', []):
        rt = f.get('recording_type')
        if rt == 'audio_transcript':       out['transcript']     = True
        elif rt == 'summary':              out['summary_url']    = f.get('download_url')
        elif rt == 'summary_next_steps':   out['next_steps_url'] = f.get('download_url')
    return out

def _zoom_download(url):
    """Download a Zoom recording file (summary/next_steps) → parsed JSON or {'_text':..}."""
    if not url:
        return None
    tok = _zoom_token()
    try:
        req = _urllib_req.Request(url, headers={'Authorization': f'Bearer {tok}'})
        with _urllib_req.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode('utf-8', 'ignore')
        try:
            return json.loads(raw)
        except Exception:
            return {'_text': raw}
    except Exception:
        return None

def _zoom_participants(meeting_id):
    """GET past-meeting participants. Returns list of names ([] if no scope/none)."""
    d = _zoom_get(f"https://api.zoom.us/v2/past_meetings/{meeting_id}/participants?page_size=300")
    names = []
    for p in (d or {}).get('participants', []):
        nm = p.get('name') or p.get('user_name')
        if nm and nm not in names:
            names.append(nm)
    return names

def _parse_summary(j):
    if not j:
        return ''
    if isinstance(j, dict):
        return (j.get('overall_summary') or j.get('summary') or j.get('_text') or '').strip()
    return str(j).strip()

def _parse_next_steps(j):
    if not j:
        return ''
    if isinstance(j, dict):
        for k in ('next_steps', 'summary_next_steps', 'items'):
            v = j.get(k)
            if isinstance(v, list) and v:
                parts = []
                for it in v:
                    if isinstance(it, str):
                        parts.append(it)
                    elif isinstance(it, dict):
                        parts.append(it.get('text') or it.get('label') or it.get('description') or '')
                return '\n'.join([p for p in parts if p]).strip()
            if isinstance(v, str) and v.strip():
                return v.strip()
        return (j.get('_text') or '').strip()
    return str(j).strip()

def _zoom_meeting_outcome(meeting_id, start_time, cache):
    """Classify a single correct meeting → done / upcoming / didnt_happen,
    fetching AI summary + next steps + participants for completed meetings.
    'done' results are cached permanently; others are recomputed each call."""
    now = datetime.utcnow()
    st  = None
    if start_time:
        try:
            st = datetime.fromisoformat(str(start_time).replace('Z', '').replace('+00:00', ''))
        except Exception:
            st = None

    # Completed meetings never change — serve from cache.
    c = cache.get(meeting_id)
    if c and c.get('outcome') == 'done':
        # Self-heal: backfill participants if they were cached empty (e.g. before
        # the list-participants scope was granted).
        if not c.get('participants'):
            parts = _zoom_participants(meeting_id)
            if parts:
                c['participants'] = parts
                cache[meeting_id] = c
        return c

    # Basic meeting info (topic/host/scheduled) from the in-memory validation cache.
    meeting = None
    with _zoom_meeting_lock:
        mc = _zoom_meeting_cache.get(meeting_id)
        if mc:
            meeting = mc.get('data')
    if meeting is None:
        meeting = _zoom_get_meeting(meeting_id)[1]
    meeting = meeting or {}
    topic = meeting.get('topic') or ''
    host  = meeting.get('host_email') or ''
    sched = meeting.get('start_time') or start_time or ''

    # Upcoming?
    if st and st > now:
        return {'outcome': 'upcoming', 'topic': topic, 'host': host, 'scheduled': sched}

    # Past → did it actually occur?
    pm = _zoom_past_meeting(meeting_id)
    occurred = bool(pm and (pm.get('duration') or 0) >= 1)
    if occurred:
        assets  = _zoom_recording_assets(meeting_id)
        summary = _parse_summary(_zoom_download(assets.get('summary_url')))
        nexts   = _parse_next_steps(_zoom_download(assets.get('next_steps_url')))
        parts   = _zoom_participants(meeting_id)
        result = {
            'outcome':      'done',
            'topic':        pm.get('topic') or topic,
            'host':         host,
            'scheduled':    sched,
            'duration':     pm.get('duration'),
            'transcript':   assets.get('transcript', False),
            'summary':      summary,
            'next_steps':   nexts,
            'participants': parts,
            'fetched_at':   now.isoformat(),
        }
        cache[meeting_id] = result
        return result

    return {'outcome': 'didnt_happen', 'topic': topic, 'host': host, 'scheduled': sched}


@app.route('/api/meeting-outcomes', methods=['POST'])
def api_meeting_outcomes():
    """Classify outcomes for the funnel's 'correct' meetings.
    Body: {"items": [{"lead_id":..,"meeting_id":..,"start_time":..}, ...]}
    Returns {"configured":bool, "outcomes": {lead_id: {...}}}.
    Heavy Zoom calls are cached (completed meetings stored permanently)."""
    if not _zoom_configured():
        return jsonify({'configured': False, 'outcomes': {}})
    body  = request.get_json(silent=True) or {}
    items = body.get('items', [])
    cache = _load_outcomes_cache()
    results = {}
    lock = threading.Lock()

    def _work(it):
        lead_id = it.get('lead_id')
        mid     = it.get('meeting_id')
        st      = it.get('start_time')
        if not mid or not lead_id:
            return
        try:
            res = _zoom_meeting_outcome(mid, st, cache)
        except Exception as e:
            res = {'outcome': 'unknown', 'error': str(e)}
        with lock:
            results[lead_id] = res

    with ThreadPoolExecutor(max_workers=5) as ex:
        list(ex.map(_work, items))
    _save_outcomes_cache(cache)
    return jsonify({'configured': True, 'outcomes': results})


# ── Meeting Generation Funnel — Stage 1: Booked on Nooks ─────────────────────

@app.route('/api/meeting-funnel')
def api_meeting_funnel():
    """Stage 1 of the Meeting Generation Funnel: meetings booked on Nooks.

    Source: SFDC Task records logged by Nooks.
      • Subject starts with '[Nooks Call]'
      • CallDisposition (Call Result) is a booking disposition
      • ActivityDate (Due Date) = the day the meeting was generated
      • Owner.Name (Assigned To) = the SDR who generated the meeting
    Nooks went live 2026-04-15, so only tasks on/after that date are considered.

    Meeting type:
      • 'Meeting Generated- Conference'                       → Conference
      • 'Answered - Booked Meeting' / 'Meeting Generated- Cold' → Cold
    """
    q = (
        "SELECT Id, Subject, CallDisposition, ActivityDate, Owner.Name, WhoId "
        "FROM Task "
        "WHERE Subject LIKE '[Nooks Call]%' "
        "AND CallDisposition IN ('Answered - Booked Meeting',"
        "'Meeting Generated- Cold','Meeting Generated- Conference') "
        "AND ActivityDate >= 2026-04-15 "
        "ORDER BY ActivityDate DESC LIMIT 5000"
    )
    result  = soql(q)
    records = result.get('records', []) if result else []

    # Collect the WhoIds (a Task's Who can be a Lead [00Q] or a Contact [003]).
    lead_ids    = [r.get('WhoId') for r in records if (r.get('WhoId') or '').startswith('00Q')]
    contact_ids = [r.get('WhoId') for r in records if (r.get('WhoId') or '').startswith('003')]

    # ── Enrich Leads (full set of fields) ──
    lead_map = {}
    for i in range(0, len(lead_ids), _BATCH_SIZE):
        batch   = lead_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{lid}'" for lid in batch)
        lq = (f"SELECT Id, FirstName, LastName, Title, Company, "
              f"Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Source__c, "
              f"Zoom_Meeting_Link_URL__c, Seller_Name__c "
              f"FROM Lead WHERE Id IN ({ids_str})")
        res = soql(lq, paginate=False)
        for lr in (res or {}).get('records', []):
            lead_map[lr.get('Id')] = lr

    # ── Enrich Contacts (only the fields a Contact has) ──
    contact_map = {}
    for i in range(0, len(contact_ids), _BATCH_SIZE):
        batch   = contact_ids[i:i + _BATCH_SIZE]
        ids_str = ','.join(f"'{cid}'" for cid in batch)
        cq = (f"SELECT Id, FirstName, LastName, Title, Account.Name "
              f"FROM Contact WHERE Id IN ({ids_str})")
        res = soql(cq, paginate=False)
        for cr in (res or {}).get('records', []):
            contact_map[cr.get('Id')] = cr

    meetings = []
    for r in records:
        disp  = r.get('CallDisposition') or ''
        mtype = 'Conference' if disp == 'Meeting Generated- Conference' else 'Cold'
        owner = r.get('Owner') or {}
        sdr   = owner.get('Name', '') if isinstance(owner, dict) else ''
        who   = r.get('WhoId') or ''

        first = last = title = company = mtg_by = zoom = seller = ''
        mtg_on = mtg_src = ''
        sf_url = ''
        if who in lead_map:
            ld      = lead_map[who]
            first   = ld.get('FirstName') or ''
            last    = ld.get('LastName')  or ''
            title   = ld.get('Title')     or ''
            company = ld.get('Company')   or ''
            mtg_by  = ld.get('Meeting_Generated_by__c')  or ''
            mtg_on  = (ld.get('Meeting_Generated_on__c') or '')[:10]
            mtg_src = ld.get('Meeting_Source__c')        or ''
            zoom    = ld.get('Zoom_Meeting_Link_URL__c') or ''
            seller  = ld.get('Seller_Name__c')           or ''
            sf_url  = f"{SF_BASE_URL}/lightning/r/Lead/{who}/view"
        elif who in contact_map:
            cd      = contact_map[who]
            first   = cd.get('FirstName') or ''
            last    = cd.get('LastName')  or ''
            title   = cd.get('Title')     or ''
            acct    = cd.get('Account') or {}
            company = acct.get('Name', '') if isinstance(acct, dict) else ''
            sf_url  = f"{SF_BASE_URL}/lightning/r/Contact/{who}/view"

        # ── Stage 2: "Updated on SFDC" requires ALL three Lead fields filled ──
        is_updated = bool(mtg_on and mtg_by and mtg_src)

        meetings.append({
            'task_id':       r.get('Id') or '',
            'sdr':           norm_sdr(sdr or '—'),
            'type':          mtype,
            'disposition':   disp,
            'date':          (r.get('ActivityDate') or '')[:10],
            'lead_id':       who,
            'first_name':    first   or '—',
            'last_name':     last    or '—',
            'title':         title   or '—',
            'company':       company or '—',
            'meeting_by':    norm_sdr(mtg_by) if mtg_by else '—',
            'meeting_on':    mtg_on,
            'meeting_source':mtg_src or '—',
            'zoom_url':      zoom,
            'seller':        seller  or '—',
            'is_updated':    is_updated,
            'sf_url':        sf_url,
        })

    # ── Stage 3: classify each Zoom link (correct / wrong / empty) ──
    # Parallelised; per-meeting results are cached so repeat loads are instant.
    def _classify(rec):
        status, data = _zoom_classify_link(rec.get('zoom_url'))
        rec['zoom_status'] = status
        rec['zoom_meeting_id'] = _zoom_extract_meeting_id(rec.get('zoom_url')) if status in ('correct', 'wrong') else ''
        if data:
            rec['zoom_topic']      = data.get('topic')
            rec['zoom_start']      = data.get('start_time')
            rec['zoom_raw_status'] = data.get('status')
            rec['zoom_duration']   = data.get('duration')
            rec['zoom_host']       = data.get('host_email')
    with ThreadPoolExecutor(max_workers=5) as ex:
        list(ex.map(_classify, meetings))

    return jsonify({
        'meetings':        meetings,
        'total':           len(meetings),
        'zoom_configured': _zoom_configured(),
    })


# ── SDR Reporting 2026 – MQL / SQL ───────────────────────────────────────────

@app.route('/api/mql-leads')
def api_mql_leads():
    """Return leads where Status = 'MQL', filtered by Meeting_Generated_on__c."""
    period = request.args.get('period', '30d').strip()
    custom_start = request.args.get('start', '').strip()
    custom_end   = request.args.get('end',   '').strip()

    if custom_start and custom_end:
        start, end = custom_start, custom_end
    else:
        start, end = period_dates(period)

    dt = ''
    if start: dt += f" AND Meeting_Generated_on__c >= {start}"
    if end:   dt += f" AND Meeting_Generated_on__c <= {end}"

    q = (f"SELECT Id, Name, Title, Company, Status, "
         f"Meeting_Generated_by__c, Meeting_Generated_on__c, "
         f"Meeting_Channel__c, Meeting_Type__c, Seller_Name__c, Meeting_Source__c, "
         f"Follow_Up_Owner__c "
         f"FROM Lead "
         f"WHERE Status = 'MQL'"
         f"{dt} "
         f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 2000")

    result = soql(q)
    records = result.get('records', []) if result else []
    leads = []
    for r in records:
        lid = r.get('Id') or ''
        leads.append({
            'id':           lid,
            'name':         r.get('Name') or '—',
            'title':        r.get('Title') or '—',
            'company':      r.get('Company') or '—',
            'generated_by': norm_sdr(r.get('Meeting_Generated_by__c') or '—'),
            'date':         (r.get('Meeting_Generated_on__c') or '')[:10],
            'channel':      r.get('Meeting_Channel__c') or '—',
            'type':         r.get('Meeting_Type__c') or '—',
            'seller':          r.get('Seller_Name__c') or '—',
            'source':          r.get('Meeting_Source__c') or '—',
            'follow_up_owner': norm_sdr(r.get('Follow_Up_Owner__c') or '—'),
            'sf_url':          f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
        })
    return jsonify({'leads': leads, 'total': len(leads)})


@app.route('/api/sql-leads')
def api_sql_leads():
    """Return all leads where SQL_Converted_Date__c > 2026-03-31.
    No current-status filter — leads that progressed beyond SQL still appear.
    Period filter applies to SQL_Converted_Date__c.
    """
    period       = request.args.get('period', 'all').strip()
    custom_start = request.args.get('start', '').strip()
    custom_end   = request.args.get('end',   '').strip()

    if custom_start and custom_end:
        start, end = custom_start, custom_end
    elif period == 'all':
        start, end = '', ''
    else:
        start, end = period_dates(period)

    dt = ''
    if start: dt += f" AND SQL_Converted_Date__c >= {start}"
    if end:   dt += f" AND SQL_Converted_Date__c <= {end}"

    q = (f"SELECT Id, Name, Title, Company, Status, "
         f"SQL_Seller_Owner__c, SQL_Converted_Date__c, SQL_Source__c, "
         f"Follow_Up_Owner__c "
         f"FROM Lead "
         f"WHERE SQL_Converted_Date__c > 2026-03-31"
         f"{dt} "
         f"ORDER BY SQL_Converted_Date__c DESC NULLS LAST LIMIT 2000")

    result = soql(q)
    records = result.get('records', []) if result else []
    leads = []
    for r in records:
        lid = r.get('Id') or ''
        leads.append({
            'id':              lid,
            'name':            r.get('Name')    or '—',
            'title':           r.get('Title')   or '—',
            'company':         r.get('Company') or '—',
            'status':          r.get('Status')  or '—',
            'seller':          norm_sdr(r.get('SQL_Seller_Owner__c') or '—'),
            'date':            (r.get('SQL_Converted_Date__c') or '')[:10],
            'source':          r.get('SQL_Source__c') or '—',
            'follow_up_owner': norm_sdr(r.get('Follow_Up_Owner__c') or '—'),
            'sf_url':          f"{SF_BASE_URL}/lightning/r/Lead/{lid}/view" if lid else '',
        })
    return jsonify({'leads': leads, 'total': len(leads)})


@app.route('/api/reporting-s1')
def api_reporting_s1():
    """Return Opportunities filtered by period (CreatedDate), grouped for SDR Reporting tab.
    Includes LeadSource (Source) field for grouping."""
    period = request.args.get('period', '30d').strip()
    start, end = period_dates(period)

    dt = ''
    if start: dt += f" AND CreatedDate >= {start}T00:00:00Z"
    if end:   dt += f" AND CreatedDate <= {end}T23:59:59Z"

    q = (f"SELECT Id, Name, Amount, StageName, Account.Name, "
         f"SDR_Owner__c, Source__c, CreatedDate "
         f"FROM Opportunity "
         f"WHERE StageName != null"
         f"{dt} "
         f"ORDER BY CreatedDate DESC NULLS LAST LIMIT 1000")

    result  = soql(q)
    records = result.get('records', []) if result else []
    opps    = []
    for r in records:
        oid     = r.get('Id') or ''
        acc     = (r.get('Account') or {})
        sdr_raw = (r.get('SDR_Owner__c') or '').strip()
        opps.append({
            'id':         oid,
            'name':       r.get('Name') or '—',
            'account':    acc.get('Name') or '—',
            'stage':      r.get('StageName') or '—',
            'source':     r.get('Source__c') or '—',
            'amount':     float(r.get('Amount') or 0),
            'sdr':        norm_sdr(sdr_raw) if sdr_raw else '—',
            'deal_type':  'DG Generated' if sdr_raw else 'By Other Sources',
            'date':       (r.get('CreatedDate') or '')[:10],
            'sf_url':     f"{SF_BASE_URL}/lightning/r/Opportunity/{oid}/view" if oid else '',
        })
    return jsonify({'opportunities': opps, 'total': len(opps)})

# ── Slack Weekly Report ───────────────────────────────────────────────────────

SLACK_WEBHOOK     = os.environ.get('SLACK_WEBHOOK', '')
_last_slack_sent  = None   # tracks the Monday date we last sent so we never double-send


def fetch_weekly_sdr_stats():
    """Query Salesforce for last-7-days metrics per SDR.
    Returns dict: {sdr_name: {calls, emails, meetings, meeting_done, sql, s1}}"""
    today  = date.today()
    start  = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end    = today.strftime('%Y-%m-%d')
    camps  = load_campaigns()
    sdr_data = {}

    def ensure(name):
        if name and name not in ('Unknown', '—', ''):
            if name not in sdr_data:
                sdr_data[name] = {'calls': 0, 'emails': 0, 'meetings': 0,
                                  'meeting_done': 0, 'sql': 0, 's1': 0}

    # ── Per-campaign task counts (calls + emails) attributed to camp SDR owner ──
    def fetch_camp_tasks(c):
        owner = norm_sdr((c.get('sdr_owner') or '').strip())
        if not owner:
            return owner, 0, 0
        n = esc(c['name'])
        lead_sub = f"SELECT Id FROM Lead WHERE Campaign__c = '{n}'"
        qc = (f"SELECT COUNT(Id) FROM Task WHERE (Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%') "
              f"AND WhoId IN ({lead_sub}) "
              f"AND ActivityDate >= {start} AND ActivityDate <= {end}")
        qe = (f"SELECT COUNT(Id) FROM Task WHERE "
              f"(Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%') "
              f"AND WhoId IN ({lead_sub}) "
              f"AND ActivityDate >= {start} AND ActivityDate <= {end}")
        with ThreadPoolExecutor(max_workers=2) as ex:
            fc = ex.submit(soql, qc)
            fe = ex.submit(soql, qe)
            calls  = cnt(fc.result())
            emails = cnt(fe.result())
        return owner, calls, emails

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(fetch_camp_tasks, c) for c in camps]
        for f in as_completed(futs):
            owner, calls, emails = f.result()
            if owner:
                ensure(owner)
                sdr_data[owner]['calls']  += calls
                sdr_data[owner]['emails'] += emails

    # ── Direct grouped queries for meeting outcomes ───────────────────────────
    dt_mtg = f"Meeting_Generated_on__c >= {start} AND Meeting_Generated_on__c <= {end}"

    q_mtg  = (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
              f"WHERE {dt_mtg} AND Meeting_Generated_by__c != null "
              f"GROUP BY Meeting_Generated_by__c")
    q_done = (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
              f"WHERE {dt_mtg} AND Meeting_Generated_by__c != null "
              f"AND Meeting_Status__c IN ('Meeting Done-Nurture',"
              f"'Meeting Done- Not Interested','Meeting Done-Unqualified') "
              f"GROUP BY Meeting_Generated_by__c")
    q_sql  = (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
              f"WHERE {dt_mtg} AND Meeting_Generated_by__c != null "
              f"AND Status = 'SQL' "
              f"GROUP BY Meeting_Generated_by__c")
    q_s1   = (f"SELECT SDR_Owner__c, COUNT(Id) FROM Opportunity "
              f"WHERE CreatedDate >= {start}T00:00:00Z "
              f"AND CreatedDate <= {end}T23:59:59Z "
              f"AND SDR_Owner__c != null "
              f"GROUP BY SDR_Owner__c")

    with ThreadPoolExecutor(max_workers=4) as ex:
        fm = ex.submit(soql, q_mtg)
        fd = ex.submit(soql, q_done)
        fs = ex.submit(soql, q_sql)
        f1 = ex.submit(soql, q_s1)
        for rec in (fm.result() or {}).get('records', []):
            name = norm_sdr(rec.get('Meeting_Generated_by__c') or '')
            if name:
                ensure(name)
                sdr_data[name]['meetings'] += int(rec.get('expr0', 0) or 0)
        for rec in (fd.result() or {}).get('records', []):
            name = norm_sdr(rec.get('Meeting_Generated_by__c') or '')
            if name:
                ensure(name)
                sdr_data[name]['meeting_done'] += int(rec.get('expr0', 0) or 0)
        for rec in (fs.result() or {}).get('records', []):
            name = norm_sdr(rec.get('Meeting_Generated_by__c') or '')
            if name:
                ensure(name)
                sdr_data[name]['sql'] += int(rec.get('expr0', 0) or 0)
        for rec in (f1.result() or {}).get('records', []):
            raw  = (rec.get('SDR_Owner__c') or '').strip()
            name = norm_sdr(raw) if raw else ''
            if name:
                ensure(name)
                sdr_data[name]['s1'] += int(rec.get('expr0', 0) or 0)

    return sdr_data


def build_slack_report():
    """Build and return the full Slack report text string."""
    today      = date.today()
    week_start = today - timedelta(days=7)
    date_range = f"{week_start.strftime('%d %b')} – {today.strftime('%d %b %Y')}"

    # ── Campaign status summary ───────────────────────────────────────────────
    camps_cfg   = load_campaigns()
    status_map  = {'Active': 0, 'Completed': 0, 'Paused': 0}
    active_list = []
    for c in camps_cfg:
        st = c.get('status', 'Active')
        status_map[st] = status_map.get(st, 0) + 1
        if st == 'Active':
            eff = effective_end_date(c)
            days_left = (eff - today).days if eff else None
            active_list.append({
                'name':      c['name'],
                'sdr':       norm_sdr((c.get('sdr_owner') or '').strip()) or '—',
                'days_left': days_left,
            })

    total_camps = sum(status_map.values())

    # ── Fetch 7-day SDR stats ─────────────────────────────────────────────────
    sdr_raw = fetch_weekly_sdr_stats()
    # Sort by meetings generated desc
    sdrs = sorted(sdr_raw.items(), key=lambda x: x[1]['meetings'], reverse=True)

    # ── Column-aligned SDR table (monospace) ──────────────────────────────────
    COL_W = [16, 7, 8, 9, 10, 5, 6]   # name, calls, emails, mtg_gen, mtg_done, sql, s1
    HDR   = ['SDR', 'Calls', 'Emails', 'Mtg Gen', 'Mtg Done', 'SQL', 'S1 🏆']

    def row(cells):
        return '  '.join(str(c).ljust(COL_W[i]) for i, c in enumerate(cells))

    sep   = '  '.join('─' * w for w in COL_W)
    lines = [row(HDR), sep]
    tot   = [0, 0, 0, 0, 0, 0]
    for name, s in sdrs:
        vals = [s['calls'], s['emails'], s['meetings'],
                s['meeting_done'], s['sql'], s['s1']]
        lines.append(row([name] + vals))
        for i, v in enumerate(vals):
            tot[i] += v
    lines.append(sep)
    lines.append(row(['TOTAL'] + tot))
    sdr_table = '\n'.join(lines)

    # ── Top 3 performers ─────────────────────────────────────────────────────
    medals = ['🥇', '🥈', '🥉']
    top3_lines = []
    for i, (name, s) in enumerate(sdrs[:3]):
        parts = [f"{s['meetings']} Mtg Gen"]
        if s['meeting_done']: parts.append(f"{s['meeting_done']} Done")
        if s['sql']:          parts.append(f"{s['sql']} SQL")
        if s['s1']:           parts.append(f"{s['s1']} S1 🏆")
        top3_lines.append(f"{medals[i]} *{name}* — {', '.join(parts)}")

    # ── Active campaigns table ────────────────────────────────────────────────
    active_list.sort(key=lambda x: (x['days_left'] if x['days_left'] is not None else 999))
    camp_lines = []
    for ac in active_list[:10]:
        dl = ac['days_left']
        if dl is None:
            flag = ''
        elif dl <= 2:
            flag = ' 🔴'
        elif dl <= 4:
            flag = ' ⚠️'
        else:
            flag = ' 🟢'
        dl_str = f"{dl}d left{flag}" if dl is not None else '—'
        name_short = ac['name'][:35] + ('…' if len(ac['name']) > 35 else '')
        camp_lines.append(f"  • {name_short}  |  {ac['sdr']}  |  {dl_str}")

    # ── Completed last week ───────────────────────────────────────────────────
    # Build a lookup of cache stats by campaign name for quick access
    cache_by_name = {c['name']: c for c in (cache.get('campaigns') or [])}

    completed_last_week = []
    for c in camps_cfg:
        eff = effective_end_date(c)
        if eff is None:
            continue
        # Campaign ended within the last 7 days
        if week_start <= eff <= today:
            cached = cache_by_name.get(c['name'], {})
            sdr    = norm_sdr((c.get('sdr_owner') or '').strip()) or '—'
            start  = c.get('start_date') or '—'
            end    = c.get('end_date') or eff.strftime('%Y-%m-%d')
            completed_last_week.append({
                'name':     c['name'],
                'sdr':      sdr,
                'start':    start,
                'end':      end,
                'calls':    cached.get('total_calls',  0),
                'emails':   cached.get('total_emails', 0),
                'meetings': cached.get('meetings',     0),
            })

    completed_last_week.sort(key=lambda x: x['end'], reverse=True)

    completed_lines = []
    for cc in completed_last_week:
        name_short = cc['name'][:32] + ('…' if len(cc['name']) > 32 else '')
        completed_lines.append(
            f"  • *{name_short}*\n"
            f"    👤 {cc['sdr']}  |  📅 {cc['start']} → {cc['end']}\n"
            f"    📞 {cc['calls']} calls  ✉️  {cc['emails']} emails  🤝 {cc['meetings']} meetings"
        )

    # ── Assemble message ──────────────────────────────────────────────────────
    bar = '━' * 46
    completed_section = f"""
✅ *CAMPAIGNS COMPLETED LAST WEEK ({len(completed_last_week)})*
{'━' * 30}
{chr(10).join(completed_lines) if completed_lines else '  No campaigns completed last week.'}
""" if True else ''

    msg = f"""{bar}
📊 *SDR Weekly Report — RapidClaims*
🗓 Week: {date_range}  _(Last 7 Days)_
{bar}

📋 *CAMPAIGN STATUS SUMMARY*
{'━' * 30}
🟢 Active       →  {status_map.get('Active', 0):>3} campaigns
🔵 Completed    →  {status_map.get('Completed', 0):>3} campaigns
🟡 Paused       →  {status_map.get('Paused', 0):>3} campaigns
📦 Total        →  {total_camps:>3} campaigns

👥 *SDR PERFORMANCE — Last 7 Days*
{'━' * 30}
```
{sdr_table}
```

🏆 *TOP PERFORMERS THIS WEEK*
{'━' * 30}
{chr(10).join(top3_lines) if top3_lines else '  No data yet.'}

📋 *ACTIVE CAMPAIGNS ({status_map.get('Active', 0)})*
{'━' * 30}
{chr(10).join(camp_lines) if camp_lines else '  No active campaigns.'}
{completed_section}
{bar}
_Sent every Monday at 5:00 PM IST  •  RapidClaims DG Team_"""

    return msg


def send_slack_report():
    """Build report and POST it to the Slack webhook."""
    global _last_slack_sent
    try:
        print('[slack] building weekly report…')
        msg     = build_slack_report()
        payload = json.dumps({'text': msg}).encode('utf-8')
        req     = _urllib_req.Request(
            SLACK_WEBHOOK,
            data    = payload,
            headers = {'Content-Type': 'application/json'},
            method  = 'POST'
        )
        with _urllib_req.urlopen(req, timeout=15) as resp:
            status = resp.getcode()
        _last_slack_sent = date.today()
        print(f'[slack] report sent — HTTP {status}')
    except Exception as e:
        print(f'[slack] ERROR sending report: {e}')


def slack_scheduler_loop():
    """Background thread: send Slack report every Monday at 17:00 IST (11:30 UTC)."""
    global _last_slack_sent
    while True:
        now_utc = datetime.utcnow()
        # Monday = weekday 0,  11:30 UTC = 17:00 IST
        if (now_utc.weekday() == 0
                and now_utc.hour == 11
                and now_utc.minute >= 30
                and now_utc.minute < 35
                and _last_slack_sent != date.today()):
            send_slack_report()
        time.sleep(60)   # check every minute


# ── Boot ──────────────────────────────────────────────────────────────────────

# ── Startup initialisation (runs under both gunicorn and direct python) ───────
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE) as f:
            saved = json.load(f)
        cache['campaigns'] = saved.get('campaigns', [])
        cache['sdr_stats'] = saved.get('sdr_stats', [])
        cache['totals']    = saved.get('totals', {'meeting_done':0,'meeting_noshow':0,'sql_gen':0,'s1':0})
        cache['last_sync'] = saved.get('last_sync')
        print('[cache] loaded from disk')
    except Exception as e:
        print(f'[cache] load error: {e}')

setup_sf_auth()
auto_complete_campaigns()

threading.Thread(target=bg_loop, daemon=True).start()
threading.Thread(target=slack_scheduler_loop, daemon=True).start()

print('\n' + '='*55)
print('  🚀  Campaign Command Center')
print('  →   http://localhost:5001')
print('  ↺   Auto-syncs daily at 05:00 IST from Salesforce')
print('='*55 + '\n')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False)
