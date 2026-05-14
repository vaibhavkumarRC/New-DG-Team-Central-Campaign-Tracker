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
    'last_sync': None, 'is_syncing': False,
    'sync_progress': 0, 'errors': []
}

# ── Period data cache (7d / 30d / qtd) ───────────────────────────────────────
PERIOD_CACHE_TTL = 600   # 10 minutes
period_cache = {}        # key: period → {'data': {...}, 'fetched_at': datetime}

# ── Salesforce helpers ────────────────────────────────────────────────────────

# ── Salesforce REST API token cache ──────────────────────────────────────────
_sf_token_cache = {'token': None, 'instance_url': None, 'fetched_at': None}
_sf_token_lock  = threading.Lock()

def _refresh_sf_token():
    """Call sf org display once to get access token + instance URL."""
    try:
        env = os.environ.copy()
        env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:' + env.get('PATH', '')
        r = subprocess.run(
            ['sf', 'org', 'display', '--target-org', SF_ORG, '--json'],
            capture_output=True, text=True, timeout=30, env=env
        )
        d = json.loads(r.stdout)
        if d.get('status') == 0:
            res = d.get('result', {})
            return res.get('accessToken'), res.get('instanceUrl', SF_BASE_URL)
        print(f'[SF-auth] error: {d.get("message")}')
    except Exception as e:
        print(f'[SF-auth] exception: {e}')
    return None, SF_BASE_URL

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

def soql(query, retries=2):
    """Run a SOQL query via Salesforce REST API — fast, no sf CLI subprocess."""
    for attempt in range(retries):
        token, instance_url = _get_sf_token()
        if not token:
            print('[SOQL] No access token available')
            return None
        try:
            url = f"{instance_url}/services/data/v59.0/query?q={urllib.parse.quote(query)}"
            req = _urllib_req.Request(url, headers={'Authorization': f'Bearer {token}'})
            with _urllib_req.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                return {'records': data.get('records', []), 'totalSize': data.get('totalSize', 0)}
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

def period_dates(period):
    """Return (start_str, end_str) as YYYY-MM-DD for SOQL date filters."""
    today = date.today()
    if period == '7d':
        start = today - timedelta(days=7)
    elif period == '30d':
        start = today - timedelta(days=30)
    elif period == 'qtd':
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        start = date(today.year, q_start_month, 1)
    else:
        return None, None
    return start.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

# ── Per-campaign metrics ──────────────────────────────────────────────────────

def campaign_metrics(c, start_override=None, end_override=None):
    n     = esc(c['name'])
    start = start_override if start_override is not None else (c.get('start_date') or '').strip()
    end   = end_override   if end_override   is not None else (c.get('end_date')   or '').strip()
    lead  = f"SELECT Id FROM Lead WHERE Campaign__c = '{n}'"

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

    # Fetch leads count first — if 0, skip all other queries to save resources
    results['leads'] = soql(f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}'")
    total_leads_check = cnt(results['leads'])

    if total_leads_check > 0:
        queries = {
            'calls':          f"SELECT COUNT(Id) FROM Task WHERE (Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%') AND WhoId IN ({lead}){dt_task}",
            'emails':         f"SELECT COUNT(Id) FROM Task WHERE (Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%') AND WhoId IN ({lead}){dt_task}",
            'unique_called':  f"SELECT COUNT_DISTINCT(WhoId) FROM Task WHERE (Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%') AND WhoId IN ({lead}){dt_task}",
            'unique_emailed': f"SELECT COUNT_DISTINCT(WhoId) FROM Task WHERE (Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%') AND WhoId IN ({lead}){dt_task}",
            'meetings': f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}' AND Meeting_Generated_on__c != null{dt_mtg}",
            'sdr':      (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
                         f"WHERE Campaign__c = '{n}' AND Meeting_Generated_on__c != null{dt_mtg} "
                         f"AND Meeting_Generated_by__c != null "
                         f"GROUP BY Meeting_Generated_by__c ORDER BY COUNT(Id) DESC LIMIT 20"),
            'meeting_done':   (f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}' "
                               f"AND Meeting_Status__c IN ('Meeting Done-Nurture', 'Meeting Done- Not Interested', 'Meeting Done-Unqualified'){dt_mtg}"),
            'meeting_noshow': (f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}' "
                               f"AND Meeting_Status__c = 'Meeting No Show'{dt_mtg}"),
            'sql_gen':        (f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}' "
                               f"AND Status = 'SQL'{dt_mtg}"),
            'status_sdr':     (f"SELECT Meeting_Generated_by__c, Meeting_Status__c, COUNT(Id) FROM Lead "
                               f"WHERE Campaign__c = '{n}' "
                               f"AND Meeting_Status__c IN ('Meeting Done-Nurture', "
                               f"'Meeting Done- Not Interested', 'Meeting Done-Unqualified', 'Meeting No Show'){dt_mtg} "
                               f"AND Meeting_Generated_by__c != null "
                               f"GROUP BY Meeting_Generated_by__c, Meeting_Status__c"),
            'sql_sdr':        (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
                               f"WHERE Campaign__c = '{n}' "
                               f"AND Status = 'SQL' "
                               f"AND Meeting_Generated_by__c != null "
                               f"GROUP BY Meeting_Generated_by__c"),
        }
        if not has_manual_s1:
            queries['s1'] = (f"SELECT COUNT(Id) FROM Opportunity "
                             f"WHERE Id IN (SELECT ConvertedOpportunityId FROM Lead "
                             f"WHERE Campaign__c = '{n}' AND IsConverted = true)")

        # Run remaining queries in parallel — REST API calls are lightweight
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(soql, q): k for k, q in queries.items()}
            for f in as_completed(futs):
                results[futs[f]] = f.result()

    sdr_bk = []
    if results.get('sdr') and results['sdr'].get('records'):
        for rec in results['sdr']['records']:
            sdr_bk.append({
                'name': norm_sdr(rec.get('Meeting_Generated_by__c') or 'Unknown'),
                'meetings': int(rec.get('expr0', 0) or 0)
            })

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

    # Parse per-SDR SQL counts (Status = 'SQL')
    if results.get('sql_sdr') and results['sql_sdr'].get('records'):
        for rec in results['sql_sdr']['records']:
            sdr_name = norm_sdr(rec.get('Meeting_Generated_by__c') or 'Unknown')
            count    = int(rec.get('expr0', 0) or 0)
            if sdr_name not in status_sdr_bk:
                status_sdr_bk[sdr_name] = {'meeting_done': 0, 'meeting_noshow': 0, 'sql_gen': 0}
            status_sdr_bk[sdr_name]['sql_gen'] += count

    total_leads         = cnt(results.get('leads'))
    total_calls         = cnt(results.get('calls'))
    total_emails        = cnt(results.get('emails'))
    unique_leads_called  = cnt(results.get('unique_called'))
    unique_leads_emailed = cnt(results.get('unique_emailed'))

    calls_per_called_lead  = round(total_calls  / unique_leads_called,  1) if unique_leads_called  > 0 else 0
    emails_per_emailed_lead= round(total_emails / unique_leads_emailed, 1) if unique_leads_emailed > 0 else 0

    return {
        **c,
        'total_leads':              total_leads,
        'total_calls':              total_calls,
        'total_emails':             total_emails,
        'unique_leads_called':      unique_leads_called,
        'unique_leads_emailed':     unique_leads_emailed,
        'calls_per_called_lead':    calls_per_called_lead,
        'emails_per_emailed_lead':  emails_per_emailed_lead,
        'meetings':           cnt(results.get('meetings')),
        's1_created':         int(manual_s1) if has_manual_s1 else cnt(results.get('s1')),
        's1_is_manual':       has_manual_s1,
        'meeting_done':       cnt(results.get('meeting_done')),
        'meeting_noshow':     cnt(results.get('meeting_noshow')),
        'sql_gen':            cnt(results.get('sql_gen')),
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
    if cache['is_syncing']:
        return
    cache['is_syncing']     = True
    cache['sync_progress']  = 0
    cache['errors']         = []

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
                cache['errors'].append(f"{c['name']}: {e}")
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

    cache['campaigns']     = results
    cache['sdr_stats']     = build_sdr_stats(results, sdr_opp_stats)
    cache['last_sync']     = datetime.now().isoformat()
    cache['is_syncing']    = False
    cache['sync_progress'] = 100

    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump({'campaigns': results, 'sdr_stats': cache['sdr_stats'],
                       'last_sync': cache['last_sync']}, f)
    except Exception as e:
        print(f'[cache] save error: {e}')

def bg_loop():
    """Auto-sync once daily at 05:00 IST (23:30 UTC)."""
    while True:
        now_utc = datetime.utcnow()
        # 05:00 IST = 23:30 UTC previous day
        target = now_utc.replace(hour=23, minute=30, second=0, microsecond=0)
        if now_utc >= target:
            target = target.replace(day=target.day + 1)
        seconds_until = (target - now_utc).total_seconds()
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
        'start_date':  d.get('start_date', ''),
        'end_date':    d.get('end_date', ''),
    }
    camps.append(c)
    save_campaigns(camps)
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
    """Return leads where Meeting_Generated_on__c is not null. Optional ?campaign= filter.
    When filtering by campaign, also applies that campaign's start/end date range."""
    camp = request.args.get('campaign', '').strip()
    if camp:
        # Look up the campaign's date range from campaigns.json
        camps_cfg = load_campaigns()
        camp_cfg  = next((c for c in camps_cfg if c['name'] == camp), {})
        start = (camp_cfg.get('start_date') or '').strip()
        end   = (camp_cfg.get('end_date')   or '').strip()
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
        # "All SDRs" view — fetch per campaign so each date range is respected,
        # then merge. This ensures the modal count matches the KPI card exactly.
        camps_cfg = load_campaigns()
        all_records = []
        for camp_cfg in camps_cfg:
            cname = camp_cfg.get('name', '').strip()
            if not cname:
                continue
            start = (camp_cfg.get('start_date') or '').strip()
            end   = (camp_cfg.get('end_date')   or '').strip()
            dt = ''
            if start: dt += f" AND Meeting_Generated_on__c >= {start}"
            if end:   dt += f" AND Meeting_Generated_on__c <= {end}"
            cq = (f"SELECT Id, Name, Title, Company, Campaign__c, "
                  f"Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Source__c "
                  f"FROM Lead "
                  f"WHERE Campaign__c = '{esc(cname)}' "
                  f"AND Meeting_Generated_on__c != null{dt} "
                  f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 500")
            res = soql(cq)
            if res and res.get('records'):
                all_records.extend(res['records'])

        # Sort combined results by date descending
        all_records.sort(
            key=lambda r: r.get('Meeting_Generated_on__c') or '',
            reverse=True
        )

        leads = []
        for r in all_records:
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
    """Return leads filtered by Meeting_Status__c. ?status=done|noshow|sql [&sdr=DisplayName]"""
    status = request.args.get('status', '').strip()
    sdr    = request.args.get('sdr',    '').strip()

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

    camps_cfg   = load_campaigns()
    all_records = []
    for camp_cfg in camps_cfg:
        cname = camp_cfg.get('name', '').strip()
        if not cname:
            continue
        start = (camp_cfg.get('start_date') or '').strip()
        end   = (camp_cfg.get('end_date')   or '').strip()
        dt = ''
        if start: dt += f" AND Meeting_Generated_on__c >= {start}"
        if end:   dt += f" AND Meeting_Generated_on__c <= {end}"
        q = (f"SELECT Id, Name, Title, Company, Campaign__c, "
             f"Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Status__c "
             f"FROM Lead "
             f"WHERE Campaign__c = '{esc(cname)}' "
             f"AND {status_clause}{sdr_clause}{dt} "
             f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 500")
        res = soql(q)
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
    """Return campaign metrics filtered to a time period.
    ?period=7d|30d|qtd  — results cached for PERIOD_CACHE_TTL seconds."""
    period = request.args.get('period', '').strip().lower()
    refresh = request.args.get('refresh', '').strip().lower() == '1'
    start, end = period_dates(period)
    if not start:
        return jsonify({'error': 'Invalid period. Use 7d, 30d, or qtd.'}), 400

    # ── Serve from cache if fresh ────────────────────────────────────────────
    cached = period_cache.get(period)
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

    results = []

    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(campaign_metrics, c, start, end): c for c in camps}
        for f in as_completed(futs):
            try:
                results.append(f.result())
            except Exception as e:
                c = futs[f]
                results.append({**c, 'total_leads': 0, 'total_calls': 0,
                                'total_emails': 0, 'meetings': 0,
                                's1_created': 0, 'sdr_breakdown': [],
                                'meeting_done': 0, 'meeting_noshow': 0,
                                'sql_gen': 0, 'status_sdr_breakdown': [],
                                'unique_leads_called': 0, 'unique_leads_emailed': 0,
                                'calls_per_called_lead': 0, 'emails_per_emailed_lead': 0})

    # Restore original campaign order
    order = {c['id']: i for i, c in enumerate(camps)}
    results.sort(key=lambda x: order.get(x['id'], 999))

    # Filter to campaigns with any activity in the period
    active = [r for r in results
              if r.get('total_calls', 0) + r.get('total_emails', 0) + r.get('meetings', 0) > 0]

    sdr_opp_stats = fetch_sdr_opp_stats(start_date=start, end_date=end)
    sdr_stats     = build_sdr_stats(active, sdr_opp_stats)

    payload = {
        'campaigns':  active,
        'sdr_stats':  sdr_stats,
        'period':     period,
        'start_date': start,
        'end_date':   end,
    }

    # ── Store in cache ───────────────────────────────────────────────────────
    period_cache[period] = {'data': payload, 'fetched_at': datetime.now()}

    return jsonify({
        'campaigns':  active,
        'sdr_stats':  sdr_stats,
        'period':     period,
        'start_date': start,
        'end_date':   end,
    })


@app.route('/api/s1-opportunities')
def api_s1_opportunities():
    """Return Opportunities created after NPV_START_DATE where SDR_Owner__c is not null.
    Optional ?sdr=DisplayName to filter to a single SDR."""
    sdr_display = request.args.get('sdr', '').strip()
    sdr_clause  = ''
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
    """Return leads where Status = 'SQL', filtered by SQL_Converted_Date__c."""
    period = request.args.get('period', '30d').strip()
    custom_start = request.args.get('start', '').strip()
    custom_end   = request.args.get('end',   '').strip()

    if custom_start and custom_end:
        start, end = custom_start, custom_end
    else:
        start, end = period_dates(period)

    dt = ''
    if start: dt += f" AND SQL_Converted_Date__c >= {start}"
    if end:   dt += f" AND SQL_Converted_Date__c <= {end}"

    q = (f"SELECT Id, Name, Title, Company, Status, "
         f"SQL_Seller_Owner__c, SQL_Converted_Date__c, SQL_Source__c, "
         f"Follow_Up_Owner__c "
         f"FROM Lead "
         f"WHERE Status = 'SQL'"
         f"{dt} "
         f"ORDER BY SQL_Converted_Date__c DESC NULLS LAST LIMIT 2000")

    result = soql(q)
    records = result.get('records', []) if result else []
    leads = []
    for r in records:
        lid = r.get('Id') or ''
        leads.append({
            'id':      lid,
            'name':    r.get('Name') or '—',
            'title':   r.get('Title') or '—',
            'company': r.get('Company') or '—',
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

if __name__ == '__main__':
    # Restore from disk cache so the UI shows data immediately on restart
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                saved = json.load(f)
            cache['campaigns'] = saved.get('campaigns', [])
            cache['sdr_stats'] = saved.get('sdr_stats', [])
            cache['last_sync'] = saved.get('last_sync')
            print('[cache] loaded from disk')
        except Exception as e:
            print(f'[cache] load error: {e}')

    # Authenticate Salesforce CLI (JWT on Railway, skipped on local dev)
    setup_sf_auth()

    # Auto-complete any campaigns that have already passed their end date
    auto_complete_campaigns()

    # Background sync thread
    threading.Thread(target=bg_loop, daemon=True).start()

    # Slack weekly report scheduler (fires every Monday 5 PM IST)
    threading.Thread(target=slack_scheduler_loop, daemon=True).start()

    print('\n' + '='*55)
    print('  🚀  Campaign Command Center')
    print('  →   http://localhost:5001')
    print('  ↺   Auto-syncs daily at 05:00 IST from Salesforce')
    print('='*55 + '\n')
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False)
