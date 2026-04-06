from flask import Flask, jsonify, request, render_template
import subprocess, json, os, threading, time
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

app = Flask(__name__)

BASE       = os.path.dirname(os.path.abspath(__file__))
CAMPS_FILE = os.path.join(BASE, 'campaigns.json')
CACHE_FILE = os.path.join(BASE, 'data_cache.json')
SF_ORG      = 'vaibhavkumar@rapidclaims.ai'
SF_BASE_URL = 'https://data-page-6243.my.salesforce.com'
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

# ── Salesforce helpers ────────────────────────────────────────────────────────

def soql(query):
    """Run a SOQL query via sf CLI and return the result dict, or None on error."""
    try:
        env = os.environ.copy()
        env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:' + env.get('PATH', '')
        r = subprocess.run(
            ['sf', 'data', 'query', '--query', query, '--json', '--target-org', SF_ORG],
            capture_output=True, text=True, timeout=60, env=env
        )
        d = json.loads(r.stdout)
        if d.get('status') == 0:
            return d.get('result')
        print(f'[SOQL] error: {d.get("message","unknown")}')
        return None
    except Exception as e:
        print(f'[SOQL] exception: {e}')
        return None

# ── SDR extraction ────────────────────────────────────────────────────────────
# ── SFDC name → preferred display name ───────────────────────────────────────
# Exact values returned by Meeting_Generated_by__c / SDR_Owner__c on SFDC
SFDC_NAME_MAP = {
    'Akil Krishna':          'Akil Krishna',
    'Anurup Bhattacharjee':  'Anurup',
    'Ananya Rao':            'Ananya',
    'Anushka HB':            'Anushka',
    'Deborah':               'Deborah',
    'Felix':                 'Felix',
    'Isaac Bartels':         'Isaac',
    'Hreeman Saha':          'Hreeman',
    'Michelle B':            'Michelle',
    'Samridhi Dutta':        'Samrudhi',
    'Shahana Abbasi':        'Shahana',
    'Rithick S':             'Rithick',
    'Sukhneet':              'Sukhneet',
    'Akhilesh':              'Akhilesh',
    'Saka':                  'Saka',
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
    'anurup':    'Anurup',
    'samridhi':  'Samrudhi',
    'michelle':  'Michelle',
    'issac':     'Isaac',
    'isaac':     'Isaac',
    'sukhneet':  'Sukhneet',
    'anushka':   'Anushka',
    'deborah':   'Deborah',
    'felix':     'Felix',
    'hreeman':   'Hreeman',
    'akil':      'Akil Krishna',
    'shahana':   'Shahana',
    'rithick':   'Rithick',
    'rithik':    'Rithick',
    'ananya':    'Ananya',
    'hursh':     'Hursh',
    'sheetal':   'Sheetal',
    'dushyant':  'Dushyant',
    'matt':      'Matt',
    'abhishek':  'Abhishek',
    'neil':      'Neil',
    'akhilesh':  'Akhilesh',
    'saka':      'Saka',
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
    queries = {
        'leads':    f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}'",
        'calls':    f"SELECT COUNT(Id) FROM Task WHERE Subject LIKE '%Orum%' AND WhoId IN ({lead}){dt_task}",
        'emails':   f"SELECT COUNT(Id) FROM Task WHERE (Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%') AND WhoId IN ({lead}){dt_task}",
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
                           f"AND Meeting_Status__c = 'Meeting Done-SQL'{dt_mtg}"),
        'status_sdr':     (f"SELECT Meeting_Generated_by__c, Meeting_Status__c, COUNT(Id) FROM Lead "
                           f"WHERE Campaign__c = '{n}' "
                           f"AND Meeting_Status__c IN ('Meeting Done-SQL', 'Meeting Done-Nurture', "
                           f"'Meeting Done- Not Interested', 'Meeting Done-Unqualified', 'Meeting No Show'){dt_mtg} "
                           f"AND Meeting_Generated_by__c != null "
                           f"GROUP BY Meeting_Generated_by__c, Meeting_Status__c"),
    }
    # Only query Salesforce for S1 if no manual override
    if not has_manual_s1:
        queries['s1'] = (f"SELECT COUNT(Id) FROM Opportunity "
                         f"WHERE Id IN (SELECT ConvertedOpportunityId FROM Lead "
                         f"WHERE Campaign__c = '{n}' AND IsConverted = true) "
                         f"AND StageName = 'S1 - Need Identified'")

    # Run all sub-queries in parallel
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
            if status == 'Meeting Done-SQL':
                status_sdr_bk[sdr_name]['sql_gen'] += count
            elif status in ('Meeting Done-Nurture', 'Meeting Done- Not Interested', 'Meeting Done-Unqualified'):
                status_sdr_bk[sdr_name]['meeting_done'] += count
            elif status == 'Meeting No Show':
                status_sdr_bk[sdr_name]['meeting_noshow'] += count

    return {
        **c,
        'total_leads':        cnt(results.get('leads')),
        'total_calls':        cnt(results.get('calls')),
        'total_emails':       cnt(results.get('emails')),
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

    # Pass 2 – assign meetings from Meeting_Generated_by__c breakdown
    for c in enriched:
        for b in c.get('sdr_breakdown', []):
            name = b['name']
            ensure(name)
            sdr[name]['meetings'] += b['meetings']
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

    camps = load_campaigns()
    if not camps:
        cache['is_syncing'] = False
        return

    results    = []
    total      = len(camps)
    done_n     = [0]

    # Process campaigns 3 at a time (each campaign already uses 6 threads internally)
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
                                'sql_gen': 0, 'status_sdr_breakdown': []})
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
    while True:
        sync()
        time.sleep(300)   # 5-minute refresh

# ── Campaign config CRUD ──────────────────────────────────────────────────────

def load_campaigns():
    if os.path.exists(CAMPS_FILE):
        with open(CAMPS_FILE) as f:
            return json.load(f)
    return []

def save_campaigns(data):
    with open(CAMPS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

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
        'sql':    "Meeting_Status__c = 'Meeting Done-SQL'",
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
              f"AND Meeting_Status__c = 'Meeting Done-SQL' "
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
    ?period=7d|30d|qtd
    Runs live Salesforce queries; ignores the in-memory cache."""
    period = request.args.get('period', '').strip().lower()
    start, end = period_dates(period)
    if not start:
        return jsonify({'error': 'Invalid period. Use 7d, 30d, or qtd.'}), 400

    camps = load_campaigns()
    if not camps:
        return jsonify({'campaigns': [], 'sdr_stats': [], 'period': period,
                        'start_date': start, 'end_date': end})

    results  = []
    done_n   = [0]
    total    = len(camps)

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
                                'sql_gen': 0, 'status_sdr_breakdown': []})
            done_n[0] += 1

    # Restore original campaign order
    order = {c['id']: i for i, c in enumerate(camps)}
    results.sort(key=lambda x: order.get(x['id'], 999))

    # Filter to campaigns with any activity in the period
    active = [r for r in results
              if r.get('total_calls', 0) + r.get('total_emails', 0) + r.get('meetings', 0) > 0]

    sdr_opp_stats = fetch_sdr_opp_stats(start_date=start, end_date=end)
    sdr_stats     = build_sdr_stats(active, sdr_opp_stats)

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

    # Background sync thread
    threading.Thread(target=bg_loop, daemon=True).start()

    print('\n' + '='*55)
    print('  🚀  Campaign Command Center')
    print('  →   http://localhost:5001')
    print('  ↺   Auto-syncs every 10 minutes from Salesforce')
    print('='*55 + '\n')
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=False, host='0.0.0.0', port=port, use_reloader=False)
