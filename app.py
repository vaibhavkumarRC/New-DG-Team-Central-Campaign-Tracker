from flask import Flask, jsonify, request, render_template
import subprocess, json, os, threading, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

BASE       = os.path.dirname(os.path.abspath(__file__))
CAMPS_FILE = os.path.join(BASE, 'campaigns.json')
CACHE_FILE = os.path.join(BASE, 'data_cache.json')
SF_ORG     = 'vaibhavkumar@rapidclaims.ai'

cache = {
    'campaigns': [], 'sdr_stats': [],
    'last_sync': None, 'is_syncing': False,
    'sync_progress': 0, 'errors': []
}

# ── Salesforce helpers ────────────────────────────────────────────────────────

def soql(query):
    """Run a SOQL query via sf CLI and return the result dict, or None on error."""
    try:
        r = subprocess.run(
            ['sf', 'data', 'query', '--query', query, '--json', '--target-org', SF_ORG],
            capture_output=True, text=True, timeout=60
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
# Campaign naming convention: "Campaign Name_SDR Name" or "Campaign Name_SDR Name_Date"
KNOWN_SDRS = {
    'anurup':    'Anurup',
    'samridhi':  'Samridhi Dutta',
    'michelle':  'Michelle B',
    'issac':     'Isaac Bartels',
    'isaac':     'Isaac Bartels',
    'sukhneet':  'Sukhneet',
    'anushka':   'Anushka HB',
    'deborah':   'Deborah',
    'felix':     'Felix',
    'hreeman':   'Hreeman Saha',
    'akil':      'Akil Krishna',
    'shahana':   'Shahana',
    'rithick':   'Rithick',
    'rithik':    'Rithick',
    'ananya':    'Ananya',
    'hursh':     'Hursh',
    'sheetal':   'Sheetal',
    'dushyant':  'Dushyant Mishra',
    'matt':      'Matt Bates',
    'abhishek':  'Abhishek Tripathi',
    'neil':      'Neil Sarkar',
    'anurup':    'Anurup',
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

# ── Per-campaign metrics ──────────────────────────────────────────────────────

def campaign_metrics(c):
    n     = esc(c['name'])
    start = (c.get('start_date') or '').strip()
    end   = (c.get('end_date')   or '').strip()
    lead  = f"SELECT Id FROM Lead WHERE Campaign__c = '{n}'"

    # Build optional date filters
    dt_task = ''
    if start: dt_task += f" AND ActivityDate >= {start}"
    if end:   dt_task += f" AND ActivityDate <= {end}"

    dt_mtg = ''
    if start: dt_mtg += f" AND Meeting_Generated_on__c >= {start}"
    if end:   dt_mtg += f" AND Meeting_Generated_on__c <= {end}"

    results = {}
    queries = {
        'leads':    f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}'",
        'calls':    f"SELECT COUNT(Id) FROM Task WHERE Subject LIKE '%Orum%' AND WhoId IN ({lead}){dt_task}",
        'emails':   f"SELECT COUNT(Id) FROM Task WHERE (Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%') AND WhoId IN ({lead}){dt_task}",
        'meetings': f"SELECT COUNT(Id) FROM Lead WHERE Campaign__c = '{n}' AND Meeting_Status__c = 'Meeting Scheduled'{dt_mtg}",
        's1':       (f"SELECT COUNT(Id) FROM Opportunity "
                     f"WHERE Id IN (SELECT ConvertedOpportunityId FROM Lead "
                     f"WHERE Campaign__c = '{n}' AND IsConverted = true) "
                     f"AND StageName = 'S1 - Need Identified'"),
        'sdr':      (f"SELECT Meeting_Generated_by__c, COUNT(Id) FROM Lead "
                     f"WHERE Campaign__c = '{n}' AND Meeting_Status__c = 'Meeting Scheduled'{dt_mtg} "
                     f"AND Meeting_Generated_by__c != null "
                     f"GROUP BY Meeting_Generated_by__c ORDER BY COUNT(Id) DESC LIMIT 20"),
    }

    # Run all sub-queries in parallel (each campaign gets 6 concurrent calls)
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(soql, q): k for k, q in queries.items()}
        for f in as_completed(futs):
            results[futs[f]] = f.result()

    sdr_bk = []
    if results.get('sdr') and results['sdr'].get('records'):
        for rec in results['sdr']['records']:
            sdr_bk.append({
                'name': rec.get('Meeting_Generated_by__c') or 'Unknown',
                'meetings': int(rec.get('expr0', 0) or 0)
            })

    return {
        **c,
        'total_leads':   cnt(results.get('leads')),
        'total_calls':   cnt(results.get('calls')),
        'total_emails':  cnt(results.get('emails')),
        'meetings':      cnt(results.get('meetings')),
        's1_created':    cnt(results.get('s1')),
        'sdr_breakdown': sdr_bk,
        'synced_at':     datetime.now().isoformat()
    }

# ── SDR aggregation ───────────────────────────────────────────────────────────

def build_sdr_stats(enriched):
    """Aggregate per-SDR stats.

    Two sources of truth:
    1. campaign.sdr_owner  → owns that campaign's leads/calls/emails
    2. sdr_breakdown       → who actually generated each meeting (Meeting_Generated_by__c)
    """
    sdr = {}

    def ensure(name):
        if name not in sdr:
            sdr[name] = {
                'name':      name,
                'meetings':  0,
                'calls':     0,
                'emails':    0,
                'leads':     0,
                'campaigns': {},   # campaign_name → meetings_generated
            }

    # Pass 1 – assign leads/calls/emails to the campaign SDR owner
    for c in enriched:
        owner = (c.get('sdr_owner') or '').strip()
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
                                's1_created': 0, 'sdr_breakdown': []})
            done_n[0] += 1
            cache['sync_progress'] = int(done_n[0] / total * 100)

    # Restore original order
    order = {c['id']: i for i, c in enumerate(camps)}
    results.sort(key=lambda x: order.get(x['id'], 999))

    cache['campaigns']     = results
    cache['sdr_stats']     = build_sdr_stats(results)
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
        time.sleep(600)   # 10-minute refresh

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

@app.route('/api/sync', methods=['POST'])
def api_sync():
    if not cache['is_syncing']:
        threading.Thread(target=sync, daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/campaigns', methods=['GET'])
def api_camps_get():
    return jsonify(load_campaigns())

@app.route('/api/campaigns', methods=['POST'])
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
    """Return leads with meetings scheduled. Optional ?campaign= filter."""
    camp = request.args.get('campaign', '').strip()
    if camp:
        q = (f"SELECT Name, Title, Company, Campaign__c, "
             f"Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Source__c "
             f"FROM Lead "
             f"WHERE Campaign__c = '{esc(camp)}' "
             f"AND Meeting_Status__c = 'Meeting Scheduled' "
             f"ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 500")
    else:
        q = ("SELECT Name, Title, Company, Campaign__c, "
             "Meeting_Generated_by__c, Meeting_Generated_on__c, Meeting_Source__c "
             "FROM Lead "
             "WHERE Meeting_Status__c = 'Meeting Scheduled' "
             "ORDER BY Meeting_Generated_on__c DESC NULLS LAST LIMIT 2000")
    result = soql(q)
    records = result.get('records', []) if result else []
    # Clean None values
    leads = []
    for r in records:
        leads.append({
            'name':         r.get('Name') or '—',
            'title':        r.get('Title') or '—',
            'company':      r.get('Company') or '—',
            'campaign':     r.get('Campaign__c') or '—',
            'sdr':          r.get('Meeting_Generated_by__c') or '—',
            'date':         r.get('Meeting_Generated_on__c') or '',
            'source':       r.get('Meeting_Source__c') or '—',
        })
    return jsonify({'leads': leads, 'total': len(leads)})

@app.route('/api/campaigns/<cid>', methods=['PUT'])
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
    app.run(debug=False, port=5001, use_reloader=False)
