"""Campaign history writer — appends every sync's facts to the dg-campaign-history
Supabase project (separate from the dashboard's JSON files, which stay the live
source for the UI).

Contract with app.py
  init_app(app, soql=..., load_campaigns=..., norm_sdr=..., data_dir=..., sf_base_url=..., cache=..., require_admin=...)
  record_campaign(cfg, result, current_ids, frozen_ids, frozen_meetings, moved, nooks_ids)
      — called from campaign_metrics(); cheap, in-memory, never raises.
  flush(app_version=None)
      — called once at the end of _run_sync(); does the Salesforce lookups and
        the REST writes; bounded by HISTORY_TIME_BUDGET_S; never raises.
  summary() → one-line text for the Slack sync message.

Safety
  • Disabled unless HISTORY_SUPABASE_URL and HISTORY_SUPABASE_KEY are set (kill switch = unset).
  • Every step is wrapped; a failure is logged as a dq_finding/writer_error and the sync continues.
  • Failed batches are appended to <data_dir>/history_queue.jsonl and replayed next flush.
  • Writes are idempotent: inserts use ignore-duplicates on the natural key.
  • Only this project is written. Salesforce is read-only. intelligence_dashboard is read-only
    (company attributes for the enrollment snapshot).

State
  <data_dir>/history_state.json.gz — what the writer already knows (per-campaign lead sets,
  meeting states, last snapshot hash, campaign config). Seeded from the database on first
  run (or if the file is lost), so a wiped volume self-heals.
"""
import gzip, hashlib, json, os, threading, time, traceback
import urllib.request, urllib.parse, urllib.error
from datetime import datetime, date, timedelta, timezone

import definitions as D

IST = timezone(timedelta(hours=5, minutes=30))
_BATCH = 500                      # SOQL IN-clause size (matches app.py)
_POST_BATCH = 500                 # rows per REST insert
_lock = threading.Lock()
_pending = {}                     # dashboard campaign id → recorded facts (this sync)
_summary = {'enabled': False, 'text': 'history: disabled'}
_cfg = {}
_deps = {}
_state = None
_last_ok_at = None

LEAD_FIELDS = ("Id, Name, Title, Company, Email, LinkedInProfileURL__c, LinkedIn_Profile__c, Account_Lookup__c, "
               "RC_Account_ID__c, Management_Level__c, Job_Function__c, Lead_Type__c, Status, DoNotCall, Region__c, "
               "Territory__c, State, Campaign__c, Meeting_Generated_on__c, Meeting_Scheduled_on__c, "
               "Meeting_Generated_by__c, Meeting_Status__c, Meeting_Source__c, Meeting_Channel__c, Meeting_Type__c, "
               "Seller_Name__c, Zoom_Meeting_Link_URL__c, SQL_Converted_Date__c, IsConverted, ConvertedDate, "
               "ConvertedContactId, ConvertedAccountId, ConvertedOpportunityId, CreatedDate")
ACCOUNT_FIELDS = ("Id, Name, RC_Account_ID__c, Organization_Type__c, Revenue_Bucket__c, Region__c, Account_Territory__c, "
                  "Territory__c, State__c, BillingState, Is_Healthcare_Provider__c")
OPP_FIELDS = "Id, StageName, Amount"

# ───────────────────────────────────────────────────────────── small helpers ──
def _now(): return datetime.now(timezone.utc)
def _iso(dt=None): return (dt or _now()).isoformat()
def _today_ist(): return datetime.now(IST).date().isoformat()
def _ymd(v): return (v or '')[:10] or None
def _sf15(v): return (v or '')[:15] or None
def _ts_ist(d): return f'{d}T00:00:00+05:30' if d else None
def _email_domain(e):
    e = (e or '').strip().lower()
    return e.split('@')[-1] if '@' in e else None
def _in_win(d, s, e):
    if not d: return False
    if s and d < s: return False
    if e and d > e: return False
    return True
def _ist_date_from_utc(dt_str):
    if not dt_str: return None
    try:
        base = datetime.strptime(str(dt_str)[:19], '%Y-%m-%dT%H:%M:%S')
        return (base + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')
    except Exception:
        return (str(dt_str) or '')[:10] or None
def _hash(obj): return hashlib.sha1(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]
def _log(msg): print(f'[history] {msg}', flush=True)
def _esc(s): return (s or '').replace("'", "\\'")

# ── title-first persona classifier (same rules as weekly_review.py) ──────────
try:
    from weekly_review import _norm_seniority, _norm_function
except Exception:                 # keep the writer importable in isolation
    def _norm_seniority(level, title): return (None, None)
    def _norm_function(fn, title): return (None, None)

# ─────────────────────────────────────────────────────────────── REST layer ──
def _headers(extra=None):
    h = {'apikey': _cfg['key'], 'Authorization': 'Bearer ' + _cfg['key'], 'Content-Type': 'application/json'}
    if extra: h.update(extra)
    return h

def _http(method, path, params=None, body=None, prefer=None, timeout=90, base=None, key=None):
    """One REST call. Returns parsed JSON (or [] for empty). Raises on HTTP error."""
    url = (base or _cfg['url']) + '/rest/v1/' + path
    if params: url += '?' + urllib.parse.urlencode(params)
    hdr = _headers({'Prefer': prefer} if prefer else None)
    if key:
        hdr['apikey'] = key; hdr['Authorization'] = 'Bearer ' + key
    req = urllib.request.Request(url, data=(json.dumps(body, default=str).encode() if body is not None else None), headers=hdr, method=method)
    raw = urllib.request.urlopen(req, timeout=timeout).read()
    return json.loads(raw) if raw else []

def _get_all(path, params, page=1000, order=None):
    """Offset paging is only stable with an explicit ORDER BY — always pass one."""
    out, off = [], 0
    while True:
        p = dict(params); p['limit'] = str(page); p['offset'] = str(off)
        if order: p['order'] = order
        b = _http('GET', path, p)
        out += b; off += page
        if len(b) < page: return out

def _insert(table, rows, on_conflict=None, prefer='resolution=ignore-duplicates', returning=False, count_col=None):
    """Idempotent batched insert; keys normalised per batch; failures → queue."""
    if not rows: return 0
    n = 0; got = []
    # PostgREST bulk insert needs identical keys per request: group by key-set instead of
    # injecting nulls (an explicit null would override a column DEFAULT and break NOT NULL).
    groups = {}
    for r in rows: groups.setdefault(tuple(sorted(r)), []).append(r)
    for keys, grp in groups.items():
        for i in range(0, len(grp), _POST_BATCH):
            chunk = grp[i:i + _POST_BATCH]
            params = {'on_conflict': on_conflict} if on_conflict else {}
            if count_col and not returning: params['select'] = count_col
            pref = prefer + (',return=representation' if (returning or count_col) else ',return=minimal')
            try:
                res = _http('POST', table, params or None, chunk, prefer=pref)
                if returning: got += res
                n += len(res) if (count_col and not returning) else len(chunk)
            except urllib.error.HTTPError as e:
                if e.code == 409:          # unique violation on an index PostgREST can't target → rows already exist
                    _log(f'{table}: {len(chunk)} rows already present (409), skipped'); continue
                _queue({'op': 'insert', 'table': table, 'rows': chunk, 'on_conflict': on_conflict, 'prefer': prefer})
                _err(f'{table} insert batch failed → queued ({len(chunk)} rows): {_short(e)}')
            except Exception as e:
                _queue({'op': 'insert', 'table': table, 'rows': chunk, 'on_conflict': on_conflict, 'prefer': prefer})
                _err(f'{table} insert batch failed → queued ({len(chunk)} rows): {_short(e)}')
    return got if returning else n

def _patch(table, params, body):
    try:
        _http('PATCH', table, params, body, prefer='return=minimal'); return True
    except Exception as e:
        _queue({'op': 'patch', 'table': table, 'params': params, 'body': body})
        _err(f'{table} patch failed → queued: {_short(e)}'); return False

def _short(e):
    if isinstance(e, urllib.error.HTTPError):
        try: return f'HTTP {e.code} {e.read()[:300].decode(errors="replace")}'
        except Exception: return f'HTTP {e.code}'
    return str(e)[:300]

# ───────────────────────────────────────────────────────────────── queue ──
def _queue_path(): return os.path.join(_cfg['data_dir'], 'history_queue.jsonl')
def _queue(item):
    try:
        with open(_queue_path(), 'a') as f: f.write(json.dumps(item, default=str) + '\n')
    except Exception as e:
        _log(f'QUEUE WRITE FAILED: {e}')

def _replay_queue():
    p = _queue_path()
    if not os.path.exists(p): return 0
    try:
        with open(p) as f: items = [json.loads(l) for l in f if l.strip()]
    except Exception as e:
        _err(f'queue unreadable: {e}'); return 0
    os.replace(p, p + '.replaying')
    ok = 0
    for it in items:
        try:
            if it['op'] == 'insert':
                _http('POST', it['table'], {'on_conflict': it['on_conflict']} if it.get('on_conflict') else None, it['rows'], prefer=it.get('prefer', 'resolution=ignore-duplicates') + ',return=minimal')
            elif it['op'] == 'patch':
                _http('PATCH', it['table'], it['params'], it['body'], prefer='return=minimal')
            ok += 1
        except urllib.error.HTTPError as e:
            if e.code == 409: ok += 1; continue
            _queue(it); _err(f'replay failed, re-queued: {_short(e)}')
        except Exception as e:
            _queue(it); _err(f'replay failed, re-queued: {_short(e)}')
    try: os.remove(p + '.replaying')
    except Exception: pass
    return ok

# ─────────────────────────────────────────────────────────────── state ──
def _state_path(): return os.path.join(_cfg['data_dir'], 'history_state.json.gz')
def _load_state():
    global _state
    try:
        with gzip.open(_state_path(), 'rt', encoding='utf-8') as f: _state = json.load(f)
        _log(f"state loaded: {len(_state.get('campaigns', {}))} campaigns")
    except FileNotFoundError:
        _state = None
    except Exception as e:
        _log(f'state unreadable ({e}) — will reseed from DB'); _state = None
def _save_state():
    tmp = _state_path() + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f: json.dump(_state, f)
    os.replace(tmp, _state_path())

def _seed_state_from_db():
    """Rebuild the writer's memory from the database (first run, or lost volume)."""
    global _state
    t0 = time.time()
    camps = _get_all('campaigns', {'select': 'id,dashboard_id,sf_name,name,campaign_type,segment,pod_team,sdr_owner,email_owner,status,start_date,end_date,settled_date,manual_s1,registered,deleted_at'}, order='id.asc')
    st = {'seeded_at': _iso(), 'campaigns': {}, 'meetings': {}, 'primary': {}, 'stamps': [], 'dq_seen': [], 'people': [], 'definitions_version': None}
    by_id = {}
    for c in camps:
        e = {'db_id': c['id'], 'dashboard_id': c.get('dashboard_id'), 'sf_name': c['sf_name'], 'config': {k: c.get(k) for k in ('name','campaign_type','segment','pod_team','sdr_owner','email_owner','status','start_date','end_date','settled_date','manual_s1')},
             'registered': c.get('registered'), 'deleted': bool(c.get('deleted_at')), 'leads': [], 'last_snapshot': None}
        st['campaigns'][c['sf_name']] = e; by_id[c['id']] = e
    rows = _get_all('campaign_leads', {'select': 'campaign_id,lead_id,removed_reason'}, page=1000, order='campaign_id.asc,lead_id.asc')
    for r in rows:
        e = by_id.get(r['campaign_id'])
        if e:
            e['leads'].append(r['lead_id'])
            if (r.get('removed_reason') or '').startswith('moved_to:'): e.setdefault('pruned', []).append(r['lead_id'])
    for m in _get_all('meetings', {'select': 'meeting_id,lead_id,generated_on,meeting_status,lead_status,opp_stage,opp_amount,superseded_by'}, order='meeting_id.asc'):
        st['meetings'][m['meeting_id']] = {'lead_id': m['lead_id'], 'on': m['generated_on'], 'h': _hash([m.get('meeting_status'), m.get('lead_status'), m.get('opp_stage'), m.get('opp_amount')])}
    st['links'] = []
    for a in _get_all('meeting_attributions', {'select': 'meeting_id,campaign_id,is_primary', 'superseded_at': 'is.null'}, order='id.asc'):
        st['links'].append(f"{a['meeting_id']}|{a['campaign_id']}")
        if a.get('is_primary'): st['primary'][a['meeting_id']] = a['campaign_id']
    for s in _get_all('v_campaign_latest_snapshot', {'select': 'campaign_id,as_of_date,is_final,total_leads,total_calls,total_connects,total_conversations,total_emails,unique_leads_called,unique_leads_emailed,li_sent,li_accepted,li_msg_sent,li_msg_reply,meetings,meeting_done,meeting_noshow,sql_gen,s1_created'}, order='campaign_id.asc'):
        e = by_id.get(s['campaign_id'])
        if e: e['last_snapshot'] = {'as_of': s['as_of_date'], 'final': s['is_final'], 'h': _metric_hash(s)}
    st['stamps'] = [c['sf_name'] for c in camps]
    st['people'] = _people_aliases()
    st['dq_seen'] = [f"{d['check_name']}|{d['subject_type']}|{d['subject_id']}" for d in _get_all('dq_findings', {'select': 'check_name,subject_type,subject_id'}, order='id.asc')]
    _state = st
    _save_state()
    _log(f'state seeded from DB in {time.time()-t0:.0f}s: {len(camps)} campaigns, {len(rows)} memberships, {len(st["meetings"])} meetings')

def _people_aliases():
    out = {}
    for p in _get_all('people', {'select': 'display_name,aliases'}, order='person_key.asc'):
        for a in (p.get('aliases') or []) + [p['display_name']]:
            out[a.strip().lower()] = p['display_name']
    return out

_METRIC_KEYS = ('total_leads','total_calls','total_connects','total_conversations','total_emails','unique_leads_called','unique_leads_emailed',
                'li_sent','li_accepted','li_msg_sent','li_msg_reply','meetings','meeting_done','meeting_noshow','sql_gen','s1_created')
def _metric_hash(r): return _hash([r.get(k) for k in _METRIC_KEYS])

# ───────────────────────────────────────────────────────────── recording ──
def record_campaign(cfg, result, current_ids, frozen_ids, frozen_meetings, moved=None, nooks_ids=None):
    """Called from campaign_metrics(). Stores facts for flush(); never raises."""
    if not _summary['enabled']: return
    try:
        with _lock:
            _pending[cfg['id']] = {
                'cfg': {k: cfg.get(k) for k in ('id','name','campaign_type','segment','pod_team','sdr_owner','email_owner','status','start_date','end_date','manual_s1')},
                'result': {k: result.get(k) for k in _METRIC_KEYS + ('s1_is_manual','call_dispositions','sdr_breakdown','status_sdr_breakdown','settled_date','synced_at')},
                'current_ids': list(current_ids or []), 'frozen_ids': list(frozen_ids or []),
                'meetings': {lid: {'date': _ymd(m.get('date')), 'sdr': m.get('sdr'), 'name': m.get('name'), 'title': m.get('title'), 'company': m.get('company')} for lid, m in (frozen_meetings or {}).items()},
                'moved': dict(moved or {}), 'nooks_ids': list(nooks_ids or []), 'recorded_at': _iso(),
            }
    except Exception as e:
        _log(f'record_campaign failed for {cfg.get("name")}: {e}')

# ───────────────────────────────────────────────────────────── the flush ──
_errors = []
def _err(msg):
    _errors.append(msg); _log('ERROR ' + msg)

def flush(app_version=None):
    """End-of-sync writer. Never raises; reports via summary()."""
    global _last_ok_at
    if not _summary['enabled']: return
    t0 = time.time(); budget = float(_cfg.get('budget', 420))
    _errors.clear()
    stats = {'members_new': 0, 'members_removed': 0, 'meetings_new': 0, 'meetings_updated': 0, 'attributions': 0, 'snapshots': 0,
             'campaigns_new': 0, 'campaigns_changed': 0, 'unregistered_new': 0, 'dq': 0, 'queued_replayed': 0, 'mismatches': 0}
    with _lock:
        pending = dict(_pending); _pending.clear()
    _flush_pending.clear(); _flush_pending.update(pending)
    run_id = None
    try:
        stats['queued_replayed'] = _replay_queue()
        if _state is None: _load_state()            # picks up a state file written after init (or by a previous process)
        if _state is None: _seed_state_from_db()
        run_id = _start_run(app_version, len(pending))
        _ensure_definitions()
        steps = [('campaigns', lambda: _sync_campaigns(run_id, stats)),
                 ('unregistered', lambda: _discover_unregistered(run_id, stats)),
                 ('membership', lambda: _sync_membership(run_id, pending, stats)),
                 ('meetings', lambda: _sync_meetings(run_id, pending, stats)),
                 ('snapshots', lambda: _sync_snapshots(run_id, pending, stats)),
                 ('reconcile', lambda: _reconcile(run_id, pending, stats))]
        for name, fn in steps:
            if time.time() - t0 > budget:
                _err(f'time budget exhausted before step {name}; remaining steps skipped (facts stay pending on disk state)'); break
            try:
                fn()
            except Exception:
                _err(f'step {name} crashed: {traceback.format_exc()[-600:]}')
            _save_state()
        _last_ok_at = _iso()
    except Exception:
        _err(f'flush crashed: {traceback.format_exc()[-800:]}')
    finally:
        stats['seconds'] = round(time.time() - t0, 1); stats['errors'] = len(_errors)
        try:
            if run_id:
                _patch('sync_runs', {'id': f'eq.{run_id}'}, {'finished_at': _iso(), 'rows_written': stats['members_new'] + stats['meetings_new'] + stats['attributions'] + stats['snapshots'], 'errors': _errors[:50], 'summary': stats})
                if _errors:
                    _dq(run_id, 'writer_error', 'error', 'run', str(run_id), '; '.join(_errors)[:1500], stats)
        except Exception as e:
            _log(f'could not close run: {e}')
        _summary['text'] = (f"history: +{stats['members_new']} members, +{stats['meetings_new']} meetings ({stats['meetings_updated']} updated), "
                            f"{stats['snapshots']} snapshots, {stats['unregistered_new']} new unregistered, {stats['mismatches']} mismatches, "
                            f"{stats['dq']} dq, {stats['errors']} errors, {stats['seconds']}s")
        _summary['stats'] = stats; _summary['run_id'] = run_id
        _log(_summary['text'])

def summary(): return _summary['text']

def _start_run(app_version, n):
    rows = _insert('sync_runs', [{'source': 'dashboard_sync', 'app_version': app_version or os.environ.get('RAILWAY_GIT_COMMIT_SHA', '')[:12] or 'local', 'campaigns_processed': n, 'notes': f'definitions v{D.DEFINITION_VERSION} {D.definitions_hash()}'}], returning=True)
    return rows[0]['id'] if rows else None

def _ensure_definitions():
    if _state.get('definitions_version') == D.DEFINITION_VERSION: return
    existing = _http('GET', 'metric_definitions', {'select': 'version', 'version': f'eq.{D.DEFINITION_VERSION}'})
    if not existing:
        _insert('metric_definitions', [{'version': D.DEFINITION_VERSION, 'effective_from': _today_ist(), 'definitions': D.as_dict(), 'notes': D.CHANGELOG.get(D.DEFINITION_VERSION, '')}], on_conflict='version')
    _state['definitions_version'] = D.DEFINITION_VERSION

def _dq(run_id, check, severity, subject_type, subject_id, detail, stats):
    key = f'{check}|{subject_type}|{subject_id}'
    if key in _state['dq_seen']: return
    _insert('dq_findings', [{'sync_run_id': run_id, 'check_name': check, 'severity': severity, 'subject_type': subject_type, 'subject_id': subject_id, 'detail': (detail or '')[:2000]}], on_conflict='check_name,subject_type,subject_id')
    _state['dq_seen'].append(key); stats['dq'] += 1

# ── step: campaigns (config diff, new, deleted, link unregistered) ───────────
_CFG_KEYS = ('name','campaign_type','segment','pod_team','sdr_owner','email_owner','status','start_date','end_date','settled_date','manual_s1')
_flush_pending = {}
def _pending_result(did): return (_flush_pending.get(did) or {}).get('result')
def _sync_campaigns(run_id, stats):
    camps = _deps['load_campaigns']()
    seen_dash = set()
    by_dash = {e['dashboard_id']: e for e in _state['campaigns'].values() if e.get('dashboard_id')}
    names = {}
    for c in camps:
        names.setdefault(c['name'].strip(), []).append(c['id'])
    for name, ids in names.items():
        if len(ids) > 1:
            _dq(run_id, 'duplicate_registration', 'warn', 'campaign', name, f'dashboard ids {ids} share the name; only the first is matched to the Salesforce stamp', stats)
    for c in sorted(camps, key=lambda x: (len(x['id']), x['id'])):
        did = c['id']; seen_dash.add(did)
        cfg = {k: (c.get(k) if c.get(k) not in ('', None) else None) for k in _CFG_KEYS if k != 'settled_date'}
        cfg['manual_s1'] = int(cfg['manual_s1']) if str(cfg['manual_s1'] or '').strip().isdigit() else None
        e = by_dash.get(did)
        if not e:
            sfn = c['name'].strip()
            e = _state['campaigns'].get(sfn)
            if e and not e.get('dashboard_id'):
                # an unregistered Salesforce stamp is being registered now → link it
                _patch('campaigns', {'id': f'eq.{e["db_id"]}'}, {'dashboard_id': did, 'registered': True, **cfg})
                _insert('campaign_config_history', [{'campaign_id': e['db_id'], 'field': 'registered', 'old_value': 'false', 'new_value': json.dumps(cfg), 'changed_by': 'dashboard', 'sync_run_id': run_id}])
                e['dashboard_id'] = did; e['config'] = cfg; e['registered'] = True; by_dash[did] = e
                stats['campaigns_new'] += 1; continue
            if e:   # name collides with a different registered campaign
                sfn = f"{sfn} [duplicate registration · dashboard id {did}]"
                if sfn in _state['campaigns']:
                    continue
            rows = _insert('campaigns', [{'dashboard_id': did, 'sf_name': sfn, 'registered': True, 'discovered_from': 'dashboard', 'created_at': _ts_ist(cfg['start_date']) or _iso(), **cfg}], on_conflict='sf_name', returning=True)
            if not rows:
                continue
            e = {'db_id': rows[0]['id'], 'dashboard_id': did, 'sf_name': sfn, 'config': cfg, 'registered': True, 'deleted': False, 'leads': [], 'last_snapshot': None}
            _state['campaigns'][sfn] = e; by_dash[did] = e; _state['stamps'].append(sfn)
            _insert('campaign_config_history', [{'campaign_id': e['db_id'], 'field': 'created', 'new_value': json.dumps(cfg), 'changed_by': 'dashboard', 'sync_run_id': run_id}])
            stats['campaigns_new'] += 1
            continue
        # existing → diff (settled_date is owned by the sync result, compared only when present)
        old = e.get('config') or {}
        sd = (_pending_result(did) or {}).get('settled_date')
        if sd: cfg['settled_date'] = sd
        changes = [(k, old.get(k), cfg.get(k)) for k in _CFG_KEYS if k in cfg and (old.get(k) or None) != (cfg.get(k) or None)]
        if changes:
            _patch('campaigns', {'id': f'eq.{e["db_id"]}'}, {k: cfg.get(k) for k, _, _ in changes})
            _insert('campaign_config_history', [{'campaign_id': e['db_id'], 'field': k, 'old_value': None if o is None else str(o), 'new_value': None if n is None else str(n), 'changed_by': 'dashboard', 'sync_run_id': run_id} for k, o, n in changes])
            if any(k in ('start_date', 'end_date') for k, _, _ in changes) and old.get('start_date'):
                _dq(run_id, 'window_edit', 'info', 'campaign', e['db_id'], f"{e['sf_name']}: " + '; '.join(f'{k} {o}→{n}' for k, o, n in changes if k in ('start_date','end_date')) + f' at {_iso()}', stats)
            e['config'] = {**old, **cfg}; stats['campaigns_changed'] += 1
        if e.get('deleted'):
            _patch('campaigns', {'id': f'eq.{e["db_id"]}'}, {'deleted_at': None}); e['deleted'] = False
    # deleted on the dashboard — guarded: a truncated/partial campaigns.json must never mass-delete history
    gone = [e for did, e in by_dash.items() if did not in seen_dash and not e.get('deleted')]
    if len(gone) > max(5, int(0.05 * max(1, len(by_dash)))):
        _dq(run_id, 'mass_deletion_suspected', 'error', 'run', str(run_id), f'{len(gone)} registered campaigns missing from the dashboard list in one sync (list has {len(camps)}); NOT marking them deleted', stats)
        gone = []
    for e in gone:
        did = e['dashboard_id']
        if True:
            _patch('campaigns', {'id': f'eq.{e["db_id"]}'}, {'deleted_at': _iso()})
            _insert('campaign_config_history', [{'campaign_id': e['db_id'], 'field': 'deleted', 'new_value': _iso(), 'changed_by': 'dashboard', 'sync_run_id': run_id}])
            _dq(run_id, 'campaign_deleted', 'warn', 'campaign', e['db_id'], f"{e['sf_name']} removed from the dashboard; its history is kept", stats)
            e['deleted'] = True

# ── step: unregistered Salesforce stamps ─────────────────────────────────────
def _discover_unregistered(run_id, stats):
    res = _deps['soql']("SELECT Campaign__c, COUNT(Id) FROM Lead WHERE Campaign__c != null GROUP BY Campaign__c")
    if not res: return
    known = set(_state['stamps']) | set(_state['campaigns'].keys())
    new = [(r.get('Campaign__c').strip(), int(r.get('expr0') or 0)) for r in res.get('records', []) if (r.get('Campaign__c') or '').strip() and r.get('Campaign__c').strip() not in known]
    now = _iso()
    for stamp, n in sorted(new, key=lambda x: -x[1])[:int(_cfg.get('max_unreg', 20))]:
        rows = _insert('campaigns', [{'dashboard_id': None, 'sf_name': stamp, 'name': stamp, 'registered': False, 'discovered_from': 'salesforce', 'last_seen_in_salesforce_at': now, 'notes': f'Lead.Campaign__c value never registered on the dashboard; {n} leads carried it when first seen ({_today_ist()}).'}], on_conflict='sf_name', returning=True)
        if not rows: continue
        e = {'db_id': rows[0]['id'], 'dashboard_id': None, 'sf_name': stamp, 'config': {}, 'registered': False, 'deleted': False, 'leads': [], 'last_snapshot': None}
        _state['campaigns'][stamp] = e; _state['stamps'].append(stamp)
        _dq(run_id, 'unregistered_stamp', 'warn', 'campaign', e['db_id'], f"'{stamp}' ({n} leads) is stamped in Salesforce but not registered on the dashboard", stats)
        # capture its members now so nothing is invisible
        lr = _deps['soql'](f"SELECT Id FROM Lead WHERE Campaign__c = '{_esc(stamp)}' LIMIT 10000")
        ids = [r['Id'] for r in (lr or {}).get('records', [])]
        _write_members(run_id, e, ids, source='salesforce_discovery', stats=stats)
        stats['unregistered_new'] += 1

# ── step: membership ─────────────────────────────────────────────────────────
def _sync_membership(run_id, pending, stats):
    by_dash = {e['dashboard_id']: e for e in _state['campaigns'].values() if e.get('dashboard_id')}
    for did, p in pending.items():
        e = by_dash.get(did)
        if not e: continue
        known = set(e['leads'])
        new_ids = [l for l in p['frozen_ids'] if l not in known]
        _write_members(run_id, e, new_ids, source='sync', stats=stats)
        # removals observed by the dashboard prune (Active campaigns only)
        moved = {l: n for l, n in (p.get('moved') or {}).items() if l in known}
        if moved:
            by_target = {}
            for l, n in moved.items(): by_target.setdefault(n or '', []).append(l)
            for target, ids in by_target.items():
                for i in range(0, len(ids), 200):
                    chunk = ids[i:i+200]
                    _patch('campaign_leads', {'campaign_id': f'eq.{e["db_id"]}', 'lead_id': f'in.({",".join(chunk)})', 'removed_at': 'is.null'},
                           {'removed_at': _iso(), 'removed_reason': (f'moved_to:{target}' if target else 'campaign_blanked')})
            e.setdefault('pruned', []).extend(k for k in moved if k not in e.get('pruned', [])); stats['members_removed'] += len(moved)

def _write_members(run_id, e, ids, source, stats):
    if not ids: return
    leads = _fetch_leads(ids)
    accts = _fetch_accounts({(r.get('Account_Lookup__c') or '') for r in leads.values()} | {(r.get('ConvertedAccountId') or '') for r in leads.values()})
    comps = _fetch_companies(accts, leads)
    now = _iso(); first_seen = _ts_ist((e.get('config') or {}).get('start_date')) or now
    if first_seen > now: first_seen = now
    rows, lead_rows, acct_rows = [], [], []
    for lid in ids:
        r = leads.get(lid)
        if not r:
            rows.append({'campaign_id': e['db_id'], 'lead_id': lid, 'first_seen_at': now, 'last_seen_at': now, 'membership_source': source, 'snapshot_quality': 'at_enroll', 'removed_reason': 'lead_deleted', 'sync_run_id': run_id})
            lead_rows.append({'lead_id': lid, 'last_seen_at': now, 'is_deleted': True}); continue
        a = accts.get(r.get('Account_Lookup__c') or '') or {}
        co = comps.get(_sf15(r.get('Account_Lookup__c'))) or comps.get((r.get('RC_Account_ID__c') or '').strip()) or {}
        sen, _ = _norm_seniority(r.get('Management_Level__c'), r.get('Title')); fn, _ = _norm_function(r.get('Job_Function__c'), r.get('Title'))
        rows.append({'campaign_id': e['db_id'], 'lead_id': lid, 'first_seen_at': now if source != 'sync' else max(first_seen, (r.get('CreatedDate') or '')[:19] + '+00:00' if r.get('CreatedDate') else first_seen), 'last_seen_at': now,
                     'membership_source': source, 'snapshot_quality': 'at_enroll', 'sync_run_id': run_id,
                     'name': r.get('Name'), 'title': r.get('Title'), 'company': r.get('Company'), 'email_domain': _email_domain(r.get('Email')),
                     'linkedin_url': r.get('LinkedInProfileURL__c') or r.get('LinkedIn_Profile__c'), 'account_sf_id': r.get('Account_Lookup__c') or None,
                     'rc_account_id': (r.get('RC_Account_ID__c') or '').strip() or None, 'management_level_sfdc': r.get('Management_Level__c'), 'job_function_sfdc': r.get('Job_Function__c'),
                     'lead_type_sfdc': r.get('Lead_Type__c'), 'seniority_norm': sen, 'function_norm': fn, 'lead_status_at_enroll': r.get('Status'), 'do_not_call_at_enroll': bool(r.get('DoNotCall')),
                     'region': r.get('Region__c') or a.get('Region__c'), 'territory': r.get('Territory__c') or a.get('Account_Territory__c') or a.get('Territory__c'),
                     'state': r.get('State') or a.get('State__c') or a.get('BillingState'), 'org_type_sfdc': a.get('Organization_Type__c'), 'revenue_bucket_sfdc': a.get('Revenue_Bucket__c'),
                     'sb_company_id': co.get('id'), 'org_type_sb': co.get('organisation_type'), 'revenue_estimate_usd': co.get('revenue_estimate_usd'),
                     'specialties_sb': co.get('specialty_type') if isinstance(co.get('specialty_type'), list) else None})
        lead_rows.append(_lead_row(r, now))
    for aid, a in accts.items():
        co = comps.get(_sf15(aid)) or comps.get((a.get('RC_Account_ID__c') or '').strip()) or {}
        acct_rows.append({'account_sf_id': aid, 'rc_account_id': (a.get('RC_Account_ID__c') or '').strip() or None, 'name': a.get('Name') or co.get('company_name'), 'org_type_sfdc': a.get('Organization_Type__c'),
                          'revenue_bucket_sfdc': a.get('Revenue_Bucket__c'), 'region': a.get('Region__c'), 'territory': a.get('Account_Territory__c') or a.get('Territory__c'), 'state': a.get('State__c') or a.get('BillingState'),
                          'sb_company_id': co.get('id'), 'org_type_sb': co.get('organisation_type'), 'revenue_estimate_usd': co.get('revenue_estimate_usd'),
                          'specialties': co.get('specialty_type') if isinstance(co.get('specialty_type'), list) else None,
                          'is_provider': co.get('is_provider') if co.get('is_provider') is not None else (bool(a.get('Is_Healthcare_Provider__c')) if a else None), 'last_seen_at': now})
    _insert('accounts', acct_rows, on_conflict='account_sf_id', prefer='resolution=merge-duplicates')
    _insert('leads', lead_rows, on_conflict='lead_id', prefer='resolution=merge-duplicates')
    n = _insert('campaign_leads', rows, on_conflict='campaign_id,lead_id', count_col='lead_id')
    e['leads'].extend(ids); stats['members_new'] += n

def _lead_row(r, now):
    return {'lead_id': r['Id'], 'last_seen_at': now, 'name': r.get('Name'), 'current_title': r.get('Title'), 'current_company': r.get('Company'),
            'current_account_sf_id': r.get('Account_Lookup__c') or None, 'rc_account_id': (r.get('RC_Account_ID__c') or '').strip() or None, 'current_status': r.get('Status'),
            'is_converted': bool(r.get('IsConverted')), 'converted_on': _ymd(r.get('ConvertedDate')), 'contact_id': r.get('ConvertedContactId') or None,
            'converted_account_id': r.get('ConvertedAccountId') or None, 'converted_opp_id': r.get('ConvertedOpportunityId') or None, 'is_deleted': False, 'do_not_call': bool(r.get('DoNotCall'))}

# ── Salesforce / company lookups (batched) ──────────────────────────────────
def _fetch_leads(ids):
    out = {}
    ids = [i for i in dict.fromkeys(ids) if i]
    for i in range(0, len(ids), _BATCH):
        chunk = ids[i:i+_BATCH]
        res = _deps['soql'](f"SELECT {LEAD_FIELDS} FROM Lead WHERE Id IN ({','.join(repr(x) for x in chunk)})", paginate=False)
        for r in (res or {}).get('records', []): out[r['Id']] = r
    return out

def _fetch_accounts(ids):
    out = {}
    ids = [i for i in dict.fromkeys(ids) if i]
    for i in range(0, len(ids), _BATCH):
        chunk = ids[i:i+_BATCH]
        res = _deps['soql'](f"SELECT {ACCOUNT_FIELDS} FROM Account WHERE Id IN ({','.join(repr(x) for x in chunk)})", paginate=False)
        for r in (res or {}).get('records', []): out[r['Id']] = r
    return out

def _fetch_companies(accts, leads):
    """READ-ONLY lookup in intelligence_dashboard (org type / revenue / specialties), keyed by 15-char SF id and RC id."""
    if not _cfg.get('intel_key'): return {}
    sf = {_sf15(a) for a in accts if a}; rc = {(v.get('RC_Account_ID__c') or '').strip() for v in list(accts.values()) + list(leads.values())} - {''}
    out = {}
    try:
        for key_list, col in ((sorted(sf), 'salesforce_account_id'), (sorted(rc), 'rc_account_id')):
            for i in range(0, len(key_list), 200):
                chunk = key_list[i:i+200]
                rows = _http('GET', 'companies', {'select': 'id,company_name,salesforce_account_id,rc_account_id,organisation_type,revenue_estimate_usd,specialty_type,is_provider', col: f'in.({",".join(chunk)})'},
                             base=_cfg['intel_url'], key=_cfg['intel_key'], timeout=60)
                for c in rows:
                    if c.get('salesforce_account_id'): out.setdefault(_sf15(c['salesforce_account_id']), c)
                    if c.get('rc_account_id'): out.setdefault(c['rc_account_id'].strip(), c)
    except Exception as e:
        _err(f'company lookup (read-only) failed: {_short(e)}')
    return out

def _fetch_opps(ids):
    out = {}
    ids = [i for i in dict.fromkeys(ids) if i]
    for i in range(0, len(ids), _BATCH):
        chunk = ids[i:i+_BATCH]
        res = _deps['soql'](f"SELECT {OPP_FIELDS} FROM Opportunity WHERE Id IN ({','.join(repr(x) for x in chunk)})", paginate=False)
        for r in (res or {}).get('records', []): out[r['Id']] = r
    return out

# ── step: meetings + attribution + state history ─────────────────────────────
def _sync_meetings(run_id, pending, stats):
    by_dash = {e['dashboard_id']: e for e in _state['campaigns'].values() if e.get('dashboard_id')}
    # candidates: (meeting_id) → list of (campaign entry, ledger entry, in_window, nooks)
    cands = {}; ledger_snap = {}
    for did, p in pending.items():
        e = by_dash.get(did)
        if not e: continue
        cfg = e.get('config') or {}
        for lid, m in p['meetings'].items():
            if not m.get('date'): continue
            mid = f"{lid}:{m['date']}"
            cands.setdefault(mid, []).append((e, m, _in_win(m['date'], cfg.get('start_date'), cfg.get('end_date')), lid in set(p.get('nooks_ids') or [])))
            ledger_snap.setdefault(mid, m)
    new_mids = [mid for mid in cands if mid not in _state['meetings']]
    # refresh window: meetings from the last 120 days plus all new ones
    cutoff = (date.today() - timedelta(days=120)).isoformat()
    refresh = [mid for mid, s in _state['meetings'].items() if (s.get('on') or '') >= cutoff]
    lead_ids = list({mid.split(':')[0] for mid in new_mids + refresh})
    leads = _fetch_leads(lead_ids)
    opps = _fetch_opps([r.get('ConvertedOpportunityId') for r in leads.values() if r.get('ConvertedOpportunityId')])
    accts = _fetch_accounts({(leads[mid.split(':')[0]].get('Account_Lookup__c') or '') for mid in new_mids if mid.split(':')[0] in leads})
    comps = _fetch_companies(accts, {k: v for k, v in leads.items() if any(m.startswith(k + ':') for m in new_mids)})
    now = _iso()
    people = _state.get('people') or {}
    # 1) new meetings
    rows, attrs, hist = [], [], []
    for mid in new_mids:
        lid, mdate = mid.split(':'); r = leads.get(lid) or {}; m = ledger_snap[mid]
        st = _state_from_lead(r, mdate, opps)
        title = (m.get('title') if m.get('title') not in (None, '—') else None) or r.get('Title')
        sen, _ = _norm_seniority(r.get('Management_Level__c'), title); fn, _ = _norm_function(r.get('Job_Function__c'), title)
        raw_sdr = m.get('sdr') or r.get('Meeting_Generated_by__c') or ''
        gen_by = _deps['norm_sdr'](raw_sdr) or None
        if raw_sdr and raw_sdr.strip().lower() not in people and (gen_by or '').strip().lower() not in people:
            _dq(run_id, 'unknown_sdr_alias', 'warn', 'meeting', mid, f"Meeting_Generated_by__c '{raw_sdr}' is not in the people table", stats)
        via = 'sfdc_field' if _ymd(r.get('Meeting_Generated_on__c')) == mdate else 'nooks_task'
        rows.append({'meeting_id': mid, 'lead_id': lid, 'generated_on': mdate, 'scheduled_at': r.get('Meeting_Scheduled_on__c'), 'scheduled_on_ist': _ist_date_from_utc(r.get('Meeting_Scheduled_on__c')),
                     'generated_by': gen_by, 'seller': _deps['norm_sdr'](r.get('Seller_Name__c') or '') or None, 'source': r.get('Meeting_Source__c'), 'channel': r.get('Meeting_Channel__c'),
                     'meeting_type': r.get('Meeting_Type__c'), 'booked_via': via, 'zoom_url': r.get('Zoom_Meeting_Link_URL__c'),
                     'name_at_meeting': (m.get('name') if m.get('name') not in (None, '—') else None) or r.get('Name'), 'title_at_meeting': title,
                     'company_at_meeting': (m.get('company') if m.get('company') not in (None, '—') else None) or r.get('Company'),
                     'account_sf_id': r.get('Account_Lookup__c') or None, 'rc_account_id': (r.get('RC_Account_ID__c') or '').strip() or None,
                     'seniority_norm': sen, 'function_norm': fn, 'lead_type_sfdc': r.get('Lead_Type__c'), 'first_seen_at': now, 'sync_run_id': run_id, **st})
        if r: hist.append(_lead_row(r, now))
        if (r.get('Campaign__c') or '').strip() == '' and r:
            _dq(run_id, 'blank_stamp_meeting', 'warn', 'lead', lid, f"{r.get('Name')} has a meeting on {mdate} but Campaign__c is blank", stats)
        # attribution: in-window candidates, tie → lead's current stamp, then latest start
        cs = cands[mid]; inw = [c for c in cs if c[2]]
        stamp = (r.get('Campaign__c') or '').strip()
        existing_primary = _state['primary'].get(mid)
        chosen = None
        if inw and not existing_primary:
            inw.sort(key=lambda c: ((c[0]['sf_name'] == stamp), (c[0].get('config') or {}).get('start_date') or ''), reverse=True)
            chosen = inw[0][0]['db_id']
        for e, m2, in_w, nooks in cs:
            is_p = (e['db_id'] == chosen)
            attrs.append({'meeting_id': mid, 'campaign_id': e['db_id'], 'rule': ('nooks_booking' if nooks else 'member_in_window') if in_w else 'member_in_window', 'in_window': in_w,
                          'is_primary': is_p, 'attributed_at': now, 'sync_run_id': run_id,
                          'note': None if in_w else 'ledger entry outside the campaign window — the dashboard does not count it here'})
        if chosen: _state['primary'][mid] = chosen
        for e, _m2, _iw, _nk in cs: _state.setdefault('links', []).append(f"{mid}|{e['db_id']}")
        _state['meetings'][mid] = {'lead_id': lid, 'on': mdate, 'h': _hash([st.get('meeting_status'), st.get('lead_status'), st.get('opp_stage'), st.get('opp_amount')])}
        if st.get('meeting_status') or st.get('lead_status'):
            _msh(mid, st, now, run_id)
    _insert('leads', hist, on_conflict='lead_id', prefer='resolution=merge-duplicates')
    stats['meetings_new'] += _insert('meetings', rows, on_conflict='meeting_id', count_col='meeting_id')
    stats['attributions'] += _insert('meeting_attributions', attrs, count_col='id')
    # 1b) an existing meeting seen in a NEW campaign's ledger (recycled lead) → non-primary link, never re-primary
    extra = []
    for mid, cs in cands.items():
        if mid in new_mids: continue
        fresh = [(e, m2, in_w, nooks) for e, m2, in_w, nooks in cs if f"{mid}|{e['db_id']}" not in _state.setdefault('links', [])]
        if not fresh: continue
        # an existing meeting with NO live primary (e.g. imported from Salesforce with a blank stamp)
        # gains one the first time it shows up in-window in a campaign ledger
        chosen = None
        if mid not in _state['primary']:
            inw = [f for f in fresh if f[2]]
            if inw:
                r = leads.get(mid.split(':')[0]) or {}; stamp = (r.get('Campaign__c') or '').strip()
                inw.sort(key=lambda c: ((c[0]['sf_name'] == stamp), (c[0].get('config') or {}).get('start_date') or ''), reverse=True)
                chosen = inw[0][0]['db_id']; _state['primary'][mid] = chosen
        for e, m2, in_w, nooks in fresh:
            is_p = (e['db_id'] == chosen)
            extra.append({'meeting_id': mid, 'campaign_id': e['db_id'], 'rule': ('nooks_booking' if nooks else 'member_in_window'), 'in_window': in_w, 'is_primary': is_p, 'attributed_at': now, 'sync_run_id': run_id,
                          'note': None if is_p else ('seen in this campaign ledger after its primary attribution' if in_w else 'ledger entry outside the campaign window — the dashboard does not count it here')})
            _state['links'].append(f"{mid}|{e['db_id']}")
    if extra:
        # only rows whose (meeting, campaign) pair is not already stored (unique live-pair index makes duplicates fail harmlessly)
        stats['attributions'] += _insert('meeting_attributions', extra, count_col='id')
    # 2) state refresh for recent meetings
    upd = []
    for mid in refresh:
        lid, mdate = mid.split(':'); r = leads.get(lid)
        if not r: continue
        cur = _ymd(r.get('Meeting_Generated_on__c'))
        if cur and cur != mdate and abs((date.fromisoformat(cur) - date.fromisoformat(mdate)).days) > 14:
            continue   # the lead has been rebooked; the new booking arrives via the ledger as its own row
        st = _state_from_lead(r, mdate, opps)
        h = _hash([st.get('meeting_status'), st.get('lead_status'), st.get('opp_stage'), st.get('opp_amount')])
        if h != _state['meetings'][mid].get('h'):
            if _patch('meetings', {'meeting_id': f'eq.{mid}'}, st):
                _msh(mid, st, now, run_id); _state['meetings'][mid]['h'] = h; stats['meetings_updated'] += 1
                upd.append(_lead_row(r, now))
    _insert('leads', upd, on_conflict='lead_id', prefer='resolution=merge-duplicates')
    # 3) superseding: a lead whose current SFDC booking date is >14d after its latest known meeting → new row will appear via ledger; mark old as superseded when the new one exists
    for mid in new_mids:
        lid, mdate = mid.split(':')
        older = [k for k, s in _state['meetings'].items() if s['lead_id'] == lid and k != mid and s['on'] < mdate and (date.fromisoformat(mdate) - date.fromisoformat(s['on'])).days > 14]
        r = leads.get(lid) or {}
        if older and _ymd(r.get('Meeting_Generated_on__c')) == mdate:
            for k in older:
                _patch('meetings', {'meeting_id': f'eq.{k}', 'superseded_by': 'is.null'}, {'superseded_by': mid})

def _state_from_lead(r, mdate, opps):
    ms, st = r.get('Meeting_Status__c'), r.get('Status')
    o = opps.get(r.get('ConvertedOpportunityId') or '') or {}
    return {'meeting_status': ms, 'lead_status': st, 'is_done': D.is_done(ms, st), 'is_noshow': D.is_noshow(ms), 'is_sql': D.is_sql(ms, st),
            'sql_converted_on': _ymd(r.get('SQL_Converted_Date__c')), 'converted_on': _ymd(r.get('ConvertedDate')), 'converted_opp_id': r.get('ConvertedOpportunityId') or None,
            'opp_stage': o.get('StageName'), 'opp_amount': (float(o['Amount']) if o.get('Amount') not in (None, '') else None), 'state_updated_at': _iso()}

def _msh(mid, st, now, run_id):
    _insert('meeting_status_history', [{'meeting_id': mid, 'observed_at': now, 'meeting_status': st.get('meeting_status'), 'lead_status': st.get('lead_status'), 'opp_stage': st.get('opp_stage'), 'opp_amount': st.get('opp_amount'), 'sync_run_id': run_id}])

# ── step: metric snapshots ───────────────────────────────────────────────────
def _sync_snapshots(run_id, pending, stats):
    by_dash = {e['dashboard_id']: e for e in _state['campaigns'].values() if e.get('dashboard_id')}
    today = _today_ist(); rows = []
    # Prefer the dashboard's FINAL cached row for this campaign: _run_sync applies a
    # zero-guard after campaign_metrics() (keeps old calls/emails when a query returned 0),
    # and the history must show exactly what the dashboard shows.
    shown = {c.get('id'): c for c in ((_deps.get('cache') or {}).get('campaigns') or [])}
    for did, p in pending.items():
        e = by_dash.get(did)
        if not e: continue
        r = dict(p['result'])
        cached = shown.get(did)
        if cached and cached.get('synced_at') == r.get('synced_at'):
            for k in _METRIC_KEYS + ('call_dispositions',):
                if k in cached: r[k] = cached[k]
        h = _metric_hash(r); last = e.get('last_snapshot') or {}
        settled = r.get('settled_date') or (e.get('config') or {}).get('settled_date')
        final_now = bool((e.get('config') or {}).get('status') == 'Completed' and settled and settled < today and not last.get('final'))
        if h == last.get('h') and last.get('as_of') == today and not final_now:
            continue
        kind = 'final' if final_now else ('sync' if h != last.get('h') else 'daily')
        snap_at = r.get('synced_at') or _iso()
        rows.append({'campaign_id': e['db_id'], 'sync_run_id': run_id, 'snapshot_at': snap_at, 'as_of_date': today, 'snapshot_kind': kind, 'definition_version': D.DEFINITION_VERSION, 'is_final': final_now,
                     **{k: r.get(k) for k in _METRIC_KEYS}, 's1_is_manual': bool(r.get('s1_is_manual')), 'call_dispositions': r.get('call_dispositions'), 'sdr_breakdown': r.get('sdr_breakdown'),
                     'status_sdr_breakdown': r.get('status_sdr_breakdown'), 'extras': {'settled_date': settled}})
        e['last_snapshot'] = {'as_of': today, 'final': final_now or bool(last.get('final')), 'h': h}
    stats['snapshots'] += _insert('campaign_metric_snapshots', rows, on_conflict='campaign_id,snapshot_at', count_col='id')

# ── step: reconcile dashboard vs history ─────────────────────────────────────
def _reconcile(run_id, pending, stats):
    by_dash = {e['dashboard_id']: e for e in _state['campaigns'].values() if e.get('dashboard_id')}
    rows = []
    for did, p in pending.items():
        e = by_dash.get(did)
        if not e: continue
        cfg = e.get('config') or {}
        hist = sum(1 for lid, m in p['meetings'].items() if _in_win(m.get('date'), cfg.get('start_date'), cfg.get('end_date')))
        dash = p['result'].get('meetings') or 0
        ok = (hist == dash)
        if not ok: stats['mismatches'] += 1
        rows.append({'sync_run_id': run_id, 'campaign_id': e['db_id'], 'metric': 'meetings', 'dashboard_value': dash, 'history_value': hist, 'matched': ok})
        lh = len(set(p['frozen_ids']) & set(e['leads'])); ld = p['result'].get('total_leads') or 0   # members the history layer knows among the dashboard's frozen ids
        rows.append({'sync_run_id': run_id, 'campaign_id': e['db_id'], 'metric': 'total_leads', 'dashboard_value': ld, 'history_value': lh, 'matched': (lh == ld)})
        if lh != ld: stats['mismatches'] += 1
    _insert('reconciliations', [r for r in rows if not r['matched']] or rows[:0])
    if rows:
        _insert('reconciliations', [{'sync_run_id': run_id, 'campaign_id': None, 'metric': 'summary', 'dashboard_value': len(rows), 'history_value': sum(1 for r in rows if r['matched']), 'matched': all(r['matched'] for r in rows)}])

# ─────────────────────────────────────────────────────────────── init ──
def init_app(app, *, soql, load_campaigns, norm_sdr, data_dir, sf_base_url='', cache=None, require_admin=None):
    global _summary
    _deps.update({'soql': soql, 'load_campaigns': load_campaigns, 'norm_sdr': norm_sdr, 'cache': cache})
    url = os.environ.get('HISTORY_SUPABASE_URL', '').strip().rstrip('/'); key = os.environ.get('HISTORY_SUPABASE_KEY', '').strip()
    _cfg.update({'url': url, 'key': key, 'data_dir': data_dir, 'budget': os.environ.get('HISTORY_TIME_BUDGET_S', '600'), 'max_unreg': os.environ.get('HISTORY_MAX_UNREG_PER_RUN', '20'),
                 'intel_url': os.environ.get('SUPABASE_URL', 'https://gvszpwyajzehqsofxzou.supabase.co').rstrip('/'), 'intel_key': os.environ.get('SUPABASE_SERVICE_KEY', '').strip()})
    _summary['enabled'] = bool(url and key)
    if not _summary['enabled']:
        _summary['text'] = 'history: disabled (HISTORY_SUPABASE_URL/KEY not set)'; _log(_summary['text']); return
    _summary['text'] = 'history: enabled, no flush yet'
    _load_state()
    _log(f'enabled → {url} (definitions v{D.DEFINITION_VERSION} {D.definitions_hash()})')

    @app.route('/api/history/status')
    def api_history_status():
        with _lock: pend = len(_pending)
        qn = 0
        try: qn = sum(1 for _ in open(_queue_path()))
        except Exception: pass
        return {'enabled': True, 'url': url, 'summary': _summary, 'pending_campaigns': pend, 'queued_batches': qn, 'last_ok_at': _last_ok_at,
                'state_campaigns': len((_state or {}).get('campaigns', {})), 'definitions_version': D.DEFINITION_VERSION, 'definitions_hash': D.definitions_hash()}

    if require_admin:
        @app.route('/api/history/flush', methods=['POST'])
        @require_admin
        def api_history_flush():
            threading.Thread(target=flush, kwargs={'app_version': 'manual'}, daemon=True).start()
            return {'started': True}
