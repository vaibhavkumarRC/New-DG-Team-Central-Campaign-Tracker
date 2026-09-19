"""Lead journey ingestion for the history layer (P1 part 2, incremental).

Runs after the touches step of history.flush(). Copies Salesforce LeadHistory rows for the
journey fields (status, campaign stamp, meeting/SQL fields, DNC, POD, merges, conversions …)
into dg-campaign-history permanently — Salesforce keeps field history for a limited time.

Also, each run:
  • applies the rows' effects to the leads dimension (created_at_sfdc; SQL apply_lead_history_effects),
  • resolves merges: leads deleted by a merge sit in the recycle bin for 15 days with MasterRecordId
    = survivor (queryAll); the loser gets leads.merged_into_lead_id + is_deleted,
  • re-evaluates touch attribution ties for leads whose Campaign__c changed (touch_stamp_at now
    knows the stamp on the day of each touch).

Same watermark discipline as touches.py: CreatedDate > watermark − 2 h, ascending, capped, slices
committed one at a time, never guesses without a watermark. Changes NO dashboard number and NO UI.
"""
import time
from datetime import datetime, timedelta, timezone

FIELDS = ('created', 'Status', 'Campaign__c', 'POD__c', 'DoNotCall', 'RC_Account_ID__c', 'Revenue_Bucket__c', 'LeadSource',
          'Meeting_Status__c', 'Meeting_Generated_by__c', 'Meeting_Generated_on__c', 'Meeting_Scheduled_on__c', 'Meeting_Outcome__c',
          'MeetingScheduled__c', 'Seller_Name__c', 'SQL_Seller_Owner__c', 'SQL_Source__c', 'SQL_Converted_Date__c',
          'Zoom_Meeting_Link_URL__c', 'leadMerged', 'leadConverted', 'ownerAssignment')
OVERLAP = timedelta(hours=2)
SLICE = 4000
IN_CHUNK = 150
IST = timezone(timedelta(hours=5, minutes=30))

status = {'last_ok_at': None, 'watermark': None, 'error': None, 'backlog': False, 'new': 0, 'seen': 0, 'skipped': None, 'merges': 0, 'ties_reevaluated': 0}

def _now(): return datetime.now(timezone.utc)
def _iso(dt=None): return (dt or _now()).isoformat()
def _parse(ts):
    s = str(ts).strip().replace('Z', '+00:00')
    if s.endswith('+0000'): s = s[:-5] + '+00:00'
    if len(s) > 6 and s[-3] != ':' and s[-5] in '+-': s = s[:-2] + ':' + s[-2:]
    dt = datetime.fromisoformat(s); return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
def _sf_ts(dt): return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
def _chunks(xs, n):
    for i in range(0, len(xs), n): yield xs[i:i + n]

def ingest(run_id, stats, H, deadline=None):
    """Flush step. Never raises."""
    t0 = time.time(); budget = float(H._cfg.get('leadhist_budget', 180)); deadline = min(deadline or 1e18, t0 + budget)
    cap = int(H._cfg.get('leadhist_max_rows', 20000))
    status.update({'error': None, 'backlog': False, 'new': 0, 'seen': 0, 'skipped': None, 'merges': 0, 'ties_reevaluated': 0})
    for k in ('leadhist_seen', 'leadhist_new', 'leadhist_merges', 'leadhist_ties'): stats.setdefault(k, 0)
    try:
        _ingest(run_id, stats, H, deadline, cap)
    except Exception as e:
        detail = H._short(e) if hasattr(H, '_short') else str(e)[:300]
        status['error'] = f'{type(e).__name__}: {detail}'; H._err(f'lead history: {status["error"]}')

def _ingest(run_id, stats, H, deadline, cap):
    row = H._http('GET', 'lead_history_ingest_state', {'select': 'watermark_created', 'id': 'eq.1'})
    if not row or not row[0].get('watermark_created'):
        raise RuntimeError('no watermark in lead_history_ingest_state (backfill first) — ingest skipped')
    wm = _parse(row[0]['watermark_created']); since = _sf_ts(wm - OVERLAP)
    fields = ','.join(f"'{f}'" for f in FIELDS)
    res = H._deps['soql'](f"SELECT Id, LeadId, Field, OldValue, NewValue, CreatedDate, CreatedById FROM LeadHistory WHERE CreatedDate > {since} AND Field IN ({fields}) ORDER BY CreatedDate ASC LIMIT {cap}", paginate=True)
    if res is None: raise RuntimeError('Salesforce LeadHistory query failed (soql returned None)')
    recs = res.get('records') or []
    status['seen'] = stats['leadhist_seen'] = len(recs); status['backlog'] = len(recs) >= cap
    users = _users(H, {r.get('CreatedById') for r in recs if r.get('CreatedById')})
    new_wm = wm; processed = 0; new_ids = []; stamp_leads = set()
    for sl in _chunks(recs, SLICE):
        if time.time() > deadline:
            status['skipped'] = f'budget: {len(recs) - processed} of {len(recs)} fetched rows left for next sync'; break
        rows = [{'history_id': r['Id'], 'lead_id': r['LeadId'], 'field': r['Field'], 'old_value': r.get('OldValue'), 'new_value': r.get('NewValue'),
                 'changed_at': _sf_ts(_parse(r['CreatedDate'])), 'changed_by_id': r.get('CreatedById'), 'changed_by_name': users.get(r.get('CreatedById')),
                 'source': 'leadhistory', 'sync_run_id': run_id} for r in sl]
        n_err = len(H._errors)
        inserted = H._insert('lead_field_changes', rows, on_conflict='history_id', returning=True, count_col='history_id')
        if len(H._errors) > n_err:
            raise RuntimeError('a lead_field_changes insert batch failed (queued for replay); watermark not advanced past this slice')
        ids = [r['history_id'] for r in (inserted or [])]
        new_ids += ids; stats['leadhist_new'] += len(ids); status['new'] += len(ids)
        stamp_leads |= {r['LeadId'] for r in sl if r['Field'] == 'Campaign__c'}
        if ids: H._http('POST', 'rpc/apply_lead_history_effects', None, {'p_history_ids': ids}, timeout=180)
        slice_max = max((_parse(r['CreatedDate']) for r in sl if r.get('CreatedDate')), default=new_wm)
        new_wm = max(new_wm, slice_max); processed += len(sl)
        _set_watermark(H, new_wm, run_id)
    # merges: recycle-bin leads with a survivor, modified since the watermark (15-day bin → an outage never loses one)
    if time.time() < deadline:
        status['merges'] = stats['leadhist_merges'] = _resolve_merges(H, since)
    # attribution ties for leads whose stamp changed this run
    if stamp_leads and time.time() < deadline:
        status['ties_reevaluated'] = stats['leadhist_ties'] = _reevaluate_ties(H, run_id, sorted(stamp_leads))
    if status['skipped'] is None: _set_watermark(H, new_wm, run_id)
    status['last_ok_at'] = _iso(); status['watermark'] = _sf_ts(new_wm)

def _set_watermark(H, wm, run_id):
    H._http('POST', 'lead_history_ingest_state', {'on_conflict': 'id'}, [{'id': 1, 'watermark_created': _sf_ts(wm), 'last_run_id': run_id, 'updated_at': _iso()}],
            prefer='resolution=merge-duplicates,return=minimal')

def _users(H, ids):
    """User Id → Name, cached in the history state (users rarely change)."""
    cache = (H._state or {}).setdefault('sf_users', {})
    missing = sorted(i for i in ids if i and i not in cache)
    for ch in _chunks(missing, 200):
        res = H._deps['soql']("SELECT Id, Name FROM User WHERE Id IN (" + ','.join(f"'{i}'" for i in ch) + ")", paginate=False)
        for u in (res or {}).get('records', []): cache[u['Id']] = u.get('Name')
        for i in ch: cache.setdefault(i, None)
    return cache

def _resolve_merges(H, since):
    res = H._deps['soql'](f"SELECT Id, MasterRecordId, LastModifiedDate FROM Lead WHERE IsDeleted = true AND MasterRecordId != null AND LastModifiedDate > {since}", paginate=True, all_rows=True)
    pairs = [(r['Id'], r['MasterRecordId']) for r in ((res or {}).get('records') or []) if r.get('MasterRecordId')]
    if not pairs: return 0
    n = 0
    for loser, survivor in pairs:
        existing = H._http('GET', 'leads', {'select': 'lead_id,merged_into_lead_id', 'lead_id': f'eq.{loser}'})
        if not existing: continue                    # never a member of any campaign → nothing to re-point
        if existing[0].get('merged_into_lead_id') == survivor: continue
        if H._patch('leads', {'lead_id': f'eq.{loser}'}, {'merged_into_lead_id': survivor, 'is_deleted': True, 'last_seen_at': _iso()}): n += 1
    return n

def _reevaluate_ties(H, run_id, lead_ids):
    """Touches of these leads with ≥ 2 eligible in-window memberships → one SQL call per 2,000 leads."""
    n = 0
    for ch in _chunks(lead_ids, 2000):
        r = H._http('POST', 'rpc/reevaluate_ties_for_leads', None, {'p_lead_ids': ch, 'p_run_id': run_id}, timeout=180)
        if r: n += int(r[0].get('touches_reevaluated') or 0)
    return n
