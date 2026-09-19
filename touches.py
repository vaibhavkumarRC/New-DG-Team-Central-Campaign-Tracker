"""Touches ingestion for the history layer (P1 part 1, incremental).

Runs as the LAST step of history.flush(): pulls Salesforce Tasks created since the stored
watermark, classifies them with definitions.classify_touch (the same function the 19 Sep
backfill used), writes them to dg-campaign-history and lets the database attribute them to
campaigns and refresh the per-lead rollup (SQL functions attribute_touches /
refresh_campaign_lead_activity_for_touches, migrations 0007 + 0009).

It changes NO dashboard number and NO UI: it only reads Salesforce and writes the history
project. Everything is idempotent (touch_id primary key, ignore-duplicates; attribution and
rollup are pure re-evaluations), so overlapping or repeated runs are harmless.

Watermark
  touch_ingest_state (id = 1) holds max(Task.CreatedDate) ingested. Each run fetches
  CreatedDate > watermark - OVERLAP (late writes), ordered ascending, capped per run; the
  watermark advances only over slices that were fully processed, so a budget stop or an
  error resumes exactly where it left off next sync. No watermark → no ingest (never guess).

Backlog (kept in history state so a skipped step never loses it)
  • members: leads newly enrolled by this sync → their EXISTING touches inside the campaign
    window are attributed now (the dashboard counts them the same way).
  • windows: a campaign whose start/end changed → every touch of its members in the old ∪ new
    window is re-evaluated (primary moves are superseded, never rewritten).
"""
import time
from datetime import datetime, timedelta, timezone

import definitions as D

TASK_FIELDS = ("Id, Subject, TaskSubtype, Type, WhoId, OwnerId, Owner.Name, CreatedDate, ActivityDate, "
               "CallDisposition, CallDurationInSeconds, CallType, Not_Relevant__c")
OVERLAP = timedelta(hours=2)
SLICE = 4000            # Task rows per insert+attribute slice
RPC_CHUNK = 2000        # touch ids per attribute_touches call
IN_CHUNK = 150          # ids per PostgREST `in.(...)` filter (URL length)
IST = timezone(timedelta(hours=5, minutes=30))

# health / status (read by health.py and /api/history/status)
status = {'last_ok_at': None, 'watermark': None, 'error': None, 'backlog': False, 'new': 0, 'seen': 0, 'skipped': None, 'attributed': 0, 'superseded': 0}

def _now(): return datetime.now(timezone.utc)
def _iso(dt=None): return (dt or _now()).isoformat()
def _parse(ts):
    """Salesforce/PostgREST timestamps → aware datetime (UTC)."""
    s = str(ts).strip().replace('Z', '+00:00')
    if s.endswith('+0000'): s = s[:-5] + '+00:00'
    if len(s) > 6 and s[-3] != ':' and s[-5] in '+-': s = s[:-2] + ':' + s[-2:]
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
def _sf_ts(dt): return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
def _norm_ts(ts): return _sf_ts(_parse(ts)) if ts else None
def _ist_day(ts):
    try: return _parse(ts).astimezone(IST).date().isoformat()
    except Exception: return None
def _chunks(xs, n):
    for i in range(0, len(xs), n): yield xs[i:i + n]

# ── backlog hooks (called by history.py) ─────────────────────────────────────
def note_members(state, campaign_db_id, lead_ids):
    if not lead_ids: return
    state.setdefault('touch_backlog', {}).setdefault('members', []).append({'campaign_id': campaign_db_id, 'lead_ids': list(lead_ids)})

def note_window_change(state, campaign_db_id, old_start, old_end, new_start, new_end):
    state.setdefault('touch_backlog', {}).setdefault('windows', []).append(
        {'campaign_id': campaign_db_id, 'starts': [d for d in (old_start, new_start) if d], 'ends': [d for d in (old_end, new_end) if d]})

# ── the step ─────────────────────────────────────────────────────────────────
def ingest(run_id, stats, H, deadline=None):
    """H = the history module (REST helpers, deps, state). Never raises."""
    t0 = time.time()
    budget = float(H._cfg.get('touch_budget', 240))
    deadline = min(deadline or 1e18, t0 + budget)
    cap = int(H._cfg.get('touch_max_rows', 20000))
    status.update({'error': None, 'backlog': False, 'new': 0, 'seen': 0, 'skipped': None, 'attributed': 0, 'superseded': 0})
    for k in ('touches_seen', 'touches_new', 'touches_attributed', 'touches_superseded', 'touches_rollup'): stats.setdefault(k, 0)
    try:
        _ingest(run_id, stats, H, deadline, cap)
    except Exception as e:
        detail = H._short(e) if hasattr(H, '_short') else str(e)[:300]
        where = getattr(e, 'url', None) or getattr(getattr(e, 'args', [None])[0] if e.args else None, 'url', None)
        status['error'] = f'{type(e).__name__}: {detail}' + (f' @ {str(where)[:160]}' if where else '')
        H._err(f'touches: {status["error"]}')

def _ingest(run_id, stats, H, deadline, cap):
    row = H._http('GET', 'touch_ingest_state', {'select': 'watermark_created,updated_at', 'id': 'eq.1'})
    if not row or not row[0].get('watermark_created'):
        raise RuntimeError('no watermark in touch_ingest_state (backfill first) — ingest skipped')
    wm = _parse(row[0]['watermark_created'])
    since = _sf_ts(wm - OVERLAP)
    res = H._deps['soql'](f"SELECT {TASK_FIELDS} FROM Task WHERE CreatedDate > {since} ORDER BY CreatedDate ASC LIMIT {cap}", paginate=True)
    if res is None:
        raise RuntimeError('Salesforce Task query failed (soql returned None)')
    recs = res.get('records') or []
    status['seen'] = stats['touches_seen'] = len(recs)
    status['backlog'] = len(recs) >= cap
    people = _people(H)
    touched = []; new_wm = wm; processed = 0
    for sl in _chunks(recs, SLICE):
        if time.time() > deadline:
            status['skipped'] = f'budget: {len(recs) - processed} of {len(recs)} fetched rows left for next sync'; break
        rows, ids = _build_rows(sl, H, people)
        n_err = len(H._errors)
        inserted = H._insert('touches', rows, on_conflict='touch_id', returning=True, count_col='touch_id')
        if len(H._errors) > n_err:        # a batch failed and was queued → stop here; the watermark stays before this slice
            raise RuntimeError('a touches insert batch failed (queued for replay); watermark not advanced past this slice')
        n_new = len(inserted or []); stats['touches_new'] += n_new; status['new'] += n_new
        # attribute EVERY id of the slice (idempotent) — a previous run may have inserted them and died before attributing
        touched += _attribute(ids, run_id, stats, H)
        slice_max = max((_parse(r['CreatedDate']) for r in sl if r.get('CreatedDate')), default=new_wm)
        new_wm = max(new_wm, slice_max); processed += len(sl)
        _set_watermark(H, new_wm, run_id)
    # backlog (new members / window edits) — after the new rows, so their attributions see them
    if time.time() < deadline:
        touched += _process_backlog(run_id, stats, H, deadline)
    # per-lead rollup, scoped to the (campaign, lead) pairs behind the touches this run attributed
    for ch in _chunks(list(dict.fromkeys(touched)), RPC_CHUNK):
        r = H._http('POST', 'rpc/refresh_campaign_lead_activity_for_touches', None, {'p_touch_ids': ch, 'p_run_id': run_id}, timeout=180)
        if r: stats['touches_rollup'] += int(r[0].get('rows_upserted') or 0) + int(r[0].get('rows_deleted') or 0)
    if status['skipped'] is None:
        _set_watermark(H, new_wm, run_id)          # also stamps updated_at on a 0-row run (proves the step ran)
    status['last_ok_at'] = _iso(); status['watermark'] = _sf_ts(new_wm)

def _set_watermark(H, wm, run_id):
    H._http('POST', 'touch_ingest_state', {'on_conflict': 'id'}, [{'id': 1, 'watermark_created': _sf_ts(wm), 'last_run_id': run_id, 'updated_at': _iso()}],
            prefer='resolution=merge-duplicates,return=minimal')

def _people(H):
    out = {}
    try:
        for p in H._http('GET', 'people', {'select': 'person_key,display_name,aliases', 'limit': '1000'}):
            for a in (p.get('aliases') or []) + [p.get('display_name') or '']:
                if a.strip(): out[a.strip().lower()] = p['person_key']
    except Exception as e:
        H._err(f'touches: people lookup failed ({H._short(e)}); owner_person_key left null this run')
    return out

def _build_rows(recs, H, people):
    """Classify + resolve one slice. Returns (rows to insert, ALL touch ids of the slice that are outreach)."""
    pre = []
    for r in recs:
        who = r.get('WhoId') or None
        if not who or r.get('Not_Relevant__c'): continue
        cl = D.classify_touch(r.get('Subject'), r.get('TaskSubtype'), r.get('Type'))
        if cl is None: continue
        pre.append((r, who, cl))
    contact_ids = sorted({who for _, who, _ in pre if who.startswith('003')})
    lead_by_contact = {}
    for ch in _chunks(contact_ids, IN_CHUNK):
        for l in H._http('GET', 'leads', {'select': 'lead_id,contact_id', 'contact_id': f'in.({",".join(ch)})', 'limit': '1000'}):
            lead_by_contact[l['contact_id']] = l['lead_id']
    lead_ids = sorted({(who if who.startswith('00Q') else lead_by_contact.get(who)) for _, who, _ in pre} - {None})
    acct_of = {}
    for ch in _chunks(lead_ids, IN_CHUNK):
        for l in H._http('GET', 'leads', {'select': 'lead_id,current_account_sf_id,converted_account_id', 'lead_id': f'in.({",".join(ch)})', 'limit': '1000'}):
            acct_of[l['lead_id']] = l.get('current_account_sf_id') or l.get('converted_account_id')
    rows, ids = [], []
    for r, who, (channel, tool, event) in pre:
        who_type = 'lead' if who.startswith('00Q') else 'contact' if who.startswith('003') else None
        lead_id = who if who_type == 'lead' else lead_by_contact.get(who)
        disp = r.get('CallDisposition') or None
        dur = int(r['CallDurationInSeconds']) if r.get('CallDurationInSeconds') not in (None, '') else None
        cards = D.dashboard_cards(r.get('Subject'))
        counted = bool(cards) and bool(r.get('ActivityDate'))   # the cards filter on ActivityDate; a null can never be counted
        is_connect, is_conv, is_booking = D.touch_flags(channel, counted, r.get('Subject'), disp, dur)
        owner = ((r.get('Owner') or {}).get('Name') or '').strip()
        ist_day = _ist_day(r.get('CreatedDate'))
        on = r.get('ActivityDate') or ist_day
        row = {'touch_id': r['Id'], 'occurred_at': _norm_ts(r.get('CreatedDate')), 'occurred_on': on, 'occurred_on_ist': ist_day,
               'who_id': who, 'who_type': who_type, 'lead_id': lead_id, 'account_sf_id': acct_of.get(lead_id) if lead_id else None,
               'owner_id': r.get('OwnerId') or None, 'owner_name': owner or None, 'owner_person_key': people.get(owner.lower()),
               'channel': channel, 'tool': tool, 'event': event, 'disposition': disp, 'duration_s': dur, 'call_type': r.get('CallType') or None,
               'task_subtype': r.get('TaskSubtype') or None, 'subject': (r.get('Subject') or '')[:200], 'is_connect': is_connect,
               'is_conversation': is_conv, 'is_booking': is_booking, 'is_counted_by_dashboard': counted, 'dashboard_cards': cards,
               'definition_version': D.DEFINITION_VERSION}
        rows.append(row); ids.append(r['Id'])
    return rows, ids

def _attribute(ids, run_id, stats, H):
    """Re-evaluate attribution for these touch ids (idempotent). Returns the ids, for the rollup."""
    ids = list(dict.fromkeys(ids))
    for ch in _chunks(ids, RPC_CHUNK):
        r = H._http('POST', 'rpc/attribute_touches', None, {'p_touch_ids': ch, 'p_run_id': run_id}, timeout=180)
        if not r: continue
        r = r[0]
        stats['touches_attributed'] += int(r.get('inserted_primary') or 0) + int(r.get('inserted_context') or 0)
        stats['touches_superseded'] += int(r.get('superseded') or 0)
        status['attributed'] += int(r.get('inserted_primary') or 0); status['superseded'] += int(r.get('superseded') or 0)
    return ids

def _process_backlog(run_id, stats, H, deadline):
    """Existing touches that a new membership or a window edit makes (in)eligible."""
    bl = (H._state or {}).get('touch_backlog') or {}
    touched = []
    by_camp = {e['db_id']: e for e in (H._state or {}).get('campaigns', {}).values()}
    for item in list(bl.get('members') or []):
        if time.time() > deadline: return touched
        e = by_camp.get(item['campaign_id']); cfg = (e or {}).get('config') or {}
        ids = _touch_ids_for(H, item['lead_ids'], cfg.get('start_date'), cfg.get('end_date'))
        touched += _attribute(ids, run_id, stats, H)
        bl['members'].remove(item)
    for item in list(bl.get('windows') or []):
        if time.time() > deadline: return touched
        e = by_camp.get(item['campaign_id'])
        if not e: bl['windows'].remove(item); continue
        ids = _touch_ids_for(H, e.get('leads') or [], min(item['starts'] or ['0001-01-01']), max(item['ends'] or ['9999-12-31']))
        ids += [a['touch_id'] for a in H._get_all('touch_attributions', {'select': 'touch_id', 'campaign_id': f"eq.{item['campaign_id']}", 'superseded_at': 'is.null'}, order='id.asc')]
        touched += _attribute(ids, run_id, stats, H)
        bl['windows'].remove(item)
    return touched

def _touch_ids_for(H, lead_ids, start, end):
    out = []
    for ch in _chunks(sorted(set(lead_ids)), IN_CHUNK):
        p = {'select': 'touch_id', 'lead_id': f'in.({",".join(ch)})'}
        if start: p['occurred_on'] = f'gte.{start}'
        if end: p['and'] = f'(occurred_on.lte.{end})'
        out += [r['touch_id'] for r in H._get_all('touches', p, order='touch_id.asc')]
    return out
