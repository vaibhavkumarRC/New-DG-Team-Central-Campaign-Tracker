"""Quarter close for the history layer (P1 part 4).

On the first sync after a quarter ends, freeze one snapshot per campaign of that quarter
(campaign_metric_snapshots.snapshot_kind = 'quarter_close', as_of_date = quarter end, values =
exactly what the dashboard shows at that moment) and record the close in quarter_closes.

Rules
  • A campaign's quarter = the quarter of its start_date (campaigns.quarter in the DB).
  • The step looks at the most recently ENDED quarter (IST calendar). If quarter_closes has a row
    for it, nothing happens (one REST read per sync, cached in the state file afterwards).
  • Values come from the dashboard's cached metrics (post zero-guard, the same row the daily
    snapshot step copies). A campaign missing from the cache (deleted on the dashboard) gets its
    latest stored snapshot instead, and the extras say so.
  • Idempotent: a crash mid-way leaves no ledger row, so the next sync resumes and writes only the
    campaigns that have no quarter_close snapshot for that quarter yet.
  • Changes NO dashboard number and NO UI. Writes only the history project.
"""
from datetime import date, datetime, timedelta, timezone

import definitions as D

IST = timezone(timedelta(hours=5, minutes=30))
status = {'last_closed': None, 'due': None, 'closed_this_run': None, 'error': None, 'checked_at': None}

def quarter_of(d):
    """'2026-09-19' → '2026-Q3' (same rule as campaigns.quarter)."""
    d = date.fromisoformat(str(d)[:10]); return f'{d.year}-Q{(d.month - 1) // 3 + 1}'

def quarter_end(q):
    y, n = q.split('-Q'); m = int(n) * 3
    nxt = date(int(y) + (m == 12), 1 if m == 12 else m + 1, 1)
    return nxt - timedelta(days=1)

def last_ended_quarter(today):
    """The most recent quarter whose end date is strictly before `today`."""
    q = quarter_of(today)
    y, n = q.split('-Q'); y, n = int(y), int(n)
    return f'{y}-Q{n - 1}' if n > 1 else f'{y - 1}-Q4'

def run(run_id, stats, H, today=None):
    """Flush step. Never raises."""
    today = today or datetime.now(IST).date()
    status.update({'error': None, 'closed_this_run': None, 'checked_at': today.isoformat()})
    stats.setdefault('quarter_close', None)
    try:
        q = last_ended_quarter(today)
        status['due'] = q
        closed = (H._state or {}).setdefault('quarter_closed', [])
        if q in closed:
            status['last_closed'] = q; return
        row = H._http('GET', 'quarter_closes', {'select': 'quarter,closed_at', 'quarter': f'eq.{q}'})
        if row:
            closed.append(q); status['last_closed'] = q; return
        res = close_quarter(H, run_id, q, today)
        closed.append(q); status['last_closed'] = q; status['closed_this_run'] = res; stats['quarter_close'] = res
        H._dq(run_id, 'quarter_closed', 'info', 'quarter', q, f"{q} closed: {res['campaigns']} campaigns, {res['snapshots_written']} snapshots written, {res['settled_campaigns']} settled", stats)
    except Exception as e:
        status['error'] = f'{type(e).__name__}: {H._short(e) if hasattr(H, "_short") else str(e)[:300]}'
        H._err(f'quarter close: {status["error"]}')

def plan(H, q, today):
    """Rows that a close of quarter q would write (no writes). Used by run() and by dry runs."""
    q_end = quarter_end(q)
    camps = [e for e in (H._state or {}).get('campaigns', {}).values()
             if not e.get('deleted') and (e.get('config') or {}).get('start_date') and quarter_of(e['config']['start_date']) == q]
    shown = {c.get('id'): c for c in ((H._deps.get('cache') or {}).get('campaigns') or [])}
    have = {s['campaign_id'] for s in H._get_all('campaign_metric_snapshots', {'select': 'campaign_id', 'snapshot_kind': 'eq.quarter_close', 'extras->>quarter': f'eq.{q}'}, order='id.asc')}
    rows, missing_cache, settled = [], [], 0
    now = datetime.now(timezone.utc).isoformat()
    for e in camps:
        cfg = e.get('config') or {}
        sd = cfg.get('settled_date')
        is_settled = bool(cfg.get('status') == 'Completed' and sd and str(sd) < today.isoformat())
        settled += is_settled
        if e['db_id'] in have: continue
        src = shown.get(e.get('dashboard_id'))
        if src:
            m = {k: src.get(k) for k in H._METRIC_KEYS}; extra_src = 'dashboard_cache'
            dispo, sdr, ssdr, manual = src.get('call_dispositions'), src.get('sdr_breakdown'), src.get('status_sdr_breakdown'), bool(src.get('s1_is_manual'))
        else:
            last = H._http('GET', 'v_campaign_latest_snapshot', {'select': ','.join(H._METRIC_KEYS) + ',s1_is_manual,call_dispositions,sdr_breakdown,status_sdr_breakdown', 'campaign_id': f"eq.{e['db_id']}"})
            if not last: missing_cache.append(e['sf_name']); continue
            m = {k: last[0].get(k) for k in H._METRIC_KEYS}; extra_src = 'latest_snapshot'
            dispo, sdr, ssdr, manual = last[0].get('call_dispositions'), last[0].get('sdr_breakdown'), last[0].get('status_sdr_breakdown'), bool(last[0].get('s1_is_manual'))
        rows.append({'campaign_id': e['db_id'], 'snapshot_at': now, 'as_of_date': q_end.isoformat(), 'snapshot_kind': 'quarter_close',
                     'definition_version': D.DEFINITION_VERSION, 'is_final': is_settled, **m, 's1_is_manual': manual,
                     'call_dispositions': dispo, 'sdr_breakdown': sdr, 'status_sdr_breakdown': ssdr,
                     'extras': {'quarter': q, 'settled_at_close': is_settled, 'settled_date': sd, 'source': extra_src, 'closed_on': today.isoformat()}})
    return {'quarter': q, 'quarter_end': q_end.isoformat(), 'campaigns': len(camps), 'already_written': len(have), 'settled_campaigns': settled, 'rows': rows, 'missing': missing_cache}

def close_quarter(H, run_id, q, today, notes=None, retroactive=False):
    p = plan(H, q, today)
    for r in p['rows']: r['sync_run_id'] = run_id
    n = H._insert('campaign_metric_snapshots', p['rows'], on_conflict='campaign_id,snapshot_at', count_col='id') if p['rows'] else 0
    if p['missing']:
        H._err(f"quarter close {q}: {len(p['missing'])} campaign(s) have neither a cached row nor a stored snapshot: {', '.join(p['missing'][:5])}")
    res = {'quarter': q, 'quarter_end': p['quarter_end'], 'campaigns': p['campaigns'], 'snapshots_written': n + p['already_written'], 'settled_campaigns': p['settled_campaigns'], 'missing': len(p['missing'])}
    H._insert('quarter_closes', [{'quarter': q, 'quarter_end': p['quarter_end'], 'sync_run_id': run_id, 'campaigns': p['campaigns'], 'snapshots_written': res['snapshots_written'],
                                  'settled_campaigns': p['settled_campaigns'], 'retroactive': retroactive, 'definition_version': D.DEFINITION_VERSION,
                                  'notes': notes or f"closed on {today.isoformat()} by sync run {run_id}; values = dashboard cache at close" + (f"; {len(p['missing'])} campaign(s) without values" if p['missing'] else '')}], on_conflict='quarter')
    return res

def summary():
    st = status
    if st.get('error'): return f"quarter close ERROR {st['error'][:100]}"
    if st.get('closed_this_run'):
        r = st['closed_this_run']; return f"QUARTER {r['quarter']} CLOSED: {r['campaigns']} campaigns, {r['snapshots_written']} snapshots, {r['settled_campaigns']} settled"
    return None
