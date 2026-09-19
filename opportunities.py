"""Opportunities for the history layer (P1 part 3, incremental). Salesforce is READ-ONLY here.

Runs after the lead-history step of history.flush():
  1. Opportunities modified since the watermark (LastModifiedDate > wm − 2 h) → upsert `opportunities`
     (full current state; the converting lead is looked up from Lead.ConvertedOpportunityId).
  2. OpportunityHistory rows created since the watermark → `opportunity_stage_history` (permanent).
  3. attribute_opportunities(<ids touched this run>) → meeting + campaign attribution (SQL, migration 0014).
The object is small (hundreds of rows), so a missing watermark means "load everything" — the only
step where that is safe. Changes NO dashboard number and NO UI.
"""
import time
from datetime import datetime, timedelta, timezone
from lead_history import _parse, _sf_ts, _chunks, _users, _iso

OPP_FIELDS = ("Id, Name, AccountId, StageName, Amount, Annual_Contract_Value__c, Proposal_Amount__c, Probability, CreatedDate, CloseDate, "
              "IsClosed, IsWon, Owner.Name, SDR_Owner__c, Source__c, LeadSource, Type, Organization_Type__c, Loss_Reason__c, "
              "Loss_Reason_Explanation__c, Nurture_Reason__c, Next_Steps__c, Competitor_Selected__c, Contract_Sign_Date__c, "
              "Subscription_Start_Date__c, LastStageChangeDate, LastModifiedDate")
OVERLAP = timedelta(hours=2)
status = {'last_ok_at': None, 'watermark': None, 'error': None, 'new': 0, 'updated': 0, 'history_new': 0, 'attributed': 0, 'superseded': 0, 'unattributed': 0, 'skipped': None}

def _num(v):
    try: return float(v) if v not in (None, '') else None
    except Exception: return None
def _d(v): return (v or '')[:10] or None

def ingest(run_id, stats, H, deadline=None):
    t0 = time.time(); budget = float(H._cfg.get('opp_budget', 120)); deadline = min(deadline or 1e18, t0 + budget)
    status.update({'error': None, 'new': 0, 'updated': 0, 'history_new': 0, 'attributed': 0, 'superseded': 0, 'unattributed': 0, 'skipped': None})
    for k in ('opps_seen', 'opps_new', 'opps_history_new', 'opps_attributed', 'opps_superseded', 'opps_unattributed'): stats.setdefault(k, 0)
    try:
        _ingest(run_id, stats, H, deadline)
    except Exception as e:
        detail = H._short(e) if hasattr(H, '_short') else str(e)[:300]
        status['error'] = f'{type(e).__name__}: {detail}'; H._err(f'opportunities: {status["error"]}')

def _ingest(run_id, stats, H, deadline):
    st = (H._http('GET', 'opp_ingest_state', {'select': 'watermark_modified,watermark_history', 'id': 'eq.1'}) or [{}])[0]
    wm_o = _parse(st['watermark_modified']) if st.get('watermark_modified') else None
    wm_h = _parse(st['watermark_history']) if st.get('watermark_history') else None
    # 1) opportunities
    where = f" WHERE LastModifiedDate > {_sf_ts(wm_o - OVERLAP)}" if wm_o else ''
    res = H._deps['soql'](f"SELECT {OPP_FIELDS} FROM Opportunity{where} ORDER BY LastModifiedDate ASC", paginate=True)
    if res is None: raise RuntimeError('Salesforce Opportunity query failed (soql returned None)')
    recs = res.get('records') or []; stats['opps_seen'] = len(recs)
    ids = [r['Id'] for r in recs]
    conv = {}
    for ch in _chunks(ids, 200):
        lr = H._deps['soql']("SELECT Id, ConvertedOpportunityId FROM Lead WHERE ConvertedOpportunityId IN (" + ','.join(f"'{i}'" for i in ch) + ")", paginate=False)
        for l in (lr or {}).get('records', []): conv.setdefault(l['ConvertedOpportunityId'], l['Id'])
    existing = set()
    for ch in _chunks(ids, 150):
        existing |= {r['opp_id'] for r in H._http('GET', 'opportunities', {'select': 'opp_id', 'opp_id': f'in.({",".join(ch)})'})}
    now = _iso(); rows = []
    for r in recs:
        rows.append({'opp_id': r['Id'], 'name': r.get('Name'), 'account_sf_id': r.get('AccountId'), 'stage': r.get('StageName'), 'amount': _num(r.get('Amount')),
                     'annual_contract_value': _num(r.get('Annual_Contract_Value__c')), 'proposal_amount': _num(r.get('Proposal_Amount__c')), 'probability': _num(r.get('Probability')),
                     'created_on': _d(r.get('CreatedDate')), 'created_at': _sf_ts(_parse(r['CreatedDate'])) if r.get('CreatedDate') else None, 'close_date': _d(r.get('CloseDate')),
                     'is_closed': bool(r.get('IsClosed')), 'is_won': bool(r.get('IsWon')), 'owner_name': (r.get('Owner') or {}).get('Name'), 'sdr_owner': r.get('SDR_Owner__c'),
                     'source': r.get('Source__c'), 'lead_source': r.get('LeadSource'), 'opp_type': r.get('Type'), 'organization_type': r.get('Organization_Type__c'),
                     'loss_reason': r.get('Loss_Reason__c'), 'loss_reason_detail': r.get('Loss_Reason_Explanation__c'), 'nurture_reason': r.get('Nurture_Reason__c'),
                     'next_steps': r.get('Next_Steps__c'), 'competitor': r.get('Competitor_Selected__c'), 'contract_sign_date': _d(r.get('Contract_Sign_Date__c')),
                     'subscription_start': _d(r.get('Subscription_Start_Date__c')), 'converted_from_lead_id': conv.get(r['Id']),
                     'last_stage_change_at': _sf_ts(_parse(r['LastStageChangeDate'])) if r.get('LastStageChangeDate') else None,
                     'sfdc_last_modified_at': _sf_ts(_parse(r['LastModifiedDate'])) if r.get('LastModifiedDate') else None, 'last_seen_at': now, 'sync_run_id': run_id})
    n_err = len(H._errors)
    if rows: H._insert('opportunities', rows, on_conflict='opp_id', prefer='resolution=merge-duplicates')
    if len(H._errors) > n_err: raise RuntimeError('an opportunities upsert batch failed (queued); watermarks not advanced')
    status['new'] = stats['opps_new'] = len([i for i in ids if i not in existing]); status['updated'] = len(existing)
    new_wm_o = max([wm_o] * bool(wm_o) + [_parse(r['LastModifiedDate']) for r in recs if r.get('LastModifiedDate')], default=wm_o)
    # 2) stage history
    where_h = f" WHERE CreatedDate > {_sf_ts(wm_h - OVERLAP)}" if wm_h else ''
    hres = H._deps['soql'](f"SELECT Id, OpportunityId, StageName, Amount, CloseDate, Probability, CreatedDate, CreatedById FROM OpportunityHistory{where_h} ORDER BY CreatedDate ASC", paginate=True)
    if hres is None: raise RuntimeError('Salesforce OpportunityHistory query failed (soql returned None)')
    hrecs = hres.get('records') or []
    new_wm_h = max([wm_h] * bool(wm_h) + [_parse(h['CreatedDate']) for h in hrecs if h.get('CreatedDate')], default=wm_h)   # over everything fetched
    known = set(ids) | existing
    missing_opps = sorted({h['OpportunityId'] for h in hrecs} - known)
    for ch in _chunks(missing_opps, 150):        # history for an opportunity we have never stored (should not happen after the first run)
        known |= {r['opp_id'] for r in H._http('GET', 'opportunities', {'select': 'opp_id', 'opp_id': f'in.({",".join(ch)})'})}
    hrecs = [h for h in hrecs if h['OpportunityId'] in known]
    users = _users(H, {h.get('CreatedById') for h in hrecs if h.get('CreatedById')})
    hrows = [{'history_id': h['Id'], 'opp_id': h['OpportunityId'], 'stage': h.get('StageName'), 'amount': _num(h.get('Amount')), 'close_date': _d(h.get('CloseDate')),
              'probability': _num(h.get('Probability')), 'changed_at': _sf_ts(_parse(h['CreatedDate'])), 'changed_by_id': h.get('CreatedById'), 'changed_by_name': users.get(h.get('CreatedById')),
              'sync_run_id': run_id} for h in hrecs]
    n_err = len(H._errors)
    ins = H._insert('opportunity_stage_history', hrows, on_conflict='history_id', returning=True, count_col='history_id') if hrows else []
    if len(H._errors) > n_err: raise RuntimeError('a stage-history insert batch failed (queued); watermarks not advanced')
    status['history_new'] = stats['opps_history_new'] = len(ins or [])
    # 3) attribution for everything touched this run
    touched = sorted(set(ids) | {h['OpportunityId'] for h in hrecs})
    if touched:
        r = H._http('POST', 'rpc/attribute_opportunities', None, {'p_opp_ids': touched, 'p_run_id': run_id}, timeout=180)
        if r:
            status['attributed'] = stats['opps_attributed'] = int(r[0].get('inserted') or 0); status['superseded'] = stats['opps_superseded'] = int(r[0].get('superseded') or 0)
            status['unattributed'] = stats['opps_unattributed'] = int(r[0].get('unattributed') or 0)
    H._http('POST', 'opp_ingest_state', {'on_conflict': 'id'}, [{'id': 1, 'watermark_modified': _sf_ts(new_wm_o) if new_wm_o else None, 'watermark_history': _sf_ts(new_wm_h) if new_wm_h else None,
                                                             'last_run_id': run_id, 'updated_at': _iso()}], prefer='resolution=merge-duplicates,return=minimal')
    status['last_ok_at'] = _iso(); status['watermark'] = _sf_ts(new_wm_o) if new_wm_o else None

def summary():
    st = status
    if st.get('error'): return f"ERROR {st['error'][:120]}"
    if st.get('skipped') and not st.get('last_ok_at'): return f"skipped ({st['skipped'][:80]})"
    return f"+{st.get('new', 0)} new / {st.get('updated', 0)} updated, +{st.get('history_new', 0)} stage changes, {st.get('attributed', 0)} attributed" + (f", {st['superseded']} moved" if st.get('superseded') else '') + (f", {st['unattributed']} unattributed" if st.get('unattributed') else '')
