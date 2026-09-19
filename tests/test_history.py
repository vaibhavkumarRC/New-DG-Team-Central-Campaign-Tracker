"""history.py behaviour with a fake Salesforce and a fake REST layer.
Also checks every row the writer produces only uses columns that exist in the
real dg-campaign-history schema (read from the project's OpenAPI, read-only)."""
import json, os, sys, tempfile, unittest, subprocess, urllib.request
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
os.environ['HISTORY_SUPABASE_URL'] = 'https://fake.supabase.co'; os.environ['HISTORY_SUPABASE_KEY'] = 'sb_secret_fake'
os.environ.pop('SUPABASE_SERVICE_KEY', None)
import history as H

# ── real schema (read-only) so we can validate payload columns ──────────────
def live_columns():
    try:
        key = subprocess.run(['security', 'find-generic-password', '-s', 'dg-campaign-history-key', '-w'], capture_output=True, text=True).stdout.strip()
        req = urllib.request.Request('https://uakmygepznvuscegoqrf.supabase.co/rest/v1/', headers={'apikey': key, 'Authorization': 'Bearer ' + key})
        spec = json.load(urllib.request.urlopen(req, timeout=30))
        return {name: set(d.get('properties', {}).keys()) for name, d in spec.get('definitions', {}).items()}
    except Exception as e:
        print('live schema unavailable:', e); return None
COLS = live_columns()

class FakeRest:
    """Records every write; answers reads from a tiny in-memory DB."""
    def __init__(self):
        self.writes = []; self.fail_tables = set(); self.next_id = 100
        self.db = {'campaigns': [], 'campaign_leads': [], 'meetings': [], 'meeting_attributions': [], 'v_campaign_latest_snapshot': [], 'people': [
            {'person_key': 'soham_saha', 'display_name': 'Soham Saha', 'aliases': ['Soham', 'Soham Saha']}], 'dq_findings': [], 'metric_definitions': [{'version': 2}, {'version': 3}],
            'touch_ingest_state': [{'watermark_created': '2026-09-19T06:54:52+00:00', 'updated_at': '2026-09-19T07:00:00+00:00'}], 'touches': [], 'touch_attributions': [],
            'quarter_closes': [{'quarter': q, 'closed_at': 'x'} for q in ('2025-Q4', '2026-Q1', '2026-Q2', '2026-Q3', '2026-Q4', '2027-Q1', '2027-Q2', '2027-Q3', '2027-Q4')],
            'lead_history_ingest_state': [{'watermark_created': '2026-09-19T07:16:09+00:00'}], 'lead_field_changes': []}
        self.rpc = []
    def __call__(self, method, path, params=None, body=None, prefer=None, timeout=90, base=None, key=None):
        if base:                                  # intelligence_dashboard lookups
            return [{'id': 'c0', 'company_name': 'Wellstar', 'salesforce_account_id': '001AAA000000001', 'rc_account_id': 'RC0000001', 'organisation_type': 'HS', 'revenue_estimate_usd': 5e9, 'specialty_type': ['Oncology'], 'is_provider': True}]
        if method == 'GET':
            rows = self.db.get(path, []); off = int((params or {}).get('offset', 0)); lim = int((params or {}).get('limit', 1000))
            if path == 'quarter_closes': rows = [r for r in rows if r['quarter'] == (params or {}).get('quarter', '')[3:]]
            return rows[off:off+lim]
        if path in self.fail_tables: raise urllib.error.HTTPError('u', 500, 'boom', {}, None)
        if path.startswith('rpc/'):
            self.rpc.append((path, body))
            if path == 'rpc/attribute_touches': return [{'inserted_primary': len(body['p_touch_ids']), 'inserted_context': 0, 'superseded': 0, 'campaign_ids': ['uuid-101']}]
            if path == 'rpc/apply_lead_history_effects': return [{'created_set': 0, 'merged_set': 0}]
            return [{'rows_upserted': 1, 'rows_deleted': 0}]
        self.writes.append((method, path, params, body))
        if COLS and method == 'POST' and path in COLS:
            for r in body:
                bad = set(r) - COLS[path]; assert not bad, f'{path}: unknown columns {bad}'
        if method == 'POST' and prefer and 'return=representation' in prefer:
            out = []
            for r in body:
                self.next_id += 1; row = {'id': self.next_id if path != 'campaigns' else f'uuid-{self.next_id}', **r}; out.append(row)
                if path == 'campaigns': self.db['campaigns'].append(row)
            return out
        return []

class FakeSoql:
    def __init__(self): self.calls = []; self.leads = {}; self.stamps = []; self.tasks = []
    def __call__(self, q, paginate=True, **kw):
        self.calls.append(q)
        if 'FROM Task WHERE CreatedDate' in q: return {'records': list(self.tasks)}
        if 'FROM LeadHistory' in q or 'IsDeleted = true' in q: return {'records': []}
        if 'GROUP BY Campaign__c' in q: return {'records': [{'Campaign__c': s, 'expr0': n} for s, n in self.stamps]}
        if q.startswith('SELECT Id FROM Lead WHERE Campaign__c'): return {'records': [{'Id': i} for i in ('00QUNREG1', '00QUNREG2')]}
        if 'FROM Lead WHERE Id IN' in q:
            ids = [x.strip("'") for x in q.split('IN (')[1].split(')')[0].split(',')]
            return {'records': [self.leads[i] for i in ids if i in self.leads]}
        if 'FROM Account' in q: return {'records': [{'Id': '001AAA000000001AAA', 'Name': 'Wellstar Health', 'RC_Account_ID__c': 'RC0000001', 'Organization_Type__c': 'Health System', 'Revenue_Bucket__c': 'More than $1B', 'Region__c': 'South', 'Account_Territory__c': 'South-1', 'State__c': 'GA'}]}
        if 'FROM Opportunity' in q: return {'records': [{'Id': '006OPP1', 'StageName': 'S2', 'Amount': 250000}]}
        return {'records': []}

def lead(i, **k):
    base = {'Id': i, 'Name': f'Lead {i}', 'Title': 'Director of Revenue Cycle', 'Company': 'Wellstar', 'Email': f'{i}@wellstar.org', 'Account_Lookup__c': '001AAA000000001AAA', 'RC_Account_ID__c': 'RC0000001',
            'Management_Level__c': 'Director Level', 'Job_Function__c': 'Coding/Billing/Account Rec', 'Lead_Type__c': 'Decision Maker', 'Status': 'Unqualified', 'DoNotCall': False, 'Region__c': 'South', 'Territory__c': 'South-1', 'State': 'GA',
            'Campaign__c': 'Camp A', 'CreatedDate': '2026-09-01T10:00:00.000+0000', 'IsConverted': False}
    base.update(k); return base

class HistoryWriter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(); self.rest = FakeRest(); self.soql = FakeSoql()
        H._cfg.update({'url': 'https://fake.supabase.co', 'key': 'k', 'data_dir': self.tmp, 'budget': '300', 'max_unreg': '20', 'intel_url': 'https://intel', 'intel_key': 'ik'})
        H._deps.update({'soql': self.soql, 'load_campaigns': lambda: self.camps, 'norm_sdr': lambda n: {'Soham': 'Soham Saha'}.get(n, n)})
        H._summary['enabled'] = True; H._http = self.rest; H._state = None; H._pending.clear()
        self.camps = [{'id': '1700000000000', 'name': 'Camp A', 'campaign_type': 'Signal Led Campaign', 'segment': 'High Intent Data', 'pod_team': 'P', 'sdr_owner': 'Soham Saha', 'email_owner': 'Vinayak', 'status': 'Active', 'start_date': '2026-09-01', 'end_date': '2026-09-30'}]
        self.soql.leads = {'00QA1': lead('00QA1'), '00QA2': lead('00QA2', Meeting_Generated_on__c='2026-09-10', Meeting_Status__c='Meeting Scheduled', Meeting_Generated_by__c='Soham', Seller_Name__c='Matt Bates', Meeting_Source__c='Cold Outreach', Meeting_Channel__c='Call', ConvertedOpportunityId='006OPP1'),
                           '00QUNREG1': lead('00QUNREG1', Campaign__c='Mystery_List'), '00QUNREG2': lead('00QUNREG2', Campaign__c='Mystery_List')}

    def writes(self, table): return [b for m, p, _, b in self.rest.writes if p == table and m == 'POST']
    def patches(self, table): return [(pr, b) for m, p, pr, b in self.rest.writes if p == table and m == 'PATCH']

    def run_sync(self, result=None, meetings=None, moved=None, nooks=()):
        res = {'total_leads': 2, 'total_calls': 10, 'total_connects': 3, 'total_conversations': 1, 'total_emails': 0, 'unique_leads_called': 2, 'unique_leads_emailed': 0, 'li_sent': 0, 'li_accepted': 0, 'li_msg_sent': 0, 'li_msg_reply': 0,
               'meetings': 1, 'meeting_done': 0, 'meeting_noshow': 0, 'sql_gen': 0, 's1_created': 0, 's1_is_manual': False, 'call_dispositions': {}, 'sdr_breakdown': [], 'status_sdr_breakdown': [], 'settled_date': None, 'synced_at': '2026-09-18T12:00:00'}
        if result: res.update(result)
        H.record_campaign(self.camps[0], res, ['00QA1', '00QA2'], ['00QA1', '00QA2'], meetings if meetings is not None else {'00QA2': {'date': '2026-09-10', 'sdr': 'Soham', 'name': 'Lead 00QA2', 'title': 'Director of Revenue Cycle', 'company': 'Wellstar'}}, moved or {}, list(nooks))
        H.flush(app_version='test')

    def test_first_sync_creates_everything(self):
        self.run_sync()
        self.assertEqual(len(self.writes('campaigns')), 1); c = self.writes('campaigns')[0][0]
        self.assertEqual((c['sf_name'], c['registered'], c['campaign_type']), ('Camp A', True, 'Signal Led Campaign'))
        cl = self.writes('campaign_leads')[0]; self.assertEqual({r['lead_id'] for r in cl}, {'00QA1', '00QA2'})
        r = cl[0]; self.assertEqual((r['snapshot_quality'], r['membership_source'], r['seniority_norm'], r['function_norm'], r['org_type_sb'], r['revenue_bucket_sfdc']), ('at_enroll', 'sync', 'Director', 'Rev Cycle', 'HS', 'More than $1B'))
        m = self.writes('meetings')[0][0]
        self.assertEqual((m['meeting_id'], m['generated_by'], m['seller'], m['booked_via'], m['opp_stage'], m['opp_amount'], m['is_done']), ('00QA2:2026-09-10', 'Soham Saha', 'Matt Bates', 'sfdc_field', 'S2', 250000.0, False))
        a = self.writes('meeting_attributions')[0][0]; self.assertTrue(a['is_primary'] and a['in_window']); self.assertEqual(a['rule'], 'member_in_window')
        s = self.writes('campaign_metric_snapshots')[0][0]; self.assertEqual((s['snapshot_kind'], s['definition_version'], s['meetings'], s['is_final']), ('sync', 3, 1, False))
        self.assertTrue(any(w[0]['source'] == 'dashboard_sync' for w in self.writes('sync_runs')))
        self.assertIn('history: +2 members, +1 meetings', H.summary()); self.assertEqual(H._summary['stats']['errors'], 0)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, 'history_state.json.gz')))

    def test_second_sync_is_incremental_and_dedupes_snapshot(self):
        self.run_sync(); n = len(self.rest.writes)
        self.run_sync()                      # nothing changed, same day
        new = self.rest.writes[n:]
        self.assertFalse([w for w in new if w[1] in ('campaign_leads', 'meetings', 'meeting_attributions', 'campaign_metric_snapshots') and w[0] == 'POST'], new)
        self.run_sync(result={'total_calls': 11})   # metric changed → one more snapshot
        self.assertEqual(len(self.writes('campaign_metric_snapshots')), 2)

    def test_status_change_appends_history_and_patches(self):
        self.run_sync()
        self.soql.leads['00QA2']['Meeting_Status__c'] = 'Meeting Done-SQL'; self.soql.leads['00QA2']['Status'] = 'SQL'
        self.run_sync()
        pr, body = self.patches('meetings')[-1]; self.assertEqual(pr['meeting_id'], 'eq.00QA2:2026-09-10'); self.assertTrue(body['is_done'] and body['is_sql'])
        self.assertEqual(len(self.writes('meeting_status_history')), 2)

    def test_moved_lead_marks_removed(self):
        self.run_sync()
        self.run_sync(moved={'00QA1': 'Camp B'})
        pr, body = self.patches('campaign_leads')[-1]
        self.assertEqual((pr['campaign_id'][:3], pr['lead_id'], body['removed_reason']), ('eq.', 'in.(00QA1)', 'moved_to:Camp B'))

    def test_out_of_window_meeting_not_primary(self):
        self.run_sync(meetings={'00QA2': {'date': '2026-05-10', 'sdr': 'Soham', 'name': 'x', 'title': 't', 'company': 'c'}}, result={'meetings': 0})
        a = self.writes('meeting_attributions')[0][0]; self.assertFalse(a['is_primary']); self.assertFalse(a['in_window']); self.assertIn('outside', a['note'])
        self.assertEqual(H._summary['stats']['mismatches'], 0)

    def test_config_change_logged_and_window_edit_flagged(self):
        self.run_sync(); self.camps[0]['end_date'] = '2026-10-15'; self.run_sync()
        ch = [r for w in self.writes('campaign_config_history') for r in w if r['field'] == 'end_date']; self.assertEqual(ch[0]['new_value'], '2026-10-15')
        self.assertTrue(any(r['check_name'] == 'window_edit' for w in self.writes('dq_findings') for r in w))

    def test_final_snapshot_once(self):
        self.camps[0]['status'] = 'Completed'; self.run_sync(result={'settled_date': '2026-09-01'})
        s = self.writes('campaign_metric_snapshots')[0][0]; self.assertTrue(s['is_final']); self.assertEqual(s['snapshot_kind'], 'final')
        self.run_sync(result={'settled_date': '2026-09-01'}); self.assertEqual(len(self.writes('campaign_metric_snapshots')), 1)

    def test_unregistered_stamp_discovered_with_members(self):
        self.soql.stamps = [('Camp A', 2), ('Mystery_List', 2)]
        self.run_sync()
        c = [r for w in self.writes('campaigns') for r in w if r['sf_name'] == 'Mystery_List'][0]; self.assertFalse(c['registered']); self.assertEqual(c['discovered_from'], 'salesforce')
        cl = [r for w in self.writes('campaign_leads') for r in w if r['membership_source'] == 'salesforce_discovery']; self.assertEqual(len(cl), 2)
        self.assertTrue(any(r['check_name'] == 'unregistered_stamp' for w in self.writes('dq_findings') for r in w))

    def test_rest_failure_queues_and_replays(self):
        self.rest.fail_tables.add('campaign_leads'); self.run_sync()
        q = os.path.join(self.tmp, 'history_queue.jsonl'); self.assertTrue(os.path.exists(q)); self.assertGreaterEqual(H._summary['stats']['errors'], 1)
        self.rest.fail_tables.clear(); self.run_sync()
        self.assertFalse(os.path.exists(q)); self.assertGreaterEqual(H._summary['stats']['queued_replayed'], 1)

    def test_disabled_is_noop(self):
        H._summary['enabled'] = False; self.run_sync(); self.assertEqual(self.rest.writes, [])

    def test_touches_step_runs_last_and_processes_new_member_backlog(self):
        self.soql.tasks = [{'Id': '00TNEW1', 'Subject': '[Nooks Call] Outbound', 'TaskSubtype': 'Call', 'Type': 'Call', 'WhoId': '00QA1', 'OwnerId': '005X', 'Owner': {'Name': 'Soham Saha'},
                            'CreatedDate': '2026-09-19T08:10:00.000+0000', 'ActivityDate': '2026-09-19', 'CallDisposition': 'Connected', 'CallDurationInSeconds': 95, 'CallType': 'Outbound', 'Not_Relevant__c': False},
                           {'Id': '00TESC', 'Subject': '[RC] ESCALATION x', 'WhoId': None, 'CreatedDate': '2026-09-19T08:11:00.000+0000', 'ActivityDate': '2026-09-19'}]
        self.rest.db['touches'] = [{'touch_id': '00TOLD1'}]      # an older touch of a lead that is being enrolled now
        self.run_sync()
        t = self.writes('touches')[0][0]
        self.assertEqual((t['touch_id'], t['channel'], t['tool'], t['is_connect'], t['is_conversation'], t['is_counted_by_dashboard'], t['dashboard_cards'], t['owner_person_key'], t['definition_version'], t['occurred_at']), ('00TNEW1', 'call', 'nooks', True, True, True, ['call'], 'soham_saha', 3, '2026-09-19T08:10:00Z'))
        self.assertEqual(len(self.writes('touches')[0]), 1)                                  # escalation dropped
        attributed = [b['p_touch_ids'] for p, b in self.rest.rpc if p == 'rpc/attribute_touches']
        self.assertIn(['00TNEW1'], attributed)                                               # new touch attributed
        self.assertTrue(any('00TOLD1' in ids for ids in attributed), attributed)             # backlog: existing touch of the new member re-evaluated
        self.assertTrue(any(p == 'rpc/refresh_campaign_lead_activity_for_touches' and set(b['p_touch_ids']) >= {'00TNEW1', '00TOLD1'} for p, b in self.rest.rpc), self.rest.rpc)
        wm = [b for m, p, _, b in self.rest.writes if p == 'touch_ingest_state'][-1][0]
        self.assertEqual(wm['watermark_created'], '2026-09-19T08:11:00Z')                     # advanced over the whole fetched slice (incl. skipped rows)
        self.assertEqual(H._summary['stats']['errors'], 0); self.assertIn('touches +1 of 2 seen', H.summary())
        self.assertEqual(H._state.get('touch_backlog'), {'members': []})
        # order: touches is the last step — its sync_runs row is patched after everything
        self.assertEqual(H.T.status['new'], 1)

    def test_touches_without_watermark_is_an_error_not_a_guess(self):
        self.rest.db['touch_ingest_state'] = []
        self.run_sync()
        self.assertFalse(self.writes('touches')); self.assertFalse(self.soql.tasks)
        self.assertTrue(any('no watermark' in e for e in H._errors), H._errors); self.assertIn('touches ERROR', H.summary())

if __name__ == '__main__':
    unittest.main(verbosity=2)
