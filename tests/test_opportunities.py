"""opportunities.py against a fake history module: first full load, incremental window, rows, attribution call, failures."""
import os, sys, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
import opportunities as OP

def opp(i, modified='2026-09-19T08:00:00.000+0000', stage='S2 - Demo Completed', **k):
    r = {'Id': i, 'Name': 'Example Health_RapidCode', 'AccountId': '001ACC', 'StageName': stage, 'Amount': 100000, 'Annual_Contract_Value__c': None, 'Proposal_Amount__c': None,
         'Probability': 40, 'CreatedDate': '2026-09-01T10:00:00.000+0000', 'CloseDate': '2026-12-31', 'IsClosed': False, 'IsWon': False, 'Owner': {'Name': 'AE One'},
         'SDR_Owner__c': 'SDR One', 'Source__c': 'Cold Outreach', 'LeadSource': None, 'Type': None, 'Organization_Type__c': 'Health System', 'Loss_Reason__c': None,
         'Loss_Reason_Explanation__c': None, 'Nurture_Reason__c': None, 'Next_Steps__c': 'Send NDA', 'Competitor_Selected__c': None, 'Contract_Sign_Date__c': None,
         'Subscription_Start_Date__c': None, 'LastStageChangeDate': '2026-09-10T10:00:00.000+0000', 'LastModifiedDate': modified}
    r.update(k); return r

class FakeH:
    def __init__(self, opps, hist=(), state=None):
        self.opps = opps; self.hist = list(hist); self._errors = []; self.calls = []; self.inserts = []; self.fail_insert = None
        self._cfg = {'opp_budget': '120'}; self._state = {'sf_users': {}}
        self.db = {'opp_ingest_state': [state] if state else [], 'opportunities': [{'opp_id': '006OLD'}]}
        self._deps = {'soql': self.soql}
    def soql(self, q, paginate=True, all_rows=False):
        self.calls.append(('soql', q))
        if q.startswith('SELECT Id, Name, AccountId'): return {'records': self.opps}
        if 'FROM OpportunityHistory' in q: return {'records': self.hist}
        if 'ConvertedOpportunityId IN' in q: return {'records': [{'Id': '00QCONV', 'ConvertedOpportunityId': '006NEW'}]}
        if 'FROM User' in q: return {'records': [{'Id': '005U1', 'Name': 'AE One'}]}
        return {'records': []}
    def _err(self, m): self._errors.append(m)
    def _short(self, e): return str(e)
    def _http(self, method, path, params=None, body=None, prefer=None, timeout=90):
        self.calls.append((method, path, params, body))
        if method == 'GET':
            rows = self.db.get(path, [])
            if path == 'opportunities': return [r for r in rows if r['opp_id'] in params['opp_id']]
            return rows
        if path == 'rpc/attribute_opportunities': return [{'inserted': 2, 'superseded': 0, 'unattributed': 1}]
        return []
    def _insert(self, table, rows, on_conflict=None, returning=False, count_col=None, prefer=None, **kw):
        if self.fail_insert == table: self._errors.append(f'{table} failed → queued'); return []
        self.inserts.append((table, rows, prefer)); return [{'history_id': r.get('history_id')} for r in rows]
    def state_writes(self): return [c[3][0] for c in self.calls if isinstance(c, tuple) and len(c) == 4 and c[1] == 'opp_ingest_state' and c[0] == 'POST']

class Opportunities(unittest.TestCase):
    def run_(self, H): stats = {}; OP.ingest(3, stats, H); return stats

    def test_first_run_loads_everything_and_sets_watermarks(self):
        H = FakeH([opp('006NEW'), opp('006OLD', modified='2026-09-19T09:00:00.000+0000', stage='Closed Won', IsClosed=True, IsWon=True, Annual_Contract_Value__c=250000)],
                  hist=[{'Id': '008A', 'OpportunityId': '006NEW', 'StageName': 'S1 - Need Identified', 'Amount': 100000, 'CloseDate': '2026-12-31', 'Probability': 20, 'CreatedDate': '2026-09-01T10:00:00.000+0000', 'CreatedById': '005U1'},
                        {'Id': '008B', 'OpportunityId': '006GHOST', 'StageName': 'S1 - Need Identified', 'CreatedDate': '2026-09-02T10:00:00.000+0000', 'CreatedById': '005U1'}])
        stats = self.run_(H)
        q = [c[1] for c in H.calls if c[0] == 'soql' and c[1].startswith('SELECT Id, Name')][0]; self.assertNotIn('WHERE', q)      # no watermark → everything
        opps = {r['opp_id']: r for t, rs, _ in H.inserts if t == 'opportunities' for r in rs}
        self.assertEqual((opps['006NEW']['converted_from_lead_id'], opps['006NEW']['stage'], opps['006NEW']['owner_name'], opps['006NEW']['created_on'], opps['006NEW']['amount']), ('00QCONV', 'S2 - Demo Completed', 'AE One', '2026-09-01', 100000.0))
        self.assertEqual((opps['006OLD']['is_won'], opps['006OLD']['annual_contract_value'], opps['006OLD']['converted_from_lead_id']), (True, 250000.0, None))
        self.assertIn('merge-duplicates', [p for t, _, p in H.inserts if t == 'opportunities'][0])
        hist = [r for t, rs, _ in H.inserts if t == 'opportunity_stage_history' for r in rs]
        self.assertEqual([h['history_id'] for h in hist], ['008A']); self.assertEqual(hist[0]['changed_by_name'], 'AE One')      # ghost opp's history skipped
        att = [c[3] for c in H.calls if isinstance(c, tuple) and len(c) == 4 and c[1] == 'rpc/attribute_opportunities'][0]
        self.assertEqual(att['p_opp_ids'], ['006NEW', '006OLD'])
        self.assertEqual((stats['opps_seen'], stats['opps_new'], stats['opps_history_new'], stats['opps_attributed'], stats['opps_unattributed']), (2, 1, 1, 2, 1))
        w = H.state_writes()[-1]; self.assertEqual((w['watermark_modified'], w['watermark_history']), ('2026-09-19T09:00:00Z', '2026-09-02T10:00:00Z'))
        self.assertIsNone(OP.status['error']); self.assertIn('+1 new / 1 updated, +1 stage changes, 2 attributed, 1 unattributed', OP.summary())

    def test_incremental_uses_overlap(self):
        H = FakeH([], state={'watermark_modified': '2026-09-19T09:00:00+00:00', 'watermark_history': '2026-09-19T08:00:00+00:00'}); self.run_(H)
        qs = [c[1] for c in H.calls if c[0] == 'soql']
        self.assertTrue(any('FROM Opportunity WHERE LastModifiedDate > 2026-09-19T07:00:00Z' in q for q in qs), qs)
        self.assertTrue(any('FROM OpportunityHistory WHERE CreatedDate > 2026-09-19T06:00:00Z' in q for q in qs), qs)
        w = H.state_writes()[-1]; self.assertEqual(w['watermark_modified'], '2026-09-19T09:00:00Z')      # unchanged on a quiet run
        self.assertFalse(any(c[1] == 'rpc/attribute_opportunities' for c in H.calls if isinstance(c, tuple) and len(c) == 4))

    def test_write_failure_keeps_watermarks(self):
        H = FakeH([opp('006NEW')]); H.fail_insert = 'opportunities'; self.run_(H)
        self.assertIn('watermarks not advanced', OP.status['error']); self.assertEqual(H.state_writes(), [])

if __name__ == '__main__':
    unittest.main()
