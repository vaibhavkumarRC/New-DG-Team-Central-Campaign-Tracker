"""lead_history.py against a fake history module: watermark discipline, effects, merges, tie re-evaluation."""
import os, sys, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
import lead_history as LH

WM = '2026-09-19T07:16:09+00:00'

def hist(i, created, field='Status', lead='00QA1', old='Unqualified', new='MQL', by='005U1'):
    return {'Id': i, 'LeadId': lead, 'Field': field, 'OldValue': old, 'NewValue': new, 'CreatedDate': created, 'CreatedById': by}

class FakeH:
    def __init__(self, rows, watermark=WM, merges=()):
        self.rows = rows; self.merges = list(merges); self._errors = []; self.calls = []; self.inserts = []; self.patches = []; self.fail_insert = False
        self._cfg = {'leadhist_budget': '180', 'leadhist_max_rows': '20000'}
        self._state = {'sf_users': {'005U1': 'SDR One'}}
        self.db = {'lead_history_ingest_state': [{'watermark_created': watermark}] if watermark else [],
                   'leads': [{'lead_id': '00QLOSER', 'merged_into_lead_id': None}],
                   'touches': [{'touch_id': '00T1'}, {'touch_id': '00T2'}],
                   'touch_attributions': [{'touch_id': '00T2'}]}          # 00T2 has a live tie
        self._deps = {'soql': self.soql}
    def soql(self, q, paginate=True, all_rows=False):
        self.calls.append(('soql', q, all_rows))
        if 'FROM LeadHistory' in q: return {'records': self.rows[:int(q.rsplit('LIMIT ', 1)[1])]}
        if 'FROM User' in q: return {'records': [{'Id': '005U2', 'Name': 'SDR Two'}]}
        if 'FROM Lead WHERE IsDeleted' in q: assert all_rows; return {'records': [{'Id': l, 'MasterRecordId': s} for l, s in self.merges]}
        return {'records': []}
    def _err(self, m): self._errors.append(m)
    def _short(self, e): return str(e)
    def _http(self, method, path, params=None, body=None, prefer=None, timeout=90):
        self.calls.append((method, path, params, body))
        if method == 'GET':
            rows = self.db.get(path, [])
            if path == 'leads': return [r for r in rows if r['lead_id'] == params['lead_id'][3:]]
            return rows
        if path == 'rpc/reevaluate_ties_for_leads': return [{'touches_reevaluated': 1, 'superseded': 2}]
        return [{'created_set': 1, 'merged_set': 0}] if path.startswith('rpc/') else []
    def _get_all(self, path, params, page=1000, order=None): return self._http('GET', path, params)
    def _patch(self, table, params, body): self.patches.append((table, params, body)); return True
    def _insert(self, table, rows, on_conflict=None, returning=False, count_col=None, **kw):
        if self.fail_insert: self._errors.append('insert failed → queued'); return []
        self.inserts.append((table, rows)); return [{'history_id': r['history_id']} for r in rows]
    def rpc(self, name): return [b for c in self.calls if isinstance(c, tuple) and len(c) == 4 and c[1] == name for b in [c[3]]]
    def watermarks(self): return [c[3][0]['watermark_created'] for c in self.calls if isinstance(c, tuple) and len(c) == 4 and c[1] == 'lead_history_ingest_state' and c[0] == 'POST']

class LeadHistory(unittest.TestCase):
    def run_(self, H): stats = {}; LH.ingest(5, stats, H); return stats

    def test_query_fields_overlap_and_cap(self):
        H = FakeH([]); self.run_(H)
        q = [c[1] for c in H.calls if c[0] == 'soql' and 'LeadHistory' in c[1]][0]
        self.assertIn("CreatedDate > 2026-09-19T05:16:09Z", q); self.assertIn("'leadMerged'", q); self.assertIn("'Campaign__c'", q); self.assertIn('LIMIT 20000', q)
        self.assertNotIn("'Account_Lookup__c'", q); self.assertNotIn("'Owner'", q)

    def test_rows_written_effects_applied_users_resolved(self):
        H = FakeH([hist('017A', '2026-09-19T08:00:00.000+0000'), hist('017B', '2026-09-19T08:01:00.000+0000', field='Campaign__c', old='Camp A', new='Camp B', by='005U2'),
                   hist('017C', '2026-09-19T08:02:00.000+0000', field='created', old=None, new=None)])
        stats = self.run_(H)
        rows = {r['history_id']: r for _, rs in H.inserts for r in rs}
        self.assertEqual((rows['017A']['field'], rows['017A']['new_value'], rows['017A']['changed_by_name'], rows['017A']['changed_at'], rows['017A']['source']), ('Status', 'MQL', 'SDR One', '2026-09-19T08:00:00Z', 'leadhistory'))
        self.assertEqual(rows['017B']['changed_by_name'], 'SDR Two'); self.assertEqual(H._state['sf_users']['005U2'], 'SDR Two')     # fetched once, cached
        self.assertIsNone(rows['017C']['old_value'])
        self.assertEqual(H.rpc('rpc/apply_lead_history_effects')[0]['p_history_ids'], ['017A', '017B', '017C'])
        self.assertEqual(H.rpc('rpc/reevaluate_ties_for_leads')[0]['p_lead_ids'], ['00QA1'])           # only the lead whose stamp changed
        self.assertEqual((stats['leadhist_seen'], stats['leadhist_new'], stats['leadhist_ties']), (3, 3, 1))
        self.assertEqual(H.watermarks()[-1], '2026-09-19T08:02:00Z'); self.assertIsNone(LH.status['error']); self.assertFalse(H._errors)

    def test_merges_from_recycle_bin(self):
        H = FakeH([], merges=[('00QLOSER', '00QSURV'), ('00QUNKNOWN', '00QSURV')]); stats = self.run_(H)
        self.assertEqual(H.patches, [('leads', {'lead_id': 'eq.00QLOSER'}, {'merged_into_lead_id': '00QSURV', 'is_deleted': True, 'last_seen_at': H.patches[0][2]['last_seen_at']})])
        self.assertEqual(stats['leadhist_merges'], 1)                                                     # unknown loser (never a member) skipped
        q = [c for c in H.calls if c[0] == 'soql' and 'IsDeleted' in c[1]][0]; self.assertTrue(q[2]); self.assertIn('LastModifiedDate > 2026-09-19T05:16:09Z', q[1])

    def test_no_watermark_and_write_failure(self):
        H = FakeH([hist('017A', '2026-09-19T08:00:00.000+0000')], watermark=None); self.run_(H)
        self.assertIn('no watermark', LH.status['error']); self.assertFalse(H.inserts)
        H = FakeH([hist('017A', '2026-09-19T08:00:00.000+0000')]); H.fail_insert = True; self.run_(H)
        self.assertIn('watermark not advanced', LH.status['error']); self.assertEqual(H.watermarks(), [])

    def test_zero_rows_stamps_state(self):
        H = FakeH([]); self.run_(H); self.assertEqual(H.watermarks(), ['2026-09-19T07:16:09Z']); self.assertIsNotNone(LH.status['last_ok_at'])

if __name__ == '__main__':
    unittest.main()
