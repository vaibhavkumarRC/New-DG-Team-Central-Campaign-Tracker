"""quarter_close.py: date rules, due/skip logic, what gets written, resume after a partial write."""
import os, sys, unittest
from datetime import date
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
import quarter_close as Q

KEYS = ('total_leads','total_calls','total_connects','total_conversations','total_emails','unique_leads_called','unique_leads_emailed',
        'li_sent','li_accepted','li_msg_sent','li_msg_reply','meetings','meeting_done','meeting_noshow','sql_gen','s1_created')

class FakeH:
    _METRIC_KEYS = KEYS
    def __init__(self):
        self._errors = []; self.calls = []; self.inserts = []; self.dq = []
        self._state = {'campaigns': {
            'Q3 settled':  {'db_id': 'u1', 'dashboard_id': 'd1', 'sf_name': 'Q3 settled', 'config': {'start_date': '2026-07-06', 'end_date': '2026-07-31', 'status': 'Completed', 'settled_date': '2026-08-20'}},
            'Q3 active':   {'db_id': 'u2', 'dashboard_id': 'd2', 'sf_name': 'Q3 active',  'config': {'start_date': '2026-09-25', 'end_date': '2026-10-15', 'status': 'Active', 'settled_date': None}},
            'Q3 deleted':  {'db_id': 'u3', 'dashboard_id': 'd3', 'sf_name': 'Q3 deleted', 'config': {'start_date': '2026-08-01', 'end_date': '2026-08-31', 'status': 'Completed', 'settled_date': '2026-09-20'}},
            'Q2 old':      {'db_id': 'u4', 'dashboard_id': 'd4', 'sf_name': 'Q2 old',     'config': {'start_date': '2026-05-01', 'end_date': '2026-05-31', 'status': 'Completed', 'settled_date': '2026-06-20'}},
            'gone':        {'db_id': 'u5', 'dashboard_id': 'd5', 'sf_name': 'gone', 'deleted': True, 'config': {'start_date': '2026-08-10'}},
            'unregistered':{'db_id': 'u6', 'dashboard_id': None, 'sf_name': 'unregistered', 'config': {}}}, 'dq_seen': []}
        self._deps = {'cache': {'campaigns': [
            {'id': 'd1', **{k: 10 for k in KEYS}, 's1_is_manual': False, 'call_dispositions': {'x': 1}, 'sdr_breakdown': [], 'status_sdr_breakdown': []},
            {'id': 'd2', **{k: 2 for k in KEYS}, 's1_is_manual': True}]}}
        self.db = {'quarter_closes': [], 'campaign_metric_snapshots': [], 'v_campaign_latest_snapshot': [{'campaign_id': 'u3', **{k: 7 for k in KEYS}, 's1_is_manual': False}]}
    def _err(self, m): self._errors.append(m)
    def _short(self, e): return str(e)
    def _dq(self, run_id, check, sev, st, sid, detail, stats): self.dq.append((check, sid, detail))
    def _http(self, method, path, params=None, body=None, prefer=None, timeout=90):
        self.calls.append((method, path, params))
        rows = self.db.get(path, [])
        if path == 'quarter_closes': return [r for r in rows if r['quarter'] == params['quarter'][3:]]
        if path == 'v_campaign_latest_snapshot': return [r for r in rows if r['campaign_id'] == params['campaign_id'][3:]]
        if path == 'campaign_metric_snapshots': return [r for r in rows if r.get('snapshot_kind') == 'quarter_close' and r['extras']['quarter'] == params['extras->>quarter'][3:]]
        return rows
    def _get_all(self, path, params, page=1000, order=None): return self._http('GET', path, params)
    def _insert(self, table, rows, on_conflict=None, count_col=None, **kw):
        self.inserts.append((table, rows)); self.db.setdefault(table, []).extend(rows); return len(rows)

class Dates(unittest.TestCase):
    def test_quarter_math(self):
        self.assertEqual(Q.quarter_of('2026-09-30'), '2026-Q3'); self.assertEqual(Q.quarter_of('2026-10-01'), '2026-Q4'); self.assertEqual(Q.quarter_of('2026-01-01'), '2026-Q1')
        self.assertEqual(Q.quarter_end('2026-Q3'), date(2026, 9, 30)); self.assertEqual(Q.quarter_end('2026-Q4'), date(2026, 12, 31)); self.assertEqual(Q.quarter_end('2026-Q1'), date(2026, 3, 31))
        self.assertEqual(Q.last_ended_quarter(date(2026, 9, 19)), '2026-Q2'); self.assertEqual(Q.last_ended_quarter(date(2026, 10, 1)), '2026-Q3')
        self.assertEqual(Q.last_ended_quarter(date(2026, 12, 31)), '2026-Q3'); self.assertEqual(Q.last_ended_quarter(date(2027, 1, 1)), '2026-Q4'); self.assertEqual(Q.last_ended_quarter(date(2026, 1, 5)), '2025-Q4')

class Close(unittest.TestCase):
    def test_nothing_due_when_quarter_already_closed(self):
        H = FakeH(); H.db['quarter_closes'] = [{'quarter': '2026-Q2', 'closed_at': 'x'}]
        stats = {}; Q.run(9, stats, H, today=date(2026, 9, 19))
        self.assertFalse(H.inserts); self.assertEqual(Q.status['last_closed'], '2026-Q2'); self.assertEqual(H._state['quarter_closed'], ['2026-Q2'])
        n = len(H.calls); Q.run(9, stats, H, today=date(2026, 9, 20)); self.assertEqual(len(H.calls), n)     # cached in state → no REST read

    def test_first_sync_after_quarter_end_closes_it(self):
        H = FakeH(); H._state['quarter_closed'] = ['2026-Q2']
        stats = {}; Q.run(9, stats, H, today=date(2026, 10, 1))
        snaps = [r for t, rs in H.inserts if t == 'campaign_metric_snapshots' for r in rs]
        self.assertEqual({r['campaign_id'] for r in snaps}, {'u1', 'u2', 'u3'})          # Q3 campaigns only; deleted / unregistered / Q2 excluded
        by = {r['campaign_id']: r for r in snaps}
        self.assertEqual((by['u1']['total_calls'], by['u1']['is_final'], by['u1']['extras']['settled_at_close'], by['u1']['extras']['source']), (10, True, True, 'dashboard_cache'))
        self.assertEqual((by['u2']['total_calls'], by['u2']['is_final'], by['u2']['s1_is_manual']), (2, False, True))         # still running: frozen as-of quarter end, not final
        self.assertEqual((by['u3']['total_calls'], by['u3']['extras']['source']), (7, 'latest_snapshot'))                 # not in the dashboard cache → last stored snapshot
        for r in snaps: self.assertEqual((r['snapshot_kind'], r['as_of_date'], r['extras']['quarter'], r['sync_run_id']), ('quarter_close', '2026-09-30', '2026-Q3', 9))
        led = [r for t, rs in H.inserts if t == 'quarter_closes' for r in rs][0]
        self.assertEqual((led['quarter'], led['quarter_end'], led['campaigns'], led['snapshots_written'], led['settled_campaigns'], led['retroactive']), ('2026-Q3', '2026-09-30', 3, 3, 2, False))
        self.assertEqual(stats['quarter_close']['snapshots_written'], 3); self.assertEqual(H.dq[0][0], 'quarter_closed'); self.assertFalse(H._errors)
        self.assertIn('QUARTER 2026-Q3 CLOSED: 3 campaigns', Q.summary())
        n = len(H.inserts); Q.run(9, stats, H, today=date(2026, 10, 2)); self.assertEqual(len(H.inserts), n)     # second sync: nothing

    def test_resume_after_partial_write(self):
        H = FakeH(); H._state['quarter_closed'] = ['2026-Q2']
        H.db['campaign_metric_snapshots'] = [{'campaign_id': 'u1', 'snapshot_kind': 'quarter_close', 'extras': {'quarter': '2026-Q3'}}]   # written by a crashed run; no ledger row
        Q.run(9, {}, H, today=date(2026, 10, 1))
        snaps = [r for t, rs in H.inserts if t == 'campaign_metric_snapshots' for r in rs]
        self.assertEqual({r['campaign_id'] for r in snaps}, {'u2', 'u3'})                     # u1 not written twice
        led = [r for t, rs in H.inserts if t == 'quarter_closes' for r in rs][0]; self.assertEqual(led['snapshots_written'], 3)

    def test_missing_values_are_an_error_not_a_zero(self):
        H = FakeH(); H._state['quarter_closed'] = ['2026-Q2']; H.db['v_campaign_latest_snapshot'] = []
        Q.run(9, {}, H, today=date(2026, 10, 1))
        snaps = [r for t, rs in H.inserts if t == 'campaign_metric_snapshots' for r in rs]
        self.assertEqual({r['campaign_id'] for r in snaps}, {'u1', 'u2'}); self.assertTrue(any('neither a cached row' in e for e in H._errors))

    def test_read_failure_is_reported_not_raised(self):
        H = FakeH(); H._state['quarter_closed'] = []
        def boom(*a, **k): raise RuntimeError('503 upstream')
        H._http = boom
        Q.run(9, {}, H, today=date(2026, 10, 1)); self.assertIn('503', Q.status['error']); self.assertTrue(H._errors); self.assertIn('ERROR', Q.summary())

if __name__ == '__main__':
    unittest.main()
