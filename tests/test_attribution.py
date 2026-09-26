"""attribution.py — the forward-tracking step (migration 0019): RPC calls, state mirroring, Slack line, health."""
import os, sys, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE); sys.path.insert(0, ROOT)
os.environ.setdefault('SUPABASE_SERVICE_KEY', 'x')
import attribution as A
import health

class StubH:
    def __init__(self, answers, fail=None):
        self.calls = []; self.answers = answers; self.fail = fail
        self._state = {'primary': {'m0:2026-09-01': 'c0'}, 'links': ['m0:2026-09-01|c0']}
    def _http(self, method, path, params=None, body=None, timeout=90, **kw):
        self.calls.append((path, body))
        if self.fail and path.endswith(self.fail): raise RuntimeError('HTTP 500 boom')
        return self.answers.get(path.split('/')[-1], [])

GRACE = [{'o_meeting_id': 'm1:2026-09-21', 'o_campaign_id': 'c1', 'o_campaign_name': 'HighIntent_3Sep_Isaac', 'o_action': 'B: member_grace_30d'},
         {'o_meeting_id': 'm2:2026-09-03', 'o_campaign_id': None, 'o_campaign_name': None, 'o_action': 'flag: unattributed_meeting'}]
LINE = '📣 attribution since last sync — meetings generated 3 (High Intent Data 2 · 2026 Q3 Conferences 1) · became done 1 · deals created 1 (Equity Health → 2026 Q3 CHI Conference, Sukhneet) · ⚠ 1 flag(s)'

class Attribution(unittest.TestCase):
    def test_run_calls_all_four_rpcs_in_order_and_mirrors_new_primaries(self):
        h = StubH({'attribute_meetings_grace': GRACE, 'refresh_done_on': [{'set_now': 2, 'by_source': 'sfdc_status_history'}, {'set_now': 1, 'by_source': 'scheduled_time_proxy'}],
                   'build_daily_digest': [{'kind': 'meeting_generated', 'n': 3}, {'kind': 'meeting_done', 'n': 1}, {'kind': 'deal_created', 'n': 1}, {'kind': 'flag', 'n': 1}], 'digest_line': LINE})
        stats = {}; A.run(46, stats, h)
        self.assertEqual([c[0] for c in h.calls], ['rpc/attribute_meetings_grace', 'rpc/refresh_done_on', 'rpc/build_daily_digest', 'rpc/digest_line'])
        self.assertTrue(all(c[1] == {'p_run': 46} for c in h.calls))
        self.assertEqual(h._state['primary']['m1:2026-09-21'], 'c1'); self.assertIn('m1:2026-09-21|c1', h._state['links'])   # writer state learns the SQL primary
        self.assertEqual(h._state['primary']['m0:2026-09-01'], 'c0')                                                          # existing untouched
        self.assertEqual((stats['attr_grace_b'], stats['attr_flags'], stats['done_on_set']), (1, 1, 3))
        self.assertEqual(stats['digest'], {'meeting_generated': 3, 'meeting_done': 1, 'deal_created': 1, 'flag': 1})
        self.assertEqual(A.digest_line(), LINE); self.assertIn('grace +1, 1 flag(s), done_on +3', A.summary())

    def test_quiet_sync_still_says_something(self):
        h = StubH({'attribute_meetings_grace': [], 'refresh_done_on': [], 'build_daily_digest': [], 'digest_line': []})
        A.run(47, {}, h)
        self.assertIn('no new meetings', A.digest_line()); self.assertIn('grace +0', A.summary())

    def test_failure_is_visible_in_line_summary_and_health(self):
        h = StubH({}, fail='refresh_done_on')
        with self.assertRaises(RuntimeError): A.run(48, {}, h)                                 # flush's per-step guard catches it…
        A.status['error'] = 'RuntimeError: HTTP 500 boom'                                       # …and records it like history.flush does
        self.assertIn('⚠️ not computed: RuntimeError', A.digest_line()); self.assertIn('attribution ERROR', A.summary())
        class FakeHist: _summary = {'enabled': True, 'stats': {}}; _errors = []; A = A; _last_ok_at = None; T = type('T', (), {'status': {}}); LH = type('L', (), {'status': {}}); OP = type('O', (), {'status': {}})
        ok, lines = health.build(cache={'campaigns': [], 'totals': {}, 'last_sync': None}, weekly=None, coldcalls=None, history=FakeHist, backup_status=lambda: {'ok': True, 'count': 3}, probe=lambda *a, **k: (True, ''))
        self.assertTrue(any('❌ Attribution: RuntimeError' in l for l in lines), lines)

    def test_skipped_is_flagged_not_silent(self):
        A.status.clear(); A.status['skipped'] = 'flush budget exhausted before the attribution step'
        self.assertIn('⚠️ not computed (flush budget', A.digest_line())

    def test_wired_into_flush_slack_and_health(self):
        hist = open(os.path.join(ROOT, 'history.py')).read(); app = open(os.path.join(ROOT, 'app.py')).read(); hl = open(os.path.join(ROOT, 'health.py')).read()
        i_opp = hist.index("('opportunities', lambda"); i_att = hist.index("('attribution', lambda: A.run(run_id, stats, _this))")
        self.assertLess(i_opp, i_att)                                                           # runs after opportunities (digest needs deals in)
        self.assertIn("A.status['skipped'] = 'flush budget exhausted", hist); self.assertIn('def digest_line', hist)
        i_hist = app.index('f"🗄️ {history.summary()}"'); i_dl = app.index('history.digest_line()')
        self.assertLess(i_hist, i_dl); self.assertLess(i_dl, app.index('lines += health_lines'))    # same Slack message, right after the history line
        self.assertIn('4f. Attribution tracking', hl)

if __name__ == '__main__':
    unittest.main()
