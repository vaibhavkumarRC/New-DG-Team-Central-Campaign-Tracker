"""refresh_from_sync(): the sync-triggered refresh of the Weekly Review and Cold-calls snapshots."""
import os, sys, time, unittest, threading
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE); sys.path.insert(0, ROOT)
os.environ.setdefault('SUPABASE_SERVICE_KEY', 'x')
import weekly_review as WR, cold_calls as CC

class SyncRefresh(unittest.TestCase):
    def _check(self, mod, fn_name, store_name):
        orig = getattr(mod, fn_name)
        try:
            calls = []
            setattr(mod, fn_name, lambda: calls.append(1))
            self.assertEqual(mod.refresh_from_sync(timeout_s=5), 'refreshed'); self.assertEqual(calls, [1]); self.assertFalse(mod._refreshing)
            # failure: reported, old snapshot keeps its data, error recorded on it
            setattr(mod, store_name, {'fetched_at': '2026-09-19T12:32:00+05:30', 'rows': [1]})
            def boom(): raise RuntimeError('HTTP 503 upstream')
            setattr(mod, fn_name, boom)
            r = mod.refresh_from_sync(timeout_s=5); self.assertTrue(r.startswith('failed: HTTP 503'), r)
            st = getattr(mod, store_name); self.assertEqual(st['rows'], [1]); self.assertIn('503', st['error']); self.assertFalse(mod._refreshing)
            # slow: the sync is not blocked beyond the timeout; the refresh finishes in the background
            done = threading.Event()
            def slow(): time.sleep(0.3); done.set()
            setattr(mod, fn_name, slow)
            t0 = time.time(); r = mod.refresh_from_sync(timeout_s=0.05); self.assertIn('still running', r); self.assertLess(time.time() - t0, 0.25)
            self.assertTrue(done.wait(2)); time.sleep(0.05); self.assertFalse(mod._refreshing)
            # already refreshing: skipped, never double-runs
            mod._refreshing = True
            try: self.assertIn('skipped', mod.refresh_from_sync(timeout_s=1))
            finally: mod._refreshing = False
        finally:
            setattr(mod, fn_name, orig)

    def test_weekly_review(self): self._check(WR, 'refresh_weekly', '_WR')
    def test_cold_calls(self): self._check(CC, 'refresh_cold_calls', '_CC')

    def test_sync_calls_both_after_history_flush(self):
        src = open(os.path.join(ROOT, 'app.py')).read()
        i_flush = src.index('history.flush()'); i_ref = src.index("_mod.refresh_from_sync()")
        self.assertLess(i_flush, i_ref)                                            # after the history layer, before the Slack message
        self.assertLess(i_ref, src.index('def _notify_slack_sync'))
        self.assertIn("('weekly_review', weekly_review), ('cold_calls', cold_calls)", src)

if __name__ == '__main__':
    unittest.main()
