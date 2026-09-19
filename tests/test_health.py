import os, sys, unittest, types, tempfile
from datetime import datetime, timezone, timedelta
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
import health

def iso(hours_ago): return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()

class HealthBlock(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.cache = {'errors': [], 'fatal_error': None}
        self.weekly = types.SimpleNamespace(_WR={'fetched_at': iso(3), 'error': None})
        self.cold = types.SimpleNamespace(_CC={'fetched_at': iso(20), 'error': None})
        self.hist = types.SimpleNamespace(_summary={'enabled': True, 'stats': {'errors': 0}}, _errors=[], _last_ok_at=iso(1), _queue_path=lambda: os.path.join(self.tmp, 'q.jsonl'),
                                          T=types.SimpleNamespace(status={'last_ok_at': iso(1), 'watermark': iso(2), 'error': None, 'backlog': False, 'new': 12, 'skipped': None}))
        self.backup = {'stamp': 'x', 'local': {'meeting_ledger.json': 'ok', 'campaigns.json': 'ok', 'segments.json': 'ok'}, 'github': {'meeting_ledger.json': 'ok', 'campaigns.json': 'ok', 'segments.json': 'ok'}}
        os.environ['SUPABASE_SERVICE_KEY'] = 'k'; os.environ['HISTORY_SUPABASE_URL'] = 'https://h.example'; os.environ['HISTORY_SUPABASE_KEY'] = 'k2'
    def run_(self, probe=lambda url, key: None):
        return health.build(cache=self.cache, weekly=self.weekly, coldcalls=self.cold, history=self.hist, backup_status=self.backup, probe=probe)

    def test_all_ok(self):
        ok, lines = self.run_(); self.assertTrue(ok); self.assertEqual(len(lines), 1)
        for w in ('Sync', 'Weekly Review', 'Cold calls', 'History', 'Touches +12', 'Backups 3/3', 'Supabase keys'): self.assertIn(w, lines[0])

    def test_stale_weekly_with_error_text(self):
        self.weekly._WR = {'fetched_at': iso(14 * 24), 'error': 'refresh failed 2026-09-18: HTTP Error 401: Unauthorized'}
        ok, lines = self.run_(); self.assertFalse(ok)
        self.assertTrue(any('Weekly Review' in l and '401' in l for l in lines), lines)

    def test_dead_key_probe(self):
        ok, lines = self.run_(probe=lambda url, key: 'HTTP 401 Legacy API keys are disabled' if 'companies' in url else None)
        self.assertFalse(ok); self.assertTrue(any('SUPABASE_SERVICE_KEY' in l and 'Legacy' in l for l in lines), lines)

    def test_backup_failure_and_queue(self):
        self.backup['github']['campaigns.json'] = 'HTTP 401 Bad credentials'
        with open(os.path.join(self.tmp, 'q.jsonl'), 'w') as f: f.write('{}\n{}\n')
        ok, lines = self.run_(); self.assertFalse(ok)
        self.assertTrue(any('GitHub campaigns.json: HTTP 401' in l for l in lines), lines)
        self.assertTrue(any('2 batch(es) queued' in l for l in lines), lines)

    def test_fatal_and_writer_error(self):
        self.cache['fatal_error'] = 'Traceback...\nRuntimeError: boom'
        self.hist._errors = ['company lookup failed: HTTP 401']
        ok, lines = self.run_(); self.assertFalse(ok)
        self.assertTrue(any('crashed' in l and 'boom' in l for l in lines)); self.assertTrue(any('History writer' in l and '401' in l for l in lines))

    def test_check_crash_is_reported_not_raised(self):
        self.weekly = None; self.cold = types.SimpleNamespace(_CC={'fetched_at': 'garbage', 'error': None})
        ok, lines = self.run_(); self.assertFalse(ok); self.assertTrue(any('Weekly Review: no snapshot' in l for l in lines)); self.assertTrue(any('Cold calls' in l for l in lines))

    def test_touches_states(self):
        self.hist.T.status = {'last_ok_at': None, 'error': 'HTTP 503 upstream connect error', 'skipped': None}
        ok, lines = self.run_(); self.assertFalse(ok); self.assertTrue(any('❌ Touches' in l and '503' in l for l in lines), lines)
        self.hist.T.status = {'last_ok_at': None, 'error': None, 'skipped': 'flush budget exhausted before the touches step; catches up next sync'}
        ok, lines = self.run_(); self.assertTrue(any('⚠️ Touches: not ingested' in l and 'budget' in l for l in lines), lines)
        self.hist.T.status = {'last_ok_at': iso(40), 'error': None, 'skipped': None}
        ok, lines = self.run_(); self.assertTrue(any('Touches: last successful ingest 40' in l for l in lines), lines)
        self.hist.T.status = {'last_ok_at': iso(1), 'error': None, 'skipped': None, 'backlog': True, 'new': 20000}
        ok, lines = self.run_(); self.assertTrue(any('Touches: partial' in l and 'row cap' in l for l in lines), lines)
        self.hist.T.status = {'last_ok_at': iso(1), 'error': None, 'skipped': None, 'backlog': False, 'new': 0}
        ok, lines = self.run_(); self.assertTrue(ok, lines); self.assertIn('Touches +0', lines[0])

if __name__ == '__main__':
    unittest.main()
