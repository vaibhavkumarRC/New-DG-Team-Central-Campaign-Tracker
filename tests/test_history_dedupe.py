"""history._dedupe — one row per conflict key (last wins); replay of a queued duplicate chunk succeeds."""
import json, os, sys, tempfile, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE); sys.path.insert(0, ROOT)
import history as H


class Dedupe(unittest.TestCase):
    def test_last_wins_single_key(self):
        rows = [{'lead_id': 'a', 'v': 1}, {'lead_id': 'b', 'v': 2}, {'lead_id': 'a', 'v': 3}]
        out = H._dedupe(rows, 'lead_id')
        self.assertEqual(sorted((r['lead_id'], r['v']) for r in out), [('a', 3), ('b', 2)])

    def test_composite_key_and_passthrough(self):
        rows = [{'m': 1, 'c': 1}, {'m': 1, 'c': 2}, {'m': 1, 'c': 1}]
        self.assertEqual(len(H._dedupe(rows, 'm,c')), 2)
        self.assertIs(H._dedupe(rows, None), rows)      # no key → untouched

    def test_replay_dedupes_queued_insert(self):
        tmp = tempfile.mkdtemp(); H._cfg['data_dir'] = tmp
        with open(H._queue_path(), 'w') as f:
            f.write(json.dumps({'op': 'insert', 'table': 'leads', 'on_conflict': 'lead_id', 'prefer': 'resolution=merge-duplicates',
                                'rows': [{'lead_id': 'x', 'v': 1}, {'lead_id': 'x', 'v': 2}]}) + '\n')
        sent = []
        orig = H._http
        H._http = lambda method, path, params=None, body=None, prefer=None, timeout=60: sent.append((method, path, body)) or []
        try:
            ok = H._replay_queue()
        finally:
            H._http = orig
        self.assertEqual(ok, 1)
        self.assertEqual(sent[0][2], [{'lead_id': 'x', 'v': 2}])
        self.assertFalse(os.path.exists(H._queue_path()))


if __name__ == '__main__':
    unittest.main()
