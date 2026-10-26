"""Admin token hardening (26 Oct 2026): no built-in fallback, whitespace-tolerant,
fail-closed when unset; SDR delete endpoint; SDR file in the backup set. No network."""
import json, os, sys, tempfile, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE); sys.path.insert(0, ROOT)
os.environ.setdefault('SUPABASE_SERVICE_KEY', 'x'); os.environ.pop('HISTORY_SUPABASE_URL', None); os.environ.pop('SLACK_WEBHOOK', None)
os.environ.setdefault('ADMIN_TOKEN', '  test-admin-token \n')   # deliberately padded: must be trimmed
import app as A


class AdminToken(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        A.CAMPS_FILE = os.path.join(self.tmp, 'campaigns.json')
        A.LEDGER_FILE = os.path.join(self.tmp, 'meeting_ledger.json')
        A.SDRS_FILE = os.path.join(self.tmp, 'sdrs.json')
        with open(A.CAMPS_FILE, 'w') as f:
            json.dump([{'id': '1', 'name': 'ZZZ-PENTEST-ARTIFACT-IGNORE', 'sdr_owner': 'pentest', 'status': 'Active'}], f)
        with open(A.SDRS_FILE, 'w') as f:
            json.dump(['ZZZSDR-PENTEST', 'pentest', 'VERIFY1408-SDR'], f)
        A.app.config['TESTING'] = True
        self.c = A.app.test_client()

    def test_env_token_is_trimmed_and_no_fallback(self):
        self.assertEqual(A.ADMIN_TOKEN, 'test-admin-token')
        self.assertNotIn('rc-admin-2026', open(os.path.join(ROOT, 'app.py')).read())

    def test_header_token_trimmed_and_old_default_rejected(self):
        ok = self.c.post('/api/sdrs', data=json.dumps({'name': 'X'}), headers={'X-Admin-Token': ' test-admin-token ', 'Content-Type': 'application/json'})
        self.assertEqual(ok.status_code, 200)
        bad = self.c.post('/api/sdrs', data=json.dumps({'name': 'Y'}), headers={'X-Admin-Token': 'rc-admin-2026', 'Content-Type': 'application/json'})
        self.assertEqual(bad.status_code, 403)
        none = self.c.post('/api/sdrs', data=json.dumps({'name': 'Y'}), headers={'Content-Type': 'application/json'})
        self.assertEqual(none.status_code, 403)
        chk = self.c.post('/api/check-admin', data=json.dumps({'token': 'test-admin-token'}), headers={'Content-Type': 'application/json'})
        self.assertTrue(chk.get_json()['ok'])
        chk = self.c.post('/api/check-admin', data=json.dumps({'token': ''}), headers={'Content-Type': 'application/json'})
        self.assertFalse(chk.get_json()['ok'])

    def test_fail_closed_when_token_unset(self):
        saved = A.ADMIN_TOKEN
        try:
            A.ADMIN_TOKEN = None
            r = self.c.post('/api/sdrs', data=json.dumps({'name': 'X'}), headers={'X-Admin-Token': '', 'Content-Type': 'application/json'})
            self.assertEqual(r.status_code, 403)
            chk = self.c.post('/api/check-admin', data=json.dumps({'token': ''}), headers={'Content-Type': 'application/json'})
            self.assertFalse(chk.get_json()['ok'])
        finally:
            A.ADMIN_TOKEN = saved

    def test_sdr_delete(self):
        h = {'X-Admin-Token': 'test-admin-token'}
        # in use by a campaign → refused
        r = self.c.delete('/api/sdrs/pentest', headers=h); self.assertEqual(r.status_code, 409)
        # built-in → refused
        r = self.c.delete('/api/sdrs/' + A.DEFAULT_SDRS[0], headers=h); self.assertEqual(r.status_code, 409)
        # unknown → 404
        r = self.c.delete('/api/sdrs/nobody', headers=h); self.assertEqual(r.status_code, 404)
        # unused custom → removed
        r = self.c.delete('/api/sdrs/ZZZSDR-PENTEST', headers=h); self.assertEqual(r.status_code, 200)
        self.assertEqual(json.load(open(A.SDRS_FILE)), ['pentest', 'VERIFY1408-SDR'])
        # without token → 403 and file untouched
        r = self.c.delete('/api/sdrs/VERIFY1408-SDR'); self.assertEqual(r.status_code, 403)
        self.assertEqual(json.load(open(A.SDRS_FILE)), ['pentest', 'VERIFY1408-SDR'])

    def test_sdrs_file_is_backed_up(self):
        A.BACKUP_DIR = os.path.join(self.tmp, 'backups')
        os.environ.pop('BACKUP_GITHUB_TOKEN', None)
        A.backup_data_files()
        self.assertEqual(A._backup_status['local'].get('sdrs.json'), 'ok')
        self.assertTrue(any(n.startswith('sdrs.json.') for n in os.listdir(A.BACKUP_DIR)))


if __name__ == '__main__':
    unittest.main()
