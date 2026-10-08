"""Campaign names must be unique (case/whitespace-insensitive) on add and rename — no Salesforce, no network."""
import json, os, sys, tempfile, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE); sys.path.insert(0, ROOT)
os.environ.setdefault('SUPABASE_SERVICE_KEY', 'x'); os.environ.pop('HISTORY_SUPABASE_URL', None); os.environ.pop('SLACK_WEBHOOK', None)
import app as A


class UniqueCampaignNames(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        A.CAMPS_FILE = os.path.join(self.tmp, 'campaigns.json')
        A.LEDGER_FILE = os.path.join(self.tmp, 'meeting_ledger.json')
        camps = [{'id': '1', 'name': 'denials_revenue_leakage_Sukhneet_9july', 'sdr_owner': 'Sukhneet Sukhneet', 'start_date': '2026-07-09', 'status': 'Completed'},
                 {'id': '2', 'name': 'HighIntent_7Oct_Saka', 'sdr_owner': 'Saka Thapa', 'status': 'Active'}]
        with open(A.CAMPS_FILE, 'w') as f: json.dump(camps, f)
        A.app.config['TESTING'] = True
        self.c = A.app.test_client()
        self.h = {'X-Admin-Token': A.ADMIN_TOKEN, 'Content-Type': 'application/json'}

    def test_helper(self):
        camps = A.load_campaigns()
        self.assertEqual(A._campaign_name_taken(camps, '  Denials_Revenue_Leakage_SUKHNEET_9july ')['id'], '1')
        self.assertIsNone(A._campaign_name_taken(camps, 'denials_revenue_leakage_Sukhneet_9july_part1'))
        self.assertIsNone(A._campaign_name_taken(camps, 'HighIntent_7Oct_Saka', exclude_id='2'))

    def test_add_duplicate_rejected(self):
        r = self.c.post('/api/campaigns', data=json.dumps({'name': 'denials_revenue_leakage_sukhneet_9july'}), headers=self.h)
        self.assertEqual(r.status_code, 409)
        self.assertIn('already exists', r.get_json()['error'])
        self.assertEqual(len(A.load_campaigns()), 2)

    def test_rename_to_existing_rejected_and_self_rename_ok(self):
        r = self.c.put('/api/campaigns/2', data=json.dumps({'name': 'denials_revenue_leakage_Sukhneet_9july'}), headers=self.h)
        self.assertEqual(r.status_code, 409)
        r = self.c.put('/api/campaigns/2', data=json.dumps({'name': 'HighIntent_7Oct_Saka', 'segment': 'High Intent Data'}), headers=self.h)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()['segment'], 'High Intent Data')


if __name__ == '__main__':
    unittest.main()
