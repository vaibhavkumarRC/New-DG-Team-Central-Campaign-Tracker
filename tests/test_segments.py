"""Segment rename/delete endpoints against temporary data files (no Salesforce, no network)."""
import json, os, sys, tempfile, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE); sys.path.insert(0, ROOT)
os.environ.setdefault('SUPABASE_SERVICE_KEY', 'x'); os.environ.pop('HISTORY_SUPABASE_URL', None); os.environ.pop('SLACK_WEBHOOK_URL', None)
import app as A

class Segments(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        A.CAMPS_FILE = os.path.join(self.tmp, 'campaigns.json'); A.SEGMENTS_FILE = os.path.join(self.tmp, 'segments.json')
        camps = [{'id': '1', 'name': 'HighIntent_A', 'segment': 'High Intent', 'status': 'Completed'},
                 {'id': '2', 'name': 'HighIntent_B', 'segment': 'High Intent Data', 'status': 'Active'},
                 {'id': '3', 'name': 'Dinner_X', 'segment': 'Geolocation DInner', 'status': 'Completed'}]
        json.dump(camps, open(A.CAMPS_FILE, 'w')); json.dump(['Geolocation DInner', 'Central Campaign S&O Teams', 'POD Calling'], open(A.SEGMENTS_FILE, 'w'))
        A.cache['campaigns'] = [dict(c, total_calls=10) for c in camps]
        self.c = A.app.test_client(); self.h = {'X-Admin-Token': A.ADMIN_TOKEN, 'Content-Type': 'application/json'}

    def test_rename_merges_into_existing_name_everywhere(self):
        r = self.c.post('/api/segments/rename', data=json.dumps({'old': 'High Intent', 'new': 'High Intent Data'}), headers=self.h)
        self.assertEqual(r.status_code, 200, r.data); d = r.get_json(); self.assertEqual(d['renamed'], 1)
        camps = json.load(open(A.CAMPS_FILE)); self.assertEqual([c['segment'] for c in camps], ['High Intent Data', 'High Intent Data', 'Geolocation DInner'])
        self.assertEqual([c['segment'] for c in A.cache['campaigns']][:2], ['High Intent Data', 'High Intent Data'])   # live cache too
        self.assertEqual(A.cache['campaigns'][0]['total_calls'], 10)                                                   # numbers untouched
        self.assertNotIn('High Intent', d['segments']); self.assertEqual(d['segments'].count('High Intent Data'), 1)

    def test_rename_typo_and_dropdown(self):
        r = self.c.post('/api/segments/rename', data=json.dumps({'old': 'Geolocation DInner', 'new': 'Geolocation Dinner'}), headers=self.h)
        d = r.get_json(); self.assertEqual(d['renamed'], 1); self.assertIn('Geolocation Dinner', d['segments']); self.assertNotIn('Geolocation DInner', d['segments'])
        self.assertEqual(json.load(open(A.SEGMENTS_FILE)), ['Central Campaign S&O Teams', 'POD Calling', 'Geolocation Dinner'])

    def test_delete_only_when_unused(self):
        r = self.c.delete('/api/segments/POD%20Calling', headers=self.h); self.assertEqual(r.status_code, 200)      # unused custom name
        r = self.c.delete('/api/segments/Geolocation%20DInner', headers=self.h); self.assertEqual(r.status_code, 409); self.assertIn('1 campaign', r.get_json()['error'])
        r = self.c.delete('/api/segments/EPIC%20Campaign', headers=self.h); self.assertEqual(r.status_code, 409)     # built-in
        r = self.c.delete('/api/segments/Central%20Campaign%20S%26O%20Teams', headers=self.h); self.assertEqual(r.status_code, 200)
        self.assertEqual(json.load(open(A.SEGMENTS_FILE)), ['Geolocation DInner'])

    def test_requires_admin_and_validates(self):
        self.assertEqual(self.c.post('/api/segments/rename', data=json.dumps({'old': 'a', 'new': 'b'}), headers={'Content-Type': 'application/json'}).status_code, 403)
        self.assertEqual(self.c.post('/api/segments/rename', data=json.dumps({'old': 'a', 'new': 'a'}), headers=self.h).status_code, 400)
        self.assertEqual(self.c.post('/api/segments/rename', data=json.dumps({'old': '', 'new': 'b'}), headers=self.h).status_code, 400)
        self.assertEqual(json.load(open(A.CAMPS_FILE))[0]['segment'], 'High Intent')                                   # nothing changed

class ManageLayout(unittest.TestCase):
    def test_manage_grid_has_exactly_two_columns(self):
        """.manage-layout is a 2-column grid (form | list); a third direct child wraps the campaign list into the narrow form column."""
        from html.parser import HTMLParser
        src = open(os.path.join(ROOT, 'templates', 'index.html')).read()
        VOID = {'input', 'br', 'img', 'hr', 'meta', 'link', 'option'}
        class P(HTMLParser):
            def __init__(self): super().__init__(); self.depth = None; self.stack = 0; self.children = []; self.seg_depth = None
            def handle_starttag(self, tag, attrs):
                if tag in VOID: return
                a = dict(attrs); self.stack += 1
                if 'manage-layout' in (a.get('class') or ''): self.depth = self.stack
                elif self.depth and self.stack == self.depth + 1: self.children.append(a.get('class') or tag)
                if a.get('id') == 'segmentAdminList' and self.depth: self.seg_depth = self.stack
            def handle_endtag(self, tag):
                if tag in VOID: return
                if self.depth and self.stack == self.depth: self.depth = None
                self.stack -= 1
        p = P(); p.feed(src)
        self.assertEqual(len(p.children), 2, p.children)
        self.assertEqual(p.children[0], 'form-card')
        self.assertIsNotNone(p.seg_depth)                                                                   # segments panel sits inside column 2

if __name__ == '__main__':
    unittest.main()
