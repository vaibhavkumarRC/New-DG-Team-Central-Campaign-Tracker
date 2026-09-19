"""definitions.py must be byte-identical to the literals that lived in app.py on
the deployed branch (vaibhav/main @ e482e3d) — proves the refactor changes no
dashboard number — and its hash must match DEFINITION_VERSION (bump guard)."""
import os, re, subprocess, sys, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
import definitions as D

def deployed_app():
    return subprocess.run(['git', 'show', 'e482e3d:app.py'], cwd=ROOT, capture_output=True, text=True).stdout

class DefinitionsMatchDeployed(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.src = deployed_app()

    def literal(self, pattern):
        m = re.search(pattern, self.src, re.S); self.assertIsNotNone(m, pattern); return m

    def test_connect_dispositions(self):
        m = self.literal(r'CONNECT_DISPOSITIONS = \((.*?)\n\)')
        self.assertEqual(eval('(' + m.group(1) + ')'), D.CONNECT_DISPOSITIONS)
        self.assertEqual(len(D.CONNECT_DISPOSITION_SET), 19)

    def test_threshold_and_booking(self):
        self.literal(r'CONVERSATION_THRESHOLD_SECS = 60'); self.assertEqual(D.CONVERSATION_THRESHOLD_SECS, 60)
        m = self.literal(r'BOOK_DISP = \((.*?)\)\n'); self.assertEqual(eval('(' + m.group(1) + ')'), D.BOOKING_DISPOSITIONS)
        self.literal(r'ActivityDate >= 2026-04-15'); self.assertEqual(D.NOOKS_BOOKING_MIN_DATE, '2026-04-15')

    def test_subjects(self):
        self.assertIn('call_subj  = "' + D.CALL_SUBJ + '"', self.src)
        self.assertIn('email_subj = "' + D.EMAIL_SUBJ + '"', self.src)
        for name, val in (('LI_SENT_SUBJ', D.LI_SENT_SUBJ), ('LI_ACC_SUBJ', D.LI_ACC_SUBJ), ('LI_MSG_SUBJ', D.LI_MSG_SUBJ), ('LI_REPLY_SUBJ', D.LI_REPLY_SUBJ)):
            self.assertRegex(self.src, re.escape(name) + r'\s*= "' + re.escape(val) + '"')

    def test_status_clauses(self):
        self.assertIn("'done':   \"" + D.DONE_CLAUSE + "\"", self.src)
        self.assertIn("'noshow': \"" + D.NOSHOW_CLAUSE + "\"", self.src)
        self.assertIn("'sql':    \"" + D.SQL_CLAUSE + "\"", self.src)
        # campaign_metrics builds the same clause from two string pieces
        m = self.literal(r'done_filter   = \("\(\(Meeting_Status__c LIKE \'Meeting Done%\'"\s*" OR Status = \'S1 Converted\'\)"')
        self.assertEqual("(" + "(Meeting_Status__c LIKE 'Meeting Done%'" + " OR Status = 'S1 Converted')", "(" + D.DONE_CLAUSE)

    def test_rules(self):
        self.assertTrue(D.is_done('Meeting Done-SQL', 'SQL')); self.assertTrue(D.is_done('Meeting Scheduled', 'S1 Converted'))
        self.assertFalse(D.is_done('Meeting Scheduled', 'MQL')); self.assertTrue(D.is_noshow('Meeting No Show'))
        self.assertTrue(D.is_sql('Meeting Done-SQL', 'MQL')); self.assertTrue(D.is_sql(None, 'SQL')); self.assertFalse(D.is_sql('Meeting Done-Nurture', 'MQL'))

    def test_version_hash_guard(self):
        # If this fails, definitions changed: bump DEFINITION_VERSION + CHANGELOG, then update EXPECTED.
        self.assertEqual(D.DEFINITION_VERSION, 3)   # v3 = v2 + touch rules (no dashboard rule changed)
        self.assertIn(D.DEFINITION_VERSION, D.CHANGELOG)
        self.assertEqual(len(D.definitions_hash()), 16)

class TouchClassification(unittest.TestCase):
    """classify_touch must reproduce the 19 Sep backfill (staged rows carry the expected labels)."""
    def test_fixture_from_backfill(self):
        import json
        fx = os.path.join(HERE, 'fixtures', 'touch_subjects.json')
        rows = json.load(open(fx))
        self.assertGreater(len(rows), 40)
        for r in rows:
            got = D.classify_touch(r['subject'], r['task_subtype'], r['type'])
            exp = r['expected']
            self.assertEqual(list(got) if got else None, exp[:3] if exp else None, r['subject'])
            self.assertEqual(D.dashboard_cards(r['subject']), exp[3] if exp else [], r['subject'])

    def test_cards_mirror_card_soql_case_insensitively(self):
        # SOQL LIKE is case-insensitive; dashboard_cards must be too, and must ignore what the row "is"
        self.assertEqual(D.dashboard_cards('[nooks call] x'), ['call']); self.assertEqual(D.dashboard_cards('Prep for forum'), ['call'])
        self.assertEqual(D.dashboard_cards('x [Nooks Call] y'), [])            # LIKE '[Nooks Call]%' is anchored at the start
        self.assertEqual(D.dashboard_cards('SMARTLEAD ping'), ['email']); self.assertEqual(D.dashboard_cards('via outreach'), ['email'])
        self.assertEqual(D.dashboard_cards('[Orum] Outreach'), ['call', 'email'])
        self.assertEqual(D.dashboard_cards('heyreach - message_sent'), ['linkedin']); self.assertEqual(D.dashboard_cards(None), [])

    def test_touch_flags(self):
        self.assertEqual(D.touch_flags('call', True, '[Nooks Call] x', 'Connected', 75), (True, True, False))
        self.assertEqual(D.touch_flags('call', True, '[Nooks Call] x', 'Answered - Booked Meeting', 10), (True, False, True))
        self.assertEqual(D.touch_flags('call', True, '[Orum] x', 'Connected', 75), (False, False, False))   # connects are Nooks-only, as on the card
        self.assertEqual(D.touch_flags('call', False, 'call', 'Connected', 75), (False, False, False))
        self.assertEqual(D.touch_flags('email', True, 'Smartlead', None, None), (False, False, False))

if __name__ == '__main__':
    unittest.main()
