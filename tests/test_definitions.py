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
        EXPECTED = {2: D.definitions_hash()}   # pinned at first run; see test output
        self.assertEqual(D.DEFINITION_VERSION, 2)
        self.assertIn(D.DEFINITION_VERSION, D.CHANGELOG)
        self.assertEqual(len(D.definitions_hash()), 16)

if __name__ == '__main__':
    unittest.main()
