"""touches.py against a fake history module: watermark discipline, budget, failures, resolution."""
import os, sys, types, unittest, urllib.error
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
import touches as T

WM = '2026-09-19T06:54:52+00:00'

def task(i, created, subject='[Nooks Call] - Connected - by SDR One', who='00QA1', activity='2026-09-19', **k):
    r = {'Id': i, 'Subject': subject, 'TaskSubtype': 'Call', 'Type': 'Call', 'WhoId': who, 'OwnerId': '005X', 'Owner': {'Name': 'SDR One'},
         'CreatedDate': created, 'ActivityDate': activity, 'CallDisposition': 'Connected', 'CallDurationInSeconds': 80, 'CallType': 'Outbound', 'Not_Relevant__c': False}
    r.update(k); return r

class FakeH:
    """Minimal stand-in for history.py: records REST traffic, answers reads."""
    def __init__(self, tasks, watermark=WM):
        self.tasks = tasks; self._errors = []; self.calls = []; self.inserts = []; self.fail_insert = False
        self._cfg = {'touch_budget': '240', 'touch_max_rows': '20000'}
        self._state = {'campaigns': {'Camp A': {'db_id': 'uuid-A', 'config': {'start_date': '2026-09-01', 'end_date': '2026-09-30'}, 'leads': ['00QA1', '00QA2']}}}
        self.db = {'touch_ingest_state': [{'watermark_created': watermark, 'updated_at': watermark}] if watermark else [],
                   'people': [{'person_key': 'sdr_one', 'display_name': 'SDR One', 'aliases': ['SDR1']}],
                   'leads': [{'lead_id': '00QA1', 'contact_id': '003C1', 'current_account_sf_id': '001ACC', 'converted_account_id': None},
                             {'lead_id': '00QA2', 'contact_id': None, 'current_account_sf_id': None, 'converted_account_id': '001CONV'}],
                   'touches': [{'touch_id': '00TOLD1'}, {'touch_id': '00TOLD2'}], 'touch_attributions': [{'touch_id': '00TATT1'}]}
        self._deps = {'soql': self.soql}
    def soql(self, q, paginate=True):
        self.calls.append(q); lim = int(q.rsplit('LIMIT ', 1)[1]); return {'records': self.tasks[:lim]}
    def _err(self, m): self._errors.append(m)
    def _short(self, e): return str(e)
    def _http(self, method, path, params=None, body=None, prefer=None, timeout=90):
        self.calls.append((method, path, params, body))
        if method == 'GET':
            rows = self.db.get(path, [])
            if path == 'leads' and params.get('contact_id'): return [r for r in rows if r['contact_id'] and r['contact_id'] in params['contact_id']]
            if path == 'leads': return [r for r in rows if r['lead_id'] in params['lead_id']]
            return rows
        if path == 'rpc/attribute_touches': return [{'inserted_primary': len(body['p_touch_ids']), 'inserted_context': 0, 'superseded': 1, 'campaign_ids': ['uuid-A']}]
        if path == 'rpc/refresh_campaign_lead_activity_for_touches': return [{'rows_upserted': 3, 'rows_deleted': 0}]
        return []
    def _get_all(self, path, params, page=1000, order=None): return self._http('GET', path, params)
    def _insert(self, table, rows, on_conflict=None, returning=False, count_col=None, **kw):
        if self.fail_insert:
            self._errors.append(f'{table} insert batch failed → queued'); return []
        self.inserts.append((table, rows)); return [{'touch_id': r['touch_id']} for r in rows]
    def watermarks(self): return [b[0]['watermark_created'] for m, p, _, b in [c for c in self.calls if isinstance(c, tuple)] if p == 'touch_ingest_state' and m == 'POST']
    def attributed(self): return [b['p_touch_ids'] for m, p, _, b in [c for c in self.calls if isinstance(c, tuple)] if p == 'rpc/attribute_touches']

class Touches(unittest.TestCase):
    def run_(self, H, deadline=None):
        stats = {}; T.ingest(7, stats, H, deadline=deadline); return stats

    def test_query_uses_overlap_and_row_cap(self):
        H = FakeH([]); self.run_(H)
        q = [c for c in H.calls if isinstance(c, str)][0]; self.assertIn('CreatedDate > 2026-09-19T04:54:52Z', q); self.assertIn('ORDER BY CreatedDate ASC LIMIT 20000', q); self.assertIn('Not_Relevant__c', q)

    def test_rows_resolved_and_flagged(self):
        H = FakeH([task('00T1', '2026-09-19T08:10:00.000+0000'),
                   task('00T2', '2026-09-19T08:11:00.000+0000', who='003C1', subject='[Outreach] [Email] [Out] hello'),     # contact → lead via leads.contact_id
                   task('00T3', '2026-09-19T08:12:00.000+0000', who='00QA2', activity=None, subject='[Outreach] [Email] [Opened] hi'),  # null ActivityDate → never counted
                   task('00T4', '2026-09-19T08:13:00.000+0000', Not_Relevant__c=True),
                   task('00T5', '2026-09-19T08:14:00.000+0000', who=None, subject='[RC] ESCALATION x')])
        stats = self.run_(H)
        rows = {r['touch_id']: r for _, rs in H.inserts for r in rs}
        self.assertEqual(set(rows), {'00T1', '00T2', '00T3'})
        self.assertEqual((rows['00T1']['is_connect'], rows['00T1']['is_conversation'], rows['00T1']['account_sf_id'], rows['00T1']['owner_person_key'], rows['00T1']['dashboard_cards']), (True, True, '001ACC', 'sdr_one', ['call']))
        self.assertEqual((rows['00T2']['who_type'], rows['00T2']['lead_id'], rows['00T2']['channel'], rows['00T2']['event'], rows['00T2']['is_counted_by_dashboard']), ('contact', '00QA1', 'email', 'email_sent', True))
        self.assertEqual((rows['00T3']['occurred_on'], rows['00T3']['is_counted_by_dashboard'], rows['00T3']['dashboard_cards'], rows['00T3']['account_sf_id']), ('2026-09-19', False, ['email'], '001CONV'))
        self.assertEqual(rows['00T1']['occurred_on_ist'], '2026-09-19'); self.assertEqual(rows['00T1']['occurred_at'], '2026-09-19T08:10:00Z')
        self.assertEqual((stats['touches_seen'], stats['touches_new'], stats['touches_attributed'], stats['touches_superseded'], stats['touches_rollup']), (5, 3, 3, 1, 3))
        self.assertEqual(H.watermarks()[-1], '2026-09-19T08:14:00Z')            # advances over skipped rows too (they were fetched)
        self.assertEqual(T.status['watermark'], '2026-09-19T08:14:00Z'); self.assertIsNone(T.status['error']); self.assertFalse(H._errors)

    def test_no_watermark_never_guesses(self):
        H = FakeH([task('00T1', '2026-09-19T08:10:00.000+0000')], watermark=None); self.run_(H)
        self.assertFalse(H.inserts); self.assertTrue(all(not isinstance(c, str) for c in H.calls))   # no SOQL issued
        self.assertIn('no watermark', T.status['error'])

    def test_write_failure_stops_before_advancing_watermark(self):
        H = FakeH([task('00T1', '2026-09-19T08:10:00.000+0000')]); H.fail_insert = True; self.run_(H)
        self.assertEqual(H.watermarks(), []); self.assertFalse(H.attributed()); self.assertIn('watermark not advanced', T.status['error'])

    def test_budget_stops_between_slices_and_keeps_watermark_honest(self):
        import time
        old = T.SLICE; T.SLICE = 2
        try:
            H = FakeH([task(f'00T{i}', f'2026-09-19T08:1{i}:00.000+0000') for i in range(5)])
            real_insert = H._insert
            def slow_insert(*a, **k): time.sleep(0.05); return real_insert(*a, **k)     # each slice takes longer than the budget
            H._insert = slow_insert
            stats = self.run_(H, deadline=time.time() + 0.02)
            self.assertEqual(stats['touches_new'], 2); self.assertEqual(H.watermarks()[-1], '2026-09-19T08:11:00Z')   # only the processed slice
            self.assertIn('budget', T.status['skipped']); self.assertIn('3 of 5', T.status['skipped'])
            self.assertTrue(any(isinstance(c, tuple) and c[1] == 'rpc/refresh_campaign_lead_activity_for_touches' for c in H.calls))  # rollup still refreshed for what was processed
        finally: T.SLICE = old

    def test_cap_reached_flags_backlog(self):
        H = FakeH([task(f'00T{i}', f'2026-09-19T08:1{i}:00.000+0000') for i in range(3)]); H._cfg['touch_max_rows'] = '3'; self.run_(H)
        self.assertTrue(T.status['backlog']); self.assertEqual(T.status['seen'], 3)

    def test_member_and_window_backlog(self):
        H = FakeH([])
        T.note_members(H._state, 'uuid-A', ['00QA1'])
        T.note_window_change(H._state, 'uuid-A', '2026-09-01', '2026-09-30', '2026-09-01', '2026-10-15')
        self.run_(H)
        att = H.attributed(); self.assertTrue(att, H.calls)
        flat = [i for ids in att for i in ids]
        self.assertIn('00TOLD1', flat); self.assertIn('00TATT1', flat)                      # member touches + the campaign's live attributions re-evaluated
        gets = [c for c in H.calls if isinstance(c, tuple) and c[1] == 'touches']
        self.assertTrue(any(p.get('occurred_on') == 'gte.2026-09-01' and p.get('and') == '(occurred_on.lte.2026-10-15)' for _, _, p, _ in gets), gets)  # old ∪ new window
        self.assertEqual(H._state['touch_backlog'], {'members': [], 'windows': []})
        self.assertTrue(any(c[1] == 'rpc/refresh_campaign_lead_activity_for_touches' for c in H.calls if isinstance(c, tuple)))

    def test_zero_rows_still_stamps_state(self):
        H = FakeH([]); self.run_(H)
        self.assertEqual(H.watermarks(), ['2026-09-19T06:54:52Z']); self.assertIsNotNone(T.status['last_ok_at'])

if __name__ == '__main__':
    unittest.main()
