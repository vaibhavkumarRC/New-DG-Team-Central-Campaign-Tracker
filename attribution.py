"""Forward attribution tracking (dg-campaign-history migration 0019) — runs last in every history flush.

One rule, one place: the logic lives in four SQL functions on dg-campaign-history; this module only calls them, keeps the
writer's in-memory primary map in step with what SQL decided, and exposes the result for the Slack line and health check.

  attribute_meetings_grace(p_run)  new meeting with no in-window campaign → grade B if the lead's campaign ended ≤ 30 days
                                   earlier and the booking SDR is that campaign's owner/pod; otherwise a dq flag. Inbound
                                   meetings on calling campaigns are graded C + flagged. Every live primary carries a grade.
  refresh_done_on(p_run)           meetings that became done get done_on + done_on_source (SFDC status history → SQL date →
                                   first sync that saw it → scheduled time proxy).
  build_daily_digest(p_run)        rows for what happened since the previous sync: meetings generated / became done /
                                   deals created / flags — with campaign, SDR, grade.
  digest_line(p_run)               the one-line summary appended to the sync Slack message.

Past meetings are never re-attributed here (Vaibhav, 26 Sep 2026: "leave the past cases … from now onwards keep a track").
"""
import time

status = {}          # health/Slack read this: line, counts, error, skipped, last_ok_at


def _rpc(H, name, run_id, timeout=120):
    return H._http('POST', f'rpc/{name}', None, {'p_run': run_id}, timeout=timeout) or []


def run(run_id, stats, H):
    """Never raises past the flush's per-step guard; records what it did in `status` and `stats`."""
    status.clear(); t0 = time.time()
    # 1) grace rule + flags. SQL may create a primary the Python state does not know about → mirror it so the writer never
    #    tries to add a second primary for the same meeting (ma_one_primary_idx would reject the whole batch).
    res = _rpc(H, 'attribute_meetings_grace', run_id)
    graced = [r for r in res if str(r.get('o_action') or '').startswith('B:')]
    flags = [r for r in res if str(r.get('o_action') or '').startswith('flag')]
    st = getattr(H, '_state', None) or {}
    for r in graced:
        mid, cid = r.get('o_meeting_id'), r.get('o_campaign_id')
        if mid and cid:
            st.setdefault('primary', {})[mid] = cid
            links = st.setdefault('links', [])
            if f'{mid}|{cid}' not in links: links.append(f'{mid}|{cid}')
    # 2) meeting done dates
    done = _rpc(H, 'refresh_done_on', run_id)
    done_n = sum(int(r.get('set_now') or 0) for r in done)
    # 3) digest rows + the Slack line
    dig = _rpc(H, 'build_daily_digest', run_id)
    counts = {r.get('kind'): int(r.get('n') or 0) for r in dig if r.get('kind')}
    line = H._http('POST', 'rpc/digest_line', None, {'p_run': run_id}, timeout=60)
    line = line if isinstance(line, str) and line.strip() else ''
    status.update({'grace_b': len(graced), 'flags': len(flags), 'done_on_set': done_n, 'digest': counts, 'line': line,
                   'seconds': round(time.time() - t0, 1), 'last_ok_at': time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime())})
    stats['attr_grace_b'] = len(graced); stats['attr_flags'] = len(flags); stats['done_on_set'] = done_n
    stats['digest'] = counts


def summary():
    """Short fragment for the history line: 'attribution B+1, 2 flags, done_on +3'."""
    if status.get('error'): return f"attribution ERROR {status['error'][:120]}"
    if status.get('skipped'): return f"attribution skipped ({status['skipped'][:80]})"
    if not status: return 'attribution not run'
    return f"attribution grace +{status.get('grace_b', 0)}, {status.get('flags', 0)} flag(s), done_on +{status.get('done_on_set', 0)}"


def digest_line():
    """The 📣 line for Slack. Always says something so a silent failure is visible."""
    if status.get('error'): return f"📣 attribution since last sync — ⚠️ not computed: {status['error'][:200]}"
    if status.get('skipped'): return f"📣 attribution since last sync — ⚠️ not computed ({status['skipped'][:120]})"
    if status.get('line'): return status['line']
    if status.get('digest') is not None: return '📣 attribution since last sync — no new meetings, done meetings or deals'
    return ''
