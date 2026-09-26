"""Dashboard health block for the daily Slack sync message.

Every dependency the DG Campaign Dashboard relies on gets one line: ✅ when it
is fine, ⚠️/❌ with the ACTUAL error text when it is not — so a pasted Slack
message is enough to diagnose the problem the same day. Never raises; a check
that itself crashes is reported as an issue.

Checks (thresholds per Vaibhav, 19 Sep 2026):
  • Salesforce sync        fatal crash → ❌; per-campaign errors → ⚠️
  • Weekly Review snapshot age > 36 h → ⚠️; last refresh error text
  • Cold-calls cache       age > 72 h → ⚠️; last refresh error text
  • History writer         errors / queued batches / last ok > 24 h; disabled → ℹ️
  • Touches (P1)           ingest error → ❌; not run / stale > 30 h / partial (cap or budget) → ⚠️
  • Quarter close (P1)     error → ❌; ended quarter not closed → ⚠️
  • Lead history (P1)      same rules as Touches
  • Opportunities (P1)     same rules as Touches
  • Backups                on-volume copy + GitHub push per file → ❌ on failure
  • Supabase keys          live probe of intelligence_dashboard (SUPABASE_SERVICE_KEY)
                           and dg-campaign-history (HISTORY_SUPABASE_KEY) → ❌ on 401/other
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
WEEKLY_STALE_H = 36
COLDCALLS_STALE_H = 72
HISTORY_STALE_H = 24
TOUCHES_STALE_H = 30

def _age_h(iso):
    if not iso: return None
    try:
        dt = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=IST)
        return round((datetime.now(timezone.utc) - dt).total_seconds() / 3600, 1)
    except Exception:
        return None

def _probe(url, key, timeout=10):
    """GET one row with the given key. Returns None when OK, else a short error string."""
    if not url or not key: return 'not configured'
    try:
        req = urllib.request.Request(url, headers={'apikey': key, 'Authorization': 'Bearer ' + key})
        urllib.request.urlopen(req, timeout=timeout).read()
        return None
    except urllib.error.HTTPError as e:
        try: body = e.read()[:200].decode(errors='replace')
        except Exception: body = ''
        try: body = json.loads(body).get('message') or body
        except Exception: pass
        return f'HTTP {e.code} {body}'.strip()
    except Exception as e:
        return str(e)[:200]

def build(*, cache=None, weekly=None, coldcalls=None, history=None, backup_status=None, probe=_probe, now=None):
    """Return (ok: bool, lines: list[str]) for the Slack message."""
    issues, okbits = [], []
    def issue(text): issues.append(text[:350])

    # 1. Salesforce sync
    try:
        c = cache or {}
        if c.get('fatal_error'):
            issue('❌ Salesforce sync: crashed — ' + str(c['fatal_error']).strip().splitlines()[-1][:250])
        elif c.get('errors'):
            issue(f"⚠️ Salesforce sync: {len(c['errors'])} campaign error(s) — first: {str(c['errors'][0])[:220]}")
        else:
            okbits.append('Sync')
    except Exception as e:
        issue(f'⚠️ health check (sync) crashed: {e}')

    # 2. Weekly Review snapshot
    try:
        wr = getattr(weekly, '_WR', None) if weekly is not None else None
        if wr is None:
            issue('⚠️ Weekly Review: no snapshot loaded (tab is empty until a refresh succeeds)')
        else:
            age = _age_h(wr.get('fetched_at')); err = wr.get('error')
            if age is None or age > WEEKLY_STALE_H:
                issue(f"⚠️ Weekly Review: snapshot {age if age is not None else '?'} h old (fetched {str(wr.get('fetched_at'))[:16]}); last error: {err or 'none recorded'}")
            elif err:
                issue(f'⚠️ Weekly Review: last refresh error: {err}')
            else:
                okbits.append(f'Weekly Review {age}h')
    except Exception as e:
        issue(f'⚠️ health check (weekly) crashed: {e}')

    # 3. Cold-calls cache
    try:
        cc = getattr(coldcalls, '_CC', None) if coldcalls is not None else None
        if cc is None:
            issue('⚠️ Cold calls: no cache loaded')
        else:
            age = _age_h(cc.get('fetched_at')); err = cc.get('error')
            if age is None or age > COLDCALLS_STALE_H:
                issue(f"⚠️ Cold calls: cache {age if age is not None else '?'} h old (fetched {str(cc.get('fetched_at'))[:16]}); last error: {err or 'none recorded'}")
            elif err:
                issue(f'⚠️ Cold calls: last refresh error: {err}')
            else:
                okbits.append(f'Cold calls {age}h')
    except Exception as e:
        issue(f'⚠️ health check (cold calls) crashed: {e}')

    # 4. History writer
    try:
        if history is None or not getattr(history, '_summary', {}).get('enabled'):
            issue('ℹ️ History writer: disabled (HISTORY_SUPABASE_URL/KEY not set)')
        else:
            s = history._summary.get('stats') or {}
            errs = list(getattr(history, '_errors', []) or [])
            qn = 0
            try:
                qp = history._queue_path()
                if os.path.exists(qp):
                    with open(qp) as f: qn = sum(1 for _ in f)
            except Exception: pass
            age = _age_h(getattr(history, '_last_ok_at', None))
            if errs or s.get('errors'):
                issue(f"❌ History writer: {len(errs) or s.get('errors')} error(s) — first: {(errs[0] if errs else '?')[:240]}")
            elif qn:
                issue(f'⚠️ History writer: {qn} batch(es) queued for replay (Supabase write failed earlier)')
            elif age is not None and age > HISTORY_STALE_H:
                issue(f'⚠️ History writer: last successful flush {age} h ago')
            else:
                okbits.append('History')
    except Exception as e:
        issue(f'⚠️ health check (history) crashed: {e}')

    # 4f. Attribution tracking (0019) — grace rule, done_on, digest; runs last in the flush
    try:
        amod = getattr(history, 'A', None) if history is not None else None
        if amod is not None and getattr(history, '_summary', {}).get('enabled'):
            a = getattr(amod, 'status', None) or {}
            if not a:
                issue('⚠️ Attribution: not run yet (no history flush since start)')
            elif a.get('error'):
                issue(f"❌ Attribution: {a['error'][:260]}")
            elif a.get('skipped'):
                issue(f"⚠️ Attribution: not computed this sync ({a.get('skipped')})")
            elif not a.get('line') and not a.get('digest'):
                issue('⚠️ Attribution: digest empty — build_daily_digest returned nothing')
            else:
                okbits.append(f"Attribution {a.get('flags', 0)} flag(s)" if a.get('flags') else 'Attribution')
    except Exception as e:
        issue(f'⚠️ health check (attribution) crashed: {e}')

    # 4b. Touches ingestion (P1) — only when the history writer is enabled
    try:
        if history is not None and getattr(history, '_summary', {}).get('enabled'):
            ts = getattr(getattr(history, 'T', None), 'status', None) or {}
            age = _age_h(ts.get('last_ok_at'))
            if ts.get('error'):
                issue(f"❌ Touches: {ts['error'][:260]}")
            elif not ts.get('last_ok_at'):
                issue(f"⚠️ Touches: not ingested this sync ({ts.get('skipped') or 'step did not run'})")
            elif age is not None and age > TOUCHES_STALE_H:
                issue(f'⚠️ Touches: last successful ingest {age} h ago')
            elif ts.get('backlog') or ts.get('skipped'):
                issue(f"⚠️ Touches: partial — {ts.get('skipped') or 'row cap reached'}; catching up next sync (+{ts.get('new', 0)} this run)")
            else:
                okbits.append(f"Touches +{ts.get('new', 0)}")
    except Exception as e:
        issue(f'⚠️ health check (touches) crashed: {e}')

    # 4d. Lead history ingestion (P1 part 2)
    try:
        if history is not None and getattr(history, '_summary', {}).get('enabled'):
            ls = getattr(getattr(history, 'LH', None), 'status', None) or {}
            age = _age_h(ls.get('last_ok_at'))
            if ls.get('error'):
                issue(f"❌ Lead history: {ls['error'][:260]}")
            elif not ls.get('last_ok_at'):
                issue(f"⚠️ Lead history: not ingested this sync ({ls.get('skipped') or 'step did not run'})")
            elif age is not None and age > TOUCHES_STALE_H:
                issue(f'⚠️ Lead history: last successful ingest {age} h ago')
            elif ls.get('backlog') or ls.get('skipped'):
                issue(f"⚠️ Lead history: partial — {ls.get('skipped') or 'row cap reached'}; catching up next sync")
            else:
                okbits.append(f"Lead history +{ls.get('new', 0)}")
    except Exception as e:
        issue(f'⚠️ health check (lead history) crashed: {e}')

    # 4e. Opportunities (P1 part 3)
    try:
        if history is not None and getattr(history, '_summary', {}).get('enabled'):
            os_ = getattr(getattr(history, 'OP', None), 'status', None) or {}
            age = _age_h(os_.get('last_ok_at'))
            if os_.get('error'):
                issue(f"❌ Opportunities: {os_['error'][:260]}")
            elif not os_.get('last_ok_at'):
                issue(f"⚠️ Opportunities: not ingested this sync ({os_.get('skipped') or 'step did not run'})")
            elif age is not None and age > TOUCHES_STALE_H:
                issue(f'⚠️ Opportunities: last successful ingest {age} h ago')
            else:
                okbits.append(f"Opps +{os_.get('new', 0)}")
    except Exception as e:
        issue(f'⚠️ health check (opportunities) crashed: {e}')

    # 4c. Quarter close (P1 part 4) — overdue or failed closes must be visible
    try:
        if history is not None and getattr(history, '_summary', {}).get('enabled'):
            qs = getattr(getattr(history, 'Q', None), 'status', None) or {}
            if qs.get('error'):
                issue(f"❌ Quarter close: {qs['error'][:260]}")
            elif qs.get('due') and qs.get('last_closed') != qs.get('due'):
                issue(f"⚠️ Quarter close: {qs['due']} has ended but is not closed yet (step did not complete this sync)")
            elif qs.get('closed_this_run'):
                okbits.append(f"Quarter {qs['closed_this_run']['quarter']} closed")
    except Exception as e:
        issue(f'⚠️ health check (quarter close) crashed: {e}')

    # 5. Backups (on-volume + GitHub)
    try:
        b = backup_status or {}
        if not b:
            issue('⚠️ Backups: no backup ran in this sync')
        else:
            bad = [f'{k}: {v}' for k, v in (b.get('local') or {}).items() if v != 'ok']
            gh = b.get('github') or {}
            if gh.get('_skipped'):
                issue('⚠️ GitHub backup: skipped — BACKUP_GITHUB_TOKEN not set')
            bad += [f'GitHub {k}: {v}' for k, v in gh.items() if not k.startswith('_') and v != 'ok']
            if bad:
                issue('❌ Backups: ' + '; '.join(bad)[:300])
            else:
                n = sum(1 for v in gh.values() if v == 'ok')
                okbits.append(f'Backups {n}/3')
    except Exception as e:
        issue(f'⚠️ health check (backups) crashed: {e}')

    # 6. Supabase key probes (live)
    try:
        intel = probe(os.environ.get('SUPABASE_URL', 'https://gvszpwyajzehqsofxzou.supabase.co').rstrip('/') + '/rest/v1/companies?select=id&limit=1',
                      os.environ.get('SUPABASE_SERVICE_KEY', '').strip())
        if intel:
            issue(f'❌ SUPABASE_SERVICE_KEY (intelligence_dashboard, used by Weekly Review / cold calls / history enrichment): {intel}')
        hurl = os.environ.get('HISTORY_SUPABASE_URL', '').strip().rstrip('/')
        hkey = os.environ.get('HISTORY_SUPABASE_KEY', '').strip()
        if hurl and hkey:
            h = probe(hurl + '/rest/v1/metric_definitions?select=version&limit=1', hkey)
            if h:
                issue(f'❌ HISTORY_SUPABASE_KEY (dg-campaign-history): {h}')
        if not intel and not (hurl and hkey and h):
            okbits.append('Supabase keys')
    except Exception as e:
        issue(f'⚠️ health check (keys) crashed: {e}')

    if issues:
        return False, [f'🩺 *Health: {len(issues)} issue(s) — copy this message to Claude*'] + ['• ' + i for i in issues]
    return True, ['🩺 Health: ✅ ' + ' · '.join(okbits)]
