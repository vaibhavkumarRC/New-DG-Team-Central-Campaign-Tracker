"""Nooks Cold Calls tab — backend.

Reads Rahul's cold-calls pipeline output from Supabase (project
gvszpwyajzehqsofxzou) STRICTLY READ-ONLY and serves the campaign dashboard's
"Nooks Cold Calls" tab: the same eight views as lead-research-v2's /cold-calls
page, plus a campaign scope his dashboard doesn't have.

Design (agreed with Vaibhav 18 Aug 2026):
  * Source of truth stays Rahul's S3→Supabase→LLM pipeline; we duplicate zero
    ingestion and zero LLM spend. His tables/views are the contract — we do NOT
    touch his Next.js APIs, so his deploys can't break us.
  * One snapshot cache on the Railway volume (atomic write, gzip). All eight
    views aggregate from it in ONE Python engine — the campaign scope is just
    one more filter, so global and scoped numbers can never drift apart.
    Aggregation semantics mirror his six cold_calls_* SQL RPCs 1:1 (validated
    against them at build time).
  * Transcripts are NEVER cached — fetched live per call on click.
  * The Supabase client below implements GET only. There is deliberately no
    write method to misuse: this module must never write to Supabase.
  * Campaign scope = the campaign's frozen-ledger lead ids (plus the Contact
    ids those leads converted into), matched against nooks_calls.sf_person_id
    (Rahul's phone-identity bridge, ~90% coverage). Every scoped payload
    carries match coverage so the subset is never mistaken for the whole.
  * Staleness is loud: /meta reports data age; the UI banners past 48h
    (weekends excluded from the count — no dialing Sat/Sun).
"""

import gzip
import json
import os
import threading
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo('America/New_York')
except Exception:                              # tzdata missing — ET basis degrades to UTC-4
    _ET = timezone(timedelta(hours=-4))

IST = timezone(timedelta(hours=5, minutes=30))

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gvszpwyajzehqsofxzou.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# Wired by init_app()
_deps = {}
CACHE_PATH = None

_cc_lock = threading.Lock()          # guards _CC swap + refresh flag
_refreshing = False
_CC = None                           # in-memory snapshot dict


# ── Supabase client (GET only — no write methods exist by design) ─────────────

def _sb_get(path, params, timeout=60):
    if not SUPABASE_KEY:
        raise RuntimeError('SUPABASE_SERVICE_KEY not set')
    url = f"{SUPABASE_URL}/rest/v1/{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Accept': 'application/json',
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def _sb_fetch_all(table, select, extra_params=None, page=1000):
    """Offset-paged full read of a table/view."""
    out = []
    for offset in range(0, 10_000_000, page):
        params = {'select': select, 'limit': page, 'offset': offset}
        if extra_params:
            params.update(extra_params)
        rows = _sb_get(table, params)
        out.extend(rows)
        if len(rows) < page:
            break
    return out


# ── snapshot refresh ──────────────────────────────────────────────────────────

CALL_SELECT = (
    'id,time,duration_sec,dialing_mode,hung_up_by,owner_name,pod,contact_name,'
    'contact_title,account_name,disposition_name,effective_outcome,call_label,'
    'talk_ratio_rep,conversation_class,is_connect,is_conversation,is_meeting,'
    'disposition_mismatch,prospect_timezone,local_hour,local_dow,sf_person_id,'
    'has_transcript,transcript_status,opening_v,analysis_v,'
    'call_grade:analysis->>call_grade,'
    'closing_attempted:analysis->closing->>attempted,'
    'closing_technique:analysis->closing->>technique'
)


def _converted_contact_map():
    """lead_id -> contact_id for converted leads, via the app's soql helper.
    Lets a campaign's match set follow leads that became Contacts (nooks
    identity matches the Contact id after conversion)."""
    try:
        recs = _deps['soql']("SELECT Id, ConvertedContactId FROM Lead "
                             "WHERE IsConverted = true AND ConvertedContactId != null")
        recs = recs.get('records', []) if isinstance(recs, dict) else (recs or [])
        return {r['Id']: r['ConvertedContactId'] for r in recs
                if r.get('Id') and r.get('ConvertedContactId')}
    except Exception as e:
        print(f"[cold_calls] converted-contact map failed (campaign scope degrades gracefully): {e}")
        return {}


def refresh_cold_calls():
    """Pull a fresh snapshot from Supabase. Failure leaves the old cache serving."""
    global _CC
    snap = {
        'fetched_at': datetime.now(IST).isoformat(),
        'calls':      _sb_fetch_all('nooks_calls', CALL_SELECT),
        'openings':   _sb_fetch_all('v_call_openings', '*'),
        'objections': _sb_fetch_all('v_call_objections', '*'),
        'mistakes':   _sb_fetch_all('v_call_mistakes', '*'),
        'meetings':   _sb_fetch_all('v_call_meetings', '*'),
        'insights':   _sb_get('cold_call_insights', {
            'select': '*', 'order': 'generated_at.desc', 'limit': 60}),
        'converted_map': _converted_contact_map(),
        'error': None,
    }
    tmp = CACHE_PATH + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(snap, f)
    os.replace(tmp, CACHE_PATH)
    with _cc_lock:
        _CC = snap
    print(f"[cold_calls] snapshot refreshed: {len(snap['calls'])} calls, "
          f"{len(snap['meetings'])} scored meetings")
    return snap


def _load_cache_from_disk():
    global _CC
    try:
        with gzip.open(CACHE_PATH, 'rt', encoding='utf-8') as f:
            _CC = json.load(f)
        print(f"[cold_calls] cache loaded: {len(_CC.get('calls', []))} calls "
              f"(fetched {_CC.get('fetched_at')})")
    except FileNotFoundError:
        _CC = None
    except Exception as e:
        print(f"[cold_calls] cache load failed: {e}")
        _CC = None


def _maybe_background_refresh():
    """Lazy daily refresh: after 15:00 IST (Rahul's S3 drop + cron land by
    ~14:30 IST), refresh once if today's snapshot hasn't been taken. Runs in a
    thread so no request ever blocks on Supabase. Deliberately NOT wired into
    the 5AM/5PM sync timer — different failure blast radius."""
    global _refreshing
    now = datetime.now(IST)
    with _cc_lock:
        cc = _CC
        if _refreshing:
            return
        fetched = cc.get('fetched_at') if cc else None
        need = cc is None or (
            now.hour >= 15 and (not fetched or fetched[:10] < now.strftime('%Y-%m-%d')))
        if not need:
            return
        _refreshing = True

    def run():
        global _refreshing
        try:
            refresh_cold_calls()
        except Exception as e:
            print(f"[cold_calls] background refresh failed (cache keeps serving): {e}")
            with _cc_lock:
                if _CC is not None:
                    _CC['error'] = f"refresh failed {datetime.now(IST).isoformat()}: {e}"
        finally:
            _refreshing = False
    threading.Thread(target=run, daemon=True).start()


# ── campaign match sets ───────────────────────────────────────────────────────

def _campaign_sets():
    """{campaign_name: set(sf ids)} from the frozen ledger + converted map."""
    cc = _CC or {}
    conv = cc.get('converted_map') or {}
    ledger = _deps['load_ledger']()
    camps = _deps['load_campaigns']()
    name_by_id = {c['id']: c.get('name') for c in camps}
    out = {}
    for cid, entry in (ledger or {}).items():
        name = name_by_id.get(cid)
        if not name:
            continue
        ids = set(entry.get('lead_ids') or [])
        ids |= {conv[i] for i in ids if i in conv}
        if ids:
            out[name] = ids
    return out


# ── the aggregation engine (mirrors the cold_calls_* RPCs 1:1) ────────────────

def _csv(args, key):
    v = (args.get(key) or '').strip()
    return [s.strip() for s in v.split(',') if s.strip()] if v else None


def _filter_calls(args):
    """Shared predicate — the Python twin of cold_calls_filter_sql, plus the
    campaign scope. Returns (rows, scope_info)."""
    cc = _CC or {}
    rows = cc.get('calls') or []
    frm, to = (args.get('from') or '').strip(), (args.get('to') or '').strip()
    reps, pods, modes = _csv(args, 'rep'), _csv(args, 'pod'), _csv(args, 'mode')
    campaign = (args.get('campaign') or '').strip()

    scope = None
    if campaign:
        ids = _campaign_sets().get(campaign)
        if ids is None:
            return [], {'campaign': campaign, 'error': 'unknown campaign'}
        total = len(rows)
        rows = [r for r in rows if r.get('sf_person_id') in ids]
        scope = {'campaign': campaign, 'lead_ids': len(ids),
                 'matched_calls': len(rows), 'all_calls': total}

    if frm:
        rows = [r for r in rows if (r.get('time') or '') >= frm]
    if to:
        # inclusive end date, matching the RPC's `time < to+1 day`
        rows = [r for r in rows if (r.get('time') or '')[:10] <= to]
    if reps:
        rows = [r for r in rows if r.get('owner_name') in reps]
    if pods:
        rows = [r for r in rows if r.get('pod') in pods]
    if modes:
        rows = [r for r in rows if r.get('dialing_mode') in modes]
    return rows, scope


def _kpis(rows):
    return {
        'dials': len(rows),
        'connects': sum(1 for r in rows if r.get('is_connect')),
        'conversations': sum(1 for r in rows if r.get('is_conversation')),
        'meetings': sum(1 for r in rows if r.get('is_meeting')),
    }


def _week_of(ts):
    """date_trunc('week', time)::date — ISO Monday, computed in UTC like the RPC."""
    d = datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(timezone.utc).date()
    return (d - timedelta(days=d.weekday())).isoformat()


def agg_overview(rows):
    weekly = defaultdict(lambda: defaultdict(int))
    by_rep = defaultdict(lambda: defaultdict(int))
    rep_pod = {}
    by_pod = defaultdict(lambda: defaultdict(int))
    dq = {'disposition_mismatches': 0, 'transcript_denied': 0,
          'unanalyzed_openings': 0, 'unanalyzed_conversations': 0, 'unknown_timezone': 0}
    for r in rows:
        wk = _week_of(r['time'])
        for key, cond in (('dials', True), ('connects', r.get('is_connect')),
                          ('conversations', r.get('is_conversation')), ('meetings', r.get('is_meeting'))):
            if cond:
                weekly[wk][key] += 1
                if r.get('owner_name'):
                    by_rep[r['owner_name']][key] += 1
                if r.get('pod'):
                    by_pod[r['pod']][key] += 1
        if r.get('owner_name') and r.get('pod'):
            rep_pod[r['owner_name']] = max(rep_pod.get(r['owner_name'], ''), r['pod'])
        if r.get('disposition_mismatch'):
            dq['disposition_mismatches'] += 1
        if r.get('transcript_status') == 'denied':
            dq['transcript_denied'] += 1
        if (r.get('call_label') == 'human' and r.get('is_connect')
                and r.get('opening_v') is None and r.get('transcript_status') == 'fetched'):
            dq['unanalyzed_openings'] += 1
        if r.get('is_conversation') and r.get('analysis_v') is None:
            dq['unanalyzed_conversations'] += 1
        if not r.get('prospect_timezone'):
            dq['unknown_timezone'] += 1
    return {
        'kpis': _kpis(rows),
        'weekly': [{'week': wk, **{k: weekly[wk].get(k, 0) for k in
                                   ('dials', 'connects', 'conversations', 'meetings')}}
                   for wk in sorted(weekly)],
        'by_rep': sorted([{'rep': n, 'pod': rep_pod.get(n), 'n': v.get('dials', 0),
                           **{k: v.get(k, 0) for k in ('dials', 'connects', 'conversations', 'meetings')}}
                          for n, v in by_rep.items()], key=lambda x: -x['dials']),
        'by_pod': sorted([{'pod': p, 'n': v.get('dials', 0),
                           **{k: v.get(k, 0) for k in ('dials', 'connects', 'conversations', 'meetings')}}
                          for p, v in by_pod.items()], key=lambda x: -x['dials']),
        'data_quality': dq,
    }


def agg_timing(rows, basis):
    cells = defaultdict(lambda: defaultdict(int))
    excluded = 0
    for r in rows:
        if basis == 'et':
            dt = datetime.fromisoformat(r['time'].replace('Z', '+00:00')).astimezone(_ET)
            dow, hr = (dt.weekday() + 1) % 7, dt.hour     # postgres dow: 0=Sun
        else:
            dow, hr = r.get('local_dow'), r.get('local_hour')
            if dow is None or hr is None:
                excluded += 1
                continue
        c = cells[(dow, hr)]
        c['dials'] += 1
        if r.get('is_connect'):
            c['connects'] += 1
        if r.get('is_conversation'):
            c['conversations'] += 1
        if r.get('is_meeting'):
            c['meetings'] += 1
    return {
        'basis': basis,
        'cells': [{'dow': d, 'hour': h,
                   'n': v.get('dials', 0), 'dials': v.get('dials', 0),
                   'connects': v.get('connects', 0), 'conversations': v.get('conversations', 0),
                   'meetings': v.get('meetings', 0)}
                  for (d, h), v in sorted(cells.items())],
        'excluded_unknown_tz': excluded if basis == 'local' else 0,
    }


def _view_rows(kind, call_ids):
    return [v for v in (_CC or {}).get(kind, []) if v.get('call_id') in call_ids]


def agg_openings(rows):
    ids = {r['id'] for r in rows}
    f = _view_rows('openings', ids)
    by_opener = defaultdict(lambda: defaultdict(int))
    deaths = defaultdict(int)
    pers = {'with': {'n': 0, 'attention_captured': 0}, 'without': {'n': 0, 'attention_captured': 0}}
    by_rep = defaultdict(lambda: defaultdict(int))
    for v in f:
        o = by_opener[v.get('opener_type')]
        o['n'] += 1
        if v.get('attention_captured'):
            o['attn'] += 1
        if v.get('is_conversation'):
            o['conv'] += 1
        if v.get('is_meeting'):
            o['met'] += 1
        if v.get('died_within_20s') and v.get('death_cause'):
            deaths[v['death_cause']] += 1
        bucket = pers['with' if v.get('personalization_used') else 'without']
        bucket['n'] += 1
        if v.get('attention_captured'):
            bucket['attention_captured'] += 1
        if v.get('owner_name'):
            rr = by_rep[v['owner_name']]
            rr['n'] += 1
            if v.get('attention_captured'):
                rr['attn'] += 1
            if v.get('died_within_20s'):
                rr['died'] += 1
    return {
        'population': len(f),
        'died_within_20s': sum(1 for v in f if v.get('died_within_20s')),
        'by_opener': sorted([{'opener_type': t, 'n': v['n'], 'attention_captured': v.get('attn', 0),
                              'became_conversation': v.get('conv', 0), 'meetings': v.get('met', 0)}
                             for t, v in by_opener.items()], key=lambda x: -x['n']),
        'death_causes': sorted([{'cause': c, 'n': n} for c, n in deaths.items()], key=lambda x: -x['n']),
        'personalization': pers,
        'by_rep': sorted([{'rep': n, 'n': v['n'], 'attention_captured': v.get('attn', 0),
                           'died_within_20s': v.get('died', 0)}
                          for n, v in by_rep.items()], key=lambda x: -x['n']),
    }


def agg_objections(rows):
    ids = {r['id'] for r in rows}
    f = _view_rows('objections', ids)
    by_type = defaultdict(lambda: defaultdict(int))
    by_rep = defaultdict(lambda: defaultdict(int))
    for v in f:
        t = by_type[v.get('objection_type')]
        t['n'] += 1
        h = v.get('handled')
        if h == 'well':
            t['hw'] += 1
        elif h == 'adequate':
            t['ha'] += 1
        elif h == 'poorly':
            t['hp'] += 1
        elif h == 'not_addressed':
            t['na'] += 1
        if v.get('is_meeting'):
            t['met'] += 1
        if v.get('owner_name'):
            r = by_rep[v['owner_name']]
            r['n'] += 1
            if h == 'well':
                r['hw'] += 1
    return {
        'population_conversations': len({v['call_id'] for v in f}),
        'by_type': sorted([{'objection_type': t, 'n': v['n'],
                            'handled_well': v.get('hw', 0), 'handled_adequate': v.get('ha', 0),
                            'handled_poorly': v.get('hp', 0), 'not_addressed': v.get('na', 0),
                            'meeting_after': v.get('met', 0)}
                           for t, v in by_type.items()], key=lambda x: -x['n']),
        'by_rep': sorted([{'rep': n, 'n': v['n'], 'handled_well': v.get('hw', 0)}
                          for n, v in by_rep.items()], key=lambda x: -x['n']),
    }


def agg_mistakes(rows):
    ids = {r['id'] for r in rows}
    f = _view_rows('mistakes', ids)
    # population counts ALL conversations incl. ownerless (RPC sums a GROUP BY
    # owner_name that keeps the NULL group); per-rep rows exclude the NULL group.
    population = sum(1 for r in rows if r.get('is_conversation'))
    convs = defaultdict(int)
    for r in rows:
        if r.get('is_conversation') and r.get('owner_name'):
            convs[r['owner_name']] += 1
    by_type = defaultdict(lambda: defaultdict(int))
    mk = defaultdict(lambda: defaultdict(int))
    for v in f:
        t = by_type[v.get('mistake_type')]
        t['n'] += 1
        s = v.get('severity')
        if s == 'minor':
            t['mi'] += 1
        elif s == 'moderate':
            t['mo'] += 1
        elif s == 'major':
            t['ma'] += 1
        if v.get('owner_name'):
            mk[v['owner_name']]['n'] += 1
            if s == 'major':
                mk[v['owner_name']]['major'] += 1
    return {
        'population_conversations': population,
        'by_type': sorted([{'mistake_type': t, 'n': v['n'], 'minor': v.get('mi', 0),
                            'moderate': v.get('mo', 0), 'major': v.get('ma', 0)}
                           for t, v in by_type.items()], key=lambda x: -x['n']),
        'by_rep': sorted([{'rep': n, 'n': c, 'mistakes': mk[n].get('n', 0),
                           'major': mk[n].get('major', 0)}
                          for n, c in convs.items()], key=lambda x: -x['n']),
    }


def agg_reps(rows):
    per = defaultdict(lambda: defaultdict(int))
    pods, cmix, gmix = {}, defaultdict(lambda: defaultdict(int)), defaultdict(lambda: defaultdict(int))
    for r in rows:
        n = r.get('owner_name')
        if not n:
            continue
        p = per[n]
        p['dials'] += 1
        if r.get('is_connect'):
            p['connects'] += 1
        if r.get('is_conversation'):
            p['conversations'] += 1
        if r.get('is_meeting'):
            p['meetings'] += 1
        if r.get('is_connect') and r.get('has_transcript'):
            p['t_exp'] += 1
            if r.get('transcript_status') == 'fetched':
                p['t_got'] += 1
        if r.get('analysis_v') is not None:
            if r.get('closing_attempted') == 'true':
                p['close_attempts'] += 1
            elif r.get('closing_attempted') == 'false':
                p['no_ask'] += 1
        tech = r.get('closing_technique')
        if tech in ('assumptive', 'choice_of_times', 'soft_ask', 'none'):
            cmix[n][tech] += 1
        g = r.get('call_grade')
        if g in ('A', 'B', 'C', 'D'):
            gmix[n][g] += 1
        if r.get('pod'):
            pods[n] = max(pods.get(n, ''), r['pod'])
    return {'reps': sorted([{
        'rep': n, 'pod': pods.get(n), 'n': v.get('conversations', 0),
        'dials': v.get('dials', 0), 'connects': v.get('connects', 0),
        'conversations': v.get('conversations', 0), 'meetings': v.get('meetings', 0),
        'transcript_coverage_pct': round(100.0 * v.get('t_got', 0) / v['t_exp']) if v.get('t_exp') else None,
        'close_attempts': v.get('close_attempts', 0), 'no_ask': v.get('no_ask', 0),
        'closing_mix': {k: cmix[n].get(k, 0) for k in ('assumptive', 'choice_of_times', 'soft_ask', 'none')},
        'grade_mix': {k: gmix[n].get(k, 0) for k in ('A', 'B', 'C', 'D')},
    } for n, v in per.items()], key=lambda x: -x['conversations'])}


# ── list / drill-down sources ─────────────────────────────────────────────────

LIST_CALL_COLS = ('id', 'time', 'owner_name', 'pod', 'contact_name', 'contact_title',
                  'account_name', 'disposition_name', 'effective_outcome', 'conversation_class',
                  'duration_sec', 'dialing_mode', 'is_meeting', 'disposition_mismatch',
                  'local_hour', 'local_dow', 'prospect_timezone', 'talk_ratio_rep', 'call_grade')


def list_rows(args):
    source = args.get('source') or 'calls'
    rows, _scope = _filter_calls(args)
    ids = {r['id'] for r in rows}
    if source == 'calls':
        f = rows
        outcome = _csv(args, 'outcome')
        if outcome:
            f = [r for r in f if r.get('effective_outcome') in outcome]
        klass = _csv(args, 'class')
        if klass:
            f = [r for r in f if r.get('conversation_class') in klass]
        if args.get('meeting') == '1':
            f = [r for r in f if r.get('is_meeting')]
        if args.get('mismatch') == '1':
            f = [r for r in f if r.get('disposition_mismatch')]
        if (args.get('hour') or '') != '':
            f = [r for r in f if r.get('local_hour') == int(args['hour'])]
        if (args.get('dow') or '') != '':
            f = [r for r in f if r.get('local_dow') == int(args['dow'])]
        f = [{k: r.get(k) for k in LIST_CALL_COLS} for r in f]
    elif source in ('openings', 'objections', 'mistakes', 'meetings'):
        f = _view_rows(source, ids)
        if source == 'openings':
            t = _csv(args, 'opener_type')
            if t:
                f = [v for v in f if v.get('opener_type') in t]
            d = _csv(args, 'death_cause')
            if d:
                f = [v for v in f if v.get('death_cause') in d]
            if args.get('died') == '1':
                f = [v for v in f if v.get('died_within_20s')]
            if args.get('attention') == '1':
                f = [v for v in f if v.get('attention_captured')]
            if args.get('attention') == '0':
                f = [v for v in f if not v.get('attention_captured')]
        elif source == 'objections':
            t = _csv(args, 'objection_type')
            if t:
                f = [v for v in f if v.get('objection_type') in t]
            h = _csv(args, 'handled')
            if h:
                f = [v for v in f if v.get('handled') in h]
            if args.get('meeting') == '1':
                f = [v for v in f if v.get('is_meeting')]
        elif source == 'mistakes':
            t = _csv(args, 'mistake_type')
            if t:
                f = [v for v in f if v.get('mistake_type') in t]
            s = _csv(args, 'severity')
            if s:
                f = [v for v in f if v.get('severity') in s]
        elif source == 'meetings':
            fm = _csv(args, 'firmness')
            if fm:
                f = [v for v in f if v.get('firmness') in fm]
            dm = _csv(args, 'dm_fit')
            if dm:
                f = [v for v in f if v.get('decision_maker_fit') in dm]
    else:
        return None
    f = sorted(f, key=lambda r: r.get('time') or '', reverse=True)
    limit = min(int(args.get('limit') or 50), 200)
    offset = int(args.get('offset') or 0)
    return {'rows': f[offset:offset + limit], 'count': len(f)}


# ── flask wiring ──────────────────────────────────────────────────────────────

AGG_BY_VIEW = {'overview': agg_overview, 'openings': agg_openings,
               'objections': agg_objections, 'mistakes': agg_mistakes, 'reps': agg_reps}


def init_app(app, soql, load_campaigns, load_ledger, require_admin, data_dir):
    global CACHE_PATH
    _deps.update(soql=soql, load_campaigns=load_campaigns, load_ledger=load_ledger)
    CACHE_PATH = os.path.join(data_dir, 'cold_calls_cache.json.gz')
    _load_cache_from_disk()

    from flask import jsonify, request

    @app.route('/api/cold-calls/meta')
    def api_cc_meta():
        _maybe_background_refresh()
        cc = _CC
        if not cc:
            return jsonify({'ready': False,
                            'reason': 'no snapshot yet — refresh from the tab or check SUPABASE_SERVICE_KEY'})
        calls = cc.get('calls') or []
        newest = max((c.get('time') or '' for c in calls), default='')[:10]
        # business-day staleness: Sat/Sun don't count (no dialing)
        stale_bd = 0
        if newest:
            d = datetime.strptime(newest, '%Y-%m-%d').date()
            today = datetime.now(IST).date()
            while d < today:
                d += timedelta(days=1)
                if d.weekday() < 5:
                    stale_bd += 1
        camp_sets = _campaign_sets()
        matched = sum(1 for c in calls if c.get('sf_person_id'))
        return jsonify({
            'ready': True,
            'fetched_at': cc.get('fetched_at'),
            'refresh_error': cc.get('error'),
            'newest_call': newest,
            'stale_business_days': stale_bd,
            'total_calls': len(calls),
            'identity_matched': matched,
            'reps': sorted({c['owner_name'] for c in calls if c.get('owner_name')}),
            'pods': sorted({c['pod'] for c in calls if c.get('pod')}),
            'campaigns': sorted(camp_sets.keys()),
            'min_date': min((c.get('time') or '9' for c in calls), default='')[:10],
            'insights': cc.get('insights') or [],
        })

    @app.route('/api/cold-calls/aggregate')
    def api_cc_aggregate():
        if not _CC:
            return jsonify({'error': 'snapshot not ready'}), 503
        view = request.args.get('view') or 'overview'
        rows, scope = _filter_calls(request.args)
        if view == 'timing':
            payload = agg_timing(rows, 'et' if request.args.get('basis') == 'et' else 'local')
        elif view in AGG_BY_VIEW:
            payload = AGG_BY_VIEW[view](rows)
        else:
            return jsonify({'error': f"unknown view '{view}'"}), 400
        if scope:
            payload['scope'] = scope
        return jsonify(payload)

    @app.route('/api/cold-calls/list')
    def api_cc_list():
        if not _CC:
            return jsonify({'error': 'snapshot not ready'}), 503
        out = list_rows(request.args)
        if out is None:
            return jsonify({'error': 'unknown source'}), 400
        return jsonify(out)

    @app.route('/api/cold-calls/call/<call_id>')
    def api_cc_call(call_id):
        """Live transcript fetch — never cached. Read-only proxy to Supabase."""
        try:
            meta = _sb_get('nooks_calls', {
                'select': ('id,time,owner_name,pod,contact_name,contact_title,account_name,'
                           'disposition_name,effective_outcome,duration_sec,talk_ratio_rep,'
                           'conversation_class,recording_url,analysis,opening'),
                'id': f'eq.{call_id}', 'limit': 1})
            turns = _sb_get('nooks_call_transcripts', {
                'select': 'turns,truncated', 'call_id': f'eq.{call_id}', 'limit': 1})
            if not meta:
                return jsonify({'error': 'call not found'}), 404
            return jsonify({'call': meta[0],
                            'turns': (turns[0].get('turns') if turns else None) or [],
                            'truncated': bool(turns and turns[0].get('truncated'))})
        except Exception as e:
            return jsonify({'error': f'transcript fetch failed: {e}'}), 502

    @app.route('/api/cold-calls/refresh', methods=['POST'])
    @require_admin
    def api_cc_refresh():
        try:
            snap = refresh_cold_calls()
            return jsonify({'ok': True, 'calls': len(snap['calls']),
                            'fetched_at': snap['fetched_at']})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 502
