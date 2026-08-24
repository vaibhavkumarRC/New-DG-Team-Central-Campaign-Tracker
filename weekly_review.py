"""Weekly Review + Weekly Deep Dive — data layer.

One snapshot of every meeting-generating Lead since 2026-01-01, joined to Rahul's
Supabase `companies` table for the CEO's revenue bands, client type and specialty.
Everything both tabs render comes from this snapshot, so the two tabs can never
disagree with each other.

Design mirrors cold_calls.py deliberately:
  * Supabase access is GET-only. No write method exists here by design.
  * One gzip snapshot on the Railway volume, written atomically.
  * Lazy daily refresh in a background thread — no cron, no scheduler. The first
    request after REFRESH_HOUR_IST on a new day kicks a refresh; a failure leaves
    the previous snapshot serving rather than blanking the tab.

Why Supabase and not Salesforce for revenue/client-type/specialty:
  * SFDC Revenue_Bucket__c is a picklist with overlapping ranges ($100-250M,
    $100-500M and $250-500M all exist) and structurally cannot produce the CEO's
    bands. companies.revenue_estimate_usd is exact dollars, so bands are computed.
  * SFDC Speciality_Type__c is filled on ~25% of meeting accounts; Supabase
    specialty_type reaches ~52%.
Neither is complete, so every aggregate carries its own coverage count and the
page states it. A silently-wrong 100% is what loses CEO trust.
"""

import gzip
import json
import os
import re
import statistics
import threading
import urllib.parse
import urllib.request
from datetime import datetime, date, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

# History starts here. The week containing this date is flagged `partial`.
WR_EPOCH = date(2026, 1, 1)

# Refresh after the app's own 05:00 IST Salesforce sync so we never read a
# half-written cache. "Polls Salesforce once a day" is exactly this.
REFRESH_HOUR_IST = 6

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gvszpwyajzehqsofxzou.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# Wired by init_app()
_deps = {}
CACHE_PATH = None

_wr_lock = threading.Lock()      # guards the _WR swap + refresh flag
_refreshing = False
_WR = None                       # in-memory snapshot dict


# ── Supabase (GET only — no write method exists here by design) ───────────────

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


# ── ID normalisation ─────────────────────────────────────────────────────────

def sf15(v):
    """Salesforce IDs appear as both 15- and 18-char forms across systems.
    The 18-char form is the 15-char form plus a checksum suffix, so truncating
    to 15 is the safe join key in both directions."""
    v = (v or '').strip()
    return v[:15] if v else ''


# ── Seniority / function vocabularies ────────────────────────────────────────
# The Title parser MUST emit the same tokens as the field normalisers, or the
# matrix double-counts the same person under two spellings.

SENIORITY_ORDER = ['C-Level', 'VP', 'Head', 'Director', 'Manager',
                   'Individual Contributor']

# "Head of X" is checked BEFORE the Salesforce field, unlike every other bucket.
# Management_Level__c has no Head value — the picklist is C/VP/Director/Manager/
# Non-Manager — so the field cannot express it and levels these people as
# Director, VP or IC. The title is the only source that can, so it wins here.
_TITLE_HEAD_RULE = r'\bhead\s+of\b|^\s*head\b|\bhead\s*,'

_LEVEL_FIELD_MAP = {
    'c level':           'C-Level',
    'vp level':          'VP',
    'director level':    'Director',
    'manager level':     'Manager',
    'non manager level': 'Individual Contributor',
}

# Ordered most-senior-first: the first hit wins, so "Chief Nursing Officer,
# Director of Ops" levels as C-Level rather than Director.
# The (?<!vice[ -]) guard is load-bearing: without it "Vice President of
# Community Health Equity" matches \bpresident\b and levels as C-Level.
_TITLE_SENIORITY_RULES = [
    ('C-Level',  r'\b(chief|c[eft]o|cio|cmo|cno|coo|cmio|ciso|c-suite)\b'
                 r'|(?<!vice )(?<!vice-)\bpresident\b'
                 r'|\bowner\b|\bfounder\b|\bpartner\b'),
    ('VP',       r'\b(vice[- ]president|vp|svp|evp|avp)\b'),
    ('Director', r'\b(director|dir\.?)\b|\bmanaging\s+director\b'),
    ('Manager',  r'\b(manager|mgr\.?|supervisor|lead|team\s+lead)\b'),
]

FUNCTION_ORDER = ['Rev Cycle', 'HIM', 'Finance', 'Medical',
                  'C-Suite', 'Operations', 'IT', 'Other']

# Job_Function__c carries 17 raw values with obvious duplicates
# ("Finance & Accounting" vs "Finance", "C-Suite & Executive Leadership" vs
# "Executive"). Collapse them into the CEO's vocabulary.
_FUNCTION_FIELD_MAP = {
    'revenue cycle management':            'Rev Cycle',
    'coding/billing/account receivable':   'Rev Cycle',
    'health information management':       'HIM',
    'finance & accounting':                'Finance',
    'finance':                             'Finance',
    'clinical & medical':                  'Medical',
    'c-suite & executive leadership':      'C-Suite',
    'executive':                           'C-Suite',
    'operations':                          'Operations',
    'administration':                      'Operations',
    'information technology':              'IT',
    'digital/data/technology':             'IT',
    'strategy':                            'Other',
    'hr, legal, sales and marketing':      'Other',
    'legal & quality':                     'Other',
    'other':                               'Other',
}

# Rev Cycle before Finance: "VP Revenue Cycle Finance" is a rev-cycle person.
# Medical before C-Suite so "Chief Medical Officer" reads as Medical, matching
# how the SFDC picklist classifies clinicians.
# Entries are (bucket, pattern, case_sensitive). Case matters for "IT": a
# case-insensitive \bit\b matches the English word "it" in any title.
# Bare "md" is deliberately absent from Medical — it is a credential suffix
# ("CEO, MD") far more often than a function.
_TITLE_FUNCTION_RULES = [
    ('Rev Cycle',  r'\brev(enue)?\s*cycle\b|\brcm\b|\bbilling\b|\bcoding\b'
                   r'|\bcollections?\b|\breimbursement\b|\bdenials?\b'
                   r'|\ba/?r\b|\bpatient\s+financial\b|\bcharge\s+capture\b', False),
    ('HIM',        r'\bhealth\s+information\b|\bhim\b|\bmedical\s+records\b'
                   r'|\bcdi\b|\bclinical\s+documentation\b', False),
    ('Medical',    r'\b(medical|clinical|physician|nursing|nurse|surgeon'
                   r'|cmo|cno|cmio)\b', False),
    ('Finance',    r'\bfinanc\w*\b|\bcfo\b|\baccounting\b|\bcontroller\b'
                   r'|\btreasur\w*\b', False),
    ('IT',         r'\bI\.?T\.?\b', True),
    ('IT',         r'\b(information\s+technology|cio|ciso|technology'
                   r'|informatics|data|digital)\b', False),
    ('Operations', r'\b(operations?|ops|administrat\w*|practice\s+manage\w*)\b', False),
    ('C-Suite',    r'\b(chief|ceo|(?<!vice )(?<!vice-)president|owner'
                   r'|founder|executive)\b', False),
]


def _norm_seniority(level_field, title):
    """(bucket, source) where source is 'field' | 'title' | 'unknown'.
    Field wins when present — it is maintained data. The Title parser only fills
    the 19% of meetings where the field is blank, and is deterministic so any
    row can be explained by pointing at the matched word.

    "Head" is the one exception to field-first: Management_Level__c has no Head
    value, so Salesforce levels these people as Director/VP/IC. The title is the
    only source that can express the bucket the CEO asked for, so it wins."""
    t = (title or '').strip()
    if t and re.search(_TITLE_HEAD_RULE, t, re.I):
        return 'Head', 'title'
    key = (level_field or '').strip().lower()
    if key in _LEVEL_FIELD_MAP:
        return _LEVEL_FIELD_MAP[key], 'field'
    if t:
        for bucket, pattern in _TITLE_SENIORITY_RULES:
            if re.search(pattern, t, re.I):
                return bucket, 'title'
    return 'Unknown', 'unknown'


def _norm_function(function_field, title):
    """(bucket, source). Same field-first contract as _norm_seniority."""
    key = (function_field or '').strip().lower()
    if key in _FUNCTION_FIELD_MAP:
        return _FUNCTION_FIELD_MAP[key], 'field'
    t = (title or '').strip()
    if t:
        for bucket, pattern, case_sensitive in _TITLE_FUNCTION_RULES:
            if re.search(pattern, t, 0 if case_sensitive else re.I):
                return bucket, 'title'
    return 'Unknown', 'unknown'


# ── Revenue bands (the CEO's own cut points) ─────────────────────────────────

REVENUE_BANDS = ['<50M', '50-200M', '200-500M', '500M-2B', '>2B']


# ── Client type ──────────────────────────────────────────────────────────────
# companies.organisation_type is a bare code (28 distinct values). Expanded here
# because "FQHC / SP / PAC" is not a CEO-facing chart. SP, PG and PAC were
# confirmed against org_type_audit.reason rather than guessed:
#   SP  → "specialty outpatient clinic … optometry practices"
#   PG  → "physician-led integrated healthcare delivery"
#   PAC → "home health care services provider matches PAC category"
# Anything unmapped falls through to a title-cased version of the code, so a new
# code added upstream degrades to readable rather than disappearing.
CLIENT_TYPE_LABELS = {
    'FQHC': 'FQHC',                       'HOS':  'Hospital',
    'HS':   'Health System',              'AMC':  'Academic Medical Center',
    'CAH':  'Critical Access Hospital',   'ASC':  'Ambulatory Surgery Center',
    'SP':   'Specialty Practice',         'PG':   'Physician Group',
    'PAC':  'Post-Acute Care',            'BH':   'Behavioral Health',
    'UC':   'Urgent Care',                'MSO':  'MSO',
    'ACO':  'ACO',                        'PAYER': 'Payer',
    'RCM_VENDOR': 'RCM Vendor',           'HEALTH_IT': 'Health IT',
    'MEDICAL_DEVICE': 'Medical Device',   'LIFE_SCIENCES': 'Life Sciences',
    'HOME_CARE_NONMEDICAL': 'Home Care (non-medical)',
    'NONPROFIT_NONCARE': 'Non-profit (non-care)',
    'GOVERNMENT_NONPROVIDER': 'Government',
    'NON_HEALTHCARE': 'Non-healthcare',   'INVESTOR_REALESTATE': 'Investor / Real Estate',
    'CONSULTING': 'Consulting',           'STAFFING': 'Staffing',
    'EDUCATION': 'Education',
}


def _client_type_label(code):
    if not code:
        return None
    c = str(code).strip()
    if c.upper() in ('UNKNOWN', 'UNRESOLVED'):
        return None          # counts as unknown, not as a category
    return CLIENT_TYPE_LABELS.get(c.upper(), c.replace('_', ' ').title())


def _specialties(v):
    """companies.specialty_type is a text[] holding 0-12 entries; an empty array
    means 'not known', not 'none'. Always return a list."""
    if not v:
        return []
    if isinstance(v, list):
        return [s for s in (str(x).strip() for x in v) if s]
    return [str(v).strip()] if str(v).strip() else []


def _revenue_band(usd):
    if usd is None:
        return None
    try:
        v = float(usd)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if v < 50e6:
        return '<50M'
    if v < 200e6:
        return '50-200M'
    if v < 500e6:
        return '200-500M'
    if v < 2e9:
        return '500M-2B'
    return '>2B'


# ── Week bucketing ───────────────────────────────────────────────────────────

def _monday(d):
    return d - timedelta(days=d.weekday())


def _ist_today():
    return datetime.now(IST).date()


def _week_list(today=None):
    """Continuous Mondays from WR_EPOCH to the current week. Zero-meeting weeks
    are included on purpose — a week that vanishes reads as 'no data' when it
    actually means 'no meetings'."""
    today = today or _ist_today()
    cur = _monday(WR_EPOCH)
    end = _monday(today)
    out = []
    while cur <= end:
        sunday = cur + timedelta(days=6)
        if cur == end:
            state = 'in_progress'
        elif cur == _monday(WR_EPOCH) and WR_EPOCH.weekday() != 0:
            # Epoch falls mid-week, so this first bucket is short by a few days.
            state = 'partial'
        else:
            state = 'complete'
        out.append({
            'week':  cur.isoformat(),
            'end':   sunday.isoformat(),
            'state': state,
            'label': f"{cur.strftime('%b %-d')} – {sunday.strftime('%b %-d')}",
            'month': cur.strftime('%Y-%m'),
        })
        cur += timedelta(days=7)
    return out


def latest_complete_week(today=None):
    """The tab opens here: every number is final, so nothing shifts under the
    reader mid-review."""
    weeks = [w for w in _week_list(today) if w['state'] == 'complete']
    return weeks[-1]['week'] if weeks else _monday(WR_EPOCH).isoformat()


def _ist_date_from_utc(dt_str):
    """Meeting_Scheduled_on__c is a UTC datetime, unlike Meeting_Generated_on__c
    which is a pure DATE. 12 of 154 Q3 meetings shift a day if you skip this."""
    if not dt_str:
        return None
    try:
        s = dt_str.replace('Z', '+00:00')
        return datetime.fromisoformat(s).astimezone(IST).date().isoformat()
    except Exception:
        return (dt_str or '')[:10] or None


# ── Snapshot build ───────────────────────────────────────────────────────────

LEAD_FIELDS = """Id, Name, Title, Company, Email, Status,
    Meeting_Generated_on__c, Meeting_Generated_by__c, Meeting_Scheduled_on__c,
    Meeting_Status__c, Meeting_Source__c, Meeting_Channel__c,
    Seller_Name__c, Follow_Up_Owner__c,
    Management_Level__c, Job_Function__c,
    RC_Account_ID__c, Account_Lookup__c,
    IsConverted, ConvertedDate, ConvertedAccountId, ConvertedOpportunityId,
    SQL_Converted_Date__c, Zoom_Meeting_Link_URL__c"""

# Straight from SFDC Opportunity, never Rahul's Supabase `deals` mirror — that
# mirror truncates Closed Lost at 90 days and would silently under-report loss
# history. Only 269 opportunities exist org-wide, so there is no window filter:
# a lead converted in 2026 can legitimately attach to a deal opened in 2025.
OPP_FIELDS = """Id, Name, AccountId, StageName, Amount, CloseDate, CreatedDate,
    IsClosed, IsWon, Owner.Name, Next_Steps__c, Next_Steps_AI__c,
    Deal_Summary_AI__c, Deal_Risk_AI__c, Deal_Risk_Manual__c,
    Follow_up_Date__c, Loss_Reason__c, Loss_Reason_Explanation__c"""

# S1-S5 are the active pipeline (SFDC_ORG_GUIDE.md:212-221).
ACTIVE_STAGES = ('S1', 'S2', 'S3', 'S4', 'S5')

# Rung 2 matches a deal on the account Salesforce itself linked at conversion,
# within this many days of the conversion date. 90 days = a quarter; wider adds
# 9 more matches but stops being a defensible claim. Zero ambiguous matches at
# every window tested, so a tie-break rule is not needed.
RUNG2_WINDOW_DAYS = 90

COMPANY_SELECT = ('company_name,salesforce_account_id,rc_account_id,'
                  'revenue_estimate_usd,organisation_type,specialty_type,state')


def _company_index():
    """account id → Supabase company row, keyed on BOTH id spaces:
    the 15-char Salesforce id and the RC-internal id ("RC0015647"). Only
    companies carrying at least one of them are fetched (30.6k of 49k rows).
    sf15() is a no-op on RC ids, which are shorter than 15 chars."""
    rows = _sb_fetch_all(
        'companies', COMPANY_SELECT,
        extra_params={'or': '(salesforce_account_id.not.is.null,'
                            'rc_account_id.not.is.null)'})
    idx = {}
    for c in rows:
        for f in ('salesforce_account_id', 'rc_account_id'):
            k = sf15(c.get(f))
            if k:
                idx.setdefault(k, c)
    return idx, len(rows)


def _is_done(r):
    """Meeting Done, OR converted to S1. The S1 override is essential:
    conversion makes the Lead read-only, freezing Meeting_Status__c at whatever
    it was at that instant — several leads sit on 'Meeting Scheduled' though
    their meeting demonstrably happened."""
    ms = (r.get('Meeting_Status__c') or '')
    return ms.startswith('Meeting Done') or r.get('Status') == 'S1 Converted'


def _is_sql(r):
    return ((r.get('Meeting_Status__c') or '') == 'Meeting Done-SQL'
            or r.get('Status') == 'S1 Converted')


def _build_rows(lead_records, comp_idx, norm_sdr):
    today = _ist_today().isoformat()
    rows = []
    for r in lead_records:
        gen = (r.get('Meeting_Generated_on__c') or '')[:10]
        if not gen:
            continue
        try:
            gd = date.fromisoformat(gen)
        except ValueError:
            continue

        # TWO DIFFERENT ID SPACES — do not collapse them.
        #   Account_Lookup__c  = the real Salesforce Account id ("001f6...")
        #   RC_Account_ID__c   = an RC-internal identifier ("RC0015647")
        # 688 of 837 meetings carry both and they never agree. Supabase indexes
        # both (salesforce_account_id / rc_account_id) so either resolves a
        # company, but Opportunity.AccountId only ever matches the Salesforce
        # one — joining deals on the RC id silently matches nothing.
        sf_acct = sf15(r.get('Account_Lookup__c'))
        rc_acct = (r.get('RC_Account_ID__c') or '').strip()
        comp = comp_idx.get(sf_acct) if sf_acct else None
        if comp is None and rc_acct:
            comp = comp_idx.get(rc_acct)

        seniority, sen_src = _norm_seniority(r.get('Management_Level__c'), r.get('Title'))
        function,  fn_src  = _norm_function(r.get('Job_Function__c'),      r.get('Title'))

        seller_raw = (r.get('Seller_Name__c') or '').strip()
        sdr_raw    = (r.get('Meeting_Generated_by__c') or '').strip()
        sched = _ist_date_from_utc(r.get('Meeting_Scheduled_on__c'))

        rows.append({
            'id':            r.get('Id'),
            'name':          r.get('Name') or '—',
            'title':         r.get('Title') or '',
            'company':       (comp or {}).get('company_name') or r.get('Company') or '—',
            'week':          _monday(gd).isoformat(),
            'generated_on':  gen,
            'scheduled_on':  sched,
            'sdr':           norm_sdr(sdr_raw) if sdr_raw else None,
            'seller':        norm_sdr(seller_raw) if seller_raw else None,
            'source':        r.get('Meeting_Source__c') or None,
            'channel':       r.get('Meeting_Channel__c') or None,
            'meeting_status': r.get('Meeting_Status__c') or None,
            'status':        r.get('Status') or None,
            'seniority':     seniority,
            'seniority_src': sen_src,
            'function':      function,
            'function_src':  fn_src,
            'sf_account_id': sf_acct or None,
            'rc_account_id': rc_acct or None,
            'revenue_usd':   (comp or {}).get('revenue_estimate_usd'),
            'revenue_band':  _revenue_band((comp or {}).get('revenue_estimate_usd')),
            'client_type':   _client_type_label((comp or {}).get('organisation_type')),
            'specialties':   _specialties((comp or {}).get('specialty_type')),
            'state':         (comp or {}).get('state') or None,
            'opp_id':        r.get('ConvertedOpportunityId') or None,
            'converted':     bool(r.get('IsConverted')),
            'converted_on':  (r.get('ConvertedDate') or '')[:10] or None,
            'converted_acct': sf15(r.get('ConvertedAccountId')) or None,
            'sql_converted_on': (r.get('SQL_Converted_Date__c') or '')[:10] or None,
            'is_done':       _is_done(r),
            'is_sql':        _is_sql(r),
            'is_upcoming':   bool(sched and sched >= today and not _is_done(r)),
            'zoom_url':      r.get('Zoom_Meeting_Link_URL__c') or None,
        })
    return rows


def _opp_view(o):
    """Only the fields a reader needs, so the snapshot stays small."""
    stage = o.get('StageName') or ''
    return {
        'id':          o.get('Id'),
        'name':        o.get('Name'),
        'stage':       stage or None,
        'is_active':   stage.startswith(ACTIVE_STAGES),
        'is_closed':   bool(o.get('IsClosed')),
        'is_won':      bool(o.get('IsWon')),
        'amount':      o.get('Amount'),
        'close_date':  (o.get('CloseDate') or '')[:10] or None,
        'created_on':  (o.get('CreatedDate') or '')[:10] or None,
        'owner':       ((o.get('Owner') or {}) or {}).get('Name'),
        'next_steps':  (o.get('Next_Steps__c') or '').strip() or None,
        'next_steps_ai': (o.get('Next_Steps_AI__c') or '').strip() or None,
        'summary_ai':  (o.get('Deal_Summary_AI__c') or '').strip() or None,
        'risk':        (o.get('Deal_Risk_Manual__c')
                        or (o.get('Deal_Risk_AI__c') or '').strip() or None),
        'follow_up_on': (o.get('Follow_up_Date__c') or '')[:10] or None,
        'loss_reason': o.get('Loss_Reason__c') or None,
        'loss_detail': (o.get('Loss_Reason_Explanation__c') or '').strip() or None,
    }


def _link_deals(rows, opp_records):
    """Attach a deal to each meeting via an explicit ladder, and tag which rung
    matched. Rung 2 is never presented as certain anywhere in the UI.

      rung 1  Lead.ConvertedOpportunityId          — Salesforce's own link
      rung 2  ConvertedAccountId + ±90d of         — the account SF linked at
              ConvertedDate                          conversion; probable

    An opportunity is claimed at most once: if rung 1 already tied a deal to
    another lead, rung 2 will not re-claim it for a second meeting. Otherwise a
    single deal would inflate several cohorts at once."""
    opps = {sf15(o.get('Id')): o for o in opp_records if o.get('Id')}
    by_account = {}
    for o in opp_records:
        by_account.setdefault(sf15(o.get('AccountId')), []).append(o)

    claimed = {sf15(r['opp_id']) for r in rows if r.get('opp_id')}

    for r in rows:
        r['deal'] = None
        r['deal_rung'] = None
        if r.get('opp_id'):
            o = opps.get(sf15(r['opp_id']))
            if o:
                r['deal'] = _opp_view(o)
                r['deal_rung'] = 1

    for r in rows:
        if r['deal'] or not r.get('converted_acct') or not r.get('converted_on'):
            continue
        try:
            cd = date.fromisoformat(r['converted_on'])
        except ValueError:
            continue
        best = None
        for o in by_account.get(r['converted_acct'], []):
            oid = sf15(o.get('Id'))
            if oid in claimed:
                continue
            created = (o.get('CreatedDate') or '')[:10]
            try:
                gap = abs((date.fromisoformat(created) - cd).days)
            except ValueError:
                continue
            if gap <= RUNG2_WINDOW_DAYS and (best is None or gap < best[0]):
                best = (gap, o)
        if best:
            claimed.add(sf15(best[1].get('Id')))
            r['deal'] = _opp_view(best[1])
            r['deal_rung'] = 2
    return rows


def refresh_weekly():
    """Pull a fresh snapshot. Failure leaves the previous snapshot serving."""
    global _WR
    soql = _deps['soql']
    norm_sdr = _deps['norm_sdr']

    q = (f"SELECT {' '.join(LEAD_FIELDS.split())} FROM Lead "
         f"WHERE Meeting_Generated_on__c >= {WR_EPOCH.isoformat()} "
         f"ORDER BY Meeting_Generated_on__c")
    res = soql(q)
    if not res:
        raise RuntimeError('SOQL returned nothing — Salesforce auth or network')
    leads = res.get('records', [])

    opp_res = soql(f"SELECT {' '.join(OPP_FIELDS.split())} FROM Opportunity")
    opp_records = (opp_res or {}).get('records', [])

    comp_idx, comp_rows = _company_index()
    rows = _link_deals(_build_rows(leads, comp_idx, norm_sdr), opp_records)

    snap = {
        'fetched_at':   datetime.now(IST).isoformat(),
        'epoch':        WR_EPOCH.isoformat(),
        'rows':         rows,
        'lead_count':   len(leads),
        'opp_count':    len(opp_records),
        'company_rows': comp_rows,
        'error':        None,
    }
    tmp = CACHE_PATH + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(snap, f)
    os.replace(tmp, CACHE_PATH)
    with _wr_lock:
        _WR = snap
    matched = sum(1 for r in rows if r['revenue_band'])
    r1 = sum(1 for r in rows if r['deal_rung'] == 1)
    r2 = sum(1 for r in rows if r['deal_rung'] == 2)
    print(f"[weekly_review] snapshot refreshed: {len(rows)} meetings, "
          f"{comp_rows} company rows, {matched} with a revenue band, "
          f"{len(opp_records)} opportunities, deals linked {r1} certain "
          f"+ {r2} probable")
    return snap


def _load_cache_from_disk():
    global _WR
    try:
        with gzip.open(CACHE_PATH, 'rt', encoding='utf-8') as f:
            _WR = json.load(f)
        print(f"[weekly_review] cache loaded: {len(_WR.get('rows', []))} meetings "
              f"(fetched {_WR.get('fetched_at')})")
    except FileNotFoundError:
        _WR = None
    except Exception as e:
        print(f"[weekly_review] cache load failed: {e}")
        _WR = None


def _maybe_background_refresh():
    """Lazy daily refresh — no cron, no scheduler. The first request after
    REFRESH_HOUR_IST on a new day kicks a refresh in a thread, so no request
    ever blocks on Salesforce. A failure leaves the old snapshot serving."""
    global _refreshing
    now = datetime.now(IST)
    with _wr_lock:
        wr = _WR
        if _refreshing:
            return
        fetched = wr.get('fetched_at') if wr else None
        need = wr is None or (
            now.hour >= REFRESH_HOUR_IST
            and (not fetched or fetched[:10] < now.strftime('%Y-%m-%d')))
        if not need:
            return
        _refreshing = True

    def run():
        global _refreshing
        try:
            refresh_weekly()
        except Exception as e:
            print(f"[weekly_review] background refresh failed "
                  f"(cache keeps serving): {e}")
            with _wr_lock:
                if _WR is not None:
                    _WR['error'] = f"refresh failed {datetime.now(IST).isoformat()}: {e}"
        finally:
            _refreshing = False
    threading.Thread(target=run, daemon=True).start()


# ── Aggregation ──────────────────────────────────────────────────────────────

def _tally(rows, key, order=None, unknown_label='Unknown'):
    """Count rows by `key`, returning ordered buckets plus an explicit coverage
    count. Every chart on the page prints that coverage — 'known for 629/836'
    survives scrutiny in a way a silent 100% does not."""
    counts = {}
    known = 0
    for r in rows:
        v = r.get(key)
        if v:
            known += 1
            counts[v] = counts.get(v, 0) + 1
    unknown = len(rows) - known
    if order:
        buckets = [{'key': k, 'n': counts.get(k, 0)} for k in order if counts.get(k)]
        extra = sorted((k for k in counts if k not in order),
                       key=lambda k: -counts[k])
        buckets += [{'key': k, 'n': counts[k]} for k in extra]
    else:
        buckets = [{'key': k, 'n': n}
                   for k, n in sorted(counts.items(), key=lambda kv: -kv[1])]
    if unknown:
        buckets.append({'key': unknown_label, 'n': unknown, 'unknown': True})
    return {'buckets': buckets,
            'coverage': {'known': known, 'total': len(rows)}}


def _tally_multi(rows, key, unknown_label='Unknown'):
    """Tally a list-valued dimension (specialty). A multi-specialty group is
    counted once under each of its specialties, so the bars deliberately sum to
    more than the meeting count — `total_tags` lets the page say so instead of
    letting the reader assume the bars are a partition."""
    counts = {}
    known = 0
    tags = 0
    for r in rows:
        vals = r.get(key) or []
        if vals:
            known += 1
            for v in set(vals):
                counts[v] = counts.get(v, 0) + 1
                tags += 1
    unknown = len(rows) - known
    buckets = [{'key': k, 'n': n}
               for k, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
    if unknown:
        buckets.append({'key': unknown_label, 'n': unknown, 'unknown': True})
    return {'buckets': buckets,
            'multi': True,
            'total_tags': tags,
            'coverage': {'known': known, 'total': len(rows)}}


def _week_rows(rows, week):
    return [r for r in rows if r['week'] == week]


def _week_key(iso):
    """Monday of the week containing an ISO date string, or None."""
    try:
        return _monday(date.fromisoformat(iso or '')).isoformat()
    except ValueError:
        return None


def _headline(rows, cohort, week):
    """generated/upcoming follow the booking cohort; done/s1 are EVENT-dated —
    done = meetings HELD in the week (scheduled_on, any cohort), s1 = leads
    CONVERTED in the week (ConvertedDate, any cohort). Every done meeting in
    the snapshot carries a scheduled_on, so nothing is dropped by the bucketing."""
    return {
        'generated': len(cohort),
        'done':      sum(1 for r in rows
                         if r['is_done'] and _week_key(r.get('scheduled_on')) == week),
        's1':        sum(1 for r in rows
                         if _week_key(r.get('converted_on')) == week),
        'upcoming':  sum(1 for r in cohort if r['is_upcoming']),
    }


def _typical(rows, weeks, upto):
    """Median of the 12 complete weeks up to and including `upto`. Median on
    purpose — a conference spike must not move what 'normal' looks like.
    `done` is event-dated (held that week) to match the headline tile."""
    complete = [w['week'] for w in weeks
                if w['state'] == 'complete' and w['week'] <= upto][-12:]
    if not complete:
        return None
    by_week = {w: 0 for w in complete}
    done = {w: 0 for w in complete}
    for r in rows:
        if r['week'] in by_week:
            by_week[r['week']] += 1
        if r['is_done']:
            dw = _week_key(r.get('scheduled_on'))
            if dw in done:
                done[dw] += 1
    return {
        'generated': int(statistics.median(by_week.values())),
        'done':      int(statistics.median(done.values())),
        'weeks_used': len(complete),
    }


def _seller_table(rows):
    agg = {}
    for r in rows:
        k = r['seller'] or 'Unattributed'
        a = agg.setdefault(k, {'seller': k, 'generated': 0, 'done': 0, 'sql': 0,
                               'unattributed': r['seller'] is None})
        a['generated'] += 1
        a['done'] += 1 if r['is_done'] else 0
        a['sql'] += 1 if r['is_sql'] else 0
    out = sorted(agg.values(), key=lambda a: (-a['generated'], a['seller']))
    known = sum(a['generated'] for a in out if not a['unattributed'])
    return {'rows': out, 'coverage': {'known': known, 'total': len(rows)}}


def _persona_matrix(rows):
    """Seniority x function heat matrix. `source_split` is what makes it
    drillable: the page can state how many cells came from Salesforce fields
    versus the Title parser."""
    cells = {}
    for r in rows:
        cells[(r['seniority'], r['function'])] = \
            cells.get((r['seniority'], r['function']), 0) + 1
    seniorities = [s for s in SENIORITY_ORDER
                   if any(k[0] == s for k in cells)] + \
                  (['Unknown'] if any(k[0] == 'Unknown' for k in cells) else [])
    functions = [f for f in FUNCTION_ORDER
                 if any(k[1] == f for k in cells)] + \
                (['Unknown'] if any(k[1] == 'Unknown' for k in cells) else [])
    return {
        'seniorities': seniorities,
        'functions':   functions,
        'cells': [{'seniority': s, 'function': f, 'n': cells.get((s, f), 0)}
                  for s in seniorities for f in functions],
        'seniority_totals': [{'key': s,
                              'n': sum(v for k, v in cells.items() if k[0] == s)}
                             for s in seniorities],
        'function_totals':  [{'key': f,
                              'n': sum(v for k, v in cells.items() if k[1] == f)}
                             for f in functions],
        'source_split': {
            'seniority': {
                'field':   sum(1 for r in rows if r['seniority_src'] == 'field'),
                'title':   sum(1 for r in rows if r['seniority_src'] == 'title'),
                'unknown': sum(1 for r in rows if r['seniority_src'] == 'unknown')},
            'function': {
                'field':   sum(1 for r in rows if r['function_src'] == 'field'),
                'title':   sum(1 for r in rows if r['function_src'] == 'title'),
                'unknown': sum(1 for r in rows if r['function_src'] == 'unknown')},
        },
        'coverage': {
            'known': sum(1 for r in rows
                         if r['seniority'] != 'Unknown' and r['function'] != 'Unknown'),
            'total': len(rows)},
    }


def _pipeline(rows):
    """Meetings done → deals created, for one cohort. The ratio's denominator is
    meetings DONE, not generated: a meeting that hasn't happened yet cannot have
    produced pipeline, and including it would drag every recent week down."""
    done = [r for r in rows if r['is_done']]
    with_deal = [r for r in done if r['deal']]

    # Two leads can legitimately convert into ONE opportunity — e.g. Bruce Maki
    # and Alexis Martin at Michigan Primary Care Association, a week apart, both
    # into the same $50k deal. So the ratio counts MEETINGS (both of those
    # meetings did produce pipeline) but the amount is summed over DISTINCT
    # deals, or that one deal would contribute $100k across two cohorts.
    seen = {}
    for r in with_deal:
        seen.setdefault(r['deal']['id'], r['deal'])
    amounts = [d['amount'] for d in seen.values() if d.get('amount') is not None]

    return {
        'done':               len(done),
        'meetings_with_deal': len(with_deal),
        'deals':              len(seen),
        'ratio_pct':   round(100 * len(with_deal) / len(done), 1) if done else None,
        'amount':      sum(amounts) if amounts else 0,
        'amount_known': len(amounts),
        'amount_total_deals': len(seen),
        'rung_split': {
            'certain':  sum(1 for r in with_deal if r['deal_rung'] == 1),
            'probable': sum(1 for r in with_deal if r['deal_rung'] == 2),
        },
    }


def _s1_this_week(rows, week):
    """'Calls that moved to S1 this week' — keyed on the date the LEAD converted,
    not on the meeting date, so a meeting from six weeks ago shows up in the week
    it actually converted. The Week column names the cohort it came from, which
    is what makes the box readable: it says which week's work is landing now."""
    try:
        start = date.fromisoformat(week)
    except ValueError:
        return {'rows': []}
    end = start + timedelta(days=6)
    out = []
    for r in rows:
        cd = r.get('converted_on')
        if not cd:
            continue
        try:
            d = date.fromisoformat(cd)
        except ValueError:
            continue
        if not (start <= d <= end):
            continue
        out.append({
            'lead_id':      r['id'],
            'cohort_week':  r['week'],
            'converted_on': cd,
            'client':       r['company'],
            'revenue_band': r['revenue_band'],
            'revenue_usd':  r['revenue_usd'],
            'seller':       r['seller'],
            'deal_amount':  (r['deal'] or {}).get('amount'),
            'deal_stage':   (r['deal'] or {}).get('stage'),
            'deal_rung':    r['deal_rung'],
        })
    out.sort(key=lambda x: (x['converted_on'], x['client']))
    return {'rows': out}


def _cohort_table(rows, weeks):
    """Every week since the epoch, followed forward. Zero-meeting weeks stay in
    the table — a gap that vanishes reads as 'no data' when it means 'no
    meetings'."""
    by_week = {w['week']: [] for w in weeks}
    for r in rows:
        if r['week'] in by_week:
            by_week[r['week']].append(r)
    out = []
    for w in weeks:
        wr = by_week[w['week']]
        p = _pipeline(wr)
        out.append({
            'week':       w['week'],
            'label':      w['label'],
            'state':      w['state'],
            'generated':  len(wr),
            'done':       p['done'],
            'sql':        sum(1 for r in wr if r['is_sql']),
            'converted':  sum(1 for r in wr if r['converted']),
            'deals':      p['deals'],
            'amount':     p['amount'],
            'ratio_pct':  p['ratio_pct'],
        })
    return out


def _deal_review(rows, week):
    """The 'last-to-last week' block: what happened to a cohort old enough to
    have deals. Young cohorts get a warning banner instead of an empty table —
    two weeks is roughly when a meeting has been held and worked."""
    with_deal = [r for r in rows if r['deal']]
    with_deal.sort(key=lambda r: (-(r['deal'].get('amount') or 0), r['company']))
    return {
        'week': week,
        'rows': [{
            'lead_id':     r['id'],
            'client':      r['company'],
            'contact':     r['name'],
            'seller':      r['seller'],
            'revenue_band': r['revenue_band'],
            'deal_rung':   r['deal_rung'],
            'deal':        r['deal'],
        } for r in with_deal],
        'meetings':  len(rows),
        'done':      sum(1 for r in rows if r['is_done']),
    }


def build_review(week=None):
    wr = _WR
    if not wr:
        return {'ready': False,
                'reason': 'no snapshot yet — POST /api/weekly/refresh, '
                          'or check SUPABASE_SERVICE_KEY and the sf CLI session'}
    rows = wr['rows']
    weeks = _week_list()
    valid = {w['week'] for w in weeks}
    if not week or week not in valid:
        week = latest_complete_week()

    cur = _week_rows(rows, week)
    prev_week = (date.fromisoformat(week) - timedelta(days=7)).isoformat()
    prev = _week_rows(rows, prev_week) if prev_week in valid else []

    # "Last to last week" = two weeks before the selected one. That cohort has
    # had time to be held and worked, so it is the one worth reviewing deals on.
    l2l_week = (date.fromisoformat(week) - timedelta(days=14)).isoformat()
    l2l_rows = _week_rows(rows, l2l_week) if l2l_week in valid else []
    l2l_age_days = (_ist_today() - date.fromisoformat(l2l_week)).days

    # Per-week generated counts drive the selector labels and the trend line.
    per_week = {w['week']: 0 for w in weeks}
    for r in rows:
        if r['week'] in per_week:
            per_week[r['week']] += 1
    for w in weeks:
        w['generated'] = per_week.get(w['week'], 0)

    state = next((w['state'] for w in weeks if w['week'] == week), 'complete')

    return {
        'ready':      True,
        'fetched_at': wr.get('fetched_at'),
        'error':      wr.get('error'),
        'week':       week,
        'week_state': state,
        'week_label': next((w['label'] for w in weeks if w['week'] == week), week),
        'weeks':      weeks,
        'headline': {
            'current':  _headline(rows, cur, week),
            'previous': _headline(rows, prev, prev_week),
            'typical':  _typical(rows, weeks, week),
        },
        'source':       _tally(cur, 'source'),
        'channel':      _tally(cur, 'channel'),
        'revenue_band': _tally(cur, 'revenue_band', order=REVENUE_BANDS),
        'client_type':  _tally(cur, 'client_type'),
        'specialty':    _tally_multi(cur, 'specialties'),
        'seller':       _seller_table(cur),
        'persona':      _persona_matrix(cur),
        'pipeline': {
            'current':  _pipeline(cur),
            'previous': _pipeline(prev),
        },
        's1_this_week': _s1_this_week(rows, week),
        'cohorts':      _cohort_table(rows, weeks),
        'deal_review':  _deal_review(l2l_rows, l2l_week),
        'deal_review_young': l2l_age_days < 14,
    }


# ── Deep Dive: filtering ─────────────────────────────────────────────────────

# The sentinel a chart's "Unknown" bar drills through on. Without it, clicking
# the 10 unknown-revenue meetings would silently return everything.
UNKNOWN = '__unknown__'

# Query-string key → row field. `specialties` is the one list-valued field: a
# meeting matches if ANY of its specialties matches.
FILTER_FIELDS = {
    'source':       'source',
    'channel':      'channel',
    'revenue_band': 'revenue_band',
    'client_type':  'client_type',
    'specialty':    'specialties',
    'seller':       'seller',
    'seniority':    'seniority',
    'function':     'function',
    'status':       'meeting_status',
}

# Values that mean "we don't know", not "a category called Unknown".
_UNKNOWN_VALUES = (None, '', 'Unknown')


def _matches(row, key, wanted):
    """Values within one filter are OR'd; separate filters are AND'd."""
    field = FILTER_FIELDS[key]
    val = row.get(field)
    if field == 'specialties':
        vals = val or []
        if not vals:
            return UNKNOWN in wanted
        return bool(set(vals) & wanted) or (UNKNOWN in wanted and not vals)
    if val in _UNKNOWN_VALUES:
        return UNKNOWN in wanted
    return val in wanted


def _facets(rows):
    """Distinct values per dimension, for the Deep Dive filter controls. Built
    from the rows actually in scope so the dropdowns never offer a dead option."""
    out = {}
    for key, field in FILTER_FIELDS.items():
        counts = {}
        unknown = 0
        for r in rows:
            if field == 'specialties':
                vals = r.get(field) or []
                if not vals:
                    unknown += 1
                for v in set(vals):
                    counts[v] = counts.get(v, 0) + 1
            else:
                v = r.get(field)
                if v in _UNKNOWN_VALUES:
                    unknown += 1
                else:
                    counts[v] = counts.get(v, 0) + 1
        items = [{'key': k, 'n': n}
                 for k, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
        if unknown:
            items.append({'key': UNKNOWN, 'label': 'Unknown', 'n': unknown})
        out[key] = items
    return out


def _row_view(r, sf_base):
    d = r.get('deal') or {}
    return {
        'id':           r['id'],
        'name':         r['name'],
        'title':        r['title'],
        'company':      r['company'],
        'week':         r['week'],
        'generated_on': r['generated_on'],
        'scheduled_on': r['scheduled_on'],
        'sdr':          r['sdr'],
        'seller':       r['seller'],
        'source':       r['source'],
        'channel':      r['channel'],
        'seniority':    r['seniority'],
        'function':     r['function'],
        'revenue_band': r['revenue_band'],
        'client_type':  r['client_type'],
        'specialties':  r['specialties'],
        'meeting_status': r['meeting_status'],
        'status':       r['status'],
        'is_done':      r['is_done'],
        'is_sql':       r['is_sql'],
        'is_upcoming':  r['is_upcoming'],
        'converted_on': r['converted_on'],
        'deal_rung':    r['deal_rung'],
        'deal': ({'amount': d.get('amount'), 'stage': d.get('stage'),
                  'owner': d.get('owner'), 'close_date': d.get('close_date'),
                  'next_steps': d.get('next_steps') or d.get('next_steps_ai'),
                  'is_won': d.get('is_won'), 'is_closed': d.get('is_closed')}
                 if d else None),
        'sf_url': f"{sf_base}/lightning/r/Lead/{r['id']}/view" if sf_base else None,
    }


def build_meetings(args, sf_base=''):
    """Deep Dive list. `week` is optional — omitting it searches all history,
    which is what the tab does when entered directly rather than drilled into."""
    wr = _WR
    if not wr:
        return {'ready': False, 'reason': 'no snapshot yet'}
    rows = wr['rows']

    week = (args.get('week') or '').strip()
    if week:
        rows = [r for r in rows if r['week'] == week]

    # Event-dated drills from the headline tiles: done_week = meetings HELD in
    # that week (any booking cohort), s1_week = leads CONVERTED in that week.
    done_week = (args.get('done_week') or '').strip()
    if done_week:
        rows = [r for r in rows
                if r['is_done'] and _week_key(r.get('scheduled_on')) == done_week]
    s1_week = (args.get('s1_week') or '').strip()
    if s1_week:
        rows = [r for r in rows if _week_key(r.get('converted_on')) == s1_week]
    scoped = rows

    applied = {}
    for key in FILTER_FIELDS:
        raw = (args.get(key) or '').strip()
        if not raw:
            continue
        wanted = {v.strip() for v in raw.split(',') if v.strip()}
        if not wanted:
            continue
        applied[key] = sorted(wanted)
        rows = [r for r in rows if _matches(r, key, wanted)]

    outcome = (args.get('outcome') or '').strip()
    if outcome == 'done':
        rows = [r for r in rows if r['is_done']]
    elif outcome == 'upcoming':
        rows = [r for r in rows if r['is_upcoming']]
    elif outcome == 'sql':
        rows = [r for r in rows if r['is_sql']]
    elif outcome == 'deal':
        rows = [r for r in rows if r['deal']]
    if outcome:
        applied['outcome'] = [outcome]
    if done_week:
        applied['done_week'] = [done_week]
    if s1_week:
        applied['s1_week'] = [s1_week]

    rows = sorted(rows, key=lambda r: (r['generated_on'], r['company']), reverse=True)

    return {
        'ready':      True,
        'fetched_at': wr.get('fetched_at'),
        'week':       week or None,
        'total':      len(wr['rows']),
        'in_scope':   len(scoped),
        'filtered':   len(rows),
        'applied':    applied,
        'facets':     _facets(scoped),
        'weeks':      [w for w in _week_list()],
        'rows':       rows[:1000],
        'truncated':  len(rows) > 1000,
    }


# ── Call detail drawer + transcript summaries (Stage 5) ─────────────────────
# One summary per meeting, generated on first drawer open and cached forever in
# DATA_DIR/call_summaries.json. Source ladder (measured coverage in
# docs/DEFINITIONS.md, sample of 50 done meetings, 2026-08-20):
#   1. zoom_ai    Zoom AI Companion summary file          (48% of done meetings)
#   2. zoom_llm   Claude over the Zoom transcript VTT     (~0% extra — every VTT
#                 in the sample also had an AI summary; kept as a safety rung)
#   3. nooks_llm  Claude over the Nooks BOOKING-CALL      (+24%; labeled as the
#                 transcript                               booking call, not the
#                                                          meeting itself)
#   4. none       28% — reasons reported honestly, never cached (a recording can
#                 appear hours after the meeting; a cached "nothing" would stick).

SUMMARY_CACHE_VER = 2            # bump to invalidate every cached summary
                                 # v2: paragraphed summaries; zoom_ai entries are
                                 # Claude-structured (challenges/key points filled)
SUMMARY_CACHE_PATH = None        # set by init_app
_sum_lock = threading.Lock()

SUMMARY_MODEL_DEFAULT = 'claude-sonnet-5'
_TRANSCRIPT_CHAR_CAP = 60_000    # ~15k tokens; a 30-min call is ~¼ of this

_TS_LINE = re.compile(r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+-->\s+\d{2}:\d{2}:\d{2}\.\d{3}')


def _load_summaries():
    try:
        with open(SUMMARY_CACHE_PATH) as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_summary(key, entry):
    with _sum_lock:
        d = _load_summaries()
        d[key] = entry
        tmp = SUMMARY_CACHE_PATH + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(d, f)
        os.replace(tmp, SUMMARY_CACHE_PATH)


def _vtt_clean(vtt_text):
    """Zoom VTT → 'Speaker: text' lines, consecutive same-speaker cues merged.
    Lifted from the Rattle-replacement service (rattle_replacement/service/vtt.py)."""
    turns = []
    for block in (vtt_text or '').replace('\r\n', '\n').split('\n\n'):
        lines = [ln.strip() for ln in block.strip().split('\n') if ln.strip()]
        if not lines or lines[0] == 'WEBVTT':
            continue
        text_lines, seen_ts = [], False
        for ln in lines:
            if _TS_LINE.match(ln):
                seen_ts = True
                continue
            if not seen_ts:
                continue            # cue id / header junk before the timestamp
            text_lines.append(ln)
        if not text_lines:
            continue
        raw = ' '.join(text_lines)
        if ':' in raw:
            speaker, text = raw.split(':', 1)
            speaker, text = speaker.strip(), text.strip()
            if len(speaker) > 60:   # sentence containing a colon, not a name
                speaker, text = '', raw
        else:
            speaker, text = '', raw
        if turns and turns[-1][0] == speaker:
            turns[-1] = (speaker, turns[-1][1] + ' ' + text)
        else:
            turns.append((speaker, text))
    return '\n'.join(f'{s}: {t}' if s else t for s, t in turns)


def _zoom_summary_text(j):
    """Zoom AI Companion summary JSON → multi-paragraph text. Keeps the section
    structure (summary_details) that app.py's _parse_summary flattens away —
    the sections are what stop the drawer being a wall of text."""
    if not isinstance(j, dict):
        return str(j or '').strip()
    parts = []
    overall = (j.get('overall_summary') or j.get('summary')
               or j.get('summary_overview') or '').strip()
    if overall:
        parts.append(overall)
    for d in (j.get('summary_details') or []):
        if not isinstance(d, dict):
            continue
        label = (d.get('label') or d.get('summary_title') or '').strip()
        text = (d.get('summary') or d.get('text') or '').strip()
        if text:
            parts.append(f'{label}: {text}' if label else text)
    return '\n\n'.join(parts) or (j.get('_text') or '').strip()


_SUMMARY_SCHEMA = {
    'type': 'object',
    'properties': {
        'summary':    {'type': 'string'},
        'challenges': {'type': 'array', 'items': {'type': 'string'}},
        'next_steps': {'type': 'array', 'items': {'type': 'string'}},
        'key_points': {'type': 'array', 'items': {'type': 'string'}},
    },
    'required': ['summary', 'challenges', 'next_steps', 'key_points'],
    'additionalProperties': False,
}

_SUMMARY_SYSTEM = """You analyze sales-call transcripts for RapidClaims \
(healthcare revenue-cycle AI). Produce meeting intelligence matching these rules:

- summary: 2-4 short paragraphs separated by a blank line, third person, naming \
who met and what was discussed and decided. Never one wall of text.
- challenges: pain points, objections and blockers the CUSTOMER raised. Empty \
list if none.
- next_steps: short imperative items ("Bianca to schedule a 30-minute demo."). \
Only commitments actually made on the call. Empty list if none.
- key_points: the discussion's most important facts and decisions.

Write plainly; no markdown inside values."""


def _summarize_transcript(meta, transcript):
    """Claude call, structured output. Deferred import so the app boots without
    the anthropic package installed (summary endpoint then reports the error)."""
    import anthropic
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise RuntimeError('ANTHROPIC_API_KEY not set')
    model = os.environ.get('CLASSIFIER_MODEL', SUMMARY_MODEL_DEFAULT)
    if len(transcript) > _TRANSCRIPT_CHAR_CAP:
        transcript = transcript[:_TRANSCRIPT_CHAR_CAP] + '\n[transcript truncated]'
    user_msg = (f"Meeting: {meta.get('topic') or '—'}\n"
                f"Kind: {meta.get('kind') or 'sales meeting'}\n"
                f"When: {meta.get('when') or '—'}, "
                f"duration {meta.get('duration_min') or '?'} min\n\n"
                f"Transcript:\n{transcript}")
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=model, max_tokens=2048, thinking={'type': 'disabled'},
        system=_SUMMARY_SYSTEM,
        output_config={'format': {'type': 'json_schema', 'schema': _SUMMARY_SCHEMA}},
        messages=[{'role': 'user', 'content': user_msg}])
    u = resp.usage
    print(f'[weekly] summarized {meta.get("topic")}: '
          f'in={u.input_tokens} out={u.output_tokens} ({model})')
    return json.loads(next(b.text for b in resp.content if b.type == 'text'))


_LEAD_ID_RE = re.compile(r'^[a-zA-Z0-9]{15,18}$')


def _lead_live(lead_id):
    """Fresh single-lead fetch: the Zoom link (not in older snapshots) and the
    converted Contact id (Nooks identity follows the Contact after conversion)."""
    recs = _deps['soql']("SELECT Id, Name, Company, Zoom_Meeting_Link_URL__c, "
                         f"ConvertedContactId FROM Lead WHERE Id = '{lead_id}'")
    recs = recs.get('records', []) if isinstance(recs, dict) else (recs or [])
    return recs[0] if recs else None


NOOKS_CALL_SELECT = ('id,time,owner_name,contact_name,contact_title,account_name,'
                     'disposition_name,effective_outcome,duration_sec,is_meeting,'
                     'is_conversation,has_transcript,talk_ratio_rep,recording_url,'
                     'call_grade:analysis->>call_grade')


def _nooks_calls_for(person_ids):
    """All Nooks calls for a lead (and its converted Contact), newest first.
    15-char-prefix match — nooks sf_person_id mixes 15- and 18-char forms."""
    pids = [sf15(p) for p in person_ids if p and _LEAD_ID_RE.match(p)]
    if not pids:
        return []
    ors = ','.join(f'sf_person_id.like.{p}*' for p in pids)
    return _sb_get('nooks_calls', {'select': NOOKS_CALL_SELECT,
                                   'or': f'({ors})',
                                   'order': 'time.desc', 'limit': 25})


def _nooks_turns(call_id):
    rows = _sb_get('nooks_call_transcripts',
                   {'select': 'turns,truncated', 'call_id': f'eq.{call_id}',
                    'limit': 1})
    turns = (rows[0].get('turns') if rows else None) or []
    return turns, bool(rows and rows[0].get('truncated'))


def _zoom_recording_files(mid):
    """GET /meetings/{id}/recordings → download URLs for the summary,
    next-steps and transcript-VTT files. (app.py's _zoom_recording_assets only
    flags that a transcript exists; the drawer needs the actual URLs.)"""
    d = _deps['zoom']['get'](f'https://api.zoom.us/v2/meetings/{mid}/recordings')
    files = (d or {}).get('recording_files', [])
    out = {'has_recording': bool(files), 'duration': (d or {}).get('duration'),
           'topic': (d or {}).get('topic'), 'start_time': (d or {}).get('start_time'),
           'summary_url': None, 'next_steps_url': None, 'vtt_url': None}
    for f in files:
        rt, ft = f.get('recording_type'), f.get('file_type')
        if rt == 'summary':
            out['summary_url'] = f.get('download_url')
        elif rt == 'summary_next_steps':
            out['next_steps_url'] = f.get('download_url')
        if ft == 'TRANSCRIPT' or rt == 'audio_transcript':
            out['vtt_url'] = out['vtt_url'] or f.get('download_url')
    return out


def _pick_booking_call(calls):
    """Best transcript-bearing Nooks call: the meeting-booking call if flagged,
    else the longest conversation."""
    cands = [c for c in calls if c.get('has_transcript')]
    if not cands:
        return None
    return sorted(cands, key=lambda c: (not c.get('is_meeting'),
                                        -(c.get('duration_sec') or 0)))[0]


def _generate_summary(lead_id, row):
    """Walk the source ladder. Returns a cache-entry dict; positives are saved,
    source=None results are NOT cached (a recording can appear later)."""
    zoom = _deps.get('zoom') or {}
    lead = _lead_live(lead_id)
    reasons = []

    zoom_url = ((lead or {}).get('Zoom_Meeting_Link_URL__c')
                or (row or {}).get('zoom_url') or '')
    mid = zoom['extract_id'](zoom_url) if (zoom and zoom_url) else ''
    assets = None
    if not zoom:
        reasons.append('Zoom API not configured')
    elif not zoom_url:
        reasons.append('no Zoom link on the lead')
    elif not mid:
        reasons.append('Zoom link is a personal room (no numeric meeting id)')
    else:
        assets = _zoom_recording_files(mid)
        if not assets['has_recording']:
            reasons.append('no Zoom cloud recording for this meeting')

    entry = None
    when = (assets or {}).get('start_time') or (row or {}).get('scheduled_on')

    # rung 1: Zoom AI Companion summary. Claude restructures Zoom's own text
    # (paragraphs + challenges/key points — Zoom provides neither); the input is
    # the short summary, not a transcript, so the call costs ~a cent. If Claude
    # is unavailable the raw Zoom text still ships rather than failing the rung.
    if assets and assets['summary_url']:
        raw = zoom['download'](assets['summary_url'])
        text = _zoom_summary_text(raw) or zoom['parse_summary'](raw)
        if text:
            steps = ''
            if assets['next_steps_url']:
                steps = zoom['parse_next_steps'](
                    zoom['download'](assets['next_steps_url']))
            steps_list = [s for s in (steps or '').split('\n') if s.strip()]
            entry = {'source': 'zoom_ai', 'meeting_id': mid, 'when': when,
                     'duration_min': assets['duration']}
            try:
                data = _summarize_transcript(
                    {'topic': assets.get('topic') or (row or {}).get('company'),
                     'kind': 'sales meeting — input is the Zoom AI Companion '
                             'summary of the meeting, not a raw transcript',
                     'when': when, 'duration_min': assets['duration']},
                    text + (f"\n\nNext steps per Zoom:\n{steps}" if steps else ''))
                entry.update(data)
                if not entry.get('next_steps') and steps_list:
                    entry['next_steps'] = steps_list
            except Exception as e:
                print(f'[weekly] zoom_ai structuring failed, shipping raw: {e}')
                entry.update({'summary': text, 'next_steps': steps_list,
                              'challenges': [], 'key_points': []})
        else:
            reasons.append('Zoom AI summary file was empty')

    # rung 2: Claude over the Zoom transcript VTT
    if entry is None and assets and assets['vtt_url']:
        raw = zoom['download'](assets['vtt_url'])
        vtt = raw.get('_text', '') if isinstance(raw, dict) else (raw or '')
        transcript = _vtt_clean(vtt)
        if transcript.strip():
            data = _summarize_transcript(
                {'topic': assets.get('topic') or (row or {}).get('company'),
                 'kind': 'sales meeting', 'when': when,
                 'duration_min': assets['duration']}, transcript)
            entry = {'source': 'zoom_llm', **data, 'meeting_id': mid,
                     'when': when, 'duration_min': assets['duration']}
        else:
            reasons.append('Zoom transcript file was empty')
    elif entry is None and assets and assets['has_recording']:
        reasons.append('recording has no transcript or AI summary files')

    # rung 3: Claude over the Nooks BOOKING-CALL transcript
    if entry is None:
        pids = [lead_id] + ([lead['ConvertedContactId']]
                            if lead and lead.get('ConvertedContactId') else [])
        try:
            best = _pick_booking_call(_nooks_calls_for(pids))
        except Exception as e:
            best = None
            reasons.append(f'Nooks lookup failed: {e}')
        if best:
            turns, _tr = _nooks_turns(best['id'])
            transcript = '\n'.join(
                f"{'Rep' if t.get('speaker') == 'rep' else 'Prospect'}: "
                f"{t.get('text', '')}" for t in turns)
            if transcript.strip():
                data = _summarize_transcript(
                    {'topic': f"booking call — {best.get('account_name') or (row or {}).get('company')}",
                     'kind': 'cold call that booked the meeting',
                     'when': best.get('time'),
                     'duration_min': round((best.get('duration_sec') or 0) / 60)},
                    transcript)
                entry = {'source': 'nooks_llm', **data, 'call_id': best['id'],
                         'when': best.get('time'),
                         'duration_min': round((best.get('duration_sec') or 0) / 60)}
            else:
                reasons.append('Nooks transcript was empty')
        else:
            reasons.append('no Nooks call with a transcript')

    if entry is None:
        return {'source': None, 'reasons': reasons,
                'ver': SUMMARY_CACHE_VER,
                'generated_at': datetime.now(IST).isoformat()}

    entry.update({'ver': SUMMARY_CACHE_VER,
                  'generated_at': datetime.now(IST).isoformat()})
    _save_summary(sf15(lead_id), entry)
    return entry


# ── Flask wiring ─────────────────────────────────────────────────────────────

def init_app(app, soql, norm_sdr, require_admin, data_dir, sf_base_url='',
             zoom=None):
    global CACHE_PATH, SUMMARY_CACHE_PATH
    _deps.update(soql=soql, norm_sdr=norm_sdr, sf_base_url=sf_base_url,
                 zoom=zoom or {})
    CACHE_PATH = os.path.join(data_dir, 'weekly_review_cache.json.gz')
    SUMMARY_CACHE_PATH = os.path.join(data_dir, 'call_summaries.json')
    _load_cache_from_disk()

    from flask import jsonify, request

    @app.route('/api/weekly/review')
    def api_weekly_review():
        _maybe_background_refresh()
        return jsonify(build_review(request.args.get('week', '').strip() or None))

    @app.route('/api/weekly/meetings')
    def api_weekly_meetings():
        _maybe_background_refresh()
        payload = build_meetings(request.args, sf_base=sf_base_url)
        if payload.get('ready'):
            payload['rows'] = [_row_view(r, sf_base_url) for r in payload['rows']]
        return jsonify(payload)

    @app.route('/api/weekly/meeting/<lead_id>')
    def api_weekly_meeting(lead_id):
        """Drawer payload — fast, never calls the LLM. Salesforce panel comes
        from the snapshot row; Nooks calls are fetched live (mirrors the
        cold-calls transcript endpoint: read-only, never cached)."""
        if not _LEAD_ID_RE.match(lead_id):
            return jsonify({'error': 'bad lead id'}), 400
        key = sf15(lead_id)
        row = next((r for r in ((_WR or {}).get('rows') or [])
                    if sf15(r['id']) == key), None)
        if not row:
            return jsonify({'error': 'meeting not found in snapshot'}), 404

        detail = _row_view(row, sf_base_url)
        detail['deal_full'] = row.get('deal')      # full _opp_view incl. AI fields
        detail['deal_rung'] = row.get('deal_rung')

        lead = None
        try:
            lead = _lead_live(row['id'])
        except Exception as e:
            print(f'[weekly] live lead fetch failed for {key}: {e}')
        zoom_url = ((lead or {}).get('Zoom_Meeting_Link_URL__c')
                    or row.get('zoom_url') or '')
        zoom = _deps.get('zoom') or {}
        mid = zoom['extract_id'](zoom_url) if (zoom and zoom_url) else ''

        nooks_calls, nooks_err = [], None
        try:
            pids = [row['id']] + ([lead['ConvertedContactId']]
                                  if lead and lead.get('ConvertedContactId') else [])
            nooks_calls = _nooks_calls_for(pids)
        except Exception as e:
            nooks_err = str(e)

        cached = _load_summaries().get(key)
        if cached and cached.get('ver') != SUMMARY_CACHE_VER:
            cached = None
        return jsonify({'row': detail,
                        'zoom': {'link': bool(zoom_url), 'meeting_id': mid or None},
                        'nooks_calls': nooks_calls, 'nooks_error': nooks_err,
                        'summary': cached})

    @app.route('/api/weekly/meeting/<lead_id>/summary', methods=['POST'])
    def api_weekly_meeting_summary(lead_id):
        """Generate-and-cache. Positives cached forever in call_summaries.json;
        source=None responses are never cached so a late-appearing recording
        gets picked up on the next open. ?force=1 bypasses the cache."""
        if not _LEAD_ID_RE.match(lead_id):
            return jsonify({'error': 'bad lead id'}), 400
        key = sf15(lead_id)
        if request.args.get('force') != '1':
            c = _load_summaries().get(key)
            if c and c.get('ver') == SUMMARY_CACHE_VER:
                return jsonify({**c, 'cached': True})
        row = next((r for r in ((_WR or {}).get('rows') or [])
                    if sf15(r['id']) == key), None)
        if not row:
            return jsonify({'error': 'meeting not found in snapshot'}), 404
        try:
            entry = _generate_summary(row['id'], row)
        except Exception as e:
            print(f'[weekly] summary generation failed for {key}: {e}')
            return jsonify({'source': None, 'error': str(e)}), 502
        return jsonify({**entry, 'cached': False})

    @app.route('/api/weekly/refresh', methods=['POST'])
    @require_admin
    def api_weekly_refresh():
        try:
            snap = refresh_weekly()
            return jsonify({'ok': True,
                            'meetings': len(snap['rows']),
                            'fetched_at': snap['fetched_at']})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 500

    print('[weekly_review] routes registered')
