"""Shared metric definitions for the DG Campaign Dashboard.

Single source of truth for every SOQL fragment and rule that turns Salesforce
records into dashboard numbers. app.py imports these (no behaviour change: the
strings are byte-identical to the literals that used to live inline) and
history.py stamps DEFINITION_VERSION onto every metric snapshot it writes, so a
stored number can always be traced to the exact rule that produced it.

Bump DEFINITION_VERSION whenever ANY value below changes, and add a line to
CHANGELOG. tests/test_definitions.py fails if the hash changes without a bump.
"""

DEFINITION_VERSION = 3

CHANGELOG = {
    1: '2026-09-18 transcribed from app.py @ e482e3d (backfill)',
    2: '2026-09-18 same rules, now shared via definitions.py; history writer live',
    3: '2026-09-19 v2 + touches classification/attribution (P1 part 1); dashboard rules unchanged',
}

# ── Activity (Task) filters ──────────────────────────────────────────────────
CALL_SUBJ  = "Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%'"
EMAIL_SUBJ = "Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%'"

# Nooks dispositions whose Outcome is "Connect" or "Meeting" in Nooks's
# disposition→outcome mapping (logged on the Task CallDisposition field).
CONNECT_DISPOSITIONS = (
    "'Answered - Booked Meeting','Answered - Follow Up Required',"
    "'Answered - No Longer with Company','Answered - Wrong Person, Gave Referral',"
    "'Answered - Wrong Person, No Referral','Busy - Call Later','Connected','DNC',"
    "'Meeting','Meeting Generated- Cold','Meeting Generated- Conference',"
    "'Not Interested','Objection: Already Have Solution','Objection: Asked to Send Info',"
    "'Objection: Not A Priority','Prospect Disconnected','Retired','Strong Follow up',"
    "'Wrong Number'"
)
CONNECT_DISPOSITION_SET = {s.strip().strip("'") for s in CONNECT_DISPOSITIONS.split("','")}

# A connect counts as a "conversation" when the call lasted at least this long
# (mirrors Nooks's default Conversation Threshold of 60 seconds).
CONVERSATION_THRESHOLD_SECS = 60

# A meeting is "generated" the moment an SDR logs a booking disposition on a
# [Nooks Call] task (counted even before the Lead's meeting fields are filled).
BOOKING_DISPOSITIONS = ("'Answered - Booked Meeting','Meeting Generated- Cold',"
                        "'Meeting Generated- Conference'")
NOOKS_BOOKING_MIN_DATE = '2026-04-15'    # ActivityDate lower bound for Nooks-booked meetings

# LinkedIn (HeyReach) stage tasks. Stages are CUMULATIVE downstream ⇒ upstream.
LI_SENT_SUBJ  = "Subject LIKE '%CONNECTION_REQUEST_SENT%'"
LI_ACC_SUBJ   = "Subject LIKE '%CONNECTION_REQUEST_ACCEPTED%'"
LI_MSG_SUBJ   = "Subject LIKE '%HeyReach - MESSAGE_SENT%'"
LI_REPLY_SUBJ = "Subject LIKE '%HeyReach - MESSAGE_REPLY_RECEIVED%'"

# ── Lead meeting-status rules ────────────────────────────────────────────────
# "Meeting Done" = ANY completed-meeting status (incl. Meeting Done-SQL) OR the
# lead reached S1 (conversion freezes Meeting_Status__c, so S1 implies held).
DONE_CLAUSE   = "(Meeting_Status__c LIKE 'Meeting Done%' OR Status = 'S1 Converted')"
NOSHOW_CLAUSE = "Meeting_Status__c = 'Meeting No Show'"
SQL_CLAUSE    = "Status = 'SQL'"

def is_done(meeting_status, lead_status):
    return (meeting_status or '').startswith('Meeting Done') or lead_status == 'S1 Converted'

def is_noshow(meeting_status):
    return (meeting_status or '') == 'Meeting No Show'

def is_sql(meeting_status, lead_status):
    """History-layer SQL flag: Meeting Done-SQL, or Status SQL / S1 Converted.
    (The campaign card's sql_gen counts Status = 'SQL' only — see SQL_CLAUSE.)"""
    return (meeting_status or '') == 'Meeting Done-SQL' or lead_status in ('SQL', 'S1 Converted')

# ── Touch classification (P1 history layer; read-only view of the same Task rows) ──
# Two independent facts per Salesforce Task:
#   dashboard_cards(subject) — which dashboard card(s) COUNT this row: a literal mirror of the
#       card SOQL above (CALL_SUBJ / EMAIL_SUBJ / LI_*), case-insensitive like SOQL LIKE. This is
#       what lets a history query reproduce every card number exactly. A row can hit two cards.
#   classify_touch(subject, …) — what the row actually IS: (channel, tool, event). Independent of
#       the cards, so e.g. a call logged through Outreach is a call here even though the email
#       card counts it, and a HeyReach step whose campaign name contains "Outreach" is LinkedIn.
# Returns None for internal escalation reminders, which are not outreach.
TOUCH_ESCALATION_PREFIX = '[RC] ESCALATION'
_LI_CARD_TOKENS = ('CONNECTION_REQUEST_SENT', 'CONNECTION_REQUEST_ACCEPTED', 'HEYREACH - MESSAGE_SENT', 'HEYREACH - MESSAGE_REPLY_RECEIVED')

def dashboard_cards(subject):
    u = (subject or '').upper(); out = []
    if 'ORUM' in u or u.startswith('[NOOKS CALL]'): out.append('call')          # Subject LIKE '%Orum%' OR Subject LIKE '[Nooks Call]%'
    if 'SMARTLEAD' in u or 'OUTREACH' in u: out.append('email')               # Subject LIKE '%Smartlead%' OR Subject LIKE '%Outreach%'
    if any(t in u for t in _LI_CARD_TOKENS): out.append('linkedin')           # LI_SENT/ACC/MSG/REPLY_SUBJ
    return out

def classify_touch(subject, task_subtype=None, task_type=None):
    s = subject or ''; u = s.upper()
    if s.startswith(TOUCH_ESCALATION_PREFIX): return None
    if '[Nooks Call]' in s:                          return 'call', 'nooks', 'call'
    if '[Orum]' in s or 'Orum' in s:                 return 'call', 'orum', 'call'
    if 'HeyReach' in s or 'CONNECTION_REQUEST_' in s:   # before Smartlead/Outreach: step names may contain those words
        if 'MESSAGE_REPLY_RECEIVED' in s:            return 'linkedin', 'heyreach', 'li_reply'
        if 'INMAIL_REPLY_RECEIVED' in s:             return 'linkedin', 'heyreach', 'li_reply'
        if 'MESSAGE_SENT' in s:                      return 'linkedin', 'heyreach', 'li_message_sent'
        if 'CONNECTION_REQUEST_ACCEPTED' in s:       return 'linkedin', 'heyreach', 'li_request_accepted'
        if 'CONNECTION_REQUEST_SENT' in s:           return 'linkedin', 'heyreach', 'li_request_sent'
        return 'linkedin', 'heyreach', 'other'
    if 'Instantly' in s:
        return 'email', 'instantly', ('email_reply' if 'REPLY' in u else 'email_opened' if 'OPEN' in u else 'email_sent')
    if 'Smartlead' in s:
        return 'email', 'smartlead', ('email_reply' if 'REPL' in u else 'email_opened' if 'OPEN' in u else 'email_sent')
    if 'Outreach' in s:
        if '[CALL]' in u:                            return 'call', 'outreach', 'call'
        if '[OTHER]' in u:                           return 'other', 'outreach', 'other'
        if '[IN]' in u:                              return 'email', 'outreach', ('email_autoreply' if 'AUTOMATIC REPLY' in u else 'email_reply')
        if '[OUT]' in u or '[DELIVERED]' in u:       return 'email', 'outreach', 'email_sent'
        if '[OPENED]' in u:                          return 'email', 'outreach', 'email_opened'
        if '[CLICKED]' in u:                         return 'email', 'outreach', 'email_clicked'
        if '[BOUNCED]' in u:                         return 'email', 'outreach', 'email_bounced'
        if 'REPL' in u:                              return 'email', 'outreach', 'email_reply'
        if 'SENT' in u:                              return 'email', 'outreach', 'email_sent'
        return 'email', 'outreach', 'email_other'
    if s.startswith('Email:') or task_subtype == 'Email' or task_type == 'Email':
        return 'email', 'manual_email', ('email_reply' if s.lower().startswith('email: re:') else 'email_other')
    if 'Outplay' in s:                               return 'other', 'outplay', 'other'
    if task_subtype == 'Call' or task_type == 'Call': return 'call', 'unknown', 'call'
    return 'other', 'unknown', 'other'

BOOKING_DISPOSITION_SET = {s.strip().strip("'") for s in BOOKING_DISPOSITIONS.split("','")}

def touch_flags(channel, counted, subject, disposition, duration_s):
    """(is_connect, is_conversation, is_booking) for a classified Task — same rule as the cards:
    connects/conversations are Nooks calls with a connect disposition; bookings any counted call.
    `counted` = the row is counted by some dashboard card (bool(dashboard_cards(subject)))."""
    is_call = channel == 'call' and counted
    is_connect = bool(is_call and disposition in CONNECT_DISPOSITION_SET and '[Nooks Call]' in (subject or ''))
    is_conv = bool(is_connect and (duration_s or 0) >= CONVERSATION_THRESHOLD_SECS)
    is_booking = bool(is_call and disposition in BOOKING_DISPOSITION_SET)
    return is_connect, is_conv, is_booking

# ── Storable description (written to metric_definitions on version bump) ─────
def as_dict():
    return {
        'version': DEFINITION_VERSION,
        'membership': "Lead.Campaign__c = <campaign name>; union into frozen ledger; pruned only while Active when the lead now carries a different non-blank Campaign__c",
        'window': "campaign start_date..end_date; Tasks by ActivityDate, Lead meeting fields by Meeting_Generated_on__c",
        'total_leads': "count of frozen ledger lead_ids",
        'total_calls': f"COUNT Task WHERE ({CALL_SUBJ}) AND WhoId IN frozen_ids AND window",
        'total_connects': f"COUNT Task WHERE Subject LIKE '[Nooks Call]%' AND CallDisposition IN ({CONNECT_DISPOSITIONS}) AND WhoId IN frozen_ids AND window",
        'total_conversations': f"connects AND CallDurationInSeconds >= {CONVERSATION_THRESHOLD_SECS}",
        'total_emails': f"COUNT Task WHERE ({EMAIL_SUBJ}) AND WhoId IN frozen_ids AND window",
        'unique_leads_called': "COUNT DISTINCT WhoId over the calls filter",
        'unique_leads_emailed': "COUNT DISTINCT WhoId over the emails filter",
        'li_sent': f"DISTINCT WhoId over CURRENT members, all-time, ({LI_SENT_SUBJ}) OR any downstream stage",
        'li_accepted': f"DISTINCT WhoId, ({LI_ACC_SUBJ}) OR downstream",
        'li_msg_sent': f"DISTINCT WhoId, ({LI_MSG_SUBJ}) OR reply",
        'li_msg_reply': f"DISTINCT WhoId, {LI_REPLY_SUBJ}",
        'meetings': f"ledger meetings dated in window: Lead.Meeting_Generated_on__c != null UNION Nooks booking Tasks (CallDisposition IN ({BOOKING_DISPOSITIONS}), ActivityDate >= {NOOKS_BOOKING_MIN_DATE}); once attributed never moved",
        'meeting_done': f"COUNT Lead IN frozen_ids WHERE {DONE_CLAUSE} AND Meeting_Generated_on__c in window",
        'meeting_noshow': f"COUNT Lead IN frozen_ids WHERE {NOSHOW_CLAUSE} AND Meeting_Generated_on__c in window",
        'sql_gen': f"COUNT Lead IN frozen_ids WHERE {SQL_CLAUSE} (no date filter)",
        's1_created': "COUNT Opportunity WHERE Id IN (SELECT ConvertedOpportunityId FROM Lead WHERE Campaign__c = <name> AND IsConverted = true), unless manual_s1",
        'history_is_done': "meeting_status startswith 'Meeting Done' OR lead_status = 'S1 Converted'",
        'history_is_sql': "meeting_status = 'Meeting Done-SQL' OR lead_status in ('SQL','S1 Converted')",
        'settled_date': "max(Meeting_Scheduled_on__c, end_date) + 14 days; Completed campaigns past it are frozen",
        # v3 — touches (history layer only; changes no dashboard number)
        'touches_source': f"Salesforce Task rows with a WhoId; excluded: Subject LIKE '{TOUCH_ESCALATION_PREFIX}%' (internal reminders, no WhoId) and Not_Relevant__c = true",
        'touch_channel_tool': "what the Task IS, parsed from Subject in this order: [Nooks Call]->call/nooks · Orum->call/orum · HeyReach/CONNECTION_REQUEST_*->linkedin/heyreach (li_request_sent/accepted, li_message_sent, li_reply incl. INMAIL) · Instantly->email/instantly (sent/opened/reply) · Smartlead->email/smartlead (sent/opened/reply) · Outreach: [Call]->call/outreach, [Other]->other/outreach, [In]->email_reply (or email_autoreply), [Out]/[Delivered]/SENT->email_sent, [Opened], [Clicked], [Bounced] · Subject 'Email:' or TaskSubtype=Email->email/manual_email · Outplay->other/outplay · TaskSubtype=Call->call/unknown · else other/unknown",
        'dashboard_cards': "which card(s) COUNT the row — literal, case-insensitive mirror of the card SOQL: call = Subject LIKE '%Orum%' OR '[Nooks Call]%'; email = Subject LIKE '%Smartlead%' OR '%Outreach%'; linkedin = CONNECTION_REQUEST_SENT/ACCEPTED or 'HeyReach - MESSAGE_SENT' / 'HeyReach - MESSAGE_REPLY_RECEIVED'. A row may hit two cards (the dashboard counts it twice). is_counted_by_dashboard = cards non-empty AND ActivityDate not null",
        'is_connect': "Nooks/Orum call AND CallDisposition IN connect set (19 values, see total_connects)",
        'is_conversation': f"is_connect AND CallDurationInSeconds >= {CONVERSATION_THRESHOLD_SECS}",
        'is_booking': f"CallDisposition IN ({BOOKING_DISPOSITIONS})",
        'touch_attribution': "primary = eligible membership (ledger/sync/discovery) whose campaign window [start_date, end_date] (removed_at narrows only while Active) contains occurred_on; several -> the lead's current stamp, then latest start_date; LeadHistory-only memberships and one nearest membership ending <=14 days before the touch are kept as NON-primary context; re-evaluation supersedes, never rewrites (SQL: attribute_touches)",
        'touch_ingest': "each sync: Task WHERE CreatedDate > watermark - 2h ORDER BY CreatedDate, capped per run; watermark = max CreatedDate ingested (touch_ingest_state); rollup campaign_lead_activity refreshed per touched campaign (SQL: refresh_campaign_lead_activity)",
        'meeting_sentiment': "positive = Meeting Done-SQL / Meeting Done-Qualified/Follow Up; neutral = Meeting Done-Nurture; negative = Meeting Done-Unqualified / Meeting Done- Not Interested (from Rahul's gtm_meeting_sentiment)",
        'changelog': CHANGELOG,
    }

def definitions_hash():
    import hashlib, json
    return hashlib.sha256(json.dumps(as_dict(), sort_keys=True).encode()).hexdigest()[:16]
