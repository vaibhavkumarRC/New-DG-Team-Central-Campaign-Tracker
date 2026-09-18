"""Shared metric definitions for the DG Campaign Dashboard.

Single source of truth for every SOQL fragment and rule that turns Salesforce
records into dashboard numbers. app.py imports these (no behaviour change: the
strings are byte-identical to the literals that used to live inline) and
history.py stamps DEFINITION_VERSION onto every metric snapshot it writes, so a
stored number can always be traced to the exact rule that produced it.

Bump DEFINITION_VERSION whenever ANY value below changes, and add a line to
CHANGELOG. tests/test_definitions.py fails if the hash changes without a bump.
"""

DEFINITION_VERSION = 2

CHANGELOG = {
    1: '2026-09-18 transcribed from app.py @ e482e3d (backfill)',
    2: '2026-09-18 same rules, now shared via definitions.py; history writer live',
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
        'changelog': CHANGELOG,
    }

def definitions_hash():
    import hashlib, json
    return hashlib.sha256(json.dumps(as_dict(), sort_keys=True).encode()).hexdigest()[:16]
