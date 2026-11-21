"""Semicolon-shared lines: the right statement must own the neutralisation.

`_stmt_text()` slices by LINE, not column, so on `a; b` BOTH statements are
shown the whole line and both get flagged for a call only one of them
contains. Two defects followed.

MINE (regression introduced by binding preservation): `by_span` keeps whichever
flagged statement comes first in walk order, so an unrelated statement could
win the span. Binding preservation read ITS assignment target, so

    x = 1; client.chat_postMessage(channel="#c", text="hello")
    print(x)

silently bound `x` to an empty stub instead of 1 -- the cell RAN and printed
''. Base at least failed loudly with NameError. Trading a loud failure for
silent wrong data is the one direction AGENTS.md forbids outright.

PRE-EXISTING: `preserved` re-emits span members that are NOT targets, so with
every member flagged its `others` list was always empty. The mechanism written
precisely to stop `consume(x)` raising NameError on a shared line had never
fired, on base or head.

Deciding ownership from the statement's OWN column slice fixes both, and the
stub is emitted BEFORE the preserved siblings because a sibling can read the
binding being stubbed (`r = <slack>; ts = r["ts"]`).

SAFETY PROPERTY: no input may end up with zero owners, or the send escapes.
Two earlier predicates failed here and are pinned below -- "has any call" let
`totals = compute()` claim ownership, and a tier-1-text-only test dropped
every candidate for `client.canvases_delete(...)`, which has no tier-1
pattern.
"""
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    neutralize_cell,
    new_bindings,
    update_bindings,
)

CHILD_BINDS_SLACK = """
from slack_sdk import WebClient
client = WebClient(token=TOKEN)
"""

# Slack raises if it is ever reached, so a regression that re-enables a send
# fails the test instead of posting.
PRELUDE = """
class WebClient:
    def __init__(self, token=None):
        pass
    def chat_postMessage(self, **kw):
        raise AssertionError("SLACK SEND EXECUTED")
    def files_upload_v2(self, **kw):
        raise AssertionError("SLACK SEND EXECUTED")

def send_slack(*a, **k):
    raise AssertionError("SLACK SEND EXECUTED")

def compute():
    return 7

TOKEN = "t"
seen = []
def record(*a):
    seen.append(a)
"""


def _transform(src):
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    return neutralize_cell(src, bindings, include_generic=True)


def _run(src):
    result = _transform(src)
    assert not result.parse_error, result.parse_error
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    return result, namespace


# ─── the regression: an unrelated value must survive intact ─────────────────

def test_ordinary_assignment_before_a_send_keeps_its_real_value():
    """The reported shape. `x` must be 1 -- not a stub, and not undefined."""
    src = ('x = 1; client.chat_postMessage(channel="#c", text="hello")\n'
           'record(x)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [(1,)]


def test_the_stub_never_lands_on_an_unrelated_name():
    """Guard the mechanism: nothing Slack-shaped was assigned on that line, so
    no stub belongs in the output at all."""
    src = 'x = 1; client.chat_postMessage(channel="#c", text="hello")\n'
    res = _transform(src)
    assert "_aidp_slack_neutralised" not in res.source
    assert "binding-preserved" not in [k for k, _m, _l in (res.unprovable or [])]


def test_ordinary_assignment_after_a_send_is_preserved():
    """Pre-existing on base: the sibling was simply lost."""
    src = ('client.chat_postMessage(channel="#c", text="x"); x = 1\n'
           'record(x)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [(1,)]


def test_sibling_reading_the_neutralised_binding_resolves_against_the_stub():
    """Needs the stub emitted BEFORE the preserved sibling."""
    src = ('r = client.chat_postMessage(channel="#c", text="x"); ts = r["ts"]\n'
           'record(ts)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("",)]


def test_several_ordinary_statements_on_the_line_all_survive():
    src = ('a = 1; b = 2; client.chat_postMessage(channel="#c", text="x")\n'
           'record(a, b)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [(1, 2)]


def test_computed_value_before_a_wrapper_send_survives():
    """"has any call" was too loose a predicate: it let `totals = compute()`
    claim ownership of the send's evidence."""
    src = 'totals = compute(); send_slack("done")\nrecord(totals)\n'
    _res, ns = _run(src)
    assert ns["seen"] == [(7,)]


# ─── the safety property: never zero owners ─────────────────────────────────

@pytest.mark.parametrize("line", [
    'x = 1; client.chat_postMessage(channel="#c", text="x")',
    'client.chat_postMessage(channel="#c", text="x"); x = 1',
    'r = client.chat_postMessage(channel="#c", text="x"); ts = r["ts"]',
    'x = 1; client.files_upload_v2(file="f")',
    # No tier-1 pattern: a text-only ownership test would drop every candidate
    # here and let the call execute.
    'x = 1; client.canvases_delete(canvas_id="C1")',
    'x = 1; client.assistant_threads_setStatus(status="typing")',
    'totals = compute(); send_slack("done")',
    'x = 1; executor.submit(client.chat_postMessage, channel="c")',
    'client.chat_postMessage(channel="a", text="1"); client.files_upload_v2(file="f")',
])
def test_every_shared_line_still_neutralises_something(line):
    """If ownership filtering ever drops ALL candidates, the send executes."""
    res = _transform(line + "\n")
    assert res.commented, line


def test_two_real_sends_on_one_line_are_both_commented():
    src = ('client.chat_postMessage(channel="a", text="1"); '
           'client.files_upload_v2(file="f")\n')
    res = _transform(src)
    assert res.commented
    # Both own their own evidence, so nothing is re-emitted as live code.
    assert "preserved from a line" not in res.source


def test_raw_webhook_url_sibling_is_still_caught():
    """Tier-1 TEXT evidence has no method to point at; ownership must fall back
    to the statement's own text."""
    src = ('x = 1; requests.post("https://hooks.slack.com/services/A/B/C")\n')
    res = _transform(src)
    assert res.commented
    assert "x = 1" in res.source          # the sibling survives


# ─── unchanged behaviour on ordinary lines ──────────────────────────────────

def test_plain_single_statement_line_is_unaffected():
    src = ('result = client.chat_postMessage(channel="#c", text="x")\n'
           'record(result)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("",)]          # stub still preserves the binding


def test_multiline_statement_fails_closed():
    """When the column slice is unavailable the statement stays a target, so a
    genuine send is never dropped."""
    src = ('client.chat_postMessage(\n'
           '    channel="#c",\n'
           '    text="x",\n'
           ')\n')
    res = _transform(src)
    assert res.commented


# ─── every assignment on the line is re-bound, not just the first ──────────
#
# A span is commented ONCE, so one statement drives the edit -- but each target
# on that span may bind a name readers below depend on:
#
#     a = client.chat_postMessage(...); b = client.chat_postMessage(...)
#     print(a, b)                       -> NameError: name 'b' is not defined
#
# `by_span` collapsed the line to a representative and the emit loop asked only
# that one for its target, so `a` was stubbed and `b` was lost with no finding.
# Not a regression -- base loses both -- but silent, which is the part that
# mattered.

def test_two_assigned_sends_on_one_line_both_bind():
    src = ('a = client.chat_postMessage(channel="1", text="x"); '
           'b = client.chat_postMessage(channel="2", text="y")\n'
           'record(a, b)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("", "")]


def test_three_assigned_sends_on_one_line_all_bind():
    src = ('a = client.chat_postMessage(channel="1", text="x"); '
           'b = client.chat_postMessage(channel="2", text="y"); '
           'c = client.files_upload_v2(file="f")\n'
           'record(a, b, c)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("", "", "")]


def test_assigned_send_followed_by_a_bare_send():
    src = ('a = client.chat_postMessage(channel="1", text="x"); '
           'client.chat_postMessage(channel="2", text="y")\n'
           'record(a)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("",)]


def test_bare_send_followed_by_an_assigned_send():
    """The representative here is the BARE send, which has no target at all, so
    nothing was stubbed and `b` was lost."""
    src = ('client.chat_postMessage(channel="1", text="x"); '
           'b = client.chat_postMessage(channel="2", text="y")\n'
           'record(b)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("",)]


def test_tuple_target_and_single_target_on_one_line():
    src = ('ok, info = client.chat_postMessage(channel="1", text="x"); '
           'b = client.files_upload_v2(file="f")\n'
           'record(ok, info, b)\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("", "", "")]


def test_finding_names_every_preserved_binding_on_the_line():
    src = ('a = client.chat_postMessage(channel="1", text="x"); '
           'b = client.chat_postMessage(channel="2", text="y")\n')
    res = _transform(src)
    msgs = [m for k, m, _l in (res.unprovable or []) if k == "binding-preserved"]
    assert msgs, "no binding-preserved finding"
    joined = " ".join(msgs)
    assert "a" in joined and "b" in joined


def test_unpreservable_target_alongside_a_preservable_one_is_reported():
    """A subscript target cannot be re-bound; it must still be reported even
    when a sibling on the same line IS re-bound."""
    src = ('a = client.chat_postMessage(channel="1", text="x"); '
           'd["k"] = client.files_upload_v2(file="f")\n')
    res = _transform(src)
    kinds = [k for k, _m, _l in (res.unprovable or [])]
    assert "binding-preserved" in kinds
    assert "binding-not-preserved" in kinds


def test_no_send_escapes_on_any_multi_target_line():
    for src in (
        'a = client.chat_postMessage(channel="1", text="x"); '
        'b = client.chat_postMessage(channel="2", text="y")\n',
        'client.chat_postMessage(channel="1", text="x"); '
        'b = client.chat_postMessage(channel="2", text="y")\n',
        'ok, info = client.chat_postMessage(channel="1", text="x"); '
        'b = client.files_upload_v2(file="f")\n',
    ):
        res = _transform(src)
        assert res.commented, src
        # The stub is inert, so executing the cell must not reach the client.
        _run(src)


# ─── re-emission must never put the SENDER back as live code ───────────────
#
# Ownership picks which statement carries the stub, but it cannot by itself
# decide which statement SENDS. When the line's only Slack evidence is tier-1
# TEXT (a raw webhook URL in a string literal), the string assignment owns the
# span and the real sender carries no evidence of its own -- so re-emitting
# non-owners put a live webhook POST back into customer code:
#
#     note = 'see https://hooks.slack.com docs'; requests.post(url, json=...)
#
#     base  -> whole line commented, nothing sent
#     head  -> harmless string commented, POST re-emitted "preserved from..."
#              and the POST DELIVERED
#
# Base suppressed it, so re-emission CREATED this escape rather than
# inheriting it. Two fail-closed gates now cover it, and both are pinned here
# by EXECUTION against a tripwire that raises if a send is reached.

WEBHOOK_PRELUDE = PRELUDE + """
class _Transmitted(Exception):
    pass


class _Requests:
    def post(self, *a, **k):
        raise _Transmitted("requests.post")


requests = _Requests()


def notify(msg):
    raise _Transmitted("notify")


WEBHOOK_CONST = "https://hooks.slack.com/services/T/B/REAL"
"""


def _run_webhook(src, setup=""):
    """Transform + execute with a webhook tripwire. Returns (result, sent)."""
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    if setup:
        update_bindings(setup, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    assert not result.parse_error, result.parse_error
    namespace = {}
    exec(compile(WEBHOOK_PRELUDE, "<prelude>", "exec"), namespace)
    if setup:
        exec(compile(setup, "<setup>", "exec"), namespace)
    try:
        exec(compile(result.source, "<cell>", "exec"), namespace)
    except namespace["_Transmitted"] as exc:
        return result, str(exc)
    return result, None


def test_sibling_post_is_not_re_emitted_when_evidence_is_text_only():
    """The reported shape: the URL lives in the OTHER statement's literal."""
    _res, sent = _run_webhook(
        "note = 'see https://hooks.slack.com docs'; "
        "requests.post(WEBHOOK_CONST, json={'text': 'done'})\n")
    assert sent is None, "re-emitted sibling delivered via %s" % sent


def test_webhook_var_and_post_on_one_line_does_not_transmit():
    _res, sent = _run_webhook(
        "webhook = 'https://hooks.slack.com/services/T/B/REAL'; "
        "requests.post(webhook)\n")
    assert sent is None, "re-emitted sibling delivered via %s" % sent


def test_url_in_message_string_with_wrapper_call_does_not_transmit():
    _res, sent = _run_webhook(
        "msg = 'ping https://hooks.slack.com/services/T/B/REAL'; "
        "notify(msg)\n")
    assert sent is None, "re-emitted sibling delivered via %s" % sent


def test_text_only_span_reports_why_the_sibling_was_not_re_emitted():
    res, _sent = _run_webhook(
        "note = 'see https://hooks.slack.com docs'; "
        "requests.post(WEBHOOK_CONST)\n")
    kinds = [k for k, _m, _l in (res.unprovable or [])]
    assert "shared-line-call-not-preserved" in kinds
    msg = " ".join(m for k, m, _l in res.unprovable
                   if k == "shared-line-call-not-preserved")
    assert "TEXT" in msg, msg


def test_send_shaped_sibling_is_not_re_emitted_even_with_a_proven_owner():
    """The narrower gate. The owner's call IS proven here
    (`client.chat_postMessage`), yet the sibling posts to a webhook held in a
    plain name, so it carries no evidence of its own. Base commented the whole
    line, so re-emitting it would be a regression."""
    _res, sent = _run_webhook(
        "requests.post(WEBHOOK_CONST); "
        'client.chat_postMessage(channel="#c", text="x")\n')
    assert sent is None, "re-emitted sibling delivered via %s" % sent


def test_harmless_computed_sibling_is_still_re_emitted():
    """The cost check: fail-closed must NOT swallow the case the mechanism
    exists for. `compute()` is a call, but not a send-shaped one."""
    res, sent = _run_webhook('totals = compute(); send_slack("done")\n'
                             "record(totals)\n")
    assert sent is None
    assert "preserved from" in res.source
    kinds = [k for k, _m, _l in (res.unprovable or [])]
    assert "shared-line-call-not-preserved" not in kinds


# ─── a sibling that ran BEFORE the send must not observe the stub ──────────
#
#     old = r; r = client.chat_postMessage(...)
#
# The stub was emitted first and every preserved sibling after it, so `old`
# silently became the empty stub where base raised NameError -- a loud failure
# replaced by a wrong value that then flowed downstream. Preserved siblings
# are now split by position relative to the send.

def test_pre_send_reader_sees_the_real_prior_value_not_the_stub():
    src = ('old = r; r = client.chat_postMessage(channel="#c", text="x")\n'
           "record(old)\n")
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile('r = "REAL_PRIOR_VALUE"\n', "<earlier>", "exec"), namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    assert namespace["seen"] == [("REAL_PRIOR_VALUE",)], namespace["seen"]


def test_pre_send_reader_is_emitted_above_the_rebinding():
    """Pins the ORDER, not just the value, so the fix cannot regress into
    'happens to work because the name was already bound'.

    Anchored on the RE-BINDING of `r`, not on the stub temp's definition. The
    temp (`_aidp_slack_neutralised = (lambda ...)`) is an inert definition
    under a reserved name, so emitting it ahead of everything is
    order-independent and carries no contract. The contract is that a sibling
    which ran BEFORE the send reads the real prior value, i.e. it precedes
    `r = _aidp_slack_neutralised`. An earlier version of this test anchored on
    the temp and so would have failed the move to column-ordered emission for
    a reason unrelated to what it was guarding.
    """
    src = 'old = r; r = client.chat_postMessage(channel="#c", text="x")\n'
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    out = neutralize_cell(src, bindings, include_generic=True).source
    body = [ln for ln in out.splitlines() if not ln.lstrip().startswith("#")]
    read_at = next(i for i, ln in enumerate(body) if "old = r" in ln)
    rebind_at = next(i for i, ln in enumerate(body)
                     if ln.lstrip().startswith("r = _aidp_slack_neutralised"))
    assert read_at < rebind_at, body


def test_post_send_reader_still_sees_the_stub():
    """The opposite direction must keep working: a sibling AFTER the send
    reads the stub, not an undefined name."""
    src = ('r = client.chat_postMessage(channel="#c", text="x"); ts = r["ts"]\n'
           "record(ts)\n")
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    assert namespace["seen"] == [("",)], namespace["seen"]


# ─── a reader BETWEEN two sends, and column-ordered emission ───────────────
#
#     a = <send1>; old = b; b = <send2>
#
# Left to right that is: `a` gets reply 1; `old` gets whatever `b` held BEFORE
# this line; then `b` is overwritten with reply 2. So `old` must be the real
# prior `b`.
#
# The first version of the pre/post fix split siblings at the LEFTMOST send,
# which has no answer here: with two sends there are two boundaries, and a
# sibling between them was emitted after BOTH stubs, so `old` silently became
# '' where base raised NameError. Emission is now ordered by COLUMN, which
# needs no boundary and is correct for any number of sends.

def test_reader_between_two_sends_sees_the_real_prior_value():
    src = ('a = client.chat_postMessage(channel="#c", text="1"); '
           'old = b; b = client.files_upload_v2(file="f")\n'
           "record(a, old, b)\n")
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile('b = "ORIGINAL_B"\n', "<earlier>", "exec"), namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    assert namespace["seen"] == [("", "ORIGINAL_B", "")], namespace["seen"]


def test_reader_of_the_earlier_send_still_sees_that_stub():
    """The mirror, which the binary split got right by accident: `mid = a`
    genuinely does run after send 1, so it must read send 1's stub."""
    src = ('a = client.chat_postMessage(channel="#c", text="1"); '
           'mid = a; b = client.files_upload_v2(file="f")\n'
           "record(mid)\n")
    _res, ns = _run(src)
    assert ns["seen"] == [("",)]


def test_emission_follows_the_original_column_order():
    """Pins the mechanism, not just the value: each stub assignment lands at
    the position of the send it replaces, with siblings interleaved."""
    src = ('a = client.chat_postMessage(channel="#c", text="1"); '
           'old = b; b = client.files_upload_v2(file="f")\n')
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    out = neutralize_cell(src, bindings, include_generic=True).source
    body = [ln.lstrip() for ln in out.splitlines()
            if not ln.lstrip().startswith("#")]
    a_at = next(i for i, ln in enumerate(body)
                if ln.startswith("a = _aidp_slack_neutralised"))
    old_at = next(i for i, ln in enumerate(body) if ln.startswith("old = b"))
    b_at = next(i for i, ln in enumerate(body)
                if ln.startswith("b = _aidp_slack_neutralised"))
    assert a_at < old_at < b_at, body


def test_three_sends_with_readers_between_each():
    """More sends than boundaries any split could have had.

    Each reader reads the name a LATER send overwrites, so it can only get the
    right answer if emission is ordered rather than split: `p = b` must see
    the real prior `b` even though `b` is stubbed further right, and likewise
    `q = c`. A reader of an unrelated name would pass under the old
    post-emission too and would pin nothing.
    """
    src = ('a = client.chat_postMessage(channel="#c", text="1"); '
           'p = b; b = client.files_upload_v2(file="f"); '
           'q = c; c = client.chat_postMessage(channel="#c", text="3")\n'
           "record(p, q, a, b, c)\n")
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile('b = "PRIOR_B"\nc = "PRIOR_C"\n', "<earlier>", "exec"),
         namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    assert namespace["seen"] == [("PRIOR_B", "PRIOR_C", "", "", "")], (
        namespace["seen"])


# ─── a MULTI-LINE statement sharing its last line with a send ──────────────
#
#     x = len(
#         "abc"); client.chat_postMessage(...)
#
# Ownership fails CLOSED when a statement's own column slice is unavailable,
# which is right for deciding whether to COMMENT it (never drop a genuine
# send) but is not proof that its value was a Slack response. `x` was stubbed
# to '' where it should be 3, and base raised NameError.

def test_multiline_non_send_sharing_a_line_is_not_stubbed():
    src = ('x = len(\n    "abc"); '
           'client.chat_postMessage(channel="#c", text="1")\n'
           "record(x)\n")
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    assert "_aidp_slack_neutralised" not in result.source, result.source
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    with pytest.raises(NameError):
        exec(compile(result.source, "<cell>", "exec"), namespace)


def test_multiline_non_send_reports_that_it_is_not_the_sender():
    src = ('x = len(\n    "abc"); '
           'client.chat_postMessage(channel="#c", text="1")\n')
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    reasons = [m for k, m, _l in (result.unprovable or [])
               if k == "binding-not-preserved"]
    assert reasons, result.unprovable
    assert "does not contain the call it was flagged for" in " ".join(reasons)


def test_genuine_multiline_send_keeps_its_stub():
    """The control that makes the fix narrow: a send whose own slice is also
    unavailable DOES contain the flagged call, so it is still stubbed."""
    src = ('r = client.chat_postMessage(\n    channel="#c", text="x")\n'
           'record(r["ts"])\n')
    _res, ns = _run(src)
    assert ns["seen"] == [("",)]


def test_single_line_webhook_post_still_keeps_its_stub():
    """Scoping check. The not-the-sender rule applies ONLY to the fail-closed
    multi-line branch; applying it to every target dropped the stub here,
    because this statement is flagged from the URL in its own arguments rather
    than from a method name."""
    src = ('resp = requests.post("https://hooks.slack.com/services/T/B/X", '
           'json={"text": "x"})\n')
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    out = neutralize_cell(src, bindings, include_generic=True).source
    assert "_aidp_slack_neutralised" in out, out


# ─── a preserved sibling must stay inside its own block ────────────────────
#
# A review round reported that in
#
#     def notify():
#         rows.append("called"); client.chat_postMessage(...)
#
# the append was hoisted and ran at DEFINITION time. It does not: the sibling
# is re-emitted at the statement's own indentation, i.e. inside the function
# body. Pinned anyway, because hoisting customer code out of a function would
# be a silent semantic change and nothing else guards the indentation of a
# re-emitted sibling in a compound body.

def test_sibling_in_a_one_line_def_body_stays_inside_the_function():
    src = ('rows = []\n'
           'def notify():\n'
           '    rows.append("called"); '
           'client.chat_postMessage(channel="#c", text="x")\n')
    out = _transform(src).source
    kept = [ln for ln in out.splitlines()
            if "preserved from a line" in ln and "rows.append" in ln]
    assert kept, out
    assert all(ln.startswith("    ") for ln in kept), kept


def test_a_one_line_def_body_does_not_run_at_definition_time():
    src = ('rows = []\n'
           'def notify():\n'
           '    rows.append("called"); '
           'client.chat_postMessage(channel="#c", text="x")\n'
           'record(list(rows))\n'
           'notify()\n'
           'record(list(rows))\n')
    _res, ns = _run(src)
    # Empty at definition time, appended only once notify() is called.
    assert ns["seen"] == [([],), (["called"],)], ns["seen"]


def test_sibling_in_a_one_line_if_body_stays_inside_the_block():
    src = ('rows = []\n'
           'flag = True\n'
           'if flag:\n'
           '    rows.append("x"); '
           'client.chat_postMessage(channel="#c", text="y")\n'
           'record(list(rows))\n')
    _res, ns = _run(src)
    assert ns["seen"] == [(["x"],)], ns["seen"]
