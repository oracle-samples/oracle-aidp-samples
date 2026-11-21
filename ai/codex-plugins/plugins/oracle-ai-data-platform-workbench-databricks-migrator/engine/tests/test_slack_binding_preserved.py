"""Neutralising an ASSIGNED Slack call must not orphan its readers.

INCIDENT (command aaaaaaaa-0000-4000-8000-000000000109): a notebook failed with
`NameError: name 'result' is not defined`. `result = client.chat_postMessage(...)`
had been commented out by the guard -- correctly, it is a real Slack send --
while `logger.info(result)` below it stayed live.

This is NOT a classification defect. The guard's decision is right; the EDIT is
incomplete. It removes a statement while leaving the data dependency live, so a
perfect classifier would not fix it. Distinct from the namespace-gate fix in
test_slack_namespace_gate.py, which is about deciding WHETHER to neutralise.

The same hazard was already solved one line over: a semicolon-shared
`x = compute(); client.files_upload_v2(...)` re-emits `x = compute()` above the
comment. This applies that to the assignment's own target.

Why a self-similar null object and not None/{}: a Slack response is consumed as
`r["ts"]`, `r.get("ok")`, `r.data` and nested `r["file"]["id"]`. None raises
TypeError and {} raises KeyError on the nested form -- moving the crash instead
of removing it. Tested here by EXECUTING the transformed cell, because source
that merely parses can still be broken.

The stub is deliberately falsy: a branch keyed on the send succeeding must take
the NOT-SENT path, since the message genuinely was not sent.
"""
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    BINDING_ANNOTATION,
    neutralize_cell,
    new_bindings,
)

# Slack is stubbed so a regression cannot send anything; calling it is a
# test failure, not a network event.
PRELUDE = """
class WebClient:
    def __init__(self, token=None):
        pass
    def chat_postMessage(self, **kw):
        raise AssertionError("SLACK SEND EXECUTED")
    def files_upload_v2(self, **kw):
        raise AssertionError("SLACK SEND EXECUTED")
    def conversations_history(self, **kw):
        return {"messages": [{"ts": "1.0"}]}

class _Log:
    def info(self, *a, **k):
        _sink.append(a)

logger = _Log()
tok = "t"

def send_slack(payload=None):
    raise AssertionError("SLACK SEND EXECUTED")

def mark_sent():
    _sink.append("mark_sent")
"""


def _neutralise(src):
    return neutralize_cell(src, new_bindings(), include_generic=True)


def _run(src):
    """Neutralise then execute. Returns (result, namespace)."""
    res = _neutralise(src)
    assert not res.parse_error, res.parse_error
    namespace = {"_sink": []}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile(res.source, "<cell>", "exec"), namespace)
    return res, namespace


def _kinds(res):
    return {kind for kind, _msg, _line in (res.unprovable or [])}


# ─── the incident, and every consumption shape, EXECUTED ────────────────────

@pytest.mark.parametrize("label,src", [
    ("logger consumes the result", '''
client = WebClient(token=tok)
result = client.chat_postMessage(channel="#c", text="done")
logger.info(result)
'''),
    ("subscripted response", '''
client = WebClient(token=tok)
resp = client.chat_postMessage(channel="#c", text="done")
ts = resp["ts"]
logger.info("ts=" + ts)
'''),
    ("nested subscript", '''
client = WebClient(token=tok)
up = client.files_upload_v2(file="/tmp/a.png", channel="C1")
logger.info(up["file"]["id"])
'''),
    ("attribute access", '''
client = WebClient(token=tok)
r = client.chat_postMessage(channel="#c", text="done")
logger.info(r.data)
'''),
    ("f-string consumption", '''
client = WebClient(token=tok)
r = client.chat_postMessage(channel="#c", text="done")
logger.info(f"ts={r['ts']} ok={r.get('ok')}")
'''),
    ("tuple unpacking", '''
ok, info = send_slack(payload={})
logger.info((ok, info))
'''),
])
def test_consumption_shapes_execute_without_nameerror(label, src):
    res, _ns = _run(src)
    assert res.commented, f"{label}: the send was not neutralised"


def test_the_send_itself_never_executes():
    """The whole point: the binding survives, the call does not."""
    src = ("client = WebClient(token=tok)\n"
           "result = client.chat_postMessage(channel='#c', text='x')\n"
           "logger.info(result)\n")
    _res, ns = _run(src)
    assert ns["_sink"] == [("",)]          # logged the inert stub, not a response


def test_branch_keyed_on_success_takes_the_SENT_path():
    """Inverted deliberately (owner decision 2026-10-06).

    This test previously pinned the opposite: the stub was falsy, so
    `if r.get('ok'):` took the else branch. That diverted the notebook onto a
    path production never executes -- and `assert r['ok']` /
    `resp.status_code >= 400` raised outright -- so conversions could not
    complete and the realistic downstream path was never exercised.

    The stand-in now reports success so the notebook follows its normal path.
    The cost (a cell can PASS without its notification path having run) is
    disclosed in the binding-preserved finding, which
    test_finding_discloses_that_success_is_fabricated pins.
    """
    src = ("client = WebClient(token=tok)\n"
           "r = client.chat_postMessage(channel='#c', text='x')\n"
           "if r.get('ok'):\n"
           "    mark_sent()\n")
    _res, ns = _run(src)
    assert "mark_sent" in ns["_sink"]


def test_tuple_unpacking_binds_every_name_with_matching_arity():
    src = "a, b, c = send_slack(payload={})\nlogger.info((a, b, c))\n"
    _res, ns = _run(src)
    assert ns["_sink"] == [(("", "", ""),)]


# ─── emitted form ───────────────────────────────────────────────────────────

def test_binding_line_is_annotated_and_original_is_preserved_as_a_comment():
    src = ("client = WebClient(token=tok)\n"
           "result = client.chat_postMessage(channel='#c', text='x')\n")
    res = _neutralise(src)
    assert BINDING_ANNOTATION.strip() in res.source
    # Customer code is preserved, never deleted.
    assert "# result = client.chat_postMessage" in res.source


def test_indentation_is_preserved_inside_a_block():
    src = ("client = WebClient(token=tok)\n"
           "if flag:\n"
           "    result = client.chat_postMessage(channel='#c', text='x')\n"
           "    logger.info(result)\n")
    res = _neutralise(src)
    assert not res.parse_error
    # Matches the annotation's current wording. The old phrase ("binding
    # preserved so consumers below still run") was removed because it was
    # false for two consumer shapes and because the stand-in now reports
    # success rather than merely preserving a binding.
    bind_lines = [ln for ln in res.source.splitlines()
                  if "NO message was delivered" in ln
                  and "= _aidp_slack" in ln]
    assert bind_lines
    assert all(ln.startswith("    ") for ln in bind_lines), bind_lines


def test_block_left_with_only_a_neutralised_assignment_stays_valid():
    """The stub is executable code, so no `pass` filler is needed — and the
    cell must still parse."""
    src = ("client = WebClient(token=tok)\n"
           "if flag:\n"
           "    result = client.chat_postMessage(channel='#c', text='x')\n")
    res = _neutralise(src)
    assert not res.parse_error
    compile(res.source, "<cell>", "exec")


def test_finding_reports_the_preserved_binding_and_its_consumer():
    src = ("client = WebClient(token=tok)\n"
           "result = client.chat_postMessage(channel='#c', text='x')\n"
           "logger.info(result)\n")
    res = _neutralise(src)
    assert "binding-preserved" in _kinds(res)
    msg = next(m for k, m, _l in res.unprovable if k == "binding-preserved")
    assert "result" in msg
    assert "REPORTS SUCCESS" in msg   # the semantic change is disclosed


def test_finding_discloses_that_success_is_fabricated():
    """The condition that makes a success-reporting stand-in acceptable.

    Reporting success lets a cell go green without its notification path
    having been exercised. That is only tolerable if the report says so, so
    the finding must state: no message was delivered, the success is
    fabricated, the success path's side effects ran on a send that never
    happened, and how to actually validate that path. Without this the stub
    would be a silent false PASS.
    """
    src = ("client = WebClient(token=tok)\n"
           "result = client.chat_postMessage(channel='#c', text='x')\n"
           "logger.info(result)\n")
    msg = next(m for k, m, _l in _neutralise(src).unprovable
               if k == "binding-preserved")
    assert "NO message was delivered" in msg
    assert "REPORTS SUCCESS" in msg
    assert "CAN PASS WITHOUT ITS NOTIFICATION PATH" in msg
    assert "throwaway workspace or test channel" in msg
    # The side effects an operator has to go looking for.
    for consequence in ("marking records notified", "writing an audit row",
                        "dropping a queue entry"):
        assert consequence in msg, consequence


# ─── targets that must NOT be re-bound ──────────────────────────────────────

def test_augmented_assignment_is_reported_not_stubbed():
    """`total += call()` — the name already holds a real value that an empty
    stub would discard."""
    src = ("client = WebClient(token=tok)\n"
           "total = 5\n"
           "total += client.chat_postMessage(channel='#c', text='x')\n")
    res = _neutralise(src)
    assert "binding-not-preserved" in _kinds(res)
    assert "_aidp_slack_neutralised" not in res.source


def test_subscript_target_is_reported_not_stubbed():
    """`d['k'] = call()` — the container may be unset, so writing into it
    could raise or mutate unrelated state."""
    src = ("client = WebClient(token=tok)\n"
           "d['k'] = client.chat_postMessage(channel='#c', text='x')\n")
    res = _neutralise(src)
    assert "binding-not-preserved" in _kinds(res)
    assert "_aidp_slack_neutralised" not in res.source


def test_attribute_target_is_reported_not_stubbed():
    src = ("client = WebClient(token=tok)\n"
           "self.resp = client.chat_postMessage(channel='#c', text='x')\n")
    res = _neutralise(src)
    assert "binding-not-preserved" in _kinds(res)


def test_starred_unpacking_is_reported_not_stubbed():
    src = ("client = WebClient(token=tok)\n"
           "first, *rest = send_slack(payload={})\n")
    res = _neutralise(src)
    assert "binding-not-preserved" in _kinds(res)


# ─── no stub where there is nothing to preserve ─────────────────────────────

def test_bare_expression_statement_gets_no_stub():
    """Nothing was bound, so nothing needs preserving."""
    src = ("client = WebClient(token=tok)\n"
           "client.chat_postMessage(channel='#c', text='x')\n")
    res = _neutralise(src)
    assert res.commented
    assert "_aidp_slack_neutralised" not in res.source
    assert "binding-preserved" not in _kinds(res)


def test_slack_reads_are_untouched_so_never_stubbed():
    """A read has no side effect and is left running entirely."""
    src = ("client = WebClient(token=tok)\n"
           "hist = client.conversations_history(channel='C1')\n"
           "logger.info(hist['messages'])\n")
    res, ns = _run(src)
    assert res.commented == 0
    assert "_aidp_slack_neutralised" not in res.source
    assert ns["_sink"] == [([{"ts": "1.0"}],)]


# ─── the stub must not depend on the notebook's namespace ───────────────────
#
# `type` and `str` are ordinary names, resolved local -> global -> builtins, so
# a notebook variable called `type` was found before the builtin and the stub
# line raised on code that ran fine before:
#
#     type = 'daily'  ->  TypeError: 'str' object is not callable
#     str  = 'x'      ->  TypeError: metaclass conflict
#
# Worse than one bad cell: the kernel namespace persists and a %run child runs
# in the parent's namespace, so one such assignment anywhere earlier poisoned
# every later neutralised assignment -- including in cells whose Slack result
# is never read. These tests EXECUTE the result, because the old expression
# parsed perfectly and only failed when run.

@pytest.mark.parametrize("shadow", [
    "type = 'daily'",
    "str = 'x'",
    "type = 'daily'\nstr = 'x'",
    "type, str = 'daily', 'x'",
])
def test_stub_survives_shadowed_builtins(shadow):
    src = (f"{shadow}\n"
           "result = client.chat_postMessage(channel='#c', text='x')\n"
           "logger.info(result)\n")
    _res, ns = _run(src)
    assert ns["_sink"] == [("",)]


def test_stub_survives_shadowing_from_an_earlier_cell():
    """The realistic shape: the shadowing is already in the namespace when the
    neutralised cell runs, exactly as a %run child would leave it."""
    res = _neutralise(
        "result = client.chat_postMessage(channel='#c', text='x')\n"
        "logger.info(result)\n")
    namespace = {"_sink": []}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile("type = 'daily'\nstr = 'x'\n", "<earlier>", "exec"), namespace)
    exec(compile(res.source, "<cell>", "exec"), namespace)
    assert namespace["_sink"] == [("",)]


def test_stub_does_not_claim_every_dunder():
    """PySpark probes `__UDT__` during schema inference; a stub that answers
    True to every dunder could be misread as a user-defined type."""
    _res, ns = _run("r = client.chat_postMessage(channel='#c', text='x')\n")
    stub = ns["r"]
    for dunder in ("__UDT__", "__array__", "__dataframe__", "__arrow_c_stream__"):
        assert not hasattr(stub, dunder), dunder
    # Payload attributes stay empty and self-similar.
    assert stub.data == ""
    # status_code is now a deliberate exception: an int, so that
    # `resp.status_code >= 400` evaluates instead of raising TypeError.
    assert stub.status_code == 200


def test_stub_is_picklable_and_degrades_to_an_empty_string():
    """A stub can be captured by a closure and shipped to a Spark worker."""
    import pickle

    _res, ns = _run("r = client.chat_postMessage(channel='#c', text='x')\n")
    stub = ns["r"]
    restored = pickle.loads(pickle.dumps(stub))
    assert restored == ""
    assert type(restored) is str


def test_stub_expression_references_no_bare_builtin():
    """Guard the mechanism, not just the behaviour: a future edit that drops
    back to bare `type(`/`(str,)` reintroduces the shadowing break."""
    from slack_ast_neutralize import _STUB_EXPR

    assert "__import__('builtins')" in _STUB_EXPR
    assert "_b.type(" in _STUB_EXPR
    assert "(_b.str,)" in _STUB_EXPR
    # The old form subclassed `str` by bare name; `(_b.str,)` does not match.
    assert "(str,)" not in _STUB_EXPR
    assert _STUB_EXPR.startswith("(lambda _b:")


# ─── binding-not-preserved must describe what actually happens ──────────────
#
# One hardcoded "a reader below it will raise NameError" was wrong for half the
# target kinds. Measured consequences:
#
#   augmented assignment, name already set -> nothing raises; STALE value
#   augmented assignment, name never set   -> NameError
#   subscript, container exists            -> KeyError / IndexError
#   subscript, container unset             -> NameError
#   attribute target                       -> AttributeError
#   starred unpacking                      -> NameError
#
# The augmented-assignment case is the one that mattered: the message promised
# a loud failure where the reader quietly sees the old value.

def _not_preserved_reason(src):
    res = _neutralise(src)
    msgs = [m for k, m, _l in (res.unprovable or [])
            if k == "binding-not-preserved"]
    assert msgs, f"no binding-not-preserved finding for: {src!r}"
    return " ".join(msgs)


def test_augmented_assignment_message_says_stale_not_nameerror():
    """Nothing raises here -- the reader sees the pre-existing value."""
    reason = _not_preserved_reason(
        "total += client.chat_postMessage(channel='c', text='x')\n")
    assert "STALE" in reason
    assert "nothing raises" in reason
    assert "will raise NameError" not in reason


def test_augmented_assignment_really_does_not_raise():
    """Pins the behaviour the message now describes: no exception, stale value."""
    res = _neutralise("total += client.chat_postMessage(channel='c', text='x')\n"
                      "logger.info(total)\n")
    assert res.commented
    namespace = {"_sink": []}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile("total = 5\n", "<earlier>", "exec"), namespace)
    exec(compile(res.source, "<cell>", "exec"), namespace)
    assert namespace["_sink"] == [(5,)]          # stale, and nothing raised


def test_subscript_message_says_keyerror():
    reason = _not_preserved_reason(
        "d['k'] = client.chat_postMessage(channel='c', text='x')\n")
    assert "KeyError" in reason


def test_attribute_message_says_attributeerror():
    reason = _not_preserved_reason(
        "self.r = client.chat_postMessage(channel='c', text='x')\n")
    assert "AttributeError" in reason


def test_starred_unpacking_message_says_nameerror():
    reason = _not_preserved_reason(
        "first, *rest = client.chat_postMessage(channel='c', text='x')\n")
    assert "NameError" in reason
