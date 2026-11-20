"""The response stub may only stand in for a value that WAS a response.

DEFECT (reproduced 2026-10-06 against this branch's head; base the dev branch is
unaffected because it never re-bound anything):

The tool comments a statement for two different reasons, and the stub was
applied to both.

  call evidence  `r = client.chat_postMessage(...)`
                 the value lost IS Slack's reply, so an inert reply is a
                 coherent stand-in. This is what the stub was built for.

  TEXT evidence  `cfg = {"webhook": "...hooks.slack.com...",
                         "threshold": 0.5, "table": "prod.sales"}`
                 the statement never calls Slack. It is commented because a
                 Slack address appears in a string literal -- which is how
                 webhook sends get caught at all. The value lost is the
                 CUSTOMER'S OWN data, and there is no "inert response" that
                 stands in for a config dict.

Measured damage on the second class, with the notebook CONTINUING:

    cfg   -> stub, so cfg["table"] == '' and cfg["threshold"] == ''
             i.e. spark.read.table(cfg["table"]) reads table ''
    links -> stub, so len(links) == 0 and a loop over them does nothing
    note  -> stub, so the message body silently becomes ''

base the dev branch raised NameError for every one of these. Converting a loud
failure into silent wrong data is the single outcome this repository forbids
outright, and the stub introduced it rather than inheriting it.

THE RULE: substitute only when the OUTERMOST expression on the right-hand
side is the FLAGGED SEND'S OWN CALL (unwrapping `await`) -- judged by that
call's identity: the flagged method name, or a send-shaped name.

Two weaker rules were tried and both leak:

  "contains a call somewhere"  -- `cfg = {"webhook": get_url(), ...}` assigns
                                  a dict, and stubbing it is the data loss.
  "is a call"                  -- `cfg = dict(...)`, `url = os.getenv(...)`,
                                  `sorted([...])` assign ORDINARY results, and
                                  every one of them got the stand-in. The
                                  getenv shape was the worst: losing the URL
                                  turned a suppressed send into an outbound
                                  POST to ''.

Nor is it a whole-statement walk, which would accept
`x = wrap(client.chat_postMessage(...))` -- there `x` is wrap's return value,
not the response.

Erring this way can only DECLINE a stand-in, which fails loudly; it can never
invent one for a value that was never a response, so unanticipated shapes land
on the safe side.
"""
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    _STUB_TEMP_NAME,
    _value_is_the_flagged_send,
    neutralize_cell,
    new_bindings,
    update_bindings,
)

CHILD_BINDS_SLACK = (
    "from slack_sdk import WebClient\nclient = WebClient(token=TOKEN)\n")

HOOK = "https://hooks.slack.com/services/T000/B000/xyz"

# Commented for TEXT evidence only -- the value was never a response.
NOT_RESPONSES = {
    "config dict": (
        'cfg = {"webhook": "%s", "threshold": 0.5, "table": "prod.sales"}\n'
        'record(cfg["table"], cfg["threshold"])\n' % HOOK),
    "message string": (
        "note = 'see %s for the format'\n"
        "record(note)\n" % HOOK),
    "url constant": (
        'WEBHOOK = "%s"\n'
        "record(WEBHOOK)\n" % HOOK),
    "list of links": (
        'links = ["%s", "https://wiki/internal"]\n'
        "record(len(links))\n" % HOOK),
    "f-string mentioning the hook": (
        'msg = f"posting to {\'%s\'}"\n'
        "record(msg)\n" % HOOK),
    "dict that CONTAINS a call": (
        'cfg = {"webhook": get_url(), "hook": "%s", "table": "prod.sales"}\n'
        'record(cfg["table"])\n' % HOOK),
}

# A call-valued RHS is NOT automatically a response. "Is the outermost node a
# Call" was necessary but not sufficient, and treating it as sufficient
# re-opened this very class for every ordinary call. Base raises NameError in
# all of them.
NOT_RESPONSES.update({
    "dict() call": (
        'cfg = dict(webhook="%s", table="prod.sales")\n'
        'record(cfg["table"])\n' % HOOK),
    "os.getenv fallback": (
        'url = os.getenv("SLACK_URL", "%s")\n'
        "record(url)\n" % HOOK),
    "helper call": (
        'cfg = load_cfg(webhook="%s", table="prod.sales")\n'
        'record(cfg["table"])\n' % HOOK),
    "str.format": (
        'msg = "post to {} now {}".format("%s", 1)\n'
        "record(msg)\n" % HOOK),
    "sorted(...)": (
        'links = sorted(["%s", "https://wiki/x"])\n'
        "record(len(links))\n" % HOOK),
    "json.dumps(...)": (
        'payload = json.dumps({"webhook": "%s"})\n'
        "record(payload)\n" % HOOK),
})

# Genuine sends -- the stub MUST survive here, or the fix has broken the thing
# the stub exists for.
REAL_RESPONSES = {
    "slack client send":
        'r = client.chat_postMessage(channel="#c", text="x")\n'
        'record(r["ts"])\n',
    "webhook post":
        'resp = requests.post("%s", json={"text": "x"})\n'
        "record(resp.status_code)\n" % HOOK,
    "wrapper send":
        "r = send_slack('done')\nrecord(r)\n",
    "tuple target":
        'ok, info = client.chat_postMessage(channel="#c", text="x")\n'
        "record(ok, info)\n",
}

PRELUDE = """
class _Client:
    def __getattr__(self, name):
        def _call(*a, **k):
            raise AssertionError("SLACK SEND EXECUTED: " + name)
        return _call


client = _Client()


class _Requests:
    def post(self, *a, **k):
        raise AssertionError("WEBHOOK POST EXECUTED")


requests = _Requests()


def send_slack(*a, **k):
    raise AssertionError("WRAPPER SEND EXECUTED")


def get_url():
    return "https://hooks.slack.com/services/T000/B000/xyz"


def wrap(value):
    return value


# Present so that a regression which RESTORED one of the call-valued shapes
# would run it and record a value, rather than raising NameError on the helper
# itself and passing the loudness test for the wrong reason.
import json                                                     # noqa: E402


def load_cfg(**kwargs):
    return dict(kwargs)


class _Env:
    @staticmethod
    def getenv(key, default=None):
        return default


os = _Env()

TOKEN = "placeholder"
seen = []


def record(*a):
    seen.append(a)
"""


def _transform(src):
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    assert not result.parse_error, result.parse_error
    return result


def _run(src):
    """(result, outcome) where outcome is 'NameError' or the recorded values."""
    result = _transform(src)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    try:
        exec(compile(result.source, "<cell>", "exec"), namespace)
    except NameError:
        return result, "NameError"
    except AssertionError as exc:
        raise AssertionError("a real send executed: %s" % exc)
    return result, namespace["seen"]


# ─── the defect: the customer's own data must not become a stub ─────────────

@pytest.mark.parametrize("case", sorted(NOT_RESPONSES))
def test_non_response_value_is_not_stubbed(case):
    out = _transform(NOT_RESPONSES[case]).source
    assert _STUB_TEMP_NAME not in out, out


@pytest.mark.parametrize("case", sorted(NOT_RESPONSES))
def test_non_response_value_fails_loudly_instead_of_running_on(case):
    """The whole point: base's NameError is restored, so no empty value flows
    downstream into a table read or a filter."""
    _result, outcome = _run(NOT_RESPONSES[case])
    assert outcome == "NameError", (
        "%s continued with %r instead of failing" % (case, outcome))


@pytest.mark.parametrize("case", sorted(NOT_RESPONSES))
def test_the_reason_says_the_value_was_never_a_response(case):
    result = _transform(NOT_RESPONSES[case])
    reasons = [m for k, m, _l in (result.unprovable or [])
               if k == "binding-not-preserved"]
    assert reasons, "no binding-not-preserved finding for %s" % case
    joined = " ".join(reasons)
    assert "TEXT" in joined, joined
    assert "never a Slack response" in joined, joined


def test_config_dict_no_longer_yields_empty_settings():
    """Named explicitly because this is the shape that reached real data:
    an empty table name and an empty threshold, with the cell still running."""
    _result, outcome = _run(NOT_RESPONSES["config dict"])
    assert outcome == "NameError"


def test_a_dict_containing_a_call_is_still_a_dict():
    """Pins the OUTERMOST-expression rule. 'contains a call anywhere' would
    stub this dict and re-introduce the data loss."""
    out = _transform(NOT_RESPONSES["dict that CONTAINS a call"]).source
    assert _STUB_TEMP_NAME not in out


# ─── the stub must still work where it was designed to ─────────────────────

@pytest.mark.parametrize("case", sorted(REAL_RESPONSES))
def test_real_send_keeps_its_stub(case):
    out = _transform(REAL_RESPONSES[case]).source
    assert _STUB_TEMP_NAME in out, out


@pytest.mark.parametrize("case", sorted(REAL_RESPONSES))
def test_real_send_still_runs_its_consumers(case):
    _result, outcome = _run(REAL_RESPONSES[case])
    assert outcome != "NameError", "%s lost its binding" % case


@pytest.mark.parametrize("case", sorted(REAL_RESPONSES))
def test_no_real_send_executes(case):
    """_run raises if any tripwire fires, so reaching here is the assertion."""
    _run(REAL_RESPONSES[case])


def test_await_is_unwrapped_so_an_async_send_still_stubs():
    src = ('async def go():\n'
           '    r = await client.chat_postMessage(channel="#c", text="x")\n'
           '    return r["ts"]\n')
    assert _STUB_TEMP_NAME in _transform(src).source


# ─── the accepted cost ─────────────────────────────────────────────────────

def test_subscript_of_a_send_loses_the_stub():
    """`r = <send>["ts"]` assigns a timestamp string, not a response, so the
    outermost expression is a subscript and the stub is declined. Base behaved
    the same way; pinned so it stays a decision rather than a surprise."""
    src = ('r = client.chat_postMessage(channel="#c", text="x")["ts"]\n'
           "record(r)\n")
    result, outcome = _run(src)
    assert _STUB_TEMP_NAME not in result.source
    assert outcome == "NameError"
    kinds = [k for k, _m, _l in (result.unprovable or [])]
    assert "binding-not-preserved" in kinds


# ─── a call-valued RHS must be the SEND's call, not just any call ──────────
#
# `1ea0b85` closed this class for literal values but judged only
# `isinstance(value, ast.Call)`, so every TEXT-evidence statement assigning an
# ordinary call's result still received the response stand-in. Measured, tip
# the dev branch vs that head:
#
#   cfg = dict(webhook="<hook>", table="prod.sales")  NameError -> cfg["table"] ''
#   url = os.getenv("SLACK_URL", "<hook>")            NameError -> url '' AND
#                                                                  the POST fired
#   cfg = load_cfg(webhook="<hook>", ...)             NameError -> ''
#   msg = "...<hook>...".format(1)                    NameError -> ''
#   links = sorted(["<hook>", ...])                   NameError -> len 0
#   payload = json.dumps({"webhook": "<hook>"})       NameError -> ''
#
# The decision is now the OUTERMOST call's own identity: the flagged method,
# or a send-shaped name. Deliberately not a whole-statement walk, which would
# accept `x = wrap(client.chat_postMessage(...))` where `x` is wrap's return
# value rather than the response.

ESCAPE_PRELUDE = PRELUDE + """
class _Transmitted(Exception):
    pass


class _OutboundRequests:
    def post(self, url, *a, **k):
        raise _Transmitted("POST to %r" % (url,))


requests = _OutboundRequests()


class _Env:
    @staticmethod
    def getenv(key, default=None):
        return default


os = _Env()
"""


def test_losing_a_webhook_url_does_not_redirect_the_post():
    """The worst shape: stubbing the URL turned a suppressed send into an
    outbound request to a NEW target (an empty URL)."""
    src = ('url = os.getenv("SLACK_URL", "%s")\n'
           "requests.post(url, json={})\n" % HOOK)
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    namespace = {}
    exec(compile(ESCAPE_PRELUDE, "<prelude>", "exec"), namespace)
    try:
        exec(compile(result.source, "<cell>", "exec"), namespace)
    except namespace["_Transmitted"] as exc:
        raise AssertionError("an outbound request was made: %s" % exc)
    except NameError:
        pass                      # the URL is gone, loudly -- as base does


@pytest.mark.parametrize("call", [
    'dict(webhook="H")',
    'os.getenv("SLACK_URL", "H")',
    'load_cfg(webhook="H")',
    '"H".format(1)',
    'sorted(["H"])',
    'json.dumps({"webhook": "H"})',
    "wrap(other())",
])
def test_ordinary_calls_are_not_the_flagged_send(call):
    import ast
    value = ast.parse("x = " + call).body[0].value
    assert not _value_is_the_flagged_send(value, "chat_postMessage"), call


@pytest.mark.parametrize("call,method", [
    ('client.chat_postMessage(channel="c")', "chat_postMessage"),
    ('requests.post("H")', "statement"),
    ("send_slack('done')", "statement"),
    ("notify_team('x')", "statement"),
    ('client.files_upload_v2(file="f")', "files_upload_v2"),
])
def test_send_shaped_calls_are_recognised(call, method):
    import ast
    value = ast.parse("x = " + call).body[0].value
    assert _value_is_the_flagged_send(value, method), call


def test_a_send_wrapped_in_another_call_is_not_stubbed():
    """`x = wrap(<send>)` assigns wrap's return value, not the response. The
    statement DOES send, so it gets the nested-value reason rather than the
    text-evidence one."""
    src = ('x = wrap(client.chat_postMessage(channel="#c", text="y"))\n')
    result = _transform(src)
    assert _STUB_TEMP_NAME not in result.source, result.source
    msg = next(m for k, m, _l in (result.unprovable or [])
               if k == "binding-not-preserved")
    assert "DOES call Slack" in msg, msg
    assert "wrap()" in msg, msg


def test_the_reason_names_the_called_function():
    src = ('cfg = dict(webhook="%s", table="prod.sales")\n' % HOOK)
    msg = next(m for k, m, _l in (_transform(src).unprovable or [])
               if k == "binding-not-preserved")
    assert "value returned by dict()" in msg, msg
