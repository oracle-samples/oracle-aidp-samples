"""The stand-in reports SUCCESS, so a notebook follows its production path.

OWNER DECISION (2026-10-06). The stub was originally falsy, on the reasoning
that nothing was sent so nothing should look successful. Measuring every
common way a notebook reads a response showed what that cost:

    if r["ok"]:                  -> took the ELSE branch
    assert r["ok"]               -> AssertionError
    resp.status_code >= 400      -> TypeError ('' vs int)

A falsy stand-in therefore diverts the notebook onto a branch production
never executes, and two shapes stop the cell outright -- so conversions could
not complete, and the realistic downstream path was never exercised.

The decision is to prefer path fidelity: report success, let the notebook run
the path it runs in production.

WHAT THIS COSTS, AND THE CONDITION THAT MAKES IT ACCEPTABLE

A cell can now go green without its notification path having been exercised,
and whatever the success path does -- marking records notified, writing an
audit row, dropping a queue entry -- runs on a send that never happened.
Table writes are redirected to the migration sandbox so they do not reach
production; non-table side effects are not covered by that.

That is a false-PASS shape, and it is only tolerable because every emission
REPORTS the fabrication. The disclosure is not decoration; it is the thing
that keeps this from being a silent false PASS, so it is pinned here as
hard as the behaviour.

NOT fabricated: payload fields. Only success-signalling fields are faked, so
the object stays honest about having no content:

    r["ok"] / r.ok / r.data["ok"] -> True        r.status_code -> 200
    bool(r) -> True                              r["ts"], r.json(), str(r) -> ''

200 rather than a 5xx: a 5xx is equally a fabrication AND would divert onto
the error branch, which is the thing this decision exists to avoid.
"""
import json
import os
import pickle
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    neutralize_cell,
    new_bindings,
    update_bindings,
)

CHILD_BINDS_SLACK = (
    "from slack_sdk import WebClient\nclient = WebClient(token=TOKEN)\n")

SLACK_SEND = 'r = client.chat_postMessage(channel="#c", text="x")\n'
WEBHOOK_SEND = ('r = requests.post("https://hooks.slack.com/services/T/B/X", '
                'json={"text": "x"})\n')

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
TOKEN = "placeholder"
seen = []


def record(*a):
    seen.append(a)
"""


def _stub_from(binder):
    """Execute the transformed binder and hand back the stand-in object."""
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(binder, bindings, include_generic=True)
    assert "_aidp_slack_neutralised" in result.source, result.source
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    return namespace["r"]


def _run(binder, consumer):
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(binder + consumer, bindings, include_generic=True)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile(result.source, "<cell>", "exec"), namespace)
    return namespace["seen"]


# ─── success signals ───────────────────────────────────────────────────────

def test_the_stand_in_is_truthy():
    assert bool(_stub_from(SLACK_SEND)) is True


@pytest.mark.parametrize("key", ["ok"])
def test_success_keys_are_true(key):
    assert _stub_from(SLACK_SEND)[key] is True


@pytest.mark.parametrize("attr", ["ok", "success"])
def test_success_attributes_are_true(attr):
    """`status` was in this list and is now 200 rather than True: it is
    aiohttp's status field and is compared NUMERICALLY, so a boolean made
    `resp.status != 200` raise. It belongs with the status codes below."""
    assert getattr(_stub_from(SLACK_SEND), attr) is True


@pytest.mark.parametrize("attr", ["status_code", "status"])
def test_status_fields_are_ints_so_numeric_checks_evaluate(attr):
    """Both spellings: requests uses status_code, aiohttp uses status."""
    stub = _stub_from(WEBHOOK_SEND)
    assert getattr(stub, attr) == 200
    assert (getattr(stub, attr) >= 400) is False
    assert (getattr(stub, attr) != 200) is False


@pytest.mark.parametrize("field", ["error", "errors", "warning", "warnings"])
def test_error_fields_are_falsy(field):
    """A real SUCCESSFUL Slack response carries no error key, so returning the
    truthy stand-in made `if r.get("error"): raise` fire on a send the object
    claims succeeded."""
    stub = _stub_from(SLACK_SEND)
    assert stub[field] is None
    assert getattr(stub, field) is None
    assert stub.get(field) is None


def test_get_consults_the_key_rather_than_returning_the_stand_in():
    """`.get` must be a real lookup. As the self-similar callable it returned
    the stand-in for every key, and the stand-in is truthy -- so
    `r.get("error")` reported an error on a fabricated success."""
    stub = _stub_from(SLACK_SEND)
    assert stub.get("ok") is True
    assert stub.get("error") is None
    assert stub.get("ts") == ""
    assert stub.get("missing", "fallback") == ""


def test_an_error_branch_is_not_taken_on_a_fabricated_success():
    seen = _run(SLACK_SEND,
                'if r.get("error"):\n    record("error branch")\nelse:\n'
                '    record("no error")\n')
    assert seen == [("no error",)]


def test_a_numeric_status_comparison_on_aiohttp_status_does_not_raise():
    seen = _run(WEBHOOK_SEND,
                'if r.status != 200:\n    record("bad")\nelse:\n'
                '    record("good")\n')
    assert seen == [("good",)]


def test_slack_sdk_data_dict_access_reports_ok():
    """`resp.data["ok"]` is the slack_sdk idiom; it must agree with r["ok"]."""
    assert _stub_from(SLACK_SEND).data["ok"] is True


# ─── the three shapes a falsy stand-in broke ───────────────────────────────

def test_truth_test_takes_the_sent_path():
    seen = _run(SLACK_SEND,
                'if r["ok"]:\n    record("sent")\nelse:\n'
                '    record("not sent")\n')
    assert seen == [("sent",)]


def test_assert_on_ok_no_longer_raises():
    assert _run(SLACK_SEND, 'assert r["ok"]\nrecord("passed")\n') == \
        [("passed",)]


def test_numeric_status_comparison_no_longer_raises():
    seen = _run(WEBHOOK_SEND,
                'if r.status_code >= 400:\n    record("err")\nelse:\n'
                '    record("ok")\n')
    assert seen == [("ok",)]


# ─── payload fields stay EMPTY: only success is fabricated ─────────────────

@pytest.mark.parametrize("expr,expected", [
    ('r["ts"]', ""),
    ('r.get("ts")', ""),
    ('str(r)', ""),
    ('r["channel"]["id"]', ""),
    ('r.json()', ""),
    ('r.text', ""),
    ('len(r.text)', 0),
    ('r + ""', ""),
])
def test_payload_fields_are_not_fabricated(expr, expected):
    assert _run(SLACK_SEND, "record(%s)\n" % expr) == [(expected,)]


def test_a_response_BODY_check_still_fails_loudly():
    """The line this design draws, pinned so it stays a decision.

    Status and success METADATA is fabricated; response BODIES are not. A
    Slack incoming webhook really replies with the body "ok", so a notebook
    doing `if resp.text != "ok": raise` still raises here. That is loud and
    base raised too (NameError), so it is not a regression -- but it does mean
    "every consumer now runs" is false, and fabricating body content to fix it
    would put invented values into artifacts.
    """
    stub = _stub_from(WEBHOOK_SEND)
    assert stub.text == ""
    assert (stub.text != "ok") is True


def test_an_artifact_still_receives_an_empty_payload_value():
    """Named rather than hidden: success is faked, content is not, so a
    serialised field is empty. This is the artifact residual."""
    seen = _run(SLACK_SEND,
                'import json\nrecord(json.dumps({"ts": r["ts"]}))\n')
    assert seen == [('{"ts": ""}',)]


# ─── the disclosure that makes this acceptable ─────────────────────────────

def _finding(binder):
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(binder + 'record(r["ts"])\n', bindings,
                             include_generic=True)
    return next(m for k, m, _l in (result.unprovable or [])
                if k == "binding-preserved")


@pytest.mark.parametrize("binder", [SLACK_SEND, WEBHOOK_SEND])
@pytest.mark.parametrize("phrase", [
    "NO message was delivered",
    "REPORTS SUCCESS",
    "CAN PASS WITHOUT ITS NOTIFICATION PATH",
    "marking records notified",
    "dropping a queue entry",
    "redirected to the migration sandbox",
    "throwaway workspace or test channel",
])
def test_finding_discloses_the_fabrication(binder, phrase):
    assert phrase in _finding(binder), phrase


@pytest.mark.parametrize("binder", [SLACK_SEND, WEBHOOK_SEND])
def test_notebook_comment_does_not_promise_a_real_send(binder):
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    out = neutralize_cell(binder, bindings, include_generic=True).source
    annotated = [ln for ln in out.splitlines() if "= _aidp_slack" in ln]
    assert annotated
    for line in annotated:
        assert "NO message was delivered" in line
        # The claim that was false for two consumer shapes must not return.
        assert "consumers below still run" not in line


# ─── properties that must survive the change ───────────────────────────────

def test_no_send_executes():
    """The guard's actual job. The tripwires raise if anything is reached."""
    _run(SLACK_SEND, 'record(r["ts"])\n')
    _run(WEBHOOK_SEND, "record(r.status_code)\n")


@pytest.mark.parametrize("dunder", [
    "__UDT__", "__array__", "__dataframe__", "__arrow_c_stream__"])
def test_stand_in_still_refuses_unknown_dunders(dunder):
    """PySpark probes `__UDT__` during schema inference. Adding `__bool__`
    must not turn the object into something that answers every dunder."""
    assert not hasattr(_stub_from(SLACK_SEND), dunder), dunder


def test_stand_in_is_still_picklable():
    """It can be captured by a closure and shipped to a Spark worker."""
    assert pickle.loads(pickle.dumps(_stub_from(SLACK_SEND))) == ""


def test_stand_in_is_still_json_serialisable_as_a_string():
    assert json.dumps(_stub_from(SLACK_SEND)) == '""'
