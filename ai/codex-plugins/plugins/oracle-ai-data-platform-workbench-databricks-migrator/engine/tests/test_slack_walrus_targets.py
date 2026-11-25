"""A walrus target holds the call's result, so it must be re-bound and reported.

DEFECT (reproduced 2026-10-06 against this branch's head):

    print((r := client.chat_postMessage(channel="#c", text="x")))
    log(r["ts"])

The send WAS neutralised -- no escape -- but nothing re-bound `r`, so the next
reader raised NameError and NO finding was emitted at all. The emit loop only
inspected STATEMENT-level assignment targets, and this statement has none:
the assignment is the `:=` inside the call's arguments.

`(r := expr)` assigns expr's value to `r` and also yields it, so a walrus
target holds the call's RESULT exactly as a plain `r = <call>` does. It is
precisely the binding the stand-in exists for, and the plain-assignment form
has always had one.

Five shapes were affected; all raised NameError and four of them reported
nothing useful about the dangling name:

    print((r := <send>))                       no finding at all
    if (r := <send>):                          container-header-write only
    while (r := <send>) and False:             container-header-write only
    vals = [(r := <send>) for _ in range(1)]   wrong reason (see below)
    ok = (r := <send>) and True                wrong reason (see below)

A SECOND defect, found while fixing the first: for the last two shapes the
report said

    "commented because a Slack address appears in its TEXT, not because it
     calls Slack -- the value it produced was the customer's own boolop"

Both claims are false. The statement IS a send, and "boolop" means nothing to
an operator. One message was serving two unrelated situations:

    the statement never called Slack          `cfg = {...hooks.slack.com...}`
      -> text-evidence wording, accurate, unchanged
    the statement DID call Slack, but assigns an expression built AROUND the
    response rather than the response itself
      -> new wording, and it says the walrus target IS re-bound

Node kinds are rendered in plain English ("a list comprehension", not "a
listcomp") because the reader is a migration operator, not a grammar reader.
"""
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    _STUB_TEMP_NAME,
    _describe_value,
    _walrus_targets,
    neutralize_cell,
    new_bindings,
    update_bindings,
)

CHILD_BINDS_SLACK = (
    "from slack_sdk import WebClient\nclient = WebClient(token=TOKEN)\n")

SEND = 'client.chat_postMessage(channel="#c", text="x")'

WALRUS_SHAPES = {
    "inside a call":
        'print((r := %s))\nrecord(r["ts"])\n' % SEND,
    "in an if test":
        'if (r := %s):\n    pass\nrecord(r["ts"])\n' % SEND,
    "in a while test":
        'while (r := %s) and False:\n    pass\nrecord(r["ts"])\n' % SEND,
    "in a comprehension":
        'vals = [(r := %s) for _i in range(1)]\nrecord(r["ts"])\n' % SEND,
    "in a boolean expression":
        'ok = (r := %s) and True\nrecord(r["ts"])\n' % SEND,
    "in a return-style expression":
        'bool((r := %s) is not None)\nrecord(r["ts"])\n' % SEND,
}

PRELUDE = """
class _Client:
    def __getattr__(self, name):
        def _call(*a, **k):
            raise AssertionError("SLACK SEND EXECUTED: " + name)
        return _call


client = _Client()
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
    result = _transform(src)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    try:
        exec(compile(result.source, "<cell>", "exec"), namespace)
    except NameError as exc:
        return result, "NameError: %s" % exc
    except AssertionError as exc:
        raise AssertionError("a send executed: %s" % exc)
    return result, namespace["seen"]


def _kinds(result):
    return [k for k, _m, _l in (result.unprovable or [])]


# ─── the walrus target is re-bound ─────────────────────────────────────────

@pytest.mark.parametrize("shape", sorted(WALRUS_SHAPES))
def test_walrus_target_is_rebound(shape):
    out = _transform(WALRUS_SHAPES[shape]).source
    assert "r = %s" % _STUB_TEMP_NAME in out, out


@pytest.mark.parametrize("shape", sorted(WALRUS_SHAPES))
def test_reader_of_a_walrus_target_no_longer_raises(shape):
    _result, outcome = _run(WALRUS_SHAPES[shape])
    assert outcome == [("",)], outcome


@pytest.mark.parametrize("shape", sorted(WALRUS_SHAPES))
def test_every_walrus_target_is_reported(shape):
    """The reported gap: `print((r := <send>))` produced no finding at all,
    so an operator saw a NameError with nothing explaining it."""
    result = _transform(WALRUS_SHAPES[shape])
    assert "binding-preserved" in _kinds(result), _kinds(result)
    msg = next(m for k, m, _l in result.unprovable if k == "binding-preserved")
    assert "r" in msg


@pytest.mark.parametrize("shape", sorted(WALRUS_SHAPES))
def test_no_send_executes(shape):
    """Unchanged guarantee -- _run raises if the tripwire fires."""
    _run(WALRUS_SHAPES[shape])


def test_several_walrus_targets_on_one_statement_are_all_rebound():
    src = ('print((a := %s), (b := %s))\nrecord(a["ts"], b["ts"])\n'
           % (SEND, SEND))
    _result, outcome = _run(src)
    assert outcome == [("", "")], outcome


def test_walrus_holding_a_non_call_is_not_rebound():
    """`(r := some_dict)` is not a response."""
    import ast
    stmt = ast.parse("print((r := {'a': 1}))").body[0]
    rebindable, declined = _walrus_targets(stmt, "chat_postMessage")
    assert rebindable == []
    assert declined == ["r"]


def test_walrus_holding_an_ORDINARY_call_is_not_rebound():
    """The regression this signature exists for. An earlier version asked only
    `isinstance(value, ast.Call)`, so

        client.chat_postMessage(channel="#c", text=f"rows={(n := len(rows))}")

    stubbed `n` to '' where it should be len(rows) -- the same data loss
    _value_is_the_flagged_send closes on the statement-level path, re-opened
    one commit later through the walrus path.
    """
    import ast
    stmt = ast.parse(
        'client.chat_postMessage(channel="c", text=f"{(n := len(rows))}")'
    ).body[0]
    rebindable, declined = _walrus_targets(stmt, "chat_postMessage")
    assert rebindable == []
    assert declined == ["n"]


def test_walrus_holding_the_flagged_send_reports_name_and_column():
    import ast
    stmt = ast.parse(
        'print((r := client.chat_postMessage(channel="c")))').body[0]
    rebindable, declined = _walrus_targets(stmt, "chat_postMessage")
    assert [name for _col, name in rebindable] == ["r"]
    assert rebindable[0][0] > 0        # a real column, not the statement's
    assert declined == []


@pytest.mark.parametrize("cell,name,kind", [
    ('client.chat_postMessage(channel="c", text=f"{(n := len(rows))}")\n',
     "n", "value returned by len()"),
    ('client.chat_postMessage(channel="c", text=str((c := compute())))\n',
     "c", "value returned by compute()"),
    ('client.chat_postMessage(channel="c", text=str((d := {"a": 1})))\n',
     "d", "dict"),
])
def test_declined_walrus_target_is_reported_not_silently_dropped(
        cell, name, kind):
    result = _transform(cell)
    assert _STUB_TEMP_NAME not in result.source, result.source
    reasons = [m for k, m, _l in (result.unprovable or [])
               if k == "binding-not-preserved"]
    joined = " ".join(reasons)
    assert "`%s` is assigned by a walrus" % name in joined, joined
    assert kind in joined, joined


def test_ordinary_call_walrus_fails_loudly_like_base():
    src = ('client.chat_postMessage(channel="#c", '
           'text=f"rows={(n := len(rows))}")\n'
           "record(n)\n")
    result = _transform(src)
    namespace = {"rows": [1, 2, 3]}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    namespace["rows"] = [1, 2, 3]
    with pytest.raises(NameError):
        exec(compile(result.source, "<cell>", "exec"), namespace)


# ─── the second defect: the reason must not deny the send ──────────────────

NESTED_VALUE_SHAPES = {
    "boolean expression": ('ok = (r := %s) and True\n' % SEND,
                           "boolean expression"),
    "comprehension": ('vals = [(r := %s) for _i in range(1)]\n' % SEND,
                      "list comprehension"),
}


@pytest.mark.parametrize("shape", sorted(NESTED_VALUE_SHAPES))
def test_nested_value_reason_admits_the_statement_sent(shape):
    src, _description = NESTED_VALUE_SHAPES[shape]
    msg = next(m for k, m, _l in _transform(src).unprovable
               if k == "binding-not-preserved")
    assert "DOES call Slack" in msg, msg
    # The false claim must not come back.
    assert "not because it calls Slack" not in msg, msg
    assert "Any walrus target inside the statement IS re-bound" in msg


@pytest.mark.parametrize("shape", sorted(NESTED_VALUE_SHAPES))
def test_nested_value_reason_names_the_kind_in_plain_english(shape):
    src, description = NESTED_VALUE_SHAPES[shape]
    msg = next(m for k, m, _l in _transform(src).unprovable
               if k == "binding-not-preserved")
    assert description in msg, msg


def test_text_evidence_reason_is_unchanged():
    """The other half of the split must keep its accurate wording: this
    statement really does not call Slack."""
    src = ('cfg = {"webhook": "https://hooks.slack.com/x", '
           '"table": "prod.sales"}\n')
    msg = next(m for k, m, _l in _transform(src).unprovable
               if k == "binding-not-preserved")
    assert "appears in its TEXT" in msg
    assert "not because it calls Slack" in msg
    assert "dict" in msg
    assert "DOES call Slack" not in msg


@pytest.mark.parametrize("source,expected", [
    ("x = {'a': 1}", "dict"),
    ("x = [1]", "list"),
    ("x = (1,)", "tuple"),
    ("x = [i for i in y]", "list comprehension"),
    ("x = {i for i in y}", "set comprehension"),
    ("x = (i for i in y)", "generator expression"),
    ("x = a and b", "boolean expression"),
    ("x = a + b", "computed value"),
    ("x = a == b", "comparison result"),
    ("x = f'{a}'", "f-string"),
    ("x = d['k']", "indexed value"),
    ("x = o.attr", "attribute value"),
    ("x = other", "copy of another variable"),
    ("x = a if b else c", "conditional expression"),
    ("x = 'text'", "string"),
    ("x = 5", "int literal"),
])
def test_value_descriptions_are_plain_english(source, expected):
    import ast
    assert _describe_value(ast.parse(source).body[0].value) == expected


def test_describe_value_handles_a_missing_value():
    assert _describe_value(None) == "value"
