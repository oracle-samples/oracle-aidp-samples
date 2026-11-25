"""Retry loops and the success-reporting stand-in.

HISTORY, because this file has held two opposite contracts and the second
only makes sense against the first.

The stand-in was originally FALSY, which made a retry loop non-terminating:

    ok = False
    while not ok:
        r = client.chat_postMessage(...)   # commented out; a no-op
        ok = r["ok"]                        # falsy stand-in -> never satisfied

Six shapes hung, and in three of them the assignment sat OUTSIDE the loop and
only the READ was inside it, so a loop-body-scoped rule would have closed only
half the class. The response was to withhold the stand-in from any cell
containing a `while`, which made those cells fail loudly with NameError.

Reporting success (owner decision 2026-10-06) removes the MECHANISM rather
than the symptom: everything derived from the stand-in is truthy, so eight of
the nine measured shapes terminate on the first iteration exactly as
production does. Withholding the stand-in would block all eight for a reason
that no longer exists, so the per-cell guard was removed and this file now
pins the opposite contract: these loops COMPLETE.

ACCEPTED RESIDUAL (pinned below as xfail, not deleted): a loop whose exit test
compares a derived value against a literal the stand-in does not fabricate --
`status_code == 201` when it is 200, `r["ts"] == "<a ts>"` or
`len(resp.text) > 0` when both are empty -- can never be satisfied and still
hangs. It is reported per cell rather than guarded, because both candidate
guards are worse: withholding blocks the eight working shapes, and detecting
"unsatisfiable literal" needs value tracking that can fail in both directions
(miss a flow -> the hang returns; over-trigger on `== 200` -> a working
conversion is blocked) while duplicating the fabricated values into the rule
that reasons about them.

The tests EXECUTE under a deadline, so a hang fails the test rather than
wedging the suite. Asserting on the emitted source alone would not prove
termination either way.
"""
import contextlib
import os
import signal
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    _STUB_TEMP_NAME,
    neutralize_cell,
    new_bindings,
    update_bindings,
)

CHILD_BINDS_SLACK = (
    "from slack_sdk import WebClient\nclient = WebClient(token=TOKEN)\n")

SEND = 'client.chat_postMessage(channel="#c", text="x")'
POST = ('requests.post("https://hooks.slack.com/services/T/B/X", '
        'json={"text": "x"})')

# Every shape that hung under a falsy stand-in, plus the poll-outside forms.
# All of these must now COMPLETE.
TERMINATING_SHAPES = {
    "retry on r['ok']":
        "ok = False\nwhile not ok:\n    r = %s\n    ok = r[\"ok\"]\n" % SEND,
    "retry on r.get('ok')":
        "ok = False\nwhile not ok:\n    r = %s\n    ok = r.get(\"ok\")\n"
        % SEND,
    "while True + if r['ok']: break":
        'while True:\n    r = %s\n    if r["ok"]:\n        break\n' % SEND,
    "retry on resp.ok":
        "sent = False\nwhile not sent:\n    resp = %s\n    sent = resp.ok\n"
        % POST,
    "retry on status_code == 200":
        "sent = False\nwhile not sent:\n    resp = %s\n"
        "    sent = resp.status_code == 200\n" % POST,
    "retry on an unknown key":
        'ok = False\nwhile not ok:\n    r = %s\n    ok = r["delivered"]\n'
        % SEND,
    "retry on a payload field":
        'ts = None\nwhile not ts:\n    r = %s\n    ts = r["ts"]\n' % SEND,
    "poll outside the loop":
        'r = %s\nwhile not r["ok"]:\n    pass\n' % SEND,
    "assign inside a for nested in a while":
        "ok = False\nwhile not ok:\n    for _i in range(2):\n"
        "        r = %s\n    ok = r[\"ok\"]\n" % SEND,
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
TOKEN = "placeholder"
"""


class _Deadline(Exception):
    pass


@contextlib.contextmanager
def _deadline(seconds=1.0):
    """Interrupt a loop that will not end.

    SIGALRM, not a subprocess: these loops are interpreted Python, so the
    signal lands between bytecodes.
    """
    def _fire(_signum, _frame):
        raise _Deadline()

    previous = signal.signal(signal.SIGALRM, _fire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


needs_alarm = pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"),
    reason="SIGALRM is unavailable on this platform, so a hang cannot be "
           "interrupted; the stand-in-emitted assertions still cover the rule")


def _transform(src):
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    result = neutralize_cell(src, bindings, include_generic=True)
    assert not result.parse_error, result.parse_error
    return result


def _execute(src):
    """"completed" / "HANG" / an exception name."""
    result = _transform(src)
    namespace = {}
    exec(compile(PRELUDE, "<prelude>", "exec"), namespace)
    try:
        with _deadline():
            exec(compile(result.source, "<cell>", "exec"), namespace)
    except _Deadline:
        return "HANG"
    except AssertionError as exc:
        raise AssertionError("a send executed: %s" % exc)
    except Exception as exc:
        return type(exc).__name__
    return "completed"


# ─── the contract: retry loops complete, as production does ────────────────

@needs_alarm
@pytest.mark.parametrize("shape", sorted(TERMINATING_SHAPES))
def test_retry_loop_completes(shape):
    assert _execute(TERMINATING_SHAPES[shape]) == "completed", shape


@pytest.mark.parametrize("shape", sorted(TERMINATING_SHAPES))
def test_while_cells_do_get_the_stand_in(shape):
    """The inverted half of the old contract: the per-cell guard is gone."""
    assert _STUB_TEMP_NAME in _transform(TERMINATING_SHAPES[shape]).source


@needs_alarm
@pytest.mark.parametrize("shape", sorted(TERMINATING_SHAPES))
def test_no_send_executes_in_any_loop_shape(shape):
    """The guard's actual job must survive both design flips. _execute raises
    if a tripwire fires, so reaching the end is the assertion."""
    _execute(TERMINATING_SHAPES[shape])


# ─── the disclosure that substitutes for a guard ───────────────────────────

@pytest.mark.parametrize("shape", sorted(TERMINATING_SHAPES))
def test_every_while_cell_reports_the_residual(shape):
    kinds = [k for k, _m, _l in
             (_transform(TERMINATING_SHAPES[shape]).unprovable or [])]
    assert "retry-loop-may-not-terminate" in kinds, kinds


def test_the_residual_finding_names_the_unsatisfiable_shapes():
    """An operator must be able to recognise their own loop in the text."""
    result = _transform(TERMINATING_SHAPES["retry on r['ok']"])
    msg = next(m for k, m, _l in result.unprovable
               if k == "retry-loop-may-not-terminate")
    assert "status_code == 201" in msg
    assert "NOT TERMINATE" in msg
    assert "ConnectionDied" in msg
    assert "CHECK THIS LOOP'S EXIT CONDITION" in msg


def test_the_residual_is_reported_once_per_cell_not_once_per_send():
    """Two sends in one while cell must not duplicate the advice."""
    src = ("ok = False\nwhile not ok:\n"
           "    a = %s\n    b = %s\n    ok = a[\"ok\"]\n" % (SEND, SEND))
    findings = [k for k, _m, _l in (_transform(src).unprovable or [])
                if k == "retry-loop-may-not-terminate"]
    assert len(findings) == 1, findings


def test_cells_without_a_while_do_not_carry_the_residual_finding():
    kinds = [k for k, _m, _l in
             (_transform("r = %s\n" % SEND).unprovable or [])]
    assert "retry-loop-may-not-terminate" not in kinds


def test_a_for_loop_alone_does_not_carry_the_residual_finding():
    """`for` terminates by construction."""
    kinds = [k for k, _m, _l in
             (_transform("for _i in range(2):\n    r = %s\n"
                         % SEND).unprovable or [])]
    assert "retry-loop-may-not-terminate" not in kinds


# ─── the accepted residual, pinned rather than deleted ─────────────────────

@needs_alarm
@pytest.mark.xfail(
    strict=True,
    reason="accepted residual (owner decision 2026-10-06): an exit test "
           "comparing a derived value against a literal the stand-in does not "
           "fabricate can never be satisfied, so the cell does not terminate. "
           "Reported per cell via retry-loop-may-not-terminate instead of "
           "guarded, because withholding the stand-in blocks the eight shapes "
           "that work and detecting the literal needs value tracking that can "
           "fail in both directions. If this XPASSes the residual has changed "
           "and the finding's wording must be revisited.")
def test_literal_comparison_against_a_non_fabricated_value_terminates():
    src = ("sent = False\nwhile not sent:\n    resp = %s\n"
           "    sent = resp.status_code == 201\n" % POST)
    assert _execute(src) == "completed"


@needs_alarm
@pytest.mark.xfail(
    strict=True,
    reason="same accepted residual, payload-field form: r['ts'] is '' so a "
           "comparison against a real timestamp is never satisfied")
def test_payload_literal_comparison_terminates():
    src = ('ts = None\nwhile not ts:\n    r = %s\n'
           '    ts = r["ts"] if r["ts"] == "1700000000.1" else None\n' % SEND)
    assert _execute(src) == "completed"


@needs_alarm
def test_the_residual_shape_still_does_not_send():
    """Even where it hangs, nothing may be delivered."""
    src = ("sent = False\nwhile not sent:\n    resp = %s\n"
           "    sent = resp.status_code == 201\n" % POST)
    assert _execute(src) == "HANG"
