"""A method passed as a VALUE goes through the same certainty check.

`executor.submit(client.chat_postMessage, ...)` never calls the method in the
cell -- the post fires when the callee invokes it -- so this path exists to
catch the reference itself. It kept the pre-#170 rule (`is_slack` plus
_KNOWN_WRITE_RE) and never consulted receiver CERTAINTY, so a receiver whose
Slack verdict survived only via _bind's conditional fail-safe still had any
write-verb-ish method neutralised:

    update_run        hits "update" in _KNOWN_WRITE_RE
    create_experiment hits "create"

Both are MLflow, and both were commented -- the same defect as #170's incident,
in the one position that was not wired to the shared helper.

The write-verb gate is KEPT in addition to the certainty check, not replaced by
it: it is what leaves attribute READS (`client.token`, `client.base_url`)
alone, which the certainty check alone would neutralise on any proven client.

A consequence worth stating, because it looks wrong at first glance: for a
PROVEN client the reference IS stubbed, so `fn = client.chat_postMessage`
followed by `fn(...)` becomes a silent no-op returning ''. That is the
behaviour we want -- the send is suppressed and the caller keeps running. A
NameError there would be worse. It only looked like a defect while
`update_run` was being neutralised by mistake.
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

CONDITIONAL_REBIND = """
if use_mlflow:
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
"""


def _definite():
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    return bindings


def _uncertain():
    bindings = _definite()
    update_bindings(CONDITIONAL_REBIND, bindings)
    return bindings


def _commented(source, bindings):
    result = neutralize_cell(source, bindings, include_generic=True)
    return [ln for ln in result.source.splitlines() if "neutralised" in ln]


# ─── a proven client: the reference is caught whatever the method ───────────

@pytest.mark.parametrize("call", [
    "executor.submit(client.chat_postMessage, channel='c', text='x')",
    "executor.submit(client.files_upload_v2, file='f')",
    "executor.submit(client.views_open, trigger_id=t)",
    # Families absent from _SLACK_RESOURCES — must not depend on that list.
    "executor.submit(client.canvases_delete, canvas_id='C1')",
    "executor.submit(client.assistant_threads_setStatus, status='typing')",
    # Shapes other than submit()
    "callbacks.append(client.chat_postMessage)",
    "fn = client.chat_postMessage",
    "handlers = {'slack': client.files_upload_v2}",
])
def test_reference_to_a_send_on_a_proven_client_is_neutralised(call):
    assert _commented(call + "\n", _definite()), call


def test_reference_is_caught_under_a_folded_child_binding_too():
    """The binding comes from a %run child, not this cell."""
    assert _commented(
        "executor.submit(client.chat_postMessage, channel='c')\n", _definite())


# ─── an uncertain receiver: the method decides ──────────────────────────────

@pytest.mark.parametrize("call", [
    "executor.submit(client.update_run, run_id)",
    "executor.submit(client.create_experiment, 'x')",
    "executor.submit(client.download_artifacts, run_id)",
    "executor.submit(client.delete_tag, run_id, 'k')",
    "fn = client.update_run",
    "callbacks.append(client.create_experiment)",
])
def test_foreign_method_reference_survives_on_an_uncertain_receiver(call):
    assert _commented(call + "\n", _uncertain()) == [], call


@pytest.mark.parametrize("call", [
    "executor.submit(client.chat_postMessage, channel='c')",
    "executor.submit(client.files_upload_v2, file='f')",
    "callbacks.append(client.chat_postMessage)",
])
def test_slack_method_reference_is_still_caught_when_uncertain(call):
    """Doubt about the receiver does not excuse a method that IS Slack's."""
    assert _commented(call + "\n", _uncertain()), call


def test_foreign_reference_is_left_entirely_alone():
    """No stub either: the value is a real MLflow function, so replacing it
    with a callable null object would make fn(...) silently return ''."""
    result = neutralize_cell("fn = client.update_run\n", _uncertain(),
                             include_generic=True)
    assert result.commented == 0
    assert "_aidp_slack_neutralised" not in result.source
    assert result.unprovable == [] or "method-as-value" not in [
        k for k, _m, _l in result.unprovable]


# ─── attribute reads stay alone (why the write-verb gate is kept) ───────────

@pytest.mark.parametrize("call", [
    "tok = client.token",
    "base = client.base_url",
    "to = client.timeout",
])
def test_attribute_reads_on_a_proven_client_are_not_neutralised(call):
    assert _commented(call + "\n", _definite()) == [], call


def test_slack_read_methods_passed_as_values_are_left_alone():
    """A read has no side effect wherever it is invoked."""
    assert _commented(
        "executor.submit(client.conversations_history, channel='C1')\n",
        _definite()) == []


# ─── a WEAK receiver in method-value position ──────────────────────────────
#
# SEND ESCAPE, reproduced at head 5424f1d and on base: the value path bailed on
# `if bindings.is_weak(recv): continue` BEFORE any method check, so a namespace
# Slack method on a slack-named-factory binding escaped with commented=0 and no
# finding at all, and the send fired when the callee invoked it:
#
#     cfg = get_slack_client()                     # weak binding
#     executor.submit(cfg.chat_postMessage, ...)   # SENT
#     cbs.append(cfg.chat_postMessage)             # SENT
#     fn = cfg.files_upload_v2                     # SENT
#
# Earlier rounds reported this as "does not reproduce" because they probed a
# PROVEN binding, which takes the conclusive path. The weak state is a
# different cell of the grid, so these tests use a WEAK binding deliberately --
# a proven-binding test does not cover it.

WEAK_BINDING = "cfg = get_slack_client()\n"

WEAK_PRELUDE = """
class _WeakClient:
    token = "placeholder-token"

    def chat_postMessage(self, **kw):
        raise AssertionError("SLACK SEND EXECUTED: chat_postMessage")

    def files_upload_v2(self, **kw):
        raise AssertionError("SLACK SEND EXECUTED: files_upload_v2")

    def update_run(self, **kw):
        return "mlflow-ok"

    def get(self, key, default=None):
        return "general"


def get_slack_client():
    return _WeakClient()


class _Executor:
    def submit(self, fn, *a, **k):
        return fn(*a, **k)


executor = _Executor()
cbs = []
seen = []


def record(*a):
    seen.append(a)
"""


def _weak_bindings():
    bindings = new_bindings()
    update_bindings(WEAK_BINDING, bindings)
    return bindings


def _weak_run(cell):
    """Transform under a weak binding and execute. Raises if a send fires."""
    result = neutralize_cell(cell, _weak_bindings(), include_generic=True)
    namespace = {}
    exec(compile(WEAK_PRELUDE, "<prelude>", "exec"), namespace)
    exec(compile(WEAK_BINDING, "<binder>", "exec"), namespace)
    try:
        exec(compile(result.source, "<cell>", "exec"), namespace)
    except NameError:
        pass
    return result


def test_the_binding_really_is_weak():
    """If this stops holding, every test below passes for the wrong reason."""
    bindings = _weak_bindings()
    assert bindings.is_slack("cfg")
    assert bindings.is_weak("cfg")


@pytest.mark.parametrize("cell", [
    'executor.submit(cfg.chat_postMessage, channel="#a", text="s")\n',
    "cbs.append(cfg.chat_postMessage)\nfor cb in cbs:\n    cb(channel='a')\n",
    'fn = cfg.files_upload_v2\nfn(file="f")\n',
    'executor.submit(cfg.files_upload_v2, file="f")\n',
    "partial(cfg.chat_postMessage, channel='#a')\n",
])
def test_weak_receiver_method_value_does_not_send(cell):
    result = _weak_run(cell)
    assert result.commented, cell


@pytest.mark.parametrize("cell", [
    'executor.submit(cfg.chat_postMessage, channel="#a", text="s")\n',
    "cbs.append(cfg.chat_postMessage)\n",
    'fn = cfg.files_upload_v2\n',
])
def test_weak_receiver_method_value_is_reported(cell):
    kinds = [k for k, _m, _l in (_weak_run(cell).unprovable or [])]
    assert "method-as-value" in kinds, kinds


@pytest.mark.parametrize("cell", [
    "executor.submit(cfg.get, 'channel')\n",
    "record(cfg.token)\n",
    "executor.submit(cfg.update_run, run_id='r')\n",
    "cbs.append(cfg.get)\n",
])
def test_weak_receiver_non_slack_method_value_is_left_alone(cell):
    """The cost side: a weak binding is often NOT a Slack client at all
    (`cfg = get_slack_config()`), so reads and foreign methods must keep
    running. `update_run` matters specifically -- it hits "update" in
    _KNOWN_WRITE_RE and is MLflow's, not Slack's."""
    result = _weak_run(cell)
    assert not result.commented, result.source


# ─── an UNRESOLVED receiver in method-value position ───────────────────────
#
# SEND ESCAPE, pre-existing (base escapes identically). The direct-call path
# has branch B'' -- an unresolvable receiver whose METHOD is unmistakably a
# Slack Web API write is neutralised -- and the value path had no equivalent.
# So the same bound method was blocked when called and sent when passed:
#
#     bot.client.files_upload_v2(file="f")                  blocked
#     executor.submit(bot.client.files_upload_v2, file="f") SENT, no finding
#
# Three receiver shapes reach this state: an unbound config attribute, an
# attribute of a WEAK binding, and a client arriving as a function parameter.
# This is the same grid as the weak-receiver escape above, one cell over.

UNRESOLVED_PRELUDE = """
class _Sent(Exception):
    pass


class _SlackLike:
    def files_upload_v2(self, **kw):
        raise _Sent("files_upload_v2")

    def chat_postMessage(self, **kw):
        raise _Sent("chat_postMessage")

    def conversations_history(self, **kw):
        return {"messages": []}


class _Bot:
    client = _SlackLike()


bot = _Bot()


class _Cfg:
    client = _SlackLike()


def get_slack_config():
    return _Cfg()


real_client = _SlackLike()


class _Frame:
    def to_dict(self):
        return {}

    def sort_values(self, key):
        return self


df = _Frame()


class _Producer:
    def send(self, topic, value):
        return "queued"

    def files_upload_v2(self, **kw):
        return "kafka-not-slack"


producer = _Producer()


def KafkaProducer():
    return _Producer()


class _Tracking:
    def update_run(self, **kw):
        return "ok"


mc = _Tracking()


class _Executor:
    def submit(self, fn, *a, **k):
        return fn(*a, **k)


executor = _Executor()
"""


def _unresolved_run(cell, setup=""):
    bindings = new_bindings()
    if setup:
        update_bindings(setup, bindings)
    result = neutralize_cell(cell, bindings, include_generic=True)
    namespace = {}
    exec(compile(UNRESOLVED_PRELUDE, "<prelude>", "exec"), namespace)
    if setup:
        exec(compile(setup, "<setup>", "exec"), namespace)
    try:
        exec(compile(result.source, "<cell>", "exec"), namespace)
    except namespace["_Sent"] as exc:
        raise AssertionError("send executed: %s" % exc)
    except NameError:
        pass
    return result


@pytest.mark.parametrize("setup,cell", [
    # unbound config attribute
    ("", 'executor.submit(bot.client.files_upload_v2, file="f")\n'),
    # attribute of a WEAK binding
    ("s = get_slack_config()\n",
     'executor.submit(s.client.files_upload_v2, file="f")\n'),
    # client arriving as a function parameter
    ("", 'def go(c):\n    executor.submit(c.files_upload_v2, file="f")\n'
         "go(real_client)\n"),
    # same three shapes with a chat send
    ("", 'executor.submit(bot.client.chat_postMessage, channel="#a")\n'),
    ("", "cbs = []\ncbs.append(bot.client.files_upload_v2)\n"
         "for cb in cbs:\n    cb(file='f')\n"),
])
def test_unresolved_receiver_method_value_does_not_send(setup, cell):
    result = _unresolved_run(cell, setup)
    assert result.commented, result.source


@pytest.mark.parametrize("setup,cell", [
    ("", 'executor.submit(bot.client.files_upload_v2, file="f")\n'),
    ("s = get_slack_config()\n",
     'executor.submit(s.client.files_upload_v2, file="f")\n'),
    ("", 'def go(c):\n    executor.submit(c.files_upload_v2, file="f")\n'),
])
def test_unresolved_receiver_method_value_is_reported(setup, cell):
    kinds = [k for k, _m, _l in (_unresolved_run(cell, setup).unprovable or [])]
    assert "method-as-value" in kinds, kinds


def test_the_unresolved_finding_says_the_receiver_was_unresolvable():
    result = _unresolved_run(
        'executor.submit(bot.client.files_upload_v2, file="f")\n')
    msg = next(m for k, m, _l in result.unprovable if k == "method-as-value")
    assert "receiver could not be resolved" in msg, msg
    assert "Slack Web API write" in msg, msg


@pytest.mark.parametrize("setup,cell", [
    # ordinary library methods passed as values
    ("", "executor.submit(df.to_dict)\n"),
    ("", "executor.submit(df.sort_values, 'a')\n"),
    ("", "executor.submit(mc.update_run, run_id='r')\n"),
    ("", "executor.submit(producer.send, 'topic', b'x')\n"),
    # a Slack READ has no side effect and must keep running
    ("", "executor.submit(bot.client.conversations_history, channel='c')\n"),
    # RESOLVED as something else: the method name must not override the binding
    ("kafka = KafkaProducer()\n",
     'executor.submit(kafka.files_upload_v2, file="f")\n'),
])
def test_unresolved_fallback_does_not_fire_on_non_slack_values(setup, cell):
    """The cost side. The fallback keys off an unmistakable Slack API WRITE on
    an UNKNOWN receiver, so a resolved non-Slack binding and a Slack read both
    stay live."""
    result = _unresolved_run(cell, setup)
    assert not result.commented, result.source
