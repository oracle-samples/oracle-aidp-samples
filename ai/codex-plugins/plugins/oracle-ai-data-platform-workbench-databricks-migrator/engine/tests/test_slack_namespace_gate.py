"""Receiver evidence decides ONLY when it is conclusive.

INCIDENT (2026-09-25, job Example_Models_Monitoring, dbx 900000000000007,
notebook Example_Monitoring_Base_Prep.ipynb, code-cells #58 and #60; failed
Phase-2 child run aaaaaaaa-0000-4000-8000-000000000207):

    [slack-exec-guard:post-call_fix-cell-66] neutralised 2 line(s) at the
    execution boundary [download_artifacts, get_model_version_by_alias]
    NameError: name 'model_version' is not defined
    Cell 66/79: FAILED after 5 attempts    RESULT: PARTIAL | OK:58 Fail:1

THE TWO SITUATIONS

The guard has two clues: WHO the receiver is, and WHAT the method is. They
carry different weight depending on how good the receiver evidence is, and
conflating them is what produced both the incident and the first attempt to
fix it:

  * Receiver PROVEN Slack (`client = WebClient(token=t)` in scope) -- the
    method is irrelevant. Every method on a real client is a Slack action, so
    stay fail-closed. Requiring method evidence here is what made
    `client.canvases_delete(...)` executable: `canvases` is simply absent from
    _SLACK_RESOURCES, which is a SNAPSHOT of Slack's API, not a closed set.
    That call destroys customer content.

  * Receiver only BELIEVED Slack -- the method is the only tie-breaker. This
    is the incident: `%run` children bound `client` to a real WebClient, the
    parent rebound it to `MlflowClient()` inside `if ... in models:`, and
    _bind's conditional fail-safe discarded the contrary verdict (correctly --
    `if cleanup: client = None` followed by a real post must not leak). The
    binding survived on weaker evidence, and nothing recorded that, so it read
    is_slack=True / is_weak=False -- IDENTICAL to a proven client.
    `Bindings.stale` now records it, and `is_uncertain()` exposes it.

ONE DECISION, THREE CALL SITES

`_acts_on_slack()` is the single answer, used by the statement path, the
control-flow header path (`_header_write_method`) and the method-as-value path.
They were independent implementations; gating only the statement path made the
same line behave differently in `x.m()` and `if x.m():`, so the incident stayed
live in header position long after the statement form was fixed.

Also unrecoverable once triggered: job_migrate re-runs the guard after every
LLM repair (job_migrate.py:17768), so a false positive makes the repair loop
unwinnable -- the model restores the line, the guard re-comments it, five times.
"""
import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from slack_ast_neutralize import (  # noqa: E402
    _acts_on_slack,
    _is_slack_namespace_method,
    neutralize_cell,
    new_bindings,
    update_bindings,
)

# A %run child binding `client` to a genuine Slack client.
CHILD_BINDS_SLACK = """
from slack_sdk import WebClient
client = WebClient(token=TOKEN)
"""

# The parent then rebinds it inside a conditional -- the incident's shape. The
# contrary verdict is discarded by the fail-safe, so the binding survives but
# is no longer conclusive.
CONDITIONAL_REBIND = """
if 'METRICV4_RETRAIN' in models:
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
"""


def _uncertain_state():
    """Slack-believed but contradicted: the incident's binding."""
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    update_bindings(CONDITIONAL_REBIND, bindings)
    return bindings


def _definite_state():
    """Proven Slack, never contradicted."""
    bindings = new_bindings()
    update_bindings(CHILD_BINDS_SLACK, bindings)
    return bindings


def _commented(source, bindings):
    result = neutralize_cell(source, bindings, include_generic=True)
    return [ln for ln in result.source.splitlines() if "neutralised" in ln]


def _kinds(source, bindings):
    result = neutralize_cell(source, bindings, include_generic=True)
    return {kind for kind, _msg, _line in (result.unprovable or [])}


# ─── the state model itself ─────────────────────────────────────────────────

def test_conditional_rebind_marks_the_binding_uncertain():
    """The gap that made the incident invisible: before `stale`, this state was
    indistinguishable from a proven client."""
    bindings = _uncertain_state()
    assert bindings.is_slack("client")          # still believed Slack
    assert not bindings.is_weak("client")       # and NOT weak — the old model
    assert bindings.is_uncertain("client")      # but no longer conclusive


def test_proven_binding_is_not_uncertain():
    bindings = _definite_state()
    assert bindings.is_slack("client")
    assert not bindings.is_uncertain("client")


def test_a_real_constructor_restores_certainty():
    """A later genuine construction overrides earlier doubt."""
    bindings = _uncertain_state()
    assert bindings.is_uncertain("client")
    update_bindings("client = WebClient(token=TOKEN)\n", bindings)
    assert not bindings.is_uncertain("client")


def test_acts_on_slack_uses_the_method_only_when_uncertain():
    definite, uncertain = _definite_state(), _uncertain_state()
    # Proven receiver: the method does not matter.
    assert _acts_on_slack("client", "download_artifacts", definite)
    assert _acts_on_slack("client", "canvases_delete", definite)
    # Uncertain receiver: the method decides.
    assert not _acts_on_slack("client", "download_artifacts", uncertain)
    assert _acts_on_slack("client", "chat_postMessage", uncertain)
    # Not Slack at all: never.
    assert not _acts_on_slack("client", "chat_postMessage", new_bindings())


# ─── the incident: uncertain receiver + foreign method → LEFT RUNNING ───────

def test_cell66_mlflow_calls_survive():
    """The exact shape from Example_Monitoring_Base_Prep.ipynb."""
    src = """
if 'METRICV4_RETRAIN' in models:
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    model_version = client.get_latest_versions('METRICV4_RETRAIN', stages=['PRODUCTION'])[0]
    tenurev4_mlflow_run_id = model_version.run_id
    artifact_path = client.download_artifacts(tenurev4_mlflow_run_id, 'regression/x')
"""
    assert _commented(src, _definite_state()) == []


def test_the_collision_is_reported_not_silently_allowed():
    src = "art = client.download_artifacts(run_id, 'model')"
    assert "nonslack-method-on-slack-bound-name" in _kinds(src, _uncertain_state())


@pytest.mark.parametrize("call", [
    "model_version = client.get_model_version_by_alias('M', 'champion')",
    "mv = client.get_latest_versions('M', stages=['PRODUCTION'])[0]",
    "client.createOrReplaceTempView('t')",
    "df2 = client.sort_values('col')",
    "out = client.to_csv('/tmp/x.csv')",
    "resp = client.mount('https://', adapter)",
    "client.refresh_cache()",
    "client.log_metric('auc', 0.91)",
    # _KNOWN_WRITE_RE substring-matches these verbs; the resource prefix, not
    # the verb, has to be the discriminator.
    "client.update_run(run_id, status='FINISHED')",
    "client.create_experiment('x')",
    "client.delete_tag(run_id, 'k')",
    "client.set_terminated(run_id)",
])
def test_foreign_methods_survive_on_an_uncertain_receiver(call):
    assert _commented(call, _uncertain_state()) == [], call


# ─── BLOCKER 1: proven receiver stays fail-closed ───────────────────────────

@pytest.mark.parametrize("call", [
    # Families absent from _SLACK_RESOURCES. These MUST NOT depend on the
    # resource list being complete — canvases_delete destroys customer content.
    "client.canvases_delete(canvas_id='C1')",
    "client.canvases_edit(canvas_id='C1', changes=[])",
    "client.assistant_threads_setStatus(status='typing')",
    "client.agents_run(agent='a')",
    "client.im_open(user='U1')",
    # And the listed families.
    "client.chat_postMessage(channel='C1', text='x')",
    "client.files_upload_v2(file='f')",
    "client.conversations_invite(channel='C1', users='U1')",
    "client.views_open(trigger_id=t, view=v)",
])
def test_any_method_on_a_proven_client_is_neutralised(call):
    """Receiver evidence is conclusive, so the method is irrelevant. This is
    the fail-open regression the first version of this fix introduced."""
    assert _commented(call, _definite_state()), call


def test_slack_methods_are_neutralised_even_when_uncertain():
    """Doubt about the receiver does not excuse a method that IS Slack's."""
    for call in ("client.chat_postMessage(channel='C1', text='x')",
                 "client.files_upload_v2(file='f')",
                 "client.send(payload)",
                 "client.api_call('chat.postMessage', json=body)",
                 "client.notify_team('deploy done')",
                 "client.dispatch_alert(payload)"):
        assert _commented(call, _uncertain_state()), call


# ─── BLOCKER 2: every position agrees ───────────────────────────────────────

POSITIONS = {
    "statement": "{call}\n",
    "if header": "if {call}:\n    pass\n",
    "while header": "while {call}:\n    break\n",
    "for iterator": "for _x in {call}:\n    break\n",
    "assert": "assert {call}\n",
    "comprehension": "_xs = [v for v in {call}]\n",
}


@pytest.mark.parametrize("position", sorted(POSITIONS))
def test_foreign_method_survives_in_every_position(position):
    """`_header_write_method` was a second implementation of the statement
    path's rule, so the incident stayed live in `if`/`while` headers."""
    src = POSITIONS[position].format(
        call="client.get_latest_versions('m')")
    assert _commented(src, _uncertain_state()) == [], position


@pytest.mark.parametrize("position", sorted(POSITIONS))
def test_real_send_is_neutralised_in_every_position(position):
    src = POSITIONS[position].format(
        call="client.chat_postMessage(channel='C1', text='x')")
    assert _commented(src, _uncertain_state()), position


@pytest.mark.parametrize("position", sorted(POSITIONS))
def test_proven_client_is_neutralised_in_every_position(position):
    src = POSITIONS[position].format(call="client.canvases_delete(canvas_id='C1')")
    assert _commented(src, _definite_state()), position


# ─── unchanged behaviour ────────────────────────────────────────────────────

def test_conditional_rebind_to_literal_still_guards_a_real_post():
    """The fail-safe this fix relies on, and does not weaken: if the branch is
    not taken at runtime `client` still holds the Slack client."""
    src = """
if cleanup:
    client = None
client.chat_postMessage(channel="#c", text="must not leak")
"""
    assert _commented(src, _definite_state())


def test_slack_reads_remain_untouched():
    src = "hist = client.conversations_history(channel='C1')"
    assert _commented(src, _definite_state()) == []


def test_weak_binding_diagnosis_is_preserved():
    """A slack-named FACTORY is weak evidence and keeps its own, more specific
    finding rather than being absorbed by the namespace gate."""
    cell = ("from slack_sdk import WebClient\n"
            "cfg = get_slack_config()\n"
            "ch = cfg.get('channel')\n")
    result = neutralize_cell(cell, new_bindings(), include_generic=True)
    assert result.commented == 0
    assert "weak-binding-nonwrite" in [k for k, _m, _l in result.unprovable]


# ─── the namespace predicate ────────────────────────────────────────────────

def test_namespace_predicate_accepts_slack_shapes():
    for method in ("chat_postMessage", "files_upload_v2", "conversations_invite",
                   "views_open", "reactions_add", "admin_users_invite",
                   "send", "api_call", "post_message",
                   "notify_team", "dispatch_alert", "send_report"):
        assert _is_slack_namespace_method(method), method


def test_namespace_predicate_rejects_foreign_shapes():
    for method in ("download_artifacts", "get_latest_versions",
                   "get_model_version_by_alias", "sort_values", "to_csv",
                   "mount", "refresh_cache", "validate_schema", "log_metric",
                   "createOrReplaceTempView", "toPandas", "collect",
                   "reset_index", "to_dict", "update_run", "create_experiment"):
        assert not _is_slack_namespace_method(method), method


def test_namespace_predicate_matches_plain_verbs_whole_name_only():
    """"create" is a substring of createOrReplaceTempView; "post" is a whole
    method name. Only the latter is a send."""
    assert _is_slack_namespace_method("post")
    assert not _is_slack_namespace_method("createOrReplaceTempView")
    assert not _is_slack_namespace_method("postprocess")
