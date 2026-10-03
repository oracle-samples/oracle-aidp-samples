"""A dropped task must tell the operator how to rebuild it, and with what.

Command, Email, Decision, Timer, Event-Wait/Raise, Control and Assignment
tasks have no AIDP job-task equivalent: an AIDP job runs notebooks, and
these run shell commands, send mail, wait on clocks and files, or branch on
workflow variables. Inventing a task for them would be fabricating
behaviour, so they stay reported -- that part is correct and stays.

What was wrong is that the report said only "dropped: reproduce it in AIDP
by hand". True, and unusable: the operator was not told what the task did,
how to replace it, or what it had been configured to do. Answering the last
one needs the export's <TASK>/<ATTRIBUTE> values, which the parser did not
read at all.
"""
from __future__ import annotations

import os

import pytest

from infa2aidp.generators.workflow_generator import (
    _TASK_REBUILD,
    WorkflowGenerator,
)
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "orchestration", "wf_task_attributes.xml"
)


@pytest.fixture(scope="module")
def review() -> list[str]:
    wf = InformaticaXMLParser().parse(FIXTURE).workflows[0]
    return WorkflowGenerator().generate(
        wf, {"s_load_main": "/nb/s_load_main"}
    ).not_translated


def _line(review: list[str], ttype: str) -> str:
    hit = [r for r in review if r.startswith(ttype)]
    assert hit, f"no review line for {ttype}: {review}"
    return hit[0]


# ── The parser now reads <TASK> attributes at all ──────────────────────

def test_task_attributes_are_parsed_off_the_task_definition():
    """A TASKINSTANCE only names a task; its settings live in <TASK>."""
    wf = InformaticaXMLParser().parse(FIXTURE).workflows[0]
    by_name = {t["name"]: (t.get("properties") or {}) for t in wf.tasks}
    assert by_name["cmd_archive"]["Command"] == "mv /infa/tgt/*.out /infa/archive/"
    assert by_name["em_load_failed"]["Email User Name"] == "dw-oncall@example.invalid"
    assert by_name["dec_month_end"]["Decision Expression"] == "$$RUN_MODE = 'MONTH_END'"
    assert by_name["tmr_wait"]["Absolute Time"] == "04:00:00"
    assert by_name["asg_batch"]["Assignment Expression"] == "$$BATCH_ID + 1"
    assert by_name["ctl_stop"]["Control Option"] == "Fail parent"


# ── Each type says what it did and how to replace it ───────────────────

@pytest.mark.parametrize("ttype", sorted(_TASK_REBUILD))
def test_every_known_task_type_has_rebuild_guidance(ttype):
    did, rebuild, attrs = _TASK_REBUILD[ttype]
    assert did and rebuild, f"{ttype} has no guidance"
    assert attrs, f"{ttype} names no attributes worth quoting"


def test_command_task_quotes_the_command_it_ran(review):
    line = _line(review, "COMMAND")
    assert "mv /infa/tgt/*.out /infa/archive/" in line, line
    assert "AIDP jobs run notebooks, not shell" in line


def test_email_task_quotes_recipient_and_subject(review):
    line = _line(review, "EMAIL")
    assert "dw-oncall@example.invalid" in line
    assert "Nightly load FAILED" in line
    # The trap worth naming: a retried cell re-sends the mail.
    assert "retried" in line


def test_decision_task_quotes_its_expression(review):
    line = _line(review, "DECISION")
    assert "$$RUN_MODE = 'MONTH_END'" in line
    assert "cannot branch on an expression" in line


def test_timer_task_quotes_its_time_and_says_to_use_the_schedule(review):
    line = _line(review, "TIMER")
    assert "04:00:00" in line
    assert "schedule" in line.lower()


def test_assignment_task_quotes_the_variable_and_expression(review):
    line = _line(review, "ASSIGNMENT")
    assert "$$BATCH_ID" in line
    assert "_param()" in line


def test_control_task_points_at_raising_from_the_notebook(review):
    line = _line(review, "CONTROL")
    assert "Fail parent" in line
    assert "Raise" in line


# ── Honesty when the export did not carry the settings ─────────────────

def test_a_task_with_no_definition_says_the_settings_are_unavailable(review):
    """The Event-Wait in the fixture deliberately has no <TASK>.

    Guidance must still appear -- the type is known -- but the line must not
    imply we know how it was configured.
    """
    line = _line(review, "EVENT-WAIT")
    assert "no <TASK> definition" in line
    assert "read them from PowerCenter" in line
    assert "file arrival is a trigger" in line


def test_no_review_line_is_multi_line(review):
    """Review items are rendered as a markdown list; a raw newline from a
    multi-line Command attribute would break the file and bury the rest."""
    for r in review:
        assert "\n" not in r, f"review item spans lines: {r!r}"


def test_sessions_are_not_reported_as_dropped(review):
    """The session has a notebook and becomes a job task."""
    assert not [r for r in review if "s_load_main" in r and "NOT translated" in r]


# ── A FAILED link is reported accurately ───────────────────────────────

def test_a_failed_only_link_says_the_task_will_also_run_on_success(review):
    """`$X.Status = FAILED` means "run on failure". The generated dependency
    is ALL_DONE, which runs on completion either way -- so the task now also
    runs on success. Saying it "runs whenever the upstream succeeds" (the
    old wording) described neither the condition nor ALL_DONE."""
    line = next((r for r in review if "FAILED" in r and "Link condition" in r), None)
    assert line, review
    assert "COMPLETES" in line
    assert "also run on success" in line
