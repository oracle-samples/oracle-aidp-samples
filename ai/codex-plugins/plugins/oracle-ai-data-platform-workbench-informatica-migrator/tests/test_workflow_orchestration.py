"""Orchestration: schedules, worklets, dropped task types, link conditions.

The defect these tests exist to prevent is not a crash. An earlier version
of ``WorkflowGenerator`` emitted a hardcoded ``0 0 2 * * ?`` whenever it
could not read a schedule, which was always, because the parser never read
``<SCHEDULER>``. Every migrated job deployed cleanly and ran at 02:00 UTC
regardless of its source schedule.

So the assertions below are mostly about what is NOT produced: no
fabricated cron, no silently-missing task, no silently-dropped condition.
"""
from __future__ import annotations

import os
import xml.etree.ElementTree as ET

import pytest

from infa2aidp.generators.workflow_generator import WorkflowGenerator
from infa2aidp.models import Workflow
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

# Deliberately NOT under fixtures/powercenter/, which is the pool of
# *mapping* fixtures that tests/test_aidp_output_validity.py sweeps to
# check generated notebooks. This is a workflow-only export: it has no
# <MAPPING> at all, so it would fail every assertion in that sweep while
# telling us nothing about notebooks.
FIXTURE = os.path.join(
    os.path.dirname(__file__), "fixtures", "orchestration", "workflow_orchestration.xml"
)


def _workflows() -> dict[str, Workflow]:
    result = InformaticaXMLParser().parse(FIXTURE)
    return {w.name: w for w in result.workflows}


# ── Parsing ────────────────────────────────────────────────────────────

def test_scheduler_element_is_parsed_at_all():
    """The root cause: <SCHEDULER> was never read, so every schedule was lost."""
    wf = _workflows()["wf_nightly_sales"]
    assert wf.scheduler, "the <SCHEDULER>/<SCHEDULEINFO> attributes must reach the model"
    assert wf.scheduler.get("DELTAVALUE") == "86400"
    assert wf.scheduler.get("STARTTIME") == "01/15/2026 03:30:00"


def test_every_task_instance_is_retained_not_just_sessions():
    """Non-session instances used to be parsed into a local dict and dropped."""
    wf = _workflows()["wf_nightly_sales"]
    types = {t["type"].upper() for t in wf.tasks}
    assert {"SESSION", "WORKLET", "COMMAND", "START"} <= types
    assert wf.sessions == ["s_load_customers", "s_load_orders"]


# ── Schedule conversion ────────────────────────────────────────────────

def test_daily_schedule_converts_to_the_source_time_not_a_default():
    tr = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {})
    job, review = tr.job, tr.not_translated
    assert job["schedule"]["quartzCronExpression"] == "0 30 3 * * ?"
    assert "0 0 2 * * ?" not in str(job), "the old fabricated 02:00 default must not appear"


def test_on_demand_workflow_gets_no_schedule_and_no_complaint():
    """'Run on demand' is faithfully represented by an unscheduled job."""
    tr = WorkflowGenerator().generate(_workflows()["wf_on_demand"], {})
    job, review = tr.job, tr.not_translated
    assert "schedule" not in job
    assert not [r for r in review if "schedule" in r.lower()]


def test_unconvertible_interval_yields_no_schedule_and_says_so():
    """Every 7 hours has no faithful cron, so none is emitted."""
    tr = WorkflowGenerator().generate(_workflows()["wf_unschedulable"], {})
    job, review = tr.job, tr.not_translated
    assert "schedule" not in job, "an interval a cron cannot express must not be approximated"
    assert any("not converted" in r and "DELTAVALUE=25200" in r for r in review), review


def test_absent_scheduler_never_becomes_a_default_cron():
    tr = WorkflowGenerator().generate(Workflow(name="wf_bare"), {})
    job, review = tr.job, tr.not_translated
    assert "schedule" not in job
    assert any("No <SCHEDULER>" in r for r in review)


@pytest.mark.parametrize(
    "delta,expected",
    [
        ("86400", "0 30 3 * * ?"),    # daily
        ("21600", "0 30 */6 * * ?"),  # every 6h -- divides 24
        ("900", "0 */15 * * * ?"),    # every 15min -- divides 60
    ],
)
def test_exact_intervals_convert(delta, expected):
    wf = Workflow(name="wf")
    wf.scheduler = {"DELTAVALUE": delta, "STARTTIME": "01/15/2026 03:30:00"}
    tr = WorkflowGenerator().generate(wf, {})
    job, review = tr.job, tr.not_translated
    assert job["schedule"]["quartzCronExpression"] == expected


@pytest.mark.parametrize("delta", ["25200", "0", "", "abc", "-3600"])
def test_inexact_or_junk_intervals_decline_rather_than_guess(delta):
    wf = Workflow(name="wf")
    wf.scheduler = {"DELTAVALUE": delta, "STARTTIME": "01/15/2026 03:30:00"}
    tr = WorkflowGenerator().generate(wf, {})
    job, review = tr.job, tr.not_translated
    assert "schedule" not in job
    assert review


# ── Review items for what cannot be translated ─────────────────────────

def test_worklet_is_reported_as_an_incomplete_dag():
    review = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {}).not_translated
    hits = [r for r in review if "Worklet" in r]
    assert hits, "a worklet whose sessions are absent must be reported"
    assert "wl_enrich" in hits[0]
    assert "incomplete" in hits[0].lower()


def test_dropped_task_type_is_named():
    review = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {}).not_translated
    assert any("COMMAND" in r and "cmd_archive" in r for r in review), review


def test_start_task_is_not_reported_as_dropped():
    """Every workflow has one; reporting it would drown the real items."""
    review = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {}).not_translated
    assert not [r for r in review if "START" in r.upper() and "dropped" in r]


def test_succeeded_link_condition_is_applied_not_reported():
    """``$s_load_customers.Status = SUCCEEDED`` on s_load_customers' own
    link is what dependsOn + runIf ALL_SUCCESS does, so nothing is lost.
    Conditions that ARE dropped are covered in
    test_workflow_worklets_params.py."""
    tr = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {})
    assert not [r for r in tr.not_translated if "condition" in r.lower()]
    tasks = {t["taskKey"]: t for t in tr.job["tasks"]}
    assert tasks["s_load_orders"]["dependsOn"] == [{"taskKey": "s_load_customers"}]
    assert tasks["s_load_orders"]["runIf"] == "ALL_SUCCESS"


def test_missing_notebook_for_a_session_is_reported():
    """The task still gets a placeholder path, but the caller is told."""
    tr = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {})
    job, review = tr.job, tr.not_translated
    assert any("No migrated notebook" in r and "s_load_customers" in r for r in review)


def test_a_fully_translatable_workflow_produces_no_review_items():
    """The absence of review items has to mean something."""
    wf = Workflow(name="wf_clean", sessions=["s_a"])
    wf.tasks = [{"name": "s_a", "type": "Session"}]
    wf.scheduler = {"DELTAVALUE": "86400", "STARTTIME": "01/15/2026 01:00:00"}
    tr = WorkflowGenerator().generate(wf, {"s_a": "/Migrated/nb_a"})
    job, review = tr.job, tr.not_translated
    assert review == []
    assert job["schedule"]["quartzCronExpression"] == "0 0 1 * * ?"


# ── The job definition stays deployable ────────────────────────────────

def test_review_items_never_leak_into_the_job_definition():
    """wf_def is POSTed verbatim to the AIDP jobs API."""
    tr = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {})
    job, review = tr.job, tr.not_translated
    assert review
    assert set(job) <= {
        "name", "description", "path", "tasks", "schedule", "maxConcurrentRuns",
    }, f"unexpected key(s) in the job definition: {sorted(job)}"


def test_job_definition_is_aidp_shaped_not_databricks_shaped():
    """The AIDP jobs API (POST /workspaces/{ws}/jobs) takes camelCase
    ``taskKey`` / ``type: NOTEBOOK_TASK`` / ``notebookPath`` / ``dependsOn``
    / ``runIf`` per task and ``schedule.quartzCronExpression`` + ``timezoneId``.
    The generator used to emit Databricks Jobs 2.x keys (task_key,
    notebook_task.notebook_path, depends_on, quartz_cron_expression), which
    the deployer then POSTed verbatim."""
    wf = Workflow(name="wf", sessions=["s_a", "s_b"])
    wf.tasks = [{"name": "s_a", "type": "Session"}, {"name": "s_b", "type": "Session"}]
    wf.dependencies = [{"from_task": "s_a", "to_task": "s_b", "condition": ""}]
    wf.scheduler = {"DELTAVALUE": "86400", "STARTTIME": "01/15/2026 01:00:00"}
    tr = WorkflowGenerator().generate(wf, {"s_a": "/Workspace/Migrated/nb_a.ipynb",
                                           "s_b": "/Workspace/Migrated/nb_b.ipynb"})
    tasks = {t["taskKey"]: t for t in tr.job["tasks"]}
    assert tasks["s_b"] == {
        # the link s_a -> s_b carries no condition, and PowerCenter runs s_b
        # once s_a COMPLETES -- see WorkflowGenerator._run_if_for
        "taskKey": "s_b", "type": "NOTEBOOK_TASK", "runIf": "ALL_DONE",
        "notebookPath": "/Workspace/Migrated/nb_b.ipynb",
        "dependsOn": [{"taskKey": "s_a"}],
    }
    assert tasks["s_a"]["dependsOn"] == []
    assert tr.job["schedule"] == {"quartzCronExpression": "0 0 1 * * ?", "timezoneId": "UTC",
                                  "pauseStatus": "PAUSED"}
    for stale in ("task_key", "notebook_task", "depends_on", "quartz_cron_expression",
                  "timezone_id", "max_concurrent_runs", "timeout_seconds"):
        assert stale not in str(tr.job)


# ── Assumptions are tracked apart from translation failures ────────────

def test_converted_schedule_reports_the_timezone_as_an_assumption():
    """STARTTIME has no zone, so the emitted timezone is a guess.

    PowerCenter records STARTTIME in the Integration Service's local time.
    "03:30" could be 03:30 anywhere. We must put something in the job, so
    UTC goes in -- and because that is assumed rather than derived, it is
    reported. A job at the right minute of the wrong hour is the same
    class of defect as the fabricated cron, just smaller.
    """
    wf = Workflow(name="wf", sessions=["s"])
    wf.tasks = [{"name": "s", "type": "Session"}]
    wf.scheduler = {"DELTAVALUE": "86400", "STARTTIME": "01/15/2026 03:30:00"}
    tr = WorkflowGenerator().generate(wf, {"s": "/nb"})

    assert tr.job["schedule"]["timezoneId"] == "UTC"
    assert any("ASSUMPTION" in a and "timezone" in a for a in tr.assumptions), tr.assumptions


def test_an_assumption_is_not_a_translation_failure():
    """Otherwise every scheduled workflow looks incomplete and the
    'nothing was lost' signal becomes worthless."""
    wf = Workflow(name="wf", sessions=["s"])
    wf.tasks = [{"name": "s", "type": "Session"}]
    wf.scheduler = {"DELTAVALUE": "86400", "STARTTIME": "01/15/2026 03:30:00"}
    tr = WorkflowGenerator().generate(wf, {"s": "/nb"})

    assert tr.not_translated == []
    assert tr.assumptions
    assert tr.needs_review is True


def test_an_unscheduled_job_carries_no_timezone_assumption():
    """No schedule emitted means no timezone was assumed."""
    tr = WorkflowGenerator().generate(_workflows()["wf_unschedulable"], {})
    assert "schedule" not in tr.job
    assert not [a for a in tr.assumptions if "timezone" in a]


# ---------------------------------------------------------------------------
# runIf is decided per task, from that task's own links
# ---------------------------------------------------------------------------

def _two_session_wf(condition: str):
    wf = Workflow(name="wf_rif", sessions=["s_a", "s_b"])
    wf.tasks = [{"name": "s_a", "type": "Session"}, {"name": "s_b", "type": "Session"}]
    wf.dependencies = [{"from_task": "s_a", "to_task": "s_b", "condition": condition}]
    wf.scheduler = {}
    tr = WorkflowGenerator().generate(wf, {"s_a": "/Workspace/Migrated/a.ipynb",
                                           "s_b": "/Workspace/Migrated/b.ipynb"})
    return {t["taskKey"]: t for t in tr.job["tasks"]}, tr


def test_an_unconditional_link_gets_all_done_matching_powercenter():
    """PowerCenter runs the downstream task once the upstream COMPLETES,
    succeeded or failed. Emitting ALL_SUCCESS was a silent tightening: safer
    pipeline, different behaviour, and the divergence is invisible in the
    notebooks when a reconciliation later disagrees."""
    tasks, _ = _two_session_wf("")
    assert tasks["s_b"]["runIf"] == "ALL_DONE"


def test_a_succeeded_condition_still_gets_all_success():
    """`$s_a.Status = SUCCEEDED` asks for success explicitly, so it is
    honoured rather than widened."""
    tasks, tr = _two_session_wf("$s_a.Status = SUCCEEDED")
    assert tasks["s_b"]["runIf"] == "ALL_SUCCESS"
    assert not [r for r in tr.not_translated if "condition" in r.lower()]


def test_a_task_with_no_incoming_link_is_unchanged():
    """It always runs, so this change should not touch it."""
    tasks, _ = _two_session_wf("")
    assert tasks["s_a"]["runIf"] == "ALL_SUCCESS"
    assert tasks["s_a"]["dependsOn"] == []


def test_the_all_done_choice_is_reported_as_an_assumption():
    """Faithful and permissive both need saying: the downstream task will
    run on a failed upstream's output."""
    _, tr = _two_session_wf("")
    hits = [a for a in tr.assumptions if "ALL_DONE" in a]
    assert hits, tr.assumptions
    assert "failed" in hits[0] and "ALL_SUCCESS" in hits[0]


# ── Declared schedule timezone ─────────────────────────────────────────
#
# PowerCenter records STARTTIME in the Integration Service's local time and
# stores no zone, so the export cannot supply it. The generator used to
# always write UTC and report it as an assumption. An operator who knows the
# zone can now declare it, which turns the schedule from plausible into
# correct -- a job firing at the right minute of the wrong hour is a silent
# defect, and "03:30 UTC" is wrong by five hours for most US estates.

def test_schedule_timezone_defaults_to_utc_and_is_reported_as_an_assumption():
    tr = WorkflowGenerator().generate(_workflows()["wf_nightly_sales"], {})
    assert tr.job["schedule"]["timezoneId"] == "UTC"
    assert any("ASSUMPTION" in a and "timezone" in a.lower()
               for a in tr.assumptions), tr.assumptions


def test_a_declared_timezone_reaches_the_job_definition():
    tr = WorkflowGenerator(schedule_timezone="America/New_York").generate(
        _workflows()["wf_nightly_sales"], {})
    assert tr.job["schedule"]["timezoneId"] == "America/New_York"
    # The hour itself is unchanged -- STARTTIME is the wall-clock time in
    # that zone, so only the zone was ever missing.
    assert tr.job["schedule"]["quartzCronExpression"] == "0 30 3 * * ?"


def test_a_declared_timezone_is_no_longer_an_assumption():
    """The point of declaring it: it stops being a thing to review."""
    tr = WorkflowGenerator(schedule_timezone="America/New_York").generate(
        _workflows()["wf_nightly_sales"], {})
    assert not [a for a in tr.assumptions if "timezone" in a.lower()], (
        f"timezone still reported as an assumption after being declared: "
        f"{tr.assumptions}"
    )


def test_a_declared_timezone_still_creates_the_job_paused():
    """Declaring the zone removes the timezone doubt, not every reason to
    check -- and a job that starts firing on deploy is an outward-facing
    side effect nobody asked for."""
    tr = WorkflowGenerator(schedule_timezone="Europe/London").generate(
        _workflows()["wf_nightly_sales"], {})
    assert tr.job["schedule"]["pauseStatus"] == "PAUSED"


def test_a_bogus_timezone_is_refused_at_construction(monkeypatch):
    """AIDP rejects an unknown timezoneId, so a typo would otherwise produce
    a whole migration's worth of undeployable jobs.

    Run against a tz database whatever the host has: without one (Windows
    with no tzdata) only a name's shape can be checked, and a plausible
    typo has the right shape -- see validate_schedule_timezone."""
    import pytest
    from infa2aidp.generators import workflow_generator as wg
    monkeypatch.setattr(wg, "_known_zones",
                        lambda: frozenset({"America/New_York", "Europe/London", "UTC"}))
    with pytest.raises(ValueError, match="not an IANA timezone"):
        WorkflowGenerator(schedule_timezone="America/New_Yrok")


def test_utc_can_be_declared_explicitly():
    """An estate that really did run UTC should be able to say so and get a
    clean review, rather than being told its own answer is an assumption."""
    tr = WorkflowGenerator(schedule_timezone="UTC").generate(
        _workflows()["wf_nightly_sales"], {})
    assert tr.job["schedule"]["timezoneId"] == "UTC"
    assert not [a for a in tr.assumptions if "timezone" in a.lower()]
