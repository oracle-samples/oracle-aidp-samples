"""The copy runs that actually exist must be on the board.

Once a plan is pushed, provision does not create the generic
snowmig_02_copy_schema job: it creates one snowmig_02_copy_<schema> job per
schema of the approved plan, and `run --job snowmig_02_copy_sales` writes
run_snowmig_02_copy_sales.json. The board's copy-workflow row read only the
fixed name run_snowmig_02_copy_schema.json, and stage_for matched only that
exact job name, so every per-schema run was logged as a bare `run`.

A copy that moved rows, or one that FAILED, was therefore invisible: the row
said NOT_RUN and was offered under "Unblocked now", needs_attention was
empty, the phase report said "SKIPPED (optional)" with the run unattributed,
and reconcile-workflow read DONE while its only prerequisite read NOT_RUN.
"""
import json

from plan.status import pipeline_status
from report.render import render_stages
from report.stages import STAGES, build_stage_board, phase_report, stage_for
from report.tokens import record_stage_run
from target.provisioning import COPY_JOB_PREFIX, copy_job_specs


def _write(d, name, data):
    (d / name).write_text(json.dumps(data), encoding="utf-8")


def _copy_row(board):
    return next(r for r in board["stages"] if r["stage"] == "copy-workflow")


def _two_schema_runs(tmp_path):
    _write(tmp_path, "run_snowmig_02_copy_sales.json",
           {"job": "snowmig_02_copy_sales", "terminal": True, "ok": False,
            "status": "FAILED"})
    _write(tmp_path, "run_snowmig_02_copy_hr.json",
           {"job": "snowmig_02_copy_hr", "terminal": True, "ok": True,
            "status": "SUCCESS"})


def test_a_per_schema_copy_run_is_the_copy_workflow():
    assert stage_for("run", "snowmig_02_copy_sales") == "copy-workflow"
    # The generic job, before a plan is pushed, is still the same stage.
    assert stage_for("run", "snowmig_02_copy_schema") == "copy-workflow"
    # And the names are the ones provision actually creates.
    for spec in copy_job_specs(["SALES", "hr"]):
        assert stage_for("run", spec["name"]) == "copy-workflow", spec["name"]


def test_the_board_reads_the_prefix_provision_uses():
    spec = next(s for s in STAGES if s["stage"] == "copy-workflow")
    assert spec["job_prefix"] == COPY_JOB_PREFIX


def test_per_schema_copy_runs_are_on_the_board_and_a_failure_needs_attention(
        tmp_path):
    _two_schema_runs(tmp_path)
    board = build_stage_board(tmp_path)
    row = _copy_row(board)
    assert row["status"] != "NOT_RUN"
    assert row["attention"] is True
    assert row["failed"] is True, "a failed copy did not do its work"
    assert "snowmig_02_copy_sales: **FAILED**" in row["found"], row["found"]
    assert "snowmig_02_copy_hr: SUCCESS" in row["found"], row["found"]
    assert "copy-workflow" in board["needs_attention"]
    md = render_stages(board)
    assert "snowmig_02_copy_sales" in md


def test_a_failed_copy_does_not_unblock_reconcile_or_read_as_not_run(tmp_path):
    # The copy's own prerequisites are met, so a board that cannot see the
    # runs offers the copy as the thing to run next.
    _write(tmp_path, "deploy_result.json", {"statement_count": 2,
                                            "verified": 2})
    _write(tmp_path, "data_options.json", {"choice": None})
    _two_schema_runs(tmp_path)
    status = pipeline_status(build_stage_board(tmp_path))
    assert "copy-workflow" not in status["complete"]
    # A failed copy may be offered again, but as a failure to look at, not
    # as a stage that never ran.
    assert "copy-workflow" in status["needs_attention"]
    assert "reconcile-workflow" in status["blocked"]


def test_all_per_schema_copies_succeeding_is_done_and_clean(tmp_path):
    for schema in ("sales", "hr"):
        _write(tmp_path, f"run_snowmig_02_copy_{schema}.json",
               {"job": f"snowmig_02_copy_{schema}", "terminal": True,
                "ok": True, "status": "SUCCESS"})
    row = _copy_row(build_stage_board(tmp_path))
    assert row["status"] == "DONE"
    assert row["attention"] is False and row["failed"] is False


def test_the_phase_report_attributes_and_fails_a_per_schema_copy(tmp_path):
    _two_schema_runs(tmp_path)
    record_stage_run(tmp_path, "run", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:10:00+00:00", 1, None,
                     job="snowmig_02_copy_sales")
    rep = phase_report(tmp_path)
    copy = next(p for p in rep["phases"] if p["stage"] == "copy-workflow")
    assert copy["runs"] == 1
    assert rep["unattributed_runs"] == 0
    assert copy["result"].startswith("FAIL"), copy["result"]
    assert "snowmig_02_copy_sales" in copy["result"]


def test_an_unlogged_per_schema_copy_artifact_is_not_skipped(tmp_path):
    _write(tmp_path, "run_snowmig_02_copy_hr.json",
           {"job": "snowmig_02_copy_hr", "terminal": True, "ok": True,
            "status": "SUCCESS"})
    copy = next(p for p in phase_report(tmp_path)["phases"]
                if p["stage"] == "copy-workflow")
    assert copy["result"] == "DONE (not logged)"


def test_the_reports_name_the_per_schema_copy_jobs():
    from report.render import DATA_OPTIONS_NOTE, architecture_section
    text = "\n".join(architecture_section({}))
    assert "snowmig_02_copy_<schema>" in text
    assert "snowmig_02_copy_<schema>" in DATA_OPTIONS_NOTE
