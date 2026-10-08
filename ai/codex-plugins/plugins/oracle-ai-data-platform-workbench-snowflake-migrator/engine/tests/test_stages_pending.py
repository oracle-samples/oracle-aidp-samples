"""A job still going is RUNNING, its twin waits, and a partial copy is partial.

Three ways the board still read more than was true (review of this branch):

* A structure run whose poll budget ran out (`terminal: False`, the live S10
  case on 2026-09-29, where the job went on to SUCCESS) read DONE and
  `failed`, and `deploy` -- the other way to create the same objects -- was
  offered as the next step: a second, concurrent write.
* One schema's copy SUCCESS read as the copy done, though provision had
  registered a job per schema (`copy_jobs`) and the others had never run;
  reconcile was then unblocked.
* An unfinished run was labelled `failed` because `ok` is False until a run
  ends.
"""
import json

from plan.status import pipeline_status
from report.stages import build_stage_board, phase_report


def _write(d, name, data):
    (d / name).write_text(json.dumps(data), encoding="utf-8")


def _row(board, stage):
    return next(r for r in board["stages"] if r["stage"] == stage)


def _structure_running(d):
    _write(d, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "status": "RUNNING",
            "terminal": False, "ok": False})


def test_a_structure_run_still_going_is_running_not_done_or_failed(tmp_path):
    _structure_running(tmp_path)
    row = _row(build_stage_board(tmp_path), "structure-workflow")
    assert row["status"] == "RUNNING"
    assert row["failed"] is False


def test_deploy_waits_on_a_running_structure_job_and_is_never_next(tmp_path):
    _structure_running(tmp_path)
    board = build_stage_board(tmp_path)
    deploy = _row(board, "deploy")
    assert deploy["status"] == "PENDING"
    assert "structure-workflow" in deploy["found"]
    assert board["next_stage"] != "deploy"
    status = pipeline_status(board)
    assert "deploy" not in status["unblocked"]
    assert status["next"] != "deploy"
    # The running job itself is not something to start again either.
    assert "structure-workflow" not in status["unblocked"]


def test_an_unreadable_structure_status_also_holds_deploy(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "status": "UNREADABLE",
            "terminal": False, "ok": False, "status_unreadable": True})
    board = build_stage_board(tmp_path)
    assert _row(board, "deploy")["status"] == "PENDING"
    assert board["next_stage"] != "deploy"


def test_a_structure_run_that_succeeded_still_satisfies_deploy(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "status": "SUCCESS",
            "terminal": True, "ok": True})
    board = build_stage_board(tmp_path)
    assert _row(board, "structure-workflow")["status"] == "DONE"
    assert _row(board, "deploy")["status"] == "SATISFIED"


def test_a_failed_structure_run_is_still_failed(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "status": "FAILED",
            "terminal": True, "ok": False})
    row = _row(build_stage_board(tmp_path), "structure-workflow")
    assert row["status"] == "DONE" and row["failed"] is True


def _two_registered_one_run(d):
    _write(d, "provision_result.json", {"copy_jobs": [
        {"schema": "SALES", "job": "snowmig_02_copy_sales", "notebook": "n"},
        {"schema": "HR", "job": "snowmig_02_copy_hr", "notebook": "n"}]})
    _write(d, "run_snowmig_02_copy_sales.json",
           {"job": "snowmig_02_copy_sales", "status": "SUCCESS",
            "terminal": True, "ok": True})


def test_one_schema_copied_of_two_registered_is_partial(tmp_path):
    _two_registered_one_run(tmp_path)
    board = build_stage_board(tmp_path)
    row = _row(board, "copy-workflow")
    assert row["status"] == "PARTIAL"
    assert "snowmig_02_copy_hr: NOT RUN" in row["found"]
    assert row["attention"] is True and row["failed"] is False
    # Reconcile waits for the whole copy.
    assert "reconcile-workflow" not in pipeline_status(board)["unblocked"]


def test_every_registered_schema_copied_is_done(tmp_path):
    _two_registered_one_run(tmp_path)
    _write(tmp_path, "run_snowmig_02_copy_hr.json",
           {"job": "snowmig_02_copy_hr", "status": "SUCCESS",
            "terminal": True, "ok": True})
    row = _row(build_stage_board(tmp_path), "copy-workflow")
    assert row["status"] == "DONE" and row["attention"] is False


def test_no_copy_run_at_all_is_still_not_run(tmp_path):
    _write(tmp_path, "provision_result.json", {"copy_jobs": [
        {"schema": "SALES", "job": "snowmig_02_copy_sales", "notebook": "n"}]})
    assert _row(build_stage_board(tmp_path),
                "copy-workflow")["status"] == "NOT_RUN"


def test_the_phase_report_agrees_that_a_partial_copy_is_partial(tmp_path):
    _two_registered_one_run(tmp_path)
    rep = phase_report(tmp_path)
    row = next(r for r in rep["phases"] if r["stage"] == "copy-workflow")
    assert row["result"].startswith("PARTIAL")
    assert "snowmig_02_copy_hr" in row["result"]
    target = next(p for p in rep["phase_summary"] if p["phase"] == "target")
    assert target["verdict"] != "PASS"


def test_a_dry_run_provision_record_expects_no_copy_runs(tmp_path):
    _write(tmp_path, "provision_result.json", {"dry_run": True, "copy_jobs": [
        {"schema": "SALES", "job": "snowmig_02_copy_sales",
         "status": "would register"},
        {"schema": "HR", "job": "snowmig_02_copy_hr",
         "status": "would register"}]})
    _write(tmp_path, "run_snowmig_02_copy_sales.json",
           {"job": "snowmig_02_copy_sales", "status": "SUCCESS",
            "terminal": True, "ok": True})
    assert _row(build_stage_board(tmp_path),
                "copy-workflow")["status"] == "DONE"


def test_a_copy_job_provision_could_not_register_says_so(tmp_path):
    _write(tmp_path, "provision_result.json", {"dry_run": False, "copy_jobs": [
        {"schema": "SALES", "job": "snowmig_02_copy_sales",
         "status": "created"},
        {"schema": "HR", "job": "snowmig_02_copy_hr",
         "status": "failed, not registered"}]})
    _write(tmp_path, "run_snowmig_02_copy_sales.json",
           {"job": "snowmig_02_copy_sales", "status": "SUCCESS",
            "terminal": True, "ok": True})
    row = _row(build_stage_board(tmp_path), "copy-workflow")
    assert row["status"] == "PARTIAL"
    assert "failed, not registered" in row["found"]
    assert "registered, no run recorded" not in row["found"]
