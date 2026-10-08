"""Only a twin that did its work satisfies the other path.

Two stages that produce the same result are twins (`alternative_to`):
assess/ingest, structure-workflow/deploy. Having done either satisfies both.
The board decided "done" from the artifact's EXISTENCE alone, so a dry run,
a FAILED or unrecognised job run, or an unreadable artifact marked its twin
"SATISFIED" with no attention -- counted complete, unblocking what came
after -- against the board's own rule that a dry run satisfies no
prerequisite. With only dry runs of provision and deploy, STAGES.md said
"1 of 23 phase(s) complete", offered teardown, and the diagram painted
PROVISION, STRUCTURE_WORKFLOW and DEPLOY green when nothing was created.
"""
import json

from plan.status import pipeline_status
from report.diagram import phase_diagram
from report.stages import build_stage_board, phase_report


def _write(d, name, data):
    (d / name).write_text(json.dumps(data), encoding="utf-8")


def _rows(board):
    return {r["stage"]: r for r in board["stages"]}


def _done_class(diagram):
    return [l for l in diagram.splitlines() if l.rstrip().endswith(" done")]


def test_a_dry_run_satisfies_no_twin_and_is_not_painted_done(tmp_path):
    _write(tmp_path, "provision_result.json", {"dry_run": True})
    _write(tmp_path, "deploy_result.json", {"dry_run": True,
                                            "statement_count": 12})
    board = build_stage_board(tmp_path)
    rows = _rows(board)
    assert rows["structure-workflow"]["status"] == "NOT_RUN"
    assert "dry run" in rows["structure-workflow"]["found"]
    status = pipeline_status(board)
    assert status["complete"] == []
    assert "teardown" in status["blocked"]
    assert _done_class(phase_diagram(board)) == [], \
        "a dry run created nothing and is not painted green"


def test_a_failed_workflow_does_not_satisfy_deploy(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": False,
            "status": "FAILED"})
    board = build_stage_board(tmp_path)
    deploy = _rows(board)["deploy"]
    assert deploy["status"] != "SATISFIED"
    assert "not satisfied" in deploy["found"]
    assert "structure-workflow" in deploy["found"]
    assert deploy["attention"] is True
    status = pipeline_status(board)
    assert "deploy" not in status["complete"]
    assert "structure-workflow or deploy" in status["blocked"]["copy-workflow"]
    assert "teardown" in status["blocked"]


def test_an_unrecognised_or_unreadable_workflow_satisfies_nothing(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": False, "ok": False,
            "unrecognised": True, "status": "WEIRD"})
    assert _rows(build_stage_board(tmp_path))["deploy"]["status"] != "SATISFIED"
    (tmp_path / "run_snowmig_01_structure.json").write_text("{not json")
    board = build_stage_board(tmp_path)
    assert _rows(board)["deploy"]["status"] != "SATISFIED"
    # Not satisfied on the board, and not "done" for unblocking either: an
    # artifact nothing can be read from unblocks nothing. It left teardown
    # unblocked while an absent one kept it blocked.
    status = pipeline_status(board)
    assert "structure-workflow or deploy" in status["blocked"]["teardown"]
    assert "structure-workflow or deploy" in status["blocked"]["copy-workflow"]


def test_an_unreadable_copy_run_does_not_unblock_reconcile(tmp_path):
    # One schema's copy ran; another's artifact is unreadable. The copy
    # phase is not established, so reconcile stays blocked on it.
    _write(tmp_path, "run_snowmig_02_copy_sales.json",
           {"job": "snowmig_02_copy_sales", "terminal": True, "ok": True,
            "status": "SUCCESS"})
    (tmp_path / "run_snowmig_02_copy_hr.json").write_text("{not json")
    status = pipeline_status(build_stage_board(tmp_path))
    assert "reconcile-workflow" not in status["unblocked"]
    assert status["blocked"]["reconcile-workflow"] == ["copy-workflow"]


def test_a_twin_that_did_its_work_still_satisfies(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": True,
            "status": "SUCCESS"})
    board = build_stage_board(tmp_path)
    deploy = _rows(board)["deploy"]
    assert deploy["status"] == "SATISFIED"
    assert deploy["attention"] is False
    assert "deploy" in pipeline_status(board)["complete"]


def test_the_phase_report_marks_a_satisfied_twin(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": True,
            "status": "SUCCESS"})
    rep = {p["stage"]: p for p in phase_report(tmp_path)["phases"]}
    assert rep["deploy"]["result"].startswith("SATISFIED")
    assert "structure-workflow" in rep["deploy"]["result"]


def test_the_phase_report_does_not_satisfy_from_a_failed_twin(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": False,
            "status": "FAILED"})
    rep = {p["stage"]: p for p in phase_report(tmp_path)["phases"]}
    assert rep["deploy"]["result"] == "NOT_RUN"
