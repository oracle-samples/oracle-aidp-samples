"""A stage that crashed is not a stage that passed.

main() logs `exit_code: null` when a stage raises something outside its
caught tuple, or on Ctrl-C. _verdict(None) said UNKNOWN, but the phase
roll-up counted only PASS/DONE as passed and FAIL/HALT as failed and then
fell through to PASS: with `plan` and `ddl` both crashed and neither
plan.json nor ddl_plan.json written, PHASES.md -- uploaded after every
stage -- said "## Phase: planning — PASS" while the board for the same
directory showed both NOT_RUN. failed_runs left the crashed runs out too.

Separately, a workflow artifact that answered ok: false with no logged run
became "DONE (not logged)" and was counted as passed: the job-failure
override ran only for a logged run.
"""
import json

import pytest

from report.render import render_phase_report
from report.stages import phase_report
from report.tokens import record_stage_run


def _row(rep, stage):
    return next(p for p in rep["phases"] if p["stage"] == stage)


def _phase(rep, phase):
    return next(p for p in rep["phase_summary"] if p["phase"] == phase)


def test_a_crashed_stage_does_not_round_its_phase_up_to_pass(tmp_path):
    record_stage_run(tmp_path, "plan", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:01+00:00", None, None)
    record_stage_run(tmp_path, "ddl", "2026-09-24T10:01:00+00:00",
                     "2026-09-24T10:01:01+00:00", None, None)
    rep = phase_report(tmp_path)
    planning = _phase(rep, "planning")
    assert planning["verdict"] != "PASS"
    assert planning["verdict"].startswith("UNKNOWN"), planning["verdict"]
    assert planning["unknown"] == 2
    assert _row(rep, "plan")["unknown_runs"] == 1
    assert _row(rep, "plan")["result"].startswith("UNKNOWN")
    md = render_phase_report(rep)
    assert "Phase: planning — PASS" not in md
    assert "Phase: planning — UNKNOWN" in md


def test_a_crash_after_a_pass_is_still_not_a_pass(tmp_path):
    # The LAST run decides; an earlier success does not cover a crash.
    record_stage_run(tmp_path, "ddl", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:01+00:00", 0, None)
    record_stage_run(tmp_path, "ddl", "2026-09-24T10:05:00+00:00",
                     "2026-09-24T10:05:01+00:00", None, None)
    rep = phase_report(tmp_path)
    ddl = _row(rep, "ddl")
    assert ddl["result"].startswith("UNKNOWN")
    assert ddl["runs"] == 2 and ddl["unknown_runs"] == 1
    assert _phase(rep, "planning")["passed"] == 0


def test_a_failure_still_outranks_a_crash(tmp_path):
    record_stage_run(tmp_path, "plan", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:01+00:00", None, None)
    record_stage_run(tmp_path, "ddl", "2026-09-24T10:01:00+00:00",
                     "2026-09-24T10:01:01+00:00", 1, None)
    assert _phase(phase_report(tmp_path), "planning")["verdict"] == "FAIL"


def test_the_real_main_logs_a_crash_that_the_report_does_not_pass(
        tmp_path, monkeypatch):
    import snowmig
    monkeypatch.setenv("SNOWMIG_NO_STAGE_PUBLISH", "1")

    def _crash(args):
        raise KeyError("boom")

    def _interrupt(args):
        raise KeyboardInterrupt

    monkeypatch.setattr(snowmig, "cmd_plan", _interrupt)
    monkeypatch.setattr(snowmig, "cmd_ddl", _crash)
    with pytest.raises(KeyboardInterrupt):
        snowmig.main(["plan", "--out-dir", str(tmp_path)])
    with pytest.raises(KeyError):
        snowmig.main(["ddl", "--out-dir", str(tmp_path)])
    logged = [json.loads(l) for l in
              (tmp_path / "run_log.jsonl").read_text().splitlines()]
    assert [r["exit_code"] for r in logged] == [None, None]
    assert not (tmp_path / "plan.json").exists()
    rep = phase_report(tmp_path)
    assert _phase(rep, "planning")["verdict"] != "PASS"
    assert "Phase: planning — PASS" not in render_phase_report(rep)


def test_an_unlogged_failed_job_artifact_is_a_failure_not_done(tmp_path):
    (tmp_path / "run_snowmig_01_structure.json").write_text(json.dumps(
        {"job": "snowmig_01_structure", "terminal": True, "ok": False,
         "status": "FAILED"}))
    rep = phase_report(tmp_path)
    row = _row(rep, "structure-workflow")
    assert row["result"].startswith("FAIL"), row["result"]
    target = _phase(rep, "target")
    assert target["passed"] == 0
    assert target["verdict"] == "FAIL"
