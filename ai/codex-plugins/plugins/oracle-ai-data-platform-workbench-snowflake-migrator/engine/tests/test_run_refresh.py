"""`run --refresh`: re-read a run that exists, and submit nothing.

Live 2026-09-29: S10's poll budget ran out, so run_snowmig_01_structure.json
said STILL RUNNING while the job went on to SUCCESS; the stage board then
read a running structure job forever. And a copy started from the console
left no local record at all. `run --refresh` (or `--run-key`) reads the run
from AIDP and rewrites the record -- never submitting, cancelling or
resubmitting anything.
"""
import argparse
import json

import pytest

import snowmig
from target import provisioning

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


def _args(tmp_path, **over):
    base = dict(out_dir=str(tmp_path), datalake_ocid=OCID, workspace="ws",
                cluster_id=None, catalog=None, backend=None, config=None,
                job="snowmig_01_structure", job_key="job-k", param=None,
                poll_seconds=0, max_polls=3, cold_start_seconds=60,
                cold_start_restarts=1, refresh=True, run_key=None)
    base.update(over)
    return argparse.Namespace(**base)


class Existing:
    """AIDP with runs that already exist: {run_key: (status, job_key)}."""

    def __init__(self, runs):
        self.runs = runs
        self.ops: list[str] = []

    def __call__(self, op, **kw):
        self.ops.append(op)
        if op == "get_job_run":
            status, job = self.runs[kw["key"]]
            return {"state": {"status": status}, "jobKey": job}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": 1}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(f"refresh must not call {op}")


def _install(monkeypatch, fake):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)


def _stale(tmp_path, **extra):
    record = {"job": "snowmig_01_structure", "job_key": "job-k",
              "run_key": "run-7", "status": "RUNNING", "terminal": False,
              "ok": False, "restarts": [], "submitted_runs": ["run-7"]}
    record.update(extra)
    (tmp_path / "run_snowmig_01_structure.json").write_text(
        json.dumps(record), encoding="utf-8")


def _record(tmp_path):
    return json.loads((tmp_path / "run_snowmig_01_structure.json")
                      .read_text(encoding="utf-8"))


def test_a_stale_still_running_record_is_refreshed_to_success(
        tmp_path, monkeypatch):
    _stale(tmp_path)
    fake = Existing({"run-7": ("SUCCESS", "job-k")})
    _install(monkeypatch, fake)
    assert snowmig.cmd_run(_args(tmp_path)) == 0
    rec = _record(tmp_path)
    assert rec["run_key"] == "run-7" and rec["status"] == "SUCCESS"
    assert rec["terminal"] is True and rec["ok"] is True
    assert rec["refreshed_at"]
    md = (tmp_path / "RUN_snowmig_01_structure.md").read_text(
        encoding="utf-8")
    assert "**SUCCESS**" in md
    # Nothing was submitted, cancelled or resubmitted.
    assert not {"run_job", "cancel_job_run", "list_job_runs"} & set(fake.ops)


def test_a_console_started_run_is_recorded_by_its_key(tmp_path, monkeypatch):
    fake = Existing({"run-c": ("FAILED", "job-k")})
    _install(monkeypatch, fake)
    rc = snowmig.cmd_run(_args(tmp_path, refresh=False, run_key="run-c"))
    assert rc == 1, "a FAILED run is still a failure when refreshed"
    assert _record(tmp_path)["run_key"] == "run-c"


def test_a_run_of_another_job_is_refused_and_nothing_written(
        tmp_path, monkeypatch):
    fake = Existing({"run-x": ("SUCCESS", "some-other-job")})
    _install(monkeypatch, fake)
    with pytest.raises(snowmig.MissingTarget, match="belongs to job"):
        snowmig.cmd_run(_args(tmp_path, run_key="run-x"))
    assert not (tmp_path / "run_snowmig_01_structure.json").exists()


def test_refresh_with_no_record_and_no_key_says_what_to_pass(
        tmp_path, monkeypatch):
    _install(monkeypatch, Existing({}))
    with pytest.raises(snowmig.MissingTarget, match="--run-key"):
        snowmig.cmd_run(_args(tmp_path))


def test_a_refresh_still_running_stays_still_running(tmp_path, monkeypatch):
    _stale(tmp_path)
    _install(monkeypatch, Existing({"run-7": ("RUNNING", "job-k")}))
    assert snowmig.cmd_run(_args(tmp_path, max_polls=2)) == 0
    rec = _record(tmp_path)
    assert rec["terminal"] is False and rec["status"] == "RUNNING"


def test_the_same_run_keeps_its_cold_start_history(tmp_path, monkeypatch):
    restarts = [{"abandoned_run": "run-6", "new_run": "run-7",
                 "cancel_state": "CANCELED", "after_seconds": 120.0}]
    _stale(tmp_path, restarts=restarts, submitted_runs=["run-6", "run-7"])
    _install(monkeypatch, Existing({"run-7": ("SUCCESS", "job-k")}))
    assert snowmig.cmd_run(_args(tmp_path)) == 0
    rec = _record(tmp_path)
    assert rec["restarts"] == restarts
    assert rec["submitted_runs"] == ["run-6", "run-7"]
