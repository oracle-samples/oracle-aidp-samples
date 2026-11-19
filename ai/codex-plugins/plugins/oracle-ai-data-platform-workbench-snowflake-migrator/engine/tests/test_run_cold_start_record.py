"""RUN_<job>.md tells the truth about the last cold-start cancel, and counts
only the runs that were really submitted.

When every cold-start attempt is spent, `run` cancels the last run so it
does not hold the job's only slot. The console already checked whether that
cancel reached a terminal state. The persistent record did not: it always
said the run "was then cancelled (cancel state X). Nothing ran. Check the
cluster, then re-run." -- also when X was CANCELING, or the cancel raised.
That tells the operator a run which may still hold the slot is gone. The
re-run is then refused as a collision, or the old run starts later, unwatched,
and for a copy job that means rows written with nobody looking.

Both messages also counted runs as `len(restarts) + 1`. A restart whose
cancel was not confirmed submitted nothing, so "did not pick up any of 3
run(s)" was printed when two runs existed (submitted_runs held two keys).
"""
import argparse
import functools
import json

import pytest

import snowmig
from target import jobs, provisioning

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"


@pytest.fixture(autouse=True)
def _no_oci_config(tmp_path, monkeypatch):
    # _oci_runner reads the auth mode off the OCI config; point it at
    # nothing so these tests never touch the operator's file.
    monkeypatch.setenv("OCI_CONFIG_FILE", str(tmp_path / "no-oci-config"))


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(jobs, "watch_job",
                        functools.partial(jobs.watch_job,
                                          sleep=lambda _s: None))


def _args(tmp_path, **over):
    base = dict(out_dir=str(tmp_path), datalake_ocid=OCID, workspace="ws-fake",
                cluster_id=None, catalog=None, backend=None, config=None,
                job="snowmig_01_structure", job_key="job-fake", param=None,
                poll_seconds=30, max_polls=20, cold_start_seconds=60,
                cold_start_restarts=1)
    base.update(over)
    return argparse.Namespace(**base)


class NeverPickedUp:
    """Every run sits RUNNING with its task unstarted. `cancels` says what
    each cancel_job_run does, in order: "ok" ends the run CANCELED,
    "stuck" leaves it CANCELING, "raise" raises (and the run stays RUNNING).
    """

    def __init__(self, *cancels):
        self.cancels = list(cancels)
        self.state: dict[str, str] = {}
        self.submitted: list[str] = []

    def __call__(self, op, **kw):
        if op == "list_job_runs":
            return {"items": []}
        if op == "run_job":
            key = f"run-{len(self.submitted) + 1}"
            self.submitted.append(key)
            self.state[key] = "RUNNING"
            return {"key": key}
        if op == "get_job_run":
            return {"state": {"status": self.state[kw["key"]],
                              "stateMessage": ""}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": None}]}
        if op == "cancel_job_run":
            what = self.cancels.pop(0)
            if what == "raise":
                raise FileNotFoundError(2, "aidp not found")
            self.state[kw["run_key"]] = ("CANCELED" if what == "ok"
                                         else "CANCELING")
            return {}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)


def _run(tmp_path, monkeypatch, fake, **over):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    rc = snowmig.cmd_run(_args(tmp_path, **over))
    md = (tmp_path / "RUN_snowmig_01_structure.md").read_text(
        encoding="utf-8")
    record = json.loads((tmp_path / "run_snowmig_01_structure.json")
                        .read_text(encoding="utf-8"))
    return rc, md, record


@pytest.mark.parametrize("last_cancel", ["stuck", "raise"])
def test_an_unconfirmed_final_cancel_is_not_recorded_as_cancelled(
        tmp_path, monkeypatch, capsys, last_cancel):
    fake = NeverPickedUp("ok", last_cancel)
    rc, md, record = _run(tmp_path, monkeypatch, fake)
    assert rc == 1
    assert record["cold_start_exhausted"]["run"] == "run-2"
    assert record["cold_start_exhausted"]["cancel_state"] not in \
        jobs.TERMINAL_STATES
    assert "NOT confirmed cancelled" in md
    assert "Nothing ran" not in md
    assert "was then cancelled" not in md
    assert "cancel it by hand" in md.lower()
    assert "NOT confirmed cancelled" in capsys.readouterr().err


def test_a_confirmed_final_cancel_is_still_recorded_as_cancelled(
        tmp_path, monkeypatch):
    rc, md, record = _run(tmp_path, monkeypatch, NeverPickedUp("ok", "ok"))
    assert rc == 1
    assert record["cold_start_exhausted"]["cancel_state"] == "CANCELED"
    assert "was then cancelled" in md
    assert "NOT confirmed" not in md


def test_both_messages_count_the_runs_that_were_submitted(
        tmp_path, monkeypatch, capsys):
    # The first cancel raises: run-1 is kept (nothing resubmitted), then
    # cancelled cleanly and replaced by run-2, which is cancelled at the end.
    fake = NeverPickedUp("raise", "ok", "ok")
    rc, md, record = _run(tmp_path, monkeypatch, fake,
                          cold_start_restarts=2)
    assert rc == 1
    assert record["submitted_runs"] == ["run-1", "run-2"]
    assert len(record["restarts"]) == 2
    err = capsys.readouterr().err
    assert "any of 2 run(s)" in err, err
    assert "any of 2 run(s)" in md
    assert "3 run(s)" not in err + md
