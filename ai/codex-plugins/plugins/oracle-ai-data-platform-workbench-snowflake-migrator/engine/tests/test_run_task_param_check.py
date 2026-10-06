"""`run` checks a job's task parameters before a run is paid for.

A task parameter reaches the notebook by name, through getParameter and the
spellings the PARAMS cell tries. A name none of them matches is read by
nothing -- `dryRn=true` leaves dry-run False, a real write -- and a value
the stage refuses fails only after minutes of job start-up. `run` reads the
job definition first and refuses both, submitting nothing.
"""
import argparse

import pytest

import snowmig
from target import provisioning
from target.provision_api import build_provision_command
from target.stage_notebooks import STAGES, build_stage_notebook, param_spellings

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


def _args(tmp_path, job="snowmig_02_copy_sales", **over):
    base = dict(out_dir=str(tmp_path), datalake_ocid=OCID, workspace="ws",
                cluster_id=None, catalog=None, backend=None, config=None,
                job=job, job_key="job-k", param=None, poll_seconds=0,
                max_polls=1, cold_start_seconds=60, cold_start_restarts=0,
                refresh=False, run_key=None)
    base.update(over)
    return argparse.Namespace(**base)


class Job:
    """A job whose definition carries `params`; runs succeed at once."""

    def __init__(self, params, *, unreadable=False):
        self.params, self.unreadable = params, unreadable
        self.ops: list[str] = []

    def __call__(self, op, **kw):
        self.ops.append(op)
        if op == "get_job":
            if self.unreadable:
                raise RuntimeError("503")
            return {"tasks": [{"parameters": [
                {"name": k, "value": v} for k, v in self.params.items()]}]}
        if op == "list_job_runs":
            return {"items": []}
        if op == "run_job":
            return {"key": "run-1"}
        if op == "get_job_run":
            return {"state": {"status": "SUCCESS"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": 1}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)


def _install(monkeypatch, fake):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)


def test_the_job_definition_is_read_with_get_job():
    cmd = build_provision_command("oci_raw", "get_job", OCID,
                                  workspace="ws", job_key="k1")
    assert cmd[:4] == ["oci", "raw-request", "--http-method", "GET"]
    assert cmd[5].endswith("/workspaces/ws/jobs/k1")


def test_a_misspelled_parameter_name_is_refused_and_nothing_submitted(
        tmp_path, monkeypatch):
    fake = Job({"schema": "SALES", "dryRn": "true"})
    _install(monkeypatch, fake)
    with pytest.raises(snowmig.MissingTarget) as err:
        snowmig.cmd_run(_args(tmp_path))
    assert "`dryRn`" in str(err.value)
    assert "dry-run" in str(err.value)          # the suggestion
    assert "run_job" not in fake.ops


def test_a_value_the_stage_refuses_is_refused_before_the_run(
        tmp_path, monkeypatch):
    fake = Job({"schema": "SALES", "mode": "apend"})
    _install(monkeypatch, fake)
    with pytest.raises(snowmig.MissingTarget, match="mode=apend"):
        snowmig.cmd_run(_args(tmp_path))
    assert "run_job" not in fake.ops


@pytest.mark.parametrize("name", ["dry-run", "dry_run", "dryRun", "dryrun",
                                  "DRY_RUN"])
def test_every_spelling_the_notebook_reads_is_accepted(
        tmp_path, monkeypatch, name):
    fake = Job({"schema": "SALES", name: "true"})
    _install(monkeypatch, fake)
    assert snowmig.cmd_run(_args(tmp_path)) == 0
    assert "run_job" in fake.ops


def test_an_unreadable_job_definition_is_said_and_does_not_block(
        tmp_path, monkeypatch, capsys):
    fake = Job({}, unreadable=True)
    _install(monkeypatch, fake)
    assert snowmig.cmd_run(_args(tmp_path)) == 0
    assert "NOT checked" in capsys.readouterr().err


def test_a_job_that_is_no_stage_is_not_checked(tmp_path, monkeypatch):
    fake = Job({"anything": "x"})
    _install(monkeypatch, fake)
    assert snowmig.cmd_run(_args(tmp_path, job="someone_elses_job")) == 0
    assert "get_job" not in fake.ops


def test_the_notebook_tries_exactly_the_spellings_run_accepts():
    stage = next(s for s in STAGES if s.key == "copy_schema")
    cell = "".join(build_stage_notebook(
        stage, overrides={"target-catalog": "l", "schema": "S"}
    )["cells"][1]["source"])
    scope = {}
    exec(compile(cell.split("def _workflow_param")[0], "<cell>", "exec"),
         scope)
    for name in stage.params:
        assert scope["_spellings"](name) == param_spellings(name), name
