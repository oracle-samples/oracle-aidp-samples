"""`snowmig jobs`: generated jobs written offline, registered only on ask.

The generator (target/generated_jobs.py) turns the plan's table snapshots
and the census's task graphs into notebooks and job specs. This is the
stage that writes them down -- generated_jobs.json, GENERATED_JOBS.md and
one notebook per task under generated_jobs/ -- and, only with --register,
creates the jobs in AIDP through the same provisioning calls `provision`
uses: `create_ws_folder`, `upload_ws_file` (NOTEBOOK), `list_ws_objects`,
`create_job`, `list_jobs` (live-verified shapes, 2026-09-16).

Two things --register never does. It never sends a schedule: the job
body has no `schedule` key, whatever the spec proposes, and the output says
the cadence is recorded, not applied. And it never adopts or overwrites a
job that already has the name: that is someone else's workflow until
proven otherwise.
"""
import json

import pytest

import snowmig
from plan.build import build_plan
from target import provisioning
from test_generated_jobs_tasks import LIVE_BODY, STAGING, _task
from test_plan_snapshots import (ORDERS, _census, _dt, _dt_census, _inv,
                                 _mv, _rec)

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


def _estate(tmp_path, *, tasks=True):
    objs = [_dt_census()]
    if tasks:
        gold = _task("TSK_REFRESH_GOLD", LIVE_BODY, schedule="60 MINUTE")
        gold["writes"] = [STAGING]      # as the census reads the body
        objs.append(gold)
        objs.append(_task("TSK_CALL", "CALL DB.CORE.P()",
                          schedule="USING CRON 0 9 * * * UTC"))
    census = _census(*objs)
    inv = _inv(_rec(ORDERS), _rec(STAGING), _dt(), _mv(), census=census)
    plan = build_plan(inv, {"edges": []})
    plan["census"] = census
    (tmp_path / "inventory.json").write_text(json.dumps(inv),
                                             encoding="utf-8")
    (tmp_path / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("decisions:\n  allow_new_objects: true\n",
                   encoding="utf-8")
    return str(cfg)


def _no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("snowmig jobs must not touch AIDP without "
                             "--register")
    monkeypatch.setattr(provisioning, "make_provision_call", refuse)


def _read(tmp_path, name):
    return (tmp_path / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------- offline

def test_offline_by_default_and_writes_every_artifact(tmp_path, monkeypatch,
                                                      capsys):
    cfg = _estate(tmp_path)
    _no_network(monkeypatch)
    assert snowmig.main(["jobs", "--out-dir", str(tmp_path),
                         "--config", cfg]) == 0
    res = json.loads(_read(tmp_path, "generated_jobs.json"))
    assert res["registered"] is False
    assert "notebooks" not in res, "notebooks are files, not JSON blobs"
    for path in res["notebook_files"]:
        nb = json.loads((tmp_path / path).read_text(encoding="utf-8"))
        assert nb["metadata"]["snowmig"]["generated"] is True
    kinds = sorted(j["kind"] for j in res["jobs"])
    assert kinds == ["refresh", "refresh", "task_graph", "task_graph"]
    out = capsys.readouterr().out
    assert "nothing registered" in out.lower()
    assert "--register" in out


def test_generated_jobs_md_says_what_is_and_is_not_generated(tmp_path,
                                                            monkeypatch):
    cfg = _estate(tmp_path)
    _no_network(monkeypatch)
    snowmig.main(["jobs", "--out-dir", str(tmp_path), "--config", cfg])
    md = _read(tmp_path, "GENERATED_JOBS.md")
    assert md.startswith("# Generated jobs")
    assert "MANUAL" in md and "not applied" in md
    assert "## Refresh jobs" in md and "DB.CORE.DT_ORDER_ROLLUP" in md
    assert "TARGET_LAG 1 day" in md
    assert "## Task-graph jobs" in md
    assert "stub: calls DB.CORE.P" in md
    assert "`0 0 * * * ?` (PAUSED" in md
    # The plan's "loads that stop" meet the job that replaces the load.
    assert "## Loads that stop at cutover" in md
    section = md[md.index("## Loads that stop at cutover"):]
    assert STAGING in section and "snowmig_task_db_core_tsk_refresh_gold" \
        in section


def test_a_missing_plan_is_an_error_not_an_empty_result(tmp_path, capsys):
    assert snowmig.main(["jobs", "--out-dir", str(tmp_path)]) == 1
    assert "plan.json" in capsys.readouterr().err


def test_stale_notebooks_from_an_earlier_run_are_removed(tmp_path,
                                                         monkeypatch):
    cfg = _estate(tmp_path)
    _no_network(monkeypatch)
    stale = tmp_path / "generated_jobs" / "snowmig_task_gone.ipynb"
    stale.parent.mkdir(parents=True)
    stale.write_text("{}", encoding="utf-8")
    keep = tmp_path / "generated_jobs" / "my_notes.txt"
    keep.write_text("mine", encoding="utf-8")
    snowmig.main(["jobs", "--out-dir", str(tmp_path), "--config", cfg])
    assert not stale.exists(), "a job this run no longer generates"
    assert keep.exists(), "only this generator's own notebooks are removed"


# --------------------------------------------------------------- register

class Workspace:
    """A fake AIDP: jobs by name, notebooks by path."""

    def __init__(self, existing=()):
        self.jobs = {n: {"displayName": n, "key": f"k-{n}"} for n in existing}
        self.files: set[str] = set()
        self.ops: list[tuple[str, dict]] = []

    def __call__(self, op, **kw):
        self.ops.append((op, kw))
        if op == "list_jobs":
            return {"items": list(self.jobs.values())}
        if op == "create_ws_folder":
            return {}
        if op == "upload_ws_file":
            assert kw["object_type"] == "NOTEBOOK"
            json.loads(open(kw["local_path"], encoding="utf-8").read())
            self.files.add(kw["path"])
            return {}
        if op == "list_ws_objects":
            return {"items": [{"path": p} for p in sorted(self.files)
                              if p.startswith(kw["path"] + "/")]}
        if op == "create_job":
            body = kw["body"]
            self.jobs[body["name"]] = {"displayName": body["name"],
                                       "key": f'k-{body["name"]}',
                                       "body": body}
            return {}
        raise AssertionError(op)


def _register(tmp_path, monkeypatch, fake, *extra):
    cfg = _estate(tmp_path)
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    return snowmig.main(["jobs", "--out-dir", str(tmp_path), "--config", cfg,
                         "--register", "--datalake-ocid", OCID,
                         "--workspace", "ws", "--cluster-id", "CL", *extra])


def test_register_creates_every_job_unscheduled(tmp_path, monkeypatch,
                                                capsys):
    fake = Workspace()
    assert _register(tmp_path, monkeypatch, fake) == 0
    res = json.loads(_read(tmp_path, "generated_jobs.json"))
    assert res["registered"] is True
    created = [j for j in fake.jobs.values() if "body" in j]
    assert len(created) == len(res["jobs"]) == 4
    for job in created:
        assert "schedule" not in job["body"]
        for task in job["body"]["tasks"]:
            assert task["cluster"] == {"clusterKey": "CL"}
            assert task["notebookPath"] in fake.files, "uploaded first"
    reg = res["registration"]
    assert sorted(reg["created"]) == sorted(j["name"] for j in res["jobs"])
    assert "not applied" in reg["schedule"]
    out = capsys.readouterr().out
    assert "schedule" in out.lower() and "not applied" in out.lower()
    # The graph's order reached the job.
    graph = next(j["body"] for j in created
                 if j["displayName"].startswith("snowmig_task_db_core_tsk_c"))
    assert graph["tasks"][0]["taskKey"] == "tsk_call"


def test_register_never_adopts_a_job_that_already_has_the_name(
        tmp_path, monkeypatch, capsys):
    name = "snowmig_refresh_db_core_dt_order_rollup"
    fake = Workspace(existing=[name])
    assert _register(tmp_path, monkeypatch, fake) == 1
    assert "body" not in fake.jobs[name], "not overwritten"
    reg = json.loads(_read(tmp_path, "generated_jobs.json"))["registration"]
    assert name in reg["name_taken"]
    assert name in capsys.readouterr().err


def test_register_needs_the_target_coordinates(tmp_path, monkeypatch, capsys):
    cfg = _estate(tmp_path)
    _no_network(monkeypatch)
    assert snowmig.main(["jobs", "--out-dir", str(tmp_path), "--config", cfg,
                         "--register"]) == 1
    err = capsys.readouterr().err
    assert "--datalake-ocid" in err and "--cluster-id" in err


def test_register_is_held_to_the_no_new_objects_decision(tmp_path,
                                                         monkeypatch, capsys):
    _estate(tmp_path)
    cfg = tmp_path / "no.yaml"
    cfg.write_text("decisions:\n  allow_new_objects: false\n",
                   encoding="utf-8")
    _no_network(monkeypatch)
    assert snowmig.main(["jobs", "--out-dir", str(tmp_path), "--config",
                         str(cfg), "--register", "--datalake-ocid", OCID,
                         "--workspace", "ws", "--cluster-id", "CL"]) == 1
    assert "allow_new_objects" in capsys.readouterr().err


def test_a_notebook_that_does_not_read_back_gets_no_job(tmp_path, monkeypatch):
    class Lossy(Workspace):
        def __call__(self, op, **kw):
            if op == "list_ws_objects":
                self.ops.append((op, kw))
                return {"items": []}
            return super().__call__(op, **kw)

    fake = Lossy()
    assert _register(tmp_path, monkeypatch, fake) == 1
    assert not [j for j in fake.jobs.values() if "body" in j], \
        "a job pointed at a notebook nobody can see is not created"


def test_a_failed_upload_gets_no_job_even_over_a_stale_notebook(
        tmp_path, monkeypatch, capsys):
    """The listing alone is not proof the notebook is THIS run's notebook.

    A re-run after a partial one finds last run's notebook still in the
    folder. If this run's upload of it fails (a 503, say), the listing
    still shows a file of that name -- the old content. Creating the job
    then points it at SQL this run did not generate, and reports it as
    created. provisioning.py already lets an upload error win over the
    listing; the generated jobs are held to the same rule: a job with any
    notebook that failed to upload is not created, is listed in `failed`,
    and `snowmig jobs` exits 1.
    """
    name = "snowmig_task_db_core_tsk_call"

    class Flaky(Workspace):
        def __call__(self, op, **kw):
            if op == "upload_ws_file" and name in kw["path"]:
                self.ops.append((op, kw))
                self.files.add(kw["path"])     # last run's copy is there
                raise RuntimeError("503 Service Unavailable")
            return super().__call__(op, **kw)

    fake = Flaky()
    assert _register(tmp_path, monkeypatch, fake) == 1
    assert name not in fake.jobs, "no job over a notebook that did not upload"
    reg = json.loads(_read(tmp_path, "generated_jobs.json"))["registration"]
    assert name in reg["failed"] and name not in reg["created"]
    assert any(s["outcome"] == "not_created" and name in s["detail"]
               and "upload" in s["detail"] for s in reg["steps"])
    # The other jobs are unaffected by one job's failed upload.
    assert len(reg["created"]) == 3
    assert name in capsys.readouterr().err


def test_register_never_takes_its_keys_from_provision_result(tmp_path, monkeypatch,
                                                            capsys):
    # Every later command takes the workspace and cluster keys from a flag or
    # the config, never implicitly from provision_result.json (README,
    # Hand-off): a record from another migration must not redirect a write.
    cfg = _estate(tmp_path)
    (tmp_path / "provision_result.json").write_text(json.dumps(
        {"workspace": {"key": "ws-from-record"}, "cluster": {"key": "cl-from-record"}}),
        encoding="utf-8")
    fake = Workspace()
    monkeypatch.setattr(provisioning, "make_provision_call", lambda ocid, **kw: fake)
    rc = snowmig.main(["jobs", "--out-dir", str(tmp_path), "--config", cfg,
                       "--register", "--datalake-ocid", OCID])
    assert rc != 0
    assert "--workspace" in capsys.readouterr().err
    assert not [op for op, _ in fake.ops if op == "create_job"]
