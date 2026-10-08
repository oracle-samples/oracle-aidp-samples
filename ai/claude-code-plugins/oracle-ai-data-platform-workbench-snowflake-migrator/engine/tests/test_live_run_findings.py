"""Regressions for what the 2026-09-29 live run (0.26.0, a 4-table plan over
a 1000-table estate) found. Each test names the behaviour it pins; the
transport is faked throughout, nothing reaches AIDP.
"""
import argparse
import json

import pytest

import snowmig
from plan.status import pipeline_status
from report.render import render_catalog, render_stages
from report.stages import build_stage_board
from target import provisioning
from target.provisioning import provision, render_provision
from test_provisioning import Fake

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


def _write(out, name, data):
    (out / name).write_text(json.dumps(data), encoding="utf-8")


def _row(board, stage):
    return next(r for r in board["stages"] if r["stage"] == stage)


PROVISIONED = {"dry_run": False, "workspace": {"name": "ws", "key": "ws-k"},
               "cluster": {"name": "cl", "key": "cl-k"},
               "steps": [{"step": "workspace", "action": "created",
                          "verified": True, "detail": "ws"}]}


# --- the stage board follows the runbook -------------------------------------

def test_a_fresh_migration_is_pointed_at_s1_not_at_a_laptop_assess(tmp_path):
    _write(tmp_path, "preflight.json", {"fields": [], "checks": []})
    board = build_stage_board(tmp_path)
    assert board["route"] == "runbook"
    assert board["next_stage"] == "provision"
    assert pipeline_status(board)["next"] == "provision"


def test_after_provision_the_catalogs_are_next(tmp_path):
    _write(tmp_path, "provision_result.json", PROVISIONED)
    board = build_stage_board(tmp_path)
    assert board["next_stage"] == "catalog"
    assert pipeline_status(board)["next"] == "catalog"


def test_an_inventory_written_by_ingest_is_not_assess_having_run(tmp_path):
    _write(tmp_path, "provision_result.json", PROVISIONED)
    _write(tmp_path, "inventory.json",
           {"object_count": 1, "counts_by_type": {}, "extraction_notes": [],
            "session": {"source": "in-AIDP discovery workflow"}})
    _write(tmp_path, "ingest_result.json", {"objects": 1})
    board = build_stage_board(tmp_path)
    assert _row(board, "assess")["status"] == "SATISFIED"
    assert _row(board, "ingest")["status"] == "DONE"


def test_a_running_structure_job_is_waited_on_never_offered(tmp_path):
    _write(tmp_path, "provision_result.json", PROVISIONED)
    _write(tmp_path, "catalog_result.json",
           {"dry_run": False, "catalog": "c", "catalog_type": "INTERNAL",
            "action": "created", "verified": True})
    _write(tmp_path, "run_snowmig_00_discover.json",
           {"job": "snowmig_00_discover", "status": "SUCCESS", "ok": True,
            "terminal": True, "run_key": "r0"})
    _write(tmp_path, "inventory.json",
           {"object_count": 1, "counts_by_type": {}, "extraction_notes": [],
            "session": {"source": "in-AIDP discovery workflow"}})
    _write(tmp_path, "ingest_result.json", {"objects": 1})
    _write(tmp_path, "dependencies.json",
           {"source_used": "not_extracted", "cycles": []})
    _write(tmp_path, "plan.json", {"summary": {"can_migrate": 1,
                                                "cannot_migrate": 0}})
    _write(tmp_path, "ddl_plan.json", {"statements": [{}], "blocked": []})
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "status": "RUNNING",
            "terminal": False, "ok": False, "watching": True,
            "run_key": "r1"})
    board = build_stage_board(tmp_path)
    assert _row(board, "structure-workflow")["status"] == "RUNNING"
    assert board["next_stage"] is None
    assert board["waiting_on"] == "structure-workflow"
    status = pipeline_status(board)
    assert status["next"] is None
    assert "deploy" not in status["unblocked"]
    assert "structure-workflow" not in status["unblocked"]
    assert "## Waiting on `structure-workflow`" in render_stages(board)


def test_a_restriction_exclusion_is_scope_not_a_problem(tmp_path):
    _write(tmp_path, "plan.json",
           {"summary": {"can_migrate": 4, "cannot_migrate": 998,
                        "cannot_by_category": {"restriction": 998}}})
    row = _row(build_stage_board(tmp_path), "plan")
    assert row["found"] == "4 can migrate, 998 left out by restrictions"
    assert row["attention"] is False


def test_the_board_lists_every_catalog_and_its_connection_test(tmp_path):
    _write(tmp_path, "catalog_result.json", {
        "dry_run": False, "catalog": "target", "catalog_type": "INTERNAL",
        "action": "created", "verified": True,
        "catalogs_recorded": [
            {"catalog": "source", "catalog_type": "EXTERNAL",
             "action": "created",
             "test_connection": {"status": "FAILED",
                                 "error": "Test connection failed: "}},
            {"catalog": "target", "catalog_type": "INTERNAL",
             "action": "created", "test_connection": None}]})
    row = _row(build_stage_board(tmp_path), "catalog")
    assert "source (EXTERNAL): created" in row["found"]
    assert "target (INTERNAL): created" in row["found"]
    assert "FAILED with an empty reason" in row["found"]
    # The known platform issue (runbook S3) is shown, not a stop.
    assert row["attention"] is False


def test_a_connection_test_failing_with_a_reason_needs_attention(tmp_path):
    _write(tmp_path, "catalog_result.json", {
        "dry_run": False, "catalog": "source", "catalog_type": "EXTERNAL",
        "action": "created",
        "test_connection": {"status": "FAILED", "error": "Incorrect password"}})
    row = _row(build_stage_board(tmp_path), "catalog")
    assert "connection test FAILED" in row["found"]
    assert row["attention"] is True


# --- catalog: S3 and S4 each keep their record --------------------------------

class CatalogRecorder:
    def __init__(self):
        self.catalogs = []

    def __call__(self, operation, **kw):
        if operation == "list_catalogs":
            return {"items": list(self.catalogs)}
        if operation == "create_catalog":
            body = kw["body"]
            self.catalogs.append({"displayName": body["displayName"],
                                  "key": body["displayName"],
                                  "catalogType": body["catalogType"]})
            return {}
        raise AssertionError(operation)


@pytest.fixture()
def catalogs(tmp_path, monkeypatch):
    rec = CatalogRecorder()
    monkeypatch.setattr(snowmig, "detect_backend", lambda: "oci_raw")
    monkeypatch.setattr(snowmig, "make_call",
                        lambda target, *, backend, **kw: rec)
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("\n".join([
        "snowflake:", "  account: ORG-ACC", "  user: SVC", "  warehouse: WH",
        "  database: SALES_DB", "  role: READER", "  schema: PUBLIC",
        "  auth: password", "  password: not-a-real-password", "aidp:",
        f"  datalake_ocid: {OCID}", "  workspace: ws", "  cluster_id: cl",
        ""]), encoding="utf-8")
    return str(cfg)


def _catalog(tmp_path, cfg, *extra):
    return snowmig.main(["catalog", "--config", cfg,
                         "--out-dir", str(tmp_path), *extra])


def test_the_s4_dry_run_after_s3_is_allowed_and_keeps_the_s3_record(
        tmp_path, catalogs):
    assert _catalog(tmp_path, catalogs, "--catalog", "src", "--execute") == 0
    rc = _catalog(tmp_path, catalogs, "--catalog", "tgt",
                  "--catalog-type", "standard")
    assert rc == 0
    latest = json.loads((tmp_path / "catalog_result.json").read_text(encoding="utf-8"))
    assert latest["catalog"] == "src" and latest["dry_run"] is False
    dry = json.loads((tmp_path / "catalog_result_tgt.json").read_text(encoding="utf-8"))
    assert dry["dry_run"] is True
    assert (tmp_path / "CATALOG_tgt.md").read_text(encoding="utf-8").startswith(
        "# Target catalog `tgt` — DRY RUN")


def test_the_s4_execute_keeps_both_catalogs_on_record(tmp_path, catalogs):
    assert _catalog(tmp_path, catalogs, "--catalog", "src", "--execute") == 0
    assert _catalog(tmp_path, catalogs, "--catalog", "tgt",
                    "--catalog-type", "standard", "--execute") == 0
    latest = json.loads((tmp_path / "catalog_result.json").read_text(encoding="utf-8"))
    assert [c["catalog"] for c in latest["catalogs_recorded"]] == ["src",
                                                                    "tgt"]
    src = json.loads((tmp_path / "catalog_result_src.json").read_text(encoding="utf-8"))
    assert src["catalog_type"] == "EXTERNAL" and src["dry_run"] is False
    assert (tmp_path / "CATALOG_src.md").read_text(encoding="utf-8").startswith(
        "# Source catalog `src`")


def test_a_dry_run_of_an_executed_catalog_is_still_refused(tmp_path,
                                                           catalogs):
    assert _catalog(tmp_path, catalogs, "--catalog", "src", "--execute") == 0
    assert _catalog(tmp_path, catalogs, "--catalog", "src") == 1


def test_the_schema_note_is_printed_for_the_external_catalog_only(
        tmp_path, catalogs, capsys):
    _catalog(tmp_path, catalogs, "--catalog", "tgt", "--catalog-type",
             "standard", "--execute")
    assert "is not used here" not in capsys.readouterr().out


def test_an_empty_reason_failure_points_at_discovery_not_the_credential():
    md = render_catalog({"dry_run": False, "catalog": "src",
                         "catalog_type": "EXTERNAL", "action": "created",
                         "verified": True, "key": "src",
                         "test_connection": {"status": "FAILED",
                                             "error": "Test connection "
                                                      "failed: "}})
    assert "returned no reason" in md and "Keep the registration" in md
    assert "discovery (S6)" in md
    assert "known platform issue" not in md
    assert "fix the credential" not in md


# --- run: the record exists while the job runs; the check says what it did ----

def _run_args(tmp_path, job="snowmig_02_copy_sales"):
    return argparse.Namespace(
        out_dir=str(tmp_path), datalake_ocid=OCID, workspace="ws",
        cluster_id=None, catalog=None, backend=None, config=None, job=job,
        job_key="job-k", param=None, poll_seconds=0, max_polls=1,
        cold_start_seconds=60, cold_start_restarts=0, refresh=False,
        run_key=None)


class Job:
    def __init__(self, tmp_path, task_parameters, job="snowmig_02_copy_sales"):
        self.tmp, self.params, self.job = tmp_path, task_parameters, job
        self.seen_while_running = None

    def __call__(self, op, **kw):
        if op == "get_job":
            return {"tasks": [{"parameters": self.params}]}
        if op == "list_job_runs":
            return {"items": []}
        if op == "run_job":
            return {"key": "run-1"}
        if op == "get_job_run":
            path = self.tmp / f"run_{self.job}.json"
            if self.seen_while_running is None and path.exists():
                self.seen_while_running = json.loads(path.read_text(encoding="utf-8"))
            return {"state": {"status": "SUCCESS"}}
        if op == "list_task_runs":
            return {"items": [{"key": "t1", "startTime": 1}]}
        if op == "fetch_task_output":
            return {"data": []}
        raise AssertionError(op)


def test_a_submitted_run_is_recorded_running_before_the_watch_ends(
        tmp_path, monkeypatch):
    fake = Job(tmp_path, [{"name": "schema", "value": "SALES"}])
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    assert snowmig.cmd_run(_run_args(tmp_path)) == 0
    seen = fake.seen_while_running
    assert seen and seen["status"] == "RUNNING" and seen["watching"] is True
    assert seen["run_key"] == "run-1"
    final = json.loads((tmp_path / "run_snowmig_02_copy_sales.json")
                       .read_text(encoding="utf-8"))
    assert final["status"] == "SUCCESS"


def test_a_passing_task_parameter_check_is_said_and_recorded(
        tmp_path, monkeypatch, capsys):
    fake = Job(tmp_path, [{"name": "schema", "value": "SALES"}])
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    snowmig.cmd_run(_run_args(tmp_path))
    assert "task parameters checked: schema=SALES" in capsys.readouterr().out
    md = (tmp_path / "RUN_snowmig_02_copy_sales.md").read_text(encoding="utf-8")
    assert "| task parameters | checked before submitting — `schema=SALES` |" \
        in md


def test_a_null_task_parameter_list_is_none_not_unreadable(
        tmp_path, monkeypatch, capsys):
    # Live: a task with no parameters answers `parameters: null`.
    fake = Job(tmp_path, None, job="snowmig_01_structure")
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    snowmig.cmd_run(_run_args(tmp_path, job="snowmig_01_structure"))
    out = capsys.readouterr()
    assert "none on the job's task" in out.out
    assert "NOT checked" not in out.err


# --- provision -----------------------------------------------------------------

class Rerun(Fake):
    """A re-push: the folders exist (409), the cluster is still CREATING."""

    def __call__(self, operation, **kw):
        if operation == "create_ws_folder":
            self.ops.append((operation, kw))
            raise RuntimeError('create_ws_folder failed (exit 153): Response:\n'
                               '{\n  "status": 409,\n  "code": "Conflict",\n'
                               '  "message": "Directory already exists"\n}')
        if operation == "list_clusters":
            self.ops.append((operation, kw))
            return {"items": [dict(c, state="CREATING")
                              for c in self.clusters]}
        return super().__call__(operation, **kw)


@pytest.fixture()
def no_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def test_an_existing_folder_is_there_not_unconfirmed(no_sleep):
    res = provision(call=Rerun(), workspace_name="acme", scripts=[],
                    execute=True, delays=())
    folders = [s for s in res["steps"] if s["step"] == "folder"]
    assert folders and all(s["action"] == "exists" and s["verified"] is True
                           for s in folders)
    md = render_provision(res)
    assert "Directory already exists" not in md
    assert "create_failed_or_exists" not in md


def test_a_creating_cluster_is_said_to_be_creating(no_sleep):
    res = provision(call=Rerun(), workspace_name="acme", scripts=[],
                    execute=True, delays=())
    step = next(s for s in res["steps"] if s["step"] == "cluster")
    assert "state CREATING" in step["detail"]
    assert res["cluster"]["state_at_create"] == "CREATING"


def test_the_report_hands_off_the_keys_and_names_the_right_next_step(
        no_sleep):
    res = provision(call=Fake(), workspace_name="acme", scripts=[],
                    execute=True, delays=())
    md = render_provision(res)
    assert "## Hand-off" in md
    assert f'--workspace {res["workspace"]["key"]}' in md
    assert "or the script by hand" not in md
    assert "EXTERNAL source (runbook S3)" in md


def test_a_cell_never_carries_a_newline_or_a_pipe():
    res = provision(call=None, workspace_name="acme", scripts=[],
                    execute=False)
    res["steps"].append({"step": "x", "action": "y", "verified": None,
                         "detail": "line one\nline | two"})
    row = [line for line in render_provision(res).splitlines()
           if line.startswith("| x |")]
    assert row == ["| x | y | — | line one line \\| two |"]


def test_a_re_push_keeps_the_cluster_name_the_first_push_used(
        tmp_path, monkeypatch, capsys, no_sleep):
    fake = Fake(workspaces=("acme",), clusters=("migration_assets_lab",))
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    _write(tmp_path, "provision_result.json", {
        "dry_run": False, "datalake_ocid": OCID,
        "workspace": {"name": "acme", "key": "ws-acme"},
        "cluster": {"requested": "migration_assets_lab",
                    "name": "migration_assets_lab",
                    "key": "cl-migration_assets_lab", "created": True},
        "steps": []})
    rc = snowmig.main(["provision", "--datalake-ocid", OCID,
                       "--workspace-name", "acme", "--skip-libraries",
                       "--out-dir", str(tmp_path), "--execute",
                       "--reuse-existing"])
    assert rc == 0
    assert not [kw for op, kw in fake.ops if op == "create_cluster"]
    assert "cluster name taken from provision_result.json" in \
        capsys.readouterr().out


# --- destination --------------------------------------------------------------

def test_a_flag_given_destination_is_announced(tmp_path, capsys):
    args = argparse.Namespace(out_dir=str(tmp_path), config=None,
                              datalake_ocid=OCID, workspace="ws-1",
                              cluster_id="cl-1", catalog=None)
    snowmig._target_coords(args)
    out = capsys.readouterr().out
    assert "destination from flags:" in out
    assert "workspace=ws-1" in out and "cluster_id=cl-1" in out
