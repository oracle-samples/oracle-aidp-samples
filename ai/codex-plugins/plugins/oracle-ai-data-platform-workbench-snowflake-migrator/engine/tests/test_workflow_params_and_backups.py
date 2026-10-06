"""Workflow parameters, per-schema copy workflows, backups and the download.

Live 2026-09-29 the migration had to hand-upload its plan and its backups,
and the copy stage was one job with a baked schema. These pin the fixes:

* a stage notebook reads a job TASK's `parameters` at run time, through
  `oidlUtils.parameters.getParameter` (resolved by the AIDP runtime) and
  then the environment -- so ONE 02_copy_schema notebook backs one workflow
  per schema;
* provision pushes the plan to plan/ AND backs it up, dated, into backup/;
* the discovery notebook backs its own manifest up, dated;
* `fetch` downloads through the console's own PAR route, size-checked.
"""
import datetime
import importlib.util
import io
import json
import pathlib

import pytest

from target.provision_api import build_job_body, build_provision_command
from target.provisioning import (
    BACKUP_FOLDER, JOB_SPECS, PLAN_FOLDER, SCRIPTS_FOLDER,
    ProvisionTransportError, copy_job_specs, download_ws_file,
    plan_backup_names, plan_copy_schemas, provision, render_provision)
from target.stage_notebooks import STAGES, build_stage_notebook

from tests.test_provisioning import Fake

OCID = "ocid1.aidataplatform.oc1.iad.a"
NOW = datetime.datetime(2026, 9, 29, 3, 10, tzinfo=datetime.timezone.utc)
DATAPLANE = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _stage(key):
    return next(s for s in STAGES if s.key == key)


def _params_cell_globals(stage_key, *, overrides=None, workflow=None,
                         env=None, monkeypatch=None):
    """Execute a generated PARAMS cell as the AIDP runtime would."""
    nb = build_stage_notebook(_stage(stage_key), overrides=overrides)
    source = "".join(nb["cells"][1]["source"])
    scope = {}
    if workflow is not None:
        class _Params:
            @staticmethod
            def getParameter(name, default):
                return workflow.get(name, default)
        scope["oidlUtils"] = type("U", (), {"parameters": _Params})
    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)
    exec(compile(source, "<params>", "exec"), scope)
    return scope


# --- the PARAMS cell reads workflow parameters --------------------------------

def test_a_task_parameter_overrides_the_baked_literal(monkeypatch):
    g = _params_cell_globals(
        "copy_schema", overrides={"target-catalog": "lake"},
        workflow={"schema": "COMMERCE"}, monkeypatch=monkeypatch)
    assert g["PARAMS"]["schema"] == "COMMERCE"
    assert g["PARAMS"]["target-catalog"] == "lake"   # baked default kept
    assert g["ARGV"][g["ARGV"].index("--schema") + 1] == "COMMERCE"
    assert g["PARAMS_FROM_WORKFLOW"] == {"schema": "COMMERCE"}


def test_the_environment_is_the_fallback_a_task_parameter_reaches(monkeypatch):
    g = _params_cell_globals(
        "copy_schema", overrides={"target-catalog": "lake"},
        workflow={}, env={"schema": "RISK"}, monkeypatch=monkeypatch)
    assert g["PARAMS"]["schema"] == "RISK"


def test_outside_aidp_there_is_no_oidlutils_and_nothing_breaks(monkeypatch):
    monkeypatch.delenv("schema", raising=False)
    monkeypatch.delenv("SCHEMA", raising=False)
    g = _params_cell_globals(
        "copy_schema",
        overrides={"target-catalog": "lake", "schema": "BAKED"},
        monkeypatch=monkeypatch)
    assert g["PARAMS"]["schema"] == "BAKED"
    assert g["PARAMS_FROM_WORKFLOW"] == {}


def test_switches_and_lists_arrive_as_text_and_are_coerced(monkeypatch):
    g = _params_cell_globals(
        "copy_schema", overrides={"target-catalog": "lake"},
        workflow={"schema": "S", "dry-run": "true", "tables": "A, B"},
        monkeypatch=monkeypatch)
    assert g["PARAMS"]["dry-run"] is True
    assert g["PARAMS"]["tables"] == ["A", "B"]
    assert "--dry-run" in g["ARGV"]


def test_a_required_parameter_is_still_refused_when_nobody_sets_it(
        monkeypatch):
    monkeypatch.delenv("schema", raising=False)
    monkeypatch.delenv("SCHEMA", raising=False)
    with pytest.raises(ValueError, match="schema"):
        _params_cell_globals("copy_schema",
                             overrides={"target-catalog": "lake"},
                             workflow={}, monkeypatch=monkeypatch)


# --- one copy workflow per schema, ONE notebook -------------------------------

PLAN = {"statements": [
    {"source_identifier": "DB.COMMERCE.ORDERS", "object_type": "TABLE",
     "target_fqn": "lake.db_commerce.orders"},
    {"source_identifier": "DB.COMMERCE.ITEMS", "object_type": "TABLE",
     "target_fqn": "lake.db_commerce.items"},
    {"source_identifier": "DB.RISK.SCORES", "object_type": "TABLE",
     "target_fqn": "lake.db_risk.scores"},
    {"source_identifier": "DB.ANALYTICS.V", "object_type": "VIEW",
     "target_fqn": "lake.db_analytics.v"},
]}


def test_the_schemas_come_from_the_approved_plan_tables_only():
    assert plan_copy_schemas(PLAN) == ["COMMERCE", "RISK"]


def test_every_copy_job_points_at_the_same_notebook_with_its_schema():
    specs = copy_job_specs(["COMMERCE", "RISK"])
    assert {s["notebook"] for s in specs} == {"02_copy_schema.ipynb"}
    assert [s["name"] for s in specs] == ["snowmig_02_copy_commerce",
                                          "snowmig_02_copy_risk"]
    assert [s["task_parameters"] for s in specs] == [
        {"schema": "COMMERCE"}, {"schema": "RISK"}]


def test_without_a_plan_the_generic_copy_job_is_still_created():
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=[], execute=True,
              delays=(), target_catalog="lake", now=NOW)
    names = {kw["body"]["name"] for op, kw in fake.ops if op == "create_job"}
    assert names == {s["name"] for s in JOB_SPECS}


def test_two_schemas_that_translate_alike_are_refused():
    with pytest.raises(ValueError, match="collide"):
        copy_job_specs(["A-B", "A_B"])


def test_a_task_parameter_goes_on_the_notebook_task():
    body = build_job_body("j", notebook_path="p.ipynb", cluster_key="c",
                          task_parameters={"schema": "RISK"})
    assert body["tasks"][0]["parameters"] == [{"name": "schema",
                                               "value": "RISK"}]
    assert "parameters" not in build_job_body(
        "j", notebook_path="p.ipynb", cluster_key="c")["tasks"][0]


def _plan_files(tmp_path):
    (tmp_path / "plan.json").write_text("{}", encoding="utf-8")
    (tmp_path / "ddl_plan.json").write_text(json.dumps(PLAN),
                                            encoding="utf-8")
    (tmp_path / "DDL_PLAN.md").write_text("#", encoding="utf-8")
    return [tmp_path / "plan.json", tmp_path / "ddl_plan.json",
            tmp_path / "DDL_PLAN.md"]


def test_provision_registers_the_copy_jobs_on_the_one_notebook(tmp_path):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    execute=True, delays=(), target_catalog="lake",
                    copy_schemas=["COMMERCE", "RISK"], now=NOW)
    bodies = {b["name"]: b for op, kw in fake.ops if op == "create_job"
              for b in [kw["body"]]}
    assert set(bodies) == ({s["name"] for s in JOB_SPECS
                            if s["name"] != "snowmig_02_copy_schema"}
                           | {"snowmig_02_copy_commerce",
                              "snowmig_02_copy_risk"}), \
        "the generic copy job has no schema, so it is not created"
    task = bodies["snowmig_02_copy_risk"]["tasks"][0]
    assert task["notebookPath"] == f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"
    assert task["parameters"] == [{"name": "schema", "value": "RISK"}]
    # ONE copy notebook on the workspace, not one per schema.
    copies = [p for p in fake.contents if "02_copy_schema" in p]
    assert copies == [f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]
    assert [j["job"] for j in res["copy_jobs"]] == [
        "snowmig_02_copy_commerce", "snowmig_02_copy_risk"]
    assert "Per-schema copy workflows" in render_provision(res)


# --- the plan push backs the plan up, dated ----------------------------------

def test_backup_names_are_dated_labelled_and_plans_only(tmp_path):
    names = [n for _, n in plan_backup_names(
        _plan_files(tmp_path), stamp="20260929T031000Z", label="FULL plan!")]
    assert names == ["plan_20260929T031000Z_FULL_plan.json",
                     "ddl_plan_20260929T031000Z_FULL_plan.json"]


def test_provision_pushes_the_plan_and_backs_it_up(tmp_path):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    plan_files=_plan_files(tmp_path), execute=True,
                    delays=(), plan_label="REDUCED", now=NOW)
    assert f"{PLAN_FOLDER}/ddl_plan.json" in fake.contents
    assert (f"{BACKUP_FOLDER}/ddl_plan_20260929T031000Z_REDUCED.json"
            in fake.contents)
    assert f"{BACKUP_FOLDER}/DDL_PLAN_20260929T031000Z_REDUCED.json" \
        not in fake.contents
    backups = [s for s in res["steps"] if s["step"] == "backup"]
    assert len(backups) == 2 and all(s["verified"] for s in backups)


def test_a_dry_run_previews_the_backup(tmp_path):
    res = provision(call=None, workspace_name="acme", scripts=[],
                    plan_files=_plan_files(tmp_path), now=NOW)
    assert [s["action"] for s in res["steps"]
            if s["step"] == "backup"] == ["would upload"] * 2


# --- discovery backs up its own manifest -------------------------------------

def _discover_module():
    spec = importlib.util.spec_from_file_location(
        "discover_under_test", DATAPLANE / "00_discover_snowflake.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_manifest_backup_is_dated_and_never_overwritten(tmp_path):
    mod = _discover_module()
    first = mod.backup_manifest({"schemas": []}, str(tmp_path / "backup"),
                                now=NOW)
    later = mod.backup_manifest({"schemas": [1]}, str(tmp_path / "backup"),
                                now=NOW + datetime.timedelta(hours=1))
    assert first.name == "discovery_manifest_20260929T031000Z.json"
    assert first != later and json.loads(first.read_text()) == {"schemas": []}


def test_an_empty_backup_dir_skips_the_backup(tmp_path):
    assert _discover_module().backup_manifest({}, "") is None


# --- the download ------------------------------------------------------------

def test_the_download_uses_the_consoles_par_route():
    cmd = build_provision_command("oci_raw", "download_ws_file", OCID,
                                  workspace="ws", path="a/b.json")
    assert cmd[:4] == ["aidp", "workspace-object", "download-with-par", "ws"]
    assert "--should-generate-new-par" in cmd


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_a_download_is_written_only_when_the_size_matches(tmp_path):
    call = lambda op, **kw: {"parUrl": "https://par/x", "size": 3}  # noqa
    res = download_ws_file(call, workspace="ws", path="a/b.json",
                           dest=tmp_path / "b.json",
                           opener=lambda url, **kw: _Resp(b"abc"))
    assert res["size"] == 3 and (tmp_path / "b.json").read_bytes() == b"abc"
    assert "par" not in json.dumps(res), "the PAR URL is never returned"


def test_a_short_download_is_an_error_and_writes_nothing(tmp_path):
    call = lambda op, **kw: {"parUrl": "https://par/x", "size": 9}  # noqa
    with pytest.raises(ProvisionTransportError, match="9"):
        download_ws_file(call, workspace="ws", path="a/b.json",
                         dest=tmp_path / "b.json",
                         opener=lambda url, **kw: _Resp(b"abc"))
    assert not (tmp_path / "b.json").exists()
