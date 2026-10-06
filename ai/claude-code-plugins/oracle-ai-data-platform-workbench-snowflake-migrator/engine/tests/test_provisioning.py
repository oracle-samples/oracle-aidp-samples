"""Provisioning: workspace, cluster, libraries, scripts folder, jobs.

The transport is injected, so every decision — look-first, name translation,
poll-the-read-back, record-don't-swallow — is tested with no environment.
"""
import json
import os
import pathlib
import types

import pytest

from target.provision_api import (
    ProvisionBackendUnsupported, build_driver_notebook, build_job_body,
    build_library_items, build_provision_command, build_test_connection_body,
    content_path,
)
from target.stage_notebooks import (
    DIAGNOSE_NOTEBOOK_NAME, STAGES, build_stage_notebook)
from target.provisioning import (
    BACKUP_FOLDER, JOB_SPECS, PLAN_FOLDER, REPORTS_FOLDER, SCRIPTS_FOLDER,
    ProvisionTransportError, make_provision_call, provision,
    render_provision,
)

OCID = "ocid1.aidataplatform.oc1.iad.a"


class Fake:
    """Provisioning transport double. Starts empty, like a fresh tenancy."""

    def __init__(self, *, workspaces=(), clusters=(), jobs=(), fail=(),
                 appear_after=0):
        self.ops: list[tuple] = []
        self.workspaces = [{"displayName": w, "key": f"ws-{w}"}
                           for w in workspaces]
        self.clusters = [{"displayName": c, "key": f"cl-{c}"}
                         for c in clusters]
        self.jobs = [{"name": j, "key": f"job-{j}"} for j in jobs]
        self.contents: dict[str, dict] = {}
        self.fail = set(fail)
        self.appear_after = appear_after
        self._ws_lists = 0

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        if operation in self.fail:
            raise RuntimeError(f"denied: {operation}")
        if operation == "list_workspaces":
            self._ws_lists += 1
            if self._ws_lists <= self.appear_after:
                return {"items": []}
            return {"items": list(self.workspaces)}
        if operation == "create_workspace":
            name = kw["body"]["displayName"]
            self.workspaces.append({"displayName": name, "key": f"ws-{name}"})
            return {}
        if operation == "list_clusters":
            return {"items": list(self.clusters)}
        if operation == "create_cluster":
            name = kw["body"]["displayName"]
            self.clusters.append({"displayName": name, "key": f"cl-{name}"})
            return {}
        if operation in ("install_libraries", "restart_cluster"):
            return {}
        if operation == "list_libraries":
            return {"items": []}
        if operation == "create_ws_folder":
            self.contents[kw["path"]] = {"type": "FOLDER"}
            return {}
        if operation == "upload_ws_file":
            # Capture the BYTES at upload time. The provisioner writes each
            # notebook to a temp file and unlinks it immediately after, so a
            # double that only remembers the path has nothing to read later.
            try:
                body = pathlib.Path(kw["local_path"]).read_text(encoding="utf-8")
            except OSError:
                body = None
            self.contents[kw["path"]] = {"type": "FILE",
                                         "local": kw["local_path"],
                                         "body": body}
            return {}
        if operation == "list_ws_objects":
            prefix = kw["path"] + "/"
            return {"items": [{"path": k, "displayName": k.rsplit("/", 1)[-1]}
                              for k in self.contents
                              if k.startswith(prefix)]}
        if operation == "list_jobs":
            return {"items": list(self.jobs)}
        if operation == "create_job":
            self.jobs.append({"name": kw["body"]["name"],
                              "key": f'job-{kw["body"]["name"]}'})
            return {}
        raise AssertionError(f"unexpected op {operation}")


@pytest.fixture()
def scripts(tmp_path):
    out = []
    for name in ("00_discover_snowflake.py", "01_create_structure.py",
                 "02_copy_schema.py", "03_reconcile.py"):
        path = tmp_path / name
        path.write_text("# script body\n", encoding="utf-8")
        out.append(path)
    return out


def test_a_dry_run_calls_nothing_and_plans_everything(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="Acme PROD", scripts=scripts,
                    execute=False)
    assert fake.ops == []
    assert res["dry_run"] is True
    kinds = {s["step"] for s in res["steps"]}
    assert {"workspace", "cluster", "upload", "job"} <= kinds


def test_the_workspace_name_is_translated_before_any_create(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="Acme PROD — Café Migration",
                    scripts=scripts, execute=True, delays=())
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_workspace"]
    assert created == ["acme_prod_cafe_migration"]
    assert res["workspace"]["renamed"] is True
    assert res["workspace"]["notes"], "a silent rename is not attributable"


def test_an_existing_workspace_is_refused_not_adopted(scripts):
    """A migration creates its own environment, so its blast radius is known.

    Adopting a workspace somebody else made means the migration's objects sit
    among strangers' and cannot be torn down as a unit. A taken name is a
    collision for the user to resolve, never a shortcut.
    """
    fake = Fake(workspaces=("acme",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    ops = [op for op, _ in fake.ops]
    assert "create_workspace" not in ops
    assert any(s["step"] == "workspace" and s["action"] == "name_taken"
               for s in res["steps"])
    assert any(s["step"] == "halt" for s in res["steps"])
    # Nothing downstream was attempted.
    assert "create_cluster" not in ops
    assert not any(s["step"] == "job" for s in res["steps"])


def test_reuse_existing_opts_back_in_explicitly(scripts):
    """The escape hatch exists, but it has to be asked for by name."""
    fake = Fake(workspaces=("acme",), clusters=("migration_assets",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), reuse_existing=True)
    ops = [op for op, _ in fake.ops]
    assert "create_workspace" not in ops
    assert "create_cluster" not in ops
    assert any(s["step"] == "workspace" and s["action"] == "reused"
               for s in res["steps"])
    assert any(s["step"] == "job" for s in res["steps"])


def test_a_created_workspace_is_polled_until_visible(scripts):
    fake = Fake(appear_after=2)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0, 0))
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["action"] == "created"
    assert ws["verified"] is True


def test_a_workspace_that_never_appears_halts_the_run(scripts):
    class NeverVisible(Fake):
        def __call__(self, operation, **kw):
            if operation == "list_workspaces":
                self.ops.append((operation, kw))
                return {"items": []}
            return super().__call__(operation, **kw)

    fake = NeverVisible()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0,))
    assert any(s["step"] == "halt" for s in res["steps"])
    assert not any(op == "create_cluster" for op, _ in fake.ops), \
        "nothing else may be attempted against a workspace that is not there"


def test_stage_notebooks_are_uploaded_and_read_back(scripts):
    # The data plane ships as `.ipynb` ONLY. AIDP types a workspace object by
    # extension -- a `.py` uploaded with --type NOTEBOOK is stored as a FILE
    # -- and a job task needs a NOTEBOOK, so a `.py` on the workspace could
    # never be run as a job.
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    notebooks = [s for s in res["steps"] if s["step"] == "notebook"]
    assert len(notebooks) == len(JOB_SPECS)
    assert all(s["verified"] is True for s in notebooks)
    assert f"{SCRIPTS_FOLDER}/00_discover_snowflake.ipynb" in fake.contents
    assert not any(k.endswith(".py") for k in fake.contents), \
        "no .py may reach the workspace: it would be stored as a FILE"


def test_a_failed_upload_is_recorded_and_the_run_continues(scripts):
    fake = Fake(
                fail={"upload_ws_file"})
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    notebooks = [s for s in res["steps"] if s["step"] == "notebook"]
    assert notebooks, "a failed upload must not silently abandon the rest"
    # A job without its notebook would be born broken, so when the upload
    # fails the job is deliberately NOT created — and both facts are on the
    # record.
    assert all(s["verified"] is False for s in notebooks)
    assert not any(s["step"] == "job" for s in res["steps"])


def test_jobs_point_straight_at_the_stage_notebook(scripts):
    # No driver wrapper: the job runs the stage notebook itself, so the code
    # a user opens in the console is the code that runs.
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              execute=True, delays=(), external_catalog="snowflake_ext",
              target_catalog="lake")
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_job"]
    assert {b["name"] for b in bodies} == {s["name"] for s in JOB_SPECS}
    copy = next(b for b in bodies if b["name"] == "snowmig_02_copy_schema")
    task = copy["tasks"][0]
    assert task["type"] == "NOTEBOOK_TASK"
    assert task["notebookPath"] == f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"
    assert task["source"] == "WORKSPACE"
    assert task["cluster"] == {"clusterKey": "cl-migration_assets"}
    uploaded = [kw for op, kw in fake.ops
                if op == "upload_ws_file"
                and kw.get("object_type") == "NOTEBOOK"]
    # One NOTEBOOK per job, plus the environment diagnosis, which has none.
    assert {kw["path"].rsplit("/", 1)[-1] for kw in uploaded} == \
        {s["notebook"] for s in JOB_SPECS} | {DIAGNOSE_NOTEBOOK_NAME}


# --- the environment diagnosis rides along, without a job -------------------
# README step 8 says to open `scripts/diagnose_environment.ipynb` on the
# cluster. provision uploaded only the four job notebooks, so it was never
# there; hand-placed, it imported a module that is inlined elsewhere and read a
# file nothing creates.

def _provisioned_with_config(scripts, tmp_path, fake=None):
    fake = fake or Fake()
    # provision reads the config to derive the snowflake-block copy it
    # uploads, so the file has to exist; an inline (fake) password keeps it
    # off the laptop-only *_path refusal.
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text("snowflake:\n  account: ACC\n  user: u\n  warehouse: WH\n"
                   "  database: DB\n  auth: password\n  password: not-a-real-one\n",
                   encoding="utf-8")
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), external_catalog="ext",
                    source_config=cfg)
    return fake, res


def test_the_diagnose_notebook_is_uploaded_beside_the_stages_without_a_job(
        scripts, tmp_path):
    fake, res = _provisioned_with_config(scripts, tmp_path)
    path = f"{SCRIPTS_FOLDER}/{DIAGNOSE_NOTEBOOK_NAME}"
    assert path in fake.contents
    upload = next(kw for op, kw in fake.ops
                  if op == "upload_ws_file" and kw["path"] == path)
    assert upload["object_type"] == "NOTEBOOK"
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_job"]
    assert {b["name"] for b in bodies} == {s["name"] for s in JOB_SPECS}, \
        "no job runs the diagnosis; it is opened by a human"
    diagnose = [s for s in res["steps"] if s["step"] == "diagnose"]
    assert len(diagnose) == 1 and diagnose[0]["verified"] is True
    # The stage-notebook accounting is untouched by the fifth upload.
    assert len([s for s in res["steps"] if s["step"] == "notebook"]) \
        == len(JOB_SPECS)


def test_the_diagnose_notebook_carries_this_run_s_config_path_and_no_mount_import(
        scripts, tmp_path):
    fake, _ = _provisioned_with_config(scripts, tmp_path)
    body = fake.contents[f"{SCRIPTS_FOLDER}/{DIAGNOSE_NOTEBOOK_NAME}"]["body"]
    assert "/Workspace/backup-snowflake-migration/plan/snowmig-config.json" \
        in body, "the same mount path the stage notebooks receive"
    assert "from snowmig_source import" not in body
    assert "sys.path.insert" not in body
    assert "def load_source_config" in body, "helpers inlined, like the stages"
    assert "EXTERNAL_CATALOG = 'ext'" in body


def test_a_failed_diagnose_upload_is_recorded_not_swallowed(scripts, tmp_path):
    fake, res = _provisioned_with_config(scripts, tmp_path,
                                         Fake(fail={"upload_ws_file"}))
    diagnose = [s for s in res["steps"] if s["step"] == "diagnose"]
    assert len(diagnose) == 1 and diagnose[0]["verified"] is False


def test_the_dry_run_previews_the_diagnose_notebook():
    out = provision(call=None, workspace_name="ws",
                    scripts=[pathlib.Path("00_discover_snowflake.py")],
                    execute=False)
    uploads = [s["detail"] for s in out["steps"] if s["step"] == "upload"]
    assert any(DIAGNOSE_NOTEBOOK_NAME in d for d in uploads), uploads


def test_this_run_s_coordinates_are_written_into_the_params_cell(scripts):
    # Job `parameters` reach a notebook neither as argv nor as env (probed
    # live), so the coordinates have to be IN the notebook.
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              execute=True, delays=(), external_catalog="snowflake_ext",
              target_catalog="lake")
    nb = json.loads(
        fake.contents[f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]["body"])
    params = "".join(nb["cells"][1]["source"])
    assert "'target-catalog': 'lake'" in params
    assert "'source-catalog': 'snowflake_ext'" in params


def test_a_stage_notebook_turns_sys_exit_into_a_real_verdict():
    # main() RETURNS a code; a cell that raises SystemExit is reported as a
    # FAILED task even when the work succeeded -- live, a fully successful
    # discovery came back failed for exactly that reason.
    nb = build_stage_notebook(STAGES[0])
    run = "".join(nb["cells"][-1]["source"])
    assert "code = main(ARGV)" in run
    assert "raise RuntimeError" in run, \
        "a non-zero exit must still fail the job"
    body = "".join(nb["cells"][-2]["source"])
    assert "__main__" not in body, \
        "the main guard must be stripped, or SystemExit escapes"


def test_an_existing_job_is_reused(scripts):
    fake = Fake(
                jobs=("snowmig_00_discover",))
    provision(call=fake, workspace_name="acme", scripts=scripts,
              execute=True, delays=())
    created = [kw["body"]["name"] for op, kw in fake.ops
               if op == "create_job"]
    assert "snowmig_00_discover" not in created


def test_libraries_come_from_the_requirements_file(scripts, tmp_path):
    req = tmp_path / "requirements-aidp.txt"
    req.write_text("# comment\nsnowflake-connector-python>=4.7.0\n", encoding="utf-8")
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              requirements=req, execute=True, delays=())
    ops = [op for op, _ in fake.ops]
    assert "install_libraries" in ops
    assert "restart_cluster" in ops, "the doc requires a restart"
    body = next(kw["body"] for op, kw in fake.ops
                if op == "install_libraries")
    assert body["items"][0]["package"].startswith("snowflake-connector")


def test_an_all_comments_requirements_file_installs_nothing(scripts, tmp_path):
    req = tmp_path / "requirements-aidp.txt"
    req.write_text("# nothing enabled\n", encoding="utf-8")
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              requirements=req, execute=True, delays=())
    assert not any(op == "install_libraries" for op, _ in fake.ops)


def test_the_report_carries_no_validation_banner():
    res = provision(call=None, workspace_name="acme", scripts=[],
                    execute=False)
    md = render_provision(res)
    assert "DRY RUN" in md
    # The report says what provisioning does, not how the API was validated.
    assert "live-verified" not in md and "20260430 contract" not in md
    assert "live API" not in md


# --- provision_api ---------------------------------------------------------

def test_commands_use_the_documented_api_family():
    cmd = build_provision_command("oci_raw", "list_workspaces", OCID)
    uri = cmd[cmd.index("--target-uri") + 1]
    assert "/20260430/aiDataPlatforms/" in uri
    assert "dataLakes" not in uri


def test_the_aidp_cli_backend_is_refused_not_guessed():
    with pytest.raises(ProvisionBackendUnsupported):
        build_provision_command("aidp_cli", "list_workspaces", OCID)


def test_a_file_upload_goes_through_the_validated_cli_surface():
    # workspace-object create with @local-path: the live-verified upload.
    cmd = build_provision_command(
        "oci_raw", "upload_ws_file", OCID, workspace="ws",
        path="backup-snowflake-migration/scripts/x.py", local_path="/tmp/x.py")
    assert cmd[:3] == ["aidp", "workspace-object", "create"]
    assert "@/tmp/x.py" in cmd and "--is-overwrite" in cmd
    assert not any(a.startswith("/backup") for a in cmd), \
        "workspace paths are relative; a leading slash is a live 400"


def test_library_items_refuse_an_empty_change():
    with pytest.raises(ValueError):
        build_library_items()


def test_job_bodies_match_the_live_verified_task_shape():
    body = build_job_body("j", notebook_path="s/run_j.ipynb", cluster_key="cl")
    assert "schedule" not in body, "migrations are driven runs, not crons"
    assert body["maxConcurrentRuns"] == 1
    task = body["tasks"][0]
    # Live-verified: runIf and a per-task cluster are required; NOTEBOOK_TASK
    # takes notebookPath + source, and is the shape that actually RAN
    # (PYTHON_TASK failed file resolution on the validated build).
    assert task["type"] == "NOTEBOOK_TASK"
    assert task["notebookPath"] == "s/run_j.ipynb"
    assert task["source"] == "WORKSPACE"
    assert task["runIf"] == "ALL_SUCCESS"
    assert task["cluster"] == {"clusterKey": "cl"}


def test_content_paths_are_absolute_and_clean():
    assert content_path("a/", "/b", "c.py") == "/a/b/c.py"


# --------------------------------------------------------------------------
# The driver notebook is the only thing an AIDP job actually runs, so its
# contract is load-bearing: it must put the scripts' own folder on sys.path
# (they import a shared module from there) and read off the /Workspace mount.
# --------------------------------------------------------------------------

def test_the_driver_notebook_makes_the_shared_module_importable():
    src = "".join(build_driver_notebook(
        "backup-snowflake-migration/scripts/00_discover_snowflake.py",
        {})["cells"][0]["source"])
    assert "sys.path.insert(0, os.path.dirname(SCRIPT))" in src, \
        "the scripts import snowmig_source from beside themselves"
    assert src.count("/Workspace/") >= 1, \
        "the workspace tree is mounted at /Workspace on the cluster"


# --------------------------------------------------------------------------
# One AIDP compute cluster per Snowflake warehouse, named after it.
#
# The requirement was explicit: same names, AIDP DEFAULT config, no sizing
# decision now. So the warehouse's size is reported and deliberately not
# translated -- a Snowflake size is not a Spark shape, and COMPUTE_PROPOSAL.md
# keeps that a decision rather than a silent default.
# --------------------------------------------------------------------------

_WAREHOUSES = [{"name": "WH_ETL", "size": "Medium"},
               {"name": "WH BI Team", "size": "X-Small"}]


def test_a_cluster_is_created_for_each_warehouse_with_its_own_name(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_cluster"
               and kw["body"]["displayName"] != "migration_assets"]
    # The awkward name is translated, and the translation is reported.
    assert created == ["wh_etl", "wh_bi_team"]
    mirrored = {t["warehouse"]: t for t in res["warehouse_clusters"]}
    assert mirrored["WH BI Team"]["name"] == "wh_bi_team"
    assert mirrored["WH BI Team"]["renamed"] is True
    assert mirrored["WH BI Team"]["notes"]


def test_the_warehouse_size_is_reported_and_not_translated(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_cluster"]
    # Every mirrored cluster gets the SAME default shape: no sizing is
    # inferred from the warehouse.
    assert len({json.dumps(b["driverConfig"], sort_keys=True)
                for b in bodies}) == 1
    assert res["warehouse_clusters"][0]["source_size"] == "Medium"
    md = render_provision(res)
    assert "Medium" in md
    assert "NOT carried over" in md, \
        "the report must say the sizing decision was left open"


def test_an_existing_warehouse_cluster_is_not_adopted(scripts):
    fake = Fake(clusters=("wh_etl",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_cluster"
               and kw["body"]["displayName"] != "migration_assets"]
    assert created == ["wh_bi_team"], "the existing one is left alone"
    taken = [s for s in res["steps"]
             if s["step"] == "warehouse-cluster"
             and s["action"] == "name_taken"]
    assert len(taken) == 1, "the collision is reported, not silently adopted"
    assert taken[0]["verified"] is False
    assert "not this migration's" in taken[0]["detail"]


def test_one_failed_warehouse_cluster_does_not_stop_the_others(scripts):
    class OneFails(Fake):
        def __call__(self, operation, **kw):
            if operation == "create_cluster" and \
                    kw["body"]["displayName"] == "wh_etl":
                self.ops.append((operation, kw))
                raise RuntimeError("quota exceeded")
            return super().__call__(operation, **kw)

    fake = OneFails()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    steps = {s["detail"].split(" -> ")[0]: s for s in res["steps"]
             if s["step"] == "warehouse-cluster"}
    assert steps["WH_ETL"]["verified"] is False
    assert "quota exceeded" in steps["WH_ETL"]["detail"]
    assert steps["WH BI Team"]["verified"] is True
    # And the migration's own jobs are unaffected: these are the customer's
    # compute, not the migration's.
    assert any(s["step"] == "job" for s in res["steps"])


def test_mirroring_never_touches_the_migration_cluster_binding(scripts):
    fake = Fake()
    provision(call=fake, workspace_name="acme", scripts=scripts,
              warehouse_clusters=_WAREHOUSES, execute=True, delays=())
    bodies = [kw["body"] for op, kw in fake.ops if op == "create_job"]
    assert {b["tasks"][0]["cluster"]["clusterKey"] for b in bodies} == \
        {"cl-migration_assets"}


def test_a_dry_run_lists_the_warehouse_clusters_it_would_create(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=_WAREHOUSES, execute=False)
    assert fake.ops == []
    planned = [s for s in res["steps"] if s["step"] == "warehouse-cluster"]
    assert len(planned) == 2
    assert all(s["verified"] is None for s in planned)
    assert "NOT carried over" in planned[0]["detail"]


def test_a_nameless_warehouse_entry_is_skipped_not_guessed(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=[{"size": "Medium"}], execute=True,
                    delays=())
    assert res["warehouse_clusters"] == []
    assert not any(op == "create_cluster"
                   and kw["body"]["displayName"] != "migration_assets"
                   for op, kw in fake.ops)


def test_the_cluster_body_carries_both_driver_and_worker_config():
    """Live-enumerated by the API: a cluster needs driverConfig AND
    workerConfig (shape + min/max worker count), and the shape is an AIDP
    compute family -- an OCI VM shape like VM.Standard.E4.Flex is rejected."""
    from target.provision_api import DEFAULT_SHAPE, build_cluster_body
    body = build_cluster_body("wh_etl")
    assert body["driverConfig"]["driverShape"] == DEFAULT_SHAPE
    worker = body["workerConfig"]
    assert worker["workerShape"] == DEFAULT_SHAPE
    assert worker["minWorkerCount"] >= 1
    assert worker["maxWorkerCount"] >= worker["minWorkerCount"]
    assert not DEFAULT_SHAPE.startswith("VM."), \
        "an OCI VM shape is not an AIDP compute shape"
    # Also required, and also learned from a 400: the RUNTIME version.
    assert body["clusterRuntimeConfig"]["sparkVersion"].startswith("3.5")


def test_the_dry_run_previews_notebooks_not_their_python_sources():
    """AIDP types a workspace object by extension: `.py` lands as a FILE and no
    job can run it, so execute uploads a generated `.ipynb` per stage and never
    the `engine/dataplane/` sources. The dry run used to list those sources,
    advertising an upload that never happens."""
    out = provision(call=None, workspace_name="ws",
                    scripts=[pathlib.Path("00_discover_snowflake.py")],
                    execute=False)
    uploads = [s["detail"] for s in out["steps"] if s["step"] == "upload"]
    assert uploads, "the dry run must preview the stage uploads"
    assert all(".ipynb" in d for d in uploads), uploads
    assert not any(d.endswith(".py") for d in uploads), uploads


def test_provisioning_creates_the_backup_folder_the_runbook_writes_into(scripts):
    """S6 backs the manifest up before any stage reads it, and S9 backs the full
    plan up before scope is reduced. Only scripts/, plan/ and reports/ were
    created, so the first backup had nowhere to land."""
    fake = Fake()
    provision(call=fake, workspace_name="ws", scripts=scripts, execute=True,
              delays=())
    folders = [kw["path"] for op, kw in fake.ops if op == "create_ws_folder"]
    assert BACKUP_FOLDER in folders, folders


# --- pagination on the provisioning transport -------------------------------
#
# `oci raw-request` surfaces `opc-next-page` under `headers`; the transport
# used to drop it, so jobs.in_flight_runs read one page of jobRuns and could
# miss the very run it exists to guard against.

def _paged_proc(pages):
    """`pages`: page token (None first) -> (items, next token)."""
    import types
    asked = []

    def fake(cmd):
        uri = cmd[cmd.index("--target-uri") + 1] if "--target-uri" in cmd \
            else " ".join(cmd)
        asked.append(uri)
        token = uri.rsplit("page=", 1)[1] if "page=" in uri else None
        items, nxt = pages[token]
        env = {"data": {"items": items}, "status": "200 OK"}
        if nxt:
            env["headers"] = {"opc-next-page": nxt}
        return types.SimpleNamespace(returncode=0, stderr="",
                                     stdout=json.dumps(env))

    return fake, asked


def test_list_job_runs_follows_the_next_page():
    from target.provisioning import make_provision_call
    fake, asked = _paged_proc({None: ([{"key": "run-1", "endTime": 1}], "P2"),
                               "P2": ([{"key": "run-2"}], None)})
    call = make_provision_call(OCID, run_process=fake)
    out = call("list_job_runs", workspace="ws", job_key="j")
    assert [i["key"] for i in out["items"]] == ["run-1", "run-2"]
    assert len(asked) == 2
    assert "jobKey=j&sortBy=timeCreated" in asked[0] and "page=" not in asked[0]
    assert asked[1].endswith("jobKey=j&sortBy=timeCreated&page=P2")


def test_in_flight_runs_sees_a_run_on_page_two():
    from target import jobs
    from target.provisioning import make_provision_call
    fake, _ = _paged_proc({None: ([{"key": "run-1", "endTime": 123}], "P2"),
                           "P2": ([{"key": "run-2", "endTime": None}], None)})
    call = make_provision_call(OCID, run_process=fake)
    assert jobs.in_flight_runs(call, workspace="ws", job_key="j") == ["run-2"]


def test_a_single_page_listing_is_one_request():
    from target.provisioning import make_provision_call
    fake, asked = _paged_proc({None: ([{"key": "ws-1"}], None)})
    call = make_provision_call(OCID, run_process=fake)
    assert call("list_workspaces")["items"] == [{"key": "ws-1"}]
    assert len(asked) == 1 and "page=" not in asked[0]


def test_a_repeating_token_is_a_transport_error_not_a_loop():
    from target.provisioning import ProvisionTransportError, make_provision_call
    fake, asked = _paged_proc({None: ([{"key": "a"}], "P2"),
                               "P2": ([{"key": "b"}], "P2")})
    call = make_provision_call(OCID, run_process=fake)
    with pytest.raises(ProvisionTransportError, match="list_jobs"):
        call("list_jobs", workspace="ws")
    assert len(asked) <= 3


def test_a_workspace_object_listing_with_a_next_page_is_refused_not_truncated():
    """list_ws_objects rides the aidp CLI, whose paging flags are unknown. A
    truncated listing would read uploads as not visible -- or worse, as
    absent; refusing names the problem."""
    import types
    from target.provisioning import ProvisionTransportError, make_provision_call

    def fake(cmd):
        assert cmd[0] == "aidp"
        return types.SimpleNamespace(
            returncode=0, stderr="",
            stdout='Response:\n' + json.dumps(
                {"data": {"items": [{"path": "a/b"}]},
                 "headers": {"opc-next-page": "P2"}}))

    call = make_provision_call(OCID, run_process=fake)
    with pytest.raises(ProvisionTransportError, match="page"):
        call("list_ws_objects", workspace="ws", path="a")


def test_provision_list_commands_carry_the_page_token():
    for op, kw in (("list_workspaces", {}),
                   ("list_clusters", {"workspace": "ws"}),
                   ("list_libraries", {"workspace": "ws", "cluster": "cl"}),
                   ("list_jobs", {"workspace": "ws"}),
                   ("list_task_runs", {"workspace": "ws", "run_key": "r"})):
        plain = build_provision_command("oci_raw", op, OCID, **kw)
        assert "page=" not in " ".join(plain)
        assert build_provision_command("oci_raw", op, OCID, page=None,
                                       **kw) == plain
        uri = build_provision_command("oci_raw", op, OCID, page="T2", **kw)
        uri = uri[uri.index("--target-uri") + 1]
        assert uri.endswith("page=T2") and uri.count("?") == 1, (op, uri)


# --- the cluster POST fails: the workspace record must survive -------------
#
# A cluster POSTed before the workspace reports ACTIVE
# is a 409 "ongoing operation". That POST was not guarded, so the
# ProvisionTransportError propagated out of provision(), cmd_provision never
# wrote provision_result.json or PROVISION.md, and the workspace created two
# seconds earlier was on record nowhere -- the next run halted on name_taken
# and called it someone else's.

_409 = ("create_cluster failed (exit 0): 409 Conflict ongoing operation on "
        "workspace")


class ClusterConflicts(Fake):
    """The first `fail_times` cluster POSTs raise `text`, then it works."""

    def __init__(self, fail_times, text=_409, **kw):
        super().__init__(**kw)
        self.fail_times = fail_times
        self.text = text

    def __call__(self, operation, **kw):
        if operation == "create_cluster" and self.fail_times:
            self.fail_times -= 1
            self.ops.append((operation, kw))
            from target.provisioning import ProvisionTransportError
            raise ProvisionTransportError(self.text)
        return super().__call__(operation, **kw)


def test_a_409_on_the_cluster_post_is_retried_then_recorded(scripts):
    fake = ClusterConflicts(2)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))
    cluster = [s for s in res["steps"] if s["step"] == "cluster"]
    assert [s["action"] for s in cluster] == ["retried", "retried", "created"]
    assert [s["verified"] for s in cluster] == [None, None, True]
    assert [op for op, _ in fake.ops].count("create_cluster") == 3
    assert any(s["step"] == "job" for s in res["steps"]), \
        "a retry that succeeds is not a halt"


def test_a_cluster_post_that_keeps_failing_returns_the_partial_record(scripts):
    fake = ClusterConflicts(99)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))          # returns, no raise
    steps = [(s["step"], s["action"], s["verified"]) for s in res["steps"]]
    assert ("workspace", "created", True) in steps
    assert ("cluster", "failed", False) in steps
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "--reuse-existing" in halt["detail"]
    assert "ws-acme" in halt["detail"], "the record names the key"
    assert res["workspace"]["key"] == "ws-acme"
    ops = [op for op, _ in fake.ops]
    assert "upload_ws_file" not in ops and "create_job" not in ops


def test_a_non_conflict_cluster_error_is_not_retried(scripts):
    fake = ClusterConflicts(
        99, text="create_cluster failed (exit 1): 403 NotAuthorized")
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))
    assert [op for op, _ in fake.ops].count("create_cluster") == 1
    actions = {(s["step"], s["action"]) for s in res["steps"]}
    assert ("cluster", "failed") in actions
    assert ("cluster", "retried") not in actions
    assert any(s["step"] == "halt" for s in res["steps"])


def test_a_failed_cluster_listing_is_recorded_not_raised(scripts):
    fake = Fake(fail={"list_clusters"})
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    steps = [(s["step"], s["action"], s["verified"]) for s in res["steps"]]
    assert ("workspace", "created", True) in steps
    assert ("cluster", "failed", False) in steps
    assert any(s["step"] == "halt" for s in res["steps"])
    assert "create_cluster" not in [op for op, _ in fake.ops], \
        "could not look is not absent"


# The two calls past the cluster that still escaped provision(): the job
# listing, and the cluster listing inside the warehouse loop. An expired
# session token on either lost provision_result.json and PROVISION.md with
# the workspace and cluster already created.

def test_a_failed_job_listing_after_the_cluster_is_recorded_not_raised(scripts):
    fake = Fake(fail={"list_jobs"})
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())                 # returns, no raise
    steps = [(s["step"], s["action"], s["verified"]) for s in res["steps"]]
    assert ("workspace", "created", True) in steps
    assert ("cluster", "created", True) in steps
    assert [st[:2] for st in steps[-2:]] == [("job", "failed"),
                                             ("halt", "stopped")]
    halt = res["steps"][-1]
    assert "--reuse-existing" in halt["detail"] and "ws-acme" in halt["detail"]
    assert "create_job" not in [op for op, _ in fake.ops]


class ClusterListingFailsLater(Fake):
    """`list_clusters` answers the migration cluster's look and its poll,
    then fails: the warehouse loop is the third caller."""

    def __init__(self, fail_from, **kw):
        super().__init__(**kw)
        self.fail_from = fail_from
        self.cluster_lists = 0

    def __call__(self, operation, **kw):
        if operation == "list_clusters":
            self.cluster_lists += 1
            if self.cluster_lists >= self.fail_from:
                self.ops.append((operation, kw))
                raise RuntimeError(
                    "list_clusters failed (exit 1): 401 NotAuthenticated")
        return super().__call__(operation, **kw)


def test_a_failed_warehouse_cluster_listing_is_recorded_and_the_rest_continue(
        scripts):
    fake = ClusterListingFailsLater(3)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    warehouse_clusters=[{"name": "WH_ETL", "size": "M"}],
                    execute=True, delays=())
    steps = [(s["step"], s["action"], s["verified"]) for s in res["steps"]]
    assert ("cluster", "created", True) in steps
    failed = [s for s in res["steps"]
              if s["step"] == "warehouse-cluster" and s["action"] == "failed"]
    assert len(failed) == 1 and failed[0]["verified"] is False
    assert "WH_ETL" in failed[0]["detail"]
    assert "list_clusters" in failed[0]["detail"]
    # Customer compute; the migration's own jobs still get created.
    assert [op for op, _ in fake.ops].count("create_job") == len(JOB_SPECS)


class Settling(Fake):
    """A created workspace reports CREATING until the `active_after`-th
    listing, then ACTIVE -- the way the live API behaves for a few seconds
    after the POST returns."""

    def __init__(self, active_after):
        super().__init__()
        self.active_after = active_after
        self.lists = 0

    def __call__(self, operation, **kw):
        if operation == "create_workspace":
            self.ops.append((operation, kw))
            name = kw["body"]["displayName"]
            self.workspaces.append({"displayName": name, "key": f"ws-{name}",
                                    "lifecycleState": "CREATING"})
            return {}
        if operation == "list_workspaces":
            self.lists += 1
            if self.lists >= self.active_after:
                for w in self.workspaces:
                    w["lifecycleState"] = "ACTIVE"
        return super().__call__(operation, **kw)


def test_the_workspace_is_waited_on_until_active(scripts):
    fake = Settling(active_after=4)      # 1 look-first + 3 polls
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0, 0))
    ops = [op for op, _ in fake.ops]
    before_cluster = ops[:ops.index("create_cluster")]
    assert before_cluster.count("list_workspaces") == 4, \
        "the cluster POST waits for ACTIVE, not just for visibility"
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["action"] == "created" and ws["verified"] is True


def test_a_workspace_that_stays_creating_is_recorded_and_not_halted(scripts):
    fake = Settling(active_after=10 ** 6)
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0,))
    assert "create_cluster" in [op for op, _ in fake.ops], \
        "a slow ACTIVE is not a reason to stop; the 409 retry covers it"
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["action"] == "created" and "CREATING" in ws["detail"]


def test_the_name_taken_halt_names_the_operator_s_own_orphan(scripts):
    fake = Fake(workspaces=("acme",))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "previous run" in halt["detail"].lower()
    assert "--reuse-existing" in halt["detail"]


# --- --source-config: only the snowflake: block travels, and it is said so --
#
# The operator's whole migration config used to be appended to plan_files and
# uploaded verbatim: the Snowflake password or PEM AND the aidp: block
# (DataLake OCID, target coordinates), as a workspace object readable by every
# member and every cluster, with a PROVISION.md row that said only
# "snowmig-config.yaml -> .../plan/snowmig-config.yaml". A `key_path:` config
# went up unchanged too, and failed five minutes later on the cluster with a
# FileNotFoundError for a laptop path.

_FAKE_PASSWORD = "FAKE-PASSWORD-not-real-123"
_FAKE_PEM = "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n"


def _config(tmp_path, **snowflake):
    import yaml
    block = {"account": "ACME-TEST", "user": "READER", "warehouse": "WH",
             "database": "DB", "auth": "password", "password": _FAKE_PASSWORD}
    block.update(snowflake)
    block = {k: v for k, v in block.items() if v is not None}
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(yaml.safe_dump({
        "snowflake": block,
        "aidp": {"datalake_ocid": "ocid1.aidataplatform.oc1.iad.fakefakefake",
                 "catalog": "lake"}}), encoding="utf-8")
    return cfg


def test_the_source_config_upload_carries_only_the_snowflake_block(scripts,
                                                                    tmp_path):
    cfg = _config(tmp_path)
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    plan_files=[cfg], source_config=cfg, execute=True,
                    delays=())
    uploads = [kw for op, kw in fake.ops if op == "upload_ws_file"]
    assert not any(kw["local_path"] == str(cfg) for kw in uploads), \
        "the operator's file itself never travels"
    assert f"{PLAN_FOLDER}/snowmig-config.yaml" not in fake.contents
    remote = f"{PLAN_FOLDER}/snowmig-config.json"
    body = json.loads(fake.contents[remote]["body"])
    assert set(body) == {"snowflake"}
    assert body["snowflake"]["password"] == _FAKE_PASSWORD
    blob = json.dumps(body)
    assert "aidp" not in blob and "datalake_ocid" not in blob
    assert not pathlib.Path(fake.contents[remote]["local"]).exists(), \
        "the derived copy does not outlive the upload"
    assert res["credential_objects"] == [remote]
    step = next(s for s in res["steps"]
                if s["step"] == "upload" and remote in s["detail"])
    assert step["verified"] is True and "CREDENTIAL" in step["detail"]
    # The notebooks read the derived copy off the mount.
    nb = json.loads(
        fake.contents[f"{SCRIPTS_FOLDER}/00_discover_snowflake.ipynb"]["body"])
    params = "".join(nb["cells"][1]["source"])
    mount = REPORTS_FOLDER.rsplit("/", 1)[0]
    assert f"'source-config': '{mount}/plan/snowmig-config.json'" in params


def test_the_dry_run_names_the_credential_object(scripts, tmp_path):
    cfg = _config(tmp_path)
    res = provision(call=None, workspace_name="acme", scripts=scripts,
                    plan_files=[cfg], source_config=cfg, execute=False)
    remote = f"{PLAN_FOLDER}/snowmig-config.json"
    assert res["credential_objects"] == [remote]
    details = [s["detail"] for s in res["steps"] if s["step"] == "upload"]
    assert any("CREDENTIAL" in d and remote in d for d in details), details
    assert not any(d.endswith("snowmig-config.yaml") for d in details), \
        "the raw file is not previewed as an upload"
    md = render_provision(res)
    assert "Credential placed on the workspace" in md and remote in md
    assert _FAKE_PASSWORD not in md


def test_a_plan_file_that_is_not_the_source_config_has_no_credential_wording(
        scripts, tmp_path):
    plan = tmp_path / "plan.json"
    plan.write_text("{}", encoding="utf-8")
    res = provision(call=None, workspace_name="acme", scripts=scripts,
                    plan_files=[plan], execute=False)
    assert res["credential_objects"] == []
    assert not any("CREDENTIAL" in s["detail"] for s in res["steps"])
    assert "Credential placed" not in render_provision(res)


def test_a_path_form_secret_is_refused_before_any_upload(scripts, tmp_path):
    from migration_config import ConfigError
    pem = tmp_path / "rsa_key.p8"
    pem.write_text(_FAKE_PEM, encoding="utf-8")
    cfg = _config(tmp_path, auth="keypair", key_path=str(pem), password=None)
    fake = Fake()
    for execute in (False, True):
        with pytest.raises(ConfigError) as exc:
            provision(call=fake if execute else None, workspace_name="acme",
                      scripts=scripts, source_config=cfg, execute=execute,
                      delays=())
        message = str(exc.value)
        assert "key_path" in message and "inline" in message.lower()
        assert "/Workspace" in message
    assert fake.ops == [], "refused before anything reached AIDP"


def test_an_inline_secret_config_is_accepted(scripts, tmp_path):
    cfg = _config(tmp_path, auth="keypair", private_key=_FAKE_PEM,
                  password=None)
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    source_config=cfg, execute=True, delays=())
    assert any(s["step"] == "job" for s in res["steps"])
    body = json.loads(
        fake.contents[f"{PLAN_FOLDER}/snowmig-config.json"]["body"])
    assert body["snowflake"]["private_key"] == _FAKE_PEM


# --- --reuse-existing keeps the stage notebooks it finds ---------------------
#
# Every `provision --execute` used to regenerate all four stage notebooks and
# upload them with --is-overwrite, before even looking at whether the job
# existed. Operators set `schema`, `mode`, `verify` and `counts` by editing
# the PARAMS cell in the console -- provision has no flags for them -- so a
# later `--reuse-existing` (the documented resume after the workspace/cluster
# 409) reset them: `schema` back to None, `verify` back to `counts`, the
# reconcile `counts` back to False, with "reused (stage notebook refreshed)"
# as the only trace.

def _seeded():
    fake = Fake(workspaces=("acme",), clusters=("migration_assets",),
                jobs=tuple(s["name"] for s in JOB_SPECS))
    for spec in JOB_SPECS:
        fake.contents[f'{SCRIPTS_FOLDER}/{spec["notebook"]}'] = {
            "type": "NOTEBOOK", "body": "console-edited"}
    return fake


def test_reuse_existing_keeps_an_existing_stage_notebook(scripts):
    fake = _seeded()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), reuse_existing=True,
                    target_catalog="mig")
    uploads = [kw for op, kw in fake.ops
               if op == "upload_ws_file" and kw.get("object_type") == "NOTEBOOK"]
    assert {kw["path"] for kw in uploads} <= {f"{SCRIPTS_FOLDER}/{DIAGNOSE_NOTEBOOK_NAME}"}, \
        "nothing already there is overwritten; only the missing diagnosis is added"
    assert fake.contents[f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]["body"] \
        == "console-edited"
    notebooks = [s for s in res["steps"] if s["step"] == "notebook"]
    assert [s["action"] for s in notebooks] == ["kept"] * len(JOB_SPECS)
    assert all(s["verified"] is True and "--refresh-notebooks" in s["detail"]
               for s in notebooks)
    assert res["notebooks_kept"] == [s["notebook"] for s in JOB_SPECS]
    jobs = [s for s in res["steps"] if s["step"] == "job"]
    assert len(jobs) == len(JOB_SPECS)
    assert all(s["action"] == "reused" and "kept" in s["detail"] for s in jobs)
    md = render_provision(res)
    assert "kept" in md and "--refresh-notebooks" in md
    assert "02_copy_schema.ipynb" in md


def test_refresh_notebooks_overwrites_and_says_so(scripts):
    fake = _seeded()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), reuse_existing=True,
                    refresh_notebooks=True, target_catalog="mig")
    body = fake.contents[f"{SCRIPTS_FOLDER}/02_copy_schema.ipynb"]["body"]
    assert body != "console-edited"
    params = "".join(json.loads(body)["cells"][1]["source"])
    assert "'target-catalog': 'mig'" in params
    assert res["notebooks_kept"] == []
    jobs = [s for s in res["steps"] if s["step"] == "job"]
    assert all(s["action"] == "reused" and "OVERWRITTEN" in s["detail"]
               and "console edits" in s["detail"] for s in jobs)
    assert "kept as found" not in render_provision(res)


def test_reuse_existing_still_uploads_a_notebook_that_is_missing(scripts):
    fake = _seeded()
    del fake.contents[f"{SCRIPTS_FOLDER}/03_reconcile.ipynb"]
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), reuse_existing=True)
    actions = sorted(s["action"] for s in res["steps"]
                     if s["step"] == "notebook")
    assert actions == ["kept", "kept", "kept", "uploaded"]
    assert res["notebooks_kept"] == [s["notebook"] for s in JOB_SPECS[:3]]
    assert f"{SCRIPTS_FOLDER}/03_reconcile.ipynb" in fake.contents


def test_a_fresh_provision_still_uploads_every_notebook(scripts):
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    assert [s["action"] for s in res["steps"] if s["step"] == "notebook"] \
        == ["uploaded"] * len(JOB_SPECS)
    assert res["notebooks_kept"] == []


def test_an_unlistable_scripts_folder_neither_overwrites_nor_creates(scripts):
    class NoScriptsListing(Fake):
        def __call__(self, operation, **kw):
            if operation == "list_ws_objects" and kw["path"] == SCRIPTS_FOLDER:
                self.ops.append((operation, kw))
                raise RuntimeError("workspace-object list: 503")
            return super().__call__(operation, **kw)

    fake = NoScriptsListing(workspaces=("acme",), clusters=("migration_assets",),
                            jobs=tuple(s["name"] for s in JOB_SPECS))
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(), reuse_existing=True)
    notebooks = [s for s in res["steps"] if s["step"] == "notebook"]
    assert [s["action"] for s in notebooks] == ["failed"] * len(JOB_SPECS)
    assert all("could not list" in s["detail"] for s in notebooks)
    assert not any(op == "upload_ws_file" and kw.get("object_type") == "NOTEBOOK"
                   for op, kw in fake.ops), "could not look is not absent"


# --- testConnection carries the Snowflake credential ------------------------
# `create_catalog` spools its credential-bearing body to a temp file so `ps`
# (and process-creation auditing) never see it. The testConnection body carries
# the SAME credential -- password, or the whole private key PEM -- and went
# inline on the `oci` argv.

_SECRET = "ZqTrickyPW_93-hunter2"
_PEM = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASC\n-----END PRIVATE KEY-----"
_PASSPHRASE = "pass-phrase-Q7"


def _ok(cmd):
    return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")


def _test_connection_body(**props):
    return build_test_connection_body(
        "cat-key", connection_properties={"SNOWFLAKE_USERNAME": "U", **props},
        display_name="src")


@pytest.mark.parametrize("props,secrets", [
    ({"SNOWFLAKE_PASSWORD": _SECRET}, [_SECRET]),
    ({"SNOWFLAKE_PRIVATE_KEY_CONTENT": _PEM,
      "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": _PASSPHRASE},
     ["BEGIN PRIVATE KEY", "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", _PASSPHRASE]),
], ids=["password", "keypair"])
def test_the_test_connection_body_travels_by_file_not_argv(props, secrets,
                                                           capsys):
    seen = {}

    def fake(cmd):
        seen["cmd"] = list(cmd)
        path = next((a for a in cmd if a.startswith("file://")), None)
        seen["file"] = path
        if path:
            with open(path[len("file://"):], encoding="utf-8") as fh:
                seen["spooled"] = fh.read()
        return _ok(cmd)

    call = make_provision_call(OCID, run_process=fake)
    call("test_connection", body=_test_connection_body(**props))
    blob = " ".join(seen["cmd"])
    for secret in secrets:
        assert secret not in blob, "the credential must not be an argv element"
    assert seen["file"], "the body must travel by file"
    assert seen["cmd"][seen["cmd"].index("--request-body") + 1] == seen["file"]
    for secret in secrets:
        assert secret in seen["spooled"], "the CLI reads the real body"
        assert secret not in capsys.readouterr().out


def test_the_test_connection_spool_is_removed_after_the_call():
    seen = {}

    def fake(cmd):
        seen["path"] = next(a[len("file://"):] for a in cmd
                            if a.startswith("file://"))
        assert os.path.exists(seen["path"]), \
            "the file must exist while the CLI runs"
        return _ok(cmd)

    call = make_provision_call(OCID, run_process=fake)
    call("test_connection",
         body=_test_connection_body(SNOWFLAKE_PASSWORD=_SECRET))
    assert not os.path.exists(seen["path"]), \
        "the spool must not outlive the call"


def test_the_test_connection_spool_is_removed_even_when_the_call_fails():
    seen = {}

    def fake(cmd):
        seen["path"] = next(a[len("file://"):] for a in cmd
                            if a.startswith("file://"))
        return types.SimpleNamespace(returncode=1, stdout="", stderr="denied")

    call = make_provision_call(OCID, run_process=fake)
    with pytest.raises(ProvisionTransportError):
        call("test_connection",
             body=_test_connection_body(SNOWFLAKE_PASSWORD=_SECRET))
    assert not os.path.exists(seen["path"])


def test_build_provision_command_prefers_a_body_file_for_test_connection():
    body = _test_connection_body(SNOWFLAKE_PASSWORD=_SECRET)
    cmd = build_provision_command("oci_raw", "test_connection", OCID,
                                  body=body, body_file="/tmp/x.json")
    assert cmd[cmd.index("--request-body") + 1] == "file:///tmp/x.json"
    assert json.dumps(body) not in cmd
    # The body-only form keeps working, so the builder stays usable alone.
    plain = build_provision_command("oci_raw", "test_connection", OCID,
                                    body=body)
    assert cmd[cmd.index("--target-uri") + 1].endswith(
        "/actions/testConnection")
    assert json.dumps(body) in plain


def _catalog_transport(fake):
    from target.coords import resolve_target
    from target.runner import make_call
    target = resolve_target(datalake_ocid=OCID, workspace="w",
                            cluster_id="c", catalog="MYDB")
    return make_call(target, backend="oci_raw", run_process=fake)


@pytest.mark.parametrize("transport,operation,body", [
    (_catalog_transport, "create_catalog",
     {"displayName": "src", "catalogType": "EXTERNAL",
      "connectionDetails": {"connectionProperties": {
          "SNOWFLAKE_PASSWORD": _SECRET}}}),
    (lambda fake: make_provision_call(OCID, run_process=fake),
     "test_connection", _test_connection_body(SNOWFLAKE_PASSWORD=_SECRET)),
], ids=["runner.create_catalog", "provisioning.test_connection"])
def test_no_transport_puts_connection_details_on_argv(transport, operation,
                                                      body):
    """The guard that would have caught the drift: two transports, one rule.
    Whatever carries `connectionDetails` never appears as an argv element."""
    seen = {}

    def fake(cmd):
        seen["cmd"] = list(cmd)
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps({"data": {"key": "k"}}),
            stderr="")

    transport(fake)(operation, body=body)
    assert not any("connectionDetails" in a for a in seen["cmd"]), seen["cmd"]
    assert _SECRET not in " ".join(seen["cmd"])


# ------------------------------- a CLI call that never returns, live 2026-09-24
#
# `provision --execute` hung for one hour and forty-seven minutes. A single
# `oci` child process, started two minutes into the run, never exited; the
# parent sat in subprocess.run() waiting for it with no timeout, printed
# nothing, and wrote no result. A Spark cluster billed for the whole of it.
# Killing the child by hand let the parent continue immediately.
#
# None of the three subprocess.run() call sites passed `timeout=`. A hung
# CLI is not exotic -- a stalled TLS handshake or a proxy black hole does
# it -- and the cost of not bounding it is measured in cluster-hours.

def test_every_subprocess_call_site_bounds_its_wait():
    """Read the sources: no subprocess.run without a timeout."""
    import re
    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = []
    for src in root.rglob("*.py"):
        if "tests" in src.parts:
            continue
        text = src.read_text(encoding="utf-8")
        for m in re.finditer(r"subprocess\.run\(", text):
            # the call's argument list, to its balancing paren
            i, depth = m.end(), 1
            while i < len(text) and depth:
                depth += (text[i] == "(") - (text[i] == ")")
                i += 1
            if "timeout=" not in text[m.end():i]:
                line = text[:m.start()].count("\n") + 1
                offenders.append(f"{src.relative_to(root)}:{line}")
    assert not offenders, (
        "subprocess.run without timeout=: " + ", ".join(offenders))


def test_a_timed_out_cli_call_is_reported_not_raised():
    """The operator gets a named failure, not a traceback and not a hang."""
    import subprocess
    from target.runner import run_cli, CliTimeout

    def hang(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 1))

    with pytest.raises(CliTimeout) as e:
        run_cli(["oci", "raw-request", "--target-uri", "https://x"],
                run_process=hang, timeout=1)
    msg = str(e.value)
    assert "1" in msg and "timed out" in msg.lower()
    assert "oci" in msg


def test_the_timeout_is_long_enough_for_a_real_call():
    """Bounded, not impatient: a cluster create legitimately takes minutes."""
    from target.runner import DEFAULT_CLI_TIMEOUT
    assert DEFAULT_CLI_TIMEOUT >= 300


# --------------- the stage parameter the refusal message promised
#
# Live 2026-09-24. `run --param schema=CORE` is refused, correctly: AIDP job
# parameters reach a notebook as neither argv nor environment, so the value
# would be silently ignored. The refusal then named
# `provision --refresh-notebooks` as the way to set stage parameters -- and
# provision writes only five coordinates into PARAMS and has no flag for any
# other. `schema` is REQUIRED by 02_copy_schema and is exactly the one it
# could not supply, so following the advice re-provisioned everything and
# left `schema` None.

def test_a_stage_param_reaches_the_notebook_params_cell():
    from target.stage_notebooks import STAGES, build_stage_notebook
    stage = next(s for s in STAGES if s.key == "copy_schema")
    nb = build_stage_notebook(stage, overrides={"schema": "SALES",
                                                "target-catalog": "lake"})
    params = next(c for c in nb["cells"]
                  if "PARAMS = {" in "".join(c["source"]))
    text = "".join(params["source"])
    assert "'schema': 'SALES'" in text, text[:400]


def test_a_stage_that_does_not_declare_the_name_ignores_it():
    from target.stage_notebooks import STAGES, build_stage_notebook
    stage = next(s for s in STAGES if s.key == "reconcile")
    nb = build_stage_notebook(stage, overrides={"schema": "SALES",
                                                "target-catalog": "lake"})
    text = "".join("".join(c["source"]) for c in nb["cells"]
                   if "PARAMS = {" in "".join(c["source"]))
    assert "'schema'" not in text, text[:300]


def test_an_explicit_stage_param_outranks_a_derived_coordinate():
    """The operator naming a value beats provision deriving one."""
    from target.stage_notebooks import STAGES, build_stage_notebook
    stage = next(s for s in STAGES if s.key == "copy_schema")
    nb = build_stage_notebook(stage, overrides={"target-catalog": "chosen"})
    text = "".join("".join(c["source"]) for c in nb["cells"]
                   if "PARAMS = {" in "".join(c["source"]))
    assert "'target-catalog': 'chosen'" in text


def test_the_refusal_names_the_flag_that_actually_works():
    """The message may not send an operator down a path that cannot set
    the value they asked for."""
    import snowmig
    src = pathlib.Path(snowmig.__file__).read_text(encoding="utf-8")
    i = src.index("--param is refused: a run-level job parameter")
    block = src[i:i + 3000]
    assert "--stage-param" in block, block[:500]


# ---------------- one listing per folder, not one per file
#
# Repeated workspace
# listings compound with the pagination-following in collect_pages. The
# plan-file loop uploaded a file and then listed the WHOLE folder to verify
# it, once per file. Each listing is a separate `aidp` CLI process; the
# live run made seven of them.
#
# The read-back discipline is the point and does not change -- a 2xx is
# still not the claim. It is the same evidence gathered once.

def _count_ops(ops, name):
    return sum(1 for op, _kw in ops if op == name)


def test_plan_files_are_verified_with_one_listing_for_the_folder(tmp_path):
    files = []
    for n in ("inventory.json", "plan.json", "ddl_plan.json"):
        f = tmp_path / n
        f.write_text("{}", encoding="utf-8")
        files.append(f)

    ops = []
    uploaded = []

    def call(operation, **kw):
        ops.append((operation, kw))
        if operation == "upload_ws_file":
            uploaded.append(kw["path"].rsplit("/", 1)[-1])
            return {}
        if operation == "list_ws_objects":
            return {"items": [{"displayName": n} for n in uploaded]}
        if operation == "list_workspaces":
            return {"items": [{"displayName": "ws", "key": "wsk",
                               "lifecycleState": "ACTIVE"}]}
        if operation == "list_clusters":
            return {"items": [{"displayName": "c", "key": "ck",
                               "lifecycleState": "ACTIVE"}]}
        if operation == "list_jobs":
            return {"items": []}
        return {}

    out = provision(workspace_name="ws", cluster_name="c", scripts=[],
                    plan_files=files, execute=True, call=call,
                    reuse_existing=True, delays=())
    plan_listings = [kw for op, kw in ops
                     if op == "list_ws_objects"
                     and kw.get("path") == PLAN_FOLDER]
    plan_uploads = [kw for op, kw in ops if op == "upload_ws_file"
                    and str(kw.get("path", "")).startswith(PLAN_FOLDER)]
    assert len(plan_uploads) == 3, plan_uploads
    assert len(plan_listings) == 1, (
        f"one listing verifies the whole folder; got {len(plan_listings)}")
    plan_steps = [s for s in out["steps"] if s["step"] == "upload"
                  and PLAN_FOLDER in str(s["detail"])]
    assert plan_steps and all(s["verified"] for s in plan_steps), plan_steps


def test_a_file_whose_upload_raised_is_failed_not_merely_unseen(tmp_path):
    good = tmp_path / "plan.json"
    good.write_text("{}", encoding="utf-8")
    bad = tmp_path / "ddl_plan.json"
    bad.write_text("{}", encoding="utf-8")

    def call(operation, **kw):
        if operation == "upload_ws_file" and kw["path"].endswith("ddl_plan.json"):
            raise RuntimeError("403 Forbidden")
        if operation == "list_ws_objects":
            return {"items": [{"displayName": "plan.json"}]}
        if operation == "list_workspaces":
            return {"items": [{"displayName": "ws", "key": "wsk",
                               "lifecycleState": "ACTIVE"}]}
        if operation == "list_clusters":
            return {"items": [{"displayName": "c", "key": "ck",
                               "lifecycleState": "ACTIVE"}]}
        if operation == "list_jobs":
            return {"items": []}
        return {}

    out = provision(workspace_name="ws", cluster_name="c", scripts=[],
                    plan_files=[good, bad], execute=True, call=call,
                    reuse_existing=True, delays=())
    by_detail = {s["detail"].split(":")[0]: s for s in out["steps"]
                 if s["step"] == "upload"}
    failed = [s for s in out["steps"]
              if s["step"] == "upload" and s["action"] == "failed"]
    assert failed, out["steps"]
    assert "403" in failed[0]["detail"]
    assert any(s["action"] == "uploaded" for s in out["steps"]
               if s["step"] == "upload")


# --- a read-back that could not look is not "never became visible" ----------
#
# Found on review, reproduced with the real provision() and render_provision.
# _poll wrapped every listing in `except Exception: found = None`, so a 401
# or 503 on the listing right after an accepted workspace or cluster create
# was indistinguishable from "not visible yet": the halt said "the workspace
# never became visible; nothing else was attempted", and the error text was
# in neither provision_result.json nor PROVISION.md. "Could not look" read
# as "absent", and a plain re-run then halted name_taken. The cluster halt
# also left out the resume hint every other post-workspace halt carries.

class _BlindAfterCreate(Fake):
    """Every listing of `listing` raises once `create` has been accepted."""

    def __init__(self, create, listing, error, **kw):
        super().__init__(**kw)
        self.blind_create, self.blind_listing = create, listing
        self.blind_error, self.blind = error, False

    def __call__(self, operation, **kw):
        if operation == self.blind_listing and self.blind:
            self.ops.append((operation, kw))
            raise RuntimeError(self.blind_error)
        result = super().__call__(operation, **kw)
        if operation == self.blind_create:
            self.blind = True
        return result


def test_a_workspace_read_back_that_errors_is_recorded_not_called_invisible(
        scripts):
    fake = _BlindAfterCreate("create_workspace", "list_workspaces",
                             "503 Service Unavailable")
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0, 0))
    ws = next(s for s in res["steps"] if s["step"] == "workspace")
    assert ws["verified"] is False
    assert "read_back_failed" in ws["detail"] and "503" in ws["detail"]
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "never became visible" not in halt["detail"]
    assert "could not be listed" in halt["detail"]
    assert "--reuse-existing" in halt["detail"]
    assert "503" in render_provision(res)
    assert "503" in json.dumps(res)


def test_a_cluster_read_back_that_errors_is_recorded_and_says_how_to_resume(
        scripts):
    fake = _BlindAfterCreate("create_cluster", "list_clusters",
                             "401 NotAuthenticated")
    res = provision(call=fake, workspace_name="acme", scripts=scripts,
                    execute=True, delays=(0,))
    cluster = next(s for s in res["steps"] if s["step"] == "cluster")
    assert cluster["verified"] is False
    assert "read_back_failed" in cluster["detail"]
    assert "401" in cluster["detail"]
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "never became visible" not in halt["detail"]
    assert "could not be listed" in halt["detail"]
    assert "--reuse-existing" in halt["detail"], "the resume hint"
    assert "401" in render_provision(res)


def test_a_cluster_that_never_appears_still_says_how_to_resume(scripts):
    class Invisible(Fake):
        def __call__(self, operation, **kw):
            if operation == "create_cluster":
                self.ops.append((operation, kw))
                return {}
            return super().__call__(operation, **kw)

    res = provision(call=Invisible(), workspace_name="acme", scripts=scripts,
                    execute=True, delays=())
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert "never became visible" in halt["detail"]
    assert "--reuse-existing" in halt["detail"]
