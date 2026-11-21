"""REF-208: dbx_export must write the cluster env for the workspace it creates
the cluster in.

main() honoured --lake-ocid / --workspace-id for the AIDP client but handed
pre-flight and Phase 3 the YAML config, so a run exported into an overridden
workspace wrote the YAML workspace ID into the cluster env. The migrated job
trigger posts to $AIDP_WORKSPACE_ID when it is set, so an armed trigger on that
cluster would target the YAML workspace (it fails there with a 404 for the
foreign job key, after the Credential Store reads).

Grid pinned here at the orchestration level: {YAML only, CLI lake, CLI
workspace, both} x {AIDP client, pre-flight cfg, cluster env}, and the loaded
YAML namespace left unchanged.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from dbx_export.cluster_converter import convert_dbx_cluster

YAML_LAKE = "ocid1.aidataplatform.oc1.ap-mumbai-1.yamllake208"
YAML_WS = "11111111-2222-4333-8444-555555555555"
CLI_LAKE = "ocid1.aidataplatform.oc1.us-ashburn-1.clilake208"
CLI_WS = "99999999-8888-4777-8666-555555555555"


@pytest.mark.parametrize("cli_lake, cli_ws", [
    (None, None), (CLI_LAKE, None), (None, CLI_WS), (CLI_LAKE, CLI_WS),
], ids=["yaml_only", "cli_lake", "cli_workspace", "cli_both"])
def test_exporter_cluster_env_targets_the_same_workspace_as_the_client(
        monkeypatch, tmp_path, cli_lake, cli_ws):
    import aidp_config
    import dbx_export.__main__ as exporter
    import dbx_export.aidp_client as aidp_client
    from dbx_export.dependency_resolver import Graph, JobNode, NotebookNode
    from dbx_export.jar_discovery import JarDiscoveryResult

    graph = Graph()
    graph.jobs["42"] = JobNode(dbx_job_id="42", depth=0, job_def={"settings": {
        "job_clusters": [{"job_cluster_key": "cpu", "new_cluster": {}}],
        "tasks": [{"task_key": "task", "job_cluster_key": "cpu",
                   "notebook_task": {"notebook_path": "/task"}}],
    }})
    graph.notebooks["/task"] = NotebookNode(
        dbx_path="/task", role="task", referenced_by=["job:42/task:task"],
        nb_dict={"cells": [{"cell_type": "code", "source": "x = 1"}]})
    yaml_aidp = SimpleNamespace(lake_ocid=YAML_LAKE, workspace_id=YAML_WS, oci_profile="DEFAULT")
    config = SimpleNamespace(aidp=yaml_aidp, paths=SimpleNamespace(export_output="/Workspace/export"),
                             dbx_source=SimpleNamespace(), spark_config={}, autoscale_config={})
    for variable in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DBX_PROXY",
                     "DBX_PROXY_BASIC_USER", "DBX_PROXY_BASIC_PASS", "MLFLOW_TRACKING_URI"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(aidp_config, "load_config", lambda **kw: config)
    monkeypatch.setattr(exporter, "configured_lib_repository_dir", lambda *a: "/Workspace/libs")
    monkeypatch.setattr(aidp_client, "set_artifact_alias_repository", lambda *a: None)
    monkeypatch.setattr(exporter, "get_oci_signer", lambda **kw: ({}, object()))
    monkeypatch.setattr(exporter, "DbxClient", lambda *a, **kw: object())
    clients = []
    monkeypatch.setattr(exporter, "AidpClient",
                        lambda *a, **kw: clients.append(kw["config"]) or object())
    monkeypatch.setattr(exporter, "_limits_startup_gate", lambda *a: None)
    monkeypatch.setattr(exporter.ph_mod, "new_run_id", lambda: "isu208")
    monkeypatch.setattr(exporter, "phase1_discover", lambda *a, **kw: graph)
    for name in ("phase1_discover_existing_cluster_libraries", "phase1_fetch_files",
                 "phase1_discover_init_script_artifacts", "resolve_failed_jar_fallbacks"):
        monkeypatch.setattr(exporter, name, lambda *a, **kw: None)
    monkeypatch.setattr(exporter, "discover_notebook_jars",
                        lambda *a: JarDiscoveryResult(fallback_path="/Workspace/libs"))
    preflight_cfgs = []
    monkeypatch.setattr(exporter, "pre_flight",
                        lambda *a, **kw: preflight_cfgs.append(kw["cfg"]) or "mock pre-flight")
    monkeypatch.setattr(exporter, "phase2_upload_notebooks", lambda **kw: ({}, []))
    monkeypatch.setattr(exporter, "phase2_upload_files", lambda **kw: ({}, False))
    cluster_envs = []

    def phase3(**kwargs):
        payload = convert_dbx_cluster({"node_type_id": "m5.xlarge"}, "ai_tool_x", prefix="ai_tool",
                                      cfg=kwargs["cfg"]).aidp_payload
        cluster_envs.append(payload["clusterRuntimeConfig"]["sparkEnvVariables"])

    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", phase3)
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    if cli_lake:
        argv += ["--lake-ocid", cli_lake]
    if cli_ws:
        argv += ["--workspace-id", cli_ws]

    assert exporter.main(argv) == exporter.ExitCode.OK

    want_lake, want_ws = cli_lake or YAML_LAKE, cli_ws or YAML_WS
    assert [(c.lake_ocid, c.workspace_id) for c in clients] == [(want_lake, want_ws)]
    assert len(cluster_envs) == 1
    assert cluster_envs[0]["AIDP_WORKSPACE_ID"] == want_ws
    assert "AIDP_LAKE_OCID" not in cluster_envs[0]
    assert [(c.aidp.lake_ocid, c.aidp.workspace_id) for c in preflight_cfgs] == [(want_lake, want_ws)]
    # The loaded YAML namespace itself is not rewritten.
    assert (yaml_aidp.lake_ocid, yaml_aidp.workspace_id) == (YAML_LAKE, YAML_WS)
