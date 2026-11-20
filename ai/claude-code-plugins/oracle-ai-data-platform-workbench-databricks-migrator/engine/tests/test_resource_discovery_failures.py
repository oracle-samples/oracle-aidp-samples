"""Resource enumeration failures survive discovery and gate real export main."""
import json
from types import SimpleNamespace

import pytest

from dbx_export import __main__ as exporter
from dbx_export.dbx_client import DbxExportError
from dbx_export.dependency_resolver import DependencyResolver, Graph, NotebookNode
from dbx_export.jar_discovery import JarDiscoveryResult


def test_scan_limit_is_incomplete_but_empty_success_is_not(monkeypatch):
    import dbx_export.dependency_resolver as resolver_module

    class Client:
        def list_workspace_folder(self, path):
            if path == "models":
                return [{"path": "models/good.yaml", "object_type": "FILE"},
                        {"path": "models/nested", "object_type": "DIRECTORY"}]
            return []

    monkeypatch.setattr(resolver_module, "_RESOURCE_SCAN_LIMIT", 1)
    node = NotebookNode(dbx_path="init", role="task")
    resolver = DependencyResolver(Client(), log=lambda *a: None)
    assert resolver._list_resource_tree(node, "models") == ["models/good.yaml"]
    assert node.resource_discovery_failures == [{
        "notebook": "init", "directory": "models", "reason": "RESOURCE_SCAN_LIMIT_REACHED"}]
    empty = NotebookNode(dbx_path="empty", role="task")
    assert resolver._list_resource_tree(empty, "empty") == []
    assert not empty.resource_discovery_failures


@pytest.mark.parametrize("git", [True, False])
@pytest.mark.parametrize("stage", ["ancestor", "nested"])
def test_listing_failure_is_retained_without_losing_other_resources(git, stage):
    class Client:
        def list_github_dir(self, *args):
            if stage == "ancestor":
                raise DbxExportError("PERMISSION_DENIED", "test")
            return {"models": "models"}

        def list_github_dir_entries(self, owner, repo, path, branch):
            if path.endswith("models"):
                return [{"name": "good.yaml", "type": "file"},
                        {"name": "blocked", "type": "dir"}]
            raise DbxExportError("PERMISSION_DENIED", "test")

        def list_workspace_folder(self, path):
            if stage == "ancestor" or path.endswith("blocked"):
                raise RuntimeError("test list failure")
            if path.endswith("models"):
                return [{"path": path + "/good.yaml", "object_type": "FILE"},
                        {"path": path + "/blocked", "object_type": "DIRECTORY"}]
            return [{"path": path + "/models", "object_type": "DIRECTORY"}]

    node = NotebookNode(dbx_path="notebooks/init", role="task",
                        nb_dict={"cells": [{"cell_type": "code", "source": "from models import config"}]},
                        git_owner="owner" if git else None, git_repo="repo" if git else None)
    graph = Graph(notebooks={node.dbx_path: node})
    resolver = DependencyResolver(Client(), log=lambda *a: None)
    first = resolver._discover_local_resources(graph, node, node.dbx_path)
    failures = list(node.resource_discovery_failures)
    resolver._discover_local_resources(graph, node, node.dbx_path)
    assert failures and node.resource_discovery_failures == failures
    assert all(f["notebook"] == node.dbx_path for f in failures)
    assert all(f["reason"] == ("GIT_DIRECTORY_LIST_FAILED" if git else
                              "WORKSPACE_DIRECTORY_LIST_FAILED") for f in failures)
    assert bool(first) == (stage == "nested")
    if first:
        assert first == ["notebooks/models/good.yaml"]


@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("failed", [True, False])
@pytest.mark.parametrize("failure_source", ["listing", "compound", "widget", "ternary", "mapping", "format_spec", "predicate_truth", "subscript_store", "ignored_kwargs", "opaque_call", "list_insert", "alias_ior", "walrus"])
def test_main_reports_incomplete_discovery_and_strictly_stops_before_upload(
        monkeypatch, tmp_path, git, strict, failed, failure_source):
    import aidp_config
    import dbx_export.aidp_client as aidp_client

    config = SimpleNamespace(
        aidp=SimpleNamespace(lake_ocid="test-lake", workspace_id="test-workspace", oci_profile="DEFAULT"),
        paths=SimpleNamespace(export_output="/Workspace/export"), dbx_source=SimpleNamespace())
    node = NotebookNode(dbx_path="notebooks/init", role="task", nb_dict={"cells": []},
                        git_owner="owner" if git else None, git_repo="repo" if git else None)
    if failed or failure_source in {"format_spec", "predicate_truth", "subscript_store", "ignored_kwargs", "opaque_call", "list_insert", "alias_ior", "walrus"}:
        class Client:
            def list_workspace_folder(self, path):
                raise RuntimeError("listing unavailable")

            def list_github_dir_entries(self, *args):
                raise DbxExportError("PERMISSION_DENIED", "listing unavailable")
        resolver = DependencyResolver(Client(), log=lambda *a: None)
        if failure_source == "listing":
            resolver._list_resource_tree(node, "models")
        else:
            source = ('try:\n    cfg = load_config("../config")\nexcept Exception:\n    raise'
                      if failure_source == "compound" else
                      'dbutils.widgets.text("root", "/Users/fixture/repo")\n'
                      'if unknown:\n    dbutils.widgets.removeAll()\n'
                      'load_config(dbutils.widgets.get("root"))')
            if failure_source in {"ternary", "mapping", "format_spec", "predicate_truth", "subscript_store", "ignored_kwargs", "opaque_call", "list_insert", "alias_ior", "walrus"}:
                source = ('ROOT = "/Users/fixture/repo/config" if flag else runtime_root()\n'
                          'load_config(ROOT)')
                if failure_source == "mapping":
                    source = ('ROOTS = {"primary": "/Users/fixture/repo/config", "other": runtime_root()}\n'
                              'load_config(ROOTS[runtime_choice()])')
                if failure_source == "format_spec":
                    spec = '"" if flag else runtime_format()' if failed else '""'
                    source = (f'FORMAT = {spec}\nROOT = "/Users/fixture/repo/config"\n'
                              'load_config(f"{ROOT:{FORMAT}}")')
                elif failure_source == "predicate_truth":
                    source = ('META = {"enabled": True, "runtime": runtime_metadata()}\n'
                              + ('META = META if flag else runtime_mapping()\n' if failed else '')
                              + 'if META:\n    ROOT = "/Users/fixture/repo/config"\n'
                              'else:\n    ROOT = "/Users/fixture/repo/other/config"\nload_config(ROOT)')
                if failure_source == "subscript_store":
                    source = ('ROOTS = {"primary": "/Users/fixture/repo/config"}\n'
                              + ('ROOTS["primary"] = "/Users/fixture/repo/other/config"\n' if failed else '')
                              + 'load_config(ROOTS["primary"])')
                elif failure_source == "ignored_kwargs":
                    source = ('load_config(os.path.join("/Users/fixture/repo/config"'
                              + (', ignored="value"' if failed else '') + '))')
                elif failure_source in {"opaque_call", "list_insert", "alias_ior", "walrus"}:
                    setups = {
                        "opaque_call": ('ROOTS = {"primary": "/Users/fixture/repo/config"}\n',
                                        'ROOTS.update(primary="/Users/fixture/repo/other/config")\n',
                                        'ROOTS["primary"]'),
                        "list_insert": ('ROOTS = ["/Users/fixture/repo/config"]\n',
                                        'ROOTS.insert(0, "/Users/fixture/repo/other/config")\n', 'ROOTS[0]'),
                        "alias_ior": ('ROOTS = {"primary": "/Users/fixture/repo/config"}\nALIAS = ROOTS\n',
                                      'ROOTS |= {"primary": "/Users/fixture/repo/other/config"}\n',
                                      'ALIAS["primary"]'),
                        "walrus": ('ROOT = "/Users/fixture/repo/config"\n',
                                   '(ROOT := "/Users/fixture/repo/other/config")\n', 'ROOT'),
                    }
                    setup, mutation, read = setups[failure_source]
                    source = setup + (mutation if failed else "") + f"load_config({read})"
                # The known directory is readable, but that cannot certify the
                # unknown alternate branch. No unrelated listing error masks it.
                resolver.dbx.list_workspace_folder = lambda path: ([{
                    "path": path + "/settings.yaml", "object_type": "FILE"}]
                    if path == "/Users/fixture/repo/config" else [])
            if git:
                source = source.replace("/Users/fixture/repo", "..")
                resolver.dbx.list_github_dir_entries = lambda owner, repo, path, branch: ([{
                    "name": "settings.yaml", "type": "file"}] if path == "config" else [])
            node.nb_dict = {"cells": [{"cell_type": "code", "source": source}]}
            resolver._discover_local_resources(Graph(notebooks={node.dbx_path: node}), node, node.dbx_path)
    graph = Graph(notebooks={node.dbx_path: node})
    for variable in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DBX_PROXY",
                     "DBX_PROXY_BASIC_USER", "DBX_PROXY_BASIC_PASS"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(aidp_config, "load_config", lambda **kw: config)
    monkeypatch.setattr(exporter, "configured_lib_repository_dir", lambda *a: "/Workspace/libs")
    monkeypatch.setattr(aidp_client, "set_artifact_alias_repository", lambda *a: None)
    monkeypatch.setattr(exporter, "get_oci_signer", lambda **kw: ({}, object()))
    monkeypatch.setattr(exporter, "DbxClient", lambda *a, **kw: object())
    monkeypatch.setattr(exporter, "AidpClient", lambda *a, **kw: object())
    monkeypatch.setattr(exporter, "_limits_startup_gate", lambda *a: None)
    monkeypatch.setattr(exporter.ph_mod, "new_run_id", lambda: "resource-failure")
    monkeypatch.setattr(exporter, "phase1_discover", lambda *a, **kw: graph)
    for name in ("phase1_discover_existing_cluster_libraries", "phase1_fetch_files",
                 "phase1_discover_init_script_artifacts", "resolve_failed_jar_fallbacks",
                 "discover_notebook_pip_deps", "consolidate_cluster_requirements"):
        monkeypatch.setattr(exporter, name, lambda *a, **kw: None)
    monkeypatch.setattr(exporter, "discover_notebook_jars",
                        lambda *a: JarDiscoveryResult(fallback_path="/Workspace/libs"))
    monkeypatch.setattr(exporter, "pre_flight", lambda *a, **kw: "test pre-flight")
    writes = []
    monkeypatch.setattr(exporter, "phase2_upload_notebooks",
                        lambda **kw: (writes.append("notebooks") or {}, []))
    monkeypatch.setattr(exporter, "phase2_upload_files",
                        lambda **kw: (writes.append("files") or {}, False))
    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows",
                        lambda **kw: writes.append("workflows"))
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    if strict:
        argv.append("--strict-deps")
    result = exporter.main(argv)
    assert result == (exporter.ExitCode.STRICT_DEPS_FAILED if strict and failed else exporter.ExitCode.OK)
    assert writes == ([] if strict and failed else ["notebooks", "files", "workflows"])
    report = json.loads((tmp_path / "export_dbx_job_42_resource-failure.json").read_text())
    assert report["resource_discovery_complete"] == (not failed)
    assert report["summary"]["resource_discovery_failures"] == int(failed)
    assert report["resource_discovery_failures"] == node.resource_discovery_failures
    if failed:
        assert any("resource-discovery" in e and e.startswith("ERROR" if strict else "WARN")
                   for e in report["errors"])
