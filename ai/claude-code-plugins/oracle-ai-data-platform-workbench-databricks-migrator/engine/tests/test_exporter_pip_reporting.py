"""Notebook pip findings must survive both scans into the final JSON report."""
import copy
import json
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
from packaging.requirements import Requirement

from dbx_export import __main__ as exporter
from dbx_export.dependency_resolver import Graph, JobNode, NotebookNode
from dbx_export.report import JobResult, Report
from test_exporter_pip_source_context import COMMANDS


def _graph(cells, child=False):
    graph = Graph()
    graph.jobs["42"] = JobNode(dbx_job_id="42", depth=0, job_def={"settings": {
        "job_clusters": [{"job_cluster_key": "cpu", "new_cluster": {}}],
        "tasks": [{"task_key": "task", "job_cluster_key": "cpu",
                   "notebook_task": {"notebook_path": "/task"}}],
    }})
    path = "/child" if child else "/task"
    graph.notebooks[path] = NotebookNode(
        dbx_path=path, role="run_dep" if child else "task",
        referenced_by=["/task" if child else "job:42/task:task"], nb_dict={"cells": cells})
    if child:
        graph.notebooks["/task"] = NotebookNode(
            dbx_path="/task", role="task", referenced_by=["job:42/task:task"],
            nb_dict={"cells": [{"cell_type": "code", "source": "%run /child"}]})
    return graph, path


def _scan(graph, consumer, report):
    if consumer in ("discovery", "both"):
        exporter.discover_notebook_pip_deps(graph, "/export", "42", report=report)
    if consumer in ("consolidation", "both"):
        exporter.consolidate_cluster_requirements(graph, "42", report=report)


def _report():
    return Report(run_id="pip-report", dbx_root_job_id="42", export_output="/export", prefix="test")


@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("command", [
    "%pip install --require-hashes hash_guard_pkg==1.0 second_guard_pkg==2.0",
    "!pip install hash_guard_pkg==1.0 second_guard_pkg==2.0 --require-hashes",
    "!python -m pip install --require-hashes hash_guard_pkg==1.0 second_guard_pkg==2.0",
    "subprocess.run(['pip', 'install', '--require-hashes', 'hash_guard_pkg==1.0', 'second_guard_pkg==2.0'])",
    "os.system('pip install --require-hashes hash_guard_pkg==1.0 second_guard_pkg==2.0')",
    "pip.main(['install', '--require-hashes', 'hash_guard_pkg==1.0', 'second_guard_pkg==2.0'])",
    "libs = ['hash_guard_pkg==1.0', 'second_guard_pkg==2.0']\nfor lib in libs:\n"
    "    subprocess.check_call(['pip', 'install', '--require-hashes', lib])",
])
def test_require_hashes_reaches_final_report_without_unhashed_artifacts(
        tmp_path, consumer, child, command):
    graph, path = _graph([
        {"cell_type": "code", "source": "%pip install independent_pkg"},
        {"cell_type": "code", "source": command},
    ], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    assert graph.notebooks == original
    # Consolidation canonicalizes underscores to hyphens; inspect requirements
    # rather than confusing that existing normalization with lost dependencies.
    names = {Requirement(line).name.replace("_", "-")
             for node in graph.files.values() for line in node.content.decode().splitlines()
             if line.strip() and not line.startswith("#")}
    assert "independent-pkg" in names
    assert "hash-guard-pkg" not in names and "second-guard-pkg" not in names
    saved = json.loads(Path(report.write(str(tmp_path))).read_text())
    assert saved["summary"]["pip_unresolved"] == 1
    assert len(saved["pip_findings"]) == 1
    finding = saved["pip_findings"][0]
    assert finding["notebook_path"] == path and finding["cell_index"] == 1
    assert finding["status"] == "UNRESOLVED"
    assert "--require-hashes" in finding["reason"]
    assert "unhashed installation not attempted" in finding["reason"]


@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("command", COMMANDS)
def test_unresolved_arguments_reach_saved_report_without_changing_dependencies(
        tmp_path, consumer, child, command):
    graph, path = _graph([
        {"cell_type": "code", "source": "%pip install real_required_pkg"},
        {"cell_type": "code", "source": command.format(pkg="{package_from_config}")},
    ], child)
    original = copy.deepcopy(graph)
    report = _report()
    # Reporting must not depend on Python's warning visibility or registry.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
        _scan(original, consumer, None)
    assert graph.notebooks == original.notebooks
    assert graph.files == original.files, "reporting must not change dependency artifacts"
    saved = json.loads(Path(report.write(str(tmp_path))).read_text())
    assert len(saved["pip_findings"]) == 1
    finding = saved["pip_findings"][0]
    assert finding["notebook_path"] == path
    assert finding["cell_index"] == 1  # zero-based, same convention as existing export provenance
    assert finding["status"] == "UNRESOLVED"
    assert "{package_from_config}" in finding["reason"]
    assert saved["summary"]["pip_unresolved"] == 1
    assert saved["summary"]["warnings"] == 1
    assert saved["summary"]["errors"] == 0
    assert any(f"{path}:cell1" in error for error in saved["errors"])


@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("source", [
    'HELP = "unterminated\n%pip install unknown_pkg',
    'HELP = """\n%pip install unknown_pkg',
    'if True:\n    x = 1\n  y = 2\n%pip install unknown_pkg',
])
def test_incomplete_scan_is_reported_even_without_any_extracted_requirement(consumer, source):
    graph, _ = _graph([{"cell_type": "code", "source": source}])
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    assert report.summary()["pip_unresolved"] == 1
    finding = report.to_dict()["pip_findings"][0]
    assert "source scan incomplete at line" in finding["reason"]
    assert finding["notebook_path"] == "/task"
    assert all(b"unknown_pkg" not in node.content for node in graph.files.values())


def test_repeated_scans_deduplicate_only_within_same_notebook_and_cell():
    source = "%pip install -r requirements.txt"
    graph, _ = _graph([{"cell_type": "code", "source": source}] * 2, child=True)
    graph.notebooks["/task"].nb_dict["cells"].append({"cell_type": "code", "source": source})
    settings = graph.jobs["42"].job_def["settings"]
    settings["job_clusters"].append({"job_cluster_key": "cpu2", "new_cluster": {}})
    settings["tasks"].append({"task_key": "task2", "job_cluster_key": "cpu2",
                              "notebook_task": {"notebook_path": "/task"}})
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, "both", report)
        _scan(graph, "both", report)
    assert report.summary()["pip_unresolved"] == 3
    assert {(f["notebook_path"], f["cell_index"]) for f in report.pip_findings} == {
        ("/child", 0), ("/child", 1), ("/task", 1)}
    assert len(report.errors) == 3


@pytest.mark.parametrize("source", [
    "%md\n%pip install -r examples.txt", "%md-sandbox\n%pip install --group docs",
    'HELP = """\n%pip install -r examples.txt\n"""',
    "# %pip install -r examples.txt", "%pip install real_required_pkg",
])
def test_clean_or_documentation_cells_add_no_findings(source):
    graph, _ = _graph([{"cell_type": "code", "source": source}])
    report = _report()
    _scan(graph, "both", report)
    assert report.to_dict()["pip_findings"] == []
    assert report.summary()["pip_unresolved"] == 0


def test_notices_are_informational_not_unresolved():
    graph, _ = _graph([{"cell_type": "code", "source": "%pip install real_pkg > /tmp/pip.log"}])
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, "both", report)
    assert report.summary()["pip_unresolved"] == 0
    assert report.summary()["pip_notes"] == 1
    assert report.summary()["info"] == 1
    assert report.summary()["errors"] == report.summary()["warnings"] == 0


@pytest.mark.parametrize("mode", ["full", "dry-run", "notebook-only", "job-failure"])
@pytest.mark.parametrize("unresolved", [False, True])
def test_main_persists_findings_and_summary_without_changing_exit_policy(
        monkeypatch, tmp_path, capsys, mode, unresolved):
    import aidp_config
    import dbx_export.aidp_client as aidp_client
    from dbx_export.jar_discovery import JarDiscoveryResult

    source = "%pip install real_required_pkg" + (" {package_from_config}" if unresolved else "")
    graph, _ = _graph([{"cell_type": "code", "source": source}])
    config = SimpleNamespace(
        aidp=SimpleNamespace(lake_ocid="test-lake", workspace_id="test-workspace", oci_profile="DEFAULT"),
        paths=SimpleNamespace(export_output="/Workspace/export"),
        dbx_source=SimpleNamespace())
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
    monkeypatch.setattr(exporter.ph_mod, "new_run_id", lambda: "pip-report")
    monkeypatch.setattr(exporter, "phase1_discover", lambda *a, **kw: graph)
    for name in ("phase1_discover_existing_cluster_libraries", "phase1_fetch_files",
                 "phase1_discover_init_script_artifacts", "resolve_failed_jar_fallbacks"):
        monkeypatch.setattr(exporter, name, lambda *a, **kw: None)
    monkeypatch.setattr(exporter, "discover_notebook_jars",
                        lambda *a: JarDiscoveryResult(fallback_path="/Workspace/libs"))
    monkeypatch.setattr(exporter, "pre_flight", lambda *a, **kw: "mock pre-flight")
    monkeypatch.setattr(exporter, "phase2_upload_notebooks", lambda **kw: ({}, []))
    monkeypatch.setattr(exporter, "phase2_upload_files", lambda **kw: ({}, False))
    phase3_calls = []

    def phase3(**kwargs):
        phase3_calls.append(True)
        if mode == "job-failure":
            kwargs["report"].jobs.append(JobResult(
                dbx_job_id="42", dbx_job_name="test", aidp_workflow_name="test", status="FAILED"))

    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", phase3)
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080",
            "--report-dir", str(tmp_path), "--strict", "--strict-deps"]
    if mode in ("dry-run", "notebook-only"):
        argv.append("--" + mode)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        result = exporter.main(argv)
    assert result == (exporter.ExitCode.JOB_CREATE_FAILED if mode == "job-failure"
                      else exporter.ExitCode.OK)
    assert len(phase3_calls) == (1 if mode in ("full", "job-failure") else 0)
    written = json.loads((tmp_path / "export_dbx_job_42_pip-report.json").read_text())
    assert written["summary"]["pip_unresolved"] == int(unresolved)
    assert len(written["pip_findings"]) == int(unresolved)
    output = capsys.readouterr().out
    if unresolved:
        assert "[pip-discovery] unresolved findings: 1" in output
        assert written["pip_findings"][0]["notebook_path"] == "/task"
    if mode != "dry-run":
        assert f"pip_unresolved: {int(unresolved)}" in output
