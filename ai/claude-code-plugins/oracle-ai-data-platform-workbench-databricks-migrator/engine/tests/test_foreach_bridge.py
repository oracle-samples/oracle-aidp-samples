"""FOREACH_BRIDGE increment 1 — discovery + loud refusal instead of silent drop.

Incident: live dry-run 2026-10-13 of Example_Base_Job (DBX 1000000000000001,
report export_dbx_job_1000000000000001_2026-10-13T06:42:28Z-cccccc87.json): the
4-task job exported "Task notebooks: 3 OK" with ZERO findings — the
``for_each_task`` was silently dropped, its nested notebook
(``notebooks/generate_base``) was never discovered, and the downstream task
kept a dangling ``dependsOn``. These tests pin the fix: the nested notebook is
discovered like any task notebook, and conversion refuses loudly for a
for-each (until the driver bridge lands) and for any other silently-droppable
task type. run_job_task keeps its documented drop, now with the promised note.

TEMPORARY: when AIDP ships native for-each, the bridge is deleted and the
refusal here becomes the native conversion — adapt, don't delete, these pins
(grep: FOREACH_BRIDGE).
"""
import json

import pytest

from dbx_export.dbx_client import ExportedFile
from dbx_export.dependency_resolver import DependencyResolver
from dbx_export.job_converter import convert_dbx_job, convert_dbx_task


def _notebook(source: str):
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [{
            "cell_type": "code",
            "source": source.splitlines(keepends=True),
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        }],
    }


def _foreach_task(nested_notebook="notebooks/generate_base", source="GIT"):
    # Shape captured live from jobs/get 2.2 for Example_Base_Job (2026-10-13).
    return {
        "task_key": "run_base_notebooks",
        "depends_on": [{"task_key": "init"}],
        "for_each_task": {
            "inputs": "{{tasks.init.values.models_to_trigger}}",
            "concurrency": 2,
            "task": {
                "task_key": "run_base_notebooks_iteration",
                "notebook_task": {
                    "notebook_path": nested_notebook,
                    "source": source,
                    "base_parameters": {
                        "model_version": "{{input.model_name}}",
                        "target_notebook": "{{input.file_path}}",
                        "target_yaml": "{{input.yaml_path}}",
                    },
                },
                "job_cluster_key": "Example_pipeline_BASE_cluster",
                "libraries": [{"maven": {
                    "coordinates": "org.apache.hudi:hudi-spark-bundle_2.12:0.5.3"}}],
            },
        },
    }


def _bridge_settings():
    return {
        "name": "Example_Base_Job",
        "format": "MULTI_TASK",
        "git_source": {
            "git_url": "https://github.com/owner/repo.git",
            "git_branch": "tornado_v1",
        },
        "job_clusters": [{"job_cluster_key": "Example_pipeline_BASE_cluster"}],
        "tasks": [
            {
                "task_key": "init",
                "notebook_task": {"notebook_path": "notebooks/init", "source": "GIT"},
            },
            _foreach_task(),
            {
                "task_key": "Consolidated_Feature_Preparation",
                "depends_on": [{"task_key": "run_base_notebooks"}],
                "notebook_task": {"notebook_path": "notebooks/consolidated", "source": "GIT"},
            },
        ],
    }


def test_resolver_discovers_the_foreach_nested_notebook():
    exported = []

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return "owner", "repo"

        def get_job(self, _job_id):
            return {"settings": _bridge_settings()}

        def export_notebook_from_git(self, path, *_context):
            exported.append(path)
            return ExportedFile(json.dumps(_notebook("x = 1\n")).encode(), True, ".ipynb")

        def list_github_dir(self, *_a):
            return {}

        def list_github_dir_entries(self, *_a):
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _m: None).resolve("1000000000000001")

    node = graph.notebooks.get("notebooks/generate_base")
    assert node is not None, "for_each nested notebook must be discovered"
    assert node.role == "task"
    assert node.export_error is None
    assert "for_each" in " ".join(node.referenced_by)
    # the nested task's templated parameters ride along untouched
    assert node.parameter_bindings["target_notebook"] == "{{input.file_path}}"
    # ...and the flat task notebooks are unaffected
    assert "notebooks/init" in graph.notebooks
    assert "notebooks/consolidated" in graph.notebooks


def _convert(settings):
    return convert_dbx_job(
        job_data={"job_id": "1000000000000001", "settings": settings},
        export_output="/Workspace/scratch/export",
        prefix="test",
        dbx_cluster_key_to_aidp_key={"Example_pipeline_BASE_cluster": "aidp-key"},
    )


def test_foreach_task_converts_to_a_driver_notebook_task():
    decisions, generated = [], []
    payload = convert_dbx_job(
        job_data={"job_id": "1000000000000001", "settings": _bridge_settings()},
        export_output="/Workspace/scratch/export",
        prefix="test",
        dbx_cluster_key_to_aidp_key={"Example_pipeline_BASE_cluster": "aidp-key"},
        conversion_decisions=decisions,
        generated_notebooks=generated,
    )
    tasks = {t["taskKey"]: t for t in payload["tasks"]}
    assert set(tasks) == {"init", "run_base_notebooks", "Consolidated_Feature_Preparation"}
    driver_task = tasks["run_base_notebooks"]
    assert driver_task["type"] == "NOTEBOOK_TASK"
    # job-scoped name: two jobs sharing task key + inner dir cannot collide
    assert driver_task["notebookPath"].endswith(
        "aidp_foreach_driver__1000000000000001__run_base_notebooks.ipynb")
    # cross-task inputs reader declaration (live-verified gap 2026-10-13:
    # sibling consumers carried it, the driver had parameters=[])
    assert driver_task["parameters"] == [
        {"name": "models_to_trigger",
         "value": "{{tasks.init.values.models_to_trigger}}"}]
    # same key → downstream dependsOn edges and clone_workflow untouched
    assert [d["taskKey"] for d in driver_task["dependsOn"]] == ["init"]
    assert [d["taskKey"] for d in
            tasks["Consolidated_Feature_Preparation"]["dependsOn"]] == [
        "run_base_notebooks"]
    # iterations ran on the NESTED task's cluster on Databricks
    assert driver_task["cluster"] == {"clusterKey": "aidp-key"}
    # the driver artifact is handed to Phase 3 for upload
    assert len(generated) == 1
    path, nb = generated[0]
    assert path == driver_task["notebookPath"]
    src = "".join(nb["cells"][0]["source"])
    assert "AIDP TOOL GENERATED" in src and "TEMPORARY" in src
    assert "oidlUtils.parameters.getParameter('models_to_trigger'" in src.replace('"', "'")
    assert "aidputils.notebook.run" in src
    assert "/Workspace/scratch/export/notebooks/GIT/notebooks/generate_base.ipynb" in src
    # the conversion is surfaced, never silent
    assert any("for_each_task converted" in d and "run_base_notebooks" in d
               for d in decisions)


def _run_driver(source, elements_feed, run_results):
    """Execute the generated driver source with fake AIDP builtins.

    run_results: dict mapping iteration index (1-based) -> Exception to raise
    or a value to return; missing indexes return "ok-<idx>".
    Returns (calls, printed, error).
    """
    calls, printed = [], []

    class _NotebookApi:
        @staticmethod
        def run(path, timeout, args):
            idx = len(calls) + 1
            calls.append((path, timeout, dict(args)))
            result = run_results.get(idx)
            if isinstance(result, Exception):
                raise result
            return result if result is not None else f"ok-{idx}"

    class _ParamsApi:
        @staticmethod
        def getParameter(_key, default):
            return elements_feed if elements_feed is not None else default

    namespace = {
        "aidputils": type("A", (), {"notebook": _NotebookApi}),
        "oidlUtils": type("O", (), {"parameters": _ParamsApi}),
        "print": lambda *a, **k: printed.append(" ".join(str(x) for x in a)),
    }
    error = None
    try:
        exec(source, namespace)
    except Exception as exc:  # noqa: BLE001 — the driver's own loud failure
        error = exc
    return calls, printed, error


def test_generated_driver_executes_the_loop_sequentially():
    from dbx_export.foreach_bridge import build_driver_notebook
    elements = [
        {"model_name": "A", "file_path": "models/A/nb.ipynb", "yaml_path": "models/A/params.yaml"},
        {"model_name": "B", "file_path": "models/B/nb.ipynb", "yaml_path": "models/B/params.yaml"},
    ]
    nb = build_driver_notebook(
        _foreach_task(), "/Workspace/x/generate_base.ipynb",
        job_id="1000000000000001", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    calls, printed, error = _run_driver(src, json.dumps(elements), {})
    assert error is None
    assert [c[0] for c in calls] == ["/Workspace/x/generate_base.ipynb"] * 2
    assert calls[0][1] == 36000
    assert calls[0][2]["model_version"] == "A"
    assert calls[0][2]["target_notebook"] == "models/A/nb.ipynb"
    assert calls[1][2]["target_yaml"] == "models/B/params.yaml"
    assert any("all 2 iteration(s) succeeded" in line for line in printed)


def test_generated_driver_runs_every_element_and_fails_loud_at_the_end():
    from dbx_export.foreach_bridge import build_driver_notebook
    elements = [{"model_name": n, "file_path": f"{n}.ipynb", "yaml_path": f"{n}.yaml"}
                for n in ("A", "B", "C")]
    nb = build_driver_notebook(
        _foreach_task(), "/Workspace/x/generate_base.ipynb",
        job_id="1", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    calls, printed, error = _run_driver(
        src, json.dumps(elements), {2: RuntimeError("boom token=SECRET123")})
    assert len(calls) == 3, "iterations are independent — B's failure must not stop C"
    assert isinstance(error, RuntimeError)
    assert "1/3 iteration(s) failed" in str(error)
    # exception MESSAGES can carry secrets: type only, never the text
    assert "SECRET123" not in str(error) and "boom" not in str(error)
    assert "RuntimeError" in str(error)
    assert "SECRET123" not in "\n".join(printed)


def test_generated_driver_fails_loud_when_the_task_value_is_missing():
    from dbx_export.foreach_bridge import build_driver_notebook
    nb = build_driver_notebook(
        _foreach_task(), "/Workspace/x/generate_base.ipynb",
        job_id="1", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    calls, _printed, error = _run_driver(src, None, {})
    assert not calls
    assert isinstance(error, RuntimeError)
    assert "models_to_trigger" in str(error) and "never" in str(error)


def test_generated_driver_embeds_static_literal_inputs():
    from dbx_export.foreach_bridge import build_driver_notebook
    task = _foreach_task()
    task["for_each_task"]["inputs"] = json.dumps(["x", "y"])
    task["for_each_task"]["task"]["notebook_task"]["base_parameters"] = {
        "value": "{{input}}"}
    nb = build_driver_notebook(task, "/Workspace/x/inner.ipynb",
                               job_id="1", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    assert "getParameter" not in src
    calls, _printed, error = _run_driver(src, None, {})
    assert error is None
    assert [c[2]["value"] for c in calls] == ["x", "y"]


@pytest.mark.parametrize("template, element, expected", [
    ("{{input}}", "plain", "plain"),
    ("{{input.a}}", {"a": "v"}, "v"),
    ("{{input.a.b}}", {"a": {"b": "deep"}}, "deep"),
    ("pre-{{input.a}}-post", {"a": "mid"}, "pre-mid-post"),
    ("constant", {"a": "x"}, "constant"),
    # live source-job typo: trailing literal braces are reproduced faithfully
    ("{{input.model_name}}}}", {"model_name": "M"}, "M}}"),
    # surrounding whitespace is literal text — preserved, never stripped
    (" {{input.a}} ", {"a": "v"}, " v "),
    # multiline literals must not break the generated expression
    ("line1\n{{input.a}}\nline2", {"a": "mid"}, "line1\nmid\nline2"),
])
def test_param_expression_matches_dbx_substitution(template, element, expected):
    from dbx_export.foreach_bridge import _param_expression
    expr = _param_expression(template)
    namespace = {
        "_el": element,
        "_str": lambda v: v if isinstance(v, str) else json.dumps(v),
        "_lookup": lambda el, keys: __import__("functools").reduce(
            lambda c, k: c[k], keys, el),
    }
    assert eval(expr, namespace) == expected  # noqa: S307 — generated code


def test_param_expression_refuses_non_input_templating():
    from dbx_export.foreach_bridge import _param_expression
    with pytest.raises(ValueError, match="non-input templating"):
        _param_expression("{{tasks.other.values.x}}")


def test_foreach_with_unsupported_inputs_shape_refuses_loudly():
    settings = _bridge_settings()
    settings["tasks"][1]["for_each_task"]["inputs"] = '{"not": "a list"}'
    with pytest.raises(ValueError, match="inputs shape is not supported"):
        _convert(settings)


def test_foreach_with_non_notebook_nested_task_names_the_gap():
    settings = _bridge_settings()
    settings["tasks"][1]["for_each_task"]["task"] = {
        "task_key": "iteration",
        "python_wheel_task": {"package_name": "pkg", "entry_point": "main"},
    }
    with pytest.raises(ValueError, match="non-notebook task"):
        _convert(settings)


def test_unknown_task_type_refuses_loudly():
    settings = _bridge_settings()
    settings["tasks"][1] = {
        "task_key": "wheel_step",
        "python_wheel_task": {"package_name": "pkg", "entry_point": "main"},
    }
    with pytest.raises(ValueError) as exc:
        _convert(settings)
    assert "wheel_step" in str(exc.value)
    assert "python_wheel_task" in str(exc.value)


def test_run_job_task_keeps_documented_drop_with_a_note():
    settings = _bridge_settings()
    settings["tasks"][1] = {
        "task_key": "trigger_child",
        "run_job_task": {"job_id": 42},
    }
    decisions = []
    payload = convert_dbx_job(
        job_data={"job_id": "1", "settings": settings},
        export_output="/Workspace/scratch/export",
        prefix="test",
        dbx_cluster_key_to_aidp_key={"Example_pipeline_BASE_cluster": "aidp-key"},
        conversion_decisions=decisions,
    )
    assert [t["taskKey"] for t in payload["tasks"]] == [
        "init", "Consolidated_Feature_Preparation",
    ]
    note = [d for d in decisions if "run_job_task" in d]
    assert len(note) == 1
    assert "trigger_child" in note[0] and "42" in note[0]


def test_convert_dbx_task_still_converts_plain_notebook_tasks():
    converted = convert_dbx_task(
        {"task_key": "init",
         "notebook_task": {"notebook_path": "/Users/a@b.com/nb"}},
        export_output="/Workspace/scratch/export",
        dbx_cluster_key_to_aidp_key={},
    )
    assert converted["type"] == "NOTEBOOK_TASK"
    assert converted["taskKey"] == "init"


def test_notebook_discovered_child_job_foreach_nested_notebook_is_discovered():
    """P2: _enqueue_child_job shares the task-notebook enumeration.

    A root WORKSPACE notebook launches child job 42 via runNow; the child is
    the EXBRIDGE-shaped for_each job. Before the shared enumeration the child's
    nested notebook was invisible on this path.
    """
    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(url):
            return ("owner", "repo") if url else (None, None)

        def get_job(self, job_id):
            if str(job_id) == "1":
                return {"settings": {"name": "root", "tasks": [{
                    "task_key": "t1",
                    "notebook_task": {"notebook_path": "/Users/x/driver"},
                }]}}
            assert str(job_id) == "42"
            return {"settings": _bridge_settings()}

        def export_notebook(self, path):
            assert path == "/Users/x/driver"
            return ExportedFile(
                json.dumps(_notebook("dbutils.jobs.runNow(job_id=42)\n")).encode(),
                True, ".ipynb")

        def export_notebook_from_git(self, _path, *_context):
            return ExportedFile(json.dumps(_notebook("x = 1\n")).encode(), True, ".ipynb")

        def list_github_dir(self, *_a):
            return {}

        def list_github_dir_entries(self, *_a):
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _m: None).resolve("1")
    assert "42" in graph.jobs and graph.jobs["42"].job_def is not None
    node = graph.notebooks.get("notebooks/generate_base")
    assert node is not None, "child job's for_each nested notebook must be discovered"
    assert node.role == "task"
    assert any("job:42/task:run_base_notebooks/for_each" in r for r in node.referenced_by)


def _unbridgeable_settings():
    """A for-each the bridge cannot convert (non-notebook nested task)."""
    settings = _bridge_settings()
    settings["tasks"][1]["for_each_task"]["task"] = {
        "task_key": "iteration",
        "python_wheel_task": {"package_name": "pkg", "entry_point": "main"},
    }
    return settings


def test_task_support_preflight_predicts_the_refusal():
    """P1 (unit): the preflight names every Phase-3 refusal, per job —
    and stays silent for a for-each the driver bridge converts."""
    from dbx_export.__main__ import _task_support_preflight_checks
    from dbx_export.dependency_resolver import Graph, JobNode

    supported = Graph()
    supported.jobs["1"] = JobNode(
        dbx_job_id="1", depth=0, job_def={"settings": _bridge_settings()})
    assert _task_support_preflight_checks(supported) == []

    graph = Graph()
    graph.jobs["1"] = JobNode(
        dbx_job_id="1", depth=0, job_def={"settings": _unbridgeable_settings()})
    graph.jobs["2"] = JobNode(dbx_job_id="2", depth=1, job_def=None)  # unfetched
    checks = _task_support_preflight_checks(graph)
    assert len(checks) == 1
    check = checks[0]
    assert check["status"] == "WILL REFUSE"
    assert check["name"] == "Example_Base_Job"
    assert "run_base_notebooks" in check["detail"]
    assert "non-notebook task" in check["detail"]


def test_main_dry_run_surfaces_the_foreach_refusal(tmp_path, monkeypatch):
    """P1 (integration): --dry-run must predict the Phase-3 refusal.

    Real main()/resolver/preflight/report; only network clients and config
    loading are faked (the repo's standard harness shape). Before the task-
    support preflight, this exact invocation exited OK with zero findings —
    the misleading-dry-run mechanism of the 2026-10-13 EXBRIDGE incident.
    """
    import warnings
    from types import SimpleNamespace
    from dbx_export.__main__ import ExitCode, main

    class FakeDbx:
        settings_factory = staticmethod(_unbridgeable_settings)

        @staticmethod
        def parse_github_owner_repo(url):
            return ("owner", "repo") if url else (None, None)

        def get_job(self, _job_id):
            return {"job_id": 1000000000000001, "settings": FakeDbx.settings_factory()}

        def export_notebook_from_git(self, _path, *_context):
            return ExportedFile(json.dumps(_notebook("x = 1\n")).encode(), True, ".ipynb")

        def list_github_dir(self, *_a):
            return {}

        def list_github_dir_entries(self, *_a):
            return []

        def get_cluster_libraries(self, _cid):
            return {}

    class JarDiscovery:
        index_fallbacks = []
        resolutions, index_errors, blockers, allow_unresolved = [], [], [], True

        @staticmethod
        def to_dict():
            return {"summary": {"missing": 0, "ambiguous": 0, "index_errors": 0}}

        @staticmethod
        def write(*_a):
            return str(tmp_path / "jar.json")

    config = SimpleNamespace(
        aidp=SimpleNamespace(lake_ocid="lake", workspace_id="ws", oci_profile="P"),
        paths=SimpleNamespace(export_output="/Workspace/export",
                              target_runtime_requirements_file=None,
                              compatibility_policy_file=None),
        dbx_source=SimpleNamespace(host=None, proxy=None),
        secrets=SimpleNamespace(dbx_proxy_basic_user=None, dbx_proxy_basic_pass=None),
    )
    mp = monkeypatch.setattr
    mp("aidp_config.load_config", lambda **k: config)
    mp("dbx_export.__main__.configured_lib_repository_dir", lambda _p: "/libs")
    mp("dbx_export.__main__.get_oci_signer", lambda **k: ({}, object()))
    mp("dbx_export.__main__.AidpClient", lambda **k: SimpleNamespace())
    mp("dbx_export.__main__.DbxClient", lambda *a, **k: FakeDbx())
    mp("dbx_export.__main__._limits_startup_gate", lambda _a: None)
    mp("dbx_export.__main__.ph_mod.new_run_id", lambda: "run")
    for name in ("phase1_fetch_files", "phase1_discover_init_script_artifacts",
                 "phase1_discover_existing_cluster_libraries",
                 "discover_notebook_pip_deps", "consolidate_cluster_requirements",
                 "resolve_failed_jar_fallbacks", "reconcile_failed_declared_jars"):
        mp(f"dbx_export.__main__.{name}", lambda *a, **k: None)
    mp("dbx_export.__main__.discover_notebook_jars", lambda *a, **k: JarDiscovery())

    # main() propagates the (stubbed) lib repository dir into aidp_client
    # module state (set_artifact_alias_repository). Restore it afterwards or
    # later library-identity tests inherit the fixture dir (full-suite-order
    # incident, 2026-10-14).
    from dbx_export import aidp_client
    _saved_alias_dirs = aidp_client._artifact_alias_dirs_configured
    monkeypatch_restore = lambda: setattr(
        aidp_client, "_artifact_alias_dirs_configured", _saved_alias_dirs)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rc = main(["--job-id", "1000000000000001", "--dbx-proxy", "proxy.invalid:8080",
                       "--cluster-key", "cl-1", "--dry-run",
                       "--report-dir", str(tmp_path)])

        assert rc == ExitCode.JOB_CREATE_FAILED
        report = json.loads((tmp_path / "export_dbx_job_1000000000000001_run.json").read_text())
        refusals = [e for e in report["errors"]
                    if "workflow-conversion-preflight" in e and "for_each_task" in e]
        assert len(refusals) == 1 and "run_base_notebooks" in refusals[0]
        assert refusals[0].startswith("ERROR")
        # NOTE: the report's `notebooks` list is populated at upload time, so a
        # dry-run leaves it empty by design — nested-notebook discovery is pinned
        # by test_resolver_discovers_the_foreach_nested_notebook instead.

        # A bridge-convertible for-each dry-runs clean: no refusal, exit OK.
        FakeDbx.settings_factory = staticmethod(_bridge_settings)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rc_ok = main(["--job-id", "1000000000000001", "--dbx-proxy", "proxy.invalid:8080",
                          "--cluster-key", "cl-1", "--dry-run",
                          "--report-dir", str(tmp_path / "ok")])
        assert rc_ok == ExitCode.OK
    finally:
        monkeypatch_restore()


def test_foreach_nested_unprovisioned_compute_refuses_loudly():
    for field, value in (("existing_cluster_id", "0123-456789-abc"),
                         ("new_cluster", {"num_workers": 2})):
        settings = _bridge_settings()
        nested = settings["tasks"][1]["for_each_task"]["task"]
        nested.pop("job_cluster_key", None)
        nested[field] = value
        with pytest.raises(ValueError, match="does not provision nested compute"):
            _convert(settings)


def test_foreach_nested_remote_library_passes_with_parity():
    # The live EXBRIDGE shape: nested maven (hudi) — same record-only treatment
    # as a top-level task; hudi additionally covered by the tool baseline.
    decisions, generated = [], []
    convert_dbx_job(
        job_data={"job_id": "1000000000000001", "settings": _bridge_settings()},
        export_output="/Workspace/scratch/export",
        prefix="test",
        dbx_cluster_key_to_aidp_key={"Example_pipeline_BASE_cluster": "aidp-key"},
        conversion_decisions=decisions,
        generated_notebooks=generated,
    )
    assert len(generated) == 1


def test_generated_driver_isolates_a_bad_element_and_runs_the_rest():
    """Arg construction is inside the per-iteration boundary: an element
    missing a referenced field fails THAT iteration only."""
    from dbx_export.foreach_bridge import build_driver_notebook
    elements = [
        {"model_name": "A", "file_path": "A.ipynb", "yaml_path": "A.yaml"},
        {"model_name": "B"},  # missing file_path/yaml_path
        {"model_name": "C", "file_path": "C.ipynb", "yaml_path": "C.yaml"},
    ]
    nb = build_driver_notebook(_foreach_task(), "/Workspace/x/inner.ipynb",
                               job_id="1", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    calls, _printed, error = _run_driver(src, json.dumps(elements), {})
    assert [c[2]["model_version"] for c in calls] == ["A", "C"]
    assert isinstance(error, RuntimeError)
    assert "1/3 iteration(s) failed" in str(error) and "KeyError" in str(error)


def test_phase3_refuses_unsupported_tasks_before_any_cluster_operation(tmp_path, monkeypatch):
    """P2 ordering: a workflow with an unconvertible task must FAIL before
    Step A — no cluster creation, library install, or workflow creation."""
    from dbx_export.__main__ import phase3_create_clusters_and_workflows
    from dbx_export.dependency_resolver import Graph, JobNode
    from dbx_export.report import Report

    calls = []

    class FakeAidp:
        def __getattr__(self, name):
            def record(*a, **k):
                calls.append(name)
                if name == "find_cluster_by_name":
                    return None
                if name == "find_workflow_by_name":
                    return None
                return {}
            return record

    graph = Graph()
    graph.jobs["1"] = JobNode(
        dbx_job_id="1", depth=0, job_def={"settings": _unbridgeable_settings()})
    report = Report(run_id="t", dbx_root_job_id="1",
                    export_output="/Workspace/x", prefix="t")
    phase3_create_clusters_and_workflows(
        aidp=FakeAidp(), dbx=object(), graph=graph, nb_target_paths={},
        uri_to_aidp={}, export_output="/Workspace/x", prefix="t",
        overwrite_workflow=False, cluster_override_key=None, report=report,
    )
    assert [j.status for j in report.jobs] == ["FAILED"]
    assert "non-notebook task" in report.jobs[0].failure_reason
    cluster_ops = [c for c in calls if "cluster" in c or "workflow" in c or "install" in c]
    assert cluster_ops == [], f"cluster/workflow ops before refusal: {cluster_ops}"


def test_model_dispatch_notebooks_export_as_notebooks():
    """Increment 3: .ipynb files in a discovered resource dir are exported as
    run_dep notebooks (the dispatch targets of a runtime-computed
    notebook.run), alongside the yaml resources — live gap 2026-10-13:
    0 of 30 model notebooks were exported."""
    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(url):
            return ("owner", "repo") if url else (None, None)

        def get_job(self, _job_id):
            settings = _bridge_settings()
            # init enumerates the models dir and reads yaml beneath it
            settings["tasks"][0]["notebook_task"]["notebook_path"] = "notebooks/init"
            return {"settings": settings}

        def export_notebook_from_git(self, path, *_context):
            if path == "notebooks/init":
                src = (
                    'TARGET_FOLDER = "models"\n'
                    'url = f"https://api.github.com/repos/o/r/contents/{TARGET_FOLDER}?ref=b"\n'
                    "directories = requests.get(url).json()\n"
                    "for item in directories:\n"
                    '    model = {"yaml_path": f"{item[\'path\']}/params.yaml"}\n'
                    '    with open("../" + model["yaml_path"], "r") as fh:\n'
                    "        fh.read()\n"
                )
            else:
                src = "x = 1\n"
            return ExportedFile(json.dumps(_notebook(src)).encode(), True, ".ipynb")

        def export_resource_from_git(self, _path, *_context):
            return ExportedFile(b"yaml: 1\n", False, ".yaml")

        def list_github_dir(self, _o, _r, directory, _b):
            return {"models": "models"} if directory == "" else {}

        def list_github_dir_entries(self, _o, _r, directory, _b):
            if directory == "models":
                return [{"name": "MODEL_A", "type": "dir"}]
            if directory == "models/MODEL_A":
                return [{"name": "params.yaml", "type": "file"},
                        {"name": "train_model.ipynb", "type": "file"}]
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _m: None).resolve("1")
    yaml_node = graph.notebooks.get("models/MODEL_A/params.yaml")
    assert yaml_node is not None and yaml_node.role == "resource"
    nb_node = graph.notebooks.get("models/MODEL_A/train_model")
    assert nb_node is not None, "dispatch-target notebook must be discovered"
    assert nb_node.role == "run_dep"
    assert nb_node.export_error is None
    assert nb_node.git_owner == "owner"


def test_import_hint_does_not_promote_notebooks_to_dependencies():
    """P1 regression (2026-10-13 review): a plain `import models` must not make
    models/*.ipynb mandatory run_deps — only the enumeration+dynamic-read
    pattern (directory contents consumed at runtime) qualifies."""
    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(url):
            return ("owner", "repo") if url else (None, None)

        def get_job(self, _job_id):
            settings = _bridge_settings()
            settings["tasks"][0]["notebook_task"]["notebook_path"] = "notebooks/init"
            return {"settings": settings}

        def export_notebook_from_git(self, path, *_context):
            src = "from models import config\n" if path == "notebooks/init" else "x = 1\n"
            return ExportedFile(json.dumps(_notebook(src)).encode(), True, ".ipynb")

        def export_resource_from_git(self, _path, *_context):
            return ExportedFile(b"cfg = 1\n", False, ".py")

        def list_github_dir(self, _o, _r, directory, _b):
            return {"models": "models"} if directory == "" else {}

        def list_github_dir_entries(self, _o, _r, directory, _b):
            if directory == "models":
                return [{"name": "config.py", "type": "file"},
                        {"name": "draft.ipynb", "type": "file"}]
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _m: None).resolve("1")
    # the .py resource still accompanies the import...
    assert "models/config.py" in graph.notebooks
    # ...but the unrelated notebook is NOT promoted to a dependency
    assert "models/draft" not in graph.notebooks
    assert "models/draft.ipynb" not in graph.notebooks


def test_driver_literal_params_use_the_workspace_resource_mapping():
    """Review finding 2026-10-14 (3/3): parity with the ordinary notebook
    branch — a pure-literal parameter naming a translated workspace resource
    is rewritten to its exported location; {{input...}} references and
    literals outside the map pass through untouched."""
    from dbx_export.foreach_bridge import build_driver_notebook
    task = _foreach_task()
    task["for_each_task"]["task"]["notebook_task"]["base_parameters"] = {
        "config_path": "/Workspace/Shared/config.yaml",
        "other_literal": "untouched-value",
        "target_notebook": "{{input.file_path}}",
    }
    nb = build_driver_notebook(
        task, "/Workspace/x/inner.ipynb", job_id="1",
        inner_timeout_seconds=36000,
        workspace_resource_path_map={
            "/Workspace/Shared/config.yaml": "/Workspace/export/resources/config.yaml"},
    )
    src = "".join(nb["cells"][0]["source"])
    assert "/Workspace/export/resources/config.yaml" in src
    assert "/Workspace/Shared/config.yaml" not in src
    assert "'untouched-value'" in src
    assert "_lookup(_el, ('file_path',))" in src


def test_driver_diagnostics_never_print_parameter_values():
    """Review finding 2026-10-14 (credential path): iteration logs show arg
    KEYS and counts only; failure entries carry the element index, never the
    element or values."""
    from dbx_export.foreach_bridge import build_driver_notebook
    elements = [{"model_name": "SECRET_TOKEN_VALUE", "file_path": "A.ipynb",
                 "yaml_path": "A.yaml"},
                {"model_name": "B"}]  # missing fields -> failure entry
    nb = build_driver_notebook(_foreach_task(), "/Workspace/x/inner.ipynb",
                               job_id="1", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    calls, printed, error = _run_driver(src, json.dumps(elements), {})
    joined = "\n".join(printed)
    assert "SECRET_TOKEN_VALUE" not in joined, "parameter values must not be printed"
    assert "args keys=" in joined
    assert isinstance(error, RuntimeError)
    assert "SECRET_TOKEN_VALUE" not in str(error)
    assert "'B'" not in str(error) and "model_name" not in str(error).split("missing key")[0]
    # ...while the actual child invocation still received the real values
    assert calls[0][2]["model_version"] == "SECRET_TOKEN_VALUE"


def test_conversion_decision_logs_input_shape_not_contents():
    settings = _bridge_settings()
    settings["tasks"][1]["for_each_task"]["inputs"] = json.dumps(
        [{"model_name": "SECRET_A"}, {"model_name": "SECRET_B"}])
    nested_params = settings["tasks"][1]["for_each_task"]["task"]["notebook_task"]
    nested_params["base_parameters"] = {"model_version": "{{input.model_name}}"}
    decisions, generated = [], []
    convert_dbx_job(
        job_data={"job_id": "1", "settings": settings},
        export_output="/Workspace/scratch/export", prefix="test",
        dbx_cluster_key_to_aidp_key={"Example_pipeline_BASE_cluster": "aidp-key"},
        conversion_decisions=decisions, generated_notebooks=generated,
    )
    line = next(d for d in decisions if "for_each_task converted" in d)
    assert "static list (2 elements)" in line
    assert "SECRET_A" not in line and "SECRET_B" not in line


def _phase3_driver_upload(existing_sources, overwrite):
    """Drive the REAL phase3 against a FakeAidp with a pre-existing driver."""
    from dbx_export.__main__ import phase3_create_clusters_and_workflows
    from dbx_export.dependency_resolver import Graph, JobNode
    from dbx_export.report import Report

    puts = []

    class FakeAidp:
        def get_notebook(self, _path):
            if existing_sources is None:
                return None
            return {"cells": [{"cell_type": "code", "source": src,
                               "metadata": {}, "outputs": []}
                              for src in existing_sources]}

        def put_notebook(self, path, nb):
            puts.append(path)

        def find_workflow_by_name(self, _n):
            return None

        def find_cluster_by_name(self, _n):
            return None

        def create_workflow_shell(self, name, description=""):
            return {"key": "wf-key"}

        def update_workflow(self, key, payload):
            return {}

        def __getattr__(self, name):
            return lambda *a, **k: {}

    graph = Graph()
    graph.jobs["1000000000000001"] = JobNode(
        dbx_job_id="1000000000000001", depth=0,
        job_def={"job_id": "1000000000000001", "settings": _bridge_settings()})
    report = Report(run_id="t", dbx_root_job_id="1000000000000001",
                    export_output="/Workspace/scratch/export", prefix="t")
    phase3_create_clusters_and_workflows(
        aidp=FakeAidp(), dbx=object(), graph=graph, nb_target_paths={},
        uri_to_aidp={}, export_output="/Workspace/scratch/export", prefix="t",
        overwrite_workflow=overwrite,
        cluster_override_key="override-cluster", report=report,
    )
    return puts, report


def test_phase3_reuses_identical_existing_driver():
    """Review finding 2026-10-14 (no-overwrite): identical driver -> reuse."""
    from dbx_export.foreach_bridge import build_driver_notebook
    from dbx_export.sanitizer import dbx_to_aidp_path, KIND_NOTEBOOK
    inner = dbx_to_aidp_path("notebooks/generate_base",
                             "/Workspace/scratch/export", KIND_NOTEBOOK, is_git=True)
    expected = build_driver_notebook(
        _foreach_task(), inner, job_id="1000000000000001",
        inner_timeout_seconds=36000)
    sources = ["".join(c["source"]) for c in expected["cells"]]
    puts, report = _phase3_driver_upload(sources, overwrite=False)
    assert puts == [], "identical existing driver must be reused, not re-PUT"
    assert [j.status for j in report.jobs] == ["CREATED"]


def test_phase3_refuses_differing_driver_without_overwrite():
    puts, report = _phase3_driver_upload(["# some other content\n"], overwrite=False)
    assert puts == []
    assert [j.status for j in report.jobs] == ["FAILED"]
    assert "--overwrite" in report.jobs[0].failure_reason


def test_phase3_replaces_differing_driver_with_overwrite():
    puts, report = _phase3_driver_upload(["# some other content\n"], overwrite=True)
    assert len(puts) == 1 and puts[0].endswith(
        "aidp_foreach_driver__1000000000000001__run_base_notebooks.ipynb")
    assert [j.status for j in report.jobs] == ["CREATED"]


def test_child_returns_and_refusals_never_leak_values():
    """Round-3 class closure: child RETURN values, refusal messages, and
    template errors carry no literal content."""
    from dbx_export.foreach_bridge import (
        build_driver_notebook, foreach_unsupported_reason, _param_expression)
    # child return value with a secret -> OK line shows type/length only
    nb = build_driver_notebook(_foreach_task(), "/Workspace/x/inner.ipynb",
                               job_id="1", inner_timeout_seconds=36000)
    src = "".join(nb["cells"][0]["source"])
    elements = [{"model_name": "A", "file_path": "A.ipynb", "yaml_path": "A.yaml"}]
    calls, printed, error = _run_driver(
        src, json.dumps(elements), {1: "result token=RETURNED_SECRET"})
    assert error is None and len(calls) == 1
    joined = "\n".join(printed)
    assert "RETURNED_SECRET" not in joined
    # the redacted OK line still reports shape: type + length of the return
    assert f"returned str len={len('result token=RETURNED_SECRET')}" in joined

    # unsupported inputs carrying values -> shape only
    task = _foreach_task()
    task["for_each_task"]["inputs"] = '{"api_token": "INPUT_SECRET"}'
    reason = foreach_unsupported_reason(task)
    assert reason and "INPUT_SECRET" not in reason and "chars" in reason

    # non-input template carrying a literal -> name only, no template text
    task2 = _foreach_task()
    task2["for_each_task"]["task"]["notebook_task"]["base_parameters"] = {
        "p": "{{tasks.x.values.y}} TEMPLATE_SECRET"}
    reason2 = foreach_unsupported_reason(task2)
    assert reason2 and "TEMPLATE_SECRET" not in reason2 and "'p'" in reason2
    try:
        _param_expression("{{tasks.x.values.y}} TEMPLATE_SECRET")
        raise AssertionError("should refuse")
    except ValueError as exc:
        assert "TEMPLATE_SECRET" not in str(exc)


def test_phase3_markdown_cell_driver_is_not_identical():
    """Round-3: identical TEXT in a markdown cell is not an executable
    driver — identity includes the cell type."""
    from dbx_export.foreach_bridge import build_driver_notebook
    from dbx_export.sanitizer import dbx_to_aidp_path, KIND_NOTEBOOK
    inner = dbx_to_aidp_path("notebooks/generate_base",
                             "/Workspace/scratch/export", KIND_NOTEBOOK, is_git=True)
    expected = build_driver_notebook(
        _foreach_task(), inner, job_id="1000000000000001",
        inner_timeout_seconds=36000)
    sources = ["".join(c["source"]) for c in expected["cells"]]
    # same text, but delivered as markdown: must be treated as DIFFERING
    puts, report = _phase3_driver_upload_md(sources, overwrite=False)
    assert puts == []
    assert [j.status for j in report.jobs] == ["FAILED"]


def _phase3_driver_upload_md(existing_sources, overwrite):
    from dbx_export.__main__ import phase3_create_clusters_and_workflows
    from dbx_export.dependency_resolver import Graph, JobNode
    from dbx_export.report import Report
    puts = []

    class FakeAidp:
        def get_notebook(self, _path):
            return {"cells": [{"cell_type": "markdown", "source": src,
                               "metadata": {}, "outputs": []}
                              for src in existing_sources]}
        def put_notebook(self, path, nb): puts.append(path)
        def find_workflow_by_name(self, _n): return None
        def find_cluster_by_name(self, _n): return None
        def create_workflow_shell(self, name, description=""): return {"key": "wf"}
        def update_workflow(self, key, payload): return {}
        def __getattr__(self, name): return lambda *a, **k: {}

    graph = Graph()
    graph.jobs["1000000000000001"] = JobNode(
        dbx_job_id="1000000000000001", depth=0,
        job_def={"job_id": "1000000000000001", "settings": _bridge_settings()})
    report = Report(run_id="t", dbx_root_job_id="1000000000000001",
                    export_output="/Workspace/scratch/export", prefix="t")
    phase3_create_clusters_and_workflows(
        aidp=FakeAidp(), dbx=object(), graph=graph, nb_target_paths={},
        uri_to_aidp={}, export_output="/Workspace/scratch/export", prefix="t",
        overwrite_workflow=overwrite, cluster_override_key="override-cluster",
        report=report)
    return puts, report
