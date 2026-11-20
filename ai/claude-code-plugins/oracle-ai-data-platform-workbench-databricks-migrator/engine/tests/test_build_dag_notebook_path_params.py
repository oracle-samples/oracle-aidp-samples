"""The DAG manifest exports notebook_path parameters with their child notebooks."""

import asyncio
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from build_dag_from_workflow import (  # noqa: E402
    build_manifest,
    _rewrite_manifest_notebook_path_parameters,
)
from job_migrate_from_workflow import derive_export_base  # noqa: E402
import build_dag_from_workflow as workflow_dag  # noqa: E402


BASE = "/Workspace/export/notebooks"
PARENT = f"{BASE}/Users/test/parent.ipynb"
CHILD = f"{BASE}/Users/test/child.ipynb"
GRANDCHILD = f"{BASE}/Users/test/grandchild.ipynb"
RAW_CHILD = "/Workspace/Users/test/child.ipynb"
RAW_GRANDCHILD = "/Workspace/Users/test/grandchild.ipynb"


def _inputs(task_value=None, job_value=None):
    task_parameters = [] if task_value is None else [
        {"name": "notebook_path", "value": task_value},
    ]
    job_parameters = [] if job_value is None else [
        {"name": "notebook_path", "value": job_value},
    ]
    tasks = [{
        "task_key": "parent", "notebook_path": PARENT,
        "depends_on": [], "parameters": task_parameters,
    }]
    run_deps = {"parent": [{
        "original_path": RAW_CHILD, "path": CHILD,
        "location": "external", "exists": True,
        "nested_deps": [{
            "original_path": RAW_GRANDCHILD, "path": GRANDCHILD,
            "location": "external", "exists": True, "nested_deps": [],
        }],
    }]}
    job_data = {"key": "job", "name": "example", "parameters": job_parameters}
    return tasks, run_deps, job_data


def _build(tasks, run_deps, job_data, **kwargs):
    return build_manifest(
        "example", tasks, [["parent"]], run_deps, job_data,
        base_prefix=BASE, source_prefix=BASE, **kwargs,
    )["jobs"][0]


@pytest.mark.parametrize("source", ["task", "job", "run", "override"])
@pytest.mark.parametrize("value", [RAW_CHILD, RAW_CHILD[:-6], CHILD])
def test_manifest_rewrites_notebook_path_from_every_parameter_source(source, value):
    tasks, run_deps, job_data = _inputs(
        task_value=value if source == "task" else None,
        job_value=value if source == "job" else None,
    )
    original = copy.deepcopy((tasks, run_deps, job_data))
    options = {}
    if source == "run":
        options["run_parameters"] = {"notebook_path": value}
    elif source == "override":
        options["parameter_overrides"] = {"notebook_path": value}
    result = _build(tasks, run_deps, job_data, **options)

    output = (result["tasks"][0]["base_parameters"] if source == "task"
              else result["parameters"])
    assert output["notebook_path"] == CHILD
    assert (tasks, run_deps, job_data) == original


def test_manifest_uses_final_cli_value_after_workflow_and_run_values():
    tasks, run_deps, job_data = _inputs(job_value=RAW_GRANDCHILD)
    result = _build(
        tasks, run_deps, job_data,
        run_parameters={"notebook_path": RAW_GRANDCHILD},
        parameter_overrides={"notebook_path": RAW_CHILD},
    )
    assert result["parameters"]["notebook_path"] == CHILD
    assert result["parameters_meta"]["notebook_path"] == {
        "source": "cli", "widget": False, "value_present": True,
    }


def test_manifest_resolves_nested_child_reference():
    tasks, run_deps, job_data = _inputs(task_value=RAW_GRANDCHILD)
    result = _build(tasks, run_deps, job_data)
    assert result["tasks"][0]["base_parameters"]["notebook_path"] == GRANDCHILD


def test_manifest_uses_validated_export_filename_after_name_correction():
    source_child = "/Workspace/Users/test/My Child.ipynb"
    exported_child = f"{BASE}/Users/test/My_Child.ipynb"
    tasks, run_deps, job_data = _inputs(job_value=source_child)
    run_deps["parent"][0]["original_path"] = source_child
    run_deps["parent"][0]["path"] = exported_child
    result = _build(tasks, run_deps, job_data)
    assert result["parameters"]["notebook_path"] == exported_child


def test_manifest_maps_source_export_path_when_target_export_base_differs():
    source_base = "/Workspace/source/notebooks"
    source_child = f"{source_base}/Users/test/child.ipynb"
    tasks, run_deps, job_data = _inputs(job_value=source_child)
    result = build_manifest(
        "example", tasks, [["parent"]], run_deps, job_data,
        base_prefix=BASE, source_prefix=source_base,
    )["jobs"][0]
    assert result["parameters"]["notebook_path"] == CHILD


@pytest.mark.parametrize("parameter_value", [None, RAW_CHILD])
def test_cross_base_task_child_uses_actual_task_path_without_conflict(
        monkeypatch, parameter_value):
    source = "/Workspace/source/notebooks"
    source_parent = f"{source}/Users/test/parent.ipynb"
    source_child = f"{source}/Users/test/child.ipynb"
    tasks = [
        {"task_key": "parent", "notebook_path": source_parent,
         "depends_on": [], "parameters": []},
        {"task_key": "child", "notebook_path": source_child,
         "depends_on": [], "parameters": []},
    ]

    async def scan(session, path, **kwargs):
        return True, [source_child] if path == source_parent else []

    monkeypatch.setattr(workflow_dag, "scan_external_notebook", scan)
    deps = asyncio.run(workflow_dag.discover_run_deps_for_tasks(
        object(), tasks, BASE, source,
    ))
    assert deps["parent"][0]["location"] == "task"
    assert deps["parent"][0]["path"] == source_child

    job_data = {"parameters": [] if parameter_value is None else [
        {"name": "notebook_path", "value": parameter_value},
    ]}
    job = build_manifest(
        "example", tasks, [["parent"], ["child"]], deps, job_data,
        base_prefix=BASE, source_prefix=source,
    )["jobs"][0]
    if parameter_value is not None:
        assert job["parameters"]["notebook_path"] == source_child


def test_cross_base_nested_task_child_uses_actual_task_path(monkeypatch):
    source = "/Workspace/source/notebooks"
    source_parent = f"{source}/Users/test/parent.ipynb"
    source_child = f"{source}/Users/test/child.ipynb"
    driver = f"{BASE}/Users/test/driver.ipynb"
    tasks = [
        {"task_key": "parent", "notebook_path": source_parent,
         "depends_on": [], "parameters": []},
        {"task_key": "child", "notebook_path": source_child,
         "depends_on": [], "parameters": []},
    ]

    async def scan(session, path, **kwargs):
        if path == source_parent:
            return True, [driver]
        return True, [source_child] if path == driver else []

    monkeypatch.setattr(workflow_dag, "scan_external_notebook", scan)
    deps = asyncio.run(workflow_dag.discover_run_deps_for_tasks(
        object(), tasks, BASE, source,
    ))
    nested = deps["parent"][0]["nested_deps"][0]
    assert nested["location"] == "task"
    assert nested["path"] == source_child
    build_manifest(
        "example", tasks, [["parent"], ["child"]], deps,
        {"parameters": []}, base_prefix=BASE, source_prefix=source,
    )


def test_manifest_resolves_reference_to_another_workflow_task():
    tasks, run_deps, job_data = _inputs(task_value=RAW_CHILD)
    tasks.append({
        "task_key": "child", "notebook_path": CHILD,
        "depends_on": [], "parameters": [],
    })
    run_deps["parent"] = [{
        "original_path": RAW_CHILD, "path": CHILD,
        "location": "task", "exists": True, "nested_deps": [],
    }]
    run_deps["child"] = []
    result = _build(tasks, run_deps, job_data)
    assert result["tasks"][0]["base_parameters"]["notebook_path"] == CHILD


@pytest.mark.parametrize("prefix", [
    "/Workspace/Users/", "Workspace/Users/", "/Users/", "/Repos/", "/Shared/", "/GIT/",
    "Users/", "Repos/", "Shared/", "GIT/",
])
@pytest.mark.parametrize("source", ["workflow", "cli"])
def test_manifest_rejects_unverified_notebook_path_instead_of_guessing(prefix, source):
    value = prefix + "other/child.ipynb"
    tasks, run_deps, job_data = _inputs(job_value=value if source == "workflow" else None)
    with pytest.raises(ValueError, match="no validated task or run_deps entry"):
        _build(tasks, run_deps, job_data, **({"parameter_overrides": {
            "notebook_path": value,
        }} if source == "cli" else {}))


def test_manifest_preserves_unset_notebook_path():
    tasks, run_deps, job_data = _inputs(job_value="")
    result = _build(tasks, run_deps, job_data)
    assert result["parameters"]["notebook_path"] == ""


@pytest.mark.parametrize("task_value,job_value,run_value,cli_value,expected", [
    (RAW_CHILD, "", None, None, CHILD),
    ("", RAW_CHILD, None, None, CHILD),
    (RAW_CHILD, "", RAW_GRANDCHILD, None, GRANDCHILD),
    (RAW_CHILD, RAW_GRANDCHILD, None, "", GRANDCHILD),
    (RAW_CHILD, "", RAW_GRANDCHILD, "", GRANDCHILD),
])
def test_empty_notebook_path_layer_does_not_hide_nonempty_binding(
        task_value, job_value, run_value, cli_value, expected):
    tasks, run_deps, job_data = _inputs(task_value=task_value, job_value=job_value)
    result = _build(
        tasks, run_deps, job_data,
        run_parameters={} if run_value is None else {"notebook_path": run_value},
        parameter_overrides={} if cli_value is None else {"notebook_path": cli_value},
    )
    effective = result["parameters"].get("notebook_path") or result["tasks"][0]["base_parameters"].get("notebook_path")
    assert effective == expected
    if cli_value == "" and run_value:
        assert result["parameters_meta"]["notebook_path"]["source"] == "run"


def test_empty_cli_notebook_path_is_absent_from_job_audit():
    tasks, run_deps, job_data = _inputs(task_value=RAW_CHILD)
    result = _build(tasks, run_deps, job_data,
                    parameter_overrides={"notebook_path": ""})
    assert "notebook_path" not in result["parameters"]
    assert "notebook_path" not in result.get("parameters_meta", {})
    assert result["tasks"][0]["base_parameters"]["notebook_path"] == CHILD


def test_cli_override_updates_widget_audit_source():
    tasks, run_deps, job_data = _inputs()
    result = _build(
        tasks, run_deps, job_data,
        widget_reads={"parent": {"reads": [
            {"name": "notebook_path", "has_call_site_default": False},
        ]}},
        parameter_overrides={"notebook_path": RAW_CHILD},
    )
    assert result["parameters_meta"]["notebook_path"] == {
        "source": "cli", "widget": True, "value_present": True,
    }


def test_discovery_ignores_empty_job_notebook_path(monkeypatch):
    tasks, _, _ = _inputs(task_value=RAW_CHILD)
    scanned = []

    async def scan(session, path, parameter_bindings=None, **kwargs):
        scanned.append(dict(parameter_bindings or {}))
        return True, [parameter_bindings["notebook_path"]] if path == PARENT else []

    monkeypatch.setattr(workflow_dag, "scan_external_notebook", scan)
    deps = asyncio.run(workflow_dag.discover_run_deps_for_tasks(
        object(), tasks, BASE, BASE,
        parameter_overrides={"notebook_path": ""},
    ))
    assert scanned[0]["notebook_path"] == RAW_CHILD
    assert deps["parent"][0]["path"] == CHILD


def test_loaded_manifest_cli_override_reuses_validated_export_child():
    tasks, run_deps, job_data = _inputs(job_value=CHILD)
    manifest = build_manifest(
        "example", tasks, [["parent"]], run_deps, job_data,
        base_prefix=BASE, source_prefix=BASE,
    )
    job = manifest["jobs"][0]
    job["parameters"]["notebook_path"] = RAW_CHILD
    export_base = derive_export_base(manifest)
    _rewrite_manifest_notebook_path_parameters(
        job["tasks"], job["parameters"], f"/Workspace/{export_base}", None,
    )
    assert job["parameters"]["notebook_path"] == CHILD


@pytest.mark.parametrize(
    "task_value,workflow_value,cli_value,expected_raw,expected",
    [
        (None, None, None, RAW_CHILD, CHILD),
        (RAW_GRANDCHILD, None, None, RAW_CHILD, CHILD),
        (None, None, RAW_GRANDCHILD, RAW_GRANDCHILD, GRANDCHILD),
        (None, RAW_GRANDCHILD, None, RAW_GRANDCHILD, GRANDCHILD),
        (None, RAW_GRANDCHILD, RAW_CHILD, RAW_CHILD, CHILD),
        (None, None, "", RAW_CHILD, CHILD),
    ],
)
def test_dag_cli_discovers_run_parameter_child_before_manifest_rewrite(
        monkeypatch, tmp_path, task_value, workflow_value, cli_value,
        expected_raw, expected):
    import aidp_config

    tasks = [{"task_key": "parent", "notebook_path": PARENT,
              "depends_on": [], "parameters": [] if task_value is None else [
                  {"name": "notebook_path", "value": task_value},
              ]}]
    job_data = {"name": "example.job", "key": "job", "parameters": [
        {"name": "notebook_path", "value": workflow_value},
    ] if workflow_value is not None else []}
    scanned = []

    class Session:
        def __init__(self, **kwargs):
            pass

        async def connect(self):
            pass

        async def close(self):
            pass

    async def validate(session, found_tasks):
        return found_tasks, []

    async def widget_reads(session, found_tasks):
        return {}

    async def scan(session, path, parameter_bindings=None, **kwargs):
        scanned.append((path, dict(parameter_bindings or {})))
        return True, [parameter_bindings["notebook_path"]] if path == PARENT else []

    monkeypatch.setattr(aidp_config, "load_config", lambda **kwargs: SimpleNamespace(
        aidp=SimpleNamespace(lake_ocid="lake", workspace_id="workspace",
                             oci_profile="DEFAULT"),
        paths=SimpleNamespace(export_output="/Workspace/export"),
    ))
    monkeypatch.setattr(workflow_dag, "AIDPSession", Session)
    monkeypatch.setattr(workflow_dag, "fetch_job_definition", lambda *a, **kw: job_data)
    monkeypatch.setattr(workflow_dag, "extract_tasks_from_job", lambda _: tasks)
    monkeypatch.setattr(workflow_dag, "validate_notebooks_exist", validate)
    monkeypatch.setattr(workflow_dag, "discover_widget_reads_for_tasks", widget_reads)
    monkeypatch.setattr(workflow_dag, "scan_external_notebook", scan)
    monkeypatch.setattr(workflow_dag, "fetch_run_parameters", lambda *a, **kw: {
        "notebook_path": RAW_CHILD,
    })
    output = tmp_path / "manifest.json"
    argv = ["build_dag_from_workflow.py", "--job-key", "job", "--cluster", "cluster",
            "--export-output", "/Workspace/export", "--params-from-run", "run",
            "--output", str(output)]
    if cli_value is not None:
        argv.extend(["--param", f"notebook_path={cli_value}"])
    monkeypatch.setattr(sys, "argv", argv)

    asyncio.run(workflow_dag.main())

    job = json.loads(output.read_text())["jobs"][0]
    assert scanned[0][1]["notebook_path"] == expected_raw
    assert job["tasks"][0]["run_deps"][0]["path"] == expected
    assert job["parameters"]["notebook_path"] == expected
    assert job["parameters_meta"]["notebook_path"]["source"] == (
        "cli" if cli_value else
        "workflow" if workflow_value is not None else "run"
    )
    if task_value is not None:
        assert job["tasks"][0]["base_parameters"]["notebook_path"] == expected
