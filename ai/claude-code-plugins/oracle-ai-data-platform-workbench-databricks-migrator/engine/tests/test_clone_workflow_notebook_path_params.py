"""A cloned workflow must pass migrated child paths through notebook_path params."""

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from clone_workflow import (  # noqa: E402
    apply_parameter_overrides,
    build_clone_payload,
    clone_workflow,
    rewrite_notebook_path_parameters,
)


EXPORTED_PARENT = "/Workspace/export/notebooks/Users/test/parent.ipynb"
EXPORTED_CHILD = "/Workspace/export/notebooks/Users/test/child.ipynb"
ORIGINAL_PARENT = "/Workspace/Users/test/parent.ipynb"
ORIGINAL_CHILD = "/Workspace/Users/test/child.ipynb"
MIGRATED_PARENT = "/Workspace/migrated/parent.ipynb"
MIGRATED_CHILD = "/Workspace/migrated/child.ipynb"


def _registry():
    return {
        ORIGINAL_PARENT: {"migrated_path": MIGRATED_PARENT},
        ORIGINAL_CHILD: {"migrated_path": MIGRATED_CHILD},
    }


def _workflow(value):
    return {
        "name": "example",
        "parameters": [
            {"name": "notebook_path", "value": value},
            {"name": "output_path", "value": value},
        ],
        "tasks": [{
            "taskKey": "parent",
            "type": "NOTEBOOK_TASK",
            "notebookPath": EXPORTED_PARENT,
            "parameters": [{"name": "notebook_path", "value": value}],
        }],
    }


@pytest.mark.parametrize("value", [EXPORTED_CHILD, EXPORTED_CHILD[:-6]])
def test_clone_rewrites_job_and_task_notebook_path_without_mutating_source(value):
    source = _workflow(value)
    original = copy.deepcopy(source)
    payload, resolved, unresolved = clone_workflow(source, _registry())

    assert not unresolved
    assert resolved == [("parent", EXPORTED_PARENT, MIGRATED_PARENT)]
    assert payload["tasks"][0]["notebookPath"] == MIGRATED_PARENT
    assert payload["parameters"][0]["value"] == MIGRATED_CHILD
    assert payload["tasks"][0]["parameters"][0]["value"] == MIGRATED_CHILD
    assert payload["parameters"][1]["value"] == value
    assert source == original


def test_clone_keeps_already_migrated_notebook_path():
    payload, _, _ = clone_workflow(_workflow(MIGRATED_CHILD), _registry())
    assert payload["parameters"][0]["value"] == MIGRATED_CHILD
    assert payload["tasks"][0]["parameters"][0]["value"] == MIGRATED_CHILD


def test_clone_prefers_full_export_alias_over_same_named_registry_fallback():
    registry = _registry()
    registry["parent.ipynb"] = {"migrated_path": "/Workspace/peer/parent.ipynb"}
    payload, resolved, _ = clone_workflow(_workflow(EXPORTED_CHILD), registry)
    assert payload["tasks"][0]["notebookPath"] == MIGRATED_PARENT
    assert resolved == [("parent", EXPORTED_PARENT, MIGRATED_PARENT)]


def test_cli_parameter_override_is_rewritten_after_application():
    payload, _, _ = clone_workflow(_workflow(MIGRATED_CHILD), _registry())
    apply_parameter_overrides(payload, {"notebook_path": EXPORTED_CHILD})
    rewrite_notebook_path_parameters(payload, _registry(), _workflow(MIGRATED_CHILD))
    assert payload["parameters"][0]["value"] == MIGRATED_CHILD
    assert payload["tasks"][0]["parameters"][0]["value"] == MIGRATED_CHILD


def test_clone_override_replaces_stale_source_before_strict_rewrite():
    source = _workflow("/Workspace/Users/test/stale.ipynb")
    original = copy.deepcopy(source)
    payload, resolved, unresolved, applied, unmatched = build_clone_payload(
        source, _registry(), {"notebook_path": ORIGINAL_CHILD},
    )
    assert not unresolved and not unmatched
    assert resolved == [("parent", EXPORTED_PARENT, MIGRATED_PARENT)]
    assert payload["parameters"][0]["value"] == MIGRATED_CHILD
    assert payload["tasks"][0]["parameters"][0]["value"] == MIGRATED_CHILD
    assert ("job", "notebook_path", ORIGINAL_CHILD) in applied
    assert source == original
    with pytest.raises(ValueError, match="No migrated child"):
        build_clone_payload(source, _registry(), {})


@pytest.mark.parametrize("source_value,registry_key", [
    ("/Workspace/Users/test/Job-1.py", "/Workspace/Users/test/Job_1.ipynb"),
    ("Users/test/Job (1)+x.scala", "Users/test/Job_1_x.ipynb"),
    ("/Workspace/Users/test/Job::Name.sql", "Users/test/Job_Name.ipynb"),
])
def test_clone_parameter_uses_exporter_sanitized_path_contract(
        source_value, registry_key):
    registry = _registry()
    registry[registry_key] = {"migrated_path": "/Workspace/migrated/sanitized.ipynb"}
    payload, _, _ = clone_workflow(_workflow(source_value), registry)
    assert payload["parameters"][0]["value"] == "/Workspace/migrated/sanitized.ipynb"
    assert payload["tasks"][0]["parameters"][0]["value"] == "/Workspace/migrated/sanitized.ipynb"


@pytest.mark.parametrize("source_value", [
    "Users/test/unregistered.ipynb", "Repos/test/unregistered.py",
    "Shared/unregistered", "GIT/Repos/test/unregistered.r",
    "/Users/test/unregistered.ipynb", "Workspace/Users/test/unregistered.ipynb",
])
def test_clone_rejects_unmatched_workspace_relative_child(source_value):
    with pytest.raises(ValueError, match="No migrated child"):
        clone_workflow(_workflow(source_value), _registry())


def test_clone_resolves_git_export_namespace_from_raw_repo_path():
    registry = _registry()
    registry["/Workspace/GIT/Repos/test/child.ipynb"] = {
        "migrated_path": "/Workspace/migrated/git-child.ipynb",
    }
    source = _workflow("Repos/test/child")
    payload, _, _ = clone_workflow(source, registry)
    assert payload["parameters"][0]["value"] == "/Workspace/migrated/git-child.ipynb"


def test_clone_rejects_ambiguous_raw_and_git_child_paths():
    registry = _registry()
    registry["/Workspace/GIT/Users/test/child.ipynb"] = {
        "migrated_path": "/Workspace/migrated/git-child.ipynb",
    }
    with pytest.raises(ValueError, match="Ambiguous migrated child"):
        clone_workflow(_workflow(ORIGINAL_CHILD), registry)


@pytest.mark.parametrize("prefix", [
    "/Workspace/GIT/", "Workspace/GIT/", "/GIT/", "GIT/",
])
def test_clone_does_not_fall_back_from_explicit_git_namespace(prefix):
    with pytest.raises(ValueError, match="No migrated child"):
        clone_workflow(_workflow(prefix + "Users/test/child.ipynb"), _registry())


def test_clone_does_not_match_unrelated_same_named_notebook():
    with pytest.raises(ValueError, match="No migrated child"):
        clone_workflow(
            _workflow("/Workspace/other/child.ipynb"), _registry(),
        )


def test_clone_rejects_conflicting_full_path_registry_entries():
    registry = _registry()
    registry["Users/test/child.ipynb"] = {
        "migrated_path": "/Workspace/migrated/other-child.ipynb",
    }
    with pytest.raises(ValueError, match="Ambiguous migrated child"):
        clone_workflow(_workflow(EXPORTED_CHILD), registry)


def test_clone_leaves_non_notebook_task_parameter_unchanged():
    source = _workflow(EXPORTED_CHILD)
    source["tasks"].append({
        "taskKey": "condition", "type": "CONDITION_TASK",
        "parameters": [{"name": "notebook_path", "value": EXPORTED_CHILD}],
    })
    original = copy.deepcopy(source)
    payload, _, _ = clone_workflow(source, _registry())
    assert payload["tasks"][1]["parameters"][0]["value"] == EXPORTED_CHILD
    assert source == original
