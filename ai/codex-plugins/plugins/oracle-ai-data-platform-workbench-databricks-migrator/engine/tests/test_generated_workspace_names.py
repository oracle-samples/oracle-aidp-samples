"""Job separator incident (caa7b2f8); preserve #300's notebook contract."""
import ast
import json
import posixpath
import re
from unittest.mock import Mock

import pytest

from dbx_export.__main__ import phase2_upload_notebooks
from dbx_export.dependency_resolver import Graph, NotebookNode
from dbx_export.job_converter import convert_dbx_task
from dbx_export.report import Report
from dbx_export.sanitizer import (
    KIND_NOTEBOOK, dbx_to_aidp_path, migration_job_output_path,
    sanitize_path, sanitize_segment,
)

JOB = "Payments Model Monitoring PSI/CSI + DS Proxy Dump"
D10 = "D10 Model Monitoring (FR, MPE, CSI, PSI) - Modular Fill Rate"
SAFE = re.compile(r"[A-Za-z0-9_.+@-]+")


@pytest.mark.parametrize("name", [JOB, "PSI/CSI", r"PSI\CSI", "Job,Part/Child", r'Aug"26\job'])
def test_job_display_name_is_one_safe_folder(name):
    base = "/Workspace/output"
    path = migration_job_output_path(base, name)
    assert posixpath.dirname(path) == base
    folder = posixpath.basename(path)
    assert SAFE.fullmatch(folder)
    assert migration_job_output_path(base, folder) == path
    assert posixpath.commonpath((base, path)) == base


@pytest.mark.parametrize("name", ["../Job", "Job/../Other", r"Job\..\Other", "Job/./Other", r"Job\.\Other", "Job/ .. /Other", " . ", " .. ", " _.._ ", "", "/", "\\"])
def test_traversal_remains_rejected(name):
    with pytest.raises(ValueError):
        migration_job_output_path("/Workspace/output", name)


@pytest.mark.parametrize("stem", [D10, "name,part.v1.2", "name.v1,part.2", "[one]", "x?y", "name'part"])
def test_workspace_mapping_is_safe_stable_and_extension_consistent(stem):
    result = sanitize_segment(stem)
    assert SAFE.fullmatch(result)
    assert sanitize_segment(result) == result
    assert sanitize_path(stem + ".ipynb") == sanitize_segment(stem + ".ipynb")
    assert dbx_to_aidp_path(stem, "/Workspace/export", KIND_NOTEBOOK) == (
        dbx_to_aidp_path(stem + ".ipynb", "/Workspace/export", KIND_NOTEBOOK)
    )


def test_notebook_names_retain_adr_0004_underscore_contract_without_digest():
    names = ["A,B", "A?B", "A_B"]
    # Export collision preflight belongs to #300; #283 must not add a competing
    # global digest mapping to resolve those identities.
    assert {sanitize_segment(name) for name in names} == {"A_B"}


@pytest.mark.parametrize("name,folder", [
    ("PSI/CSI", "PSI_CSI"),
    (r"PSI\CSI", "PSI_CSI"),
    (JOB, "Payments_Model_Monitoring_PSI_CSI_DS_Proxy_Dump"),
    ("Job,Part/Child", "Job_Part_Child"),
])
def test_only_separator_branch_uses_the_shared_underscore_mapping(name, folder):
    assert migration_job_output_path("/Workspace/output", name) == "/Workspace/output/" + folder


@pytest.mark.parametrize("name,folder", [
    ('Aug"26 job', 'Aug"26_job'),
    ('Aug"26_job', 'Aug"26_job'),
    ("Aug'26 job", "Aug'26_job"),
    ("Job,Part", "Job,Part"),
    ("Job, Part", "Job,_Part"),
    ("Plain Job", "Plain_Job"),
    ("job?name", "job?name"),
    ("[Job] Name", "[Job]_Name"),
])
def test_nonseparator_job_folders_preserve_historical_identity_and_resume(name, folder):
    expected = "/Workspace/output/" + folder
    assert migration_job_output_path("/Workspace/output", name) == expected
    assert migration_job_output_path("/Workspace/output", folder) == expected


@pytest.mark.parametrize("name", ["EXRISK_Scoring_Job", "job-v1.2", "job__name", "job@v1+part"])
def test_previously_valid_space_free_job_folders_are_unchanged(name):
    assert migration_job_output_path("/Workspace/output", name) == "/Workspace/output/" + name


@pytest.mark.parametrize("role", ["task", "run_dep"])
@pytest.mark.parametrize("is_git", [False, True])
@pytest.mark.parametrize("extension", ["", ".ipynb"])
def test_upload_and_all_callers_agree_on_d10_name(monkeypatch, role, is_git, extension):
    from test_pip_magic_gate import jm

    root = "/Workspace/export"
    stem = ("" if is_git else "/Shared/") + "Parent,Folder/" + D10
    source = stem + extension
    expected = dbx_to_aidp_path(source, root, KIND_NOTEBOOK, is_git=is_git)
    assert all(SAFE.fullmatch(segment) for segment in expected.split("/") if segment)
    notebook = {"cells": [{"cell_type": "code", "source": ["VALUE = 1"]}]}
    node = NotebookNode(dbx_path=source, role=role, nb_dict=notebook,
                        content=json.dumps(notebook).encode(),
                        git_owner="owner" if is_git else None,
                        git_repo="repo" if is_git else None)
    aidp = Mock()
    # #300 probes current and historical identities before creating a target.
    aidp.get_notebook.side_effect = [None, None, None, notebook]
    report = Report("naming", "synthetic", root, "test")
    targets, failed = phase2_upload_notebooks(
        aidp, Graph(notebooks={source: node}), root, False, "test", "synthetic", report)
    assert not failed and not report.path_plan_errors
    assert all(entry.startswith("INFO path-resolution:") for entry in report.errors)
    aidp.put_notebook.assert_called_once_with(expected, notebook)
    assert targets[source] == expected
    assert report.notebooks[0].dbx_path == source
    assert node.content == json.dumps(notebook).encode()
    task = convert_dbx_task({"task_key": "test", "notebook_task": {
        "notebook_path": source, "source": "GIT" if is_git else "WORKSPACE"}}, root, {})
    assert task["notebookPath"] == expected
    export_base = root + "/notebooks"
    monkeypatch.setattr(jm, "EXPORT_BASE", export_base)
    requested = export_base + "/" + ("GIT/" if is_git else "") + stem.lstrip("/") + ".ipynb"
    assert expected in jm._download_path_variants(requested)
    assert jm._download_path_variants(expected)[0] == expected
    for reference in (f'%run "{source}"', f'dbutils.notebook.run("{source}", 30)'):
        assert expected in jm._rewrite_internal_paths(reference, export_base, dep_path_map=targets)


@pytest.mark.parametrize("name", [JOB, r"PSI\CSI", 'Aug"26 job', "Job, Part"])
def test_job_registry_bridge_and_clone_reference_use_the_same_folder(name):
    from test_migration_job_output_path import extracted

    folder = migration_job_output_path("/Workspace/output", name)
    assert extracted("_registry_path_for_job")(name) == folder + "/migration_registry.json"
    bridge = extracted("build_oidlutils_bridge_snippet")("/Workspace/output", name)
    constants = {node.value for node in ast.walk(ast.parse(bridge))
                 if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    assert folder + "/manifest_params.json" in constants
    assert folder + "/task_values.json" in constants
    target = folder + "/notebooks/child.ipynb"
    assert extracted("_quote_percent_run_target")(target) == target


def test_generated_dag_lookup_in_fresh_kernel_has_all_sanitizer_dependencies():
    from scripts import build_dag

    namespace = {"re": re, "posixpath": posixpath}
    exec(build_dag._PORTABLE_PATH_LOOKUP_SOURCE, namespace)
    raw = "/Shared/Parent,Folder/" + D10 + ".ipynb"
    expected = dbx_to_aidp_path(raw, "/Workspace/export", KIND_NOTEBOOK)
    original, migrated = namespace["_to_export_paths"](raw, "/Workspace/export/notebooks")
    assert original == "/Workspace" + raw
    assert migrated == expected


def test_clone_registry_matching_uses_the_exported_notebook_identity():
    from scripts.clone_workflow import _notebook_match_key

    raw = "/Workspace/Users/owner/" + D10
    exported_suffix = dbx_to_aidp_path(raw, "/Workspace/export", KIND_NOTEBOOK).removeprefix("/Workspace/export/notebooks")
    assert _notebook_match_key(raw) == _notebook_match_key(exported_suffix)
