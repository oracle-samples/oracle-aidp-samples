"""Focused regressions for the shared ampersand path mapping."""

import json
from unittest.mock import Mock

import pytest

from dbx_export.__main__ import _path_for_file, phase2_upload_notebooks
from dbx_export.dependency_resolver import Graph, NotebookNode
from dbx_export.job_converter import convert_dbx_task
from dbx_export.report import Report
from dbx_export.sanitizer import sanitize_segment
from dbx_export.validator import cell_fingerprint


@pytest.mark.parametrize("source, expected", [
    ("A&B", "A_B"), ("A&&B", "A_B"), ("&A", "A"), ("A&", "A"),
    ("_&A&&_B&_", "A_B"), ("&&", ""), ("Plain_Name.py", "Plain_Name.py"),
])
def test_ampersand_segment_uses_existing_trimming(source, expected):
    assert sanitize_segment(source) == expected


@pytest.mark.parametrize("kind, extension, bucket", [
    ("jar", ".jar", "jars"), ("whl", ".whl", "python_dependencies"),
    ("requirements", ".txt", "python_dependencies"),
    ("init_script", ".sh", "init_scripts"), ("config", ".yaml", "configs"),
])
def test_ampersand_file_mapping(kind, extension, bucket):
    assert _path_for_file(f"/Shared/A&B{extension}", kind, "/Workspace/export") == (
        f"/Workspace/export/{bucket}/A_B{extension}"
    )


@pytest.mark.parametrize("is_git", [False, True])
@pytest.mark.parametrize("role", ["task", "run_dep"])
@pytest.mark.parametrize("extension", ["", ".ipynb", ".py", ".scala", ".sql", ".r"])
def test_ampersand_upload_readback_and_references(monkeypatch, is_git, role, extension):
    from test_pip_magic_gate import jm

    root = "/Workspace/export"
    stem = f"{'' if is_git else '/Shared/'}Folder&One/HELPER_ADF&FRESH_Monitoring"
    source = stem + extension
    expected = (
        f"{root}/notebooks/{'GIT/' if is_git else 'Shared/'}"
        "Folder_One/HELPER_ADF_FRESH_Monitoring.ipynb"
    )
    notebook = {"cells": [{"cell_type": "code", "source": ["VALUE = 'A&B'"]}]}
    node = NotebookNode(
        dbx_path=source, role=role, nb_dict=notebook, content=json.dumps(notebook).encode(),
        git_owner="test" if is_git else None, git_repo="test" if is_git else None,
    )
    aidp = Mock()
    aidp.get_notebook.side_effect = [None, notebook]
    report = Report("test", "synthetic", root, "test")
    targets, failed = phase2_upload_notebooks(
        aidp, Graph(notebooks={source: node}), root, False, "test", "synthetic", report,
    )
    assert targets == {source: expected} and failed == [] and report.errors == []
    aidp.put_notebook.assert_called_once_with(expected, notebook)
    result, = report.notebooks
    assert result.dbx_path == source and result.status == "VALIDATED"
    assert result.dbx_fingerprint == result.aidp_fingerprint == cell_fingerprint(notebook["cells"])
    assert node.content == json.dumps(notebook).encode()
    task = convert_dbx_task({"task_key": "test", "notebook_task": {
        "notebook_path": source, "source": "GIT" if is_git else "WORKSPACE",
    }}, root, {})
    assert task["notebookPath"] == expected
    monkeypatch.setattr(jm, "EXPORT_BASE", "export/notebooks")
    requested = f"{root}/notebooks/{'GIT/' if is_git else ''}{stem.lstrip('/')}.ipynb"
    assert expected in jm._download_path_variants(requested)
    for reference in (f"%run {source}", f'dbutils.notebook.run("{source}", 30)'):
        assert expected in jm._rewrite_internal_paths(
            reference, root + "/notebooks", dep_path_map={source: expected},
        )
