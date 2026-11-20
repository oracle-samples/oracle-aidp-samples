"""REF-244: API naming contract, verified live on 2026-10-26.

Folder POST and notebook PUT reject unsupported names; notebook PUT can fail
with HTTP 500 when an apostrophe occurs in an auto-created parent folder.
"""
import string
import json
from unittest.mock import Mock

from dbx_export.__main__ import phase2_upload_notebooks
from dbx_export.dependency_resolver import Graph, NotebookNode
from dbx_export.job_converter import convert_dbx_task
from dbx_export.report import Report
from dbx_export.validator import cell_fingerprint

import pytest

from dbx_export.__main__ import _path_for_file
from dbx_export.sanitizer import KIND_NOTEBOOK, dbx_to_aidp_path, sanitize_segment


@pytest.mark.parametrize("character", [
    c for c in string.printable if c not in string.ascii_letters + string.digits + "_.+@ -/"
] + ["é", "中", "😀"])
def test_unsupported_characters_in_folder_and_notebook_name(character):
    assert sanitize_segment(f"Folder{character}One") == "Folder_One"
    assert dbx_to_aidp_path(
        f"/Shared/Folder{character}One/Notebook{character}Two",
        "/Workspace/export", KIND_NOTEBOOK,
    ) == "/Workspace/export/notebooks/Shared/Folder_One/Notebook_Two.ipynb"


@pytest.mark.parametrize("name", ["Alpha123", "user@example.com", "with.dot", "1digit", ".dot"])
def test_api_names_do_not_require_a_leading_letter(name):
    assert sanitize_segment(name) == name


def test_existing_export_mapping_is_preserved():
    assert sanitize_segment("Folder - Name+Part&(draft):v2") == "Folder_Name_Part_draft_v2"
    assert sanitize_segment("Workspace") == "Workspace_"


@pytest.mark.parametrize("source", [
    "/Shared/'", "/Shared/😀", "/Shared/&&",
    "/Shared/'/Notebook", "/Shared/😀/Notebook", "/Shared/&&/Notebook",
])
def test_empty_folder_or_notebook_name_is_rejected(source):
    with pytest.raises(ValueError, match="empty name"):
        dbx_to_aidp_path(source, "/Workspace/export", KIND_NOTEBOOK)


@pytest.mark.parametrize("kind", ["jar", "whl", "requirements", "init_script", "config"])
def test_empty_artifact_name_is_rejected(kind):
    with pytest.raises(ValueError, match="empty name"):
        _path_for_file("/Shared/'", kind, "/Workspace/export")


@pytest.mark.parametrize("kind, extension, bucket", [
    ("jar", ".jar", "jars"), ("whl", ".whl", "python_dependencies"),
    ("requirements", ".txt", "python_dependencies"),
    ("init_script", ".sh", "init_scripts"), ("config", ".yaml", "configs"),
])
def test_artifact_names_share_api_charset(kind, extension, bucket):
    assert _path_for_file(f"/Shared/Owner's{extension}", kind, "/Workspace/export") == (
        f"/Workspace/export/{bucket}/Owner_s{extension}"
    )


@pytest.mark.parametrize("is_git", [False, True])
@pytest.mark.parametrize("role", ["task", "run_dep"])
@pytest.mark.parametrize("extension", ["", ".ipynb", ".py", ".scala", ".sql", ".r"])
@pytest.mark.parametrize("folder, filename, safe_folder, safe_filename", [
    ("Aug'26", "generic_scoring", "Aug_26", "generic_scoring"),
    ('Aug"26', 'generic_scoring_upi_offbook_stable', "Aug_26", "generic_scoring_upi_offbook_stable"),
    ("Folder[one]", "Owner's[copy]", "Folder_one", "Owner_s_copy"),
])
def test_upload_task_download_and_percent_run_paths_agree(
    monkeypatch, is_git, role, extension, folder, filename, safe_folder, safe_filename,
):
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    import job_migrate as jm

    root = "/Workspace/export"
    stem = f"{'' if is_git else '/Shared/'}{folder}/{filename}"
    source = stem + extension
    expected = (
        f"{root}/notebooks/{'GIT/' if is_git else 'Shared/'}"
        f"{safe_folder}/{safe_filename}.ipynb"
    )
    notebook = {"cells": [{"cell_type": "code", "source": ["VALUE = 'A&B'"]}]}
    node = NotebookNode(
        dbx_path=source, role=role, nb_dict=notebook, content=json.dumps(notebook).encode(),
        git_owner="test" if is_git else None, git_repo="test" if is_git else None,
    )
    aidp = Mock()
    saved = {}
    aidp.get_notebook.side_effect = lambda path: saved.get(path)
    aidp.put_notebook.side_effect = lambda path, content: saved.__setitem__(path, content)
    report = Report("test", "synthetic", root, "test")
    targets, failed = phase2_upload_notebooks(
        aidp, Graph(notebooks={source: node}), root, False, "test", "synthetic", report,
    )
    assert targets == {source: expected} and failed == []
    assert all(error.startswith("INFO ") for error in report.errors)
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
    # Quote source paths as real %run cells must do for embedded quotes.
    reference = f"%run {jm._quote_percent_run_target(source)}"
    assert expected in jm._rewrite_internal_paths(
        reference, root + "/notebooks", dep_path_map={source: expected},
    )


def test_portable_dag_lookup_includes_sanitizer_dependencies():
    import posixpath
    import re
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import build_dag

    namespace = {"re": re, "posixpath": posixpath}
    exec(build_dag._PORTABLE_PATH_LOOKUP_SOURCE, namespace)
    assert namespace["sanitize_path"]("Shared/Aug'26/Owner's[copy]") == (
        "Shared/Aug_26/Owner_s_copy"
    )
