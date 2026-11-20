"""PR #300 review repros through real export orchestration, without service writes."""
import copy
import json
from types import SimpleNamespace

import pytest

from dbx_export import __main__ as exporter
from dbx_export.aidp_client import AidpApiError
from dbx_export.dependency_resolver import DependencyResolver, FileNode, Graph, JobNode, NotebookNode
from dbx_export.dbx_client import ExportedFile, _export_api_path
from dbx_export.job_converter import convert_dbx_job, convert_dbx_task
from dbx_export.path_plan import file_target_path, prepare_export_paths
from dbx_export.report import Report
from dbx_export.sanitizer import KIND_NOTEBOOK, dbx_to_aidp_path, migration_job_output_path, sanitize_path

ROOT = "/Workspace/export"


class MemoryAidp:
    def __init__(self):
        self.notebooks, self.files, self.writes, self.reads = {}, {}, [], []
        self.lookup_error = None

    def get_notebook(self, path):
        self.reads.append(path)
        if self.lookup_error:
            raise self.lookup_error
        return copy.deepcopy(self.notebooks.get(path))

    def put_notebook(self, path, notebook):
        self.writes.append(path)
        self.notebooks[path] = copy.deepcopy(notebook)

    def list_workspace_objects(self, path):
        self.reads.append(path)
        if self.lookup_error:
            raise self.lookup_error
        return [{"path": path}] if path in self.files else []

    def object_exists(self, path):
        return path in self.files

    def upload_file(self, path, content, overwrite=False):
        self.writes.append(path)
        self.files[path] = content

    def download_file(self, path):
        self.reads.append(path)
        if self.lookup_error:
            raise self.lookup_error
        if path not in self.files:
            raise AidpApiError("downloadFileMeta", 404, "absent")
        return self.files[path]


def notebook(source, *, role="task", git=False, utility=False, value=1):
    nb = {"cells": [{"cell_type": "code", "source": [f"VALUE = {value}"]}]}
    return NotebookNode(dbx_path=source, role=role, nb_dict=nb,
        content=b"VALUE = 1" if utility else json.dumps(nb).encode(),
        is_notebook=not utility, final_extension=".py" if utility else ".ipynb",
        git_owner="owner" if git else None, git_repo="repo" if git else None)


def report():
    return Report("review", "42", ROOT, "tool")


def upload(aidp, graph, result, overwrite=False, *, files=False):
    if files:
        return exporter.phase2_upload_files(aidp, graph, ROOT, "/Workspace/libs", overwrite, result)
    return exporter.phase2_upload_notebooks(aidp, graph, ROOT, overwrite, "review", "42", result)


@pytest.mark.parametrize("bad", ["中文笔记本", "'''", "&&", "[..]", "[.]", "(..)"])
@pytest.mark.parametrize("parent", [False, True])
@pytest.mark.parametrize("role", ["task", "run_dep", "resource"])
def test_invalid_components_are_reported_before_any_upload(bad, parent, role):
    source = f"/Shared/{bad}/Notebook" if parent else f"/Shared/{bad}"
    node = notebook(source, role=role)
    aidp, result = MemoryAidp(), report()
    graph = Graph(notebooks={"/Shared/Good": notebook("/Shared/Good"), source: node})
    upload(aidp, graph, result)
    assert not aidp.writes and not aidp.reads
    assert result.path_plan_errors and result.summary()["errors"]
    assert [(r.dbx_path, r.status) for r in result.notebooks] == [(source, "EXPORT_FAILED")]
    assert result.notebooks[0].aidp_path == "UNRESOLVED"


@pytest.mark.parametrize("kind", ["jar", "whl", "requirements", "init_script", "config"])
@pytest.mark.parametrize("bad", ["中文笔记本", "'''", "[..]", "[.]"])
def test_invalid_file_basename_is_reported_without_writes(kind, bad):
    source = "/Shared/" + bad
    graph = Graph(files={source: FileNode(source, kind, content=b"x")})
    aidp, result = MemoryAidp(), report()
    targets, failed = upload(aidp, graph, result, files=True)
    assert failed and targets == {} and not aidp.writes
    assert result.files[0].status == "FAILED" and result.path_plan_errors


@pytest.mark.parametrize("character", ["'", '"', " ", "-", "(", ":", "+", "&", "中"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("role,git,utility", [("task", False, False), ("run_dep", True, False), ("resource", False, True)])
def test_duplicate_notebook_targets_stop_the_entire_batch(character, overwrite, existing, role, git, utility):
    first, second = f"/Shared/A{character}B", "/Shared/A_B"
    nodes = {p: notebook(p, role=role, git=git, utility=utility, value=i) for i, p in enumerate([first, second])}
    nodes["/Shared/Good"] = notebook("/Shared/Good")
    aidp, result = MemoryAidp(), report()
    if existing:
        path = dbx_to_aidp_path(second, ROOT, KIND_NOTEBOOK, is_git=git, extension=".py" if utility else ".ipynb")
        (aidp.files if utility else aidp.notebooks)[path] = b"old" if utility else notebook(second).nb_dict
    upload(aidp, Graph(notebooks=nodes), result, overwrite)
    assert not aidp.writes and not aidp.reads
    assert len(result.path_plan_errors) == 2
    assert all("duplicate export destination" in item["reason"] for item in result.path_plan_errors)
    assert {row.dbx_path for row in result.notebooks} == {first, second}
    assert all(row.status == "EXPORT_FAILED" for row in result.notebooks)


@pytest.mark.parametrize("kind", ["jar", "whl", "requirements", "init_script", "config"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_flattened_file_target_collisions_are_hard_failures(kind, overwrite):
    paths = ["/Shared/one/A B.txt", "/Shared/two/A_B.txt"]
    graph = Graph(files={p: FileNode(p, kind, content=str(i).encode()) for i, p in enumerate(paths)})
    aidp, result = MemoryAidp(), report()
    _, failed = upload(aidp, graph, result, overwrite, files=True)
    assert failed and not aidp.writes and len(result.path_plan_errors) == 2


@pytest.mark.parametrize("kind,state", [(kind, state)
    for kind in ("jar", "whl", "requirements", "init_script", "config")
    for state in ("external", "failed", "stale_failed", "not_fetched", "shared")]
    + [("jar", "fallback"), ("jar", "replaced")])
@pytest.mark.parametrize("overwrite", [False, True])
def test_nonwriting_files_do_not_compete_with_fetched_artifact(kind, state, overwrite):
    first, second = "file:/opt/custom/helper.jar", "dbfs:/libs/helper.jar"
    skipped = FileNode(first, kind)
    if state == "external":
        skipped.fetch_error = "external_uri_skipped"
    elif state in {"failed", "stale_failed", "fallback", "replaced"}:
        skipped.fetch_error = "HTTP 404"
    if state == "stale_failed":
        skipped.content = b"stale bytes must not be uploaded"
    if state == "shared":
        skipped.aidp_repo_path = "/Workspace/libs/helper.jar"
    if state == "fallback":
        skipped.aidp_fallback_path = "/Workspace/libs/helper.jar"
    if state == "replaced":
        skipped.aidp_replacement_paths = ["/Workspace/libs/replacement.jar"]
    graph = Graph(files={first: skipped, second: FileNode(second, kind, content=b"echo ready\n")})
    aidp, result = MemoryAidp(), report()
    targets, failed = upload(aidp, graph, result, overwrite, files=True)
    assert not result.path_plan_errors
    assert aidp.writes == [targets[second]]
    expected = {"external": "EXTERNAL_RECORD_ONLY", "failed": "FAILED", "stale_failed": "FAILED",
        "not_fetched": "FAILED", "shared": "SKIPPED_EXISTS", "fallback": "SKIPPED_EXISTS", "replaced": "SKIPPED_REPLACED"}[state]
    assert [(row.dbx_source, row.status) for row in result.files] == [(first, expected), (second, "VALIDATED")]
    assert failed == (state in {"failed", "stale_failed", "not_fetched"})


@pytest.mark.parametrize("strict_jars", [False, True])
@pytest.mark.parametrize("state", ["external_uri_skipped", "HTTP 404"])
def test_nonwriting_jar_retains_strict_failure_policy(strict_jars, state):
    first, second = "file:/opt/custom/helper.jar", "dbfs:/libs/helper.jar"
    graph = Graph(files={first: FileNode(first, "jar", fetch_error=state),
        second: FileNode(second, "jar", content=b"realjar")})
    aidp, result = MemoryAidp(), report()
    call = lambda: exporter.phase2_upload_files(aidp, graph, ROOT, "/Workspace/libs", False, result, strict_jars)
    if strict_jars and state == "HTTP 404":
        with pytest.raises(RuntimeError, match="Cannot proceed"):
            call()
    else:
        call()
        assert len(aidp.writes) == 1
    assert not result.path_plan_errors


def test_nonwriting_invalid_file_name_does_not_require_a_destination():
    first, second = "file:/opt/中文", "dbfs:/libs/good.jar"
    graph = Graph(files={first: FileNode(first, "jar", fetch_error="external_uri_skipped"),
        second: FileNode(second, "jar", content=b"realjar")})
    aidp, result = MemoryAidp(), report()
    targets, failed = upload(aidp, graph, result, files=True)
    assert not failed and not result.path_plan_errors and aidp.writes == [targets[second]]
    assert result.files[0].status == "EXTERNAL_RECORD_ONLY"


def test_zero_byte_file_is_still_a_write_candidate():
    first, second = "/Shared/one/empty.txt", "/Shared/two/empty.txt"
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(files={first: FileNode(first, "config", content=b"")}), result, files=True)
    assert aidp.writes == [ROOT + "/configs/empty.txt"] and result.files[0].size_bytes == 0
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(files={p: FileNode(p, "config", content=b"") for p in (first, second)}), result, files=True)
    assert len(result.path_plan_errors) == 2 and not aidp.writes


@pytest.mark.parametrize("role", ["task", "run_dep", "resource"])
@pytest.mark.parametrize("state", ["failed", "not_fetched"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_failed_notebook_still_reserves_its_workflow_destination(role, state, overwrite):
    first, second = "/Shared/A B", "/Shared/A_B"
    skipped = notebook(first, role=role)
    if state == "failed":
        skipped.export_error = "HTTP 404"
    else:
        skipped.content, skipped.nb_dict = None, None
    graph = Graph(notebooks={first: skipped, second: notebook(second)})
    aidp, result = MemoryAidp(), report()
    upload(aidp, graph, result, overwrite)
    # Failed notebooks still have workflow/dependency paths. A distinct
    # source must not occupy that path and execute in place of the failure.
    assert len(result.path_plan_errors) == 2 and not aidp.writes
    assert all(row.status == "EXPORT_FAILED" for row in result.notebooks)


class ModernAliasDbx:
    """Documented modern jobs/get paths; dependencies use DBX REST paths."""
    @staticmethod
    def parse_github_owner_repo(url):
        return None, None

    def get_job(self, job_id):
        return {"job_id": job_id, "settings": {"name": "alias job", "tasks": [
            {"task_key": name.lower(), "notebook_task": {"notebook_path": f'/Workspace/Shared/{name}.ipynb'}}
            for name in ("Helper", "Main")]}}

    def export_notebook(self, path):
        canonical = _export_api_path(path)
        assert canonical in {"/Shared/Helper", "/Shared/Main"}
        node = notebook(path)
        node.nb_dict.update(nbformat=4, nbformat_minor=5, metadata={})
        if canonical.endswith("/Main"):
            node.nb_dict["cells"][0]["source"] = "%run /Workspace/Shared/Helper\n"
        return ExportedFile(json.dumps(node.nb_dict).encode(), True, ".ipynb")

    def list_workspace_folder(self, path):
        return []

    def get_workspace_status(self, path):
        return {"object_type": "NOTEBOOK"}


@pytest.mark.parametrize("overwrite", [False, True])
def test_actual_resolver_task_and_run_aliases_share_destination(overwrite):
    graph = DependencyResolver(ModernAliasDbx(), log=lambda *args: None).resolve("42")
    assert set(graph.notebooks) == {"/Workspace/Shared/Helper.ipynb", "/Workspace/Shared/Main.ipynb", "/Shared/Helper"}
    aidp, result = MemoryAidp(), report()
    paths, failed = upload(aidp, graph, result, overwrite)
    assert not failed and not result.path_plan_errors
    assert paths["/Workspace/Shared/Helper.ipynb"] == paths["/Shared/Helper"]
    assert len(aidp.writes) == (3 if overwrite else 2)
    converted = convert_dbx_job(graph.jobs["42"].job_def, ROOT, "tool", {}, notebook_path_map=paths)
    assert converted["tasks"][0]["notebookPath"] == paths["/Shared/Helper"]
    assert any("INFO notebook-source-aliases" in message for message in result.errors)


@pytest.mark.parametrize("utility", [False, True])
@pytest.mark.parametrize("mismatch", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
def test_workspace_aliases_require_consistent_source_contents(utility, mismatch, overwrite):
    first = "/Workspace/Shared/Helper.py" if utility else "/Workspace/Shared/Helper.ipynb"
    second = "/Shared/Helper.py" if utility else "/Shared/Helper"
    nodes = {source: notebook(source, utility=utility) for source in (first, second)}
    if mismatch:
        nodes[second].content = b"VALUE = 2" if utility else nodes[second].content
        nodes[second].nb_dict["cells"][0]["source"] = "VALUE = 2"
    aidp, result = MemoryAidp(), report()
    _, failed = upload(aidp, Graph(notebooks=nodes), result, overwrite)
    if mismatch:
        assert failed and not aidp.writes and len(result.path_plan_errors) == 2
        assert "conflicting content for notebook source aliases" in result.path_plan_errors[0]["reason"]
    else:
        assert not failed and not result.path_plan_errors
        assert len(aidp.writes) == (2 if overwrite else 1)


@pytest.mark.parametrize("state", ["current", "legacy", "both", "neither"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("utility", [False, True])
def test_source_aliases_preserve_verified_legacy_selection(state, overwrite, utility):
    suffix = ".py" if utility else ".ipynb"
    first = '/Workspace/Shared/Folder"26/Helper' + suffix
    second = '/Shared/Folder"26/Helper' + (suffix if utility else "")
    nodes = {p: notebook(p, utility=utility) for p in (first, second)}
    node = nodes[first]
    current = dbx_to_aidp_path(first, ROOT, KIND_NOTEBOOK, extension=node.final_extension)
    legacy = dbx_to_aidp_path(first, ROOT, KIND_NOTEBOOK, extension=node.final_extension, legacy=True)
    aidp, result = MemoryAidp(), report()
    store, content = (aidp.files, node.content) if utility else (aidp.notebooks, node.nb_dict)
    if state in {"current", "both"}:
        store[current] = copy.deepcopy(content)
    if state in {"legacy", "both"}:
        store[legacy] = copy.deepcopy(content)
    targets, failed = upload(aidp, Graph(notebooks=nodes), result, overwrite)
    if state == "both":
        assert failed and len(result.path_plan_errors) == 2 and not aidp.writes
    else:
        assert not failed and not result.path_plan_errors
        assert targets[first] == targets[second] == (legacy if state == "legacy" else current)
        expected_writes = ([current, current] if overwrite else [current]) if state == "neither" else []
        assert aidp.writes == expected_writes


@pytest.mark.parametrize("git", [False, True])
def test_equal_contents_do_not_establish_alias_identity_for_distinct_sources(git):
    sources = ["/Shared/A B", "/Shared/A_B"]
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(notebooks={source: notebook(source, git=git) for source in sources}), result)
    assert len(result.path_plan_errors) == 2 and not aidp.writes


@pytest.mark.parametrize("field,value", [("git_owner", "other"), ("git_repo", "other"), ("git_branch", "other")])
def test_git_aliases_require_same_repository_context(field, value):
    first, second = "Shared/Helper", "/Shared/Helper"
    nodes = {source: notebook(source, git=True) for source in (first, second)}
    setattr(nodes[second], field, value)
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(notebooks=nodes), result)
    assert len(result.path_plan_errors) == 2 and not aidp.writes


@pytest.mark.parametrize("overwrite", [False, True])
def test_git_leading_slash_aliases_use_the_same_repository_file(overwrite):
    sources = ["Shared/Helper", "/Shared/Helper"]
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(notebooks={source: notebook(source, git=True) for source in sources}), result, overwrite)
    assert not result.path_plan_errors and len(aidp.writes) == (2 if overwrite else 1)


def test_git_file_extensions_remain_distinct_source_identity():
    sources = ["Shared/Helper", "Shared/Helper.ipynb"]
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(notebooks={source: notebook(source, git=True) for source in sources}), result)
    assert len(result.path_plan_errors) == 2 and not aidp.writes


@pytest.mark.parametrize("state", ["current", "legacy", "both", "neither"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("kind", ["notebook", "utility", "jar", "whl", "requirements", "init_script", "config"])
def test_current_legacy_existence_matrix_uses_verified_physical_target(state, overwrite, kind):
    source = '/Shared/Folder"26/Owner"s' + (".txt" if kind not in {"notebook", "utility"} else "")
    aidp, result = MemoryAidp(), report()
    if kind in {"notebook", "utility"}:
        node = notebook(source, utility=kind == "utility")
        graph = Graph(notebooks={source: node})
        kwargs = dict(extension=node.final_extension)
        current = dbx_to_aidp_path(source, ROOT, KIND_NOTEBOOK, **kwargs)
        legacy = dbx_to_aidp_path(source, ROOT, KIND_NOTEBOOK, legacy=True, **kwargs)
        store, content = (aidp.notebooks, node.nb_dict) if node.is_notebook else (aidp.files, node.content)
    else:
        node = FileNode(source, kind, content=b"echo ready\n" if kind == "init_script" else b"payload")
        graph = Graph(files={source: node})
        current, legacy = file_target_path(source, kind, ROOT), file_target_path(source, kind, ROOT, legacy=True)
        store, content = aidp.files, node.content
    if state in {"current", "both"}:
        store[current] = copy.deepcopy(content)
    if state in {"legacy", "both"}:
        store[legacy] = copy.deepcopy(content)
    targets, _ = upload(aidp, graph, result, overwrite, files=bool(graph.files))
    if state == "both":
        assert result.path_plan_errors and not aidp.writes
        assert "both current and legacy" in result.path_plan_errors[0]["reason"]
        return
    expected = legacy if state == "legacy" else current
    assert targets[source] == expected and not result.path_plan_errors
    assert aidp.writes == ([current] if state == "neither" else [])
    assert result.summary()["errors"] == 0
    if kind == "notebook":
        task = {"task_key": "task", "notebook_task": {"notebook_path": source}}
        payload = convert_dbx_job({"job_id": "42", "settings": {"name": "job", "tasks": [task]}}, ROOT, "tool", {}, notebook_path_map=targets)
        assert payload["tasks"][0]["notebookPath"] == expected
    if state != "neither":
        rows = result.files if graph.files else result.notebooks
        assert rows[0].status == "VALIDATED"
        assert rows[0].size_bytes == len(node.content)
        if kind == "notebook":
            assert rows[0].cells == len(node.nb_dict["cells"])
            assert rows[0].export_method == node.export_method


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("artifact_state", ["current", "legacy", "neither"])
def test_legacy_init_script_verification_uses_selected_artifact_paths(overwrite, artifact_state):
    from dbx_export.cluster_converter import rewrite_init_script
    source = '/Workspace/Shared/Owner"s.jar'
    script = '/Workspace/Shared/Init"script.sh'
    script_bytes = f'echo installing {source}\n'.encode()
    aidp, result = MemoryAidp(), report()
    graph = Graph(files={source: FileNode(source, "jar", content=b"jar"),
        script: FileNode(script, "init_script", content=script_bytes)})
    artifact_path = file_target_path(source, "jar", ROOT, legacy=artifact_state == "legacy")
    script_path = file_target_path(script, "init_script", ROOT, legacy=True)
    expected = rewrite_init_script(script_bytes.decode(), {source: artifact_path})[0].encode()
    aidp.files[script_path] = expected
    if artifact_state != "neither":
        aidp.files[artifact_path] = b"jar"
    targets, failed = upload(aidp, graph, result, overwrite, files=True)
    assert not failed and not result.path_plan_errors
    assert targets == {source: artifact_path, script: script_path}
    assert aidp.writes == ([artifact_path] if artifact_state == "neither" else [])
    row = next(row for row in result.files if row.dbx_source == script)
    assert row.status == "VALIDATED" and row.size_bytes == len(expected)


@pytest.mark.parametrize("where", ["current", "legacy"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("files", [False, True])
def test_mismatched_remapped_content_cannot_be_overwritten(where, overwrite, files):
    source = '/Shared/Folder"26/NB"copy'
    aidp, result = MemoryAidp(), report()
    if files:
        graph = Graph(files={source: FileNode(source, "config", content=b"new")})
        target = file_target_path(source, "config", ROOT, legacy=where == "legacy")
        aidp.files[target] = b"wrong content"
    else:
        graph = Graph(notebooks={source: notebook(source)})
        target = dbx_to_aidp_path(source, ROOT, KIND_NOTEBOOK, legacy=where == "legacy")
        aidp.notebooks[target] = notebook(source, value=2).nb_dict
    upload(aidp, graph, result, overwrite, files=files)
    assert not aidp.writes and result.path_plan_errors
    assert "differs from source" in result.path_plan_errors[0]["reason"]


@pytest.mark.parametrize("files", [False, True])
@pytest.mark.parametrize("status", [403, 500])
def test_legacy_lookup_failure_is_not_absence(files, status):
    source = '/Shared/Folder"26/NB"copy'
    graph = Graph(files={source: FileNode(source, "config", content=b"x")}) if files else Graph(notebooks={source: notebook(source)})
    aidp, result = MemoryAidp(), report()
    aidp.lookup_error = AidpApiError("lookup", status, "service failure")
    upload(aidp, graph, result, files=files)
    assert not aidp.writes and result.path_plan_errors
    assert "not absence" in result.path_plan_errors[0]["reason"]


def test_wrapped_par_transport_error_does_not_expose_temporary_url():
    source = '/Shared/Owner"config.txt'
    aidp, result = MemoryAidp(), report()
    aidp.lookup_error = AidpApiError("par_get", 0, "secret-temporary-download-url")
    upload(aidp, Graph(files={source: FileNode(source, "config", content=b"x")}), result, files=True)
    assert result.path_plan_errors and not aidp.writes
    assert "secret-temporary-download-url" not in json.dumps(result.to_dict())


def test_shared_task_references_do_not_create_a_collision():
    source = "/Shared/NB"
    node = notebook(source)
    node.referenced_by = ["job/one", "job/two"]
    aidp, result = MemoryAidp(), report()
    upload(aidp, Graph(notebooks={source: node}), result)
    assert len(aidp.writes) == 1 and not result.path_plan_errors


@pytest.mark.parametrize("source", ["/Shared/Example", "/Shared/Example.py", "/Shared/Example.sql"])
def test_task_mapping_preserves_exact_same_stem_artifact_identity(source):
    paths = {"/Shared/Example": ROOT + "/notebooks/Shared/Example.ipynb",
        "/Shared/Example.py": ROOT + "/notebooks/Shared/Example.py",
        "/Shared/Example.sql": ROOT + "/notebooks/Shared/Example.sql"}
    task = {"task_key": "task", "notebook_task": {"notebook_path": source}}
    assert convert_dbx_task(task, ROOT, {}, notebook_path_map=paths)["notebookPath"] == paths[source]


@pytest.mark.parametrize("overwrite", [False, True])
def test_generated_foreach_driver_is_part_of_collision_preflight(overwrite):
    inner = '/Shared/Folder"26/Inner'
    task = {"task_key": "loop-one", "for_each_task": {"inputs": "[1]", "task": {
        "task_key": "iteration", "notebook_task": {"notebook_path": inner}}}}
    other = copy.deepcopy(task)
    other["task_key"] = "loop_one"
    graph = Graph(notebooks={inner: notebook(inner)}, jobs={
        "42": JobNode("42", 0, job_def={"job_id": "42", "settings": {"name": "job", "tasks": [task, other]}})})
    aidp, result = MemoryAidp(), report()
    upload(aidp, graph, result, overwrite)
    assert not aidp.writes and result.path_plan_errors
    assert any(item["artifact_type"] == "generated" for item in result.path_plan_errors)


def test_generated_driver_namespace_is_distinct_from_sanitized_customer_names():
    from dbx_export.foreach_bridge import driver_notebook_name
    inner = "/Shared/Folder/Inner"
    source = "/Shared/Folder/" + driver_notebook_name("loop", "42")
    task = {"task_key": "loop", "for_each_task": {"inputs": "[1]", "task": {
        "task_key": "iteration", "notebook_task": {"notebook_path": inner}}}}
    graph = Graph(notebooks={p: notebook(p) for p in [inner, source]}, jobs={
        "42": JobNode("42", 0, job_def={"job_id": "42", "settings": {"name": "job", "tasks": [task]}})})
    result = report()
    plan = prepare_export_paths(graph, ROOT, result)
    assert not plan.errors
    # Exported user components collapse '__'; the driver's reserved name does not.
    assert set(plan.generated.values()).isdisjoint(plan.notebooks.values())


def test_relative_resolution_and_historical_resume_folder_are_preserved():
    assert sanitize_path("/Shared/sub/../NB") == "/Shared/NB"
    assert migration_job_output_path(ROOT, "Aug'26 job") == ROOT + "/Aug'26_job"
    assert migration_job_output_path(ROOT, 'Aug"26 job') == ROOT + '/Aug"26_job'


@pytest.fixture
def main_harness(monkeypatch, tmp_path):
    import aidp_config
    from dbx_export import aidp_client
    from dbx_export.jar_discovery import JarDiscoveryResult
    # main() configures the process-wide library alias scope; restore it so
    # orchestration tests cannot change subsequent library-identity tests.
    monkeypatch.setattr(aidp_client, "_artifact_alias_dirs_configured", aidp_client._artifact_alias_dirs_configured)
    config = SimpleNamespace(aidp=SimpleNamespace(lake_ocid="lake", workspace_id="ws", oci_profile="DEFAULT"),
        paths=SimpleNamespace(export_output=ROOT), dbx_source=SimpleNamespace())
    for name in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DBX_PROXY", "DBX_PROXY_BASIC_USER", "DBX_PROXY_BASIC_PASS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(aidp_config, "load_config", lambda **kwargs: config)
    monkeypatch.setattr(exporter, "configured_lib_repository_dir", lambda *args: "/Workspace/libs")
    monkeypatch.setattr(exporter, "get_oci_signer", lambda **kwargs: ({}, object()))
    monkeypatch.setattr(exporter, "DbxClient", lambda *args, **kwargs: object())
    client = MemoryAidp()
    monkeypatch.setattr(exporter, "AidpClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(exporter, "_limits_startup_gate", lambda *args: None)
    monkeypatch.setattr(exporter.ph_mod, "new_run_id", lambda: "path-plan")
    for name in ("phase1_discover_existing_cluster_libraries", "phase1_fetch_files", "phase1_discover_init_script_artifacts",
                 "resolve_failed_jar_fallbacks", "discover_notebook_pip_deps", "consolidate_cluster_requirements"):
        monkeypatch.setattr(exporter, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(exporter, "discover_notebook_jars", lambda *args, **kwargs: JarDiscoveryResult(fallback_path="/Workspace/libs"))
    return client, tmp_path


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("problem", ["empty", "traversal", "collision", "file"])
def test_main_persists_report_and_refuses_invalid_plan_in_all_modes(main_harness, monkeypatch, dry_run, overwrite, strict, problem):
    aidp, tmp_path = main_harness
    if problem == "file":
        graph = Graph(files={"中文": FileNode("中文", "config", content=b"x")})
    else:
        paths = {"empty": ["/Shared/中文"], "traversal": ["/Shared/[..]/NB"],
                 "collision": ["/Shared/DataProxy Live Flow Dump Hourly", "/Shared/DataProxy_Live_Flow_Dump_Hourly"]}[problem]
        graph = Graph(notebooks={p: notebook(p) for p in paths})
    monkeypatch.setattr(exporter, "phase1_discover", lambda *args, **kwargs: graph)
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    argv += [flag for flag, enabled in [("--dry-run", dry_run), ("--overwrite", overwrite), ("--strict", strict)] if enabled]
    assert exporter.main(argv) == exporter.ExitCode.PATH_PLAN_FAILED
    saved = json.loads(next(tmp_path.glob("export_dbx_job_42_*.json")).read_text())
    assert saved["path_plan_errors"] and saved["summary"]["errors"]
    assert not aidp.writes


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("problem", ["external", "failed", "not_fetched", "aliases"])
def test_main_continues_past_false_collisions_with_original_outcome_policy(main_harness, monkeypatch, dry_run, overwrite, strict, problem):
    aidp, tmp_path = main_harness
    if problem == "aliases":
        graph = DependencyResolver(ModernAliasDbx(), log=lambda *args: None).resolve("42")
    else:
        first, second = "file:/opt/custom/helper.jar", "dbfs:/libs/helper.jar"
        error = {"external": "external_uri_skipped", "failed": "HTTP 404", "not_fetched": None}[problem]
        graph = Graph(files={first: FileNode(first, "jar", fetch_error=error),
            second: FileNode(second, "jar", content=b"realjar")},
            jobs={"42": JobNode("42", 0, job_def={"job_id": "42", "settings": {"name": "job", "tasks": []}})})
    monkeypatch.setattr(exporter, "phase1_discover", lambda *args, **kwargs: graph)
    phase3 = []
    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", lambda **kwargs: phase3.append(kwargs))
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    argv += [flag for flag, enabled in [("--dry-run", dry_run), ("--overwrite", overwrite), ("--strict", strict)] if enabled]
    expected = exporter.ExitCode.OPTIONAL_FILE_UPLOAD_FAILED if not dry_run and problem in {"failed", "not_fetched"} else exporter.ExitCode.OK
    assert exporter.main(argv) == expected
    saved = json.loads(next(tmp_path.glob("export_dbx_job_42_*.json")).read_text())
    assert not saved["path_plan_errors"]
    assert len(phase3) == (0 if dry_run else 1)
    if dry_run:
        assert not aidp.writes
    elif problem == "aliases":
        assert len(aidp.writes) == (3 if overwrite else 2)
    else:
        assert len(aidp.writes) == 1
        assert any(row["status"] == ("EXTERNAL_RECORD_ONLY" if problem == "external" else "FAILED") for row in saved["files"])


def test_invalid_download_alias_preserves_read_candidates(monkeypatch, capsys):
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    import job_migrate
    monkeypatch.setattr(job_migrate, "EXPORT_BASE", ROOT + "/notebooks")
    raw = ROOT + "/notebooks/Shared/中文.ipynb"
    assert raw in job_migrate._download_path_variants(raw)
    assert "omitted invalid sanitized download alias" in capsys.readouterr().out


@pytest.mark.parametrize("files", [False, True])
def test_transport_errors_are_reported_without_temporary_urls(files):
    from requests.exceptions import ConnectionError
    source = '/Shared/Folder"26/NB"copy'
    graph = Graph(files={source: FileNode(source, "config", content=b"x")}) if files else Graph(notebooks={source: notebook(source)})
    aidp, result = MemoryAidp(), report()
    aidp.lookup_error = ConnectionError("https://temporary.invalid/private-par-token")
    upload(aidp, graph, result, files=files)
    assert result.path_plan_errors and not aidp.writes
    assert "private-par-token" not in json.dumps(result.to_dict())


def test_expired_par_is_not_misclassified_as_an_absent_file():
    source = '/Shared/Owner"s.txt'
    aidp, result = MemoryAidp(), report()
    aidp.lookup_error = AidpApiError("par_get", 404, "expired download")
    upload(aidp, Graph(files={source: FileNode(source, "config", content=b"x")}), result, files=True)
    assert result.path_plan_errors and not aidp.writes


def test_legacy_notebook_target_is_used_inside_foreach_driver():
    source = '/Shared/Folder"26/NB'
    node = notebook(source)
    aidp, result = MemoryAidp(), report()
    legacy = dbx_to_aidp_path(source, ROOT, KIND_NOTEBOOK, legacy=True)
    aidp.notebooks[legacy] = copy.deepcopy(node.nb_dict)
    targets, _ = upload(aidp, Graph(notebooks={source: node}), result)
    task = {"task_key": "loop", "for_each_task": {"inputs": "[1]", "task": {
        "task_key": "iteration", "notebook_task": {"notebook_path": source}}}}
    generated = []
    convert_dbx_task(task, ROOT, {}, notebook_path_map=targets, generated_notebooks=generated)
    import ast
    code = "\n".join("".join(cell["source"]) for cell in generated[0][1]["cells"] if cell["cell_type"] == "code")
    calls = [node for node in ast.walk(ast.parse(code)) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute) and node.func.attr == "run"]
    assert any(ast.literal_eval(call.args[0]) == legacy for call in calls)


# REF-294: run c8cee683 / c0083e57 (2026-10-28 UTC) discovered one Git
# constants.py both as an extensionless sibling and an exact resource path.
def git_source_alias_graph(monkeypatch, suffix=".py"):
    from urllib.parse import unquote, urlsplit
    import requests
    from dbx_export.dbx_client import DbxClient, DbxConfig

    main = notebook("main").nb_dict
    main.update(nbformat=4, nbformat_minor=5, metadata={})
    main["cells"][0]["source"] = "from HelperFunctions.constants import VALUE\n%run ./needed\n"
    constant_notebook = notebook("constants").nb_dict
    constant_notebook.update(nbformat=4, nbformat_minor=5, metadata={})
    if suffix == ".ipynb":
        main["cells"][0]["source"] += "%run ./constants\n"
    files = {
        "project/HelperFunctions/Main.ipynb": json.dumps(main).encode(),
        "project/HelperFunctions/constants" + suffix: (
            b"VALUE = 1\n" if suffix == ".py" else json.dumps(constant_notebook).encode()),
        "project/HelperFunctions/needed.py": b"# Databricks notebook source\nVALUE = 2\n",
    }

    def get(url, **kwargs):
        path = unquote(urlsplit(url).path.split("/contents", 1)[1]).lstrip("/")
        response = requests.Response()
        response.status_code = 200
        if path in files:
            response._content = files[path]
        else:
            children = {}
            prefix = path + "/" if path else ""
            for filename in files:
                if filename.startswith(prefix):
                    child, _, tail = filename[len(prefix):].partition("/")
                    children[child] = {"name": child, "type": "dir" if tail else "file"}
            response._content = json.dumps(list(children.values())).encode()
        return response

    monkeypatch.setattr("dbx_export.dbx_client.requests.get", get)
    client = DbxClient(DbxConfig(proxy="proxy.example:8080", proxy_max_retries=1))
    tasks = [{"task_key": "main", "notebook_task": {
        "source": "GIT", "notebook_path": "project/HelperFunctions/Main"}}]
    if suffix == ".ipynb":
        # The job's exact path and a %run's normalized path reach one Git file.
        tasks.append({"task_key": "constants", "notebook_task": {
            "source": "GIT", "notebook_path": "project/HelperFunctions/constants.ipynb"}})
    monkeypatch.setattr(client, "get_job", lambda job: {"job_id": job, "settings": {
        "git_source": {"git_url": "https://github.com/owner/repo", "git_branch": "test"},
        "tasks": tasks}})
    return DependencyResolver(client, log=lambda message: None).resolve("42")


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("suffix", [".py", ".ipynb"])
def test_git_source_aliases_resolve_and_upload_once(monkeypatch, reverse, overwrite, existing, suffix):
    graph = git_source_alias_graph(monkeypatch, suffix)
    stem = "project/HelperFunctions/constants"
    exact = stem + suffix
    assert graph.notebooks[stem].role == ("py_sibling" if suffix == ".py" else "run_dep")
    assert graph.notebooks[exact].role == ("resource" if suffix == ".py" else "task")
    assert graph.notebooks[stem].resolved_git_path == graph.notebooks[exact].resolved_git_path == exact
    # The graph keys and existing explicit-notebook promotion remain intact.
    assert graph.notebooks["project/HelperFunctions/needed"].role == "run_dep"
    if reverse:
        graph.notebooks = dict(reversed(list(graph.notebooks.items())))
    canonical, alias = [path for path in graph.notebooks if path in (stem, exact)]
    target = dbx_to_aidp_path(stem, ROOT, KIND_NOTEBOOK, is_git=True, extension=suffix)
    aidp, result = MemoryAidp(), report()
    if existing:
        if suffix == ".py":
            aidp.files[target] = graph.notebooks[stem].content
        else:
            aidp.notebooks[target] = graph.notebooks[stem].nb_dict
    paths, failed = upload(aidp, graph, result, overwrite)
    assert not failed and not result.path_plan_errors
    assert paths[stem] == paths[exact] == target
    assert aidp.writes.count(target) == (0 if existing and not overwrite else 1)
    rows = {row.dbx_path: row for row in result.notebooks}
    assert rows[canonical].status == rows[alias].status == ("SKIPPED_EXISTS" if existing and not overwrite else "VALIDATED")
    assert rows[alias].alias_of == canonical
    for source in (stem, exact):
        assert rows[source].referenced_by == graph.notebooks[source].referenced_by
    assert any("INFO notebook-source-aliases" in message for message in result.errors)
    converted = convert_dbx_job(graph.jobs["42"].job_def, ROOT, "tool", {}, notebook_path_map=paths)
    assert converted["tasks"][0]["notebookPath"] == paths["project/HelperFunctions/Main"]


@pytest.mark.parametrize("problem", ["owner", "repo", "ref", "missing_identity", "distinct_file",
                                    "content", "representation", "failed", "unfetched", "workspace"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("suffix", [".py", ".ipynb"])
def test_source_identity_boundary(monkeypatch, problem, overwrite, suffix):
    graph = git_source_alias_graph(monkeypatch, suffix)
    stem = "project/HelperFunctions/constants"
    exact = stem + suffix
    graph.notebooks = {path: graph.notebooks[path] for path in (stem, exact)}
    other = graph.notebooks[exact]
    if problem in {"owner", "repo", "ref"}:
        setattr(other, {"owner": "git_owner", "repo": "git_repo", "ref": "git_branch"}[problem], "other")
    elif problem == "missing_identity":
        other.resolved_git_path = None
    elif problem == "distinct_file":
        graph.notebooks[stem].resolved_git_path = stem + (".ipynb" if suffix == ".py" else ".py")
    elif problem == "content":
        other.content = b"VALUE = 2\n"
    elif problem == "representation":
        other.is_notebook = not other.is_notebook
    elif problem == "failed":
        other.export_error = "not_found"
    elif problem == "unfetched":
        other.content = None
    elif problem == "workspace":
        for node in graph.notebooks.values():
            node.git_owner = node.git_repo = node.git_branch = node.resolved_git_path = None
    aidp, result = MemoryAidp(), report()
    upload(aidp, graph, result, overwrite)
    if problem == "workspace" and suffix == ".ipynb":
        # Existing workspace API aliases remain valid without Git metadata.
        assert not result.path_plan_errors
        assert len(aidp.writes) == (2 if overwrite else 1)
    else:
        assert len(result.path_plan_errors) == 2 and not aidp.writes
        assert all(row.status == "EXPORT_FAILED" for row in result.notebooks)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("failure", ["upload", "readback", "checksum"])
@pytest.mark.parametrize("suffix", [".py", ".ipynb"])
def test_source_aliases_share_upload_failure(monkeypatch, reverse, failure, suffix):
    graph = git_source_alias_graph(monkeypatch, suffix)
    stem = "project/HelperFunctions/constants"
    exact = stem + suffix
    sources = (exact, stem) if reverse else (stem, exact)
    graph.notebooks = {path: graph.notebooks[path] for path in sources}
    graph.notebooks[stem].role = "task"
    graph.notebooks[exact].role = "resource"
    aidp, result = MemoryAidp(), report()
    def fail(*args, **kwargs):
        raise AidpApiError("test", 500, "injected failure")
    if failure == "upload":
        monkeypatch.setattr(aidp, "upload_file" if suffix == ".py" else "put_notebook", fail)
    elif suffix == ".py":
        monkeypatch.setattr(aidp, "download_file", fail if failure == "readback" else lambda path: b"wrong bytes")
    else:
        original_get = aidp.get_notebook
        def get_notebook(path):
            if path not in aidp.notebooks:
                return original_get(path)
            if failure == "readback":
                return fail()
            return notebook(path, value=2).nb_dict
        monkeypatch.setattr(aidp, "get_notebook", get_notebook)
    _, failed = upload(aidp, graph, result, overwrite=True)
    assert failed == [stem]
    assert len(aidp.writes) == (0 if failure == "upload" else 1)
    assert {row.dbx_path for row in result.notebooks} == {stem, exact}
    assert len({row.status for row in result.notebooks}) == 1
    assert all(row.status in {"FAILED", "FINGERPRINT_MISMATCH"} for row in result.notebooks)
    assert result.summary()["local_resources_failed"] == 1


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("failure", [None, "upload", "content"])
@pytest.mark.parametrize("strict_deps", [False, True])
@pytest.mark.parametrize("suffix", [".py", ".ipynb"])
def test_main_git_source_alias_outcome(main_harness, monkeypatch, dry_run, overwrite, failure, strict_deps, suffix):
    graph = git_source_alias_graph(monkeypatch, suffix)
    stem = "project/HelperFunctions/constants"
    aidp, tmp_path = main_harness
    if failure == "content":
        graph.notebooks[stem + suffix].content = b"different bytes"
    if failure == "upload":
        original_upload = aidp.upload_file if suffix == ".py" else aidp.put_notebook
        def upload_artifact(path, content, **kwargs):
            if path.endswith("/constants" + suffix):
                raise AidpApiError("upload", 500, "injected failure")
            return original_upload(path, content, **kwargs)
        monkeypatch.setattr(aidp, "upload_file" if suffix == ".py" else "put_notebook", upload_artifact)
    monkeypatch.setattr(exporter, "phase1_discover", lambda *args, **kwargs: graph)
    phase3 = []
    monkeypatch.setattr(exporter, "phase3_create_clusters_and_workflows", lambda **kwargs: phase3.append(kwargs))
    argv = ["--job-id", "42", "--dbx-proxy", "test-proxy:8080", "--report-dir", str(tmp_path)]
    argv += [flag for flag, enabled in [("--dry-run", dry_run), ("--overwrite", overwrite), ("--strict-deps", strict_deps)] if enabled]
    upload_failure = exporter.ExitCode.STRICT_DEPS_FAILED if strict_deps else exporter.ExitCode.MANDATORY_FILE_UPLOAD_FAILED
    expected = (exporter.ExitCode.PATH_PLAN_FAILED if failure == "content" else
                upload_failure if failure == "upload" and not dry_run else exporter.ExitCode.OK)
    assert exporter.main(argv) == expected
    saved = json.loads(next(tmp_path.glob("export_dbx_job_42_*.json")).read_text())
    assert bool(saved["path_plan_errors"]) == (failure == "content")
    assert len(phase3) == (1 if failure is None and not dry_run else 0)
    if dry_run or failure == "content":
        assert not aidp.writes
    elif failure is None:
        assert sum(path.endswith("/constants" + suffix) for path in aidp.writes) == 1
    else:
        rows = [row for row in saved["notebooks"] if row["dbx_path"] in (stem, stem + suffix)]
        assert len(rows) == 2 and all(row["status"] == "FAILED" for row in rows)


@pytest.mark.parametrize("state", ["current", "legacy", "both", "neither"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("suffix", [".py", ".ipynb"])
def test_git_source_aliases_preserve_legacy_destinations(monkeypatch, state, overwrite, suffix):
    graph = git_source_alias_graph(monkeypatch, suffix)
    original = "project/HelperFunctions/constants"
    stem = 'project/Helper"Functions/constants'
    nodes = {}
    for path in (original, original + suffix):
        node = graph.notebooks[path]
        source = path.replace("HelperFunctions", 'Helper"Functions')
        node.dbx_path = source
        node.resolved_git_path = stem + suffix
        nodes[source] = node
    graph.notebooks = nodes
    current = dbx_to_aidp_path(stem, ROOT, KIND_NOTEBOOK, is_git=True, extension=suffix)
    legacy = dbx_to_aidp_path(stem, ROOT, KIND_NOTEBOOK, is_git=True, extension=suffix, legacy=True)
    aidp, result = MemoryAidp(), report()
    for target, present in ((current, state in {"current", "both"}), (legacy, state in {"legacy", "both"})):
        if present:
            if suffix == ".py":
                aidp.files[target] = nodes[stem].content
            else:
                aidp.notebooks[target] = nodes[stem].nb_dict
    paths, _ = upload(aidp, graph, result, overwrite)
    if state == "both":
        assert len(result.path_plan_errors) == 2 and not aidp.writes
    else:
        assert not result.path_plan_errors
        assert paths[stem] == paths[stem + suffix] == (legacy if state == "legacy" else current)
        assert len(aidp.writes) == (1 if state == "neither" else 0)
        assert all(row.status == "VALIDATED" for row in result.notebooks)
