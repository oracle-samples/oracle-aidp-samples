"""Keep `%run` parsing and inline validation equivalent to delivery."""

import asyncio
import contextlib
import io
import json
import re
from types import SimpleNamespace

import pytest

from dbx_export.dependency_resolver import extract_run_targets
from scripts.context_tools import extract_notebook_dependencies
from scripts.job_preflight import scan_for_children
from test_pip_magic_gate import jm


def _notebook(source: str) -> dict:
    return {"cells": [{"cell_type": "code", "source": [source]}]}


def test_context_tools_keeps_quoted_and_unquoted_spaced_targets():
    source = (
        '%run "/Workspace/Quoted Folder/child" $mode=test\n'
        "%run /Workspace/Unquoted Folder/child $date=2026-09-15\n"
    )
    assert extract_notebook_dependencies(_notebook(source)) == [
        "/Workspace/Quoted Folder/child",
        "/Workspace/Unquoted Folder/child",
    ]


def test_preprocess_leaves_percent_run_path_for_shared_resolver():
    source = "/Workspace/Users/engineer/init_util"
    mapped = "/Workspace/migrated/Monitoring Job/notebooks/Users/engineer/init_util.ipynb"
    mapping = {source: mapped}
    assert jm._preprocess_cell_source(f"%run {source}\n", mapping) == f"%run {source}\n"
    # Other call sites retain their established literal-path substitution.
    assert mapped in jm._preprocess_cell_source(
        f'dbutils.notebook.run("{source}", 30)', mapping
    )


def test_rewrite_preserves_physical_spaced_root_and_repeat_pass():
    base = "/Workspace/migrated/Monitoring Job/notebooks"
    source = "/Workspace/Users/engineer/init_util"
    mapped = f"{base}/Users/engineer/init_util.ipynb"
    rewritten = jm._rewrite_internal_paths(
        f"%run {source}\n", base, source, {source: mapped}
    )
    assert f'%run "{mapped}"' in rewritten
    assert jm._rewrite_internal_paths(rewritten, base, source, {source: mapped}) == rewritten


def test_rewrite_sanitizes_relative_leaf_without_renaming_output_root():
    base = "/Workspace/migrated/Monitoring Job/notebooks"
    parent = f"{base}/Users/engineer/parent.ipynb"
    rewritten = jm._rewrite_internal_paths("%run ./Leaf Notebook", base, parent)
    assert f'%run "{base}/Users/engineer/Leaf_Notebook.ipynb"' in rewritten


def test_rewrite_corrects_stale_physical_path_from_dependency_map():
    base = "/Workspace/migrated/Monitoring Job/notebooks"
    stale = f"{base}/Users/engineer/Leaf Notebook.ipynb"
    physical = f"{base}/Users/engineer/Leaf_Notebook.ipynb"
    mapping = {stale: physical}
    rewritten = jm._rewrite_internal_paths(
        f'%run "{stale}" $mode=prod\n', base, stale, mapping
    )
    assert f'%run "{physical}" $mode=prod' in rewritten
    assert stale not in rewritten.splitlines()[-1]
    assert jm._rewrite_internal_paths(rewritten, base, stale, mapping) == rewritten


def test_rewrite_parses_old_staging_target_before_replacing_base(monkeypatch):
    base = "/Workspace/migrated/Monitoring Job/notebooks"
    monkeypatch.setattr(jm, "_OLD_NB_BASES", ["/Workspace/old/notebooks"])
    source = "%run /Workspace/old/notebooks/Users/engineer/init_util $day=today"
    rewritten = jm._rewrite_internal_paths(source, base)
    target, trailing = jm._parse_percent_run_arguments(
        next(line for line in rewritten.splitlines() if line.startswith("%run "))[5:]
    )
    assert target == f"{base}/Users/engineer/init_util.ipynb"
    assert trailing == " $day=today"
    assert jm._rewrite_internal_paths(rewritten, base) == rewritten


@pytest.mark.parametrize("run_all", [True, False])
@pytest.mark.parametrize("source_name", ["init_util", "init&util"])
def test_parent_loop_uses_mapped_physical_child_with_spaced_root(
    monkeypatch, tmp_path, run_all, source_name
):
    from test_pip_magic_gate import _Kernel, _patch_common

    source_path = f"/Workspace/Users/engineer/{source_name}.ipynb"
    base = "/Workspace/migrated/Monitoring Job/notebooks"
    physical_path = f"{base}/Users/engineer/init_util.ipynb"
    notebook = _notebook(f"%run {source_path}")
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(jm, "MIGRATED_BASE", base)
    called = []

    async def child(path, *args, **kwargs):
        called.append(path)
        return {"status": "ok", "cells_ok": 1, "cells_fixed": 0}

    monkeypatch.setattr(jm, "_inline_child_notebook", child)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/Users/engineer/parent.ipynb", kernel,
        "fixture", "task", {}, run_all=run_all,
        dep_path_map={source_path: physical_path},
    ))
    assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
    assert called == ([physical_path] if run_all else [])
    saved = next(json.loads(text) for path, text in kernel.files.items()
                 if path.endswith(".ipynb"))
    assert any(
        f'%run "{physical_path}"' in "".join(cell.get("source", []))
        for cell in saved["cells"]
    )


def test_parent_persists_child_path_reported_by_reader(monkeypatch, tmp_path):
    from test_pip_magic_gate import _Kernel, _patch_common

    base = "/Workspace/migrated/Monitoring Job/notebooks"
    source = "/Workspace/Users/engineer/Leaf Notebook.ipynb"
    requested = f"{base}/Users/engineer/Leaf Notebook.ipynb"
    physical = f"{base}/Users/engineer/Leaf_Notebook.ipynb"
    _patch_common(monkeypatch, tmp_path, _notebook(f'%run "{source}"'), [])
    monkeypatch.setattr(jm, "MIGRATED_BASE", base)
    called = []

    async def child(path, *args, **kwargs):
        called.append(path)
        return {"status": "ok", "resolved_path": physical,
                "cells_ok": 1, "cells_fixed": 0}

    monkeypatch.setattr(jm, "_inline_child_notebook", child)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/Users/engineer/parent.ipynb", kernel,
        "fixture", "task", {}, run_all=True,
        dep_path_map={source: requested},
    ))
    assert result["status"] == "PASS", result
    assert called == [requested]
    saved = next(json.loads(data) for path, data in kernel.files.items()
                 if path.endswith(".ipynb"))
    assert f'%run "{physical}"' in "".join(saved["cells"][0]["source"])


def test_nested_persists_child_path_reported_by_reader(monkeypatch, tmp_path):
    from test_child_exit_conversion import _run

    base = tmp_path / "Monitoring Job"
    base.mkdir()
    requested = (base / "Leaf Notebook.ipynb").as_posix()
    physical = (base / "Leaf_Notebook.ipynb").as_posix()
    called = []

    async def child(path, *args, **kwargs):
        called.append(path)
        return {"status": "ok", "resolved_path": physical,
                "cells_ok": 1, "cells_fixed": 0}

    monkeypatch.setattr(jm, "_inline_isolated_percent_run_child", child)
    result, saved, kernel, _, _ = _run(
        monkeypatch, base,
        {"child": ['%run "/Workspace/Leaf Notebook"'],
         "Leaf Notebook": ["VALUE = 1"], "Leaf_Notebook": ["VALUE = 2"]},
        fire=False,
    )
    assert result["status"] == "ok", result
    assert [path.replace("\\", "/") for path in called] == [requested]
    assert f'%run "{physical}"' in re.sub(r"\\+", "/", saved["child"][0])
    assert kernel.saves == 1


def test_nested_relative_path_is_saved_under_physical_root(monkeypatch, tmp_path):
    from test_child_exit_conversion import _run

    base = tmp_path / "Monitoring Job"
    base.mkdir()
    result, saved, kernel, _, _ = _run(
        monkeypatch, base,
        {"child": ["%run ./Leaf Notebook"], "Leaf_Notebook": ["VALUE = 42"]},
        fire=False,
    )
    expected = (base / "Leaf_Notebook.ipynb").as_posix()
    assert result["status"] == "ok", result
    assert f'%run "{expected}"' in re.sub(r"\\+", "/", saved["child"][0])
    assert kernel.saves >= 1


@pytest.mark.parametrize("save_outputs", [
    [{"type": "error", "evalue": "synthetic child-save failure"}],
    [{"output_type": "error", "evalue": "synthetic child-save failure"}],
    [],
    [{"type": "stream", "text": "Replaced 0/1 cells (config_offset=0)\n"}],
    [{"type": "stream", "text": "Replaced 0/5 cells (config_offset=0)\n"}],
    [{"type": "stream", "text": "Replaced 1/2 cells (config_offset=0)\n"}],
])
def test_path_only_child_save_error_output_fails_without_publishing_cache(
    monkeypatch, tmp_path, save_outputs
):
    import test_child_exit_conversion as child_tests

    class ErrorOutputKernel(child_tests._ExitKernel):
        async def execute(self, code, **kwargs):
            if "all_cells = payload.get" in code:
                self.saves += 1
                return {"status": "ok", "outputs": save_outputs}
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(child_tests, "_ExitKernel", ErrorOutputKernel)
    base = tmp_path / "Monitoring Job"
    base.mkdir()
    original = "%run ./Leaf Notebook"
    result, saved, kernel, _, _ = child_tests._run(
        monkeypatch, base,
        {"child": [original], "Leaf_Notebook": ["VALUE = 42"]},
        fire=False,
    )
    child = base / "child.ipynb"
    assert kernel.saves == 1
    assert result["status"] == "error" and result["kind"] == "CHILD_PERSISTENCE"
    assert saved["child"] == [original]
    assert json.loads(jm._notebook_content_cache[str(child)])["cells"][0]["source"] == [original]


def test_cold_cache_reads_physical_child_and_keeps_its_save_path(monkeypatch, tmp_path):
    from test_child_exit_conversion import _ExitKernel
    from test_pip_magic_gate import _patch_common

    base = tmp_path / "Monitoring Job" / "notebooks"
    base.mkdir(parents=True)
    requested = base / "Leaf Notebook.ipynb"
    physical = base / "Leaf_Notebook.ipynb"
    physical.write_text(json.dumps(_notebook("VALUE = 42")), encoding="utf8")
    _patch_common(monkeypatch, tmp_path, _notebook(""), [])
    monkeypatch.setattr(jm, "MIGRATED_BASE", base.as_posix())
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)

    class ColdKernel(_ExitKernel):
        async def execute(self, code, **kwargs):
            if "found_path = None" in code or "src = ''.join(nb['cells']" in code:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    exec(code, {})
                return {"status": "ok", "outputs": [
                    {"type": "stream", "text": output.getvalue()}]}
            return await super().execute(code, **kwargs)

    result = asyncio.run(jm._inline_child_notebook(
        requested.as_posix(), ColdKernel(fire=False),
        SimpleNamespace(log=lambda message: None), "fixture", "", "", "", {},
    ))
    assert result["status"] == "ok", result
    assert result["resolved_path"].replace("\\", "/") == physical.as_posix()
    assert any(
        key.replace("\\", "/") == physical.as_posix()
        for key in jm._notebook_content_cache
    )
    assert requested.as_posix() not in jm._notebook_content_cache


def test_preflight_keeps_quoted_and_unquoted_spaced_targets():
    source = (
        "%run '/Workspace/Quoted Folder/child'\n"
        "%run /Workspace/Unquoted Folder/child $mode=test\n"
    )
    assert scan_for_children(json.dumps(_notebook(source)), "/Workspace/parent") == [
        "/Workspace/Quoted Folder/child",
        "/Workspace/Unquoted Folder/child",
    ]


def test_export_dependency_resolver_keeps_spaced_target_and_widget_boundary():
    targets = extract_run_targets(
        _notebook(
            "%run /Workspace/FL Queries Run/SomeNotebook "
            '$date="2026-09-15"\n'
        ),
        "/Workspace/Jobs/parent",
    )
    assert targets == ["/FL Queries Run/SomeNotebook"]


def test_all_consumers_reject_unclosed_quote_without_guessing(capsys):
    source = '%run "/Workspace/Broken Folder/child\n'
    notebook = _notebook(source)
    messages = []

    assert extract_notebook_dependencies(notebook) == []
    assert scan_for_children(json.dumps(notebook), "/Workspace/parent") == []
    assert extract_run_targets(
        notebook, "/Workspace/Jobs/parent", log=messages.append
    ) == []
    assert "malformed or empty" in capsys.readouterr().out
    assert any("malformed or empty" in message for message in messages)


def test_fix_7h_uses_shared_quote_aware_parser_for_spaced_targets(capsys):
    source = (
        '%run "/Workspace/Quoted Folder/child" $mode=test\n'
        "%run /Workspace/Unquoted Folder/child $date=2026-09-15\n"
        '%run "/Workspace/Broken Folder/child\n'
    )

    assert jm._extract_run_targets_from_source(source) == [
        "/Workspace/Quoted Folder/child",
        "/Workspace/Unquoted Folder/child",
    ]
    assert "malformed or empty Fix 7h target" in capsys.readouterr().out


def test_percent_run_bindings_parse_literals_and_reject_unknown_trailing_text():
    assert jm._parse_percent_run_bindings(
        ' $mode=prod $run_date="2026-09-28" # validation arguments'
    ) == {"mode": "prod", "run_date": "2026-09-28"}
    assert jm._parse_percent_run_bindings(" $mode=prod unexpected") is None
    assert jm._parse_percent_run_bindings(' $mode="unterminated') is None


def test_percent_run_bindings_are_isolated_and_restored_for_empty_sibling(
    monkeypatch, tmp_path
):
    class Widgets:
        def __init__(self):
            self.values = {"mode": "parent"}

        def getAll(self):
            return dict(self.values)

        def remove(self, key):
            self.values.pop(key, None)

        def text(self, key, value):
            self.values.setdefault(key, value)

    widgets = Widgets()

    class SparkConf:
        def __init__(self):
            self.values = {"spark.fixture.mode": "parent"}

        def get(self, key, default=None):
            return self.values.get(key, default)

        def set(self, key, value):
            self.values[key] = value

        def unset(self, key):
            self.values.pop(key, None)

    spark_conf = SparkConf()
    namespace = {
        "aidputils": SimpleNamespace(widgets=widgets),
        "spark": SimpleNamespace(conf=spark_conf),
    }

    class Session:
        async def execute(self, code, **kwargs):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exec(code, namespace)
            return {"status": "ok", "outputs": [{
                "type": "stream", "name": "stdout", "text": stdout.getvalue()
            }]}

    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    manifest = tmp_path / "fixture-job" / "manifest_params.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"mode": "parent"}), encoding="utf-8")
    session = Session()

    asyncio.run(jm._seed_percent_run_validation_bindings(
        session, {"mode": "prod"}, "fixture-job"
    ))
    assert widgets.values == {"mode": "prod"}
    assert spark_conf.values == {"spark.fixture.mode": "parent"}  # no Spark bridge writes
    assert json.loads(manifest.read_text()) == {"mode": "prod"}
    asyncio.run(jm._restore_percent_run_validation_bindings(
        session, "fixture-job"
    ))
    assert widgets.values == {"mode": "parent"}
    assert spark_conf.values == {"spark.fixture.mode": "parent"}
    assert json.loads(manifest.read_text()) == {"mode": "parent"}

    # The next argument-free child still enters a reset/verify scope and sees
    # the parent's baseline rather than the preceding child's value.
    asyncio.run(jm._seed_percent_run_validation_bindings(
        session, {}, "fixture-job"
    ))
    assert widgets.values == {"mode": "parent"}
    assert spark_conf.values == {"spark.fixture.mode": "parent"}
    assert json.loads(manifest.read_text()) == {"mode": "parent"}
    asyncio.run(jm._restore_percent_run_validation_bindings(
        session, "fixture-job"
    ))


def test_nested_malformed_percent_run_fails_loud(monkeypatch):
    path = "/Workspace/migrated/child.ipynb"
    notebook = {
        "cells": [{"cell_type": "code", "source": ['%run "unterminated']}],
        "metadata": {},
    }

    monkeypatch.setattr(jm, "_notebook_content_cache", {path: json.dumps(notebook)})
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/migrated")

    result = asyncio.run(jm._inline_child_notebook(
        path,
        SimpleNamespace(cluster_id="fixture"),
        SimpleNamespace(log=lambda *args: None),
        "fixture-job",
        "",
        "",
        "",
        {},
    ))

    assert result["status"] == "error"
    assert result["kind"] == "UNRESOLVED_PERCENT_RUN"


@pytest.mark.parametrize("caller", ["percent_run", "python", "scala", "cohort"])
@pytest.mark.parametrize("failure", ["returned", "raised", "none"])
@pytest.mark.parametrize("strict", [False, True])
def test_inline_failure_stops_parent_execution_and_success_cache(
    monkeypatch, tmp_path, capsys, caller, failure, strict
):
    from test_pip_magic_gate import _Kernel, _patch_common

    sources = {
        "percent_run": "%run /Workspace/child",
        "python": 'dbutils.notebook.run("/Workspace/child", 60, {})\nPARENT_EXECUTION_MARKER = 1',
        "scala": '%scala\nval result = dbutils.notebook.run("/Workspace/child", 60, Map())\nval PARENT_EXECUTION_MARKER = 1',
        "cohort": 'dbutils.notebook.run(child_path, 60, {})\nPARENT_EXECUTION_MARKER = 1',
    }
    _patch_common(monkeypatch, tmp_path, _notebook(sources[caller]), [])
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    monkeypatch.setattr(jm, "_resolve_nbrun_child_path",
                        lambda *a: "/Workspace/test_pip/notebooks/child.ipynb")
    monkeypatch.setattr(jm, "extract_notebook_run_cohort_map", lambda *a: {
        "/Workspace/child": "items", "/Workspace/second": "items"})
    async def samples(*a, **kw):
        return {"items": "fixture"}

    monkeypatch.setattr(jm, "extract_cohort_samples", samples)
    called = []

    async def child(path, *a, **kw):
        called.append(path)
        if failure == "none":
            return {"status": "ok", "cells_ok": 1, "cells_fixed": 0}
        if failure == "raised":
            raise RuntimeError("synthetic save refused")
        return {"status": "error", "kind": "CHILD_PERSISTENCE",
                "error": "synthetic save refused"}

    monkeypatch.setattr(jm, "_inline_child_notebook", child)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/parent.ipynb", kernel, "fixture", "task", {}, run_all=True,
        dep_path_map={"/Workspace/child": "/Workspace/test_pip/notebooks/child.ipynb"}))
    if failure == "none":
        assert called
        assert result["status"] == "PASS", result
        if caller in ("python", "cohort"):
            assert jm._inlined_nb_run_children
        if caller != "percent_run":
            assert any("PARENT_EXECUTION_MARKER" in code for code in kernel.executed
                       if "base64.b64decode" not in code)
        return
    assert len(called) == 1, result
    assert result["status"] not in ("PASS", "CONVERTED_NOT_VALIDATED"), result
    assert "synthetic save refused" in json.dumps(result) + capsys.readouterr().out, result
    assert not jm._inlined_nb_run_children
    assert not any("PARENT_EXECUTION_MARKER" in code for code in kernel.executed
                   if "base64.b64decode" not in code)


@pytest.mark.parametrize("result", [None, {}, {"status": "error"}, {"status": "partial"}])
def test_inline_status_gate_rejects_non_success(result):
    assert jm._inline_child_error("/Workspace/child", result)
    assert jm._inline_child_error("/Workspace/child", {"status": "ok"}) is None


@pytest.mark.parametrize("read_failure", ["empty", "error", "partial"])
def test_unreadable_discovered_child_cannot_fall_back_to_original(monkeypatch, read_failure):
    requested = "/Workspace/migrated/Monitoring Job/notebooks/Child Notebook.ipynb"
    physical = requested.replace("Child Notebook", "Child_Notebook")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/migrated/Monitoring Job/notebooks")
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    api_calls = []

    async def original(path):
        api_calls.append(path)
        return json.dumps(_notebook("ORIGINAL_MUST_NOT_EXECUTE = True"))

    monkeypatch.setattr(jm, "download_notebook_async", original)

    class Session:
        reads = 0
        source3_paths = []

        async def execute(self, code, **kwargs):
            if "found_path = None" in code:
                data = {"count": 2, "indices": [0, 1], "resolved_path": physical, "lang": "python"}
                return {"status": "ok", "outputs": [{"type": "stream", "text": json.dumps(data)}]}
            if "src = ''.join" in code:
                self.reads += 1
                if read_failure == "partial" and self.reads == 1:
                    return {"status": "ok", "outputs": [{"type": "stream", "text": "x = 1"}]}
                return {"status": "error" if read_failure == "error" else "ok", "outputs": []}
            if "with open('" in code:
                self.source3_paths.append(code.split("with open('", 1)[1].split("')", 1)[0])
                return {"status": "ok", "outputs": [{"type": "stream", "text": '{"ok": false}'}]}
            pytest.fail("Must not execute child code or save a fallback artifact")

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        requested, session, SimpleNamespace(log=lambda *a: None), "fixture", "", "", "", {}))
    assert result["status"] == "error"
    assert result["kind"] == "CHILD_SOURCE_UNAVAILABLE"
    assert result["resolved_path"] == physical
    assert session.source3_paths[0] == physical
    assert not api_calls
    assert not jm._notebook_content_cache


@pytest.mark.parametrize("provenance", ["dep_map", "migration_cache", "unrelated", "none"])
@pytest.mark.parametrize("reference", ["source", "target", "target_no_extension", "corrected_target"])
@pytest.mark.parametrize("readable", [False, True])
def test_child_artifact_provenance_controls_original_fallback(
    monkeypatch, provenance, reference, readable
):
    root = "/Workspace/migrated/Monitoring Job/notebooks"
    source = "/Workspace/Users/fixture/Child_Notebook.ipynb"
    physical = root + "/Users/fixture/Child_Notebook.ipynb"
    if reference == "corrected_target":
        physical = root + "/corrected/Physical_Child.ipynb"
    requested = {"source": source, "target": physical,
                 "target_no_extension": physical[:-6], "corrected_target": physical}[reference]
    monkeypatch.setattr(jm, "MIGRATED_BASE", root)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    cache = {"Users/fixture/Child_Notebook.ipynb": physical} if provenance == "migration_cache" else {}
    dep_map = {source: physical} if provenance == "dep_map" else {}
    if provenance == "unrelated":
        # Same basename is not evidence that THIS child has been migrated.
        cache = {"Users/other/Child_Notebook.ipynb": root + "/Users/other/Child_Notebook.ipynb"}
        dep_map = {"/Workspace/Users/other/Child_Notebook.ipynb": cache[next(iter(cache))]}
    monkeypatch.setattr(jm, "_migration_cache", cache)
    async def no_sleep(*args):
        pass

    monkeypatch.setattr(jm.asyncio, "sleep", no_sleep)
    api_calls = []

    async def original(path):
        api_calls.append(path)
        return json.dumps(_notebook(""))

    monkeypatch.setattr(jm, "download_notebook_async", original)

    class Session:
        async def execute(self, code, **kwargs):
            if "found_path = None" in code:
                data = {"error": "not_found"}
            elif "with open('" in code:
                path = code.split("with open('", 1)[1].split("')", 1)[0]
                data = {"ok": True, "cells": [], "lang": "python"} if readable and path == physical else {"ok": False}
            else:
                pytest.fail("No executable fixture cells or saves expected")
            return {"status": "ok", "outputs": [{"type": "stream", "text": json.dumps(data)}]}

    result = asyncio.run(jm._inline_child_notebook(
        requested, Session(), SimpleNamespace(log=lambda *a: None), "fixture", "", "", "", dep_map))
    if not readable and provenance in ("dep_map", "migration_cache"):
        assert result["status"] == "error"
        assert result["kind"] == "CHILD_SOURCE_UNAVAILABLE"
        assert result["resolved_path"] == physical
        assert not api_calls
        assert not jm._notebook_content_cache
    else:
        assert result["status"] == "ok", result
        assert bool(api_calls) is not readable


@pytest.mark.parametrize("provenance", ["dep_map", "migration_cache"])
def test_known_unreadable_child_fails_parent_without_original_download(
    monkeypatch, tmp_path, capsys, provenance
):
    from test_pip_magic_gate import _Kernel, _patch_common

    source = "/Workspace/Users/fixture/child.ipynb"
    physical = "/Workspace/test_pip/notebooks/Users/fixture/child.ipynb"
    _patch_common(monkeypatch, tmp_path, _notebook(f"%run {source}"), [])
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_migration_cache", {"Users/fixture/child.ipynb": physical}
                        if provenance == "migration_cache" else {})
    api_calls = []

    async def original(path):
        api_calls.append(path)
        if path == physical:
            return b""
        return json.dumps(_notebook("ORIGINAL_MUST_NOT_EXECUTE = True"))

    monkeypatch.setattr(jm, "download_notebook_async", original)

    # Scope preparation reads mapped children asynchronously before execution. Model the
    # unreadable child on both transports; the common mock returns the parent
    # for every path, which would incorrectly make this child self-recursive.
    monkeypatch.setattr(jm, "download_notebook", lambda path:
                        json.dumps(_notebook(f"%run {source}")).encode()
                        if path == "/Workspace/parent.ipynb" else b"")

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            if "found_path = None" in code:
                data = {"error": "not_found"}
            elif "with open('" in code:
                data = {"ok": False}
            else:
                return await super().execute(code, **kwargs)
            return {"status": "ok", "outputs": [{"type": "stream", "text": json.dumps(data)}]}

    async def run_parent():
        return await jm.process_notebook(
            "/Workspace/parent.ipynb", Kernel(), "fixture", "task", {}, run_all=True,
            dep_path_map={source: physical} if provenance == "dep_map" else {})

    if provenance == "dep_map":
        result = asyncio.run(run_parent())
        assert result["status"] == "FAIL"
        assert result["kind"] == "CHILD_SOURCE_UNAVAILABLE"
        assert physical in result["error"]
        assert api_calls == [physical]
    else:
        result = asyncio.run(run_parent())
        assert result["status"] != "PASS", result
        output = json.dumps(result) + capsys.readouterr().out
        assert f"CHILD FAILED: Known migrated child could not be read: {physical}" in output, output
        assert not api_calls
    assert source not in api_calls
    assert physical not in jm._notebook_content_cache
    assert source not in jm._notebook_content_cache
