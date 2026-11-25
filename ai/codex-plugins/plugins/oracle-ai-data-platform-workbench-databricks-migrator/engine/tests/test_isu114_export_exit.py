"""REF-114: SOURCE indentation must survive export, analysis and migration.

Incident run aaaaaaaa-0000-4000-8000-000000000205 (2026-09-18):
a Python guard in a Scala notebook was flattened, then repaired after analysis.
The stale unconditional-exit plan incorrectly skipped the remaining cells.
"""
import ast
import asyncio
import json

import pytest

from dbx_export.dbx_client import DbxClient
from test_conditional_exit_below_execute import (
    _Kernel, _patch_common, _saved_sources, cell_analyzer, jm,
)
from test_child_exit_conversion import _ExitKernel, _run as run_child


def exported_notebook(language, body, downstream="SENTINEL_AFTER_GUARD = 1"):
    prefix = {"python": "#", "scala": "//", "sql": "--"}[language]
    cells = ["%python\n" + body, "%python\n" + downstream]
    source = (f"{prefix} Databricks notebook source\n" +
              f"\n{prefix} COMMAND ----------\n".join(
                  "\n".join(f"{prefix} MAGIC {line}" for line in cell.splitlines())
                  for cell in cells)).encode()
    converted = DbxClient._dbx_source_to_ipynb(source, language)
    return source, json.loads(converted)


@pytest.mark.parametrize("language", ["python", "scala", "sql"])
@pytest.mark.parametrize("indent", ["  ", "    ", "\t"])
def test_magic_body_preserves_nested_indentation_and_rejects_loss(language, indent):
    body = ("if enabled:\n" + indent + "if missing:\n" +
            indent * 2 + "dbutils.notebook.exit('no features')")
    source, notebook = exported_notebook(language, body)
    actual = "".join(notebook["cells"][0]["source"])
    assert actual == "%python\n" + body + "\n"
    ast.parse(actual.split("\n", 1)[1])  # Independent oracle, not the export parser.
    assert DbxClient._validate_conversion(source, json.dumps(notebook).encode(), language) == (True, "OK")
    notebook["cells"][0]["source"] = ["\n".join(line.lstrip() for line in actual.splitlines())]
    valid, detail = DbxClient._validate_conversion(source, json.dumps(notebook).encode(), language)
    assert not valid and "text mismatch" in detail


class GuardKernel(_Kernel):
    def __init__(self, fires):
        super().__init__()
        self.fires = fires

    async def execute(self, code, **kwargs):
        if "notebook.exit(" in code and not code.lstrip().startswith("#"):
            self.executed.append(code)
            if self.fires:
                return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": "__AIDP_NOTEBOOK_EXIT__ no features\n"}]}
            return {"status": "ok", "outputs": []}
        return await super().execute(code, **kwargs)


def stub_model(monkeypatch, repair=False):
    prompts = []
    async def migrate_cell(_system, prompt, **kwargs):
        prompts.append(prompt)
        source = prompt.rsplit("```", 2)[1].split("\n", 1)[1]
        if repair:
            source = repair_guard(source)
        return source, {"input": 0, "output": 0}
    monkeypatch.setattr(jm, "call_opus_with_tools", migrate_cell)
    return prompts


def repair_guard(source):
    for qualifier in ("dbutils", "aidputils", "oidlUtils"):
        source = source.replace(f"\n{qualifier}.notebook.exit(", "\n    aidputils.notebook.exit(")
    return source


@pytest.mark.parametrize("language", ["python", "scala", "sql"])
@pytest.mark.parametrize("fires", [False, True])
@pytest.mark.parametrize("repair_after_analysis", [False, True])
def test_export_to_real_analyzer_and_cell_loop(monkeypatch, tmp_path, language, fires, repair_after_analysis):
    body = "if feature_missing:\n    dbutils.notebook.exit('no features')"
    _, notebook = exported_notebook(language, body)
    if repair_after_analysis:
        # A pre-fix exported notebook already on AIDP; migration repairs it.
        notebook["cells"][0]["source"] = ["%python\n" + body.replace("    ", "")]
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: repair_after_analysis)

    # Scala-default notebooks enter the model branch even for explicit Python
    # cells. Echo its source, repairing the incident input only when requested.
    stub_model(monkeypatch, repair_after_analysis)
    if repair_after_analysis:
        # Only the malformed guard requires the model; downstream is native.
        monkeypatch.setattr(jm, "_has_databricks_markers", lambda code, *a, **k: "notebook.exit(" in code)
        if language == "sql":
            # SQL-default cells take the deterministic path, not the model.
            original_preprocess = jm._preprocess_cell_source
            def repair_preprocess(source, *args, **kwargs):
                return repair_guard(original_preprocess(source, *args, **kwargs))
            monkeypatch.setattr(jm, "_preprocess_cell_source", repair_preprocess)
    kernel = GuardKernel(fires)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    assert result["status"] == ("PASS_CONDITIONAL_EXIT_COVERAGE" if fires else "PASS"), result
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    assert ("SENTINEL_AFTER_GUARD = 1" in executed) is not fires
    saved, saved_nb = _saved_sources(kernel)
    assert len(saved_nb["cells"]) == 2
    assert "SENTINEL_AFTER_GUARD = 1" in saved
    assert "    aidputils.notebook.exit(" in saved


@pytest.mark.parametrize("language", ["python", "scala", "sql"])
def test_genuine_unconditional_exit_still_preserves_dead_cells(monkeypatch, tmp_path, language):
    _, notebook = exported_notebook(language, "dbutils.notebook.exit('done')")
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    stub_model(monkeypatch)
    kernel = GuardKernel(True)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    assert result["status"] == "PASS", result
    assert not any("SENTINEL_AFTER_GUARD = 1" in c for c in kernel.executed if "base64.b64decode" not in c)
    assert "SENTINEL_AFTER_GUARD = 1" in _saved_sources(kernel)[0]


@pytest.mark.parametrize("qualifier", ["dbutils", "aidputils", "oidlUtils"])
@pytest.mark.parametrize("source, expected", [
    ('if (missing) {\nQUAL.notebook.exit("done")\n}', False),
    ('if (ready) {\nif (missing) {\nQUAL.notebook.exit("done")\n}\n}', False),
    ('if (missing) {\n// } ignored comment\nQUAL.notebook.exit("done")\n}', False),
    ('// { ignored comment\nQUAL.notebook.exit("done")', True),
    ('if (ready) {\nprintln("ready")\n}\nQUAL.notebook.exit("done")', True),
    ('if missing:\n    QUAL.notebook.exit("done")', False),
    ('// QUAL.notebook.exit("comment only")', False),
    ('if (missing) { QUAL.notebook.exit("done") }', False),
])
def test_runtime_and_plan_classifiers_agree(qualifier, source, expected):
    source = source.replace("QUAL", qualifier)
    assert cell_analyzer._is_unconditional_notebook_exit(source) is expected
    assert jm._exit_is_unconditional(source) is expected


@pytest.mark.parametrize("qualifier", ["dbutils", "oidlUtils"])
@pytest.mark.parametrize("shape", [
    "conditional", "nested_with_comment", "unconditional",
    "quoted_open", "escaped_open", "regex_open", "docstring_open",
    "comment_open", "quoted_close_in_guard", "comment_close_in_guard",
])
@pytest.mark.parametrize("fires", [False, True])
def test_scala_exit_through_real_child_loop(monkeypatch, tmp_path, qualifier, shape, fires):
    exit_call = f'{qualifier}.notebook.exit("done")'
    body = {
        "conditional": f"if (missing) {{\n{exit_call}\n}}",
        "nested_with_comment": f"if (ready) {{\nif (missing) {{\n// }} ignored comment\n{exit_call}\n}}\n}}",
        "unconditional": f'if (ready) {{\nprintln("ready")\n}}\n{exit_call}',
        "quoted_open": f'val text = "{{"\n{exit_call}',
        "escaped_open": 'val text = "escaped \\\" {"\n' + exit_call,
        "regex_open": 'val pattern = "\\\\{".r\n' + exit_call,
        "docstring_open": f'val text = """{{"""\n{exit_call}',
        "comment_open": f'val value = 1 // {{\n{exit_call}',
        "quoted_close_in_guard": f'if (missing) {{\nval text = "}}"\n{exit_call}\n}}',
        "comment_close_in_guard": f'if (missing) {{\nval value = 1 // }}\n{exit_call}\n}}',
    }[shape]
    tail = "%scala\nval SENTINEL_SCALA_TAIL = 1"
    original_execute = _ExitKernel.execute
    async def scala_execute(self, code, **kwargs):
        if fires and code.startswith("%scala\n") and 'notebook.exit("done")' in code:
            self.executed.append(code)
            # Native Scala signals NotebookExit, not the Python wrapper marker.
            return {"status": "error", "outputs": [
                {"type": "error", "ename": "NotebookExit", "evalue": "done",
                 "traceback": ["NotebookExit: done"]}]}
        return await original_execute(self, code, **kwargs)
    monkeypatch.setattr(_ExitKernel, "execute", scala_execute)
    result, saved, kernel, installs, logs = run_child(
        monkeypatch, tmp_path, {"child": ["%scala\n" + body, tail]}, fire=False)
    conditional = shape in {
        "conditional", "nested_with_comment", "quoted_close_in_guard", "comment_close_in_guard",
    }
    assert result["status"] == "ok", result
    assert result["did_exit"] is fires, (kernel.executed, logs)
    assert bool(result.get("exit_coverage")) is (conditional and fires)
    assert any("NOT runtime-validated" in msg for msg in logs) is (conditional and fires)
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    # Existing API translation may change the qualifier; braces must reach the
    # real execution loop intact, not just a Python-shaped language fixture.
    assert 'notebook.exit("done")' in executed
    if conditional or shape == "unconditional":
        assert "if (" in executed
    assert (tail in executed) is not fires
    assert len(saved["child"]) == 2
    assert saved["child"][1] == tail
    assert installs == []


@pytest.mark.parametrize("qualifier", ["dbutils", "aidputils", "oidlUtils"])
@pytest.mark.parametrize("marker", [False, True])
@pytest.mark.parametrize("brace", ["{", "}"])
@pytest.mark.parametrize("prefix", [
    "text = 'BRACE'",
    'text = "BRACE"',
    r'text = "escaped \" BRACE"',
    r'pattern = r"\BRACE"',
    '"""usage BRACE"""',
    'value = 1 # BRACE',
    'text = "# // BRACE" # ignored "BRACE',
], ids=["single", "double", "escaped", "regex", "docstring", "comment", "quoted-comment"])
def test_quoted_braces_do_not_revive_unconditional_tail(
        monkeypatch, tmp_path, qualifier, marker, brace, prefix):
    """PR #210 owner repro: quoted braces must not change exit coverage/plans."""
    body = prefix.replace("BRACE", brace) + f"\n{qualifier}.notebook.exit('done')"
    ast.parse(body)  # The fixture is valid Python, including the escaped quotes.
    _, notebook = exported_notebook("python", body)
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    kernel = GuardKernel(marker)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    assert result["status"] == "PASS", result
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    assert "notebook.exit(" in executed
    assert "SENTINEL_AFTER_GUARD = 1" not in executed
    report = next(text for path, text in kernel.files.items() if path.endswith("test_report.md"))
    assert "Cell 1 - SKIPPED (unreachable — after unconditional notebook.exit())" in report
    saved, saved_nb = _saved_sources(kernel)
    assert len(saved_nb["cells"]) == 2
    assert "SENTINEL_AFTER_GUARD = 1" in saved


@pytest.mark.parametrize("qualifier", ["dbutils", "aidputils"])
@pytest.mark.parametrize("direct", [False, True])
def test_top_level_exit_without_marker_uses_finalized_plan(monkeypatch, tmp_path, qualifier, direct):
    _, notebook = exported_notebook("python", f"{qualifier}.notebook.exit('done')")
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", direct)
    kernel = GuardKernel(False)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    assert result["status"] == "PASS", result
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    # Diagnostic direct-execute intentionally has no static skip plan: without
    # a runtime marker it executes the tail. Normal migration must still skip.
    assert ("SENTINEL_AFTER_GUARD = 1" in executed) is direct
    saved, saved_nb = _saved_sources(kernel)
    assert len(saved_nb["cells"]) == 2
    assert "SENTINEL_AFTER_GUARD = 1" in saved


def test_native_alias_plan_retains_originating_exit():
    cells = [{"cell_type": "code", "source": [source]} for source in (
        "aidputils.notebook.exit('done')", "SENTINEL_AFTER_GUARD = 1")]
    plans = asyncio.run(cell_analyzer.analyze_notebook_cells(cells, None, None, "", ""))
    assert plans[1]["action"] == "skip_after_exit"
    assert plans[1]["exit_cell_index"] == 0


@pytest.mark.parametrize("qualifier", ["dbutils", "aidputils", "oidlUtils"])
@pytest.mark.parametrize("spacers", [False, True])
@pytest.mark.parametrize("later_source, cell_type, marker, skip_tail, coverage", [
    ("%python\nQUAL.notebook.exit('all done')", "code", False, True, False),
    ("%python\nQUAL.notebook.exit('all done')", "code", True, True, False),
    ("%python\nif later_missing:\n    QUAL.notebook.exit('all done')", "code", False, False, False),
    ("%python\nif later_missing:\n    QUAL.notebook.exit('all done')", "code", True, True, True),
    ('%scala\nQUAL.notebook.exit("all done")', "code", False, True, False),
    ('%scala\nif (later_missing) {\nQUAL.notebook.exit("all done")\n}', "code", False, False, False),
    ("QUAL.notebook.exit('all done')", "markdown", False, False, False),
    ("QUAL.notebook.exit('all done')", "raw", False, False, False),
], ids=["python-markerless", "python-marker", "python-guard-not-fired", "python-guard-fired",
        "scala-markerless", "scala-guard-not-fired",
        "markdown", "raw"])
def test_revived_region_rechecks_all_finalized_exits(
        monkeypatch, tmp_path, qualifier, spacers, later_source, cell_type, marker,
        skip_tail, coverage):
    """B2: repair the first skip origin, then honor the next finalized exit."""
    _, notebook = exported_notebook(
        "python", "if feature_missing:\ndbutils.notebook.exit('no features')",
        downstream="SENTINEL_DEAD_TAIL = 1")
    notebook["cells"].insert(1, dict(
        notebook["cells"][1], cell_type=cell_type,
        source=[later_source.replace("QUAL", qualifier)]))
    if spacers:
        template = notebook["cells"][-1]
        notebook["cells"][1:1] = [
            dict(template, cell_type="markdown", source=["aidputils.notebook.exit('prose')"]),
            dict(template, source=[]),
            dict(template, source=["SENTINEL_BEFORE_SECOND = 1"]),
        ]
        notebook["cells"].insert(-1, dict(template, source=["SENTINEL_AFTER_SECOND = 1"]))
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    monkeypatch.setattr(jm, "_has_databricks_markers",
                        lambda code, *a, **k: "notebook.exit(" in code)

    async def migrate_cell(_system, prompt, **kwargs):
        source = prompt.rsplit("```", 2)[1].split("\n", 1)[1]
        if "feature_missing" in source:
            source = repair_guard(source)
        return source, {"input": 0, "output": 0}
    monkeypatch.setattr(jm, "call_opus_with_tools", migrate_cell)
    kernel = GuardKernel(False)
    execute = kernel.execute
    async def later_exit_only(code, **kwargs):
        kernel.fires = marker and "all done" in code
        return await execute(code, **kwargs)
    monkeypatch.setattr(kernel, "execute", later_exit_only)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    assert ("all done" in executed) is (cell_type == "code")
    assert ("SENTINEL_DEAD_TAIL = 1" in executed) is not skip_tail
    if spacers:
        assert "SENTINEL_BEFORE_SECOND = 1" in executed
        assert ("SENTINEL_AFTER_SECOND = 1" in executed) is not skip_tail
    assert result["status"] == ("PASS_CONDITIONAL_EXIT_COVERAGE" if coverage else "PASS"), result
    saved, saved_nb = _saved_sources(kernel)
    assert len(saved_nb["cells"]) == len(notebook["cells"])
    assert "SENTINEL_DEAD_TAIL = 1" in saved


def test_fired_repaired_guard_converts_past_unexecuted_later_exit(monkeypatch, tmp_path):
    _, notebook = exported_notebook(
        "python", "if feature_missing:\ndbutils.notebook.exit('no features')")
    notebook["cells"].insert(1, dict(
        notebook["cells"][1], source=["%python\naidputils.notebook.exit('all done')"]))
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: True)
    stub_model(monkeypatch, repair=True)
    convert = jm._convert_unexecuted_exit_tail
    converted = []
    def record_conversion(source, *args, **kwargs):
        converted.append(source)
        return convert(source, *args, **kwargs)
    monkeypatch.setattr(jm, "_convert_unexecuted_exit_tail", record_conversion)
    kernel = GuardKernel(True)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert len(converted) == 2
    assert "notebook.exit('all done')" in converted[0]
    assert "SENTINEL_AFTER_GUARD = 1" in converted[1]
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    assert "notebook.exit('all done')" not in executed
    assert "SENTINEL_AFTER_GUARD = 1" not in executed
    assert len(_saved_sources(kernel)[1]["cells"]) == 3


def test_repaired_guard_replans_child_and_model_context(monkeypatch, tmp_path):
    _, notebook = exported_notebook("python", "if missing:\ndbutils.notebook.exit('done')")
    child_cell = dict(notebook["cells"][1], source=["%run /Workspace/isu114/child"])
    notebook["cells"].insert(1, child_cell)
    real_analyze = cell_analyzer.analyze_notebook_cells
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", real_analyze)
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: True)
    prompts = stub_model(monkeypatch, repair=True)
    inline_calls = []
    async def inline_child(child_path, *a, **k):
        inline_calls.append(child_path)
        return {"status": "ok", "did_exit": False, "cells_ok": 1, "cells_fixed": 0}
    monkeypatch.setattr(jm, "_inline_child_notebook", inline_child)
    kernel = GuardKernel(False)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/isu114/source.ipynb", kernel, "isu114", "task", {}))
    assert result["status"] == "PASS", result
    assert len(inline_calls) == 1
    assert any("SENTINEL_AFTER_GUARD = 1" in c for c in kernel.executed if "base64.b64decode" not in c)
    assert len(prompts) >= 2  # Guard repair plus the downstream migration.
    assert all("skip_after_exit" not in p and "unreachable" not in p for p in prompts)
    assert len(_saved_sources(kernel)[1]["cells"]) == 3
