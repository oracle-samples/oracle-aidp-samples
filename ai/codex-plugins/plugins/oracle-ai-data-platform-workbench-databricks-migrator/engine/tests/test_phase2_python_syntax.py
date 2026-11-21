"""Phase 2 normalizes directives; customer body indentation is preserved."""
import ast
import asyncio
import contextlib
import io
import json

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, _saved_notebook, jm
from test_child_exit_conversion import _ExitKernel
from slack_ast_neutralize import _strip_magics
from slack_token_redaction import build_redactor, SlackRedactionError


def _promote(source):
    promotion = jm._promote_leading_magic(source)
    return promotion.source, bool(promotion.reason)


BODY = "from math import sqrt\nfrom math import floor\n"
BLOCKS = (
    "def f():\n    return sqrt(4)\n"
    "class C:\n    value = 1\n"
    "for x in range(2):\n    print(x)\n"
    "if True:\n    value = 2\n"
)


@pytest.mark.parametrize("prefix", [" ", "   ", "\t"])
def test_directive_normalization_does_not_touch_body(prefix):
    source = prefix + "%python\n" + BODY + BLOCKS
    fixed, changed = _promote(source)
    assert changed
    assert fixed == "%python\n" + BODY + BLOCKS
    assert _promote(fixed) == (fixed, False)


@pytest.mark.parametrize("directive", ["%python\n", "", "# annotation\n %python\n"])
@pytest.mark.parametrize("indent", [" ", "  ", "\t"])
def test_initial_import_indentation_is_preserved(directive, indent):
    source = directive + indent + BODY + BLOCKS
    fixed, _ = _promote(source)
    assert fixed.endswith(indent + BODY + BLOCKS)
    with pytest.raises(IndentationError):
        ast.parse(_strip_magics(fixed))
    assert _promote(fixed) == (fixed, False)


@pytest.mark.parametrize("source", [
    " value = 1\n",                         # not an import
    "value = 1\n import math\n",           # not initial
    " import math\n value = 1\n",          # trial still invalid
    " from math import (\n    sqrt,\n)\n", # multiline, ambiguous
    " import math; print('x')\n",           # multiple statements
    "if True:\nimport math\n",             # missing block indentation
    "def f():\n    import math\n  return 1\n", # invalid unindent
])
def test_genuine_body_errors_are_not_repaired(source):
    fixed, changed = _promote(source)
    assert fixed == source
    assert not changed
    with pytest.raises(SyntaxError):
        ast.parse(_strip_magics(fixed))


@pytest.mark.parametrize("source,default", [
    ("%scala\n import java.time.LocalDate", "python"),
    ("%sql\n SELECT 1", "python"),
    ("val x = 1", "scala"),
    (" import java.time.LocalDate", "scala"),
    ("%python\n" + BODY + BLOCKS, "scala"),
    ("# %python\nval x = 1", "scala"),
    ('text = """\n %python\n"""\n' + BLOCKS, "python"),
    ("%%bash\necho hello", "python"),
    ("%md-sandbox\nExample:\n!pip install demo_pkg", "python"),
    ("# annotation\n%md-sandbox\n<div>Example</div>", "python"),
    ('DOC = "%md-sandbox"\nvalue = 1', "python"),
    ("%timeit sum(range(10))", "python"),
    ("!echo hello\nvalue = 1", "python"),
])
def test_other_languages_valid_blocks_and_inert_magics_are_unchanged(source, default):
    # Upstream now promotes comment-prefixed markdown directives. Preserve
    # that behavior while checking the whitespace fix adds no other changes.
    if source.startswith("# annotation\n%md-sandbox"):
        assert _promote(source) == (
            "%md-sandbox\n# annotation\n<div>Example</div>", True)
    else:
        assert _promote(source) == (source, False)


def test_directive_arguments_are_preserved():
    assert _promote(" \t%run ./child arg=value\n") == (
        "%run ./child arg=value\n", True)


def _notebook(source, default):
    return {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                       "outputs": [], "execution_count": None}],
            "metadata": {"language_info": {"name": default}},
            "nbformat": 4, "nbformat_minor": 5}


class _ChildKernel(_ExitKernel):
    async def execute(self, code, **kwargs):
        if "__AIDP_CHILD_NB__" in code:
            self.executed.append(code)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                exec(code, {})  # read the actual pre-normalization disk copy
            return {"status": "ok", "outputs": [{"type": "stream", "text": out.getvalue()}]}
        return await super().execute(code, **kwargs)


def _drive(monkeypatch, tmp_path, source, default, entry, *, code_only=False):
    notebook = _notebook(source, default)
    export = tmp_path / "exported.ipynb"
    export.write_text(json.dumps(notebook))
    export_bytes = export.read_bytes()
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    # Exercise normalization/redaction independently of the pre-existing
    # notebook-default migration routing, which this narrow fix does not alter.
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", default in ("scala", "sql"))
    if entry == "main":
        kernel = _Kernel()
        result = asyncio.run(jm.process_notebook(
            "/Workspace/fixture.ipynb", kernel, "test_pip", "01_task", {},
            run_all=not code_only))
        saved = _saved_notebook(kernel)
    else:
        # Inline children are Phase-2 migrated counterparts, never raw exports.
        migrated = tmp_path / "migrated.ipynb"
        migrated.write_bytes(export_bytes)
        monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
        monkeypatch.setattr(jm, "_notebook_content_cache", {str(migrated): export_bytes.decode()})
        kernel = _ChildKernel(fire=False)
        logs = []

        class Log:
            def log(self, message):
                logs.append(message)

        result = asyncio.run(jm._inline_child_notebook(
            str(migrated), kernel, Log(), "test_pip", "", "", "", {}))
        saved = json.loads(migrated.read_text())
    assert export.read_bytes() == export_bytes
    return result, saved, kernel


@pytest.mark.parametrize("entry", ["main", "child"])
@pytest.mark.parametrize("default", ["python", "scala", "sql"])
@pytest.mark.parametrize("source", [
    " %python\n" + BODY + BLOCKS,
    "\t%python\n" + BODY + BLOCKS,
    "# annotation\n %python\n" + BODY + BLOCKS,
    "%python\n" + BODY + BLOCKS,
])
def test_real_phase2_paths_normalize_execute_and_save_directives(
        monkeypatch, tmp_path, entry, default, source):
    result, saved, kernel = _drive(monkeypatch, tmp_path, source, default, entry)
    assert result["status"] in ("PASS", "ok"), result
    cells = ["".join(c["source"]) for c in saved["cells"]]
    cell = next(c for c in cells if BLOCKS.rstrip() in c)
    assert cell.startswith("%python\n")
    assert BODY in cell
    ast.parse(jm._python_cell_ast_source(cell))
    jm._assert_final_notebook_cells(saved["cells"], default)
    executed = [c for c in kernel.executed if "from math import sqrt" in c
                and "base64.b64decode" not in c]
    assert executed, "repaired cell never reached the production execution loop"
    assert all(c.startswith("%python\n") for c in executed)


@pytest.mark.parametrize("entry", ["main", "child"])
def test_artifact_finalizer_normalizes_directive(entry):
    source = " %python\n" + BODY + BLOCKS
    if entry == "child":
        fixed = jm._finalize_child_artifact_sources([source], "scala")[0]
    else:
        fixed = "".join(jm._finalize_leading_cell_magics(
            [{"cell_type": "code", "source": [source]}])[0]["source"])
    assert fixed == "%python\n" + BODY + BLOCKS
    ast.parse(_strip_magics(fixed))


def test_code_only_phase2_saves_corrected_python(monkeypatch, tmp_path):
    result, saved, _ = _drive(
        monkeypatch, tmp_path, " %python\n" + BODY, "scala", "main", code_only=True)
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    cell = next("".join(c["source"]) for c in saved["cells"]
                if BODY.rstrip() in "".join(c["source"]))
    assert cell.startswith("%python\n")
    assert "original indented import" not in cell


@pytest.mark.parametrize("entry", ["main", "child"])
def test_genuine_body_indentation_keeps_redaction_fail_closed(entry):
    source = "%python\n from slack_sdk import WebClient\nclient = WebClient(token='fixture-secret')"
    fixed, changed = _promote(source)
    assert not changed and fixed == source
    with pytest.raises(IndentationError):
        ast.parse(_strip_magics(fixed))
    raw = [fixed] if entry == "main" else fixed
    with pytest.raises(SlackRedactionError):
        build_redactor([{"cell_type": "code", "source": raw}])


@pytest.mark.parametrize("entry", ["main", "child"])
def test_bare_python_initial_import_is_not_repaired(entry):
    source = " " + BODY
    assert _promote(source) == (source, False)
    with pytest.raises(IndentationError):
        ast.parse(_strip_magics(source))


@pytest.mark.parametrize("default,source", [
    ("scala", " import java.time.LocalDate"),
    ("", "// Databricks notebook source\n import java.time.LocalDate"),
])
def test_native_default_body_is_unchanged(monkeypatch, tmp_path, default, source):
    result, saved, _ = _drive(monkeypatch, tmp_path, source, default, "main")
    assert result["status"] == "PASS", result
    cell = next("".join(c["source"]) for c in saved["cells"]
                if "import java.time.LocalDate" in "".join(c["source"]))
    assert cell.startswith("%scala\n")
    assert "\n import java.time.LocalDate" in cell
    assert "original indented import" not in cell


@pytest.mark.parametrize("entry", ["main", "child"])
@pytest.mark.parametrize("prefix", [" ", "  ", "\t"])
def test_leading_python_magic_no_longer_blocks_slack_redaction(
        monkeypatch, tmp_path, entry, prefix):
    source = (prefix + "%python\nfrom slack_sdk import WebClient\n"
              "client = WebClient(token='fixture-secret')\n")
    with pytest.raises(IndentationError):
        ast.parse(_strip_magics(source))
    with pytest.raises(SlackRedactionError):
        build_redactor([{"cell_type": "code", "source": [source]}])
    result, saved, kernel = _drive(monkeypatch, tmp_path, source, "scala", entry)
    assert result["status"] in ("PASS", "ok"), result
    assert "fixture-secret" not in json.dumps(saved)
    cell = next("".join(c["source"]) for c in saved["cells"]
                if "from slack_sdk import WebClient" in "".join(c["source"]))
    # Redaction may prepend its existing disclosure comment. The directive
    # itself must have no indentation, and the parser must accept the result.
    first_statement = next(line for line in cell.splitlines()
                           if line.strip() and not line.startswith("#"))
    assert first_statement == "%python"
    ast.parse(_strip_magics(cell))
    if entry == "child":
        assert any("__AIDP_CHILD_NB__" in code for code in kernel.executed)
