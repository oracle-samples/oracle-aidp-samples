"""REF-176: RU's original syntax error is in a separate cell after an exit.

The finalized exit cell must parse in full; a preserved dead tail is advisory.
Exercise parent code-only/execution and inline-child save/report boundaries.
"""
import asyncio
import copy

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, _saved_notebook, jm
from test_child_exit_conversion import _run as run_child


INVALID = "createTable(frame.select('customer_id'),,config['db'],'overwrite')"


def cell(source, cell_type="code"):
    return {"cell_type": cell_type, "source": [source], "metadata": {},
            "outputs": [], "execution_count": None}


@pytest.mark.parametrize("alias", ["dbutils", "aidputils", "oidlUtils"])
@pytest.mark.parametrize("prefix", ["", "%python\n", "%pyspark\n"])
def test_valid_exit_preserves_invalid_separate_tail(alias, prefix):
    cells = [cell(prefix + f"{alias}.notebook.exit(1)"), cell(INVALID)]
    original = copy.deepcopy(cells)
    notes = []
    jm._assert_final_notebook_cells(cells, log_fn=notes.append)
    assert cells == original
    assert any("ADVISORY: cell 2" in note and "line 1" in note for note in notes)
    assert any("unconditional exit in cell 1" in note for note in notes)


@pytest.mark.parametrize("sources", [
    [INVALID, "aidputils.notebook.exit(1)"],
    ["return", "aidputils.notebook.exit(1)", INVALID],
    ["aidputils.notebook.exit(1)\n" + INVALID],
    ["aidputils.notebook.exit(1)\nif True:"],
    ["aidputils.notebook.exit(1)\nreturn"],
    ["aidputils.notebook.exit(1)\nbreak"],
])
def test_before_exit_and_entire_exit_cell_still_block(sources):
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        jm._assert_final_notebook_cells([cell(s) for s in sources])


@pytest.mark.parametrize("remainder", [
    "\n" + INVALID,
    "\n    " + INVALID,
])
def test_conditional_exit_does_not_stop_validation_within_same_cell(remainder):
    source = "if stop:\n    aidputils.notebook.exit(1)" + remainder
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        jm._assert_final_notebook_cells([cell(source)])


@pytest.mark.parametrize("source, cell_type", [
    ("if stop:\n    aidputils.notebook.exit(1)", "code"),
    ("def helper():\n    aidputils.notebook.exit(1)", "code"),
    ("try:\n    aidputils.notebook.exit(1)\nexcept Exception:\n    pass", "code"),
    ('doc = """\naidputils.notebook.exit(1)\n"""', "code"),
    ("# aidputils.notebook.exit(1)", "code"),
    ("aidputils.notebook.exit(1)", "markdown"),
    ("aidputils.notebook.exit(1)", "raw"),
    ("%scala\ndbutils.notebook.exit(1)", "code"),
    ("unknown.notebook.exit(1)", "code"),
])
def test_conditional_inert_or_uncertain_exit_does_not_exempt_tail(source, cell_type):
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        jm._assert_final_notebook_cells([cell(source, cell_type), cell(INVALID)])


def test_finalized_guard_decides_boundary_instead_of_original_plan():
    final = [cell("if stop:\n    aidputils.notebook.exit(1)"), cell(INVALID)]
    with pytest.raises(jm.FinalArtifactValidationError):
        jm._assert_final_notebook_cells(final)


def test_displaced_magic_in_unreachable_tail_is_advisory():
    tail = "value = 1\n%sql\nSELECT 1"
    cells = [cell("aidputils.notebook.exit(1)"), cell(tail)]
    notes = []
    jm._assert_final_notebook_cells(cells, log_fn=notes.append)
    assert any("ADVISORY" in note and "%sql sits below code" in note for note in notes)
    assert "".join(cells[-1]["source"]) == tail


def test_allowed_invalid_source_cannot_establish_exit():
    source = "aidputils.notebook.exit(1)\n" + INVALID
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        jm._assert_final_notebook_cells(
            [cell(source), cell(INVALID)], allowed_invalid_sources={source})


@pytest.mark.parametrize("default", ["python", "scala", "sql"])
def test_explicit_python_exit_in_any_notebook_default(default):
    notes = []
    jm._assert_final_notebook_cells(
        [cell("%python\nawait prepare()\naidputils.notebook.exit(1)"),
         cell("%python\n" + INVALID)], default, log_fn=notes.append)
    assert any("syntax ADVISORY: cell 2" in note for note in notes)


def test_safety_contract_still_checks_tail(monkeypatch):
    cells = [cell("aidputils.notebook.exit(1)"), cell(INVALID)]
    inspected = []
    def refuse(all_cells, native_lang):
        inspected.extend(all_cells)
        raise jm.FinalArtifactValidationError("safety contract refused")
    monkeypatch.setattr(jm, "_assert_taskvalue_setters_finalized", refuse)
    with pytest.raises(jm.FinalArtifactValidationError, match="safety contract refused"):
        jm._assert_final_notebook_cells(cells)
    assert inspected == cells


@pytest.mark.parametrize("run_all", [False, True])
@pytest.mark.parametrize("alias", ["dbutils", "aidputils", "oidlUtils"])
def test_ru_shape_parent_delivers_tail_without_execution_or_repair(
        monkeypatch, tmp_path, run_all, alias):
    sources = ["value = 1"] * 24 + [f"{alias}.notebook.exit(1)", INVALID] + ["TAIL = 1"] * 74
    notebook = {"cells": [cell(s) for s in sources], "metadata": {},
                "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    kernel = _Kernel(fire_exit=True)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/exit_fixture.ipynb", kernel, "exit_fixture", "dep_fixture", {},
        run_all=run_all))
    assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
    saved = _saved_notebook(kernel)
    assert len(saved["cells"]) == 100
    assert "".join(saved["cells"][25]["source"]) == INVALID
    executed = [s for s in kernel.executed if "base64.b64decode" not in s]
    assert not any(INVALID in s or "TAIL = 1" in s for s in executed)
    reports = "\n".join(text for path, text in kernel.files.items() if path.endswith('.md'))
    assert "syntax ADVISORY: cell 26" in reports
    assert "unreachable after unconditional exit in cell 25" in reports


@pytest.mark.parametrize("run_all", [False, True])
@pytest.mark.parametrize("exit_source", ["if stop:\n    aidputils.notebook.exit(1)", "value = 1"])
def test_parent_conditional_or_no_exit_rejects_same_syntax_error(
        monkeypatch, tmp_path, run_all, exit_source):
    notebook = {"cells": [cell(exit_source), cell(INVALID)], "metadata": {},
                "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    kernel = _Kernel()
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        asyncio.run(jm.process_notebook(
            "/Workspace/exit_fixture.ipynb", kernel, "exit_fixture", "dep_fixture", {},
            run_all=run_all))
    assert not any(path.endswith('.ipynb') for path in kernel.files)


@pytest.mark.parametrize("run_all", [False, True])
def test_parent_conditional_exit_rejects_invalid_remainder_of_same_cell(
        monkeypatch, tmp_path, run_all):
    source = "if stop:\n    aidputils.notebook.exit(1)\n" + INVALID
    notebook = {"cells": [cell(source)], "metadata": {},
                "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    kernel = _Kernel()
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        asyncio.run(jm.process_notebook(
            "/Workspace/exit_fixture.ipynb", kernel, "exit_fixture", "dep_fixture", {},
            run_all=run_all))
    assert not any(path.endswith('.ipynb') for path in kernel.files)


def test_inline_child_preserves_and_reports_invalid_dead_tail(monkeypatch, tmp_path):
    sources = ["aidputils.notebook.exit('exit_fixture')", INVALID]
    result, saved, kernel, installs, logs = run_child(
        monkeypatch, tmp_path, {"child": sources})
    assert result["status"] == "ok", result
    assert result["did_exit"] is True
    assert saved["child"] == sources
    assert any("syntax ADVISORY: cell 2" in note for note in logs)
    assert any("syntax ADVISORY: cell 2" in note for note in result["notes"])
    assert not any(INVALID in code for code in kernel.executed
                   if "base64.b64decode" not in code)
    assert installs == []


def test_child_finalizer_keeps_conditional_tail_blocking():
    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        jm._finalize_child_artifact_sources(
            ["if stop:\n    aidputils.notebook.exit(1)", INVALID])
