"""REF-158: Python cells below a fired notebook.exit meet the model, code-only.

A conditional notebook.exit that fires at migration time leaves the cells
below it unexecuted (they run only when the exit does not fire). A Python
cell there that holds an S3/DBFS path is converted by the model without being
run: no tool that runs the cell's code or replays a cell is offered, the reply
goes through the same post-model chain as any model-migrated cell, and it is
saved as the model returned it, with any S3/DBFS path form left in it reported
UNRESOLVED. Nothing rewrites its storage strings deterministically, so a path
check keeps recognizing its input. When the model cannot convert the cell, it
keeps its source paths and is reported UNRESOLVED; the run still passes.

The real cell loop runs offline; only the model and the kernel are faked.
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm  # noqa: E402
from test_conditional_exit_below_execute import _Kernel  # noqa: E402
from test_path_recognition_migration import (  # noqa: E402
    CASES, _migration_report, _model_code, _notebook, _patch_cell_loop, _probe,
    _reply, _saved_cell_sources, _source)


EXIT_CELL = "if MIGRATION_TIME_FLAG:\n    aidputils.notebook.exit('done')\n"
PATH_CELL = ('DATA_PATH = "s3://fixture-bucket/input"\n'
             'rows = spark.read.parquet(DATA_PATH)\n')
PATH_REPLY = ('# Oracle tool modification: fixture translation\n'
              'DATA_PATH = "oci://oci-fixture-bucket@test_namespace/input"\n'
              'rows = spark.read.parquet(DATA_PATH)\n')
TOOLS_THAT_RUN_CODE = {"run_on_cluster", "fixup_cell", "repair_dependency",
                       "submit_data_recovery_plan"}


def _model(calls, reply=None, error=None):
    async def migrate(prompt, content, **kwargs):
        source = content.split("```python\n", 1)[1].rsplit("\n```", 1)[0]
        calls.append({"source": source, "content": content, "kwargs": kwargs})
        if error is not None:
            raise error
        return (source if reply is None else reply(source)), {"input": 0, "output": 0}
    return migrate


def _run_tail(monkeypatch, tmp_path, tail, model):
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([EXIT_CELL, *tail]))
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: False)
    monkeypatch.setattr(jm, "call_opus_with_tools", model)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_cond", "01_task", {}))
    return result, kernel


def _executed(kernel):
    # Save-back writes the saved notebook base64-encoded; everything else ran.
    return "\n".join(code for code in kernel.executed if "base64.b64decode" not in code)


def test_a_path_cell_below_a_fired_exit_is_converted_by_the_model_without_running(
        monkeypatch, tmp_path):
    calls = []
    result, kernel = _run_tail(monkeypatch, tmp_path, [PATH_CELL],
                               _model(calls, lambda source: PATH_REPLY))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert [call["source"] for call in calls] == [PATH_CELL]
    content, kwargs = calls[0]["content"], calls[0]["kwargs"]
    assert "CODE-ONLY" in content and "notebook.exit" in content
    assert TOOLS_THAT_RUN_CODE <= set(kwargs.get("exclude_tools") or ())
    assert _saved_cell_sources(kernel)[1] == _model_code(PATH_REPLY).rstrip("\n")
    assert "fixture-bucket" not in _executed(kernel), "the cell below the exit must not run"
    report = _migration_report(kernel)
    assert "Cell 1: converted by the model without running it" in report
    assert "UNRESOLVED storage paths" not in report


@pytest.mark.parametrize("case", CASES)
def test_path_checks_below_a_fired_exit_are_saved_as_the_model_returns_them(
        monkeypatch, tmp_path, case):
    # The REF-158 shapes: S3 gold-bucket checks and DBFS readers. Nothing may
    # rewrite the strings they recognize their input with.
    calls = []
    reply = _reply(case)
    result, kernel = _run_tail(monkeypatch, tmp_path, [_source(case)],
                               _model(calls, lambda source: reply))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert len(calls) == 1
    saved = _saved_cell_sources(kernel)[1]
    assert saved == _model_code(reply).rstrip("\n")
    _probe(saved, case)


def test_a_reply_that_keeps_a_raw_path_is_saved_as_returned_and_reported(monkeypatch, tmp_path):
    # Nothing runs the cell, so nothing shows whether an S3/DBFS form left in
    # the model's code is a destination it missed or a check it kept on purpose.
    calls = []
    result, kernel = _run_tail(monkeypatch, tmp_path, [PATH_CELL], _model(calls))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert len(calls) == 1
    assert _saved_cell_sources(kernel)[1] == PATH_CELL.rstrip("\n")
    report = _migration_report(kernel)
    assert "Cell 1: converted by the model without running it" in report
    assert ("Cell 1: UNRESOLVED storage paths — the model's code still contains S3/DBFS "
            "path forms s3://; it was not executed") in report
    assert "kept as in the source" not in report
    assert "fixture-bucket" not in _executed(kernel)


def test_a_model_failure_keeps_the_source_paths_and_reports_them(monkeypatch, tmp_path):
    calls = []
    failure = RuntimeError("Conversion failed: no usable final code after 2 delivery retries")
    result, kernel = _run_tail(monkeypatch, tmp_path, [PATH_CELL], _model(calls, error=failure))
    # A conditional exit is normal control flow: the run still passes.
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert len(calls) == 1
    assert _saved_cell_sources(kernel)[1] == PATH_CELL.rstrip("\n")
    report = _migration_report(kernel)
    assert "Cell 1: UNRESOLVED storage paths — S3/DBFS path forms s3://" in report
    assert "no usable final code" in report
    assert "fixture-bucket" not in _executed(kernel)


def test_an_unexpected_error_is_not_turned_into_a_fallback(monkeypatch, tmp_path):
    calls = []
    with pytest.raises(TypeError, match="fixture bug"):
        _run_tail(monkeypatch, tmp_path, [PATH_CELL],
                  _model(calls, error=TypeError("fixture bug")))
    assert len(calls) == 1


def test_a_reply_that_restores_a_slack_post_is_neutralised(monkeypatch, tmp_path):
    calls = []
    reply = PATH_REPLY + 'client.chat_postMessage(channel="#c", text="done")\n'
    tail = ["client = WebClient(token=TOKEN)\n", PATH_CELL]
    result, kernel = _run_tail(monkeypatch, tmp_path, tail, _model(calls, lambda source: reply))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    saved = _saved_cell_sources(kernel)[2]
    live = [line for line in saved.splitlines() if not line.lstrip().startswith("#")]
    assert not any("chat_postMessage" in line for line in live), saved
    assert "oci://oci-fixture-bucket@test_namespace/input" in saved


def test_a_cell_the_slack_guard_changed_stays_on_the_deterministic_path(monkeypatch, tmp_path):
    calls = []
    tail = ["client = WebClient(token=TOKEN)\n",
            PATH_CELL + 'client.chat_postMessage(channel="#c", text="done")\n']
    result, kernel = _run_tail(monkeypatch, tmp_path, tail, _model(calls, lambda source: PATH_REPLY))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert calls == []
    saved = _saved_cell_sources(kernel)[2]
    assert 'DATA_PATH = "s3://fixture-bucket/input"' in saved
    assert "Cell 2: UNRESOLVED storage paths" in _migration_report(kernel)


@pytest.mark.parametrize("cell", [
    "SENTINEL_BELOW = 1\n",
    'TABLE = "ds.fixture_table"\nrows = spark.table(TABLE)\n',
    '%pip install fixture_pkg\nDATA_PATH = "s3://fixture-bucket/input"\n',
])
def test_cells_without_a_path_or_with_a_pip_install_do_not_meet_the_model(
        monkeypatch, tmp_path, cell):
    calls = []
    result, kernel = _run_tail(monkeypatch, tmp_path, [cell], _model(calls))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert calls == []
