"""Child exit policy through real cell loops, count probes, and save-back.

Only customer execution, package installs and external services are mocked.
Generated count/save scripts run against pytest-owned notebook files.
"""
import asyncio
import contextlib
import io
import json

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, jm


def _notebook(sources):
    return {"cells": [{"cell_type": "code", "source": [s], "metadata": {},
                       "outputs": [], "execution_count": None} for s in sources],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


class _ExitKernel(_Kernel):
    def __init__(self, fire=True):
        super().__init__()
        self.fire = fire
        self.saves = 0

    async def execute(self, code, **kwargs):
        if "__AIDP_NESTED_RUN_ARGS__" in code:
            # Evaluate Python argument probes; Scala literal fixtures use the
            # equivalent values (Scala execution is exercised live).
            import ast
            import re
            out = io.StringIO()
            if code.startswith("%scala"):
                target = ast.literal_eval(re.search(r"\}\)\((.+?),", code)[1])
                out.write("__AIDP_NESTED_RUN_ARGS__" + json.dumps(
                    {"path": target, "arguments": {}}))
            else:
                self.namespace = getattr(self, "namespace", {})
                with contextlib.redirect_stdout(out):
                    exec(code, self.namespace)
            return {"status": "ok", "outputs": [
                {"type": "stream", "text": out.getvalue()}]}
        if code.startswith("globals().pop('_aidp_run_args_"):
            exec(code, getattr(self, "namespace", {}))
            return {"status": "ok", "outputs": []}
        if "__AIDP_CHILD_COUNT__" in code or "all_cells = payload.get" in code:
            self.saves += int("all_cells = payload.get" in code)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                exec(code, {})
            return {"status": "ok", "outputs": [
                {"type": "stream", "text": out.getvalue()}]}
        if self.fire and "notebook.exit('exit_fixture')" in code:
            self.executed.append(code)
            return {"status": "ok", "outputs": [
                {"type": "stream", "text": "__AIDP_NOTEBOOK_EXIT__ value='exit_fixture'\n"}]}
        return await super().execute(code, **kwargs)


def _run(monkeypatch, tmp_path, notebooks, *, entry="child", parent=None,
         fire=True, strict=False, cohort=False, real_paths=False, native_protection=False):
    installs, logs = [], []
    _patch_common(monkeypatch, tmp_path, _notebook(parent or []), installs)
    paths = {name: tmp_path / f"{name}.ipynb" for name in notebooks}
    cache = {}
    for name, sources in notebooks.items():
        content = json.dumps(_notebook(sources))
        paths[name].write_text(content)
        cache[str(paths[name])] = content
    # Native-copy filesystem I/O is covered separately with actual readback.
    # This harness models /Workspace aliases, so return the probed native path.
    async def protected_copy(target, session):
        values = [v for k, v in getattr(session, 'namespace', {}).items()
                  if k.startswith('_aidp_run_args_')]
        return values[-1][0] if values else '/Workspace/' + target.rsplit('/', 1)[-1].removesuffix('.ipynb')
    if not native_protection:
        monkeypatch.setattr(jm, "_prepare_native_validation", protected_copy)
    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    monkeypatch.setattr(jm, "_notebook_content_cache", cache)
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_ns_for_oci_bucket", lambda bucket: "fixture_namespace")
    monkeypatch.setattr(jm, "_KNOWN_TEMP_VIEWS", set())

    async def summarize(*args, **kwargs):
        return "fixture cell"

    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    mapping = {f"/Workspace/{name}.ipynb": str(path) for name, path in paths.items()}
    mapping.update({str(path): str(path) for path in paths.values()})
    if not real_paths:
        # Existing fixtures model /Workspace names as aliases of local files.
        resolve = lambda name, *a, **kw: str(
            paths[name.rsplit('/', 1)[-1].removesuffix('.ipynb')])
        monkeypatch.setattr(jm, "_resolve_nbrun_child_path", resolve)
        monkeypatch.setattr(jm, "_native_notebook_path", resolve, raising=False)
    if cohort:
        monkeypatch.setattr(jm, "extract_notebook_run_cohort_map",
                            lambda nb: {"/Workspace/child": "items"})

        async def samples(*args, **kwargs):
            return {"items": "fixture"}

        monkeypatch.setattr(jm, "extract_cohort_samples", samples)
    kernel = _ExitKernel(fire)
    if parent is not None:
        result = asyncio.run(jm.process_notebook(
            "/Workspace/parent.ipynb", kernel, "test_pip", "01_task", {},
            dep_path_map=mapping))
    else:
        class Log:
            def log(self, message):
                logs.append(message)

        result = asyncio.run(jm._inline_child_notebook(
            str(paths[entry]), kernel, Log(), "test_pip", "", "", "", mapping))
    saved = {name: ["".join(c["source"]) for c in json.loads(path.read_text())["cells"]]
             for name, path in paths.items()}
    return result, saved, kernel, installs, logs


CONDITIONAL = "if STOP_EARLY:\n    aidputils.notebook.exit('exit_fixture')"
UNCONDITIONAL = "aidputils.notebook.exit('exit_fixture')"


@pytest.mark.parametrize("remainder", ["%restart_python\nTAIL_PY = 42",
                                       "%sql SELECT 42 AS TAIL_SQL",
                                       "%scala\nval TAIL_SCALA = 42"])
@pytest.mark.parametrize("parent_tail", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_mixed_pip_exit_tail_is_converted_but_never_executed(
        monkeypatch, tmp_path, remainder, parent_tail, strict):
    tail = "!pip install tailpkg\n" + remainder
    notebooks = {"child": [CONDITIONAL] if parent_tail else [CONDITIONAL, tail]}
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path, notebooks, strict=strict,
        parent=["%run /Workspace/child", tail] if parent_tail else None)
    assert installs == []
    assert result["status"] == ("PASS_CONDITIONAL_EXIT_COVERAGE" if parent_tail else "ok"), result
    if parent_tail:
        notebook = next(json.loads(text) for path, text in kernel.files.items()
                        if path.endswith('.ipynb'))
        converted = ''.join(notebook['cells'][-1]['source'])
    else:
        converted = saved['child'][-1]
    assert not jm._find_pip_installs(converted)[0]
    assert "not installed by this run" in converted
    if remainder.startswith(('%sql', '%scala')):
        assert converted.startswith(remainder.split()[0] + '\n')
    else:
        compile(converted, '<tail>', 'exec')
    executed = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert 'TAIL_PY' not in executed and 'TAIL_SQL' not in executed and 'TAIL_SCALA' not in executed


TAIL = [
    'TAIL_S3 = "s3://fixture-bucket/input"',
    'TAIL_DBFS = "dbfs:/input"',
    'dbutils.jobs.taskValues.set(key="TAIL_API", value=1)',
    "%sql\nSELECT 'TAIL_SQL'",
    '%scala\nval TAIL_SCALA = "s3://fixture-bucket/input"',
    "!pip install tailpkg\nTAIL_PY = 42",
    "%run /Workspace/not_called",
]


@pytest.mark.parametrize("magic", ["%sql", "%scala"])
def test_failed_mixed_native_child_reaches_real_parent(monkeypatch, tmp_path, magic):
    original_execute = _ExitKernel.execute

    async def native_failure(self, code, **kwargs):
        if code.startswith(magic + '\n'):
            self.executed.append(code)
            return {"status": "ok", "outputs": [
                {"type": "error", "ename": "SyntaxError", "evalue": "fixture"}]}
        return await original_execute(self, code, **kwargs)

    monkeypatch.setattr(_ExitKernel, "execute", native_failure)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 1)
    result, saved, _, installs, _ = _run(
        monkeypatch, tmp_path,
        {"child": ["!pip install real_pkg\n" + magic + "\nINVALID_NATIVE_BODY"]},
        parent=["%run /Workspace/child"], fire=False)
    assert installs == ["real_pkg"]
    assert result["status"] in ("PARTIAL", "FAIL"), result
    assert result["pip_unresolved"] >= 1
    assert any("remaining code failed execution" in c.get('note', '')
               for c in result['cell_results'])
    assert not jm._find_pip_installs(saved['child'][0])[0]


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("exit_source", [CONDITIONAL, UNCONDITIONAL])
def test_child_exit_converts_only_conditional_tail(monkeypatch, tmp_path, strict, exit_source):
    result, saved, kernel, installs, logs = _run(
        monkeypatch, tmp_path, {"child": [exit_source, *TAIL]}, strict=strict)
    assert result["status"] == "ok", result
    assert result["did_exit"] is True
    assert result["exit_value"] == "exit_fixture"
    assert result["cells_ok"] == 1
    assert len(saved["child"]) == 1 + len(TAIL)
    assert saved["child"][0] == exit_source
    assert installs == []
    assert not any("TAIL_" in c for c in kernel.executed if "base64.b64decode" not in c)
    assert kernel.saves == 1
    assert not any("TAIL_" in entry.get("replay_code", "") for entry in jm._cell_history), \
        "unexecuted child cells must not become executable rewind history"
    assert not jm._find_pip_installs(saved["child"][6])[0]
    if exit_source == CONDITIONAL:
        # REF-158: an unexecuted Python cell keeps its storage paths, reported.
        assert saved["child"][1:3] == TAIL[:2]
        for index, form in ((1, "s3://"), (2, "dbfs:")):
            assert any(note.startswith(f"child.ipynb cell {index}: UNRESOLVED storage paths")
                       and form in note for note in result["notes"]), result["notes"]
        assert "oidlUtils.parameters.setTaskValue" in saved["child"][3]
        assert saved["child"][5].startswith("%scala\n")
        assert "oci://" in saved["child"][5]
        assert result.get("exit_coverage"), "caller must receive the unvalidated-tail finding"
        assert any("NOT runtime-validated" in message for message in logs)
    else:
        # The unconditional tail is not conversion-executed, but the
        # persisted child still passes through the accepted PR #92 final
        # artifact contract.  Preserve ordinary cells byte-for-byte while
        # normalizing an unsafe cross-task setter before save-back.
        assert saved["child"][1:3] == TAIL[:2]
        assert "oidlUtils.parameters.setTaskValue" in saved["child"][3]
        assert "str(1)" in saved["child"][3]
        assert saved["child"][4:6] == TAIL[3:5]
        assert saved["child"][7] == TAIL[6]
        assert not result.get("exit_coverage")


@pytest.mark.parametrize("depth", [1, 2])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("exit_source", [CONDITIONAL, UNCONDITIONAL])
def test_nested_run_exit_propagates_without_executing_any_enclosing_tail(
        monkeypatch, tmp_path, depth, strict, exit_source):
    notebooks = {"leaf": [exit_source, "!pip install leaftail"]}
    next_child = "leaf"
    for n in range(depth):
        name = f"level{n}"
        notebooks[name] = [f"%run ./{next_child}",
                           f'TAIL_{n} = "s3://fixture-bucket/input"',
                           f"!pip install tail{n}"]
        next_child = name
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path, notebooks, entry=next_child, strict=strict)
    assert result["did_exit"] is True
    assert result["exit_value"] == "exit_fixture"
    assert result["status"] == "ok"
    assert result["cells_ok"] == 1
    assert installs == []
    assert not any("TAIL_" in c for c in kernel.executed if "base64.b64decode" not in c)
    for n in range(depth):
        assert len(saved[f"level{n}"]) == 3
        assert saved[f"level{n}"][1] == f'TAIL_{n} = "s3://fixture-bucket/input"'
        assert any(note.startswith(f"level{n}.ipynb cell 1: UNRESOLVED storage paths")
                   for note in result["notes"]), result["notes"]
    assert result.get("exit_coverage")


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("invocation,cohort", [
    ("%run /Workspace/child", False),
    ('dbutils.notebook.run("/Workspace/child", 60)', False),
    ('%scala\ndbutils.notebook.run("/Workspace/child", 60)', False),
    ('dbutils.notebook.run(dynamic_path, 60)', True),
])
def test_conditional_child_coverage_reaches_parent_report(
        monkeypatch, tmp_path, strict, invocation, cohort):
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path, {"child": [CONDITIONAL, *TAIL]},
        parent=[invocation, "PARENT_CONTINUES = 1"], strict=strict, cohort=cohort)
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    reports = [text for path, text in kernel.files.items() if path.endswith("test_report.md")]
    assert reports and "child.ipynb" in reports[0]
    assert "NOT runtime-validated" in reports[0]
    parent_executed = any("PARENT_CONTINUES = 1" in c for c in kernel.executed
                          if "base64.b64decode" not in c)
    assert parent_executed is (not invocation.startswith("%run"))
    assert installs == []


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("invocation", ["%run ./leaf", 'dbutils.notebook.run("./leaf", 60)'])
@pytest.mark.parametrize("fire", [False, True])
def test_nested_notebook_run_is_a_return_boundary(monkeypatch, tmp_path, strict, invocation, fire):
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path,
        {"child": [invocation, "!pip install parenttail\nPARENT_TAIL = 1"],
         "leaf": [CONDITIONAL, "!pip install leaftail\nLEAF_TAIL = 1"]},
        strict=strict, fire=fire)
    propagates = fire and invocation.startswith("%run")
    assert result["status"] == "ok", result
    assert result["did_exit"] is propagates
    assert result.get("exit_value") == ("exit_fixture" if propagates else None)
    assert installs == ([] if propagates else ["parenttail"] if fire
                        else ["leaftail", "parenttail"])
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    assert ("PARENT_TAIL = 1" in executed) is not propagates
    assert ("LEAF_TAIL = 1" in executed) is not fire
    assert len(saved["child"]) == len(saved["leaf"]) == 2
    assert bool(result.get("exit_coverage")) is fire


@pytest.mark.parametrize("exit_source", [CONDITIONAL, UNCONDITIONAL])
@pytest.mark.parametrize("tail", [[], ["UNCHANGED_TAIL = 1"], ['TAIL_PATH = "s3://fixture-bucket/x"'],
                                  ['%scala\nval TAIL_PATH = "s3://fixture-bucket/x"']])
def test_exit_tail_without_pip_preserves_value_cache_and_save_policy(
        monkeypatch, tmp_path, exit_source, tail):
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path, {"child": [exit_source, *tail]})
    # Only non-Python storage paths are rewritten in a tail (REF-158).
    changed = exit_source == CONDITIONAL and any(s.startswith("%scala") for s in tail)
    assert kernel.saves == int(changed)
    assert result["exit_value"] == "exit_fixture"
    assert result["cells_ok"] == 1
    assert bool(result.get("exit_coverage")) is (exit_source == CONDITIONAL and bool(tail))
    assert len(saved["child"]) == len(tail) + 1
    cached = json.loads(jm._notebook_content_cache[str(tmp_path / "child.ipynb")])
    assert ["".join(c["source"]) for c in cached["cells"]] == saved["child"]
    assert installs == []


def test_conditional_tail_includes_parent_language_specific_preprocessing(monkeypatch, tmp_path):
    tail = ["dbutils.library.restartPython()", "import matplotlib.pyplot as plt",
            '%scala\nval TAIL_MAP = Map("key" -> "value")',
            'from slack_sdk import WebClient\nposter = WebClient(token="fixture")',
            'poster.files_upload_v2(file="/tmp/never_sent")']
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path, {"child": [CONDITIONAL, *tail]})
    assert result["status"] == "ok"
    assert saved["child"][1].startswith("# Oracle tool modification: restartPython")
    assert "matplotlib.use('Agg')" in saved["child"][2]
    assert saved["child"][3] == tail[2], "Scala must not pass through Python preprocessing"
    assert all(line.lstrip().startswith("#") for line in saved["child"][5].splitlines()
               if "files_upload_v2" in line)
    assert installs == []


def test_nested_exit_reaches_top_parent_without_executing_parent_tail(monkeypatch, tmp_path):
    result, saved, kernel, installs, _ = _run(
        monkeypatch, tmp_path,
        {"child": ["%run ./leaf", "!pip install enclosingtail\nENCLOSING_TAIL = 1"],
         "leaf": [UNCONDITIONAL, "!pip install leaftail"]},
        parent=["%run /Workspace/child", "!pip install parenttail\nPARENT_TAIL = 1"],
        strict=True)
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    assert installs == []
    executed = "\n".join(c for c in kernel.executed if "base64.b64decode" not in c)
    assert "PARENT_TAIL = 1" not in executed
    assert "ENCLOSING_TAIL = 1" not in executed
    reports = [text for path, text in kernel.files.items() if path.endswith("test_report.md")]
    assert reports and "child.ipynb" in reports[0]
    assert "NOT runtime-validated" in reports[0]


def test_unexecuted_tail_state_does_not_leak_into_continuing_caller(monkeypatch, tmp_path):
    tail = ['df.createOrReplaceTempView("tail_view")',
            'TAIL_READ = spark.table("tail_view")',
            'from slack_sdk import WebClient\nposter = WebClient(token="fixture")',
            'poster.files_upload_v2(file="/tmp/never_sent")']
    result, saved, kernel, _, _ = _run(
        monkeypatch, tmp_path, {"child": [CONDITIONAL, *tail]},
        parent=['dbutils.notebook.run("/Workspace/child", 60)',
                'CALLER_READ = spark.table("tail_view")'])
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    # The converted tail models its own preceding declarations, but those
    # declarations never happened in the live caller's kernel namespace.
    assert 'spark.table("tail_view")' in saved["child"][2]
    assert "tail_view" not in jm._KNOWN_TEMP_VIEWS
    assert "poster" not in jm._current_slack_bindings()
    assert any('spark.table("default.default.tail_view")' in code for code in kernel.executed
               if "base64.b64decode" not in code)


@pytest.mark.parametrize("exit_source", [CONDITIONAL, UNCONDITIONAL])
@pytest.mark.parametrize("value", ["None", "", "result with spaces"])
def test_child_exit_value_preserved_with_converted_tail(monkeypatch, tmp_path, exit_source, value):
    original_execute = _ExitKernel.execute

    async def exit_with_value(self, code, **kwargs):
        if "notebook.exit('exit_fixture')" in code:
            return {"status": "ok", "outputs": [
                {"type": "stream", "text": f"__AIDP_NOTEBOOK_EXIT__ value={value}\n"}]}
        return await original_execute(self, code, **kwargs)

    monkeypatch.setattr(_ExitKernel, "execute", exit_with_value)
    result, _, _, _, _ = _run(
        monkeypatch, tmp_path, {"child": [exit_source, 'TAIL = "s3://fixture-bucket/x"']})
    assert result["did_exit"] is True
    assert result["exit_value"] == value


def test_conversion_failure_is_not_silently_saved_as_success(monkeypatch, tmp_path):
    original = jm._neutralize_for_execution

    def reject_tail(code, **kwargs):
        if "tail_view" in code:
            raise jm.SlackGuardError("fixture: cannot certify tail")
        return original(code, **kwargs)

    monkeypatch.setattr(jm, "_neutralize_for_execution", reject_tail)
    with pytest.raises(jm.SlackGuardError, match="cannot certify tail"):
        _run(monkeypatch, tmp_path, {"child": [
            CONDITIONAL, 'df.createOrReplaceTempView("tail_view")']})
    assert "tail_view" not in jm._KNOWN_TEMP_VIEWS
