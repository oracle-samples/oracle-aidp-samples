"""Empty delivery incident: retry final code, never re-run exploration or keep raw AWS code."""
import asyncio
import copy
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import agent_migrate as am
import job_migrate as jm


def reply(text=None, tool=None, code=None):
    blocks = []
    if text is not None:
        blocks.append(NS(type="text", text=text))
    if tool:
        blocks.append(NS(type="tool_use", name=tool, id="tool-id",
                         input={"code": code or ""}))
    return NS(content=blocks, stop_reason="tool_use" if tool else "end_turn",
              usage=NS(input_tokens=10, output_tokens=5))


def model(monkeypatch, responses):
    requests = []
    responses = iter(responses)

    def create(**kwargs):
        requests.append(copy.deepcopy(kwargs))
        return next(responses)

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    return requests


def run(**kwargs):
    return asyncio.run(am.call_opus_with_tools(
        "Migrate code", "def read_data(): return old_client.read()",
        code_validator=jm._valid_python_migration_response, **kwargs))


@pytest.mark.parametrize("bad", ["", "  ", "Producing the final migrated code.", "def broken(:"])
def test_bad_delivery_retried_without_exploration(monkeypatch, bad):
    requests = model(monkeypatch, [reply(bad), reply(tool="submit_code", code="print('fixed')")])
    code, usage = run()
    assert code == "print('fixed')"
    assert usage == {"input": 20, "output": 10}
    assert len(requests) == 2
    assert [t["name"] for t in requests[1]["tools"]] == ["submit_code"]
    assert requests[1]["messages"][0] == requests[0]["messages"][0]


def test_exhausted_exploration_preserves_successful_result_once(monkeypatch):
    requests = model(monkeypatch, [reply(tool="run_on_cluster"), reply(),
                                  reply(tool="submit_code", code="print('fixed')")])
    executed = []

    async def tool(name, args, **kwargs):
        executed.append(name)
        return "OCI_TEST_SUCCEEDED"

    monkeypatch.setattr(am, "_handle_tool_call", tool)
    code, usage = run(max_tool_rounds=0)
    assert code == "print('fixed')"
    assert executed == ["run_on_cluster"]
    assert len(requests) == 3
    for request in requests[1:]:
        results = [b for m in request["messages"] if isinstance(m["content"], list)
                   for b in m["content"] if isinstance(b, dict) and b.get("type") == "tool_result"]
        assert [b["content"] for b in results] == ["OCI_TEST_SUCCEEDED"]
    assert usage == {"input": 30, "output": 15}


@pytest.mark.parametrize("initial", [reply(), reply(tool="submit_code"), reply("def broken(:")])
def test_delivery_exhaustion_fails_loudly(monkeypatch, initial):
    requests = model(monkeypatch, [initial, reply(), reply("Not code.")])
    with pytest.raises(RuntimeError, match="no usable final code after 2 delivery retries"):
        run()
    assert len(requests) == 3


@pytest.mark.parametrize("initial", [reply("print(1)"), reply(tool="submit_code", code="print(1)")])
def test_valid_delivery_does_not_retry(monkeypatch, initial):
    requests = model(monkeypatch, [initial])
    assert run()[0] == "print(1)"
    assert len(requests) == 1


def test_non_code_evaluation_contract_unchanged(monkeypatch):
    requests = model(monkeypatch, [reply("VERIFIED")])
    assert asyncio.run(am.call_opus_with_tools("Evaluate", "input"))[0] == "VERIFIED"
    assert len(requests) == 1


@pytest.mark.parametrize(
    "code",
    [
        "```python\nprint(1)\n```",
        "await task()",
        "%sql\nSELECT 1",
        "%python # explicit language\nprint(1)",
        "%pyspark # source alias\nprint(1)",
        "# preserved comment",
    ],
)
def test_valid_notebook_syntax(code):
    assert jm._valid_python_migration_response(code)


@pytest.mark.parametrize("task_key", ["01_task", "dep_helper"])
@pytest.mark.parametrize("recover", [True, False])
@pytest.mark.parametrize("run_all", [True, False])
def test_real_cell_loop_delivery(monkeypatch, tmp_path, task_key, recover, run_all):
    from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources
    source = "def read_data():\n    return boto3.client('s3')\n"
    notebook = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                           "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: True)
    monkeypatch.setattr(jm, "call_opus_with_tools", am.call_opus_with_tools)
    responses = [reply(), reply(tool="submit_code", code="def read_data():\n    return 1\n")] if recover else [reply(), reply(), reply()]
    requests = model(monkeypatch, responses)
    kernel = _Kernel()
    operation = jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_delivery", task_key, {}, run_all=run_all)
    if recover:
        result = asyncio.run(operation)
        assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
        saved, _ = _saved_sources(kernel)
        assert "def read_data():" in saved
        assert "return 1" in saved
        assert "boto3.client" not in saved
    else:
        with pytest.raises(RuntimeError, match="no usable final code"):
            asyncio.run(operation)
        assert not kernel.files, "failed conversion must not publish an original-source fallback"
    assert len(requests) == (2 if recover else 3)


def test_native_scala_code_only_does_not_need_model_delivery(monkeypatch, tmp_path):
    from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources
    notebook = {"cells": [{"cell_type": "code", "source": ["%scala\nval old_value = 1"],
                           "metadata": {}, "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "call_opus_with_tools", am.call_opus_with_tools)
    requests = model(monkeypatch, [])
    kernel = _Kernel()
    operation = jm.process_notebook("/Workspace/test_cond/source.ipynb", kernel,
                                    "test_delivery", "dep_scala", {}, run_all=False)
    assert asyncio.run(operation)["status"] == "CONVERTED_NOT_VALIDATED"
    saved, _ = _saved_sources(kernel)
    assert "%scala\nval old_value = 1" in saved
    assert not requests


def test_retry_does_not_execute_unexpected_tools(monkeypatch):
    requests = model(monkeypatch, [reply(), reply(tool="run_on_cluster"),
                                  reply(tool="submit_code", code="print(1)")])
    async def forbidden(*args, **kwargs):
        raise AssertionError("Delivery must not execute tools")
    monkeypatch.setattr(am, "_handle_tool_call", forbidden)
    assert run()[0] == "print(1)"
    results = [b for m in requests[-1]["messages"] if isinstance(m["content"], list)
               for b in m["content"] if isinstance(b, dict) and b.get("type") == "tool_result"]
    assert len(results) == 1 and results[0]["is_error"] is True


@pytest.mark.parametrize("fanout", [1, 2])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("manifest_only", [False, True])
@pytest.mark.parametrize("code_only", [False, True])
@pytest.mark.parametrize("failure", ["delivery", "unsuccessful_result"])
def test_dependency_delivery_failure_records_job_and_blocks_parent(
        monkeypatch, tmp_path, fanout, strict, manifest_only, code_only, failure):
    """Real job -> recursive dependency -> cell loop -> failed delivery.

    Both dependency schedulers must return a failed job report, not drop the
    dependency mapping or throw before task-level reporting.
    """
    import json
    from test_conditional_exit_below_execute import _Kernel, _patch_common

    def nb(source):
        return {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                           "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}

    paths = {
        "parent.ipynb": nb("print('PARENT_MUST_NOT_RUN')" if manifest_only else
                           "%run /Workspace/deps/middle.ipynb"),
        "middle.ipynb": nb("%run /Workspace/deps/bad.ipynb"),
        "bad.ipynb": nb("def read_data():\n    return boto3.client('s3')\n"),
        "good.ipynb": nb("print('GOOD_DEP')"),
    }
    if fanout == 2:
        paths["middle.ipynb"]["cells"].append(
            nb("%run /Workspace/deps/good.ipynb")["cells"][0])
    _patch_common(monkeypatch, tmp_path, paths["bad.ipynb"])
    for name in ("MIGRATED_BASE", "JOB_ROOT", "_db_to_aidp_job_map", "_unmapped_db_job_ids"):
        monkeypatch.setattr(jm, name, getattr(jm, name))
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "CODE_ONLY", code_only)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "START_TASK", None)
    monkeypatch.setattr(jm, "ONLY_TASKS", [])
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None)
    monkeypatch.setattr(jm, "download_notebook",
                        lambda path: json.dumps(paths[path.rsplit("/", 1)[-1]]).encode())
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda src, **kw: "boto3" in src)
    monkeypatch.setattr(jm, "call_opus_with_tools", am.call_opus_with_tools)
    requests = model(monkeypatch, [reply(), reply(), reply()])
    if failure == "unsuccessful_result":
        real_process = jm.process_notebook

        async def failed_result(path, *args, **kwargs):
            if path.endswith("bad.ipynb"):
                return {"status": "FAIL"}
            return await real_process(path, *args, **kwargs)

        monkeypatch.setattr(jm, "process_notebook", failed_result)

    async def noop(*args, **kwargs):
        pass

    async def no_backfill(*args, **kwargs):
        pytest.fail("failed dependency job must not continue to backfill")

    monkeypatch.setattr(jm, "stage_export_resources", noop)
    monkeypatch.setattr(jm, "_ensure_redirect_schema", noop)
    monkeypatch.setattr(jm, "backfill_missing_deps", no_backfill)

    class Kernel(_Kernel):
        async def run_stateless(self, code, **kwargs):
            if "results = {}" in code:
                return {"status": "ok", "outputs": [{"type": "stream", "text": "{}"}]}
            return await self.execute(code, **kwargs)

    kernel = Kernel()
    tasks = [{"task_key": key, "notebook_path": "/Workspace/deps/parent.ipynb",
              "run_deps": ([{"path": "/Workspace/deps/middle.ipynb", "exists": True}]
                           if manifest_only and key == "parent" else []),
              "depends_on": ["parent"] if key == "downstream" else []}
             for key in ("parent", "same_layer", "downstream")]
    result = asyncio.run(jm.process_job({
        "job_name": "dependency_delivery", "tasks": tasks,
        "execution_layers": [["parent", "same_layer"], ["downstream"]],
        "default_cluster": kernel.cluster_id,
    }, kernel))
    parent = result["task_results"]["parent"]
    assert parent["status"] == "FAIL", result
    assert parent["failed"] == 1
    expected_error = "no usable final code" if failure == "delivery" else "bad.ipynb"
    assert expected_error in parent["error"]
    assert result["total_failed"] == 1
    for key in ("same_layer", "downstream"):
        assert result["task_results"][key]["status"] == "BLOCKED"
    assert len(requests) == (3 if failure == "delivery" else 0)
    reports = [value for path, value in kernel.files.items() if path.endswith("JOB_REPORT.md")]
    assert len(reports) == 1
    assert "| parent | FAIL |" in reports[0]
    assert expected_error in reports[0]
    assert not any(path.endswith(("parent.ipynb", "middle.ipynb", "bad.ipynb"))
                   for path in kernel.files)
    assert not any("PARENT_MUST_NOT_RUN" in code for code in kernel.executed)
    # Parallel siblings finish before reporting; the failed path is never cached.
    if fanout == 2:
        assert any(path.endswith("good.ipynb") for path in kernel.files)
    assert not any(path.endswith(("middle.ipynb", "bad.ipynb")) for path in jm._migration_cache)


@pytest.mark.parametrize("fanout", [1, 2])
def test_successful_dependency_preparation_preserves_all_mappings(monkeypatch, tmp_path, fanout):
    import json
    from test_conditional_exit_below_execute import _Kernel, _patch_common
    notebook = {"cells": [{"cell_type": "code", "source": ["print(1)"]}], "metadata": {}}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "EXPORT_BASE", "")
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "_migration_cache", {})
    deps = [f"/Workspace/deps/leaf_{i}.ipynb" for i in range(fanout)]
    monkeypatch.setattr(jm, "scan_for_run_deps",
                        lambda content: [(p, {}) for p in deps] if content == b"root" else [])
    monkeypatch.setattr(jm, "download_notebook",
                        lambda path: b"root" if path.endswith("root.ipynb") else json.dumps(notebook).encode())
    calls = []

    async def converted(path, *args, **kwargs):
        await asyncio.sleep(0)
        calls.append((path, dict(kwargs["dep_path_map"])))
        return {"status": "CONVERTED_NOT_VALIDATED"}

    monkeypatch.setattr(jm, "process_notebook", converted)

    class Kernel(_Kernel):
        async def run_stateless(self, code, **kwargs):
            return {"status": "ok", "outputs": [{"type": "stream", "text": "{}"}]}

    result = asyncio.run(jm.ensure_migrated("/Workspace/deps/root.ipynb", Kernel()))
    assert result.endswith("deps/root.ipynb")
    assert len(calls) == fanout + 1
    assert calls[-1][0].endswith("root.ipynb")
    for path in deps:
        assert calls[-1][1][path].endswith(jm.normalize_nb_path(path))
