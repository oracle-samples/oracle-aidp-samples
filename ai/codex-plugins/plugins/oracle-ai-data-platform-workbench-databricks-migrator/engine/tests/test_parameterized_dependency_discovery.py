import asyncio

from dbx_export.dependency_resolver import extract_run_targets


def _notebook(*sources):
    return {
        "cells": [
            {"cell_type": "code", "source": source.splitlines(keepends=True)}
            for source in sources
        ]
    }


def test_param_override_resolves_widget_fstring_notebook_run():
    notebook = _notebook(
        'root = dbutils.widgets.get("notebook_root")\n'
        'target = f"{root}/process_alerts"\n'
        'dbutils.notebook.run(target, 3600)\n'
    )

    targets = extract_run_targets(
        notebook,
        "/Workspace/Jobs/Hourly_Monitoring/master",
        parameter_bindings={
            "notebook_root": "/Workspace/Jobs/Hourly_Monitoring",
        },
    )

    assert "/Jobs/Hourly_Monitoring/process_alerts" in targets


def test_bounded_data_flow_supports_dict_format_join_and_loop_candidates():
    notebook = _notebook(
        'root = dbutils.widgets.getArgument("root", "/Workspace/default")\n'
        'names = {"alert": "process_alerts"}\n'
        'first = posixpath.join(root, names["alert"])\n'
        'second = "{}/daily_rollup".format(root)\n'
        'candidates = [first, second]\n'
        'for target in candidates:\n'
        '    dbutils.notebook.run(target, 3600)\n'
    )

    targets = extract_run_targets(
        notebook,
        "/Workspace/Jobs/master",
        parameter_bindings={"root": "/Workspace/Jobs/Hourly_Monitoring"},
    )

    assert "/Jobs/Hourly_Monitoring/process_alerts" in targets
    assert "/Jobs/Hourly_Monitoring/daily_rollup" in targets
    assert all("/Workspace/default" not in target for target in targets)


def test_unknown_runtime_call_is_not_executed_or_guessed():
    messages = []
    notebook = _notebook(
        "target = lookup_notebook_from_database()\n"
        "dbutils.notebook.run(target, 3600)\n"
    )

    targets = extract_run_targets(
        notebook, "/Workspace/Jobs/master", log=messages.append,
        parameter_bindings={"root": "/Workspace/Jobs"},
    )

    assert targets == []
    assert any("cannot resolve statically" in message for message in messages)


def test_aidputils_notebook_run_is_discovered():
    notebook = _notebook(
        'aidputils.notebook.run("./child_notebook", 0, {})\n'
    )

    assert extract_run_targets(
        notebook, "/Workspace/team/parent.ipynb"
    ) == ["/Workspace/team/child_notebook"]


def test_workflow_discovery_uses_runtime_parameter_precedence(monkeypatch):
    from scripts import build_dag_from_workflow as workflow_dag

    calls = []

    async def fake_scan(session, path, parameter_bindings=None):
        calls.append((path, dict(parameter_bindings or {})))
        return True, []

    monkeypatch.setattr(workflow_dag, "scan_external_notebook", fake_scan)
    tasks = [{
        "task_key": "master",
        "notebook_path": "/Workspace/Jobs/master.ipynb",
        "parameters": [
            {"key": "notebook_root", "value": "/Workspace/task-default"},
            {"key": "task_only", "value": "kept"},
        ],
    }]

    result = asyncio.run(workflow_dag.discover_run_deps_for_tasks(
        object(), tasks,
        parameter_overrides={
            "notebook_root": "/Workspace/PARAM_OVERRIDE",
            "job_only": "present",
        },
    ))

    assert result == {"master": []}
    assert calls == [(
        "/Workspace/Jobs/master.ipynb",
        {
            "notebook_root": "/Workspace/PARAM_OVERRIDE",
            "task_only": "kept",
            "job_only": "present",
        },
    )]


def test_cluster_scan_code_serializes_parameters_and_remains_valid_python():
    from scripts import build_dag

    class CaptureSession:
        code = ""

        async def _execute_locked(self, code, timeout=60):
            self.code = code
            compile(code, "<cluster-scan>", "exec")
            return {"outputs": [{
                "type": "stream",
                "text": '{"exists": false, "runs": []}\n',
            }]}

    session = CaptureSession()
    result = asyncio.run(build_dag.scan_external_notebook(
        session,
        "/Workspace/Jobs/master.ipynb",
        parameter_bindings={"root": "/Workspace/O'Reilly/Jobs"},
    ))

    assert result == (False, [])
    assert "extract_run_targets" in session.code
    assert "O'Reilly" in session.code


def test_dbx_export_cli_accepts_repeatable_discovery_parameters():
    from dbx_export.__main__ import build_arg_parser

    args = build_arg_parser().parse_args([
        "--job-id", "123",
        "--param", "root=/Workspace/Jobs",
        "--param", "selector=a=b",
    ])

    assert args.param == ["root=/Workspace/Jobs", "selector=a=b"]
