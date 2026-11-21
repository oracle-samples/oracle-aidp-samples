"""Regression coverage for trailing-slash notebook dependencies."""

import asyncio
import ast
import builtins
import importlib.util
import json
import os
import sys
import types
from pathlib import Path
from unittest import mock

import pytest


def _load_build_dag():
    """Load build_dag with temporary dependency stubs.

    The stubs must exist only while this private module instance is executed.
    Leaving them in ``sys.modules`` poisons later tests that need the real
    ``aidp_executor`` and ``context_tools`` modules.
    """
    aidp_executor_stub = types.ModuleType("aidp_executor")
    aidp_executor_stub.AIDPSession = object
    context_tools_stub = types.ModuleType("context_tools")
    context_tools_stub._unwrap_aidp_text = lambda value: value

    source = Path(__file__).resolve().parent.parent / "scripts" / "build_dag.py"
    spec = importlib.util.spec_from_file_location(
        "_test_build_dag_trailing_slash_module", source
    )
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(
        sys.modules,
        {
            "aidp_executor": aidp_executor_stub,
            "context_tools": context_tools_stub,
        },
    ):
        spec.loader.exec_module(module)
    return module


BUILD_DAG = _load_build_dag()


class _ExecutingSession:
    """Execute the scanner's generated cluster code against a virtual notebook."""

    def __init__(self, notebook_path, notebook, extra_notebooks=None):
        self.notebook_path = notebook_path
        self.notebooks = {notebook_path: notebook, **(extra_notebooks or {})}
        self.payloads = []

    async def _execute_locked(self, code, timeout=60):
        namespace = {}
        original_exists = os.path.exists
        original_isfile = os.path.isfile
        original_open = open

        def fake_exists(path):
            return path in self.notebooks or original_exists(path)

        def fake_isfile(path):
            return path in self.notebooks or original_isfile(path)

        class _NotebookFile:
            def __init__(inner_self, notebook):
                inner_self.notebook = notebook

            def __enter__(inner_self):
                return inner_self

            def __exit__(inner_self, *_args):
                return False

            def read(inner_self):
                return json.dumps(inner_self.notebook)

        def fake_open(path, *args, **kwargs):
            if path in self.notebooks:
                if isinstance(self.notebooks[path], Exception):
                    raise self.notebooks[path]
                return _NotebookFile(self.notebooks[path])
            return original_open(path, *args, **kwargs)

        output = []

        def fake_print(value):
            output.append(str(value))

        os.path.exists = fake_exists
        os.path.isfile = fake_isfile
        try:
            exec(
                compile(code, "<generated-build-dag-scan>", "exec"),
                {
                    **namespace,
                    "__builtins__": {
                        **vars(builtins),
                        "open": fake_open,
                        "print": fake_print,
                    },
                    "os": os,
                },
            )
        finally:
            os.path.exists = original_exists
            os.path.isfile = original_isfile
        self.payloads.append(json.loads(output[-1]))
        return {"outputs": [{"type": "stream", "text": "\n".join(output)}]}


def test_scan_external_notebook_strips_trailing_slash_before_ipynb():
    notebook_path = "/Workspace/root.ipynb"
    notebook = {
        "metadata": {"language_info": {"name": "python"}},
        "cells": [
            {
                "cell_type": "code",
                "source": [
                    "%run /Users/user5@example.com/Imports/\n",
                    'dbutils.notebook.run("/Users/example/DbutilsChild/", 60)\n',
                    'oidlUtils.notebook.run("/Users/example/OidlChild/", 60)\n',
                ],
            }
        ],
    }
    session = _ExecutingSession(notebook_path, notebook)

    exists, runs = asyncio.run(
        BUILD_DAG.scan_external_notebook(session, notebook_path)
    )

    assert exists is True
    assert "/Users/user5@example.com/Imports.ipynb" in runs
    assert "/Users/example/DbutilsChild.ipynb" in runs
    assert "/Users/example/OidlChild.ipynb" in runs
    assert not any("/.ipynb" in path for path in runs)


def test_scan_external_notebook_resolves_path_from_percent_run_config():
    parent_path = "/Workspace/exported/Project/Jobs/main.ipynb"
    config_path = "/Workspace/exported/Project/Jobs/Config/0_Config.ipynb"
    parent = {
        "cells": [{
            "cell_type": "code",
            "source": (
                "%run ./Config/0_Config\n"
                'cohort = dbutils.widgets.get("cohort")\n'
                'query_path = cohort_config[cohort]["fresh_base_query_path"]\n'
                "dbutils.notebook.run(query_path, 0)\n"
            ),
        }],
    }
    config = {
        "cells": [{
            "cell_type": "code",
            "source": (
                "cohort_config = {\n"
                '    "segment_a": {"fresh_base_query_path": "./Queries/segment_a"},\n'
                '    "segment_b": {"fresh_base_query_path": "./Queries/segment_b"},\n'
                "}\n"
            ),
        }],
    }
    session = _ExecutingSession(
        parent_path, parent, extra_notebooks={config_path: config},
    )

    exists, runs = asyncio.run(BUILD_DAG.scan_external_notebook(session, parent_path))

    assert exists is True
    assert "/Workspace/exported/Project/Jobs/Queries/segment_a.ipynb" in runs
    assert "/Workspace/exported/Project/Jobs/Queries/segment_b.ipynb" in runs


def test_nested_percent_run_is_not_resolved_relative_to_root_task():
    """Guard against this PR's initial child-cell flattening regression.

    Import static config values, while scanning each nested %run in its own
    notebook's scope. The pre-PR base already preserved that nested scope;
    its gap was the widget-selected child supplied by the config namespace.
    """
    parent_path = "/Workspace/exported/Project/Jobs/main.ipynb"
    config_path = "/Workspace/exported/Project/Jobs/Config/0_Config.ipynb"
    parent = {
        "cells": [{
            "cell_type": "code",
            "source": (
                "%run ./Config/0_Config\n"
                "dbutils.notebook.run(query_path, 0)\n"
            ),
        }],
    }
    config = {
        "cells": [{
            "cell_type": "code",
            "source": (
                "%run ../Shared/notebook_utils\n"
                'query_path = "./Queries/segment_a"\n'
            ),
        }],
    }
    session = _ExecutingSession(
        parent_path, parent, extra_notebooks={config_path: config},
    )

    exists, runs = asyncio.run(BUILD_DAG.scan_external_notebook(session, parent_path))

    assert exists is True
    assert "/Workspace/exported/Project/Jobs/Config/0_Config.ipynb" in runs
    assert "/Workspace/exported/Project/Jobs/Queries/segment_a.ipynb" in runs
    assert "/Workspace/exported/Project/Shared/notebook_utils.ipynb" not in runs


def test_percent_run_child_notebook_run_uses_child_path_not_root_path():
    parent_path = "/Workspace/exported/Project/Jobs/main.ipynb"
    config_path = "/Workspace/exported/Project/Jobs/Config/0_Config.ipynb"
    parent = {"cells": [{"cell_type": "code", "source": "%run ./Config/0_Config\n"}]}
    config = {"cells": [{"cell_type": "code", "source": (
        'dbutils.notebook.run("./Helpers/h", 0)\n'
    )}]}
    session = _ExecutingSession(
        parent_path, parent, extra_notebooks={config_path: config},
    )

    _, root_runs = asyncio.run(BUILD_DAG.scan_external_notebook(session, parent_path))
    _, config_runs = asyncio.run(BUILD_DAG.scan_external_notebook(session, config_path))

    assert "/Workspace/exported/Project/Jobs/Helpers/h.ipynb" not in root_runs
    assert config_runs == ["/Workspace/exported/Project/Jobs/Config/Helpers/h.ipynb"]


def test_percent_run_absolute_workspace_config_contributes_namespace():
    parent_path = "/Workspace/exported/Project/Jobs/main.ipynb"
    config_path = "/Workspace/Shared/Config/cfg.ipynb"
    parent = {"cells": [{"cell_type": "code", "source": (
        "%run /Workspace/Shared/Config/cfg\n"
        "target = config[dbutils.widgets.get('selector')]\n"
        "dbutils.notebook.run(target, 0)\n"
    )}]}
    config = {"cells": [{"cell_type": "code", "source": (
        "config = {'a': '/Shared/Queries/a', 'b': '/Shared/Queries/b'}\n"
    )}]}
    session = _ExecutingSession(
        parent_path, parent, extra_notebooks={config_path: config},
    )

    exists, runs = asyncio.run(BUILD_DAG.scan_external_notebook(session, parent_path))

    assert exists is True
    assert "/Shared/Queries/a.ipynb" in runs
    assert "/Shared/Queries/b.ipynb" in runs


def test_normalize_ws_path_strips_trailing_slash():
    assert (
        BUILD_DAG._normalize_ws_path(
            "/Users/user5@example.com/Imports/"
        )
        == "/Workspace/Users/user5@example.com/Imports.ipynb"
    )


def test_extract_dependencies_ignores_whitespace_only_run_magic():
    notebook_path = "/Workspace/root.ipynb"
    notebook = {
        "metadata": {"language_info": {"name": "python"}},
        "cells": [{
            "cell_type": "code",
            "source": ["%run     \n", "print('still valid')\n"],
        }],
    }
    session = _ExecutingSession(notebook_path, notebook)

    result = asyncio.run(BUILD_DAG.extract_dependencies(session, "root.ipynb"))

    assert result["root.ipynb"]["runs"] == []
    assert result["root.ipynb"]["cells"] == 1
    assert result["root.ipynb"]["code_cells"] == 1


def _workflow_discovery():
    """Load the real workflow preflight functions without OCI startup effects."""
    from dbx_export.sanitizer import (
        rewrite_embedded_workspace_ref, sanitize_path,
    )
    from notebook_path_parameters import effective_task_parameters

    source = Path(__file__).resolve().parent.parent / "scripts" / "build_dag_from_workflow.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    names = {"_to_export_paths", "discover_run_deps_for_tasks"}
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and node.name in names]
    namespace = {
        **vars(BUILD_DAG),
        "_sanitize_path": sanitize_path,
        "_rewrite_embedded_workspace_ref": rewrite_embedded_workspace_ref,
        "effective_task_parameters": effective_task_parameters,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    return namespace["discover_run_deps_for_tasks"]


def _code_notebook(source):
    return {"cells": [{"cell_type": "code", "source": source}]}


@pytest.mark.parametrize("distinct_source", [False, True])
def test_workflow_discovers_verified_legacy_dependency_paths(distinct_source):
    source = "/Workspace/source/notebooks"
    target = "/Workspace/target/notebooks" if distinct_source else source
    parent_path = source + "/Shared/main.ipynb"
    child_path = source + '/Shared/Aug"26/cfg.ipynb'
    parent = _code_notebook("%run './Aug\"26/cfg'\n")
    child = _code_notebook("VALUE = 1\n")
    session = _ExecutingSession(parent_path, parent, {child_path: child})
    result = asyncio.run(_workflow_discovery()(
        session, [{"task_key": "main", "notebook_path": parent_path}],
        base_prefix=target, source_prefix=source,
    ))
    assert result["main"][0]["path"] == child_path
    assert result["main"][0]["exists"] is True
    assert BUILD_DAG.collect_missing_external(result) == []


@pytest.mark.parametrize("run_target,config_relative,query_root", [
    ("/Shared/Config/cfg", "Shared/Config/cfg.ipynb", "/Shared/Queries"),
    ("/Workspace/Shared/Config/cfg", "Shared/Config/cfg.ipynb", "/Shared/Queries"),
    ('"./My Config/cfg"', "Project/Jobs/My_Config/cfg.ipynb", "./Queries"),
    ('"./My Config/Workspace/cfg"', "Project/Jobs/My_Config/Workspace_/cfg.ipynb", "./Queries"),
])
def test_workflow_namespace_uses_export_layout_and_skips_missing_inferred_child(
    run_target, config_relative, query_root, capsys,
):
    base = "/Workspace/exported/notebooks"
    parent_path = base + "/Project/Jobs/main.ipynb"
    parent = _code_notebook(
        f"%run {run_target}\n"
        "target = config[dbutils.widgets.get('selector')]\n"
        "dbutils.notebook.run(target, 0)\n"
    )
    config = _code_notebook(
        f"config = {{'a': '{query_root}/a', 'b': '{query_root}/b'}}\n"
    )
    query_prefix = base + ("/Shared/Queries" if query_root.startswith("/") else "/Project/Jobs/Queries")
    session = _ExecutingSession(parent_path, parent, {
        base + "/" + config_relative: config,
        query_prefix + "/b.ipynb": _code_notebook("print('present')\n"),
    })

    result = asyncio.run(_workflow_discovery()(
        session, [{"task_key": "main", "notebook_path": parent_path}], base_prefix=base,
    ))

    by_path = {entry["path"]: entry for entry in result["main"]}
    assert by_path[query_prefix + "/b.ipynb"]["exists"] is True
    assert query_prefix + "/a.ipynb" not in by_path
    assert BUILD_DAG.collect_missing_external(result) == []
    assert "[WARN][loose-candidate-skip]" in capsys.readouterr().out
    assert session.payloads[0].get("namespace_skipped", []) == []


@pytest.mark.parametrize("config_value,reason", [
    (None, "not found"),
    (PermissionError("fixture cannot read config"), "unreadable"),
])
def test_namespace_skip_reports_target_candidates_and_reason(config_value, reason, capsys):
    parent_path = "/Workspace/exported/notebooks/Jobs/main.ipynb"
    config_path = "/Workspace/exported/notebooks/Jobs/cfg.ipynb"
    parent = _code_notebook("%run ./cfg\ndbutils.notebook.run(query_path, 0)\n")
    extra = {} if config_value is None else {config_path: config_value}
    session = _ExecutingSession(parent_path, parent, extra)

    assert asyncio.run(BUILD_DAG.scan_external_notebook(session, parent_path))[0] is True

    skipped = session.payloads[0]["namespace_skipped"]
    assert len(skipped) == 1
    assert skipped[0]["target"].endswith("/Jobs/cfg")
    assert config_path in skipped[0]["tried"]
    assert reason in skipped[0]["reason"]
    output = capsys.readouterr().out
    assert "[namespace-skip]" in output
    assert parent_path in output
    assert "static config values NOT imported" in output
    assert reason in output


@pytest.mark.parametrize("include_nested_and_parameter", [False, True])
def test_workflow_shared_config_variable_unions_children_before_preflight(include_nested_and_parameter):
    base = "/Workspace/exported/notebooks"
    parent_path = base + "/Jobs/main.ipynb"
    parent = _code_notebook(
        "%run ./cfg1\n%run ./cfg2\n"
        "dbutils.notebook.run(query_path, 0)\n"
    )
    extra = {
        base + "/Jobs/cfg1.ipynb": _code_notebook(
            ("%run ./nested\n" if include_nested_and_parameter else "")
            + "query_path = './q1'\n"
        ),
        base + "/Jobs/cfg2.ipynb": _code_notebook("query_path = './q2'\n"),
        base + "/Jobs/q2.ipynb": _code_notebook("print('present')\n"),
    }
    if include_nested_and_parameter:
        extra.update({
            base + "/Jobs/nested.ipynb": _code_notebook("query_path = './q_nested'\n"),
            base + "/Jobs/q0.ipynb": _code_notebook("print('parameter child')\n"),
            base + "/Jobs/q_nested.ipynb": _code_notebook("print('nested child')\n"),
        })
    session = _ExecutingSession(parent_path, parent, extra)

    result = asyncio.run(_workflow_discovery()(
        session, [{"task_key": "main", "notebook_path": parent_path}], base_prefix=base,
        parameter_overrides={"query_path": "./q0"} if include_nested_and_parameter else None,
    ))

    names = {"cfg1", "cfg2", "q1", "q2"}
    if include_nested_and_parameter:
        names.update({"q0", "q_nested"})
    assert {entry["path"] for entry in result["main"]} == {
        base + "/Jobs/" + name + ".ipynb" for name in names
    }
    assert BUILD_DAG.collect_missing_external(result) == [{
        "path": base + "/Jobs/q1.ipynb", "referenced_by": ["main"],
    }]


def test_workflow_namespace_strips_distinct_source_export_prefix():
    source = "/Workspace/source_export/notebooks"
    target = "/Workspace/target_export/notebooks"
    parent_path = source + "/Jobs/main.ipynb"
    parent = _code_notebook("%run ./cfg\ndbutils.notebook.run(query_path, 0)\n")
    session = _ExecutingSession(parent_path, parent, {
        target + "/Jobs/cfg.ipynb": _code_notebook("query_path = './child'\n"),
        target + "/Jobs/child.ipynb": _code_notebook("print('present')\n"),
    })

    result = asyncio.run(_workflow_discovery()(
        session, [{"task_key": "main", "notebook_path": parent_path}],
        base_prefix=target, source_prefix=source,
    ))

    assert {entry["path"] for entry in result["main"]} == {
        target + "/Jobs/cfg.ipynb", target + "/Jobs/child.ipynb",
    }
    assert BUILD_DAG.collect_missing_external(result) == []


def test_scan_reports_unknown_selector_through_variable_and_keeps_loose_candidate(capsys):
    path = "/Workspace/Jobs/main.ipynb"
    notebook = _code_notebook(
        "config = {'a': './nb_a', 'b': translate_path('./nb_b'), 'label': 'LIVE'}\n"
        "target = config[dbutils.widgets.get('selector')]\n"
        "dbutils.notebook.run(target, 0)\n"
    )

    exists, runs = asyncio.run(BUILD_DAG.scan_external_notebook(_ExecutingSession(path, notebook), path))

    assert exists is True
    assert set(runs) == {"/Workspace/Jobs/nb_a.ipynb", "/Workspace/Jobs/nb_b.ipynb"}
    output = capsys.readouterr().out
    assert "selector not statically known" in output
    assert "LIVE" in output


@pytest.mark.parametrize("selector,expected_children", [
    ("dbutils.widgets.get('selector')", {"nb_a"}),
    ("'a'", {"nb_a"}),
])
def test_workflow_imported_unknown_call_candidate_obeys_selected_config_branch(
    selector, expected_children, capsys,
):
    """An unresolved config value remains discoverable across %run import.

    An explicitly selected sibling must not inherit fallback candidates from
    the unknown call or the unknown-selector metadata in the same map.
    """
    base = "/Workspace/exported/notebooks"
    parent_path = base + "/Jobs/main.ipynb"
    parent = _code_notebook(
        "%run ./cfg\n"
        f"target = config[{selector}]\n"
        "dbutils.notebook.run(target, 0)\n"
    )
    config = _code_notebook(
        "metadata = {'description': 'job'}\n"
        "config = {'a': './nb_a', 'b': translate_path('./nb_b'), 'label': 'job',\n"
        "          'opaque': metadata[dbutils.widgets.get('metadata_selector')]}\n"
    )
    session = _ExecutingSession(parent_path, parent, {
        base + "/Jobs/cfg.ipynb": config,
        base + "/Jobs/nb_a.ipynb": _code_notebook("print('present')\n"),
    })

    result = asyncio.run(_workflow_discovery()(
        session, [{"task_key": "main", "notebook_path": parent_path}], base_prefix=base,
    ))

    assert {entry["path"] for entry in result["main"]} == {
        base + "/Jobs/" + name + ".ipynb" for name in {"cfg", *expected_children}
    }
    missing = BUILD_DAG.collect_missing_external(result)
    output = capsys.readouterr().out
    if selector.startswith("dbutils.widgets.get"):
        assert missing == []
        assert "selector not statically known" in output
        assert "[WARN][loose-candidate-skip]" in output
        assert base + "/Jobs/nb_b.ipynb" in output
    else:
        assert missing == []
        assert "selector not statically known" not in output


def test_workflow_imported_canonical_absolute_candidates_skip_missing_inferred(capsys):
    """Unknown selection accepts arbitrary DBX roots without inventing data deps."""
    base = "/Workspace/exported/notebooks"
    parent_path = base + "/Jobs/main.ipynb"
    parent = _code_notebook(
        "%run ./cfg\n"
        "selected = config[dbutils.widgets.get('selector')]\n"
        "target = selected\n"
        "dbutils.notebook.run(target, 0)\n"
    )
    config = _code_notebook(
        "config = {\n"
        "    'present': '/Jobs/child',\n"
        "    'missing': '/Optimiser_Utils/Randomizer trigger',\n"
        "    'volume': '/Volumes/main/default/model',\n"
        "    'dbfs': '/dbfs/mnt/data',\n"
        "    'mount': '/mnt/data',\n"
        "}\n"
    )
    session = _ExecutingSession(parent_path, parent, {
        base + "/Jobs/cfg.ipynb": config,
        base + "/Jobs/child.ipynb": _code_notebook("print('present')\n"),
    })

    result = asyncio.run(_workflow_discovery()(
        session, [{"task_key": "main", "notebook_path": parent_path}], base_prefix=base,
    ))

    missing_path = base + "/Optimiser_Utils/Randomizer_trigger.ipynb"
    assert {entry["path"] for entry in result["main"]} == {
        base + "/Jobs/cfg.ipynb", base + "/Jobs/child.ipynb",
    }
    assert BUILD_DAG.collect_missing_external(result) == []
    output = capsys.readouterr().out
    assert "selector not statically known" in output
    assert "[WARN][loose-candidate-skip]" in output
    assert missing_path in output
    for data_path in ("/Volumes/main/default/model", "/dbfs/mnt/data", "/mnt/data"):
        assert data_path in output
