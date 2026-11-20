"""The embedded dependency extractor must run without the repository importable."""

import base64
import json
import subprocess
import sys

from dbx_export.dependency_resolver import portable_run_extractor_source


def test_portable_extractor_runs_in_isolated_python_process():
    notebook = {
        "cells": [{
            "cell_type": "code",
            "source": "%run ./Utils\n%run     \n",
        }]
    }
    source = portable_run_extractor_source()
    payload = base64.b64encode(json.dumps(notebook).encode()).decode()
    program = (
        source
        + "\nimport base64, json\n"
        + f"_nb = json.loads(base64.b64decode({payload!r}))\n"
        + "print(json.dumps(extract_run_targets(_nb, '/Workspace/job/Main.ipynb')))\n"
    )

    result = subprocess.run(
        # Send source on stdin: Windows limits the entire command line to
        # 32,767 characters, below the portable extractor's generated size.
        [sys.executable, "-I", "-"],
        input=program,
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ["/Workspace/job/Utils"]


def test_portable_extractor_resolves_parameter_bindings_in_isolated_process():
    notebook = {
        "cells": [{
            "cell_type": "code",
            "source": (
                'root = dbutils.widgets.get("notebook_root")\n'
                'dbutils.notebook.run(f"{root}/Child", 0)\n'
            ),
        }]
    }
    source = portable_run_extractor_source()
    payload = base64.b64encode(json.dumps(notebook).encode()).decode()
    program = (
        source
        + "\nimport base64, json\n"
        + f"_nb = json.loads(base64.b64decode({payload!r}))\n"
        + "print(json.dumps(extract_run_targets(\n"
        + "    _nb, '/Workspace/job/Main.ipynb',\n"
        + "    parameter_bindings={'notebook_root': '/Workspace/job'},\n"
        + ")))\n"
    )

    result = subprocess.run(
        [sys.executable, "-I", "-"],
        input=program,
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ["/job/Child"]



def test_portable_extractor_embeds_optional_completeness_channel():
    source = portable_run_extractor_source()
    program = source + "\n" + '''
state = _StaticEvaluation()
values = _static_values(ast.parse('"/repo" or runtime_root()', mode="eval").body,
                        {}, {}, config_paths=True, completeness=state)
assert values == ["/repo"] and state.complete
state = _StaticEvaluation()
values = _static_values(ast.parse(
    '{"primary": "/repo/config", "other": runtime_root()}[runtime_choice()]',
    mode="eval").body, {}, {}, config_paths=True, completeness=state)
assert values == ["/repo/config"] and not state.complete
state = _StaticEvaluation()
values = _static_values(ast.parse('f"{ROOT:{SPEC}}"', mode="eval").body,
                        {"ROOT": ["../config"], "SPEC": [".8"]}, {}, completeness=state)
assert values == ["../config"] and not state.complete
state = _StaticEvaluation()
_static_values(ast.parse('{"enabled": True, "runtime": runtime_metadata()}', mode="eval").body,
               {}, {}, completeness=state)
assert state.truth_complete and not state.complete
print("optional channel passed")
'''
    result = subprocess.run([sys.executable, "-I", "-"], input=program,
                            text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "optional channel passed"
