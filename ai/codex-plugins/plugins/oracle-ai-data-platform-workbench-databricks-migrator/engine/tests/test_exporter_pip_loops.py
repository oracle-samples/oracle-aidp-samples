"""F3: export complete subprocess argv, never detached loop values.

Exercise both real notebook consumers and their dependency/report artifacts.
No customer code, subprocess, pip installation, or remote service is executed.
"""
import copy
import json
import shlex
import warnings
from pathlib import Path

import pytest

from dbx_export import __main__ as exporter
from test_exporter_pip_reporting import _graph, _report, _scan
from test_exporter_pip_source_context import _artifact


def _loop(items, tail="item", *, iterable="items", before="", body_before=""):
    return (f"items = {items!r}\n{before}for item in {iterable}:\n"
            f"{body_before}    subprocess.check_call([sys.executable, '-m', 'pip', "
            f"'install', {tail}])\n")


def _function(source, header="def install():"):
    return header + "\n" + "".join("    " + line + "\n" for line in source.splitlines())


CASES = [
    pytest.param(_loop(["pandas", "openpyxl==3.1.5"]),
                 {"pandas", "openpyxl==3.1.5"}, [], id="normal-packages"),
    pytest.param(_loop(["base.txt", "extras.txt"], "'-r', item"), set(),
                 ["-r base.txt", "-r extras.txt"], id="requirements-files"),
    pytest.param(_loop(["ignore", "warn"], "'--root-user-action', item, 'pandas'"),
                 {"pandas"}, [], id="option-values-not-packages"),
    pytest.param(_loop(["-r", "requirements.txt"]), set(),
                 ["missing option value"], id="broken-split-options"),
    pytest.param(_loop(["pandas openpyxl"]), set(),
                 ["not a valid package"], id="argv-items-not-shell-split"),
    pytest.param(_loop(["requests[security]>=2", "foo @ https://example.test/foo.whl"]),
                 {"requests[security]>=2", "foo @ https://example.test/foo.whl"}, [],
                 id="extras-and-direct-reference"),
    pytest.param(_loop(["foo.whl", "https://example.test/bar.zip"]),
                 {"foo.whl", "https://example.test/bar.zip"}, [], id="archives"),
    pytest.param(_loop(["old_pkg"], before="items = ['new_pkg']\n"),
                 {"new_pkg"}, [], id="assignment-before-loop"),
    pytest.param(_loop(["first_pkg"]) + "items = ['later_pkg']\n",
                 {"first_pkg"}, [], id="later-assignment-not-used"),
    pytest.param(_loop([], "item, 'never_installed'"), set(), [], id="empty-loop"),
    pytest.param(_loop(["old_pkg"], before="items = runtime_packages()\n"), set(),
                 ["cannot safely resolve"], id="dynamic-reassignment"),
    pytest.param(_loop(["old_pkg"], body_before="    item = runtime_package()\n"),
                 set(), ["cannot safely resolve"], id="loop-target-reassignment"),
    pytest.param(_loop(["old_pkg"], before="items.append('new_pkg')\n"), set(),
                 ["cannot safely resolve"], id="mutated-list"),
    pytest.param(_loop(["old_pkg"], iterable="sorted(items)"), set(),
                 ["cannot safely resolve"], id="unsupported-iterable"),
    pytest.param(_loop(["one_pkg"]) + _loop(["two_pkg"]),
                 {"one_pkg", "two_pkg"}, [], id="separate-loops"),
    pytest.param("%python\n%pip install magic_pkg\n" + _loop(["loop_pkg"]),
                 {"magic_pkg", "loop_pkg"}, [], id="mixed-magics"),
    pytest.param(_loop(["real_pkg"], "'--log', '/tmp/pip.log', item, '--quiet'"),
                 {"real_pkg"}, [], id="literal-options-surround-package"),
    pytest.param(_loop(["real_pkg"], "'-r', 'base.txt', item"), set(),
                 ["-r base.txt"], id="unresolved-loop-not-partially-harvested"),
    pytest.param(_loop(["real_pkg"], iterable="['inline_pkg']"),
                 {"inline_pkg"}, [], id="inline-list"),
    pytest.param(_loop(["old_pkg"], before="alias = items\n"), set(),
                 ["cannot safely resolve"], id="aliased-iterable"),
    *[
        pytest.param(
            _function(_loop(["real_pkg"], before=statement + "\n") if placement == "before"
                      else _loop(["real_pkg"], body_before="    " + statement + "\n")),
            {"real_pkg"} if supported else set(),
            [] if supported else ["cannot safely resolve"],
            id=f"{label}-{placement}",
        )
        for placement in ("before", "body")
        for label, statement, supported in [
            ("formatted-diagnostic", 'print(f"Installing {items}")', True),
            ("formatted-repr", 'print(f"Installing {items!r}")', True),
            ("formatted-ascii", 'print(f"Installing {items!a}")', True),
            ("formatted-string", 'print(f"Installing {items!s}")', True),
            ("saved-diagnostic", 'message = f"Installing {items}"', False),
            ("escaped-format", 'print(f"Installing {{packages}} {items}")', True),
            ("list-escapes-to-call", 'print(items)', False),
            ("format-call", 'print("Installing {}".format(items))', False),
            ("mutation-in-diagnostic", 'print(f"{items.pop()}")', False),
            ("alias-in-diagnostic", 'print(f"{(alias := items)}")', False),
            ("rebind-in-diagnostic", 'print(f"{(items := other)}")', False),
            ("mixed-safe-unsafe", 'print(f"{items}", items)', False),
            ("dynamic-format", 'print(f"{items:{width}}")', False),
        ]
    ],
    pytest.param(_loop(["old_pkg"], before="if flag:\n    items = ['new_pkg']\n"),
                 set(), ["cannot safely resolve"], id="conditional-assignment"),
    pytest.param("items = ['old_pkg']\nif (items := runtime_packages()):\n"
                 "    for item in items:\n"
                 "        subprocess.run(['pip', 'install', item])\n", set(),
                 ["cannot safely resolve"], id="header-reassignment"),
    pytest.param("items = ['old_pkg']\ntry:\n    items = runtime_packages()\n"
                 "except Exception:\n    for item in items:\n"
                 "        subprocess.run(['pip', 'install', item])\n", set(),
                 ["cannot safely resolve"], id="exception-path-binding"),
    pytest.param("items = ['outer_pkg']\ndef install(items):\n"
                 "    for item in items:\n"
                 "        subprocess.run(['pip', 'install', item])\n", set(),
                 ["cannot safely resolve"], id="function-parameter-shadows-list"),
    pytest.param("for item in ['outer_pkg']:\n    def install(item):\n"
                 "        subprocess.run(['pip', 'install', item])\n", set(),
                 ["cannot safely resolve"], id="function-shadows-loop-target"),
    pytest.param("for item in ['real_pkg']:\n    try:\n"
                 "        subprocess.run(['pip', 'install', item])\n"
                 "    except Exception:\n        pass\n", {"real_pkg"}, [],
                 id="try-wrapped-call"),
    pytest.param("def install():\n    items = ['real_pkg']\n    try:\n"
                 "        for item in items:\n"
                 "            subprocess.run(['pip', 'install', item])\n"
                 "    except Exception:\n        pass\n", {"real_pkg"}, [],
                 id="function-local-list-and-try"),
    pytest.param(_loop(["real_pkg"], body_before="    # " + "x" * 600 + "\n"),
                 {"real_pkg"}, [], id="no-character-window"),
    pytest.param("items = ['not_a_package']\nfor item in items:\n    print(item)\n"
                 "subprocess.run(['pip', 'install', '--log', item, 'real_pkg'])\n",
                 set(), ["cannot safely resolve"], id="call-outside-loop"),
    pytest.param("for item in ['outer_pkg']:\n    for other in ['inner_pkg']:\n"
                 "        subprocess.run(['pip', 'install', item, other])\n",
                 set(), ["cannot safely resolve"], id="nested-loop-not-guessed"),
    pytest.param(_loop(["real_pkg"]).replace(
        "'install', item", "'install', f'{item}==1.0'"), set(),
                 ["cannot safely resolve"], id="expressions-not-evaluated"),
    pytest.param(_loop(["https://example.test/a,b.whl"]),
                 {"https://example.test/a,b.whl"}, [], id="comma-in-argv"),
    pytest.param(_loop(['real_pkg; python_version >= "3.11"']),
                 {'real_pkg; python_version >= "3.11"'}, [], id="quoted-marker"),
    pytest.param(_loop(["first_pkg"]).replace("sys.executable, '-m', 'pip', ",
                 "sys.executable,\n        '-m', 'pip', "),
                 {"first_pkg"}, [], id="multiline-argv"),
    pytest.param(_loop(["-r", "requirements.txt"])
                 + "subprocess.run(['pip', 'install', 'independent_pkg'])\n",
                 {"independent_pkg"}, ["missing option value"],
                 id="independent-call-survives-broken-loop"),
    pytest.param("for item in ['real_pkg']:\n"
                 "    subprocess.run(['pip', 'install', item, '--log', 'pip.log'])\n"
                 "    subprocess.run(['pip', 'install', '--log', item, 'second_pkg'])\n",
                 {"real_pkg", "second_pkg"}, [], id="two-calls-distinct-argv-roles"),
    pytest.param("x =\n" + _loop(["unknown_pkg"])
                 + "subprocess.run(['pip', 'install', 'independent_pkg'])\n",
                 {"independent_pkg"}, ["cannot safely resolve"],
                 id="syntax-error-no-regex-loop-guess"),
    pytest.param(_loop(["old_pkg"], body_before="    import other as item\n"),
                 set(), ["cannot safely resolve"], id="import-rebinds-loop-target"),
    pytest.param(_loop(["old_pkg"], body_before="    def item():\n        pass\n"),
                 set(), ["cannot safely resolve"], id="function-rebinds-loop-target"),
    pytest.param("for item in ['old_pkg']:\n    try:\n        risky()\n"
                 "    except Exception as item:\n"
                 "        subprocess.run(['pip', 'install', item])\n",
                 set(), ["cannot safely resolve"], id="exception-rebinds-loop-target"),
    pytest.param(_loop(["old_pkg"], before="import other as items\n"),
                 set(), ["cannot safely resolve"], id="import-rebinds-list"),
    pytest.param("HELP = 'text\u2028%python'\n" + _loop(["real_pkg"]),
                 {"real_pkg"}, [], id="unicode-line-separator-in-string"),
]


FORMATTED_USES = [
    pytest.param('print(f"Installing {items!r}")', True, id="builtin-print"),
    pytest.param('print(f"{items!s:>20}")', True, id="static-string-format"),
    pytest.param('exec(f"items = {items!r} + [\'new_pkg\']")', False, id="exec"),
    pytest.param('eval(f"(items := {items!r} + [\'new_pkg\'])")', False, id="eval"),
    pytest.param('code = f"items = {items!r} + [\'new_pkg\']"\nexec(code)',
                 False, id="saved-then-executed"),
    pytest.param('execute = exec\nexecute(f"items = {items!r} + [\'new_pkg\']")',
                 False, id="exec-alias"),
    pytest.param('exec(f"items.clear() # {items!r}")', False, id="mutation"),
    pytest.param('consume(f"{items}")', False, id="arbitrary-call"),
    pytest.param('print = exec\nprint(f"items = {items!r} + [\'new_pkg\']")',
                 False, id="rebound-print"),
    pytest.param('from custom import output as print\nprint(f"{items}")',
                 False, id="imported-print"),
    pytest.param('from custom import *\nprint(f"{items}")',
                 False, id="wildcard-import"),
    pytest.param('print(f"{items}", file=sink)', False, id="custom-output"),
    pytest.param('print(f"{items}", mutate())', False, id="additional-argument"),
    pytest.param('print(f"{items} {mutate()}")', False, id="additional-expression"),
    pytest.param('print(f"{items:{width}}")', False, id="dynamic-format"),
    pytest.param('result = print(f"{items}")', False, id="non-standalone-print"),
]


@pytest.mark.parametrize("statement,supported", FORMATTED_USES)
@pytest.mark.parametrize("scope", ["module", "function"])
@pytest.mark.parametrize("placement", ["before", "body", "compound"])
@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_formatted_list_use_preserves_requirements_and_unresolved_barrier(
        tmp_path, capsys, statement, supported, placement, consumer, child, scope):
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    if placement == "before":
        source = _loop(["old_pkg"], before=statement + "\n")
    elif placement == "body":
        source = _loop(["old_pkg"], body_before="".join(
            "    " + line + "\n" for line in statement.splitlines()))
    else:
        # Exercise _pip_loop_items' scan of another suite in a compound node.
        source = ("items = ['old_pkg']\ntry:\n"
                  + "".join("    " + line + "\n" for line in statement.splitlines())
                  + "except Exception:\n    for item in items:\n"
                    "        subprocess.run(['pip', 'install', item])\n")
    if scope == "function":
        source = _function(source)
    supported = supported and scope == "function"
    if scope == "function" and "from custom import *" in statement:
        # Python disallows function-local wildcard imports; the diagnostic
        # must remain unresolved when the scope cannot be compiled.
        with pytest.raises(SyntaxError):
            compile(source, "<formatted-list-regression>", "exec")
    else:
        compile(source, "<formatted-list-regression>", "exec")
    graph, path = _graph([
        {"cell_type": "code", "source": source},
        {"cell_type": "code", "source": "%pip install independent_pkg"},
        {"cell_type": "code", "source": "import matplotlib"},
    ], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    names = {canonicalize_name(Requirement(line).name)
             for node in graph.files.values() for line in node.content.decode().splitlines()
             if line.strip() and not line.startswith("#")}
    assert "independent-pkg" in names
    assert ("old-pkg" in names) is supported
    assert "new-pkg" not in names
    if consumer != "discovery":
        assert ("matplotlib" in names) is supported
        assert ("pip-inference-barrier" in capsys.readouterr().out) is not supported
    saved = json.loads(Path(report.write(str(tmp_path))).read_text())
    assert saved["summary"]["pip_unresolved"] == (0 if supported else 1)
    assert all(f["notebook_path"] == path and f["cell_index"] == 0
               and f["status"] == "UNRESOLVED"
               and "cannot safely resolve" in f["reason"] for f in saved["pip_findings"])
    assert graph.notebooks == original


@pytest.mark.parametrize("signature", ["print", "*print", "**print", "*, print"])
@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
def test_print_parameter_does_not_receive_diagnostic_exemption(signature, consumer):
    source = (f"def install({signature}):\n"
              + "".join("    " + line + "\n" for line in _loop(
                  ["old_pkg"], before='print(f"{items}")\n').splitlines()))
    graph, _ = _graph([{"cell_type": "code", "source": source}])
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    assert report.summary()["pip_unresolved"] == 1


def _conditional_installer():
    # Run cf1893d0, 2026-10-19: a read-only f-string between the literal
    # assignment and loop hid these explicit dependencies from both scans.
    return '''def import_libraries():
    try:
        from slack_sdk import WebClient
        from knockknock import slack_sender
        import openpyxl
    except:
        import sys, subprocess
        libraries_to_install = ["slack_sdk", "knockknock", "openpyxl"]
        print(f"Installing libraries {libraries_to_install} in the cluster")
        for each_library in libraries_to_install:
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', each_library, '--quiet'])
            except:
                print(f"Failed to install library : {each_library}")
        import_libraries()
import_libraries()
'''


CASES.append(pytest.param(
    _conditional_installer(), {"slack_sdk", "knockknock", "openpyxl"}, [],
    id="incident-conditional-installer-with-diagnostic",
))


@pytest.mark.parametrize("source,expected,reasons", CASES)
@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_loop_dependency_and_final_report_artifacts(
        tmp_path, source, expected, reasons, consumer, child):
    graph, path = _graph([{"cell_type": "code", "source": source}], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        detected = {spec for spec, _ in exporter._notebook_cell_pip_specs(
            {"cell_type": "code", "source": source})}
        assert detected == expected
        _scan(graph, consumer, report)
    assert graph.notebooks == original
    dependencies = {line.strip() for node in graph.files.values()
                    for line in node.content.decode().splitlines()
                    if line.strip() and not line.startswith("#")}
    # Consolidation adds runtime baselines and normalizes specs. Compare with
    # equivalent literal magic commands through that same production pipeline.
    expected_graph, _ = _graph([{"cell_type": "code", "source":
        "\n".join("%pip install " + shlex.quote(spec) for spec in sorted(expected))}], child)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(expected_graph, consumer, None)
    expected_dependencies = {line.strip() for node in expected_graph.files.values()
                             for line in node.content.decode().splitlines()
                             if line.strip() and not line.startswith("#")}
    assert dependencies == expected_dependencies
    saved = json.loads(Path(report.write(str(tmp_path))).read_text())
    findings = saved["pip_findings"]
    assert len(findings) == len(reasons)
    for reason in reasons:
        assert any(reason in f["reason"] for f in findings)
    assert all(f["notebook_path"] == path and f["cell_index"] == 0
               and f["status"] == "UNRESOLVED" for f in findings)


def test_unresolved_loop_does_not_discard_independent_literal_command():
    source = (_loop(["-r", "requirements.txt"])
              + "subprocess.run(['pip', 'install', 'real_pkg'])")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        assert exporter._extract_subprocess_pip_specs(source) == ["real_pkg"]


@pytest.mark.parametrize("launcher", ["python", "python3", "python3.11"])
@pytest.mark.parametrize("quote", ["'", '"'])
def test_shell_init_script_literal_python_heredoc_loop_is_preserved(launcher, quote):
    script = (f"#!/bin/bash\nset -e\n{launcher} - <<{quote}PY{quote}\n"
              + _loop(["init_required_pkg"]) + "PY\n")
    _, names = _artifact([], "consolidation", init_source=script)
    assert "init-required-pkg" in names


def test_multiple_python_heredocs_keep_separate_bindings_and_commands():
    script = ("python - <<'FIRST'\n" + _loop(["one_pkg"]) + "FIRST\n"
              "python3 - <<'SECOND'\n" + _loop(["two_pkg"]) + "SECOND\n")
    assert exporter._extract_subprocess_pip_specs(script) == ["one_pkg", "two_pkg"]


@pytest.mark.parametrize("consumer", ["consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("list_state", ["known", "unknown", "mutated", "aliased"])
def test_diagnostic_list_resolution_preserves_import_inference_barrier(
        consumer, child, list_state, capsys):
    # Live CLI dry-runs on 2026-10-19 reproduced the missing explicit packages
    # AND suppressed matplotlib/sklearn inference. Resolve both only when the
    # complete literal install list is known; retain the barrier otherwise.
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    source = _conditional_installer()
    if list_state == "unknown":
        source = source.replace(
            '["slack_sdk", "knockknock", "openpyxl"]', 'runtime_packages()')
    elif list_state in {"mutated", "aliased"}:
        statement = ('libraries_to_install.append("new_pkg")'
                     if list_state == "mutated" else 'alias = libraries_to_install')
        source = source.replace(
            '        for each_library in libraries_to_install:',
            '        ' + statement + '\n        for each_library in libraries_to_install:')
    graph, _ = _graph([
        {"cell_type": "code", "source": source},
        {"cell_type": "code", "source": "import matplotlib\nimport sklearn"},
    ], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    names = {
        canonicalize_name(Requirement(line).name)
        for uri, node in graph.files.items()
        if uri.startswith(exporter.CANONICAL_REQUIREMENTS_PREFIX)
        for line in node.content.decode().splitlines()
        if line.strip() and not line.startswith("#")
    }
    affected = {"slack-sdk", "knockknock", "openpyxl", "matplotlib", "scikit-learn"}
    known = list_state == "known"
    if known:
        assert affected <= names
        assert not any("unknown-python-import" in error for error in report.errors)
    else:
        assert not affected.intersection(names)
    assert bool(report.pip_findings) is not known
    assert ("pip-inference-barrier" in capsys.readouterr().out) is not known
    assert graph.notebooks == original


CROSS_CELL_PRINT_BINDINGS = [
    pytest.param('def print(message):\n    items.append("new_pkg")\n', id="function"),
    pytest.param('print = custom_output', id="assignment"),
    pytest.param('from custom import output as print', id="from-import"),
    pytest.param('import custom as print', id="import-alias"),
    pytest.param('from custom import *', id="wildcard-import"),
    pytest.param('def helper(print):\n    pass', id="parameter"),
    pytest.param('del print', id="delete"),
    pytest.param('for print in callbacks:\n    pass', id="loop-target"),
    pytest.param('try:\n    operation()\nexcept Exception as print:\n    pass', id="except-target"),
    pytest.param('class print:\n    pass', id="class"),
    pytest.param('with output_context() as print:\n    pass', id="with-target"),
    pytest.param('if (print := custom_output):\n    pass', id="walrus"),
]


@pytest.mark.parametrize("binding", CROSS_CELL_PRINT_BINDINGS)
@pytest.mark.parametrize("position", ["before", "after"])
@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_notebook_print_binding_keeps_unresolved_and_inference_barrier(
        tmp_path, capsys, binding, position, consumer, child):
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    # PR #273 review 6283038716: a static binding in a separate cell bypassed
    # the cell-only check. Module lists now never receive the exception,
    # regardless of where a potential print binding appears.
    install = _loop(["old_pkg"], before='print(f"Installing {items}")\n')
    sources = [binding, install] if position == "before" else [install, binding]
    cells = [{"cell_type": "code", "source": s} for s in sources]
    cells.extend([
        {"cell_type": "code", "source": "%pip install independent_pkg"},
        {"cell_type": "code", "source": "import matplotlib"},
    ])
    graph, path = _graph(cells, child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    names = {canonicalize_name(Requirement(line).name)
             for node in graph.files.values() for line in node.content.decode().splitlines()
             if line.strip() and not line.startswith("#")}
    assert "independent-pkg" in names
    assert not {"old-pkg", "new-pkg", "matplotlib"}.intersection(names)
    if consumer != "discovery":
        assert "pip-inference-barrier" in capsys.readouterr().out
    saved = json.loads(Path(report.write(str(tmp_path))).read_text())
    assert saved["summary"]["pip_unresolved"] == 1
    assert saved["pip_findings"][0]["notebook_path"] == path
    assert saved["pip_findings"][0]["cell_index"] == (1 if position == "before" else 0)
    assert "cannot safely resolve" in saved["pip_findings"][0]["reason"]
    assert graph.notebooks == original


@pytest.mark.parametrize("cell,supported", [
    pytest.param({"cell_type":"markdown", "source":"def print(message): pass"}, True, id="markdown"),
    pytest.param({"cell_type":"raw", "source":"print = exec"}, True, id="raw"),
    pytest.param({"cell_type":"code", "source":"# print = exec"}, True, id="comment"),
    pytest.param({"cell_type":"code", "source":'HELP = "print = exec"'}, True, id="string"),
    pytest.param({"cell_type":"code", "source":'HELP = """\n%python\nprint = exec\n"""'}, True, id="multiline-string"),
    pytest.param({"cell_type":"code", "source":"# annotation\n%md\nprint = exec"}, True, id="markdown-magic"),
    pytest.param({"cell_type":"code", "source":"%md-sandbox\nprint = exec"}, True, id="markdown-sandbox"),
    pytest.param({"cell_type":"code", "source":["%python\n", "print = custom_output\n"]}, True, id="python-magic-lines"),
    pytest.param({"cell_type":"code", "source":"%pip install independent_pkg\nprint = custom_output"}, True, id="pip-magic-binding"),
    pytest.param({"cell_type":"code", "source":"def print("}, True, id="unparsed-binding"),
    pytest.param({"cell_type":"code", "source":"x ="}, True, id="unparsed-context"),
])
@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_unrelated_cells_do_not_disable_function_local_diagnostic(
        cell, supported, consumer, child):
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    graph, _ = _graph([
        cell,
        {"cell_type":"code", "source":_function(_loop(["old_pkg"], before='print(f"{items}")\n'))},
        {"cell_type":"code", "source":"%pip install independent_pkg"},
        {"cell_type":"code", "source":"import matplotlib"},
    ], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    names = {canonicalize_name(Requirement(line).name)
             for node in graph.files.values() for line in node.content.decode().splitlines()
             if line.strip() and not line.startswith("#")}
    assert "independent-pkg" in names
    assert ("old-pkg" in names) is supported
    if consumer != "discovery":
        assert ("matplotlib" in names) is supported
    assert report.summary()["pip_unresolved"] == (0 if supported else 1)
    assert graph.notebooks == original


@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_print_binding_context_does_not_leak_between_notebooks(consumer, child):
    from dbx_export.dependency_resolver import NotebookNode

    graph, _ = _graph([{"cell_type":"code", "source":_function(_loop(
        ["old_pkg"], before='print(f"{items}")\n'))}], child)
    graph.notebooks["/unrelated"] = NotebookNode(
        dbx_path="/unrelated", role="task", referenced_by=[],
        nb_dict={"cells":[{"cell_type":"code", "source":"print = custom_output"}]})
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    assert report.summary()["pip_unresolved"] == 0
    assert any("old_pkg" in node.content.decode() or "old-pkg" in node.content.decode()
               for node in graph.files.values())


def test_cross_cell_print_mutation_requests_more_than_the_original_literal_list():
    from types import SimpleNamespace

    requests = []
    namespace = {"subprocess":SimpleNamespace(check_call=lambda argv: requests.append(argv))}
    exec('def print(message):\n    items.append("new_pkg")\n', namespace)
    exec(_loop(["old_pkg"], before='print(f"Installing {items}")\n').replace(
        "sys.executable, '-m', ", ""), namespace)
    assert [argv[-1] for argv in requests] == ["old_pkg", "new_pkg"]


_LOCAL_DIAGNOSTIC = _loop(["old_pkg"], before='print(f"Installing {items}")\n')
FUNCTION_SCOPE_CASES = [
    pytest.param(_function(_LOCAL_DIAGNOSTIC), True, id="fresh-local"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC, "async def install():"), True, id="async-local"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC, "def install(items):"), True, id="fresh-assignment-replaces-parameter"),
    pytest.param('@decorate\n' + _function(_LOCAL_DIAGNOSTIC), True, id="decorated-function"),
    pytest.param(_function(_function(_LOCAL_DIAGNOSTIC)), True, id="nested-function"),
    pytest.param(_function(_function(_LOCAL_DIAGNOSTIC), "class Installer:"), True, id="nested-method"),
    pytest.param(_function(_loop(("old_pkg",), before='print(f"{items!r}")\n')), True, id="local-tuple"),
    pytest.param('await prepare()\n' + _function(_LOCAL_DIAGNOSTIC), True, id="top-level-await"),
    pytest.param(_LOCAL_DIAGNOSTIC, False, id="notebook-global"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC, "class Installer:"), False, id="class-namespace"),
    pytest.param(_function('global items\n' + _LOCAL_DIAGNOSTIC), False, id="explicit-global"),
    pytest.param('def outer():\n    items = []\n' + ''.join(
        '    ' + line + '\n' for line in _function('nonlocal items\n' + _LOCAL_DIAGNOSTIC).splitlines()), False, id="nonlocal"),
    pytest.param(_function('def capture():\n    return items\n' + _LOCAL_DIAGNOSTIC), False, id="closure-before-assignment"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC + 'def capture():\n    return items\n'), False, id="closure-after-loop"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC + 'capture = lambda: items\n'), False, id="lambda-capture"),
    pytest.param(_function(_loop(["old_pkg"], before='alias = items\nprint(f"{items}")\n')), False, id="alias"),
    pytest.param(_function(_loop(["old_pkg"], before='publish(items)\nprint(f"{items}")\n')), False, id="escaped-list"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC, "def install(print):"), False, id="local-print-parameter"),
    pytest.param(_function(_loop(["old_pkg"], before='items.append("new_pkg")\nprint(f"{items}")\n')), False, id="mutated-list"),
    pytest.param(_function(_LOCAL_DIAGNOSTIC.replace("['old_pkg']", "runtime_packages()")), False, id="unknown-list"),
]


@pytest.mark.parametrize("source,supported", FUNCTION_SCOPE_CASES)
@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_diagnostic_exception_requires_fresh_uncaptured_function_local_list(
        tmp_path, capsys, source, supported, consumer, child):
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    graph, path = _graph([
        {"cell_type":"code", "source":"from pyspark.sql.types import *"},
        {"cell_type":"code", "source":source},
        {"cell_type":"code", "source":"%pip install independent_pkg"},
        {"cell_type":"code", "source":"import matplotlib"},
    ], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _scan(graph, consumer, report)
    names = {canonicalize_name(Requirement(line).name)
             for node in graph.files.values() for line in node.content.decode().splitlines()
             if line.strip() and not line.startswith('#')}
    assert 'independent-pkg' in names
    assert ('old-pkg' in names) is supported
    if consumer != 'discovery':
        assert ('matplotlib' in names) is supported
        assert ('pip-inference-barrier' in capsys.readouterr().out) is not supported
    saved = json.loads(Path(report.write(str(tmp_path))).read_text())
    assert saved['summary']['pip_unresolved'] == (0 if supported else 1)
    assert all(f['notebook_path'] == path and f['cell_index'] == 1
               and f['status'] == 'UNRESOLVED' for f in saved['pip_findings'])
    assert graph.notebooks == original


@pytest.mark.parametrize("consumer", ["discovery", "consolidation", "both"])
@pytest.mark.parametrize("child", [False, True])
def test_customer_installer_with_prior_wildcard_preserves_explicit_and_inferred_packages(
        consumer, child, capsys):
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    graph, _ = _graph([
        {"cell_type":"code", "source":"from pyspark.sql.types import *"},
        {"cell_type":"code", "source":_conditional_installer()},
        {"cell_type":"code", "source":"import matplotlib\nimport sklearn"},
    ], child)
    original = copy.deepcopy(graph.notebooks)
    report = _report()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        _scan(graph, consumer, report)
    names = {canonicalize_name(Requirement(line).name)
             for node in graph.files.values() for line in node.content.decode().splitlines()
             if line.strip() and not line.startswith('#')}
    assert {'slack-sdk','knockknock','openpyxl'} <= names
    if consumer != 'discovery':
        assert {'matplotlib','scikit-learn'} <= names
    assert report.summary()['pip_unresolved'] == 0
    assert 'pip-inference-barrier' not in capsys.readouterr().out
    assert graph.notebooks == original
