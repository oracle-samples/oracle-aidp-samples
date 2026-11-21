"""Documentation must never become an installed cluster dependency.

Exercise both exporter consumers and the actual consolidated artifact, not
just regex matches. The notebook graph is in memory; nothing is installed.
"""
import copy
import warnings

import pytest
from packaging.requirements import Requirement

from dbx_export import __main__ as exporter
from dbx_export.dependency_resolver import FileNode, Graph, JobNode, NotebookNode


COMMANDS = [
    "%pip install {pkg}",
    "!pip install {pkg}",
    "!pip3 install {pkg}",
    "!python -m pip install {pkg}",
    "!python3 -m pip install {pkg}",
    'subprocess.check_call([sys.executable, "-m", "pip", "install", "{pkg}"])',
    'os.system("pip install {pkg}")',
    'pip.main(args=["install", "{pkg}"])',
    'libraries = ["{pkg}"]\nfor lib in libraries:\n'
    '    subprocess.check_call([sys.executable, "-m", "pip", "install", lib])',
]


def _artifact(cells, consumer, *, child=False, init_source=None):
    graph = Graph()
    graph.jobs["42"] = JobNode(dbx_job_id="42", depth=0, job_def={"settings": {
        "job_clusters": [{"job_cluster_key": "cpu", "new_cluster": {}}],
        "tasks": [{"task_key": "task", "job_cluster_key": "cpu",
                   "notebook_task": {"notebook_path": "/task"}}],
    }})
    path = "/child" if child else "/task"
    graph.notebooks[path] = NotebookNode(
        dbx_path=path, role="dependency" if child else "task",
        referenced_by=["/task" if child else "job:42/task:task"],
        nb_dict={"cells": cells})
    if child:
        graph.notebooks["/task"] = NotebookNode(
            dbx_path="/task", role="task", referenced_by=["job:42/task:task"],
            nb_dict={"cells": [{"cell_type": "code", "source": "%run /child"}]})
    if init_source is not None:
        graph.files["/init.sh"] = FileNode(
            dbx_source="/init.sh", kind="init_script",
            referenced_by=["job:42/task:task"], content=init_source.encode())
    before = copy.deepcopy(graph.notebooks)
    if consumer == "discovery":
        uri = exporter.discover_notebook_pip_deps(graph, "/export", "42")
    else:
        # Deliberately do not call discovery first: consolidation re-scans
        # notebook source and must independently enforce the same contract.
        uris = exporter.consolidate_cluster_requirements(graph, "42")
        uri = uris["job:42/job_cluster:cpu"]
    assert graph.notebooks == before, "dependency scanning must not rewrite notebooks"
    content = graph.files[uri].content.decode() if uri else ""
    names = {Requirement(line).name.replace("_", "-").lower()
             for line in content.splitlines() if line and not line.startswith("#")}
    return content, names


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
@pytest.mark.parametrize("command", COMMANDS)
@pytest.mark.parametrize("wrapper", ['"""{}"""', "'''{}'''", 'r"""{}"""',
                                      'f"""{}"""', "comment", "%md", "%md-sandbox",
                                      "markdown", "raw"])
def test_documentation_and_comments_are_not_requirements(consumer, command, wrapper):
    example = command.format(pkg="documentation_only_pkg")
    cell_type = "code"
    if wrapper == "comment":
        source = "\n".join("# " + line for line in example.splitlines())
    elif wrapper in ("markdown", "raw"):
        cell_type, source = wrapper, example
    elif wrapper.startswith("%"):
        source = "\n# notebook annotation\n" + wrapper + "\nExample:\n" + example
    else:
        source = "HELP = " + wrapper.format("\n" + example + "\n")
    _, names = _artifact([
        {"cell_type": cell_type, "source": source},
        {"cell_type": "code", "source": "!pip install real_required_pkg"},
    ], consumer)
    assert "documentation-only-pkg" not in names
    assert "real-required-pkg" in names


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
@pytest.mark.parametrize("command", COMMANDS)
@pytest.mark.parametrize("as_lines", [False, True])
@pytest.mark.parametrize("child", [False, True])
def test_real_commands_after_strings_keep_their_quoted_arguments(
        consumer, command, as_lines, child):
    source = ('%python\nHELP = """\n!pip install documentation_only_pkg\n"""\n'
              + command.format(pkg="real_required_pkg"))
    _, names = _artifact([{"cell_type": "code",
                           "source": source.splitlines(keepends=True) if as_lines else source}],
                         consumer, child=child)
    assert "documentation-only-pkg" not in names
    assert "real-required-pkg" in names


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
def test_documentation_only_notebook_has_no_example_dependency(consumer):
    content, names = _artifact([{"cell_type": "code", "source":
        '"""\n!pip install documentation_only_pkg\n"""'}], consumer)
    assert "documentation-only-pkg" not in names
    if consumer == "discovery":
        assert content == "", "do not create a synthetic file for documentation alone"


@pytest.mark.parametrize("child", [False, True])
def test_filtered_dependencies_keep_per_notebook_provenance(child):
    # Integration with the dev branch/#109: keep its per-notebook attribution while
    # filtering examples. Do not restore the old aggregate source label.
    source = 'HELP = """\n!pip install documentation_only_pkg\n"""\n!pip install real_required_pkg'
    content, names = _artifact([{"cell_type": "code", "source": source}],
                               "consolidation", child=child)
    assert "documentation-only-pkg" not in names
    assert "real-required-pkg" in names
    path = "/child" if child else "/task"
    assert f"notebook %pip: {path}" in content
    assert "notebook pip commands" not in content


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
@pytest.mark.parametrize("source", [
    'HELP = "os.system(\'pip install documentation_only_pkg\')"\nos.system("pip install real_required_pkg")',
    "HELP = '''" + "\\'''" + "\n!pip install documentation_only_pkg\n'''\n!pip install real_required_pkg",
    'HELP = "example\\\n!pip install documentation_only_pkg"\n!pip install real_required_pkg',
    'HELP = f"{data[\'key\']}"\n!pip install real_required_pkg',
    '# a quote does not start a string: """\n!pip install real_required_pkg',
    'if flag:\n    !pip install real_required_pkg\n',
    '\n\n   !pip install real_required_pkg\n',
    'HELP = """documentation"""; os.system("pip install real_required_pkg")',
    'HELP = "line\u2028break\ftext"\n!pip install real_required_pkg',
    'TITLE = "%md-sandbox"\n!pip install real_required_pkg',
    'HELP = """\r\n!pip install documentation_only_pkg\r\n"""\r\n!pip install real_required_pkg',
])
def test_quote_comment_and_indentation_boundaries(consumer, source):
    _, names = _artifact([{"cell_type": "code", "source": source}], consumer)
    assert "documentation-only-pkg" not in names
    assert "real-required-pkg" in names


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
def test_real_pins_extras_markers_and_flags_still_use_shared_parser(consumer):
    source = ('HELP = """\n!pip install documentation_only_pkg\n"""\n'
              '!pip install --log=/tmp/pip.log '
              '"real_required_pkg[fast]>=1; python_version < \'4\'"')
    content, names = _artifact([{"cell_type": "code", "source": source}], consumer)
    specs = [Requirement(line) for line in content.splitlines()
             if line and not line.startswith("#")]
    spec = next(s for s in specs if s.name.replace("_", "-") == "real-required-pkg")
    assert spec.extras == {"fast"}
    assert str(spec.specifier) == ">=1"
    assert str(spec.marker) == 'python_version < "4"'
    assert "documentation-only-pkg" not in names
    assert "/tmp/pip.log" not in content


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
@pytest.mark.parametrize("source", [
    '"""\n!pip install -r examples.txt\n"""',
    '%md\n!pip install --group examples',
    '%md-sandbox\n!pip install --hash sha256:example demo',
])
def test_documentation_is_filtered_before_argument_parsing(consumer, source):
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        _artifact([{"cell_type": "code", "source": source}], consumer)
    assert not recorded, "documentation must not trigger unresolved dependency warnings"


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
def test_unrelated_source_does_not_get_pip_scan_warnings(consumer):
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        _artifact([{"cell_type": "code", "source": '%sql\nSELECT "pipeline'}], consumer)
    assert not recorded, "an unrelated syntax issue is not an unresolved pip command"


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
def test_real_direct_reference_retains_its_url(consumer):
    source = ('HELP = """\n!pip install documentation_only_pkg\n"""\n'
              '!pip install real_required_pkg @ https://example.com/pkg.whl')
    content, names = _artifact([{"cell_type": "code", "source": source}], consumer)
    assert "real-required-pkg" in names
    assert "https://example.com/pkg.whl" in content
    assert "documentation-only-pkg" not in names


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
@pytest.mark.parametrize("ending", ['HELP = """\n!pip install documentation_only_pkg',
                                    'HELP = "unterminated'])
def test_unclassifiable_source_is_reported_without_raw_text_fallback(consumer, ending):
    source = "!pip install real_required_pkg\n" + ending
    with pytest.warns(UserWarning, match=r"pip UNRESOLVED: /task:cell0: source scan incomplete"):
        _, names = _artifact([{"cell_type": "code", "source": source}], consumer)
    assert "real-required-pkg" in names
    assert "documentation-only-pkg" not in names


def test_shell_init_script_discovery_is_not_python_tokenized():
    # Python code in a shell heredoc is executable, not a Python docstring.
    # The notebook-only context filter must not change init-script discovery.
    script = "python - <<'PY'\n" + COMMANDS[5].format(pkg="init_required_pkg") + "\nPY\n"
    _, names = _artifact([{"cell_type": "code", "source":
        '"""\n!pip install documentation_only_pkg\n"""'}], "consolidation", init_source=script)
    assert "init-required-pkg" in names
    assert "documentation-only-pkg" not in names


@pytest.mark.parametrize("consumer", ["discovery", "consolidation"])
@pytest.mark.parametrize("inactive_part", ["assignment", "loop", "call"])
def test_loop_inference_uses_only_executable_parts(consumer, inactive_part):
    parts = {
        "assignment": 'libraries = ["documentation_only_pkg"]',
        "loop": 'for lib in libraries:',
        "call": '    subprocess.check_call([sys.executable, "-m", "pip", "install", lib])',
    }
    # Hiding any one part must prevent a documentation example from supplying
    # the missing evidence for the variable-list install heuristic.
    source = "\n".join(('"""' + part + '"""') if key == inactive_part else part
                       for key, part in parts.items())
    _, names = _artifact([{"cell_type": "code", "source": source}], consumer)
    assert "documentation-only-pkg" not in names
