"""Run cae52f15: documentation must not become a required notebook dependency."""

import asyncio
import contextlib
import io
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dbx_export import dependency_resolver as resolver
from dbx_export.dbx_client import DbxExportError, ExportedFile
from scripts import build_dag
from test_soft_match_child_job_integration import FakeDbx, harness as export_harness


def notebook(source, language="python"):
    return {"nbformat": 4, "nbformat_minor": 5,
            "metadata": {"language_info": {"name": language}}, "cells": [
        {"cell_type": "code", "source": source.splitlines(keepends=True)},
    ]}


@pytest.fixture(params=[False, True], ids=["local", "portable"])
def extractors(request):
    scope = vars(resolver)
    if request.param:
        scope = {}
        exec(resolver.portable_run_extractor_source(), scope)
    return scope["extract_run_targets"], scope["_extract_percent_run_targets"]


@pytest.mark.parametrize("documentation,assigned", [
    ('def render():\n    """\n    %run 14, so the helper has to exist here too\n    """\n', False),
    ("'''\n%run ./example\n'''\n", False),
    ('example = r"""\n%run ./example\n"""\n', True),
    ('example = f"""\n%run ./example\n{1 + 1}\n"""\n', True),
    ('example = b"""\n%run ./example\n"""\n', True),
    ('"""\n# This is inside the string, including its closing quotes: """\n', False),
    ('# %run ./example\n', False),
])
def test_docstrings_are_excluded_but_assigned_strings_keep_base_behavior(extractors, documentation, assigned):
    nb = notebook(documentation + '%run "./Real Helper" $mode="test"\n')
    for extract in extractors:
        expected = (["/Jobs/example"] if assigned else []) + ["/Jobs/Real Helper"]
        assert extract(nb, "/Jobs/main") == expected


@pytest.mark.parametrize("source,legacy_candidate", [
    ("example = 'dbutils.notebook.run(\"./example\", 0)'\n", True),
    ('"""\naidputils.notebook.run("./example", 0)\n"""\n', True),
    ('# oidlUtils.notebook.run("./example", 0)\n', False),
    ('value = 1  # dbutils.notebook.run("./example", 0)\n', True),
    ('a = """\n# """\n"""\ndbutils.notebook.run("./example", 0)\n# """\nb = """\n"""\n', True),
])
def test_notebook_call_matching_keeps_base_behavior(extractors, source, legacy_candidate):
    main, percent = extractors
    nb = notebook(source + 'dbutils.notebook.run("./real_child", 0)\n')
    expected = (["/Jobs/example"] if legacy_candidate else []) + ["/Jobs/real_child"]
    assert main(nb, "/Jobs/main") == expected
    assert percent(nb, "/Jobs/main") == []


@pytest.mark.parametrize("kind", ["f", "t"])
def test_formatted_and_template_strings_keep_base_candidates(extractors, kind):
    nb = notebook('example = ' + kind + '"""\n%run ./example\n"""\n%run ./real_child\n')
    for extract in extractors:
        assert extract(nb, "/Jobs/main") == ["/Jobs/example", "/Jobs/real_child"]


def test_executed_call_inside_fstring_keeps_explicit_provenance(extractors):
    main, _ = extractors
    explicit = set()
    nb = notebook('message = f"{dbutils.notebook.run(\'./child\', 1)}"\n')
    assert main(nb, "/Jobs/main", explicit_targets=explicit) == ["/Jobs/child"]
    assert explicit == {"/Jobs/child"}


_NEWER_PYTHON_SOURCES = [
    'result = f"{dbutils.notebook.run("./child", 1)}"',
    'result = f"{\n    dbutils.notebook.run("./child", 1) # actual call\n}"',
    'result = f"{\'\\n\'.join([])} {dbutils.notebook.run(\'./child\', 1)}"',
]


@pytest.mark.parametrize("body", _NEWER_PYTHON_SOURCES)
@pytest.mark.parametrize("prefix", ["", "%python\n", "%python "])
def test_newer_python_source_retains_dependency_fallback(extractors, body, prefix):
    main, percent = extractors
    messages = []
    explicit = set()
    nb = notebook(prefix + body + "\n%run ./magic_child\n")
    assert main(nb, "/Jobs/main", log=messages.append, explicit_targets=explicit) == [
        "/Jobs/magic_child", "/Jobs/child",
    ]
    assert explicit == {"/Jobs/magic_child", "/Jobs/child"}
    assert percent(nb, "/Jobs/main") == ["/Jobs/magic_child"]
    try:
        compile(body, "<Python 3.12+ source>", "exec")
    except SyntaxError:
        # The ordinary notebook.run fallback is unchanged, including its
        # existing diagnostic behavior for a literal first argument.
        assert not messages


@pytest.mark.parametrize("body", _NEWER_PYTHON_SOURCES)
@pytest.mark.parametrize("prefix", ["", "%python\n", "%python "])
@pytest.mark.parametrize("missing", [False, True])
def test_newer_python_source_keeps_strict_export_outcome(
    export_harness, monkeypatch, body, prefix, missing,
):
    original = FakeDbx.export_notebook

    def export(client, path):
        if path != "/Jobs/child":
            return original(client, path)
        if missing:
            raise DbxExportError("not_found", "missing required child", 404)
        return ExportedFile(json.dumps(notebook("VALUE = 1\n")).encode(), True, ".ipynb")

    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    rc, _, _ = export_harness("/Jobs/main", prefix + body, "--strict-deps", "--dry-run")
    assert int(rc) == (6 if missing else 0)


@pytest.mark.parametrize("language,prefix,body", [
    ("scala", "%python", '"""\n%run ./example\n"""\n'),
    ("python", "%scala", "val symbol = 'example\n"),
])
def test_language_override_after_comments(extractors, language, prefix, body):
    nb = notebook('# preserved annotation\n' + prefix + '\n' + body
                  + '%run ./real_child\n', language)
    for extract in extractors:
        assert extract(nb, "/Jobs/main") == ["/Jobs/real_child"]


@pytest.mark.parametrize("target", ["./John's Helper", './Helper (copy)', './Helper[1]'])
def test_notebook_magic_arguments_need_not_be_python(extractors, target):
    for extract in extractors:
        assert extract(notebook(f"%run {target}\n"), "/Jobs/main") == [
            "/Jobs/" + target[2:],
        ]


def test_quoted_magic_arguments_are_data_not_notebook_calls(extractors):
    source = ("%run ./real_child $example='dbutils.notebook.run(\"./example\", 0)'\n"
              '%run "./Second Helper"\n')
    main, percent = extractors
    assert main(notebook(source), "/Jobs/main") == [
        "/Jobs/real_child", "/Jobs/Second Helper", "/Jobs/example",
    ]
    assert percent(notebook(source), "/Jobs/main") == [
        "/Jobs/real_child", "/Jobs/Second Helper",
    ]


@pytest.mark.parametrize("separator", ["\n", "\r", "\r\n", "\u2028", "\f"])
def test_source_positions_survive_magic_preparation(extractors, separator):
    source = ('# dbutils.notebook.run("./comment", 0)\n'
              '"""before' + separator + '%run ./example\n"""\n'
              '%run "./real_child"\n')
    if separator in ("\r", "\r\n"):
        source = source.replace("\r\n", "\n").replace("\r", "\n").replace("\n", separator)
    nb = notebook("")
    # Preserve physical line boundaries, including separators inside string literals.
    nb["cells"][0]["source"] = source
    for extract in extractors:
        assert extract(nb, "/Jobs/main") == ["/Jobs/real_child"]


@pytest.mark.parametrize("language,prefix", [
    ("scala", ""), ("r", ""), ("sql", ""),
    ("python", "%scala\n"), ("scala", "%python\n"),
])
def test_language_metadata_and_overrides_preserve_real_runs(extractors, language, prefix):
    nb = notebook(prefix + '%run "./Real Helper"\n', language)
    for extract in extractors:
        assert extract(nb, "/Jobs/main") == ["/Jobs/Real Helper"]


_MAGIC_COMMAND_SOURCES = [
    '%python dbutils.notebook.run("./child", 1)\n',
    '%python f"{dbutils.notebook.run(\'./child\', 1)}"\n',
    '%python dbutils.notebook.run("./child", 1) # dbutils.notebook.run("./example", 1)\n',
    '%scala dbutils.notebook.run("./child", 1)\n',
    "%scala // annotation\nval sym = 'example\ndbutils.notebook.run(\"./child\", 1)\n",
    '%time dbutils.notebook.run("./child", 1)\n',
    '%time f"{dbutils.notebook.run(\'./child\', 1)}"\n',
]


@pytest.mark.parametrize("source", _MAGIC_COMMAND_SOURCES)
@pytest.mark.parametrize("language", ["python", "scala"])
def test_magic_commands_keep_executable_code(extractors, source, language):
    main, percent = extractors
    explicit = set()
    nb = notebook(source, language)
    expected = ["/Jobs/child"] + (["/Jobs/example"] if "# dbutils" in source else [])
    assert main(nb, "/Jobs/main", explicit_targets=explicit) == expected
    assert explicit == set(expected)
    assert percent(nb, "/Jobs/main") == []


@pytest.mark.parametrize("source", _MAGIC_COMMAND_SOURCES)
@pytest.mark.parametrize("missing", [False, True])
def test_magic_commands_keep_strict_export_outcome(export_harness, monkeypatch, source, missing):
    original = FakeDbx.export_notebook

    def export(client, path):
        if path not in ("/Jobs/child", "/Jobs/example"):
            return original(client, path)
        if missing and path == "/Jobs/child":
            raise DbxExportError("not_found", "missing required child", 404)
        return ExportedFile(json.dumps(notebook("VALUE = 1\n")).encode(), True, ".ipynb")

    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    rc, _, _ = export_harness("/Jobs/main", source, "--strict-deps", "--dry-run")
    assert int(rc) == (6 if missing else 0)


_CHILD_EXPRESSION = 'f"{dbutils.notebook.run(\'./child\', 1)}"'
_PYTHON_BODY_CASES = [
    'if True:\n    result = ' + _CHILD_EXPRESSION,
    'for item in [1]:\n    result = ' + _CHILD_EXPRESSION,
    'while True:\n    result = ' + _CHILD_EXPRESSION + '\n    break',
    'def helper():\n    return ' + _CHILD_EXPRESSION + '\nhelper()',
    'class Helper:\n    result = ' + _CHILD_EXPRESSION,
    "with __import__('contextlib').nullcontext():\n    result = " + _CHILD_EXPRESSION,
    'try:\n    result = ' + _CHILD_EXPRESSION + '\nexcept RuntimeError:\n    pass',
    ('notes = """\n%run ./doc_example\n# end of notes"""\n'
     'example = \'dbutils.notebook.run("./string_example", 1)\'\n'
     'if True:\n    result = ' + _CHILD_EXPRESSION + ' # dbutils.notebook.run("./comment_example", 1)'),
]


@pytest.mark.parametrize("body", _PYTHON_BODY_CASES)
@pytest.mark.parametrize("language,prefix", [
    ("python", ""), ("python", "%python\n"),
    ("python", "%python "), ("scala", "%python "),
])
def test_python_source_structure_is_preserved(extractors, body, language, prefix):
    # Ground truth: the actual Python body executes precisely this child call.
    calls = []
    dbutils = SimpleNamespace(notebook=SimpleNamespace(
        run=lambda *args: calls.append(args) or "ok"))
    exec(compile(body, "<customer Python body>", "exec"), {"dbutils": dbutils})
    assert calls == [("./child", 1)]

    main, percent = extractors
    explicit = set()
    nb = notebook(prefix + body, language)
    expected = (["/Jobs/doc_example", "/Jobs/string_example", "/Jobs/child", "/Jobs/comment_example"]
                if body == _PYTHON_BODY_CASES[-1] else ["/Jobs/child"])
    assert main(nb, "/Jobs/main", explicit_targets=explicit) == expected
    assert explicit == set(expected)
    assert percent(nb, "/Jobs/main") == (["/Jobs/doc_example"] if body == _PYTHON_BODY_CASES[-1] else [])


@pytest.mark.parametrize("body", _PYTHON_BODY_CASES)
@pytest.mark.parametrize("prefix", ["", "%python\n", "%python "])
@pytest.mark.parametrize("missing", [False, True])
def test_python_structure_keeps_strict_export_outcome(
    export_harness, monkeypatch, body, prefix, missing,
):
    original = FakeDbx.export_notebook

    def export(client, path):
        if path not in ("/Jobs/child", "/Jobs/doc_example", "/Jobs/string_example", "/Jobs/comment_example"):
            return original(client, path)
        if missing and path == "/Jobs/child":
            raise DbxExportError("not_found", "missing required child", 404)
        return ExportedFile(json.dumps(notebook("VALUE = 1\n")).encode(), True, ".ipynb")

    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    rc, _, _ = export_harness("/Jobs/main", prefix + body, "--strict-deps", "--dry-run")
    assert int(rc) == (6 if missing else 0)


def _scala_assignment_notebook(language, prefix):
    nb = notebook("", language)
    nb["cells"] = [{"cell_type": "code", "source": prefix + source} for source in (
        'var path = ""', '// assignment note\npath = "./child"',
        'dbutils.notebook.run(path, 1)',
    )]
    return nb


@pytest.mark.parametrize("language,prefix", [("scala", ""), ("python", "%scala\n")])
def test_non_python_constant_propagation_is_preserved(extractors, language, prefix):
    main, percent = extractors
    loose = set()
    nb = _scala_assignment_notebook(language, prefix)
    assert main(nb, "/Jobs/main", loose_only=loose) == ["/Jobs/child"]
    assert not loose
    assert percent(nb, "/Jobs/main") == []


@pytest.mark.parametrize("language,prefix", [("scala", ""), ("python", "%scala\n")])
@pytest.mark.parametrize("missing", [False, True])
def test_non_python_assignment_keeps_strict_outcome(
    export_harness, monkeypatch, language, prefix, missing,
):
    def export(client, path):
        if path == "/Jobs/child" and missing:
            raise DbxExportError("not_found", "missing required child", 404)
        nb = (_scala_assignment_notebook(language, prefix) if path == client.nb_path
              else notebook("VALUE = 1\n"))
        return ExportedFile(json.dumps(nb).encode(), True, ".ipynb")

    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    rc, _, _ = export_harness("/Jobs/main", "", "--strict-deps", "--dry-run")
    assert int(rc) == (6 if missing else 0)


def test_unclosed_python_string_preserves_legacy_discovery(extractors):
    for extract in extractors:
        assert extract(notebook('"""\n%run ./unknown\n'), "/Jobs/main") == ["/Jobs/unknown"]


@pytest.mark.parametrize("missing", [False, True])
def test_resolver_only_fetches_executable_references(missing):
    fetched = []
    source = ('"""\n%run 14, so the helper has to exist here too\n"""\n'
              '%run "./Real Helper"\n')

    class Dbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{"task_key": "main", "notebook_task": {
                "notebook_path": "/Jobs/main",
            }}]}}

        def export_notebook(self, path):
            fetched.append(path)
            if path == "/Jobs/Real Helper" and missing:
                raise DbxExportError("not_found", "missing notebook", 404)
            body = source if path == "/Jobs/main" else "VALUE = 1\n"
            return ExportedFile(json.dumps(notebook(body)).encode(), True, ".ipynb")

        def get_workspace_status(self, _path):
            return {"object_type": "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = resolver.DependencyResolver(Dbx(), log=lambda _: None).resolve("1")
    assert set(fetched) == {"/Jobs/main", "/Jobs/Real Helper"}
    assert set(graph.notebooks) == set(fetched)
    assert bool(graph.notebooks["/Jobs/Real Helper"].export_error) == missing


@pytest.mark.parametrize("expression", [
    'dbutils.notebook.run("./child", 1)',
    'f"{dbutils.notebook.run(\'./child\', 1)}"',
])
@pytest.mark.parametrize("child_kind", ["notebook", "file", "untyped_file", "missing"])
def test_resolver_preserves_existing_child_classification(expression, child_kind):
    class Dbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{"task_key": "main", "notebook_task": {
                "notebook_path": "/Jobs/main",
            }}]}}

        def export_notebook(self, path):
            if path == "/Jobs/child":
                if child_kind == "missing":
                    raise DbxExportError("not_found", "missing notebook", 404)
                if child_kind in ("file", "untyped_file"):
                    return ExportedFile(b"VALUE = 1\n", False, ".py")
            source = expression if path == "/Jobs/main" else "VALUE = 1\n"
            return ExportedFile(json.dumps(notebook(source)).encode(), True, ".ipynb")

        def get_workspace_status(self, _path):
            if _path == "/Jobs/child" and child_kind == "untyped_file":
                return None
            return {"object_type": "FILE" if _path == "/Jobs/child"
                    and child_kind == "file" else "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = resolver.DependencyResolver(Dbx(), log=lambda _: None).resolve("1")
    assert graph.notebooks["/Jobs/main"].content == json.dumps(notebook(expression)).encode()
    if child_kind == "file":
        assert "/Jobs/child" not in graph.notebooks
        assert "/Jobs/child" in graph.skipped_assets
        return
    child = graph.notebooks["/Jobs/child"]
    assert child.explicit_run_ref
    assert child.export_error == {"notebook": None, "untyped_file": "not_a_notebook",
                                  "missing": "not_found"}[child_kind]


@pytest.mark.parametrize("object_type", sorted(resolver._NON_NOTEBOOK_WORKSPACE_OBJECT_TYPES))
@pytest.mark.parametrize("reference", [
    '%run ./child', 'dbutils.notebook.run("./child", 1)',
    'f"{dbutils.notebook.run(\'./child\', 1)}"',
    'candidate = "./child"\ndbutils.notebook.run(runtime_path, 1)',
])
def test_workspace_status_preserves_existing_non_notebook_skip(object_type, reference):
    class Dbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{"task_key": "main", "notebook_task": {
                "notebook_path": "/Jobs/main",
            }}]}}

        def export_notebook(self, path):
            assert path == "/Jobs/main", "Known non-notebook objects must not be downloaded"
            return ExportedFile(json.dumps(notebook(reference)).encode(), True, ".ipynb")

        def get_workspace_status(self, _path):
            return {"object_type": object_type}

        def list_workspace_folder(self, _path):
            return []

    graph = resolver.DependencyResolver(Dbx(), log=lambda _: None).resolve("1")
    assert "/Jobs/child" not in graph.notebooks
    assert "/Jobs/child" in graph.skipped_assets


@pytest.mark.parametrize("source", [
    'dbutils.notebook.run("./" "child", 1)',
    'dbutils.notebook.run(\n    "./"\n    "child", 1)',
    'f"{dbutils.notebook.run(\'./\' \'child\', 1)}"',
])
def test_adjacent_literals_preserve_successful_strict_export(export_harness, monkeypatch, source):
    calls = []
    dbutils = SimpleNamespace(notebook=SimpleNamespace(
        run=lambda *args: calls.append(args) or "ok"))
    exec(compile(source, "<customer Python body>", "exec"), {"dbutils": dbutils})
    assert calls == [("./child", 1)]

    fetched = []

    def export(client, path):
        fetched.append(path)
        assert path in ("/Jobs/main", "/Jobs/child")
        body = source if path == "/Jobs/main" else "VALUE = 1\n"
        return ExportedFile(json.dumps(notebook(body)).encode(), True, ".ipynb")

    # The legacy regex also discovers /Jobs from the first adjacent literal.
    # Preserve its existing asset handling rather than make this fatal.
    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    monkeypatch.setattr(FakeDbx, "get_workspace_status", lambda client, path: {
        "object_type": "DIRECTORY" if path == "/Jobs" else "NOTEBOOK",
    }, raising=False)
    rc, _, _ = export_harness("/Jobs/main", source, "--strict-deps", "--dry-run")
    assert int(rc) == 0
    assert fetched == ["/Jobs/main", "/Jobs/child"]


@pytest.mark.parametrize("source,target", [
    ('"""\n%run ./example\n"""\n%run ./real_child\n', "real_child"),
    ('%python ' + _PYTHON_BODY_CASES[0].replace('./child', './real_child'), "real_child"),
    (_PYTHON_BODY_CASES[-1].replace('./child', './real_child'), "real_child"),
    ('"""\n%run ./unknown\n', "unknown"),
    *((source.replace('./child', './real_child'), "real_child")
      for source in _NEWER_PYTHON_SOURCES),
])
def test_remote_scan_uses_same_filter_and_legacy_fallback(source, target):
    path = "/Workspace/Jobs/main.ipynb"
    files = {path: notebook(source),
             "/Workspace/Jobs/real_child.ipynb": notebook("VALUE = 1\n")}

    class Session:
        async def _execute_locked(self, code, timeout=60):
            # Execute the actual generated scanner with a workspace filesystem fixture.
            output = io.StringIO()
            with contextlib.redirect_stdout(output), \
                    patch("os.path.exists", side_effect=files.__contains__), \
                    patch("builtins.open", side_effect=lambda p: io.StringIO(json.dumps(files[p]))):
                exec(code, {})
            return {"outputs": [{"type": "stream", "text": output.getvalue()}]}

    exists, targets = asyncio.run(build_dag.scan_external_notebook(Session(), path))
    assert exists
    names = (["doc_example", "string_example", target, "comment_example"]
             if "string_example" in source else [target])
    assert list(targets) == [f"/Workspace/Jobs/{name}.ipynb" for name in names]


_EMBEDDED_CALL = 'dbutils.notebook.run("./child", 1)'
_EXECUTED_STRING_SOURCES = [
    'exec(' + repr(_EMBEDDED_CALL) + ')',
    'eval(' + repr(_EMBEDDED_CALL) + ')',
    'exec(compile(' + repr(_EMBEDDED_CALL) + ', "<embedded>", "exec"))',
    'code = ' + repr(_EMBEDDED_CALL) + '\nexec(code)',
    'exec("""\n' + _EMBEDDED_CALL + '\n""")',
    'def helper():\n    exec(' + repr(_EMBEDDED_CALL) + ')\nhelper()',
    '"""' + _EMBEDDED_CALL + '"""\nexec(__doc__)',
    'def helper():\n    """' + _EMBEDDED_CALL + '"""\nexec(helper.__doc__)',
    'class Helper:\n    """' + _EMBEDDED_CALL + '"""\nexec(Helper.__doc__)',
]


@pytest.mark.parametrize("source", _EXECUTED_STRING_SOURCES)
def test_executed_strings_keep_required_children(extractors, source):
    calls = []
    stub = SimpleNamespace(notebook=SimpleNamespace(
        run=lambda *args: calls.append(args) or "ok"))
    exec(compile(source, "<actual customer body>", "exec"), {"dbutils": stub})
    assert calls == [("./child", 1)]
    main, percent = extractors
    messages = []
    explicit = set()
    assert main(notebook(source), "/Jobs/main", log=messages.append,
                explicit_targets=explicit) == ["/Jobs/child"]
    assert explicit == {"/Jobs/child"}
    assert percent(notebook(source), "/Jobs/main") == []


@pytest.mark.parametrize("source", _EXECUTED_STRING_SOURCES)
@pytest.mark.parametrize("missing", [False, True])
def test_executed_strings_keep_strict_export_outcome(export_harness, monkeypatch, source, missing):
    original = FakeDbx.export_notebook
    fetched = []

    def export(client, path):
        fetched.append(path)
        if path != "/Jobs/child":
            return original(client, path)
        if missing:
            raise DbxExportError("not_found", "missing required child", 404)
        return ExportedFile(json.dumps(notebook("VALUE = 1\n")).encode(), True, ".ipynb")

    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    rc, _, _ = export_harness("/Jobs/main", source, "--strict-deps", "--dry-run")
    assert int(rc) == (6 if missing else 0)
    assert fetched == ["/Jobs/main", "/Jobs/child"]


@pytest.mark.parametrize("source", _EXECUTED_STRING_SOURCES)
def test_generated_remote_scanner_keeps_executed_strings(source):
    path = "/Workspace/Jobs/main.ipynb"
    files = {path: notebook(source), "/Workspace/Jobs/child.ipynb": notebook("VALUE = 1\n")}

    class Session:
        async def _execute_locked(self, code, timeout=60):
            output = io.StringIO()
            with contextlib.redirect_stdout(output), \
                    patch("os.path.exists", side_effect=files.__contains__), \
                    patch("builtins.open", side_effect=lambda p: io.StringIO(json.dumps(files[p]))):
                exec(code, {})
            return {"outputs": [{"type": "stream", "text": output.getvalue()}]}

    exists, targets = asyncio.run(build_dag.scan_external_notebook(Session(), path))
    assert exists
    assert list(targets) == ["/Workspace/Jobs/child.ipynb"]


_DOCSTRING_SOURCES = [
    '"""\n%run ./example\n"""\n',
    'def helper():\n    """\n%run ./example\n# end of notes"""\n',
    'async def helper():\n    """\n%run ./example\n"""\n',
    'class Helper:\n    """\n%run ./example\n"""\n',
    '("prefix"\n """\n%run ./example\n""")\n',
    'r"""\n%run ./example\n"""\n',
]


@pytest.mark.parametrize("source", _DOCSTRING_SOURCES)
@pytest.mark.parametrize("prefix", ["", "%python\n", "%python "])
def test_only_genuine_docstrings_exclude_percent_run(extractors, source, prefix):
    nb = notebook(prefix + source + "%run ./child\n")
    original = json.dumps(nb)
    for extract in extractors:
        assert extract(nb, "/Jobs/main") == ["/Jobs/child"]
    assert json.dumps(nb) == original


@pytest.mark.parametrize("source", _DOCSTRING_SOURCES)
@pytest.mark.parametrize("prefix", ["", "%python\n", "%python "])
@pytest.mark.parametrize("missing", [False, True])
def test_docstring_scopes_keep_strict_export_outcome(export_harness, monkeypatch, source, prefix, missing):
    original = FakeDbx.export_notebook

    def export(client, path):
        if path != "/Jobs/child":
            return original(client, path)
        if missing:
            raise DbxExportError("not_found", "missing required child", 404)
        return ExportedFile(json.dumps(notebook("VALUE = 1\n")).encode(), True, ".ipynb")

    monkeypatch.setattr(FakeDbx, "export_notebook", export)
    rc, _, _ = export_harness("/Jobs/main", prefix + source + "%run ./child\n",
                              "--strict-deps", "--dry-run")
    assert int(rc) == (6 if missing else 0)


@pytest.mark.parametrize("prefix", ["", "%python\n", "%python "])
def test_docstring_byte_spans_preserve_neighboring_assigned_strings(extractors, prefix):
    source = ('def café(): """\u2028%run ./example\u2028"""; other = """\u2028%run ./assigned\n"""\n'
              '%run ./child\n')
    nb = notebook(prefix + source)
    nb["cells"][0]["source"] = prefix + source  # Keep physical rows intact.
    for extract in extractors:
        assert extract(nb, "/Jobs/main") == ["/Jobs/assigned", "/Jobs/child"]
