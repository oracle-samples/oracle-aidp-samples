"""Customer bindings take precedence over every Python implicit-import rule.

Incident: run aaaaaaaa-0000-4000-8000-000000000209 (2026-09-25), Sheets
child cells 9/10 received datetime.date instead of the endDate widget value.
Scope: use the existing symbol scan; do not change parameter handling or rules.
Grid: every Python rule x same-cell/notebook/%run binding; main/child execution
x plain/%python cells; supplied/missing widget; conversion then inline replay.
Visibility regression grid: local scopes, RHS/eager reads, future definitions,
markdown/empty positions, inherited bindings, deletion/%run deletion/rebinding.
"""

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm


PYTHON_IMPORTS = [
    line.split("  #", 1)[0]
    for _, line, language in jm._IMPLICIT_IMPORT_RULES
    if language == "python"
]


def _notebook(*sources):
    return {
        "cells": [{"cell_type": "code", "source": [source], "metadata": {},
                   "outputs": [], "execution_count": None} for source in sources],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }


@pytest.mark.parametrize("consumer", ["main", "converted_child"])
@pytest.mark.parametrize("cache_state", ["converted", "reused"])
@pytest.mark.parametrize("magic", ["", "%python\n"])
@pytest.mark.parametrize("target", [
    "/Workspace/test_imports/values.ipynb", "./values.ipynb", "values",
])
def test_run_binding_after_real_dependency_conversion(
    monkeypatch, tmp_path, consumer, cache_state, magic, target
):
    # Live control02b (2026-10-01): conversion caches only the migrated path.
    # Seeding both source and migrated cache keys hides the production failure.
    from test_conditional_exit_below_execute import _patch_common
    from test_leading_magic_injection import _Kernel

    dependency = "/Workspace/test_imports/values.ipynb"
    parent = "/Workspace/test_imports/source.ipynb"
    notebooks = {
        dependency: _notebook('# MAPPED_RUN_REGRESSION\ndate = "31-3-2026"\n'),
        parent: _notebook(
            f"%run {target}\n",
            magic + '# MAPPED_RUN_REGRESSION\nobserved = date\n'
            'assert observed == "31-3-2026"\n',
        ),
    }
    _patch_common(monkeypatch, tmp_path, notebooks[parent])
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 1)
    def download(path):
        if path in kernel.files:
            return kernel.files[path].encode()
        return json.dumps(notebooks[
            path if path.startswith("/") else "/Workspace/" + path
        ]).encode()

    monkeypatch.setattr(jm, "download_notebook", download)

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__(on_disk_cells=1)
            self.namespace = {}

        async def execute(self, code, **kwargs):
            result = await super().execute(code, **kwargs)
            if "# MAPPED_RUN_REGRESSION" in code and "base64.b64decode" not in code:
                exec("\n".join(line for line in code.splitlines()
                               if line.strip() != "%python"), self.namespace)
            return result

    kernel = Kernel()
    converted = asyncio.run(jm.process_notebook(
        dependency, kernel, "test_imports", "dep_values", {}, run_all=False))
    assert converted["status"] == "CONVERTED_NOT_VALIDATED"
    assert dependency not in jm._notebook_content_cache
    migrated_dependency, = jm._notebook_content_cache
    # Review #183: a bare target must be anchored at the caller before suffix
    # lookup. An unrelated earlier map entry must never supply its bindings.
    dep_map = {
        "/Workspace/other_job/values.ipynb": "/Workspace/other_output/values.ipynb",
        dependency: migrated_dependency,
    }
    if cache_state == "reused":
        # Registry reuse supplies a migrated path without populating the cache.
        jm._notebook_content_cache.clear()

    result = asyncio.run(jm.process_notebook(
        parent, kernel, "test_imports", "01_parent", {},
        run_all=consumer == "main", dep_path_map=dep_map))
    if consumer == "main":
        assert result["status"] == "PASS", result
    else:
        assert result["status"] == "CONVERTED_NOT_VALIDATED", result
        migrated_parent, = set(jm._notebook_content_cache) - {migrated_dependency}
        kernel.on_disk_cells = 2
        log = jm.NotebookLog(migrated_parent, "test_imports", "child")
        result = asyncio.run(jm._inline_child_notebook(
            migrated_parent, kernel, log, "test_imports", "", "", "", dep_map))
        assert result["status"] == "ok", result
    assert kernel.namespace["observed"] == "31-3-2026"
    assert not any("from datetime import date" in code for code in kernel.executed)


@pytest.mark.parametrize("unreadable", [b"", b"invalid json", b"{}"])
def test_unreadable_mapped_run_fails_before_parent_execution(monkeypatch, tmp_path, unreadable):
    from test_conditional_exit_below_execute import _patch_common
    from test_leading_magic_injection import _Kernel

    dependency = "/Workspace/test_imports/values.ipynb"
    migrated = "/Workspace/test_cond/notebooks/test_imports/values.ipynb"
    parent = "/Workspace/test_imports/parent.ipynb"
    nb = _notebook(f"%run {dependency}\n", "observed = date\n")
    _patch_common(monkeypatch, tmp_path, nb)
    # A stale source cache must not override execution's mapped dependency.
    monkeypatch.setattr(jm, "_notebook_content_cache", {
        dependency: json.dumps(_notebook('date = "stale"\n')),
    })
    monkeypatch.setattr(jm, "download_notebook", lambda path:
                        json.dumps(nb).encode() if path == parent else unreadable)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        parent, kernel, "test_imports", "01_parent", {},
        dep_path_map={dependency: migrated}))
    assert result["status"] == "FAIL"
    assert result["kind"] == "CHILD_SOURCE_UNAVAILABLE"
    assert result["job_should_stop"]
    assert "UNRESOLVED symbol scope" in result["error"]
    assert not any("observed = date" in code for code in kernel.executed)


@pytest.mark.parametrize("import_line", PYTHON_IMPORTS)
@pytest.mark.parametrize("binding_origin", ["same_cell", "notebook", "run_dependency"])
def test_customer_binding_prevents_import(monkeypatch, import_line, binding_origin):
    symbol = import_line.rsplit(" import ", 1)[1]
    binding = f'{symbol} = "customer-value"\n'
    use = f"observed = {symbol}\n"
    source = binding + use if binding_origin == "same_cell" else use
    if binding_origin == "run_dependency":
        dependency = "/Workspace/test_imports/values.ipynb"
        monkeypatch.setattr(jm, "_notebook_content_cache", {
            dependency: json.dumps(_notebook(binding)),
        })
        scope_cells = _notebook(f"%run {dependency}\n", source)["cells"]
    else:
        scope_cells = _notebook(binding, source)["cells"]
    scope = (None if binding_origin == "same_cell" else
             jm._resolve_symbol_scope("/Workspace/test_imports/main.ipynb", scope_cells))

    transformed = jm._add_missing_imports(source, scope)

    assert transformed == source
    assert jm._add_missing_imports(transformed, scope) == transformed
    namespace = {}
    exec(binding, namespace)
    exec(transformed, namespace)
    assert namespace["observed"] == "customer-value"


@pytest.mark.parametrize("import_line", PYTHON_IMPORTS)
def test_unbound_names_still_receive_imports(import_line):
    symbol = import_line.rsplit(" import ", 1)[1]
    result = jm._add_missing_imports(f"observed = {symbol}()\n")
    assert import_line in result
    assert jm._add_missing_imports(result) == result


@pytest.mark.parametrize("path", ["main", "child"])
@pytest.mark.parametrize("magic", ["", "%python\n"])
@pytest.mark.parametrize(("binding_origin", "widget_present"), [
    ("notebook", True), ("same_cell", True), ("run_dependency", True),
    ("notebook", False),
])
def test_widget_binding_through_real_cell_loops(
    monkeypatch, tmp_path, path, magic, binding_origin, widget_present
):
    # Reuse transport/save mocks; execute the incident's Python cells locally.
    # No AIDP, LLM, or Google Sheets service is contacted by this test.
    from test_conditional_exit_below_execute import _patch_common
    from test_leading_magic_injection import _Kernel

    observed = []

    def get_widget(name):
        assert name == "endDate"
        if not widget_present:
            raise ValueError("Widget not found: endDate")
        return "2026-09-25"

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__(on_disk_cells=2)
            self.namespace = {
                "aidputils": SimpleNamespace(widgets=SimpleNamespace(get=get_widget)),
                "spreadsheet": SimpleNamespace(worksheet=observed.append),
            }
            self.errors = []

        async def execute(self, code, **kwargs):
            result = await super().execute(code, **kwargs)
            if "# BINDING_REGRESSION" in code and "base64.b64decode" not in code:
                python = "\n".join(line for line in code.splitlines()
                                   if line.strip() not in {"%python", "%pyspark"})
                try:
                    exec(python, self.namespace)
                except Exception as exc:
                    self.errors.append((type(exc).__name__, str(exc)))
                    return {"status": "error", "outputs": [{
                        "type": "error", "ename": type(exc).__name__,
                        "evalue": str(exc), "traceback": [f"{type(exc).__name__}: {exc}"],
                    }]}
            return result

    binding = '# BINDING_REGRESSION\ndate = aidputils.widgets.get("endDate")\n'
    use = '# BINDING_REGRESSION\nworksheet = spreadsheet.worksheet(date)\n'
    if binding_origin == "same_cell":
        nb = _notebook(magic + binding + use)
    elif binding_origin == "run_dependency":
        nb = _notebook("%run /Workspace/test_imports/values.ipynb\n", magic + use)
    else:
        nb = _notebook(magic + binding, magic + use)
    _patch_common(monkeypatch, tmp_path, nb)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    if binding_origin == "run_dependency":
        for dependency in ("/Workspace/test_imports/values.ipynb",
                           jm.MIGRATED_BASE + "/test_imports/values.ipynb"):
            monkeypatch.setitem(jm._notebook_content_cache, dependency,
                                json.dumps(_notebook(magic + binding)))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 1)
    # The fix model returns the same source: missing widgets stay unresolved.
    async def unchanged(source, *args, **kwargs):
        return source, {"input": 0, "output": 0}
    monkeypatch.setattr(jm, "call_fix", unchanged)
    kernel = Kernel()
    source_path = ("/Workspace/test_imports/source.ipynb" if path == "main"
                   else jm.MIGRATED_BASE + "/source.ipynb")
    if path == "main":
        result = asyncio.run(jm.process_notebook(
            source_path, kernel, "test_imports", "01_task", {}))
        if widget_present:
            assert result["status"] == "PASS", result
        else:
            assert result["status"] != "PASS", result
    else:
        # Model the code-only dependency pass before entering the real inliner.
        scope = jm._resolve_symbol_scope(source_path, nb["cells"])
        for index, cell in enumerate(nb["cells"]):
            cell["source"] = [jm._add_missing_imports(
                "".join(cell["source"]), scope, cell_index=index)]
        monkeypatch.setitem(jm._notebook_content_cache, source_path, json.dumps(nb))
        log = jm.NotebookLog(source_path, "test_imports", "child")
        result = asyncio.run(jm._inline_child_notebook(
            source_path, kernel, log, "test_imports", "", "", "", {}))
        if widget_present:
            assert result["status"] == "ok", result

    if widget_present:
        assert observed == ["2026-09-25"]
        assert kernel.namespace["date"] == "2026-09-25"
        assert kernel.errors == []
    else:
        assert observed == []
        assert "date" not in kernel.namespace
        assert any("Widget not found: endDate" in message for _, message in kernel.errors)
        if path == "child":
            assert any(kind == "NameError" for kind, _ in kernel.errors)
    assert not any("from datetime import date" in code for code in kernel.executed)
    for saved_path, content in kernel.files.items():
        if saved_path.endswith(".ipynb"):
            saved = json.loads(content)
            assert not any("from datetime import date" in "".join(cell["source"])
                           for cell in saved["cells"])
    if path == "child":
        cached = json.loads(jm._notebook_content_cache[source_path])
        assert not any("from datetime import date" in "".join(cell["source"])
                       for cell in cached["cells"])


STANDARD_IMPORTS = [line for line in PYTHON_IMPORTS if "pyspark" not in line]


@pytest.mark.parametrize("import_line", STANDARD_IMPORTS)
@pytest.mark.parametrize("local_kind", ["function", "class", "lambda", "comprehension"])
def test_local_bindings_do_not_suppress_module_imports(import_line, local_kind):
    symbol = import_line.rsplit(" import ", 1)[1]
    bindings = {
        "function": f"def helper():\n    {symbol} = 'local'\n",
        "class": f"class Helper:\n    {symbol} = 'local'\n",
        "lambda": f"helper = lambda {symbol}: {symbol}\n",
        "comprehension": f"items = [{symbol} for {symbol} in []]\n",
    }
    cells = _notebook(bindings[local_kind], f"observed = {symbol}()\n")["cells"]
    scope = jm._resolve_symbol_scope("/Workspace/imports.ipynb", cells)
    result = jm._add_missing_imports("".join(cells[1]["source"]), scope, cell_index=1)
    assert import_line in result


@pytest.mark.parametrize("import_line", STANDARD_IMPORTS)
def test_assignment_rhs_still_receives_required_import(import_line):
    symbol = import_line.rsplit(" import ", 1)[1]
    result = jm._add_missing_imports(f"{symbol} = {symbol}()\n")
    assert import_line in result
    assert jm._add_missing_imports(result) == result


@pytest.mark.parametrize("binding", [
    'date = "customer"', 'date: str = "customer"',
    'date, other = "customer", 1', 'other, *date = 1, "customer"',
    'if True:\n    date = "customer"', 'for date in ["customer"]:\n    pass',
    'with manager() as date:\n    pass', '(date := "customer")',
    'from custom_module import custom_date as date', 'import custom_module as date',
    'def date():\n    return "customer"', 'class date:\n    pass',
])
def test_module_binding_forms_are_preserved(binding):
    source = binding + "\nobserved = date\n"
    assert jm._add_missing_imports(source) == source


@pytest.mark.parametrize("path", ["main", "child"])
@pytest.mark.parametrize("magic", ["", "%python\n"])
@pytest.mark.parametrize("scenario", [
    "local", "self_rhs", "future_cell", "future_statement", "spaced_cells",
    "class_body", "annotation", "assignment_target",
    "deferred_rhs", "deleted_cell", "delete_rebind", "run_deletion",
    "conditional_delete", "lambda_rhs", "nested_def_rhs",
    "empty_loop_delete", "unreachable_handler_delete", "nested_lambda_rhs",
    "global_write", "globals_write", "exec_write", "star_write",
    "dynamic_absent", "match_capture", "except_capture",
])
def test_import_visibility_order_through_real_cell_loops(
    monkeypatch, tmp_path, path, magic, scenario
):
    from test_conditional_exit_below_execute import _patch_common
    from test_leading_magic_injection import _Kernel

    constructor = 'observed = date(2026, 1, 1).isoformat()\n'
    binding = 'date = "31-3-2026"\n'
    use = 'sheet_value = date\n'
    sources = {
        "local": ['def helper():\n    date = "local"\n', constructor],
        "self_rhs": ['date = date(2026, 1, 1)\nobserved = date.isoformat()\n'],
        "future_cell": [constructor, binding, use],
        "future_statement": [constructor + binding + use],
        "spaced_cells": [constructor, binding, use],
        "parent_inheritance": [binding, "%run /Workspace/test_imports/consumer.ipynb\n"],
        "class_body": ['class Helper:\n    value = date(2026, 1, 1)\n'
                       + binding + 'observed = Helper.value.isoformat()\n'],
        "annotation": ['def helper(value: date):\n    pass\n' + binding
                       + 'observed = helper.__annotations__["value"](2026, 1, 1).isoformat()\n'],
        "assignment_target": ['data = {}\ndata[date(2026, 1, 1)] = 1\n' + binding
                              + 'observed = next(iter(data)).isoformat()\n'],
        "deferred_rhs": ['def make_date():\n    return date(2026, 1, 1)\n'
                         'date = make_date()\nobserved = date.isoformat()\n'],
        "conditional_delete": [binding, 'if False:\n    del date\n', 'observed = date\n'],
        "lambda_rhs": ['make = lambda: date(2026, 1, 1)\n'
                       'date = make()\nobserved = date.isoformat()\n'],
        "nested_def_rhs": ['def make():\n    def inner():\n        return date(2026, 1, 1)\n'
                           '    return inner()\ndate = make()\nobserved = date.isoformat()\n'],
        "empty_loop_delete": [binding, 'for unused in []:\n    del date\n', 'observed = date\n'],
        "unreachable_handler_delete": [binding, 'try:\n    pass\nexcept ValueError:\n    del date\n',
                                       'observed = date\n'],
        "nested_lambda_rhs": ['make = lambda: (lambda: date(2026, 1, 1))()\n'
                              'date = make()\nobserved = date.isoformat()\n'],
        "global_write": ['def setup():\n    global date\n    date = "31-3-2026"\nsetup()\n', 'observed = date\n'],
        "globals_write": ['globals()["date"] = "31-3-2026"\n', 'observed = date\n'],
        "exec_write": ["exec('date = \"31-3-2026\"')\n", 'observed = date\n'],
        "star_write": ['from review_exports import *\n', 'observed = date\n'],
        "dynamic_absent": ['globals()["unrelated"] = "customer"\n', constructor],
        "match_capture": ['match "31-3-2026":\n    case date:\n        pass\n', 'observed = date\n'],
        "except_capture": ['try:\n    raise ValueError("31-3-2026")\n'
                           'except ValueError as date:\n    observed = str(date)\n'],
        "deleted_cell": [binding, 'del date\n', constructor],
        "delete_rebind": [binding, 'del date\n', binding, 'observed = date\n'],
        "run_deletion": [binding, "%run /Workspace/test_imports/consumer.ipynb\n", constructor],
    }[scenario]
    nb = _notebook(*(magic + '# IMPORT_ORDER_REGRESSION\n' + src for src in sources))
    if scenario in {"parent_inheritance", "run_deletion"}:
        nb["cells"][1]["source"] = [sources[1]]
    if scenario == "spaced_cells":
        nb["cells"].insert(1, {"cell_type": "markdown", "source": ["# date = prose"]})
        nb["cells"].insert(3, _notebook("")["cells"][0])
    _patch_common(monkeypatch, tmp_path, nb)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    if scenario in {"parent_inheritance", "run_deletion"}:
        child = _notebook(magic + '# IMPORT_ORDER_REGRESSION\n' + (
            'observed = date\n' if scenario == "parent_inheritance" else 'del date\n'))
        for child_path in ("/Workspace/test_imports/consumer.ipynb",
                           jm.MIGRATED_BASE + "/test_imports/consumer.ipynb"):
            monkeypatch.setitem(jm._notebook_content_cache, child_path, json.dumps(child))

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__(on_disk_cells=(1 if scenario in {
                "parent_inheritance", "run_deletion"} else len(sources)))
            self.namespace = {}

        async def execute(self, code, **kwargs):
            if scenario == "run_deletion" and "__AIDP_CHILD_COUNT__" in code:
                self.on_disk_cells = 1 if "consumer.ipynb" in code else len(sources)
            result = await super().execute(code, **kwargs)
            if "# IMPORT_ORDER_REGRESSION" in code and "base64.b64decode" not in code:
                python = "\n".join(line for line in code.splitlines()
                                   if line.strip() not in {"%python", "%pyspark"})
                # A missing import fails the test at the real execute boundary;
                # the repair model must not manufacture a passing fixture.
                exec(python, self.namespace)
            return result

    monkeypatch.setitem(sys.modules, "review_exports", SimpleNamespace(date="31-3-2026"))
    kernel = Kernel()
    source_path = jm.MIGRATED_BASE + "/source.ipynb"
    if path == "main":
        result = asyncio.run(jm.process_notebook(
            source_path, kernel, "test_imports", "01_task", {}))
        assert result["status"] == "PASS", result
    else:
        scope = jm._resolve_symbol_scope(source_path, nb["cells"])
        for index, cell in enumerate(nb["cells"]):
            cell["source"] = [jm._add_missing_imports(
                "".join(cell["source"]), scope, cell_index=index)]
        monkeypatch.setitem(jm._notebook_content_cache, source_path, json.dumps(nb))
        log = jm.NotebookLog(source_path, "test_imports", "child")
        result = asyncio.run(jm._inline_child_notebook(
            source_path, kernel, log, "test_imports", "", "", "", {}))
        assert result["status"] == "ok", result

    assert kernel.namespace["observed"] == (
        "31-3-2026" if scenario in {"parent_inheritance", "delete_rebind", "conditional_delete",
                                   "empty_loop_delete", "unreachable_handler_delete", "global_write",
                                   "globals_write", "exec_write", "star_write", "match_capture",
                                   "except_capture"}
        else "2026-01-01")
    if scenario.startswith("future") or scenario == "spaced_cells":
        assert kernel.namespace["sheet_value"] == "31-3-2026"


@pytest.mark.parametrize("magic", ["", "%python\n"])
def test_parent_binding_reaches_child_importer(monkeypatch, tmp_path, magic):
    test_import_visibility_order_through_real_cell_loops(
        monkeypatch, tmp_path, "main", magic, "parent_inheritance")


def test_nested_run_visibility_inherits_only_prior_parent_bindings(monkeypatch):
    child = "/Workspace/imports/child.ipynb"
    monkeypatch.setattr(jm, "_notebook_content_cache", {
        child: json.dumps(_notebook('child_value = "child"\n')),
    })
    cells = _notebook('date = "parent"\n', f"%run {child}\n", 'later = 1\n')["cells"]
    scope = jm._resolve_symbol_scope("/Workspace/imports/parent.ipynb", cells)
    scope["import_cell_index"] = 1
    assert "date" in jm._visible_import_bindings(scope)
    assert "child_value" not in jm._visible_import_bindings(scope)
    assert "later" not in jm._visible_import_bindings(scope)
    scope["import_cell_index"] = 2
    assert "child_value" in jm._visible_import_bindings(scope)


def test_name_error_union_keeps_nested_and_future_bindings():
    cells = _notebook('def helper():\n    local_value = 1\n', 'future = 2\n')["cells"]
    scope = jm._resolve_symbol_scope("/Workspace/imports.ipynb", cells)
    assert {"helper", "local_value", "future"} <= scope["defined"]
    assert "local_value" not in scope["import_defined"]
    assert scope["import_defined_before"] == [set(), {"helper"}]


def test_local_function_reads_do_not_mask_failed_widget_binding():
    source = ('def helper(date):\n    return date\n'
              'date = aidputils.widgets.get("endDate")\n'
              'spreadsheet.worksheet(date)\n')
    assert jm._add_missing_imports(source) == source


@pytest.mark.parametrize("magic", ["%r", "%sql", "%scala", "%md"])
def test_non_python_bindings_do_not_suppress_python_imports(magic):
    cells = _notebook(magic + '\ndate = "other-language"\n',
                     'observed = date(2026, 1, 1)\n')["cells"]
    scope = jm._resolve_symbol_scope("/Workspace/imports.ipynb", cells)
    result = jm._add_missing_imports("".join(cells[1]["source"]), scope, cell_index=1)
    assert "from datetime import date" in result


@pytest.mark.parametrize("path", ["main", "child"])
@pytest.mark.parametrize("magic", ["", "%python\n"])
@pytest.mark.parametrize("change", ["adds_binding", "removes_binding"])
def test_accepted_repair_refreshes_downstream_import_scope(monkeypatch, tmp_path, path, magic, change):
    from test_conditional_exit_below_execute import _patch_common
    from test_leading_magic_injection import _Kernel

    original = 'legacy = legacy_api()\n' if change == "adds_binding" else 'date = legacy_api()\n'
    replacement = 'date = current_api()\n' if change == "adds_binding" else 'legacy = current_api()\n'
    repaired = ('# BINDING_REPAIR\n# ' + original.rstrip() + '\n'
                '# Oracle tool modification: use current API\n' + replacement)
    consumer = ('observed = date\n' if change == "adds_binding" else
                'observed = date(2026, 1, 1).isoformat()\n')
    nb = _notebook(magic + '# BINDING_REPAIR\n' + original,
                   magic + '# BINDING_REPAIR\n' + consumer)
    _patch_common(monkeypatch, tmp_path, nb)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    fixes = []

    async def fix(code, *a, **k):
        fixes.append(code)
        assert "legacy_api()" in code
        return magic + repaired

    async def evaluate(*a, **k):
        return "PASS", {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "call_opus_with_tools", evaluate)

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__(on_disk_cells=2)
            self.namespace = {"current_api": lambda: "customer-date"}

        async def execute(self, code, **kwargs):
            result = await super().execute(code, **kwargs)
            if "# BINDING_REPAIR" in code and "base64.b64decode" not in code:
                if "= legacy_api()" in code and "# Oracle tool modification: use current API" not in code:
                    return {"status": "error", "outputs": [{"type": "error", "ename": "ValueError",
                            "evalue": "legacy API is unsupported", "traceback": ["ValueError: legacy API is unsupported"]}]}
                exec("\n".join(line for line in code.splitlines() if line.strip() != "%python"), self.namespace)
            return result

    kernel = Kernel()
    source_path = jm.MIGRATED_BASE + "/repair_source.ipynb"
    if path == "main":
        result = asyncio.run(jm.process_notebook(source_path, kernel, "test_imports", "01_task", {}))
        assert result["status"] == "PASS", result
    else:
        monkeypatch.setitem(jm._notebook_content_cache, source_path, json.dumps(nb))
        result = asyncio.run(jm._inline_child_notebook(
            source_path, kernel, jm.NotebookLog(source_path, "test_imports", "child"),
            "test_imports", "", "", "", {}))
        assert result["status"] == "ok", result
    assert len(fixes) == 1
    assert kernel.namespace["observed"] == ("customer-date" if change == "adds_binding" else "2026-01-01")


@pytest.mark.parametrize("import_line", PYTHON_IMPORTS)
@pytest.mark.parametrize("present", [False, True])
def test_uncertain_namespace_checks_each_import_at_runtime(monkeypatch, import_line, present):
    module, symbol = import_line.removeprefix("from ").split(" import ")
    imported = lambda: "imported-value"
    customer = lambda: "customer-value"
    import builtins
    def fixture_import(name, *args, **kwargs):
        assert name == module
        return SimpleNamespace(**{symbol: imported})
    source = f"observed = {symbol}()\n"
    scope = jm._resolve_symbol_scope("/Workspace/review/runtime.ipynb",
                                    _notebook('exec("unrelated = 1")\n', source)["cells"])
    transformed = jm._add_missing_imports(source, scope, cell_index=1)
    namespace = {symbol: customer} if present else {}
    namespace["__builtins__"] = dict(vars(builtins), __import__=fixture_import)
    exec(transformed, namespace)
    assert namespace["observed"] == ("customer-value" if present else "imported-value")
    assert jm._add_missing_imports(transformed, scope, cell_index=1) == transformed
