"""Regression: Phase 1 data check for repository-relative and imported paths.

Incident (Phase 1, 2026-10-16, export run 2026-10-16T11:14:09Z-cc33d9c0):
``DataBricks_Access_Control`` (DBX job 300000000000004) is a Git notebook that
does ``from imports.constants import *`` and later
``pd.read_csv(relative_file_path)`` where ``relative_file_path`` comes from
``ACCESS_CONTROL_CSV_RELATVE_PATH = "database/access_control.csv"`` in the
imported module.  The exported repository had no ``database/`` folder, yet the
data check reported 0 paths and 0 unresolved reads for the notebook: the
constant was never seen and relative or unknown read paths were dropped.

The saved (migrated) notebook runs with its folder and each parent folder up
to the export root on ``sys.path``, nearest first, and changes the working
directory to the notebook folder for Git notebooks
(``job_migrate.build_artifact_workspace_bootstrap_cell``).  The data check
mirrors that: imported workspace modules supply constants, relative reads of
Git notebooks resolve against the notebook folder, and anything it cannot
resolve is reported as unresolved instead of being dropped.
"""
import asyncio
import os
import sys

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import check_data_availability_for_workflow as data_checker  # noqa: E402
from data_check_extract import extract_from_notebook, python_import_froms  # noqa: E402
from data_check_imports import (  # noqa: E402
    ImportedModuleSymbols,
    module_search_dirs,
    repository_notebook_dir,
)

EXPORT = "/Workspace/exp"
GIT = f"{EXPORT}/notebooks/GIT"
NOTEBOOK = f"{GIT}/DataBricks_Access_Control.ipynb"


def _nb(*cells):
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"language_info": {"name": "python"}},
        "cells": [
            {"cell_type": "code", "source": cell.splitlines(keepends=True),
             "metadata": {}, "outputs": [], "execution_count": None}
            for cell in cells
        ],
    }


def _reads(result):
    return {ref["object_name"]: ref for ref in result["read_references"]}


def _unresolved_reads(result):
    return [ref for ref in result["unresolved_scopes"] if ref.get("object_name") is None]


# ─── Extractor: where a non-Spark read path points ───────────────────────

@pytest.mark.parametrize("relative, expected", [
    ("database/access_control.csv", f"{GIT}/database/access_control.csv"),
    ("./database/access_control.csv", f"{GIT}/database/access_control.csv"),
    ("../shared/lookup.csv", f"{EXPORT}/notebooks/shared/lookup.csv"),
])
@pytest.mark.parametrize("reader", ["pd.read_csv({})", "open({})", "open({}, 'r')"])
def test_relative_read_of_repository_notebook_resolves_against_notebook_folder(
    relative, expected, reader,
):
    nb = _nb(f"data = {reader.format(repr(relative))}")
    result = extract_from_notebook(nb, notebook_dir=GIT)

    ref = _reads(result)[expected]
    assert ref["usage"] == "read"
    assert ref["relative_path"] == relative
    assert ref["base_directory"] == GIT
    assert expected in result["paths"]
    assert not _unresolved_reads(result)


def test_incident_shape_constant_from_initial_symbols_resolves():
    nb = _nb(
        "relative_file_path = ACCESS_CONTROL_CSV_RELATVE_PATH\n"
        "access_control_csv = pd.read_csv(relative_file_path)\n"
    )
    result = extract_from_notebook(
        nb,
        initial_symbols={"ACCESS_CONTROL_CSV_RELATVE_PATH": "database/access_control.csv"},
        notebook_dir=GIT,
    )
    ref = _reads(result)[f"{GIT}/database/access_control.csv"]
    assert ref["argument_expression"] == "relative_file_path"
    assert ref["api"] == "pd.read_csv"


def test_relative_read_without_known_working_directory_is_unresolved():
    nb = _nb("df = pd.read_csv('database/access_control.csv')")
    result = extract_from_notebook(nb)

    assert not result["read_references"]
    [gap] = _unresolved_reads(result)
    assert gap["argument_expression"] == "'database/access_control.csv'"
    assert gap["api"] == "pd.read_csv"
    assert "working directory" in gap["reason"]


def test_unknown_read_path_is_unresolved_not_dropped():
    nb = _nb("relative_file_path = ACCESS_CONTROL_CSV_RELATVE_PATH\n"
             "df = pd.read_csv(relative_file_path)\n")
    result = extract_from_notebook(nb, notebook_dir=GIT)

    [gap] = _unresolved_reads(result)
    assert gap["argument_expression"] == "relative_file_path"
    assert gap["cell_number"] == 1 and gap["line_number"] == 2
    assert "not statically resolvable" in gap["reason"]


def test_relative_path_escaping_the_workspace_is_unresolved():
    nb = _nb("df = pd.read_csv('../../../../../outside.csv')")
    result = extract_from_notebook(nb, notebook_dir=GIT)

    assert not result["read_references"]
    [gap] = _unresolved_reads(result)
    assert "outside /Workspace" in gap["reason"]


@pytest.mark.parametrize("source", [
    "df = pd.read_csv(io.StringIO(text))",
    "df = pd.read_csv(io.BytesIO(payload))",
    "df = pd.read_csv(StringIO(text))",
    "df = pd.read_csv('/tmp/scratch.csv')",
    "df = pd.read_csv('https://example.invalid/data.csv')",
    "with open('database/out.csv', 'w') as f:\n    f.write('x')",
])
def test_non_workspace_inputs_are_not_reported(source):
    result = extract_from_notebook(_nb(source), notebook_dir=GIT)
    assert not result["read_references"]
    assert not _unresolved_reads(result)


def test_absolute_workspace_read_is_unchanged():
    nb = _nb("df = pd.read_csv('/Workspace/project/config/Feature_List.csv')")
    ref = _reads(extract_from_notebook(nb, notebook_dir=GIT))[
        "/Workspace/project/config/Feature_List.csv"]
    assert "relative_path" not in ref


def test_python_import_froms_lists_module_level_imports_only():
    nb = _nb(
        "!pip install pyaml",
        "import os\nfrom imports.imports import*\nfrom imports.constants import*\n"
        "from helperFunctions.helper import load_yaml as ly, other\n"
        "from . import sibling\n"
        "try:\n    from settings.local import BASE\nexcept ImportError:\n    pass\n"
        "def f():\n    from inner.module import hidden\n",
    )
    assert [(i["module"], i["names"], i["level"], i["cell_number"])
            for i in python_import_froms(nb)] == [
        ("imports.imports", [("*", None)], 0, 2),
        ("imports.constants", [("*", None)], 0, 2),
        ("helperFunctions.helper", [("load_yaml", "ly"), ("other", None)], 0, 2),
        (None, [("sibling", None)], 1, 2),
        ("settings.local", [("BASE", None)], 0, 2),
    ]


# ─── Imported module resolution ──────────────────────────────────────────

def test_search_dirs_mirror_phase2_sys_path():
    assert module_search_dirs(f"{GIT}/jobs/daily/run.ipynb") == [
        f"{GIT}/jobs/daily", f"{GIT}/jobs", GIT, f"{EXPORT}/notebooks", EXPORT,
    ]
    assert module_search_dirs(
        f"{EXPORT}/notebooks/Users/a@b.com/proj/nb.ipynb") == [
        f"{EXPORT}/notebooks/Users/a@b.com/proj", f"{EXPORT}/notebooks/Users/a@b.com",
        f"{EXPORT}/notebooks/Users", f"{EXPORT}/notebooks", EXPORT,
    ]
    assert module_search_dirs("/Workspace/Users/a@b.com/nb.ipynb") == []
    assert repository_notebook_dir(f"{GIT}/jobs/run.ipynb") == f"{GIT}/jobs"
    assert repository_notebook_dir(f"{EXPORT}/notebooks/Users/a@b.com/nb.ipynb") is None


class FakeWorkspace:
    """In-memory workspace: ``files`` maps absolute paths to source text."""

    def __init__(self, files, failing_lists=(), failing_reads=()):
        self.files = dict(files)
        self.failing_lists = set(failing_lists)
        self.failing_reads = set(failing_reads)
        self.list_calls = []
        self.read_calls = []

    def list_entries(self, directory):
        self.list_calls.append(directory)
        if directory in self.failing_lists:
            raise RuntimeError("HTTP 500 listing")
        prefix = directory.rstrip("/") + "/"
        files, folders = set(), set()
        for path in self.files:
            if path.startswith(prefix):
                head, _, rest = path[len(prefix):].partition("/")
                (folders if rest else files).add(head)
        if not files and not folders:
            return None
        return frozenset(files), frozenset(folders)

    def read_file(self, path):
        self.read_calls.append(path)
        if path in self.failing_reads:
            raise RuntimeError("HTTP 500 download")
        return self.files[path].encode()


def _resolver(workspace):
    return ImportedModuleSymbols(workspace.list_entries, workspace.read_file)


def _bound(workspace, notebook, notebook_path=NOTEBOOK, resolver=None):
    """Names bound at the end of the notebook, and the resolver's gaps."""
    namespaces, gaps = (resolver or _resolver(workspace)).for_notebook(notebook, notebook_path)
    result = extract_from_notebook(notebook, import_namespaces=namespaces)
    return result["resolved_symbols"], gaps


INCIDENT_NOTEBOOK = _nb(
    "!pip install slack_sdk",
    "import os\nfrom imports.imports import*\nfrom imports.constants import*\n",
    "relative_file_path = ACCESS_CONTROL_CSV_RELATVE_PATH\n"
    "access_control_csv = pd.read_csv(relative_file_path)\n",
)
INCIDENT_FILES = {
    f"{GIT}/imports/imports.py": "import pandas as pd\nimport yaml\n",
    f"{GIT}/imports/constants.py": (
        'ACCESS_CONTROL_CSV_RELATVE_PATH = "database/access_control.csv"\n'
        'JOB_MANAGE_LEVEL_PERMISSION_CONFIG_PATH = "database/job_manage_permissions.yaml"\n'
        '_PRIVATE = "hidden"\n'
    ),
}


def test_star_import_binds_public_module_constants():
    workspace = FakeWorkspace(INCIDENT_FILES)
    symbols, gaps = _bound(workspace, INCIDENT_NOTEBOOK)

    assert symbols["ACCESS_CONTROL_CSV_RELATVE_PATH"] == "database/access_control.csv"
    assert symbols["JOB_MANAGE_LEVEL_PERMISSION_CONFIG_PATH"] == "database/job_manage_permissions.yaml"
    assert "_PRIVATE" not in symbols
    assert gaps == []


def test_named_import_alias_chain_and_ancestor_search():
    files = {
        f"{GIT}/settings/base.py": 'ROOT = "database"\n',
        f"{GIT}/settings/__init__.py": "",
        f"{GIT}/imports/constants.py": (
            "from settings.base import ROOT\n"
            'CSV = ROOT + "/access_control.csv"\n'
        ),
    }
    notebook = _nb("from imports.constants import CSV as ACL\n")
    symbols, gaps = _bound(FakeWorkspace(files), notebook, f"{GIT}/jobs/daily/run.ipynb")
    assert symbols == {"ACL": "database/access_control.csv"}
    assert gaps == []


def test_package_init_and_all_are_honoured():
    files = {
        f"{GIT}/config/__init__.py": '__all__ = ["A"]\nA = "a.csv"\nB = "b.csv"\n',
    }
    symbols, _ = _bound(FakeWorkspace(files), _nb("from config import *\n"))
    assert symbols == {"A": "a.csv"}


def test_installed_packages_are_not_workspace_modules():
    workspace = FakeWorkspace(INCIDENT_FILES)
    symbols, gaps = _bound(
        workspace,
        _nb("from sklearn.metrics import roc_auc_score\nfrom os.path import join\n"))
    assert symbols == {} and gaps == []
    assert workspace.read_calls == []


def test_folder_listings_are_cached_across_notebooks():
    workspace = FakeWorkspace(INCIDENT_FILES)
    resolver = _resolver(workspace)
    resolver.for_notebook(INCIDENT_NOTEBOOK, NOTEBOOK)
    calls = len(workspace.list_calls)
    resolver.for_notebook(INCIDENT_NOTEBOOK, f"{GIT}/other.ipynb")
    assert len(workspace.list_calls) == calls
    assert len(workspace.read_calls) == 2


def test_module_read_failure_is_reported_not_analyzed():
    workspace = FakeWorkspace(
        INCIDENT_FILES, failing_reads={f"{GIT}/imports/constants.py"})
    symbols, gaps = _bound(workspace, INCIDENT_NOTEBOOK)

    assert "ACCESS_CONTROL_CSV_RELATVE_PATH" not in symbols
    [gap] = gaps
    assert gap["kind"] == "python_module"
    assert gap["module"] == "imports.constants"
    assert gap["path"] == f"{GIT}/imports/constants.py"
    assert "HTTP 500 download" in gap["reason"]


def test_cached_module_failure_is_reported_for_every_importing_notebook():
    workspace = FakeWorkspace(
        INCIDENT_FILES, failing_reads={f"{GIT}/imports/constants.py"})
    resolver = _resolver(workspace)
    _, first = resolver.for_notebook(INCIDENT_NOTEBOOK, NOTEBOOK)
    _, second = resolver.for_notebook(INCIDENT_NOTEBOOK, f"{GIT}/other.ipynb")

    assert first == second
    assert [gap["module"] for gap in second] == ["imports.constants"]
    assert workspace.read_calls.count(f"{GIT}/imports/constants.py") == 1


def test_folder_listing_failure_is_reported_not_analyzed():
    workspace = FakeWorkspace(INCIDENT_FILES, failing_lists={GIT})
    _, gaps = _resolver(workspace).for_notebook(INCIDENT_NOTEBOOK, NOTEBOOK)
    assert any(
        gap["kind"] == "python_module_search" and gap["directory"] == GIT
        and "HTTP 500 listing" in gap["reason"]
        for gap in gaps
    )


def test_unparsable_module_is_reported_not_analyzed():
    files = dict(INCIDENT_FILES)
    files[f"{GIT}/imports/constants.py"] = "def broken(:\n"
    _, gaps = _resolver(FakeWorkspace(files)).for_notebook(INCIDENT_NOTEBOOK, NOTEBOOK)
    assert any(
        gap["kind"] == "python_module" and gap["module"] == "imports.constants"
        and "could not be analysed" in gap["reason"]
        for gap in gaps
    )


def test_import_cycle_is_reported_and_terminates():
    files = {
        f"{GIT}/a.py": "from b import *\nA = 'a.csv'\n",
        f"{GIT}/b.py": "from a import *\nB = 'b.csv'\n",
    }
    symbols, gaps = _bound(FakeWorkspace(files), _nb("from a import *\n"))
    assert symbols["A"] == "a.csv" and symbols["B"] == "b.csv"
    assert any("cycle" in gap["reason"] for gap in gaps)


# ─── Binding order and scope (which binding a read sees) ────────────────

ORDER_FILES = {
    f"{GIT}/settings.py": 'CSV = "data/missing.csv"\n',
    f"{GIT}/other.py": 'CSV = "data/present.csv"\n',
}


def _notebook_reads(files, *cells, notebook_path=NOTEBOOK):
    notebook = _nb(*cells)
    namespaces, _ = _resolver(FakeWorkspace(files)).for_notebook(notebook, notebook_path)
    result = extract_from_notebook(
        notebook, import_namespaces=namespaces,
        notebook_dir=repository_notebook_dir(notebook_path))
    reads = sorted((r["cell_number"], r["object_name"]) for r in result["read_references"])
    gaps = [(u["cell_number"], u["argument_expression"]) for u in _unresolved_reads(result)]
    return reads, gaps


def test_each_read_sees_the_import_active_at_its_position():
    reads, gaps = _notebook_reads(
        ORDER_FILES,
        "from settings import CSV", "pd.read_csv(CSV)",
        "from other import CSV", "pd.read_csv(CSV)",
    )
    assert reads == [(2, f"{GIT}/data/missing.csv"), (4, f"{GIT}/data/present.csv")]
    assert gaps == []


def test_import_in_a_branch_that_cannot_run_binds_nothing():
    reads, gaps = _notebook_reads(
        ORDER_FILES,
        "from settings import CSV\nif False:\n    from other import CSV\n",
        "pd.read_csv(CSV)",
    )
    assert reads == [(2, f"{GIT}/data/missing.csv")] and gaps == []


@pytest.mark.parametrize("cell", [
    "from settings import CSV\nif flag:\n    from other import CSV\n",
    "if flag:\n    from settings import CSV\nelse:\n    from other import CSV\n",
    "try:\n    from settings import CSV\nexcept ImportError:\n    from other import CSV\n",
    "from settings import CSV\nfor item in items:\n    from other import CSV\n",
    "CSV = 'data/missing.csv'\nif flag:\n    CSV = 'data/present.csv'\n",
])
def test_binding_that_depends_on_an_unknown_path_is_unresolved(cell):
    reads, gaps = _notebook_reads(ORDER_FILES, cell, "pd.read_csv(CSV)")
    assert reads == []
    assert gaps == [(2, "CSV")]


def test_same_binding_on_every_path_stays_resolved():
    reads, gaps = _notebook_reads(
        ORDER_FILES,
        "if flag:\n    from settings import CSV\nelse:\n    CSV = 'data/missing.csv'\n",
        "pd.read_csv(CSV)",
    )
    assert reads == [(2, f"{GIT}/data/missing.csv")] and gaps == []


@pytest.mark.parametrize("rebind", [
    "from tools import CSV",             # a workspace module binding a non-constant
    "from sklearn.datasets import CSV",  # an installed package
    "import CSV",
    "def CSV():\n    pass",
    "class CSV:\n    pass",
    "for CSV in items:\n    pass",
    "with open('x') as CSV:\n    pass",
    "CSV += suffix",
    "del CSV",
])
def test_any_rebinding_of_the_name_hides_the_earlier_constant(rebind):
    files = dict(ORDER_FILES)
    files[f"{GIT}/tools.py"] = "def CSV():\n    return 'data/present.csv'\n"
    reads, gaps = _notebook_reads(files, "from settings import CSV", rebind, "pd.read_csv(CSV)")
    assert (3, f"{GIT}/data/missing.csv") not in reads
    assert (3, "CSV") in gaps


def test_function_local_assignment_does_not_leak_into_notebook_globals():
    reads, gaps = _notebook_reads(
        {},
        "CSV = 'data/missing.csv'\ndef helper():\n    CSV = 'data/present.csv'\n    frame = pd.read_csv(CSV)\n",
        "pd.read_csv(CSV)",
    )
    assert (2, f"{GIT}/data/missing.csv") in reads
    assert (2, f"{GIT}/data/present.csv") not in reads
    # The read inside the function still sees the function's own binding.
    assert (1, f"{GIT}/data/present.csv") in reads


def test_global_assignment_inside_a_function_makes_the_name_unknown():
    reads, gaps = _notebook_reads(
        {},
        "CSV = 'data/missing.csv'\ndef helper():\n    global CSV\n    CSV = 'data/present.csv'\n",
        "pd.read_csv(CSV)",
    )
    assert reads == [] and gaps == [(2, "CSV")]


def test_module_function_locals_are_not_module_constants():
    files = {f"{GIT}/settings.py": (
        'CSV = "data/missing.csv"\n'
        "def helper():\n"
        '    CSV = "data/present.csv"\n'
        "class Config:\n"
        '    CSV = "data/other.csv"\n'
    )}
    reads, gaps = _notebook_reads(files, "from settings import *", "pd.read_csv(CSV)")
    assert reads == [(2, f"{GIT}/data/missing.csv")] and gaps == []


def test_module_import_order_decides_the_module_constant():
    files = dict(ORDER_FILES)
    files[f"{GIT}/consts.py"] = "from settings import CSV\nfrom other import CSV\n"
    reads, _ = _notebook_reads(files, "from consts import CSV", "pd.read_csv(CSV)")
    assert reads == [(2, f"{GIT}/data/present.csv")]


def test_handler_does_not_assume_the_try_body_never_ran():
    reads, gaps = _notebook_reads(
        {},
        "CSV = 'data/present.csv'\ntry:\n    CSV = 'data/missing.csv'\n    raise RuntimeError()\n"
        "except RuntimeError:\n    frame = pd.read_csv(CSV)\n",
    )
    assert reads == [] and gaps == [(1, "CSV")]


def test_handler_keeps_bindings_the_try_body_does_not_touch():
    reads, gaps = _notebook_reads(
        {},
        "CSV = 'data/present.csv'\ntry:\n    other = 1\nexcept RuntimeError:\n    frame = pd.read_csv(CSV)\n",
    )
    assert reads == [(1, f"{GIT}/data/present.csv")] and gaps == []


def test_loop_body_does_not_assume_its_first_iteration():
    reads, gaps = _notebook_reads(
        {},
        "CSV = 'data/present.csv'\nfor item in items:\n    frame = pd.read_csv(CSV)\n    CSV = 'data/missing.csv'\n",
    )
    assert reads == [] and gaps == [(1, "CSV")]


@pytest.mark.parametrize("header", [
    "def load(CSV):", "def load(CSV='data/other.csv'):", "def load(*CSV):",
    "def load(*, CSV):", "def load(**CSV):", "def load(CSV, /):",
    "async def load(CSV):",
])
def test_function_parameters_shadow_globals(header):
    reads, gaps = _notebook_reads(
        ORDER_FILES,
        "from settings import CSV",
        f"{header}\n    frame = pd.read_csv(CSV)\n",
        "load('data/present.csv')",
    )
    assert reads == [] and gaps == [(2, "CSV")]


def test_name_assigned_later_in_the_function_is_local_throughout():
    reads, gaps = _notebook_reads(
        ORDER_FILES,
        "from settings import CSV",
        "def load():\n    frame = pd.read_csv(CSV)\n    CSV = 'data/present.csv'\n",
    )
    assert reads == [] and gaps == [(2, "CSV")]


def test_function_body_does_not_trust_a_global_rebound_elsewhere():
    reads, gaps = _notebook_reads(
        {},
        "CSV = 'data/missing.csv'",
        "def load():\n    frame = pd.read_csv(CSV)\n",
        "CSV = 'data/present.csv'\nload()\n",
    )
    assert reads == [] and gaps == [(2, "CSV")]


def test_function_body_reads_a_global_bound_once():
    reads, gaps = _notebook_reads(
        ORDER_FILES,
        "from settings import CSV",
        "def load():\n    frame = pd.read_csv(CSV)\n",
        "load()",
    )
    assert reads == [(2, f"{GIT}/data/missing.csv")] and gaps == []


def test_module_function_does_not_change_module_constants():
    files = {f"{GIT}/settings.py": (
        "def configure(CSV):\n    frame = pd.read_csv(CSV)\n"
        'CSV = "data/missing.csv"\n'
    )}
    reads, gaps = _notebook_reads(files, "from settings import CSV", "pd.read_csv(CSV)")
    assert reads == [(2, f"{GIT}/data/missing.csv")] and gaps == []


# A class body is a scope its nested functions, classes, lambdas and
# comprehensions cannot see; lambda parameters and comprehension targets are
# local to their own scope.  Each read resolves to what Python reads there.
MISSING = f"{GIT}/data/missing.csv"
PRESENT = f"{GIT}/data/present.csv"
CLASS_HEAD = "class Reader:\n    CSV = 'data/present.csv'\n"


@pytest.mark.parametrize("cell, expected", [
    (CLASS_HEAD + "    def read(self):\n        frame = pd.read_csv(CSV)\n", MISSING),
    (CLASS_HEAD + "    async def read(self):\n        frame = pd.read_csv(CSV)\n", MISSING),
    (CLASS_HEAD + "    class Inner:\n        frame = pd.read_csv(CSV)\n", MISSING),
    (CLASS_HEAD + "    if flag:\n        def read(self):\n            frame = pd.read_csv(CSV)\n",
     MISSING),
    (CLASS_HEAD + "    read = lambda self: pd.read_csv(CSV)\n", MISSING),
    (CLASS_HEAD + "    frames = [pd.read_csv(CSV) for _ in range(2)]\n", MISSING),
    (CLASS_HEAD + "    frame = pd.read_csv(CSV)\n", PRESENT),
    (CLASS_HEAD + "    rows = [row for row in pd.read_csv(CSV)]\n", PRESENT),
    ("def build():\n    CSV = 'data/present.csv'\n    class Reader:\n"
     "        CSV = 'data/other.csv'\n        def read(self):\n"
     "            frame = pd.read_csv(CSV)\n", PRESENT),
])
def test_class_body_names_are_hidden_from_nested_scopes(cell, expected):
    reads, gaps = _notebook_reads({}, "CSV = 'data/missing.csv'", cell)
    assert reads == [(2, expected)] and gaps == []


def test_method_without_a_global_is_unresolved_not_the_class_attribute():
    reads, gaps = _notebook_reads(
        {}, CLASS_HEAD + "    def read(self):\n        frame = pd.read_csv(CSV)\n")
    assert reads == [] and gaps == [(1, "CSV")]


@pytest.mark.parametrize("cell", [
    "load = lambda CSV: pd.read_csv(CSV)",
    "load = lambda *CSV: pd.read_csv(CSV)",
    "frames = [pd.read_csv(CSV) for CSV in paths]",
    "frames = {pd.read_csv(CSV) for CSV in paths}",
    "frames = (pd.read_csv(CSV) for CSV in paths)",
    "frames = {CSV: pd.read_csv(CSV) for CSV in paths}",
    "frames = [pd.read_csv(CSV) for _, CSV in pairs]",
    "frames = [row for CSV in paths for row in pd.read_csv(CSV)]",
    "frames = [CSV for CSV in paths if pd.read_csv(CSV) is not None]",
])
def test_lambda_and_comprehension_names_shadow_globals(cell):
    reads, gaps = _notebook_reads({}, "CSV = 'data/missing.csv'", cell)
    assert reads == [] and gaps == [(2, "CSV")]


@pytest.mark.parametrize("cell", [
    "frames = [pd.read_csv(CSV) for _ in range(2)]",
    "rows = [row for row in pd.read_csv(CSV)]",
    "load = lambda path=pd.read_csv(CSV): path",
    "load = lambda: pd.read_csv(CSV)",
])
def test_lambda_and_comprehension_still_see_unshadowed_globals(cell):
    reads, gaps = _notebook_reads({}, "CSV = 'data/missing.csv'", cell)
    assert reads == [(2, MISSING)] and gaps == []


@pytest.mark.parametrize("cell", [
    "class A:\n    global CSV\n    CSV = 'data/present.csv'\n"
    "    frames = [pd.read_csv(CSV) for _ in range(1)]\n",
    "class A:\n    global CSV\n    CSV = 'data/present.csv'\n"
    "    class B:\n        frame = pd.read_csv(CSV)\n",
    "class A:\n    global CSV\n    CSV = 'data/present.csv'\n"
    "    load = lambda self: pd.read_csv(CSV)\n",
    "def build():\n    CSV = 'data/missing.csv'\n    class A:\n        nonlocal CSV\n"
    "        CSV = 'data/present.csv'\n"
    "        frames = [pd.read_csv(CSV) for _ in range(1)]\n",
])
def test_class_body_global_rebinding_is_unknown_to_nested_scopes(cell):
    reads, gaps = _notebook_reads({}, "CSV = 'data/missing.csv'", cell)
    assert reads == [] and gaps == [(2, "CSV")]


@pytest.mark.parametrize("cell", [
    "load = lambda: pd.read_csv(CSV)",
    "frames = (pd.read_csv(CSV) for _ in range(2))",
    "frames = [lambda: pd.read_csv(CSV) for _ in range(2)]",
])
def test_lambda_and_generator_bodies_do_not_trust_a_rebound_global(cell):
    reads, gaps = _notebook_reads(
        {}, "CSV = 'data/missing.csv'", cell, "CSV = 'data/present.csv'")
    assert reads == [] and gaps == [(2, "CSV")]


def test_comprehension_runs_immediately_with_the_current_binding():
    reads, gaps = _notebook_reads(
        {}, "CSV = 'data/missing.csv'", "frames = [pd.read_csv(CSV) for _ in range(2)]",
        "CSV = 'data/present.csv'")
    assert reads == [(2, MISSING)] and gaps == []


# ─── Module lookup follows Python's import system ───────────────────────

JOB = f"{GIT}/jobs"
JOB_NOTEBOOK = f"{JOB}/run.ipynb"

# Each layout: files (relative to GIT), the import, and the file Python
# loads (relative to GIT), "namespace", "unsupported" or None (not found).
LOOKUP_LAYOUTS = {
    "package beats module in the same folder": (
        {"config/__init__.py": "", "config/settings.py": 'CSV = "data/present.csv"\n',
         "config/settings/__init__.py": 'CSV = "data/missing.csv"\n'},
        "config.settings", "config/settings/__init__.py"),
    "top-level package beats module": (
        {"settings.py": 'CSV = "data/present.csv"\n',
         "settings/__init__.py": 'CSV = "data/missing.csv"\n'},
        "settings", "settings/__init__.py"),
    "ancestor regular package beats nearer namespace folder": (
        {"jobs/config/other.py": "", "config/__init__.py": 'CSV = "data/missing.csv"\n'},
        "config", "config/__init__.py"),
    "ancestor module beats nearer namespace folder": (
        {"jobs/config/other.py": "", "config.py": 'CSV = "data/missing.csv"\n'},
        "config", "config.py"),
    "module in two folders": (
        {"jobs/config.py": 'CSV = "data/missing.csv"\n', "config/__init__.py": ""},
        "config", "ambiguous"),
    "submodule found in a later namespace portion": (
        {"jobs/ns/other.py": "", "ns/mod.py": 'CSV = "data/missing.csv"\n'},
        "ns.mod", "ns/mod.py"),
    "regular package in two folders": (
        {"jobs/pkg/__init__.py": "", "pkg/__init__.py": "", "pkg/mod.py": ""},
        "pkg.mod", "ambiguous"),
    "submodule in two namespace portions": (
        {"jobs/ns/mod.py": 'CSV = "data/missing.csv"\n', "ns/mod.py": ""},
        "ns.mod", "ambiguous"),
    "namespace package itself": (
        {"ns/mod.py": ""}, "ns", "namespace"),
    "plain module is not a package": (
        {"settings.py": ""}, "settings.sub", None),
    "source beats bytecode": (
        {"settings.py": 'CSV = "data/missing.csv"\n', "settings.pyc": ""},
        "settings", "settings.py"),
    "bytecode only": (
        {"settings.pyc": ""}, "settings", "unsupported"),
    "extension module beats source": (
        {"settings.py": 'CSV = "data/present.csv"\n', "EXT": ""},
        "settings", "unsupported"),
    "package with a compiled __init__": (
        {"settings/__init__.pyc": "", "settings.py": 'CSV = "data/present.csv"\n'},
        "settings", "unsupported"),
}


def _layout_files(layout):
    import importlib.machinery
    extension = "settings" + importlib.machinery.EXTENSION_SUFFIXES[0]
    return {f"{GIT}/{extension if rel == 'EXT' else rel}": text for rel, text in layout.items()}


def _python_finds(tmp_path, files, module, outermost_first=False):
    """What CPython's PathFinder selects for ``module`` with the saved
    notebook's sys.path order (nearest folder first), or with the
    migration-time order (outermost first), on a real file tree (nothing is
    executed)."""
    import importlib.machinery
    root = tmp_path / "Workspace"
    for path, text in files.items():
        target = root / path[len("/Workspace/"):]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    search = [str(root / d[len("/Workspace/"):]) for d in module_search_dirs(JOB_NOTEBOOK)]
    if outermost_first:
        search.reverse()
    importlib.invalidate_caches()
    spec = None
    parts = module.split(".")
    for index in range(len(parts)):
        spec = importlib.machinery.PathFinder.find_spec(".".join(parts[: index + 1]), search)
        if spec is None:
            return None
        search = list(spec.submodule_search_locations or [])
        if index < len(parts) - 1 and spec.submodule_search_locations is None:
            return None
    if spec.origin is None or spec.origin == "namespace":
        return "namespace"
    origin = spec.origin.replace(str(root), "/Workspace", 1)
    if not origin.endswith(".py"):
        return "unsupported"
    return origin[len(GIT) + 1:]


@pytest.mark.parametrize("name", sorted(LOOKUP_LAYOUTS))
def test_module_lookup_matches_cpython(name, tmp_path):
    layout, module, expected = LOOKUP_LAYOUTS[name]
    files = _layout_files(layout)
    nearest_first = _python_finds(tmp_path, files, module)
    outermost_first = _python_finds(tmp_path, files, module, outermost_first=True)
    if expected == "ambiguous":
        assert nearest_first != outermost_first, "layout expectation is wrong"
    else:
        assert nearest_first == outermost_first == expected, "layout expectation is wrong"

    workspace = FakeWorkspace(files)
    namespaces, gaps = _resolver(workspace).for_notebook(
        _nb(f"from {module} import CSV"), JOB_NOTEBOOK)
    if expected == "ambiguous":
        assert namespaces[module] is None
        assert [g["kind"] for g in gaps] == ["python_module"]
        assert "more than one searched folder" in gaps[0]["reason"]
        assert len(gaps[0]["paths"]) == 2
    elif expected is None:
        assert module not in namespaces and gaps == []
    elif expected == "namespace":
        assert namespaces[module] is not None and dict(namespaces[module].values) == {}
    elif expected == "unsupported":
        assert namespaces[module] is None
        assert [g["kind"] for g in gaps] == ["python_module"]
        assert "not Python source" in gaps[0]["reason"]
    else:
        assert namespaces[module] is not None
        assert [path for path in workspace.read_calls] == [f"{GIT}/{expected}"]


@pytest.mark.parametrize("files, failing", [
    ({f"{GIT}/settings.py": 'CSV = "data/present.csv"\n'}, f"{GIT}/jobs"),
    ({f"{GIT}/settings/__init__.py": 'CSV = "data/present.csv"\n'}, f"{GIT}/settings"),
])
def test_lookup_through_a_folder_that_cannot_be_listed_is_unresolved(files, failing):
    notebook = _nb("from settings import CSV", "pd.read_csv(CSV)")
    namespaces, gaps = _resolver(FakeWorkspace(files, failing_lists={failing})).for_notebook(
        notebook, JOB_NOTEBOOK)
    result = extract_from_notebook(
        notebook, import_namespaces=namespaces,
        notebook_dir=repository_notebook_dir(JOB_NOTEBOOK))
    assert namespaces["settings"] is None
    assert [(g["kind"], g["directory"]) for g in gaps] == [("python_module_search", failing)]
    assert result["read_references"] == []
    assert [u["argument_expression"] for u in _unresolved_reads(result)] == ["CSV"]


def test_compiled_module_read_is_unresolved_and_reported():
    reads, gaps = _notebook_reads(
        _layout_files({"settings.py": 'CSV = "data/present.csv"\n', "EXT": ""}),
        "from settings import CSV", "pd.read_csv(CSV)")
    assert reads == [] and gaps == [(2, "CSV")]


# ─── Orchestration: extract_data_refs with the real HTTP seams ───────────

def _http_404():
    response = requests.Response()
    response.status_code = 404
    return requests.HTTPError("404 Not Found", response=response)


def _wire(monkeypatch, notebooks, files):
    workspace = FakeWorkspace(files)
    monkeypatch.setattr(data_checker, "get_oci_signer", lambda _profile: ({}, object()))
    monkeypatch.setattr(
        data_checker, "_download_notebook_json",
        lambda _signer, _base, _lake, _ws, path: notebooks[path],
    )
    monkeypatch.setattr(
        data_checker, "_list_workspace_entries",
        lambda _signer, _base, _lake, _ws, directory: workspace.list_entries(directory),
    )

    def download(_signer, _base, _lake, _ws, path):
        if path not in workspace.files:
            raise _http_404()
        return workspace.read_file(path)

    monkeypatch.setattr(data_checker, "_download_workspace_file", download)
    return workspace


def _extract(notebook_paths):
    return asyncio.run(data_checker.extract_data_refs(
        None, notebook_paths, aidp_base="https://example.invalid",
        lake_ocid="ocid1.aidataplatform.oc1.iad.example", workspace_id="workspace",
    ))


def test_incident_csv_read_reaches_the_workspace_probe(monkeypatch):
    _wire(monkeypatch, {NOTEBOOK: INCIDENT_NOTEBOOK}, INCIDENT_FILES)
    refs = _extract([NOTEBOOK])

    expected = f"{GIT}/database/access_control.csv"
    assert expected in refs[NOTEBOOK]["paths"]
    ref = next(r for r in refs[NOTEBOOK]["read_references"] if r["object_name"] == expected)
    assert ref["relative_path"] == "database/access_control.csv"
    assert not [u for u in refs[NOTEBOOK]["unresolved_scopes"] if u.get("object_name") is None]

    classified = asyncio.run(data_checker.classify_refs_on_cluster(None, [NOTEBOOK], refs))
    assert any(entry.get("path") == expected and entry.get("usage") == "read"
               for entry in classified[NOTEBOOK]["paths"])


def test_workspace_notebook_relative_read_stays_unresolved(monkeypatch):
    notebook = f"{EXPORT}/notebooks/Users/a@b.com/job/nb.ipynb"
    files = {f"{EXPORT}/notebooks/Users/a@b.com/job/settings.py": 'CSV = "data/x.csv"\n'}
    _wire(monkeypatch, {notebook: _nb("from settings import CSV\n", "df = pd.read_csv(CSV)\n")},
          files)
    refs = _extract([notebook])

    assert not refs[notebook]["read_references"]
    [gap] = [u for u in refs[notebook]["unresolved_scopes"] if u.get("object_name") is None]
    assert gap["argument_expression"] == "CSV"
    assert "working directory" in gap["reason"]


def test_reviewer_import_order_repro_through_extract_data_refs(monkeypatch):
    notebook = _nb(
        "from settings import CSV", "pd.read_csv(CSV)",
        "from other import CSV", "pd.read_csv(CSV)",
    )
    _wire(monkeypatch, {NOTEBOOK: notebook}, ORDER_FILES)
    refs = _extract([NOTEBOOK])
    assert sorted(
        (r["cell_number"], r["object_name"]) for r in refs[NOTEBOOK]["read_references"]
    ) == [(2, f"{GIT}/data/missing.csv"), (4, f"{GIT}/data/present.csv")]


def test_reviewer_function_local_repro_through_extract_data_refs(monkeypatch):
    files = {f"{GIT}/settings.py": (
        'CSV = "data/missing.csv"\n'
        "def helper():\n"
        '    CSV = "data/present.csv"\n'
    )}
    _wire(monkeypatch, {NOTEBOOK: _nb("from settings import CSV", "pd.read_csv(CSV)")}, files)
    refs = _extract([NOTEBOOK])
    assert [r["object_name"] for r in refs[NOTEBOOK]["read_references"]] == [
        f"{GIT}/data/missing.csv"]


def test_reviewer_stale_handler_repro_through_extract_data_refs(monkeypatch):
    notebook = _nb(
        'CSV = "/Workspace/data/present.csv"\n'
        "try:\n"
        '    CSV = "/Workspace/data/missing.csv"\n'
        "    raise RuntimeError()\n"
        "except RuntimeError:\n"
        "    pd.read_csv(CSV)\n"
    )
    _wire(monkeypatch, {NOTEBOOK: notebook}, {})
    refs = _extract([NOTEBOOK])
    assert "/Workspace/data/present.csv" not in refs[NOTEBOOK]["paths"]
    [gap] = [u for u in refs[NOTEBOOK]["unresolved_scopes"] if u.get("object_name") is None]
    assert gap["argument_expression"] == "CSV"


def test_reviewer_parameter_shadow_repro_through_extract_data_refs(monkeypatch):
    files = {f"{GIT}/settings.py": 'CSV = "data/present.csv"\n'}
    notebook = _nb(
        "from settings import CSV",
        "def load(CSV):\n    frame = pd.read_csv(CSV)\n",
        'load("data/missing.csv")',
    )
    _wire(monkeypatch, {NOTEBOOK: notebook}, files)
    refs = _extract([NOTEBOOK])
    assert not refs[NOTEBOOK]["read_references"]
    [gap] = [u for u in refs[NOTEBOOK]["unresolved_scopes"] if u.get("object_name") is None]
    assert gap["argument_expression"] == "CSV" and gap["cell_number"] == 2


def test_reviewer_package_over_module_repro_through_extract_data_refs(monkeypatch):
    files = {
        f"{GIT}/config/__init__.py": "",
        f"{GIT}/config/settings.py": 'CSV = "data/present.csv"\n',
        f"{GIT}/config/settings/__init__.py": 'CSV = "data/missing.csv"\n',
    }
    _wire(monkeypatch, {NOTEBOOK: _nb("from config.settings import CSV", "pd.read_csv(CSV)")}, files)
    refs = _extract([NOTEBOOK])
    assert [r["object_name"] for r in refs[NOTEBOOK]["read_references"]] == [
        f"{GIT}/data/missing.csv"]


def test_reviewer_class_attribute_repro_through_extract_data_refs(monkeypatch):
    notebook = _nb(
        "CSV = '/Workspace/export/data/missing.csv'\n"
        "class Reader:\n"
        "    CSV = '/Workspace/export/data/present.csv'\n"
        "    def read(self):\n"
        "        frame = pd.read_csv(CSV)\n"
        "Reader().read()\n"
    )
    _wire(monkeypatch, {NOTEBOOK: notebook}, {})
    refs = _extract([NOTEBOOK])
    assert refs[NOTEBOOK]["paths"] == ["/Workspace/export/data/missing.csv"]


def test_reviewer_module_in_two_folders_repro_through_extract_data_refs(monkeypatch):
    files = {
        f"{GIT}/settings.py": 'CSV = "data/present.csv"\n',
        f"{JOB}/settings.py": 'CSV = "data/missing.csv"\n',
    }
    _wire(monkeypatch, {JOB_NOTEBOOK: _nb("from settings import CSV", "pd.read_csv(CSV)")},
          files)
    refs = _extract([JOB_NOTEBOOK])

    assert not refs[JOB_NOTEBOOK]["read_references"]
    [gap] = [u for u in refs[JOB_NOTEBOOK]["unresolved_scopes"] if u.get("object_name") is None]
    assert gap["argument_expression"] == "CSV"
    [module_gap] = [g for g in refs[JOB_NOTEBOOK]["not_analyzed"]
                    if g.get("kind") == "python_module"]
    assert module_gap["module"] == "settings"
    assert sorted(module_gap["paths"]) == sorted(files)
    assert any(u.get("module") == "settings" for u in refs[JOB_NOTEBOOK]["unresolved"])


def test_module_download_error_surfaces_on_the_notebook(monkeypatch):
    workspace = _wire(monkeypatch, {NOTEBOOK: INCIDENT_NOTEBOOK}, INCIDENT_FILES)
    workspace.failing_reads.add(f"{GIT}/imports/constants.py")
    refs = _extract([NOTEBOOK])

    gaps = refs[NOTEBOOK]["not_analyzed"]
    assert any(g.get("kind") == "python_module" and g.get("module") == "imports.constants"
               for g in gaps)
    # Gaps join the counted ``unresolved`` list with their own kind, like the
    # existing notebook_run gaps.
    assert any(u.get("kind") == "python_module" and u.get("module") == "imports.constants"
               for u in refs[NOTEBOOK]["unresolved"])
    [gap] = [u for u in refs[NOTEBOOK]["unresolved_scopes"] if u.get("object_name") is None]
    assert gap["argument_expression"] == "relative_file_path"
