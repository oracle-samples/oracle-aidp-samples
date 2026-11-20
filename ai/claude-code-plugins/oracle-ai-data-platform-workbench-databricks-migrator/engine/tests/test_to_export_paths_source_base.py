"""Tests for _to_export_paths + source_prefix fix in build_dag_from_workflow.py.

Covers:
  - Regression: existing behavior when source == target (Model_Mon-style)
  - Regression: raw Databricks paths (/Workspace/Users/... outside any base)
  - Regression: 2-arg callers (source_prefix defaulted to None)
  - Positive: source ≠ target case (Example_Txn_Budget_Monitoring bug)
  - Positive: auto-inferred source_prefix from task list
  - Negative: paths outside both bases fall back to /Workspace/ strip
  - Edge: task paths without common /notebooks/ prefix → auto-infer bails

Extracts _to_export_paths + _infer_source_prefix from source via AST to avoid the
heavy full-module import (which pulls oci SDK + aidp_executor + asyncio deps).
"""

from __future__ import annotations

import ast
import pathlib
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "build_dag_from_workflow.py"


def _load_helpers():
    """Load _to_export_paths + the source-base auto-inferrer + _sanitize_path
    into a fresh namespace, without importing the full module (which requires
    oci / aidp_executor)."""
    # Stub _sanitize_path (imported from dbx_export.sanitizer). We use a
    # pass-through stub so canonical == canonical_sanitized — sanitizer
    # behavior is tested separately.
    def _sanitize_path_stub(p):
        return p

    wanted_funcs = {"_to_export_paths", "_infer_source_prefix"}
    picked = []
    for source in (SCRIPT, REPO / "scripts" / "build_dag.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in wanted_funcs:
                picked.append(node)

    if not picked:
        raise AssertionError("no target functions found in build_dag_from_workflow.py")

    module = ast.Module(body=picked, type_ignores=[])
    ns = {
        "_sanitize_path": _sanitize_path_stub,
        "os": __import__("os"),
        "posixpath": __import__("posixpath"),
        "re": __import__("re"),
    }
    exec(compile(module, str(SCRIPT), "exec"), ns)
    return ns


NS = _load_helpers()
_to_export_paths = NS["_to_export_paths"]
_infer_source_prefix = NS.get("_infer_source_prefix")  # may not exist until we add it


# ═════ Regression: existing 2-arg behavior ═════

class TestBackwardCompat(unittest.TestCase):
    """When called with 2 args (source_prefix defaulted), behavior must be
    IDENTICAL to the old version. This is the Model_Mon path and any pre-fix
    call site that hasn't been updated to pass source_prefix."""

    def test_raw_workspace_path_maps_under_base(self):
        # /Workspace/Users/foo.ipynb — customer raw Databricks path, no base
        original, migrated = _to_export_paths(
            "/Workspace/Users/foo@bar.com/Notebook.ipynb",
            "/Workspace/scratch/user/exp/notebooks",
        )
        self.assertEqual(original, "/Workspace/Users/foo@bar.com/Notebook.ipynb")
        self.assertEqual(
            migrated,
            "/Workspace/scratch/user/exp/notebooks/Users/foo@bar.com/Notebook.ipynb",
        )

    def test_path_already_under_base_stays_stable(self):
        # Idempotency: a path already under base_prefix maps to itself
        base = "/Workspace/scratch/user/exp/notebooks"
        p = base + "/Users/foo@bar.com/Notebook.ipynb"
        original, migrated = _to_export_paths(p, base)
        # migrated is under the base (identity)
        self.assertEqual(migrated, p)

    def test_no_base_prefix_keeps_path(self):
        # base_prefix empty — return raw canonical for both
        original, migrated = _to_export_paths(
            "/Workspace/Users/foo@bar.com/Notebook.ipynb", ""
        )
        self.assertEqual(original, migrated)
        self.assertEqual(original, "/Workspace/Users/foo@bar.com/Notebook.ipynb")


# ═════ POSITIVE: The ExampleVendor bug (source ≠ target) ═════

class TestSourceDifferentFromTarget(unittest.TestCase):
    """The failing case: source lives at a nested /Workspace/x/y/notebooks/,
    target lives at /Workspace/x/z/notebooks/. %run deps resolve to source
    paths — must be mapped under target WITHOUT doubling."""

    def test_examplevendor_utils_imports_not_doubled(self):
        # The exact failing path from the 2026-07-28 ExampleVendor report.
        # With source_prefix set, canonical is stripped correctly, and
        # `migrated` re-roots under base_prefix (target) — NOT doubled.
        SOURCE = "/Workspace/examplecorp_tool_demo/dbx_export/notebooks"
        TARGET = "/Workspace/examplecorp_tool_demo/examplecorp_migrate/notebooks"
        run_target = (
            "/Workspace/examplecorp_tool_demo/dbx_export/notebooks/GIT/Hourly_Monitoring"
            "/utils/imports.ipynb"
        )
        original, migrated = _to_export_paths(run_target, TARGET, SOURCE)
        # Original = raw Databricks form
        self.assertEqual(
            original,
            "/Workspace/GIT/Hourly_Monitoring/utils/imports.ipynb",
        )
        # Migrated = under TARGET, clean (NOT doubled)
        expected = TARGET + "/GIT/Hourly_Monitoring/utils/imports.ipynb"
        self.assertEqual(migrated, expected)
        # Sanity: the doubled shape (target base then source base inside) must not appear
        self.assertNotIn("examplecorp_migrate/notebooks/examplecorp_tool_demo", migrated)

    def test_examplevendor_savetableutils_not_doubled(self):
        SOURCE = "/Workspace/examplecorp_tool_demo/dbx_export/notebooks"
        TARGET = "/Workspace/examplecorp_tool_demo/examplecorp_migrate/notebooks"
        run_target = (
            "/Workspace/examplecorp_tool_demo/dbx_export/notebooks/Users/"
            "user2@example.com/SaveTableUtils.ipynb"
        )
        _, migrated = _to_export_paths(run_target, TARGET, SOURCE)
        self.assertEqual(
            migrated,
            TARGET + "/Users/user2@example.com/SaveTableUtils.ipynb",
        )

    def test_source_and_target_can_be_deeply_nested(self):
        SOURCE = "/Workspace/level1/level2/dbx_export/notebooks"
        TARGET = "/Workspace/level1/level2/target_out/notebooks"
        run_target = SOURCE + "/some/user/nb.ipynb"
        _, migrated = _to_export_paths(run_target, TARGET, SOURCE)
        # Path under source stripped; re-rooted under target
        self.assertEqual(migrated, TARGET + "/some/user/nb.ipynb")

    def test_raw_databricks_path_when_source_different_from_target(self):
        # Raw /Workspace/Users/... path outside both source and target — falls
        # through to the /Workspace/ strip branch, then re-rooted under target.
        SOURCE = "/Workspace/examplecorp_tool_demo/dbx_export/notebooks"
        TARGET = "/Workspace/examplecorp_tool_demo/examplecorp_migrate/notebooks"
        run_target = "/Workspace/Users/foo@bar.com/UtilLib.ipynb"
        _, migrated = _to_export_paths(run_target, TARGET, SOURCE)
        self.assertEqual(
            migrated,
            TARGET + "/Users/foo@bar.com/UtilLib.ipynb",
        )


# ═════ Regression: source == target (Model_Mon-style auto-derive) ═════

class TestSourceEqualsTarget(unittest.TestCase):
    """When source_prefix == base_prefix (Model_Mon happy path), the
    existing base_prefix branch should catch the path FIRST — behavior
    identical to old 2-arg call."""

    def test_model_mon_style_source_equals_target(self):
        # Auto-derived — user didn't pass --export-output
        SOURCE = "/Workspace/scratch/user/ai_migration_phase/dbx_export_2/notebooks"
        TARGET = SOURCE  # same
        run_target = SOURCE + "/Users/user4@example.com/Model_Monitoring/Utils.ipynb"
        original, migrated = _to_export_paths(run_target, TARGET, SOURCE)
        # migrated == source (stays put, matches Model_Mon manifest exactly)
        self.assertEqual(migrated, run_target)


# ═════ Positive: raw Databricks path with source_prefix set ═════

class TestRawPathsWithSourcePrefix(unittest.TestCase):
    """Paths OUTSIDE both source and target — fall back to /Workspace/ strip."""

    def test_raw_workspace_path_with_source_prefix_still_maps(self):
        SOURCE = "/Workspace/examplecorp_tool_demo/dbx_export/notebooks"
        TARGET = "/Workspace/examplecorp_tool_demo/examplecorp_migrate/notebooks"
        # Raw Databricks /Workspace/Users/... — NOT under source or target
        run_target = "/Workspace/Users/user0@example.com/UtilLib.ipynb"
        original, migrated = _to_export_paths(run_target, TARGET, SOURCE)
        # Falls through to /Workspace/ strip; migrated goes under TARGET
        self.assertEqual(original, run_target)
        self.assertEqual(
            migrated,
            TARGET + "/Users/user0@example.com/UtilLib.ipynb",
        )


# ═════ Idempotency across all shapes ═════

class TestIdempotency(unittest.TestCase):

    def test_path_already_under_target_stays_stable(self):
        SOURCE = "/Workspace/examplecorp_tool_demo/dbx_export/notebooks"
        TARGET = "/Workspace/examplecorp_tool_demo/examplecorp_migrate/notebooks"
        # Already at target location (idempotency)
        p = TARGET + "/GIT/foo.ipynb"
        _, migrated = _to_export_paths(p, TARGET, SOURCE)
        self.assertEqual(migrated, p)


# ═════ Auto-infer source_prefix from task list ═════

class TestAutoInferSourcePrefix(unittest.TestCase):

    def _skip_if_no_inferrer(self):
        if _infer_source_prefix is None:
            self.skipTest("_infer_source_prefix not yet implemented in module")

    def test_infer_common_prefix_from_examplevendor_tasks(self):
        self._skip_if_no_inferrer()
        tasks = [
            {"notebook_path":
             "/Workspace/examplecorp_tool_demo/dbx_export/notebooks/GIT/Hourly_Monitoring/master.ipynb"},
            {"notebook_path":
             "/Workspace/examplecorp_tool_demo/dbx_export/notebooks/GIT/Hourly_Monitoring/x.ipynb"},
            {"notebook_path":
             "/Workspace/examplecorp_tool_demo/dbx_export/notebooks/GIT/Hourly_Monitoring/y.ipynb"},
        ]
        got = _infer_source_prefix(tasks)
        self.assertEqual(got, "/Workspace/examplecorp_tool_demo/dbx_export/notebooks")

    def test_infer_common_prefix_from_model_mon_tasks(self):
        self._skip_if_no_inferrer()
        tasks = [
            {"notebook_path":
             "/Workspace/scratch/user/ai_migration_phase/dbx_export_2/notebooks/"
             "Users/user7@example.com/foo.ipynb"},
            {"notebook_path":
             "/Workspace/scratch/user/ai_migration_phase/dbx_export_2/notebooks/"
             "Users/user7@example.com/bar.ipynb"},
        ]
        got = _infer_source_prefix(tasks)
        self.assertEqual(
            got,
            "/Workspace/scratch/user/ai_migration_phase/dbx_export_2/notebooks",
        )

    def test_infer_returns_none_when_no_notebooks_dir(self):
        self._skip_if_no_inferrer()
        tasks = [
            {"notebook_path": "/Workspace/foo/direct.ipynb"},
            {"notebook_path": "/Workspace/foo/bar.ipynb"},
        ]
        got = _infer_source_prefix(tasks)
        self.assertIsNone(got)

    def test_infer_returns_none_when_no_common_prefix(self):
        self._skip_if_no_inferrer()
        tasks = [
            {"notebook_path": "/Workspace/foo/notebooks/a.ipynb"},
            {"notebook_path": "/Workspace/bar/notebooks/b.ipynb"},
        ]
        got = _infer_source_prefix(tasks)
        # No common /notebooks/ prefix beyond /Workspace → None
        self.assertIsNone(got)

    def test_infer_empty_task_list_returns_none(self):
        self._skip_if_no_inferrer()
        got = _infer_source_prefix([])
        self.assertIsNone(got)

    def test_infer_uses_posixpath_cross_platform(self):
        """PR-35 reviewer feedback: _infer_source_prefix MUST use posixpath (not
        os.path) because AIDP workspace paths are always POSIX. os.path on
        Windows uses '\\\\' separators and silently returns garbage for
        POSIX inputs — the exact bug class 239f6c9 fixed elsewhere.

        Structural guard: this test asserts the source uses `posixpath`
        (not `os.path`) for the commonpath/dirname calls. If the imports
        get refactored back to os.path, this test breaks — Windows CI
        would then fail the functional tests above."""
        self._skip_if_no_inferrer()
        src_file = REPO / "scripts" / "build_dag_from_workflow.py"
        src = src_file.read_text()
        # Locate _infer_source_prefix definition and confirm posixpath usage
        m = _re_search_infer_body(src)
        self.assertIsNotNone(m, "_infer_source_prefix body not found")
        body = m.group(0)
        self.assertIn(
            "posixpath.commonpath",
            body,
            "_infer_source_prefix must use posixpath.commonpath (not os.path.commonpath) — "
            "AIDP paths are POSIX; os.path is platform-dependent."
        )
        self.assertIn(
            "posixpath.dirname",
            body,
            "_infer_source_prefix must use posixpath.dirname (not os.path.dirname) — "
            "same reason as above."
        )


def _re_search_infer_body(src):
    import re
    return re.search(
        r'def _infer_source_prefix\([^)]*\).*?(?=\ndef |\Z)',
        src, re.S,
    )


if __name__ == "__main__":
    unittest.main()
