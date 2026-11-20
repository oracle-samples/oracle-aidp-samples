"""Tests for the runtime-pin pre-flight in scripts/cluster_lifecycle.py.

The AIDP stock cluster image ships pandas 3.0.x + numpy 2.x; on that combo
pandas backs string columns with ArrowStringArray and read_csv/.apply die with
a cryptic `TypeError: Cannot interpret 'dtype('O')'`. default_requirements.txt
pins the triad (pandas==2.2.3, numpy==1.26.4, pyarrow==12.0.1). Root cause of
the 2026-07-31 failure: that file was never attached to the cluster, so it kept
stock pandas 3.0.5. `_runtime_pin_violation` detects the unsupported versions so
the migration installs the pin (cure) and fails fast if it still didn't take.

Extracted via AST so importing doesn't pull cluster_lifecycle's heavy oci /
requests top-level imports (same pattern as tests/test_dbfs_path_translation.py).

Run: python -m pytest tests/test_verify_pandas_pin.py -v
"""
from __future__ import annotations

import ast
import pathlib
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
SRC = REPO / "scripts" / "cluster_lifecycle.py"


def _load_pin_helper():
    """Return _runtime_pin_violation + the two threshold consts, without importing
    the whole module (which needs oci/requests)."""
    tree = ast.parse(SRC.read_text())
    ns: dict = {}
    # Include consts the message string interpolates (in file order so
    # DEFAULT_REQUIREMENTS_PATH's f-string sees AIDP_COMPAT_DEPS_DIR first).
    wanted_consts = {
        "_PANDAS_MAX_MAJOR_EXCLUSIVE", "_NUMPY_MAX_MAJOR_EXCLUSIVE",
        "AIDP_COMPAT_DEPS_DIR", "DEFAULT_REQUIREMENTS_PATH",
        "TARGET_RUNTIME_REQUIREMENTS_PATH",
    }
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") in wanted_consts for t in node.targets
        ):
            exec(compile(ast.Module([node], []), str(SRC), "exec"), ns)
        if isinstance(node, ast.FunctionDef) and node.name == "_runtime_pin_violation":
            exec(compile(ast.Module([node], []), str(SRC), "exec"), ns)
    return ns["_runtime_pin_violation"]


class TestRuntimePinViolation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fn = staticmethod(_load_pin_helper())

    # ── pandas dimension ──
    def test_pinned_pandas_ok(self):
        self.assertIsNone(self.fn("2.2.3"))

    def test_older_pandas_2x_ok(self):
        self.assertIsNone(self.fn("2.1.4"))   # a real pandas 2.x (not a numpy version)

    def test_pandas_3x_flagged(self):
        # The exact stock-image version that broke the LPP migration.
        msg = self.fn("3.0.5")
        self.assertIsNotNone(msg)
        self.assertIn("compatibility constraints", msg)
        self.assertIn("dtype", msg)                      # names the symptom
        self.assertIn("target_runtime_requirements.txt", msg)

    def test_pandas_future_4x_flagged(self):
        self.assertIsNotNone(self.fn("4.1.0"))

    # ── numpy dimension (verify_runtime_pins is plural — checks numpy too) ──
    def test_pinned_numpy_ok(self):
        self.assertIsNone(self.fn("2.2.3", "1.26.4"))    # the pinned triad

    def test_numpy_2x_flagged_even_with_ok_pandas(self):
        msg = self.fn("2.2.3", "2.4.6")
        self.assertIsNotNone(msg)
        self.assertIn("numpy", msg)

    def test_both_bad_reported(self):
        msg = self.fn("3.0.5", "2.4.6")
        self.assertIn("pandas", msg)
        self.assertIn("numpy", msg)

    def test_numpy_omitted_is_pandas_only(self):
        self.assertIsNone(self.fn("2.2.3"))              # numpy_ver=None -> not checked

    # ── fail-open on unparseable ──
    def test_unparseable_does_not_block(self):
        for bad in ("", "  ", "unknown", None, "abc.def"):
            self.assertIsNone(self.fn(bad), f"should not block on pandas={bad!r}")
        # unparseable numpy alongside a good pandas is also a no-op
        self.assertIsNone(self.fn("2.2.3", "weird"))


if __name__ == "__main__":
    unittest.main()
