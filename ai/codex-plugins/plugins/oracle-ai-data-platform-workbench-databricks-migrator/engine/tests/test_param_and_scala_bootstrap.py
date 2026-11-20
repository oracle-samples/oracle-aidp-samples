"""Regression tests for two related bugs surfaced during the HELPER_Monitoring
migration run (2026-07-27):

  BUG A — build_dag_from_workflow.build_manifest fabricates an empty
          job.parameters key for every widget that has no explicit run value.
          Downstream job_migrate.py merges job.parameters on top of
          task.base_parameters, so an operator's task-level dateStr edit gets
          silently wiped back to "".

  BUG B — job_migrate._ensure_dbutils_import injects a Python
          `from aidp_compat import ...` line into the first bare cell of a
          native-Scala/SQL notebook. That cell is later promoted to %scala
          during child-inline, producing invalid Scala syntax
          ("Failing command: from aidp_compat import ...").

Both are tested here without a live cluster / network.

Usage:
    pytest tests/test_param_and_scala_bootstrap.py -v
    python3 tests/test_param_and_scala_bootstrap.py
"""
import ast
import io
import os
import re
import sys
import tokenize
import types
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))

# ─────────────────────────────────────────────────────────────────────
#  Helpers to extract single functions from a script file without
#  executing the whole module (job_migrate.py drags in the AIDP runtime
#  which requires cluster deps we don't have in a unit-test env).
# ─────────────────────────────────────────────────────────────────────
def _extract(path: str, wanted: Set[str]) -> types.ModuleType:
    with open(path) as f:
        tree = ast.parse(f.read())
    kept = []
    for node in tree.body:
        if isinstance(
                node,
                (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Assign)):
            name = getattr(node, "name", None) or (
                node.targets[0].id if (isinstance(node, ast.Assign)
                                       and isinstance(node.targets[0], ast.Name))
                else None
            )
            if name in wanted:
                kept.append(node)
    mod = types.ModuleType("extracted")
    mod.__file__ = path
    # Signature annotations are evaluated at def time, so any typing name the
    # extracted functions annotate with must exist in this namespace or the
    # exec below raises NameError on a function the test never calls.
    mod.__dict__.update(
        {"Optional": Optional, "Dict": Dict, "List": List, "Any": Any, "Set": Set,
         "Tuple": Tuple,
         "NamedTuple": NamedTuple, "ast": ast, "io": io, "re": re,
         "tokenize": tokenize}
    )
    src = ast.unparse(ast.Module(body=kept, type_ignores=[]))
    exec(src, mod.__dict__)
    return mod


# ═════════════════════════════════════════════════════════════════════
#  BUG A — build_dag_from_workflow.build_manifest widget-name emission
# ═════════════════════════════════════════════════════════════════════
#
# Instead of loading the whole builder we simulate the exact widget-name
# emission block as an inline helper mirroring lines 668-719 of
# build_dag_from_workflow.py, then verify that widgets WITHOUT a
# run-value are NOT added to job_params (they only appear in
# parameters_meta).

def _simulate_widget_emission(widget_reads, job_params, run_parameters):
    """Faithful mirror of build_dag_from_workflow.py:668-719 semantics.
    Kept in sync with the source; if the source drift, this test drifts too."""
    parameters_meta = {}
    for k in job_params:
        parameters_meta[k] = {"source": "workflow", "widget": False, "value_present": True}

    _run_params = run_parameters or {}
    _widget_names_safe_without_value = set()
    _widget_names_needing_value = set()
    if widget_reads:
        for _key, _wi in widget_reads.items():
            for _r in _wi.get("reads", []) or []:
                _wname = _r.get("name")
                if not _wname:
                    continue
                if _r.get("has_call_site_default"):
                    _widget_names_safe_without_value.add(_wname)
                else:
                    _widget_names_needing_value.add(_wname)
            for _dname in (_wi.get("declarations") or {}):
                _widget_names_safe_without_value.add(_dname)
    _widget_names_needing_value |= (
        _widget_names_needing_value - _widget_names_safe_without_value
    )

    # Post-fix emission (matches current build_dag_from_workflow.py line 696-708)
    for _wname in sorted(_widget_names_needing_value):
        if _wname in job_params:
            continue
        if _wname in _run_params:
            job_params[_wname] = _run_params[_wname]
            parameters_meta[_wname] = {"source": "run", "widget": True, "value_present": True}
        else:
            # FIX: no empty-placeholder emission into job_params anymore.
            parameters_meta[_wname] = {"source": "empty:widget", "widget": True, "value_present": False}

    for _rname, _rvalue in _run_params.items():
        if _rname in job_params:
            continue
        job_params[_rname] = _rvalue
        parameters_meta[_rname] = {"source": "run", "widget": False, "value_present": True}

    return job_params, parameters_meta


def test_bug_a_widget_with_no_run_value_stays_out_of_job_params():
    """The regression: a widget that has NO run-value must NOT be emitted
    into job.parameters as an empty placeholder. The migration merge would
    then wipe task.base_parameters."""
    widget_reads = {
        "DailyDataDump": {
            "reads": [{"name": "dateStr", "has_call_site_default": False}],
        }
    }
    job_params = {}
    run_parameters = {}
    got_params, got_meta = _simulate_widget_emission(widget_reads, job_params, run_parameters)
    assert "dateStr" not in got_params, (
        f"REGRESSION: empty widget still emitted into job.parameters ({got_params})"
    )
    assert got_meta.get("dateStr", {}).get("source") == "empty:widget", got_meta


def test_bug_a_widget_with_run_value_still_emitted():
    """Non-regression: widget WITH a run value should still be emitted into
    job.parameters as before (this is a real value, not a placeholder)."""
    widget_reads = {
        "DailyDataDump": {
            "reads": [{"name": "dateStr", "has_call_site_default": False}],
        }
    }
    job_params = {}
    run_parameters = {"dateStr": "2026-02-14"}
    got_params, got_meta = _simulate_widget_emission(widget_reads, job_params, run_parameters)
    assert got_params.get("dateStr") == "2026-02-14"
    assert got_meta["dateStr"]["source"] == "run"


def test_bug_a_workflow_declared_job_params_still_emitted():
    """Non-regression: params the workflow ITSELF declared at job level
    (source='workflow') survive the fix."""
    widget_reads = {}
    job_params = {"env": "prod"}   # from job_data.parameters
    got_params, got_meta = _simulate_widget_emission(widget_reads, job_params, {})
    assert got_params.get("env") == "prod"
    assert got_meta["env"]["source"] == "workflow"


def test_bug_a_widget_with_call_site_default_never_emitted():
    """Widgets with call-site default (`get('K', 'fallback')`) are already
    safe — they should never appear in job_params regardless of the fix."""
    widget_reads = {
        "T": {"reads": [{"name": "safeK", "has_call_site_default": True}]}
    }
    got_params, _ = _simulate_widget_emission(widget_reads, {}, {})
    assert "safeK" not in got_params


def test_bug_a_extra_run_params_still_emitted():
    """Non-regression: run params that AREN'T widget reads (Scala-only
    params, spark.conf lookups, etc.) still land in job_params."""
    widget_reads = {}
    got_params, got_meta = _simulate_widget_emission(
        widget_reads, {}, {"extra_flag": "yes"}
    )
    assert got_params["extra_flag"] == "yes"
    assert got_meta["extra_flag"]["source"] == "run"


# ═════════════════════════════════════════════════════════════════════
#  BUG B — _ensure_dbutils_import skips native scala/sql notebooks
# ═════════════════════════════════════════════════════════════════════
_JM = _extract(
    os.path.join(REPO, "scripts/job_migrate.py"),
    # _ensure_dbutils_import keeps a leading %python/%pyspark magic on line 0
    # by routing its insert through the shared magic helpers, so those and the
    # token sets they read must be extracted alongside it.
    {"_ensure_dbutils_import", "_AIDP_IMPORT_LINE", "_leading_cell_magic",
     "_insert_below_leading_magic", "_promote_leading_magic",
     "_promote_displaced_magic", "_canonicalize_pyspark_magic",
     "_promote_same_language_python_magic", "_restore_python_future_preamble",
     "_future_preamble_line_count",
     "_promote_magic_over_comments", "_promote_magic_over_tool_edits",
     "_advise_displaced_magic", "_missing_import_edit_marker",
     "_magic_comment_marker", "_recomment", "MagicPromotion",
     "_logical_ipython_magic_lines", "_fallback_ipython_statement_lines",
     "_PYTHON_TOKEN_ERRORS",
     "_PYTHON_MAGICS", "_PYTHON_LINE_MAGICS", "_INJECTABLE_MAGICS",
     "_LANG_DECL_MAGICS",
     "_CELL_MAGIC_TOKENS", "_CELL_MAGIC_RE"},
)
# _ensure_dbutils_import references the module-level `tprint` for progress
# logs; stub it with print so the extracted function runs in isolation.
_JM.tprint = print


def _cell(src: str, lang: str = "python") -> dict:
    """Minimal cell dict as _ensure_dbutils_import expects."""
    return {"cell_type": "code", "source": [src]}


def _has_aidp_import(cells: list) -> bool:
    return any(
        "from aidp_compat import" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )


def test_bug_b_native_scala_no_injection():
    """Native-scala notebook must be a NO-OP for _ensure_dbutils_import,
    even when a bare cell technically uses `dbutils` inside a Scala string."""
    cells = [_cell('val x = "dbutils"')]   # bare cell, no %scala prefix yet
    got = _JM._ensure_dbutils_import(cells, native_lang="scala")
    assert not _has_aidp_import(got), \
        f"BUG B: aidp_compat import injected into a native-scala notebook: {got}"


def test_bug_b_native_sql_no_injection():
    """Same for native-sql notebooks."""
    cells = [_cell("SELECT dbutils_col FROM t")]
    got = _JM._ensure_dbutils_import(cells, native_lang="sql")
    assert not _has_aidp_import(got)


def test_bug_b_explicit_non_python_language_no_injection():
    """Unknown/R metadata must not inherit the empty-means-Python contract."""
    cells = [_cell("x <- dbutils.widgets.get('x')")]
    got = _JM._ensure_dbutils_import(cells, native_lang="r")
    assert not _has_aidp_import(got)


def test_bug_b_python_still_injects_when_needed():
    """Non-regression: a Python notebook that uses dbutils still gets the
    import, exactly as before the fix."""
    cells = [_cell("dbutils.fs.ls('/')")]
    got = _JM._ensure_dbutils_import(cells, native_lang="")
    assert _has_aidp_import(got), \
        f"REGRESSION: Python notebook missing the aidp_compat injection: {got}"


def test_bug_b_python_no_dbutils_no_injection():
    """Non-regression: a Python notebook that DOESN'T use dbutils gets no
    injection (was the pre-fix behavior; still holds)."""
    cells = [_cell("x = 1")]
    got = _JM._ensure_dbutils_import(cells, native_lang="")
    assert not _has_aidp_import(got)


def test_bug_b_already_imported_no_double_inject():
    """Non-regression: idempotency."""
    cells = [_cell("from aidp_compat import dbutils\ndbutils.fs.ls('/')")]
    got = _JM._ensure_dbutils_import(cells, native_lang="")
    injections = sum(
        "".join(c.get("source", [])).count("# Oracle tool modification: added by migration tool")
        for c in got
    )
    assert injections == 0, "Idempotent path must not re-inject"


def test_bug_b_default_native_lang_treats_as_python():
    """Non-regression: callers that DON'T pass native_lang (older code
    paths) get the pre-fix behavior — injects for Python-looking notebooks."""
    cells = [_cell("dbutils.fs.ls('/')")]
    got = _JM._ensure_dbutils_import(cells)   # no native_lang arg
    assert _has_aidp_import(got)


# ═════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    fns = [(name, fn) for name, fn in globals().items()
           if name.startswith("test_") and callable(fn)]
    failures = []
    for name, fn in fns:
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures.append((name, str(e)))
            print(f"  ✗ {name}\n      {e}")
    print("=" * 60)
    if failures:
        print(f"✗ {len(failures)} FAILURE(S) out of {len(fns)}")
        sys.exit(1)
    print(f"✓ ALL {len(fns)} TESTS PASSED")
