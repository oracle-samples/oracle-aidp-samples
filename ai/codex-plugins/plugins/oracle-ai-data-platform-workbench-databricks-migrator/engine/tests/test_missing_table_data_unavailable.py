"""Missing-table → DATA_UNAVAILABLE reclassification (all execution loops).

A genuinely-missing TABLE is a DATA gap (the operator provisions the table and
resumes), NOT a migration FAIL (a bug to debug). This suite pins the
reclassification so a later edit can't silently regress it back to FAIL — or,
worse, turn it into a false-GREEN.

Design contract (see the doc comments at each wiring site in job_migrate.py):

  * SCOPE: missing TABLE only (Spark TABLE_OR_VIEW_NOT_FOUND class). A missing
    storage PATH or an empty-for-date-range read is deliberately NOT reclassified.
  * OUTCOME PARITY at default (--strict-cell-gating off): unchanged — still
    non-PASS, the task reports PARTIAL_DATA_UNAVAILABLE, and the runner still
    exits non-zero (no false-green). ONLY the label changes FAIL→DATA_UNAVAILABLE.
  * STRICT (--strict-cell-gating on): still halts the task + blocks the rest
    (the strict gate counts cells_data_unavailable).
  * ALL loops: Python main loop, %scala, %sql, and inline %run/notebook.run
    child (strict path). Replay is intentionally excluded — it re-runs an
    already-executed cell, so a missing table would have surfaced (and been
    reclassified) on that cell's first execution.

The counter/status/runner logic lives inside a ~6k-line async function that
can't run without a live cluster session, so — matching the sibling
test_missing_table_prompt_and_exit.py — the real pure predicate is unit-tested
across its full positive/negative matrix and each wiring site is pinned by a
source-guard assertion.
"""
import os as _os

from scripts.job_migrate import _output_confirms_missing_table

_SCRIPTS = _os.path.join(_os.path.dirname(__file__), "..", "scripts")


def _read(name):
    with open(_os.path.join(_SCRIPTS, name), encoding="utf-8") as f:
        return f.read()


# ── The predicate: missing-TABLE signatures, and ONLY those ──────────────────

def test_spark_structured_error_class_matches():
    assert _output_confirms_missing_table(
        "AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view "
        "`data_science`.`ds_base` cannot be found.")


def test_analysis_message_form_matches():
    assert _output_confirms_missing_table(
        "org.apache.spark.sql.AnalysisException: Table or view not found: db.t")


def test_catalog_nosuchtable_matches():
    assert _output_confirms_missing_table(
        "NoSuchTableException: Table db.t does not exist in catalog")


def test_missing_path_is_NOT_a_missing_table():
    # A missing storage PATH is a different class — must NOT be reclassified here.
    assert not _output_confirms_missing_table(
        "AnalysisException: Path does not exist: oci://bucket@ns/prefix/x")


def test_empty_data_is_NOT_a_missing_table():
    # Empty-for-date-range / empty-result exceptions are handled elsewhere
    # (and are often legitimate) — not reclassified as a missing table.
    assert not _output_confirms_missing_table("IndexError: list index out of range")
    assert not _output_confirms_missing_table(
        "IllegalArgumentException: requirement failed: Required input is empty")


def test_plain_error_is_NOT_a_missing_table():
    assert not _output_confirms_missing_table("NameError: name 'foo' is not defined")
    assert not _output_confirms_missing_table("TypeError: unsupported operand")


def test_bare_does_not_exist_does_not_match():
    # "does not exist" alone also fires on missing paths/files, so the TABLE
    # predicate must NOT match it without a table-specific signature.
    assert not _output_confirms_missing_table("File does not exist: /tmp/x")


def test_empty_and_none_are_false():
    assert not _output_confirms_missing_table("")
    assert not _output_confirms_missing_table(None)


# ── Wiring: the main Python fix-loop early-exit reclassifies to DATA_UNAVAIL ──

def test_main_loop_missing_data_note_increments_data_unavailable_not_failed():
    src = _read("job_migrate.py")
    # The early-exit block (guarded by _note_confirms_missing_data) must route to
    # cells_data_unavailable + a data_unavailable status, NOT cells_failed/error.
    anchor = "data/resource confirmed unavailable"
    assert anchor in src
    block = src.split(anchor, 1)[1][:800]
    assert "cells_data_unavailable += 1" in block
    assert '"status": "data_unavailable"' in block
    assert "cells_failed += 1" not in block, (
        "missing-table early-exit must not increment cells_failed anymore")


# ── P2: the broad note-predicate must NOT be labelled "missing table" ────────

def test_note_predicate_is_broad_beyond_tables():
    # The predicate intentionally matches the whole won't-resolve-on-retry
    # resource-gap class — including non-table forms (module/file). That breadth
    # is exactly why the main-loop label must stay generic (see next test).
    from scripts.job_migrate import _note_confirms_missing_data
    assert _note_confirms_missing_data(["MISSING module: no module named foo"])
    assert _note_confirms_missing_data(["config key does not exist"])


def test_main_loop_note_path_uses_generic_label_not_missing_table():
    src = _read("job_migrate.py")
    anchor = "data/resource confirmed unavailable"
    block = src.split(anchor, 1)[1][:900]
    # echoes the actual note + a generic fallback — never hard-claims "table"
    assert "DATA_UNAVAILABLE: {_mn" in block
    assert "DATA_UNAVAILABLE: data/resource not available" in block
    assert "missing table" not in block, (
        "the broad note-predicate path must not overclaim 'missing table' — "
        "the note may be a missing module/file/path")


# ── Wiring: strict gate stops on data-unavailable (else relabel breaks strict) ─

def test_strict_gate_includes_data_unavailable():
    src = _read("job_migrate.py")
    assert "or cells_data_unavailable > 0):" in src, (
        "strict cell gate must also stop on cells_data_unavailable, else "
        "relabelling FAIL→DATA_UNAVAILABLE would silently disable strict-stop")


# ── Wiring: no false-green — task result + job aggregate + runner exit ────────

def test_task_result_carries_data_unavailable():
    src = _read("job_migrate.py")
    assert '"data_unavailable": cells_data_unavailable,' in src


def test_job_aggregate_sums_data_unavailable():
    src = _read("job_migrate.py")
    assert '"total_data_unavailable": sum(r.get("data_unavailable", 0)' in src


def test_runner_exits_nonzero_on_data_unavailable():
    src = _read("job_migrate_from_workflow.py")
    assert "migration_failed = bool(fail or errored or data_unavail)" in src, (
        "a genuinely-missing table must still exit non-zero (no false-green)")
    assert 'result.get("total_data_unavailable", 0)' in src


# ── Wiring: overall status — a data-unavailable-only task is not FAIL ─────────

def test_fail_fallthrough_excludes_data_unavailable():
    src = _read("job_migrate.py")
    assert ("cells_ok == 0 and cells_data_substituted == 0 "
            "and cells_data_unavailable == 0") in src, (
        "a task whose only non-OK cell is a missing table must report "
        "PARTIAL_DATA_UNAVAILABLE, not fall through to FAIL")


# ── Wiring: %scala and %sql terminal failures reclassify ─────────────────────

def test_scala_terminal_missing_table_is_data_unavailable():
    src = _read("job_migrate.py")
    assert 'DATA_UNAVAILABLE: missing table (%scala)' in src
    # keyed off the output helper on the terminal attempt
    assert "_output_confirms_missing_table(out)" in src


def test_sql_terminal_missing_table_is_data_unavailable():
    src = _read("job_migrate.py")
    assert 'DATA_UNAVAILABLE: missing table (%sql)' in src


# ── Wiring: inline %run/notebook.run child (strict path) tag + parent route ──

def test_child_strict_return_tags_missing_table():
    src = _read("job_migrate.py")
    assert 'return _child_result(' in src
    assert 'kind="DATA_UNAVAILABLE" if _child_missing_table else None,' in src


def test_parent_routes_child_data_unavailable():
    src = _read("job_migrate.py")
    assert 'elif child_result.get("kind") == "DATA_UNAVAILABLE":' in src
    # ...to cells_data_unavailable, not cells_failed
    anchor = 'elif child_result.get("kind") == "DATA_UNAVAILABLE":'
    block = src.split(anchor, 1)[1][:900]
    assert "cells_data_unavailable += 1" in block


# ── P1: strict runs must RETAIN the classification (no fabricated failure) ────
# Under strict, a pure data-unavailable stop (missing table, no real cell
# failure, complete cell set) previously hit two spots that clobbered it back to
# a generic failure: (a) overall forced to FAIL, (b) failed forced to max(1,…) —
# once in process_notebook's strict return AND once in process_job's strict
# halt — while data_unavailable was dropped from the strict return entirely.

def test_strict_overall_preserves_pure_data_unavailable():
    src = _read("job_migrate.py")
    assert "_strict_pure_data_unavail" in src
    # overall is only forced to FAIL when it is NOT a pure data-unavailable stop
    assert "if strict_gate_failed and not _strict_pure_data_unavail:" in src
    # The guard must key on the data-unavailable signal, NOT on a complete cell
    # set: under --strict-cell-gating the data-unavailable cell gates the cells
    # below it, so migrated_cells is legitimately short. Requiring the full set
    # here mislabelled the task FAIL (live-caught, cluster c91dc735, 2026-09-19).
    anchor = "_strict_pure_data_unavail = ("
    block = src.split(anchor, 1)[1][:220]
    assert "cells_data_unavailable > 0" in block
    assert "cells_failed == 0" in block
    assert "len(migrated_cells) >= total_cells" not in block, (
        "must NOT require a complete cell set — strict gating shortens it by design")


def test_strict_return_carries_data_unavailable_and_honest_failed():
    src = _read("job_migrate.py")
    anchor = "partial artifact upload and registry update skipped"
    block = src.split(anchor, 1)[1][:900]
    # status comes from `overall` (PARTIAL_DATA_UNAVAILABLE for the pure case),
    # not a hardcoded "FAIL"
    assert '"status": overall,' in block
    assert '"data_unavailable": cells_data_unavailable,' in block
    # failed is NOT fabricated for a pure data-unavailable stop
    assert '"failed": cells_failed if _strict_pure_data_unavail else max(1, cells_failed),' in block


def test_process_job_strict_halt_preserves_data_unavailable():
    src = _read("job_migrate.py")
    # the strict-halt no longer unconditionally re-inflates failed
    anchor = "STRICT CELL GATE FAILED — halting remaining tasks"
    pre = src.split(anchor, 1)[0][-1200:]
    assert "_pure_data_unavail" in pre
    assert "if not _pure_data_unavail:" in pre
    assert 'result["failed"] = max(1, int(result.get("failed", 0) or 0))' in pre


def test_job_report_exposes_data_unavailable_column():
    src = _read("job_migrate.py")
    assert "| Task | Status | OK | Failed | DataUnavail | Fixed | Tokens |" in src
    assert 'data_unavail = r.get("data_unavailable", "-")' in src
