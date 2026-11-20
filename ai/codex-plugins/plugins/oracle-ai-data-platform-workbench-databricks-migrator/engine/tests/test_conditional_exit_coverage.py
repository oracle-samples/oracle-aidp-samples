"""Conditional notebook.exit() coverage — convert-below, never execute, PASS.

Incident (2026-09-10): `03_master` had `if datetime.now().hour % 2 == 0:
dbutils.notebook.exit(...)`. At an even hour the exit fired, so cells 3-5 and
the child notebooks below were never migrated, yet the task reported SUCCESS.
A clone run at an ODD hour took the other branch into that never-migrated code
→ `Widget not found: experiment_name` in Txn_Budget_Monitoring_example_M14.

Contract (see scripts/job_migrate.py::_exit_is_unconditional and AGENTS.md):
  * UNCONDITIONAL exit (top-level) → code below dead in every run → skip raw
    (unchanged).
  * CONDITIONAL exit (nested — hour/date/data-count/any guard) → code below is
    conditionally-live → CONVERT it (code-only, NEVER execute — the guard may
    be legitimate, e.g. data-count, and forcing the branch is unsound) and
    report the coverage gap; task stays PASS (PASS_CONDITIONAL_EXIT_COVERAGE).

These tests pin the DISCRIMINATOR (the load-bearing, predicate-agnostic part).
The full cell-loop behaviour (convert-only continuation for EVERY cell type
below a fired exit, no execution, no code loss, PASS taxonomy) is exercised
against the real process_notebook loop with a fake kernel in
test_conditional_exit_below_execute.py.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import job_migrate as jm


def test_nested_hour_guard_is_conditional():
    src = "current_hour = datetime.now().hour\nif current_hour % 2 == 0:\n    dbutils.notebook.exit('even')"
    assert jm._exit_is_unconditional(src) is False


def test_nested_data_count_guard_is_conditional():
    """The soundness case: a data-count guard must be classified conditional so
    the tool NEVER force-executes its data-dependent downstream."""
    src = "if data_count == 0:\n    aidputils.notebook.exit('no data')\ncompute(data)"
    assert jm._exit_is_unconditional(src) is False


def test_nested_date_guard_is_conditional():
    src = "if datetime.now().date().day == 1:\n    dbutils.notebook.exit('month start')"
    assert jm._exit_is_unconditional(src) is False


def test_top_level_exit_is_unconditional():
    assert jm._exit_is_unconditional("dbutils.notebook.exit('always')") is True
    assert jm._exit_is_unconditional("x = 1\naidputils.notebook.exit('done')") is True
    assert jm._exit_is_unconditional("oidlUtils.notebook.exit('done')") is True


def test_commented_exit_never_counts():
    assert jm._exit_is_unconditional("# dbutils.notebook.exit('x')\nprint(1)") is False


def test_exit_inside_loop_or_try_is_conditional():
    assert jm._exit_is_unconditional("for i in r:\n    dbutils.notebook.exit('e')") is False
    assert jm._exit_is_unconditional("try:\n    dbutils.notebook.exit('t')\nexcept Exception:\n    pass") is False


def test_unseen_guard_shape_defaults_to_conditional():
    """Predicate-agnostic guarantee: a guard shape the tool has never seen must
    still be treated as conditional (indented → not top-level), so it can never
    slip through into a false-SUCCESS skip."""
    src = "if weird_custom_predicate(foo, bar) and baz.qux:\n    dbutils.notebook.exit('?')"
    assert jm._exit_is_unconditional(src) is False


def test_compound_one_line_exit_defaults_to_conditional():
    """The classifier remains conservative even when reporting is generic."""
    assert jm._exit_is_unconditional(
        "except: dbutils.notebook.exit('missing')") is False


def test_below_conditional_exit_status_string_stays_pass_family():
    """The RESULT taxonomy uses a PASS_-prefixed terminal so every downstream
    success gate treats a conditional-exit run as success, not PARTIAL."""
    src = open(os.path.join(os.path.dirname(__file__), "..", "scripts",
                            "job_migrate.py")).read()
    assert 'overall = "PASS_CONDITIONAL_EXIT_COVERAGE"' in src
    # and it must sit in the PASS branch region, never trip PARTIAL_DATA_UNAVAILABLE
    assert "cells_below_conditional_exit" in src


def test_strict_gating_treats_conditional_exit_as_success():
    """Decided 2026-09-10: PASS_CONDITIONAL_EXIT_COVERAGE is success EVEN under
    --strict-cell-gating (unlike PASS_DATA_SUBSTITUTED). Both strict gates must
    exempt this terminal, or a conditional-exit run would wrongly FAIL/halt the
    job under strict mode — contradicting the agreed 'always PASS' behaviour."""
    src = open(os.path.join(os.path.dirname(__file__), "..", "scripts",
                            "job_migrate.py")).read()
    # process_notebook strict gate: success set includes the terminal
    assert '("PASS", "PASS_CONDITIONAL_EXIT_COVERAGE")' in src
    # job-level strict gate: the not-in check includes the terminal
    assert 'not in ("PASS", "PASS_CONDITIONAL_EXIT_COVERAGE")' in src
    # scope guard: the STRICT success set does NOT include PASS_DATA_SUBSTITUTED
    # (its strict behaviour is deliberately unchanged — only conditional-exit
    # is exempted).
    assert '("PASS", "PASS_DATA_SUBSTITUTED")' not in src.split("strict_gate_failed")[1][:400]


def test_multiline_guards_keep_exact_reporting():
    cases = {
        "if ready:\n    dbutils.notebook.exit('x')": "if ready:",
        "try:\n    aidputils.notebook.exit('x')": "try:",
        "for row in rows:\n    oidlUtils.notebook.exit('x')": "for row in rows:",
        "%scala\nif (ready) {\n  oidlUtils.notebook.exit(\"x\")\n}": "if (ready) {",
    }
    for source, expected in cases.items():
        assert jm._conditional_exit_guard_line(source) == expected
        assert jm._conditional_exit_guard_hint(source) == f"under guard [{expected}]"


def test_compound_and_unsupported_shapes_use_generic_fallback():
    cases = (
        "except: dbutils.notebook.exit('x')",
        "if not rows: dbutils.notebook.exit('x')",
        "else: aidputils.notebook.exit('x')",
        "x = 1; dbutils.notebook.exit('x')",
        "match value:\n    case 1: dbutils.notebook.exit('x')",
        "async for row in rows: dbutils.notebook.exit('x')",
        "%scala\nif (!rows.nonEmpty) oidlUtils.notebook.exit(\"x\")",
        "print(1)",
        "",
    )
    for source in cases:
        assert jm._conditional_exit_guard_line(source) is None, source
        hint = jm._conditional_exit_guard_hint(source)
        assert "inspect this cell" in hint
        assert "controlling predicate to flip" in hint


def test_identifier_prefixes_are_not_promoted_to_guards():
    cases = (
        "elsewhere:\n    dbutils.notebook.exit('x')",
        "tryit:\n    dbutils.notebook.exit('x')",
        "exceptional_case:\n    dbutils.notebook.exit('x')",
    )
    for source in cases:
        assert jm._conditional_exit_guard_line(source) is None


def test_comment_only_exit_uses_generic_fallback():
    source = "# dbutils.notebook.exit('documented')\nprint(1)"
    assert jm._conditional_exit_guard_line(source) is None
    assert "inspect this cell" in jm._conditional_exit_guard_hint(source)
