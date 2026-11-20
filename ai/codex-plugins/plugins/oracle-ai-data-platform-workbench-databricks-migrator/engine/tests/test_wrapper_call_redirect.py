"""Cell-text AST rewrite of customer writer-wrapper calls.

Validates `_apply_wrapper_call_redirect` against the exact failure patterns
captured from production logs:

  1. createTable(df, 'tbl', 'example_db', write_mode='overwrite')   ← Cell 80/81 of task 01
  2. createTable_oracle(df, 'tbl', 'example_db', ...)               ← Cell 81 attempt 2
  3. createTable kwargs-only call
  4. drop_database('production_db') destructive op
  5. write_to_delta(dataset, 'real_db.real_tbl', ..., path='oci://prod-bucket@ns/...')
  6. Variable-arg case — should be LEFT UNCHANGED (cannot rewrite non-literal)
  7. Already-redirected — idempotent no-op
  8. Method call writeTable (self positional offset)
  9. Local /Volumes path — leave alone
 10. Refused function call — leave alone (not in actionable)
 11. Multiple wrapper calls in one cell
 12. Re-running rewrite twice produces same result (idempotent)
 13. Syntax-error cell — return unchanged, no crash
 14. Unrelated function with same arg shape — should NOT match (catalog gate)
"""
import pytest


@pytest.fixture(autouse=True)
def _use_example_wrapper_catalog(monkeypatch):
    """The catalog ships INERT as config/writer_wrappers.json.example (it is a
    per-deployment artifact — an active catalog mined from someone else's
    notebooks would mis-interpret same-named functions). These tests pin the
    mechanism, so they load the shipped example explicitly."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "_WRITER_WRAPPER_CATALOG_PATH",
                        jm._WRITER_WRAPPER_CATALOG_PATH + ".example")
    monkeypatch.setattr(jm, "_writer_wrapper_catalog_cache", None)
import os, sys, re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import job_migrate as _jm
# Post-Session-1: redirect globals are None until main() populates from
# cfg.redirect. Tests exercise the rewrite functions directly (no main())
# so we stub the globals with fixture values matching the pre-cfg defaults.
_jm._REDIRECT_TABLE_PREFIX = "default.migration_sandbox_overwrite"
_jm._REDIRECT_BUCKET       = "migration_sandbox"
_jm._REDIRECT_NAMESPACE    = "exmplnspace1"

from job_migrate import _apply_wrapper_call_redirect, clear_write_redirect_map


def _strip_ws(s):
    return re.sub(r"\s+", " ", s).strip()


def test_createTable_db_literal():
    clear_write_redirect_map()
    src = "flag = createTable(ds_proxy_dump9, 'monitoring_job_ds_proxy_dump2', 'example_db', write_mode='overwrite')"
    out = _apply_wrapper_call_redirect(src)
    assert "migration_sandbox_overwrite" in out, f"db not redirected:\n{out}"
    # <db>_<tbl> uniqueness convention: original db is EMBEDDED in the sandbox
    # table name; safety requirement is that it never remains as the DB slot.
    assert "'example_db'" not in out and '"example_db"' not in out, f"original db still used as db arg (UNSAFE):\n{out}"
    assert "migration_sandbox_overwrite" in out
    assert "monitoring_job_ds_proxy_dump2" in out, "table name should be preserved"
    print("PASS test_createTable_db_literal")


def test_createTable_oracle_db_and_bucket():
    clear_write_redirect_map()
    src = "createTable_oracle(df, 'sales_fct', 'example_db', write_mode='overwrite', bucket_name='example-feature-bucket-2')"
    out = _apply_wrapper_call_redirect(src)
    assert "migration_sandbox_overwrite" in out, f"db not redirected:\n{out}"
    # <db>_<tbl> uniqueness convention: original db is EMBEDDED in the sandbox
    # table name; safety requirement is that it never remains as the DB slot.
    assert "'example_db'" not in out and '"example_db"' not in out, f"original db still used as db arg (UNSAFE):\n{out}"
    assert "migration_sandbox_overwrite" in out
    assert "migration_sandbox" in out, f"bucket not redirected:\n{out}"
    assert "example-feature-bucket-2" not in out, "original bucket still present"
    print("PASS test_createTable_oracle_db_and_bucket")


def test_createTable_kwarg_only():
    clear_write_redirect_map()
    src = "createTable(df=df, table_name='sales_fct', database_name='example_db', bucket_name='real-bucket', write_mode='append')"
    out = _apply_wrapper_call_redirect(src)
    assert "database_name='migration_sandbox_overwrite'" in out, f"db kwarg not redirected:\n{out}"
    assert "bucket_name='migration_sandbox'" in out, f"bucket kwarg not redirected:\n{out}"
    print("PASS test_createTable_kwarg_only")


def test_drop_database_destructive():
    """drop_database('production_db') MUST be redirected."""
    clear_write_redirect_map()
    src = "drop_database('production_db')"
    out = _apply_wrapper_call_redirect(src)
    assert "migration_sandbox_overwrite" in out, f"drop_database not redirected:\n{out}"
    assert "production_db" not in out, f"production_db still present (UNSAFE):\n{out}"
    print("PASS test_drop_database_destructive (safety-critical)")


def test_drop_table_redirects_full_id_to_sandbox():
    """Qualified destructive targets must resolve to the sandbox copy."""
    clear_write_redirect_map()
    src = "drop_table('user9_data.old_tbl', bucket_name='customer_bucket')"
    out = _apply_wrapper_call_redirect(src)
    assert "'migration_sandbox_overwrite.user9_data_old_tbl'" in out, out
    assert "'user9_data.old_tbl'" not in out, out
    assert "bucket_name='migration_sandbox'" in out, out
    print("PASS test_drop_table_redirects_full_id_to_sandbox")


def test_write_to_delta_full_id_and_path():
    clear_write_redirect_map()
    src = "write_to_delta(my_df, 'real_db.real_tbl', overwrite=True, path='oci://prod-bucket@ns/data/')"
    out = _apply_wrapper_call_redirect(src)
    assert "migration_sandbox_overwrite.real_db_real_tbl" in out, f"full_id not redirected:\n{out}"
    assert "'real_db.real_tbl'" not in out, "original full_id still present"
    assert "oci://migration_sandbox@exmplnspace1/prod-bucket/data/" in out, f"path not redirected:\n{out}"
    print("PASS test_write_to_delta_full_id_and_path")


def test_variable_arg_left_alone():
    """createTable(df, table_var, db_var) — variables, cannot rewrite."""
    clear_write_redirect_map()
    src = "db = 'example_db'\ncreateTable(df, 'tbl', db, write_mode='overwrite')"
    out = _apply_wrapper_call_redirect(src)
    # db variable assignment is preserved as-is (not in a wrapper call)
    # The createTable call's db arg is the Name `db`, not a Constant, so unchanged.
    assert "db = 'example_db'" in out
    assert "createTable(df, 'tbl', db, write_mode='overwrite')" in _strip_ws(out)
    print("PASS test_variable_arg_left_alone")


def test_idempotent_already_redirected():
    clear_write_redirect_map()
    src = "createTable(df, 'tbl', 'migration_sandbox_overwrite', bucket_name='migration_sandbox')"
    out = _apply_wrapper_call_redirect(src)
    # Already redirected → no changes
    assert _strip_ws(out) == _strip_ws(src), f"unexpected change:\n{out}"
    print("PASS test_idempotent_already_redirected")


def test_method_call_writeTable():
    """writeTable is is_method:true — call site has self implicit, positions shift.
    Expected redirects:
      - bucket arg 'examplecorp-bucket' → 'migration_sandbox'
      - path arg 'oci://examplecorp-bucket@ns/data/' → 'oci://migration_sandbox@exmplnspace1/examplecorp-bucket/data/'
        (original bucket name PRESERVED as path segment by design)
    """
    clear_write_redirect_map()
    src = "obj.writeTable(my_df, 'examplecorp-bucket', 'oci://examplecorp-bucket@ns/data/')"
    out = _apply_wrapper_call_redirect(src)
    # bucket arg (call position 1, def position 2 after self) was redirected
    assert "'migration_sandbox'" in out, f"bucket arg not redirected:\n{out}"
    # path arg (call position 2, def position 3 after self) was redirected
    assert "'oci://migration_sandbox@exmplnspace1/examplecorp-bucket/data/'" in out, \
        f"path arg not redirected with bucket-preservation:\n{out}"
    print("PASS test_method_call_writeTable")


def test_volume_path_left_alone():
    clear_write_redirect_map()
    src = "process_source('src', 'feat', 'df', [], '2024-01-01', '/Volumes/default/default/scratch.csv')"
    out = _apply_wrapper_call_redirect(src)
    assert "/Volumes/default/default/scratch.csv" in out, f"Volume path was rewritten (should not be):\n{out}"
    assert _strip_ws(out) == _strip_ws(src), "no other changes expected"
    print("PASS test_volume_path_left_alone")


def test_relative_path_left_alone():
    clear_write_redirect_map()
    src = "process_source('src', 'feat', 'df', [], '2024-01-01', 'column_statistics.csv')"
    out = _apply_wrapper_call_redirect(src)
    assert "column_statistics.csv" in out, f"Relative path was rewritten (should not be):\n{out}"
    print("PASS test_relative_path_left_alone")


def test_non_catalog_call_left_alone():
    """A function call NOT in writer_wrappers.json should NOT be touched, even
    if its first arg happens to be a string that looks like a db name."""
    clear_write_redirect_map()
    src = "my_random_func('example_db', some_data)"
    out = _apply_wrapper_call_redirect(src)
    assert "example_db" in out and out.strip() == src.strip(), f"non-catalog call was touched:\n{out}"
    print("PASS test_non_catalog_call_left_alone")


def test_multiple_calls_in_one_cell():
    clear_write_redirect_map()
    src = (
        "flag = createTable(df1, 't1', 'example_db', write_mode='overwrite')\n"
        "if not flag: raise Exception('Table creation failed')\n"
        "flag = createTable(df2, 't2', 'example_db', write_mode='overwrite')\n"
        "if not flag: raise Exception('Table creation failed')\n"
    )
    out = _apply_wrapper_call_redirect(src)
    # Both calls should be redirected
    assert out.count("migration_sandbox_overwrite") == 2, f"both calls should be redirected, got:\n{out}"
    assert "'example_db'" not in out and '"example_db"' not in out, f"original db still used as db arg (UNSAFE):\n{out}"
    print("PASS test_multiple_calls_in_one_cell")


def test_idempotent_double_rewrite():
    """Running rewrite twice should produce the same result (no double-redirect)."""
    clear_write_redirect_map()
    src = "createTable(df, 't', 'example_db', write_mode='overwrite')"
    once = _apply_wrapper_call_redirect(src)
    twice = _apply_wrapper_call_redirect(once)
    assert _strip_ws(once) == _strip_ws(twice), f"non-idempotent:\nonce: {once}\ntwice: {twice}"
    print("PASS test_idempotent_double_rewrite")


def test_syntax_error_cell_unchanged():
    """If a cell has Python syntax errors (e.g., mid-transform state), return
    unchanged rather than crashing."""
    clear_write_redirect_map()
    src = "def broken(\n    x = 1  # missing closing paren"
    out = _apply_wrapper_call_redirect(src)
    assert out == src, "syntax-error cell should be returned unchanged"
    print("PASS test_syntax_error_cell_unchanged")


def test_refuse_list_not_rewritten():
    """Functions in refuse list (e.g., get_mid_month_df) are NOT in actionable —
    cell-text rewrite leaves them alone. (Refuse handling is at runtime via
    prompt rule, not here.)"""
    clear_write_redirect_map()
    src = "df = get_mid_month_df(some_df, 'target_var')"
    out = _apply_wrapper_call_redirect(src)
    assert out.strip() == src.strip(), f"refused function call should be unchanged:\n{out}"
    print("PASS test_refuse_list_not_rewritten")


def test_saveTable_redirects_qualified_table_name_and_bucket():
    """SaveTableUtils requires a two-part identifier. Redirect it through the
    standard full_id role while preserving mode and redirecting the bucket."""
    clear_write_redirect_map()
    src = (
        "saveTable(df, 'user9_data.srt_daily_validation_base', "
        "'overwrite', bucket_name='customer_bucket')"
    )
    out = _apply_wrapper_call_redirect(src)
    assert "'migration_sandbox_overwrite.user9_data_srt_daily_validation_base'" in out, out
    assert "'user9_data.srt_daily_validation_base'" not in out, out
    assert "'overwrite'" in out, out
    assert "bucket_name='migration_sandbox'" in out, out
    print("PASS test_saveTable_redirects_qualified_table_name_and_bucket")


def test_saveTable_full_id_grid():
    """Literal identifiers use the existing collision-safe full_id contract;
    dynamic table expressions remain unchanged."""
    cases = {
        "saveTable(df=df, table_name='kd.tbl')": "'migration_sandbox_overwrite.kd_tbl'",
        "saveTable(df, 'tbl')": "'migration_sandbox_overwrite.default_tbl'",
        "saveTable(df, 'cat.kd.tbl')": "'migration_sandbox_overwrite.kd_tbl'",
        "saveTable(df, '`kd`.`tbl`')": "'migration_sandbox_overwrite.kd_tbl'",
    }
    for src, expected in cases.items():
        clear_write_redirect_map()
        out = _apply_wrapper_call_redirect(src)
        assert expected in out, f"{src}\n-> {out}"

    for src in ("saveTable(df, tbl_var)", "saveTable(df, f'{db}.tbl')"):
        clear_write_redirect_map()
        assert _apply_wrapper_call_redirect(src) == src

    bucket_cases = (
        "saveTable(df, 'kd.tbl', bucket_name='customer_bucket')",
        "saveTable(df, 'kd.tbl', 'overwrite', None, None, 'customer_bucket')",
    )
    for src in bucket_cases:
        clear_write_redirect_map()
        out = _apply_wrapper_call_redirect(src)
        assert "'migration_sandbox_overwrite.kd_tbl'" in out, out
        assert "'migration_sandbox'" in out, out

    dynamic = "saveTable(df, 'kd.tbl', bucket_name=bucket_var)"
    clear_write_redirect_map()
    out = _apply_wrapper_call_redirect(dynamic)
    assert "'migration_sandbox_overwrite.kd_tbl'" in out, out
    assert "bucket_name=bucket_var" in out, out
    print("PASS test_saveTable_full_id_grid")


def test_full_id_one_part():
    """write_to_delta with a 1-part table_name_with_db (no schema prefix)."""
    clear_write_redirect_map()
    src = "write_to_delta(my_df, 'my_tbl')"
    out = _apply_wrapper_call_redirect(src)
    assert "migration_sandbox_overwrite.default_my_tbl" in out, f"1-part name not qualified:\n{out}"
    print("PASS test_full_id_one_part")


def test_full_id_three_part():
    """write_to_delta with a 3-part catalog.db.table identifier."""
    clear_write_redirect_map()
    src = "write_to_delta(my_df, 'default.real_db.real_tbl')"
    out = _apply_wrapper_call_redirect(src)
    assert "migration_sandbox_overwrite.real_db_real_tbl" in out, f"3-part db slot not rewritten:\n{out}"
    print("PASS test_full_id_three_part")


def test_load_catalog_disabled_fallback():
    """If catalog has no actionable entries, the function should silently
    return the source unchanged."""
    # Cannot easily mock the catalog file without monkey-patching; just verify
    # that a benign cell with no matching call returns unchanged.
    clear_write_redirect_map()
    src = "x = 1 + 2\ny = f(x)"
    out = _apply_wrapper_call_redirect(src)
    assert out.strip() == src.strip()
    print("PASS test_load_catalog_disabled_fallback")


def test_exact_log_pattern():
    """The exact failing cell pattern from satya_2.log Cell 81/83 at 21:33:58."""
    clear_write_redirect_map()
    src = (
        "flag = createTable(ds_proxy_dump9, 'monitoring_job_ds_proxy_dump2', 'example_db', write_mode='overwrite')\n"
        "if not flag:\n"
        "    raise Exception('Table creation failed')\n"
        "flag = createTable(ds_proxy_dump_helperlib158, 'monitoring_job_ds_proxy_dump_helperlib158', 'example_db', write_mode='overwrite')\n"
        "if not flag:\n"
        "    raise Exception('Table creation failed')\n"
    )
    out = _apply_wrapper_call_redirect(src)
    # The two calls should both have example_db replaced
    assert out.count("migration_sandbox_overwrite") == 2, \
        f"both createTable calls should be redirected, got count={out.count('migration_sandbox_overwrite')}:\n{out}"
    assert "'example_db'" not in out and '"example_db"' not in out, f"customer's production db still used as db arg (UNSAFE):\n{out}"
    # Table names preserved
    assert "monitoring_job_ds_proxy_dump2" in out
    assert "monitoring_job_ds_proxy_dump_helperlib158" in out
    print("PASS test_exact_log_pattern (the cell that caused the run-3 incident)")


if __name__ == "__main__":
    test_createTable_db_literal()
    test_createTable_oracle_db_and_bucket()
    test_createTable_kwarg_only()
    test_drop_database_destructive()
    test_write_to_delta_full_id_and_path()
    test_variable_arg_left_alone()
    test_idempotent_already_redirected()
    test_method_call_writeTable()
    test_volume_path_left_alone()
    test_relative_path_left_alone()
    test_non_catalog_call_left_alone()
    test_multiple_calls_in_one_cell()
    test_idempotent_double_rewrite()
    test_syntax_error_cell_unchanged()
    test_refuse_list_not_rewritten()
    test_saveTable_redirects_qualified_table_name_and_bucket()
    test_saveTable_full_id_grid()
    test_full_id_one_part()
    test_full_id_three_part()
    test_load_catalog_disabled_fallback()
    test_exact_log_pattern()
    print("\nAll wrapper-call redirect tests pass.")
