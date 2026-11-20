"""End-to-end test for the runtime writer-wrapper interceptor.

Validates:
  1. Catalog loads from config/writer_wrappers.json.
  2. The install code, when exec()'d in a synthetic globals(), correctly:
     - wraps actionable functions (createTable, drop_table, etc.) with
       redirect logic
     - replaces refuse-list functions with raisers
     - is idempotent (re-running doesn't double-wrap)
  3. Each role redirector produces the right output:
     - db: any non-temp name → "migration_sandbox_overwrite"
     - bucket: any non-temp name → "migration_sandbox"
     - path: oci://X@ns/path → oci://migration_sandbox@exmplnspace1/X/path;
             non-OCI paths unchanged (/Volumes/, relative, local)
     - full_id: source identity is folded into a table in the redirect schema
  4. The wrapper passes through unredirected args, calls original with rewritten args.
  5. A refused function raises with a useful message.

Tests are pure Python — no cluster, no AIDP API calls. We exec the install
code in a fake module namespace and exercise the resulting wrappers.
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
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

# Post-Session-1: redirect globals are None until main() populates from
# cfg.redirect. Tests exercise the rewrite functions directly (no main()) so
# we stub the globals with fixture values matching the pre-cfg defaults.
import job_migrate as _jm
_jm._REDIRECT_TABLE_PREFIX = "default.migration_sandbox_overwrite"
_jm._REDIRECT_BUCKET       = "migration_sandbox"
_jm._REDIRECT_NAMESPACE    = "exmplnspace1"


def _build_test_namespace():
    """Build a fake kernel namespace with example customer wrappers."""
    ns = {}

    # createTable: SaveTableUtils signature
    def createTable(df, table_name, database_name, write_mode='append',
                    write_format='parquet', bucket_name='example-datascience-temporary-bucket',
                    pod_name='default_user', partition_by_column=None):
        ns['_last_createTable_args'] = {
            'df': df, 'table_name': table_name, 'database_name': database_name,
            'write_mode': write_mode, 'bucket_name': bucket_name, 'pod_name': pod_name,
        }
        return True

    # drop_database: destructive
    def drop_database(db, bucket_name='example-datascience-userspace-bucket',
                      pod_name='default_user', delete_paths=False):
        ns['_last_drop_database_args'] = {'db': db, 'bucket_name': bucket_name}
        return True

    def drop_table(table_name, bucket_name='example-datascience-userspace-bucket',
                   pod_name='default_user', delete_paths=False):
        ns['_last_drop_table_args'] = {
            'table_name': table_name, 'bucket_name': bucket_name,
        }
        return True

    # write_to_delta: uses full_id role
    def write_to_delta(dataset, table_name_with_db, overwrite=False,
                      overwrite_schema=False, append=False, partitionBy=None, path=None):
        ns['_last_write_to_delta_args'] = {
            'dataset': dataset, 'table_name_with_db': table_name_with_db, 'path': path,
        }
        return True

    def saveTable(df, table_name, mode='append', partition_by=None,
                  write_type='parquet', bucket_name='customer_bucket'):
        ns['_last_saveTable_args'] = {
            'df': df, 'table_name': table_name, 'mode': mode,
            'partition_by': partition_by, 'write_type': write_type,
            'bucket_name': bucket_name,
        }
        return True

    # process_source: writes to output_path
    def process_source(source, feature_name, dsp_df, flags_to_remove, run_date, output_path):
        ns['_last_process_source_args'] = {
            'source': source, 'output_path': output_path,
        }
        return True

    # A "refuse" wrapper: writes to hardcoded targets
    def get_mid_month_df(df, target_var):
        ns['_last_get_mid_month_df_called'] = True
        return df

    # A non-wrapper function (not in catalog) — should be left untouched
    def regular_function(x):
        return x * 2

    ns['createTable'] = createTable
    ns['drop_database'] = drop_database
    ns['drop_table'] = drop_table
    ns['write_to_delta'] = write_to_delta
    ns['saveTable'] = saveTable
    ns['process_source'] = process_source
    ns['get_mid_month_df'] = get_mid_month_df
    ns['regular_function'] = regular_function
    return ns


def _install_into(ns):
    """Run the actual install code from job_migrate in the given namespace."""
    from job_migrate import _load_writer_wrappers_catalog, _build_writer_interceptor_install_code
    catalog = _load_writer_wrappers_catalog()
    install_code = _build_writer_interceptor_install_code(catalog)
    exec(install_code, ns)


def test_catalog_loads():
    from job_migrate import _load_writer_wrappers_catalog
    catalog = _load_writer_wrappers_catalog()
    assert "actionable" in catalog
    assert "refuse" in catalog
    actionable_names = [a["name"] for a in catalog["actionable"]]
    # Spot-check that core wrappers are in the catalog
    assert "createTable" in actionable_names, "createTable must be in catalog"
    assert "drop_database" in actionable_names, "drop_database must be in catalog"
    assert "drop_table" in actionable_names, "drop_table must be in catalog"
    assert "write_to_delta" in actionable_names, "write_to_delta must be in catalog"
    full_id_consumers = {
        entry["name"]
        for entry in catalog["actionable"]
        if "full_id" in entry.get("args", {}).values()
    }
    assert full_id_consumers == {"saveTable", "write_to_delta", "drop_table"}
    refuse_names = [r["name"] if isinstance(r, dict) else r for r in catalog["refuse"]]
    assert "get_mid_month_df" in refuse_names, "get_mid_month_df must be in refuse list"
    print(f"PASS test_catalog_loads ({len(actionable_names)} actionable, {len(refuse_names)} refuse)")


def test_install_wraps_actionable():
    ns = _build_test_namespace()
    orig_createTable = ns['createTable']
    _install_into(ns)
    assert getattr(ns['createTable'], '__aidp_redirected__', False), \
        "createTable should be wrapped"
    assert ns['createTable'] is not orig_createTable, \
        "createTable should be replaced with wrapper"
    assert ns['createTable'].__wrapped__ is orig_createTable, \
        "wrapper should preserve original"
    print("PASS test_install_wraps_actionable")


def test_install_refuses_non_actionable():
    ns = _build_test_namespace()
    _install_into(ns)
    assert getattr(ns['get_mid_month_df'], '__aidp_refused__', False), \
        "get_mid_month_df should be a refusal stub"
    # Calling it should raise
    try:
        ns['get_mid_month_df']('df', 'tgt')
        assert False, "refused wrapper should raise"
    except Exception as e:
        assert 'AIDP tool refuses to invoke' in str(e), f"unexpected message: {e}"
        # The remediation hint must point at the LIVE catalog path — the loader
        # reads config/ first and consults reports/ only as a deprecated
        # fallback, so directing operators to reports/ silently discards their
        # catalog entry (PR#84 review, 2026-09-08).
        assert 'config/writer_wrappers.json' in str(e), \
            f"refusal must direct operators to config/, got: {e}"
        assert 'reports/writer_wrappers.json' not in str(e), \
            f"refusal must not direct operators to the ignored legacy path: {e}"
    print("PASS test_install_refuses_non_actionable")


def test_install_leaves_non_catalog_alone():
    ns = _build_test_namespace()
    orig_regular = ns['regular_function']
    _install_into(ns)
    assert ns['regular_function'] is orig_regular, \
        "regular_function (not in catalog) should be unchanged"
    assert ns['regular_function'](5) == 10
    print("PASS test_install_leaves_non_catalog_alone")


def test_install_idempotent():
    ns = _build_test_namespace()
    _install_into(ns)
    wrapped1 = ns['createTable']
    _install_into(ns)
    wrapped2 = ns['createTable']
    assert wrapped1 is wrapped2, "double-install should NOT replace already-wrapped function"
    print("PASS test_install_idempotent")


def test_createTable_redirects_db_and_bucket():
    ns = _build_test_namespace()
    _install_into(ns)
    ns['createTable']('df_obj', 'sales_fct', 'example_db',
                       write_mode='overwrite', bucket_name='example-prod')
    args = ns['_last_createTable_args']
    assert args['table_name'] == 'sales_fct', "table_name should NOT be redirected"
    assert args['database_name'] == 'migration_sandbox_overwrite', \
        f"database_name should be redirected: {args['database_name']}"
    assert args['bucket_name'] == 'migration_sandbox', \
        f"bucket_name should be redirected: {args['bucket_name']}"
    assert args['write_mode'] == 'overwrite', "write_mode should NOT be redirected"
    print("PASS test_createTable_redirects_db_and_bucket")


def test_createTable_idempotent_on_already_redirected():
    ns = _build_test_namespace()
    _install_into(ns)
    # Passing already-redirected names should be unchanged
    ns['createTable']('df', 'sales_fct', 'migration_sandbox_overwrite',
                       bucket_name='migration_sandbox')
    args = ns['_last_createTable_args']
    assert args['database_name'] == 'migration_sandbox_overwrite'
    assert args['bucket_name'] == 'migration_sandbox'
    print("PASS test_createTable_idempotent_on_already_redirected")


def test_drop_database_redirects_db():
    """Critical: drop_database('production_db') MUST redirect to temp schema."""
    ns = _build_test_namespace()
    _install_into(ns)
    ns['drop_database']('production_db')
    args = ns['_last_drop_database_args']
    assert args['db'] == 'migration_sandbox_overwrite', \
        f"drop_database('production_db') must NOT hit production: db={args['db']}"
    print("PASS test_drop_database_redirects_db (safety-critical)")


def test_write_to_delta_redirects_full_id():
    ns = _build_test_namespace()
    _install_into(ns)
    # 2-part name
    ns['write_to_delta']('df', 'real_db.real_tbl', path='oci://prod-bucket@ns/data/')
    args = ns['_last_write_to_delta_args']
    assert args['table_name_with_db'] == 'migration_sandbox_overwrite.real_db_real_tbl', \
        f"full_id should preserve source schema identity: {args['table_name_with_db']}"
    assert args['path'] == 'oci://migration_sandbox@exmplnspace1/prod-bucket/data/', \
        f"path should be redirected: {args['path']}"
    print("PASS test_write_to_delta_redirects_full_id")


def test_drop_table_runtime_redirects_full_id():
    ns = _build_test_namespace()
    _install_into(ns)
    ns['drop_table']('user9_data.old_tbl', bucket_name='customer_bucket')
    args = ns['_last_drop_table_args']
    assert args['table_name'] == 'migration_sandbox_overwrite.user9_data_old_tbl'
    assert args['bucket_name'] == 'migration_sandbox'
    print("PASS test_drop_table_runtime_redirects_full_id")


def test_saveTable_runtime_redirects_qualified_table_name_and_bucket():
    ns = _build_test_namespace()
    _install_into(ns)
    ns['saveTable'](
        'df', 'user9_data.srt_daily_validation_base',
        mode='overwrite', bucket_name='customer_bucket',
    )
    args = ns['_last_saveTable_args']
    assert args['table_name'] == 'migration_sandbox_overwrite.user9_data_srt_daily_validation_base'
    assert args['bucket_name'] == 'migration_sandbox'
    assert args['mode'] == 'overwrite'
    print("PASS test_saveTable_runtime_redirects_qualified_table_name_and_bucket")


def test_path_redirect_preserves_local_paths():
    """Local /Volumes/, /Workspace/, relative, and bare filenames must NOT be redirected."""
    ns = _build_test_namespace()
    _install_into(ns)
    # /Volumes path → preserve
    ns['process_source']('src', 'feat', 'df', [], '2024-01-01', '/Volumes/default/default/x/')
    assert ns['_last_process_source_args']['output_path'] == '/Volumes/default/default/x/'
    # /Workspace path → preserve
    ns['process_source']('src', 'feat', 'df', [], '2024-01-01', '/Workspace/Users/x/')
    assert ns['_last_process_source_args']['output_path'] == '/Workspace/Users/x/'
    # Relative bare filename → preserve
    ns['process_source']('src', 'feat', 'df', [], '2024-01-01', 'column_statistics.csv')
    assert ns['_last_process_source_args']['output_path'] == 'column_statistics.csv'
    print("PASS test_path_redirect_preserves_local_paths")


def test_path_redirect_rewrites_oci():
    ns = _build_test_namespace()
    _install_into(ns)
    ns['process_source']('src', 'feat', 'df', [], '2024-01-01',
                          'oci://example-data@exmplnspace1/sales/2024/')
    out = ns['_last_process_source_args']['output_path']
    assert out == 'oci://migration_sandbox@exmplnspace1/example-data/sales/2024/', \
        f"OCI path should be rewritten: {out}"
    print("PASS test_path_redirect_rewrites_oci")


def test_kwargs_only_call():
    ns = _build_test_namespace()
    _install_into(ns)
    # Pure kwargs call
    ns['createTable'](df='df', table_name='t', database_name='real_db', bucket_name='real_bucket')
    args = ns['_last_createTable_args']
    assert args['database_name'] == 'migration_sandbox_overwrite'
    assert args['bucket_name'] == 'migration_sandbox'
    assert args['table_name'] == 't'
    print("PASS test_kwargs_only_call")


def test_default_values_redirected_if_role_matches():
    """If user calls createTable() with defaults (no bucket_name override), the
    DEFAULT value ('example-datascience-temporary-bucket') should ALSO be redirected,
    because apply_defaults() fills it in and the wrapper redirects all role-matched args."""
    ns = _build_test_namespace()
    _install_into(ns)
    ns['createTable']('df', 'sales_fct', 'example_db')  # no bucket_name → default applies
    args = ns['_last_createTable_args']
    assert args['bucket_name'] == 'migration_sandbox', \
        f"default bucket value should be redirected: {args['bucket_name']}"
    print("PASS test_default_values_redirected_if_role_matches")


if __name__ == "__main__":
    test_catalog_loads()
    test_install_wraps_actionable()
    test_install_refuses_non_actionable()
    test_install_leaves_non_catalog_alone()
    test_install_idempotent()
    test_createTable_redirects_db_and_bucket()
    test_createTable_idempotent_on_already_redirected()
    test_drop_database_redirects_db()
    test_write_to_delta_redirects_full_id()
    test_path_redirect_preserves_local_paths()
    test_path_redirect_rewrites_oci()
    test_kwargs_only_call()
    test_default_values_redirected_if_role_matches()
    print("\nAll writer-wrapper interceptor tests pass.")
