"""Execution-only isolation of explicit CREATE TABLE OCI locations."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


# Post-Session-1: redirect globals are None until main() populates from
# cfg.redirect. Tests exercise the rewrite functions directly (no main())
# so we stub the globals with fixture values matching the pre-cfg defaults.
import job_migrate as _jm
_jm._REDIRECT_TABLE_PREFIX = "default.migration_sandbox_overwrite"
_jm._REDIRECT_BUCKET       = "migration_sandbox"
_jm._REDIRECT_NAMESPACE    = "exmplnspace1"

from job_migrate import _apply_write_redirects, clear_write_redirect_map


def setup_function():
    clear_write_redirect_map()


def test_create_table_redirects_name_and_explicit_oci_location():
    source = """
spark.sql(\"\"\"
CREATE TABLE IF NOT EXISTS example_feature_backup.example_offer_v1
USING DELTA
LOCATION 'oci://oci-example-feature-bucket@exmplnspace1/example_central_tables/example_feature_backup/example_offer_v1/partitioned_data'
\"\"\")
"""

    result = _apply_write_redirects(source)

    assert (
        "default.migration_sandbox_overwrite."
        "example_feature_backup_example_offer_v1"
    ) in result
    assert (
        "LOCATION "
        "'oci://migration_sandbox@exmplnspace1/"
        "oci-example-feature-bucket/"
        "example_central_tables/example_feature_backup/"
        "example_offer_v1/partitioned_data'"
    ) in result
    assert "LOCATION 'oci://oci-example-feature-bucket@" not in result


def test_retry_with_already_redirected_table_still_redirects_location():
    source = """
spark.sql(\"\"\"
CREATE TABLE default.migration_sandbox_overwrite.db_tbl
USING PARQUET
LOCATION "oci://prod-bucket@prodns/data/db_tbl"
\"\"\")
"""

    result = _apply_write_redirects(source)

    assert (
        'LOCATION "oci://migration_sandbox@exmplnspace1/'
        'prod-bucket/data/db_tbl"'
    ) in result


def test_non_create_sql_location_is_not_changed_by_this_fix():
    source = """
spark.sql(\"\"\"
ALTER TABLE customer_db.customer_table
SET LOCATION 'oci://prod-bucket@prodns/data/customer_table'
\"\"\")
"""

    result = _apply_write_redirects(source)

    assert "default.migration_sandbox_overwrite.customer_db_customer_table" in result
    assert "LOCATION 'oci://prod-bucket@prodns/data/customer_table'" in result


def test_create_table_with_already_sandboxed_location_is_idempotent():
    source = """
spark.sql(\"\"\"
CREATE TABLE customer_db.customer_table
USING DELTA
LOCATION 'oci://migration_sandbox@exmplnspace1/customer/data'
\"\"\")
"""

    once = _apply_write_redirects(source)
    twice = _apply_write_redirects(once)

    assert once == twice
    assert once.count("migration_sandbox@exmplnspace1") == 1
