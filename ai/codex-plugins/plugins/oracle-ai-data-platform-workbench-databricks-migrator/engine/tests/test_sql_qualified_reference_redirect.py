"""Regression coverage for SQL qualifiers after write/read isolation."""

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

from job_migrate import (
    _apply_read_redirects,
    _apply_write_redirects,
    clear_write_redirect_map,
)


def setup_function():
    clear_write_redirect_map()


def test_correlated_subquery_qualified_column_follows_redirect():
    source = '''query = f"""
INSERT INTO example_feature_backup.example_loan_o1_v{fl_version}
SELECT A.decision_id
FROM example_loan_table A
WHERE NOT EXISTS (
  SELECT NULL
  FROM example_feature_backup.example_loan_o1_v{fl_version}
  WHERE A.decision_id =
    example_feature_backup.example_loan_o1_v{fl_version}.decision_id
)
"""'''

    result = _apply_read_redirects(_apply_write_redirects(source))
    redirected = (
        "default.migration_sandbox_overwrite."
        "example_feature_backup_example_loan_o1_v{fl_version}"
    )

    assert f"INSERT INTO {redirected}" in result
    assert f"FROM {redirected}" in result
    assert f"{redirected}.decision_id" in result
    assert (
        "example_feature_backup.example_loan_o1_v"
        "{fl_version}.decision_id"
    ) not in result.replace(f"{redirected}.decision_id", "")


def test_qualified_reference_redirect_is_idempotent():
    source = '''query = """
INSERT INTO customer_db.target_table
SELECT source_id
FROM customer_db.target_table
WHERE customer_db.target_table.source_id IS NOT NULL
"""'''

    once = _apply_read_redirects(_apply_write_redirects(source))
    twice = _apply_read_redirects(_apply_write_redirects(once))

    assert once == twice
    assert (
        once.count(
            "default.migration_sandbox_overwrite."
            "customer_db_target_table"
        )
        == 3
    )
