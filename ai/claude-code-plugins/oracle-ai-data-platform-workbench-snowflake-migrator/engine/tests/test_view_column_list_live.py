"""A view's column list is carried by aliasing the body, never as a view
column list -- AIDP cannot read a view created with one.

Live 2026-09-29, AIDP (Spark 3.5 + Hive metastore): the structure job ran
the plan's

    CREATE VIEW IF NOT EXISTS <fqn> (`CUSTOMER`, `TOTAL`) AS
    SELECT CUST_ID, SUM(AMT) ...

The CREATE succeeded; every READ of the view then failed

    [INCOMPATIBLE_VIEW_SCHEMA_CHANGE] ... column customer cannot be
    resolved. Expected 1 columns named customer but got [].

A probe on the live cluster tried four forms. A view column list, upper or
lower case, was unreadable; aliases in the select list read (columns
customer, total); the derived-table alias `SELECT * FROM (<body>) AS
named_columns(`CUSTOMER`, `TOTAL`)` read. A list that does NOT rename (its
names equal the body's output) read fine, which is why most views never
showed it.

So build_create_view carries the list the way the catalog API's viewText
already did: `CREATE VIEW IF NOT EXISTS <fqn> [COMMENT ...] AS SELECT *
FROM (<body>) AS named_columns(<list>)`, and the reviewed SQL and the
catalog body are the same query. The structure job creates views from the
plan's SQL, so it inherits the form.
"""
import json

from snowflake_source.dialect.views import extract_view_columns
from target.ddl import _view_text, build_create_view, build_ddl_payload
from test_data_migration_scripts import _inject_spark, _load
from test_structure_views import _ViewSpark
from test_view_column_list import RENAMING, _view

_WRAPPED = ("SELECT * FROM (\nselect CUST_ID, SUM(AMT) from DB.S.ORDERS "
            "group by 1\n) AS named_columns(`CUSTOMER`, `TOTAL`)")


def _payload(record):
    inv = {"inventory": [record]}
    plan = {"target_names": {"DB.S.V_CUST_TOTALS": "lake.db_s.v_cust_totals"},
            "waves": [["DB.S.V_CUST_TOTALS"]],
            "clone_targets": ["DB.S.V_CUST_TOTALS"]}
    (stmt,) = build_ddl_payload(inv, plan)["statements"]
    return stmt


def test_the_create_view_aliases_the_body_instead_of_a_column_list():
    res = build_create_view(_view(RENAMING), "lake.db_s.v_cust_totals")
    assert res.blocked is False, res.blocked_reason
    assert res.sql == ("CREATE VIEW IF NOT EXISTS `lake`.`db_s`.`v_cust_totals`"
                       " AS " + _WRAPPED), res.sql
    assert extract_view_columns(res.sql) == [], \
        "no view column list: AIDP cannot read a view created with one"


def test_the_comment_stays_in_the_header():
    record = _view(RENAMING)
    record["source_metadata"] = {"comment": "per customer"}
    res = build_create_view(record, "lake.db_s.v_cust_totals")
    assert res.sql == ("CREATE VIEW IF NOT EXISTS `lake`.`db_s`.`v_cust_totals`"
                       " COMMENT 'per customer' AS " + _WRAPPED), res.sql


def test_the_reviewed_sql_and_the_catalog_body_are_the_same_query():
    stmt = _payload(_view(RENAMING))
    assert stmt["view_text"] == _WRAPPED
    assert stmt["sql"].endswith(" AS " + stmt["view_text"])
    assert _view_text(stmt["sql"]) == stmt["view_text"]


def test_a_non_renaming_list_is_carried_the_same_way():
    """It read fine live either way; one form for every list keeps the two
    paths from diverging on a case nobody can tell apart in review."""
    ddl = ("create or replace view V_CUST_TOTALS(CUST_ID, N) as "
           "select CUST_ID, N from DB.S.ORDERS")
    res = build_create_view(_view(ddl), "lake.db_s.v_cust_totals")
    assert res.sql.endswith(" AS SELECT * FROM (\nselect CUST_ID, N from "
                            "DB.S.ORDERS\n) AS named_columns(`CUST_ID`, `N`)")


def test_a_view_without_a_list_is_unchanged():
    res = build_create_view(_view("create view V_CUST_TOTALS as select "
                                  "CUST_ID from DB.S.ORDERS"),
                            "lake.db_s.v_cust_totals")
    assert res.sql == ("CREATE VIEW IF NOT EXISTS `lake`.`db_s`.`v_cust_totals`"
                       " AS\nselect CUST_ID from DB.S.ORDERS")
    assert "named_columns" not in res.sql


def test_r44_says_why_the_list_is_not_a_view_column_list():
    res = build_create_view(_view(RENAMING), "lake.db_s.v_cust_totals")
    (r44,) = [r for r in res.rules_applied
              if r.rule_id == "R44_VIEW_COLUMN_LIST"]
    assert "named_columns" in r44.detail
    assert "INCOMPATIBLE_VIEW_SCHEMA_CHANGE" in r44.detail
    assert "live" in r44.detail


def test_the_structure_job_creates_the_view_without_a_column_list(
        monkeypatch, tmp_path):
    stmt = _payload(_view(RENAMING))
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "S", "tables": [], "views": [{"name": "V_CUST_TOTALS"}],
         "errors": []}]}), encoding="utf-8")
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(
        json.dumps({"statements": [stmt]}), encoding="utf-8")
    spark = _ViewSpark()
    _inject_spark(monkeypatch, spark)
    _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "S", "--reports-dir",
         str(reports), "--output-dir", ""])
    (created,) = [s for s in spark.statements
                  if s.upper().startswith("CREATE VIEW")]
    assert created.startswith(
        "CREATE VIEW IF NOT EXISTS `lake`.`db_s`.`v_cust_totals` AS SELECT * "
        "FROM ( select CUST_ID"), created
    assert created.endswith(") AS named_columns(`CUSTOMER`, `TOTAL`)")
