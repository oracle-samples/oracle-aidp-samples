"""A materialized view the plan migrates as a TABLE SNAPSHOT is created,
copied and reconciled as a table by the in-AIDP data plane.

Live 2026-09-29, coverage run on AIDP: the plan carried the Snowflake
materialized view CORE.MV_ORDER_TOTALS as a table snapshot -- a ddl_plan
statement with `object_type` TABLE, `snapshot_of` "materialized view" and
target `...mv_order_totals` -- and nothing in the data plane ever created or
copied it:

* in-AIDP discovery (00_discover_snowflake) files a materialized view under
  the manifest schema's `views` (INFORMATION_SCHEMA's TABLE_TYPE is not
  BASE TABLE);
* 01_create_structure built only the manifest's `tables`, so the planned
  CREATE TABLE never ran -- and the view over it failed
  `[TABLE_OR_VIEW_NOT_FOUND] snowmig_coverage_core.mv_order_totals`;
* 02_copy_schema scoped only names the manifest lists as tables (the filter
  that keeps real views out of the copy), so its rows were never read;
* 03_reconcile listed it as a view.

A dynamic table snapshot worked, only because discovery files it under
`tables`. The plan is the reviewed artifact: its TABLE statements decide
what is a table, whatever list discovery filed the source under -- or when
discovery did not list it at all. A real view is still never copied.

The ddl stage also left the snapshot's per-column read spec (`columns`)
and Delta features off the statement, because it asked the SOURCE record's
kind (VIEW) rather than the statement's (TABLE); the copy would then have
read it bare, with the connector's lossy typing.
"""
import decimal
import json

import pytest

from fake_pushdown import FakeLakeSpark, FakeSnowflake
from test_data_migration_scripts import _CatalogSpark, _inject_spark, _load
from test_structure_views import _ViewSpark

D = decimal.Decimal
MV = "MV_ORDER_TOTALS"

_ORDERS_EXPECTED = [{"name": "CUST_ID", "type": "DECIMAL(38,0)"},
                    {"name": "AMT", "type": "DECIMAL(18,2)"}]
_MV_EXPECTED = [{"name": "CUSTOMER_ID", "type": "DECIMAL(38,0)"},
                {"name": "TOTAL", "type": "DECIMAL(38,2)"}]


def _k1(expected):
    """The column spec, as the ddl stage writes it for a NUMBER column."""
    return [{"name": c["name"], "target_type": c["type"],
             "read_expr": f'"{c["name"]}"::VARCHAR',
             "convert_expr": f'CAST(`{c["name"]}` AS {c["type"]})'}
            for c in expected]


def _statements():
    """The ddl_plan the coverage run approved, reduced to CORE."""
    return [
        {"source_identifier": "SNOWMIG_DB.CORE.ORDERS", "object_type": "TABLE",
         "target_fqn": "lake.core.ORDERS",
         "expected_columns": _ORDERS_EXPECTED,
         "columns": _k1(_ORDERS_EXPECTED), "delta_features": {}},
        {"source_identifier": f"SNOWMIG_DB.CORE.{MV}", "object_type": "TABLE",
         "snapshot_of": "materialized view",
         "target_fqn": f"lake.core.{MV}",
         "expected_columns": _MV_EXPECTED,
         "columns": _k1(_MV_EXPECTED), "delta_features": {}},
        {"source_identifier": "SNOWMIG_DB.CORE.V_TOP", "object_type": "VIEW",
         "target_fqn": "lake.core.V_TOP",
         "sql": ("CREATE VIEW IF NOT EXISTS `lake`.`core`.`V_TOP` AS SELECT "
                 f"CUSTOMER_ID, TOTAL FROM `lake`.`core`.`{MV}`")},
    ]


def _estate(tmp_path, *, mv_listed_as="views"):
    """The manifest as in-AIDP discovery wrote it: the materialized view
    under `views` (or, `mv_listed_as=None`, not listed at all)."""
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    views = [{"name": "V_TOP"}]
    if mv_listed_as == "views":
        views.insert(0, {"name": MV})
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "CORE", "tables": [{"name": "ORDERS", "columns": []}],
         "views": views, "errors": []}]}), encoding="utf-8")
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(
        json.dumps({"statements": _statements()}), encoding="utf-8")
    config = tmp_path / "source.json"
    config.write_text(json.dumps({"snowflake": {
        "account": "acct", "warehouse": "WH", "database": "SNOWMIG_DB",
        "user": "svc", "auth": "password", "password": "p",
        "schema": "CORE"}}), encoding="utf-8")
    return reports, config


def _source_tables():
    return {
        ("SNOWMIG_DB", "CORE", "ORDERS"): {
            "columns": [("CUST_ID", "NUMBER", 38, 0), ("AMT", "NUMBER", 18, 2)],
            "rows": [{"CUST_ID": D(1), "AMT": D("10.50")},
                     {"CUST_ID": D(1), "AMT": D("2.25")},
                     {"CUST_ID": D(2), "AMT": D("7.00")}]},
        # INFORMATION_SCHEMA.COLUMNS lists a materialized view's columns.
        ("SNOWMIG_DB", "CORE", MV): {
            "columns": [("CUSTOMER_ID", "NUMBER", 38, 0),
                        ("TOTAL", "NUMBER", 38, 2)],
            "rows": [{"CUSTOMER_ID": D(1), "TOTAL": D("12.75")},
                     {"CUSTOMER_ID": D(2), "TOTAL": D("7.00")}]},
    }


def _structure(monkeypatch, reports, spark):
    _inject_spark(monkeypatch, spark)
    return _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "CORE", "--reports-dir",
         str(reports), "--output-dir", ""])


def _copy(monkeypatch, reports, config, spark, *argv):
    _inject_spark(monkeypatch, spark)
    return _load("02_copy_schema").main(
        ["--target-catalog", "lake", "--schema", "CORE", "--reports-dir",
         str(reports), "--source-config", str(config), "--output-dir", "",
         "--retries", "0", *argv])


def _report(reports, name):
    return json.loads((reports / name).read_text(encoding="utf-8"))


def _tables_only(catalog, views):
    return {k: v for k, v in catalog.items() if k not in views}


# ------------------------------------------------------ 01 creates it

@pytest.mark.parametrize("listed", ["views", None])
def test_the_structure_job_creates_the_snapshot_as_a_table(
        monkeypatch, tmp_path, listed):
    reports, _config = _estate(tmp_path, mv_listed_as=listed)
    spark = _ViewSpark()
    rc = _structure(monkeypatch, reports, spark)
    report = _report(reports, "structure_report_core.json")
    assert rc == 0, report
    fqn = f"`lake`.`core`.`{MV}`"
    assert fqn in spark.catalog and fqn not in spark.views, \
        "created as a TABLE, from the plan's columns"
    assert spark.catalog[fqn] == [("CUSTOMER_ID", "decimal(38,0)"),
                                  ("TOTAL", "decimal(38,2)")]
    entry = report["objects"][MV]
    assert entry["status"] == "created"
    assert entry["kind"] == "TABLE"
    assert entry["snapshot_of"] == "materialized view"
    assert entry["target_fqn"] == f"lake.core.{MV}"
    assert MV not in report.get("views", {}), \
        "never also listed as a view the plan does not carry"
    # The view over it now finds it (live: TABLE_OR_VIEW_NOT_FOUND).
    assert report["views"]["V_TOP"]["status"] == "created"


def test_a_snapshot_there_with_another_layout_is_type_drift(monkeypatch,
                                                            tmp_path):
    reports, _config = _estate(tmp_path)
    spark = _ViewSpark({f"`lake`.`core`.`{MV}`": [
        ("TOTAL", "decimal(38,2)"), ("CUSTOMER_ID", "decimal(38,0)")]})
    rc = _structure(monkeypatch, reports, spark)
    entry = _report(reports, "structure_report_core.json")["objects"][MV]
    assert rc == 1
    assert entry["status"] == "type_drift"


# ----------------------------------------------- 02 copies it, exactly

# What 01 records for CORE, and the tables it creates -- so 02 and 03 are
# held to the snapshot on their own (`via="report"`), as well as behind the
# real stage (`via="stage"`).
_STRUCTURE_REPORT = {
    "schema": "CORE", "target": "lake.core", "mode": "ddl-plan",
    "objects": {
        "ORDERS": {"status": "created", "mode": "ddl-plan",
                   "target_fqn": "lake.core.ORDERS"},
        MV: {"status": "created", "mode": "ddl-plan",
             "target_fqn": f"lake.core.{MV}", "kind": "TABLE",
             "snapshot_of": "materialized view"}},
    "views": {"V_TOP": {"status": "created", "in_plan": True,
                        "target_fqn": "lake.core.V_TOP"}}}
_CREATED = {"`lake`.`core`.`ORDERS`": [("CUST_ID", "decimal(38,0)"),
                                       ("AMT", "decimal(18,2)")],
            f"`lake`.`core`.`{MV}`": [("CUSTOMER_ID", "decimal(38,0)"),
                                      ("TOTAL", "decimal(38,2)")]}


def _structured(monkeypatch, tmp_path, *, listed="views", via="stage"):
    reports, config = _estate(tmp_path, mv_listed_as=listed)
    if via == "report":
        (reports / "structure_report_core.json").write_text(
            json.dumps(_STRUCTURE_REPORT), encoding="utf-8")
        tables = _CREATED
    else:
        structure = _ViewSpark()
        assert _structure(monkeypatch, reports, structure) == 0
        tables = _tables_only(structure.catalog, structure.views)
        assert tables == _CREATED
    lake = FakeLakeSpark(FakeSnowflake(_source_tables()), tables)
    return reports, config, lake


@pytest.mark.parametrize("via", ["report", "stage"])
@pytest.mark.parametrize("listed", ["views", None])
def test_the_copy_scopes_and_copies_the_snapshot(monkeypatch, tmp_path,
                                                 listed, via):
    reports, config, spark = _structured(monkeypatch, tmp_path, listed=listed,
                                         via=via)
    rc = _copy(monkeypatch, reports, config, spark)
    report = _report(reports, "copy_report_core.json")
    assert rc == 0, report
    assert sorted(report["tables"]) == [MV, "ORDERS"], \
        "the snapshot is in scope; the real view V_TOP never is"
    rec = report["tables"][MV]
    assert rec["status"] == "verified"
    assert rec["read"]["from_plan"] == ["CUSTOMER_ID", "TOTAL"], \
        "read with the plan's column spec, not bare"
    assert spark.rows[f"`lake`.`core`.`{MV}`"] == [
        {"CUSTOMER_ID": D(1), "TOTAL": D("12.75")},
        {"CUSTOMER_ID": D(2), "TOTAL": D("7.00")}]
    reads = [s for s in spark.pushdowns if f'"{MV}"' in s
             and "INFORMATION_SCHEMA" not in s and "SNOWMIG_TABLE" not in s]
    assert reads == ['select "CUSTOMER_ID"::VARCHAR as "CUSTOMER_ID", '
                     '"TOTAL"::VARCHAR as "TOTAL" from '
                     f'"SNOWMIG_DB"."CORE"."{MV}"']
    assert not any("V_TOP" in s for s in spark.statements
                   if s.startswith("INSERT"))


def test_without_a_structure_report_the_snapshot_is_still_in_scope(
        monkeypatch, tmp_path):
    reports, config, spark = _structured(monkeypatch, tmp_path, via="report")
    (reports / "structure_report_core.json").unlink()
    rc = _copy(monkeypatch, reports, config, spark)
    report = _report(reports, "copy_report_core.json")
    assert rc == 0, report
    assert sorted(report["tables"]) == [MV, "ORDERS"]


def test_a_plain_view_the_manifest_lists_is_still_never_copied(
        monkeypatch, tmp_path):
    """The widening is the plan's snapshots only: V_TOP is a VIEW
    statement, so the structure report keeps it under `views` and the copy
    never takes it for an INSERT target."""
    reports, config, spark = _structured(monkeypatch, tmp_path)
    structure = _report(reports, "structure_report_core.json")
    assert "V_TOP" not in structure["objects"]
    assert _copy(monkeypatch, reports, config, spark) == 0
    assert "V_TOP" not in _report(reports, "copy_report_core.json")["tables"]


# --------------------------------------------- 03 reports it as a table

@pytest.mark.parametrize("via", ["report", "stage"])
def test_reconcile_reports_the_snapshot_as_a_verified_table(monkeypatch,
                                                            tmp_path, via):
    reports, config, spark = _structured(monkeypatch, tmp_path, via=via)
    if via == "report":
        # The copy's record as 02 writes it, so 03 is held on its own too.
        (reports / "copy_report_core.json").write_text(json.dumps({
            "schema": "CORE", "target": "lake.core", "tables": {
                "ORDERS": {"status": "verified", "target_count": 3},
                MV: {"status": "verified", "target_count": 2}}}),
            encoding="utf-8")
        spark.rows[f"`lake`.`core`.`{MV}`"] = [{}, {}]
        spark.rows["`lake`.`core`.`ORDERS`"] = [{}, {}, {}]
    else:
        assert _copy(monkeypatch, reports, config, spark) == 0
    catalog = _CatalogSpark({**spark.lake,
                             "`lake`.`core`.`V_TOP`": [("CUSTOMER_ID", "x")]})
    catalog.counts = {fqn: len(rows) for fqn, rows in spark.rows.items()}
    _inject_spark(monkeypatch, catalog)
    rc = _load("03_reconcile").main(["--target-catalog", "lake",
                                     "--reports-dir", str(reports),
                                     "--output-dir", "", "--counts"])
    rec = _report(reports, "reconciliation.json")
    assert rc == 0, rec
    schema = rec["schemas"][0]
    tables = {t["table"]: t for t in schema["tables"]}
    assert tables[MV]["verdict"] == "MIGRATED_VERIFIED"
    assert tables[MV]["target_count"] == 2
    assert [v["view"] for v in schema["views"]] == ["V_TOP"], \
        "the snapshot is not also a view row"
    assert MV.lower() not in schema["in_target_but_not_in_manifest"]
    assert f"| {MV} (2 rows) | created | verified |" in \
        (reports / "MIGRATION_REPORT.md").read_text(encoding="utf-8")


# -------------------------------- the ddl stage carries its column spec

def test_the_ddl_statement_for_a_materialized_view_carries_its_read_spec():
    from plan.build import build_plan
    from target.ddl import build_ddl_payload
    from test_plan_snapshots import ORDERS, _inv, _mv, _rec

    inv = _inv(_rec(ORDERS), _mv())
    payload = build_ddl_payload(inv, build_plan(inv, {"edges": []}))
    stmt = next(s for s in payload["statements"]
                if s["source_identifier"] == "DB.CORE.MV_ORDER_TOTALS")
    assert stmt["object_type"] == "TABLE"
    assert stmt["columns"] == [{
        "name": "CUSTOMER_ID", "source_type": "number",
        "target_type": "DECIMAL(38,0)",
        "read_expr": '"CUSTOMER_ID"::VARCHAR',
        "convert_expr": "CAST(`CUSTOMER_ID` AS DECIMAL(38,0))"}]
    assert "delta_features" in stmt and "carried_properties" in stmt
