"""A source setting AIDP's Delta can express is CARRIED into the CREATE
TABLE, not reported as a decision for later.

Until now `ddl` listed every clustering key, retention period and change
tracking flag as "deferred -- NOT applied", because nothing had shown the
target accepts them. Probe 1 (live 2026-09-29, AIDP Spark 3.5.0 / Delta
3.1.0) did: `CLUSTER BY` (liquid clustering), the retention TBLPROPERTIES
and `delta.enableChangeDataFeed = true` all created, and `table_changes`
read the feed back. So:

  * `cluster_by` `LINEAR(SEGMENT)` (the live SHOW TABLES shape) ->
    `CLUSTER BY (SEGMENT)`, when every key is a plain column Delta can
    cluster on: at most four, with statistics (first 32 columns, a type
    that has min/max). An expression key stays deferred, with the reason.
  * `retention_time` N days -> `delta.deletedFileRetentionDuration` /
    `delta.logRetentionDuration`, raised to N only above Delta's own 7 /
    30 day defaults; below them the defaults already reach further back
    than the source did, and lowering them would trip VACUUM's safety
    check. Nothing narrows.
  * `change_tracking = ON`, or a STREAM on the table (census) ->
    `delta.enableChangeDataFeed = true`.

The catalog-API deploy path has no field for any of it, and says so.
"""
import importlib.util
import pathlib
import sys

import pytest

from fake_sql import FakeSql
from plan.build import _maintenance_facts
from report.render import render_ddl_plan, render_maintenance
from snowflake_source.extract.census import build_census
from snowflake_source.extract.maintenance import build_maintenance
from target import ddl
from target.ddl import build_create_table, build_ddl_payload
from test_census_coverage import _responses

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _col(name, target, pos, dt="TEXT"):
    return {"COLUMN_NAME": name, "DATA_TYPE": dt, "target_type": target,
            "ORDINAL_POSITION": pos, "IS_NULLABLE": "YES", "COMMENT": None}


def _table(meta, columns=None, ident="SNOWMIG_COVERAGE.CORE.CUSTOMERS"):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": "TABLE",
            "source_database": db, "source_schema": schema,
            "compatibility_status": "supported", "source_metadata": meta,
            "columns": columns or [
                _col("ID", "DECIMAL(38,0)", 1, "NUMBER"),
                _col("SEGMENT", "STRING", 2),
                _col("PAYLOAD", "ARRAY<FLOAT>", 3, "VECTOR"),
                _col("ACTIVE", "BOOLEAN", 4, "BOOLEAN")]}


def _carried(res):
    return {c["property"]: c for c in res.carried_properties}


def _deferred(res):
    return {d["property"]: d for d in res.deferred_properties}


# ---------------------------------------------------------- clustering

def test_a_live_clustering_key_becomes_liquid_clustering():
    # CUSTOMERS on the trial: cluster_by "LINEAR(SEGMENT)", automatic ON.
    res = build_create_table(_table({"cluster_by": "LINEAR(SEGMENT)",
                                     "automatic_clustering": "ON"}),
                             "cat.core.customers")
    assert "\nCLUSTER BY (SEGMENT)" in res.sql
    assert res.delta_features["cluster_by"] == ["SEGMENT"]
    assert "cluster_by" in _carried(res)
    assert "cluster_by" not in _deferred(res)
    assert any(r.rule_id == "R12_DELTA_FEATURES_CARRIED"
               for r in res.rules_applied)


def test_two_plain_keys_keep_their_order():
    res = build_create_table(_table({"cluster_by": "LINEAR(SEGMENT, ID)"}),
                             "cat.core.customers")
    assert res.delta_features["cluster_by"] == ["SEGMENT", "ID"]


@pytest.mark.parametrize("cluster_by,why", [
    ("LINEAR(TO_DATE(SEGMENT), ID)", "expression"),
    ("LINEAR(A, B, C, D, E)", "at most 4"),
    ("LINEAR(PAYLOAD)", "ARRAY<FLOAT>"),
    ("LINEAR(ACTIVE)", "BOOLEAN"),
    ("LINEAR(NOT_A_COLUMN)", "not a column"),
])
def test_a_key_delta_cannot_cluster_on_stays_deferred_with_the_reason(
        cluster_by, why):
    cols = [_col(n, "STRING", i + 1) for i, n in enumerate("ABCDE")] \
        if cluster_by.startswith("LINEAR(A, B") else None
    res = build_create_table(_table({"cluster_by": cluster_by}, cols),
                             "cat.core.customers")
    assert "CLUSTER BY" not in res.sql
    deferred = _deferred(res)["cluster_by"]
    assert why in deferred["reason"], deferred["reason"]
    assert any(r.rule_id == "R11_MAINTENANCE_DEFERRED"
               for r in res.rules_applied)


def test_a_key_past_the_statistics_columns_is_deferred():
    cols = [_col(f"C{i}", "STRING", i) for i in range(1, 41)]
    res = build_create_table(_table({"cluster_by": "LINEAR(C35)"}, cols),
                             "cat.core.customers")
    assert "CLUSTER BY" not in res.sql
    assert "32" in _deferred(res)["cluster_by"]["reason"]


# ------------------------------------------------------------ retention

def test_a_long_retention_raises_both_delta_windows():
    res = build_create_table(_table({"retention_time": "90"}),
                             "cat.core.customers")
    props = res.delta_features["tblproperties"]
    assert props == {"delta.deletedFileRetentionDuration": "interval 90 days",
                     "delta.logRetentionDuration": "interval 90 days"}
    assert "TBLPROPERTIES ('delta.deletedFileRetentionDuration' = " \
           "'interval 90 days', 'delta.logRetentionDuration' = " \
           "'interval 90 days')" in res.sql


def test_a_retention_between_the_defaults_raises_only_the_file_window():
    res = build_create_table(_table({"retention_time": 14}),
                             "cat.core.customers")
    assert res.delta_features["tblproperties"] == {
        "delta.deletedFileRetentionDuration": "interval 14 days"}


@pytest.mark.parametrize("days", ["1", "0", "7"])
def test_a_short_retention_is_met_by_the_delta_defaults_not_lowered(days):
    # The trial's default is 1 day. Delta keeps 7 days of files and 30 of
    # log by default: the target already reaches further back.
    res = build_create_table(_table({"retention_time": days}),
                             "cat.core.customers")
    assert "TBLPROPERTIES" not in res.sql
    carried = _carried(res)["retention_time"]
    assert "default" in carried["carried_as"]
    assert "retention_time" not in _deferred(res)


# ------------------------------------------------------ change data feed

def test_change_tracking_turns_on_the_change_data_feed():
    # ORDERS on the trial: change_tracking ON (a stream reads it).
    res = build_create_table(_table({"change_tracking": "ON"}),
                             "cat.core.orders")
    assert res.delta_features["tblproperties"] == {
        "delta.enableChangeDataFeed": "true"}
    assert "change_tracking" in _carried(res)


def test_a_stream_on_the_table_turns_it_on_too():
    res = build_create_table(_table({"change_tracking": "OFF"}),
                             "cat.core.orders",
                             streams_on=["SNOWMIG_COVERAGE.CORE.STR_ORDERS"])
    assert res.delta_features["tblproperties"] == {
        "delta.enableChangeDataFeed": "true"}
    carried = _carried(res)["stream"]
    assert "STR_ORDERS" in carried["value"]
    assert "offsets" in carried["carried_as"]


def test_the_census_records_which_table_a_stream_reads():
    row = {"name": "STR_ORDERS", "database_name": "SNOWMIG_COVERAGE",
           "schema_name": "CORE", "table_name": "SNOWMIG_COVERAGE.CORE.ORDERS",
           "source_type": "Table", "base_tables": "SNOWMIG_COVERAGE.CORE.ORDERS",
           "type": "DELTA", "stale": "false", "mode": "DEFAULT"}
    census = build_census(FakeSql(_responses(**{"show streams": [row]})),
                          ["SNOWMIG_COVERAGE"])
    stream = next(o for o in census["objects"] if o["kind"] == "STREAM")
    assert stream["on_table"] == "SNOWMIG_COVERAGE.CORE.ORDERS"


def test_the_payload_joins_streams_to_their_tables():
    inv = {"inventory": [_table({}, ident="SNOWMIG_COVERAGE.CORE.ORDERS")],
           "census": {"objects": [
               {"kind": "STREAM",
                "source_identifier": "SNOWMIG_COVERAGE.CORE.STR_ORDERS",
                "on_table": "SNOWMIG_COVERAGE.CORE.ORDERS"}]}}
    ident = "SNOWMIG_COVERAGE.CORE.ORDERS"
    payload = build_ddl_payload(inv, {"waves": [[ident]],
                                      "clone_targets": [ident],
                                      "target_names": {ident: "c.core.orders"}})
    stmt = payload["statements"][0]
    assert stmt["delta_features"]["tblproperties"] == {
        "delta.enableChangeDataFeed": "true"}
    assert stmt["carried_properties"]


# ------------------------------------------- the path that cannot carry it

def test_the_catalog_api_gap_is_named_per_table():
    res = build_create_table(_table({"change_tracking": "ON",
                                     "cluster_by": "LINEAR(SEGMENT)"}),
                             "cat.core.orders")
    gap = next(r for r in res.rules_applied
               if r.rule_id == "R13_DELTA_FEATURES_CATALOG_API_GAP")
    assert "deploy --execute" in gap.detail and "ALTER TABLE" in gap.detail


# R13 lives in DDL_PLAN.md, which the operator read BEFORE deploying. The
# deploy's own result is what they read AFTER, and it used to say "verified"
# and nothing else: the reviewed CREATE's clustering, change data feed and
# retention were dropped by the catalog-API body without a word on that
# path. A stream consumer relying on the feed would find none. So the
# deploy names each feature per object, beside the NOT NULL it already
# names, in `properties_not_applied`.

def _deploy_plan(features, *, nullable=True):
    return {"statements": [{
        "source_identifier": "DB.CORE.ORDERS", "object_type": "TABLE",
        "target_fqn": "lake.DB.ORDERS", "sql": "CREATE TABLE ...",
        # The shape the fake transport reads back, so structure verifies.
        "expected_columns": [{"name": "A", "type": "STRING",
                              "nullable": nullable}],
        "delta_features": features}], "blocked": []}


def _deploy(plan, **recorder_kw):
    from target.catalog_deploy import deploy_catalog
    from test_catalog_deploy import TARGET, Recorder
    return deploy_catalog(plan, target=TARGET, execute=True,
                          call=Recorder(**recorder_kw), retry_delays=(),
                          verify_delays=())


_ALL_FEATURES = {
    "cluster_by": ["A"],
    "tblproperties": {"delta.deletedFileRetentionDuration": "interval 90 days",
                      "delta.enableChangeDataFeed": "true",
                      "delta.logRetentionDuration": "interval 90 days"}}


def test_the_catalog_deploy_names_every_delta_feature_it_did_not_apply():
    out = _deploy(_deploy_plan(_ALL_FEATURES))
    # Still created and verified for structure: the gap is in the settings.
    assert out["verified"] == 1
    gaps = {p["property"]: p for p in out["properties_not_applied"]}
    assert set(gaps) == {
        "CLUSTER BY",
        "TBLPROPERTIES delta.deletedFileRetentionDuration",
        "TBLPROPERTIES delta.enableChangeDataFeed",
        "TBLPROPERTIES delta.logRetentionDuration"}
    assert gaps["CLUSTER BY"]["columns"] == ["A"]
    assert "CLUSTER BY (A)" in gaps["CLUSTER BY"]["reason"]
    cdf = gaps["TBLPROPERTIES delta.enableChangeDataFeed"]
    assert cdf["value"] == "true"
    # It says what to do about it, not only that it happened.
    assert "SET TBLPROPERTIES" in cdf["reason"]
    assert "01_create_structure" in cdf["reason"]
    assert out["properties_not_applied_targets"] == ["DB.CORE.ORDERS"]


def test_a_not_null_and_a_feature_gap_name_the_object_once():
    out = _deploy(_deploy_plan({"cluster_by": ["A"]}, nullable=False))
    assert [p["property"] for p in out["properties_not_applied"]] == [
        "NOT NULL", "CLUSTER BY"]
    assert out["properties_not_applied_targets"] == ["DB.CORE.ORDERS"]


def test_no_feature_gap_for_a_table_that_carries_none():
    out = _deploy(_deploy_plan({}))
    assert out["properties_not_applied"] == []


def test_no_feature_gap_for_a_table_that_failed_to_create():
    out = _deploy(_deploy_plan(_ALL_FEATURES), fail_on=("ORDERS",))
    assert out["failed"], "the create must have failed for this to mean anything"
    assert out["properties_not_applied"] == []


def test_the_deploy_summary_cites_the_plan_rule_for_the_features():
    from report.render import render_soft_clone_summary
    out = _deploy(_deploy_plan(_ALL_FEATURES))
    md = render_soft_clone_summary({"can_migrate": [], "blocked": []}, out)
    assert "`lake.DB.ORDERS` — TBLPROPERTIES delta.enableChangeDataFeed" in md
    assert "R13" in md


def test_a_table_with_nothing_to_carry_is_unchanged():
    res = build_create_table(_table({}), "cat.core.customers")
    assert res.delta_features == {}
    assert "CLUSTER BY" not in res.sql and "TBLPROPERTIES" not in res.sql
    assert not any(r.rule_id.startswith(("R12_", "R13_"))
                   for r in res.rules_applied)


# ----------------------------------------------------------- the reports

def test_ddl_plan_says_what_was_carried_and_what_still_waits():
    inv = {"inventory": [_table({"cluster_by": "LINEAR(TO_DATE(SEGMENT))",
                                 "change_tracking": "ON"})]}
    ident = "SNOWMIG_COVERAGE.CORE.CUSTOMERS"
    md = render_ddl_plan(build_ddl_payload(
        inv, {"waves": [[ident]], "clone_targets": [ident],
              "target_names": {ident: "c.core.customers"}}))
    assert "Carried into the CREATE TABLE" in md
    assert "delta.enableChangeDataFeed" in md
    assert "`snowmig deploy --execute`" in md
    waiting = md.split("## Maintenance and layout")[1]
    assert "cluster_by" in waiting and "change_tracking" not in waiting


def test_the_plan_summary_no_longer_counts_a_carried_setting_as_deferred():
    deferred, _ = _maintenance_facts(_table({"change_tracking": "ON",
                                             "cluster_by": "LINEAR(SEGMENT)"}))
    assert deferred == []
    deferred, _ = _maintenance_facts(_table({"cluster_by": "LINEAR(TO_DATE(X))"}))
    assert [d["property"] for d in deferred] == ["cluster_by"]


def test_the_maintenance_report_says_ddl_now_carries_the_settings():
    inv = {"databases_in_scope": ["D"],
           "inventory": [_table({"cluster_by": "LINEAR(SEGMENT)",
                                 "automatic_clustering": "ON",
                                 "change_tracking": "ON",
                                 "retention_time": 1},
                                ident="D.S.T")]}
    maint = build_maintenance(FakeSql({"show parameters": [],
                                       "account_usage": []}), inv)
    md = render_maintenance(maint)
    assert "carried into the CREATE TABLE by `ddl`" in md
    assert "deploy --execute" in md
    # Scheduling is still the customer's: nothing runs OPTIMIZE for them.
    assert "OPTIMIZE" in md


# ------------------------------------------- the in-AIDP structure stage

@pytest.fixture(scope="module")
def structure():
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        "snowmig_script_01_delta_features", SCRIPTS / "01_create_structure.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _DF:
    def __init__(self, rows=None):
        self._rows = rows or []

    def collect(self):
        return self._rows


class _Row(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key)


class _Spark:
    def __init__(self, exists=False, props=None):
        self.statements: list[str] = []
        self._exists = exists
        self._props = props

    def sql(self, statement):
        s = " ".join(statement.split())
        self.statements.append(s)
        upper = s.upper()
        if upper.startswith("DESCRIBE "):
            if not self._exists:
                raise RuntimeError("Table or view not found")
            return _DF([_Row(col_name="ID", data_type="decimal(38,0)",
                             comment=None)])
        if upper.startswith("SHOW TBLPROPERTIES"):
            return _DF([_Row(key=k, value=v)
                        for k, v in (self._props or {}).items()])
        if upper.startswith("CREATE TABLE"):
            self._exists = True
        return _DF()


_FEATURES = {"cluster_by": ["ID"],
             "tblproperties": {"delta.enableChangeDataFeed": "true"}}
_COLUMNS = [{"name": "ID", "type": "DECIMAL(38,0)", "nullable": True}]


def test_the_structure_stage_renders_the_same_clauses_as_ddl(structure):
    assert structure._cluster_by_sql(_FEATURES) == ddl.render_cluster_by(_FEATURES)
    assert structure._tblproperties_sql(_FEATURES) == \
        ddl.render_tblproperties(_FEATURES)


def test_the_structure_stage_creates_with_the_clauses_and_reads_them_back(
        structure):
    spark = _Spark(props={"delta.enableChangeDataFeed": "true"})
    notes: list[str] = []
    status = structure.create_table_from_columns(
        spark, _COLUMNS, "cat", "core", "orders", notes=notes,
        features=_FEATURES)
    assert status == "created"
    create = next(s for s in spark.statements if s.startswith("CREATE TABLE"))
    assert create.endswith("USING DELTA CLUSTER BY (ID) TBLPROPERTIES "
                           "('delta.enableChangeDataFeed' = 'true')")
    assert any(s.startswith("SHOW TBLPROPERTIES") for s in spark.statements)
    # Applied and read back: the only note is the one about clustering,
    # which DESCRIBE cannot show.
    assert notes == [n for n in notes if "CLUSTER BY" in n]


def test_a_property_that_did_not_arrive_is_reported_not_assumed(structure):
    spark = _Spark(props={})
    notes: list[str] = []
    structure.create_table_from_columns(
        spark, _COLUMNS, "cat", "core", "orders", notes=notes,
        features=_FEATURES)
    assert any("delta.enableChangeDataFeed" in n and "not found" in n
               for n in notes), notes


def test_a_table_that_was_already_there_is_not_claimed_to_carry_them(
        structure):
    spark = _Spark(exists=True, props={})
    notes: list[str] = []
    status = structure.create_table_from_columns(
        spark, _COLUMNS, "cat", "core", "orders", notes=notes,
        features=_FEATURES)
    assert status == "already_existed"
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements)
    assert any("already there" in n for n in notes), notes


def test_the_structure_stage_reads_the_features_from_the_plan(structure):
    plan = {"statements": [{"source_identifier": "D.CORE.ORDERS",
                            "object_type": "TABLE",
                            "target_fqn": "cat.core.orders",
                            "delta_features": _FEATURES}]}
    assert structure.features_from_ddl_plan(plan) == {
        ("CORE", "ORDERS"): _FEATURES}
