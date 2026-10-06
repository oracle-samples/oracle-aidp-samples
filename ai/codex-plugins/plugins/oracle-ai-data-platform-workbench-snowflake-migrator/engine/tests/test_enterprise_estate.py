"""The ENTERPRISE emulation: the Snowflake objects a trial account cannot hold.

A trial account has no external volume, no catalog integration, no hybrid
tables, no replication, no second account to share with and no Snowpark
Container Services. Every one of those is common in a real estate, and every
one of them changes what a migration can promise. Without a source that
holds them, the pipeline's handling of them was only ever read, never run.

The estate is only worth having if it answers exactly as Snowflake does:

  * the fields of every row that DOES exist in a trial are the ones a live
    trial returned on 2026-09-29 (tests/fixtures/snowflake_live_field_names.json,
    names only). A fake whose SHOW TABLES row lacks `is_hybrid` would let the
    planner pass a test the real row would fail;
  * the rest follow Snowflake's documented output and say so in the module;
  * it answers every read the pipeline issues and REFUSES anything else --
    an improvised answer is how a fake starts lying -- and it refuses a write
    through the same guard the real transport uses (invariant I1).
"""
import json
import pathlib

import pytest

from emulation.snowflake_fake import ENTERPRISE_DB, enterprise_run_sql
from snowflake_source.conn import SourceWriteRefused, assert_read_only
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.census import build_census
from snowflake_source.extract.dependencies import extract_dependencies
from snowflake_source.extract.maintenance import build_maintenance
from snowflake_source.extract.security import build_security
from snowflake_source.extract.warehouses import extract_warehouses

_LIVE = json.loads((pathlib.Path(__file__).parent / "fixtures"
                    / "snowflake_live_field_names.json").read_text(encoding="utf-8"))


class _Recorder:
    """The enterprise transport, with every statement kept for inspection."""

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        return enterprise_run_sql(sql, params)


@pytest.fixture(scope="module")
def estate():
    run = _Recorder()
    inv = build_inventory(run, [ENTERPRISE_DB], semi_structured="block",
                          timestamp_ntz="timestamp")
    inv["census"] = build_census(run, inv["databases_in_scope"],
                                 role=inv["session"].get("ROLE"))
    deps = extract_dependencies(run, inv)
    maint = build_maintenance(run, inv)
    sec = build_security(run, inv)
    wh = extract_warehouses(run)
    return {"run": run, "inv": inv, "deps": deps, "maint": maint,
            "sec": sec, "wh": wh}


def _record(estate, ident):
    return next(r for r in estate["inv"]["inventory"]
                if r["source_identifier"] == ident)


def _show(sql):
    return enterprise_run_sql(sql)


# ------------------------------------------------------------ the transport

def test_the_estate_answers_every_read_the_pipeline_issues(estate):
    # A statement the fake cannot answer raises, and every extractor records
    # a raise as a note ("could not be read"). So an empty note list is the
    # proof that assess, census, deps, maintenance and security each got an
    # answer to every question they asked.
    assert estate["inv"]["extraction_notes"] == []
    assert estate["inv"]["census"]["unreadable"] == []
    assert estate["sec"]["unreadable"] == []
    assert estate["maint"]["unreadable"] == []
    assert estate["deps"]["source_used"] == "account_usage"


def test_every_statement_it_answered_is_a_read(estate):
    # I1: the same guard the live connection applies. If the pipeline ever
    # sent a write, the fake would have refused it; this re-checks the log.
    assert estate["run"].calls
    for sql in estate["run"].calls:
        assert_read_only(sql)


def test_a_write_is_refused_by_the_real_guard_not_answered():
    with pytest.raises(SourceWriteRefused):
        enterprise_run_sql('create table "SNOWENT"."SALES"."X" (a int)')
    with pytest.raises(SourceWriteRefused):
        enterprise_run_sql("alter share ENT_PARTNER_SHARE add accounts = X")


def test_an_unrecognised_read_raises_rather_than_improvising():
    with pytest.raises(ValueError, match="no answer"):
        enterprise_run_sql("show pipes in account")


# --------------------------------------------------- live-captured shapes

def test_show_tables_rows_carry_exactly_the_live_field_set():
    # Live SHOW TABLES carries is_external / is_iceberg / is_hybrid / is_event
    # / is_dynamic on EVERY row. The planner reads those flags; a fake row
    # without them would make every exotic table look standard.
    for schema in ("SALES", "LAKE", "OPS"):
        rows = _show(f'show tables in schema "SNOWENT"."{schema}" limit 10000')
        assert rows, schema
        for row in rows:
            assert list(row) == _LIVE["show_tables"], (schema, row["name"])


@pytest.mark.parametrize("statement,shape", [
    ('show materialized views in database "SNOWENT"', "show_materialized_views"),
    ('show streams in database "SNOWENT"', "show_streams"),
])
def test_census_rows_carry_the_live_field_set(statement, shape):
    rows = _show(statement)
    assert rows
    for row in rows:
        assert list(row) == _LIVE[shape], row["name"]


# ----------------------------------------------- the enterprise objects

def test_every_exotic_table_kind_is_present_and_flagged(estate):
    flags = {r["source_identifier"]: r["source_metadata"]
             for r in estate["inv"]["inventory"] if r["object_type"] == "TABLE"}
    assert flags["SNOWENT.LAKE.EXT_CLICKS"]["is_external"] == "Y"
    assert flags["SNOWENT.LAKE.EXT_PARTNER_FEED"]["is_external"] == "Y"
    assert flags["SNOWENT.LAKE.ICE_EVENTS"]["is_iceberg"] == "Y"
    assert flags["SNOWENT.LAKE.ICE_GLUE_ORDERS"]["is_iceberg"] == "Y"
    assert flags["SNOWENT.OPS.HYB_SESSIONS"]["is_hybrid"] == "Y"
    assert flags["SNOWENT.OPS.APP_EVENTS"]["is_event"] == "Y"
    orders = flags["SNOWENT.SALES.ORDERS"]
    assert orders["search_optimization"] == "ON"
    assert orders["cluster_by"] == "LINEAR(ORDER_DATE)"
    # A standard table is a standard table: every flag off.
    assert all(flags["SNOWENT.SALES.CUSTOMERS"][f] == "N" for f in (
        "is_external", "is_iceberg", "is_hybrid", "is_event", "is_dynamic"))


def test_the_event_table_has_snowflakes_fixed_event_schema(estate):
    # An event table's columns are fixed by Snowflake; OBJECT and VARIANT
    # among them, which is what tempts a planner into calling it a type
    # problem rather than the object-kind problem it is.
    cols = [c["COLUMN_NAME"] for c in _record(estate, "SNOWENT.OPS.APP_EVENTS")["columns"]]
    assert cols == ["TIMESTAMP", "START_TIMESTAMP", "OBSERVED_TIMESTAMP",
                    "TRACE", "RESOURCE", "RESOURCE_ATTRIBUTES", "SCOPE",
                    "SCOPE_ATTRIBUTES", "RECORD_TYPE", "RECORD",
                    "RECORD_ATTRIBUTES", "VALUE", "EXEMPLARS"]


def test_the_secure_view_and_the_clustered_materialized_view(estate):
    sv = _record(estate, "SNOWENT.SALES.CUSTOMER_360_SV")
    assert sv["source_metadata"]["is_secure"] == "true"
    assert "SECURE VIEW" in sv["view_ddl_get_ddl"]
    mv = _record(estate, "SNOWENT.SALES.ORDER_TOTALS_MV")
    assert mv["source_metadata"]["is_materialized"] == "true"
    census_mv = _show('show materialized views in database "SNOWENT"')[0]
    assert census_mv["cluster_by"] == "LINEAR(CUSTOMER_ID)"
    assert census_mv["automatic_clustering"] == "ON"


def test_external_tables_sit_on_an_s3_stage_with_a_parquet_file_format():
    ext = {r["name"]: r for r in _show(
        'show external tables in schema "SNOWENT"."LAKE"')}
    clicks = ext["EXT_CLICKS"]
    assert clicks["location"].startswith("s3://")
    assert clicks["file_format_type"] == "PARQUET"
    assert clicks["stage"] == "@SNOWENT.LAKE.S3_LAKE_STAGE"
    assert clicks["cloud"] == "AWS"
    # The stage the census reads is the same stage, with the same URL root.
    stages = _show('select stage_name, stage_schema, stage_type, stage_url, '
                   'stage_region from "SNOWENT".information_schema.stages '
                   'order by 1')
    stage = next(s for s in stages if s["STAGE_NAME"] == "S3_LAKE_STAGE")
    assert stage["STAGE_TYPE"] == "External Named"
    assert clicks["location"].startswith(stage["STAGE_URL"])
    formats = _show('select file_format_name, file_format_schema from '
                    '"SNOWENT".information_schema.file_formats order by 1')
    assert "FF_PARQUET" in {f["FILE_FORMAT_NAME"] for f in formats}


def test_iceberg_tables_name_their_external_volume_and_catalog():
    ice = {r["name"]: r for r in _show(
        'show iceberg tables in schema "SNOWENT"."LAKE"')}
    assert ice["ICE_EVENTS"]["catalog_name"] == "SNOWFLAKE"
    assert ice["ICE_EVENTS"]["iceberg_table_type"] == "MANAGED"
    assert ice["ICE_EVENTS"]["external_volume_name"] == "EV_ENT_LAKE"
    glue = ice["ICE_GLUE_ORDERS"]
    assert glue["iceberg_table_type"] == "UNMANAGED"
    assert glue["catalog_name"] == "GLUE_ENT_CATALOG"
    # DESCRIBE EXTERNAL VOLUME: each storage location is a JSON document in
    # property_value, and ACTIVE names the one in use.
    vol = _show('describe external volume "EV_ENT_LAKE"')
    active = next(r["property_value"] for r in vol if r["property"] == "ACTIVE")
    loc = next(json.loads(r["property_value"]) for r in vol
               if r["property"].startswith("STORAGE_LOCATION_"))
    assert loc["NAME"] == active
    assert loc["STORAGE_PROVIDER"] == "S3"
    assert loc["STORAGE_BASE_URL"].startswith("s3://")
    cat = {r["property"]: r["property_value"] for r in _show(
        'describe catalog integration "GLUE_ENT_CATALOG"')}
    assert cat["CATALOG_SOURCE"] == "GLUE"
    assert cat["TABLE_FORMAT"] == "ICEBERG"


def test_an_outbound_share_with_two_consumers_and_an_inbound_one(estate):
    shares = {r["name"]: r for r in _show("show shares")}
    out = shares["ENT_PARTNER_SHARE"]
    assert out["kind"] == "OUTBOUND"
    assert [c.strip() for c in out["to"].split(",")] == [
        "EMUORG.PARTNER_A", "EMUORG.PARTNER_B"]
    assert shares["WEATHER_SHARE"]["kind"] == "INBOUND"
    objects = {(r["kind"], r["name"]) for r in _show(
        'describe share "ENT_PARTNER_SHARE"')}
    assert ("TABLE", "SNOWENT.SALES.ORDERS") in objects
    assert ("VIEW", "SNOWENT.SALES.CUSTOMER_360_SV") in objects
    census = {o["source_identifier"]: o for o in estate["inv"]["census"]["objects"]
              if o["kind"] == "SHARE"}
    assert "to EMUORG.PARTNER_A, EMUORG.PARTNER_B" in census["ENT_PARTNER_SHARE"]["detail"]


def test_masking_row_access_and_column_tags_are_attached(estate):
    sec = estate["sec"]
    kinds = {(e["object"], e.get("column"), e["policy_kind"])
             for e in sec["exposures"]}
    assert ("SNOWENT.SALES.CUSTOMERS", "EMAIL", "MASKING_POLICY") in kinds
    assert ("SNOWENT.SALES.ORDERS", None, "ROW_ACCESS_POLICY") in kinds
    tags = {(a["object"], a.get("column"), a["tag"])
            for a in sec["tag_references"]["attachments"]}
    assert ("SNOWENT.SALES.CUSTOMERS", "EMAIL", "SNOWENT.SALES.PII") in tags


def test_containers_apps_and_the_external_function_are_in_the_census(estate):
    census = estate["inv"]["census"]
    by_kind = census["by_kind"]
    assert by_kind.get("SERVICE") == 1
    assert by_kind.get("COMPUTE_POOL") == 1
    assert by_kind.get("APPLICATION") == 1
    ext = next(o for o in census["objects"] if o["kind"] == "EXTERNAL_FUNCTION")
    assert ext["source_identifier"] == "SNOWENT.OPS.SCORE_LEAD"
    assert "api_integration=ENT_SCORING_API" in ext["detail"]


def test_replication_and_failover_groups_answer_in_snowflakes_shape():
    # SHOW REPLICATION GROUPS lists replication AND failover groups, told
    # apart by `type`; SHOW FAILOVER GROUPS lists only the failover ones.
    groups = {r["name"]: r for r in _show("show replication groups")}
    assert groups["ENT_RG"]["type"] == "REPLICATION"
    assert groups["ENT_FG"]["type"] == "FAILOVER"
    assert [r["name"] for r in _show("show failover groups")] == ["ENT_FG"]
    assert groups["ENT_FG"]["allowed_accounts"] == "EMUORG.EMU_ENT_DR"


def test_no_value_in_the_estate_looks_like_a_real_identifier(estate):
    # Hard rule for the whole plugin: fakes only. Everything the estate
    # returns is serialised and checked for the shapes a real one has.
    blob = json.dumps({k: v for k, v in estate.items() if k != "run"},
                      default=str).lower()
    for real in ("snowflakecomputing.com", "ocid1.", "oraclecloud.com",
                 "@oracle.com", "amazonaws.com"):
        assert real not in blob, real
