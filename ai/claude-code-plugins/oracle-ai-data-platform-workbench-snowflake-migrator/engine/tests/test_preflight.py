"""The pre-flight summary: what WOULD happen, shown before anything is created.

Required behaviour: once source and destination are both known and before the
first write, the plugin states every source -> destination mapping, what it
will create, and what it will not do. A migration that starts without the user
having seen this is a migration they did not approve.
"""
import pytest

from plan import preflight
from report.render import render_preflight

PLAN = {
    "bronze_catalog_prefix": "lake",
    "bronze_schema_style": "db",
    "summary": {"can_migrate": 2, "cannot_migrate": 1, "tables": 1, "views": 1,
                "objects_inventoried": 3},
    "can_migrate": [
        {"source_identifier": "TEST_DB.PUBLIC.ORDERS", "object_type": "TABLE",
         "target": "lake.test_db.orders", "rows": 100, "columns": 41},
        {"source_identifier": "TEST_DB.PUBLIC.V_ORDERS", "object_type": "VIEW",
         "target": "lake.test_db.v_orders", "rows": None, "columns": 17}],
    "cannot_migrate": [
        {"source_identifier": "TEST_DB.PUBLIC.BAD", "object_type": "TABLE",
         "reason": "VARIANT column"}],
    "catalogs_to_create": [], "schemas_to_create": [["lake", "test_db"]],
    "silver_gold_jobs": [{"name": "silver_x", "layer": "SILVER"}],
}
SOURCE = {"account": "TESTACCT01", "region": "AWS_US_EAST_2", "role": "ACCOUNTADMIN",
          "databases": ["TEST_DB"]}
TARGET = {"datalake_ocid": "ocid1.aidataplatform.oc1.iad.aaa",
          "workspace": "ws-key", "cluster_id": "cl-key", "catalog": "lake"}


def test_it_states_both_ends_before_anything_happens():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "TESTACCT01" in md and "ocid1.aidataplatform" in md
    assert "lake" in md


def test_every_object_shows_source_to_destination():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "TEST_DB.PUBLIC.ORDERS" in md
    assert "lake.test_db.orders" in md
    assert "→" in md or "->" in md


def test_it_says_the_destination_names_are_lower_cased_and_why():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "lower" in md.lower()
    assert "fold" in md.lower() or "AIDP" in md


def test_it_lists_what_will_be_created():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "lake.test_db" in md
    assert "1 schema" in md or "schema(s)" in md


def test_it_states_plainly_that_no_data_moves():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET).lower()
    assert "no data" in md or "zero rows" in md
    assert "read-only" in md


def test_it_names_what_will_not_be_created():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET)
    assert "TEST_DB.PUBLIC.BAD" in md
    assert "VARIANT column" in md


def test_it_says_jobs_are_not_triggered():
    md = render_preflight(PLAN, source=SOURCE, target=TARGET).lower()
    assert "never triggered" in md or "not triggered" in md


def test_a_missing_target_is_stated_not_faked():
    md = render_preflight(PLAN, source=SOURCE, target=None)
    assert "not supplied" in md.lower()
    assert "nothing will be created" in md.lower()


def _sql_for(schema_counts):
    """A transport that answers the three preflight reads."""
    def run_sql(sql, *a, **k):
        low = sql.lower()
        if "current_user" in low:
            return [{"U": "U", "R": "R", "W": "W", "D": "DB"}]
        if "show schemas" in low:
            return [{"name": s} for s in schema_counts]
        if "group by table_schema" in low:
            return [{"S": s, "N": n} for s, n in
                    sorted(schema_counts.items(), key=lambda kv: -kv[1])]
        if "information_schema.tables" in low:
            name = sql.split("'")[1]
            return [{"N": schema_counts.get(name, 0)}]
        return []
    return run_sql


BASE = {"account": "A", "warehouse": "W", "database": "DB", "user": "u",
        "auth": "password", "password": "p"}


def _named(result, name):
    return next(c for c in result["checks"] if c["name"] == name)


def test_an_empty_session_schema_fails_before_the_cluster_does():
    """PUBLIC exists but holds nothing, so the connector rejects it with
    DATA_ACCESS_LAYER_0031 six minutes into a job run. Preflight is where that
    belongs."""
    res = preflight.run_preflight(dict(BASE, schema="PUBLIC"),
                                  run_sql=_sql_for({"PUBLIC": 0, "SALES": 10}))
    check = _named(res, "connector session schema")
    assert check["ok"] is False
    assert "DATA_ACCESS_LAYER_0031" in check["detail"]


def test_a_populated_session_schema_passes_and_says_it_is_not_a_filter():
    res = preflight.run_preflight(dict(BASE, schema="SALES"),
                                  run_sql=_sql_for({"PUBLIC": 0, "SALES": 10}))
    check = _named(res, "connector session schema")
    assert check["ok"] is True
    assert "not the discovery" in check["detail"]


def test_the_suggested_schemas_are_read_from_the_account_not_shipped():
    """A generic migrator cannot know which schema is populated in someone
    else's estate, so the suggestion is DERIVED from the account in front of
    it. Nothing schema-shaped is hardcoded in the plugin."""
    res = preflight.run_preflight(dict(BASE, schema="PUBLIC"),
                                  run_sql=_sql_for({"PUBLIC": 0, "SALES": 10,
                                                    "OPS": 7}))
    detail = _named(res, "connector session schema")["detail"]
    assert "SALES (10)" in detail and "OPS (7)" in detail
    assert "PUBLIC (0)" not in detail, "an empty schema is not a suggestion"
