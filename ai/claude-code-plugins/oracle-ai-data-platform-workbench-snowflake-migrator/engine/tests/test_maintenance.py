"""Snowflake maintenance and layout state, captured as first-class inventory.

The plan cannot propose a maintenance cadence it cannot see. Two rules govern
this module:

  * ACCOUNT_USAGE needs a grant the plugin must not assume. An unreadable
    history degrades to "not measured", NEVER to zero -- "0 credits" and "we
    could not look" lead to opposite decisions.
  * The expensive probes are avoided. Per-table effective retention is already
    in SHOW TABLES output, so the cascade is resolved from one account query
    plus one per database and schema, not one per table.
"""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.maintenance import (
    NO_AIDP_EQUIVALENT, build_maintenance,
)


def _inv(*tables):
    return {"databases_in_scope": ["DB"],
            "inventory": [dict(t) for t in tables]}


def _table(name="T", **meta):
    base = {"cluster_by": "", "automatic_clustering": "OFF",
            "change_tracking": "OFF", "retention_time": 1,
            "search_optimization": "OFF", "rows": 100, "bytes": 4096}
    base.update(meta)
    return {"source_identifier": f"DB.PUBLIC.{name}", "object_type": "TABLE",
            "source_database": "DB", "source_schema": "PUBLIC",
            "source_metadata": base}


def _responses(**over):
    r = {
        "show parameters like 'data_retention_time_in_days' in account":
            [{"key": "DATA_RETENTION_TIME_IN_DAYS", "value": "1", "level": ""}],
        "show parameters like 'max_data_extension_time_in_days' in account":
            [{"key": "MAX_DATA_EXTENSION_TIME_IN_DAYS", "value": "14", "level": ""}],
        "in database": [{"key": "DATA_RETENTION_TIME_IN_DAYS", "value": "1",
                         "level": ""}],
        "in schema": [{"key": "DATA_RETENTION_TIME_IN_DAYS", "value": "1",
                       "level": ""}],
        "automatic_clustering_history": [],
        "table_dml_history": [],
    }
    r.update(over)
    return r


# ------------------------------------------------------------ per-table state

def test_layout_state_is_captured_per_table():
    m = build_maintenance(FakeSql(_responses()),
                          _inv(_table("ORDERS", cluster_by="(ORDER_DATE)",
                                      automatic_clustering="ON",
                                      change_tracking="ON",
                                      search_optimization="ON")))
    t = m["tables"][0]
    assert t["source_identifier"] == "DB.PUBLIC.ORDERS"
    assert t["cluster_by"] == "(ORDER_DATE)"
    assert t["automatic_clustering"] is True
    assert t["change_tracking"] is True
    assert t["search_optimization"] is True


def test_unset_flags_are_false_not_missing():
    t = build_maintenance(FakeSql(_responses()), _inv(_table()))["tables"][0]
    assert t["automatic_clustering"] is False
    assert t["clustered"] is False


def test_views_are_not_given_maintenance_state():
    inv = _inv(_table("T"))
    inv["inventory"].append({"source_identifier": "DB.PUBLIC.V",
                             "object_type": "VIEW", "source_database": "DB",
                             "source_schema": "PUBLIC", "source_metadata": {}})
    m = build_maintenance(FakeSql(_responses()), inv)
    assert [t["source_identifier"] for t in m["tables"]] == ["DB.PUBLIC.T"]


# --------------------------------------------------------- retention cascade

def test_retention_cascade_records_the_level_that_set_it():
    m = build_maintenance(FakeSql(_responses()), _inv(_table()))
    assert m["retention"]["account"]["data_retention_time_in_days"] == 1
    assert m["retention"]["account"]["max_data_extension_time_in_days"] == 14


def test_a_table_that_overrides_its_schema_is_flagged_as_table_level():
    # Effective retention comes free from SHOW TABLES; the LEVEL is inferred
    # by comparing it with the schema default, so no per-table SHOW PARAMETERS
    # is needed on a large estate.
    m = build_maintenance(FakeSql(_responses()), _inv(_table(retention_time=30)))
    t = m["tables"][0]
    assert t["retention_days"] == 30
    assert t["retention_set_at"] == "table"


def test_a_table_matching_its_schema_default_is_not_flagged_as_an_override():
    m = build_maintenance(FakeSql(_responses()), _inv(_table(retention_time=1)))
    assert m["tables"][0]["retention_set_at"] == "inherited"


def test_table_level_parameter_probing_is_opt_in():
    fake = FakeSql(_responses())
    build_maintenance(fake, _inv(_table()))
    assert not any("in table" in c.lower() for c in fake.calls), \
        "one SHOW PARAMETERS per table is the expensive path; keep it opt-in"


# -------------------------------------------------- ACCOUNT_USAGE degradation

def test_reclustering_history_is_summarised_per_table():
    m = build_maintenance(FakeSql(_responses(**{
        "automatic_clustering_history": [
            {"TABLE_NAME": "ORDERS", "SCHEMA_NAME": "PUBLIC",
             "DATABASE_NAME": "DB", "EVENTS": 12, "CREDITS": 34.5,
             "BYTES": 999, "ROWS_RECLUSTERED": 500}]})),
        _inv(_table("ORDERS", cluster_by="(A)", automatic_clustering="ON")))
    rec = m["tables"][0]["reclustering"]
    assert rec["measured"] is True
    assert rec["events"] == 12
    assert rec["credits"] == 34.5


def test_an_unreadable_account_usage_is_not_measured_rather_than_zero():
    # "0 credits" and "we could not look" lead to opposite decisions.
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "account_usage" in sql.lower():
                raise RuntimeError("Insufficient privileges on ACCOUNT_USAGE")
            return super().__call__(sql, params)

    m = build_maintenance(Denied(_responses()), _inv(_table()))
    assert m["account_usage"]["readable"] is False
    assert "privileges" in m["account_usage"]["note"]
    rec = m["tables"][0]["reclustering"]
    assert rec["measured"] is False
    assert rec["events"] is None and rec["credits"] is None
    assert any("ACCOUNT_USAGE" in u for u in m["unreadable"])


def test_a_dml_history_failure_after_clustering_history_succeeded_is_not_zero_churn():
    # The two ACCOUNT_USAGE reads degrade independently. TABLE_DML_HISTORY is
    # the larger view and the one that hits a statement timeout on a big
    # account; when it fails AFTER the clustering read succeeded, churn is
    # "not measured", never a measured zero -- a table rewriting 50M rows a
    # day must not land under "no measured churn above the threshold".
    class DmlTimesOut(FakeSql):
        def __call__(self, sql, params=None):
            if "table_dml_history" in sql.lower():
                raise RuntimeError("000630 (57014): Statement reached its "
                                   "statement or warehouse timeout")
            return super().__call__(sql, params)

    m = build_maintenance(
        DmlTimesOut(_responses(**{"automatic_clustering_history": [
            {"TABLE_NAME": "T", "SCHEMA_NAME": "PUBLIC", "DATABASE_NAME": "DB",
             "EVENTS": 3, "CREDITS": 1.5, "BYTES": 10, "ROWS_RECLUSTERED": 5}]})),
        _inv(_table("T", cluster_by="(A)", automatic_clustering="ON"),
             _table("U")))
    acct = m["account_usage"]
    assert acct["readable"] is False
    assert acct["clustering_readable"] is True
    assert acct["dml_readable"] is False
    assert "DML history failed" in acct["note"]

    by = {t["source_identifier"]: t for t in m["tables"]}
    # The reclustering read succeeded, so its data is real and kept.
    assert by["DB.PUBLIC.T"]["reclustering"]["measured"] is True
    assert by["DB.PUBLIC.T"]["reclustering"]["credits"] == 1.5
    assert by["DB.PUBLIC.U"]["reclustering"]["measured"] is True
    assert by["DB.PUBLIC.U"]["reclustering"]["events"] == 0
    # The DML read failed, so churn is not measured -- for every table.
    for t in m["tables"]:
        assert t["dml_churn"]["measured"] is False
        assert t["dml_churn"]["rows_rewritten"] is None
        assert not any("churn" in s["signal"] for s in t["signals"])
    assert any("TABLE_DML_HISTORY" in u for u in m["unreadable"])

    # And the consumers follow: the report and the stage board say so.
    from report.render import render_maintenance
    from report.stages import _finding
    md = render_maintenance(m)
    assert "no measured churn above the threshold" not in md
    row = next(line for line in md.splitlines() if "`DB.PUBLIC.T`" in line)
    assert "not measured" in row, row
    text, attention = _finding("maintenance", m)
    assert "NOT measured" in text and attention is True


def test_a_readable_but_empty_history_is_measured_zero():
    # Distinct from the above: we looked, and there was nothing.
    m = build_maintenance(FakeSql(_responses()), _inv(_table()))
    rec = m["tables"][0]["reclustering"]
    assert rec["measured"] is True
    assert rec["events"] == 0


def test_dml_churn_is_summarised_per_table():
    m = build_maintenance(FakeSql(_responses(**{
        "table_dml_history": [
            {"TABLE_NAME": "T", "SCHEMA_NAME": "PUBLIC", "DATABASE_NAME": "DB",
             "ROWS_ADDED": 1000, "ROWS_REMOVED": 40, "ROWS_UPDATED": 60,
             "WINDOWS": 7}]})), _inv(_table()))
    churn = m["tables"][0]["dml_churn"]
    assert churn["measured"] is True
    assert churn["rows_added"] == 1000
    assert churn["rows_rewritten"] == 100, "removed + updated drive compaction"


# ---------------------------------------------------------------- the signals

def test_a_clustered_table_is_flagged_as_needing_a_layout_decision():
    m = build_maintenance(FakeSql(_responses()),
                          _inv(_table(cluster_by="(A)", automatic_clustering="ON")))
    sigs = m["tables"][0]["signals"]
    assert any("clustering" in s["signal"] for s in sigs)
    assert any(s["aidp_requires"] for s in sigs)


def test_search_optimization_is_reported_as_having_no_equivalent():
    m = build_maintenance(FakeSql(_responses()),
                          _inv(_table(search_optimization="ON")))
    sigs = m["tables"][0]["signals"]
    so = [s for s in sigs if "search optimization" in s["signal"].lower()]
    assert so and so[0]["aidp_equivalent"] is None


def test_high_churn_is_flagged_as_a_compaction_signal():
    m = build_maintenance(FakeSql(_responses(**{
        "table_dml_history": [
            {"TABLE_NAME": "T", "SCHEMA_NAME": "PUBLIC", "DATABASE_NAME": "DB",
             "ROWS_ADDED": 10_000_000, "ROWS_REMOVED": 5_000_000,
             "ROWS_UPDATED": 5_000_000, "WINDOWS": 30}]})), _inv(_table()))
    assert any("churn" in s["signal"].lower() for s in m["tables"][0]["signals"])


def test_fail_safe_is_always_named_as_having_no_equivalent():
    m = build_maintenance(FakeSql(_responses()), _inv(_table()))
    names = [g["capability"].lower() for g in m["no_equivalent"]]
    assert any("fail-safe" in n for n in names)
    assert NO_AIDP_EQUIVALENT


def test_nothing_is_proposed_only_reported():
    """Naming the equivalent is the point. Emitting a STATEMENT is not.

    The signals say "OPTIMIZE (compaction)" and "a VACUUM to follow it" on
    purpose -- that is the equivalent being named. What must not appear is a
    runnable statement: a verb aimed at a target, or a concrete retention. That
    would be proposing a cadence, which needs the customer's recovery
    requirements and query patterns (item M3).
    """
    import re
    m = build_maintenance(FakeSql(_responses()),
                          _inv(_table(cluster_by="(A)", retention_time=30)))
    blob = repr(m)
    # A statement aims a verb at a QUALIFIED target, so require a dot or a
    # backtick after the verb. Prose like "VACUUM is the whole recovery story"
    # is the equivalent being named, which is exactly what this module is for.
    statement_shaped = [
        r"\bOPTIMIZE\s+[`\"\w]*\.",     # OPTIMIZE cat.schema.table
        r"\bVACUUM\s+[`\"\w]*\.",       # VACUUM cat.schema.table
        r"\bRETAIN\s+\d+",             # a chosen retention
        r"\bZORDER\s+BY\s*\(",         # a chosen key
        r"\bCLUSTER\s+BY\s*\(",
    ]
    for pattern in statement_shaped:
        found = re.search(pattern, blob, re.IGNORECASE)
        assert not found, f"emitted a statement, not a report: {found.group(0)!r}"
