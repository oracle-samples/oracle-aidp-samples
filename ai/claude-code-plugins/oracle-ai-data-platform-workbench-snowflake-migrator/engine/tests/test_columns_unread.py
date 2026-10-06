"""A schema whose column read FAILED is not a schema of supported tables.

Review 2026-09-25. On a schema where the INFORMATION_SCHEMA.COLUMNS read
timed out (000630), catalog._columns caught the error, wrote an extraction
note and returned nothing. _record then computed compatibility_status from
blocked reasons over an EMPTY column list -- and no blocked reasons reads as
`supported`. The four reports disagreed from there: INVENTORY.md said
`supported` with Cols 0, PLANNED_OBJECTS counted it as movable, SUMMARY.md
rated it LOW "structure clones cleanly", and only DDL_PLAN refused it -- with
a privilege reason ("no columns visible to this role") for what had been a
timeout, sending the operator off to fix grants.

The extraction side is fixed here: a record whose schema's column read
failed says so, as compatibility_status `unassessed`, columns_read `failed`
and the error text in columns_read_error. A read that succeeded says
columns_read `ok`, so "read and found none" stays distinguishable from
"never read". What the planner and the reports do with `unassessed` is
decided downstream.
"""
from fake_sql import FakeSql
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.manifest import inventory_from_manifest

from test_catalog import _base_responses, _col

_TIMEOUT = ("000630 (57014): Statement reached its statement or warehouse "
            "timeout of 30 second(s) and was canceled.")


def _timing_out_columns(tables, views=()):
    base = FakeSql(_base_responses(tables=tables, views=views))
    base.responses["get_ddl"] = [{"D": "create view V as select 1"}]

    def run_sql(sql, params=None):
        if "information_schema.columns" in sql.lower():
            raise RuntimeError(_TIMEOUT)
        return base(sql, params)
    return run_sql


def test_a_failed_column_read_is_unassessed_not_supported():
    inv = build_inventory(_timing_out_columns([{"name": "ORDERS", "rows": 5}]),
                          row_counts="none")
    rec = inv["inventory"][0]
    assert rec["compatibility_status"] == "unassessed"
    assert rec["columns_read"] == "failed"
    assert "000630" in rec["columns_read_error"]
    assert rec["columns"] == []
    # The note is still written: the record flag does not replace it.
    assert any("columns" in n and "000630" in n
               for n in inv["extraction_notes"])


def test_every_object_in_the_schema_carries_the_failure_views_too():
    inv = build_inventory(_timing_out_columns(
        [{"name": "T", "rows": 1}], views=[{"name": "V", "text": "select 1"}]),
        row_counts="none")
    assert {r["object_type"]: r["columns_read"] for r in inv["inventory"]} \
        == {"TABLE": "failed", "VIEW": "failed"}
    assert all(r["compatibility_status"] == "unassessed"
               for r in inv["inventory"])


def test_the_error_text_is_bounded():
    long = "x" * 5000

    base = FakeSql(_base_responses(tables=[{"name": "T", "rows": 1}]))

    def run_sql(sql, params=None):
        if "information_schema.columns" in sql.lower():
            raise RuntimeError(long)
        return base(sql, params)

    rec = build_inventory(run_sql, row_counts="none")["inventory"][0]
    assert len(rec["columns_read_error"]) <= 300


def test_a_read_that_succeeded_says_ok_and_keeps_its_verdict():
    inv = build_inventory(FakeSql(_base_responses(
        tables=[{"name": "T", "rows": 1}], columns=[_col("T")])),
        row_counts="none")
    rec = inv["inventory"][0]
    assert rec["columns_read"] == "ok"
    assert rec["compatibility_status"] == "supported"
    assert "columns_read_error" not in rec


def test_a_read_that_succeeded_and_found_no_columns_is_still_ok():
    """Read, answered, nothing visible: a visibility fact, not a failed
    read. Kept distinct so the DDL reason can name the right cause."""
    inv = build_inventory(FakeSql(_base_responses(
        tables=[{"name": "T", "rows": 1}], columns=[])), row_counts="none")
    rec = inv["inventory"][0]
    assert rec["columns_read"] == "ok"
    assert rec["compatibility_status"] == "supported"


# ------------------------------------------------------ the manifest bridge
#
# manifest._record had the same shape: an object discovery could not read
# the columns of was noted INCOMPLETE and still marked supported.

def _manifest_with_unread(error):
    return {"schemas": [{"name": "S", "views": [], "tables": [
        {"name": "ORDERS", "columns": [], "source_rows": 5},
        {"name": "OK", "source_rows": 1, "columns": [
            {"name": "ID", "type": "NUMBER(38,0)", "nullable": True,
             "data_type": "NUMBER", "numeric_precision": 38,
             "numeric_scale": 0, "character_maximum_length": None}]}],
        "errors": [{"object": "ORDERS", "kind": "TABLE", "error": error}]}]}


def test_a_manifest_object_discovery_could_not_read_is_unassessed():
    inv = inventory_from_manifest(
        _manifest_with_unread("DESCRIBE failed: " + _TIMEOUT), database="DB")
    recs = {r["source_identifier"]: r for r in inv["inventory"]}
    unread = recs["DB.S.ORDERS"]
    assert unread["compatibility_status"] == "unassessed"
    assert unread["columns_read"] == "failed"
    assert "000630" in unread["columns_read_error"]
    assert recs["DB.S.OK"]["columns_read"] == "ok"
    assert recs["DB.S.OK"]["compatibility_status"] == "supported"


# ------------------------------------------------ a failure with no message
#
# Review of 90d7dd9: the failure was carried as the error TEXT and tested for
# truthiness. An exception whose message is empty -- TimeoutError() is the
# realistic one -- produced an empty string, so the read counted as a
# success: compatibility_status `supported`, columns_read `ok`, no
# columns_read_error, while extraction_notes held "DB.PUBLIC columns: ".
# That is the very defect this file pins, reached through a blank message.

def test_a_failed_column_read_with_an_empty_message_is_still_unassessed():
    base = FakeSql(_base_responses(tables=[{"name": "T", "rows": 1}]))

    def run_sql(sql, params=None):
        if "information_schema.columns" in sql.lower():
            raise TimeoutError()
        return base(sql, params)

    rec = build_inventory(run_sql, row_counts="none")["inventory"][0]
    assert rec["compatibility_status"] == "unassessed"
    assert rec["columns_read"] == "failed"
    # Something the operator can act on, not a blank: the exception type.
    assert "TimeoutError" in rec["columns_read_error"]


def test_a_manifest_error_entry_with_no_text_still_marks_the_object_unread():
    """Discovery writes str(exc)[:300], which is empty for the same kind of
    exception; the bridge dropped such an entry and marked the object
    supported over zero columns."""
    inv = inventory_from_manifest(_manifest_with_unread(""), database="DB")
    recs = {r["source_identifier"]: r for r in inv["inventory"]}
    unread = recs["DB.S.ORDERS"]
    assert unread["compatibility_status"] == "unassessed"
    assert unread["columns_read"] == "failed"
    assert unread["columns_read_error"]
    assert recs["DB.S.OK"]["compatibility_status"] == "supported"
