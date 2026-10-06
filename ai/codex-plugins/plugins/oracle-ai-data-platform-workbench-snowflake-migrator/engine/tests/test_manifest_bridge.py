"""The in-AIDP manifest -> inventory bridge (runbook S6 -> S7).

The property that matters is AGREEMENT: a column planned from a manifest and
the same column planned from a live `assess` must reach the same verdict. Two
mappers that agree today diverge after the first change to either, so the
bridge is required to call the shared one.
"""
import pytest

from snowflake_source.extract.manifest import (ManifestShapeError,
                                               inventory_from_manifest)
from snowflake_source.dialect.types import map_type


def _col(name, data_type, *, precision=None, scale=None, length=None,
         nullable=True):
    return {"name": name, "type": data_type, "nullable": nullable,
            "data_type": data_type, "numeric_precision": precision,
            "numeric_scale": scale, "character_maximum_length": length}


def _manifest(**kw):
    table = {"name": "ORDERS", "source_rows": 10, "source_bytes": 100,
             "columns": [_col("ID", "NUMBER", precision=38, scale=0)]}
    table.update(kw)
    return {"schemas": [{"name": "SALES", "tables": [table], "views": [],
                         "errors": []}]}


def test_the_bridge_uses_the_same_mapper_as_a_live_assess():
    """Not "a mapping that looks right" -- the same function, same verdict."""
    inv = inventory_from_manifest(
        _manifest(columns=[_col("AMOUNT", "NUMBER", precision=14, scale=2)]),
        database="DB")
    got = inv["inventory"][0]["columns"][0]["target_type"]
    assert got == map_type("NUMBER", precision=14, scale=2).spark_type


def test_a_variant_column_blocks_its_table_exactly_as_it_does_live():
    inv = inventory_from_manifest(
        _manifest(columns=[_col("PAYLOAD", "VARIANT")]), database="DB")
    rec = inv["inventory"][0]
    assert rec["compatibility_status"] == "blocked"
    assert "PAYLOAD" in rec["blocked_reasons"][0]


def test_the_semi_structured_mode_is_honoured():
    inv = inventory_from_manifest(
        _manifest(columns=[_col("PAYLOAD", "VARIANT")]), database="DB",
        semi_structured="string")
    rec = inv["inventory"][0]
    assert rec["compatibility_status"] == "supported"
    assert any("PAYLOAD" in w for w in rec["warnings"]), \
        "carrying VARIANT as text is a deferral and must warn"


def test_a_manifest_row_count_is_metadata_and_never_called_verified():
    inv = inventory_from_manifest(_manifest(), database="DB")
    rec = inv["inventory"][0]
    assert rec["row_count_exact"] == 10
    assert rec["row_count_source"] == "show_metadata"
    assert "not a count(*)" in rec["row_count_note"].lower()


def test_a_view_arrives_without_its_sql_and_says_so():
    """A manifest carries columns, not definitions.

    Emitting a view record with no text and no complaint would read as
    translatable, and the planner would refuse it with no reason to show.
    """
    m = {"schemas": [{"name": "S", "tables": [], "errors": [],
                      "views": [{"name": "V", "columns": [_col("A", "TEXT")]}]}]}
    inv = inventory_from_manifest(m, database="DB")
    rec = inv["inventory"][0]
    assert rec["object_type"] == "VIEW"
    assert rec["view_text_show"] is None
    assert "cannot be dialect-translated" in rec["view_ddl_error"]
    assert any("view SQL absent" in n for n in inv["extraction_notes"])


def test_an_old_manifest_without_raw_type_fields_is_refused_not_parsed():
    """Re-parsing "NUMBER(38,0)" would be a second, lossier mapper."""
    m = {"schemas": [{"name": "S", "errors": [], "views": [], "tables": [
        {"name": "T", "columns": [{"name": "C", "type": "NUMBER(38,0)",
                                   "nullable": True}]}]}]}
    with pytest.raises(ManifestShapeError, match="raw-type fields"):
        inventory_from_manifest(m, database="DB")


def test_the_database_is_required_and_never_guessed():
    with pytest.raises(ManifestShapeError, match="needs `database`"):
        inventory_from_manifest(_manifest(), database="")


def test_a_shape_that_is_not_a_manifest_is_refused():
    with pytest.raises(ManifestShapeError, match="top-level `schemas`"):
        inventory_from_manifest({"inventory": []}, database="DB")


def test_discovery_errors_become_extraction_notes():
    """An object discovery could not read is ABSENT from the inventory, and
    absence must never read as "it does not exist"."""
    m = {"schemas": [{"name": "S", "tables": [], "views": [], "errors": [
        {"object": "BROKEN", "kind": "BASE TABLE",
         "error": "INFORMATION_SCHEMA.COLUMNS returned no column"}]}]}
    inv = inventory_from_manifest(m, database="DB")
    assert inv["inventory"] == []
    assert any("BROKEN" in n for n in inv["extraction_notes"])


def test_case_collisions_are_detected_on_the_manifest_path_too():
    m = {"schemas": [{"name": "S", "views": [], "errors": [], "tables": [
        {"name": "ORDERS", "columns": [_col("A", "TEXT")]},
        {"name": "orders", "columns": [_col("A", "TEXT")]}]}]}
    inv = inventory_from_manifest(m, database="DB")
    assert inv["identifier_case_collisions"], \
        "Spark folds to lower and would merge these two, losing data"


def test_the_inventory_shape_matches_what_the_planning_stages_read():
    inv = inventory_from_manifest(_manifest(), database="DB")
    for key in ("probed_at", "databases_in_scope", "row_count_mode",
                "semi_structured_mode", "geospatial_mode",
                "timestamp_ntz_mode", "object_count", "counts_by_type",
                "identifier_case_collisions", "extraction_notes",
                "inventory"):
        assert key in inv, key
    rec = inv["inventory"][0]
    for key in ("source_identifier", "object_type", "source_database",
                "source_schema", "identifier_case_form", "migration_status",
                "compatibility_status", "blocked_reasons", "warnings",
                "columns"):
        assert key in rec, key
    assert rec["source_identifier"] == "DB.SALES.ORDERS"


# --- source_metadata: the keys every consumer reads ------------------------

def test_manifest_metadata_uses_the_same_keys_as_a_live_assess():
    """Agreement extends past types to metadata: `restrictions.max_bytes`,
    `render_inventory`, `maintenance` and `ddl` all read `source_metadata`
    under the keys a live `assess` writes. A manifest-built record that spells
    them differently is invisible to every one of them."""
    from snowflake_source.extract.catalog import _META_KEYS
    rec = inventory_from_manifest(_manifest(), database="DB")["inventory"][0]
    assert set(rec["source_metadata"]) <= set(_META_KEYS), rec["source_metadata"]
    assert rec["source_metadata"]["bytes"] == 100
    assert rec["source_metadata"]["rows"] == 10


def test_a_max_bytes_restriction_excludes_an_ingested_table():
    # The runbook path: discover in AIDP, ingest, plan a first wave with a size
    # cap. The cap used to exclude nothing while plan.json recorded it as
    # applied, so the largest tables entered the canary.
    from plan.restrictions import apply_restrictions
    big = {"name": "ORDERS", "source_rows": 10, "source_bytes": 4096,
           "columns": [_col("ID", "NUMBER", precision=38, scale=0)]}
    tiny = {"name": "TINY", "source_rows": 1, "source_bytes": 8,
            "columns": [_col("ID", "NUMBER", precision=38, scale=0)]}
    inv = inventory_from_manifest(
        {"schemas": [{"name": "SALES", "tables": [big, tiny], "views": [],
                      "errors": []}]}, database="DB")
    kept, excluded = apply_restrictions(inv["inventory"], {"max_bytes": 100})
    assert [e["source_identifier"] for e in excluded] == ["DB.SALES.ORDERS"]
    assert "exceeds max_bytes" in excluded[0]["reason"]
    assert [k["source_identifier"] for k in kept] == ["DB.SALES.TINY"]


def test_an_ingested_inventory_renders_its_sizes():
    from report.render import render_inventory
    inv = inventory_from_manifest(_manifest(source_bytes=4096), database="DB")
    row = next(line for line in render_inventory(inv).splitlines()
               if "`DB.SALES.ORDERS`" in line)
    assert "4.0 KB" in row, row


# ------------------- the column facts the in-AIDP path silently dropped
#
# Live 2026-09-23. The same estate, planned two ways, compared column by
# column: 71 columns, identical types and identical verdicts -- and six
# facts present from a live `assess` and absent from a manifest.
#
#     CUSTOMERS.SEGMENT       COLUMN_DEFAULT  live "'SMB'"  manifest None
#     CUSTOMERS.CREDIT_LIMIT  COLUMN_DEFAULT  live "0"      manifest None
#     CUSTOMERS.IS_ACTIVE     COLUMN_DEFAULT  live "TRUE"   manifest None
#     CUSTOMERS.COMMENT_COL   COMMENT         live "a ..."  manifest None
#     ORDERS.ORDER_ID         IDENTITY_START/INCREMENT      manifest None
#
# Those are exactly the facts R22, R23 and the column-COMMENT fidelity work
# report on. An estate planned through runbook S6/S7 -- the path for a large
# estate, the one that exists BECAUSE the estate is large -- gets a DDL plan
# with no warning that its defaults and identity columns stop working at
# cutover. The laptop path warns; the cluster path does not; neither says
# they differ.
#
# Two halves: discovery never selected the columns, and the reader ignored
# them when present. And the third thing that matters most -- a manifest
# written before this change carries no defaults, and "absent" must not be
# rendered as "there is no default".

def _facts_manifest(cols, kind="tables"):
    return {"schemas": [{"name": "S", kind: [{"name": "T", "columns": cols}]}]}


def _fact_col(**over):
    base = {"name": "C", "data_type": "NUMBER", "numeric_precision": 38,
            "numeric_scale": 0, "character_maximum_length": None,
            "nullable": True, "ordinal_position": 1}
    base.update(over)
    return base


def test_a_default_in_the_manifest_reaches_the_inventory():
    inv = inventory_from_manifest(
        _facts_manifest([_fact_col(column_default="'SMB'")]), database="D")
    col = inv["inventory"][0]["columns"][0]
    assert col["COLUMN_DEFAULT"] == "'SMB'"


def test_identity_in_the_manifest_reaches_the_inventory():
    inv = inventory_from_manifest(
        _facts_manifest([_fact_col(identity_start=1, identity_increment=1)]),
        database="D")
    col = inv["inventory"][0]["columns"][0]
    assert col["IDENTITY_START"] == 1
    assert col["IDENTITY_INCREMENT"] == 1


def test_a_column_comment_in_the_manifest_reaches_the_inventory():
    inv = inventory_from_manifest(
        _facts_manifest([_fact_col(comment="a column comment")]), database="D")
    assert inv["inventory"][0]["columns"][0]["COMMENT"] == "a column comment"


def test_an_old_manifest_says_the_facts_are_UNKNOWN_not_absent():
    """The one that matters. A manifest written before discovery recorded
    these carries no key at all, and rendering that as "no default" is the
    same false negative the census rule exists to prevent."""
    inv = inventory_from_manifest(_facts_manifest([_fact_col()]), database="D")
    rec = inv["inventory"][0]
    assert any("default" in n.lower() and "unknown" in n.lower()
               for n in inv["extraction_notes"]), inv["extraction_notes"]
    assert rec.get("column_facts_unknown") is True


def test_a_new_manifest_that_simply_has_no_default_is_not_unknown():
    """`column_default: None` explicitly recorded is a real answer."""
    inv = inventory_from_manifest(
        _facts_manifest([_fact_col(column_default=None, identity_start=None,
                        identity_increment=None, comment=None,
                        facts_recorded=True)]), database="D")
    rec = inv["inventory"][0]
    assert rec.get("column_facts_unknown") is not True
    assert not [n for n in inv["extraction_notes"] if "unknown" in n.lower()]


def test_the_discovery_query_selects_the_facts_it_has_to_carry():
    """Half of the fix is upstream: the manifest cannot carry what
    discovery never read."""
    import pathlib as _p
    sql = (_p.Path(__file__).resolve().parents[1]
           / "dataplane" / "00_discover_snowflake.py").read_text(encoding="utf-8")
    block = sql[sql.index("_COLUMNS_SQL"):sql.index("_COLUMNS_SQL") + 700]
    for field in ("COLUMN_DEFAULT", "IDENTITY_START", "IDENTITY_INCREMENT",
                  "COMMENT"):
        assert field in block, f"discovery must select {field}"
