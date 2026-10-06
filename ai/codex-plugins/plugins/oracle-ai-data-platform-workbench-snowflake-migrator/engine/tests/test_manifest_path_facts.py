"""The in-AIDP discovery must WRITE what it reads, or the manifest path plans
a different estate from the laptop path.

Commit 50a79ee ("the in-AIDP planning path dropped the column facts") added
COLUMN_DEFAULT, IDENTITY_START, IDENTITY_INCREMENT and COMMENT to discovery's
INFORMATION_SCHEMA.COLUMNS query -- and to nothing else. The per-column dict
`discover_via_connector` writes still ended at CHARACTER_MAXIMUM_LENGTH, so
the four values were fetched and dropped on the floor. Observed with the
real discover_via_connector: seven keys per column,
`manifest_records_column_facts` False, the DDL plan's rules
[R01, R02, R03, R03, R21, R30] with no R22/R23, and no column COMMENT in the
CREATE TABLE. INVENTORY.md then told the operator to re-run discovery, and a
re-run could never fix it.

Same shape for the table's KIND. TABLE_TYPE was read only to choose the
tables or the views bucket, and the bridge built `source_metadata` from rows
and bytes alone, so `object_kind_block` never saw an event or external
table: DB.SALES.APP_EVENTS was planned can_migrate from a manifest while a
live `assess` of the same estate refused it as a Snowflake event table.

These tests drive the real discovery function into the real bridge, planner
and DDL builder -- the unit tests on each half passed throughout, because
each half was tested against a hand-written manifest the other half never
produced.
"""
import importlib.util
import pathlib
import sys

import pytest

from plan.build import build_plan, object_kind_block
from snowflake_source.extract.manifest import (inventory_from_manifest,
                                               manifest_records_column_facts)
from target.ddl import build_ddl_payload

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


@pytest.fixture(scope="module")
def discover():
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        "snowmig_script_00_facts", SCRIPTS / "00_discover_snowflake.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _DF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]


class _Source:
    """INFORMATION_SCHEMA as Snowflake answers it: TABLES and COLUMNS rows."""

    def __init__(self, tables, columns):
        self._tables, self._columns = tables, columns
        self.queries: list[str] = []

    def pushdown(self, sql, schema=None):
        self.queries.append(sql)
        return _DF(self._columns if "COLUMNS" in sql else self._tables)


def _rel(name, table_type="BASE TABLE", **extra):
    return {"TABLE_SCHEMA": "SALES", "TABLE_NAME": name,
            "TABLE_TYPE": table_type, "ROW_COUNT": 5, "BYTES": 50, **extra}


def _col(table, name, pos, data_type="NUMBER", **extra):
    base = {"TABLE_SCHEMA": "SALES", "TABLE_NAME": table, "COLUMN_NAME": name,
            "ORDINAL_POSITION": pos, "DATA_TYPE": data_type,
            "IS_NULLABLE": "YES", "NUMERIC_PRECISION": None,
            "NUMERIC_SCALE": None, "CHARACTER_MAXIMUM_LENGTH": None,
            "COLUMN_DEFAULT": None, "IDENTITY_START": None,
            "IDENTITY_INCREMENT": None, "COMMENT": None}
    if data_type == "NUMBER":
        base.update(NUMERIC_PRECISION=38, NUMERIC_SCALE=0)
    if data_type == "TEXT":
        base.update(CHARACTER_MAXIMUM_LENGTH=20)
    base.update(extra)
    return base


def _estate():
    tables = [_rel("ORDERS", COMMENT="one row per order"),
              _rel("APP_EVENTS", "EVENT TABLE")]
    columns = [
        _col("ORDERS", "ID", 1, IDENTITY_START="1", IDENTITY_INCREMENT="1",
             IS_NULLABLE="NO"),
        _col("ORDERS", "STATUS", 2, "TEXT", COLUMN_DEFAULT="'NEW'",
             COMMENT="order lifecycle state"),
        _col("APP_EVENTS", "TS", 1, "TIMESTAMP_NTZ")]
    return _Source(tables, columns)


def _manifest(discover, source):
    return {"schemas": discover.discover_via_connector(
        source, wanted=None, exclude={"information_schema"})}


def _plan_both(manifest):
    inv = inventory_from_manifest(manifest, database="DB")
    plan = build_plan(inv, {"edges": []})
    return inv, plan, build_ddl_payload(inv, plan)


def test_discovery_writes_the_column_facts_it_selects(discover):
    manifest = _manifest(discover, _estate())
    orders = next(t for t in manifest["schemas"][0]["tables"]
                  if t["name"] == "ORDERS")
    by_name = {c["name"]: c for c in orders["columns"]}
    assert by_name["STATUS"]["column_default"] == "'NEW'"
    assert by_name["STATUS"]["comment"] == "order lifecycle state"
    assert by_name["ID"]["identity_start"] == "1", "carried verbatim"
    assert by_name["ID"]["identity_increment"] == "1"
    # Discovery says outright that it read them, so a column that simply
    # has no default is a real "none", never "unknown".
    assert all(c["facts_recorded"] is True for c in orders["columns"])
    assert [c["ordinal_position"] for c in orders["columns"]] == [1, 2]
    assert manifest_records_column_facts(manifest) is True


def test_a_manifest_planned_estate_gets_R22_R23_and_the_column_comment(discover):
    inv, _, ddl = _plan_both(_manifest(discover, _estate()))
    assert not any(r.get("column_facts_unknown") for r in inv["inventory"])
    stmt = next(s for s in ddl["statements"]
                if s["source_identifier"] == "DB.SALES.ORDERS")
    rules = [r["rule_id"] for r in stmt["rules_applied"]]
    assert "R22_COLUMN_DEFAULT_NOT_EMITTED" in rules, rules
    assert "R23_IDENTITY_NOT_EMITTED" in rules, rules
    assert "COMMENT 'order lifecycle state'" in stmt["sql"], stmt["sql"]
    # The table COMMENT travels too, as it does from a live assess (R24).
    assert "R24_TABLE_COMMENT_CARRIED" in rules, rules
    assert stmt["description"] == "one row per order"


def test_an_event_table_is_refused_with_the_laptop_paths_reason(discover):
    _, plan, _ = _plan_both(_manifest(discover, _estate()))
    can = [c["source_identifier"] for c in plan["can_migrate"]]
    assert "DB.SALES.APP_EVENTS" not in can, can
    refused = {c["source_identifier"]: c for c in plan["cannot_migrate"]}
    got = refused["DB.SALES.APP_EVENTS"]
    # Exactly what a live assess records from SHOW TABLES (is_event = Y).
    laptop = object_kind_block({"object_type": "TABLE",
                                "source_metadata": {"is_event": "Y"}})
    assert got["category"] == "unsupported_object"
    assert got["reason"] == laptop[1]


@pytest.mark.parametrize("table_type, flags, label", [
    ("EXTERNAL TABLE", {}, "external table"),
    ("BASE TABLE", {"IS_DYNAMIC": "YES"}, "dynamic table"),
    ("BASE TABLE", {"IS_ICEBERG": "YES"}, "Iceberg table"),
    ("BASE TABLE", {"IS_HYBRID": "YES"}, "hybrid table"),
])
def test_every_kind_the_laptop_path_refuses_is_refused_from_a_manifest(
        discover, table_type, flags, label):
    source = _Source([_rel("T", table_type, **flags)], [_col("T", "ID", 1)])
    inv = inventory_from_manifest(_manifest(discover, source), database="DB")
    block = object_kind_block(inv["inventory"][0])
    assert block and block[0] == label, inv["inventory"][0]["source_metadata"]


def test_a_plain_table_carries_no_kind_flag_that_reads_as_set(discover):
    """`NO` is not in the planner's unset list, so a raw INFORMATION_SCHEMA
    `NO` passed through would be reported as a dropped property. The flags
    are spelled the way SHOW spells them: Y or N."""
    source = _Source([_rel("T", IS_TRANSIENT="NO", IS_DYNAMIC="NO",
                           IS_ICEBERG="NO", IS_HYBRID="NO")],
                     [_col("T", "ID", 1)])
    inv, plan, ddl = _plan_both(_manifest(discover, source))
    rec = inv["inventory"][0]
    assert object_kind_block(rec) is None
    assert set(rec["source_metadata"].get(k) for k in (
        "is_dynamic", "is_iceberg", "is_hybrid", "is_event",
        "is_external")) <= {"N", None}
    assert not ddl["statements"][0]["omitted_properties"]


def test_transient_and_temporary_are_planned_with_the_laptop_warning(discover):
    source = _Source([_rel("TR", IS_TRANSIENT="YES"),
                      _rel("TMP", "TEMPORARY TABLE")],
                     [_col("TR", "ID", 1), _col("TMP", "ID", 1)])
    _, plan, _ = _plan_both(_manifest(discover, source))
    kinds = {w["source_identifier"]: w["kind"]
             for w in plan["table_kind_warnings"]}
    assert kinds == {"DB.SALES.TR": "TRANSIENT", "DB.SALES.TMP": "TEMPORARY"}


def test_the_kind_flags_that_may_not_exist_do_not_cost_the_discovery(discover):
    """IS_DYNAMIC / IS_ICEBERG / IS_HYBRID are newer INFORMATION_SCHEMA
    columns. An account where one is missing must not lose the whole
    discovery to `invalid identifier`: the narrower query is retried and the
    gap is recorded, not guessed."""
    class Older(_Source):
        def pushdown(self, sql, schema=None):
            if "IS_DYNAMIC" in sql:
                self.queries.append(sql)
                raise RuntimeError("SQL compilation error: error line 1 at "
                                   "position 80 invalid identifier 'IS_DYNAMIC'")
            return super().pushdown(sql, schema)

    source = Older([_rel("T")], [_col("T", "ID", 1)])
    schemas = discover.discover_via_connector(source, wanted=None, exclude=set())
    entry = schemas[0]["tables"][0]
    assert entry["name"] == "T" and entry["columns"]
    assert "invalid identifier" in entry["kind_flags_unread"]
    inv = inventory_from_manifest({"schemas": schemas}, database="DB")
    assert any("IS_DYNAMIC" in n or "kind" in n.lower()
               for n in inv["extraction_notes"]), inv["extraction_notes"]
