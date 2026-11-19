"""A dot inside a quoted Snowflake name must not become a fourth name part.

Snowflake allows `"orders.v2"` as a table name. The extractor joins the raw
parts into source_identifier `MYDB.PUBLIC.orders.v2`, and the planner split
that back with `ident.split(".", 2)` -- so the table part was `orders.v2`,
bronze_target produced `mydb.public.orders.v2`, and the name check split the
whole FQN on `.` and found four fragments that each pass the rule. The table
was planned can_migrate, PLANNED_OBJECTS.md showed the four-part target, and
`snowmig ddl` then died on "target_fqn must be three-part
catalog.schema.table" with exit 1 and no ddl_plan.json for ANY object. A
dotted schema name did the same to schemas_to_create.

The planner now takes the parts from the record's own source_database and
source_schema, and refuses a part containing a dot as
unacceptable_target_name, so ddl never sees it.
"""
from fake_sql import FakeSql
from plan.build import build_plan
from snowflake_source.extract.catalog import build_inventory
from target.ddl import build_ddl_payload
from test_catalog import COLUMNS, base_responses


def _inventory():
    dotted = [dict(c, TABLE_NAME="orders.v2") for c in COLUMNS[:2]]
    return build_inventory(FakeSql(base_responses(**{
        "information_schema.columns": COLUMNS[:2] + dotted,
        "show tables": [
            {"name": "ORDERS", "rows": 1, "bytes": 10, "created_on": "x",
             "comment": None, "cluster_by": None},
            {"name": "orders.v2", "rows": 1, "bytes": 10, "created_on": "x",
             "comment": None, "cluster_by": None}],
        "show views": []})), databases=["MYDB"])


def test_a_dotted_table_name_is_refused_not_planned():
    plan = build_plan(_inventory(), {"edges": []})
    assert [c["source_identifier"] for c in plan["can_migrate"]] == [
        "MYDB.PUBLIC.ORDERS"]
    entry = next(c for c in plan["cannot_migrate"]
                 if c["source_identifier"] == "MYDB.PUBLIC.orders.v2")
    assert entry["category"] == "unacceptable_target_name"
    assert "'orders.v2'" in entry["reason"]


def test_ddl_still_emits_the_rest_of_the_estate():
    inv = _inventory()
    payload = build_ddl_payload(inv, build_plan(inv, {"edges": []}))
    assert [s["target_fqn"] for s in payload["statements"]] == [
        "mydb.public.orders"]


def test_a_dotted_schema_name_is_refused_and_creates_no_schema():
    rec = {"source_identifier": "MYDB.a.b.T", "object_type": "TABLE",
           "source_database": "MYDB", "source_schema": "a.b",
           "compatibility_status": "supported", "blocked_reasons": [],
           "columns": [{"COLUMN_NAME": "ID"}], "row_count_exact": 1,
           "source_metadata": {}}
    plan = build_plan({"inventory": [rec]}, {"edges": []})
    assert plan["can_migrate"] == []
    assert plan["cannot_migrate"][0]["category"] == "unacceptable_target_name"
    assert plan["schemas_to_create"] == []


def test_the_parts_come_from_the_record_not_from_splitting_the_identifier():
    # The same table under a prefix: the schema part must be `mydb_public`,
    # and the refused name is the table's, whole.
    plan = build_plan(_inventory(), {"edges": []}, bronze_catalog_prefix="lake")
    entry = next(c for c in plan["cannot_migrate"]
                 if c["source_identifier"] == "MYDB.PUBLIC.orders.v2")
    assert "'orders.v2'" in entry["reason"]
    assert plan["target_names"]["MYDB.PUBLIC.ORDERS"] == "lake.mydb_public.orders"
