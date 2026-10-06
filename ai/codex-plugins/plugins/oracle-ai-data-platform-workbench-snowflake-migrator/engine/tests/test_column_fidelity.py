"""The reviewed DDL and the applied DDL must carry the same column facts.

`DDL_PLAN.md` is the approval artifact. Every property that appears in the
SQL an operator signs off -- `NOT NULL`, a column `COMMENT`, a table
`COMMENT` -- has to reach the object that is actually created, on BOTH
execution paths, or be named per object as something that path cannot carry.
Anything else is an approval of a document that was never applied.

The parity test at the bottom is the one that stops the two paths drifting
apart again: the same input goes through `build_create_table` and through the
structure notebook's column renderer, and the two are compared property by
property.
"""
import importlib.util
import pathlib
import sys

import pytest

from fake_sql import FakeSql
from snowflake_source.extract.catalog import build_inventory
from target import catalog_api, catalog_deploy
from target.ddl import build_create_table, build_ddl_payload

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _load(name: str):
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        f"snowmig_fidelity_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def structure():
    return _load("01_create_structure")


def col(name, dt, target, *, nullable="YES", pos=1, comment=None,
        default=None, identity_start=None, identity_increment=None):
    return {"COLUMN_NAME": name, "DATA_TYPE": dt, "target_type": target,
            "IS_NULLABLE": nullable, "ORDINAL_POSITION": pos,
            "COMMENT": comment, "COLUMN_DEFAULT": default,
            "IDENTITY_START": identity_start,
            "IDENTITY_INCREMENT": identity_increment}


def record(columns, **over):
    r = {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "source_database": "D", "source_schema": "PUBLIC",
         "compatibility_status": "supported", "blocked_reasons": [],
         "columns": columns, "source_metadata": {}}
    r.update(over)
    return r


# --- defect 1: reviewed != applied ---------------------------------------

def test_expected_columns_carry_nullability_and_description():
    res = build_create_table(
        record([col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO",
                    comment="the order key"),
                col("PAID", "NUMBER", "DECIMAL(18,2)", pos=2)]),
        "bronze.PUBLIC.ORDERS")
    assert "NOT NULL" in res.sql and "COMMENT" in res.sql
    assert res.expected_columns == [
        {"name": "ORDER_ID", "type": "DECIMAL(38,0)", "nullable": False,
         "description": "the order key"},
        {"name": "PAID", "type": "DECIMAL(18,2)", "nullable": True,
         "description": None}]


def test_table_comment_is_emitted_and_carried():
    res = build_create_table(
        record([col("A", "TEXT", "STRING")],
               source_metadata={"comment": "orders, one row per line"}),
        "bronze.PUBLIC.ORDERS")
    assert "COMMENT 'orders, one row per line'" in res.sql
    assert res.description == "orders, one row per line"


def test_generated_sql_with_comments_and_not_null_parses_as_spark():
    res = build_create_table(
        record([col("A", "TEXT", "STRING", nullable="NO", comment="it's a"),
                col("B", "TEXT", "STRING", pos=2)],
               source_metadata={"comment": "a table"}),
        "bronze.PUBLIC.ORDERS")
    sqlglot = pytest.importorskip("sqlglot", reason="dev-only SQL parse check")
    parsed = sqlglot.parse_one(res.sql, read="spark")
    assert parsed is not None


def test_the_plan_names_the_property_the_catalog_api_cannot_carry():
    """A path that cannot apply NOT NULL must say so where the rules are."""
    res = build_create_table(
        record([col("A", "TEXT", "STRING", nullable="NO")]),
        "bronze.PUBLIC.ORDERS")
    rule = next(r for r in res.rules_applied
                if r.rule_id == "R21_NOT_NULL_CATALOG_API_GAP")
    assert "catalog" in rule.detail.lower()
    assert "NULLABLE" in rule.detail


def test_catalog_api_body_carries_the_column_description():
    body = catalog_api.build_table_body(
        "lake", "sales", "orders",
        [{"name": "A", "type": "STRING", "description": "the a"}],
        description="a table")
    assert body["tableFields"][0]["fieldDescription"] == "the a"
    assert body["description"] == "a table"


def test_catalog_deploy_sends_the_table_and_column_descriptions():
    plan = {"statements": [{
        "source_identifier": "D.S.T", "object_type": "TABLE",
        "target_fqn": "lake.s.t", "sql": "...",
        "description": "orders, one row per line",
        "expected_columns": [{"name": "A", "type": "STRING",
                              "nullable": False, "description": "the a"}]}]}
    sent = {}

    def call(operation, **kw):
        if operation == "list_catalogs":
            return {"items": [{"displayName": "lake",
                               "catalogType": "INTERNAL"}]}
        if operation == "list_schemas":
            return {"items": [{"key": "lake.s", "lifecycleState": "ACTIVE"}]}
        if operation == "create_table":
            sent["body"] = kw["body"]
            return {}
        if operation == "list_tables_in":
            return {"items": [{"key": "lake.s.t"}]}
        if operation == "get_table":
            return {"tableFields": [{"fieldName": "A", "fieldType": "string",
                                     "fieldDescription": "the a"}]}
        raise AssertionError(operation)

    out = catalog_deploy.deploy_catalog(
        plan, target=_Target("lake"), execute=True, call=call,
        retry_delays=(), verify_delays=(), schema_wait=())
    assert sent["body"]["description"] == "orders, one row per line"
    assert sent["body"]["tableFields"][0]["fieldDescription"] == "the a"
    assert out["verified"] == 1
    # The property this transport cannot carry is reported, not dropped.
    assert out["properties_not_applied"], out


def test_catalog_deploy_reports_a_description_the_target_dropped():
    plan = {"statements": [{
        "source_identifier": "D.S.T", "object_type": "TABLE",
        "target_fqn": "lake.s.t", "sql": "...",
        "expected_columns": [{"name": "A", "type": "STRING",
                              "nullable": True, "description": "the a"}]}]}

    def call(operation, **kw):
        if operation == "list_catalogs":
            return {"items": [{"displayName": "lake",
                               "catalogType": "INTERNAL"}]}
        if operation == "list_schemas":
            return {"items": [{"key": "lake.s", "lifecycleState": "ACTIVE"}]}
        if operation == "create_table":
            return {}
        if operation == "list_tables_in":
            return {"items": [{"key": "lake.s.t"}]}
        if operation == "get_table":
            return {"tableFields": [{"fieldName": "A", "fieldType": "string"}]}
        raise AssertionError(operation)

    out = catalog_deploy.deploy_catalog(
        plan, target=_Target("lake"), execute=True, call=call,
        retry_delays=(), verify_delays=(), schema_wait=())
    assert out["description_drift"], "a dropped description must be reported"
    assert "the a" in out["description_drift"][0]["reason"]


class _Target:
    def __init__(self, catalog):
        self.catalog = catalog


def test_a_view_comment_is_emitted_and_its_body_still_extracts():
    from target.ddl import build_create_view
    rec = {"source_identifier": "D.PUBLIC.V", "object_type": "VIEW",
           "columns": [col("A", "TEXT", "STRING")],
           "view_ddl_get_ddl": "create view V as select A from T",
           "source_metadata": {"comment": "the v AS seen by sales"}}
    res = build_create_view(rec, "bronze.PUBLIC.V", {})
    assert "COMMENT 'the v AS seen by sales'" in res.sql
    assert res.description == "the v AS seen by sales"
    sqlglot = pytest.importorskip("sqlglot", reason="dev-only SQL parse check")
    assert sqlglot.parse_one(res.sql, read="spark") is not None
    payload = build_ddl_payload(
        {"inventory": [rec]},
        {"target_names": {"D.PUBLIC.V": "bronze.PUBLIC.V"},
         "waves": [["D.PUBLIC.V"]], "clone_targets": [], "cycles": []})
    # The catalog API takes the BODY, which must survive the new header.
    assert payload["statements"][0]["view_text"] == "select A from T"


# --- defect 1: the structure notebook path -------------------------------

class _DeltaSpark:
    """A Spark double with a catalog that remembers per-column facts."""

    def __init__(self):
        self.statements: list[str] = []
        self.tables: dict[str, list[dict]] = {}

    def sql(self, statement):
        flat = " ".join(statement.split())
        self.statements.append(flat)
        low = flat.lower()
        if low.startswith("create schema"):
            return _DF([])
        if low.startswith("describe"):
            fqn = flat.split(None, 1)[1].strip()
            if fqn not in self.tables:
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {fqn}")
            return _DF([{"col_name": c["name"], "data_type": c["type"],
                         "comment": c.get("comment")}
                        for c in self.tables[fqn]])
        if low.startswith("create table if not exists"):
            fqn = flat.split()[5]
            body = flat[flat.index("(") + 1:flat.rindex(") USING DELTA")]
            self.tables.setdefault(fqn, _parse_columns(body))
            return _DF([])
        raise AssertionError(flat)

    def table(self, fqn):
        return _Table(self.tables[fqn])


class _Table:
    def __init__(self, columns):
        self.schema = _Schema(columns)


class _Schema:
    def __init__(self, columns):
        self.fields = [_Field(c) for c in columns]


class _Field:
    def __init__(self, c):
        self.name = c["name"]
        self.nullable = c.get("nullable", True)


class _DF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


def _parse_columns(body: str) -> list[dict]:
    out = []
    for part in body.split(", `"):
        part = part if part.startswith("`") else "`" + part
        name = part.split("`")[1]
        rest = part.split("`", 2)[2].strip()
        nullable = "NOT NULL" not in rest.upper()
        comment = None
        if " COMMENT " in rest.upper():
            comment = rest[rest.upper().index(" COMMENT ") + 9:].strip(" '")
            rest = rest[:rest.upper().index(" COMMENT ")]
        type_text = rest.replace("NOT NULL", "").strip().lower()
        out.append({"name": name, "type": type_text, "nullable": nullable,
                    "comment": comment})
    return out


def test_structure_notebook_applies_not_null_and_comments(structure):
    spark = _DeltaSpark()
    status = structure.create_table_from_columns(
        spark,
        [{"name": "ID", "type": "DECIMAL(38,0)", "nullable": False,
          "description": "the key"},
         {"name": "NOTE", "type": "STRING", "nullable": True,
          "description": None}],
        "lake", "sales", "orders", description="one row per order")
    created = next(s for s in spark.statements if "CREATE TABLE" in s)
    assert "`ID` DECIMAL(38,0) NOT NULL COMMENT 'the key'" in created
    assert "COMMENT 'one row per order'" in created
    assert status == "created"


def test_structure_notebook_reports_a_nullability_that_did_not_apply(
        structure):
    spark = _DeltaSpark()
    spark.tables["`lake`.`sales`.`orders`"] = [
        {"name": "ID", "type": "decimal(38,0)", "nullable": True,
         "comment": "the key"}]
    with pytest.raises(structure.TypeDrift) as exc:
        structure.create_table_from_columns(
            spark, [{"name": "ID", "type": "DECIMAL(38,0)", "nullable": False,
                     "description": "the key"}],
            "lake", "sales", "orders")
    assert "NOT NULL" in str(exc.value)


def test_structure_notebook_reports_a_comment_that_did_not_apply(structure):
    spark = _DeltaSpark()
    spark.tables["`lake`.`sales`.`orders`"] = [
        {"name": "ID", "type": "decimal(38,0)", "nullable": False,
         "comment": None}]
    with pytest.raises(structure.TypeDrift) as exc:
        structure.create_table_from_columns(
            spark, [{"name": "ID", "type": "DECIMAL(38,0)", "nullable": False,
                     "description": "the key"}],
            "lake", "sales", "orders")
    assert "comment" in str(exc.value).lower()


class _BlindSpark(_DeltaSpark):
    """A Spark whose read-back carries neither nullability nor comments."""

    def sql(self, statement):
        flat = " ".join(statement.split())
        if flat.lower().startswith("describe"):
            self.statements.append(flat)
            fqn = flat.split(None, 1)[1].strip()
            if fqn not in self.tables:
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {fqn}")
            return _DF([{"col_name": c["name"], "data_type": c["type"]}
                        for c in self.tables[fqn]])
        return super().sql(statement)

    def table(self, fqn):
        raise RuntimeError("no schema read on this cluster")


def test_a_property_that_could_not_be_read_back_is_not_called_applied(
        structure):
    spark = _BlindSpark()
    notes: list[str] = []
    status = structure.create_table_from_columns(
        spark, [{"name": "ID", "type": "DECIMAL(38,0)", "nullable": False,
                 "description": "the key"}],
        "lake", "sales", "orders", notes=notes)
    assert status == "created"
    assert any("UNCHECKED" in n and "NOT NULL" in n for n in notes), notes
    assert any("UNCHECKED" in n and "COMMENT" in n for n in notes), notes


# --- the two paths must not drift apart ----------------------------------

def test_both_execution_paths_carry_the_same_column_properties(structure):
    """One input, two paths, the same properties. This is the regression
    guard: the defect was that the SQL said NOT NULL and both appliers
    silently rendered `name type`."""
    columns = [col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO",
                   comment="the order key"),
               col("NOTE", "TEXT", "STRING", pos=2, comment="free text"),
               col("PAID", "NUMBER", "DECIMAL(18,2)", pos=3)]
    res = build_create_table(record(columns), "bronze.PUBLIC.ORDERS")

    spark = _DeltaSpark()
    structure.create_table_from_columns(
        spark, res.expected_columns, "bronze", "PUBLIC", "ORDERS",
        description=res.description)
    applied = spark.tables["`bronze`.`PUBLIC`.`ORDERS`"]

    reviewed = [
        {"name": "ORDER_ID", "nullable": False, "comment": "the order key"},
        {"name": "NOTE", "nullable": True, "comment": "free text"},
        {"name": "PAID", "nullable": True, "comment": None}]
    assert [{"name": c["name"], "nullable": c["nullable"],
             "comment": c["comment"]} for c in applied] == reviewed

    # And the catalog-API body, built from the same expected_columns.
    body = catalog_api.build_table_body(
        "bronze", "PUBLIC", "ORDERS", res.expected_columns)
    assert [f.get("fieldDescription") for f in body["tableFields"]] == [
        "the order key", "free text", None]


# --- defect 2: DEFAULT and IDENTITY --------------------------------------

def _responses(**over):
    r = {
        "current_user()": [{"U": "U", "A": "A", "R": "R", "ROLE": "R",
                            "WH": "W", "V": "1"}],
        "show databases": [{"name": "MYDB"}],
        "show schemas": [{"name": "PUBLIC"}],
        "information_schema.columns": [
            {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS",
             "ORDINAL_POSITION": 1, "COLUMN_NAME": "ORDER_ID",
             "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
             "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
             "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None,
             "COMMENT": None, "COLUMN_DEFAULT": None,
             "IDENTITY_START": "1", "IDENTITY_INCREMENT": "1"},
            {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS",
             "ORDINAL_POSITION": 2, "COLUMN_NAME": "STATUS",
             "DATA_TYPE": "TEXT", "IS_NULLABLE": "YES",
             "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
             "CHARACTER_MAXIMUM_LENGTH": 16, "DATETIME_PRECISION": None,
             "COMMENT": None, "COLUMN_DEFAULT": "'NEW'",
             "IDENTITY_START": None, "IDENTITY_INCREMENT": None}],
        "show tables": [{"name": "ORDERS", "rows": 1, "bytes": 1}],
        "show views": [],
        "show primary keys": [],
        "show unique keys": [],
        "show imported keys": [],
    }
    r.update(over)
    return r


def test_column_default_and_identity_are_read():
    inv = build_inventory(FakeSql(_responses()), databases=["MYDB"])
    columns = inv["inventory"][0]["columns"]
    assert columns[0]["IDENTITY_START"] == "1"
    assert columns[1]["COLUMN_DEFAULT"] == "'NEW'"


def test_the_select_asks_for_default_and_identity():
    fake = FakeSql(_responses())
    build_inventory(fake, databases=["MYDB"])
    select = next(c for c in fake.calls if "information_schema.columns" in c)
    for column in ("column_default", "identity_start", "identity_increment"):
        assert column in select.lower()


def test_default_and_identity_get_a_verdict_in_the_plan_and_the_ddl():
    res = build_create_table(
        record([col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO",
                    identity_start="1", identity_increment="1"),
                col("STATUS", "TEXT", "STRING", pos=2, default="'NEW'")]),
        "bronze.PUBLIC.ORDERS")
    ids = {r.rule_id for r in res.rules_applied}
    assert "R22_COLUMN_DEFAULT_NOT_EMITTED" in ids
    assert "R23_IDENTITY_NOT_EMITTED" in ids
    assert any("STATUS" in w and "DEFAULT" in w for w in res.warnings)
    assert any("ORDER_ID" in w and "IDENTITY" in w for w in res.warnings)
    # Not emitted: neither is confirmed on the target.
    assert "DEFAULT" not in res.sql.upper().replace("DEFAULTS", "")
    assert "IDENTITY" not in res.sql.upper()


def test_the_plan_entry_warns_about_defaults_and_identity():
    from plan.build import build_plan
    inv = build_inventory(FakeSql(_responses()), databases=["MYDB"])
    plan = build_plan(inv, {"edges": []})
    entry = plan["can_migrate"][0]
    assert any("DEFAULT" in w for w in entry["warnings"]), entry["warnings"]
    assert any("IDENTITY" in w for w in entry["warnings"]), entry["warnings"]


# --- defect 3: constraints ------------------------------------------------

PK = [{"database_name": "MYDB", "schema_name": "PUBLIC",
       "table_name": "ORDERS", "column_name": "ORDER_ID", "key_sequence": 1,
       "constraint_name": "SYS_PK", "rely": "false"}]
UNIQUE = [{"database_name": "MYDB", "schema_name": "PUBLIC",
           "table_name": "ORDERS", "column_name": "STATUS", "key_sequence": 1,
           "constraint_name": "U_STATUS", "rely": "false"}]
FK = [{"pk_database_name": "MYDB", "pk_schema_name": "PUBLIC",
       "pk_table_name": "CUSTOMERS", "pk_column_name": "ID",
       "fk_database_name": "MYDB", "fk_schema_name": "PUBLIC",
       "fk_table_name": "ORDERS", "fk_column_name": "CUSTOMER_ID",
       "key_sequence": 1, "fk_name": "FK_ORDERS_CUSTOMER", "rely": "false"}]


def test_constraints_are_extracted_into_the_record():
    inv = build_inventory(
        FakeSql(_responses(**{"show primary keys": PK,
                              "show unique keys": UNIQUE,
                              "show imported keys": FK})),
        databases=["MYDB"])
    table = inv["inventory"][0]
    kinds = {c["constraint_type"] for c in table["constraints"]}
    assert kinds == {"PRIMARY KEY", "UNIQUE", "FOREIGN KEY"}
    pk = next(c for c in table["constraints"]
              if c["constraint_type"] == "PRIMARY KEY")
    assert pk["columns"] == ["ORDER_ID"]
    fk = next(c for c in table["constraints"]
              if c["constraint_type"] == "FOREIGN KEY")
    assert fk["references"] == "MYDB.PUBLIC.CUSTOMERS"
    assert fk["referenced_columns"] == ["ID"]


def test_the_constraint_reads_are_show_statements_only():
    fake = FakeSql(_responses(**{"show primary keys": PK,
                                 "show unique keys": UNIQUE,
                                 "show imported keys": FK}))
    build_inventory(fake, databases=["MYDB"])
    issued = [c for c in fake.calls if "keys" in c.lower()]
    assert issued and all(c.strip().lower().startswith("show")
                          for c in issued)


def test_a_composite_key_keeps_its_declared_column_order():
    from snowflake_source.extract.constraints import build_constraints
    rows = [{"database_name": "MYDB", "schema_name": "PUBLIC",
             "table_name": "ORDERS", "column_name": "LINE_NO",
             "key_sequence": 2, "constraint_name": "PK_ORDERS"},
            {"database_name": "MYDB", "schema_name": "PUBLIC",
             "table_name": "ORDERS", "column_name": "ORDER_ID",
             "key_sequence": 1, "constraint_name": "PK_ORDERS"}]
    out = build_constraints(
        FakeSql({"show primary keys": rows, "show unique keys": [],
                 "show imported keys": []}), "MYDB")
    assert out["MYDB.PUBLIC.ORDERS"][0]["columns"] == ["ORDER_ID", "LINE_NO"]


def test_a_constraint_read_that_fails_is_a_note_not_silence():
    notes: list[str] = []
    out = build_constraints_or_fail(notes)
    assert out == {}
    assert len(notes) == 3 and all("keys in database" in n for n in notes)


def build_constraints_or_fail(notes):
    from snowflake_source.extract.constraints import build_constraints

    def denied(sql, params=None):
        raise RuntimeError("insufficient privileges")

    return build_constraints(denied, "MYDB", notes)


def test_r20_names_the_constraints_it_says_are_captured():
    res = build_create_table(
        record([col("ORDER_ID", "NUMBER", "DECIMAL(38,0)")],
               constraints=[{"constraint_type": "PRIMARY KEY",
                             "name": "SYS_PK", "columns": ["ORDER_ID"]}]),
        "bronze.PUBLIC.ORDERS")
    rule = next(r for r in res.rules_applied
                if r.rule_id == "R20_CONSTRAINTS_NOT_EMITTED")
    assert "PRIMARY KEY" in rule.detail and "ORDER_ID" in rule.detail
    # The claim that they are "captured in the inventory" is only allowed
    # because the extractor now populates them.
    assert "CHECK" in rule.detail, "say what Snowflake cannot even declare"


def test_the_plan_carries_the_constraints_it_does_not_create():
    from plan.build import build_plan
    inv = build_inventory(
        FakeSql(_responses(**{"show primary keys": PK,
                              "show unique keys": UNIQUE,
                              "show imported keys": FK})),
        databases=["MYDB"])
    plan = build_plan(inv, {"edges": []})
    entry = plan["can_migrate"][0]
    assert {c["constraint_type"] for c in entry["constraints_not_created"]} \
        == {"PRIMARY KEY", "UNIQUE", "FOREIGN KEY"}


def test_the_whole_payload_keeps_the_properties_end_to_end():
    inventory = {"inventory": [record(
        [col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO",
             comment="the key")],
        source_metadata={"comment": "one row per order"})]}
    plan = {"target_names": {"D.PUBLIC.ORDERS": "bronze.PUBLIC.ORDERS"},
            "waves": [["D.PUBLIC.ORDERS"]], "clone_targets": [],
            "cycles": []}
    payload = build_ddl_payload(inventory, plan)
    stmt = payload["statements"][0]
    assert stmt["description"] == "one row per order"
    assert stmt["expected_columns"][0]["nullable"] is False
    assert stmt["expected_columns"][0]["description"] == "the key"


# ------------- the approved plan names the TARGET, not just the types
#
# Live 2026-09-24, the whole four-job chain on a real cluster. The approved
# ddl_plan.json named
#
#     snowmig_coverage_v2.snowmig_coverage_core.customers
#
# and the structure stage created
#
#     snowmig_coverage_v2.CORE.CUSTOMERS
#
# In `--mode ddl-plan` the stage takes the COLUMN TYPES from the plan and
# the NAMESPACE from the source schema name: `target_schema = args.
# target_schema or schema`, while `columns_from_ddl_plan` keys by
# `(source_schema, table)` off `source_identifier` -- so `target_fqn`, the
# name the reviewer approved, is never read.
#
# Three consequences, all observed live:
#   * what DDL_PLAN.md showed is not what exists;
#   * `--bronze-schema-style db_schema` exists precisely to stop two
#     same-named schemas from different databases merging, and this
#     discards it;
#   * the catalog ended up holding BOTH -- `snowmig_coverage_core.customers`
#     empty from the control-plane deploy, and `core.customers` with 500
#     rows from the notebook -- and copy and reconcile inherited the wrong
#     namespace, so MIGRATION_REPORT.md reported MIGRATED_VERIFIED about a
#     namespace nobody approved.

def _ns_plan(source_ident="DB.SALES.ORDERS", target_fqn="lake.db_sales.orders",
             object_type="TABLE"):
    return {"statements": [{
        "source_identifier": source_ident,
        "target_fqn": target_fqn,
        "object_type": object_type,
        "sql": "CREATE TABLE ...",
        "expected_columns": [{"name": "A", "type": "STRING"}],
    }]}


def test_the_plan_yields_the_target_name_it_carries(structure):
    targets = structure.targets_from_ddl_plan(_ns_plan())
    assert targets[("SALES", "ORDERS")] == ("db_sales", "orders")


def test_a_plan_without_a_target_fqn_yields_nothing_rather_than_a_guess(
        structure):
    plan = _ns_plan()
    del plan["statements"][0]["target_fqn"]
    assert structure.targets_from_ddl_plan(plan) == {}


def test_views_are_not_in_the_target_map(structure):
    assert structure.targets_from_ddl_plan(
        _ns_plan(object_type="VIEW")) == {}


def test_a_two_part_target_is_ignored_rather_than_misread(structure):
    assert structure.targets_from_ddl_plan(
        _ns_plan(target_fqn="db_sales.orders")) == {}


def test_the_plans_catalog_is_carried_too(structure):
    """A plan that targets another catalog is the operator's mistake to see,
    not something to silently rewrite into the one they passed."""
    targets = structure.targets_from_ddl_plan(
        _ns_plan(target_fqn="other_lake.db_sales.orders"))
    assert ("SALES", "ORDERS") in targets
    assert structure.catalogs_from_ddl_plan(_ns_plan(
        target_fqn="other_lake.db_sales.orders")) == {"other_lake"}
