"""An AIDP schema the tool emits that AIDP's metastore will not create.

MEASURED on an AIDP cluster (Spark 3.5.0), 2026-09-30:

    CREATE SCHEMA IF NOT EXISTS default.`fabric-data-engineering-ws_on-prem-warehouse-test-wh`
      -> MetaException(message:name: fabric-data-engineering-ws_on-prem-
         warehouse-test-wh. Only lower-case characters, numbers and
         underscores are allowed.)
    same for default.`R2 Sales Lake` and default.`r2Ünï`
    CREATE SCHEMA IF NOT EXISTS default.R2MixedCase  -> OK, listed as r2mixedcase

Before these rules the Warehouse and notebook paths emitted such names with a
`rewrite` finding only, so the object graded PASS; only the Dataflow path
(M34) said anything.
"""
import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
from fabric_aidp.translate import tsql_to_spark_sql as tsql

SQ24 = "SQ24_SCHEMA_NOT_ADDRESSABLE"
NB39 = "NB39_SCHEMA_NOT_ADDRESSABLE"
HEADER = "# Fabric notebook source\n\n# CELL ********************\n\n"
CATALOG = {"tables": {
    "dbo.claim": {"tier": "supplied", "name": "dbo.claim"},
}}


def _rules(result):
    return [f.rule for f in result.findings]


class WarehouseSchemaTests(unittest.TestCase):
    ITEM = "on-prem-warehouse-test-wh"

    def test_a_create_under_a_hyphenated_warehouse_is_review_not_pass(self):
        result = tsql.translate("CREATE TABLE dbo.orders (a int)",
                                kind="table", item=self.ITEM)
        self.assertIn("default.`on-prem-warehouse-test-wh`.orders",
                      result.translated_sql)
        self.assertEqual(_rules(result).count(SQ24), 1)
        self.assertTrue(result.needs_manual_review)
        detail = next(f.detail for f in result.findings if f.rule == SQ24)
        self.assertIn(repr(self.ITEM), detail)
        self.assertIn("cannot be created on AIDP", detail)

    def test_the_name_itself_is_not_changed(self):
        """Flag, not rename: the convention is the author's call."""
        before = tsql.translate("SELECT a FROM dbo.t", kind="view",
                                item=self.ITEM).translated_sql
        self.assertIn("`on-prem-warehouse-test-wh`", before)

    def test_one_finding_per_object_naming_every_refused_schema(self):
        result = tsql.translate(
            "SELECT * FROM dbo.a JOIN dbo.b ON 1=1 JOIN sales.c ON 1=1",
            kind="view", item=self.ITEM)
        found = [f for f in result.findings if f.rule == SQ24]
        self.assertEqual(len(found), 1)
        self.assertIn("'on-prem-warehouse-test-wh'", found[0].detail)
        self.assertIn("'on-prem-warehouse-test-wh_sales'", found[0].detail)

    def test_a_three_part_name_into_a_spaced_item_is_flagged(self):
        result = tsql.translate("SELECT * FROM [Sales Lake].dbo.t",
                                kind="view", item="AcmeDW")
        self.assertIn(SQ24, _rules(result))

    def test_mixed_case_is_not_flagged_the_metastore_folds_it(self):
        result = tsql.translate("CREATE TABLE dbo.orders (a int)",
                                kind="table", item="AcmeDW")
        self.assertNotIn(SQ24, _rules(result))
        self.assertFalse(result.needs_manual_review)

    def test_a_schema_that_adds_the_bad_character_is_flagged(self):
        result = tsql.translate("SELECT * FROM [my-schema].t",
                                kind="view", item="AcmeDW")
        detail = next(f.detail for f in result.findings if f.rule == SQ24)
        self.assertIn("'AcmeDW_my-schema'", detail)


class NotebookSchemaTests(unittest.TestCase):
    def _translate(self, code, lakehouse):
        return nb2spark.translate(HEADER + code + "\n", namespace="ns",
                                  default_lakehouse=lakehouse,
                                  catalog=CATALOG)

    def test_a_table_under_a_spaced_lakehouse_is_review_not_pass(self):
        result = self._translate('spark.table("dbo.claim")', "Sales Lake")
        self.assertIn("default.`Sales Lake`.claim", result.translated_sql)
        self.assertEqual(_rules(result).count(NB39), 1)
        self.assertTrue(result.needs_manual_review)

    def test_one_finding_for_many_references(self):
        result = self._translate(
            'spark.table("dbo.claim")\nspark.sql("SELECT * FROM dbo.claim")',
            "Sales Lake")
        self.assertEqual(_rules(result).count(NB39), 1)

    def test_a_tables_path_under_a_hyphenated_lakehouse_is_flagged(self):
        result = self._translate(
            'spark.read.load("/lakehouse/default/Tables/claim")', "my-lake")
        self.assertIn("NB20_TABLES_PATH", _rules(result))
        self.assertIn(NB39, _rules(result))

    def test_a_plain_lakehouse_is_not_flagged(self):
        result = self._translate('spark.table("dbo.claim")', "SalesLake")
        self.assertNotIn(NB39, _rules(result))


def _manifest(lakehouses=(), warehouses=()):
    """A minimal manifest: Lakehouse names, and (warehouse, schema, table)."""
    by_wh: dict = {}
    for name, schema, table in warehouses:
        by_wh.setdefault(name, []).append(
            {"kind": "table", "schema": schema, "name": table,
             "file": f"{schema}/{table}.sql",
             "sql": f"CREATE TABLE {schema}.{table} (a int)"})
    return {"sources": {
        "lakehouse": {"items": {"lakehouses": [
            {"name": n, "shortcuts": []} for n in lakehouses]}},
        "warehouse": {"items": {"warehouses": [
            {"name": n, "objects": objs} for n, objs in by_wh.items()]}},
    }}


class PlanSchemaCollisionTests(unittest.TestCase):
    """Two Fabric containers that land in one AIDP schema.

    MEASURED on an AIDP cluster (Spark 3.5.0), 2026-09-30:
    `CREATE SCHEMA IF NOT EXISTS default.R2MixedCase` succeeded, SHOW
    SCHEMAS listed `r2mixedcase`, and `CREATE SCHEMA default.r2mixedcase`
    then failed [SCHEMA_ALREADY_EXISTS]. Before this check the plan was
    silent and both containers' tables were planned into one schema.
    """

    def _plan(self, **kw):
        from fabric_aidp.plan.planner import build_plan
        return build_plan(_manifest(**kw))

    def _flagged(self, plan):
        return sorted(a["id"] for a in plan["assets"]
                      if any(f["rule"] == "NM01_AIDP_SCHEMA_COLLISION"
                             for f in a.get("plan_findings") or []))

    def test_a_lakehouse_and_a_warehouse_differing_only_in_case(self):
        plan = self._plan(lakehouses=["SalesLake"],
                          warehouses=[("saleslake", "dbo", "claim")])
        collisions = plan["warnings"]["schema_collisions"]
        self.assertEqual([c["aidp_schema"] for c in collisions], ["saleslake"])
        self.assertEqual(self._flagged(plan), [
            "lakehouse.SalesLake", "warehouse.saleslake.table.dbo.claim"])

    def test_an_item_whose_name_is_another_items_item_underscore_schema(self):
        plan = self._plan(warehouses=[("AcmeDW_sales", "dbo", "a"),
                                      ("AcmeDW", "sales", "b")])
        collisions = plan["warnings"]["schema_collisions"]
        self.assertEqual([c["aidp_schema"] for c in collisions],
                         ["acmedw_sales"])
        self.assertEqual(len(collisions[0]["sources"]), 2)

    def test_one_container_is_not_a_collision_with_itself(self):
        plan = self._plan(lakehouses=["SalesLake"],
                          warehouses=[("AcmeDW", "dbo", "a"),
                                      ("AcmeDW", "", "b"),
                                      ("AcmeDW", "sales", "c")])
        self.assertEqual(plan["warnings"]["schema_collisions"], [])
        self.assertEqual(self._flagged(plan), [])

    def test_the_summary_names_the_collision(self):
        from fabric_aidp.plan.planner import summarize_plan
        plan = self._plan(lakehouses=["SalesLake"],
                          warehouses=[("saleslake", "dbo", "claim")])
        self.assertIn("saleslake: Lakehouse 'SalesLake', Warehouse "
                      "'saleslake'", summarize_plan(plan))

    def test_migrate_grades_the_colliding_object_review_not_pass(self):
        import tempfile
        from pathlib import Path
        from fabric_aidp.migrate.runner import migrate
        plan = self._plan(lakehouses=["SalesLake"],
                          warehouses=[("saleslake", "dbo", "claim"),
                                      ("Other", "dbo", "claim")])
        with tempfile.TemporaryDirectory() as out:
            report = migrate(plan, out_dir=Path(out))
        rows = {r["asset_id"]: r for r in report["results"]}
        hit = rows["warehouse.saleslake.table.dbo.claim"]
        self.assertEqual(hit["status"], "needs_manual_review")
        self.assertIn("NM01_AIDP_SCHEMA_COLLISION",
                      [f["rule"] for f in hit["findings"]])
        self.assertEqual(rows["warehouse.Other.table.dbo.claim"]["status"],
                         "ok")


if __name__ == "__main__":
    unittest.main()
