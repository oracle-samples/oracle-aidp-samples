"""One Fabric table, every emitter, one name.

This is the regression guard for the whole change, and it is driven from a
real `inventory` of the bundled demo estate rather than from hand-written
dicts. That is not a style preference. The version of this file that shipped
with the change passed while three of its four claims were false, because
its fixtures were shapes that cannot occur:

  * its `warehouse_ddl` entry omitted the `warehouse` key that
    `inventory/catalog.py` writes on every single real one, which is exactly
    what let the notebook translator silently substitute the default
    lakehouse;
  * it fed the T-SQL rules a three-part name, a shape that occurs zero times
    in the real Warehouse corpus -- DacFx writes `CREATE TABLE dbo.claim`,
    so the rule under test never fired on anything the tool actually reads;
  * it compared three reads and never a write or a DDL target, so the
    dataflow's unqualified `saveAsTable("dim_agent")` went unnoticed;
  * and it wrote the missing `--catalog` thread into itself as "deliberate".

What is asserted here instead is the artifact text the tool writes for the
demo estate, plus the plan that describes it. The four bugs above each break
at least one assertion below; which one is recorded beside it.

`TableSurfaceTests` at the foot of the file covers the fifth version of the
same mistake: the estate is not an exhaustive list of Spark's table-taking
APIs and cannot be. Measured on the demo, every one of its 13 qualified
table references goes through `spark.table`, `saveAsTable` or SQL, and the
two bare names that reach a table-taking API are each correctly refused
(`claims_raw_s3` NB11, `orphan_output` NB13) -- so no notebook in it
addresses one table two ways and an estate-driven assertion could not see
the defect at all. `spark.readStream.table`, `insertInto`, `toTable`,
`writeTo` and `DeltaTable.forName` all resolved to nothing with no finding.
That class therefore drives the translator directly, off the module's own
list of call shapes rather than a copy of it, so the next API added to the
translator is under the invariant the day it is added.

One case this file cannot reach, written down so nobody assumes it does:
every emitter in the bundled estate reads `dbo.claim` from inside the
Warehouse that declares it, so the folder an artifact came out of and the
item the catalog records are the same answer, and an emitter that ignores
the catalog still passes. A read of a table *another* Warehouse declares is
where the two come apart, and the warehouse T-SQL rules used to name it
under their own folder -- `default.AcmeDW.ledger` for a table the catalog
puts in `OtherDW`, with no finding, while the notebook path said
`default.OtherDW.ledger`. Both go through `inventory.catalog.owning_item`
now, and the case is asserted in `test_tsql_catalog.AgreementTests.
test_a_table_another_warehouse_declares_gets_one_name_from_both`; reaching
it from here would mean a second Warehouse in the fixture.

The same gap one tier down, and it is the same gap. The estate's inferred
tables -- `claims_daily`, `claims_agg` -- are recorded against `SalesLake`
and read only from `SalesLake`-bound notebooks, and its `supplied` tier is
empty, so an emitter that discards a `notebook_inferred` or `supplied`
entry's recorded item passes every assertion here as well. One did, until
issue #1's last finding: `owning_item` preferred a recorded item for
`warehouse_ddl` alone. Fixing it moved nothing in this file, nothing in the
demo's 470 raw / 214 distinct findings and not one emitted artifact byte,
which is this docstring's point restated. `test_tsql_catalog.
OwningItemTests` and `AgreementTests` are where the case is asserted;
reaching it from here would mean a second *binding* in the fixture -- a
notebook attached to `ArchiveLake` that reads `claims_daily`.
"""
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.fixtures import demo_workspace_path
from fabric_aidp.inventory.manifest import build_manifest
from fabric_aidp.migrate.runner import migrate
from fabric_aidp.plan.planner import build_plan
from fabric_aidp.translate import m_parser
from fabric_aidp.translate import m_to_pyspark as m

# The one table the whole design document argues about: declared as
# `dbo.claim` in Warehouse `AcmeDW`, read bare from two notebooks (one
# Python, one `%%sql`), and planned as an `aidp_table`.
WAREHOUSE = "AcmeDW"
TABLE = "claim"
WAREHOUSE_ASSET = "warehouse.AcmeDW.table.dbo.claim"
NAMESPACE = "myns"
# The Lakehouse the demo Dataflow reads from and writes back into. A Git
# export cannot resolve this GUID on its own -- the Lakehouse item's
# logicalId is a different identifier from the lakehouseId M navigates by --
# so it is supplied here, which is what makes the write target assertable.
LAKEHOUSE_ID = "22222222-2222-2222-2222-222222222222"
LAKEHOUSE = "SalesLake"
SOURCES = ("notebook", "warehouse", "lakehouse", "dataflow")


def _migrated(catalog=None):
    """inventory -> plan -> migrate over the bundled estate; artifacts back.

    Everything downstream reads the files the tool actually wrote, so a name
    that only exists in a finding message cannot satisfy this file.
    """
    manifest = build_manifest(demo_workspace_path(), SOURCES)
    plan = build_plan(manifest, oci_namespace=NAMESPACE,
                      **({"catalog": catalog} if catalog else {}))
    with TemporaryDirectory() as tmp:
        out = Path(tmp)
        report = migrate(plan, out_dir=out)
        artifacts = {row["asset_id"]: (out / row["output_path"]).read_text("utf-8")
                     for row in report["results"] if row.get("output_path")}
    return plan, report, artifacts


class _Estate:
    """Built once: three CLI verbs over 12 items is slow to repeat."""

    default = None
    custom = None

    @classmethod
    def load(cls):
        if cls.default is None:
            cls.default = _migrated()
            cls.custom = _migrated(catalog="myc")
        return cls.default


def _plan_target(plan, asset_id):
    return next(a["target"] for a in plan["assets"] if a["id"] == asset_id)


# Where a table name can appear in something this tool emits. The filter
# keeps the generated header out: a Dataflow script opens with
# `Query: dim_agent`, which is prose, not a reference.
#
# The write and streaming forms are here for the same reason the read forms
# are: a bare `insertInto("claim")` in an emitted artifact is a second name
# for a table something else calls `default.AcmeDW.claim`, and with only the
# first three markers below this scanner walked straight past it. None of
# them occurs in the demo estate today, so this widening moves no assertion
# here; it is what makes the estate-wide sweeps below -- the namespace one,
# the `--catalog` one and the hyphen one -- true of every artifact rather
# than of the three call shapes the estate happens to use.
_TABLE_CONTEXT = ("spark.table(", "spark.read.table(", "saveAsTable(",
                  ".table(", "insertInto(", "toTable(", "writeTo(",
                  "forName(",
                  "FROM ", "JOIN ", "CREATE TABLE ", "CREATE VIEW ",
                  "INSERT INTO ")


def _names_in(text, table=TABLE):
    """Every name a reference position in `text` gives to `<table>`.

    Bare names count: an unqualified `saveAsTable("dim_agent")` is precisely
    the bug, so it has to come back as a name and fail the comparison rather
    than be filtered out as "not a three-part name".
    """
    found = set()
    for line in text.split("\n"):
        if not any(marker in line for marker in _TABLE_CONTEXT):
            continue
        for token in line.replace('"', " ").replace("'", " ").replace("(", " ") \
                         .replace(")", " ").replace(",", " ").split():
            token = token.strip(";")
            if token == table or token.endswith("." + table):
                found.add(token)
    return found


class OneNameTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan, cls.report, cls.artifacts = _Estate.load()
        cls.expected = _plan_target(cls.plan, WAREHOUSE_ASSET)["name"]

    def test_the_plan_names_the_table_the_way_the_rule_says(self):
        self.assertEqual(self.expected, f"default.{WAREHOUSE}.{TABLE}")

    def test_the_ddl_that_creates_the_table_uses_that_name(self):
        """Fails on I1. DacFx writes `CREATE TABLE [dbo].[claim]`; the rule
        that qualified names only handled three-part ones, so it fired zero
        times across the whole 106-asset demo and the artifact creating the
        table disagreed with the plan describing it."""
        ddl = self.artifacts[WAREHOUSE_ASSET]
        self.assertIn(f"CREATE TABLE {self.expected} (", ddl)
        self.assertEqual(_names_in(ddl.split("(")[0]), {self.expected})

    def test_a_python_notebook_read_uses_that_name(self):
        """Fails on C1. `03_Report_Claims` reads `dbo.claim`; the translator
        discarded the Warehouse the catalog entry names and used the
        notebook's default lakehouse, emitting `default.SalesLake.claim`."""
        self.assertEqual(_names_in(self.artifacts["notebook.03_Report_Claims"]),
                         {self.expected})

    def test_a_sql_notebook_cell_uses_that_name(self):
        """Fails on C1 as well: `06_Sql_Summary` reaches the same table
        through `FROM dbo.claim` in a `%%sql` cell."""
        self.assertEqual(_names_in(self.artifacts["notebook.06_Sql_Summary"]),
                         {self.expected})

    def test_every_emitter_agrees_on_exactly_one_name(self):
        names = set()
        for asset_id in (WAREHOUSE_ASSET, "notebook.03_Report_Claims",
                         "notebook.06_Sql_Summary"):
            names |= _names_in(self.artifacts[asset_id])
        names.add(self.expected)
        self.assertEqual(names, {self.expected},
                         f"one table, {len(names)} names: {sorted(names)}")

    def test_a_view_and_the_table_it_reads_agree(self):
        """The view body is the other half of the DDL case: `v_open_claims`
        selects `FROM dbo.claim`, and must name what `claim.sql` created."""
        view = self.artifacts["warehouse.AcmeDW.view.dbo.v_open_claims"]
        self.assertIn(f"FROM {self.expected}", view)

    def test_the_oci_namespace_is_in_no_table_name(self):
        """`--namespace` is object storage. A Dataflow used to put it in the
        catalog position, which is what section 4 of the spec is about."""
        for asset_id, text in self.artifacts.items():
            for name in _names_in(text):
                with self.subTest(asset=asset_id, name=name):
                    self.assertNotIn(NAMESPACE, name)


class CatalogReachesEveryEmitterTests(unittest.TestCase):
    """Fails on C2. `plan --catalog myc` renamed tables in the plan, the
    warehouse SQL and the dataflows; the notebook translator had no
    AIDP-catalog parameter at all -- its `catalog=` is the tier-resolution
    map -- so notebooks kept saying `default`."""

    @classmethod
    def setUpClass(cls):
        _Estate.load()
        cls.plan, cls.report, cls.artifacts = _Estate.custom

    def test_the_plan_records_it(self):
        self.assertEqual(self.plan["target_aidp"]["catalog"], "myc")
        self.assertEqual(_plan_target(self.plan, WAREHOUSE_ASSET)["name"],
                         f"myc.{WAREHOUSE}.{TABLE}")

    def test_no_artifact_anywhere_still_says_default(self):
        for asset_id, text in self.artifacts.items():
            for name in _names_in(text) | _names_in(text, "claims_daily") \
                    | _names_in(text, "claims_agg") | _names_in(text, "agent"):
                with self.subTest(asset=asset_id, name=name):
                    self.assertTrue(name.startswith("myc."),
                                    f"{name!r} ignores --catalog")

    def test_the_notebooks_use_it(self):
        for asset_id in ("notebook.03_Report_Claims", "notebook.06_Sql_Summary"):
            with self.subTest(asset=asset_id):
                self.assertEqual(_names_in(self.artifacts[asset_id]),
                                 {f"myc.{WAREHOUSE}.{TABLE}"})

    def test_the_warehouse_ddl_uses_it(self):
        self.assertIn(f"CREATE TABLE myc.{WAREHOUSE}.{TABLE} (",
                      self.artifacts[WAREHOUSE_ASSET])


class QuotedNameTests(unittest.TestCase):
    """Fails on I2. The naming helper stripped every quote off its inputs
    and put none back, so a part that is not a plain identifier came out as
    an expression -- `on-prem-wh` is a subtraction Spark evaluates happily.
    The bundled estate has no such item name, so the catalog is used as the
    part that needs quoting; the mechanism is the same one."""

    @classmethod
    def setUpClass(cls):
        cls.plan, cls.report, cls.artifacts = _migrated(catalog="my-cat")

    def test_the_plan_quotes_the_part_that_needs_it(self):
        self.assertEqual(_plan_target(self.plan, WAREHOUSE_ASSET)["name"],
                         f"`my-cat`.{WAREHOUSE}.{TABLE}")

    def test_the_artifacts_quote_it_the_same_way(self):
        self.assertIn(f"CREATE TABLE `my-cat`.{WAREHOUSE}.{TABLE} (",
                      self.artifacts[WAREHOUSE_ASSET])
        self.assertEqual(_names_in(self.artifacts["notebook.03_Report_Claims"]),
                         {f"`my-cat`.{WAREHOUSE}.{TABLE}"})

    def test_no_emitted_name_has_a_bare_hyphen_in_it(self):
        for asset_id, text in self.artifacts.items():
            for name in _names_in(text) | _names_in(text, "agent") \
                    | _names_in(text, "claims_daily") | _names_in(text, "claims_agg"):
                with self.subTest(asset=asset_id, name=name):
                    self.assertTrue(name.startswith("`my-cat`."),
                                    f"{name!r} would parse as arithmetic")


@unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
class DataflowWriteTests(unittest.TestCase):
    """Fails on C3. The generated Dataflow script qualified every read and
    left its one write as `saveAsTable("dim_agent")`, so the write landed
    wherever the session default pointed -- the destructive side of the
    same bug."""

    @classmethod
    def setUpClass(cls):
        manifest = build_manifest(demo_workspace_path(), ("dataflow",))
        flows = manifest["sources"]["dataflow"]["items"]["dataflows"]
        queries = {q["name"]: q for f in flows for q in f["queries"]}
        cls.query = queries["dim_agent"]
        cls.helper = cls.query.get("helper")
        if cls.query.get("parsed") is None:
            raise unittest.SkipTest("the bundled dataflow did not parse")

    def _translate(self, **kw):
        helper = self.helper
        return m.translate_query(
            self.query["parsed"],
            queries_by_name={helper["name"]: helper} if helper else {},
            namespace=NAMESPACE, lakehouses={LAKEHOUSE_ID: LAKEHOUSE},
            source="Agents_Dim/mashup.pq", **kw).translated_sql

    def test_the_write_is_fully_qualified(self):
        self.assertIn(f'saveAsTable("default.{LAKEHOUSE}.dim_agent")',
                      self._translate())

    def test_the_write_matches_the_name_the_rule_says(self):
        """Spec section 3, verbatim: Dataflow writing into `SalesLake` ->
        `default.SalesLake.dim_agent`."""
        self.assertEqual(_names_in(self._translate(), "dim_agent"),
                         {f"default.{LAKEHOUSE}.dim_agent"})

    def test_the_read_and_the_write_are_built_the_same_way(self):
        body = self._translate()
        self.assertIn(f'spark.table("default.{LAKEHOUSE}.agent")', body)
        self.assertIn(f'saveAsTable("default.{LAKEHOUSE}.dim_agent")', body)

    def test_the_catalog_reaches_the_write(self):
        self.assertIn(f'saveAsTable("myc.{LAKEHOUSE}.dim_agent")',
                      self._translate(catalog="myc"))

    def test_the_oci_namespace_is_not_the_catalog(self):
        for name in _names_in(self._translate(), "dim_agent"):
            self.assertNotIn(NAMESPACE, name)


class DestinationLakehouseTests(unittest.TestCase):
    """The write uses the *destination's* lakehouse, not the source's.

    The class above cannot tell those apart: the bundled Dataflow reads and
    writes the same lakehouse, so "the destination lakehouse is dropped and
    the source's reused" and "the destination lakehouse is honoured" produce
    identical text there. This drives the two apart with a
    `DataDestinations` helper naming a different lakehouse, which is the
    shape the claim was about.
    """

    SRC, DEST = "lh-src", "lh-dest"
    CATALOG = {SRC: "SalesLake", DEST: "GoldLake"}
    ATTRS = ('[DataDestinations = {[Definition = [Kind = "Reference", '
             'QueryName = "dim_agent_DataDestination", IsNewTarget = true], '
             'Settings = [Kind = "Automatic", UpdateMethod = '
             '[Kind = "Replace"]]]}]')

    def _steps(self, table, lakehouse):
        def step(name, fn, args=(), nav=()):
            return {"name": name, "fn": fn, "args": list(args),
                    "nav": list(nav), "inputs": [], "raw": ""}
        return [step("P", "Lakehouse.Contents", ["[]"]),
                step("N", "P", nav=['{[lakehouseId = "%s"]}' % lakehouse,
                                    "[Data]"]),
                step("T", "N", nav=['{[Id = "%s", ItemKind = "Table"]}' % table,
                                    "[Data]"])]

    def _translate(self, **kw):
        helper = {"name": "dim_agent_DataDestination", "attrs": None,
                  "steps": self._steps("dim_agent", self.DEST), "final": "T"}
        query = {"name": "Q", "attrs": self.ATTRS,
                 "steps": self._steps("agent", self.SRC), "final": "T"}
        return m.translate_query(
            query, queries_by_name={helper["name"]: helper},
            namespace=NAMESPACE,
            lakehouses=kw.pop("lakehouses", self.CATALOG), **kw)

    def test_the_write_lands_in_the_destination_lakehouse(self):
        body = self._translate().translated_sql
        self.assertIn('spark.table("default.SalesLake.agent")', body)
        self.assertIn('saveAsTable("default.GoldLake.dim_agent")', body)

    def test_it_is_the_name_aidp_table_builds(self):
        from fabric_aidp.naming import aidp_table
        self.assertIn(
            'saveAsTable("%s")' % aidp_table("GoldLake", "dim_agent"),
            self._translate().translated_sql)

    def test_the_oci_namespace_reaches_no_part_of_the_write(self):
        for name in _names_in(self._translate().translated_sql, "dim_agent"):
            self.assertNotIn(NAMESPACE, name)

    def test_a_destination_lakehouse_that_does_not_resolve_is_flagged(self):
        """Two parts, and never a silent rewrite: a write with no schema
        lands in whatever database the session points at, which is the
        destructive half of getting a name wrong."""
        result = self._translate(lakehouses={self.SRC: "SalesLake"})
        self.assertIn('saveAsTable("default.dim_agent")',
                      result.translated_sql)
        destination = [f for f in result.findings if f.rule == "M12_DESTINATION"]
        self.assertEqual([f.severity for f in destination], ["flag"])


class ReportShapeTests(unittest.TestCase):
    """The counts the demo is judged on, asserted where a name change would
    show up first."""

    @classmethod
    def setUpClass(cls):
        cls.plan, cls.report, _artifacts = _Estate.load()

    def test_nothing_errored(self):
        self.assertEqual(self.report["counts"].get("error", 0), 0,
                         json.dumps(self.report["counts"]))


# One spelling of every Spark API that takes a table name, keyed by the
# method the translator matches on. The write and streaming halves are the
# point: `spark.table` resolved and `insertInto`, `writeTo`, `toTable`,
# `readStream.table` and `DeltaTable.forName` did not, so a notebook that
# read with the first and wrote with any of the others shipped two names
# for one table in one file.
#
# The `spark.catalog.*` surfaces are generated from the translator's own
# tuple rather than listed, and `test_no_call_shape_is_added_without_a_case`
# checks the hand-written half against the other tuple -- so a method added
# to the translator and not to this file fails here rather than silently
# going untested, which is how these five got in.
_READ_SURFACES = {
    "table": 'df = {recv}.table("{t}")',
    "read.table": 'df = {recv}.read.table("{t}")',
    "readStream.table": 'df = {recv}.readStream.table("{t}")',
    "forName": 'dt = DeltaTable.forName({recv}, "{t}")',
}
_WRITE_SURFACES = {
    "saveAsTable": 'df.write.saveAsTable("{t}")',
    "insertInto": 'df.write.insertInto("{t}")',
    "toTable": 'df.writeStream.toTable("{t}")',
    "writeTo": 'df.writeTo("{t}").append()',
}


def _notebook_with(cells):
    """Fabric notebook source bound to the demo's own lakehouse.

    Written out rather than taken from a fixture because the point is the
    cell *contents*: the binding is the one the bundled notebooks carry, so
    resolution happens exactly as it does for them.
    """
    head = ('# Fabric notebook source\n\n'
            '# METADATA ********************\n\n'
            '# META {\n'
            '# META   "dependencies": {\n'
            '# META     "lakehouse": {\n'
            '# META       "default_lakehouse_name": "%s"\n'
            '# META     }\n'
            '# META   }\n'
            '# META }\n' % LAKEHOUSE)
    body = "".join(
        '\n# CELL ********************\n\n%s\n\n'
        '# METADATA ********************\n\n'
        '# META {\n# META   "language": "python"\n# META }\n' % cell
        for cell in cells)
    return head + body


class TableSurfaceTests(unittest.TestCase):
    """Every Spark API that names a table gives it the same name.

    Driven through `translate` on a whole notebook -- not through the rule
    on its own -- because the defect these guard against is a call shape
    the rule never sees, and only the whole-notebook path proves the rule
    was reached with that shape in it.

    The catalog is the demo estate's real `resolved_catalog`, so `claim` is
    the `warehouse_ddl` entry `AcmeDW` declares, with the `warehouse` key
    `inventory/catalog.py` writes on every real one. See this file's
    docstring for why a hand-written dict is not good enough.
    """

    @classmethod
    def setUpClass(cls):
        from fabric_aidp.inventory.manifest import build_manifest
        cls.catalog = build_manifest(
            demo_workspace_path(), SOURCES)["resolved_catalog"]
        cls.plan, _report, _artifacts = _Estate.load()
        cls.expected = _plan_target(cls.plan, WAREHOUSE_ASSET)["name"]

    def _translate(self, cells, **kw):
        from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
        return nb2spark.translate(
            _notebook_with(cells), namespace=NAMESPACE,
            default_lakehouse=LAKEHOUSE, catalog=self.catalog, **kw)

    def test_no_call_shape_is_added_without_a_case_here(self):
        """The completeness guard. `_TABLE_NAME_METHODS` is the translator's
        own list of methods whose argument is a table name; a method added
        there without a spelling below is an API the invariant has never
        been run on, which is the state all five of the write and streaming
        surfaces were in."""
        from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
        covered = set(_READ_SURFACES) | set(_WRITE_SURFACES)
        self.assertEqual(
            set(nb2spark._TABLE_NAME_METHODS) - covered, set(),
            "a table-taking method was added to the translator with no case "
            "in tests/test_one_name.py")

    def test_every_surface_resolves_to_the_one_name(self):
        surfaces = dict(_READ_SURFACES, **_WRITE_SURFACES)
        for method, spelling in sorted(surfaces.items()):
            line = spelling.format(t=TABLE, recv="spark")
            with self.subTest(method=method, call=line):
                result = self._translate([line])
                self.assertEqual(_names_in(result.translated_sql),
                                 {self.expected},
                                 f"{line!r} -> {result.translated_sql!r}")

    def test_every_spark_catalog_surface_resolves_to_the_one_name(self):
        """Generated from the translator's tuple, so this needs no edit when
        a `spark.catalog` method is added to it."""
        from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
        for method in nb2spark._CATALOG_TABLE_CALLS:
            line = f'spark.catalog.{method}("{TABLE}")'
            with self.subTest(method=method):
                result = self._translate([line])
                self.assertIn(self.expected, result.translated_sql)

    def test_the_session_may_be_bound_to_another_name(self):
        """`ss = SparkSession.builder.getOrCreate()` is ordinary Fabric
        notebook code, and `inventory/catalog.py` already counts `ss.table`
        as a read when it builds the dependency graph. The translator
        anchoring on the literal `spark` meant the inventory and the
        artifact disagreed about the same line."""
        result = self._translate([f'df = ss.table("{TABLE}")'])
        self.assertEqual(_names_in(result.translated_sql), {self.expected})

    def test_a_read_and_a_write_in_one_notebook_agree(self):
        """The invariant itself, one notebook at a time: every pairing of a
        read surface with a write surface has to produce exactly one name.
        Before the fix `spark.table` + `insertInto` produced two -- one
        three-part name and one bare `claim`, which does not exist on
        AIDP."""
        for read, read_spelling in sorted(_READ_SURFACES.items()):
            for write, write_spelling in sorted(_WRITE_SURFACES.items()):
                cells = [read_spelling.format(t=TABLE, recv="spark"),
                         write_spelling.format(t=TABLE)]
                with self.subTest(read=read, write=write):
                    result = self._translate(cells)
                    names = _names_in(result.translated_sql)
                    self.assertEqual(
                        names, {self.expected},
                        f"one table, {len(names)} names: {sorted(names)}")

    def test_the_catalog_flag_reaches_every_surface(self):
        surfaces = dict(_READ_SURFACES, **_WRITE_SURFACES)
        for method, spelling in sorted(surfaces.items()):
            line = spelling.format(t=TABLE, recv="spark")
            with self.subTest(method=method):
                result = self._translate([line], aidp_catalog="myc")
                self.assertEqual(_names_in(result.translated_sql),
                                 {f"myc.{WAREHOUSE}.{TABLE}"})

    def test_a_method_name_that_merely_ends_in_one_of_these_is_not_a_call(self):
        """`saveAsTable` is matched on the method name alone, so the guard
        that keeps `mysaveAsTable(...)` and `soup.tableName(...)` out of it
        is part of the contract."""
        for line in (f'x = obj.mysaveAsTable("{TABLE}")',
                     f'x = soup.tableName("{TABLE}")',
                     f'note = "we call spark.table({TABLE}) in the docs"'):
            with self.subTest(line=line):
                result = self._translate([line])
                self.assertIn(line, result.translated_sql)
                self.assertEqual([f.rule for f in result.findings], [])


if __name__ == "__main__":
    unittest.main()
