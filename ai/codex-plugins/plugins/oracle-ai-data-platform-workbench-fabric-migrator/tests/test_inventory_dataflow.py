import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import dataflow
from fabric_aidp.inventory.git_workspace import discover_items
from fabric_aidp.inventory.dataflow import QUERY_METADATA
from fabric_aidp.inventory.manifest import ALL_SOURCES

MASHUP = '''section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "T_DataDestination"]]}]
shared T = let
  Pattern = Lakehouse.Contents([]),
  Nav = Pattern{[Id = "claim", ItemKind = "Table"]}[Data]
in
  Nav;
shared T_DataDestination = let
  Pattern = Lakehouse.Contents([]),
  Nav = Pattern{[Id = "claim", ItemKind = "Table"]}[Data]
in
  Nav;
shared startDate = #date(2024, 1, 1);
'''


def _workspace(tmp, mashup=MASHUP):
    item = Path(tmp) / "Sales.Dataflow"
    item.mkdir(parents=True)
    (item / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Dataflow", "displayName": "Sales"},
        "config": {"logicalId": "11111111-1111-1111-1111-111111111111"}}))
    (item / "mashup.pq").write_text(mashup)
    (item / "queryMetadata.json").write_text(json.dumps(
        {"queriesMetadata": {"T": {"queryName": "T", "loadEnabled": True}}}))
    return discover_items(tmp)


class RegistrationTests(unittest.TestCase):
    def test_dataflow_is_a_known_source(self):
        self.assertIn("dataflow", ALL_SOURCES)


class NoNodeTests(unittest.TestCase):
    """These must pass whether or not Node is installed."""

    def _scan(self):
        with TemporaryDirectory() as tmp:
            return dataflow.scan(_workspace(tmp))

    def test_dataflows_are_counted(self):
        self.assertEqual(self._scan()["summary"]["dataflow_count"], 1)

    def test_parser_state_is_reported(self):
        self.assertIn(self._scan()["summary"]["parser"], ("available", "unavailable"))

    def test_translatable_count_is_none_when_the_parser_is_absent(self):
        summary = self._scan()["summary"]
        if summary["parser"] == "unavailable":
            self.assertIsNone(summary["translatable_count"])
        else:
            self.assertIsInstance(summary["translatable_count"], int)

    def test_a_missing_mashup_is_recorded_not_raised(self):
        with TemporaryDirectory() as tmp:
            items = _workspace(tmp)
            (Path(tmp) / "Sales.Dataflow" / "mashup.pq").unlink()
            result = dataflow.scan(items)
        self.assertEqual(result["summary"]["dataflow_count"], 1)
        self.assertIn("mashup.pq", result["items"]["dataflows"][0]["parse_error"])


class ForcedNoParserTests(unittest.TestCase):
    """The no-Node contract, proven on machines that DO have Node.

    The conditional assertion in NoNodeTests only exercises whichever branch
    this machine happens to be in, so the `None`-not-`0` rule was never
    actually run where Node is installed. This forces it.
    """

    def setUp(self):
        self.real = dataflow.m_parser.parser_available
        dataflow.m_parser.parser_available = lambda *a, **k: False
        self.addCleanup(setattr, dataflow.m_parser, "parser_available", self.real)

    def _scan(self):
        with TemporaryDirectory() as tmp:
            return dataflow.scan(_workspace(tmp))["summary"]

    def test_translatable_count_is_none_not_zero(self):
        summary = self._scan()
        self.assertIsNone(summary["translatable_count"])
        self.assertNotEqual(summary["translatable_count"], 0)

    def test_parser_is_reported_unavailable(self):
        self.assertEqual(self._scan()["parser"], "unavailable")

    def test_queries_are_still_counted_without_the_parser(self):
        # Counting Dataflows is a no-Node capability; losing the count would
        # silently drop them from the inventory.
        self.assertEqual(self._scan()["query_count"], 3)

    def test_dataflows_are_still_counted_without_the_parser(self):
        self.assertEqual(self._scan()["dataflow_count"], 1)


class QueryMetadataTests(unittest.TestCase):
    """queryMetadata.json is valid JSON that is not an object.

    `payload.get("queriesMetadata")` on a list, a string or `null` raises
    AttributeError out of `scan`, and `build_manifest` catches a scanner
    exception per *source*: one malformed file in one Dataflow took the
    whole dataflow source out of the manifest, the plan, the migration and
    verify. Measured on a two-Dataflow workspace where only the first file
    was bad -- dataflows in the manifest went from 2 to 0 for a list, a
    string and null, and stayed 2 for malformed JSON, which was already
    caught.
    """

    def _scan(self, body):
        with TemporaryDirectory() as tmp:
            items = _workspace(tmp)
            (Path(tmp) / "Sales.Dataflow" / "queryMetadata.json").write_text(body)
            return dataflow.scan(items)

    def test_a_list_costs_one_dataflow_not_the_source(self):
        self.assertEqual(self._scan("[]")["summary"]["dataflow_count"], 1)

    def test_a_string_costs_one_dataflow_not_the_source(self):
        self.assertEqual(self._scan('"nope"')["summary"]["dataflow_count"], 1)

    def test_null_costs_one_dataflow_not_the_source(self):
        self.assertEqual(self._scan("null")["summary"]["dataflow_count"], 1)

    def test_malformed_json_costs_one_dataflow_not_the_source(self):
        self.assertEqual(self._scan("{ not json")["summary"]["dataflow_count"], 1)

    def test_the_mashup_is_still_scanned(self):
        # The two files are independent. A Dataflow whose metadata will not
        # read still has queries, and they still migrate.
        record = self._scan("[]")["items"]["dataflows"][0]
        self.assertEqual(record["parse_error"], "")

    def test_each_shape_names_what_is_wrong(self):
        for body, expected in (("[]", "list"), ('"nope"', "str"),
                               ("null", "NoneType"), ("{ not json", "JSON")):
            with self.subTest(body=body):
                record = self._scan(body)["items"]["dataflows"][0]
                self.assertIn(QUERY_METADATA, record["metadata_error"])
                self.assertIn(expected, record["metadata_error"])

    def test_unreadable_metadata_leaves_load_enabled_unknown_not_empty(self):
        # `[]` is a claim: "no query in this Dataflow is load-enabled".
        # Nobody read the file, so the honest value is None.
        self.assertIsNone(self._scan("[]")["items"]["dataflows"][0]["load_enabled"])

    def test_a_queriesmetadata_that_is_not_an_object_is_also_reported(self):
        record = self._scan('{"queriesMetadata": []}')["items"]["dataflows"][0]
        self.assertIn("queriesMetadata", record["metadata_error"])
        self.assertIsNone(record["load_enabled"])

    def test_a_good_file_reports_no_error_and_a_list(self):
        with TemporaryDirectory() as tmp:
            record = dataflow.scan(_workspace(tmp))["items"]["dataflows"][0]
        self.assertEqual(record["metadata_error"], "")
        self.assertEqual(record["load_enabled"], ["T"])

    def test_an_absent_file_is_not_an_error(self):
        # Not every export writes one, and its absence is not a fault.
        with TemporaryDirectory() as tmp:
            items = _workspace(tmp)
            (Path(tmp) / "Sales.Dataflow" / QUERY_METADATA).unlink()
            record = dataflow.scan(items)["items"]["dataflows"][0]
        self.assertEqual(record["metadata_error"], "")
        self.assertIsNone(record["load_enabled"])

    def test_the_summary_counts_the_unreadable_ones(self):
        self.assertEqual(self._scan("[]")["summary"]["metadata_unreadable"], 1)


@unittest.skipUnless(
    __import__("fabric_aidp.translate.m_parser", fromlist=["x"]).parser_available(),
    "Node + mparse not installed (optional)")
class WithNodeTests(unittest.TestCase):
    def _scan(self):
        with TemporaryDirectory() as tmp:
            return dataflow.scan(_workspace(tmp))

    def test_queries_are_counted(self):
        self.assertEqual(self._scan()["summary"]["query_count"], 3)

    def test_helper_and_parameter_queries_are_counted_separately(self):
        summary = self._scan()["summary"]
        self.assertEqual(summary["helper_count"], 1)
        self.assertEqual(summary["parameter_count"], 1)
        self.assertEqual(summary["pipeline_count"], 1)

    def test_helpers_do_not_inflate_the_pipeline_count(self):
        # 23% of real corpus members are *_DataDestination helpers. Counting
        # them as migrations inflated the sibling project's numbers twice.
        summary = self._scan()["summary"]
        self.assertNotEqual(summary["pipeline_count"], summary["query_count"])

    def test_unparseable_mashup_is_recorded_not_raised(self):
        with TemporaryDirectory() as tmp:
            items = _workspace(tmp, "section S; shared x = let in in;")
            result = dataflow.scan(items)
        self.assertTrue(result["items"]["dataflows"][0]["parse_error"])


@unittest.skipUnless(
    __import__("fabric_aidp.translate.m_parser", fromlist=["x"]).parser_available(),
    "Node + mparse not installed (optional)")
class UnreadMemberTests(unittest.TestCase):
    """A query written without `let` used to be counted as a parameter.

    `startDate = #date(2024, 1, 1)` really is one. `Orders =
    Table.FromRows(...)` is a complete query producing a table, and both
    arrive from `parse.js` with `steps: []` -- so both were counted as
    constants and neither reached the plan. Only one of them should.
    """

    MASHUP = MASHUP + 'shared Orders = Table.FromRows({{1, "a"}}, {"id", "n"});\n'

    def _scan(self):
        with TemporaryDirectory() as tmp:
            return dataflow.scan(_workspace(tmp, self.MASHUP))

    def test_it_is_counted_apart_from_the_parameters(self):
        summary = self._scan()["summary"]
        self.assertEqual(summary["unread_member_count"], 1)
        self.assertEqual(summary["parameter_count"], 1)

    def test_it_is_not_counted_as_translatable(self):
        # It produces no PySpark; counting it would overstate the coverage.
        self.assertEqual(self._scan()["summary"]["translatable_count"], 1)

    def test_it_carries_the_parsed_query_so_the_plan_can_report_it(self):
        entry = next(q for q in self._scan()["items"]["dataflows"][0]["queries"]
                     if q["name"] == "Orders")
        self.assertEqual(entry["kind"], "unread_member")
        self.assertIsNotNone(entry.get("parsed"))

    def test_the_plan_gives_it_an_asset_so_it_is_not_silent(self):
        from fabric_aidp.plan.planner import build_plan
        manifest = {"sources": {"dataflow": self._scan()}}
        plan = build_plan(manifest, oci_namespace="ns")
        ids = [a["id"] for a in plan["assets"]]
        self.assertIn("dataflow.Sales.Orders", ids)
        self.assertIn("dataflow.Sales.T", ids)
        self.assertNotIn("dataflow.Sales.startDate", ids)


if __name__ == "__main__":
    unittest.main()
