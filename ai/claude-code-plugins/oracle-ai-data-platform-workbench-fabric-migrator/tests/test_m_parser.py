import json
import unittest
from pathlib import Path

from fabric_aidp.translate import m_parser

SECTION = '''section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "T_DataDestination"]]}]
shared T = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "ws-1"]}[Data],
  TableNavigation = Navigation_1{[Id = "T", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
'''

requires_node = unittest.skipUnless(
    m_parser.parser_available(), "Node + mparse not installed (optional)")


class UnquoteIdentifierTests(unittest.TestCase):
    def test_quoted_identifier_is_unwrapped(self):
        self.assertEqual(m_parser.unquote_identifier('#"Promoted headers"'),
                         "Promoted headers")

    def test_plain_identifier_is_unchanged(self):
        self.assertEqual(m_parser.unquote_identifier("Source"), "Source")

    def test_embedded_doubled_quote_is_collapsed(self):
        self.assertEqual(m_parser.unquote_identifier('#"say ""hi"""'), 'say "hi"')

    def test_none_becomes_empty_string(self):
        self.assertEqual(m_parser.unquote_identifier(None), "")


class ParserAvailabilityTests(unittest.TestCase):
    def test_parser_available_returns_a_bool(self):
        self.assertIsInstance(m_parser.parser_available(), bool)

    def test_missing_node_raises_unavailable_not_generic_error(self):
        with self.assertRaises(m_parser.MParserUnavailable):
            m_parser.parse_text("section S; shared a = 1;", node="definitely-not-node")


@requires_node
class ContractTests(unittest.TestCase):
    def test_section_member_yields_a_query_with_steps(self):
        out = m_parser.parse_text(SECTION)
        self.assertTrue(out["ok"])
        names = [q["name"] for q in out["queries"]]
        self.assertEqual(names, ["T"])

    def test_literal_attributes_are_captured_as_attrs(self):
        query = m_parser.parse_text(SECTION)["queries"][0]
        self.assertIn("DataDestinations", query["attrs"])
        self.assertIn("T_DataDestination", query["attrs"])

    def test_navigation_step_fn_is_the_previous_step_name(self):
        steps = {s["name"]: s for s in m_parser.parse_text(SECTION)["queries"][0]["steps"]}
        self.assertEqual(steps["Navigation_1"]["fn"], "Pattern")
        self.assertEqual(steps["Navigation_1"]["inputs"], ["Pattern"])

    def test_table_name_is_in_nav_not_args(self):
        steps = {s["name"]: s for s in m_parser.parse_text(SECTION)["queries"][0]["steps"]}
        nav = " ".join(steps["TableNavigation"]["nav"])
        self.assertIn('Id = "T"', nav)
        self.assertEqual(steps["TableNavigation"]["args"], [])

    def test_every_step_has_all_six_contract_keys(self):
        for step in m_parser.parse_text(SECTION)["queries"][0]["steps"]:
            self.assertEqual(set(step), {"name", "fn", "args", "nav", "inputs", "raw"})

    def test_non_let_member_is_a_constant_not_a_failure(self):
        out = m_parser.parse_text('section S;\nshared d = #date(2024, 1, 1);\n')
        query = out["queries"][0]
        self.assertEqual(query["steps"], [])
        self.assertIn("note", query)

    def test_unparseable_m_raises_mparseerror(self):
        with self.assertRaises(m_parser.MParseError):
            m_parser.parse_text("section S; shared broken = let in in;")


@requires_node
class TheSourcePaneShowsTheQueryTests(unittest.TestCase):
    """The report puts a Dataflow query's source beside its translation.

    It showed the query's `in` step name and nothing else. `translate_query`
    read `raw or final`, and the parser writes `raw` only for a NON-`let`
    member, so every `let` query -- every query that translates -- fell
    through to `final`. MEASURED on the demo: all four translated Dataflow
    queries showed 16-22 characters (`#"Filtered rows"`,
    `#"Changed column type"`) beside a 1944-character mashup.pq.

    `source` is a separate field rather than `raw` on purpose: `raw` is parsed
    downstream as a single value expression, and a `let` carrying one would
    be classified as something else.
    """

    MASHUP = """section Section1;
[Loaded = true]
shared q = let
    Source = #table({"a"}, {{1}}),
    #"Filtered rows" = Table.SelectRows(Source, each [a] > 0)
in
    #"Filtered rows";
shared p = #date(2026, 1, 1);
"""

    @classmethod
    def setUpClass(cls):
        cls.parsed = {q["name"]: q for q in m_parser.parse_text(cls.MASHUP)["queries"]}

    def test_a_let_query_carries_its_whole_member_as_source(self):
        source = self.parsed["q"]["source"]
        self.assertIn("shared q = let", source)
        self.assertIn("Table.SelectRows", source)
        self.assertIn("in", source)

    def test_it_is_far_more_than_the_final_step(self):
        q = self.parsed["q"]
        self.assertEqual(q["final"], '#"Filtered rows"')
        self.assertGreater(len(q["source"]), 4 * len(q["final"]))

    def test_the_member_attributes_are_part_of_it(self):
        """`[DataDestinations = ...]` is written here, on the member, and it is
        the half of a query that says where it writes -- so a reviewer needs
        it in the pane, not just the body."""
        self.assertTrue(self.parsed["q"]["source"].startswith("[Loaded = true]"))

    def test_a_let_query_still_carries_no_raw(self):
        """The field that would have changed classification stays absent."""
        self.assertNotIn("raw", self.parsed["q"])

    def test_a_non_let_member_keeps_raw_and_gains_source(self):
        p = self.parsed["p"]
        self.assertEqual(p["raw"], "#date(2026, 1, 1)")
        self.assertIn("shared p = #date(2026, 1, 1)", p["source"])

    def test_the_translation_result_carries_it(self):
        from fabric_aidp.translate.m_to_pyspark import translate_query
        result = translate_query(self.parsed["q"])
        self.assertIn("Table.SelectRows", result.source_sql)
        self.assertNotEqual(result.source_sql.strip(), '#"Filtered rows"')


if __name__ == "__main__":
    unittest.main()
