"""An Aggregator's GROUP BY key derivation must not silently come
back empty for a bare ``PORTTYPE="INPUT"`` port.

Two shipped fixtures -- order_to_cash.xml's AGG_REVENUE (a per-customer
revenue rollup) and time_series_rollup.xml's AGG_DAILY_STATS (a per-sensor
daily rollup) -- used to resolve ``group_by_fields=[]``. Neither the
"Group by" TABLEATTRIBUTE, nor a PORTTYPE containing "GROUP BY", nor the old
pass-through match (``f.expression.strip() == f.name``) ever fired for them,
so the generated notebook silently collapsed a per-group aggregation into
ONE GLOBAL ROW. It ran, it produced output, and every number was wrong.

The fix, ported from the Rust reference implementation's ``aggregator_group_keys`` /
``expr_references_port`` (src/ast.rs):

1. An explicit per-port group-by flag (GROUPBY/ISGROUPBY on a PowerCenter
   TRANSFORMFIELD; groupBy/isGroupBy on an IICS field) wins outright when
   present, and stops -- no fallback runs.
2. Otherwise, a heuristic: non-aggregated Input-direction ports (no
   expression), excluding any port that is itself consumed by an aggregate
   expression -- matched as a whole identifier (word boundary), so a port
   named AMOUNT is not excluded by an expression that only mentions
   LINE_AMOUNT.
3. If that still resolves nothing, and the Aggregator has aggregate
   expressions, the generated code must carry a ``REVIEW REQUIRED`` marker
   instead of silently emitting a global aggregation.

This file asserts on resolved *values* and on the *generated code text*,
never merely that parsing succeeded.
"""
from __future__ import annotations

import os
import tempfile
import unittest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.models import (
    DataFlowDirection,
    Transformation,
    TransformationField,
    TransformationType,
)
from infa2aidp.parsers.iics_parser import IICSParser
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "powercenter")
ORDER_TO_CASH = os.path.join(FIXTURES_DIR, "order_to_cash.xml")
TIME_SERIES_ROLLUP = os.path.join(FIXTURES_DIR, "time_series_rollup.xml")


def _parse_xml(xml_text: str):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
        f.write(xml_text)
        path = f.name
    try:
        return InformaticaXMLParser().parse(path)
    finally:
        os.remove(path)


def _first_aggregator(mapping):
    aggs = [t for t in mapping.transformations if t.type == TransformationType.AGGREGATOR]
    assert len(aggs) == 1, f"expected exactly one Aggregator, found {len(aggs)}"
    return aggs[0]


def _agg_xml(name: str, fields_xml: str) -> str:
    return f"""<?xml version="1.0"?>
<MAPPING NAME="m_{name}">
    <TRANSFORMATION NAME="{name}" TYPE="Aggregator">
{fields_xml}
    </TRANSFORMATION>
</MAPPING>"""


class TestOrderToCashRevenueAggregatorResolvesGroupKey(unittest.TestCase):
    """The reported defect, fixture 1: a per-customer revenue rollup must
    not collapse into one global row."""

    def test_group_by_fields_is_non_empty_and_correct(self):
        result = InformaticaXMLParser().parse(ORDER_TO_CASH)
        agg = _first_aggregator(result.mappings[0])
        self.assertEqual(agg.name, "AGG_REVENUE")
        self.assertEqual(agg.group_by_fields, ["CUSTOMER_ID"])

    def test_generated_notebook_contains_group_by_on_customer_id(self):
        result = InformaticaXMLParser().parse(ORDER_TO_CASH)
        mapping = result.mappings[0]
        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))
        self.assertIn('.groupBy(F.col("CUSTOMER_ID"))', code)
        # This must be a real per-group aggregation, not a flagged global one.
        self.assertNotIn("REVIEW REQUIRED", code)


class TestTimeSeriesRollupAggregatorResolvesGroupKey(unittest.TestCase):
    """The reported defect, fixture 2: a per-sensor daily rollup must not
    collapse into one global row."""

    def test_group_by_fields_is_non_empty_and_correct(self):
        result = InformaticaXMLParser().parse(TIME_SERIES_ROLLUP)
        agg = _first_aggregator(result.mappings[0])
        self.assertEqual(agg.name, "AGG_DAILY_STATS")
        self.assertEqual(agg.group_by_fields, ["SENSOR_ID", "READING_DATE"])

    def test_generated_notebook_contains_group_by_on_sensor_and_date(self):
        result = InformaticaXMLParser().parse(TIME_SERIES_ROLLUP)
        mapping = result.mappings[0]
        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))
        self.assertIn('.groupBy(F.col("SENSOR_ID"), F.col("READING_DATE"))', code)
        self.assertNotIn("REVIEW REQUIRED", code)


class TestWordBoundaryGuardAgainstSubstringFalsePositive(unittest.TestCase):
    """A port named AMOUNT must not be excluded from the fallback just
    because a *different* port, LINE_AMOUNT, is consumed by an aggregate
    expression -- a naive substring check would wrongly match "AMOUNT"
    inside "LINE_AMOUNT"."""

    def test_amount_survives_while_line_amount_is_excluded(self):
        xml = _agg_xml(
            "AGG_WB",
            """        <TRANSFORMFIELD NAME="AMOUNT" DATATYPE="decimal" PORTTYPE="INPUT"/>
        <TRANSFORMFIELD NAME="LINE_AMOUNT" DATATYPE="decimal" PORTTYPE="INPUT"/>
        <TRANSFORMFIELD NAME="TOTAL" DATATYPE="decimal" PORTTYPE="OUTPUT" EXPRESSION="SUM(LINE_AMOUNT)"/>""",
        )
        agg = _first_aggregator(_parse_xml(xml).mappings[0])
        self.assertIn("AMOUNT", agg.group_by_fields)
        self.assertNotIn("LINE_AMOUNT", agg.group_by_fields)


class TestExplicitGroupByFlagWinsOverHeuristic(unittest.TestCase):
    """An explicit GROUPBY="YES" flag on one port must be honoured exactly
    -- and stop -- even though the fallback heuristic would otherwise also
    pick up a second, unflagged Input port."""

    def test_only_flagged_port_is_used(self):
        xml = _agg_xml(
            "AGG_FLAG",
            """        <TRANSFORMFIELD NAME="REGION" DATATYPE="string" PORTTYPE="INPUT" GROUPBY="YES"/>
        <TRANSFORMFIELD NAME="OTHER_KEY" DATATYPE="string" PORTTYPE="INPUT"/>
        <TRANSFORMFIELD NAME="AMT" DATATYPE="decimal" PORTTYPE="INPUT"/>
        <TRANSFORMFIELD NAME="TOTAL" DATATYPE="decimal" PORTTYPE="OUTPUT" EXPRESSION="SUM(AMT)"/>""",
        )
        agg = _first_aggregator(_parse_xml(xml).mappings[0])
        self.assertEqual(agg.group_by_fields, ["REGION"])


class TestTruthySpellingsAndCaseVariants(unittest.TestCase):
    """GROUPBY/ISGROUPBY must accept the usual Informatica truthy
    spellings -- YES, TRUE, 1, Y -- case-insensitively."""

    def _group_by_fields_for(self, attr_name: str, attr_value: str) -> list:
        xml = _agg_xml(
            "AGG_TRUTHY",
            f"""        <TRANSFORMFIELD NAME="REGION" DATATYPE="string" PORTTYPE="INPUT" {attr_name}="{attr_value}"/>
        <TRANSFORMFIELD NAME="TOTAL" DATATYPE="decimal" PORTTYPE="OUTPUT" EXPRESSION="COUNT(*)"/>""",
        )
        return _first_aggregator(_parse_xml(xml).mappings[0]).group_by_fields

    def test_groupby_yes(self):
        self.assertEqual(self._group_by_fields_for("GROUPBY", "YES"), ["REGION"])

    def test_groupby_lowercase_true(self):
        self.assertEqual(self._group_by_fields_for("GROUPBY", "true"), ["REGION"])

    def test_groupby_numeric_one(self):
        self.assertEqual(self._group_by_fields_for("GROUPBY", "1"), ["REGION"])

    def test_groupby_bare_y_mixed_case(self):
        self.assertEqual(self._group_by_fields_for("GROUPBY", "y"), ["REGION"])

    def test_isgroupby_spelling_uppercase(self):
        self.assertEqual(self._group_by_fields_for("ISGROUPBY", "TRUE"), ["REGION"])

    def test_isgroupby_spelling_mixed_case_yes(self):
        self.assertEqual(self._group_by_fields_for("ISGROUPBY", "Yes"), ["REGION"])

    def test_falsy_value_does_not_flag(self):
        # GROUPBY="NO" must NOT be treated as an explicit flag. If it wrongly
        # resolved truthy, REGION alone would win (per-port flags stop the
        # chain and suppress the fallback); since it correctly resolves
        # false, the fallback heuristic runs instead and picks up BOTH
        # ungrouped Input ports, not just the one carrying the (falsy) attr.
        xml = _agg_xml(
            "AGG_FALSY",
            """        <TRANSFORMFIELD NAME="REGION" DATATYPE="string" PORTTYPE="INPUT" GROUPBY="NO"/>
        <TRANSFORMFIELD NAME="OTHER" DATATYPE="string" PORTTYPE="INPUT"/>
        <TRANSFORMFIELD NAME="TOTAL" DATATYPE="decimal" PORTTYPE="OUTPUT" EXPRESSION="COUNT(*)"/>""",
        )
        agg = _first_aggregator(_parse_xml(xml).mappings[0])
        self.assertEqual(agg.group_by_fields, ["REGION", "OTHER"])


class TestNoDerivableGroupKeyEmitsReviewItem(unittest.TestCase):
    """An Aggregator with aggregate expressions but NO derivable group key
    (every candidate Input port is itself consumed by the aggregate) must
    not silently emit a global aggregation -- it must carry a REVIEW
    REQUIRED marker. A global aggregation can be legitimate (Informatica's
    own no-GROUP-BY semantic), so the code must still be generated, not
    refused."""

    def _build_transformation(self) -> Transformation:
        tx = Transformation(name="AGG_GLOBAL", type=TransformationType.AGGREGATOR)
        tx.fields = [
            TransformationField(
                name="AMOUNT",
                direction=DataFlowDirection.INPUT,
                expression="",
            ),
            TransformationField(
                name="TOTAL",
                direction=DataFlowDirection.OUTPUT,
                expression="SUM(AMOUNT)",
            ),
        ]
        # No TABLEATTRIBUTE-derived, no per-port-flagged, and the fallback
        # heuristic excludes AMOUNT (it's consumed by SUM(AMOUNT)) -- so
        # group_by_fields legitimately comes back empty here.
        tx.group_by_fields = []
        return tx

    def test_review_marker_present(self):
        tx = self._build_transformation()
        lines = TransformationConverter().convert(tx, "df_in", "df_out")
        code = "\n".join(lines)
        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn("AGG_GLOBAL", code)

    def test_still_generates_the_aggregation_not_a_refusal(self):
        tx = self._build_transformation()
        lines = TransformationConverter().convert(tx, "df_in", "df_out")
        code = "\n".join(lines)
        self.assertIn("df_out = df_in.agg(", code)
        self.assertNotIn(".groupBy(", code)


class TestIICSPerFieldGroupByFlag(unittest.TestCase):
    """The IICS JSON equivalent: a per-field groupBy/isGroupBy flag must be
    read and honoured, mirroring the PowerCenter GROUPBY/ISGROUPBY fix."""

    def _mapping_json(self, flag_key: str) -> str:
        return f"""{{
            "name": "m_iics_group_by_flag",
            "transformations": [
                {{
                    "name": "AGG_IICS",
                    "type": "AGGREGATOR",
                    "fields": [
                        {{"name": "REGION", "portType": "INPUT", "{flag_key}": true}},
                        {{"name": "OTHER_KEY", "portType": "INPUT"}},
                        {{"name": "AMT", "portType": "INPUT"}},
                        {{"name": "TOTAL", "portType": "OUTPUT", "expression": "SUM(AMT)"}}
                    ]
                }}
            ]
        }}"""

    def test_group_by_flag(self):
        result = IICSParser().parse(self._mapping_json("groupBy"))
        agg = _first_aggregator(result.mappings[0])
        self.assertEqual(agg.group_by_fields, ["REGION"])

    def test_is_group_by_flag(self):
        result = IICSParser().parse(self._mapping_json("isGroupBy"))
        agg = _first_aggregator(result.mappings[0])
        self.assertEqual(agg.group_by_fields, ["REGION"])

    def test_iics_fallback_still_works_with_no_explicit_flag_or_list(self):
        # No groupByFields list and no per-field flag -- the IICS fallback
        # heuristic must resolve the same way the XML one does.
        result = IICSParser().parse("""{
            "name": "m_iics_fallback",
            "transformations": [
                {
                    "name": "AGG_IICS_FALLBACK",
                    "type": "AGGREGATOR",
                    "fields": [
                        {"name": "CUSTOMER_ID", "portType": "INPUT"},
                        {"name": "LINE_AMOUNT", "portType": "INPUT"},
                        {"name": "TOTAL_REVENUE", "portType": "OUTPUT", "expression": "SUM(LINE_AMOUNT)"}
                    ]
                }
            ]
        }""")
        agg = _first_aggregator(result.mappings[0])
        self.assertEqual(agg.group_by_fields, ["CUSTOMER_ID"])


if __name__ == "__main__":
    unittest.main()
