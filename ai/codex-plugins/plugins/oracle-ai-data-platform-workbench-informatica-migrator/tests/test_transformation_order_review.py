"""A mapping whose transformation order cannot be derived from
CONNECTOR edges must say so, instead of silently falling back to document
position.

Transformation order derives from `<CONNECTOR>` edges, which is verified
correct on 6 fixtures that would otherwise be inverted. But when the DAG cannot be
derived -- no connector edges among the transformations at all, or edges
covering only *some* of them (a partial DAG, which can be confidently
wrong) -- the generator emitted a notebook with no warning whatsoever.

This reuses the same `# REVIEW REQUIRED: ...` marker mechanism
established for an unresolvable Joiner master side (see
transformation_converter.py and test_property_lookup_and_joiner.py) --
not a second mechanism.

Per the brief: do not refuse to generate. A notebook with a loud caveat is
better than no notebook at all -- the defect is silence, not the fallback.
"""
from __future__ import annotations

import copy
import re
import unittest

from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURE = "tests/fixtures/powercenter/order_to_cash.xml"


def _parse(xml_text: str):
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
        f.write(xml_text)
        path = f.name
    try:
        result = InformaticaXMLParser().parse(path)
    finally:
        os.remove(path)
    return result.mappings[0]


def _strip_all_connectors(xml_text: str) -> str:
    return re.sub(r"<CONNECTOR\b[^>]*/>\s*", "", xml_text)


def _strip_connectors_into(xml_text: str, to_instance: str) -> str:
    """Remove only the CONNECTOR lines whose TOINSTANCE matches, leaving
    every other edge intact -- produces a *partial* DAG rather than an
    empty one."""
    return re.sub(
        rf'<CONNECTOR\b[^>]*TOINSTANCE="{to_instance}"[^>]*/>\s*',
        "",
        xml_text,
    )


class TestFullyDerivedOrderIsSilent(unittest.TestCase):
    """The baseline fixture (all CONNECTOR edges present) must NOT trigger
    the new review marker -- order is fully verified from the DAG."""

    def test_no_order_review_item_when_dag_is_complete(self):
        with open(FIXTURE, encoding="utf-8") as f:
            xml_text = f.read()
        mapping = _parse(xml_text)
        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))
        self.assertNotIn("REVIEW REQUIRED", code)


class TestNoConnectorsAtAllIsFlagged(unittest.TestCase):
    """Stripping every CONNECTOR reproduces the reported defect: order
    falls back to document position. It must now emit a review item
    naming the mapping, and it must still produce a notebook (not refuse
    to generate)."""

    def setUp(self):
        with open(FIXTURE, encoding="utf-8") as f:
            self.xml_text = f.read()

    def test_review_marker_present_naming_the_mapping(self):
        mapping = _parse(_strip_all_connectors(self.xml_text))
        self.assertEqual(mapping.connectors, [])
        cells = NotebookGenerator()._transformation_cells(mapping, {})
        code = "\n".join(cells)
        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn(mapping.name, code)
        # It must still generate real transformation cells -- not refuse.
        self.assertTrue(any("FIL_PAID" in c for c in cells))
        self.assertTrue(any("AGG_REVENUE" in c for c in cells))
        self.assertTrue(any("EXP_SEGMENT" in c for c in cells))

    def test_review_marker_says_order_is_unverified(self):
        mapping = _parse(_strip_all_connectors(self.xml_text))
        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))
        lowered = code.lower()
        self.assertIn("unverified", lowered)


class TestPartialConnectorCoverageIsFlagged(unittest.TestCase):
    """The nastier case: connector edges exist and cover *some* of the
    transformations, but not all. An order derived from a partial DAG can
    be confidently wrong -- this must be flagged too, not just the
    fully-empty case."""

    def setUp(self):
        with open(FIXTURE, encoding="utf-8") as f:
            self.xml_text = f.read()

    def test_partial_dag_still_emits_review_item(self):
        # Remove only the edges feeding EXP_SEGMENT (AGG_REVENUE ->
        # EXP_SEGMENT). FIL_PAID -> AGG_REVENUE remains, so the DAG is
        # partially, not fully, derivable among the transformations.
        partial_xml = _strip_connectors_into(self.xml_text, "EXP_SEGMENT")
        mapping = _parse(partial_xml)
        # Sanity: some tx-to-tx edges remain (SQ_ORDERS->FIL_PAID,
        # FIL_PAID->AGG_REVENUE) -- this is a partial, not empty, DAG.
        tx_names = {t.name for t in mapping.transformations}
        remaining_tx_edges = [
            c for c in mapping.connectors
            if c.from_instance in tx_names and c.to_instance in tx_names
        ]
        self.assertTrue(remaining_tx_edges)

        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))
        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn(mapping.name, code)
        self.assertIn("EXP_SEGMENT", code)


if __name__ == "__main__":
    unittest.main()
