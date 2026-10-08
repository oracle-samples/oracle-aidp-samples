"""Attribute-spelling tolerance outside the parsers, plus the real
Joiner master/detail defect that tolerant reading exposed.

``tests/test_attribute_spellings.py`` covers the *parsers'* own
TABLEATTRIBUTE/JSON-key reads being spelling-tolerant. Eight more call sites --
in converters/, generators/, and handlers/ -- did an exact-match
``tx.properties.get("Some Spelling")`` on the dict those parsers produce,
with the same silent-default failure mode on any casing/spacing variance.
Every test below proves a *value* comes back identically from at least two
spellings of the same concept -- never merely that parsing/conversion
"succeeded".

The Joiner tests at the bottom cover a separate, real correctness defect
that tolerant reading surfaced: "Master Source" is a transformation-level
property that may not resolve at all, and the pre-fix code then guessed
``master_df = "df_source_1"`` -- a literal that may not even be one of the
Joiner's actual inputs, and which silently inverts a Master/Detail Outer
Join whenever the detail source happens to be wired first. The fix adds a
port-level ``is_master`` flag (mirroring the Rust reference implementation's
``Port::is_master`` / ``joiner_detail_upstream``) and, when neither the
property nor the flag resolves a master side, emits a review item instead
of guessing.
"""
from __future__ import annotations

import unittest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.handlers.confidence_scorer import (
    ConfidenceScorer,
    ConversionConfidence,
)
from infa2aidp.models import (
    Connector,
    Mapping,
    Transformation,
    TransformationField,
    TransformationType,
)
from infa2aidp.parsers.iics_parser import IICSParser
from infa2aidp.parsers.xml_parser import InformaticaXMLParser


# ───────────────────────────────────────────── Site 1: confidence_scorer ──

class TestLookupPolicyOnMultipleMatchSpelling(unittest.TestCase):
    """handlers/confidence_scorer.py used to decide `is_unconnected` from
    the PRESENCE of the "Lookup policy on multiple match" property. That
    property is on every PowerCenter Lookup (it is how the export records
    Use First/Last/Any/Report Error), so every connected lookup was scored
    LOW as "Unconnected lookup". Presence of the property, in any spelling,
    must not mean unconnected; the explicit connection marker must."""

    def _issues(self, key: str) -> tuple:
        tx = Transformation(name="LKP_T", type=TransformationType.LOOKUP,
                            lookup_table="T", lookup_condition="LKP_ID = IN_ID")
        tx.properties[key] = "Use First Value"
        cs = ConfidenceScorer().score_transformation(tx)
        return cs.confidence, cs.issues

    def test_title_case_spelling_is_not_unconnected(self):
        confidence, issues = self._issues("Lookup Policy On Multiple Match")
        self.assertNotEqual(confidence, ConversionConfidence.LOW)
        self.assertNotIn("Unconnected lookup — complex join pattern", issues)

    def test_lower_underscore_spelling_is_not_unconnected(self):
        confidence, issues = self._issues("lookup_policy_on_multiple_match")
        self.assertNotEqual(confidence, ConversionConfidence.LOW)
        self.assertNotIn("Unconnected lookup — complex join pattern", issues)

    def test_explicit_unconnected_marker_still_downgrades(self):
        tx = Transformation(name="LKP_T", type=TransformationType.LOOKUP)
        tx.properties["connection_type"] = "unconnected"
        cs = ConfidenceScorer().score_transformation(tx)
        self.assertEqual(cs.confidence, ConversionConfidence.LOW)
        self.assertIn("Unconnected lookup — complex join pattern", cs.issues)


# ───────────────────────────────── Sites 2-4: "Master Source" (read-only) ──

class TestMasterSourcePropertySpellingInJoinerConverter(unittest.TestCase):
    """converters/transformation_converter.py:340 -- only feeds the
    "# Join type: ... | Master: ..." comment now (actual master/detail
    *selection* moved to notebook_generator's port-flag-aware resolver,
    see below), but the read itself must still be spelling-tolerant."""

    def _comment(self, key: str) -> str:
        tx = Transformation(name="JNR_T", type=TransformationType.JOINER)
        tx.join_type = "INNER"
        tx.join_condition = "A_ID = B_ID"
        tx.properties[key] = "SQ_B"
        lines = TransformationConverter().convert(
            tx, input_df="df_a", output_df="df",
            extra_inputs={"SQ_B": "df_b"},
        )
        return "\n".join(lines)

    def test_title_case_spelling_shows_in_comment(self):
        code = self._comment("Master Source")
        self.assertIn("Master: SQ_B", code)
        self.assertNotIn("REVIEW REQUIRED", code)

    def test_lower_underscore_spelling_parses_identically(self):
        code = self._comment("master_source")
        self.assertIn("Master: SQ_B", code)
        self.assertNotIn("REVIEW REQUIRED", code)


class TestMasterSourcePropertySpellingInJoinerSideResolver(unittest.TestCase):
    """generators/notebook_generator.py's `_resolve_joiner_sides` (formerly
    the exact-match read at :411) -- the primary master/detail signal."""

    def _resolve(self, key: str):
        jnr = Transformation(name="JNR_T", type=TransformationType.JOINER)
        jnr.properties[key] = "SQ_B"
        all_preds = [("SQ_A", "df_a"), ("SQ_B", "df_b")]
        mapping = Mapping(name="m", transformations=[jnr], connectors=[])
        return NotebookGenerator._resolve_joiner_sides(jnr, all_preds, mapping)

    def test_title_case_spelling_resolves_master_and_detail(self):
        self.assertEqual(self._resolve("Master Source"), ("SQ_B", "SQ_A"))

    def test_lower_underscore_spelling_parses_identically(self):
        self.assertEqual(self._resolve("master_source"), ("SQ_B", "SQ_A"))


class TestMasterSourcePropertySpellingInResolveInputDf(unittest.TestCase):
    """generators/notebook_generator.py:601 (`_resolve_input_df`'s Joiner
    branch) -- the degenerate-case fallback used when fewer than two
    predecessors resolve. Exercised directly since the normal (>=2
    predecessor) path now bypasses it via `_resolve_joiner_sides`."""

    def _detail_df(self, key: str) -> str:
        jnr = Transformation(name="JNR_T", type=TransformationType.JOINER)
        jnr.properties[key] = "SQ_B"
        back = {"JNR_T": {"SQ_A", "SQ_B"}}
        df_out = {"SQ_A": "df_a", "SQ_B": "df_b"}
        return NotebookGenerator()._resolve_input_df(
            "JNR_T", jnr, back, {}, df_out, [], set()
        )

    def test_title_case_spelling_picks_the_non_master_predecessor(self):
        # "SQ_B" is Master Source -> detail is SQ_A -> "df_a".
        self.assertEqual(self._detail_df("Master Source"), "df_a")

    def test_lower_underscore_spelling_parses_identically(self):
        self.assertEqual(self._detail_df("master_source"), "df_a")


# ──────────────────────────────────────── Site 5: Sequence Generator ──

class TestCurrentValueSpelling(unittest.TestCase):
    """converters/transformation_converter.py:735 -- wrong or missing
    means surrogate keys silently start from 0 instead of the
    Informatica-side high-water mark."""

    def _offset_comment(self, key: str) -> str:
        tx = Transformation(
            name="SEQ_T",
            type=TransformationType.SEQUENCE_GENERATOR,
            fields=[TransformationField(name="NEXTVAL")],
        )
        tx.properties[key] = "500"
        lines = TransformationConverter().convert(tx, input_df="df", output_df="df")
        return "\n".join(lines)

    def test_title_case_spelling_is_read(self):
        self.assertIn("Informatica Current Value: 500", self._offset_comment("Current Value"))

    def test_lower_underscore_spelling_parses_identically(self):
        self.assertIn("Informatica Current Value: 500", self._offset_comment("current_value"))


# ────────────────────────────────────────────────────── Site 6: Sorter ──

class TestSorterDistinctSpelling(unittest.TestCase):
    """converters/transformation_converter.py:827 -- a spelling miss
    silently retains duplicate rows a real export asked to drop."""

    def _code(self, key: str) -> str:
        tx = Transformation(
            name="SRT_T",
            type=TransformationType.SORTER,
            sort_keys=["AMOUNT"],
            sort_direction="ASC",
        )
        tx.properties[key] = "YES"
        lines = TransformationConverter().convert(tx, input_df="df", output_df="df")
        return "\n".join(lines)

    def test_title_case_spelling_applies_dropDuplicates(self):
        self.assertIn(".dropDuplicates()", self._code("Distinct"))

    def test_upper_case_spelling_parses_identically(self):
        self.assertIn(".dropDuplicates()", self._code("DISTINCT"))


# ─────────────────────────────────────────── Site 7: Stored Procedure ──

class TestStoredProcedureNameSpelling(unittest.TestCase):
    """converters/transformation_converter.py:991 -- an empty name yields
    an unusable stub with no indication of the real procedure."""

    def _proc_comment(self, key: str) -> str:
        tx = Transformation(name="SP_T", type=TransformationType.STORED_PROCEDURE)
        tx.properties[key] = "SP_CALC_TOTALS"
        lines = TransformationConverter().convert(tx, input_df="df", output_df="df")
        return "\n".join(lines)

    def test_title_case_spelling_is_read(self):
        self.assertIn("SP_CALC_TOTALS", self._proc_comment("Stored Procedure Name"))

    def test_lower_underscore_spelling_parses_identically(self):
        self.assertIn("SP_CALC_TOTALS", self._proc_comment("stored_procedure_name"))


# ───────────────────────────────────────────────── Site 8: Normalizer ──

class TestNormalizerOccursSpelling(unittest.TestCase):
    """converters/transformation_converter.py:1159 -- wrong explode arity
    when the occurrence count is silently dropped."""

    def _occurs_comment(self, key: str) -> str:
        tx = Transformation(name="NRM_T", type=TransformationType.NORMALIZER)
        tx.properties[key] = "3"
        lines = TransformationConverter().convert(tx, input_df="df", output_df="df")
        return "\n".join(lines)

    def test_title_case_spelling_is_read(self):
        self.assertIn("VSAM normalization: 3 occurrences", self._occurs_comment("Occurs"))

    def test_upper_case_spelling_parses_identically(self):
        self.assertIn("VSAM normalization: 3 occurrences", self._occurs_comment("OCCURS"))


# ───────────────────────── xml_parser.py TRANSACTION_CONTROL branch ──

class TestTransactionControlExpressionSpelling(unittest.TestCase):
    """parsers/xml_parser.py's TRANSACTION_CONTROL branch used
    `table_attrs.get(...)` directly (no fallback at all) -- any spelling
    other than the one exact TABLEATTRIBUTE NAME silently dropped the
    commit-point logic."""

    def _tc_expression(self, ta_name: str) -> str:
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_tc">
    <TRANSFORMATION NAME="TC_T" TYPE="Transaction Control">
        <TABLEATTRIBUTE NAME="{ta_name}" VALUE="IIF(NEW_DAY, TC_COMMIT_BEFORE, TC_CONTINUE_TRANSACTION)"/>
    </TRANSFORMATION>
</MAPPING>"""
        import os
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write(xml)
            path = f.name
        try:
            result = InformaticaXMLParser().parse(path)
        finally:
            os.remove(path)
        return result.mappings[0].transformations[0].properties["tc_expression"]

    def test_title_case_spelling_is_read(self):
        expr = self._tc_expression("Transaction Control Expression")
        self.assertIn("TC_COMMIT_BEFORE", expr)

    def test_lower_underscore_spelling_parses_identically(self):
        expr = self._tc_expression("transaction_control_expression")
        self.assertIn("TC_COMMIT_BEFORE", expr)


# ══════════════════════════ is_master port-flag parsing ══════════════════

class TestXmlIsMasterPortFlag(unittest.TestCase):
    """parsers/xml_parser.py must read MASTER or ISMASTER on a
    TRANSFORMFIELD (PowerCenter exports vary) into TransformationField.is_master."""

    def _is_master(self, attr_name: str, value: str) -> bool:
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_master_flag">
    <TRANSFORMATION NAME="JNR_T" TYPE="Joiner">
        <TRANSFORMFIELD NAME="CUST_ID" DATATYPE="NUMBER" PORTTYPE="INPUT" {attr_name}="{value}"/>
    </TRANSFORMATION>
</MAPPING>"""
        import os
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write(xml)
            path = f.name
        try:
            result = InformaticaXMLParser().parse(path)
        finally:
            os.remove(path)
        return result.mappings[0].transformations[0].fields[0].is_master

    def test_master_attribute_yes_sets_is_master(self):
        self.assertTrue(self._is_master("MASTER", "YES"))

    def test_ismaster_attribute_yes_parses_identically(self):
        self.assertTrue(self._is_master("ISMASTER", "YES"))

    def test_no_value_is_not_master(self):
        self.assertFalse(self._is_master("MASTER", "NO"))


class TestIicsIsMasterPortFlag(unittest.TestCase):
    """parsers/iics_parser.py must read a boolean "master"/"isMaster" flag
    OR a "portGroup"/"group" of "master" into TransformationField.is_master."""

    def test_master_boolean_flag(self):
        field = IICSParser()._parse_field({"name": "CUST_ID", "master": True})
        self.assertTrue(field.is_master)

    def test_isMaster_boolean_flag_parses_identically(self):
        field = IICSParser()._parse_field({"name": "CUST_ID", "isMaster": True})
        self.assertTrue(field.is_master)

    def test_portGroup_master_string_parses_identically(self):
        field = IICSParser()._parse_field({"name": "CUST_ID", "portGroup": "master"})
        self.assertTrue(field.is_master)

    def test_string_false_is_not_master(self):
        """bool("false") is True in Python -- a naive truthiness check on
        a JSON export that serialized the flag as the string "false"
        would silently mark a detail port as master."""
        field = IICSParser()._parse_field({"name": "ORDER_ID", "master": "false"})
        self.assertFalse(field.is_master)

    def test_missing_flag_is_not_master(self):
        field = IICSParser()._parse_field({"name": "ORDER_ID"})
        self.assertFalse(field.is_master)


# ═══════════════ Joiner master/detail: the real defect ═══════

def _joiner_mapping(*, mark_master: bool) -> Mapping:
    """Two Source Qualifiers feed a Joiner. SQ_ORDERS (the DETAIL side) is
    wired FIRST and SQ_CUSTOMERS (the MASTER side) SECOND -- deliberately
    the ordering the Rust reference implementation's docs call out as the one a
    correct implementation must NOT use to infer master/detail (src/ast.rs
    :78-86). No "Master Source" property is set anywhere, so the only
    possible signal is the port-level is_master flag on the Joiner's own
    fields, cross-referenced via CONNECTORs -- when `mark_master` is
    False, neither field is flagged, so no signal exists at all.
    """
    sq_orders = Transformation(name="SQ_ORDERS", type=TransformationType.SOURCE_QUALIFIER)
    sq_customers = Transformation(name="SQ_CUSTOMERS", type=TransformationType.SOURCE_QUALIFIER)

    jnr = Transformation(name="JNR_ORD_CUST", type=TransformationType.JOINER)
    jnr.join_type = "MASTER OUTER"
    jnr.join_condition = "CUST_ID_M = CUST_ID_D"
    jnr.fields = [
        TransformationField(name="CUST_ID_M", is_master=mark_master),
        TransformationField(name="ORDER_ID_D", is_master=False),
    ]

    connectors = [
        Connector(from_instance="SQ_ORDERS", from_field="ORDER_ID",
                  to_instance="JNR_ORD_CUST", to_field="ORDER_ID_D"),
        Connector(from_instance="SQ_CUSTOMERS", from_field="CUST_ID",
                  to_instance="JNR_ORD_CUST", to_field="CUST_ID_M"),
    ]
    return Mapping(
        name="m_join_master_flag",
        transformations=[sq_orders, sq_customers, jnr],
        connectors=connectors,
    )


class TestJoinerMasterResolvedFromPortFlag(unittest.TestCase):
    """A Joiner whose master side is resolvable ONLY from a port flag
    must join with the master (SQ_CUSTOMERS) broadcast and the detail
    (SQ_ORDERS) as the base -- even though the detail source is wired
    first. The pre-fix code inferred detail/master from alphabetically
    sorted predecessor order (`_get_resolved_predecessors` sorts by
    name), which for these two names ("SQ_CUSTOMERS" < "SQ_ORDERS")
    inverts this exact case."""

    def test_join_uses_master_broadcast_and_detail_base_correctly(self):
        mapping = _joiner_mapping(mark_master=True)
        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))

        self.assertNotIn("REVIEW REQUIRED", code)
        # SQ_ORDERS (detail) is df_source (first SQ); SQ_CUSTOMERS
        # (master) is df_source_1 (second SQ). Each side is first renamed
        # to the Joiner's own port names on a per-consumer copy
        # (df_in_<joiner>_<predecessor>), and the join uses those copies.
        self.assertIn("df_in_jnr_ord_cust_sq_customers = _rename_cols(df_source_1, [('CUST_ID', 'CUST_ID_M')])", code)
        self.assertIn("df_in_jnr_ord_cust_sq_orders = _rename_cols(df_source, [('ORDER_ID', 'ORDER_ID_D')])", code)
        self.assertIn("F.broadcast(df_in_jnr_ord_cust_sq_customers)", code)
        self.assertIn("df_in_jnr_ord_cust_sq_orders.join(", code)
        # Must NOT have inverted: the detail side is never the broadcast
        # side, and the master side is never the join's left/base df.
        self.assertNotIn("F.broadcast(df_in_jnr_ord_cust_sq_orders)", code)
        self.assertNotIn("df_in_jnr_ord_cust_sq_customers.join(", code)
        self.assertNotIn("F.broadcast(df_source)", code)


class TestJoinerUnresolvedMasterYieldsReviewItem(unittest.TestCase):
    """When NEITHER the Master Source property NOR any port-level
    MASTER/ISMASTER flag resolves the master side, the converter must
    emit a review item and skip the join -- never guess (and, in
    particular, never silently join against a hardcoded "df_source_1")."""

    def test_no_master_signal_produces_a_review_item_not_a_guessed_join(self):
        mapping = _joiner_mapping(mark_master=False)
        code = "\n".join(NotebookGenerator()._transformation_cells(mapping, {}))

        self.assertIn("REVIEW REQUIRED", code)
        self.assertIn("JNR_ORD_CUST", code)
        # No join was attempted at all -- not even a wrong-direction one.
        self.assertNotIn(".join(", code)
        self.assertNotIn("F.broadcast(", code)


if __name__ == "__main__":
    unittest.main()
