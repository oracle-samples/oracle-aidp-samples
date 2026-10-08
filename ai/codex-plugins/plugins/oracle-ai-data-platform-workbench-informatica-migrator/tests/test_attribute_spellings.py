"""Attribute-name spelling tolerance in the XML and IICS parsers (
).

The Rust original these parsers were ported from reads a primary attribute
key *plus fallbacks* -- alternate casings/spacings for the same TABLEATTRIBUTE
concept (PowerCenter XML), or alternate literal JSON key names for the same
concept (IICS). Our port read only the primary spelling in each case, so an
export using any other spelling silently produced an empty field or dropped
the whole construct, with no error. Every test here asserts on a parsed
*value* (or generated code content), never merely that parsing "succeeded" --
a defect in this class would still let the parse complete, just with the
field silently empty.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.models import (
    DataFlowDirection,
    Mapping,
    Session,
    SourceDefinition,
    Transformation,
    TransformationType,
)
from infa2aidp.parsers.iics_parser import IICSParser
from infa2aidp.parsers.xml_parser import InformaticaXMLParser, get_ci, normalize_key


def _parse_xml(xml: str):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
        f.write(xml)
        path = f.name
    try:
        return InformaticaXMLParser().parse(path)
    finally:
        os.remove(path)


def _first_tx(result):
    return result.mappings[0].transformations[0]


# ─────────────────────────────────────────────────────────────── helpers ──

class TestNormalizeKeyAndGetCi(unittest.TestCase):
    """The shared canonicalization helper both xml_parser.py and
    notebook_generator.py's Source Table lookup are built on."""

    def test_normalize_key_collapses_case_and_underscore(self):
        self.assertEqual(normalize_key("Sql Query"), "sql query")
        self.assertEqual(normalize_key("SQL_QUERY"), "sql query")
        self.assertEqual(normalize_key("sql   query"), "sql query")

    def test_get_ci_matches_any_listed_spelling_regardless_of_source_casing(self):
        table_attrs = {"SQL_QUERY": "SELECT 1"}
        self.assertEqual(get_ci(table_attrs, "sql query", "sqlovrd"), "SELECT 1")

    def test_get_ci_returns_default_when_no_spelling_matches(self):
        self.assertEqual(get_ci({}, "sql query", default="fallback"), "fallback")


# ───────────────────────────────────────────────────── XML: TABLEATTRIBUTE ──

class TestXmlAttributeSpellingTolerance(unittest.TestCase):
    """Each concept, parsed once per accepted spelling, asserting the two
    parses land on the identical value."""

    def _sql_override(self, ta_name: str) -> str:
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_sq">
    <TRANSFORMATION NAME="SQ_T" TYPE="Source Qualifier">
        <TABLEATTRIBUTE NAME="{ta_name}" VALUE="SELECT * FROM ORDERS"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml)).sql_override

    def test_sql_query_title_case(self):
        self.assertEqual(self._sql_override("Sql Query"), "SELECT * FROM ORDERS")

    def test_sql_query_lower_underscore_spelling_parses_identically(self):
        self.assertEqual(self._sql_override("sql_query"), "SELECT * FROM ORDERS")

    def test_sqlovrd_alias_also_accepted(self):
        # The alternate token upstream accepts alongside "sql query"
        # (powercentre.rs:219) -- a genuinely different word, not a casing
        # variant, so it needs its own listed spelling.
        self.assertEqual(self._sql_override("sqlovrd"), "SELECT * FROM ORDERS")

    def _filter_condition(self, ta_name: str) -> str:
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_filt">
    <TRANSFORMATION NAME="FIL_T" TYPE="Filter">
        <TABLEATTRIBUTE NAME="{ta_name}" VALUE="STATUS = 'ACTIVE'"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml)).filter_condition

    def test_filter_condition_title_case(self):
        self.assertEqual(self._filter_condition("Filter Condition"), "STATUS = 'ACTIVE'")

    def test_filter_condition_lower_underscore_spelling_parses_identically(self):
        self.assertEqual(self._filter_condition("filter_condition"), "STATUS = 'ACTIVE'")

    def _joiner(self, cond_name: str, type_name: str):
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_join">
    <TRANSFORMATION NAME="JNR_T" TYPE="Joiner">
        <TABLEATTRIBUTE NAME="{cond_name}" VALUE="A.ID = B.ID"/>
        <TABLEATTRIBUTE NAME="{type_name}" VALUE="LEFT OUTER JOIN"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml))

    def test_join_condition_and_type_title_case(self):
        tx = self._joiner("Join Condition", "Join Type")
        self.assertEqual(tx.join_condition, "A.ID = B.ID")
        self.assertEqual(tx.join_type, "LEFT OUTER JOIN")

    def test_join_condition_bare_condition_spelling_parses_identically(self):
        """Upstream also accepts bare "condition" for a Joiner
        (powercentre.rs:221) -- a genuinely different spelling, distinct
        from the Lookup's own "condition" usage below by transformation
        type, not by attribute name."""
        tx = self._joiner("condition", "join_type")
        self.assertEqual(tx.join_condition, "A.ID = B.ID")
        self.assertEqual(tx.join_type, "LEFT OUTER JOIN")

    def _lookup(self, cond_name: str, sql_name: str, table_name: str):
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_lkp">
    <TRANSFORMATION NAME="LKP_T" TYPE="Lookup">
        <TABLEATTRIBUTE NAME="{cond_name}" VALUE="LKP.ID = SRC.ID"/>
        <TABLEATTRIBUTE NAME="{sql_name}" VALUE="SELECT * FROM DIM_T"/>
        <TABLEATTRIBUTE NAME="{table_name}" VALUE="DIM_T"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml))

    def test_lookup_title_case_spellings(self):
        tx = self._lookup("Lookup condition", "Lookup Sql Override", "Lookup table name")
        self.assertEqual(tx.lookup_condition, "LKP.ID = SRC.ID")
        self.assertEqual(tx.lookup_sql, "SELECT * FROM DIM_T")
        self.assertEqual(tx.lookup_table, "DIM_T")

    def test_lookup_lower_underscore_spellings_parse_identically(self):
        tx = self._lookup(
            "lookup_condition", "lookup_sql_override", "lookup_table_name"
        )
        self.assertEqual(tx.lookup_condition, "LKP.ID = SRC.ID")
        self.assertEqual(tx.lookup_sql, "SELECT * FROM DIM_T")
        self.assertEqual(tx.lookup_table, "DIM_T")

    def test_lookup_bare_condition_spelling_matches_real_shipped_fixtures(self):
        """tests/fixtures/corpus/scd_type2.xml and star_schema_fact.xml both
        use bare "condition" for a Lookup's join predicate. Before this fix
        that TABLEATTRIBUTE was never read (only "Lookup condition" was
        accepted) and lookup_condition silently came back "" for those two
        shipped fixtures -- no error, no test catching it."""
        xml = """<?xml version="1.0"?>
<MAPPING NAME="m_lkp_bare">
    <TRANSFORMATION NAME="LKP_CURRENT" TYPE="Lookup">
        <TABLEATTRIBUTE NAME="condition" VALUE="LKP_CURRENT.EMP_ID = SQ_EMPLOYEES.EMP_ID"/>
    </TRANSFORMATION>
</MAPPING>"""
        tx = _first_tx(_parse_xml(xml))
        self.assertEqual(tx.lookup_condition, "LKP_CURRENT.EMP_ID = SQ_EMPLOYEES.EMP_ID")

    def _group_by(self, ta_name: str):
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_agg">
    <TRANSFORMATION NAME="AGG_T" TYPE="Aggregator">
        <TABLEATTRIBUTE NAME="{ta_name}" VALUE="CUST_ID, REGION"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml))

    def test_group_by_title_case(self):
        tx = self._group_by("Group by")
        self.assertEqual(tx.group_by_fields, ["CUST_ID", "REGION"])

    def test_group_by_upper_underscore_spelling_parses_identically(self):
        tx = self._group_by("GROUP_BY")
        self.assertEqual(tx.group_by_fields, ["CUST_ID", "REGION"])

    def _update_strategy(self, ta_name: str):
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_upd">
    <TRANSFORMATION NAME="UPD_T" TYPE="Update Strategy">
        <TABLEATTRIBUTE NAME="{ta_name}" VALUE="IIF(NEW, DD_INSERT, DD_UPDATE)"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml))

    def test_update_strategy_expression_title_case(self):
        tx = self._update_strategy("Update Strategy Expression")
        self.assertEqual(tx.update_strategy_expression, "IIF(NEW, DD_INSERT, DD_UPDATE)")

    def test_update_strategy_expression_lower_underscore_spelling_parses_identically(self):
        tx = self._update_strategy("update_strategy_expression")
        self.assertEqual(tx.update_strategy_expression, "IIF(NEW, DD_INSERT, DD_UPDATE)")

    def _sequence_generator(self, start_name: str, incr_name: str):
        xml = f"""<?xml version="1.0"?>
<MAPPING NAME="m_seq">
    <TRANSFORMATION NAME="SEQ_T" TYPE="Sequence Generator">
        <TABLEATTRIBUTE NAME="{start_name}" VALUE="100"/>
        <TABLEATTRIBUTE NAME="{incr_name}" VALUE="5"/>
    </TRANSFORMATION>
</MAPPING>"""
        return _first_tx(_parse_xml(xml))

    def test_sequence_generator_title_case(self):
        tx = self._sequence_generator("Start Value", "Increment By")
        self.assertEqual(tx.start_value, 100)
        self.assertEqual(tx.increment_by, 5)

    def test_sequence_generator_lower_underscore_spelling_parses_identically(self):
        tx = self._sequence_generator("start_value", "increment_by")
        self.assertEqual(tx.start_value, 100)
        self.assertEqual(tx.increment_by, 5)


class TestRouterGroupConditionUnionOfThree(unittest.TestCase):
    """PowerCenter's DTD spells the Router GROUP condition EXPRESSION;
    CONDITION is seen in the wild (and used in our own
    tests/fixtures/powercenter/data_quality_route.xml); upstream's Rust parser
    reads CONDITION or FILTER_CONDITION. Neither one of us alone reads all
    three, and there is no licensed PowerCenter install to settle which is
    canonical -- read the union of all three, and this test exists so
    nobody "tidies" the union back down to one spelling."""

    ROUTER_XML = """<?xml version="1.0"?>
<MAPPING NAME="m_router_union">
    <TRANSFORMATION NAME="RTR_T" TYPE="Router">
        <GROUP NAME="g_expression" EXPRESSION="AMOUNT > 100"/>
        <GROUP NAME="g_condition" CONDITION="AMOUNT &lt;= 100"/>
        <GROUP NAME="g_filter_condition" FILTER_CONDITION="AMOUNT = 0"/>
    </TRANSFORMATION>
</MAPPING>"""

    def test_all_three_spellings_are_read(self):
        tx = _first_tx(_parse_xml(self.ROUTER_XML))
        groups = {g["name"]: g["condition"] for g in tx.router_groups}
        self.assertEqual(groups["g_expression"], "AMOUNT > 100")
        self.assertEqual(groups["g_condition"], "AMOUNT <= 100")
        self.assertEqual(groups["g_filter_condition"], "AMOUNT = 0")


class TestXmlAttributeNameCaseInsensitivity(unittest.TestCase):
    """Plain XML attribute reads (not TABLEATTRIBUTE values) must also
    tolerate a differently-cased attribute name -- e.g. an export that
    writes ``Name=`` / ``Type=`` instead of ``NAME=`` / ``TYPE=``."""

    def test_mixed_case_transformation_and_field_attributes_are_read(self):
        xml = """<?xml version="1.0"?>
<MAPPING Name="m_mixed_case">
    <TRANSFORMATION Name="EXP_T" Type="Expression">
        <TRANSFORMFIELD Name="OUT_COL" Datatype="string" Porttype="OUTPUT"
                         Expression="UPPER(IN_COL)"/>
    </TRANSFORMATION>
</MAPPING>"""
        result = _parse_xml(xml)
        m = result.mappings[0]
        self.assertEqual(m.name, "m_mixed_case")
        tx = m.transformations[0]
        self.assertEqual(tx.name, "EXP_T")
        self.assertEqual(tx.type, TransformationType.EXPRESSION)
        field = tx.fields[0]
        self.assertEqual(field.name, "OUT_COL")
        self.assertEqual(field.expression, "UPPER(IN_COL)")
        self.assertEqual(field.direction, DataFlowDirection.OUTPUT)


class TestNotebookGeneratorSourceTableSpelling(unittest.TestCase):
    """generators/notebook_generator.py:179's Source Table lookup used to
    be an exact-string match with no fallback -- an export spelling this
    TABLEATTRIBUTE "source_table" silently skipped the Source Qualifier
    WHERE-clause filter with no error."""

    def _generate(self, ta_name: str) -> str:
        src = SourceDefinition(name="ORDERS", table_name="ORDERS")
        sq = Transformation(name="SQ_ORDERS", type=TransformationType.SOURCE_QUALIFIER)
        sq.properties[ta_name] = "ORDERS"
        sq.sql_override = "SELECT * FROM ORDERS WHERE STATUS = 'ACTIVE'"
        mapping = Mapping(name="m_test", sources=[src], transformations=[sq])
        session = Session(name="s_test")
        return NotebookGenerator()._default_source_read(mapping, src, session, 0)

    def test_title_case_source_table_applies_the_where_filter(self):
        code = self._generate("Source Table")
        self.assertIn("STATUS = 'ACTIVE'", code)

    def test_lower_underscore_source_table_applies_the_where_filter_identically(self):
        code = self._generate("source_table")
        self.assertIn("STATUS = 'ACTIVE'", code)


# ─────────────────────────────────────────────────────────────── IICS ──

def _parse_iics(data: dict):
    return IICSParser().parse(json.dumps(data))


class TestIicsAlternateKeyNames(unittest.TestCase):
    """Each concept fromread via its primary key
    and its fallback, asserting the two parses land on the identical
    value."""

    def test_transform_type_primary_key(self):
        tx = IICSParser()._parse_transformation({"name": "FIL_T", "type": "filter"})
        self.assertEqual(tx.type, TransformationType.FILTER)

    def test_transform_type_fallback_key_parses_identically(self):
        tx = IICSParser()._parse_transformation(
            {"name": "FIL_T", "transformationType": "filter"}
        )
        self.assertEqual(tx.type, TransformationType.FILTER)

    def test_transform_type_fallback_key_routes_a_source_definition(self):
        """The _parse_mapping-level source/target special-casing must also
        honor the fallback key -- otherwise an export using
        "transformationType" for a Source never gets routed into
        mapping.sources at all."""
        result = _parse_iics({
            "name": "m_alt_type",
            "transformations": [
                {
                    "name": "SRC_ORD",
                    "transformationType": "source",
                    "tableName": "DB.SCHEMA.ORDERS",
                    "fields": [{"name": "ID", "dataType": "integer"}],
                },
            ],
            "connections": [],
        })
        m = result.mappings[0]
        self.assertEqual(len(m.sources), 1)
        self.assertEqual(m.sources[0].table_name, "ORDERS")

    def test_ports_primary_key(self):
        tx = IICSParser()._parse_transformation({
            "name": "T", "type": "filter",
            "fields": [{"name": "A", "dataType": "string"}],
        })
        self.assertEqual([f.name for f in tx.fields], ["A"])

    def test_ports_fallback_key_parses_identically(self):
        tx = IICSParser()._parse_transformation({
            "name": "T", "type": "filter",
            "ports": [{"name": "A", "dataType": "string"}],
        })
        self.assertEqual([f.name for f in tx.fields], ["A"])

    def test_links_primary_key(self):
        result = _parse_iics({
            "name": "m_links",
            "transformations": [
                {"name": "SRC", "type": "source", "fields": []},
                {"name": "TGT", "type": "target", "fields": []},
            ],
            "connections": [
                {"fromTransformation": "SRC", "fromField": "ID",
                 "toTransformation": "TGT", "toField": "ID"},
            ],
        })
        self.assertEqual(len(result.mappings[0].connectors), 1)

    def test_links_fallback_key_parses_identically(self):
        result = _parse_iics({
            "name": "m_links_alt",
            "transformations": [
                {"name": "SRC", "type": "source", "fields": []},
                {"name": "TGT", "type": "target", "fields": []},
            ],
            "connectors": [
                {"fromTransformation": "SRC", "fromField": "ID",
                 "toTransformation": "TGT", "toField": "ID"},
            ],
        })
        self.assertEqual(len(result.mappings[0].connectors), 1)

    def test_router_groups_primary_key(self):
        tx = IICSParser()._parse_transformation({
            "name": "RTR", "type": "router",
            "routerGroups": [{"name": "g1", "condition": "A > 0"}],
        })
        self.assertEqual(tx.router_groups, [{"name": "g1", "condition": "A > 0"}])

    def test_router_groups_fallback_key_parses_identically(self):
        tx = IICSParser()._parse_transformation({
            "name": "RTR", "type": "router",
            "groups": [{"name": "g1", "condition": "A > 0"}],
        })
        self.assertEqual(tx.router_groups, [{"name": "g1", "condition": "A > 0"}])

    def test_group_condition_primary_key(self):
        tx = IICSParser()._parse_transformation({
            "name": "RTR", "type": "router",
            "routerGroups": [{"name": "g1", "condition": "A > 0"}],
        })
        self.assertEqual(tx.router_groups[0]["condition"], "A > 0")

    def test_group_condition_fallback_key_parses_identically(self):
        tx = IICSParser()._parse_transformation({
            "name": "RTR", "type": "router",
            "routerGroups": [{"name": "g1", "filterCondition": "A > 0"}],
        })
        self.assertEqual(tx.router_groups[0]["condition"], "A > 0")

    def test_connection_ref_primary_key(self):
        tx = IICSParser()._parse_transformation({
            "name": "T", "type": "filter", "connectionName": "ORA_CONN",
        })
        self.assertEqual(tx.properties["connection_ref"], "ORA_CONN")

    def test_connection_ref_fallback_key_parses_identically(self):
        tx = IICSParser()._parse_transformation({
            "name": "T", "type": "filter", "connection": "ORA_CONN",
        })
        self.assertEqual(tx.properties["connection_ref"], "ORA_CONN")

    def test_datatype_dataType_spelling(self):
        field = IICSParser()._parse_field({"name": "A", "dataType": "decimal"})
        self.assertEqual(field.datatype, "decimal")

    def test_datatype_lowercase_spelling_parses_identically(self):
        field = IICSParser()._parse_field({"name": "A", "datatype": "decimal"})
        self.assertEqual(field.datatype, "decimal")

    def test_datatype_type_fallback_spelling_parses_identically(self):
        field = IICSParser()._parse_field({"name": "A", "type": "decimal"})
        self.assertEqual(field.datatype, "decimal")


class TestIicsPortDirectionSpellings(unittest.TestCase):
    """Port direction: IN/OUT/INOUT/VAR short forms, and a "-" normalized
    to "_", must resolve to the same DataFlowDirection as the long forms."""

    def _direction(self, port_type: str) -> DataFlowDirection:
        return IICSParser()._parse_field({"name": "A", "portType": port_type}).direction

    def test_long_and_short_input_forms_match(self):
        self.assertEqual(self._direction("INPUT"), self._direction("IN"))
        self.assertEqual(self._direction("IN"), DataFlowDirection.INPUT)

    def test_long_and_short_output_forms_match(self):
        self.assertEqual(self._direction("OUTPUT"), self._direction("OUT"))
        self.assertEqual(self._direction("OUT"), DataFlowDirection.OUTPUT)

    def test_long_and_short_input_output_forms_match(self):
        self.assertEqual(self._direction("INPUT_OUTPUT"), self._direction("INOUT"))
        self.assertEqual(self._direction("INOUT"), DataFlowDirection.INPUT_OUTPUT)

    def test_hyphenated_spelling_normalizes_to_the_same_direction(self):
        self.assertEqual(self._direction("input-output"), DataFlowDirection.INPUT_OUTPUT)


class TestIicsVariablePortDirection(unittest.TestCase):
    """IICS "variable"/"var" must map to
    DataFlowDirection.VARIABLE, not INPUT_OUTPUT -- the gap left after Task
    21 wired the XML path to VARIABLE but not this one."""

    def test_variable_maps_to_variable_not_input_output(self):
        field = IICSParser()._parse_field({"name": "V_RUN", "portType": "variable"})
        self.assertEqual(field.direction, DataFlowDirection.VARIABLE)

    def test_var_short_form_parses_identically(self):
        field = IICSParser()._parse_field({"name": "V_RUN", "portType": "var"})
        self.assertEqual(field.direction, DataFlowDirection.VARIABLE)

    def test_self_referencing_iics_variable_port_produces_a_review_item(self):
        """The full defect, end to end through the converter: on IICS --
        the primary source format -- a self-referencing variable port
        (v_run = v_run + AMOUNT) must become an explicit review item
        requiring a window function, exactly as it already does on the XML
        path (tests/test_variable_ports.py). Before this fix, IICS mapped
        "variable" to INPUT_OUTPUT, so this silently became an ordinary
        withColumn -- wrong for a running total, with no error."""
        from infa2aidp.converters.transformation_converter import TransformationConverter

        tx = IICSParser()._parse_transformation({
            "name": "EXP_RUNNING_TOTAL",
            "type": "expression",
            "fields": [
                {"name": "AMOUNT", "dataType": "decimal", "portType": "input"},
                {
                    "name": "v_run", "dataType": "decimal", "portType": "variable",
                    "expression": "v_run + AMOUNT",
                },
                {
                    "name": "RUNNING_TOTAL", "dataType": "decimal", "portType": "output",
                    "expression": "v_run",
                },
            ],
        })
        # The parser itself must have produced a VARIABLE-direction field --
        # if this assertion fails, the converter assertions below would be
        # testing nothing (the self-reference check only fires for VARIABLE).
        v_run_field = next(f for f in tx.fields if f.name == "v_run")
        self.assertEqual(v_run_field.direction, DataFlowDirection.VARIABLE)

        # A running total is now translated (a cumulative window over the row
        # order), not a review item -- and never a row-independent withColumn.
        code = "\n".join(TransformationConverter().convert(tx))
        self.assertIn("Stateful variable ports (v_run)", code)
        self.assertIn(".over(_wr)", code)
        self.assertNotIn("F.col('v_run') + ", code)
        # And it must not silently emit a naive row-independent withColumn
        # that would produce a wrong answer for every row.
        v_run_lines = [l for l in code.splitlines() if 'withColumn("v_run"' in l]
        self.assertTrue(v_run_lines, "expected a placeholder withColumn for v_run")
        for line in v_run_lines:
            self.assertIn("F.lit(None)", line)


if __name__ == "__main__":
    unittest.main()
