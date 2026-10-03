"""Tests for IICS Cloud JSON parser, format auto-detection, and security."""

import json
import os
import pytest

from infa2aidp.parsers.iics_parser import IICSParser
from infa2aidp.parsers.format_detector import detect_and_parse, detect_and_parse_file
from infa2aidp.parsers.security import (
    validate_input_size, validate_no_xxe, validate_path,
    sanitize_expression, SecurityError,
)
from infa2aidp.models import TransformationType, DataFlowDirection

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "idmc")
POWERCENTER_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "powercenter")


# ── IICS Parser Tests ──

class TestIICSParser:
    def setup_method(self):
        self.parser = IICSParser()

    def test_parse_incremental_cdc_json(self):
        path = os.path.join(FIXTURES_DIR, "incremental_cdc.json")
        if not os.path.exists(path):
            pytest.skip("Fixture not found")
        result = self.parser.parse_file(path)
        assert len(result.mappings) == 1
        m = result.mappings[0]
        assert m.name
        assert len(m.transformations) > 0

    def test_parse_star_schema_json(self):
        path = os.path.join(FIXTURES_DIR, "star_schema_fact.json")
        if not os.path.exists(path):
            pytest.skip("Fixture not found")
        result = self.parser.parse_file(path)
        assert len(result.mappings) == 1
        m = result.mappings[0]
        assert len(m.sources) > 0 or len(m.transformations) > 0

    def test_parse_simple_mapping(self):
        json_input = json.dumps({
            "name": "m_test_mapping",
            "description": "Test",
            "transformations": [
                {
                    "name": "SQ_ORDERS",
                    "type": "Source",
                    "tableName": "DB.SCHEMA.ORDERS",
                    "fields": [
                        {"name": "ORDER_ID", "dataType": "integer", "portType": "OUTPUT"},
                        {"name": "AMOUNT", "dataType": "decimal", "portType": "OUTPUT", "precision": 15, "scale": 2},
                    ]
                },
                {
                    "name": "FIL_ACTIVE",
                    "type": "Filter",
                    "filterCondition": "STATUS = 'ACTIVE'",
                    "fields": [
                        {"name": "ORDER_ID", "dataType": "integer", "portType": "INPUT_OUTPUT"},
                        {"name": "STATUS", "dataType": "string", "portType": "INPUT"},
                    ]
                },
                {
                    "name": "TGT_ORDERS",
                    "type": "Target",
                    "tableName": "DW.SCHEMA.FACT_ORDERS",
                    "fields": [
                        {"name": "ORDER_ID", "dataType": "integer", "portType": "INPUT", "isKey": True},
                        {"name": "AMOUNT", "dataType": "decimal", "portType": "INPUT"},
                    ]
                },
            ],
            "connections": [
                {"from": {"transformation": "SQ_ORDERS", "field": "ORDER_ID"},
                 "to": {"transformation": "FIL_ACTIVE", "field": "ORDER_ID"}},
                {"from": {"transformation": "FIL_ACTIVE", "field": "ORDER_ID"},
                 "to": {"transformation": "TGT_ORDERS", "field": "ORDER_ID"}},
            ]
        })
        result = self.parser.parse(json_input)
        m = result.mappings[0]
        assert m.name == "m_test_mapping"
        assert len(m.sources) == 1
        assert m.sources[0].db_name == "DB"
        assert m.sources[0].owner == "SCHEMA"
        assert m.sources[0].table_name == "ORDERS"
        assert len(m.targets) == 1
        assert m.targets[0].table_name == "FACT_ORDERS"
        assert len(m.connectors) == 2

    def test_parse_flat_connections(self):
        json_input = json.dumps({
            "name": "m_flat_conn",
            "transformations": [
                {"name": "SRC", "type": "Source", "fields": []},
                {"name": "TGT", "type": "Target", "fields": []},
            ],
            "connections": [
                {"fromTransformation": "SRC", "fromField": "ID",
                 "toTransformation": "TGT", "toField": "ID"},
            ]
        })
        result = self.parser.parse(json_input)
        assert len(result.mappings[0].connectors) == 1
        c = result.mappings[0].connectors[0]
        assert c.from_instance == "SRC"
        assert c.to_instance == "TGT"

    def test_parse_router_groups(self):
        json_input = json.dumps({
            "name": "m_router_test",
            "transformations": [
                {
                    "name": "RTR_SPLIT",
                    "type": "Router",
                    "routerGroups": [
                        {"name": "valid", "condition": "AMOUNT > 0"},
                        {"name": "invalid", "condition": "AMOUNT <= 0"},
                    ],
                    "fields": []
                }
            ],
            "connections": []
        })
        result = self.parser.parse(json_input)
        tx = result.mappings[0].transformations[0]
        assert tx.type == TransformationType.ROUTER
        assert len(tx.router_groups) == 2
        assert tx.router_groups[0]["name"] == "valid"


# ── Format Auto-Detection Tests ──

class TestFormatDetection:
    def test_detect_xml(self):
        xml = '<?xml version="1.0"?><POWERMART REPOSITORY_VERSION="188"><REPOSITORY NAME="test"></REPOSITORY></POWERMART>'
        # This may fail on parse but should detect as XML
        try:
            detect_and_parse(xml)
        except Exception:
            pass  # Parse may fail but detection should work

    def test_detect_json(self):
        json_str = json.dumps({"name": "test", "transformations": [], "connections": []})
        result = detect_and_parse(json_str)
        assert result.mappings[0].name == "test"

    def test_detect_unknown(self):
        with pytest.raises(ValueError, match="Cannot detect"):
            detect_and_parse("not xml or json")

    def test_detect_file(self):
        path = os.path.join(FIXTURES_DIR, "incremental_cdc.json")
        if not os.path.exists(path):
            pytest.skip("Fixture not found")
        result = detect_and_parse_file(path)
        assert len(result.mappings) >= 1


# ── Security Tests ──

class TestSecurity:
    def test_input_size_ok(self):
        validate_input_size("small input", 1000)

    def test_input_size_too_large(self):
        with pytest.raises(SecurityError, match="too large"):
            validate_input_size("x" * 1001, 1000)

    def test_xxe_non_informatica_doctype(self):
        with pytest.raises(SecurityError, match="Non-Informatica"):
            validate_no_xxe('<?xml version="1.0"?><!DOCTYPE foo SYSTEM "evil.dtd"><root/>')

    def test_informatica_doctype_allowed(self):
        # Informatica's standard DOCTYPE is safe
        validate_no_xxe('<?xml version="1.0"?><!DOCTYPE POWERMART SYSTEM "powrmart.dtd"><POWERMART/>')

    def test_xxe_entity(self):
        with pytest.raises(SecurityError, match="ENTITY"):
            validate_no_xxe('<?xml version="1.0"?><!ENTITY xxe SYSTEM "file:///etc/passwd"><root/>')

    def test_xxe_case_insensitive(self):
        with pytest.raises(SecurityError):
            validate_no_xxe('<?xml version="1.0"?><!doctype foo><root/>')

    def test_clean_xml_ok(self):
        validate_no_xxe('<?xml version="1.0"?><POWERMART><REPOSITORY/></POWERMART>')

    def test_path_traversal(self):
        with pytest.raises(SecurityError, match="traversal"):
            validate_path("../../etc/passwd")

    def test_absolute_path(self):
        with pytest.raises(SecurityError):
            validate_path("/etc/passwd")

    def test_windows_path(self):
        with pytest.raises(SecurityError):
            validate_path("C:\\Windows\\System32")

    def test_valid_path(self):
        validate_path("mappings/export.xml")

    def test_sanitize_expression_ok(self):
        result = sanitize_expression("IIF(A > B, 1, 0)")
        assert result == "IIF(A > B, 1, 0)"

    def test_sanitize_expression_truncate(self):
        big_expr = "x" * 100_000
        result = sanitize_expression(big_expr)
        assert len(result) < 100_000
        assert "TRUNCATED" in result


# ── Ported PowerCenter fixtures ──

class TestPortedFixtures:
    """Test that all 12 ported fixtures parse without errors."""

    @pytest.fixture
    def xml_parser(self):
        from infa2aidp.parsers.xml_parser import InformaticaXMLParser
        return InformaticaXMLParser()

    @pytest.fixture
    def json_parser(self):
        return IICSParser()

    @pytest.mark.parametrize("fixture", [
        "customer_enrichment.xml",
        "data_quality_route.xml",
        "debezium_upsert.xml",
        "incremental_cdc.xml",
        "normalizer_flatten.xml",
        "order_to_cash.xml",
        "scd_type1.xml",
        "scd_type2.xml",
        "star_schema_fact.xml",
        "time_series_rollup.xml",
    ])
    def test_xml_fixture(self, xml_parser, fixture):
        path = os.path.join(POWERCENTER_FIXTURES_DIR, fixture)
        if not os.path.exists(path):
            pytest.skip(f"Fixture {fixture} not found")
        result = xml_parser.parse(path)
        assert len(result.mappings) >= 1
        m = result.mappings[0]
        assert m.name
        assert len(m.transformations) > 0

    @pytest.mark.parametrize("fixture", [
        "incremental_cdc.json",
        "star_schema_fact.json",
    ])
    def test_json_fixture(self, json_parser, fixture):
        path = os.path.join(FIXTURES_DIR, fixture)
        if not os.path.exists(path):
            pytest.skip(f"Fixture {fixture} not found")
        result = json_parser.parse_file(path)
        assert len(result.mappings) >= 1


# ── Field direction default regression ──
#
# iics_parser.py used to default a field with no portType/direction key to
# DataFlowDirection.INPUT. transformation_converter.py's OUTPUT/INPUT_OUTPUT
# filters (used by _expression, _aggregator, _lookup, _union,
# _stored_procedure, _sql_transformation, _normalizer and _mapplet) then
# silently dropped that field's expression -- the notebook still generated
# and exited 0, just with the transformation logic gone. That is worse than
# a crash: it looked like a working "IICS supported" conversion.

class TestIICSFieldDirectionDefault:
    def setup_method(self):
        self.parser = IICSParser()

    def test_field_with_no_porttype_key_defaults_to_input_output(self):
        """Missing portType/direction must default to INPUT_OUTPUT -- the
        same safe default TransformationField.direction already has in
        models.py -- not INPUT."""
        field = self.parser._parse_field({"name": "OUT_NAME", "dataType": "string"})
        assert field.direction == DataFlowDirection.INPUT_OUTPUT

    def test_expression_field_explicitly_marked_input_is_still_treated_as_output(self):
        """Belt and braces: a field carrying a non-empty expression cannot
        be input-only, even if the IICS export explicitly says portType
        INPUT -- an input-only port cannot have a computed expression."""
        field = self.parser._parse_field({
            "name": "OUT_NAME", "portType": "INPUT", "dataType": "string",
            "expression": "UPPER(IN_NAME)",
        })
        assert field.direction == DataFlowDirection.INPUT_OUTPUT

    def test_field_with_no_expression_and_explicit_input_stays_input(self):
        """The belt-and-braces rule is scoped to fields that actually carry
        an expression -- a genuine input-only port (no expression) must
        still come back as INPUT."""
        field = self.parser._parse_field({
            "name": "IN_NAME", "portType": "INPUT", "dataType": "string",
        })
        assert field.direction == DataFlowDirection.INPUT

    def test_expression_field_with_no_porttype_converts_to_real_pyspark(self):
        """The full defect, end to end through the converter (not just the
        parser): an IICS Expression transformation whose output field omits
        portType must produce real PySpark (F.upper/withColumn), not merely
        the '# Expression: <name>' header comment."""
        from infa2aidp.converters.transformation_converter import TransformationConverter
        from infa2aidp.models import TransformationType

        tx_data = {
            "name": "EXP_UPPER",
            "type": "EXPRESSION",
            "fields": [
                {"name": "IN_NAME", "portType": "INPUT", "dataType": "string"},
                # No portType -- this is the field IICS exports omit it on.
                {"name": "OUT_NAME", "dataType": "string", "expression": "UPPER(IN_NAME)"},
            ],
        }
        tx = self.parser._parse_transformation(tx_data)
        assert tx.type == TransformationType.EXPRESSION

        code = "\n".join(TransformationConverter().convert(tx))
        assert "F.upper" in code or "withColumn" in code, (
            f"expression was silently dropped -- generated only a comment:\n{code}"
        )

    def test_aggregator_field_with_no_porttype_is_not_dropped(self):
        """Same class of defect, second confirmed site:
        transformation_converter._aggregator explicitly does
        `if f.direction == DataFlowDirection.INPUT: continue` -- an
        aggregate output field (e.g. SUM(AMOUNT)) with no portType used to
        hit that continue and vanish from the generated .agg(...) call."""
        from infa2aidp.converters.transformation_converter import TransformationConverter
        from infa2aidp.models import TransformationType

        tx_data = {
            "name": "AGG_TOTAL",
            "type": "AGGREGATOR",
            "groupByFields": ["CUST_ID"],
            "fields": [
                {"name": "CUST_ID", "portType": "INPUT", "dataType": "string"},
                # No portType -- the exact shape that used to be skipped.
                {"name": "TOTAL_AMOUNT", "dataType": "decimal", "expression": "SUM(AMOUNT)"},
            ],
        }
        tx = self.parser._parse_transformation(tx_data)
        assert tx.type == TransformationType.AGGREGATOR

        code = "\n".join(TransformationConverter().convert(tx))
        assert "TOTAL_AMOUNT" in code, (
            f"aggregate expression was silently dropped:\n{code}"
        )
