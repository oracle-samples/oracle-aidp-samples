"""Tests for ExpressionConverter — Informatica to PySpark expression mapping."""

import unittest

from infa2aidp.converters.expression_converter import ExpressionConverter


class TestIIF(unittest.TestCase):
    """IIF(condition, true_val, false_val) -> F.when(...).otherwise(...)"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_iif_isnull(self):
        result = self.conv.convert("IIF(ISNULL(AMOUNT), 0, AMOUNT)")
        self.assertIn("F.when", result)
        self.assertIn("otherwise", result)
        self.assertIn("F.isnull", result)

    def test_iif_simple_comparison(self):
        result = self.conv.convert("IIF(STATUS = 'A', 'Active', 'Inactive')")
        self.assertIn("F.when", result)
        self.assertIn("otherwise", result)


class TestToDate(unittest.TestCase):
    """TO_DATE(str, fmt) -> F.to_timestamp(col, spark_fmt) for type consistency"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_to_date_with_format(self):
        result = self.conv.convert("TO_DATE(DATE_STR, 'MM/DD/YYYY')")
        self.assertIn("F.to_timestamp", result)
        # MM stays MM, DD->dd, YYYY->yyyy
        self.assertIn("MM/dd/yyyy", result)

    def test_to_date_no_format(self):
        result = self.conv.convert("TO_DATE(DATE_STR)")
        self.assertIn("F.to_timestamp", result)


class TestNVL(unittest.TestCase):
    """NVL(expr, default) -> F.coalesce(expr, default)"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_nvl_simple(self):
        result = self.conv.convert("NVL(NAME, 'UNKNOWN')")
        self.assertIn("F.coalesce", result)
        self.assertIn("UNKNOWN", result)

    def test_nvl_numeric(self):
        result = self.conv.convert("NVL(AMOUNT, 0)")
        self.assertIn("F.coalesce", result)


class TestTrimFunctions(unittest.TestCase):
    """LTRIM/RTRIM -> F.ltrim/F.rtrim"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_ltrim_rtrim_nested(self):
        result = self.conv.convert("LTRIM(RTRIM(NAME))")
        self.assertIn("F.ltrim", result)
        self.assertIn("F.rtrim", result)

    def test_trim(self):
        result = self.conv.convert("TRIM(NAME)")
        self.assertIn("F.trim", result)


class TestDecode(unittest.TestCase):
    """DECODE(val, m1, r1, m2, r2, ..., default) -> chained F.when().otherwise()"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_decode_with_default(self):
        result = self.conv.convert("DECODE(STATUS, 'A', 'Active', 'I', 'Inactive', 'Unknown')")
        self.assertIn("F.when", result)
        self.assertIn("otherwise", result)
        self.assertIn("Active", result)
        self.assertIn("Inactive", result)
        self.assertIn("Unknown", result)

    def test_decode_two_pairs(self):
        result = self.conv.convert("DECODE(FLAG, 'Y', 1, 'N', 0)")
        # Two when clauses, no explicit default (should use F.lit(None))
        self.assertIn("F.when", result)


class TestConcatenation(unittest.TestCase):
    """|| operator -> F.concat(...)"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_concat_operator(self):
        result = self.conv.convert("FIRST_NAME || ' ' || LAST_NAME")
        self.assertIn("F.concat", result)


class TestSimpleFunctions(unittest.TestCase):
    """Simple 1:1 function mappings."""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_upper(self):
        result = self.conv.convert("UPPER(NAME)")
        self.assertIn("F.upper", result)

    def test_lower(self):
        result = self.conv.convert("LOWER(NAME)")
        self.assertIn("F.lower", result)

    def test_length(self):
        result = self.conv.convert("LENGTH(NAME)")
        self.assertIn("F.length", result)

    def test_substr(self):
        result = self.conv.convert("SUBSTR(NAME, 1, 5)")
        self.assertIn("F.substring", result)


class TestAggregateFunctions(unittest.TestCase):
    """Aggregate functions used in Aggregator expressions."""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_sum(self):
        result = self.conv.convert("SUM(AMOUNT)")
        self.assertIn("F.sum", result)

    def test_count(self):
        result = self.conv.convert("COUNT(EMPLOYEE_ID)")
        self.assertIn("F.count", result)

    def test_avg(self):
        result = self.conv.convert("AVG(SALARY)")
        self.assertIn("F.avg", result)


class TestSpecialValues(unittest.TestCase):
    """SYSDATE, NULL, etc."""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_sysdate(self):
        result = self.conv.convert("SYSDATE")
        self.assertIn("F.current_timestamp()", result)

    def test_null_literal(self):
        result = self.conv.convert("NULL")
        self.assertIn("F.lit(None)", result)

    def test_parameter_reference(self):
        # Resolved through the notebook's _param() helper (parameters cell),
        # which reads spark.conf under the migration.* namespace with the
        # mapping's DEFAULTVALUE as fallback -- never a bare
        # spark.conf.get('NAME') on a key nothing sets.
        result = self.conv.convert("$$LAST_EXTRACT_DATE")
        self.assertEqual(result, "F.lit(_param('LAST_EXTRACT_DATE'))")

    def test_parameter_in_comparison_is_a_column_expression(self):
        result = self.conv.convert("$$THRESHOLD > 100")
        self.assertEqual(result, "(F.lit(_param('THRESHOLD')) > 100)")

    def test_session_start_time_is_bound_once_not_per_row(self):
        self.assertEqual(self.conv.convert("SESSSTARTTIME"), "F.lit(_SESSION_START_TIME)")
        self.assertEqual(self.conv.convert("$$$SessStartTime"), "F.lit(_SESSION_START_TIME)")

    def test_service_variable_is_refused_not_turned_into_a_column(self):
        from infa2aidp.converters.expression_converter import UnconvertibleExpression
        with self.assertRaises(UnconvertibleExpression):
            self.conv.convert("$PMSessionName")


class TestToChar(unittest.TestCase):
    """TO_CHAR(date, fmt) -> F.date_format(col, spark_fmt)"""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_to_char(self):
        result = self.conv.convert("TO_CHAR(TRX_DATE, 'YYYYMM')")
        self.assertIn("F.date_format", result)


class TestEmptyInput(unittest.TestCase):
    """Edge cases."""

    def setUp(self):
        self.conv = ExpressionConverter()

    def test_empty_string(self):
        result = self.conv.convert("")
        self.assertEqual(result, "")

    def test_whitespace(self):
        result = self.conv.convert("   ")
        self.assertEqual(result, "")

    def test_none(self):
        result = self.conv.convert(None)
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()
