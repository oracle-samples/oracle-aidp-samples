"""What each fix changes, and the Spark value that proves it was needed.

Every `spark_old`/`spark_new` below was executed on Spark 4.2.0 against the
input in the first column. The test asserts the emitted source; the values
are here so that changing a mapping means facing what it was measured to do.
"""
import unittest

from fabric_aidp.translate.m_expr import translate_expression

# m expression, input, old emitted, its measured value, new emitted, its value
CASES = [
    ("each Number.Round([x])", "2.5",
     "F.round(F.col('x'), 0)", "3.0",
     "F.bround(F.col('x'), 0)", "2.0"),
    ("each Number.RoundUp([x], 2)", "0.07",
     "F.ceil(F.col('x'))", "1",
     "(F.ceil((F.col('x')).cast('decimal(38,18)') * F.lit(100))"
     " / F.lit(100)).cast('double')", "0.07"),
    ("each Text.Trim([s])", "'  x\\t '",
     "F.trim(F.col('s'))", "'x\\t'",
     r"F.regexp_replace(F.col('s'), r'^\s+|\s+$', '')", "'x'"),
    ("each Date.StartOfWeek([d])", "2024-03-15 (a Friday)",
     "F.trunc(F.col('d'), 'week')", "2024-03-11 (Monday)",
     "_m_start_of_week(df, F.col('d'))", "2024-03-10 (Sunday)"),
    ("each Date.AddDays([d], 1)", "2024-03-15 13:45:30",
     "F.date_add(F.col('d'), F.lit(1))", "2024-03-16 (time lost)",
     "_m_add_days(df, F.col('d'), F.lit(1))", "2024-03-16 13:45:30"),
    ("each Date.EndOfMonth([d])", "2024-03-15 13:45:30",
     "F.last_day(F.col('d'))", "2024-03-31 (time lost)",
     "_m_end_of_month(df, F.col('d'))", "2024-03-31 23:59:59.999999"),
    ("each Text.From([n])", "1e7",
     "(F.col('n')).cast('string')", "'1.0E7'",
     "_m_text(df, F.col('n'))", "'10000000'"),
    ("each Number.From([d])", "2024-03-15 13:45:30",
     "(F.col('d')).cast('double')", "1710510330.0 (epoch seconds)",
     "_m_number(df, F.col('d'))", "TypeError; M gives the serial ~45366.57"),
]


class MeasuredSemanticsTests(unittest.TestCase):
    def test_every_case_emits_its_recorded_new_expression(self):
        for source, value, old, was, new, now in CASES:
            with self.subTest(source=source):
                self.assertEqual(
                    translate_expression(source, frame="df", helpers=set()),
                    new,
                    "%s on %s: was %s -> %s, now expected %s -> %s"
                    % (source, value, old, was, new, now))

    def test_no_case_still_emits_the_old_expression(self):
        for source, _value, old, _was, _new, _now in CASES:
            with self.subTest(source=source):
                self.assertNotEqual(
                    translate_expression(source, frame="df", helpers=set()),
                    old)
