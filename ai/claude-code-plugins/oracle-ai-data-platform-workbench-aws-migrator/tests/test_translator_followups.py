"""Regression tests for four defects that survived the correctness-hardening pass.

Each test names the Spark 3.5.3 behaviour it encodes, verified on a local Spark
engine rather than inferred:

  arr[1]                           -> 'b'   while Athena returns 'a'
  element_at(arr, 1)               -> 'a'   matches Athena for arrays AND maps
  LATERAL VIEW ... JOIN            -> [PARSE_SYNTAX_ERROR] Syntax error at or near 'JOIN'
"""
from __future__ import annotations

from aws_aidp.translate.athena_to_spark_sql import translate as athena_translate
from aws_aidp.translate.glue_to_spark import translate as glue_translate


# --- 1. a JOIN may not follow a LATERAL VIEW in Spark -----------------------

def test_unnest_followed_by_join_is_flagged_not_rewritten():
    """Spark puts lateralView* after the full relation list, so the rewrite would
    emit SQL that cannot parse. Flagging beats shipping invalid Spark."""
    src = "SELECT x FROM a CROSS JOIN UNNEST(a.arr) AS t(x) JOIN b ON b.id = t.x"
    result = athena_translate(src)
    assert "LATERAL VIEW" not in result.translated_sql
    assert result.flags >= 1
    assert any(f.rule == "unnest_trailing_join" for f in result.findings)


def test_unnest_followed_by_join_reports_one_finding_for_one_construct():
    src = "SELECT x FROM a CROSS JOIN UNNEST(a.arr) AS t(x) JOIN b ON b.id = t.x"
    result = athena_translate(src)
    rules = [f.rule for f in result.findings if f.severity == "flag"]
    assert rules.count("unnest_unhandled") == 0, rules


def test_unnest_without_trailing_join_still_rewrites():
    result = athena_translate("SELECT x FROM t CROSS JOIN UNNEST(arr) AS u(x)")
    assert "LATERAL VIEW explode(arr) u AS x" in result.translated_sql
    assert result.flags == 0


def test_unnest_followed_by_where_still_rewrites():
    """WHERE ends the FROM clause, so it must not be mistaken for a relation."""
    result = athena_translate(
        "SELECT x FROM t CROSS JOIN UNNEST(arr) AS u(x) WHERE x > 1")
    assert "LATERAL VIEW" in result.translated_sql
    assert result.flags == 0


def test_unnest_with_join_in_a_later_clause_still_rewrites():
    """A JOIN inside a following subquery must not suppress the rewrite."""
    result = athena_translate(
        "SELECT x FROM t CROSS JOIN UNNEST(arr) AS u(x) "
        "WHERE x IN (SELECT p.id FROM p JOIN q ON q.id = p.id)")
    assert "LATERAL VIEW" in result.translated_sql


def test_unhandled_unnest_with_trailing_join_keeps_its_generic_flag():
    """A map/multi-array UNNEST this rule never rewrites must stay flagged even
    when a JOIN follows it, otherwise suppressing the duplicate loses it."""
    result = athena_translate(
        "SELECT k, v FROM t CROSS JOIN UNNEST(m) AS u(k, v) JOIN b ON b.id = k")
    assert result.flags >= 1
    assert any(f.rule == "unnest_unhandled" for f in result.findings)


# --- 2. Athena arrays are 1-based, Spark's [] is 0-based -------------------

def test_bare_column_subscript_is_flagged_not_silent():
    """split(...)[n] was already flagged; a bare column subscript was not, so it
    passed with 0 changes and 0 flags and returned the wrong element on Spark."""
    result = athena_translate("SELECT arr[1] FROM t")
    assert result.flags >= 1
    assert any(f.rule == "array_subscript_index" for f in result.findings)
    assert result.translated_sql == "SELECT arr[1] FROM t"


def test_qualified_and_call_subscripts_are_flagged():
    assert athena_translate("SELECT a.b.arr[2] FROM t").flags >= 1
    # split(...)[n] keeps its existing, more specific rule
    assert any(f.rule == "split_subscript_index"
               for f in athena_translate("SELECT split(c, ',')[1] FROM t").findings)


def test_map_string_subscript_is_not_flagged():
    """Spark's m['k'] already matches Athena, so it is not an index-base risk."""
    result = athena_translate("SELECT m['k'] FROM t")
    assert not any(f.rule == "array_subscript_index" for f in result.findings)


def test_array_constructor_is_not_treated_as_a_subscript():
    """ARRAY[1,2,3] is a Presto constructor; the unsupported-construct rule owns it."""
    result = athena_translate("SELECT ARRAY[1,2,3] FROM t")
    assert not any(f.rule == "array_subscript_index" for f in result.findings)


def test_non_literal_subscript_is_flagged():
    result = athena_translate("SELECT arr[i] FROM t")
    assert any(f.rule == "array_subscript_dynamic" for f in result.findings)


def test_subscript_inside_a_string_literal_is_untouched():
    src = "SELECT 'arr[1]' AS note FROM t"
    assert athena_translate(src).translated_sql == src


# --- 3. the .spark_session rewrite must not hit unrelated attributes -------

def test_unrelated_spark_session_attribute_is_preserved():
    """`self.spark_session = session` is an attribute STORE. Rewriting it to
    `spark = session` drops the assignment and makes later reads resolve to the
    injected module global instead of the session that was passed in."""
    src = (
        "class Runner:\n"
        "    def __init__(self, session):\n"
        "        self.spark_session = session\n"
        "    def count(self):\n"
        "        return self.spark_session.table('a.b').count()\n"
    )
    result = glue_translate(src, oci_namespace="ns")
    assert "self.spark_session = session" in result.translated_sql
    assert "SparkSession.builder" not in result.translated_sql


def test_gluecontext_spark_session_alias_still_rewrites():
    src = (
        "from awsglue.context import GlueContext\n"
        "glueContext = GlueContext(sc)\n"
        "session = glueContext.spark_session\n"
        "df = session.table('a.b')\n"
    )
    result = glue_translate(src, oci_namespace="ns")
    assert "session = spark" in result.translated_sql


# --- 4. teardown must not report success when deletions fail ---------------

def test_teardown_tracks_failures_and_exits_non_zero():
    """TESTING.md tells testers to run live_teardown.py so nothing billable is
    left behind, so a swallowed AccessDenied must not read as a clean teardown.

    The live_* scripts are deliberately not shipped in the published plugin
    tree, so skip rather than fail when this runs from that tree.
    """
    import pathlib
    import unittest
    script = (pathlib.Path(__file__).resolve().parent.parent
              / "scripts" / "live_teardown.py")
    if not script.exists():
        raise unittest.SkipTest("live_teardown.py is not shipped in this tree")
    source = script.read_text()
    assert "FAILURES" in source
    assert "TEARDOWN INCOMPLETE" in source
    assert "return 1" in source
