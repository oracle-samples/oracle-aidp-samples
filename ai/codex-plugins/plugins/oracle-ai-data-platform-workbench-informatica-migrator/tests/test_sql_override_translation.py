"""A SQL override that JOINs is now RUN, not reported and skipped.

A PowerCenter Source Qualifier can carry a SQL Query override. A
single-table ``SELECT ... WHERE`` was already translated into a filter. An
override that JOINed, UNIONed or sub-selected was not: the generator read
the base table and reported that the override's logic had not been applied.
That is honest but it migrates nothing -- and worse, the notebook runs and
produces rows, just the wrong ones, so a reviewer who skips the comment
sees a working pipeline.

Spark SQL accepts most of the ANSI subset these overrides are written in, so
the override can be run as written against catalog-qualified tables. The
trade this makes is deliberate: a mistranslation now surfaces as an
AnalysisException on the first run instead of as silently different rows.

Two gates guard it, and both must hold or the refusal stands:
  1. no Oracle-only construct -- ``(+)``, ROWNUM, CONNECT BY, MINUS, binds;
  2. every table resolves to a source in the mapping, so it can be qualified.

The second half of this file executes the generated SQL on a real Spark
session and checks the ROWS, because "it emitted a spark.sql call" is not
the claim being made.
"""
from __future__ import annotations

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import Transformation, TransformationType

from tests.conftest_spark import spark  # noqa: F401

SOURCES = {"ORDERS": "oltp.app.orders", "CUSTOMERS": "oltp.app.customers"}


def _read(sql: str, sources=SOURCES) -> list[str]:
    tx = Transformation(name="SQ_ORDERS", type=TransformationType.SOURCE_QUALIFIER)
    tx.sql_override = sql
    return TransformationConverter().source_read_lines(
        tx, "oltp.app.orders", "df", sources
    )


def _translated(lines: list[str]) -> bool:
    """A real emitted read, not the words "spark.sql()" inside the refusal
    message -- which is what a naive substring check matches."""
    return any(
        not l.strip().startswith("#") and "spark.sql(" in l for l in lines
    )


def _refused(lines: list[str]) -> bool:
    return any("REVIEW REQUIRED" in l for l in lines)


# ── Translated ─────────────────────────────────────────────────────────

def test_a_joining_override_is_translated_and_tables_qualified():
    lines = _read(
        "SELECT o.ORDER_ID, c.NAME FROM ORDERS o "
        "JOIN CUSTOMERS c ON o.CUST_ID = c.CUST_ID"
    )
    assert _translated(lines) and not _refused(lines)
    sql_line = next(l for l in lines if not l.strip().startswith("#") and "spark.sql(" in l)
    assert "oltp.app.orders" in sql_line and "oltp.app.customers" in sql_line
    # The bare names must be gone -- a bare name resolves against whatever
    # catalog the session happens to be using.
    assert " ORDERS " not in sql_line and " CUSTOMERS " not in sql_line


def test_a_union_override_is_translated():
    lines = _read("SELECT ORDER_ID FROM ORDERS UNION SELECT ORDER_ID FROM CUSTOMERS")
    assert _translated(lines) and not _refused(lines)


def test_a_translated_override_carries_a_review_marker():
    """Running it is not the same as proving it matches Oracle."""
    lines = _read("SELECT o.ORDER_ID FROM ORDERS o JOIN CUSTOMERS c ON o.CUST_ID=c.CUST_ID")
    assert any("REVIEW" in l for l in lines)


def test_a_param_reference_resolves_at_run_time_not_generation_time():
    """Baking one run's parameter value into the SQL would be wrong on the
    next run."""
    lines = _read(
        "SELECT o.ORDER_ID FROM ORDERS o JOIN CUSTOMERS c ON o.CUST_ID=c.CUST_ID "
        "WHERE o.REGION = $$REGION"
    )
    sql_line = next(l for l in lines if not l.strip().startswith("#") and "spark.sql(" in l)
    assert "_param_text('REGION')" in sql_line
    assert "$$REGION" not in sql_line
    assert sql_line.strip().startswith('df = spark.sql(f"""'), sql_line


# ── Refused, with the reason named ─────────────────────────────────────

@pytest.mark.parametrize("sql,needle", [
    ("SELECT o.X, c.Y FROM ORDERS o, CUSTOMERS c WHERE o.ID = c.ID(+)", "(+)"),
    ("SELECT X FROM ORDERS o JOIN CUSTOMERS c ON o.ID=c.ID WHERE ROWNUM < 10", "ROWNUM"),
    ("SELECT X FROM ORDERS o JOIN CUSTOMERS c ON o.ID=c.ID CONNECT BY PRIOR X = Y", "CONNECT"),
    ("SELECT X FROM ORDERS MINUS SELECT X FROM CUSTOMERS", "MINUS"),
    ("SELECT X FROM ORDERS o JOIN CUSTOMERS c ON o.ID=c.ID WHERE X = :bind", "bind"),
])
def test_oracle_only_sql_is_refused_not_guessed(sql, needle):
    """Guessing the ANSI equivalent of an Oracle join is how a migration
    produces plausible wrong numbers."""
    lines = _read(sql)
    assert _refused(lines), f"{needle} override was translated: {lines}"
    assert not _translated(lines)
    assert any("Oracle-only SQL" in l for l in lines), lines


def test_an_unresolvable_table_is_refused_and_named():
    lines = _read("SELECT a.X FROM ORDERS a JOIN NOT_A_SOURCE b ON a.ID=b.ID")
    assert _refused(lines)
    assert any("NOT_A_SOURCE" in l for l in lines)


def test_no_source_map_falls_back_to_the_refusal():
    """The comparison-report path calls this without a source map; it must
    not crash, and must not translate blind."""
    lines = _read("SELECT a.X FROM ORDERS a JOIN CUSTOMERS b ON a.ID=b.ID", sources=None)
    assert _refused(lines) and not _translated(lines)


def test_a_simple_single_table_override_still_becomes_a_filter():
    """The pre-existing path must be untouched -- a plain WHERE is better as
    a filter on a table read than as a SQL string."""
    lines = _read("SELECT ORDER_ID, AMT FROM ORDERS WHERE AMT > 100")
    assert not _translated(lines), "a single-table override should stay a table read"
    assert any(".filter(" in l for l in lines)


# ── Executed on real Spark: the rows, not the string ───────────────────

def _run_generated(spark, lines: list[str]) -> list:
    """Execute the generated read lines, as a notebook would."""
    ns = {"spark": spark}
    code = "\n".join(l for l in lines if not l.strip().startswith("#"))
    exec(code, ns)  # noqa: S102
    return ns["df"].collect()


@pytest.fixture(scope="module")
def tables(spark):  # noqa: F811
    spark.sql("CREATE DATABASE IF NOT EXISTS app")
    spark.createDataFrame(
        [(1, 10, 500.0), (2, 11, 50.0), (3, 99, 900.0)],
        "ORDER_ID int, CUST_ID int, AMT double",
    ).createOrReplaceTempView("orders_v")
    spark.createDataFrame(
        [(10, "Acme"), (11, "Globex")], "CUST_ID int, NAME string",
    ).createOrReplaceTempView("customers_v")
    return {"ORDERS": "orders_v", "CUSTOMERS": "customers_v"}


def test_the_translated_join_returns_the_joined_rows(spark, tables):  # noqa: F811
    """Order 3 has CUST_ID 99, which no customer matches: an inner join must
    drop it. Getting three rows back would mean the join was not applied --
    exactly the silent defect this change fixes."""
    lines = _read(
        "SELECT o.ORDER_ID, c.NAME FROM ORDERS o "
        "JOIN CUSTOMERS c ON o.CUST_ID = c.CUST_ID ORDER BY o.ORDER_ID",
        sources=tables,
    )
    rows = _run_generated(spark, lines)
    assert [(r[0], r[1]) for r in rows] == [(1, "Acme"), (2, "Globex")]


def test_the_translated_join_applies_its_where_clause(spark, tables):  # noqa: F811
    lines = _read(
        "SELECT o.ORDER_ID FROM ORDERS o JOIN CUSTOMERS c ON o.CUST_ID = c.CUST_ID "
        "WHERE o.AMT > 100",
        sources=tables,
    )
    rows = _run_generated(spark, lines)
    assert [r[0] for r in rows] == [1]


def test_the_translated_union_returns_both_sides(spark, tables):  # noqa: F811
    lines = _read(
        "SELECT CUST_ID FROM ORDERS UNION SELECT CUST_ID FROM CUSTOMERS",
        sources=tables,
    )
    rows = _run_generated(spark, lines)
    assert sorted(r[0] for r in rows) == [10, 11, 99]


# ── The regression this file found ─────────────────────────────────────

def test_a_minus_override_is_not_mistaken_for_a_table_alias():
    """`SELECT x FROM A MINUS SELECT x FROM B` used to match the
    single-table pattern with MINUS read as A's table ALIAS.

    The generator emitted a plain read of A, the second half of the query
    vanished, and because it never reached the complex-SQL path there was no
    review item either -- a silently wrong read with nothing to notice. The
    set operators now mark the override complex, so it reaches the refusal
    (MINUS has no safe Spark rewrite here).
    """
    lines = _read("SELECT X FROM ORDERS MINUS SELECT X FROM CUSTOMERS")
    assert _refused(lines), (
        "a MINUS override must not be read as a single-table query with "
        "MINUS as an alias"
    )
    assert any("MINUS" in l for l in lines)


@pytest.mark.parametrize("op", ["MINUS", "INTERSECT", "EXCEPT", "UNION"])
def test_every_set_operator_reaches_a_decided_outcome(op):
    """Either translated or refused -- never a silent single-table read."""
    lines = _read(f"SELECT CUST_ID FROM ORDERS {op} SELECT CUST_ID FROM CUSTOMERS")
    assert _translated(lines) or _refused(lines), (
        f"{op} override produced neither a translation nor a review: {lines}"
    )
