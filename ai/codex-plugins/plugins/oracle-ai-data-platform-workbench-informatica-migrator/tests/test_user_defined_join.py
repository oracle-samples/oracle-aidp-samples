"""A User Defined Join is applied, not reported and skipped.

A PowerCenter Source Qualifier over several sources carries the join between
them as a User Defined Join. The generator used to read one source and
report the join as a review item -- so the notebook ran, returned the rows
of a single table, and the join simply did not happen.

A UDJ is semantically ``SELECT <columns> FROM a, b WHERE <condition>``, so it is
synthesised into that query and put through the same translation and the
same two gates as a SQL override. That reuse is the point: an Oracle ``(+)``
outer join is refused by the shared gate rather than guessed at by a second,
weaker join translator.
"""
from __future__ import annotations

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import Transformation, TransformationType

from tests.conftest_spark import spark  # noqa: F401

SOURCES = {"ORDERS": "oltp.app.orders", "CUSTOMERS": "oltp.app.customers"}
# The sources' columns, as the notebook generator passes them. The join is
# written with an explicit select list -- SELECT * would return CUST_ID
# twice -- so without these it is refused (tests/test_sq_sql_review_fixes.py).
COLUMNS = {"ORDERS": ["ORDER_ID", "CUST_ID"], "CUSTOMERS": ["CUST_ID", "NAME"]}


def _read(udj: str, sources=SOURCES, columns=COLUMNS) -> list[str]:
    tx = Transformation(name="SQ_ORDERS", type=TransformationType.SOURCE_QUALIFIER)
    tx.user_defined_join = udj
    return TransformationConverter().source_read_lines(
        tx, "oltp.app.orders", "df", sources, columns
    )


def _code(lines: list[str]) -> str:
    return "\n".join(l for l in lines if not l.strip().startswith("#"))


def test_a_user_defined_join_is_applied():
    lines = _read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID")
    assert "spark.sql(" in _code(lines)
    assert not any("REVIEW REQUIRED" in l for l in lines)


def test_both_sides_of_the_join_are_catalog_qualified():
    """A bare table name resolves against whatever catalog the session is
    using -- which may be a different table of the same name."""
    code = _code(_read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID"))
    assert "oltp.app.orders" in code and "oltp.app.customers" in code
    for bare in ("ORDERS", "CUSTOMERS"):
        assert f" {bare}," not in code and f" {bare} " not in code, code


def test_an_oracle_outer_join_is_refused_by_the_shared_gate():
    lines = _read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID(+)")
    assert any("REVIEW REQUIRED" in l for l in lines)
    assert any("(+)" in l for l in lines)
    assert "spark.sql(" not in _code(lines)


def test_a_join_naming_a_table_outside_the_mapping_is_refused():
    """We cannot qualify a table that is not a source here, and joining the
    wrong table silently is the failure to avoid."""
    lines = _read("ORDERS.CUST_ID = FOREIGN_TBL.CUST_ID")
    assert any("REVIEW REQUIRED" in l for l in lines)
    assert "spark.sql(" not in _code(lines)


def test_a_single_table_condition_is_not_a_join():
    """One table named on both sides is not something to join."""
    lines = _read("ORDERS.CUST_ID = ORDERS.PARENT_ID")
    assert any("REVIEW REQUIRED" in l for l in lines)


def test_without_a_source_map_it_falls_back_to_the_review_item():
    lines = _read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID", sources=None)
    assert any("REVIEW REQUIRED" in l for l in lines)


def test_a_translated_join_still_asks_for_a_row_count_check():
    lines = _read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID")
    assert any("REVIEW" in l for l in lines)


# ── Executed: the rows, not the string ─────────────────────────────────

@pytest.fixture(scope="module")
def tables(spark):  # noqa: F811
    spark.createDataFrame(
        [(1, 10), (2, 11), (3, 99)], "ORDER_ID int, CUST_ID int",
    ).createOrReplaceTempView("udj_orders")
    spark.createDataFrame(
        [(10, "Acme"), (11, "Globex")], "CUST_ID int, NAME string",
    ).createOrReplaceTempView("udj_customers")
    return {"ORDERS": "udj_orders", "CUSTOMERS": "udj_customers"}


def test_the_applied_join_drops_the_unmatched_row(spark, tables):  # noqa: F811
    """Order 3 points at customer 99, which does not exist. Three rows back
    would mean the join was never applied -- the defect this fixes."""
    ns = {"spark": spark}
    exec(_code(_read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID", tables)), ns)  # noqa: S102
    rows = ns["df"].orderBy("ORDER_ID").collect()
    assert [r["ORDER_ID"] for r in rows] == [1, 2]
    assert [r["NAME"] for r in rows] == ["Acme", "Globex"]


def test_the_applied_join_is_an_inner_join_not_a_cross_join(spark, tables):  # noqa: F811
    """`FROM a, b WHERE <cond>` is an inner join. Losing the WHERE would
    give 3 x 2 = 6 rows, which is the shape of mistake worth pinning."""
    ns = {"spark": spark}
    exec(_code(_read("ORDERS.CUST_ID = CUSTOMERS.CUST_ID", tables)), ns)  # noqa: S102
    assert ns["df"].count() == 2
