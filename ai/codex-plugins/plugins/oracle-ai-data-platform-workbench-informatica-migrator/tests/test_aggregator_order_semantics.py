"""Aggregator constructs whose Informatica meaning depends on row order.

Spark is unordered and partitioned. Informatica processes rows in arrival
order. Every construct that means "first", "last", "running" or "moving"
therefore has no faithful translation unless the generated code supplies an
ordering the export does not carry -- the ordering column is a property of
the data, not of the mapping.

The tool's job is to say so, not to guess. These tests assert the marker,
because a silently arbitrary value is the failure mode: it runs, it looks
plausible, and it can differ between runs of the same job on the same data.
"""
from __future__ import annotations

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.models import (
    DataFlowDirection as D,
    Transformation,
    TransformationField,
    TransformationType,
)


def _agg(fields, name="AGG_T", group=("CUST_ID",)):
    tx = Transformation(name=name, type=TransformationType.AGGREGATOR)
    tx.group_by_fields = list(group)
    tx.fields = fields
    return "\n".join(TransformationConverter().convert(tx, "df_in", "df_out", {}) or [])


KEY = TransformationField(name="CUST_ID", direction=D.INPUT_OUTPUT, is_group_by=True)
SUM = TransformationField(name="TOTAL", direction=D.OUTPUT, expression="SUM(AMT)")


def test_passthrough_field_is_flagged():
    """A port that is neither grouped nor aggregated.

    Informatica returns the LAST row of each group. Spark's aggregate picks
    an arbitrary row -- potentially a different one per column, so the
    output row need not correspond to any single source row.
    """
    out = _agg([KEY, SUM, TransformationField(name="REGION", direction=D.INPUT_OUTPUT)])
    assert "REVIEW REQUIRED" in out
    assert "REGION" in out
    assert "passes through" in out


def test_passthrough_uses_last_not_first():
    """F.last matches what Informatica meant, so adding a Window.orderBy
    later makes it correct rather than merely different."""
    out = _agg([KEY, SUM, TransformationField(name="REGION", direction=D.INPUT_OUTPUT)])
    assert 'F.last(F.col("REGION"))' in out
    assert 'F.first(F.col("REGION"))' not in out


def test_a_clean_aggregator_is_not_flagged():
    """The marker has to mean something, so it must not fire on every
    Aggregator."""
    out = _agg([KEY, SUM])
    assert "REVIEW REQUIRED" not in out


@pytest.mark.parametrize("fn", ["FIRST", "LAST", "CUME", "MOVINGAVG", "MOVINGSUM"])
def test_order_dependent_aggregate_is_flagged(fn):
    expr = f"{fn}(AMT, 3)" if fn.startswith("MOVING") else f"{fn}(AMT)"
    out = _agg([KEY, TransformationField(name="V", direction=D.OUTPUT, expression=expr)])
    assert "ROW ORDER" in out, out
    assert fn in out


def test_order_independent_aggregate_is_not_flagged():
    out = _agg([KEY, TransformationField(name="V", direction=D.OUTPUT, expression="MAX(AMT)")])
    assert "ROW ORDER" not in out


def test_the_marker_names_the_field_not_just_the_function():
    """A notebook with several aggregates needs to say which one."""
    out = _agg([
        KEY,
        TransformationField(name="FIRST_AMT", direction=D.OUTPUT, expression="FIRST(AMT)"),
        SUM,
    ])
    assert "FIRST_AMT uses FIRST" in out


# ── Session SQL: reproduced rather than dropped ────────────────────────

def test_pre_and_post_session_sql_reach_the_notebook():
    """Parsed into the Session model since forever, never emitted.

    A pre-SQL that truncates a staging table or a post-SQL that rebuilds
    an index is a side effect the original pipeline depended on. Dropping
    it silently made the migrated job look complete while doing less.
    """
    from infa2aidp.generators.notebook_generator import NotebookGenerator
    from infa2aidp.models import Session

    s = Session(name="s_load", pre_sql="TRUNCATE TABLE STG;", post_sql="ANALYZE TABLE DW;")
    pre = NotebookGenerator._session_sql_cell(s, "pre")
    post = NotebookGenerator._session_sql_cell(s, "post")
    assert "TRUNCATE TABLE STG;" in pre
    assert "ANALYZE TABLE DW;" in post
    assert "REVIEW REQUIRED" in pre and "REVIEW REQUIRED" in post


def test_session_sql_is_emitted_commented_out():
    """It is in the source database's dialect, against a connection this
    notebook does not hold. Silently dropping it was the bug; silently
    running it would be a different one."""
    from infa2aidp.generators.notebook_generator import NotebookGenerator
    from infa2aidp.models import Session

    cell = NotebookGenerator._session_sql_cell(
        Session(name="s", pre_sql="DROP TABLE X;"), "pre"
    )
    for line in cell.splitlines():
        assert line.startswith("#"), f"executable line in a session-SQL cell: {line!r}"


def test_a_session_with_no_sql_adds_no_cell():
    from infa2aidp.generators.notebook_generator import NotebookGenerator
    from infa2aidp.models import Session

    assert NotebookGenerator._session_sql_cell(Session(name="x"), "pre") == ""
    assert NotebookGenerator._session_sql_cell(Session(name="x"), "post") == ""
