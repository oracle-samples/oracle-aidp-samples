"""A Joiner whose export carries no MASTER flag still joins.

Master/detail resolution reads the port-level MASTER/ISMASTER flag or the
Master Source property. An export that carries neither used to lose the join
entirely: the converter emitted a review comment and passed the detail side
through, so the notebook ran, produced rows, and the second source was
simply absent from the result.

Refusing to guess the DIRECTION is right -- an inverted Master/Detail Outer
Join silently changes which rows survive. But direction only matters for an
OUTER join. An inner join is symmetric: `a.join(b, cond, "inner")` and
`b.join(a, cond, "inner")` hold the same rows. So an inner join is applied
and an outer join is still refused.

Three defects were found building this, all on the same mapping:

1. the join was dropped whenever no MASTER flag existed, even for an inner
   join, which is silent data loss;
2. the Joiner read the RAW predecessor DataFrame while the prepared
   column-rename copy sat unused, so the join referenced pre-rename column
   names -- the key was re-pointed for every input except this one, because
   its dictionary key begins with "__" like the sentinels;
3. the join condition was oriented by the Designer's "master written first"
   convention, which this export contradicts. `TEAM_ID = TEAM_ID_D` has TEAM_ID fed by the DETAIL side, so each column
   was referenced on the wrong DataFrame. Orientation now comes from the
   CONNECTORs -- which DataFrame actually carries the port -- because the
   wiring is a fact and the writing order is a convention.
"""
from __future__ import annotations

import json
import os

import pytest

from infa2aidp.migrator import run_migration

FIXTURE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fixtures", "powercenter", "joiner_no_master_flag.xml",
)


@pytest.fixture(scope="module")
def notebook(tmp_path_factory) -> str:
    """The mapping whose Joiner has no MASTER port flag."""
    out = tmp_path_factory.mktemp("out")
    run_migration([FIXTURE], str(out), use_llm=False)
    books = [p for p in out.rglob("*.ipynb") if "team_headcount" in p.name]
    assert books, [p.name for p in out.rglob("*.ipynb")]
    cells = json.loads(books[0].read_text())["cells"]
    return "\n".join(
        "".join(c["source"]) for c in cells if c["cell_type"] == "code"
    )


def _join_call(code: str) -> str:
    start = code.index("df = df.join(")
    return code[start:code.index(")", code.index('how="'))]


def test_the_join_is_applied_not_skipped(notebook: str):
    assert "join(" in notebook
    assert "join skipped" not in notebook, (
        "an inner join was dropped for want of a MASTER flag it does not need"
    )


def test_the_join_uses_the_renamed_input_not_the_raw_predecessor(notebook: str):
    """The rename cell maps TEAM_ID -> TEAM_ID_D. Joining the raw
    predecessor instead references a column that rename was meant to create."""
    call = _join_call(notebook)
    assert "df_in_jnr_staff_team_sq_teams" in call, call
    assert "df_source_1" not in call, call


def test_no_prepared_input_is_left_unused(notebook: str):
    """An abandoned rename copy is the signature of a dropped join."""
    assert notebook.count("df_in_jnr_staff_team_sq_teams") >= 2, (
        "the renamed input is created but never read"
    )


def test_each_port_is_referenced_on_the_dataframe_that_carries_it(notebook: str):
    """TEAM_ID comes from the detail side, TEAM_ID_D from the
    master side. Swapping them is an AnalysisException at best and a wrong
    join at worst."""
    call = _join_call(notebook)
    assert 'df["TEAM_ID"]' in call, call
    assert 'df_in_jnr_staff_team_sq_teams["TEAM_ID_D"]' in call, call


def test_the_applied_join_is_an_inner_join(notebook: str):
    assert 'how="inner"' in _join_call(notebook)


def test_the_notebook_says_why_the_direction_did_not_matter(notebook: str):
    """Applying a join the export under-specified needs its reasoning on the
    page, or the next reader cannot tell it from a guess."""
    assert "symmetric" in notebook
    assert "Master/detail side was not identified" in notebook


def test_the_run_reports_no_broken_notebook(tmp_path):
    """The validator flagged this mapping as unrunnable; it must not any
    more, and that is the end-to-end statement of the fix."""
    result = run_migration([FIXTURE], str(tmp_path), use_llm=False)
    assert not result.broken_notebooks, result.broken_notebooks


# ── The refusal that must survive ──────────────────────────────────────

def test_an_outer_join_without_a_master_flag_is_still_refused():
    """Direction decides which rows survive an outer join, so it cannot be
    inferred from a symmetric-join argument."""
    from infa2aidp.converters.transformation_converter import TransformationConverter
    from infa2aidp.models import Transformation, TransformationType

    tx = Transformation(name="JNR_X", type=TransformationType.JOINER)
    tx.join_type = "MASTER OUTER"
    tx.join_condition = "A = B"
    lines = TransformationConverter()._joiner(
        tx, "df_detail", "df_out",
        {"__master_unresolved__": "1", "__unordered_second__": "df_other"},
    )
    code = "\n".join(lines)
    assert "join skipped" in code, code
    assert ".join(" not in code, code
