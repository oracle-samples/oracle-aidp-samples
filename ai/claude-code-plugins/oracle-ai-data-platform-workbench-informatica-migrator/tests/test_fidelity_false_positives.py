"""The fidelity report must not cry wolf.

Source fidelity re-reads the raw export and asks whether each construct in
it shows up in the generated notebook. It is a review signal, so its value
depends entirely on being believable: a report that flags things which are
fine teaches the reader to skim, and then it fails the one time it matters.

Two systematic false positives are fixed here. On the bundled exports alone
they accounted for most of the reported gaps -- 17 of 19 mappings flagged,
down to 12 once both were gone, with no change to a single notebook.
"""
from __future__ import annotations

import json
import os

import pytest

from infa2aidp.generators.source_fidelity import source_fidelity
from infa2aidp.migrator import run_migration

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
SAMPLE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fixtures", "powercenter", "joiner_no_master_flag.xml",
)


def _notebook_code(out_dir, needle: str) -> str:
    books = [p for p in out_dir.rglob("*.ipynb") if needle in p.name]
    assert books, [p.name for p in out_dir.rglob("*.ipynb")]
    cells = json.loads(books[0].read_text())["cells"]
    return "\n".join("".join(c["source"]) for c in cells if c["cell_type"] == "code")


# ── 1. The Source Qualifier's name reaches the notebook ────────────────

def test_the_read_cell_names_its_source_qualifier(tmp_path):
    """The generator drops the converter's own `# Source Qualifier: <name>`
    line. Without putting the name back on the read cell it appeared NOWHERE
    in the notebook: a reviewer could not tie the read back to the export,
    and fidelity reported every Source Qualifier as missing."""
    run_migration([SAMPLE], str(tmp_path), use_llm=False)
    code = _notebook_code(tmp_path, "team_headcount")
    assert "Source Qualifier: SQ_staff" in code, code[:600]


def test_source_qualifiers_are_not_reported_missing(tmp_path):
    run_migration([SAMPLE], str(tmp_path), use_llm=False)
    code = _notebook_code(tmp_path, "team_headcount")
    report = source_fidelity(SAMPLE, code)
    sq_missing = [n for n in report.transformations_missing if n.upper().startswith("SQ_")]
    assert not sq_missing, (
        f"Source Qualifier(s) reported missing although the read implements "
        f"them: {sq_missing}"
    )


# ── 2. A pass-through port is not an expression ────────────────────────

PASS_THROUGH = """<?xml version="1.0"?>
<MAPPING NAME="m_pt">
  <TRANSFORMATION NAME="SQ_T" TYPE="Source Qualifier">
    <TRANSFORMFIELD NAME="EMAIL" DATATYPE="string" PORTTYPE="OUTPUT" EXPRESSION="EMAIL"/>
    <TRANSFORMFIELD NAME="HIRE_DATE" DATATYPE="date" PORTTYPE="OUTPUT" EXPRESSION="HIRE_DATE"/>
  </TRANSFORMATION>
  <TRANSFORMATION NAME="EXP_T" TYPE="Expression">
    <TRANSFORMFIELD NAME="NET" DATATYPE="decimal" PORTTYPE="OUTPUT" EXPRESSION="GROSS - TAX"/>
    <TRANSFORMFIELD NAME="SAME" DATATYPE="string" PORTTYPE="OUTPUT" EXPRESSION="same"/>
  </TRANSFORMATION>
</MAPPING>
"""


def test_a_port_whose_expression_is_its_own_name_is_not_an_expression(tmp_path):
    """`EXPRESSION="EMAIL"` on a port named EMAIL computes nothing.

    PowerCenter writes one on every Source Qualifier port, so counting them
    made the report demand that every unconnected source column appear in
    the notebook -- backwards, since Informatica does not carry an
    unconnected port either.
    """
    src = tmp_path / "m_pt.xml"
    src.write_text(PASS_THROUGH)
    report = source_fidelity(str(src), "# nothing")
    named = set(report.expressions_in_source)
    assert "SQ_T.EMAIL" not in named, named
    assert "SQ_T.HIRE_DATE" not in named, named


def test_a_real_expression_is_still_counted(tmp_path):
    """The suppression must be narrow: a computed port still counts, and a
    case-different pass-through still does not."""
    src = tmp_path / "m_pt.xml"
    src.write_text(PASS_THROUGH)
    report = source_fidelity(str(src), "# nothing")
    named = set(report.expressions_in_source)
    assert "EXP_T.NET" in named, named
    # EXPRESSION="same" on port SAME differs only in case -- still a
    # pass-through, so still excluded.
    assert "EXP_T.SAME" not in named, named


def test_a_real_expression_is_reported_missing_when_it_is_missing(tmp_path):
    """The point of all of this is that the report still works."""
    src = tmp_path / "m_pt.xml"
    src.write_text(PASS_THROUGH)
    report = source_fidelity(str(src), "df = df")
    assert "EXP_T.NET" in report.expressions_missing
    assert report.has_gaps


# ── The end-to-end statement ───────────────────────────────────────────

def test_no_notebook_is_broken_for_the_bundled_sample(tmp_path):
    result = run_migration([SAMPLE], str(tmp_path), use_llm=False)
    assert not result.broken_notebooks, result.broken_notebooks
