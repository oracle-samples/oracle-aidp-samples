"""Session-level constructs, exercised end to end from a real export.

Why this file is separate from the unit tests
=============================================
``tests/test_aggregator_order_semantics.py`` tests ``_session_sql_cell``
directly. That is worth having, but it is the wrong test on its own: it
calls the cell builder with a hand-made Session, so it keeps passing even
if nothing ever hands that builder a real one. The wiring is the part that
breaks.

These tests run the full path -- parse a PowerCenter export, generate a
notebook, assert the construct survived. Before the fixture below existed,
**no fixture anywhere carried session SQL**, so that path had never been
exercised and a break in it could not be detected by anything in the suite.

One fixture per construct, not per realistic mapping.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "tests" / "fixtures" / "constructs" / "session_pre_post_sql.xml"


@pytest.fixture(scope="module")
def notebook_code(tmp_path_factory):
    """Migrate the fixture through the CLI and return the notebook source."""
    import os

    out = tmp_path_factory.mktemp("session_constructs")
    proc = subprocess.run(
        [sys.executable, "-m", "infa2aidp.cli", "migrate",
         "-i", str(FIXTURE), "-o", str(out)],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
        env={**os.environ, "PYTHONPATH": str(ROOT / "engine")},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    nbs = list(out.rglob("*.ipynb"))
    assert len(nbs) == 1, f"expected one notebook, got {[n.name for n in nbs]}"
    nb = json.loads(nbs[0].read_text())
    return "\n".join("".join(c.get("source", [])) for c in nb.get("cells", []))


def test_the_parser_reads_session_sql():
    """<SESSIONEXTENSION><ATTRIBUTE NAME="Pre SQL"> reaches the model."""
    sys.path.insert(0, str(ROOT / "engine"))
    from infa2aidp.parsers.xml_parser import InformaticaXMLParser

    result = InformaticaXMLParser().parse(str(FIXTURE))
    assert len(result.sessions) == 1
    s = result.sessions[0]
    assert s.pre_sql == "TRUNCATE TABLE STG_ORDERS"
    assert s.post_sql == "ANALYZE TABLE STG_ORDERS COMPUTE STATISTICS"
    assert s.commit_interval == 5000


def test_pre_session_sql_survives_to_the_notebook(notebook_code):
    """The wiring, not the cell builder.

    A pre-SQL that truncates a staging table is a side effect the original
    pipeline depended on; dropping it made the migrated job look complete
    while doing less.
    """
    assert "TRUNCATE TABLE STG_ORDERS" in notebook_code
    assert "PRE-SESSION" in notebook_code


def test_post_session_sql_survives_to_the_notebook(notebook_code):
    assert "ANALYZE TABLE STG_ORDERS COMPUTE STATISTICS" in notebook_code
    assert "POST-SESSION" in notebook_code


def test_session_sql_is_flagged_for_review(notebook_code):
    """It is not executed, and the notebook has to say so."""
    assert "session SQL not executed by this notebook" in notebook_code


def test_session_sql_does_not_become_executable_code(notebook_code):
    """Running a source-dialect statement through spark.sql() would either
    fail or succeed against the wrong system."""
    for line in notebook_code.splitlines():
        if "TRUNCATE TABLE STG_ORDERS" in line or "ANALYZE TABLE STG_ORDERS" in line:
            assert line.lstrip().startswith("#"), f"uncommented session SQL: {line!r}"


# ── Multi-mapping exports ──────────────────────────────────────────────

MULTI = ROOT / "tests" / "fixtures" / "constructs" / "multi_mapping.xml"


def test_the_parser_returns_every_mapping_in_a_file():
    """A folder export routinely carries many mappings.

    Every other fixture in this repo carries exactly one, so a consumer
    that reads only ``mappings[0]`` is indistinguishable from one that
    reads all of them. This pins the parser side; the batch path's
    single-mapping read is recorded in references/conversion-hazards.md
    and is not fixed here.
    """
    sys.path.insert(0, str(ROOT / "engine"))
    from infa2aidp.parsers.xml_parser import InformaticaXMLParser

    result = InformaticaXMLParser().parse(str(MULTI))
    assert [m.name for m in result.mappings] == ["m_first", "m_second", "m_third"]


def test_the_default_migration_path_generates_every_mapping(tmp_path):
    """One notebook per mapping, not one per file."""
    import os

    proc = subprocess.run(
        [sys.executable, "-m", "infa2aidp.cli", "migrate",
         "-i", str(MULTI), "-o", str(tmp_path)],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
        env={**os.environ, "PYTHONPATH": str(ROOT / "engine")},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    produced = sorted(p.stem for p in tmp_path.rglob("*.ipynb"))
    assert len(produced) == 3, f"expected 3 notebooks, got {produced}"
