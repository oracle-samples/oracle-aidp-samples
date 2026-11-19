"""The parallel batch path, asserted on outcome rather than on "it ran".

Three defects lived on this path simultaneously, all invisible to a check
that only asked whether notebooks were produced:

1. the parsed ``Session`` was discarded and a synthetic one used, so
   pre/post SQL, connections, commit interval and session parameters
   silently vanished;
2. only ``parse_result.mappings[0]`` was migrated, so a multi-mapping
   export lost every mapping after the first;
3. ``status`` was set to "success" regardless of whether the generator had
   declined to accept its own output.

Each of the three gets its own assertion here. A single "did the batch
produce output" test is exactly the instrument that missed all three -- it
confirms presence, and every one of these failures leaves presence intact.

The LLM branch is not exercised: these tests run the rule-based fallback,
which is the path available without an API key. The three properties under
test are branch-independent -- they are about which mappings are found,
which session is attached, and whether review state is reported.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from infa2aidp import config as cfg
from infa2aidp.batch import BatchMigrator

ROOT = Path(__file__).resolve().parent.parent
CONSTRUCTS = ROOT / "tests" / "fixtures" / "constructs"


@pytest.fixture
def rule_based(monkeypatch):
    """Batch with no LLM handler, so the deterministic branch runs."""
    monkeypatch.setattr(cfg, "RULE_BASED_FALLBACK", True, raising=False)
    return BatchMigrator(llm_handler=None, max_workers=2)


def _run(migrator, src: Path, out: Path):
    staged = out / "in"
    staged.mkdir(parents=True, exist_ok=True)
    (staged / src.name).write_text(src.read_text())
    return migrator.migrate_folder(str(staged), str(out / "nb"))


# ── 1. every mapping, not just the first ──────────────────────────────

def test_every_mapping_in_a_file_is_migrated(rule_based, tmp_path):
    summary = _run(rule_based, CONSTRUCTS / "multi_mapping.xml", tmp_path)
    names = sorted(r.mapping_name for r in summary.results)
    assert names == ["m_first", "m_second", "m_third"], names


def test_one_notebook_is_written_per_mapping(rule_based, tmp_path):
    _run(rule_based, CONSTRUCTS / "multi_mapping.xml", tmp_path)
    produced = sorted(p.stem for p in (tmp_path / "nb").rglob("*.ipynb"))
    assert produced == ["nb_m_first", "nb_m_second", "nb_m_third"], produced


def test_the_summary_counts_mappings_not_files(rule_based, tmp_path):
    """Reporting a three-mapping file as one unit of work is what hid the
    two that were being dropped."""
    summary = _run(rule_based, CONSTRUCTS / "multi_mapping.xml", tmp_path)
    assert summary.total == 3


# ── 2. the parsed session, not a synthetic one ────────────────────────

def test_the_parsed_session_reaches_the_generated_notebook(rule_based, tmp_path):
    """A synthetic Session carries no pre/post SQL. If one is substituted,
    this SQL cannot appear -- which is what made the loss silent."""
    _run(rule_based, CONSTRUCTS / "session_pre_post_sql.xml", tmp_path)
    nbs = list((tmp_path / "nb").rglob("*.ipynb"))
    assert len(nbs) == 1, [n.name for n in nbs]
    nb = json.loads(nbs[0].read_text())
    code = "\n".join("".join(c.get("source", [])) for c in nb.get("cells", []))
    assert "TRUNCATE TABLE STG_ORDERS" in code
    assert "ANALYZE TABLE STG_ORDERS COMPUTE STATISTICS" in code


# ── 3. review state reaches the reported status ───────────────────────

def test_a_notebook_carrying_a_marker_is_reported_as_needing_review(
    rule_based, tmp_path
):
    """The assertion that actually bites.

    An earlier version of this test compared ``review_required`` against
    the marker's presence across the multi-mapping fixture -- whose
    notebooks carry no markers, so it read False == False and passed even
    with the fix disabled. A test of review state needs input that
    produces a review. The session-SQL fixture does: its SQL is emitted
    with a marker because the notebook cannot execute it.
    """
    summary = _run(rule_based, CONSTRUCTS / "session_pre_post_sql.xml", tmp_path)
    assert len(summary.results) == 1
    r = summary.results[0]

    code = Path(r.notebook_path).read_text()
    assert "REVIEW REQUIRED" in code, "fixture no longer produces a marker"
    assert r.review_required is True, (
        "the notebook carries a REVIEW REQUIRED marker but the result "
        "reports no review needed"
    )


def test_a_clean_notebook_is_not_reported_as_needing_review(
    rule_based, tmp_path
):
    """The flag has to discriminate, not just be set."""
    summary = _run(rule_based, CONSTRUCTS / "multi_mapping.xml", tmp_path)
    for r in summary.results:
        nb = Path(r.notebook_path)
        if not nb.exists():
            continue
        code = nb.read_text()
        assert r.review_required == ("REVIEW REQUIRED" in code), (
            f"{r.mapping_name}: review_required={r.review_required} but the "
            f"notebook {'does' if 'REVIEW REQUIRED' in code else 'does not'} "
            f"carry a marker"
        )


def test_the_batch_report_records_the_review_bucket(rule_based, tmp_path):
    _run(rule_based, CONSTRUCTS / "multi_mapping.xml", tmp_path)
    report = json.loads((tmp_path / "nb" / "batch_report.json").read_text())
    assert "review" in report, sorted(report)
