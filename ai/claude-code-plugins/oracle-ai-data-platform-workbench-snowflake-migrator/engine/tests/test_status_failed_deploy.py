"""A create that failed is not work in progress.

In the demo, SNOWDEMO.SALES.LEGACY_AUDIT's create returned 202 Accepted and
never appeared; the diagnosis probe showed the NAME was burned -- refused
for ever in that schema, recoverable only in a fresh one. deploy_result.json
had it in failed_targets and poisoned_names, and STAGES.md said "1
failed/mismatched". SUMMARY.md showed the same table as MEDIUM /
IN_PROGRESS with no failure note, and its footer said "2 verified, 1
unverified": migration_status never read failed_targets, so a failed ident
fell through to the attempted-targets fallback, and render_summary called
the failed count "unverified". A permanent failure read as work under way.
"""
import pytest

from emulation.runbook import run_demo
from plan.status import deploy_failure, migration_status
from report.render import render_summary

_FAILED = {"dry_run": False, "attempted_targets": ["D.S.T", "D.S.OK"],
           "verified_targets": ["D.S.OK"], "failed_targets": ["D.S.T"],
           "failed": [{"source_identifier": "D.S.T", "target_fqn": "d.s.t",
                       "reason": "the create was REFUSED by the target"}],
           "poisoned_names": [], "catalog_in_scope": "d"}


def test_a_failed_create_is_not_in_progress():
    assert migration_status("D.S.T", deployed=_FAILED) == "BLOCKED"


def test_a_burned_name_is_blocked_and_says_so():
    dep = dict(_FAILED, poisoned_names=["d.s.t"])
    assert migration_status("D.S.T", deployed=dep) == "BLOCKED"
    assert "cannot be reused" in deploy_failure("D.S.T", dep)
    assert "fresh schema" in deploy_failure("D.S.T", dep)


def test_no_failure_means_no_failure_note():
    assert deploy_failure("D.S.OK", _FAILED) is None
    assert deploy_failure("D.S.T", None) is None
    assert deploy_failure("D.S.T", dict(_FAILED, dry_run=True)) is None


def test_the_summary_row_names_the_failure_and_the_footer_counts_it_as_failed():
    plan = {"can_migrate": [
        {"source_identifier": "D.S.T", "object_type": "TABLE", "rows": 1},
        {"source_identifier": "D.S.OK", "object_type": "TABLE", "rows": 1}]}
    md = render_summary(plan, {}, _FAILED, None)
    row = next(l for l in md.splitlines() if "`D.S.T`" in l)
    assert "IN_PROGRESS" not in row
    assert "| HIGH | BLOCKED |" in row
    assert "REFUSED" in row
    assert "1 verified, 1 failed" in md
    assert "unverified." not in md


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    out = tmp_path_factory.mktemp("demo_failed")
    run_demo(out)
    return out


def test_the_demo_summary_agrees_with_stages_about_legacy_audit(demo):
    md = (demo / "SUMMARY.md").read_text(encoding="utf-8")
    row = next(l for l in md.splitlines() if "LEGACY_AUDIT" in l)
    assert "IN_PROGRESS" not in row
    assert "BLOCKED" in row and "cannot be reused" in row
    assert "1 unverified" not in md and "1 failed" in md
