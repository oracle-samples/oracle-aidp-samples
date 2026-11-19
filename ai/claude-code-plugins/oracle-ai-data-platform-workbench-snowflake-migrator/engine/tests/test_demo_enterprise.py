"""`demo --estate enterprise`: the enterprise estate as a dev-mode walkthrough.

The standard demo (SNOWDEMO) teaches the lessons a trial account can hold.
The enterprise one runs the SAME production stages over SNOWENT -- the
estate a trial cannot hold -- and stops before anything AIDP-side: nothing
in it (registration, Delta Sharing) is live-verified, so no emulated AIDP
pretends to accept it. Every artifact is marked emulated, exactly as the
standard demo's are.
"""
import json

import pytest

import snowmig
from emulation.runbook import run_enterprise_demo

_ARTIFACTS = ("emulation.json", "inventory.json", "INVENTORY.md", "CENSUS.md",
              "dependencies.json", "maintenance.json", "MAINTENANCE.md",
              "security.json", "SECURITY.md", "plan.json",
              "PLANNED_OBJECTS.md", "ddl_plan.json", "DDL_PLAN.md",
              "external_registration.json", "EXTERNAL_REGISTRATION.md",
              "share_plan.json", "SHARE_PLAN.md", "SUMMARY.md", "DEMO.md")


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    out = tmp_path_factory.mktemp("demo_ent")
    return out, run_enterprise_demo(out)


def test_it_writes_every_artifact(demo):
    out, _ = demo
    assert [n for n in _ARTIFACTS if not (out / n).exists()] == []


def test_it_is_marked_emulated_and_contacts_no_aidp(demo):
    out, _ = demo
    marker = json.loads((out / "emulation.json").read_text(encoding="utf-8"))
    assert marker["emulated"] is True and marker["estate"] == "SNOWENT"
    text = (out / "DEMO.md").read_text(encoding="utf-8")
    assert "EMULATED" in text.splitlines()[0]
    assert "No Snowflake account and no AIDP DataLake were contacted" in text
    assert not (out / "deploy_result.json").exists()


def test_demo_md_claims_no_aidp_behaviour_the_run_did_not_have(demo):
    # The enterprise run has no AIDP step, emulated or real. DEMO.md once
    # reused the standard demo's boilerplate and said "only the two
    # transports were replaced" and that the asynchronous-create,
    # name-poisoning and type-drift behaviours were "demonstrated here" --
    # none of which this run touches. A generated artifact that claims
    # behaviour it did not show is the I3 defect in miniature.
    out, _ = demo
    text = (out / "DEMO.md").read_text(encoding="utf-8")
    low = text.lower()
    for claim in ("two transports", "asynchronous-create", "name-poisoning",
                  "type-drift", "demonstrated here", "live datalake",
                  "`catalog` and `deploy`", "snowdemo"):
        assert claim not in low, claim
    # What it does say: no AIDP at all, only the Snowflake transport faked,
    # and the AIDP-side steps of its reports are not live-verified.
    assert "no aidp" in low
    assert "only the snowflake transport" in low
    assert "not live-verified" in low


def test_the_narrative_names_each_enterprise_path(demo):
    _, result = demo
    text = " ".join(result["narrative"] + result["lessons"])
    for words in ("register in place", "Delta Sharing", "HELD",
                  "failover group", "hybrid", "event table",
                  "search optimization", "SECURE"):
        assert words.lower() in text.lower(), words


def test_the_cli_flag_runs_it(tmp_path, capsys):
    assert snowmig.main(["demo", "--estate", "enterprise",
                         "--out-dir", str(tmp_path)]) == 0
    assert (tmp_path / "SHARE_PLAN.md").is_file()
    assert "EMULATED" in capsys.readouterr().out


def test_the_narrative_does_not_count_iceberg_as_registered(demo):
    # An Iceberg table's generated CALL needs its metadata rewritten first,
    # so it is not "registered in place" by what this run generated.
    _, result = demo
    line = next(l for l in result["narrative"]
                if l.startswith("external-registration:"))
    assert "2 of 4" in line
    assert "2 Iceberg" in line and "rewrite" in line
