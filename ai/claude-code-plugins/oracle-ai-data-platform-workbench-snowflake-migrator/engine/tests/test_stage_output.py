"""The accumulated report lands in the workspace at the end of every stage.

`reporting.publish_each_stage: true` makes every logged stage upload the
cumulative token report, the phase report and the run log -- overwritten, so
they accumulate -- plus one per-stage snapshot named by its runbook step, into
`reporting.workspace_dir` (default report/output) of the migration workspace
provision created. A failure to upload never fails the stage.
"""
import json

import pytest

import snowmig
from migration_config import ConfigError, reporting_block
from report.stage_output import publish_stage_output, snapshot_name


class FakeCall:
    def __init__(self, fail=False):
        self.calls, self.uploaded, self.fail = [], [], fail

    def __call__(self, op, **kw):
        self.calls.append((op, kw))
        if self.fail and op == "upload_ws_file":
            raise RuntimeError("503 unavailable")
        if op == "upload_ws_file":
            self.uploaded.append((kw["path"], open(kw["local_path"]).read()))
        if op == "list_ws_objects":
            return {"items": [{"path": p} for p, _ in self.uploaded]}
        return {}


def _run(tmp_path, stage="assess", runbook_exit=0):
    from report.tokens import record_stage_run
    (tmp_path / "provision_result.json").write_text(json.dumps(
        {"dry_run": False, "workspace": {"key": "ws-key"}}))
    record_stage_run(tmp_path, stage, "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:30+00:00", runbook_exit, None)
    return tmp_path


def test_the_block_defaults_off_and_validates():
    assert reporting_block({}) == {"publish_each_stage": False,
                                   "workspace_dir": "report/output"}
    with pytest.raises(ConfigError):
        reporting_block({"reporting": {"publish_each_stage": "yes"}})
    with pytest.raises(ConfigError):
        reporting_block({"reporting": {"workspace_dir": "/abs/../x"}})


@pytest.mark.parametrize("stage,runbook,name", [
    ("discover-workflow", "S6", "007_S06_discover-workflow.json"),
    ("provision", "S1 S2 S5", "007_S01-S02-S05_provision.json"),
    ("deps", "-", "007_deps.json"),
    ("plan", "S7-S9", "007_S07-S09_plan.json")])
def test_the_snapshot_is_named_by_run_and_runbook_step(stage, runbook, name):
    assert snapshot_name(7, stage, runbook) == name


def test_the_accumulated_report_and_a_snapshot_are_uploaded(tmp_path):
    out = _run(tmp_path)
    call = FakeCall()
    res = publish_stage_output(call, "ws-key", out, "report/output")
    remote = [p for p, _ in call.uploaded]
    for name in ("tokens.json", "TOKENS.md", "PHASES.md", "run_log.jsonl"):
        assert f"report/output/{name}" in remote
    snap = [p for p in remote if p.endswith("_assess.json")]
    assert snap == ["report/output/001_S07_assess.json"], remote
    body = json.loads(dict(call.uploaded)[snap[0]])
    assert body["stage"] == "assess" and body["exit_code"] == 0
    assert "tokens" in body and "cumulative_tokens" in body
    assert res["verified"] == len(remote)
    assert call.calls[0][0] == "create_ws_folder"


def test_an_upload_failure_is_reported_not_raised(tmp_path):
    out = _run(tmp_path)
    res = publish_stage_output(FakeCall(fail=True), "ws-key", out,
                               "report/output")
    assert res["verified"] == 0 and res["not_verified"]


def test_main_publishes_after_a_stage_only_when_switched_on(tmp_path,
                                                            monkeypatch):
    monkeypatch.delenv("SNOWMIG_NO_STAGE_PUBLISH")
    (tmp_path / "provision_result.json").write_text(json.dumps(
        {"dry_run": False, "workspace": {"key": "ws-key"}}))
    cfg = tmp_path / "c.yaml"
    calls = FakeCall()
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: calls)
    cfg.write_text("aidp:\n  datalake_ocid: ocid1.aidataplatform.oc1.iad.a\n")
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: cfg)
    monkeypatch.setattr(snowmig, "cmd_notebook", lambda args: 0)
    snowmig.main(["notebook", "--out-dir", str(tmp_path)])
    assert calls.calls == [], "off by default"
    cfg.write_text("aidp:\n  datalake_ocid: ocid1.aidataplatform.oc1.iad.a\n"
                   "reporting:\n  publish_each_stage: true\n")
    snowmig.main(["notebook", "--out-dir", str(tmp_path)])
    assert any(p.endswith("_notebook.json") for p, _ in calls.uploaded)


def test_no_provisioned_workspace_means_nothing_is_published(tmp_path,
                                                             monkeypatch):
    monkeypatch.delenv("SNOWMIG_NO_STAGE_PUBLISH")
    cfg = tmp_path / "c.yaml"
    cfg.write_text("aidp:\n  datalake_ocid: ocid1.aidataplatform.oc1.iad.a\n"
                   "reporting:\n  publish_each_stage: true\n")
    calls = FakeCall()
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: calls)
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: cfg)
    assert snowmig.main(["stages", "--out-dir", str(tmp_path)]) == 0
    assert calls.calls == []


def test_exclusions_given_once_hold_for_every_later_token_build(tmp_path):
    """Found live: summary excluded development windows, then the next
    stage's publish rebuilt TOKENS.md without them, and the two reports
    disagreed about the same run."""
    from report.tokens import build_token_report, save_exclusions
    save_exclusions(tmp_path, [("2026-09-24T10:00:00Z", "2026-09-24T11:00:00Z")])
    save_exclusions(tmp_path, [("2026-09-24T10:00:00Z", "2026-09-24T11:00:00Z"),
                               ("2026-09-24T12:00:00Z", "2026-09-24T12:30:00Z")])
    saved = json.loads((tmp_path / "token_exclusions.json").read_text())
    assert len(saved["windows"]) == 2, "merged, not duplicated"
    from report.tokens import record_stage_run
    record_stage_run(tmp_path, "plan", "2026-09-24T09:00:00+00:00",
                     "2026-09-24T13:00:00+00:00", 0, "s")
    rep = build_token_report(tmp_path, transcripts=[])
    assert rep["exclude_windows"] == [list(w) for w in
                                      [("2026-09-24T10:00:00Z", "2026-09-24T11:00:00Z"),
                                       ("2026-09-24T12:00:00Z", "2026-09-24T12:30:00Z")]]


def test_teardown_takes_the_config_flag():
    args = snowmig.build_parser().parse_args(["teardown", "--config", "c.yaml"])
    assert args.config == "c.yaml"


def test_a_utility_command_publishes_no_snapshot(tmp_path, monkeypatch):
    """Found live: build-notebooks and stages published 049_build-notebooks
    and 044_stages snapshots into the run's report/output. Tools are not
    phases; only a STAGES phase ends with a published report."""
    monkeypatch.delenv("SNOWMIG_NO_STAGE_PUBLISH")
    (tmp_path / "provision_result.json").write_text(json.dumps(
        {"dry_run": False, "workspace": {"key": "ws-key"}}))
    cfg = tmp_path / "c.yaml"
    cfg.write_text("aidp:\n  datalake_ocid: ocid1.aidataplatform.oc1.iad.a\n"
                   "reporting:\n  publish_each_stage: true\n")
    calls = FakeCall()
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: cfg)
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: calls)
    snowmig.main(["stages", "--out-dir", str(tmp_path)])
    assert calls.calls == [], "stages is a utility, not a phase"
