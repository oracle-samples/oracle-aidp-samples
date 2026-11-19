"""Publishing the finished report into the AIDP workspace.

The report was local-only: a run's record existed on the laptop that drove
it. `publish` copies inputs and outputs into the migration's own workspace
folder through the same workspace-object calls `provision` uses, reading
each file back. Dry run unless --execute, like every stage that writes.
"""
from target.publish import publishable_files, publish_report


def _out(tmp_path):
    for name in ("plan.json", "inventory.json", "ddl_plan.json",
                 "SUMMARY.md", "TRANSLATION_MAP.md", "PHASES.mmd",
                 "run_log.jsonl", "README.md", ".gitignore",
                 "snowmig-config.yaml", "my_private_key.p8"):
        (tmp_path / name).write_text("x")
    (tmp_path / "manifest-path").mkdir()
    (tmp_path / "manifest-path" / "inventory.json").write_text("x")
    return tmp_path


class FakeCall:
    def __init__(self, missing=()):
        self.calls, self.uploaded, self.missing = [], [], set(missing)

    def __call__(self, op, **kw):
        self.calls.append((op, kw))
        if op == "upload_ws_file":
            self.uploaded.append(kw["path"])
        if op == "list_ws_objects":
            return {"items": [{"path": p} for p in self.uploaded
                              if p.rsplit("/", 1)[1] not in self.missing]}
        return {}


def test_inputs_and_outputs_are_published_and_secrets_never_are(tmp_path):
    names = {p.name for p in publishable_files(_out(tmp_path))}
    assert {"plan.json", "inventory.json", "ddl_plan.json", "SUMMARY.md",
            "TRANSLATION_MAP.md", "PHASES.mmd", "run_log.jsonl"} <= names
    assert "snowmig-config.yaml" not in names
    assert "my_private_key.p8" not in names
    assert "README.md" not in names and ".gitignore" not in names


def test_a_dry_run_uploads_nothing(tmp_path):
    call = FakeCall()
    res = publish_report(call, "ws", _out(tmp_path), execute=False,
                         stamp="20260924T200000Z")
    assert res["dry_run"] is True and call.calls == []
    assert all(s["action"] == "would upload" for s in res["steps"])
    assert res["folder"].endswith("/reports/final-20260924T200000Z")


def test_every_file_is_uploaded_then_read_back(tmp_path):
    call = FakeCall()
    res = publish_report(call, "ws", _out(tmp_path), execute=True,
                         stamp="20260924T200000Z")
    ops = [op for op, _ in call.calls]
    assert ops[0] == "create_ws_folder"
    assert ops.count("upload_ws_file") == len(res["steps"])
    assert all(s["verified"] for s in res["steps"])
    assert res["verified"] == len(res["steps"])


def test_a_file_not_visible_after_upload_is_not_called_published(tmp_path):
    call = FakeCall(missing={"SUMMARY.md"})
    res = publish_report(call, "ws", _out(tmp_path), execute=True, stamp="s")
    bad = [s for s in res["steps"] if not s["verified"]]
    assert [s["file"] for s in bad] == ["SUMMARY.md"]
    assert bad[0]["action"] == "upload_requested"


def test_publish_is_a_phase_that_writes():
    from report.stages import STAGES, RUNS_ON
    pub = next(s for s in STAGES if s["stage"] == "publish")
    assert pub["writes"] is True and pub["phase"] == "reporting"
    assert pub["runs_on"] == RUNS_ON["control_plane"]


def test_the_cli_is_a_dry_run_by_default(tmp_path, monkeypatch):
    import snowmig
    _out(tmp_path)
    calls = FakeCall()
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: calls)
    assert snowmig.main(["publish", "--out-dir", str(tmp_path),
                         "--datalake-ocid", "ocid1.x", "--workspace",
                         "ws"]) == 0
    assert calls.calls == []
    assert (tmp_path / "publish_result.json").exists()
