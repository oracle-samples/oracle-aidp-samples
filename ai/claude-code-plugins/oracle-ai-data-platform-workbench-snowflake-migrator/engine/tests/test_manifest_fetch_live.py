"""The manifest `run` fetches after discovery is THIS run's, from where it
was written -- and a Windows path never reaches a stage notebook.

Live 2026-09-29, coverage run on AIDP. Provision was given
`--stage-param reports-dir=/Workspace/backup-snowflake-migration/reports_r5`
from Git Bash, which rewrote it (MSYS path conversion) to
`C:/Program Files/Git/Workspace/backup-snowflake-migration/reports_r5`.
Provision baked that into every PARAMS cell; discovery wrote its manifest
to that odd local path on the cluster; the structure stage then failed
with FileNotFoundError. Meanwhile `run` printed "manifest fetched" -- it
had downloaded the DEFAULT reports/discovery_manifest.json, the manifest
of an EARLIER run, and saved it as this run's.

Two defects: the fetch ignored the stage's reports-dir and never checked
that what it downloaded was this run's; and a drive-letter path was
accepted as a workspace path.
"""
import datetime
import json
import pathlib

import pytest

import snowmig
from target.stage_notebooks import STAGES, check_stage_params

UTC = datetime.timezone.utc
SUBMITTED = datetime.datetime(2026, 9, 29, 10, 58, 0, tzinfo=UTC)


def _manifest(generated_at):
    doc = {"schemas": [{"name": "CORE", "tables": [], "views": [], "errors": []}]}
    if generated_at is not None:
        doc["generated_at"] = generated_at.isoformat()
    return json.dumps(doc)


def _downloader(body, seen):
    def download(call, *, workspace, path, dest):
        seen.append(path)
        pathlib.Path(dest).write_text(body, encoding="utf-8")
        return {"dest": str(dest), "size": len(body)}
    return download


def test_the_fetch_reads_the_reports_dir_the_stage_was_given(tmp_path):
    seen = []
    snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=SUBMITTED,
        stage_params={"reports-dir": "/Workspace/backup-snowflake-migration/reports_r5"},
        download=_downloader(_manifest(SUBMITTED + datetime.timedelta(minutes=2)), seen))
    assert seen == ["backup-snowflake-migration/reports_r5/discovery_manifest.json"]


def test_without_a_reports_dir_param_the_default_folder_is_used(tmp_path):
    seen = []
    snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=SUBMITTED, stage_params={},
        download=_downloader(_manifest(SUBMITTED + datetime.timedelta(minutes=2)), seen))
    assert seen == [snowmig.DISCOVERY_MANIFEST_REMOTE]


def test_a_fresh_manifest_is_saved_as_this_runs(tmp_path):
    res = snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=SUBMITTED, stage_params={},
        download=_downloader(_manifest(SUBMITTED + datetime.timedelta(minutes=2)), []))
    assert res["fresh"] is True
    assert (tmp_path / "discovery_manifest.json").exists()


def test_a_manifest_older_than_the_run_is_never_saved_as_this_runs(tmp_path):
    """The live case: the default folder held an earlier run's manifest."""
    (tmp_path / "discovery_manifest.json").write_text("previous", encoding="utf-8")
    res = snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=SUBMITTED, stage_params={},
        download=_downloader(_manifest(SUBMITTED - datetime.timedelta(hours=5)), []))
    assert res["fresh"] is False
    assert "predates this run" in res["message"]
    assert (tmp_path / "discovery_manifest.json").read_text(encoding="utf-8") == "previous"
    assert (tmp_path / "discovery_manifest.STALE.json").exists()


def test_a_manifest_without_a_timestamp_says_freshness_is_unknown(tmp_path):
    res = snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=SUBMITTED, stage_params={},
        download=_downloader(_manifest(None), []))
    assert res["fresh"] is None
    assert "could not be checked" in res["message"]


def test_small_clock_skew_between_cluster_and_laptop_is_not_stale(tmp_path):
    res = snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=SUBMITTED, stage_params={},
        download=_downloader(_manifest(SUBMITTED - datetime.timedelta(minutes=2)), []))
    assert res["fresh"] is True


@pytest.mark.parametrize("value", [
    "C:/Program Files/Git/Workspace/backup-snowflake-migration/reports_r5",
    r"C:\Users\someone\reports",
    "D:/x",
])
def test_a_windows_drive_path_is_refused_as_a_stage_param(value):
    with pytest.raises(ValueError) as e:
        check_stage_params({"reports-dir": value})
    msg = str(e.value)
    assert "Git Bash" in msg and "MSYS_NO_PATHCONV" in msg


def test_a_workspace_path_is_still_accepted():
    check_stage_params({"reports-dir": "/Workspace/backup-snowflake-migration/reports_r5"})


def test_a_refresh_has_no_submission_to_compare_so_freshness_is_not_claimed(tmp_path):
    # `run --refresh` re-reads an existing run and submits nothing: there is
    # no submission time, so the manifest is saved but never called fresh.
    res = snowmig.fetch_discovery_manifest(
        None, workspace="ws", out=tmp_path, submitted_at=None, stage_params={},
        download=_downloader(_manifest(SUBMITTED - datetime.timedelta(hours=5)), []))
    assert res["fresh"] is None
    assert "not checked" in res["message"]
    assert (tmp_path / "discovery_manifest.json").exists()
