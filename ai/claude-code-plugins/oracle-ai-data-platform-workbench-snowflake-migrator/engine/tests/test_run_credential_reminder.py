"""After a copy job (or the reconcile) SUCCEEDS, `run` reminds the operator
that the Snowflake credential `provision --source-config` placed is still on
the workspace, and what removes it once the copies are done (#175).

Only from an executed provision record of the SAME workspace the job ran
on: a record in this out dir may be another migration's. Never on a failed
run (a re-run needs the credential), and never changes the exit code.
"""
import json

import pytest

import snowmig
from test_run_command import Runs, _args, _install, _no_real_sleep  # noqa: F401

CRED = "backup-snowflake-migration/plan/snowmig-config.json"


def _record(tmp_path, **over):
    rec = {"dry_run": False, "workspace": {"name": "lab", "key": "ws"},
           "credential_objects": [CRED]}
    rec.update(over)
    (tmp_path / "provision_result.json").write_text(json.dumps(rec))


@pytest.mark.parametrize("job", ["snowmig_02_copy_sales",
                                 "snowmig_03_reconcile"])
def test_a_successful_copy_or_reconcile_reminds(tmp_path, monkeypatch,
                                                capsys, job):
    _record(tmp_path)
    _install(monkeypatch, Runs("SUCCESS"))
    assert snowmig.cmd_run(_args(tmp_path, job=job)) == 0
    err = capsys.readouterr().err
    assert "SNOWFLAKE CREDENTIAL" in err and CRED in err
    assert "teardown --scope credential --execute" in err


def test_a_failed_copy_does_not(tmp_path, monkeypatch, capsys):
    _record(tmp_path)
    _install(monkeypatch, Runs("FAILED"))
    assert snowmig.cmd_run(_args(tmp_path, job="snowmig_02_copy_sales")) == 1
    assert "SNOWFLAKE CREDENTIAL" not in capsys.readouterr().err


def test_another_stage_does_not(tmp_path, monkeypatch, capsys):
    _record(tmp_path)
    _install(monkeypatch, Runs("SUCCESS"))
    assert snowmig.cmd_run(_args(tmp_path, job="snowmig_01_structure")) == 0
    assert "SNOWFLAKE CREDENTIAL" not in capsys.readouterr().err


@pytest.mark.parametrize("over", [
    {"workspace": {"name": "other", "key": "ws-other"}},   # another workspace
    {"dry_run": True},                                    # never placed
    {"credential_objects": []},                           # no --source-config
])
def test_a_record_that_does_not_place_it_here_does_not(tmp_path, monkeypatch,
                                                       capsys, over):
    _record(tmp_path, **over)
    _install(monkeypatch, Runs("SUCCESS"))
    assert snowmig.cmd_run(_args(tmp_path, job="snowmig_02_copy_sales")) == 0
    assert "SNOWFLAKE CREDENTIAL" not in capsys.readouterr().err


def test_no_record_at_all_does_not(tmp_path, monkeypatch, capsys):
    _install(monkeypatch, Runs("SUCCESS"))
    assert snowmig.cmd_run(_args(tmp_path, job="snowmig_02_copy_sales")) == 0
    assert "SNOWFLAKE CREDENTIAL" not in capsys.readouterr().err
