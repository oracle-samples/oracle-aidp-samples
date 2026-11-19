"""Unit tests for ``aidp-fusion-autopilot init-config``.

The command's only network surface is ``AidpRestClient``; these tests mock
it so we exercise name→key resolution, the YAML merge/write, the collision
guard, and the graceful error paths without a live tenant.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner
from oracle_ai_data_platform_fusion_autopilot import cli
from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
    AidpRestError,
    ClusterSummary,
    WorkspaceSummary,
)

CLIENT_PATH = "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient"


def _fake_client(*, cluster_state: str = "ACTIVE") -> MagicMock:
    client = MagicMock()
    client.find_workspace_by_name.return_value = WorkspaceSummary(
        key="ws-uuid-123", display_name="My Workspace"
    )
    client.find_cluster_by_name.return_value = ClusterSummary(
        key="cluster-uuid-456", display_name="My Cluster", state=cluster_state
    )
    return client


def _args(*extra: str) -> list[str]:
    """Build an init-config argv with the three required flags + any extra tokens."""
    out: list[str] = [
        "init-config",
        "--aidp-id", "ocid1.datalake.oc1.iad.aaaa",
        "--workspace", "My Workspace",
        "--cluster", "My Cluster",
    ]
    out += list(extra)
    return out


class TestInitConfig:
    def test_resolves_names_and_writes(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, return_value=_fake_client()):
            result = CliRunner().invoke(cli.main, _args())
        assert result.exit_code == 0, result.output

        cfg = yaml.safe_load((tmp_path / "aidp.config.yaml").read_text())
        env = cfg["environments"]["dev"]
        assert env["workspaceKey"] == "ws-uuid-123"
        assert env["clusterKey"] == "cluster-uuid-456"
        assert env["aiDataPlatformId"] == "ocid1.datalake.oc1.iad.aaaa"

    def test_dry_run_writes_nothing(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, return_value=_fake_client()):
            result = CliRunner().invoke(cli.main, _args("--dry-run"))
        assert result.exit_code == 0
        assert "ws-uuid-123" in result.output
        assert not (tmp_path / "aidp.config.yaml").exists()

    def test_inactive_cluster_warns_but_writes(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, return_value=_fake_client(cluster_state="STOPPED")):
            result = CliRunner().invoke(cli.main, _args())
        assert result.exit_code == 0
        assert "STOPPED" in result.output
        assert (tmp_path / "aidp.config.yaml").exists()

    def test_unknown_workspace_lists_available(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        client = _fake_client()
        client.find_workspace_by_name.side_effect = AidpRestError("no workspace")
        client.list_workspaces.return_value = [
            WorkspaceSummary(key="k1", display_name="Alpha WS"),
            WorkspaceSummary(key="k2", display_name="Beta WS"),
        ]
        with patch(CLIENT_PATH, return_value=client):
            result = CliRunner().invoke(cli.main, _args())
        assert result.exit_code == 1
        assert "Alpha WS" in result.output and "Beta WS" in result.output
        assert not (tmp_path / "aidp.config.yaml").exists()

    def test_collision_requires_force(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, return_value=_fake_client()):
            first = CliRunner().invoke(cli.main, _args())
            assert first.exit_code == 0
            again = CliRunner().invoke(cli.main, _args())
        assert again.exit_code == 2
        assert "already exists" in again.output

    def test_force_overwrites_existing_env(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, return_value=_fake_client()):
            CliRunner().invoke(cli.main, _args())
            result = CliRunner().invoke(cli.main, _args("--force"))
        assert result.exit_code == 0

    def test_second_env_preserves_sibling(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, return_value=_fake_client()):
            CliRunner().invoke(cli.main, _args())
            # target a different env via the top-level --env flag
            result = CliRunner().invoke(cli.main, ["--env", "prod", *_args()])
        assert result.exit_code == 0
        cfg = yaml.safe_load((tmp_path / "aidp.config.yaml").read_text())
        assert set(cfg["environments"]) == {"dev", "prod"}

    def test_bad_oci_profile_is_graceful(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        with patch(CLIENT_PATH, side_effect=Exception("Profile 'X' not found")):
            result = CliRunner().invoke(cli.main, _args("--oci-profile", "X"))
        assert result.exit_code == 1
        assert "Could not initialise" in result.output
        assert not (tmp_path / "aidp.config.yaml").exists()
