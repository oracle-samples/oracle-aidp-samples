"""Unit tests for the Phase 4.1 bootstrap dispatch-mode plumbing.

Step 3 only — CLI option threading + the cluster-dispatch resolution
chain. The cluster-mode behaviour gates (missing-field check,
``--skip-preonboarding-probes`` conflict, mandatory ``aidp-rest``
probe) ship with Step 9 + their tests land alongside.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.bootstrap import (
    ResolvedClusterDispatchConfig,
    _ENV_VAR_CLUSTER_KEY,
    _ENV_VAR_CLUSTER_NAME,
    _ENV_VAR_WORKSPACE_DIR,
    _resolve_cluster_dispatch_config,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Defaults, EnvSpec


def _env(**overrides) -> EnvSpec:
    base = {"workspaceKey": "ws-abc-123"}
    base.update(overrides)
    return EnvSpec.model_validate(base)


def _defaults(**overrides) -> Defaults:
    return Defaults.model_validate(overrides)


# ---------------------------------------------------------------------------
# Override chain: CLI flag → env var → YAML
# ---------------------------------------------------------------------------


class TestResolveClusterDispatchConfigOverrideChain:
    def test_cli_override_wins_over_env_var(self, monkeypatch) -> None:
        monkeypatch.setenv(_ENV_VAR_CLUSTER_KEY, "from-env")
        env = _env(clusterKey="from-yaml")
        resolved = _resolve_cluster_dispatch_config(
            env,
            _defaults(),
            cluster_key_override="from-cli",
        )
        assert resolved.cluster_key == "from-cli"

    def test_env_var_wins_over_yaml_when_no_cli_flag(self, monkeypatch) -> None:
        monkeypatch.setenv(_ENV_VAR_CLUSTER_KEY, "from-env")
        env = _env(clusterKey="from-yaml")
        resolved = _resolve_cluster_dispatch_config(env, _defaults())
        assert resolved.cluster_key == "from-env"

    def test_yaml_used_when_no_cli_or_env(self, monkeypatch) -> None:
        # Make sure no leakage from the test runner's env.
        monkeypatch.delenv(_ENV_VAR_CLUSTER_KEY, raising=False)
        env = _env(clusterKey="from-yaml", clusterName="cluster_dev")
        resolved = _resolve_cluster_dispatch_config(env, _defaults())
        assert resolved.cluster_key == "from-yaml"
        assert resolved.cluster_name == "cluster_dev"

    def test_workspace_dir_chain(self, monkeypatch) -> None:
        # YAML-only.
        monkeypatch.delenv(_ENV_VAR_WORKSPACE_DIR, raising=False)
        resolved = _resolve_cluster_dispatch_config(
            _env(),
            _defaults(workspaceDir="/Workspace/Custom/path"),
        )
        assert resolved.workspace_dir == "/Workspace/Custom/path"
        # CLI override.
        resolved = _resolve_cluster_dispatch_config(
            _env(),
            _defaults(workspaceDir="/yaml"),
            workspace_dir_override="/cli",
        )
        assert resolved.workspace_dir == "/cli"


# ---------------------------------------------------------------------------
# Derived defaults / missing-fields detection
# ---------------------------------------------------------------------------


class TestResolveClusterDispatchConfigDerived:
    def test_workspace_dir_derives_when_fully_unresolved(self, monkeypatch) -> None:
        monkeypatch.delenv(_ENV_VAR_WORKSPACE_DIR, raising=False)
        resolved = _resolve_cluster_dispatch_config(
            _env(), _defaults(workspaceRoot="Team")
        )
        assert resolved.workspace_dir == "/Workspace/Team/fusion-autopilot-bootstrap"

    def test_workspace_dir_uses_default_workspace_root(self, monkeypatch) -> None:
        monkeypatch.delenv(_ENV_VAR_WORKSPACE_DIR, raising=False)
        resolved = _resolve_cluster_dispatch_config(_env(), _defaults())
        # `Defaults.workspace_root` defaults to "Shared".
        assert resolved.workspace_dir == "/Workspace/Shared/fusion-autopilot-bootstrap"

    def test_missing_fields_when_cluster_coords_absent(self, monkeypatch) -> None:
        monkeypatch.delenv(_ENV_VAR_CLUSTER_KEY, raising=False)
        monkeypatch.delenv(_ENV_VAR_CLUSTER_NAME, raising=False)
        resolved = _resolve_cluster_dispatch_config(_env(), _defaults())
        missing = resolved.missing_fields()
        # All three dispatch coords absent on a bare EnvSpec.
        assert "aiDataPlatformId" in missing
        assert "clusterKey" in missing
        assert "clusterName" in missing

    def test_missing_fields_empty_when_all_resolved(self, monkeypatch) -> None:
        monkeypatch.delenv(_ENV_VAR_CLUSTER_KEY, raising=False)
        monkeypatch.delenv(_ENV_VAR_CLUSTER_NAME, raising=False)
        env = _env(
            aiDataPlatformId="ocid1.aidp.test",
            clusterKey="cl-uuid",
            clusterName="cluster_dev",
        )
        resolved = _resolve_cluster_dispatch_config(env, _defaults())
        assert resolved.missing_fields() == []

    def test_region_falls_back_to_us_ashburn_1(self) -> None:
        # No region in env or defaults → hardcoded fallback.
        resolved = _resolve_cluster_dispatch_config(_env(), _defaults())
        assert resolved.region == "us-ashburn-1"

    def test_resolved_config_is_frozen(self) -> None:
        resolved = _resolve_cluster_dispatch_config(_env(), _defaults())
        with pytest.raises((AttributeError, TypeError)):
            resolved.cluster_key = "mutated"  # type: ignore[misc]


class TestResolvedClusterDispatchConfigShape:
    def test_dataclass_round_trip(self) -> None:
        cfg = ResolvedClusterDispatchConfig(
            aidp_id="ocid1.aidp.x",
            workspace_key="ws-1",
            cluster_key="cl-1",
            cluster_name="cluster_dev",
            region="us-ashburn-1",
            oci_profile="DEFAULT",
            workspace_dir="/Workspace/Shared/fusion-autopilot-bootstrap",
        )
        assert cfg.missing_fields() == []


# ---------------------------------------------------------------------------
# Step 9 — AIDPF-2047 gates (conflicting_flags / aidp_rest_probe_failed /
# missing_config). All three exit non-zero with no diagnostic artifact;
# the CLI message carries the reason enum + remediation.
# ---------------------------------------------------------------------------


class TestAIDPF2047Gates:
    """End-to-end CLI invocations covering the three AIDPF-2047
    reason enums. Uses ``CliRunner`` since the gates live in the
    bootstrap CLI's main control flow, not in a single helper."""

    def _bundle_files(self, tmp_path):
        """Minimal valid bundle + aidp.config that the gate code
        loads before exiting. Reuses the shape from
        ``test_cli_bootstrap_phase3a.py``."""
        bundle_path = tmp_path / "bundle.yaml"
        config_path = tmp_path / "aidp.config.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "name: test-bundle\n"
            "aidp:\n"
            "  catalog: fusion_catalog\n"
            "  bronzeSchema: bronze\n"
            "  silverSchema: silver\n"
            "  goldSchema: gold\n"
            "fusion:\n"
            "  serviceUrl: https://example.com\n"
            "  username: u\n"
            "  password: p\n"
            "  externalStorage: stor\n"
            "datasets: []\n",
            encoding="utf-8",
        )
        config_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: test\n"
            "environments:\n"
            "  dev:\n"
            "    workspaceKey: ws-test\n",
            encoding="utf-8",
        )
        return bundle_path, config_path

    def test_cluster_plus_skip_probes_emits_conflicting_flags(self, tmp_path) -> None:
        from click.testing import CliRunner
        from oracle_ai_data_platform_fusion_autopilot.cli import main

        bundle_path, config_path = self._bundle_files(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--bundle", str(bundle_path),
                "--config", str(config_path),
                "bootstrap",
                "--dispatch-mode", "cluster",
                "--skip-preonboarding-probes",
            ],
        )
        assert result.exit_code != 0
        assert "AIDPF-2047" in result.output
        assert "conflicting_flags" in result.output
        # No diagnostic artifact written — Step 8 contract.
        diag = tmp_path / ".aidp" / "diagnostics"
        assert not diag.exists() or not any(diag.iterdir())

    def test_cluster_missing_cluster_key_emits_missing_config(
        self, tmp_path, monkeypatch
    ) -> None:
        """Calls the bootstrap impl directly with mocked phase-1
        results so the gate at AIDPF-2047 (reason=missing_config)
        fires deterministically — sidesteps schema validation +
        OCI-SDK-dependent probe behaviour."""
        from io import StringIO
        from unittest.mock import MagicMock, patch

        from rich.console import Console

        from oracle_ai_data_platform_fusion_autopilot.commands.bootstrap import (
            _ProbeResult,
            bootstrap as bootstrap_impl,
        )

        # Clear env var leakage so the missing-config path is exercised.
        monkeypatch.delenv(_ENV_VAR_CLUSTER_KEY, raising=False)
        monkeypatch.delenv(_ENV_VAR_CLUSTER_NAME, raising=False)

        # Fake bundle + config with a contentPack so phase 2 starts.
        fake_bundle = MagicMock(name="Bundle")
        fake_bundle.content_pack = MagicMock(profile="finance-default")
        fake_config = MagicMock(name="AidpConfig")
        fake_env = _env()  # EnvSpec with no clusterKey / clusterName
        fake_config.environments = {"dev": fake_env}
        fake_config.defaults = _defaults()

        buf = StringIO()
        console = Console(file=buf, force_terminal=False, color_system=None, width=200)

        def _fake_load(bundle_path, config_path, results):
            results.append(_ProbeResult("bundle.yaml", "PASS", "0 datasets"))
            return fake_bundle, fake_config

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.bootstrap._load",
            side_effect=_fake_load,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.bootstrap._probe_bicc"
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.bootstrap._probe_aidp",
            side_effect=lambda env, results: results.append(
                _ProbeResult("aidp-rest", "PASS", "workspace reachable")
            ),
        ):
            exit_code = bootstrap_impl(
                bundle_path=tmp_path / "bundle.yaml",
                config_path=tmp_path / "aidp.config.yaml",
                env_name="dev",
                console=console,
                dispatch_mode="cluster",
            )
        output = buf.getvalue()
        assert exit_code == 1, output
        assert "AIDPF-2047" in output
        assert "missing_config" in output
        # No diagnostic artifact written — Step 8 contract.
        diag = tmp_path / ".aidp" / "diagnostics"
        assert not diag.exists() or not any(diag.iterdir())
