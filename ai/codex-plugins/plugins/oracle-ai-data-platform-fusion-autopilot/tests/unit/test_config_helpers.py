"""P1.5ε §Step 7a — commands/_config_helpers.py tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands._config_helpers import (
    env_or_error,
    load_aidp_config,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import AidpConfig
from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
    OrchestratorConfigError,
)


_GOOD_CONFIG = """\
apiVersion: aidp-fusion-autopilot/v1
project: test
environments:
  dev:
    workspaceKey: wk-123
    region: us-ashburn-1
    ociProfile: AIDP_SESSION
"""


class TestLoadAidpConfig:
    def test_happy_path_returns_aidp_config(self, tmp_path: Path) -> None:
        p = tmp_path / "aidp.config.yaml"
        p.write_text(_GOOD_CONFIG)
        config = load_aidp_config(p)
        assert isinstance(config, AidpConfig)
        assert config.project == "test"
        assert "dev" in config.environments

    def test_missing_file_raises_with_path(self, tmp_path: Path) -> None:
        with pytest.raises(OrchestratorConfigError, match="not found"):
            load_aidp_config(tmp_path / "missing.yaml")

    def test_invalid_yaml_raises_with_parse_error_prefix(
        self, tmp_path: Path
    ) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text("not: valid: yaml: at: all")
        with pytest.raises(OrchestratorConfigError, match="YAML parse error"):
            load_aidp_config(p)

    def test_schema_error_raises_with_schema_prefix(
        self, tmp_path: Path
    ) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text("apiVersion: aidp-fusion-autopilot/v1\nproject: x\n")  # no environments
        with pytest.raises(OrchestratorConfigError, match="schema errors"):
            load_aidp_config(p)


class TestEnvOrError:
    def test_returns_env_block(self, tmp_path: Path) -> None:
        p = tmp_path / "aidp.config.yaml"
        p.write_text(_GOOD_CONFIG)
        config = load_aidp_config(p)
        env = env_or_error(config, "dev")
        assert env.workspace_key == "wk-123"

    def test_unknown_env_lists_available_names(self, tmp_path: Path) -> None:
        p = tmp_path / "aidp.config.yaml"
        p.write_text(_GOOD_CONFIG)
        config = load_aidp_config(p)
        with pytest.raises(OrchestratorConfigError, match="\\['dev'\\]") as ei:
            env_or_error(config, "production")
        assert "production" in str(ei.value)
