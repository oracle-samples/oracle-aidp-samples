"""Tests for aidp_config.loader — discovery, merge, env / CLI overrides."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from aidp_config import ConfigError, discover_config_path, load_config  # noqa: E402


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def isolated_env(monkeypatch, tmp_path):
    """Clear AIDP_* env vars and point HOME + cwd at tmp_path with no config file.

    Also materialise a dummy `mapping.json` because the loader's
    _check_referenced_files_exist now fails fast if `bucket_mapping.path` (which
    every fixture sets to `mapping.json`) does not exist on disk.
    """
    for k in list(os.environ):
        if k.startswith("AIDP_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    # Windows: Path.home() ignores HOME (ntpath.expanduser reads USERPROFILE
    # first — bpo-36264, Python 3.8+). Set both so the fixture isolates the
    # home directory on every platform.
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    (tmp_path / "mapping.json").write_text("s3_bucket,oci_bucket,oci_namespace\n")
    return tmp_path


# ─── discovery ──────────────────────────────────────────────────────────────

def test_discover_via_cli(isolated_env, tmp_path):
    p = tmp_path / "custom.yaml"
    p.write_text("aidp: {}")
    assert discover_config_path(str(p)) == p


def test_discover_via_env(isolated_env, monkeypatch, tmp_path):
    p = tmp_path / "env.yaml"
    p.write_text("aidp: {}")
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(p))
    assert discover_config_path() == p


def test_discover_via_cwd(isolated_env, tmp_path):
    p = tmp_path / "aidp_migrator.yaml"
    p.write_text("aidp: {}")
    assert discover_config_path() == p


def test_discover_via_home(isolated_env, tmp_path):
    # HOME = tmp_path; cwd is a subdir so home doesn't collide with cwd
    (tmp_path / "elsewhere").mkdir()
    os.chdir(tmp_path / "elsewhere")
    home_file = tmp_path / "aidp_migrator.yaml"
    home_file.write_text("aidp: {}")
    assert discover_config_path() == home_file


def test_discover_cli_wins_over_env_and_cwd(isolated_env, monkeypatch, tmp_path):
    (tmp_path / "aidp_migrator.yaml").write_text("aidp: {}")
    env_p = tmp_path / "env.yaml"
    env_p.write_text("aidp: {}")
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(env_p))
    cli_p = tmp_path / "cli.yaml"
    cli_p.write_text("aidp: {}")
    assert discover_config_path(str(cli_p)) == cli_p


def test_discover_env_wins_over_cwd(isolated_env, monkeypatch, tmp_path):
    (tmp_path / "aidp_migrator.yaml").write_text("aidp: {}")
    env_p = tmp_path / "env.yaml"
    env_p.write_text("aidp: {}")
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(env_p))
    assert discover_config_path() == env_p


def test_discover_none_raises_with_pointer_to_example(isolated_env):
    with pytest.raises(ConfigError) as exc:
        discover_config_path()
    msg = str(exc.value)
    assert "No customer configuration file found" in msg
    assert "default_aidp_migrator.yaml" in msg


# ─── loading + merge ────────────────────────────────────────────────────────

def _stage(fixture_name: str, tmp_path: Path) -> None:
    shutil.copy(FIXTURES / fixture_name, tmp_path / "aidp_migrator.yaml")


def test_load_minimal_returns_namespace(isolated_env, tmp_path):
    _stage("minimal.yaml", tmp_path)
    cfg = load_config()
    assert isinstance(cfg, SimpleNamespace)
    assert cfg.aidp.oci_profile == "DEFAULT"
    assert cfg.aidp.default_namespace == "exmplnspace1"
    assert cfg.paths.migrate_output == "/Workspace/test/output"
    assert cfg.dbx_source.host == "https://test.cloud.databricks.com"


def test_runtime_values_from_minimal_yaml(isolated_env, tmp_path):
    """Customer YAML is sole source of truth; runtime values are exposed verbatim."""
    _stage("minimal.yaml", tmp_path)
    cfg = load_config()
    assert cfg.runtime.cell_execute_timeout_s == 14400
    assert cfg.runtime.fix_max_attempts == 5
    assert cfg.runtime.ws_recycle_interval_s == 300


def test_runtime_values_from_full_yaml(isolated_env, tmp_path):
    """Different customer YAML → different runtime values (no merge, no defaults file)."""
    _stage("full.yaml", tmp_path)
    cfg = load_config()
    assert cfg.runtime.cell_execute_timeout_s == 7200
    assert cfg.runtime.fix_max_attempts == 5
    assert cfg.runtime.ws_recycle_interval_s == 60


def test_spark_and_autoscale_config_stay_as_dict(isolated_env, tmp_path):
    """Their keys contain dots; SimpleNamespace attribute access would break."""
    _stage("full.yaml", tmp_path)
    cfg = load_config()
    assert isinstance(cfg.spark_config, dict)
    assert cfg.spark_config["spark.sql.autoBroadcastJoinThreshold"] == "200m"
    assert cfg.spark_config["spark.sql.shuffle.partitions"] == 800
    assert cfg.spark_config["spark.sql.execution.arrow.pyspark.selfDestruct.enabled"] is True
    assert isinstance(cfg.autoscale_config, dict)
    assert cfg.autoscale_config["spark.aidp.compute.autoscale.executorIdleTimeoutSeconds"] == 900


def test_only_default_namespace_field_present(isolated_env, tmp_path):
    """Post-Option-A simplification: only `default_namespace` — no nested
    workspace/data_lake/nfs/collections block."""
    _stage("minimal.yaml", tmp_path)
    cfg = load_config()
    assert isinstance(cfg.aidp.default_namespace, str)
    assert not hasattr(cfg.aidp, "namespaces")


def test_source_path_recorded(isolated_env, tmp_path):
    _stage("minimal.yaml", tmp_path)
    cfg = load_config()
    assert cfg._source_path.endswith("aidp_migrator.yaml")


# ─── env overrides ──────────────────────────────────────────────────────────

def test_env_override_wins_over_yaml(isolated_env, monkeypatch, tmp_path):
    _stage("minimal.yaml", tmp_path)
    monkeypatch.setenv("AIDP_OCI_PROFILE", "OVERRIDE_PROFILE")
    cfg = load_config()
    assert cfg.aidp.oci_profile == "OVERRIDE_PROFILE"


def test_readiness_report_env_override_uses_runtime_config(
    isolated_env, monkeypatch, tmp_path
):
    _stage("minimal.yaml", tmp_path)
    monkeypatch.setenv("AIDP_READINESS_REPORT", "/Workspace/reports/ready.json")
    cfg = load_config()
    assert cfg.runtime.readiness_report_path == "/Workspace/reports/ready.json"


def test_env_override_creates_missing_secret(isolated_env, monkeypatch, tmp_path):
    """Customer omitted secrets.mlflow_tracking_uri; env fills it in."""
    _stage("minimal.yaml", tmp_path)
    monkeypatch.setenv("AIDP_MLFLOW_URI", "https://from:env@mlflow.host")
    cfg = load_config()
    assert cfg.secrets.mlflow_tracking_uri == "https://from:env@mlflow.host"


def test_empty_env_value_ignored(isolated_env, monkeypatch, tmp_path):
    """Empty env var must not clobber a real YAML value."""
    _stage("minimal.yaml", tmp_path)
    monkeypatch.setenv("AIDP_OCI_PROFILE", "")
    cfg = load_config()
    assert cfg.aidp.oci_profile == "DEFAULT"


# ─── CLI overrides ──────────────────────────────────────────────────────────

def test_cli_override_wins_over_env(isolated_env, monkeypatch, tmp_path):
    _stage("minimal.yaml", tmp_path)
    monkeypatch.setenv("AIDP_OCI_PROFILE", "FROM_ENV")
    cfg = load_config(cli_overrides={"aidp.oci_profile": "FROM_CLI"})
    assert cfg.aidp.oci_profile == "FROM_CLI"


def test_cli_override_none_ignored(isolated_env, tmp_path):
    """None CLI values (unset flags) must not clobber YAML."""
    _stage("minimal.yaml", tmp_path)
    cfg = load_config(cli_overrides={"aidp.oci_profile": None})
    assert cfg.aidp.oci_profile == "DEFAULT"


def test_cli_override_dotted_path_creates_missing_parents(isolated_env, tmp_path):
    _stage("minimal.yaml", tmp_path)
    cfg = load_config(cli_overrides={"paths.migrate_output": "/Workspace/override"})
    assert cfg.paths.migrate_output == "/Workspace/override"
