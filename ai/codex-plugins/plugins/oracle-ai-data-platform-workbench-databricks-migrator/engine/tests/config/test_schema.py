"""Tests for aidp_config JSON Schema — valid + invalid fixtures."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from aidp_config import ConfigError, load_config  # noqa: E402


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def isolated_env(monkeypatch, tmp_path):
    for k in list(os.environ):
        if k.startswith("AIDP_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    # Every fixture uses `bucket_mapping.path: mapping.json` (relative). The
    # loader now fail-fasts if that file is absent (see loader.py::
    # _check_referenced_files_exist), so materialise a dummy here. Tests that
    # want to prove missing-file behaviour use a fixture with a different path.
    (tmp_path / "mapping.json").write_text("s3_bucket,oci_bucket,oci_namespace\n")
    return tmp_path


def _load(tmp_path: Path, fixture_name: str):
    shutil.copy(FIXTURES / fixture_name, tmp_path / "aidp_migrator.yaml")
    return load_config()


# ─── valid cases ────────────────────────────────────────────────────────────

def test_valid_minimal_passes(isolated_env, tmp_path):
    _load(tmp_path, "minimal.yaml")


def test_valid_full_passes(isolated_env, tmp_path):
    _load(tmp_path, "full.yaml")


def _load_minimal_with_dbx_source(tmp_path: Path, extra_lines: str):
    """Load the minimal fixture with extra lines injected under dbx_source.

    Lets the optional `proxy_max_retries` knob be exercised without a whole new
    fixture file (it is an add-on to the existing dbx_source block).
    """
    text = (FIXTURES / "minimal.yaml").read_text()
    marker = "  host: https://test.cloud.databricks.com\n"
    assert marker in text, "minimal.yaml dbx_source shape changed — update this test"
    (tmp_path / "aidp_migrator.yaml").write_text(text.replace(marker, marker + extra_lines))
    return load_config()


def test_proxy_max_retries_valid(isolated_env, tmp_path):
    # Optional knob (dbx_source.proxy_max_retries) accepts an in-range integer.
    cfg = _load_minimal_with_dbx_source(tmp_path, "  proxy_max_retries: 20\n")
    assert cfg.dbx_source.proxy_max_retries == 20


def test_proxy_max_retries_absent_is_fine(isolated_env, tmp_path):
    # Omitting it must NOT fail (backward-compatible; code defaults to 20).
    cfg = _load(tmp_path, "minimal.yaml")
    assert getattr(cfg.dbx_source, "proxy_max_retries", None) is None


def test_proxy_max_retries_above_cap_fails(isolated_env, tmp_path):
    # Cap is 20 — a fat-fingered value must fail loudly at load, not wedge an
    # export for hours (21 is just over the cap).
    with pytest.raises(ConfigError, match="proxy_max_retries"):
        _load_minimal_with_dbx_source(tmp_path, "  proxy_max_retries: 21\n")


# ─── migration.cluster_idle_timeout_minutes (optional) ───────────────────────

def _load_minimal_with_top_section(tmp_path: Path, extra_top_level: str):
    """Load minimal.yaml with an extra TOP-LEVEL section appended.

    Lets the optional `migration` section be exercised without a whole new
    fixture file.
    """
    text = (FIXTURES / "minimal.yaml").read_text()
    (tmp_path / "aidp_migrator.yaml").write_text(text.rstrip() + "\n" + extra_top_level)
    return load_config()


def test_migration_idle_timeout_valid(isolated_env, tmp_path):
    cfg = _load_minimal_with_top_section(
        tmp_path, "migration:\n  cluster_idle_timeout_minutes: 45\n")
    assert cfg.migration.cluster_idle_timeout_minutes == 45


def test_migration_section_absent_is_fine(isolated_env, tmp_path):
    # Optional: omitting it must not fail (code defaults to 30).
    cfg = _load(tmp_path, "minimal.yaml")
    assert not hasattr(cfg, "migration")


def test_migration_idle_timeout_below_min_fails(isolated_env, tmp_path):
    # Floor is AUTO_TERMINATION_MIN (15); below it must fail loudly at load.
    with pytest.raises(ConfigError, match="cluster_idle_timeout_minutes"):
        _load_minimal_with_top_section(
            tmp_path, "migration:\n  cluster_idle_timeout_minutes: 5\n")


def test_migration_idle_timeout_above_max_fails(isolated_env, tmp_path):
    # Cap is AUTO_TERMINATION_MAX (44640); above it must fail loudly.
    with pytest.raises(ConfigError, match="cluster_idle_timeout_minutes"):
        _load_minimal_with_top_section(
            tmp_path, "migration:\n  cluster_idle_timeout_minutes: 50000\n")


# ─── llm provider/model ──────────────────────────────────────────────────────
# minimal.yaml now ends with a valid `llm:` block (it's required for a runnable
# config). Replace that trailing block to exercise llm validation without a
# duplicate top-level key.

def _load_minimal_replace_llm(tmp_path: Path, llm_block: str):
    text = (FIXTURES / "minimal.yaml").read_text()
    base = text[:text.rindex("\nllm:")].rstrip() + "\n"
    (tmp_path / "aidp_migrator.yaml").write_text(base + (llm_block or ""))
    return load_config()


def test_llm_section_valid(isolated_env, tmp_path):
    cfg = _load_minimal_replace_llm(
        tmp_path,
        "llm:\n  provider: anthropic\n  model: claude-opus-5-5\n")
    assert cfg.llm.provider == "anthropic"
    assert cfg.llm.model == "claude-opus-5-5"


def test_llm_section_absent_passes_schema(isolated_env, tmp_path):
    # `llm` is optional at the SCHEMA level (so unrelated configs still load);
    # presence for the migrator is enforced at read time by llm_config.configure().
    cfg = _load_minimal_replace_llm(tmp_path, "")   # drop the block entirely
    assert not hasattr(cfg, "llm")


def test_llm_missing_model_fails(isolated_env, tmp_path):
    # If the block is present, provider + model are REQUIRED (no code default).
    with pytest.raises(ConfigError, match="model"):
        _load_minimal_replace_llm(tmp_path, "llm:\n  provider: anthropic\n")


def test_llm_missing_provider_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="provider"):
        _load_minimal_replace_llm(tmp_path, "llm:\n  model: claude-opus-5-5\n")


def test_llm_provider_unknown_fails(isolated_env, tmp_path):
    # This bundle dispatches 'anthropic' and 'openai' (openai_model adapter);
    # anything else fails loudly at load, not later in the client.
    with pytest.raises(ConfigError, match="provider"):
        _load_minimal_replace_llm(
            tmp_path, "llm:\n  provider: gemini\n  model: gemini-pro\n")


def test_llm_provider_openai_accepted(isolated_env, tmp_path):
    cfg = _load_minimal_replace_llm(
        tmp_path, "llm:\n  provider: openai\n  model: gpt-5.5\n")
    assert cfg.llm.provider == "openai"


@pytest.mark.parametrize("model", ["claude-opus-5", "claude-opus-5-5"])
def test_llm_model_enum_accepts_opus5_family(isolated_env, tmp_path, model):
    cfg = _load_minimal_replace_llm(
        tmp_path, f"llm:\n  provider: anthropic\n  model: {model}\n")
    assert cfg.llm.model == model


@pytest.mark.parametrize("model", ["claude-sonnet-5", "gpt-4o", "claude-opus-4", ""])
def test_llm_model_outside_opus5_family_fails(isolated_env, tmp_path, model):
    # PR#146: `model` is restricted to the validated Opus-5 family; anything else
    # (typo, unvalidated model, empty) fails LOUDLY at load, not at a runtime 400.
    with pytest.raises(ConfigError):
        _load_minimal_replace_llm(
            tmp_path, f"llm:\n  provider: anthropic\n  model: {model}\n")


@pytest.mark.parametrize("knob", [
    "  thinking: adaptive\n", "  thinking: 'off'\n", "  thinking: 'enabled:8000'\n",
    "  max_tokens: 16000\n", "  max_tokens: 200000\n",
])
def test_llm_unsupported_knobs_rejected(isolated_env, tmp_path, knob):
    # PR#146: thinking + output-budget are FIXED internal constants, not config knobs
    # (on the Opus-5 family `thinking: off` can't disable adaptive thinking and
    # max_tokens is not a true per-request cap). additionalProperties:false makes the
    # schema reject them at load rather than silently accept an unsupported surface.
    with pytest.raises(ConfigError):
        _load_minimal_replace_llm(
            tmp_path, "llm:\n  provider: anthropic\n  model: claude-opus-5-5\n" + knob)


# ─── invalid cases ──────────────────────────────────────────────────────────

def test_missing_required_field_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="lake_ocid"):
        _load(tmp_path, "invalid_missing_required.yaml")


def test_unknown_top_level_key_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="extra_top_level"):
        _load(tmp_path, "invalid_unknown_key.yaml")


def test_bad_region_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="region"):
        _load(tmp_path, "invalid_bad_region.yaml")


def test_bad_ocid_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="lake_ocid"):
        _load(tmp_path, "invalid_bad_ocid.yaml")


def test_bad_uuid_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="workspace_id"):
        _load(tmp_path, "invalid_bad_uuid.yaml")


def test_bad_migrate_output_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="migrate_output"):
        _load(tmp_path, "invalid_bad_migrate_output.yaml")


def test_bad_dbx_host_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="host"):
        _load(tmp_path, "invalid_bad_dbx_host.yaml")


# ─── dbx_source host / proxy alternatives (anyOf) ───────────────────────────

def test_dbx_source_proxy_only_passes(isolated_env, tmp_path):
    """proxy-only YAML must validate (the pilot deployment's actual setup)."""
    cfg = _load(tmp_path, "valid_dbx_source_proxy.yaml")
    assert cfg.dbx_source.proxy.endswith(":8080")


def test_dbx_source_neither_host_nor_proxy_fails(isolated_env, tmp_path):
    """Empty dbx_source block must fail (schema anyOf)."""
    with pytest.raises(ConfigError, match="dbx_source"):
        _load(tmp_path, "invalid_dbx_source_neither.yaml")


def test_dbx_source_bad_proxy_pattern_fails(isolated_env, tmp_path):
    """proxy value missing :port must fail pattern check."""
    with pytest.raises(ConfigError, match="proxy"):
        _load(tmp_path, "invalid_dbx_source_bad_proxy.yaml")


def test_runtime_bound_below_min_fails(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="cell_execute_timeout_s"):
        _load(tmp_path, "invalid_runtime_bounds.yaml")


def test_spark_override_key_must_start_with_spark(isolated_env, tmp_path):
    with pytest.raises(ConfigError, match="not.a.spark.key|spark_config_overrides"):
        _load(tmp_path, "invalid_spark_override_key.yaml")


# ─── credential-store secret shape ──────────────────────────────────────────

def test_credential_store_ref_loads_as_nested_object(isolated_env, tmp_path):
    """Credential-store refs are accepted and preserved as nested attributes.
    Laptop-side does NOT fetch — cluster-side resolves via aidputils.secrets."""
    cfg = _load(tmp_path, "valid_credential_store_ref.yaml")
    ref = cfg.secrets.mlflow_tracking_uri
    # `from` is a Python keyword; access via getattr
    assert getattr(ref, "from") == "aidp_credential_store"
    assert ref.name == "mlflow-tracking-uri"


def test_credential_store_ref_with_key_field(isolated_env, tmp_path):
    """Multi-key credentials use the optional `key:` field to select which
    field to fetch from a single named credential."""
    cfg = _load(tmp_path, "valid_credential_ref_with_key.yaml")
    ref = cfg.secrets.mlflow_tracking_uri
    assert ref.name == "migration_tool_secrets"
    assert ref.key == "mlflow-tracking-uri"


def test_credential_store_ref_missing_name_fails_at_schema(isolated_env, tmp_path):
    """Missing `name` in the credential-store form fails schema validation."""
    with pytest.raises(ConfigError, match="mlflow_tracking_uri"):
        _load(tmp_path, "invalid_credential_ref_missing_name.yaml")


# ─── aidp.oci_credentials block (multi-key OCI signer credentials) ────────

def test_oci_credentials_block_loads(isolated_env, tmp_path):
    """`aidp.oci_credentials` block validates and loads as a nested object.
    Consumed cluster-side by scripts/oci_credential_helper.py::get_signer()."""
    cfg = _load(tmp_path, "valid_oci_credentials_block.yaml")
    oci_creds = cfg.aidp.oci_credentials
    assert oci_creds.name == "migration_tool_api"
    # keys mapping (LHS = Signer field, RHS = credential-store key name)
    assert oci_creds.keys.user == "user"
    assert oci_creds.keys.private_key_content == "key_file_content"


def test_oci_credentials_block_missing_key_fails(isolated_env, tmp_path):
    """All 5 signer fields must be mapped when the block is present."""
    with pytest.raises(ConfigError, match="oci_credentials|private_key_content|region"):
        _load(tmp_path, "invalid_oci_credentials_missing_key.yaml")


def test_oci_credentials_block_absent_is_ok(isolated_env, tmp_path):
    """The block is optional — laptop-side falls back to ~/.oci/config."""
    cfg = _load(tmp_path, "minimal.yaml")
    assert not hasattr(cfg.aidp, "oci_credentials")


# ─── redirect section ──────────────────────────────────────────────────────

def test_valid_redirect_section_passes(isolated_env, tmp_path):
    cfg = _load(tmp_path, "full_with_redirect.yaml")
    assert cfg.redirect.bucket == "sandbox_bucket"
    assert cfg.redirect.namespace == "sandboxns"
    assert cfg.redirect.table_prefix == "default.sandbox_overwrite"


def test_redirect_partial_fails(isolated_env, tmp_path):
    """If redirect is present, all three fields must be there."""
    with pytest.raises(ConfigError, match="namespace|table_prefix"):
        _load(tmp_path, "invalid_redirect_partial.yaml")


def test_redirect_absent_is_ok(isolated_env, tmp_path):
    """No redirect section: the LOADER stays permissive (export / data-check
    flows don't need it); job_migrate.apply_cfg_to_modules enforces it for
    migration runs (see test_runtime_caps_propagation)."""
    cfg = _load(tmp_path, "minimal_no_redirect.yaml")
    assert not hasattr(cfg, "redirect")


# ─── assert_present (per-customer must-be-set assertions) ─────────────────

def test_assert_present_populated_passes(isolated_env, tmp_path):
    cfg = _load(tmp_path, "full_with_redirect.yaml")
    assert "secrets.mlflow_tracking_uri" in cfg.assert_present


def test_assert_present_listed_but_missing_fails(isolated_env, tmp_path):
    """Field listed in assert_present but absent → loader errors."""
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, "invalid_assert_present_missing.yaml")
    msg = str(exc.value)
    assert "assert_present" in msg
    assert "secrets.mlflow_tracking_uri" in msg


# ─── file-existence check (bucket_mapping.path must exist on disk) ────────

def test_bucket_mapping_file_missing_fails(isolated_env, tmp_path):
    """If bucket_mapping.path points at a file that doesn't exist, load fails."""
    (tmp_path / "mapping.json").unlink()  # remove the dummy from isolated_env
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, "minimal.yaml")
    msg = str(exc.value)
    assert "bucket_mapping.path" in msg
    assert "do not exist" in msg
