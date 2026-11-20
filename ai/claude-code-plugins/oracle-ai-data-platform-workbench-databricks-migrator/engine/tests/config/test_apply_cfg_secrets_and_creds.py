"""Verify apply_cfg_to_modules handles credential-store refs + wires env vars.

- Secret refs must NOT crash the loader-integration in apply_cfg_to_modules
  (previously it called `.replace(...)` on the raw dict → AttributeError).
- aidp.oci_credentials must set AIDP_CRED_OCI_NAME + AIDP_CRED_OCI_KEY_*
  env vars so the kernel bootstrap snippet + scripts/oci_credential_helper.py
  + aidp_compat modules can use the customer's chosen credential name.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in [
        "AIDP_CRED_OCI_NAME",
        "AIDP_CRED_OCI_KEY_USER",
        "AIDP_CRED_OCI_KEY_FINGERPRINT",
        "AIDP_CRED_OCI_KEY_TENANCY",
        "AIDP_CRED_OCI_KEY_REGION",
        "AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT",
    ]:
        monkeypatch.delenv(var, raising=False)


def _stage_and_load(tmp_path, fixture_name):
    import shutil
    fixtures = Path(__file__).parent / "fixtures"
    dest = tmp_path / "aidp_migrator.yaml"
    shutil.copy(fixtures / fixture_name, dest)
    (tmp_path / "mapping.json").write_text("s3_bucket,oci_bucket,oci_namespace\n")
    import os as _os
    _cwd = _os.getcwd()
    _prev_home = _os.environ.get("HOME")
    _os.chdir(tmp_path)
    _os.environ["HOME"] = str(tmp_path)
    try:
        from aidp_config import load_config
        return load_config()
    finally:
        _os.chdir(_cwd)
        # restore HOME: leaking it poisons config discovery for every later
        # test in the process (first tripped by the curated-pip caller pins)
        if _prev_home is None:
            _os.environ.pop("HOME", None)
        else:
            _os.environ["HOME"] = _prev_home


def test_apply_cfg_with_ref_secret_does_not_crash(tmp_path, monkeypatch):
    """Loading a cfg where secrets are credential-store refs must not raise —
    the old resolver used to blow up on .replace() against a dict."""
    cfg = _stage_and_load(tmp_path, "valid_credential_ref_with_key.yaml")
    import job_migrate as jm
    # apply_cfg_to_modules should complete cleanly with a ref secret.
    jm.apply_cfg_to_modules(cfg, bucket_mapping_path=str(tmp_path / "mapping.json"))
    # _ENTITY_CRYPTO_ARN should be empty (ref shape → migrator can't resolve laptop-side;
    # _inject_entity_crypto_arn skips injection and logs a warning).
    assert jm._ENTITY_CRYPTO_ARN == ""


def test_apply_cfg_config_only_caller_can_skip_llm(tmp_path, monkeypatch):
    """`apply_cfg_to_modules(configure_llm=False)` is for **config-only callers/tests**
    that never touch the LLM — it must NOT require an `llm` block and must leave the
    model un-configured. (This is NOT the --direct-execute contract anymore: the
    migrator entrypoints always pass True — pinned by the next test.) The default
    (AI) path must still fail loudly when `llm` is absent (no code default)."""
    cfg = _stage_and_load(tmp_path, "minimal.yaml")
    if hasattr(cfg, "llm"):
        delattr(cfg, "llm")                       # simulate a config with no llm block
    import job_migrate as jm
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", None)   # pretend un-configured (auto-restored)

    # config-only caller: no llm block required, model stays un-configured
    jm.apply_cfg_to_modules(cfg, bucket_mapping_path=str(tmp_path / "mapping.json"),
                            configure_llm=False)
    assert llm_config.MODEL is None

    # AI path (default): a missing llm block must fail loudly
    with pytest.raises(llm_config.LLMConfigError):
        jm.apply_cfg_to_modules(cfg, bucket_mapping_path=str(tmp_path / "mapping.json"))


def test_production_entrypoints_always_configure_llm():
    """PR#146 #3: BOTH migrator entrypoints must pass `configure_llm=True` — even for
    --direct-execute — because the failure circuit-breaker (`monitor_analyze_failures`)
    and child/Scala fix paths can invoke the LLM before the direct-mode guard, so
    skipping setup left MODEL=None and crashed. The old
    `configure_llm=not args.direct_execute` wiring must be gone."""
    import os
    import job_migrate as jm
    scripts = os.path.dirname(os.path.abspath(jm.__file__))
    for name in ("job_migrate.py", "job_migrate_from_workflow.py"):
        src = open(os.path.join(scripts, name), encoding="utf-8").read()
        # match the actual CALL argument (`configure_llm=True)` — closing paren), not a
        # mere mention in a comment (which would false-pass).
        assert "configure_llm=True)" in src, f"{name} must pass configure_llm=True in the call"
        assert "configure_llm=not args.direct_execute" not in src, \
            f"{name} still has the old direct-execute LLM-skip wiring"


def test_apply_cfg_oci_credentials_block_sets_env_vars(tmp_path, monkeypatch):
    """cfg.aidp.oci_credentials → AIDP_CRED_OCI_NAME + AIDP_CRED_OCI_KEY_* env vars.
    Downstream: scripts/oci_credential_helper reads them, aidp_compat reads them,
    build_env_bootstrap_snippet propagates them to the cluster kernel."""
    cfg = _stage_and_load(tmp_path, "valid_oci_credentials_block.yaml")
    import job_migrate as jm
    jm.apply_cfg_to_modules(cfg, bucket_mapping_path=str(tmp_path / "mapping.json"))

    assert os.environ.get("AIDP_CRED_OCI_NAME") == "migration_tool_api"
    assert os.environ.get("AIDP_CRED_OCI_KEY_USER") == "user"
    assert os.environ.get("AIDP_CRED_OCI_KEY_FINGERPRINT") == "fingerprint"
    assert os.environ.get("AIDP_CRED_OCI_KEY_TENANCY") == "tenancy"
    assert os.environ.get("AIDP_CRED_OCI_KEY_REGION") == "region"
    assert os.environ.get("AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT") == "key_file_content"


def test_apply_cfg_no_oci_credentials_leaves_env_untouched(tmp_path, monkeypatch):
    """No cfg.aidp.oci_credentials → env vars remain unset. Cluster-side falls
    back to defaults (`migration_tool_api` etc.)."""
    cfg = _stage_and_load(tmp_path, "minimal.yaml")
    import job_migrate as jm
    jm.apply_cfg_to_modules(cfg, bucket_mapping_path=str(tmp_path / "mapping.json"))

    assert "AIDP_CRED_OCI_NAME" not in os.environ
    assert "AIDP_CRED_OCI_KEY_USER" not in os.environ


def test_apply_cfg_sets_secrets_credential_name_env_var(tmp_path, monkeypatch):
    """cfg.secrets.<field>.name → AIDP_CRED_SECRETS_NAME env var.

    Consumed by Opus-emitted MLflow cells. Non-default names must propagate
    for a generic customer to work end-to-end."""
    cfg = _stage_and_load(tmp_path, "valid_credential_ref_with_key.yaml")
    import job_migrate as jm
    jm.apply_cfg_to_modules(cfg, bucket_mapping_path=str(tmp_path / "mapping.json"))
    assert os.environ.get("AIDP_CRED_SECRETS_NAME") == "migration_tool_secrets"


def test_apply_cfg_ref_secret_env_override_does_not_downgrade(tmp_path, monkeypatch):
    """A stale AIDP_MLFLOW_URI shell export MUST NOT silently clobber a
    credential-store ref with plaintext (would leak the secret into prompts /
    logs — the exact regression Phase 2 aimed to eliminate)."""
    monkeypatch.setenv("AIDP_MLFLOW_URI", "https://stale:leak@mlflow.example")
    cfg = _stage_and_load(tmp_path, "valid_credential_ref_with_key.yaml")
    # secrets.mlflow_tracking_uri should still be a ref (dict), not the env-var plaintext.
    ref = cfg.secrets.mlflow_tracking_uri
    assert hasattr(ref, "name"), "env override silently downgraded ref to plaintext string"
    assert ref.name == "migration_tool_secrets"


def test_apply_cfg_env_vars_cleared_between_calls(tmp_path, monkeypatch):
    """Second apply_cfg_to_modules() with a leaner cfg must NOT inherit env
    vars set by the first call — env vars are cleared before conditional
    re-set, so a customer switching cfgs mid-process cannot carry stale
    credential names into aidp_compat / Opus-emitted cells."""
    cfg_full = _stage_and_load(tmp_path, "valid_oci_credentials_block.yaml")
    import job_migrate as jm
    jm.apply_cfg_to_modules(cfg_full, bucket_mapping_path=str(tmp_path / "mapping.json"))
    assert os.environ.get("AIDP_CRED_OCI_NAME") == "migration_tool_api"

    tmp_path2 = tmp_path / "minimal_dir"
    tmp_path2.mkdir()
    cfg_min = _stage_and_load(tmp_path2, "minimal.yaml")
    jm.apply_cfg_to_modules(cfg_min, bucket_mapping_path=str(tmp_path2 / "mapping.json"))
    # env vars set by the earlier call must be cleared now that cfg lacks the block.
    assert "AIDP_CRED_OCI_NAME" not in os.environ
    assert "AIDP_CRED_OCI_KEY_USER" not in os.environ
