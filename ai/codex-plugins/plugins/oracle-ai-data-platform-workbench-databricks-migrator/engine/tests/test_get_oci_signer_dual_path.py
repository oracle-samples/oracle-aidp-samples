"""Dual-path OCI signer (aidp_executor.get_oci_signer).

The scripts run from BOTH a laptop and an AIDP cluster:
  * cluster — no ~/.oci/config; auth comes from the AIDP Credential Store via
    the kernel-injected `aidputils` (cluster-only). Driven by the YAML's
    `aidp.oci_credentials` block (name + key map).
  * laptop  — `aidputils` is unavailable (`import aidputils` fails), so auth
    comes from `oci.config.from_file` (~/.oci/config or $OCI_CONFIG_FILE).

`get_oci_signer` tries the credential-store path first and falls back to
`from_file` on ImportError — the ImportError IS the "am I on a laptop?" signal.
These tests are hermetic (no real kernel, key, or network): they mock the
credential-store helper and the config loader.
"""
import os
import sys
import types

import pytest

_SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

import aidp_executor as ae  # noqa: E402
import oci_auth as oa  # noqa: E402  # implementation home; ae re-exports


def _clear_cred_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("AIDP_CRED_OCI"):
            monkeypatch.delenv(k, raising=False)


def test_cluster_path_used_when_aidputils_available(monkeypatch):
    """On-cluster: credential-store signer available → used, config is None."""
    _clear_cred_env(monkeypatch)
    sentinel = object()
    fake_helper = types.ModuleType("oci_credential_helper")
    fake_helper.get_signer = lambda: sentinel            # aidputils present → no ImportError
    monkeypatch.setitem(sys.modules, "oci_credential_helper", fake_helper)
    monkeypatch.setattr(oa, "_apply_cred_env_from_config", lambda *a, **k: None)

    config, signer = ae.get_oci_signer("DEFAULT")
    assert signer is sentinel
    assert config is None


def test_laptop_fallback_when_aidputils_missing(monkeypatch):
    """Off-cluster: credential-store helper raises ImportError → from_file path."""
    _clear_cred_env(monkeypatch)
    fake_helper = types.ModuleType("oci_credential_helper")

    def _raise():
        raise ImportError("aidputils global not found on __main__ — cluster-only")

    fake_helper.get_signer = _raise
    monkeypatch.setitem(sys.modules, "oci_credential_helper", fake_helper)
    monkeypatch.setattr(oa, "_apply_cred_env_from_config", lambda *a, **k: None)
    monkeypatch.setenv("OCI_CONFIG_FILE", "/tmp/__no_such_oci_config__")

    import oci
    with pytest.raises(oci.exceptions.ConfigFileNotFound):
        ae.get_oci_signer("DEFAULT")     # fell through to from_file (laptop behavior, unchanged)


class _FakeConfigError(RuntimeError):
    """Stand-in for aidp_config.ConfigError (raised on discovery/parse/validation)."""


def _install_fake_aidp_config(monkeypatch, *, load_config, discover=lambda *a, **k: "/x/aidp_migrator.yaml"):
    """Install a fake aidp_config module exporting the three names _apply now imports."""
    fake_ac = types.ModuleType("aidp_config")
    fake_ac.load_config = load_config
    fake_ac.discover_config_path = discover
    fake_ac.ConfigError = _FakeConfigError
    monkeypatch.setitem(sys.modules, "aidp_config", fake_ac)
    return fake_ac


def test_apply_cred_env_reads_yaml_block(monkeypatch):
    """The AIDP_CRED_* env is sourced from cfg.aidp.oci_credentials — not hardcoded."""
    _clear_cred_env(monkeypatch)
    from types import SimpleNamespace
    fake_cfg = SimpleNamespace(aidp=SimpleNamespace(oci_credentials=SimpleNamespace(
        name="migration_tool_api_test",
        keys=SimpleNamespace(user="user", fingerprint="fingerprint", tenancy="tenancy",
                             region="region", private_key_content="key_file_content"))))
    _install_fake_aidp_config(monkeypatch, load_config=lambda *a, **k: fake_cfg)

    ae._apply_cred_env_from_config()
    assert os.environ["AIDP_CRED_OCI_NAME"] == "migration_tool_api_test"
    assert os.environ["AIDP_CRED_OCI_KEY_TENANCY"] == "tenancy"
    assert os.environ["AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT"] == "key_file_content"


def test_absent_config_is_silent_noop(monkeypatch):
    """No config file discoverable → silent fallback to default cred names (no raise)."""
    _clear_cred_env(monkeypatch)

    def _no_config(*a, **k):
        raise _FakeConfigError("No customer configuration file found")

    # discover raises 'absent'; load_config must never be reached
    def _must_not_load(*a, **k):
        raise AssertionError("load_config must not run when no config is discoverable")

    _install_fake_aidp_config(monkeypatch, load_config=_must_not_load, discover=_no_config)
    ae._apply_cred_env_from_config()                 # must NOT raise
    assert "AIDP_CRED_OCI_NAME" not in os.environ    # fell back to defaults


def test_invalid_config_propagates_not_swallowed(monkeypatch):
    """A discoverable-but-invalid config must FAIL LOUD, not silently use defaults."""
    _clear_cred_env(monkeypatch)

    def _bad_load(*a, **k):
        raise _FakeConfigError("schema validation failed: aidp.oci_credentials.name: typo")

    # discover succeeds (file exists), load_config raises a validation ConfigError
    _install_fake_aidp_config(monkeypatch, load_config=_bad_load)
    with pytest.raises(_FakeConfigError):
        ae._apply_cred_env_from_config()
    assert "AIDP_CRED_OCI_NAME" not in os.environ    # never fabricated a default


import os.path as _osp

_ENTRYPOINTS = (
    "build_dag_from_workflow.py",
    "check_data_availability_for_workflow.py",
    "clone_workflow.py",
    # The canonical migration runner calls build_dag_from_workflow.fetch_job_definition()
    # DIRECTLY (bypassing that module's main() export), so it must export --config
    # itself before the first signer call. Missed in the first #129 pass; caught in
    # review (o-vazunnavar) — the fix-the-class 4th site.
    "job_migrate_from_workflow.py",
)


def _read_script(name):
    with open(_osp.join(_SCRIPTS, name), encoding="utf-8") as f:
        return f.read()


@pytest.mark.parametrize("entrypoint", _ENTRYPOINTS)
def test_entrypoint_exports_explicit_config_for_cred_store(entrypoint):
    """Every standalone entrypoint that runs on the cluster must export an
    explicit --config so the Credential-Store signer honors it.

    get_oci_signer -> _apply_cred_env_from_config re-discovers the YAML and only
    sees $AIDP_CONFIG_PATH / default locations, NOT --config. If an entrypoint
    loads a custom YAML via --config but does not export it, a renamed credential
    silently falls back to default names -> 401 on the cluster (a reviewer #129 P1;
    the AGENTS.md "config routing covered the env var but missed --config" class).
    """
    src = _read_script(entrypoint)
    assert 'os.environ["AIDP_CONFIG_PATH"] = args.config' in src, (
        f"{entrypoint} must export --config so the cred-store signer honors it")
    # ...and it must be wired in the config-loading path (after load_config),
    # so it runs before the first get_oci_signer call.
    load_at = src.find("_load_cfg(cli_config_path=args.config)")
    export_at = src.find('os.environ["AIDP_CONFIG_PATH"] = args.config')
    assert load_at != -1 and export_at > load_at, (
        f"{entrypoint} must export --config right after loading it")


def _mk_cfg(name, user=None, tenancy=None):
    from types import SimpleNamespace
    return SimpleNamespace(aidp=SimpleNamespace(oci_credentials=SimpleNamespace(
        name=name,
        keys=SimpleNamespace(user=user, fingerprint=None, tenancy=tenancy,
                             region=None, private_key_content=None))))


def test_preset_env_respected_when_no_config(monkeypatch):
    """New contract: a pre-existing (bootstrap-set) env is respected ONLY when NO
    config is discoverable — and never clobbered."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("AIDP_CRED_OCI_NAME", "bootstrap_name")

    def _no_config(*a, **k):
        raise _FakeConfigError("No customer configuration file found")

    def _must_not_load(*a, **k):
        raise AssertionError("must not load config when none is discoverable")

    _install_fake_aidp_config(monkeypatch, load_config=_must_not_load, discover=_no_config)
    ae._apply_cred_env_from_config()
    assert os.environ["AIDP_CRED_OCI_NAME"] == "bootstrap_name"


def test_config_authoritative_reresolves_over_preset(monkeypatch):
    """#129 P1b: a discoverable config is authoritative and is re-resolved even
    when AIDP_CRED_OCI_NAME is already set — no longer sticky to a prior value."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("AIDP_CRED_OCI_NAME", "stale_name")
    _install_fake_aidp_config(monkeypatch,
                              load_config=lambda *a, **k: _mk_cfg("fresh_name", tenancy="T"))
    ae._apply_cred_env_from_config()
    assert os.environ["AIDP_CRED_OCI_NAME"] == "fresh_name"        # config wins, not sticky
    assert os.environ["AIDP_CRED_OCI_KEY_TENANCY"] == "T"           # keys applied fresh


def test_partial_env_gets_key_mapping_from_config(monkeypatch):
    """#129 P1b: name pre-set but key map absent must NOT skip the YAML's key
    mapping (else the default key names are requested)."""
    _clear_cred_env(monkeypatch)
    monkeypatch.setenv("AIDP_CRED_OCI_NAME", "some_name")           # name only, no keys
    _install_fake_aidp_config(monkeypatch,
                              load_config=lambda *a, **k: _mk_cfg("some_name", user="custom_user_key"))
    ae._apply_cred_env_from_config()
    assert os.environ["AIDP_CRED_OCI_KEY_USER"] == "custom_user_key"


def test_sequential_yaml_selection_reresolves(monkeypatch):
    """#129 P1b: selecting YAML A then YAML B in one process must land B's creds,
    not stay on A."""
    _clear_cred_env(monkeypatch)
    _install_fake_aidp_config(monkeypatch, load_config=lambda *a, **k: _mk_cfg("A", user="userA"))
    ae._apply_cred_env_from_config()
    assert os.environ["AIDP_CRED_OCI_NAME"] == "A" and os.environ["AIDP_CRED_OCI_KEY_USER"] == "userA"
    _install_fake_aidp_config(monkeypatch, load_config=lambda *a, **k: _mk_cfg("B", user="userB"))
    ae._apply_cred_env_from_config()
    assert os.environ["AIDP_CRED_OCI_NAME"] == "B" and os.environ["AIDP_CRED_OCI_KEY_USER"] == "userB"
