"""Unit tests for the aidp-fusion-seed skill's reusable precondition checker.

Asserts the emitted JSON contract over fixtures: valid/invalid bundle, profile
present/absent, cluster active/stopped/unprobed (injected probe — no live OCI),
multi-tenant env selection, and placeholder-coords -> ``config_placeholders``.

The skill lives outside the installed package, so import via sys.path
(mirrors ``test_aidp_rest_skill_client.py``). The plugin package itself must be
importable — these tests run under the repo's ``.venv`` like the rest.
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SKILL_DIR = _REPO_ROOT / "skills" / "aidp-fusion-seed"
sys.path.insert(0, str(_SKILL_DIR))

import preconditions as pre  # noqa: E402

# ---------------------------------------------------------------------------
# fixture writers
# ---------------------------------------------------------------------------


def _write(p: Path, text: str) -> Path:
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return p


def _config(tmp: Path, *, placeholder: bool = False, second_env_clean: bool = False) -> Path:
    aidp = "ocid1.datalake.oc1.iad.AIDP-ID-PLACEHOLDER" if placeholder else "ocid1.datalake.oc1.iad.real"
    ck = "CLUSTER-UUID-PLACEHOLDER" if placeholder else "real-cluster-uuid"
    prod = ""
    if second_env_clean:
        prod = """
          prod:
            workspaceKey: ws-prod
            region: us-ashburn-1
            aiDataPlatformId: ocid1.datalake.oc1.iad.prod
            clusterKey: prod-cluster-uuid
            clusterName: prod_cluster
        """
    return _write(tmp / "aidp.config.yaml", f"""
        apiVersion: aidp-fusion-autopilot/v1
        project: test-lake
        defaults:
          region: us-ashburn-1
        environments:
          dev:
            workspaceKey: ws-dev
            region: us-ashburn-1
            aiDataPlatformId: {aidp}
            clusterKey: {ck}
            clusterName: dev_cluster
        {prod}
    """)


def _bundle_with_profile(tmp: Path, profile_name: str | None) -> Path:
    cp = f"""
        contentPack:
          name: fusion-finance-starter
          profile: {profile_name}
    """ if profile_name else ""
    return _write(tmp / "bundle.yaml", f"""
        apiVersion: aidp-fusion-autopilot/v1
        version: "0.2.0"
        project: test-lake
        fusion:
          serviceUrl: https://example.fa.oraclecloud.com
          username: u
          password: p
          externalStorage: stor
        aidp:
          catalog: c
          bronzeSchema: bronze
          silverSchema: silver
          goldSchema: gold
        datasets:
          - id: erp_suppliers
            mode: full
        {cp}
    """)


# ---------------------------------------------------------------------------
# check_config_placeholders
# ---------------------------------------------------------------------------


def test_config_placeholders_detected(tmp_path):
    cfg = _config(tmp_path, placeholder=True)
    bad, _ = pre.check_config_placeholders(cfg, "dev")
    assert set(bad) == {"aiDataPlatformId", "clusterKey"}


def test_config_clean_no_placeholders(tmp_path):
    cfg = _config(tmp_path, placeholder=False)
    bad, detail = pre.check_config_placeholders(cfg, "dev")
    assert bad == []
    assert "resolved" in detail


def test_config_multitenant_selects_named_env(tmp_path):
    """A clean prod env is unaffected by a placeholder dev env."""
    cfg = _config(tmp_path, placeholder=True, second_env_clean=True)
    bad_dev, _ = pre.check_config_placeholders(cfg, "dev")
    bad_prod, _ = pre.check_config_placeholders(cfg, "prod")
    assert bad_dev  # dev is placeholder
    assert bad_prod == []  # prod is clean


# ---------------------------------------------------------------------------
# resolve_profile
# ---------------------------------------------------------------------------


def test_profile_absent_when_no_contentpack(tmp_path):
    b = _bundle_with_profile(tmp_path, None)
    tenant, _path, present, detail = pre.resolve_profile(b)
    assert tenant is None
    assert present is False
    assert "no contentPack.profile" in detail


def test_profile_absent_when_file_missing(tmp_path):
    b = _bundle_with_profile(tmp_path, "acme-prod")
    tenant, path, present, _detail = pre.resolve_profile(b)
    assert tenant == "acme-prod"
    assert present is False
    # OS-agnostic: resolve_profile returns a native path (backslashes on
    # Windows), so normalize to forward slashes before the suffix check.
    assert Path(path).as_posix().endswith("profiles/acme-prod.yaml")


def test_profile_present_when_file_exists(tmp_path):
    b = _bundle_with_profile(tmp_path, "acme-prod")
    (tmp_path / "profiles").mkdir()
    _write(tmp_path / "profiles" / "acme-prod.yaml", "schemaVersion: 1\ntenant: acme-prod\npinnedAt: 2026-06-14T00:00:00Z\n")
    tenant, _path, present, _detail = pre.resolve_profile(b)
    assert tenant == "acme-prod"
    assert present is True


def test_profile_resolution_survives_unset_env_vars(tmp_path):
    """contentPack.profile is static — must resolve even with ${ENV} unset."""
    b = _bundle_with_profile(tmp_path, "acme-prod")  # bundle has ${...}-free body here
    # The real dev bundle uses ${FUSION_BICC_BASE_URL}; emulate one to prove
    # resolve_profile reads RAW yaml (no interpolation).
    _write(tmp_path / "bundle.yaml", b.read_text(encoding="utf-8").replace(
        "https://example.fa.oraclecloud.com", "${FUSION_BICC_BASE_URL}"
    ))
    tenant, _path, _present, _detail = pre.resolve_profile(tmp_path / "bundle.yaml")
    assert tenant == "acme-prod"  # did not blow up on the unset env var


# ---------------------------------------------------------------------------
# check_preconditions aggregation (injected cluster probe — no live OCI)
# ---------------------------------------------------------------------------


def _probe(state):
    return lambda _cfg, _env: (state, f"injected {state}")


def test_aggregate_cluster_active_not_missing(tmp_path):
    cfg = _config(tmp_path, placeholder=False)
    b = _bundle_with_profile(tmp_path, "acme-prod")
    (tmp_path / "profiles").mkdir()
    _write(tmp_path / "profiles" / "acme-prod.yaml", "schemaVersion: 1\ntenant: x\npinnedAt: 2026-06-14T00:00:00Z\n")
    r = pre.check_preconditions(
        bundle_path=b, config_path=cfg, env_name="dev",
        cluster_probe=_probe("ACTIVE"),
    )
    assert "cluster" not in r.missing
    assert r.cluster_state == "ACTIVE"


def test_aggregate_cluster_stopped_is_missing(tmp_path):
    cfg = _config(tmp_path, placeholder=False)
    b = _bundle_with_profile(tmp_path, "acme-prod")
    r = pre.check_preconditions(
        bundle_path=b, config_path=cfg, env_name="dev",
        cluster_probe=_probe("STOPPED"),
    )
    assert "cluster" in r.missing
    assert r.cluster_state == "STOPPED"


def test_aggregate_cluster_unprobed_is_missing(tmp_path):
    """Could-not-probe must classify as missing (fail closed), not pass."""
    cfg = _config(tmp_path, placeholder=False)
    b = _bundle_with_profile(tmp_path, "acme-prod")
    r = pre.check_preconditions(
        bundle_path=b, config_path=cfg, env_name="dev",
        cluster_probe=_probe("unprobed"),
    )
    assert "cluster" in r.missing


def test_aggregate_inline_skips_cluster_probe(tmp_path):
    cfg = _config(tmp_path, placeholder=False)
    b = _bundle_with_profile(tmp_path, "acme-prod")
    (tmp_path / "profiles").mkdir()
    _write(tmp_path / "profiles" / "acme-prod.yaml", "schemaVersion: 1\ntenant: x\npinnedAt: 2026-06-14T00:00:00Z\n")
    called = {"n": 0}

    def probe(_c, _e):
        called["n"] += 1
        return ("ACTIVE", "")

    r = pre.check_preconditions(
        bundle_path=b, config_path=cfg, env_name="dev",
        dispatch_mode="inline", cluster_probe=probe,
    )
    assert called["n"] == 0  # inline never probes
    assert "cluster" not in r.missing


def test_aggregate_placeholder_config_is_missing_config(tmp_path):
    cfg = _config(tmp_path, placeholder=True)
    b = _bundle_with_profile(tmp_path, "acme-prod")
    r = pre.check_preconditions(
        bundle_path=b, config_path=cfg, env_name="dev",
        cluster_probe=_probe("ACTIVE"),
    )
    assert "config" in r.missing
    assert set(r.config_placeholders) == {"aiDataPlatformId", "clusterKey"}


def test_aggregate_invalid_bundle_is_missing_bundle(tmp_path):
    cfg = _config(tmp_path, placeholder=False)
    missing_bundle = tmp_path / "nope.yaml"
    r = pre.check_preconditions(
        bundle_path=missing_bundle, config_path=cfg, env_name="dev",
        cluster_probe=_probe("ACTIVE"),
    )
    assert "bundle" in r.missing
    assert r.validate_ok is False


def test_ok_true_only_when_nothing_missing(tmp_path):
    cfg = _config(tmp_path, placeholder=False)
    b = _bundle_with_profile(tmp_path, "acme-prod")
    (tmp_path / "profiles").mkdir()
    _write(tmp_path / "profiles" / "acme-prod.yaml", "schemaVersion: 1\ntenant: x\npinnedAt: 2026-06-14T00:00:00Z\n")
    r = pre.check_preconditions(
        bundle_path=b, config_path=cfg, env_name="dev",
        cluster_probe=_probe("ACTIVE"),
    )
    assert r.ok is (not r.missing)
