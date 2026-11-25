"""cluster_lifecycle._get_signer delegates to the shared dual-path signer.

Before 2026-10-12, cluster_lifecycle built its own API-key signer from
~/.oci/config only — the ONE module without the Credential Store path, which
forced a materialized key file onto AIDP clusters solely for lifecycle REST
calls (cluster state, library install, idle timeout). The fix delegates to
oci_auth.get_oci_signer, whose precedence (Credential Store on-cluster →
$OCI_CONFIG_FILE → ~/.oci/config) is pinned by
tests/test_get_oci_signer_dual_path.py — these tests pin only the delegation,
the cache, and the callers' reset contract (`_cl._SIGNER = None`).

Hermetic: get_oci_signer is monkeypatched; oci.config.from_file is rigged to
fail so any regression back to the module-local file-only path fails loudly.
"""
import os
import sys

import pytest

_SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

import oci_auth as oa  # noqa: E402
import cluster_lifecycle as cl  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_signer_cache(monkeypatch):
    monkeypatch.setattr(cl, "_SIGNER", None)
    # A regression back to the old module-local from_file path must fail loudly.
    monkeypatch.setattr(
        cl.oci.config, "from_file",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("cluster_lifecycle must not read oci config files directly")),
    )
    yield


def test_delegates_to_shared_get_oci_signer(monkeypatch):
    sentinel = object()
    seen = {}

    def fake(profile):
        seen["profile"] = profile
        return None, sentinel

    monkeypatch.setattr(oa, "get_oci_signer", fake)
    monkeypatch.setattr(cl, "OCI_PROFILE", "UNIT_PROFILE")
    assert cl._get_signer() is sentinel
    assert seen["profile"] == "UNIT_PROFILE"


def test_signer_is_cached_until_reset(monkeypatch):
    calls = []

    def fake(profile):
        calls.append(profile)
        return None, object()

    monkeypatch.setattr(oa, "get_oci_signer", fake)
    first = cl._get_signer()
    assert cl._get_signer() is first
    assert len(calls) == 1
    # The runners' documented contract: reset after a profile change rebuilds.
    cl._SIGNER = None
    second = cl._get_signer()
    assert second is not first
    assert len(calls) == 2


def test_shared_signer_failure_propagates(monkeypatch):
    def fake(profile):
        raise RuntimeError("no auth available")

    monkeypatch.setattr(oa, "get_oci_signer", fake)
    with pytest.raises(RuntimeError, match="no auth available"):
        cl._get_signer()
    assert cl._SIGNER is None  # failure must not poison the cache
