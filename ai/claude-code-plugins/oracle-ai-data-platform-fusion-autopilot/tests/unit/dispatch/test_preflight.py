"""P1.5ε §Step 5 — dispatch/preflight.py tests.

Covers both phases:

- Phase A (local): bundle, dispatch coords, OCI profile + session-token
  validation. No HTTP, no AidpRestClient construction.
- Phase B (remote): control plane reachability, cluster state +
  auto-start. Mocked AidpRestClient.

Critical invariant — Phase A FAIL never lets Phase B run. Locked by the
two-phase function split: ``run_local_preflight`` returns SKIP entries for
subsequent checks once anything fails; ``run_remote_preflight`` is invoked
by the dispatch entry point only when local preflight is all-PASS.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import oci
import pytest

from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight import (
    PreflightResult,
    any_failed,
    render,
    run_local_preflight,
    run_remote_preflight,
)
from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
    AidpRestError,
    ClusterSummary,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
    AidpConfig,
    AuthSpec,
    EnvSpec,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_GOOD_BUNDLE = """\
apiVersion: aidp-fusion-autopilot/v1
project: test-preflight
fusion:
  serviceUrl: https://fusion.example.com
  username: user
  password: not-a-secret
  externalStorage: storage-1
datasets:
  - id: erp_suppliers
"""


@pytest.fixture
def bundle_path(tmp_path: Path) -> Path:
    p = tmp_path / "bundle.yaml"
    p.write_text(_GOOD_BUNDLE)
    return p


def _env(**overrides) -> EnvSpec:
    base = dict(
        workspaceKey="wk-123",
        aiDataPlatformId="ocid1.datalake.oc1.iad.test",
        clusterKey="cluster-uuid-1",
        clusterName="test-cluster",
        ociProfile="AIDP_SESSION",
    )
    base.update(overrides)
    return EnvSpec.model_validate(base)


def _config() -> AidpConfig:
    return AidpConfig.model_validate(
        {
            "apiVersion": "aidp-fusion-autopilot/v1",
            "project": "test",
            "environments": {"dev": _env().model_dump(by_alias=True)},
        }
    )


# ---------------------------------------------------------------------------
# Phase A — local preflight
# ---------------------------------------------------------------------------


class TestPhaseALocalPreflight:
    def test_all_pass_when_inputs_clean(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.oci.config.from_file",
            return_value={"tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/k"},
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        assert [r.status for r in results] == ["PASS", "PASS", "PASS"]
        assert not any_failed(results)

    def test_bundle_fail_skips_remaining(self, tmp_path: Path) -> None:
        bad_bundle = tmp_path / "missing.yaml"
        results = run_local_preflight(
            bundle_path=bad_bundle,
            config=_config(),
            env_name="dev",
            env=_env(),
        )
        assert results[0].status == "FAIL"
        assert results[1].status == "SKIP"
        assert results[2].status == "SKIP"
        assert any_failed(results)
        assert "validate" in (results[0].remediation or "")

    def test_missing_dispatch_coords_fails_with_field_names(
        self, bundle_path: Path
    ) -> None:
        env = _env(aiDataPlatformId=None, clusterKey=None)
        results = run_local_preflight(
            bundle_path=bundle_path,
            config=_config(),
            env_name="dev",
            env=env,
        )
        coords = results[1]
        assert coords.status == "FAIL"
        assert "aiDataPlatformId" in coords.detail
        assert "clusterKey" in coords.detail
        # OCI profile check must SKIP — we didn't get to construct a client.
        assert results[2].status == "SKIP"

    def test_vault_auth_rejected_with_profile_remediation(
        self, bundle_path: Path
    ) -> None:
        env = _env()
        env = env.model_copy(update={"auth": AuthSpec(mode="vault")})
        results = run_local_preflight(
            bundle_path=bundle_path,
            config=_config(),
            env_name="dev",
            env=env,
        )
        coords = results[1]
        assert coords.status == "FAIL"
        assert "vault" in coords.detail
        assert "auth.mode: profile" in (coords.remediation or "")
        assert "ociProfile" in (coords.remediation or "")

    def test_oci_profile_not_found_fails_cleanly(
        self, bundle_path: Path
    ) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.oci.config.from_file",
            side_effect=oci.exceptions.ProfileNotFound("no such profile"),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        oci_result = results[2]
        assert oci_result.status == "FAIL"
        assert "AIDP_SESSION" in (oci_result.remediation or "")

    def test_api_key_profile_skips_session_validation(
        self, bundle_path: Path
    ) -> None:
        # An API-key profile (no security_token_file) — no subprocess invocation.
        cfg = {"tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/k"}
        with (
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.subprocess.run",
                side_effect=AssertionError("subprocess.run must not be called for API-key profiles"),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        assert results[2].status == "PASS"

    def test_session_token_valid_passes(self, bundle_path: Path) -> None:
        cfg = {
            "security_token_file": "/tmp/token",
            "key_file": "/tmp/key.pem",
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="Session is valid", stderr=""
                ),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        assert results[2].status == "PASS"

    def test_session_token_expired_fails_with_refresh_hint(
        self, bundle_path: Path
    ) -> None:
        cfg = {
            "security_token_file": "/tmp/token",
            "key_file": "/tmp/key.pem",
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=1,
                    stdout="",
                    stderr="session is not valid",
                ),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        oci_result = results[2]
        assert oci_result.status == "FAIL"
        assert "oci session refresh" in (oci_result.remediation or "")
        assert "AIDP_SESSION" in (oci_result.remediation or "")

    def test_oci_cli_missing_session_profile_fails(
        self, bundle_path: Path
    ) -> None:
        cfg = {
            "security_token_file": "/tmp/token",
            "key_file": "/tmp/key.pem",
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.preflight.subprocess.run",
                side_effect=FileNotFoundError("oci"),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        # Plan §Step 5 — session-token profile + missing oci CLI = FAIL,
        # NOT a soft SKIP. SKIP here would mask an expired session as a
        # Phase-B 401.
        assert results[2].status == "FAIL"
        assert "CLI" in results[2].detail or "CLI" in (results[2].remediation or "")


# ---------------------------------------------------------------------------
# Phase B — remote preflight
# ---------------------------------------------------------------------------


def _client_with(
    *,
    list_clusters=None,
    start_cluster=None,
    wait_active=None,
    credential_exists=True,  # P1.5ε-fix1: default to PASS so existing
                             # tests don't have to opt into check 6.
) -> MagicMock:
    client = MagicMock(spec=[
        "list_clusters", "start_cluster", "wait_cluster_active", "get_cluster",
        "check_credential_exists",
    ])
    if list_clusters is not None:
        if isinstance(list_clusters, BaseException):
            client.list_clusters.side_effect = list_clusters
        else:
            client.list_clusters.return_value = list_clusters
    if start_cluster is not None:
        if isinstance(start_cluster, BaseException):
            client.start_cluster.side_effect = start_cluster
        else:
            client.start_cluster.return_value = start_cluster
    if wait_active is not None:
        if isinstance(wait_active, BaseException):
            client.wait_cluster_active.side_effect = wait_active
    if isinstance(credential_exists, BaseException):
        client.check_credential_exists.side_effect = credential_exists
    else:
        client.check_credential_exists.return_value = credential_exists
    return client


class TestPhaseBRemotePreflight:
    def test_control_plane_unreachable_fails_and_skips_cluster(self) -> None:
        client = _client_with(
            list_clusters=AidpRestError("HTTP 401 body=bad signature"),
        )
        results = run_remote_preflight(client=client, env=_env())
        # control plane FAIL → cluster SKIP + credential SKIP (P1.5ε-fix1)
        assert results[0].status == "FAIL"
        assert results[1].status == "SKIP"
        assert results[2].status == "SKIP"
        assert "region" in (results[0].remediation or "")

    def test_cluster_active_passes(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="ACTIVE")
            ],
        )
        results = run_remote_preflight(client=client, env=_env())
        # control-plane + cluster + BICC credential (P1.5ε-fix1) all PASS
        assert [r.status for r in results] == ["PASS", "PASS", "PASS"]
        client.start_cluster.assert_not_called()
        client.wait_cluster_active.assert_not_called()

    # Note (P1.5ε-fix1 review-driven correction): cluster-state is
    # check 6 (index 2), NOT check 5 (index 1) — credential preflight
    # moved ahead to avoid paying ~5min cluster cold-start before a
    # missing-credential fast-fail. See run_remote_preflight docstring.

    def test_cluster_not_found_fails(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="other-uuid", display_name="other", state="ACTIVE")
            ],
        )
        results = run_remote_preflight(client=client, env=_env())
        assert results[2].status == "FAIL"
        assert "cluster-uuid-1" in results[2].detail

    def test_stopped_auto_start_invokes_start_and_wait(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="STOPPED")
            ],
        )
        client.start_cluster.return_value = {}
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        client.start_cluster.assert_called_once_with("cluster-uuid-1")
        client.wait_cluster_active.assert_called_once()
        assert results[2].status == "PASS"

    def test_stopped_no_auto_start_fails(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="STOPPED")
            ],
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=False
        )
        assert results[2].status == "FAIL"
        client.start_cluster.assert_not_called()

    def test_auto_start_failure_surfaces(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="STOPPED")
            ],
        )
        client.start_cluster.return_value = {}
        client.wait_cluster_active.side_effect = AidpRestError(
            "cluster transitioned to FAILED while waiting"
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        assert results[2].status == "FAIL"
        assert "FAILED" in results[2].detail

    def test_cluster_failed_state_no_auto_recovery(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="FAILED")
            ],
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        assert results[2].status == "FAIL"
        client.start_cluster.assert_not_called()


class TestRender:
    def test_renders_all_results(self) -> None:
        out = render(
            [
                PreflightResult(name="x", status="PASS", detail="ok"),
                PreflightResult(
                    name="y",
                    status="FAIL",
                    detail="bad",
                    remediation="fix it",
                ),
            ]
        )
        assert "PASS x: ok" in out
        assert "FAIL y: bad" in out
        assert "fix it" in out


# ---------------------------------------------------------------------------
# P1.5ε-fix1 — BICC credential preflight (Phase B check 6)
# ---------------------------------------------------------------------------


def _active_cluster_list() -> list:
    """Reusable: a cluster list with the test env's cluster_key ACTIVE so
    check 5 PASSes and we can isolate check 6's behavior."""
    return [
        ClusterSummary(key="cluster-uuid-1", display_name="dev", state="ACTIVE")
    ]


class TestBiccCredentialCheck:
    """Check 5 in the new ordering — credential runs BEFORE cluster
    state (which is now check 6 at index 2) so a missing credential
    fast-fails without paying ~5min cluster cold-start. Reviewer-driven
    correction; see ``run_remote_preflight`` docstring."""

    def test_credential_present_passes(self) -> None:
        """Credential exists → check 5 (index 1) PASS with the secret
        name in the detail line."""
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=True,
        )
        results = run_remote_preflight(client=client, env=_env())
        assert len(results) == 3
        cred_result = results[1]
        assert cred_result.name == "BICC credential"
        assert cred_result.status == "PASS"
        assert "fusion_bicc_password" in cred_result.detail

    def test_credential_missing_fails_with_remediation(self) -> None:
        """Missing credential → check 5 FAIL with the secret name in
        detail + a copy-pasteable AIDP UI remediation."""
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=False,
        )
        results = run_remote_preflight(client=client, env=_env())
        cred_result = results[1]
        assert cred_result.status == "FAIL"
        assert "fusion_bicc_password" in cred_result.detail
        assert cred_result.remediation is not None
        assert "AIDP UI" in cred_result.remediation
        assert "biccSecretName" in cred_result.remediation

    def test_credential_check_rest_error_propagates_as_fail(self) -> None:
        """Transport / IAM error from check_credential_exists → check 5
        FAIL classifying as transport (so a flaky AIDP plane doesn't
        mask a real missing-credential)."""
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=AidpRestError("HTTP 500 body=ServerError"),
        )
        results = run_remote_preflight(client=client, env=_env())
        cred_result = results[1]
        assert cred_result.status == "FAIL"
        assert "transport" in cred_result.detail or "IAM" in cred_result.detail
        assert "use aiDataPlatformCredentials" in (
            cred_result.remediation or ""
        )

    def test_credential_check_runs_for_literal_password(self) -> None:
        """LOCKS the always-check decision (plan §Technical Decisions row 5):
        even when bundle.fusion.password is a literal string (not an
        ${env:X} ref), check_credential_exists IS still called.

        The notebook's creds-cell unconditionally fetches the AIDP secret
        before importing the orchestrator (notebook_builder._build_creds_cell),
        so a missing entry crashes the dispatch regardless of how the
        password is structured. SKIP-on-literal would reopen exactly the
        fast-fail gap fix1 closes."""
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=True,
        )
        # Phase A is bypassed; we're exercising Phase B directly. The
        # bundle.fusion.password shape is not consulted by the preflight
        # function at all — the assertion is that the credential check
        # fires unconditionally based on env.bicc_secret_name.
        run_remote_preflight(client=client, env=_env())
        client.check_credential_exists.assert_called_once()

    def test_credential_check_runs_for_vault_ref_password(self) -> None:
        """Same as above but with a ${vault:OCID} placeholder password —
        the credential check still fires. Locks the always-check
        invariant against the second SKIP-style regression path."""
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=True,
        )
        run_remote_preflight(client=client, env=_env())
        client.check_credential_exists.assert_called_once()

    def test_credential_check_uses_bicc_secret_name_from_env(self) -> None:
        """Wiring lock: the preflight call MUST use env.bicc_secret_name
        (the same field notebook_builder threads into the cluster-side
        aidputils.secrets.get call). A different value would mean the
        preflight checks one secret while the notebook fetches a
        different one — divergence reviewer flagged in the plan review."""
        env = _env(biccSecretName="custom_secret_name")
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=True,
        )
        run_remote_preflight(client=client, env=env)
        client.check_credential_exists.assert_called_once_with(
            "custom_secret_name"
        )

    # ------------------------------------------------------------------
    # Reviewer round-3 regression locks
    # ------------------------------------------------------------------

    def test_missing_credential_skips_cluster_check_and_does_not_start_stopped_cluster(
        self,
    ) -> None:
        """**Blocking-fix regression lock** (reviewer round 3):
        when the credential is missing AND the cluster is STOPPED,
        ``_check_cluster_state`` MUST NOT run — otherwise the dispatcher
        spends ~5 min auto-starting a cluster that the dispatch can
        never use (because the cluster-side creds-cell will fail).
        Negates fix1's ~300ms fast-fail promise.

        Asserts: credential check FAILs at results[1]; cluster check
        SKIPs at results[2] with the BICC-credential reason; neither
        ``start_cluster`` nor ``wait_cluster_active`` is invoked."""
        client = _client_with(
            list_clusters=[
                ClusterSummary(
                    key="cluster-uuid-1", display_name="dev", state="STOPPED"
                )
            ],
            credential_exists=False,
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        assert results[1].status == "FAIL"
        assert results[1].name == "BICC credential"
        # Cluster SKIPpped — fast-fail invariant.
        assert results[2].status == "SKIP"
        assert results[2].name == "cluster state"
        assert "BICC credential" in results[2].detail
        # Critical: compute NEVER started.
        client.start_cluster.assert_not_called()
        client.wait_cluster_active.assert_not_called()

    def test_credential_remediation_uses_custom_secret_key(self) -> None:
        """**Should-fix regression lock** (reviewer round 3):
        remediation hint must reference ``env.bicc_secret_key``, NOT a
        hardcoded ``'password'``. With ``biccSecretKey: custom_key``,
        a hardcoded-'password' hint would tell the operator to register
        the entry with key `password`; the next preflight would PASS
        (entry exists by display name) but the cluster-side notebook
        would still fail because it asks for key `custom_key`."""
        env = _env(
            biccSecretName="myapp_creds",
            biccSecretKey="custom_key",
        )
        client = _client_with(
            list_clusters=_active_cluster_list(),
            credential_exists=False,
        )
        results = run_remote_preflight(client=client, env=env)
        cred_result = results[1]
        assert cred_result.status == "FAIL"
        assert cred_result.remediation is not None
        assert "'custom_key'" in cred_result.remediation, (
            f"remediation must name the custom key the operator configured; "
            f"got: {cred_result.remediation!r}"
        )
        # Defensive: ensure we didn't accidentally include the default
        # 'password' string in the remediation too (which would confuse
        # the operator about what key to register).
        assert "'password'" not in cred_result.remediation, (
            f"remediation leaked the default 'password' key when "
            f"biccSecretKey='custom_key'; got: {cred_result.remediation!r}"
        )
