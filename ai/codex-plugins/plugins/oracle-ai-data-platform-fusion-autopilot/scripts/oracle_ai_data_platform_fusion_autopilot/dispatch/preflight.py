"""Fast-fail preflight checks for laptop-CLI REST dispatch.

Split into local and remote checks so the REST client is never constructed
against a malformed config:

- :func:`run_local_preflight` — bundle.yaml schema, dispatch-coordinate
  presence, OCI profile load + session-token validation. No HTTP. Runs first
  and must return PASS for every check before the client is built.
- :func:`run_remote_preflight` — AIDP control-plane reachability, BICC
  credential-store presence, and cluster state with optional auto-start.
  Requires a constructed :class:`AidpRestClient`.
"""

from __future__ import annotations

import logging
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import oci

from ..schema.bundle import AidpConfig, EnvSpec, load_bundle
from ..schema.errors import BundleLoadError
from .rest_client import AidpRestClient, AidpRestError

logger = logging.getLogger(__name__)


PreflightStatus = Literal["PASS", "FAIL", "SKIP"]


@dataclass(frozen=True)
class PreflightResult:
    """One check's outcome. ``remediation`` is the single-line hint the
    operator sees when ``status == FAIL`` — should be copy-pasteable."""

    name: str
    status: PreflightStatus
    detail: str
    remediation: str | None = None


# ---------------------------------------------------------------------------
# Local checks (no REST client, no HTTP)
# ---------------------------------------------------------------------------


def _check_bundle_yaml(bundle_path: Path) -> PreflightResult:
    try:
        load_bundle(bundle_path)
    except BundleLoadError as e:
        return PreflightResult(
            name="bundle.yaml",
            status="FAIL",
            detail=str(e).splitlines()[0],
            remediation="run `aidp-fusion-autopilot validate` for the full schema error",
        )
    return PreflightResult(
        name="bundle.yaml",
        status="PASS",
        detail=f"loaded {bundle_path}",
    )


def _check_dispatch_coords(env: EnvSpec, env_name: str) -> PreflightResult:
    missing: list[str] = []
    if not env.ai_data_platform_id:
        missing.append("aiDataPlatformId")
    if not env.cluster_key:
        missing.append("clusterKey")
    if not env.cluster_name:
        missing.append("clusterName")
    if missing:
        return PreflightResult(
            name="aidp.config.yaml dispatch coords",
            status="FAIL",
            detail=f"missing field(s) under environments.{env_name}: {', '.join(missing)}",
            remediation=(
                f"add {', '.join(missing)} under environments.{env_name} in "
                "aidp.config.yaml; see examples/aidp.config.example.yaml"
            ),
        )
    # REST dispatch currently signs control-plane calls with an OCI profile.
    # Vault/resource-principal signing is intentionally rejected until the
    # dispatcher has a cloud-side signer implementation.
    if env.auth.mode == "vault":
        return PreflightResult(
            name="aidp.config.yaml dispatch coords",
            status="FAIL",
            detail=(
                f"environments.{env_name}.auth.mode='vault' is not supported "
                "for REST dispatch"
            ),
            remediation=(
                "set auth.mode: profile and populate ociProfile; use an OCI "
                "profile or session token for dispatch"
            ),
        )
    return PreflightResult(
        name="aidp.config.yaml dispatch coords",
        status="PASS",
        detail=f"all dispatch coords present for env={env_name!r}",
    )


def _check_oci_profile_and_session(env: EnvSpec) -> PreflightResult:
    profile_name = env.oci_profile or "DEFAULT"

    # Config-file probe; this does not prove a session token is valid.
    try:
        cfg = oci.config.from_file(profile_name=profile_name)
    except oci.exceptions.ConfigFileNotFound as e:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=str(e),
            remediation="check ~/.oci/config exists",
        )
    except oci.exceptions.ProfileNotFound as e:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=str(e),
            remediation=(
                f"add a [{profile_name}] section to ~/.oci/config, or change "
                "environments.<env>.ociProfile to a profile that exists"
            ),
        )
    except oci.exceptions.InvalidConfig as e:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=f"invalid OCI profile {profile_name!r}: {e}",
            remediation="check ~/.oci/config — required fields missing or malformed",
        )

    # Session-token validation for session-token profiles only.
    token_file = cfg.get("security_token_file")
    if not token_file:
        # API-key profile: signature is verified by the AIDP control-plane
        # probe. Nothing to validate locally.
        return PreflightResult(
            name="OCI profile",
            status="PASS",
            detail=f"API-key profile {profile_name!r} loaded",
        )

    try:
        proc = subprocess.run(
            ["oci", "session", "validate", "--profile", profile_name],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        # oci CLI not on PATH. For a session-token profile this is a hard
        # FAIL because the remote checks would otherwise misclassify an
        # expired session as "AIDP plane unreachable".
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=(
                "session-token profile but `oci` CLI not on PATH; cannot "
                "validate session token locally"
            ),
            remediation=(
                "install/configure the OCI CLI "
                "(https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm), "
                "OR switch ociProfile to an API-key profile"
            ),
        )
    except subprocess.TimeoutExpired:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=f"`oci session validate --profile {profile_name}` timed out after 5s",
            remediation=f"run `oci session validate --profile {profile_name}` interactively to investigate",
        )

    if proc.returncode != 0:
        stderr_summary = (proc.stderr or proc.stdout or "").strip().splitlines()
        detail_tail = stderr_summary[-1] if stderr_summary else "(no stderr)"
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=f"session token for profile {profile_name!r} is invalid or expired: {detail_tail}",
            remediation=f"run `oci session refresh --profile {profile_name}`",
        )

    return PreflightResult(
        name="OCI profile",
        status="PASS",
        detail=f"session-token profile {profile_name!r} valid",
    )


def run_local_preflight(
    *,
    bundle_path: Path,
    config: AidpConfig,
    env_name: str,
    env: EnvSpec,
) -> list[PreflightResult]:
    """Run all three local-phase checks in cheapest-first order.

    Returns the full list (one entry per check) so the caller can render
    every result, but short-circuits subsequent checks to ``SKIP`` once a
    FAIL is hit — there's no value in probing OCI if the bundle.yaml is
    malformed, and the operator should fix one thing at a time.
    """
    results: list[PreflightResult] = []

    bundle_result = _check_bundle_yaml(bundle_path)
    results.append(bundle_result)
    if bundle_result.status != "PASS":
        results.append(
            PreflightResult(
                name="aidp.config.yaml dispatch coords",
                status="SKIP",
                detail="skipped — bundle.yaml check failed",
            )
        )
        results.append(
            PreflightResult(
                name="OCI profile",
                status="SKIP",
                detail="skipped — bundle.yaml check failed",
            )
        )
        return results

    coords_result = _check_dispatch_coords(env, env_name)
    results.append(coords_result)
    if coords_result.status != "PASS":
        results.append(
            PreflightResult(
                name="OCI profile",
                status="SKIP",
                detail="skipped — dispatch-coord check failed",
            )
        )
        return results

    results.append(_check_oci_profile_and_session(env))
    return results


# ---------------------------------------------------------------------------
# Remote checks (require a constructed client)
# ---------------------------------------------------------------------------


def _check_aidp_control_plane(
    client: AidpRestClient,
) -> tuple[PreflightResult, list]:
    """Probe ``list_clusters`` to confirm the AIDP plane is reachable.
    Returns ``(result, clusters_or_empty)`` so later checks can reuse the list."""
    try:
        clusters = client.list_clusters()
    except AidpRestError as e:
        # First 200 chars of the underlying HTTP excerpt — enough to
        # diagnose region/IAM/wrong-workspace without flooding the terminal.
        detail = str(e).splitlines()[0][:200]
        return (
            PreflightResult(
                name="AIDP control plane",
                status="FAIL",
                detail=detail,
                remediation=(
                    "verify region + workspaceKey + aiDataPlatformId in "
                    "aidp.config.yaml, then check OCI IAM grants for the "
                    "current profile on the target workspace"
                ),
            ),
            [],
        )
    return (
        PreflightResult(
            name="AIDP control plane",
            status="PASS",
            detail=f"reachable; {len(clusters)} cluster(s) visible",
        ),
        clusters,
    )


def _check_cluster_state(
    client: AidpRestClient,
    cluster_key: str,
    clusters: list,
    *,
    auto_start: bool,
    log: Callable[[str], None],
) -> PreflightResult:
    target = next((c for c in clusters if c.key == cluster_key), None)
    if target is None:
        return PreflightResult(
            name="cluster state",
            status="FAIL",
            detail=f"clusterKey {cluster_key!r} not found in workspace",
            remediation=(
                "verify clusterKey under environments.<env> in aidp.config.yaml — "
                "the UUID must match a cluster visible to this workspace"
            ),
        )

    state = target.state
    if state == "ACTIVE":
        return PreflightResult(
            name="cluster state",
            status="PASS",
            detail=f"cluster {cluster_key!r} ACTIVE",
        )

    if state == "STOPPED" and not auto_start:
        return PreflightResult(
            name="cluster state",
            status="FAIL",
            detail=f"cluster {cluster_key!r} is STOPPED",
            remediation=(
                "start it manually via the AIDP UI, or invoke dispatch with "
                "auto-start enabled"
            ),
        )

    if state == "STOPPED" and auto_start:
        log(f"cluster {cluster_key!r} STOPPED — auto-starting (~5 min)…")
        try:
            client.start_cluster(cluster_key)
            client.wait_cluster_active(cluster_key, timeout_s=600)
        except AidpRestError as e:
            return PreflightResult(
                name="cluster state",
                status="FAIL",
                detail=f"cluster {cluster_key!r} auto-start failed: {str(e).splitlines()[0][:200]}",
                remediation="check the AIDP console for the failure reason",
            )
        return PreflightResult(
            name="cluster state",
            status="PASS",
            detail=f"cluster {cluster_key!r} auto-started to ACTIVE",
        )

    # FAILED / CREATING / UNKNOWN / etc — no auto-recovery.
    return PreflightResult(
        name="cluster state",
        status="FAIL",
        detail=f"cluster {cluster_key!r} state={state!r}, expected ACTIVE",
        remediation="check the AIDP console for the cluster's current state",
    )


def _check_bicc_credential(
    client: AidpRestClient,
    secret_name: str,
    secret_key: str,
) -> PreflightResult:
    """Confirm the AIDP credential entry exists before dispatch.

    The cluster-side notebook's credentials cell at
    ``notebook_builder._build_creds_cell`` unconditionally calls
    ``aidputils.secrets.get(name=env.bicc_secret_name, key=env.bicc_secret_key)``
    before writing the bundle or importing the orchestrator. A missing
    credential entry would otherwise surface mid-notebook after wheel build,
    upload, job submit, and cluster ramp. This check fast-fails the same
    condition before compute is started.

    Always check, regardless of ``bundle.fusion.password`` shape. The
    notebook's secret fetch is independent of how the password is referenced.

    ``secret_key`` is used ONLY for the operator-facing remediation
    hint so they can register the entry with the right key the notebook
    will look up — the credential REST endpoint can't validate individual
    keys (it lists/looks-up credential ENTRIES by displayName, not
    key contents). If `biccSecretKey` is the default ``"password"`` the
    AIDP UI default also matches, but for `biccSecretKey: custom_key`
    the remediation must say `custom_key` or the operator creates the
    entry with the wrong key and the next preflight runs PASS while
    the cluster-side notebook still fails mid-flight.
    """
    try:
        exists = client.check_credential_exists(secret_name)
    except AidpRestError as exc:
        detail = str(exc).splitlines()[0][:200]
        return PreflightResult(
            name="BICC credential",
            status="FAIL",
            detail=(
                f"credential-store check failed (transport / IAM): {detail}"
            ),
            remediation=(
                "verify IAM grants for `use aiDataPlatformCredentials` on "
                "the current OCI profile; if AIDP is degraded, retry"
            ),
        )
    if not exists:
        return PreflightResult(
            name="BICC credential",
            status="FAIL",
            detail=(
                f"AIDP credential entry {secret_name!r} not found in the "
                "data-lake credential store"
            ),
            remediation=(
                f"add a credential named {secret_name!r} with key "
                f"{secret_key!r} via the AIDP UI before running, OR "
                f"change environments.<env>.biccSecretName in "
                f"aidp.config.yaml to match an existing entry"
            ),
        )
    return PreflightResult(
        name="BICC credential",
        status="PASS",
        detail=f"credential {secret_name!r} present in AIDP store",
    )


def run_remote_preflight(
    *,
    client: AidpRestClient,
    env: EnvSpec,
    auto_start_cluster: bool = True,
    log: Callable[[str], None] = lambda msg: None,
) -> list[PreflightResult]:
    """Run checks that require an AIDP control-plane round-trip.

    Order (cheapest-first; cluster check moved to LAST because it can
    auto-start a STOPPED cluster + block in ``wait_cluster_active`` for
    up to 10 min):

      1. AIDP control plane reachable (``list_clusters`` probe) — ~300ms
      2. BICC credential entry exists in AIDP credential store — ~300ms
      3. Cluster state ACTIVE (or auto-start if STOPPED) — can take
         ~5 min; skipped when the credential check failed

    The credential check runs before the cluster check to avoid paying cluster
    startup for a dispatch that is guaranteed to fail.
    """
    results: list[PreflightResult] = []
    plane_result, clusters = _check_aidp_control_plane(client)
    results.append(plane_result)
    if plane_result.status != "PASS":
        results.append(
            PreflightResult(
                name="BICC credential",
                status="SKIP",
                detail="skipped — control-plane check failed",
            )
        )
        results.append(
            PreflightResult(
                name="cluster state",
                status="SKIP",
                detail="skipped — control-plane check failed",
            )
        )
        return results

    # Credential preflight runs before the cluster check so a missing
    # credential fast-fails without paying cluster cold-start cost.
    credential_result = _check_bicc_credential(
        client, env.bicc_secret_name, env.bicc_secret_key
    )
    results.append(credential_result)

    if credential_result.status != "PASS":
        # Dispatch is guaranteed to fail with a missing/unreachable
        # credential; don't spend compute starting the cluster.
        results.append(
            PreflightResult(
                name="cluster state",
                status="SKIP",
                detail="skipped — BICC credential check failed",
            )
        )
        return results

    assert env.cluster_key is not None  # Local coords check guarantees this.
    results.append(
        _check_cluster_state(
            client,
            env.cluster_key,
            clusters,
            auto_start=auto_start_cluster,
            log=log,
        )
    )
    return results


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------


def any_failed(results: list[PreflightResult]) -> bool:
    """Return True if any check in ``results`` is FAIL."""
    return any(r.status == "FAIL" for r in results)


def render(results: list[PreflightResult]) -> str:
    """One-line-per-check rendering for plain-text logs. The CLI renders
    via Rich; this is a fallback for non-Rich consumers."""
    lines: list[str] = []
    for r in results:
        lines.append(f"[preflight] {r.status} {r.name}: {r.detail}")
        if r.status == "FAIL" and r.remediation:
            lines.append(f"             → {r.remediation}")
    return "\n".join(lines)


__all__ = [
    "PreflightResult",
    "PreflightStatus",
    "any_failed",
    "render",
    "run_local_preflight",
    "run_remote_preflight",
]
