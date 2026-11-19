#!/usr/bin/env python3
"""Reusable precondition checker for the ``aidp-fusion-seed`` skill family.

Emits ONE structured JSON object describing whether a tenant is ready for a
``run --mode seed`` (or any orchestrator run), so the skill — and the planned
``incremental`` / ``bootstrap`` / ``status`` sibling skills — consume a single
machine-readable result instead of each re-deriving readiness.

It is a **helper, not orchestration logic** (CLAUDE.md layering rule): it calls
the plugin's own loaders + the existing ``AidpRestClient`` cluster probe. It
NEVER re-implements OCI signing, never touches ``fusion_autopilot_state``, and
never dispatches a run.

Output contract (stdout JSON):

    {
      "ok": false,
      "missing": ["profile", "config"],     # subset of bundle|config|profile|cluster
      "tenant": "acme-prod",                 # active profile name (bundle.contentPack.profile)
      "profile_path": "/abs/profiles/acme-prod.yaml",
      "profile_present": false,
      "dispatch_mode": "cluster",            # default; "inline" only when asked / notebook
      "cluster_state": "unknown",            # ACTIVE | STOPPED | <state> | unknown | unprobed
      "config_placeholders": ["aiDataPlatformId", "clusterKey"],
      "validate_ok": true,
      "details": { "<check>": "<human detail>", ... }
    }

``missing`` semantics (drives the SKILL.md auto-fix ladder):
  - "bundle"  -> ``validate`` failed / bundle.yaml unloadable -> stop + ask user to init/fix.
  - "config"  -> ``aidp.config.yaml`` coords absent or still ``*-PLACEHOLDER``
                 -> suggest ``/aidp-fusion-config`` (init-config resolves keys by name).
  - "profile" -> no ``contentPack`` block, or ``profiles/<tenant>.yaml`` absent
                 -> auto-run ``bootstrap`` (surfacing what it resolves).
  - "cluster" -> cluster not provably ACTIVE (STOPPED / other / could-not-probe)
                 -> surface the start command / ask.

Usage:
    python3 preconditions.py --bundle bundle.yaml --config aidp.config.yaml --env dev
    python3 preconditions.py ... --skip-cluster-probe   # static checks only (CI / offline)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

# The skill runs from a plugin checkout where the package is not pip-installed;
# add scripts/ to sys.path so ``oracle_ai_data_platform_fusion_autopilot.*`` imports
# (mirrors skills/aidp-rest/client.py).
_PLUGIN_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if _PLUGIN_SCRIPTS.is_dir() and str(_PLUGIN_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_SCRIPTS))

# Sentinel substring marking an un-filled coordinate in aidp.config.yaml.
_PLACEHOLDER = "PLACEHOLDER"


@dataclass
class Preconditions:
    ok: bool = False
    missing: list[str] = field(default_factory=list)
    tenant: str | None = None
    profile_path: str | None = None
    profile_present: bool = False
    dispatch_mode: str = "cluster"
    cluster_state: str = "unknown"
    config_placeholders: list[str] = field(default_factory=list)
    validate_ok: bool = False
    details: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Individual checks — each pure-ish and independently testable.
# ---------------------------------------------------------------------------


def check_validate(bundle_path: Path, config_path: Path, env_name: str) -> tuple[bool, str]:
    """Run the real ``validate`` command, swallowing its Rich output.

    Returns ``(ok, detail)``. Reuses ``commands.validate.validate`` so the
    skill never drifts from the CLI's own schema + ref-integrity rules.
    """
    try:
        from io import StringIO

        from oracle_ai_data_platform_fusion_autopilot.commands.validate import (
            validate as validate_impl,
        )
        from rich.console import Console

        buf = StringIO()
        quiet = Console(file=buf, force_terminal=False, no_color=True)
        rc = validate_impl(
            bundle_path=bundle_path,
            config_path=config_path,
            env_name=env_name,
            console=quiet,
        )
        if rc == 0:
            return True, "bundle.yaml + aidp.config.yaml validate clean"
        # First non-empty output line is the most useful summary.
        first = next((ln for ln in buf.getvalue().splitlines() if ln.strip()), "")
        return False, f"validate exited {rc}: {first.strip()[:200]}"
    except Exception as exc:
        return False, f"validate raised {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"


def check_config_placeholders(config_path: Path, env_name: str) -> tuple[list[str], str]:
    """Return the list of dispatch coords that are missing or still placeholder.

    Distinguishes the Step-4a config-coords case (resolvable by
    ``/aidp-fusion-config`` / ``init-config``) from missing ``fusion:``
    connectivity (which only a human can supply).
    """
    try:
        from oracle_ai_data_platform_fusion_autopilot.commands._config_helpers import (
            env_or_error,
            load_aidp_config,
        )

        config = load_aidp_config(config_path)
        env = env_or_error(config, env_name)
    except Exception as exc:
        return [], f"could not load aidp.config.yaml env={env_name!r}: {str(exc).splitlines()[0][:200]}"

    coords = {
        "workspaceKey": env.workspace_key,
        "aiDataPlatformId": env.ai_data_platform_id,
        "clusterKey": env.cluster_key,
    }
    bad: list[str] = []
    for name, value in coords.items():
        if not value or _PLACEHOLDER in str(value):
            bad.append(name)
    if bad:
        return bad, f"placeholder/missing dispatch coords for env={env_name!r}: {', '.join(bad)}"
    return [], f"dispatch coords resolved for env={env_name!r}"


def resolve_profile(bundle_path: Path) -> tuple[str | None, str | None, bool, str]:
    """Resolve the active tenant profile.

    Returns ``(tenant, profile_path, present, detail)``. ``tenant`` /
    ``profile_path`` are None when the bundle declares no ``contentPack`` block.

    Reads ``contentPack.profile`` from the RAW YAML rather than via
    ``load_bundle`` — that field is static and must not depend on
    ``${ENV}`` interpolation succeeding (a laptop without ``FUSION_*`` env
    vars set should still be able to learn which profile the bundle names).
    """
    try:
        import yaml
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            resolve_profile_path,
        )

        if not bundle_path.exists():
            return None, None, False, f"bundle not found: {bundle_path}"
        raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, None, False, f"could not read bundle: {str(exc).splitlines()[0][:200]}"

    cp = raw.get("contentPack") if isinstance(raw, dict) else None
    tenant = cp.get("profile") if isinstance(cp, dict) else None
    if not tenant:
        return (
            None,
            None,
            False,
            "bundle.yaml has no contentPack.profile — run bootstrap to resolve a tenant profile",
        )
    try:
        path = resolve_profile_path(bundle_path, tenant)
    except Exception as exc:
        return tenant, None, False, f"profile path unresolvable: {str(exc).splitlines()[0][:200]}"
    present = path.exists()
    detail = (
        f"profile {tenant!r} present at {path}" if present
        else f"profile {tenant!r} absent (expected {path}) — run bootstrap"
    )
    return tenant, str(path), present, detail


def _default_cluster_probe(config_path: Path, env_name: str) -> tuple[str, str]:
    """Probe live cluster state via the existing ``AidpRestClient``.

    Returns ``(state, detail)``. ``state`` is the AIDP lifecycle string
    (ACTIVE / STOPPED / FAILED / ...) or ``"unprobed"`` when the probe cannot
    run (offline, missing coords, OCI auth failure). NEVER raises — a probe
    failure must classify as not-ACTIVE so the skill fails closed.
    """
    try:
        from oracle_ai_data_platform_fusion_autopilot.commands._config_helpers import (
            env_or_error,
            load_aidp_config,
        )
        from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
            AidpRestClient,
        )

        config = load_aidp_config(config_path)
        env = env_or_error(config, env_name)
        region = env.region or config.defaults.region
        if not (env.ai_data_platform_id and env.workspace_key and env.cluster_key):
            return "unprobed", "cluster probe skipped — dispatch coords incomplete"
        if _PLACEHOLDER in str(env.cluster_key) or _PLACEHOLDER in str(env.ai_data_platform_id):
            return "unprobed", "cluster probe skipped — placeholder dispatch coords"

        client = AidpRestClient(
            region=region,
            aidp_id=env.ai_data_platform_id,
            workspace_key=env.workspace_key,
            oci_profile=env.oci_profile or "DEFAULT",
        )
        clusters = client.list_clusters()
        target = next((c for c in clusters if c.key == env.cluster_key), None)
        if target is None:
            return "unprobed", f"clusterKey {env.cluster_key!r} not visible in workspace"
        return target.state, f"cluster {env.cluster_key!r} state={target.state}"
    except Exception as exc:
        return "unprobed", f"cluster probe failed: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


def check_preconditions(
    *,
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    dispatch_mode: str = "cluster",
    probe_cluster: bool = True,
    cluster_probe: Callable[[Path, str], tuple[str, str]] | None = None,
) -> Preconditions:
    """Run every check and assemble the structured readiness result.

    ``cluster_probe`` is injectable so tests can simulate ACTIVE / STOPPED /
    unprobed without live OCI. Defaults to :func:`_default_cluster_probe`.
    """
    r = Preconditions(dispatch_mode=dispatch_mode)

    # 1. validate (bundle + config schema + ref-integrity).
    validate_ok, validate_detail = check_validate(bundle_path, config_path, env_name)
    r.validate_ok = validate_ok
    r.details["validate"] = validate_detail
    if not validate_ok:
        r.missing.append("bundle")

    # 2. config dispatch-coord placeholders (distinct from missing connectivity).
    placeholders, config_detail = check_config_placeholders(config_path, env_name)
    r.config_placeholders = placeholders
    r.details["config"] = config_detail
    if placeholders:
        r.missing.append("config")

    # 3. tenant profile presence.
    tenant, profile_path, present, profile_detail = resolve_profile(bundle_path)
    r.tenant = tenant
    r.profile_path = profile_path
    r.profile_present = present
    r.details["profile"] = profile_detail
    if not present:
        r.missing.append("profile")

    # 4. cluster state (only meaningful for cluster dispatch; skip for inline).
    if dispatch_mode == "cluster" and probe_cluster:
        probe = cluster_probe or _default_cluster_probe
        state, cluster_detail = probe(config_path, env_name)
        r.cluster_state = state
        r.details["cluster"] = cluster_detail
        if state != "ACTIVE":
            r.missing.append("cluster")
    else:
        r.cluster_state = "unknown"
        r.details["cluster"] = (
            "cluster probe skipped (inline dispatch)" if dispatch_mode != "cluster"
            else "cluster probe skipped (--skip-cluster-probe)"
        )

    r.ok = not r.missing
    return r


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Emit JSON readiness for a seed run.")
    ap.add_argument("--bundle", default="bundle.yaml", type=Path)
    ap.add_argument("--config", default="aidp.config.yaml", type=Path)
    ap.add_argument("--env", default="dev")
    ap.add_argument(
        "--dispatch-mode", default="cluster", choices=["cluster", "inline"],
        help="cluster (default) probes live cluster state; inline skips it.",
    )
    ap.add_argument(
        "--skip-cluster-probe", action="store_true",
        help="Static checks only — no OCI/REST call (CI / offline).",
    )
    ns = ap.parse_args(argv)

    result = check_preconditions(
        bundle_path=ns.bundle,
        config_path=ns.config,
        env_name=ns.env,
        dispatch_mode=ns.dispatch_mode,
        probe_cluster=not ns.skip_cluster_probe,
    )
    print(json.dumps(asdict(result), indent=2))
    # Exit 0 always — the JSON IS the result; readiness is in `ok`, not the
    # process code (so the skill parses one artifact, never guesses from rc).
    return 0


if __name__ == "__main__":
    sys.exit(main())
