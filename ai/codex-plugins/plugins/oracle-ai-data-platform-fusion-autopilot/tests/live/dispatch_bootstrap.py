"""Phase 4.1 / D3 — operator-runnable cluster-side bootstrap dispatcher.

Mirror of ``tests/live/dispatch_v2_seed.py`` for the new
``aidp-fusion-autopilot bootstrap --dispatch-mode=cluster`` flow. Runs a
full bootstrap against a real tenant cluster + captures the produced
``profiles/<tenant>.yaml`` + evidence + schema snapshot for the
v2-phase-4.1 live-evidence trail.

NOT executed by CI — operator-driven only. The output files this
produces (profile YAML, evidence snapshot, schema snapshot) are the
evidence the Phase 4.1 ship-ready summary cites.

Two ways to run:

**Mode A — driver (recommended)**: this script as a thin wrapper that
shells out to ``aidp-fusion-autopilot bootstrap --dispatch-mode=cluster``
with the operator's CLI flags. Validates the produced files
post-invocation. Sidesteps the need to author cluster identifiers
inside this script.

**Mode B — direct**: call
``commands.cluster_bootstrap_probe.dispatch_cluster_probe(...)``
in-process with operator-supplied identifiers. Useful when iterating
on the dispatcher itself without the full bootstrap CLI surface.

Usage:
    .venv/bin/python tests/live/dispatch_bootstrap.py \\
        --bundle dev/fusion-finance-starter.live.yaml \\
        --config dev/aidp.config.yaml \\
        --env dev

Environment-variable defaults so operators wire identifiers into
``.envrc`` once:
    AIDP_REGION              (default: us-ashburn-1)
    AIDP_FUSION_CLUSTER_KEY
    AIDP_FUSION_CLUSTER_NAME
    AIDP_FUSION_WORKSPACE_DIR  (optional; CLI derives from workspace_root)

Outputs (mode A):
    profiles/<tenant>.yaml
    evidence/<tenant>/<ISO-ts>.yaml
    profiles/<tenant>.schema-snapshot.yaml
    tests/live/phase4_1_dispatch_bootstrap_<ts>.json
        (this script's marker file — operator pastes its contents into
         the live-evidence markdown template)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import yaml
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--bundle", type=Path, required=True,
        help="Path to the operator's bundle.yaml.",
    )
    p.add_argument(
        "--config", type=Path, required=True,
        help="Path to the operator's aidp.config.yaml.",
    )
    p.add_argument(
        "--env", default="dev",
        help="Environment key from aidp.config.yaml (default: dev).",
    )
    p.add_argument(
        "--cluster-key", dest="cluster_key", default=None,
        help="Cluster UUID; falls back to AIDP_FUSION_CLUSTER_KEY env var.",
    )
    p.add_argument(
        "--cluster-name", dest="cluster_name", default=None,
        help="Cluster display name; falls back to AIDP_FUSION_CLUSTER_NAME.",
    )
    p.add_argument(
        "--workspace-dir", dest="workspace_dir", default=None,
        help=(
            "Server-side notebook upload root; falls back to "
            "AIDP_FUSION_WORKSPACE_DIR. When unset, the CLI derives "
            "/Workspace/{workspace_root}/fusion-autopilot-bootstrap."
        ),
    )
    p.add_argument(
        "--refresh", action="store_true",
        help="Pass --refresh to bootstrap (re-walk against drifted bronze).",
    )
    p.add_argument(
        "--operator", default=os.environ.get("AIDP_OPERATOR"),
        help="Explicit --operator value (defaults to $AIDP_OPERATOR).",
    )
    return p.parse_args()


def _resolve(arg: str | None, env_var: str) -> str | None:
    if arg:
        return arg
    return os.environ.get(env_var) or None


def _assert_bundle_layout(bundle: Path) -> tuple[str, Path]:
    """Read the bundle YAML to extract the tenant name + bundle parent
    dir. The post-invocation assertions key off both."""
    raw = yaml.safe_load(bundle.read_text(encoding="utf-8"))
    content_pack = raw.get("contentPack")
    if not content_pack:
        raise SystemExit(
            f"bundle {bundle!r} has no contentPack: section — Phase 4.1 "
            f"bootstrap requires content-pack-enabled bundles."
        )
    tenant = content_pack.get("profile") or content_pack.get("name")
    if not tenant:
        raise SystemExit(
            f"bundle {bundle!r} contentPack lacks profile/name."
        )
    return tenant, bundle.resolve().parent


def _run_bootstrap(args: argparse.Namespace) -> int:
    """Invoke ``aidp-fusion-autopilot bootstrap`` as a subprocess with the
    operator-supplied flags. Returns the CLI's exit code."""
    # Resolve the `aidp-fusion-autopilot` console script alongside the
    # interpreter that started this dispatcher. The plugin package
    # registers an entry point but no `__main__`, so `python -m
    # oracle_ai_data_platform_fusion_autopilot` fails — see
    # pyproject.toml::[project.scripts].
    cli_bin = Path(sys.executable).parent / "aidp-fusion-autopilot"
    if not cli_bin.exists():
        raise SystemExit(
            f"aidp-fusion-autopilot console script not found at {cli_bin}. "
            f"Install the plugin into {Path(sys.executable).parent.parent} "
            f"(e.g. `pip install -e .`)."
        )
    cmd = [
        str(cli_bin),
        "--bundle", str(args.bundle),
        "--config", str(args.config),
        "--env", args.env,
        "bootstrap",
        "--dispatch-mode", "cluster",
    ]
    cluster_key = _resolve(args.cluster_key, "AIDP_FUSION_CLUSTER_KEY")
    cluster_name = _resolve(args.cluster_name, "AIDP_FUSION_CLUSTER_NAME")
    workspace_dir = _resolve(args.workspace_dir, "AIDP_FUSION_WORKSPACE_DIR")
    if cluster_key:
        cmd += ["--cluster-key", cluster_key]
    if cluster_name:
        cmd += ["--cluster-name", cluster_name]
    if workspace_dir:
        cmd += ["--workspace-dir", workspace_dir]
    if args.refresh:
        cmd.append("--refresh")
    if args.operator:
        cmd += ["--operator", args.operator]

    print(f"[dispatch_bootstrap] cmd: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(REPO))
    return proc.returncode


def _assert_outputs(tenant: str, workdir: Path) -> dict:
    """Post-invocation assertions — confirm the laptop-side writers
    populated the three expected files. Returns a summary dict the
    marker JSON carries."""
    profile_path = workdir / "profiles" / f"{tenant}.yaml"
    snapshot_path = workdir / "profiles" / f"{tenant}.schema-snapshot.yaml"
    evidence_dir = workdir / "evidence" / tenant

    assert profile_path.exists(), (
        f"expected profile at {profile_path} — bootstrap did not "
        f"write it. Check .aidp/diagnostics/ for AIDPF-2048 / 2049."
    )
    assert evidence_dir.exists() and any(evidence_dir.iterdir()), (
        f"expected evidence under {evidence_dir} — bootstrap did not "
        f"write any snapshot."
    )

    profile_raw = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    summary = {
        "tenant": tenant,
        "profile_path": str(profile_path.relative_to(REPO)),
        "evidence_dir": str(evidence_dir.relative_to(REPO)),
        "schema_snapshot_path": (
            str(snapshot_path.relative_to(REPO)) if snapshot_path.exists() else None
        ),
        "fingerprint": profile_raw.get("bronzeSchemaFingerprint"),
        "resolved_column_count": len(
            (profile_raw.get("resolved", {}) or {}).get("column", {}) or {}
        ),
        "resolved_semantic_count": len(
            (profile_raw.get("resolved", {}) or {}).get("semantic", {}) or {}
        ),
    }
    if not snapshot_path.exists():
        print(
            f"[dispatch_bootstrap] warn: no schema-snapshot at "
            f"{snapshot_path} — pre-3d profile, expected for tenants "
            f"first bootstrapped before Phase 3d shipped."
        )
    return summary


def main() -> int:
    args = _parse_args()
    if not args.bundle.exists():
        raise SystemExit(f"--bundle {args.bundle!r} not found")
    if not args.config.exists():
        raise SystemExit(f"--config {args.config!r} not found")

    tenant, workdir = _assert_bundle_layout(args.bundle)
    print(f"[dispatch_bootstrap] tenant={tenant} workdir={workdir}")

    t_start = time.time()
    rc = _run_bootstrap(args)
    elapsed = time.time() - t_start

    if rc != 0:
        print(
            f"[dispatch_bootstrap] bootstrap exited rc={rc} after "
            f"{elapsed:.1f}s. Inspect "
            f"{workdir / '.aidp' / 'diagnostics'} for diagnostic "
            f"artifacts."
        )
        return rc

    summary = _assert_outputs(tenant, workdir)
    summary["elapsed_seconds"] = round(elapsed, 1)
    summary["dispatch_mode"] = "cluster"
    summary["refresh"] = args.refresh

    marker_path = REPO / "tests" / "live" / f"phase4_1_dispatch_bootstrap_{int(time.time())}.json"
    marker_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(
        f"[dispatch_bootstrap] PASS — wrote summary to "
        f"{marker_path.relative_to(REPO)}"
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
