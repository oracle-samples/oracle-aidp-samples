"""Phase 4 Step 8 — promoted v2 live dispatcher (parametrized).

Promoted from ``dev/dispatch_v2_seed.py`` (gitignored, hardcoded
demo-pod identifiers) to a permanent test artefact with sensitive
identifiers accepted via CLI flags + env / OCI config. Operator-runnable;
captures the live A/B evidence the ship-ready report cites.

Key differences from ``dev/`` version:

1. **No hardcoded OCIDs / cluster keys / pod URLs.** Accepts:
   ``--region``, ``--aidp-id``, ``--workspace-key``, ``--cluster-key``,
   ``--cluster-name``, ``--bundle``, ``--profile`` — every identifier
   is operator-supplied at dispatch time. Defaults: read from env vars
   (``AIDP_REGION``, ``AIDP_ID``, etc.) so operators can wire it into
   their own ``.envrc`` without editing this file.

2. **Schema-snapshot staging (Phase 3d contract).** When the local
   profile has a paired ``.schema-snapshot.yaml``, the dispatcher
   inlines it into the notebook alongside the profile YAML — cluster-side
   preflight then resolves the snapshot from the same profile-relative
   path the laptop does. Without this, the live v2 run silently falls
   into the warn-and-proceed graceful-degrade branch and loses the
   ``datasetDeltas`` evidence Phase 3d was supposed to surface.

3. **A/B mode.** Pass ``--ab`` to run BOTH backends back-to-back against
   a shared frozen bronze snapshot. **A/B mode is isolation-strict**:
   the dispatcher REQUIRES separate ``--v1-bundle`` and ``--v2-bundle``
   files with distinct ``aidp.bronzeSchema`` / ``aidp.silverSchema`` /
   ``aidp.goldSchema`` keys per backend, and validates the distinction
   before any cluster work. Operators set up the shared frozen bronze
   snapshot themselves (one-shot bronze extract into
   ``bronze_live_snapshot`` then
   ``CREATE TABLE bronze_v{1,2}.<id> AS SELECT * FROM bronze_live_snapshot.<id>``
   for each dataset) — the dispatcher cannot prevent contamination
   if you point both bundles at the same bronze schema.

   The dispatcher writes one structured JSON per backend
   (``phase4_dispatch_<mode>_<backend>_<ts>.json``) carrying the
   ``AIDP_PHASE4_LIVE_RESULT`` marker payload. The operator pastes
   these into the ``TC<N>_v{1,2}_seed.md`` / ``TC<N>_v2_vs_v1_parity.md``
   templates per ``plan.md`` Step 8, including the per-migrated-node
   row-count diff, ``DESCRIBE TABLE`` schema diff, ``xxhash64_agg``
   non-audit checksum, and audit-column presence verification. The
   dispatcher CANNOT compute the checksums itself (the agg needs to
   run server-side on the cluster's silver/gold schemas, with per-table
   natural-key projections that differ per node) — operator-runbook
   responsibility, not dispatcher logic.

NOT executed by CI — operator-driven only. The evidence files this
produces are committed to ``tests/live/`` once captured.

Usage:
  .venv/bin/python tests/live/dispatch_v2_seed.py \\
      --region us-ashburn-1 \\
      --aidp-id ocid1.datalake.oc1.iad.... \\
      --workspace-key <uuid> \\
      --cluster-key <uuid> \\
      --cluster-name <name> \\
      --bundle dev/fusion-finance-starter.live.yaml \\
      --profile finance-default
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "skills/aidp-rest"))
sys.path.insert(0, str(REPO / ".codex/skills/fusion-tc26-run"))

# The aidp-rest client and the tc26 build_wheel helper are shipped
# skills — reuse rather than reimplement.
try:
    from client import AidpRestClient  # type: ignore[import-not-found]
    from dispatch import build_wheel  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover — operator environment only
    raise SystemExit(
        f"phase4 dispatcher requires the aidp-rest + fusion-tc26-run "
        f"skills on sys.path; {exc}"
    )


def _env_or(arg: str | None, env_key: str, *, required: bool = True) -> str:
    if arg is not None:
        return arg
    val = os.environ.get(env_key)
    if val:
        return val
    if not required:
        return ""
    raise SystemExit(
        f"missing required arg: --{env_key.lower().replace('aidp_', '')} "
        f"or env var {env_key!r}"
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default=None)
    p.add_argument("--aidp-id", dest="aidp_id", default=None)
    p.add_argument("--workspace-key", dest="workspace_key", default=None)
    p.add_argument("--cluster-key", dest="cluster_key", default=None)
    p.add_argument("--cluster-name", dest="cluster_name", default=None)
    p.add_argument(
        "--workspace-dir", dest="workspace_dir",
        default="/Workspace/Shared/fusion-autopilot-phase4-seed",
        help="Server-side notebook upload root.",
    )
    p.add_argument(
        "--secret-name", dest="secret_name",
        default=os.environ.get("AIDP_FUSION_SECRET_NAME", "fusion_bicc_password"),
    )
    p.add_argument(
        "--secret-key", dest="secret_key",
        default=os.environ.get("AIDP_FUSION_SECRET_KEY", "password"),
    )
    p.add_argument(
        "--bundle", default=None,
        help="Single-backend mode: path to local bundle.yaml. "
             "Mutually exclusive with --v1-bundle/--v2-bundle.",
    )
    p.add_argument(
        "--v1-bundle", dest="v1_bundle", default=None,
        help="A/B mode: path to the legacy-python bundle. MUST have "
             "distinct aidp.bronzeSchema / silverSchema / goldSchema "
             "from --v2-bundle (dispatcher enforces).",
    )
    p.add_argument(
        "--v2-bundle", dest="v2_bundle", default=None,
        help="A/B mode: path to the content-pack bundle. MUST have "
             "distinct aidp.bronzeSchema / silverSchema / goldSchema "
             "from --v1-bundle.",
    )
    p.add_argument(
        "--profile", required=True,
        help="Profile name (looked up at <bundle.parent>/profiles/<name>.yaml).",
    )
    p.add_argument(
        "--ab", action="store_true",
        help="A/B mode — REQUIRES --v1-bundle + --v2-bundle (refuses to "
             "run with a single shared --bundle). See module docstring.",
    )
    p.add_argument(
        "--mode", default="seed", choices=("seed", "incremental"),
        help="orchestrator.run mode.",
    )
    p.add_argument(
        "--force-fingerprint-skip", dest="force_fingerprint_skip",
        action="store_true",
        help="Break-glass: skip the bronze-fingerprint drift gate (writes an "
             "audit row). Use ONLY when the schema is known-current — e.g. a "
             "just-seeded, bronze-only, single-dataset test where the profile-"
             "wide fingerprint references datasets not materialized in scope.",
    )
    p.add_argument(
        "--layers", default="silver,gold",
        help="Comma-separated layers to run (orchestrator.run --layers). "
             "Default 'silver,gold' assumes a pre-populated bronze (A/B "
             "shared-frozen-bronze pattern). For a fresh dispatch with no "
             "cached bronze, pass 'bronze,silver,gold'.",
    )
    p.add_argument(
        "--out-dir", dest="out_dir", default="tests/live/",
        help="Where evidence markdown files are written (post-dispatch).",
    )
    return p.parse_args()


def _stage_yaml(name: str, local: Path) -> str:
    """Return a Python literal string the notebook cell uses to write
    the YAML to a cluster-side path. Inlining avoids a second upload."""
    return local.read_text()


def _load_profile_with_snapshot(bundle: Path, profile_name: str) -> tuple[str, str | None]:
    """Read profile + paired schema-snapshot YAMLs from
    ``<bundle.parent>/profiles/<profile_name>.{yaml,schema-snapshot.yaml}``.

    Returns ``(profile_text, snapshot_text_or_None)``. Phase 3d contract:
    when the snapshot is absent, return ``None`` so the dispatcher
    omits it from the notebook (cluster-side preflight degrades to
    warn-and-proceed).
    """
    profiles_dir = bundle.resolve().parent / "profiles"
    profile_path = profiles_dir / f"{profile_name}.yaml"
    if not profile_path.exists():
        raise SystemExit(f"profile not found at {profile_path}")
    snapshot_path = profiles_dir / f"{profile_name}.schema-snapshot.yaml"
    snapshot_text = snapshot_path.read_text() if snapshot_path.exists() else None
    return profile_path.read_text(), snapshot_text


def build_notebook(
    *,
    wheel: Path, bundle_yaml: str, profile_name: str,
    profile_yaml: str, snapshot_yaml: str | None,
    secret_name: str, secret_key: str,
    backend: str, mode: str, layers: list[str],
    force_fingerprint_skip: bool = False,
) -> dict:
    """Generate the executable notebook payload.

    The cluster-side flow:
    1. Install the wheel from inlined base64.
    2. Resolve BICC secret via AIDP's secrets helper.
    3. Write bundle + profile (+ snapshot if present) to local cwd.
    4. Run ``orchestrator.run`` with the requested backend.
    5. Emit ``AIDP_PHASE4_LIVE_RESULT_BEGIN ... END`` marker carrying
       run_id, per-step status, fingerprint metadata, and timing.
    """
    wheel_b64 = base64.b64encode(wheel.read_bytes()).decode()

    install_cell = (
        f'import base64, subprocess, sys, tempfile, pathlib\n'
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="phase4_plugin_"))\n'
        f'_whl = _stage / "{wheel.name}"\n'
        f'_whl.write_bytes(base64.b64decode(WHEEL_B64))\n'
        f'_target = _stage / "site-packages"\n'
        f'_target.mkdir()\n'
        f'res = subprocess.run([sys.executable, "-m", "pip", "install", '
        f'"--quiet", "--no-deps", "--target", str(_target), str(_whl)], '
        f'capture_output=True, text=True, timeout=180)\n'
        f'print(f"pip rc={{res.returncode}}")\n'
        f'if res.returncode != 0:\n'
        f'    print("STDOUT:", res.stdout[-2000:]); print("STDERR:", res.stderr[-2000:])\n'
        f'    raise RuntimeError("wheel install failed")\n'
        f'sys.path.insert(0, str(_target))\n'
    )

    snapshot_write = ""
    if snapshot_yaml is not None:
        # Phase 3d staging: write snapshot alongside profile.
        snapshot_write = (
            f'pathlib.Path("profiles/{profile_name}.schema-snapshot.yaml")'
            f'.write_text({snapshot_yaml!r})\n'
            f'print("phase 3d snapshot staged at profiles/'
            f'{profile_name}.schema-snapshot.yaml")\n'
        )
    else:
        snapshot_write = (
            'print("phase 3d snapshot NOT staged — preflight will use '
            'warn-and-proceed graceful-degrade branch")\n'
        )

    creds_cell = (
        f'import os, pathlib\n'
        f'pw = aidputils.secrets.get(name={secret_name!r}, key={secret_key!r})  # noqa: F821\n'
        f'os.environ["FUSION_BICC_PASSWORD"] = pw\n'
        f'pathlib.Path("bundle.yaml").write_text({bundle_yaml!r})\n'
        f'pathlib.Path("profiles").mkdir(exist_ok=True)\n'
        f'pathlib.Path("profiles/{profile_name}.yaml")'
        f'.write_text({profile_yaml!r})\n'
        f'{snapshot_write}'
        f'BUNDLE_PATH = pathlib.Path("bundle.yaml").resolve()\n'
        f'print("bundle + profile written")\n'
    )

    run_cell = (
        f'import time, json, traceback\n'
        f'from oracle_ai_data_platform_fusion_autopilot import orchestrator\n'
        f'def _fmt_step(s):\n'
        f'    rc = s.row_count if s.row_count is not None else "-"\n'
        f'    em = (s.error_message or "")[:120]\n'
        f'    err = " err=" + em if s.status in ("failed", "skipped") and em else ""\n'
        f'    print("  {{:7s}} {{:24s}} {{:10s}} rows={{:>10s}} dur={{:.2f}}s{{}}".format(\n'
        f'        s.layer, s.dataset_id, s.status, str(rc), s.duration_seconds, err))\n'
        f'print("=== Phase 4 — backend={backend!r} mode={mode!r} ===")\n'
        f't0 = time.time()\n'
        f'payload = {{"backend": {backend!r}, "mode": {mode!r}}}\n'
        f'try:\n'
        f'    if {backend!r} == "content-pack":\n'
        f'        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_full_chain\n'
        f'        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle, resolve_content_pack_root\n'
        f'        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import load_tenant_profile\n'
        f'        bundle_obj, _paths = load_bundle(BUNDLE_PATH)\n'
        f'        pack_root = resolve_content_pack_root(BUNDLE_PATH, bundle_obj.content_pack)\n'
        f'        resolved_pack = load_full_chain(pack_root)\n'
        f'        profile_path = BUNDLE_PATH.parent / "profiles" / f"{{bundle_obj.content_pack.profile}}.yaml"\n'
        f'        tenant_profile = load_tenant_profile(profile_path)\n'
        f'        payload["pinned_fingerprint"] = tenant_profile.bronze_schema_fingerprint\n'
        f'        summary = orchestrator.run(\n'
        f'            bundle_path=BUNDLE_PATH, spark=spark, mode={mode!r},\n'
        f'            layers={layers!r}, dry_run=False,\n'
        f'            force_fingerprint_skip={force_fingerprint_skip!r},\n'
        f'            resolved_pack=resolved_pack, tenant_profile=tenant_profile,\n'
        f'        )\n'
        f'    else:\n'
        f'        summary = orchestrator.run(\n'
        f'            bundle_path=BUNDLE_PATH, spark=spark, mode={mode!r},\n'
        f'            layers={layers!r}, dry_run=False,\n'
        f'            force_fingerprint_skip={force_fingerprint_skip!r},\n'
        f'        )\n'
        f'    for s in summary.steps: _fmt_step(s)\n'
        f'    payload.update({{\n'
        f'        "run_id": summary.run_id,\n'
        f'        "succeeded": summary.succeeded,\n'
        f'        "failed": summary.failed,\n'
        f'        "skipped": summary.skipped,\n'
        f'        "total_duration_seconds": summary.total_duration_seconds,\n'
        f'        "wall_seconds": time.time() - t0,\n'
        f'        "steps": [\n'
        f'            {{"dataset_id":s.dataset_id,"layer":s.layer,"status":s.status,\n'
        f'             "row_count":s.row_count,"duration_seconds":s.duration_seconds,\n'
        f'             "skip_reason":s.skip_reason,"error_message":(s.error_message or "")[:200]}}\n'
        f'            for s in summary.steps\n'
        f'        ],\n'
        f'    }})\n'
        f'except Exception as e:\n'
        f'    traceback.print_exc()\n'
        f'    payload["error"] = str(e); payload["traceback"] = traceback.format_exc()[-2000:]\n'
        # AIDP cluster Jupyter wraps stdout as display_data text/plain and
        # strips JSON-escape backslashes, corrupting embedded quotes. Wrap
        # the payload in base64 so the on-cluster encoding can be lossy
        # without breaking the marker.
        f'import base64 as _b64\n'
        f'_b64_payload = _b64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")\n'
        f'print("AIDP_PHASE4_LIVE_RESULT_BEGIN", _b64_payload, "AIDP_PHASE4_LIVE_RESULT_END")\n'
    )

    def code_cell(src: str) -> dict:
        return {"cell_type": "code", "metadata": {}, "source": src, "outputs": [],
                "execution_count": None}

    return {
        "cells": [code_cell(install_cell), code_cell(creds_cell), code_cell(run_cell)],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3", "language": "python", "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4, "nbformat_minor": 5,
    }


def _parse_b64_marker(
    executed: dict,
    *,
    begin: str = "AIDP_PHASE4_LIVE_RESULT_BEGIN",
    end: str = "AIDP_PHASE4_LIVE_RESULT_END",
) -> dict | None:
    """Walk executed-notebook outputs for a base64-wrapped marker block.

    The cluster-side payload is base64-encoded JSON to survive AIDP's
    display_data text/plain formatter (which strips JSON-escape
    backslashes). Producer side: ``build_notebook`` ``run_cell``.
    """
    for cell in executed.get("cells", []):
        for output in cell.get("outputs", []):
            for src in ("text", "data"):
                value = output.get(src)
                if value is None:
                    continue
                if src == "data":
                    value = value.get("text/plain", "")
                if isinstance(value, list):
                    value = "".join(value)
                if begin in value:
                    b = value.index(begin) + len(begin)
                    e = value.index(end, b)
                    token = value[b:e].strip()
                    try:
                        raw = base64.b64decode(token).decode("utf-8")
                        return json.loads(raw)
                    except Exception:
                        return {"_marker_decode_error": token[:200]}
    return None


def dispatch_one(
    client: "AidpRestClient", *,
    workspace_dir: str, notebook_name: str, notebook: dict,
    cluster_key: str, cluster_name: str, task_key: str,
    poll_timeout_s: int = 1800, poll_interval_s: int = 20,
) -> dict:
    """Upload notebook + create Job + JobRun + poll to terminal +
    fetch executed notebook. Returns the parsed marker payload (empty
    dict on failure-without-marker — caller decides how to surface).
    """
    print(f"==> uploading {notebook_name} to {workspace_dir}/")
    nb_path = f"{workspace_dir}/{notebook_name}"
    client.upload_notebook(nb_path, notebook)

    # AIDP job names: letter-start, [A-Za-z0-9_/] only — strip the
    # `.ipynb` extension and any other punctuation the timestamp/path
    # may have carried in.
    job_name = notebook_name.removesuffix(".ipynb").replace(".", "_")
    job_key = client.create_notebook_job(
        name=job_name,
        description=f"Phase 4 dispatch ({task_key})",
        notebook_path=nb_path,
        cluster_key=cluster_key, cluster_name=cluster_name,
        task_key=task_key,
    )
    run_key = client.submit_run(job_key)
    print(f"==> job={job_key} run={run_key} — polling")
    result = client.poll_run(
        run_key, timeout_s=poll_timeout_s, interval_s=poll_interval_s,
    )
    print(f"==> terminal status: {result.status}")
    task_run_key = client.resolve_task_run_key(result.raw, task_key)
    nb_str = client.fetch_output(task_run_key)
    executed = json.loads(nb_str) if nb_str else {}
    marker = _parse_b64_marker(executed) or {}
    marker.setdefault("_terminal_status", result.status)
    marker.setdefault("_job_key", job_key)
    marker.setdefault("_run_key", run_key)
    marker.setdefault("_task_run_key", task_run_key)
    if result.status != "SUCCESS":
        cell_errs = client.extract_cell_errors(executed)
        if cell_errs:
            marker.setdefault("_cell_errors", cell_errs[:5])
    return marker


def _read_bundle_schemas(bundle_path: Path) -> tuple[str, str, str]:
    """Parse a bundle YAML and return ``(bronzeSchema, silverSchema,
    goldSchema)`` from its ``aidp:`` block. Used by ``--ab`` mode to
    enforce isolation between the v1 and v2 bundles BEFORE any cluster
    work fires — a contaminated A/B is unciteable and any later
    enforcement is moot."""
    import yaml  # type: ignore[import-not-found]
    obj = yaml.safe_load(bundle_path.read_text())
    aidp = obj.get("aidp", {}) if isinstance(obj, dict) else {}
    return (
        str(aidp.get("bronzeSchema", "")),
        str(aidp.get("silverSchema", "")),
        str(aidp.get("goldSchema", "")),
    )


def _assert_ab_bundle_isolation(v1: Path, v2: Path) -> None:
    """A/B isolation precondition. Refuses to proceed when the two
    bundles point at the same target schemas — that's the contamination
    failure mode the reviewer flagged.

    Per ``plan.md`` Step 8: bundle isolation is non-negotiable. The
    dispatcher cannot guarantee a shared-frozen-bronze setup
    (operators do that out of band), but it CAN block obvious
    contamination."""
    v1_b, v1_s, v1_g = _read_bundle_schemas(v1)
    v2_b, v2_s, v2_g = _read_bundle_schemas(v2)
    issues = []
    if v1_b == v2_b:
        issues.append(
            f"aidp.bronzeSchema identical ({v1_b!r}) — both backends "
            "would write to the same fusion_autopilot_state table and "
            "contaminate each other's state rows"
        )
    if v1_s == v2_s:
        issues.append(
            f"aidp.silverSchema identical ({v1_s!r}) — the second "
            "backend's silver writes would clobber the first's"
        )
    if v1_g == v2_g:
        issues.append(
            f"aidp.goldSchema identical ({v1_g!r}) — the second "
            "backend's gold writes would clobber the first's"
        )
    if issues:
        raise SystemExit(
            "A/B mode refuses to run with non-isolated bundles. "
            "Phase 4 ship-ready evidence is unciteable if backends "
            "share target schemas. Issues found:\n  - "
            + "\n  - ".join(issues)
            + "\nRemediate: edit --v1-bundle and --v2-bundle so each "
            "carries DISTINCT bronzeSchema / silverSchema / goldSchema "
            "(e.g. bronze_live_v1 / bronze_live_v2). The shared "
            "frozen bronze snapshot copy is an operator pre-step; "
            "see the module docstring."
        )


def main() -> int:
    args = _parse_args()
    region = _env_or(args.region, "AIDP_REGION")
    aidp_id = _env_or(args.aidp_id, "AIDP_ID")
    workspace_key = _env_or(args.workspace_key, "AIDP_WORKSPACE_KEY")
    cluster_key = _env_or(args.cluster_key, "AIDP_CLUSTER_KEY")
    cluster_name = _env_or(args.cluster_name, "AIDP_CLUSTER_NAME")

    # ----- Validate bundle args + A/B isolation ----------------------
    if args.ab:
        if not (args.v1_bundle and args.v2_bundle):
            raise SystemExit(
                "A/B mode requires --v1-bundle AND --v2-bundle (--bundle "
                "is single-backend only). The two bundles MUST carry "
                "distinct aidp.bronzeSchema / silverSchema / goldSchema; "
                "the dispatcher validates this before any cluster work."
            )
        if args.bundle:
            raise SystemExit(
                "A/B mode rejects --bundle: pass --v1-bundle + --v2-bundle. "
                "A shared --bundle would point both backends at the same "
                "schemas, which the dispatcher cannot allow."
            )
        v1_bundle = Path(args.v1_bundle).resolve()
        v2_bundle = Path(args.v2_bundle).resolve()
        if not v1_bundle.exists():
            raise SystemExit(f"--v1-bundle not found: {v1_bundle}")
        if not v2_bundle.exists():
            raise SystemExit(f"--v2-bundle not found: {v2_bundle}")
        _assert_ab_bundle_isolation(v1_bundle, v2_bundle)
        bundle_for_backend = {
            "legacy-python": v1_bundle,
            "content-pack": v2_bundle,
        }
    else:
        if not args.bundle:
            raise SystemExit(
                "Single-backend mode requires --bundle. For A/B parity, "
                "use --ab --v1-bundle <p1> --v2-bundle <p2>."
            )
        single = Path(args.bundle).resolve()
        if not single.exists():
            raise SystemExit(f"--bundle not found: {single}")
        bundle_for_backend = {"content-pack": single}

    # Profile + snapshot live next to one of the bundles (operator
    # convention: both bundles share a `profiles/` sibling dir, since
    # the profile is tenant-identity-bound, not backend-bound).
    profile_anchor = next(iter(bundle_for_backend.values()))
    profile_text, snapshot_text = _load_profile_with_snapshot(
        profile_anchor, args.profile,
    )

    print(f"==> building wheel from {REPO}")
    workdir = Path(tempfile.mkdtemp(prefix="phase4_dispatch_"))
    wheel = build_wheel(REPO, workdir / "dist")
    print(f"==> wheel: {wheel.name} ({wheel.stat().st_size // 1024} KiB)")

    # cluster_name is collected for operator readability in the marker
    # payload + audit trail, but AidpRestClient itself addresses the
    # cluster by key; cluster_name is not passed to __init__.
    _ = cluster_name
    client = AidpRestClient(
        region=region, aidp_id=aidp_id, workspace_key=workspace_key,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    backends = ("legacy-python", "content-pack") if args.ab else ("content-pack",)
    results: dict[str, dict] = {}
    for backend in backends:
        backend_bundle = bundle_for_backend[backend]
        bundle_text = backend_bundle.read_text()
        notebook = build_notebook(
            wheel=wheel, bundle_yaml=bundle_text,
            profile_name=args.profile,
            profile_yaml=profile_text, snapshot_yaml=snapshot_text,
            secret_name=args.secret_name, secret_key=args.secret_key,
            backend=backend, mode=args.mode,
            layers=[s.strip() for s in args.layers.split(",") if s.strip()],
            force_fingerprint_skip=args.force_fingerprint_skip,
        )
        notebook_name = (
            f"phase4_{args.mode}_{backend.replace('-', '_')}_"
            f"{int(time.time())}.ipynb"
        )
        marker = dispatch_one(
            client, workspace_dir=args.workspace_dir,
            notebook_name=notebook_name, notebook=notebook,
            cluster_key=cluster_key, cluster_name=cluster_name,
            task_key=f"phase4_{backend.replace('-', '_')}_{args.mode}",
        )
        results[backend] = marker
        print(f"==> {backend} marker: {json.dumps(marker, indent=2)[:800]}")

        # One per-backend marker JSON so operators can paste them into
        # the TC<N>_v{1,2}_seed.md templates verbatim. The combined
        # parity report (TC<N>_v2_vs_v1_parity.md) is operator-written
        # because it needs server-side xxhash64_agg checksums that the
        # dispatcher cannot compute from the marker payload alone.
        per_backend_path = out_dir / (
            f"phase4_dispatch_{args.mode}_"
            f"{backend.replace('-', '_')}_{int(time.time())}.json"
        )
        per_backend_path.write_text(json.dumps(marker, indent=2))
        print(f"==> {backend} marker written to {per_backend_path}")

    payload = {"region": region, "mode": args.mode, "results": results}
    out_path = out_dir / f"phase4_dispatch_{args.mode}_combined_{int(time.time())}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"==> combined dispatch payload written to {out_path}")
    if args.ab:
        print(
            "==> A/B run complete. Next steps (operator runbook):\n"
            "    1. Copy the v1 + v2 marker JSONs into the matching\n"
            "       tests/live/TC<N>_v{1,2}_seed.md templates.\n"
            "    2. Run server-side `SELECT xxhash64_agg(struct(<non-audit cols>) ORDER BY <natural_key>) "
            "FROM <silver|gold>_<v1,v2>.<dataset>` for every migrated node\n"
            "       and paste the checksum pair verbatim into TC<N>_v2_vs_v1_parity.md.\n"
            "    3. Run `DESCRIBE TABLE` on each (v1, v2) pair and commit the diff (must be empty\n"
            "       modulo audit cols).\n"
            "    4. Run `SELECT COUNT(*)` on each (v1, v2) pair and confirm delta = 0.\n"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover — operator entry point
    raise SystemExit(main())
