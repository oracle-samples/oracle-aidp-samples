#!/usr/bin/env python3
"""Phase-1 readiness gate for every cluster referenced by a migration manifest.

The exporter creates the AIDP workflow, clusters, and job-specific library
attachments.  This gate runs after DAG generation and before data validation so
Phase 2 never starts with libraries still pending or waiting for a restart.
Library mutations remain serial within a cluster, while independent clusters
are prepared concurrently with a bounded worker count.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    from .cluster_states import (
        STABLE_LIBRARY_MUTATION_CLUSTER_STATES,
        TERMINAL_CLUSTER_STATES as FAILED_CLUSTER_STATES,
        TRANSIENT_CLUSTER_STATES,
    )
except ImportError:
    from cluster_states import (
        STABLE_LIBRARY_MUTATION_CLUSTER_STATES,
        TERMINAL_CLUSTER_STATES as FAILED_CLUSTER_STATES,
        TRANSIENT_CLUSTER_STATES,
    )


HEALTHY_LIBRARY_STATES = {"INSTALLED", "RESOLVED"}
RESTART_LIBRARY_STATES = {"INSTALL_ON_RESTART"}
TRANSIENT_LIBRARY_STATES = {"PENDING", "RESOLVING", "INSTALLING"}
FAILED_LIBRARY_STATES = {"FAILED", "SKIPPED"}


def collect_manifest_cluster_ids(
    manifest: dict, fallback_cluster: str = ""
) -> list[str]:
    """Return unique task/default cluster IDs in deterministic DAG order."""
    found: list[str] = []

    def add(value) -> None:
        if isinstance(value, dict):
            value = (
                value.get("cluster_id")
                or value.get("clusterId")
                or value.get("cluster_key")
                or value.get("clusterKey")
                or value.get("key")
            )
        value = str(value or "").strip()
        if value and value not in found:
            found.append(value)

    for job in manifest.get("jobs") or []:
        for task in job.get("tasks") or []:
            add(task.get("cluster_id"))
            add(task.get("cluster"))
        add(job.get("default_cluster"))

    if not found:
        add(fallback_cluster)
    return found


def _library_state_map(items: Iterable[dict], lifecycle) -> dict[str, dict]:
    """Reconcile settled artifacts without hiding an in-flight operation.

    Artifact equivalence comes ONLY from the configured lifecycle's PR #88
    helper (exact repository directories, binary artifacts only). A loaded
    alias can satisfy a settled duplicate, but cannot prove that a PENDING,
    INSTALLING, or unknown operation has released the cluster mutation lock.
    Passing the lifecycle explicitly also preserves the caller's --config.
    """
    grouped: dict[str, list[dict]] = {}
    for item in items:
        # PR #90 review 5694086105: the API retains DELETED tombstones
        # beside newer registrations. Completed deletion is absence, not an
        # in-flight mutation or a loaded library. Exclude it before identity
        # reconciliation; required missing paths still need installation and
        # cannot satisfy a reused report. DELETING/unknown states still block.
        if str(item.get("status") or "UNKNOWN").upper() == "DELETED":
            continue
        path = str(item.get("path") or item.get("name") or "").strip()
        if path:
            grouped.setdefault(path, []).append({
                "path": path,
                "status": str(item.get("status") or "UNKNOWN").upper(),
                "state_message": str(item.get("stateMessage") or ""),
            })

    priority = {
        "INSTALLED": 0,
        "RESOLVED": 0,
        "INSTALL_ON_RESTART": 1,
        "PENDING": 2,
        "RESOLVING": 2,
        "INSTALLING": 2,
        "FAILED": 3,
        "SKIPPED": 3,
    }
    settled = HEALTHY_LIBRARY_STATES | RESTART_LIBRARY_STATES | FAILED_LIBRARY_STATES
    current = {}
    for path, rows in grouped.items():
        pending = [row for row in rows if row["status"] not in settled]
        current[path] = min(
            pending or rows,
            key=lambda row: (priority.get(row["status"], 4), row["status"], row["state_message"]),
        )

    result: dict[str, dict] = {}
    registrations: dict[str, set[str]] = {}
    for path in sorted(current):
        selected_path, item = path, current[path]
        if item["status"] in settled:
            alias_path, alias = lifecycle._best_artifact_registration(current, path)
            if alias is not None and alias["status"] in HEALTHY_LIBRARY_STATES:
                selected_path, item = alias_path, alias
        if selected_path != path:
            print(
                f"[phase-1-cluster] settled library {path} "
                f"({current[path]['status']}) is satisfied by configured "
                f"repository alias {selected_path} ({item['status']})",
                flush=True,
            )
        result[selected_path] = dict(item)
        registrations.setdefault(selected_path, set()).update((path, selected_path))
    for path, paths in registrations.items():
        if len(paths) > 1:
            result[path]["equivalent_paths"] = sorted(paths)
    return result


def summarize_libraries(items: Iterable[dict], lifecycle) -> list[dict]:
    current = _library_state_map(items, lifecycle)
    return [current[path] for path in sorted(current)]


def resolve_max_parallel_clusters(cli_value, runtime_config) -> int:
    """Resolve CLI override over the customer YAML runtime setting."""
    configured = getattr(runtime_config, "phase1_max_parallel_clusters", 3)
    resolved = int(configured if cli_value is None else cli_value)
    if not 1 <= resolved <= 32:
        raise ValueError(
            "phase1_max_parallel_clusters must be between 1 and 32"
        )
    return resolved


def build_readiness_key(lifecycle, cluster_ids: Iterable[str]) -> str:
    """Fingerprint the AIDP target and library contract used by this gate."""
    payload = {
        "aidp_base": str(getattr(lifecycle, "AIDP_BASE", "")),
        "datalake_ocid": str(getattr(lifecycle, "DATALAKE_OCID", "")),
        "workspace_id": str(getattr(lifecycle, "WORKSPACE_ID", "")),
        "cluster_ids": sorted({str(item) for item in cluster_ids}),
        "runtime_requirements": str(
            getattr(lifecycle, "TARGET_RUNTIME_REQUIREMENTS_PATH", "")
        ),
        "compat_wheel": str(getattr(lifecycle, "TARGET_WHEEL_PATH", "")),
        "baseline_jars": sorted(
            str(item) for item in getattr(lifecycle, "BASELINE_JARS", [])
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def read_readiness_report(report_path: str) -> dict:
    """Read an explicitly configured report; malformed shapes are fatal.

    Keep this separate from freshness/contract checks so the launcher can
    validate configuration before opening or modifying any cluster.
    """
    path = Path(report_path)
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise json.JSONDecodeError(
            f"Invalid readiness report {path}: {exc.msg}", exc.doc, exc.pos
        ) from exc
    if not isinstance(report, dict):
        raise ValueError(f"readiness report must be a JSON object: {path}")
    clusters = report.get("clusters", [])
    if not isinstance(clusters, list) or any(not isinstance(item, dict) for item in clusters):
        raise ValueError(f"readiness report clusters must be a list of objects: {path}")
    for cluster in clusters:
        libraries = cluster.get("libraries_after", [])
        if not isinstance(libraries, list) or any(not isinstance(item, dict) for item in libraries):
            raise ValueError(f"readiness report libraries_after must be a list of objects: {path}")
    return report


def load_reusable_readiness_report(
    report_path: str,
    expected_key: str,
    max_age_seconds: int = 1800,
    now: datetime | None = None,
) -> dict[str, dict]:
    """Load a fresh matching Phase-1 report, keyed by cluster ID.

    This is only the disk-level check. Callers must still confirm current live
    cluster/library state before skipping readiness work.
    """
    path = Path(report_path)
    report = read_readiness_report(report_path)
    if report.get("status") != "READY":
        raise RuntimeError(f"readiness report is not READY: {path}")
    if report.get("readiness_key") != expected_key:
        raise RuntimeError(f"readiness report target/library contract changed: {path}")
    try:
        generated = datetime.fromisoformat(str(report["generated_at"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"readiness report has an invalid generated_at: {path}") from exc
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=timezone.utc)
    current_time = now or datetime.now(timezone.utc)
    age = (current_time - generated.astimezone(timezone.utc)).total_seconds()
    if age < 0 or age > max_age_seconds:
        raise RuntimeError(
            f"readiness report is outside the {max_age_seconds}s reuse window: "
            f"age={age:.0f}s ({path})"
        )
    return {
        str(item.get("cluster_id")): item
        for item in report.get("clusters", [])
        if item.get("cluster_id") and item.get("status") == "READY"
    }


def reusable_cluster_report_is_live(
    lifecycle,
    cluster_id: str,
    cluster_report: dict,
) -> tuple[bool, str]:
    """Verify that a reusable report still matches live AIDP state."""
    state = str(lifecycle.get_cluster_state(cluster_id) or "UNKNOWN").upper()
    if state != "ACTIVE":
        return False, f"cluster is {state}, not ACTIVE"
    current = _library_state_map(lifecycle.get_installed_libraries(cluster_id), lifecycle)
    unhealthy = [
        item for item in current.values()
        if item["status"] not in HEALTHY_LIBRARY_STATES
    ]
    if unhealthy:
        return False, f"live libraries are not settled/healthy: {unhealthy}"
    expected_paths = {
        str(item.get("path"))
        for item in cluster_report.get("libraries_after", [])
        if item.get("path")
    }
    # A report can name the old repository path after that inert registration
    # has disappeared. Use the SAME configured alias contract as preparation.
    missing = sorted(
        path for path in expected_paths
        if lifecycle._best_artifact_registration(current, path)[0] is None
    )
    if missing:
        return False, f"reported libraries are now missing: {missing}"
    return True, "live state matches fresh Phase-1 report"


async def wait_for_cluster_library_mutation_state(
    lifecycle,
    cluster_id: str,
    timeout: int = 1800,
    poll_interval: int = 15,
) -> str:
    """Wait until AIDP can safely accept a cluster-library PATCH.

    Newly exported clusters can still be CREATING when Phase 1 starts. AIDP
    rejects library mutations during that lifecycle transition with HTTP 409.
    Preserve stopped clusters as stopped so dependencies can still be staged
    before their next start; only wait out transient lifecycle states here.
    """
    elapsed = 0
    while True:
        state = str(lifecycle.get_cluster_state(cluster_id) or "UNKNOWN").upper()
        if state in STABLE_LIBRARY_MUTATION_CLUSTER_STATES:
            return state
        if state in FAILED_CLUSTER_STATES:
            raise RuntimeError(
                f"Cluster {cluster_id} entered state {state} before library "
                "installation could begin."
            )
        if state not in TRANSIENT_CLUSTER_STATES:
            raise RuntimeError(
                f"Cluster {cluster_id} is in unexpected state {state}; "
                "cannot safely modify libraries."
            )
        if elapsed >= timeout:
            raise RuntimeError(
                f"Cluster {cluster_id} remained {state} for {timeout}s; "
                "timed out waiting to modify libraries."
            )
        print(
            f"[phase-1-cluster] cluster {cluster_id[:12]}... is {state}; "
            f"waiting before library installation "
            f"({elapsed}s / {timeout}s)",
            flush=True,
        )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval


async def wait_for_attached_libraries_to_settle(
    lifecycle,
    cluster_id: str,
    timeout: int = 600,
    poll_interval: int = 15,
    fail_on_failed: bool = True,
) -> list[str]:
    """Wait for attached library operations and return restart-queued paths.

    ``fail_on_failed=False`` is used only by the pre-mutation quiescence gate:
    terminal FAILED/SKIPPED rows no longer own the async cluster lock and must
    remain available to the subsequent dependency-specific repair routines.
    The final readiness check keeps the default fail-loud behavior.
    """
    elapsed = 0
    while True:
        current = _library_state_map(lifecycle.get_installed_libraries(cluster_id), lifecycle)
        failed = [
            item for item in current.values()
            if item["status"] in FAILED_LIBRARY_STATES
        ]
        if failed and fail_on_failed:
            raise RuntimeError(
                f"Attached library installation failed on {cluster_id}: {failed}"
            )

        waiting = [
            item for item in current.values()
            if item["status"] in TRANSIENT_LIBRARY_STATES
            or item["status"] not in (
                HEALTHY_LIBRARY_STATES
                | RESTART_LIBRARY_STATES
                | FAILED_LIBRARY_STATES
            )
        ]
        if not waiting:
            return [
                path for path, item in current.items()
                if item["status"] in RESTART_LIBRARY_STATES
            ]
        if elapsed >= timeout:
            raise RuntimeError(
                f"Attached libraries did not settle within {timeout}s on "
                f"{cluster_id}: {waiting}"
            )
        print(
            f"[phase-1-cluster] waiting for libraries on "
            f"{cluster_id[:12]}...: {waiting}",
            flush=True,
        )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval


async def verify_all_attached_libraries_loaded(
    lifecycle,
    cluster_id: str,
    timeout: int = 600,
    poll_interval: int = 15,
) -> list[dict]:
    """Require every attached library to finish in INSTALLED/RESOLVED."""
    elapsed = 0
    while True:
        items = lifecycle.get_installed_libraries(cluster_id)
        current = _library_state_map(items, lifecycle)
        failures = [
            item for item in current.values()
            if item["status"] in FAILED_LIBRARY_STATES
        ]
        if failures:
            raise RuntimeError(
                f"Attached libraries are unavailable on {cluster_id}: {failures}"
            )
        waiting = [
            item for item in current.values()
            if item["status"] not in HEALTHY_LIBRARY_STATES
        ]
        if not waiting:
            return summarize_libraries(items, lifecycle)
        if elapsed >= timeout:
            raise RuntimeError(
                f"Attached libraries did not become INSTALLED/RESOLVED within "
                f"{timeout}s on {cluster_id}: {waiting}"
            )
        print(
            f"[phase-1-cluster] waiting for loaded libraries on "
            f"{cluster_id[:12]}...: {waiting}",
            flush=True,
        )
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval


def configure_lifecycle(config_path: str):
    """Load YAML coordinates and configure the existing lifecycle module."""
    from aidp_config import load_config
    import cluster_lifecycle as lifecycle

    cfg = load_config(cli_config_path=config_path)
    lifecycle.AIDP_BASE = (
        f"https://aidp.{cfg.aidp.region}.oci.oraclecloud.com/20240831"
    )
    lifecycle.DATALAKE_OCID = cfg.aidp.lake_ocid
    lifecycle.WORKSPACE_ID = cfg.aidp.workspace_id
    lifecycle.OCI_PROFILE = cfg.aidp.oci_profile
    lifecycle._SIGNER = None
    # Re-resolve the dependency repository from the SAME --config so the
    # aidp_compat prerequisite / baseline-jar lookups honor a custom YAML.
    lifecycle.configure(config_path)
    return cfg, lifecycle


async def prepare_cluster(
    lifecycle,
    cluster_id: str,
    cluster_timeout: int = 1800,
    library_timeout: int = 600,
) -> dict:
    """Install required migration libraries and make one target cluster ready."""
    display_name = lifecycle.get_cluster_display_name(cluster_id)
    state_before = str(lifecycle.get_cluster_state(cluster_id)).upper()
    libraries_before = summarize_libraries(
        lifecycle.get_installed_libraries(cluster_id), lifecycle
    )
    print(
        f"[phase-1-cluster] validating {display_name} ({cluster_id}), "
        f"state={state_before}",
        flush=True,
    )

    if state_before == "FAILED":
        await lifecycle.ensure_cluster_running(
            cluster_id,
            timeout=cluster_timeout,
            allow_failed_recovery=True,
        )

    # A newly exported cluster may still be CREATING. The AIDP libraries API
    # returns HTTP 409 while a lifecycle operation owns the cluster lock.
    await wait_for_cluster_library_mutation_state(
        lifecycle,
        cluster_id,
        timeout=cluster_timeout,
    )

    # Cluster lifecycle state can already be ACTIVE while a previously
    # submitted library PATCH is still PENDING/RESOLVING/INSTALLING. A second
    # PATCH during that window returns HTTP 409. Wait for the existing async
    # operation to reach a terminal library state before installing the pinned
    # requirements or any other migration dependency. A short platform lock can
    # remain after status becomes terminal; the bounded HTTP-409 retry remains
    # the authoritative protection for that invisible window.
    initially_queued = await wait_for_attached_libraries_to_settle(
        lifecycle,
        cluster_id,
        timeout=library_timeout,
        fail_on_failed=False,
    )

    # Stage every required library before starting/restarting the cluster. AIDP
    # serializes mutations on one cluster, so these installs intentionally stay
    # ordered. The readiness gate below activates the whole staged set with at
    # most one normal lifecycle transition.
    await lifecycle.ensure_pinned_requirements_installed(
        cluster_id, timeout=library_timeout
    )
    await lifecycle.ensure_aidp_compat_installed(
        cluster_id, timeout=library_timeout
    )
    staged_restart_paths = await wait_for_attached_libraries_to_settle(
        lifecycle,
        cluster_id,
        timeout=library_timeout,
        fail_on_failed=False,
    )
    restart_paths = list(dict.fromkeys([
        *initially_queued,
        *staged_restart_paths,
    ]))
    restarted = bool(await lifecycle.ensure_cluster_ready_for_execution(
        cluster_id,
        cluster_timeout=cluster_timeout,
        library_timeout=library_timeout,
        allow_fresh_cluster_restart=state_before in {"CREATING", "STARTING"},
        additional_restart_paths=restart_paths,
    ))

    # A new restart requirement after activation is an external state change,
    # not permission for a second restart. Fail loudly: restarting here would
    # bypass the shared/control-cluster safety gate and the activation plan.
    queued = await wait_for_attached_libraries_to_settle(
        lifecycle, cluster_id, timeout=library_timeout
    )
    if queued:
        raise RuntimeError(
            f"Cluster {cluster_id} library state changed after activation: "
            f"INSTALL_ON_RESTART: {queued}. No additional restart was attempted. "
            "Resolve the external library change, then rerun readiness."
        )

    libraries_after = await verify_all_attached_libraries_loaded(
        lifecycle, cluster_id, timeout=library_timeout
    )
    state_after = str(lifecycle.get_cluster_state(cluster_id)).upper()
    if state_after != "ACTIVE":
        raise RuntimeError(
            f"Cluster {cluster_id} readiness failed: expected ACTIVE, got {state_after}"
        )

    print(
        f"[phase-1-cluster] READY {display_name} ({cluster_id}); "
        f"libraries={len(libraries_after)}, restarted={restarted}",
        flush=True,
    )
    return {
        "cluster_id": cluster_id,
        "cluster_name": display_name,
        "state_before": state_before,
        "state_after": state_after,
        "restarted": restarted,
        "libraries_before": libraries_before,
        "libraries_after": libraries_after,
        "status": "READY",
    }


async def prepare_clusters_bounded(
    lifecycle,
    cluster_ids: list[str],
    max_parallel: int,
    cluster_timeout: int,
    library_timeout: int,
) -> tuple[list[dict], list[tuple[str, Exception]]]:
    """Prepare distinct clusters concurrently, preserving report order.

    The semaphore limits pressure on the workspace APIs. Each individual
    ``prepare_cluster`` call still enforces the strict per-cluster mutation
    queue and its lifecycle barriers.
    """
    semaphore = asyncio.Semaphore(max(1, max_parallel))

    async def prepare_one(index: int, cluster_id: str):
        async with semaphore:
            try:
                result = await prepare_cluster(
                    lifecycle,
                    cluster_id,
                    cluster_timeout=cluster_timeout,
                    library_timeout=library_timeout,
                )
                return index, result, None
            except Exception as exc:  # retain all cluster outcomes in the report
                return index, {
                    "cluster_id": cluster_id,
                    "status": "FAILED",
                    "error": f"{type(exc).__name__}: {exc}",
                }, exc

    outcomes = await asyncio.gather(*(
        prepare_one(index, cluster_id)
        for index, cluster_id in enumerate(cluster_ids)
    ))
    outcomes.sort(key=lambda item: item[0])
    results = [item[1] for item in outcomes]
    failures = [
        (cluster_ids[item[0]], item[2])
        for item in outcomes
        if item[2] is not None
    ]
    return results, failures


async def run(args) -> dict:
    manifest_path = Path(args.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cluster_ids = collect_manifest_cluster_ids(manifest, args.fallback_cluster)
    if not cluster_ids:
        raise RuntimeError(
            f"No task/default cluster IDs found in manifest {manifest_path}"
        )

    cfg, lifecycle = configure_lifecycle(args.config)
    max_parallel_clusters = resolve_max_parallel_clusters(
        args.max_parallel_clusters, cfg.runtime
    )
    started_at = datetime.now(timezone.utc).isoformat()
    report = {
        "started_at": started_at,
        "manifest": str(manifest_path),
        "status": "RUNNING",
        "cluster_count": len(cluster_ids),
        "max_parallel_clusters": max_parallel_clusters,
        "readiness_key": build_readiness_key(lifecycle, cluster_ids),
        "clusters": [],
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    results, failures = await prepare_clusters_bounded(
        lifecycle,
        cluster_ids,
        max_parallel=max_parallel_clusters,
        cluster_timeout=args.cluster_timeout,
        library_timeout=args.library_timeout,
    )
    report["clusters"] = results
    # Report freshness begins when all readiness work finishes, not when a long
    # multi-cluster Phase 1 starts.
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    if failures:
        first_cluster, first_error = failures[0]
        report["status"] = "FAILED"
        report["error"] = (
            f"{len(failures)} cluster(s) failed; first failure on "
            f"{first_cluster}: {type(first_error).__name__}: {first_error}"
        )
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        raise RuntimeError(report["error"]) from first_error

    report["status"] = "READY"
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[phase-1-cluster] report: {output}", flush=True)
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Validate and prepare all AIDP clusters in a migration manifest"
    )
    parser.add_argument("--config", required=True, help="aidp_migrator YAML path")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fallback-cluster", default="")
    parser.add_argument("--cluster-timeout", type=int, default=1800)
    parser.add_argument("--library-timeout", type=int, default=600)
    parser.add_argument(
        "--max-parallel-clusters",
        type=int,
        default=None,
        help=(
            "Override runtime.phase1_max_parallel_clusters. "
            "Library installs within each cluster always remain serial."
        ),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    report = asyncio.run(run(args))
    return 0 if report.get("status") == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
