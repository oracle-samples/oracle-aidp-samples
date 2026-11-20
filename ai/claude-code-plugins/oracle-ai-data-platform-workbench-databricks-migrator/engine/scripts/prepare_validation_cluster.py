#!/usr/bin/env python3
"""Prepare an exported AIDP task cluster before Pass-2 conversion validation.

The export phase creates/reuses the target cluster and attaches source task
libraries.  This gate makes that asynchronous state deterministic: attach any
required workload libraries, restart when JARs are queued for restart, start
the cluster, install aidp_compat, and refuse to continue until every attached
library is healthy.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

import oci
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:  # Package import in tests/tools; script import in AIDP notebooks.
    from . import cluster_lifecycle as lifecycle  # type: ignore
    from .cluster_states import (  # type: ignore
        STARTABLE_CLUSTER_STATES,
        TERMINAL_CLUSTER_STATES,
    )
except ImportError:
    import cluster_lifecycle as lifecycle  # noqa: E402
    from cluster_states import (  # noqa: E402
        STARTABLE_CLUSTER_STATES,
        TERMINAL_CLUSTER_STATES,
    )


HEALTHY = {"INSTALLED", "RESOLVED"}
QUEUED = {"INSTALL_ON_RESTART"}
TRANSIENT = {"PENDING", "RESOLVING", "INSTALLING"}
FAILED = {"FAILED", "SKIPPED"}
DEFAULT_COMPAT_LIBRARIES = (
    "/Workspace/migration-dependencies/wheels/setuptools-80.10.2-py3-none-any.whl",
)


def _status(item: dict) -> str:
    return str(item.get("status") or "UNKNOWN").upper()


def summarize_libraries(items: list[dict]) -> dict:
    return {
        str(item.get("path") or item.get("name") or "<unknown>"): _status(item)
        for item in items
    }


def queued_jars(items: list[dict]) -> list[str]:
    return [
        str(item.get("path"))
        for item in items
        if _status(item) in QUEUED
        and str(item.get("path") or "").lower().endswith(".jar")
    ]


def failed_libraries(items: list[dict]) -> list[dict]:
    return [item for item in items if _status(item) in FAILED]


async def _stop_cluster(cluster_id: str) -> None:
    url = f"{lifecycle._cluster_url(cluster_id)}/actions/stop"
    response = None
    retry_waits = (0, 2, 4, 8, 16, 30)
    for wait_seconds in retry_waits:
        if wait_seconds:
            await asyncio.sleep(wait_seconds)
        response = requests.post(
            url,
            auth=lifecycle._get_signer(),
            json={},
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        if response.status_code != 409:
            break
    if response is None or response.status_code not in {200, 201, 202, 204}:
        raise RuntimeError(
            f"Failed to stop cluster {cluster_id}: HTTP "
            f"{response.status_code if response is not None else '(no response)'}: "
            f"{response.text[:500] if response is not None else ''}"
        )


async def _wait_for_state(
    cluster_id: str, desired: set[str], timeout: int, poll: int = 20
) -> str:
    elapsed = 0
    while elapsed <= timeout:
        state = lifecycle.get_cluster_state(cluster_id)
        if state in desired:
            return state
        if state in TERMINAL_CLUSTER_STATES:
            raise RuntimeError(f"Cluster {cluster_id} entered terminal state {state}")
        await asyncio.sleep(poll)
        elapsed += poll
    raise RuntimeError(
        f"Cluster {cluster_id} did not reach {sorted(desired)} within {timeout}s"
    )


async def _wait_for_libraries(cluster_id: str, timeout: int) -> list[dict]:
    elapsed = 0
    while elapsed <= timeout:
        items = lifecycle.get_installed_libraries(cluster_id)
        failed = failed_libraries(items)
        if failed:
            detail = [
                {
                    "path": item.get("path"),
                    "status": _status(item),
                    "message": item.get("stateMessage"),
                }
                for item in failed
            ]
            raise RuntimeError("Cluster library installation failed: " + json.dumps(detail))
        pending = [item for item in items if _status(item) in TRANSIENT | QUEUED]
        if not pending:
            return items
        await asyncio.sleep(15)
        elapsed += 15
    raise RuntimeError(
        f"Cluster libraries did not become ready within {timeout}s: "
        + json.dumps(summarize_libraries(lifecycle.get_installed_libraries(cluster_id)))
    )


def _configure_signer(config_path: str, profile: str) -> None:
    # Single auth source: oci_auth. An explicit --oci-config path is honored via
    # $OCI_CONFIG_FILE (the shared helper's documented file-override mechanism);
    # on-cluster the Credential Store wins before any file is consulted.
    import os
    from oci_auth import get_oci_signer as _shared
    if config_path:
        os.environ["OCI_CONFIG_FILE"] = config_path
    _, lifecycle._SIGNER = _shared(profile)


async def prepare(args: argparse.Namespace) -> dict:
    lifecycle.AIDP_BASE = args.aidp_base.rstrip("/")
    lifecycle.DATALAKE_OCID = args.lake_ocid
    lifecycle.WORKSPACE_ID = args.workspace_id
    lifecycle.OCI_PROFILE = args.oci_profile
    _configure_signer(args.oci_config_path, args.oci_profile)

    required_libraries = list(dict.fromkeys([
        *DEFAULT_COMPAT_LIBRARIES,
        *args.library,
    ]))

    current = lifecycle.get_installed_libraries(args.cluster)
    by_path = {str(item.get("path")): _status(item) for item in current}
    submitted = []
    for path in required_libraries:
        if by_path.get(path) not in HEALTHY | QUEUED | TRANSIENT:
            lifecycle.install_library(args.cluster, path)
            submitted.append(path)

    # Wait until all install requests reach a terminal state. Queued JARs are
    # accepted here because the following restart/start is what loads them.
    deadline = time.monotonic() + args.library_timeout
    while True:
        current = lifecycle.get_installed_libraries(args.cluster)
        failed = failed_libraries(current)
        if failed:
            raise RuntimeError(
                "Cluster library installation failed before restart: "
                + json.dumps([
                    {
                        "path": item.get("path"),
                        "status": _status(item),
                        "message": item.get("stateMessage"),
                    }
                    for item in failed
                ])
            )
        if not [item for item in current if _status(item) in TRANSIENT]:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("Timed out waiting for library install requests to settle")
        await asyncio.sleep(15)

    queued = queued_jars(current)
    state = lifecycle.get_cluster_state(args.cluster)
    restarted = False
    if queued and state != "ACTIVE":
        # Fresh clusters can finish resolving JARs while still CREATING.
        # Wait for initial activation before applying the required restart.
        await lifecycle.ensure_cluster_running(
            args.cluster, timeout=args.cluster_timeout
        )
        state = lifecycle.get_cluster_state(args.cluster)
    if queued and state == "ACTIVE":
        print(
            f"[validation-cluster] {len(queued)} JAR(s) require restart; stopping "
            f"{args.cluster[:12]}...",
            flush=True,
        )
        await _stop_cluster(args.cluster)
        await _wait_for_state(
            args.cluster, STARTABLE_CLUSTER_STATES, args.cluster_timeout
        )
        restarted = True

    await lifecycle.ensure_cluster_running(args.cluster, timeout=args.cluster_timeout)
    await lifecycle.ensure_aidp_compat_installed(
        args.cluster, timeout=args.library_timeout
    )
    ready = await _wait_for_libraries(args.cluster, args.library_timeout)

    missing = [
        path for path in required_libraries
        if summarize_libraries(ready).get(path) not in HEALTHY
    ]
    if missing:
        raise RuntimeError(f"Required validation libraries are not loaded: {missing}")

    return {
        "cluster": args.cluster,
        "state": lifecycle.get_cluster_state(args.cluster),
        "submitted": submitted,
        "restarted": restarted,
        "queued_jars_before_start": queued,
        "libraries": summarize_libraries(ready),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--aidp-base", required=True)
    parser.add_argument("--lake-ocid", required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--oci-profile", default="DEFAULT")
    parser.add_argument("--oci-config-path", default=None, help="Explicit OCI config file (laptop override). Optional since PR #234: on-cluster the AIDP Credential Store is used; on a laptop the standard ~/.oci/config resolution applies.")
    parser.add_argument("--library", action="append", default=[])
    parser.add_argument("--cluster-timeout", type=int, default=1800)
    parser.add_argument("--library-timeout", type=int, default=1200)
    return parser.parse_args()


def main() -> None:
    print(json.dumps(asyncio.run(prepare(parse_args())), indent=2))


if __name__ == "__main__":
    main()
