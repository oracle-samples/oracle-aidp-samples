#!/usr/bin/env python3
"""Create, refresh, and deploy Oracle AI Data Platform Workbench bundles.

The program uses ``oci raw-request`` so it works with an existing OCI CLI
profile; it does not require an AIDP-specific SDK package.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote


# Keep these generic. A customer supplies values with flags or environment
# variables; see migration.env.template and CUSTOMER_MIGRATION_GUIDE.md.
DEFAULT_TENANCY = os.environ.get("AIDP_TENANCY_OCID", "")
DEFAULT_AIDP_ID = os.environ.get("AIDP_WORKBENCH_OCID", "")
DEFAULT_PROFILE = os.environ.get("OCI_CLI_PROFILE", "DEFAULT")
DEFAULT_REGION = os.environ.get("OCI_REGION", "")
DEFAULT_AUTH = os.environ.get("OCI_CLI_AUTH", "security_token")
DEFAULT_EXPORT_DIR = os.environ.get("AIDP_EXPORT_DIR", "./aidp-exports")
API_VERSION = "20260430"
BUNDLE_PATH_RE = re.compile(r"^/Workspace(?:/(?!\.{1,2}(?:/|$))[^/\\\r\n]+)*/?$")
BUNDLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


class AidpApiError(RuntimeError):
    """An unsuccessful AIDP REST operation."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export/import AIDP Workbench jobs and agent flows as a portable bundle."
    )
    parser.add_argument("--tenancy", default=DEFAULT_TENANCY, help="OCI tenancy OCID (or AIDP_TENANCY_OCID).")
    parser.add_argument("--aidp-id", default=DEFAULT_AIDP_ID, help="AI Data Platform Workbench OCID (or AIDP_WORKBENCH_OCID).")
    parser.add_argument("--profile", default=DEFAULT_PROFILE, help="OCI CLI profile.")
    parser.add_argument("--region", default=DEFAULT_REGION, help="OCI region.")
    parser.add_argument("--auth", default=DEFAULT_AUTH,
                        help="OCI CLI auth mode (default: security_token).")
    parser.add_argument("--no-wait", action="store_true", help="Do not wait for an asynchronous operation to finish.")

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List workspaces and external catalogs.")

    export = commands.add_parser("export", help="Create a bundle from selected jobs and agent flows.")
    export.add_argument("--workspace-key", required=True)
    export.add_argument("--name", required=True, help="Bundle folder name: letters, digits, underscore.")
    export.add_argument("--path", default="/Workspace/aidp_bundles", help="Parent workspace folder for the bundle.")
    export.add_argument("--description", default="Created by aidp_workspace_bundle.py")
    export.add_argument("--resource", action="append", default=[], metavar="TYPE:KEY",
                        help="Resource to include; TYPE is JOB or AGENTFLOW. Repeat as needed.")
    export.add_argument("--all-jobs", action="store_true", help="Include every job in the source workspace.")

    sync = commands.add_parser("sync", help="Refresh an existing source bundle from its recorded origins.")
    sync.add_argument("--workspace-key", required=True)
    sync.add_argument("--path", required=True, help="Bundle root path, e.g. /Workspace/aidp_bundles/my_bundle.")

    deploy = commands.add_parser("import", help="Deploy an existing bundle into a target workspace.")
    deploy.add_argument("--workspace-key", required=True, help="Target workspace key.")
    deploy.add_argument("--path", required=True, help="Bundle root path in the target workspace volume.")
    deploy.add_argument("--confirm", action="store_true", help="Required acknowledgement because deploy creates/updates resources.")

    archive = commands.add_parser("archive", help="Create a read-only metadata and file archive of one or all workspaces.")
    archive.add_argument("--workspace-key", action="append", default=[],
                         help="Workspace key to archive. Repeat to select several; default is all workspaces.")
    archive.add_argument("--output-dir", default=os.environ.get("AIDP_EXPORT_DIR", DEFAULT_EXPORT_DIR),
                         help="Local archive parent directory (or set AIDP_EXPORT_DIR).")
    archive.add_argument("--metadata-only", action="store_true",
                         help="Do not download workspace file contents; still exports all available metadata.")
    return parser.parse_args()


def api_base(args: argparse.Namespace) -> str:
    if not args.aidp_id or not args.region:
        raise AidpApiError("Set --aidp-id and --region, or AIDP_WORKBENCH_OCID and OCI_REGION.")
    return f"https://aidp.{args.region}.oci.oraclecloud.com/{API_VERSION}/aiDataPlatforms/{args.aidp_id}"


def raw_request(
    args: argparse.Namespace,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    request_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    command = ["oci", "raw-request", "--profile", args.profile, "--region", args.region,
               "--http-method", method, "--target-uri", f"{api_base(args)}{path}",
               "--opc-request-id", "aidp-bundle-" + uuid.uuid4().hex]
    if args.auth:
        command.extend(["--auth", args.auth])

    if request_headers:
        command.extend(["--request-headers", json.dumps(request_headers)])

    body_file: Path | None = None
    try:
        if body is not None:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as handle:
                json.dump(body, handle)
                body_file = Path(handle.name)
            command.extend(["--request-body", f"file://{body_file}"])
        result = subprocess.run(command, text=True, capture_output=True, check=False)
    finally:
        if body_file:
            body_file.unlink(missing_ok=True)

    if result.returncode and not result.stdout:
        raise AidpApiError(result.stderr.strip() or "OCI CLI raw-request failed.")
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AidpApiError(result.stderr.strip() or result.stdout.strip()) from exc

    status = int(str(response.get("status", "0")).split()[0])
    if status < 200 or status >= 300:
        error = response.get("data", {})
        message = error.get("message") if isinstance(error, dict) else str(error)
        raise AidpApiError(f"{response.get('status', 'request failed')}: {message}")
    return response


def items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        values = payload.get("items", payload.get("data", []))
        return values if isinstance(values, list) else []
    return payload if isinstance(payload, list) else []


def key(resource: dict[str, Any], *names: str) -> str | None:
    for name in names:
        value = resource.get(name)
        if value:
            return str(value)
    return None


def operation_key(response: dict[str, Any]) -> str | None:
    headers = response.get("headers", {})
    if not isinstance(headers, dict):
        return None
    for name, value in headers.items():
        if name.lower() == "aidp-async-operation-key":
            return str(value)
    return None


def wait_for_operation(args: argparse.Namespace, response: dict[str, Any]) -> None:
    async_key = operation_key(response)
    if not async_key:
        print("Request accepted; no async-operation header was returned.")
        return
    print(f"Async operation: {async_key}")
    if args.no_wait:
        return
    for _ in range(120):
        time.sleep(3)
        payload = raw_request(args, "GET", f"/asyncOperations/{async_key}").get("data", {})
        status = str(payload.get("status", payload.get("lifecycleState", ""))).upper()
        print(f"  {status or 'IN_PROGRESS'}")
        if status in {"SUCCEEDED", "SUCCESS", "COMPLETED"}:
            return
        if status in {"FAILED", "CANCELED", "CANCELLED"}:
            raise AidpApiError(f"Async operation {async_key} ended as {status}: {json.dumps(payload)}")
    raise AidpApiError(f"Timed out waiting for async operation {async_key}.")


def validate_path(path: str) -> None:
    if not BUNDLE_PATH_RE.fullmatch(path):
        raise AidpApiError("Bundle path must start with /Workspace and cannot contain '.' or '..' path components.")


def parse_resource(value: str) -> dict[str, str]:
    try:
        resource_type, resource_key = value.split(":", 1)
    except ValueError as exc:
        raise AidpApiError(f"Invalid resource '{value}'; use JOB:key or AGENTFLOW:key.") from exc
    resource_type = resource_type.upper()
    if resource_type not in {"JOB", "AGENTFLOW"} or not resource_key:
        raise AidpApiError(f"Invalid resource '{value}'; type must be JOB or AGENTFLOW.")
    return {"resourceType": resource_type, "resourceKey": resource_key}


def list_resources(args: argparse.Namespace) -> None:
    workspaces = raw_request(args, "GET", "/workspaces").get("data", {})
    catalogs = raw_request(args, "GET", "/catalogs").get("data", {})
    print(json.dumps({"workspaces": items(workspaces), "externalCatalogs": items(catalogs)}, indent=2))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def safe_file_path(root: Path, workspace_path: str) -> Path:
    relative = workspace_path.lstrip("/")
    candidate = (root / relative).resolve()
    if root.resolve() not in candidate.parents and candidate != root.resolve():
        raise AidpApiError(f"Refusing unsafe workspace path: {workspace_path}")
    return candidate


def is_folder(item: dict[str, Any]) -> bool:
    return str(item.get("type", "")).upper() in {"FOLDER", "DIRECTORY"}


def get_workspace_objects(args: argparse.Namespace, workspace_key: str, folder: str = "/") -> list[dict[str, Any]]:
    path = f"/workspaces/{workspace_key}/objects?path={quote(folder, safe='')}&limit=1000"
    response = raw_request(args, "GET", path).get("data", {})
    return items(response)


def download_workspace_file(args: argparse.Namespace, workspace_key: str, item: dict[str, Any], destination: Path) -> dict[str, Any]:
    workspace_path = str(item["path"])
    metadata = raw_request(
        args,
        "POST",
        f"/workspaces/{workspace_key}/actions/downloadFileMeta?shouldGenerateNewPar=true",
        request_headers={"path": workspace_path, "type": str(item.get("type", "FILE"))},
    ).get("data", {})
    par_url = metadata.get("parUrl")
    if not par_url:
        raise AidpApiError(f"No download PAR returned for {workspace_path}.")
    target = safe_file_path(destination, workspace_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(str(par_url), timeout=120) as source, target.open("wb") as output:
        while chunk := source.read(1024 * 1024):
            output.write(chunk)
    return {"path": workspace_path, "localPath": str(target.relative_to(destination)), "size": metadata.get("size")}


def archive_workspace(args: argparse.Namespace, workspace_key: str, root: Path, download_files: bool) -> dict[str, Any]:
    workspace_root = root / "workspaces" / workspace_key
    workspace_root.mkdir(parents=True, exist_ok=True)
    endpoints = {
        "workspace.json": f"/workspaces/{workspace_key}",
        "permissions.json": f"/workspaces/{workspace_key}/permissions",
        "clusters.json": f"/workspaces/{workspace_key}/clusters",
        "jobs.json": f"/workspaces/{workspace_key}/jobs",
    }
    for filename, endpoint in endpoints.items():
        write_json(workspace_root / "metadata" / filename, raw_request(args, "GET", endpoint).get("data", {}))

    clusters = items(json.loads((workspace_root / "metadata" / "clusters.json").read_text(encoding="utf-8")))
    for cluster in clusters:
        cluster_key = key(cluster, "clusterKey", "key", "id")
        if cluster_key:
            write_json(workspace_root / "clusters" / f"{cluster_key}.json", raw_request(args, "GET", f"/workspaces/{workspace_key}/clusters/{cluster_key}").get("data", {}))
            write_json(workspace_root / "clusters" / f"{cluster_key}.libraries.json", raw_request(args, "GET", f"/workspaces/{workspace_key}/clusters/{cluster_key}/libraries").get("data", {}))

    jobs = items(json.loads((workspace_root / "metadata" / "jobs.json").read_text(encoding="utf-8")))
    for job in jobs:
        job_key = key(job, "jobKey", "key", "id")
        if job_key:
            write_json(workspace_root / "jobs" / f"{job_key}.json", raw_request(args, "GET", f"/workspaces/{workspace_key}/jobs/{job_key}").get("data", {}))

    object_queue = ["/"]
    seen_folders: set[str] = set()
    all_objects: list[dict[str, Any]] = []
    downloaded: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    while object_queue:
        folder = object_queue.pop()
        if folder in seen_folders:
            continue
        seen_folders.add(folder)
        for item in get_workspace_objects(args, workspace_key, folder):
            all_objects.append(item)
            item_path = str(item.get("path", ""))
            if is_folder(item) and item_path and item_path not in seen_folders:
                object_queue.append(item_path)
            elif download_files and item_path:
                try:
                    downloaded.append(download_workspace_file(args, workspace_key, item, workspace_root / "files"))
                except (AidpApiError, OSError) as exc:
                    failures.append({"path": item_path, "error": str(exc)})
    write_json(workspace_root / "metadata" / "workspace_objects.json", all_objects)
    return {"workspaceKey": workspace_key, "objects": len(all_objects), "filesDownloaded": len(downloaded), "fileDownloadFailures": failures}


def archive(args: argparse.Namespace) -> None:
    export_root = Path(args.output_dir).expanduser() / datetime.now(timezone.utc).strftime("aidp-export-%Y%m%dT%H%M%SZ")
    export_root.mkdir(parents=True, exist_ok=False)
    workspaces = items(raw_request(args, "GET", "/workspaces").get("data", {}))
    catalogs = raw_request(args, "GET", "/catalogs").get("data", {})
    selected = set(args.workspace_key)
    available = {key(workspace, "workspaceKey", "key", "id") for workspace in workspaces}
    unknown = selected - available
    if unknown:
        raise AidpApiError(f"Unknown workspace key(s): {', '.join(sorted(unknown))}")
    selected_workspaces = [workspace for workspace in workspaces if not selected or key(workspace, "workspaceKey", "key", "id") in selected]
    write_json(export_root / "metadata" / "workspaces.json", workspaces)
    write_json(export_root / "metadata" / "catalogs.json", catalogs)
    summary = [archive_workspace(args, str(key(workspace, "workspaceKey", "key", "id")), export_root, not args.metadata_only) for workspace in selected_workspaces]
    write_json(export_root / "manifest.json", {
        "schemaVersion": "aidp.full-archive.v1",
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "tenancyOcid": args.tenancy,
        "aiDataPlatformOcid": args.aidp_id,
        "region": args.region,
        "profile": args.profile,
        "scope": "metadata and workspace files" if not args.metadata_only else "metadata only",
        "notExported": ["catalog credential secrets", "compute runtime state", "notebook sessions", "job run history", "underlying catalog data"],
        "workspaces": summary,
    })
    print(f"Archive created: {export_root}")


def export_bundle(args: argparse.Namespace) -> None:
    validate_path(args.path)
    if not BUNDLE_NAME_RE.fullmatch(args.name):
        raise AidpApiError("Bundle name may contain only letters, digits, and underscores.")
    resources = [parse_resource(value) for value in args.resource]
    if args.all_jobs:
        jobs_response = raw_request(args, "GET", f"/workspaces/{args.workspace_key}/jobs").get("data", {})
        resources.extend(
            {"resourceType": "JOB", "resourceKey": job_key}
            for job in items(jobs_response)
            if (job_key := key(job, "jobKey", "key", "id"))
        )
    resources = list({(r["resourceType"], r["resourceKey"]): r for r in resources}.values())
    if not resources:
        raise AidpApiError("Select at least one --resource or use --all-jobs.")
    response = raw_request(args, "POST", f"/workspaces/{args.workspace_key}/bundles", {
        "name": args.name,
        "path": args.path,
        "description": args.description,
        "bundledResources": resources,
    })
    print(f"Bundle creation requested at {args.path.rstrip('/')}/{args.name}.")
    wait_for_operation(args, response)


def sync_bundle(args: argparse.Namespace) -> None:
    validate_path(args.path)
    response = raw_request(args, "POST", f"/workspaces/{args.workspace_key}/bundles/actions/sync", {"path": args.path})
    print(f"Bundle sync requested for {args.path}.")
    wait_for_operation(args, response)


def deploy_bundle(args: argparse.Namespace) -> None:
    validate_path(args.path)
    if not args.confirm:
        raise AidpApiError("Import can create or update jobs and agent flows. Re-run with --confirm to execute it.")
    response = raw_request(args, "POST", f"/workspaces/{args.workspace_key}/bundles/actions/deploy", {"path": args.path})
    print(f"Bundle deployment requested to workspace {args.workspace_key} from {args.path}.")
    wait_for_operation(args, response)


def main() -> int:
    args = parse_args()
    try:
        if args.command == "list":
            list_resources(args)
        elif args.command == "export":
            export_bundle(args)
        elif args.command == "sync":
            sync_bundle(args)
        elif args.command == "archive":
            archive(args)
        else:
            deploy_bundle(args)
    except AidpApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
