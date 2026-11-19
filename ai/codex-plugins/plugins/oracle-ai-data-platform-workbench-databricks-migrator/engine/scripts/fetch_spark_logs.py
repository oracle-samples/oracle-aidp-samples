#!/usr/bin/env python3
"""
Fetch Spark driver logs from AIDP cluster.
Useful for debugging errors that don't show up in the WebSocket output.

Usage:
    python3 fetch_spark_logs.py --last 60  # last 60 minutes
    python3 fetch_spark_logs.py --level error --last 30
"""

import json
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone

import oci
import requests

AIDP_BASE = "https://aidp.<OCI_REGION>.oci.oraclecloud.com/20240831"
DATALAKE_OCID = os.environ.get("AIDP_DATALAKE", "")
WORKSPACE_ID = os.environ.get("AIDP_WORKSPACE", "")
DEFAULT_CLUSTER = os.environ.get("AIDP_CLUSTER", "")
OCI_PROFILE = os.environ.get("OCI_PROFILE", "DEFAULT")


def get_signer():
    config = oci.config.from_file(profile_name=OCI_PROFILE)
    return oci.signer.Signer(
        tenancy=config["tenancy"], user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
    )


def fetch_logs(cluster_id: str = DEFAULT_CLUSTER,
               minutes_back: int = 60,
               log_level: str = "error",
               subject: str = "spark-driver",
               content_type: str = "driver") -> list:
    """Fetch Spark logs from AIDP cluster."""
    url = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
           f"/clusters/{cluster_id}/actions/searchLogs")

    now = datetime.now(timezone.utc)
    time_begin = (now - timedelta(minutes=minutes_back)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    time_end = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    body = {
        "timeBegin": time_begin,
        "timeEnd": time_end,
        "logContentTypeContains": content_type,
        "subjectContains": subject,
        "logLevel": log_level,
    }

    signer = get_signer()
    resp = requests.post(url, json=body, auth=signer,
                         headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    return resp.json()


def extract_python_errors(logs: dict) -> list:
    """Extract just the Python error messages from the verbose Java stack traces."""
    errors = []
    items = logs.get("items", logs) if isinstance(logs, dict) else logs

    if isinstance(items, list):
        for item in items:
            msg = item.get("message", "") if isinstance(item, dict) else str(item)
            # Extract the Python traceback part
            if "Traceback" in msg or "Error" in msg:
                # Find the actual Python error line
                lines = msg.split("\n")
                for line in lines:
                    if any(err in line for err in ["Error:", "Exception:", "NameError", "TypeError", "ModuleNotFoundError", "FileNotFoundError", "RuntimeError"]):
                        clean = line.strip().replace("[0;31m", "").replace("[0m", "")
                        if clean and clean not in errors:
                            errors.append(clean)
    return errors


def main():
    parser = argparse.ArgumentParser(description="Fetch AIDP Spark logs")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--last", type=int, default=60, help="Minutes to look back")
    parser.add_argument("--level", default="error", choices=["error", "warn", "info"])
    parser.add_argument("--raw", action="store_true", help="Show raw JSON")
    parser.add_argument("--summary", action="store_true", help="Show unique errors only")
    args = parser.parse_args()

    print(f"Fetching {args.level} logs from last {args.last} minutes...")
    logs = fetch_logs(args.cluster, args.last, args.level)

    if args.raw:
        print(json.dumps(logs, indent=2))
    elif args.summary:
        errors = extract_python_errors(logs)
        print(f"\nUnique errors ({len(errors)}):")
        for e in errors:
            print(f"  {e}")
    else:
        items = logs.get("items", []) if isinstance(logs, dict) else logs
        print(f"Log entries: {len(items) if isinstance(items, list) else '?'}")
        if isinstance(items, list):
            for item in items[:20]:
                if isinstance(item, dict):
                    ts = item.get("timestamp", "")
                    msg = item.get("message", "")[:200]
                    print(f"  [{ts}] {msg}")


if __name__ == "__main__":
    main()
