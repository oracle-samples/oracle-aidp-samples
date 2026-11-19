"""Combine per-source scans into one manifest + a top-line summary report."""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Callable

from aws_aidp.aws_client import AwsClient, check_creds
from aws_aidp.inventory import s3 as s3_mod
from aws_aidp.inventory import glue as glue_mod
from aws_aidp.inventory import athena as athena_mod
from aws_aidp.inventory import emr as emr_mod
from aws_aidp.inventory import sagemaker as sm_mod

ALL_SOURCES = ("s3", "glue", "athena", "emr", "sagemaker")

_SCANNERS: dict[str, Callable[[AwsClient], dict]] = {
    "s3":        s3_mod.scan,
    "glue":      glue_mod.scan,
    "athena":    athena_mod.scan,
    "emr":       emr_mod.scan,
    "sagemaker": sm_mod.scan,
}


def build_manifest(
    client: AwsClient,
    sources: tuple[str, ...] = ALL_SOURCES,
    *,
    log: Callable[[str], None] | None = None,
) -> dict:
    normalized_sources = tuple(dict.fromkeys(sources))
    unknown = [source for source in normalized_sources if source not in _SCANNERS]
    if not normalized_sources or unknown:
        detail = f"unsupported source(s): {unknown}" if unknown else "source list is empty"
        raise ValueError(f"{detail}; valid sources: {ALL_SOURCES}")
    account_id = check_creds(client.cfg)
    if log:
        log(f"account={account_id} region={client.cfg.region}")
    sources_data: dict[str, dict] = {}
    for src in normalized_sources:
        if log:
            log(f"  scanning {src}...")
        try:
            sources_data[src] = _SCANNERS[src](client)
        except Exception as e:
            sources_data[src] = {"summary": {"error": str(e)}, "items": {}}
            if log:
                log(f"  {src}: failed — {e}")
    return {
        "account_id": account_id,
        "region": client.cfg.region,
        "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources_scanned": list(normalized_sources),
        "sources": sources_data,
    }


def write_manifest(manifest: dict, out_path: str | Path) -> Path:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    temp = p.with_name(f".{p.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("w", encoding="utf-8", newline="") as stream:
            stream.write(json.dumps(manifest, indent=2, default=str))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, p)
        # Persist the directory entry where the platform supports directory fsync.
        try:
            directory_fd = os.open(p.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                try:
                    os.fsync(directory_fd)
                except OSError:
                    # Directory fsync is unavailable on some platforms/filesystems.
                    pass
            finally:
                os.close(directory_fd)
    finally:
        temp.unlink(missing_ok=True)
    return p


def summarize(manifest: dict) -> str:
    """One-screen summary string for the user."""
    lines = [
        f"AWS account: {manifest['account_id']}  region: {manifest['region']}",
        f"scanned at:  {manifest['scanned_at']}",
        "",
    ]
    for src, data in manifest.get("sources", {}).items():
        s = data.get("summary", {})
        if "error" in s:
            lines.append(f"  {src:10s}  ERROR: {s['error']}")
            continue
        kv = ", ".join(f"{k}={v}" for k, v in s.items() if not isinstance(v, dict))
        warning_count = len(data.get("warnings", []))
        if warning_count:
            kv = f"{kv}, warnings={warning_count}" if kv else f"warnings={warning_count}"
        lines.append(f"  {src:10s}  {kv}")
    return "\n".join(lines)
