"""Publish the finished report into the AIDP workspace.

A run's record lived only on the laptop that drove it. This copies the
inputs and outputs into the migration's own workspace folder, next to the
plan and scripts `provision` put there, so it can be read later from AIDP
without the operator's machine.

It adds no transport. Files go through the same workspace-object operations
`provision` uses -- `create_ws_folder`, `upload_ws_file`, `list_ws_objects` --
and each one is read back in a listing before it is called published: a 2xx
on the upload is not the claim.

Never published: the connection config and anything named like a key or
credential. The artifacts are what the run wrote; the secrets that let it
run stay where they are.
"""
from __future__ import annotations

import datetime
import pathlib
import re

from .provisioning import _ROOT

__all__ = ["publishable_files", "publish_report", "FINAL_FOLDER_PREFIX"]

FINAL_FOLDER_PREFIX = f"{_ROOT}/reports/final-"

_SUFFIXES = (".json", ".jsonl", ".md", ".mmd")
_NEVER = re.compile(
    r"(snowmig-config|secret|password|passphrase|token\.|private|\.p8$|\.pem$"
    r"|\.key$)", re.IGNORECASE)
_HOUSEKEEPING = {"README.md", ".gitignore"}


def publishable_files(out_dir) -> list[pathlib.Path]:
    """The run's top-level artifacts, minus housekeeping and anything that
    could carry a credential."""
    out = []
    for path in sorted(pathlib.Path(out_dir).iterdir()):
        if not path.is_file() or path.name in _HOUSEKEEPING:
            continue
        if _NEVER.search(path.name):
            continue
        if path.suffix.lower() in _SUFFIXES:
            out.append(path)
    return out


def publish_report(call, workspace: str, out_dir, *, execute: bool,
                   stamp: str | None = None) -> dict:
    stamp = stamp or datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    folder = f"{FINAL_FOLDER_PREFIX}{stamp}"
    files = publishable_files(out_dir)
    steps: list[dict] = []

    if not execute:
        steps = [{"file": p.name, "remote": f"{folder}/{p.name}",
                  "action": "would upload", "verified": None} for p in files]
        return {"dry_run": True, "workspace": workspace, "folder": folder,
                "steps": steps, "verified": 0}

    try:
        call("create_ws_folder", workspace=workspace, path=folder)
    except Exception as exc:
        # Existence is decided by the per-file read-back, not by this.
        steps.append({"file": None, "remote": folder,
                      "action": "folder_create_failed_or_exists",
                      "verified": None, "detail": str(exc)[:200]})

    for path in files:
        remote = f"{folder}/{path.name}"
        try:
            call("upload_ws_file", workspace=workspace, path=remote,
                 local_path=str(path))
            items = call("list_ws_objects", workspace=workspace,
                         path=folder).get("items") or []
            seen = any(str(i.get("path") or "").endswith("/" + path.name)
                       or i.get("displayName") == path.name for i in items)
            steps.append({"file": path.name, "remote": remote,
                          "action": "uploaded" if seen else "upload_requested",
                          "verified": seen})
        except Exception as exc:
            steps.append({"file": path.name, "remote": remote,
                          "action": "failed", "verified": False,
                          "detail": str(exc)[:200]})

    file_steps = [s for s in steps if s["file"]]
    return {"dry_run": False, "workspace": workspace, "folder": folder,
            "steps": steps,
            "verified": sum(1 for s in file_steps if s["verified"]),
            "not_verified": [s["file"] for s in file_steps
                             if not s["verified"]]}
