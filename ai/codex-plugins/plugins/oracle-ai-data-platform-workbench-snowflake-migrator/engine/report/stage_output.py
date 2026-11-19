"""The accumulated run report, written to the workspace after every stage.

With `reporting.publish_each_stage: true`, every logged stage ends by
uploading, into `reporting.workspace_dir` (default `report/output`) of the
migration workspace provision created:

  * the CUMULATIVE reports -- tokens.json, TOKENS.md, phase_report.json,
    PHASES.md, run_log.jsonl -- overwritten each time, so the folder always
    holds the run so far;
  * one SNAPSHOT for the stage that just ended, named by its position in
    the run and its runbook step (`016_S06_discover-workflow.json`): its
    window, exit code, the tokens spent on it, and the cumulative totals.

Uploads go through the same workspace-object calls `provision` and `publish`
use, and each is read back. Nothing here ever fails the stage: an upload
problem is reported, not raised.
"""
from __future__ import annotations

import json
import pathlib
import re
import tempfile

from .render import render_phase_report
from .stages import STAGES, phase_report
from .tokens import build_token_report, read_stage_log, render_tokens

__all__ = ["publish_stage_output", "snapshot_name"]

_CUMULATIVE = ("tokens.json", "TOKENS.md", "phase_report.json", "PHASES.md",
               "run_log.jsonl")


def snapshot_name(seq: int, stage: str, runbook: str) -> str:
    steps = [f"S{int(n):02d}" for n in re.findall(r"S(\d+)", runbook or "")]
    if len(steps) == 2 and "-" in (runbook or "") and " " not in runbook:
        tag = f"{steps[0]}-{steps[1]}"
    else:
        tag = "-".join(steps)
    return f"{seq:03d}_{tag + '_' if tag else ''}{stage}.json"


def _runbook(stage: str) -> str:
    return next((s["runbook"] for s in STAGES if s["stage"] == stage), "-")


def publish_stage_output(call, workspace: str, out_dir, folder: str) -> dict:
    out = pathlib.Path(out_dir)
    runs = read_stage_log(out)
    last = runs[-1] if runs else {}

    tokens = build_token_report(out)
    (out / "tokens.json").write_text(json.dumps(tokens, indent=2, default=str))
    (out / "TOKENS.md").write_text(render_tokens(tokens))
    phases = phase_report(out)
    (out / "phase_report.json").write_text(json.dumps(phases, indent=2))
    (out / "PHASES.md").write_text(render_phase_report(phases))

    stage = last.get("stage", "unknown")
    this_run = next((r for r in reversed(tokens.get("runs") or [])
                     if r.get("started_at") == last.get("started_at")), {})
    snapshot = {
        "stage": stage, "phase": last.get("phase"),
        "runbook": _runbook(stage), "started_at": last.get("started_at"),
        "ended_at": last.get("ended_at"), "exit_code": last.get("exit_code"),
        "retries": last.get("retries", 0),
        "tokens": this_run.get("tokens") if tokens.get("measured") else None,
        "cumulative_tokens": tokens.get("totals") if tokens.get("measured")
        else None,
        "resources_allocated_by_this_stage": [
            r for r in phases.get("resources", {}).get("resources") or []
            if r.get("allocated_by") == stage],
        "resources_accruing_now": phases.get("resources", {}).get(
            "accruing_now") or [],
        # Per run: a report can be measured while THIS stage's session had
        # no transcript read -- its tokens are then None, not zero.
        "tokens_measured": bool(tokens.get("measured")
                                and this_run.get("measured", True)),
        "tokens_note": (tokens.get("reason") if not tokens.get("measured")
                        else this_run.get("unmeasured_reason")),
        "tokens_partial": bool(tokens.get("partial")),
    }
    name = snapshot_name(len(runs), stage, snapshot["runbook"])

    steps: list[dict] = []
    try:
        call("create_ws_folder", workspace=workspace, path=folder)
    except Exception as exc:
        steps.append({"file": None, "verified": None,
                      "detail": f"folder: {str(exc)[:160]}"})

    with tempfile.TemporaryDirectory(prefix="snowmig_stage_out_") as tmp:
        snap_path = pathlib.Path(tmp) / name
        snap_path.write_text(json.dumps(snapshot, indent=2, default=str))
        files = [out / f for f in _CUMULATIVE if (out / f).is_file()]
        files.append(snap_path)
        for path in files:
            remote = f"{folder}/{path.name}"
            try:
                call("upload_ws_file", workspace=workspace, path=remote,
                     local_path=str(path))
                items = call("list_ws_objects", workspace=workspace,
                             path=folder).get("items") or []
                seen = any(str(i.get("path") or "").endswith("/" + path.name)
                           or i.get("displayName") == path.name for i in items)
                steps.append({"file": path.name, "verified": seen})
            except Exception as exc:
                steps.append({"file": path.name, "verified": False,
                              "detail": str(exc)[:160]})

    file_steps = [s for s in steps if s["file"]]
    return {"folder": folder, "snapshot": name, "steps": steps,
            "verified": sum(1 for s in file_steps if s["verified"]),
            "not_verified": [s["file"] for s in file_steps
                             if not s["verified"]]}
