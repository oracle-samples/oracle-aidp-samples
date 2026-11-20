"""Run report + pre-flight write plan."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class NotebookResult:
    dbx_path: str
    aidp_path: str
    role: str               # "task" | "run_dep" | "py_sibling" | "resource"
    status: str             # "VALIDATED" | "PLACEHOLDER" | "SKIPPED_EXISTS" | "FAILED"
    placeholder_reason: Optional[str] = None
    placeholder_detail: Optional[str] = None
    dbx_fingerprint: Optional[str] = None
    aidp_fingerprint: Optional[str] = None
    cells: Optional[int] = None
    size_bytes: Optional[int] = None
    export_method: Optional[str] = None
    alias_of: Optional[str] = None
    referenced_by: List[str] = field(default_factory=list)


@dataclass
class FileResult:
    dbx_source: str
    aidp_path: str
    kind: str               # "jar" | "whl" | "init_script" | "requirements"
    status: str             # "VALIDATED" | "SKIPPED_EXISTS" | "SKIPPED_REPLACED" | "FAILED" | "EXTERNAL_RECORD_ONLY"
    sha256: Optional[str] = None
    size_bytes: Optional[int] = None
    failure_reason: Optional[str] = None


@dataclass
class JobResult:
    dbx_job_id: str
    dbx_job_name: str
    aidp_workflow_name: str
    aidp_workflow_key: Optional[str] = None
    status: str = "PENDING"  # "CREATED" | "SKIPPED_EXISTS" | "FAILED" | "PENDING"
    failure_reason: Optional[str] = None
    cluster_name: Optional[str] = None
    cluster_key: Optional[str] = None
    cluster_status: Optional[str] = None  # "CREATED" | "REUSED" | "FAILED"


@dataclass
class Report:
    run_id: str
    dbx_root_job_id: str
    export_output: str
    prefix: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    notebooks: List[NotebookResult] = field(default_factory=list)
    files: List[FileResult] = field(default_factory=list)
    jobs: List[JobResult] = field(default_factory=list)
    external_refs: List[str] = field(default_factory=list)
    skipped_loose_candidates: List[Dict[str, str]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    jar_discovery: Optional[Dict[str, Any]] = None
    pip_findings: List[Dict[str, Any]] = field(default_factory=list)
    resource_discovery_failures: List[Dict[str, str]] = field(default_factory=list)
    path_plan_errors: List[Dict[str, str]] = field(default_factory=list)
    _pip_finding_keys: set = field(default_factory=set, init=False, repr=False, compare=False)

    def add_pip_findings(self, notebook_path: str, cell_index: int, findings) -> None:
        """Record discovery gaps/notes, not failed installs. Cell indices are zero-based.

        Both discovery passes (and multiple cluster contexts) may scan a cell.
        Deduplicate by source location and finding, never by message alone.
        These WARN/INFO entries do not change the exporter's exit-code policy.
        """
        for status, reason in findings:
            key = (notebook_path, cell_index, status, reason)
            if key in self._pip_finding_keys:
                continue
            self._pip_finding_keys.add(key)
            self.pip_findings.append({
                "notebook_path": notebook_path, "cell_index": cell_index,
                "status": status, "reason": reason,
            })
            severity = "WARN" if status == "UNRESOLVED" else "INFO"
            self.errors.append(
                f"{severity} pip-discovery {status}: {notebook_path}:cell{cell_index}: {reason}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dbx_root_job_id": self.dbx_root_job_id,
            "export_output": self.export_output,
            "prefix": self.prefix,
            "timestamp": self.timestamp,
            "notebooks": [n.__dict__ for n in self.notebooks],
            "files": [f.__dict__ for f in self.files],
            "jobs": [j.__dict__ for j in self.jobs],
            "external_refs": self.external_refs,
            "skipped_loose_candidates": self.skipped_loose_candidates,
            "jar_discovery": self.jar_discovery,
            "pip_findings": self.pip_findings,
            "resource_discovery_failures": self.resource_discovery_failures,
            "path_plan_errors": self.path_plan_errors,
            "resource_discovery_complete": not self.resource_discovery_failures,
            "errors": self.errors,
            "summary": self.summary(),
        }

    def summary(self) -> Dict[str, int]:
        nb_status = {}
        for n in self.notebooks:
            nb_status[n.status] = nb_status.get(n.status, 0) + 1
        file_status = {}
        for f in self.files:
            file_status[f.status] = file_status.get(f.status, 0) + 1
        # Split report.errors by severity prefix — informational entries (shape
        # mapping decisions, etc.) use "INFO "/"WARN " prefixes and should NOT
        # inflate the errors count. Bare (no prefix) entries are real errors.
        _info  = sum(1 for e in self.errors if e.startswith("INFO "))
        _warn  = sum(1 for e in self.errors if e.startswith("WARN "))
        _errs  = len(self.errors) - _info - _warn
        return {
            "notebooks_total": len(self.notebooks),
            "notebooks_validated": nb_status.get("VALIDATED", 0),
            "notebooks_export_failed": nb_status.get("EXPORT_FAILED", 0),
            "notebooks_fingerprint_mismatch": nb_status.get("FINGERPRINT_MISMATCH", 0),
            # Retained from main: counts notebooks recorded with the legacy
            # FAILED status (superseded by EXPORT_FAILED but kept so nothing is lost).
            "notebooks_failed": nb_status.get("FAILED", 0),
            "notebooks_skipped_exists": nb_status.get("SKIPPED_EXISTS", 0),
            "task_notebooks_export_failed": sum(
                1 for n in self.notebooks if n.status == "EXPORT_FAILED" and n.role == "task"
            ),
            "run_dep_notebooks_export_failed": sum(
                1 for n in self.notebooks if n.status == "EXPORT_FAILED" and n.role == "run_dep"
            ),
            "local_resources_total": sum(
                1 for n in self.notebooks if n.role == "resource"
            ),
            "local_resources_failed": sum(
                1 for n in self.notebooks
                if n.role == "resource" and n.status in (
                    "EXPORT_FAILED", "FAILED", "FINGERPRINT_MISMATCH"
                )
            ),
            "resource_discovery_failures": len(self.resource_discovery_failures),
            "files_total": len(self.files),
            "files_validated": file_status.get("VALIDATED", 0),
            "files_failed": file_status.get("FAILED", 0),
            # Upload returned 200 but the read-back verification could not run.
            "files_unverified": file_status.get("UNVERIFIED", 0),
            "jobs_created": sum(1 for j in self.jobs if j.status == "CREATED"),
            "jobs_updated": sum(1 for j in self.jobs if j.status == "UPDATED"),
            "jobs_skipped": sum(1 for j in self.jobs if j.status == "SKIPPED_EXISTS"),
            "jobs_failed": sum(1 for j in self.jobs if j.status == "FAILED"),
            "jobs_soft_match_unresolved": sum(
                1 for j in self.jobs if j.status == "SKIPPED_SOFT_MATCH"),
            "external_refs": len(self.external_refs),
            "pip_unresolved": sum(f["status"] == "UNRESOLVED" for f in self.pip_findings),
            "pip_notes": sum(f["status"] == "NOTE" for f in self.pip_findings),
            "errors":   _errs,
            "warnings": _warn,
            "info":     _info,
        }

    def write(self, dir_path: str) -> str:
        os.makedirs(dir_path, exist_ok=True)
        path = os.path.join(dir_path, f"export_dbx_job_{self.dbx_root_job_id}_{self.run_id}.json")
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        return path


def render_pre_flight(
    *,
    export_output: str,
    workflow_name: str,
    cluster_summary: List[str],
    notebooks_real: int,
    notebooks_failed: List[Dict[str, str]],
    run_dep_real: int,
    run_dep_failed: List[Dict[str, str]],
    files_ok: int,
    files_failed: List[Dict[str, str]],
    child_jobs: List[str],
    external_refs: List[str],
    skipped_loose_candidates: Optional[List[Dict[str, str]]] = None,
    cluster_shape_mappings: Optional[List[str]] = None,
    cluster_totals: Optional[str] = None,
    workflow_refusal: Optional[str] = None,
    workflow_checks: Optional[List[str]] = None,
) -> str:
    """Render the human-readable pre-flight summary.

    Failed notebooks do NOT get a placeholder upload — the workflow task
    is still created with the canonical path, runtime task will fail with
    'notebook not found' if anyone tries to run it. We surface the count
    + reasons here so the user knows what's missing.

    ``cluster_shape_mappings`` (optional) lists per-cluster driver/worker
    DBX→AIDP mapping decisions computed up-front so a --dry-run operator can
    verify them BEFORE any AIDP cluster is created. Each entry is already
    formatted (comes from cluster_converter.format_mapping_log_line).

    ``cluster_totals`` (optional) is one pre-rendered line summing the compute
    the export would provision across ALL jobs (root + recursed children).
    Without it the operator has to add up per-cluster autoscale floors by hand,
    which is how a 2.5x over-commitment went unnoticed.
    """
    lines = []
    lines.append("=== PRE-FLIGHT SUMMARY ===")
    lines.append(f"Target base:        {export_output}")
    workflow_status = "WILL REFUSE" if workflow_refusal else "WILL CREATE"
    lines.append(f"Workflow:           {workflow_name}      [{workflow_status}]")
    if workflow_refusal:
        lines.append(f"                      - {workflow_refusal}")
    for check in workflow_checks or []:
        lines.append(f"Workflow check:     {check}")
    for c in cluster_summary:
        lines.append(f"Cluster:            {c}")
    if cluster_totals:
        lines.append(f"Cluster totals:     {cluster_totals}")
    # Shape mapping decisions (driver/worker per cluster). Empty on legacy
    # code paths that don't compute them; kept out of the summary if so.
    if cluster_shape_mappings:
        lines.append("")
        lines.append("Cluster shape mapping (DBX → AIDP):")
        for _line in cluster_shape_mappings:
            lines.append(f"  {_line}")
    lines.append("")
    lines.append(f"Task notebooks:     {notebooks_real} OK, {len(notebooks_failed)} export-failed (no upload, task path still set)")
    for ph in notebooks_failed:
        lines.append(f"                      - {ph['dbx_path']}  ({ph['reason']})")
    lines.append(f"%run dep notebooks: {run_dep_real} OK, {len(run_dep_failed)} export-failed")
    for ph in run_dep_failed[:10]:
        lines.append(f"                      - {ph['dbx_path']}  ({ph['reason']})")
    if len(run_dep_failed) > 10:
        lines.append(f"                      … and {len(run_dep_failed) - 10} more")
    if skipped_loose_candidates:
        lines.append(f"Loose candidates:   {len(skipped_loose_candidates)} absent, skipped with warning")
        for candidate in skipped_loose_candidates[:10]:
            lines.append(
                f"                      - {candidate['path']} "
                f"(from {candidate['referenced_by']}; {candidate['reason']})"
            )
        if len(skipped_loose_candidates) > 10:
            lines.append(f"                      … and {len(skipped_loose_candidates) - 10} more")
    lines.append(f"Files (jar/whl/init): {files_ok} OK, {len(files_failed)} FAILED")
    for ff in files_failed:
        lines.append(f"                      - {ff['dbx_source']}  ({ff['reason']})")
    if child_jobs:
        lines.append(f"Recursed child jobs: {len(child_jobs)}  ({', '.join(child_jobs[:5])}{'…' if len(child_jobs) > 5 else ''})")
    if external_refs:
        lines.append(f"External refs (record-only): {len(external_refs)}")
        for r in external_refs[:5]:
            lines.append(f"                      - {r}")
    lines.append("==========================")
    return "\n".join(lines)
