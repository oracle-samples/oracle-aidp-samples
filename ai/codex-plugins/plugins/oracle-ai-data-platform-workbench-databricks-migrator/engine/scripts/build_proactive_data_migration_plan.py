#!/usr/bin/env python3
"""Build an availability-independent migration requirement plan.

Unlike the live migration tracker loader, this planner answers: "What data
would this workflow require if the target environment were empty?" Current
availability is retained as evidence but never suppresses an external input.
Workflow-produced outputs and notebook-local/temp table names are excluded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping


DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
LOWER_RE = re.compile(r"(?P<column>[A-Za-z_]\w*)\s*>=?\s*['\"](?P<date>\d{4}-\d{2}-\d{2})['\"]", re.I)
UPPER_RE = re.compile(r"(?P<column>[A-Za-z_]\w*)\s*<=?\s*['\"](?P<date>\d{4}-\d{2}-\d{2})['\"]", re.I)
BETWEEN_RE = re.compile(
    r"(?P<column>[A-Za-z_]\w*)\s+BETWEEN\s+['\"](?P<start>\d{4}-\d{2}-\d{2})['\"]"
    r"\s+AND\s+['\"](?P<end>\d{4}-\d{2}-\d{2})['\"]",
    re.I,
)
EQUALITY_DATE_RE = re.compile(
    r"(?P<column>[A-Za-z_]\w*)\s*(?:=|==)\s*['\"](?P<date>\d{4}[-_]\d{2}[-_]\d{2})['\"]",
    re.I,
)
LIKE_DATE_RE = re.compile(
    r"(?P<column>[A-Za-z_]\w*)\s+LIKE\s+['\"]%(?P<date>\d{4}[-_]\d{2}(?:[-_]\d{2})?)%['\"]",
    re.I,
)
FILENAME_DATE_RE = re.compile(r"(?<!\d)(?P<date>\d{4}[-_]\d{2}[-_]\d{2})(?!\d)")
PYSPARK_IN_LIST_RE = re.compile(r"\bIN\s*\(\s*\[\s*(?P<values>.*?)\s*\]\s*\)", re.I)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _glob_base(path: str | None) -> str | None:
    if not path:
        return path
    positions = [path.find(char) for char in ("*", "?", "[", "{")]
    positions = [position for position in positions if position >= 0]
    if not positions:
        return path
    slash = path.rfind("/", 0, min(positions))
    return path[: slash + 1] if slash >= 0 else path


def _normalize_filter(value: Any) -> str:
    """Normalize extractor display artifacts without changing filter meaning."""
    expression = " ".join(str(value).strip().split())
    return PYSPARK_IN_LIST_RE.sub(lambda match: f"IN ({match.group('values')})", expression)


def _filename_date_range(path: str | None) -> tuple[str | None, str | None]:
    """Return an ordered ISO date range when a filename contains two dates."""
    if not path:
        return None, None
    filename = path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    dates = []
    for match in FILENAME_DATE_RE.finditer(filename):
        value = match.group("date").replace("_", "-")
        try:
            date.fromisoformat(value)
        except ValueError:
            continue
        if value not in dates:
            dates.append(value)
    if len(dates) < 2:
        return None, None
    return dates[0], dates[1]


def _scope(entry: Mapping[str, Any], source_path: str | None = None) -> dict[str, Any]:
    filters = []
    for value in entry.get("filters") or []:
        normalized = _normalize_filter(value)
        if normalized and normalized not in filters:
            filters.append(normalized)
    column = _text(entry.get("partition_column"))
    value = _text(entry.get("partition_value"))
    start = _text(entry.get("start_date"))
    end = _text(entry.get("end_date"))
    for expression in filters:
        between = BETWEEN_RE.search(expression)
        if between:
            column = column or between.group("column")
            start = start or between.group("start")
            end = end or between.group("end")
            continue
        lower, upper = LOWER_RE.search(expression), UPPER_RE.search(expression)
        if lower:
            column = column or lower.group("column")
            start = start or lower.group("date")
        if upper:
            column = column or upper.group("column")
            end = end or upper.group("date")
        equality = EQUALITY_DATE_RE.search(expression)
        if equality:
            column = column or equality.group("column")
            value = value or equality.group("date")
        like = LIKE_DATE_RE.search(expression)
        if like:
            column = column or like.group("column")
            value = value or like.group("date")

    if not start and not end:
        inferred_start, inferred_end = _filename_date_range(source_path)
        start = inferred_start
        end = inferred_end

    if start or end:
        request_type = "DATE_RANGE_BACKFILL"
        migration_scope = "PARTIAL"
    elif column and value:
        request_type = "PARTITION_BACKFILL"
        migration_scope = "PARTIAL"
    elif filters:
        request_type = "PARTIAL"
        migration_scope = "PARTIAL"
    else:
        request_type = "FULL"
        migration_scope = "FULL"
    return {
        "filters": filters,
        "partition_column": column,
        "partition_value": value,
        "start_date": start,
        "end_date": end,
        "request_type": request_type,
        "migration_scope": migration_scope,
    }


def _path_object_type(path: str, source_format: str | None) -> str:
    if path.startswith("/Volumes/"):
        return "VOLUME"
    if source_format or not path.endswith("/"):
        return "FILE"
    return "DIRECTORY"


def _plan_id(job_id: str, row: Mapping[str, Any]) -> str:
    identity = {
        "job_id": job_id,
        "object_type": row["object_type"],
        "object_name": row["object_name"],
        "filters": row["filters"],
        "partition_column": row["partition_column"],
        "partition_value": row["partition_value"],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
    }
    if row.get("source_connector"):
        identity.update(source_connector=row["source_connector"],
                        source_object=row["source_object"])
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_plan(report: Mapping[str, Any], job_id: str) -> dict[str, Any]:
    notebooks = report.get("notebooks")
    if not isinstance(notebooks, Mapping):
        raise ValueError("Report is missing the notebooks object")

    produced_tables = set()
    produced_paths = set()
    notebook_order = {str(notebook): index for index, notebook in enumerate(notebooks)}
    write_events: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for notebook, data in notebooks.items():
        produced_tables.update(value for value in (data.get("write_tables") or []) if isinstance(value, str))
        produced_paths.update(value for value in (data.get("write_paths") or []) if isinstance(value, str))
        for event in data.get("write_references") or []:
            if not isinstance(event, Mapping):
                continue
            kind = str(event.get("object_type") or "").upper()
            name = _text(event.get("object_name"))
            if kind not in {"TABLE", "PATH"} or not name:
                continue
            write_events[(kind, name)].append({
                "notebook_path": str(notebook),
                "task_order": notebook_order[str(notebook)],
                "cell_number": int(event.get("cell_number") or 10**9),
                "line_number": int(event.get("line_number") or 10**9),
                "write_mode": str(event.get("write_mode") or "").lower() or None,
                "usage": str(event.get("usage") or ""),
            })

    candidates: dict[tuple[Any, ...], dict[str, Any]] = {}
    exclusions = []
    unresolved = []
    unresolved_seen = set()

    def add_unresolved(notebook: str, reference: Any, category: str) -> None:
        if isinstance(reference, Mapping):
            item = {
                "notebook_path": notebook,
                "category": category,
                **dict(reference),
            }
        else:
            item = {
                "notebook_path": notebook,
                "category": category,
                "reference": str(reference),
            }
        marker = json.dumps(item, sort_keys=True, default=str)
        if marker not in unresolved_seen:
            unresolved_seen.add(marker)
            unresolved.append(item)

    def add_candidate(kind: str, name: str, notebook: str, entry: Mapping[str, Any]) -> None:
        external = bool(entry.get("source_connector"))
        # A connector table lives in the source system, not as a same-named
        # Spark output/view. A connector-resolved path is the same object as any
        # write to that path, so it keeps producer/order handling.
        foreign_table = external and kind == "TABLE"
        read_task = notebook_order.get(notebook, 10**9)
        read_cell = int(entry.get("cell_number") or 10**9)
        read_line = int(entry.get("line_number") or 10**9)
        events = [] if foreign_table else write_events.get((kind, name), [])
        prior_events = [
            event for event in events
            if event["task_order"] < read_task
            or (event["task_order"] == read_task and (
                event["cell_number"] < read_cell
                or (event["cell_number"] == read_cell
                    and event["line_number"] < read_line)
            ))
        ]
        prior_complete_write = next(
            (event for event in prior_events if event.get("write_mode") != "append"), None
        )
        legacy_produced = not foreign_table and not events and name in (
            produced_tables if kind == "TABLE" else produced_paths
        )
        if prior_complete_write or legacy_produced:
            exclusions.append({
                "object_type": kind, "object_name": name,
                "reason": "WORKFLOW_GENERATED_OUTPUT", "notebook_path": notebook,
                "producer": prior_complete_write,
            })
            return
        if kind == "TABLE" and "." not in name and not foreign_table:
            exclusions.append({
                "object_type": kind, "object_name": name,
                "reason": "NOTEBOOK_LOCAL_OR_TEMP_TABLE", "notebook_path": notebook,
            })
            return

        prior_append = next(
            (event for event in prior_events if event.get("write_mode") == "append"), None
        )
        stateful_reason = None
        if prior_append:
            # An append target carries historical state even when a previous
            # task adds the current batch before the downstream read.
            scope = _scope({}, name if kind == "PATH" else None)
            stateful_reason = "STATEFUL_APPEND_HISTORY"
        else:
            scope = _scope(entry, name if kind == "PATH" else None)
            if events:
                stateful_reason = "STATEFUL_READ_BEFORE_WRITE"
        source = name if kind == "PATH" else None
        target = _text(entry.get("oci_path")) if kind == "PATH" else name
        if external:
            target = _text(entry.get("target_object"))
        target = _glob_base(target)
        source_base = _glob_base(source)
        source_format = _text(entry.get("source_format"))
        explicit_object_type = str(entry.get("object_type") or "").strip().upper()
        object_type = (
            kind if kind == "TABLE"
            else explicit_object_type
            if explicit_object_type in {"FILE", "DIRECTORY", "VOLUME"}
            else _path_object_type(source_base or name, source_format)
        )
        # A connector's format names the encoding, not the object's shape:
        # a parquet directory is still a DIRECTORY. Type it by location.
        if external and kind == "PATH":
            object_type = _path_object_type(target or source_base or name, None)
        object_name = target or source_base or name
        status = ("MAPPING_REQUIRED" if (external or kind == "PATH") and not target
                  else "PROACTIVE_REQUIRED")
        key = (
            object_type, object_name, tuple(scope["filters"]),
            scope["partition_column"], scope["partition_value"],
            scope["start_date"], scope["end_date"],
            entry.get("source_connector"), entry.get("source_object"),
        )
        row = candidates.get(key)
        if row is None:
            row = {
                "plan_id": "",
                "databricks_job_id": job_id,
                "status": status,
                "migration_scope": scope["migration_scope"],
                "request_type": scope["request_type"],
                "object_type": object_type,
                "object_name": object_name,
                "source_path": source_base,
                "source_pattern": source if source != source_base else None,
                "target_path": target,
                "source_format": source_format.upper() if source_format else None,
                "partition_column": scope["partition_column"],
                "partition_value": scope["partition_value"],
                "start_date": scope["start_date"],
                "end_date": scope["end_date"],
                "filters": scope["filters"],
                "stateful_reason": stateful_reason,
                "currently_exists": entry.get("exists"),
                "access_failure": "",
                "current_scope_exists": entry.get("scope_exists"),
                "notebook_paths": [],
                "cell_numbers": [],
                "stakeholder_message": "",
            }
            candidates[key] = row
            if external:
                row.update({key: entry.get(key) for key in (
                    "source_connector", "source_object")})
                row["source_site_ids"] = []
        # A shared row cannot certify a Python glob using another reader's probe.
        if entry.get("glob_match_status") == "UNVERIFIED":
            row["currently_exists"] = "unverified"
        if external and entry.get("site_id"):
            row["source_site_ids"] = sorted(set(row["source_site_ids"] + [entry["site_id"]]))
        _status = str(entry.get("probe_status") or "")
        if _status in ("ACCESS_DENIED", "ACCESS_ERROR", "PROBE_ERROR") and not row.get("access_failure"):
            row["access_failure"] = _status
        row["notebook_paths"].append(notebook)
        if entry.get("cell_number") is not None:
            row["cell_numbers"].append(entry.get("cell_number"))
        row["notebook_paths"] = sorted(set(row["notebook_paths"]))
        row["cell_numbers"] = sorted(set(row["cell_numbers"]))

    for notebook, data in notebooks.items():
        # `reason` is the exclusion category for every row; keep a saved
        # report's free-text explanation separately so it cannot impersonate
        # another category.
        exclusions.extend({**item, "notebook_path": str(notebook), "reason": "NOT_AN_INPUT_READ",
                           "analysis_reason": item.get("reason")}
                          for item in data.get("excluded_reads") or [])
        for entry in data.get("read_tables") or []:
            if isinstance(entry, Mapping) and _text(entry.get("table")):
                add_candidate("TABLE", str(entry["table"]), str(notebook), entry)
        for entry in data.get("read_paths") or []:
            if isinstance(entry, Mapping) and _text(entry.get("source")):
                add_candidate("PATH", str(entry["source"]), str(notebook), entry)
        for reference in data.get("unresolved") or []:
            add_unresolved(str(notebook), reference, "DYNAMIC_REFERENCE")
        for reference in data.get("unresolved_scopes") or []:
            add_unresolved(str(notebook), reference, "UNRESOLVED_SCOPE")
        for reference in data.get("unresolved_mlflow_artifacts") or []:
            add_unresolved(str(notebook), reference, "UNRESOLVED_MLFLOW_ARTIFACT")

    requirements = sorted(
        candidates.values(),
        key=lambda row: (row["status"], row["object_type"], row["object_name"], json.dumps(row["filters"])),
    )
    for row in requirements:
        row["plan_id"] = _plan_id(job_id, row)
        scope_text = row["request_type"]
        if row["partition_column"] and row["partition_value"]:
            scope_text += f" ({row['partition_column']}={row['partition_value']})"
        if row["start_date"] or row["end_date"]:
            scope_text += f" ({row['start_date'] or 'open'} to {row['end_date'] or 'open'})"
        if row.get("access_failure"):
            # A data-copy request cannot fix an access failure; say what the
            # probe actually saw (REF-257). OCI masks "not authorized" as
            # 404 BucketNotFound, so the ambiguity is stated, not guessed.
            detail = {
                "ACCESS_DENIED": "access denied (403)",
                "ACCESS_ERROR": ("bucket not found OR not authorized — OCI "
                                  "masks authorization failures as 404"),
                "PROBE_ERROR": "availability probe failed",
            }[row["access_failure"]]
            row["stakeholder_message"] = (
                f"Access failure probing {row['object_type']} {row['object_name']}: {detail}. "
                f"Verify the bucket/prefix exists and the AIDP principal has read+list "
                f"permission on it, then prepare as {scope_text} before migration."
            )
        else:
            row["stakeholder_message"] = (
                f"Prepare {row['object_type']} {row['object_name']} as {scope_text} before migration."
            )

    summary = {
        "requirements": len(requirements),
        "full": sum(row["migration_scope"] == "FULL" for row in requirements),
        "partial": sum(row["migration_scope"] == "PARTIAL" for row in requirements),
        "tables": sum(row["object_type"] == "TABLE" for row in requirements),
        "files_or_paths": sum(row["object_type"] != "TABLE" for row in requirements),
        "mapping_required": sum(row["status"] == "MAPPING_REQUIRED" for row in requirements),
        "workflow_outputs_excluded": sum(row["reason"] == "WORKFLOW_GENERATED_OUTPUT" for row in exclusions),
        "local_or_temp_excluded": sum(row["reason"] == "NOTEBOOK_LOCAL_OR_TEMP_TABLE" for row in exclusions),
        "unresolved_references": len(unresolved),
    }
    return {
        "schema_version": "1.0",
        "document_type": "data_migration_requirements",
        "job_id": job_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "assumption": "Target environment is empty; current availability does not suppress requirements.",
        "loadable": str(job_id).isdigit(),
        "summary": summary,
        "requirements": requirements,
        "exclusions": exclusions,
        "unresolved": unresolved,
    }


def write_excel(path: Path, plan: Mapping[str, Any]) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter

    workbook = Workbook()

    def add_sheet(name: str, headers: list[str], rows: list[Mapping[str, Any]]) -> None:
        sheet = workbook.active if len(workbook.sheetnames) == 1 and workbook.active.max_row == 1 and workbook.active["A1"].value is None else workbook.create_sheet()
        sheet.title = name
        sheet.append(headers)
        for row in rows:
            sheet.append([
                json.dumps(row.get(header), ensure_ascii=False, default=str)
                if isinstance(row.get(header), (list, dict)) else row.get(header)
                for header in headers
            ])
        fill = PatternFill("solid", fgColor="1F4E78")
        for cell in sheet[1]:
            cell.fill = fill
            cell.font = Font(color="FFFFFF", bold=True)
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
        for cells in sheet.columns:
            width = max((len(str(cell.value or "")) for cell in cells), default=10) + 2
            sheet.column_dimensions[get_column_letter(cells[0].column)].width = min(max(width, 12), 65)

    summary_rows = [{"metric": key, "value": value} for key, value in plan["summary"].items()]
    summary_rows.insert(0, {"metric": "assumption", "value": plan["assumption"]})
    add_sheet("Summary", ["metric", "value"], summary_rows)
    requirement_headers = [
        "plan_id", "status", "migration_scope", "request_type", "object_type",
        "object_name", "source_path", "source_pattern", "target_path", "source_format",
        "partition_column", "partition_value", "start_date", "end_date", "filters",
        "stateful_reason",
        "currently_exists", "current_scope_exists", "access_failure",
        "notebook_paths", "cell_numbers",
        "stakeholder_message",
        "source_connector", "source_object", "source_site_ids",
    ]
    add_sheet("Proactive Requirements", requirement_headers, list(plan["requirements"]))
    add_sheet("Excluded Workflow Outputs", ["object_type", "object_name", "reason", "notebook_path"], [
        row for row in plan["exclusions"] if row["reason"] == "WORKFLOW_GENERATED_OUTPUT"
    ])
    add_sheet("Excluded Local Temp", ["object_type", "object_name", "reason", "notebook_path"], [
        row for row in plan["exclusions"] if row["reason"] == "NOTEBOOK_LOCAL_OR_TEMP_TABLE"
    ])
    add_sheet("Unresolved References", ["notebook_path", "category", "kind", "reference",
                                       "cell_number", "line_number", "expression", "reason"],
              list(plan["unresolved"]))
    add_sheet("Excluded Read Sites", ["notebook_path", "site_id", "cell_number",
                                     "line_number", "expression", "analysis_reason"], [
        row for row in plan["exclusions"] if row["reason"] == "NOT_AN_INPUT_READ"
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an empty-target proactive data migration plan")
    parser.add_argument("--input", required=True, help="Fresh data-check JSON report")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-excel")
    args = parser.parse_args()
    report = json.loads(Path(args.input).read_text(encoding="utf-8"))
    plan = build_plan(report, args.job_id)
    Path(args.output_json).write_text(json.dumps(plan, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    if args.output_excel:
        write_excel(Path(args.output_excel), plan)
    print(json.dumps(plan["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
