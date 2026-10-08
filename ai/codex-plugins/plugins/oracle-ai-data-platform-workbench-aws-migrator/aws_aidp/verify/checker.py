"""Verify a complete migration report and re-classify each result:

- `ok`                  → PASS  (deterministic translation, no manual flags)

PASS means "no known issue was detected", not "this runs on Spark".  Nothing
here parses or executes the artifact -- the Athena translator has no parser --
so a construct no rule covers is reported clean.  The wording in the summary,
the reports and the docs must not claim more than that.
- `needs_manual_review` → REVIEW
- `planned`             → SKIP  (asset type not yet implemented)
- `error`               → FAIL
"""
from __future__ import annotations

import json
import os
from pathlib import Path

_SOURCE_NAMES = ("s3", "glue", "athena", "emr", "sagemaker")
_KIND_SOURCE_PREFIXES = {
    "s3": ("s3_",),
    "glue": ("glue_",),
    "athena": ("athena_",),
    "emr": ("emr_",),
    "sagemaker": ("sagemaker_", "sm_"),
}
_STATUSES = ("ok", "needs_manual_review", "planned", "skipped", "error")
_IN_PROGRESS_MARKER = ".aws-aidp-migration-in-progress"


def _result_source(row: dict) -> str | None:
    asset_id = row.get("asset_id")
    if isinstance(asset_id, str) and "." in asset_id:
        prefix = asset_id.split(".", 1)[0]
        if prefix in _SOURCE_NAMES:
            return prefix
    kind = row.get("kind")
    if isinstance(kind, str):
        for source, prefixes in _KIND_SOURCE_PREFIXES.items():
            if kind == source or kind.startswith(prefixes):
                return source
    return None


def _artifact_problem(output_path: object, report_path: Path) -> str | None:
    if not isinstance(output_path, str) or not output_path.strip():
        return "successful result is missing output_path"
    report_dir = report_path.resolve().parent
    raw_path = Path(output_path)
    if raw_path.is_absolute():
        return "output_path must be relative to the migration report directory"
    if ".." in raw_path.parts:
        return "output_path escapes the migration report directory"
    resolved = (report_dir / raw_path).resolve(strict=False)
    try:
        inside = os.path.commonpath((str(report_dir), str(resolved))) == str(report_dir)
    except ValueError:
        inside = False
    if not inside:
        return "output_path escapes the migration report directory"
    if not resolved.is_file():
        return "output artifact is missing"
    return None


def _validate_counts(report: dict) -> None:
    counts = report.get("counts")
    if not isinstance(counts, dict):
        raise ValueError("complete migration report field 'counts' must be a JSON object")
    expected = {status: 0 for status in _STATUSES}
    for row in report["results"]:
        if isinstance(row, dict) and isinstance(row.get("status"), str):
            status = row["status"]
            if status in expected:
                expected[status] += 1
    for status, expected_count in expected.items():
        actual = counts.get(status)
        if isinstance(actual, bool) or not isinstance(actual, int) or actual < 0:
            raise ValueError(f"migration report count {status!r} must be a non-negative integer")
        if actual != expected_count:
            raise ValueError(
                f"migration report count mismatch for {status!r}: "
                f"reported {actual}, results contain {expected_count}"
            )


def verify(report_path: Path, *, filter_kind: str | None = None) -> dict:
    if filter_kind is not None and filter_kind not in _SOURCE_NAMES:
        raise ValueError(
            f"unknown verification filter {filter_kind!r}; valid: {_SOURCE_NAMES}"
        )
    report_path = Path(report_path)
    marker = report_path.resolve().parent / _IN_PROGRESS_MARKER
    if marker.exists() or marker.is_symlink():
        raise ValueError(
            f"migration is incomplete: in-progress marker exists: {marker}"
        )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("migration report must contain a JSON object")
    if report.get("complete") is not True:
        raise ValueError("migration report is incomplete or predates the completeness contract")
    if "results" not in report or not isinstance(report["results"], list):
        raise ValueError("migration report field 'results' must be a JSON array")
    _validate_counts(report)
    summary = {"PASS": 0, "REVIEW": 0, "SKIP": 0, "FAIL": 0}
    rows: list[dict] = []
    for index, value in enumerate(report["results"]):
        if not isinstance(value, dict):
            # Corrupt rows are verification failures and must not disappear behind a filter.
            summary["FAIL"] += 1
            rows.append({
                "asset_id": f"<invalid-result-{index}>",
                "verdict": "FAIL",
                "changes": 0,
                "flags": 0,
                "note": "report result must be a JSON object",
            })
            continue
        r = value
        if filter_kind and _result_source(r) != filter_kind:
            continue
        asset_id = r.get("asset_id")
        malformed_id = not isinstance(asset_id, str) or not asset_id.strip()
        status = r.get("status")
        known_status = status if isinstance(status, str) else None
        verdict = {
            "ok": "PASS",
            "needs_manual_review": "REVIEW",
            "planned": "SKIP",
            "error": "FAIL",
            "skipped": "SKIP",
        }.get(known_status, "FAIL")
        note = r.get("note") or r.get("error") or ""
        if malformed_id:
            verdict = "FAIL"
            asset_id = f"<missing-asset-id-{index}>"
            note = "report result is missing a non-empty asset_id"
        elif known_status not in {"ok", "needs_manual_review", "planned", "error", "skipped"}:
            note = note or f"unknown report status: {status!r}"
        flags = r.get("flags", 0)
        invalid_flags = isinstance(flags, bool) or not isinstance(flags, int) or flags < 0
        if invalid_flags:
            verdict = "FAIL"
            note = "report result flags must be a non-negative integer"
            flags = 0
        elif known_status == "ok" and flags:
            verdict = "FAIL"
            note = "result status is 'ok' but manual-review flags are present"
        if known_status in {"ok", "needs_manual_review"}:
            artifact_problem = _artifact_problem(r.get("output_path"), report_path)
            if artifact_problem:
                verdict = "FAIL"
                note = artifact_problem
        summary[verdict] += 1
        rows.append({
            "asset_id": asset_id,
            "verdict": verdict,
            "changes": r.get("changes", 0),
            "flags": flags,
            "note": note,
        })
    return {"summary": summary, "rows": rows}


def format_verify(result: dict) -> str:
    s = result["summary"]
    lines = [
        "verify summary:",
        f"  PASS:   {s['PASS']}",
        f"  REVIEW: {s['REVIEW']}",
        f"  SKIP:   {s['SKIP']}",
        f"  FAIL:   {s['FAIL']}",
        "",
        "  PASS = translated, no known issue detected -- not execution-verified.",
        "         Nothing parses or runs the artifact, so a construct no rule",
        "         covers is reported clean. Review before running in production.",
        "",
        "per-asset:",
    ]
    for r in result["rows"]:
        suffix = ""
        if r["changes"]:
            suffix = f"  changes={r['changes']}"
        if r["flags"]:
            suffix += f" flags={r['flags']}"
        if r["note"]:
            suffix += f"  ({r['note']})"
        lines.append(f"  {r['verdict']:<6s} {r['asset_id']}{suffix}")
    return "\n".join(lines)
