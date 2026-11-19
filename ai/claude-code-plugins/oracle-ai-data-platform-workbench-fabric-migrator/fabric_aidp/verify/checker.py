"""Verify a complete migration report and re-classify each result.

    ok                  -> PASS
    needs_manual_review -> REVIEW
    planned / skipped   -> SKIP
    blocked             -> REVIEW
    error               -> FAIL

PASS means "no known issue was detected", not "this runs on Spark". Nothing
here parses or executes the artifact, so a construct no rule covers is
reported clean. The wording in the summary, the reports and the docs must not
claim more than that — tests/test_verify.py::ClaimTests enforces it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from fabric_aidp.namespace import ADVICE as _NAMESPACE_ADVICE
from fabric_aidp.namespace import carries_placeholder
from fabric_aidp.sources import ALL_SOURCES, SOURCE_BY_PLAN_TYPE

# The slices `--filter` selects, shared with `inventory --sources` and
# `migrate --filter` so the three cannot drift: this copy was missing
# `dataflow`, which the CLI help offered and this function then refused.
SOURCE_NAMES = ALL_SOURCES
_STATUSES = ("ok", "needs_manual_review", "planned", "skipped", "blocked",
             "error")
# `blocked` is an honest refusal -- a translator exists but this object uses
# something out of scope, so nothing was emitted and a human must port it by
# hand. That is REVIEW: FAIL means *this tool* broke, and SKIP would read as
# "nothing to do here" when there is a whole object to migrate.
_VERDICTS = {"ok": "PASS", "needs_manual_review": "REVIEW", "planned": "SKIP",
             "skipped": "SKIP", "blocked": "REVIEW", "error": "FAIL"}
_IN_PROGRESS_MARKER = ".fabric-aidp-migration-in-progress"


def _row_source(row):
    """Which source slice this report row belongs to, or None.

    Three ways, because the runner writes `kind` differently depending on
    how far the row got: a translated row carries a report kind
    (`warehouse_table`, `shortcut`), an errored one carries the plan's own
    source type (`fabric_notebook`) and nothing else. The middle lookup was
    missing, so `kind` could never attribute an error row and only the
    asset_id prefix ever did.
    """
    asset_id = row.get("asset_id")
    if isinstance(asset_id, str) and "." in asset_id:
        prefix = asset_id.split(".", 1)[0]
        if prefix in SOURCE_NAMES:
            return prefix
    kind = row.get("kind")
    if isinstance(kind, str):
        if kind in SOURCE_BY_PLAN_TYPE:
            return SOURCE_BY_PLAN_TYPE[kind]
        for source in SOURCE_NAMES:
            if kind == source or kind.startswith(source):
                return source
    return None


def _artifact_problem(output_path, report_path: Path):
    """(problem, resolved path). `problem` is None when the artifact is there.

    The resolved path comes back so the caller can read what was written:
    `verify` is the last gate before `publish`, and the report alone cannot
    say whether the file on disk still carries a placeholder target.
    """
    if not isinstance(output_path, str) or not output_path.strip():
        return "successful result is missing output_path", None
    report_dir = report_path.resolve().parent
    raw = Path(output_path)
    # `anchor` too: on Windows `/etc/passwd` has a root but no drive, so
    # is_absolute() is False for a report written on Linux.
    if raw.is_absolute() or raw.anchor:
        return "output_path must be relative to the migration report directory", None
    if ".." in raw.parts:
        return "output_path escapes the migration report directory", None
    resolved = (report_dir / raw).resolve(strict=False)
    try:
        inside = os.path.commonpath((str(report_dir), str(resolved))) == str(report_dir)
    except ValueError:
        inside = False
    if not inside:
        return "output_path escapes the migration report directory", None
    if not resolved.is_file():
        return "output artifact is missing", None
    return None, resolved


def _first_reason(row) -> str:
    """The first finding's detail, for a row that has no note and no error.

    A refusal keeps its reason in `findings`, so `verify` printed
    `REVIEW pipeline.Daily` and nothing else -- the reader was told there was
    something to do and not what.
    """
    for finding in row.get("findings") or []:
        if isinstance(finding, dict) and finding.get("detail"):
            return str(finding["detail"])
    return ""


def _artifact_carries_placeholder(path) -> bool:
    """Whether the written artifact still spells the OCI namespace placeholder.

    Read from disk rather than from the report's `translated_sql`: the file
    is what `publish` uploads, and a report can disagree with it.
    """
    if path is None:
        return False
    try:
        return carries_placeholder(path.read_text(encoding="utf-8", errors="replace"))
    except OSError:
        return False


def _validate_counts(report: dict) -> None:
    counts = report.get("counts")
    if not isinstance(counts, dict):
        raise ValueError("complete migration report field 'counts' must be a JSON object")
    expected = {status: 0 for status in _STATUSES}
    for row in report["results"]:
        if isinstance(row, dict) and isinstance(row.get("status"), str):
            if row["status"] in expected:
                expected[row["status"]] += 1
    for status, count in expected.items():
        actual = counts.get(status, 0)
        if isinstance(actual, bool) or not isinstance(actual, int) or actual < 0:
            raise ValueError(f"migration report count {status!r} must be a "
                             f"non-negative integer")
        if actual != count:
            raise ValueError(f"migration report count mismatch for {status!r}: "
                             f"reported {actual}, results contain {count}")


def verify(report_path, *, filter_kind=None) -> dict:
    if filter_kind is not None and filter_kind not in SOURCE_NAMES:
        raise ValueError(f"unknown verification filter {filter_kind!r}; "
                         f"valid: {SOURCE_NAMES}")
    report_path = Path(report_path)
    marker = report_path.resolve().parent / _IN_PROGRESS_MARKER
    if marker.exists() or marker.is_symlink():
        raise ValueError(f"migration is incomplete: in-progress marker exists: {marker}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("migration report must contain a JSON object")
    if report.get("complete") is not True:
        raise ValueError("migration report is incomplete or predates the "
                         "completeness contract")
    if not isinstance(report.get("results"), list):
        raise ValueError("migration report field 'results' must be a JSON array")
    _validate_counts(report)

    summary = {"PASS": 0, "REVIEW": 0, "SKIP": 0, "FAIL": 0}
    rows = []
    for index, value in enumerate(report["results"]):
        if not isinstance(value, dict):
            # A corrupt row is a verification failure and must not vanish
            # behind a filter.
            summary["FAIL"] += 1
            rows.append({"asset_id": f"<invalid-result-{index}>", "verdict": "FAIL",
                         "changes": 0, "flags": 0,
                         "note": "report result must be a JSON object"})
            continue
        status = value.get("status") if isinstance(value.get("status"), str) else None
        verdict = _VERDICTS.get(status, "FAIL")
        note = value.get("note") or value.get("error") or _first_reason(value)
        asset_id = value.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id.strip():
            verdict, asset_id = "FAIL", f"<missing-asset-id-{index}>"
            note = "report result is missing a non-empty asset_id"
        elif status not in _VERDICTS:
            note = note or f"unknown report status: {value.get('status')!r}"

        flags = value.get("flags", 0)
        if isinstance(flags, bool) or not isinstance(flags, int) or flags < 0:
            verdict, flags = "FAIL", 0
            note = "report result flags must be a non-negative integer"
        elif status == "ok" and flags:
            verdict = "FAIL"
            note = "result status is 'ok' but manual-review flags are present"

        if status in ("ok", "needs_manual_review"):
            problem, artifact = _artifact_problem(value.get("output_path"),
                                                  report_path)
            if problem:
                verdict, note = "FAIL", problem
            elif _artifact_carries_placeholder(artifact):
                # The runner flags this (NS01) so its own reports never call
                # such a row `ok`. One that does came from an older migrate
                # or was edited by hand, and is the same contradiction as an
                # `ok` row with manual-review flags: FAIL. A row already at
                # REVIEW keeps that verdict and gains the reason, because
                # "which of these flags stops it running" is the question the
                # reader is actually asking.
                verdict = "FAIL" if status == "ok" else verdict
                note = _NAMESPACE_ADVICE

        # The filter is applied last, and two kinds of row are exempt from
        # it -- the same rule the corrupt-row branch above already follows.
        #
        # A FAIL, because `verify --filter notebook` printed FAIL 0 and
        # exited 0 on a migration that had errors.
        #
        # A row this tool cannot attribute to any slice, whatever its
        # verdict. `unreadable.<path>` (INV01) and `unsupported.<type>.
        # <name>` (INV02) are refusals that belong to no scanner by design,
        # and the runner grades them `blocked`, which is REVIEW and not
        # FAIL -- so the FAIL exemption never reached them. MEASURED on a
        # report of one PASS notebook and those two refusals: unfiltered
        # gave PASS 1 / REVIEW 2, and every one of the six `--filter`
        # values gave REVIEW 0. Hidden in each slice is recoverable by
        # asking for another; hidden in all six is not.
        row_source = _row_source(value)
        if filter_kind and row_source != filter_kind:
            if verdict != "FAIL" and row_source is not None:
                continue
            note = (f"{note}; " if note else "") + (
                f"shown despite --filter {filter_kind}: a failure is never "
                f"hidden by a filter" if verdict == "FAIL" else
                f"shown despite --filter {filter_kind}: this row belongs to "
                f"no source slice, so no filter can select it and a refusal "
                f"is never hidden by one")

        summary[verdict] += 1
        rows.append({"asset_id": asset_id, "verdict": verdict,
                     "changes": value.get("changes", 0), "flags": flags, "note": note})
    # Files in the directory that `migrate` did not write. Carried through
    # rather than re-derived, because verify is handed a report and the
    # runner is the thing that knows what it wrote. Not a verdict: a second
    # `--filter` run into one directory is a workflow this tool recommends,
    # so failing it would fail correct use. It is surfaced because verify is
    # the last thing read before `publish`.
    unclaimed = report.get("unclaimed_artifacts")
    unclaimed = [str(name) for name in unclaimed] if isinstance(unclaimed, list) else []
    return {"summary": summary, "rows": rows, "unclaimed_artifacts": unclaimed}


def format_verify(result: dict) -> str:
    summary = result["summary"]
    lines = [
        "verify summary:",
        f"  PASS:   {summary['PASS']}",
        f"  REVIEW: {summary['REVIEW']}",
        f"  SKIP:   {summary['SKIP']}",
        f"  FAIL:   {summary['FAIL']}",
        "",
        "  PASS = translated, no known issue detected -- not execution-verified.",
        "         Nothing parses or runs the artifact, so a construct no rule",
        "         covers is reported clean. Review before running in production.",
        "",
        "per-asset:",
    ]
    for row in result["rows"]:
        suffix = f"  changes={row['changes']}" if row["changes"] else ""
        if row["flags"]:
            suffix += f" flags={row['flags']}"
        if row["note"]:
            suffix += f"  ({row['note']})"
        lines.append(f"  {row['verdict']:<6s} {row['asset_id']}{suffix}")
    unclaimed = result.get("unclaimed_artifacts") or []
    if unclaimed:
        lines += [
            "",
            f"{len(unclaimed)} file(s) in this directory were not written by "
            f"the run that wrote this report,",
            "and it does not vouch for them. `publish` reads the report, so it "
            "will not send them.",
        ]
        lines += [f"  {name}" for name in unclaimed]
    return "\n".join(lines)
