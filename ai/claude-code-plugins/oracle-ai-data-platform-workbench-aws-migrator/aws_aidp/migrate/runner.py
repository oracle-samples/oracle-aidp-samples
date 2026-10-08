"""Execute a migration plan. In --demo mode: offline, writes artifacts + a report.

Offline demo mode translates Athena SQL and Glue ETL scripts. Other asset types
are listed as planned so the report never implies an unsupported migration ran.

Live write-side migration is not implemented yet. Calls without ``--demo`` fail
closed instead of producing a successful report that did not migrate anything.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import threading
import time
import unicodedata
import uuid
from pathlib import Path
from typing import Callable

from aws_aidp.translate.athena_to_spark_sql import translate as athena_translate
from aws_aidp.translate.glue_to_spark import translate as glue_translate
from aws_aidp.translate.s3_to_oci import build_transfer as s3_build_transfer


def _emit(line: str, log: Callable[[str], None] | None) -> None:
    if log:
        log(line)


_FILTER_TYPES = {
    "s3": {"s3_bucket"},
    "glue": {"glue_database", "glue_table", "glue_job"},
    "athena": {"athena_named_query"},
    "emr": {"emr_cluster", "emr_notebook"},
    "sagemaker": {
        "sm_notebook_instance", "sm_training_job", "sm_model", "sm_pipeline"
    },
}

_OUTPUT_LOCKS: dict[str, threading.RLock] = {}
_OUTPUT_LOCKS_GUARD = threading.Lock()


def _matches_filter(source_type: str, filter_kind: str | None) -> bool:
    if not filter_kind:
        return True
    return source_type in _FILTER_TYPES.get(filter_kind, set())


def _safe_name(name: object) -> str:
    value = unicodedata.normalize("NFC", str(name or "unnamed"))
    value = re.sub(r"[\\/:*?\"<>|\x00-\x1f\x7f]", "_", value).strip().rstrip(". ")
    if value in ("", ".", ".."):
        value = "unnamed"
    # Windows treats these basenames as devices even when an extension exists.
    if value.split(".", 1)[0].upper() in {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }:
        value = f"_{value}"
    value = value[:180]
    while len(value.encode("utf-8")) > 180:
        value = value[:-1]
    return value or "unnamed"


def _path_key(path: Path) -> str:
    """A conservative key that also avoids collisions on case-folding filesystems."""
    return unicodedata.normalize("NFC", str(path)).casefold()


def _within(root: Path, candidate: Path) -> bool:
    try:
        return os.path.commonpath([str(root.resolve()), str(candidate.resolve())]) == str(root.resolve())
    except ValueError:
        return False


def _artifact_path(
    out_dir: Path,
    category: str,
    name: object,
    suffix: str,
    asset_id: str,
    used_paths: set[Path],
) -> Path:
    directory = out_dir / category
    safe_name = _safe_name(name)
    candidate = directory / f"{safe_name}{suffix}"
    used_keys = {_path_key(path) for path in used_paths}
    if _path_key(candidate) in used_keys:
        digest = hashlib.sha256(asset_id.encode("utf-8")).hexdigest()[:10]
        candidate = directory / f"{safe_name}-{digest}{suffix}"
    counter = 2
    while _path_key(candidate) in used_keys:
        candidate = directory / f"{safe_name}-{counter}{suffix}"
        counter += 1
    if not _within(out_dir, candidate):
        raise ValueError(f"artifact path escapes output directory: {candidate}")
    used_paths.add(candidate)
    return candidate


def _report_artifact_path(path: Path, out_dir: Path) -> str:
    """Return the portable path contract stored in report.json.

    Artifact paths are always relative to the directory containing report.json,
    so reports can be verified from another process or working directory.
    """
    try:
        relative = path.resolve().relative_to(out_dir.resolve())
    except ValueError as exc:
        raise ValueError(f"artifact path escapes output directory: {path}") from exc
    return relative.as_posix()


def _write_text_atomic(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("w", encoding="utf-8", newline="") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        # Persist the directory entry as well as the file contents on POSIX.
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                try:
                    os.fsync(directory_fd)
                except OSError:
                    # Some filesystems/platforms do not support directory
                    # fsync. The file itself is already durable and replaced.
                    pass
            finally:
                os.close(directory_fd)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


@contextlib.contextmanager
def _output_lock(out_dir: Path):
    """Serialize a migration directory across threads and POSIX processes."""
    out_dir.mkdir(parents=True, exist_ok=True)
    resolved = out_dir.resolve()
    key = _path_key(resolved)
    with _OUTPUT_LOCKS_GUARD:
        thread_lock = _OUTPUT_LOCKS.setdefault(key, threading.RLock())
    with thread_lock:
        lock_path = resolved / ".aws-aidp-migrate.lock"
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as error:
            raise ValueError(f"cannot safely lock output directory {resolved}: {error}") from error
        try:
            try:
                import fcntl
            except ImportError:  # pragma: no cover - Windows uses the in-process lock.
                fcntl = None
            windows_lock = None
            if fcntl is not None:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            else:  # pragma: no cover - exercised by the Windows CI matrix.
                import msvcrt
                if os.fstat(descriptor).st_size == 0:
                    os.write(descriptor, b"\0")
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
                windows_lock = msvcrt
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            elif windows_lock is not None:  # pragma: no cover - Windows only.
                os.lseek(descriptor, 0, os.SEEK_SET)
                windows_lock.locking(descriptor, windows_lock.LK_UNLCK, 1)
            os.close(descriptor)


def _header_value(value: object) -> str:
    """Make untrusted metadata safe inside a generated one-line comment."""
    return re.sub(r"[\x00-\x1f\x7f]+", " ", str(value if value is not None else "")).strip()


def _inline_code(value: object) -> str:
    clean = _header_value(value)
    longest = max((len(match.group(0)) for match in re.finditer(r"`+", clean)), default=0)
    fence = "`" * max(1, longest + 1)
    return f"{fence} {clean} {fence}"


def _fenced_block(language: str, comment: str, value: str) -> str:
    longest = max((len(match.group(0)) for match in re.finditer(r"`+", value)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}{language}\n{comment}\n{value}\n{fence}"


def _stale_artifacts(
    out_dir: Path, used_paths: set[Path], filter_kind: str | None
) -> list[str]:
    tracked = {_path_key(path.resolve()) for path in used_paths}
    stale = []
    categories = {
        "athena": (out_dir / "athena", "*.spark.sql"),
        "glue": (out_dir / "glue", "*.py"),
    }
    selected = categories.values() if filter_kind is None else (
        (categories[filter_kind],) if filter_kind in categories else ()
    )
    for directory, pattern in selected:
        if not directory.is_dir():
            continue
        # Never traverse an artifact-directory symlink outside the selected
        # output root merely to build a stale-file list.
        if not _within(out_dir, directory):
            continue
        for path in directory.glob(pattern):
            if not _within(out_dir, path):
                continue
            if _path_key(path.resolve()) not in tracked:
                stale.append(str(path))
    return sorted(stale)


def _migrate_athena_demo(asset: dict, out_dir: Path, used_paths: set[Path]) -> dict:
    """Translate an Athena query and write the result + per-query diff."""
    q = asset["source"]
    res = athena_translate(q["query"])
    name = q["name"]
    sql_path = _artifact_path(
        out_dir, "athena", name, ".spark.sql", asset["id"], used_paths
    )
    header = (
        "-- migrated from athena.named_query "
        f"id={_header_value(q['id'])} workgroup={_header_value(q.get('workgroup'))}\n"
    )
    try:
        _write_text_atomic(sql_path, header + res.translated_sql + "\n")
    except Exception:
        used_paths.discard(sql_path)
        raise
    return {
        "asset_id": asset["id"],
        "kind": "athena_query",
        "status": "ok" if not res.needs_manual_review else "needs_manual_review",
        "changes": res.changes,
        "flags": res.flags,
        "output_path": _report_artifact_path(sql_path, out_dir),
        "findings": [{"rule": f.rule, "detail": f.detail, "severity": f.severity}
                     for f in res.findings],
        "source_sql": res.source_sql,
        "translated_sql": res.translated_sql,
    }


def _migrate_glue_demo(
    asset: dict, out_dir: Path, ns: str, used_paths: set[Path]
) -> dict:
    """Translate a Glue ETL script → Spark/PySpark and write it + findings."""
    src = asset["source"]
    script = src.get("script") or ""
    name = src["name"]
    if not script.strip():
        return {"asset_id": asset["id"], "kind": "glue_job", "status": "skipped",
                "note": "no script text in manifest (script_location not fetched); "
                        "re-run inventory with S3 read access"}
    res = glue_translate(script, oci_namespace=ns)
    py_path = _artifact_path(out_dir, "glue", name, ".py", asset["id"], used_paths)
    header = (
        f"# migrated from glue.job {_header_value(name)}  "
        f"src={_header_value(src.get('script_location'))}\n"
    )
    try:
        _write_text_atomic(py_path, header + res.translated_sql + "\n")
    except Exception:
        used_paths.discard(py_path)
        raise
    return {
        "asset_id": asset["id"],
        "kind": "glue_job",
        "status": "ok" if not res.needs_manual_review else "needs_manual_review",
        "changes": res.changes,
        "flags": res.flags,
        "output_path": _report_artifact_path(py_path, out_dir),
        "findings": [{"rule": f.rule, "detail": f.detail, "severity": f.severity}
                     for f in res.findings],
        "source_sql": res.source_sql,
        "translated_sql": res.translated_sql,
    }


def _migrate_s3_demo(asset: dict, out_dir: Path, ns: str, used_paths: set[Path]) -> dict:
    """Generate a reviewable rclone S3→OCI transfer script for a bucket."""
    src = asset["source"]
    tgt = asset["target"]
    bucket = src["name"]
    res = s3_build_transfer(
        bucket=bucket,
        oci_bucket=tgt.get("name", bucket),
        namespace=tgt.get("namespace", ns),
        aws_region=src.get("region"),
    )
    sh_path = _artifact_path(out_dir, "transfer", bucket, ".transfer.sh", asset["id"], used_paths)
    sh_path.parent.mkdir(parents=True, exist_ok=True)
    sh_path.write_text(res.translated_sql, encoding="utf-8")
    sh_path.chmod(0o755)
    return {
        "asset_id": asset["id"],
        "kind": "s3_bucket",
        "status": "ok" if not res.needs_manual_review else "needs_manual_review",
        "changes": res.changes,
        "flags": res.flags,
        "output_path": _report_artifact_path(sh_path, out_dir),
        "findings": [{"rule": f.rule, "detail": f.detail, "severity": f.severity}
                     for f in res.findings],
        "source_sql": res.source_sql,
        "translated_sql": res.translated_sql,
    }


def _stub(asset: dict, kind: str) -> dict:
    return {"asset_id": asset["id"], "kind": kind, "status": "planned",
            "note": "translator not yet implemented; deferred to v0.2"}


def _migrate_locked(
    plan: dict,
    *,
    out_dir: Path,
    filter_kind: str | None = None,
    demo: bool = False,
    log: Callable[[str], None] | None = None,
) -> dict:
    plan_is_mapping = isinstance(plan, dict)
    raw_plan_id = plan.get("plan_id") if plan_is_mapping else None
    plan_id = None if raw_plan_id is None else str(raw_plan_id)
    target = plan.get("target_aidp") if plan_is_mapping else None
    ns = target.get("namespace") if isinstance(target, dict) else None
    ns = ns or "<your-oci-namespace>"
    results: list[dict] = []
    used_paths: set[Path] = set()
    counts = {"ok": 0, "needs_manual_review": 0, "planned": 0, "skipped": 0, "error": 0}
    assets = plan.get("assets") if plan_is_mapping else None
    if not isinstance(assets, list):
        results.append({
            "asset_id": "<invalid-plan>",
            "kind": "unknown",
            "status": "error",
            "error": "migration plan must be an object whose assets field is a list",
        })
        counts["error"] += 1
        assets = []

    report_id = uuid.uuid4().hex
    marker = out_dir / ".aws-aidp-migration-in-progress"
    for report_name in ("report.json", "report.md"):
        current = out_dir / report_name
        previous = out_dir / f".aws-aidp-previous-{report_name}"
        if current.exists():
            os.replace(current, previous)
    _write_text_atomic(marker, json.dumps({
        "report_id": report_id,
        "plan_id": plan_id,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }))

    for a in assets:
        target_type = "unknown"
        try:
            target_type = a["target"]["type"]
            source_type = a["source"]["type"]
            if not _matches_filter(source_type, filter_kind):
                continue
            if source_type == "athena_named_query":
                if demo:
                    r = _migrate_athena_demo(a, out_dir, used_paths)
                else:
                    r = {"asset_id": a["id"], "kind": "athena_query", "status": "skipped",
                         "note": "live mode for Athena not yet wired; use --demo"}
            elif source_type == "glue_job":
                if demo:
                    r = _migrate_glue_demo(a, out_dir, ns, used_paths)
                else:
                    r = {"asset_id": a["id"], "kind": "glue_job", "status": "skipped",
                         "note": "live mode for Glue not yet wired; use --demo"}
            elif source_type == "s3_bucket":
                r = _migrate_s3_demo(a, out_dir, ns, used_paths)
            else:
                r = _stub(a, target_type)
        except Exception as e:
            r = {
                "asset_id": str(a.get("id", "<missing-id>")) if isinstance(a, dict) else "<invalid-asset>",
                "kind": target_type,
                "status": "error",
                "error": str(e),
            }
        counts[r["status"]] = counts.get(r["status"], 0) + 1
        results.append(r)
        if r["status"] in ("ok", "needs_manual_review"):
            _emit(f"  {('OK' if r['status'] == 'ok' else 'REVIEW'):<6s} {r['asset_id']:<40s} changes={r.get('changes', 0)} flags={r.get('flags', 0)}", log)
        elif r["status"] == "planned":
            pass
        elif r["status"] == "error":
            _emit(f"  ERROR  {r['asset_id']:<40s} {r['error']}", log)

    report = {
        "report_id": report_id,
        "complete": True,
        "plan_id": plan_id,
        "migrated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "demo" if demo else "live",
        "filter": filter_kind,
        "counts": counts,
        "results": results,
        "stale_artifacts": _stale_artifacts(out_dir, used_paths, filter_kind),
    }
    # human-readable markdown report
    md = [f"# Migration report  ({report['migrated_at']})", ""]
    md.append(
        f"**Report**: {_inline_code(report_id)}  **Plan**: {_inline_code(plan_id)}  "
        f"**Mode**: {_inline_code(report['mode'])}  "
        f"**Filter**: {_inline_code(filter_kind or 'all')}"
    )
    md.append("")
    md.append("| Status | Count |")
    md.append("|---|---|")
    for k, v in counts.items():
        if v:
            md.append(f"| {k} | {v} |")
    md.append("")
    md.append(
        "> `ok` means translated with no known issue detected — **not "
        "execution-verified**. Nothing here parses or runs the generated "
        "artifacts, so a construct no rule covers is reported clean. Review "
        "them before running in production."
    )
    if report["stale_artifacts"]:
        md.extend(["", "## Stale artifacts from an earlier run", ""])
        md.extend(f"- {_inline_code(path)}" for path in report["stale_artifacts"])
    md.append("")
    md.append("## Per-asset diffs")
    for r in results:
        if r["status"] not in ("ok", "needs_manual_review"):
            continue
        md.append(
            f"### {_inline_code(r['asset_id'])}  "
            f"({r['status']}, {r['changes']} changes, {r['flags']} flags)"
        )
        for f in r.get("findings", []):
            md.append(
                f"- _{_header_value(f['severity'])}_ "
                f"**{_header_value(f['rule'])}** — {_header_value(f['detail'])}"
            )
        md.append("")
        lang = {"glue_job": "python", "s3_bucket": "bash"}.get(r.get("kind"), "sql")
        cmt = "#" if lang in ("python", "bash") else "--"
        md.append(_fenced_block(lang, f"{cmt} source", r["source_sql"]))
        md.append(_fenced_block(lang, f"{cmt} translated", r["translated_sql"]))
        md.append("")
    # JSON is the machine-readable commit marker, so publish it only after the
    # human report is durable.  The in-progress marker makes an interrupted
    # first run unambiguously incomplete.
    _write_text_atomic(out_dir / "report.md", "\n".join(md))
    _write_text_atomic(out_dir / "report.json", json.dumps(report, indent=2, ensure_ascii=False))
    # visual HTML report (open in any browser) — best-effort, never fails the run
    try:
        from aws_aidp.migrate.html_report import render as _render_html
        _render_html(report, out_dir / "report.html")
    except Exception:
        pass
    marker.unlink()
    # A successful retry also clears backups left by an earlier interrupted run.
    for report_name in ("report.json", "report.md"):
        previous = out_dir / f".aws-aidp-previous-{report_name}"
        try:
            previous.unlink()
        except FileNotFoundError:
            pass
    return report


def migrate(
    plan: dict,
    *,
    out_dir: Path,
    filter_kind: str | None = None,
    demo: bool = False,
    log: Callable[[str], None] | None = None,
) -> dict:
    """Execute one coherent migration transaction for an output directory."""
    out_dir = Path(out_dir)
    if filter_kind is not None and filter_kind not in _FILTER_TYPES:
        raise ValueError(
            f"unknown migration filter {filter_kind!r}; expected one of: "
            + ", ".join(sorted(_FILTER_TYPES))
        )
    if not demo:
        raise ValueError(
            "live AIDP migration is not implemented; rerun with --demo to generate offline artifacts"
        )
    with _output_lock(out_dir):
        return _migrate_locked(
            plan,
            out_dir=out_dir,
            filter_kind=filter_kind,
            demo=demo,
            log=log,
        )
