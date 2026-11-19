"""Per-object migration status and migration risk. Pure, zero I/O.

The status vocabulary is deliberately closed, and two of its values --
DATA_CLONE and DONE -- are never produced here. This module sees only the
control-plane deploy result, which copies no data; whether rows were copied
by the in-AIDP job snowmig_02_copy_schema is known to snowmig_03_reconcile
(MIGRATION_REPORT.md), not to this module, so claiming either value would be
a report of something it cannot see. They exist so the vocabulary does not
have to change if that result is ever ingested.
"""
from __future__ import annotations

__all__ = ["MIGRATION_STATUS", "RISK_LEVELS", "assess_risk", "deploy_failure",
           "migration_status", "pipeline_status"]

MIGRATION_STATUS = ("NOT_YET_DONE", "IN_PROGRESS", "SHALLOW_CLONE",
                    "DATA_CLONE", "DONE", "BLOCKED")
RISK_LEVELS = ("LOW", "MEDIUM", "HIGH")

# Above this, the later data phase needs a wave/staging plan of its own.
_LARGE_ROWS = 100_000_000

_RISK_ORDER = {level: i for i, level in enumerate(RISK_LEVELS)}


def _raise(level: str, to: str) -> str:
    """Risk only ever goes up. A VIEW is HIGH; a column warning on the same
    view is a lesser fact and used to overwrite it down to MEDIUM."""
    return to if _RISK_ORDER[to] > _RISK_ORDER[level] else level


def migration_status(identifier: str, *, deployed: dict | None,
                     blocked: bool = False) -> str:
    if blocked:
        return "BLOCKED"
    if not deployed or deployed.get("dry_run"):
        return "NOT_YET_DONE"
    if identifier in set(deployed.get("mismatched_targets") or []):
        # Present in AIDP, but not the object we planned -- and the DDL is
        # CREATE IF NOT EXISTS, so it was left exactly as it was found. This is
        # BLOCKED, not cloned: something else owns that name.
        return "BLOCKED"
    if identifier in set(deployed.get("failed_targets") or []):
        # The create failed -- refused, or accepted and never appeared, or a
        # burned name. It was also attempted, so it used to fall through to
        # IN_PROGRESS below: a permanent failure read as work under way while
        # STAGES.md, from the same result, counted it failed.
        return "BLOCKED"
    if identifier in set(deployed.get("verified_targets") or []):
        # Structure only. DATA_CLONE/DONE are never returned here: the deploy
        # result says nothing about rows (the copy job's outcome lives in
        # 03_reconcile), and claiming otherwise would be a false report.
        return "SHALLOW_CLONE"
    if identifier in set(deployed.get("derived_type_drift_targets") or []):
        # The view exists and is ours; the target derived some column types
        # from the SQL rather than taking ours. Structure-cloned, with a
        # fidelity caveat -- not BLOCKED, and not silently clean either.
        return "SHALLOW_CLONE"
    if identifier in set(deployed.get("unverified_structure_targets") or []):
        # It exists, but its columns were never compared, so "cloned" is not a
        # claim we have earned.
        return "IN_PROGRESS"
    if identifier in set(deployed.get("attempted_targets") or []):
        return "IN_PROGRESS"
    return "NOT_YET_DONE"


def deploy_failure(identifier: str, deployed: dict | None) -> str | None:
    """The deploy result's own sentence for a failed create, or None.

    A burned name gets the short form: its full reason is a paragraph, and
    the one thing the row must say is that only a fresh schema recovers it.
    """
    if not deployed or deployed.get("dry_run"):
        return None
    if identifier not in set(deployed.get("failed_targets") or []):
        return None
    entry = next((f for f in deployed.get("failed") or []
                  if f.get("source_identifier") == identifier), {})
    target = entry.get("target_fqn")
    if target and target in set(deployed.get("poisoned_names") or []):
        return (f"deploy failed and the name `{target}` cannot be reused in "
                f"that schema after a failed create, so retry into a fresh "
                f"schema")
    reason = " ".join(str(entry.get("reason") or "no reason recorded").split())
    return "deploy failed: " + (reason if len(reason) <= 300
                                else reason[:297] + "...")


def assess_risk(obj: dict, *, blocked: bool = False) -> tuple[str, str]:
    """Return (level, one-sentence note) for one object."""
    if blocked:
        reason = obj.get("reason") or "cannot be migrated"
        return "HIGH", f"Cannot migrate: {reason}"
    if (obj.get("compatibility_status") == "unassessed"
            or obj.get("columns_read") == "failed"):
        # No column facts is not good column facts. Falling through below
        # rated a table whose column read timed out LOW, "structure clones
        # cleanly". The planner refuses these (`columns_unread`); this is
        # the guard for any caller that scores the record itself.
        error = obj.get("columns_read_error") or "no error text was recorded"
        return "HIGH", (f"Columns were not read, so nothing was assessed: "
                        f"{error}.")

    notes: list[str] = []
    level = "LOW"

    if obj.get("object_type") == "VIEW":
        # HIGH, not MEDIUM: an untranslated or mistranslated view CREATES
        # SUCCESSFULLY and then returns wrong numbers. A loud failure would be
        # safer than this, so it gets the higher level.
        level = "HIGH"
        notes.append("view SQL is carried over without full dialect "
                     "translation; it will create successfully even if the "
                     "semantics differ, so verify its result against the "
                     "source before anyone relies on it")

    omitted = obj.get("omitted_properties") or []
    if omitted:
        level = _raise(level, "MEDIUM")
        notes.append("source properties dropped with no AIDP equivalent: "
                     + ", ".join(omitted))

    deferred = obj.get("deferred_properties") or []
    if deferred:
        level = _raise(level, "MEDIUM")
        notes.append(
            "source maintenance/layout settings not applied on the target: "
            + ", ".join(f'{d["property"]}={d["value"]}' for d in deferred))

    warnings = obj.get("warnings") or []
    tz = [w for w in warnings if "timezone" in w.lower()]
    if tz:
        level = _raise(level, "MEDIUM")
        notes.append("timezone semantics differ for one or more columns")
    other = [w for w in warnings if w not in tz]
    if other:
        level = _raise(level, "MEDIUM")
        notes.append(f"{len(other)} column warning(s) recorded")

    for load_warning in obj.get("load_warnings") or []:
        # A pipe or task that fills this table stays behind. Named, not
        # counted: the note has to say which load to rebuild.
        level = _raise(level, "MEDIUM")
        notes.append(load_warning)

    kind_warning = obj.get("kind_warning")
    if kind_warning:
        # A TRANSIENT/TEMPORARY table planned as a permanent Delta table. The
        # sentence itself travels, not a count: the row must say what to
        # confirm.
        level = _raise(level, "MEDIUM")
        notes.append(kind_warning)

    rows = obj.get("rows")
    if rows is not None and rows >= _LARGE_ROWS:
        level = _raise(level, "MEDIUM")
        notes.append(f"{rows:,} rows: the later data phase will need its own "
                     "staging and wave plan")

    if not notes:
        notes.append("all column types mapped, no properties dropped; structure "
                     "clones cleanly")
    return level, ". ".join(n[0].upper() + n[1:] for n in notes) + "."


def pipeline_status(board: dict) -> dict:
    """What can run now, from a stage board.

    A phase is COMPLETE when it ran (or its alternative did -- for real:
    the board marks a twin SATISFIED only when the alternative is DONE, not
    a dry run, not failed and readable), found nothing needing attention,
    and was not a dry run. It is UNBLOCKED when it is not
    complete and every one of its `requires` groups has a complete member --
    so a run whose source was already extracted in AIDP (discovery workflow
    + ingest) unblocks planning without a laptop assess. `next` is the first
    unblocked phase that is not optional, in pipeline order.
    """
    rows = board.get("stages") or []
    complete = [r["stage"] for r in rows
                if r["status"] in ("DONE", "SATISFIED")
                and not r.get("attention") and not r.get("dry_run")]
    # Unblocking asks "did it run", which is not the same question as "is it
    # clean". `deps` over a manifest is `not_extracted` -- flagged on
    # purpose, because "did not look" must not read as "looked and found
    # nothing" -- and treating that flag as "did not run" would block the
    # documented ingest path forever. Attention is a review marker; it is
    # reported, and it does not stop the pipeline. An unreadable artifact
    # is neither: nothing it says can be relied on, so -- as for a twin --
    # it unblocks nothing.
    done = {r["stage"] for r in rows
            if r["status"] in ("DONE", "SATISFIED")
            and not r.get("dry_run") and not r.get("failed")
            and not r.get("unreadable")}
    unblocked, blocked = [], {}
    for r in rows:
        if r["stage"] in done:
            continue
        # A job still going is not something to start again, and a stage
        # PENDING on its running twin must not be offered alongside it.
        if r["status"] in ("RUNNING", "PENDING"):
            blocked[r["stage"]] = [r.get("found") or r["status"]]
            continue
        missing = [group for group in r.get("requires") or []
                   if not done & set(group)]
        if missing:
            blocked[r["stage"]] = [" or ".join(g) for g in missing]
        else:
            unblocked.append(r["stage"])
    optional = {r["stage"] for r in rows if r.get("optional")}
    if board.get("route") == "runbook":
        # On the runbook the board has already walked its order (S1-S12,
        # report.stages.RUNBOOK_ROUTE) and knows when the next step is a
        # job still running -- then there is nothing to start. Re-deciding
        # here from the preview order is how "Suggested next" kept naming a
        # laptop stage after the structure job had run.
        suggested = board.get("next_stage")
    else:
        suggested = next((s for s in unblocked if s not in optional),
                         unblocked[0] if unblocked else None)
    return {
        "total": len(rows),
        "complete": complete,
        "unblocked": unblocked,
        "blocked": blocked,
        "needs_attention": [r["stage"] for r in rows if r.get("attention")],
        "next": suggested,
        "waiting_on": board.get("waiting_on"),
    }
