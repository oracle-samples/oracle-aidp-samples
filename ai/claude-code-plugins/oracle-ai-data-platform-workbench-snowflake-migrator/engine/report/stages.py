"""The stage board: what is supposed to run, what has run, what it found.

Read the run before executing it. This module makes no decisions and touches
no environment -- it reads the artifacts already in `--out-dir` and reports
the shape of the pipeline against them.

Two rules it holds to, both learned the hard way elsewhere in this plugin:

  * A stage that could not look is FLAGGED, never shown as clean. "0
    exposures" and "we could not read the policy references" are opposite
    findings and must not render the same.
  * Seven stages write to AIDP, and the board says which (every STAGES
    entry with `writes: True`): `provision`, `catalog` and `deploy`, each a
    dry run without `--execute`; `structure-workflow` and `copy-workflow`,
    the in-AIDP jobs `run` starts, which have no dry run -- one creates the
    structure, the other (snowmig_02_copy_<schema>) copies rows; `publish`,
    into the workspace; and `teardown`, destructive (it stops or deletes
    the migration's clusters), both dry runs without `--execute`. The one
    further write is `smoke --write-probe --execute`:
    one probe schema, created and removed; `--write-probe` alone is a dry
    run. `notebook --upload` sends nothing -- a dry run without `--execute`,
    refused with it.
"""
from __future__ import annotations

import datetime
import json
import pathlib
import re

from plan.smoke import smoke_verdict

__all__ = ["RUNS_ON", "STAGES", "UTILITY_COMMANDS", "RUN_CASES", "run_case",
           "build_stage_board", "phase_report",
           "run_verdict", "stage_for"]

# THE ordered pipeline. Every other view of a run -- the board, the phase
# report, the diagram, the token roll-up, "what can run now" -- reads this,
# so none of them can disagree about what the phases are.
#
#   command  the CLI subcommand that runs it (`run` for an AIDP workflow)
#   phase    setup / discovery / planning / target / reporting
#   runbook  the runbook step(s) it implements, or "-" for a support stage
#   requires  groups of prerequisites; every group must have ONE of its
#            stages complete before this one is unblocked
#   alternative_to  another stage that produces the same result; having
#            done either satisfies both, so the board never stalls on the
#            path that was not taken
#   job / job_prefix  the AIDP job a `run` stage starts, or the prefix of a
#            job per schema; its artifact is then a glob, one file per job
# Where a phase's work actually executes. Traced from each command's
# transport: the catalog API is a control-plane call and uses no cluster;
# `deploy --transport sql` runs on the configured aidp.cluster_id; the
# workflows run on the migration cluster S2 provisions.
RUNS_ON = {
    "local": "operator machine (offline)",
    "local_snowflake": "operator machine -> Snowflake (read-only)",
    "local_both": "operator machine -> Snowflake + AIDP control plane",
    "control_plane": "AIDP control-plane API (no cluster)",
    "control_plane_or_configured": (
        "AIDP control-plane API; --transport sql uses the configured "
        "aidp.cluster_id"),
    "migration_cluster": "migration cluster (provisioned at S2)",
}

STAGES: tuple[dict, ...] = (
    # Optional only in the sense that a run can skip it; skipping it is how
    # a wrong host or role costs hours later.
    {"stage": "preflight", "requires": [], "runs_on": RUNS_ON["local_both"], "command": "preflight", "phase": "setup",
     "runbook": "-", "needs": "a connection config", "writes": False,
     "optional": True, "artifact": "preflight.json",
     "purpose": "read the connection config back to the user, field by "
                "field, and test both ends before anything else runs"},
    {"stage": "assess", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "assess", "phase": "discovery",
     "runbook": "S7 (views)", "alternative_to": "ingest", "needs": "Snowflake", "writes": False,
     "artifact": "inventory.json",
     "purpose": "inventory tables and views, plus a census of everything that "
                "is not one"},
    # Optional: the in-AIDP path (S6 -> S7) instead of a laptop assess.
    {"stage": "ingest", "requires": [['discover-workflow']], "runs_on": RUNS_ON["local"], "command": "ingest", "phase": "discovery",
     "runbook": "S7", "alternative_to": "assess", "needs": "the S6 discovery manifest", "writes": False,
     "optional": True, "artifact": "ingest_result.json",
     "purpose": "bridge the in-AIDP discovery manifest into inventory.json "
                "with the same type mapper assess uses"},
    {"stage": "deps", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "deps", "phase": "discovery", "runbook": "-",
     "needs": "Snowflake", "writes": False,
     "artifact": "dependencies.json",
     "purpose": "lineage, so views land after their base tables"},
    {"stage": "maintenance", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "maintenance", "phase": "discovery",
     "runbook": "-", "needs": "Snowflake", "writes": False,
     "artifact": "maintenance.json",
     "purpose": "clustering, retention and churn — who inherits OPTIMIZE/VACUUM"},
    {"stage": "security", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "security", "phase": "discovery",
     "runbook": "-", "needs": "Snowflake", "writes": False,
     "artifact": "security.json",
     "purpose": "masking/row-access/aggregation/projection policies, tag "
                "attachments, secure views, grants — what arrives "
                "unprotected"},
    {"stage": "compute", "requires": [], "runs_on": RUNS_ON["local_snowflake"], "command": "compute", "phase": "discovery",
     "runbook": "S12", "needs": "Snowflake", "writes": False,
     "artifact": "compute.json",
     "purpose": "warehouse-to-cluster sizing proposal"},
    # Optional: it feeds `plan` when run, but `plan` runs without it, so the
    # board must not stall on it as "next".
    {"stage": "data-options", "requires": [], "runs_on": RUNS_ON["local"], "command": "data-options", "phase": "planning",
     "runbook": "S11", "needs": "nothing (offline)", "writes": False,
     "optional": True,
     "artifact": "data_options.json",
     "purpose": "the data-movement architecture options — presented, never chosen"},
    {"stage": "plan", "requires": [['assess', 'ingest'], ['deps']], "runs_on": RUNS_ON["local"], "command": "plan", "phase": "planning",
     "runbook": "S7-S9", "needs": "nothing (offline)", "writes": False,
     "artifact": "plan.json",
     "purpose": "what can migrate, in what order, to which target name"},
    {"stage": "ddl", "requires": [['plan']], "runs_on": RUNS_ON["local"], "command": "ddl", "phase": "planning", "runbook": "S7",
     "needs": "nothing (offline)", "writes": False,
     "artifact": "ddl_plan.json",
     "purpose": "the statements/bodies that would create the structure"},
    {"stage": "smoke", "requires": [], "runs_on": RUNS_ON["control_plane"], "command": "smoke", "phase": "target", "runbook": "-",
     "needs": "Snowflake + AIDP", "writes": False,
     "artifact": "smoke.json",
     "purpose": "connectivity and permissions on both ends"},
    # Optional: required for the in-AIDP data path, not for a structure-only
    # clone, so the board must not stall on it as "next".
    {"stage": "provision", "requires": [], "runs_on": RUNS_ON["control_plane"], "command": "provision", "phase": "setup",
     "runbook": "S1 S2 S5", "needs": "AIDP", "writes": True, "optional": True,
     "artifact": "provision_result.json",
     "purpose": "workspace, migration-assets cluster, the backup-snowflake-"
                "migration/ folder with scripts + plan, and the four "
                "migration jobs. Dry-run unless --execute"},
    {"stage": "catalog", "requires": [], "runs_on": RUNS_ON["control_plane"], "command": "catalog", "phase": "setup",
     "runbook": "S3 S4", "needs": "AIDP", "writes": True,
     "artifact": "catalog_result.json",
     "purpose": "register the target catalog — EXTERNAL/SNOWFLAKE by default, "
                "a read-only pointer that copies nothing. Dry-run unless "
                "--execute"},
    {"stage": "discover-workflow", "requires": [['provision']], "runs_on": RUNS_ON["migration_cluster"], "command": "run", "job": "snowmig_00_discover",
     "phase": "discovery", "runbook": "S6", "needs": "AIDP (provisioned)",
     "writes": False, "optional": True,
     "artifact": "run_snowmig_00_discover.json",
     "purpose": "discovery as an AIDP workflow: two INFORMATION_SCHEMA "
                "queries, a manifest written in the workspace"},
    {"stage": "structure-workflow", "requires": [['ddl'], ['provision']], "runs_on": RUNS_ON["migration_cluster"], "command": "run",
     "job": "snowmig_01_structure", "phase": "target", "runbook": "S10",
     "needs": "AIDP (provisioned) + an approved ddl_plan.json",
     "writes": True, "optional": True, "alternative_to": "deploy",
     "artifact": "run_snowmig_01_structure.json",
     "purpose": "create schemas, empty tables and views by workflow, from "
                "the approved plan on the workspace"},
    {"stage": "deploy", "requires": [['ddl'], ['catalog']], "runs_on": RUNS_ON["control_plane_or_configured"], "command": "deploy", "phase": "target",
     "runbook": "S10 (catalog API)", "needs": "AIDP", "writes": True,
     "alternative_to": "structure-workflow",
     "artifact": "deploy_result.json",
     "purpose": "create schemas, tables and views in a STANDARD catalog. "
                "Refuses an EXTERNAL target. Dry-run unless --execute"},
    # Registered by provision and NEVER run by the migrator: moving rows is
    # the customer's later decision. Once a plan is pushed there is one job
    # per schema, snowmig_02_copy_<schema> (target.provisioning's
    # COPY_JOB_PREFIX), and no generic snowmig_02_copy_schema job; each run
    # writes run_<job>.json, so the stage is every artifact the prefix names.
    {"stage": "copy-workflow", "requires": [['structure-workflow', 'deploy'], ['data-options']], "runs_on": RUNS_ON["migration_cluster"], "command": "run", "job": "snowmig_02_copy_schema",
     "job_prefix": "snowmig_02_copy_",
     "phase": "target", "runbook": "S11", "needs": "an architecture decision",
     "writes": True, "optional": True,
     "artifact": "run_snowmig_02_copy_*.json",
     "purpose": "copy one schema's rows, one job per schema "
                "(snowmig_02_copy_<schema>). Registered, never run by the "
                "migrator"},
    {"stage": "reconcile-workflow", "requires": [['copy-workflow']], "runs_on": RUNS_ON["migration_cluster"], "command": "run",
     "job": "snowmig_03_reconcile", "phase": "target", "runbook": "S11",
     "needs": "a copy that ran", "writes": False, "optional": True,
     "artifact": "run_snowmig_03_reconcile.json",
     "purpose": "compare source and target counts after a copy"},
    {"stage": "notebook", "requires": [['ddl']], "runs_on": RUNS_ON["local"], "command": "notebook", "phase": "target",
     "runbook": "-", "needs": "nothing (offline)", "writes": False,
     "artifact": "NOTEBOOK.md",
     "purpose": "the clone as an executable AIDP notebook"},
    {"stage": "summary", "requires": [['plan']], "runs_on": RUNS_ON["local"], "command": "summary", "phase": "reporting",
     "runbook": "S9 S12", "needs": "nothing (offline)", "writes": False,
     "artifact": "SUMMARY.md",
     "purpose": "per-object roll-up: rows, risk, migration status, the "
                "translation map and the token cost"},
    {"stage": "publish", "requires": [['summary']], "runs_on": RUNS_ON["control_plane"],
     "command": "publish", "phase": "reporting", "runbook": "-",
     "needs": "AIDP (the migration workspace)", "writes": True,
     "optional": True, "artifact": "publish_result.json",
     "purpose": "copy the finished report, inputs and outputs, into the "
                "workspace's reports/final-<UTC> folder, each file read back. "
                "Dry-run unless --execute"},
    {"stage": "tokens", "requires": [], "runs_on": RUNS_ON["local"], "command": "tokens", "phase": "reporting",
     "runbook": "-", "needs": "nothing (local files)", "writes": False,
     "optional": True, "artifact": "tokens.json",
     "purpose": "LLM token usage per stage and phase"},
    # Last: the compute is released once the structure exists and the run's
    # record is written. Stop (reversible) unless delete is asked for.
    {"stage": "teardown", "requires": [['structure-workflow', 'deploy']],
     "runs_on": RUNS_ON["control_plane"], "command": "teardown",
     "phase": "teardown", "runbook": "-", "needs": "AIDP (provisioned)",
     "writes": True, "optional": True, "artifact": "teardown_result.json",
     "purpose": "terminate the clusters this migration allocated (stop by "
                "default, delete when asked); the workspace, catalogs and "
                "jobs are kept -- unless --scope credential (the Snowflake "
                "credential only) or --scope all (everything the migration "
                "created) is asked for. Dry-run unless --execute"},
)

# CLI commands that are tools, not steps of a migration. Every other command
# must be a phase above -- a test holds that.
UTILITY_COMMANDS = ("stages", "demo", "databases", "catalogs", "clean",
                    "build-notebooks", "init-config", "fetch",
                    # Optional reports for objects the plan does not copy,
                    # and the generated jobs (GENERATED_JOBS.md): they gate
                    # nothing, so they are not board phases.
                    "external-registration", "share-plan", "jobs")

_UNKNOWN = "could not be determined"


def _artifact_paths(out_dir: pathlib.Path, name: str) -> list[pathlib.Path]:
    """The files a stage's artifact names: one, or every match of a glob."""
    if "*" in name:
        return sorted(p for p in out_dir.glob(name) if p.is_file())
    path = out_dir / name
    return [path] if path.exists() else []


def _load(out_dir: pathlib.Path, name: str):
    if "*" in name:
        # One artifact per invocation target (run_<job>.json): the stage has
        # run if any exists, and each is reported. A record that does not
        # name its job is named by its file, never left as "?".
        paths = _artifact_paths(out_dir, name)
        if not paths:
            return None
        many = []
        for p in paths:
            run = _load(out_dir, p.name)
            if isinstance(run, dict) and not run.get("job"):
                run = {**run, "job": run.get("job_key")
                       or p.stem.removeprefix("run_")}
            many.append(run)
        return {"_many": many}
    path = out_dir / name
    if not path.exists():
        return None
    if path.suffix != ".json":
        return {"_exists": True}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"_unreadable": True}


def _expected_jobs(out_dir: pathlib.Path, spec: dict) -> dict[str, str] | None:
    """The jobs a per-job stage must have run, from what provision REGISTERED.

    For the copy stage that is `copy_jobs` in provision_result.json -- one job
    per schema of the approved plan. Without it the run files that happen to
    exist were the whole answer, so one schema's SUCCESS read as the copy
    done while the others had never run. {job: registration status} (empty
    status for a record written before it was kept); None when there is no
    such record.
    """
    if not spec.get("job_prefix"):
        return None
    record = _load(out_dir, "provision_result.json")
    # A dry run registered nothing, so it sets no expectation.
    if (not isinstance(record, dict) or record.get("_unreadable")
            or record.get("dry_run")):
        return None
    jobs = {str(j.get("job")): str(j.get("status") or "")
            for j in record.get("copy_jobs") or []
            if isinstance(j, dict) and j.get("job")}
    return jobs or None


def _load_stage(out_dir: pathlib.Path, spec: dict):
    """A stage's artifact, with every registered job that has NOT run
    listed as such -- so "some ran" can never read as "all ran"."""
    data = _load(out_dir, spec["artifact"])
    expected = _expected_jobs(out_dir, spec)
    if not expected or not isinstance(data, dict) or "_many" not in data:
        return data
    seen = {r.get("job") for r in data["_many"] if isinstance(r, dict)}
    missing = [{"job": job, "_not_run": True, "registration": status}
               for job, status in expected.items() if job not in seen]
    return {"_many": data["_many"] + missing} if missing else data


def cold_start_outcome(exhausted: dict) -> str:
    """What the last cancel of an exhausted cold start really established.

    "cancelled" only when the run read back CANCELED: then nothing ran. Any
    other terminal state means the run ended on its own between the pick-up
    check and the cancel -- it DID run, and "nothing ran, re-run" would
    repeat its work (twice the rows, for an append copy). A cancel that
    reached no terminal state leaves a run that may still start.
    Returns "cancelled", "ended", or "unconfirmed". One reading for the
    console, RUN.md and the board.
    """
    state = str((exhausted or {}).get("cancel_state") or "")
    if state == "CANCELED":
        return "cancelled"
    from target.jobs import TERMINAL_STATES
    return "ended" if state in TERMINAL_STATES else "unconfirmed"


# Every way a job-run record can read, in the ONE order they are checked.
RUN_CASES = ("unreadable", "cold_start_exhausted", "unrecognised",
             "cancel_unconfirmed", "still_running", "success", "failed")


def run_case(run: dict) -> str:
    """Which of RUN_CASES a job-run record is. The single decision behind
    the console's exit branches (cmd_run), RUN.md (_render_run) and the
    board (run_verdict): each words the case its own way, none re-decides
    it. Three hand-kept copies of this order are how round 3's verdicts
    became dead code on the board without a test noticing.

    A record written before `terminal` existed is read as terminal.
    """
    if run.get("status_unreadable"):
        return "unreadable"
    if not run.get("terminal", True):
        if run.get("cold_start_exhausted"):
            return "cold_start_exhausted"
        if run.get("unrecognised"):
            return "unrecognised"
        if run.get("cancel_unconfirmed"):
            return "cancel_unconfirmed"
        return "still_running"
    return "success" if run.get("ok") else "failed"


def run_verdict(run: dict) -> tuple[str, str]:
    """(verdict, kind) for one job-run record -- the ONE reading of a run,
    for every workflow row and the phase report alike.

    Checked in the order cmd_run and RUN.md check them, so the three never
    disagree: a status that could not be read, the cold-start attempts
    exhausted (nothing ran), a state this plugin does not classify, a
    cancel that never confirmed (nothing was resubmitted), a poll budget
    that ran out with the job going (STILL RUNNING, never rounded to a
    verdict), then the terminal status. Only a restart that actually
    resubmitted (`new_run` set) is counted as one.

    kind is success, failed, running, unknown or pending (a registered job
    with no run recorded). A record written before `terminal` existed is
    read as terminal.
    """
    if run.get("_not_run"):
        reg = run.get("registration") or ""
        if "not registered" in reg:
            return (f"NOT RUN — **{reg}**: provision could not register it",
                    "pending")
        return ("NOT RUN — registered, no run recorded", "pending")
    status = run.get("status") or _UNKNOWN
    resubmitted = sum(1 for r in run.get("restarts") or []
                      if isinstance(r, dict) and r.get("new_run"))
    after = (f" after {resubmitted} cold-start restart(s)"
             if resubmitted else "")
    case = run_case(run)
    if case == "unreadable":
        return ("**STATUS UNREADABLE** — the run was submitted and may still "
                "be going; check it in the console before starting another",
                "unknown")
    if case == "cold_start_exhausted":
        tried = len(run.get("restarts") or []) + 1
        ex = run["cold_start_exhausted"]
        outcome = cold_start_outcome(ex)
        if outcome == "cancelled":
            return (f"**COLD START — none of {tried} run(s) was picked up; "
                    "nothing ran**", "failed")
        if outcome == "ended":
            return (f"**COLD START race — the last run ended "
                    f"{ex.get('cancel_state')} before the cancel landed; it "
                    "RAN: check its output before any re-run**", "unknown")
        return ("**COLD START — last run NOT confirmed cancelled; it may "
                "still run**", "unknown")
    if case == "unrecognised":
        return (f"**UNRECOGNISED STATE {status}**", "unknown")
    if case == "cancel_unconfirmed":
        return ("**cold start suspected; cancel unconfirmed — nothing was "
                "resubmitted**" + after, "unknown")
    if case == "still_running":
        return ("STILL RUNNING" + after, "running")
    if case == "success":
        return ((run.get("status") or "SUCCESS") + after, "success")
    return (f'**{run.get("status") or "FAILED"}**' + after, "failed")


def _finding(stage: str, data: dict) -> tuple[str, bool]:
    """(what it found, needs attention)."""
    if data.get("_unreadable"):
        return ("artifact present but unreadable", True)
    if data.get("_exists"):
        return ("written", False)

    if stage == "assess":
        counts = data.get("counts_by_type") or {}
        census = data.get("census") or {}
        text = (f'{data.get("object_count", "?")} object(s) '
                f'({", ".join(f"{v} {k.lower()}" for k, v in sorted(counts.items())) or "none"})')
        if census:
            text += f'; {census.get("total", 0)} non-table/view object(s) that cannot migrate'
        notes = data.get("extraction_notes") or []
        if notes:
            return (text + f'; **{len(notes)} scope(s) unreadable**', True)
        return (text, False)

    if stage == "deps":
        # `cycles` lives in plan.json, never here. What dependencies.json
        # does say is WHERE the graph came from: account_usage is the full
        # graph; parsed_ddl is partial (view DDL only); account_usage_empty
        # and account_usage+parsed_ddl mean ACCOUNT_USAGE was readable but
        # lagged behind the DDL for some or all views, whose DDL was parsed
        # instead; not_extracted is "did not look" and must not read as
        # clean. A value the board does not know is flagged, not guessed at.
        source = data.get("source_used")
        edges = len(data.get("edges") or [])
        if not source:
            # Both producers write the key. Without it, how the graph was got
            # is unknown, and "view DDL only" would be a guess about it.
            return (f'lineage source not recorded; {edges} edge(s) -- '
                    '**completeness unknown**', True)
        text = f'lineage from {source}; {edges} edge(s)'
        if source == "not_extracted":
            return (text + " -- **NOT extracted; view order unchecked**", True)
        unresolved = len(data.get("unresolved_references") or [])
        tail = f', {unresolved} unresolved reference(s)' if unresolved else ''
        if source == "account_usage":
            return (text, False)
        if source == "parsed_ddl":
            return (text + " -- **partial graph (view DDL only)**" + tail, True)
        if source in ("account_usage+parsed_ddl", "account_usage_empty"):
            # The producer writes a per-run warning naming the lagged views
            # and the ones still unordered; surface it rather than restate
            # a weaker version.
            missing = len(data.get("views_without_account_usage_edge") or [])
            note = data.get("warning") or (
                f"{missing} view(s) had no ACCOUNT_USAGE edge; their DDL was "
                "parsed (OBJECT_DEPENDENCIES lags DDL up to ~3 h) -- re-run "
                "deps before relying on the wave order")
            return (text + f" -- **{note}**" + tail, True)
        return (text + " -- **provenance not recognised; completeness unknown**",
                True)

    if stage == "maintenance":
        flagged = data.get("objects_with_signals")
        readable = (data.get("account_usage") or {}).get("readable", True)
        text = f'{flagged} of {len(data.get("tables") or [])} table(s) need a maintenance decision'
        if not readable:
            return (text + "; **ACCOUNT_USAGE unreadable — churn NOT measured**",
                    True)
        return (text, bool(flagged))

    if stage == "security":
        count = data.get("exposure_count")
        secure = len(data.get("secure_views") or [])
        if count is None:
            return ("**policy attachments unreadable — exposure UNKNOWN, "
                    "not zero**", True)
        text = f'{count} policy exposure(s), {secure} secure view(s)'
        # A policy object that exists while POLICY_REFERENCES (which lags
        # ~2 h) lists no attachment is UNCONFIRMED, not zero; and an empty
        # attachment list is uncorroborated when SHOW MASKING/ROW ACCESS
        # POLICIES was denied. `readable` defaults to True so an artefact
        # written before the `policies` block existed stays unflagged.
        unattached = data.get("policies_defined_without_attachment") or 0
        if unattached:
            return (text + f'; **{unattached} policy object(s) defined, '
                    'attachment UNCONFIRMED (ACCOUNT_USAGE.POLICY_REFERENCES '
                    'lags ~2 h)**', True)
        pol = data.get("policies") or {}
        denied = [k for k in ("masking", "row_access", "aggregation",
                              "projection")
                  if not (pol.get(k) or {}).get("readable", True)]
        if denied:
            return (text + f'; **{", ".join(denied)} policy objects could not '
                    'be enumerated — empty attachment list uncorroborated**',
                    True)
        # Tag attachments are a separate ACCOUNT_USAGE view with the same
        # failure mode: unreadable is UNKNOWN, never "no tags attached".
        tags = data.get("tag_references")
        if tags is not None and not tags.get("measured", True):
            return (text + '; **tag attachments unreadable — classification '
                    'UNKNOWN, not zero**', True)
        return (text, bool(count or secure))

    if stage == "compute":
        # compute.json is what propose_all writes: `proposals` and `blocked`.
        # A warehouse with no proposal is a decision still owed, not clean.
        blocked = len(data.get("blocked") or [])
        text = f'{len(data.get("proposals") or [])} warehouse(s) sized'
        if blocked:
            return (text + f', **{blocked} blocked (no shape proposed)**', True)
        return (text, False)

    if stage == "plan":
        s = data.get("summary") or {}
        # An object the operator's restrictions left out is a scope choice
        # (S9), not a failure to migrate: "4 can migrate, 998 cannot" with a
        # warning read a 4-table canary as 998 problems.
        by_cat = s.get("cannot_by_category") or {}
        scoped_out = by_cat.get("restriction", 0) if isinstance(by_cat, dict) else 0
        cannot = s.get("cannot_migrate")
        if isinstance(cannot, int) and scoped_out:
            rest = cannot - scoped_out
            text = (f'{s.get("can_migrate", "?")} can migrate, '
                    f'{scoped_out} left out by restrictions'
                    + (f', {rest} cannot' if rest else ""))
            return (text, bool(rest))
        text = (f'{s.get("can_migrate", "?")} can migrate, '
                f'{s.get("cannot_migrate", "?")} cannot')
        return (text, bool(s.get("cannot_migrate")))

    if stage == "ddl":
        blocked = len(data.get("blocked") or [])
        return (f'{len(data.get("statements") or [])} object(s) to create, '
                f'{blocked} blocked', bool(blocked))

    if stage == "smoke":
        verdict = smoke_verdict(data)
        if verdict == "PASS":
            return ("PASS", False)
        if verdict == "PARTIAL":
            return ("**PARTIAL** — destination not checked", True)
        return ("**FAIL**", True)

    if stage == "preflight":
        cfg = data.get("config") or {}
        failed, skipped = data.get("failed", 0), data.get("skipped", 0)
        text = (f'{len(cfg.get("fields") or [])} field(s) echoed; '
                f'{failed} check(s) failed, {skipped} skipped')
        if cfg.get("missing"):
            return (text + f' — **missing: {", ".join(cfg["missing"])}**', True)
        return (text, bool(failed))

    if stage == "provision":
        if data.get("dry_run"):
            return ("DRY RUN — nothing was provisioned", False)
        steps = data.get("steps") or []
        bad = [s for s in steps if s.get("verified") is False]
        # None in execute mode is "not confirmed, not assumed" (a library
        # install awaiting the restart, a folder create that may have hit
        # an existing one). Pending is pending; it does not read as clean.
        pending = [s for s in steps if s.get("verified") is None]
        text = (f'{len(steps)} step(s); workspace '
                f'{(data.get("workspace") or {}).get("name", "?")}')
        if bad:
            text += f' — **{len(bad)} failed/unverified**'
        if pending:
            text += f' — **{len(pending)} not confirmed**'
        return (text, bool(bad or pending))

    if stage == "catalog":
        if data.get("dry_run"):
            return (f'DRY RUN — {data.get("catalog", "?")} '
                    f'({data.get("catalog_type", "?")}) would be registered; '
                    f'nothing was', False)
        # Every catalog the migration registered (S3 EXTERNAL, S4 INTERNAL),
        # not only the last one run: the board used to show just the
        # INTERNAL container once S4 ran, and never a failed connection test.
        recorded = data.get("catalogs_recorded") or [data]
        parts, attention = [], False
        for c in recorded:
            action = c.get("action", _UNKNOWN)
            text = (f'{c.get("catalog", "?")} '
                    f'({c.get("catalog_type", "?")}): {action}')
            if action == "create_requested":
                # The create was accepted but the catalog never became
                # visible. Pending is pending; it must not read as success.
                text += " — **requested, never became visible**"
                attention = True
            test = c.get("test_connection") or {}
            status = str(test.get("status") or "").upper()
            if status and status not in ("SUCCEEDED", "SUCCESS"):
                reason = str(test.get("error") or "").strip()
                empty = (not reason or reason.rstrip(":").strip().lower()
                         in ("test connection failed", "failed"))
                if status == "FAILED" and empty:
                    # The known platform issue (runbook S3): shown, and not
                    # a stop -- the connector proves the credential at S6.
                    text += ", connection test FAILED with an empty reason"
                else:
                    text += f", **connection test {status}**"
                    attention = True
            parts.append(text)
        return ("; ".join(parts), attention)

    if stage == "deploy":
        if data.get("dry_run"):
            return (f'DRY RUN — {data.get("statement_count", 0)} object(s) '
                    f'would be created; nothing was', False)
        verified = data.get("verified", 0)
        total = data.get("statement_count", 0)
        # Every outcome the deploy buckets, so the row adds up to the
        # statement count. The default transport (catalog_api) is the one
        # that produces "exists but its structure could not be read" and
        # derived type drift; neither was counted, so a board could read
        # "verified 4/7" with no warning and three tables unverified.
        bad = (len(data.get("failed") or [])
               + len(data.get("mismatched_targets") or []))
        unverified = len(data.get("unverified_structure_targets") or [])
        drift = len(data.get("derived_type_drift_targets") or [])
        errors = (len(data.get("errors") or [])
                  + len(data.get("chunk_errors") or []))
        text = f'**verified {verified}/{total}**'
        if bad:
            text += f', {bad} failed/mismatched'
        if unverified:
            text += f', {unverified} structure not verified'
        if drift:
            text += f', {drift} created with derived type drift'
        if errors:
            text += f', {errors} error(s)'
        return (text, bool(bad or unverified or drift or errors))

    if "_many" in data:
        # The last recorded result per job (one job per schema).
        parts, attention = [], False
        for run in data.get("_many") or []:
            if run.get("_unreadable"):
                parts.append("a run artifact is unreadable")
                attention = True
                continue
            verdict, kind = run_verdict(run)
            attention = attention or kind != "success"
            parts.append(f'{run.get("job") or run.get("job_key") or "?"}: '
                         f'{verdict}')
        return ("; ".join(parts) or "written", attention)

    if stage == "data-options":
        choice = data.get("choice")
        return (("architecture recorded" if choice else
                 "options presented; none chosen"), False)

    if "run_key" in data or stage.endswith("-workflow"):
        verdict, kind = run_verdict(data)
        return (f"job run {verdict}", kind != "success")

    return ("written", False)


def _satisfies(row: dict | None) -> bool:
    """Whether a stage's row did the work its twin would have done: it ran,
    for real, did not fail, and its artifact could be read. Existence alone
    is not enough -- a dry run, a failed or unrecognised job run and an
    unreadable artifact all exist and created nothing that can be relied
    on."""
    return bool(row and row["status"] == "DONE" and not row.get("dry_run")
                and not row.get("failed") and not row.get("unreadable"))


def _unsatisfied(twin: dict) -> str:
    if twin.get("dry_run"):
        return f"not satisfied: `{twin['stage']}` was a dry run"
    if twin.get("unreadable"):
        return f"not satisfied: `{twin['stage']}` artifact unreadable"
    return f"not satisfied: `{twin['stage']}` {twin['found']}"


# The runbook's order (overview skill, S1-S12), for the board's "next" once
# a migration is on it. The STAGES order is the laptop-preview order, which
# put `assess` and then `maintenance` first: live (2026-09-29), after every
# runbook step up to the structure job, the board still suggested a laptop
# read the runbook says does not count. The copy is deliberately absent: it
# is the customer's decision and never proposed on their behalf; reconcile
# is proposed only after a copy has run.
RUNBOOK_ROUTE = ("provision", "catalog", "discover-workflow", "ingest",
                 "plan", "ddl", "structure-workflow", "compute")


def _inventory_from_ingest(data) -> bool:
    """inventory.json is written by `assess` (a laptop read) AND by `ingest`
    (the in-AIDP manifest). Only the first is `assess` having run; reading
    the second as it made the board say DONE for a stage nobody ran."""
    session = data.get("session") if isinstance(data, dict) else None
    return (isinstance(session, dict)
            and "discovery workflow" in str(session.get("source") or ""))


def _route_mode(ran: dict) -> bool:
    """On the runbook route unless this is a laptop preview: a live `assess`
    ran and no provision has been executed. With neither, a migration starts
    at S1 (`provision`)."""
    prov = ran.get("provision")
    provisioned = bool(prov and not prov.get("dry_run")
                       and prov["status"] == "DONE")
    return provisioned or "assess" not in ran


def _route_next(rows: list[dict]) -> tuple[str | None, str | None]:
    """(next, waiting_on) along RUNBOOK_ROUTE: the first route stage not yet
    done, or -- when that stage is still running -- nothing to start, and
    the stage to wait for. Reconcile follows a copy the operator ran."""
    by = {r["stage"]: r for r in rows}
    finished = ("DONE", "SATISFIED")
    for stage in RUNBOOK_ROUTE:
        row = by.get(stage)
        if row is None or (row["status"] in finished
                           and not row.get("dry_run")):
            continue
        if row["status"] in ("RUNNING", "PENDING"):
            return None, stage
        return stage, None
    copy, rec = by.get("copy-workflow"), by.get("reconcile-workflow")
    if copy and copy["status"] in ("RUNNING", "PARTIAL"):
        return None, "copy-workflow" if copy["status"] == "RUNNING" else None
    if (copy and copy["status"] == "DONE" and rec
            and rec["status"] not in finished):
        if rec["status"] in ("RUNNING", "PENDING"):
            return None, "reconcile-workflow"
        return "reconcile-workflow", None
    return None, None


def build_stage_board(out_dir) -> dict:
    out_dir = pathlib.Path(out_dir)
    rows: list[dict] = []
    next_stage = None
    # First every stage that wrote an artifact, so a twin is judged by what
    # its artifact says, not by the file being there.
    ran = {}
    for spec in STAGES:
        data = _load_stage(out_dir, spec)
        if spec["stage"] == "assess" and _inventory_from_ingest(data):
            continue
        if data is not None:
            ran[spec["stage"]] = _done_row(spec, data)
    for spec in STAGES:
        if spec["stage"] in ran:
            rows.append(ran[spec["stage"]])
            continue
        twin = ran.get(spec.get("alternative_to"))
        if twin and twin["status"] == "RUNNING":
            # The alternative is still going (or its state is unknown): it
            # may be creating exactly what this stage would. Offering this
            # one as next would be a second, concurrent write of the same
            # objects -- live, S10's poll budget ran out while the job went
            # on to SUCCESS. Wait for the twin, never run past it.
            rows.append({**spec, "status": "PENDING",
                         "found": f"pending on `{twin['stage']}`: "
                                  f"{twin['found']}",
                         "attention": True})
            continue
        if _satisfies(twin):
            rows.append({**spec, "status": "SATISFIED",
                         "found": f"satisfied by `{twin['stage']}`",
                         "attention": False})
            continue
        rows.append({**spec, "status": "NOT_RUN",
                     "found": _unsatisfied(twin) if twin else "—",
                     # A twin that failed or could not be read is a finding;
                     # a dry run is only a note.
                     "attention": bool(twin and not twin.get("dry_run"))})
        # An optional stage that has not run is not "next": the pipeline
        # proceeds without it.
        if next_stage is None and not spec.get("optional"):
            next_stage = spec["stage"]
    route = _route_mode(ran)
    waiting_on = None
    if route:
        next_stage, waiting_on = _route_next(rows)
    return {"out_dir": str(out_dir), "stages": rows, "next_stage": next_stage,
            "route": "runbook" if route else "preview",
            "waiting_on": waiting_on,
            "needs_attention": [r["stage"] for r in rows if r["attention"]]}


def _run_kinds(spec: dict, data) -> list[str]:
    """run_verdict's kind for every job-run record behind a workflow row."""
    if not spec.get("job") or not isinstance(data, dict):
        return []
    return [run_verdict(r)[1] for r in _job_runs(data)
            if not r.get("_unreadable")]


def _done_row(spec: dict, data) -> dict:
    found, attention = _finding(spec["stage"], data)
    kinds = _run_kinds(spec, data)
    # A job run that is still going, or whose state is not established, did
    # not finish: it is RUNNING, not DONE -- and not "failed" either, which
    # is what `ok: false` on an unfinished run used to make it. A registered
    # job with no run makes a per-job stage PARTIAL.
    if "failed" in kinds:
        status = "DONE"
    elif "running" in kinds or "unknown" in kinds:
        status = "RUNNING"
    elif "pending" in kinds:
        status = "PARTIAL"
    else:
        status = "DONE"
    return {**spec, "status": status, "found": found,
           "attention": attention,
           # A caveat and a failure both raise `attention`, and
           # they are not the same for what comes next: `deps`
           # over a manifest is flagged `not_extracted` on
           # purpose and planning still proceeds, while a job run
           # that answered `ok: false` did not do its work. Only
           # the second one blocks.
           "failed": (("failed" in kinds) if kinds else
                      bool(isinstance(data, dict)
                           and (data.get("ok") is False
                                or data.get("failed")))),
           # Present and unreadable: nothing it says can be relied
           # on, so it satisfies no twin.
           "unreadable": bool(isinstance(data, dict)
                              and (data.get("_unreadable")
                                   or any(isinstance(r, dict)
                                          and r.get("_unreadable")
                                          for r in data.get("_many")
                                          or []))),
           # A dry run wrote its artifact and created nothing, so
           # it satisfies no prerequisite.
           "dry_run": bool(isinstance(data, dict)
                           and data.get("dry_run"))}


def stage_for(command: str, job: str | None = None) -> str:
    """The phase a CLI invocation belongs to. A workflow run is named by its
    job, so S6 and S10 are logged as themselves rather than as a flat `run`."""
    for spec in STAGES:
        if spec["command"] != command:
            continue
        if spec.get("job") is None and command != "run":
            return spec["stage"]
        if job and spec.get("job") == job:
            return spec["stage"]
        # A job per schema (snowmig_02_copy_<schema>) is the same stage.
        if job and spec.get("job_prefix") and job.startswith(spec["job_prefix"]):
            return spec["stage"]
    return command


def _ts(value) -> datetime.datetime | None:
    if not value:
        return None
    ts = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)


def _legacy_run_stage(run: dict, out_dir: pathlib.Path) -> str:
    """A bare `run` logged before job names were recorded: attribute it to
    the workflow whose artifact was written inside that run's window."""
    lo, hi = _ts(run.get("started_at")), _ts(run.get("ended_at"))
    if not lo or not hi:
        return "run"
    for spec in STAGES:
        if not spec.get("job"):
            continue
        for art in _artifact_paths(out_dir, spec["artifact"]):
            if not art.is_file():
                continue
            written = datetime.datetime.fromtimestamp(
                art.stat().st_mtime, datetime.timezone.utc)
            if lo <= written <= hi + datetime.timedelta(seconds=5):
                return spec["stage"]
    return "run"


# `RETRY ...` as the notebooks print it, after their `[copy] ` log prefix.
_RETRY_LINE = re.compile(r"^\s*(?:\[[\w-]+\]\s+)?RETRY\s")


def _job_runs(art) -> list[dict]:
    """The job-run records behind a workflow artifact: one, or one per job."""
    if not isinstance(art, dict):
        return []
    many = art.get("_many")
    runs = many if many is not None else [art]
    return [r for r in runs if isinstance(r, dict)]


# A run logged with no exit code raised out of main() or was interrupted
# (Ctrl-C): it neither passed nor failed, and it is never rounded to either.
UNKNOWN_RESULT = "UNKNOWN (no exit code: crashed or interrupted)"


def _verdict(code) -> str:
    if code is None:
        return UNKNOWN_RESULT
    return {0: "PASS", 3: "HALT"}.get(int(code), "FAIL")


def _job_result(art) -> str | None:
    """The phase-report result a workflow artifact decides, else None.

    FAIL when a job did not do its work, UNKNOWN when its state is not
    established (unreadable, unrecognised, cancel unconfirmed), STILL
    RUNNING when the poll budget ran out with it going -- the same reading
    as the board (run_verdict). Applies whether or not the run was logged.
    A workflow's exit code is not the finer statement: cmd_run exits 0 for
    a run still going and 1 for one it cannot classify."""
    runs = _job_runs(art)
    if not runs:
        return None
    many = "_many" in art
    graded = []
    for run in runs:
        if run.get("_unreadable"):
            graded.append((run, "artifact unreadable", "unknown"))
            continue
        verdict, kind = run_verdict(run)
        # "STILL RUNNING (job STILL RUNNING)" says nothing: name the status.
        graded.append((run, (str(run.get("status") or _UNKNOWN)
                             if kind == "running"
                             else verdict.replace("**", "")), kind))
    for kind, head in (("failed", "FAIL"), ("unknown", "UNKNOWN"),
                       ("running", "STILL RUNNING"), ("pending", "PARTIAL")):
        hits = [(r, v) for r, v, k in graded if k == kind]
        if hits:
            # One job per schema: name the ones behind the verdict.
            return f"{head} (" + ", ".join(
                (f'job {r.get("job") or "?"} {v}' if many else f"job {v}")
                for r, v in hits) + ")"
    return None


def _phase_summary(rows: list[dict]) -> list[dict]:
    """Stage rows rolled up into their phases, in pipeline order.

    A phase FAILS if any of its stages' last run failed or halted; it is
    UNKNOWN if any stage's outcome was not established (a crash or an
    interrupt logs no exit code) -- never PASS; STILL RUNNING while a job
    it started is still going; it is NOT_RUN while any
    required stage in it has not run; SKIPPED when every stage in it is
    optional and none ran; otherwise it PASSES."""
    out = []
    for phase in dict.fromkeys(r["phase"] for r in rows):
        mine = [r for r in rows if r["phase"] == phase]
        optional = {s["stage"] for s in STAGES if s.get("optional")}
        res = [r["result"] for r in mine]
        passed = sum(1 for x in res if x == "PASS"
                     or x.startswith(("DONE", "SATISFIED")))
        failed = sum(1 for x in res if x.startswith(("FAIL", "HALT")))
        unknown = sum(1 for x in res if x.startswith("UNKNOWN"))
        running = sum(1 for x in res if x.startswith("STILL RUNNING"))
        # Some of a per-job stage's registered jobs have not run.
        partial = sum(1 for x in res if x.startswith("PARTIAL"))
        not_run = [r["stage"] for r in mine if r["result"] == "NOT_RUN"]
        skipped = sum(1 for x in res if x.startswith("SKIPPED"))
        if failed:
            verdict = "FAIL"
        elif unknown:
            verdict = "UNKNOWN"
        elif running:
            verdict = "STILL RUNNING"
        elif partial:
            verdict = "PARTIAL"
        elif not_run:
            verdict = "NOT_RUN" if not passed else "PARTIAL"
        elif not passed and skipped == len(mine):
            verdict = "SKIPPED (optional)"
        else:
            verdict = "PASS"
        out.append({
            "phase": phase, "stages": len(mine), "passed": passed,
            "failed": failed, "unknown": unknown, "running": running,
            "not_run": len(not_run),
            "skipped": skipped,
            "duration_seconds": round(sum(r["duration_seconds"] or 0
                                          for r in mine), 1),
            "retries": sum(r.get("retries", 0) for r in mine),
            "runbook": " ".join(r["runbook"] for r in mine
                                if r["runbook"] != "-"),
            "verdict": verdict,
            "optional_only": all(r["stage"] in optional for r in mine)})
    return out


def phase_report(out_dir) -> dict:
    """Every phase with its start, end, duration and verdict.

    Read from run_log.jsonl. A phase that never ran is listed as NOT_RUN (or
    SKIPPED for an optional one) rather than left out; an artifact with no
    logged run is DONE (not logged) -- or FAIL when its job answered ok:
    false -- never given a time it was not measured at. A run logged with
    no exit code is UNKNOWN. The LAST run of a phase decides its verdict, and earlier failures
    are counted, not forgotten.
    """
    out_dir = pathlib.Path(out_dir)
    runs: dict[str, list[dict]] = {}
    path = out_dir / "run_log.jsonl"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            stage = rec.get("stage")
            if stage == "run":
                stage = (stage_for("run", rec.get("job")) if rec.get("job")
                         else _legacy_run_stage(rec, out_dir))
            runs.setdefault(stage, []).append(rec)

    board = {r["stage"]: r for r in build_stage_board(out_dir)["stages"]}
    phases = []
    for spec in STAGES:
        mine = sorted(runs.get(spec["stage"], []),
                      key=lambda r: r.get("started_at") or "")
        row = {"stage": spec["stage"], "phase": spec["phase"],
               "runbook": spec["runbook"], "runs_on": spec["runs_on"],
               "runs": len(mine),
               "failed_runs": sum(1 for r in mine
                                  if r.get("exit_code") not in (0, None)),
               # Logged with no exit code: crashed or interrupted. Counted
               # apart -- not a failure the stage reported, not a pass.
               "unknown_runs": sum(1 for r in mine
                                   if r.get("exit_code") is None),
               "started_at": None, "ended_at": None,
               "duration_seconds": None, "retries": 0}
        # A workflow can exit 0 locally with a job that did not succeed.
        art = _load_stage(out_dir, spec) if spec.get("job") else None
        job_result = _job_result(art)
        if mine:
            last = mine[-1]
            lo, hi = _ts(last.get("started_at")), _ts(last.get("ended_at"))
            row.update({
                "started_at": last.get("started_at"),
                "ended_at": last.get("ended_at"),
                "duration_seconds": (round((hi - lo).total_seconds(), 1)
                                     if lo and hi else None),
                "result": _verdict(last.get("exit_code")),
                "retries": sum(int(r.get("retries") or 0) for r in mine)})
            # Retries inside an AIDP job are in its own output, one RETRY
            # line each (the copy notebook writes them).
            for run in _job_runs(art):
                row["retries"] += sum(
                    1 for line in str(run.get("output") or "").splitlines()
                    if _RETRY_LINE.search(line))
            if job_result:
                row["result"] = job_result
        elif _artifact_paths(out_dir, spec["artifact"]):
            row["result"] = job_result or "DONE (not logged)"
        elif board[spec["stage"]]["status"] == "SATISFIED":
            # The board's reading, so the two reports agree: the twin ran
            # for real and did the work this stage would have done.
            row["result"] = (f'SATISFIED (by `{spec["alternative_to"]}`)')
        else:
            row["result"] = ("SKIPPED (optional)" if spec.get("optional")
                             else "NOT_RUN")
        phases.append(row)
    summary = _phase_summary(phases)
    from .resources import build_resources, resources_by_phase
    resources = build_resources(out_dir)
    grouped = resources_by_phase(resources)
    for ph in summary:
        ph["resources"] = grouped.get(ph["phase"], [])
    events = []
    rpath = out_dir / "retries.jsonl"
    if rpath.is_file():
        for line in rpath.read_text(encoding="utf-8").splitlines():
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
    return {"out_dir": str(out_dir), "phases": phases,
            "phase_summary": summary,
            "resources": resources,
            "retry_events": events,
            "unattributed_runs": len(runs.get("run", []))}
