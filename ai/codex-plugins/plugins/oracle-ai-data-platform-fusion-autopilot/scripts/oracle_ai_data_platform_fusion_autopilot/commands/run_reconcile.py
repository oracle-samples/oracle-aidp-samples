"""Pure run-outcome reconciliation — a job status is never the run's verdict.

The incident this closes: an AIDP job reported terminal ``SUCCESS`` while the
run itself had aborted at the in-loop COA gate after landing one bronze table
— and the CLI's ``summary.failed`` check was the only thing standing between
that and a "seed completed" report. This module makes the verdict a pure
function of the run's own evidence, with one hard rule: **a job status of
``SUCCESS`` may only ADD failure signal, never remove one.**

Two callers, ONE completeness definition (:func:`classify_run_completeness`):

* the orchestrator derives the durable ``__run_outcome__`` state row from it
  cluster-side (first-party knowledge of its expected execution set), and
* the CLI applies it laptop-side via :func:`reconcile_run_outcome`, wrapped
  in the R0/R5/R6 rules below —

so the printed verdict and the durable row cannot disagree on the same
evidence.

Everything here is pure: no Spark, no HTTP, no filesystem. ``StepView`` is
deliberately NOT ``RunStep`` — the reconciler has no schema/engine coupling
and both callers adapt their own step shapes into it.

Rules, in order (design §9.1). R5/R6 document the existing exception arms for
completeness — all ``DispatchError`` subclasses already exit 2 before a parsed
summary exists — but are still enforced here defensively:

====  =========================================================  ==========
R0    ``dry_run`` / empty-bundle non-executing summary           planned, 0
R1    any step ``status == "failed"``                            aborted, 1
R2    any step ``skip_reason == "aborted"``                      aborted, 1
R3    any reserved ``__…__`` step with status != success         aborted, 1
R4    an expected node with NO terminal step row at all          unproven, 1
R5    marker missing / degraded (defensive; normally raises)     unproven, 1
R6    ``job_status != "SUCCESS"`` (defensive; normally raises)   aborted, 1
R7    none of the above                                          completed, 0
====  =========================================================  ==========

``AIDPF-4023`` is stamped by the reconciler itself (FR-15.10): always on an
``unproven`` verdict (R4's missing-terminal-row case has no failing step to
extract a code from), and whenever a ``SUCCESS`` job status masked an
``aborted``/``unproven`` verdict.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Literal, Sequence

AIDPF_4023_RUN_RECONCILIATION = "AIDPF-4023"
"""The job/notebook reported success but reconciliation proved the run aborted
or its completion unproven. The job status is not the run's verdict."""

_AIDPF_CODE_RE = re.compile(r"AIDPF-\d{4}")

#: Every status a state/step row can carry counts as a TERMINAL row for
#: completeness — *no row at all* is the violation R4 exists to catch.
TERMINAL_STATUSES = frozenset(
    {"success", "failed", "skipped", "deferred", "resumed_skipped"}
)

Verdict = Literal["planned", "completed", "aborted", "unproven", "not_checked"]

#: Codes the COA remediation loop acts on — used only to decide whether the
#: verdict block prints the COA remediation hint.
_COA_REMEDIABLE_CODES = ("AIDPF-2018", "AIDPF-2017")


@dataclass(frozen=True)
class StepView:
    """Engine-agnostic view of one executed step / state row."""

    dataset_id: str
    layer: str = ""
    status: str = ""
    skip_reason: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class RunOutcome:
    """The reconciled verdict the CLI prints and returns."""

    verdict: Verdict
    exit_code: int
    codes: tuple[str, ...] = ()
    lines: tuple[str, ...] = ()


def _is_reserved(dataset_id: str) -> bool:
    return dataset_id.startswith("__") and dataset_id.endswith("__")


def classify_run_completeness(
    steps: Sequence[StepView],
    expected_terminal_node_ids: frozenset[str] | None,
) -> Literal["completed", "aborted", "unproven", "not_checked"]:
    """THE shared completeness core (design D-10) — rules R1–R4 only.

    ``expected_terminal_node_ids`` is the orchestrator-declared execution set
    (design D-9): exactly the node ids the run intended to execute or
    resume-skip. It is NOT the resolved lineage plan — mart-only runs
    deliberately leave lineage bronze nodes step-less. ``None`` means the set
    is unavailable (old marker on a scoped run): completeness is honestly
    ``not_checked``, never a silent pass and never a false ``unproven``.
    """
    for step in steps:  # R1
        if step.status == "failed":
            return "aborted"
    for step in steps:  # R2
        if step.skip_reason == "aborted":
            return "aborted"
    for step in steps:  # R3
        if _is_reserved(step.dataset_id) and step.status != "success":
            return "aborted"
    if expected_terminal_node_ids is None:
        return "not_checked"
    seen = {s.dataset_id for s in steps if s.status in TERMINAL_STATUSES}
    missing = expected_terminal_node_ids - seen
    if missing:  # R4
        return "unproven"
    return "completed"


def _extract_codes(steps: Iterable[StepView]) -> tuple[str, ...]:
    """Every ``AIDPF-\\d{4}`` from failing/aborted steps, de-duplicated in
    first-seen order."""
    codes: list[str] = []
    for step in steps:
        if step.status == "failed" or step.skip_reason == "aborted" or (
            _is_reserved(step.dataset_id) and step.status != "success"
        ):
            for blob in (step.error_message or "", step.skip_reason or ""):
                for code in _AIDPF_CODE_RE.findall(blob):
                    if code not in codes:
                        codes.append(code)
    return tuple(codes)


def _first_line(text: str | None) -> str:
    return (text or "").strip().splitlines()[0] if (text or "").strip() else ""


def _verdict_lines(
    *,
    verdict: str,
    codes: tuple[str, ...],
    steps: Sequence[StepView],
    mode: str,
    run_id: str | None,
    missing: tuple[str, ...],
) -> tuple[str, ...]:
    """The ``RUN VERDICT`` block the CLI prints verbatim after the summary.

    Command strings here follow the same rule as the diagnostics (FR-15.9 /
    design §9.3.4b): only commands that exist in the shipped CLI — enforced
    by the CLI-introspection invariant test. P2 registered
    ``--resolve-coa-from-metadata``, so the COA hint is now the executable
    remediation loop.
    """
    ident = f"run_id={run_id or '<unknown>'}, mode={mode}"
    lines = [f"RUN VERDICT: {verdict.upper()}  ({ident})"]
    seen_msgs: set[str] = set()
    for step in steps:
        if step.status == "failed" or step.skip_reason == "aborted":
            msg = _first_line(step.error_message) or (
                f"{step.dataset_id}: {step.status}"
                + (f" ({step.skip_reason})" if step.skip_reason else "")
            )
            if msg not in seen_msgs:
                seen_msgs.add(msg)
                lines.append(f"  {msg}")
    if missing:
        shown = ", ".join(sorted(missing)[:8])
        more = len(missing) - min(len(missing), 8)
        lines.append(
            f"  {AIDPF_4023_RUN_RECONCILIATION}  {len(missing)} expected node(s) "
            f"finished with NO terminal state row: {shown}"
            + (f" (+{more} more)" if more > 0 else "")
        )
    lines.append(
        "  The AIDP job status is NOT the run's verdict — do not report this "
        "run as complete."
    )
    if any(code in codes for code in _COA_REMEDIABLE_CODES):
        lines.append(
            "  Remediation: `bootstrap --refresh --resolve-coa-from-metadata` "
            "(derives + Tier-B-verifies the per-chart COA mapping from Fusion "
            "metadata), then `run --resume "
            f"{run_id or '<run_id>'}` (no --mode: the resume adopts the "
            "run's recorded mode). Fallback: author "
            "profile.chartOfAccounts.byChart via $medallion-author."
        )
        if run_id:
            lines.append(
                f"  Diagnostic: .aidp/diagnostics/{run_id}/"
                f"AIDPF-<primary>__coa-gate.json"
            )
    if "AIDPF-2093" in codes:
        lines.append(
            "  Remediation: `bronze diagnose-encode --dataset <id>` names "
            "the mismatched column; then add a `schemaPatches` entry for it "
            "on the bundle dataset (read-side repair; declared types "
            "restored + integrity-guarded at landing) and re-run."
        )
    return tuple(lines)


def reconcile_run_outcome(
    *,
    job_status: str | None,
    marker_present: bool,
    marker_degraded: bool,
    steps: Sequence[StepView],
    mode: str,
    expected_terminal_node_ids: frozenset[str] | None,
    dry_run: bool,
    run_id: str | None = None,
) -> RunOutcome:
    """Laptop-side verdict: R0 + the shared completeness core + R5/R6.

    ``job_status=None`` means the inline path (no dispatch job exists) — it
    neither adds nor masks failure signal. New rules only ever turn a
    previously-0 exit into non-zero on EXECUTED runs; R0 pins dry-runs to 0.
    """
    if dry_run:  # R0 — RunSummary.empty() is zero steps + populated plan.
        return RunOutcome(verdict="planned", exit_code=0)

    completeness = classify_run_completeness(steps, expected_terminal_node_ids)

    # Defensive R5/R6 — the CLI's exception arms normally fire long before a
    # summary reaches this function (DispatchMarkerMissing/Degraded and
    # DispatchRunFailedError exit 2); enforced anyway so the pure contract
    # does not depend on caller discipline.
    if completeness in ("completed", "not_checked"):
        if not marker_present or marker_degraded:
            completeness = "unproven"  # R5
        elif job_status is not None and job_status != "SUCCESS":
            completeness = "aborted"  # R6

    codes = _extract_codes(steps)
    missing: tuple[str, ...] = ()
    if completeness == "unproven":
        if expected_terminal_node_ids is not None:
            seen = {s.dataset_id for s in steps if s.status in TERMINAL_STATUSES}
            missing = tuple(sorted(expected_terminal_node_ids - seen))
        # FR-15.10 — unproven has no failing step to extract a code from.
        if AIDPF_4023_RUN_RECONCILIATION not in codes:
            codes = (AIDPF_4023_RUN_RECONCILIATION, *codes)

    if completeness in ("aborted", "unproven"):
        # FR-15.10 — SUCCESS masked a failure: stamp the reconciliation code.
        if (
            job_status == "SUCCESS"
            and AIDPF_4023_RUN_RECONCILIATION not in codes
        ):
            codes = (AIDPF_4023_RUN_RECONCILIATION, *codes)
        lines = _verdict_lines(
            verdict=completeness, codes=codes, steps=steps, mode=mode,
            run_id=run_id, missing=missing,
        )
        return RunOutcome(
            verdict=completeness, exit_code=1, codes=codes, lines=lines,
        )

    if completeness == "not_checked":
        return RunOutcome(
            verdict="not_checked",
            exit_code=0,
            lines=(
                "completeness: unproven-not-checked (no expected execution "
                "set — old marker on a scoped run); per-step results above "
                "are still authoritative.",
            ),
        )

    return RunOutcome(verdict="completed", exit_code=0)


# ── status() run-level banner (pure; supersession semantics) ────────────────

_OUTCOME_ROW = "__run_outcome__"
_GATE_ROW = "__coa_gate__"


@dataclass(frozen=True)
class RunBanner:
    """The run-level verdict `status` renders above the per-dataset table."""

    run_id: str
    label: Literal["COMPLETED", "ABORTED", "UNPROVEN"]
    codes: tuple[str, ...] = ()
    detail: str = ""


def banner_verdict(rows: Sequence[dict]) -> RunBanner | None:
    """Verdict for the LATEST run among the reserved (``__…__``) state rows.

    ``rows`` are plain mappings with ``run_id`` / ``dataset_id`` / ``status``
    / ``error_message`` / ``last_run_at`` (any mutually comparable timestamp
    type). Semantics (FR-15.8, design §9.2.5):

    * The latest reserved row **of any kind** picks the run — keying on the
      manifest alone would silently show the previous run for pre-feature
      history or an AIDPF-4022 manifest-commit failure.
    * Within that run (a ``--resume`` deliberately reuses the run_id, rows
      are append-only): the verdict is the **latest** ``__run_outcome__``
      row — a successful resume supersedes the earlier attempt's failures.
    * A ``__coa_gate__`` failure row is consulted only when NEWER than the
      latest outcome row (a gate abort whose best-effort outcome write
      failed) or when no outcome row exists.
    * Reserved rows but no outcome row at all → ``UNPROVEN`` (pre-feature
      run or interrupted — no completion record).
    """
    if not rows:
        return None
    latest_row = max(rows, key=lambda r: r["last_run_at"])
    run_id = str(latest_row["run_id"])
    run_rows = [r for r in rows if str(r["run_id"]) == run_id]

    outcomes = [r for r in run_rows if r["dataset_id"] == _OUTCOME_ROW]
    gates = [r for r in run_rows if r["dataset_id"] == _GATE_ROW]
    latest_outcome = max(outcomes, key=lambda r: r["last_run_at"]) if outcomes else None

    def _codes(*blobs: str | None) -> tuple[str, ...]:
        found: list[str] = []
        for blob in blobs:
            for code in _AIDPF_CODE_RE.findall(blob or ""):
                if code not in found:
                    found.append(code)
        return tuple(found)

    newer_failed_gate = None
    for gate in gates:
        if str(gate.get("status")) == "success":
            continue
        if latest_outcome is None or gate["last_run_at"] > latest_outcome["last_run_at"]:
            newer_failed_gate = gate

    if newer_failed_gate is not None:
        return RunBanner(
            run_id=run_id, label="ABORTED",
            codes=_codes(newer_failed_gate.get("error_message")),
            detail=_first_line(newer_failed_gate.get("error_message")),
        )
    if latest_outcome is None:
        return RunBanner(
            run_id=run_id, label="UNPROVEN",
            detail="no completion record — pre-feature run or interrupted",
        )
    if str(latest_outcome.get("status")) == "success":
        return RunBanner(run_id=run_id, label="COMPLETED")
    return RunBanner(
        run_id=run_id, label="ABORTED",
        codes=_codes(latest_outcome.get("error_message")),
        detail=_first_line(latest_outcome.get("error_message")),
    )


__all__ = [
    "AIDPF_4023_RUN_RECONCILIATION",
    "RunBanner",
    "RunOutcome",
    "StepView",
    "TERMINAL_STATUSES",
    "banner_verdict",
    "classify_run_completeness",
    "reconcile_run_outcome",
]
