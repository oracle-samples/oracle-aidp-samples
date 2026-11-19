"""Implementation of ``aidp-fusion-autopilot run`` and ``status``.

  * ``run --inline`` calls ``orchestrator.run(bundle_path, ...)`` directly
    (the architectural primary — Spark + checkpointer + vault + Delta
    catalog all live inside the AIDP notebook session). Catches every
    ``OrchestratorConfigError`` subclass + ``NotImplementedError`` and
    exits 2 with a single-line message (no traceback). Anything else
    propagates with full traceback — that's an orchestrator bug, not a
    user error.

  * ``run`` without ``--inline`` is the laptop-terminal REST dispatch
    path.

  * ``status`` reads ``fusion_autopilot_state`` with one-row-per-dataset
    semantics (``ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY
    last_run_at DESC)``) and surfaces the ``skip_reason`` column
    distinctly.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

logger = logging.getLogger(__name__)


def run(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    mode: str | None = None,
    datasets: str | None = None,
    layers: str | None = None,
    inline: bool = False,
    resume_run_id: str | None = None,
    dry_run: bool = False,
    poll_timeout_s: int = 3600,
    force_fingerprint_skip: bool = False,
    repin_plan_hash: bool = False,
    strict_scope: bool | None = None,
    auto_remediate_coa: bool = False,
    console: Console | None = None,
) -> int:
    """Submit the bundle's pipeline to AIDP, or run inline if --inline.

    ``layers`` parses as the same CSV shape as ``datasets`` and threads
    through to ``orchestrator.run``. Validation lives in the content-pack
    plan resolver, which raises ``MissingDependencyError`` for unknown
    layer names.

    ``auto_remediate_coa`` (design §10.4, default OFF): on an
    AIDPF-2018/AIDPF-2017 abort, run the metadata resolution
    (``bootstrap --refresh --resolve-coa-from-metadata``) and resume the
    aborted run — at most ONE pass (R9), resuming ONLY when the resolution
    phase exits 0 (FR-14a S1); a non-zero phase stops with its diagnostic
    (AIDPF-2021/2023, with AIDPF-2022 per-arm details). The internal resume
    never passes ``--mode`` — it adopts the aborted run's recorded mode
    (AIDPF-1046 protection, D-12), identically for seed and incremental.
    """
    console = console or Console()

    # One-time logging setup so mid-run WARNs from
    # `orchestrator._safe_write_state_row` (state-write soft-fails) and
    # `_resolve_password` (literal-credential WARN) surface on stderr with
    # Rich formatting alongside the run summary. The orchestrator emits via
    # stdlib `logging.getLogger(__name__).warning(...)` and takes no
    # `console` parameter; the CLI wires the RichHandler so output is
    # consistent.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.WARNING,
            format="%(message)s",
            handlers=[
                RichHandler(console=console, show_time=False, show_path=False),
            ],
        )

    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1

    # Parse CSV → list[str] or None. Do NOT pre-resolve against
    # bundle.datasets[] — that would limit the filter to bronze IDs
    # and silently skip silver/gold. The content-pack plan resolver
    # classifies user-typed identifiers across every pack node (bronze
    # + silver + gold) and raises MissingDependencyError (exit 2 via
    # OrchestratorConfigError marker) if a name doesn't exist.
    dataset_filter: list[str] | None = (
        [s.strip() for s in datasets.split(",") if s.strip()]
        if datasets else None
    )
    # Same CSV-parse shape as `datasets`. Empty string after split -> None
    # (consistent with --datasets "" behavior). Typo validation lives in
    # the plan resolver, not here.
    layer_filter: list[str] | None = (
        [s.strip() for s in layers.split(",") if s.strip()]
        if layers else None
    )

    # The content-pack backend's per-node atomic-commit model is the
    # resume unit. The orchestrator adopts ``resume_run_id`` as the
    # run_id so the resumed run's state rows join with the prior failed
    # run's rows under one identifier.

    # ``--auto-remediate-coa`` keys on the reconciled verdict's codes; the
    # sink carries (RunOutcome, run_id, mode) up from `_reconcile_exit`
    # without changing the backends' exit-code contract.
    coa_abort_sink: list = []

    if inline:
        # Pass the PATH (not parsed dict): orchestrator.run re-reads
        # the file because `_render_env_vars` must run BEFORE Pydantic
        # validation, and that step needs the raw YAML text.
        exit_code = _run_inline(
            bundle_path, mode, dataset_filter, layer_filter,
            resume_run_id, dry_run, console,
            force_fingerprint_skip=force_fingerprint_skip,
            repin_plan_hash=repin_plan_hash,
            strict_scope=strict_scope,
            coa_abort_sink=coa_abort_sink,
        )
    else:
        # REST-dispatch resume threads `resume_run_id` into the
        # cluster-side `orchestrator.run(...)` call so the resumed run
        # adopts the supplied id and joins state rows with the prior
        # failed run. Banner gated on `not dry_run`: dispatch short-circuits
        # before any resume work happens under --dry-run, so a "Resuming
        # run X" banner there would mislead the operator.
        if resume_run_id is not None and not dry_run:
            console.print(
                f"[bold cyan]Resuming run[/bold cyan] [dim]{resume_run_id}[/dim] — "
                f"reading fusion_autopilot_state, computing reattempt plan…"
            )
        exit_code = _run_via_aidp_dispatch(
            bundle_path, config_path, env_name, dataset_filter, layer_filter,
            mode, dry_run, poll_timeout_s, console,
            force_fingerprint_skip=force_fingerprint_skip,
            repin_plan_hash=repin_plan_hash,
            resume_run_id=resume_run_id,
            strict_scope=strict_scope,
            coa_abort_sink=coa_abort_sink,
        )

    if auto_remediate_coa and exit_code != 0:
        return _maybe_auto_remediate_coa(
            exit_code,
            coa_abort_sink,
            bundle_path=bundle_path,
            config_path=config_path,
            env_name=env_name,
            datasets=datasets,
            layers=layers,
            inline=inline,
            resume_run_id=resume_run_id,
            dry_run=dry_run,
            poll_timeout_s=poll_timeout_s,
            console=console,
        )
    return exit_code


def _maybe_auto_remediate_coa(
    exit_code: int,
    coa_abort_sink: list,
    *,
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    datasets: str | None,
    layers: str | None,
    inline: bool,
    resume_run_id: str | None,
    dry_run: bool,
    poll_timeout_s: int,
    console: Console,
) -> int:
    """One bounded ``--auto-remediate-coa`` pass (design §10.4, R9).

    Preconditions checked here, ALL of which must hold or the original
    exit code is returned untouched:

    * the run executed for real (not ``--dry-run``) and was NOT itself a
      resume (a user- or loop-issued ``--resume`` is already the second
      act — never chain remediation passes);
    * the reconciled verdict carries an AIDPF-2018/AIDPF-2017 code
      (``run_reconcile._COA_REMEDIABLE_CODES`` — the same constant that
      gates the verdict block's remediation hint) and a ``run_id``.

    The resolution phase is the REAL ``bootstrap`` entry point with
    ``refresh + resolve_coa_from_metadata + non_interactive`` — the
    additive path needs no prompts (design §10.1) and its FR-14a exit code
    IS the loop's stop/resume key: 0 → resume (adopting the recorded mode:
    ``mode=None``); non-zero → stop, the phase already printed its
    2021/2023 diagnostic. The resume re-enters :func:`run` with
    ``auto_remediate_coa=False`` — the one-pass guard.
    """
    if dry_run or resume_run_id is not None or not coa_abort_sink:
        return exit_code
    from .run_reconcile import _COA_REMEDIABLE_CODES

    outcome, run_id, run_mode = coa_abort_sink[-1]
    coa_codes = [c for c in getattr(outcome, "codes", ()) if c in _COA_REMEDIABLE_CODES]
    if not coa_codes or not run_id:
        return exit_code

    console.print()
    console.print(
        f"[bold cyan]--auto-remediate-coa[/bold cyan]: {', '.join(coa_codes)} "
        f"abort on run [dim]{run_id}[/dim] (mode={run_mode}) — running the "
        f"metadata resolution (one pass, R9)…"
    )
    from .bootstrap import bootstrap as bootstrap_impl

    resolution_rc = bootstrap_impl(
        bundle_path,
        config_path,
        env_name,
        refresh=True,
        resolve_coa_from_metadata=True,
        non_interactive=True,
        console=console,
    )
    if resolution_rc != 0:
        console.print(
            f"[red]--auto-remediate-coa: the resolution phase exited "
            f"{resolution_rc} — STOP (no resume). Act on its diagnostic "
            f"(AIDPF-2021 unreachable / AIDPF-2023 unresolved charts, with "
            f"AIDPF-2022 per-arm details), then `run --resume {run_id}` "
            f"(no --mode).[/red]"
        )
        return exit_code
    console.print(
        f"[bold cyan]--auto-remediate-coa[/bold cyan]: resolution phase "
        f"exited 0 — resuming run [dim]{run_id}[/dim] (no --mode; adopts the "
        f"run's recorded mode '{run_mode}')…"
    )
    return run(
        bundle_path,
        config_path,
        env_name,
        mode=None,
        datasets=datasets,
        layers=layers,
        inline=inline,
        resume_run_id=run_id,
        dry_run=False,
        poll_timeout_s=poll_timeout_s,
        auto_remediate_coa=False,
        console=console,
    )


def _run_inline(
    bundle_path: Path,
    mode: str | None,
    datasets: list[str] | None,
    layers: list[str] | None,
    resume_run_id: str | None,
    dry_run: bool,
    console: Console,
    *,
    force_fingerprint_skip: bool = False,
    repin_plan_hash: bool = False,
    strict_scope: bool | None = None,
    coa_abort_sink: list | None = None,
) -> int:
    """Run the orchestrator in-process.

    Catches `(OrchestratorConfigError, NotImplementedError)` and exits 2
    with a single-line message — no traceback. Any other exception
    propagates with full traceback (orchestrator bug, not user error).

    ``resume_run_id`` triggers checkpoint-resume: the orchestrator reads
    ``fusion_autopilot_state`` for that run_id and skips datasets whose
    latest terminal status is ``success`` or ``resumed_skipped``. The
    three resume failure modes (``ResumeRunNotFoundError`` /
    ``ResumeRunNotResumableError`` / ``ResumeBundleMismatchError``)
    subclass ``OrchestratorConfigError`` and exit 2 cleanly.
    """
    from oracle_ai_data_platform_fusion_autopilot import orchestrator
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
        OrchestratorConfigError,
    )
    from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
        EXIT_CODE_SCHEMA_DRIFT,
        SchemaDriftDetectedError,
    )

    # Dedicated stderr console for AIDPF hand-off messages.
    # Rich Console.print does NOT accept a stdlib `file=` kwarg
    # the constructor binds to its output stream.
    error_console = Console(stderr=True)

    if resume_run_id is not None:
        console.print(
            f"[bold cyan]Resuming run[/bold cyan] [dim]{resume_run_id}[/dim] — "
            f"reading fusion_autopilot_state, computing reattempt plan…"
        )

    # Content-pack is the only backend. Resolve the pack
    # + profile up front and pass them into orchestrator.run. Skip
    # gracefully when the bundle has no contentPack block (legacy
    # bundles still pass through the underlying orchestrator code
    # path until they're migrated).
    resolved_pack = None
    tenant_profile = None
    _has_content_pack = False
    try:
        from ..schema.bundle import load_bundle as _peek_load_bundle
        _peek_bundle, _ = _peek_load_bundle(bundle_path)
        _has_content_pack = _peek_bundle.content_pack is not None
    except Exception:
        _has_content_pack = False
    if _has_content_pack:
        from ..schema.bundle import (
            AIDPF_1030_PROFILE_MISSING,
            AIDPF_1031_CONTENT_PACK_MISSING,
            AIDPF_1033_PROFILE_FILE_NOT_FOUND,
            ContentPackValidationFailedError,
            load_bundle as _load_bundle,
            resolve_content_pack_root,
        )
        from ..schema.tenant_profile import (
            load_tenant_profile,
            resolve_profile_path,
        )
        from ..orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from ..orchestrator.content_pack_validators import validate_pack_full

        bundle, _paths = _load_bundle(bundle_path)
        if bundle.content_pack is None:
            console.print(
                f"[red]{AIDPF_1031_CONTENT_PACK_MISSING}: bundle.yaml has no "
                f"`contentPack:` block; "
                f"requires it.[/red]"
            )
            return 2
        if bundle.content_pack.profile is None:
            console.print(
                f"[red]{AIDPF_1030_PROFILE_MISSING}: bundle.yaml's "
                f"`contentPack.profile` field is required when running under "
                f"content-pack.[/red]"
            )
            return 2
        pack_root = resolve_content_pack_root(bundle_path, bundle.content_pack)
        resolved_pack = load_full_chain(
            pack_root, base_resolver=make_filesystem_base_resolver(pack_root),
        )
        # Full validation BEFORE any profile/stage/dispatch work.
        # validate_dag/validate_template_variables/validate_dashboard_*/etc.
        # catch errors that the runtime DAG resolver doesn't — e.g. a typo
        # in dependsOn.silver that points to a non-existent node would
        # otherwise let the dependent execute against stale upstream tables.
        report = validate_pack_full(resolved_pack)
        if not report.ok:
            err = ContentPackValidationFailedError(report=report)
            console.print(f"[red]{err}[/red]")
            return 2
        profile_path = resolve_profile_path(bundle_path, bundle.content_pack.profile)
        if not profile_path.exists():
            console.print(
                f"[red]{AIDPF_1033_PROFILE_FILE_NOT_FOUND}: profile YAML not "
                f"found at {profile_path}.[/red]"
            )
            return 2
        tenant_profile = load_tenant_profile(profile_path)

    try:
        summary = orchestrator.run(
            bundle_path=bundle_path,
            mode=mode,
            datasets=datasets,
            layers=layers,
            resume_run_id=resume_run_id,
            dry_run=dry_run,
            resolved_pack=resolved_pack,
            tenant_profile=tenant_profile,
            force_fingerprint_skip=force_fingerprint_skip,
            repin_plan_hash=repin_plan_hash,
            strict_scope=strict_scope,
        )
    except SchemaDriftDetectedError as exc:
        # Runtime preflight detected bronze-schema drift. Print the hand-off
        # message on STDERR and exit 14. This arm MUST precede the
        # OrchestratorConfigError arm because the exception does NOT inherit
        # from OrchestratorConfigError; otherwise we'd return exit 2 instead.
        error_console.print(f"[red]{exc.summary}[/red]")
        return EXIT_CODE_SCHEMA_DRIFT
    except (OrchestratorConfigError, NotImplementedError) as exc:
        # User-facing config / not-implemented errors. Exit 2 with a
        # single-line message and no traceback. The error class is
        # responsible for emitting a self-explanatory message; the
        # CLI prints `str(exc)` directly without extra framing.
        console.print(f"[red]{exc}[/red]")
        return 2
    # Diagnostics persistence parity (FR-15.13, D-14): the shared best-effort
    # persister runs on the inline path too — the remediation loop's artifact
    # must be reachable regardless of how the run executed.
    from ..schema.diagnostic_artifact import persist_run_diagnostics

    persist_run_diagnostics(bundle_path.resolve().parent, summary)
    _render_summary(console, summary)
    # Inline path: no dispatch job exists, so job_status is None (neutral —
    # it can neither add nor mask failure signal).
    return _reconcile_exit(
        console, summary, dry_run=dry_run, job_status=None,
        scoped=(datasets is not None or layers is not None),
        coa_abort_sink=coa_abort_sink,
    )


def _run_via_aidp_dispatch(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    datasets: list[str] | None,
    layers: list[str] | None,
    mode: str | None,
    dry_run: bool,
    poll_timeout_s: int,
    console: Console,
    *,
    force_fingerprint_skip: bool = False,
    repin_plan_hash: bool = False,
    resume_run_id: str | None = None,
    strict_scope: bool | None = None,
    coa_abort_sink: list | None = None,
) -> int:
    """Submit the bundle to AIDP via the REST job API.

    Loads ``aidp.config.yaml``, runs preflight, builds the wheel, generates
    the orchestrator notebook, uploads it, creates a job, submits a run,
    polls to terminal status, fetches the executed notebook, parses the
    ``AIDP_LIVE_TEST_RESULT`` marker, and renders the RunSummary.

    Same exit-code contract as :func:`_run_inline`: 0 on success, 1 if any
    step failed, 2 on any dispatch-layer error (config, preflight, network).

    ``resume_run_id`` is threaded into ``dispatch_via_rest`` which injects it
    into the run-cell as a ``repr()``-quoted literal. Bad run_ids surface as
    cell-3 ``ResumeRunNotFoundError`` /
    ``ResumeRunNotResumableError`` / ``ResumeBundleMismatchError`` —
    enriched into ``DispatchRunFailedError``'s message by
    ``dispatch_via_rest`` so the operator sees the typed orchestrator
    exception class without opening the executed notebook.
    """
    from ._config_helpers import env_or_error, load_aidp_config
    from ..dispatch import dispatch_via_rest
    from ..dispatch.errors import DispatchError
    from ..schema.errors import (
        EXIT_CODE_SCHEMA_DRIFT,
        OrchestratorConfigError,
        SchemaDriftDetectedError,
    )

    error_console = Console(stderr=True)

    # Prepare content-pack staging primitives at the CLI layer
    # (orchestrator-side imports are allowed here; dispatch/ cannot import
    # them). Bundles without a contentPack block skip staging.
    profile_yaml: str | None = None
    pack_files: dict[str, str] | None = None
    pack_manifest: dict | None = None
    schema_snapshot_yaml: str | None = None
    resolved_pack = None
    _has_content_pack = False
    try:
        from ..schema.bundle import load_bundle as _peek_load_bundle
        _peek_bundle, _ = _peek_load_bundle(bundle_path)
        _has_content_pack = _peek_bundle.content_pack is not None
    except Exception:
        _has_content_pack = False
    if _has_content_pack:
        from ..schema.bronze_schema_snapshot import resolve_snapshot_path
        from ..schema.bundle import (
            AIDPF_1030_PROFILE_MISSING,
            AIDPF_1031_CONTENT_PACK_MISSING,
            AIDPF_1033_PROFILE_FILE_NOT_FOUND,
            ContentPackValidationFailedError,
            load_bundle,
            resolve_content_pack_root,
        )
        from ..schema.tenant_profile import resolve_profile_path
        from ..orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from ..orchestrator.content_pack_staging import stage_pack_files
        from ..orchestrator.content_pack_validators import validate_pack_full

        bundle, _bundle_paths = load_bundle(bundle_path)
        if bundle.content_pack is None:
            console.print(
                f"[red]{AIDPF_1031_CONTENT_PACK_MISSING}: bundle.yaml has no "
                f"`contentPack:` block; requires "
                f"the block. Add `contentPack.name` and `contentPack.profile` "
                f"before running.[/red]"
            )
            return 2
        if bundle.content_pack.profile is None:
            console.print(
                f"[red]{AIDPF_1030_PROFILE_MISSING}: bundle.yaml's "
                f"`contentPack.profile` field is required when running under "
                f"content-pack.[/red]"
            )
            return 2
        pack_root = resolve_content_pack_root(bundle_path, bundle.content_pack)
        resolved_pack = load_full_chain(
            pack_root, base_resolver=make_filesystem_base_resolver(pack_root),
        )
        # Full validation BEFORE staging. An invalid pack must NOT reach the
        # cluster; fail fast on the laptop with AIDPF-1036 carrying the
        # per-error report.
        report = validate_pack_full(resolved_pack)
        if not report.ok:
            err = ContentPackValidationFailedError(report=report)
            console.print(f"[red]{err}[/red]")
            return 2
        profile_path = resolve_profile_path(bundle_path, bundle.content_pack.profile)
        if not profile_path.exists():
            console.print(
                f"[red]{AIDPF_1033_PROFILE_FILE_NOT_FOUND}: profile YAML not "
                f"found at {profile_path}.[/red]"
            )
            return 2
        profile_yaml = profile_path.read_text(encoding="utf-8")
        pack_files, pack_manifest = stage_pack_files(resolved_pack)
        # Stage the snapshot if it exists. Profiles without one degrade to
        # empty `datasetDeltas` + WARN, same as the laptop path.
        snapshot_path = resolve_snapshot_path(
            bundle_path, bundle.content_pack.profile
        )
        if snapshot_path.exists():
            schema_snapshot_yaml = snapshot_path.read_text(encoding="utf-8")

    try:
        config = load_aidp_config(config_path)
        env = env_or_error(config, env_name)
        # Explicit backend selection from the bundle: if a contentPack block
        # is present we staged the pack files above and want the cluster-side
        # notebook to invoke the content-pack runner.
        dispatch_execution_backend = (
            "content-pack" if _has_content_pack else "legacy-python"
        )
        summary = dispatch_via_rest(
            bundle_path=bundle_path,
            config=config,
            env=env,
            env_name=env_name,
            mode=mode,  # type: ignore[arg-type]
            datasets=datasets,
            layers=layers,
            resume_run_id=resume_run_id,
            dry_run=dry_run,
            poll_timeout_s=poll_timeout_s,
            log=lambda msg: console.print(f"[dim]{msg}[/dim]"),
            execution_backend=dispatch_execution_backend,
            profile_yaml=profile_yaml,
            pack_files=pack_files,
            pack_manifest=pack_manifest,
            force_fingerprint_skip=force_fingerprint_skip,
            repin_plan_hash=repin_plan_hash,
            schema_snapshot_yaml=schema_snapshot_yaml,
            # Pack threaded through so dispatch's dry-run path can call the
            # schema plan resolver without importing orchestrator modules.
            resolved_pack=resolved_pack,
            # --strict-scope must reach the cluster-side orchestrator.run()
            # AND the dispatch dry-run resolver.
            strict_scope=strict_scope,
        )
    except SchemaDriftDetectedError as exc:
        # Drift surfaces from REST-dispatch via marker translation in
        # `dispatch_via_rest`. Same exit-14 contract as the inline path;
        # hand-off message lands on stderr so stdout stays clean for piping.
        error_console.print(f"[red]{exc.summary}[/red]")
        return EXIT_CODE_SCHEMA_DRIFT
    except (DispatchError, OrchestratorConfigError) as exc:
        console.print(f"[red]{exc}[/red]")
        return 2

    _render_summary(console, summary)
    # A summary from dispatch means the job reached terminal SUCCESS and the
    # marker parsed (DispatchRunFailedError / DispatchMarkerMissing/Degraded
    # raised above otherwise, exiting 2) — which is exactly the state where
    # "job SUCCESS" must never be mistaken for "run completed".
    return _reconcile_exit(
        console, summary, dry_run=dry_run, job_status="SUCCESS",
        scoped=(datasets is not None or layers is not None),
        coa_abort_sink=coa_abort_sink,
    )


def _reconcile_exit(
    console: Console,
    summary,
    *,
    dry_run: bool,
    job_status: str | None,
    scoped: bool,
    coa_abort_sink: list | None = None,
) -> int:
    """Print the ``RUN VERDICT`` block and return the reconciled exit code.

    The verdict is a pure function of the run's own evidence
    (:mod:`..commands.run_reconcile` — one completeness definition shared
    with the durable ``__run_outcome__`` row). Rules only ever turn a
    previously-0 exit into non-zero on EXECUTED runs; dry-runs are pinned to
    exit 0 (rule R0 — ``RunSummary.empty()`` is zero steps + populated plan).

    ``coa_abort_sink`` (design §10.4): when provided, the reconciled
    ``(outcome, run_id, mode)`` triple is appended so ``run()``'s
    ``--auto-remediate-coa`` loop can key on the verdict's codes without
    changing this function's exit-code contract.
    """
    from .run_reconcile import StepView, reconcile_run_outcome

    steps = tuple(
        StepView(
            dataset_id=str(getattr(s, "dataset_id", "")),
            layer=str(getattr(s, "layer", "") or ""),
            status=str(getattr(s, "status", "") or ""),
            skip_reason=getattr(s, "skip_reason", None),
            error_message=getattr(s, "error_message", None),
        )
        for s in summary.steps
    )
    expected = getattr(summary, "expected_terminal_node_ids", None)
    # D-9 fallback ladder for OLD markers only: an UNSCOPED run's plan IS its
    # expected set (eligible nodes; the orchestrator executes all of them).
    # A scoped run must NOT fall back to plan∩scope — those flags select
    # ROOTS whose transitive dependencies the resolver auto-includes, so the
    # intersection would silently pass a run missing upstream rows;
    # completeness stays honestly `not_checked` there.
    if expected is None and not scoped:
        plan = getattr(summary, "plan", None)
        if plan:
            expected = tuple(
                n.dataset_id for n in plan
                if getattr(n, "status", "eligible") == "eligible"
            )
    outcome = reconcile_run_outcome(
        job_status=job_status,
        marker_present=True,
        marker_degraded=False,
        steps=steps,
        mode=str(summary.mode),
        expected_terminal_node_ids=(
            frozenset(expected) if expected is not None else None
        ),
        dry_run=dry_run,
        run_id=getattr(summary, "run_id", None),
    )
    if coa_abort_sink is not None:
        coa_abort_sink.append(
            (outcome, getattr(summary, "run_id", None), str(summary.mode))
        )
    if outcome.lines:
        style = "red" if outcome.exit_code else "yellow"
        console.print()
        for line in outcome.lines:
            console.print(f"[{style}]{line}[/{style}]")
    return outcome.exit_code


def _render_summary(console: Console, summary) -> None:
    """Render a RunSummary as a Rich table.

    Handles two shapes:
      - normal run: per-step table with success/failed/skipped/deferred counters.
      - empty-bundle / dry-run: shows the would-run plan + extra-plan prereqs.
    """
    # Empty-bundle / dry-run path — RunSummary.empty(...) shape.
    if not summary.steps:
        if summary.plan is None and summary.prereqs is None:
            console.print(
                f"[yellow]Empty plan for project [cyan]{summary.bundle_project}[/cyan]"
                f" (mode={summary.mode}) — nothing to do.[/yellow]"
            )
            return
        console.print(
            f"[bold]Dry-run plan[/bold] for project [cyan]{summary.bundle_project}[/cyan]"
            f" (mode={summary.mode}):"
        )
        if summary.plan:
            plan_table = Table(title="Would dispatch", show_lines=False)
            plan_table.add_column("dataset_id", style="cyan")
            plan_table.add_column("layer")
            for node in summary.plan:
                plan_table.add_row(node.dataset_id, node.layer)
            console.print(plan_table)
        if summary.prereqs:
            prereqs_table = Table(title="Extra-plan prerequisites (must exist on disk)")
            prereqs_table.add_column("dataset_id", style="cyan")
            prereqs_table.add_column("layer")
            prereqs_table.add_column("consumer")
            prereqs_table.add_column("table path", overflow="fold")
            for dep in summary.prereqs:
                prereqs_table.add_row(
                    dep.dataset_id, dep.layer, dep.consumer, dep.table_path,
                )
            console.print(prereqs_table)
        return

    # Normal run — per-step table.
    table = Table(
        title=f"Run summary — {summary.bundle_project} ({summary.mode})",
        show_lines=False,
    )
    for col in ("dataset_id", "layer", "status", "row_count", "duration_s"):
        table.add_column(col)
    for step in summary.steps:
        # `resumed_skipped` is cyan — distinguishes carry-forwards
        # (no work done, but explicitly recorded) from cascade/abort
        # skips (work was needed but pre-empted).
        status_color = {
            "success": "green",
            "failed": "red",
            "skipped": "yellow",
            "deferred": "dim",
            "resumed_skipped": "cyan",
        }.get(step.status, "white")
        status_display = step.status.upper()
        if step.status in ("skipped", "resumed_skipped") and step.skip_reason:
            status_display = f"{status_display} ({step.skip_reason})"
        table.add_row(
            step.dataset_id,
            step.layer,
            f"[{status_color}]{status_display}[/{status_color}]",
            str(step.row_count) if step.row_count is not None else "-",
            f"{step.duration_seconds:.2f}",
        )
    console.print(table)

    # Synthetic gate-failure RunSteps (dataset_id starts + ends with
    # double-underscore) carry a multi-line error_message
    # with the AIDPF code + remediation runbook. The table cell would
    # truncate the message; render the full text below the table so
    # operators see the actionable guidance.
    for step in summary.steps:
        if (
            step.status == "failed"
            and step.dataset_id.startswith("__")
            and step.dataset_id.endswith("__")
            and step.error_message
        ):
            console.print(
                f"\n[bold red]Gate failure — {step.dataset_id}[/bold red]"
            )
            console.print(step.error_message)

    # Summary counters. `resumed_skipped` shows up only on a resumed
    # run — kept off the line for normal runs so the common case stays
    # terse.
    counters = [
        f"[green]{summary.succeeded} success[/green]",
        f"[red]{summary.failed} failed[/red]",
        f"[yellow]{summary.skipped} skipped[/yellow]",
    ]
    if summary.resumed_skipped:
        counters.append(f"[cyan]{summary.resumed_skipped} resumed-skipped[/cyan]")
    counters.append(f"[dim]{summary.deferred} deferred[/dim]")
    console.print(
        f"\nrun_id=[dim]{summary.run_id}[/dim] · "
        + " · ".join(counters)
        + f" · total {summary.total_duration_seconds:.2f}s"
    )

    # schemaPatches provenance (FR-9): a patched landing must be visible
    # from the run output, sourced from the EFFECTIVE adapter plan.
    applied_patches = getattr(summary, "applied_schema_patches", None)
    if applied_patches:
        for _ds, _cols in sorted(applied_patches.items()):
            console.print(
                f"[yellow]bronze {_ds} landed with schemaPatches: "
                f"{', '.join(_cols)} (read-side; declared types restored + "
                f"integrity-guarded)[/yellow]"
            )

    # Recommendations footer: auto-correction by preflight emits one entry per
    # PVO whose schema diverged from the catalog. Operator should add these to
    # bundle.fusion.schemaOverrides to skip the discovery probe + WARN on
    # subsequent runs.
    if summary.recommendations:
        console.print(
            f"\n[bold yellow]Recommendations[/bold yellow] "
            f"(auto-corrected this run):"
        )
        for rec in summary.recommendations:
            console.print(f"  [dim]•[/dim] {rec}")


def status(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    console: Console | None = None,
) -> int:
    """Show last-run summary per dataset (reads ``fusion_autopilot_state``).

    Should-fix-5 (2026-05-17): returns ONE row per dataset_id (the latest),
    not every historical row. Includes `skip_reason` so cascade-vs-abort
    is visible to the operator without grepping `error_message`.
    """
    console = console or Console()
    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1
    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
    paths = TablePaths.from_bundle(bundle)
    state_table = paths.bronze("fusion_autopilot_state")

    # Latest-per-dataset query via row_number window. Selects skip_reason
    # so the renderer can show cascade vs aborted on `status='skipped'` rows.
    #
    # Reserved ``__*__`` synthetic ids (the run manifest + gate markers) are
    # audit-only rows retained for resume/drift — they are NOT operator-facing
    # datasets, so they are excluded from the status view and any completion-
    # health rollup (else a fully-successful run would forever show
    # ``__run_manifest__`` as deferred/aborted and trip health checks).
    latest_query = f"""
        WITH ranked AS (
          SELECT
            dataset_id, layer, mode, last_watermark, last_run_at, status,
            row_count, error_message, skip_reason, duration_seconds,
            ROW_NUMBER() OVER (
              PARTITION BY dataset_id
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {state_table}
          WHERE dataset_id NOT LIKE '\\_\\_%\\_\\_'
        )
        SELECT
          dataset_id, layer, mode, last_watermark, last_run_at, status,
          row_count, error_message, skip_reason, duration_seconds
        FROM ranked
        WHERE rn = 1
        ORDER BY layer, dataset_id
    """

    # Run-level verdict banner (FR-15.8, design §9.2.5): a SEPARATE query
    # over the reserved ``__…__`` rows only — the per-dataset query above
    # stays byte-identical and keeps excluding them. Keyed on the latest
    # reserved row of ANY kind (pre-feature runs and AIDPF-4022
    # manifest-commit failures leave no manifest row).
    reserved_query = f"""
        SELECT run_id, dataset_id, status, error_message, last_run_at
        FROM {state_table}
        WHERE dataset_id LIKE '\\_\\_%\\_\\_'
    """

    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError:
        console.print(
            f"[yellow]pyspark not available locally; cannot read {state_table}[/yellow]"
        )
        console.print(
            "Run this query inside an AIDP notebook session:\n"
            f"  [cyan]{latest_query.strip()}[/cyan]"
        )
        console.print(
            "For the run-level verdict, also run:\n"
            f"  [cyan]{reserved_query.strip()}[/cyan]"
        )
        return 0

    spark = SparkSession.builder.appName("aidp-fusion-autopilot-status").getOrCreate()
    try:
        df = spark.sql(latest_query)
        rows = df.collect()
    except Exception as exc:
        console.print(f"[red]could not read {state_table}:[/red] {exc}")
        return 1

    _render_run_banner(console, spark, reserved_query)

    if not rows:
        console.print(
            f"[yellow]{state_table} is empty — no runs recorded yet[/yellow]"
        )
        return 0

    table = Table(title=f"{state_table} (latest per dataset)")
    for col in (
        "dataset_id", "layer", "mode", "last_watermark", "last_run_at",
        "status", "skip_reason", "row_count",
    ):
        table.add_column(col)
    for r in rows:
        status_val = str(r["status"])
        if status_val == "skipped" and r["skip_reason"]:
            status_val = f"{status_val} ({r['skip_reason']})"
        table.add_row(
            str(r["dataset_id"]),
            str(r["layer"]),
            str(r["mode"]),
            str(r["last_watermark"]) if r["last_watermark"] else "-",
            str(r["last_run_at"]),
            status_val,
            str(r["skip_reason"]) if r["skip_reason"] else "-",
            str(r["row_count"]) if r["row_count"] is not None else "-",
        )
    console.print(table)
    return 0


def _render_run_banner(console: Console, spark, reserved_query: str) -> None:
    """Render the run-level verdict from the reserved rows (best-effort —
    the per-dataset table must render even if this read fails, e.g. a
    pre-feature state table without newer rows is simply banner-less)."""
    from .run_reconcile import banner_verdict

    try:
        reserved = [r.asDict() for r in spark.sql(reserved_query).collect()]
    except Exception:
        return
    banner = banner_verdict(reserved)
    if banner is None:
        return
    style = {"COMPLETED": "green", "ABORTED": "red", "UNPROVEN": "yellow"}[
        banner.label
    ]
    line = f"RUN {banner.run_id}: {banner.label}"
    if banner.codes:
        line += "  " + " ".join(banner.codes)
    if banner.detail:
        line += f"  — {banner.detail}"
    console.print(f"[bold {style}]{line}[/bold {style}]")


__all__ = ["run", "status"]
