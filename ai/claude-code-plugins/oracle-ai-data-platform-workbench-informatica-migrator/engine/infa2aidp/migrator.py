"""Migration orchestration: parse -> convert -> generate -> score.

This is the library entrypoint the CLI's ``migrate`` command delegates to.
It exists so that path selection (LLM-first / agentic / rule-based),
parallel batch processing, side-by-side comparison reports, and confidence
scoring live in one place instead of being written inline in ``cli.py``.
"""
from __future__ import annotations

import glob
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class MigrationFailedError(RuntimeError):
    """Raised when a :func:`run_migration` call produced zero notebooks from
    a non-empty input list -- i.e. every input failed to parse (or, on the
    batch path, every input failed outright), so the run has nothing to
    show and must not report success.

    A *partial* failure (at least one notebook produced) never raises this;
    it is surfaced instead as a per-input warning plus an accurate count in
    the returned :class:`MigrationRunResult`. See spec section 11.
    """


@dataclass
class MappingOutcome:
    """Per-mapping result of a migration run."""
    xml_path: str
    mapping_name: str
    path_used: str  # "llm", "agentic", "rule-based"
    notebook_path: str = ""
    score: int = 0
    comparison_path: str = ""
    # source_fidelity (extended to every path in
    # Part A) -- the independent, non-circular check that compares the
    # notebook against the RAW export rather than the parser's own spec.
    # Populated whenever a notebook was actually produced AND a source path
    # was available to check against, regardless of whether the LLM,
    # agentic, or rule-based path produced that notebook.
    fidelity_checked: bool = False
    fidelity_has_gaps: bool = False
    fidelity_summary: str = ""


@dataclass
class MigrationRunResult:
    """Aggregate result of a :func:`run_migration` call."""
    output_dir: str
    xml_files: int = 0
    notebooks: int = 0
    workflows: int = 0
    optimize_suggestions: int = 0
    outcomes: list[MappingOutcome] = field(default_factory=list)
    confidence_summary: dict = field(default_factory=dict)
    fidelity_summary: dict = field(default_factory=dict)
    # (mapping_name, TargetDDL) for targets whose generated DDL carries a
    # REVIEW comment -- an aggregate-fed column, or a declaration we could
    # not translate faithfully.
    ddl_warnings: list = field(default_factory=list)
    # [(notebook_path, [problem, ...])] -- notebooks this run wrote that
    # cannot run as written. A migrator defect, reported rather than
    # delivered silently.
    broken_notebooks: list = field(default_factory=list)
    # [(mapping, folder, written_as)] -- same folder AND same mapping name as
    # another in this run. Kept, suffixed, reported; never silently dropped.
    notebook_collisions: list = field(default_factory=list)
    # {workflow_name: [review item, ...]} -- everything in a source workflow
    # that could not be translated into the generated job.
    workflow_reviews: dict = field(default_factory=dict)
    # {workflow_name: [item, ...]} -- values the generator had to supply
    # because the export does not carry them, and which may be wrong. Kept
    # apart from workflow_reviews so "translated whole" stays meaningful.
    workflow_assumptions: dict = field(default_factory=dict)
    # {workflow_name: [task notebookPath, ...]} -- what each generated job
    # runs, so a NAME CLASH can say which job got which of the two notebooks.
    workflow_notebooks: dict = field(default_factory=dict)
    batch_summary: Optional[object] = None  # infa2aidp.batch.BatchSummary


def format_run_summary(result: "MigrationRunResult") -> str:
    """Human-readable summary of a completed run.

    Lives here rather than in ``cli.py`` so the CLI stays a dispatcher:
    every caller of :func:`run_migration` gets the same summary, and a new
    result field does not widen the CLI.
    """
    lines = [
        f"Migration complete: {result.notebooks} notebook(s), "
        f"{result.workflows} workflow(s) -> {os.path.abspath(result.output_dir)}"
    ]
    n_lost = sum(len(v) for v in result.workflow_reviews.values())
    if n_lost:
        n_wf = len([k for k, v in result.workflow_reviews.items() if v])
        lines.append(
            f"Orchestration: {n_wf}/{result.workflows} workflow(s) did not translate "
            f"completely, {n_lost} item(s) -- read workflows/*.review.md before "
            f"enabling a job"
        )
    # Schedule assumptions and every other kind are reported separately.
    # Counting them together under a "Schedules:" heading sent operators to
    # check a job schedule that was correct, when the real assumption was
    # about link semantics -- and buried the schedule warning on runs where
    # it did matter.
    all_assumptions = [a for v in result.workflow_assumptions.values() for a in v]
    tz_assumed = [a for a in all_assumptions if "timezone" in a.lower()]
    other_assumed = [a for a in all_assumptions if a not in tz_assumed]
    if tz_assumed:
        lines.append(
            f"Schedules: {len(tz_assumed)} timezone assumption(s) -- a converted "
            f"schedule was emitted as UTC because the export records no timezone. "
            f"CHECK AND CORRECT THE JOB SCHEDULE IN AIDP before enabling it, or "
            f"re-run with --schedule-timezone."
        )
    if other_assumed:
        lines.append(
            f"Orchestration assumptions: {len(other_assumed)} item(s) to confirm -- "
            f"read workflows/*.review.md"
        )
    if result.confidence_summary:
        lines.append(f"Auto-convertible: {result.confidence_summary['auto_rate']}%")
    if result.fidelity_summary:
        fs = result.fidelity_summary
        lines.append(
            f"Source fidelity: {fs['with_gaps']}/{fs['checked']} mapping(s) checked "
            f"against the raw export have gaps -- see {fs['report_path']}"
        )
    if getattr(result, "notebook_collisions", None):
        _c = result.notebook_collisions
        lines.append(
            f"NAME CLASH: {len(_c)} mapping(s) share a folder and name with "
            f"another in this run ({', '.join(n for n, _f, _a in _c[:3])}"
            f"{', ...' if len(_c) > 3 else ''}). Both were kept, the later one "
            f"suffixed __2 -- but two exports disagree about the same mapping, "
            f"so decide which is current before deploying either."
        )
        # Which job runs which copy. Without it the operator deciding "which
        # is current" has to open every job definition to see what deploying
        # one of them would actually run.
        _jobs = getattr(result, "workflow_notebooks", None) or {}
        for _m, _f, _alt in _c[:3]:
            _pairs = []
            for _nb in sorted({f"nb_{_m}.ipynb", _alt}):
                _who = sorted(j for j, ps in _jobs.items()
                              if any(p.endswith(f"/{_f}/{_nb}") for p in ps))
                _pairs.append(f"{', '.join(_who) or '(no job)'} -> {_f}/{_nb}")
            lines.append(f"  {_m}: " + "; ".join(_pairs))
    if getattr(result, "broken_notebooks", None):
        lines.append(
            f"BROKEN: {len(result.broken_notebooks)} generated notebook(s) cannot "
            f"run as written -- see {os.path.join('reports', 'broken_notebooks.md')}. "
            f"This is a defect in the migrator, not in the export; the mapping(s) "
            f"need re-generating once it is fixed."
        )
    if getattr(result, "ddl_warnings", None):
        lines.append(
            f"Target DDL: {len(result.ddl_warnings)} target(s) have a column whose "
            f"declared type will not match what the notebook computes "
            f"(aggregates widen; a Sequence Generator is always BIGINT) -- "
            f"read the REVIEW comments in ddl/ before applying"
        )
    return "\n".join(lines)


def _emit_workflows(parsed, notebook_paths: dict, output_dir: str, result,
                    parameter_file=None, schedule_timezone: str | None = None) -> None:
    """Write one AIDP job definition (and, when needed, a review file) per
    workflow in ``parsed``."""
    if not parsed.workflows:
        return
    from .generators.workflow_generator import WorkflowGenerator
    wf_gen = WorkflowGenerator(schedule_timezone=schedule_timezone)
    wf_dir = os.path.join(output_dir, "workflows")
    os.makedirs(wf_dir, exist_ok=True)
    for workflow in parsed.workflows:
        tr = wf_gen.generate(
            workflow, notebook_paths,
            sessions={s.name: s for s in parsed.sessions},
            parameter_file=parameter_file,
        )
        with open(os.path.join(wf_dir, f"{workflow.name}.json"), "w", encoding="utf-8") as f:
            json.dump(tr.job, f, indent=2, default=str)
        result.workflows += 1
        result.workflow_notebooks[workflow.name] = [
            t.get("notebookPath", "") for t in tr.job.get("tasks", [])
        ]
        # Review items live BESIDE the job definition, never in it: the
        # definition is POSTed verbatim to the AIDP jobs API. A companion
        # .review.md is what makes an incomplete translation visible rather
        # than inferable.
        if tr.needs_review:
            result.workflow_reviews[workflow.name] = list(tr.not_translated)
            result.workflow_assumptions[workflow.name] = list(tr.assumptions)
            review_path = os.path.join(wf_dir, f"{workflow.name}.review.md")
            with open(review_path, "w", encoding="utf-8") as f:
                f.write(_render_workflow_review(workflow.name, tr))


def _warn_divergent_sessions(mapping_name: str, sessions: list) -> None:
    """Warn when sessions sharing a mapping differ in how they load.

    Their $$ values travel as task parameters, but properties such as
    "Treat source rows as" and the per-target Insert/Update/Delete flags
    are baked into the one notebook, from the first session.
    """
    if len(sessions) < 2:
        return
    first = sessions[0]
    for other in sessions[1:]:
        if (other.properties != first.properties
                or other.target_load_options != first.target_load_options):
            logger.warning(
                "Mapping %s is run by sessions %s and %s with different session "
                "properties or target load options; the notebook follows %s. "
                "Review the load strategy for %s.",
                mapping_name, first.name, other.name, first.name, other.name,
            )


def _render_workflow_review(name: str, tr) -> str:
    """Companion report for one generated job definition.

    Written when a workflow either lost something in translation or carries
    an assumption. The two are kept in separate sections: the absence of a
    "Not translated" section means the workflow came across whole, and that
    claim is only worth making if assumptions cannot dilute it.
    """
    job = tr.job
    scheduled = "schedule" in job
    lines = [f"# Orchestration review: {name}", ""]

    if tr.not_translated:
        lines.append(
            f"**{len(tr.not_translated)} construct(s) in the source workflow were "
            f"not translated.** The generated job is incomplete until they are "
            f"handled."
        )
    else:
        lines.append(
            "Every construct in the source workflow was translated. The items "
            "below are assumptions to confirm, not gaps."
        )

    lines += ["", "## Schedule", ""]
    if scheduled:
        cron = job["schedule"].get("quartzCronExpression", "?")
        tz = job["schedule"].get("timezoneId", "?")
        tz_assumed = any("timezone" in a.lower() for a in tr.assumptions)
        if tz_assumed:
            lines.append(
                f"Converted from the source schedule: `{cron}` ({tz}). **Confirm the "
                f"timezone before enabling the job** -- see Assumptions below."
            )
        else:
            # The zone was declared (--schedule-timezone), so there is no
            # timezone assumption to read. Saying "see Assumptions below"
            # anyway pointed at an item that does not exist.
            lines.append(
                f"Converted from the source schedule: `{cron}` ({tz}). The timezone "
                f"was supplied for this migration rather than assumed, so the hour "
                f"is as intended. The job is still created PAUSED -- unpause it once "
                f"you have checked the rest of this file."
            )
    else:
        lines.append(
            "**This job has no schedule.** That is deliberate: a schedule that "
            "could not be derived from the export is omitted rather than guessed, "
            "so set it by hand in AIDP. See the items below for the source values."
        )

    if tr.not_translated:
        lines += ["", "## Not translated", ""]
        lines += [f"{i}. {item}" for i, item in enumerate(tr.not_translated, 1)]

    if tr.assumptions:
        lines += ["", "## Assumptions to confirm", ""]
        lines += [f"{i}. {item}" for i, item in enumerate(tr.assumptions, 1)]

    lines += [
        "",
        "## What was generated",
        "",
        f"- Tasks: {len(job.get('tasks', []))}",
        f"- Scheduled: {'yes' if scheduled else 'no'}",
        "",
    ]
    return "\n".join(lines)



def _common_input_dir(inputs: list[str]) -> str:
    """Best-effort common ancestor directory of a list of XML file paths."""
    if len(inputs) == 1:
        return str(Path(inputs[0]).resolve().parent)
    try:
        return os.path.commonpath([str(Path(p).resolve()) for p in inputs])
    except ValueError:
        return str(Path(inputs[0]).resolve().parent)


def _unrequested_files_under(inputs: list[str], batch_dir: str) -> list[str]:
    """Files ``BatchMigrator.migrate_folder`` would pick up under
    *batch_dir* (it recursively rescans for every ``.xml``/``.json``) that
    were not in the caller's explicit *inputs* list.

    ``run_migration`` is a public library entrypoint, not just something the
    CLI calls after ``rglob``-ing a directory the user named -- a direct
    caller can hand it a scattered file list. Widening a batch run to
    whatever else happens to sit in the computed ancestor directory would
    silently migrate files nobody asked for.

    Both sides MUST be normalised the same way. ``Path.resolve()`` follows
    symlinks (e.g. macOS's ``/tmp`` -> ``/private/tmp``,
    ``/var/folders/...`` -> ``/private/var/folders/...``); a plain
    ``os.path.abspath`` does not. Mixing the two makes every requested file
    look "unrequested" on any platform/mount where that distinction exists.
    """
    requested = {str(Path(p).resolve()) for p in inputs}
    found = {
        str(p.resolve()) for p in Path(batch_dir).rglob("*")
        if p.is_file() and p.suffix.lower() in (".xml", ".json")
    }
    return sorted(found - requested)


def _rule_based_transformations(mapping, converter) -> dict[str, str]:
    """Per-transformation PySpark code, via the plain rule-based converter.

    Used for comparison/confidence purposes regardless of how the mapping's
    actual notebook was generated (LLM or otherwise) -- both
    ComparisonGenerator and ConfidenceScorer need a name -> code map to
    assess, and the rule-based conversion is always available.
    """
    return {tx.name: "\n".join(converter.convert(tx)) for tx in mapping.transformations}


def _compare_and_score(mapping, transformations, comparison_gen, scorer, all_scores, output_dir) -> str:
    """Run whichever of comparison/confidence-scoring is active for one
    mapping, given its already-built name -> PySpark-code map. Returns the
    comparison report path, or "" if emit_comparison is off."""
    if not (comparison_gen or scorer):
        return ""
    comparison_path = ""
    if comparison_gen:
        comp_dir = os.path.join(output_dir, "comparisons")
        os.makedirs(comp_dir, exist_ok=True)
        entries = comparison_gen.generate(mapping, transformations)
        comparison_path = os.path.join(comp_dir, f"{mapping.name}_comparison.md")
        comparison_gen.export_markdown(entries, mapping.name, comparison_path)
        comparison_gen.export_html(
            entries, mapping.name,
            os.path.join(comp_dir, f"{mapping.name}_comparison.html"),
        )
    if scorer is not None:
        all_scores.extend(scorer.score_mapping(mapping, transformations))
    return comparison_path


def _emit_lineage(mapping, output_dir: str) -> None:
    from .generators.lineage_generator import LineageGenerator
    lin_gen = LineageGenerator()
    lin_dir = os.path.join(output_dir, "lineage")
    os.makedirs(lin_dir, exist_ok=True)
    lin_gen.export_lineage_report(lin_gen.generate(mapping), os.path.join(lin_dir, mapping.name))


def _validate_generated(output_dir: str) -> list:
    """Check every notebook this run wrote, and name the broken ones.

    The generator can emit a notebook that parses, passes every string
    assertion, and still dies with NameError on its first cell -- a
    variable the dataflow bookkeeping named but never assigned. Two such
    notebooks turned up during breadth testing, and nothing in the run
    would have mentioned them: the summary counted them as generated and
    reported a high auto-convertible rate.

    So `migrate` now reads back what it wrote. A notebook that references
    an unassigned name, or that prepares a per-consumer input copy and then
    never uses it, is listed by name. Cheap, and it turns a class of
    silent defect into a reported one.

    Returns [(notebook_path, [problem, ...])].
    """
    import ast as _ast
    import json as _json
    from .generators.code_validation import abandoned_dataframes, unresolved_names

    broken = []
    for path in sorted(glob.glob(os.path.join(output_dir, "**", "*.ipynb"),
                                 recursive=True)):
        try:
            nb = _json.load(open(path, encoding="utf-8"))
            code = "\n".join(
                "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
                for c in nb.get("cells", []) if c.get("cell_type") == "code"
            )
        except Exception as exc:
            broken.append((path, [f"could not be read back: {exc}"]))
            continue
        # A notebook that REFUSES is not a notebook that broke. When the
        # generator could not convert something it raises NotImplementedError
        # with a REVIEW REQUIRED message, which is a declared outcome
        # reported through the review path. This check is about the other
        # case: a notebook that looks finished and dies anyway. Conflating
        # them would fill the report with items that are working as intended,
        # and a report full of those gets ignored.
        declared_refusal = ("REVIEW REQUIRED" in code
                            and "raise NotImplementedError" in code)

        problems = []
        try:
            _ast.parse(code)
        except SyntaxError as exc:
            problems.append(f"does not parse: {exc}")
        else:
            missing = unresolved_names(code)
            if missing:
                problems.append(
                    "reads name(s) never assigned, so it raises NameError before "
                    f"touching data: {', '.join(missing)}"
                )
            orphaned = abandoned_dataframes(code)
            if orphaned:
                problems.append(
                    "prepared an input copy that is never read, so a join or "
                    f"route was dropped: {', '.join(orphaned)}"
                )
        if problems and not declared_refusal:
            broken.append((path, problems))
        elif problems and declared_refusal:
            # Still surfaced, but as what it is.
            pass
    return broken


def _emit_ddl(mapping, output_dir: str) -> list:
    """Write one CREATE TABLE per target, from the export's declared types.

    Separate from the notebook on purpose: the notebook MERGEs and emits no
    DDL, and creating tables in a catalog should not be a side effect of
    converting code. A human applies these. Returns the TargetDDL objects so
    the caller can surface the columns whose declared type will not match
    what the notebook computes.
    """
    from .generators.ddl_generator import mapping_ddl
    ddls = mapping_ddl(mapping)
    if not ddls:
        return []
    ddl_dir = os.path.join(output_dir, "ddl")
    os.makedirs(ddl_dir, exist_ok=True)
    for d in ddls:
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", d.table or d.target_name)
        with open(os.path.join(ddl_dir, f"{safe}.sql"), "w") as fh:
            fh.write(d.sql)
    return ddls


def _finalize_confidence(scorer, all_scores: list, output_dir: str) -> dict:
    if not (scorer and all_scores):
        return {}
    from .handlers.confidence_scorer import ConversionConfidence
    high = sum(1 for s in all_scores if s.confidence == ConversionConfidence.HIGH)
    medium = sum(1 for s in all_scores if s.confidence == ConversionConfidence.MEDIUM)
    low = sum(1 for s in all_scores if s.confidence == ConversionConfidence.LOW)
    manual = sum(1 for s in all_scores if s.confidence == ConversionConfidence.MANUAL)
    total = len(all_scores)
    summary = {
        "high": high, "medium": medium, "low": low, "manual": manual,
        "auto_rate": round((high + medium) / total * 100, 1) if total else 0,
    }
    conf_dir = os.path.join(output_dir, "reports")
    os.makedirs(conf_dir, exist_ok=True)
    scorer.generate_confidence_report(all_scores, conf_dir)
    return summary


def _finalize_fidelity(outcomes: list[MappingOutcome], output_dir: str) -> dict:
    """Aggregate source_fidelity results into a summary dict + a markdown
    report, the same pattern ``_finalize_confidence`` uses for confidence
    scoring -- so a fidelity gap reaches the report/result object instead
    of only ever being logged. ``source_fidelity`` is wired into
    ``LLMNotebookGenerator.generate``; closed the remaining
    gap so the rule-based/agentic paths run it too, not only the LLM path).

    Returns ``{}`` only if no mapping was ever checked at all -- e.g. every
    input failed to produce a notebook, or no source path was available.
    """
    checked = [o for o in outcomes if o.fidelity_checked]
    if not checked:
        return {}

    with_gaps = [o for o in checked if o.fidelity_has_gaps]
    summary = {
        "checked": len(checked),
        "with_gaps": len(with_gaps),
    }

    report_dir = os.path.join(output_dir, "reports")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, "fidelity_report.md")

    lines = [
        "# Source Fidelity Report",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Independent, non-circular check: compares each generated "
        "notebook against the RAW Informatica export, not the parser's own "
        "spec -- so it can catch a construct the parser silently dropped, "
        "even when the (circular) validator score is 100/100.",
        "",
        f"{len(with_gaps)}/{len(checked)} mapping(s) checked have fidelity gaps.",
        "",
        "How to read this number. Expression output fields and connector "
        "target fields are matched against comment-free code, so naming one "
        "in a comment does not count. Transformation NAMES are matched "
        "against the code text including comments, because a transformation "
        "has no runtime artifact in PySpark -- an Expression becomes "
        "withColumn calls, not a symbol named EXP_TAX -- so a comment is "
        "its only possible representation. That means this check cannot "
        "distinguish a generator that handles a Source Qualifier from one "
        "that merely mentions it. Compare fidelity counts only between runs "
        "of the same generator, and read the named gaps rather than the "
        "ratio.",
        "",
    ]
    if with_gaps:
        lines.append("## Mappings with gaps")
        lines.append("")
        for o in with_gaps:
            lines.append(f"### {o.mapping_name}")
            lines.append("")
            lines.append("```")
            lines.append(o.fidelity_summary)
            lines.append("```")
            lines.append("")
    else:
        lines.append("No gaps found in any checked mapping.")
        lines.append("")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    summary["report_path"] = report_path
    return summary


def run_migration(
    inputs: list[str],
    output_dir: str,
    *,
    use_llm: bool = True,
    agentic: bool = False,
    custom_rules_path: str | None = None,
    params_path: str | None = None,
    max_workers: int = 1,
    emit_comparison: bool = False,
    score_confidence: bool = True,
    skip_lineage: bool = False,
    skip_optimize: bool = False,
    target_catalog_type: str = "delta",
    schedule_timezone: str | None = None,
) -> MigrationRunResult:
    """Orchestrate a migration run over one or more Informatica XML exports.

    Args:
        inputs: Informatica export file paths -- PowerCenter XML or
            IICS/IDMC JSON (already expanded from a file-or-directory CLI
            argument -- see ``cli._collect_input_files``).
        output_dir: Directory notebooks/workflows/reports are written under.
        use_llm: Try Claude-generated notebooks first; falls back to
            rule-based conversion (or the agentic pipeline) when the LLM is
            unavailable or a mapping fails to generate.
        agentic: Use the spec -> generate -> validate -> fix agentic
            pipeline as the fallback when LLM-first is off or unavailable.
        custom_rules_path: Optional custom conversion rules file, consumed
            by the agentic pipeline's code-gen agent. Not compatible with
            the parallel batch path (see ``max_workers``) -- raises
            ``ValueError`` rather than silently dropping it.
        params_path: Optional Informatica ``.par`` parameter file; when
            given, a parameter report is written alongside the notebooks.
            Honored on every path, including the parallel batch path, since
            it only produces a standalone report and never feeds
            per-mapping conversion.
        max_workers: When > 1 (and an LLM is available, and there is more
            than one input file, and the inputs cleanly cover their common
            ancestor directory -- see ``_unrequested_files_under``) the run
            is delegated to :class:`infa2aidp.batch.BatchMigrator` for
            parallel LLM generation instead of the serial per-mapping loop.
            `emit_comparison`, `score_confidence` and `skip_lineage` are
            still honored on this path: they're applied per-mapping after
            ``BatchMigrator.migrate_folder`` returns, using its per-result
            ``mapping_name``/``xml_path`` to re-derive what each needs.
        emit_comparison: Write a side-by-side Informatica-vs-PySpark
            fidelity report per mapping via
            :class:`infa2aidp.generators.comparison_generator.ComparisonGenerator`.
        score_confidence: Score every converted transformation via
            :class:`infa2aidp.handlers.confidence_scorer.ConfidenceScorer`
            and write an aggregate confidence report. This is the review
            gate: unsupported transformations must surface here as
            LOW/MANUAL items, never as silent approximations.
        skip_lineage: Skip field-level lineage report generation.
        skip_optimize: Skip the post-run Spark optimization-suggestion scan.
        target_catalog_type: Which
            :class:`infa2aidp.generators.write_strategies.WriteStrategy` the
            rule-based notebook generator uses for every target write cell
            -- ``"delta"`` (managed Delta, the default) or ``"adw"``
            (external ADW/ALH/ATP catalog). Always an explicit input --
            never inferred from a table name or connection string (
            /). Only affects the rule-based
            :class:`~infa2aidp.generators.notebook_generator.NotebookGenerator`
            path; the LLM-first and parallel-batch paths are unaffected by
            this release.
        schedule_timezone: IANA zone the source Integration Service ran in,
            e.g. ``America/New_York``. PowerCenter records a workflow's
            STARTTIME in that service's local time and stores no zone, so
            the export cannot supply it. Passing it makes a converted
            schedule correct rather than plausible; omitting it falls back
            to UTC and reports the assumption in the workflow's review file.

    Returns:
        A :class:`MigrationRunResult` with per-mapping outcomes, the
        confidence summary, and (when the batch path was taken) the raw
        :class:`infa2aidp.batch.BatchSummary`.

    Raises:
        ValueError: if ``custom_rules_path`` is given and the run would
            otherwise take the parallel batch path -- the batch path only
            runs LLM-first/rule-based conversion, never the
            custom-rules-aware agentic pipeline, so honoring it there is
            structurally impossible. Run serially or drop the option.
        MigrationFailedError: if every input in a non-empty ``inputs`` list
            failed to parse (or otherwise produce a notebook), so the run
            has zero notebooks to show. A partial failure -- some inputs
            parsed, some didn't -- never raises this; it stays a warning
            plus an accurate count in the returned result. See spec
            section 11.
        ValueError: if ``schedule_timezone`` is not a zone AIDP accepts
            (see ``workflow_generator.validate_schedule_timezone``) --
            raised before anything is written.
    """
    # Checked before any output exists. The generator checks it too, but it
    # is first built when the first export's workflows are emitted -- after
    # that export's notebooks and DDL are on disk -- so a typo used to leave
    # a half-written output directory behind the error.
    if schedule_timezone is not None:
        from .generators.workflow_generator import validate_schedule_timezone
        validate_schedule_timezone(schedule_timezone)

    _ddl_warnings: list = []
    _written_notebooks: set = set()
    _nb_collisions: list = []
    llm = None
    if use_llm:
        from .handlers.codellama_handler import LLMHandler
        llm = LLMHandler()
        if llm.is_available():
            logger.info("LLM connected -- Claude API (%s)", llm.claude_model)
        else:
            logger.warning("ANTHROPIC_API_KEY not set -- falling back to rule-based")
            llm = None

    # Parallel batch path: only worth it for LLM-first generation across
    # more than one mapping at a time, and only safe when the inputs are
    # exactly what BatchMigrator would find by rescanning their common
    # ancestor directory. Resolved before any filesystem side effect so
    # the ValueError below is genuinely raised before any work is done.
    batch_dir = None
    will_batch = bool(max_workers > 1 and llm and len(inputs) > 1)
    if will_batch:
        batch_dir = _common_input_dir(inputs)
        extra = _unrequested_files_under(inputs, batch_dir)
        if extra:
            logger.warning(
                "Batch mode disabled: %d file(s) under %s were not in the "
                "requested input list (e.g. %s) -- falling back to serial "
                "processing so the run migrates exactly what was requested. "
                "Pass exactly the directory you want batched to use "
                "--workers > 1.",
                len(extra), batch_dir, extra[0],
            )
            will_batch = False

    if will_batch and custom_rules_path:
        raise ValueError(
            "--custom-rules is not supported with --workers > 1 (the "
            "parallel batch path only runs LLM-first/rule-based conversion, "
            "not the custom-rules-aware agentic pipeline). Run serially "
            "(--workers 1) or omit --custom-rules."
        )

    os.makedirs(output_dir, exist_ok=True)
    result = MigrationRunResult(output_dir=output_dir, xml_files=len(inputs))

    # params_path only ever produces a standalone report; it never feeds
    # per-mapping conversion, so it's honored on every path -- including
    # the parallel batch path -- with no special-casing needed.
    parameter_file = None
    if params_path and os.path.isfile(params_path):
        from .parsers.parameter_parser import ParameterFileParser
        par_parser = ParameterFileParser()
        pr = par_parser.parse(params_path)
        parameter_file = pr
        report_dir = os.path.join(output_dir, "reports")
        os.makedirs(report_dir, exist_ok=True)
        par_parser.generate_report([pr], os.path.join(report_dir, "parameter_report.md"))

    comparison_gen = None
    if emit_comparison:
        from .generators.comparison_generator import ComparisonGenerator
        comparison_gen = ComparisonGenerator()

    scorer = None
    if score_confidence:
        from .handlers.confidence_scorer import ConfidenceScorer
        scorer = ConfidenceScorer()
    all_scores: list = []

    from .converters.transformation_converter import TransformationConverter

    converter = TransformationConverter()

    # Populated with (input_path, error) pairs for inputs that failed to
    # parse -- used below to build a clear MigrationFailedError message if
    # *every* input fails, and otherwise just to have logged a warning.
    parse_failures: list[tuple[str, str]] = []

    if will_batch:
        from .batch import BatchMigrator
        migrator = BatchMigrator(llm_handler=llm, max_workers=max_workers)
        summary = migrator.migrate_folder(batch_dir, output_dir)
        result.batch_summary = summary
        result.notebooks = summary.success + summary.fallback

        for r in summary.results:
            outcome = MappingOutcome(
                xml_path=r.xml_path,
                mapping_name=r.mapping_name,
                path_used="llm" if r.status == "success" else "rule-based",
                notebook_path=r.notebook_path,
                score=r.score,
                fidelity_checked=getattr(r, "fidelity_checked", False),
                fidelity_has_gaps=getattr(r, "fidelity_has_gaps", False),
                fidelity_summary=getattr(r, "fidelity_summary", ""),
            )
            # Only a mapping that actually produced a notebook has anything
            # to compare/score/trace lineage for.
            if r.notebook_path and (comparison_gen or scorer or not skip_lineage):
                try:
                    # BatchMigrator's own rglob picks up both PowerCenter
                    # XML and IICS JSON and routes each through
                    # detect_and_parse_file (batch.py:_migrate_single) --
                    # match that here, not a hardcoded XML-only parser, or
                    # every JSON/IICS mapping silently loses this step.
                    from .parsers.format_detector import detect_and_parse_file
                    parsed = detect_and_parse_file(r.xml_path)
                except Exception as exc:
                    logger.warning("Post-batch re-parse failed for %s: %s", r.xml_path, exc)
                    parsed = None
                # BatchMigrator returns one result per MAPPING, so match
                # this result's mapping by name rather than taking the first
                # in the file. It used to convert only mappings[0] and this
                # matched that; now that every mapping is migrated, taking
                # the first would attach one mapping's comparison, confidence
                # and lineage to every other mapping in the same file.
                mapping = None
                if parsed and parsed.mappings:
                    mapping = next(
                        (m for m in parsed.mappings if m.name == r.mapping_name),
                        parsed.mappings[0],
                    )
                if mapping is not None:
                    transformations = _rule_based_transformations(mapping, converter)
                    outcome.comparison_path = _compare_and_score(
                        mapping, transformations, comparison_gen, scorer, all_scores, output_dir
                    )
                    if not skip_lineage:
                        _emit_lineage(mapping, output_dir)
                    _ddl_warnings.extend(
                        (mapping.name, d) for d in _emit_ddl(mapping, output_dir)
                        if d.has_warnings
                    )
            result.outcomes.append(outcome)
            if r.status not in ("success", "fallback"):
                parse_failures.append((r.xml_path, r.error or "failed"))

        # Workflows. This path used to emit none at all: a parallel run
        # produced notebooks and no job definition, so the orchestration of
        # every workflow in the batch was lost without a word.
        by_file: dict[str, dict[str, str]] = {}
        for r in summary.results:
            if r.notebook_path and r.status in ("success", "fallback"):
                by_file.setdefault(r.xml_path, {})[r.mapping_name] = r.notebook_path
        for xml_path, nb_by_mapping in by_file.items():
            try:
                from .parsers.format_detector import detect_and_parse_file
                parsed = detect_and_parse_file(xml_path)
            except Exception as exc:
                logger.warning("Workflow re-parse failed for %s: %s", xml_path, exc)
                continue
            folder_of = {m.name: (m.folder or "Migrated") for m in parsed.mappings}
            notebook_paths = {
                s.name: f"/Workspace/Migrated/{folder_of.get(s.mapping_name, 'Migrated')}/"
                        f"{os.path.basename(nb_by_mapping[s.mapping_name])}"
                for s in parsed.sessions if s.mapping_name in nb_by_mapping
            }
            _emit_workflows(parsed, notebook_paths, output_dir, result, parameter_file,
                            schedule_timezone=schedule_timezone)
    else:
        from .generators.notebook_generator import NotebookGenerator
        from .models import Session

        custom_rules = None
        if custom_rules_path:
            from .converters.custom_rules import CustomRuleEngine
            custom_rules = CustomRuleEngine(custom_rules_path)
            custom_rules.validate_rules()

        llm_gen = None
        if llm:
            from .generators.llm_notebook_generator import LLMNotebookGenerator
            llm_gen = LLMNotebookGenerator(llm)

        agentic_pipeline = None
        if agentic and not llm_gen:
            from .agents.pipeline import ConversionPipeline
            from .agents.rag_store import RAGStore
            agentic_pipeline = ConversionPipeline(
                llm_handler=llm, custom_rules=custom_rules, rag_store=RAGStore()
            )

        gen = NotebookGenerator(target_catalog_type=target_catalog_type)

        from .parsers.format_detector import detect_and_parse_file

        for xml_file in inputs:
            try:
                # Route through the same format-detecting dispatcher as
                # batch.py, so a PowerCenter XML export or an IICS/IDMC JSON
                # export are both reachable here -- not just on the
                # parallel batch path. See the design notes.
                parsed = detect_and_parse_file(xml_file)
            except Exception as exc:
                logger.warning("Failed to parse %s: %s", xml_file, exc)
                parse_failures.append((xml_file, str(exc)))
                continue
            # One mapping can be run by several sessions (a delta and a full
            # load, one per region...). The notebook is generated once, from
            # the FIRST session in export order -- this used to be the last,
            # silently -- and every session of the mapping points at it; the
            # per-session $$ values reach it as job task parameters.
            sessions_of: dict[str, list] = {}
            for s in parsed.sessions:
                sessions_of.setdefault(s.mapping_name, []).append(s)
            session_map = {m: ss[0] for m, ss in sessions_of.items()}
            notebook_paths: dict[str, str] = {}

            for mapping in parsed.mappings:
                session = session_map.get(mapping.name, Session(name=f"s_{mapping.name}"))
                path_used = "rule-based"
                score = 0
                fidelity_checked = False
                fidelity_has_gaps = False
                fidelity_summary_text = ""
                notebook_code = (
                    llm_gen.generate(
                        mapping, session, output_format="ipynb", source_path=xml_file,
                    )
                    if llm_gen else None
                )
                if notebook_code:
                    path_used = "llm"
                    score = getattr(llm_gen, "_last_score", 0)
                    fidelity = getattr(llm_gen, "_last_fidelity", None)
                    if fidelity is not None:
                        fidelity_checked = True
                        fidelity_has_gaps = fidelity.has_gaps
                        fidelity_summary_text = fidelity.summary()

                # Always build a per-transformation code map -- needed for
                # the comparison report and confidence scoring even on the
                # LLM path.
                transformations: dict[str, str] = {}
                if not notebook_code and agentic_pipeline:
                    path_used = "agentic"
                    for record in agentic_pipeline.convert_mapping(mapping):
                        transformations[record.transformation_name] = record.final_code
                else:
                    transformations = _rule_based_transformations(mapping, converter)

                if not notebook_code:
                    conversion_result = {
                        "transformations": transformations,
                        "source_reads": [],
                        "target_write": "",
                    }
                    notebook_code = gen.generate(mapping, session, conversion_result, output_format="ipynb")

                # source_fidelity: LLMNotebookGenerator.generate
                # already runs this internally when the LLM path is taken (see
                # ``fidelity_checked`` above, set from ``llm_gen._last_fidelity``).
                # But every one of this project's 13 known defects was found on
                # the RULE-BASED path -- the one with no API key, the one
                # ``demo.sh`` exercises, and the one recommended as the
                # default -- and that path never called source_fidelity() at
                # all, so the one check independent of the parser's own
                # understanding was silently absent from it. Run it here too,
                # for whichever path actually produced this notebook (rule-based
                # or agentic), so ``fidelity_summary`` is never empty just
                # because ``use_llm=False``.
                if notebook_code and not fidelity_checked:
                    from .generators.source_fidelity import source_fidelity
                    try:
                        fidelity = source_fidelity(xml_file, notebook_code)
                        fidelity_checked = True
                        fidelity_has_gaps = fidelity.has_gaps
                        fidelity_summary_text = fidelity.summary()
                        if fidelity.has_gaps:
                            logger.warning("source_fidelity gaps for '%s': %s",
                                           mapping.name, fidelity_summary_text)
                    except Exception as exc:
                        logger.warning("source_fidelity check failed for '%s': %s",
                                       mapping.name, exc)

                folder = mapping.folder or "Migrated"
                nb_dir = os.path.join(output_dir, folder)
                os.makedirs(nb_dir, exist_ok=True)
                nb_name = f"nb_{mapping.name}.ipynb"
                nb_path = os.path.join(nb_dir, nb_name)
                # Two mappings with the same name in the same folder used to
                # overwrite each other in silence: the run reported one
                # notebook per mapping and delivered fewer, with no mention
                # of which were lost: the run reported one notebook per
                # mapping and delivered fewer.
                #
                # Informatica itself allows the same mapping name in
                # different folders, which the folder directory already
                # handles; this is the same folder AND the same name, which
                # means two exports disagree. Both are kept, the later one
                # suffixed, and the clash is reported -- losing one silently
                # is the only unacceptable outcome.
                if nb_path in _written_notebooks:
                    _n = 2
                    while True:
                        alt = os.path.join(nb_dir, f"nb_{mapping.name}__{_n}.ipynb")
                        if alt not in _written_notebooks:
                            break
                        _n += 1
                    _nb_collisions.append((mapping.name, folder, os.path.basename(alt)))
                    nb_path = alt
                _written_notebooks.add(nb_path)
                with open(nb_path, "w", encoding="utf-8") as f:
                    f.write(notebook_code)
                result.notebooks += 1
                # The path the deployer will upload this notebook to under
                # the AIDP workspace (see deployer.DeployConfig.workspace_path,
                # default /Workspace/Migrated). The deployer re-points every
                # task at the path it actually uploaded to, so this is the
                # default, not a contract. Built from the name actually
                # written: after a clash that is nb_X__2, and using the
                # unsuffixed nb_name pointed the second export's job at the
                # FIRST export's notebook -- the deployer re-points by that
                # name, so it ran the other mapping without a word.
                for s in sessions_of.get(mapping.name) or [session]:
                    notebook_paths[s.name] = (
                        f"/Workspace/Migrated/{folder}/{os.path.basename(nb_path)}"
                    )
                _warn_divergent_sessions(mapping.name, sessions_of.get(mapping.name) or [])

                comparison_path = _compare_and_score(
                    mapping, transformations, comparison_gen, scorer, all_scores, output_dir
                )

                result.outcomes.append(MappingOutcome(
                    xml_path=xml_file, mapping_name=mapping.name, path_used=path_used,
                    notebook_path=nb_path, score=score, comparison_path=comparison_path,
                    fidelity_checked=fidelity_checked,
                    fidelity_has_gaps=fidelity_has_gaps,
                    fidelity_summary=fidelity_summary_text,
                ))

            if not skip_lineage:
                for mapping in parsed.mappings:
                    _emit_lineage(mapping, output_dir)
            for mapping in parsed.mappings:
                _ddl_warnings.extend(
                    (mapping.name, dd) for dd in _emit_ddl(mapping, output_dir)
                    if dd.has_warnings
                )

            _emit_workflows(parsed, notebook_paths, output_dir, result, parameter_file,
                            schedule_timezone=schedule_timezone)

    # Spec section 11: a run that produced zero notebooks from a non-empty
    # input list must raise rather than report success -- but only when
    # EVERY input failed. A partial failure (>=1 notebook produced) stays a
    # warning plus an accurate (lower) count in the result, never an
    # exception.
    if inputs and result.notebooks == 0:
        if parse_failures:
            detail = "; ".join(f"{p}: {err}" for p, err in parse_failures)
        else:
            detail = "no mappings were found in any input"
        raise MigrationFailedError(
            f"run_migration produced 0 notebooks from {len(inputs)} input(s) "
            f"-- every input failed. {detail}"
        )

    # Shared tail: confidence aggregation and the optimize scan are both
    # path-agnostic (the latter just globs output_dir for *.ipynb), so they
    # run once here regardless of which branch produced the notebooks.
    result.confidence_summary = _finalize_confidence(scorer, all_scores, output_dir)
    result.fidelity_summary = _finalize_fidelity(result.outcomes, output_dir)
    result.ddl_warnings = _ddl_warnings
    result.notebook_collisions = _nb_collisions
    result.broken_notebooks = _validate_generated(output_dir)
    if result.broken_notebooks:
        _report_dir = os.path.join(output_dir, "reports")
        os.makedirs(_report_dir, exist_ok=True)
        with open(os.path.join(_report_dir, "broken_notebooks.md"), "w") as _fh:
            _fh.write("# Notebooks that cannot run as written\n\n")
            _fh.write("Found by reading back what this run generated. Each of "
                      "these is a defect in the migrator, not in the export.\n\n")
            for _p, _probs in result.broken_notebooks:
                _fh.write(f"## {os.path.basename(_p)}\n\n")
                for _pr in _probs:
                    _fh.write(f"- {_pr}\n")
                _fh.write("\n")

    if not skip_optimize:
        from .optimizer.optimizer import SparkOptimizer
        opt = SparkOptimizer()
        nb_files = glob.glob(os.path.join(output_dir, "**", "*.ipynb"), recursive=True)
        result.optimize_suggestions = sum(
            len(opt.optimize(open(f, encoding="utf-8").read()).suggestions) for f in nb_files
        )

    return result
