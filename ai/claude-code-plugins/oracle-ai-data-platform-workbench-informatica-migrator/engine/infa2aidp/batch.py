"""Parallel batch migration for high-volume Informatica → AIDP conversion.

Runs multiple LLM migrations concurrently to reduce wall-clock time.
For 770 pipelines: sequential = ~60 hours, parallel (10x) = ~6 hours.

Usage:
    infa2aidp migrate -i /path/to/xml_folder/ -o ./output --use-llm -v

    Or programmatically:
    from infa2aidp.batch import BatchMigrator
    migrator = BatchMigrator(llm_handler, max_workers=10)
    results = migrator.migrate_folder("/path/to/xmls", "./output")
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .models import Session
from .parsers.format_detector import detect_and_parse_file

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """Result of a single migration in a batch."""
    xml_path: str
    mapping_name: str = ""
    notebook_path: str = ""
    score: int = 0
    attempts: int = 0
    status: str = ""  # success, review, fallback, failed
    # True when the generated notebook needs a human before it can be
    # trusted -- either the validator declined its own output, or the
    # deterministic path emitted a REVIEW REQUIRED marker.
    review_required: bool = False
    error: str = ""
    duration_seconds: float = 0.0
    # Validation details
    critical_issues: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    info: list = field(default_factory=list)
    correct_steps: list = field(default_factory=list)
    # source_fidelity: the independent,
    # non-circular check against the RAW export, as opposed to the
    # critical_issues/warnings/info above which all come from the
    # validator's circular spec-vs-notebook comparison.
    fidelity_checked: bool = False
    fidelity_has_gaps: bool = False
    fidelity_summary: str = ""


@dataclass
class BatchSummary:
    """Summary of a batch migration run."""
    total: int = 0
    success: int = 0
    # Generated, but the generator declined to accept its own output or the
    # deterministic path emitted a REVIEW REQUIRED marker. Counted apart
    # from success: a run that needs a human before anything can be trusted
    # is not the same outcome as one that does not, and reporting both as
    # "success" is what made the distinction invisible.
    review: int = 0
    fallback: int = 0
    failed: int = 0
    avg_score: float = 0.0
    total_duration: float = 0.0
    results: list = field(default_factory=list)


class BatchMigrator:
    """Run migrations in parallel for high-volume batch processing.

    Uses ThreadPoolExecutor for concurrent LLM API calls.
    Each mapping runs independently — failures don't block others.
    """

    def __init__(
        self,
        llm_handler=None,
        max_workers: int = None,
        target_score: int = None,
        max_attempts: int = None,
    ):
        from . import config
        self.llm_handler = llm_handler
        self.max_workers = max_workers or config.BATCH_WORKERS
        self.target_score = target_score or config.TARGET_SCORE
        self.max_attempts = max_attempts or config.MAX_ATTEMPTS

    def migrate_folder(
        self,
        input_dir: str,
        output_dir: str,
    ) -> BatchSummary:
        """Migrate all XML/JSON files in a folder in parallel.

        Args:
            input_dir: Directory containing Informatica XML/JSON files
            output_dir: Directory for generated notebooks

        Returns:
            BatchSummary with results for all mappings
        """
        # Collect input files (recursive, case-insensitive — handles *.XML / *.Xml too)
        input_path = Path(input_dir)
        output_path = Path(output_dir).resolve()
        all_files = sorted(
            p for p in input_path.rglob("*")
            if p.is_file() and p.suffix.lower() in (".xml", ".json")
        )
        # Filter out files inside the output directory
        files = [f for f in all_files if not str(f.resolve()).startswith(str(output_path))]

        if not files:
            logger.warning("No XML/JSON files found in %s", input_dir)
            return BatchSummary()

        logger.info("Batch migration: %d files, %d parallel workers",
                     len(files), self.max_workers)

        os.makedirs(output_dir, exist_ok=True)
        summary = BatchSummary(total=len(files))
        start_time = time.time()

        # Run migrations in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            for xml_path in files:
                future = executor.submit(
                    self._migrate_single, str(xml_path), output_dir
                )
                futures[future] = str(xml_path)

            for future in as_completed(futures):
                xml_path = futures[future]
                try:
                    # One result per MAPPING, so a file carrying several
                    # contributes several. The counts below are therefore
                    # over mappings, which is what a reader wants to know.
                    for result in future.result():
                        summary.results.append(result)
                        n = len(summary.results)

                        if result.status == "success":
                            summary.success += 1
                            logger.info("[%d] %s: score %d/100 (attempt %d) — %.1fs",
                                        n, result.mapping_name, result.score,
                                        result.attempts, result.duration_seconds)
                        elif result.status == "review":
                            summary.review += 1
                            logger.warning(
                                "[%d] %s: generated but NEEDS REVIEW — score %d/100",
                                n, result.mapping_name, result.score)
                        elif result.status == "fallback":
                            summary.fallback += 1
                            logger.warning("[%d] %s: LLM failed, used rule-based fallback%s",
                                           n, result.mapping_name,
                                           " (needs review)" if result.review_required else "")
                        else:
                            summary.failed += 1
                            logger.error("[%d] %s: FAILED — %s",
                                         n, result.mapping_name, result.error)

                except Exception as exc:
                    summary.failed += 1
                    summary.results.append(BatchResult(
                        xml_path=xml_path, status="failed", error=str(exc)
                    ))
                    logger.error("Migration failed for %s: %s", xml_path, exc)

        # total counts MAPPINGS, not files: a folder export with three
        # mappings is three units of work, and reporting it as one hid the
        # two that used to be dropped.
        summary.total = len(summary.results)
        summary.total_duration = time.time() - start_time
        scores = [r.score for r in summary.results if r.score > 0]
        summary.avg_score = sum(scores) / len(scores) if scores else 0

        # Auto-rerun failed/low-score mappings
        from . import config as _cfg
        if _cfg.AUTO_RERUN:
            for rerun_cycle in range(1, _cfg.MAX_RERUNS + 1):
                failed_results = [
                    r for r in summary.results
                    if r.status == "failed" or r.score < self.target_score
                    or len(r.critical_issues) > _cfg.MAX_CRITICAL
                ]
                if not failed_results:
                    break
                logger.info("Auto-rerun cycle %d/%d: %d mappings to retry",
                            rerun_cycle, _cfg.MAX_RERUNS, len(failed_results))
                for old_result in failed_results:
                    # _migrate_single now returns one result per mapping in
                    # the file, so pick the one this retry is about rather
                    # than whichever mapping happens to come first.
                    retried = self._migrate_single(old_result.xml_path, output_dir)
                    new_result = next(
                        (r for r in retried if r.mapping_name == old_result.mapping_name),
                        None,
                    )
                    if new_result is None:
                        continue
                    if new_result.score > old_result.score or (
                        new_result.score == old_result.score
                        and len(new_result.critical_issues) < len(old_result.critical_issues)
                    ):
                        # Replace with better result
                        idx = summary.results.index(old_result)
                        summary.results[idx] = new_result
                        if old_result.status != new_result.status:
                            _bucket = {
                                "success": "success", "review": "review",
                                "fallback": "fallback", "failed": "failed",
                            }
                            old_b = _bucket.get(old_result.status)
                            new_b = _bucket.get(new_result.status)
                            if old_b:
                                setattr(summary, old_b, getattr(summary, old_b) - 1)
                            if new_b:
                                setattr(summary, new_b, getattr(summary, new_b) + 1)
                        logger.info("  %s improved: %d → %d",
                                    new_result.mapping_name, old_result.score, new_result.score)

            # Recalculate stats
            scores = [r.score for r in summary.results if r.score > 0]
            summary.avg_score = sum(scores) / len(scores) if scores else 0

        # Save batch report + per-mapping validation reports
        report_path = os.path.join(output_dir, "batch_report.json")
        self._save_report(summary, report_path)
        self._save_per_mapping_reports(summary, output_dir)
        logger.info(
            "Batch complete over %d mapping(s): %d success, %d need review, "
            "%d fallback, %d failed, avg score %.0f/100, total %.0fs",
            summary.total, summary.success, summary.review, summary.fallback,
            summary.failed, summary.avg_score, summary.total_duration,
        )

        return summary

    def _migrate_single(self, xml_path: str, output_dir: str) -> list[BatchResult]:
        """Migrate every mapping in one XML/JSON file.

        Returns one :class:`BatchResult` per mapping, not per file. A
        PowerCenter folder export routinely carries many mappings; this
        used to read ``parse_result.mappings[0]`` with no loop, so every
        mapping after the first was dropped and the run still reported
        success. Nothing detected it because every fixture in the suite
        carried exactly one mapping.
        """
        parse_start = time.time()
        try:
            parse_result = detect_and_parse_file(xml_path)
        except Exception as exc:
            r = BatchResult(xml_path=xml_path, status="failed", error=str(exc))
            r.duration_seconds = time.time() - parse_start
            return [r]

        if not parse_result.mappings:
            r = BatchResult(xml_path=xml_path, status="failed",
                            error="No mappings found")
            r.duration_seconds = time.time() - parse_start
            return [r]

        # Resolve each mapping's real session, exactly as the non-batch
        # path does. A synthetic Session carries no pre/post SQL, no
        # connections, no commit interval and no session parameters, so
        # every one of those silently vanished on this path -- and any
        # future fix that routes more session metadata to the generator
        # would have been inert here while looking correct everywhere else.
        session_map = {s.mapping_name: s for s in parse_result.sessions}

        return [
            self._migrate_one_mapping(
                xml_path, output_dir, mapping,
                session_map.get(mapping.name, Session(name=f"s_{mapping.name}")),
            )
            for mapping in parse_result.mappings
        ]

    def _migrate_one_mapping(
        self, xml_path: str, output_dir: str, mapping, session: Session
    ) -> BatchResult:
        """Generate and write the notebook for one mapping."""
        result = BatchResult(xml_path=xml_path)
        start = time.time()

        try:
            result.mapping_name = mapping.name

            notebook_code = None

            # Try LLM-first
            if self.llm_handler:
                try:
                    from .generators.llm_notebook_generator import LLMNotebookGenerator
                    llm_gen = LLMNotebookGenerator(self.llm_handler)
                    notebook_code = llm_gen.generate(
                        mapping, session,
                        output_format="ipynb",
                        target_score=self.target_score,
                        max_attempts=self.max_attempts,
                        source_path=xml_path,
                    )
                    if notebook_code:
                        result.score = getattr(llm_gen, '_last_score', 0)
                        result.attempts = getattr(llm_gen, '_last_attempts', 1)
                        # The generator sets this when it could not accept
                        # its own output. Nothing here read it, so a run
                        # that produced notebooks needing human review
                        # reported exactly the same as one that did not.
                        result.review_required = bool(
                            getattr(llm_gen, '_last_review_required', False)
                        )
                        result.status = (
                            "review" if result.review_required else "success"
                        )
                        # Capture validation details (same as individual execution)
                        validation = getattr(llm_gen, '_last_validation', None)
                        if validation:
                            result.critical_issues = validation.critical_issues
                            result.warnings = validation.warnings
                            result.info = validation.info
                            result.correct_steps = validation.correct_steps
                        # Capture the independent source_fidelity result
                        # -- computed whenever a
                        # source_path was available, regardless of the
                        # validator's (circular) score.
                        fidelity = getattr(llm_gen, '_last_fidelity', None)
                        if fidelity is not None:
                            result.fidelity_checked = True
                            result.fidelity_has_gaps = fidelity.has_gaps
                            result.fidelity_summary = fidelity.summary()
                except Exception as exc:
                    logger.warning("LLM failed for %s: %s — falling back",
                                   mapping.name, exc)

            # Fallback to rule-based — only if explicitly allowed
            if not notebook_code:
                from . import config as _cfg
                if _cfg.RULE_BASED_FALLBACK:
                    from .converters.transformation_converter import TransformationConverter
                    from .generators.notebook_generator import NotebookGenerator

                    converter = TransformationConverter()
                    gen = NotebookGenerator()
                    conversion_result = {"transformations": {}, "source_reads": [], "target_write": ""}
                    for tx in mapping.transformations:
                        code_lines = converter.convert(tx)
                        conversion_result["transformations"][tx.name] = "\n".join(code_lines)
                    notebook_code = gen.generate(mapping, session, conversion_result, output_format="ipynb")
                    # The deterministic path has no validator to consult, so
                    # review state comes from the markers it emitted.
                    result.review_required = "REVIEW REQUIRED" in (notebook_code or "")
                    result.status = "fallback"

                    # source_fidelity: the LLM branch above
                    # already runs this on success. The rule-based fallback
                    # used to skip it entirely, leaving the deterministic
                    # path -- the one every historical defect in this
                    # project was found on -- with no independent check.
                    if notebook_code:
                        from .generators.source_fidelity import source_fidelity
                        try:
                            fidelity = source_fidelity(xml_path, notebook_code)
                            result.fidelity_checked = True
                            result.fidelity_has_gaps = fidelity.has_gaps
                            result.fidelity_summary = fidelity.summary()
                        except Exception as exc:
                            logger.warning("source_fidelity check failed for %s: %s", xml_path, exc)
                else:
                    result.status = "failed"
                    result.error = (
                        f"LLM generation failed after {self.max_attempts} attempts. "
                        f"Set RULE_BASED_FALLBACK=true in .env to fall back to rule-based "
                        f"converters (legacy behavior), or check the debug log to diagnose."
                    )

            # Write notebook
            if notebook_code:
                folder = mapping.folder or "Migrated"
                nb_dir = os.path.join(output_dir, folder)
                os.makedirs(nb_dir, exist_ok=True)
                nb_path = os.path.join(nb_dir, f"nb_{mapping.name}.ipynb")
                with open(nb_path, "w", encoding="utf-8") as f:
                    f.write(notebook_code)
                result.notebook_path = nb_path
            else:
                result.status = "failed"
                result.error = "No notebook generated"

        except Exception as exc:
            result.status = "failed"
            result.error = str(exc)

        result.duration_seconds = time.time() - start
        return result

    @staticmethod
    def _save_report(summary: BatchSummary, path: str):
        """Save batch report as JSON with full validation details."""
        data = {
            "total": summary.total,
            "success": summary.success,
            "review": summary.review,
            "fallback": summary.fallback,
            "failed": summary.failed,
            "avg_score": round(summary.avg_score, 1),
            "total_duration_seconds": round(summary.total_duration, 1),
            "results": [
                {
                    "xml_path": r.xml_path,
                    "mapping_name": r.mapping_name,
                    "notebook_path": r.notebook_path,
                    "score": r.score,
                    "attempts": r.attempts,
                    "status": r.status,
                    "error": r.error,
                    "duration_seconds": round(r.duration_seconds, 1),
                    "critical_issues": r.critical_issues,
                    "warnings": r.warnings,
                    "info": r.info,
                    "correct_steps": r.correct_steps,
                    "fidelity_checked": r.fidelity_checked,
                    "fidelity_has_gaps": r.fidelity_has_gaps,
                    "fidelity_summary": r.fidelity_summary,
                }
                for r in summary.results
            ],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    @staticmethod
    def _save_per_mapping_reports(summary: BatchSummary, output_dir: str):
        """Save individual validation reports per mapping."""
        reports_dir = os.path.join(output_dir, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        for r in summary.results:
            if not r.mapping_name:
                continue
            report = {
                "mapping": r.mapping_name,
                "score": r.score,
                "attempts": r.attempts,
                "status": r.status,
                "critical_issues": r.critical_issues,
                "warnings": r.warnings,
                "info": r.info,
                "correct_steps": r.correct_steps,
                "fidelity_checked": r.fidelity_checked,
                "fidelity_has_gaps": r.fidelity_has_gaps,
                "fidelity_summary": r.fidelity_summary,
            }
            report_path = os.path.join(reports_dir, f"validation_{r.mapping_name}.json")
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)

        # Save input→output filename mapping (CSV)
        mapping_path = os.path.join(reports_dir, "migration_mapping.csv")
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write("input_xml,mapping_name,output_notebook,score,status,critical,warnings\n")
            for r in summary.results:
                critical = len(r.critical_issues)
                warnings = len(r.warnings)
                f.write(f"{r.xml_path},{r.mapping_name},{r.notebook_path},"
                        f"{r.score},{r.status},{critical},{warnings}\n")
        logger.info("Migration mapping saved: %s", mapping_path)
