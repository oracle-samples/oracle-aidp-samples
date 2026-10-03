"""infa2aidp -- thin CLI over the migration engine.

Claude Code skills are the primary interface; this CLI exists for headless
and CI use. Each handler is a thin adapter onto a library entrypoint -- keep
logic in the library, not here.
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
from pathlib import Path
from typing import Callable, NamedTuple

# Load .env before any handler needs an env-derived setting (AIDP_REGION,
# ANTHROPIC_API_KEY, etc).
from . import config as _config  # noqa: F401
from . import __version__

logger = logging.getLogger(__name__)


class Command(NamedTuple):
    name: str
    help: str
    handler: Callable[[argparse.Namespace], int]


def _collect_input_files(path: str) -> list[str]:
    """Return Informatica export files under *path* (a single file or a
    directory tree).

    Collects BOTH PowerCenter XML and IICS/IDMC JSON exports -- see spec
    . Before this, every CLI entry point globbed ``*.xml`` only, so a
    ``.json`` IICS export was invisible to every command.
    """
    if os.path.isfile(path):
        return [path]
    if os.path.isdir(path):
        files = sorted(
            str(p) for p in Path(path).rglob("*")
            if p.is_file() and p.suffix.lower() in (".xml", ".json")
        )
        if not files:
            raise FileNotFoundError(f"No Informatica exports (.xml / .json) found in {path}")
        return files
    raise FileNotFoundError(f"Path does not exist: {path}")


# ── Commands ─────────────────────────────────────────────────────────

def _cmd_version(args: argparse.Namespace) -> int:
    import platform
    print(f"infa2aidp {__version__} (python {platform.python_version()})")
    if _config.ANTHROPIC_API_KEY:
        print(f"Claude API: configured (model: {_config.CLAUDE_MODEL})")
    else:
        print("Claude API: ANTHROPIC_API_KEY not set -- rule-based only")
    return 0


def _cmd_discover(args: argparse.Namespace) -> int:
    """Extract mappings/workflows from a live PowerCenter repository."""
    from .crawlers.informatica_crawler import InformaticaCrawler, InfaConnectionConfig

    cfg = InfaConnectionConfig(
        host=args.host,
        port=args.port,
        username=args.user or os.environ.get("INFA_USER", ""),
        password=args.password or os.environ.get("INFA_PASSWORD", ""),
        repository=args.repo or os.environ.get("INFA_REPO", ""),
        domain=args.domain or os.environ.get("INFA_DOMAIN", ""),
    )
    crawler = InformaticaCrawler(cfg)
    method = crawler.connect(method=args.method)
    logger.info("Connected via %s", method)

    os.makedirs(args.output, exist_ok=True)
    folders = args.folders.split(",") if args.folders else None
    result = crawler.crawl_repository(
        os.path.join(args.output, "exported_xml"), folders=folders, export_xml=True
    )
    report_path = os.path.join(args.output, "infa_inventory_report.md")
    crawler.generate_inventory_report(result, report_path)
    crawler.disconnect()

    for err in result.errors[:5]:
        logger.warning(err)
    print(f"{len(result.mappings)} mappings, {len(result.workflows)} workflows, "
          f"{len(result.exported_xml_files)} XMLs exported -> {report_path}")
    return 1 if result.errors and not result.exported_xml_files else 0


def _merge_migration_results(parsed_list):
    """Merge several per-file ``MigrationResult``s into one, the same way
    ``InformaticaXMLParser._parse_folder`` merges a directory of XML files
    -- generalized here to also cover IICS/IDMC JSON inputs (the design notes),
    since ``detect_and_parse_file`` runs per-file rather than per-folder.
    """
    from .models import MigrationResult

    merged = MigrationResult()
    for parsed in parsed_list:
        merged.mappings.extend(parsed.mappings)
        merged.sessions.extend(parsed.sessions)
        merged.workflows.extend(parsed.workflows)
        merged.compatibility_issues.extend(parsed.compatibility_issues)
        if not merged.version_detail and parsed.version_detail:
            merged.version = parsed.version
            merged.version_detail = parsed.version_detail
        if not merged.repository_name and parsed.repository_name:
            merged.repository_name = parsed.repository_name
        if not merged.folder_name and parsed.folder_name:
            merged.folder_name = parsed.folder_name
    return merged


def _cmd_analyze(args: argparse.Namespace) -> int:
    """Inventory, complexity and compatibility report (no cluster needed)."""
    from .analyzer.analyzer import InformaticaAnalyzer
    from .parsers.format_detector import detect_and_parse_file

    formats = [args.format] if args.format != "all" else ["markdown", "json", "csv"]

    # _collect_input_files + detect_and_parse_file (rather than handing
    # args.input straight to InformaticaAnalyzer.analyze(), which only ever
    # globs *.xml internally) so a directory of IICS/IDMC JSON exports is
    # actually analyzed instead of silently reporting "No XML files found"
    # and exiting 0. See the design notes.
    input_files = _collect_input_files(args.input)

    # Per-file, not per-run: one input rejected by the version gate (an
    # unsupported PowerCenter release -- see
    # parsers.version_detector.require_supported) must not abort analysis
    # of everything else in a directory. This mirrors run_migration's
    # existing policy (migrator.py) -- skip the offending file, report it
    # as an explicit finding, and only fail the whole command when EVERY
    # input failed to parse, exactly the "unsupported input becomes a
    # finding, not a silent pass, but a batch survives it" rule already
    # applied there.
    parsed_results = []
    parse_failures: list[tuple[str, str]] = []
    for f in input_files:
        try:
            parsed_results.append(detect_and_parse_file(f))
        except Exception as exc:
            logger.warning("Skipping %s: %s", f, exc)
            parse_failures.append((f, str(exc)))

    if not parsed_results:
        detail = "; ".join(f"{p}: {err}" for p, err in parse_failures) or "no inputs"
        logger.error("analyze produced no usable input -- every input failed: %s", detail)
        return 1

    merged = _merge_migration_results(parsed_results)
    report = InformaticaAnalyzer().analyze_result(
        merged, output_dir=args.output, formats=formats
    )
    if parse_failures:
        print(f"Skipped {len(parse_failures)} file(s) that failed to parse:")
        for path, err in parse_failures:
            print(f"  - {path}: {err}")
    inv = report.inventory
    issues = report.compatibility_issues or []
    errors = sum(1 for i in issues if i.severity == "ERROR")
    print(f"Mappings: {inv.total_mappings}  Workflows: {inv.total_workflows}  "
          f"Sessions: {inv.total_sessions}  Transformations: {inv.total_transformations}")
    print(f"Compatibility: {errors} error(s), {len(issues) - errors} other issue(s)")
    print(f"Reports written to {os.path.abspath(args.output)}")
    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    """Convert Informatica mappings into PySpark/AIDP notebooks."""
    from .migrator import format_run_summary, run_migration

    input_files = _collect_input_files(args.input)
    result = run_migration(
        input_files,
        args.output,
        use_llm=args.use_llm,
        agentic=args.agentic,
        custom_rules_path=args.custom_rules,
        params_path=args.params,
        max_workers=args.workers,
        emit_comparison=args.comparison,
        skip_lineage=args.skip_lineage,
        skip_optimize=args.skip_optimize,
        target_catalog_type=args.target_catalog_type,
        schedule_timezone=getattr(args, 'schedule_timezone', None),
    )

    print(format_run_summary(result))
    if result.optimize_suggestions:
        logger.info("%d optimization suggestion(s) -- run 'infa2aidp optimize' for details",
                    result.optimize_suggestions)
    return 0


def _cmd_deploy(args: argparse.Namespace) -> int:
    """Upload notebooks and create workflows/jobs on AIDP."""
    from .deployer.deployer import AIDPDeployer
    from .deployer.models import DeployConfig

    config = DeployConfig(
        region=args.region or _config.AIDP_REGION or os.environ.get("AIDP_REGION", ""),
        instance_id=args.instance_id or _config.AIDP_INSTANCE_ID or os.environ.get("AIDP_INSTANCE_ID", ""),
        workspace_key=args.workspace_key or _config.AIDP_WORKSPACE_KEY or os.environ.get("AIDP_WORKSPACE_KEY", ""),
        oci_profile=args.profile or _config.OCI_PROFILE or os.environ.get("OCI_PROFILE", "DEFAULT"),
        cluster_key=args.cluster_key or os.environ.get("AIDP_CLUSTER_KEY", ""),
        workspace_path=args.workspace_path or os.environ.get("AIDP_WORKSPACE_PATH", "/Workspace/Migrated"),
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    required = {"--region / AIDP_REGION": config.region, "--instance-id / AIDP_INSTANCE_ID": config.instance_id,
                "--workspace-key / AIDP_WORKSPACE_KEY": config.workspace_key}
    missing = [flag for flag, value in required.items() if not value]
    if missing and not config.dry_run:
        logger.error("AIDP deploy needs %s (or use --dry-run)", ", ".join(missing))
        return 1

    deployer = AIDPDeployer(config)
    wf_dir = os.path.join(args.input, "workflows")
    result = deployer.deploy(args.input, wf_dir if os.path.isdir(wf_dir) else None)

    out_dir = args.output or os.path.join(args.input, "reports")  # beside migrate's own reports, not the CWD
    os.makedirs(out_dir, exist_ok=True)
    deployer.generate_deploy_report(result, os.path.join(out_dir, "deploy_report.md"))
    print(f"Uploaded: {result.total_uploaded}  Updated: {result.total_updated}  Failed: {result.total_failed}  "
          f"Skipped: {result.total_skipped}" + (f"  Would deploy: {result.total_dry_run}" if result.dry_run else ""))
    if result.dry_run:
        print("DRY RUN -- nothing was deployed")
    return 1 if result.total_failed else 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    """Compare source-DB rows against migrated AIDP targets."""
    from .reconciler.reconciler import DataReconciler
    from .reconciler.report import ReconcileReport

    reconciler = DataReconciler()
    configs = reconciler.load_config(args.config)
    results = reconciler.reconcile_batch(configs)

    os.makedirs(args.output, exist_ok=True)
    report = ReconcileReport(results)
    formats = [args.format] if args.format != "all" else ["markdown", "json", "csv"]
    if "markdown" in formats:
        report.save_markdown(os.path.join(args.output, "reconcile_report.md"))
    if "json" in formats:
        report.save_json(os.path.join(args.output, "reconcile_report.json"))
    if "csv" in formats:
        report.save_csv(os.path.join(args.output, "reconcile_report.csv"))

    passed = sum(1 for r in results if r.status == "PASSED")  # the reconciler's own words
    failed = sum(1 for r in results if r.status == "FAILED")
    print(f"{len(configs)} config(s): {passed} passed, {failed} failed, "
          f"{len(results) - passed - failed} error(s) -> {os.path.abspath(args.output)}")
    return 1 if failed else 0


def _cmd_optimize(args: argparse.Namespace) -> int:
    """Spark performance suggestions for generated notebooks."""
    from .optimizer.optimizer import SparkOptimizer

    if not os.path.isdir(args.input):
        logger.error("Directory not found: %s", args.input)
        return 1
    os.makedirs(args.output, exist_ok=True)

    optimizer = SparkOptimizer()
    nb_files = sorted(
        glob.glob(os.path.join(args.input, "**", "*.ipynb"), recursive=True)
        + glob.glob(os.path.join(args.input, "**", "*.py"), recursive=True)
    )
    reports = []
    for nb_file in nb_files:
        raw = open(nb_file, encoding="utf-8").read()
        code = raw
        if nb_file.endswith(".ipynb"):
            try:
                nb = json.loads(raw)
                code = "\n".join(
                    "".join(c["source"]) for c in nb.get("cells", [])
                    if c.get("cell_type") == "code"
                )
            except (json.JSONDecodeError, KeyError):
                pass
        report = optimizer.optimize(code, context={"notebook_name": os.path.basename(nb_file)})
        report.notebook_name = os.path.basename(nb_file)
        reports.append(report)
        if report.suggestions and args.auto_apply:
            optimizer.apply_to_file(nb_file, raw)

    optimizer.generate_report(reports, os.path.join(args.output, "optimization_report.md"))
    total = sum(len(r.suggestions) for r in reports)
    auto = sum(r.auto_applicable for r in reports)
    print(f"{len(nb_files)} notebook(s): {total} suggestion(s), {auto} auto-applicable "
          f"-> {os.path.abspath(args.output)}")
    return 0


def _cmd_review(args: argparse.Namespace) -> int:
    """Human approval gate for LOW/MANUAL-confidence conversions."""
    from .agents.reviewer import HumanReviewer

    reviewer = HumanReviewer()
    if args.action == "generate":
        from .parsers.format_detector import detect_and_parse_file
        from .agents.pipeline import ConversionPipeline

        pipeline = ConversionPipeline()
        records = []
        for input_file in _collect_input_files(args.input):
            for mapping in detect_and_parse_file(input_file).mappings:
                records.extend(pipeline.convert_mapping(mapping))

        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        count = reviewer.generate_review_file(
            records, args.output, include_confidence=["LOW", "MANUAL", "MEDIUM"]
        )
        print(f"Review file: {args.output} ({count} item(s) to review)")
    else:
        reviewed = reviewer.import_review_file(args.input)
        os.makedirs(args.output, exist_ok=True)
        reviewer.generate_review_report(reviewed, os.path.join(args.output, "review_report.md"))
        print(f"Approved: {sum(1 for r in reviewed if r.decision == 'approved')}  "
              f"Edited: {sum(1 for r in reviewed if r.decision == 'edited')}  "
              f"Rejected: {sum(1 for r in reviewed if r.decision == 'rejected')}")
    return 0


def _cmd_rag(args: argparse.Namespace) -> int:
    """Manage the learned conversion-pattern (RAG) store."""
    from .agents.rag_store import RAGStore

    store = RAGStore()
    if args.action == "stats":
        stats = store.stats()
        print(f"Entries: {stats['total_entries']}  Approved: {stats['approved_entries']}  "
              f"Retrievals: {stats['total_retrievals']}")
    elif args.action == "list":
        for e in store.list_entries(approved_only=args.approved_only)[:20]:
            status = "approved" if e.approved else "pending"
            print(f"[{e.id}] {e.transformation_type} -- {status} -- used {e.used_count}x")
    elif args.action == "export":
        store.export(args.output)
        print(f"Exported to {args.output}")
    elif args.action == "import":
        store.import_entries(args.input)
        print(f"Imported from {args.input}; total entries now {store.stats()['total_entries']}")
    elif args.action == "clear":
        count = len(store.entries)
        store.entries.clear()
        store._save()
        print(f"Cleared {count} entries")
    return 0


def _cmd_lineage(args: argparse.Namespace) -> int:
    """Field-level data lineage report for one or more mappings."""
    from .parsers.format_detector import detect_and_parse_file
    from .generators.lineage_generator import LineageGenerator

    input_files = _collect_input_files(args.input)
    os.makedirs(args.output, exist_ok=True)
    lin_gen = LineageGenerator()

    count = 0
    for input_file in input_files:
        for mapping in detect_and_parse_file(input_file).mappings:
            lineage = lin_gen.generate(mapping)
            lin_gen.export_lineage_report(lineage, os.path.join(args.output, mapping.name))
            count += 1
    print(f"Lineage generated for {count} mapping(s) -> {os.path.abspath(args.output)}")
    return 0


COMMANDS: dict[str, Command] = {
    "discover":  Command("discover",  "Extract assets from a live PowerCenter repository", _cmd_discover),
    "analyze":   Command("analyze",   "Inventory, complexity and compatibility report (no cluster)", _cmd_analyze),
    "migrate":   Command("migrate",   "Convert mappings to PySpark notebooks", _cmd_migrate),
    "deploy":    Command("deploy",    "Upload notebooks and create AIDP jobs", _cmd_deploy),
    "reconcile": Command("reconcile", "Compare source-DB rows against AIDP targets", _cmd_reconcile),
    "optimize":  Command("optimize",  "Spark performance suggestions for generated notebooks", _cmd_optimize),
    "review":    Command("review",    "Human approval gate for low-confidence items", _cmd_review),
    "rag":       Command("rag",       "Manage the learned-pattern store", _cmd_rag),
    "lineage":   Command("lineage",   "Field-level data lineage report", _cmd_lineage),
    "version":   Command("version",   "Print tool and Python version", _cmd_version),
}


# ── Argument wiring ──────────────────────────────────────────────────

def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")


# (command, flags, kwargs) triples -- data-driven, so a new flag is one line, not a new elif branch. "version" takes none.
_ARG_SPECS: list[tuple[str, tuple[str, ...], dict]] = [
    ("discover", ("--host",), dict(required=True, help="Informatica PowerCenter host")),
    ("discover", ("--port",), dict(type=int, default=6005)),
    ("discover", ("--user",), dict(default=None)),
    ("discover", ("--password",), dict(default=None)),
    ("discover", ("--repo",), dict(default=None, help="Repository name")),
    ("discover", ("--domain",), dict(default=None, help="Informatica domain")),
    ("discover", ("--method",), dict(choices=["auto", "soap", "pmrep"], default="auto")),
    ("discover", ("--folders",), dict(default=None, help="Comma-separated folder list")),
    ("discover", ("-o", "--output"), dict(default="./crawl_output")),
    ("analyze", ("-i", "--input"), dict(required=True)),
    ("analyze", ("-o", "--output"), dict(default="./analysis_report")),
    ("analyze", ("--format",), dict(choices=["markdown", "json", "csv", "all"], default="all")),
    ("migrate", ("-i", "--input"), dict(required=True)),
    ("migrate", ("-o", "--output"), dict(required=True)),
    ("migrate", ("--use-llm",), dict(action="store_true")),
    ("migrate", ("--agentic",), dict(action="store_true")),
    ("migrate", ("--custom-rules",), dict(default=None)),
    ("migrate", ("--params",), dict(default=None)),
    ("migrate", ("--workers",), dict(type=int, default=1, help="Parallel LLM workers (batch mode when >1)")),
    ("migrate", ("--comparison",), dict(action="store_true", help="Emit side-by-side Informatica-vs-PySpark fidelity reports")),
    ("migrate", ("--skip-lineage",), dict(action="store_true")),
    ("migrate", ("--skip-optimize",), dict(action="store_true")),
    ("migrate", ("--schedule-timezone",), dict(default=None, metavar="IANA_ZONE", help="IANA timezone the source Integration Service ran in, e.g. America/New_York. PowerCenter records a workflow's STARTTIME in that service's local time and stores NO zone, so the export cannot supply it and only you know it. Set it and a converted schedule fires at the right hour; omit it and the job is created as UTC with the assumption reported for review. Rejected if not a real IANA zone.")),
    ("migrate", ("--target-catalog-type",), dict(choices=["delta", "adw"], default=_config.TARGET_CATALOG_TYPE, help="Target catalog family generated write cells assume: 'delta' (managed Delta, default) or 'adw' (external ADW/ALH/ATP catalog -- JDBC overwrite, staged MERGE for upserts). Never inferred -- always explicit.")),
    ("deploy", ("-i", "--input"), dict(required=True)),
    ("deploy", ("-o", "--output"), dict(default=None, help="deploy_report.md folder, default <input>/reports")),
    ("deploy", ("--region",), dict(default=None, help="AIDP_REGION")),
    ("deploy", ("--instance-id",), dict(default=None, help="DataLake OCID (AIDP_INSTANCE_ID)")),
    ("deploy", ("--workspace-key",), dict(default=None, help="AIDP_WORKSPACE_KEY")),
    ("deploy", ("--profile",), dict(default=None, help="~/.oci/config profile (OCI_PROFILE)")),
    ("deploy", ("--cluster-key",), dict(default=None, help="cluster the jobs run on (AIDP_CLUSTER_KEY)")),
    ("deploy", ("--workspace-path",), dict(default=None, help="AIDP_WORKSPACE_PATH, default /Workspace/Migrated")),
    ("deploy", ("--overwrite",), dict(action="store_true")),
    ("deploy", ("--dry-run",), dict(action="store_true")),
    ("reconcile", ("-c", "--config"), dict(required=True)),
    ("reconcile", ("-o", "--output"), dict(default="./reconcile_report")),
    ("reconcile", ("--format",), dict(choices=["markdown", "json", "csv", "all"], default="all")),
    ("optimize", ("-i", "--input"), dict(required=True)),
    ("optimize", ("-o", "--output"), dict(default="./optimization_report")),
    ("optimize", ("--auto-apply",), dict(action="store_true")),
    ("review", ("action",), dict(choices=["generate", "import"])),
    ("review", ("-i", "--input"), dict(required=True)),
    ("review", ("-o", "--output"), dict(default="./review")),
    ("rag", ("action",), dict(choices=["stats", "list", "export", "import", "clear"])),
    ("rag", ("-i", "--input"), dict(default=None, help="Input file (for import)")),
    ("rag", ("-o", "--output"), dict(default="rag_export.json")),
    ("rag", ("--approved-only",), dict(action="store_true")),
    ("lineage", ("-i", "--input"), dict(required=True)),
    ("lineage", ("-o", "--output"), dict(default="./lineage")),
]


def _configure(name: str, p: argparse.ArgumentParser) -> None:
    for cmd, flags, kwargs in _ARG_SPECS:
        if cmd == name:
            p.add_argument(*flags, **kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="infa2aidp",
        description="Migrate Informatica PowerCenter ETL to Spark on Oracle AI Data Platform.",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    for cmd in COMMANDS.values():
        p = sub.add_parser(cmd.name, help=cmd.help)
        _add_common(p)
        _configure(cmd.name, p)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 2

    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "verbose", False) else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    try:
        return COMMANDS[args.command].handler(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        logger.error("%s failed: %s", args.command, exc)
        if getattr(args, "verbose", False):
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
