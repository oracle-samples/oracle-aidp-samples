"""CLI entry point for Qualifire."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from qualifire.core.config import load_config
from qualifire.core.exceptions import QualifireConfigError, QualifireValidationError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="qualifire",
        description="Qualifire — Data quality validation CLI",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run command
    run_parser = subparsers.add_parser("run", help="Run validations from a config file")
    run_parser.add_argument("--config", "-c", required=True, help="Path to YAML/JSON config")
    run_parser.add_argument(
        "--context", "-C", nargs="*", metavar="KEY=VALUE",
        help="Extra context variables (e.g. ds=2024-01-01)",
    )
    run_parser.add_argument(
        "--skip-recollection",
        action="store_true",
        help=(
            "Short-circuit cache-eligible collectors when a matching "
            "cached value already exists in the system table. The "
            "validators still re-evaluate against the cached values."
        ),
    )
    run_parser.add_argument(
        "--skip-renotification",
        action="store_true",
        help=(
            "Skip notification dispatch for any (dataset, validation, "
            "metric, dim) key whose prior run already carried the same "
            "severity. Use on retries / replays / CI rehearsals to "
            "avoid duplicate paging. Default is to always dispatch."
        ),
    )
    run_parser.add_argument(
        "--skip-revalidation",
        action="store_true",
        help=(
            "Skip the validator's compute when an active validation row "
            "already exists at the dataset's resolved partition_ts. "
            "Replays the persisted verdict (with details.from_cache=True) "
            "instead of re-running the validator. Use on retries / "
            "replays where verdicts are deterministic."
        ),
    )

    # validate-config command
    vc_parser = subparsers.add_parser("validate-config", help="Validate a config file")
    vc_parser.add_argument("--config", "-c", required=True, help="Path to YAML/JSON config")

    # report command
    report_parser = subparsers.add_parser("report", help="Generate a health report")
    report_parser.add_argument("--config", "-c", required=True, help="Path to YAML/JSON config")
    report_parser.add_argument("--output", "-o", default="qualifire_health.html", help="Output HTML path")
    report_parser.add_argument("--days", "-d", type=int, default=30, help="Days of history (default: 30)")

    # backfill command
    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Backfill metrics for past partition(s)",
    )
    backfill_parser.add_argument("--config", "-c", required=True, help="Path to YAML/JSON config")
    backfill_parser.add_argument(
        "--partition", required=True,
        help=(
            "Partition value(s): single ISO-8601 date/datetime, "
            "comma-separated list (e.g. '2026-04-01,2026-04-02'), or "
            "inclusive range with '..' separator (e.g. "
            "'2026-04-01..2026-04-07')"
        ),
    )
    backfill_parser.add_argument(
        "--selector",
        help=(
            "Optional selector restricting scope: "
            "'<dataset>:<validation>[:<metric>]' with '*' wildcards. "
            "Comma-separated for multiple selectors."
        ),
    )
    backfill_parser.add_argument(
        "--data", action="store_true",
        help=(
            "Run the full WAP cycle on the past partition (write → "
            "audit → publish). Default is metrics-only refresh "
            "(reads target_table for WAP datasets). Only valid for "
            "WAP datasets."
        ),
    )
    backfill_parser.add_argument(
        "--skip-recollection", action="store_true",
        help="Pass-through to the engine's cache pre-pass.",
    )
    backfill_parser.add_argument(
        "--skip-renotification", action="store_true",
        help=(
            "Skip notification dispatch for any (dataset, validation, "
            "metric, dim) key whose prior run already carried the same "
            "severity. Recommended for backfill replays so a 90-day "
            "replay doesn't fire 90 days of pages."
        ),
    )
    backfill_parser.add_argument(
        "--skip-revalidation", action="store_true",
        help=(
            "Skip the validator's compute when an active validation row "
            "already exists at the partition. Replays the persisted "
            "verdict instead of re-running the validator."
        ),
    )
    backfill_parser.add_argument(
        "--soft-delete-prior", action="store_true",
        help=(
            "Write a tombstone INSERT for every metric the backfill "
            "touches BEFORE the new collection INSERTs."
        ),
    )
    backfill_parser.add_argument(
        "--json", action="store_true",
        help="Emit the BackfillReport as JSON to stdout instead of a summary.",
    )
    backfill_parser.add_argument(
        "--parallelism", type=int, default=1,
        help=(
            "Number of concurrent worker threads processing distinct "
            "(scope, anchor) units. Default 1 (serial). Capped at 64. "
            "parallelism > 1 forces notifiers={} for the inner engine "
            "call to avoid suppression races; the report's "
            "notifications_suppressed field flags this."
        ),
    )
    backfill_parser.add_argument(
        "--max-partitions", type=int, default=10000,
        help=(
            "Upper bound on partitions produced by (start..end) range "
            "expansion. Default 10000."
        ),
    )

    # deactivate-metric command
    da_parser = subparsers.add_parser(
        "deactivate-metric",
        help="Tombstone a metric (mark inactive) so subsequent reads exclude it",
    )
    da_parser.add_argument("--config", "-c", required=True, help="Path to YAML/JSON config")
    da_parser.add_argument("--dataset", required=True, help="Dataset name")
    da_parser.add_argument("--metric", required=True, help="Metric name")
    da_parser.add_argument(
        "--dimension",
        help="Optional encoded dimension (e.g. '{\"region\":\"US\"}')",
    )
    da_parser.add_argument(
        "--partition",
        help="Optional partition_ts (ISO-8601). Default = all partitions.",
    )
    da_parser.add_argument(
        "--note",
        help="Free-form note captured in the tombstone's details_json.",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "validate-config":
        return _cmd_validate_config(args)
    elif args.command == "run":
        return _cmd_run(args)
    elif args.command == "report":
        return _cmd_report(args)
    elif args.command == "backfill":
        return _cmd_backfill(args)
    elif args.command == "deactivate-metric":
        return _cmd_deactivate_metric(args)

    return 0


def _cmd_validate_config(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
        print(f"Config is valid: {len(config.datasets)} dataset(s), owner='{config.owner}'")
        return 0
    except QualifireConfigError as e:
        # ``load_config`` raises ``QualifireConfigError`` with the
        # ``Config validation failed: ...`` prefix already applied.
        # Printing ``{e}`` verbatim avoids the doubled prefix seen
        # during earlier review rounds.
        print(str(e), file=sys.stderr)
        return 1


def _cmd_run(args: argparse.Namespace) -> int:
    # Parse context key=value pairs
    context = {}
    if args.context:
        for item in args.context:
            if "=" not in item:
                print(f"Invalid context format: '{item}'. Expected KEY=VALUE", file=sys.stderr)
                return 1
            key, value = item.split("=", 1)
            context[key] = value

    # Load config BEFORE any runtime wiring so operator-visible config
    # errors (missing fields, duplicate dataset names, legacy
    # custom_query.filter, unsupported backend values, ...) surface as
    # a clean exit-1 message instead of a raw Python traceback. Matches
    # the _cmd_validate_config handling.
    try:
        config = load_config(args.config)
    except QualifireConfigError as e:
        # Prefix lives in ``load_config``; print verbatim to avoid
        # ``Config validation failed: Config validation failed: ...``.
        print(str(e), file=sys.stderr)
        return 1

    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    except ImportError:
        print("PySpark is required for CLI execution", file=sys.stderr)
        return 1

    from qualifire.api import Qualifire
    from qualifire.backends.spark_backend import SparkBackend

    # Storage initialization can fail at construction time —
    # ``JDBCStorage.initialize()`` deliberately raises on bad URL,
    # missing driver, or CREATE-table failure so the engine can't
    # silently write into the void. Convert that RuntimeError into
    # a clean CLI exit-1 instead of letting a raw traceback escape.
    try:
        # ``_from_parsed_config`` accepts the already-parsed YAML so
        # the CLI never re-reads the file — preserves single-load
        # TOCTOU semantics. ``warn_on_reinit=False`` because the CLI
        # constructs and runs against the same YAML; the warning
        # would never fire usefully and the CLI's stderr is
        # reserved for operator-relevant signals only.
        qf = Qualifire._from_parsed_config(
            config,
            backend=SparkBackend(spark),
            warn_on_reinit=False,
        )
    except RuntimeError as e:
        print(f"System table initialization failed: {e}", file=sys.stderr)
        return 1

    try:
        # Hand the already-parsed config to the engine rather than
        # letting ``run_config(path)`` re-read the file. Two reads of
        # the same path are a TOCTOU window: the file could be
        # regenerated or swapped between the preflight load above and
        # execution, and the CLI would report "validation passed" for
        # snapshot A while running snapshot B. Using the parsed object
        # makes the CLI single-source-of-truth for this invocation.
        result = qf.run_config_parsed(
            config, context=context,
            skip_recollection=getattr(args, "skip_recollection", False),
            skip_renotification=getattr(args, "skip_renotification", False),
            skip_revalidation=getattr(args, "skip_revalidation", False),
        )
        print(f"Run complete: {result.overall_severity.value}")
        for ds in result.datasets:
            print(f"  {ds.dataset_name}: {ds.overall_severity.value}")
            for vr in ds.validation_results:
                print(f"    [{vr.severity.value}] {vr.message}")
        return 0
    except QualifireValidationError as e:
        print(f"Validation FAILED: {e}", file=sys.stderr)
        if e.result:
            for ds in e.result.datasets:
                for vr in ds.validation_results:
                    if vr.severity.value == "ERROR":
                        print(f"  [{ds.dataset_name}] {vr.message}", file=sys.stderr)
        return 1


def _cmd_report(args: argparse.Namespace) -> int:
    # Config errors must exit cleanly — see _cmd_run for rationale.
    try:
        config = load_config(args.config)
    except QualifireConfigError as e:
        # Prefix lives in ``load_config``; print verbatim to avoid
        # doubled ``Config validation failed:`` prefixes.
        print(str(e), file=sys.stderr)
        return 1

    from qualifire.api import Qualifire

    try:
        from pyspark.sql import SparkSession

        from qualifire.backends.spark_backend import SparkBackend

        spark = SparkSession.builder.getOrCreate()
        backend = SparkBackend(spark)
    except ImportError:
        # Only the SQLite system-table backend can run a report without Spark.
        # Catalog/Delta/JDBC all need a SparkSession to read history.
        if config.system_table_backend != "sqlite":
            print(
                f"qualifire report requires PySpark for "
                f"system_table_backend='{config.system_table_backend}'. "
                f"Install PySpark, or switch the system table to "
                f"system_table_backend='sqlite' for the no-Spark path.",
                file=sys.stderr,
            )
            return 1
        backend = None

    # See _cmd_run — storage init can raise, and the CLI contract is
    # "clean stderr + exit 1" for setup errors, not a raw traceback.
    try:
        qf = Qualifire._from_parsed_config(
            config,
            backend=backend,
            warn_on_reinit=False,
        )
    except RuntimeError as e:
        print(f"System table initialization failed: {e}", file=sys.stderr)
        return 1

    try:
        report = qf.health_report(days=args.days, output_path=args.output)
        print(f"Health report generated: {args.output}")
        print(f"  Period: last {report.days} days")
        print(f"  Total checks: {report.total_checks}")
        print(f"  Pass rate: {report.pass_rate}%")
        print(f"  Warnings: {report.warning_count} | Errors: {report.error_count}")
        if report.worst_offenders:
            print("  Worst offenders:")
            for d in report.worst_offenders[:5]:
                print(f"    {d['dataset']}: {d['error']} errors")
        return 0
    except Exception as e:
        print(f"Report generation failed: {e}", file=sys.stderr)
        return 1


def _parse_partition_arg(value: str) -> object:
    """Parse the ``--partition`` CLI argument into the shape
    :meth:`Qualifire.backfill` accepts.

    Grammar:
    - ``2026-04-01`` → single string
    - ``2026-04-01,2026-04-02,...`` → list of strings (no spaces)
    - ``2026-04-01..2026-04-07`` → ``(start, end)`` tuple
    - Combining ``,`` and ``..`` is rejected as ambiguous.
    """
    if ".." in value:
        if "," in value:
            raise argparse.ArgumentTypeError(
                "--partition cannot combine ',' (list) and '..' (range)"
            )
        parts = value.split("..", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise argparse.ArgumentTypeError(
                f"--partition range malformed: {value!r}; "
                f"expected START..END"
            )
        return (parts[0], parts[1])
    if "," in value:
        items = [v.strip() for v in value.split(",") if v.strip()]
        if not items:
            raise argparse.ArgumentTypeError(
                f"--partition empty list: {value!r}"
            )
        return items
    return value


def _open_qualifire_for_op(config) -> "object":
    """Build a Qualifire instance suitable for backfill / deactivate.

    Mirrors ``_cmd_run``'s pattern: try Spark, fall back to no-backend
    only when the system-table backend is SQLite (Spark-free path).
    """
    from qualifire.api import Qualifire

    backend = None
    try:
        from pyspark.sql import SparkSession
        from qualifire.backends.spark_backend import SparkBackend

        spark = SparkSession.builder.getOrCreate()
        backend = SparkBackend(spark)
    except ImportError:
        if config.system_table_backend != "sqlite":
            raise RuntimeError(
                f"PySpark required for system_table_backend="
                f"'{config.system_table_backend}'. Install PySpark or "
                f"switch to system_table_backend='sqlite'."
            )

    # ``_from_parsed_config`` reuses the already-loaded YAML — no
    # second ``load_config`` call (TOCTOU clean). The
    # ``warn_on_reinit=False`` is correct for the CLI single-YAML
    # workflow: backfill / deactivate construct from the same YAML
    # they execute against, so the reinit warning would be noise.
    return Qualifire._from_parsed_config(
        config,
        backend=backend,
        warn_on_reinit=False,
    )


def _cmd_backfill(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except QualifireConfigError as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        partition = _parse_partition_arg(args.partition)
    except argparse.ArgumentTypeError as e:
        print(str(e), file=sys.stderr)
        return 2

    try:
        qf = _open_qualifire_for_op(config)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    # Item 1 visibility (HS1 carve-out A): warn the operator
    # before the run that parallelism > 1 suppresses notifications.
    if getattr(args, "parallelism", 1) > 1:
        print(
            f"note: parallelism={args.parallelism} — notifications "
            f"suppressed for this run",
            file=sys.stderr,
        )

    try:
        report = qf.backfill(
            config,
            partition_ts=partition,
            selector=args.selector,
            data=args.data,
            skip_recollection=args.skip_recollection,
            skip_renotification=getattr(args, "skip_renotification", False),
            skip_revalidation=getattr(args, "skip_revalidation", False),
            soft_delete_prior=args.soft_delete_prior,
            parallelism=getattr(args, "parallelism", 1),
            max_partitions=getattr(args, "max_partitions", 10000),
        )
    except QualifireConfigError as e:
        print(f"Backfill config error: {e}", file=sys.stderr)
        return 1
    except QualifireValidationError as e:
        # Plan N18: backfill raises with a ``.report`` attribute
        # carrying the full BackfillReport. Surface it so operators
        # see the per-partition diff alongside the failure.
        print(f"Backfill failed: {e}", file=sys.stderr)
        report = getattr(e, "report", None)
        if report is None:
            return 1
        # Fall through to the report-printing path below.

    if args.json:
        print(json.dumps(report.to_dict(), default=str))
    else:
        print(
            f"Backfill complete: total={report.total} "
            f"refreshed={report.refreshed} "
            f"unchanged={report.unchanged} "
            f"skipped={report.skipped} "
            f"errored={report.errored}"
        )
        for diff in report.partitions:
            line = (
                f"  [{diff.status.upper()}] "
                f"{diff.dataset_name}.{diff.metric_name} "
                f"@ {diff.partition_ts.isoformat()}: "
                f"{diff.original_value} → {diff.backfilled_value}"
            )
            if diff.error:
                line += f"  error: {diff.error}"
            elif diff.skip_reason:
                line += f"  skip_reason: {diff.skip_reason}"
            # Item 5: route errored summary lines to stderr so
            # operators piping non-`--json` output to a downstream
            # tool can grep stderr for failures while keeping
            # stdout for the run's normal report.
            if diff.status == "errored":
                print(line, file=sys.stderr)
            else:
                print(line)

    return 1 if report.has_errors else 0


def _cmd_deactivate_metric(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except QualifireConfigError as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        qf = _open_qualifire_for_op(config)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        n = qf.deactivate_metric(
            config,
            dataset_name=args.dataset,
            metric_name=args.metric,
            dimension_value=args.dimension,
            partition_ts=args.partition,
            note=args.note,
        )
    except QualifireConfigError as e:
        print(str(e), file=sys.stderr)
        return 1

    print(
        f"deactivate-metric: wrote {n} tombstone(s) for "
        f"dataset={args.dataset!r} metric={args.metric!r}"
        + (f" partition={args.partition!r}" if args.partition else "")
        + (f" dimension={args.dimension!r}" if args.dimension else "")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
