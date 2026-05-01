"""Implementation of ``aidp-fusion-bundle run`` and ``status``.

The bundle's pipeline runs **inside AIDP** (Spark on the customer's tpcds
or equivalent cluster) — the CLI's job is to dispatch the right notebook
job(s) with the right parameters, then return immediately with a job
handle. State is persisted to the AIDP-side ``fusion_bundle_state`` Delta
table so ``status`` can read it back without re-running the pipeline.

Two execution modes:
  * **dispatch** (default): submits an AIDP REST job per dataset; the
    notebook reads ``bundle.yaml`` from its working directory and invokes
    the ``orchestrator`` subpackage.
  * **inline** (``--inline``, when running under an AIDP notebook):
    imports and runs the orchestrator directly in-process — useful for
    debugging from inside a notebook session.

This implementation calls the AIDP REST job API generically; the
notebook entry point is ``orchestrator.run(bundle_path, mode, datasets)``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console
from rich.table import Table


def run(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    mode: str = "incremental",
    datasets: str | None = None,
    inline: bool = False,
    console: Console | None = None,
) -> int:
    """Submit the bundle's pipeline to AIDP, or run inline if --inline."""
    console = console or Console()
    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1

    bundle_data = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    requested_ids = _resolve_datasets(bundle_data, datasets)
    if not requested_ids:
        console.print("[red]no datasets to run[/red] (none enabled in bundle.yaml or all filtered out)")
        return 1

    if inline:
        return _run_inline(bundle_data, requested_ids, mode, console)
    return _run_via_aidp_dispatch(
        bundle_path, config_path, env_name, requested_ids, mode, console
    )


def status(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    console: Console | None = None,
) -> int:
    """Show last-run summary per dataset (reads ``fusion_bundle_state`` Delta table).

    Reading the state table requires Spark; if Spark isn't available
    locally, this falls back to printing the AIDP workspace path the
    state lives at and prompts the user to query it via a notebook.
    """
    console = console or Console()
    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1
    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    aidp = bundle.get("aidp", {})
    catalog = aidp.get("catalog", "fusion_catalog")
    state_table = f"{catalog}.bronze.fusion_bundle_state"

    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError:
        console.print(
            f"[yellow]pyspark not available locally; cannot read {state_table}[/yellow]"
        )
        console.print(
            "Run this query inside an AIDP notebook session:\n"
            f"  [cyan]SELECT dataset_id, mode, last_watermark, last_run_at, status, "
            f"row_count FROM {state_table} ORDER BY last_run_at DESC[/cyan]"
        )
        return 0

    spark = SparkSession.builder.appName("aidp-fusion-bundle-status").getOrCreate()
    try:
        df = spark.sql(
            f"SELECT dataset_id, mode, last_watermark, last_run_at, status, row_count "
            f"FROM {state_table} ORDER BY last_run_at DESC"
        )
        rows = df.collect()
    except Exception as exc:
        console.print(f"[red]could not read {state_table}:[/red] {exc}")
        return 1

    if not rows:
        console.print(f"[yellow]{state_table} is empty — no runs recorded yet[/yellow]")
        return 0

    table = Table(title=state_table)
    for col in ("dataset_id", "mode", "last_watermark", "last_run_at", "status", "row_count"):
        table.add_column(col)
    for r in rows:
        table.add_row(
            str(r["dataset_id"]),
            str(r["mode"]),
            str(r["last_watermark"]) if r["last_watermark"] else "-",
            str(r["last_run_at"]),
            str(r["status"]),
            str(r["row_count"]) if r["row_count"] is not None else "-",
        )
    console.print(table)
    return 0


def _resolve_datasets(bundle_data: dict, datasets_filter: str | None) -> list[str]:
    enabled = [
        d["id"] for d in bundle_data.get("datasets", [])
        if d.get("enabled", True)
    ]
    if not datasets_filter:
        return enabled
    requested = {x.strip() for x in datasets_filter.split(",") if x.strip()}
    return [i for i in enabled if i in requested]


def _run_inline(
    bundle_data: dict, dataset_ids: list[str], mode: str, console: Console
) -> int:
    """Run the orchestrator in-process. Imports the orchestrator subpackage lazily."""
    try:
        from .. import orchestrator  # type: ignore[attr-defined]
    except ImportError as exc:
        console.print(f"[red]orchestrator not importable:[/red] {exc}")
        return 1

    # The orchestrator subpackage currently has no top-level run(); this is the contract
    # the notebook entry point implements. Surface the contract clearly so the customer
    # knows what their notebook should call.
    fn = getattr(orchestrator, "run", None)
    if fn is None:
        console.print(
            "[yellow]orchestrator.run() not implemented yet[/yellow] — "
            "the inline path expects an AIDP notebook to import "
            "[cyan]oracle_ai_data_platform_fusion_bundle.orchestrator[/cyan] "
            "and call its run(bundle_data, mode, dataset_ids) entry point."
        )
        console.print(f"\nWould have run: mode={mode}, datasets={dataset_ids}")
        return 0
    fn(bundle_data, mode=mode, dataset_ids=dataset_ids)
    return 0


def _run_via_aidp_dispatch(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    dataset_ids: list[str],
    mode: str,
    console: Console,
) -> int:
    """Submit one AIDP REST job per dataset.

    The bundle ships ``notebooks/run_orchestrator.ipynb`` (TODO) which is
    the per-dataset entry point. Until that notebook is published, this
    command prints the dispatch plan so the user can run it manually.
    """
    console.print(
        f"[bold]Dispatch plan[/bold] (env=[cyan]{env_name}[/cyan], mode=[cyan]{mode}[/cyan]):"
    )
    table = Table()
    table.add_column("dataset", style="cyan")
    table.add_column("would dispatch")
    for dsid in dataset_ids:
        table.add_row(
            dsid,
            f"AIDP job: notebooks/run_orchestrator.ipynb "
            f"--params dataset_id={dsid} mode={mode}",
        )
    console.print(table)
    console.print(
        "\n[yellow]NOTE:[/yellow] dispatch submission is wired only when "
        "[cyan]notebooks/run_orchestrator.ipynb[/cyan] exists in the workspace. "
        "Today, run those commands manually inside an AIDP notebook session."
    )
    return 0


__all__ = ["run", "status"]
