"""Implementation of ``aidp-fusion-bundle catalog list`` and ``catalog probe``.

* ``list`` — pretty-print the bundle's curated PVO catalog.
* ``probe`` — hit the live BICC ``/biacm/rest/meta/datastores`` endpoint
  and reconcile each catalog entry against what the customer's pod
  actually exposes.
"""

from __future__ import annotations

from typing import Any

import requests
from rich.console import Console
from rich.table import Table

from ..schema.fusion_catalog import CATALOG, PvoEntry, PvoKind


def list_catalog(*, console: Console | None = None) -> int:
    """Pretty-print the catalog as a table."""
    console = console or Console()
    table = Table(title=f"PVO catalog ({len(CATALOG)} entries)", show_lines=False)
    table.add_column("id", no_wrap=True, style="cyan")
    table.add_column("datastore", overflow="fold")
    table.add_column("schema")
    table.add_column("bronze table", style="green")
    table.add_column("kind")
    table.add_column("✓", justify="center")

    for entry in sorted(CATALOG.values(), key=lambda e: (e.schema, e.id)):
        kind_marker = (
            "[red]OTBI[/red]" if entry.kind is PvoKind.OTBI else "ExtractPVO"
        )
        confirmed = "[green]ok[/green]" if entry.confirmed else "[yellow]?[/yellow]"
        table.add_row(
            entry.id,
            entry.datastore,
            entry.schema,
            entry.bronze_table,
            kind_marker,
            confirmed,
        )
    console.print(table)
    confirmed_count = sum(1 for e in CATALOG.values() if e.confirmed)
    console.print(
        f"\n[green]{confirmed_count}[/green] verbatim-from-Oracle, "
        f"[yellow]{len(CATALOG) - confirmed_count}[/yellow] need live verification "
        f"([dim]run [/dim][cyan]catalog probe --pod <url>[/cyan][dim] to reconcile[/dim])"
    )
    return 0


def probe_catalog(
    pod: str,
    *,
    username: str | None = None,
    password: str | None = None,
    console: Console | None = None,
) -> int:
    """Hit ``GET {pod}/biacm/rest/meta/datastores`` and reconcile against CATALOG.

    Args:
        pod: Fusion pod URL (e.g. ``https://my-pod.fa.<region>.oraclecloud.com``).
        username: HTTP Basic username (BIAdmin role required to read datastores).
            If absent, falls back to env var ``FUSION_BICC_USER``.
        password: HTTP Basic password. Falls back to env var ``FUSION_BICC_PASSWORD``.

    Returns:
        Process exit code: 0 if every catalog entry resolves to a live datastore,
        1 if any are missing or the BICC API call failed.
    """
    import os
    console = console or Console()
    user = username or os.environ.get("FUSION_BICC_USER")
    pwd = password or os.environ.get("FUSION_BICC_PASSWORD")
    if not (user and pwd):
        console.print(
            "[red]missing creds:[/red] pass --user/--password or set "
            "FUSION_BICC_USER + FUSION_BICC_PASSWORD env vars"
        )
        return 2

    url = pod.rstrip("/") + "/biacm/rest/meta/datastores"
    console.print(f"GET [cyan]{url}[/cyan] ...")
    try:
        response = requests.get(url, auth=(user, pwd), timeout=60)
    except requests.RequestException as exc:
        console.print(f"[red]network error:[/red] {exc}")
        return 1
    if response.status_code != 200:
        console.print(
            f"[red]HTTP {response.status_code}:[/red] {response.text[:200]}"
        )
        return 1

    body = response.json()
    live_datastores = _extract_datastore_names(body)
    console.print(f"  [green]{len(live_datastores)}[/green] datastores in catalog")

    table = Table(title="Catalog reconcile", show_lines=False)
    table.add_column("id", style="cyan")
    table.add_column("datastore", overflow="fold")
    table.add_column("status")
    missing: list[PvoEntry] = []
    for entry in sorted(CATALOG.values(), key=lambda e: e.id):
        live = entry.datastore in live_datastores
        if live:
            table.add_row(entry.id, entry.datastore, "[green]LIVE[/green]")
        else:
            table.add_row(entry.id, entry.datastore, "[red]MISSING[/red]")
            missing.append(entry)
    console.print(table)

    if missing:
        console.print(
            f"\n[red]{len(missing)} catalog entries missing on this pod:[/red]"
        )
        for e in missing:
            console.print(f"  - {e.id}: {e.datastore}")
        return 1
    console.print(f"\n[green]all {len(CATALOG)} entries reconcile against {pod}.[/green]")
    return 0


def _extract_datastore_names(body: Any) -> set[str]:
    """Pull datastore names out of BICC's response (shape varies by release)."""
    names: set[str] = set()

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            for key in ("name", "datastoreName", "viewObjectName", "dataStoreName"):
                val = node.get(key)
                if isinstance(val, str):
                    names.add(val)
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(body)
    return names


__all__ = ["list_catalog", "probe_catalog"]
