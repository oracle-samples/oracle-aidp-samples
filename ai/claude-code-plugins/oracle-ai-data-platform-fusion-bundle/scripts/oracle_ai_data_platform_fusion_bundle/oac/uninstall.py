"""Implementation of ``aidp-fusion-bundle dashboard uninstall --target oac``.

Removes the bundle's OAC artifacts in reverse order:
  1. Each imported ``oac/workbooks/*.dva`` workbook (matched by name)
  2. The bundle's data source connection (default ``aidp_fusion_jdbc``)

Customer data in AIDP (gold marts, BICC bronze tables) is untouched. Only
the OAC catalog entries the bundle created are removed.
"""

from __future__ import annotations

from dataclasses import dataclass

from rich.console import Console

from .rest import OacOauthFlow, OacRestClient, OacRestError, derive_oac_scope


@dataclass(frozen=True)
class UninstallParams:
    oac_url: str
    connection_name: str
    workbook_names: list[str]
    idcs_url: str
    client_id: str
    client_secret: str
    oauth_scope: str = ""  # Empty triggers auto-derive from oac_url


@dataclass
class UninstallResult:
    deleted_workbooks: list[str]
    connection_deleted: bool


def uninstall(params: UninstallParams, *, console: Console | None = None) -> UninstallResult:
    console = console or Console()

    fetcher = OacOauthFlow(
        idcs_url=params.idcs_url,
        client_id=params.client_id,
        client_secret=params.client_secret,
        scope=params.oauth_scope or derive_oac_scope(params.oac_url),
    )
    client = OacRestClient(params.oac_url, fetcher)

    # Workbooks first, so the connection can be cleanly removed afterwards.
    deleted_workbooks: list[str] = []
    for wb_name in params.workbook_names:
        try:
            matches = client.list_workbooks(name=wb_name)
        except OacRestError as exc:
            console.print(f"[red]list_workbooks(name={wb_name}) failed:[/red] {exc}")
            continue
        for wb in matches:
            wb_id = str(wb.get("id") or wb.get("workbookId") or "")
            if not wb_id:
                continue
            console.print(f"Deleting workbook [bold]{wb_name}[/bold] (id={wb_id}) ...")
            try:
                if client.delete_workbook(wb_id):
                    deleted_workbooks.append(wb_name)
                    console.print("  [green]done[/green]")
            except OacRestError as exc:
                console.print(f"  [red]failed:[/red] {exc}")

    # Connection
    console.print(f"Deleting connection [bold]{params.connection_name}[/bold] ...")
    try:
        connection_deleted = client.delete_connection(params.connection_name)
        console.print(
            "  [green]done[/green]"
            if connection_deleted
            else "  [yellow]not found (already absent)[/yellow]"
        )
    except OacRestError as exc:
        console.print(f"  [red]failed:[/red] {exc}")
        connection_deleted = False

    return UninstallResult(
        deleted_workbooks=deleted_workbooks, connection_deleted=connection_deleted
    )


__all__ = ["UninstallParams", "UninstallResult", "uninstall"]
