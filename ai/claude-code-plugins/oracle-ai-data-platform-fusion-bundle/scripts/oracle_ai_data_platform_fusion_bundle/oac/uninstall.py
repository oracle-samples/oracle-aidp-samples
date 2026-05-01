"""Implementation of ``aidp-fusion-bundle dashboard uninstall --target oac``.

Removes the bundle's OAC artifacts in this order:
  1. Optionally deregister the bundle's snapshot record (``--snapshot-id``)
  2. Delete the bundle's data source connection (default ``aidp_fusion_jdbc``)

Caveat: there is no public REST endpoint to selectively delete workbooks
that were restored from a snapshot. If you want to fully roll back the
restored workbooks, take a snapshot BEFORE installing and restore that
"pre-install" snapshot to revert. Or use OAC Console -> Catalog to
manually delete the ``/shared/AIDP_Fusion_Bundle/`` folder.

Customer data in AIDP (gold marts, BICC bronze tables) is untouched.
"""

from __future__ import annotations

from dataclasses import dataclass

from rich.console import Console

from .rest import OacOauthFlow, OacRestClient, OacRestError, derive_oac_scope


@dataclass(frozen=True)
class UninstallParams:
    oac_url: str
    connection_name: str
    snapshot_id: str | None
    idcs_url: str
    client_id: str
    client_secret: str
    oauth_scope: str = ""  # Empty triggers auto-derive


@dataclass
class UninstallResult:
    connection_deleted: bool
    snapshot_deleted: bool | None = None  # None = not requested


def uninstall(params: UninstallParams, *, console: Console | None = None) -> UninstallResult:
    console = console or Console()

    fetcher = OacOauthFlow(
        idcs_url=params.idcs_url,
        client_id=params.client_id,
        client_secret=params.client_secret,
        scope=params.oauth_scope or derive_oac_scope(params.oac_url),
    )
    client = OacRestClient(params.oac_url, fetcher)

    # Snapshot deregistration (optional)
    snapshot_deleted: bool | None = None
    if params.snapshot_id:
        console.print(f"Deregistering snapshot [bold]{params.snapshot_id}[/bold] ...")
        try:
            snapshot_deleted = client.delete_snapshot(params.snapshot_id)
            console.print(
                "  [green]done[/green]" if snapshot_deleted
                else "  [yellow]not found (already absent)[/yellow]"
            )
        except OacRestError as exc:
            console.print(f"  [red]failed:[/red] {exc}")
            snapshot_deleted = False

    # Connection
    console.print(f"Deleting connection [bold]{params.connection_name}[/bold] ...")
    try:
        existing = client.find_connection(params.connection_name)
        if existing:
            connection_deleted = client.delete_connection(
                str(existing.get("id") or existing.get("connectionId") or params.connection_name),
                owner=existing.get("owner"),
            )
        else:
            connection_deleted = False
        console.print(
            "  [green]done[/green]"
            if connection_deleted
            else "  [yellow]not found (already absent)[/yellow]"
        )
    except OacRestError as exc:
        console.print(f"  [red]failed:[/red] {exc}")
        connection_deleted = False

    return UninstallResult(
        connection_deleted=connection_deleted,
        snapshot_deleted=snapshot_deleted,
    )


__all__ = ["UninstallParams", "UninstallResult", "uninstall"]
