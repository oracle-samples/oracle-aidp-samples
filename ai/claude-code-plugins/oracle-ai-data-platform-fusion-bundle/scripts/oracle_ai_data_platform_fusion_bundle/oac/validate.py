"""Implementation of ``aidp-fusion-bundle dashboard validate --target oac``.

Read-only probe: confirms the bundle's connection is present in OAC. Optionally
checks that a named snapshot is registered. Does NOT touch AIDP-side data.
Useful in CI to detect drift between what the bundle expects and what's
actually installed.
"""

from __future__ import annotations

from dataclasses import dataclass

from rich.console import Console

from .rest import OacOauthFlow, OacRestClient, OacRestError, derive_oac_scope


@dataclass(frozen=True)
class ValidateParams:
    oac_url: str
    connection_name: str
    snapshot_name: str | None
    idcs_url: str
    client_id: str
    client_secret: str
    oauth_scope: str = ""  # empty triggers auto-derive


@dataclass
class ValidateResult:
    connection_present: bool
    snapshot_present: bool | None = None  # None = not probed

    @property
    def all_ok(self) -> bool:
        if not self.connection_present:
            return False
        if self.snapshot_present is False:  # explicitly probed and missing
            return False
        return True


def validate(params: ValidateParams, *, console: Console | None = None) -> ValidateResult:
    console = console or Console()

    fetcher = OacOauthFlow(
        idcs_url=params.idcs_url,
        client_id=params.client_id,
        client_secret=params.client_secret,
        scope=params.oauth_scope or derive_oac_scope(params.oac_url),
    )
    client = OacRestClient(params.oac_url, fetcher)

    # Connection
    try:
        connection = client.find_connection(params.connection_name)
    except OacRestError as exc:
        console.print(f"[red]find_connection failed:[/red] {exc}")
        connection = None
    connection_present = connection is not None
    console.print(
        f"Connection [bold]{params.connection_name}[/bold]: "
        f"{'[green]present[/green]' if connection else '[red]MISSING[/red]'}"
    )

    # Snapshot (optional probe)
    snapshot_present: bool | None = None
    if params.snapshot_name:
        try:
            snapshots = client.list_snapshots()
            snapshot_present = any(
                s.get("name") == params.snapshot_name or s.get("displayName") == params.snapshot_name
                for s in snapshots
            )
            console.print(
                f"Snapshot [bold]{params.snapshot_name}[/bold]: "
                f"{'[green]registered[/green]' if snapshot_present else '[red]NOT REGISTERED[/red]'}"
            )
        except OacRestError as exc:
            console.print(f"[red]list_snapshots failed:[/red] {exc}")
            snapshot_present = False

    return ValidateResult(
        connection_present=connection_present,
        snapshot_present=snapshot_present,
    )


__all__ = ["ValidateParams", "ValidateResult", "validate"]
