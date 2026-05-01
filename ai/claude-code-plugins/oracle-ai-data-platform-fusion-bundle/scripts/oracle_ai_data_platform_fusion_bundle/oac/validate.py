"""Implementation of ``aidp-fusion-bundle dashboard validate --target oac``.

Read-only probe: confirms the bundle's connection + workbooks exist in OAC.
Does NOT touch AIDP-side data. Useful in CI to detect drift between what
the bundle expects and what's actually in the customer's OAC catalog.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from rich.console import Console

from .rest import OacOauthFlow, OacRestClient, OacRestError


@dataclass(frozen=True)
class ValidateParams:
    oac_url: str
    connection_name: str
    workbook_names: list[str]
    idcs_url: str
    client_id: str
    client_secret: str
    oauth_scope: str = ""  # Empty triggers auto-derive from oac_url


@dataclass
class ValidateResult:
    connection_present: bool
    present_workbooks: list[str] = field(default_factory=list)
    missing_workbooks: list[str] = field(default_factory=list)

    @property
    def all_ok(self) -> bool:
        return self.connection_present and not self.missing_workbooks


def validate(params: ValidateParams, *, console: Console | None = None) -> ValidateResult:
    console = console or Console()

    from .rest import derive_oac_scope
    fetcher = OacOauthFlow(
        idcs_url=params.idcs_url,
        client_id=params.client_id,
        client_secret=params.client_secret,
        scope=params.oauth_scope or derive_oac_scope(params.oac_url),
    )
    client = OacRestClient(params.oac_url, fetcher)

    result = ValidateResult(connection_present=False)

    # Connection
    try:
        connection = client.find_connection(params.connection_name)
    except OacRestError as exc:
        console.print(f"[red]find_connection failed:[/red] {exc}")
        connection = None
    result.connection_present = connection is not None
    console.print(
        f"Connection [bold]{params.connection_name}[/bold]: "
        f"{'[green]present[/green]' if connection else '[red]MISSING[/red]'}"
    )

    # Workbooks
    for wb_name in params.workbook_names:
        try:
            matches = client.list_workbooks(name=wb_name)
        except OacRestError as exc:
            console.print(f"[red]list_workbooks(name={wb_name}) failed:[/red] {exc}")
            matches = []
        if matches:
            result.present_workbooks.append(wb_name)
            console.print(f"Workbook [bold]{wb_name}[/bold]: [green]present[/green]")
        else:
            result.missing_workbooks.append(wb_name)
            console.print(f"Workbook [bold]{wb_name}[/bold]: [red]MISSING[/red]")

    return result


__all__ = ["ValidateParams", "ValidateResult", "validate"]
