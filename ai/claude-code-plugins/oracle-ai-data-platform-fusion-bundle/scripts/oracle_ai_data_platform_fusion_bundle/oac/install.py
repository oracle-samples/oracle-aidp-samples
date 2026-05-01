"""Implementation of ``aidp-fusion-bundle dashboard install --target oac``.

Three modes:

1. **Print-only fallback** (``--print-only``): writes the 6-key JSON file the
   user uploads via OAC's UI (Data -> Connections -> Create -> "Oracle AI
   Data Platform"). Works without any IDCS app registration. Use this when
   IDCS admin access is unavailable.

2. **Full REST install** (default): authenticates to IDCS, POSTs the
   connection to ``/api/<v>/catalog/connections``, then uploads each
   ``oac/workbooks/*.dva`` archive via ``/api/<v>/catalog/workbooks/imports``.

3. **Validate-only** (``--validate``): no writes; lists existing connections /
   workbooks to confirm the bundle is already installed.

The print-only fallback is the safer default for first-time runs because
IDCS confidential-application registration requires admin access most
non-admins don't have. Full-REST mode kicks in once the admin has run the
one-time setup in ``docs/oac_rest_api_setup.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from .rest import (
    AidpConnectionPayload,
    OacOauthFlow,
    OacRestClient,
    OacRestError,
    build_payload,
    render_template,
)


@dataclass(frozen=True)
class InstallParams:
    """All inputs the install command needs (collected from bundle.yaml + CLI flags)."""

    oac_url: str
    connection_name: str
    region: str

    # Connection JSON inputs
    user_ocid: str
    tenancy_ocid: str
    fingerprint: str
    idl_ocid: str
    cluster_key: str
    catalog: str

    # IDCS OAuth (only required for non-print-only flow)
    idcs_url: str | None
    client_id: str | None
    client_secret: str | None
    oauth_scope: str

    # Local file paths
    private_key_pem_path: Path
    workbooks_dir: Path

    # OAuth flow choice: "auth_code" (browser popup) or "device" (headless)
    auth_flow: str = "auth_code"

    # Behaviour flags
    print_only: bool = False
    skip_workbooks: bool = False


@dataclass
class InstallResult:
    """Summary of what was created / printed by an install run."""

    json_template_path: Path | None = None
    connection_id: str | None = None
    imported_workbooks: list[str] | None = None
    skipped_reason: str | None = None

    def __post_init__(self) -> None:
        if self.imported_workbooks is None:
            self.imported_workbooks = []


def install(params: InstallParams, *, console: Console | None = None) -> InstallResult:
    """Run a full ``dashboard install --target oac`` against the configured OAC instance."""
    console = console or Console()
    payload = build_payload(
        user_ocid=params.user_ocid,
        tenancy_ocid=params.tenancy_ocid,
        region=params.region,
        fingerprint=params.fingerprint,
        idl_ocid=params.idl_ocid,
        cluster_key=params.cluster_key,
        catalog=params.catalog,
    )

    if params.print_only:
        return _install_print_only(params, payload, console)

    return _install_via_rest(params, payload, console)


# ---------------------------------------------------------------------------
# Print-only mode
# ---------------------------------------------------------------------------


def _install_print_only(
    params: InstallParams,
    payload: AidpConnectionPayload,
    console: Console,
) -> InstallResult:
    """Write the connection JSON + tell the user where to upload it."""
    out_path = Path("oac") / "data_source" / f"{params.connection_name}.json"
    written = render_template(payload, out_path)
    console.print(
        f"[green][PRINT-ONLY][/green] Wrote OAC connection JSON: [bold]{written}[/bold]"
    )
    console.print(
        "\n[bold]Next steps (manual upload via OAC UI):[/bold]\n"
        f"  1. Open [cyan]{params.oac_url}[/cyan] -> Data -> Connections -> Create -> "
        f"\"Oracle AI Data Platform\"\n"
        f"  2. Connection Name: [bold]{params.connection_name}[/bold]\n"
        f"  3. Connection Details: upload [bold]{written}[/bold]\n"
        f"  4. Private API Key: upload [bold]{params.private_key_pem_path}[/bold]\n"
        f"  5. Save -> verify schemas tree shows [bold]fusion_catalog[/bold]\n"
        f"\n[dim]To automate steps 1-5, register an IDCS confidential application "
        f"(see docs/oac_rest_api_setup.md) and re-run without [/dim][cyan]--print-only[/cyan].\n"
    )
    return InstallResult(json_template_path=written)


# ---------------------------------------------------------------------------
# Full REST mode
# ---------------------------------------------------------------------------


def _install_via_rest(
    params: InstallParams,
    payload: AidpConnectionPayload,
    console: Console,
) -> InstallResult:
    """Hybrid install: connection JSON via print-only, workbooks via REST.

    OAC's public REST API does not publish a documented schema for the
    "Oracle AI Data Platform" connectionType (verified live 2026-05-01,
    TC10h). The 6-key JSON is UI-form-only on OAC's side. Workbook imports,
    however, work via REST with a user-context Bearer token.

    So this function:

      1. Writes the connection JSON file (the proven print-only path) and
         tells the user the 3-min UI upload step.
      2. Authenticates via Auth Code + PKCE / Device Code (one-time consent;
         refresh token persists).
      3. Imports each ``oac/workbooks/*.dva`` via REST.

    If the user passed ``--skip-workbooks``, step 3 is a no-op and the function
    devolves to print-only.
    """
    # Step 1: write connection JSON (always, as a deliverable for the admin)
    out_path = Path("oac") / "data_source" / f"{params.connection_name}.json"
    written = render_template(payload, out_path)
    console.print(
        f"[green][CONNECTION][/green] Wrote OAC connection JSON: [bold]{written}[/bold]"
    )
    console.print(
        f"\n[bold]Connection upload (one-time, ~3 min):[/bold]\n"
        f"  Open [cyan]{params.oac_url}[/cyan] -> Data -> Connections -> Create -> "
        f"\"Oracle AI Data Platform\"\n"
        f"  Connection Name: [bold]{params.connection_name}[/bold]\n"
        f"  Connection Details: upload [bold]{written}[/bold]\n"
        f"  Private API Key: upload [bold]{params.private_key_pem_path}[/bold]\n"
        f"\n[dim]REST schema for the AIDP connectionType is undocumented by Oracle "
        f"(see TC10h). UI upload is currently the only path. Workbooks will be "
        f"imported via REST below.[/dim]\n"
    )

    if params.skip_workbooks:
        console.print("[yellow]--skip-workbooks set; not importing .dva files via REST.[/yellow]")
        return InstallResult(json_template_path=written, imported_workbooks=[])

    # Step 2: authenticate
    if not (params.idcs_url and params.client_id and params.client_secret):
        raise ValueError(
            "Workbook REST import requires --idcs-url, --client-id, --client-secret "
            "(or the equivalent fields in bundle.yaml). Pass --skip-workbooks to "
            "stop after the connection JSON is written."
        )

    fetcher = OacOauthFlow(
        idcs_url=params.idcs_url,
        client_id=params.client_id,
        client_secret=params.client_secret,
        scope=params.oauth_scope,
        flow=params.auth_flow,  # "auth_code" (default) or "device"
    )
    client = OacRestClient(params.oac_url, fetcher)

    # Step 3: import workbooks
    imported: list[str] = []
    for dva in _iter_workbook_files(params.workbooks_dir):
        console.print(f"Importing workbook [bold]{dva.name}[/bold] ...")
        try:
            client.import_workbook(dva)
            imported.append(dva.name)
            console.print("  [green]done[/green]")
        except OacRestError as exc:
            console.print(f"  [red]failed:[/red] {exc}")

    return InstallResult(
        json_template_path=written,
        imported_workbooks=imported,
    )


def _iter_workbook_files(workbooks_dir: Path) -> list[Path]:
    if not workbooks_dir.exists():
        return []
    return sorted(workbooks_dir.glob("*.dva"))


__all__ = ["InstallParams", "InstallResult", "install"]
