"""Implementation of ``aidp-fusion-bundle dashboard install --target oac``.

Architecture (TC10h-2 refactor, 2026-05-01) — strictly Oracle-documented endpoints.

The bundle ships content via two documented public APIs:

1. **Connection** — ``POST /api/20210901/catalog/connections``
   Documented endpoint. The body schema is open (``type: object``); the
   AIDP-specific ``connectionType: "idljdbc"`` payload is reverse-engineered
   from the OAC UI's create-connection traffic (TC10h, 2026-05-01).
   Per-customer secrets (PEM, fingerprint, OCIDs) are sent here.

2. **Workbook content** — Snapshot register + restore (the only public path)
   The bundle author builds a ``.bar`` once via
   ``aidp-fusion-bundle bundle build-bar`` from a clean dev OAC containing
   the 5 workbooks under ``/shared/AIDP_Fusion_Bundle/``. The customer
   uploads the .bar to OCI Object Storage; the bundle then:
       * ``POST /api/20210901/snapshots`` (REGISTER, async)
       * ``POST /api/20210901/system/actions/restoreSnapshot`` (async)
       * Polls ``GET /workRequests/{id}`` until SUCCEEDED.
   The .bar excludes ``Credentials`` and ``Connections`` content types so
   workbooks reference the connection-by-name created in step 1.

3. **Print-only** (``--print-only``) — writes the connection JSON for manual
   UI upload; no REST calls. Useful when no IDCS confidential app is set up.

Why not workbook .dva imports? ``POST /catalog/workbooks/imports`` is NOT
in Oracle's openapi.json; it's UI-only with no API stability guarantee.

User must hold ``BI Service Administrator`` application role on the OAC instance.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from .rest import (
    AidpConnectionPayload,
    OacOauthFlow,
    OacRestClient,
    OacRestError,
    WorkRequestStatus,
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

    # Snapshot configuration (workbook content delivery)
    bar_bucket: str | None       # OCI Object Storage bucket name containing the bundle .bar
    bar_uri: str | None          # Object name (relative path) of the .bar within the bucket
    bar_password: str | None     # Optional BAR password
    snapshot_name: str           # Display name for the registered snapshot

    # OAuth flow choice
    auth_flow: str = "auth_code"
    prompt_login: bool = False

    # Behaviour flags
    print_only: bool = False
    skip_workbooks: bool = False
    overwrite_connection: bool = False


@dataclass
class InstallResult:
    """Summary of what was created / restored by an install run."""

    json_template_path: Path | None = None
    connection_id: str | None = None
    snapshot_id: str | None = None
    work_request_id: str | None = None
    work_request_status: str | None = None
    skipped_reason: str | None = None


def install(params: InstallParams, *, console: Console | None = None) -> InstallResult:
    """Run a ``dashboard install --target oac`` against the configured OAC instance."""
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
    """Write the connection JSON + tell the user how to upload via the OAC UI.

    No REST calls; safe when no IDCS confidential app is configured.
    """
    out_path = Path("oac") / "data_source" / f"{params.connection_name}.json"
    written = render_template(payload, out_path)
    console.print(
        f"[green][PRINT-ONLY][/green] Wrote OAC connection JSON: [bold]{written}[/bold]"
    )
    console.print(
        f"\n[bold]Next steps (manual upload via OAC UI):[/bold]\n"
        f"  1. Open [cyan]{params.oac_url}[/cyan] -> Data -> Connections -> Create -> "
        f'"Oracle AI Data Platform"\n'
        f"  2. Connection Name: [bold]{params.connection_name}[/bold]\n"
        f"  3. Connection Details: upload [bold]{written}[/bold]\n"
        f"  4. Private API Key: upload [bold]{params.private_key_pem_path}[/bold]\n"
        f"  5. Save -> verify schemas tree shows [bold]fusion_catalog[/bold]\n"
        f"  6. Workbooks: ask your OAC admin to restore the bundle's .bar via\n"
        f"     Console -> Snapshots -> Restore.\n"
        f"\n[dim]To automate steps 1-5, register an IDCS confidential application "
        f"(see docs/oac_rest_api_setup.md) and re-run without[/dim] [cyan]--print-only[/cyan].\n"
    )
    return InstallResult(json_template_path=written)


# ---------------------------------------------------------------------------
# Full REST mode (hybrid: connection POST + snapshot restore)
# ---------------------------------------------------------------------------


def _install_via_rest(
    params: InstallParams,
    payload: AidpConnectionPayload,
    console: Console,
) -> InstallResult:
    """Full REST install: connection POST + snapshot register/restore + poll work request."""
    if not (params.idcs_url and params.client_id and params.client_secret):
        raise ValueError(
            "Full REST install requires --idcs-url, --client-id, --client-secret. "
            "Use --print-only to skip REST and write a JSON file for manual upload."
        )

    fetcher = OacOauthFlow(
        idcs_url=params.idcs_url,
        client_id=params.client_id,
        client_secret=params.client_secret,
        scope=params.oauth_scope,
        flow=params.auth_flow,
        prompt_login=params.prompt_login,
    )
    client = OacRestClient(params.oac_url, fetcher)

    result = InstallResult()

    # Step 1: connection
    existing = client.find_connection(params.connection_name)
    if existing and not params.overwrite_connection:
        connection_id = str(existing.get("id") or existing.get("connectionId") or "<unknown>")
        result.connection_id = connection_id
        console.print(
            f"[yellow]Connection '{params.connection_name}' already exists "
            f"(id={connection_id}). Skipping create. Use --overwrite-connection to recreate.[/yellow]"
        )
    else:
        if existing and params.overwrite_connection:
            console.print(
                f"Deleting existing connection [bold]{params.connection_name}[/bold] (overwrite)..."
            )
            try:
                client.delete_connection(
                    str(existing.get("id") or existing.get("connectionId") or params.connection_name),
                    owner=existing.get("owner"),
                )
            except OacRestError as exc:
                console.print(f"  [yellow]delete failed (continuing): {exc}[/yellow]")

        console.print(f"Creating OAC connection [bold]{params.connection_name}[/bold] ...")
        try:
            created = client.create_connection(
                name=params.connection_name,
                payload=payload,
                private_key_pem_path=params.private_key_pem_path,
                description=(
                    "AIDP Fusion bundle JDBC connection — auto-installed by "
                    "aidp-fusion-bundle dashboard install"
                ),
                catalog=params.catalog,
            )
            connection_id = str(created.get("connectionId") or created.get("id") or "<unknown>")
            result.connection_id = connection_id
            console.print(f"  [green]done[/green] (connectionId={connection_id})")
        except OacRestError as exc:
            console.print(f"  [red]connection POST rejected:[/red] {exc}")
            console.print(
                "  [dim]Note: AIDP `idljdbc` connectionType is reverse-engineered from\n"
                "  the OAC UI; if your OAC version rejects it, fall back to --print-only\n"
                "  and upload the JSON via Data -> Connections -> Create -> 'Oracle AI Data Platform'.[/dim]"
            )
            raise

    # Step 2: workbook content via snapshot
    if params.skip_workbooks:
        console.print("[yellow]--skip-workbooks set; not restoring snapshot.[/yellow]")
        return result

    if not (params.bar_bucket and params.bar_uri):
        console.print(
            "[yellow]No --bar-bucket / --bar-uri provided; skipping snapshot restore.\n"
            "  To deploy workbooks, re-run with the snapshot location:\n"
            "    --bar-bucket <oci-bucket> --bar-uri <object-name-of-.bar>\n"
            "  See docs/oac_rest_api_setup.md for OCI Object Storage prep.[/yellow]"
        )
        return result

    console.print(
        f"Registering snapshot [bold]{params.snapshot_name}[/bold] from "
        f"[cyan]{params.bar_bucket}/{params.bar_uri}[/cyan] ..."
    )
    try:
        snap = client.register_snapshot(
            name=params.snapshot_name,
            bucket=params.bar_bucket,
            bar_uri=params.bar_uri,
            password=params.bar_password,
        )
    except OacRestError as exc:
        console.print(f"  [red]register_snapshot failed:[/red] {exc}")
        raise
    snapshot_id = str(snap.get("id") or "<unknown>")
    result.snapshot_id = snapshot_id
    console.print(f"  [green]registered[/green] (snapshotId={snapshot_id})")

    console.print(f"Restoring snapshot [bold]{snapshot_id}[/bold] ...")
    try:
        wr_id = client.restore_snapshot(snapshot_id, password=params.bar_password)
    except OacRestError as exc:
        console.print(f"  [red]restore_snapshot failed:[/red] {exc}")
        raise
    result.work_request_id = wr_id
    console.print(f"  [green]restore accepted[/green] (workRequestId={wr_id}); polling ...")

    try:
        wr = client.poll_work_request(wr_id, timeout=900, poll_interval=10)
    except TimeoutError as exc:
        console.print(f"  [red]restore timed out:[/red] {exc}")
        result.work_request_status = "TIMED_OUT"
        return result
    status = wr.get("status") or wr.get("lifecycleState") or "<unknown>"
    result.work_request_status = status
    if status == WorkRequestStatus.SUCCEEDED:
        console.print(f"  [green]restore complete[/green] ({status})")
    else:
        err = wr.get("errorDetails") or wr.get("error") or wr
        console.print(f"  [red]restore did not succeed: {status}[/red]\n  Details: {err}")

    return result


__all__ = ["InstallParams", "InstallResult", "install"]
