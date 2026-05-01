"""CLI entry point for `aidp-fusion-bundle`.

Subcommand groups:
    init / validate / bootstrap / catalog / run / status         (orchestration)
    dashboard install / validate / uninstall / mcp-config        (OAC integration)

Each command body lives in its own module under this package — `cli.py`
only wires click together so `--help` is the single source of truth for
the user-facing surface.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
from rich.console import Console

from . import __version__

console = Console()


# ---------------------------------------------------------------------------
# Top-level group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(__version__, prog_name="aidp-fusion-bundle")
@click.option("--bundle", "bundle_path", type=click.Path(path_type=Path), default=Path("bundle.yaml"),
              help="Path to bundle.yaml (default: ./bundle.yaml).")
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=Path("aidp.config.yaml"),
              help="Path to aidp.config.yaml (default: ./aidp.config.yaml).")
@click.option("--env", "env_name", default="dev", help="Environment name from aidp.config.yaml (default: dev).")
@click.pass_context
def main(ctx: click.Context, bundle_path: Path, config_path: Path, env_name: str) -> None:
    """Productized Fusion -> AIDP pipeline (BICC + Delta + OAC)."""
    ctx.ensure_object(dict)
    ctx.obj["bundle_path"] = bundle_path
    ctx.obj["config_path"] = config_path
    ctx.obj["env_name"] = env_name


# ---------------------------------------------------------------------------
# Orchestration commands
# ---------------------------------------------------------------------------


@main.command()
@click.option("--template", type=click.Choice(["minimal", "full-finance"]), default="minimal",
              help="Which example to scaffold (default: minimal).")
@click.option("--force", is_flag=True, help="Overwrite existing bundle.yaml / aidp.config.yaml.")
def init(template: str, force: bool) -> None:
    """Scaffold a bundle.yaml + aidp.config.yaml in the current directory."""
    from .commands.init import init as init_impl
    sys.exit(init_impl(template=template, force=force, console=console))


@main.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Validate bundle.yaml schema + ref-integrity (no network calls)."""
    from .commands.validate import validate as validate_impl
    sys.exit(validate_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        console=console,
    ))


@main.command()
@click.option("--check-iam", is_flag=True, help="Also probe OCI IAM policies (requires AIDP RP credentials).")
@click.pass_context
def bootstrap(ctx: click.Context, check_iam: bool) -> None:
    """Probe all prerequisites against the live Fusion pod + AIDP workspace."""
    from .commands.bootstrap import bootstrap as bootstrap_impl
    sys.exit(bootstrap_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        check_iam=check_iam,
        console=console,
    ))


@main.group()
def catalog() -> None:
    """Inspect and probe the curated PVO catalog."""


@catalog.command("list")
def catalog_list() -> None:
    """Show the bundle's curated PVO catalog."""
    from .commands.catalog import list_catalog
    sys.exit(list_catalog(console=console))


@catalog.command("probe")
@click.option("--pod", required=True, help="Fusion pod URL (e.g. https://<host>.fa.<region>.oraclecloud.com).")
@click.option("--user", "username", default=None, help="HTTP Basic username (else $FUSION_BICC_USER).")
@click.option("--password", default=None, help="HTTP Basic password (else $FUSION_BICC_PASSWORD).")
def catalog_probe(pod: str, username: str | None, password: str | None) -> None:
    """Probe the Fusion BICC console for live PVO names; reconcile against the bundle catalog."""
    from .commands.catalog import probe_catalog
    sys.exit(probe_catalog(pod=pod, username=username, password=password, console=console))


@main.command()
@click.option("--mode", type=click.Choice(["full", "incremental", "seed"]), default="incremental",
              help="seed = first-time full extract; incremental = delta since watermark.")
@click.option("--datasets", default=None, help="Comma-separated dataset ids to run (default: all enabled in bundle.yaml).")
@click.option("--inline", is_flag=True,
              help="Run the orchestrator in-process (only inside an AIDP notebook).")
@click.pass_context
def run(ctx: click.Context, mode: str, datasets: str | None, inline: bool) -> None:
    """Invoke the orchestrator: extract -> bronze -> silver -> gold."""
    from .commands.run import run as run_impl
    sys.exit(run_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        mode=mode,
        datasets=datasets,
        inline=inline,
        console=console,
    ))


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show last-run summary per dataset (reads fusion_bundle_state Delta table)."""
    from .commands.run import status as status_impl
    sys.exit(status_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        console=console,
    ))


# ---------------------------------------------------------------------------
# Dashboard commands (OAC integration)
# ---------------------------------------------------------------------------


@main.group()
def dashboard() -> None:
    """OAC dashboard install/validate via OAC REST API. End-user chat uses OAC MCP."""


@dashboard.command("install")
@click.option("--target", type=click.Choice(["oac"]), default="oac",
              help="Dashboard target system (only OAC is wired today).")
@click.option("--oac-url", required=True, help="OAC instance URL (e.g. https://oac.example.com).")
@click.option("--connection-name", default="aidp_fusion_jdbc",
              help="Name of the OAC connection to create (default: aidp_fusion_jdbc).")
@click.option("--region", default="us-ashburn-1", help="OCI region key.")
@click.option("--user-ocid", required=True, help="OCID of the user that owns the registered API key.")
@click.option("--tenancy-ocid", required=True, help="OCID of the tenancy.")
@click.option("--fingerprint", required=True, help="Public-key fingerprint registered on the user.")
@click.option("--idl-ocid", required=True, help="AIDP DataLake OCID.")
@click.option("--cluster-key", required=True, help="AIDP cluster key (UUID-like).")
@click.option("--catalog", default="default", help="Default JDBC catalog (default: default).")
@click.option("--private-key-pem", required=True,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Path to the private API key PEM file.")
@click.option("--workbooks-dir", default=Path("oac/workbooks"), show_default=True,
              type=click.Path(file_okay=False, path_type=Path),
              help="Directory containing .dva workbook archives to import.")
@click.option("--idcs-url", default=None,
              help="IDCS stripe URL (https://idcs-<stripe>.identity.oraclecloud.com). "
                   "Required unless --print-only.")
@click.option("--client-id", default=None,
              help="IDCS confidential-app client_id (must have authorization_code + "
                   "refresh_token grants enabled). Required unless --print-only.")
@click.option("--client-secret", default=None,
              help="IDCS confidential-app client_secret, or ${vault:OCID}. "
                   "Required unless --print-only.")
@click.option("--oauth-scope", default=None,
              help="Override the auto-derived scope (default: <oac-url>urn:opc:resource:consumer::all "
                   "offline_access). Only set if your IDCS admin published a custom scope.")
@click.option("--auth-flow", type=click.Choice(["auth_code", "device"]), default="auth_code",
              show_default=True,
              help="OAuth flow: auth_code opens browser (laptop), device for headless boxes.")
@click.option("--print-only", is_flag=True,
              help="Skip OAC REST calls; write the connection JSON for manual UI upload.")
@click.option("--skip-workbooks", is_flag=True,
              help="Create the connection but don't import any .dva workbooks.")
def dashboard_install(
    target: str,
    oac_url: str,
    connection_name: str,
    region: str,
    user_ocid: str,
    tenancy_ocid: str,
    fingerprint: str,
    idl_ocid: str,
    cluster_key: str,
    catalog: str,
    private_key_pem: Path,
    workbooks_dir: Path,
    idcs_url: str | None,
    client_id: str | None,
    client_secret: str | None,
    oauth_scope: str | None,
    auth_flow: str,
    print_only: bool,
    skip_workbooks: bool,
) -> None:
    """Register AIDP JDBC data source in OAC + import oac/workbooks/*.dva via OAC REST API.

    Two modes:
      * Default: full REST install. First run opens browser for one-time SSO consent;
        refresh token persists for silent reuse. Requires --idcs-url, --client-id,
        --client-secret. The signed-in user must have BI Service Administrator role on OAC.
      * --print-only: writes the 6-key JSON for manual UI upload (no IDCS app needed).
    """
    from .oac.install import InstallParams, install
    from .oac.rest import derive_oac_scope, discover_oac_audience
    from .utils import vault

    resolved_secret = vault.resolve(client_secret) if client_secret else None
    if oauth_scope:
        resolved_scope = oauth_scope
    else:
        # Auto-discover the IDCS audience prefix (different host from oac_url)
        try:
            audience = discover_oac_audience(oac_url)
            resolved_scope = derive_oac_scope(oac_url, audience=audience)
        except Exception as exc:
            console.print(f"[yellow]audience discovery failed ({exc}); falling back to oac_url[/yellow]")
            resolved_scope = derive_oac_scope(oac_url)

    params = InstallParams(
        oac_url=oac_url,
        connection_name=connection_name,
        region=region,
        user_ocid=user_ocid,
        tenancy_ocid=tenancy_ocid,
        fingerprint=fingerprint,
        idl_ocid=idl_ocid,
        cluster_key=cluster_key,
        catalog=catalog,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=resolved_secret,
        oauth_scope=resolved_scope,
        auth_flow=auth_flow,
        private_key_pem_path=private_key_pem,
        workbooks_dir=workbooks_dir,
        print_only=print_only,
        skip_workbooks=skip_workbooks,
    )
    try:
        result = install(params, console=console)
    except Exception as exc:
        console.print(f"[red]install failed:[/red] {exc}")
        sys.exit(1)

    if result.imported_workbooks:
        console.print(
            f"\n[bold green]Done.[/bold green] Connection: [bold]{connection_name}[/bold] "
            f"({result.connection_id}); workbooks: {', '.join(result.imported_workbooks)}"
        )
    elif result.connection_id:
        console.print(
            f"\n[bold green]Done.[/bold green] Connection: [bold]{connection_name}[/bold] "
            f"({result.connection_id}). No workbooks imported."
        )


@dashboard.command("validate")
@click.option("--target", type=click.Choice(["oac"]), default="oac")
@click.option("--oac-url", required=True)
@click.option("--connection-name", default="aidp_fusion_jdbc")
@click.option("--idcs-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True,
              help="IDCS confidential-app client_secret, or ${vault:OCID}.")
@click.option("--oauth-scope", default=None,
              help="Override auto-derived scope (default: <oac-url>urn:opc:resource:consumer::all offline_access).")
@click.option("--workbooks", default="", help="Comma-separated workbook names (default: probe none).")
def dashboard_validate(
    target: str,
    oac_url: str,
    connection_name: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    oauth_scope: str,
    workbooks: str,
) -> None:
    """Probe OAC: confirm connection + workbooks exist (read-only)."""
    from .oac.validate import ValidateParams, validate
    from .utils import vault

    workbook_names = [w.strip() for w in workbooks.split(",") if w.strip()]
    params = ValidateParams(
        oac_url=oac_url,
        connection_name=connection_name,
        workbook_names=workbook_names,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=vault.resolve(client_secret),
        oauth_scope=oauth_scope or "",
    )
    result = validate(params, console=console)
    sys.exit(0 if result.all_ok else 1)


@dashboard.command("uninstall")
@click.option("--target", type=click.Choice(["oac"]), default="oac")
@click.option("--oac-url", required=True)
@click.option("--connection-name", default="aidp_fusion_jdbc")
@click.option("--idcs-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True)
@click.option("--oauth-scope", default=None,
              help="Override auto-derived scope (default: <oac-url>urn:opc:resource:consumer::all offline_access).")
@click.option("--workbooks", default="",
              help="Comma-separated workbook names to remove (default: only delete connection).")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
def dashboard_uninstall(
    target: str,
    oac_url: str,
    connection_name: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    oauth_scope: str,
    workbooks: str,
    yes: bool,
) -> None:
    """Remove imported OAC workbooks + the bundle's data source."""
    from .oac.uninstall import UninstallParams, uninstall
    from .utils import vault

    if not yes:
        click.confirm(
            f"Remove connection '{connection_name}' + workbooks from {oac_url}?",
            abort=True,
        )
    workbook_names = [w.strip() for w in workbooks.split(",") if w.strip()]
    params = UninstallParams(
        oac_url=oac_url,
        connection_name=connection_name,
        workbook_names=workbook_names,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=vault.resolve(client_secret),
        oauth_scope=oauth_scope or "",
    )
    result = uninstall(params, console=console)
    console.print(
        f"\n[bold]Removed:[/bold] "
        f"connection={result.connection_deleted}, "
        f"workbooks={result.deleted_workbooks}"
    )


@dashboard.command("mcp-config")
@click.option("--oac-url", required=True, help="OAC instance URL.")
@click.option("--oac-mcp-connect-js", required=True, type=click.Path(exists=True, path_type=Path),
              help="Local path to oac-mcp-connect.js (extract from oac-mcp-connect.zip — get from OAC Profile -> MCP Connect tab).")
def dashboard_mcp_config(oac_url: str, oac_mcp_connect_js: Path) -> None:
    """Print the JSON snippet to add to claude_desktop_config.json (or Claude Code / Cline / Copilot)."""
    import json
    snippet = {
        "mcpServers": {
            "oac-mcp-server": {
                "command": "node",
                "args": [str(oac_mcp_connect_js.resolve())],
                "env": {
                    "OAC_INSTANCE_URL": oac_url
                }
            }
        }
    }
    console.print("[bold]Paste into your AI client's MCP config:[/bold]\n")
    console.print(json.dumps(snippet, indent=2))
    console.print(
        "\n[dim]Note: this is a starter template; the canonical JSON is the one OAC's "
        "Profile -> MCP Connect tab generates. See:[/dim]\n"
        "  https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/add-oracle-analytics-cloud-mcp-server-your-ai-client-preview.html"
    )


if __name__ == "__main__":
    main()
