"""CLI entry point for `aidp-fusion-autopilot`.

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
from dotenv import load_dotenv
from rich.console import Console

from . import __version__


def _force_utf8_output() -> None:
    """Reconfigure stdout/stderr to UTF-8 so rich status glyphs (``✓``, ``→``)
    don't crash on a non-UTF-8 console (e.g. Windows ``cp1252``).

    Safe no-op when the stream is already UTF-8 or doesn't support
    ``reconfigure`` (e.g. a captured pipe wrapper without the method).
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            current = (getattr(stream, "encoding", "") or "").lower().replace("-", "")
            if stream is not None and current != "utf8":
                stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError, OSError):
            pass


_force_utf8_output()

console = Console()


def _load_cwd_dotenv() -> None:
    """Load `.env` from the directory where the operator ran the CLI.

    `load_dotenv()` without an explicit path searches relative to the Python
    module in some execution modes, which can miss a customer bundle's `.env`
    when the package is installed from another checkout.
    """
    env_path = Path.cwd() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)


# ---------------------------------------------------------------------------
# Top-level group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(__version__, prog_name="aidp-fusion-autopilot")
@click.option("--bundle", "bundle_path", type=click.Path(path_type=Path), default=Path("bundle.yaml"),
              help="Path to bundle.yaml (default: ./bundle.yaml).")
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=Path("aidp.config.yaml"),
              help="Path to aidp.config.yaml (default: ./aidp.config.yaml).")
@click.option("--env", "env_name", default="dev", help="Environment name from aidp.config.yaml (default: dev).")
@click.pass_context
def main(ctx: click.Context, bundle_path: Path, config_path: Path, env_name: str) -> None:
    """Productized Fusion -> AIDP pipeline (BICC + Delta + OAC)."""
    _load_cwd_dotenv()
    ctx.ensure_object(dict)
    ctx.obj["bundle_path"] = bundle_path
    ctx.obj["config_path"] = config_path
    ctx.obj["env_name"] = env_name


# ---------------------------------------------------------------------------
# Orchestration commands
# ---------------------------------------------------------------------------


@main.command()
@click.option(
    "--template",
    type=click.Choice(["full-finance-starter", "minimal-bundle", "minimal", "full-finance"]),
    default="full-finance-starter",
    help="Which example to scaffold (default: full-finance-starter).",
)
@click.option("--force", is_flag=True, help="Overwrite existing bundle.yaml / aidp.config.yaml.")
def init(template: str, force: bool) -> None:
    """Scaffold a bundle.yaml + aidp.config.yaml in the current directory."""
    from .commands.init import init as init_impl
    sys.exit(init_impl(template=template, force=force, console=console))


@main.command("init-config")
@click.option("--aidp-id", "aidp_id", required=True,
              help="AIDP / DataLake OCID (the one root id — copy once from the AIDP console URL).")
@click.option("--workspace", "workspace_name", required=True,
              help="Workspace DISPLAY NAME (resolved to workspaceKey via the AIDP REST API).")
@click.option("--cluster", "cluster_name", required=True,
              help="Cluster DISPLAY NAME (resolved to clusterKey + live state).")
@click.option("--region", default="us-ashburn-1", show_default=True, help="OCI region key.")
@click.option("--oci-profile", "oci_profile", default="DEFAULT", show_default=True,
              help="~/.oci/config profile used to sign the discovery calls.")
@click.option("--project", default=None,
              help="Project name for aidp.config.yaml (default: read from bundle.yaml, else 'fusion-autopilot').")
@click.option("--catalog", default=None,
              help="Catalog name — NOT written here (it lives in bundle.yaml aidp.catalog); "
                   "pass it only to get a reminder of where to set it.")
@click.option("--bicc-secret-name", "bicc_secret_name", default="fusion_bicc_password",
              show_default=True, help="AIDP credential-store entry name for the BICC password.")
@click.option("--force", is_flag=True, help="Overwrite the env block if it already exists.")
@click.option("--dry-run", "dry_run", is_flag=True,
              help="Resolve names → keys and print the resulting config; write nothing.")
@click.pass_context
def init_config(
    ctx: click.Context,
    aidp_id: str,
    workspace_name: str,
    cluster_name: str,
    region: str,
    oci_profile: str,
    project: str | None,
    catalog: str | None,
    bicc_secret_name: str,
    force: bool,
    dry_run: bool,
) -> None:
    """Resolve workspace/cluster NAMES into keys and write aidp.config.yaml.

    Saves the operator from hand-copying workspaceKey / clusterKey: give the
    AIDP/DataLake OCID once plus the workspace + cluster display names, and the
    command resolves the rest via the AIDP REST API (OCI-signed) and writes the
    named environment block into aidp.config.yaml.
    """
    from .commands.init_config import init_config as init_config_impl
    sys.exit(init_config_impl(
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        aidp_id=aidp_id,
        workspace_name=workspace_name,
        cluster_name=cluster_name,
        region=region,
        oci_profile=oci_profile,
        project=project,
        catalog=catalog,
        bicc_secret_name=bicc_secret_name,
        bundle_path=ctx.obj["bundle_path"],
        force=force,
        dry_run=dry_run,
        console=console,
    ))


@main.command("use-pack")
@click.argument("pack")
@click.option("--profile", required=True, help="Tenant profile name (contentPack.profile).")
@click.option("--align/--no-align", default=True, show_default=True,
              help="Align dimensions.build / gold.marts to the resolved pack's nodes.")
@click.option("--fix-credentials/--no-fix-credentials", default=True, show_default=True,
              help="Rewrite a placeholder-vault fusion.password to ${FUSION_BICC_PASSWORD}.")
@click.pass_context
def use_pack(ctx: click.Context, pack: str, profile: str, align: bool, fix_credentials: bool) -> None:
    """Wire bundle.yaml to a content pack / overlay (contentPack + marts + creds) in one step."""
    from .commands.use_pack import use_pack as use_pack_impl
    sys.exit(use_pack_impl(
        bundle_path=ctx.obj["bundle_path"],
        pack_spec=pack,
        profile=profile,
        align=align,
        fix_credentials=fix_credentials,
        console=console,
    ))


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
@click.option(
    "--refresh", is_flag=True,
    help="Re-walk every variation point against the live bronze; resolves pinned-profile drift.",
)
@click.option(
    "--operator", "operator", type=str, default=None,
    help="Explicit operator identity for the SOX-floor audit trail (overrides $AIDP_OPERATOR / $USER).",
)
@click.option(
    "--non-interactive", is_flag=True,
    help="Sandbox/CI mode: multi-match auto-picks the first candidate; refuses --refresh changes to pinned values.",
)
@click.option(
    "--resolutions", "resolutions_path", type=click.Path(path_type=Path, exists=True),
    default=None,
    help="JSON file scripting multi-match resolutions (feature #3 / CI use).",
)
@click.option(
    "--accept-coa-convention", is_flag=True,
    help=(
        "Accept the pack's conventional COA role->segment default as a deliberate "
        "convention (records `defaulted_convention`). Required for a non-interactive "
        "run with no explicit profile.chartOfAccounts; otherwise the COA semantic-role "
        "resolver fails closed (AIDPF-2013). Also upgrades a legacy back-derived pin "
        "from `legacy_unverified` to operator-confirmed."
    ),
)
@click.option(
    "--accept-singleton-coa", is_flag=True,
    help=(
        "Assert all active charts of accounts share the COA role->segment layout. "
        "Persists profile.chartOfAccounts.singletonAccepted so the multi-COA preflight "
        "gate (AIDPF-2018) passes for a singleton mapping. Use only when every chart "
        "genuinely uses the same segment positions."
    ),
)
@click.option(
    "--skip-preonboarding-probes", is_flag=True,
    help=(
        "Skip BICC / AIDP pre-onboarding probes; useful for --refresh after initial "
        "onboarding succeeded. INCOMPATIBLE with --dispatch-mode=cluster "
        "(the aidp-rest probe is load-bearing in cluster mode; "
        "conflicting use raises AIDPF-2047)."
    ),
)
@click.option(
    "--dispatch-mode",
    "dispatch_mode",
    type=click.Choice(["cluster", "local"]),
    default="cluster",
    show_default=True,
    help=(
        "Where the variation-phase bronze probe runs. 'cluster' "
        "(default) dispatches a notebook to the AIDP cluster "
        "where 3-part-namespace DESCRIBE works natively. 'local' uses "
        "the laptop's in-process Spark session — backward-compat for "
        "unit tests and laptop-POC bundles."
    ),
)
@click.option(
    "--cluster-key", "cluster_key", type=str, default=None,
    help=(
        "Cluster UUID for cluster-mode dispatch; overrides "
        "EnvSpec.clusterKey. Env var: AIDP_FUSION_CLUSTER_KEY."
    ),
)
@click.option(
    "--cluster-name", "cluster_name", type=str, default=None,
    help=(
        "Cluster display name for cluster-mode dispatch; overrides "
        "EnvSpec.clusterName. Env var: AIDP_FUSION_CLUSTER_NAME."
    ),
)
@click.option(
    "--workspace-dir", "workspace_dir", type=str, default=None,
    help=(
        "Server-side notebook upload root for cluster-mode dispatch; "
        "overrides Defaults.workspaceDir. Env var: "
        "AIDP_FUSION_WORKSPACE_DIR. When unset (and not in EnvSpec / "
        "Defaults), derives '/Workspace/{workspace_root}/fusion-autopilot-bootstrap'."
    ),
)
@click.pass_context
def bootstrap(
    ctx: click.Context,
    check_iam: bool,
    refresh: bool,
    operator: str | None,
    non_interactive: bool,
    accept_coa_convention: bool,
    accept_singleton_coa: bool,
    resolutions_path: Path | None,
    skip_preonboarding_probes: bool,
    dispatch_mode: str,
    cluster_key: str | None,
    cluster_name: str | None,
    workspace_dir: str | None,
) -> None:
    """Probe all prerequisites + run the variation-resolution phase when content-pack-enabled."""
    from .commands.bootstrap import bootstrap as bootstrap_impl
    sys.exit(bootstrap_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        check_iam=check_iam,
        console=console,
        refresh=refresh,
        operator=operator,
        non_interactive=non_interactive,
        accept_coa_convention=accept_coa_convention,
        accept_singleton_coa=accept_singleton_coa,
        resolutions_path=resolutions_path,
        skip_preonboarding_probes=skip_preonboarding_probes,
        dispatch_mode=dispatch_mode,
        cluster_key_override=cluster_key,
        cluster_name_override=cluster_name,
        workspace_dir_override=workspace_dir,
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


@catalog.command("probe-pvo")
@click.argument("dataset_id")
@click.option(
    "--datastore", required=True,
    help="BICC datastore identifier (e.g. SupplierExtractPVO).",
)
@click.option(
    "--bicc-schema", required=True,
    help="BICC offering schema (Financial / HCM / SCM).",
)
@click.option(
    "--pvo-id", default=None,
    help="Optional full AM-hierarchy path "
         "(e.g. FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO). "
         "Used for WARN cross-reference against the curated catalog.",
)
@click.option(
    "--incremental-capable/--no-incremental-capable", default=True,
    help="Whether fusion.initial.extract-date is meaningful for this PVO "
         "(default: True). Set --no-incremental-capable for snapshot-style "
         "PVOs (gl_period_balances, gl_coa) where LastUpdateDate doesn't "
         "track meaningful change events monotonically.",
)
@click.option(
    "--emit-pack-yaml", required=True,
    help="Path to write the draft YAML to "
         "(typically content_packs/<overlay-pack>/bronze/<id>.yaml).",
)
@click.pass_context
def catalog_probe_pvo(
    ctx: click.Context,
    dataset_id: str,
    datastore: str,
    bicc_schema: str,
    pvo_id: str | None,
    incremental_capable: bool,
    emit_pack_yaml: str,
) -> None:
    """Probe a BICC PVO and emit a draft content-pack bronze YAML.

    Runs a metadata-only ``extract_pvo().schema`` roundtrip (no row pull),
    translates the discovered StructType to outputSchema.columns, and
    writes a draft YAML with commented-out refresh.incremental TODOs.

    Operator must review the generated YAML before production use:
    fill in naturalKey, watermark.column, requiredColumns, and pii
    classifications.
    """
    from .commands.catalog import probe_pvo_emit_pack_yaml
    sys.exit(probe_pvo_emit_pack_yaml(
        dataset_id=dataset_id,
        datastore=datastore,
        bicc_schema=bicc_schema,
        pvo_id=pvo_id,
        incremental_capable=incremental_capable,
        emit_pack_yaml=emit_pack_yaml,
        bundle_path=ctx.obj.get("bundle_path") if ctx.obj else None,
        config_path=ctx.obj.get("config_path") if ctx.obj else None,
        env_name=ctx.obj.get("env_name", "dev") if ctx.obj else "dev",
        console=console,
    ))


@main.command()
@click.option(
    "--mode", type=click.Choice(["seed", "incremental"]), default=None,
    help="seed = full BICC pull + replace strategy per layer; incremental "
         "= delta-merge using prior watermarks from fusion_autopilot_state "
         "(bronze MERGE on natural key + payload diff for "
         "incremental_capable=False PVOs; silver/gold MERGE on the "
         "primary source's row-max watermark). The retired alias 'full' "
         "is now 'seed'. Omitted on a fresh run → seed (unchanged default); "
         "omitted on --resume → adopt the resumed run's mode (never silently "
         "flip seed<->incremental)."
)
@click.option("--datasets", default=None, help="Comma-separated dataset/dim/mart names to filter (default: all in bundle.yaml).")
@click.option(
    "--layers", default=None,
    help="Comma-separated layer names to filter (bronze, silver, gold). "
         "Mutually compatible with --datasets — both apply. "
         "Useful for scoped runs such as --inline --layers gold.",
)
@click.option("--inline", is_flag=True,
              help="Run the orchestrator in-process (architectural primary — needs Spark + checkpointer + vault from an AIDP notebook session).")
@click.option(
    "--resume", "resume_run_id", default=None,
    help="Resume an interrupted run by its run_id. Skips datasets whose latest "
         "terminal status under this run_id is 'success' or 'resumed_skipped'; "
         "re-attempts the rest under the ORIGINAL run_id (preserves the "
         "medallion _run_id audit invariant). Scope is reconstructed from the "
         "stored plan_snapshot when --datasets/--layers are omitted. Drift "
         "(plan shape, effective schemas, fusion pod/storage/user, AIDP target "
         "paths, plugin version) raises ResumeBundleMismatchError pre-dispatch. "
         "Supported on both --inline and REST dispatch.",
)
@click.option(
    "--dry-run", "dry_run", is_flag=True, default=False,
    help="Resolve the plan + run preflight, then exit 0 without dispatching. "
         "On --inline: returns the orchestrator's dry-run summary. On REST "
         "dispatch: runs local and remote preflight (bundle, config, OCI, AIDP "
         "plane, cluster state) and returns an empty RunSummary. No wheel "
         "build, no notebook upload, no job submission.",
)
@click.option(
    "--poll-timeout", "poll_timeout_s",
    type=click.IntRange(60, 14400),
    default=3600,
    show_default=True,
    help="Seconds to wait for the dispatched cluster job to reach a terminal "
         "status before raising DISPATCH_TIMEOUT. Default 3600 (1h) covers "
         "cold-cache BICC extracts on slow tenants. Bump to 14400 (4h) for "
         "first-time seed runs against especially slow Fusion pods. Below 60s "
         "is rejected at parse time. Only meaningful for REST dispatch "
         "(no --inline).",
)
@click.option(
    "--force-fingerprint-skip", "force_fingerprint_skip",
    is_flag=True,
    default=False,
    hidden=True,
    help="Dev/sandbox: bypass the bronze-schema fingerprint "
         "drift gate. Records an audit warn row in fusion_autopilot_state "
         "with mode='fingerprint_skip'. Production runs MUST NOT use "
         "this; SOX-audit environments should policy-disable.",
)
@click.option(
    "--repin-plan-hash", "repin_plan_hash",
    is_flag=True,
    default=False,
    hidden=True,
    help="Dev/sandbox: bypass the AIDPF-4040 plan-hash continuity gate on "
         "incremental. When a node's plan-hash diverged because you EDITED "
         "the SQL / profile / adapter on purpose, this repins the new hash "
         "(records an audit row in fusion_autopilot_state with "
         "mode='plan_hash_repin') and proceeds instead of forcing a full "
         "re-seed. Production runs MUST NOT use this; SOX-audit environments "
         "should policy-disable. Does NOT mask a real, unintended drift — "
         "use only when the change was deliberate.",
)
@click.option(
    "--strict-scope/--no-strict-scope", "strict_scope", default=None,
    help="Disable implicit transitive include. When set, "
         "every declared root's `dependsOn` must ALSO appear in "
         "`--datasets` / `bundle.datasets[]` explicitly; missing deps "
         "raise AIDPF-1042 STRICT_SCOPE_MISSING_DEPENDENCY. Use for "
         "debug-style runs where exact control over the plan is "
         "required (e.g. re-run only `dim_supplier` against pre-staged "
         "bronze). Tri-state: omitted → False on a fresh run, or adopt the "
         "resumed run's manifest value; an explicit value that conflicts with "
         "the manifest raises AIDPF-1047.",
)
@click.pass_context
def run(ctx: click.Context, mode: str | None, datasets: str | None, layers: str | None,
        inline: bool, resume_run_id: str | None, dry_run: bool,
        poll_timeout_s: int,
        force_fingerprint_skip: bool,
        repin_plan_hash: bool,
        strict_scope: bool | None) -> None:
    """Invoke the orchestrator: extract -> bronze -> silver -> gold."""
    from .commands.run import run as run_impl
    sys.exit(run_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        mode=mode,
        datasets=datasets,
        layers=layers,
        inline=inline,
        resume_run_id=resume_run_id,
        dry_run=dry_run,
        poll_timeout_s=poll_timeout_s,
        force_fingerprint_skip=force_fingerprint_skip,
        repin_plan_hash=repin_plan_hash,
        strict_scope=strict_scope,
        console=console,
    ))


@main.command("migrate-bundle")
@click.option("--from", "from_version", required=True, help="Source schema version (e.g. 0.1.0).")
@click.option("--to", "to_version", required=True, help="Target schema version (e.g. 0.2.0).")
@click.pass_context
def migrate_bundle(ctx: click.Context, from_version: str, to_version: str) -> None:
    """Migrate bundle.yaml from one schema version to another.

    Today only v0.2.0 exists, so any non-no-op invocation exits 2 with a
    "no migration path" message. The verb is present so future breaking schema
    changes can ship without making callers update their scripts.
    """
    from .commands.migrate_bundle import migrate_bundle as migrate_impl
    sys.exit(migrate_impl(
        bundle_path=ctx.obj["bundle_path"],
        from_version=from_version,
        to_version=to_version,
        console=console,
    ))


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show last-run summary per dataset (reads fusion_autopilot_state Delta table)."""
    from .commands.run import status as status_impl
    sys.exit(status_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        console=console,
    ))


# ---------------------------------------------------------------------------
# Content pack commands (v2 — schema validation + introspection)
# ---------------------------------------------------------------------------


@main.group("content-pack")
def content_pack() -> None:
    """Inspect and validate content packs (v2 schema layer)."""


@content_pack.command("list")
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
def content_pack_list(json_output: bool) -> None:
    """List installed content packs."""
    from .commands.content_pack import list_packs

    sys.exit(list_packs(json_output=json_output, console=console))


@content_pack.command("info")
@click.argument("name")
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
def content_pack_info(name: str, json_output: bool) -> None:
    """Show detailed info about an installed pack (or a pack by path)."""
    from .commands.content_pack import info_pack

    sys.exit(info_pack(name, json_output=json_output, console=console))


@content_pack.command("validate")
@click.argument("name")
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
@click.option(
    "--profile",
    default=None,
    help="Tenant profile (path to a profile YAML, or a bare name resolved "
    "against ./profiles/<name>.yaml). Enables the profile-aware leg of the "
    "column-contract gate (AIDPF-2045) so $column.*/$coa.* consumer demands "
    "are checked. Omit for profile-less validation (literal demands only).",
)
def content_pack_validate(name: str, json_output: bool, profile: str | None) -> None:
    """Validate a content pack against the schema + content validators."""
    from .commands.content_pack import validate_pack_cli

    sys.exit(
        validate_pack_cli(
            name, json_output=json_output, console=console, profile=profile
        )
    )


@content_pack.command("refresh-fork")
@click.argument("name")
@click.option(
    "--node",
    default=None,
    help="Restrict to a single `<layer>/<id>` replaceNode override (e.g. "
    "`silver/dim_account`). Omit to re-stamp every replaceNode in the overlay.",
)
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
def content_pack_refresh_fork(name: str, node: str | None, json_output: bool) -> None:
    """Re-stamp `replaceNode.forkedFrom` fingerprints from the current base.

    The operator's exit from an `AIDPF-2064` (fork-base drift) failure: after
    re-reconciling a replaceNode overlay against an updated base mart, this
    recomputes and rewrites all three `forkedFrom` stamps (sqlSha256,
    contractSha256, packVersion).
    """
    from .commands.content_pack import refresh_fork_cli

    sys.exit(
        refresh_fork_cli(name, node=node, json_output=json_output, console=console)
    )


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
@click.option("--bar-bucket", default=None,
              help="OCI Object Storage bucket containing the bundle's .bar snapshot. "
                   "Customer-uploaded; the bundle ships only the connection + IAM glue. "
                   "Omit to skip workbook restore (connection-only install).")
@click.option("--bar-uri", default=None,
              help="Object name (relative path) of the .bar in --bar-bucket.")
@click.option("--bar-password", default=None,
              help="BAR password (if the .bar was created with password protection).")
@click.option("--snapshot-name", default="aidp-fusion-autopilot",
              help="Display name for the registered snapshot (default: aidp-fusion-autopilot).")
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
              help="Override the auto-derived scope (default: <audience>urn:opc:resource:consumer::all "
                   "offline_access). Only set if your IDCS admin published a custom scope.")
@click.option("--auth-flow", type=click.Choice(["auth_code", "device"]), default="auth_code",
              show_default=True,
              help="OAuth flow: auth_code opens browser (laptop), device for headless boxes.")
@click.option("--prompt-login", is_flag=True,
              help="Force IDCS to reprompt for credentials (don't reuse cached SSO session).")
@click.option("--print-only", is_flag=True,
              help="Skip OAC REST calls; write the connection JSON for manual UI upload.")
@click.option("--skip-workbooks", is_flag=True,
              help="Create the connection but don't restore the snapshot (workbooks).")
@click.option("--overwrite-connection", is_flag=True,
              help="Delete + recreate the connection if it already exists (default: skip).")
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
    bar_bucket: str | None,
    bar_uri: str | None,
    bar_password: str | None,
    snapshot_name: str,
    idcs_url: str | None,
    client_id: str | None,
    client_secret: str | None,
    oauth_scope: str | None,
    auth_flow: str,
    prompt_login: bool,
    print_only: bool,
    skip_workbooks: bool,
    overwrite_connection: bool,
) -> None:
    """Register AIDP JDBC connection in OAC + restore the workbook snapshot via REST.

    Architecture — Oracle-documented endpoints only:
      1. POST /catalog/connections                       (creates AIDP connection)
      2. POST /snapshots                                 (registers customer-uploaded .bar)
      3. POST /system/actions/restoreSnapshot            (async restore)
      4. GET  /workRequests/{id}                         (polls until SUCCEEDED)

    Two modes:
      * Default: full REST install. First run opens browser for one-time SSO consent;
        refresh token persists for silent reuse. The signed-in user must hold the
        BI Service Administrator role on OAC.
      * --print-only: writes the 6-key JSON for manual UI upload (no IDCS app needed).
    """
    from .oac.install import InstallParams, install
    from .oac.rest import derive_oac_scope, discover_oac_audience
    from .utils import vault

    resolved_secret = vault.resolve(client_secret) if client_secret else None
    if oauth_scope:
        resolved_scope = oauth_scope
    else:
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
        prompt_login=prompt_login,
        private_key_pem_path=private_key_pem,
        bar_bucket=bar_bucket,
        bar_uri=bar_uri,
        bar_password=bar_password,
        snapshot_name=snapshot_name,
        print_only=print_only,
        skip_workbooks=skip_workbooks,
        overwrite_connection=overwrite_connection,
    )
    try:
        result = install(params, console=console)
    except Exception as exc:
        console.print(f"[red]install failed:[/red] {exc}")
        sys.exit(1)

    # Summary
    parts: list[str] = []
    if result.connection_id:
        parts.append(f"connection={connection_name} (id={result.connection_id})")
    if result.snapshot_id:
        parts.append(f"snapshot={result.snapshot_id} (status={result.work_request_status})")
    if result.json_template_path:
        parts.append(f"json={result.json_template_path}")
    if parts:
        console.print(f"\n[bold green]Done.[/bold green] " + " | ".join(parts))


@dashboard.command("validate")
@click.option("--target", type=click.Choice(["oac"]), default="oac")
@click.option("--oac-url", required=True)
@click.option("--connection-name", default="aidp_fusion_jdbc")
@click.option("--idcs-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True,
              help="IDCS confidential-app client_secret, or ${vault:OCID}.")
@click.option("--oauth-scope", default=None,
              help="Override auto-derived scope.")
@click.option("--snapshot-name", default=None,
              help="Snapshot display name to verify is registered (default: probe none).")
def dashboard_validate(
    target: str,
    oac_url: str,
    connection_name: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    oauth_scope: str | None,
    snapshot_name: str | None,
) -> None:
    """Probe OAC: confirm connection (and optionally a snapshot) is present (read-only)."""
    from .oac.validate import ValidateParams, validate
    from .utils import vault

    params = ValidateParams(
        oac_url=oac_url,
        connection_name=connection_name,
        snapshot_name=snapshot_name,
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
@click.option("--oauth-scope", default=None, help="Override auto-derived scope.")
@click.option("--snapshot-id", default=None,
              help="Snapshot ID to deregister (omit to skip).")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
def dashboard_uninstall(
    target: str,
    oac_url: str,
    connection_name: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    oauth_scope: str | None,
    snapshot_id: str | None,
    yes: bool,
) -> None:
    """Remove the bundle's connection (and optionally deregister its snapshot).

    Note: workbook restored content cannot be selectively deleted via the public
    REST API — to fully roll back, restore an earlier snapshot of the OAC instance.
    Or use Console -> Catalog to delete the /shared/AIDP_Fusion_Bundle/ folder.
    """
    from .oac.uninstall import UninstallParams, uninstall
    from .utils import vault

    if not yes:
        click.confirm(
            f"Remove connection '{connection_name}'"
            + (f" + deregister snapshot {snapshot_id}" if snapshot_id else "")
            + f" from {oac_url}?",
            abort=True,
        )
    params = UninstallParams(
        oac_url=oac_url,
        connection_name=connection_name,
        snapshot_id=snapshot_id,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=vault.resolve(client_secret),
        oauth_scope=oauth_scope or "",
    )
    result = uninstall(params, console=console)
    console.print(
        f"\n[bold]Removed:[/bold] "
        f"connection={result.connection_deleted}, "
        f"snapshot={result.snapshot_deleted}"
    )


@dashboard.command("mcp-config")
@click.option("--oac-url", required=True, help="OAC instance URL.")
@click.option("--oac-mcp-connect-js", required=True, type=click.Path(exists=True, path_type=Path),
              help="Local path to oac-mcp-connect.js (extract from oac-mcp-connect.zip — get from OAC Profile -> MCP Connect tab).")
def dashboard_mcp_config(oac_url: str, oac_mcp_connect_js: Path) -> None:
    """Print the MCP server JSON for Claude Code (project .mcp.json) / Claude Desktop / Cline / Copilot."""
    import json
    # The connector takes the OAC URL as a POSITIONAL argument and reads no env vars
    # (verified against oac-mcp-connect 1.4: `grep process.env` is empty, `--help` shows `<url>`).
    # An `env: {OAC_INSTANCE_URL: ...}` block does NOT work — the connector ignores it.
    snippet = {
        "mcpServers": {
            "oac-mcp-server": {
                "command": "node",
                "args": [str(oac_mcp_connect_js.resolve()), oac_url],
            }
        }
    }
    console.print("[bold]MCP server config (browser auth):[/bold]\n")
    console.print(json.dumps(snippet, indent=2))
    console.print(
        "\n[dim]Where to put it:[/dim]\n"
        "  • Claude Code (project-scoped): merge under [bold]mcpServers[/bold] in [bold].mcp.json[/bold] at your repo root\n"
        "    (the bundle ships one driven by ${OAC_URL} / ${OAC_MCP_CONNECT_PATH} — just set those env vars instead).\n"
        "  • Claude Desktop: ~/Library/Application Support/Claude/claude_desktop_config.json\n"
        "  • Cline / Copilot: their respective MCP settings file.\n"
        "[dim]For token auth, append the token-file path as a third arg. Canonical JSON: OAC Profile -> MCP Connect tab.[/dim]"
    )


@dashboard.command("mcp-token")
@click.option("--oac-url", required=True, help="OAC instance URL (base, e.g. https://oac.example.com).")
@click.option("--import-from", "import_from", default=None,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Place an existing OAC-downloaded tokens.json (Profile -> Access Tokens) as the "
                   "connector token file. No OAuth client needed. Mutually exclusive with minting.")
@click.option("--idcs-url", default=None,
              help="IDCS stripe URL. Required when minting (omit only with --import-from).")
@click.option("--client-id", default=None,
              help="IDCS confidential-app client_id. Required when minting.")
@click.option("--client-secret", default=None,
              help="IDCS confidential-app client_secret, or ${vault:OCID}. Required when minting.")
@click.option("--oauth-scope", default=None, help="Override the auto-derived OAC scope.")
@click.option("--flow", type=click.Choice(["auth_code", "device"]), default="auth_code",
              show_default=True, help="OAuth flow for minting: auth_code (browser) or device (headless).")
@click.option("--prompt-login", is_flag=True, help="Force IDCS to reprompt for credentials.")
@click.option("--token-file", default=None, type=click.Path(dir_okay=False, path_type=Path),
              help="Where to write the connector token file (default: ~/.oac-connect/token.json).")
@click.option("--mcp-json", default=Path(".mcp.json"), type=click.Path(dir_okay=False, path_type=Path),
              show_default=True, help="Path to the .mcp.json to wire the token file into.")
@click.option("--no-wire", is_flag=True, help="Write the token file only; don't touch .mcp.json.")
def dashboard_mcp_token(
    oac_url: str,
    import_from: Path | None,
    idcs_url: str | None,
    client_id: str | None,
    client_secret: str | None,
    oauth_scope: str | None,
    flow: str,
    prompt_login: bool,
    token_file: Path | None,
    mcp_json: Path,
    no_wire: bool,
) -> None:
    """Produce a token file the OAC MCP connector can use non-interactively, and wire .mcp.json.

    The connector falls back to interactive browser auth otherwise, which cannot
    complete inside the Claude Code MCP client (elicitation unsupported). This
    command seeds a Bearer token so tool calls authenticate silently.

    Two modes:
      * --import-from <tokens.json>: reuse an OAC Profile -> Access Tokens download
        (already connector-format). No OAuth client needed.
      * mint (default): run the IDCS Auth-Code+PKCE/device flow (needs --idcs-url +
        --client-id + --client-secret); a refresh token persists for silent reuse.
    """
    from .oac.mcp_token import (
        DEFAULT_CONNECTOR_TOKEN_FILE,
        import_connector_token,
        mint_connector_token,
        wire_mcp_json_file,
    )
    from .utils import vault

    out_path = token_file or DEFAULT_CONNECTOR_TOKEN_FILE

    try:
        if import_from is not None:
            written, payload = import_connector_token(import_from, token_file=out_path)
        else:
            missing = [n for n, v in
                       (("--idcs-url", idcs_url), ("--client-id", client_id),
                        ("--client-secret", client_secret)) if not v]
            if missing:
                console.print(
                    f"[red]minting requires {', '.join(missing)}[/red] "
                    f"(or use --import-from to reuse an OAC token download)."
                )
                sys.exit(2)
            written, payload = mint_connector_token(
                oac_url=oac_url,
                idcs_url=idcs_url,  # type: ignore[arg-type]
                client_id=client_id,  # type: ignore[arg-type]
                client_secret=vault.resolve(client_secret),  # type: ignore[arg-type]
                token_file=out_path,
                scope=oauth_scope,
                flow=flow,
                prompt_login=prompt_login,
            )
    except Exception as exc:  # noqa: BLE001 — surface a clean CLI error
        console.print(f"[red]mcp-token failed:[/red] {exc}")
        sys.exit(1)

    # Mask the token in all output — never echo the secret.
    tok = str(payload.get("accessToken", ""))
    masked = (tok[:6] + "…" + tok[-4:]) if len(tok) > 12 else "<short>"
    console.print(
        f"[green]Wrote connector token file:[/green] {written} "
        f"(accessToken={masked}, expiresIn={payload.get('expiresIn')}s, "
        f"refreshToken={'present' if payload.get('refreshToken') else 'none'})"
    )

    if no_wire:
        console.print(
            f"[dim]Skipped .mcp.json wiring (--no-wire). To wire manually, add "
            f'"{written}" as the 3rd connector arg in {mcp_json}.[/dim]'
        )
        return
    if not mcp_json.exists():
        console.print(f"[yellow].mcp.json not found at {mcp_json}; skipping wiring.[/yellow]")
        return
    try:
        wire_mcp_json_file(mcp_json, token_file=str(written))
    except (KeyError, ValueError) as exc:
        console.print(f"[yellow]Could not wire {mcp_json}: {exc}[/yellow]")
        return
    console.print(
        f"[green]Wired[/green] {mcp_json} → connector now uses the token file. "
        f"[dim]Restart Claude Code to pick it up.[/dim]"
    )


@dashboard.command("mcp-setup")
@click.option("--oac-url", default=None,
              help="OAC base URL (default: $OAC_URL).")
@click.option("--user", "user", default=None,
              help="OAC basic-auth username (default: $OAC_MCP_USER; legacy fallback: $OAC_ADMIN_USER).")
@click.option("--password", "password", default=None,
              help="OAC basic-auth password, or ${vault:OCID} (default: $OAC_MCP_PASSWORD; legacy fallback: $OAC_ADMIN_PASSWORD).")
@click.option("--connector-js", default=None,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Path to the downloaded oac-mcp-connect.js to stage. Omit if one is "
                   "already staged at ~/.oac-connect/oac-mcp-connect.js.")
@click.option("--mcp-json", default=Path(".mcp.json"), type=click.Path(dir_okay=False, path_type=Path),
              show_default=True, help="Path to the .mcp.json to wire (credential-free).")
@click.option("--no-headless", is_flag=True,
              help="Allow the connector to fall back to browser auth (NOT for terminal clients).")
@click.option("--no-wire", is_flag=True, help="Write the config file + stage the connector only; don't touch .mcp.json.")
def dashboard_mcp_setup(
    oac_url: str | None,
    user: str | None,
    password: str | None,
    connector_js: Path | None,
    mcp_json: Path,
    no_headless: bool,
    no_wire: bool,
) -> None:
    """Configure the OAC MCP connector for non-interactive **basic auth** (works in Claude Code).

    On non-IDCS instances, issued tokens are rejected and the connector's only other
    auth is interactive browser login — which terminal MCP clients can't drive
    (elicitation unsupported). Basic auth supplies credentials up front, so it never
    elicits. This command:

      1. writes a 0600 connector config at ~/.oac-connect/oac_mcp_connect_config.json
         (URL + basicAuth + headless) — the connector auto-discovers it;
      2. stages the connector to ~/.oac-connect/oac-mcp-connect.js (stable path);
      3. wires a credential-free .mcp.json (single connector arg — URL and creds stay
         in the 0600 config, never in the committed repo).

    Credentials are read from --user/--password or $OAC_MCP_USER/$OAC_MCP_PASSWORD
    and never echoed. $OAC_ADMIN_USER/$OAC_ADMIN_PASSWORD remain supported as a
    backward-compatible fallback, but the user does not need to be an administrator.
    Scope the OAC user to least privilege — v1.4 exposes catalog write/delete/ACL
    tools governed by that user's grants.
    """
    import os as _os

    from .oac.mcp_token import DEFAULT_CONNECTOR_CONFIG_FILE, setup_basic_auth
    from .utils import vault

    def _env_first(*names: str) -> str | None:
        for name in names:
            value = _os.environ.get(name)
            if value:
                return value
        return None

    oac_url = oac_url or _os.environ.get("OAC_URL")
    user = user or _env_first("OAC_MCP_USER", "OAC_ADMIN_USER")
    password = password or _env_first("OAC_MCP_PASSWORD", "OAC_ADMIN_PASSWORD")

    missing = [n for n, v in (("--oac-url/$OAC_URL", oac_url),
                              ("--user/$OAC_MCP_USER", user),
                              ("--password/$OAC_MCP_PASSWORD", password)) if not v]
    if missing:
        console.print(f"[red]mcp-setup requires {', '.join(missing)}[/red]")
        sys.exit(2)

    try:
        summary = setup_basic_auth(
            oac_url=oac_url,  # type: ignore[arg-type]
            user=user,  # type: ignore[arg-type]
            password=vault.resolve(password),  # type: ignore[arg-type]
            connector_js=connector_js,
            mcp_json=None if no_wire else mcp_json,
            headless=not no_headless,
        )
    except FileNotFoundError as exc:
        console.print(
            f"[red]mcp-setup failed:[/red] {exc}\n"
            "[dim]Pass --connector-js <path to downloaded oac-mcp-connect.js> the first time.[/dim]"
        )
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — surface a clean CLI error
        console.print(f"[red]mcp-setup failed:[/red] {exc}")
        sys.exit(1)

    console.print(
        f"[green]Wrote connector config:[/green] {summary['config_file']} "
        f"(url={summary['oac_url']}, user={summary['user']}, basicAuth=present, 0600)"
    )
    console.print(f"[green]Staged connector:[/green] {summary['connector']}")
    if summary["mcp_json"]:
        console.print(
            f"[green]Wired[/green] {summary['mcp_json']} → "
            f'"{summary["connector_arg"]}" (no URL/creds in .mcp.json). '
            f"[dim]Restart/reconnect Claude Code to pick it up.[/dim]"
        )
    else:
        console.print(
            f"[dim]Skipped .mcp.json (--no-wire). Point the {DEFAULT_CONNECTOR_CONFIG_FILE.name} "
            f"connector at it with a single arg: {summary['connector_arg']}[/dim]"
        )


if __name__ == "__main__":
    main()
