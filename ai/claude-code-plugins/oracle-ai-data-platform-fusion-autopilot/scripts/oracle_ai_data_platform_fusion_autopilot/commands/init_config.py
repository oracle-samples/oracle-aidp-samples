"""Implementation of ``aidp-fusion-autopilot init-config``.

Resolves human-friendly AIDP names into the OCID/UUID keys that
``aidp.config.yaml`` needs, so a customer never hand-copies a
``workspaceKey`` or ``clusterKey`` out of a console URL.

The customer supplies:

* the **AIDP / DataLake OCID** (the one true root — it anchors the REST
  client; copy it once from the AIDP console URL),
* the **workspace display name**,
* the **cluster display name**,

and this command reuses the already-built
:class:`~oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient`
discovery primitives (``find_workspace_by_name`` /
``find_cluster_by_name``, OCI-signed) to resolve:

* workspace name  → ``workspaceKey``
* cluster name    → ``clusterKey`` (+ live state, surfaced as a warning
  if not ACTIVE)

then writes (or updates) the named environment block in
``aidp.config.yaml``. The catalog name is NOT written here — it lives in
``bundle.yaml`` under ``aidp.catalog`` — but ``--catalog`` is accepted so
the command can remind the operator where it belongs.

Layering note (CLAUDE.md): this is a thin discovery+write helper. It
does NOT re-implement OCI signing or cluster probing — it calls the
existing ``AidpRestClient``, mirroring how the seed skill shells out to
the CLI rather than importing the orchestrator.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from rich.console import Console


def init_config(
    *,
    config_path: Path,
    env_name: str,
    aidp_id: str,
    workspace_name: str,
    cluster_name: str,
    region: str,
    oci_profile: str,
    project: str | None,
    catalog: str | None,
    bicc_secret_name: str,
    bundle_path: Path | None,
    force: bool,
    dry_run: bool,
    console: Console | None = None,
) -> int:
    """Resolve names → keys and write the env block in aidp.config.yaml.

    Returns a process exit code:
      0 — resolved + written (or dry-run printed)
      1 — name not found / REST error
      2 — config exists and --force not given (collision)
    """
    console = console or Console()

    # Reuse the canonical, OCI-signed REST client. Import locally so the
    # `init-config` help text renders without pulling oci/requests at
    # module import for unrelated commands.
    from ..dispatch.rest_client import AidpRestClient, AidpRestError

    # ---- Resolve names → keys via the existing discovery primitives ----
    # `find_workspace_by_name` hits the data-lake-scoped endpoint and does
    # not need a workspace key, so we construct with an empty placeholder
    # and set the resolved key before the cluster lookup (which IS
    # workspace-scoped).
    try:
        client = AidpRestClient(
            region=region,
            aidp_id=aidp_id,
            workspace_key="",
            oci_profile=oci_profile,
        )
    except Exception as exc:  # noqa: BLE001 — OCI config / profile errors
        console.print(
            f"[red]Could not initialise the AIDP REST client:[/red] {exc}\n"
            f"[dim]Check ~/.oci/config has a '{oci_profile}' profile "
            f"(run `oci session authenticate` for a session profile).[/dim]"
        )
        return 1

    try:
        ws = client.find_workspace_by_name(workspace_name)
    except AidpRestError as exc:
        console.print(f"[red]Workspace lookup failed:[/red] {exc}")
        _print_available(console, "workspaces", _safe_list(client.list_workspaces))
        return 1

    client.workspace_key = ws.key  # cluster discovery is workspace-scoped

    try:
        cluster = client.find_cluster_by_name(cluster_name)
    except AidpRestError as exc:
        console.print(f"[red]Cluster lookup failed:[/red] {exc}")
        _print_available(
            console, "clusters", _safe_list(client.list_clusters), with_state=True
        )
        return 1

    console.print(
        f"[green]Resolved[/green] workspace [cyan]{workspace_name}[/cyan] → "
        f"[dim]{ws.key}[/dim]"
    )
    state_tag = (
        f"[green]{cluster.state}[/green]"
        if cluster.state == "ACTIVE"
        else f"[yellow]{cluster.state}[/yellow]"
    )
    console.print(
        f"[green]Resolved[/green] cluster [cyan]{cluster_name}[/cyan] → "
        f"[dim]{cluster.key}[/dim] (state={state_tag})"
    )
    if cluster.state != "ACTIVE":
        console.print(
            f"[yellow]Note:[/yellow] cluster is [bold]{cluster.state}[/bold], not "
            f"ACTIVE. The config is still written; start it before `run`."
        )

    # ---- Build the env block ----
    resolved_project = project or _project_from_bundle(bundle_path) or "fusion-autopilot"

    env_block: dict[str, Any] = {
        "workspaceKey": ws.key,
        "region": region,
        "ociProfile": oci_profile,
        "aiDataPlatformId": aidp_id,
        "clusterKey": cluster.key,
        "clusterName": cluster.display_name or cluster_name,
        "biccSecretName": bicc_secret_name,
    }

    # ---- Merge into existing config (preserve sibling environments) ----
    merged = _merge_into_config(
        config_path=config_path,
        env_name=env_name,
        env_block=env_block,
        project=resolved_project,
        region=region,
    )

    # Validate the merged result against the schema before writing, so we
    # never emit an aidp.config.yaml that `validate` would reject.
    try:
        from ..schema.bundle import AidpConfig

        AidpConfig.model_validate(merged)
    except Exception as exc:  # noqa: BLE001 — surface schema problems cleanly
        console.print(f"[red]Refusing to write — schema validation failed:[/red] {exc}")
        return 1

    rendered = yaml.safe_dump(merged, sort_keys=False, default_flow_style=False)

    if dry_run:
        console.print(
            f"\n[bold]--dry-run[/bold] — would write [cyan]{config_path}[/cyan] "
            f"(env [bold]{env_name}[/bold]):\n"
        )
        console.print(rendered)
        _print_catalog_reminder(console, catalog, bundle_path)
        return 0

    # A --force is only required when we'd overwrite an env that already
    # exists. Adding a brand-new env into an existing multi-env file is a
    # safe merge and proceeds without --force.
    if (
        config_path.exists()
        and not force
        and _env_already_present(config_path, env_name)
    ):
        console.print(
            f"[red]Environment '{env_name}' already exists in {config_path}.[/red] "
            f"Pass [bold]--force[/bold] to overwrite it."
        )
        return 2

    config_path.write_text(rendered, encoding="utf-8")
    console.print(
        f"\n[bold green]Wrote[/bold green] {config_path} "
        f"([dim]env={env_name}[/dim])"
    )
    _print_catalog_reminder(console, catalog, bundle_path)
    console.print(
        "\n[bold]Next:[/bold] "
        "[cyan]aidp-fusion-autopilot validate[/cyan] → "
        "[cyan]aidp-fusion-autopilot bootstrap[/cyan]"
    )
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_list(fn: Any) -> list[Any]:
    """Call a list_* method, swallowing REST errors (best-effort hinting)."""
    try:
        return list(fn())
    except Exception:  # noqa: BLE001 — hinting only; original error already shown
        return []


def _print_available(
    console: Console, label: str, items: list[Any], *, with_state: bool = False
) -> None:
    if not items:
        return
    console.print(f"[dim]Available {label}:[/dim]")
    for it in items:
        name = getattr(it, "display_name", None) or "<unnamed>"
        if with_state:
            console.print(f"  • {name}  [dim](state={getattr(it, 'state', '?')})[/dim]")
        else:
            console.print(f"  • {name}")


def _project_from_bundle(bundle_path: Path | None) -> str | None:
    """Best-effort read of `project:` from bundle.yaml for a sensible default."""
    if not bundle_path or not bundle_path.exists():
        return None
    try:
        raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            val = raw.get("project")
            return val if isinstance(val, str) else None
    except Exception:  # noqa: BLE001 — default-derivation only
        return None
    return None


def _load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:  # noqa: BLE001 — a malformed existing file → start fresh
        return {}


def _env_already_present(config_path: Path, env_name: str) -> bool:
    existing = _load_config(config_path)
    envs = existing.get("environments")
    return isinstance(envs, dict) and env_name in envs


def _merge_into_config(
    *,
    config_path: Path,
    env_name: str,
    env_block: dict[str, Any],
    project: str,
    region: str,
) -> dict[str, Any]:
    """Return a full aidp.config.yaml dict with env_name set to env_block.

    Preserves sibling environments and any existing top-level keys. Note:
    YAML comments in an existing file are NOT preserved (PyYAML round-trip
    limitation) — the operator's resolved values are.
    """
    existing = _load_config(config_path)

    merged: dict[str, Any] = dict(existing) if existing else {}
    merged.setdefault("apiVersion", "aidp-fusion-autopilot/v1")
    merged["project"] = existing.get("project", project) if existing else project

    defaults = dict(merged.get("defaults") or {})
    defaults.setdefault("region", region)
    defaults.setdefault("workspaceRoot", "Shared")
    merged["defaults"] = defaults

    envs = dict(merged.get("environments") or {})
    envs[env_name] = env_block
    merged["environments"] = envs
    return merged


def _print_catalog_reminder(
    console: Console, catalog: str | None, bundle_path: Path | None
) -> None:
    if not catalog:
        return
    where = bundle_path.name if bundle_path else "bundle.yaml"
    console.print(
        f"[dim]Catalog [bold]{catalog}[/bold] is not part of aidp.config.yaml — "
        f"set it under [bold]aidp.catalog[/bold] in {where}.[/dim]"
    )


__all__ = ["init_config"]
