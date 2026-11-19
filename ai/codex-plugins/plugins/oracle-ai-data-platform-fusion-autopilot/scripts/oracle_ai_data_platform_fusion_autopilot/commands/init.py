"""Implementation of ``aidp-fusion-autopilot init``.

Scaffolds ``bundle.yaml`` + ``aidp.config.yaml`` in the current directory by
copying one of the bundled customer-project templates.
"""

from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path

from rich.console import Console

TEMPLATES: dict[str, tuple[str, str]] = {
    "full-finance-starter": (
        "full-finance-starter/bundle.yaml",
        "full-finance-starter/aidp.config.yaml",
    ),
    "minimal-bundle": ("minimal-bundle/bundle.yaml", "minimal-bundle/aidp.config.yaml"),
    "minimal": ("minimal_gl_only.yaml", "aidp.config.example.yaml"),
    "full-finance": ("full_finance.yaml", "aidp.config.example.yaml"),
}


def init(template: str, *, force: bool, console: Console | None = None) -> int:
    """Copy templates into ./bundle.yaml and ./aidp.config.yaml.

    Returns process exit code (0 on success, 1 on collision without --force).
    """
    console = console or Console()
    if template not in TEMPLATES:
        console.print(f"[red]unknown template: {template}[/red]; pick one of {list(TEMPLATES)}")
        return 2

    bundle_target = Path("bundle.yaml")
    config_target = Path("aidp.config.yaml")
    env_target = Path(".env")

    if not force and (bundle_target.exists() or config_target.exists()):
        console.print(
            f"[red]existing files found:[/red] "
            f"{[p.name for p in (bundle_target, config_target) if p.exists()]}; "
            f"pass --force to overwrite."
        )
        return 1

    examples_dir = _examples_dir()
    bundle_source, config_source = TEMPLATES[template]
    shutil.copy(examples_dir / bundle_source, bundle_target)
    shutil.copy(examples_dir / config_source, config_target)

    console.print(f"[green]wrote[/green] {bundle_target}  ([dim]{bundle_source}[/dim])")
    console.print(f"[green]wrote[/green] {config_target}  ([dim]{config_source}[/dim])")

    # Also scaffold a .env from .env.example so users can fill in creds in
    # one spot. The bundle auto-loads .env at startup via load_dotenv().
    # Never overwrite an existing .env (could contain real secrets) — even
    # with --force, that's too dangerous.
    env_example = _plugin_root() / ".env.example"
    if env_target.exists():
        console.print(f"[dim]skipped[/dim] {env_target}  ([dim].env already exists; left untouched[/dim])")
    elif env_example.exists():
        shutil.copy(env_example, env_target)
        console.print(f"[green]wrote[/green] {env_target}  ([dim].env.example[/dim])")
    else:
        console.print(f"[yellow]skipped[/yellow] {env_target}  ([dim].env.example not found[/dim])")
    console.print(
        "\n[bold]Next steps:[/bold]\n"
        "  1. Fill in [cyan]variables.team[/cyan] and the Fusion/OAC values in [cyan]bundle.yaml[/cyan]\n"
        "     (${FUSION_*}, ${OAC_URL}, schemas, and dataSourceName as needed).\n"
        "  2. Run [cyan]aidp-fusion-autopilot init-config[/cyan] with the AIDP OCID plus workspace/cluster names\n"
        "     to write [cyan]workspaceKey, aiDataPlatformId, clusterKey, clusterName[/cyan] in [cyan]aidp.config.yaml[/cyan].\n"
        "  3. Run [cyan]aidp-fusion-autopilot validate[/cyan] to schema-check the bundle.\n"
        "  4. Run [cyan]aidp-fusion-autopilot dashboard mcp-setup[/cyan] before OAC workbook phases.\n"
        "  5. Run [cyan]aidp-fusion-autopilot bootstrap[/cyan] to probe live prereqs.\n"
    )
    return 0


def _plugin_root() -> Path:
    """Locate the plugin root (where .env.example, pyproject.toml live)."""
    here = Path(__file__).resolve()
    return here.parent.parent.parent.parent


def _examples_dir() -> Path:
    """Locate the bundled examples directory.

    When installed via pip, examples ship as package data. For editable
    installs (and test runs), they're at ``../../../examples/`` relative to
    this file.
    """
    # Editable install: ../../../examples relative to this module
    here = Path(__file__).resolve()
    candidate = here.parent.parent.parent.parent / "examples"
    if candidate.exists():
        return candidate
    # Future: package-data fallback once pyproject.toml ships examples in the wheel.
    raise FileNotFoundError(f"examples directory not found at {candidate}")


__all__ = ["init", "TEMPLATES"]
