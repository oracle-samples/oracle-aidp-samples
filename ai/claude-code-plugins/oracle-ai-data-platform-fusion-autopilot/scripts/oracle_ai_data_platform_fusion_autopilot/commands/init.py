"""Implementation of ``aidp-fusion-autopilot init``.

Scaffolds ``bundle.yaml`` + ``aidp.config.yaml`` in the current directory by
copying one of the bundled customer-project templates.
"""

from __future__ import annotations

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

    bundle_source, config_source = TEMPLATES[template]
    bundle_target.write_bytes(_read_scaffold(bundle_source))
    config_target.write_bytes(_read_scaffold(config_source))

    console.print(f"[green]wrote[/green] {bundle_target}  ([dim]{bundle_source}[/dim])")
    console.print(f"[green]wrote[/green] {config_target}  ([dim]{config_source}[/dim])")

    # Also scaffold a .env from .env.example so users can fill in creds in
    # one spot. The bundle auto-loads .env at startup via load_dotenv().
    # Never overwrite an existing .env (could contain real secrets) — even
    # with --force, that's too dangerous.
    if env_target.exists():
        console.print(f"[dim]skipped[/dim] {env_target}  ([dim].env already exists; left untouched[/dim])")
    else:
        env_bytes = _read_scaffold_optional(".env.example")
        if env_bytes is not None:
            env_target.write_bytes(env_bytes)
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


def _read_scaffold(relpath: str) -> bytes:
    """Return the bytes of a scaffold template, raising if it's missing.

    See :func:`_read_scaffold_optional` for the resolution order.
    """
    data = _read_scaffold_optional(relpath)
    if data is None:
        raise FileNotFoundError(
            f"scaffold template {relpath!r} not found in package data "
            f"(oracle_ai_data_platform_fusion_autopilot/_scaffold/) or in the "
            f"repo examples/ dev fallback. The wheel may be built without "
            f"the _scaffold package-data — check pyproject.toml."
        )
    return data


def _read_scaffold_optional(relpath: str) -> bytes | None:
    """Read a scaffold template's bytes, or ``None`` if it doesn't exist.

    Resolution order:

    1. **Package data** — ``oracle_ai_data_platform_fusion_autopilot/_scaffold/``.
       This is what ships in the wheel, so a ``pip install`` customer gets a
       working ``init``. Read via :mod:`importlib.resources` so it works
       regardless of install layout (wheel dir, zipimport).
    2. **Repo dev fallback** — the repo-root ``examples/`` tree (and
       ``.env.example``) relative to this module, for editable installs and
       the test suite. Kept so contributors editing ``examples/`` see their
       changes without re-syncing ``_scaffold/`` (a drift-guard test pins the
       two in sync).

    ``relpath`` is the path under the scaffold root, e.g.
    ``"full-finance-starter/bundle.yaml"`` or ``".env.example"``.
    """
    # 1. Package data (the shipped path).
    try:
        scaffold = resources.files(
            "oracle_ai_data_platform_fusion_autopilot"
        ).joinpath("_scaffold", relpath)
        if scaffold.is_file():
            return scaffold.read_bytes()
    except (ModuleNotFoundError, FileNotFoundError):
        pass

    # 2. Repo dev fallback. ``.env.example`` lives at the repo root; every
    # other template lives under ``examples/``.
    repo_root = Path(__file__).resolve().parents[3]
    candidate = (
        repo_root / ".env.example"
        if relpath == ".env.example"
        else repo_root / "examples" / relpath
    )
    if candidate.is_file():
        return candidate.read_bytes()

    return None


__all__ = ["init", "TEMPLATES"]
