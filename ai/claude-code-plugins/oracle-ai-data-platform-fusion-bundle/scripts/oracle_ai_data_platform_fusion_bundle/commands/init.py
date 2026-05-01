"""Implementation of ``aidp-fusion-bundle init``.

Scaffolds ``bundle.yaml`` + ``aidp.config.yaml`` in the current directory by
copying one of the bundled examples (``minimal_gl_only.yaml`` or
``full_finance.yaml``) and the canonical ``aidp.config.example.yaml``.
"""

from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path

from rich.console import Console

TEMPLATES = {
    "minimal": "minimal_gl_only.yaml",
    "full-finance": "full_finance.yaml",
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

    if not force and (bundle_target.exists() or config_target.exists()):
        console.print(
            f"[red]existing files found:[/red] "
            f"{[p.name for p in (bundle_target, config_target) if p.exists()]}; "
            f"pass --force to overwrite."
        )
        return 1

    examples_dir = _examples_dir()
    shutil.copy(examples_dir / TEMPLATES[template], bundle_target)
    shutil.copy(examples_dir / "aidp.config.example.yaml", config_target)

    console.print(f"[green]wrote[/green] {bundle_target}  ([dim]{TEMPLATES[template]}[/dim])")
    console.print(f"[green]wrote[/green] {config_target}  ([dim]aidp.config.example.yaml[/dim])")
    console.print(
        "\n[bold]Next steps:[/bold]\n"
        "  1. Fill in [cyan]variables.team[/cyan] + ${FUSION_*} env vars + ${vault:OCID} refs\n"
        "  2. Set workspace coords in [cyan]aidp.config.yaml[/cyan] (workspaceKey, dataLakeOcid, region)\n"
        "  3. Run [cyan]aidp-fusion-bundle validate[/cyan] to schema-check\n"
        "  4. Run [cyan]aidp-fusion-bundle bootstrap[/cyan] to probe live prereqs\n"
    )
    return 0


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
