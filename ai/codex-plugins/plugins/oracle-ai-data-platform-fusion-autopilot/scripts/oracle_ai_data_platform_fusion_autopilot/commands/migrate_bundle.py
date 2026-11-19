"""Implementation of ``aidp-fusion-autopilot migrate-bundle --from X --to Y``.

Today no migrations are registered — the only supported version is `"0.2.0"`.
When v0.3 ships with a breaking schema change, the migrator gains a
real implementation here and the CLI surface stays stable.

**Returns exit codes directly** — does NOT raise NotImplementedError
because this is a separate top-level CLI verb and must produce its own exit
codes.
"""

from __future__ import annotations

from pathlib import Path

from rich.console import Console


def migrate_bundle(
    bundle_path: Path,
    from_version: str,
    to_version: str,
    *,
    console: Console | None = None,
) -> int:
    """Migrate a bundle.yaml from one schema version to another in place.

    Args:
        bundle_path: path to the bundle.yaml to migrate (unused today;
            present for forward compatibility).
        from_version: source schema version (e.g. "0.1.0").
        to_version: target schema version (e.g. "0.2.0").
        console: optional Rich console for output.

    Returns:
        0 if `from_version == to_version` (no-op).
        2 if no migration path is available (today: every non-trivial
          case, since only v0.2.0 exists).
    """
    console = console or Console()

    if from_version == to_version:
        console.print(
            f"[green]Bundle is already at version {to_version}.[/green]"
        )
        return 0

    console.print(
        f"[red]No migration path from {from_version!r} to "
        f"{to_version!r}.[/red] This plugin version supports only "
        f"v0.2.0. Migration helpers ship alongside the plugin version "
        f"that introduces the target schema."
    )
    return 2


__all__ = ["migrate_bundle"]
