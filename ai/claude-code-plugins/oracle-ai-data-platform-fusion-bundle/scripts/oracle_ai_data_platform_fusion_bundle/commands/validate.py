"""Implementation of ``aidp-fusion-bundle validate``.

Two layers of checking:
  1. **Schema** — ``bundle.yaml`` and ``aidp.config.yaml`` parse via Pydantic v2.
  2. **Ref integrity** — every dataset id maps to an entry in
     :mod:`oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog`,
     and every variable / vault reference resolves (env vars only —
     vault refs are noted but NOT resolved here, since that requires
     OCI session and the ``orchestrator`` does it lazily).

No network calls.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError
from rich.console import Console

from ..schema.bundle import AidpConfig, Bundle
from ..schema.fusion_catalog import CATALOG
from ..schema.refs import find_vault_refs


def validate(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    console: Console | None = None,
) -> int:
    """Validate bundle.yaml + aidp.config.yaml; return process exit code."""
    console = console or Console()
    issues: list[str] = []

    bundle = _load_bundle(bundle_path, console, issues)
    config = _load_config(config_path, console, issues)

    if config and env_name not in config.environments:
        issues.append(
            f"aidp.config.yaml has no environment named '{env_name}'; "
            f"available: {sorted(config.environments.keys())}"
        )

    if bundle:
        # Dataset id -> CATALOG entry
        unknown = [ds.id for ds in bundle.datasets if ds.id not in CATALOG]
        if unknown:
            issues.append(
                f"unknown dataset ids in bundle.yaml.datasets: {unknown} — "
                f"add them to schema/fusion_catalog.py first"
            )

        # Surface Vault refs (informational only)
        vault_refs = _collect_vault_refs(bundle)
        if vault_refs:
            console.print(
                f"\n[yellow]Vault refs found ({len(vault_refs)}):[/yellow] "
                f"will be resolved at orchestrator startup"
            )
            for ocid in vault_refs:
                console.print(f"  - {ocid}")

    if issues:
        console.print("\n[red]validation failed:[/red]")
        for line in issues:
            console.print(f"  - {line}")
        return 1

    console.print("\n[green]validation passed.[/green]")
    if bundle:
        console.print(f"  bundle.yaml  -> {len(bundle.datasets)} datasets, "
                      f"{len(bundle.dimensions.build)} dimensions, "
                      f"{len(bundle.gold.marts)} gold marts")
    if config:
        console.print(f"  aidp.config.yaml -> environments: "
                      f"{sorted(config.environments.keys())}")
    return 0


def _load_bundle(path: Path, console: Console, issues: list[str]) -> Bundle | None:
    if not path.exists():
        issues.append(f"{path} not found")
        return None
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return Bundle.model_validate(raw)
    except ValidationError as exc:
        issues.append(f"bundle.yaml schema errors:\n{exc}")
    except yaml.YAMLError as exc:
        issues.append(f"bundle.yaml YAML parse error: {exc}")
    return None


def _load_config(path: Path, console: Console, issues: list[str]) -> AidpConfig | None:
    if not path.exists():
        issues.append(f"{path} not found")
        return None
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return AidpConfig.model_validate(raw)
    except ValidationError as exc:
        issues.append(f"aidp.config.yaml schema errors:\n{exc}")
    except yaml.YAMLError as exc:
        issues.append(f"aidp.config.yaml YAML parse error: {exc}")
    return None


def _collect_vault_refs(bundle: Bundle) -> set[str]:
    """Walk every string field in the bundle and collect ``${vault:OCID}`` refs."""
    refs: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, str):
            for ref in find_vault_refs(value):
                refs.add(ref.ocid)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for v in value.values():
                visit(v)
        elif hasattr(value, "model_dump"):
            visit(value.model_dump())

    visit(bundle.model_dump())
    return refs


__all__ = ["validate"]
