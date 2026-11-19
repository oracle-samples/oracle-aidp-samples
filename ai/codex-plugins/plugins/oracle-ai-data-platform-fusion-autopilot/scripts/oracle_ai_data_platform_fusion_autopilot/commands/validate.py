"""Implementation of ``aidp-fusion-autopilot validate``.

Two layers of checking:
  1. **Schema** — ``bundle.yaml`` and ``aidp.config.yaml`` parse via Pydantic v2.
  2. **Ref integrity** — every dataset id resolves cross-layer in the
     configured content pack (``pack.bronze ∪ pack.silver ∪ pack.gold``).
     Bundles may declare silver/gold node ids as high-level intent, and
     customer overlay packs may declare custom PVOs not in the curated
     catalog (``AIDPF-2080`` is WARN-only for those custom PVOs).
     Variable / vault references are noted but NOT resolved here.

No network calls.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError
from rich.console import Console

from ..schema.bundle import AidpConfig, Bundle
from ..schema.fusion_catalog import CATALOG
from ..schema.refs import find_vault_refs, render_tree


AIDPF_2081_BUNDLE_DATASET_NOT_IN_PACK = "AIDPF-2081"


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
        # Cross-layer pack dataset resolution.
        _validate_datasets_against_pack(bundle, bundle_path, issues, console)

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


def _validate_datasets_against_pack(
    bundle: Bundle,
    bundle_path: Path,
    issues: list[str],
    console: Console,
) -> None:
    """Every ``datasets[].id`` must resolve in
    ``pack.bronze ∪ pack.silver ∪ pack.gold`` (cross-layer intent).

    Loads the bundle's content pack (with overlay chain) via
    :func:`load_full_chain` — mirrors what the runtime path does so
    customer overlay-pack-authored bronze ids are visible.

    The legacy catalog fallback is reserved for bundles WITHOUT a
    declared ``contentPack`` block.
    """
    if bundle.content_pack is None:
        # No content pack declared — legacy bundle shape. Fall back
        # to the catalog membership check so bronze-only bundles still
        # get typo detection.
        from ..schema.fusion_catalog import CATALOG
        unknown = [ds.id for ds in bundle.datasets if ds.id not in CATALOG]
        if unknown:
            issues.append(
                f"unknown dataset ids in bundle.yaml.datasets: {unknown} — "
                f"add them to schema/fusion_catalog.py first OR add a content "
                f"pack with bronze/silver/gold YAMLs declaring them."
            )
        return

    # Bundle DECLARES a content pack: any failure to resolve the pack
    # root is a validation issue, not a silent fallback to the legacy
    # catalog. The run command uses the same resolver and will fail
    # with the same code; validate must catch it first.
    from ..schema.bundle import (
        ContentPackRootInvalidError,
        ContentPackRootNotFoundError,
        resolve_content_pack_root,
    )
    try:
        pack_root = resolve_content_pack_root(bundle_path, bundle.content_pack)
    except (
        ContentPackRootNotFoundError,
        ContentPackRootInvalidError,
    ) as exc:
        issues.append(str(exc))
        return
    except Exception as exc:  # noqa: BLE001 — defense in depth for
                              # unforeseen resolver failures (e.g.
                              # permission denied, broken symlink).
        issues.append(
            f"content pack declared in bundle.yaml but failed to resolve: "
            f"{type(exc).__name__}: {exc}"
        )
        return

    try:
        from ..orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
    except ImportError as exc:
        issues.append(f"pack loader unavailable: {exc}")
        return

    try:
        resolver = make_filesystem_base_resolver(pack_root)
        pack = load_full_chain(pack_root, base_resolver=resolver)
    except Exception as exc:  # noqa: BLE001 — surface loader failures uniformly
        issues.append(f"content pack at {pack_root} failed to load: {exc}")
        return

    layer_ids = set(pack.bronze) | set(pack.silver) | set(pack.gold)
    unknown = [ds.id for ds in bundle.datasets if ds.id not in layer_ids]
    if unknown:
        issues.append(
            f"{AIDPF_2081_BUNDLE_DATASET_NOT_IN_PACK}: bundle.yaml datasets "
            f"do not resolve in any pack layer: {unknown}. Known across "
            f"all layers: bronze={sorted(pack.bronze)!r}, "
            f"silver={sorted(pack.silver)!r}, gold={sorted(pack.gold)!r}."
        )


def _load_bundle(path: Path, console: Console, issues: list[str]) -> Bundle | None:
    if not path.exists():
        issues.append(f"{path} not found")
        return None
    try:
        raw = render_tree(yaml.safe_load(path.read_text(encoding="utf-8")))
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
        raw = render_tree(yaml.safe_load(path.read_text(encoding="utf-8")))
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
