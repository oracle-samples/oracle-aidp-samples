"""CLI verbs for content pack management.

Wires ``aidp-fusion-autopilot content-pack {validate,list,info}`` against the
loader (orchestrator.content_pack) and validators
(orchestrator.content_pack_validators).

Operator-facing behavior is documented in ``docs/content_pack_execution.md``.

Exit codes:
    0 — success
    1 — I/O or unexpected error
    2 — validation errors found
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

import oracle_ai_data_platform_fusion_autopilot as _pkg
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    PackLoaderError,
    ResolvedPack,
    load_pack,
    merge_overlay,
    resolve_overlay_chain,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
    ValidationReport,
    validate_pack_full,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import PackOverlayRef

INSTALLED_CONTENT_PACKS_DIR = Path(_pkg.__file__).parent / "content_packs"


# ---------------------------------------------------------------------------
# Pack discovery
# ---------------------------------------------------------------------------


def discover_installed_packs(root: Path = INSTALLED_CONTENT_PACKS_DIR) -> list[Path]:
    """Return paths to all packs shipped under the installed content_packs dir."""
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir() and (p / "pack.yaml").exists())


def resolve_pack_path(spec: str) -> Path:
    """Resolve a pack reference to a directory path.

    Accepts:
        - A pack name (e.g., ``fusion-finance-starter``) — looked up under
          the installed ``content_packs/`` directory.
        - A filesystem path to a pack root directory.
    """
    candidate = Path(spec)
    if candidate.exists() and (candidate / "pack.yaml").exists():
        return candidate.resolve()
    # Try installed packs by name.
    by_name = INSTALLED_CONTENT_PACKS_DIR / spec
    if by_name.exists() and (by_name / "pack.yaml").exists():
        return by_name.resolve()
    raise FileNotFoundError(
        f"could not resolve content pack {spec!r} — not a directory and not "
        f"an installed pack under {INSTALLED_CONTENT_PACKS_DIR}"
    )


# ---------------------------------------------------------------------------
# Overlay base resolution for the CLI
# ---------------------------------------------------------------------------


def _make_cli_base_resolver(overlay_root: Path):
    """Build a base resolver for `resolve_overlay_chain`.

    Looks up referenced base packs in two places, in order:

    1. As a sibling directory of the overlay root with the matching pack id.
       This is the common workflow during development — the customer's
       overlay sits next to the base it extends.
    2. Under the installed ``content_packs/`` directory (Oracle-shipped packs).

    On miss, raises ``FileNotFoundError`` so ``resolve_overlay_chain`` surfaces
    a clean error.
    """

    def resolver(ref: "PackOverlayRef") -> Path:
        # Sibling-directory lookup.
        sibling = overlay_root.parent / ref.name
        if sibling.exists() and (sibling / "pack.yaml").exists():
            return sibling.resolve()
        # Installed-packs lookup.
        installed = INSTALLED_CONTENT_PACKS_DIR / ref.name
        if installed.exists() and (installed / "pack.yaml").exists():
            return installed.resolve()
        raise FileNotFoundError(
            f"base pack {ref.name!r} (referenced as `extends: {ref.to_string()}`) "
            f"not found beside {overlay_root} or in {INSTALLED_CONTENT_PACKS_DIR}"
        )

    return resolver


def _load_full_chain(pack_path: Path) -> "ResolvedPack":
    """Backwards-compat alias for the orchestrator-owned loader.

    The generated REST notebook imports
    :func:`orchestrator.content_pack.load_full_chain` without crossing the
    dispatch import boundary. This alias preserves the older CLI-private import
    path for internal callers and matches the behavior 1:1: same callable
    shape, same default base resolver.
    """
    from ..orchestrator.content_pack import load_full_chain
    return load_full_chain(pack_path, base_resolver=_make_cli_base_resolver(pack_path))


# ---------------------------------------------------------------------------
# Verb: content-pack list
# ---------------------------------------------------------------------------


def list_packs(*, json_output: bool, console) -> int:
    """Enumerate installed content packs."""
    packs_info: list[dict[str, str]] = []
    for pack_dir in discover_installed_packs():
        try:
            pack = load_pack(pack_dir)
            packs_info.append(
                {
                    "name": pack.pack.id,
                    "version": pack.pack.version,
                    "path": str(pack_dir),
                }
            )
        except (PackLoaderError, ValidationError) as exc:
            packs_info.append(
                {
                    "name": pack_dir.name,
                    "version": "<load-error>",
                    "path": str(pack_dir),
                    "error": str(exc),
                }
            )

    if json_output:
        print(json.dumps({"packs": packs_info}, indent=2))
        return 0

    if not packs_info:
        console.print("[yellow]No installed content packs found.[/yellow]")
        return 0

    # Pretty table
    name_w = max(len(p["name"]) for p in packs_info)
    ver_w = max(len(p["version"]) for p in packs_info)
    console.print(f"[bold]{'NAME':<{name_w}}  {'VERSION':<{ver_w}}  PATH[/bold]")
    for p in packs_info:
        console.print(f"{p['name']:<{name_w}}  {p['version']:<{ver_w}}  {p['path']}")
    return 0


# ---------------------------------------------------------------------------
# Verb: content-pack info
# ---------------------------------------------------------------------------


def info_pack(name: str, *, json_output: bool, console) -> int:
    """Print details about an installed pack (overlay-aware)."""
    try:
        pack_path = resolve_pack_path(name)
        pack = _load_full_chain(pack_path)
    except (FileNotFoundError, PackLoaderError, ValidationError) as exc:
        if json_output:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 1

    info: dict[str, Any] = {
        "id": pack.pack.id,
        "version": pack.pack.version,
        "description": pack.pack.description,
        "compatibility": {
            "pluginMinVersion": pack.pack.compatibility.plugin_min_version,
            "fusionFamilies": pack.pack.compatibility.fusion_families,
            "requiresDelta": pack.pack.compatibility.aidp.requires_delta,
        },
        "path": str(pack.root),
        "nodes": {
            "bronze": list(sorted(pack.bronze)),
            "silver": list(sorted(pack.silver)),
            "gold": list(sorted(pack.gold)),
            "builtin_count": sum(
                1 for n in pack.silver.values() if n.implementation.type == "builtin"
            )
            + sum(1 for n in pack.gold.values() if n.implementation.type == "builtin"),
        },
        "variation_points": {
            "columnAliases": list(sorted(pack.pack.column_aliases)),
            "semanticVariants": list(sorted(pack.pack.semantic_variants)),
        },
        "dashboards": list(sorted(pack.dashboards)),
        "pack_hash": pack.compute_hash(),
    }

    if json_output:
        print(json.dumps(info, indent=2))
        return 0

    console.print(f"[bold]Pack:[/bold]            {info['id']}")
    console.print(f"[bold]Version:[/bold]         {info['version']}")
    console.print(f"[bold]Path:[/bold]            {info['path']}")
    console.print(
        f"[bold]Compatibility:[/bold]   pluginMinVersion >= {info['compatibility']['pluginMinVersion']}"
    )
    console.print(
        f"                  fusionFamilies: {info['compatibility']['fusionFamilies']}"
    )
    console.print(
        f"                  aidp.requiresDelta: {info['compatibility']['requiresDelta']}"
    )
    console.print(
        f"[bold]Nodes:[/bold]           {len(info['nodes']['bronze'])} bronze, "
        f"{len(info['nodes']['silver'])} silver, "
        f"{len(info['nodes']['gold'])} gold "
        f"({info['nodes']['builtin_count']} builtin)"
    )
    console.print(
        f"[bold]Variation points:[/bold] {len(info['variation_points']['columnAliases'])} columnAliases, "
        f"{len(info['variation_points']['semanticVariants'])} semanticVariants"
    )
    console.print(
        f"[bold]Dashboards:[/bold]      {len(info['dashboards'])}"
        + (" (" + ", ".join(info["dashboards"]) + ")" if info["dashboards"] else "")
    )
    return 0


# ---------------------------------------------------------------------------
# Verb: content-pack validate
# ---------------------------------------------------------------------------


def validate_pack_cli(
    name: str, *, json_output: bool, console, profile: str | None = None
) -> int:
    """Run schema + content validation; surface AIDPF codes.

    Resolves the full ``extends:`` chain via ``resolve_overlay_chain`` +
    ``merge_overlay`` before validating, so overlay failures (orphan
    overrides → AIDPF-2001, inherited dashboard / SQL errors, etc.) are
    surfaced to the operator.

    ``profile`` (optional) enables the profile-aware leg of the
    column-contract gate (AIDPF-2045): ``$column.*`` / ``$coa.*`` consumer
    demands resolve against the active tenant profile. Accepts either a direct
    path to a profile YAML or a bare profile name resolved against
    ``./profiles/<name>.yaml`` in the current bundle. When omitted, validation
    is profile-less (literal + watermark demands still gate).
    """
    loaded_profile = None
    if profile is not None:
        from ..schema.tenant_profile import (
            load_tenant_profile,
            resolve_profile_path,
        )

        profile_path = Path(profile)
        if not profile_path.exists():
            # Bare name → resolve against the current bundle's profiles/ dir.
            try:
                profile_path = resolve_profile_path(Path.cwd() / "bundle.yaml", profile)
            except Exception as exc:  # UnsafePathSegmentError etc.
                msg = f"AIDPF-1033: could not resolve --profile {profile!r}: {exc}"
                if json_output:
                    print(json.dumps({"errors": [{"code": "AIDPF-1033", "message": msg}]}, indent=2))
                else:
                    console.print(f"[red]{msg}[/red]")
                return 2
        if not profile_path.exists():
            msg = f"AIDPF-1033: profile YAML not found for --profile {profile!r} (looked at {profile_path})."
            if json_output:
                print(json.dumps({"errors": [{"code": "AIDPF-1033", "message": msg}]}, indent=2))
            else:
                console.print(f"[red]{msg}[/red]")
            return 2
        try:
            loaded_profile = load_tenant_profile(profile_path)
        except Exception as exc:
            if json_output:
                print(json.dumps({"errors": [{"code": "AIDPF-2000", "message": str(exc)}]}, indent=2))
            else:
                console.print(f"[red]profile load failed:[/red] {exc}")
            return 2

    try:
        pack_path = resolve_pack_path(name)
    except FileNotFoundError as exc:
        if json_output:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 1

    try:
        pack = _load_full_chain(pack_path)
    except PackLoaderError as exc:
        if json_output:
            print(json.dumps({"errors": [{"code": exc.code, "message": str(exc)}]}, indent=2))
        else:
            console.print(f"[red]{exc.code}:[/red] {exc}")
        return 2
    except FileNotFoundError as exc:
        # Base pack referenced by `extends:` not found.
        if json_output:
            print(json.dumps({"errors": [{"code": "AIDPF-2000", "message": str(exc)}]}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 2
    except ValidationError as exc:
        if json_output:
            print(
                json.dumps(
                    {
                        "errors": [
                            {
                                "code": "AIDPF-2000",
                                "message": str(e),
                                "loc": list(e.get("loc", ())),
                            }
                            for e in exc.errors()
                        ]
                    },
                    indent=2,
                )
            )
        else:
            console.print("[red]Pack schema validation failed:[/red]")
            for e in exc.errors():
                console.print(f"  - {e.get('msg', '')} (at {e.get('loc')})")
        return 2

    report: ValidationReport = validate_pack_full(pack, profile=loaded_profile)
    if json_output:
        print(
            json.dumps(
                {
                    "pack": pack.pack.id,
                    "version": pack.pack.version,
                    "ok": report.ok,
                    "errors": [
                        {"code": e.code, "message": e.message, "location": e.location}
                        for e in report.errors
                    ],
                    "warnings": [
                        {"code": w.code, "message": w.message, "location": w.location}
                        for w in report.warnings
                    ],
                },
                indent=2,
            )
        )
    else:
        if report.ok:
            console.print(
                f"[green]✓[/green] {pack.pack.id}@{pack.pack.version} validates clean."
            )
        else:
            console.print(
                f"[red]✗[/red] {pack.pack.id}@{pack.pack.version} has "
                f"{len(report.errors)} validation error(s):"
            )
            for e in report.errors:
                loc_part = f" [{e.location}]" if e.location else ""
                console.print(f"  [red]{e.code}[/red]{loc_part}: {e.message}")

    return 0 if report.ok else 2


# ---------------------------------------------------------------------------
# Verb: content-pack refresh-fork
# ---------------------------------------------------------------------------


def _resolve_base_path(overlay_path: Path, extends: str) -> Path | None:
    """Resolve the parent base pack a `replaceNode` overlay forked from.

    Mirrors :func:`_make_cli_base_resolver`: sibling directory first, then the
    installed content-packs dir. Returns ``None`` if not found.
    """
    base_name = str(extends).split("@")[0]
    sibling = overlay_path.parent / base_name
    if sibling.exists() and (sibling / "pack.yaml").exists():
        return sibling.resolve()
    installed = INSTALLED_CONTENT_PACKS_DIR / base_name
    if installed.exists() and (installed / "pack.yaml").exists():
        return installed.resolve()
    return None


def refresh_fork_cli(
    name: str, *, node: str | None, json_output: bool, console
) -> int:
    """Re-stamp ``replaceNode.forkedFrom`` fingerprints from the current base.

    Recomputes **all three** stamps (``sqlSha256`` / ``contractSha256`` /
    ``packVersion``) — using the SAME helpers the validate-time gate uses — and
    rewrites them in the leaf overlay's ``pack.yaml``. Re-stamping only the SQL
    fingerprint would leave an operator stuck after a contract-only ``AIDPF-2064``.

    Load sequencing (must fingerprint the PARENT base, not the merged/replaced
    node): read the leaf overlay raw ``pack.yaml`` for the ``replaceNode`` block,
    resolve the ancestor chain **excluding** the leaf to get the base node + root,
    compute the stamps from that parent base, then rewrite only the leaf file.
    """
    import yaml

    from ..orchestrator.content_pack import (
        AIDPF_2004_EXTENDS_VERSION_MISMATCH,
        _split_override_key,
        load_full_chain,
    )
    from ..orchestrator.sql_renderer import (
        compute_contract_fingerprint,
        compute_fork_fingerprint,
    )

    def _emit_error(code: str, message: str, exit_code: int) -> int:
        if json_output:
            print(json.dumps({"errors": [{"code": code, "message": message}]}, indent=2))
        else:
            console.print(f"[red]{code}:[/red] {message}")
        return exit_code

    try:
        overlay_path = resolve_pack_path(name)
    except FileNotFoundError as exc:
        return _emit_error("AIDPF-2000", str(exc), 1)

    raw = yaml.safe_load((overlay_path / "pack.yaml").read_text()) or {}
    overrides = raw.get("overrides") or {}
    targets = {
        k: v
        for k, v in overrides.items()
        if isinstance(v, dict) and isinstance(v.get("replaceNode"), dict)
    }
    if node is not None:
        targets = {k: v for k, v in targets.items() if k == node}
        if not targets:
            return _emit_error(
                "AIDPF-2000",
                f"no replaceNode override for --node {node!r} in {name!r}.",
                1,
            )
    if not targets:
        return _emit_error(
            "AIDPF-2000",
            f"pack {name!r} has no replaceNode overrides to refresh.",
            1,
        )

    extends = raw.get("extends")
    if not extends:
        return _emit_error(
            "AIDPF-2000", f"pack {name!r} is not an overlay (no `extends:`).", 1
        )
    try:
        ref = PackOverlayRef.parse(extends) if isinstance(extends, str) else None
    except Exception:
        ref = None
    if ref is None:
        return _emit_error(
            "AIDPF-2000", f"pack {name!r} has a malformed `extends:` value {extends!r}.", 1
        )
    base_path = _resolve_base_path(overlay_path, extends)
    if base_path is None:
        return _emit_error(
            "AIDPF-2000",
            f"base pack for `extends: {extends}` not found beside {overlay_path} "
            f"or in {INSTALLED_CONTENT_PACKS_DIR}.",
            1,
        )

    # Guard the `extends` version (AIDPF-2004) against the leaf's DIRECT parent.
    # resolve_overlay_chain enforces this while walking the chain, but refresh-fork
    # resolves the parent by NAME, so the version is otherwise unchecked — an
    # overlay declaring `extends: pack@0.1.0` against an installed pack@0.2.0 would
    # re-stamp forkedFrom from a base it does not extend, corrupting provenance.
    # Validate against the RAW candidate pack.yaml (its own declared id/version),
    # NOT a merged ResolvedPack: when the direct parent is itself an overlay,
    # merge_overlay keeps the ROOT base's identity, so the merged id/version would
    # not match the (overlay) parent's ref. Check the raw candidate before merging.
    raw_base = yaml.safe_load((base_path / "pack.yaml").read_text()) or {}
    base_id, base_version = raw_base.get("id"), raw_base.get("version")
    if base_id != ref.name or base_version != ref.version:
        return _emit_error(
            AIDPF_2004_EXTENDS_VERSION_MISMATCH,
            f"overlay {name!r} declares `extends: {ref.to_string()}` but the resolved "
            f"parent pack at {base_path} is {base_id}@{base_version}. Refusing to "
            f"re-stamp forkedFrom from a base the overlay does not extend — align the "
            f"versions first.",
            2,
        )

    # Now merge the parent chain (excluding the leaf) for fingerprint computation.
    try:
        base = load_full_chain(base_path, base_resolver=_make_cli_base_resolver(base_path))
    except (PackLoaderError, ValidationError, FileNotFoundError) as exc:
        return _emit_error("AIDPF-2000", f"could not load base pack: {exc}", 1)

    changes: list[dict[str, Any]] = []
    for key, entry in targets.items():
        layer, nid = _split_override_key(key)
        base_nodes = base.silver if layer == "silver" else base.gold
        base_node = base_nodes.get(nid) if layer in ("silver", "gold") else None
        if base_node is None:
            return _emit_error(
                "AIDPF-2000",
                f"replaceNode key {key!r} does not resolve to a shipped "
                f"silver/gold base node.",
                1,
            )
        old = dict(entry["replaceNode"].get("forkedFrom") or {})
        new = {
            "sqlSha256": compute_fork_fingerprint(base_node, base),
            "contractSha256": compute_contract_fingerprint(base_node),
            "packVersion": base.pack.version,
        }
        entry["replaceNode"]["forkedFrom"] = new
        changes.append(
            {
                "node": key,
                "changed": old != new,
                "sqlSha256": {"old": old.get("sqlSha256"), "new": new["sqlSha256"]},
                "contractSha256": {
                    "old": old.get("contractSha256"),
                    "new": new["contractSha256"],
                },
                "packVersion": {"old": old.get("packVersion"), "new": new["packVersion"]},
            }
        )

    (overlay_path / "pack.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))

    if json_output:
        print(json.dumps({"pack": raw.get("id", name), "refreshed": changes}, indent=2))
    else:
        any_changed = any(c["changed"] for c in changes)
        if not any_changed:
            console.print("[green]✓[/green] all forkedFrom stamps already current.")
        for c in changes:
            if not c["changed"]:
                console.print(f"  {c['node']}: unchanged")
                continue
            sql_moved = c["sqlSha256"]["old"] != c["sqlSha256"]["new"]
            contract_moved = c["contractSha256"]["old"] != c["contractSha256"]["new"]
            what = ", ".join(
                w for w, moved in (("logic", sql_moved), ("contract", contract_moved)) if moved
            ) or "version"
            console.print(
                f"  [yellow]re-stamped[/yellow] {c['node']} (base {what} changed) — "
                f"review your overlay against the new base before seeding."
            )
    return 0
