"""Unit tests for overlay pack resolution and merge.

Covers Step 5 of v2-phase-1-content-pack-schema. Uses tmp_path-based fixture
packs so the loader's filesystem path is exercised end-to-end.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    AIDPF_2001,
    AIDPF_2004_EXTENDS_VERSION_MISMATCH,
    ExtendsVersionMismatchError,
    OrphanOverrideError,
    OverlayCycleError,
    load_pack,
    merge_overlay,
    resolve_overlay_chain,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import PackOverlayRef


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def _make_base_pack(root: Path) -> Path:
    """Write a minimal valid base pack under ``root``; return the pack root."""
    pack_root = root / "fusion-finance-starter"
    pack_root.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        pack_root / "pack.yaml",
        {
            "id": "fusion-finance-starter",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "columnAliases": {
                "invoice_currency_code": {
                    "appliesTo": "bronze.ap_invoices",
                    "required": True,
                    "candidates": [
                        "ApInvoicesInvoiceCurrencyCode",
                        "ApInvoicesCurrencyCode",
                    ],
                }
            },
        },
    )
    # One silver node so override tests have a target.
    _write_yaml(
        pack_root / "silver" / "dim_supplier.yaml",
        {
            "id": "dim_supplier",
            "layer": "silver",
            "implementation": {
                "type": "builtin",
                "callable": "pkg.dim_supplier:build",
            },
            "target": "dim_supplier",
            "dependsOn": {
                "bronze": [
                    {"id": "erp_suppliers", "watermark": {"column": "_extract_ts"}}
                ]
            },
            "refresh": {
                "seed": {"strategy": "replace"},
                "incremental": {
                    "strategy": "merge",
                    "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
                    "naturalKey": ["supplier_number"],
                },
            },
            "outputSchema": {
                "columns": [
                    {"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"},
                ]
            },
        },
    )
    return pack_root


def _make_overlay(
    root: Path,
    base_ref: str,
    extra: dict | None = None,
    overrides: dict | None = None,
) -> Path:
    """Write a minimal overlay pack; return its root."""
    overlay_root = root / "acme-finance"
    overlay_root.mkdir(parents=True, exist_ok=True)
    body = {
        "id": "acme-finance",
        "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": base_ref,
    }
    if extra:
        body.update(extra)
    if overrides:
        body["overrides"] = overrides
    _write_yaml(overlay_root / "pack.yaml", body)
    return overlay_root


# ---------------------------------------------------------------------------
# load_pack
# ---------------------------------------------------------------------------


def test_load_pack_base_only(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    pack = load_pack(pack_root)
    assert pack.pack.id == "fusion-finance-starter"
    assert "invoice_currency_code" in pack.pack.column_aliases
    assert "dim_supplier" in pack.silver
    assert pack.chain == ("fusion-finance-starter",)


def test_load_pack_computes_stable_hash(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    h1 = load_pack(pack_root).compute_hash()
    h2 = load_pack(pack_root).compute_hash()
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex digest


# ---------------------------------------------------------------------------
# resolve_overlay_chain
# ---------------------------------------------------------------------------


def test_resolve_chain_single_pack_no_extends(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    chain = resolve_overlay_chain(pack_root)
    assert chain == [pack_root.resolve()]


def test_resolve_chain_overlay_to_base(tmp_path: Path) -> None:
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(tmp_path, "fusion-finance-starter@0.1.0")

    def resolver(ref: PackOverlayRef) -> Path:
        assert ref.name == "fusion-finance-starter"
        return base_root

    chain = resolve_overlay_chain(overlay_root, base_resolver=resolver)
    assert chain == [base_root.resolve(), overlay_root.resolve()]


def test_resolve_chain_rejects_wrong_version_base(tmp_path: Path) -> None:
    """Overlay declaring extends: foo@9.9.9 must not silently resolve to foo@0.1.0.

    Regression test for Finding 4 — without the centralised version check
    in resolve_overlay_chain, a name-only resolver could substitute a
    different base version than the overlay expects, masking orphan
    overrides / dashboard drift / SQL changes.
    """
    # Build a base pack whose actual version is 0.1.0.
    base_root = _make_base_pack(tmp_path)
    assert (base_root / "pack.yaml").exists()

    # Overlay declares extends: fusion-finance-starter@9.9.9 (wrong version).
    overlay_root = _make_overlay(tmp_path, "fusion-finance-starter@9.9.9")

    def resolver(ref: PackOverlayRef) -> Path:
        # Resolver returns the 0.1.0 base regardless of the version in `ref`.
        return base_root

    with pytest.raises(ExtendsVersionMismatchError) as exc:
        resolve_overlay_chain(overlay_root, base_resolver=resolver)
    assert AIDPF_2004_EXTENDS_VERSION_MISMATCH in str(exc.value)
    assert "9.9.9" in str(exc.value)
    assert "0.1.0" in str(exc.value)


def test_resolve_chain_rejects_wrong_id_base(tmp_path: Path) -> None:
    """Resolver returns a pack whose id doesn't match the ref's name → AIDPF-2004."""
    # Base pack with id 'sibling-base'.
    base_root = tmp_path / "sibling-base"
    base_root.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        base_root / "pack.yaml",
        {
            "id": "sibling-base",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        },
    )

    # Overlay declares extends: some-other-name@0.1.0 but resolver maps to sibling-base.
    overlay_root = tmp_path / "broken-overlay"
    overlay_root.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        overlay_root / "pack.yaml",
        {
            "id": "broken-overlay",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "extends": "some-other-name@0.1.0",
        },
    )

    def bad_resolver(ref: PackOverlayRef) -> Path:
        return base_root  # always returns sibling-base regardless of ref.name

    with pytest.raises(ExtendsVersionMismatchError) as exc:
        resolve_overlay_chain(overlay_root, base_resolver=bad_resolver)
    assert AIDPF_2004_EXTENDS_VERSION_MISMATCH in str(exc.value)
    assert "sibling-base" in str(exc.value)


def test_overlay_chain_cycle_rejected(tmp_path: Path) -> None:
    pack_a = tmp_path / "pack-a"
    pack_b = tmp_path / "pack-b"
    _write_yaml(
        pack_a / "pack.yaml",
        {
            "id": "pack-a",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0"},
            "extends": "pack-b@0.1.0",
        },
    )
    _write_yaml(
        pack_b / "pack.yaml",
        {
            "id": "pack-b",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0"},
            "extends": "pack-a@0.1.0",
        },
    )

    def resolver(ref: PackOverlayRef) -> Path:
        return pack_a if ref.name == "pack-a" else pack_b

    with pytest.raises(OverlayCycleError) as exc:
        resolve_overlay_chain(pack_a, base_resolver=resolver)
    assert AIDPF_2001 in str(exc.value)


# ---------------------------------------------------------------------------
# merge_overlay
# ---------------------------------------------------------------------------


def test_orphan_override_rejected(tmp_path: Path) -> None:
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(
        tmp_path,
        "fusion-finance-starter@0.1.0",
        overrides={
            "silver/dim_nonexistent": {"profile": "finance-default"},
        },
    )
    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    with pytest.raises(OrphanOverrideError) as exc:
        merge_overlay(base, overlay)
    assert AIDPF_2001 in str(exc.value)


def test_column_aliases_inherit_keyword(tmp_path: Path) -> None:
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(
        tmp_path,
        "fusion-finance-starter@0.1.0",
        extra={
            "columnAliases": {
                "invoice_currency_code": {
                    "appliesTo": "bronze.ap_invoices",
                    "required": True,
                    "candidates": ["APInvoicesCurrencyCode", "inherit"],
                }
            }
        },
    )
    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    merged = merge_overlay(base, overlay)
    candidates = merged.pack.column_aliases["invoice_currency_code"].candidates
    # `inherit` expands to base candidates in position.
    assert candidates == [
        "APInvoicesCurrencyCode",
        "ApInvoicesInvoiceCurrencyCode",
        "ApInvoicesCurrencyCode",
    ]


def test_overrides_sql_full_replace(tmp_path: Path) -> None:
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(
        tmp_path,
        "fusion-finance-starter@0.1.0",
        overrides={"silver/dim_supplier": {"sql": "silver/dim_supplier_acme.sql"}},
    )
    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    merged = merge_overlay(base, overlay)
    # The merged node now has type: sql pointing at the overlay's SQL path.
    assert merged.silver["dim_supplier"].implementation.type == "sql"
    assert merged.silver["dim_supplier"].implementation.sql == "silver/dim_supplier_acme.sql"


def test_overrides_quality_tests_list_extend(tmp_path: Path) -> None:
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(
        tmp_path,
        "fusion-finance-starter@0.1.0",
        overrides={
            "silver/dim_supplier": {
                "quality": {
                    "tests": [{"type": "row_count_min", "min": 1}],
                }
            }
        },
    )
    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    merged = merge_overlay(base, overlay)
    tests = merged.silver["dim_supplier"].quality.tests
    # Base had no tests; overlay added one.
    assert len(tests) == 1
    assert tests[0].type == "row_count_min"


def test_merged_source_roots_track_provenance(tmp_path: Path) -> None:
    """Merged pack records per-node source-root provenance.

    Inherited base nodes keep base.root; overlay overrides + overlay-only
    additions get overlay.root. This is what lets validators resolve SQL
    paths correctly against the pack each artifact actually lives in.
    """
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(
        tmp_path,
        "fusion-finance-starter@0.1.0",
        overrides={"silver/dim_supplier": {"sql": "silver/dim_supplier_acme.sql"}},
    )
    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    merged = merge_overlay(base, overlay)

    # The overridden node's source root is the overlay (where the new SQL lives).
    assert merged.root_for("silver/dim_supplier") == overlay_root.resolve()
    # And merged.root_for() falls back to overlay.root for anything unmapped.
    assert merged.root_for("nonexistent") == overlay_root.resolve()


def test_merged_inherited_sql_node_resolves_against_base_root(tmp_path: Path) -> None:
    """An overlay that does NOT override an inherited type:sql base node
    must validate that node's SQL against the BASE root, not the overlay root.

    Regression test for the Finding 3 bug where `merge_overlay` used
    `root=overlay.root` for everything, causing inherited base SQL paths
    to be searched under the overlay directory.
    """
    import yaml

    from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
        AIDPF_2003_SQL_FILE_MISSING,
        validate_sql_paths,
    )

    # Base pack with a real type:sql node + on-disk SQL file.
    base_root = tmp_path / "base-pack"
    base_root.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        base_root / "pack.yaml",
        {
            "id": "base-pack",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        },
    )
    _write_yaml(
        base_root / "silver" / "dim_thing.yaml",
        {
            "id": "dim_thing",
            "layer": "silver",
            "implementation": {"type": "sql", "sql": "silver/dim_thing.sql"},
            "target": "dim_thing",
            "dependsOn": {
                "bronze": [
                    {"id": "src", "watermark": {"column": "_extract_ts"}}
                ]
            },
            "refresh": {
                "seed": {"strategy": "replace"},
                "incremental": {
                    "strategy": "merge",
                    "watermark": {"source": "src", "column": "_extract_ts"},
                    "naturalKey": ["k"],
                },
            },
            "outputSchema": {
                "columns": [
                    {"name": "k", "type": "string", "nullable": False, "pii": "none"}
                ]
            },
        },
    )
    # SQL lives in the BASE pack, NOT under the overlay directory.
    (base_root / "silver" / "dim_thing.sql").write_text("SELECT 1")

    # Overlay pack with NO override on dim_thing (inheritance only).
    overlay_root = tmp_path / "overlay-pack"
    overlay_root.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        overlay_root / "pack.yaml",
        {
            "id": "overlay-pack",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "extends": "base-pack@0.1.0",
        },
    )

    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    merged = merge_overlay(base, overlay)

    # The inherited node's source root must be the BASE root, not the overlay root.
    assert merged.root_for("silver/dim_thing") == base_root.resolve()

    # And the SQL validator must resolve the file successfully.
    errors = validate_sql_paths(merged)
    assert not [e for e in errors if e.code == AIDPF_2003_SQL_FILE_MISSING], (
        f"Inherited base SQL should resolve against base root; got errors: {errors!r}"
    )


def test_pack_hash_deterministic(tmp_path: Path) -> None:
    base_root = _make_base_pack(tmp_path)
    overlay_root = _make_overlay(
        tmp_path,
        "fusion-finance-starter@0.1.0",
        extra={
            "columnAliases": {
                "invoice_currency_code": {
                    "appliesTo": "bronze.ap_invoices",
                    "required": True,
                    "candidates": ["APInvoicesCurrencyCode", "inherit"],
                }
            }
        },
    )
    base = load_pack(base_root)
    overlay = load_pack(overlay_root)
    h1 = merge_overlay(base, overlay).compute_hash()
    h2 = merge_overlay(base, overlay).compute_hash()
    assert h1 == h2
    # And differs from the base alone.
    assert h1 != base.compute_hash()
