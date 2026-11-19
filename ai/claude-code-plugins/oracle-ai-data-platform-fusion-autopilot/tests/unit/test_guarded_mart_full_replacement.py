"""Unit tests for guarded same-id silver/gold full replacement (``replaceNode``).

Covers the merge-time gate (AIDPF-2001 shape errors, AIDPF-2064 fork-base drift,
AIDPF-2065 identity change) and the two profile-independent fork fingerprints.
Uses tmp_path fixture packs so the loader + merge run end-to-end.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    AIDPF_2001,
    AIDPF_2064_FORK_BASE_DRIFT,
    AIDPF_2065_REPLACE_NODE_IDENTITY,
    ForkBaseDriftError,
    OrphanOverrideError,
    ReplaceNodeIdentityError,
    _normalize_depends_on_edges,
    _split_override_key,
    load_pack,
    merge_overlay,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
    compute_contract_fingerprint,
    compute_fork_fingerprint,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BRONZE_NODE = {
    "id": "erp_suppliers",
    "layer": "bronze",
    "implementation": {
        "type": "bronze_extract",
        "datastore": "Fscm.PozBiccExtractAM.SupplierExtractPVO",
        "biccSchema": "Financial",
        "incrementalCapable": True,
        "auditColumnsMode": "bronze_v1",
    },
    "target": "erp_suppliers",
    "dependsOn": {"bronze": [], "silver": []},
    "refresh": {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "watermark": {"source": "erp_suppliers", "column": "LASTUPDATEDATE"},
            "naturalKey": ["SEGMENT1"],
        },
    },
    "requiredColumns": {"erp_suppliers": ["SEGMENT1", "VENDORID"]},
    "outputSchema": {
        "columns": [
            {"name": "SEGMENT1", "type": "string", "nullable": True, "pii": "low"},
            {"name": "VENDORID", "type": "decimal(18,0)", "nullable": True, "pii": "low"},
            {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
            {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
        ]
    },
    "quality": {"tests": [{"type": "not_null", "columns": ["SEGMENT1"]}]},
}

_SILVER_NODE = {
    "id": "dim_supplier",
    "layer": "silver",
    "implementation": {"type": "sql", "sql": "silver/dim_supplier.sql"},
    "target": "dim_supplier",
    "dependsOn": {
        "bronze": [{"id": "erp_suppliers", "watermark": {"column": "_extract_ts"}}]
    },
    "refresh": {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
            "naturalKey": ["supplier_key"],
        },
    },
    "requiredColumns": {"erp_suppliers": ["SEGMENT1"]},
    "outputSchema": {
        "columns": [
            {"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"},
        ]
    },
    "quality": {"tests": []},
}

_BASE_SQL = "SELECT 1 AS supplier_key\n"


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def _make_base(root: Path) -> Path:
    pack_root = root / "fusion-finance-starter"
    _write_yaml(
        pack_root / "pack.yaml",
        {
            "id": "fusion-finance-starter",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        },
    )
    _write_yaml(pack_root / "bronze" / "erp_suppliers.yaml", copy.deepcopy(_BRONZE_NODE))
    _write_yaml(pack_root / "silver" / "dim_supplier.yaml", copy.deepcopy(_SILVER_NODE))
    (pack_root / "silver" / "dim_supplier.sql").write_text(_BASE_SQL)
    return pack_root


def _stamps(base_root: Path, node_id: str = "dim_supplier") -> dict:
    """Compute correct forkedFrom stamps against the current base."""
    base = load_pack(base_root)
    node = base.silver[node_id]
    return {
        "sqlSha256": compute_fork_fingerprint(node, base),
        "contractSha256": compute_contract_fingerprint(node),
        "packVersion": base.pack.version,
    }


def _make_replace_overlay(
    root: Path,
    *,
    stamps: dict,
    node: dict | None = None,
    sql_text: str = "SELECT 2 AS supplier_key\n",
    reason: str = "Rewrote the supplier rollup; see TICKET-123.",
    key: str = "silver/dim_supplier",
    write_file: bool = True,
    name: str = "acme-finance",
) -> Path:
    overlay_root = root / name
    body = {
        "id": name,
        "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "fusion-finance-starter@0.1.0",
        "overrides": {key: {"replaceNode": {"reason": reason, "forkedFrom": stamps}}},
    }
    _write_yaml(overlay_root / "pack.yaml", body)
    if write_file:
        repl = copy.deepcopy(node if node is not None else _SILVER_NODE)
        _write_yaml(overlay_root / "silver" / "dim_supplier.yaml", repl)
        (overlay_root / "silver" / "dim_supplier.sql").write_text(sql_text)
    return overlay_root


def _merge(base_root: Path, overlay_root: Path):
    return merge_overlay(load_pack(base_root), load_pack(overlay_root))


# ---------------------------------------------------------------------------
# Happy path + acceptance
# ---------------------------------------------------------------------------


def test_replace_node_happy_path(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_replace_overlay(tmp_path, stamps=_stamps(base))
    merged = _merge(base, ov)
    # The node is replaced and its source root points at the overlay (new SQL).
    assert "dim_supplier" in merged.silver
    assert merged.source_roots["silver/dim_supplier"] == ov


def test_add_column_round_trip(tmp_path: Path) -> None:
    """Adding a column goes through replaceNode (no separate additive path)."""
    base = _make_base(tmp_path)
    node = copy.deepcopy(_SILVER_NODE)
    node["outputSchema"]["columns"].append(
        {"name": "supplier_tier", "type": "string", "nullable": True, "pii": "none"}
    )
    ov = _make_replace_overlay(
        tmp_path,
        stamps=_stamps(base),
        node=node,
        sql_text="SELECT 1 AS supplier_key, 'A' AS supplier_tier\n",
    )
    merged = _merge(base, ov)
    cols = [c.name for c in merged.silver["dim_supplier"].output_schema.columns]
    assert "supplier_tier" in cols


def test_bare_same_id_file_rejected(tmp_path: Path) -> None:
    """A same-id silver file with NO replaceNode block stays AIDPF-2001."""
    base = _make_base(tmp_path)
    overlay_root = tmp_path / "acme-finance"
    _write_yaml(
        overlay_root / "pack.yaml",
        {
            "id": "acme-finance",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "extends": "fusion-finance-starter@0.1.0",
        },
    )
    _write_yaml(overlay_root / "silver" / "dim_supplier.yaml", copy.deepcopy(_SILVER_NODE))
    (overlay_root / "silver" / "dim_supplier.sql").write_text("SELECT 9 AS supplier_key\n")
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, overlay_root)
    assert AIDPF_2001 in str(exc.value)


# ---------------------------------------------------------------------------
# AIDPF-2064 — fork-base drift (logic + contract variants)
# ---------------------------------------------------------------------------


def test_stale_sql_stamp_trips_2047(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    stamps = _stamps(base)
    stamps["sqlSha256"] = "0" * 64  # stale logic fingerprint
    ov = _make_replace_overlay(tmp_path, stamps=stamps)
    with pytest.raises(ForkBaseDriftError) as exc:
        _merge(base, ov)
    assert AIDPF_2064_FORK_BASE_DRIFT in str(exc.value)
    assert "logic" in str(exc.value)


def test_base_sql_edit_trips_2047(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    stamps = _stamps(base)
    ov = _make_replace_overlay(tmp_path, stamps=stamps)
    # Edit the base SQL *after* the fork was stamped.
    (base / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key -- fixed\n")
    with pytest.raises(ForkBaseDriftError) as exc:
        _merge(base, ov)
    assert AIDPF_2064_FORK_BASE_DRIFT in str(exc.value)


def test_base_contract_pii_edit_trips_2047(tmp_path: Path) -> None:
    """A base YAML-only change (PII reclassification) trips the contract variant."""
    base = _make_base(tmp_path)
    stamps = _stamps(base)
    ov = _make_replace_overlay(tmp_path, stamps=stamps)
    # Reclassify the base column's PII — SQL untouched, only the contract changes.
    node = copy.deepcopy(_SILVER_NODE)
    node["outputSchema"]["columns"][0]["pii"] = "high"
    _write_yaml(base / "silver" / "dim_supplier.yaml", node)
    with pytest.raises(ForkBaseDriftError) as exc:
        _merge(base, ov)
    assert AIDPF_2064_FORK_BASE_DRIFT in str(exc.value)
    assert "contract" in str(exc.value)


# ---------------------------------------------------------------------------
# AIDPF-2065 — identity change
# ---------------------------------------------------------------------------


def test_target_change_trips_2048(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_SILVER_NODE)
    node["target"] = "dim_supplier_v2"
    ov = _make_replace_overlay(tmp_path, stamps=_stamps(base), node=node)
    with pytest.raises(ReplaceNodeIdentityError) as exc:
        _merge(base, ov)
    assert AIDPF_2065_REPLACE_NODE_IDENTITY in str(exc.value)


def test_refresh_change_trips_2048(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_SILVER_NODE)
    node["refresh"]["incremental"]["naturalKey"] = ["supplier_key", "extra_key"]
    ov = _make_replace_overlay(tmp_path, stamps=_stamps(base), node=node)
    with pytest.raises(ReplaceNodeIdentityError) as exc:
        _merge(base, ov)
    assert AIDPF_2065_REPLACE_NODE_IDENTITY in str(exc.value)


def test_depends_on_normalization_is_layer_aware() -> None:
    """The edge tuple includes `layer`, so the same id under bronze vs silver is a
    different edge — a `silver/foo` move can't be mistaken for `bronze/foo`."""
    from types import SimpleNamespace

    from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
        DependsOn,
        SourceRef,
        WatermarkSpec,
    )

    wm = WatermarkSpec(column="_extract_ts")
    on_bronze = SimpleNamespace(
        depends_on=DependsOn(bronze=[SourceRef(id="foo", watermark=wm)])
    )
    on_silver = SimpleNamespace(
        depends_on=DependsOn(silver=[SourceRef(id="foo", watermark=wm)])
    )
    assert _normalize_depends_on_edges(on_bronze) != _normalize_depends_on_edges(
        on_silver
    )
    # A pure reorder is NOT a change.
    reordered = SimpleNamespace(
        depends_on=DependsOn(
            bronze=[SourceRef(id="b", watermark=wm), SourceRef(id="a", watermark=wm)]
        )
    )
    original = SimpleNamespace(
        depends_on=DependsOn(
            bronze=[SourceRef(id="a", watermark=wm), SourceRef(id="b", watermark=wm)]
        )
    )
    assert _normalize_depends_on_edges(reordered) == _normalize_depends_on_edges(
        original
    )


# ---------------------------------------------------------------------------
# AIDPF-2001 — replaceNode shape errors
# ---------------------------------------------------------------------------


def test_replace_node_missing_file_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_replace_overlay(tmp_path, stamps=_stamps(base), write_file=False)
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)
    assert "no matching" in str(exc.value)


def test_replace_node_bronze_key_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    # A bronze-prefixed replaceNode key is not allowed (silver/gold only).
    ov = _make_replace_overlay(tmp_path, stamps=_stamps(base), key="bronze/erp_suppliers")
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


def test_replace_node_non_shipped_id_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    overlay_root = tmp_path / "acme-finance"
    body = {
        "id": "acme-finance",
        "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "fusion-finance-starter@0.1.0",
        "overrides": {
            "silver/dim_unknown": {
                "replaceNode": {"reason": "x", "forkedFrom": _stamps(base)}
            }
        },
    }
    _write_yaml(overlay_root / "pack.yaml", body)
    node = copy.deepcopy(_SILVER_NODE)
    node["id"] = "dim_unknown"
    node["target"] = "dim_unknown"
    _write_yaml(overlay_root / "silver" / "dim_unknown.yaml", node)
    (overlay_root / "silver" / "dim_unknown.sql").write_text("SELECT 1 AS supplier_key\n")
    # A brand-new id is a plain addition, not a replacement → the replaceNode
    # block on a non-shipped id is rejected.
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, overlay_root)
    assert AIDPF_2001 in str(exc.value)


# ---------------------------------------------------------------------------
# Fingerprint helpers (profile-independent, semantic-aware)
# ---------------------------------------------------------------------------


def test_fork_fingerprint_stable_and_sql_sensitive(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    p1 = load_pack(base)
    h1 = compute_fork_fingerprint(p1.silver["dim_supplier"], p1)
    # Reload → identical (deterministic).
    p2 = load_pack(base)
    assert compute_fork_fingerprint(p2.silver["dim_supplier"], p2) == h1
    # Cosmetic whitespace doesn't move the hash; a real text change does.
    (base / "silver" / "dim_supplier.sql").write_text("SELECT   1   AS   supplier_key\n")
    p3 = load_pack(base)
    assert compute_fork_fingerprint(p3.silver["dim_supplier"], p3) == h1
    (base / "silver" / "dim_supplier.sql").write_text("SELECT 2 AS supplier_key\n")
    p4 = load_pack(base)
    assert compute_fork_fingerprint(p4.silver["dim_supplier"], p4) != h1


def _make_semantic_base(root: Path, *, fragment: str = "ACTIVE_FLAG = 'Y'") -> Path:
    """A base whose dim_supplier.sql references `{{ semantic.active }}`, declared in
    pack.yaml's semanticVariants — the pack-owned render input the raw-SQL hash
    alone would miss."""
    pack_root = root / "fusion-finance-starter"
    _write_yaml(
        pack_root / "pack.yaml",
        {
            "id": "fusion-finance-starter",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "semanticVariants": {
                "active": {
                    "appliesTo": "bronze.erp_suppliers",
                    "required": False,
                    "candidates": [
                        {
                            "id": "by_flag",
                            "detect": {"columnExists": "ACTIVE_FLAG"},
                            "fragment": fragment,
                        }
                    ],
                }
            },
        },
    )
    _write_yaml(pack_root / "bronze" / "erp_suppliers.yaml", copy.deepcopy(_BRONZE_NODE))
    _write_yaml(pack_root / "silver" / "dim_supplier.yaml", copy.deepcopy(_SILVER_NODE))
    (pack_root / "silver" / "dim_supplier.sql").write_text(
        "SELECT 1 AS supplier_key WHERE {{ semantic.active }}\n"
    )
    return pack_root


def test_semantic_fragment_drift_changes_fork_fingerprint(tmp_path: Path) -> None:
    """Editing only a referenced semantic candidate fragment (SQL file untouched)
    must move compute_fork_fingerprint — the pack-owned-render-input guard."""
    base = _make_semantic_base(tmp_path, fragment="ACTIVE_FLAG = 'Y'")
    p1 = load_pack(base)
    h1 = compute_fork_fingerprint(p1.silver["dim_supplier"], p1)
    # Rewrite ONLY the candidate fragment in pack.yaml; leave dim_supplier.sql alone.
    _make_semantic_base(tmp_path, fragment="ACTIVE_FLAG = 'A'")
    p2 = load_pack(base)
    assert compute_fork_fingerprint(p2.silver["dim_supplier"], p2) != h1


def test_semantic_fragment_drift_trips_2064_via_merge(tmp_path: Path) -> None:
    base = _make_semantic_base(tmp_path, fragment="ACTIVE_FLAG = 'Y'")
    ov = _make_replace_overlay(tmp_path, stamps=_stamps(base))
    # Stamped against the current base → merges clean.
    assert "dim_supplier" in _merge(base, ov).silver
    # Edit ONLY the base semantic fragment → fork is now stale on the logic side.
    _make_semantic_base(tmp_path, fragment="ACTIVE_FLAG = 'A'")
    with pytest.raises(ForkBaseDriftError) as exc:
        _merge(base, ov)
    assert AIDPF_2064_FORK_BASE_DRIFT in str(exc.value)


def test_contract_fingerprint_pii_sensitive(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = load_pack(base).silver["dim_supplier"]
    h1 = compute_contract_fingerprint(node)
    node2 = copy.deepcopy(_SILVER_NODE)
    node2["outputSchema"]["columns"][0]["pii"] = "high"
    _write_yaml(base / "silver" / "dim_supplier.yaml", node2)
    h2 = compute_contract_fingerprint(load_pack(base).silver["dim_supplier"])
    assert h1 != h2


# ---------------------------------------------------------------------------
# Pure-unit helpers
# ---------------------------------------------------------------------------


def test_split_override_key() -> None:
    assert _split_override_key("silver/foo") == ("silver", "foo")
    assert _split_override_key("gold/bar") == ("gold", "bar")
    assert _split_override_key("bronze/baz") == ("bronze", "baz")
    assert _split_override_key("plain") == (None, "plain")


# ---------------------------------------------------------------------------
# Builtin fail-closed
# ---------------------------------------------------------------------------

_BUILTIN_NODE = {
    "id": "dim_calendar",
    "layer": "silver",
    "implementation": {
        "type": "builtin",
        "callable": "oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar:build",
    },
    "target": "dim_calendar",
    "dependsOn": {"bronze": [], "silver": []},
    "refresh": {"seed": {"strategy": "replace"}},
    "outputSchema": {
        "columns": [
            {"name": "calendar_key", "type": "bigint", "nullable": False, "pii": "none"},
        ]
    },
    "quality": {"tests": []},
}


def test_replace_node_on_builtin_fails_closed(tmp_path: Path) -> None:
    """replaceNode against a builtin (non-SQL) base mart → actionable AIDPF-2001."""
    base = _make_base(tmp_path)
    _write_yaml(base / "silver" / "dim_calendar.yaml", copy.deepcopy(_BUILTIN_NODE))
    # Overlay ships a replaceNode for the builtin node, with the matching file.
    overlay_root = tmp_path / "acme-finance"
    _write_yaml(
        overlay_root / "pack.yaml",
        {
            "id": "acme-finance",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "extends": "fusion-finance-starter@0.1.0",
            "overrides": {
                "silver/dim_calendar": {
                    "replaceNode": {
                        "reason": "x",
                        "forkedFrom": {
                            "sqlSha256": "a",
                            "contractSha256": "b",
                            "packVersion": "0.1.0",
                        },
                    }
                }
            },
        },
    )
    _write_yaml(overlay_root / "silver" / "dim_calendar.yaml", copy.deepcopy(_BUILTIN_NODE))
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, overlay_root)
    assert AIDPF_2001 in str(exc.value)
    assert "builtin" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# Schema-level acknowledgement guards (OverrideEntry / ReplaceNode)
# ---------------------------------------------------------------------------


def test_replace_node_mutually_exclusive() -> None:
    from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import OverrideEntry

    fk = {"sqlSha256": "a", "contractSha256": "b", "packVersion": "1"}
    with pytest.raises(Exception) as exc:
        OverrideEntry.model_validate(
            {"replaceNode": {"reason": "x", "forkedFrom": fk}, "sql": "x.sql"}
        )
    assert AIDPF_2001 in str(exc.value)


def test_replace_node_blank_reason_rejected() -> None:
    from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import OverrideEntry

    fk = {"sqlSha256": "a", "contractSha256": "b", "packVersion": "1"}
    with pytest.raises(Exception) as exc:
        OverrideEntry.model_validate({"replaceNode": {"reason": "   ", "forkedFrom": fk}})
    assert AIDPF_2001 in str(exc.value)


def test_forked_from_blank_fingerprint_rejected() -> None:
    from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import OverrideEntry

    with pytest.raises(Exception) as exc:
        OverrideEntry.model_validate(
            {
                "replaceNode": {
                    "reason": "x",
                    "forkedFrom": {"sqlSha256": "  ", "contractSha256": "b", "packVersion": "1"},
                }
            }
        )
    assert AIDPF_2001 in str(exc.value)
