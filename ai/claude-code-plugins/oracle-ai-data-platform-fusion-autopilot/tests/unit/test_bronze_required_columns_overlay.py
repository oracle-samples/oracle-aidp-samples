"""Unit tests for the bronze ``requiredColumns`` overlay feature.

Covers both mechanisms (the ``overrides:`` block and the same-id bronze file),
the add/remove asymmetry (additive ``requiredColumns`` vs acknowledged
``relaxRequiredColumns``), the fail-closed guards (AIDPF-2062 same-id drop,
AIDPF-2063 orphan relaxation), the mandatory non-blank ``reason``, bronze-only
scope, the identical-merged-node invariant, and the wiring into the gate inputs.
Offline tmp_path fixture packs — loader + merge run end-to-end, no Spark.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    AIDPF_2001,
    AIDPF_2062_SAMEID_DROPS_REQUIRED_COLUMN,
    AIDPF_2063_RELAX_REQUIRED_COLUMN_ORPHAN,
    OrphanOverrideError,
    RelaxRequiredColumnOrphanError,
    RequiredColumnDropError,
    load_pack,
    merge_overlay,
)

# ---------------------------------------------------------------------------
# Fixture builders (mirror test_bronze_outputschema_overlay.py)
# ---------------------------------------------------------------------------

_BRONZE_NODE = {
    "id": "erp_suppliers",
    "layer": "bronze",
    "implementation": {
        "type": "bronze_extract",
        "datastore": "Fscm.PozBiccExtractAM.SupplierExtractPVO",
        "pvo_id": "Fscm.PozBiccExtractAM.SupplierExtractPVO",
        "biccSchema": "Financial",
        "incrementalCapable": True,
        "auditColumnsMode": "bronze_v1",
    },
    "target": "erp_suppliers",
    "dependsOn": {"bronze": [], "silver": []},
    "refresh": {"seed": {"strategy": "replace"}},
    "requiredColumns": {"erp_suppliers": ["SEGMENT1", "VENDORID"]},
    "outputSchema": {
        "columns": [
            {"name": "SEGMENT1", "type": "string", "nullable": True, "pii": "low"},
            {"name": "VENDORID", "type": "decimal(38,30)", "nullable": True, "pii": "low"},
            {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
            {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
        ]
    },
    "quality": {"tests": []},
}

_SILVER_NODE = {
    "id": "dim_supplier",
    "layer": "silver",
    "implementation": {"type": "sql", "sql": "silver/dim_supplier.sql"},
    "target": "dim_supplier",
    "dependsOn": {"bronze": [{"id": "erp_suppliers"}]},
    "refresh": {"seed": {"strategy": "replace"}},
    "requiredColumns": {"erp_suppliers": ["SEGMENT1"]},
    "outputSchema": {
        "columns": [{"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"}]
    },
    "quality": {"tests": []},
}


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
    (pack_root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key\n")
    return pack_root


def _make_overlay(
    root: Path,
    *,
    name: str = "acme-finance",
    overrides: dict | None = None,
    bronze_files: dict[str, dict] | None = None,
    column_aliases: dict | None = None,
) -> Path:
    overlay_root = root / name
    body = {
        "id": name,
        "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "fusion-finance-starter@0.1.0",
    }
    if overrides:
        body["overrides"] = overrides
    if column_aliases:
        body["columnAliases"] = column_aliases
    _write_yaml(overlay_root / "pack.yaml", body)
    for fname, node in (bronze_files or {}).items():
        _write_yaml(overlay_root / "bronze" / fname, node)
    return overlay_root


def _merge(base_root: Path, overlay_root: Path):
    return merge_overlay(load_pack(base_root), load_pack(overlay_root))


def _req(node) -> dict:
    return dict(node.required_columns)


# ---------------------------------------------------------------------------
# Adds
# ---------------------------------------------------------------------------


def test_block_add_unions_into_base(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={
        "bronze/erp_suppliers": {"requiredColumns": {"erp_suppliers": ["PARTYID", "VENDORID"]}}
    })
    merged = _merge(base, ov)
    # base [SEGMENT1, VENDORID] + add [PARTYID, VENDORID] → union, order-stable, deduped.
    assert _req(merged.bronze["erp_suppliers"]) == {"erp_suppliers": ["SEGMENT1", "VENDORID", "PARTYID"]}


def test_same_id_file_add_superset_allowed_and_identical_to_block(tmp_path: Path) -> None:
    # Same-id file with a superset requiredColumns is allowed (add-only) ...
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["requiredColumns"]["erp_suppliers"] = ["SEGMENT1", "VENDORID", "PARTYID"]
    ov_file = _make_overlay(tmp_path, name="ov-file", bronze_files={"erp_suppliers.yaml": node})
    merged_file = _merge(base, ov_file)

    # ... and produces an identical merged node to the block-add path.
    ov_block = _make_overlay(tmp_path, name="ov-block", overrides={
        "bronze/erp_suppliers": {"requiredColumns": {"erp_suppliers": ["PARTYID"]}}
    })
    merged_block = _merge(base, ov_block)

    assert (
        merged_file.bronze["erp_suppliers"].model_dump(by_alias=True)
        == merged_block.bronze["erp_suppliers"].model_dump(by_alias=True)
    )


def test_column_ref_add_survives_merge_verbatim(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(
        tmp_path,
        overrides={"bronze/erp_suppliers": {
            "requiredColumns": {"erp_suppliers": ["$column.supplier_extra"]}}},
        column_aliases={"supplier_extra": {
            "appliesTo": "bronze.erp_suppliers", "required": True, "candidates": ["EXTRACOL"]}},
    )
    merged = _merge(base, ov)
    assert "$column.supplier_extra" in merged.bronze["erp_suppliers"].required_columns["erp_suppliers"]


# ---------------------------------------------------------------------------
# Removal (acknowledged) + guards
# ---------------------------------------------------------------------------


def test_block_relax_with_reason_removes_column(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "relaxRequiredColumns": {"erp_suppliers": [
            {"column": "VENDORID", "reason": "tenant pod does not expose VENDORID"}]}}})
    merged = _merge(base, ov)
    assert _req(merged.bronze["erp_suppliers"]) == {"erp_suppliers": ["SEGMENT1"]}


@pytest.mark.parametrize("bad_reason", [None, "", "   ", "\t\n"])
def test_relax_blank_or_missing_reason_rejected(tmp_path: Path, bad_reason) -> None:
    base = _make_base(tmp_path)
    entry = {"column": "VENDORID"}
    if bad_reason is not None:
        entry["reason"] = bad_reason
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "relaxRequiredColumns": {"erp_suppliers": [entry]}}})
    # Schema validation fires when the overlay pack.yaml is parsed.
    with pytest.raises(Exception):
        load_pack(ov)


def test_relax_orphan_column_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "relaxRequiredColumns": {"erp_suppliers": [
            {"column": "NOTABASECOL", "reason": "x"}]}}})
    with pytest.raises(RelaxRequiredColumnOrphanError) as exc:
        _merge(base, ov)
    assert AIDPF_2063_RELAX_REQUIRED_COLUMN_ORPHAN in str(exc.value)


def test_same_id_file_drop_required_column_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["requiredColumns"]["erp_suppliers"] = ["SEGMENT1"]  # drops VENDORID
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    with pytest.raises(RequiredColumnDropError) as exc:
        _merge(base, ov)
    assert AIDPF_2062_SAMEID_DROPS_REQUIRED_COLUMN in str(exc.value)
    assert "VENDORID" in str(exc.value)


# ---------------------------------------------------------------------------
# Scope + mutual exclusion
# ---------------------------------------------------------------------------


def test_required_columns_override_on_silver_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"silver/dim_supplier": {
        "requiredColumns": {"erp_suppliers": ["PARTYID"]}}})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)
    assert "bronze-only" in str(exc.value)


def test_block_and_same_id_file_mutually_exclusive(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["requiredColumns"]["erp_suppliers"] = ["SEGMENT1", "VENDORID", "PARTYID"]
    ov = _make_overlay(
        tmp_path,
        overrides={"bronze/erp_suppliers": {"requiredColumns": {"erp_suppliers": ["PARTYID"]}}},
        bronze_files={"erp_suppliers.yaml": node},
    )
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


# ---------------------------------------------------------------------------
# Wiring + coupling + non-regression
# ---------------------------------------------------------------------------


def test_added_column_reaches_gate_input(tmp_path: Path) -> None:
    # A bronze node's own requiredColumns is read by node_preflight
    # (_check_required_columns) + the AIDPF-4071 batch source-schema gate, both of
    # which read node.required_columns. Proving the merged node carries the add is
    # the offline wiring proof; the live assertion is those gates' own coverage.
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={
        "bronze/erp_suppliers": {"requiredColumns": {"erp_suppliers": ["PARTYID"]}}})
    merged = _merge(base, ov)
    node = merged.bronze["erp_suppliers"]
    assert "PARTYID" in node.required_columns["erp_suppliers"]
    # And it resolves through the shared resolver the gates use.
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.required_column_resolver import (
        resolve_required_column_entries,
    )
    resolved = resolve_required_column_entries(
        node.required_columns["erp_suppliers"], resolved_pack=merged, tenant_profile=None
    )
    assert "PARTYID" in resolved


def test_extend_columns_and_required_columns_coupling(tmp_path: Path) -> None:
    # A column appended to outputSchema via extendColumns AND asserted via
    # requiredColumns in the same overlay → both present in the merged node.
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "outputSchema": {"extendColumns": True, "columns": [
            {"name": "PARTYID", "type": "decimal(18,0)", "pii": "low"}]},
        "requiredColumns": {"erp_suppliers": ["PARTYID"]},
    }})
    merged = _merge(base, ov)
    node = merged.bronze["erp_suppliers"]
    assert "PARTYID" in node.required_columns["erp_suppliers"]
    assert any(c.name == "PARTYID" for c in node.output_schema.columns)


def test_overlay_without_required_columns_change_is_non_regressive(tmp_path: Path) -> None:
    # An overlay that only touches outputSchema leaves requiredColumns untouched.
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "outputSchema": {"columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    merged = _merge(base, ov)
    assert _req(merged.bronze["erp_suppliers"]) == {"erp_suppliers": ["SEGMENT1", "VENDORID"]}
