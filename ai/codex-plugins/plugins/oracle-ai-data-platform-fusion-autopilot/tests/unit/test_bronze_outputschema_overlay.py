"""Unit tests for the bronze ``outputSchema`` overlay feature.

Covers both mechanisms (the ``overrides:`` block and the same-id bronze file),
their fail-closed guards, the bronze-only scope, provenance, and chain_roots
accumulation. Uses tmp_path fixture packs so the loader + merge run end-to-end.
"""

from __future__ import annotations

import copy
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    AIDPF_2001,
    OrphanOverrideError,
    PackLoaderError,
    load_full_chain,
    load_pack,
    merge_overlay,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_staging import (
    materialize_staged_pack,
    stage_pack_files,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
    validate_sql_paths,
)


# ---------------------------------------------------------------------------
# Fixture builders
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
            {"name": "VENDORID", "type": "decimal(38,30)", "nullable": True, "pii": "low"},
            {"name": "PARTYID", "type": "decimal(38,30)", "nullable": True, "pii": "low"},
            {"name": "LASTUPDATEDATE", "type": "timestamp", "nullable": True, "pii": "none"},
            {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
            {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
        ]
    },
    "quality": {"tests": [{"type": "not_null", "columns": ["SEGMENT1"]}]},
}

_SILVER_SQL_NODE = {
    "id": "dim_supplier",
    "layer": "silver",
    "implementation": {"type": "sql", "sql": "silver/dim_supplier.sql"},
    "target": "dim_supplier",
    "dependsOn": {"bronze": [{"id": "erp_suppliers", "watermark": {"column": "_extract_ts"}}]},
    "refresh": {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
            "naturalKey": ["supplier_key"],
        },
    },
    "outputSchema": {
        "columns": [
            {"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"},
        ]
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
    _write_yaml(pack_root / "silver" / "dim_supplier.yaml", copy.deepcopy(_SILVER_SQL_NODE))
    (pack_root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key\n")
    return pack_root


def _make_overlay(
    root: Path,
    *,
    name: str = "acme-finance",
    extends: str = "fusion-finance-starter@0.1.0",
    overrides: dict | None = None,
    bronze_files: dict[str, dict] | None = None,
    silver_files: dict[str, dict] | None = None,
    gold_files: dict[str, dict] | None = None,
) -> Path:
    overlay_root = root / name
    body = {
        "id": name,
        "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": extends,
    }
    if overrides:
        body["overrides"] = overrides
    _write_yaml(overlay_root / "pack.yaml", body)
    for fname, node in (bronze_files or {}).items():
        _write_yaml(overlay_root / "bronze" / fname, node)
    for fname, node in (silver_files or {}).items():
        _write_yaml(overlay_root / "silver" / fname, node)
    for fname, node in (gold_files or {}).items():
        _write_yaml(overlay_root / "gold" / fname, node)
    return overlay_root


def _merge(base_root: Path, overlay_root: Path):
    return merge_overlay(load_pack(base_root), load_pack(overlay_root))


def _cols(node) -> dict[str, str]:
    return {c.name: c.type for c in node.output_schema.columns}


# ---------------------------------------------------------------------------
# Block override path
# ---------------------------------------------------------------------------


def test_overrides_bronze_column_retype(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(
        tmp_path,
        overrides={"bronze/erp_suppliers": {"outputSchema": {"columns": [
            {"name": "VENDORID", "type": "decimal(18,0)"},
            {"name": "PARTYID", "type": "decimal(18,0)"},
        ]}}},
    )
    merged = _merge(base, ov)
    cols = _cols(merged.bronze["erp_suppliers"])
    assert cols["VENDORID"] == "decimal(18,0)"
    assert cols["PARTYID"] == "decimal(18,0)"


def test_bronze_override_partial_preserves_unmentioned_columns(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "outputSchema": {"columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    merged = _merge(base, ov)
    cols = _cols(merged.bronze["erp_suppliers"])
    # Untouched columns (incl. natural key + audit) preserved, in order.
    assert cols["SEGMENT1"] == "string"
    assert cols["LASTUPDATEDATE"] == "timestamp"
    assert cols["_extract_ts"] == "timestamp"
    assert list(cols) == ["SEGMENT1", "VENDORID", "PARTYID", "LASTUPDATEDATE",
                          "_extract_ts", "_run_id"]


def test_overrides_extend_columns(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "extendColumns": True,
        "columns": [{"name": "NEWCOL", "type": "string", "pii": "none"}]}}})
    merged = _merge(base, ov)
    cols = _cols(merged.bronze["erp_suppliers"])
    assert cols["NEWCOL"] == "string"
    assert len(cols) == 7


def test_extend_column_without_pii_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "extendColumns": True,
        "columns": [{"name": "NEWCOL", "type": "string"}]}}})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


def test_orphan_column_override_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "columns": [{"name": "NOPE", "type": "string", "pii": "none"}]}}})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


def test_name_only_override_column_rejected(tmp_path: Path) -> None:
    _make_base(tmp_path)
    # Pydantic rejects the name-only column at overlay load time.
    with pytest.raises(Exception) as exc:
        _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
            "outputSchema": {"columns": [{"name": "VENDORID"}]}}})
        load_pack(tmp_path / "acme-finance")
    assert "VENDORID" in str(exc.value) or AIDPF_2001 in str(exc.value)


def test_duplicate_column_name_rejected_overlay(tmp_path: Path) -> None:
    _make_base(tmp_path)
    with pytest.raises(Exception) as exc:
        ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
            "columns": [{"name": "VENDORID", "type": "int"},
                        {"name": "vendorid", "type": "int"}]}}})
        load_pack(ov)
    assert AIDPF_2001 in str(exc.value)


def test_block_override_required_columns_now_accepted(tmp_path: Path) -> None:
    # `requiredColumns` is no longer out of scope — the bronze-required-columns-
    # overlay feature added it as a supported additive override key. Parsing the
    # overlay pack must succeed (merge behavior is covered in
    # test_bronze_required_columns_overlay.py).
    _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
        "requiredColumns": {"erp_suppliers": ["X"]}}})
    pack = load_pack(ov)
    entry = pack.pack.overrides["bronze/erp_suppliers"]
    assert entry.required_columns == {"erp_suppliers": ["X"]}


def test_block_override_truly_unknown_key_rejected(tmp_path: Path) -> None:
    # A genuinely unsupported key still fails closed (AIDPF-2001).
    _make_base(tmp_path)
    with pytest.raises(Exception) as exc:
        ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {
            "grain": "row"}})
        load_pack(ov)
    assert AIDPF_2001 in str(exc.value)


def test_nonbronze_outputschema_override_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"silver/dim_supplier": {"outputSchema": {
        "columns": [{"name": "supplier_key", "type": "string"}]}}})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)
    assert "bronze-only" in str(exc.value)


# ---------------------------------------------------------------------------
# Provenance + chain_roots
# ---------------------------------------------------------------------------


def test_quality_only_override_on_sql_node_keeps_base_root(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"silver/dim_supplier": {
        "quality": {"tests": [{"type": "not_null", "columns": ["supplier_key"]}]}}})
    merged = _merge(base, ov)
    # Metadata-only override must NOT relocate the source root...
    assert merged.root_for("silver/dim_supplier") == load_pack(base).root
    # ...so the inherited SQL still resolves (no spurious AIDPF-2003).
    assert validate_sql_paths(merged) == []


def test_chain_roots_include_overlay_for_pure_metadata_overlay(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    merged = _merge(base, ov)
    assert merged.chain_roots == (load_pack(base).root, load_pack(ov).root)


def test_chain_roots_preserve_multi_overlay_chain(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    o1 = _make_overlay(tmp_path, name="o1", extends="fusion-finance-starter@0.1.0",
                       overrides={"bronze/erp_suppliers": {"outputSchema": {
                           "columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    o2 = _make_overlay(tmp_path, name="o2", extends="o1@0.1.0",
                       overrides={"bronze/erp_suppliers": {"outputSchema": {
                           "columns": [{"name": "PARTYID", "type": "decimal(18,0)"}]}}})
    merged = merge_overlay(merge_overlay(load_pack(base), load_pack(o1)), load_pack(o2))
    assert merged.chain_roots == (load_pack(base).root, load_pack(o1).root, load_pack(o2).root)
    # Both overrides survive (no layer dropped).
    cols = _cols(merged.bronze["erp_suppliers"])
    assert cols["VENDORID"] == "decimal(18,0)"
    assert cols["PARTYID"] == "decimal(18,0)"


def test_bronze_override_shifts_plan_hash(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    merged = _merge(base, ov)
    assert merged.compute_hash() != load_pack(base).compute_hash()


def test_block_and_file_produce_identical_merged_node(tmp_path: Path) -> None:
    # Block path.
    b1 = _make_base(tmp_path / "a")
    ov_block = _make_overlay(tmp_path / "a", overrides={"bronze/erp_suppliers": {
        "outputSchema": {"columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    merged_block = _merge(b1, ov_block)
    # File path: full node redeclared with VENDORID retyped.
    b2 = _make_base(tmp_path / "b")
    file_node = copy.deepcopy(_BRONZE_NODE)
    for c in file_node["outputSchema"]["columns"]:
        if c["name"] == "VENDORID":
            c["type"] = "decimal(18,0)"
    ov_file = _make_overlay(tmp_path / "b", bronze_files={"erp_suppliers.yaml": file_node})
    merged_file = _merge(b2, ov_file)
    assert _cols(merged_block.bronze["erp_suppliers"]) == _cols(merged_file.bronze["erp_suppliers"])


# ---------------------------------------------------------------------------
# Same-id file replacement path
# ---------------------------------------------------------------------------


def _file_node(**changes) -> dict:
    node = copy.deepcopy(_BRONZE_NODE)
    return node


def test_same_id_file_replaces_base_node(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    for c in node["outputSchema"]["columns"]:
        if c["name"] == "VENDORID":
            c["type"] = "decimal(18,0)"
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    merged = _merge(base, ov)
    assert _cols(merged.bronze["erp_suppliers"])["VENDORID"] == "decimal(18,0)"


def test_same_id_file_preserves_unmentioned_via_full_redeclare(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    for c in node["outputSchema"]["columns"]:
        if c["name"] == "VENDORID":
            c["type"] = "decimal(18,0)"
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    cols = _cols(_merge(base, ov).bronze["erp_suppliers"])
    assert cols["SEGMENT1"] == "string" and cols["_extract_ts"] == "timestamp"


def test_same_id_file_provenance_points_to_overlay(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    merged = _merge(base, ov)
    assert merged.root_for("bronze/erp_suppliers") == load_pack(ov).root


@pytest.mark.parametrize("mutate,label", [
    (lambda n: n["implementation"].__setitem__("datastore", "Other.PVO"), "pvo"),
    (lambda n: n["refresh"]["incremental"].__setitem__("naturalKey", ["VENDORID"]), "natural_key"),
    (lambda n: n.__setitem__("target", "erp_suppliers_x"), "target"),
    # `requiredColumns` is no longer an identity field — it is add-only-mutable
    # via the bronze-required-columns-overlay feature. A *drop* via same-id file
    # raises AIDPF-2062 (tested in test_bronze_required_columns_overlay.py).
    (lambda n: n["implementation"].__setitem__("biccSchema", "HCM"), "bicc_schema"),
])
def test_same_id_file_changing_identity_field_rejected(tmp_path, mutate, label) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    mutate(node)
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


def test_same_id_file_dropping_base_output_schema_column_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["outputSchema"]["columns"] = [c for c in node["outputSchema"]["columns"]
                                       if c["name"] != "PARTYID"]
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value) and "PARTYID" in str(exc.value)


def test_same_id_file_dropping_audit_column_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["outputSchema"]["columns"] = [c for c in node["outputSchema"]["columns"]
                                       if c["name"] != "_extract_ts"]
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert "_extract_ts" in str(exc.value)


def test_same_id_file_appending_output_schema_column_requires_pii(tmp_path: Path) -> None:
    # A NodeYaml column without pii is invalid at load time → cannot even append unclassified.
    _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["outputSchema"]["columns"].append({"name": "NEWCOL", "type": "string"})
    ov_root = tmp_path / "acme-finance"
    with pytest.raises(Exception):
        _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
        load_pack(ov_root)


def test_same_id_file_dropping_base_quality_test_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["quality"]["tests"] = []  # drop the base not_null test
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


def test_same_id_file_adding_quality_test_allowed(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["quality"]["tests"].append({"type": "unique", "columns": ["SEGMENT1"]})
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    merged = _merge(base, ov)
    assert len(merged.bronze["erp_suppliers"].quality.tests) == 2


def test_same_id_file_stem_id_mismatch_rejected(tmp_path: Path) -> None:
    _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    node["id"] = "erp_suppliers_fix"  # filename will be erp_suppliers.yaml
    ov = _make_overlay(tmp_path, bronze_files={"erp_suppliers.yaml": node})
    with pytest.raises(PackLoaderError) as exc:
        load_pack(ov)
    assert AIDPF_2001 in str(exc.value)


def test_same_id_silver_gold_file_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_SILVER_SQL_NODE)
    ov = _make_overlay(tmp_path, silver_files={"dim_supplier.yaml": node})
    # base ships dim_supplier.sql so the overlay node validates; replacement is rejected.
    (tmp_path / "acme-finance" / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key\n")
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


# ---------------------------------------------------------------------------
# Step 5 regression: the merged (overridden) type reaches the AIDPF-4070 gate
# ---------------------------------------------------------------------------


def _describe_mock(rows: list[tuple[str, str]]):
    spark = MagicMock()
    df = MagicMock()
    df.collect.return_value = [(n, t, None) for n, t in rows]
    spark.sql.return_value = df
    return spark


def _merged_retyped_node(tmp_path: Path):
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    return _merge(base, ov).bronze["erp_suppliers"]


def test_merged_override_type_passes_4070_gate(tmp_path: Path) -> None:
    """The 4070 post-write gate accepts a materialised table that matches the
    overridden decimal(18,0) — i.e. the override flows into the gate."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
        _assert_materialized_matches_declared,
    )

    node = _merged_retyped_node(tmp_path)
    declared = {c.name: c.type for c in node.output_schema.columns}
    # Materialised exactly matches the merged/overridden declared schema.
    spark = _describe_mock(list(declared.items()))
    # subset=True (bronze) — no raise.
    _assert_materialized_matches_declared(spark, "cat.bronze.erp_suppliers", node, subset=True)


def test_merged_override_type_enforced_by_4070_gate(tmp_path: Path) -> None:
    """If the live/materialised VENDORID is still decimal(38,30) (un-overridden),
    the gate fails against the *overridden* decimal(18,0) declaration."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
        _assert_materialized_matches_declared,
        MaterializedSchemaDriftError,
    )

    node = _merged_retyped_node(tmp_path)
    rows = [(c.name, c.type) for c in node.output_schema.columns]
    rows = [("VENDORID", "decimal(38,30)") if n == "VENDORID" else (n, t) for n, t in rows]
    spark = _describe_mock(rows)
    with pytest.raises(MaterializedSchemaDriftError):
        _assert_materialized_matches_declared(spark, "cat.bronze.erp_suppliers", node, subset=True)


def test_file_and_block_for_same_node_conflict_rejected(tmp_path: Path) -> None:
    base = _make_base(tmp_path)
    node = copy.deepcopy(_BRONZE_NODE)
    ov = _make_overlay(
        tmp_path,
        overrides={"bronze/erp_suppliers": {"outputSchema": {
            "columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}},
        bronze_files={"erp_suppliers.yaml": node},
    )
    with pytest.raises(OrphanOverrideError) as exc:
        _merge(base, ov)
    assert AIDPF_2001 in str(exc.value)


# ---------------------------------------------------------------------------
# Step 5b: staging roundtrip — the override must survive stage -> reconstruct
# ---------------------------------------------------------------------------


def test_staging_roundtrip_preserves_pure_output_schema_overlay(tmp_path: Path) -> None:
    """A pure-outputSchema bronze overlay (owns no artifact file) must still be
    staged so the cluster-side reconstruction re-applies the override."""
    base = _make_base(tmp_path)
    ov = _make_overlay(tmp_path, overrides={"bronze/erp_suppliers": {"outputSchema": {
        "columns": [{"name": "VENDORID", "type": "decimal(18,0)"}]}}})
    merged = merge_overlay(load_pack(base), load_pack(ov))
    files, manifest = stage_pack_files(merged)
    top_root, resolver = materialize_staged_pack(files, manifest)
    reconstructed = load_full_chain(top_root, base_resolver=resolver)
    assert _cols(reconstructed.bronze["erp_suppliers"])["VENDORID"] == "decimal(18,0)"
