"""Snapshot tests for the fusion-finance-starter content pack.

Loads the shipped pack via `load_pack`, asserts:

* Schema validation passes (Steps 2-4).
* Content validators report no errors (Step 6).
* Pack matches v1 registry parity (dependencies, natural keys,
  incremental_capable) per tests/fixtures/v1_registry_snapshot.yaml.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_validators import (
    validate_pack_full,
)

# Resolve the starter pack relative to the package, so the test works in
# both editable and installed modes.
import oracle_ai_data_platform_fusion_autopilot as _pkg

STARTER_PACK_ROOT = (
    Path(_pkg.__file__).parent / "content_packs" / "fusion-finance-starter"
)

PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent
V1_SNAPSHOT_PATH = PLUGIN_ROOT / "tests" / "fixtures" / "v1_registry_snapshot.yaml"


def test_starter_pack_loads() -> None:
    pack = load_pack(STARTER_PACK_ROOT)
    assert pack.pack.id == "fusion-finance-starter"
    assert pack.pack.version == "0.1.0"
    # Expected nodes (per v1 snapshot).
    assert set(pack.silver) == {"dim_supplier", "dim_account", "dim_calendar"}
    assert set(pack.gold) == {"gl_balance", "supplier_spend", "ap_aging"}
    assert set(pack.dashboards) == {"executive_cfo", "payables"}


def test_starter_pack_passes_full_validation() -> None:
    pack = load_pack(STARTER_PACK_ROOT)
    report = validate_pack_full(pack)
    assert report.ok, (
        "Starter pack failed validation:\n"
        + "\n".join(f"  - {e.code} [{e.location}]: {e.message}" for e in report.errors)
    )


def test_starter_pack_natural_keys_match_v1_snapshot() -> None:
    """Per Phase 1 acceptance: starter pack natural keys match v1 parity baseline."""
    pack = load_pack(STARTER_PACK_ROOT)
    snapshot = yaml.safe_load(V1_SNAPSHOT_PATH.read_text())

    # Silver dims
    for dim_id, dim_meta in snapshot["silver_dim_metadata"].items():
        node = pack.silver.get(dim_id)
        assert node is not None, f"silver dim {dim_id} missing from starter pack"
        v1_nk = dim_meta["natural_key"]
        if not v1_nk:  # dim_calendar has empty natural_key in v1
            continue
        v1_keys = [v1_nk] if isinstance(v1_nk, str) else list(v1_nk)
        assert node.refresh.incremental is not None, f"{dim_id} missing incremental block"
        assert list(node.refresh.incremental.natural_key) == v1_keys, (
            f"{dim_id} natural_key drift: pack={node.refresh.incremental.natural_key!r} "
            f"v1={v1_keys!r}"
        )

    # Gold marts
    for mart_id, mart_meta in snapshot["gold_mart_metadata"].items():
        node = pack.gold.get(mart_id)
        assert node is not None, f"gold mart {mart_id} missing from starter pack"
        v1_inc = mart_meta["incremental_capable"]
        if v1_inc:
            # v1 mart used row-level merge: starter should have an incremental
            # block with naturalKey matching v1.
            assert node.refresh.incremental is not None, (
                f"{mart_id} marked incremental_capable=True in v1 but starter has no incremental block"
            )
            v1_nk = mart_meta["natural_key"]
            v1_keys = [v1_nk] if isinstance(v1_nk, str) else list(v1_nk)
            assert list(node.refresh.incremental.natural_key) == v1_keys, (
                f"{mart_id} natural_key drift: pack={node.refresh.incremental.natural_key!r} "
                f"v1={v1_keys!r}"
            )
        else:
            # v1 mart was incremental_capable=False → starter should use
            # `replace` strategy on incremental block (or no incremental).
            if node.refresh.incremental is not None:
                assert node.refresh.incremental.strategy == "replace", (
                    f"{mart_id} was incremental_capable=False in v1 but starter "
                    f"declares `{node.refresh.incremental.strategy}`"
                )


def test_starter_pack_bronze_datasets_match_v1_snapshot() -> None:
    """Phase 1 ships a subset of v1 bronze datasets — the ones the shipped
    silver/gold actually depend on.

    Verify that subset is consistent: every bronze dataset declared in
    starter pack `bronze.yaml` MUST exist in the v1 snapshot.
    """
    pack = load_pack(STARTER_PACK_ROOT)
    snapshot = yaml.safe_load(V1_SNAPSHOT_PATH.read_text())
    v1_bronze = set(snapshot["bronze_extract_metadata"])
    pack_bronze = {ds["id"] for ds in pack.bronze_yaml.get("datasets", [])}
    missing_in_v1 = pack_bronze - v1_bronze
    assert not missing_in_v1, (
        f"Starter pack declares bronze datasets not in v1 snapshot: "
        f"{sorted(missing_in_v1)!r}. Either add them to the v1 snapshot or "
        "remove them from starter pack bronze.yaml."
    )
