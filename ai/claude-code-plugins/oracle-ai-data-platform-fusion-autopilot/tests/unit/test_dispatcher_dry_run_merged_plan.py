"""Phase 5 — content-pack dry-run merges bronze + silver/gold plan rows.

Companion to ``test_content_pack_dry_run_plan.py`` (silver/gold-only
builder) — this module covers the dispatcher-level fix: dry-run is
routed through the same scope-split classifier the real run uses, and
bronze ``PlanNode`` rows from the v1 spec list are merged with the
silver/gold rows from the resolved pack so the summary renderer shows
EVERY layer the run would touch.

The pre-fix dry-run bypassed the dispatcher entirely and listed only
silver/gold — which lied to operators about the bronze work the real
default-flipped run would do first.

Uses real registered bronze ids (``erp_suppliers``) so the v1
``resolve_plan`` round-trip succeeds without monkeypatching.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot import orchestrator
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
)


# --- Pack + profile + bundle authored inline (real bronze ids only) -------


_PACK_YAML = """\
id: dispatch-dry-run-merge-pack
version: 1.0.0
description: Phase 5 dispatcher dry-run merge pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

_BRONZE_NODE = """\
id: erp_suppliers
layer: bronze
implementation:
  type: bronze_extract
  datastore: SupplierExtractPVO
  biccSchema: Financial
target: erp_suppliers
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: erp_suppliers
      column: LASTUPDATEDATE
    naturalKey: [SEGMENT1]
outputSchema:
  columns:
    - name: SEGMENT1
      type: string
      nullable: true
      pii: low
    - name: _extract_ts
      type: timestamp
      nullable: false
      pii: none
    - name: _source_pvo
      type: string
      nullable: false
      pii: none
    - name: _run_id
      type: string
      nullable: false
      pii: none
    - name: _watermark_used
      type: timestamp
      nullable: true
      pii: none
"""

_SILVER_NODE = """\
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""

_GOLD_NODE = """\
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""

_PROFILE_YAML = """\
schemaVersion: 1
tenant: dry-run-merge-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:dry-run-merge-fixture"
resolved:
  column: {}
  semantic: {}
profile:
  calendar:
    fiscalStartMonth: 1
    startDate: "2024-01-01"
"""

_BUNDLE_YAML = """\
apiVersion: aidp-fusion-autopilot/v1
project: dry-run-merge
fusion:
  serviceUrl: https://example.com
  username: test
  password: test
  externalStorage: test-storage
aidp:
  catalog: dry_run_merge_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
datasets:
  - id: erp_suppliers
    mode: incremental
  # Phase 9 cross-layer datasets[]: declare silver + gold roots so
  # the bundle_scope picks them up. D-1 auto-pulls erp_suppliers
  # as a transitive bronze dep of dim_supplier.
  - id: dim_supplier
  - id: supplier_spend
dimensions:
  build: []
gold:
  marts: []
contentPack:
  name: dispatch-dry-run-merge-pack
  path: ./pack
  profile: dry-run-merge-tenant
"""


@pytest.fixture
def fixture(tmp_path: Path):
    """Build a self-contained content-pack bundle in tmp_path."""
    pack_root = tmp_path / "pack"
    bronze = pack_root / "bronze"
    silver = pack_root / "silver"
    gold = pack_root / "gold"
    bronze.mkdir(parents=True)
    silver.mkdir(parents=True)
    gold.mkdir(parents=True)
    (pack_root / "pack.yaml").write_text(_PACK_YAML, encoding="utf-8")
    (bronze / "erp_suppliers.yaml").write_text(_BRONZE_NODE, encoding="utf-8")
    (silver / "dim_supplier.yaml").write_text(_SILVER_NODE, encoding="utf-8")
    (silver / "dim_supplier.sql").write_text("SELECT 1\n", encoding="utf-8")
    (gold / "supplier_spend.yaml").write_text(_GOLD_NODE, encoding="utf-8")
    (gold / "supplier_spend.sql").write_text("SELECT 1\n", encoding="utf-8")

    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "dry-run-merge-tenant.yaml").write_text(
        _PROFILE_YAML, encoding="utf-8",
    )
    bundle_path = tmp_path / "bundle.yaml"
    bundle_path.write_text(_BUNDLE_YAML, encoding="utf-8")

    pack = load_full_chain(
        pack_root, base_resolver=make_filesystem_base_resolver(pack_root),
    )
    profile = load_tenant_profile(profiles / "dry-run-merge-tenant.yaml")
    return bundle_path, pack, profile


def _plan_ids_by_layer(summary) -> dict[str, list[str]]:
    by_layer: dict[str, list[str]] = {}
    for node in summary.plan:
        by_layer.setdefault(node.layer, []).append(node.dataset_id)
    return by_layer


class TestDispatcherDryRunMergedPlan:
    def test_no_filter_includes_bronze_AND_silver_gold(self, fixture) -> None:
        """A no-filter content-pack dry-run lists bronze + silver + gold
        — the same layers the real default-flipped run would touch."""
        bundle_path, pack, profile = fixture
        summary = orchestrator.run(
            bundle_path=bundle_path,
            resolved_pack=pack,
            tenant_profile=profile,
            dry_run=True,
        )
        assert summary.steps == ()
        by_layer = _plan_ids_by_layer(summary)
        assert "bronze" in by_layer, (
            f"no-filter dry-run is missing bronze rows; got layers="
            f"{list(by_layer)!r}. This is the lie the pre-fix path told "
            f"operators — bronze ran in reality but never showed in "
            f"--dry-run."
        )
        assert "erp_suppliers" in by_layer["bronze"]
        assert by_layer.get("silver") == ["dim_supplier"]
        assert by_layer.get("gold") == ["supplier_spend"]

    def test_layers_bronze_only_returns_bronze_no_silver_gold(
        self, fixture,
    ) -> None:
        """``--layers bronze`` produces a bronze-only plan."""
        bundle_path, pack, profile = fixture
        summary = orchestrator.run(
            bundle_path=bundle_path,
            resolved_pack=pack,
            tenant_profile=profile,
            layers=["bronze"],
            dry_run=True,
        )
        assert summary.steps == ()
        by_layer = _plan_ids_by_layer(summary)
        assert "bronze" in by_layer
        assert "erp_suppliers" in by_layer["bronze"]
        assert "silver" not in by_layer
        assert "gold" not in by_layer

    def test_layers_silver_only_returns_silver_no_bronze(
        self, fixture,
    ) -> None:
        """Phase 9 D-1: ``--layers silver`` filters declared roots but
        auto-includes transitive bronze deps. The silver-only filter
        keeps dim_supplier as the declared root; D-1 pulls in
        erp_suppliers (its bronze dep). gold is filtered out."""
        bundle_path, pack, profile = fixture
        summary = orchestrator.run(
            bundle_path=bundle_path,
            resolved_pack=pack,
            tenant_profile=profile,
            layers=["silver"],
            dry_run=True,
        )
        assert summary.steps == ()
        by_layer = _plan_ids_by_layer(summary)
        # D-1 auto-includes transitive bronze dep.
        assert by_layer.get("bronze") == ["erp_suppliers"]
        assert by_layer.get("silver") == ["dim_supplier"]
        assert "gold" not in by_layer

    def test_unknown_dataset_raises_resolver_error(
        self, fixture,
    ) -> None:
        """Dry-run routes through the SAME resolver as the real run,
        so a typo in ``--datasets`` fails the same way on planning as
        it would on execution (Phase 9: AIDPF-1034 from the content-
        pack plan resolver)."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            UnknownDatasetFilterError,
        )

        bundle_path, pack, profile = fixture
        with pytest.raises(UnknownDatasetFilterError):
            orchestrator.run(
                bundle_path=bundle_path,
                resolved_pack=pack,
                tenant_profile=profile,
                datasets=["this_does_not_exist"],
                dry_run=True,
            )
