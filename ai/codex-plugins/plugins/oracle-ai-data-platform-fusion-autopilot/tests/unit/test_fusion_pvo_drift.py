"""Unit tests for the Fusion PVO drift gate (Phase 5 Step 2d, AIDPF-2072)."""

from __future__ import annotations

import json
import pathlib
from datetime import datetime, timezone

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.fusion_pvo_drift import (
    AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
    FusionPvoDriftError,
    assert_fusion_pvo_compatibility,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_snapshot import (
    BronzeSchemaSnapshotV1,
    SnapshotColumn,
    SnapshotDataset,
)


PACK_YAML = """
id: phase5-fusion-pvo-drift-test
version: 1.0.0
description: Phase 5 PVO drift gate test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

SILVER_DIM_SUPPLIER = """
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
requiredColumns:
  erp_suppliers: [vendor_id, vendor_name]
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""

GOLD_AP_AGING = """
id: ap_aging
layer: gold
implementation:
  type: sql
  sql: gold/ap_aging.sql
target: ap_aging
dependsOn:
  bronze:
    - id: ap_invoices
refresh:
  seed:
    strategy: replace
requiredColumns:
  ap_invoices: [invoice_id, due_date, amount]
outputSchema:
  columns:
    - name: aging_bucket
      type: string
      nullable: false
      pii: none
"""


def _build_pack(tmp_path: pathlib.Path):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_supplier.yaml").write_text(SILVER_DIM_SUPPLIER, encoding="utf-8")
    (silver / "dim_supplier.sql").write_text("SELECT 1\n", encoding="utf-8")
    gold = pack_root / "gold"
    gold.mkdir()
    (gold / "ap_aging.yaml").write_text(GOLD_AP_AGING, encoding="utf-8")
    (gold / "ap_aging.sql").write_text("SELECT 1\n", encoding="utf-8")
    return load_pack(pack_root)


def _make_snapshot(per_ds_cols: dict[str, list[tuple[str, str]]]) -> BronzeSchemaSnapshotV1:
    return BronzeSchemaSnapshotV1(
        schema_version=1,
        tenant="acme-corp",
        pinned_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        bronze_schema_fingerprint="sha256:test",
        datasets=[
            SnapshotDataset(
                dataset_id=ds_id,
                columns=[SnapshotColumn(name=n, type=t) for n, t in cols],
            )
            for ds_id, cols in per_ds_cols.items()
        ],
    )


class TestHappyPath:
    def test_all_in_sync_silent_return(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        snapshot = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"), ("vendor_name", "string"),
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
            ],
        })
        live_pvo_columns = {
            "erp_suppliers": {"vendor_id": "long", "vendor_name": "string"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        # Returns None on success.
        assert assert_fusion_pvo_compatibility(
            live_pvo_columns=live_pvo_columns,
            resolved_pack=pack,
            cp_filter=(None, ["silver", "gold"]),
            bronze_filter=(None, ["bronze"]),
            schema_snapshot=snapshot,
            run_id="r1",
            diagnostics_root=tmp_path / "diag",
        ) is None


class TestDriftPaths:
    def test_missing_required_column(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        # Live PVO is missing 'vendor_name' (dim_supplier requires it).
        live = {
            "erp_suppliers": {"vendor_id": "long"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        with pytest.raises(FusionPvoDriftError) as exc:
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=None,
                run_id="r2",
                diagnostics_root=tmp_path / "diag",
            )
        assert AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED in str(exc.value)
        assert "vendor_name" in str(exc.value)
        assert exc.value.gaps["erp_suppliers"]["missing_required_columns"] == [
            "vendor_name"
        ]

    def test_type_change_caught_with_snapshot(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        snapshot = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"), ("vendor_name", "string"),
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
            ],
        })
        # Live changed amount from double → string (incompatible).
        live = {
            "erp_suppliers": {"vendor_id": "long", "vendor_name": "string"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "string",
            },
        }
        with pytest.raises(FusionPvoDriftError) as exc:
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=snapshot,
                run_id="r3",
                diagnostics_root=tmp_path / "diag",
            )
        assert "type_changes" in exc.value.gaps["ap_invoices"]
        tc = exc.value.gaps["ap_invoices"]["type_changes"][0]
        assert tc["column"] == "amount"
        assert tc["snapshot_type"] == "double"
        assert tc["live_type"] == "string"

    def test_snapshot_absent_skips_type_check(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(tmp_path)
        # Type changed but no snapshot — type check skipped; required-
        # column check still passes.
        live = {
            "erp_suppliers": {"vendor_id": "long", "vendor_name": "string"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "string",
            },
        }
        # Returns None — type change can't be detected without snapshot.
        assert assert_fusion_pvo_compatibility(
            live_pvo_columns=live,
            resolved_pack=pack,
            cp_filter=(None, ["silver", "gold"]),
            bronze_filter=(None, ["bronze"]),
            schema_snapshot=None,
            run_id="r4",
            diagnostics_root=tmp_path / "diag",
        ) is None

    def test_extra_live_column_allowed(self, tmp_path: pathlib.Path) -> None:
        # Live PVO has an extra column not in the snapshot or requirements
        # — bronze seed adopts via overwriteSchema=true. Should NOT raise.
        pack = _build_pack(tmp_path)
        snapshot = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"), ("vendor_name", "string"),
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
            ],
        })
        live = {
            "erp_suppliers": {
                "vendor_id": "long", "vendor_name": "string",
                "new_extra_column": "string",  # extra
            },
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        assert assert_fusion_pvo_compatibility(
            live_pvo_columns=live,
            resolved_pack=pack,
            cp_filter=(None, ["silver", "gold"]),
            bronze_filter=(None, ["bronze"]),
            schema_snapshot=snapshot,
            run_id="r5",
            diagnostics_root=tmp_path / "diag",
        ) is None

    def test_bronze_filter_narrows_scope(self, tmp_path: pathlib.Path) -> None:
        # Only ap_invoices in bronze_filter — drift in erp_suppliers
        # is ignored because it's out of bronze scope.
        pack = _build_pack(tmp_path)
        live = {
            "erp_suppliers": {"vendor_id": "long"},  # missing vendor_name
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        assert assert_fusion_pvo_compatibility(
            live_pvo_columns=live,
            resolved_pack=pack,
            cp_filter=(["ap_aging"], None),
            bronze_filter=(["ap_invoices"], None),
            schema_snapshot=None,
            run_id="r6",
            diagnostics_root=tmp_path / "diag",
        ) is None


class TestRenameAndSnapshotOnly:
    """Plan Step 2d sub-cases 8 + 9: rename detection (paired missing
    + extra) and snapshot-only column detection when no rename pair
    materialises."""

    def test_paired_missing_extra_surfaces_as_candidate_rename(
        self, tmp_path: pathlib.Path,
    ) -> None:
        """When a column disappears from the snapshot AND a new column
        appears in live, surface as candidate_renames so operators can
        confirm via medallion-author."""
        pack = _build_pack(tmp_path)
        snapshot = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"),
                ("OLDname", "string"),  # disappeared
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
            ],
        })
        live = {
            "erp_suppliers": {
                "vendor_id": "long",
                "vendor_name": "string",  # required by dim_supplier
                "NEWname": "string",  # extra in live (rename candidate)
            },
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        with pytest.raises(FusionPvoDriftError) as exc:
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=snapshot,
                run_id="rename-r",
                diagnostics_root=tmp_path / "diag",
            )
        gap = exc.value.gaps.get("erp_suppliers", {})
        assert "candidate_renames" in gap, (
            f"expected candidate_renames in gap; got {gap!r}"
        )
        rename = gap["candidate_renames"][0]
        assert rename["snapshot"].lower() == "oldname"
        assert rename["live"].lower() == "newname"

    def test_snapshot_only_columns_surface_without_rename_pair(
        self, tmp_path: pathlib.Path,
    ) -> None:
        """A snapshot column missing from live without a paired extra
        in live surfaces as snapshot_columns_missing_from_live."""
        pack = _build_pack(tmp_path)
        snapshot = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"),
                ("vendor_name", "string"),
                ("extra_pinned_col", "string"),  # disappears
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
            ],
        })
        live = {
            "erp_suppliers": {
                "vendor_id": "long", "vendor_name": "string",
            },
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        with pytest.raises(FusionPvoDriftError) as exc:
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=snapshot,
                run_id="snap-only-r",
                diagnostics_root=tmp_path / "diag",
            )
        gap = exc.value.gaps.get("erp_suppliers", {})
        assert "snapshot_columns_missing_from_live" in gap, (
            f"expected snapshot_columns_missing_from_live in gap; got {gap!r}"
        )
        assert "extra_pinned_col" in gap["snapshot_columns_missing_from_live"]


class TestAuditColumnDefensiveGuard:
    """The drift gate must tolerate snapshots that still contain bronze
    audit columns (`_extract_ts` / `_source_pvo` / `_run_id` /
    `_watermark_used`). New snapshots written post-fix exclude them,
    but pre-fix snapshots on disk may still have them and forcing every
    tenant to re-bootstrap before the next run is too sharp a tool.
    `_snapshot_columns_by_dataset` strips audit columns defensively so
    the gate compares apples-to-apples against live BICC `inferSchema`
    (which never has audit columns).
    """

    def test_audit_columns_in_stale_snapshot_do_not_trigger_drift(
        self, tmp_path: pathlib.Path,
    ) -> None:
        pack = _build_pack(tmp_path)
        # Pre-fix snapshot shape: BICC columns PLUS the 4 audit columns
        # the bronze adapter appends after extraction. Live BICC will
        # never report these (they don't exist in the PVO).
        snapshot_with_audit = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"), ("vendor_name", "string"),
                ("_extract_ts", "timestamp"),
                ("_source_pvo", "string"),
                ("_run_id", "string"),
                ("_watermark_used", "timestamp"),
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
                ("_extract_ts", "timestamp"),
                ("_source_pvo", "string"),
                ("_run_id", "string"),
                ("_watermark_used", "timestamp"),
            ],
        })
        # Live BICC schema — clean, no audit columns (it never has them).
        live = {
            "erp_suppliers": {"vendor_id": "long", "vendor_name": "string"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        # Must NOT raise — defensive strip lets the gate ignore audit
        # columns the writer should never have persisted in the first
        # place.
        assert assert_fusion_pvo_compatibility(
            live_pvo_columns=live,
            resolved_pack=pack,
            cp_filter=(None, ["silver", "gold"]),
            bronze_filter=(None, ["bronze"]),
            schema_snapshot=snapshot_with_audit,
            run_id="r-defensive",
            diagnostics_root=tmp_path / "diag",
        ) is None

    def test_real_drift_still_caught_when_audit_present(
        self, tmp_path: pathlib.Path,
    ) -> None:
        """The defensive strip must NOT mask actual drift. Snapshot has
        audit cols + a non-audit column the live PVO lost (rename or
        Oracle-side drop) — the gate must still fire on the real loss.
        """
        pack = _build_pack(tmp_path)
        snapshot = _make_snapshot({
            "erp_suppliers": [
                ("vendor_id", "long"), ("vendor_name", "string"),
                ("vendor_class", "string"),  # real column that live lost
                ("_extract_ts", "timestamp"),
                ("_run_id", "string"),
            ],
            "ap_invoices": [
                ("invoice_id", "long"), ("due_date", "date"),
                ("amount", "double"),
            ],
        })
        live = {
            "erp_suppliers": {"vendor_id": "long", "vendor_name": "string"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        with pytest.raises(FusionPvoDriftError) as exc:
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=snapshot,
                run_id="r-real-drift",
                diagnostics_root=tmp_path / "diag",
            )
        # The real lost column surfaces; audit columns do not appear.
        gap = exc.value.gaps["erp_suppliers"]
        # Either snapshot_columns_missing_from_live or candidate_renames
        # (depending on pair-up logic) must mention vendor_class — but
        # NEVER an audit column.
        flat = json.dumps(gap)
        assert "vendor_class" in flat
        for forbidden in (
            "_extract_ts", "_source_pvo", "_run_id", "_watermark_used",
        ):
            assert forbidden not in flat, (
                f"audit column {forbidden!r} leaked into drift gap: {gap!r}"
            )


class TestDiagnostic:
    def test_diagnostic_json_written(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        live = {
            "erp_suppliers": {"vendor_id": "long"},
            "ap_invoices": {
                "invoice_id": "long", "due_date": "date", "amount": "double",
            },
        }
        with pytest.raises(FusionPvoDriftError):
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=None,
                run_id="diag-r",
                diagnostics_root=tmp_path / "diag",
            )
        diag_path = tmp_path / "diag" / "diag-r" / "AIDPF-2072.json"
        assert diag_path.exists()
        payload = json.loads(diag_path.read_text(encoding="utf-8"))
        assert payload["code"] == AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED
        assert "bootstrap --refresh" in payload["remediation"]
