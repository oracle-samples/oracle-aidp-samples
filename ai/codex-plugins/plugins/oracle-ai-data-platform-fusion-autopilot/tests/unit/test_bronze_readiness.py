"""Unit tests for the bronze readiness gate (Phase 5 Step 2c, AIDPF-2071)."""

from __future__ import annotations

import json
import pathlib
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
from oracle_ai_data_platform_fusion_autopilot.orchestrator.bronze_readiness import (
    AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
    BronzeReadinessGateError,
    assert_bronze_readiness,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack


PACK_YAML = """
id: phase5-bronze-readiness-test
version: 1.0.0
description: Phase 5 bronze readiness gate test pack
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

GOLD_SUPPLIER_SPEND = """
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  bronze:
    - id: ap_invoices
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
requiredColumns:
  ap_invoices: [invoice_id, vendor_id, amount]
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
  ap_invoices: [invoice_id, due_date]
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
    (gold / "supplier_spend.yaml").write_text(GOLD_SUPPLIER_SPEND, encoding="utf-8")
    (gold / "supplier_spend.sql").write_text("SELECT 1\n", encoding="utf-8")
    (gold / "ap_aging.yaml").write_text(GOLD_AP_AGING, encoding="utf-8")
    (gold / "ap_aging.sql").write_text("SELECT 1\n", encoding="utf-8")

    return load_pack(pack_root)


def _paths() -> TablePaths:
    return TablePaths(
        catalog="tenant_cat",
        bronze_schema="bronze_sch",
        silver_schema="silver_sch",
        gold_schema="gold_sch",
    )


def _spark_with_describe(table_cols: dict[str, list[str]]) -> MagicMock:
    """Build a Spark mock where DESCRIBE TABLE returns cols per FQN.

    Tables absent from the dict raise AnalysisException-like behaviour
    by returning an exception so the readiness gate treats them as
    missing.
    """
    spark = MagicMock()
    spark.describe_calls = []  # type: ignore[attr-defined]

    def sql_side_effect(stmt, *args, **kwargs):
        spark.describe_calls.append(stmt)  # type: ignore[attr-defined]
        if "DESCRIBE TABLE" not in stmt.upper():
            df = MagicMock()
            df.collect.return_value = []
            return df
        fqn = stmt.split()[-1].strip().rstrip(";")
        if fqn not in table_cols:
            # Simulate AnalysisException by raising RuntimeError; the
            # gate's broad except catches it.
            raise RuntimeError(f"AnalysisException: table {fqn} not found")
        df = MagicMock()
        df.collect.return_value = [(name, "string", None) for name in table_cols[fqn]]
        return df

    spark.sql.side_effect = sql_side_effect
    return spark


class TestBronzeReadinessHappyPath:
    def test_all_bronze_present_silent_return(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(tmp_path)
        spark = _spark_with_describe({
            "tenant_cat.bronze_sch.erp_suppliers": ["vendor_id", "vendor_name"],
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        # Returns None on success.
        result = assert_bronze_readiness(
            spark,
            resolved_pack=pack,
            cp_filter=(None, ["silver", "gold"]),
            paths=_paths(),
            run_id="test-run-1",
            diagnostics_root=tmp_path / "diag",
        )
        assert result is None
        # No diagnostic JSON written for success.
        assert not (tmp_path / "diag" / "test-run-1" / "AIDPF-2071.json").exists()


class TestBronzeReadinessFailures:
    def test_missing_bronze_table(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        # erp_suppliers missing entirely.
        spark = _spark_with_describe({
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        with pytest.raises(BronzeReadinessGateError) as exc:
            assert_bronze_readiness(
                spark,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                paths=_paths(),
                run_id="test-run-2",
                diagnostics_root=tmp_path / "diag",
            )
        assert AIDPF_2071_BRONZE_READINESS_GATE_FAILED in str(exc.value)
        assert "erp_suppliers" in str(exc.value)
        assert exc.value.gaps["erp_suppliers"]["table_missing"] is True

    def test_missing_required_column_single(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        # vendor_name dropped from erp_suppliers.
        spark = _spark_with_describe({
            "tenant_cat.bronze_sch.erp_suppliers": ["vendor_id"],
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        with pytest.raises(BronzeReadinessGateError) as exc:
            assert_bronze_readiness(
                spark,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                paths=_paths(),
                run_id="test-run-3",
                diagnostics_root=tmp_path / "diag",
            )
        assert "vendor_name" in str(exc.value)
        assert exc.value.gaps["erp_suppliers"]["missing_columns"] == [
            "vendor_name"
        ]

    def test_missing_columns_unioned_across_nodes(
        self, tmp_path: pathlib.Path
    ) -> None:
        # supplier_spend needs invoice_id+vendor_id+amount from ap_invoices;
        # ap_aging needs invoice_id+due_date. Drop both amount AND due_date.
        pack = _build_pack(tmp_path)
        spark = _spark_with_describe({
            "tenant_cat.bronze_sch.erp_suppliers": ["vendor_id", "vendor_name"],
            "tenant_cat.bronze_sch.ap_invoices": ["invoice_id", "vendor_id"],
        })
        with pytest.raises(BronzeReadinessGateError) as exc:
            assert_bronze_readiness(
                spark,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                paths=_paths(),
                run_id="test-run-4",
                diagnostics_root=tmp_path / "diag",
            )
        missing = exc.value.gaps["ap_invoices"]["missing_columns"]
        # Union from BOTH supplier_spend (amount) and ap_aging (due_date).
        assert "amount" in missing
        assert "due_date" in missing

    def test_transitive_bronze_via_silver(self, tmp_path: pathlib.Path) -> None:
        # supplier_spend → dim_supplier → erp_suppliers transitively.
        # Restrict scope to gold only (supplier_spend).
        pack = _build_pack(tmp_path)
        spark = _spark_with_describe({
            # erp_suppliers is missing — gate should pick it up via the
            # silver→bronze walk even though supplier_spend itself
            # doesn't declare erp_suppliers in its dependsOn.
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        with pytest.raises(BronzeReadinessGateError) as exc:
            assert_bronze_readiness(
                spark,
                resolved_pack=pack,
                cp_filter=(["supplier_spend"], None),
                paths=_paths(),
                run_id="test-run-5",
                diagnostics_root=tmp_path / "diag",
            )
        assert "erp_suppliers" in exc.value.gaps

    def test_cp_filter_narrows_readiness_set(
        self, tmp_path: pathlib.Path
    ) -> None:
        # ap_aging only (gold) — depends on ap_invoices, not erp_suppliers.
        # erp_suppliers MUST NOT be probed because nothing in scope needs it.
        pack = _build_pack(tmp_path)
        spark = _spark_with_describe({
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        # Should not raise — ap_invoices is complete for ap_aging.
        assert_bronze_readiness(
            spark,
            resolved_pack=pack,
            cp_filter=(["ap_aging"], None),
            paths=_paths(),
            run_id="test-run-6",
            diagnostics_root=tmp_path / "diag",
        )
        # erp_suppliers was NOT probed.
        described_tables = [
            s.split()[-1] for s in spark.describe_calls
            if "DESCRIBE TABLE" in s
        ]
        assert "tenant_cat.bronze_sch.ap_invoices" in described_tables
        assert "tenant_cat.bronze_sch.erp_suppliers" not in described_tables

    def test_analysis_exception_caught_cleanly(
        self, tmp_path: pathlib.Path
    ) -> None:
        # erp_suppliers raises (simulated AnalysisException) → treated as
        # missing, surfaces as table_missing in gaps.
        pack = _build_pack(tmp_path)
        spark = _spark_with_describe({
            # erp_suppliers absent → DESCRIBE raises RuntimeError
            # (mimicking AnalysisException).
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        with pytest.raises(BronzeReadinessGateError) as exc:
            assert_bronze_readiness(
                spark,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                paths=_paths(),
                run_id="test-run-7",
                diagnostics_root=tmp_path / "diag",
            )
        assert exc.value.gaps["erp_suppliers"].get("table_missing") is True


class TestBronzeReadinessDiagnostic:
    def test_diagnostic_json_written(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        spark = _spark_with_describe({
            "tenant_cat.bronze_sch.ap_invoices": [
                "invoice_id", "vendor_id", "amount", "due_date",
            ],
        })
        with pytest.raises(BronzeReadinessGateError):
            assert_bronze_readiness(
                spark,
                resolved_pack=pack,
                cp_filter=(None, ["silver", "gold"]),
                paths=_paths(),
                run_id="diag-run",
                diagnostics_root=tmp_path / "diag",
            )
        diag_path = tmp_path / "diag" / "diag-run" / "AIDPF-2071.json"
        assert diag_path.exists()
        payload = json.loads(diag_path.read_text(encoding="utf-8"))
        assert payload["code"] == AIDPF_2071_BRONZE_READINESS_GATE_FAILED
        assert payload["run_id"] == "diag-run"
        assert "erp_suppliers" in payload["gaps"]
        assert "remediation" in payload
        assert "bootstrap --refresh" in payload["remediation"]
