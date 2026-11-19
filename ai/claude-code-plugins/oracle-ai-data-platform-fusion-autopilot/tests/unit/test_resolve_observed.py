"""Unit tests for :func:`bronze_probe.resolve_observed` (shared producer selection).

Covers: all-landed → DESCRIBE path; all-absent → source path; mixed → merge;
and the pack-id != physical-target divergence (`gl_journal_lines` targets table
`gl_journal_headers`) honored in BOTH the landed and source branches and keyed
by node id. Scope derives from `pack.bronze` (+ legacy `pack.bronze_yaml`), not
`bundle.datasets`.

Spark/BICC are mocked: the strict detector's `DESCRIBE ... .take(1)` and the
landed `DESCRIBE ... .collect()` are driven by a fake `sql()`, and the source
producer is monkeypatched.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from oracle_ai_data_platform_fusion_autopilot.commands import bronze_probe
from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    resolve_observed,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    ColumnInfo,
)


class _NotFound(Exception):  # noqa: N818 — mirrors Spark's `AnalysisException` name
    def getErrorClass(self) -> str:  # noqa: N802 — mirrors Spark's API
        return "TABLE_OR_VIEW_NOT_FOUND"


def _row(col_name: str, data_type: str = "string"):
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _pack(nodes: dict[str, str]):
    """Build a fake ResolvedPack: {node_id: target_table}."""
    bronze = {
        nid: SimpleNamespace(target=target) for nid, target in nodes.items()
    }
    return SimpleNamespace(bronze=bronze, bronze_yaml={})


def _spark(landed: dict[str, list[dict]], absent_tables: set[str]) -> MagicMock:
    """Fake Spark. `landed` keys + `absent_tables` are physical table names.

    - `DESCRIBE TABLE ....take(1)` (absence probe): raises _NotFound if the
      physical table is in `absent_tables`, else returns a row.
    - `DESCRIBE TABLE ....collect()` (landed probe): returns the fixture rows.
    """
    spark = MagicMock(name="spark")

    def _sql(query: str):
        table = query.split()[-1].split(".")[-1]  # physical table name
        df = MagicMock(name=f"df_{table}")
        if table in absent_tables:
            df.take.side_effect = _NotFound()
        else:
            df.take.return_value = [object()]
        df.collect.return_value = landed.get(table, [])
        return df

    spark.sql.side_effect = _sql
    return spark


class TestResolveObserved:
    def test_all_landed_uses_describe(self, monkeypatch) -> None:
        called = {"source": False}
        monkeypatch.setattr(
            bronze_probe, "describe_bronze_from_source",
            lambda *a, **k: called.__setitem__("source", True) or {},
        )
        pack = _pack({"erp_suppliers": "erp_suppliers", "ap_invoices": "ap_invoices"})
        spark = _spark(
            landed={
                "erp_suppliers": [_row("VENDORID")],
                "ap_invoices": [_row("INVOICEID")],
            },
            absent_tables=set(),
        )
        observed = resolve_observed(
            spark, catalog="cat", bronze_schema="bronze",
            pack=pack, bundle=object(), resolved_password="pw",
        )
        assert observed == {
            "erp_suppliers": [ColumnInfo(name="VENDORID", type="string")],
            "ap_invoices": [ColumnInfo(name="INVOICEID", type="string")],
        }
        assert called["source"] is False  # source producer never invoked

    def test_all_absent_uses_source(self, monkeypatch) -> None:
        monkeypatch.setattr(
            bronze_probe, "describe_bronze_from_source",
            lambda *a, **k: {
                nid: [ColumnInfo(name="SRC", type="string")]
                for nid in k["dataset_ids"]
            },
        )
        pack = _pack({"erp_suppliers": "erp_suppliers", "ap_invoices": "ap_invoices"})
        spark = _spark(landed={}, absent_tables={"erp_suppliers", "ap_invoices"})
        observed = resolve_observed(
            spark, catalog="cat", bronze_schema="bronze",
            pack=pack, bundle=object(), resolved_password="pw",
        )
        assert observed == {
            "erp_suppliers": [ColumnInfo(name="SRC", type="string")],
            "ap_invoices": [ColumnInfo(name="SRC", type="string")],
        }

    def test_mixed_merges_both(self, monkeypatch) -> None:
        captured_source_ids = {}

        def _src(spark, *, pack, bundle, resolved_password, dataset_ids=None):
            captured_source_ids["ids"] = list(dataset_ids or [])
            return {nid: [ColumnInfo(name="SRC", type="string")] for nid in dataset_ids}

        monkeypatch.setattr(bronze_probe, "describe_bronze_from_source", _src)
        pack = _pack({"landed_ds": "landed_ds", "fresh_ds": "fresh_ds"})
        spark = _spark(
            landed={"landed_ds": [_row("L")]},
            absent_tables={"fresh_ds"},
        )
        observed = resolve_observed(
            spark, catalog="cat", bronze_schema="bronze",
            pack=pack, bundle=object(), resolved_password="pw",
        )
        assert observed == {
            "landed_ds": [ColumnInfo(name="L", type="string")],
            "fresh_ds": [ColumnInfo(name="SRC", type="string")],
        }
        # Only the absent node went to the source producer.
        assert captured_source_ids["ids"] == ["fresh_ds"]

    def test_physical_target_landed_path(self, monkeypatch) -> None:
        """`gl_journal_lines` targets table `gl_journal_headers`: the absence
        probe and DESCRIBE must hit the physical table, result keyed by id."""
        monkeypatch.setattr(
            bronze_probe, "describe_bronze_from_source", lambda *a, **k: {}
        )
        pack = _pack({"gl_journal_lines": "gl_journal_headers"})
        spark = _spark(
            landed={"gl_journal_headers": [_row("LEDGERID")]},
            absent_tables=set(),
        )
        observed = resolve_observed(
            spark, catalog="cat", bronze_schema="bronze",
            pack=pack, bundle=object(), resolved_password="pw",
        )
        # Keyed by node id, not the physical table name.
        assert observed == {
            "gl_journal_lines": [ColumnInfo(name="LEDGERID", type="string")]
        }

    def test_physical_target_source_path(self, monkeypatch) -> None:
        """Same divergence, source branch: probe by id (descriptor builds the
        physical target internally), result keyed by id."""
        def _src(spark, *, pack, bundle, resolved_password, dataset_ids=None):
            # probe_bronze_schemas keys by node id; assert we asked for the id.
            assert list(dataset_ids) == ["gl_journal_lines"]
            return {"gl_journal_lines": [ColumnInfo(name="LEDGERID", type="bigint")]}

        monkeypatch.setattr(bronze_probe, "describe_bronze_from_source", _src)
        pack = _pack({"gl_journal_lines": "gl_journal_headers"})
        spark = _spark(landed={}, absent_tables={"gl_journal_headers"})
        observed = resolve_observed(
            spark, catalog="cat", bronze_schema="bronze",
            pack=pack, bundle=object(), resolved_password="pw",
        )
        assert observed == {
            "gl_journal_lines": [ColumnInfo(name="LEDGERID", type="bigint")]
        }

    def test_scope_includes_legacy_bronze_yaml_id(self, monkeypatch) -> None:
        """Legacy `bronze_yaml`-only ids are in scope and take the landed path
        (no node descriptor → not source-probable)."""
        monkeypatch.setattr(
            bronze_probe, "describe_bronze_from_source", lambda *a, **k: {}
        )
        pack = _pack({"erp_suppliers": "erp_suppliers"})
        pack.bronze_yaml = {"datasets": [{"id": "legacy_ds"}]}
        spark = _spark(
            landed={"erp_suppliers": [_row("V")], "legacy_ds": [_row("X")]},
            absent_tables=set(),
        )
        observed = resolve_observed(
            spark, catalog="cat", bronze_schema="bronze",
            pack=pack, bundle=object(), resolved_password="pw",
        )
        assert set(observed) == {"erp_suppliers", "legacy_ds"}
