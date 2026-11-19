"""Phase 5 — AIDPF-2071 / AIDPF-2072 gates resolve ``$column.<key>``
references against the tenant profile.

Before this fix, ``bronze_readiness._compute_required_columns`` and
``fusion_pvo_drift._required_columns_union`` compared the literal
``$column.invoice_currency_code`` string against the live source's
physical columns and reported the literal as missing. The shipped
starter pack uses ``$column.*`` syntax, so any default content-pack
run on a tenant that resolved the alias to a real column would
false-fail one (or both) gates.

These tests use the per-node preflight resolver's syntax — a pack
declaring ``columnAliases`` and a profile pinning the resolved value
— and assert the gates compare the RESOLVED physical column name to
the live source.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.bronze_readiness import (
    AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
    BronzeReadinessGateError,
    assert_bronze_readiness,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.fusion_pvo_drift import (
    AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
    FusionPvoDriftError,
    assert_fusion_pvo_compatibility,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.required_column_resolver import (
    resolve_required_column_entries,
)


# ---------------------------------------------------------------------------
# Resolver unit tests
# ---------------------------------------------------------------------------


class _FakeResolved:
    def __init__(self, column: dict[str, str]) -> None:
        self.column = column
        self.semantic: dict[str, str] = {}


class _FakeProfile:
    def __init__(self, resolved_col: dict[str, str]) -> None:
        self.resolved = _FakeResolved(resolved_col)


class _FakePack:
    def __init__(self, alias_keys: list[str]) -> None:
        # Pack exposes ``.pack.column_aliases`` — mimic that shape.
        self.pack = MagicMock()
        self.pack.column_aliases = {k: MagicMock() for k in alias_keys}


class TestResolveRequiredColumnEntries:
    def test_literal_entries_pass_through(self) -> None:
        out = resolve_required_column_entries(
            ["VendorId", "InvoiceNumber"],
            resolved_pack=_FakePack([]),
            tenant_profile=_FakeProfile({}),
        )
        assert out == {"VendorId", "InvoiceNumber"}

    def test_alias_reference_resolves_against_profile(self) -> None:
        out = resolve_required_column_entries(
            ["$column.invoice_currency_code"],
            resolved_pack=_FakePack(["invoice_currency_code"]),
            tenant_profile=_FakeProfile(
                {"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"}
            ),
        )
        assert out == {"ApInvoicesInvoiceCurrencyCode"}

    def test_alias_unknown_key_dropped_silently(self) -> None:
        """Resolution failures are dropped — per-node preflight is the
        canonical place to raise AIDPF-2046."""
        out = resolve_required_column_entries(
            ["$column.unknown_alias", "RealCol"],
            resolved_pack=_FakePack(["other_alias"]),
            tenant_profile=_FakeProfile({"other_alias": "X"}),
        )
        assert out == {"RealCol"}

    def test_alias_known_key_no_profile_value_dropped(self) -> None:
        out = resolve_required_column_entries(
            ["$column.invoice_currency_code"],
            resolved_pack=_FakePack(["invoice_currency_code"]),
            tenant_profile=_FakeProfile({}),  # key declared but not pinned
        )
        assert out == set()

    def test_no_profile_drops_alias_refs(self) -> None:
        out = resolve_required_column_entries(
            ["$column.invoice_currency_code", "LiteralCol"],
            resolved_pack=_FakePack(["invoice_currency_code"]),
            tenant_profile=None,
        )
        assert out == {"LiteralCol"}


# ---------------------------------------------------------------------------
# AIDPF-2071 / AIDPF-2072 gate integration — alias-aware comparison
# ---------------------------------------------------------------------------


class _FakeNode:
    """Minimum NodeYaml-shaped duck for the gate's traversal."""

    def __init__(
        self,
        *,
        node_id: str,
        layer: str,
        bronze_dep: str,
        required_cols: list[str],
    ) -> None:
        self.id = node_id
        self.layer = layer
        # depends_on.bronze[].id  /  depends_on.silver  (empty here)
        bronze_src = MagicMock()
        bronze_src.id = bronze_dep
        deps = MagicMock()
        deps.bronze = [bronze_src]
        deps.silver = []
        self.depends_on = deps
        self.required_columns = {bronze_dep: required_cols}
        self.refresh = MagicMock()
        self.refresh.incremental = None


def _make_pack_with_silver(silver_nodes: list[_FakeNode]) -> MagicMock:
    pack = MagicMock()
    pack.pack.column_aliases = {"invoice_currency_code": MagicMock()}
    pack.silver = {n.id: n for n in silver_nodes}
    pack.gold = {}
    return pack


class TestBronzeReadinessGateResolvesAliases:
    """AIDPF-2071: a pack using ``$column.*`` syntax should PASS the
    gate when the live bronze surfaces the resolved physical column.

    Pre-fix, the gate compared the literal alias-reference string —
    e.g. ``$column.invoice_currency_code`` — against the live bronze
    schema (which carries the physical name) and false-failed.
    """

    def test_gate_passes_when_resolved_physical_column_present(
        self, tmp_path,
    ) -> None:
        node = _FakeNode(
            node_id="dim_supplier",
            layer="silver",
            bronze_dep="ap_invoices",
            required_cols=["$column.invoice_currency_code"],
        )
        pack = _make_pack_with_silver([node])
        profile = _FakeProfile(
            {"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"}
        )

        # Fake spark: bronze DESCRIBE returns the resolved physical name.
        fake_spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [
            {"col_name": "ApInvoicesInvoiceCurrencyCode"},
        ]
        fake_spark.sql.return_value = df

        # Paths .bronze(<id>) — only used as the DESCRIBE TABLE argument.
        paths = MagicMock()
        paths.bronze.return_value = "catalog.bronze.ap_invoices"

        # Should NOT raise — resolved column IS present in the live schema.
        assert_bronze_readiness(
            fake_spark,
            resolved_pack=pack,
            cp_filter=(None, ["silver"]),
            paths=paths,
            run_id="r1",
            diagnostics_root=tmp_path / "diag",
            tenant_profile=profile,
        )

    def test_gate_fails_when_resolved_physical_column_missing(
        self, tmp_path,
    ) -> None:
        """The gate still surfaces real gaps — when the resolved
        physical column is absent from the live bronze, AIDPF-2071
        fires with the *resolved* name in the diagnostic (not the
        literal alias reference)."""
        node = _FakeNode(
            node_id="dim_supplier",
            layer="silver",
            bronze_dep="ap_invoices",
            required_cols=["$column.invoice_currency_code"],
        )
        pack = _make_pack_with_silver([node])
        profile = _FakeProfile(
            {"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"}
        )

        fake_spark = MagicMock()
        df = MagicMock()
        # Live bronze does NOT surface the resolved column.
        df.collect.return_value = [{"col_name": "SomeOtherColumn"}]
        fake_spark.sql.return_value = df

        paths = MagicMock()
        paths.bronze.return_value = "catalog.bronze.ap_invoices"

        with pytest.raises(BronzeReadinessGateError) as exc:
            assert_bronze_readiness(
                fake_spark,
                resolved_pack=pack,
                cp_filter=(None, ["silver"]),
                paths=paths,
                run_id="r2",
                diagnostics_root=tmp_path / "diag",
                tenant_profile=profile,
            )
        message = str(exc.value)
        assert AIDPF_2071_BRONZE_READINESS_GATE_FAILED in message
        # The RESOLVED name appears in the diagnostic.
        assert "ApInvoicesInvoiceCurrencyCode" in message
        # The literal alias-reference does NOT (we don't want operators
        # chasing the literal "$column.invoice_currency_code" as a
        # column to add to Fusion).
        assert "$column.invoice_currency_code" not in message


class TestFusionPvoDriftGateResolvesAliases:
    """AIDPF-2072: same fix, applied to the live-PVO drift gate."""

    def test_gate_passes_when_resolved_column_in_live_pvo(
        self, tmp_path,
    ) -> None:
        node = _FakeNode(
            node_id="dim_supplier",
            layer="silver",
            bronze_dep="ap_invoices",
            required_cols=["$column.invoice_currency_code"],
        )
        pack = _make_pack_with_silver([node])
        profile = _FakeProfile(
            {"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"}
        )
        # Live PVO surfaces the resolved physical name.
        live_pvo_columns = {
            "ap_invoices": {
                "apinvoicesinvoicecurrencycode": "string",
            },
        }
        # Snapshot None → degraded mode (missing-column / rename detection only).
        assert_fusion_pvo_compatibility(
            live_pvo_columns=live_pvo_columns,
            resolved_pack=pack,
            cp_filter=(None, ["silver"]),
            bronze_filter=(None, ["bronze"]),
            schema_snapshot=None,
            run_id="r3",
            diagnostics_root=tmp_path / "diag",
            tenant_profile=profile,
        )

    def test_gate_fails_when_resolved_column_missing_from_live_pvo(
        self, tmp_path,
    ) -> None:
        node = _FakeNode(
            node_id="dim_supplier",
            layer="silver",
            bronze_dep="ap_invoices",
            required_cols=["$column.invoice_currency_code"],
        )
        pack = _make_pack_with_silver([node])
        profile = _FakeProfile(
            {"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"}
        )
        live_pvo_columns = {
            "ap_invoices": {"unrelated_col": "string"},
        }

        with pytest.raises(FusionPvoDriftError) as exc:
            assert_fusion_pvo_compatibility(
                live_pvo_columns=live_pvo_columns,
                resolved_pack=pack,
                cp_filter=(None, ["silver"]),
                bronze_filter=(None, ["bronze"]),
                schema_snapshot=None,
                run_id="r4",
                diagnostics_root=tmp_path / "diag",
                tenant_profile=profile,
            )
        message = str(exc.value)
        assert AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED in message
        # Resolved name in the gap; literal alias ref not.
        assert "ApInvoicesInvoiceCurrencyCode" in message
        assert "$column.invoice_currency_code" not in message
