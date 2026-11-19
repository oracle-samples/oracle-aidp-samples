"""Unit tests for :func:`bronze_probe.describe_bronze_from_source`.

The source builder converts the ``{dataset_id: StructType}`` map returned
by ``bronze_extract_adapter.probe_bronze_schemas`` (a metadata-only BICC
``inferSchema`` probe) into the same ``{dataset_id: [ColumnInfo]}`` shape
``describe_bronze`` produces — so the variation walker and the fingerprint
helper consume either producer interchangeably (fresh-tenant feature).

Spark/BICC are mocked: ``probe_bronze_schemas`` is monkeypatched to return
fake ``StructType``-shaped objects, so no live cluster or pyspark import is
needed.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    _struct_type_to_columns,
    describe_bronze_from_source,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
    bronze_extract_adapter,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    ColumnInfo,
)


class _FakeDataType:
    """Duck-types ``pyspark.sql.types.DataType.simpleString()``."""

    def __init__(self, simple: str) -> None:
        self._simple = simple

    def simpleString(self) -> str:  # noqa: N802 — match Spark's API name
        return self._simple


class _FakeField:
    def __init__(self, name: str, simple: str) -> None:
        self.name = name
        self.dataType = _FakeDataType(simple)


class _FakeStructType:
    def __init__(self, fields: list[_FakeField]) -> None:
        self.fields = fields


def _struct(pairs: list[tuple[str, str]]) -> _FakeStructType:
    return _FakeStructType([_FakeField(n, t) for n, t in pairs])


class TestStructTypeToColumns:
    def test_uses_simplestring_not_repr(self) -> None:
        cols = _struct_type_to_columns(
            _struct([("VendorId", "bigint"), ("Segment1", "string")])
        )
        assert cols == [
            ColumnInfo(name="VendorId", type="bigint"),
            ColumnInfo(name="Segment1", type="string"),
        ]

    def test_preserves_parametrized_types(self) -> None:
        cols = _struct_type_to_columns(_struct([("Amount", "decimal(38,2)")]))
        assert cols == [ColumnInfo(name="Amount", type="decimal(38,2)")]

    def test_empty_struct(self) -> None:
        assert _struct_type_to_columns(_struct([])) == []


class TestDescribeBronzeFromSource:
    def test_converts_and_keys_by_dataset_id(self, monkeypatch) -> None:
        fake_schemas = {
            "erp_suppliers": _struct([("VendorId", "bigint")]),
            "gl_journal_lines": _struct([("LedgerId", "bigint"), ("Amount", "decimal(38,2)")]),
        }
        monkeypatch.setattr(
            bronze_extract_adapter,
            "probe_bronze_schemas",
            lambda *a, **k: fake_schemas,
        )
        observed = describe_bronze_from_source(
            object(),  # spark — unused by the fake
            pack=object(),
            bundle=object(),
            resolved_password="pw",
        )
        assert observed == {
            "erp_suppliers": [ColumnInfo(name="VendorId", type="bigint")],
            "gl_journal_lines": [
                ColumnInfo(name="LedgerId", type="bigint"),
                ColumnInfo(name="Amount", type="decimal(38,2)"),
            ],
        }

    def test_threads_args_to_probe(self, monkeypatch) -> None:
        captured = {}

        def _fake(spark, *, pack, bundle, resolved_password, dataset_ids=None):
            captured.update(
                pack=pack, bundle=bundle,
                resolved_password=resolved_password, dataset_ids=dataset_ids,
            )
            return {}

        monkeypatch.setattr(bronze_extract_adapter, "probe_bronze_schemas", _fake)
        pack, bundle = object(), object()
        describe_bronze_from_source(
            object(), pack=pack, bundle=bundle,
            resolved_password="secret", dataset_ids=["a", "b"],
        )
        assert captured == {
            "pack": pack, "bundle": bundle,
            "resolved_password": "secret", "dataset_ids": ["a", "b"],
        }

    def test_shape_matches_describe_bronze(self, monkeypatch) -> None:
        """Both producers return ``{id: [ColumnInfo]}`` with identical
        ColumnInfo values for the same logical columns — the precondition
        for fingerprint parity (Risk R1)."""
        monkeypatch.setattr(
            bronze_extract_adapter,
            "probe_bronze_schemas",
            lambda *a, **k: {"erp_suppliers": _struct([("VENDORID", "string")])},
        )
        source = describe_bronze_from_source(
            object(), pack=object(), bundle=object(), resolved_password="pw"
        )
        assert isinstance(source["erp_suppliers"], list)
        assert all(isinstance(c, ColumnInfo) for c in source["erp_suppliers"])
        # describe_bronze (landed) yields the same ColumnInfo for the same column.
        assert source["erp_suppliers"] == [ColumnInfo(name="VENDORID", type="string")]
