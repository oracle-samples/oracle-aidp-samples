"""Adapter wiring for bronze schemaPatches (feature bronze-extract-schema-patch).

Drives the REAL ``bronze_extract_adapter.run()`` with a fake Spark, a fake
BICC extractor, and a chain-recording DataFrame. The unit suite must be
runnable WITHOUT PySpark installed (the test extra does not declare it),
so a minimal fake ``pyspark`` module tree is injected into ``sys.modules``
for the adapter's lazy imports (types + expression builders) — the
assertions target the adapter's OBSERVABLE seams:

* NFR-1: zero extra connector calls / behavior change without patches;
* the metadata-only probe + patched read (user_schema carries the swapped
  type AND the base field's nullable/metadata verbatim);
* §5a guard: violations raise ``CastIntegrityError`` BEFORE any write;
* clean guard: temps dropped, metadata restored, base column order
  restored, ONE overwrite write, effective columns reported;
* unknown patch column fails loudly.
"""

from __future__ import annotations

import datetime as _dt
import pathlib
import sys
import types as _types
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.extractors import bicc as bicc_extractor
from oracle_ai_data_platform_fusion_autopilot.orchestrator import runtime
from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
    bronze_extract_adapter,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_patch import (
    CastIntegrityError,
    SchemaPatchError,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)

PACK_YAML = """
id: besp-adapter-test
version: 1.0.0
description: bronze schemaPatches adapter wiring pack
compatibility:
  pluginMinVersion: 0.3.0
"""

BRONZE_NODE_YAML = """
id: scm_items
layer: bronze
implementation:
  type: bronze_extract
  datastore: ItemExtractPVO
  biccSchema: Financial
  incrementalCapable: false
target: scm_items
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: scm_items
      column: LastUpdateDate
    naturalKey:
      - Id
outputSchema:
  columns:
    - { name: Id, type: long, nullable: true, pii: none }
    - { name: _extract_ts, type: timestamp, nullable: false, pii: none }
    - { name: _source_pvo, type: string, nullable: false, pii: none }
    - { name: _run_id, type: string, nullable: false, pii: none }
    - { name: _watermark_used, type: timestamp, nullable: true, pii: none }
quality:
  tests: []
"""

PROFILE_YAML = """
schemaVersion: 1
tenant: acme-corp
pinnedAt: 2026-06-05T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:besp-adapter-test"
"""


class _FakeCol:
    """Inert expression: every op returns another _FakeCol."""

    def _chain(self, *a, **k):
        return _FakeCol()

    cast = isNull = isNotNull = eqNullSafe = alias = otherwise = when = _chain

    def __and__(self, other):
        return _FakeCol()

    def __invert__(self):
        return _FakeCol()


class _FakeDF:
    """Chain-recording DataFrame; schema/columns tracked through renames,
    drops, selects and withColumn appends (Spark append semantics)."""

    def __init__(self, columns, log=None, guard_row=None, count_value=7):
        self.columns = list(columns)
        self.log = log if log is not None else []
        self._guard_row = guard_row or {}
        self._count = count_value
        self.schema = MagicMock()
        self.schema.fields = [
            MagicMock(name_attr=c, **{"name": c}) for c in self.columns
        ]
        self.schema.names = list(self.columns)

    def _clone(self, columns):
        return _FakeDF(columns, self.log, self._guard_row, self._count)

    def withColumnRenamed(self, old, new):
        self.log.append(("rename", old, new))
        cols = [new if c == old else c for c in self.columns]
        return self._clone(cols)

    def withColumn(self, name, _expr):
        self.log.append(("withColumn", name))
        cols = list(self.columns)
        if name not in cols:
            cols.append(name)  # Spark appends new columns at the END
        return self._clone(cols)

    def withMetadata(self, name, metadata):
        self.log.append(("withMetadata", name, dict(metadata)))
        return self._clone(self.columns)

    def drop(self, *names):
        self.log.append(("drop", names))
        return self._clone([c for c in self.columns if c not in set(names)])

    def select(self, *names):
        self.log.append(("select", tuple(names)))
        return self._clone(list(names))

    def cache(self):
        self.log.append(("cache",))
        return self

    def unpersist(self):
        self.log.append(("unpersist",))
        return self

    def count(self):
        self.log.append(("count",))
        return self._count

    def agg(self, *aggs):
        self.log.append(("agg", len(aggs)))
        outer = self

        class _Agg:
            def collect(self_inner):
                return [outer._guard_row]

        return _Agg()

    @property
    def write(self):
        self.log.append(("write",))
        writer = MagicMock(name="writer")
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        return writer

    def createOrReplaceTempView(self, name):
        self.log.append(("tempview", name))


class _FakeDataType:
    def __init__(self, ddl: str):
        self._ddl = ddl

    def simpleString(self) -> str:
        return self._ddl


def _fake_simple_type(ddl):
    return lambda: _FakeDataType(ddl)


class _FakeDecimalType(_FakeDataType):
    def __init__(self, precision: int, scale: int):
        super().__init__(f"decimal({precision},{scale})")


class _FakeStructField:
    def __init__(self, name, dataType, nullable=True, metadata=None):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}


class _FakeStructType:
    def __init__(self, fields):
        self.fields = list(fields)


def _install_fake_pyspark(monkeypatch) -> None:
    """Register a minimal fake ``pyspark`` tree in ``sys.modules`` so the
    adapter's lazy imports resolve WITHOUT PySpark installed — and
    deterministically even when it is."""
    types_mod = _types.ModuleType("pyspark.sql.types")
    types_mod.StructField = _FakeStructField
    types_mod.StructType = _FakeStructType
    types_mod.DecimalType = _FakeDecimalType
    for name, ddl in (
        ("LongType", "bigint"), ("IntegerType", "int"),
        ("DoubleType", "double"), ("FloatType", "float"),
        ("StringType", "string"), ("BooleanType", "boolean"),
        ("DateType", "date"), ("TimestampType", "timestamp"),
    ):
        setattr(types_mod, name, _fake_simple_type(ddl))

    functions_mod = _types.ModuleType("pyspark.sql.functions")
    for fn in ("col", "lit", "when", "count", "sum"):
        setattr(functions_mod, fn, lambda *a, **k: _FakeCol())

    sql_mod = _types.ModuleType("pyspark.sql")
    sql_mod.types = types_mod
    sql_mod.functions = functions_mod
    root_mod = _types.ModuleType("pyspark")
    root_mod.sql = sql_mod

    monkeypatch.setitem(sys.modules, "pyspark", root_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_mod)


def _base_schema_mock():
    """Fake StructType — the adapter reuses base StructFields verbatim and
    only reads .name/.dataType.simpleString()/.nullable/.metadata."""
    return _FakeStructType([
        _FakeStructField("Id", _FakeDecimalType(18, 0), True),
        _FakeStructField("ItemBasePEOMaterialCost", _FakeDecimalType(38, 0),
                         True, {"comment": "kept"}),
        _FakeStructField("ItemType", _FakeDataType("string"), True),
    ])


def _bundle(patches):
    ds = MagicMock()
    ds.id = "scm_items"
    ds.schema_patches = patches
    bundle = MagicMock(name="bundle")
    bundle.datasets = [ds]
    return bundle


def _drive(
    tmp_path: pathlib.Path,
    monkeypatch,
    *,
    patches,
    guard_row=None,
):
    root = tmp_path / "pack"
    (root / "bronze").mkdir(parents=True)
    (root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    (root / "bronze" / "scm_items.yaml").write_text(
        BRONZE_NODE_YAML, encoding="utf-8"
    )
    pack = load_pack(root)
    node = pack.bronze["scm_items"]
    profile = load_tenant_profile_from_string(PROFILE_YAML)

    log: list = []
    default_guard = {"__n": 7, "__nulled___patch_raw_0": 0, "__rt___patch_raw_0": 0}
    extract_df = _FakeDF(
        ["Id", "ItemBasePEOMaterialCost", "ItemType"],
        log=log,
        guard_row=guard_row or default_guard,
    )

    probe_df = MagicMock(name="probe_df")
    probe_df.schema = _base_schema_mock()

    extract_calls: list[dict] = []

    def fake_extract(spark, descriptor, **kwargs):
        extract_calls.append(kwargs)
        # The metadata-only probe passes NO watermark/user_schema kwargs.
        if "user_schema" not in kwargs and "watermark" not in kwargs:
            return probe_df
        return extract_df

    monkeypatch.setattr(bicc_extractor, "extract_pvo", fake_extract)
    monkeypatch.setattr(runtime, "enrich_bronze_audit_cols", lambda d, **k: d)
    monkeypatch.setattr(
        runtime, "_resolve_password",
        lambda pw: MagicMock(get_secret_value=lambda: "pw"),
    )
    monkeypatch.setattr(
        runtime, "_resolve_safety_window",
        lambda b: _dt.timedelta(hours=1),
    )
    monkeypatch.setattr(
        bronze_extract_adapter, "_table_exists", lambda spark, target: False
    )
    _install_fake_pyspark(monkeypatch)

    spark = MagicMock()
    spark.table.return_value = MagicMock(name="materialized_df")

    paths = MagicMock()
    paths.bronze.side_effect = lambda t: f"cat.bronze.{t}"

    ctx = RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="besp-adapter-test",
        active_profile_name="finance-default",
        prior_watermark={},
        mode="seed",
        bundle=_bundle(patches),
    )
    result = bronze_extract_adapter.run(
        spark, node=node, pack=pack, profile=profile,
        ctx=ctx, paths=paths, mode="seed",
    )
    return result, log, extract_calls


def test_no_patches_is_byte_identical_single_extract(tmp_path, monkeypatch):
    result, log, calls = _drive(tmp_path, monkeypatch, patches={})
    assert len(calls) == 1                       # NFR-1: no probe roundtrip
    assert calls[0].get("user_schema") is None
    assert result.applied_patch_columns == ()
    assert ("count",) in log                      # plain count path
    assert not any(op[0] == "agg" for op in log)  # no guard aggregate


def test_patched_read_probe_plus_user_schema_preserving_attrs(tmp_path, monkeypatch):
    result, log, calls = _drive(
        tmp_path, monkeypatch,
        patches={"ItemBasePEOMaterialCost": "bigint"},
    )
    assert len(calls) == 2                        # probe + real read
    user_schema = calls[1]["user_schema"]
    field = {f.name: f for f in user_schema.fields}["ItemBasePEOMaterialCost"]
    assert field.dataType.simpleString() == "bigint"
    assert dict(field.metadata) == {"comment": "kept"}   # verbatim carry
    # Unpatched fields are the ORIGINAL StructField objects.
    assert {f.name for f in user_schema.fields} == {
        "Id", "ItemBasePEOMaterialCost", "ItemType",
    }
    assert result.applied_patch_columns == ("ItemBasePEOMaterialCost",)


def test_guard_clean_drops_temps_restores_metadata_and_order(tmp_path, monkeypatch):
    result, log, _ = _drive(
        tmp_path, monkeypatch,
        patches={"ItemBasePEOMaterialCost": "bigint"},
    )
    assert ("rename", "ItemBasePEOMaterialCost", "__patch_raw_0") in log
    assert ("agg", 3) in log                      # count + nulled + roundtrip
    assert ("drop", ("__patch_raw_0",)) in log
    assert ("withMetadata", "ItemBasePEOMaterialCost", {"comment": "kept"}) in log
    selects = [op for op in log if op[0] == "select"]
    assert selects and selects[-1][1][:3] == (
        "Id", "ItemBasePEOMaterialCost", "ItemType",
    )                                             # base order restored
    assert any(op[0] == "write" for op in log)


def test_guard_violation_raises_2094_before_write(tmp_path, monkeypatch):
    with pytest.raises(CastIntegrityError) as exc:
        _drive(
            tmp_path, monkeypatch,
            patches={"ItemBasePEOMaterialCost": "bigint"},
            guard_row={"__n": 7, "__nulled___patch_raw_0": 3,
                       "__rt___patch_raw_0": 0},
        )
    msg = str(exc.value)
    assert "AIDPF-2094" in msg
    assert "ItemBasePEOMaterialCost" in msg
    assert "diagnose-encode" in msg


def test_guard_roundtrip_violation_also_raises(tmp_path, monkeypatch):
    with pytest.raises(CastIntegrityError):
        _drive(
            tmp_path, monkeypatch,
            patches={"ItemBasePEOMaterialCost": "bigint"},
            guard_row={"__n": 7, "__nulled___patch_raw_0": 0,
                       "__rt___patch_raw_0": 2},
        )


def test_unknown_patch_column_fails_loudly(tmp_path, monkeypatch):
    with pytest.raises(SchemaPatchError):
        _drive(tmp_path, monkeypatch, patches={"NoSuchColumn": "bigint"})


def test_noop_patch_reports_nothing(tmp_path, monkeypatch):
    result, log, calls = _drive(
        tmp_path, monkeypatch,
        patches={"ItemBasePEOMaterialCost": "decimal(38,0)"},
    )
    # Probe happened (patches configured) but the plan is empty → plain path.
    assert len(calls) == 2
    assert calls[1].get("user_schema") is None
    assert result.applied_patch_columns == ()
    assert not any(op[0] == "agg" for op in log)
