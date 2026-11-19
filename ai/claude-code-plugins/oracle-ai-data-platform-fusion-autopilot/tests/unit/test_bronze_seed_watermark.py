"""Regression — bronze seed-shape watermark advances only on non-empty extract.

PR #23 blocking #3: the seed-shape (full-replace) branch in
``bronze_extract_adapter.run`` advanced ``output_watermark`` to
``extract_started_at - safety_window`` whenever ``mode == "seed"`` — even if
``source_delta_count == 0``. An empty initial BICC pull would then persist a
cursor of ``now - safety_window``; the next incremental run uses that cursor as
its lower bound and SKIPS any late-arriving source records older than it.

The adapter's own contract (run() docstring steps 4 + 10): the cursor advances
ONLY on a non-empty extract and carries forward ``prior_watermark`` on an empty
delta — independent of mode. Both an empty seed and an empty first-incremental
route through the seed-shape branch and must carry forward (``None`` on a true
first run → next run re-seeds via a full pull, which is correct).

These tests drive the real ``run()`` with a fake Spark + fake BICC extractor so
the two empty scenarios exercise distinct runtime entry conditions
(``mode="seed"`` vs ``prior_watermark is None``).
"""

from __future__ import annotations

import datetime as _dt
import pathlib
from unittest.mock import MagicMock

from oracle_ai_data_platform_fusion_autopilot.orchestrator import runtime
from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
    bronze_extract_adapter,
)
from oracle_ai_data_platform_fusion_autopilot.extractors import bicc as bicc_extractor
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


PACK_YAML = """
id: phase9-bronze-watermark-test
version: 1.0.0
description: bronze seed-shape watermark regression pack
compatibility:
  pluginMinVersion: 0.3.0
"""

BRONZE_NODE_YAML = """
id: erp_a
layer: bronze
implementation:
  type: bronze_extract
  datastore: ErpAExtractPVO
  biccSchema: Financial
  incrementalCapable: true
target: erp_a
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: erp_a
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
bronzeSchemaFingerprint: "sha256:bronze-watermark-test"
"""

SAFETY_WINDOW = _dt.timedelta(hours=1)
PRIOR_CURSOR = _dt.datetime(2026, 6, 1, 0, 0, 0, tzinfo=_dt.timezone.utc)


def _build_bronze_pack(tmp_path: pathlib.Path):
    root = tmp_path / "pack"
    (root / "bronze").mkdir(parents=True)
    (root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    (root / "bronze" / "erp_a.yaml").write_text(BRONZE_NODE_YAML, encoding="utf-8")
    return load_pack(root)


def _paths() -> MagicMock:
    paths = MagicMock()
    paths.bronze.side_effect = lambda t: f"cat.bronze.{t}"
    return paths


def _drive_run(
    *,
    mode: str,
    prior_watermark: _dt.datetime | None,
    delta_count: int,
    tmp_path: pathlib.Path,
    monkeypatch,
    target_exists: bool = False,
):
    """Run the bronze adapter with everything but the watermark decision faked.

    Returns the ``output_watermark`` the adapter chose.
    """
    pack = _build_bronze_pack(tmp_path)
    node = pack.bronze["erp_a"]
    profile = load_tenant_profile_from_string(PROFILE_YAML)

    # Fake BICC result df. The seed-shape branch only touches
    # .cache()/.count()/.write...saveAsTable() — all no-ops on a MagicMock.
    df = MagicMock(name="bicc_df")
    df.count.return_value = delta_count

    monkeypatch.setattr(bicc_extractor, "extract_pvo", lambda *a, **k: df)
    monkeypatch.setattr(runtime, "enrich_bronze_audit_cols", lambda d, **k: d)
    monkeypatch.setattr(
        runtime,
        "_resolve_password",
        lambda pw: MagicMock(get_secret_value=lambda: "pw"),
    )
    monkeypatch.setattr(runtime, "_resolve_safety_window", lambda b: SAFETY_WINDOW)
    monkeypatch.setattr(
        bronze_extract_adapter, "_resolve_effective_schema", lambda node, bundle: "Financial"
    )
    monkeypatch.setattr(
        bronze_extract_adapter, "_table_exists", lambda spark, target: target_exists
    )

    spark = MagicMock()
    spark.table.return_value = MagicMock(name="materialized_df")

    ctx = RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="bronze-wm-test",
        active_profile_name="finance-default",
        prior_watermark={"erp_a": prior_watermark} if prior_watermark else {},
        mode=mode,
        bundle=MagicMock(name="bundle"),
    )

    _df, output_watermark = bronze_extract_adapter.run(
        spark,
        node=node,
        pack=pack,
        profile=profile,
        ctx=ctx,
        paths=_paths(),
        mode=mode,
    )
    return output_watermark


def test_empty_seed_does_not_advance_watermark(tmp_path, monkeypatch) -> None:
    """Empty initial seed: 0 source rows, no prior cursor. Must NOT persist
    ``extract_started_at - safety_window`` — carry forward ``None`` so the
    next run re-seeds rather than skipping backdated source records."""
    wm = _drive_run(
        mode="seed",
        prior_watermark=None,
        delta_count=0,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    assert wm is None, (
        "empty seed advanced the cursor — the next incremental would skip "
        "late-arriving source records older than now - safety_window"
    )


def test_empty_first_incremental_does_not_advance_watermark(tmp_path, monkeypatch) -> None:
    """First incremental with no prior cursor downgrades to seed-shape. An
    empty extract must carry forward (``None``), not invent a cursor."""
    wm = _drive_run(
        mode="incremental",
        prior_watermark=None,
        delta_count=0,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    assert wm is None


def test_non_empty_seed_advances_watermark(tmp_path, monkeypatch) -> None:
    """Non-empty seed advances to ``extract_started_at - safety_window``."""
    wm = _drive_run(
        mode="seed",
        prior_watermark=None,
        delta_count=5,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    assert isinstance(wm, _dt.datetime), "non-empty seed must persist a cursor"
    # extract_started_at is ~now; cursor = extract_started_at - 1h. Bound it
    # generously to avoid clock flakiness while still proving advancement.
    now = _dt.datetime.now(_dt.timezone.utc)
    assert now - _dt.timedelta(minutes=5) <= wm + SAFETY_WINDOW <= now + _dt.timedelta(minutes=5)
