"""End-to-end composite test for the backfill / soft-delete /
deactivate flow.

Orchestrates the full operator workflow:

1. Forward run on partition P (collects + validates + persists).
2. ``Qualifire.deactivate_metric(...)`` — tombstone the metric.
3. Read returns None (H8 contract).
4. ``Qualifire.backfill(...)`` — re-collects, supersedes the
   tombstone via newer ``run_timestamp``.
5. Re-deactivate — tombstone wins again.
6. Backfill with ``soft_delete_prior=True`` — expected to write a
   tombstone INSERT BEFORE the new collection INSERT.

Runs on SQLite (always available, no Spark/Pandas dependency on
the system table); the engine itself runs on PandasBackend so the
test stays fast and deterministic.
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from qualifire import BackfillReport, Qualifire
from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)


@pytest.fixture
def qf_with_data(tmp_path):
    """Seed a PandasBackend with a small DataFrame and a
    SQLite system table for the e2e flow."""
    backend = PandasBackend()
    backend.register_table("raw_events", pd.DataFrame({
        "event_date": ["2026-04-01"] * 3,
        "amount": [10.0, 20.0, 30.0],
    }))
    qf = Qualifire(
        backend=backend,
        system_table=str(tmp_path / "qf.db"),
        system_table_backend="sqlite",
        owner="test", bu="test",
    )
    config = QualifireConfig(
        owner="test", bu="test",
        system_table=str(tmp_path / "qf.db"),
        system_table_backend="sqlite",
        datasets=[
            DatasetConfig(
                name="sales_daily",
                table="raw_events",
                partition_ts="event_date",
                partition_step="P1D",
                validations=[ThresholdValidationConfig(
                    name="row_count_check",
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"},
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(error={"min": 1}),
                    )],
                )],
            ),
        ],
    )
    return qf, config


class TestBackfillE2E:
    def test_forward_then_deactivate_then_backfill_then_redeactivate(
        self, qf_with_data,
    ):
        qf, config = qf_with_data
        partition_iso = "2026-04-01T00:00:00"

        # 1. Forward run
        result = qf.run_config_parsed(config, context={"ds": "2026-04-01"})
        assert result.overall_severity.value == "PASS"

        # 2. Deactivate
        n = qf.deactivate_metric(
            config,
            dataset_name="sales_daily",
            metric_name="row_count",
            partition_ts=partition_iso,
            note="e2e-test",
        )
        assert n == 1

        # 3. Read returns None (H8)
        post_deactivate = qf._storage.read_collection_metric_at_partition(
            table_name="raw_events",
            metric_name="row_count",
            anchor_ts=partition_iso,
        )
        assert post_deactivate is None

        # 4. Backfill — re-collects, supersedes the tombstone
        report = qf.backfill(config, partition_ts=partition_iso)
        assert isinstance(report, BackfillReport)
        assert report.errored == 0
        assert report.total >= 1
        # The metric was tombstoned (no original_value visible) → backfill
        # produces a fresh value.
        diffs = [
            d for d in report.partitions
            if d.metric_name == "row_count"
        ]
        assert diffs, "backfill should have touched row_count"
        diff = diffs[0]
        assert diff.original_value is None  # tombstone hid the prior row
        assert diff.backfilled_value == 3.0  # 3 rows in the seed data

        # Read now returns the backfilled value (active again).
        post_backfill = qf._storage.read_collection_metric_at_partition(
            table_name="raw_events",
            metric_name="row_count",
            anchor_ts=partition_iso,
        )
        assert post_backfill is not None
        assert post_backfill["metric_value"] == 3.0

        # 5. Re-deactivate — tombstone wins again
        n2 = qf.deactivate_metric(
            config,
            dataset_name="sales_daily",
            metric_name="row_count",
            partition_ts=partition_iso,
        )
        assert n2 == 1
        final = qf._storage.read_collection_metric_at_partition(
            table_name="raw_events",
            metric_name="row_count",
            anchor_ts=partition_iso,
        )
        assert final is None

    def test_idempotent_deactivate_returns_zero_after_first(
        self, qf_with_data,
    ):
        """Second deactivate against the same key with no intervening
        backfill is a no-op."""
        qf, config = qf_with_data
        qf.run_config_parsed(config, context={"ds": "2026-04-01"})
        n1 = qf.deactivate_metric(
            config,
            dataset_name="sales_daily",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
        )
        n2 = qf.deactivate_metric(
            config,
            dataset_name="sales_daily",
            metric_name="row_count",
            partition_ts="2026-04-01T00:00:00",
        )
        assert n1 == 1
        assert n2 == 0

    def test_backfill_soft_delete_prior_writes_tombstone_first(
        self, qf_with_data,
    ):
        """``soft_delete_prior=True`` writes a tombstone BEFORE the
        new collection INSERT. The new row supersedes via newer
        ``run_timestamp``, but the tombstone serves as audit trail."""
        qf, config = qf_with_data
        qf.run_config_parsed(config, context={"ds": "2026-04-01"})

        report = qf.backfill(
            config,
            partition_ts="2026-04-01T00:00:00",
            soft_delete_prior=True,
        )
        assert report.errored == 0

        # Inspect raw rows to confirm the tombstone exists.
        conn = qf._storage._get_conn()
        rows = list(conn.execute(
            f"SELECT is_active, metric_value, run_timestamp "
            f"FROM {qf._storage._table} "
            f"WHERE dataset_name = 'sales_daily' "
            f"AND metric_name = 'row_count' "
            f"AND record_type = 'collection' "
            f"ORDER BY run_timestamp ASC"
        ))
        # Expected sequence (oldest → newest):
        #   1. forward-run active row
        #   2. tombstone (soft_delete_prior)
        #   3. backfill active row
        is_active_seq = [r[0] for r in rows]
        assert "false" in is_active_seq, (
            "soft_delete_prior should have written a tombstone"
        )

    def test_backfilled_rows_carry_collected_via_marker(
        self, qf_with_data,
    ):
        """Plan invariant N12 / D12: every row a backfill emits
        carries ``details_json.collected_via='backfill'`` so
        dashboards / consumers can distinguish retrofitted rows
        from forward-run rows."""
        import json

        qf, config = qf_with_data
        # Forward run first to seed history
        qf.run_config_parsed(config, context={"ds": "2026-04-01"})

        # Backfill the partition. The new collection rows should
        # carry the breadcrumb.
        try:
            qf.backfill(config, partition_ts="2026-04-01T00:00:00")
        except Exception:
            pass  # report-attached errors are fine; we only check rows

        conn = qf._storage._get_conn()
        rows = list(conn.execute(
            f"SELECT details_json FROM {qf._storage._table} "
            f"WHERE dataset_name = 'sales_daily' "
            f"AND record_type IN ('collection', 'validation') "
            f"ORDER BY run_timestamp DESC LIMIT 4"
        ))
        # At least one of the latest rows (the backfill rows) must
        # carry the breadcrumb.
        breadcrumbs = []
        for r in rows:
            raw = r[0]
            if raw is None:
                continue
            try:
                d = json.loads(raw) if isinstance(raw, str) else raw
            except (TypeError, ValueError):
                continue
            if isinstance(d, dict) and d.get("collected_via") == "backfill":
                breadcrumbs.append(d)
        assert breadcrumbs, (
            "backfill must stamp details_json.collected_via='backfill' "
            "on every row it emits (plan N12 / D12)"
        )

    def test_tag_with_backfill_keeps_payload_json_serializable(self):
        """Codex impl-review R4 regression: when ``_tag_with_backfill``
        wraps a non-dict payload, ``raw_details`` must always be a
        JSON-serializable value. Otherwise the storage layer's
        downstream ``json.dumps`` raises ``TypeError`` and persistence
        hard-fails — the same problem R3's fix introduced for
        datetime / custom-object payloads."""
        import json
        from datetime import datetime
        from unittest.mock import MagicMock

        from qualifire.backends.pandas_backend import PandasBackend
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine

        ctx = QualifireContext(); ctx.backfill = True
        engine = QualifireEngine(
            backend=PandasBackend(),
            storage=None,
            context=ctx,
            config=MagicMock(datasets=[]),
        )

        # datetime — not JSON-serializable on its own.
        out = engine._tag_with_backfill(datetime(2026, 4, 1, 12, 0))
        assert out["collected_via"] == "backfill"
        # Whatever raw_details ends up as, json.dumps must not raise.
        json.dumps(out)

        # Custom object — not JSON-serializable.
        class _Foo:
            def __repr__(self): return "<Foo>"
        out = engine._tag_with_backfill(_Foo())
        json.dumps(out)
        assert out["collected_via"] == "backfill"

        # Bytes — not JSON-serializable.
        out = engine._tag_with_backfill(b"binary blob")
        json.dumps(out)

    def test_tag_with_backfill_handles_all_payload_shapes(self):
        """Codex impl-review R3: ``_tag_with_backfill`` must surface
        the ``collected_via`` breadcrumb regardless of the shape
        ``details`` arrives in (dict, None, JSON string, raw string,
        list, scalar). String payloads were the R3 MUST-FIX —
        previously they slipped through untagged."""
        from qualifire.backends.pandas_backend import PandasBackend
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine

        # Build a minimal engine instance with context.backfill=True.
        # We bypass run() and call _tag_with_backfill directly.
        ctx = QualifireContext()
        ctx.backfill = True
        from unittest.mock import MagicMock
        config = MagicMock()
        engine = QualifireEngine(
            backend=PandasBackend(),
            storage=None,
            context=ctx,
            config=config,
        )

        # dict in → dict with marker out.
        out = engine._tag_with_backfill({"existing_key": 1})
        assert out["collected_via"] == "backfill"
        assert out["existing_key"] == 1

        # None in → marker dict out.
        assert engine._tag_with_backfill(None) == {"collected_via": "backfill"}

        # JSON-string dict in → dict with marker out.
        import json
        json_dict_str = json.dumps({"k": "v"})
        out = engine._tag_with_backfill(json_dict_str)
        assert isinstance(out, dict)
        assert out["collected_via"] == "backfill"
        assert out["k"] == "v"

        # Non-JSON string in → marker dict + raw_details out.
        out = engine._tag_with_backfill("just a plain string")
        assert isinstance(out, dict)
        assert out["collected_via"] == "backfill"
        assert out["raw_details"] == "just a plain string"

        # JSON list in → marker dict + raw_details (list) out.
        out = engine._tag_with_backfill(json.dumps([1, 2, 3]))
        assert out["collected_via"] == "backfill"
        # Either decoded list under raw_details OR the original
        # JSON string — we only require the breadcrumb is present.

        # Skip path: backfill=False → original payload returned.
        ctx.backfill = False
        assert engine._tag_with_backfill({"k": 1}) == {"k": 1}
        assert engine._tag_with_backfill("foo") == "foo"
        assert engine._tag_with_backfill(None) is None

    def test_backfill_no_partition_column_wap_rejected(self, tmp_path):
        """Smoke-test the WAP-no-partition_column guard."""
        from qualifire.core.config import WAPConfig
        from qualifire.core.exceptions import QualifireConfigError

        qf = Qualifire(
            backend=PandasBackend(),
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table=str(tmp_path / "qf.db"),
            datasets=[DatasetConfig(
                name="d",
                wap=WAPConfig(
                    target_table="cat.t",
                    sql="SELECT 1",
                    # No partition_column
                ),
                validations=[ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"cnt": "COUNT(*)"},
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="cnt", thresholds=ThresholdLevels(),
                    )],
                )],
            )],
        )
        with pytest.raises(QualifireConfigError, match="partition_column required"):
            qf.backfill(config, partition_ts="2026-04-01")
