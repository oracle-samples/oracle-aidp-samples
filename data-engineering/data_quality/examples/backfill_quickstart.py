"""Backfill quickstart — operator walkthrough on SQLite.

Demonstrates the operator-facing surfaces shipped in
``backfill-api-and-wap-target-mode``:

1. Forward run on a partition (collects + validates + persists).
2. ``Qualifire.deactivate_metric(...)`` writes a tombstone.
3. Subsequent reads honour the tombstone (H8 contract).
4. ``Qualifire.backfill(partition_ts=...)`` reactivates the metric.
5. The matching CLI subcommands.

Run with::

    python examples/backfill_quickstart.py

The example uses an in-memory SQLite system table — no Spark / no
external infrastructure required.
"""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from qualifire import Qualifire
from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="qf_backfill_quickstart_"))
    db_path = tmp / "qf.db"

    # 1. Set up a Pandas DataFrame as the "raw" source.
    df = pd.DataFrame({
        "event_date": ["2026-04-01", "2026-04-01", "2026-04-01"],
        "amount": [10.0, 20.0, 30.0],
    })

    backend = PandasBackend()
    backend.register_table("raw_events", df)

    qf = Qualifire(
        backend=backend,
        system_table=str(db_path),
        system_table_backend="sqlite",
        owner="data-eng",
        bu="finance",
    )

    # Build a minimal config with a single threshold validation.
    config = QualifireConfig(
        owner="data-eng",
        bu="finance",
        system_table=str(db_path),
        system_table_backend="sqlite",
        datasets=[
            DatasetConfig(
                name="sales_daily",
                table="raw_events",
                partition_ts="event_date",
                partition_step="P1D",
                validations=[
                    ThresholdValidationConfig(
                        name="row_count_check",
                        collection=AggregationCollectionConfig(
                            expressions={"row_count": "COUNT(*)"},
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="row_count",
                            thresholds=ThresholdLevels(error={"min": 1}),
                        )],
                    ),
                ],
            ),
        ],
    )

    # 2. Forward run.
    print("=== Step 1: Forward run ===")
    result = qf.run_config_parsed(
        config,
        context={"ds": "2026-04-01"},
    )
    print(f"Severity: {result.overall_severity.value}")

    # 3. Deactivate the metric. Note: ``partition_ts`` must match
    # what the forward run stamped — the engine renders
    # ``partition_ts="event_date"`` against each row, and pandas
    # serializes the date as ``2026-04-01T00:00:00``. Pass the
    # exact string (or use ``partition_ts=None`` to deactivate
    # every partition).
    print("\n=== Step 2: Deactivate row_count ===")
    n = qf.deactivate_metric(
        config,
        dataset_name="sales_daily",
        metric_name="row_count",
        partition_ts="2026-04-01T00:00:00",
        note="quickstart-demo",
    )
    print(f"Wrote {n} tombstone(s)")

    # 4. Confirm the read returns None for the tombstoned key.
    storage = qf._storage
    deactivated = storage.read_collection_metric_at_partition(
        table_name="raw_events",
        metric_name="row_count",
        anchor_ts="2026-04-01T00:00:00",
    )
    print(f"Post-deactivate read: {deactivated}")

    # 5. Backfill — re-collects the metric, supersedes the tombstone.
    # Optional kwargs (defaults shown):
    #   parallelism=1       # 1..64 thread-pool workers; >1 forces
    #                       # notifiers={} (suppression race avoidance)
    #   max_partitions=10000  # cap on (start, end) range expansion
    print("\n=== Step 3: Backfill ===")
    report = qf.backfill(
        config,
        partition_ts="2026-04-01",
    )
    print(
        f"Backfill: total={report.total} refreshed={report.refreshed} "
        f"unchanged={report.unchanged} skipped={report.skipped} "
        f"errored={report.errored}"
    )
    for diff in report.partitions:
        print(
            f"  {diff.dataset_name}.{diff.metric_name} @ "
            f"{diff.partition_ts.isoformat()}: "
            f"{diff.original_value} → {diff.backfilled_value} "
            f"[{diff.status}]"
        )

    print("\nDone. Artifacts at:", tmp)


if __name__ == "__main__":
    main()
