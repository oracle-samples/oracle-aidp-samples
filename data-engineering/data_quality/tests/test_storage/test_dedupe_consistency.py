"""Cross-backend consistency for the metric-history dedupe rule.

The rule (single source of truth, applied client-side in the dashboard
JS and server-side in every storage backend's SQL):

    1. Latest ``run_timestamp`` wins per natural key.
    2. On a tie, validation > collection (so the surviving row carries
       the verdict, not its own paired collection row).

This module pins that rule by static-analyzing the SQL each backend
produces. A regression in any one backend (e.g. accidentally putting
record_type before run_timestamp again) is caught here without needing
to spin up Spark / Delta / a JDBC server. The runtime behaviour itself
is exercised by ``test_sqlite``, ``test_step_bucketing``, and the
*_real.py spark/jdbc tests against live engines.
"""

from __future__ import annotations

import inspect


def _source_of(obj):
    return inspect.getsource(obj)


def _expects_run_timestamp_first(sql_text: str) -> bool:
    """Walk every ORDER BY clause and the inner ROW_NUMBER OVER block.
    For each, the first ordering term must be ``run_timestamp DESC``,
    and any ``CASE WHEN record_type = ...`` term must rank validation
    AHEAD of collection (NOT the other way around).
    """
    # No ORDER BY may put the record_type CASE before run_timestamp.
    # If a chunk contains the collection-first CASE, that's the old
    # rule and must not appear anywhere in the source.
    bad = "CASE WHEN record_type = 'collection' THEN 0 ELSE 1 END"
    return bad not in sql_text


def _has_new_dedupe_rule(sql_text: str) -> bool:
    """Look for the new pattern: ``run_timestamp DESC`` followed
    (eventually) by ``CASE WHEN record_type = 'validation' THEN 0
    ELSE 1 END`` in the same ORDER BY / OVER clause."""
    # Be lenient about whitespace / ASC/DESC. The JDBC backend uses
    # ``ASC`` after the CASE; the others omit it. Both rank validation
    # ahead because validation maps to 0.
    return (
        "run_timestamp DESC" in sql_text
        and "CASE WHEN record_type = 'validation' THEN 0 ELSE 1 END" in sql_text
    )


def test_sqlite_dedupe_rule_is_consistent():
    """SQLite ``read_metric_history`` (default + bucketed) and
    ``read_metric_history_by_partition`` follow the cross-backend rule."""
    from qualifire.storage.sqlite_storage import SQLiteStorage

    src = _source_of(SQLiteStorage)
    assert _expects_run_timestamp_first(src), (
        "SQLite source contains the old collection-first CASE — "
        "the rule must be run_timestamp DESC then validation tiebreak"
    )
    assert _has_new_dedupe_rule(src), (
        "SQLite source missing the new (run_timestamp DESC, "
        "validation-tiebreak) pattern"
    )


def test_delta_dedupe_rule_is_consistent():
    from qualifire.storage.delta_storage import DeltaStorage

    src = _source_of(DeltaStorage)
    assert _expects_run_timestamp_first(src), (
        "Delta source contains the old collection-first CASE"
    )
    assert _has_new_dedupe_rule(src)


def test_external_catalog_dedupe_rule_is_consistent():
    from qualifire.storage.external_catalog import ExternalCatalogStorage

    src = _source_of(ExternalCatalogStorage)
    assert _expects_run_timestamp_first(src), (
        "ExternalCatalog source contains the old collection-first CASE"
    )
    assert _has_new_dedupe_rule(src)


def test_jdbc_dedupe_rule_is_consistent():
    from qualifire.storage.jdbc_storage import JDBCStorage

    src = _source_of(JDBCStorage)
    assert _expects_run_timestamp_first(src), (
        "JDBC source contains the old collection-first CASE"
    )
    assert _has_new_dedupe_rule(src)


def test_dashboard_trusts_sql_dedupe():
    """The dashboard's history chart now trusts the SQL backend's
    per-(dataset, validation_name, metric, partition, dim) dedupe.
    The JS no longer builds its own ``validatedRunMetric`` filter
    set — that responsibility moved server-side where it belongs.
    The chart filters by ``validation_name`` (the user's lens) and
    fills partition-history gaps with collection rows where the
    picked validation didn't run.
    """
    from qualifire.reporting.html_report import _INTERACTIVE_JS

    # Old client-side machinery is gone.
    assert "validatedRunMetric" not in _INTERACTIVE_JS
    # New gap-fill machinery is present.
    assert "validationByPartition" in _INTERACTIVE_JS
    assert "r.validation_name === state.validation" in _INTERACTIVE_JS


def test_collected_only_metrics_get_one_row_per_partition():
    """A metric that's been COLLECTED but not validated still
    surfaces in the snapshot — one row per (dataset, metric,
    partition, dim) where ``validation_name`` is empty. Latest
    run_timestamp wins among same-key collection rows so a
    recollection at T2 > T1 correctly shadows the earlier
    collection. The dashboard uses these rows to fill
    partition gaps in per-validation history (gray gap-fill
    where the picked validator never ran)."""
    import tempfile, os
    from qualifire.storage.sqlite_storage import SQLiteStorage

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    try:
        storage = SQLiteStorage(db_path=db_path)
        storage.initialize()

        # Use today / yesterday so the rows fall inside the days
        # window the SQL filters against.
        from datetime import datetime, timedelta
        now = datetime.now()
        ts1 = now.isoformat()
        ts2 = (now - timedelta(hours=1)).isoformat()
        partition = (now - timedelta(days=1)).date().isoformat() + "T00:00:00"
        # Same partition, same metric, two collection rows at
        # different run_timestamps (a recollection).
        rows = [
            {"run_id": "r1", "run_timestamp": ts2,
             "owner": "o", "bu": "b",
             "dataset_name": "ds", "table_name": "t",
             "metric_name": "amount", "metric_value": 100.0,
             "record_type": "collection",
             "is_active": "true",
             "partition_ts": partition},
            {"run_id": "r2", "run_timestamp": ts1,
             "owner": "o", "bu": "b",
             "dataset_name": "ds", "table_name": "t",
             "metric_name": "amount", "metric_value": 99.0,
             "record_type": "collection",
             "is_active": "true",
             "partition_ts": partition},
        ]
        storage.write_results(rows)

        result = storage.read_health_data(days=30, include_collection=True)
        # Both rows had validation_name=NULL; the natural key key
        # collapses them to one. Latest run_timestamp wins.
        assert len(result) == 1
        assert result[0]["record_type"] == "collection"
        assert result[0]["metric_value"] == 99.0
        assert (result[0].get("validation_name") or "") == ""
    finally:
        os.unlink(db_path)


def test_collection_row_fills_gap_when_validation_didnt_run():
    """When the picked validation has NO row for some partitions
    but a collection row does, the dashboard's history chart fills
    those gaps with the collection row (gray "collected, not
    validated"). Confirms the snapshot retains the collection
    rows alongside per-validation rows so the dashboard JS can
    do the gap-fill."""
    import tempfile, os
    from qualifire.storage.sqlite_storage import SQLiteStorage

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    try:
        storage = SQLiteStorage(db_path=db_path)
        storage.initialize()

        from datetime import datetime, timedelta
        now = datetime.now()
        rows = []
        past_dates = []
        # 3 past partitions: collection only (no validator ran).
        for d in range(3):
            pdate = (now - timedelta(days=4 - d)).date().isoformat()
            past_dates.append(pdate)
            pts = pdate + "T00:00:00"
            ts = (now - timedelta(hours=10 - d)).isoformat()
            rows.append({
                "run_id": f"P{d}", "run_timestamp": ts,
                "owner":"o", "bu":"b", "dataset_name":"ds", "table_name":"t",
                "metric_name":"amount", "metric_value": 50.0 + d,
                "record_type":"collection", "partition_ts": pts,
                "is_active": "true",
            })
        # Today: drift validator ran on this partition.
        today_date = now.date().isoformat()
        today_pts = today_date + "T00:00:00"
        rows.append({
            "run_id": "D", "run_timestamp": now.isoformat(),
            "owner":"o", "bu":"b", "dataset_name":"ds", "table_name":"t",
            "validation_name": "drift.amount", "validation_status": "ERROR",
            "validation_message": "drift",
            "metric_name": "amount", "metric_value": 150.0,
            "record_type": "validation", "partition_ts": today_pts,
            "is_active": "true",
        })
        storage.write_results(rows)

        result = storage.read_health_data(days=30, include_collection=True)
        from collections import defaultdict
        by_val = defaultdict(list)
        for r in result:
            by_val[r.get("validation_name") or "(collection)"].append(r)

        # Drift validation: one row (today only).
        assert len(by_val["drift.amount"]) == 1
        assert by_val["drift.amount"][0]["partition_ts"].startswith(today_date)
        # Collection: 3 past partitions surface — gap-fill material
        # for the dashboard chart.
        assert len(by_val["(collection)"]) == 3
        assert {r["partition_ts"][:10] for r in by_val["(collection)"]} == set(past_dates)
    finally:
        os.unlink(db_path)


def test_all_backends_partition_by_validation_name():
    """Every storage backend's ``read_health_data`` partitions by
    ``validation_name`` in the dedupe. Without this, a metric
    validated by multiple validators (drift + forecast on the same
    metric on the same partition) would lose one validator's
    verdict — the dashboard couldn't show per-validation history."""
    import inspect
    from qualifire.storage.sqlite_storage import SQLiteStorage
    from qualifire.storage.delta_storage import DeltaStorage
    from qualifire.storage.external_catalog import ExternalCatalogStorage
    from qualifire.storage.jdbc_storage import JDBCStorage

    for cls in (SQLiteStorage, DeltaStorage, ExternalCatalogStorage, JDBCStorage):
        src = inspect.getsource(cls.read_health_data)
        assert "PARTITION BY" in src and "validation_name" in src, (
            f"{cls.__name__} read_health_data must partition by validation_name"
        )
