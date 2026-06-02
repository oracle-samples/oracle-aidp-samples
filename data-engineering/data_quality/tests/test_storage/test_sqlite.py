"""Tests for SQLite storage backend."""

from qualifire.storage.sqlite_storage import SQLiteStorage


class TestSQLiteStorage:
    def _make_storage(self) -> SQLiteStorage:
        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        return storage

    def test_initialize_creates_table(self):
        storage = self._make_storage()
        conn = storage._get_conn()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='qualifire_history'"
        )
        assert cursor.fetchone() is not None

    def test_write_and_read_metric_history(self):
        storage = self._make_storage()
        storage.write_results([
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "test",
                "bu": "test",
                "dataset_name": "ds1",
                "table_name": "t1",
                "metric_name": "count",
                "metric_value": 100.0,
                "collection_type": "aggregation",
                "validation_name": "v1",
                "validation_type": "threshold",
                "validation_status": "PASS",
                "validation_message": "ok",
                "notification_channel": None,
                "notification_status": None,
                "details_json": {"extra": "info"},
                "partition_ts": "2024-01-01T00:00:00",
            },
            {
                "run_id": "r2",
                "run_timestamp": "2024-01-02T00:00:00",
                "owner": "test",
                "bu": "test",
                "dataset_name": "ds1",
                "table_name": "t1",
                "metric_name": "count",
                "metric_value": 110.0,
                "collection_type": "aggregation",
                "validation_name": "v1",
                "validation_type": "threshold",
                "validation_status": "PASS",
                "validation_message": "ok",
                "notification_channel": None,
                "notification_status": None,
                "details_json": {},
                "partition_ts": "2024-01-02T00:00:00",
            },
        ])

        # Plan H1/H2: read_metric_history dedups per partition_ts.
        # Two distinct partitions ⇒ two history rows (was raw rows).
        history = storage.read_metric_history(table_name="t1", metric_name="count", limit=10)
        assert len(history) == 2
        assert float(history[0]["metric_value"]) == 110.0

    def test_read_latest_run(self):
        storage = self._make_storage()
        storage.write_results([
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "t",
                "bu": "t",
                "dataset_name": "ds1",
                "table_name": "t1",
                "metric_name": "m",
                "metric_value": 1.0,
                "collection_type": "a",
                "validation_name": "v",
                "validation_type": "t",
                "validation_status": "PASS",
                "validation_message": "ok",
                "notification_channel": None,
                "notification_status": None,
                "details_json": {},
            },
        ])
        latest = storage.read_latest_run("ds1")
        assert latest is not None
        assert latest["run_id"] == "r1"

    def test_read_latest_run_empty(self):
        storage = self._make_storage()
        assert storage.read_latest_run("nonexistent") is None

    def test_read_validation_history(self):
        storage = self._make_storage()
        storage.write_results([
            {
                "run_id": f"r{i}",
                "run_timestamp": f"2024-01-0{i}T00:00:00",
                "owner": "t",
                "bu": "t",
                "dataset_name": "ds1",
                "table_name": "t1",
                "metric_name": "m",
                "metric_value": float(i),
                "collection_type": "a",
                "validation_name": "v1",
                "validation_type": "threshold",
                "validation_status": "PASS" if i < 3 else "WARNING",
                "validation_message": "msg",
                "notification_channel": None,
                "notification_status": None,
                "details_json": {},
                "record_type": "validation",
                "is_active": "true",
                "collector_name": None,
                "dimension_value": None,
                "partition_ts": f"2024-01-0{i}T00:00:00",
            }
            for i in range(1, 4)
        ])
        # Plan H1/H2: read_validation_history dedups per partition_ts.
        # Distinct partition_ts ⇒ three rows (was raw rows).
        history = storage.read_validation_history("ds1", "v1", limit=5)
        assert len(history) == 3

    def test_write_collection_rows(self):
        storage = self._make_storage()
        storage.write_results([
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "test",
                "bu": "test",
                "dataset_name": "ds1",
                "table_name": "t1",
                "metric_name": "row_count",
                "metric_value": 500.0,
                "collection_type": "aggregation",
                "validation_name": None,
                "validation_type": None,
                "validation_status": None,
                "validation_message": None,
                "notification_channel": None,
                "notification_status": None,
                "details_json": {},
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "count_last_7d",
                "dimension_value": "2024-01-01",
            },
        ])
        history = storage.read_metric_history(
            table_name="t1",
            metric_name="row_count",
            limit=10,
            dimension_value="2024-01-01",
        )
        assert len(history) == 1
        assert history[0]["record_type"] == "collection"
        assert history[0]["collector_name"] == "count_last_7d"
        assert history[0]["dimension_value"] == "2024-01-01"

    def test_validation_wins_tiebreak_at_same_run_timestamp(self):
        """When a validator emits a vrow + crow at the same instant
        (typical same-call pattern), the dedupe rule's tiebreaker —
        latest run_timestamp wins, then validation > collection —
        keeps the validation row so the surviving record carries
        the verdict (status / message) rather than its own paired
        collection row. Mirrors the dashboard JS dedupe."""
        storage = self._make_storage()
        storage.write_results([
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "t", "bu": "t",
                "dataset_name": "ds1", "table_name": "t1",
                "metric_name": "count", "metric_value": 100.0,
                "collection_type": "aggregation",
                "validation_name": "v1", "validation_type": "threshold",
                "validation_status": "PASS", "validation_message": "ok",
                "notification_channel": None, "notification_status": None,
                "details_json": {},
                "record_type": "validation",
                "is_active": "true",
                "collector_name": None,
                "dimension_value": None,
            },
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "t", "bu": "t",
                "dataset_name": "ds1", "table_name": "t1",
                "metric_name": "count", "metric_value": 100.0,
                "collection_type": "aggregation",
                "validation_name": None, "validation_type": None,
                "validation_status": None, "validation_message": None,
                "notification_channel": None, "notification_status": None,
                "details_json": {},
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "threshold.ds1",
                "dimension_value": None,
            },
        ])
        history = storage.read_metric_history(table_name="t1", metric_name="count", limit=10)
        assert len(history) == 1
        assert history[0]["record_type"] == "validation"
        assert history[0]["validation_status"] == "PASS"

    def test_recollection_at_later_timestamp_shadows_validation(self):
        """Recollection at T2 > T1 must shadow an earlier validation
        verdict — the underlying value was rewritten and the verdict
        is now stale. Pure wall-clock dedupe (no record_type override)
        ensures the dashboard and SQL agree."""
        storage = self._make_storage()
        storage.write_results([
            # T1: validation (with paired collection at the same instant)
            {"run_id": "r1", "run_timestamp": "2024-01-01T10:00:00",
             "owner": "t", "bu": "t",
             "dataset_name": "ds1", "table_name": "t1",
             "metric_name": "count", "metric_value": 150.0,
             "collection_type": "aggregation",
             "validation_name": "v1", "validation_type": "threshold",
             "validation_status": "ERROR", "validation_message": "drift",
             "notification_channel": None, "notification_status": None,
             "details_json": {}, "record_type": "validation",
             "collector_name": None, "dimension_value": None,
             "partition_ts": "2024-01-01"},
            # T2 > T1: recollection only — value rewritten to 99
            {"run_id": "r2", "run_timestamp": "2024-01-01T11:00:00",
             "owner": "t", "bu": "t",
             "dataset_name": "ds1", "table_name": "t1",
             "metric_name": "count", "metric_value": 99.0,
             "collection_type": "aggregation",
             "validation_name": None, "validation_type": None,
             "validation_status": None, "validation_message": None,
             "notification_channel": None, "notification_status": None,
             "details_json": {}, "record_type": "collection",
             "collector_name": "threshold.ds1", "dimension_value": None,
             "partition_ts": "2024-01-01"},
        ])
        history = storage.read_metric_history(table_name="t1", metric_name="count", limit=10)
        assert len(history) == 1
        assert history[0]["record_type"] == "collection"
        assert history[0]["metric_value"] == 99.0

    def test_schema_migration_adds_missing_columns(self):
        """Initialize on an old-schema table should add missing columns."""
        storage = SQLiteStorage(db_path=":memory:")
        conn = storage._get_conn()

        # Create table with only the original 16 columns (no record_type, etc.)
        conn.execute(f"""
            CREATE TABLE qualifire_history (
                run_id TEXT,
                run_timestamp TEXT,
                owner TEXT,
                bu TEXT,
                dataset_name TEXT,
                table_name TEXT,
                metric_name TEXT,
                metric_value REAL,
                collection_type TEXT,
                validation_name TEXT,
                validation_type TEXT,
                validation_status TEXT,
                validation_message TEXT,
                notification_channel TEXT,
                notification_status TEXT,
                details_json TEXT
            )
        """)
        conn.commit()

        # Now initialize — should add missing columns via ALTER TABLE
        storage.initialize()

        # Check that new columns exist
        cursor = conn.execute("PRAGMA table_info(qualifire_history)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        assert "record_type" in existing_cols
        assert "collector_name" in existing_cols
        assert "dimension_value" in existing_cols
        assert "collected_at" in existing_cols
        assert "validated_at" in existing_cols
        assert "notified_at" in existing_cols

    def test_initialize_is_idempotent(self):
        """Calling initialize() twice should not fail."""
        storage = self._make_storage()
        storage.initialize()  # second call
        # Should not raise — verify table still works
        storage.write_results([
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "t", "bu": "t",
                "dataset_name": "ds1", "table_name": "t1",
                "metric_name": "m", "metric_value": 1.0,
                "collection_type": "a",
                "validation_name": "v", "validation_type": "t",
                "validation_status": "PASS", "validation_message": "ok",
                "notification_channel": None, "notification_status": None,
                "details_json": {},
                "record_type": "validation",
                "is_active": "true",
                "collector_name": None, "dimension_value": None,
                "collected_at": None, "validated_at": None, "notified_at": None,
            },
        ])
        history = storage.read_metric_history(table_name="t1", metric_name="m", limit=10)
        assert len(history) == 1

    def test_write_works_after_migration(self):
        """After schema migration, writing rows with new columns should work."""
        storage = SQLiteStorage(db_path=":memory:")
        conn = storage._get_conn()

        # Create old-schema table
        conn.execute("""
            CREATE TABLE qualifire_history (
                run_id TEXT, run_timestamp TEXT, owner TEXT, bu TEXT,
                dataset_name TEXT, table_name TEXT, metric_name TEXT,
                metric_value REAL, collection_type TEXT, validation_name TEXT,
                validation_type TEXT, validation_status TEXT,
                validation_message TEXT, notification_channel TEXT,
                notification_status TEXT, details_json TEXT
            )
        """)
        conn.commit()

        # Initialize triggers migration
        storage.initialize()

        # Write a row with new columns
        storage.write_results([
            {
                "run_id": "r1",
                "run_timestamp": "2024-01-01T00:00:00",
                "owner": "t", "bu": "t",
                "dataset_name": "ds1", "table_name": "t1",
                "metric_name": "count", "metric_value": 100.0,
                "collection_type": "aggregation",
                "validation_name": None, "validation_type": None,
                "validation_status": None, "validation_message": None,
                "notification_channel": None, "notification_status": None,
                "details_json": {},
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "count_check",
                "dimension_value": None,
                "collected_at": "2024-01-01T00:00:00",
                "validated_at": None, "notified_at": None,
            },
        ])

        # Verify new columns are readable
        history = storage.read_metric_history(table_name="t1", metric_name="count", limit=10)
        assert len(history) == 1
        assert history[0]["record_type"] == "collection"
        assert history[0]["collector_name"] == "count_check"


