"""Backend-specific tests for ``read_validation_history_bulk``.

Plan §P4.3 contract: every storage backend must satisfy the bulk-read
shape with NULL-safe metric matching, per-key ``LIMIT`` semantics, and
correct per-key dispatch on the result. PR #6 review surfaced that
the regression coverage in ``tests/test_findings_sweep.py`` only
checked the engine-level ``read_validation_history_bulk.return_value``
mock — the production implementations (SQLite real CTE, Delta /
ExternalCatalog Spark temp-view JOIN, JDBC server-side IN-pair filter)
were not exercised. Each test class below pins the backend's contract:

* Multiple ``ValidationKey`` inputs.
* ``metric_name=None`` and non-NULL metrics in the same call.
* Non-default ``dimension_value``.
* Per-key correctness — each ValidationKey only sees its own rows.
* One backend read/query (or fallback to per-key reads in JDBC's
  graceful-degrade path).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from qualifire.core.models import ValidationKey


# ---------------------------------------------------------------------------
# SQLite — real CTE-based bulk read (the production path).
# ---------------------------------------------------------------------------


class TestSQLiteBulkSuppression:
    def _seeded_storage(self):
        """SQLite storage seeded with rows that exercise NULL/non-NULL
        metric_name and default/custom dimension_value combinations."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        rows = [
            # (dataset, validation, metric, dim, status)
            ("ds_a", "check.row_count", "row_count", None, "WARNING",
             "2024-01-01T01:00:00"),
            ("ds_a", "check.null_pct",  "null_pct",  None, "ERROR",
             "2024-01-01T01:00:01"),
            ("ds_a", "check.row_count", "row_count", '{"region": "US"}', "PASS",
             "2024-01-01T01:00:02"),
            ("ds_a", "check.row_count", "row_count", '{"region": "EU"}', "ERROR",
             "2024-01-01T01:00:03"),
            ("ds_b", "engine_warn",     None,         None, "WARNING",
             "2024-01-01T01:00:04"),
            # Decoy: matches dataset+validation but wrong metric — must not
            # leak into a key asking for `metric_name=None`.
            ("ds_b", "engine_warn",     "noise",      None, "ERROR",
             "2024-01-01T01:00:05"),
        ]
        for r in rows:
            ds, vn, mn, dv, status, ts = r
            storage.write_results([{
                "run_id": f"run-{ts}", "run_timestamp": ts,
                "owner": "o", "bu": "b",
                "dataset_name": ds, "table_name": "t",
                "metric_name": mn, "metric_value": 1.0,
                "collection_type": "validation",
                "validation_name": vn, "validation_type": "threshold",
                "validation_status": status, "validation_message": "",
                "notification_channel": None, "notification_status": None,
                "details_json": "{}",
                "record_type": "validation",
                "is_active": "true",
                "collector_name": None,
                "dimension_value": dv,
                "collected_at": None, "validated_at": None, "notified_at": None,
            }])
        return storage

    def test_bulk_returns_per_key_dispatch(self):
        storage = self._seeded_storage()
        keys = [
            ValidationKey("ds_a", "check.row_count", "row_count", None),
            ValidationKey("ds_a", "check.null_pct", "null_pct", None),
            ValidationKey("ds_a", "check.row_count", "row_count", '{"region": "US"}'),
            ValidationKey("ds_a", "check.row_count", "row_count", '{"region": "EU"}'),
            ValidationKey("ds_b", "engine_warn", None, None),
            # Cold key — no matching history.
            ValidationKey("ds_a", "check.absent", "absent", None),
        ]
        result = storage.read_validation_history_bulk(keys, limit=1)

        # Every key has an entry (cold keys map to []).
        assert set(result.keys()) == set(keys)

        # Per-key correctness:
        assert result[keys[0]][0]["validation_status"] == "WARNING"  # row_count default
        assert result[keys[1]][0]["validation_status"] == "ERROR"    # null_pct default
        assert result[keys[2]][0]["validation_status"] == "PASS"     # row_count US
        assert result[keys[3]][0]["validation_status"] == "ERROR"    # row_count EU
        # NULL-metric key matches the NULL-metric row, NOT the noise row.
        assert result[keys[4]][0]["validation_status"] == "WARNING"
        assert result[keys[4]][0]["metric_name"] is None
        # Cold key returns empty list.
        assert result[keys[5]] == []

    def test_bulk_empty_keys_returns_empty_dict(self):
        storage = self._seeded_storage()
        assert storage.read_validation_history_bulk([], limit=1) == {}

    def test_bulk_per_key_limit(self):
        """``limit=1`` returns at most one row per key even when
        multiple historical matches exist."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=":memory:")
        storage.initialize()
        for ts in ("2024-01-01T01:00:00", "2024-01-01T02:00:00", "2024-01-01T03:00:00"):
            storage.write_results([{
                "run_id": f"r-{ts}", "run_timestamp": ts,
                "owner": "o", "bu": "b",
                "dataset_name": "ds", "table_name": "t",
                "metric_name": "m", "metric_value": 1.0,
                "collection_type": "validation",
                "validation_name": "check.m", "validation_type": "threshold",
                "validation_status": "WARNING", "validation_message": "",
                "notification_channel": None, "notification_status": None,
                "details_json": "{}",
                "record_type": "validation",
                "is_active": "true",
                "collector_name": None, "dimension_value": None,
                "collected_at": None, "validated_at": None, "notified_at": None,
            }])
        keys = [ValidationKey("ds", "check.m", "m", None)]
        result = storage.read_validation_history_bulk(keys, limit=1)
        assert len(result[keys[0]]) == 1
        # ROW_NUMBER() partitioned + ORDER BY run_timestamp DESC → newest wins
        assert result[keys[0]][0]["run_timestamp"] == "2024-01-01T03:00:00"


# ---------------------------------------------------------------------------
# Delta — Spark temp-view-of-keys JOIN. Mocked Spark.
# ---------------------------------------------------------------------------


class _CapturingSpark:
    """Tiny Spark double that records SQL bodies and DataFrame
    creations. Lets us assert the bulk path issues exactly one
    JOIN-shaped SQL while still returning a deterministic result."""

    def __init__(self, joined_rows: list[dict]):
        self.sql_calls: list[str] = []
        self._joined_rows = joined_rows
        self.created_dfs: list = []

    def sql(self, query: str):
        self.sql_calls.append(query)
        # If this is the JOIN query (CTE form), return rows; everything
        # else (DROP VIEW etc.) returns an empty DataFrame.
        df = MagicMock()
        if "JOIN" in query.upper() and "ROW_NUMBER" in query.upper():
            df.collect.return_value = [_AsDictRow(r) for r in self._joined_rows]
        else:
            df.collect.return_value = []
        return df

    def createDataFrame(self, rows, schema=None):
        df = MagicMock()
        df.createOrReplaceTempView = MagicMock()
        self.created_dfs.append((rows, schema, df))
        return df


class _AsDictRow:
    """Spark Row-like object: ``.asDict()`` returns the underlying dict."""

    def __init__(self, d: dict):
        self._d = d

    def asDict(self):
        return dict(self._d)


class TestDeltaBulkSuppression:
    def _make_storage_with_join_result(self, rows_per_key: dict[int, list[dict]]):
        """Build DeltaStorage with a fake spark whose JOIN query returns
        rows pre-tagged with `_idx` for each input key index."""
        from qualifire.storage.delta_storage import DeltaStorage

        flat_rows = []
        for idx, rows in rows_per_key.items():
            for r in rows:
                flat_rows.append({**r, "_idx": idx, "_rn": 1})
        spark = _CapturingSpark(joined_rows=flat_rows)
        storage = DeltaStorage(spark=spark, table="cat.h")
        return storage, spark

    def test_bulk_issues_single_join_query(self):
        storage, spark = self._make_storage_with_join_result(rows_per_key={})
        keys = [
            ValidationKey("ds_a", "check.m", "m", None),
            ValidationKey("ds_a", "check.m", None, '{"region": "US"}'),
        ]
        storage.read_validation_history_bulk(keys, limit=1)

        # One createDataFrame for the keys view; SQL calls for the JOIN
        # and the DROP VIEW cleanup. The actual JOIN-shaped query:
        join_calls = [s for s in spark.sql_calls if "JOIN" in s.upper() and "ROW_NUMBER" in s.upper()]
        assert len(join_calls) == 1
        sql = join_calls[0]
        # NULL-safe equality (Spark `<=>`) for both metric and dimension.
        assert "<=>" in sql
        # NULL handling for metric_name / dimension_value is via `<=>`
        # only — no COALESCE sentinel on those columns. The is_active
        # COALESCE (added by the soft-delete read-side enforcement) is
        # a separate concern and is allowed.
        upper = sql.upper()
        non_is_active_coalesce = (
            "COALESCE(METRIC_NAME" in upper
            or "COALESCE(K._MN" in upper
            or "COALESCE(H.METRIC_NAME" in upper
            or "COALESCE(DIMENSION_VALUE" in upper
            or "COALESCE(K._DV" in upper
            or "COALESCE(H.DIMENSION_VALUE" in upper
        )
        assert not non_is_active_coalesce, (
            "COALESCE on metric_name or dimension_value is forbidden — use <=>"
        )
        # Per-key LIMIT 1 via ROW_NUMBER.
        assert "ROW_NUMBER" in sql.upper()
        # Cleanup drops the temp view.
        drop_calls = [s for s in spark.sql_calls if "DROP VIEW" in s.upper()]
        assert len(drop_calls) == 1

    def test_bulk_dispatches_rows_per_key_index(self):
        storage, spark = self._make_storage_with_join_result(rows_per_key={
            0: [{
                "validation_name": "check.m",
                "validation_type": "threshold",
                "validation_status": "WARNING",
                "validation_message": "warned",
                "run_timestamp": "2024-01-02",
                "metric_name": "m",
                "dimension_value": None,
            }],
            1: [{
                "validation_name": "check.m",
                "validation_type": "threshold",
                "validation_status": "ERROR",
                "validation_message": "errored",
                "run_timestamp": "2024-01-03",
                "metric_name": None,
                "dimension_value": '{"region": "US"}',
            }],
        })
        keys = [
            ValidationKey("ds_a", "check.m", "m", None),
            ValidationKey("ds_a", "check.m", None, '{"region": "US"}'),
        ]
        result = storage.read_validation_history_bulk(keys, limit=1)
        assert result[keys[0]][0]["validation_status"] == "WARNING"
        assert result[keys[1]][0]["validation_status"] == "ERROR"
        assert result[keys[1]][0]["metric_name"] is None

    def test_bulk_empty_keys_returns_empty_dict(self):
        storage, spark = self._make_storage_with_join_result(rows_per_key={})
        assert storage.read_validation_history_bulk([], limit=1) == {}
        assert spark.sql_calls == []  # no Spark roundtrip


# ---------------------------------------------------------------------------
# ExternalCatalog — same Spark temp-view-of-keys JOIN. Mocked Spark.
# ---------------------------------------------------------------------------


class TestExternalCatalogBulkSuppression:
    def _make_storage_with_join_result(self, rows_per_key: dict[int, list[dict]]):
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        flat_rows = []
        for idx, rows in rows_per_key.items():
            for r in rows:
                flat_rows.append({**r, "_idx": idx, "_rn": 1})
        spark = _CapturingSpark(joined_rows=flat_rows)
        storage = ExternalCatalogStorage(spark=spark, table="cat.h")
        return storage, spark

    def test_bulk_issues_single_join_query(self):
        storage, spark = self._make_storage_with_join_result(rows_per_key={})
        keys = [ValidationKey("ds", "v", "m", None)]
        storage.read_validation_history_bulk(keys, limit=1)
        join_calls = [s for s in spark.sql_calls if "JOIN" in s.upper() and "ROW_NUMBER" in s.upper()]
        assert len(join_calls) == 1
        # NULL-safe + ROW_NUMBER assertions same as Delta.
        sql = join_calls[0]
        assert "<=>" in sql
        assert "ROW_NUMBER" in sql.upper()
        # NULL handling for metric_name / dimension_value is via `<=>`
        # only — never via COALESCE on those columns. The COALESCE on
        # is_active (added by the soft-delete read-side enforcement)
        # is a separate concern and is allowed.
        upper = sql.upper()
        forbidden = (
            "COALESCE(METRIC_NAME" in upper
            or "COALESCE(K._MN" in upper
            or "COALESCE(H.METRIC_NAME" in upper
            or "COALESCE(DIMENSION_VALUE" in upper
            or "COALESCE(K._DV" in upper
            or "COALESCE(H.DIMENSION_VALUE" in upper
        )
        assert not forbidden, (
            "COALESCE on metric_name or dimension_value is forbidden — use <=>"
        )

    def test_bulk_per_key_correctness(self):
        storage, spark = self._make_storage_with_join_result(rows_per_key={
            0: [{
                "validation_name": "check.m",
                "validation_type": "threshold",
                "validation_status": "PASS",
                "validation_message": "ok",
                "run_timestamp": "2024-01-04",
                "metric_name": "m",
                "dimension_value": None,
            }],
        })
        keys = [
            ValidationKey("ds", "check.m", "m", None),
            ValidationKey("ds", "check.m", "m", '{"region": "EU"}'),  # cold
        ]
        result = storage.read_validation_history_bulk(keys, limit=1)
        assert result[keys[0]][0]["validation_status"] == "PASS"
        assert result[keys[1]] == []


# ---------------------------------------------------------------------------
# JDBC — server-side IN-pair filter, single jdbc read, client-side
# NULL-safe metric/dim filtering.
# ---------------------------------------------------------------------------


class TestJDBCBulkSuppression:
    def _make_storage(self):
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://h:5432/db",
            connection_properties={"user": "u", "password": "p"},
        )
        return storage, spark

    def test_bulk_uses_or_pair_predicate_in_one_jdbc_read(self, monkeypatch):
        storage, spark = self._make_storage()

        # Capture the predicate query and return remote rows in one call.
        captured: dict = {}

        def fake_read_jdbc(query: str):
            captured["query"] = query
            df = MagicMock()
            df.collect.return_value = [
                _AsDictRow({
                    "dataset_name": "ds_a",
                    "validation_name": "check.m",
                    "validation_type": "threshold",
                    "validation_status": "WARNING",
                    "validation_message": "w",
                    "run_timestamp": "2024-01-05T00:00:00",
                    "metric_name": "m",
                    "dimension_value": None,
                }),
                _AsDictRow({
                    "dataset_name": "ds_b",
                    "validation_name": "engine_warn",
                    "validation_type": "engine",
                    "validation_status": "WARNING",
                    "validation_message": "ew",
                    "run_timestamp": "2024-01-05T00:00:01",
                    "metric_name": None,
                    "dimension_value": None,
                }),
                # Decoy in the remote-fetched batch — must be filtered
                # out client-side because metric_name doesn't match.
                _AsDictRow({
                    "dataset_name": "ds_a",
                    "validation_name": "check.m",
                    "validation_type": "threshold",
                    "validation_status": "ERROR",
                    "validation_message": "noise",
                    "run_timestamp": "2024-01-05T00:00:02",
                    "metric_name": "noise",
                    "dimension_value": None,
                }),
            ]
            return df

        monkeypatch.setattr(storage, "_read_jdbc", fake_read_jdbc)

        keys = [
            ValidationKey("ds_a", "check.m", "m", None),
            ValidationKey("ds_b", "engine_warn", None, None),
        ]
        result = storage.read_validation_history_bulk(keys, limit=1)

        # Single JDBC read with a portable OR'd (dataset, validation) predicate
        assert "query" in captured
        sql = captured["query"]
        assert "FROM qf_history" in sql
        # OR-of-AND form (portable across Oracle/PG/MySQL/DB2)
        assert "OR" in sql.upper()
        assert "ds_a" in sql and "ds_b" in sql
        assert "check.m" in sql and "engine_warn" in sql

        # Per-key correctness. The decoy (metric=noise) is filtered out
        # client-side; ds_a key only sees its own row, ds_b key only sees
        # its NULL-metric row.
        assert len(result[keys[0]]) == 1
        assert result[keys[0]][0]["validation_message"] == "w"
        assert len(result[keys[1]]) == 1
        assert result[keys[1]][0]["metric_name"] is None
        assert result[keys[1]][0]["validation_message"] == "ew"

    def test_bulk_falls_back_to_per_key_on_remote_failure(self, monkeypatch):
        """If the bulk pushdown query fails (rare, dialect-incompatible
        edge), the impl falls back to per-key reads — same shape, just
        slower. Ensures the contract still returns correct results."""
        storage, spark = self._make_storage()

        def fail_read(query: str):
            raise RuntimeError("bulk query rejected by driver")

        per_key_calls: list = []

        def fake_per_key(**kwargs):
            per_key_calls.append(kwargs)
            return [{"dataset_name": kwargs["dataset_name"]}]

        monkeypatch.setattr(storage, "_read_jdbc", fail_read)
        monkeypatch.setattr(storage, "read_validation_history", fake_per_key)

        keys = [
            ValidationKey("ds", "v1", "m1", None),
            ValidationKey("ds", "v2", "m2", None),
        ]
        result = storage.read_validation_history_bulk(keys, limit=1)
        # Fallback hit each key once.
        assert len(per_key_calls) == 2
        assert all(k in result for k in keys)
        assert all(len(result[k]) == 1 for k in keys)

    def test_bulk_falls_back_to_per_key_at_limit_3(self, monkeypatch):
        """T3.7 (codex impl-review R1 MEDIUM): the JDBC fallback path
        forwards the operator's `limit` to the per-key reads — proves
        the multi-row contract holds across the fallback shape, not
        just `limit=1`.
        """
        storage, spark = self._make_storage()

        def fail_read(query: str):
            raise RuntimeError("bulk query rejected by driver")

        per_key_calls: list = []

        def fake_per_key(**kwargs):
            per_key_calls.append(kwargs)
            # Return 3 rows so the per-key result honours limit=3.
            return [
                {"dataset_name": kwargs["dataset_name"], "i": 0},
                {"dataset_name": kwargs["dataset_name"], "i": 1},
                {"dataset_name": kwargs["dataset_name"], "i": 2},
            ]

        monkeypatch.setattr(storage, "_read_jdbc", fail_read)
        monkeypatch.setattr(storage, "read_validation_history", fake_per_key)

        keys = [ValidationKey("ds", "v", "m", None)]
        result = storage.read_validation_history_bulk(keys, limit=3)
        # Forwarded limit value — fallback uses limit=3, not the
        # default limit=1.
        assert per_key_calls[0]["limit"] == 3
        assert len(result[keys[0]]) == 3

    def test_bulk_predicate_or_group_parenthesized(self, monkeypatch):
        """Codex impl-review R1 HIGH (precedence bug): the JDBC bulk
        query must wrap the OR-joined dataset/validation predicate in
        parentheses so ``record_type='validation'`` applies to the
        whole group, not just the trailing pair.
        """
        storage, spark = self._make_storage()

        captured: dict = {}

        def fake_read_jdbc(query: str):
            captured["query"] = query
            df = MagicMock()
            df.collect.return_value = []
            return df

        monkeypatch.setattr(storage, "_read_jdbc", fake_read_jdbc)
        keys = [
            ValidationKey("ds_a", "v1", "m1", None),
            ValidationKey("ds_b", "v2", "m2", None),
        ]
        storage.read_validation_history_bulk(keys, limit=1)

        sql = captured["query"]
        # The OR group is parenthesized BEFORE
        # `AND record_type = 'validation'`.
        upper = sql.upper().replace(" ", "")
        assert ")AND" in upper or ")\nAND" in upper or ")\tAND" in upper, sql
        assert "RECORD_TYPE='VALIDATION'" in sql.upper().replace(" ", "")

    def test_bulk_empty_keys_short_circuits(self):
        storage, spark = self._make_storage()
        assert storage.read_validation_history_bulk([], limit=1) == {}
        # No JDBC roundtrip
        spark.read.jdbc.assert_not_called()
