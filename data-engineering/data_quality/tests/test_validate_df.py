"""Tests for Qualifire.validate(df=...) — the df must actually be validated.

Covers P1.1 of codex-review-fixes:
- Regression: df= takes precedence over a same-named permanent table (both backends).
- Teardown: the engine-generated temp view is removed after success and after raise.
- Collision: two back-to-back validate(df=...) calls in the same session do not error.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock

import pandas as pd
import pytest

from qualifire.api import Qualifire
from qualifire.backends.pandas_backend import PandasBackend
from qualifire.core.exceptions import QualifireValidationError
from qualifire.core.models import Severity


def _threshold_row_count_over(min_rows: int):
    return Qualifire.threshold_check(
        aggregations={"row_count": "COUNT(*)"},
        rules=[{"metric": "row_count", "thresholds": {"error": {"min": min_rows}}}],
    )


# ---------------------------------------------------------------------------
# Pandas path — uses real PandasBackend + pandasql
# ---------------------------------------------------------------------------


class TestValidateDfPandas:
    def test_df_takes_precedence_over_registered_table(self):
        """validate(df=fresh) must metric the df, not the same-named permanent table."""
        stale = pd.DataFrame({"id": [1, 2]})  # 2 rows
        fresh = pd.DataFrame({"id": list(range(10))})  # 10 rows

        backend = PandasBackend(tables={"prod_table": stale})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        # error floor of 5: stale (2 rows) would raise; fresh (10 rows) passes.
        result = qf.validate(
            table="prod_table",
            df=fresh,
            validations=[_threshold_row_count_over(min_rows=5)],
        )
        assert result.overall_severity == Severity.PASS

    def test_teardown_on_success_removes_temp_view(self):
        fresh = pd.DataFrame({"id": list(range(10))})
        backend = PandasBackend(tables={"prod_table": pd.DataFrame({"id": [1]})})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        before = set(backend._tables)
        qf.validate(
            table="prod_table",
            df=fresh,
            validations=[_threshold_row_count_over(min_rows=5)],
        )
        after = set(backend._tables)
        # Only the pre-existing table should remain; no engine temp views left behind.
        assert after == before
        assert "prod_table" in after

    def test_teardown_on_error_removes_temp_view(self):
        """Even on ERROR severity (which raises), the temp view must be cleaned up."""
        fresh = pd.DataFrame({"id": [1, 2]})  # 2 rows — below floor of 5
        backend = PandasBackend(tables={"prod_table": pd.DataFrame({"id": [1]})})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        with pytest.raises(QualifireValidationError):
            qf.validate(
                table="prod_table",
                df=fresh,
                validations=[_threshold_row_count_over(min_rows=5)],
            )
        assert set(backend._tables) == {"prod_table"}

    def test_back_to_back_calls_do_not_collide(self):
        """Two consecutive validate(df=...) calls with the same dataset name must not error."""
        backend = PandasBackend(tables={})
        qf = Qualifire(backend=backend, owner="o", bu="b")
        df_a = pd.DataFrame({"id": list(range(10))})
        df_b = pd.DataFrame({"id": list(range(12))})

        r1 = qf.validate(
            table="ds", df=df_a, validations=[_threshold_row_count_over(min_rows=5)]
        )
        r2 = qf.validate(
            table="ds", df=df_b, validations=[_threshold_row_count_over(min_rows=5)]
        )
        assert r1.overall_severity == Severity.PASS
        assert r2.overall_severity == Severity.PASS
        # No leaked temp views.
        assert backend._tables == {}


# ---------------------------------------------------------------------------
# Spark path — mocked. Pre-findings-sweep the engine branched on
# `hasattr(backend, "register_table")` to decide between Pandas and Spark
# materialization. After P4.5 both methods are on the Backend Protocol and
# the engine calls them unconditionally, so a Spark-like mock just needs
# to expose `spark`. We assert temp-view registration via the new
# `register_table` Protocol method and cleanup via `drop_temp_view`.
# ---------------------------------------------------------------------------


def _spark_like_backend(agg_return):
    """Mock a Spark backend: ``spark`` attr present, register_table /
    drop_temp_view auto-stubbed by MagicMock. The legacy
    ``createOrReplaceTempView`` assertion still works because tests
    pass a MagicMock as ``df`` and the SparkBackend's
    ``register_table`` would call ``df.createOrReplaceTempView``;
    we route the same call through ``backend.register_table(view, df)``
    under MagicMock, so we now check the registration via that
    protocol method's first arg and observe
    ``df.createOrReplaceTempView`` only when the test explicitly
    wires it.
    """
    backend = MagicMock()
    backend.spark = MagicMock()
    backend.execute_sql.return_value = MagicMock(columns=["row_count"])
    backend.get_aggregations.return_value = agg_return
    backend.sample_records.return_value = MagicMock()
    # Wire register_table to also call df.createOrReplaceTempView so legacy
    # assertions on the df mock still capture the registration.
    def _register(name, df):
        df.createOrReplaceTempView(name)
    backend.register_table.side_effect = _register
    return backend


class TestValidateDfSpark:
    def test_df_is_registered_and_metriced(self):
        """The engine must call createOrReplaceTempView on df, and metrics must hit the view."""
        backend = _spark_like_backend({"row_count": 5000})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        df = MagicMock(name="user_df")
        result = qf.validate(
            table="db.prod_table",
            df=df,
            validations=[_threshold_row_count_over(min_rows=100)],
        )

        assert result.overall_severity == Severity.PASS
        # df was registered as a temp view (Spark branch).
        df.createOrReplaceTempView.assert_called_once()
        temp_view_name = df.createOrReplaceTempView.call_args[0][0]
        # Engine uses its own name prefix, not the user table.
        assert temp_view_name.startswith("_qf_")
        assert temp_view_name != "db.prod_table"
        # Aggregations were pulled from the temp view, not the user-supplied table.
        agg_calls = [c.args for c in backend.get_aggregations.call_args_list]
        assert all(call[0] == temp_view_name for call in agg_calls)
        assert all(call[0] != "db.prod_table" for call in agg_calls)

    def test_teardown_drops_view_on_success(self):
        backend = _spark_like_backend({"row_count": 5000})
        qf = Qualifire(backend=backend, owner="o", bu="b")
        df = MagicMock(name="user_df")

        qf.validate(
            table="t",
            df=df,
            validations=[_threshold_row_count_over(min_rows=100)],
        )
        temp_view_name = df.createOrReplaceTempView.call_args[0][0]

        # P4.5: cleanup goes through ``backend.drop_temp_view``, not raw
        # ``DROP VIEW`` SQL.
        drop_views = [c.args[0] for c in backend.drop_temp_view.call_args_list]
        assert temp_view_name in drop_views, (
            f"Expected drop_temp_view({temp_view_name!r}), got: {drop_views!r}"
        )

    def test_teardown_drops_view_on_raise(self):
        # 2 rows — below error floor of 5 → ERROR severity raises.
        backend = _spark_like_backend({"row_count": 2})
        qf = Qualifire(backend=backend, owner="o", bu="b")
        df = MagicMock(name="user_df")

        with pytest.raises(QualifireValidationError):
            qf.validate(
                table="t",
                df=df,
                validations=[_threshold_row_count_over(min_rows=5)],
            )

        temp_view_name = df.createOrReplaceTempView.call_args[0][0]
        drop_views = [c.args[0] for c in backend.drop_temp_view.call_args_list]
        assert temp_view_name in drop_views

    def test_back_to_back_calls_do_not_collide(self):
        backend = _spark_like_backend({"row_count": 5000})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        df_a = MagicMock(name="df_a")
        df_b = MagicMock(name="df_b")
        qf.validate(
            table="ds", df=df_a, validations=[_threshold_row_count_over(min_rows=100)]
        )
        qf.validate(
            table="ds", df=df_b, validations=[_threshold_row_count_over(min_rows=100)]
        )

        # Each call registered its own df; view names differ (different run_ids).
        name_a = df_a.createOrReplaceTempView.call_args[0][0]
        name_b = df_b.createOrReplaceTempView.call_args[0][0]
        assert name_a != name_b

    def test_fqn_table_name_is_sanitized_in_view(self):
        """Regression for Codex adversarial finding #2.

        When the caller passes a fully-qualified table name containing dots
        or hyphens, the engine-generated temp view identifier must be a
        single, SQL-safe name (``[A-Za-z0-9_]``). A raw ``db.prod-table``
        embedded in ``_qf_...`` would be parsed by Spark as a qualified
        reference and blow up at DROP VIEW / SELECT time.
        """
        backend = _spark_like_backend({"row_count": 5000})
        qf = Qualifire(backend=backend, owner="o", bu="b")
        df = MagicMock(name="user_df")

        qf.validate(
            table="db.prod-table",
            df=df,
            validations=[_threshold_row_count_over(min_rows=100)],
        )

        view = df.createOrReplaceTempView.call_args[0][0]
        # The temp view identifier must be a single token — no dots, hyphens,
        # or other characters that Spark would parse as a qualified name.
        assert "." not in view
        assert "-" not in view
        # The drop_temp_view call at teardown references the sanitized name.
        drop_views = [c.args[0] for c in backend.drop_temp_view.call_args_list]
        assert view in drop_views
        assert "db.prod-table" not in drop_views


# ---------------------------------------------------------------------------
# Identity preservation — Codex adversarial finding #1.
# Temp-view swap must not leak into the persistence layer or the history
# lookup key. These tests use real SQLite storage + PandasBackend so the
# end-to-end write/read path is exercised without needing Spark.
# ---------------------------------------------------------------------------


class TestValidateDfIdentityPreservation:
    def test_persistence_uses_caller_table_not_temp_view(self, tmp_path):
        """System-table row must record ``table_name = caller's table``.

        If the temp view swap leaks into persistence, each ``validate(df=...)``
        would write under a unique ``_qf_<run_id>`` key and future runs
        would never find prior history for the same logical table.
        """
        db = tmp_path / "qf.db"
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(db),
            system_table_backend="sqlite",
        )

        df = pd.DataFrame({"id": list(range(10))})
        qf.validate(
            table="prod_table",
            df=df,
            validations=[_threshold_row_count_over(min_rows=5)],
        )

        import sqlite3
        conn = sqlite3.connect(str(db))
        rows = list(conn.execute(
            "SELECT DISTINCT table_name FROM qualifire_history"
        ))
        conn.close()

        table_names = {r[0] for r in rows}
        assert table_names == {"prod_table"}, (
            f"Expected persistence under caller's 'prod_table', got: {table_names}"
        )
        # And emphatically: the temp view name must NOT have leaked in.
        assert not any(n and n.startswith("_qf_") for n in table_names)

    def test_dataset_result_table_reflects_caller_not_temp_view(self, tmp_path):
        """``DatasetResult.table`` is what downstream reporting keys on —
        it must be the caller's table, not the engine's temp view name."""
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
        )

        df = pd.DataFrame({"id": list(range(10))})
        result = qf.validate(
            table="prod_table",
            df=df,
            validations=[_threshold_row_count_over(min_rows=5)],
        )

        assert len(result.datasets) == 1
        assert result.datasets[0].table == "prod_table"
        assert not result.datasets[0].table.startswith("_qf_")

    def test_drift_check_finds_prior_history_under_logical_table(self, tmp_path):
        """End-to-end history lookup test.

        Seed the system table with prior metric rows keyed on the caller's
        logical table name, then run ``validate(df=...)`` with a drift check.
        If the engine mis-keys the history lookup on the temp view name,
        the drift check would see zero history and the assertion would fail.
        """
        from qualifire.storage.sqlite_storage import SQLiteStorage

        db = tmp_path / "qf.db"
        storage = SQLiteStorage(db_path=str(db))
        storage.initialize()

        # Seed history: 3 prior runs, all under logical table "prod_table".
        # Partition-anchored history reads now require partition_ts on each
        # seed row and an anchor that lines up via anchor − k·step. Anchor
        # = 2026-01-04, step = P1D ⇒ k=1..3 lookups land on 2026-01-03,
        # 2026-01-02, 2026-01-01 (the seeded partition_ts values).
        storage.write_results([
            {
                "run_id": f"run_{i}",
                "run_timestamp": f"2026-01-0{i+1}T00:00:00",
                "owner": "o",
                "bu": "b",
                "dataset_name": "prod_table",
                "table_name": "prod_table",
                "metric_name": "row_count",
                "metric_value": 100.0,
                "collection_type": "aggregation",
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "drift.prod_table",
                "partition_ts": f"2026-01-0{i+1}T00:00:00",
            }
            for i in range(3)
        ])

        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(db),
            system_table_backend="sqlite",
        )

        drift = Qualifire.drift_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{
                "metric": "row_count",
                "compare": {"to": "mean", "past_values": 3, "step": "P1D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -50, "max": 50}}},
            }],
        )

        df = pd.DataFrame({"id": list(range(102))})  # ~2% diff from history mean
        result = qf.validate(
            table="prod_table", df=df, validations=[drift],
            partition_ts="'2026-01-04'",
            partition_step="P1D",
        )

        # The drift check must have found the 3 seeded history rows. If the
        # lookup key leaked the temp view name, past_values would be empty
        # and the validator would emit a "missing history" warning instead
        # of a PASS drift result.
        drift_vrs = [
            vr for ds in result.datasets
            for vr in ds.validation_results
            if vr.validation_type == "drift"
        ]
        assert drift_vrs, "Expected at least one drift validation result"
        # Positive assertion: the cold-start branch in
        # qualifire/validation/historical.py:114 emits
        # details={"cold_start": True} with a "No historical data for
        # ..., skipping comparison" message — *not* the "historical
        # values" message at line 100. If we mis-route the lookup key
        # and past_values is empty, we hit cold_start. So grepping
        # messages alone gives a false green. Verify no drift VR is
        # a cold-start — that guarantees a real comparison ran.
        cold_starts = [vr for vr in drift_vrs if vr.details.get("cold_start")]
        assert not cold_starts, (
            f"Drift check did not find seeded history — lookup key was mis-routed "
            f"and history query returned zero rows. "
            f"Got: {[(vr.severity.value, vr.message, vr.details) for vr in drift_vrs]}"
        )


# ---------------------------------------------------------------------------
# View-name injectivity — Codex adversarial review round 2.
# Naive sanitization (``[^A-Za-z0-9_]`` -> ``_``) collapses distinct
# dataset identities onto the same temp view name. With
# ``dataset_parallelism > 1``, two datasets in one run could then trample
# each other's data or DROP VIEW mid-validation.
# ---------------------------------------------------------------------------


class TestViewNameInjectivity:
    def _engine(self):
        """Build a minimal engine so we can call ``_make_view_name`` directly."""
        from qualifire.core.config import QualifireConfig
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine

        backend = MagicMock()
        # Engine constructor requires a config with at least one dataset;
        # the dataset body doesn't matter for _make_view_name unit tests.
        cfg = QualifireConfig(
            owner="o",
            bu="b",
            system_table="",
            datasets=[{"name": "stub", "table": "stub"}],  # type: ignore[list-item]
        )
        return QualifireEngine(
            backend=backend,
            storage=None,
            context=QualifireContext(),
            config=cfg,
        )

    def test_punctuation_variants_do_not_collide(self):
        """``orders.us`` and ``orders-us`` must not share a view name."""
        eng = self._engine()
        v_dot = eng._make_view_name("orders.us")
        v_dash = eng._make_view_name("orders-us")
        v_under = eng._make_view_name("orders_us")
        assert v_dot != v_dash
        assert v_dot != v_under
        assert v_dash != v_under

    def test_long_names_diverging_past_truncation_do_not_collide(self):
        """Two long names that only differ after the sanitize-truncation
        boundary must still produce distinct view names."""
        eng = self._engine()
        prefix = "a" * 80
        a = eng._make_view_name(prefix + "_alpha")
        b = eng._make_view_name(prefix + "_beta")
        assert a != b

    def test_view_name_stays_within_identifier_limit(self):
        """Keep the view identifier short enough for the strictest common
        SQL dialect (PostgreSQL: 63 chars). If this assertion ever fails,
        review the digest / prefix / run_id budget in ``_make_view_name``."""
        eng = self._engine()
        name = eng._make_view_name("x" * 200 + ".with.dots-and-hyphens")
        assert len(name) <= 63

    def test_view_name_contains_only_safe_chars(self):
        eng = self._engine()
        name = eng._make_view_name("db.prod-table!@#")
        assert re.match(r"^[A-Za-z0-9_]+$", name), f"unsafe chars in {name!r}"


# ---------------------------------------------------------------------------
# End-to-end collision test under dataset parallelism. Uses the run_config
# path with two datasets whose names would collapse under naive sanitization.
# With the fix, each dataset gets its own temp view and neither observes
# the other's data.
# ---------------------------------------------------------------------------


class TestParallelDatasetNoCollision:
    def test_parallel_datasets_with_colliding_sanitized_names(self, tmp_path):
        import yaml
        from qualifire.api import Qualifire

        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
        )

        df_small = pd.DataFrame({"id": list(range(3))})   # 3 rows
        df_large = pd.DataFrame({"id": list(range(100))}) # 100 rows

        # Write a config with two datasets whose sanitized names collide.
        # If the view-name collision reappears, one dataset's materialized
        # view would overwrite the other's and row counts would mismatch.
        config_dict = {
            "owner": "o",
            "bu": "b",
            "system_table": str(tmp_path / "qf.db"),
            "system_table_backend": "sqlite",
            "dataset_parallelism": 2,
            "datasets": [
                {"name": "orders.us", "table": "orders_dot"},
                {"name": "orders-us", "table": "orders_dash"},
            ],
        }
        # Register the two source tables so they resolve.
        backend.register_table("orders_dot", df_small)
        backend.register_table("orders_dash", df_large)

        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text(yaml.dump(config_dict))

        # Drive through run_config so we exercise dataset_parallelism.
        # We build the validations programmatically for each dataset.
        from qualifire.core.config import load_config
        cfg = load_config(str(cfg_path))
        # Manually attach a threshold check per dataset — floors chosen so
        # a collision (cross-dataset view read) would change the outcome.
        for ds in cfg.datasets:
            ds.validations = [
                _threshold_row_count_over(min_rows=1),
            ]

        # Direct engine invocation (bypass YAML re-serialization of validations).
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        eng = QualifireEngine(
            backend=backend, storage=qf._storage,
            context=QualifireContext(), config=cfg,
        )
        result = eng.run()

        assert result.overall_severity == Severity.PASS
        # Both datasets must have their own DatasetResult and both must pass.
        names = {ds.dataset_name for ds in result.datasets}
        assert names == {"orders.us", "orders-us"}, f"got {names!r}"


# ---------------------------------------------------------------------------
# DatasetConfig source contract — Codex adversarial review round 3.
# ``df`` is a public field on DatasetConfig and the engine has a real
# code path for it, but _require_source() used to reject df-only
# construction, leaving the contract half-wired (usable only through
# Qualifire.validate()'s wrapper). Round 3 makes df a first-class
# source mode.
# ---------------------------------------------------------------------------


class TestDatasetConfigDfSource:
    def test_df_only_is_accepted(self):
        """``DatasetConfig(name=..., df=...)`` must construct without error.

        The engine materializes df via _materialize_df and uses the
        dataset name as the logical identifier when no table is given.
        Rejecting this at the model layer was an integration trap.
        """
        from qualifire.core.config import DatasetConfig

        df = pd.DataFrame({"id": [1, 2, 3]})
        cfg = DatasetConfig(name="my_ds", df=df)
        assert cfg.name == "my_ds"
        assert cfg.df is df
        assert cfg.table is None

    def test_df_plus_table_is_accepted(self):
        """``table + df`` is the API layer's normal shape: df supplies
        the data, table supplies the logical identity for history +
        persistence (see engine.py:_run_dataset logical_table capture)."""
        from qualifire.core.config import DatasetConfig

        df = pd.DataFrame({"id": [1]})
        cfg = DatasetConfig(name="my_ds", table="prod.orders", df=df)
        assert cfg.table == "prod.orders"
        assert cfg.df is df

    def test_df_plus_query_is_rejected(self):
        """``df`` and ``query`` are mutually exclusive source modes."""
        from pydantic import ValidationError
        from qualifire.core.config import DatasetConfig

        with pytest.raises(ValidationError, match="mutually exclusive"):
            DatasetConfig(
                name="my_ds",
                df=pd.DataFrame({"id": [1]}),
                query="SELECT 1",
            )

    def test_no_source_still_rejected(self):
        """Nothing set at all still fails — must pick one source."""
        from pydantic import ValidationError
        from qualifire.core.config import DatasetConfig

        with pytest.raises(ValidationError, match="table.*df.*query.*wap"):
            DatasetConfig(name="my_ds")


# ---------------------------------------------------------------------------
# Run-id sanitization in temp-view names — Codex adversarial review round 4.
# _make_view_name previously appended self.context.run_id[:8] verbatim. Users
# who set a custom run_id (``QualifireContext(run_id="2026-04-20")``, or a
# dotted release tag) would get ``-`` or ``.`` baked into the identifier.
# Spark would parse the result as a qualified reference; PostgreSQL would
# reject it outright. The fix runs the run_id slice through the same
# non-alphanum sub as the dataset name.
# ---------------------------------------------------------------------------


class TestRunIdSanitizedInViewName:
    def _engine_with_run_id(self, run_id: str):
        from qualifire.core.config import QualifireConfig
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine

        backend = MagicMock()
        cfg = QualifireConfig(
            owner="o", bu="b", system_table="",
            datasets=[{"name": "stub", "table": "stub"}],  # type: ignore[list-item]
        )
        return QualifireEngine(
            backend=backend,
            storage=None,
            context=QualifireContext(run_id=run_id),
            config=cfg,
        )

    def test_hyphen_run_id_produces_safe_view_name(self):
        """A human-readable hyphenated run_id must not leak ``-`` into
        the view identifier."""
        eng = self._engine_with_run_id("2026-04-20")
        name = eng._make_view_name("orders")
        assert "-" not in name
        assert re.match(r"^[A-Za-z0-9_]+$", name), f"unsafe chars in {name!r}"

    def test_dotted_run_id_produces_safe_view_name(self):
        """A dotted release-tag run_id must not leak ``.`` into the view."""
        eng = self._engine_with_run_id("prod.daily.v2")
        name = eng._make_view_name("orders")
        assert "." not in name
        assert re.match(r"^[A-Za-z0-9_]+$", name), f"unsafe chars in {name!r}"

    def test_mixed_punctuation_run_id_is_sanitized(self):
        eng = self._engine_with_run_id("a-b.c+d/e")
        name = eng._make_view_name("orders")
        assert re.match(r"^[A-Za-z0-9_]+$", name), f"unsafe chars in {name!r}"

    def test_end_to_end_spark_with_hyphenated_run_id(self):
        """Full validate(df=...) with a hyphenated custom run_id must
        register and drop the temp view using a SQL-safe identifier —
        no unquoted hyphens that Spark would try to parse as subtraction.
        """
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )

        backend = _spark_like_backend({"row_count": 5000})
        df = MagicMock(name="user_df")

        cfg = QualifireConfig(
            owner="o", bu="b", system_table="",
            datasets=[
                DatasetConfig(
                    name="ds",
                    df=df,
                    validations=[
                        ThresholdValidationConfig(
                            collection=AggregationCollectionConfig(
                                expressions={"row_count": "COUNT(*)"},
                            ),
                            rules=[ThresholdRuleConfig(
                                metric="row_count",
                                thresholds=ThresholdLevels(),
                            )],
                        ),
                    ],
                )
            ],
        )
        engine = QualifireEngine(
            backend=backend,
            storage=None,
            context=QualifireContext(run_id="2026-04-20"),
            config=cfg,
        )
        engine.run()

        view = df.createOrReplaceTempView.call_args[0][0]
        assert "-" not in view
        assert re.match(r"^[A-Za-z0-9_]+$", view), f"unsafe chars in {view!r}"
        # drop_temp_view at teardown names the sanitized identifier.
        drop_views = [c.args[0] for c in backend.drop_temp_view.call_args_list]
        assert view in drop_views


# ---------------------------------------------------------------------------
# df-only programmatic API — Codex adversarial review round 4.
# ``Qualifire.validate()`` used to require ``table: str`` even for df-only
# callers. The ``table`` value became the persistence/history key, forcing
# callers to invent a fake table name that then shaped drift lookups and
# report grouping. Round 4 makes ``table`` optional when ``df`` is given,
# and falls back to ``name`` as the logical identifier.
# ---------------------------------------------------------------------------


class TestValidateDfOnly:
    def test_table_is_optional_when_df_is_provided(self, tmp_path):
        """validate(df=..., name=..., validations=...) must work with no table."""
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
        )

        df = pd.DataFrame({"id": list(range(10))})
        result = qf.validate(
            df=df,
            name="in_memory_ds",
            validations=[_threshold_row_count_over(min_rows=5)],
        )
        assert result.overall_severity == Severity.PASS

    def test_persistence_uses_dataset_name_when_no_table(self, tmp_path):
        """With no table, the dataset name must flow through to the
        system-table ``table_name`` column. Otherwise df-only callers
        silently key history on ``None`` / empty string and drift
        lookups cold-start forever."""
        db = tmp_path / "qf.db"
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(db),
            system_table_backend="sqlite",
        )

        df = pd.DataFrame({"id": list(range(10))})
        qf.validate(
            df=df,
            name="ephemeral_ds",
            validations=[_threshold_row_count_over(min_rows=5)],
        )

        import sqlite3
        conn = sqlite3.connect(str(db))
        rows = list(conn.execute(
            "SELECT DISTINCT table_name FROM qualifire_history"
        ))
        conn.close()
        table_names = {r[0] for r in rows}
        assert table_names == {"ephemeral_ds"}, (
            f"Expected persistence under dataset name 'ephemeral_ds', got: {table_names}"
        )
        assert not any(n and n.startswith("_qf_") for n in table_names)

    def test_drift_history_lookup_keys_on_dataset_name_for_df_only(self, tmp_path):
        """Drift check with df-only + no table must find seeded history
        keyed on the dataset name. Regression for mis-keyed lookup."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        db = tmp_path / "qf.db"
        storage = SQLiteStorage(db_path=str(db))
        storage.initialize()
        storage.write_results([
            {
                "run_id": f"run_{i}",
                "run_timestamp": f"2026-01-0{i+1}T00:00:00",
                "owner": "o",
                "bu": "b",
                "dataset_name": "ephemeral_ds",
                "table_name": "ephemeral_ds",
                "metric_name": "row_count",
                "metric_value": 100.0,
                "collection_type": "aggregation",
                "record_type": "collection",
                "is_active": "true",
                "collector_name": "drift.ephemeral_ds",
                "partition_ts": f"2026-01-0{i+1}T00:00:00",
            }
            for i in range(3)
        ])

        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(db),
            system_table_backend="sqlite",
        )

        drift = Qualifire.drift_check(
            aggregations={"row_count": "COUNT(*)"},
            rules=[{
                "metric": "row_count",
                "compare": {"to": "mean", "past_values": 3, "step": "P1D"},
                "thresholds": {"warning": {"deviation_pct": {"min": -50, "max": 50}}},
            }],
        )

        df = pd.DataFrame({"id": list(range(102))})
        result = qf.validate(
            df=df, name="ephemeral_ds", validations=[drift],
            partition_ts="'2026-01-04'",
            partition_step="P1D",
        )

        drift_vrs = [
            vr for ds in result.datasets
            for vr in ds.validation_results
            if vr.validation_type == "drift"
        ]
        assert drift_vrs
        # Same positive guard as
        # test_drift_check_finds_prior_history_under_logical_table:
        # cold_start=True is the *only* deterministic signal that the
        # lookup returned zero rows. Messaging differs by code path
        # (missing-history vs cold-start), so grep-on-message misses
        # regressions.
        cold_starts = [vr for vr in drift_vrs if vr.details.get("cold_start")]
        assert not cold_starts, (
            f"Drift check did not find seeded history — df-only lookup key mis-routed "
            f"and history query returned zero rows. "
            f"Got: {[(vr.severity.value, vr.message, vr.details) for vr in drift_vrs]}"
        )

    def test_no_table_and_no_df_raises(self):
        """Calling validate() without table or df is a programming error;
        surface it with a clear message rather than letting Pydantic emit
        a generic source-required error from deep in model validation."""
        backend = PandasBackend(tables={})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        with pytest.raises(ValueError, match="table.*df"):
            qf.validate(
                validations=[_threshold_row_count_over(min_rows=5)],
            )


# ---------------------------------------------------------------------------
# Round-5 identity-safety regression: df-only validations must not share a
# logical-identity key when the caller omits ``name``.
#
# Previously the code fell back to ``dataset_name = name or table or
# "dataframe"``. That looks innocuous until you consider two unrelated
# df-only callers: both land under ``table_name = "dataframe"`` in the
# system table, so caller B's drift check reads caller A's history, and
# caller A's alert suppression can silence caller B's errors. This class
# forces the caller to own the identity choice.
# ---------------------------------------------------------------------------


class TestValidateDfRequiresNameWhenNoTable:
    def test_df_without_name_and_without_table_raises(self):
        """Most common trap: forgot both. Must not silently proceed."""
        backend = PandasBackend(tables={})
        qf = Qualifire(backend=backend, owner="o", bu="b")

        df = pd.DataFrame({"id": list(range(10))})
        with pytest.raises(ValueError, match="name"):
            qf.validate(
                df=df,
                validations=[_threshold_row_count_over(min_rows=5)],
            )

    def test_df_with_only_name_is_accepted(self):
        """``name`` alone is a valid identity — no table needed."""
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
        )
        df = pd.DataFrame({"id": list(range(10))})
        # Must not raise.
        result = qf.validate(
            df=df,
            name="explicit_identity",
            validations=[_threshold_row_count_over(min_rows=5)],
        )
        assert result.overall_severity == Severity.PASS

    def test_unrelated_unnamed_df_calls_cannot_share_history_key(self, tmp_path):
        """End-to-end identity-bleed regression.

        Two unrelated df-only validations without ``name`` would
        previously both persist under ``table_name = "dataframe"``. The
        second call's drift check would then see the first call's
        history rows — a silent, very-hard-to-diagnose correctness bug.
        With the round-5 fix, each call must raise before any rows are
        written. Assert that no persistence happened after the rejected
        attempt.
        """
        db = tmp_path / "qf.db"
        backend = PandasBackend(tables={})
        qf = Qualifire(
            backend=backend,
            owner="o",
            bu="b",
            system_table=str(db),
            system_table_backend="sqlite",
        )

        df_a = pd.DataFrame({"id": list(range(10))})
        df_b = pd.DataFrame({"id": list(range(100))})

        # Both attempts must raise — no fallback key allowed.
        with pytest.raises(ValueError, match="name"):
            qf.validate(df=df_a, validations=[_threshold_row_count_over(min_rows=5)])
        with pytest.raises(ValueError, match="name"):
            qf.validate(df=df_b, validations=[_threshold_row_count_over(min_rows=5)])

        # Belt-and-braces: confirm nothing leaked into persistence under
        # a shared default identity. If a future refactor silently
        # reintroduces the "dataframe" default, this assertion flips and
        # fails fast.
        import sqlite3
        conn = sqlite3.connect(str(db))
        try:
            rows = list(conn.execute(
                "SELECT DISTINCT table_name FROM qualifire_history"
            ))
        except sqlite3.OperationalError:
            # Table not initialized because no validation ran. Good.
            rows = []
        conn.close()
        names = {r[0] for r in rows}
        assert "dataframe" not in names, (
            f"Unrelated df-only calls leaked identity key; got {names!r}"
        )


# ---------------------------------------------------------------------------
# Round-8 restores the round-5 hard-fail on duplicate dataset names after
# Codex flagged the round-6 deprecation-only approach as an under-fix.
# The dataset name is the persistence key, the drift/forecast history
# lookup key, the health-report grouping key, and the notification
# deduplication key — duplicates silently mix rows across unrelated
# datasets, not just race on a temp view. A deprecation warning cannot
# prevent silent data corruption; the hard fail forces the rename that
# operators need to do anyway.
#
# The engine's per-instance temp-view disambiguation
# (``_make_view_name`` + ``_instance_key_for``) is kept as
# belt-and-braces protection for any programmatic caller that bypasses
# the config validator (e.g. ``QualifireConfig.model_construct``).
# ---------------------------------------------------------------------------


class TestDuplicateDatasetNamesRejected:
    def test_duplicate_dataset_names_raise_at_config_load(self):
        from pydantic import ValidationError
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )

        def _ds(name: str, table: str) -> DatasetConfig:
            return DatasetConfig(
                name=name,
                table=table,
                validations=[
                    ThresholdValidationConfig(
                        collection=AggregationCollectionConfig(
                            expressions={"c": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="c", thresholds=ThresholdLevels()
                        )],
                    ),
                ],
            )

        with pytest.raises(ValidationError, match="[Dd]uplicate dataset name"):
            QualifireConfig(
                owner="o", bu="b", system_table="s",
                datasets=[_ds("orders", "t1"), _ds("orders", "t2")],
            )

    def test_duplicate_error_enumerates_all_duplicates(self):
        """The message must name every offending duplicate so the
        operator can see exactly which entries to rename — not just the
        first collision."""
        from pydantic import ValidationError
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )

        def _ds(name: str, table: str) -> DatasetConfig:
            return DatasetConfig(
                name=name, table=table,
                validations=[ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"c": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="c", thresholds=ThresholdLevels()
                    )],
                )],
            )

        with pytest.raises(ValidationError) as excinfo:
            QualifireConfig(
                owner="o", bu="b", system_table="s",
                datasets=[
                    _ds("a", "t1"), _ds("a", "t2"),
                    _ds("b", "t3"), _ds("b", "t4"),
                ],
            )
        msg = str(excinfo.value)
        assert "'a'" in msg and "'b'" in msg, (
            f"Error must enumerate all duplicate names, got: {msg!r}"
        )

    def test_engine_disambiguates_value_equal_configs_bypassing_validator(self):
        """Belt-and-braces: the config validator rejects duplicates, but
        a programmatic caller building a ``QualifireConfig`` via
        ``model_construct`` (or otherwise bypassing validation) could
        still hand the engine duplicates. The engine's
        ``_instance_key_for`` must use **identity** (``is``), not
        equality (``list.index``), because Pydantic ``BaseModel``
        compares by value — two identical-content ``DatasetConfig``
        objects resolve to the same index under ``list.index()`` and
        their temp views collide under ``dataset_parallelism > 1``.
        This is the round-7 regression preserved after round-8 restored
        the config-level hard fail.
        """
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine

        def _ds() -> DatasetConfig:
            return DatasetConfig(
                name="dup", table="t",
                validations=[ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"c": "COUNT(*)"}
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="c", thresholds=ThresholdLevels()
                    )],
                )],
            )

        # Bypass the config validator — mirrors a programmatic caller
        # that constructs the model without running validators.
        cfg = QualifireConfig.model_construct(
            owner="o", bu="b", system_table="",
            datasets=[_ds(), _ds()],
            dataset_parallelism=1,
            notifications={}, context={},
        )
        assert cfg.datasets[0] == cfg.datasets[1], (
            "Pre-check: value-equality must hold for this test to "
            "exercise the regression"
        )
        assert cfg.datasets[0] is not cfg.datasets[1]

        eng = QualifireEngine(
            backend=MagicMock(), storage=None,
            context=QualifireContext(run_id="abc12345"), config=cfg,
        )
        k0 = eng._instance_key_for(cfg.datasets[0])
        k1 = eng._instance_key_for(cfg.datasets[1])
        assert k0 != k1, (
            f"Value-equal DatasetConfigs collapsed to same instance_key "
            f"{k0!r}; _instance_key_for must use identity (``is``), not "
            f"equality, when scanning config.datasets."
        )
        n0 = eng._make_view_name(cfg.datasets[0].name, k0)
        n1 = eng._make_view_name(cfg.datasets[1].name, k1)
        assert n0 != n1

    def test_distinct_names_still_accepted(self):
        """Negative control: unique-name validator must not over-trigger.

        The round-2 collision regression uses names like ``orders.us`` /
        ``orders-us`` which share a sanitized prefix but are *distinct*
        logical identities — those still need to load.
        """
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )

        def _ds(name: str, table: str) -> DatasetConfig:
            return DatasetConfig(
                name=name, table=table,
                validations=[
                    ThresholdValidationConfig(
                        collection=AggregationCollectionConfig(
                            expressions={"c": "COUNT(*)"}
                        ),
                        rules=[ThresholdRuleConfig(
                            metric="c", thresholds=ThresholdLevels()
                        )],
                    ),
                ],
            )

        cfg = QualifireConfig(
            owner="o", bu="b", system_table="s",
            datasets=[_ds("orders.us", "t1"), _ds("orders-us", "t2")],
        )
        assert len(cfg.datasets) == 2
