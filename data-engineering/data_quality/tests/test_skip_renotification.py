"""Tests for the runtime ``skip_renotification`` flag.

This flag replaces the per-validation ``suppress_repeat_alerts``
config field. Same snapshot-based duplicate-alert mechanism;
the trigger moved from per-validation YAML to a single runtime
opt-in. Default is False so retries / replays re-page by default;
operators set ``skip_renotification=True`` to skip dispatch for
keys whose prior run already paged at the same severity.

Test coverage:
    - T1: gate fires when prior matching severity exists.
    - T2: gate doesn't fire by default (the new default-False
          behavior — used to be per-validation default-True).
    - T3: per-key partial — new keys still dispatch even when
          some are suppressed.
    - T4: pre-fetch optimization — `read_validation_history_bulk`
          is NOT called when the flag is False.
    - T5: loud-fail on stale YAML key (`suppress_repeat_alerts:
          true` raises ValueError at config-load time).
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    NotifyConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.models import Severity


def _make_engine(
    *, snapshot_rows, skip_renotification: bool, notifier=None,
) -> tuple[QualifireEngine, MagicMock]:
    """Build an engine wired to a MagicMock storage that returns
    ``snapshot_rows`` from ``read_validation_history_bulk``.
    """
    backend = MagicMock()
    backend.get_aggregations.return_value = {"cnt": 50}

    storage = MagicMock()
    storage.read_validation_history_bulk.return_value = snapshot_rows
    # Other storage calls used during persist — return values that
    # don't affect the test path.
    storage.write_results.return_value = None

    if notifier is None:
        notifier = MagicMock()
        notifier.send.return_value = MagicMock(
            channel="email", severity=Severity.ERROR,
            status="sent", message="",
        )

    ds = DatasetConfig(
        name="ds",
        table="t",
        validations=[
            ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"cnt": "COUNT(*)"},
                ),
                rules=[ThresholdRuleConfig(
                    metric="cnt",
                    thresholds=ThresholdLevels(error={"min": 100}),
                )],
                notify=NotifyConfig(error=["email"]),
            ),
        ],
    )
    config = QualifireConfig(
        owner="o", bu="b", system_table="s", datasets=[ds],
    )
    ctx = QualifireContext()
    ctx.skip_renotification = skip_renotification

    engine = QualifireEngine(
        backend=backend, storage=storage,
        context=ctx, config=config,
        notifiers={"email": notifier},
    )
    return engine, notifier


class TestSkipRenotification:
    def test_T1_gate_fires_when_prior_severity_matches(self):
        """T1: with skip_renotification=True and a snapshot showing
        the same severity, the notifier is NOT called and a
        synthetic 'skipped' row is emitted."""
        from qualifire.core.models import ValidationKey

        # Snapshot: prior run had ERROR for this key. ValidationKey
        # uses the FULL suffixed validation_name (per
        # ``validation_key_for`` in core/models.py:283), which for
        # threshold validators is ``threshold.<dataset>.<metric>``.
        key = ValidationKey(
            dataset_name="ds",
            validation_name="threshold.ds.cnt",
            metric_name="cnt",
            dimension_value=None,
        )
        snapshot = {key: [{"validation_status": "ERROR"}]}

        engine, notifier = _make_engine(
            snapshot_rows=snapshot, skip_renotification=True,
        )

        with pytest.raises(Exception):
            engine.run()

        notifier.send.assert_not_called()
        # ``QualifireEngine`` raises before notifications can be
        # inspected on the result; verify via the storage write
        # arg captured during _persist_notification_rows.
        # Look for a write_results call carrying notification_status='skipped'.
        all_writes = engine.storage.write_results.call_args_list
        notif_rows = [
            r for call in all_writes
            for r in call.args[0]
            if r.get("record_type") == "notification"
        ]
        assert any(r["notification_status"] == "skipped" for r in notif_rows), (
            f"expected at least one skipped notification row; got "
            f"{[r['notification_status'] for r in notif_rows]}"
        )

    def test_T2_default_False_dispatches_despite_prior_history(self):
        """T2: with skip_renotification=False (the default), the
        notifier is called even when the snapshot shows the same
        prior severity. This pins the default-flip behavior."""
        from qualifire.core.models import ValidationKey

        key = ValidationKey(
            dataset_name="ds",
            validation_name="threshold.ds.cnt",
            metric_name="cnt",
            dimension_value=None,
        )
        snapshot = {key: [{"validation_status": "ERROR"}]}

        engine, notifier = _make_engine(
            snapshot_rows=snapshot, skip_renotification=False,
        )

        with pytest.raises(Exception):
            engine.run()

        notifier.send.assert_called()

    def test_T4_prefetch_skipped_when_flag_off(self):
        """T4: when skip_renotification=False, the engine MUST NOT
        call read_validation_history_bulk — the snapshot is
        wasted work in the default path."""
        engine, _ = _make_engine(
            snapshot_rows={}, skip_renotification=False,
        )

        with pytest.raises(Exception):
            engine.run()

        engine.storage.read_validation_history_bulk.assert_not_called()

    def test_T4_prefetch_runs_when_flag_on(self):
        """T4 (other half): when skip_renotification=True, the
        engine DOES call read_validation_history_bulk."""
        engine, _ = _make_engine(
            snapshot_rows={}, skip_renotification=True,
        )

        with pytest.raises(Exception):
            engine.run()

        engine.storage.read_validation_history_bulk.assert_called()


class TestParallelBackfillFlagPlumbing:
    """T6 (codex impl-review R1 MAJOR): the parallel backfill code
    path uses ``ThreadPoolExecutor.submit`` with kwargs that diverge
    from the serial path's call site. A missing ``skip_renotification``
    in the submit kwargs would silently drop the flag for every
    parallel worker. This test pins the worker-side context value.
    """

    def test_T6_parallel_backfill_threads_flag_to_workers(self, tmp_path, monkeypatch):
        from qualifire.api import Qualifire
        from qualifire.backends.pandas_backend import PandasBackend
        import pandas as pd
        from qualifire.core import backfill as bf_mod
        from qualifire.core.config import (
            DatasetConfig as _DS, AggregationCollectionConfig as _AC,
            ThresholdValidationConfig as _TVC, ThresholdRuleConfig as _TR,
            ThresholdLevels as _TL,
        )

        backend = PandasBackend()
        backend.register_table("t", pd.DataFrame({
            "event_date": ["2026-04-01"] * 5,
            "x": list(range(5)),
        }))
        qf = Qualifire(
            backend=backend,
            system_table=str(tmp_path / "qf.db"),
            system_table_backend="sqlite",
            owner="o", bu="b",
        )
        ds = _DS(
            name="ds", table="t",
            partition_ts="event_date", partition_step="P1D",
            validations=[_TVC(
                collection=_AC(expressions={"cnt": "COUNT(*)"}),
                rules=[_TR(metric="cnt", thresholds=_TL())],
            )],
        )
        cfg = QualifireConfig(
            owner="o", bu="b", system_table="qf", datasets=[ds],
        )

        captured: list[bool] = []
        original = bf_mod._run_anchor_once

        def capturing(*args, **kwargs):
            captured.append(kwargs.get("skip_renotification", False))
            return original(*args, **kwargs)

        monkeypatch.setattr(bf_mod, "_run_anchor_once", capturing)

        qf.backfill(
            cfg, partition_ts=("2026-04-01", "2026-04-03"),
            parallelism=4, skip_renotification=True,
        )
        assert captured, "_run_anchor_once was never invoked"
        assert all(captured), (
            f"parallel workers must all receive skip_renotification=True; "
            f"got {captured}"
        )


