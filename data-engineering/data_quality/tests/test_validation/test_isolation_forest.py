"""Tests for Isolation Forest + SHAP anomaly detection validator."""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.isolation_forest import IsolationForestValidator


def _make_sample_collected(
    current_df: pd.DataFrame,
    past_dfs: list[tuple[str, pd.DataFrame]],
) -> list[CollectionResult]:
    """Build a CollectionResult that mimics SamplerCollector output."""
    return [
        CollectionResult(
            metric_name="sample",
            metric_value=None,
            collected_at=datetime.now(),
            metadata={
                "table": "test_table",
                "collection_type": "sample",
                "n_records": len(current_df),
                "current_df": current_df,
                "past_dfs": past_dfs,
            },
        )
    ]


def _normal_data(n: int, seed: int = 42) -> pd.DataFrame:
    """Generate normal numeric data."""
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        "amount": rng.normal(100, 10, n),
        "count": rng.randint(1, 50, n),
        "category": rng.choice(["A", "B", "C"], n),
    })


def _anomalous_data(n: int, seed: int = 99) -> pd.DataFrame:
    """Generate data with very different distributions."""
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        "amount": rng.normal(500, 100, n),  # shifted mean
        "count": rng.randint(100, 500, n),  # shifted range
        "category": rng.choice(["A", "B", "C"], n),
    })


# ===========================================================================
# Basic functionality
# ===========================================================================


class TestIsolationForestValidator:
    def test_pass_normal_data(self):
        """Normal current data against normal historical -> low anomaly ratio."""
        current = _normal_data(200, seed=1)
        past = [
            ("past_1", _normal_data(200, seed=2)),
            ("past_2", _normal_data(200, seed=3)),
        ]
        validator = IsolationForestValidator(
            warning_threshold=0.5,
            error_threshold=0.8,
            explain=False,
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert len(results) == 1
        assert results[0].validation_type == "shape"
        assert results[0].actual_value < 0.5  # low anomaly ratio
        assert results[0].severity == Severity.PASS
        assert results[0].details["total_current"] == 200

    def test_error_anomalous_data(self):
        """Highly anomalous current data -> high anomaly ratio -> ERROR."""
        current = _anomalous_data(200, seed=1)
        past = [
            ("past_1", _normal_data(200, seed=2)),
            ("past_2", _normal_data(200, seed=3)),
            ("past_3", _normal_data(200, seed=4)),
        ]
        validator = IsolationForestValidator(
            warning_threshold=0.3,
            error_threshold=0.5,
            explain=False,
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert len(results) == 1
        # Anomaly ratio should be high because current is very different
        assert results[0].actual_value > 0.3
        assert results[0].severity in (Severity.WARNING, Severity.ERROR)

    def test_empty_current_returns_warning_by_default(self):
        """Empty current DataFrame -> WARNING (on_empty_data default)."""
        current = pd.DataFrame(columns=["a", "b"])
        past = [("past_1", _normal_data(50))]
        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.WARNING
        assert "Insufficient data" in results[0].message

    def test_no_past_data_passes_by_default(self):
        """No historical data -> PASS (on_missing_history='ignore' default)."""
        current = _normal_data(100)
        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, []))

        assert results[0].severity == Severity.PASS
        assert "No historical data" in results[0].message
        assert results[0].details.get("cold_start") is True

    def test_no_past_data_on_missing_history_warn(self):
        """on_missing_history='warn' returns WARNING when no past data."""
        current = _normal_data(100)
        validator = IsolationForestValidator(explain=False, on_missing_history="warn")
        results = validator.validate(_make_sample_collected(current, []))

        assert results[0].severity == Severity.WARNING
        assert results[0].details.get("cold_start") is True

    def test_no_past_data_on_missing_history_error(self):
        """on_missing_history='error' returns ERROR when no past data."""
        current = _normal_data(100)
        validator = IsolationForestValidator(explain=False, on_missing_history="error")
        results = validator.validate(_make_sample_collected(current, []))

        assert results[0].severity == Severity.ERROR
        assert results[0].details.get("cold_start") is True

    def test_empty_current_respects_on_empty_data_pass(self):
        """With on_empty_data=PASS, empty current DataFrame -> PASS."""
        current = pd.DataFrame(columns=["a", "b"])
        past = [("past_1", _normal_data(50))]
        validator = IsolationForestValidator(explain=False, on_empty_data=Severity.PASS)
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.PASS

    def test_empty_current_respects_on_empty_data_error(self):
        """With on_empty_data=ERROR, empty current DataFrame -> ERROR."""
        current = pd.DataFrame(columns=["a", "b"])
        past = [("past_1", _normal_data(50))]
        validator = IsolationForestValidator(explain=False, on_empty_data=Severity.ERROR)
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.ERROR

    def test_skips_non_sample_metrics(self):
        """Non-sample CollectionResults are ignored."""
        collected = [
            CollectionResult(
                metric_name="not_a_sample",
                metric_value=42,
                collected_at=datetime.now(),
            )
        ]
        validator = IsolationForestValidator(explain=False)
        results = validator.validate(collected)
        assert results == []


# ===========================================================================
# Schema change detection
# ===========================================================================


class TestSchemaChangeDetection:
    def test_uses_column_intersection(self):
        """When schemas differ, only common columns are used."""
        current = pd.DataFrame({"a": [1, 2], "b": [3, 4], "new_col": [5, 6]})
        past = [("past_1", pd.DataFrame({"a": [10, 20], "b": [30, 40], "old_col": [50, 60]}))]

        validator = IsolationForestValidator(
            explain=False, alert_on_schema_change=False
        )
        results = validator.validate(_make_sample_collected(current, past))

        # Should run without error using intersection {a, b}
        assert len(results) == 1
        assert results[0].validation_type == "shape"

    def test_alert_on_schema_change(self):
        """With alert_on_schema_change=True, emit a WARNING for schema differences."""
        current = pd.DataFrame({"a": [1, 2], "new_col": [3, 4]})
        past = [("past_1", pd.DataFrame({"a": [10, 20], "old_col": [30, 40]}))]

        validator = IsolationForestValidator(
            explain=False, alert_on_schema_change=True
        )
        results = validator.validate(_make_sample_collected(current, past))

        # Should have schema change warning + anomaly result
        schema_results = [r for r in results if "schema" in r.validation_name]
        assert len(schema_results) == 1
        assert schema_results[0].severity == Severity.WARNING
        assert "new_col" in schema_results[0].message or "old_col" in schema_results[0].message


# ===========================================================================
# Column encoding
# ===========================================================================


class TestColumnEncoding:
    def test_numeric_columns(self):
        """Numeric columns are passed through."""
        current = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [4, 5, 6]})
        past = [("past_1", pd.DataFrame({"x": [1.5, 2.5, 3.5], "y": [4, 5, 6]}))]

        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))
        assert len(results) == 1

    def test_boolean_columns(self):
        """Boolean columns are encoded as 0/1."""
        current = pd.DataFrame({"flag": [True, False, True], "val": [1, 2, 3]})
        past = [("past_1", pd.DataFrame({"flag": [False, True, False], "val": [4, 5, 6]}))]

        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))
        assert len(results) >= 1

    def test_low_cardinality_categorical_one_hot(self):
        """Categorical columns with <=20 unique values get one-hot encoded."""
        current = pd.DataFrame({
            "color": ["red", "blue", "red", "green"],
            "val": [1, 2, 3, 4],
        })
        past = [("past_1", pd.DataFrame({
            "color": ["red", "blue", "green", "blue"],
            "val": [5, 6, 7, 8],
        }))]

        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))
        assert len(results) >= 1

    def test_high_cardinality_categorical_label_encoded(self):
        """Categorical columns with >20 unique values get label encoded."""
        n = 100
        rng = np.random.RandomState(42)
        # 25 unique categories > threshold of 20
        categories = [f"cat_{i}" for i in range(25)]
        current = pd.DataFrame({
            "id": rng.choice(categories, n),
            "val": rng.normal(0, 1, n),
        })
        past = [("past_1", pd.DataFrame({
            "id": rng.choice(categories, n),
            "val": rng.normal(0, 1, n),
        }))]

        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))
        assert len(results) >= 1

    def test_no_encodable_columns_returns_warning_by_default(self):
        """If no columns can be encoded, return WARNING (on_empty_data default)."""
        current = pd.DataFrame({"data": [{"a": 1}, {"a": 2}]})
        past = [("past_1", pd.DataFrame({"data": [{"a": 3}, {"a": 4}]}))]

        validator = IsolationForestValidator(explain=False, drop_complex=True)
        results = validator.validate(_make_sample_collected(current, past))

        empty_results = [r for r in results if "No encodable" in r.message]
        assert len(empty_results) == 1
        assert empty_results[0].severity == Severity.WARNING

    def test_numeric_with_nulls(self):
        """Nulls in numeric columns are imputed with median."""
        current = pd.DataFrame({"x": [1.0, np.nan, 3.0], "y": [4, 5, 6]})
        past = [("past_1", pd.DataFrame({"x": [2.0, 2.5, np.nan], "y": [7, 8, 9]}))]

        validator = IsolationForestValidator(explain=False)
        # Should not raise — nulls are handled
        results = validator.validate(_make_sample_collected(current, past))
        assert len(results) >= 1


# ===========================================================================
# SHAP explainability
# ===========================================================================


class TestSHAPExplainability:
    def test_shap_top_features_with_mock(self):
        """When anomalies are detected with explain=True and SHAP is available,
        top_contributing_features appear in details."""
        from unittest.mock import patch, MagicMock

        rng = np.random.RandomState(42)
        current = pd.DataFrame({
            "amount": rng.normal(500, 50, 100),
            "count": rng.normal(200, 20, 100),
        })
        past = [
            ("past_1", pd.DataFrame({
                "amount": rng.normal(100, 10, 200),
                "count": rng.normal(50, 5, 200),
            })),
        ]

        # Mock SHAP so the test works without the shap package
        mock_shap = MagicMock()
        mock_explainer = MagicMock()
        # Return fake SHAP values matching the encoded shape
        mock_explainer.shap_values.side_effect = lambda x: np.random.rand(*x.shape)
        mock_shap.TreeExplainer.return_value = mock_explainer

        with patch("qualifire.validation.isolation_forest._import_shap", return_value=mock_shap):
            validator = IsolationForestValidator(
                warning_threshold=0.1,
                error_threshold=0.9,
                explain=True,
            )
            results = validator.validate(_make_sample_collected(current, past))

        vr = results[0]
        if vr.details.get("anomaly_count", 0) > 0:
            assert "top_contributing_features" in vr.details
            features = vr.details["top_contributing_features"]
            assert len(features) > 0
            assert "feature" in features[0]
            assert "importance" in features[0]

    def test_value_drift_explainer_present_on_alert_when_enabled(self):
        """When SHAP populates top features, drift explainer block is parallel."""
        from unittest.mock import patch, MagicMock

        rng = np.random.RandomState(42)
        current = pd.DataFrame({
            "amount": rng.normal(500, 50, 100),
            "count": rng.normal(200, 20, 100),
        })
        past = [
            ("past_1", pd.DataFrame({
                "amount": rng.normal(100, 10, 200),
                "count": rng.normal(50, 5, 200),
            })),
        ]
        mock_shap = MagicMock()
        mock_explainer = MagicMock()
        mock_explainer.shap_values.side_effect = lambda x: np.random.rand(*x.shape)
        mock_shap.TreeExplainer.return_value = mock_explainer
        with patch("qualifire.validation.isolation_forest._import_shap", return_value=mock_shap):
            validator = IsolationForestValidator(
                warning_threshold=0.1,
                error_threshold=0.9,
                explain=True,
                explain_value_drift=True,
            )
            results = validator.validate(_make_sample_collected(current, past))
        vr = results[0]
        if vr.details.get("anomaly_count", 0) > 0:
            tf = vr.details.get("top_contributing_features") or []
            drift = vr.details.get("value_drift_explainer")
            assert drift is not None
            assert len(drift) == len(tf)
            for entry in drift:
                assert "feature" in entry
                assert "kind" in entry
                assert "summary" in entry and entry["summary"]

    def test_value_drift_explainer_skipped_when_disabled(self):
        """explain_value_drift=False -> top features still present, drift block absent."""
        from unittest.mock import patch, MagicMock

        rng = np.random.RandomState(42)
        current = pd.DataFrame({"amount": rng.normal(500, 50, 100)})
        past = [("past_1", pd.DataFrame({"amount": rng.normal(100, 10, 200)}))]
        mock_shap = MagicMock()
        mock_explainer = MagicMock()
        mock_explainer.shap_values.side_effect = lambda x: np.random.rand(*x.shape)
        mock_shap.TreeExplainer.return_value = mock_explainer
        with patch("qualifire.validation.isolation_forest._import_shap", return_value=mock_shap):
            validator = IsolationForestValidator(
                warning_threshold=0.1,
                error_threshold=0.9,
                explain=True,
                explain_value_drift=False,
            )
            results = validator.validate(_make_sample_collected(current, past))
        vr = results[0]
        if vr.details.get("anomaly_count", 0) > 0:
            assert "top_contributing_features" in vr.details
            assert "value_drift_explainer" not in vr.details

    def test_shap_graceful_fallback_when_unavailable(self):
        """When SHAP is not installed, explain=True still works — just no features."""
        current = _anomalous_data(100)
        past = [("past_1", _normal_data(200))]

        validator = IsolationForestValidator(
            warning_threshold=0.1,
            error_threshold=0.9,
            explain=True,
        )
        results = validator.validate(_make_sample_collected(current, past))

        # Should not crash; SHAP silently skipped
        assert results[0].validation_type == "shape"

    def test_explain_false_no_shap(self):
        """With explain=False, no SHAP details even if anomalies exist."""
        current = _anomalous_data(100)
        past = [("past_1", _normal_data(200))]

        validator = IsolationForestValidator(
            warning_threshold=0.1,
            error_threshold=0.9,
            explain=False,
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert "top_contributing_features" not in results[0].details


# ===========================================================================
# Threshold behavior
# ===========================================================================


class TestAnomalyThresholds:
    def test_custom_thresholds(self):
        """Verify custom warning/error thresholds are used."""
        validator = IsolationForestValidator(
            warning_threshold=0.1,
            error_threshold=0.2,
            explain=False,
        )
        assert validator.warning_threshold == 0.1
        assert validator.error_threshold == 0.2

    def test_result_contains_threshold_info(self):
        """Result expected_value contains the configured thresholds."""
        current = _normal_data(50)
        past = [("past_1", _normal_data(100))]

        validator = IsolationForestValidator(
            warning_threshold=0.55,
            error_threshold=0.85,
            explain=False,
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].expected_value["warning_threshold"] == 0.55
        assert results[0].expected_value["error_threshold"] == 0.85

    def test_result_contains_anomaly_details(self):
        """Result details include anomaly_count, total_current, anomaly_ratio."""
        current = _normal_data(80)
        past = [("past_1", _normal_data(80))]

        validator = IsolationForestValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        d = results[0].details
        assert "anomaly_count" in d
        assert "total_current" in d
        assert d["total_current"] == 80
        assert "anomaly_ratio" in d
        assert 0 <= d["anomaly_ratio"] <= 1.0


# ===========================================================================
# exclude_columns — leakage suppression
# ===========================================================================


class TestExcludeColumns:
    """Mirrors the partition-column leakage suppression pattern from
    pattern_check. The IF tree splits on the partition column trivially
    when it's included (today's rows have the same value, all past rows
    have different values → 100% current-period rows flagged). New
    behaviour: the engine auto-prepends ``slice_column`` to the operator's
    ``exclude_columns`` list; this test pins the validator-side
    contract end-to-end."""

    def test_excluded_column_not_in_top_features(self):
        import pandas as pd
        # Build current+past samples that share every column EXCEPT a
        # ``sale_date`` partition marker that would otherwise leak.
        rows = lambda day, n, seed: pd.DataFrame({
            "amount": [50.0 + (i % 7) for i in range(n)],
            "region": (["us", "uk", "jp"] * n)[:n],
            "sale_date": [day] * n,
        })
        current = rows("2026-05-08", 80, seed=1)
        past = [
            ("past_1", rows("2026-05-07", 80, seed=2)),
            ("past_2", rows("2026-05-06", 80, seed=3)),
        ]
        validator = IsolationForestValidator(
            warning_threshold=0.5,
            error_threshold=0.8,
            explain=True,
            exclude_columns=["sale_date"],
        )
        results = validator.validate(_make_sample_collected(current, past))
        feats = results[0].details.get("top_contributing_features") or []
        feat_names = [f["feature"] for f in feats]
        # ``sale_date`` should never appear (excluded before encoding,
        # so no ``sale_date_<value>`` one-hot dummy can survive).
        assert not any(name.startswith("sale_date") for name in feat_names), (
            f"sale_date leaked into top features: {feat_names}"
        )

    def test_exclude_columns_dropped_before_encoding(self):
        """A column listed in exclude_columns must not produce any
        feature in ``top_contributing_features`` even when it has
        very high cardinality (per-row IDs typically do)."""
        import pandas as pd
        rows = lambda n, seed=1: pd.DataFrame({
            "sale_id": [f"id_{i}_{seed}" for i in range(n)],
            "amount": [50.0 + (i % 7) for i in range(n)],
        })
        current = rows(60, seed=1)
        past = [("past_1", rows(60, seed=2))]
        validator = IsolationForestValidator(
            warning_threshold=0.5,
            error_threshold=0.8,
            explain=True,
            exclude_columns=["sale_id"],
        )
        results = validator.validate(_make_sample_collected(current, past))
        feats = results[0].details.get("top_contributing_features") or []
        feat_names = [f["feature"] for f in feats]
        assert "sale_id" not in feat_names


def test_engine_auto_prepends_slice_column_to_excludes():
    """The engine helper ``_with_auto_slice_column_excluded`` adds the
    sample collection's ``slice_column`` to whatever the operator
    passed in. Closes the partition-column leakage footgun without
    requiring operators to repeat ``slice_column`` in
    ``exclude_columns`` themselves."""
    from qualifire.core.engine import _with_auto_slice_column_excluded
    from qualifire.core.config import SampleCollectionConfig, SampleHistoryConfig

    coll = SampleCollectionConfig(
        slice_column="sale_date",
        slice_value="{{ ds }}",
        history=SampleHistoryConfig(past_dates=2, step="P1D"),
    )
    # Operator passes ['sale_id'] — engine prepends 'sale_date'.
    out = _with_auto_slice_column_excluded(["sale_id"], coll)
    assert "sale_id" in out
    assert "sale_date" in out

    # No duplication when operator already listed slice_column.
    out2 = _with_auto_slice_column_excluded(["sale_id", "sale_date"], coll)
    assert out2.count("sale_date") == 1

    # No slice_column → no auto-add (history.filters mode).
    coll_noslice = SampleCollectionConfig(
        history=SampleHistoryConfig(past_dates=2, step="P1D"),
    )
    out3 = _with_auto_slice_column_excluded(["sale_id"], coll_noslice)
    assert out3 == ["sale_id"]

    # ``None`` operator excludes — auto-add still works.
    out4 = _with_auto_slice_column_excluded(None, coll)
    assert out4 == ["sale_date"]
