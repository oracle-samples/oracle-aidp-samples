"""Tests for PatternCheckValidator (Random Forest two-sample classifier)."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.pattern_check import PatternCheckValidator


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


def _normal_data(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        "amount": rng.normal(100, 10, n),
        "count": rng.randint(1, 50, n),
        "category": rng.choice(["A", "B", "C"], n),
    })


def _shifted_data(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        # Clear distribution shift on every numeric column plus a
        # skewed categorical distribution. Needs to be separable
        # enough that a seeded Random Forest will reach AUC > 0.65.
        "amount": rng.normal(500, 10, n),
        "count": rng.randint(200, 300, n),
        "category": rng.choice(["A", "B", "C"], n, p=[0.9, 0.05, 0.05]),
    })


# ===========================================================================
# Same-distribution PASS / shifted-distribution ALERT
# ===========================================================================


class TestPatternCheckPassFail:
    def test_pass_on_same_distribution(self):
        """Current & past drawn from the same RNG family -> AUC < 0.65 -> PASS."""
        current = _normal_data(300, seed=1)
        past = [
            ("past_1", _normal_data(300, seed=2)),
            ("past_2", _normal_data(300, seed=3)),
            ("past_3", _normal_data(300, seed=4)),
        ]
        validator = PatternCheckValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        assert len(results) == 1
        r = results[0]
        assert r.validation_type == "pattern"
        assert r.metric_name == "auc"
        assert 0.4 <= r.actual_value < 0.65, f"AUC was {r.actual_value}"
        assert r.severity == Severity.PASS
        assert "cv_folds" in r.details

    def test_alert_on_shifted_distribution(self):
        """Clearly shifted current vs past -> high AUC -> WARNING or ERROR."""
        current = _shifted_data(300, seed=1)
        past = [
            ("past_1", _normal_data(300, seed=2)),
            ("past_2", _normal_data(300, seed=3)),
            ("past_3", _normal_data(300, seed=4)),
        ]
        validator = PatternCheckValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        assert len(results) == 1
        r = results[0]
        assert r.actual_value > 0.65, f"AUC was {r.actual_value}"
        assert r.severity in (Severity.WARNING, Severity.ERROR)


# ===========================================================================
# Cold start — on_missing_history branches (acceptance §4)
# ===========================================================================


class TestColdStart:
    def test_no_past_defaults_to_pass(self):
        current = _normal_data(100, seed=1)
        validator = PatternCheckValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, []))

        assert results[0].severity == Severity.PASS
        assert results[0].details == {"cold_start": True}

    def test_no_past_on_missing_history_warn(self):
        current = _normal_data(100, seed=1)
        validator = PatternCheckValidator(
            explain=False, on_missing_history="warn"
        )
        results = validator.validate(_make_sample_collected(current, []))

        assert results[0].severity == Severity.WARNING
        assert results[0].details["cold_start"] is True

    def test_no_past_on_missing_history_error(self):
        current = _normal_data(100, seed=1)
        validator = PatternCheckValidator(
            explain=False, on_missing_history="error"
        )
        results = validator.validate(_make_sample_collected(current, []))

        assert results[0].severity == Severity.ERROR


# ===========================================================================
# Empty current — on_empty_data branches (acceptance §5)
# ===========================================================================


class TestEmptyCurrent:
    def test_empty_current_warns_by_default(self):
        current = pd.DataFrame(columns=["amount", "count", "category"])
        past = [("past_1", _normal_data(50, seed=2))]
        validator = PatternCheckValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.WARNING
        assert "Insufficient data" in results[0].message

    def test_empty_current_respects_on_empty_pass(self):
        current = pd.DataFrame(columns=["amount", "count", "category"])
        past = [("past_1", _normal_data(50, seed=2))]
        validator = PatternCheckValidator(
            explain=False, on_empty_data=Severity.PASS
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.PASS

    def test_empty_current_respects_on_empty_error(self):
        current = pd.DataFrame(columns=["amount", "count", "category"])
        past = [("past_1", _normal_data(50, seed=2))]
        validator = PatternCheckValidator(
            explain=False, on_empty_data=Severity.ERROR
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.ERROR


# ===========================================================================
# Tiny-sample CV guard (Risk §1 / plan.md Design §6)
# ===========================================================================


class TestTinySampleGuard:
    def test_returns_empty_data_when_class_too_small_for_cv(self):
        """min(class) < cv_folds must refuse to train, not crash."""
        current = _normal_data(3, seed=1)  # 3 rows < 5 folds
        past = [("past_1", _normal_data(3, seed=2))]
        validator = PatternCheckValidator(
            explain=False, cv_folds=5, on_empty_data=Severity.WARNING
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].severity == Severity.WARNING
        assert "Insufficient samples" in results[0].message
        assert "5-fold CV" in results[0].message

    def test_runs_at_sklearn_floor(self):
        """min(class) == cv_folds is the StratifiedKFold floor and must run."""
        # 5 rows per class with 5 folds exercises sklearn's real constraint.
        # Codex round 2 flagged the previous 2*cv_folds heuristic as stricter
        # than necessary.
        current = _normal_data(5, seed=1)
        past = [("past_1", _normal_data(5, seed=2))]
        validator = PatternCheckValidator(explain=False, cv_folds=5)
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].metric_name == "auc"
        assert results[0].actual_value is not None

    def test_runs_with_lower_cv_folds(self):
        """With cv_folds=2, 20 per class is plenty and must not hit the guard."""
        current = _normal_data(40, seed=1)
        past = [("past_1", _normal_data(40, seed=2))]
        validator = PatternCheckValidator(explain=False, cv_folds=2)
        results = validator.validate(_make_sample_collected(current, past))

        # Must return a real AUC result, not an empty-data one.
        assert results[0].metric_name == "auc"
        assert results[0].actual_value is not None


# ===========================================================================
# Schema-change detection (acceptance §6 structural)
# ===========================================================================


class TestSchemaChange:
    def test_alert_on_schema_change_emits_auxiliary_result(self):
        current = pd.DataFrame({
            "a": np.arange(100.0),
            "b": np.arange(100.0) * 2,
            "new_col": np.arange(100.0),
        })
        past = [(
            "past_1",
            pd.DataFrame({
                "a": np.arange(100.0) + 0.1,
                "b": np.arange(100.0) * 2 + 0.1,
                "old_col": np.arange(100.0),
            }),
        )]
        validator = PatternCheckValidator(
            explain=False, alert_on_schema_change=True
        )
        results = validator.validate(_make_sample_collected(current, past))

        schema_results = [r for r in results if "schema" in r.validation_name]
        assert len(schema_results) == 1
        assert schema_results[0].severity == Severity.WARNING


# ===========================================================================
# Explain on / off + SHAP failure fallback (acceptance §6, §7)
# ===========================================================================


class TestExplain:
    def test_explain_false_has_no_top_features_key(self):
        """When explain=False, key must be absent (not present-but-empty)."""
        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        assert "top_contributing_features" not in results[0].details

    def test_explain_true_populates_top_features_when_alerting(self):
        """severity != PASS + explain=True -> top_contributing_features list (len <= 5)."""
        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(explain=True)
        results = validator.validate(_make_sample_collected(current, past))

        r = results[0]
        if r.severity != Severity.PASS:
            assert "top_contributing_features" in r.details
            tf = r.details["top_contributing_features"]
            assert 1 <= len(tf) <= 5
            assert all("feature" in f and "importance" in f for f in tf)

    def test_value_drift_explainer_present_on_alert(self):
        """severity != PASS + explain_value_drift=True -> value_drift_explainer
        block parallel to top_contributing_features."""
        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(
            explain=True, explain_value_drift=True,
        )
        results = validator.validate(_make_sample_collected(current, past))
        r = results[0]
        if r.severity != Severity.PASS:
            tf = r.details.get("top_contributing_features") or []
            drift = r.details.get("value_drift_explainer")
            assert drift is not None
            assert len(drift) == len(tf)
            for entry in drift:
                assert "feature" in entry
                assert "kind" in entry
                assert "summary" in entry and entry["summary"]

    def test_value_drift_explainer_absent_when_disabled(self):
        """explain_value_drift=False -> top features still present, drift block absent."""
        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(
            explain=True, explain_value_drift=False,
        )
        results = validator.validate(_make_sample_collected(current, past))
        r = results[0]
        if r.severity != Severity.PASS:
            assert "value_drift_explainer" not in r.details
            assert "top_contributing_features" in r.details

    def test_value_drift_explainer_failure_degrades_gracefully(self, monkeypatch):
        """A drift-explainer raise must NOT crash the validator; record error key."""
        import qualifire.validation.pattern_check as mod

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated explainer failure")

        monkeypatch.setattr(mod, "explain_value_drift", _boom)
        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(
            explain=True, explain_value_drift=True,
        )
        results = validator.validate(_make_sample_collected(current, past))
        r = results[0]
        if r.severity != Severity.PASS:
            assert "value_drift_explainer" not in r.details
            assert "value_drift_explainer_error" in r.details
            assert "RuntimeError" in r.details["value_drift_explainer_error"]
            # Original verdict survived; top features still present.
            assert "top_contributing_features" in r.details

    def test_shap_failure_degrades_gracefully(self, monkeypatch):
        """A SHAP raise during explanation must NOT crash the validator."""
        import qualifire.validation.pattern_check as mod

        def _boom():
            class _Dummy:
                class TreeExplainer:
                    def __init__(self, *a, **kw):
                        raise RuntimeError("simulated SHAP failure")
            return _Dummy

        monkeypatch.setattr(mod, "_import_shap", _boom)

        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(explain=True)
        results = validator.validate(_make_sample_collected(current, past))

        r = results[0]
        # AUC + severity must still be populated; no top features key.
        assert r.actual_value is not None
        assert r.severity in (Severity.PASS, Severity.WARNING, Severity.ERROR)
        assert "top_contributing_features" not in r.details


# ===========================================================================
# Leakage control (acceptance §13)
# ===========================================================================


class TestLeakageControl:
    def test_without_exclude_columns_date_leaks_trivially(self):
        """Without exclude_columns the date column gives AUC ~ 1.0."""
        n = 300
        rng = np.random.RandomState(0)
        numeric = rng.normal(100, 10, n)

        current = pd.DataFrame({
            "amount": numeric,
            "event_date": ["2026-04-20"] * n,
        })
        past = [("past_1", pd.DataFrame({
            "amount": rng.normal(100, 10, n),
            "event_date": ["2026-04-13"] * n,
        }))]

        # No exclude_columns -> classifier trivially separates via event_date.
        validator = PatternCheckValidator(
            explain=False, warning_threshold=0.65, error_threshold=0.80
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].actual_value > 0.95, (
            f"expected near-perfect AUC from leakage, got {results[0].actual_value}"
        )
        assert results[0].severity == Severity.ERROR

    def test_with_exclude_columns_date_suppressed(self):
        """Adding event_date to exclude_columns brings AUC near 0.5 -> PASS."""
        n = 300
        rng = np.random.RandomState(0)

        current = pd.DataFrame({
            "amount": rng.normal(100, 10, n),
            "event_date": ["2026-04-20"] * n,
        })
        past = [("past_1", pd.DataFrame({
            "amount": rng.normal(100, 10, n),
            "event_date": ["2026-04-13"] * n,
        }))]

        validator = PatternCheckValidator(
            explain=False,
            warning_threshold=0.65,
            error_threshold=0.80,
            exclude_columns=["event_date"],
        )
        results = validator.validate(_make_sample_collected(current, past))

        assert results[0].actual_value < 0.65
        assert results[0].severity == Severity.PASS


# ===========================================================================
# No encodable columns (acceptance §5 structural)
# ===========================================================================


class TestNoEncodableColumns:
    def test_no_common_columns_returns_empty_data(self):
        """Zero intersection columns -> _empty_data_result with on_empty_data severity."""
        current = pd.DataFrame({"only_current": [1, 2, 3]})
        past = [("past_1", pd.DataFrame({"only_past": [4, 5, 6]}))]
        validator = PatternCheckValidator(
            explain=False, on_empty_data=Severity.WARNING
        )
        results = validator.validate(_make_sample_collected(current, past))

        # common_cols is empty -> encode of an empty-column frame -> 0 features.
        assert results[0].severity == Severity.WARNING
        assert "No encodable columns" in results[0].message


# ===========================================================================
# JSON-safety of details (Design §10)
# ===========================================================================


class TestPersistenceSafety:
    def test_details_are_json_serializable(self):
        import json

        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(explain=False)
        results = validator.validate(_make_sample_collected(current, past))

        # Must round-trip through json.dumps without a TypeError on
        # numpy scalars.
        json.dumps(results[0].details)
        json.dumps(results[0].expected_value)
        assert isinstance(results[0].actual_value, float)

    def test_details_with_shap_are_json_serializable(self):
        import json

        current = _shifted_data(200, seed=1)
        past = [("past_1", _normal_data(200, seed=2))]
        validator = PatternCheckValidator(explain=True)
        results = validator.validate(_make_sample_collected(current, past))

        json.dumps(results[0].details)


# ===========================================================================
# Skips non-sample metrics
# ===========================================================================


def test_skips_non_sample_metrics():
    validator = PatternCheckValidator(explain=False)
    results = validator.validate([
        CollectionResult(
            metric_name="not_a_sample",
            metric_value=42,
            collected_at=datetime.now(),
        )
    ])
    assert results == []


# ===========================================================================
# Programmatic builder <-> YAML parity (acceptance §8)
# ===========================================================================


class TestBuilderYamlParity:
    def test_pattern_check_builder_matches_yaml_default(self):
        from qualifire.api import Qualifire
        from qualifire.core.config import PatternValidationConfig

        built = Qualifire.pattern_check(
            n_records=10000,
            past_dates=3,
            step="P7D",
        )

        # Equivalent YAML path.
        import yaml

        yaml_src = yaml.safe_dump({
            "type": "pattern",
            "collection": {
                "type": "sample",
                "n_records": 10000,
                "history": {"past_dates": 3, "step": "P7D"},
            },
        })
        from_yaml = PatternValidationConfig.model_validate(yaml.safe_load(yaml_src))

        assert built.model_dump() == from_yaml.model_dump()

    def test_pattern_check_builder_with_history_filters(self):
        from qualifire.api import Qualifire
        from qualifire.core.config import PatternValidationConfig

        built = Qualifire.pattern_check(
            n_records=10000,
            history_filters=["ds='2026-04-01'", "ds='2026-04-08'"],
            step="P7D",
        )
        from_yaml = PatternValidationConfig.model_validate({
            "type": "pattern",
            "collection": {
                "type": "sample",
                "n_records": 10000,
                "history": {
                    "past_dates": 2,
                    "step": "P7D",
                    "filters": ["ds='2026-04-01'", "ds='2026-04-08'"],
                },
            },
        })
        assert built.model_dump() == from_yaml.model_dump()

    def test_mismatched_past_dates_and_history_filters_raises(self):
        """Codex round 3: don't silently rewrite caller intent."""
        from qualifire.api import Qualifire

        with pytest.raises(ValueError, match="past_dates.*does not match"):
            Qualifire.pattern_check(
                n_records=10000,
                past_dates=5,
                history_filters=["a", "b"],  # len=2 != 5
                step="P7D",
            )

    def test_explicit_past_dates_matching_history_filters_accepted(self):
        """Caller can legitimately pass both as a consistency cross-check."""
        from qualifire.api import Qualifire

        built = Qualifire.pattern_check(
            n_records=10000,
            past_dates=2,
            history_filters=["a", "b"],
            step="P7D",
        )
        assert built.collection.history.past_dates == 2
        assert built.collection.history.filters == ["a", "b"]


# ===========================================================================
# Preflight hyperparameter validation (Codex round 4)
# ===========================================================================


class TestPatternModelConfigValidation:
    def test_n_estimators_zero_rejected_at_config_time(self):
        from qualifire.core.config import PatternModelConfig

        with pytest.raises(ValueError, match="n_estimators"):
            PatternModelConfig(n_estimators=0)

    def test_n_estimators_negative_rejected_at_config_time(self):
        from qualifire.core.config import PatternModelConfig

        with pytest.raises(ValueError, match="n_estimators"):
            PatternModelConfig(n_estimators=-5)

    def test_max_depth_zero_rejected_at_config_time(self):
        from qualifire.core.config import PatternModelConfig

        with pytest.raises(ValueError, match="max_depth"):
            PatternModelConfig(max_depth=0)

    def test_max_depth_none_accepted(self):
        from qualifire.core.config import PatternModelConfig

        cfg = PatternModelConfig(max_depth=None)
        assert cfg.max_depth is None
