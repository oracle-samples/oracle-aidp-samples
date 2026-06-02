"""Tests for Pydantic config models."""

import tempfile
from pathlib import Path

import pytest
import yaml

from qualifire.core.config import (
    AggregationCollectionConfig,
    AnomalyDetectionValidationConfig,
    CustomQueryCollectionConfig,
    DatasetConfig,
    ForecastValidationConfig,
    HistoricalValidationConfig,
    MetricsCollectionConfig,
    QualifireConfig,
    RecencyCollectionConfig,
    SampleCollectionConfig,
    SLOValidationConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
    WAPConfig,
    load_config,
)
from qualifire.core.exceptions import QualifireConfigError


class TestRecencyCollectionConfig:
    def test_max_column_requires_column(self):
        with pytest.raises(ValueError, match="column"):
            RecencyCollectionConfig(strategy="max_column")

    def test_custom_sql_requires_sql(self):
        with pytest.raises(ValueError, match="sql"):
            RecencyCollectionConfig(strategy="custom_sql")

    def test_valid_max_column(self):
        rc = RecencyCollectionConfig(strategy="max_column", column="updated_at")
        assert rc.column == "updated_at"


class TestDatasetConfig:
    def test_requires_source(self):
        with pytest.raises(ValueError, match="table.*query.*wap"):
            DatasetConfig(name="test")

    def test_valid_with_table(self):
        ds = DatasetConfig(name="test", table="db.schema.table")
        assert ds.table == "db.schema.table"

    def test_valid_with_query(self):
        ds = DatasetConfig(name="test", query="SELECT 1 AS val")
        assert ds.query == "SELECT 1 AS val"
        assert ds.table is None

    def test_query_with_dimensions_and_measures(self):
        ds = DatasetConfig(
            name="test",
            query="SELECT region, SUM(amount) AS total FROM t GROUP BY region",
            dimensions=["region"],
            measures=["total"],
        )
        assert ds.dimensions == ["region"]
        assert ds.measures == ["total"]

    def test_query_and_table_mutual_exclusivity(self):
        # Error wording changed when 'df' became a first-class source
        # alongside 'table'. The semantics are unchanged.
        with pytest.raises(ValueError, match="mutually exclusive"):
            DatasetConfig(name="test", table="t1", query="SELECT 1")

    def test_query_and_wap_mutual_exclusivity(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            DatasetConfig(
                name="test",
                query="SELECT 1",
                wap=WAPConfig(target_table="t1"),
            )

    def test_dimensions_and_measures_optional(self):
        ds = DatasetConfig(name="test", query="SELECT 1 AS val")
        assert ds.dimensions is None
        assert ds.measures is None


class TestQualifireConfig:
    def test_minimal(self):
        cfg = QualifireConfig(
            owner="team", bu="finance", system_table="sys.qf.history",
            datasets=[DatasetConfig(name="ds1", table="t1")],
        )
        assert cfg.owner == "team"

    def test_defaults(self):
        cfg = QualifireConfig(
            owner="o", bu="b", system_table="s",
            datasets=[DatasetConfig(name="d", table="t")],
        )
        assert cfg.system_table_backend == "external_catalog"
        assert cfg.context == {}

class TestValidationConfigName:
    def test_name_defaults_to_none(self):
        vc = ThresholdValidationConfig(
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
        )
        assert vc.name is None

    def test_name_set_explicitly(self):
        vc = ThresholdValidationConfig(
            name="count_last_7d",
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
        )
        assert vc.name == "count_last_7d"


class TestCollectionConfigDimensions:
    def test_aggregation_dimensions_defaults_none(self):
        cc = AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"})
        assert cc.dimensions is None

    def test_aggregation_dimensions_set(self):
        cc = AggregationCollectionConfig(
            expressions={"cnt": "COUNT(*)"}, dimensions=["created_at"]
        )
        assert cc.dimensions == ["created_at"]

    def test_metrics_dimensions(self):
        mc = MetricsCollectionConfig(metrics={"cnt": "COUNT(*)"}, dimensions=["region"])
        assert mc.dimensions == ["region"]

    def test_custom_query_dimensions(self):
        cq = CustomQueryCollectionConfig(sql="SELECT 1", dimensions=["date_col"])
        assert cq.dimensions == ["date_col"]

    def test_dimensions_rejects_sql_injection(self):
        with pytest.raises(ValueError, match="valid SQL column identifier"):
            AggregationCollectionConfig(
                expressions={"cnt": "COUNT(*)"},
                dimensions=["1; DROP TABLE x; --"],
            )

    def test_dimensions_accepts_qualified_names(self):
        cc = AggregationCollectionConfig(
            expressions={"cnt": "COUNT(*)"}, dimensions=["schema.table_col"]
        )
        assert cc.dimensions == ["schema.table_col"]

    def test_multiple_dimensions(self):
        cc = AggregationCollectionConfig(
            expressions={"cnt": "COUNT(*)"}, dimensions=["region", "category"]
        )
        assert cc.dimensions == ["region", "category"]


class TestValidationConfigNameValidation:
    def test_name_rejects_sql_metacharacters(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            ThresholdValidationConfig(
                name="'; DROP TABLE --",
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            )

    def test_name_accepts_valid_characters(self):
        vc = ThresholdValidationConfig(
            name="count_last_7d.v2-beta",
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
        )
        assert vc.name == "count_last_7d.v2-beta"


class TestLoadConfig:
    def test_load_yaml(self, tmp_path):
        config_data = {
            "owner": "team-x",
            "bu": "finance",
            "system_table": "cat.schema.history",
            "datasets": [
                {
                    "name": "sales",
                    "table": "cat.schema.sales",
                    "validations": [
                        {
                            "type": "slo",
                            "recency": {"strategy": "max_column", "column": "updated_at"},
                            "thresholds": {"warning": "PT4H", "error": "PT8H"},
                        }
                    ],
                }
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config_data))

        cfg = load_config(str(config_path))
        assert cfg.owner == "team-x"
        assert len(cfg.datasets) == 1
        assert cfg.datasets[0].validations[0].type == "slo"

    def test_missing_file(self):
        with pytest.raises(QualifireConfigError, match="not found"):
            load_config("/nonexistent/path.yaml")

    def test_invalid_yaml(self, tmp_path):
        config_path = tmp_path / "bad.yaml"
        config_path.write_text(": : : invalid")
        with pytest.raises(QualifireConfigError):
            load_config(str(config_path))


class TestOnEmptyDataConfig:
    """Tests for the on_empty_data configuration field."""

    def test_default_is_warning(self):
        cfg = ThresholdValidationConfig(
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
        )
        assert cfg.on_empty_data == "warning"

    def test_explicit_pass(self):
        cfg = ThresholdValidationConfig(
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            on_empty_data="pass",
        )
        assert cfg.on_empty_data == "pass"

    def test_explicit_error(self):
        cfg = ThresholdValidationConfig(
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
            on_empty_data="error",
        )
        assert cfg.on_empty_data == "error"

    def test_invalid_value_rejected(self):
        with pytest.raises(ValueError):
            ThresholdValidationConfig(
                collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
                on_empty_data="crash",
            )

    def test_present_on_all_validation_types(self):
        """All five validation config types support on_empty_data."""
        slo = SLOValidationConfig(
            recency=RecencyCollectionConfig(strategy="max_column", column="ts"),
            thresholds={"warning": "PT4H"},
        )
        threshold = ThresholdValidationConfig(
            collection=AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"}),
            rules=[ThresholdRuleConfig(metric="cnt", thresholds=ThresholdLevels())],
        )
        assert slo.on_empty_data == "warning"
        assert threshold.on_empty_data == "warning"

    def test_yaml_round_trip(self, tmp_path):
        config_data = {
            "owner": "team",
            "bu": "bu",
            "system_table": "sys",
            "datasets": [
                {
                    "name": "ds",
                    "table": "t",
                    "validations": [
                        {
                            "type": "threshold",
                            "on_empty_data": "error",
                            "collection": {
                                "type": "aggregation",
                                "expressions": {"cnt": "COUNT(*)"},
                            },
                            "rules": [
                                {"metric": "cnt", "thresholds": {"error": {"min": 1}}},
                            ],
                        }
                    ],
                }
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config_data))

        cfg = load_config(str(config_path))
        assert cfg.datasets[0].validations[0].on_empty_data == "error"


class TestSampleCollectionConfig:
    def test_slice_column_alone_raises(self):
        """``slice_column`` without ``slice_value`` is incoherent —
        the sampler has no value to compare against."""
        with pytest.raises(ValueError, match="set together"):
            SampleCollectionConfig(slice_column="event_dt")

    def test_slice_value_alone_raises(self):
        with pytest.raises(ValueError, match="set together"):
            SampleCollectionConfig(slice_value="{{ ds }}")

    def test_slice_column_with_slice_value(self):
        cfg = SampleCollectionConfig(
            slice_column="event_dt", slice_value="{{ ds }}"
        )
        assert cfg.slice_column == "event_dt"
        assert cfg.slice_value == "{{ ds }}"

    def test_slice_column_accepts_dotted_qualifier(self):
        """``schema.column`` form is accepted for tables that need
        qualified references."""
        cfg = SampleCollectionConfig(
            slice_column="schema.event_dt", slice_value="{{ ds }}"
        )
        assert cfg.slice_column == "schema.event_dt"

    def test_slice_column_rejects_sql_fragment(self):
        """A non-identifier value (typo, accidental SQL fragment)
        must fail at config-load with a clear error — not surface
        as a backend SQL error far from the YAML line that caused
        it. Defensive identifier check."""
        for bad in (
            "event_dt; DROP TABLE x",
            "event_dt = 'x'",
            "event dt",     # space
            "1col",         # leading digit
            "col-name",     # hyphen
            "",             # empty
        ):
            with pytest.raises(ValueError, match="not a valid SQL identifier|set together|cannot be"):
                SampleCollectionConfig(
                    slice_column=bad, slice_value="{{ ds }}"
                )

    def test_neither_set(self):
        """Sample collection is valid without slice_column /
        slice_value — caller may rely on history.filters or pure
        n_records sampling."""
        cfg = SampleCollectionConfig()
        assert cfg.slice_column is None
        assert cfg.slice_value is None


class TestExplainValueDriftFlag:
    """AC#14 / AC#17: ``explain_value_drift`` accepted on both
    AnomalyModelConfig and PatternModelConfig, default True."""

    def test_anomaly_model_default_true(self):
        from qualifire.core.config import AnomalyModelConfig
        assert AnomalyModelConfig().explain_value_drift is True

    def test_pattern_model_default_true(self):
        from qualifire.core.config import PatternModelConfig
        assert PatternModelConfig().explain_value_drift is True

    def test_anomaly_model_accepts_false(self):
        from qualifire.core.config import AnomalyModelConfig
        cfg = AnomalyModelConfig(explain_value_drift=False)
        assert cfg.explain_value_drift is False

    def test_pattern_model_accepts_false(self):
        from qualifire.core.config import PatternModelConfig
        cfg = PatternModelConfig(explain_value_drift=False)
        assert cfg.explain_value_drift is False

    def test_anomaly_yaml_round_trip(self):
        """YAML load → AnomalyDetectionValidationConfig surfaces the flag."""
        import yaml

        from qualifire.core.config import AnomalyDetectionValidationConfig

        raw = yaml.safe_load(
            """
            type: shape
            collection:
              type: sample
              n_records: 100
              slice_column: ds
              slice_value: '{{ ds }}'
              past_dates: 2
              step: P1D
            model:
              explain_value_drift: false
            """
        )
        cfg = AnomalyDetectionValidationConfig.model_validate(raw)
        assert cfg.model.explain_value_drift is False

    def test_pattern_yaml_round_trip(self):
        """YAML load → PatternValidationConfig surfaces the flag."""
        import yaml

        from qualifire.core.config import PatternValidationConfig

        raw = yaml.safe_load(
            """
            type: pattern
            collection:
              type: sample
              n_records: 100
              slice_column: ds
              slice_value: '{{ ds }}'
              past_dates: 2
              step: P1D
            model:
              explain_value_drift: false
            """
        )
        cfg = PatternValidationConfig.model_validate(raw)
        assert cfg.model.explain_value_drift is False
