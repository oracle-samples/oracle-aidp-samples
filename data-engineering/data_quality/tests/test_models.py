"""Tests for core data models."""

from datetime import datetime

from qualifire.core.models import (
    CollectionResult,
    DatasetResult,
    QualifireResult,
    Severity,
    ValidationResult,
)


class TestSeverity:
    def test_ordering(self):
        assert Severity.ERROR > Severity.WARNING
        assert Severity.WARNING > Severity.PASS
        assert Severity.PASS < Severity.ERROR

    def test_equality(self):
        assert Severity.PASS == Severity.PASS
        assert not (Severity.PASS > Severity.PASS)

    def test_ge_le(self):
        assert Severity.ERROR >= Severity.WARNING
        assert Severity.ERROR >= Severity.ERROR
        assert Severity.PASS <= Severity.WARNING


class TestDatasetResult:
    def test_overall_severity_error(self):
        ds = DatasetResult(
            dataset_name="test",
            table="t",
            validation_results=[
                ValidationResult(
                    validation_name="v1", validation_type="threshold",
                    severity=Severity.PASS, message="ok",
                    validation_base_name="v1",
                ),
                ValidationResult(
                    validation_name="v2", validation_type="slo",
                    severity=Severity.ERROR, message="bad",
                    validation_base_name="v2",
                ),
            ],
        )
        assert ds.overall_severity == Severity.ERROR
        assert ds.has_errors
        assert not ds.has_warnings

    def test_overall_severity_empty(self):
        ds = DatasetResult(dataset_name="empty", table="t")
        assert ds.overall_severity == Severity.PASS
        assert not ds.has_errors


class TestCollectionResult:
    def test_default_dimension_value(self):
        # Default is None (persisted as NULL). NULL is the only
        # no-dimension form across the API surface and storage —
        # No `"_default"` sentinel — NULL is the only no-dimension form.
        cr = CollectionResult(
            metric_name="count", metric_value=42, collected_at=datetime.now()
        )
        assert cr.dimension_value is None

    def test_custom_dimension_value(self):
        cr = CollectionResult(
            metric_name="count", metric_value=42,
            collected_at=datetime.now(), dimension_value="2026-04-01",
        )
        assert cr.dimension_value == "2026-04-01"


class TestDatasetResultCollections:
    def test_collection_results_default_empty(self):
        ds = DatasetResult(dataset_name="test", table="t")
        assert ds.collection_results == []

    def test_collection_results_populated(self):
        cr = CollectionResult(
            metric_name="count", metric_value=100, collected_at=datetime.now()
        )
        ds = DatasetResult(dataset_name="test", table="t", collection_results=[cr])
        assert len(ds.collection_results) == 1
        assert ds.collection_results[0].metric_name == "count"


class TestValidationResultRequiresBaseName:
    def test_constructing_without_validation_base_name_raises(self):
        """`validation_base_name` is required keyword-only (P1.1).

        Plan §P1.1 explicitly rejects an empty default — a default
        lets missed emit sites collapse onto each other under "" and
        silently mis-route alerts. Regression for PR-6 review fix.
        """
        import pytest
        with pytest.raises(TypeError):
            ValidationResult(
                validation_name="v",
                validation_type="t",
                severity=Severity.PASS,
                message="ok",
            )

    def test_constructing_positionally_does_not_silently_default_base(self):
        """Even with all four positional args supplied, the field must
        still be passed as a keyword."""
        import pytest
        with pytest.raises(TypeError):
            # Four positional args + no validation_base_name kw → TypeError
            ValidationResult("v", "t", Severity.PASS, "msg")


class TestQualifireResult:
    def test_aggregated_severity(self):
        result = QualifireResult(
            owner="test", bu="test",
            datasets=[
                DatasetResult(
                    dataset_name="ds1", table="t1",
                    validation_results=[
                        ValidationResult(
                            validation_name="v1", validation_type="t",
                            severity=Severity.WARNING, message="w",
                            validation_base_name="v1",
                        )
                    ],
                ),
                DatasetResult(
                    dataset_name="ds2", table="t2",
                    validation_results=[
                        ValidationResult(
                            validation_name="v2", validation_type="t",
                            severity=Severity.PASS, message="ok",
                            validation_base_name="v2",
                        )
                    ],
                ),
            ],
        )
        assert result.overall_severity == Severity.WARNING
        assert result.has_warnings
        assert not result.has_errors
