"""Tests for HTML report generation."""

from qualifire.core.models import (
    DatasetResult,
    QualifireResult,
    Severity,
    ValidationResult,
)
from qualifire.reporting.html_report import generate_html_report


class TestHTMLReport:
    def test_generates_valid_html(self):
        result = QualifireResult(
            owner="team-x",
            bu="finance",
            run_id="test-run-1",
            datasets=[
                DatasetResult(
                    dataset_name="sales",
                    table="cat.schema.sales",
                    run_id="test-run-1",
                    validation_results=[
                        ValidationResult(
                            validation_name="slo_check",
                            validation_type="slo",
                            severity=Severity.PASS,
                            message="Data is fresh",
                            actual_value="PT2H",
                            validation_base_name="slo_check",
                        ),
                        ValidationResult(
                            validation_name="threshold_check",
                            validation_type="threshold",
                            severity=Severity.WARNING,
                            message="avg_sales below warning threshold",
                            actual_value=85.5,
                            validation_base_name="threshold_check",
                        ),
                    ],
                ),
            ],
        )
        html = generate_html_report(result)
        assert "<!DOCTYPE html>" in html
        assert "team-x" in html
        assert "slo_check" in html
        assert "WARNING" in html

    def test_write_to_file(self, tmp_path):
        result = QualifireResult(owner="o", bu="b", run_id="r1")
        output = tmp_path / "report.html"
        generate_html_report(result, str(output))
        assert output.exists()
        assert "<!DOCTYPE html>" in output.read_text()

    def test_static_report_does_not_carry_detail_panel_markers(self):
        """AC#12 (dashboard-rich-detail-panel): the new detail panel
        ships only on the interactive dashboard. The static HTML
        report keeps its existing tiny filter-table script — adding
        the per-validator renderer registry, click toggles, or
        SNAPSHOT projection would expand its threat surface and is
        out of scope per the plan's hard stops.
        """
        result = QualifireResult(
            owner="o", bu="b", run_id="r1",
            datasets=[
                DatasetResult(
                    dataset_name="sales", table="t1",
                    validation_results=[
                        ValidationResult(
                            validation_name="v", validation_type="threshold",
                            validation_base_name="v",
                            severity=Severity.PASS, message="ok",
                        ),
                    ],
                ),
            ],
        )
        html = generate_html_report(result)
        # New feature's markers MUST be absent from the static report.
        for forbidden in [
            "DETAIL_RENDERERS",
            "qf-detail-toggle",
            "wireDetailToggles",
            "renderShapePattern",
            "value_drift_explainer",
            "snapshot-truncation-banner",
            "SNAPSHOT_TRUNCATED",
        ]:
            assert forbidden not in html, (
                f"static report leaked dashboard-rich-detail-panel marker: {forbidden}"
            )
