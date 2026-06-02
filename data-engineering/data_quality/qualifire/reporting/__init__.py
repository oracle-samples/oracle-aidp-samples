"""Public reporting surface for Qualifire.

Three layers, all importable from the top-level package:

* **Health aggregation** — :class:`HealthReporter`, :class:`HealthReport`
  compute pass/warn/error counts and trends from a SystemTableStorage.
* **HTML rendering** — :func:`generate_html_report` (single run),
  :func:`generate_health_html` (aggregated), and
  :func:`generate_interactive_html` (Plotly drill-down dashboard) emit
  self-contained HTML files.
* **Charts** — matplotlib + Plotly helpers that take either a
  ``QualifireResult`` (per-run) or a coerced DataFrame from
  :func:`load_health_dataframe` (aggregate). The aggregate helpers back
  the demo notebooks under ``tests/manual/``.

Helpers for opening a backend-agnostic storage handle
(:func:`make_storage`) and reconstructing a QualifireResult from
persisted rows (:func:`build_result_from_system_table`) round out the
surface so notebooks don't have to re-implement that wiring.
"""

from qualifire.reporting.health import HealthReport, HealthReporter
from qualifire.reporting.html_report import (
    generate_health_html,
    generate_html_report,
    generate_interactive_html,
)
from qualifire.reporting.plots import (
    SEVERITY_COLORS,
    plot_anomaly_shap,
    plot_dataset_day_heatmap,
    plot_executive_summary,
    plot_executive_summary_interactive,
    plot_forecast,
    plot_metric_distribution_by_severity,
    plot_metric_history,
    plot_severity_by_type,
    plot_severity_hierarchy,
    plot_validation_history_interactive,
    plot_validation_summary,
)
from qualifire.reporting.storage_factory import make_storage
from qualifire.reporting.system_table import (
    build_result_from_system_table,
    load_health_dataframe,
)

__all__ = [
    "HealthReport",
    "HealthReporter",
    "SEVERITY_COLORS",
    "build_result_from_system_table",
    "generate_health_html",
    "generate_html_report",
    "generate_interactive_html",
    "load_health_dataframe",
    "make_storage",
    "plot_anomaly_shap",
    "plot_dataset_day_heatmap",
    "plot_executive_summary",
    "plot_executive_summary_interactive",
    "plot_forecast",
    "plot_metric_distribution_by_severity",
    "plot_metric_history",
    "plot_severity_by_type",
    "plot_severity_hierarchy",
    "plot_validation_history_interactive",
    "plot_validation_summary",
]
