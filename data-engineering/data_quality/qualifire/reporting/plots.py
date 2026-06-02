"""Notebook-friendly plots for validation results and metrics history.

Two flavors of helpers:

* **Per-metric / per-result plots** (``plot_metric_history``,
  ``plot_validation_summary``, ``plot_anomaly_shap``,
  ``plot_forecast``) — take an in-memory ``QualifireResult`` or raw
  history rows. Useful right after a run completes.
* **Aggregate dashboard plots** (``plot_executive_summary*``,
  ``plot_dataset_day_heatmap``, ``plot_severity_hierarchy``, …) — take
  a coerced DataFrame produced by
  :func:`qualifire.reporting.system_table.load_health_dataframe`. These
  power the demo notebooks under ``tests/manual/`` and keep the
  notebooks free of inlined matplotlib / Plotly stitching.

Severity colors are exposed as :data:`SEVERITY_COLORS` so callers
matching their own UI palette have a single place to override.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


SEVERITY_COLORS: dict[str, str] = {
    "PASS": "#22c55e",
    "WARNING": "#f59e0b",
    "ERROR": "#ef4444",
    "UNKNOWN": "#6b7280",
}


def plot_metric_history(
    history: list[dict[str, Any]],
    metric_name: str = "",
    title: str | None = None,
) -> Any:
    """Plot metric values over time with validation status coloring.

    Args:
        history: List of dicts with 'run_timestamp', 'metric_value', 'validation_status'.
        metric_name: Metric name for axis label.
        title: Plot title.

    Returns:
        Matplotlib Figure object.
    """
    import matplotlib.pyplot as plt

    timestamps = [h["run_timestamp"] for h in history]
    values = [float(h["metric_value"]) for h in history]
    statuses = [h.get("validation_status", "PASS") for h in history]

    colors = {"PASS": "#22c55e", "WARNING": "#f59e0b", "ERROR": "#ef4444"}
    point_colors = [colors.get(s, "#6b7280") for s in statuses]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(timestamps, values, "-", color="#94a3b8", linewidth=1)
    ax.scatter(timestamps, values, c=point_colors, s=30, zorder=5)
    ax.set_title(title or f"Metric History: {metric_name}")
    ax.set_ylabel(metric_name)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    return fig


def plot_validation_summary(result: Any, title: str = "Validation Summary") -> Any:
    """Plot a bar chart of validation results by severity.

    Args:
        result: QualifireResult object.
        title: Plot title.
    """
    import matplotlib.pyplot as plt

    counts = {"PASS": 0, "WARNING": 0, "ERROR": 0}
    for ds in result.datasets:
        for vr in ds.validation_results:
            counts[vr.severity.value] += 1

    colors = ["#22c55e", "#f59e0b", "#ef4444"]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(counts.keys(), counts.values(), color=colors)
    ax.set_title(title)
    ax.set_ylabel("Count")
    fig.tight_layout()
    return fig


def plot_anomaly_shap(
    details: dict[str, Any],
    title: str = "Top Contributing Features (SHAP)",
) -> Any:
    """Plot SHAP feature importance from anomaly detection results.

    Args:
        details: ValidationResult.details dict containing 'top_contributing_features'.
        title: Plot title.
    """
    import matplotlib.pyplot as plt

    features = details.get("top_contributing_features", [])
    if not features:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No SHAP data available", ha="center", va="center")
        return fig

    names = [f["feature"] for f in features]
    importances = [f["importance"] for f in features]

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(names, importances, color="#6366f1")
    ax.set_xlabel("Mean |SHAP value|")
    ax.set_title(title)
    ax.invert_yaxis()
    fig.tight_layout()
    return fig


def plot_value_drift(
    explainer: list[dict[str, Any]],
    title: str = "Value Drift (current vs past)",
) -> Any:
    """Plot per-feature value-drift summaries from the explainer.

    Renders a horizontal grouped bar chart with one row per feature,
    current vs past on the per-kind scalar:

    - numeric → ``mean``
    - boolean → ``true_rate``
    - onehot → ``rate``
    - label_encoded → top-1 category's ``rate``
    - datetime → omitted from the bar metric; rendered as a text
      annotation row
    - unknown / truncated → omitted

    Args:
        explainer: Output of ``explain_value_drift`` (typically read
            from ``ValidationResult.details["value_drift_explainer"]``).
        title: Plot title.
    """
    import matplotlib.pyplot as plt

    rows: list[tuple[str, float, float]] = []
    annotations: list[str] = []
    for entry in explainer or []:
        if not isinstance(entry, dict):
            continue
        kind = entry.get("kind")
        feat = str(entry.get("feature") or entry.get("source_column") or "?")
        cur = entry.get("current") or {}
        past = entry.get("past") or {}
        if kind == "numeric":
            cv, pv = cur.get("mean"), past.get("mean")
        elif kind == "boolean":
            cv, pv = cur.get("true_rate"), past.get("true_rate")
        elif kind == "onehot":
            cv, pv = cur.get("rate"), past.get("rate")
        elif kind == "label_encoded":
            cur_top = (cur.get("top") or [{}])[:1]
            past_top = (past.get("top") or [{}])[:1]
            cv = cur_top[0].get("rate") if cur_top else None
            pv = past_top[0].get("rate") if past_top else None
        elif kind == "datetime":
            annotations.append(
                f"{feat}: cur=[{cur.get('min')}..{cur.get('max')}] "
                f"past=[{past.get('min')}..{past.get('max')}]"
            )
            continue
        else:
            continue
        if cv is None or pv is None:
            continue
        rows.append((feat, float(cv), float(pv)))

    if not rows and not annotations:
        fig, ax = plt.subplots(figsize=(6, 2))
        ax.text(0.5, 0.5, "No plottable drift data", ha="center", va="center")
        ax.set_axis_off()
        return fig

    n = max(len(rows), 1)
    fig, ax = plt.subplots(figsize=(8, max(2.5, 0.6 * n + 1.5)))
    if rows:
        labels = [r[0] for r in rows]
        cur_vals = [r[1] for r in rows]
        past_vals = [r[2] for r in rows]
        y = list(range(len(labels)))
        bar_h = 0.4
        ax.barh([i + bar_h / 2 for i in y], cur_vals, height=bar_h,
                color="#3b82f6", label="current")
        ax.barh([i - bar_h / 2 for i in y], past_vals, height=bar_h,
                color="#94a3b8", label="past")
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.invert_yaxis()
        ax.set_xlabel("value")
        ax.legend(loc="lower right", fontsize=8)
    else:
        ax.set_axis_off()
    if annotations:
        ax.set_title(title)
        annotation_text = "\n".join(annotations)
        fig.text(0.02, 0.02, annotation_text, fontsize=8, family="monospace")
    else:
        ax.set_title(title)
    fig.tight_layout()
    return fig


def plot_forecast(
    history: list[dict[str, Any]],
    forecast_details: dict[str, Any],
    current_value: float,
    metric_name: str = "",
    title: str | None = None,
) -> Any:
    """Plot forecast prediction band with historical and current values.

    Args:
        history: Historical metric values.
        forecast_details: Expected value dict with yhat, warning/error bounds.
        current_value: Current observed value.
        metric_name: Metric name for labels.
        title: Plot title.
    """
    import matplotlib.pyplot as plt

    timestamps = [h["run_timestamp"] for h in history]
    values = [float(h["metric_value"]) for h in history]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(timestamps, values, ".-", color="#94a3b8", label="Historical")

    # Plot current value
    ax.scatter(["Current"], [current_value], color="#1f2937", s=80, zorder=10, label="Current")

    # Plot bands
    yhat = forecast_details.get("yhat")
    if yhat is not None:
        ax.axhline(y=yhat, color="#3b82f6", linestyle="--", alpha=0.7, label="Predicted")
    wl = forecast_details.get("warning_lower")
    wu = forecast_details.get("warning_upper")
    if wl is not None and wu is not None:
        ax.axhspan(wl, wu, alpha=0.1, color="#f59e0b", label="Warning band")
    el = forecast_details.get("error_lower")
    eu = forecast_details.get("error_upper")
    if el is not None and eu is not None:
        ax.axhspan(el, eu, alpha=0.05, color="#ef4444", label="Error band")

    ax.set_title(title or f"Forecast: {metric_name}")
    ax.set_ylabel(metric_name)
    ax.legend(loc="upper left", fontsize=8)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Aggregate dashboard plots — operate on a DataFrame produced by
# ``qualifire.reporting.system_table.load_health_dataframe``. The DataFrame
# carries: ``run_timestamp``, ``partition_ts`` (datetimes), ``metric_value``
# (numeric), ``severity`` (uppercase status), ``day`` (date), plus the raw
# system-table columns (``dataset_name``, ``validation_name``,
# ``validation_type``, …).
#
# All helpers are no-ops on an empty DataFrame: they return an empty
# matplotlib Figure (or ``None`` for Plotly) so calling code can render
# them unconditionally without an ``if df.empty`` guard at every site.
# ---------------------------------------------------------------------------


def _empty_figure(message: str) -> Any:
    """Render a matplotlib Figure with a single centered message."""
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 2.5))
    ax.text(0.5, 0.5, message, ha="center", va="center",
            color="#6b7280", fontsize=11)
    ax.axis("off")
    return fig


def _save_if_requested(fig: Any, output_dir: Any, filename: str) -> None:
    """Persist ``fig`` as PNG into ``output_dir`` when given."""
    if output_dir is None:
        return
    out_path = Path(output_dir) / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=140, bbox_inches="tight")


def plot_executive_summary(
    df: Any,
    *,
    target_pass_rate: float = 95.0,
    output_dir: Any = None,
) -> Any:
    """Three-panel print-friendly executive view: KPI cards + daily
    pass-rate trend + worst-offending datasets.

    Args:
        df: DataFrame from ``load_health_dataframe``.
        target_pass_rate: Reference line on the trend chart (% pass).
        output_dir: Optional directory; when provided the figure is
            also written as ``executive_summary.png``.

    Returns:
        Matplotlib Figure.
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import pandas as pd

    if df is None or df.empty:
        return _empty_figure("No system-table rows in window — nothing to chart.")

    fig = plt.figure(figsize=(13, 9), constrained_layout=True)
    gs = fig.add_gridspec(2, 3)

    # Cards.
    ax_cards = fig.add_subplot(gs[0, :])
    ax_cards.axis("off")
    total = len(df)
    counts = df["severity"].value_counts()
    pass_rate = counts.get("PASS", 0) / total * 100 if total else 0
    warn_rate = counts.get("WARNING", 0) / total * 100 if total else 0
    err_rate = counts.get("ERROR", 0) / total * 100 if total else 0
    cards = [
        ("Total checks", f"{total:,}", "#374151"),
        ("Pass rate", f"{pass_rate:.1f}%", SEVERITY_COLORS["PASS"]),
        ("Warning rate", f"{warn_rate:.1f}%", SEVERITY_COLORS["WARNING"]),
        ("Error rate", f"{err_rate:.1f}%", SEVERITY_COLORS["ERROR"]),
    ]
    for i, (label, value, color) in enumerate(cards):
        x = 0.05 + i * 0.24
        ax_cards.text(x, 0.6, value, fontsize=28, fontweight="bold", color=color)
        ax_cards.text(x, 0.25, label, fontsize=12, color="#6b7280")

    # Daily pass-rate trend.
    ax_trend = fig.add_subplot(gs[1, :2])
    daily = (
        df.groupby("day")
        .apply(lambda g: (g["severity"] == "PASS").mean() * 100, include_groups=False)
        .reset_index(name="pass_rate")
    )
    daily["day"] = pd.to_datetime(daily["day"])
    daily = daily.sort_values("day")
    ax_trend.plot(daily["day"], daily["pass_rate"], marker="o",
                  linewidth=2, color=SEVERITY_COLORS["PASS"])
    ax_trend.axhline(target_pass_rate, color="#9ca3af", linestyle="--",
                     linewidth=1, label=f"{target_pass_rate:g}% target")
    ax_trend.set_ylim(0, 105)
    ax_trend.set_ylabel("Pass rate %")
    ax_trend.set_title("Daily pass rate", fontsize=12, loc="left")
    ax_trend.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax_trend.grid(True, alpha=0.25)
    ax_trend.legend(loc="lower left", fontsize=9)

    # Worst offenders by error count.
    ax_worst = fig.add_subplot(gs[1, 2])
    worst = (
        df[df["severity"] == "ERROR"]
        .groupby("dataset_name")
        .size()
        .sort_values(ascending=True)
        .tail(8)
    )
    if len(worst):
        ax_worst.barh(worst.index, worst.values, color=SEVERITY_COLORS["ERROR"])
        for i, v in enumerate(worst.values):
            ax_worst.text(v, i, f" {v}", va="center", fontsize=9)
    else:
        ax_worst.text(0.5, 0.5, "No errors in this window",
                      ha="center", va="center", transform=ax_worst.transAxes,
                      color=SEVERITY_COLORS["PASS"], fontsize=12)
    ax_worst.set_title("Worst offenders (errors)", fontsize=12, loc="left")
    ax_worst.set_xlabel("Error count")

    fig.suptitle("Data Quality — Executive summary", fontsize=15, fontweight="bold")
    _save_if_requested(fig, output_dir, "executive_summary.png")
    return fig


def plot_executive_summary_interactive(
    df: Any,
    *,
    days: int | None = None,
    target_pass_rate: float = 95.0,
) -> Any:
    """Plotly version of :func:`plot_executive_summary` — donut +
    daily-pass-rate line. Returns ``None`` on empty input so callers
    can ``if fig: fig.show()`` cleanly."""
    if df is None or df.empty:
        return None
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    counts = df["severity"].value_counts().to_dict()
    daily = (
        df.groupby("day")
        .apply(lambda g: (g["severity"] == "PASS").mean() * 100, include_groups=False)
        .reset_index(name="pass_rate")
        .sort_values("day")
    )

    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[0.4, 0.6],
        specs=[[{"type": "domain"}, {"type": "xy"}]],
        subplot_titles=("Severity mix", "Daily pass rate"),
    )
    fig.add_trace(go.Pie(
        labels=list(counts.keys()),
        values=list(counts.values()),
        hole=0.55,
        marker={"colors": [SEVERITY_COLORS.get(k, SEVERITY_COLORS["UNKNOWN"])
                            for k in counts.keys()]},
        hovertemplate="%{label}: %{value} (%{percent})<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=daily["day"], y=daily["pass_rate"], mode="lines+markers",
        line={"color": SEVERITY_COLORS["PASS"], "width": 2},
        marker={"size": 8},
        name="Pass rate %",
        hovertemplate="%{x}<br>%{y:.1f}%<extra></extra>",
    ), row=1, col=2)
    # Hardcoded xref="x2 domain" / yref="y2" because the trend
    # subplot is the second cell (col=2) of a 1×2 grid built by
    # `make_subplots` above, and Plotly numbers axes by position.
    # If the subplot layout ever changes (e.g. a 2×2 grid), this
    # reference must be updated to match. ``add_hline(row=1, col=2)``
    # would auto-resolve, but it crashes on figures that mix
    # ``domain`` (Pie) and xy subplots — Plotly's subplot scanner
    # reads ``xaxis`` off the Pie trace and explodes. Adding a
    # ``shape`` directly bypasses the scanner.
    fig.add_shape(
        type="line", xref="x2 domain", yref="y2",
        x0=0, x1=1, y0=target_pass_rate, y1=target_pass_rate,
        line={"color": "#9ca3af", "dash": "dash"},
    )
    fig.update_yaxes(title_text="%", range=[0, 105], row=1, col=2)
    title = (
        f"Data Quality — last {days} days" if days
        else "Data Quality — overview"
    )
    fig.update_layout(
        title=title,
        height=420, showlegend=False,
        margin={"l": 20, "r": 20, "t": 60, "b": 40},
    )
    return fig


def plot_dataset_day_heatmap(df: Any, *, output_dir: Any = None) -> Any:
    """Dataset × day heatmap colored by worst severity per cell."""
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    if df is None or df.empty:
        return _empty_figure("No system-table rows — nothing to chart.")

    score_map = {"PASS": 0, "WARNING": 1, "ERROR": 2, "UNKNOWN": 0}
    pivot = (
        df.assign(score=df["severity"].map(score_map))
        .groupby(["dataset_name", "day"])["score"]
        .max()
        .unstack(fill_value=-1)
        .sort_index()
    )

    fig, ax = plt.subplots(
        figsize=(max(10, 0.4 * len(pivot.columns)),
                 max(3, 0.45 * len(pivot.index))),
    )
    cmap = mcolors.ListedColormap([
        "#1a1d23",  # missing
        SEVERITY_COLORS["PASS"],
        SEVERITY_COLORS["WARNING"],
        SEVERITY_COLORS["ERROR"],
    ])
    norm = mcolors.BoundaryNorm([-1.5, -0.5, 0.5, 1.5, 2.5], cmap.N)
    im = ax.imshow(pivot.values, aspect="auto", cmap=cmap, norm=norm)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(
        [d.strftime("%b %d") for d in pivot.columns],
        rotation=45, ha="right", fontsize=8,
    )
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)
    ax.set_title("Dataset × day — worst severity per cell",
                 fontsize=12, loc="left")

    cbar = fig.colorbar(im, ticks=[-1, 0, 1, 2], shrink=0.5)
    cbar.ax.set_yticklabels(["no run", "PASS", "WARNING", "ERROR"])

    fig.tight_layout()
    _save_if_requested(fig, output_dir, "dataset_day_heatmap.png")
    return fig


def plot_validation_history_interactive(
    df: Any,
    *,
    dataset_name: str | None = None,
    validation_name: str | None = None,
) -> Any:
    """Per-(dataset, validation) metric history — Plotly scatter, one
    trace per ``dimension_value``, severity-colored markers.

    When ``dataset_name`` / ``validation_name`` are omitted, picks the
    pair with the most ERROR rows (so ad-hoc demos land somewhere
    interesting). Returns ``None`` if there's no numeric history to
    show after filtering.
    """
    if df is None or df.empty:
        return None
    import plotly.graph_objects as go

    if dataset_name is None or validation_name is None:
        err_rank = (
            df[df["severity"] == "ERROR"]
            .groupby(["dataset_name", "validation_name"])
            .size()
            .sort_values(ascending=False)
        )
        if len(err_rank):
            dataset_name = dataset_name or err_rank.index[0][0]
            validation_name = validation_name or err_rank.index[0][1]
        else:
            head = df.iloc[0]
            dataset_name = dataset_name or head["dataset_name"]
            validation_name = validation_name or head["validation_name"]

    sub = df[
        (df["dataset_name"] == dataset_name)
        & (df["validation_name"] == validation_name)
        & df["metric_value"].notna()
    ].copy()
    if sub.empty:
        return None

    sub["x"] = sub["partition_ts"].fillna(sub["run_timestamp"])
    sub = sub.sort_values("x")

    fig = go.Figure()
    # Display label only — the persisted column carries SQL NULL for
    # non-dimensioned rows; show "(no dimension)" so the legend reads
    # cleanly. Guard against the column being missing entirely.
    if "dimension_value" in sub.columns:
        dim_label = sub["dimension_value"].fillna("(no dimension)")
    else:
        import pandas as pd

        dim_label = pd.Series(["(no dimension)"] * len(sub), index=sub.index)
    for dim, sub_dim in sub.groupby(dim_label):
        fig.add_trace(go.Scatter(
            x=sub_dim["x"], y=sub_dim["metric_value"],
            mode="lines+markers",
            name=str(dim),
            line={"color": "#9ca3af", "width": 1},
            marker={
                "size": 10,
                "color": sub_dim["severity"].map(SEVERITY_COLORS).tolist(),
                "line": {"color": "#374151", "width": 1},
            },
            text=[
                f"{s}<br>{m}"
                for s, m in zip(
                    sub_dim["severity"],
                    sub_dim.get("validation_message", "").fillna("")
                    if "validation_message" in sub_dim.columns
                    else [""] * len(sub_dim),
                )
            ],
            hovertemplate="%{x}<br>value=%{y}<br>%{text}<extra></extra>",
        ))

    fig.update_layout(
        title=f"{dataset_name} › {validation_name} — partition history",
        xaxis_title="partition_ts (or run_timestamp if absent)",
        yaxis_title="metric_value",
        height=460,
    )
    # Surface descriptions when present — they ride on every system-table row.
    annotations: list[str] = []
    if "dataset_description" in sub.columns:
        ds_desc = sub["dataset_description"].dropna().head(1).tolist()
        if ds_desc:
            annotations.append(f"<b>Dataset:</b> {ds_desc[0]}")
    if "validation_description" in sub.columns:
        val_desc = sub["validation_description"].dropna().head(1).tolist()
        if val_desc:
            annotations.append(f"<b>Validation:</b> {val_desc[0]}")
    if annotations:
        fig.add_annotation(
            xref="paper", yref="paper", x=0, y=1.13, showarrow=False,
            text="<br>".join(annotations), align="left",
            font={"size": 11, "color": "#6b7280"},
        )
    return fig


def plot_severity_hierarchy(df: Any) -> tuple[Any, Any]:
    """Return ``(sunburst, treemap)`` Plotly figures showing
    ``dataset_name → validation_type → severity``. Both fall back to
    ``None`` on empty input."""
    if df is None or df.empty:
        return None, None
    import plotly.express as px

    grouped = (
        df.assign(severity=df["severity"].fillna("UNKNOWN"))
        .groupby(["dataset_name", "validation_type", "severity"])
        .size()
        .reset_index(name="count")
    )
    sunburst = px.sunburst(
        grouped,
        path=["dataset_name", "validation_type", "severity"],
        values="count",
        color="severity",
        color_discrete_map=SEVERITY_COLORS,
        title="Dataset → validation type → severity",
    )
    sunburst.update_layout(height=520)

    treemap = px.treemap(
        grouped,
        path=["dataset_name", "validation_type", "severity"],
        values="count",
        color="severity",
        color_discrete_map=SEVERITY_COLORS,
        title="Same data as treemap",
    )
    treemap.update_layout(height=420)
    return sunburst, treemap


def plot_severity_by_type(df: Any, *, output_dir: Any = None) -> Any:
    """Stacked bar chart of severity by ``validation_type``."""
    import matplotlib.pyplot as plt
    import numpy as np

    if df is None or df.empty:
        return _empty_figure("No system-table rows — nothing to chart.")

    by_type = (
        df.groupby(["validation_type", "severity"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=["PASS", "WARNING", "ERROR"], fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(10, 4.5))
    bottom = np.zeros(len(by_type))
    for sev in ("PASS", "WARNING", "ERROR"):
        ax.bar(by_type.index, by_type[sev], bottom=bottom,
               label=sev, color=SEVERITY_COLORS[sev])
        bottom += by_type[sev].values
    ax.set_title("Severity by validation type", loc="left")
    ax.set_ylabel("Count")
    ax.legend()
    fig.tight_layout()
    _save_if_requested(fig, output_dir, "severity_by_type.png")
    return fig


def plot_metric_distribution_by_severity(df: Any) -> Any:
    """Plotly violin of ``metric_value`` by severity. Returns ``None``
    when there are no numeric rows to plot."""
    if df is None or df.empty:
        return None
    import plotly.express as px

    numeric = df.dropna(subset=["metric_value"])
    if numeric.empty:
        return None
    fig = px.violin(
        numeric, y="metric_value", x="severity", box=True, points="outliers",
        color="severity", color_discrete_map=SEVERITY_COLORS,
        title="Distribution of metric_value by severity (numeric metrics only)",
    )
    fig.update_layout(height=420)
    return fig
