"""Static HTML dashboard report generator."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from qualifire.core.models import QualifireResult, Severity
from qualifire.reporting._snapshot_details import (
    _safe_json_for_html_script,
    _snapshot_details,
    enforce_total_cap,
)
from qualifire.reporting.health import _row_is_internal_failure


_SEVERITY_COLORS = {
    Severity.PASS: "#22c55e",
    Severity.WARNING: "#f59e0b",
    Severity.ERROR: "#ef4444",
}


def generate_html_report(result: QualifireResult, output_path: str | None = None) -> str:
    """Generate a standalone HTML report from a QualifireResult.

    Args:
        result: The QualifireResult to render.
        output_path: If provided, write HTML to this file.

    Returns:
        The HTML string.
    """
    rows_html = []
    for ds in result.datasets:
        for vr in ds.validation_results:
            color = _SEVERITY_COLORS.get(vr.severity, "#6b7280")
            rows_html.append(f"""
                <tr>
                    <td>{html.escape(ds.dataset_name)}</td>
                    <td>{html.escape(ds.table or 'N/A')}</td>
                    <td>{html.escape(vr.validation_name)}</td>
                    <td>{html.escape(vr.validation_type)}</td>
                    <td style="color: {color}; font-weight: bold;">{vr.severity.value}</td>
                    <td>{html.escape(vr.message)}</td>
                    <td>{html.escape(str(vr.actual_value) if vr.actual_value is not None else 'N/A')}</td>
                </tr>""")

    overall_color = _SEVERITY_COLORS.get(result.overall_severity, "#6b7280")
    report_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Qualifire Report — {html.escape(result.run_id)}</title>
    <style>
        .qf-dashboard {{
            --bg: #f9fafb;
            --surface: #ffffff;
            --text: #1f2937;
            --text-muted: #6b7280;
            --border: #e5e7eb;
            --header-bg: #1f2937;
            --header-fg: #ffffff;
            --row-hover: #f3f4f6;
            --select-border: #d1d5db;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            padding: 2rem;
            background: var(--bg);
            color: var(--text);
        }}
        @media (prefers-color-scheme: dark) {{
            .qf-dashboard {{
                --bg: #0f1115;
                --surface: #1a1d23;
                --text: #e5e7eb;
                --text-muted: #9ca3af;
                --border: #2d3138;
                --header-bg: #11151c;
                --header-fg: #e5e7eb;
                --row-hover: #232730;
                --select-border: #3d434d;
            }}
        }}
        .qf-dashboard h1 {{ color: var(--text); margin-top: 0; }}
        .qf-dashboard .meta {{ color: var(--text-muted); margin-bottom: 1.5rem; }}
        .qf-dashboard .overall {{ font-size: 1.2rem; font-weight: bold; color: {overall_color}; }}
        .qf-dashboard table {{ width: 100%; border-collapse: collapse; background: var(--surface); box-shadow: 0 1px 3px rgba(0,0,0,0.25); border-radius: 8px; overflow: hidden; }}
        .qf-dashboard th {{ background: var(--header-bg); color: var(--header-fg); text-align: left; padding: 0.75rem 1rem; }}
        .qf-dashboard td {{ padding: 0.6rem 1rem; border-bottom: 1px solid var(--border); color: var(--text); }}
        .qf-dashboard tr:hover {{ background: var(--row-hover); }}
        .qf-dashboard .filter {{ margin-bottom: 1rem; }}
        .qf-dashboard .filter select {{ padding: 0.4rem; border-radius: 4px; border: 1px solid var(--select-border); background: var(--surface); color: var(--text); }}
    </style>
    <script>
        function filterTable() {{
            const sev = document.getElementById('sevFilter').value;
            const rows = document.querySelectorAll('tbody tr');
            rows.forEach(row => {{
                const cell = row.cells[4].textContent;
                row.style.display = (!sev || cell === sev) ? '' : 'none';
            }});
        }}
    </script>
</head>
<body>
<div class="qf-dashboard">
    <h1>Qualifire Report</h1>
    <div class="meta">
        <div>Run ID: {html.escape(result.run_id)}</div>
        <div>Owner: {html.escape(result.owner)} | BU: {html.escape(result.bu)}</div>
        <div class="overall">Overall: {result.overall_severity.value}</div>
    </div>
    <div class="filter">
        <label>Filter by severity:
            <select id="sevFilter" onchange="filterTable()">
                <option value="">All</option>
                <option value="ERROR">ERROR</option>
                <option value="WARNING">WARNING</option>
                <option value="PASS">PASS</option>
            </select>
        </label>
    </div>
    <table>
        <thead>
            <tr>
                <th>Dataset</th>
                <th>Table</th>
                <th>Validation</th>
                <th>Type</th>
                <th>Severity</th>
                <th>Message</th>
                <th>Value</th>
            </tr>
        </thead>
        <tbody>
            {''.join(rows_html)}
        </tbody>
    </table>
</div>
</body>
</html>"""

    if output_path:
        Path(output_path).write_text(report_html, encoding="utf-8")

    return report_html


def generate_health_html(
    report: Any,
    output_path: str | None = None,
) -> str:
    """Generate a standalone HTML health dashboard from a HealthReport.

    Args:
        report: HealthReport dataclass instance.
        output_path: If provided, write HTML to this file.

    Returns:
        The HTML string.
    """
    # Summary cards
    cards_html = f"""
        <div class="cards">
            <div class="card">
                <div class="card-value">{report.total_checks}</div>
                <div class="card-label">Total Checks</div>
            </div>
            <div class="card" style="border-left: 4px solid #22c55e;">
                <div class="card-value" style="color: #22c55e;">{report.pass_rate}%</div>
                <div class="card-label">Pass Rate</div>
            </div>
            <div class="card" style="border-left: 4px solid #f59e0b;">
                <div class="card-value" style="color: #f59e0b;">{report.warning_rate}%</div>
                <div class="card-label">Warning Rate</div>
            </div>
            <div class="card" style="border-left: 4px solid #ef4444;">
                <div class="card-value" style="color: #ef4444;">{report.error_rate}%</div>
                <div class="card-label">Error Rate</div>
            </div>
        </div>"""

    # Worst offenders table
    offender_rows = ""
    for d in report.worst_offenders:
        offender_rows += f"""
            <tr>
                <td>{html.escape(str(d['dataset']))}</td>
                <td style="color: #ef4444; font-weight: bold;">{d['error']}</td>
                <td style="color: #f59e0b;">{d['warning']}</td>
                <td style="color: #22c55e;">{d['pass']}</td>
                <td>{d['total']}</td>
            </tr>"""

    offenders_html = f"""
        <h2>Worst Offenders</h2>
        <table>
            <thead><tr><th>Dataset</th><th>Errors</th><th>Warnings</th><th>Passes</th><th>Total</th></tr></thead>
            <tbody>{offender_rows if offender_rows else '<tr><td colspan="5">No errors in this period</td></tr>'}</tbody>
        </table>""" if True else ""

    # Check type distribution
    type_rows = ""
    for t in report.by_check_type:
        type_rows += f"""
            <tr>
                <td>{html.escape(str(t['type']))}</td>
                <td>{t['total']}</td>
                <td>{t['pass_rate']}%</td>
                <td>{t['error']}</td>
            </tr>"""

    types_html = f"""
        <h2>Check Type Distribution</h2>
        <table>
            <thead><tr><th>Type</th><th>Total</th><th>Pass Rate</th><th>Errors</th></tr></thead>
            <tbody>{type_rows if type_rows else '<tr><td colspan="4">No data</td></tr>'}</tbody>
        </table>"""

    # Trend — CSS bar chart
    trend_bars = ""
    for t in report.trend:
        bar_pct = t["pass_rate"]
        color = "#22c55e" if bar_pct >= 90 else "#f59e0b" if bar_pct >= 70 else "#ef4444"
        trend_bars += f"""
            <div class="trend-row">
                <span class="trend-date">{html.escape(t['date'])}</span>
                <div class="trend-bar-bg">
                    <div class="trend-bar" style="width: {bar_pct}%; background: {color};"></div>
                </div>
                <span class="trend-pct">{bar_pct}%</span>
            </div>"""

    trend_html = f"""
        <h2>Daily Pass Rate Trend</h2>
        <div class="trend-chart">{trend_bars if trend_bars else '<p>No trend data</p>'}</div>"""

    health_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Qualifire Health Report</title>
    <style>
        .qf-dashboard {{
            --bg: #f9fafb;
            --surface: #ffffff;
            --text: #1f2937;
            --text-strong: #111827;
            --text-muted: #6b7280;
            --border: #e5e7eb;
            --header-bg: #1f2937;
            --header-fg: #ffffff;
            --row-hover: #f3f4f6;
            --bar-track: #e5e7eb;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            padding: 2rem;
            background: var(--bg);
            color: var(--text);
        }}
        @media (prefers-color-scheme: dark) {{
            .qf-dashboard {{
                --bg: #0f1115;
                --surface: #1a1d23;
                --text: #e5e7eb;
                --text-strong: #f3f4f6;
                --text-muted: #9ca3af;
                --border: #2d3138;
                --header-bg: #11151c;
                --header-fg: #e5e7eb;
                --row-hover: #232730;
                --bar-track: #2d3138;
            }}
        }}
        .qf-dashboard h1 {{ color: var(--text-strong); margin-top: 0; }}
        .qf-dashboard h2 {{ color: var(--text); margin-top: 2rem; }}
        .qf-dashboard .meta {{ color: var(--text-muted); margin-bottom: 1.5rem; }}
        .qf-dashboard .cards {{ display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }}
        .qf-dashboard .card {{ background: var(--surface); padding: 1.2rem 1.5rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.25); min-width: 150px; }}
        .qf-dashboard .card-value {{ font-size: 1.8rem; font-weight: bold; color: var(--text-strong); }}
        .qf-dashboard .card-label {{ color: var(--text-muted); font-size: 0.85rem; margin-top: 0.3rem; }}
        .qf-dashboard table {{ width: 100%; border-collapse: collapse; background: var(--surface); box-shadow: 0 1px 3px rgba(0,0,0,0.25); border-radius: 8px; overflow: hidden; margin-bottom: 1.5rem; }}
        .qf-dashboard th {{ background: var(--header-bg); color: var(--header-fg); text-align: left; padding: 0.75rem 1rem; }}
        .qf-dashboard td {{ padding: 0.6rem 1rem; border-bottom: 1px solid var(--border); color: var(--text); }}
        .qf-dashboard tr:hover {{ background: var(--row-hover); }}
        .qf-dashboard .trend-chart {{ background: var(--surface); padding: 1rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.25); }}
        .qf-dashboard .trend-row {{ display: flex; align-items: center; gap: 0.5rem; margin: 0.4rem 0; }}
        .qf-dashboard .trend-date {{ width: 90px; font-size: 0.8rem; color: var(--text-muted); text-align: right; }}
        .qf-dashboard .trend-bar-bg {{ flex: 1; height: 18px; background: var(--bar-track); border-radius: 4px; overflow: hidden; }}
        .qf-dashboard .trend-bar {{ height: 100%; border-radius: 4px; transition: width 0.3s; }}
        .qf-dashboard .trend-pct {{ width: 45px; font-size: 0.8rem; color: var(--text); }}
    </style>
</head>
<body>
<div class="qf-dashboard">
    <h1>Data Quality Health Report</h1>
    <div class="meta">Last {report.days} days &mdash; {report.total_checks} validation checks</div>
    {cards_html}
    {offenders_html}
    {types_html}
    {trend_html}
</div>
</body>
</html>"""

    if output_path:
        Path(output_path).write_text(health_html, encoding="utf-8")

    return health_html


# ---------------------------------------------------------------------------
# Interactive dashboard with dataset/validation drill-down + time range
# ---------------------------------------------------------------------------


def generate_interactive_html(
    storage: Any,
    output_path: str | None = None,
    days: int = 90,
    inline_plotly_js: bool = False,
    as_iframe: bool = False,
    iframe_height: int = 900,
) -> str:
    """Render a single self-contained interactive HTML dashboard.

    Embeds a snapshot of the system table (last ``days`` days) as JSON,
    then renders interactive Plotly charts client-side. Features:

    - **Time range** picker (default 90 days; shrinks the dataset client-side).
    - **Dataset** selector — default "All", drill into one to see its
      validations.
    - **Validation** selector (visible after dataset chosen) — drill into one
      validation's per-partition history with threshold lines and severity
      coloring.
    - **Severity distribution** pie + **pass-rate trend** line, both
      filter-aware.
    - **Dark/light** CSS variables — adapts to OS theme automatically.

    Plotly delivery
    ---------------
    By default the page references Plotly via CDN
    (``cdn.plot.ly/plotly-X.Y.Z.min.js``). That keeps the file small
    (~20–60 KB depending on snapshot size) and works in plain browsers
    or anywhere the CDN is reachable. **VS Code's notebook webview
    sandboxes the inline-HTML output** — it blocks external script
    loads, so the CDN approach renders blank when displayed via
    ``IPython.display.HTML(...)``.

    Pass ``inline_plotly_js=True`` to embed the full Plotly bundle
    inline. The HTML grows by ~3.5 MB but works in sandboxed
    contexts (VS Code notebooks, restricted intranets, air-gapped
    environments). Recommend the default (CDN) for files saved to
    disk + opened in a real browser, and the inline form for inline
    notebook display.

    Notebook embedding (`as_iframe=True`)
    -------------------------------------
    VS Code's notebook webview also strips / sandboxes inline
    ``<script>`` tags inside ``display(HTML(...))`` outputs. Even
    with Plotly bundled inline, the dashboard JS that populates the
    dataset/validation dropdowns and renders charts may not execute,
    leaving empty dropdowns. ``as_iframe=True`` wraps the entire
    document inside an ``<iframe srcdoc="...">``, which gets its own
    document context and runs scripts normally. Recommended for any
    notebook display (``display(HTML(...))``) usage.
    """
    import json

    # Pull a snapshot of recent rows. read_health_data returns the
    # validation-row shape we want; supplement with metric_value for
    # per-validation drill-down history.
    # Pull both validation AND collection rows. Validation rows drive
    # the dataset/validation dropdowns + status counts; collection
    # rows carry the historical per-partition metric values that
    # history-backed validators (drift / forecast / shape / pattern)
    # only validate on the current partition. Without collection
    # rows, picking ``avg_amount_drift.avg_amount`` shows a single
    # today-only point even though every past partition wrote the
    # ``avg_amount`` value as a collection row.
    raw_rows: list[dict[str, Any]] = []
    try:
        raw_rows = list(storage.read_health_data(
            days=days, include_collection=True,
        ))
    except TypeError:
        # Storage backends predating ``include_collection`` — fall back
        # to validation-only.
        raw_rows = list(storage.read_health_data(days=days))
    except Exception:
        pass

    snapshot: list[dict[str, Any]] = []
    for r in raw_rows:
        snapshot.append({
            "run_id": str(r.get("run_id") or ""),
            "run_timestamp": str(r.get("run_timestamp") or ""),
            "partition_ts": str(r.get("partition_ts") or "") or None,
            "dataset_name": str(r.get("dataset_name") or "(unknown)"),
            "table_name": str(r.get("table_name") or "") or None,
            "validation_name": str(r.get("validation_name") or ""),
            "validation_type": str(r.get("validation_type") or ""),
            "validation_status": str(r.get("validation_status") or "PASS").upper(),
            "validation_message": str(r.get("validation_message") or ""),
            # Phase 2 of external-catalog-system-table-hardening:
            # surface the qualifire-internal-failure marker so the
            # dashboard can render persistence-infrastructure outages
            # and validator-execution exceptions distinctly from
            # genuine data-quality findings (icon + chart bucketing).
            "qualifire_internal_failure": _row_is_internal_failure(r),
            "metric_name": str(r.get("metric_name") or "") or None,
            "metric_value": (
                float(r["metric_value"])
                if r.get("metric_value") not in (None, "")
                else None
            ),
            "expected_value": str(r.get("expected_value") or "") or None,
            "actual_value_text": str(r.get("actual_value_text") or "") or None,
            "dataset_description": str(r.get("dataset_description") or "") or None,
            "validation_description": str(r.get("validation_description") or "") or None,
            "dimension_value": str(r.get("dimension_value") or "") or None,
            # 'validation' or 'collection'. JS uses this to filter
            # status counts (validations only) vs history chart
            # (validations + collections joined on metric_name).
            "record_type": str(r.get("record_type") or "validation"),
            # dashboard-rich-detail-panel: project the persisted
            # ``details_json`` into the SNAPSHOT so the click-to-
            # expand renderer registry can dispatch on
            # ``validation_type`` and surface SHAP top features,
            # the value-drift-explainer block, drift's signed
            # deltas, etc. The helper applies path-aware
            # redaction + 8 KB per-row cap and preserves the
            # explainer's parallel-list invariant. ``None`` when
            # the row carries no details / parse failed.
            "details": _snapshot_details(r.get("details_json")),
        })

    # dashboard-rich-detail-panel: enforce the 8 MB total cap.
    # Stage 1 strips details, Stage 2 drops oldest rows entirely
    # using the same stable sort key so the result is
    # deterministic across regenerations.
    snapshot, snapshot_truncated_marker = enforce_total_cap(snapshot)

    # ``_safe_json_for_html_script`` escapes ``</``, U+2028,
    # U+2029, and ``<!--`` so a hostile or buggy ``details``
    # value cannot terminate the surrounding ``<script>`` block.
    snapshot_json = _safe_json_for_html_script(snapshot)
    snapshot_truncated_json = _safe_json_for_html_script(
        snapshot_truncated_marker
    )

    # Plotly delivery — inline bundles the JS into the HTML (works in
    # sandboxed VS Code notebook webviews); CDN keeps the file tiny.
    if inline_plotly_js:
        try:
            from plotly.offline import get_plotlyjs
            plotly_script = f"<script>{get_plotlyjs()}</script>"
        except Exception:
            # plotly Python package isn't installed; fall back to CDN.
            plotly_script = (
                "<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\" "
                "charset=\"utf-8\"></script>"
            )
    else:
        plotly_script = (
            "<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\" "
            "charset=\"utf-8\"></script>"
        )

    html_doc = (
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "<meta charset=\"utf-8\">\n"
        "<title>Qualifire — Interactive Dashboard</title>\n"
        + plotly_script + "\n"
        "<style>\n" + _INTERACTIVE_CSS + "</style>\n"
        "</head>\n<body>\n"
        "<div class=\"qf-dashboard\">\n"
        "<header>\n"
        "  <h1>Qualifire — Interactive Dashboard</h1>\n"
        "  <div class=\"meta\">Snapshot loaded: <span id=\"snapshot-meta\"></span></div>\n"
        "  <div id=\"snapshot-truncation-banner\" class=\"qf-banner\" hidden></div>\n"
        "</header>\n"
        # Single-view layout — controls always visible, dataset and
        # validation default to "All".
        "<section class=\"controls\">\n"
        "  <label>Time range (days)\n"
        f"    <input type=\"number\" id=\"days-input\" value=\"{int(days)}\" min=\"1\" max=\"3650\">\n"
        "  </label>\n"
        "  <label>Dataset\n"
        "    <select id=\"dataset-select\"><option value=\"__all__\">All datasets</option></select>\n"
        "  </label>\n"
        "  <label id=\"validation-label\">Validation\n"
        "    <select id=\"validation-select\"><option value=\"__all__\">All validations</option></select>\n"
        "  </label>\n"
        "  <button id=\"reset-btn\" type=\"button\">Reset</button>\n"
        "</section>\n"
        "<section class=\"context-panel\" id=\"context-panel\" hidden>\n"
        "  <h2 id=\"context-title\"></h2>\n"
        "  <div id=\"context-desc\" class=\"meta\"></div>\n"
        "</section>\n"
        "<section>\n"
        "  <h2 class=\"panel-label\">Status counts</h2>\n"
        "  <div class=\"cards\" id=\"cards\"></div>\n"
        "</section>\n"
        "<section class=\"chart-row\">\n"
        "  <div class=\"chart\">\n"
        "    <h2 class=\"panel-label\">Severity distribution</h2>\n"
        "    <div id=\"severity-pie\" class=\"chart-canvas\"></div>\n"
        "  </div>\n"
        "  <div class=\"chart\">\n"
        "    <h2 class=\"panel-label\">Pass-rate trend</h2>\n"
        "    <div id=\"trend-line\" class=\"chart-canvas\"></div>\n"
        "  </div>\n"
        "</section>\n"
        # Per-validation history is hidden until the user picks a
        # specific validation — an empty placeholder area on first
        # load was reported as confusing.
        "<section class=\"chart-row\" id=\"history-section\" hidden>\n"
        "  <div class=\"chart wide\">\n"
        "    <h2 class=\"panel-label\">Per-partition history</h2>\n"
        "    <div id=\"history-line\" class=\"chart-canvas\"></div>\n"
        "  </div>\n"
        "</section>\n"
        "<section>\n"
        "  <h2 id=\"table-title\" class=\"panel-label\">Datasets</h2>\n"
        # Pagination — hidden by default; ``renderPager`` reveals it
        # for result sets larger than ``PAGE_SIZE`` and updates the
        # status line ("X–Y of N (page A of B)").
        "  <div id=\"table-pager\" class=\"pager\" hidden>\n"
        "    <button id=\"pager-prev\" type=\"button\">&laquo; Prev</button>\n"
        "    <span id=\"pager-status\"></span>\n"
        "    <button id=\"pager-next\" type=\"button\">Next &raquo;</button>\n"
        "  </div>\n"
        "  <table id=\"data-table\">\n"
        "    <thead><tr id=\"data-table-head\"></tr></thead>\n"
        "    <tbody id=\"data-table-body\"></tbody>\n"
        "  </table>\n"
        "</section>\n"
        "</div>\n"  # close .qf-dashboard
        f"<script>\nconst SNAPSHOT = {snapshot_json};\n"
        f"const SNAPSHOT_TRUNCATED = {snapshot_truncated_json};\n"
        f"const DEFAULT_DAYS = {int(days)};\n"
        + _INTERACTIVE_JS +
        "</script>\n"
        "</body>\n</html>\n"
    )

    if output_path:
        Path(output_path).write_text(html_doc, encoding="utf-8")

    if as_iframe:
        # Wrap in an <iframe srcdoc="..."> so VS Code / JupyterLab notebook
        # webviews — which sandbox or strip <script> tags inserted via
        # display(HTML(...)) — execute the dashboard's JS in a fresh
        # document context. Only escape the chars that would break out of
        # the srcdoc attribute value: & and ". HTML-escaping more
        # aggressively (e.g. < and >) corrupts the embedded script.
        #
        # Fixed iframe height with INTERNAL scroll. Earlier versions
        # auto-grew the iframe to its content height to "let the host
        # page scroll past the dashboard," but that made the iframe
        # taller than the notebook viewport (4000px+) and pushed the
        # next cell off-screen. With a fixed height the iframe is a
        # bounded viewport — mouse-wheel inside scrolls the iframe;
        # mouse-wheel outside the iframe scrolls the host notebook
        # naturally. ``iframe_height`` sets the fixed viewport size
        # (default 900px), and the iframe handles its own overflow.
        srcdoc = html_doc.replace("&", "&amp;").replace('"', "&quot;")
        return (
            f'<iframe srcdoc="{srcdoc}" '
            f'style="width:100%;height:{int(iframe_height)}px;'
            f'border:1px solid #d1d5db;border-radius:6px;display:block;" '
            f'sandbox="allow-scripts allow-same-origin">'
            f'</iframe>'
        )
    return html_doc


# CSS scoped to `.qf-dashboard` so embedding the dashboard inline (e.g.
# via display(HTML(...)) in a notebook) does not bleed styles into the
# surrounding page. Each generated dashboard sits inside one container
# div; CSS variables are scoped to that container, and every selector is
# qualified — no `body`, no `:root`, no bare element selectors.
_INTERACTIVE_CSS = """
.qf-dashboard {
    --bg: #f9fafb; --surface: #ffffff;
    --text: #1f2937; --text-strong: #111827; --text-muted: #6b7280;
    --border: #e5e7eb; --header-bg: #1f2937; --header-fg: #ffffff;
    --row-hover: #f3f4f6; --bar-track: #e5e7eb;
    --pass: #22c55e; --warn: #f59e0b; --error: #ef4444;
    --plot-paper: #ffffff; --plot-grid: #e5e7eb; --plot-text: #1f2937;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg); color: var(--text);
}
@media (prefers-color-scheme: dark) {
    .qf-dashboard {
        --bg: #0f1115; --surface: #1a1d23;
        --text: #e5e7eb; --text-strong: #f3f4f6; --text-muted: #9ca3af;
        --border: #2d3138; --header-bg: #11151c; --header-fg: #e5e7eb;
        --row-hover: #232730; --bar-track: #2d3138;
        --plot-paper: #1a1d23; --plot-grid: #2d3138; --plot-text: #e5e7eb;
    }
}
.qf-dashboard, .qf-dashboard * { box-sizing: border-box; }
/* The HTML5 ``hidden`` attribute defaults to ``display:none``, but
   any class rule that sets ``display`` (e.g. ``.chart-row { display:
   flex }``) wins by specificity and the element stays visible. Make
   ``hidden`` win unconditionally for everything inside the dashboard
   so toggling ``section.hidden = true`` from JS actually hides it. */
.qf-dashboard [hidden] { display: none !important; }
.qf-dashboard header { padding: 1.5rem 2rem 0; }
.qf-dashboard h1 { color: var(--text-strong); margin: 0; }
.qf-dashboard h2 { color: var(--text); margin-top: 1.5rem; }
.qf-dashboard .meta { color: var(--text-muted); font-size: 0.9rem; }
.qf-dashboard .controls { display: flex; gap: 1rem; flex-wrap: wrap;
    padding: 1rem 2rem; align-items: end; }
.qf-dashboard .controls label { display: flex; flex-direction: column;
    font-size: 0.8rem; color: var(--text-muted); }
.qf-dashboard .controls input, .qf-dashboard .controls select {
    margin-top: 0.25rem; padding: 0.4rem 0.6rem;
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 4px;
    font-size: 0.95rem;
}
.qf-dashboard .controls button {
    padding: 0.5rem 0.9rem; background: var(--header-bg); color: var(--header-fg);
    border: none; border-radius: 4px; cursor: pointer; font-size: 0.9rem;
}
.qf-dashboard .controls button:hover { opacity: 0.85; }
.qf-dashboard .panel-label {
    margin: 0 0 0.5rem 0; padding: 0 2rem;
    font-size: 0.95rem; font-weight: 600; color: var(--text-strong);
    letter-spacing: 0.02em;
}
.qf-dashboard .chart .panel-label { padding: 0.25rem 0.75rem 0.5rem; }
.qf-dashboard .chart-canvas { min-height: 320px; }
.qf-dashboard .pager {
    display: flex; align-items: center; gap: 0.75rem; padding: 0.25rem 2rem 0.5rem;
    color: var(--text-muted); font-size: 0.85rem;
}
.qf-dashboard .pager button {
    padding: 0.3rem 0.7rem; background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 4px; cursor: pointer;
    font-size: 0.85rem;
}
.qf-dashboard .pager button:disabled { opacity: 0.4; cursor: default; }
.qf-dashboard .pager button:hover:not(:disabled) { background: var(--row-hover); }
.qf-dashboard .pager .edge-hint {
    font-style: italic; color: var(--text-muted); margin-left: 0.5rem;
}
.qf-dashboard .context-panel { padding: 0.5rem 2rem 1rem; }
.qf-dashboard .context-panel h2 { margin: 0; font-size: 1.1rem; }
.qf-dashboard .cards { display: flex; gap: 1rem; padding: 0 2rem 1rem; flex-wrap: wrap; }
.qf-dashboard .card { background: var(--surface); padding: 1rem 1.4rem; border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.25); min-width: 140px;
    border-left: 4px solid var(--border); }
.qf-dashboard .card.pass { border-left-color: var(--pass); }
.qf-dashboard .card.warn { border-left-color: var(--warn); }
.qf-dashboard .card.error { border-left-color: var(--error); }
.qf-dashboard .card-value { font-size: 1.6rem; font-weight: bold; color: var(--text-strong); }
.qf-dashboard .card-label { color: var(--text-muted); font-size: 0.8rem; margin-top: 0.25rem; }
.qf-dashboard .chart-row { display: flex; gap: 1rem; padding: 0 2rem 1rem; flex-wrap: wrap; }
.qf-dashboard .chart { flex: 1; min-width: 360px; min-height: 340px; background: var(--surface);
    border-radius: 8px; padding: 0.5rem; }
.qf-dashboard .chart.wide { flex: 1 1 100%; min-height: 380px; }
.qf-dashboard table { width: calc(100% - 4rem); margin: 0 2rem 2rem; border-collapse: collapse;
    background: var(--surface); border-radius: 8px; overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.25); }
.qf-dashboard th { background: var(--header-bg); color: var(--header-fg); text-align: left;
    padding: 0.6rem 0.9rem; font-size: 0.85rem; }
.qf-dashboard td { padding: 0.55rem 0.9rem; border-bottom: 1px solid var(--border);
    font-size: 0.9rem; color: var(--text); }
.qf-dashboard tr.row-clickable { cursor: pointer; }
.qf-dashboard tr.row-clickable:hover { background: var(--row-hover); }
.qf-dashboard .sev-pass { color: var(--pass); font-weight: 600; }
.qf-dashboard .sev-warning { color: var(--warn); font-weight: 600; }
.qf-dashboard .sev-error { color: var(--error); font-weight: 600; }
/* Phase 2 of external-catalog-system-table-hardening: rows tagged
   qualifire_internal_failure=true render with a wrench icon prefix
   and a muted color so operators can tell at a glance that the
   ERROR is a library / infra failure, not a data-quality finding. */
.qf-dashboard .qf-banner {
    margin: 8px 0;
    padding: 8px 12px;
    border-left: 3px solid var(--warn);
    background: var(--card-bg);
    border-radius: 3px;
    font-size: 0.9em;
}
.qf-dashboard .qf-detail-toggle {
    background: transparent;
    border: 1px solid var(--text-muted);
    color: var(--text);
    cursor: pointer;
    font-size: 0.9em;
    padding: 0 6px;
    border-radius: 3px;
    line-height: 1.4;
}
.qf-dashboard .qf-detail-toggle:hover {
    background: var(--card-bg);
}
.qf-dashboard tr.detail-row td {
    background: var(--card-bg);
    padding: 12px 16px;
    border-top: 0;
    font-size: 0.9em;
}
.qf-dashboard .qf-detail pre {
    margin: 4px 0;
    white-space: pre-wrap;
    word-break: break-word;
    font-size: 0.85em;
}
.qf-dashboard .qf-detail-table {
    width: 100%;
    border-collapse: collapse;
    margin: 6px 0;
}
.qf-dashboard .qf-detail-table th,
.qf-dashboard .qf-detail-table td {
    border-bottom: 1px solid var(--border);
    padding: 4px 8px;
    text-align: left;
    font-size: 0.85em;
}
.qf-dashboard .qf-drift-table {
    border-collapse: collapse;
    margin: 4px 0;
    font-size: 0.8em;
    width: auto;
}
.qf-dashboard .qf-drift-table th,
.qf-dashboard .qf-drift-table td {
    border: 1px solid var(--border);
    padding: 2px 6px;
    text-align: right;
}
.qf-dashboard .qf-drift-table th { text-align: left; font-weight: 500; }
.qf-dashboard .qf-detail-summary {
    color: var(--text-muted);
    font-size: 0.85em;
    margin-bottom: 4px;
}
.qf-dashboard .qf-warn { color: var(--warn); margin: 4px 0; }
.qf-dashboard .qf-muted { color: var(--text-muted); margin: 4px 0; }
.qf-dashboard .qf-detail-header { font-weight: 600; margin-bottom: 4px; }
.qf-dashboard .qf-internal-failure {
    color: var(--text-muted);
    font-style: italic;
}
.qf-dashboard .qf-internal-failure::before { content: "🔧 "; }
"""


_INTERACTIVE_JS = r"""
// Read CSS variables from the dashboard container (not document root)
// so the scoped overrides — including the dark-mode media query — apply
// correctly when the dashboard is embedded inline in a notebook output.
function _qfRoot() {
    return document.querySelector('.qf-dashboard');
}
function cssVar(name) {
    const root = _qfRoot() || document.documentElement;
    return getComputedStyle(root).getPropertyValue(name).trim();
}

function plotlyTheme() {
    return {
        paper_bgcolor: cssVar('--plot-paper'),
        plot_bgcolor: cssVar('--plot-paper'),
        font: { color: cssVar('--plot-text') },
        xaxis: { gridcolor: cssVar('--plot-grid'), zerolinecolor: cssVar('--plot-grid') },
        yaxis: { gridcolor: cssVar('--plot-grid'), zerolinecolor: cssVar('--plot-grid') },
        margin: { l: 50, r: 30, t: 40, b: 40 },
        legend: { orientation: 'h', y: -0.2 },
    };
}

const sevColor = (s) => ({ PASS: cssVar('--pass'), WARNING: cssVar('--warn'),
                            ERROR: cssVar('--error') })[s] || cssVar('--text-muted');

let state = {
    days: DEFAULT_DAYS,
    dataset: '__all__',
    validation: '__all__',
    page: 0,
};
const PAGE_SIZE = 25;
// Severity rank for sorting (ERROR first, then WARNING, then PASS).
// Used by the dataset-selected table view.
const SEV_RANK = { ERROR: 0, WARNING: 1, PASS: 2 };

// dashboard-rich-detail-panel: per-validator-type renderer registry
// + click-to-expand state.
//
// State is kept in a Set keyed by ``rowDetailKey`` so re-renders
// (filter / pager change rebuilds tbody.innerHTML) can reopen any
// row that was open before. Key includes ``dimension_value`` because
// dimensional validators emit multiple rows under the same
// (validation_name, partition_ts, run_timestamp) triple.
const openDetailRows = new Set();
function rowDetailKey(r) {
    // JSON.stringify of a tuple is collision-free regardless of
    // separator characters in any component — operator-supplied
    // dimension_value can be an arbitrary string and any
    // separator-based key (U+001F / | / etc.) is theoretically
    // defeatable. JSON.stringify quotes each element so no embedded
    // character can mimic the separator (codex impl-review R1 HIGH #1).
    return JSON.stringify([
        r.validation_name || '',
        r.partition_ts || '',
        r.run_timestamp || '',
        r.dimension_value || '',
    ]);
}

// Permissive expected_value parser. The Python serializer stores
// ``expected_value`` as ``str(dict)`` — Python's repr uses single
// quotes which JSON.parse rejects. Try strict JSON first, then
// rewrite single → double quotes (good enough for the simple
// {warning, error, min, max} shapes the validators emit).
function parseExpected(raw) {
    if (raw == null || raw === '') return null;
    if (typeof raw !== 'string') return raw;
    try { return JSON.parse(raw); } catch (e) {}
    try {
        const rewritten = raw
            .replace(/'/g, '"')
            .replace(/\bNone\b/g, 'null')
            .replace(/\bTrue\b/g, 'true')
            .replace(/\bFalse\b/g, 'false');
        return JSON.parse(rewritten);
    } catch (e) { return raw; }
}

function renderInternalFailure(row) {
    const err = (row.details && row.details.error) || '(no error message)';
    return '<div class="qf-detail qf-detail-internal">'
        + '<div class="qf-detail-header">Qualifire-internal failure</div>'
        + '<pre>' + escapeHtml(err) + '</pre>'
        + '</div>';
}

function renderGeneric(row) {
    if (row.details == null) return '<div class="qf-detail">No details persisted for this row.</div>';
    if (row.details && row.details._truncated) {
        return '<div class="qf-detail">Details unavailable: '
            + escapeHtml(row.details._reason || 'truncated') + '</div>';
    }
    const pretty = JSON.stringify(row.details, null, 2);
    return '<div class="qf-detail"><pre>' + escapeHtml(pretty) + '</pre></div>';
}

// Renderer for a single value_drift_explainer entry (one feature).
// Per-kind layout pinned by docs/features/dashboard-rich-detail-panel/plan.md
// — returns a small HTML fragment that fits a table cell.
function renderDriftEntry(e) {
    if (!e || typeof e !== 'object') return '<span class="qf-muted">(no drift)</span>';
    const kind = e.kind || 'unknown';
    const cur = (e.current && typeof e.current === 'object') ? e.current : {};
    const past = (e.past && typeof e.past === 'object') ? e.past : {};
    const summary = e.summary
        ? '<div class="qf-detail-summary">' + escapeHtml(String(e.summary)) + '</div>'
        : '';
    if (kind === 'truncated') {
        return '<span class="qf-muted">' + escapeHtml(String(e.summary || 'payload truncated')) + '</span>';
    }
    if (kind === 'unknown') {
        return summary || '<span class="qf-muted">(unknown kind)</span>';
    }
    function fmt(v) {
        if (v == null) return '<span class="qf-muted">n/a</span>';
        if (typeof v === 'number') {
            return escapeHtml(Math.abs(v) < 1 ? v.toFixed(4) : v.toFixed(2));
        }
        return escapeHtml(String(v));
    }
    function row3(label, c, p) {
        return '<tr><th>' + escapeHtml(label) + '</th><td>' + fmt(c)
            + '</td><td>' + fmt(p) + '</td></tr>';
    }
    const rows = [];
    if (kind === 'numeric') {
        rows.push(row3('mean', cur.mean, past.mean));
        rows.push(row3('p50',  cur.p50,  past.p50));
        rows.push(row3('p99',  cur.p99,  past.p99));
        rows.push(row3('null %', cur.null_pct, past.null_pct));
        rows.push(row3('count', cur.count, past.count));
    } else if (kind === 'boolean') {
        rows.push(row3('true_rate', cur.true_rate, past.true_rate));
        rows.push(row3('null %', cur.null_pct, past.null_pct));
        rows.push(row3('count', cur.count, past.count));
    } else if (kind === 'datetime') {
        rows.push(row3('min', cur.min, past.min));
        rows.push(row3('max', cur.max, past.max));
        rows.push(row3('null %', cur.null_pct, past.null_pct));
        rows.push(row3('count', cur.count, past.count));
    } else if (kind === 'onehot') {
        const catLabel = e.is_null_bin
            ? '&lt;null&gt;'
            : 'category=' + escapeHtml(String(e.category != null ? e.category : ''));
        rows.push('<tr><td colspan="3">' + catLabel + '</td></tr>');
        rows.push(row3('rate', cur.rate, past.rate));
        rows.push(row3('count', cur.count, past.count));
    } else if (kind === 'label_encoded') {
        const cTop = Array.isArray(cur.top) ? cur.top : [];
        const pTop = Array.isArray(past.top) ? past.top : [];
        const N = Math.max(cTop.length, pTop.length);
        rows.push('<tr><th>category</th><th>cur rate</th><th>past rate</th></tr>');
        for (let i = 0; i < N; i++) {
            const c = cTop[i] || {}, p = pTop[i] || {};
            const label = c.value != null ? c.value : (p.value != null ? p.value : '');
            rows.push('<tr><td>' + escapeHtml(String(label))
                + '</td><td>' + fmt(c.rate)
                + '</td><td>' + fmt(p.rate) + '</td></tr>');
        }
        rows.push(row3('null %', cur.null_pct, past.null_pct));
        rows.push(row3('count', cur.count, past.count));
    } else {
        return summary || '<span class="qf-muted">(no drift)</span>';
    }
    const head = (kind === 'label_encoded')
        ? ''
        : '<thead><tr><th></th><th>current</th><th>past</th></tr></thead>';
    return summary
        + '<table class="qf-drift-table">' + head + '<tbody>'
        + rows.join('') + '</tbody></table>';
}

function renderShapePattern(row) {
    if (!row.details) return '<div class="qf-detail">No details persisted for this row.</div>';
    if (row.details._truncated) {
        return '<div class="qf-detail">Details unavailable: '
            + escapeHtml(row.details._reason || 'truncated') + '</div>';
    }
    const d = row.details;
    const out = ['<div class="qf-detail">'];
    if (d.auc != null) {
        out.push('<div>AUC: <code>' + escapeHtml(String(d.auc))
            + (d.auc_std != null ? ' &plusmn; ' + escapeHtml(String(d.auc_std)) : '')
            + '</code></div>');
    }
    if (d.anomaly_ratio != null) {
        out.push('<div>Anomaly ratio: <code>' + escapeHtml(String(d.anomaly_ratio)) + '</code></div>');
    }
    if (d.n_current != null || d.n_past != null || d.total_current != null) {
        const samples = [];
        if (d.n_current != null) samples.push('current=' + d.n_current);
        if (d.n_past != null) samples.push('past=' + d.n_past);
        if (d.total_current != null) samples.push('total_current=' + d.total_current);
        out.push('<div>Sample sizes: ' + escapeHtml(samples.join(', ')) + '</div>');
    }
    const feats = Array.isArray(d.top_contributing_features) ? d.top_contributing_features : [];
    const drift = Array.isArray(d.value_drift_explainer) ? d.value_drift_explainer : [];
    if (feats.length === 0 && drift.length === 0) {
        out.push('</div>');
        return out.join('');
    }
    // Length-mismatch warning fires whenever the two parallel lists
    // disagree on length, including the drift-only / feats-only
    // edge cases that can show up in operator hand-edited payloads
    // (codex impl-review R2 LOW).
    if (feats.length !== drift.length) {
        out.push('<div class="qf-warn">value drift list length mismatch (top features='
            + feats.length + ', explainer=' + drift.length + ')</div>');
    }
    // Walk the longer of the two so a drift-only payload (no
    // top_contributing_features) still surfaces its per-feature
    // drift summaries.
    const N = Math.max(feats.length, drift.length);
    if (N > 0) {
        out.push('<table class="qf-detail-table"><thead><tr>'
            + '<th>Feature</th><th>Importance</th><th>Value drift</th>'
            + '</tr></thead><tbody>');
        for (let i = 0; i < N; i++) {
            const f = (i < feats.length && feats[i] && typeof feats[i] === 'object')
                ? feats[i] : {};
            const e = (i < drift.length && drift[i] && typeof drift[i] === 'object')
                ? drift[i] : null;
            // For drift-only rows, fall back to the explainer entry's
            // own ``feature`` field so the row carries an identifier.
            const featName = f.feature || (e && e.feature) || '';
            out.push('<tr><td>' + escapeHtml(String(featName))
                + '</td><td>' + escapeHtml(typeof f.importance === 'number'
                    ? f.importance.toFixed(4) : String(f.importance || ''))
                + '</td><td>' + renderDriftEntry(e)
                + '</td></tr>');
        }
        out.push('</tbody></table>');
    }
    if (d.value_drift_explainer_truncated) {
        out.push('<div class="qf-muted">value drift explainer payload truncated; full set in details_json</div>');
    }
    if (d.value_drift_explainer_error) {
        out.push('<div class="qf-warn">value drift unavailable: ' + escapeHtml(String(d.value_drift_explainer_error)) + '</div>');
    } else if (d.explanation_error) {
        out.push('<div class="qf-warn">explanation unavailable: ' + escapeHtml(String(d.explanation_error)) + '</div>');
    }
    out.push('</div>');
    return out.join('');
}

function renderDrift(row) {
    if (!row.details) return '<div class="qf-detail">No details persisted for this row.</div>';
    const d = row.details;
    const lines = ['<div class="qf-detail">'];
    if (d.current_value != null && d.mean_past != null) {
        lines.push('<div>current: <code>' + escapeHtml(String(d.current_value))
            + '</code> &nbsp; past mean: <code>' + escapeHtml(String(d.mean_past)) + '</code></div>');
    }
    ['deviation_pct', 'deviation_abs', 'z_score',
     'rate_of_change_pct', 'rate_of_change_abs'].forEach(k => {
        if (d[k] != null) {
            lines.push('<div>' + escapeHtml(k) + ': <code>'
                + escapeHtml(String(d[k])) + '</code></div>');
        }
    });
    lines.push('</div>');
    return lines.join('');
}

function renderForecast(row) {
    if (!row.details && row.expected_value == null) {
        return '<div class="qf-detail">No details persisted for this row.</div>';
    }
    const ev = parseExpected(row.expected_value);
    const lines = ['<div class="qf-detail">'];
    if (row.actual_value_text != null && ev && ev.yhat != null) {
        lines.push('<div>observed: <code>' + escapeHtml(String(row.actual_value_text))
            + '</code> &nbsp; predicted: <code>' + escapeHtml(String(ev.yhat)) + '</code></div>');
    }
    if (ev && (ev.yhat_lower != null || ev.warning_lower != null)) {
        const lo = ev.yhat_lower != null ? ev.yhat_lower : ev.warning_lower;
        const hi = ev.yhat_upper != null ? ev.yhat_upper : ev.warning_upper;
        lines.push('<div>prediction band: [<code>' + escapeHtml(String(lo))
            + '</code>, <code>' + escapeHtml(String(hi)) + '</code>]</div>');
    }
    lines.push('</div>');
    return lines.join('');
}

function renderThreshold(row) {
    if (row.details == null && row.expected_value == null
        && row.actual_value_text == null && row.metric_value == null) {
        return '<div class="qf-detail">No details persisted for this row.</div>';
    }
    const ev = parseExpected(row.expected_value);
    const lines = ['<div class="qf-detail">'];
    if (row.actual_value_text != null && row.actual_value_text !== '') {
        lines.push('<div>actual: <code>' + escapeHtml(String(row.actual_value_text)) + '</code></div>');
    } else if (row.metric_value != null) {
        lines.push('<div>actual: <code>' + escapeHtml(String(row.metric_value)) + '</code></div>');
    }
    if (ev && typeof ev === 'object') {
        ['warning', 'error'].forEach(level => {
            const block = ev[level];
            if (block && typeof block === 'object') {
                const parts = [];
                if (block.min != null) parts.push('min=' + block.min);
                if (block.max != null) parts.push('max=' + block.max);
                if (parts.length) {
                    lines.push('<div>' + level + ': <code>'
                        + escapeHtml(parts.join(', ')) + '</code></div>');
                }
            }
        });
    } else if (ev != null) {
        lines.push('<div>threshold: <code>' + escapeHtml(String(ev)) + '</code></div>');
    }
    lines.push('</div>');
    return lines.join('');
}

function renderSLO(row) {
    if (row.details == null && row.expected_value == null) {
        return '<div class="qf-detail">No details persisted for this row.</div>';
    }
    const d = row.details || {};
    const ev = parseExpected(row.expected_value);
    const age = d.age != null ? d.age : d.freshness_age;
    const lines = ['<div class="qf-detail">'];
    if (age != null) {
        lines.push('<div>data age: <code>' + escapeHtml(String(age)) + '</code></div>');
    }
    let target = d.threshold != null ? d.threshold : ev;
    // ev can be a parsed object (e.g. from threshold's str(dict)
    // representation); JSON-stringify dicts so the rendered cell
    // shows the structure rather than ``[object Object]``.
    if (target != null && typeof target === 'object') {
        try { target = JSON.stringify(target); } catch (e) { target = String(target); }
    }
    if (target != null && target !== '') {
        lines.push('<div>threshold: <code>' + escapeHtml(String(target)) + '</code></div>');
    }
    lines.push('</div>');
    return lines.join('');
}

const DETAIL_RENDERERS = {
    shape:     renderShapePattern,
    pattern:   renderShapePattern,
    drift:     renderDrift,
    historical: renderDrift,
    trend:     renderForecast,
    forecast:  renderForecast,
    threshold: renderThreshold,
    slo:       renderSLO,
};

function renderDetailPanel(row) {
    if (isInternalFailure(row)) return renderInternalFailure(row);
    const fn = DETAIL_RENDERERS[row.validation_type];
    if (fn) return fn(row);
    return renderGeneric(row);
}

function wireDetailToggles() {
    document.querySelectorAll('button.qf-detail-toggle').forEach(btn => {
        btn.addEventListener('click', (ev) => {
            ev.stopPropagation();
            const tr = btn.closest('tr');
            if (!tr) return;
            const key = tr.dataset.detailKey;
            if (!key) return;
            const next = tr.nextElementSibling;
            if (next && next.classList.contains('detail-row')
                && next.dataset.detailKey === key) {
                next.remove();
                openDetailRows.delete(key);
                btn.textContent = '▸';
                btn.setAttribute('aria-expanded', 'false');
                return;
            }
            const cols = tr.parentElement.parentElement
                .querySelector('thead tr').children.length;
            const detailTr = document.createElement('tr');
            detailTr.className = 'detail-row';
            detailTr.dataset.detailKey = key;
            // The row's snapshot data is stashed on the parent
            // via dataset.rowIdx so we look up the JSON directly
            // (avoids re-parsing data attributes).
            const rowIdx = parseInt(tr.dataset.rowIdx || '-1', 10);
            const row = (window.__qfDetailRows || [])[rowIdx];
            const cell = document.createElement('td');
            cell.colSpan = cols;
            cell.innerHTML = row ? renderDetailPanel(row)
                : '<div class="qf-detail">Row data unavailable.</div>';
            detailTr.appendChild(cell);
            tr.parentElement.insertBefore(detailTr, tr.nextSibling);
            openDetailRows.add(key);
            btn.textContent = '▾';
            btn.setAttribute('aria-expanded', 'true');
        });
    });
}

function maybeShowSnapshotBanner() {
    const banner = document.getElementById('snapshot-truncation-banner');
    if (!banner) return;
    const t = (typeof SNAPSHOT_TRUNCATED === 'object' && SNAPSHOT_TRUNCATED) ? SNAPSHOT_TRUNCATED : null;
    const anyRowTruncated = SNAPSHOT.some(
        r => r && r.details && (r.details._truncated
            || (Array.isArray(r.details.value_drift_explainer)
                && r.details.value_drift_explainer.some(
                    e => e && e.kind === 'truncated'))));
    if (t || anyRowTruncated) {
        banner.hidden = false;
        banner.textContent = 'Some details were truncated to keep the dashboard fast — see the system table for full payloads.';
    } else {
        banner.hidden = true;
        banner.textContent = '';
    }
}

function withinRange(row) {
    // The "Time range (days)" picker is a *data-domain* filter — the
    // user types it expecting to see the last N partitions, not "rows
    // whose Qualifire run happened in the last N hours". Keying it
    // strictly on ``partition_ts`` keeps backfills, replays, and
    // out-of-order runs all consistent with the user's mental model.
    // Rows without ``partition_ts`` are excluded — operators expect a
    // value here, and surfacing a row with no data-domain timestamp
    // under "last N days" would lie about its provenance.
    if (!row.partition_ts) return false;
    const ts = new Date(row.partition_ts);
    if (isNaN(ts.getTime())) return false;
    const cutoff = new Date(Date.now() - state.days * 86400 * 1000);
    return ts >= cutoff;
}

// Validation-only view of the snapshot. Used everywhere except the
// per-validation history chart (which intentionally reaches into
// collection rows so it can plot historical metric values that
// only have a validation entry on the current partition).
function validationRows() {
    return SNAPSHOT.filter(r => r.record_type === 'validation');
}

function filtered() {
    return validationRows().filter(r => withinRange(r)
        && (state.dataset === '__all__' || r.dataset_name === state.dataset)
        && (state.validation === '__all__' || r.validation_name === state.validation));
}

function uniqueSorted(arr, key) {
    return Array.from(new Set(arr.map(r => r[key]).filter(v => v))).sort();
}

function populateDatasetSelect() {
    // Only validation rows define the dataset menu. Collection rows
    // can carry a dataset_name for ETL-only datasets that have no
    // validations attached; surfacing those in the dropdown would
    // produce empty status panes (no PASS/WARN/ERROR to count).
    const inWindow = validationRows().filter(withinRange);
    const datasets = uniqueSorted(inWindow, 'dataset_name');
    const sel = document.getElementById('dataset-select');
    sel.innerHTML = '<option value="__all__">All datasets</option>'
        + datasets.map(d => `<option value="${d}">${d}</option>`).join('');
    sel.value = state.dataset;
    // Diagnostic — explains the "empty dashboard" case directly in
    // the meta line so operators can tell at a glance whether the
    // system table is empty (no seeding) vs. all rows fall outside
    // the days-window (try increasing the days input).
    const meta = document.getElementById('snapshot-meta');
    if (meta) {
        if (SNAPSHOT.length === 0) {
            meta.textContent = '0 rows in system table — seed history or run a validation first.';
        } else if (inWindow.length === 0) {
            meta.textContent = SNAPSHOT.length
                + ' total rows, but 0 within the last ' + state.days
                + ' days (filtered by partition_ts) — increase the days input '
                + 'or check that partition_ts is being stamped.';
        } else {
            meta.textContent = inWindow.length
                + ' rows over the last ' + state.days
                + ' days (' + datasets.length + ' dataset'
                + (datasets.length === 1 ? '' : 's') + ')';
        }
    }
}

function populateValidationSelect() {
    // Validation dropdown is hidden until the user picks a specific
    // dataset — when dataset === '__all__' there's no per-dataset
    // story to drill into yet, so showing the dropdown is just noise.
    // Cascade: every time the dataset changes, the validations list
    // is rebuilt from the snapshot rows that match the new dataset.
    const valLabel = document.getElementById('validation-label');
    if (state.dataset === '__all__') {
        valLabel.hidden = true;
        state.validation = '__all__';
        const sel = document.getElementById('validation-select');
        sel.innerHTML = '<option value="__all__">All validations</option>';
        return;
    }
    valLabel.hidden = false;
    const inWindow = validationRows().filter(withinRange);
    const pool = inWindow.filter(r => r.dataset_name === state.dataset);
    const validations = uniqueSorted(pool, 'validation_name');
    const sel = document.getElementById('validation-select');
    sel.innerHTML = '<option value="__all__">All validations</option>'
        + validations.map(v => `<option value="${v}">${v}</option>`).join('');
    // If the previously-selected validation no longer exists in the
    // narrowed pool, fall back to "__all__" rather than leaving the
    // dropdown showing a value that maps to nothing.
    if (state.validation !== '__all__' && !validations.includes(state.validation)) {
        state.validation = '__all__';
    }
    sel.value = state.validation;
}

function renderContext() {
    const panel = document.getElementById('context-panel');
    if (state.dataset === '__all__') { panel.hidden = true; return; }
    panel.hidden = false;
    const dsRow = SNAPSHOT.find(r => r.dataset_name === state.dataset && r.dataset_description);
    const valRow = state.validation !== '__all__'
        ? SNAPSHOT.find(r => r.validation_name === state.validation && r.validation_description)
        : null;
    let title = state.dataset;
    if (state.validation !== '__all__') title += ` › ${state.validation}`;
    document.getElementById('context-title').textContent = title;
    const desc = [
        dsRow && dsRow.dataset_description ? `Dataset: ${dsRow.dataset_description}` : '',
        valRow && valRow.validation_description ? `Validation: ${valRow.validation_description}` : '',
    ].filter(Boolean).join(' — ');
    document.getElementById('context-desc').textContent = desc;
}

// Phase 2 of external-catalog-system-table-hardening: rows tagged
// with ``qualifire_internal_failure=true`` are still severity=ERROR
// (so the run raised) but represent qualifire-internal failures —
// persistence-infrastructure outages or validator-execution
// exceptions — not data-quality findings. Charts and counters
// segregate them into a separate "Internal" bucket so operators
// can tell at a glance whether ERROR spikes are real findings
// (paged-on) or library bugs (suppressed). The flag arrives on
// every snapshot row from the Python serializer (line 397+ in
// generate_interactive_html), pre-decoded from details_json.
const isInternalFailure = (r) => Boolean(r.qualifire_internal_failure);

function renderCards() {
    const rows = filtered();
    const total = rows.length;
    const counts = { PASS: 0, WARNING: 0, ERROR: 0, INTERNAL: 0 };
    rows.forEach(r => {
        if (isInternalFailure(r)) {
            counts.INTERNAL += 1;
            // Don't double-count: internal-failure rows should not
            // inflate the data-quality ERROR bucket.
            return;
        }
        counts[r.validation_status] = (counts[r.validation_status] || 0) + 1;
    });
    const pct = (n) => total ? (n * 100 / total).toFixed(1) : '0.0';
    document.getElementById('cards').innerHTML = `
        <div class="card"><div class="card-value">${total}</div><div class="card-label">Validations</div></div>
        <div class="card pass"><div class="card-value">${pct(counts.PASS)}%</div><div class="card-label">Pass</div></div>
        <div class="card warn"><div class="card-value">${pct(counts.WARNING)}%</div><div class="card-label">Warning</div></div>
        <div class="card error"><div class="card-value">${pct(counts.ERROR)}%</div><div class="card-label">Error</div></div>
        <div class="card internal"><div class="card-value">${pct(counts.INTERNAL)}%</div><div class="card-label">Internal</div></div>
    `;
}

function renderSeverityPie() {
    const rows = filtered();
    const counts = { PASS: 0, WARNING: 0, ERROR: 0, INTERNAL: 0 };
    rows.forEach(r => {
        if (isInternalFailure(r)) {
            counts.INTERNAL += 1;
            return;
        }
        counts[r.validation_status] = (counts[r.validation_status] || 0) + 1;
    });
    Plotly.react('severity-pie', [{
        type: 'pie',
        labels: Object.keys(counts),
        values: Object.values(counts),
        marker: { colors: [
            cssVar('--pass'), cssVar('--warn'),
            cssVar('--error'), cssVar('--text-muted'),
        ] },
        hole: 0.45,
    }], { ...plotlyTheme(), title: 'Severity distribution' }, { responsive: true });
}

function renderTrendLine() {
    // Group by ``partition_ts`` only — that's the data-domain
    // timestamp every validator anchors on (drift / forecast / shape
    // lookbacks all key on it). ``run_timestamp`` is when the
    // validation EXECUTED, which would collapse every backfill /
    // replay onto the wall-clock day they ran.
    //
    // Internal-failure rows are excluded from the pass-rate trend
    // — Phase 2 of external-catalog-system-table-hardening — so
    // a storage outage doesn't tank the displayed pass rate when
    // the validations themselves succeeded.
    const rows = filtered().filter(r => !isInternalFailure(r));
    const byDay = {};
    rows.forEach(r => {
        const day = (r.partition_ts || '').slice(0, 10);
        if (!day) return;
        if (!byDay[day]) byDay[day] = { total: 0, pass: 0 };
        byDay[day].total += 1;
        if (r.validation_status === 'PASS') byDay[day].pass += 1;
    });
    const days = Object.keys(byDay).sort();
    const passRate = days.map(d => byDay[d].pass * 100 / byDay[d].total);
    const total = days.map(d => byDay[d].total);
    Plotly.react('trend-line', [
        { x: days, y: passRate, type: 'scatter', mode: 'lines+markers',
          name: 'Pass rate %', line: { color: cssVar('--pass'), width: 2 } },
        { x: days, y: total, type: 'bar', name: 'Validations', yaxis: 'y2',
          marker: { color: cssVar('--text-muted'), opacity: 0.4 } },
    ], { ...plotlyTheme(),
         title: 'Pass rate trend (by partition_ts, excluding internal failures)',
         xaxis: { title: 'partition_ts' },
         yaxis: { title: 'Pass rate %', range: [0, 100] },
         yaxis2: { title: 'Total', overlaying: 'y', side: 'right', showgrid: false },
       }, { responsive: true });
}

function renderHistoryLine() {
    // Hide the entire history-line section unless BOTH a dataset
    // and a validation are picked. Per-validation history makes no
    // sense in the cross-dataset Health view; an empty placeholder
    // area on first load was reported as confusing.
    const section = document.getElementById('history-section');
    const div = document.getElementById('history-line');
    if (state.dataset === '__all__' || state.validation === '__all__') {
        if (section) section.hidden = true;
        Plotly.purge(div);
        div.innerHTML = '';
        return;
    }
    if (section) section.hidden = false;
    Plotly.purge(div);
    div.innerHTML = '';
    // The SQL backend dedupes per natural key — one row per
    // (dataset_name, validation_name, metric_name, partition_ts,
    // dimension_value) — with latest run_timestamp winning and
    // validation > collection on tie. So the snapshot is already
    // canonical; we just filter and merge.
    //
    // For the picked validation, the chart shows two series stacked
    // by partition:
    //   - The picked validation's own row → severity color (the
    //     verdict at that partition).
    //   - For partitions where the picked validation never ran, the
    //     latest collection row for the same metric → gray
    //     ("collected, not validated by this validation").
    //
    // We never draw rows belonging to OTHER validators (different
    // ``validation_name``) — picking a validation in the dropdown
    // is the user's lens, so unrelated validators stay off-screen
    // even when they touched the same metric.
    const datasetMatches = (r) => r.dataset_name === state.dataset;
    const validationRowsForChart = validationRows().filter(r =>
        datasetMatches(r)
        && r.validation_name === state.validation
        && r.metric_value !== null
        && r.partition_ts);
    const metricNames = new Set(
        validationRowsForChart.map(r => r.metric_name).filter(Boolean));

    // Per-partition map of "did the picked validation run here?".
    const partitionKeyOf = (r) => [
        r.partition_ts,
        r.dimension_value || '',
        r.metric_name || '',
    ].join('|');
    const validationByPartition = new Map();
    validationRowsForChart.forEach(r => {
        validationByPartition.set(partitionKeyOf(r), r);
    });

    // Collection rows fill in only the gaps — partitions where the
    // picked validation has no row. (The SQL dedupe already gives
    // one collection row per (dataset, '', metric, partition, dim);
    // the dashboard just picks them up.)
    const collectionRowsForChart = SNAPSHOT.filter(r =>
        r.record_type === 'collection'
        && datasetMatches(r)
        && r.metric_name && metricNames.has(r.metric_name)
        && r.metric_value !== null
        && r.partition_ts
        && !validationByPartition.has(partitionKeyOf(r)));

    const valRows = [...validationRowsForChart, ...collectionRowsForChart];
    if (valRows.length === 0) {
        div.innerHTML = '<div style="padding:1rem;color:var(--text-muted);">'
            + 'No numeric history rows for this validation.</div>';
        return;
    }
    // Sort by partition_ts. Group by
    // dimension_value so each dimension surfaces as its own legend
    // entry; when dataset == '__all__' include the dataset name in
    // the group key so two datasets running the same validation
    // don't merge into a single trace.
    //
    // Pretty-print dimension_value: validators persist it as a JSON
    // object string ('{"region":"us"}') which is unfriendly in a
    // legend. Decode → ``region=us`` (multi-key →
    // ``region=us, tier=prem``). Falls back to the raw string when
    // it's not JSON (older data, custom collectors).
    const formatDimension = (v) => {
        if (!v) return '(no dimension)';
        try {
            const obj = JSON.parse(v);
            if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
                return Object.entries(obj).map(([k, val]) => `${k}=${val}`).join(', ');
            }
        } catch (e) { /* not JSON — fall through */ }
        return String(v);
    };
    const groups = {};
    valRows.forEach(r => {
        const dim = formatDimension(r.dimension_value);
        const groupKey = state.dataset === '__all__'
            ? `${r.dataset_name} · ${dim}`
            : dim;
        groups[groupKey] = groups[groupKey] || [];
        groups[groupKey].push(r);
    });
    // Marker color encodes one of four mutually exclusive states:
    //   1. collected but not validated → ``--text-muted`` (gray)
    //   2. collected + validated PASS → ``--pass`` (green)
    //   3. collected + validated WARNING → ``--warn`` (orange)
    //   4. collected + validated ERROR → ``--error`` (red)
    //
    // After dedupe, "validation" rows ARE both collected AND validated
    // (every threshold validator persists the metric value alongside
    // its result), so categories 2-4 reduce to the validation row's
    // status. Pure "collection" rows are partitions where no
    // validation existed — drift / forecast / shape / pattern only
    // validate the current partition, so past partitions surface as
    // category 1.
    const markerColor = (r) => {
        if (r.record_type === 'collection') return cssVar('--text-muted');
        return sevColor(r.validation_status);
    };
    const markerLabel = (r) => {
        if (r.record_type === 'collection') return 'collected (not validated)';
        return r.validation_status;
    };
    const traces = Object.entries(groups).map(([dim, rows]) => {
        rows.sort((a, b) => a.partition_ts.localeCompare(b.partition_ts));
        return {
            x: rows.map(r => r.partition_ts),
            y: rows.map(r => r.metric_value),
            type: 'scatter', mode: 'lines+markers',
            name: dim,
            text: rows.map(r => r.record_type === 'collection'
                ? markerLabel(r)
                : `${markerLabel(r)}<br>${r.validation_message}`),
            hovertemplate: '%{x}<br>%{y}<br>%{text}<extra></extra>',
            marker: {
                color: rows.map(markerColor),
                size: 8,
            },
        };
    });
    Plotly.react(div, traces, { ...plotlyTheme(),
        title: `${state.validation} — partition history`,
        xaxis: { title: 'partition_ts' },
        yaxis: { title: 'metric_value' },
    }, { responsive: true });
}

function renderTable() {
    const head = document.getElementById('data-table-head');
    const body = document.getElementById('data-table-body');
    const rows = filtered();
    // (dataset='__all__', validation specific) — recent runs across
    // every dataset that has this validation name. Shown before the
    // bare-dataset branch so the validation pick wins over the
    // "Datasets" grouped view.
    if (state.dataset === '__all__' && state.validation !== '__all__') {
        document.getElementById('table-title').textContent =
            `${state.validation} — recent runs (all datasets)`;
        head.innerHTML = '<th>Dataset</th><th>partition_ts</th><th>run_timestamp</th>'
            + '<th>severity</th><th>metric_value</th><th>actual_value_text</th><th>message</th>';
        rows.sort((a, b) => {
            const ptCmp = (b.partition_ts || '').localeCompare(a.partition_ts || '');
            if (ptCmp !== 0) return ptCmp;
            return (b.run_timestamp || '').localeCompare(a.run_timestamp || '');
        });
        const pageRows = paginate(rows);
        body.innerHTML = pageRows.map(r => {
            const sevClass = isInternalFailure(r)
                ? 'qf-internal-failure'
                : `sev-${(r.validation_status||'').toLowerCase()}`;
            const sevLabel = isInternalFailure(r)
                ? `${r.validation_status} (internal)`
                : r.validation_status;
            return `<tr><td>${escapeHtml(r.dataset_name || '')}</td>
                <td>${escapeHtml(r.partition_ts || '')}</td>
                <td>${escapeHtml(r.run_timestamp || '')}</td>
                <td class="${sevClass}">${sevLabel}</td>
                <td>${r.metric_value !== null ? (r.metric_value.toFixed ? r.metric_value.toFixed(4) : r.metric_value) : ''}</td>
                <td>${escapeHtml(r.actual_value_text || '')}</td>
                <td>${escapeHtml((r.validation_message||'').slice(0, 200))}</td></tr>`;
        }).join('');
        renderPager(rows.length);
        return;
    }
    if (state.dataset === '__all__') {
        document.getElementById('table-title').textContent = 'Datasets';
        head.innerHTML = '<th>Dataset</th><th>Description</th><th>Total</th>'
            + '<th>Pass</th><th>Warning</th><th>Error</th><th>Pass rate</th>';
        const grouped = {};
        rows.forEach(r => {
            const k = r.dataset_name;
            grouped[k] = grouped[k] || { total: 0, pass: 0, warning: 0, error: 0,
                                         desc: r.dataset_description };
            grouped[k].total += 1;
            grouped[k][r.validation_status.toLowerCase()] = (grouped[k][r.validation_status.toLowerCase()] || 0) + 1;
        });
        body.innerHTML = Object.entries(grouped).map(([ds, g]) => {
            const pr = g.total ? (g.pass * 100 / g.total).toFixed(1) : '0.0';
            const desc = g.desc ? escapeHtml(g.desc) : '';
            return `<tr class="row-clickable" data-ds="${escapeAttr(ds)}">
                <td>${escapeHtml(ds)}</td><td>${desc}</td>
                <td>${g.total}</td><td class="sev-pass">${g.pass||0}</td>
                <td class="sev-warning">${g.warning||0}</td>
                <td class="sev-error">${g.error||0}</td><td>${pr}%</td></tr>`;
        }).join('');
        body.querySelectorAll('tr.row-clickable').forEach(tr => {
            tr.addEventListener('click', () => {
                document.getElementById('dataset-select').value = tr.dataset.ds;
                onControlChange();
            });
        });
        renderPager(0);  // grouped summary view — no pagination
        return;
    }
    if (state.validation === '__all__') {
        // Dataset-selected view. Per the operator request: flat row
        // list (not grouped by validation), include partition_ts,
        // sort by partition_ts DESC ▸ severity DESC (ERROR ▸ WARN ▸
        // PASS) ▸ validation_name ASC, paginate.
        document.getElementById('table-title').textContent =
            `Validations in ${state.dataset}`;
        head.innerHTML = '<th>partition_ts</th><th>Validation</th><th>Type</th>'
            + '<th>severity</th><th>metric_value</th><th>message</th>';
        rows.sort((a, b) => {
            const ptCmp = (b.partition_ts || '').localeCompare(a.partition_ts || '');
            if (ptCmp !== 0) return ptCmp;
            const sa = SEV_RANK[a.validation_status] ?? 99;
            const sb = SEV_RANK[b.validation_status] ?? 99;
            if (sa !== sb) return sa - sb;
            return (a.validation_name || '').localeCompare(b.validation_name || '');
        });
        const pageRows = paginate(rows);
        body.innerHTML = pageRows.map(r => {
            const fmt = r.metric_value !== null
                ? (r.metric_value.toFixed ? r.metric_value.toFixed(4) : r.metric_value)
                : '';
            const sevClass = isInternalFailure(r)
                ? 'qf-internal-failure'
                : `sev-${(r.validation_status||'').toLowerCase()}`;
            const sevLabel = isInternalFailure(r)
                ? `${r.validation_status} (internal)`
                : r.validation_status;
            return `<tr class="row-clickable" data-val="${escapeAttr(r.validation_name)}">
                <td>${escapeHtml(r.partition_ts || '')}</td>
                <td>${escapeHtml(r.validation_name || '')}</td>
                <td>${escapeHtml(r.validation_type || '')}</td>
                <td class="${sevClass}">${sevLabel}</td>
                <td>${fmt}</td>
                <td>${escapeHtml((r.validation_message||'').slice(0, 200))}</td></tr>`;
        }).join('');
        body.querySelectorAll('tr.row-clickable').forEach(tr => {
            tr.addEventListener('click', () => {
                document.getElementById('validation-select').value = tr.dataset.val;
                onControlChange();
            });
        });
        renderPager(rows.length);
        return;
    }
    document.getElementById('table-title').textContent =
        `${state.validation} — recent runs`;
    // dashboard-rich-detail-panel: this is the validation-selected
    // view — the only entry point for the click-to-expand detail
    // panel. A leading qf-detail-toggle column owns the click;
    // existing tr.row-clickable behavior is preserved on the
    // grouped / dataset-selected views above (they navigate, they
    // don't expand).
    head.innerHTML = '<th></th><th>partition_ts</th><th>run_timestamp</th><th>severity</th>'
        + '<th>metric_value</th><th>actual_value_text</th><th>message</th>';
    rows.sort((a, b) => {
        const ptCmp = (b.partition_ts || '').localeCompare(a.partition_ts || '');
        if (ptCmp !== 0) return ptCmp;
        return (b.run_timestamp || '').localeCompare(a.run_timestamp || '');
    });
    const pageRows = paginate(rows);
    // Stash the visible page rows in a parallel array so the
    // detail-row injection in wireDetailToggles can render
    // directly from the parsed object (no JSON.parse on a
    // data-attribute round-trip).
    window.__qfDetailRows = pageRows;
    body.innerHTML = pageRows.map((r, idx) => {
        const sevClass = isInternalFailure(r)
            ? 'qf-internal-failure'
            : `sev-${(r.validation_status||'').toLowerCase()}`;
        const sevLabel = isInternalFailure(r)
            ? `${r.validation_status} (internal)`
            : r.validation_status;
        const key = rowDetailKey(r);
        const isOpen = openDetailRows.has(key);
        const arrow = isOpen ? '▾' : '▸';
        return `<tr data-row-idx="${idx}" data-detail-key="${escapeAttr(key)}">`
            + `<td><button type="button" class="qf-detail-toggle" aria-expanded="${isOpen}" aria-label="Toggle details">${arrow}</button></td>`
            + `<td>${escapeHtml(r.partition_ts || '')}</td>`
            + `<td>${escapeHtml(r.run_timestamp || '')}</td>`
            + `<td class="${sevClass}">${sevLabel}</td>`
            + `<td>${r.metric_value !== null ? r.metric_value.toFixed ? r.metric_value.toFixed(4) : r.metric_value : ''}</td>`
            + `<td>${escapeHtml(r.actual_value_text || '')}</td>`
            + `<td>${escapeHtml((r.validation_message||'').slice(0, 200))}</td></tr>`;
    }).join('');
    wireDetailToggles();
    // Re-open any rows that were open before the re-render.
    pageRows.forEach((r, idx) => {
        const key = rowDetailKey(r);
        if (!openDetailRows.has(key)) return;
        const tr = body.querySelector(`tr[data-row-idx="${idx}"]`);
        if (!tr) return;
        const next = tr.nextElementSibling;
        if (next && next.classList.contains('detail-row')) return;
        const cols = head.querySelectorAll('th').length;
        const detailTr = document.createElement('tr');
        detailTr.className = 'detail-row';
        detailTr.dataset.detailKey = key;
        const cell = document.createElement('td');
        cell.colSpan = cols;
        cell.innerHTML = renderDetailPanel(r);
        detailTr.appendChild(cell);
        tr.parentElement.insertBefore(detailTr, tr.nextSibling);
    });
    renderPager(rows.length);
}

function paginate(rows) {
    const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
    if (state.page >= totalPages) state.page = totalPages - 1;
    if (state.page < 0) state.page = 0;
    const start = state.page * PAGE_SIZE;
    return rows.slice(start, start + PAGE_SIZE);
}

function renderPager(totalRows) {
    const pager = document.getElementById('table-pager');
    if (!pager) return;
    const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
    if (totalRows <= PAGE_SIZE) {
        pager.hidden = true;
        return;
    }
    pager.hidden = false;
    const start = state.page * PAGE_SIZE + 1;
    const end = Math.min(totalRows, (state.page + 1) * PAGE_SIZE);
    // Status text includes an explicit edge hint so operators see
    // "you're at the first/last page" without having to look at
    // disabled-button styling.
    let edge = '';
    if (state.page === 0 && totalPages > 1) edge = ' — first page';
    else if (state.page >= totalPages - 1) edge = ' — last page';
    document.getElementById('pager-status').innerHTML =
        `${start}–${end} of ${totalRows} (page ${state.page + 1} of ${totalPages})`
        + (edge ? `<span class="edge-hint">${edge}</span>` : '');
    document.getElementById('pager-prev').disabled = state.page === 0;
    document.getElementById('pager-next').disabled = state.page >= totalPages - 1;
}

function escapeHtml(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, c =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function escapeAttr(s) { return escapeHtml(s); }

function onControlChange() {
    state.days = parseInt(document.getElementById('days-input').value || DEFAULT_DAYS, 10) || DEFAULT_DAYS;
    const newDataset = document.getElementById('dataset-select').value;
    const datasetChanged = newDataset !== state.dataset;
    state.dataset = newDataset;
    if (datasetChanged) {
        populateValidationSelect();
    }
    const valSel = document.getElementById('validation-select');
    const newValidation = valSel ? valSel.value : '__all__';
    const validationChanged = newValidation !== state.validation;
    state.validation = newValidation;
    // Reset pagination whenever the underlying filter changes — a
    // page index from a different result set is meaningless on the
    // new one.
    if (datasetChanged || validationChanged) state.page = 0;
    renderAll();
}

function renderAll() {
    renderContext();
    renderCards();
    renderSeverityPie();
    renderTrendLine();
    renderHistoryLine();
    renderTable();
    maybeShowSnapshotBanner();
}

function init() {
    // Default view: "All datasets" + "All validations" — operators
    // see the cross-dataset overview first, then drill in via the
    // dataset / validation dropdowns.
    document.getElementById('snapshot-meta').textContent =
        `${SNAPSHOT.length} rows over the last ${DEFAULT_DAYS} days`;
    populateDatasetSelect();
    populateValidationSelect();
    document.getElementById('days-input').addEventListener('change', () => {
        onControlChange();
        populateDatasetSelect();
    });
    document.getElementById('dataset-select').addEventListener('change', onControlChange);
    document.getElementById('validation-select').addEventListener('change', onControlChange);
    document.getElementById('reset-btn').addEventListener('click', () => {
        state = { days: DEFAULT_DAYS, dataset: '__all__', validation: '__all__',
                  page: 0 };
        document.getElementById('days-input').value = DEFAULT_DAYS;
        populateDatasetSelect();
        populateValidationSelect();
        renderAll();
    });
    // Pager wiring — buttons hidden until ``renderPager`` reveals
    // them on result sets larger than PAGE_SIZE.
    const prev = document.getElementById('pager-prev');
    const next = document.getElementById('pager-next');
    if (prev) prev.addEventListener('click', () => {
        if (state.page > 0) { state.page -= 1; renderTable(); }
    });
    if (next) next.addEventListener('click', () => {
        state.page += 1; renderTable();
    });
    renderAll();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}

// Iframe embedding (``as_iframe=True``) keeps a fixed height and
// lets the iframe handle its own scroll. The contract: mouse-wheel
// inside scrolls the iframe; once the iframe scroll hits its top
// or bottom edge, further wheel motion CHAINS to the host
// notebook page so users don't get stuck at the iframe boundary.
// Browsers don't chain across iframe boundaries by default —
// wheel events get consumed by the iframe regardless of edge —
// so we forward them explicitly.
(function() {
    if (window === window.parent) return;  // not iframed
    window.addEventListener('wheel', (e) => {
        const dy = e.deltaY;
        if (!dy) return;
        const atTop = window.scrollY <= 0;
        const docH = Math.max(
            document.documentElement.scrollHeight,
            document.body.scrollHeight,
        );
        const atBottom = window.scrollY + window.innerHeight >= docH - 1;
        if ((dy < 0 && atTop) || (dy > 0 && atBottom)) {
            try {
                window.parent.scrollBy({ top: dy, behavior: 'auto' });
            } catch (_) { /* sandbox or detached parent */ }
        }
    }, { passive: true });
})();
"""
