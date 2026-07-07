"""
SizeWise — HTML report generator.

Renders the sizing_engine result into two Oracle-branded HTML reports that match
the visual language of the reference AIDP-Capacity-Comprehensive-v20.html:
  - generate_comprehensive(result)  -> detailed technical report (multi-scenario)
  - generate_summary(result)        -> executive summary

Both are self-contained single-file HTML (inline CSS), safe to email or open by
double-click. Multi-scenario: a comparison table up top, full detail for the
recommended (Balanced) scenario, and compact detail for the alternatives.
"""

from __future__ import annotations

from html import escape

# CSS is kept as a plain string (not an f-string) because of the many braces.
_CSS = """
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #1a1a1a; line-height: 1.6; background: #f5f5f5; }
    .container { max-width: 1400px; margin: 0 auto; padding: 30px 20px; background: white; box-shadow: 0 0 30px rgba(0,0,0,0.1); }
    .header { text-align: center; padding: 50px 20px; background: linear-gradient(135deg, #c74634, #a83828); color: white; margin: -30px -20px 50px -20px; border-radius: 0 0 20px 20px; }
    .header h1 { font-size: 3em; margin-bottom: 15px; font-weight: 700; }
    .header .subtitle { font-size: 1.4em; opacity: 0.95; font-weight: 300; margin-bottom: 10px; }
    .header .date { font-size: 1em; opacity: 0.9; margin-top: 20px; }
    .section { margin-bottom: 60px; }
    .section-title { font-size: 2em; color: #c74634; border-bottom: 4px solid #c74634; padding-bottom: 15px; margin-bottom: 30px; font-weight: 600; }
    .subsection-title { font-size: 1.4em; color: #333; margin: 35px 0 20px 0; font-weight: 600; border-left: 5px solid #c74634; padding-left: 15px; }
    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 25px; margin: 30px 0; }
    .stat-card { background: linear-gradient(135deg, #312d2a, #4a4543); color: white; padding: 35px; border-radius: 15px; text-align: center; box-shadow: 0 6px 20px rgba(0,0,0,0.2); transition: transform 0.3s; }
    .stat-card:hover { transform: translateY(-5px); box-shadow: 0 8px 25px rgba(0,0,0,0.3); }
    .stat-card.green { background: linear-gradient(135deg, #27ae60, #229954); }
    .stat-card.blue { background: linear-gradient(135deg, #3498db, #2980b9); }
    .stat-card.orange { background: linear-gradient(135deg, #f39c12, #e67e22); }
    .stat-card.red { background: linear-gradient(135deg, #c74634, #a83828); }
    .stat-card.purple { background: linear-gradient(135deg, #8e44ad, #7d3c98); }
    .stat-value { font-size: 3.2em; font-weight: 700; margin-bottom: 12px; text-shadow: 0 2px 4px rgba(0,0,0,0.2); }
    .stat-label { font-size: 1em; opacity: 0.95; text-transform: uppercase; letter-spacing: 0.8px; font-weight: 500; }
    table { width: 100%; border-collapse: collapse; margin: 25px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-radius: 10px; overflow: hidden; font-size: 0.9em; }
    .table-wrap { overflow-x: auto; overflow-y: hidden; }
    html, body { overflow-x: hidden; }
    thead { background: linear-gradient(135deg, #312d2a, #4a4543); color: white; }
    th { padding: 14px 12px; text-align: left; font-weight: 600; font-size: 0.9em; }
    td { padding: 12px; border-bottom: 1px solid #e5e7eb; }
    tbody tr:nth-child(even) { background: #f9fafb; }
    tbody tr:hover { background: #f3f4f6; }
    .highlight-row { background: #fff3cd !important; font-weight: 600; }
    .rec-col { background: #f0fdf4 !important; }
    .rec-badge { display: inline-block; background: #27ae60; color: white; font-size: 0.7em; padding: 2px 10px; border-radius: 12px; vertical-align: middle; margin-left: 8px; letter-spacing: 0.5px; }
    .info-box { background: #f0f6ff; border-left: 6px solid #3498db; padding: 25px; margin: 25px 0; border-radius: 8px; }
    .info-box.success { background: #f0fdf4; border-left-color: #27ae60; }
    .info-box.warning { background: #fff8f0; border-left-color: #f39c12; }
    .info-box.error { background: #fef2f2; border-left-color: #c74634; }
    .info-box strong { color: #1a1a1a; font-size: 1.05em; }
    .cost-breakdown { background: linear-gradient(135deg, #f0fdf4, #dcfce7); border-radius: 15px; padding: 35px; margin: 35px 0; box-shadow: 0 4px 15px rgba(39, 174, 96, 0.2); }
    .cost-total { text-align: center; padding: 40px; background: white; border-radius: 15px; margin-top: 25px; box-shadow: 0 6px 20px rgba(0,0,0,0.15); }
    .cost-total .amount { font-size: 4em; color: #c74634; font-weight: 700; margin: 15px 0; text-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .cost-total .label { font-size: 1.3em; color: #6b7280; font-weight: 500; }
    code { background: #f4f4f4; padding: 3px 8px; border-radius: 4px; font-family: 'Monaco', 'Courier New', monospace; font-size: 0.9em; color: #c74634; font-weight: 500; }
    .pill { display:inline-block; padding:2px 10px; border-radius: 12px; font-size:0.8em; font-weight:600; }
    .pill.ok { background:#dcfce7; color:#166534; }
    .pill.warn { background:#fef3c7; color:#92400e; }
    .footer { text-align: center; padding: 50px 20px; margin-top: 80px; border-top: 3px solid #e5e7eb; color: #6b7280; }
    @media (max-width: 768px) { .stat-grid { grid-template-columns: 1fr; } }
"""

_SCEN_ORDER = ["conservative", "balanced", "cost_optimized"]


def _money(x):
    return f"${x:,.2f}"


def _money0(x):
    return f"${x:,.0f}"


def _doc(title, body):
    return (f"<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\">\n"
            f"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
            f"<title>{escape(title)}</title>\n<style>{_CSS}</style>\n</head>\n<body>\n"
            f"<div class=\"container\">\n{body}\n</div>\n</body>\n</html>\n")


def _header(result, subtitle):
    cust = escape(result["customer_name"])
    return f"""
<div class="header">
    <h1>AIDP Capacity Planning</h1>
    <div class="subtitle">{cust}</div>
    <div class="subtitle">{subtitle}</div>
    <div class="date">Region: {escape(result['region'])} &nbsp;|&nbsp; Generated: {escape(result['generated_at'])} &nbsp;|&nbsp; Model v{escape(result['config_version'])}</div>
</div>
"""


def _exec_cards(result):
    bal = result["scenarios"]["balanced"]
    d = result["derived"]
    return f"""
    <div class="stat-grid">
        <div class="stat-card green"><div class="stat-value">{result['inputs_echo']['num_users']:,}</div><div class="stat-label">Total Users</div></div>
        <div class="stat-card blue"><div class="stat-value">{d['teams']}</div><div class="stat-label">Teams / Groups</div></div>
        <div class="stat-card purple"><div class="stat-value">{_cluster_total(bal)}</div><div class="stat-label">Clusters (Balanced)</div></div>
        <div class="stat-card orange"><div class="stat-value">{result['inputs_echo']['total_source_data_tb']:,.0f} TB</div><div class="stat-label">Source Data</div></div>
        <div class="stat-card red"><div class="stat-value">{_money0(bal['total_usd_month'])}</div><div class="stat-label">Balanced / Month</div></div>
        <div class="stat-card"><div class="stat-value">{_money0(bal['cost_per_user_month'])}</div><div class="stat-label">Cost / User / Mo</div></div>
    </div>
"""


def _cluster_total(scen):
    return sum(c["count"] for c in scen["clusters"])


def _scenario_comparison(result):
    cols = _SCEN_ORDER
    scc_map = {k: result["scenarios"][k] for k in cols}

    def row(label, fn):
        cells = ""
        for k in cols:
            cls = ' class="rec-col"' if k == "balanced" else ""
            cells += f"<td{cls}>{fn(scc_map[k])}</td>"
        return f"<tr><td><strong>{label}</strong></td>{cells}</tr>"

    headers = ""
    for k in cols:
        badge = ' <span class="rec-badge">RECOMMENDED</span>' if k == "balanced" else ""
        cls = ' class="rec-col"' if k == "balanced" else ""
        headers += f"<th{cls}>{escape(scc_map[k]['scenario_name'])}{badge}</th>"

    body = ""
    body += row("Total Monthly Cost", lambda s: f"<strong>{_money(s['total_usd_month'])}</strong>")
    body += row("Annual Cost", lambda s: _money0(s['total_usd_year']))
    body += row("Cost / User / Month", lambda s: _money(s['cost_per_user_month']))
    body += row("Cost / TB (compute)", lambda s: _money(s['cost_per_tb_month']))
    body += row("Production Clusters", lambda s: f"{s['num_prod_clusters']}")
    body += row("Total Clusters", lambda s: f"{_cluster_total(s)}")
    body += row("Executor Processor", lambda s: escape(s['processor']))
    body += row("Prod Executors (min/max)", lambda s: _prod_exec_range(s))
    body += row("Avg Utilization", lambda s: f"{s['avg_utilization']*100:.0f}%")
    body += row("Prod Hours / Day", lambda s: f"{s['hours_per_day']:.0f}h")
    body += row("Peak OCPU (fleet)", lambda s: f"{s['total_ocpu']:,}")
    body += row("Peak Memory (fleet)", lambda s: f"{s['total_memory_gb']:,} GB")
    body += row("Envelope Check", lambda s: _flag_pill(s['envelope_flags']))

    return f"""
<div class="section">
    <h2 class="section-title">Scenario Comparison</h2>
    <div class="info-box success">
        <strong>Three configurations from the same inputs.</strong>
        <strong>Balanced</strong> is recommended — it meets the stated SLA with ARM economics and autoscale headroom.
        <strong>Conservative</strong> adds high-availability, extra headroom and redundant services.
        <strong>Cost-Optimized</strong> trims utilization, hours and non-prod footprint for the lowest spend.
    </div>
    <div class="table-wrap">
    <table>
        <thead><tr><th>Metric</th>{headers}</tr></thead>
        <tbody>{body}</tbody>
    </table>
    </div>
</div>
"""


def _prod_exec_range(scen):
    for c in scen["clusters"]:
        if c["role"] == "production":
            return f"{c['min_executors']} / {c['max_executors']}"
    return "-"


def _flag_pill(flags):
    if flags == ["OK"]:
        return '<span class="pill ok">OK</span>'
    return " ".join(f'<span class="pill warn">{escape(f)}</span>' for f in flags)


def _workspace_design(result):
    d = result["derived"]
    teams = d["teams"]
    return f"""
<div class="section">
    <h2 class="section-title">1. Workspace &amp; Catalog Design</h2>
    <div class="info-box success">
        <strong>Configuration: 1 Production Workspace</strong><br>
        Supports {teams} team(s) / isolation group(s) and {result['inputs_echo']['num_users']:,} total users,
        with centralized governance across all workloads.
    </div>
    <div class="table-wrap"><table>
        <thead><tr><th>Catalog</th><th>Schemas</th><th>Access Model</th><th>Purpose</th></tr></thead>
        <tbody>
            <tr><td><strong>Shared / Anonymized Catalog</strong></td><td>-</td><td>Read access for all users</td><td>Cross-team shared data</td></tr>
            <tr><td><strong>Sandbox Catalog</strong></td><td>{teams} schema(s), one per group</td><td>Read/write own schema; read shared</td><td>Isolated per-group workspaces</td></tr>
        </tbody>
    </table></div>
</div>
"""


def _cluster_table(scen, detailed=True):
    """Full cluster spec table for a scenario."""
    rows = ""
    for c in scen["clusters"]:
        exec_shape = (f"{c['executor_ocpu']}/{c['executor_memory_gb']}GB"
                      + (f" +{c['gpu_per_executor']}GPU" if c['gpu_per_executor'] else "")
                      if c['max_executors'] or c['gpu_per_executor'] else "-")
        autoscale = (f"{c['min_executors']}-{c['max_executors']}" if c['max_executors'] else "fixed")
        rows += f"""
            <tr>
                <td><strong>{escape(c['name'])}</strong></td>
                <td>{c['count']}</td>
                <td>{escape(c['processor'])}</td>
                <td>{c['driver_ocpu']}/{c['driver_memory_gb']}GB</td>
                <td>{exec_shape}</td>
                <td>{autoscale}</td>
                <td>{c['hours_per_day']:.0f}h</td>
                <td>{_money(c['cluster_usd_month'])}</td>
                <td><strong>{_money(c['role_usd_month'])}</strong></td>
            </tr>"""
    total = scen["compute_usd_month"]
    rows += f"""
            <tr class="highlight-row"><td colspan="8"><strong>TOTAL COMPUTE</strong></td><td><strong>{_money(total)}</strong></td></tr>"""
    return f"""
    <div class="table-wrap"><table>
        <thead><tr>
            <th>Cluster</th><th>Count</th><th>Proc</th><th>Driver<br>OCPU/Mem</th>
            <th>Executor<br>OCPU/Mem</th><th>Autoscale<br>Min-Max</th><th>Uptime</th>
            <th>Each<br>$/mo</th><th>Subtotal<br>$/mo</th>
        </tr></thead>
        <tbody>{rows}</tbody>
    </table></div>
"""


def _rationale_notes(scen):
    items = ""
    for c in scen["clusters"]:
        if c.get("sizing_rationale"):
            items += f"        &bull; <strong>{escape(c['name'])}:</strong> {escape(c['sizing_rationale'])}<br>\n"
    return f'<div class="info-box">{items}</div>' if items else ""


def _cost_breakdown(scen):
    rows = ""
    total = scen["total_usd_month"]
    rows += '<tr><td colspan="4" style="background:#e8f5e9;font-weight:700;color:#27ae60;">COMPUTE CLUSTERS</td></tr>'
    for c in scen["clusters"]:
        pct = (c["role_usd_month"] / total * 100) if total else 0
        rows += (f'<tr><td style="padding-left:30px;">{escape(c["name"])} (x{c["count"]})</td>'
                 f'<td>{escape(c["processor"])}, {c["hours_per_day"]:.0f}h/day</td>'
                 f'<td><strong>{_money(c["role_usd_month"])}</strong></td><td>{pct:.1f}%</td></tr>')
    rows += (f'<tr class="highlight-row"><td colspan="2"><strong>SUBTOTAL — Compute</strong></td>'
             f'<td><strong>{_money(scen["compute_usd_month"])}</strong></td>'
             f'<td><strong>{(scen["compute_usd_month"]/total*100 if total else 0):.1f}%</strong></td></tr>')
    rows += '<tr><td colspan="4" style="background:#fff3e0;font-weight:700;color:#ff9800;">STORAGE &amp; SERVICES</td></tr>'
    for s in scen["services"]:
        pct = (s["usd_month"] / total * 100) if total else 0
        rows += (f'<tr><td style="padding-left:30px;">{escape(s["name"])}</td>'
                 f'<td>{escape(s["detail"])}</td>'
                 f'<td><strong>{_money(s["usd_month"])}</strong></td><td>{pct:.1f}%</td></tr>')
    svc_stor = scen["storage_usd_month"] + scen["services_usd_month"]
    rows += (f'<tr class="highlight-row"><td colspan="2"><strong>SUBTOTAL — Storage &amp; Services</strong></td>'
             f'<td><strong>{_money(svc_stor)}</strong></td>'
             f'<td><strong>{(svc_stor/total*100 if total else 0):.1f}%</strong></td></tr>')
    return f"""
    <div class="cost-breakdown">
        <h3 style="text-align:center;color:#27ae60;margin-bottom:30px;">Monthly Cost Breakdown</h3>
        <div class="table-wrap"><table>
            <thead><tr><th>Component</th><th>Details</th><th>Monthly</th><th>% of Total</th></tr></thead>
            <tbody>{rows}</tbody>
        </table></div>
        <div class="label" style="font-size:0.8em;color:#9ca3af;text-align:right;margin-top:-12px;">Percentages are rounded to 0.1% and may not sum to exactly 100.</div>
        <div class="cost-total">
            <div class="label">TOTAL MONTHLY COST</div>
            <div class="amount">{_money(total)}</div>
            <div class="label" style="margin-top:10px;">Annual: {_money(scen['total_usd_year'])}</div>
            <div class="label" style="margin-top:10px;font-size:0.95em;color:#666;">
                {_money(scen['cost_per_user_month'])}/user/mo &nbsp;|&nbsp; {_money(scen['cost_per_tb_month'])}/TB compute
            </div>
        </div>
    </div>
"""


def _sla_section(result):
    tier = escape(result["inputs_echo"]["sla_tier"])
    return f"""
<div class="section">
    <h2 class="section-title">3. Expected SLA Performance</h2>
    <div class="info-box success"><strong>Selected SLA tier: {tier}.</strong>
        Executor sizing and utilization headroom are derived from this tier; stricter tiers add slots-per-query and lower target utilization to protect latency.</div>
    <div class="table-wrap"><table>
        <thead><tr><th>Query Type</th><th>Expected Duration</th><th>Notes</th></tr></thead>
        <tbody>
            <tr><td><strong>Simple SELECT</strong> (single table, filters)</td><td>1-3 s</td><td>With partition pruning</td></tr>
            <tr><td><strong>Medium</strong> (2-3 JOINs, aggregations)</td><td>5-15 s</td><td>Spark SQL adaptive execution</td></tr>
            <tr><td><strong>High</strong> (3+ JOINs, CTEs)</td><td>30-90 s</td><td>Autoscale to max executors</td></tr>
            <tr><td><strong>Data-intensive</strong> (full scans)</td><td>2-5 min</td><td>Max executors per cluster</td></tr>
            <tr><td><strong>BI dashboards</strong> (JDBC)</td><td>10-30 s</td><td>With result caching</td></tr>
        </tbody>
    </table></div>
    <div class="info-box"><strong>Optimizations:</strong><br>
        &bull; Partition pruning: 10-50x faster filtered queries<br>
        &bull; Result caching: instant repeated queries<br>
        &bull; Autoscale: scales executors with load<br>
        &bull; Dedicated clusters: no cross-team contention</div>
</div>
"""


def _recommended_detail(result):
    bal = result["scenarios"]["balanced"]
    return f"""
<div class="section">
    <h2 class="section-title">2. Recommended Configuration — Balanced</h2>
    <div class="info-box success"><strong>Balanced is the recommended starting point</strong> at {_money(bal['total_usd_month'])}/month
        ({_money(bal['cost_per_user_month'])}/user). {bal['num_prod_clusters']} production cluster(s) plus platform/optional clusters below.</div>
    {_cluster_table(bal)}
    {_rationale_notes(bal)}
</div>
"""


def _alternatives(result):
    out = ['<div class="section"><h2 class="section-title">4. Alternative Scenarios</h2>']
    for key in ("conservative", "cost_optimized"):
        s = result["scenarios"][key]
        when = ("Choose when availability and burst headroom outweigh cost — production-critical, spiky workloads."
                if key == "conservative"
                else "Choose for dev-heavy or cost-sensitive rollouts that can tolerate tighter headroom and shorter hours.")
        out.append(f"""
    <h3 class="subsection-title">{escape(s['scenario_name'])} — {_money(s['total_usd_month'])}/mo ({_money(s['cost_per_user_month'])}/user)</h3>
    <div class="info-box">{when} &nbsp; Envelope: {_flag_pill(s['envelope_flags'])}</div>
    {_cluster_table(s)}
""")
    out.append("</div>")
    return "".join(out)


def _cost_section(result):
    bal = result["scenarios"]["balanced"]
    return f"""
<div class="section">
    <h2 class="section-title">5. Cost Analysis — Balanced</h2>
    {_cost_breakdown(bal)}
    <h3 class="subsection-title">Cost Optimization Levers</h3>
    <div class="table-wrap"><table>
        <thead><tr><th>Lever</th><th>Effect</th><th>Status in Balanced</th></tr></thead>
        <tbody>
            <tr><td><strong>ARM processors</strong></td><td>~70% vs Intel (27 vs 87 AIDPU/OCPU)</td><td style="color:#27ae60;">Applied</td></tr>
            <tr><td><strong>Business-hours operation</strong></td><td>~50% vs 24/7 for prod clusters</td><td style="color:#27ae60;">Applied ({bal['hours_per_day']:.0f}h/day)</td></tr>
            <tr><td><strong>Autoscale headroom</strong></td><td>Pay avg, not peak</td><td style="color:#27ae60;">Applied ({bal['avg_utilization']*100:.0f}% util)</td></tr>
            <tr><td><strong>Partition enforcement</strong></td><td>10-50x faster filtered scans</td><td style="color:#c74634;">Recommended</td></tr>
        </tbody>
    </table></div>
</div>
"""


def _methodology(result):
    d = result["derived"]
    cal = result["calibration"]
    pr = result["pricing_rates"]
    ie = result["inputs_echo"]
    # inputs echo (key ones)
    inp_rows = ""
    key_inputs = [
        ("Total source data", f"{ie['total_source_data_tb']:,.0f} TB"),
        ("Data sources", ie["num_data_sources"]),
        ("Total users", f"{ie['num_users']:,}"),
        ("Peak concurrent users", d["concurrent"]),
        ("SLA tier", ie["sla_tier"]),
        ("Monthly growth", f"{d['monthly_growth']:,.1f} TB/mo"),
        ("Daily ingest", f"{d['daily_ingest']:,.0f} GB/day"),
        ("Workload mix (int/etl/ml)", f"{d['mix']['interactive']*100:.0f}/{d['mix']['etl_batch']*100:.0f}/{d['mix']['ml_gpu']*100:.0f}"),
        ("Teams / isolation", f"{d['teams']} ({escape(ie['isolation_model'])})"),
        ("Business hours/day", f"{ie['business_hours_per_day']:.0f}h"),
        ("Processor preference", ie["processor_preference"]),
        ("HA / Dev / Storage", f"{ie['need_ha']} / {ie['need_dev_env']} / {ie['include_storage_cost']}"),
        ("Billing model", ie["billing_model"]),
    ]
    for k, v in key_inputs:
        inp_rows += f"<tr><td>{escape(str(k))}</td><td>{escape(str(v))}</td></tr>"

    byol = ("<br>&bull; <strong>BYOL:</strong> Bring-Your-Own-License does not change AIDPU compute math; it may reduce the effective credit rate under your Oracle agreement."
            if ie["billing_model"] == "byol" else "")

    warn_html = ""
    for w in d.get("input_warnings", []):
        warn_html += f'<div class="info-box warning"><strong>Input note:</strong> {escape(w)}</div>'

    return f"""
<div class="section">
    <h2 class="section-title">6. Inputs, Methodology &amp; Assumptions</h2>
    {warn_html}
    <h3 class="subsection-title">Inputs Used</h3>
    <div class="table-wrap"><table><thead><tr><th>Input</th><th>Value (resolved)</th></tr></thead><tbody>{inp_rows}</tbody></table></div>

    <h3 class="subsection-title">Pricing Basis</h3>
    <div class="info-box"><strong>AIDP Unit (AIDPU) rates, $1 AIDPU = ${pr['aidpu_usd']}:</strong><br>
        &bull; ARM {pr['ARM']} &nbsp; AMD {pr['AMD']} &nbsp; Intel {pr['Intel']} AIDPU/OCPU/hr &nbsp; | &nbsp; Memory {pr['memory']} AIDPU/GB/hr &nbsp; | &nbsp; GPU {pr['GPU']} AIDPU/GPU/hr<br>
        &bull; Object Storage ${pr['object_storage']}/GB-month (hot) &nbsp; | &nbsp; Default Master Catalog billed floor {pr['platform_min_aidpu']:.0f} AIDPU/hr<br>
        &bull; Monthly = hourly x hours/day x 30. Autoscale avg executors = min + (max-min) x utilization.{byol}
    </div>

    <h3 class="subsection-title">Calibration</h3>
    <div class="info-box success"><strong>Model validated against the Entel v20 reference.</strong><br>
        Balanced core (DMC + production + fixed services) = {_money(cal['balanced_core_monthly'])}/mo vs the
        {_money(cal['anchor_monthly'])}/mo reference &rarr; <strong>{cal['delta_pct']:+.2f}%</strong>.
        {escape(cal['note'])}</div>

    <div class="info-box warning"><strong>Assumptions &amp; caveats:</strong><br>
        &bull; 30-day months; annual = monthly x 12.<br>
        &bull; Storage is an avg-over-retention estimate; toggle it off to compare against compute-only figures.<br>
        &bull; Concurrency sizing uses Little's Law with default query rate/runtime — override for concurrency-bound workloads.<br>
        &bull; Additional-service costs (ATP, networking, egress) are defensible placeholders; refine per region/workload.<br>
        &bull; This is a planning estimate, not a quote — validate against a pilot and your Oracle pricing.</div>
</div>
"""


def _recommendations():
    return """
<div class="section">
    <h2 class="section-title">7. Recommendations &amp; Next Steps</h2>
    <div class="table-wrap"><table>
        <thead><tr><th>Recommendation</th><th>Priority</th><th>Rationale</th></tr></thead>
        <tbody>
            <tr><td><strong>Partition strategy</strong></td><td style="color:#c74634;">Critical</td><td>10-50x performance on filtered queries; do before migration</td></tr>
            <tr><td><strong>Pilot with a subset</strong></td><td style="color:#f39c12;">High</td><td>Validate the Balanced sizing before full rollout</td></tr>
            <tr><td><strong>Per-group monitoring</strong></td><td style="color:#f39c12;">High</td><td>Track usage/cost; right-size utilization and hours from real data</td></tr>
            <tr><td><strong>Convert to Parquet + compression</strong></td><td style="color:#3498db;">Medium</td><td>Reduce storage and scan volume</td></tr>
        </tbody>
    </table></div>
    <div class="info-box warning"><strong>Recommended path:</strong> start on the <strong>Balanced</strong> configuration,
        monitor 2-4 weeks, then move toward Cost-Optimized levers (utilization, hours) where SLAs allow, or Conservative elements (HA) where risk warrants.</div>
</div>
"""


def _footer(result, kind):
    return f"""
<div class="footer">
    <p><strong>AIDP Capacity Planning — {kind}</strong></p>
    <p>{escape(result['customer_name'])} &bull; Oracle AI Data Platform</p>
    <p style="margin-top:15px;font-size:0.9em;">Generated by SizeWise (model v{escape(result['config_version'])}) &bull; {escape(result['generated_at'])}<br>
    Estimates use Oracle AIDP Unit pricing. Planning aid — validate against a pilot and your Oracle agreement.</p>
</div>
"""


def generate_comprehensive(result) -> str:
    bal = result["scenarios"]["balanced"]
    subtitle = (f"Multi-Scenario Capacity Plan &bull; Balanced {_money0(bal['total_usd_month'])}/mo")
    body = (
        _header(result, subtitle)
        + '<div class="section"><h2 class="section-title">Executive Summary</h2>'
        + _exec_cards(result)
        + f'<div class="info-box success"><strong>Recommended (Balanced):</strong> '
          f'{_cluster_total(bal)} clusters, {bal["processor"]} executors, {_money(bal["total_usd_month"])}/month '
          f'({_money(bal["total_usd_year"])}/year). See the scenario comparison for Conservative and Cost-Optimized alternatives.</div></div>'
        + _scenario_comparison(result)
        + _workspace_design(result)
        + _recommended_detail(result)
        + _sla_section(result)
        + _alternatives(result)
        + _cost_section(result)
        + _methodology(result)
        + _recommendations()
        + _footer(result, "Comprehensive Report")
    )
    return _doc(f"AIDP Capacity Planning — {result['customer_name']}", body)


def generate_summary(result) -> str:
    bal = result["scenarios"]["balanced"]
    subtitle = f"Executive Summary &bull; Balanced {_money0(bal['total_usd_month'])}/mo"
    body = (
        _header(result, subtitle)
        + '<div class="section"><h2 class="section-title">Executive Overview</h2>'
        + _exec_cards(result)
        + f'<div class="info-box success"><strong>Recommendation:</strong> deploy on the <strong>Balanced</strong> '
          f'configuration — {_cluster_total(bal)} clusters, {_money(bal["total_usd_month"])}/month, '
          f'{_money(bal["cost_per_user_month"])}/user. Meets the {escape(result["inputs_echo"]["sla_tier"])} SLA tier with ARM economics.</div></div>'
        + _scenario_comparison(result)
        + '<div class="section"><h2 class="section-title">Cost Summary — Balanced</h2>'
        + _cost_breakdown(bal) + '</div>'
        + _footer(result, "Executive Summary")
    )
    return _doc(f"AIDP Summary — {result['customer_name']}", body)
