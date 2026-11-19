"""Generate AIDP notebooks that render migration analysis as interactive dashboards.

Produces Databricks-style Python notebooks with cell separators that display
charts, tables, and visual reports using matplotlib and HTML.
"""

from datetime import datetime

CELL_SEP = "# COMMAND ----------"


def _md(text: str) -> str:
    """Wrap text as a MAGIC %md cell."""
    lines = text.strip().split("\n")
    return "\n".join(f"# MAGIC {line}" for line in lines)


class DashboardGenerator:
    """Generate AIDP notebook that renders migration analysis as interactive dashboard."""

    def generate_analysis_dashboard(self, report_json_path: str, output_path: str) -> str:
        """Generate an AIDP notebook that reads the analysis JSON and renders a dashboard.

        The notebook reads the analysis_report.json file and displays:
        1. Executive Summary cards (total mappings, conversion rate, effort)
        2. Complexity distribution pie chart
        3. Transformation type bar chart
        4. Migration risk heatmap
        5. Dependency graph visualization
        6. Subject-area / mapping-folder breakdown
        7. Conversion confidence breakdown
        8. Timeline/effort estimation table
        """
        cells: list[str] = [
            self._title_cell(),
            self._setup_cell(report_json_path),
            _md("## Executive Summary"),
            self._executive_summary_code(),
            _md("## Complexity Distribution"),
            self._complexity_pie_code(),
            _md("## Transformation Type Distribution"),
            self._transformation_bar_code(),
            _md("## Migration Risk Assessment"),
            self._risk_table_code(),
            _md("## Subject Area Breakdown"),
            self._subject_area_code(),
            _md("## Conversion Confidence Breakdown"),
            self._confidence_bar_code(),
            _md("## Effort Estimation"),
            self._effort_table_code(),
            _md("## Dependency Graph"),
            self._dependency_graph_code(),
            _md("## Compatibility Issues"),
            self._compatibility_issues_code(),
            _md("## Recommendations"),
            self._recommendations_code(),
        ]

        notebook = self._assemble(cells)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(notebook)
        return notebook

    def generate_reconcile_dashboard(self, reconcile_report_path: str, output_path: str) -> str:
        """Generate notebook dashboard for reconciliation results.

        Shows: pass/fail per table, row count diffs, schema mismatches, sample data diffs.
        """
        cells: list[str] = [
            self._reconcile_title_cell(),
            self._reconcile_setup_cell(reconcile_report_path),
            _md("## Reconciliation Summary"),
            self._reconcile_summary_code(),
            _md("## Row Count Comparison"),
            self._reconcile_row_counts_code(),
            _md("## Schema Mismatches"),
            self._reconcile_schema_diff_code(),
            _md("## Sample Data Differences"),
            self._reconcile_data_diff_code(),
            _md("## Verdict"),
            self._reconcile_verdict_code(),
        ]

        notebook = self._assemble(cells)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(notebook)
        return notebook

    # ------------------------------------------------------------------
    # Analysis dashboard cells
    # ------------------------------------------------------------------

    @staticmethod
    def _title_cell() -> str:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        return _md(
            "# Informatica to AIDP Migration Dashboard\n"
            "Auto-generated analysis of Informatica PowerCenter export\n"
            "\n"
            f"_Generated: {ts}_"
        )

    @staticmethod
    def _setup_cell(report_json_path: str) -> str:
        return (
            "import json\n"
            "import matplotlib\n"
            "matplotlib.use('Agg')\n"
            "import matplotlib.pyplot as plt\n"
            "from collections import Counter\n"
            "\n"
            "def _display_html(html):\n"
            '    """Render HTML via displayHTML on AIDP/Databricks, falls back to print."""\n'
            "    try:\n"
            "        displayHTML(html)\n"
            "    except NameError:\n"
            "        print(html)\n"
            "\n"
            "def _show_fig(fig):\n"
            '    """Display matplotlib figure via display() on AIDP, falls back to plt.show."""\n'
            "    try:\n"
            "        display(fig)\n"
            "    except Exception:\n"
            "        plt.show()\n"
            "    plt.close(fig)\n"
            "\n"
            # repr() so a Windows path (backslashes) or a quote in the path
            # cannot turn the generated cell into a SyntaxError.
            f'with open({report_json_path!r}, encoding="utf-8") as f:\n'
            "    report = json.load(f)\n"
            "\n"
            'inventory = report.get("inventory", {})\n'
            'assessments = report.get("complexity_assessments", [])\n'
            'dependencies = report.get("dependencies", {})\n'
            'issues = report.get("compatibility_issues", [])\n'
            'summary = report.get("summary", {})'
        )

    @staticmethod
    def _executive_summary_code() -> str:
        return """\
total_mappings = inventory.get('total_mappings', 0)
total_transforms = inventory.get('total_transformations', 0)
total_sources = inventory.get('total_sources', 0)
total_targets = inventory.get('total_targets', 0)

high_conf = sum(1 for a in assessments if a.get('migration_risk') == 'LOW')
auto_pct = round(high_conf / max(total_mappings, 1) * 100, 1)

total_effort = sum(a.get('estimated_effort_hours', 0) for a in assessments)

risk_counts = Counter(a.get('migration_risk', 'LOW') for a in assessments)
if risk_counts.get('CRITICAL', 0) > 0 or risk_counts.get('HIGH', 0) > total_mappings * 0.3:
    overall_risk = 'HIGH'
elif risk_counts.get('HIGH', 0) > 0 or risk_counts.get('MEDIUM', 0) > total_mappings * 0.5:
    overall_risk = 'MEDIUM'
else:
    overall_risk = 'LOW'

risk_color = {'LOW': '#2ecc71', 'MEDIUM': '#f39c12', 'HIGH': '#e74c3c'}.get(overall_risk, '#95a5a6')

html = f'''
<div style="display:flex; gap:24px; flex-wrap:wrap; margin:16px 0;">
  <div style="background:#f8f9fa; border-left:4px solid #3498db; padding:16px 24px; min-width:160px;">
    <div style="font-size:36px; font-weight:bold; color:#2c3e50;">{total_mappings}</div>
    <div style="color:#7f8c8d;">Total Mappings</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid #2ecc71; padding:16px 24px; min-width:160px;">
    <div style="font-size:36px; font-weight:bold; color:#27ae60;">{auto_pct}%</div>
    <div style="color:#7f8c8d;">Auto-Convertible</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid #9b59b6; padding:16px 24px; min-width:160px;">
    <div style="font-size:36px; font-weight:bold; color:#8e44ad;">{total_effort:.0f}h</div>
    <div style="color:#7f8c8d;">Estimated Effort</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid {risk_color}; padding:16px 24px; min-width:160px;">
    <div style="font-size:36px; font-weight:bold; color:{risk_color};">{overall_risk}</div>
    <div style="color:#7f8c8d;">Risk Level</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid #e67e22; padding:16px 24px; min-width:160px;">
    <div style="font-size:36px; font-weight:bold; color:#d35400;">{total_transforms}</div>
    <div style="color:#7f8c8d;">Transformations</div>
  </div>
</div>
'''
_display_html(html)"""

    @staticmethod
    def _complexity_pie_code() -> str:
        return """\
levels = [a.get('complexity_level', 'SIMPLE') for a in assessments]
level_counts = Counter(levels)

ordered = ['SIMPLE', 'MEDIUM', 'COMPLEX', 'VERY_COMPLEX']
labels = ['Simple', 'Medium', 'Complex', 'Very Complex']
sizes = [level_counts.get(k, 0) for k in ordered]
colors = ['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c']

filtered = [(l, s, c) for l, s, c in zip(labels, sizes, colors) if s > 0]
if filtered:
    f_labels, f_sizes, f_colors = zip(*filtered)
else:
    f_labels, f_sizes, f_colors = ['No Data'], [1], ['#bdc3c7']

fig, ax = plt.subplots(1, 1, figsize=(8, 6))
ax.pie(f_sizes, labels=f_labels, colors=f_colors, autopct='%1.1f%%', startangle=90,
       textprops={'fontsize': 12})
ax.set_title('Mapping Complexity Distribution', fontsize=14, fontweight='bold')
plt.tight_layout()
_show_fig(fig)"""

    @staticmethod
    def _transformation_bar_code() -> str:
        return """\
type_counts = inventory.get('transformation_type_counts', {})

if type_counts:
    sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
    names = [t[0] for t in sorted_types]
    counts = [t[1] for t in sorted_types]

    fig, ax = plt.subplots(figsize=(10, max(4, len(names) * 0.4)))
    bars = ax.barh(range(len(names)), counts, color='#3498db', edgecolor='white')
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=10)
    ax.set_xlabel('Count', fontsize=12)
    ax.set_title('Transformation Types', fontsize=14, fontweight='bold')
    ax.invert_yaxis()

    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                str(count), va='center', fontsize=10)

    plt.tight_layout()
    _show_fig(fig)
else:
    print('No transformation type data available.')"""

    @staticmethod
    def _risk_table_code() -> str:
        return """\
risk_colors = {
    'LOW': '#d5f5e3',
    'MEDIUM': '#fdebd0',
    'HIGH': '#fadbd8',
    'CRITICAL': '#f1948a',
}

rows_html = ''
for a in sorted(assessments, key=lambda x: {'CRITICAL':0,'HIGH':1,'MEDIUM':2,'LOW':3}.get(x.get('migration_risk','LOW'), 4)):
    risk = a.get('migration_risk', 'LOW')
    bg = risk_colors.get(risk, '#ffffff')
    issue_list = ', '.join(a.get('issues', [])) or 'None'
    rows_html += f'''
    <tr style="background:{bg};">
      <td style="padding:8px; border:1px solid #ddd;">{a.get('mapping_name','')}</td>
      <td style="padding:8px; border:1px solid #ddd;">{a.get('complexity_level','')}</td>
      <td style="padding:8px; border:1px solid #ddd; font-weight:bold;">{risk}</td>
      <td style="padding:8px; border:1px solid #ddd;">{a.get('num_transformations',0)}</td>
      <td style="padding:8px; border:1px solid #ddd;">{a.get('estimated_effort_hours',0):.1f}h</td>
      <td style="padding:8px; border:1px solid #ddd; font-size:0.9em;">{issue_list}</td>
    </tr>'''

html = f'''
<table style="border-collapse:collapse; width:100%; font-family:sans-serif;">
  <thead>
    <tr style="background:#2c3e50; color:white;">
      <th style="padding:10px; border:1px solid #ddd;">Mapping</th>
      <th style="padding:10px; border:1px solid #ddd;">Complexity</th>
      <th style="padding:10px; border:1px solid #ddd;">Risk</th>
      <th style="padding:10px; border:1px solid #ddd;">Transforms</th>
      <th style="padding:10px; border:1px solid #ddd;">Effort</th>
      <th style="padding:10px; border:1px solid #ddd;">Issues</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
'''
_display_html(html)"""

    @staticmethod
    def _subject_area_code() -> str:
        return """\
subject_areas = dependencies.get('subject_area_groups', {})

if subject_areas:
    rows_html = ''
    for area, mappings in sorted(subject_areas.items()):
        names = ', '.join(mappings[:5])
        if len(mappings) > 5:
            names += f' (+{len(mappings) - 5} more)'
        rows_html += f'''
        <tr>
          <td style="padding:8px; border:1px solid #ddd;">{area}</td>
          <td style="padding:8px; border:1px solid #ddd;">{names}</td>
          <td style="padding:8px; border:1px solid #ddd; text-align:right;">{len(mappings)}</td>
        </tr>'''

    html = f'''
    <table style="border-collapse:collapse; width:100%; font-family:sans-serif;">
      <thead>
        <tr style="background:#2c3e50; color:white;">
          <th style="padding:10px; border:1px solid #ddd;">Subject Area</th>
          <th style="padding:10px; border:1px solid #ddd;">Mappings</th>
          <th style="padding:10px; border:1px solid #ddd;">Count</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    '''
    _display_html(html)
else:
    print('No subject area / folder grouping available for this export.')"""

    @staticmethod
    def _confidence_bar_code() -> str:
        return """\
if assessments:
    mapping_names = [a.get('mapping_name', f'M{i}') for i, a in enumerate(assessments)]

    high_pcts, med_pcts, low_pcts, manual_pcts = [], [], [], []
    for a in assessments:
        risk = a.get('migration_risk', 'LOW')
        has_sp = a.get('has_stored_procedures', False)
        has_custom = a.get('has_custom_transformations', False)
        has_sql = a.get('has_sql_overrides', False)

        manual_frac = sum([has_sp, has_custom, has_sql]) / 3.0
        if risk == 'LOW':
            h, m, l = 0.8, 0.15, 0.05
        elif risk == 'MEDIUM':
            h, m, l = 0.4, 0.4, 0.2
        elif risk == 'HIGH':
            h, m, l = 0.1, 0.3, 0.6
        else:
            h, m, l = 0.0, 0.1, 0.9
        man = manual_frac * 0.3
        factor = 1 - man
        high_pcts.append(h * factor * 100)
        med_pcts.append(m * factor * 100)
        low_pcts.append(l * factor * 100)
        manual_pcts.append(man * 100)

    fig, ax = plt.subplots(figsize=(max(8, len(mapping_names) * 0.8), 6))
    x = range(len(mapping_names))

    ax.bar(x, high_pcts, label='HIGH', color='#2ecc71')
    ax.bar(x, med_pcts, bottom=high_pcts, label='MEDIUM', color='#f1c40f')
    bottom2 = [h + m for h, m in zip(high_pcts, med_pcts)]
    ax.bar(x, low_pcts, bottom=bottom2, label='LOW', color='#e67e22')
    bottom3 = [b + l for b, l in zip(bottom2, low_pcts)]
    ax.bar(x, manual_pcts, bottom=bottom3, label='MANUAL', color='#e74c3c')

    ax.set_xticks(x)
    short_names = [n[:20] + '...' if len(n) > 20 else n for n in mapping_names]
    ax.set_xticklabels(short_names, rotation=45, ha='right', fontsize=9)
    ax.set_ylabel('Confidence %')
    ax.set_title('Conversion Confidence per Mapping', fontsize=14, fontweight='bold')
    ax.legend(loc='upper right')
    ax.set_ylim(0, 105)
    plt.tight_layout()
    _show_fig(fig)
else:
    print('No mapping assessments available.')"""

    @staticmethod
    def _effort_table_code() -> str:
        return """\
rows_html = ''
total_effort = 0
for a in sorted(assessments, key=lambda x: x.get('estimated_effort_hours', 0), reverse=True):
    effort = a.get('estimated_effort_hours', 0)
    total_effort += effort
    risk = a.get('migration_risk', 'LOW')
    auto = {'LOW': '90%', 'MEDIUM': '60%', 'HIGH': '30%', 'CRITICAL': '5%'}.get(risk, '50%')
    priority = {'CRITICAL': '1-Urgent', 'HIGH': '2-High', 'MEDIUM': '3-Medium', 'LOW': '4-Low'}.get(risk, '4-Low')
    rows_html += f'''
    <tr>
      <td style="padding:8px; border:1px solid #ddd;">{a.get('mapping_name','')}</td>
      <td style="padding:8px; border:1px solid #ddd;">{a.get('complexity_level','')}</td>
      <td style="padding:8px; border:1px solid #ddd;">{auto}</td>
      <td style="padding:8px; border:1px solid #ddd;">{effort:.1f}h</td>
      <td style="padding:8px; border:1px solid #ddd;">{priority}</td>
    </tr>'''

rows_html += f'''
    <tr style="background:#ecf0f1; font-weight:bold;">
      <td style="padding:8px; border:1px solid #ddd;" colspan="3">TOTAL</td>
      <td style="padding:8px; border:1px solid #ddd;">{total_effort:.1f}h</td>
      <td style="padding:8px; border:1px solid #ddd;"></td>
    </tr>'''

html = f'''
<table style="border-collapse:collapse; width:100%; font-family:sans-serif;">
  <thead>
    <tr style="background:#2c3e50; color:white;">
      <th style="padding:10px; border:1px solid #ddd;">Mapping</th>
      <th style="padding:10px; border:1px solid #ddd;">Complexity</th>
      <th style="padding:10px; border:1px solid #ddd;">Auto %</th>
      <th style="padding:10px; border:1px solid #ddd;">Manual Effort</th>
      <th style="padding:10px; border:1px solid #ddd;">Priority</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
'''
_display_html(html)"""

    @staticmethod
    def _dependency_graph_code() -> str:
        return """\
mapping_deps = dependencies.get('mapping_dependencies', {})
table_deps = dependencies.get('table_dependencies', {})

if mapping_deps or table_deps:
    lines = ['<pre style="font-family:monospace; font-size:12px; background:#f8f9fa; padding:16px; border-radius:4px;">']

    if mapping_deps:
        lines.append('MAPPING DEPENDENCIES')
        lines.append('=' * 60)
        for src, targets in mapping_deps.items():
            if isinstance(targets, list):
                for t in targets:
                    lines.append(f'  {src} ──→ {t}')
            else:
                lines.append(f'  {src} ──→ {targets}')
        lines.append('')

    if table_deps:
        lines.append('TABLE DEPENDENCIES')
        lines.append('=' * 60)
        for src, targets in table_deps.items():
            if isinstance(targets, list):
                for t in targets:
                    lines.append(f'  {src} ──→ {t}')
            else:
                lines.append(f'  {src} ──→ {targets}')

    lines.append('</pre>')
    _display_html('\\n'.join(lines))
else:
    print('No dependency data available.')"""

    @staticmethod
    def _compatibility_issues_code() -> str:
        return """\
if issues:
    sev_colors = {
        'CRITICAL': '#e74c3c',
        'HIGH': '#e67e22',
        'MEDIUM': '#f1c40f',
        'LOW': '#3498db',
        'INFO': '#95a5a6',
    }
    rows_html = ''
    for issue in issues:
        sev = issue.get('severity', 'INFO') if isinstance(issue, dict) else 'INFO'
        color = sev_colors.get(sev, '#95a5a6')
        component = issue.get('component', '') if isinstance(issue, dict) else ''
        desc = issue.get('description', str(issue)) if isinstance(issue, dict) else str(issue)
        fix = issue.get('suggested_fix', '') if isinstance(issue, dict) else ''
        rows_html += f'''
        <tr>
          <td style="padding:8px; border:1px solid #ddd;"><span style="background:{color}; color:white; padding:2px 8px; border-radius:3px; font-size:0.85em;">{sev}</span></td>
          <td style="padding:8px; border:1px solid #ddd;">{component}</td>
          <td style="padding:8px; border:1px solid #ddd;">{desc}</td>
          <td style="padding:8px; border:1px solid #ddd;">{fix}</td>
        </tr>'''

    html = f'''
    <table style="border-collapse:collapse; width:100%; font-family:sans-serif;">
      <thead>
        <tr style="background:#2c3e50; color:white;">
          <th style="padding:10px; border:1px solid #ddd;">Severity</th>
          <th style="padding:10px; border:1px solid #ddd;">Component</th>
          <th style="padding:10px; border:1px solid #ddd;">Description</th>
          <th style="padding:10px; border:1px solid #ddd;">Suggested Fix</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    '''
    _display_html(html)
else:
    print('No compatibility issues found.')"""

    @staticmethod
    def _recommendations_code() -> str:
        return """\
recs = []

critical = [a for a in assessments if a.get('migration_risk') == 'CRITICAL']
high_risk = [a for a in assessments if a.get('migration_risk') == 'HIGH']
has_sp = [a for a in assessments if a.get('has_stored_procedures')]
has_custom = [a for a in assessments if a.get('has_custom_transformations')]
has_scd = [a for a in assessments if a.get('has_scd_logic')]

if critical:
    names = ', '.join(a.get('mapping_name', '?') for a in critical)
    recs.append(f'<li><strong>CRITICAL:</strong> Review mappings with critical risk before migration: {names}</li>')

if high_risk:
    recs.append(f'<li><strong>HIGH RISK:</strong> {len(high_risk)} mapping(s) need manual review and possible redesign.</li>')

if has_sp:
    recs.append(f'<li><strong>Stored Procedures:</strong> {len(has_sp)} mapping(s) use stored procedures that must be rewritten as PySpark/SQL.</li>')

if has_custom:
    recs.append(f'<li><strong>Custom Transformations:</strong> {len(has_custom)} mapping(s) use custom/Java transformations needing AIDP equivalents.</li>')

if has_scd:
    recs.append(f'<li><strong>SCD Logic:</strong> {len(has_scd)} mapping(s) have SCD logic. Use Delta Lake MERGE for SCD Type 2.</li>')

low_risk = [a for a in assessments if a.get('migration_risk') == 'LOW']
if low_risk:
    recs.append(f'<li><strong>Quick Wins:</strong> {len(low_risk)} low-risk mapping(s) can be auto-converted and validated first.</li>')

recs.append('<li><strong>Testing:</strong> Run reconciliation checks (row counts + checksums) for each migrated mapping.</li>')
recs.append('<li><strong>Rollback:</strong> Keep Informatica mappings active until AIDP pipelines are validated in production.</li>')

if not recs:
    recs.append('<li>All mappings appear straightforward. Proceed with auto-conversion.</li>')

html = '<div style="font-family:sans-serif; padding:8px;"><ol style="line-height:2;">' + ''.join(recs) + '</ol></div>'
_display_html(html)"""

    # ------------------------------------------------------------------
    # Reconciliation dashboard cells
    # ------------------------------------------------------------------

    @staticmethod
    def _reconcile_title_cell() -> str:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        return _md(
            "# Migration Reconciliation Dashboard\n"
            "Post-migration validation: row counts, schema checks, and data diffs\n"
            "\n"
            f"_Generated: {ts}_"
        )

    @staticmethod
    def _reconcile_setup_cell(reconcile_report_path: str) -> str:
        return (
            "import json\n"
            "import matplotlib\n"
            "matplotlib.use('Agg')\n"
            "import matplotlib.pyplot as plt\n"
            "\n"
            "def _display_html(html):\n"
            "    try:\n"
            "        displayHTML(html)\n"
            "    except NameError:\n"
            "        print(html)\n"
            "\n"
            "def _show_fig(fig):\n"
            "    try:\n"
            "        display(fig)\n"
            "    except Exception:\n"
            "        plt.show()\n"
            "    plt.close(fig)\n"
            "\n"
            # repr(): see the analysis setup cell above.
            f'with open({reconcile_report_path!r}, encoding="utf-8") as f:\n'
            "    reconcile = json.load(f)\n"
            "\n"
            "# reconcile_report.json (ReconcileReport.to_json) carries the per-config\n"
            "# results under 'results' with status PASSED/FAILED/ERROR; the older\n"
            "# 'tables'/'PASS' spelling is accepted too so a hand-written file works.\n"
            "tables = reconcile.get('results', reconcile.get('tables', []))\n"
            "for t in tables:\n"
            "    t.setdefault('table_name', t.get('config_name', ''))\n"
            "    t.setdefault('schema_mismatches', t.get('schema_diffs', []))\n"
            "    if t.get('status') == 'PASS':\n"
            "        t['status'] = 'PASSED'\n"
            "summary = reconcile.get('summary', {})"
        )

    @staticmethod
    def _reconcile_summary_code() -> str:
        return """\
total = len(tables)
passed = sum(1 for t in tables if t.get('status') == 'PASSED')
failed = total - passed
pass_pct = round(passed / max(total, 1) * 100, 1)

pass_color = '#2ecc71' if pass_pct >= 90 else '#f39c12' if pass_pct >= 70 else '#e74c3c'

html = f'''
<div style="display:flex; gap:24px; flex-wrap:wrap; margin:16px 0;">
  <div style="background:#f8f9fa; border-left:4px solid #3498db; padding:16px 24px;">
    <div style="font-size:36px; font-weight:bold;">{total}</div>
    <div style="color:#7f8c8d;">Tables Checked</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid #2ecc71; padding:16px 24px;">
    <div style="font-size:36px; font-weight:bold; color:#27ae60;">{passed}</div>
    <div style="color:#7f8c8d;">Passed</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid #e74c3c; padding:16px 24px;">
    <div style="font-size:36px; font-weight:bold; color:#e74c3c;">{failed}</div>
    <div style="color:#7f8c8d;">Failed</div>
  </div>
  <div style="background:#f8f9fa; border-left:4px solid {pass_color}; padding:16px 24px;">
    <div style="font-size:36px; font-weight:bold; color:{pass_color};">{pass_pct}%</div>
    <div style="color:#7f8c8d;">Pass Rate</div>
  </div>
</div>
'''
_display_html(html)"""

    @staticmethod
    def _reconcile_row_counts_code() -> str:
        return """\
if tables:
    names = [t.get('table_name', f'T{i}') for i, t in enumerate(tables)]
    src_counts = [t.get('source_row_count', 0) for t in tables]
    tgt_counts = [t.get('target_row_count', 0) for t in tables]

    fig, ax = plt.subplots(figsize=(max(8, len(names) * 0.8), 6))
    x = range(len(names))
    width = 0.35
    ax.bar([i - width/2 for i in x], src_counts, width, label='Source (Informatica)', color='#3498db')
    ax.bar([i + width/2 for i in x], tgt_counts, width, label='Target (AIDP)', color='#2ecc71')
    ax.set_xticks(x)
    short_names = [n[:18] + '..' if len(n) > 18 else n for n in names]
    ax.set_xticklabels(short_names, rotation=45, ha='right', fontsize=9)
    ax.set_ylabel('Row Count')
    ax.set_title('Row Count: Source vs Target', fontsize=14, fontweight='bold')
    ax.legend()
    plt.tight_layout()
    _show_fig(fig)

    rows_html = ''
    for t in tables:
        src = t.get('source_row_count', 0)
        tgt = t.get('target_row_count', 0)
        diff = tgt - src
        pct = round(diff / max(src, 1) * 100, 2)
        status = t.get('status', 'UNKNOWN')
        bg = '#d5f5e3' if status == 'PASSED' else '#fadbd8'
        rows_html += f'''
        <tr style="background:{bg};">
          <td style="padding:6px; border:1px solid #ddd;">{t.get('table_name','')}</td>
          <td style="padding:6px; border:1px solid #ddd; text-align:right;">{src:,}</td>
          <td style="padding:6px; border:1px solid #ddd; text-align:right;">{tgt:,}</td>
          <td style="padding:6px; border:1px solid #ddd; text-align:right;">{diff:+,}</td>
          <td style="padding:6px; border:1px solid #ddd; text-align:right;">{pct:+.2f}%</td>
          <td style="padding:6px; border:1px solid #ddd; font-weight:bold;">{status}</td>
        </tr>'''

    html = f'''
    <table style="border-collapse:collapse; width:100%; font-family:sans-serif;">
      <thead>
        <tr style="background:#2c3e50; color:white;">
          <th style="padding:8px; border:1px solid #ddd;">Table</th>
          <th style="padding:8px; border:1px solid #ddd;">Source Rows</th>
          <th style="padding:8px; border:1px solid #ddd;">Target Rows</th>
          <th style="padding:8px; border:1px solid #ddd;">Diff</th>
          <th style="padding:8px; border:1px solid #ddd;">Diff %</th>
          <th style="padding:8px; border:1px solid #ddd;">Status</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    '''
    _display_html(html)
else:
    print('No table reconciliation data available.')"""

    @staticmethod
    def _reconcile_schema_diff_code() -> str:
        return """\
schema_issues = [t for t in tables if t.get('schema_mismatches')]

if schema_issues:
    rows_html = ''
    for t in schema_issues:
        for m in t.get('schema_mismatches', []):
            if isinstance(m, dict):
                col = m.get('column', '')
                src_type = m.get('source_type', '')
                tgt_type = m.get('target_type', '')
            else:
                col, src_type, tgt_type = str(m), '', ''
            rows_html += f'''
            <tr>
              <td style="padding:6px; border:1px solid #ddd;">{t.get('table_name','')}</td>
              <td style="padding:6px; border:1px solid #ddd;">{col}</td>
              <td style="padding:6px; border:1px solid #ddd;">{src_type}</td>
              <td style="padding:6px; border:1px solid #ddd;">{tgt_type}</td>
            </tr>'''

    html = f'''
    <table style="border-collapse:collapse; width:100%; font-family:sans-serif;">
      <thead>
        <tr style="background:#e67e22; color:white;">
          <th style="padding:8px; border:1px solid #ddd;">Table</th>
          <th style="padding:8px; border:1px solid #ddd;">Column</th>
          <th style="padding:8px; border:1px solid #ddd;">Source Type</th>
          <th style="padding:8px; border:1px solid #ddd;">Target Type</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
    '''
    _display_html(html)
else:
    print('No schema mismatches detected.')"""

    @staticmethod
    def _reconcile_data_diff_code() -> str:
        return """\
data_diffs = [t for t in tables if t.get('sample_diffs')]

if data_diffs:
    for t in data_diffs:
        print(f"\\nTable: {t.get('table_name', '')}")
        print('-' * 60)
        for diff in t.get('sample_diffs', [])[:10]:
            if isinstance(diff, dict):
                key = diff.get('key', '')
                col = diff.get('column', '')
                src_val = diff.get('source_value', '')
                tgt_val = diff.get('target_value', '')
                print(f'  Key={key}  Column={col}  Source={src_val}  Target={tgt_val}')
            else:
                print(f'  {diff}')
else:
    print('No sample data differences found.')"""

    @staticmethod
    def _reconcile_verdict_code() -> str:
        return """\
total = len(tables)
passed = sum(1 for t in tables if t.get('status') == 'PASSED')
failed = total - passed

if total == 0:
    # No results at all is not a pass: the report was empty or its shape
    # was not recognised.
    verdict = 'NO RECONCILIATION RESULTS'
    color = '#e74c3c'
    msg = 'The reconcile report contains no per-config results. Nothing was verified.'
elif failed == 0:
    verdict = 'ALL CHECKS PASSED'
    color = '#2ecc71'
    msg = 'Migration reconciliation complete. All tables match between source and target.'
elif failed <= total * 0.1:
    verdict = 'MOSTLY PASSED'
    color = '#f39c12'
    msg = f'{failed} table(s) have minor discrepancies. Review before sign-off.'
else:
    verdict = 'ACTION REQUIRED'
    color = '#e74c3c'
    msg = f'{failed} table(s) failed reconciliation. Investigate and re-run migration.'

html = f'''
<div style="text-align:center; padding:32px; margin:16px 0; background:#f8f9fa; border-radius:8px;">
  <div style="font-size:48px; font-weight:bold; color:{color};">{verdict}</div>
  <div style="font-size:16px; color:#7f8c8d; margin-top:8px;">{msg}</div>
</div>
'''
_display_html(html)"""

    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------

    @staticmethod
    def _assemble(cells: list[str]) -> str:
        """Join cells into a single notebook string."""
        parts = ["# Databricks notebook source"]
        for cell in cells:
            parts.append(CELL_SEP)
            parts.append(cell)
        return "\n\n".join(parts) + "\n"
