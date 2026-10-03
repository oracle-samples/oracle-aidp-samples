"""Generate side-by-side comparison of Informatica vs PySpark code.

Produces markdown and HTML reports showing original Informatica
transformation logic alongside the converted PySpark output, with
confidence indicators and reviewer notes.
"""

from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent

from ..models import (
    DataFlowDirection,
    Transformation,
    TransformationType,
)


@dataclass
class ComparisonEntry:
    transformation_name: str
    transformation_type: str
    original_informatica: str
    converted_pyspark: str
    confidence: str = "HIGH"
    notes: list = field(default_factory=list)


class ComparisonGenerator:
    """Generate side-by-side comparison of Informatica vs PySpark code."""

    def generate(
        self, mapping, conversion_results: dict
    ) -> list[ComparisonEntry]:
        """Create comparison entries for all transformations in a mapping.

        Args:
            mapping: A Mapping instance from models.
            conversion_results: dict mapping transformation name -> PySpark code string.
        """
        entries = []
        for tx in mapping.transformations:
            original = self._extract_original(tx)
            converted = conversion_results.get(tx.name, "# No conversion generated")
            confidence = self._assess_confidence(tx, converted)
            notes = self._build_notes(tx, converted)
            entries.append(
                ComparisonEntry(
                    transformation_name=tx.name,
                    transformation_type=(
                        tx.type.value if hasattr(tx.type, "value") else str(tx.type)
                    ),
                    original_informatica=original,
                    converted_pyspark=converted,
                    confidence=confidence,
                    notes=notes,
                )
            )
        return entries

    # ------------------------------------------------------------------
    # Original-logic extraction
    # ------------------------------------------------------------------

    def _extract_original(self, tx: Transformation) -> str:
        """Extract the original Informatica logic from a transformation."""
        handler = _EXTRACT_DISPATCH.get(tx.type)
        if handler:
            return handler(self, tx)
        return self._extract_generic(tx)

    def _extract_expression(self, tx: Transformation) -> str:
        lines = []
        for f in tx.fields:
            if f.expression and f.direction in (
                DataFlowDirection.OUTPUT,
                DataFlowDirection.INPUT_OUTPUT,
            ):
                lines.append(f"{f.name} = {f.expression}")
        return "\n".join(lines) if lines else "(no expressions)"

    def _extract_filter(self, tx: Transformation) -> str:
        return tx.filter_condition or "(no filter condition)"

    def _extract_joiner(self, tx: Transformation) -> str:
        parts = []
        if tx.join_type:
            parts.append(f"Join type: {tx.join_type}")
        if tx.join_condition:
            parts.append(f"Condition: {tx.join_condition}")
        return "\n".join(parts) if parts else "(no join details)"

    def _extract_lookup(self, tx: Transformation) -> str:
        parts = []
        if tx.lookup_table:
            parts.append(f"Lookup table: {tx.lookup_table}")
        if tx.lookup_condition:
            parts.append(f"Condition: {tx.lookup_condition}")
        if tx.lookup_sql:
            parts.append(f"SQL override:\n{tx.lookup_sql}")
        return "\n".join(parts) if parts else "(no lookup details)"

    def _extract_aggregator(self, tx: Transformation) -> str:
        parts = []
        if tx.group_by_fields:
            parts.append(f"Group by: {', '.join(tx.group_by_fields)}")
        for f in tx.fields:
            if f.expression:
                parts.append(f"{f.name} = {f.expression}")
        return "\n".join(parts) if parts else "(no aggregator details)"

    def _extract_update_strategy(self, tx: Transformation) -> str:
        return tx.update_strategy_expression or "(no update strategy expression)"

    def _extract_source_qualifier(self, tx: Transformation) -> str:
        if tx.sql_override:
            return f"SQL override:\n{tx.sql_override}"
        if tx.filter_condition:
            return f"Source filter: {tx.filter_condition}"
        return "(default source qualifier)"

    def _extract_router(self, tx: Transformation) -> str:
        if not tx.router_groups:
            return "(no router groups)"
        lines = []
        for group in tx.router_groups:
            if isinstance(group, dict):
                lines.append(
                    f"Group '{group.get('name', '?')}': {group.get('condition', '?')}"
                )
            else:
                lines.append(str(group))
        return "\n".join(lines)

    def _extract_sorter(self, tx: Transformation) -> str:
        parts = []
        if tx.sort_keys:
            # A key is either a plain field-name string (legacy/simple
            # shape, one shared tx.sort_direction) or a {"field",
            # "direction"} dict carrying its OWN direction (xml_parser.py's
            # per-key SORTKEY parsing) -- render each key
            # with its actual direction rather than assuming every key
            # shares tx.sort_direction.
            rendered = []
            for key in tx.sort_keys:
                if isinstance(key, dict):
                    field = key.get("field", key.get("name", "?"))
                    direction = key.get("direction", tx.sort_direction)
                    rendered.append(f"{field} {direction}")
                else:
                    rendered.append(str(key))
            parts.append(f"Sort keys: {', '.join(rendered)}")
        if tx.sort_direction:
            parts.append(f"Direction: {tx.sort_direction}")
        return "\n".join(parts) if parts else "(no sorter details)"

    def _extract_sequence_generator(self, tx: Transformation) -> str:
        return f"Start: {tx.start_value}, Increment: {tx.increment_by}"

    def _extract_generic(self, tx: Transformation) -> str:
        parts = []
        if tx.sql_override:
            parts.append(f"SQL:\n{tx.sql_override}")
        for f in tx.fields:
            if f.expression:
                parts.append(f"{f.name} = {f.expression}")
        if tx.properties:
            for k, v in tx.properties.items():
                parts.append(f"{k}: {v}")
        return "\n".join(parts) if parts else f"({tx.type.value} — no extractable logic)"

    # ------------------------------------------------------------------
    # Confidence & notes
    # ------------------------------------------------------------------

    def _assess_confidence(self, tx: Transformation, converted: str) -> str:
        if "# TODO" in converted or "# MANUAL" in converted:
            return "LOW"
        if "# No conversion generated" in converted:
            return "MANUAL"
        if tx.type in (
            TransformationType.STORED_PROCEDURE,
            TransformationType.JAVA,
            TransformationType.CUSTOM,
            TransformationType.HTTP,
        ):
            return "LOW"
        if tx.type in (
            TransformationType.NORMALIZER,
            TransformationType.XML_PARSER,
            TransformationType.XML_GENERATOR,
        ):
            return "MEDIUM"
        return "HIGH"

    def _build_notes(self, tx: Transformation, converted: str) -> list[str]:
        notes = []
        if tx.type == TransformationType.STORED_PROCEDURE:
            notes.append("Stored procedures need manual migration to Spark UDFs or notebooks.")
        if tx.type == TransformationType.JAVA:
            notes.append("Java transformation requires manual rewrite.")
        if "CONNECT BY" in (tx.sql_override or ""):
            notes.append("Oracle CONNECT BY hierarchy detected — rewrite as recursive CTE or GraphX.")
        if "# TODO" in converted:
            notes.append("Contains TODO items requiring manual review.")
        return notes

    # ------------------------------------------------------------------
    # Export: Markdown
    # ------------------------------------------------------------------

    def export_markdown(
        self,
        entries: list[ComparisonEntry],
        mapping_name: str,
        output_path: str,
    ):
        """Write a markdown comparison report to *output_path*."""
        stats = self._summary_stats(entries)
        lines = [
            f"# Side-by-Side Comparison: {mapping_name}",
            "",
            self._stats_block(stats),
            "",
        ]

        for entry in entries:
            badge = _CONFIDENCE_BADGE[entry.confidence]
            lines.append(
                f"## {entry.transformation_name} ({entry.transformation_type}) "
                f"[{entry.confidence} confidence] {badge}"
            )
            lines.append("")
            lines.append("**Informatica:**")
            lines.append("```")
            lines.append(entry.original_informatica)
            lines.append("```")
            lines.append("")
            lines.append("**PySpark:**")
            lines.append("```python")
            lines.append(entry.converted_pyspark)
            lines.append("```")
            if entry.notes:
                lines.append("")
                lines.append("**Notes:**")
                for note in entry.notes:
                    lines.append(f"- {note}")
            lines.append("")
            lines.append("---")
            lines.append("")

        Path(output_path).write_text("\n".join(lines), encoding="utf-8")

    # ------------------------------------------------------------------
    # Export: HTML
    # ------------------------------------------------------------------

    def export_html(
        self,
        entries: list[ComparisonEntry],
        mapping_name: str,
        output_path: str,
    ):
        """Write a standalone HTML comparison report to *output_path*."""
        stats = self._summary_stats(entries)
        rows_html = "\n".join(self._html_row(e) for e in entries)

        html = dedent(f"""\
        <!DOCTYPE html>
        <html lang="en">
        <head>
        <meta charset="utf-8">
        <title>Comparison: {mapping_name}</title>
        <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               margin: 0; padding: 20px; background: #f5f5f5; color: #333; }}
        h1 {{ margin-bottom: 4px; }}
        .stats {{ display: flex; gap: 16px; margin-bottom: 24px; }}
        .stat {{ background: #fff; border-radius: 8px; padding: 12px 20px;
                 box-shadow: 0 1px 3px rgba(0,0,0,.1); }}
        .stat .num {{ font-size: 28px; font-weight: 700; }}
        .stat .lbl {{ font-size: 13px; color: #666; }}
        .entry {{ background: #fff; border-radius: 8px; margin-bottom: 16px;
                  box-shadow: 0 1px 3px rgba(0,0,0,.1); overflow: hidden; }}
        .entry-header {{ padding: 12px 16px; cursor: pointer; display: flex;
                         align-items: center; gap: 10px; user-select: none; }}
        .entry-header:hover {{ background: #fafafa; }}
        .entry-header .arrow {{ transition: transform .2s; }}
        .entry.collapsed .entry-header .arrow {{ transform: rotate(-90deg); }}
        .entry.collapsed .entry-body {{ display: none; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
                  font-size: 12px; font-weight: 600; color: #fff; }}
        .badge-HIGH {{ background: #22863a; }}
        .badge-MEDIUM {{ background: #b08800; }}
        .badge-LOW {{ background: #cb2431; }}
        .badge-MANUAL {{ background: #6a737d; }}
        .columns {{ display: grid; grid-template-columns: 1fr 1fr; }}
        .col {{ padding: 12px 16px; }}
        .col:first-child {{ border-right: 1px solid #e1e4e8; }}
        .col h3 {{ margin: 0 0 8px; font-size: 14px; color: #586069; }}
        pre {{ background: #f6f8fa; padding: 12px; border-radius: 6px;
               overflow-x: auto; font-size: 13px; line-height: 1.5; margin: 0; }}
        .notes {{ padding: 8px 16px 12px; font-size: 13px; color: #586069;
                  border-top: 1px solid #e1e4e8; }}
        .notes ul {{ margin: 4px 0 0; padding-left: 20px; }}
        </style>
        </head>
        <body>
        <h1>Side-by-Side Comparison: {mapping_name}</h1>
        <div class="stats">
          <div class="stat"><div class="num">{stats['total']}</div><div class="lbl">Total</div></div>
          <div class="stat"><div class="num" style="color:#22863a">{stats['high']}</div><div class="lbl">High</div></div>
          <div class="stat"><div class="num" style="color:#b08800">{stats['medium']}</div><div class="lbl">Medium</div></div>
          <div class="stat"><div class="num" style="color:#cb2431">{stats['low']}</div><div class="lbl">Low</div></div>
          <div class="stat"><div class="num" style="color:#6a737d">{stats['manual']}</div><div class="lbl">Manual</div></div>
        </div>
        {rows_html}
        <script>
        document.querySelectorAll('.entry-header').forEach(h => {{
          h.addEventListener('click', () => h.parentElement.classList.toggle('collapsed'));
        }});
        </script>
        </body>
        </html>
        """)

        Path(output_path).write_text(html, encoding="utf-8")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _html_row(self, entry: ComparisonEntry) -> str:
        notes_html = ""
        if entry.notes:
            items = "".join(f"<li>{_esc(n)}</li>" for n in entry.notes)
            notes_html = f'<div class="notes"><ul>{items}</ul></div>'
        return dedent(f"""\
        <div class="entry">
          <div class="entry-header">
            <span class="arrow">&#x25BC;</span>
            <strong>{_esc(entry.transformation_name)}</strong>
            <span style="color:#586069">({_esc(entry.transformation_type)})</span>
            <span class="badge badge-{entry.confidence}">{entry.confidence}</span>
          </div>
          <div class="entry-body">
            <div class="columns">
              <div class="col">
                <h3>Informatica</h3>
                <pre>{_esc(entry.original_informatica)}</pre>
              </div>
              <div class="col">
                <h3>PySpark</h3>
                <pre>{_esc(entry.converted_pyspark)}</pre>
              </div>
            </div>
            {notes_html}
          </div>
        </div>
        """)

    @staticmethod
    def _summary_stats(entries: list[ComparisonEntry]) -> dict:
        stats = {"total": len(entries), "high": 0, "medium": 0, "low": 0, "manual": 0}
        for e in entries:
            key = e.confidence.lower()
            if key in stats:
                stats[key] += 1
        return stats

    @staticmethod
    def _stats_block(stats: dict) -> str:
        return (
            f"| Metric | Count |\n|--------|-------|\n"
            f"| Total transformations | {stats['total']} |\n"
            f"| HIGH confidence | {stats['high']} |\n"
            f"| MEDIUM confidence | {stats['medium']} |\n"
            f"| LOW confidence | {stats['low']} |\n"
            f"| MANUAL review | {stats['manual']} |"
        )


# Dispatch table for extraction by transformation type
_EXTRACT_DISPATCH = {
    TransformationType.EXPRESSION: ComparisonGenerator._extract_expression,
    TransformationType.FILTER: ComparisonGenerator._extract_filter,
    TransformationType.JOINER: ComparisonGenerator._extract_joiner,
    TransformationType.LOOKUP: ComparisonGenerator._extract_lookup,
    TransformationType.AGGREGATOR: ComparisonGenerator._extract_aggregator,
    TransformationType.UPDATE_STRATEGY: ComparisonGenerator._extract_update_strategy,
    TransformationType.SOURCE_QUALIFIER: ComparisonGenerator._extract_source_qualifier,
    TransformationType.ROUTER: ComparisonGenerator._extract_router,
    TransformationType.SORTER: ComparisonGenerator._extract_sorter,
    TransformationType.SEQUENCE_GENERATOR: ComparisonGenerator._extract_sequence_generator,
}

_CONFIDENCE_BADGE = {
    "HIGH": "",
    "MEDIUM": "**",
    "LOW": "**!!**",
    "MANUAL": "**!!!**",
}


def _esc(text: str) -> str:
    """Escape HTML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
