def truncate_output(text, max_lines=200):
    if not text:
        return ""
    lines = text.strip().split("\n")
    if len(lines) > max_lines:
        lines = lines[:max_lines] + [f"... ({len(lines) - max_lines} lines truncated)"]
    return "\n".join(lines)


def format_table(col_names, rows, display_names=None):
    """Format query results as an aligned text table.

    col_names: keys used to look up values in row dicts (jsonColumnName from ORDS)
    display_names: column headers to display (columnName from ORDS, uppercase)
    """
    if not rows:
        return "(no rows)"
    if display_names is None:
        display_names = col_names

    # Convert all values to strings
    str_rows = []
    for row in rows:
        str_rows.append([str(row.get(c, "")) for c in col_names])

    # Calculate column widths
    widths = [len(c) for c in display_names]
    for row in str_rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(val))

    # Build table
    def fmt_row(vals):
        return " | ".join(v.ljust(w) for v, w in zip(vals, widths))

    lines = [fmt_row(display_names), "-+-".join("-" * w for w in widths)]
    for row in str_rows:
        lines.append(fmt_row(row))

    return "\n".join(lines)
