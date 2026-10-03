"""Non-circular source fidelity check.

``llm_validator.validate()`` scores generated code against a spec produced
by the SAME parse that fed the generator -- it answers "does this code
match what the parser understood?", never "does this code match the
Informatica mapping?". If the parser drops a construct, the spec drops it
too, and the generator and validator are blind to the gap identically.

This is not hypothetical: in this repo, ``PORTTYPE="VARIABLE"`` used to
resolve to ``INPUT``, silently discarding every Expression variable port's
expression -- running totals, prior-row comparisons, change-detection
flags. The spec never carried those ports either, so a notebook missing
every one of them could still score 100/100 (see ``test_variable_ports.py``
for the fix; see ``tests/test_source_fidelity.py`` for the regression
proof against this exact defect).

``source_fidelity()`` breaks that circularity by counting constructs
directly in the RAW export file -- the PowerCenter XML or IICS/IDMC JSON
on disk, never the parsed IR, never the LLM spec -- and checking whether
each named construct shows up in the generated notebook's code. Anything
lost between export and notebook is visible here even if it was lost
during parsing, before the generator or validator ever saw it.

Some gaps are legitimate: a pass-through Expression port may correctly
produce no new column, and a Filter condition may be folded into a single
``.filter()`` call that doesn't repeat every field name it touches. This
module never fails a build -- it names the gaps; a human decides whether
each one matters. See the module docstring on ``FidelityReport`` for the
exact fields a caller/report can rely on.
"""
from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class FidelityReport:
    """Construct-level comparison of a notebook against the raw export it
    claims to implement -- independent of whatever the parser/IR/LLM spec
    understood about that same export.

    ``*_missing`` lists are NAMED constructs (e.g. ``"EXP_TAX.NET_AMOUNT"``
    for an expression, or the bare transformation name), not just a count
    -- a name is what makes a gap actionable in review.
    """

    source_path: str
    source_format: str  # "xml" (PowerCenter) or "json" (IICS/IDMC)

    transformations_in_source: list[str] = field(default_factory=list)
    transformations_missing: list[str] = field(default_factory=list)

    # Each entry is "TRANSFORMATION_NAME.FIELD_NAME" for a field carrying a
    # non-empty expression in the source.
    expressions_in_source: list[str] = field(default_factory=list)
    expressions_missing: list[str] = field(default_factory=list)

    # Each entry is "FROM_INSTANCE.FROM_FIELD -> TO_INSTANCE.TO_FIELD".
    connectors_in_source: list[str] = field(default_factory=list)
    connectors_missing: list[str] = field(default_factory=list)

    # Parse errors or reviewer-facing caveats -- never the cause of a
    # non-empty *_missing list by itself.
    notes: list[str] = field(default_factory=list)

    @property
    def has_gaps(self) -> bool:
        return bool(
            self.transformations_missing
            or self.expressions_missing
            or self.connectors_missing
        )

    def summary(self) -> str:
        """Human-readable report block -- counts plus named gaps."""

        def _line(label: str, total: list[str], missing: list[str]) -> str:
            present = len(total) - len(missing)
            text = f"  {label}: {present}/{len(total)} represented"
            if missing:
                text += f" -- MISSING: {', '.join(missing)}"
            return text

        lines = [f"source_fidelity: {self.source_path} ({self.source_format})"]
        lines.append(_line("transformations", self.transformations_in_source, self.transformations_missing))
        lines.append(_line("expressions", self.expressions_in_source, self.expressions_missing))
        lines.append(_line("connectors", self.connectors_in_source, self.connectors_missing))
        if self.notes:
            lines.append("  notes: " + "; ".join(self.notes))
        return "\n".join(lines)


def _strip_comments(code: str) -> str:
    """Remove ``#`` comments, keeping string literals intact.

    A construct named only in a comment is not implemented, and counting it
    as represented made this check reward verbosity. The LLM path emits
    comment-rich notebooks and scored 0/12 gaps against the rule-based
    path's 10/12 on the same corpus -- not better fidelity, just more
    prose. ``EXP_ENRICH`` in ``m_customer_enrichment`` was "represented"
    by ``# SOURCE: CUSTOMERS -> SQ_CUSTOMERS`` and nothing else.

    String literals are deliberately kept: ``F.col("BIRTH_DATE")`` is the
    real way a column is referenced, so dropping literals would invent
    gaps everywhere.

    Uses ``tokenize`` so a ``#`` inside a string is not mistaken for a
    comment. Generated notebooks are valid Python, but a cell can be a
    fragment; on any tokenize error the text is returned unchanged and a
    caller-visible note is not raised here -- over-reporting gaps on
    unparseable code would be worse than the verbosity it guards against.
    """
    import io
    import tokenize

    try:
        out, last_row, last_col = [], 1, 0
        for tok in tokenize.generate_tokens(io.StringIO(code).readline):
            if tok.start[0] > last_row:
                out.append("\n" * (tok.start[0] - last_row))
                last_col = 0
            if tok.start[1] > last_col:
                out.append(" " * (tok.start[1] - last_col))
            if tok.type != tokenize.COMMENT:
                out.append(tok.string)
            last_row, last_col = tok.end
        return "".join(out)
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return code


def _extract_notebook_code(notebook_code: str) -> str:
    """Return the concatenated code-cell text, comments removed.

    Accepts either a full .ipynb JSON string (as produced by
    ``LLMNotebookGenerator._assemble_ipynb``) or plain source text (a .py
    script, or bare cell/code text) -- whichever shape a caller has on
    hand, transparently.

    Markdown cells are excluded. Comments are KEPT here: a transformation
    name has no runtime artifact in PySpark, so a comment is the only way
    it can be represented, and the transformation check matches against
    this text. Dimensions that must appear in executable code use
    :func:`_strip_comments` on top of this -- see :func:`source_fidelity`.
    """
    text = notebook_code or ""
    stripped = text.lstrip()
    if stripped.startswith("{"):
        try:
            nb = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(nb, dict) and "cells" in nb:
            parts = []
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    src = cell.get("source", "")
                    parts.append("".join(src) if isinstance(src, list) else src)
            return "\n".join(parts)
    return text


def _xml_constructs(root: ET.Element) -> tuple[list[str], list[str], list[str]]:
    """(transformation_names, expression_names, connector_names) read
    straight off a PowerCenter XML MAPPING element -- no IR involved."""
    tx_names: list[str] = []
    expr_names: list[str] = []
    for tx in root.iter("TRANSFORMATION"):
        tx_name = tx.get("NAME", "")
        if not tx_name:
            continue
        tx_names.append(tx_name)
        for tf in tx.findall("TRANSFORMFIELD"):
            expr = tf.get("EXPRESSION", "")
            fname = tf.get("NAME", "")
            if not (expr and expr.strip() and fname):
                continue
            # A port whose EXPRESSION is just its own name computes nothing
            # -- it is a pass-through, and PowerCenter writes one on every
            # port of a Source Qualifier. Counting those as expressions made
            # the report demand that every unconnected source column appear
            # in the notebook, which is backwards: Informatica does not
            # carry an unconnected port either. They were a large share of
            # the reported "missing expressions" and every one was a false
            # positive.
            if expr.strip().upper() == fname.strip().upper():
                continue
            expr_names.append(f"{tx_name}.{fname}")

    conn_names: list[str] = []
    for c in root.iter("CONNECTOR"):
        from_i = c.get("FROMINSTANCE") or c.get("FROMTRANSFORMATION") or ""
        from_f = c.get("FROMFIELD", "")
        to_i = c.get("TOINSTANCE") or c.get("TOTRANSFORMATION") or ""
        to_f = c.get("TOFIELD", "")
        conn_names.append(f"{from_i}.{from_f} -> {to_i}.{to_f}")

    return tx_names, expr_names, conn_names


def _json_constructs(data: dict) -> tuple[list[str], list[str], list[str]]:
    """(transformation_names, expression_names, connector_names) read
    straight off a parsed IICS/IDMC JSON export -- no IR involved."""
    tx_names: list[str] = []
    expr_names: list[str] = []
    for tx in data.get("transformations", []) or []:
        tx_name = tx.get("name", "")
        if not tx_name:
            continue
        tx_names.append(tx_name)
        for f_ in tx.get("fields", []) or []:
            expr = f_.get("expression", "")
            fname = f_.get("name", "")
            if expr and str(expr).strip() and fname:
                expr_names.append(f"{tx_name}.{fname}")

    conn_names: list[str] = []
    connections = data.get("connections") or data.get("connectors") or []
    for c in connections:
        from_part = c.get("from")
        to_part = c.get("to")
        if isinstance(from_part, dict):
            from_i = from_part.get("transformation", "")
            from_f = from_part.get("field", "")
        else:
            from_i = from_part or ""
            from_f = c.get("from_field") or c.get("fromField", "")
        if isinstance(to_part, dict):
            to_i = to_part.get("transformation", "")
            to_f = to_part.get("field", "")
        else:
            to_i = to_part or ""
            to_f = c.get("to_field") or c.get("toField", "")
        conn_names.append(f"{from_i}.{from_f} -> {to_i}.{to_f}")

    return tx_names, expr_names, conn_names


def _dedupe(items: list[str]) -> list[str]:
    return list(dict.fromkeys(items))


def _decode_export(raw: bytes) -> str:
    """Decode an export honouring its XML encoding declaration, falling
    back to UTF-8 (with BOM) and then cp1252 so a stray byte is never a
    reason to skip the check."""
    import re as _re

    m = _re.match(rb'^\s*<\?xml[^>]*encoding=["\']([A-Za-z0-9._-]+)["\']', raw)
    candidates = []
    if m:
        candidates.append(m.group(1).decode("ascii", "ignore"))
    candidates += ["utf-8-sig", "cp1252"]
    for enc in candidates:
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", errors="replace")


def source_fidelity(source_path: str, notebook_code: str) -> FidelityReport:
    """Compare a generated notebook against the RAW export, bypassing the IR.

    The validator score compares generated code against a spec derived from
    the same parse, so parser loss is invisible to it: when
    ``PORTTYPE="VARIABLE"`` was dropped, the spec lacked those ports too and
    a notebook missing every running total could score 100/100.

    This check counts constructs in the source file itself, so anything
    lost between export and notebook shows up as a gap.

    Args:
        source_path: Path to the raw Informatica export -- PowerCenter XML
            or IICS/IDMC JSON. Format is auto-detected the same way
            ``format_detector.detect_and_parse_file`` does (extension
            first, content sniff as fallback), but this function never
            calls that parser -- it reads the file directly.
        notebook_code: The generated notebook (.ipynb JSON string, .py
            source, or bare code text).

    Returns:
        A :class:`FidelityReport` naming what's missing, never raising for
        a content mismatch -- only for an unreadable file.
    """
    path = Path(source_path)
    # PowerCenter exports declare their own encoding (Windows-1252 is
    # common, and the bundled samples use it). Decoding as UTF-8 raised on
    # the first accented character and the caller recorded the mapping as
    # "not checked" -- the one independent check silently absent for
    # exactly the real exports it exists for.
    raw_bytes = path.read_bytes()
    raw = _decode_export(raw_bytes)
    trimmed = raw.lstrip()

    is_json = path.suffix.lower() == ".json" or (
        path.suffix.lower() != ".xml" and (trimmed.startswith("{") or trimmed.startswith("["))
    )

    if is_json:
        source_format = "json"
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            return FidelityReport(
                source_path=source_path,
                source_format=source_format,
                notes=[f"could not parse source as JSON: {exc}"],
            )
        tx_names, expr_names, conn_names = _json_constructs(data)
    else:
        source_format = "xml"
        try:
            root = ET.fromstring(raw)
        except ET.ParseError as exc:
            return FidelityReport(
                source_path=source_path,
                source_format=source_format,
                notes=[f"could not parse source as XML: {exc}"],
            )
        tx_names, expr_names, conn_names = _xml_constructs(root)

    tx_names = _dedupe(tx_names)
    expr_names = _dedupe(expr_names)
    conn_names = _dedupe(conn_names)

    code = _extract_notebook_code(notebook_code)
    # Two views of the same notebook, because the three dimensions are not
    # alike. A transformation NAME never appears in executable PySpark --
    # an Expression becomes withColumn calls, not a symbol called
    # EXP_TAX -- so a comment is its only possible representation and
    # `code` (comments kept) is the right text to match it against.
    #
    # An expression's output field and a connector's target field DO appear
    # in executable code, as withColumn("NET_AMOUNT", ...) or
    # F.col("BIRTH_DATE"). Matching those against comments rewards
    # verbosity: the LLM path emits comment-rich notebooks and reported
    # 0/12 fidelity gaps where the rule-based path reported 10/12 on the
    # same corpus -- not better fidelity, just more prose. So those two
    # match against comment-free code.
    #
    # String literals are deliberately kept in both views: F.col("X") is
    # how a column is really referenced.
    runnable = _strip_comments(code)

    tx_missing = [n for n in tx_names if n not in code]

    expr_missing = []
    for qualified in expr_names:
        field_name = qualified.split(".", 1)[1]
        if field_name not in runnable:
            expr_missing.append(qualified)

    conn_missing = []
    for qualified in conn_names:
        to_field = qualified.rsplit(" -> ", 1)[-1].split(".", 1)[-1]
        if to_field and to_field not in runnable:
            conn_missing.append(qualified)

    notes: list[str] = []
    if tx_missing or expr_missing or conn_missing:
        notes.append(
            "gaps are reported, not failed -- some (e.g. a pass-through "
            "Expression, or a condition folded into a single .filter() "
            "call) may be legitimate; this is a human review signal, not "
            "a build gate."
        )

    return FidelityReport(
        source_path=source_path,
        source_format=source_format,
        transformations_in_source=tx_names,
        transformations_missing=tx_missing,
        expressions_in_source=expr_names,
        expressions_missing=expr_missing,
        connectors_in_source=conn_names,
        connectors_missing=conn_missing,
        notes=notes,
    )
