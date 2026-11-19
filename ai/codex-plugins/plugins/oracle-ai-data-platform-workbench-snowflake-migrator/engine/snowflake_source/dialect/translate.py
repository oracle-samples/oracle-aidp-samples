"""Snowflake -> AIDP (Spark SQL) dialect translation. SKELETON.

The framework is real and the coverage is honest. Rules come in two states, and
`coverage()` reports the split so the gap is inspectable rather than implied:

  implemented -- a provably EXACT rewrite. Applied automatically.
  declared    -- recognised, described, and deliberately NOT rewritten, because
                 doing it safely needs statement-level restructuring rather than
                 a token substitution.

The governing rule is the same one that runs through the rest of this plugin:
**never approximate.** A rule either produces SQL that means the same thing, or
it reports what is needed and leaves the input untouched. SQL that "mostly works"
returns numbers, and wrong numbers are worse than a blocked object.

Adding a rule: append to RULES with status="implemented" and a `translate`
callable, or status="declared" with `translate=None` and a `detail` that says
what a real implementation would have to do.
"""
from __future__ import annotations

import re

from . import lexer
from .types import map_type
from dataclasses import dataclass, field
from typing import Callable

__all__ = ["RULES", "TranslationResult", "TranslationRule", "coverage",
           "translate_sql"]


@dataclass
class TranslationResult:
    sql: str
    applied: list[dict] = field(default_factory=list)
    unsupported: list[dict] = field(default_factory=list)
    # A rewrite that is exact in shape but carries a semantic note -- the type
    # mapper's "timezone semantics differ" on a `::TIMESTAMP` cast, say. Not a
    # refusal; the view still migrates and the note travels with it.
    warnings: list[str] = field(default_factory=list)

    @property
    def fully_translated(self) -> bool:
        return not self.unsupported


@dataclass(frozen=True)
class TranslationRule:
    rule_id: str
    construct: str
    description: str
    status: str                      # "implemented" | "declared"
    detect: str                      # regex, case-insensitive
    # Returns (sql, problem) or (sql, problem, warnings).
    translate: Callable[[str], tuple] | None = None
    detail: str = ""
    # Set when the rewrite is exact only under a stated condition. It travels
    # with every application of the rule, so the DDL plan cannot call the
    # result "exact" without the condition next to it.
    caveat: str = ""
    # Which lexer segments `detect` is matched against. "code" is the default
    # and is what every construct rule wants. A rule about the QUOTING of an
    # identifier or a literal has to look at the "ident" or "string" segments
    # themselves -- code_only() blanks exactly those, so a code rule can never
    # see a `"` or a `''`.
    scope: str = "code"              # "code" | "ident" | "string" | "comment"


# --------------------------------------------------------------------------
# implemented rewrites -- exact, token-level
# --------------------------------------------------------------------------

def _iff(sql: str) -> tuple[str, str | None]:
    return lexer.sub_code(r"\bIFF\s*\(", "IF(", sql)[0], None


# A single-quoted literal exactly as lexer._closing_quote reads it: `\'`, `\\`
# and `''` are escapes, not the end. The old `'[^']*'` stopped at the first
# quote, so `'don\'t'::string` matched the tail `'t'::string` and the rewrite
# was spliced into the middle of the literal.
_LITERAL = r"'(?:[^'\\]|\\.|'')*'"

# A double-quoted Snowflake identifier, `""` being an escaped quote.
_QUOTED_IDENT = r'"(?:[^"]|"")*"'

# Only a bare identifier, qualified column, quoted identifier or literal.
# Anything else (a closing paren, an operator) means the operand's left edge is
# ambiguous. A preceding backslash or `$` is excluded too: the first is the
# inside of a literal, the second a `$$` closer or a `$1` positional
# reference -- never an operand.
_CAST_SIMPLE = (
    r"(?<![\w).\"'\\$])([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)*|" + _QUOTED_IDENT
    + "|" + _LITERAL + r"|\d+(?:\.\d+)?)"
    r"\s*(?P<op>::)\s*((?:DOUBLE\s+PRECISION|[A-Za-z_][\w$]*)"
    r"(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)")

# The type name as written after `::`: NAME, NAME(p) or NAME(p, s).
_CAST_TYPE = re.compile(
    r"^\s*([A-Za-z_][\w$]*(?:\s+[A-Za-z_]+)?)\s*"
    r"(?:\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\))?\s*$")

# A bare numeric cast is NUMBER(38,0) in Snowflake -- documented, not a
# guess, unlike INFORMATION_SCHEMA where an absent precision is unknown.
_NUMERIC_BARE = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
                 "SMALLINT", "TINYINT", "BYTEINT"}


def _cast(sql: str) -> tuple[str, str | None, list[str]]:
    # The type goes through the SAME mapper as table DDL. Copying the Snowflake
    # name into CAST was silently wrong: Spark FLOAT is single precision, bare
    # DECIMAL is DECIMAL(10,0), INT is 32-bit, and NUMBER / TEXT / TIME are not
    # Spark type names at all.
    problems: list[str] = []
    warnings: list[str] = []

    # The operand is matched against the RAW text -- only `::` has to be code,
    # since a literal operand is legitimate -- so it could END inside a
    # `$$...$$` string or a line comment: `$$a$$::string` matched `a$$`, and
    # `x -- note a` with `::int` on the next line matched the `a` in the
    # comment, and the rewrite was spliced into the segment. The residue
    # check cannot see that: the re-lex reads the spliced text as the string.
    # So the operand has to be wholly code, or exactly one whole string or
    # identifier segment.
    segs: list[tuple[int, int, str]] = []
    pos = 0
    for kind, text in lexer.segments(sql):
        segs.append((pos, pos + len(text), kind))
        pos += len(text)

    def operand_ok(start: int, end: int) -> bool:
        for a, b, kind in segs:
            if a <= start and end <= b:
                return kind == "code" or (
                    kind in ("string", "ident") and (start, end) == (a, b))
        return False

    def repl(m: re.Match) -> str:
        if not operand_ok(*m.span(1)):
            return m.group(0)
        written = m.group(3)
        parsed = _CAST_TYPE.match(written)
        if parsed is None:
            problems.append(f"::{written}: type spelling not understood")
            return m.group(0)
        name = " ".join(parsed.group(1).upper().split())
        precision = int(parsed.group(2)) if parsed.group(2) else None
        scale = int(parsed.group(3)) if parsed.group(3) else None
        if name in _NUMERIC_BARE and precision is None:
            precision, scale = 38, 0
        if name == "TIME":
            # The mapper's TIME -> STRING is right for a TIME column, whose
            # text is preserved. For a cast it is not: the usual operand is a
            # timestamp (`order_ts::time`, time-of-day extraction), and
            # CAST(ts AS STRING) keeps the whole 'yyyy-MM-dd HH:mm:ss' where
            # Snowflake returns 'HH:MI:SS'. Which one applies depends on the
            # operand's type, which a token rule cannot see.
            problems.append(
                f"::{written}: Spark has no TIME type. CAST(x AS STRING) "
                f"returns a TIMESTAMP operand's full date and time where "
                f"Snowflake returns the time of day, and the operand's type "
                f"is not visible to a token rule, so the cast is refused "
                f"(date_format(x, 'HH:mm:ss') is the rewrite for a "
                f"timestamp operand)")
            return m.group(0)
        mapped = map_type(name, precision=precision, scale=scale)
        if mapped.blocked:
            problems.append(f"::{written}: {mapped.reason}")
            return m.group(0)
        if mapped.warning:
            warnings.append(
                f"{m.group(1)}::{written} -> {mapped.spark_type}: {mapped.warning}")
        return f"CAST({m.group(1)} AS {mapped.spark_type})"

    # The operand may legitimately BE a literal ('x'::int), so the anchor is the
    # `::` operator: that is the token which must be code.
    out = lexer.sub_code(_CAST_SIMPLE, repl, sql, anchor_group="op")[0]
    if problems:
        return sql, "; ".join(problems), []
    if lexer.find_code(r"::", out):
        return sql, ("a `::` cast whose left operand is an expression, not a bare "
                     "column or literal. Rewriting it needs the expression "
                     "boundary, which a token rule cannot determine safely"), []
    return out, None, list(dict.fromkeys(warnings))


def _rename(pattern: str, replacement: str):
    def fn(sql: str) -> tuple[str, str | None]:
        return lexer.sub_code(pattern, replacement, sql)[0], None
    return fn


# DATEADD(unit, n, col). Snowflake's argument order differs from Spark's, and
# the unit decides which Spark function applies -- a rename would be wrong.
_DATE_UNITS = {
    "year": "months", "y": "months", "yy": "months", "yyyy": "months",
    "month": "months", "mm": "months", "mon": "months",
    "day": "days", "d": "days", "dd": "days",
    "week": "weeks", "w": "weeks", "wk": "weeks",
    "hour": "interval", "h": "interval", "hh": "interval",
    "minute": "interval", "mi": "interval", "n": "interval",
    "second": "interval", "s": "interval", "ss": "interval",
}
_CANONICAL_INTERVAL = {"hour": "HOUR", "h": "HOUR", "hh": "HOUR",
                       "minute": "MINUTE", "mi": "MINUTE", "n": "MINUTE",
                       "second": "SECOND", "s": "SECOND", "ss": "SECOND"}
_DATEADD = (
    r"\b(?P<kw>DATEADD)\s*\(\s*([A-Za-z]+)\s*,\s*([^,()]+?)\s*,\s*([^,()]+?)\s*\)")

# The only amounts rewritten: an integer literal, or a (qualified) column. An
# expression such as `a + b` would need parenthesising before `* 7`, and
# deciding where its boundary lies is exactly what a token rule cannot do.
_INT_LITERAL = re.compile(r"[+-]?\d+")
_COLUMN_REF = re.compile(r"[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)*")

# Where the rewrite is exact only for a DATE operand. Spark's date_add and
# add_months return DATE, so a TIMESTAMP operand loses its time-of-day;
# Snowflake's DATEADD returns the operand's own type.
_DATEADD_CAVEAT = ("exact for DATE operands only: Spark date_add/add_months "
                   "return DATE, so a TIMESTAMP operand is truncated to DATE")


def _dateadd(sql: str) -> tuple[str, str | None]:
    problems: list[str] = []

    def repl(m: re.Match) -> str:
        unit, amount, col = m.group(2).lower(), m.group(3).strip(), m.group(4).strip()
        kind = _DATE_UNITS.get(unit)
        if kind is None:
            problems.append(
                f"unrecognised DATEADD unit `{unit}`. Units are not guessed -- "
                "add them to _DATE_UNITS once the intended granularity is confirmed")
            return m.group(0)
        literal = _INT_LITERAL.fullmatch(amount) is not None
        if not literal and _COLUMN_REF.fullmatch(amount) is None:
            problems.append(
                f"DATEADD({unit}, {amount}, ...): the amount is an expression, "
                "not an integer literal or a column. Its precedence against the "
                "unit multiplier would be guessed, so it is not rewritten")
            return m.group(0)
        # A column is parenthesised where it meets the multiplier; a literal
        # keeps its bare form.
        n = amount if literal else f"({amount})"
        if kind == "days":
            return f"date_add({col}, {amount})"
        if kind == "weeks":
            return f"date_add({col}, {n} * 7)"
        if kind == "months":
            if unit.startswith("y"):
                return f"add_months({col}, {n} * 12)"
            return f"add_months({col}, {amount})"
        if not literal:
            problems.append(
                f"DATEADD({unit}, {amount}, ...): Spark's INTERVAL literal takes "
                "a numeric constant only; a column amount needs make_interval() "
                "or timestampadd(), which is not an implemented rule")
            return m.group(0)
        return f"({col} + INTERVAL {amount} {_CANONICAL_INTERVAL[unit]})"

    out = lexer.sub_code(_DATEADD, repl, sql, anchor_group="kw")[0]
    if problems:
        return sql, "; ".join(dict.fromkeys(problems))
    # Residue check, as _cast and _listagg do: a form the pattern did not match
    # -- a quoted unit, a nested call such as CURRENT_DATE() -- must be refused,
    # not carried over verbatim and reported as portable.
    if lexer.find_code(r"\bDATEADD\s*\(", out):
        return sql, ("a DATEADD form beyond DATEADD(unit, n, col) with simple "
                     "arguments -- a quoted unit or a nested call such as "
                     "CURRENT_DATE() -- needs argument parsing, which a token "
                     "rule cannot do safely")
    return out, None


_LISTAGG = (
    r"(?P<kw>\bLISTAGG\s*\()\s*([^,()]+?)\s*,\s*('(?:[^']*)')\s*\)")
_OVER = re.compile(r"\s*OVER\b", re.IGNORECASE)


def _listagg(sql: str) -> tuple[str, str | None]:
    if lexer.find_code(r"\bWITHIN\s+GROUP\b", sql):
        return sql, ("LISTAGG ... WITHIN GROUP (ORDER BY ...) -- Spark's "
                     "collect_list does not guarantee ordering, so the ordering "
                     "semantics would not be preserved")
    # LISTAGG(...) OVER (...) is the window form. The match ends at LISTAGG's
    # own closing paren, so it became concat_ws(...) OVER (...) -- a scalar
    # function with a window clause, which Spark rejects at create -- and was
    # stamped exact. Checked over the code mask, so a comment between the
    # call and OVER does not hide it.
    mask = lexer.code_only(sql)
    if any(_OVER.match(mask, m.end()) for m in re.finditer(
            _LISTAGG, sql, re.IGNORECASE)
            if mask[m.start("kw"):m.end("kw")] == sql[m.start("kw"):m.end("kw")]):
        return sql, ("LISTAGG(...) OVER (...) -- the window form. "
                     "concat_ws(sep, collect_list(x)) is an aggregate rewrite; "
                     "with the OVER clause left after it, Spark rejects the "
                     "view. The window would have to move inside, onto "
                     "collect_list, which is not an implemented rule")
    # The separator IS a literal and is reproduced verbatim, so the anchor is
    # the LISTAGG keyword rather than the whole span.
    out = lexer.sub_code(
        _LISTAGG, lambda m: f"concat_ws({m.group(3)}, collect_list({m.group(2)}))",
        sql, anchor_group="kw")[0]
    if lexer.find_code(r"\bLISTAGG\s*\(", out):
        return sql, ("a LISTAGG form beyond LISTAGG(expr, 'sep') -- e.g. DISTINCT "
                     "or an ON OVERFLOW clause")
    return out, None


# --------------------------------------------------------------------------
# quoting rules -- operate on the lexer's ident / string segments, not on code
# --------------------------------------------------------------------------

def _quoted_identifiers(sql: str) -> tuple[str, str | None]:
    # Snowflake "..." and Spark `...` are both exact, case-preserving quoted
    # identifiers, so the rewrite is exact. Spark reads "..." as a STRING
    # LITERAL by default (spark.sql.ansi.doubleQuotedIdentifiers=false), so
    # `select "Order ID" from t` returned the constant text on every row.
    parts: list[str] = []
    for kind, text in lexer.segments(sql):
        if kind == "ident" and text.startswith('"'):
            inner = text[1:-1].replace('""', '"').replace("`", "``")
            parts.append("`" + inner + "`")
        else:
            parts.append(text)
    return "".join(parts), None


def _string_escapes(sql: str) -> tuple[str, str | None]:
    # Spark escapes with a backslash; `''` is not an escape there but two
    # adjacent literals, which Spark concatenates -- 'O''Brien' read as
    # 'OBrien'. Existing backslash escapes are Spark's own and stay as they are.
    parts: list[str] = []
    for kind, text in lexer.segments(sql):
        if kind != "string" or not text.startswith("'"):
            parts.append(text)
            continue
        inner, out, i = text[1:-1], [], 0
        while i < len(inner):
            ch = inner[i]
            if ch == "\\":
                out.append(inner[i:i + 2])
                i += 2
            elif ch == "'" and inner[i:i + 2] == "''":
                out.append("\\'")
                i += 2
            else:
                out.append(ch)
                i += 1
        parts.append("'" + "".join(out) + "'")
    return "".join(parts), None


def _slash_comments(sql: str) -> tuple[str, str | None]:
    # `//` is a Snowflake line comment that Spark does not have: carried
    # verbatim, the rest of the line is parsed as code on the target. `--` is
    # the same comment in both dialects, so the rewrite is exact. Only the
    # lexer's comment segments are touched, so `'http://x'` is data.
    return "".join(
        "--" + text[2:] if kind == "comment" and text.startswith("//") else text
        for kind, text in lexer.segments(sql)), None


# --------------------------------------------------------------------------
# registry
# --------------------------------------------------------------------------

RULES: tuple[TranslationRule, ...] = (
    TranslationRule(
        "T01_IFF", "IFF", "IFF(c, a, b) -> IF(c, a, b)", "implemented",
        r"\bIFF\s*\(", _iff),
    TranslationRule(
        "T02_CAST_SHORTHAND", "::",
        "x::TYPE -> CAST(x AS <mapped Spark type>) via the type mapper",
        "implemented", r"::\s*[A-Za-z]", _cast),
    TranslationRule(
        "T03_ARRAY_CONSTRUCT", "ARRAY_CONSTRUCT",
        "ARRAY_CONSTRUCT(...) -> array(...)", "implemented",
        r"\bARRAY_CONSTRUCT\s*\(",
        _rename(r"\bARRAY_CONSTRUCT\s*\(", "array(")),
    TranslationRule(
        "T04_OBJECT_CONSTRUCT", "OBJECT_CONSTRUCT",
        "OBJECT_CONSTRUCT('k', v, ...) -> named_struct('k', v, ...)",
        "implemented", r"\bOBJECT_CONSTRUCT\s*\(",
        _rename(r"\bOBJECT_CONSTRUCT\s*\(", "named_struct(")),
    TranslationRule(
        "T05_DATEADD", "DATEADD",
        "DATEADD(unit, n, col) -> date_add / add_months / + INTERVAL, per unit; "
        "n an integer literal or a column, anything else refused",
        "implemented", r"\bDATEADD\s*\(", _dateadd, caveat=_DATEADD_CAVEAT),
    TranslationRule(
        "T06_LISTAGG", "LISTAGG",
        "LISTAGG(x, sep) -> concat_ws(sep, collect_list(x))", "implemented",
        r"\bLISTAGG\s*\(", _listagg),
    # The quoting rules run AFTER the construct rules above, so those still see
    # the original Snowflake text (T02's operand pattern reads `"c"`, not a
    # backtick), and before the declared rules, whose detection is code-only
    # and unaffected either way.
    TranslationRule(
        "T07_QUOTED_IDENTIFIER", '"quoted identifier"',
        '"x" -> `x`: Spark reads "..." as a string literal, so every '
        "double-quoted identifier becomes a backtick-quoted one; case is kept",
        "implemented", r'^"', _quoted_identifiers, scope="ident"),
    TranslationRule(
        "T08_STRING_ESCAPE", "'' in a literal",
        "'it''s' -> 'it\\'s': Spark reads a doubled quote as two adjacent "
        "literals and concatenates them", "implemented",
        r"^'(?:[^'\\]|\\.)*''", _string_escapes, scope="string"),
    TranslationRule(
        "T21_SLASH_COMMENT", "// line comment",
        "// comment -> -- comment: Spark has no `//` comment, so the rest of "
        "the line would be parsed as code", "implemented",
        r"^//", _slash_comments, scope="comment"),
    TranslationRule(
        "T09_DOLLAR_QUOTED", "$$...$$ string",
        "$$...$$ dollar-quoted string -> single-quoted literal", "declared",
        r"^\$\$", None,
        "Spark has no dollar quoting. The content is raw text, so a rewrite to "
        "a single-quoted, backslash-escaped literal is possible but is not "
        "applied: it is refused with the construct named, per the "
        "never-approximate rule, until an owner decides.", scope="string"),

    TranslationRule(
        "T10_QUALIFY", "QUALIFY", "QUALIFY <predicate on a window function>",
        "declared", r"\bQUALIFY\b", None,
        "Needs statement restructuring: the window expression must be projected "
        "into a subquery and the predicate moved to an outer WHERE. That changes "
        "the select list and cannot be done by token substitution."),
    TranslationRule(
        "T11_LATERAL_FLATTEN", "LATERAL FLATTEN",
        "LATERAL FLATTEN(input => v) -> explode / LATERAL VIEW", "declared",
        r"\bLATERAL\s+FLATTEN\b|\bFLATTEN\s*\(", None,
        "The target shape depends on the VARIANT's structure and on which of "
        "value/index/key the query reads, so it needs the semi-structured design "
        "decision made first."),
    TranslationRule(
        "T12_GENERATOR", "GENERATOR / SEQ4",
        "TABLE(GENERATOR(ROWCOUNT => n)) -> range(n)", "declared",
        r"\bGENERATOR\s*\(|\bSEQ[48]\s*\(", None,
        "Replaces a table function in the FROM clause and SEQ4() has no exact "
        "Spark equivalent (monotonically_increasing_id is not gapless), so row "
        "identity would change."),
    TranslationRule(
        "T13_PIVOT", "PIVOT / UNPIVOT", "PIVOT/UNPIVOT clause differences",
        "declared", r"\b(?:UN)?PIVOT\s*\(", None,
        "Spark's PIVOT syntax and aggregate placement differ materially; a "
        "mechanical rewrite risks changing the grouping."),
    TranslationRule(
        "T14_SYSTEM_FUNCTION", "SYSTEM$", "SYSTEM$* built-ins", "declared",
        r"\bSYSTEM\$", None,
        "Snowflake-internal functions with no AIDP equivalent. Each needs an "
        "explicit decision about what, if anything, replaces it."),
    TranslationRule(
        "T15_TIME_TRAVEL", "Time Travel", "AT / BEFORE clauses", "declared",
        r"\bAT\s*\(\s*(?:TIMESTAMP|OFFSET|STATEMENT)\b|\bBEFORE\s*\(", None,
        "Delta time travel uses VERSION AS OF / TIMESTAMP AS OF and its retention "
        "is configured differently, so the two are not interchangeable."),
    # An identifier followed by ONE colon. The field after it may be bare
    # (`v:customer`) or quoted (`v:"Field Name"`); wanting an identifier
    # character after the colon let the quoted form through, where T07 then
    # quoted the field and the colon path stayed in a body stamped exact. The
    # lookahead keeps `x::int` out.
    TranslationRule(
        "T16_VARIANT_PATH", "VARIANT path", "col:field.sub -> struct access",
        "declared", r"\b[A-Za-z_][\w$]*\s*:(?!:)", None,
        "Requires the VARIANT column to have been given a concrete struct type "
        "first; until then there is no field to address."),
    TranslationRule(
        "T17_DECODE", "DECODE", "DECODE(x, a, b, ...) -> CASE", "declared",
        r"\bDECODE\s*\(", None,
        "Argument count is variable and the trailing default is positional, so "
        "a correct CASE needs the argument list parsed, not matched."),
    TranslationRule(
        "T18_NVL2", "NVL2", "NVL2(a, b, c) -> CASE WHEN a IS NOT NULL ...",
        "declared", r"\bNVL2\s*\(", None,
        "Mechanically simple but the operands may themselves contain commas, so "
        "it needs argument parsing to split safely."),
    TranslationRule(
        "T19_DATEDIFF", "DATEDIFF / TIMESTAMPDIFF",
        "DATEDIFF(unit, a, b) / TIMESTAMPDIFF(unit, a, b) -> unit-specific "
        "Spark expression", "declared",
        r"\b(?:DATEDIFF|TIMESTAMPDIFF)\s*\(", None,
        "Snowflake counts unit-boundary crossings (DATEDIFF(day, '23:59', "
        "'00:01') is 1) while Spark's datediff / months_between truncate, and "
        "the Snowflake unit abbreviations (dd, yy, mm, hh, mi, ss) are not Spark "
        "datetime units. An exact rewrite needs a per-unit expression and a "
        "decision for sub-day units. Not guessed."),
    TranslationRule(
        "T20_TIMESTAMPADD", "TIMESTAMPADD / TIMEADD",
        "TIMESTAMPADD(unit, n, ts) / TIMEADD(unit, n, ts) -> DATEADD-equivalent",
        "declared", r"\b(?:TIMESTAMPADD|TIMEADD)\s*\(", None,
        "Aliases of DATEADD in Snowflake. Routing them through the DATEADD unit "
        "table would inherit its DATE-return caveat on operands that are "
        "TIMESTAMP or TIME by construction (Spark has no TIME type), so they "
        "are refused until that is decided."),
)


def coverage() -> dict:
    implemented = [r for r in RULES if r.status == "implemented"]
    declared = [r for r in RULES if r.status == "declared"]
    return {
        "total": len(RULES),
        "implemented": len(implemented),
        "declared": len(declared),
        "implemented_rule_ids": [r.rule_id for r in implemented],
        "declared_rule_ids": [r.rule_id for r in declared],
    }


def _detected(rule: TranslationRule, sql: str) -> bool:
    # A construct rule is detected over CODE only. Otherwise a row containing
    # the text "QUALIFY", or a JSON-ish literal like '{"a": 1}' matching the
    # VARIANT path rule, blocks a view that has no such construct in it. A
    # quoting rule is detected over the ident / string segments it is about.
    if rule.scope == "code":
        return bool(lexer.find_code(rule.detect, sql))
    return any(kind == rule.scope
               and re.search(rule.detect, text, re.IGNORECASE | re.DOTALL)
               for kind, text in lexer.segments(sql))


def translate_sql(sql: str) -> TranslationResult:
    """Apply every implemented rule; report every declared one that matches."""
    result = TranslationResult(sql=sql)
    for rule in RULES:
        if not _detected(rule, result.sql):
            continue
        if rule.status == "declared" or rule.translate is None:
            result.unsupported.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": rule.detail or rule.description})
            continue
        try:
            new_sql, problem, *rest = rule.translate(result.sql)
        except lexer.UnterminatedLiteral as exc:
            # A rule that produced SQL the lexer cannot read has a bug. That is
            # a per-view refusal naming the rule, not a stage-wide exit 1 that
            # names nothing.
            result.unsupported.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": f"translator rule {rule.rule_id} produced SQL the "
                          f"lexer cannot read ({exc}); left untranslated"})
            continue
        if problem:
            result.unsupported.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": problem})
            continue
        if rule.scope == "code" and lexer.find_code(rule.detect, new_sql):
            # The rule ran and its construct is still there: a form it does
            # not cover. Left in place it would be carried over verbatim and
            # the view stamped portable, so it is a refusal.
            result.unsupported.append({
                "rule_id": rule.rule_id, "construct": rule.construct,
                "detail": f"a {rule.construct} form the rule does not cover "
                          "was left in place; it would otherwise be carried "
                          "over verbatim and reported as portable"})
            continue
        if new_sql != result.sql:
            entry = {"rule_id": rule.rule_id, "construct": rule.construct,
                     "detail": rule.description}
            warnings = list(rest[0]) if rest else []
            # A rewrite that carries a semantic warning -- `::TIMESTAMP`'s
            # timezone semantics -- is exact in shape only, so the warning is
            # a caveat on this application too. R43 counts caveats; without
            # this it called the view "every one is an exact rewrite" next to
            # the warning that says otherwise.
            caveats = ([rule.caveat] if rule.caveat else []) + warnings
            if caveats:
                entry["caveat"] = "; ".join(caveats)
            result.applied.append(entry)
            result.sql = new_sql
            result.warnings.extend(warnings)
    return result
