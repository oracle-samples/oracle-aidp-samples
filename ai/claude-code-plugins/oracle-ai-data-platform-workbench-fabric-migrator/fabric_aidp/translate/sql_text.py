"""Shared SQL text handling: masking for safe scanning, offset-safe rewriting.

Two masking passes, deliberately separate:

  strip_sql_comments   blanks -- and /* */ but KEEPS string literals, because
                       a comment marker inside a literal is not a comment
  mask_literals        blanks literal *contents* but keeps the quotes, because
                       a keyword inside a literal is not that keyword

Both passes skip a *quoted identifier* whole -- T-SQL `[...]` and `"..."`,
Spark `` `...` `` -- because every character inside one, apostrophe and `--`
included, is part of the name.

`"..."` is an identifier here, and that is a dialect decision worth stating.
T-SQL has two modes: under QUOTED_IDENTIFIER ON `"my col"` is a name, under
OFF it is a string literal. ON is the default, is required for filtered
indexes and computed columns, and is what DacFx and Fabric generate under, so
ON is what these passes assume. It was previously masked as a literal, and
the cost was measured: Spark reads `"my col"` as the *text* `my col`, so an
object selecting a double-quoted column returned the column's name instead
of its value, with no finding raised. A file containing
`SET QUOTED_IDENTIFIER OFF` is flagged by the translator rather than modelled
here; a pure text pass cannot know a mode, and guessing it would change
values silently. Missing that was a silent, total failure:
`SELECT [it's], c FROM dbo.t` opened a literal that never closed, so every
rule scanning the masked copy read the rest of the statement as literal text.
The measured result was the statement returned unchanged with NO finding --
graded PASS, with `dbo.t` still naming a different table in Spark.

`masked()` composes both and is what every rule should scan. Both passes
preserve length and newlines, so an offset found in the masked copy is valid
in the original.
"""
from __future__ import annotations

import re

# opener -> closer. Inside any of them, the closer doubled is an escaped
# closer: `[a]]b]` is the one identifier `a]b`, `"a""b"` is `a"b`, and
# `` `a``b` `` is `` a`b ``, which is exactly the spelling SQ10 emits.
#
# `"` is the one form that differs between the dialects, so every pass takes
# a `quoted_identifier` flag rather than the module picking a side. The
# default is False -- Spark's reading -- and that is a deliberate choice, not
# an accident of ordering: this is a shared utility, Spark is the tool's
# target dialect, and the two mistakes are not equally bad. Masking a
# literal's body can only *suppress* a rewrite; exposing an identifier's body
# can *corrupt* one. Measured, when the notebook SQL-cell path inherited
# T-SQL's reading by omission:
#
#   in:  SELECT "text FROM dbo.claim" AS c FROM dbo.claim
#   out: SELECT "text FROM default.Sales.claim" AS c FROM default.Sales.claim
#
# -- a table name rewritten inside a user's string literal, reported as two
# ordinary NB10_TABLE_REF rewrites. T-SQL therefore opts in explicitly.
_IDENTIFIER_QUOTES = {"[": "]", "`": "`"}
_TSQL_IDENTIFIER_QUOTES = {**_IDENTIFIER_QUOTES, '"': '"'}
_LITERAL_QUOTES = "'\""
_TSQL_LITERAL_QUOTES = "'"


def _dialect(quoted_identifier: bool):
    """(identifier openers, literal openers) for the requested reading."""
    if quoted_identifier:
        return _TSQL_IDENTIFIER_QUOTES, _TSQL_LITERAL_QUOTES
    return _IDENTIFIER_QUOTES, _LITERAL_QUOTES


# Content that says "the closer I just found is probably not mine". A newline
# or a block-comment marker inside a quoted identifier means the search has
# most likely walked out of the code and into a comment, where a stray `]`
# lives. Measured, with the search unbounded:
#
#   SELECT [a\n-- ] closes here\nFROM dbo.t
#     -> SELECT `a\n-- ` closes here\nFROM default.W.t   flags=0  PASS
#   SELECT [a /* ] */ FROM dbo.t
#     -> SELECT `a /* ` */ FROM default.W.t               flags=0  PASS
#
# -- an identifier built out of comment text, the second leaving a dangling
# `*/` Spark rejects, both graded PASS. A name that really does contain a
# newline or `/*` is legal T-SQL and essentially never written; an
# unterminated `[` from a typo or a truncation is not. So the shape is
# refused, which makes it an open identifier that SQ01 flags.
#
# `--` alone is deliberately NOT here: T-SQL's lexer reads `[` before `--`,
# so `SELECT [a -- ] here FROM t` really is the name `a -- ` with `here` as
# an alias, and the backquoted form runs on Spark. Measured.
_NOT_IN_AN_IDENTIFIER = ("\n", "\r", "/*", "*/")


def _quoted_identifier_end(sql: str, index: int, quotes=None):
    """Index just past the quoted identifier opening at `index`, or None.

    None means the opener is never closed *as an identifier*: either no closer
    follows, or the only candidate sits past a boundary an identifier does not
    cross (see `_NOT_IN_AN_IDENTIFIER`). The caller then treats the opener as
    an ordinary character, deliberately: consuming the rest of the text would
    blind every rule to it, which is the failure this module exists to avoid.
    """
    closer = (quotes or _IDENTIFIER_QUOTES)[sql[index]]
    position = index + 1
    while True:
        position = sql.find(closer, position)
        if position == -1:
            return None
        if sql.startswith(closer * 2, position):
            position += 2
            continue
        body = sql[index + 1:position]
        if any(marker in body for marker in _NOT_IN_AN_IDENTIFIER):
            return None
        return position + 1


def _literal_end(sql: str, index: int):
    """(index just past the literal opening at `index`, was it closed?).

    A doubled quote inside is an escaped quote. An unterminated literal ends
    at the end of the text -- pretending the quote was not there would be
    worse -- and says so, because a caller has to be able to report it.
    """
    quote = sql[index]
    position, length = index + 1, len(sql)
    while position < length:
        if sql[position] == quote:
            if sql.startswith(quote * 2, position):
                position += 2
                continue
            return position + 1, True
        position += 1
    return length, False


def _quoted_spans(sql: str, quoted_identifier: bool = False) -> list:
    """(start, end, kind, closed) for literals and quoted identifiers, in order.

    One scanner, so the two views cannot disagree about which is which: a
    quote inside `[...]` is a letter in a name, and a `[` inside a literal is
    text. `end` is one past the closer. An unterminated identifier opener is
    no span at all, see `_quoted_identifier_end`, so `closed` is only ever
    False for a literal.
    """
    quotes, literals = _dialect(quoted_identifier)
    spans, index, length = [], 0, len(sql)
    while index < length:
        char = sql[index]
        if char in quotes:
            end = _quoted_identifier_end(sql, index, quotes)
            if end is not None:
                spans.append((index, end, "identifier", True))
                index = end
                continue
        if char in literals:
            end, closed = _literal_end(sql, index)
            spans.append((index, end, "literal", closed))
            index = end
            continue
        index += 1
    return spans


def _comment_spans(sql: str, quoted_identifier: bool = False) -> list:
    """(start, end, closed) of every comment, in order.

    Literals and quoted identifiers are skipped, because a comment marker
    inside either is text: `'-- x'` is a string and `[a -- b]` is a name. A
    `--` comment is closed by its newline or by the end of the text, which is
    legal SQL; only `/* ... */` can be left genuinely open.
    """
    quotes, literals = _dialect(quoted_identifier)
    spans, index, length = [], 0, len(sql)
    while index < length:
        char = sql[index]
        if char in quotes:
            end = _quoted_identifier_end(sql, index, quotes)
            if end is not None:
                index = end
                continue
        if char in literals:
            index = _literal_end(sql, index)[0]
            continue
        if char == "-" and sql.startswith("--", index):
            newline = sql.find("\n", index)
            end = length if newline == -1 else newline
            spans.append((index, end, True))
            index = end
            continue
        if char == "/" and sql.startswith("/*", index):
            closer = sql.find("*/", index + 2)
            end = length if closer == -1 else closer + 2
            spans.append((index, end, closer != -1))
            index = end
            continue
        index += 1
    return spans


def string_literal_spans(sql: str, *, quoted_identifier: bool = False) -> list:
    """(start, end) of every string literal; `end` is one past the closer."""
    return [(start, end)
            for start, end, kind, _closed in _quoted_spans(sql, quoted_identifier)
            if kind == "literal"]


def unterminated_span(sql: str, *, quoted_identifier: bool = False):
    """(kind, offset) of a construct left open at end of input, or None.

    `kind` is "literal", "identifier" or "comment"; `offset` points at the
    opener in `sql`. Read-only -- the masking passes keep their signatures and
    their length-preserving contract -- because "the masking ran off the end"
    is invisible in the masked copy: the rest of the object simply looks like
    literal or comment text, so every rule rewrites nothing, raises nothing,
    and the object grades PASS. Measured:

      SELECT 'abc FROM dbo.t   -> unchanged, findings=[], PASS
      SELECT [a]]              -> unchanged, findings=[], PASS
      SELECT 1 /* x FROM dbo.t -> unchanged, findings=[], PASS

    In the first, `dbo.t` is inside what the masker reasonably reads as a
    string, so SQ11 never sees it and an unqualified name ships as checked.
    None of this is hypothetical: a truncated file, a bad encoding or a
    partial export produces exactly this shape.
    """
    if not isinstance(sql, str):
        return None
    found = []
    for start, _end, closed in _comment_spans(sql, quoted_identifier):
        if not closed:
            found.append(("comment", start))
            break
    # Comments are blanked before looking for quotes, so the apostrophe in
    # `-- don't` is prose rather than an unterminated literal.
    stripped = strip_sql_comments(sql, quoted_identifier=quoted_identifier)
    spans = _quoted_spans(stripped, quoted_identifier)
    for start, _end, kind, closed in spans:
        if kind == "literal" and not closed:
            found.append(("literal", start))
            break
    # An unterminated identifier opener is no span at all -- the scanner
    # declines it and treats it as an ordinary character on purpose -- so it
    # is whichever opener is left uncovered by the spans.
    position, next_span = 0, 0
    while position < len(stripped):
        if next_span < len(spans) and spans[next_span][0] == position:
            position = spans[next_span][1]
            next_span += 1
            continue
        if stripped[position] in _dialect(quoted_identifier)[0]:
            found.append(("identifier", position))
            break
        position += 1
    return min(found, key=lambda item: item[1]) if found else None


def quoted_identifier_spans(sql: str, *, quoted_identifier: bool = False) -> list:
    """(start, end) of every quoted identifier -- `[...]` and `` `...` ``.

    A rule that scans for a keyword needs this: `masked()` blanks literal
    bodies but deliberately keeps identifier bodies, so the word TOP inside
    `` `TOP secret` `` looks exactly like the keyword and SQ50 reported a
    column name as an unsupported row count. The quoting is known here, so
    asking is cheaper and safer than each rule guessing.
    """
    return [(start, end)
            for start, end, kind, _closed in _quoted_spans(sql, quoted_identifier)
            if kind == "identifier"]


def strip_sql_comments(sql: str, *, quoted_identifier: bool = False) -> str:
    """Replace comment characters with spaces, preserving offsets and literals.

    Offsets are preserved so a match position in the stripped text is valid in
    the original.
    """
    if not isinstance(sql, str):
        return ""
    out = list(sql)
    for start, end, _closed in _comment_spans(sql, quoted_identifier):
        for position in range(start, end):
            if out[position] != "\n":
                out[position] = " "
    return "".join(out)


def mask_literals(sql: str, *, quoted_identifier: bool = False) -> str:
    """Blank the *contents* of string literals, preserving quotes and offsets.

    Structural scanning needs this: `EXEC('CREATE TABLE x')` defines no table,
    and classifying it as one would invent an asset the warehouse does not have.
    `strip_sql_comments` deliberately keeps literals intact, so the two passes
    compose rather than duplicate.

    Only `'...'` is a literal. A `"..."` body is a *name* and stays visible,
    for the same reason a `[...]` body does: a rule that cannot see the name
    cannot rewrite it.
    """
    if not isinstance(sql, str):
        return ""
    out = list(sql)
    for start, end, kind, closed in _quoted_spans(sql, quoted_identifier):
        if kind != "literal":
            continue
        # Keep the delimiters so a rule can still see where a literal begins
        # and ends. An unterminated literal has no closer to keep, so its
        # body runs to `end`.
        for position in range(start + 1, end - 1 if closed else end):
            if out[position] != "\n":
                out[position] = " "
    return "".join(out)



def masked(sql: str, *, quoted_identifier: bool = False) -> str:
    """The standard scanning view: no comments, no literal bodies, same offsets.

    `quoted_identifier` picks the dialect's reading of `"..."`; see the note
    above `_IDENTIFIER_QUOTES` for why the default is Spark's.
    """
    return mask_literals(strip_sql_comments(sql, quoted_identifier=quoted_identifier),
                         quoted_identifier=quoted_identifier)


# One name part: a bracketed, double-quoted, backticked or plain identifier.
_REF_IDENT = r"(?:\[[^\]]+\]|\"[^\"]+\"|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)"
# `WITH cte AS (` and `, cte AS (`. A common table expression is a name that
# lives for one statement, so a real object of the same name is not what the
# query reads and an edge to it would be invented out of a coincidence.
_CTE_RE = re.compile(r"(?:\bWITH\b|,)\s*(?P<name>" + _REF_IDENT
                     + r")\s*(?:\([^)]*\)\s*)?AS\s*\(", re.IGNORECASE)


def _unquote_part(part: str) -> str:
    part = part.strip()
    for opener, closer in (("[", "]"), ('"', '"'), ("`", "`")):
        if len(part) >= 2 and part[0] == opener and part[-1] == closer:
            return part[1:-1]
    return part


def referenced_tables(sql, *, keywords, quoted_identifier: bool = False) -> list:
    """The objects `sql` names after any of `keywords`, in order, as written.

    Used for dependency edges, so the question is "what has to exist
    already", and the answer is deliberately conservative. Four things are
    not references:

      a derived table          FROM (SELECT ...)
      a function call          FROM STRING_SPLIT(...)   -- a `(` follows
      a temp table or variable #t, @t -- neither starts an identifier here
      a common table expression

    Scanned over `masked`, so a name in a comment or inside a string
    literal is not a reference. Callers choose their own `keywords`
    because the question differs: a warehouse object has to be created
    after everything it writes as well as everything it reads, while a
    notebook's *reads* are FROM and JOIN and its writes come from
    `saveAsTable` and `CREATE TABLE` instead.
    """
    if not isinstance(sql, str) or not sql:
        return []
    view = masked(sql, quoted_identifier=quoted_identifier)
    pattern = re.compile(
        r"\b(?:" + "|".join(keywords) + r")\s+(?P<name>" + _REF_IDENT
        + r"(?:\s*\.\s*" + _REF_IDENT + r"){0,3})", re.IGNORECASE)
    ctes = {_unquote_part(m.group("name")).casefold()
            for m in _CTE_RE.finditer(view)}
    found = []
    for match in pattern.finditer(view):
        if view[match.end():match.end() + 1] == "(":
            continue
        parts = [part for part in
                 (_unquote_part(p) for p in match.group("name").split("."))
                 if part]
        if not parts or parts[-1].casefold() in ctes:
            continue
        name = ".".join(parts)
        if name not in found:
            found.append(name)
    return found


def apply_replacements(sql: str, replacements) -> str:
    """Apply (start, end, text) edits to `sql`.

    Applied right-to-left so an earlier edit never invalidates a later offset.
    Overlapping ranges are a programming error in a rule and raise rather than
    silently producing mangled SQL.
    """
    ordered = sorted(replacements, key=lambda item: (item[0], item[1]))
    previous_end = None
    for start, end, _text in ordered:
        if start > end:
            raise ValueError(f"replacement start {start} is after end {end}")
        if previous_end is not None and start < previous_end:
            raise ValueError(
                f"overlapping replacements: {start}-{end} overlaps a range "
                f"ending at {previous_end}")
        previous_end = end
    for start, end, text in reversed(ordered):
        sql = sql[:start] + text + sql[end:]
    return sql


def split_call_arguments(view: str, open_index: int):
    """Top-level argument spans of the call whose '(' is at `open_index`.

    `view` must be a masked copy, so commas inside string literals do not split.
    Returns (spans, close_index), or None for an unterminated call — consuming
    the rest of the statement as an argument list is worse than declining.
    """
    depth = 0
    spans = []
    start = open_index + 1
    for index in range(open_index, len(view)):
        char = view[index]
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
            if depth == 0:
                spans.append((start, index))
                return spans, index
        elif char == "," and depth == 1:
            spans.append((start, index))
            start = index + 1
    return None


# The SQL-standard functions whose argument list is separated by the keyword
# FROM rather than by a comma: `TRIM([LEADING|TRAILING|BOTH] chars FROM str)`,
# `EXTRACT(field FROM ts)`, `SUBSTRING(str FROM pos [FOR len])`,
# `OVERLAY(str PLACING x FROM pos [FOR len])`. Spark SQL accepts all four, so
# a migrated notebook or view may contain any of them.
#
# Here rather than in either translator because both of them anchor a table
# rule on the keyword FROM and both got this wrong -- separately. A general
# "is this paren a function call" test was tried first in the T-SQL rule and
# is worse: the paren of a derived table, `FROM (SELECT ...)`, is also
# preceded by a word, so the heuristic swallowed real tables inside
# subqueries. A short, explicit list of the functions that actually do this
# is both correct and reviewable.
FROM_TAKING_CALLS = frozenset({"trim", "extract", "substring", "overlay"})
_CALL_NAME_RE = re.compile(r"([A-Za-z_][\w$]*)\s*$")


def in_a_from_taking_call(view, index) -> bool:
    """Is `index` inside the parentheses of a `TRIM(... FROM ...)`-style call?

    `view` must be a masked copy, so a bracket inside a literal or a comment
    is already blanked and only real ones are counted.

    Without this a rule anchored on FROM reads `TRIM(BOTH ' ' FROM d.name)`
    as a table source. Measured on both carriers: the T-SQL two-part rule
    rewrote the *column* `d.name` into a reference to a schema that does not
    exist, as a `rewrite` with no flag, so it graded PASS; the notebook
    rule's `_resolved_table_name` looked `name` up, missed, and emitted
    NB12_TABLE_UNKNOWN for a column, which grades the cell REVIEW.
    """
    depth = 0
    for position in range(index - 1, -1, -1):
        char = view[position]
        if char == ")":
            depth += 1
        elif char == "(":
            if depth:
                depth -= 1
                continue
            name = _CALL_NAME_RE.search(view[:position])
            return bool(name) and name.group(1).casefold() in FROM_TAKING_CALLS
    return False


# One entry of a `WITH` clause: the name, an optional column list, `AS`, an
# optional Spark/Postgres materialisation hint, and the opening paren of the
# body. Applied from a `WITH` keyword and then from each comma that follows a
# complete entry, so a bare `x AS (` elsewhere in the statement is never read
# as a CTE.
_CTE_ITEM_RE = re.compile(
    r"\s*(?:RECURSIVE\s+)?(?P<name>[A-Za-z_][\w$]*)\s*"
    r"(?:\([^()]*\)\s*)?AS\s*(?:(?:NOT\s+)?MATERIALIZED\s+)?\(",
    re.IGNORECASE)
_WITH_RE = re.compile(r"\bWITH\b", re.IGNORECASE)


def cte_names(view) -> set:
    """Every name a `WITH` clause in `view` defines, casefolded.

    `view` must be a masked copy. A CTE name is not a catalog table: it names
    a result set that exists only for the statement that declares it, so a
    rule resolving `FROM <name>` against a catalog has to skip it. Without
    this, `WITH recent AS (SELECT * FROM claim) SELECT * FROM recent` resolved
    `claim` correctly and then reported NB12_TABLE_UNKNOWN for `recent`,
    which grades an otherwise clean notebook REVIEW.

    Nested `WITH` clauses inside a subquery are collected too; their names are
    equally not tables, and a name collected from a scope that has closed can
    only suppress a rewrite, never invent one.
    """
    names = set()
    if not isinstance(view, str):
        return names
    for head in _WITH_RE.finditer(view):
        position = head.end()
        while True:
            item = _CTE_ITEM_RE.match(view, position)
            if not item:
                break
            names.add(item.group("name").casefold())
            body = split_call_arguments(view, item.end() - 1)
            if body is None:
                break
            position = body[1] + 1
            while position < len(view) and view[position].isspace():
                position += 1
            if position >= len(view) or view[position] != ",":
                break
            position += 1
    return names


# ---------------------------------------------------------------------------
# Column references
#
# The catalog has carried a `columns` list since it landed and nothing read
# it. Reading it needs the other half: which columns a statement references.
# That is harder than which TABLES it references, because a table name sits
# in a grammatically marked position (after FROM/JOIN/UPDATE) and a column
# does not -- an identifier in a select list may be a column, a function
# name, a date part, an alias, a type name or a keyword.
#
# So this answers only where it can be certain, and returns None rather than
# a guess. The measurement that set the bar is the one non-trivial named-
# column view in the bundled estate:
#
#   SELECT TOP 100
#          [claim id],
#          ISNULL(policy_no, 'unknown') AS policy_no,
#          DATEDIFF(day, opened, GETDATE()) AS age_days,
#          IIF(is_open = 1, 'open', 'closed') AS status
#   FROM dbo.claim
#   ORDER BY opened
#
# `dbo.claim` declares exactly `claim id, policy_no, opened, is_open`, so the
# right answer is "every reference is declared". A naive extractor reports
# `day` as an undeclared column -- a false positive on one of the only two
# checkable statements that exist here. `ISNULL`, `DATEDIFF`, `GETDATE` and
# `IIF` are calls; `policy_no`, `age_days` and `status` in the AS positions
# are aliases, not references; `TOP` and `100` are neither.

# T-SQL date parts. A bare word in the first argument of DATEDIFF & co, and
# not a column.
#
# Excluded ONLY in that position, never globally. `year`, `month`, `day` and
# `hour` are ordinary column names, and a blanket exclusion would make every
# column so named permanently uncheckable -- a blind spot wider than the
# false positive it avoids.
_DATE_PARTS = frozenset({
    "year", "yy", "yyyy", "quarter", "qq", "q", "month", "mm", "m",
    "dayofyear", "dy", "y", "day", "dd", "d", "week", "wk", "ww",
    "weekday", "dw", "w", "hour", "hh", "minute", "mi", "n",
    "second", "ss", "s", "millisecond", "ms", "microsecond", "mcs",
    "nanosecond", "ns", "tzoffset", "tz", "iso_week", "isowk", "isoww",
})
_DATE_PART_CALLS = frozenset({
    "datediff", "datediff_big", "dateadd", "datepart", "datename",
    "datetrunc",
})

# Words that can stand where a column stands and are not one. Type names are
# absent on purpose: they occur after AS (`CAST(x AS int)`), which is already
# the alias rule, and adding `int`/`date`/`text` here would silently stop
# checking columns with those perfectly ordinary names.
_NOT_A_COLUMN = frozenset({
    "select", "distinct", "all", "top", "percent", "ties", "as", "from",
    "where", "group", "by", "order", "having", "asc", "desc", "and", "or",
    "not", "null", "is", "in", "like", "between", "exists", "case", "when",
    "then", "else", "end", "over", "partition", "rows", "range", "unbounded",
    "preceding", "following", "current", "row", "collate", "with", "into",
    "union", "except", "intersect", "on", "offset", "fetch", "next", "only",
    "escape", "some", "any", "true", "false",
    # Niladic functions, written without parentheses. Each was reported as an
    # undeclared column (SQ23), pushing a clean view to REVIEW.
    "current_timestamp", "current_date", "current_time", "current_user",
    "session_user", "system_user", "user",
})

# Multi-word constructs whose words stand where a column could. Each is
# matched as the whole sequence, so a real column called `time`, `first` or
# `value` is still checked everywhere else. Every one was MEASURED reporting
# its words as undeclared columns of a declared table (SQ23).
_NOT_A_COLUMN_SEQUENCES = re.compile(
    r"\bAT\s+TIME\s+ZONE\b|\bWITHIN\s+GROUP\b|\bGROUPING\s+SETS\b"
    r"|\bWITH\s+(?:ROLLUP|CUBE)\b|\bFETCH\s+(?:FIRST|NEXT)\b"
    r"|\bNEXT\s+VALUE\s+FOR\s+" + r"(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z_]\w*)(?:\s*\.\s*(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z_]\w*))*"
    r"|\bCOLLATE\s+[A-Za-z_]\w*"
    r"|\bUSING\b",
    re.IGNORECASE)
# Calls whose first argument is a type, not an expression. CAST's type sits
# after AS and is already skipped by the alias rule.
_TYPE_FIRST_CALLS = frozenset({"convert", "try_convert", "identity"})
_TOP_PREFIX = re.compile(
    r"^\s*(?:(?:DISTINCT|ALL)\s+)?(?:TOP\s*(?:\(\s*[^)]*\)|\d+)\s*(?:PERCENT\s+)?(?:WITH\s+TIES\s+)?)?",
    re.IGNORECASE)
_ALIAS_ASSIGNMENT_HEAD = re.compile(
    r"^\s*(\[[^\]]+\]|\"[^\"]+\"|`[^`]+`|[A-Za-z_]\w*)\s*=(?![=<>])")

# The only things allowed to follow the FROM target. A whitelist, because the
# blacklist it replaced let a comma join through. `OPTION` and `FOR` end a
# statement without holding column references; the rest do hold them and are
# scanned.
_TAIL_CLAUSE = re.compile(
    r"(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|OPTION\b|FOR\b)",
    re.IGNORECASE)

# ...and the ones that hold NO column references, so the scan stops at them.
# MEASURED after the whitelist above first admitted them and then scanned
# them: `FROM dbo.t OPTION (RECOMPILE)` reported `RECOMPILE` as a column and
# `FROM dbo.t FOR JSON PATH` reported `FOR`, `JSON` and `PATH`. Allowed to
# follow the table, never read for columns.
_TAIL_STOP = re.compile(r"\b(?:OPTION|FOR)\b", re.IGNORECASE)

_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)
_FROM_RE = re.compile(r"\bFROM\b", re.IGNORECASE)
# Not mid-number: `1e5` read `e5` and `0x1F` read `x1F` as columns.
_WORD_RE = re.compile(
    r"(\[[^\]]+\]|\"[^\"]+\"|`[^`]+`|(?<![0-9$#\w])@?[A-Za-z_][A-Za-z0-9_$#]*)")


def _top_level_split(text: str) -> list:
    """`text` cut at depth-0 commas, as (start, item) pairs."""
    out, depth, start = [], 0, 0
    for position, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            out.append((start, text[start:position]))
            start = position + 1
    out.append((start, text[start:]))
    return out


def _select_aliases(select_list: str):
    """(aliases, blanked) for one SELECT list.

    `aliases` is every output-column name the list defines -- `expr AS a`,
    `expr a` (AS is optional in T-SQL) and `a = expr` (T-SQL's assignment
    form) -- casefolded. `blanked` is the list with the alias words of the
    last two forms replaced by spaces, so the column scan never reads them.
    MEASURED before this: `SELECT policy_no p`, `ROW_NUMBER() OVER (...) rn`
    and `SELECT p = policy_no` each reported the alias as an undeclared
    column of a declared table.
    """
    aliases, blanks = set(), []
    for index, (offset, item) in enumerate(_top_level_split(select_list)):
        body_start = _TOP_PREFIX.match(item).end() if index == 0 else 0
        body = item[body_start:]
        head = _ALIAS_ASSIGNMENT_HEAD.match(body)
        if head:
            aliases.add(_unquote_part(head.group(1)).casefold())
            start = offset + body_start + head.start(1)
            blanks.append((start, start + len(head.group(1))))
            continue
        words = list(_WORD_RE.finditer(body))
        if not words:
            continue
        last = words[-1]
        if body[last.end():].strip():
            continue                                   # something follows it
        before = body[:last.start()].rstrip()
        if not before:
            continue                                   # a lone column
        previous = before[-1]
        trailing = list(_WORD_RE.finditer(before))
        prev_text = trailing[-1].group(1) if trailing and trailing[-1].end() == len(before) else ""
        if prev_text and _unquote_part(prev_text).casefold() == "as":
            aliases.add(_unquote_part(last.group(1)).casefold())
            continue
        if previous in "+-*/%,(=<>|&^~.!":
            continue                                   # an operand, not an alias
        if prev_text and _unquote_part(prev_text).casefold() in _NOT_A_COLUMN - {"end"}:
            continue                                   # `DISTINCT a`, `NOT a`, ...
        aliases.add(_unquote_part(last.group(1)).casefold())
        start = offset + body_start + last.start()
        blanks.append((start, start + len(last.group(1))))
    blanked = select_list
    for start, end in blanks:
        blanked = blanked[:start] + " " * (end - start) + blanked[end:]
    return aliases, blanked


def _depth_at(view: str, index: int) -> int:
    return view.count("(", 0, index) - view.count(")", 0, index)


def referenced_columns(sql, *, quoted_identifier: bool = False):
    """The columns one single-table SELECT references, or None.

    `None` means "this statement is not one I can read columns out of with
    certainty", and it is the answer for every shape below. A caller must not
    read it as "no columns referenced" -- see `undeclared_columns`, which
    keeps the same three-state contract for the same reason.

    Refused, each because getting it wrong invents a column that is not
    there or misses one that is:

      no SELECT, or no FROM        nothing to attribute to a table
      more than one SELECT/FROM    a subquery or a derived table: the
                                   columns belong to different scopes and
                                   this cannot say which
      a CTE                        `cte_names` already exists because NB12
                                   resolved CTE names as real tables (#27);
                                   a CTE's columns are not a catalog table's
      JOIN/UNION/APPLY/PIVOT       more than one table in scope, so an
                                   undeclared column may be perfectly
                                   declared on the other one
      INSERT/UPDATE/DELETE/MERGE   a different grammar; none appears in any
                                   fixture here, so supporting them would be
                                   code with no evidence behind it
      SELECT *                     names no columns

    Returns `(table, columns)` where `table` is the FROM target as written
    and `columns` is each referenced column in source order, de-duplicated
    on the folded name, in the spelling the statement used -- so a finding
    can quote the text a reader will find in the file.
    """
    if not isinstance(sql, str) or not sql.strip():
        return None
    view = masked(sql, quoted_identifier=quoted_identifier)
    # A keyword blacklist (JOIN|UNION|CTE|MERGE|INSERT|...) stood here and was
    # removed as a NET LOSS, not merely as redundancy. MEASURED with it
    # disabled, every shape it named is refused anyway, by the two structural
    # guards below:
    #
    #   a CTE, a subquery, a UNION   two SELECTs -> the count guard
    #   any JOIN, APPLY, PIVOT       a tail that is not a clause keyword
    #   UPDATE/DELETE/MERGE, and
    #     INSERT ... VALUES          no SELECT at all
    #
    # Its one remaining effect was to refuse `INSERT INTO t (a, b) SELECT c, d
    # FROM u`, whose unguarded answer is ('u', ('c', 'd')) -- which is right:
    # `c` and `d` do come from `u`. So the blacklist bought nothing and cost
    # one correct answer. Structure decides this, not a word list.
    selects = _SELECT_RE.findall(view)
    froms = [m for m in _FROM_RE.finditer(view)
             if not in_a_from_taking_call(view, m.start())]
    if len(selects) != 1 or len(froms) != 1:
        return None
    select = _SELECT_RE.search(view)
    from_match = froms[0]
    if from_match.start() < select.end():
        return None

    # The FROM target: one name, then the clause ends. An alias makes the
    # bare column names ambiguous to attribute, so it is refused too.
    tail = view[from_match.end():]
    name_match = re.match(r"\s*(" + _REF_IDENT + r"(?:\." + _REF_IDENT
                          + r"){0,2})", tail)
    if not name_match:
        return None
    # Whatever follows the table name must be the end of the statement or a
    # clause keyword. Anything else means a bare column cannot be attributed
    # to this table with certainty, so this refuses. Written as a whitelist
    # after the first draft was a blacklist that let `FROM dbo.t, dbo.u`
    # through: MEASURED, it answered ('dbo.t', ('a', 'u')) -- it accepted a
    # two-table query AND reported the second TABLE as a column of the first.
    # A comma join carries no JOIN keyword, so `_UNSAFE_SHAPES` never sees it.
    after = tail[name_match.end():].strip()
    if after and not _TAIL_CLAUSE.match(after):
        return None
    table = name_match.group(1)
    # `*` is deliberately NOT a refusal. It is not an identifier, so it is
    # never collected, and every column of the table it expands to is
    # declared by construction -- there is nothing undeclared about
    # `SELECT *`. Refusing it would call an answerable statement unanswerable,
    # and `SELECT t.*, misspelt FROM t` would stop being checked at all.
    stop = _TAIL_STOP.search(after)
    scanned = after[:stop.start()] if stop else after
    select_list = view[select.end():from_match.start()]
    # `SELECT ... INTO dbo.x FROM ...`: the INTO target is a table being
    # created, not a column. MEASURED: it was reported as one ('x', 'newt').
    into = re.search(r"\bINTO\b", select_list, re.IGNORECASE)
    if into and _depth_at(select_list, into.start()) == 0:
        select_list = select_list[:into.start()]
    aliases, select_list = _select_aliases(select_list)
    # ORDER BY (the tail) may name an output column by its alias. Only the
    # tail is filtered: in the list itself an alias is already skipped, and
    # `coalesce(policy_no, 'x') AS policy_no` still references policy_no.
    head = _columns_in(select_list)
    folded = {_unquote_part(word).casefold() for word in head}
    tail = tuple(word for word in _columns_in(scanned)
                 if _unquote_part(word).casefold() not in aliases | folded)
    return (table, head + tail)


def _columns_in(text):
    """Column spellings in `text`, in order, de-duplicated on the fold.

    `text` is the MASKED view, so a column name inside a string literal or a
    comment cannot become a reference -- verified: `SELECT \'opened\' FROM
    dbo.t` and `SELECT a -- opened` both report no `opened`.
    """
    out, seen = [], set()
    skip_until = -1
    # Constructs whose words are not columns, blanked as whole sequences.
    text = _NOT_A_COLUMN_SEQUENCES.sub(lambda m: " " * len(m.group(0)), text)
    words = list(_WORD_RE.finditer(text))
    for index, match in enumerate(words):
        if match.start() < skip_until:
            continue
        word = match.group(1)
        folded = _unquote_part(word).casefold()
        rest = text[match.end():]
        quoted = word[:1] in "[\"`"

        # `N'x'`: the N is a literal's prefix (SQ13 strips it later), and
        # `{d '2020-01-01'}`: an ODBC escape's type letter. Neither is a column.
        if not quoted and folded == "n" and rest[:1] == "'":
            continue
        if text[:match.start()].rstrip().endswith("{"):
            continue
        # A call: the name is not a column, and if it takes a date part the
        # first argument is not one either.
        if re.match(r"\s*\(", rest):
            if folded in _DATE_PART_CALLS:
                first = re.match(r"\s*\(\s*(" + _REF_IDENT + r")", rest)
                if first and _unquote_part(first.group(1)).casefold() in _DATE_PARTS:
                    skip_until = match.end() + first.end()
            elif folded in _TYPE_FIRST_CALLS:
                # CONVERT(int, x), TRY_CONVERT(DATE, x), IDENTITY(INT, 1, 1):
                # the first argument is a type. MEASURED reported as columns.
                first = re.match(r"\s*\(\s*(" + _REF_IDENT + r")\s*(?:\([^)]*\))?", rest)
                if first:
                    skip_until = match.end() + first.end()
            elif index and _unquote_part(words[index - 1].group(1)).casefold() == "as":
                # `AS VARCHAR(MAX)`, `AS DECIMAL(19, 4)`: a type's own
                # arguments. MEASURED: `MAX` was reported as a column.
                close = rest.find(")")
                if close != -1:
                    skip_until = match.end() + close + 1
            continue
        # An alias, not a reference.
        if index and _unquote_part(words[index - 1].group(1)).casefold() == "as":
            continue
        if word.startswith("@"):          # a variable
            continue
        if not quoted and folded in _NOT_A_COLUMN:
            continue
        # A dotted reference: the column is the last part.
        if re.match(r"\s*\.", rest):
            continue
        if folded in seen:
            continue
        seen.add(folded)
        out.append(word)
    return tuple(out)
