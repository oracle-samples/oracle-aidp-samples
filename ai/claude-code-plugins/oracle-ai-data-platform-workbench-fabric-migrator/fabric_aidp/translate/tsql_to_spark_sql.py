"""Translate Warehouse T-SQL to Spark SQL.

This header used to read "Plan 1 ships NO rewrite rules -- those arrive in
Plan 3. Until then every object is flagged", and listed SQ02 as "no rewrite
rules yet". Plan 3 landed: `RULES` below holds the rewrite rules, SQ02 is
the GO-batch rule, and the placeholder id is gone. The claim outlived the
thing it described by an entire plan, which is the failure mode this file
spends most of its comments guarding against in SQL.

What is still true, and is the design:

  SQ01  the object uses a construct with no Spark form at all -> refused
        whole, nothing rewritten, nothing emitted as if it worked
  SQ10..SQ90  one rule per construct: rewrite where the rewrite is exact,
        flag where it is not, and never quietly leave a difference

Returning T-SQL unchanged with zero findings would let `verify` report PASS
on SQL that is certainly not valid Spark SQL. A shallow translator must fail
loud, not quiet -- and the corollary, which several of the findings below
were filed against, is that a rule which cannot fire is worse than no rule:
it reads as coverage.
"""
from __future__ import annotations

import re
from functools import partial

from fabric_aidp.inventory.catalog import (
    REF_AMBIGUOUS, REF_INFERRED, REF_SHORTCUT, REF_UNKNOWN, classify_reference,
    names_no_tables, owning_item, undeclared_columns,
)
from fabric_aidp.naming import (
    DEFAULT_CATALOG, aidp_table, note_unaddressable_schema, quote_part,
)
from fabric_aidp.translate.sql_text import (
    FROM_TAKING_CALLS, apply_replacements, cte_names, in_a_from_taking_call,
    masked, quoted_identifier_spans, referenced_columns, split_call_arguments,
    string_literal_spans, unterminated_span,
)
from fabric_aidp.translate.types import Finding, TranslationResult

RULESET_COVERAGE = "tsql-v1"

# SQ24: an SQ11 rewrite emitted an AIDP schema -- `<item>[_<schema>]` -- that
# AIDP's metastore refuses to create (`naming.addressable`: [a-z0-9_] after
# case-folding, measured on the cluster 2026-09-30). The name is still
# emitted as the convention builds it -- renaming is the author's call -- and
# the object grades REVIEW instead of PASS: before this, every object in the
# `fabric-data-engineering-ws_on-prem-warehouse-test-wh` Warehouse graded
# PASS with a CREATE TABLE that cannot run. One finding per object.
SCHEMA_NOT_ADDRESSABLE = "SQ24_SCHEMA_NOT_ADDRESSABLE"

# Every scan in this module reads the text as T-SQL, which differs from the
# shared module's default in one place: `"..."` is a quoted identifier here,
# not a string literal (QUOTED_IDENTIFIER ON is T-SQL's default and what
# DacFx and Fabric generate under -- see the note in sql_text). The default
# there is Spark's reading on purpose, so that a caller which forgets can
# only suppress a rewrite rather than corrupt one; this module opts in, once,
# for all twenty-odd rules rather than repeating the flag at each of them.
_masked = partial(masked, quoted_identifier=True)
_literal_spans = partial(string_literal_spans, quoted_identifier=True)
_identifier_spans = partial(quoted_identifier_spans, quoted_identifier=True)
_unterminated = partial(unterminated_span, quoted_identifier=True)

_PROCEDURAL_KINDS = {"procedure", "function"}
# What SQ01 refuses the whole object for. Every construct here was measured
# on Spark 4.2.0 (JAVA_HOME=openjdk@21) before it was added; the nine at the
# bottom of the list were reaching the artifact unchanged with findings=[]
# until this batch, and the tool was claiming to flag "any T-SQL control
# flow" while doing it.
#
#   IF EXISTS (SELECT 1 FROM t) SELECT 1  PARSE_SYNTAX_ERROR at 'IF'
#   BEGIN\n SELECT 1\nEND                 PARSE_SYNTAX_ERROR at end of input
#   THROW 50000, 'x', 1                   PARSE_SYNTAX_ERROR at 'THROW'
#   RAISERROR('x', 16, 1)                 PARSE_SYNTAX_ERROR at 'RAISERROR'
#   WAITFOR DELAY '00:00:05'              PARSE_SYNTAX_ERROR at 'WAITFOR'
#   BEGIN TRANSACTION                     PARSE_SYNTAX_ERROR at end of input
#   COMMIT                                INVALID_STATEMENT_OR_CLAUSE
#   ROLLBACK                              INVALID_STATEMENT_OR_CLAUSE
#   RETURN                                PARSE_SYNTAX_ERROR at 'RETURN'
#
# RETURN is the one worth reading twice. On its own it is a syntax error,
# but `SELECT 1\nRETURN` was **ACCEPTED** -- Spark 4.2.0 reads the word as
# the select item's alias, and the measured result is one column NAMED
# `RETURN` holding 1. A procedural keyword silently became a column name,
# which is worse than a rejection and is why RETURN is refused rather than
# left with a flag.
#
# Two constructs measured alongside these are NOT here, because refusing a
# whole object over them would be over-refusal when only one clause is at
# fault: a join hint (`INNER HASH JOIN`) and `CROSS`/`OUTER APPLY`. They are
# SQ80_JOIN_HINT and SQ80_APPLY, flagged in place exactly like the
# `WITH (NOLOCK)` and `OPTION (...)` hints already were.
#
# `IF` is anchored to the start of a line on purpose: `DROP TABLE IF EXISTS`
# and `CREATE TABLE IF NOT EXISTS` are valid Spark SQL and both put `IF`
# mid-line, while T-SQL's conditional is a statement. Checked against the
# 22 `.sql` files of the demo corpus: `IF` appears in none of them, at line
# start or anywhere else. An object that broke `DROP TABLE` across lines so
# that `IF EXISTS` began one would be refused when it need not be; that is
# the safe direction, and the one that does not silently pass unrunnable
# procedural SQL.
#
# `BEGIN` on its own subsumes BEGIN TRY / BEGIN CATCH / BEGIN TRANSACTION.
# It is a reserved word in T-SQL, so it cannot be a bare column name, and
# the scan runs on a view with identifier bodies blanked, so it cannot be a
# bracketed one either.
#
# `#temp` used to be in this pattern and is not any more -- see
# `rule_temp_table`. It was the reason SQ51_TEMP_TABLE could not fire: the
# gate matched `#\w+`, `translate()` returns before RULES run when the gate
# fires, and the specific message ("use a view or a persisted table") was
# unreachable code pretending to cover the case.
_CONTROL_FLOW_RE = re.compile(
    r"\b(DECLARE\s+@|SET\s+@|BEGIN\b|WHILE\s|GOTO\s|"
    r"CURSOR\b|EXEC\b|EXECUTE\b|RETURN\b|THROW\b|RAISERROR\b|"
    r"WAITFOR\b|COMMIT\b|ROLLBACK\b)"
    r"|(?m:^[ \t]*IF\b(?=[\s(]))",
    re.IGNORECASE,
)
# The constructs SQ01 names in its finding, so the message stays in step
# with the pattern above rather than drifting from it the way the old
# hand-written list did.
_CONTROL_FLOW_NAMED = (
    "DECLARE @ / SET @ / IF / BEGIN...END / BEGIN TRY...CATCH / WHILE / "
    "GOTO / RETURN / THROW / RAISERROR / WAITFOR / "
    "BEGIN TRANSACTION / COMMIT / ROLLBACK / CURSOR / EXEC"
)


# No pattern for a quoted identifier: `[a]]b]` is the one identifier `a]b`,
# and a regex that stops at the first `]` cut it in half and emitted
# `` `a`]b] `` -- measured, and Spark rejects it. The maskers already locate
# these correctly, escape and all, so this rule asks them.
#
# opener -> (closer, ident id, type id, empty id). `"..."` is here because
# QUOTED_IDENTIFIER ON is T-SQL's default and what Fabric exports are
# generated under, while Spark reads `"my col"` as the *text* `my col`: the
# measured cost of leaving it was an object that returned a column's name
# instead of its value, graded PASS with zero findings.
_QUOTED_IDENT_FORMS = {
    "[": ("]", "SQ10_BRACKET_IDENT", "SQ10_BRACKET_TYPE", "SQ10_BRACKET_EMPTY"),
    '"': ('"', "SQ10_DQUOTE_IDENT", "SQ10_DQUOTE_TYPE", "SQ10_DQUOTE_EMPTY"),
}
_QUOTED_IDENTIFIER_OFF_RE = re.compile(
    r"\bSET\s+QUOTED_IDENTIFIER\s+OFF\b", re.IGNORECASE)
_IDENT_PART = r"(?:`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)"
_DOTTED_RE = re.compile(
    rf"(?<![\w.`\]]){_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{2,}}(?![\w.])")


def _bare(part: str) -> str:
    part = part.strip()
    if len(part) >= 2 and part[0] == "[" and part[-1] == "]":
        return part[1:-1]
    if len(part) >= 2 and part[0] == "`" and part[-1] == "`":
        return part[1:-1]
    return part


_PART_RE = re.compile(_IDENT_PART)


def _split_name(text: str) -> list:
    """Split a dotted name on the dots that separate parts, and no others.

    `name.split(".")` was enough while every part was a bare word. Now that
    the naming helper emits `` `Sales Lake` `` a quoted part can itself hold a
    dot, and splitting on all of them would shear a name in half.
    """
    parts, index = [], 0
    while index < len(text):
        match = _PART_RE.match(text, index)
        if match is None:
            return [p.strip() for p in text.split(".")]
        parts.append(match.group(0))
        index = match.end()
        while index < len(text) and text[index] in " \t":
            index += 1
        if index < len(text):
            if text[index] != ".":
                return [p.strip() for p in text.split(".")]
            index += 1
            while index < len(text) and text[index] in " \t":
                index += 1
    return parts


def _without_identifier_bodies(view: str) -> str:
    """`view` with quoted-identifier bodies blanked, delimiters left in place.

    A scan that looks for *keywords* must not read a column name. `_masked`
    keeps identifier bodies visible on purpose -- the naming rules have to
    read them -- so a keyword scan has to blank them itself, and there is more
    than one such scan. Measured, before this existed:

      SELECT [exec time], c FROM dbo.t       -> unchanged, SQ01_PROCEDURAL
      SELECT [cursor position], c FROM dbo.t -> unchanged, SQ01_PROCEDURAL
      SELECT [#units], c FROM dbo.t          -> unchanged, SQ01_PROCEDURAL

    `translate()` short-circuits on the procedural gate and emits the T-SQL
    verbatim, so one realistic BI column name disabled every rule in the file.
    Length-preserving, like every other view in this module.
    """
    return apply_replacements(view, [
        (start + 1, end - 1, " " * (end - start - 2))
        for start, end in _identifier_spans(view)])


def _outermost(items):
    """Drop any item whose span sits inside another item's span.

    A nested match cannot be rewritten in the same pass as its parent: the
    parent's replacement text is built from the ORIGINAL argument source, so
    an inner rewrite applied simultaneously would be discarded. Dropping the
    inner one and re-running recovers it -- see _to_fixed_point.
    """
    kept = []
    for index, item in enumerate(items):
        start, end = item[0], item[1]
        if any(other[0] <= start and end <= other[1]
               and (other[1] - other[0]) > (end - start)
               for position, other in enumerate(items) if position != index):
            continue
        kept.append(item)
    return kept


def _to_fixed_point(sql, findings, collect, max_passes: int = 8) -> str:
    """Apply `collect` outermost-first, repeatedly, until nothing changes.

    `collect(sql)` returns (start, end, replacement, finding) tuples. Only the
    findings for replacements actually applied are recorded, so a dropped
    nested match is reported on the pass that rewrites it, exactly once.

    A zero-width item is a refusal rather than an edit, and is taken out of
    the loop here: applying it changes nothing, so the same item returns every
    pass, the budget burns, and the post-loop check then reports deep nesting
    for a flat expression. Measured on a depth-ONE expression,
    `CASE ... END + ' ' + [Region]`: 8x SQ41_CONCAT_KEYWORD plus a spurious
    SQ03_NESTING_TOO_DEEP, flags=9. Three of the four callers stripped these
    themselves; doing it here covers the fourth and every future one, and
    reports each refusal once.

    `max_passes = 8` IS A SAFETY VALVE, NOT A MEASURED BOUND, and the number
    was filed against for having no justification. It still has none, and
    saying so is the honest answer. What IS measured, with the loop
    instrumented to record the passes each call actually used:

      a full `migrate` of the 106-asset demo estate
        228 calls -- 224 used 0 passes, 4 used 1.  Deepest: 1
      the 9 vendored real Warehouse objects in tests/fixtures/real/corpora
        0 passes on every one: nothing in them is a nested rewrite at all
      the whole test suite, 10775 calls
        10448 x 0, 298 x 1, 23 x 2, 4 x 3, and 2 that exhaust the budget --
        those two are `SELECT ISNULL(a, ISNULL(a, ... ))` ten deep, written
        by hand to make SQ03 fire.  Deepest not written for that purpose: 3

    So nothing anyone has fed this needs more than three passes, and 8 is
    not derived from that: three observations of an estate bound what that
    estate needs, not what the next export needs, and raising 3 to 8 is a
    guess with a margin on it rather than a measurement. Do not write "8
    because the corpus reaches 3" here -- it does not follow.

    What the number actually has to do is bound the work done on a
    pathological input, and what makes any value of it safe is the check
    below: exhausting the budget raises SQ03_NESTING_TOO_DEEP, so the
    unrewritten T-SQL is reported rather than returned quietly. Raise it and
    a deeper expression finishes; lower it and more objects reach a human
    with a flag. Neither is silently wrong, which is why the number can be a
    valve and not a proof.
    """
    reported = set()

    def take(items):
        """Findings for the zero-width refusals; the real edits, returned.

        Runs before `_outermost`, which would drop a zero-width item sitting
        inside a wider span and lose its finding with it.
        """
        edits = []
        for item in items:
            if item[0] == item[1]:
                finding = item[3]
                if finding is not None:
                    key = (finding.rule, finding.detail)
                    if key not in reported:
                        reported.add(key)
                        findings.append(finding)
                continue
            edits.append(item)
        return edits

    for _pass in range(max_passes):
        items = _outermost(take(collect(sql)))
        if not items:
            return sql
        for item in items:
            if item[3] is not None:
                findings.append(item[3])
        sql = apply_replacements(sql, [(a, b, c) for a, b, c, _f in items])
    if _outermost(take(collect(sql))):
        # Out of passes with work still outstanding, which needs nesting
        # deeper than max_passes. Returning quietly would leave unrewritten
        # T-SQL in the output with no finding against it -- the one thing this
        # module must not do. (`collect` dedupes its own flags, so asking it
        # once more reports nothing twice.)
        findings.append(Finding(
            "SQ03_NESTING_TOO_DEEP",
            f"nesting deeper than {max_passes} levels was not rewritten all "
            f"the way down; what is left is the T-SQL as written and needs a "
            f"hand rewrite",
            "flag"))
    return sql


# kind -> (rule id, what was left open, what the masking then read the rest
# of the object as). Malformed input rather than a translation gap, so nothing
# is rewritten: guessing where a quote should have closed would be invention.
_UNTERMINATED = {
    "literal": ("SQ01_UNTERMINATED_QUOTE", "string literal", "literal"),
    "identifier": ("SQ01_UNTERMINATED_IDENTIFIER", "quoted identifier",
                   "identifier"),
    "comment": ("SQ01_UNTERMINATED_COMMENT", "block comment", "comment"),
}


def rule_unterminated_text(sql: str, findings: list) -> str:
    """SQ01: flag a quote, bracket or block comment left open at end of input.

    A diagnosis, not a repair -- the SQL comes back exactly as written.
    Without it the failure is invisible, which is D1's shape reached through
    malformed input instead of a bracketed apostrophe. Measured:

      SELECT 'abc FROM dbo.t   -> unchanged, findings=[], PASS
      SELECT [a]]              -> unchanged, findings=[], PASS
      SELECT 1 /* x FROM dbo.t -> unchanged, findings=[], PASS

    The first is the worst: `dbo.t` sits inside what the masker reasonably
    reads as a string, so SQ11 never sees it and the user ships an unqualified
    table name believing the file was checked.

    The flag is about the file, so the rules still run: a truncated multi-
    statement export has sound statements before the break and their rewrites
    are worth keeping.
    """
    found = _unterminated(sql)
    if found is None:
        return sql
    kind, offset = found
    rule_id, what, read_as = _UNTERMINATED[kind]
    opener = "/*" if kind == "comment" else sql[offset]
    findings.append(Finding(
        rule_id,
        f"a {what} opened by {opener!r} at offset {offset} "
        f"(line {sql.count(chr(10), 0, offset) + 1}) is never closed, so "
        f"everything after it masked as {read_as} text and no rule could see "
        f"it. The SQL is malformed -- a truncated file, a bad encoding or a "
        f"partial export does this -- and is left exactly as written, because "
        f"guessing where it should have closed would be invention",
        "flag"))
    return sql


# `#name` is a session temp table, `##name` a global one. The lookbehind
# keeps the `#` of a name that is already part of a longer token out of it;
# the caller blanks quoted-identifier bodies, so `[a#b]` is a column and not
# a match.
_TEMP_TABLE_RE = re.compile(r"(?<![\w$#])#{1,2}\w+")


def rule_temp_table(sql: str, findings: list) -> str:
    r"""SQ51: flag every T-SQL temp table reference. Nothing is rewritten.

    This id existed and could not fire. `_CONTROL_FLOW_RE` matched `#\w+`,
    and `translate()` returns on the procedural gate before RULES runs at
    all, so the branch in `rule_select_into` that carried the specific
    message was unreachable. Measured on the tree before this:

      SELECT a INTO #tmp FROM dbo.t  -> ['SQ01_PROCEDURAL'], unchanged
      SELECT a INTO ##g   FROM dbo.t -> ['SQ01_PROCEDURAL'], unchanged
      _CONTROL_FLOW_RE.search("SELECT a INTO #tmp FROM dbo.t") -> truthy

    Made reachable rather than deleted. A temp table is not control flow:
    the statement around it is an ordinary SELECT, worth qualifying and
    type-mapping, and the operator needs to be told *which* construct has
    to change and what to change it to -- not the generic
    "object uses T-SQL control flow" that names nine other things it does
    not contain.

    Reported, never rewritten, because there is no rewrite that preserves
    the scope. Measured on Spark 4.2.0, every spelling:

      SELECT a INTO #tmp FROM t     PARSE_SYNTAX_ERROR at '#'
      SELECT a INTO ##g FROM t      PARSE_SYNTAX_ERROR at '#'
      SELECT a FROM #tmp            PARSE_SYNTAX_ERROR at '#'
      INSERT INTO #tmp SELECT ...   PARSE_SYNTAX_ERROR at '#'
      DROP TABLE #tmp               PARSE_SYNTAX_ERROR at '#'
      CREATE OR REPLACE TEMP VIEW   ACCEPTED

    The TEMP VIEW is the replacement for a read and is named in the
    finding; a write needs a persisted table, which is a different lifetime
    and a decision for a human.

    One finding per object, naming every distinct temp table in it, rather
    than one per occurrence: a procedure body touches the same `#stage`
    eight times and eight identical flags say nothing the first did not.
    """
    view = _without_identifier_bodies(_masked(sql))
    names = []
    for match in _TEMP_TABLE_RE.finditer(view):
        if match.group(0) not in names:
            names.append(match.group(0))
    if names:
        findings.append(Finding(
            "SQ51_TEMP_TABLE",
            f"temp table(s) {', '.join(names)}: T-SQL's `#name` (session) and "
            f"`##name` (global) tables have no Spark equivalent and Spark "
            f"rejects the `#` outright, so this object cannot run as "
            f"written. For a read, CREATE OR REPLACE TEMP VIEW; for a write, "
            f"a persisted table -- which outlives the session the original "
            f"was scoped to, so it is a decision rather than a substitution. "
            f"A `SELECT ... INTO #name` is left as written for the same "
            f"reason: the CTAS this rule set would otherwise build creates a "
            f"permanent table where the original created a temporary one",
            "flag"))
    return sql


# T-SQL's `N'...'` is a national-character literal. Spark has no N prefix --
# its strings are already Unicode -- so `SELECT N'x'` is a syntax error on
# Spark 4.2.0 (measured), and the stray `N` also derailed the concat rule:
# `SELECT N'a' + N'b'` came out as `SELECT Nconcat('a', N)'b'`, which is not
# SQL at all. The prefix is stripped first, before any rule rewrites around a
# literal.
_UNICODE_PREFIX_RE = re.compile(r"(?<![\w$])[Nn](?=')")


def rule_unicode_literals(sql: str, findings: list) -> str:
    """SQ13: N'...' -> '...'.

    The `N` has to be a whole token, and the quote after it has to *open* a
    literal rather than close one: `'N'` is the letter N inside a literal and
    `[N'x]` is a column named `N'x`. The masked view answers both -- it blanks
    literal bodies and skips quoted identifiers -- so this is a lookup rather
    than another guess about where literals are.
    """
    view = _masked(sql)
    openings = {start for start, _end in _literal_spans(view)}
    replacements = [(match.start(), match.end(), "")
                    for match in _UNICODE_PREFIX_RE.finditer(view)
                    if match.end() in openings]
    if replacements:
        findings.append(Finding(
            "SQ13_UNICODE_LITERAL",
            f"{len(replacements)} N'...' prefix(es) removed; Spark has no "
            f"N-prefixed literal -- its strings are already Unicode -- and "
            f"stops at the N with a syntax error",
            "rewrite"))
    return apply_replacements(sql, replacements)


def _rewrite_call(sql, _view, pattern, findings, builder):
    """Shared shape: find calls, split args, let `builder` decide the outcome.

    `builder(args)` returns (replacement_text, finding) or (None, finding).
    Runs to a fixed point so a nested call of the same kind -- CONVERT inside
    CONVERT -- is rewritten on a later pass instead of colliding with its
    parent.
    """
    seen = set()
    deferred = []
    passes = 0

    def collect(current):
        nonlocal passes
        passes += 1
        view = _masked(current)
        items = []
        for match in pattern.finditer(view):
            split = split_call_arguments(view, match.end() - 1)
            if split is None:
                continue
            spans, close = split
            args = [current[start:end].strip() for start, end in spans]
            text, finding = builder(args)
            if text is None:
                # A refusal is a statement about the SQL the user wrote, and
                # the first pass already sees every call in it -- nested ones
                # included, since only the *replacements* are thinned by
                # _outermost. Later passes are looking at this rule's own
                # output, which is not the user's SQL: SQ20 emits Spark's
                # two-argument `datediff(end, start)`, its own pattern matches
                # that, and reporting it as a T-SQL arity error took a clean
                # view out of PASS on a measured run.
                if passes == 1 and finding is not None:
                    key = (finding.rule, finding.detail)
                    if key not in seen:
                        seen.add(key)
                        deferred.append(finding)
                continue
            items.append((match.start(), close + 1, text, finding))
        return items

    result = _to_fixed_point(sql, findings, collect)
    findings.extend(deferred)
    return result


_CONVERT_CALL_RE = re.compile(
    r"(?<![\w$.])(?:TRY_)?CONVERT\s*\(", re.IGNORECASE)
_BARE_WORD_RE = re.compile(r"[A-Za-z_][\w$]*")
# `PIVOT (` and, inside it, the `IN (` of the FOR clause. `\b` matches no
# boundary inside UNPIVOT -- `N` and `P` are both word characters -- which is
# exactly right: an UNPIVOT's `IN` list really does hold column names, in
# T-SQL and in Spark alike, and must keep being backquoted.
_PIVOT_CALL_RE = re.compile(r"\bPIVOT\s*\(", re.IGNORECASE)
_IN_CALL_RE = re.compile(r"\bIN\s*\(", re.IGNORECASE)
_FOR_WORD_RE = re.compile(r"\bFOR\b", re.IGNORECASE)


def _pivot_value_spans(view: str):
    """(start, end) inside each T-SQL `PIVOT (... FOR c IN (...))` list.

    The one place a bracketed token is a VALUE. T-SQL spells the pivoted
    values as identifiers because they become the output column names;
    Spark's PIVOT takes literals there and reads an identifier as a column
    reference. Measured on Spark 4.2.0 (pyspark 4.2.0, JAVA_HOME=openjdk@21,
    local[1], source table `USING parquet`):

      PIVOT (SUM(v) FOR k IN (`a`,`b`))    REJECTED
        UNRESOLVED_COLUMN.WITH_SUGGESTION -- "with name `a` cannot be
        resolved. Did you mean one of the following? [`k`, `v`]"
      PIVOT (SUM(v) FOR k IN ('a','b'))    ACCEPTED

    and the exception has to stop at PIVOT, measured in the same session:

      UNPIVOT (v FOR k IN (`a`,`b`))       ACCEPTED
      UNPIVOT (v FOR k IN ('a','b'))       REJECTED  PARSE_SYNTAX_ERROR at ''a''

    so an UNPIVOT's IN list is the opposite case and must stay backquoted.

    The value list is the `IN (` that follows the `FOR`, both at the PIVOT
    group's own depth. "The first IN in the group" is not the same thing and
    was measured wrong:
    `PIVOT (SUM(CASE WHEN x IN (1,2) THEN v END) FOR k IN ([a]))` has an
    ordinary predicate over ordinary columns inside the aggregate, and
    taking that one left `[a]` backquoted.
    """
    # Brackets are counted, so the structural scan runs on a copy with
    # identifier bodies blanked: `[a]]b]` holds a `]` that is not a closer.
    scan = _without_identifier_bodies(view)
    spans = []
    for match in _PIVOT_CALL_RE.finditer(scan):
        open_index = match.end() - 1
        close = _closing_index(scan, open_index)
        if close is None:
            continue
        body_end = close - 1
        depth, index, seen_for = 0, open_index + 1, False
        while index < body_end:
            char = scan[index]
            if char in "([":
                depth += 1
            elif char in ")]":
                depth -= 1
            elif depth == 0 and not seen_for:
                word = _FOR_WORD_RE.match(scan, index)
                if word is not None:
                    seen_for = True
                    index = word.end()
                    continue
            elif depth == 0:
                found = _IN_CALL_RE.match(scan, index)
                if found is not None:
                    in_open = found.end() - 1
                    in_close = _closing_index(scan, in_open)
                    if in_close is not None and in_close <= close:
                        spans.append((in_open + 1, in_close - 1))
                    break
            index += 1
    return spans


def _definition_items(scan: str, start: int, end: int):
    """(start, end) of each depth-0 comma-separated item in `scan[start:end]`.

    `decimal(7,2)` puts a comma inside a column definition, so this counts
    parentheses rather than calling `.split(",")`.
    """
    items, depth, item_start = [], 0, start
    for index in range(start, end):
        char = scan[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items.append((item_start, index))
            item_start = index + 1
    items.append((item_start, end))
    return items


def _type_positions(view: str):
    """Offsets at which a quoted token is a TYPE name and not an identifier.

    The places T-SQL writes a type, and nowhere else: the second token of a
    column definition, the target of a CAST or TRY_CAST, and the first
    argument of CONVERT or TRY_CONVERT. Everything in this module that reads
    a bare word out of context has needed a companion like this eventually;
    SQ10's type branch was the last one without one.

    "The SECOND token of the item" rather than "a quoted token with an
    identifier in front of it": the looser form reads the `[df]` of
    `CONSTRAINT [df] DEFAULT 0` as a type, and a constraint named `[date]`
    would then lose its quoting -- the very thing this is here to stop.

    The name token is located from the masking module's identifier spans
    rather than from a pattern, for the reason recorded at
    `_QUOTED_IDENT_FORMS`: `[a]]b]` is the ONE identifier `a]b`, and a regex
    that stops at the first `]` reads it as two tokens and then takes the
    real type for the name. Measured while writing this, with a pattern:
    `CREATE TABLE dbo.t ([a]]b] [int] NOT NULL)` came back with `` `INT` ``
    backquoted as an identifier, which Spark rejects as a data type.

    The helpers it calls are defined further down the file and read at call
    time, deliberately: the column-list body finder, the CAST type-slot
    finder and the ALTER ... ADD pattern each belong with the rule that owns
    them, and the point of this function is that SQ10 agrees with all three
    rather than keeping a fourth opinion of its own.
    """
    # Structure is read from a copy with identifier bodies blanked -- a
    # column can be called `[a,b]` -- and both views are length-preserving,
    # so an offset means the same thing in each.
    scan = _without_identifier_bodies(view)
    span_end = {start: end for start, end in _identifier_spans(view)}
    regions = [(open_index + 1, body_end - 1)
               for open_index, body_end in _column_list_bodies(view)]
    for match in _ALTER_ADD_RE.finditer(view):
        word = _LEADING_WORD_RE.match(scan, match.end())
        if word is not None and word.group(0).casefold() in _NOT_A_COLUMN_ADD:
            continue
        regions.append((match.end(), _statement_bounds(view, match.start())[1]))

    positions = set()
    for region_start, region_end in regions:
        for item_start, item_end in _definition_items(scan, region_start,
                                                      region_end):
            index = item_start
            while index < item_end and scan[index] in _WS:
                index += 1
            if index in span_end:
                index = span_end[index]           # a quoted column name
            else:
                word = _BARE_WORD_RE.match(scan, index)
                if word is None or word.end() > item_end:
                    continue
                index = word.end()
            while index < item_end and scan[index] in _WS:
                index += 1
            if index < item_end and index in span_end:
                positions.add(index)
    for start, _end in _cast_type_spans(view):
        positions.add(start)
    for match in _CONVERT_CALL_RE.finditer(view):
        split = split_call_arguments(view, match.end() - 1)
        if split is None:
            continue
        arg_spans, _close = split
        if not arg_spans:
            continue
        start, end = arg_spans[0]
        while start < end and view[start] in _WS:
            start += 1
        positions.add(start)
    return positions


def rule_bracket_identifiers(sql: str, findings: list) -> str:
    """SQ10: `[ident]` and `"ident"` -> `` `ident` ``, except in type position.

    SQL Server scripting brackets the type as well as the column name:
    `[CustomerID] [int] NOT NULL`. Backticking both turns the type into a
    quoted identifier and Spark rejects it with UNSUPPORTED_DATATYPE -- a live
    cluster run found exactly that. A bracketed token that names a known SQL
    type and STANDS IN A TYPE POSITION is therefore unwrapped to a bare word
    so the type rules can map it.

    The position test is the second half of that sentence and it used to be
    absent: the check was `name.casefold() in _KNOWN_TYPE_NAMES` and nothing
    else, so any column whose name happened to be a type name lost the
    quoting its author wrote. Measured before `_type_positions`:

      SELECT [timestamp], [int] FROM dbo.t -> SELECT timestamp, int FROM ...
        SQ10_BRACKET_TYPE x2, both reported as successful rewrites
      SELECT t.[date] FROM dbo.t           -> SELECT t.date FROM ...
      CREATE TABLE t([int] INT)            -> CREATE TABLE t(int INT)

    That last one is the clearest: a column *named* `int`, in the name slot
    of a column definition, unquoted by the rule that exists to quote it.
    This is the same shape as the bug `_without_identifier_bodies` was
    written for and as the one `_UNMAPPABLE_COLUMN_TYPE_RE`'s comment
    records -- a position-blind match on a word that is somebody's column
    name -- and it is the shape this file spends its comments guarding
    against.

    Whether the BARE name then runs depends on a Spark setting. Measured on
    pyspark 4.2.0, JAVA_HOME=openjdk@21, local[1], against a view that really
    has a column of each name -- all 37 names in `_KNOWN_TYPE_NAMES`, one
    statement each:

      spark.sql.ansi.enabled = true (the Spark 4 default)
      spark.sql.ansi.enforceReservedKeywords = false (the default)
        SELECT <name> FROM x     ACCEPTED for all 37
      spark.sql.ansi.enforceReservedKeywords = true
        SELECT time FROM x       REJECTED  PARSE_SYNTAX_ERROR at or near 'time'
        the other 36, `int` and `timestamp` and `date` among them,  ACCEPTED
      ansi.enabled = false with enforceReservedKeywords = true
        SELECT time FROM x       ACCEPTED -- the setting only bites with ANSI on
      the backquoted spelling, `` `time` ``, ACCEPTED in every combination,
      and `CREATE TABLE ct (time INT)` is the same story: REJECTED bare under
      enforceReservedKeywords, ACCEPTED backquoted.

    So the blast radius of the unwrap is one name out of thirty-seven and one
    non-default setting -- narrower than it looks, and not nothing. The
    backtick is correct in all four combinations above and costs nothing, so
    the rule emits one rather than deciding whether this particular bare name
    is safe on this particular cluster today. The position test is what makes
    that possible without regressing `[CustomerID] [int]`.

    The identifier spans come from the masking module rather than from a
    pattern here, so the `]]` and `""` escapes are read the same way by the
    rule and by every scan that runs over its output.

    `SET QUOTED_IDENTIFIER OFF` in the file flips `"` back to meaning a string
    literal. That is a whole-file mode this rule refuses to model: it flags
    the statement (SQ14) and leaves every double-quoted span as written.
    Bracketed names are unaffected -- the mode says nothing about them.
    """
    view = _masked(sql)
    spans = _identifier_spans(view)
    literal_mode = _QUOTED_IDENTIFIER_OFF_RE.search(view)
    if literal_mode:
        doubles = sum(1 for start, _end in spans if sql[start] == '"')
        findings.append(Finding(
            "SQ14_QUOTED_IDENTIFIER_OFF",
            f"`SET QUOTED_IDENTIFIER OFF` flips the meaning of `\"` for the "
            f"whole file: under it `\"x\"` is a string literal, not an "
            f"identifier. The {doubles} double-quoted span(s) here were left "
            f"exactly as written -- modelling a mode switch mid-file, or "
            f"tracking the setting across batches, would mean guessing, and a "
            f"wrong guess changes values with no error. The statement itself "
            f"has no Spark equivalent and needs removing by hand",
            "flag"))
    replacements = []
    type_positions = _type_positions(view)
    pivot_values = _pivot_value_spans(view)
    for start, end in spans:
        form = _QUOTED_IDENT_FORMS.get(sql[start])
        if form is None:
            continue  # already a Spark `...` identifier; nothing to convert
        closer, ident_id, type_id, empty_id = form
        if closer == '"' and literal_mode:
            continue  # in this file it is a literal; see SQ14 above
        # `]]` / `""` is how T-SQL spells the closer inside a quoted name.
        # Spark needs no escape for `]` or `"` in a backquoted identifier and
        # doubles a backtick, so the name is unescaped once and re-escaped.
        name = sql[start + 1:end - 1].replace(closer * 2, closer)
        written = sql[start:end]
        if not name:
            # T-SQL rejects an empty identifier itself, so this input is
            # already malformed. `` `` `` is an empty Spark identifier, which
            # cannot resolve, and emitting it as a rewrite would call that a
            # success.
            findings.append(Finding(
                empty_id,
                f"`{written}` is an empty identifier, which neither T-SQL nor "
                f"Spark accepts; left as written for a hand fix rather than "
                f"emitted as an empty backquoted name",
                "flag"))
            continue
        if any(low <= start and end <= high for low, high in pivot_values):
            # The one position where a bracketed token is a VALUE. T-SQL
            # writes the pivoted values as identifiers because they become
            # the output column names; Spark's PIVOT takes literals and
            # reads an identifier as a column reference, so the backquoted
            # form is UNRESOLVED_COLUMN -- two values silently became two
            # column references, reported as two successful rewrites.
            #
            # Written in T-SQL spelling, `''` and all: every rule in this
            # file emits literals that way and `rule_spark_string_literals`
            # respells the lot once, last.
            replacements.append(
                (start, end, "'" + name.replace("'", "''") + "'"))
            findings.append(Finding(
                "SQ53_PIVOT_VALUE",
                f"{written} -> '{name}' in a PIVOT's IN list; T-SQL spells "
                f"the pivoted VALUES as identifiers because they become the "
                f"output column names, and Spark wants literals there. "
                f"Backquoting it made Spark read the value as a column "
                f"(measured on 4.2.0: UNRESOLVED_COLUMN.WITH_SUGGESTION). An "
                f"UNPIVOT's IN list really does hold column names and is "
                f"still backquoted",
                "rewrite"))
            continue
        if start in type_positions and name.casefold() in _KNOWN_TYPE_NAMES:
            replacements.append((start, end, name))
            findings.append(Finding(
                type_id,
                f"{written} unwrapped to a bare type name; it stands in a "
                f"type position -- a column definition, a CAST or a CONVERT "
                f"-- and backticking it there would make Spark read the type "
                f"as a quoted identifier (UNSUPPORTED_DATATYPE)",
                "rewrite"))
            continue
        replacements.append(
            (start, end, "`" + name.replace("`", "``") + "`"))
        findings.append(Finding(
            ident_id,
            f"{written} -> `{name}`; Spark reads square brackets as an array "
            f"subscript and a double-quoted name as a string literal, neither "
            f"of which is a quoted identifier",
            "rewrite"))
    return apply_replacements(sql, replacements)


# Whether the keyword in front of an object name means this statement is
# *creating* that object. The catalog must not be asked about one: the
# warehouse_ddl tier is built out of these very CREATE statements, so
# `CREATE TABLE dbo.claim` is where `dbo.claim` comes from, and answering
# "not found in warehouse DDL" on the file that IS the warehouse DDL would
# be the tool refusing to read its own input. Every other keyword in
# `_OBJECT_KEYWORDS` -- FROM, JOIN, UPDATE, ALTER, DROP, TRUNCATE, and INTO
# in its INSERT/MERGE reading -- names a table that has to be there already.
_CREATE_KEYWORD_RE = re.compile(r"\s*CREATE\b", re.IGNORECASE)
# The word in front of a bare `INTO`. `INSERT INTO t` and `MERGE INTO t`
# reference `t`; `SELECT a INTO t FROM u` creates it, and that is the one
# object-creating form `_OBJECT_KEYWORDS` matches without the word CREATE.
_WORD_BEFORE_RE = re.compile(r"([A-Za-z_]\w*)\s*$")
_INTO_REFERENCES = frozenset({"INSERT", "MERGE"})


def _creates_the_object(view: str, keyword: str, start: int) -> bool:
    """Whether `keyword` at `start` introduces an object being created."""
    if _CREATE_KEYWORD_RE.match(keyword):
        return True
    if " ".join(keyword.upper().split()) != "INTO":
        return False
    before = _WORD_BEFORE_RE.search(view[:start])
    return not (before and before.group(1).upper() in _INTO_REFERENCES)


def _catalog_declines(table_catalog, original: str, owner, findings) -> tuple:
    """SQ19: `(declined, entry)` -- whether to leave this name as written.

    `declined` is True when the catalog says the name must not be rewritten.
    `entry` is the catalog entry when there is exactly one, so the caller can
    ask `owning_item` *where* the table is rather than assuming its own
    binding; it is None when the catalog declined or was not supplied.

    The warehouse path used to consult no catalog at all. Every table
    reference it recognised by *shape* got a confident three-part name and
    a `rewrite` finding, so an object reading a shortcut -- whose data is in
    S3 and not in the lakehouse -- or a table nothing in the export declares
    came out as runnable Spark SQL and graded PASS. Measured side by side
    against the notebook path on the demo's resolved catalog, `FROM
    dbo.nowhere_at_all` in an AcmeDW view:

        notebook   NB12_TABLE_UNKNOWN  flag,    left as written
        warehouse  SQ11_TWO_PART_NAME  rewrite, default.AcmeDW.nowhere_at_all

    -- same reference, same estate, opposite verdicts. The answer comes from
    `inventory.catalog.classify_reference`, which is the one the notebook
    rules ask too; only the wording is per-translator.

    Refusing the rewrite rather than flagging beside it is the notebook
    path's choice and is copied deliberately. A name left as written is
    visibly unfinished; a three-part name for a table that does not exist
    reads as migrated work. `REF_INFERRED` is the exception: the rewrite is
    the same one a known table would get, and only the ordering caveat is
    worth saying, so it is reported `info` and the caller carries on.

    There is no warehouse analogue of the notebook's NB13 ("no owning item")
    and one was not added: `rule_two_part_names` returns before this point
    when `item` is empty, and a three-part name carries its own item in the
    SQL, so the state is unreachable from here. A rule that cannot fire
    reads as coverage.

    With no catalog supplied this declines nothing and hands back no entry,
    so the rules behave exactly as they did. That is load-bearing:
    `translate()` is called without one by `%%tsql` cells and by most of the
    test suite, and treating "nothing was supplied" as "this table does not
    exist" would flag every reference in the estate.

    The test for that is `names_no_tables` and not `not table_catalog`, and
    the difference is the whole of a defect this guard used to have. A plan
    carries `resolved_catalog = {"summary": {}, "tables": {}}` when nothing
    resolved -- a truthy dict naming nothing -- so the guard let it through
    and every reference in the estate collected SQ19_TABLE_UNKNOWN. That is
    not what an empty catalog means. The run says once that it resolved
    nothing, in `migrate.runner`, which is the only scope at which the
    statement is true.
    """
    if names_no_tables(table_catalog):
        return (False, None)
    verdict, entry, matched = classify_reference(
        table_catalog, original, owner=owner)
    if verdict == REF_AMBIGUOUS:
        named = ", ".join(sorted(
            repr(str(m.get("name") or "?")) for m in matched))
        findings.append(Finding(
            "SQ19_TABLE_AMBIGUOUS",
            f"table {original!r} matches {len(matched)} catalog entries "
            f"({named}) and this reference does not say which; it was left "
            f"exactly as written rather than qualified with one of them. "
            f"Qualify it with the owning Lakehouse or Warehouse, or its "
            f"schema",
            "flag"))
        return (True, None)
    if verdict == REF_UNKNOWN:
        findings.append(Finding(
            "SQ19_TABLE_UNKNOWN",
            f"table {original!r} was not found in warehouse DDL, shortcuts, a "
            f"supplied catalog, or any notebook write, so it was left exactly "
            f"as written rather than given a three-part AIDP name for a table "
            f"that may not exist; confirm it exists on AIDP before running "
            f"this",
            "flag"))
        return (True, None)
    if verdict == REF_SHORTCUT:
        findings.append(Finding(
            "SQ19_TABLE_IS_SHORTCUT",
            f"table {original!r} is a shortcut to "
            f"{entry.get('target', '?')!r}; its data lives outside the "
            f"lakehouse, so it was not rewritten to a three-part name. "
            f"Migrate the shortcut's target and read that instead",
            "flag"))
        return (True, None)
    if verdict == REF_INFERRED:
        # `info`, where the notebook path's NB14 is `rewrite`, and the
        # difference is not an oversight. `TranslationResult.changes` counts
        # `rewrite` findings, NB14 is the only finding the notebook emits for
        # that name, and reporting it `info` made verify print no change count
        # for an edited artifact. Here SQ11 already emits the `rewrite` for
        # the very same substitution, so a second one would count one rewrite
        # twice.
        findings.append(Finding(
            "SQ19_TABLE_INFERRED",
            f"table {original!r} was still qualified, but the catalog knows "
            f"it only because notebook {entry.get('created_by', '?')!r} "
            f"writes it -- no DDL in this export declares it, so that "
            f"notebook must run first",
            "info"))
    return (False, entry)


def rule_three_part_names(sql: str, findings: list, *,
                          catalog: str = DEFAULT_CATALOG,
                          table_catalog=None,
                          resolve_objects: bool = True) -> str:
    """SQ11: `db.schema.object` -> `<catalog>.<item>[_<schema>].object`.

    This used to rewrite the *first* part to `default` and keep the rest, on
    the stated premise that "AIDP has one catalog". A live instance has five,
    and worse: in T-SQL the first part is the database -- the Fabric Warehouse
    -- so dropping it and keeping `dbo` produced `default.dbo.claim`, which is
    not a differently-spelled name for the same table but a different table.

    SQ15: `a.b.c` is ambiguous in T-SQL. It is `database.schema.object` in an
    object position and `schema.table.column` everywhere else, and this rule
    used to read every one of them as a table. Measured:

        SELECT dbo.t.c FROM dbo.t -> SELECT default.dbo_t.c FROM default.W.t

    -- the FROM right, the column reference built into a table name that
    resolves to nothing, flags=0. Position decides it, the same way position
    decides `timestamp` between rowversion and a datetime; `_OBJECT_KEYWORDS`
    is the list the two-part rule already uses, so the two cannot disagree
    about what an object position is.

    In the column reading only the *table* part is kept, as the user wrote
    it. `FROM dbo.t` exposes the table under the name `t` whether or not the
    two-part rule qualified it, so `t.c` resolves in every case this rule can
    produce -- which a longer qualifier built from the rewritten table name
    would not obviously do, and this tool has no Spark measurement of a
    catalog-qualified column reference to lean on.

    `resolve_objects=False` keeps everything above except the rewrite of a
    name in an object position. A `%%tsql` notebook cell asks for that: the
    notebook translator's own NB15 has already resolved every table name in
    the body against the catalog, which knows more than this rule can, and
    where NB15 *declined* -- an unknown name (NB12) or one matching two
    catalog entries (NB20), both of which say "left exactly as written" --
    this rule rewrote it anyway, from a three-part shape and nothing else.
    That produced a finding that contradicts the artifact beside it. The
    two-part rule is already switched off there the same way, by passing no
    `item`, with the same reasoning written above it.

    `catalog` and `table_catalog` are two different things that share a word,
    and the notebook translator's docstring records what conflating them
    cost. `catalog` is the **AIDP catalog name** -- the string `default`, or
    whatever `plan --catalog` set -- and it is the first part of every name
    this rule emits. `table_catalog` is the **resolved table catalog**, the
    `name -> {tier, owner, ...}` map `inventory.catalog` builds from the
    export, and it decides whether the table being named exists at all. See
    `_catalog_declines`; without it this rule rewrites on name shape alone.
    """
    view = _masked(sql)
    # span -> `(keyword, where it starts)`, which `_creates_the_object` needs
    # to tell `CREATE TABLE dbo.t` (a definition, not a lookup) from
    # `FROM dbo.t`. It was a set of spans.
    objects = {match.span("name"): (match.group("kw"), match.start("kw"))
               for match in _THREE_PART_OBJECT_RE.finditer(view)
               if not _in_a_from_taking_call(view, match.start())}
    # MERGE's USING source, whose position no keyword marks. Without it a
    # three-part source fell into the *column* branch below and lost its
    # database, which is F1's defect in a second place: MEASURED on 2a223bd,
    # `MERGE INTO dbo.t USING mydb.dbo.s AS src ON 1=1` came back
    # `MERGE INTO default.AcmeDW.t USING dbo.s AS src ON 1=1` under
    # SQ15_QUALIFIED_COLUMN. See `_MERGE_USING_RE`.
    objects.update(
        (match.span("name"), (match.group("kw"), match.start("kw")))
        for match in _MERGE_USING_RE.finditer(view))
    # Spans SQ14 has already claimed and reported, which this rule owns
    # neither reading of. See `_identity_insert_targets`.
    claimed = _identity_insert_targets(_without_identifier_bodies(view))
    replacements = []
    for match in _DOTTED_RE.finditer(view):
        original = sql[match.start():match.end()]
        parts = _split_name(original)
        if len(parts) > 3:
            findings.append(Finding(
                "SQ11_LINKED_SERVER",
                f"{original!r} has more than three parts, which means a linked "
                f"server or remote database; there is no AIDP equivalent",
                "flag"))
            continue
        if len(parts) < 3:
            continue
        if match.span() not in objects:
            if any(start <= match.start() and match.end() <= end
                   for start, end in claimed):
                # A `SET IDENTITY_INSERT` target: an object name, not a
                # column, and SQ14's flag already names it. Neither
                # rewritten nor separately reported -- a second finding
                # saying "and the name was left alone" is what SQ14 says.
                continue
            replacement, finding = _qualified_column(sql, view, match, parts)
            findings.append(finding)
            if replacement is not None:
                replacements.append(replacement)
            continue
        if not resolve_objects:
            continue          # a caller that has already resolved the name
        database, schema, obj = _bare(parts[0]), _bare(parts[1]), _bare(parts[2])
        if database.casefold() == catalog.casefold():
            continue
        # No `owner`: a three-part name carries its own Fabric item in the
        # first part, and `resolve` prefers what the author wrote over any
        # binding a caller could pass.
        declined, entry = (False, None)
        if not _creates_the_object(view, *objects[match.span()]):
            declined, entry = _catalog_declines(
                table_catalog, original, None, findings)
        if declined:
            continue
        # The catalog's recorded owner outranks the database the author
        # wrote, the same way it outranks a notebook's binding. The two
        # agree for every name a DacFx export produces; where they do not,
        # the entry is the one that knows.
        written = database
        database, schema = owning_item(entry, schema, database)
        rebuilt = aidp_table(database, obj, schema=schema, catalog=catalog)
        replacements.append((match.start(), match.end(), rebuilt))
        if written and database and _bare(written).casefold() != _bare(database).casefold():
            # Owner-wins is deliberate; doing it silently over a database the
            # author wrote is not. `FROM OtherDW.dbo.claim`, with only
            # `W.dbo.claim` in the catalog, came out as `default.W.claim`
            # graded PASS -- another warehouse's table, and nothing said so.
            findings.append(Finding(
                "SQ25_DATABASE_OVERRIDDEN",
                f"{original!r} names database {_bare(written)!r}, but the only "
                f"catalog entry for {obj!r} belongs to {_bare(database)!r}, so "
                f"it was migrated as {rebuilt!r}; confirm which one this "
                f"object reads",
                "flag"))
        findings.append(Finding(
            "SQ11_THREE_PART_NAME",
            f"{original!r} -> {rebuilt!r}; the Fabric database becomes the "
            f"AIDP schema, under catalog {catalog!r}",
            "rewrite"))
        note_unaddressable_schema(findings, SCHEMA_NOT_ADDRESSABLE,
                                  database, schema)
    return apply_replacements(sql, replacements)


def _qualified_column(sql, view, match, parts):
    """(replacement, finding) for `schema.table.column` outside an object slot.

    `replacement` is None when the name is refused, and the finding says why.
    """
    original = sql[match.start():match.end()]
    probe = match.end()
    while probe < len(view) and view[probe] in _WS:
        probe += 1
    if probe < len(view) and view[probe] == "(":
        # `db.dbo.fn(x)` is a three-part FUNCTION call, not a column. Spark
        # has no schema-qualified user function to map it to, and reading it
        # as a column would emit `dbo.fn(x)`, which is confidently wrong
        # rather than obviously unfinished.
        return (None, Finding(
            "SQ15_QUALIFIED_CALL",
            f"{original!r} is a three-part call, which means a user-defined "
            f"function in another database; Spark has no equivalent name for "
            f"one, so it is left as written for a hand rewrite rather than "
            f"read as a column or as a table",
            "flag"))
    table, column = parts[1].strip(), parts[2].strip()
    rebuilt = f"{table}.{column}"
    return ((match.start(), match.end(), rebuilt), Finding(
        "SQ15_QUALIFIED_COLUMN",
        f"{original!r} -> {rebuilt!r}; outside an object position a "
        f"three-part T-SQL name is schema.table.column, not "
        f"database.schema.object. Reading it as a table produced a name that "
        f"resolves to nothing; the schema is dropped because the FROM clause "
        f"exposes the table under {table!r} however it was qualified",
        "rewrite"))


def _identity_insert_targets(view: str):
    """Spans of the table name in every `SET IDENTITY_INSERT <t> ON|OFF` here.

    The one object position `_OBJECT_KEYWORDS` cannot reach and SQ15 must not
    guess at. `IDENTITY_INSERT` is not an object keyword -- SQ14 flags the
    statement and leaves the name alone on purpose, because a rewritten table
    name inside a statement Spark rejects reads as translated work -- so a
    three-part target fell into SQ15's *column* branch, whose premise
    ("outside an object position `a.b.c` is schema.table.column") is false
    here: T-SQL's grammar for IDENTITY_INSERT takes
    `database.schema.table` and nothing else. MEASURED on 2a223bd,
    `translate(sql, kind="view", item="AcmeDW")`:

        SET IDENTITY_INSERT mydb.dbo.t ON
          -> SET IDENTITY_INSERT dbo.t ON
             ['SQ14_IDENTITY_INSERT', 'SQ15_QUALIFIED_COLUMN']

    -- a different object, silently, beside a SQ14 flag whose own text says
    "left exactly as written, name included". `_OBJECT_KEYWORDS` is a list of
    positions in which a dotted name *cannot be anything else*; it was never
    a list of every object position, so "not in it" does not mean "a column".
    The two- and one-part targets were already safe (`SET IDENTITY_INSERT
    dbo.t ON` and `SET IDENTITY_INSERT t ON` both came back unchanged with
    `['SQ14_IDENTITY_INSERT']`), which is why only SQ15 had to move.

    `_SET_IDENTITY_INSERT_RE` and `_heads_a_statement` are read at call time
    and defined below, the same way `rule_three_part_names` reads
    `_THREE_PART_OBJECT_RE`: the pattern belongs beside the SQ14 rule that
    owns the statement, and the point here is that the two agree about which
    span SQ14 has claimed.
    """
    return [match.span("target")
            for match in _SET_IDENTITY_INSERT_RE.finditer(view)
            if _heads_a_statement(view, match.start())]


# The positions in which a two-part name is a schema-qualified object and
# cannot be anything else. A bare `a.b` anywhere in a statement is far more
# often `alias.column`, so this rule never looks outside these.
# `CREATE PROCEDURE` / `CREATE FUNCTION` are absent on purpose: what an AIDP
# name for a stored procedure should be is a separate, open question, and
# those objects are flagged whole by SQ01 anyway.
_OBJECT_KEYWORDS = (
    r"CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?"
    r"|CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?VIEW"
    r"|ALTER\s+TABLE|ALTER\s+VIEW"
    r"|DROP\s+(?:TABLE|VIEW)(?:\s+IF\s+EXISTS)?"
    r"|TRUNCATE\s+TABLE"
    # Bare `INTO` rather than `INSERT INTO` / `MERGE INTO`, which it covers:
    # `SELECT a INTO dbo.u FROM dbo.t` was the one object-creating form no
    # keyword here matched, so the source was qualified and the table being
    # created was not -- a new table in a different place from the one the
    # plan names, measured with flags=0 and PASS. (`INSERT dbo.t VALUES ...`
    # without INTO is still not covered, as before.)
    r"|INTO"
    r"|UPDATE"
    r"|FROM"
    r"|JOIN"
)
# `kw` is captured, not grouped away: `_creates_the_object` reads it to tell
# the object a statement defines from one it looks up, and only the second
# kind is a question the table catalog can be asked.
_TWO_PART_RE = re.compile(
    rf"\b(?P<kw>{_OBJECT_KEYWORDS})\s+"
    rf"(?P<name>{_IDENT_PART}\s*\.\s*{_IDENT_PART})(?![\w.])",
    re.IGNORECASE)
# The same keyword list, three parts wide. `rule_three_part_names` is defined
# above and reads this at call time: it has to, because `_OBJECT_KEYWORDS`
# belongs with the two-part rule that needs it in its own pattern, and the
# point of SQ15 is that the two rules agree on what an object position is.
_THREE_PART_OBJECT_RE = re.compile(
    rf"\b(?P<kw>{_OBJECT_KEYWORDS})\s+"
    rf"(?P<name>{_IDENT_PART}\s*\.\s*{_IDENT_PART}\s*\.\s*{_IDENT_PART})"
    rf"(?![\w.])",
    re.IGNORECASE)
# SQL Server's own schemas. They have no AIDP counterpart, and qualifying
# them would turn a recognisably-unsupported reference into one that looks
# like a real migrated table.
_SYSTEM_SCHEMAS = frozenset({"sys", "information_schema"})


# The functions that spell an argument separator as the word FROM, and the
# test for being inside one. Both live in `sql_text` now: the notebook
# translator's table rule anchors on FROM as well and had this defect
# separately, reporting NB12_TABLE_UNKNOWN for the column in
# `TRIM(BOTH ' ' FROM name)`. Two copies of the list would drift, which is
# the argument that put `CELL_MAGIC_RE` and `SQL_LANGUAGES` in one place too.
_FROM_TAKING_CALLS = FROM_TAKING_CALLS
_in_a_from_taking_call = in_a_from_taking_call


def rule_two_part_names(sql: str, findings: list, *,
                        catalog: str = DEFAULT_CATALOG, item=None,
                        table_catalog=None) -> str:
    """SQ11: `schema.object` -> `<catalog>.<item>[_<schema>].object`.

    DacFx, which is what writes a Fabric Warehouse into a Git export, emits
    two-part DDL: 9 of the 9 warehouse files in the real corpus and 42 of
    their 44 objects are `CREATE TABLE schema.object`, with the Warehouse
    name carried by the folder rather than the SQL. SQ11's three-part rule
    therefore fired zero times across the whole 106-asset demo, and the plan
    promised `default.AcmeDW.claim` while the artifact that creates the table
    said ``CREATE TABLE `dbo`.`claim` ``.

    `item` is the Fabric Warehouse or Lakehouse these objects belong to --
    the runner reads it off the asset. Without it this rule does nothing:
    a two-part name whose owning item is genuinely unknown must stay as
    written rather than acquire a guessed one.

    `table_catalog` is the resolved table catalog, NOT the AIDP catalog name
    in `catalog` above -- see `_catalog_declines`. It is this rule, not the
    three-part one, that the missing catalog actually cost: the three-part
    rule fires zero times on the whole demo (see above), so every confident
    name the warehouse path emitted for a table nothing declares came from
    here.

    The positions come from `_object_positions`, which is `_TWO_PART_RE`
    plus MERGE's USING source. MEASURED on 2a223bd:

        MERGE INTO dbo.t USING dbo.s ON 1=1
          -> MERGE INTO default.AcmeDW.t USING dbo.s ON 1=1

    -- the target qualified and the source not, one statement naming one
    table the plan promises and one it does not. `USING` is not an entry in
    `_OBJECT_KEYWORDS` and must not become one; `_MERGE_USING_RE` anchors on
    the MERGE instead, which is the statement's own structure.

    A table-valued function in that slot is read as a table, the same way
    the FROM position already reads one: MEASURED,
    `SELECT * FROM dbo.fn(1) x -> SELECT * FROM default.AcmeDW.fn(1) x`,
    which is this rule's behaviour since it shipped. Answering the two
    positions differently would be two answers to one question, and a TVF in
    a read position is a separate open question about both of them.
    """
    if not item:
        return sql
    view = _masked(sql)
    replacements = []
    for match in _object_positions(view, _TWO_PART_RE):
        if _in_a_from_taking_call(view, match.start()):
            # `TRIM(BOTH ' ' FROM d.name)` -- the FROM belongs to the call,
            # and `d.name` is a column, not a table. Rewriting it produced a
            # reference to a schema that does not exist, as a `rewrite` with
            # no flag, so it graded PASS.
            continue
        original = sql[match.start("name"):match.end("name")]
        parts = _split_name(original)
        if len(parts) != 2:
            continue
        schema, obj = _bare(parts[0]), _bare(parts[1])
        if schema.casefold() in _SYSTEM_SCHEMAS:
            continue
        # After the system-schema skip, so `sys.objects` is not reported as
        # a table the catalog has never heard of -- it is one it should not
        # have heard of.
        declined, entry = (False, None)
        if not _creates_the_object(view, match.group("kw"), match.start("kw")):
            declined, entry = _catalog_declines(
                table_catalog, original, item, findings)
        if declined:
            continue
        # Which item the table is actually in. `item` is the folder this
        # `.sql` came out of, and for the object this file defines that is
        # right by construction -- but a *read* can name a table another
        # Warehouse declares, a Lakehouse holds, or a `--tables-csv` row
        # placed, and the catalog knows which. Naming it under this folder's
        # item instead produced a confident three-part name for a table that
        # does not exist, and disagreed with what the notebook path called
        # the same table. Measured with an entry recorded
        # `warehouse: OtherDW`, `FROM dbo.ledger`, item `AcmeDW`: notebook
        # `default.OtherDW.ledger`, warehouse `default.AcmeDW.ledger`.
        # `owning_item` returns `item` unchanged whenever the catalog
        # records no owning item -- a write from a notebook with no binding
        # is the shape that still does.
        owner_item, schema = owning_item(entry, schema, item)
        rebuilt = aidp_table(owner_item, obj, schema=schema, catalog=catalog)
        replacements.append((match.start("name"), match.end("name"), rebuilt))
        findings.append(Finding(
            "SQ11_TWO_PART_NAME",
            f"{original!r} -> {rebuilt!r}; the Fabric item {owner_item!r} "
            f"owning this object is not in the SQL, so the two-part name had "
            f"to be qualified with it to mean the same table the plan does",
            "rewrite"))
        note_unaddressable_schema(findings, SCHEMA_NOT_ADDRESSABLE,
                                  owner_item, schema)
    return apply_replacements(sql, replacements)


# `_OBJECT_KEYWORDS` minus `UPDATE`, one name part wide. The positions in
# which a *bare* name is an object and cannot be anything else.
#
# UPDATE is dropped, and that is the whole of the measurement behind this
# pattern. Three T-SQL shapes put something other than a table name
# immediately after it, and none of them can be told from a table by shape:
#
#   UPDATE c SET c.a = 1 FROM dbo.claim AS c   `c` is an ALIAS
#   UPDATE STATISTICS dbo.claim                `STATISTICS` is a keyword
#   UPDATE TOP (1) dbo.claim SET a = 1         `TOP` is a keyword
#
# The two keywords could be excluded by a word list, the way
# `_NOT_AN_INSERT_TARGET` excludes the INSERT-position ones. The alias cannot:
# it is an arbitrary identifier the same statement declares further along.
# MEASURED over a 22-case battery: with UPDATE the pattern produced 3 false
# positives (those three), without it 0. The cost is one missed shape,
# `UPDATE claim SET a = 1` with a genuinely bare target, and it is small --
# a bare UPDATE only reaches here inside an object `kind` that is not
# procedural, and `translate` returns on SQ01 before any rule runs for the
# procedural ones.
#
# Two-part names cannot match: `(?!\s*\.)` rejects anything with a further
# part, so `dbo.claim` and the `default.AcmeDW.claim` the two-part rule has
# already emitted are both invisible here. `#tmp` and `@tv` are excluded by
# `_IDENT_PART` itself, which starts a bare part at `[A-Za-z_]`.
_ONE_PART_KEYWORDS = (
    r"CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?"
    r"|CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?VIEW"
    r"|ALTER\s+TABLE|ALTER\s+VIEW"
    r"|DROP\s+(?:TABLE|VIEW)(?:\s+IF\s+EXISTS)?"
    r"|TRUNCATE\s+TABLE"
    r"|INTO"
    r"|FROM"
    r"|JOIN"
)
_ONE_PART_RE = re.compile(
    rf"\b(?P<kw>{_ONE_PART_KEYWORDS})\s+(?P<name>{_IDENT_PART})"
    rf"(?!\s*\.)(?![\w$])",
    re.IGNORECASE)


def rule_one_part_names(sql: str, findings: list, *, item=None) -> str:
    """SQ11: flag a bare one-part object name. Never rewrites it.

    MEASURED on the tree before this rule, `translate(sql, kind="table",
    item="W")`:

        SELECT * FROM claim      ->  SELECT * FROM claim      findings: []

    Not rewritten, not flagged, nothing -- while the notebook path reports
    NB12_TABLE_UNKNOWN for the identical reference. A one-part name is
    ordinary in a DacFx export, where the object's own schema is implied, so
    this was silence on a common shape.

    **It is flagged and not rewritten, and that is the decision this rule
    exists to record.** The tempting reading is that a bare name needs only
    the item, which the two-part rule already takes from the export's folder
    -- so nothing new would be guessed. MEASURED, that reading is wrong, and
    `naming.aidp_schema` is where it goes wrong:

        aidp_table("AcmeDW", "claim", schema="")      default.AcmeDW.claim
        aidp_table("AcmeDW", "claim", schema="dbo")   default.AcmeDW.claim
        aidp_table("AcmeDW", "claim", schema="sales") default.AcmeDW_sales.claim

    Fabric's default schema is dropped from the AIDP name, so rewriting a
    bare name as though it meant `dbo` produces *exactly* the name `dbo.claim`
    would -- correct only when the session's default schema really is `dbo`.
    T-SQL resolves a one-part name against the **caller's** default schema
    (`CREATE USER ... WITH DEFAULT_SCHEMA`), and a DacFx export records no
    user and no default. Where that schema is not `dbo` the rewrite names a
    different table, silently, under a `rewrite` finding that reads as
    migrated work -- the same failure mode SQ19 was added to stop. A flag
    says the one thing that is true: the name is unqualified, and nothing in
    the export says which schema it means.

    Resolving it against the table catalog instead was considered and not
    taken. `candidates` lets a slot either side leave the schema unsaid, so a
    bare `claim` matches `dbo.claim` and `sales.claim` alike: a catalog
    holding both answers AMBIGUOUS, and one holding only `sales.claim`
    answers with a schema the query may well not have meant. Neither removes
    the guess; they move it.

    Gated on `item`, exactly as `rule_two_part_names` is, and for the same
    reason rather than because a flag needs a name built. A `%%tsql` notebook
    cell passes no `item`, and there NB15 has already put the catalog over
    every table name in the body -- including bare ones. Firing here too
    would report one reference twice, which is what SQ11's three-part rewrite
    did next to NB20 before `resolve_table_names` existed.
    """
    if not item:
        return sql
    view = _without_identifier_bodies(_masked(sql))
    defined_here = cte_names(view)
    for match in _ONE_PART_RE.finditer(view):
        end = match.end("name")
        if view[end:end + 1] == "(":
            # `FROM STRING_SPLIT('a,b', ',')` -- a table-valued function, not
            # a table. `sql_text.referenced_tables` draws the line at the
            # same place and for the same reason: a `(` with no space is a
            # call, and `INSERT INTO t (a, b)` keeps its space.
            continue
        if _in_a_from_taking_call(view, match.start("kw")):
            # `TRIM(BOTH ' ' FROM name)` / `EXTRACT(year FROM d)`: the FROM
            # belongs to the call and the name after it is a column.
            continue
        original = sql[match.start("name"):match.end("name")]
        if _bare(original).casefold() in defined_here:
            # A CTE this statement declares names a result set that lives for
            # one statement, so the reference is already correct and there is
            # nothing to say about it.
            continue
        findings.append(Finding(
            "SQ11_ONE_PART_NAME",
            f"{original!r} is a one-part name and was left exactly as "
            f"written. T-SQL resolves it against the caller's default schema, "
            f"which a Fabric export does not record, so the schema it means "
            f"is not in this file -- and Fabric's default schema is dropped "
            f"from an AIDP name, so qualifying it as {item!r} + `dbo` would "
            f"produce the same name as `dbo.{_bare(original)}` and would name "
            f"a different table wherever that assumption is wrong. Qualify it "
            f"in the source with the schema it means, and this tool will "
            f"rewrite it",
            "flag"))
    return sql


_DATEDIFF_RE = re.compile(r"\bDATEDIFF\s*\(", re.IGNORECASE)
_CHARINDEX_RE = re.compile(r"\bCHARINDEX\s*\(", re.IGNORECASE)
# Only 'day' has an order-preserving Spark equivalent. months_between returns a
# fraction, and the time parts need timestamp arithmetic — both are flagged.
_DAY_PARTS = {"day", "dd", "d"}


def rule_datediff(sql: str, findings: list) -> str:
    """SQ20: DATEDIFF(day, a, b) -> datediff(b, a). Operands reverse.

    T-SQL's DATEDIFF(part, start, end) is end - start; Spark's
    datediff(end, start) is also end - start, so the arguments must swap.
    Miss this and every duration comes out negated — no error, just wrong
    numbers, which is the whole reason this translator exists.

    Goes through `_rewrite_call`, so a DATEDIFF inside a DATEDIFF is rewritten
    on a later pass rather than overlapping its parent. Collecting every match
    in one pass raised, measured on
    `SELECT DATEDIFF(day, a, DATEDIFF(day, b, c))`:
    "overlapping replacements: 24-43 overlaps a range ending at 44".
    """
    def build(args):
        if len(args) != 3:
            return (None, Finding(
                "SQ20_DATEDIFF_ARITY",
                f"DATEDIFF with {len(args)} argument(s) is not the T-SQL "
                f"3-argument form; review by hand",
                "flag"))
        part = args[0].strip("'\"[]`").casefold()
        if part not in _DAY_PARTS:
            return (None, Finding(
                "SQ20_DATEDIFF_UNIT",
                f"DATEDIFF({part}, ...) has no direct Spark equivalent: "
                f"months_between returns a fraction and the time units need "
                f"timestamp arithmetic; rewrite by hand",
                "flag"))
        return (f"datediff({args[2]}, {args[1]})", Finding(
            "SQ20_DATEDIFF",
            f"DATEDIFF({part}, {args[1]}, {args[2]}) -> "
            f"datediff({args[2]}, {args[1]}); operand order reverses between "
            f"T-SQL and Spark",
            "rewrite"))
    return _rewrite_call(sql, _masked(sql), _DATEDIFF_RE, findings, build)


def rule_charindex(sql: str, findings: list) -> str:
    """SQ21: CHARINDEX(needle, hay[, start]) -> locate(needle, hay[, start]).

    locate() takes the same argument order as CHARINDEX and supports the
    optional start position. The tempting mapping, instr(str, substr), would
    require reversing the operands — this avoids that class of mistake.

    Goes through `_rewrite_call` for the nesting, which real SQL has:
    `SUBSTRING(s, CHARINDEX('-', s)+1, CHARINDEX('-', SUBSTRING(s,
    CHARINDEX('-', s)+1, 99)))` raised "overlapping replacements: 69-86
    overlaps a range ending at 94", measured, and lost the whole object to an
    error row that said nothing about the SQL.
    """
    def build(args):
        if len(args) not in (2, 3):
            return (None, Finding(
                "SQ21_CHARINDEX_ARITY",
                f"CHARINDEX with {len(args)} argument(s) is not a recognised "
                f"form; review by hand",
                "flag"))
        return (f"locate({', '.join(args)})", Finding(
            "SQ21_CHARINDEX",
            f"CHARINDEX -> locate, argument order preserved; note that mapping "
            f"to instr instead would require reversing the operands",
            "rewrite"))
    return _rewrite_call(sql, _masked(sql), _CHARINDEX_RE, findings, build)


_SUBSTRING_RE = re.compile(r"(?<![\w$.])SUBSTRING\s*\(", re.IGNORECASE)
_INTEGER_LITERAL_RE = re.compile(r"^[+-]?\d+$")


def rule_substring_from_zero(sql: str, findings: list) -> str:
    """SQ22: SUBSTRING(s, 0, n) -> substring(s, 1, n - 1).

    T-SQL's SUBSTRING with a start of 0 consumes one slot *before* the
    string, so the length is counted from position 0 and one fewer character
    comes back. Spark clamps a start of 0 to 1 and counts the full length.
    Measured on 'a-b-c' with CHARINDEX('-') = 2:

        T-SQL SUBSTRING(s, 0, 2)   -> 'a'
        Spark substring(s, 0, 2)   -> 'a-'
        Spark substring(s, 1, 2-1) -> 'a'

    so `SUBSTRING(s, 0, CHARINDEX(sep, s))` -- a common way to spell "the
    part before the first separator" -- returned the separator as well, with
    no finding raised.

    Only a literal start is decided here. `SUBSTRING(s, 1, n)` is identical
    in both dialects and is untouched; a computed start is left alone because
    whether it can reach 0 is not visible in the text, and the one idiom that
    produces it, `CHARINDEX(...) + 1`, never can.
    """
    def build(args):
        if len(args) != 3:
            return (None, None)  # not the T-SQL three-argument form
        source, start, length = (arg.strip() for arg in args)
        if not _INTEGER_LITERAL_RE.match(start):
            return (None, None)
        position = int(start)
        if position > 0:
            return (None, None)  # 1 and up mean the same thing in both
        if position < 0:
            # T-SQL counts the missing slots from a negative start too, and
            # this tool has no measurement of that case. A guessed formula
            # here would be the same shape of harm as the defect.
            return (None, Finding(
                "SQ22_SUBSTRING_START",
                f"SUBSTRING with a start of {position} is not a form this "
                f"tool maps: T-SQL counts the positions before the string "
                f"against the length and Spark clamps the start to 1, so the "
                f"two return different substrings. Left as written",
                "flag"))
        if _INTEGER_LITERAL_RE.match(length):
            # Worked out rather than left as `(2) - 1`, so the output reads
            # the way a hand rewrite would.
            adjusted = str(int(length) - 1)
        else:
            # Parenthesised because the length may be any expression, and
            # `a - b - 1` must subtract from the whole of it.
            adjusted = f"({length}) - 1"
        return (f"substring({source}, 1, {adjusted})", Finding(
            "SQ22_SUBSTRING_ZERO",
            f"SUBSTRING({source}, 0, {length}) -> "
            f"substring({source}, 1, {adjusted}); a start of 0 makes T-SQL "
            f"count one slot before the string against the length, so the "
            f"untranslated call returned one character too many -- measured "
            f"on 'a-b-c' with a length of 2, T-SQL gives 'a' and Spark's "
            f"substring(s, 0, 2) gives 'a-'",
            "rewrite"))
    return _rewrite_call(sql, _masked(sql), _SUBSTRING_RE, findings, build)


# `CONCAT_WS(` is deliberately not matched: the `(` has to follow the word
# CONCAT, so the longer name cannot reach this pattern. T-SQL's own CONCAT_WS
# skips NULL arguments and so does Spark's, which is why it needs no rule.
_CONCAT_RE = re.compile(r"(?<![\w$.])CONCAT\s*\(", re.IGNORECASE)


def rule_concat(sql: str, findings: list) -> str:
    """SQ31: CONCAT(a, b, ...) -> concat_ws('', a, b, ...).

    T-SQL's CONCAT reads a NULL argument as an empty string. Spark's concat
    propagates it. Measured on Spark 4.2.0:

        concat('a', NULL)             -> NULL
        concat_ws('', 'a', NULL)      -> 'a'
        concat_ws('', 'a', NULL, 'b') -> 'ab'

    so `concat_ws('', ...)` is the exact reproduction and the name-for-name
    mapping is not. Before this rule `SELECT CONCAT(a, b) FROM dbo.t` came
    back with CONCAT untouched and flags=0, and one NULL column emptied the
    whole expression.

    MUST stay ahead of `rule_string_concat` in RULES. That rule converts
    T-SQL's `+`, which *does* propagate NULL, and emits `concat(...)` for
    exactly that reason -- this pattern is case-insensitive and would read
    SQ40's own output as a CONCAT call and turn `'a' + NULL` from NULL into
    'a'. Asserted in the tests.

    Non-string arguments are no different after this than before: T-SQL
    converts them, and Spark's concat_ws takes the same argument types as
    concat, so whatever Spark did with a numeric operand it still does.
    """
    def build(args):
        if len(args) < 2:
            # T-SQL requires at least two. One argument is either not this
            # function or is already broken SQL; rewriting it would invent
            # a meaning for it.
            return (None, Finding(
                "SQ31_CONCAT_ARITY",
                f"CONCAT with {len(args)} argument(s) is not the T-SQL form, "
                f"which takes two or more; left as written",
                "flag"))
        return (f"concat_ws('', {', '.join(args)})", Finding(
            "SQ31_CONCAT",
            f"CONCAT({', '.join(args)}) -> concat_ws('', ...); T-SQL's CONCAT "
            f"reads a NULL argument as an empty string and Spark's concat "
            f"returns NULL for the whole expression, while concat_ws with an "
            f"empty separator skips the NULL -- measured on Spark 4.2.0, "
            f"concat('a', NULL) is NULL and concat_ws('', 'a', NULL) is 'a'",
            "rewrite"))
    return _rewrite_call(sql, _masked(sql), _CONCAT_RE, findings, build)


# `LOG10(` is not a match: the `(` has to follow the word LOG. `LOG10` means
# the same thing in both dialects and takes one argument, so it needs no rule.
_LOG_RE = re.compile(r"(?<![\w$.])LOG\s*\(", re.IGNORECASE)


def _swap_log(text: str, findings: list, seen: set) -> str:
    """LOG's operands swapped, innermost call included, in ONE pass.

    Not `_rewrite_call`, and the reason is the whole shape of this function.
    A swap is its own inverse: the fixed-point loop re-reads this rule's own
    output, sees a two-argument LOG again, and swaps it back -- forever, until
    the pass budget burns and SQ03 reports deep nesting for a flat expression.
    SQ20 escapes that only because its second reading is an arity error.

    So nesting is handled by recursion into the ARGUMENTS of the outermost
    calls instead of by re-scanning the result: each call is visited exactly
    once, and `LOG(LOG(x, 2), 10)` comes out `log(10, log(2, x))`.
    """
    view = _masked(text)
    items = []
    for match in _LOG_RE.finditer(view):
        split = split_call_arguments(view, match.end() - 1)
        if split is None:
            continue
        spans, close = split
        items.append((match.start(), close + 1, match.end() - 1, spans))
    replacements = []
    for start, end, open_index, spans in _outermost(items):
        old = [text[a:b] for a, b in spans]
        new = [_swap_log(part, findings, seen) for part in old]
        if len(spans) == 2:
            value, base = (part.strip() for part in new)
            findings.append(Finding(
                "SQ32_LOG",
                f"LOG({value}, {base}) -> log({base}, {value}); T-SQL spells "
                f"it LOG(value, base) and Spark spells it log(base, value), "
                f"so the operands reverse. Measured on Spark 4.2.0, "
                f"log(10, 100) is 2.0 and log(100, 10) is 0.5 -- an unswapped "
                f"call runs and returns a different number",
                "rewrite"))
            replacements.append((start, end, f"log({base}, {value})"))
            continue
        if len(spans) != 1:
            # T-SQL's LOG takes one argument or two. Anything else is not
            # this function, or is already broken; guessing an order for it
            # would be invention.
            key = ("SQ32_LOG_ARITY", len(spans))
            if key not in seen:
                seen.add(key)
                findings.append(Finding(
                    "SQ32_LOG_ARITY",
                    f"LOG with {len(spans)} argument(s) is neither T-SQL form "
                    f"-- LOG(value) or LOG(value, base) -- so the operand "
                    f"order cannot be decided; left as written",
                    "flag"))
        # One argument: the natural log in both dialects, nothing to swap.
        # Only a rewritten argument goes back, so a call this rule did not
        # change keeps its spacing exactly as the user wrote it.
        if new != old:
            replacements.append(
                (start, end, text[start:open_index + 1] + ",".join(new) + ")"))
    return apply_replacements(text, replacements)


def rule_log(sql: str, findings: list) -> str:
    """SQ32: LOG(value, base) -> log(base, value).

    T-SQL is LOG(value, base); Spark is LOG(base, value). Measured on Spark
    4.2.0: log(10, 100) -> 2.0 and log(100, 10) -> 0.5. Before this rule
    `SELECT LOG(x, 10) FROM dbo.t` passed through at flags=0 and returned a
    different number for every row.

    The one-argument form is the natural logarithm in both dialects and is
    deliberately untouched, as is LOG10, which the pattern cannot reach.
    """
    return _swap_log(sql, findings, set())


# name -> (spark_template, required_arity | None, finding_detail)
# The template is formatted with the original argument texts.
_RENAMES = {
    "isnull": ("coalesce({0}, {1})", 2,
               "ISNULL(a, b) -> coalesce(a, b); exact for the two-argument form"),
    "getdate": ("current_timestamp()", 0,
                "GETDATE() -> current_timestamp(); both return session-local now"),
    "sysdatetime": ("current_timestamp()", 0,
                    "SYSDATETIME() -> current_timestamp(); Spark's precision is lower "
                    "but the value is the same instant"),
    "space": ("repeat(' ', {0})", 1, "SPACE(n) -> repeat(' ', n)"),
    "square": ("power({0}, 2)", 1, "SQUARE(x) -> power(x, 2)"),
}
# Mappings that look obvious and are subtly wrong.
_RENAME_FLAGS = {
    "getutcdate": "GETUTCDATE() returns UTC, but Spark's current_timestamp() is "
                  "session-local; use to_utc_timestamp(current_timestamp(), <tz>) "
                  "or set the session timezone deliberately",
    "len": "T-SQL LEN(x) ignores trailing spaces but Spark's length(x) counts them; "
           "length(rtrim(x)) matches for trailing blanks, but Spark's rtrim strips "
           "all trailing whitespace, so confirm the intent before applying it",
}
_CALL_RE = re.compile(r"(?<![\w.])(?P<name>[A-Za-z_][\w]*)\s*\(")


# Functions whose rewrite depends on the arguments, not just their count.
# Each builder takes the argument texts and returns (replacement, finding),
# or (None, finding) to refuse. Before these, all three passed through at
# flags=0 -- graded PASS -- and failed on the cluster with
# UNRESOLVED_ROUTINE, measured live on AIDP Spark 3.5.0: Spark has none of
# them.
def _build_eomonth(args):
    if len(args) == 1:
        emitted = f"last_day({args[0]})"
    elif len(args) == 2:
        emitted = f"last_day(add_months({args[0]}, {args[1]}))"
    else:
        return (None, Finding(
            "SQ30_EOMONTH_ARITY",
            f"EOMONTH with {len(args)} argument(s) is not the T-SQL form, "
            f"which takes a date and an optional month offset; left as "
            f"written", "flag"))
    return (emitted, Finding(
        "SQ30_EOMONTH",
        f"EOMONTH({', '.join(args)}) -> {emitted}; both return the last day "
        f"of the month as a DATE, and add_months, like T-SQL's offset, "
        f"clamps to the month's end (2024-01-31 plus one month is "
        f"2024-02-29), so last_day of it is the same day", "rewrite"))


def _build_replicate(args):
    if len(args) != 2:
        return (None, Finding(
            "SQ30_REPLICATE_ARITY",
            f"REPLICATE with {len(args)} argument(s) is not the T-SQL form; "
            f"left as written", "flag"))
    text, count = args
    if _INTEGER_LITERAL_RE.match(count) and int(count) >= 0:
        emitted = f"repeat({text}, {count})"
        why = "exact for a non-negative count"
    else:
        # T-SQL returns NULL for a negative count and Spark's repeat returns
        # ''. The guard restores NULL: a negative count becomes a NULL count,
        # repeat(s, NULL) is NULL, and a NULL count makes the condition NULL,
        # which `if` sends to the else branch -- NULL again, as
        # REPLICATE(s, NULL) is. Inside repeat(...) rather than around it,
        # so the call still reads as a string term to SQ40: the padding
        # idiom `REPLICATE('0', n) + c` must still become concat(...).
        guarded = count if _IDENT_RUN_RE.fullmatch(count) else f"({count})"
        emitted = f"repeat({text}, if({guarded} < 0, NULL, {count}))"
        why = ("T-SQL returns NULL for a negative count and Spark's repeat "
               "returns an empty string, so the count is guarded; it is "
               "written twice, which only matters for a non-deterministic "
               "count such as RAND()")
    return (emitted, Finding(
        "SQ30_REPLICATE",
        f"REPLICATE({text}, {count}) -> {emitted}; {why}", "rewrite"))


# DATENAME part -> the Spark pattern that spells the same name. Only the two
# parts whose DATENAME is a NAME; for the numeric parts DATENAME returns the
# number as text, a different shape, and those are refused below.
_DATENAME_PATTERNS = {
    "month": "MMMM", "mm": "MMMM", "m": "MMMM",
    "weekday": "EEEE", "dw": "EEEE", "w": "EEEE",
}


def _build_datename(args):
    if len(args) != 2:
        return (None, Finding(
            "SQ30_DATENAME_ARITY",
            f"DATENAME with {len(args)} argument(s) is not the T-SQL form; "
            f"left as written", "flag"))
    part = args[0].strip("'\"[]`").casefold()
    pattern = _DATENAME_PATTERNS.get(part)
    if pattern is None:
        return (None, Finding(
            "SQ30_DATENAME_PART",
            f"DATENAME({args[0]}, ...) is not mapped: only month and weekday "
            f"are names, and for the other parts DATENAME returns the number "
            f"as text, which needs a cast chosen by hand (for example "
            f"CAST(year(d) AS STRING)). Left as written; Spark has no "
            f"DATENAME and fails with UNRESOLVED_ROUTINE", "flag"))
    emitted = f"date_format({args[1]}, '{pattern}')"
    return (emitted, Finding(
        "SQ30_DATENAME",
        f"DATENAME({args[0]}, {args[1]}) -> {emitted}; Spark formats names "
        f"in US English, which matches SQL Server under its default "
        f"us_english language. Under another SET LANGUAGE, SQL Server "
        f"returned the name in that language", "rewrite"))


_RENAME_BUILDERS = {
    "eomonth": _build_eomonth,
    "replicate": _build_replicate,
    "datename": _build_datename,
}


def _collect_scalar_renames(sql: str):
    view = _masked(sql)
    items = []
    for match in _CALL_RE.finditer(view):
        name = match.group("name").casefold()
        if name in _RENAME_FLAGS:
            items.append((match.start("name"), match.start("name"), "",
                          Finding(f"SQ30_{name.upper()}", _RENAME_FLAGS[name], "flag")))
            continue
        if name in _RENAME_BUILDERS:
            split = split_call_arguments(view, match.end() - 1)
            if split is None:
                continue
            spans, close = split
            args = [sql[a:b].strip() for a, b in spans]
            text, finding = _RENAME_BUILDERS[name](
                [] if args == [""] else args)
            items.append((match.start("name"),
                          match.start("name") if text is None else close + 1,
                          text or "", finding))
            continue
        if name not in _RENAMES:
            continue
        template, arity, detail = _RENAMES[name]
        split = split_call_arguments(view, match.end() - 1)
        if split is None:
            continue
        spans, close = split
        args = [sql[a:b].strip() for a, b in spans]
        if arity == 0:
            args = [a for a in args if a]
        if arity is not None and len(args) != arity:
            items.append((match.start("name"), match.start("name"), "", Finding(
                f"SQ30_{name.upper()}_ARITY",
                f"{name.upper()} with {len(args)} argument(s) does not match the "
                f"form this rule handles; review by hand", "flag")))
            continue
        items.append((match.start("name"), close + 1, template.format(*args),
                      Finding(f"SQ30_{name.upper()}", detail, "rewrite")))
    return items


def rule_scalar_renames(sql: str, findings: list) -> str:
    """SQ30: rename scalar functions whose Spark equivalent is exact.

    `_RENAMES` covers the ones a template can express; `_RENAME_BUILDERS`
    the ones whose rewrite depends on an argument -- EOMONTH's optional
    offset, REPLICATE's count, DATENAME's part.

    Runs to a fixed point so a nested call -- GETDATE() inside ISNULL(...),
    which appears in real warehouse views -- is renamed on a later pass rather
    than being lost to its parent's replacement.
    """
    seen = set()
    collected = []

    def collect(current):
        items = _collect_scalar_renames(current)
        # Zero-width entries are flags, not rewrites; emit each only once.
        out = []
        for start, end, text, finding in items:
            if start == end:
                key = (finding.rule, finding.detail)
                if key in seen:
                    continue
                seen.add(key)
                collected.append(finding)
                continue
            out.append((start, end, text, finding))
        return out

    result = _to_fixed_point(sql, findings, collect)
    findings.extend(collected)
    return result


_WS = " \t\r\n"
_SET_OP_RE = re.compile(r"\b(?:UNION|INTERSECT|EXCEPT)\b", re.IGNORECASE)


def _statement_bounds(view, index):
    """(start, end) of the depth-0 `;`-delimited statement containing `index`.

    `end` excludes trailing whitespace and the `;` itself, so it is where a
    trailing clause belongs. LIMIT used to be appended at the end of the
    whole *file*, so with two statements the cap landed on the wrong one.
    """
    start, depth = 0, 0
    for position in range(index):
        char = view[position]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == ";" and depth == 0:
            start = position + 1
    end, depth = len(view), 0
    for position in range(index, len(view)):
        char = view[position]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == ";" and depth == 0:
            end = position
            break
    while end > start and view[end - 1] in _WS:
        end -= 1
    return start, end


def _heads_a_statement(view, at: int) -> bool:
    """Whether the keyword matched at `at` is the first thing in a statement.

    Shared by the two rules that need it, which are the two whose keyword is
    also an ordinary word elsewhere: a session setting's `SET` is otherwise
    `UPDATE t SET c = 1`, and a `MERGE` statement's keyword is otherwise the
    `MERGE` of an `INNER MERGE JOIN` hint or a column called `merge`. Both
    statements always begin one, and neither of the other readings ever does.

    A `WITH` clause in front of the statement therefore hides it -- T-SQL
    allows `WITH c AS (...) MERGE ...` -- which costs a miss and never a
    false positive. `view` must be a masked copy, so a keyword inside a
    literal or a comment is not there to find.
    """
    start, _end = _statement_bounds(view, at)
    while start < len(view) and view[start] in _WS:
        start += 1
    return start == at
# A quoted identifier may contain spaces, so the pattern must understand the
# quoting rather than relying on a character class. `claim id` cut at the space
# produces mangled SQL, not an error.
_IDENT_PART = r"(?:`[^`]*`|\[[^\]]*\]|[A-Za-z_][\w$]*)"
_IDENT_RUN_RE = re.compile(rf"{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART})*")
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def _closing_index(view: str, index: int):
    """Index just past the bracket group opening at `index`, or None."""
    depth = 0
    for position in range(index, len(view)):
        if view[position] in "([":
            depth += 1
        elif view[position] in ")]":
            depth -= 1
            if depth == 0:
                return position + 1
    return None


# A bare SQL keyword is never a concat operand. A live run produced
# `concat(END, ' ', Region)` from `... END + ' ' + [Region]`, silently
# destroying the CASE it belonged to.
_TERM_KEYWORDS = frozenset("""
select from where group by having order union all distinct case when then else
end and or not null is in exists between like as on join inner left right full
outer cross apply insert update delete values set into top with over partition
asc desc""".split())


def _is_keyword_term(view: str, span) -> bool:
    return view[span[0]:span[1]].strip().casefold() in _TERM_KEYWORDS


def _term_forward(view: str, index: int):
    """(start, end) of the simple term beginning at or after `index`, else None."""
    length = len(view)
    while index < length and view[index] in _WS:
        index += 1
    if index >= length:
        return None
    start = index
    char = view[index]
    if char in "'\"":
        position = index + 1
        while position < length:
            if view[position] == char:
                if position + 1 < length and view[position + 1] == char:
                    position += 2
                    continue
                return (start, position + 1)
            position += 1
        return None
    if char == "(":
        end = _closing_index(view, index)
        return (start, end) if end else None
    match = _IDENT_RUN_RE.match(view, index)
    if match:
        end = match.end()
        probe = end
        while probe < length and view[probe] in " \t":
            probe += 1
        if probe < length and view[probe] == "(":
            call_end = _closing_index(view, probe)
            if call_end:
                end = call_end
        return (start, end)
    match = _NUMBER_RE.match(view, index)
    return (start, match.end()) if match else None


def _term_backward(view: str, index: int):
    """(start, end) of the simple term ending at or before `index`, else None."""
    while index > 0 and view[index - 1] in _WS:
        index -= 1
    if index == 0:
        return None
    end = index
    char = view[index - 1]
    if char in "'\"":
        position = index - 2
        while position >= 0:
            if view[position] == char:
                if position - 1 >= 0 and view[position - 1] == char:
                    position -= 2
                    continue
                return (position, end)
            position -= 1
        return None
    if char == ")":
        depth = 0
        position = index - 1
        while position >= 0:
            if view[position] == ")":
                depth += 1
            elif view[position] == "(":
                depth -= 1
                if depth == 0:
                    break
            position -= 1
        if position < 0:
            return None
        name = position
        while name > 0 and (view[name - 1].isalnum() or view[name - 1] in "_$."):
            name -= 1
        return (name, end)
    # Bare, backticked and bracketed identifiers all go through the same
    # quote-aware pattern; find the run that ends exactly here.
    for candidate in _IDENT_RUN_RE.finditer(view):
        if candidate.end() == index:
            return (candidate.start(), index)
    match = _NUMBER_RE.search(view[:index])
    if match and match.end() == index:
        return (match.start(), index)
    return None


def _is_string_term(view: str, span) -> bool:
    return view[span[0]] in "'\""


# T-SQL's numeric types, all of which outrank every string type in its data
# type precedence -- so a `+` with one of them on either side is addition,
# and the string is converted to the number, not the other way round.
_NUMERIC_TYPE_NAMES = frozenset("""
tinyint smallint int integer bigint bit decimal dec numeric float real money
smallmoney""".split())
_CONVERSION_CALL_RE = re.compile(
    r"(?P<name>TRY_CAST|CAST|TRY_CONVERT|CONVERT)\s*\(", re.IGNORECASE)
_AS_RE = re.compile(r"\bAS\s+", re.IGNORECASE)
_CONVERSION_TYPE_RE = re.compile(
    r"`?(?P<type>[A-Za-z_]\w*)`?\s*"
    r"(?:\(\s*(?:\d+|max)\s*(?:,\s*\d+\s*)?\))?", re.IGNORECASE)


def _conversion_target(view: str, span):
    """The type a CAST/CONVERT term converts to, casefolded, else None.

    Only when the whole term is that one call: `CAST(a AS int).x` is not.
    """
    head = _CONVERSION_CALL_RE.match(view, span[0])
    if head is None:
        return None
    split = split_call_arguments(view, head.end() - 1)
    if split is None or split[1] + 1 != span[1]:
        return None
    spans = split[0]
    if head.group("name").upper().endswith("CAST"):
        if len(spans) != 1:
            return None
        # The LAST `AS` at this depth; an inner CAST has its own parens.
        arg = view[spans[0][0]:spans[0][1]]
        found = None
        for found in _AS_RE.finditer(arg):
            pass
        if found is None:
            return None
        written = arg[found.end():].strip()
    else:
        if len(spans) < 2:
            return None
        written = view[spans[0][0]:spans[0][1]].strip()
    typed = _CONVERSION_TYPE_RE.fullmatch(written)
    return typed.group("type").casefold() if typed else None


# Calls that return a string whatever their arguments are, spelled as they
# reach rule_string_concat -- after SQ31 (CONCAT -> concat_ws), SQ22
# (SUBSTRING -> substring) and SQ30 (SPACE -> repeat), which run earlier.
# Without these, `LEFT(a, 2) + RIGHT(a, 2)` was left as `+`, and Spark casts
# both sides to double: measured live on AIDP, NULL where SQL Server returns
# the concatenation.
_STRING_FUNCTIONS = frozenset("""
left right upper lower ltrim rtrim trim substring replace concat concat_ws
format stuff replicate repeat space reverse quotename char nchar
datename date_format
""".split())
# A conversion to one of these is a string term. `string` is Spark's own
# spelling, for a CAST another rule has already rewritten.
_STRING_TYPE_NAMES = frozenset(
    "char nchar varchar nvarchar text ntext sysname string".split())
_CALL_HEAD_RE = re.compile(r"(?P<name>[A-Za-z_]\w*)\s*\(")


def _is_string_call(view: str, span) -> bool:
    """True when the whole term is one call known to return a string.

    COALESCE/ISNULL qualify only with a string literal among the arguments:
    their type is their arguments', and `coalesce(a, 0)` is not a string.
    A qualified name -- `dbo.left(x)` -- is a user function and does not.
    """
    head = _CALL_HEAD_RE.match(view, span[0])
    if head is None:
        return False
    split = split_call_arguments(view, head.end() - 1)
    if split is None or split[1] + 1 != span[1]:
        return False
    name = head.group("name").casefold()
    if name in ("coalesce", "isnull"):
        return any(view[a:b].strip().startswith("'") for a, b in split[0])
    return name in _STRING_FUNCTIONS


def _term_kind(view: str, span):
    """"string", "number", or None where the text does not say.

    Deliberately narrow: a term is typed only when its spelling alone fixes
    the type. A bare column is None, and so is anything this cannot read.
    """
    if _is_string_term(view, span):
        return "string"
    if _is_string_call(view, span):
        return "string"
    if _conversion_target(view, span) in _STRING_TYPE_NAMES:
        return "string"
    text = view[span[0]:span[1]].strip()
    if _NUMBER_RE.fullmatch(text):
        return "number"
    if _conversion_target(view, span) in _NUMERIC_TYPE_NAMES:
        return "number"
    return None


# Operators that take the term next to a `+` run before `+` does. T-SQL puts
# `* / %` above `+`, and `- & ^ |` level with it and left-associative -- so
# `x - 'a' + b` is `(x - 'a') + b`. Wrapping the run in concat(...) would
# silently regroup either. After the run only the higher tier matters: a
# same-level operator there applies to the whole run, which concat keeps.
_BINDS_BEFORE = frozenset("*/%-&^|~")
_BINDS_AFTER = frozenset("*/%")


def _binding_neighbour(view: str, start: int, end: int):
    """The operator that would regroup a `+` run spanning start..end, or None."""
    before = start
    while before > 0 and view[before - 1] in _WS:
        before -= 1
    if before > 0 and view[before - 1] in _BINDS_BEFORE:
        return view[before - 1]
    after = end
    while after < len(view) and view[after] in _WS:
        after += 1
    if after < len(view) and view[after] in _BINDS_AFTER:
        return view[after]
    return None


def _collect_string_concat(sql: str):
    view = _masked(sql)
    consumed = set()
    items = []
    for match in re.finditer(r"\+", view):
        if match.start() in consumed:
            continue
        left = _term_backward(view, match.start())
        right = _term_forward(view, match.end())
        if left is None or right is None:
            continue
        terms = [left, right]
        consumed.add(match.start())
        position = right[1]
        while True:
            probe = position
            while probe < len(view) and view[probe] in _WS:
                probe += 1
            if probe >= len(view) or view[probe] != "+":
                break
            following = _term_forward(view, probe + 1)
            if following is None:
                break
            consumed.add(probe)
            terms.append(following)
            position = following[1]
        kinds = [_term_kind(view, term) for term in terms]
        if "string" not in kinds:
            continue
        keyword = next((t for t in terms if _is_keyword_term(view, t)), None)
        if keyword is not None:
            items.append((terms[0][0], terms[0][0], "", Finding(
                "SQ41_CONCAT_KEYWORD",
                f"'+' concatenation next to the SQL keyword "
                f"{view[keyword[0]:keyword[1]].strip().upper()!r} was left alone: "
                f"one operand is a whole expression (a CASE, for example), which "
                f"this rule cannot span. Rewrite it to concat(...) by hand -- "
                f"Spark's '+' on strings yields NULL",
                "flag")))
            continue
        operator = _binding_neighbour(view, terms[0][0], terms[-1][1])
        if operator is not None:
            items.append((terms[0][0], terms[0][0], "", Finding(
                "SQ41_CONCAT_PRECEDENCE",
                f"'+' concatenation next to the operator {operator!r} was "
                f"left alone: {operator!r} takes the neighbouring term first, "
                f"so the operand of '+' is a larger expression than this rule "
                f"can span. Rewrite it to concat(...) by hand -- Spark's '+' "
                f"on strings yields NULL",
                "flag")))
            continue
        if "number" in kinds:
            # Not concatenation at all. T-SQL resolves `+` by data type
            # precedence, and every numeric type outranks every string, so
            # the string is converted and the two are ADDED: live on AIDP,
            # `'5' + 3` became concat('5', 3), which is '53' where SQL
            # Server returns 8. No rewrite is exact -- Spark's own `+`
            # converts to DOUBLE (8.0) and returns NULL where T-SQL raises
            # on a non-numeric string, and a CAST to the "right" type
            # disagrees with T-SQL about '5.5' -- so it stays as written.
            number = terms[kinds.index("number")]
            items.append((terms[0][0], terms[0][0], "", Finding(
                "SQ42_STRING_PLUS_NUMBER",
                f"'+' between a string and the number "
                f"{sql[number[0]:number[1]].strip()!r} was left as written: "
                f"T-SQL converts the string to the number's type and adds, "
                f"so '5' + 3 is 8, not '53', and concat(...) would be wrong. "
                f"As written Spark adds as DOUBLE (8.0) and returns NULL "
                f"where T-SQL raises a conversion error; CAST the string to "
                f"the intended numeric type by hand",
                "flag")))
            continue
        texts = [sql[start:end].strip() for start, end in terms]
        items.append((terms[0][0], terms[-1][1], f"concat({', '.join(texts)})",
                      Finding(
                          "SQ40_STRING_CONCAT",
                          f"'+' concatenation of {len(terms)} terms -> concat(...); "
                          f"in Spark '+' between strings coerces to numeric and "
                          f"yields NULL instead of concatenating",
                          "rewrite")))
    return items


def rule_string_concat(sql: str, findings: list) -> str:
    """SQ40: 'lit' + x -> concat('lit', x) when a string term is involved.

    Spark's `+` on strings coerces to numeric and yields NULL rather than
    concatenating, so an unrewritten T-SQL concat is a silent wrong answer.
    Operand types cannot be inferred from the expression alone, so this fires
    only when at least one term's spelling makes it a string. Runs of bare
    identifiers are left alone deliberately (spec §8.4).

    A string term is a string literal, a call known to return a string --
    LEFT, UPPER, REPLACE, CAST(... AS varchar), and the rest of
    `_STRING_FUNCTIONS` -- or COALESCE/ISNULL with a string literal in it.

    SQ41_CONCAT_PRECEDENCE: a run beside `*`, `/`, `%`, or preceded by a
    `-`/bitwise operator, is refused: that operator groups the neighbouring
    term with something outside the run, and concat(...) would regroup it.

    SQ42_STRING_PLUS_NUMBER: a run with a numeric literal, or a CAST/CONVERT
    to a numeric type, in it is refused and flagged. That `+` is addition in
    T-SQL, not concatenation; see the comment where it is raised.

    Runs to a fixed point: a `+` run nested inside a parenthesised term -- a
    CASE expression inside a larger concatenation, which real warehouse views
    contain -- is converted on a later pass.
    """
    return _to_fixed_point(sql, findings, _collect_string_concat)


_TOP_RE = re.compile(
    r"\bTOP\s*(?:\(\s*(?P<paren>[^)]*?)\s*\)|(?P<bare>@?[\w.]+))"
    r"(?P<tail>\s+PERCENT\b|\s+WITH\s+TIES\b)?",
    re.IGNORECASE)
_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)
_INTO_RE = re.compile(
    rf"\bINTO\s+(?P<target>#{{0,2}}{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}})",
    re.IGNORECASE)
def _depth_at(view: str, index: int) -> int:
    """Parenthesis nesting depth at `index` in a masked view."""
    depth = 0
    for position in range(index):
        if view[position] == "(":
            depth += 1
        elif view[position] == ")":
            depth -= 1
    return depth


# T-SQL's five DML verbs, which are also exactly the five that can own an
# `INTO` and the five that can carry a `TOP`. `SELECT ... INTO` creates a
# table; `INSERT [TOP (n)] INTO` writes to one that exists;
# `UPDATE`/`DELETE`/`MERGE` reach `INTO` through their `OUTPUT` clause. Only
# the first is a CTAS -- and only the first two can take a trailing LIMIT in
# Spark, which is what `rule_top` reads this for.
_STATEMENT_VERB_RE = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE)


def _owning_verb(view: str, start: int, index: int) -> str:
    """Which statement verb owns the clause at `index`, upper-cased.

    The last verb before the `INTO` that is no deeper than the `INTO` is the
    one it belongs to: a verb inside a subquery or a CTE body cannot own a
    clause outside it. `start` is the statement's start, so the scan never
    reaches back over a `;` into the previous statement.

    Returns "" when there is no verb at all in front of it, which is not a
    statement either caller can rewrite.

    Two rules ask: `rule_select_into`, for whose `INTO` a given one is, and
    `rule_top`, for which statement a `TOP` caps. It was named
    `_into_owner` while only the first asked.
    """
    here = _depth_at(view, index)
    owner, depth, position = "", 0, start
    for match in _STATEMENT_VERB_RE.finditer(view, start, index):
        depth += (view.count("(", position, match.start())
                  - view.count(")", position, match.start()))
        position = match.start()
        if depth <= here:
            owner = match.group(1).upper()
    return owner


# The statement verbs T-SQL lets `TOP` modify whose *Spark* statement takes
# no trailing LIMIT, so SQ50's rewrite cannot be applied to one. LIMIT is a
# clause of a query; MERGE, UPDATE and DELETE are not queries.
#
# MEASURED on pyspark 3.5.0 -- the version the cluster runs -- JAVA_HOME=
# openjdk@21, master local[1], session defaults (spark.sql.ansi.enabled
# false). "PARSED" means the statement got past the parser and failed on
# TABLE_OR_VIEW_NOT_FOUND for the probe's empty catalog:
#
#   MERGE INTO t USING s ON t.a=s.a WHEN MATCHED THEN UPDATE SET t.b=s.b
#                                        PARSED
#   ... the same statement + ` LIMIT 10`  PARSE_SYNTAX_ERROR
#                                          "Syntax error at or near 'LIMIT'"
#                                          (line 1, pos 69)
#   DELETE FROM t WHERE a=1               PARSED
#   DELETE FROM t WHERE a=1 LIMIT 10      PARSE_SYNTAX_ERROR at 'LIMIT' (24)
#   UPDATE t SET b=1                      PARSED
#   UPDATE t SET b=1 LIMIT 10             PARSE_SYNTAX_ERROR at 'LIMIT' (17)
#
# SELECT and INSERT are absent because both accept it, measured on the same
# session -- `INSERT INTO t SELECT a FROM s LIMIT 10` and
# `INSERT INTO t VALUES (1) LIMIT 10` both PARSED -- so the rewrite is
# correct for them and is what SQ50 is for.
_NO_LIMIT_VERBS = frozenset({"MERGE", "UPDATE", "DELETE"})


def _statement_end(view: str) -> int:
    """Index where a trailing LIMIT should be inserted: before any final ';'."""
    end = len(view)
    while end > 0 and view[end - 1] in _WS:
        end -= 1
    if end > 0 and view[end - 1] == ";":
        end -= 1
        while end > 0 and view[end - 1] in _WS:
            end -= 1
    return end


def rule_top(sql: str, findings: list) -> str:
    """SQ50: SELECT TOP n -> SELECT ... LIMIT n, only for the outermost query.

    LIMIT binds to the statement it terminates. Appending it for a TOP that
    lives inside a subquery would move the row cap to the outer query and
    silently change the result, so that case is flagged instead -- as is a
    TOP in a statement with UNION/INTERSECT/EXCEPT, where a trailing LIMIT
    would cap the whole set operation rather than one branch.

    T-SQL lets TOP modify five verbs and Spark takes a trailing LIMIT on two
    of them, so the verb decides as well as the position. MEASURED on
    2a223bd, `translate(sql, kind="view", item="AcmeDW")`:

        MERGE TOP (10) INTO dbo.t USING s ON 1=1
          -> MERGE INTO default.AcmeDW.t USING s ON 1=1 LIMIT 10
        UPDATE TOP (10) dbo.t SET a = 1  -> UPDATE dbo.t SET a = 1 LIMIT 10
        DELETE TOP (10) FROM dbo.t
          -> DELETE FROM default.AcmeDW.t LIMIT 10

    -- all three reported SQ50_TOP as a `rewrite`, and all three are
    PARSE_SYNTAX_ERROR on Spark 3.5.0 at the word LIMIT. See
    `_NO_LIMIT_VERBS` for the session and the verdicts either side. Those
    now get SQ50_TOP_NO_LIMIT and keep their TOP; SELECT and INSERT, which
    Spark does accept a LIMIT on, are unchanged.

    A TOP inside a quoted identifier is not the keyword and is skipped; the
    masking module knows where those are.
    """
    view = _masked(sql)
    identifiers = _identifier_spans(view)
    replacements = []
    for match in _TOP_RE.finditer(view):
        if any(start <= match.start() < end for start, end in identifiers):
            # `[TOP secret]` is a column name. The masking blanks literal
            # bodies but keeps identifier bodies visible on purpose -- the
            # naming rules need to read the name -- so this rule has to ask
            # where the identifiers are. Measured: the correctly translated
            # `SELECT [TOP secret], [it's] FROM dbo.t` carried
            # SQ50_TOP_NON_LITERAL and needed a human for a column name.
            continue
        count = (match.group("paren") or match.group("bare") or "").strip()
        tail = (match.group("tail") or "").strip()
        if tail:
            findings.append(Finding(
                "SQ50_TOP_UNSUPPORTED",
                f"TOP ... {tail.upper()} has no direct Spark equivalent; "
                f"rewrite with a window function",
                "flag"))
            continue
        if not count or count.startswith("@") or not count.replace(".", "").isdigit():
            findings.append(Finding(
                "SQ50_TOP_NON_LITERAL",
                f"TOP {count!r} is not a literal row count; Spark's LIMIT requires "
                f"a constant",
                "flag"))
            continue
        if _depth_at(view, match.start()) != 0:
            findings.append(Finding(
                "SQ50_TOP_SUBQUERY",
                f"TOP {count} appears inside a subquery; LIMIT would attach to the "
                f"outer query and change the result, so it was left in place",
                "flag"))
            continue
        start, end = _statement_bounds(view, match.start())
        verb = _owning_verb(view, start, match.start())
        if verb in _NO_LIMIT_VERBS:
            findings.append(Finding(
                "SQ50_TOP_NO_LIMIT",
                f"TOP {count} caps a {verb} statement, and Spark's {verb} "
                f"takes no LIMIT: appending one is PARSE_SYNTAX_ERROR at "
                f"the word LIMIT on 3.5.0, so the whole statement stops "
                f"parsing. LIMIT is a clause of a query and a {verb} is not "
                f"a query. TOP {count} is left exactly where it was -- also "
                f"not Spark, and the statement needs a human either way -- "
                f"rather than moved somewhere it changes what rows are "
                f"written. Cap the source instead: a subquery or CTE with "
                f"its own LIMIT {count}, used as the {verb}'s source",
                "flag"))
            continue
        if any(_depth_at(view, m.start()) == 0
               for m in _SET_OP_RE.finditer(view, start, end)):
            findings.append(Finding(
                "SQ50_TOP_SET_OPERATION",
                f"TOP {count} in a statement with UNION/INTERSECT/EXCEPT; a "
                f"trailing LIMIT would cap the whole set operation, not this "
                f"branch, so it was left in place -- rewrite with a subquery",
                "flag"))
            continue
        # `TOP n` sits between two runs of whitespace and removing only the
        # keyword leaves both, so the run in FRONT of it goes with it. Taken
        # from the masked view, and only for a TOP actually being removed.
        #
        # This replaced a document-wide, unmasked, unconditional
        # `re.sub(r"(\bSELECT(?:\s+DISTINCT)?)\s{2,}", r"\1 ", ...)` that
        # ran on every object, including objects with no TOP anywhere.
        # Measured, all three with findings=[]:
        #
        #   SELECT 'SELECT   me' AS a FROM dbo.t
        #     -> SELECT 'SELECT me' AS a FROM dbo.t     (a user's string value)
        #   -- SELECT   a\nSELECT b FROM dbo.t
        #     -> -- SELECT a\nSELECT b FROM dbo.t       (a comment)
        #   SELECT\n    a,\n    b\nFROM dbo.t
        #     -> SELECT a,\n    b\nFROM dbo.t          (the layout of every
        #                                               multi-line object)
        #
        # Taking the leading run rather than one trailing space is what keeps
        # the third case right when a TOP *is* removed: `SELECT\n  TOP 5\n  a`
        # comes out `SELECT\n  a` with its line break and indent intact.
        cut_start, cut_end = match.start(), match.end()
        if cut_end < len(view) and view[cut_end] in _WS:
            while cut_start > 0 and view[cut_start - 1] in _WS:
                cut_start -= 1
        replacements.append((cut_start, cut_end, ""))
        replacements.append((end, end, f" LIMIT {count}"))
        findings.append(Finding(
            "SQ50_TOP", f"TOP {count} -> LIMIT {count} at the end of the statement",
            "rewrite"))
    return apply_replacements(sql, replacements)


def rule_select_into(sql: str, findings: list) -> str:
    """SQ51: SELECT ... INTO t ... -> CREATE TABLE t AS SELECT ... .

    The CTAS is built from the STATEMENT that contains the INTO, not from the
    whole document. It used to be the whole document, and in a multi-statement
    file that populated the created table from the wrong query. Measured:

      SELECT * FROM dbo.b;
      SELECT a INTO dbo.u FROM dbo.t
        -> CREATE TABLE default.W.u AS SELECT * FROM default.W.b;
           SELECT a FROM default.W.t                        flags=0  PASS

    `dbo.u` created from `dbo.b` instead of `dbo.t`, and the statement that
    had the INTO silently stripped of its target: a table with the wrong
    contents, graded PASS. `_statement_bounds` already answers "which
    `;`-delimited statement is this offset in", which is all this needed --
    and SQ02 now writes that `;` where a `GO` stood, so the case arrives from
    every batched export rather than only from a hand-written file.

    A refusal no longer abandons the rest of the file: a temp-table target or
    an INTO inside a subquery is flagged and skipped, and the other
    statements are still rewritten.

    `INTO` belongs to more than one statement, so which one owns it is asked
    per statement rather than per document. This used to be
    `if _INSERT_RE.search(view): return sql` -- one `INSERT` anywhere in the
    file disarmed the rule for all of it. Measured on Spark 4.2.0
    (pyspark 4.2.0, JAVA_HOME=openjdk@21, defaults, `USING parquet`) before
    that guard was replaced:

      INSERT INTO dbo.log VALUES (1);
      SELECT a INTO dbo.u FROM dbo.t
        -> INSERT INTO default.W.log VALUES (1);
           SELECT a INTO default.W.u FROM default.W.t   findings: SQ11 x3

      SELECT a INTO u FROM t          PARSE_SYNTAX_ERROR at or near 'u'

    A load and a staging SELECT INTO in one `.sql` is an ordinary warehouse
    file, and it graded clean with unrunnable SQL in the artifact -- worse
    than the over-refusal the guard was avoiding. `_owning_verb` answers the
    question the guard was approximating, and answers it for the OUTPUT
    clause too, which the old guard did not cover at all:

      SELECT a FROM dbo.x;
      DELETE FROM dbo.t OUTPUT deleted.id INTO dbo.audit
        -> ... CREATE TABLE default.W.audit AS
               DELETE FROM default.W.t OUTPUT deleted.id    SQ51_SELECT_INTO

    A DELETE wrapped in a CTAS, reported as a rewrite. There is no INSERT in
    that file, so no document-wide INSERT check could ever have caught it.
    """
    view = _masked(sql)
    if not _SELECT_RE.search(view):
        return sql
    replacements = []
    wrapped = set()
    for match in _INTO_RE.finditer(view):
        target = match.group("target").strip()
        if target.startswith("#"):
            # Left as written, and NOT reported here: `rule_temp_table` has
            # already named this target, with the reason. Reporting it twice
            # under one id with two different details is how a rule set
            # starts to read like noise.
            continue
        start, end = _statement_bounds(view, match.start())
        if _owning_verb(view, start, match.start()) != "SELECT":
            # Somebody else's INTO: the target of an `INSERT [TOP (n)] INTO`,
            # or the destination of an `OUTPUT ... INTO` on an INSERT, UPDATE,
            # DELETE or MERGE. Not a finding -- those statements are ordinary
            # and SQ11 has already qualified the name -- and, crucially, not a
            # reason to stop reading the file.
            continue
        if _depth_at(view, match.start()) != 0:
            findings.append(Finding(
                "SQ51_INTO_SUBQUERY",
                f"SELECT INTO {target} appears inside a subquery; rewrite by hand",
                "flag"))
            continue
        # `_statement_bounds` starts just after the previous `;`, so the
        # newline between statements is inside the span. Stepping over it
        # keeps it in the output instead of gluing the CTAS to the `;`.
        while start < end and view[start] in _WS:
            start += 1
        if (start, end) in wrapped:
            # Two depth-0 INTOs in one statement is not valid T-SQL, but the
            # pattern can see it, and wrapping the same span twice raises on
            # overlapping replacements. Refused, named, and the first one is
            # left standing.
            findings.append(Finding(
                "SQ51_INTO_TWICE",
                f"a second INTO ({target}) in the same statement: one "
                f"statement creates one table, so this is either not a "
                f"SELECT ... INTO or the SQL is already malformed. Left as "
                f"written inside the statement the first INTO produced",
                "flag"))
            continue
        wrapped.add((start, end))
        # Remove `INTO t` and one of the two spaces it sat between. A global
        # whitespace collapse used to reach inside string literals too.
        cut_start, cut_end = match.start(), match.end()
        if (cut_start > start and sql[cut_start - 1] in " \t"
                and cut_end < end and sql[cut_end] in " \t"):
            cut_end += 1
        statement = apply_replacements(
            sql[start:end], [(cut_start - start, cut_end - start, "")])
        replacements.append(
            (start, end, f"CREATE TABLE {target} AS {statement.strip()}"))
        findings.append(Finding(
            "SQ51_SELECT_INTO",
            f"SELECT ... INTO {target} -> CREATE TABLE {target} AS "
            f"{' '.join(statement.split())}",
            "rewrite"))
    return apply_replacements(sql, replacements)


# T-SQL's second spelling of a column alias, `SELECT <alias> = <expr>`, and
# the one Spark reads as something else entirely. MEASURED on the AIDP
# cluster (Spark 3.5.0, spark.sql.ansi.enabled false, 2026-09-30):
#
#   SELECT total = qty * price AS v
#     FROM (SELECT 6 AS total, 2 AS qty, 3 AS price)      -> v = true
#
# Spark parsed `total = qty * price` as a comparison and returned a boolean,
# where T-SQL returns 6 in a column named `total`. No error: the statement
# runs, the column has the wrong name, the wrong type and the wrong value.
#
# At depth 0 of a T-SQL select list a leading `ident =` is ALWAYS an alias,
# because T-SQL has no boolean select items -- `SELECT a = b` cannot mean a
# comparison there. The one other reading is `SELECT @v = expr`, variable
# assignment, which starts with `@` and is left alone (a list containing one
# is a variable-assigning SELECT and is skipped whole). `=` anywhere else --
# WHERE, ON, HAVING, CASE WHEN, UPDATE ... SET, a function argument, a
# subquery's own WHERE -- is not at the head of a select item and is never
# looked at.
#
# The alias head: a bare word, a backticked name (SQ10 has already run), a
# bracketed or double-quoted one SQ10 declined, or a string literal, which
# T-SQL also accepts as an alias (`SELECT 'Total' = x`). `=` not followed by
# `=` or `>`, so nothing that merely starts with one is taken.
_ALIAS_HEAD_RE = re.compile(
    r"(?P<alias>`[^`]*`|\[[^\]]*\]|\"[^\"]*\"|'[^']*'|[A-Za-z_][\w$#]*)"
    r"\s*=(?![=>])\s*")
# `SELECT [ALL|DISTINCT] [TOP n | TOP (expr)] [PERCENT] [WITH TIES]`: what
# stands between the keyword and the first select item. TOP is still in the
# text here -- rule_top runs later.
_SELECT_PREFIX_RE = re.compile(
    r"\bSELECT\b(?:\s+(?:ALL|DISTINCT)\b)?"
    r"(?:\s+TOP\b\s*(?:\d+|(?=\()))?", re.IGNORECASE)
_TOP_TAIL_RE = re.compile(
    r"(?:\s+PERCENT\b)?(?:\s+WITH\s+TIES\b)?", re.IGNORECASE)
# What ends a select list at its own depth. The statement verbs are here so
# that two statements with no `;` between them do not read as one list.
_ALIAS_LIST_END_RE = re.compile(
    r"(?:FROM|INTO|WHERE|GROUP|HAVING|ORDER|UNION|INTERSECT|EXCEPT|WINDOW|"
    r"LIMIT|OPTION|FOR|SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|"
    r"WITH)\b", re.IGNORECASE)
# Words that can open a select item and are never an alias.
_NOT_AN_ALIAS = frozenset({
    "case", "not", "null", "exists", "cast", "distinct", "all", "top"})


def _select_items(view: str, start: int):
    """(lo, hi) of each depth-0 item of the select list starting at `start`."""
    items, depth, item_start, position = [], 0, start, start
    while position < len(view):
        char = view[position]
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                break
            depth -= 1
        elif depth == 0:
            if char == ";":
                break
            if char == ",":
                items.append((item_start, position))
                item_start = position + 1
            elif ((char.isalpha()) and
                  (position == 0 or not (view[position - 1].isalnum()
                                         or view[position - 1] in "_$#@`"))
                  and _ALIAS_LIST_END_RE.match(view, position)):
                break
        position += 1
    items.append((item_start, position))
    return items


def _spark_alias(text: str) -> str:
    """The alias as Spark should read it: bare words stay bare."""
    if text[0] == "`":
        return text
    if text[0] in "[\"'":
        body = text[1:-1]
        if text[0] == "[":
            body = body.replace("]]", "]")
        else:
            body = body.replace(text[0] * 2, text[0])
        return "`" + body.replace("`", "``") + "`"
    return text


def rule_alias_assignment(sql: str, findings: list) -> str:
    """SQ54: `SELECT alias = expr` -> `SELECT expr AS alias`.

    See `_ALIAS_HEAD_RE` for the measurement and for why the leading
    `ident =` of a select item is always an alias in T-SQL. Every SELECT in
    the text is visited -- a CTE body, a subquery, each UNION branch -- and
    each list is read at its own depth, so the `=` of a nested WHERE or of
    an `IIF(a = b, ...)` is inside parentheses and never reaches the test.

    Each rewrite is two small edits -- the `alias =` head removed, ` AS
    alias` inserted after the item's last non-blank character -- rather than
    one replacement of the whole item, so an alias-assigned item holding a
    subquery that has its own alias-assigned items rewrites both in one pass
    without overlapping edits.

    Runs after SQ10 so a bracketed alias is already backticked, and before
    SQ40 and the name rules, none of which reads an alias. The translated
    output has NOT been run on a cluster: only the untranslated statement
    above was, and the rewrite's shape is covered by text tests.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for match in _SELECT_PREFIX_RE.finditer(view):
        start = match.end()
        if view[start:start + 1] == "(" and re.search(
                r"TOP\s*$", view[match.start():start], re.IGNORECASE):
            closing = _closing_index(view, start)
            if closing is None:
                continue
            start = closing
        start = _TOP_TAIL_RE.match(view, start).end()
        items = _select_items(view, start)
        heads = []
        for lo, hi in items:
            while lo < hi and view[lo] in _WS:
                lo += 1
            heads.append(lo)
        if any(view[lo:lo + 1] == "@" for lo in heads):
            continue  # SELECT @v = expr: variable assignment, not an alias
        for (_, hi), lo in zip(items, heads):
            head = _ALIAS_HEAD_RE.match(view, lo, hi)
            if head is None:
                continue
            alias_view = head.group("alias")
            if alias_view.casefold() in _NOT_AN_ALIAS:
                continue
            end = hi
            while end > head.end() and view[end - 1] in _WS:
                end -= 1
            if end <= head.end():
                continue
            alias = sql[head.start("alias"):head.end("alias")]
            spark_alias = _spark_alias(alias)
            expr = " ".join(sql[head.end():end].split())
            replacements.append((lo, head.end(), ""))
            replacements.append((end, end, f" AS {spark_alias}"))
            findings.append(Finding(
                "SQ54_ALIAS_ASSIGNMENT",
                f"{alias} = {expr} -> {expr} AS {spark_alias}; T-SQL's "
                f"`alias = expr` select item is a column alias, and Spark "
                f"reads the same text as a comparison -- it runs, and "
                f"returns a boolean where T-SQL returns the value",
                "rewrite"))
    return apply_replacements(sql, replacements)


# `INSERT [TOP (n)] [INTO] target`: T-SQL's INTO is optional and DacFx-era
# hand-written loads leave it out. The `(?P<top>...)` group is here so the
# target is still found when it is not the first thing after the keyword.
_INSERT_TARGET_RE = re.compile(
    rf"\bINSERT\s+(?:TOP\s*\([^)]*\)\s*)?"
    rf"(?P<target>{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}})(?![\w.])",
    re.IGNORECASE)
# What can stand where the target would and is not one. `INTO` means the
# statement already has it; the rest are the INTO-less spellings that name no
# table at this position -- including MERGE's `WHEN NOT MATCHED THEN INSERT
# VALUES (...)`, whose INSERT has no target at all.
_NOT_AN_INSERT_TARGET = frozenset({
    "into", "values", "default", "select", "exec", "execute", "with"})
# `BULK INSERT dbo.t FROM 'file'` is a different statement whose target does
# NOT take INTO. It has no Spark form either, but inventing `BULK INSERT INTO`
# would turn something a reader can recognise into something nobody wrote.
_BULK_BEFORE_RE = re.compile(r"\bBULK\s*$", re.IGNORECASE)


def rule_insert_without_into(sql: str, findings: list) -> str:
    """SQ52: `INSERT t VALUES (...)` -> `INSERT INTO t VALUES (...)`.

    T-SQL makes INTO optional. Two things went wrong without it, measured on
    the tree before this rule:

      INSERT dbo.t VALUES (1)      -> INSERT dbo.t VALUES (1)            []
      INSERT INTO dbo.t VALUES (1) -> INSERT INTO default.W.t VALUES (1) SQ11

    The target is the one name `_OBJECT_KEYWORDS` could not see -- SQ11
    anchors on the word INTO -- so the write landed in whatever catalog
    happened to be current rather than in the table the plan names, with no
    finding to say so. And the statement does not run in the first place:
    measured on Spark 4.2.0 (pyspark 4.2.0, JAVA_HOME=openjdk@21, session
    defaults, target created `USING parquet`),

      INSERT t VALUES (1, 2, 'x')      PARSE_SYNTAX_ERROR at or near 't'
      INSERT INTO t VALUES (1, 2, 'x') ACCEPTED

    so writing the keyword in is both the fix for the syntax and the thing
    that lets SQ11 qualify the name. It runs before SQ11 for that reason.

    Nothing else about the statement is touched, and a statement that already
    has INTO is left exactly alone.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for match in _INSERT_TARGET_RE.finditer(view):
        target = match.group("target")
        if _bare(target).lower() in _NOT_AN_INSERT_TARGET:
            continue
        if _BULK_BEFORE_RE.search(view, 0, match.start()):
            continue
        at = match.start("target")
        replacements.append((at, at, "INTO "))
        findings.append(Finding(
            "SQ52_INSERT_NO_INTO",
            f"INSERT {target} -> INSERT INTO {target}; T-SQL makes INTO "
            f"optional and Spark does not (PARSE_SYNTAX_ERROR at the target "
            f"name), and without the keyword the table rule cannot see this "
            f"as an object position, so the write was left unqualified",
            "rewrite"))
    return apply_replacements(sql, replacements)


# `MERGE [TOP (n) [PERCENT]] [INTO] target`: T-SQL's INTO is optional here
# too, and this is the second of the two statements that takes its target
# with no keyword in front of it. `PERCENT` is part of T-SQL's TOP clause on
# MERGE and is consumed with it so the target is still found behind it.
_MERGE_TARGET_RE = re.compile(
    rf"\bMERGE\s+(?:TOP\s*\([^)]*\)\s*(?:PERCENT\s*)?)?"
    rf"(?P<target>{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}})(?![\w.])",
    re.IGNORECASE)
# What can stand where the target would and is not one. `INTO` means the
# statement already has it. `JOIN` is the `INNER MERGE JOIN` hint, which
# SQ80_JOIN_HINT reports and whose `MERGE` is not a statement at all; it is
# excluded here as well as by `_heads_a_statement`, because a word list is
# cheaper to read than a proof that the other test covers it.
_NOT_A_MERGE_TARGET = frozenset({"into", "join"})
# `MERGE [TOP (n) [PERCENT]] [INTO] <target> [[AS] <alias>] USING`: the MERGE
# statement's own head, matched whole. T-SQL makes INTO optional and USING
# mandatory, and nothing may stand between the target and USING but an alias,
# so this shape is what tells the statement from the other two readings of
# the word -- the `INNER MERGE JOIN` hint, and a column called `merge`.
#
# Structure rather than the keyword in front of it, because the keyword is
# not enough on its own: `\bMERGE\s+(?!JOIN\b)` fired on five shapes that
# are not MERGE statements, MEASURED on 2a223bd (see SQ80_MERGE). And
# structure rather than `_heads_a_statement`, which SQ52 uses: that test
# costs a miss on `WITH c AS (...) MERGE ...`, which T-SQL allows, and losing
# SQ80_MERGE there would grade a real MERGE clean. This shape keeps it.
_MERGE_HEAD = (
    rf"\bMERGE\s+(?:TOP\s*\([^)]*\)\s*(?:PERCENT\s*)?)?"
    rf"(?:INTO\s+)?{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}}"
    rf"(?:\s+(?:AS\s+)?(?!USING\b){_IDENT_PART})?"
    rf"\s+(?P<kw>USING)\b")
_MERGE_STATEMENT_RE = re.compile(_MERGE_HEAD, re.IGNORECASE)
# The same head plus the name in the USING slot: the one object position in
# T-SQL that no keyword in `_OBJECT_KEYWORDS` can anchor on. `USING` cannot
# be added to that list -- `SELECT * FROM a JOIN b USING (c)` and
# `CREATE TABLE t (...) USING parquet` both have one and neither names a
# table, MEASURED on 2a223bd as leaving both alone -- so the MERGE in front
# of it is what makes the position unambiguous, and `_MERGE_HEAD` is that
# MERGE. `kw` is captured for `_creates_the_object`, which answers False for
# it: a MERGE's source is read, never created.
#
# Up to three name parts, so a four-part source still falls through to
# SQ11_LINKED_SERVER. A derived table (`USING (SELECT ...)`) cannot match --
# `(` is not an identifier -- and the rules inside it already reach its own
# names; a one-part source is not matched by `_ONE_PART_KEYWORDS` either,
# and `WITH c AS (...) MERGE INTO t USING c` is why: a bare source is as
# often a CTE name as a table, which is the same reason UPDATE is absent
# from that list.
_MERGE_USING_RE = re.compile(
    rf"{_MERGE_HEAD}\s+"
    rf"(?P<name>{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}})(?![\w.])",
    re.IGNORECASE)


def _object_positions(view: str, pattern):
    """Every `(kw, name)` object position in `view`, in text order.

    `pattern` is the keyword-anchored one -- `_TWO_PART_RE` -- plus MERGE's
    USING source, which has no keyword to anchor on. Deduplicated on the
    name span and keyword-anchored-first, because two matches over one span
    would reach `apply_replacements` as an overlap, which it rightly refuses
    by raising: #33 is what an AssertionError out of a rule costs.
    """
    seen = set()
    matches = sorted(list(pattern.finditer(view))
                     + list(_MERGE_USING_RE.finditer(view)),
                     key=lambda found: (found.start("name"),
                                        found.group("kw").upper() == "USING"))
    for found in matches:
        if found.span("name") in seen:
            continue
        seen.add(found.span("name"))
        yield found


# T-SQL table hints. Spark has no locking or access-path hints, and rejects
# the syntax: MEASURED on the AIDP cluster (Spark 3.5.0, 2026-09-30),
# `MERGE INTO <t> WITH (HOLDLOCK) AS tgt USING ...` -> PARSE_SYNTAX_ERROR
# "at or near 'AS': missing 'USING'", while the same MERGE without the hint
# ran. The hint also hid the statement from `_MERGE_HEAD`, so a hinted MERGE
# lost SQ80_MERGE and kept its USING source unqualified.
#
# Most hints are concurrency or plan advice with no effect on the rows a
# statement sees, so dropping them is a rewrite. Three change WHAT IS READ --
# NOLOCK / READUNCOMMITTED read uncommitted rows, READPAST skips locked ones
# -- and Delta reads a consistent snapshot instead; those are dropped too
# (Spark cannot parse them) but flagged, keeping SQ80_HINT's id for them.
_TABLE_HINT_WORDS = (
    r"NOLOCK|READUNCOMMITTED|READPAST|UPDLOCK|HOLDLOCK|SERIALIZABLE|"
    r"READCOMMITTED|READCOMMITTEDLOCK|REPEATABLEREAD|ROWLOCK|PAGLOCK|TABLOCK|"
    r"TABLOCKX|XLOCK|NOWAIT|NOEXPAND|FORCESEEK|FORCESCAN|KEEPIDENTITY|"
    r"KEEPDEFAULTS|IGNORE_CONSTRAINTS|IGNORE_TRIGGERS|SNAPSHOT|"
    r"INDEX\s*(?:\([^)]*\)|=\s*[\w\[\]]+)")
_TABLE_HINT_LIST = rf"(?:{_TABLE_HINT_WORDS})(?:\s*,\s*(?:{_TABLE_HINT_WORDS}))*"
# `WITH (` straight into a hint word: a CTE is `WITH name AS (`, so the word
# after the parenthesis is what tells the two apart. The legacy form has no
# WITH -- `FROM dbo.t (NOLOCK)` -- and needs whitespace after a name so that
# a call `f(NOLOCK)` is never read as one.
_TABLE_HINT_RE = re.compile(
    rf"\s*\bWITH\s*\(\s*(?P<hints>{_TABLE_HINT_LIST})\s*\)"
    rf"|(?<=[\w\]`\"])\s+\(\s*(?P<legacy>{_TABLE_HINT_LIST})\s*\)",
    re.IGNORECASE)
_READ_CHANGING_HINTS = frozenset({"nolock", "readuncommitted", "readpast"})


def rule_table_hints(sql: str, findings: list) -> str:
    """SQ55: drop T-SQL table hints, which Spark cannot parse.

    `NOLOCK`, `READUNCOMMITTED` and `READPAST` are dropped and flagged
    (SQ80_HINT): they changed which rows T-SQL read. The rest are dropped as
    a rewrite. See `_TABLE_HINT_RE` for the measurement.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for match in _TABLE_HINT_RE.finditer(view):
        text = sql[match.start():match.end()]
        hints = [h.strip() for h in
                 re.split(r"\s*,\s*", (match.group("hints") or match.group("legacy")).strip())]
        words = {re.match(r"\w+", h).group(0).casefold() for h in hints}
        replacements.append((match.start(), match.end(), ""))
        shown = " ".join(text.split())
        if words & _READ_CHANGING_HINTS:
            findings.append(Finding(
                "SQ80_HINT",
                f"table hint {shown!r} removed: Spark has no table hints and "
                f"rejects the syntax. {', '.join(sorted(words & _READ_CHANGING_HINTS)).upper()} "
                f"changed which rows T-SQL read (uncommitted or skipped rows); "
                f"Delta reads a consistent snapshot, so confirm the object "
                f"does not depend on that",
                "flag"))
        else:
            findings.append(Finding(
                "SQ55_TABLE_HINT",
                f"table hint {shown!r} removed: Spark has no table hints and "
                f"rejects the syntax; this one is locking or plan advice and "
                f"does not change the rows read",
                "rewrite"))
    return apply_replacements(sql, replacements)


def rule_merge_without_into(sql: str, findings: list) -> str:
    """SQ52: `MERGE t USING ...` -> `MERGE INTO t USING ...`.

    The same defect as F2's `INSERT` without `INTO`, in the other statement
    that has it, and it is fixed the same way. T-SQL takes MERGE's target
    with no keyword in front of it, so `_OBJECT_KEYWORDS` has nothing to
    anchor on and the target is the one name in the statement the name rules
    cannot see.

    MEASURED on 03f019b, `translate(sql, kind="table", item="W")`:

        MERGE dbo.t AS tgt USING dbo.s AS src ON tgt.id=src.id
          WHEN MATCHED THEN UPDATE SET tgt.a=src.a;

        -> the target `dbo.t` stays two-part     findings: ['SQ80_MERGE']

    while the same statement written `MERGE INTO dbo.t` came back
    `MERGE INTO default.W.t` under SQ11_TWO_PART_NAME. One statement, two
    spellings, and only one of them got the name the plan promises.

    SQ80_MERGE flags the statement either way, so this always reached a
    human -- which is why it is a minor and not worse. What it did not do is
    reach the human with the right name: a reader resolving the flag by hand
    keeps whatever the artifact says, and the artifact said `dbo.t`.

    And the statement does not run in the first place. MEASURED on pyspark
    4.2.0, JAVA_HOME=openjdk@21, local[1], session defaults:

        MERGE t USING u ON t.a=u.a WHEN MATCHED THEN UPDATE SET t.b=u.b
          PARSE_SYNTAX_ERROR "Syntax error at or near 't'" (line 1, pos 6)
        MERGE INTO t USING u ON t.a=u.a WHEN MATCHED THEN UPDATE SET t.b=u.b
          TABLE_OR_VIEW_NOT_FOUND

    -- the second failure is the probe having no table `t`, which means the
    statement PARSED. Spark requires INTO; T-SQL does not. So writing the
    keyword in is both the fix for the syntax and the thing that lets SQL11
    qualify the name, exactly as in SQ52's INSERT case, and it runs before
    the name rules for that reason.

    Nothing else about the statement is touched. A MERGE that already has
    INTO is left exactly alone, and SQ80_MERGE still says what it said: the
    clause sets differ, and the rewrite is a human's to finish.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for match in _MERGE_TARGET_RE.finditer(view):
        if not _heads_a_statement(view, match.start()):
            # `SELECT * FROM t INNER MERGE JOIN u ...` -- a join hint, whose
            # `MERGE` never begins a statement. A MERGE statement always
            # does. `SELECT merge FROM t`, a column of that name, is caught
            # here too: the `FROM` behind it would otherwise have been read
            # as the target and `INTO` written in front of it.
            continue
        target = match.group("target")
        if _bare(target).lower() in _NOT_A_MERGE_TARGET:
            continue
        at = match.start("target")
        replacements.append((at, at, "INTO "))
        findings.append(Finding(
            "SQ52_MERGE_NO_INTO",
            f"MERGE {target} -> MERGE INTO {target}; T-SQL makes INTO "
            f"optional on MERGE and Spark does not (PARSE_SYNTAX_ERROR at "
            f"the target name), and without the keyword the table rules "
            f"cannot see this as an object position, so the target was the "
            f"one name in the statement left unqualified. SQ80_MERGE still "
            f"applies to the statement as a whole",
            "rewrite"))
    return apply_replacements(sql, replacements)


# `ALTER TABLE <name> ADD ` and nothing about what comes after it; the word
# after the keyword decides which `ADD` this is.
_ALTER_ADD_RE = re.compile(
    rf"\bALTER\s+TABLE\s+{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}}\s+"
    rf"ADD\s+",
    re.IGNORECASE)
# The `ADD` forms that do not declare a column. `CONSTRAINT`/`PRIMARY`/... go
# to SQ75, which removes the whole statement; `COLUMN`/`COLUMNS` mean the
# document is already Spark-shaped and there is nothing to write in.
_NOT_A_COLUMN_ADD = frozenset({
    "constraint", "primary", "foreign", "unique", "check", "default",
    "column", "columns", "period", "index", "with"})
_LEADING_WORD_RE = re.compile(r"[A-Za-z_][\w$]*")
_DEPTH_ZERO_AS_RE = re.compile(r"\bAS\b", re.IGNORECASE)


def rule_alter_add_column(sql: str, findings: list) -> str:
    """SQ76: `ALTER TABLE t ADD c <type>` -> `ALTER TABLE t ADD COLUMNS (...)`.

    Two defects in one line, both measured on the tree before this rule:

      ALTER TABLE dbo.t ADD c timestamp
        -> ALTER TABLE default.W.t ADD c timestamp     SQ11 only, flags=0
      ALTER TABLE dbo.t ADD c money
        -> ALTER TABLE default.W.t ADD c money         SQ11 only, flags=0

    The syntax is the first. Measured on Spark 4.2.0 (pyspark 4.2.0,
    JAVA_HOME=openjdk@21, session defaults, table created `USING parquet`):

      ALTER TABLE t ADD c timestamp            PARSE_SYNTAX_ERROR at 'c'
      ALTER TABLE t ADD COLUMNS (c TIMESTAMP)  ACCEPTED

    The second is that the columns were declared somewhere no column rule
    looked: `_column_list_bodies` knew only about CREATE TABLE, so `money`
    (which has no Spark type at all -- `ADD COLUMNS (c7 money)` is
    UNSUPPORTED_DATATYPE) and `timestamp` (T-SQL's rowversion, a binary row
    version, not a point in time) both passed with flags=0. That is the exact
    pair SQ60 exists to catch in a CREATE TABLE body. Writing the parentheses
    in is what puts these columns where SQ60, SQ70 and SQ72 can see them, so
    this rule runs before all three.

    A computed column (`ADD c AS <expr>`) is refused rather than wrapped:
    Spark has no computed columns, `ADD COLUMNS (c AS x + 1)` does not parse,
    and emitting it with a `rewrite` finding would report a success.

    One measurement worth writing down because it did NOT change the rule:
    `ALTER TABLE t ADD COLUMNS (c INT NOT NULL)` is rejected on a v1 table
    with _LEGACY_ERROR_TEMP_1052, "ADD COLUMN with v1 tables cannot specify
    NOT NULL". NOT NULL is kept anyway, because a CREATE TABLE body keeps it
    too and the AIDP target is a v2 table; a bare `NULL` is stripped by SQ72,
    which now reaches here -- measured, `ADD COLUMNS (c5 INT NULL)` is a
    PARSE_SYNTAX_ERROR at 'NULL'.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for match in _ALTER_ADD_RE.finditer(view):
        if _depth_at(view, match.start()) != 0:
            continue
        body_start = match.end()
        _statement_start, end = _statement_bounds(view, match.start())
        if body_start >= end:
            continue
        word = _LEADING_WORD_RE.match(view, body_start)
        if word is not None and word.group(0).casefold() in _NOT_A_COLUMN_ADD:
            continue
        if any(_depth_at(view, found.start()) == 0
               for found in _DEPTH_ZERO_AS_RE.finditer(view, body_start, end)):
            findings.append(Finding(
                "SQ76_ALTER_ADD_COMPUTED",
                f"{' '.join(sql[match.start():end].split())!r}: Spark has no "
                f"computed columns, so there is no ADD COLUMNS form of this. "
                f"Left as written -- it needs the expression moved into the "
                f"query or the job that writes the table",
                "flag"))
            continue
        columns = " ".join(sql[body_start:end].split())
        replacements.append((body_start, body_start, "COLUMNS ("))
        replacements.append((end, end, ")"))
        findings.append(Finding(
            "SQ76_ALTER_ADD_COLUMN",
            f"ALTER TABLE ... ADD {columns} -> ADD COLUMNS ({columns}); "
            f"Spark's column-add takes the COLUMNS keyword and a "
            f"parenthesised list (PARSE_SYNTAX_ERROR at the column name "
            f"without them), and the type rules only read a parenthesised "
            f"column list, so these columns were never type-checked either",
            "rewrite"))
    return apply_replacements(sql, replacements)


# name -> (spark type, keep the (args))
_TYPE_MAP = {
    "nvarchar": ("STRING", False), "varchar": ("STRING", False),
    "nchar": ("STRING", False), "char": ("STRING", False),
    "ntext": ("STRING", False), "text": ("STRING", False),
    "bit": ("BOOLEAN", False),
    "datetime": ("TIMESTAMP", False), "datetime2": ("TIMESTAMP", False),
    "smalldatetime": ("TIMESTAMP", False),
    "float": ("DOUBLE", False), "real": ("FLOAT", False),
    # `decimal` is here only so `_ALL_TYPE_NAMES` -- and with it the COLUMN
    # pattern -- can see a bare one. It is already a Spark type name, so the
    # only thing that ever changes is the absent precision; see SQ62.
    "numeric": ("DECIMAL", True), "decimal": ("DECIMAL", True),
    "varbinary": ("BINARY", False), "binary": ("BINARY", False),
    "image": ("BINARY", False),
}
_TYPE_FLAGS = {
    "time": "Spark has no TIME type; store the value as STRING, or fold it into "
            "a TIMESTAMP with the date it belongs to",
    "money": "T-SQL MONEY has fixed 4-digit scale with its own rounding; "
             "DECIMAL(19,4) matches the scale but not the rounding, so the "
             "column is left as-is for a deliberate decision",
    "smallmoney": "see MONEY: scale maps but rounding does not",
    "uniqueidentifier": "Spark has no native UUID type; choose STRING and decide "
                        "how values are generated",
    "datetimeoffset": "Spark TIMESTAMP does not carry a timezone offset; the "
                      "offset would be silently lost",
    "xml": "Spark has no XML type; store as STRING and parse explicitly",
    "sql_variant": "Spark has no variant type",
    "hierarchyid": "no Spark equivalent",
    "geography": "no Spark equivalent; consider a geospatial library",
    "geometry": "no Spark equivalent; consider a geospatial library",
}
# T-SQL type names with no Spark equivalent AND no entry in the two tables
# above, so nothing matched them: a column declared with one passed through
# verbatim at flags=0 and the DDL cannot run. Measured:
#
#   CREATE TABLE dbo.t (c rowversion) -> unchanged, flags=0, PASS
#   and on Spark 4.2.0: create table t (c rowversion) using parquet -> fails
#
# They are refused rather than mapped, in both positions, so the cast path and
# the column path keep agreeing: `sysname` is really `nvarchar(128)` and could
# be mapped, but mapping it in one place and refusing it in the other is the
# inconsistency E-c just removed.
_UNMAPPABLE_TYPE_NAMES = {
    "rowversion": "an 8-byte row version stamp maintained by SQL Server. "
                  "Spark has no equivalent, nothing would maintain it, and "
                  "the DDL as written does not run",
    "sysname": "a system alias for NVARCHAR(128). Spark does not know the "
               "name, so the DDL as written does not run; write the "
               "underlying type out by hand if a string column is wanted",
}
_ALL_TYPE_NAMES = "|".join(sorted(set(_TYPE_MAP) | set(_TYPE_FLAGS), key=len,
                                  reverse=True))
# Unmappable in a COLUMN DEFINITION only, and the asymmetry is the point.
# T-SQL's `timestamp` column type is a documented synonym for `rowversion` --
# an 8-byte binary row version, not a datetime -- so passing it through gave
# the migrated table a datetime column where the source had a row version.
# Measured: `CREATE TABLE dbo.t (c timestamp)` came back unchanged at flags=0.
# That is worse than the unrunnable cases this rule mostly catches, because
# unrunnable DDL fails loudly on the cluster and this succeeds with a
# different schema.
#
# In a CAST it is deliberately left alone: the word is also a legitimate Spark
# type, and in a migrated file `CAST(c AS timestamp)` far more likely means
# what Spark means. T-SQL admits no second reading in a column definition, so
# splitting by position settles it without having to decide whether the source
# dialect is trusted for the whole file. Please do not "fix" the inconsistency
# by moving this into `_UNMAPPABLE_TYPE_NAMES`, which both positions consult.
_COLUMN_ONLY_UNMAPPABLE_TYPES = {
    "timestamp": "a T-SQL `timestamp` COLUMN is a synonym for `rowversion`, "
                 "an 8-byte row version stamp, NOT a datetime; passing it "
                 "through would silently give the table a datetime column. "
                 "Write `rowversion`'s replacement out by hand, or `TIMESTAMP` "
                 "explicitly if a datetime really was meant",
}
_COLUMN_UNMAPPABLE_TYPES = {**_UNMAPPABLE_TYPE_NAMES,
                            **_COLUMN_ONLY_UNMAPPABLE_TYPES}
_UNMAPPABLE_NAMES = "|".join(sorted(_COLUMN_UNMAPPABLE_TYPES, key=len,
                                    reverse=True))
# `CREATE TABLE <name> (` with nothing between the name and the `(`. The
# pattern this replaced was `\bCREATE\s+TABLE\b[^(]*\(`, whose `[^(]*` ran
# straight past `AS SELECT ... WHERE` and stopped at the first `(` anywhere
# after CREATE TABLE. Measured on main, these two statements in one file:
#
#   CREATE TABLE dbo.t AS SELECT a FROM dbo.u WHERE (a > 0);
#   CREATE TABLE dbo.v (id INT IDENTITY(1,1) PRIMARY KEY);
#
# `(a > 0)` was read as dbo.t's column list, nothing in it matched a
# constraint pattern, so rule_constraints returned early and dbo.v was never
# examined: `IDENTITY(1,1) PRIMARY KEY` reached the artifact with no finding
# against it. Spark 4.2.0 rejects that statement -- measured, with
# `USING parquet`: PARSE_SYNTAX_ERROR at 'IDENTITY'.
#
# Declining to match a CTAS is the point rather than a side effect: a
# `CREATE TABLE ... AS SELECT` has no column list, so there is nothing in
# its parentheses for a column rule to read.
_CREATE_TABLE_NAME_PART = r"(?:`[^`]*`|\[[^\]]*\]|\"[^\"]*\"|[A-Za-z_][\w$]*)"
_CREATE_TABLE_BODY_RE = re.compile(
    rf"\bCREATE\s+TABLE\b\s+(?:IF\s+NOT\s+EXISTS\s+)?{_CREATE_TABLE_NAME_PART}"
    rf"(?:\s*\.\s*{_CREATE_TABLE_NAME_PART}){{0,2}}\s*\(",
    re.IGNORECASE)


# The other place a document declares columns: the `ADD COLUMNS (...)` that
# SQ76 writes for T-SQL's `ALTER TABLE t ADD c <type>`. Same shape, same
# contents, and the column rules had never been pointed at it -- see
# `rule_alter_add_column`.
_ALTER_ADD_COLUMNS_BODY_RE = re.compile(
    rf"\bALTER\s+TABLE\b\s+{_CREATE_TABLE_NAME_PART}"
    rf"(?:\s*\.\s*{_CREATE_TABLE_NAME_PART}){{0,2}}\s+ADD\s+COLUMNS?\s*\(",
    re.IGNORECASE)


def _column_list_bodies(view: str):
    """(open, close) of EVERY column list in `view`.

    `open` indexes the `(`; `close` is one past its `)`.

    Two of the three callers used `search`, which finds one body per
    document. Measured on main with two tables in one file, each
    `id INT IDENTITY(1,1) PRIMARY KEY, n VARCHAR(10)`: the first came back
    `(id INT, n STRING)` and the second kept `IDENTITY(1,1) PRIMARY KEY`
    with no finding of its own, so the four findings that did fire read as
    if the whole file had been cleaned. One object per `.sql` is the DacFx
    norm, but a hand-written script holds several, and SQ02 rewrites every
    `GO` in a batched export as a `;` inside one document.

    It was `_create_table_bodies` and answered only for CREATE TABLE, which
    is why the type, constraint and nullability rules all missed the columns
    an `ALTER TABLE ... ADD` declares. Measured before SQ76:

      ALTER TABLE dbo.t ADD c money -> ALTER TABLE default.W.t ADD c money
        findings: SQ11 only, flags=0

    `money` has no Spark type and SQ60 exists to say so; it never looked
    here. One list of column-list positions rather than one per rule is what
    keeps the answer the same for all three.
    """
    # The closer is found on a copy with identifier bodies blanked, and the
    # offsets come back meaning the same thing because both views are
    # length-preserving. `_closing_index` counts brackets, and a column named
    # `[a]]b]` holds one it must not count: measured,
    # `CREATE TABLE dbo.t ([a]]b] [int] NOT NULL)` gave the body as `[a]]`,
    # four characters in, so every column rule stopped at the first column's
    # name. Two of the three callers hand in a view that still has its
    # identifier bodies, so doing it here covers all of them.
    scan = _without_identifier_bodies(view)
    bodies = []
    for pattern in (_CREATE_TABLE_BODY_RE, _ALTER_ADD_COLUMNS_BODY_RE):
        for header in pattern.finditer(scan):
            open_index = header.end() - 1
            close = _closing_index(scan, open_index)
            if close is not None:
                bodies.append((open_index, close))
    return sorted(bodies)


# The trailing guard is a negative lookahead, not \b: after a closing ")" the
# next character is usually "," or a newline, and \b needs a word/non-word
# transition, so it would fail there and backtrack into not matching the args
# at all -- silently leaving NVARCHAR(100) as STRING(100).
def _column_type_pattern(names: str):
    """A column definition whose type is one of `names`.

    Position is what makes this safe to use with a bare name list: the type
    has to follow an identifier, so `rowversion INT` -- a column *named*
    rowversion -- is not a match, and neither is the word anywhere outside a
    CREATE TABLE body. The trailing guard is the negative lookahead described
    above, not `\b`.
    """
    return re.compile(
        rf"(?<=[\s,(])(?P<name>{_IDENT_PART})\s+(?P<type>{names})"
        rf"(?P<args>\s*\([^)]*\))?(?![\w(])",
        re.IGNORECASE)


_COLUMN_TYPE_RE = _column_type_pattern(_ALL_TYPE_NAMES)
# DacFx brackets the type as well as the column name, and SQ10 has turned an
# unrecognised bracketed token into a backquoted one by the time this runs --
# `[c] [rowversion]` arrives here as `` `c` `rowversion` `` -- so the type slot
# has to accept that wrapper. It cannot simply be added to _KNOWN_TYPE_NAMES
# for SQ10 to unwrap: that unwrapping is position-blind, and a column *named*
# `[rowversion]` then lost its quoting. Measured while writing this.
_UNMAPPABLE_COLUMN_TYPE_RE = _column_type_pattern(
    rf"`?(?:{_UNMAPPABLE_NAMES})`?")
# TRY_CAST too: Spark has `try_cast` under the same name, so only the type
# needs mapping. `\b` would not match it -- the `_` is a word character, so
# there is no boundary before CAST -- and the pattern this replaced was
# anchored on `AS <type>)`, which caught TRY_CAST by accident.
_CAST_CALL_RE = re.compile(r"(?<![\w$.])(?:TRY_)?CAST\s*\(", re.IGNORECASE)
_AS_WORD_RE = re.compile(r"\bAS\b", re.IGNORECASE)


def _column_type_matches(view: str, pattern=None):
    """Column-definition matches inside every CREATE TABLE body.

    The match rather than a tuple, because the callers want different groups
    out of it: the type and its arguments to rewrite, and the column name to
    name in a finding.
    """
    found = []
    for open_index, body_end in _column_list_bodies(view):
        found.extend((pattern or _COLUMN_TYPE_RE).finditer(
            view, open_index + 1, body_end))
    return found


# The column types whose `(n)` is a length that disappears into STRING.
# `text`/`ntext` take no length and are not here; `(max)` is excluded at the
# call site, because there the mapping really is faithful -- neither side has
# a maximum. `char`/`nchar` blank-pad as well as cap, so they carry a second
# clause.
_LENGTH_BEARING_STRING_TYPES = frozenset({"nvarchar", "varchar", "nchar", "char"})
_BLANK_PADDED_STRING_TYPES = frozenset({"nchar", "char"})
_MAX_LENGTH_RE = re.compile(r"\(\s*max\s*\)", re.IGNORECASE)


def _length_loss_detail(name: str, args: str) -> str:
    """Why `NVARCHAR(50) -> STRING` is a loss, and why it is STRING anyway.

    The finding used to be that bare arrow. It named the two types and said
    nothing about the difference between them, and the difference is silent:
    an INSERT T-SQL refused now succeeds.

    The reason for keeping STRING lives here, in the finding, rather than in
    a commit message, because it is the answer to the obvious objection --
    Spark 4 DOES have VARCHAR(n). Measured on pyspark 4.2.0
    (JAVA_HOME=openjdk@21, local[1], `USING parquet`):

      CREATE TABLE v1 (n VARCHAR(5)) USING parquet   ACCEPTED
      DESCRIBE TABLE v1                              n  varchar(5)
      INSERT INTO v1 VALUES ('abcdefghij')           REJECTED EXCEED_LIMIT_LENGTH
      the same INSERT with spark.sql.legacy.charVarcharAsString=true
                                                     ACCEPTED, stored whole
      CREATE TABLE v2 (n VARCHAR(max)) USING parquet PARSE_SYNTAX_ERROR at 'max'
      SELECT CAST('abcdefghij' AS VARCHAR(5))        'abcdefghij', and DESCRIBE
                                                     reports `string`
      CREATE TABLE (n CHAR(5)); INSERT 'ab'          reads back '[ab   ]', 5

    So VARCHAR(n) holds in one position -- a table column, under one default
    setting -- and evaporates in the others: `nvarchar(max)` has no such
    form at all, one config switch stops the enforcement, and in a cast or a
    view the type erases to STRING and stops truncating, which is exactly
    why SQ61 emits `substring(cast(...), 1, n)` there instead.

    Emitting VARCHAR(n) for a column while SQ61 emits STRING for a cast
    would make the two positions disagree about what a T-SQL string column
    becomes, and it would give `nvarchar(50)` and `nvarchar(max)` two
    different Spark types for one T-SQL concept. The length goes; the
    finding now says what that costs and where to put the cap instead.
    """
    padded = (" The blank padding T-SQL applies to a SHORTER value goes with "
              "it, so a value read back from this column is no longer the "
              "same length as the one read back from the source."
              if name.casefold() in _BLANK_PADDED_STRING_TYPES else "")
    return (
        f"the declared length is dropped. T-SQL {name.upper()}{args} refuses "
        f"a longer value and Spark STRING accepts any, so an INSERT that "
        f"used to fail now succeeds and the column holds more than the "
        f"schema promised.{padded} Spark 4.2.0 does have VARCHAR(n) and does "
        f"enforce it on a table -- measured, ten characters into a "
        f"VARCHAR(5) column is EXCEED_LIMIT_LENGTH -- but `varchar(max)` has "
        f"no such form, spark.sql.legacy.charVarcharAsString switches the "
        f"enforcement off, and in a cast or a view the type erases to STRING "
        f"and stops truncating, which is why SQ61 caps a cast with "
        f"substring() rather than with a type. STRING is emitted here for "
        f"the same reason, and the cap belongs in whatever writes the table"
    )


def _cast_type_spans(view: str):
    """(start, end) of the target type in every `CAST(expr AS type)`.

    Anchored on the call, not on the word AS. The pattern this replaces was
    `AS <known type>)`, which could only ever see the names in SQ60's own
    table -- so `CAST(c AS rowversion)` matched nothing, raised nothing and
    graded PASS. Widening that pattern instead would have read a column alias
    in a subquery as a type: `FROM (SELECT c AS name) t` ends in `AS name)`.

    The last top-level AS in the argument is this cast's; an earlier one
    belongs to a nested CAST, which this scan reaches on its own match.
    """
    spans = []
    for match in _CAST_CALL_RE.finditer(view):
        split = split_call_arguments(view, match.end() - 1)
        if split is None:
            continue
        arg_spans, _close = split
        if len(arg_spans) != 1:
            continue
        start, end = arg_spans[0]
        depth, last, index = 0, None, start
        while index < end:
            char = view[index]
            if char in "([":
                depth += 1
            elif char in ")]":
                depth -= 1
            elif depth == 0:
                word = _AS_WORD_RE.match(view, index)
                if word is not None and word.end() <= end:
                    last = word
                    index = word.end()
                    continue
            index += 1
        if last is None:
            continue
        type_start, type_end = last.end(), end
        while type_start < type_end and view[type_start] in _WS:
            type_start += 1
        while type_end > type_start and view[type_end - 1] in _WS:
            type_end -= 1
        if type_start < type_end:
            spans.append((type_start, type_end))
    return spans


def rule_data_types(sql: str, findings: list) -> str:
    """SQ60: map T-SQL types to Spark types in column definitions and casts.

    The two positions are resolved differently, on purpose. A *column* keeps a
    type this tool will not map -- `amount MONEY` stays, flagged, because what
    to do about MONEY's rounding is a decision about stored data and is argued
    separately. A *cast* cannot keep one: `CAST(c AS money)` does not run, so
    it resolves through `_spark_convert_type`, the same table CONVERT uses.
    That is what makes the two paths agree, and a type outside every table is
    refused rather than passed through -- measured before this rule saw it at
    all, `SELECT CAST(c AS rowversion)` graded PASS with no finding.

    One name is read differently in the two positions, on purpose: T-SQL's
    `timestamp` is `rowversion` in a column definition and is also Spark's own
    datetime type in a cast. The position decides, which is why there is no
    whole-file dialect question to answer here.
    """
    view = _masked(sql)
    replacements = []
    # The enumerated unmappable names, searched for by *position*: the type
    # slot of a column definition. The list is wider here than in a cast --
    # `timestamp` means `rowversion` in a column and means TIMESTAMP in a
    # cast, see _COLUMN_ONLY_UNMAPPABLE_TYPES. Position is what makes a bare name list
    # safe -- it is not the blanking of identifier bodies, which I expected to
    # be the guard and measured not to be. A column called `rowversion` sits
    # in the name slot, and the word anywhere outside a CREATE TABLE body is
    # not matched at all. Both asserted.
    for match in _column_type_matches(view, _UNMAPPABLE_COLUMN_TYPE_RE):
        column = sql[match.start("name"):match.end("name")]
        written = sql[match.start("type"):match.end("type")]
        findings.append(Finding(
            "SQ60_TYPE_UNKNOWN",
            f"column {column} is declared {written}, left in place: "
            # The consequence belongs in the per-type explanation, not here:
            # Spark rejects most of these outright but accepts `timestamp`
            # with the wrong meaning, and a shared tail claiming a rejection
            # would mislead a reviewer about the one that matters most.
            f"{_COLUMN_UNMAPPABLE_TYPES[_bare(written).casefold()]}. This "
            f"needs a decision rather than a mapping",
            "flag"))
    for match in _column_type_matches(view):
        start, end = match.start("type"), match.end()
        name, args = match.group("type"), match.group("args")
        key = name.casefold()
        if key in _TYPE_FLAGS:
            findings.append(Finding(
                f"SQ60_{key.upper()}", f"{name.upper()}: {_TYPE_FLAGS[key]}", "flag"))
            continue
        spark_type, keep_args = _TYPE_MAP[key]
        replacement = spark_type + ((args or "").strip() if keep_args else "")
        written = sql[start:end]
        rule_id = "SQ60_TYPE"
        detail = f"{name.upper()}{(args or '').strip()} -> {replacement}"
        written_args = (args or "").strip()
        if (key in _LENGTH_BEARING_STRING_TYPES and written_args
                and not _MAX_LENGTH_RE.fullmatch(written_args)):
            # `(max)` is left with the bare arrow: there the mapping really
            # is faithful, because neither T-SQL's `max` nor Spark's STRING
            # has a maximum to lose.
            detail += "; " + _length_loss_detail(name, written_args)
        if _bare_decimal(written):
            replacement = spark_type + _TSQL_DECIMAL_DEFAULT
            rule_id = "SQ62_DECIMAL_DEFAULT"
            detail = f"{written} -> {replacement}; {_DECIMAL_DETAIL}"
        elif replacement.casefold() == written.casefold():
            # Already what Spark spells it, down to the arguments. Emitting a
            # replacement and a `rewrite` finding for a no-op would add a row
            # to every report that names a `decimal(18,2)` column and say
            # nothing. (Only decimal reaches this: every other mapping here
            # changes the name.)
            continue
        replacements.append((start, end, replacement))
        findings.append(Finding(rule_id, detail, "rewrite"))
    for start, end in _cast_type_spans(view):
        written = sql[start:end]
        key = _bare(written.split("(")[0].strip()).casefold()
        if key in _TYPE_FLAGS and key not in _CONVERT_ONLY_TYPES:
            # No Spark type at all. The specific advice is worth more than a
            # generic refusal, so this keeps SQ60's own message.
            findings.append(Finding(
                f"SQ60_{key.upper()}",
                f"{key.upper()}: {_TYPE_FLAGS[key]}", "flag"))
            continue
        resolved = _spark_convert_type(written)
        if resolved is None:
            findings.append(Finding(
                "SQ60_TYPE_UNKNOWN",
                f"CAST(... AS {written}) left in place: {written!r} is not a "
                f"type this tool can map to Spark. It may be a user-defined "
                f"or alias type, in which case the underlying type has to be "
                f"written out by hand -- passing the name through would emit "
                f"a cast Spark rejects",
                "flag"))
            continue
        spark_type, caveat = resolved
        if spark_type == written:
            continue  # already a Spark type, written as Spark spells it
        replacements.append((start, end, spark_type))
        if _bare_decimal(written):
            findings.append(Finding(
                "SQ62_DECIMAL_DEFAULT",
                f"{written} -> {spark_type}; {_DECIMAL_DETAIL}", "rewrite"))
            continue
        findings.append(Finding(
            "SQ60_TYPE_LOSS" if caveat else "SQ60_TYPE",
            f"{written} -> {spark_type}" + (f"; {caveat}" if caveat else ""),
            "flag" if caveat else "rewrite"))
    return apply_replacements(sql, replacements)


# The type names CONVERT accepts, resolved against the SAME tables SQ60 uses
# for a bare CAST, so the two paths cannot give different answers for one
# type: CAST(c AS varchar(20)) and CONVERT(varchar(20), c) both yield STRING.
#
# These three are the one place the two paths deliberately differ. SQ60 only
# *flags* them and leaves the written type alone, which is right for a column
# definition -- a MONEY column keeps its name until someone decides about the
# rounding. CONVERT has nowhere to leave them: `CAST(c AS money)` is rejected
# by Spark, so passing the name through emits SQL that cannot run. They are
# mapped here and the loss is flagged, the same call SQ71 and SQ73 make when
# they remove a clause they cannot keep.
_CONVERT_ONLY_TYPES = {
    "money": ("DECIMAL(19,4)",
              "MONEY's fixed 4-digit scale maps but its rounding does not, "
              "which is why SQ60 leaves a MONEY column as written"),
    "smallmoney": ("DECIMAL(19,4)",
                   "see MONEY: the scale maps, the rounding does not"),
    "uniqueidentifier": ("STRING",
                         "Spark has no native UUID type; STRING carries the "
                         "value but nothing generates or validates it"),
}
_CONVERT_TYPE_RE = re.compile(r"(?s)^(?P<name>[A-Za-z_]\w*)\s*(?P<args>\(.*\))?$")
# T-SQL's DECIMAL and NUMERIC default to (18, 0) when no precision is
# written. Spark's default is (10, 0), and the eight missing digits are not a
# rounding difference: measured on Spark 4.2.0,
#     cast(12345678901234 as decimal)       -> None
#     cast(12345678901234 as decimal(18,0)) -> 12345678901234
# so a 14-digit value silently became NULL. The precision is therefore always
# written out rather than left to whichever engine reads the SQL.
_DECIMAL_NAMES = frozenset({"decimal", "numeric"})
_TSQL_DECIMAL_DEFAULT = "(18,0)"


def _bare_decimal(written: str) -> bool:
    """Is `written` a DECIMAL/NUMERIC with no precision written out?"""
    match = _CONVERT_TYPE_RE.match(_bare(written.strip()))
    return (match is not None
            and match.group("name").casefold() in _DECIMAL_NAMES
            and not (match.group("args") or "").strip())


_DECIMAL_DETAIL = (
    "T-SQL's bare DECIMAL/NUMERIC is DECIMAL(18,0) and Spark's default is "
    "DECIMAL(10,0), so the precision was silently narrowed by eight digits. "
    "Measured on Spark 4.2.0, cast(12345678901234 as decimal) is NULL while "
    "cast(12345678901234 as decimal(18,0)) is the value -- data loss, not a "
    "precision nicety")


def _spark_convert_type(written: str):
    """(spark type, caveat) for a T-SQL type as written in CONVERT, or None.

    None means "not a type name this tool can map", and the caller refuses.
    Refusing is the point: emitting the name verbatim is what produced
    `CAST(c AS nvarchar(50))`, which Spark rejects -- measured on 4.2.0.

    The length is still dropped HERE, and that is no longer the whole story:
    every caller asks `_string_length_limit` first and wraps the cast in a
    `substring(..., 1, n)` when the type carries one, so `CONVERT(nvarchar(50),
    x)` and `CAST(x AS nvarchar(50))` both truncate the way T-SQL truncates.
    See SQ61 / `_truncating_cast`; this function answers only "which Spark
    type", which is why it can stay length-blind.

    Still NOT handled, and filed rather than guessed: `binary(n)` and
    `varbinary(n)`, which truncate too; a bare `varchar` with no length,
    whose T-SQL default differs between a cast and a column definition; and
    the length on a COLUMN, where T-SQL raises on an over-long INSERT rather
    than truncating, so STRING loses a constraint and not a value.
    """
    match = _CONVERT_TYPE_RE.match(_bare(written.strip()))
    if match is None:
        return None
    key = match.group("name").casefold()
    args = (match.group("args") or "").strip()
    if key in _DECIMAL_NAMES:
        # Ahead of _TYPE_MAP so `decimal(10,2)` keeps the name the user wrote
        # -- it is already Spark's -- while `numeric` is renamed and a missing
        # precision is written out. See _TSQL_DECIMAL_DEFAULT.
        name = "DECIMAL" if key == "numeric" else match.group("name")
        return (name + (args or _TSQL_DECIMAL_DEFAULT), None)
    if key in _TYPE_MAP:
        spark_type, keep_args = _TYPE_MAP[key]
        return (spark_type + (args if keep_args else ""), None)
    if key in _CONVERT_ONLY_TYPES:
        return _CONVERT_ONLY_TYPES[key]
    if key in _KNOWN_TYPE_NAMES and key not in _TYPE_FLAGS:
        # Already a Spark type: int, bigint, date, decimal(10,2). Kept exactly
        # as written -- `CONVERT(int, c)` -> `CAST(c AS int)` was already
        # correct and stays that way. Anything in _TYPE_FLAGS and not mapped
        # above (TIME, XML, DATETIMEOFFSET, ...) has no Spark type at all and
        # falls through to the refusal.
        return (match.group("name") + args, None)
    return None


# type name -> does T-SQL blank-pad the value out to n as well as truncate it?
# `text`/`ntext` are absent: they carry no length. `binary(n)`/`varbinary(n)`
# truncate too and are NOT handled here -- see the report.
_LENGTH_LIMITED_STRING_TYPES = {
    "varchar": False, "nvarchar": False, "char": True, "nchar": True,
}


def _string_length_limit(written: str):
    """(n, pads) for a `varchar(n)`-shaped type, else None.

    None for `varchar(max)` -- there is no practical truncation at 2GB -- and
    for a bare `varchar`, whose T-SQL default length differs between a cast
    and a column definition. Reproducing that default is filed, not done:
    this tool has no measurement of it, and a truncation applied on a
    remembered number is the same class of harm as the one being fixed.
    """
    match = _CONVERT_TYPE_RE.match(_bare(written.strip()))
    if match is None:
        return None
    pads = _LENGTH_LIMITED_STRING_TYPES.get(match.group("name").casefold())
    if pads is None:
        return None
    inner = (match.group("args") or "").strip()[1:-1].strip()
    if not inner.isdigit():
        return None
    return int(inner), pads


def _truncating_cast(emit: str, expr: str, written: str, limit):
    """(replacement, finding) for a cast to a length-limited string type.

    One helper for all four call sites -- CAST, TRY_CAST, CONVERT and
    TRY_CONVERT -- for the same reason `_spark_convert_type` is shared: the
    truncation is one decision, and answering it in four places is how the
    four would drift apart. The rule id is SQ61 in every position for the
    same reason.
    """
    length, pads = limit
    emitted = f"substring({emit}({expr} AS STRING), 1, {length})"
    measured = (
        f"Spark does not truncate in a cast -- measured on 4.2.0, "
        f"CAST('abcdefghijklmnopqrstuvwxyz' AS varchar(20)) is accepted and "
        f"returns all 26 characters -- while T-SQL cuts the value to the "
        f"declared length. substring(cast(c as string), 1, n) is the "
        f"measured reproduction")
    if pads:
        # T-SQL's CHAR(n) also blank-pads a shorter value out to n. That half
        # is NOT reproduced, and the choice is a real one rather than an
        # omission: padding would make LEN and concat right and comparison
        # wrong, because T-SQL compares strings ignoring trailing blanks and
        # Spark does not, so `CAST(c AS char(10)) = 'ab'` would flip from
        # true to false. Neither answer is uniformly correct, so the
        # truncation is applied and the padding is put to a human -- the same
        # call SQ60 makes for MONEY's rounding.
        return (emitted, Finding(
            "SQ61_CHAR_PADDING",
            f"{written} -> {emitted}; the truncation to {length} is "
            f"reproduced but CHAR's blank-padding out to {length} is not. "
            f"{measured}. Padding would fix LEN and concat and break "
            f"equality -- T-SQL ignores trailing blanks when it compares "
            f"strings and Spark does not -- so which one this column wants "
            f"is a decision, not a mapping",
            "flag"))
    return (emitted, Finding(
        "SQ61_CAST_LENGTH",
        f"{written} -> {emitted}; {measured}, so a value longer than "
        f"{length} characters used to survive whole with no finding raised",
        "rewrite"))


def _cast_parts(arg: str):
    """(expression, target type) of a CAST's single `expr AS type` argument.

    The LAST top-level AS is this cast's; an earlier one belongs to a nested
    CAST, which the caller reaches on its own match. Same rule as
    `_cast_type_spans`, which cannot be reused here because it works in
    whole-statement offsets and this works on one argument's text.
    """
    view = _masked(arg)
    depth, last, index = 0, None, 0
    while index < len(view):
        char = view[index]
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif depth == 0:
            word = _AS_WORD_RE.match(view, index)
            if word is not None:
                last = word
                index = word.end()
                continue
        index += 1
    if last is None:
        return None
    return arg[:last.start()].strip(), arg[last.end():].strip()


# Separate patterns rather than one with an optional group, because the two
# emit different functions: TRY_CAST returns NULL where CAST raises, and
# `try_cast` is the Spark function with that semantics (see SQ84). The
# lookbehind keeps `CAST` from matching inside `TRY_CAST`.
_CAST_ONLY_RE = re.compile(r"(?<![\w$.])CAST\s*\(", re.IGNORECASE)
_TRY_CAST_ONLY_RE = re.compile(r"(?<![\w$.])TRY_CAST\s*\(", re.IGNORECASE)


def rule_cast_length(sql: str, findings: list) -> str:
    """SQ61: CAST(c AS varchar(n)) -> substring(CAST(c AS STRING), 1, n).

    Registered BEFORE `rule_data_types`, which is what makes it work at all:
    SQ60 maps `varchar(20)` to `STRING` and the length is gone after it. What
    this rule leaves behind is `CAST(... AS STRING)`, which SQ60 then
    recognises as already-Spark and skips.

    CONVERT and TRY_CONVERT do not come through here -- they build their own
    call text -- but they share `_truncating_cast`, so all four positions give
    one answer. Asserted.
    """
    def builder(emit):
        def build(args):
            if len(args) != 1:
                return (None, None)
            parts = _cast_parts(args[0])
            if parts is None:
                return (None, None)
            expr, written = parts
            limit = _string_length_limit(written)
            if limit is None:
                return (None, None)  # not a length-limited string; SQ60's
            return _truncating_cast(emit, expr, written, limit)
        return build

    sql = _rewrite_call(sql, _masked(sql), _TRY_CAST_ONLY_RE, findings,
                        builder("try_cast"))
    return _rewrite_call(sql, _masked(sql), _CAST_ONLY_RE, findings,
                         builder("CAST"))


# A constraint name may be quoted and contain spaces: `` CONSTRAINT `pk
# claim` PRIMARY KEY `` (SQ10 has turned the brackets into backticks by now).
_TABLE_CONSTRAINT_RE = re.compile(
    r"^\s*(?:CONSTRAINT\s+(?:\[[^\]]+\]|`[^`]+`|\"[^\"]+\"|\S+)\s+)?"
    r"(?P<kind>PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\b",
    re.IGNORECASE)
_INLINE_PK_RE = re.compile(r"\s+(?P<kind>PRIMARY\s+KEY|UNIQUE)\b", re.IGNORECASE)
# The closing \b matters: without it a column called `identity_no` lost its
# first eight letters.
_IDENTITY_RE = re.compile(r"\s*\bIDENTITY\b\s*(?:\([^)]*\))?", re.IGNORECASE)
_DEFAULT_RE = re.compile(r"\bDEFAULT\b", re.IGNORECASE)
_CHECK_WORD_RE = re.compile(r"\s*\bCHECK\s*(?=\()", re.IGNORECASE)
# `REFERENCES other(id) ON DELETE CASCADE NOT FOR REPLICATION` is one clause.
# A referenced column list cannot nest, so a plain `\([^)]*\)` is enough
# there; `CHECK` can nest and is found by `_inline_check_spans` instead.
_INLINE_REFERENCES_RE = re.compile(
    rf"\s*\bREFERENCES\s+{_CREATE_TABLE_NAME_PART}"
    rf"(?:\s*\.\s*{_CREATE_TABLE_NAME_PART}){{0,2}}\s*(?:\([^)]*\))?"
    rf"(?:\s+ON\s+(?:DELETE|UPDATE)\s+"
    rf"(?:NO\s+ACTION|CASCADE|SET\s+NULL|SET\s+DEFAULT))*"
    rf"(?:\s+NOT\s+FOR\s+REPLICATION)?",
    re.IGNORECASE)
_INLINE_COLLATE_RE = re.compile(
    rf"\s*\bCOLLATE\s+{_CREATE_TABLE_NAME_PART}", re.IGNORECASE)


def _inline_check_spans(item_view: str):
    """(start, end) of every inline `CHECK (...)`, parentheses balanced.

    A regex cannot do this one: `CHECK (a IN (1, 2))` nests, and a
    `\\([^)]*\\)` tail would cut at the inner `)` and leave a stray one in the
    column list. `_closing_index` already counts depth for the table-level
    form, so this asks it.
    """
    spans = []
    for match in _CHECK_WORD_RE.finditer(item_view):
        close = _closing_index(item_view, match.end())
        if close is not None:
            spans.append((match.start(), close))
    return spans


def _spans_of(pattern):
    """A span finder for a plain regex, so every modifier has one shape."""
    def find(item_view: str):
        return [(m.start(), m.end()) for m in pattern.finditer(item_view)]
    return find


# Inline column modifiers Spark has no usable syntax for: (rule id, finder,
# why it went). Each is removed, and each says which one it was -- "the DDL
# was cleaned" is not an answer to "what did you take out of my table".
#
# Measured on Spark 4.2.0 AND 3.5.0, both with `USING parquet`. (`USING delta` measures
# nothing here: the probe has no Delta jar, so every one of these statements
# fails at DATA_SOURCE_NOT_FOUND before the parser or the analyzer is
# reached.) What each did:
#
#                                          Spark 4.2.0            Spark 3.5.0
#   id INT IDENTITY(1,1)                   PARSE_SYNTAX_ERROR     PARSE_SYNTAX_ERROR
#   id INT PRIMARY KEY                     UNSUPPORTED_FEATURE    PARSE_SYNTAX_ERROR
#   id INT UNIQUE                          UNSUPPORTED_FEATURE    PARSE_SYNTAX_ERROR
#   id INT NOT NULL CHECK (id > 0)         UNSUPPORTED_FEATURE    PARSE_SYNTAX_ERROR
#   fk INT REFERENCES q_base(id)           UNSUPPORTED_FEATURE    PARSE_SYNTAX_ERROR
#   n STRING COLLATE Latin1_General_CI_AS  COLLATION_INVALID_NAME PARSE_SYNTAX_ERROR
#   n STRING COLLATE UTF8_LCASE            ACCEPTED               PARSE_SYNTAX_ERROR
#   n INT DEFAULT 1                        ACCEPTED               ACCEPTED
#
# **The AIDP cluster runs Spark 3.5.0**, measured on fabricTest, so the
# right-hand column is the one that decides. The 4.2.0 column was here
# alone and is kept because the difference is the point: on 4.2.0 the four
# constraints PARSE and fail at ANALYSIS, and on the target they do not
# parse at all. Every one is rejected on both, so all of them are removed
# rather than left behind a flag -- the decision never depended on which.
#
# COLLATE is where the versions genuinely part company, and the original
# caveat here was written for the wrong Spark. It said 4.2.0 "does have
# collations and does accept its own names -- COLLATE UTF8_LCASE and
# COLLATE UTF8_BINARY were both ACCEPTED", so only T-SQL's names had to go.
# Collations arrived in Spark 4.0. On 3.5.0 **no** COLLATE clause parses,
# UTF8_LCASE included. Removing the clause unconditionally is therefore
# more clearly right on the target than the old note implied, not less --
# but the reason had to be corrected, because a reader on 3.5 who tried to
# preserve a Spark-native collation would find it does not exist.
#
# Order matters only in that PRIMARY KEY/UNIQUE and IDENTITY come first, so
# the two findings that existed before this list did keep their old relative
# positions in a mixed column definition.
_INLINE_MODIFIERS = (
    ("SQ70_INLINE_CONSTRAINT", _spans_of(_INLINE_PK_RE),
     "Spark has no column constraint syntax, and Fabric declares these NOT "
     "ENFORCED, so nothing that was being enforced is lost"),
    ("SQ70_IDENTITY", _spans_of(_IDENTITY_RE),
     "Spark has no auto-increment column; generate keys explicitly or use "
     "monotonically_increasing_id()"),
    ("SQ70_INLINE_CHECK", _inline_check_spans,
     "Spark CREATE TABLE has no CHECK constraint. Unlike the others this "
     "one carried information: Fabric did not enforce it either, but the "
     "predicate was the only written record of what the column is supposed "
     "to hold, and it is now nowhere in the artifact -- keep it somewhere"),
    ("SQ70_INLINE_REFERENCES", _spans_of(_INLINE_REFERENCES_RE),
     "Spark CREATE TABLE has no foreign key syntax; the relationship was "
     "NOT ENFORCED in Fabric and is now not declared either, so nothing "
     "downstream can read it"),
    ("SQ70_COLLATE", _spans_of(_INLINE_COLLATE_RE),
     "Spark 4.2.0 has collations but not T-SQL's names, so this one is a "
     "COLLATION_INVALID_NAME error. The column falls back to Spark's "
     "default, which is case- and accent-SENSITIVE: a comparison, a join "
     "key or an ORDER BY that relied on a CI/AI collation will now give a "
     "different answer"),
)


def rule_constraints(sql: str, findings: list) -> str:
    """SQ70: remove table constraints and inline modifiers; keep DEFAULT.

    Spark CREATE TABLE has no constraint syntax, so leaving these in place
    makes the DDL unparseable and the artifact useless. This is the one rule
    that deletes rather than flags-and-leaves, and every deletion says so --
    see `_INLINE_MODIFIERS` for what each one costs and for what Spark 4.2.0
    actually did with it.

    EVERY CREATE TABLE column list in the document is cleaned, not the
    first, and a CTAS's SELECT parentheses are not one: `_column_list_bodies`
    records what the single-`search` version left behind.

    Every pattern here looks for a *keyword*, so the view has identifier
    bodies blanked as well as literals. Without that, these patterns read the
    word inside the backticks SQ10 had just produced and cut at those offsets:
    `[identity]` became ``` `` ``` -- the empty identifier E1 refuses, emitted
    two rules later with a `rewrite` finding calling it a success -- while
    `[identity no]` became `` `no` `` and `[my unique id]` became `` `my id` ``.
    Renaming a user's column silently is exactly what this tool must not do.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for open_index, _close in _column_list_bodies(view):
        split = split_call_arguments(view, open_index)
        if split is None:
            # Unterminated, and SQ01_UNTERMINATED_* has already said so for
            # the document. Skipping this one body rather than the whole
            # rule keeps the other CREATE TABLEs in the file cleaned.
            continue
        spans, _close_index = split
        body = _cleaned_table_body(sql, view, spans, findings)
        if body is not None:
            replacements.append((spans[0][0], spans[-1][1], body))
    return apply_replacements(sql, replacements)


def _cleaned_table_body(sql, view, spans, findings: list):
    """One CREATE TABLE body with its constraints out, or None if untouched.

    Rebuilt from the kept items, each with its inline modifiers cut out, so
    every edit stays inside this body. The document-wide `.sub` this
    replaced reached string literals ('is unique' lost a word) and any later
    statement in the file.
    """
    kept = []
    for start, end in spans:
        constraint = _TABLE_CONSTRAINT_RE.match(view[start:end])
        if constraint:
            kind = " ".join(constraint.group("kind").upper().split())
            findings.append(Finding(
                "SQ70_TABLE_CONSTRAINT",
                f"{kind} constraint removed: Spark CREATE TABLE has no constraint "
                f"syntax, and Fabric declares these NOT ENFORCED, so nothing that "
                f"was enforced is lost",
                "flag"))
            continue
        kept.append((start, end))

    changed = len(kept) != len(spans)
    cleaned = []
    for start, end in kept:
        item, item_view = sql[start:end], view[start:end]
        if _DEFAULT_RE.search(item_view):
            findings.append(Finding(
                "SQ70_DEFAULT",
                "DEFAULT kept but needs review: Spark supports column defaults only "
                "behind a table feature, and dropping one would change what a later "
                "INSERT writes",
                "flag"))
        for rule_id, find_spans, why in _INLINE_MODIFIERS:
            # Locate on the masked item, name the text from the raw item at
            # the same offsets, cut from both, then keep the two views
            # aligned for the next modifier.
            cuts = find_spans(item_view)
            if not cuts:
                continue
            for cut_start, cut_end in cuts:
                findings.append(Finding(
                    rule_id,
                    f"{' '.join(item[cut_start:cut_end].split())!r} removed "
                    f"from a column definition: {why}",
                    "flag"))
            edits = [(a, b, "") for a, b in cuts]
            item = apply_replacements(item, edits)
            item_view = apply_replacements(item_view, edits)
            changed = True
        cleaned.append(item.rstrip())

    if not changed:
        return None
    return ",".join(cleaned)


_CONVERT_RE = re.compile(r"\bCONVERT\s*\(", re.IGNORECASE)
# Separate from _CONVERT_RE rather than an optional group in it, so the two
# rules stay separately registered and separately reported. The patterns are
# disjoint: `\b` matches no boundary inside `TRY_CONVERT`.
_TRY_CONVERT_RE = re.compile(r"(?<![\w$.])TRY_CONVERT\s*\(", re.IGNORECASE)
_DATEADD_RE = re.compile(r"\bDATEADD\s*\(", re.IGNORECASE)
_IIF_RE = re.compile(r"\bIIF\s*\(", re.IGNORECASE)
# Construct -> (pattern, explanation). Each fires at most once per statement.
_FLAG_ONLY = {
    "SQ80_STRING_AGG": (
        re.compile(r"\bSTRING_AGG\s*\(", re.IGNORECASE),
        "STRING_AGG has no exact Spark equivalent: concat_ws(sep, collect_list(x)) "
        "loses the WITHIN GROUP ordering guarantee"),
    # `_MERGE_STATEMENT_RE` and not the bare keyword. The pattern here was
    # `\bMERGE\s+(?!JOIN\b)`, which reads the word and not the statement,
    # so this rule's message -- about OUTPUT and WHEN NOT MATCHED BY SOURCE
    # -- was printed for SQL containing neither. MEASURED on 2a223bd,
    # `translate(sql, kind="view", item="AcmeDW")`, all five SQ80_MERGE:
    #
    #   SELECT merge FROM dbo.claim          a column of that name
    #   SELECT t.merge FROM dbo.claim t      qualified by its alias
    #   SELECT a AS merge FROM dbo.claim     an output column named that
    #   CREATE TABLE dbo.t (merge INT)       a column being declared
    #   UPDATE dbo.t SET merge = 1           a column being assigned
    #
    # Each was otherwise clean and each was pushed to REVIEW by a name --
    # the family fixed in #27, where NB12 hit CTE names and `EXTRACT/TRIM
    # ... FROM`. The rewrites were all correct; only the finding was wrong.
    # `INNER MERGE JOIN` is excluded by the same shape rather than by the
    # old `(?!JOIN\b)`, and SQ80_JOIN_HINT still reports it.
    "SQ80_MERGE": (
        _MERGE_STATEMENT_RE,
        "T-SQL MERGE and Delta MERGE INTO differ in their clause set (OUTPUT, "
        "WHEN NOT MATCHED BY SOURCE); rewrite deliberately"),
    "SQ80_OUTPUT": (
        re.compile(r"\bOUTPUT\s+(?:inserted|deleted)\b", re.IGNORECASE),
        "the OUTPUT clause has no Spark equivalent; capture affected rows with a "
        "separate query"),
    "SQ80_HINT": (
        re.compile(r"\bWITH\s*\(\s*NOLOCK\b|\bOPTION\s*\(", re.IGNORECASE),
        "query hints have no Spark equivalent and are not needed; remove them"),
    # A join hint names a physical strategy. The join itself translates, so
    # refusing the whole object over the hint word would be over-refusal;
    # it is flagged in place, the same treatment WITH (NOLOCK) and
    # OPTION (...) already get, and for the same reason. Measured on Spark
    # 4.2.0: `SELECT t.a FROM t INNER HASH JOIN u ON t.a=u.a` and the MERGE
    # spelling both give PARSE_SYNTAX_ERROR "extra input 'HASH'" / "'MERGE'".
    "SQ80_JOIN_HINT": (
        re.compile(r"\b(?:HASH|LOOP|MERGE|REMOTE|REDUCE|REPLICATE|"
                   r"REDISTRIBUTE)\s+JOIN\b", re.IGNORECASE),
        "a T-SQL join hint (HASH/LOOP/MERGE/REMOTE JOIN) names a physical "
        "join strategy; Spark rejects the word and chooses its own plan. "
        "Delete the hint -- the join itself is fine"),
    # APPLY is a lateral join, not a hint, and the rewrite depends on what
    # is being applied: a correlated subquery becomes LATERAL, a
    # table-valued function has no Spark equivalent at all. Named rather
    # than guessed at, and narrower than SQ01 because the rest of the
    # statement is an ordinary SELECT. Measured on Spark 4.2.0:
    # `SELECT * FROM t CROSS APPLY f(t.a) x` -> PARSE_SYNTAX_ERROR at
    # 'APPLY': missing 'JOIN'; the OUTER spelling likewise.
    "SQ80_APPLY": (
        re.compile(r"\b(?:CROSS|OUTER)\s+APPLY\b", re.IGNORECASE),
        "CROSS/OUTER APPLY is T-SQL's lateral join and Spark rejects the "
        "keyword. A correlated subquery rewrites to LATERAL; a table-valued "
        "function has no Spark equivalent and needs a hand rewrite"),
    "SQ80_DATEPART": (
        re.compile(r"\bDATEPART\s*\(", re.IGNORECASE),
        "DATEPART(unit, x) maps to different Spark functions per unit "
        "(year/month/dayofmonth/hour); rewrite explicitly"),
    "SQ80_STUFF": (
        re.compile(r"\bSTUFF\s*\(", re.IGNORECASE),
        "STUFF has no Spark equivalent; compose substring and concat"),
    "SQ80_ISNUMERIC": (
        re.compile(r"\bISNUMERIC\s*\(", re.IGNORECASE),
        "ISNUMERIC has no Spark equivalent; use a regex or try_cast(... AS DOUBLE) "
        "IS NOT NULL, which is stricter"),
    # Passed through untouched, these changed values with no finding: T-SQL
    # 7 / 2 is 3, Spark's is 3.5, and the view was graded PASS. Operand
    # types are not visible in the text, so every occurrence is flagged.
    "SQ80_DIVISION": (
        re.compile(r"(?<![/*])/(?![/*])"),
        "T-SQL int / int truncates toward zero; Spark `/` always returns a "
        "double. Where both operands are integers use `div` (or CAST one side "
        "if the fraction is wanted)"),
    "SQ80_AVG": (
        re.compile(r"\bAVG\s*\(", re.IGNORECASE),
        "T-SQL AVG over an integer column returns an integer; Spark avg returns "
        "a double. Where the column is an integer, wrap it: CAST(avg(x) AS INT)"),
}


def _convert_builder(call: str, emit: str, prefix: str, why: str):
    """The shared decision for CONVERT and TRY_CONVERT.

    One path, parameterised, because the two differ in exactly two ways: what
    they emit, and what happens on a bad value. Everything else -- the type
    resolution, the lossy-mapping flag, the unmapped refusal, the style code,
    the arity -- is the same question, and answering it twice is how the two
    would drift apart.
    """
    def build(args):
        if len(args) == 2:
            resolved = _spark_convert_type(args[0])
            if resolved is None:
                return (None, Finding(
                    f"{prefix}_TYPE",
                    f"{call}({args[0]}, {args[1]}) left in place: "
                    f"{args[0].strip()!r} is not a type this tool can map to "
                    f"Spark, and a cast to it would not run -- rewrite by hand",
                    "flag"))
            limit = _string_length_limit(args[0])
            if limit is not None:
                # `CONVERT(varchar(20), c)` truncates in T-SQL and a plain
                # cast to STRING does not; SQ61 owns that decision for every
                # position, so this asks it rather than answering again.
                return _truncating_cast(emit, args[1], args[0].strip(), limit)
            spark_type, caveat = resolved
            emitted = f"{emit}({args[1]} AS {spark_type})"
            if caveat:
                return (emitted, Finding(
                    f"{prefix}_TYPE_LOSS",
                    f"{call}({args[0]}, {args[1]}) -> {emitted}; Spark has no "
                    f"{args[0].strip().upper()}, so the nearest type was used "
                    f"and the difference needs review -- {caveat}",
                    "flag"))
            return (emitted, Finding(
                prefix,
                f"{call}({args[0]}, {args[1]}) -> {emitted}; operand order "
                f"reverses (T-SQL puts the target type first, Spark the value) "
                f"and the type name maps to Spark's. {why}",
                "rewrite"))
        if len(args) == 3:
            return (None, Finding(
                f"{prefix}_STYLE",
                f"{call} with style code {args[2]!r} has no Spark equivalent; use "
                f"date_format or to_date with an explicit pattern",
                "flag"))
        return (None, Finding(
            f"{prefix}_ARITY",
            f"{call} with {len(args)} argument(s) is not a recognised form", "flag"))
    return build


def rule_convert(sql: str, findings: list) -> str:
    """SQ81: CONVERT(type, expr) -> CAST(expr AS <spark type>).

    Two things change: the operand order reverses, and the type name is
    mapped. Without the mapping this emitted `CAST(c AS nvarchar(50))`, which
    Spark rejects -- measured. An unmapped name is refused rather than passed
    through, because passing it through is what made the output unrunnable.
    """
    return _rewrite_call(sql, _masked(sql), _CONVERT_RE, findings,
                         _convert_builder(
                             "CONVERT", "CAST", "SQ81_CONVERT",
                             "CONVERT raises on a value it cannot convert, "
                             "and so does Spark's CAST under ANSI mode."))


def rule_try_convert(sql: str, findings: list) -> str:
    """SQ84: TRY_CONVERT(type, expr) -> try_cast(expr AS <spark type>).

    `_CONVERT_RE` never matched this: `\b` finds no boundary inside
    `TRY_CONVERT`, the `_` being a word character -- the same miss E-c found
    for TRY_CAST. So every TRY_CONVERT passed through verbatim at flags=0, and
    on Spark 4.2.0 `TRY_CONVERT` is UNRESOLVED_ROUTINE whatever its type
    argument, so the silence was the bug independently of the mapping.

    The target is `try_cast`, not `CAST`. TRY_CONVERT returns NULL where
    CONVERT raises, and `try_cast` is the Spark function with that semantics:
    measured, `try_cast('abc' AS int)` is NULL while `cast('abc' AS int)`
    raises CAST_INVALID_INPUT under ANSI mode. Mapping this to CAST would turn
    a NULL the query was written to expect into a failed query.

    Same type table as CONVERT, so the same F3 deferral applies: a length is
    dropped, see `_spark_convert_type`.
    """
    return _rewrite_call(sql, _masked(sql), _TRY_CONVERT_RE, findings,
                         _convert_builder(
                             "TRY_CONVERT", "try_cast", "SQ84_TRY_CONVERT",
                             "try_cast is the Spark function that returns NULL "
                             "rather than raising, which is what TRY_CONVERT "
                             "does; CAST would raise instead."))


def rule_dateadd(sql: str, findings: list) -> str:
    """SQ82: DATEADD(day, n, d) -> (d + make_interval(0, 0, 0, n))."""
    def build(args):
        if len(args) != 3:
            return (None, Finding(
                "SQ82_DATEADD_ARITY",
                f"DATEADD with {len(args)} argument(s) is not the T-SQL form", "flag"))
        part = args[0].strip("'\"[]`").casefold()
        if part not in _DAY_PARTS:
            return (None, Finding(
                "SQ82_DATEADD_UNIT",
                f"DATEADD({part}, ...) has no single Spark equivalent; add_months "
                f"covers months and interval arithmetic covers the time units",
                "flag"))
        # T-SQL DATEADD returns the type of its date argument. Spark's date_add
        # always returns DATE, so a datetime lost its time of day and
        # `>= DATEADD(day, -7, GETDATE())` became midnight a week ago. Adding
        # an interval keeps DATE as DATE and TIMESTAMP as TIMESTAMP; a string
        # literal is a datetime in T-SQL, which is what timestampadd returns.
        if args[2].strip().lstrip("( ").upper().startswith(("'", "N'")):
            emitted = f"timestampadd(DAY, {args[1]}, {args[2]})"
        else:
            emitted = f"({args[2]} + make_interval(0, 0, 0, {args[1]}))"
        return (emitted, Finding(
            "SQ82_DATEADD",
            f"DATEADD({part}, {args[1]}, {args[2]}) -> {emitted}",
            "rewrite"))
    return _rewrite_call(sql, _masked(sql), _DATEADD_RE, findings, build)


def rule_iif(sql: str, findings: list) -> str:
    """SQ83: IIF(c, a, b) -> if(c, a, b)."""
    def build(args):
        if len(args) != 3:
            return (None, Finding(
                "SQ83_IIF_ARITY",
                f"IIF with {len(args)} argument(s) is not the T-SQL form", "flag"))
        return (f"if({', '.join(args)})", Finding(
            "SQ83_IIF", "IIF(c, a, b) -> if(c, a, b)", "rewrite"))
    return _rewrite_call(sql, _masked(sql), _IIF_RE, findings, build)


# T-SQL's LIKE understands `[A-Z]`, `[^0-9]` and `[abc]`; Spark's does not,
# and reads each character of the class literally. Measured on Spark 4.2.0
# against what T-SQL returns:
#
#   'Alpha' like '[A-Z]%'        -> False   (T-SQL: true)
#   'Alpha' like '[^0-9]%'       -> False   (T-SQL: true)
#   'a5bc9' like 'a[0-9]_c%'     -> False   (T-SQL: true)
#   'Alpha' rlike '^[A-Z].*$'    -> True
#   'Alpha' rlike '^[^0-9].*$'   -> True
#   'a5bc9' rlike '^a[0-9].c.*$' -> True
#
# so the predicate silently matched nothing. RLIKE is a whole-row regex
# rather than a pushdown-friendly prefix match, so it is used ONLY when a
# class is actually present: a plain `LIKE 'abc%'` is already correct.
_LIKE_KEYWORD_RE = re.compile(r"\bLIKE\b", re.IGNORECASE)
_LIKE_ESCAPE_RE = re.compile(r"\s*\bESCAPE\b", re.IGNORECASE)
# The characters that mean something to a regex and nothing to LIKE, so they
# have to be escaped where they appear as literal pattern text.
_REGEX_METACHARACTERS = frozenset(".*+?()[]{}|^$\\")
# Special INSIDE a Java character class, where the outer set is not: `\` and
# `[` open things, `]` closes the class, and `&&` is a set intersection. `-`
# is deliberately absent -- a range is exactly what it means in T-SQL too.
_REGEX_CLASS_METACHARACTERS = frozenset("\\[]&")


def _escape_class_body(body: str) -> str:
    return "".join(("\\" + c) if c in _REGEX_CLASS_METACHARACTERS else c
                   for c in body)


def _like_to_regex(pattern: str):
    """(regex, None) for a T-SQL LIKE pattern, or (None, why it was refused).

    Anchored at both ends because LIKE matches the whole value and RLIKE is a
    search. `%` -> `.*`, `_` -> `.`, `[...]`/`[^...]` kept, and every regex
    metacharacter appearing as literal text escaped.
    """
    out, index, length = ["^"], 0, len(pattern)
    while index < length:
        char = pattern[index]
        if char == "%":
            out.append(".*")
        elif char == "_":
            out.append(".")
        elif char == "[":
            cursor = index + 1
            negate = cursor < length and pattern[cursor] == "^"
            if negate:
                cursor += 1
            body_start = cursor
            # T-SQL reads a `]` immediately after `[` or `[^` as a member of
            # the set rather than its closer, and so does POSIX; Java does
            # not, which is why the body is re-escaped below.
            if cursor < length and pattern[cursor] == "]":
                cursor += 1
            while cursor < length and pattern[cursor] != "]":
                cursor += 1
            if cursor >= length:
                return (None, f"the `[` at offset {index} is never closed")
            body = pattern[body_start:cursor]
            if not body:
                return (None, f"the `[]` at offset {index} is an empty "
                              f"character class, which matches nothing")
            out.append("[" + ("^" if negate else "")
                       + _escape_class_body(body) + "]")
            index = cursor + 1
            continue
        else:
            if char in _REGEX_METACHARACTERS:
                out.append("\\")
            out.append(char)
        index += 1
    out.append("$")
    return ("".join(out), None)


def _tsql_string_literal(text: str) -> str:
    r"""`text` as a T-SQL single-quoted literal: `'` doubled, nothing else.

    T-SQL spelling on purpose, like every literal a rule emits. The backslash
    doubling Spark needs -- its parser processes backslash escapes inside a
    literal by default (spark.sql.parser.escapedStringLiterals=false), and an
    escape it does not recognise, `\.`, loses the backslash rather than
    raising -- is done once, for every literal, by SQ17 at the end of RULES.
    Doing it here as well used to be right; with SQ17 in place it would
    double twice, and the escaped dot `\.` this rule builds would reach the
    regex engine as `\\.` -- a literal backslash, then any character.
    """
    return "'" + text.replace("'", "''") + "'"


def rule_like_character_class(sql: str, findings: list) -> str:
    """SQ90: LIKE '<pattern with a character class>' -> RLIKE '<regex>'.

    Only when a class is present. A plain `LIKE 'abc%'` means the same thing
    in both dialects and is left alone, which keeps the cheap operator where
    it is correct and keeps the diff small.

    A non-literal pattern -- `c LIKE other` -- cannot be inspected and is left
    exactly as written; nothing here guesses at a value it cannot see.

    An `ESCAPE` clause is refused when a class is present rather than
    modelled: the escape character decides which `[` is a class opener and
    which is a literal, and getting that wrong changes which rows match with
    no error. With no class present the clause is left alone -- Spark's LIKE
    supports ESCAPE, so there is nothing to fix.

    SQ90_LIKE_BACKSLASH: a plain pattern holding a backslash, with no ESCAPE
    clause, has the backslash doubled. T-SQL's LIKE has no default escape
    character and Spark's is the backslash, so `c LIKE 'a\\b%'` meant a
    different pattern on Spark. In the RLIKE form the regex builder already
    escapes a literal backslash, and SQ17 does the literal-level doubling
    for both forms.
    """
    view = _masked(sql)
    # Keyword scan, so identifier bodies are blanked too: `[like]` is a
    # column name, and SQ10 has already turned it into `` `like` `` by now.
    keywords = _without_identifier_bodies(view)
    literals = {start: end for start, end in _literal_spans(view)}
    replacements = []
    for match in _LIKE_KEYWORD_RE.finditer(keywords):
        index = match.end()
        while index < len(view) and view[index] in _WS:
            index += 1
        end = literals.get(index)
        if end is None or end - index < 2 or sql[end - 1] != "'":
            # Not a literal pattern, or an unterminated one -- SQ01 has the
            # second case. Either way there is nothing here to read.
            continue
        pattern = sql[index + 1:end - 1].replace("''", "'")
        written = sql[match.start():end]
        escape = _LIKE_ESCAPE_RE.match(keywords, end)
        if "[" not in pattern:
            if "\\" in pattern and escape is None:
                # T-SQL's LIKE has no escape character unless ESCAPE names
                # one, so `\` is a plain character to it. Spark's LIKE
                # escapes with `\` by default, so the same pattern escaped
                # whatever followed the backslash instead of matching it.
                # Doubled at the pattern level here; SQ17 then doubles again
                # at the literal level, so Spark's LIKE receives `\\`, an
                # escaped backslash, which matches one backslash.
                doubled = _tsql_string_literal(pattern.replace("\\", "\\\\"))
                replacements.append((index, end, doubled))
                findings.append(Finding(
                    "SQ90_LIKE_BACKSLASH",
                    f"{written}: the backslash doubled in the pattern. T-SQL's "
                    f"LIKE has no default escape character, so `\\` matches "
                    f"itself; Spark's LIKE escapes with `\\` by default, so it "
                    f"escaped the next character instead -- or raised, where "
                    f"that character is not one Spark's LIKE lets it escape",
                    "rewrite"))
            continue
        if escape is not None:
            findings.append(Finding(
                "SQ90_LIKE_ESCAPE",
                f"{written} carries an ESCAPE clause and a `[...]` character "
                f"class. The escape character decides which `[` opens a class "
                f"and which is a literal, and a wrong reading changes which "
                f"rows match with no error -- so the predicate is left "
                f"exactly as written for a hand rewrite. Note that Spark's "
                f"LIKE reads the class literally, so as it stands it matches "
                f"nothing the class was meant to match",
                "flag"))
            continue
        regex, refused = _like_to_regex(pattern)
        if regex is None:
            findings.append(Finding(
                "SQ90_LIKE_PATTERN",
                f"{written} was left as written: {refused}. T-SQL's reading "
                f"of a malformed class is not something this tool will guess "
                f"at, and Spark's LIKE matches the class literally either way",
                "flag"))
            continue
        replacements.append((match.start(), match.end(), "RLIKE"))
        emitted = _tsql_string_literal(regex)
        replacements.append((index, end, emitted))
        findings.append(Finding(
            "SQ90_LIKE_CHARACTER_CLASS",
            f"{written} -> RLIKE '{_spark_literal_body(emitted[1:-1])}'; "
            f"Spark's LIKE "
            f"reads `[A-Z]` as four literal characters, so the predicate "
            f"matched nothing -- measured on 4.2.0, 'Alpha' like '[A-Z]%' is "
            f"False while 'Alpha' rlike '^[A-Z].*$' is True. The regex is "
            f"case-SENSITIVE, as Spark is; if the source column has T-SQL's "
            f"usual case-insensitive collation then `[A-Z]` matched lower "
            f"case too and the class needs widening by hand",
            "rewrite"))
    return apply_replacements(sql, replacements)


# Where the left operand of an `=` begins, the word or mark that opened it
# says whether the `=` compares or assigns. T-SQL spells a column alias
# `SELECT label = expr` and an update `SET c = expr`, and neither compares
# anything; a flag there would be noise on every UPDATE in the estate.
_COMPARISON_OPENERS = frozenset("where on having when and or not (".split())
_ASSIGNMENT_OPENERS = frozenset("set select ,".split())
_CLAUSE_WORDS = _COMPARISON_OPENERS | _ASSIGNMENT_OPENERS
_WORD_RE = re.compile(r"[A-Za-z_]\w*")
_WHEN_BEFORE_RE = re.compile(r"(?<![\w$])WHEN$", re.IGNORECASE)
_THEN_AFTER_RE = re.compile(r"THEN\b", re.IGNORECASE)


def _operand_opener(view: str, index: int):
    """The clause word, `(` or `,` that opens the expression ending at index.

    Walks left at the same parenthesis depth, so `f(a, b) = 'x '` reads past
    the call to the WHERE before it. None when nothing recognisable opens it.
    """
    depth, position = 0, index
    while position > 0:
        position -= 1
        char = view[position]
        if char == ")":
            depth += 1
        elif char == "(":
            if depth == 0:
                return "("
            depth -= 1
        elif depth == 0 and char in ",;":
            return char if char == "," else None
        elif (depth == 0 and (char.isalpha() or char == "_")
              and (position == 0 or not (view[position - 1].isalnum()
                                         or view[position - 1] in "_$@#"))):
            word = _WORD_RE.match(view, position).group(0).casefold()
            if word in _CLAUSE_WORDS:
                return word
    return None


def _padded_comparison(view: str, start: int, end: int) -> bool:
    """True when the literal at start..end is an operand of = / <> / !=, or
    the comparand of a simple CASE's WHEN, in a comparing position."""
    after = end
    while after < len(view) and view[after] in _WS:
        after += 1
    before = start
    while before > 0 and view[before - 1] in _WS:
        before -= 1
    if view.startswith(("<>", "!=", "="), after):
        operator = after
    elif view[max(before - 2, 0):before] in ("<>", "!="):
        operator = before - 2
    elif (before > 0 and view[before - 1] == "="
          and view[before - 2:before - 1] not in ("<", ">", "!")):
        operator = before - 1
    else:
        # `CASE c WHEN 'abc ' THEN ...` compares c to the literal, padded,
        # with no operator in sight. A WHEN directly before and a THEN
        # directly after is that form and no other.
        return bool(_WHEN_BEFORE_RE.search(view, 0, before)
                    and _THEN_AFTER_RE.match(view, after))
    return _operand_opener(view, operator) in _COMPARISON_OPENERS


def rule_padded_comparison(sql: str, findings: list) -> str:
    """SQ18: flag = / <> against a string literal that ends in a space.

    T-SQL compares strings with ANSI padding -- the shorter side is padded
    with spaces first, so trailing spaces never decide an = or <> -- and
    Spark compares exactly. Reproduced live on AIDP Spark 3.5.0:

        SELECT 'abc' = 'abc   '    SQL Server: true    Spark: false

    A flag, not a rewrite: rtrim on both sides reproduces the padding, but
    it also changes what an index or a partition filter can use, and it is
    not this rule's call whether the trailing space was meant.

    The LITERAL case only, by design. A column holding 'abc   ' compared to
    'abc' has the same difference, and the text cannot show it; a detector
    for that would have to flag every string comparison in the estate. The
    positions read as comparisons are the operand of `=`, `<>` or `!=` whose
    left side opens with WHERE, ON, HAVING, WHEN, AND, OR, NOT or `(`, and
    a simple CASE's `WHEN 'x ' THEN`. `SELECT label = 'x '` (an alias) and
    `SET c = 'x '` (an assignment) are T-SQL's other `=` and are left
    alone, as is anything this cannot place. IN lists and `<`/`>` are not
    covered.
    """
    view = _masked(sql)
    keywords = _without_identifier_bodies(view)
    unterminated = _unterminated(sql)
    open_at = (unterminated[1] if unterminated and unterminated[0] == "literal"
               else None)
    padded = []
    for start, end in _literal_spans(view):
        if start == open_at or end - start < 2:
            continue
        body = sql[start + 1:end - 1]
        if body.endswith(" ") and _padded_comparison(keywords, start, end):
            padded.append(sql[start:end])
    if padded:
        findings.append(Finding(
            "SQ18_TRAILING_SPACE_COMPARE",
            f"{len(padded)} comparison(s) against a string literal ending in "
            f"a space ({', '.join(padded[:3])}"
            f"{', ...' if len(padded) > 3 else ''}): T-SQL ignores trailing "
            f"spaces in = and <>, so 'abc' = 'abc   ' is true, and Spark "
            f"compares exactly and returns false -- measured on AIDP Spark "
            f"3.5.0. Left as written; if the padding is not meant, rtrim "
            f"both sides by hand",
            "flag"))
    return sql


def _spark_literal_body(body: str) -> str:
    """The body of a T-SQL string literal, respelled for Spark's lexer.

    T-SQL writes an apostrophe inside a literal as `''`. Spark's lexer does
    not: it reads `'it''s'` as the two adjacent literals `'it'` and `'s'`,
    which it concatenates -- measured on AIDP Spark 3.5.0, `SELECT 'it''s'`
    returns `its`. Spark's own spelling is the backslash escape `\\'`.

    T-SQL has no backslash escapes at all, and Spark does: measured on the
    same cluster, `'C:\\new\\table'` returned C:<newline>ew<tab>able. So
    every backslash is doubled -- FIRST, so that the backslash the quote
    escape introduces is not doubled with the rest.
    """
    return body.replace("\\", "\\\\").replace("''", "\\'")


def rule_spark_string_literals(sql: str, findings: list) -> str:
    r"""SQ16/SQ17: respell every string literal for Spark's lexer, LAST.

    SQ16_QUOTE_ESCAPE: `'it''s'` -> `'it\'s'`. Before this rule the literal
    passed through untouched at flags=0 and returned `its` -- the apostrophe
    silently gone, on every row.

    SQ17_BACKSLASH_ESCAPE: `'C:\new\table'` -> `'C:\\new\\table'`. T-SQL
    has no backslash escapes; Spark does, so the path came back as
    C:<newline>ew<tab>able, again at flags=0. This assumes Spark's default
    spark.sql.parser.escapedStringLiterals=false, which is AIDP's.

    Last in RULES, and that is the whole design. Every rule above scans with
    the T-SQL lexer (`_masked`), which reads `\'` as a closing quote followed
    by an unterminated literal; run any of them after this and the rest of
    the object masks as literal text. The rules above also emit literals of
    their own -- SQ90's RLIKE regex, SQ31's `''` separator -- and they emit
    them in T-SQL spelling, so this one pass converts every literal in the
    output exactly once, whoever wrote it.
    """
    view = _masked(sql)
    unterminated = _unterminated(sql)
    open_at = (unterminated[1] if unterminated and unterminated[0] == "literal"
               else None)
    replacements, quoted, backslashed = [], 0, 0
    for start, end in _literal_spans(view):
        if start == open_at:
            # SQ01 has flagged it and the object is left as written; a
            # respelling of half a literal would be invention.
            continue
        body = sql[start + 1:end - 1]
        spark = _spark_literal_body(body)
        if spark != body:
            replacements.append((start + 1, end - 1, spark))
            quoted += "''" in body
            backslashed += "\\" in body
    if backslashed:
        findings.append(Finding(
            "SQ17_BACKSLASH_ESCAPE",
            f"{backslashed} string literal(s) with a backslash respelled: "
            f"T-SQL has no backslash escapes, while Spark reads one inside a "
            f"literal as an escape -- measured on AIDP Spark 3.5.0, "
            f"'C:\\new\\table' returned C:<newline>ew<tab>able. Each `\\` "
            f"is now `\\\\`, one literal backslash",
            "rewrite"))
    if quoted:
        findings.append(Finding(
            "SQ16_QUOTE_ESCAPE",
            f"{quoted} string literal(s) with a doubled quote "
            f"respelled: T-SQL's `''` inside a literal is one apostrophe, "
            f"while Spark reads 'it''s' as two adjacent literals and "
            f"concatenates them -- measured on AIDP Spark 3.5.0, "
            f"SELECT 'it''s' returns its. Each `''` is now Spark's `\\'`",
            "rewrite"))
    return apply_replacements(sql, replacements)


def rule_flag_only(sql: str, findings: list) -> str:
    """SQ80: constructs with no safe rewrite. Reported once each, never changed."""
    # Another keyword scan, so the same view: `[Sales/Units]` is a column name
    # and its `/` is not a division. This used to blank backtick bodies only,
    # with its own pattern, which covered the case where SQ10 had already
    # converted the brackets and missed the rest.
    view = _without_identifier_bodies(_masked(sql))
    for rule_id, (pattern, detail) in _FLAG_ONLY.items():
        if pattern.search(view):
            findings.append(Finding(rule_id, detail, "flag"))
    return sql


_KNOWN_TYPE_NAMES = frozenset(
    set(_TYPE_MAP) | set(_TYPE_FLAGS) | {
        "int", "integer", "bigint", "smallint", "tinyint", "date", "time",
        "timestamp", "boolean", "double", "float", "string", "binary",
        "decimal", "numeric", "bit", "real",
    })
_CREATE_OR_ALTER_RE = re.compile(
    r"\bCREATE\s+OR\s+ALTER\s+(?P<what>VIEW|FUNCTION|PROCEDURE|PROC)\b",
    re.IGNORECASE)
_FILEGROUP_RE = re.compile(
    r"\s*(?:TEXTIMAGE_ON|ON)\s+(?:\[[^\]]+\]|`[^`]+`|\"[^\"]+\"|\w+)"
    r"(?=\s*(?:;|$|TEXTIMAGE_ON))",
    re.IGNORECASE)
# `DEFAULT NULL` is a default, not a nullability marker; stripping its NULL
# left `mgr INT DEFAULT,` behind.
_COLUMN_NULL_RE = re.compile(
    r"(?<!NOT)(?<!IS)(?<!DEFAULT)\s+NULL\b(?=\s*(?:,|\)))", re.IGNORECASE)
# Two shapes. The first is `CONSTRAINT <name> PRIMARY KEY`, still intact.
# The second is an *orphan*: rule_constraints runs earlier and has already
# cut the `PRIMARY KEY`, so the name is left alone against a `,` or `)` --
# `CREATE TABLE t (a INT CONSTRAINT `my pk`)`, which no engine accepts.
# Found here; the sibling project's fix branch has the same gap.
_INLINE_NAMED_CONSTRAINT_RE = re.compile(
    r"\s+CONSTRAINT\s+(?:\[[^\]]+\]|`[^`]+`|\w+)"
    r"(?:\s+(?=DEFAULT\b|PRIMARY\b|UNIQUE\b|CHECK\b|FOREIGN\b)"
    r"|\s*(?=[,)]|$))",
    re.IGNORECASE)


def rule_create_or_alter(sql: str, findings: list) -> str:
    """SQ12: CREATE OR ALTER <x> -> CREATE OR REPLACE <x>.

    The single most common rejection in a live parse of 31 real objects.
    """
    view = _masked(sql)
    replacements = []
    for match in _CREATE_OR_ALTER_RE.finditer(view):
        what = match.group("what").upper()
        replacements.append((match.start(), match.end(),
                             f"CREATE OR REPLACE {what}"))
        findings.append(Finding(
            "SQ12_CREATE_OR_ALTER",
            f"CREATE OR ALTER {what} -> CREATE OR REPLACE {what}", "rewrite"))
    return apply_replacements(sql, replacements)


def rule_filegroup(sql: str, findings: list) -> str:
    """SQ71: strip ON <filegroup> / TEXTIMAGE_ON <filegroup>.

    Storage placement is a SQL Server concept with no Spark equivalent and no
    effect on the data; leaving it makes the DDL unparseable.
    """
    view = _masked(sql)
    replacements = []
    for match in _FILEGROUP_RE.finditer(view):
        replacements.append((match.start(), match.end(), ""))
        findings.append(Finding(
            "SQ71_FILEGROUP",
            f"{' '.join(sql[match.start():match.end()].split())!r} removed; "
            f"filegroup placement has no Spark equivalent and does not affect "
            f"the data",
            "flag"))
    return apply_replacements(sql, replacements)


def rule_column_nullability(sql: str, findings: list) -> str:
    """SQ72: strip an explicit column-level NULL marker.

    T-SQL allows `mgr INT NULL` to mean "nullable". Spark's DDL has no such
    marker -- columns are nullable unless declared NOT NULL -- so the word is
    a parse error. NOT NULL and `IS NULL` predicates are untouched.
    """
    view = _masked(sql)
    replacements = []
    matches = [m for open_index, body_end in _column_list_bodies(view)
               for m in _COLUMN_NULL_RE.finditer(view, open_index + 1, body_end)]
    for match in matches:
        replacements.append((match.start(), match.end(), ""))
        findings.append(Finding(
            "SQ72_COLUMN_NULL",
            "explicit NULL nullability marker removed; Spark columns are "
            "nullable unless declared NOT NULL",
            "rewrite"))
    return apply_replacements(sql, replacements)


# Delta's own list: DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES names exactly these.
_DELTA_INVALID_NAME_CHARS = frozenset(" ,;{}()\n\t=")
_DELTA_COLUMN_MAPPING = ("TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
                         "'delta.minReaderVersion' = '2', "
                         "'delta.minWriterVersion' = '5')")
# Searched only between the end of a table's column list (or, for a CTAS,
# between the table name and `AS SELECT`) and the end of the statement --
# never across the query. A document-wide search matched the `USING` of a
# join: measured before that was anchored,
# `SELECT [claim id] INTO dbo.t FROM dbo.a JOIN dbo.b USING (k)` reported
# SQ77_DELTA_COLUMN_NAME "this table already declares USING or
# TBLPROPERTIES" -- it declares neither, and the table that needed mapping
# did not get it.
_TABLE_CLAUSE_RE = re.compile(r"\b(TBLPROPERTIES|USING)\b", re.IGNORECASE)
_CREATE_TABLE_RE = re.compile(r"\bCREATE\s+TABLE\b", re.IGNORECASE)
_CTAS_RE = re.compile(r"\bAS\s+(?:SELECT|WITH|\()", re.IGNORECASE)
_SELECT_RE = re.compile(r"\bSELECT\b(?:\s+(?:ALL|DISTINCT)\b)?", re.IGNORECASE)
# What ends a select list. `INTO` is not here: rule_select_into runs earlier
# in RULES and has already rewritten it into the CREATE TABLE header.
_SELECT_LIST_END_RE = re.compile(
    r"\b(?:FROM|UNION|INTERSECT|EXCEPT)\b", re.IGNORECASE)


def _ctas_select_lists(view: str, start: int, end: int) -> list:
    """(lo, hi) of each depth-0 select list in the CTAS body `view[start:end]`.

    A CTAS's columns are the names its select list projects, and nothing
    else in the query names a column of the table being created. Scanning
    the whole query instead counted the *table* it reads from: measured,
    `SELECT a INTO dbo.t FROM [my table]` raised column mapping on a table
    whose only column is `a` and reported "column name(s) `my table`" -- the
    wrong construct named, and reader 2 / writer 5 on a table that never
    needed it.

    Depth is counted from `start`, so a `WITH c AS (SELECT ...)` in front of
    the real query contributes nothing and `AS (SELECT ...)` -- where `start`
    is placed inside the parenthesis by the caller -- contributes its own
    list. Every depth-0 list is taken, not just the first: a set operation
    takes its output names from the leading branch, and reading the others
    too can only ask for mapping that was not needed, never miss it.

    A `*` in the list is not resolvable here -- the names come from a table
    this function cannot see -- so it contributes nothing, which is what a
    star has always done in this translator.
    """
    spans, depth, position = [], 0, start
    while position < end:
        char = view[position]
        if char in "([":
            depth += 1
            position += 1
            continue
        if char in ")]":
            depth -= 1
            position += 1
            continue
        if depth == 0:
            match = _SELECT_RE.match(view, position)
            if match:
                stop = end
                inner, cursor = 0, match.end()
                while cursor < end:
                    if view[cursor] in "([":
                        inner += 1
                    elif view[cursor] in ")]":
                        if inner == 0:
                            break
                        inner -= 1
                    elif inner == 0:
                        tail = _SELECT_LIST_END_RE.match(view, cursor)
                        if tail:
                            stop = cursor
                            break
                    cursor += 1
                else:
                    stop = end
                if cursor < end and stop == end:
                    stop = cursor
                spans.append((match.end(), stop))
                position = match.end()
                continue
        position += 1
    return spans


def _column_names(sql: str, view: str, start: int, end: int) -> list:
    """The name opening each top-level entry of a CREATE TABLE body, unquoted.

    `view` has identifier bodies blanked, so a `,` or `(` inside `` `a,b` ``
    does not split the entry; the name itself is read back from `sql`, which
    has the same offsets.
    """
    names, depth, entry = [], 0, start
    for position in range(start, end + 1):
        char = view[position] if position < end else ","
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif char == "," and depth == 0:
            text = sql[entry:position].strip()
            if text.startswith("`"):
                close = text.find("`", 1)
                while close != -1 and text[close + 1:close + 2] == "`":
                    close = text.find("`", close + 2)
                if close != -1:
                    names.append(text[1:close].replace("``", "`"))
            elif text:
                names.append(text.split()[0])
            entry = position + 1
    return names


def rule_delta_column_mapping(sql: str, findings: list) -> str:
    """SQ77: a column name Delta refuses gets Delta column mapping.

    T-SQL allows any character in a bracketed column name, and `[claim id]`
    is ordinary in a Warehouse. SQ10 backquotes it correctly -- Spark's parser
    accepts `` `claim id` `` -- but AIDP creates a Delta table by default and
    Delta refuses the name unless the table uses column mapping. Measured on a
    live workspace: the translated `CREATE TABLE default.AcmeDW.claim
    (`claim id` BIGINT NOT NULL, ...)` failed with
    DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES, graded OK, and every view and
    notebook reading the table failed after it. The same DDL with the
    properties below created the table, took inserts and served the view.

    `spark.sql.sources.default=delta` IS AIDP's, measured on the cluster
    rather than assumed: on fabricTest (Spark 3.5.0) a bare
    `CREATE TABLE fabric_probe.p2_src (a INT)` came back `Provider =
    delta`, `spark.sql.sources.default` read `delta`, and
    `spark.sql.legacy.createHiveTableByDefault` read `false`. So this rule
    is not inert on the target. It is still named here -- the
    same way SQ17 assumes `spark.sql.parser.escapedStringLiterals=false`.
    That setting is the one thing that would make this rule inert, and it is
    named here so the next reader knows what to check rather than having to
    rediscover it. Nothing above holds on a stock Spark: measured on
    pyspark 4.2.0 (JAVA_HOME=openjdk@21, Hive support on, session defaults)
    `spark.sql.sources.default` is `parquet`, and
    `CREATE TABLE z (`claim id` INT) TBLPROPERTIES (...)` is accepted, comes
    back `Provider = parquet`, and carries the three properties as ordinary
    table properties that nothing reads. Reproducing the Delta failure this
    rule exists for took `spark.sql.sources.default=delta` -- and, on the
    Spark 3.5 the review used, `spark.sql.legacy.createHiveTableByDefault
    =false` as well, which already defaults to false on 4.2.0.

    The failure mode of a wrong assumption here is a no-op, not bad SQL: on a
    non-Delta default the properties are inert and the table still creates.

    Mapping by name raises the table's protocol (reader 2, writer 5), which
    AIDP reads. That cost lands on the table being created, so the rule asks
    for it only where a name of that table is the one Delta refuses -- see
    `_ctas_select_lists` for the CTAS half, which used to read the whole
    query and bill a table for a name that was never its column.

    An explicit USING or TBLPROPERTIES is the author's own choice and is
    flagged rather than merged into. Measured on Spark 4.2.0
    (pyspark 4.2.0, JAVA_HOME=openjdk@21, session defaults), that refusal is
    load-bearing rather than polite: `CREATE TABLE q (c INT) TBLPROPERTIES
    (...) USING parquet` is PARSE_SYNTAX_ERROR at 'USING', so merging by
    appending would have had to know the clause order too. Inserting
    straight after the column list is safe for the clauses Spark does
    accept -- `TBLPROPERTIES` before and after `PARTITIONED BY` and before
    `COMMENT` all parse, the grammar takes those in any order.

    One shape is left alone on purpose. A trailing Synapse
    `WITH (DISTRIBUTION = HASH(x))` ends up after the inserted properties and
    does not parse -- but measured on the same Spark, neither does the input
    without this rule (`CREATE TABLE q (c INT) WITH (DISTRIBUTION = HASH(x))`
    is PARSE_SYNTAX_ERROR at '(' with no rule involved), and neither does the
    other order. Moving `insert_at` past the trailing clause would buy a
    statement that still does not parse, so it is not done; the construct
    needs a rule of its own, and it is Synapse-shaped rather than Fabric
    Warehouse.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for header in _CREATE_TABLE_RE.finditer(view):
        statement_end = _statement_bounds(view, header.start())[1]
        ctas = _CTAS_RE.search(view, header.end(), statement_end)
        body = None if ctas else _CREATE_TABLE_BODY_RE.match(view, header.start())
        if body:
            body_end = _closing_index(view, body.end() - 1)
            if body_end is None:
                continue
            names = _column_names(sql, view, body.end(), body_end - 1)
            insert_at = body_end
            # A table clause sits after the column list, never inside it.
            clause_from, clause_to = body_end, statement_end
        elif ctas:
            # SELECT ... INTO arrives here as CREATE TABLE t AS SELECT, and
            # Delta checks a CTAS schema the same way. Its columns are the
            # ones the select list projects -- only there, because the rest
            # of the query names tables, not columns of the table being
            # created.
            lists = _ctas_select_lists(
                view, ctas.end() if view[ctas.end() - 1] == "(" else ctas.start(),
                statement_end)
            # A name *followed* by `.` is a qualifier, so `[my db].[claim id]`
            # offers one candidate, not two. A name *preceded* by `.` is the
            # last part of a qualified column reference and is exactly the
            # output column: `SELECT c.[claim id] INTO dbo.t` creates a table
            # whose column is `claim id`, and skipping it left that table
            # failing to create with the error this rule exists to prevent.
            names = [sql[start + 1:end - 1].replace("``", "`")
                     for start, end in _identifier_spans(_masked(sql))
                     if sql[start] == "`" and view[end:end + 1] != "."
                     and any(lo <= start < hi for lo, hi in lists)]
            insert_at = ctas.start()
            while insert_at > header.end() and view[insert_at - 1] in _WS:
                insert_at -= 1
            # A CTAS takes its table clauses between the name and `AS`.
            clause_from, clause_to = header.end(), ctas.start()
        else:
            continue
        refused = [name for name in names if _DELTA_INVALID_NAME_CHARS & set(name)]
        if not refused:
            continue
        shown = ", ".join(f"`{name}`" for name in refused)
        if _TABLE_CLAUSE_RE.search(view, clause_from, clause_to):
            findings.append(Finding(
                "SQ77_DELTA_COLUMN_NAME",
                f"column name(s) {shown} hold a character Delta refuses "
                f"(space , ; {{ }} ( ) = tab or newline) and this table already "
                f"declares USING or TBLPROPERTIES; enable "
                f"'delta.columnMapping.mode' = 'name' or rename the column(s)",
                "flag"))
            continue
        replacements.append((insert_at, insert_at, " " + _DELTA_COLUMN_MAPPING))
        findings.append(Finding(
            "SQ77_DELTA_COLUMN_MAPPING",
            f"column name(s) {shown} hold a character Delta refuses; the table "
            f"is created with Delta column mapping by name so the names survive",
            "rewrite"))
    return apply_replacements(sql, replacements)


def rule_inline_named_constraint(sql: str, findings: list) -> str:
    """SQ73: drop the `CONSTRAINT <name>` prefix from an inline constraint.

    Spark has no constraint-naming syntax. Removing only the name leaves the
    clause itself for SQ70 to handle or keep.
    """
    view = _masked(sql)
    replacements = []
    for match in _INLINE_NAMED_CONSTRAINT_RE.finditer(view):
        # Keep the separating space only when a clause follows it; an orphan
        # name sits against `,` or `)` and needs none.
        following = view[match.end():match.end() + 1]
        replacements.append((match.start(), match.end(),
                             "" if following in (",", ")", "") else " "))
        findings.append(Finding(
            "SQ73_CONSTRAINT_NAME",
            f"{' '.join(sql[match.start():match.end()].split())!r} removed; "
            f"Spark has no constraint-naming syntax",
            "flag"))
    return apply_replacements(sql, replacements)


# Ported back from the sibling Fabric migrator, which took this rule set and
# then found these three gaps against a real Fabric tenant. All three were
# still live here, and all three produced output that looks migrated and does
# not run.
# Case-insensitive, because `go` is the same directive as `GO` to sqlcmd and
# SSMS and a lower-case one used to pass straight through with no finding --
# the same silent hole SQ02_GO_COUNT closes for the batch-repeat form. A line
# whose entire content is the word `go` is not SQL under any reading, so
# matching it costs nothing.
_GO_RE = re.compile(r"(?mi)^[ \t\r]*GO[ \t\r]*;?[ \t\r]*$\n?")
# `GO 5` is T-SQL's batch-repeat: "send the batch and run it 5 times".
# `_GO_RE` cannot match it -- it wants GO alone on the line -- so it reached
# the output verbatim with no finding, and Spark's parser stops at it.
# Measured: `SELECT 1 / GO 5 / SELECT 2` came back unchanged, findings=[],
# flags=0, PASS. Separate pattern rather than an optional group in _GO_RE,
# because the two have opposite outcomes: one is rewritten, one is refused.
_GO_COUNT_RE = re.compile(
    r"(?mi)^[ \t\r]*GO[ \t\r]+(?P<count>\d+)[ \t\r]*;?[ \t\r]*$")
_SCHEMABINDING_RE = re.compile(
    r"\bWITH\s+SCHEMABINDING\s*(?:,\s*VIEW_METADATA\s*)?", re.IGNORECASE)
_ALTER_ADD_CONSTRAINT_RE = re.compile(
    r"\bALTER\s+TABLE\s+[^;]*?\bADD\s+(?:CONSTRAINT\s+[^\s(]+\s+)?"
    r"(?P<kind>PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK|DEFAULT)\b[^;]*;?\s*",
    re.IGNORECASE)


def rule_go_batch(sql: str, findings: list) -> str:
    """SQ02: replace the `GO` batch separator with the `;` it stands for.

    `GO` is not SQL. It is a client instruction meaning "send everything since
    the last one", and Spark has no notion of batches -- its parser stops dead
    at the word. But it IS a statement separator, and this rule used to delete
    it and leave nothing in its place, which ran two statements together.
    Measured:

      SELECT TOP 5 * FROM dbo.a / GO / SELECT * FROM dbo.b
        -> SELECT * FROM default.W.a
           SELECT * FROM default.W.b LIMIT 5      flags=0

    Both halves wrong: the statements merged, and SQ50 then read the merged
    text as one statement and appended the first batch's row cap to the end
    of the second. `_statement_bounds` was written for exactly this and
    already understands `;` at depth 0, so giving it the separator fixes both
    halves at once.

    Splitting rather than refusing, deliberately. A refusal -- "this GO batch
    carries a TOP, review it by hand" -- would fix the misplaced LIMIT and
    leave the merge, which is wrong on its own and wrong in every batch that
    has no TOP in it; and since a DacFx export puts GO between objects, it
    would push whole files to REVIEW for a separator the tool knows how to
    write. The `;` costs nothing that deleting the GO did not already cost.

    A `;` is added only where one is actually needed: not when the preceding
    statement already ends in one, not before a GO with nothing after it, and
    not for a leading GO, which terminates nothing.

    SQ02_GO_COUNT: `GO n`, the batch-repeat form, is refused rather than
    split. It means "run the preceding batch n times", which Spark SQL cannot
    express, and it is a separate pattern because the two forms have opposite
    outcomes -- one is rewritten, one is left as written and flagged.
    """
    view = _masked(sql)
    counts = [m.group("count") for m in _GO_COUNT_RE.finditer(view)]
    if counts:
        # Refused, not rewritten: `GO n` runs the batch n times, and Spark
        # SQL has no loop to express that. Writing the statement out n times
        # would be inventing a meaning for a directive whose point is often
        # a side effect the repetition is supposed to accumulate.
        findings.append(Finding(
            "SQ02_GO_COUNT",
            f"`GO {'`, `GO '.join(counts)}` is T-SQL's batch-repeat form: it "
            f"runs the preceding batch that many times. Spark SQL cannot "
            f"express it and its parser stops at the word, so the line is "
            f"left exactly as written for a hand rewrite rather than dropped "
            f"-- dropping it would silently run the batch once",
            "flag"))
    spans = [(m.start(), m.end()) for m in _GO_RE.finditer(view)]
    if not spans:
        return sql
    # A scratch copy with the GO lines blanked, so a run of two GOs does not
    # read the first one as the end of the preceding statement and try to
    # insert a `;` inside a span this rule is already deleting -- which
    # `apply_replacements` rightly refuses as overlapping.
    scratch = apply_replacements(view, [(a, b, " " * (b - a)) for a, b in spans])
    replacements = [(a, b, "") for a, b in spans]
    inserts = set()
    for start, end in spans:
        before = scratch[:start].rstrip()
        if not before or before.endswith(";"):
            continue
        if not scratch[end:].strip():
            continue  # nothing follows: there is no second statement to part
        inserts.add(len(before))
    replacements.extend((at, at, ";") for at in inserts)
    findings.append(Finding(
        "SQ02_GO_BATCH",
        f"{len(spans)} `GO` batch separator(s) replaced by `;`: GO is a "
        f"client directive rather than SQL and Spark has no batches, but it "
        f"does separate statements, and deleting it without a separator ran "
        f"consecutive statements together -- which also moved a TOP's LIMIT "
        f"onto the following statement. {len(inserts)} `;` added",
        "rewrite"))
    return apply_replacements(sql, replacements)


# `SET <option> ON|OFF`, T-SQL's session-setting statement. The tail is part
# of the match so the `;` and the line break go with the statement when one
# is removed, rather than leaving a bare `;` on a line of its own.
# `SET IDENTITY_INSERT dbo.t ON` does not match -- the table name sits where
# ON|OFF has to be -- and neither does Spark's own `SET key=value`...
_SET_OPTION_RE = re.compile(
    r"\bSET\s+(?P<option>[A-Za-z_]\w*)\s+(?P<state>ON|OFF)\b"
    r"(?P<tail>[ \t]*;?[ \t]*(?:\r?\n)?)",
    re.IGNORECASE)
# ...so `SET IDENTITY_INSERT <table> ON|OFF` gets its own pattern. It is the
# one session setting that carries an object name, which is why it reached no
# rule at all: the name sits exactly where `_SET_OPTION_RE` needs ON|OFF.
# Up to three name parts, because T-SQL permits `db.schema.table` here.
_SET_IDENTITY_INSERT_RE = re.compile(
    rf"\bSET\s+IDENTITY_INSERT\s+"
    rf"(?P<target>{_IDENT_PART}(?:\s*\.\s*{_IDENT_PART}){{0,2}})\s+"
    rf"(?P<state>ON|OFF)\b",
    re.IGNORECASE)


def rule_session_settings(sql: str, findings: list) -> str:
    """SQ14: drop `SET QUOTED_IDENTIFIER ON`; flag every other session setting.

    Three outcomes, and which one a statement gets is the whole of this rule:
    QUOTED_IDENTIFIER ON is dropped, `IDENTITY_INSERT <table> ON|OFF` is
    flagged under its own id, and everything else is flagged as
    SQ14_SESSION_SETTING. The reasons the three differ are below.

    Measured before this rule:

      SET QUOTED_IDENTIFIER ON;
      SELECT a FROM dbo.t   -> unchanged apart from SQ11; findings: SQ11 only

    and on Spark 4.2.0 (pyspark 4.2.0, JAVA_HOME=openjdk@21, session
    defaults) every one of these is REJECTED with INVALID_SET_SYNTAX,
    "Expected format is 'SET', 'SET key', or 'SET key=value'":

      SET QUOTED_IDENTIFIER ON     SET ANSI_NULLS ON    SET NOCOUNT ON
      SET QUOTED_IDENTIFIER OFF    SET XACT_ABORT ON    SET ANSI_PADDING ON

    while `SET spark.sql.shuffle.partitions=8` is ACCEPTED -- it is this
    shape, not the keyword, that Spark has no reading for. DacFx puts two of
    these at the top of every exported file, so one unflagged pair takes the
    whole object down.

    QUOTED_IDENTIFIER ON is the only one dropped, and dropping rather than
    refusing is the whole of the decision here. ON is T-SQL's default, and it
    is the reading every scan in this module already uses -- see the `_masked`
    note at the top of the file. The statement therefore asserts what was
    assumed anyway: removing it cannot change how a single character of the
    object is read, and removing it is what lets the object run. Refusing an
    object over a line that says "behave normally" would be over-refusal.

    Everything else is flagged and left as written, including
    QUOTED_IDENTIFIER OFF, whose own branch in SQ10 reports the consequence
    that makes it matter. `SET ANSI_NULLS ON` and `SET NOCOUNT ON` are very
    probably free as well, and `SET ARITHABORT` / `SET ANSI_WARNINGS` very
    probably are not -- each is a separate question about a separate setting,
    and none of them was measured beyond the rejection above. A flag naming
    the option is what this rule can honestly say.

    `SET IDENTITY_INSERT <table> ON|OFF` is the one session setting that
    carries an *object name*, and that is why it used to reach no rule here
    at all: the table sits exactly where `_SET_OPTION_RE` needs ON|OFF.
    MEASURED before SQ14_IDENTITY_INSERT, `translate(sql, kind="table",
    item="W")`:

        SET IDENTITY_INSERT dbo.t ON;
        SELECT 1              ->  unchanged                 findings: []

    and on Spark 4.2.0 (pyspark 4.2.0, JAVA_HOME=openjdk@21, session
    defaults) it is rejected in exactly the same class as the settings above:

        SET IDENTITY_INSERT dbo.t ON   INVALID_SET_SYNTAX
        SET IDENTITY_INSERT t ON       INVALID_SET_SYNTAX

    It is flagged and **not** dropped, and the difference from
    QUOTED_IDENTIFIER ON is the point. That one asserts the reading every
    scan in this module already applies, so removing it cannot change how a
    character of the object is read. This one is a *permission*: it is what
    lets a following INSERT write the table's identity column at all, so
    removing the line changes what that INSERT is allowed to do. The name is
    left as written too -- `_OBJECT_KEYWORDS` does not reach it -- because a
    rewritten table name inside a statement Spark rejects would make the line
    read as translated work.

    What the finding also has to say is that the premise has moved
    underneath the statement. SQ70_IDENTITY strips `IDENTITY(...)` out of the
    column definition, so a table *this tool* emitted has no identity
    property left for IDENTITY_INSERT to override, and the following INSERT
    is legal for a different reason than the source relied on. A Delta table
    declared `GENERATED ALWAYS AS IDENTITY` by hand would refuse it. Which of
    the two a reader has is not in the SQL, so both are named and neither is
    assumed.
    """
    view = _without_identifier_bodies(_masked(sql))
    replacements = []
    for match in _SET_IDENTITY_INSERT_RE.finditer(view):
        if not _heads_a_statement(view, match.start()):
            continue
        target = sql[match.start("target"):match.end("target")]
        state = match.group("state").upper()
        findings.append(Finding(
            "SQ14_IDENTITY_INSERT",
            f"`SET IDENTITY_INSERT {target} {state}` has no Spark equivalent "
            f"and the statement as written stops the object running: Spark's "
            f"SET takes `key=value` and rejects this shape outright "
            f"(INVALID_SET_SYNTAX on 4.2.0, the same class as `SET "
            f"QUOTED_IDENTIFIER ON`). It is left exactly as written, name "
            f"included, and NOT dropped the way QUOTED_IDENTIFIER ON is: "
            f"that one asserts the reading this translator already applies, "
            f"while this one is a permission -- it is what lets a following "
            f"INSERT write the identity column of {target} at all, so "
            f"removing the line changes what that INSERT is allowed to do. "
            f"Note the premise has also moved: SQ70_IDENTITY strips "
            f"`IDENTITY(...)` out of the column definition, so a table this "
            f"tool emitted has no identity property left to override and the "
            f"INSERT is legal for a different reason than the source relied "
            f"on -- while a Delta table declared `GENERATED ALWAYS AS "
            f"IDENTITY` by hand would refuse it. Decide which target you "
            f"have before deleting the line",
            "flag"))

    for match in _SET_OPTION_RE.finditer(view):
        if not _heads_a_statement(view, match.start()):
            # Not the head of a statement, so this `SET` is part of something
            # else. A session setting always begins one.
            continue
        option = match.group("option").upper()
        state = match.group("state").upper()
        if option == "QUOTED_IDENTIFIER":
            if state == "OFF":
                continue  # SQ14_QUOTED_IDENTIFIER_OFF, raised by SQ10
            replacements.append((match.start(), match.end(), ""))
            findings.append(Finding(
                "SQ14_QUOTED_IDENTIFIER_ON",
                "`SET QUOTED_IDENTIFIER ON` removed; Spark rejects the "
                "statement outright (INVALID_SET_SYNTAX on 4.2.0) and it "
                "would have taken the whole object down with it. ON is "
                "T-SQL's default and is the reading this translator already "
                "applies to every double-quoted name in the file, so nothing "
                "about the object's meaning changes with the line gone",
                "rewrite"))
            continue
        findings.append(Finding(
            "SQ14_SESSION_SETTING",
            f"`SET {option} {state}` has no Spark equivalent: Spark's SET "
            f"takes `key=value` and rejects this shape outright "
            f"(INVALID_SET_SYNTAX on 4.2.0), so the statement as written "
            f"stops the object running. It is left in place rather than "
            f"dropped, because whether {option} changes the answer is a "
            f"question about that one setting and this tool has not measured "
            f"it -- QUOTED_IDENTIFIER ON is dropped only because it is the "
            f"default and is what the translator already assumes",
            "flag"))
    return apply_replacements(sql, replacements)


def rule_schemabinding(sql: str, findings: list) -> str:
    """SQ74: strip `WITH SCHEMABINDING` from a view definition.

    It stops the base tables being altered underneath the view. Spark cannot
    express that guarantee, and the view's own logic does not depend on it --
    so the clause goes and the loss is flagged.
    """
    view = _masked(sql)
    replacements = [(m.start(), m.end(), "") for m in
                    _SCHEMABINDING_RE.finditer(view)]
    if replacements:
        findings.append(Finding(
            "SQ74_SCHEMABINDING",
            "`WITH SCHEMABINDING` removed; Spark cannot stop the base tables "
            "changing under a view, so that protection is lost",
            "flag"))
    return apply_replacements(sql, replacements)


def rule_alter_add_constraint(sql: str, findings: list) -> str:
    """SQ75: drop a whole `ALTER TABLE ... ADD CONSTRAINT ...` statement.

    SQ70 only reaches constraints written inside a `CREATE TABLE (...)` body.
    Fabric exports the same constraints as separate `ALTER TABLE` statements
    after the table, and those reached no rule here -- so SQ73 stripped the
    constraint *name* and left ``ADD PRIMARY KEY NONCLUSTERED (...)``, which no
    engine accepts, with no finding raised. Emitting invalid SQL silently is
    the one thing this tool is not allowed to do.

    Spark has no `ALTER TABLE ... ADD CONSTRAINT` in any form, so the statement
    goes whole, the same call SQ70 makes for the inline case. Fabric declares
    these NOT ENFORCED, so nothing that was enforced is lost.
    """
    view = _masked(sql)
    replacements, kinds = [], []
    for match in _ALTER_ADD_CONSTRAINT_RE.finditer(view):
        kinds.append(" ".join(match.group("kind").upper().split()))
        replacements.append((match.start(), match.end(), ""))
    for kind in kinds:
        findings.append(Finding(
            "SQ75_ALTER_ADD_CONSTRAINT",
            f"ALTER TABLE ... ADD {kind} statement removed; Spark has no "
            f"ALTER TABLE ADD CONSTRAINT, and Fabric declares these "
            f"NOT ENFORCED, so nothing that was enforced is lost",
            "flag"))
    return apply_replacements(sql, replacements)


# Rules that need to know the target catalog, and the smaller set that also
# needs the owning Fabric item. Kept explicit so the other twenty keep their
# two-argument shape.
def rule_undeclared_columns(sql: str, findings: list, *, table_catalog=None,
                            item=None) -> str:
    """SQ23: a column this statement names that its own table does not declare.

    `_columns_from_ddl` has populated the catalog's `columns` list since the
    catalog landed and nothing read it. This is what reads it.

    Rewrites nothing and never refuses. A column reference is not a name this
    tool can correct -- only a person knows whether the query is wrong or the
    catalog is stale -- so the whole value is in saying which column and which
    table, and letting a human decide.

    It answers ONLY where the table's column list is in the catalog. MEASURED
    on the bundled estate:

        tier                tables   with a column list
        warehouse_ddl           19                   17
        notebook_inferred        3                    0
        shortcut                 2                    0

    A shortcut points at storage this tool does not open, and an inferred
    table is known to exist because a notebook writes it -- neither says
    anything about shape. So for 7 of 24 tables "column not declared" would
    mean *we do not know*, and `undeclared_columns` hands back None there
    rather than a clean answer.

    Collapsing that None with the empty tuple below is deliberate and is not
    the misuse `undeclared_columns` warns about: the warning is against
    reporting the unknown case as VERIFIED, and emitting no finding claims
    nothing either way. What this must never do is grade a shortcut's column
    references as checked.

    Early in `RULES`, with the other two rules that rewrite nothing, because
    it has to read the T-SQL names the export wrote: by the time SQ11 has run,
    `dbo.claim` is `default.AcmeDW.claim` and resolves differently.
    """
    if names_no_tables(table_catalog):
        return sql
    read = referenced_columns(sql, quoted_identifier=True)
    if read is None:
        return sql
    table, columns = read
    missing = undeclared_columns(table_catalog, table, columns, item)
    if not missing:
        return sql
    findings.append(Finding(
        "SQ23_COLUMN_NOT_DECLARED",
        f"{', '.join(repr(c) for c in missing)} "
        f"{'is' if len(missing) == 1 else 'are'} referenced against "
        f"{table!r}, and the catalog's record of that table does not declare "
        f"{'it' if len(missing) == 1 else 'them'}. Either this query names a "
        f"column that does not exist -- in which case Spark will fail with "
        f"UNRESOLVED_COLUMN and the migration has carried the mistake across "
        f"-- or the table has changed since its CREATE TABLE was exported and "
        f"the catalog is stale. Nothing is rewritten: which of the two it is "
        f"cannot be decided from the export",
        "flag"))
    return sql


_CATALOG_AWARE = frozenset({rule_three_part_names, rule_two_part_names})
_ITEM_AWARE = frozenset({rule_two_part_names, rule_one_part_names,
                         rule_undeclared_columns})
# The rules that ask the resolved table catalog whether the table they are
# about to name exists. A different thing from `_CATALOG_AWARE` above, which
# is about the AIDP catalog *name*; the two sets happen to hold the same two
# rules and mean unrelated things, which is exactly the collision the
# parameter names are kept apart for.
_TABLE_CATALOG_AWARE = frozenset({rule_three_part_names, rule_two_part_names,
                                  rule_undeclared_columns})
# The rules that turn a name shape into a table name with no catalog behind
# it. `rule_two_part_names` is already switched off by passing no `item`, so
# only the three-part one needs a flag of its own.
_OBJECT_RESOLUTION_AWARE = frozenset({rule_three_part_names})

# Rules whose rewrite is its own inverse: run one on its own output and it
# undoes itself. `rule_log` is the only one, because T-SQL's LOG(value, base)
# and Spark's log(base, value) are the same call with the operands swapped,
# and a swap cannot tell its input from its output. Measured:
#
#   SELECT LOG(x, 10) -> log(10, x) -> log(x, 10) -> log(10, x) ...
#
# Harmless where it stands -- `_swap_log` visits each call exactly once --
# and fatal the moment such a rule is routed through `_to_fixed_point`, which
# re-runs a rule until the text stops changing: an alternating rewrite never
# converges, the 8-pass budget burns, and SQ03_NESTING_TOO_DEEP is then
# reported for a one-level expression. That is F2 of the previous batch
# reached from the other side.
#
# Declared here rather than left implicit for two reasons: a test asserts
# that nothing in this set reaches `_to_fixed_point`, and adding a second
# swapping rule now has to be written down rather than noticed later.
#
# An emission that did not alternate was looked for and not taken. The only
# candidate is `ln(value) / ln(base)`, and it costs an SQ80_DIVISION flag on
# every object that contains a logarithm -- measured, `SELECT ln(x) / ln(10)`
# comes back flags=1 -- so it would trade a landmine nothing currently steps
# on for a guaranteed REVIEW on every translated LOG.
_SELF_INVERSE_RULES = frozenset({rule_log})

RULES = [
    # First, and it rewrites nothing: if the masking ran off the end of the
    # input then every rule below is reading a copy in which the rest of the
    # object is literal or comment text, and the object must not grade PASS
    # for coming back unchanged.
    rule_unterminated_text,
    # Also rewrites nothing, and early so that the `#` it looks for is still
    # the one the export wrote: no rule below produces a `#`, but several
    # move text around it.
    rule_temp_table,
    # Rewrites nothing either, and early for the same reason read the other
    # way round: it resolves the table against the catalog under the name the
    # export wrote, and by the time SQ11 has run that name is the AIDP one.
    rule_undeclared_columns,
    # First of the rewrites: the N prefix has to go before anything rewrites
    # around a literal. `SELECT N'a' + N'b'` reached SQ40 as measured and came
    # back as `Nconcat('a', N)'b'`.
    rule_unicode_literals,
    rule_go_batch,
    # After rule_go_batch: DacFx separates its two header statements with
    # `GO`, and this rule asks whether a `SET` stands at the head of a
    # statement, which needs the `;` SQ02 writes where the `GO` was.
    rule_session_settings,
    rule_schemabinding,
    rule_create_or_alter,
    rule_filegroup,
    # Before the MERGE and INSERT rules: a hint between a MERGE target and
    # USING hid the statement from `_MERGE_HEAD` (see `_TABLE_HINT_RE`).
    rule_table_hints,
    rule_bracket_identifiers,
    # After SQ10, so a bracketed alias is already backticked; before SQ40,
    # SQ50 and the name rules, none of which reads a select item's alias.
    rule_alias_assignment,
    # Before the two name rules: `INSERT dbo.t VALUES (...)` has no keyword
    # in front of its target for `_OBJECT_KEYWORDS` to anchor on, and writing
    # the optional INTO in is what puts one there.
    rule_insert_without_into,
    # The same defect in the other statement whose INTO T-SQL makes optional,
    # and before the name rules for the same reason.
    rule_merge_without_into,
    rule_three_part_names,
    # After the three-part rule: that one has already consumed every name it
    # recognises, so what reaches here is two-part or nothing.
    rule_two_part_names,
    # Last of the three name rules, and the descent is deliberate: by here
    # every three- and two-part name has either been rewritten to a
    # three-part AIDP name or declined, and both are invisible to a pattern
    # that requires no following dot. What reaches this one is bare or
    # nothing.
    rule_one_part_names,
    rule_datediff,
    rule_charindex,
    # After rule_charindex, so the length argument this rule copies into the
    # output is already `locate(...)` rather than the T-SQL spelling.
    rule_substring_from_zero,
    # Before rule_string_concat, which emits `concat(...)` for T-SQL's `+`.
    # `+` propagates NULL and CONCAT does not, so the two must not share a
    # target -- and this rule's pattern would read SQ40's output as a call.
    rule_concat,
    rule_log,
    rule_scalar_renames,
    rule_string_concat,
    rule_top,
    rule_select_into,
    # Before every column rule below: it writes the parentheses that make an
    # `ALTER TABLE ... ADD` a column list, and `_column_list_bodies` is what
    # SQ60, SQ70 and SQ72 all use to find one.
    rule_alter_add_column,
    # Before rule_data_types: that rule maps `varchar(20)` to `STRING`, and
    # once it has, the length this one needs is gone.
    rule_cast_length,
    rule_data_types,
    # Before rule_constraints: that rule finishes with a document-wide
    # `_INLINE_PK_RE.sub`, which would strip "primary key" out of an ALTER
    # statement it never examined and report nothing.
    rule_alter_add_constraint,
    rule_constraints,
    rule_inline_named_constraint,
    rule_column_nullability,
    # After the column rules above, so the body it reads is final: types
    # mapped, constraints and NULL markers gone.
    rule_delta_column_mapping,
    rule_convert,
    rule_try_convert,
    rule_dateadd,
    rule_iif,
    rule_like_character_class,
    rule_padded_comparison,
    rule_flag_only,
    # LAST, after every rule that masks: its output is spelled for Spark's
    # lexer, which the T-SQL masker misreads -- `\'` looks like a closing
    # quote to it. Every literal the rules above emitted is in T-SQL spelling
    # and is converted here along with the user's, exactly once.
    rule_spark_string_literals,
]



def translate(sql, *, kind: str = "other", catalog: str = DEFAULT_CATALOG,
              item=None, table_catalog=None,
              resolve_table_names: bool = True) -> TranslationResult:
    """Translate one Warehouse object.

    `catalog` is the **AIDP catalog name** -- the string `default`, or what
    `plan --catalog` set -- and it is the first part of every table name the
    rules emit. `table_catalog` is the **resolved table catalog**, the
    `name -> {tier, owner, ...}` map `inventory.catalog` builds from the
    export, and it says whether a table exists at all; the notebook
    translator calls the same two things `aidp_catalog` and `catalog`, which
    is the wrong way round from here and is the reason both docstrings spell
    it out. Conflating them once already cost a release in which
    `plan --catalog myc` renamed tables everywhere except in notebooks.

    Without `table_catalog` the rules rewrite on name shape alone, which is
    what the whole warehouse path used to do: a reference to a shortcut, or
    to a table nothing in the export declares, got a confident three-part
    name and graded PASS while the notebook path refused the same reference.
    See `_catalog_declines`.

    `item` is the Fabric Warehouse or Lakehouse the object belongs to, which
    a DacFx export keeps in the folder name rather than in the SQL. Without
    `item` a two-part name is left alone.

    `resolve_table_names=False` is for a caller that has already resolved
    every table name in `sql` against something better than a name shape --
    a `%%tsql` notebook cell, where NB15 has run the catalog over the body
    first. SQ11's three-part rewrite is skipped there; everything else in
    that rule, the SQ15 column reading and the linked-server flag, still
    runs. See `rule_three_part_names`.
    """
    source = sql if isinstance(sql, str) else ""
    findings: list = []
    # Keyword scan, so identifier bodies are blanked as well as literals:
    # `[exec time]` is a column, not an EXEC.
    scannable = _without_identifier_bodies(_masked(source))

    procedural = kind in _PROCEDURAL_KINDS or bool(_CONTROL_FLOW_RE.search(scannable))
    if procedural:
        findings.append(Finding(
            "SQ01_PROCEDURAL",
            f"object uses T-SQL control flow ({_CONTROL_FLOW_NAMED}), which "
            f"has no Spark SQL equivalent; rewrite as PySpark by hand. This "
            f"list is what is detected, not everything T-SQL can do: a "
            f"procedural construct outside it is reported clean",
            "flag"))
    if procedural:
        return TranslationResult(source, source, findings)

    translated = source
    for rule in RULES:
        extra = {}
        if rule in _CATALOG_AWARE:
            extra["catalog"] = catalog
        if rule in _ITEM_AWARE:
            extra["item"] = item
        if rule in _TABLE_CATALOG_AWARE:
            extra["table_catalog"] = table_catalog
        if rule in _OBJECT_RESOLUTION_AWARE:
            extra["resolve_objects"] = resolve_table_names
        translated = rule(translated, findings, **extra)
    return TranslationResult(source, translated, findings)
