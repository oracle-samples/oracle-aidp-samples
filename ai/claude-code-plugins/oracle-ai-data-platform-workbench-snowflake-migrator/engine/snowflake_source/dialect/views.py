"""Is a Snowflake view body portable to Spark SQL? Pure leaf, zero I/O.

This is a SOURCE-dialect question, not a target one, which is why it lives
here: both the planner (deciding what can migrate) and the DDL generator
(emitting it) read this module, and neither imports the other.

A Snowflake-only construct is DETECTED AND REPORTED, never rewritten on a
guess. A view that ships with a subtly wrong translation is worse than one
reported as needing manual work, because the wrong one returns numbers.

Quoting is normalised on the way through (see translate.py): "quoted
identifiers" become `backticked`, `''` inside a literal becomes `\\'`, and a
$$...$$ string is refused. Spark reads "..." as a string literal and `''` as
two adjacent literals, so a body carried verbatim returned wrong data.
"""
from __future__ import annotations

import re

from . import lexer
from .translate import RULES, translate_sql

__all__ = ["UNSUPPORTED_CONSTRUCTS", "detect_unsupported_constructs",
           "extract_view_body", "extract_view_columns", "translate_view_body"]

# construct -> (regex, brief reason). Derived from the translator's declared
# rules, so this table cannot disagree with what detection actually does. A
# hand-maintained copy did: it listed DATEDIFF as blocking while no rule
# detected it, and listed IFF as blocking long after IFF was translated.
UNSUPPORTED_CONSTRUCTS: dict[str, tuple[str, str]] = {
    r.construct: (r.detect, r.detail or r.description)
    for r in RULES if r.status == "declared"}

# The header is located by SCANNING, not by regex. Two failures drove that:
#
#   * `view\s+[\w$".]+` cannot match a legal quoted name containing a space,
#     so `create view "my view" as ...` was unreadable
#   * a scan for ` as ` finds the one inside `COMMENT = \'defined as a rollup\'`
#     before the real one, and an earlier version matched the first `) as`
#     anywhere, so `select IFF(a, \'y\', \'n\') AS f` had its `) AS` mistaken for
#     the header end and the body was silently TRUNCATED to `f, ...`
#
# Truncated-but-valid SQL is the worst failure mode available here: it runs.
_CREATE_VIEW_PREFIX = re.compile(
    r"(?is)^\s*create\s+(?:or\s+replace\s+)?"
    r"(?:(?:secure|recursive|transient|volatile)\s+)*view\b")


_IF_NOT_EXISTS = re.compile(r"(?is)\s*(?:if\s+not\s+exists\b)?")
_BARE_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


def extract_view_columns(ddl: str) -> list[str]:
    """The names in a CREATE VIEW's header column list, or [] if it has none.

    GET_DDL writes `create or replace view V(\\n\\tCUSTOMER,\\n\\tTOTAL\\n) as
    select CUST_ID, SUM(AMT) ...`, and the list RENAMES the body's output
    columns. Dropping it created a view whose columns were CUST_ID and
    SUM(AMT) while the plan and the catalog API's viewFields said CUSTOMER
    and TOTAL. Each item's name is its first token; what follows it (a
    COMMENT, a MASKING POLICY, a TAG) is not a name. Unquoted names fold to
    upper case as Snowflake folds them; a quoted name is exact. A list whose
    items cannot be read raises ValueError: a guessed name is a wrong one.
    """
    if not ddl:
        return []
    try:
        parts = lexer.segments(ddl)
    except lexer.UnterminatedLiteral as exc:
        raise ValueError(f"could not scan the view DDL: {exc}") from exc
    mask = lexer.code_only(ddl)
    prefix = _CREATE_VIEW_PREFIX.match(mask)
    if not prefix:
        return []
    # Quoted identifiers are blank in the mask; their spans come from the scan.
    quoted: dict[int, int] = {}
    pos = 0
    for kind, text in parts:
        if kind == "ident":
            quoted[pos] = pos + len(text)
        pos += len(text)

    def name_end(i: int) -> int | None:
        if i in quoted:
            return quoted[i]
        m = _BARE_PART.match(mask, i)
        return m.end() if m else None

    def skip_blank(k: int) -> int:
        # Whitespace and comments are blank in the mask -- and so is a quoted
        # identifier, which is where a name may start.
        while k < len(mask) and mask[k].isspace() and k not in quoted:
            k += 1
        return k

    i = skip_blank(_IF_NOT_EXISTS.match(mask, prefix.end()).end())
    # The view name: one to three parts.
    while True:
        end = name_end(i)
        if end is None:
            return []
        i = skip_blank(end)
        if i < len(mask) and mask[i] == ".":
            i = skip_blank(i + 1)
            continue
        break
    if i >= len(mask) or mask[i] != "(":
        return []

    # Items at depth 1 of the list, split on its own commas.
    depth, start, items = 0, i + 1, []
    for j in range(i, len(mask)):
        ch = mask[j]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                items.append((start, j))
                break
        elif ch == "," and depth == 1:
            items.append((start, j))
            start = j + 1
    else:
        raise ValueError("could not read the view's column list: it never closes")

    names: list[str] = []
    for a, b in items:
        k = skip_blank(a)
        end = name_end(k) if k < b else None
        if end is None or end > b:
            raise ValueError(
                "could not read the view's column list: item "
                f"{ddl[a:b].strip()!r} does not start with a column name")
        token = ddl[k:end]
        names.append(token[1:-1].replace(token[0] * 2, token[0])
                     if token[0] in '"`' else token.upper())
    return names


def extract_view_body(ddl: str) -> str:
    """Return the SELECT body of a CREATE VIEW statement, without the trailing `;`.

    The body starts after the first `AS` keyword that is in code and at paren
    depth zero -- so an `AS` inside a literal, a comment, a quoted identifier
    or the optional column list is not mistaken for the end of the header.

    The column list is not part of the body (extract_view_columns reads it),
    but a list that cannot be read makes the view unreadable here too, so the
    planner and the DDL generator refuse the same views.
    """
    if not ddl:
        raise ValueError("could not locate a view body: empty DDL")
    try:
        parts = lexer.segments(ddl)
    except lexer.UnterminatedLiteral as exc:
        raise ValueError(f"could not scan the view DDL: {exc}") from exc

    prefix = _CREATE_VIEW_PREFIX.match(lexer.code_only(ddl))
    if not prefix:
        raise ValueError(
            "could not locate a view body: the DDL does not begin with CREATE VIEW")
    extract_view_columns(ddl)

    offset, depth = 0, 0
    for kind, text in parts:
        if kind != "code":
            offset += len(text)
            continue
        for m in re.finditer(r"[()]|\bAS\b", text, re.IGNORECASE):
            token = m.group(0)
            if token == "(":
                depth += 1
            elif token == ")":
                depth = max(0, depth - 1)
            elif depth == 0 and offset + m.start() >= prefix.end():
                body = ddl[offset + m.end():].strip().rstrip(";").strip()
                if not body:
                    raise ValueError("view body is empty after the AS keyword")
                return body
        offset += len(text)
    raise ValueError("could not locate a view body: no AS keyword outside parentheses")


def translate_view_body(body: str):
    """Translate what can be translated; report what cannot.

    Delegates to the dialect translator so the planner and the DDL generator
    cannot disagree about whether a view is migratable.
    """
    return translate_sql(body)


def detect_unsupported_constructs(body: str) -> list[dict]:
    """Constructs that BLOCK migration -- i.e. cannot be translated exactly.

    A construct the translator handles exactly is no longer a blocker, so this
    reports only the residue. Anything listed here needs statement-level
    restructuring or a design decision, not a substitution.
    """
    result = translate_sql(body)
    return [{"construct": u["construct"], "reason": u["detail"],
             "rule_id": u["rule_id"]} for u in result.unsupported]


