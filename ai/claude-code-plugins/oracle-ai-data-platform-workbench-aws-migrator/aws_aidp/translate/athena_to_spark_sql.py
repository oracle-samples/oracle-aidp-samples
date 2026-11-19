"""Translate Athena (Presto/Trino) SQL to Spark SQL.

Deterministic-first: each rule is a small regex transform with a clear reason.
Returns (translated_sql, findings, manual_review_flags).

Coverage is intentionally narrow but DEEP: the rules we ship are validated against
realistic insurance-domain queries. Anything we can't safely rewrite is FLAGGED
(left in place + reported), never silently translated.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from aws_aidp.translate.spark_builtins import (
    SPARK_BUILTINS,
    SPARK_KEYWORDS,
    SPARK_VERSION,
)
from typing import Iterable


@dataclass
class Finding:
    rule: str           # rule name
    detail: str
    severity: str       # "rewrite" (applied) | "flag" (manual review needed)

    def __str__(self) -> str:
        return f"[{self.severity}] {self.rule}: {self.detail}"


@dataclass
class TranslationResult:
    source_sql: str
    translated_sql: str
    findings: list[Finding] = field(default_factory=list)

    @property
    def changes(self) -> int:
        return sum(1 for f in self.findings if f.severity == "rewrite")

    @property
    def flags(self) -> int:
        return sum(1 for f in self.findings if f.severity == "flag")

    @property
    def needs_manual_review(self) -> bool:
        return self.flags > 0


def _masked_sql(sql: str) -> str:
    """Mask SQL comments and quoted strings while preserving character offsets."""
    chars = list(sql)
    i = 0
    while i < len(sql):
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            end = len(sql) if end < 0 else end
            chars[i:end] = " " * (end - i)
            i = end
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = len(sql) if end < 0 else end + 2
            for pos in range(i, end):
                if chars[pos] != "\n":
                    chars[pos] = " "
            i = end
            continue
        if sql[i] in ("'", '"', "`"):
            quote = sql[i]
            start = i
            i += 1
            while i < len(sql):
                if sql[i] == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                # Athena/Trino SQL escapes a quote by doubling it.  A
                # backslash is ordinary literal content and must not swallow
                # the closing quote while we mask strings.
                i += 1
            for pos in range(start, i):
                if chars[pos] != "\n":
                    chars[pos] = " "
            continue
        i += 1
    return "".join(chars)


def _is_code(mask: str, index: int) -> bool:
    return 0 <= index < len(mask) and mask[index] != " "


def _matching_paren(sql: str, open_index: int) -> int | None:
    mask = _masked_sql(sql)
    depth = 0
    for index in range(open_index, len(mask)):
        if mask[index] == "(":
            depth += 1
        elif mask[index] == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _function_calls(sql: str, name: str) -> Iterable[tuple[re.Match, int | None]]:
    """Yield code-level function starts and their matching closing parenthesis."""
    mask = _masked_sql(sql)
    pattern = re.compile(rf"\b{re.escape(name)}\s*\(", re.IGNORECASE)
    for match in pattern.finditer(mask):
        yield match, _matching_paren(sql, match.end() - 1)


def _top_level_argument_spans(
    sql: str, open_index: int, close_index: int,
) -> list[tuple[int, int]]:
    """Locate function arguments without splitting nested expressions."""
    mask = _masked_sql(sql)
    spans: list[tuple[int, int]] = []
    start = open_index + 1
    round_depth = 0
    square_depth = 0
    for index in range(start, close_index):
        char = mask[index]
        if char == "(":
            round_depth += 1
        elif char == ")":
            round_depth -= 1
        elif char == "[":
            square_depth += 1
        elif char == "]":
            square_depth -= 1
        elif char == "," and round_depth == 0 and square_depth == 0:
            left, right = start, index
            while left < right and sql[left].isspace():
                left += 1
            while right > left and sql[right - 1].isspace():
                right -= 1
            spans.append((left, right))
            start = index + 1
    left, right = start, close_index
    while left < right and sql[left].isspace():
        left += 1
    while right > left and sql[right - 1].isspace():
        right -= 1
    spans.append((left, right))
    return spans


def _split_top_level_arguments(sql: str, open_index: int, close_index: int) -> list[str]:
    return [sql[start:end] for start, end in _top_level_argument_spans(
        sql, open_index, close_index,
    )]


def _single_quoted_literal(value: str) -> str | None:
    """Return a SQL single-quoted literal's value, or None for dynamic input."""
    value = value.strip()
    if not re.fullmatch(r"'(?:''|[^'])*'", value, re.DOTALL):
        return None
    return value[1:-1].replace("''", "'")


def _apply_replacements(sql: str, replacements: list[tuple[int, int, str]]) -> str:
    """Apply non-overlapping source-offset replacements from right to left."""
    for start, end, replacement in sorted(
        replacements, key=lambda item: item[0], reverse=True,
    ):
        sql = sql[:start] + replacement + sql[end:]
    return sql


# ----- strftime (MySQL/Presto) → Java SimpleDateFormat token map -----

_STRFTIME_TO_JAVA = [
    ("%Y", "yyyy"),
    ("%y", "yy"),
    ("%m", "MM"),
    ("%c", "M"),
    ("%d", "dd"),
    ("%e", "d"),
    ("%H", "HH"),
    ("%k", "H"),
    ("%h", "hh"),
    ("%I", "hh"),
    ("%l", "h"),
    ("%i", "mm"),
    ("%s", "ss"),
    ("%S", "ss"),
    ("%p", "a"),
    ("%j", "DDD"),
    ("%a", "EEE"),
    ("%W", "EEEE"),
    ("%b", "MMM"),
    ("%M", "MMMM"),
    ("%f", "SSSSSS"),
    ("%T", "HH:mm:ss"),
    ("%r", "hh:mm:ss a"),
    ("%%", "%"),
]

_STRFTIME_MAP = dict(_STRFTIME_TO_JAVA)


def _translate_strftime(value: str) -> tuple[str, list[str]]:
    """Translate tokens atomically; never partially accept an unknown token."""
    out: list[str] = []
    unknown: list[str] = []
    index = 0
    while index < len(value):
        if value[index] != "%":
            if value[index].isalpha():
                end = index + 1
                while end < len(value) and value[end].isalpha():
                    end += 1
                # Athena treats bare letters as literals; Spark treats many as
                # datetime pattern symbols, so quote the entire literal run.
                out.append(f"'{value[index:end]}'")
                index = end
            else:
                if value[index] == "'":
                    unknown.append("literal apostrophe")
                out.append(value[index])
                index += 1
            continue
        token = value[index:index + 2]
        replacement = _STRFTIME_MAP.get(token)
        if replacement is None:
            unknown.append(token)
            out.append(token)
        else:
            out.append(replacement)
        index += len(token)
    return "".join(out), unknown


def _rule_date_format(sql: str, findings: list[Finding]) -> str:
    replacements: list[tuple[int, int, str]] = []
    for match, close in _function_calls(sql, "date_format"):
        if close is None:
            findings.append(Finding(
                rule="date_format_unhandled",
                detail="date_format call is unbalanced; left unchanged",
                severity="flag",
            ))
            continue
        spans = _top_level_argument_spans(sql, match.end() - 1, close)
        args = [sql[start:end] for start, end in spans]
        if len(args) != 2:
            findings.append(Finding(
                rule="date_format_unhandled",
                detail="date_format must have a timestamp and a static format; left unchanged",
                severity="flag",
            ))
            continue
        fmt = _single_quoted_literal(args[1])
        if fmt is None:
            findings.append(Finding(
                rule="date_format_unhandled",
                detail="dynamic or non-single-quoted date format cannot be translated safely",
                severity="flag",
            ))
            continue
        translated, unknown = _translate_strftime(fmt)
        if unknown:
            findings.append(Finding(
                rule="date_format_unhandled",
                detail=f"unsupported Athena strftime token(s): {', '.join(sorted(set(unknown)))}",
                severity="flag",
            ))
            continue
        if "%" not in fmt and re.search(r"[A-Za-z]", fmt):
            findings.append(Finding(
                rule="date_format_dialect",
                detail="format has Java-style letters but no Athena percent tokens; target intent is ambiguous",
                severity="flag",
            ))
            continue
        if translated != fmt:
            # Escape for Spark, not for Trino.  Doubling the quote produces
            # 'yyyy-MM-dd''T''HH', which Spark reads as three adjacent
            # literals and then rejects with "Unknown pattern letter: T".
            # Backslash escaping is what Spark's default parser expects
            # (verified on 3.5.9: 'yyyy-MM-dd\'T\'HH' -> 2024-01-03T10:20:30).
            escaped = translated.replace("\\", "\\\\").replace("'", "\\'")
            replacements.append((spans[1][0], spans[1][1], f"'{escaped}'"))
            findings.append(Finding(
                rule="date_format",
                detail=f"strftime '{fmt}' → java '{translated}'",
                severity="rewrite",
            ))
    return _apply_replacements(sql, replacements)


# ----- CROSS JOIN UNNEST(arr) AS t(x[, idx])  →  LATERAL VIEW explode(arr) t AS x -----

_CROSS_UNNEST = re.compile(r"\bCROSS\s+JOIN\s+UNNEST\s*\(", re.IGNORECASE)
_UNNEST_ANY = re.compile(r"\bUNNEST\s*\(", re.IGNORECASE)
_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"

# A relation that continues the FROM clause after an UNNEST. Spark's grammar puts
# `lateralView*` after the whole relation list, so a JOIN (or a comma join) that
# follows a LATERAL VIEW is a parse error, not just unusual style. Stop scanning at
# the first keyword that ends the FROM clause so a later JOIN in a different clause
# (e.g. inside a following subquery's FROM) does not suppress the rewrite.
_FROM_CLAUSE_END = re.compile(
    r"\b(?:WHERE|GROUP|HAVING|ORDER|WINDOW|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|QUALIFY)\b",
    re.IGNORECASE,
)
_TRAILING_RELATION = re.compile(
    r"\b(?:(?:INNER|CROSS|LEFT|RIGHT|FULL|NATURAL)\s+)*JOIN\b|,",
    re.IGNORECASE,
)
# Another UNNEST clause does not block the rewrite: consecutive LATERAL VIEWs are
# legal Spark. Verified on Spark 3.5.3 --
#   FROM t LATERAL VIEW explode(a) u AS x LATERAL VIEW explode(b) v AS y   -> runs
#   FROM t LATERAL VIEW explode(a) u AS x JOIN j ON j.id = u.x             -> PARSE_SYNTAX_ERROR
_UNNEST_RELATION = re.compile(
    r"(?:\bCROSS\s+JOIN\b|,)\s*UNNEST\s*\(", re.IGNORECASE)


def _blank_nested_parens(text: str) -> str:
    """Blank everything inside parentheses, so only top-level tokens remain.

    Without this, a comma belonging to a function call (`split(b, ',')`) reads as
    a comma join and suppresses an otherwise valid rewrite.
    """
    out = list(text)
    depth = 0
    for i, char in enumerate(text):
        if char == "(":
            depth += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            continue
        if depth > 0:
            out[i] = " "
    return "".join(out)


def _has_trailing_relation(mask: str, start: int) -> bool:
    """Whether a non-UNNEST JOIN/comma relation follows `start` in the FROM clause."""
    tail = mask[start:]
    end = _FROM_CLAUSE_END.search(tail)
    if end is not None:
        tail = tail[: end.start()]
    # Blank out UNNEST clauses so their own JOIN/comma keyword is not mistaken for a
    # table relation: consecutive LATERAL VIEWs are legal Spark. Then blank nested
    # parens so only top-level relation separators are considered.
    tail = _UNNEST_RELATION.sub(lambda m: " " * len(m.group(0)), tail)
    return _TRAILING_RELATION.search(_blank_nested_parens(tail)) is not None


def _rule_unnest(sql: str, findings: list[Finding]) -> str:
    """Rewrite only one-array/one-output CROSS JOIN UNNEST forms.

    Structural matching is deliberate: regex nesting previously stopped at one
    level, so valid expressions such as transform(filter(...), ...) either
    leaked through or were misclassified. Map and multi-array UNNEST require a
    different Spark projection and remain review items.
    """
    mask = _masked_sql(sql)
    replacements: list[tuple[int, int, str]] = []
    emitted_trailing_join = False
    for match in _CROSS_UNNEST.finditer(mask):
        close = _matching_paren(sql, match.end() - 1)
        if close is None:
            continue
        inputs = _split_top_level_arguments(sql, match.end() - 1, close)
        tail = mask[close + 1:]
        alias_match = re.match(
            rf"\s+AS\s+({_IDENTIFIER})\s*\(", tail, re.IGNORECASE,
        )
        if len(inputs) != 1 or not inputs[0] or alias_match is None:
            continue
        output_open = close + 1 + alias_match.end() - 1
        output_close = _matching_paren(sql, output_open)
        if output_close is None:
            continue
        outputs = _split_top_level_arguments(sql, output_open, output_close)
        if len(outputs) != 1 or not re.fullmatch(_IDENTIFIER, outputs[0]):
            continue
        alias = alias_match.group(1)
        column = outputs[0]
        expression = inputs[0]
        # A LATERAL VIEW cannot be followed by another relation in Spark, so the
        # otherwise-correct rewrite would emit SQL that fails to parse. Flag and
        # leave the Presto form in place instead of shipping invalid Spark.
        if _has_trailing_relation(mask, output_close + 1):
            emitted_trailing_join = True
            findings.append(Finding(
                rule="unnest_trailing_join",
                detail=(
                    f"CROSS JOIN UNNEST({expression}) is followed by another JOIN; "
                    "Spark requires LATERAL VIEW after the full relation list, so "
                    "reorder the joins before the explode manually"
                ),
                severity="flag",
            ))
            continue
        findings.append(Finding(
            rule="unnest",
            detail=f"CROSS JOIN UNNEST({expression}) → LATERAL VIEW explode({expression})",
            severity="rewrite",
        ))
        replacements.append((
            match.start(), output_close + 1,
            f"LATERAL VIEW explode({expression}) {alias} AS {column}",
        ))
    sql = _apply_replacements(sql, replacements)

    # Safety net covers every residual UNNEST, including LEFT JOIN, comma joins,
    # maps, multiple arrays, quoted aliases, missing aliases, and malformed calls.
    residual_mask = _masked_sql(sql)
    has_ordinality = False
    has_unhandled = False
    for match in _UNNEST_ANY.finditer(residual_mask):
        close = _matching_paren(sql, match.end() - 1)
        if close is not None and re.match(
            r"\s+WITH\s+ORDINALITY\b", residual_mask[close + 1:], re.IGNORECASE,
        ):
            has_ordinality = True
        elif (
            emitted_trailing_join
            and close is not None
            and _has_trailing_relation(residual_mask, close + 1)
        ):
            # Already reported as unnest_trailing_join with a specific remedy; one
            # construct should not produce two findings. Guarded on having actually
            # emitted that finding, so an UNNEST form this rule never recognised
            # (a map or multi-array form that also has a trailing join) still gets
            # the generic flag rather than silently losing it.
            continue
        else:
            has_unhandled = True
    if has_ordinality:
        findings.append(Finding(
            rule="unnest_with_ordinality",
            detail="UNNEST(...) WITH ORDINALITY requires Spark posexplode and index-base review; left as-is",
            severity="flag",
        ))
    if has_unhandled:
        findings.append(Finding(
            rule="unnest_unhandled",
            detail="UNNEST form is not a one-array/one-output CROSS JOIN; review explode/map/row semantics",
            severity="flag",
        ))
    return sql


# ----- validated function rewrites -----

_SIMPLE_RENAMES = [
    (
        "APPROX_DISTINCT", "approx_count_distinct", {1, 2},
        "Athena APPROX_DISTINCT → Spark approx_count_distinct",
    ),
    # Trino has no string contains(); its contains() is always array membership,
    # which is exactly Spark's array_contains().  Spark's contains() is a string
    # function, so leaving the name unchanged would silently change semantics.
    (
        "contains", "array_contains", {2},
        "Athena contains(array, x) → Spark array_contains(array, x)",
    ),
    (
        "levenshtein_distance", "levenshtein", {2},
        "Athena levenshtein_distance → Spark levenshtein",
    ),
]


def _rule_function_renames(sql: str, findings: list[Finding]) -> str:
    for athena_name, spark_name, allowed_arities, reason in _SIMPLE_RENAMES:
        replacements: list[tuple[int, int, str]] = []
        invalid = False
        for match, close in _function_calls(sql, athena_name):
            if close is None:
                invalid = True
                continue
            args = _split_top_level_arguments(sql, match.end() - 1, close)
            if len(args) not in allowed_arities or any(not arg for arg in args):
                invalid = True
                continue
            replacements.append((match.start(), match.end(), f"{spark_name}("))
        if replacements:
            findings.append(Finding(
                rule="fn_rename",
                detail=f"{reason} ({len(replacements)}x)",
                severity="rewrite",
            ))
            sql = _apply_replacements(sql, replacements)
        if invalid:
            findings.append(Finding(
                rule=f"{athena_name.lower()}_unhandled",
                detail=f"{athena_name} has malformed or unsupported arguments; left unchanged",
                severity="flag",
            ))
    return sql


def _top_level_order_by(argument: str) -> bool:
    mask = _masked_sql(argument)
    depth = 0
    for match in re.finditer(r"[()]|\bORDER\s+BY\b", mask, re.IGNORECASE):
        token = match.group(0)
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0:
            return True
    return False


def _rule_array_agg(sql: str, findings: list[Finding]) -> str:
    for match, close in _function_calls(sql, "array_agg"):
        if close is None:
            findings.append(Finding(
                rule="array_agg_unhandled",
                detail="unbalanced array_agg call; left unchanged",
                severity="flag",
            ))
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) != 1 or not args[0]:
            findings.append(Finding(
                rule="array_agg_unhandled",
                detail="array_agg must have exactly one expression; left unchanged",
                severity="flag",
            ))
            continue
        distinct = re.match(r"\s*DISTINCT\b", _masked_sql(args[0]), re.IGNORECASE)
        ordered = _top_level_order_by(args[0])
        if distinct:
            findings.append(Finding(
                rule="array_agg_distinct",
                detail="array_agg(DISTINCT ...) requires explicit collect_set/null/order semantics; left unchanged",
                severity="flag",
            ))
        if ordered:
            findings.append(Finding(
                rule="array_agg_ordered",
                detail="ordered array_agg requires an explicit Spark ordering strategy; left unchanged",
                severity="flag",
            ))
        if not distinct and not ordered:
            findings.append(Finding(
                rule="array_agg_semantics",
                detail="Spark 3.5 array_agg/collect_list drops null inputs while Athena retains them; review null policy",
                severity="flag",
            ))
    return sql


def _rule_cardinality(sql: str, findings: list[Finding]) -> str:
    """Keep Spark 3.5's native cardinality and expose its NULL configuration risk."""
    found_valid = False
    found_invalid = False
    for match, close in _function_calls(sql, "cardinality"):
        if close is None:
            found_invalid = True
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) != 1 or not args[0]:
            found_invalid = True
        else:
            found_valid = True
    if found_valid:
        findings.append(Finding(
            rule="cardinality_null_semantics",
            detail="Spark cardinality(NULL) depends on ANSI/legacy sizeOfNull settings; verify it returns Athena-compatible NULL",
            severity="flag",
        ))
    if found_invalid:
        findings.append(Finding(
            rule="cardinality_unhandled",
            detail="cardinality must have exactly one array or map expression; left unchanged",
            severity="flag",
        ))
    return sql


_SIMPLE_JSON_PATH = re.compile(
    r"\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\])*"
)


def _rule_json_extract_scalar(sql: str, findings: list[Finding]) -> str:
    replacements: list[tuple[int, int, str]] = []
    invalid = False
    for match, close in _function_calls(sql, "JSON_EXTRACT_SCALAR"):
        if close is None:
            invalid = True
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        path = _single_quoted_literal(args[1]) if len(args) == 2 else None
        if (
            len(args) != 2 or not args[0] or path is None
            or _SIMPLE_JSON_PATH.fullmatch(path) is None
        ):
            invalid = True
            continue
        replacements.append((match.start(), match.end(), "get_json_object("))
    if replacements:
        findings.append(Finding(
            rule="fn_rename",
            detail=f"Athena JSON_EXTRACT_SCALAR → Spark get_json_object ({len(replacements)}x)",
            severity="rewrite",
        ))
    if invalid:
        findings.append(Finding(
            rule="json_extract_scalar_unhandled",
            detail="JSON_EXTRACT_SCALAR needs two arguments and a simple static Spark-compatible JSONPath; left unchanged",
            severity="flag",
        ))
    return _apply_replacements(sql, replacements)


# ----- zip(a, b) → arrays_zip(a, b)  (NOT zip in Spark; arrays_zip is the equivalent) -----

def _rule_zip(sql: str, findings: list[Finding]) -> str:
    replacements: list[tuple[int, int, str]] = []
    invalid = False
    for match, close in _function_calls(sql, "zip"):
        if close is None:
            invalid = True
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) < 2 or any(not arg for arg in args):
            invalid = True
            continue
        replacements.append((match.start(), match.end(), "arrays_zip("))
    if replacements:
        findings.append(Finding(
            rule="zip",
            detail=f"Athena zip(...) → Spark arrays_zip(...) ({len(replacements)}x)",
            severity="rewrite",
        ))
    if invalid:
        findings.append(Finding(
            rule="zip_unhandled",
            detail="Athena zip needs at least two non-empty arrays; left unchanged",
            severity="flag",
        ))
    return _apply_replacements(sql, replacements)


# ----- Athena/Spark string-function semantic differences -----

_REGEX_METACHARACTERS = frozenset(r".^$*+?{}[]\|()")


def _rule_split_semantics(sql: str, findings: list[Finding]) -> str:
    """Expose cases where Spark's regex split is not Athena's literal split.

    Array subscripts are also dialect-sensitive: Athena arrays are one-based,
    while Spark's bracket operator is zero-based.  These are intentionally
    review flags rather than speculative rewrites because the desired index and
    regex quoting depend on query intent.
    """
    regex_delimiter = False
    dynamic_delimiter = False
    subscript = False
    for match, close in _function_calls(sql, "split"):
        if close is None:
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) in {2, 3}:
            delimiter = _single_quoted_literal(args[1])
            if delimiter is None:
                dynamic_delimiter = True
            elif any(character in _REGEX_METACHARACTERS for character in delimiter):
                regex_delimiter = True
        if re.match(r"\s*\[", _masked_sql(sql)[close + 1:]):
            subscript = True
    if regex_delimiter:
        findings.append(Finding(
            rule="split_regex_delimiter",
            detail="Athena split uses a literal delimiter but Spark split uses a regex pattern; left unchanged",
            severity="flag",
        ))
    if dynamic_delimiter:
        findings.append(Finding(
            rule="split_dynamic_delimiter",
            detail="Athena split treats a computed delimiter literally but Spark interprets it as a regex pattern; left unchanged",
            severity="flag",
        ))
    if subscript:
        findings.append(Finding(
            rule="split_subscript_index",
            detail="Athena array subscripts are one-based but Spark bracket subscripts are zero-based; left unchanged",
            severity="flag",
        ))
    return sql


def _rule_regexp_extract(sql: str, findings: list[Finding]) -> str:
    """Flag Athena's two-argument whole-match regexp_extract overload."""
    default_group = False
    invalid = False
    for match, close in _function_calls(sql, "regexp_extract"):
        if close is None:
            invalid = True
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) == 2 and all(args):
            default_group = True
        elif len(args) != 3 or any(not arg for arg in args):
            invalid = True
    if default_group:
        findings.append(Finding(
            rule="regexp_extract_default_group",
            detail="Athena's two-argument regexp_extract returns the whole match; Spark defaults to capture group 1",
            severity="flag",
        ))
    if invalid:
        findings.append(Finding(
            rule="regexp_extract_unhandled",
            detail="regexp_extract must have two or three non-empty arguments; left unchanged",
            severity="flag",
        ))
    return sql


def _rule_cast_varchar(sql: str, findings: list[Finding]) -> str:
    """Flag Athena VARCHAR casts, which are not Spark 3.5 expression types."""
    found = False
    for match, close in _function_calls(sql, "cast"):
        if close is None:
            continue
        parts = _top_level_as_parts(sql[match.end():close])
        if parts is not None and re.fullmatch(
            r"VARCHAR(?:\s*\(\s*[0-9]+\s*\))?", parts[1], re.IGNORECASE,
        ):
            found = True
    if found:
        findings.append(Finding(
            rule="cast_varchar",
            detail="Athena CAST(... AS VARCHAR) must be reviewed and changed to a Spark 3.5 string type",
            severity="flag",
        ))
    return sql


def _rule_strpos(sql: str, findings: list[Finding]) -> str:
    """Rewrite the shared two-argument, one-based strpos contract to instr."""
    replacements: list[tuple[int, int, str]] = []
    invalid = False
    for match, close in _function_calls(sql, "strpos"):
        if close is None:
            invalid = True
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) == 2 and all(args):
            replacements.append((match.start(), match.end(), "instr("))
        else:
            invalid = True
    if replacements:
        findings.append(Finding(
            rule="strpos",
            detail=f"Athena strpos(string, substring) → Spark instr(string, substring) ({len(replacements)}x)",
            severity="rewrite",
        ))
    if invalid:
        findings.append(Finding(
            rule="strpos_unhandled",
            detail="only two-argument strpos has a direct Spark instr equivalent; left unchanged",
            severity="flag",
        ))
    return _apply_replacements(sql, replacements)


# ----- regex literals with backslash escapes -----

# The pattern is always the second argument of these Athena regex functions.
# regexp_like is flagged unconditionally by _UNSUPPORTED_PATTERNS.
_REGEX_PATTERN_FUNCTIONS = (
    "regexp_replace",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_split",
    "regexp_count",
    "regexp_position",
)


def _rule_regex_escape_sequences(sql: str, findings: list[Finding]) -> str:
    """Flag regex literals whose backslashes Spark's default parser consumes.

    With ``spark.sql.parser.escapedStringLiterals=false`` (the default) the SQL
    parser turns ``'\\d+'`` into ``'d+'`` before the regex engine sees it, so an
    Athena pattern that relies on ``\\d``, ``\\w``, ``\\s`` or ``\\.`` changes meaning
    without any error.  Left unchanged: the correct fix (double escaping or the
    parser flag) is a deployment decision.
    """
    found = False
    for name in _REGEX_PATTERN_FUNCTIONS:
        for match, close in _function_calls(sql, name):
            if close is None:
                continue
            args = _split_top_level_arguments(sql, match.end() - 1, close)
            if len(args) >= 2 and "\\" in args[1]:
                found = True
    if found:
        findings.append(Finding(
            rule="regex_escape_sequence",
            detail=(
                "regex literal contains backslash escapes; Spark's default parser "
                "(spark.sql.parser.escapedStringLiterals=false) rewrites them before "
                "the regex engine sees the pattern"
            ),
            severity="flag",
        ))
    return sql


# ----- "identifier" → `identifier` -----


def _double_quoted_identifier_spans(sql: str) -> list[tuple[int, int, bool]]:
    """Locate Athena double-quoted identifiers outside literals and comments.

    Returns ``(start, end, closed)`` spans; ``end`` is exclusive.
    """
    spans: list[tuple[int, int, bool]] = []
    i = 0
    while i < len(sql):
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end < 0 else end
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            i = len(sql) if end < 0 else end + 2
            continue
        quote = sql[i]
        if quote not in ("'", '"', "`"):
            i += 1
            continue
        start = i
        i += 1
        closed = False
        while i < len(sql):
            if sql[i] == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    i += 2
                    continue
                i += 1
                closed = True
                break
            i += 1
        if quote == '"':
            spans.append((start, i, closed))
    return spans


def _rule_double_quoted_identifiers(sql: str, findings: list[Finding]) -> str:
    """Rewrite Trino ``"identifier"`` quoting to Spark's backtick quoting.

    In Athena/Trino a double-quoted token is always an identifier.  Spark's
    default parser (``spark.sql.ansi.doubleQuotedIdentifiers=false``) reads it as
    a string literal, so ``SELECT "id" FROM "db"."t"`` returns constant strings
    and fails at ``FROM``.  Backticks are identifiers in every Spark mode.
    """
    replacements: list[tuple[int, int, str]] = []
    invalid = False
    for start, end, closed in _double_quoted_identifier_spans(sql):
        content = sql[start + 1:end - 1].replace('""', '"') if closed else ""
        if not closed or not content:
            invalid = True
            continue
        replacements.append((start, end, "`" + content.replace("`", "``") + "`"))
    if replacements:
        findings.append(Finding(
            rule="double_quoted_identifier",
            detail=(
                "Athena double-quoted identifiers → Spark backtick identifiers "
                f"({len(replacements)}x); Spark would otherwise read them as string literals"
            ),
            severity="rewrite",
        ))
    if invalid:
        findings.append(Finding(
            rule="double_quoted_identifier_unhandled",
            detail="empty or unterminated double-quoted identifier; left unchanged",
            severity="flag",
        ))
    return _apply_replacements(sql, replacements)


# ----- histogram(...) — no Spark equivalent: FLAG -----

_HISTOGRAM = re.compile(r"\bhistogram\s*\(", re.IGNORECASE)


def _rule_histogram_flag(sql: str, findings: list[Finding]) -> str:
    if _HISTOGRAM.search(_masked_sql(sql)):
        findings.append(Finding(
            rule="histogram",
            detail="Athena histogram() has no direct Spark equivalent — replace with CASE buckets or width_bucket()",
            severity="flag",
        ))
    return sql


_UNSUPPORTED_PATTERNS = [
    (
        "json_extract",
        re.compile(r"\bJSON_EXTRACT\s*\(", re.IGNORECASE),
        "Athena JSON_EXTRACT returns JSON and is not always equivalent to Spark get_json_object; review result type",
    ),
    (
        "date_diff",
        re.compile(r"\bdate_diff\s*\(", re.IGNORECASE),
        "Athena three-argument date_diff requires a unit-specific Spark rewrite",
    ),
    (
        "athena_datetime",
        re.compile(
            r"\b(?:date_parse|parse_datetime|format_datetime|"
            r"from_iso8601_timestamp|to_iso8601|last_day_of_month)\s*\(",
            re.IGNORECASE,
        ),
        "Athena date/time function has target-specific parsing, timezone, or return-type semantics",
    ),
    (
        "map_agg",
        re.compile(r"\b(?:map_agg|multimap_agg)\s*\(", re.IGNORECASE),
        "Athena map aggregation requires a manual Spark rewrite",
    ),
    (
        "arbitrary",
        re.compile(r"\barbitrary\s*\(", re.IGNORECASE),
        "Athena arbitrary() requires a reviewed Spark aggregate equivalent",
    ),
    (
        "regexp_like",
        re.compile(r"\bregexp_like\s*\(", re.IGNORECASE),
        "regexp_like syntax exists in Spark 3.5, but regex/string escaping depends on Spark parser configuration",
    ),
    (
        "higher_order_lambda",
        re.compile(r"->"),
        "Athena higher-order lambda requires target Spark type/null/capture validation",
    ),
    (
        "from_unixtime",
        re.compile(r"\bfrom_unixtime\s*\(", re.IGNORECASE),
        "Athena from_unixtime returns TIMESTAMP but Spark returns a formatted STRING; use timestamp_seconds()",
    ),
    (
        "to_unixtime",
        re.compile(r"\bto_unixtime\s*\(", re.IGNORECASE),
        "Athena to_unixtime returns DOUBLE seconds; Spark unix_timestamp returns BIGINT and drops fractions",
    ),
    (
        "from_iso8601_date",
        re.compile(r"\bfrom_iso8601_date\s*\(", re.IGNORECASE),
        "Athena from_iso8601_date accepts ISO week and ordinal dates; review a to_date() replacement",
    ),
    (
        "try_expression",
        re.compile(r"\bTRY\s*\(", re.IGNORECASE),
        "Athena TRY(expr) has no Spark equivalent; use a try_* function or explicit NULL handling",
    ),
    (
        "url_extract",
        re.compile(r"\burl_extract_\w+\s*\(", re.IGNORECASE),
        "Athena url_extract_* maps to Spark parse_url with different NULL and type semantics; rewrite manually",
    ),
    (
        "json_parse",
        re.compile(r"\bjson_parse\s*\(", re.IGNORECASE),
        "Athena json_parse returns a JSON type Spark does not have; use from_json with an explicit schema",
    ),
    (
        "day_of_week",
        # Three spellings of one gap.  The EXTRACT form is standard SQL and was
        # the silent one: Spark accepts it and returns a different number
        # (measured 4 where Athena returns 3), so it never surfaced.
        re.compile(
            r"\b(?:day_of_week|dow)\s*\(|"
            r"\bEXTRACT\s*\(\s*(?:DOW|DAY_OF_WEEK)\s+FROM\b",
            re.IGNORECASE,
        ),
        "Athena day_of_week/dow/EXTRACT(DOW) is ISO (Monday=1) but Spark is "
        "Sunday=1; use weekday()+1",
    ),
    (
        "greatest_least_null_semantics",
        re.compile(r"\b(?:greatest|least)\s*\(", re.IGNORECASE),
        "Athena greatest/least return NULL if any argument is NULL; Spark skips "
        "NULLs (measured: greatest(1, NULL) is 1 on Spark, NULL on Athena)",
    ),
    (
        "array_constructor",
        # Spark has no ARRAY[...] constructor and no array(...)/map(...)/row(...)
        # type syntax; all of these are ParseExceptions on 3.5.9.
        re.compile(
            r"\bARRAY\s*\[|"
            r"\bAS\s+(?:ARRAY|MAP|ROW)\s*\(|"
            r"\b(?:array|map|row)\s*\(\s*(?:varchar|bigint|integer|double|boolean|"
            r"timestamp|date|real|smallint|tinyint|decimal|char)\b",
            re.IGNORECASE,
        ),
        "Presto collection syntax has no Spark equivalent; use array(...), "
        "map_from_arrays(...) and STRUCT<...> types",
    ),
]


def _rule_unsupported_constructs(sql: str, findings: list[Finding]) -> str:
    mask = _masked_sql(sql)
    for rule, pattern, detail in _UNSUPPORTED_PATTERNS:
        if pattern.search(mask):
            findings.append(Finding(rule=rule, detail=detail, severity="flag"))
    return sql


def _rule_date_add(sql: str, findings: list[Finding]) -> str:
    """Flag Athena's three-argument date_add but allow Spark's two-arg form."""
    invalid = False
    for match, close in _function_calls(sql, "date_add"):
        if close is None:
            invalid = True
            continue
        args = _split_top_level_arguments(sql, match.end() - 1, close)
        if len(args) != 2 or any(not arg for arg in args):
            invalid = True
    if invalid:
        findings.append(Finding(
            rule="date_add",
            detail="Athena date_add(unit, value, timestamp) is not Spark date_add(date, days)",
            severity="flag",
        ))
    return sql


def _rule_complex_interval(sql: str, findings: list[Finding]) -> str:
    """Flag interval shapes outside the shared single-unit literal subset."""
    mask = _masked_sql(sql)
    complex_interval = re.compile(
        r"\bINTERVAL\b[^,;\n)]{0,120}(?:"
        r"\b(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)\s+TO\s+"
        r"(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)\b|[*/])",
        re.IGNORECASE,
    )
    if complex_interval.search(mask):
        findings.append(Finding(
            rule="complex_interval",
            detail="multi-unit or scaled Athena interval needs explicit target Spark validation",
            severity="flag",
        ))
    return sql


def _top_level_as_parts(body: str) -> tuple[str, str] | None:
    mask = _masked_sql(body)
    depth = 0
    positions: list[tuple[int, int]] = []
    for match in re.finditer(r"[()]|\bAS\b", mask, re.IGNORECASE):
        token = match.group(0)
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0:
            positions.append(match.span())
    if len(positions) != 1:
        return None
    expression = body[:positions[0][0]].strip()
    target_type = body[positions[0][1]:].strip()
    if not expression or not target_type:
        return None
    return expression, target_type


_SAFE_TRY_CAST_TYPE = re.compile(
    r"(?:TINYINT|SMALLINT|INT|INTEGER|BIGINT|REAL|FLOAT|DOUBLE|BOOLEAN|STRING|"
    r"DECIMAL\s*\(\s*[0-9]+\s*,\s*[0-9]+\s*\))",
    re.IGNORECASE,
)


def _rule_target_spark_compatibility(sql: str, findings: list[Finding]) -> str:
    """Validate constructs intentionally preserved for the pinned Spark 3.5 target.

    Spark 3.5 accepts primitive TRY_CAST and MAX_BY/MIN_BY with Athena-compatible
    surface syntax. Valid calls therefore remain unchanged; dialect-specific cast
    types and malformed or wrong-arity calls must not escape as zero-flag output.
    """
    invalid_rules: set[str] = set()
    for match, close in _function_calls(sql, "try_cast"):
        parts = _top_level_as_parts(
            sql[match.end():close] if close is not None else "",
        )
        if parts is None or _SAFE_TRY_CAST_TYPE.fullmatch(parts[1]) is None:
            invalid_rules.add("try_cast_unhandled")
    for name, arity in (("max_by", 2), ("min_by", 2)):
        for match, close in _function_calls(sql, name):
            if close is None:
                invalid_rules.add(f"{name}_unhandled")
                continue
            args = _split_top_level_arguments(sql, match.end() - 1, close)
            if len(args) != arity or any(not arg for arg in args):
                invalid_rules.add(f"{name}_unhandled")
    for rule in sorted(invalid_rules):
        findings.append(Finding(
            rule=rule,
            detail=f"{rule.removesuffix('_unhandled')} call is malformed for the pinned Spark target",
            severity="flag",
        ))
    return sql


# ----- ordered rule list -----

# ----- subscript base: Athena arrays are 1-based, Spark's `[]` is 0-based -----

# `element_at` is 1-based for arrays AND looks up by key for maps, so it matches
# Athena semantics for every subscript type Athena allows. Verified on Spark 3.5.3:
#   array('a','b','c')[1]            -> 'b'   (Athena: 'a')   <- the off-by-one
#   element_at(array('a','b','c'),1) -> 'a'   (Athena: 'a')
#   map(1,'x')[1] / element_at(map(1,'x'),1) -> 'x' both      <- maps unaffected
_SUBSCRIPT_INTEGER = re.compile(r"\A\s*\d+\s*\Z")
_SUBSCRIPT_STRING = re.compile(r"\A\s*'(?:[^']|'')*'\s*\Z")
_BASE_TAIL = re.compile(r"(?:[A-Za-z_][A-Za-z0-9_]*|`[^`]*`)(?:\s*\.\s*(?:[A-Za-z_][A-Za-z0-9_]*|`[^`]*`))*\s*\Z")


def _matching_bracket(mask: str, open_index: int) -> int | None:
    """Index of the `]` closing the `[` at `open_index`, or None if unbalanced."""
    depth = 0
    for i in range(open_index, len(mask)):
        if mask[i] == "[":
            depth += 1
        elif mask[i] == "]":
            depth -= 1
            if depth == 0:
                return i
    return None


def _subscript_base_start(sql: str, mask: str, open_index: int) -> int | None:
    """Start offset of the expression a `[` subscripts, or None if not a subscript."""
    end = open_index
    while end > 0 and mask[end - 1].isspace():
        end -= 1
    if end == 0:
        return None
    if mask[end - 1] == ")":
        depth = 0
        i = end - 1
        while i >= 0:
            if mask[i] == ")":
                depth += 1
            elif mask[i] == "(":
                depth -= 1
                if depth == 0:
                    break
            i -= 1
        if i < 0:
            return None
        head = _BASE_TAIL.search(mask[:i])
        return head.start() if head is not None else i
    head = _BASE_TAIL.search(mask[:end])
    if head is None:
        return None
    return head.start()


def _rule_array_subscript(sql: str, findings: list[Finding]) -> str:
    """Rewrite Athena 1-based subscripts to Spark `element_at`.

    Athena/Presto arrays are 1-based and reject index 0; Spark's `[]` operator is
    0-based. The two run identically and return a DIFFERENT element, so leaving the
    subscript alone is silent data corruption rather than a visible failure.
    """
    mask = _masked_sql(sql)
    replacements: list[tuple[int, int, str]] = []
    flagged_dynamic = False
    for match in re.finditer(r"\[", mask):
        open_index = match.start()
        close = _matching_bracket(mask, open_index)
        if close is None:
            continue
        start = _subscript_base_start(sql, mask, open_index)
        if start is None:
            continue
        base = sql[start:open_index].strip()
        if not base:
            continue
        # `ARRAY[1,2]` is a Presto constructor, not a subscript; the unsupported
        # construct rule already reports it.
        if re.fullmatch(r"(?i:array|map|row)", base.split(".")[-1].strip("` ")):
            continue
        # A subscripted split(...) is already owned by `split_subscript_index`,
        # which carries the same index-base warning. One construct, one finding.
        if re.match(r"(?i:split)\s*\(", base):
            continue
        subscript = sql[open_index + 1:close]
        if _SUBSCRIPT_INTEGER.match(subscript):
            # Flag rather than rewrite, matching the existing `split_subscript_index`
            # decision for split(...)[n]. element_at(base, n) is a provably safe
            # auto-rewrite (see the module note above), but flipping this rule from
            # flag to rewrite is a translator-wide policy call, not a bug fix.
            replacements.append((start, close + 1, f"element_at({base}, {subscript.strip()})"))
        elif not _SUBSCRIPT_STRING.match(subscript):
            # A non-literal index cannot be checked statically, and the 1-based vs
            # 0-based difference silently changes the result.
            flagged_dynamic = True
    if replacements:
        suggestion = ", ".join(text for _start, _end, text in replacements[:3])
        findings.append(Finding(
            rule="array_subscript_index",
            detail=(
                f"Athena array subscripts are one-based but Spark bracket subscripts "
                f"are zero-based; left unchanged ({len(replacements)}x). If the base is "
                f"an array or a map, element_at is the 1-based equivalent: {suggestion}"
            ),
            severity="flag",
        ))
    if flagged_dynamic:
        findings.append(Finding(
            rule="array_subscript_dynamic",
            detail=(
                "non-literal subscript index left unchanged; Athena arrays are 1-based "
                "and Spark's [] is 0-based, so wrap the base in element_at(...) if it is "
                "an array rather than a map"
            ),
            severity="flag",
        ))
    return sql


_RULES = [
    _rule_date_format,
    _rule_unnest,
    _rule_array_subscript,
    _rule_array_agg,
    _rule_cardinality,
    _rule_function_renames,
    _rule_json_extract_scalar,
    _rule_zip,
    _rule_split_semantics,
    _rule_regexp_extract,
    _rule_cast_varchar,
    _rule_strpos,
    _rule_regex_escape_sequences,
    _rule_double_quoted_identifiers,
    _rule_histogram_flag,
    _rule_date_add,
    _rule_complex_interval,
    _rule_target_spark_compatibility,
    _rule_unsupported_constructs,
]


# ----- Athena string literals mean something else in Spark -----------------

_SINGLE_QUOTED = re.compile(r"'(?:[^']|'')*'")


def _rule_string_literal_escaping(sql: str, findings: list[Finding]) -> str:
    """Flag literals whose text changes meaning under Spark's parser.

    Athena escapes a quote by doubling it and treats a backslash as ordinary
    text.  Spark does neither: it reads ``'it''s'`` as two adjacent literals
    and concatenates them to ``its``, and it expands ``\\n`` to a newline.
    Both are silent -- the query runs and returns different data.  Measured on
    3.5.9: ``SELECT 'it''s'`` returns ``its``.

    Flagged rather than rewritten.  The correct Spark spelling is ``'it\\'s'``,
    but every other rule in this module locates literals with the doubled-quote
    convention, so rewriting here would change the text those rules are still
    scanning.  Centralising literal handling first is the prerequisite for a
    safe rewrite.
    """
    # Literals inside a regex call are the escape hazard that
    # _rule_regex_escape_sequences and _rule_split_semantics already report.
    # One construct must produce one finding, so skip them here.
    regex_spans: list[tuple[int, int]] = []
    for name in ("regexp_replace", "regexp_extract", "regexp_extract_all",
                 "regexp_like", "regexp_split", "split"):
        for call, close in _function_calls(sql, name):
            if close is not None:
                regex_spans.append((call.start(), close))

    def inside_regex_call(position: int) -> bool:
        return any(start <= position <= end for start, end in regex_spans)

    doubled = 0
    backslash = 0
    for match in _SINGLE_QUOTED.finditer(sql):
        body = match.group(0)[1:-1]
        if "''" in body:
            doubled += 1
        if "\\" in body and not inside_regex_call(match.start()):
            backslash += 1
    if doubled:
        findings.append(Finding(
            rule="string_literal_escaping",
            detail=(f"{doubled} string literal(s) escape a quote by doubling it; "
                    "Spark concatenates instead and silently drops the quote "
                    r"-- use \' escaping"),
            severity="flag",
        ))
    if backslash:
        findings.append(Finding(
            rule="string_literal_escaping",
            detail=(f"{backslash} string literal(s) contain a backslash; Athena "
                    "keeps it literally, Spark expands it as an escape sequence"),
            severity="flag",
        ))
    return sql


# ----- integer division truncates on Athena, not on Spark ------------------

_INTEGER_LITERAL = re.compile(r"(?<![\w.])\d+(?![\w.])")
# Aggregates whose result type is integral on both engines.
_INTEGER_FUNCTIONS = r"count|cardinality|size|length|char_length|instr|position|year|month|day|hour|minute|second"
_INTEGER_CALL_END = re.compile(rf"\b(?:{_INTEGER_FUNCTIONS})\s*\([^()]*\)\s*$", re.IGNORECASE)
_INTEGER_CALL_START = re.compile(rf"^\s*(?:{_INTEGER_FUNCTIONS})\s*\(", re.IGNORECASE)


def _rule_integer_division(sql: str, findings: list[Finding]) -> str:
    """Flag a division whose operands are provably integers on both engines.

    Athena truncates (``7/2`` is 3); Spark returns a double (3.5).  Nothing
    fails, so the drift is invisible -- and a ratio of counts is routine
    analytics.

    Deliberately conservative: a bare ``a/b`` is left alone because operand
    types need a catalog this tool does not have at translation time, and
    flagging every ``/`` would bury the review queue.
    """
    mask = _masked_sql(sql)
    hits = 0
    for index, char in enumerate(mask):
        if char != "/" or mask[index - 1:index] == "/" or mask[index + 1:index + 2] == "/":
            continue
        left = mask[:index].rstrip()
        right = mask[index + 1:].lstrip()
        left_int = bool(_INTEGER_LITERAL.search(left[-20:]) and
                        re.search(r"\d\s*$", left)) or bool(_INTEGER_CALL_END.search(left))
        right_int = bool(re.match(r"\d+(?![\w.])", right)) or bool(_INTEGER_CALL_START.match(right))
        if left_int and right_int:
            hits += 1
    if hits:
        findings.append(Finding(
            rule="integer_division",
            detail=(f"{hits} division(s) with integer operands; Athena truncates "
                    "to an integer, Spark returns a double (7/2 is 3 on Athena, "
                    "3.5 on Spark) -- cast or use div() to keep the old result"),
            severity="flag",
        ))
    return sql


# Registered out of line because both are defined after the table above.
# The literal rule must run FIRST, while the literals are still Athena's: once
# _rule_date_format has emitted its own \' escaping, a later pass would read
# that backslash as source text and flag it.  Neither rule rewrites anything.
_RULES.insert(0, _rule_string_literal_escaping)
_RULES.append(_rule_integer_division)


# ---------------------------------------------------------------------------
# Output validation gates
#
# The rules above only speak up about constructs they were written for, so
# "no rule matched" is not the same claim as "this is runnable Spark SQL".
# Without the gates below the translator reports unrecognised input as a clean
# PASS -- an earlier sqlglot-based version got input validation for free
# because it had to parse the query to translate it at all.
#
# These mirror the three checks glue_to_spark.translate() already performs
# (input shape, residual AWS paths, output validity).  They only ever append
# flags; no rewrite is applied and no SQL is altered here.
# ---------------------------------------------------------------------------

# Leading keywords that begin a statement we are willing to hand to Spark.
# Athena-only statement verbs are listed too, so gate C can give them a
# specific message instead of the generic "unrecognised" one.
_STATEMENT_KEYWORDS = frozenset({
    "select", "with", "insert", "update", "delete", "merge", "values", "table",
    "create", "alter", "drop", "truncate", "show", "describe", "desc", "explain",
    "msck", "repair", "refresh", "analyze", "cache", "uncache", "set", "reset",
    "use", "grant", "revoke", "call", "comment", "add", "list",
    # Athena / Trino statement verbs -- recognised, then flagged by gate C.
    "prepare", "execute", "deallocate", "unload",
})

# Constructs Spark's parser rejects outright.  Every entry was executed
# against Spark 3.5.9 and observed to raise PARSE_SYNTAX_ERROR or equivalent.
_PRESTO_ONLY_SYNTAX: tuple[tuple[str, "re.Pattern[str]", str], ...] = (
    ("WITH RECURSIVE", re.compile(r"\bWITH\s+RECURSIVE\b", re.IGNORECASE),
     "Spark 3.5 has no recursive CTE"),
    ("FETCH FIRST/NEXT", re.compile(r"\bFETCH\s+(?:FIRST|NEXT)\b", re.IGNORECASE),
     "use LIMIT; Spark has no FETCH clause and no WITH TIES"),
    ("AT TIME ZONE", re.compile(r"\bAT\s+TIME\s+ZONE\b", re.IGNORECASE),
     "use from_utc_timestamp/to_utc_timestamp"),
    ("TABLESAMPLE BERNOULLI/SYSTEM",
     re.compile(r"\bTABLESAMPLE\s+(?:BERNOULLI|SYSTEM|POISSONIZED)\b", re.IGNORECASE),
     "Spark supports TABLESAMPLE (n PERCENT | n ROWS) only"),
    ("quantified comparison",
     re.compile(r"(?:[<>]=?|<>|!=|=)\s*(?:ANY|SOME|ALL)\s*\(", re.IGNORECASE),
     "Spark has no > ANY/ALL (subquery); rewrite as a join or aggregate"),
    ("CAST AS JSON", re.compile(r"\bAS\s+JSON\s*\)", re.IGNORECASE),
     "Spark has no JSON data type"),
    ("CAST AS ROW", re.compile(r"\bAS\s+ROW\s*\(", re.IGNORECASE),
     "use Spark's STRUCT<...> type"),
    ("PREPARE/EXECUTE/DEALLOCATE",
     re.compile(r"^\s*(?:PREPARE|EXECUTE|DEALLOCATE)\b", re.IGNORECASE),
     "Athena prepared statements have no Spark equivalent"),
    ("UNLOAD", re.compile(r"^\s*UNLOAD\b", re.IGNORECASE),
     "use a Spark write instead"),
    ("CTAS WITH (properties)",
     re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?TABLE\b[^;]*?\bWITH\s*\(",
                re.IGNORECASE),
     "use Spark's USING/TBLPROPERTIES/LOCATION clauses"),
)

_CALL_CANDIDATE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
# `WITH name AS (` / `, name(cols) AS (` -- common-table-expression names.
_CTE_NAME = re.compile(
    r"(?:\bWITH\b|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\([^()]*\))?\s+AS\s*\(",
    re.IGNORECASE,
)
_S3_SCHEME = re.compile(r"\bs3[an]?://", re.IGNORECASE)

# `CREATE TABLE foo (a INT)` / `INSERT INTO db.target (id) ...` -- the
# parenthesis after a table or view name opens a column list, not an argument
# list, so the identifier in front of it is not a function call.
_IDENT = r"(?:[A-Za-z_][A-Za-z0-9_]*|`[^`]*`)"
_TABLE_NAME_PARENS = re.compile(
    rf"\b(?:TABLE|VIEW|INTO)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"{_IDENT}(?:\s*\.\s*{_IDENT})*\s*\(",
    re.IGNORECASE,
)

# Valid Spark SQL, but grammar productions rather than registry entries, so
# they never appear in SHOW FUNCTIONS.
_SPARK_SYNTAX_FUNCTIONS = frozenset({"try_cast"})

# Presto functions whose names collide with a Spark keyword.  Spark has no
# `truncate(x, n)` scalar and no `ROW(...)` constructor, so a call spelled that
# way must still be flagged even though the bare word is reserved.  Kept
# explicit and tested; the set is bounded by the overlap between Presto's
# function names and Spark's 326 keywords, which is small.
_KEYWORD_NAMED_FUNCTIONS = frozenset({"truncate", "row"})

# Words that may legally precede "(" without being a call -- `GROUPING SETS (…)`,
# `CASE … THEN (…) ELSE (…)`, `CUBE (…)`, `TBLPROPERTIES (…)`.  Sourced from
# Spark's own lexer vocabulary rather than hand-maintained: guessing this list
# is how `sets`, `then` and `else` shipped as false "unknown function" flags.
# `recursive` and `returns` are absent from the Spark 3.5 vocabulary but appear
# in Athena input, so they are added here.
_NON_FUNCTION_KEYWORDS = (
    (SPARK_KEYWORDS | {"recursive", "returns"}) - _KEYWORD_NAMED_FUNCTIONS
)

# A statement cannot end on a binary operator or conjunction.  Unlike clause
# adjacency this is decidable without a parser: Spark 3.5 reserves none of its
# clause keywords by default (`SELECT * FROM t WHERE group = 1` is valid, with
# `group` an ordinary column), so keyword position proves nothing -- but no
# valid statement ends on a dangling operator.
_DANGLING_TAIL = frozenset({
    "=", "==", "!=", "<>", "<", ">", "<=", ">=", "+", "-", "*", "/", "%",
    "||", "&", "|", "^", "~",
    "and", "or", "not", "like", "rlike", "ilike", "in", "is", "between",
    "div", "mod",
})
_BY_CLAUSES = frozenset({"order", "group", "sort", "cluster", "distribute", "partition"})
_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|<=>|<>|!=|<=|>=|==|\|\||\S")


def _statement_segments(sql: str, mask: str) -> list[str]:
    """Split on semicolons that sit in code, not in a literal or comment."""
    segments: list[str] = []
    start = 0
    for index, char in enumerate(mask):
        if char == ";":
            if mask[start:index].strip():
                segments.append(sql[start:index])
            start = index + 1
    if mask[start:].strip():
        segments.append(sql[start:])
    return segments


def _has_unterminated_quote(sql: str) -> bool:
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            index = len(sql) if end < 0 else end
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            if end < 0:
                return False        # unterminated comment; not this gate's job
            index = end + 2
            continue
        char = sql[index]
        if char in ("'", '"', "`"):
            index += 1
            closed = False
            while index < len(sql):
                if sql[index] == char:
                    if index + 1 < len(sql) and sql[index + 1] == char:
                        index += 2
                        continue
                    index += 1
                    closed = True
                    break
                index += 1
            if not closed:
                return True
            continue
        index += 1
    return False


def _comment_free(sql: str) -> str:
    """Blank comments but keep string literals: an s3:// path lives in one."""
    chars = list(sql)
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            end = len(sql) if end < 0 else end
        elif sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            end = len(sql) if end < 0 else end + 2
        else:
            index += 1
            continue
        for pos in range(index, end):
            if chars[pos] != "\n":
                chars[pos] = " "
        index = end
    return "".join(chars)


def _paren_imbalance(mask: str) -> str | None:
    """Describe a parenthesis problem, checking order as well as totals.

    Counting alone is not enough: ``SELECT )(`` has one of each but Spark
    rejects it, so depth must never go negative either.
    """
    depth = 0
    for index, char in enumerate(mask):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                return (f"a closing parenthesis at offset {index} has no matching "
                        "opening parenthesis")
    if depth > 0:
        noun = "parenthesis is" if depth == 1 else "parentheses are"
        return f"{depth} {noun} never closed"
    return None


def _clause_mask(sql: str) -> str:
    """Mask comments away but collapse quoted spans to a placeholder word.

    _masked_sql blanks literals to spaces, which is right for the rewrite
    rules but wrong here: the clause check counts tokens, so a blanked
    literal would make ``SELECT 'x' FROM t`` look like an empty projection.
    """
    chars = list(sql)
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            end = len(sql) if end < 0 else end
            for pos in range(index, end):
                chars[pos] = " "
            index = end
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            end = len(sql) if end < 0 else end + 2
            for pos in range(index, end):
                if chars[pos] != "\n":
                    chars[pos] = " "
            index = end
            continue
        if sql[index] in ("'", '"', "`"):
            quote = sql[index]
            start = index
            index += 1
            while index < len(sql):
                if sql[index] == quote:
                    if index + 1 < len(sql) and sql[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            for pos in range(start, index):
                if chars[pos] != "\n":
                    chars[pos] = "a"
            continue
        index += 1
    return "".join(chars)


def _dangling_tail(mask: str) -> str | None:
    """Flag a statement that ends on an operator, so its expression is cut off.

    Deliberately narrow.  An earlier version of this check looked at clause
    adjacency ("does a clause keyword follow SELECT?") and had to be removed:
    Spark 3.5's default parser reserves none of its clause keywords, so
    ``SELECT * FROM t WHERE group = 1`` is a valid query over a column named
    ``group``.  Keyword position therefore proves nothing, and every
    adjacency rule produced false REVIEWs on valid SQL.  A trailing operator
    is the part that stays decidable.
    """
    tokens = _TOKEN.findall(mask)
    if not tokens:
        return None
    last = tokens[-1].lower()
    if last in _DANGLING_TAIL:
        return f"the statement ends on {tokens[-1]!r}, so its expression is incomplete"
    if last == ",":
        return "the statement ends on a comma, so the list is unfinished"
    # A two-word BY clause is unambiguous.  A *lone* trailing keyword is not:
    # Spark rereads `SELECT a FROM` as `SELECT a AS from` and `... t WHERE` as
    # `... t AS where`, both of which parse, so only the paired form is safe
    # to flag.
    if last == "by" and len(tokens) >= 2 and tokens[-2].lower() in _BY_CLAUSES:
        return (f"the statement ends on {tokens[-2].upper()} BY "
                "with nothing to sort or group by")
    return None


def _leading_keyword(mask: str) -> str | None:
    # A set operation may parenthesise its operands --
    # `(SELECT …) UNION ALL (SELECT …)` is valid -- so skip any opening
    # parentheses before looking for the verb.
    stripped = mask.lstrip().lstrip("(").lstrip()
    while stripped.startswith("("):
        stripped = stripped[1:].lstrip()
    if not stripped:
        return None
    match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", stripped)
    return match.group(0).lower() if match else None


def _already_reported(name: str, findings: list[Finding]) -> bool:
    """Has a specific rule already spoken for this function?

    The targeted rules name the Spark equivalent ("use weekday()+1"), so they
    are strictly more useful than the generic gate.  One construct must
    produce one finding, so the gate stands down where a rule already fired.
    """
    for finding in findings:
        rule = finding.rule
        if name == rule or rule.startswith(f"{name}_") or name.startswith(f"{rule}_"):
            return True
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])",
                     finding.detail, re.IGNORECASE):
            return True
    return False


def _unknown_functions(mask: str, findings: list[Finding] | None = None) -> list[str]:
    cte_names = {m.group(1).lower() for m in _CTE_NAME.finditer(mask)}
    # Offsets of "(" that open a column list rather than an argument list.
    column_list_parens = {m.end() - 1 for m in _TABLE_NAME_PARENS.finditer(mask)}
    unknown: list[str] = []
    for match in _CALL_CANDIDATE.finditer(mask):
        name = match.group(1).lower()
        if match.end() - 1 in column_list_parens:
            continue
        if name in SPARK_BUILTINS or name in _SPARK_SYNTAX_FUNCTIONS:
            continue
        if name in _NON_FUNCTION_KEYWORDS or name in cte_names:
            continue
        # `... AS t(signal)` is a table alias with a column list, not a call.
        prefix = mask[:match.start(1)].rstrip()
        if re.search(r"(?:^|[^A-Za-z0-9_])AS$", prefix, re.IGNORECASE):
            continue
        # `AS` is optional: `FROM (VALUES (1)) t(x)` and `UNNEST(arr) u(tag)`
        # are aliases too.  A call can never directly follow a closing paren --
        # `f(x) g(y)` is not valid SQL -- so a ")" here means an alias.
        if prefix.endswith(")"):
            continue
        if findings is not None and _already_reported(name, findings):
            continue
        if name not in unknown:
            unknown.append(name)
    return unknown


def _validate(sql: str, findings: list[Finding]) -> None:
    """Inspect the translated SQL and flag anything Spark could not run."""
    mask = _masked_sql(sql)

    # Gate A -- Spark executes one statement per call.
    segments = _statement_segments(sql, mask)
    if len(segments) > 1:
        findings.append(Finding(
            "multi_statement",
            f"input holds {len(segments)} statements; Spark executes one statement "
            "per call -- split these into separate saved queries",
            "flag",
        ))

    # Gate B -- does this look like a SQL statement at all?
    keyword = _leading_keyword(mask)
    if keyword is None or keyword not in _STATEMENT_KEYWORDS:
        shown = keyword if keyword else (sql.strip()[:20] or "empty input")
        findings.append(Finding(
            "statement_not_recognized",
            f"does not begin with a recognised SQL statement keyword (found {shown!r}); "
            "Spark's parser would reject this",
            "flag",
        ))
    if _has_unterminated_quote(sql):
        findings.append(Finding(
            "statement_unbalanced",
            "a quoted literal or identifier is never closed; Spark's parser would reject this",
            "flag",
        ))
    else:
        imbalance = _paren_imbalance(mask)
        if imbalance:
            findings.append(Finding(
                "statement_unbalanced",
                f"{imbalance}; Spark's parser would reject this",
                "flag",
            ))
        dangling = _dangling_tail(_clause_mask(sql))
        if dangling:
            findings.append(Finding(
                "statement_incomplete",
                f"{dangling}; Spark's parser would reject this",
                "flag",
            ))

    # Gate C -- grammar Spark rejects outright.
    for label, pattern, hint in _PRESTO_ONLY_SYNTAX:
        if pattern.search(mask):
            findings.append(Finding(
                "presto_only_syntax",
                f"{label} is Athena/Trino-only and Spark 3.5 cannot parse it -- {hint}",
                "flag",
            ))

    # Gate D -- the function has to exist in Spark.
    for name in _unknown_functions(mask, findings):
        findings.append(Finding(
            "unknown_function",
            f"{name!r} is not a Spark {SPARK_VERSION} built-in and no equivalent was "
            "applied; the job would fail with UNRESOLVED_ROUTINE",
            "flag",
        ))

    # Gate E -- a migrated query must not still point at AWS.
    residual = _S3_SCHEME.findall(_comment_free(sql))
    if residual:
        first = _S3_SCHEME.search(_comment_free(sql))
        context = _comment_free(sql)[first.start():first.start() + 60].strip()
        findings.append(Finding(
            "s3_path_unhandled",
            f"{len(residual)} AWS object-storage path(s) remain after translation "
            f"(first: {context!r}); point these at OCI Object Storage",
            "flag",
        ))


def translate(sql: str) -> TranslationResult:
    findings: list[Finding] = []
    out = sql
    for rule in _RULES:
        out = rule(out, findings)
    _validate(out, findings)
    return TranslationResult(source_sql=sql, translated_sql=out, findings=findings)
