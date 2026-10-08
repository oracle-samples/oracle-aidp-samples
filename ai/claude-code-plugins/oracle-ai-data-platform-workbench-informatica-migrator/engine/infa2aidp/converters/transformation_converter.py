"""Convert Informatica transformation objects to PySpark code lines.

Each public method accepts a ``Transformation`` (and optional context) and
returns a list of PySpark code strings that can be assembled into a notebook
cell or script.
"""

from __future__ import annotations

import re
from typing import Optional

from ..models import (
    DataFlowDirection,
    Transformation,
    TransformationField,
    TransformationType,
)
from ..properties import get_ci
from .expression_converter import ExpressionConverter, UnconvertibleExpression

# Aggregate functions whose Informatica meaning is defined by ROW ARRIVAL
# ORDER. Spark is unordered and partitioned, so each returns an arbitrary
# value unless the generated code supplies an explicit Window.orderBy --
# which the export cannot tell us, because the ordering column is a
# property of the data rather than of the mapping.
# Functions whose Spark equivalent is right for common input and wrong for
# some of it. Unlike an unconvertible construct there is nothing better to
# emit -- Spark has no primitive with the same rule -- and unlike a clean
# mapping the result cannot be trusted without knowing the data. So the
# conversion stands and the caller is told which rows would differ.
#
# Refusing these outright would be worse: INITCAP is correct for every
# name without punctuation, and most are.
_DIVERGENT_EXPR_FN: dict[str, str] = {
    "SETMAXVARIABLE": (
        "The row value is the running maximum in the order rows are read; the "
        "variable's end-of-session value is NOT persisted for the next run. "
        "Carry the high-water mark forward yourself (e.g. a control table)"
    ),
    "SETMINVARIABLE": (
        "The row value is the running minimum in the order rows are read; the "
        "variable's end-of-session value is NOT persisted for the next run"
    ),
    "SETVARIABLE": (
        "The row value passes through; the variable's end-of-session value is "
        "NOT persisted for the next run"
    ),
    "SOUNDEX": (
        "Both implement Soundex, but edge cases differ (leading "
        "non-alpha, H/W separators, strings under 4 characters). If these "
        "codes are STORED or used as join keys, recompute both sides "
        "rather than comparing old codes with new"
    ),
    "TO_DECIMAL": (
        "with no scale argument, Informatica keeps the input's own scale "
        "while Spark must be told one at cast time. decimal(38,10) is "
        "emitted, which preserves the fraction and matches Oracle's "
        "published AIDP mapping -- but the input's real scale lives in the "
        "port definition, which an expression cannot see. Confirm it for "
        "any column feeding a numeric total"
    ),
    "MD5": (
        "The digest depends on the input encoding. A non-ASCII source "
        "read under a different charset than Informatica used will hash "
        "differently, and the values look equally valid"
    ),
}
_DIVERGENT_EXPR_RE = re.compile(
    r"\b(" + "|".join(_DIVERGENT_EXPR_FN) + r")\s*\(", re.IGNORECASE
)

_ORDER_DEPENDENT_AGG = re.compile(
    r"\b(FIRST|LAST|CUME|MOVINGAVG|MOVINGSUM)\s*\(", re.IGNORECASE
)


_INTEGER_TYPES = {"integer", "small integer", "smallint", "int", "bigint", "long", "tinyint"}
_PREDICATE_FN = re.compile(r"^\s*(IS_NUMBER|IS_DATE|IS_SPACES|REG_MATCH)\s*\(", re.IGNORECASE)


def _as_column(pyspark_expr: str) -> str:
    """A whole-port expression that converted to a bare literal (``4``,
    ``'X'``) is a Python value, and withColumn needs a Column."""
    s = (pyspark_expr or "").strip()
    if re.fullmatch(r"-?\d+(?:\.\d+)?", s) or (
        len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"" and s.count(s[0]) == 2
    ):
        return f"F.lit({s})"
    return pyspark_expr


_AGGREGATE_FN = re.compile(
    r"\b(SUM|COUNT|AVG|MIN|MAX|FIRST|LAST|MEDIAN|PERCENTILE|STDDEV|VARIANCE|"
    r"MOVINGAVG|MOVINGSUM|CUME)\s*\(",
    re.IGNORECASE,
)


def lookup_prefix(tx: Transformation) -> str:
    """The prefix a connected Lookup's ports carry in the pipeline until a
    connector renames them (see TransformationConverter._lookup_plan)."""
    return f"{tx.name}__"


def _conversion_failed(pyspark_expr: str, review) -> bool:
    """True when ``_convert_or_flag`` could not convert (placeholder
    returned), False when it converted and only attached a divergence note."""
    return bool(review) and pyspark_expr == "F.lit(None)" and "could not convert" in review


_REFUSAL_NOTE = (
    "A condition that could not be converted used to be emitted as a comment "
    "followed by .filter(F.lit(None)), which is not a NULL condition -- it is "
    "FALSE for every row, so the branch silently received nothing. The "
    "notebook ran, wrote an empty result and reported success, which is worse "
    "than failing. _conversion_failed existed for exactly this case and was "
    "never called."
)


def _refuse_lines(what: str, review: str, indent: str = "") -> list:
    """REVIEW marker plus a raise, for a condition we could not convert.

    The review quotes the export's expression, and exports carry CR/LF
    inside expressions. Interpolated into one ``# ...`` line, everything
    after the first line break landed in code position: the notebook failed
    to compile, so the raise that was meant to stop it never ran either.
    Every line of the review gets its own prefix, as ``_comment_lines``
    does, and ``what`` goes in through repr() for the same reason.
    """
    body = (review or "").replace("\r\n", "\n").replace("\r", "\n")
    return [
        *(f"{indent}# {ln.rstrip()}" for ln in body.split("\n")),
        f"{indent}raise NotImplementedError(",
        f"{indent}    {'REVIEW REQUIRED: ' + what + ' -- the condition could not be '!r}",
        f'{indent}    "converted, so this branch cannot be filtered correctly. "',
        f'{indent}    "Emitting it anyway would silently drop every row. "',
        f'{indent}    "Translate the condition by hand."',
        f"{indent})",
    ]


def _safe_var(name: str) -> str:
    """Convert a name to a safe Python variable."""
    return re.sub(r'[^a-zA-Z0-9]', '_', name).lower().strip('_')


def _null_of(field) -> str:
    """``F.lit(None)`` cast to the port's declared type.

    A bare ``F.lit(None)`` is Spark's NullType (VOID). It survives the
    notebook's transformations but not the write: Delta refuses to create
    or evolve a table with a VOID column (``DELTA_MERGE_ADD_VOID_COLUMN``,
    seen on AIDP 2026-09-25 for an unconvertible expression port), so a
    notebook carrying one REVIEW marker could not write at all. The port's
    Informatica datatype is known, so the placeholder can carry it.
    """
    return f'F.lit(None).cast("{_port_spark_type(field)}")'


def _port_spark_type(field) -> str:
    """Informatica port datatype -> Spark SQL type name."""
    dt = (getattr(field, "datatype", "") or "string").strip().lower()
    precision = getattr(field, "precision", 0) or 0
    scale = getattr(field, "scale", 0) or 0
    if dt in ("integer", "int", "small integer", "smallint", "short"):
        return "int"
    if dt in ("bigint", "long"):
        return "bigint"
    if dt in ("number", "decimal", "numeric", "number(p,s)", "money"):
        if scale:
            # An export that carries a scale but no precision (0) means
            # "unknown", not "tiny": decimal(scale+1, scale) would overflow
            # every value >= 10 to NULL once real data flows.
            return f"decimal({precision if precision >= scale and precision > 0 else 38},{scale})"
        if precision and precision <= 9:
            return "int"
        if precision and precision <= 18:
            return "bigint"
        return "decimal(38,0)" if precision else "decimal(38,10)"
    if dt in ("double", "float", "real"):
        return "double"
    if dt in ("date",):
        return "date"
    if dt in ("date/time", "datetime", "timestamp", "time"):
        return "timestamp"
    if dt in ("boolean", "bit"):
        return "boolean"
    if dt in ("binary", "raw", "blob", "varbinary"):
        return "binary"
    return "string"


def _references_own_port(expression: str, port_name: str) -> bool:
    """True if `expression` references `port_name` as a bare identifier.

    Detects a self-referencing variable port (``v_run = v_run + AMOUNT``):
    row-ordered accumulator state that only a window function can express
    correctly, never a plain ``withColumn`` (Tier 1 item 1;
    ). Informatica port names are bare identifiers inside
    expressions, so a word-boundary match is enough to tell ``V_RUN`` apart
    from e.g. ``V_RUNNING_TOTAL``.
    """
    return bool(
        re.search(
            rf"(?<![A-Za-z0-9_]){re.escape(port_name)}(?![A-Za-z0-9_])",
            expression,
        )
    )


def _comment_lines(text: str, label: str) -> list[str]:
    """``text`` from the export, as comment lines that cannot escape.

    Export free text -- an Update Strategy expression, a SQL override, a
    description -- can contain newlines and non-ASCII. Interpolating it into
    a single f-string comment prefixes only the FIRST line, so the rest
    lands in code position. A real export's strategy expression was written
    across four lines with an arrow in it, and the notebook failed to
    compile: "invalid character '->' (U+2192)".

    Every line gets its own prefix here, and a blank result becomes a single
    line so the label is never left dangling.
    """
    body = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not body.strip():
        return [f"# {label}: (none)"]
    out = [f"# {label}:"]
    out.extend(f"#   {ln.rstrip()}" for ln in body.split("\n"))
    return out


class TransformationConverter:
    """Produce PySpark code for every supported Informatica transformation type."""

    def __init__(self) -> None:
        self.expr = ExpressionConverter()

    def _convert_or_flag(self, infa_expr: str) -> tuple[str, Optional[str]]:
        """Convert one Informatica expression; never raise and never return
        anything that could corrupt the enclosing generated statement.

        Returns ``(pyspark_expr, review_reason)``. On success ``review_reason``
        is ``None`` and ``pyspark_expr`` is the real converted code. On
        failure -- ExpressionConverter raised ``UnconvertibleExpression``, or
        raised anything else, or (belt-and-suspenders) returned something
        unusable -- ``pyspark_expr`` is always the valid placeholder
        ``"F.lit(None)"`` and ``review_reason`` is a message the caller must
        emit as its OWN comment line, never spliced into the expression
        itself.
        """
        divergent = _DIVERGENT_EXPR_RE.search(infa_expr or "")
        try:
            result = self.expr.convert(infa_expr)
        except UnconvertibleExpression as exc:
            return "F.lit(None)", f"REVIEW REQUIRED: could not convert `{infa_expr}` -- {exc}"
        except Exception as exc:  # a converter bug must not crash code generation
            return (
                "F.lit(None)",
                f"REVIEW REQUIRED: could not convert `{infa_expr}` -- "
                f"{type(exc).__name__}: {exc}",
            )
        if not result or "#" in result or "\n" in result:
            return (
                "F.lit(None)",
                f"REVIEW REQUIRED: could not convert `{infa_expr}` -- converter "
                f"returned an unusable result: {result!r}",
            )
        if divergent:
            fn = divergent.group(1).upper()
            return result, (
                f"REVIEW REQUIRED: `{infa_expr}` converted, but {fn} differs "
                f"between engines for some inputs. {_DIVERGENT_EXPR_FN[fn]}."
            )
        return result, None

    # ---------------------------------------------------------------- public

    def convert(
        self,
        tx: Transformation,
        input_df: str = "df",
        output_df: str = "df",
        extra_inputs: Optional[dict[str, str]] = None,
    ) -> list[str]:
        """Dispatch to the appropriate handler based on transformation type.

        Args:
            tx: The Informatica transformation model.
            input_df: Variable name of the incoming DataFrame.
            output_df: Variable name for the result DataFrame.
            extra_inputs: Map of transformation-instance-name to DataFrame
                          variable name (used by Joiner, Union, etc.).

        Returns:
            List of PySpark code lines.
        """
        extra_inputs = extra_inputs or {}
        dispatch = {
            TransformationType.SOURCE_QUALIFIER: self._source_qualifier,
            TransformationType.EXPRESSION: self._expression,
            TransformationType.FILTER: self._filter,
            TransformationType.JOINER: self._joiner,
            TransformationType.LOOKUP: self._lookup,
            TransformationType.AGGREGATOR: self._aggregator,
            TransformationType.ROUTER: self._router,
            TransformationType.SEQUENCE_GENERATOR: self._sequence_generator,
            TransformationType.UPDATE_STRATEGY: self._update_strategy,
            TransformationType.SORTER: self._sorter,
            TransformationType.RANK: self._rank,
            TransformationType.UNION: self._union,
            TransformationType.STORED_PROCEDURE: self._stored_procedure,
            TransformationType.SQL: self._sql_transformation,
            TransformationType.NORMALIZER: self._normalizer,
            TransformationType.MAPPLET: self._mapplet,
            TransformationType.INPUT: self._mapplet_port,
            TransformationType.OUTPUT: self._mapplet_port,
            TransformationType.DEDUPLICATE: self._deduplicate,
            TransformationType.TRANSACTION_CONTROL: self._transaction_control,
            TransformationType.HIERARCHY_PARSER: self._hierarchy_parser,
            TransformationType.HIERARCHY_BUILDER: self._hierarchy_builder,
            TransformationType.HIERARCHY_PROCESSOR: self._hierarchy_processor,
            TransformationType.STRUCTURE_PARSER: self._structure_parser,
            # Phase 4 -- embedded source code, preserved not translated
            TransformationType.JAVA: self._code_carrying,
            TransformationType.PYTHON: self._code_carrying,
            TransformationType.VELOCITY: self._code_carrying,
            TransformationType.CUSTOM: self._code_carrying,
            TransformationType.EXTERNAL_PROCEDURE: self._code_carrying,
            # Phase 5 -- AIDP-native equivalents
            TransformationType.CHUNKING: self._chunking,
            TransformationType.VECTOR_EMBEDDING: self._vector_embedding,
            TransformationType.MACHINE_LEARNING: self._machine_learning,
            # Source Qualifier variants. Application/MQ/XML Source
            # Qualifiers are Source Qualifiers over a non-relational source:
            # the read differs, the transformation semantics do not, so they
            # share the handler rather than falling through to "unsupported".
            TransformationType.APPLICATION_SOURCE_QUALIFIER: self._source_qualifier,
            TransformationType.MQ_SOURCE_QUALIFIER: self._source_qualifier,
            TransformationType.XML_SOURCE_QUALIFIER: self._source_qualifier,
            # Phase 6 -- partial conversion where one exists
            TransformationType.DATA_MASKING: self._data_masking,
        }
        # Phase 6 -- no Spark equivalent; reported with a specific reason
        for _t in self._NO_EQUIVALENT:
            dispatch.setdefault(_t, self._report_no_equivalent)
        handler = dispatch.get(tx.type, self._unsupported)
        return handler(tx, input_df, output_df, extra_inputs)

    # ----------------------------------------------------- Source Qualifier

    def _source_qualifier(
        self, tx: Transformation, _in: str, out: str, _extra: dict
    ) -> list[str]:
        source_table = get_ci(tx.properties, "source table", default="") or tx.name
        return self.source_read_lines(tx, source_table, out)

    # Oracle/ANSI SQL syntax that has no Spark-SQL equivalent and would
    # fail (or, worse, silently change meaning) inside F.expr().
    _SQL_UNTRANSLATABLE = re.compile(
        r"\(\+\)|\bROWNUM\b|\bCONNECT\s+BY\b|\bNVL2?\s*\(|\bDECODE\s*\(|\bSYSDATE\b|"
        r"\bTO_DATE\s*\(|\bTRUNC\s*\(|\bMINUS\b|\bINTERSECT\b|:\w+",
        re.IGNORECASE,
    )

    # The FROM/JOIN clause of a query: everything after the keyword up to
    # the next clause keyword. Captured as a whole because a FROM clause is
    # a comma-separated LIST -- matching only the first identifier after
    # FROM left the second table of `FROM A a, B b` unqualified, which in
    # Spark resolves against whatever catalog the session happens to be
    # using. A wrong table that reads successfully is the worst outcome
    # available here.
    _SQL_FROM_CLAUSE = re.compile(
        r"\b(?:FROM|JOIN)\s+(.+?)"
        r"(?=\b(?:WHERE|GROUP|ORDER|HAVING|UNION|MINUS|INTERSECT|EXCEPT|"
        r"JOIN|INNER|LEFT|RIGHT|FULL|CROSS|ON|CONNECT|START)\b|$)",
        re.IGNORECASE | re.DOTALL,
    )

    # An Oracle string literal ('' is an escaped quote), a double-quoted
    # identifier or a /* block comment */, scanned in one pass so a quote
    # of one kind inside another cannot start a false token. The comment
    # has to be in the same pass: the apostrophe in `/* customer's */`
    # opened a literal that swallowed the `JOIN CUSTOMERS` after it, so
    # that table was never qualified and the join read whatever the
    # session's catalog called CUSTOMERS.
    _SQL_QUOTED = re.compile(r"'(?:[^']|'')*'|\"[^\"]*\"|/\*.*?\*/", re.DOTALL)
    _SQL_MASK = re.compile(r"\x00(\d+)\x00")

    @staticmethod
    def _mask_sql_literals(sql: str) -> tuple[str, list[str]]:
        """``sql`` with every string literal replaced by a placeholder, and
        the literals, for ``_unmask_sql_literals`` to put back.

        Table qualification is a textual rewrite, and it reached inside
        literals: ``WHERE c.SEGMENT = 'CUSTOMERS'`` became ``=
        'oltp.app.customers'`` -- a query that runs and matches nothing.
        A FROM inside a literal was likewise scanned as a table.

        A double-quoted identifier that is a plain name is unquoted: Spark
        reads ``"X"`` as a string literal, not an identifier, so
        ``SELECT "NAME"`` would return the text NAME on every row. Any other
        double-quoted identifier is left as written; the override gate
        refuses it.
        """
        lits: list[str] = []

        def _one(m: re.Match) -> str:
            tok = m.group(0)
            if tok.startswith("/*"):
                return " "  # a comment is not SQL: dropped, not restored
            if tok.startswith('"'):
                return tok[1:-1] if re.fullmatch(r"[A-Za-z_][\w$#]*", tok[1:-1]) else tok
            lits.append(tok)
            return f"\x00{len(lits) - 1}\x00"

        return TransformationConverter._SQL_QUOTED.sub(_one, sql), lits

    @staticmethod
    def _unmask_sql_literals(masked: str, lits: list[str]) -> str:
        return TransformationConverter._SQL_MASK.sub(
            lambda m: lits[int(m.group(1))], masked
        )

    @staticmethod
    def _sql_tables_referenced(sql: str) -> set[str]:
        """Bare table names the FROM/JOIN clauses name, upper-cased.

        Aliases are dropped (``ORDERS o`` -> ``ORDERS``) and a derived table
        (``FROM (SELECT ...``) contributes nothing for that position -- the
        caller treats "no resolvable table" as a reason to refuse, so an
        unparsed shape fails safe.
        """
        return {
            ref.split(".")[-1].upper()
            for ref in TransformationConverter._sql_table_refs(sql)
        }

    @staticmethod
    def _sql_table_refs(sql: str) -> list[str]:
        """The table references the FROM/JOIN clauses name, as written --
        ``APP.ORDERS`` stays dotted, because it is the dotted text that has
        to be replaced by the catalog name. String literals are masked
        first, so a FROM inside one is not a table."""
        sql = TransformationConverter._mask_sql_literals(sql)[0]
        out: list[str] = []
        for m in TransformationConverter._SQL_FROM_CLAUSE.finditer(sql):
            for item in m.group(1).split(","):
                item = item.strip()
                if not item:
                    continue
                if item.startswith("("):
                    # A derived table: `FROM (SELECT ... FROM REAL_TBL) t`.
                    # The base tables are inside it, and they still have to
                    # be qualified, so recurse rather than skip -- skipping
                    # made every sub-select refuse.
                    inner = item[1:]
                    if inner.rstrip().endswith(")"):
                        inner = inner.rstrip()[:-1]
                    else:
                        inner = inner.rsplit(")", 1)[0] if ")" in inner else inner
                    out.extend(TransformationConverter._sql_table_refs(inner))
                    continue
                # A table named inside a subquery arrives with the closing
                # paren attached (`FROM CUSTOMERS)` in `IN (SELECT id FROM
                # CUSTOMERS)`). Without stripping it the token failed the
                # identifier check and the table was silently never
                # qualified -- a bare name that resolves against whatever
                # catalog the session is using.
                first = item.split()[0].strip('"').rstrip(");,")
                if not re.fullmatch(r"[A-Za-z_][\w$#.]*", first):
                    continue
                if first.upper() in ("SELECT", "DUAL", "TABLE", "LATERAL"):
                    continue
                out.append(first)
        return out

    @staticmethod
    def _sql_unscannable(masked: str) -> Optional[str]:
        """Why literal-masked SQL cannot be rewritten safely, or None.

        The override reaches here with its whitespace collapsed, so a
        ``--`` line comment would run to the end of the whole query in
        Spark and silently cut off what followed it. A quote left after
        masking is an unterminated literal: the scan cannot tell SQL from
        data past it.
        """
        if "--" in masked:
            return "it carries a -- line comment, which would comment out the rest of the query once it is on one line"
        if "'" in masked:
            return "it has an unterminated string literal, so its tables cannot be found reliably"
        return None

    def _sql_refusal_reason(
        self, sql: str, source_tables: Optional[dict[str, str]]
    ) -> str:
        """Why the override was not run as Spark SQL. Goes in the review."""
        masked = self._mask_sql_literals(sql)[0]
        hit = self._SQL_UNTRANSLATABLE.search(masked)
        if hit:
            return (
                f"it uses Oracle-only SQL that Spark would reject or read "
                f"differently: {hit.group(0).strip()!r}"
            )
        quoted = re.search(r'"[^"]*"', masked)
        if quoted:
            return (
                f"it uses a double-quoted identifier, {quoted.group(0)}, which "
                f"Spark would read as a string literal"
            )
        unscanned = self._sql_unscannable(masked)
        if unscanned:
            return unscanned
        known = {k.upper() for k in (source_tables or {})}
        unknown = sorted(self._sql_tables_referenced(sql) - known)
        if unknown:
            return (
                f"it reads table(s) that are not sources in this mapping, so "
                f"they cannot be catalog-qualified: {', '.join(unknown)}"
            )
        if not source_tables:
            return "the mapping's source tables were not available to qualify it"
        return "it could not be parsed well enough to qualify its tables"

    # A qualified column reference in a join condition: TABLE.COLUMN.
    _SQL_QUALIFIED_COL = re.compile(r"\b([A-Za-z_][\w$#]*)\s*\.\s*[A-Za-z_][\w$#]*")
    # The same reference with the column captured, for backquoting it.
    _SQL_QUALIFIED_COL_PARTS = re.compile(r"\b([A-Za-z_][\w$#]*)\s*\.\s*([A-Za-z_][\w$#]*)")

    # One conjunct of a join condition that equates a column across two
    # tables: TABLE.COLUMN = TABLE.COLUMN, optionally parenthesised.
    _SQL_EQUI_CONJUNCT = re.compile(
        r"\s*\(*\s*([A-Za-z_][\w$#]*)\s*\.\s*([A-Za-z_][\w$#]*)\s*=\s*"
        r"([A-Za-z_][\w$#]*)\s*\.\s*([A-Za-z_][\w$#]*)\s*\)*\s*"
    )

    def _udj_query(
        self, udj: str,
        source_tables: Optional[dict[str, str]],
        source_columns: Optional[dict[str, list[str]]],
    ) -> tuple[Optional[str], list[str], Optional[str]]:
        """``(query, tables, None)`` for a User Defined Join, or
        ``(None, [], reason)`` to refuse it.

        The condition names its tables through qualified column references
        (``ORDERS.CUST_ID = CUSTOMERS.CUST_ID``), so the table list is
        derivable from the condition itself.

        The select list is explicit. ``SELECT *`` over ``FROM a, b`` returns
        the join key once per table, so the first reference to CUST_ID
        failed AMBIGUOUS_REFERENCE and Delta refuses to write duplicate
        column names. Each name is taken once, from the first table that has
        it -- which is only the same value as the other tables' column when
        the condition equates the two (an inner equi-join key). A name
        shared any other way is refused rather than picked from one side.
        """
        masked = self._mask_sql_literals(udj)[0]
        if "{" in masked or re.search(r"\bJOIN\b", masked, re.IGNORECASE):
            # PowerCenter's own outer-join syntax, `{ A LEFT OUTER JOIN B ON
            # ... }`. Placed after WHERE it is a ParseException at run time.
            return None, [], (
                "it is written in Informatica's outer-join syntax "
                "({ A LEFT OUTER JOIN B ON ... }), which is not translated"
            )
        if self._SQL_UNTRANSLATABLE.search(masked) or self._sql_unscannable(masked):
            return None, [], self._sql_refusal_reason(udj, source_tables)
        if not source_tables:
            return None, [], "the mapping's source tables were not available to qualify it"
        known = {k.upper() for k in source_tables}
        all_refs = list(dict.fromkeys(
            m.group(1).upper() for m in self._SQL_QUALIFIED_COL.finditer(masked)
        ))
        named = [ref for ref in all_refs if ref in known]
        # Every qualifier in the condition must be a table we know. An
        # unrecognised one may be an alias for a source not in this mapping,
        # and joining the wrong tables silently is the failure to avoid.
        unknown = [ref for ref in all_refs if ref not in known]
        if unknown:
            return None, [], (
                f"it qualifies columns with {', '.join(unknown)}, which is not "
                f"a source in this mapping"
            )
        if len(named) < 2:
            # A join we cannot name both sides of is not one we can write.
            return None, [], "it does not name two of this mapping's sources to join"

        cols = {k.upper(): list(v or []) for k, v in (source_columns or {}).items()}
        missing = [t for t in named if not cols.get(t)]
        if missing:
            return None, [], (
                f"the column list of {', '.join(missing)} was not available, and "
                f"SELECT * over a join returns each join key once per table"
            )
        holders: dict[str, list[str]] = {}
        select: list[str] = []
        for t in named:
            for c in cols[t]:
                tables = holders.setdefault(c.upper(), [])
                if t in tables:
                    continue
                if not tables:
                    # Backquoted: Oracle and PowerCenter allow # and $ in a
                    # column name, and ORDER# unquoted is a ParseException.
                    select.append(f"{t}.`{c}` AS `{c}`")
                tables.append(t)
        equated = self._udj_equated(masked)
        for col, tables in holders.items():
            if len(tables) > 1 and not equated(col, tables):
                return None, [], (
                    f"column {col} exists in {', '.join(tables)} and the join does "
                    f"not equate it, so one table's value cannot stand for both"
                )
        # The condition's column references are backquoted too, for the
        # same reason as the select list: an order number is usually the
        # join key, and `ORDERS.ORDER# = LINES.ORDER#` unquoted is a
        # ParseException. Done on the masked text so literals are untouched.
        cond_masked, cond_lits = self._mask_sql_literals(udj)
        cond = self._unmask_sql_literals(self._SQL_QUALIFIED_COL_PARTS.sub(
            lambda m: f"{m.group(1)}.`{m.group(2)}`", cond_masked), cond_lits)
        return f"SELECT {', '.join(select)} FROM {', '.join(named)} WHERE {cond}", named, None

    def _udj_equated(self, masked_udj: str):
        """``equated(col, tables)``: whether the condition's top-level AND
        conjuncts chain ``T1.col = T2.col = ...`` across all of ``tables``.

        Only an equality that holds for every joined row counts, so any OR
        in the condition disqualifies them all.
        """
        parent: dict[tuple[str, str], tuple[str, str]] = {}

        def find(x):
            while parent.get(x, x) != x:
                x = parent[x]
            return x

        if not re.search(r"\bOR\b", masked_udj, re.IGNORECASE):
            for conj in re.split(r"\bAND\b", masked_udj, flags=re.IGNORECASE):
                m = self._SQL_EQUI_CONJUNCT.fullmatch(conj)
                if m and m.group(2).upper() == m.group(4).upper():
                    col = m.group(2).upper()
                    a, b = (m.group(1).upper(), col), (m.group(3).upper(), col)
                    parent[find(a)] = find(b)

        def equated(col: str, tables: list[str]) -> bool:
            return len({find((t, col)) for t in tables}) == 1

        return equated

    def _user_defined_join_as_spark_sql(
        self, udj: str, table: str, out: str,
        source_tables: Optional[dict[str, str]],
        source_columns: Optional[dict[str, list[str]]] = None,
    ) -> Optional[list[str]]:
        """A User Defined Join, as a query over the sources it names.

        Synthesised by :meth:`_udj_query` into ``SELECT <columns> FROM a, b
        WHERE <condition>``, which then goes through
        :meth:`_sql_override_as_spark_sql` -- so an Oracle ``(+)`` outer
        join is refused there rather than being guessed at here.

        Returns None to refuse; :meth:`_udj_refusal_reason` says why.
        """
        synthetic, named, _ = self._udj_query(udj, source_tables, source_columns)
        if synthetic is None:
            return None
        emitted = self._sql_override_as_spark_sql(
            synthetic, table, out, source_tables
        )
        if not emitted:
            return None
        return [
            "# User Defined Join run as Spark SQL. The qualifier joined its own",
            f"# sources ({', '.join(named)}); that join is applied here rather than",
            "# left as a review item. REVIEW: confirm the row count matches the",
            "# Informatica session before trusting the output.",
        ] + [l for l in emitted if not l.startswith("#")]

    def _udj_refusal_reason(
        self, udj: str,
        source_tables: Optional[dict[str, str]],
        source_columns: Optional[dict[str, list[str]]] = None,
    ) -> str:
        """Why the User Defined Join was not run. Goes in the review."""
        synthetic, _, reason = self._udj_query(udj, source_tables, source_columns)
        return reason or self._sql_refusal_reason(synthetic or udj, source_tables)

    def _sql_override_as_spark_sql(
        self, sql: str, table: str, out: str,
        source_tables: Optional[dict[str, str]],
    ) -> Optional[list[str]]:
        """Emit the override as ``spark.sql(...)``, or None to refuse.

        Two gates, both of which must pass:

        1. **No Oracle-only construct.** ``(+)`` outer joins, ``ROWNUM``,
           ``CONNECT BY``, ``MINUS`` and bind variables either fail on Spark
           or -- worse -- parse and mean something else. Refused, not
           rewritten: guessing the ANSI equivalent of an Oracle join is how
           a migration produces plausible wrong numbers. So is a
           double-quoted identifier that is not a plain name, which Spark
           would read as a string literal.
        2. **Every table resolves to a source in this mapping.** Spark needs
           a catalog-qualified name and the override carries Oracle's
           unqualified one. A table we cannot map is one we cannot qualify,
           and a bare name would resolve against whatever the session's
           current catalog happens to be.

        When both pass, the override runs as written against the qualified
        tables. A mistranslation then surfaces as an AnalysisException on
        the first run rather than as silently different rows, which is the
        trade this is making.
        """
        if not source_tables:
            return None
        # Every check and rewrite below runs with string literals masked: a
        # literal is data, and neither a table name nor ':' inside one is SQL.
        masked, lits = self._mask_sql_literals(sql)
        if (self._SQL_UNTRANSLATABLE.search(masked) or '"' in masked
                or self._sql_unscannable(masked)):
            return None

        known = {k.upper(): v for k, v in source_tables.items()}
        refs = self._sql_table_refs(masked)
        referenced = {r.split(".")[-1].upper() for r in refs}
        if not referenced or (referenced - set(known)):
            return None

        # Qualify each table reference in place, AS WRITTEN in FROM/JOIN:
        # `APP.ORDERS` is replaced whole (and so is `APP.ORDERS.COL`). Only
        # the last part used to be kept, and the lookbehind that stops
        # `o.ORDERS` from being rewritten also skipped `APP.ORDERS`, so a
        # schema-qualified override ran unqualified against the session's
        # catalog. One pass over an alternation, longest first, so a
        # replacement is never itself rewritten and a table whose name is a
        # prefix of another cannot be half-substituted.
        target = {
            re.sub(r"\s+", "", r).upper(): known[r.split(".")[-1].upper()]
            for r in refs
        }
        alternation = "|".join(
            r"\s*\.\s*".join(re.escape(p) for p in ref.split("."))
            for ref in sorted(target, key=len, reverse=True)
        )
        qualified = re.sub(
            rf"(?<![\w$#.])(?:{alternation})(?![\w$#])",
            lambda m: target[re.sub(r"\s+", "", m.group(0)).upper()],
            masked, flags=re.IGNORECASE,
        )

        # $$PARAM references resolve through the notebook's _param(); a
        # literal would bake one run's value into the query.
        has_param = "$$" in sql
        if has_param:
            # The query becomes an f-string, so a brace the SQL itself
            # carries (`RLIKE '^[0-9]{5}$'`) would be evaluated by Python.
            def _braces(s: str) -> str:
                return s.replace("{", "{{").replace("}", "}}")

            def _in_literal(lit: str) -> str:
                # '$$X' is a whole literal: the value, quoted, as
                # _convert_where_fragment does. Written out as-is it became
                # ''EU'' -- a ParseException.
                whole = re.fullmatch(r"'\$\$(\w+)'", lit)
                if whole:
                    return "{_sql_lit(_param_text(%r))}" % whole.group(1)
                # '%$$X%': Informatica substitutes the parameter's TEXT into
                # the query before it runs, so it goes inside the literal.
                # Escaped with backslashes: Spark reads 'it''s' as two
                # adjacent literals, i.e. "its". chr() because a backslash
                # in an f-string expression is a SyntaxError before 3.12.
                return re.sub(
                    r"\$\$(\w+)",
                    lambda m: (
                        "{str(_param_text(%r)).replace(chr(92), chr(92) * 2)"
                        ".replace(chr(39), chr(92) + chr(39))}" % m.group(1)
                    ),
                    _braces(lit),
                )

            lits = [_in_literal(l) for l in lits]
            qualified = re.sub(
                r"\$\$(\w+)", lambda m: f"{{_sql_lit(_param_text('{m.group(1)}'))}}",
                _braces(qualified),
            )
        qualified = self._unmask_sql_literals(qualified, lits)

        lines = [
            f"# SQL override run as Spark SQL. The override JOINs/UNIONs or",
            f"# sub-selects, so it cannot be a spark.table() read -- it is run",
            f"# as written, with its tables resolved to catalog names. REVIEW:",
            f"# Spark SQL accepts most of ANSI, but confirm the result matches",
            f"# what Oracle returned before trusting the output.",
        ]
        quoted = qualified.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        prefix = "f" if has_param else ""
        lines.append(f'{out} = spark.sql({prefix}"""{quoted}""")')
        return lines

    def source_read_lines(
        self, tx: Transformation, table: str, out: str,
        source_tables: Optional[dict[str, str]] = None,
        source_columns: Optional[dict[str, list[str]]] = None,
    ) -> list[str]:
        """The read cell for a Source Qualifier: ``spark.table(...)`` plus
        every qualifier property that changes WHICH rows are read.

        Shared by the converter (comparison/confidence reports) and the
        notebook generator (the actual source cell) so both agree.

        ``source_columns`` maps each source's bare table name (upper-cased)
        to its column names; a User Defined Join needs them to write an
        explicit select list, and is refused without them.

        Properties honoured, in the order PowerCenter applies them. A SQL
        Query, when set, overrides the rest and Number Of Sorted Ports --
        they are reported as ignored, not applied:

        - **SQL Query** (override). A single-table ``SELECT ... FROM <table>
          [alias] WHERE ...`` becomes a table read plus a filter. An override
          that JOINs, UNIONs or sub-selects cannot be a ``spark.table()``
          read, so it is run as ``spark.sql()`` over catalog-qualified
          tables instead -- see ``_sql_override_as_spark_sql`` for the two
          gates it must pass, and the REVIEW REQUIRED item (naming which
          gate failed) it falls back to.
        - **Source Filter**: a WHERE fragment, applied as a filter.
        - **User Defined Join**: joins the qualifier's several sources.
          Synthesised into ``SELECT <columns> FROM a, b WHERE <condition>`` and put
          through the same translation and the same gates, rather than
          maintaining a second join translator.
        - **Select Distinct**: ``.distinct()``.

        WHERE fragments are Oracle SQL. ``F.expr`` accepts a useful subset
        (comparisons, AND/OR, IN, LIKE, IS NULL, BETWEEN); Oracle-only
        constructs are detected and the whole fragment becomes a review
        item rather than an AnalysisException at run time. ``$$PARAM``
        references resolve through the notebook's ``_param()`` and a
        ``<table>.`` or ``<alias>.`` qualifier is stripped, since the
        DataFrame has no table alias.
        """
        lines: list[str] = [f"# Source Qualifier: {tx.name}"]
        lines.append(f'{out} = spark.table("{table}")')

        where_clauses: list[tuple[str, str]] = []  # (label, raw where)
        aliases: set[str] = {table.split(".")[-1]}

        has_override = bool(tx.sql_override and tx.sql_override.strip())
        if has_override:
            sql = " ".join(tx.sql_override.split())
            simple = re.match(
                r"^\s*SELECT\s+(?P<cols>.+?)\s+FROM\s+(?P<tbl>[\w$#.\"]+)"
                r"(?:\s+(?:AS\s+)?(?P<alias>(?!WHERE\b|ORDER\b|GROUP\b)\w+))?"
                r"(?:\s+WHERE\s+(?P<where>.+?))?"
                r"(?:\s+ORDER\s+BY\s+.+)?\s*;?\s*$",
                sql, re.IGNORECASE,
            )
            complex_sql = (
                simple is None
                # MINUS / INTERSECT / EXCEPT are set operators, like UNION.
                # Without them here, `SELECT x FROM A MINUS SELECT x FROM B`
                # matched the single-table pattern with "MINUS" read as a
                # table ALIAS: the generator emitted a plain read of A, the
                # second half of the query vanished, and -- because it never
                # reached the complex path -- NOTHING was reported. A silent
                # wrong-rows read with no review item at all.
                or re.search(r"\bJOIN\b|\bUNION\b|\bMINUS\b|\bINTERSECT\b|\bEXCEPT\b|\bGROUP\s+BY\b|\bSELECT\b.*\bSELECT\b|,\s*[\w$#.\"]+\s+\w*\s*(?:WHERE|$)",
                             sql[sql.upper().find(" FROM "):] if " FROM " in sql.upper() else sql,
                             re.IGNORECASE) is not None
                or (simple is not None and re.search(r"\(|\bCASE\b", simple.group("cols"), re.IGNORECASE))
                # The single-table path keeps only the WHERE. DISTINCT and
                # ORDER BY in the override used to come back through the
                # Select Distinct / Sorted Ports settings -- PowerCenter's
                # Generate SQL writes them into the override -- but a SQL
                # Query now overrides those settings, so such an override is
                # run as written. A comment is only masked on that path too.
                or (simple is not None and re.match(r"DISTINCT\b", simple.group("cols").strip(), re.IGNORECASE))
                or re.search(r"\bORDER\s+BY\b|/\*|--", sql, re.IGNORECASE) is not None
            )
            if complex_sql:
                # A join/union/sub-select override cannot be reproduced by
                # one spark.table() read -- but it CAN often be run as SQL.
                # Spark SQL accepts most of the ANSI subset PowerCenter
                # overrides are written in, so running the override is a
                # real migration of it, where the alternative (read the base
                # table, report the rest) silently returns different rows.
                #
                # Translated only when every table it names resolves to a
                # source in this mapping (so it can be catalog-qualified)
                # and it contains no Oracle-only construct. Otherwise the
                # refusal below still stands: a wrong query that runs is
                # worse than one that is reported.
                translated = self._sql_override_as_spark_sql(
                    sql, table, out, source_tables
                )
                if translated:
                    lines = [lines[0]] + translated
                else:
                    lines.append(
                        f"# REVIEW REQUIRED: Source Qualifier '{tx.name}' has a SQL override "
                        f"that a single-table read cannot reproduce (join/union/sub-select/"
                        f"computed columns), and it could not be run as Spark SQL either "
                        f"({self._sql_refusal_reason(sql, source_tables)}). Only the base "
                        f"table is read below; the override's logic is NOT applied. Rewrite "
                        f"it as DataFrame operations or a spark.sql() over the catalog tables."
                    )
                    for i in range(0, len(sql), 110):
                        # chunked by width, so a newline inside the chunk would
                        # still escape: strip it to a space first
                        _chunk = sql[i:i + 110].replace("\r", " ").replace("\n", " ")
                        lines.append(f"#   SQL> {_chunk}")
            else:
                if simple.group("alias"):
                    aliases.add(simple.group("alias"))
                aliases.add(simple.group("tbl").split(".")[-1].strip('"'))
                if simple.group("where"):
                    where_clauses.append(("SQL override WHERE", simple.group("where")))

        # Number Of Sorted Ports = N: PowerCenter adds ORDER BY on the
        # qualifier's first N ports. Row order matters to whatever reads it
        # in order -- SETMAXVARIABLE, a Sorted Input Aggregator/Joiner, a
        # variable port carrying the previous row -- and was dropped.
        try:
            n_sorted = int(str(get_ci(tx.properties, "number of sorted ports", default="0") or "0"))
        except ValueError:
            n_sorted = 0

        # A SQL Query overrides the User-Defined Join, Source Filter, Number
        # Of Sorted Ports and Select Distinct settings: PowerCenter runs the
        # query as written and ignores them. Applying them as well joined a
        # second time, filtered rows the query kept -- and a translated UDJ
        # replaced the cell outright, override and its refusal included.
        if has_override:
            ignored = [
                name for name, is_set in (
                    ("User Defined Join", bool((tx.user_defined_join or "").strip())),
                    ("Source Filter", bool((tx.source_filter or "").strip())),
                    ("Number Of Sorted Ports", n_sorted > 0),
                    ("Select Distinct", bool(tx.select_distinct)),
                ) if is_set
            ]
            if ignored:
                lines.append(
                    f"# {', '.join(ignored)} set on '{tx.name}' but NOT applied: "
                    f"the SQL Query overrides them, as it does in PowerCenter."
                )

        if not has_override and tx.source_filter and tx.source_filter.strip():
            where_clauses.append(("Source Filter", tx.source_filter.strip()))

        if not has_override and tx.user_defined_join and tx.user_defined_join.strip():
            # A User Defined Join joins the qualifier's OWN sources. That is
            # exactly what `SELECT <columns> FROM a, b WHERE <join>` means,
            # so it is synthesised into that query and put through the same SQL
            # translation (and the same two gates) as a SQL override --
            # rather than maintaining a second, weaker join translator.
            #
            # Before this, the join was always a review item: the notebook
            # read one of the sources and the join never happened.
            udj = " ".join(tx.user_defined_join.split())
            joined = self._user_defined_join_as_spark_sql(
                udj, table, out, source_tables, source_columns
            )
            if joined:
                lines = [lines[0]] + joined
            else:
                lines.append(
                    f"# REVIEW REQUIRED: Source Qualifier '{tx.name}' has a User Defined Join "
                    f"({udj[:120]}) across its sources, which could not be translated "
                    f"({self._udj_refusal_reason(udj, source_tables, source_columns)}). Only "
                    f"{table} is read here -- add the join as a DataFrame .join() before "
                    f"running this notebook."
                )

        for label, raw in where_clauses:
            converted = self._convert_where_fragment(raw, aliases)
            if converted is None:
                lines.append(
                    f"# REVIEW REQUIRED: {label} on '{tx.name}' uses SQL that Spark's "
                    f"F.expr() cannot evaluate -- NOT applied, so this read returns "
                    f"more rows than the source did. Rewrite by hand:"
                )
                lines.append(f"#   {raw[:160]}")
            else:
                lines.append(f"# {label}: {raw[:120]}")
                lines.append(f"{out} = {out}.filter({converted})")

        if tx.select_distinct and not has_override:
            lines.append(f"{out} = {out}.distinct()  # Select Distinct = YES")

        if n_sorted > 0 and not has_override:
            ports = [f.name for f in tx.fields if isinstance(f, TransformationField)][:n_sorted]
            if ports:
                lines.append(
                    f"{out} = {out}.orderBy({', '.join(repr(c) for c in ports)})"
                    f"  # Number Of Sorted Ports = {n_sorted}"
                )

        return lines

    def _convert_where_fragment(self, raw: str, aliases: set[str]) -> Optional[str]:
        """Best-effort translation of an Oracle WHERE fragment to an
        ``F.expr(...)`` string, or ``None`` when it cannot be trusted.

        ``$$PARAM`` becomes a Python-interpolated ``_param()`` value (quoted
        as a SQL string literal), qualifiers matching the table/alias are
        stripped, and TO_DATE(literal, fmt) becomes to_timestamp with the
        Informatica mask converted. Anything in ``_SQL_UNTRANSLATABLE`` after
        that returns ``None``.
        """
        frag = raw.strip().rstrip(";")
        for alias in sorted(aliases, key=len, reverse=True):
            if alias:
                frag = re.sub(rf"(?<![\w.]){re.escape(alias)}\.(?=\w)", "", frag)

        # TO_DATE('literal','mask') -> to_timestamp('literal', 'java mask')
        from infa_compat.datemask import to_java_format

        def _to_date(m: re.Match) -> str:
            return f"to_timestamp({m.group(1)}, '{to_java_format(m.group(2))}')"

        frag = re.sub(
            r"TO_DATE\s*\(\s*('[^']*')\s*,\s*'([^']+)'\s*\)", _to_date, frag, flags=re.IGNORECASE
        )
        # $$PARAM -> Python interpolation of the resolved parameter value
        params = set(re.findall(r"\$\$(\w+)", frag))
        if params:
            # The parameter's TEXT: Informatica substitutes it into the SQL
            # as written, before the query runs.
            frag = re.sub(r"'\$\$(\w+)'", lambda m: "{_sql_lit(_param_text(%r))}" % m.group(1), frag)
            frag = re.sub(r"\$\$(\w+)", lambda m: "{_sql_lit(_param_text(%r))}" % m.group(1), frag)
        # Searched with string literals blanked: the date mask just written
        # ('MM/dd/yyyy HH:mm:ss') contains ":mm", which the bind-variable
        # pattern matched, so every Source Filter with a TO_DATE time mask
        # was dropped and the whole table read.
        if self._SQL_UNTRANSLATABLE.search(re.sub(r"'(?:[^']|'')*'", "''", frag)):
            return None
        if params:
            return "F.expr(f" + repr(frag) + ")"
        return "F.expr(" + repr(frag) + ")"

    # ----------------------------------------------------------- Expression

    def _expression(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Expression: {tx.name}")

        # Variable ports (PORTTYPE VARIABLE) are evaluated row-by-row in
        # declaration order and later ports (including output ports) may
        # reference them -- they are not merely "also an output" (
        # Step 1; matches the Rust port's expression-collection filter,
        # which keys on `direction != Input`, not on an OUTPUT/INPUT_OUTPUT
        # allowlist). tx.fields is already in XML declaration order (see
        # xml_parser._parse_transformation's plain findall("TRANSFORMFIELD")
        # append loop), and PowerCenter itself only allows a port to
        # reference a port declared above it -- so iterating this list in
        # order and including VARIABLE alongside OUTPUT/INPUT_OUTPUT emits
        # every variable's expression before any later port that uses it,
        # with no extra ordering logic needed. A variable port is also not
        # necessarily part of the target schema; that's already handled
        # downstream -- the final target write selects only the target's
        # declared columns, so an intermediate variable column left in the
        # dataframe here is harmless.
        computed_fields = [
            f for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction != DataFlowDirection.INPUT
            and f.expression
        ]
        # Informatica evaluates input ports, then VARIABLE ports top to
        # bottom, then output ports -- so an output port sees the CURRENT
        # row's value of every variable port, even one declared below it.
        # Emitting in declaration order computed such an output before its
        # variable existed (and it was flagged as a previous-row reference).
        computed_fields = (
            [f for f in computed_fields if f.direction == DataFlowDirection.VARIABLE]
            + [f for f in computed_fields if f.direction != DataFlowDirection.VARIABLE]
        )

        # Variable ports declared LATER than the port that references them.
        # Informatica evaluates ports top to bottom and a variable port keeps
        # its value from the previous row until it is re-evaluated, so
        # ``v_PREV = v_CURR`` written ABOVE ``v_CURR = KEY`` is the standard
        # PowerCenter idiom for "the previous row's key" -- row-ordered
        # state, exactly like a self-reference. A plain withColumn either
        # fails (column not yet defined) or, if a same-named input column
        # exists, silently reads the CURRENT row.
        variable_index = {
            f.name: i for i, f in enumerate(tx.fields)
            if isinstance(f, TransformationField) and f.direction == DataFlowDirection.VARIABLE
        }

        def _forward_variable_refs(field: TransformationField) -> list[str]:
            # Only a VARIABLE port can see a previous row's value: output
            # ports are evaluated after every variable port.
            if field.direction != DataFlowDirection.VARIABLE:
                return []
            position = tx.fields.index(field)
            return [
                name for name, idx in variable_index.items()
                if idx > position and _references_own_port(field.expression, name)
            ]

        stateful = [
            f for f in computed_fields
            if f.direction == DataFlowDirection.VARIABLE and (
                _forward_variable_refs(f) or _references_own_port(f.expression, f.name))
        ]
        if stateful:
            plan = self._stateful_expression(tx, in_df, out, computed_fields, variable_index)
            if plan is not None:
                return lines + plan

        current = in_df
        ulkp_cache: dict = {}
        for field in computed_fields:
            # Skip pass-through fields (expression == field name) — they're no-ops
            if field.expression.strip() == field.name:
                continue

            forward = _forward_variable_refs(field)
            if forward:
                lines.append(
                    f"# REVIEW REQUIRED: port {field.name} references variable "
                    f"port(s) {', '.join(forward)} declared BELOW it "
                    f"({field.name} = {field.expression}). In Informatica that "
                    "reads the PREVIOUS row's value (row-ordered state); it "
                    "needs a Window/lag over the mapping's sort order and was "
                    "not auto-converted. Do not run this notebook until this "
                    "is fixed."
                )
                lines.append(
                    f'{current} = {current}.withColumn("{field.name}", '
                    f'{_null_of(field)})  # REVIEW REQUIRED: previous-row variable reference'
                )
                continue

            if field.direction == DataFlowDirection.VARIABLE and _references_own_port(
                field.expression, field.name
            ):
                # A variable port that references itself (e.g. v_run =
                # v_run + AMOUNT) is row-ordered running state -- Spark can
                # only express that with a window function, never a plain
                # withColumn. That translation is separate M2 work (spec
                # Sec 21 Tier 1 item 1); do not guess it here. Emit a valid
                # placeholder plus an explicit, un-missable review item
                # instead of silently producing a wrong (row-independent)
                # column.
                lines.append(
                    f"# REVIEW REQUIRED: variable port {field.name} is "
                    f"self-referencing ({field.name} = {field.expression}) -- "
                    "row-ordered running/accumulator state requires a window "
                    "function and was not auto-converted. Do not run this "
                    "notebook until this is fixed."
                )
                lines.append(
                    f'{current} = {current}.withColumn("{field.name}", '
                    f'{_null_of(field)})  # REVIEW REQUIRED: self-referencing variable port'
                )
                continue

            expression = field.expression
            if ":LKP." in expression.upper():
                expression, ulkp_lines = self._resolve_unconnected_lookups(
                    expression, current, _extra.get("__unconnected_lookups__") or {}, ulkp_cache,
                )
                lines.extend(ulkp_lines)
            pyspark_expr, review = self._convert_or_flag(expression)
            pyspark_expr = _as_column(pyspark_expr)
            if (review is None and field.datatype.lower() in _INTEGER_TYPES
                    and _PREDICATE_FN.match(field.expression or "")):
                # IS_NUMBER/IS_DATE/IS_SPACES/REG_MATCH are 1/0 in Informatica;
                # into an integer port they must not land as BOOLEAN.
                pyspark_expr = f"({pyspark_expr}).cast('int')"
            if review and not _conversion_failed(pyspark_expr, review):
                # Converted, with an engine-divergence note (INITCAP, MD5,
                # TO_DECIMAL...): the conversion stands and the note travels
                # with it. This branch used to NULL the whole column, so a
                # mapping using TO_DECIMAL anywhere lost that port's values.
                lines.append(f"# {review}")
                review = None
            if review:
                lines.append(f"# {review}")
                lines.append(f"# {field.name}: {field.expression}")
                lines.append(
                    f'{current} = {current}.withColumn("{field.name}", '
                    f"{_null_of(field)})  # REVIEW REQUIRED: manual conversion"
                )
            else:
                # Cast to DecimalType if field has precision/scale and involves arithmetic
                needs_decimal = (
                    field.datatype.lower() in ("number", "decimal", "numeric", "float", "double")
                    and field.scale > 0
                    and any(op in field.expression for op in ("*", "/", "+", "-"))
                    and field.expression.strip() != field.name  # not a pass-through
                )
                if needs_decimal:
                    lines.append(
                        f'{current} = {current}.withColumn("{field.name}", '
                        f"({pyspark_expr}).cast(DecimalType({field.precision}, {field.scale})))"
                    )
                else:
                    lines.append(
                        f'{current} = {current}.withColumn("{field.name}", {pyspark_expr})'
                    )

        if current != out:
            lines.append(f"{out} = {current}")
        return lines

    @staticmethod
    def _split_top(text: str, sep: str = ",") -> list:
        """Split on ``sep`` outside parentheses and quotes."""
        parts, depth, quote, cur = [], 0, None, ""
        for ch in text:
            if quote:
                cur += ch
                if ch == quote:
                    quote = None
                continue
            if ch in "'\"":
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == sep and depth == 0:
                parts.append(cur.strip())
                cur = ""
                continue
            cur += ch
        parts.append(cur.strip())
        return parts

    @staticmethod
    def _initial_value(field) -> str:
        """A variable port's value before the first row: Informatica
        initialises numeric variables to 0, strings to '', dates to
        01/01/1753 -- not NULL."""
        dt = (field.datatype or "").lower()
        if "date" in dt or "time" in dt:
            return "F.lit('1753-01-01 00:00:00').cast('timestamp')"
        if dt in ("string", "nstring", "text", "ntext", "char", "varchar", "varchar2"):
            return "F.lit('')"
        return "F.lit(0)"

    def _stateful_expression(self, tx, in_df, out, computed_fields, variable_index):
        """Variable ports that read a previous row, translated to windows
        over the order the rows arrive in, or None when a port is outside
        the forms below.

        Informatica evaluates an Expression row by row: variable ports top
        to bottom, each keeping its value from the previous row until it
        is re-evaluated. So a variable reading one declared BELOW it gets
        that port's previous-row value (``v_PREV = v_CURR`` above
        ``v_CURR = KEY``), and a self-reference reads its own previous
        value. Before the first row a variable holds its type's initial
        value (0, '', 01/01/1753). Supported self-references:

        - ``V + x``                  running total
        - ``IIF(c, V + x, y)``       running total that restarts at y when c is not true
        - ``IIF(c, y, V + x)``       the same with c negated
        - ``IIF(c, V, y)``           carry forward, y when c is not true

        The order is the DataFrame's row order on entry -- the sorted
        order when the qualifier or a Sorter sorts, as Informatica's is;
        with no sort it is as undefined as Informatica's own arrival order.
        The windows run in one partition, as the sequential evaluation
        itself is sequential.
        """
        fields = [f for f in tx.fields if isinstance(f, TransformationField)]
        by_name = {f.name.upper(): f for f in fields}
        variables = [f for f in computed_fields if f.direction == DataFlowDirection.VARIABLE]
        pos = {f.name.upper(): i for i, f in enumerate(fields)}

        def refs(expr: str) -> set:
            return {f.name for f in variables if _references_own_port(expr, f.name)}

        # Dependencies: a variable needs every variable it names (current
        # value if declared above, previous-row value -- still a computed
        # column -- if declared below); itself only via its recurrence.
        deps = {v.name: refs(v.expression) - {v.name} for v in variables}
        order, done, visiting = [], set(), set()

        def visit(n: str) -> bool:
            if n in done:
                return True
            if n in visiting:
                return False
            visiting.add(n)
            for d in sorted(deps[n], key=lambda x: pos[x.upper()]):
                if not visit(d):
                    return False
            visiting.discard(n)
            done.add(n)
            order.append(n)
            return True

        for v in variables:
            if not visit(v.name):
                return None   # a cycle through current values: not row-by-row expressible

        cur = "_st"
        lines = [
            f"# Stateful variable ports ({', '.join(v.name for v in variables)}): evaluated in row order",
            f'{cur} = {in_df}.withColumn("__ord", F.monotonically_increasing_id())',
            '_w = Window.orderBy("__ord")',
            '_wr = Window.orderBy("__ord").rowsBetween(Window.unboundedPreceding, Window.currentRow)',
        ]
        lagged: set = set()

        def prepare(expr: str, owner) -> str:
            """Rewrite references to variables declared below ``owner`` into
            their previous-row columns."""
            for name in sorted(refs(expr) - {owner.name}, key=len, reverse=True):
                if pos[name.upper()] > pos[owner.name.upper()]:
                    lag = f"__prev_{name}"
                    if lag not in lagged:
                        init = self._initial_value(by_name[name.upper()])
                        lines.append(
                            f'{cur} = {cur}.withColumn("{lag}", F.when(F.row_number().over(_w) == 1, {init})'
                            f'.otherwise(F.lag("{name}").over(_w)))'
                        )
                        lagged.add(lag)
                    expr = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])", lag, expr)
            return expr

        def conv(expr: str):
            code, review = self._convert_or_flag(expr)
            if review and _conversion_failed(code, review):
                return None
            return _as_column(code)

        for name in order:
            v = by_name[name.upper()]
            expr = v.expression.strip()
            if not _references_own_port(expr, v.name):
                code = conv(prepare(expr, v))
                if code is None:
                    return None
                lines.append(f'{cur} = {cur}.withColumn("{v.name}", {code})')
                continue
            form = self._recurrence(expr, v.name)
            if form is None:
                return None
            kind, c, x, y = form
            init = self._initial_value(v)
            cx = conv(prepare(x, v)) if x is not None else None
            cy = conv(prepare(y, v)) if y is not None else None
            cc = conv(prepare(c, v)) if c is not None else None
            if (x is not None and cx is None) or (y is not None and cy is None) or (c is not None and cc is None):
                return None
            tag = v.name
            if kind == "sum":
                lines.append(
                    f'{cur} = {cur}.withColumn("{tag}", F.when(F.sum(F.when(({cx}).isNull(), 1).otherwise(0))'
                    f'.over(_wr) > 0, F.lit(None)).otherwise({init} + F.sum({cx}).over(_wr)))'
                )
            elif kind in ("reset", "reset_neg"):
                keep = f"F.coalesce({cc}, F.lit(False))"
                if kind == "reset_neg":
                    keep = f"~F.coalesce({cc}, F.lit(False))"
                lines += [
                    f'{cur} = {cur}.withColumn("__rs_{tag}", ~({keep}))',
                    f'{cur} = {cur}.withColumn("__seg_{tag}", F.sum(F.col("__rs_{tag}").cast("int")).over(_wr))',
                    f'_ws_{tag} = Window.partitionBy("__seg_{tag}").orderBy("__ord")'
                    f'.rowsBetween(Window.unboundedPreceding, Window.currentRow)',
                    f'{cur} = {cur}.withColumn("__x_{tag}", F.when(F.col("__rs_{tag}"), F.lit(0)).otherwise({cx}))',
                    f'{cur} = {cur}.withColumn("__b_{tag}", F.when(F.col("__seg_{tag}") == 0, {init})'
                    f'.otherwise(F.first(F.when(F.col("__rs_{tag}"), {cy})).over(_ws_{tag})))',
                    # NULL arithmetic: one NULL x makes the rest of the segment NULL
                    f'{cur} = {cur}.withColumn("{tag}", F.when(F.sum(F.when(F.col("__x_{tag}").isNull(), 1)'
                    f'.otherwise(0)).over(_ws_{tag}) > 0, F.lit(None))'
                    f'.otherwise(F.col("__b_{tag}") + F.sum("__x_{tag}").over(_ws_{tag})))',
                    f'{cur} = {cur}.drop("__rs_{tag}", "__seg_{tag}", "__x_{tag}", "__b_{tag}")',
                ]
            elif kind == "carry":
                lines += [
                    f'{cur} = {cur}.withColumn("__set_{tag}", ~F.coalesce({cc}, F.lit(False)))',
                    f'{cur} = {cur}.withColumn("{tag}", F.coalesce(F.last(F.when(F.col("__set_{tag}"), {cy}), '
                    f'ignorenulls=True).over(_wr), {init}))',
                    f'{cur} = {cur}.drop("__set_{tag}")',
                ]
        for f in computed_fields:
            if f.direction == DataFlowDirection.VARIABLE or f.expression.strip() == f.name:
                continue
            code = conv(f.expression)
            if code is None:
                return None
            lines.append(f'{cur} = {cur}.withColumn("{f.name}", {code})')
        drops = ["__ord"] + sorted(lagged)
        lines.append(f"{out} = {cur}.drop({', '.join(repr(d) for d in drops)})")
        return lines

    def _recurrence(self, expr: str, name: str):
        """``(kind, cond, x, y)`` for the supported self-references of
        ``name`` (see _stateful_expression), or None."""
        e = expr.strip()
        v = re.escape(name)
        plus = re.compile(rf"^\s*{v}\s*\+\s*(?P<x>.+)$|^\s*(?P<x2>.+?)\s*\+\s*{v}\s*$", re.S)

        def as_sum(s: str):
            m = plus.match(s)
            if not m:
                return None
            x = m.group("x") or m.group("x2")
            return None if _references_own_port(x, name) else x

        x = as_sum(e)
        if x is not None:
            return "sum", None, x, None
        m = re.match(r"^IIF\s*\((?P<body>.*)\)\s*$", e, re.S | re.I)
        if not m:
            return None
        args = self._split_top(m.group("body"))
        if len(args) != 3:
            return None
        c, a, b = args
        if _references_own_port(c, name):
            return None
        if re.fullmatch(rf"\s*{v}\s*", a) and not _references_own_port(b, name):
            return "carry", c, None, b
        xa = as_sum(a)
        if xa is not None and not _references_own_port(b, name):
            return "reset", c, xa, b
        xb = as_sum(b)
        if xb is not None and not _references_own_port(a, name):
            return "reset_neg", c, xb, a
        return None

    @staticmethod
    def _split_lkp_call(expr: str, start: int):
        """``(name, [arg texts], end)`` for the ``:LKP.NAME(...)`` at
        ``start``, or None."""
        m = re.compile(r":LKP\.(\w+)\s*\(", re.IGNORECASE).match(expr, start)
        if not m:
            return None
        depth, i, quote, args, cur = 1, m.end(), None, [], ""
        while i < len(expr):
            ch = expr[i]
            if quote:
                cur += ch
                if ch == quote:
                    quote = None
            elif ch in "'\"":
                quote = ch
                cur += ch
            elif ch == "(":
                depth += 1
                cur += ch
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    if cur.strip() or args:
                        args.append(cur.strip())
                    return m.group(1), args, i + 1
                cur += ch
            elif ch == "," and depth == 1:
                args.append(cur.strip())
                cur = ""
            else:
                cur += ch
            i += 1
        return None

    def _resolve_unconnected_lookups(self, expression: str, current: str,
                                     lookups: dict, cache: dict) -> tuple[str, list[str]]:
        """Replace each ``:LKP.NAME(args)`` with a column the lines join in.

        An unconnected Lookup is called like a function: its INPUT ports
        take the call's arguments in port order and it returns its RETURN
        port (NULL on no match). That is a left join of the lookup on the
        argument values, done here before the port that calls it; the same
        call written three times is joined once. The call used to become
        ``_lookup_NAME(...)``, a helper nothing defined (NameError).
        """
        lines: list[str] = []
        out = []
        i = 0
        while i < len(expression):
            j = expression.upper().find(":LKP.", i)
            if j < 0:
                out.append(expression[i:])
                break
            out.append(expression[i:j])
            call = self._split_lkp_call(expression, j)
            if call is None:
                out.append(expression[j:])
                break
            name, args, end = call
            tx = lookups.get(name.upper())
            key = (name.upper(), tuple(a.upper() for a in args))
            if tx is None:
                out.append(expression[j:end])      # left for the converter to flag
                i = end
                continue
            if key not in cache:
                n = len(cache) + 1
                col = f"__ulkp{n}"
                inputs = [f.name for f in tx.fields if isinstance(f, TransformationField)
                          and f.direction == DataFlowDirection.INPUT and not f.is_lookup]
                if len(inputs) != len(args):
                    out.append(expression[j:end])
                    i = end
                    continue
                port_map = {}
                for k, (port, arg) in enumerate(zip(inputs, args), 1):
                    code, review = self._convert_or_flag(arg)
                    if review and _conversion_failed(code, review):
                        return expression, [f"# {review}"]
                    arg_col = f"{col}_a{k}"
                    lines.append(f'{current} = {current}.withColumn("{arg_col}", {_as_column(code)})')
                    port_map[port] = arg_col
                ret = next((f for f in tx.fields if isinstance(f, TransformationField) and f.is_return), None)
                if ret is None:
                    outs = [f for f in tx.fields if isinstance(f, TransformationField)
                            and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)]
                    ret = outs[0] if len(outs) == 1 else None
                prefix = f"{col}__"
                plan = self._lookup_plan(tx, tx.lookup_table or "", port_map, prefix) if ret else None
                if plan is None:
                    lines.append(f"# REVIEW REQUIRED: unconnected lookup {name} could not be "
                                 f"converted (no RETURN port or an unrecognised condition)")
                    out.append(expression[j:end])
                    i = end
                    continue
                select_parts, condition, order_cols, _drops = plan
                policy, policy_note = self._lookup_policy(tx.lookup_policy)
                lkp_var = f"_lkp{n}"
                table = tx.lookup_table or ""
                lines.append(f"# Unconnected Lookup {name}({', '.join(args)}) -> {ret.name}; {policy_note}")
                if tx.lookup_sql:
                    lines.append(f'{lkp_var} = spark.sql("""{tx.lookup_sql}""")')
                else:
                    lines.append(f'{lkp_var} = spark.table("{table}")')
                if tx.lookup_source_filter and tx.lookup_source_filter.strip():
                    conv = self._convert_where_fragment(
                        tx.lookup_source_filter, {table.split(".")[-1]} if table else set())
                    if conv is None:
                        lines.append(f"# REVIEW REQUIRED: Lookup Source Filter on {name} NOT applied: "
                                     f"{tx.lookup_source_filter.strip()[:120]}")
                    else:
                        lines.append(f"{lkp_var} = {lkp_var}.filter({conv})")
                lines.append(f"{lkp_var} = {lkp_var}.select({', '.join(select_parts)})")
                lines.append("import infa_compat")
                lines.append(
                    f"{current} = infa_compat.lookup_join({current}, {lkp_var}, condition={condition!r}, "
                    f"policy={policy!r}, order_by={order_cols!r})"
                )
                keep = f"{prefix}{ret.name}"
                drop = [c for c in order_cols if c != keep] + list(port_map.values())
                lines.append(f'{current} = {current}.withColumnRenamed("{keep}", "{col}")'
                             + (f".drop({', '.join(repr(d) for d in drop)})" if drop else ""))
                cache[key] = col
            out.append(cache[key])
            i = end
        return "".join(out), lines

    # --------------------------------------------------------------- Filter

    def _filter(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Filter: {tx.name}")
        if tx.filter_condition:
            cond, review = self._convert_or_flag(tx.filter_condition)
            if _conversion_failed(cond, review):
                lines.extend(_refuse_lines(f"Filter {tx.name!r}", review))
                return lines
            if review:
                lines.append(f"# {review}")
            lines.append(f"{out} = {in_df}.filter({cond})")
        else:
            lines.append(f"{out} = {in_df}  # no filter condition found")
        return lines

    # --------------------------------------------------------------- Joiner

    def _joiner(
        self, tx: Transformation, in_df: str, out: str, extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Joiner: {tx.name}")

        # Informatica join types:
        # NORMAL = inner join
        # MASTER OUTER = preserve all master rows (= RIGHT join from detail perspective)
        #   But we put detail.join(master), so Master Outer = LEFT join
        #   because detail is the left side and we preserve the LEFT side
        #   Wait — "Master Outer" means keep ALL rows from Master even if no match
        #   in Detail. Since we do detail.join(broadcast(master)):
        #   - Master Outer → we want to keep all DETAIL rows → LEFT join
        #   Actually in Informatica: Master Outer = keep unmatched master rows
        #   = equivalent to Detail LEFT OUTER from detail perspective
        # DETAIL OUTER = preserve all detail rows
        #   Since detail is left side: this is LEFT join
        # So: MASTER OUTER really means a full outer from the detail side
        #   preserving both, but commonly used as LEFT in practice.
        #
        # Per Informatica docs: Master Outer = return all rows from DETAIL
        # plus matching master rows (same as LEFT from detail perspective).
        # Detail Outer = return all rows from MASTER plus matching detail.
        join_type_map = {
            "NORMAL": "inner",
            "NORMAL JOIN": "inner",
            "INNER": "inner",
            "MASTER OUTER": "left",     # Detail is left, preserve all detail rows
            "MASTER OUTER JOIN": "left",
            "LEFT": "left",
            "DETAIL OUTER": "right",    # Preserve all master rows
            "DETAIL OUTER JOIN": "right",
            "RIGHT": "right",
            "FULL OUTER": "full",
            "FULL OUTER JOIN": "full",
            "FULL": "full",
        }
        spark_join = join_type_map.get((tx.join_type or "").upper(), "inner")

        # Determine master and detail DataFrames. The actual master/detail
        # *resolution* (from the "Master Source" property or the port-level
        # MASTER/ISMASTER flag) happens upstream in notebook_generator.py,
        # which has access to the mapping's connectors and can classify each
        # predecessor -- this method only consumes the result. `extra`
        # carries the resolved master predecessor's DataFrame under its own
        # name UNLESS resolution failed, in which case notebook_generator
        # sets the "__master_unresolved__" sentinel instead of a real
        # predecessor entry.
        extra = dict(extra)  # local copy -- never mutate the caller's dict
        master_unresolved = extra.pop("__master_unresolved__", None) is not None
        # The other input, when the master side could not be identified. Only
        # usable for a symmetric (inner) join -- see below.
        unordered_second = extra.pop("__unordered_second__", None)
        # {port: "master"|"detail"} from the mapping's CONNECTORs -- which
        # DataFrame actually carries each port, rather than which side the
        # condition's author wrote first.
        port_sides = extra.pop("__port_sides__", None) or {}
        master_source = get_ci(tx.properties, "Master Source", "master_source")
        detail_df = in_df
        master_df = next(iter(extra.values())) if extra else None

        # An inner join is symmetric, so an unresolved master/detail side
        # does not stop it: the same rows come out either way. Only an OUTER
        # join depends on which side is which, and that is the case worth
        # refusing. Previously ANY unresolved master lost the join, which is
        # silent data loss on exports that simply carry no MASTER flag.
        if master_df is None and unordered_second and spark_join == "inner":
            master_df = unordered_second
            lines.append(
                f"# Master/detail side was not identified in the export (no "
                f"port-level MASTER flag, no Master Source property). This is "
                f"an INNER join, which is symmetric -- the same rows result "
                f"either way -- so it is applied. REVIEW: if this was meant "
                f"to be an outer join, the join TYPE in the export is wrong, "
                f"and direction would then matter."
            )

        if master_df is None:
            # No distinguishable second DataFrame was resolved as master.
            # Per spec, do not guess: silently defaulting to a hardcoded
            # df name here previously joined against whatever variable
            # happened to have that name -- not necessarily one of this
            # Joiner's actual inputs -- which can silently invert a
            # Master/Detail Outer Join. Skip the join and flag it instead.
            reason = (
                "no port-level MASTER/ISMASTER flag or Master Source "
                "property resolved a second input"
                if master_unresolved
                else "only one input DataFrame was available"
            )
            lines.append(
                f"# REVIEW REQUIRED: Joiner '{tx.name}' master/detail side "
                f"could not be determined ({reason}) -- join skipped, "
                "resolve manually before running this notebook."
            )
            lines.append(f"{out} = {detail_df}")
            return lines

        lines.append(f"# Join type: {spark_join} | Master: {master_source or 'unknown'}")

        if tx.join_condition:
            # The condition names the Joiner's OWN ports; the notebook
            # generator has renamed each side's columns to those port
            # names before this cell (consumer-side connector renames), so
            # the ports are used verbatim. The previous suffix stripping
            # (_M/_D/_1/_2 -> base name) guessed at a naming convention and
            # joined on columns that did not exist when the convention did
            # not hold.
            join_conds = self._parse_joiner_condition(tx.join_condition, tx)
            # Re-orient each pair so the master port is the one the MASTER
            # DataFrame carries. The written order and the MASTER flags are
            # both conventions an export can break; the connector wiring is
            # the fact. Pairs whose sides are unknown are left as parsed.
            if port_sides:
                oriented = []
                for a, b in join_conds:
                    sa, sb = port_sides.get(a), port_sides.get(b)
                    if sa == "detail" and sb == "master":
                        oriented.append((b, a))
                    else:
                        oriented.append((a, b))
                join_conds = oriented

            if len(join_conds) == 1 and join_conds[0][0] == join_conds[0][1]:
                # Same column name in both tables — use simple string join
                col = join_conds[0][0]
                lines.append(
                    f'{out} = {detail_df}.join(\n'
                    f'    F.broadcast({master_df}),\n'
                    f'    on="{col}",\n'
                    f'    how="{spark_join}"\n'
                    f')'
                )
            elif join_conds:
                # Build explicit join condition
                cond_parts = []
                for master_col, detail_col in join_conds:
                    cond_parts.append(
                        f'{detail_df}["{detail_col}"] == {master_df}["{master_col}"]'
                    )
                cond_str = " & ".join(f"({p})" for p in cond_parts) if len(cond_parts) > 1 else cond_parts[0]
                lines.append(
                    f'{out} = {detail_df}.join(\n'
                    f'    F.broadcast({master_df}),\n'
                    f'    on={cond_str},\n'
                    f'    how="{spark_join}"\n'
                    f')'
                )
            else:
                cond, review = self._convert_or_flag(tx.join_condition)
                if review:
                    lines.append(f"# {review}")
                lines.append(
                    f'{out} = {detail_df}.join(F.broadcast({master_df}), on={cond}, how="{spark_join}")'
                )
        else:
            lines.append(
                f'{out} = {detail_df}.join(F.broadcast({master_df}), how="{spark_join}")'
                f"  # TODO: add join condition"
            )
        return lines

    @staticmethod
    def _parse_joiner_condition(
        condition: str, tx: Optional[Transformation] = None
    ) -> list[tuple[str, str]]:
        """Parse an Informatica Joiner condition into (master_port, detail_port)
        pairs, port names verbatim.

        The Designer writes a condition as ``master_port = detail_port``;
        when the Joiner's fields carry MASTER flags those decide which side
        is which regardless of the order written.

        Examples:
            "PRODUCT_ID_M = PRODUCT_ID_D" -> [("PRODUCT_ID_M", "PRODUCT_ID_D")]
            "ORDER_ID1 = ORDER_ID"       -> [("ORDER_ID1", "ORDER_ID")]
        """
        master_ports = set()
        if tx is not None:
            master_ports = {
                f.name for f in tx.fields
                if isinstance(f, TransformationField) and f.is_master
            }
        pairs = []
        parts = [p.strip() for p in re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE)]
        for part in parts:
            if "=" not in part:
                continue
            left, right = (s.strip() for s in part.split("=", 1))
            if master_ports and right in master_ports and left not in master_ports:
                left, right = right, left
            pairs.append((left, right))
        return pairs

    # --------------------------------------------------------------- Lookup

    def _lookup(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Lookup: {tx.name}")

        lookup_table = tx.lookup_table or tx.properties.get("lookup_table", "")
        is_connected = tx.properties.get("connection_type", "connected").lower() == "connected"
        lkp_var = f"lkp_{_safe_var(tx.name)}"

        # The mapping-level connectors feeding this Lookup's input ports:
        # {lookup input port: upstream column name}. Informatica's lookup
        # condition names the lookup's OWN input port (IN_CUST_ID), but the
        # pipeline DataFrame carries the upstream column (CUST_ID) -- the
        # generator resolves that here. Without it the join was on a column
        # that did not exist in the pipeline.
        port_map: dict[str, str] = dict(_extra.get("__port_map__", {}) or {})

        if tx.lookup_dynamic:
            lines.append(
                f"# REVIEW REQUIRED: Lookup '{tx.name}' uses a DYNAMIC lookup cache "
                f"(the cache is updated with rows the session inserts, and "
                f"NewLookupRow drives insert-vs-update). A static join below does "
                f"NOT reproduce that: a key that appears twice in one run is looked "
                f"up against the target as it was BEFORE the run both times. "
                f"Deduplicate the source on the lookup key first, or rewrite as a "
                f"MERGE. Do not run this notebook until this is resolved."
            )

        if tx.lookup_sql:
            # SQL Override provided — use it (handles complex queries like MAX date dedup)
            sql = " ".join(tx.lookup_sql.split())
            lines.append("# Lookup SQL Override (source-database SQL, run as Spark SQL --")
            lines.append("# REVIEW REQUIRED: check Oracle-only syntax and catalog-qualify the table):")
            lines.append(f'# {sql[:120]}')
            lines.append(f'{lkp_var} = spark.sql("""{tx.lookup_sql}""")')
        elif lookup_table:
            lines.append(f'{lkp_var} = spark.table("{lookup_table}")')
        else:
            lines.append(
                f'{lkp_var} = spark.table("UNKNOWN")  # TODO: set lookup table'
            )

        if tx.lookup_source_filter and tx.lookup_source_filter.strip():
            converted = self._convert_where_fragment(
                tx.lookup_source_filter, {lookup_table.split(".")[-1]} if lookup_table else set()
            )
            if converted is None:
                lines.append(
                    f"# REVIEW REQUIRED: Lookup Source Filter on '{tx.name}' uses SQL "
                    f"F.expr() cannot evaluate -- NOT applied: {tx.lookup_source_filter.strip()[:120]}"
                )
            else:
                lines.append(f"# Lookup Source Filter: {tx.lookup_source_filter.strip()[:120]}")
                lines.append(f"{lkp_var} = {lkp_var}.filter({converted})")

        if _extra.get("__snapshot__"):
            # The lookup source is also a target of this mapping. Informatica
            # builds a static cache BEFORE the session writes; Spark reads
            # lazily, so a later target write would otherwise see rows an
            # earlier write in this notebook just made.
            lines.append(f"{lkp_var} = {lkp_var}.localCheckpoint()  # static cache: snapshot before any write")

        policy, policy_note = self._lookup_policy(tx.lookup_policy)

        plan = self._lookup_plan(tx, lookup_table, port_map, lookup_prefix(tx)) if (
            is_connected and tx.lookup_condition) else None
        if plan is not None and tx.lookup_dynamic:
            dyn = self._dynamic_lookup(tx, in_df, out, lkp_var, port_map, plan, lookup_prefix(tx))
            if dyn is not None:
                # The static-cache warning emitted above does not apply.
                lines = [ln for ln in lines if "DYNAMIC lookup cache" not in ln]
                return lines + dyn
        if plan is not None:
            select_parts, condition, order_cols, drops = plan
            lines.append(f"{lkp_var} = {lkp_var}.select({', '.join(select_parts)})")
            lines.append(f"# Lookup condition: {tx.lookup_condition}")
            lines.append(f"# Lookup policy on multiple match: {policy_note}")
            lines.append("import infa_compat")
            lines.append(
                f"{out} = infa_compat.lookup_join({in_df}, {lkp_var}, condition={condition!r}, "
                f"policy={policy!r}, order_by={order_cols!r})"
            )
            if drops:
                lines.append(f"{out} = {out}.drop({', '.join(repr(d) for d in drops)})")
            return lines

        if is_connected and tx.lookup_condition:
            # Parse lookup condition into join keys, pre-filters, and return columns
            join_keys, filter_parts, lkp_return_cols = self._parse_lookup_condition(
                tx.lookup_condition, tx, lookup_table
            )
            # Resolve lookup input ports to the pipeline's column names.
            join_keys = [(lkp_col, port_map.get(src_col, src_col)) for lkp_col, src_col in join_keys]

            # Apply pre-filters to lookup table (e.g., CURRENT_FLAG = 'Y')
            if filter_parts:
                filter_str = " & ".join(filter_parts)
                lines.append(f"{lkp_var} = {lkp_var}.filter({filter_str})")

            # Project the lookup table to its join keys plus the Lookup's
            # output ports, each under its port name (LKP_DEPT, CUSTOMER_SK).
            # Whenever the ports are known -- also when the only output is the
            # key itself (an existence check) -- so no other lookup-table
            # column reaches the pipeline.
            if (lkp_return_cols or tx.fields) and (join_keys or lkp_return_cols):
                rename_parts = []
                for orig_col, port in lkp_return_cols:
                    rename_parts.append(
                        f'F.col("{orig_col}")' if orig_col == port
                        else f'F.col("{orig_col}").alias("{port}")'
                    )
                # Also keep the join key columns (unaliased) for the join itself
                join_col_names = [jk[0] for jk in join_keys]
                select_parts = [f'F.col("{c}")' for c in join_col_names]
                select_parts.extend(rename_parts)
                lines.append(
                    f"{lkp_var} = {lkp_var}.select({', '.join(select_parts)})"
                )

            # Multiple-match policy (which lookup row wins when more than
            # one lookup row shares a key) is Class C -- call
            # infa_compat.cached_lookup() instead of a hand-rolled broadcast
            # join + manual dedup. It also owns the left-join / NULL-key
            # pass-through semantics this used to build by hand.
            if join_keys:
                on_cols = []
                for lkp_col, src_col in join_keys:
                    if lkp_col != src_col:
                        # cached_lookup joins on a name common to BOTH
                        # sides -- rename the lookup-side join key to the
                        # source-side port name first, since Informatica's
                        # own condition often names them differently (e.g.
                        # "CURRENT.EMP_ID" vs "SQ_EMPLOYEES.EMP_ID").
                        lines.append(
                            f'{lkp_var} = {lkp_var}.withColumnRenamed("{lkp_col}", "{src_col}")'
                        )
                    on_cols.append(src_col)
                lines.append(f"# Lookup policy on multiple match: {policy_note}")
                lines.append("import infa_compat")
                lines.append(
                    f'{out} = infa_compat.cached_lookup({in_df}, {lkp_var}, '
                    f'on={on_cols!r}, policy={policy!r})'
                )
            else:
                lines.append(
                    f'{out} = {in_df}.join(F.broadcast({lkp_var}), how="left")'
                    f"  # TODO: add lookup condition"
                )
        elif is_connected:
            lines.append(
                f'{out} = {in_df}.join(F.broadcast({lkp_var}), how="left")'
                f"  # TODO: add lookup condition"
            )
        else:
            lines.append(f"# Unconnected lookup -- consider UDF or broadcast join")
            lines.append(
                f'{out} = {in_df}.join(F.broadcast({lkp_var}), how="left")'
                f"  # TODO: convert unconnected lookup logic"
            )

        return lines

    _LKP_TERM = re.compile(r"^\s*(?P<l>.+?)\s*(?P<op><=|>=|<>|!=|=|<|>)\s*(?P<r>.+?)\s*$")
    _MIRROR = {"<": ">", ">": "<", "<=": ">=", ">=": "<=", "=": "=", "<>": "<>", "!=": "<>"}

    def _lookup_plan(self, tx: Transformation, lookup_table: str, port_map: dict, prefix: str):
        """``(select parts, condition, order_by, drop)`` for a Lookup whose
        condition names its own ports, or None to use the older path.

        Informatica's condition is ``<lookup port> <op> <input port>``; the
        lookup ports are the lookup source's columns. Every lookup port is
        selected under ``<prefix><port>`` so it cannot collide with a
        pipeline column -- two lookups returning SEGMENT, or a lookup on
        the target returning CUST_NAME next to the source's CUST_NAME,
        were AMBIGUOUS_REFERENCE -- and a condition column can be returned
        too (``ISNULL(LKP_PRODUCT_ID)``, the usual insert test, used to
        lose the key it tested). The connectors rename the returned ones.
        """
        fields = [f for f in tx.fields if isinstance(f, TransformationField)]
        flagged = any(f.is_lookup for f in fields)
        if flagged:
            lookup_ports = [f for f in fields if f.is_lookup]
            input_ports = {f.name.upper(): f.name for f in fields if not f.is_lookup
                           and f.direction == DataFlowDirection.INPUT}
        else:
            input_ports = {f.name.upper(): f.name for f in fields
                           if f.direction == DataFlowDirection.INPUT}
            lookup_ports = [f for f in fields if f.direction in (
                DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)]
        by_name = {f.name.upper(): f for f in lookup_ports}
        if not lookup_ports:
            return None

        def column_of(f: TransformationField) -> str:
            # A real export's lookup port IS the lookup column. Fixtures
            # without LOOKUP flags follow the older convention: a
            # TABLE.COLUMN expression, or an LKP_ prefix on the port.
            if not flagged:
                if f.expression and "." in f.expression:
                    return f.expression.split(".")[-1]
                if f.name.upper().startswith("LKP_"):
                    return f.name[4:]
            return f.name

        lookup_names = {tx.name.upper()}
        if lookup_table:
            lookup_names |= {lookup_table.upper(), lookup_table.split(".")[-1].upper()}

        terms = []
        extra_ports: list[str] = []
        parts = [p.strip() for p in re.split(r"\s+AND\s+", tx.lookup_condition.strip(), flags=re.IGNORECASE)]
        for part in parts:
            m = self._LKP_TERM.match(part)
            if not m:
                return None
            op = m.group("op")
            lq, left = self._split_qualifier(m.group("l"))
            rq, right = self._split_qualifier(m.group("r"))
            lit = re.compile(r"^('.*'|-?\d+(\.\d+)?)$")

            def is_lookup_side(q: str, n: str) -> bool:
                return n.upper() in by_name and (not q or q.upper() in lookup_names
                                                 or n.upper() not in input_ports)

            if is_lookup_side(lq, left) and not is_lookup_side(rq, right):
                lport, other = left, right
            elif is_lookup_side(rq, right) and not is_lookup_side(lq, left):
                lport, other, op = right, left, self._MIRROR[op]
            else:
                return None
            if op == "!=":
                op = "<>"
            if lit.match(other):
                rhs = other
            elif other.upper() in input_ports:
                src = port_map.get(input_ports[other.upper()], input_ports[other.upper()])
                rhs = f"`{src}`"
            else:
                return None
            port = by_name[lport.upper()].name
            if port not in extra_ports:
                extra_ports.append(port)
            terms.append(f"`{prefix}{port}` {op} {rhs}")

        select_parts = [f'F.col("{column_of(f)}").alias("{prefix}{f.name}")' for f in lookup_ports]
        order_cols = [f"{prefix}{f.name}" for f in lookup_ports]
        returned = {f.name for f in lookup_ports if f.direction in (
            DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)}
        drops = [f"{prefix}{f.name}" for f in lookup_ports if f.name not in returned]
        return select_parts, " AND ".join(terms), order_cols, drops

    def _dynamic_lookup(self, tx, in_df, out, lkp_var, port_map, plan, prefix):
        """A dynamic lookup cache with Insert Else Update, as row-order
        windows, or None when the lookup is outside that form.

        The cache starts as the lookup table and is updated by every row the
        lookup flags, so for each key, in row order:

        - first occurrence: not in the table -> NewLookupRow 1 (inserted);
          in the table with a different value in an associated port -> 2
          (updated); identical -> 0;
        - later occurrences compare with the PREVIOUS occurrence's values,
          which is what the cache holds by then -- a key arriving twice is
          inserted once and then 0 (or 2), never inserted twice.

        The lookup's output ports carry the cache after the row (the
        incoming values), as with Output Old Value On Update = NO. The
        condition must be equalities on input ports; each lookup port's
        associated input is the one the condition pairs it with, else the
        input port named IN_<port> or <port>.
        """
        fields = [f for f in tx.fields if isinstance(f, TransformationField)]
        if str(get_ci(tx.properties, "update else insert", default="NO")).upper() == "YES" and \
                str(get_ci(tx.properties, "insert else update", default="NO")).upper() != "YES":
            return None
        if str(get_ci(tx.properties, "output old value on update", default="NO")).upper() == "YES":
            return None
        inputs = {f.name.upper(): f.name for f in fields if not f.is_lookup
                  and f.direction == DataFlowDirection.INPUT}
        lookups = [f for f in fields if f.is_lookup]
        newrow = next((f for f in fields if f.name.upper() == "NEWLOOKUPROW"), None)
        if not lookups or newrow is None:
            return None
        keys = []
        for part in re.split(r"\s+AND\s+", tx.lookup_condition.strip(), flags=re.IGNORECASE):
            m = self._LKP_TERM.match(part)
            if not m or m.group("op") != "=":
                return None
            a, b = self._split_qualifier(m.group("l"))[1], self._split_qualifier(m.group("r"))[1]
            lk = next((f.name for f in lookups if f.name.upper() in (a.upper(), b.upper())), None)
            ip = next((inputs[x.upper()] for x in (a, b) if x.upper() in inputs), None)
            if lk is None or ip is None:
                return None
            keys.append((lk, port_map.get(ip, ip)))
        assoc = {}
        for f in lookups:
            key_in = next((col for lk, col in keys if lk == f.name), None)
            ip = inputs.get(f"IN_{f.name}".upper()) or inputs.get(f.name.upper())
            assoc[f.name] = key_in or (port_map.get(ip, ip) if ip else None)
        select_parts = plan[0]
        key_cols = [col for _lk, col in keys]
        compared = [f.name for f in lookups if assoc[f.name] and f.name not in {lk for lk, _ in keys}]
        lines = [
            f"# Dynamic lookup cache ({tx.name}): Insert Else Update over the row order on entry",
            f"{lkp_var} = {lkp_var}.select({', '.join(select_parts)})",
            f'_dl = {in_df}.withColumn("__ord", F.monotonically_increasing_id())',
            "import infa_compat",
            f"_dl = infa_compat.lookup_join(_dl, {lkp_var}, condition={plan[1]!r}, policy='first', "
            f"order_by={plan[2]!r})",
            f"_dlw = Window.partitionBy({', '.join(repr(c) for c in key_cols)}).orderBy('__ord')",
            '_dl = _dl.withColumn("__first", F.row_number().over(_dlw) == 1)',
            f'_dl = _dl.withColumn("__in_table", F.col("{prefix}{keys[0][0]}").isNotNull())',
        ]
        diffs = []
        for p in compared:
            col = assoc[p]
            lines.append(
                f'_dl = _dl.withColumn("__was_{p}", F.when(F.col("__first"), F.col("{prefix}{p}"))'
                f'.otherwise(F.lag("{col}").over(_dlw)))'
            )
            diffs.append(f'~F.col("{col}").eqNullSafe(F.col("__was_{p}"))')
        changed = " | ".join(diffs) if diffs else "F.lit(False)"
        lines.append(
            f'_dl = _dl.withColumn("{prefix}{newrow.name}", F.when(F.col("__first") & ~F.col("__in_table"), F.lit(1))'
            f'.when({changed}, F.lit(2)).otherwise(F.lit(0)))'
        )
        for f in lookups:
            if assoc[f.name]:
                lines.append(f'_dl = _dl.withColumn("{prefix}{f.name}", F.col("{assoc[f.name]}"))')
        drops = ["__ord", "__first", "__in_table"] + [f"__was_{p}" for p in compared]
        lines.append(f"{out} = _dl.orderBy('__ord').drop({', '.join(repr(d) for d in drops)})")
        return lines

    @staticmethod
    def _lookup_policy(raw: str) -> tuple[str, str]:
        """Map the export's "Lookup policy on multiple match" to
        ``infa_compat.cached_lookup``'s policy vocabulary, with the note the
        generated cell carries.

        Use First Value -> "first"; Use Last Value -> "last"; Report Error
        -> "error" (the session stops, as Informatica's does); Use Any Value
        -> "first" (documented assumption in infa_compat.lookup). An absent
        property is Informatica's default, Use First Value.
        """
        text = (raw or "").strip().lower()
        if not text:
            return "first", 'export does not set it; Informatica default "Use First Value"'
        if "last" in text:
            return "last", f"{raw.strip()!r}"
        if "error" in text:
            return "error", f"{raw.strip()!r} -- the job FAILS if a key matches more than one lookup row, as the session did"
        if "any" in text:
            return "first", f"{raw.strip()!r} -- treated as Use First Value (documented assumption, see infa_compat.lookup)"
        if "first" in text:
            return "first", f"{raw.strip()!r}"
        return "first", f"unrecognised value {raw.strip()!r} -- treated as Use First Value; REVIEW REQUIRED"

    @staticmethod
    def _split_qualifier(side: str) -> tuple[str, str]:
        """``"SQ_X.COL"`` -> ``("SQ_X", "COL")``; an unqualified name, a
        string literal or a number comes back with an empty qualifier."""
        s = side.strip()
        if s.startswith("'") or re.fullmatch(r"-?\d+(\.\d+)?", s) or "." not in s:
            return "", s
        qualifier, _, col = s.rpartition(".")
        return qualifier.strip(), col.strip()

    def _parse_lookup_condition(
        self, condition: str, tx: Transformation, lookup_table: str
    ) -> tuple[list[tuple[str, str]], list[str], list[str]]:
        """Parse lookup condition into join keys, pre-filters, and return columns.

        Informatica lookup conditions use LKP_ prefixed port names for the
        lookup table columns. We strip LKP_ to get the actual column name.

        Examples:
            LKP_CUSTOMER_ID = CUSTOMER_ID  → join key: ("CUSTOMER_ID", "CUSTOMER_ID")
            LKP_CURRENT_FLAG = 'Y'         → pre-filter: CURRENT_FLAG = 'Y'

        Returns:
            (join_keys, filter_conditions, return_columns)
            - join_keys: list of (lookup_col, source_col) tuples
            - filter_conditions: list of PySpark filter strings
            - return_columns: list of (lookup_col, output port name) tuples
        """
        join_keys = []     # (lookup_actual_col, source_col)
        filter_parts = []
        join_key_cols = set()  # track which lookup cols are join keys

        # The Lookup's output ports, as (lookup-table column, port name).
        # EVERY output port, not only LKP_-prefixed ones: Informatica returns
        # exactly the Lookup's connected ports, and the projection built from
        # this list is what keeps the rest of the lookup table out of the
        # pipeline. With only the prefixed ports collected, a Lookup whose
        # ports are named plainly (CUSTOMER_SK, REGION) joined the WHOLE
        # table -- two such lookups on AIDP collided on AMBIGUOUS_REFERENCE,
        # and any shared audit column (LOAD_DATE, ETL_BATCH_ID) would too.
        lkp_output_fields = []
        for f in tx.fields:
            if isinstance(f, TransformationField) and f.direction in (
                    DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT):
                # Column from EXPRESSION when it names one
                # (EXPRESSION="ICD_CODES.ICD_DESCRIPTION" -> ICD_DESCRIPTION),
                # else the port name, LKP_ prefix stripped by convention.
                if f.expression and "." in f.expression:
                    actual = f.expression.split(".")[-1]
                elif f.name.upper().startswith("LKP_"):
                    actual = f.name[4:]
                else:
                    actual = f.name
                lkp_output_fields.append((actual, f.name))

        # Split condition by AND
        parts = [p.strip() for p in condition.replace(" AND ", " and ").split(" and ")]
        for part in parts:
            part = part.strip()
            if "=" not in part:
                continue

            sides = part.split("=", 1)
            left = sides[0].strip()
            right = sides[1].strip()

            right_is_literal = (
                (right.startswith("'") and right.endswith("'"))
                or right.replace(".", "").isdigit()
            )
            left_is_literal = (
                (left.startswith("'") and left.endswith("'"))
                or left.replace(".", "").isdigit()
            )

            # A side may be qualified with an instance name
            # (LKP_CURRENT.EMP_ID = SQ_EMPLOYEES.EMP_ID). The qualifier says
            # which side is which -- the Lookup's own name marks the lookup
            # column -- and must be stripped before anything else: the
            # LKP_-prefix rule below used to fire on the QUALIFIER, turning
            # LKP_CURRENT.EMP_ID into CURRENT.EMP_ID, which Spark reads as a
            # struct field and cannot resolve (seen on AIDP 2026-09-25).
            lq, left = self._split_qualifier(left)
            rq, right = self._split_qualifier(right)
            # Either the Lookup's own name or its lookup table's name marks
            # the lookup side (exports and hand-written conditions use both).
            lookup_names = {tx.name.upper()}
            if lookup_table:
                lookup_names |= {lookup_table.upper(), lookup_table.split(".")[-1].upper()}
            lq_is_lkp, rq_is_lkp = lq.upper() in lookup_names, rq.upper() in lookup_names

            def _bare(col: str, qualifier: str) -> str:
                # The LKP_ port-naming convention applies to unqualified
                # names only; a qualified name is already the column.
                return col[4:] if not qualifier and col.upper().startswith("LKP_") else col

            if right_is_literal:
                filter_parts.append(f"F.col('{_bare(left, lq)}') == F.lit({right})")
            elif left_is_literal:
                filter_parts.append(f"F.col('{_bare(right, rq)}') == F.lit({left})")
            else:
                # Both columns — this is a join key
                if lq_is_lkp != rq_is_lkp:
                    # The qualifier names the lookup side outright.
                    lkp_col, src_col = (left, right) if lq_is_lkp else (right, left)
                else:
                    lkp_col = _bare(left, lq)
                    src_col = _bare(right, rq)
                    # If left was the source side and right was LKP_, swap
                    if (not rq and right.upper().startswith("LKP_")
                            and not (not lq and left.upper().startswith("LKP_"))):
                        lkp_col, src_col = src_col, lkp_col
                join_keys.append((lkp_col, src_col))
                join_key_cols.add(lkp_col.upper())

        # Return columns = lookup output ports that are NOT join keys
        return_columns = [
            (col, port) for col, port in lkp_output_fields
            if col.upper() not in join_key_cols
        ]

        return join_keys, filter_parts, return_columns

    # ----------------------------------------------------------- Aggregator

    def _aggregator(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Aggregator: {tx.name}")

        group_fields = tx.group_by_fields or []

        # Build group-by column list
        if group_fields:
            group_cols = ", ".join(f'F.col("{g}")' for g in group_fields)
        else:
            group_cols = ""

        # Collect aggregate expressions from output fields
        agg_exprs: list[str] = []
        passthrough_review: list[str] = []
        order_review: list[tuple[str, str]] = []
        for f in tx.fields:
            if not isinstance(f, TransformationField):
                continue
            if f.direction == DataFlowDirection.INPUT:
                continue
            if f.name in group_fields:
                continue
            if f.expression and not _AGGREGATE_FN.search(f.expression):
                # EXPRESSION="REGION_MGR" (a real export writes the port's own
                # name) or any expression with no aggregate function: it is
                # evaluated on the group's LAST row, like a passthrough. Put
                # bare inside .agg() it failed MISSING_AGGREGATION.
                pyspark_expr, review = self._convert_or_flag(f.expression)
                if review:
                    lines.append(f"# {review}")
                passthrough_review.append(f.name)
                agg_exprs.append(f'F.last({pyspark_expr}).alias("{f.name}")')
            elif f.expression:
                pyspark_expr, review = self._convert_or_flag(f.expression)
                if review:
                    lines.append(f"# {review}")
                order_dependent = _ORDER_DEPENDENT_AGG.search(f.expression or "")
                if order_dependent:
                    order_review.append((f.name, order_dependent.group(1).upper()))
                agg_exprs.append(f'{pyspark_expr}.alias("{f.name}")')
            else:
                # A passthrough port: neither a group-by key nor an
                # aggregate. Informatica emits the LAST row of each group
                # for these, because it processes rows in arrival order.
                # Spark has no arrival order, so F.last without an ORDER BY
                # picks an arbitrary row within the group and can return a
                # different value on every run -- including a different one
                # per column, so the output row need not correspond to any
                # single source row.
                #
                # F.last rather than F.first because it matches what
                # Informatica meant, so adding a Window.orderBy later makes
                # it correct rather than merely different. The marker is the
                # point: this cannot be resolved from the export, because
                # the ordering column is a property of the data.
                passthrough_review.append(f.name)
                agg_exprs.append(f'F.last(F.col("{f.name}")).alias("{f.name}")')

        for field_name, fn in order_review:
            lines.append(
                f"# REVIEW REQUIRED: {field_name} uses {fn}, which is defined by "
                f"ROW ORDER. Informatica processes rows in arrival order; Spark is "
                f"unordered and partitioned, so this returns an arbitrary value "
                f"that can change between runs. Add an explicit "
                f"Window.partitionBy(group keys).orderBy(<a real ordering column>). "
                f"If the source has no such column, the result cannot be made "
                f"deterministic and the original logic depended on something the "
                f"export does not carry."
            )

        if passthrough_review:
            names = ", ".join(sorted(passthrough_review))
            lines.append(
                f"# REVIEW REQUIRED: Aggregator '{tx.name}' passes through "
                f"{names} without grouping or aggregating. Informatica returns "
                f"the LAST row of each group for these; Spark has no row order, "
                f"so F.last picks an arbitrary row and may differ per run and "
                f"per column. Replace with F.last(col, ignorenulls=True) over a "
                f"Window.partitionBy(group keys).orderBy(<a real ordering "
                f"column>), or confirm the value is constant within each group."
            )

        if group_cols and agg_exprs:
            agg_str = ",\n    ".join(agg_exprs)
            lines.append(
                f"{out} = {in_df}.groupBy({group_cols}).agg(\n"
                f"    {agg_str}\n)"
            )
        elif agg_exprs:
            # No GROUP BY key was found for an Aggregator that DOES have
            # aggregate expressions. A global aggregation (Informatica's own
            # no-GROUP-BY semantic -- see Tier 1 item 3) can be
            # legitimate, so this must not refuse to generate. But silently
            # collapsing what may have been a per-group aggregation into one
            # global row is exactly the defect -- so make it visible
            # with the same REVIEW REQUIRED marker the rest of this module
            # uses, instead of emitting the global `.agg()` with no comment.
            lines.append(
                f"# REVIEW REQUIRED: Aggregator '{tx.name}' has aggregate "
                f"expressions but no GROUP BY key -- this produces ONE "
                f"GLOBAL ROW, not a per-group aggregation. Verify this is "
                f"intentional; if a group key was expected, check the "
                f"source export for an explicit group-by flag."
            )
            agg_str = ",\n    ".join(agg_exprs)
            lines.append(f"{out} = {in_df}.agg(\n    {agg_str}\n)")
        else:
            lines.append(
                f"{out} = {in_df}.groupBy({group_cols}).count()"
                f"  # TODO: add aggregate expressions"
            )

        return lines

    # --------------------------------------------------------------- Router

    def _router(
        self, tx: Transformation, in_df: str, _out: str, _extra: dict
    ) -> list[str]:
        """Router -- fan a single input into one DataFrame per group.

        A group with no condition is Informatica's DEFAULT group: it gets
        the rows that matched none of the other groups, not "all rows"
        (that earlier behavior silently duplicated every row into every
        group -- a data-correctness bug, not just a stub).
        """
        lines: list[str] = []
        lines.append(f"# Router: {tx.name}")
        lines.append(f"# Creates multiple output DataFrames from filter conditions")

        if not tx.router_groups:
            lines.append("# TODO: no router groups found -- check transformation XML")
            return lines

        conditioned: list[tuple[str, str]] = []  # (group_name, pyspark_condition)
        defaults: list[str] = []  # group name(s) with no condition

        # DataFrame names use the same normalisation the notebook generator
        # uses when it wires a group to its consumers (df_<lower-case,
        # non-alphanumerics -> _>). Emitting the raw group name here while
        # the generator expected the normalised one meant every Router
        # with an upper-case group name (GRP_INSERT -- i.e. every real
        # export) produced a NameError at the first consumer.
        for i, group in enumerate(tx.router_groups):
            group_name = group.get("name", f"group_{i}")
            df_name = f"df_{_safe_var(group_name)}"
            condition = (group.get("condition") or "").strip()
            if not condition:
                defaults.append(group_name)
                continue
            # Handle trivial "all rows" conditions: 1=1, TRUE, etc.
            trivial = condition.replace(" ", "") in ("1=1", "TRUE")
            if trivial:
                lines.append(f'{df_name} = {in_df}  # All rows (condition: {condition})')
                conditioned.append((group_name, "F.lit(True)"))
            else:
                cond, review = self._convert_or_flag(condition)
                if _conversion_failed(cond, review):
                    # See _REFUSAL_NOTE: .filter(F.lit(None)) is FALSE for
                    # every row, so this group would silently get nothing.
                    lines.extend(_refuse_lines(
                        f"Router group {group_name!r} on {tx.name}", review))
                    continue
                if review:
                    lines.append(f"# {review}")
                lines.append(f'{df_name} = {in_df}.filter({cond})')
                conditioned.append((group_name, cond))

        if len(defaults) == 1:
            default_name = defaults[0]
            lines.append(
                f"# Default group: {default_name} -- rows matching none of the "
                f"conditioned groups above"
            )
            if conditioned:
                # A row reaches DEFAULT when no group condition is TRUE -- a
                # NULL condition is "not true". ~(c1 | c2) is NULL whenever a
                # condition is NULL and filter() drops the row, so a NULL
                # country fell out of the Router entirely.
                negation = " | ".join(f"F.coalesce({cond}, F.lit(False))" for _, cond in conditioned)
                lines.append(f'df_{_safe_var(default_name)} = {in_df}.filter(~({negation}))')
            else:
                lines.append(f"df_{_safe_var(default_name)} = {in_df}")
        elif len(defaults) > 1:
            # Informatica allows exactly one DEFAULT group per Router. More
            # than one with no condition is malformed input -- flag it for
            # a human rather than guessing which one is authoritative.
            lines.append(
                "# REVIEW REQUIRED: more than one Router group has no "
                f"condition ({', '.join(defaults)}) -- Informatica allows "
                "only one DEFAULT group per Router. Resolve manually; "
                "do not run this notebook until this is fixed."
            )
            for group_name in defaults:
                lines.append(
                    f"df_{_safe_var(group_name)} = {in_df}"
                    f"  # REVIEW REQUIRED: ambiguous default group"
                )

        return lines

    # ------------------------------------------------- Sequence Generator

    def _sequence_generator(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Sequence Generator: {tx.name}")

        seq_field = next(
            (f.name for f in tx.fields
             if isinstance(f, TransformationField) and "NEXTVAL" in f.name.upper()),
            "SEQ_ID",
        )

        increment = tx.increment_by or 1
        # The first NEXTVAL a session gets is the repository's Current Value
        # (it then advances by Increment By); Start Value is only where Reset
        # or Cycle return to. "Current Value 1000" started at 1001 here.
        raw_current = get_ci(tx.properties, "current value", default="")
        if str(raw_current).strip():
            first = int(raw_current)
        else:
            first = tx.start_value or 1
        offset = first - 1
        lines.append(f"# Informatica Current Value: {first} (the first NEXTVAL)")
        # NEXTVAL is a persisted, restart-safe counter (Class C) --
        # call infa_compat.sequence() rather than hand-rolling one with
        # F.monotonically_increasing_id()/row_number(), which is
        # non-contiguous and not restart-safe across runs.
        lines.append("import infa_compat")
        seq_var = f"_seq_{_safe_var(tx.name)}"
        lines.append(
            f'{seq_var} = infa_compat.sequence(spark, "{tx.name}", '
            f'target_catalog_type="delta", start={offset + 1}, '
            f"increment={increment})"
        )
        lines.append(f'{out} = {seq_var}.assign({in_df}, "{seq_field}")')
        return lines

    # -------------------------------------------------------- Update Strategy

    def _update_strategy(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Update Strategy: {tx.name}")

        strategy_expr = (tx.update_strategy_expression or "").strip()
        lines.extend(_comment_lines(
            strategy_expr or "(none -- Informatica default DD_INSERT)",
            "Original strategy expression"))

        # DD_INSERT/DD_UPDATE/DD_DELETE/DD_REJECT routing is Class C (spec
        # S7) -- infa_compat.apply_update_strategy()/write_update_strategy()
        # (called from the target-write cell) consume a real "DD_STRATEGY"
        # column of infa_compat.UpdateStrategyCode INT values (0/1/2/3),
        # never a string label.
        #
        # The expression is converted as a WHOLE: ExpressionConverter emits
        # DD_INSERT/DD_UPDATE/DD_DELETE/DD_REJECT as the integer literals
        # Informatica defines them as, so IIF(cond, DD_INSERT, DD_UPDATE)
        # becomes when(cond, 0).otherwise(1) with the branches exactly where
        # the mapping put them. The previous keyword scan hard-wired
        # "condition true -> DD_UPDATE", which INVERTED the most common
        # form -- IIF(ISNULL(LKP_KEY), DD_INSERT, DD_UPDATE) -- and, because
        # it also converted the whole IIF as the condition, referenced
        # columns named DD_INSERT/DD_UPDATE that do not exist.
        if not strategy_expr:
            # Informatica's default Update Strategy expression is 0 (DD_INSERT).
            lines.append(
                f'{out} = {in_df}.withColumn("DD_STRATEGY", F.lit(0).cast("int"))'
            )
            return lines

        code, review = self._convert_or_flag(strategy_expr)
        if review and not _conversion_failed(code, review):
            lines.append(f"# {review}")  # converted; divergence note only
            review = None
        if review:
            lines.append(f"# {review}")
            lines.append(
                "# REVIEW REQUIRED: every row is tagged DD_REJECT below so nothing is "
                "inserted, updated or deleted by a strategy that could not be converted."
            )
            lines.append(
                f'{out} = {in_df}.withColumn("DD_STRATEGY", F.lit(3).cast("int"))'
            )
            return lines

        lines.append(
            f'{out} = {in_df}.withColumn("DD_STRATEGY", ({code}).cast("int"))'
        )
        return lines

    # --------------------------------------------------------------- Sorter

    def _sorter(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Sorter: {tx.name}")

        if tx.sort_keys:
            # NULL placement: Informatica sorts NULL as the HIGHEST value
            # unless the Sorter's "Null Treated Low" property is YES; Spark's
            # asc() puts NULL first. Match the source.
            nulls_low = get_ci(tx.properties, "null treated low", default="NO").strip().upper() in (
                "YES", "TRUE", "1",
            )
            order_parts: list[str] = []
            for key in tx.sort_keys:
                if isinstance(key, dict):
                    col_name = key.get("field", key.get("name", ""))
                    direction = key.get("direction", "ASC").upper()
                else:
                    col_name = str(key)
                    direction = tx.sort_direction.upper()
                is_asc = direction.startswith("ASC")
                if nulls_low:
                    suffix = ".asc_nulls_first()" if is_asc else ".desc_nulls_last()"
                else:
                    suffix = ".asc_nulls_last()" if is_asc else ".desc_nulls_first()"
                order_parts.append(f'F.col("{col_name}"){suffix}')
            lines.append(f"{out} = {in_df}.orderBy({', '.join(order_parts)})")

            # Handle Distinct = YES — deduplicate rows
            distinct = get_ci(tx.properties, "distinct", default="NO")
            if distinct.upper() == "YES":
                # Distinct compares the Sorter's OUTPUT ports; a column that
                # only exists upstream (not connected into the Sorter) must
                # not make two rows different.
                ports = [f.name for f in tx.fields if isinstance(f, TransformationField)
                         and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)]
                if ports:
                    lines.append(f"{out} = {out}.select({', '.join(repr(p) for p in ports)}).dropDuplicates()")
                else:
                    lines.append(f"{out} = {out}.dropDuplicates()")
        else:
            lines.append(
                f"{out} = {in_df}  # TODO: no sort keys found"
            )

        return lines

    # ----------------------------------------------------------------- Rank

    def _rank(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Rank: {tx.name}")
        lines.append("from pyspark.sql.window import Window")

        # Determine partition and order columns from transformation fields/properties
        group_fields = tx.group_by_fields or []
        sort_keys = tx.sort_keys or []

        partition_clause = (
            ", ".join(f'F.col("{g}")' for g in group_fields) if group_fields else ""
        )
        order_clause = ""
        if sort_keys:
            order_parts: list[str] = []
            for key in sort_keys:
                if isinstance(key, dict):
                    col_name = key.get("field", key.get("name", ""))
                    direction = key.get("direction", "ASC").upper()
                else:
                    col_name = str(key)
                    direction = "ASC"
                suffix = ".asc()" if direction == "ASC" else ".desc()"
                order_parts.append(f'F.col("{col_name}"){suffix}')
            order_clause = ", ".join(order_parts)

        rank_field = next(
            (f.name for f in tx.fields
             if isinstance(f, TransformationField) and "RANK" in f.name.upper()),
            "RANK_NUM",
        )

        top_n = tx.properties.get("top_bottom", "")

        if partition_clause and order_clause:
            win = f"Window.partitionBy({partition_clause}).orderBy({order_clause})"
        elif order_clause:
            win = f"Window.orderBy({order_clause})"
        else:
            win = "Window.orderBy(F.lit(1))  # TODO: add orderBy columns"

        lines.append(f'rank_window = {win}')
        # Informatica: "If two rank values match, they receive the same
        # value in the rank index and the transformation skips the next
        # value" -- SQL RANK(), not ROW_NUMBER().
        lines.append(
            f'{out} = {in_df}.withColumn("{rank_field}", '
            f"F.rank().over(rank_window))"
        )

        if top_n:
            lines.append(f'{out} = {out}.filter(F.col("{rank_field}") <= {top_n})')

        return lines

    # ---------------------------------------------------------------- Union

    def _union(
        self, tx: Transformation, in_df: str, out: str, extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Union: {tx.name}")

        branches = extra.get("__union_branches__")
        if branches:
            # Port-exact: each input group's connected columns, by position,
            # under the output port names. UNION ALL, as Informatica's Union.
            for i, (df_name, cols) in enumerate(branches):
                parts = [
                    f'F.col("{src}").alias("{dst}")' if src else f'F.lit(None).alias("{dst}")'
                    for src, dst in cols
                ]
                lines.append(f"_u{i} = {df_name}.select({', '.join(parts)})")
            chain = "_u0" + "".join(f".unionByName(_u{i})" for i in range(1, len(branches)))
            lines.append(f"{out} = {chain}")
            return lines

        all_dfs = [in_df] + [v for k, v in extra.items() if not str(k).startswith("__")]

        # Determine canonical output column names from the Union's OUTPUT ports
        output_cols = [
            f.name for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)
            and f.name  # skip empty
        ]

        if len(all_dfs) == 1:
            lines.append(f"{out} = {in_df}  # TODO: only one input found for Union")
        elif output_cols:
            # Each branch has O_* prefixed columns (or other naming).
            # Select and alias each branch to the canonical output column names,
            # then union. This handles schema normalization across branches.
            lines.append(f"# Canonical output columns: {', '.join(output_cols)}")

            # Build select+alias for each branch
            # The branches have columns named O_COLNAME or COLNAME — try both
            branch_exprs = []
            for i, df_name in enumerate(all_dfs):
                select_parts = []
                for col in output_cols:
                    # Try O_ prefixed version first (normalizer pattern)
                    o_col = f"O_{col}"
                    select_parts.append(
                        f'F.col("{o_col}").alias("{col}") '
                        f'if "{o_col}" in {df_name}.columns '
                        f'else F.col("{col}")'
                    )
                # Generate a clean select for this branch
                lines.append(
                    f'_branch_{i} = {df_name}.select(\n'
                    f'    *[F.col(c).alias(c.replace("O_", "", 1)) '
                    f'if c.startswith("O_") else F.col(c) '
                    f'for c in {df_name}.columns '
                    f'if c.startswith("O_") or c in {repr(output_cols)}]\n'
                    f')'
                )

            # Each branch has O_* prefixed columns from the normalizer expression.
            # The Union OUTPUT ports define the canonical post-union names.
            # Strip O_ prefix from output names to get the target column names.
            lines.clear()
            lines.append(f"# Union: {tx.name}")
            lines.append(f"# Select and rename O_* columns to canonical names per branch")

            # Determine canonical target names (strip O_ prefix from output ports)
            canonical_targets = []
            for col in output_cols:
                clean = col[2:] if col.startswith("O_") else col
                canonical_targets.append((col, clean))  # (source_O_name, target_name)

            for i, df_name in enumerate(all_dfs):
                col_selects = []
                for o_col, target_col in canonical_targets:
                    # The branch columns are named O_* (from EXP_NORM)
                    col_selects.append(f'F.col("{o_col}").alias("{target_col}")')
                select_str = ",\n        ".join(col_selects)
                lines.append(
                    f"_u{i} = {df_name}.select(\n"
                    f"        {select_str}\n"
                    f"    )"
                )
            # Build union chain
            union_chain = "_u0"
            for i in range(1, len(all_dfs)):
                union_chain = f"{union_chain}.unionByName(_u{i})"
            lines.append(f"{out} = {union_chain}")
        else:
            # No output ports found — fall back to simple unionByName
            union_chain = all_dfs[0]
            for df_name in all_dfs[1:]:
                union_chain = (
                    f"{union_chain}.unionByName({df_name}, allowMissingColumns=True)"
                )
            lines.append(f"{out} = {union_chain}")
        return lines

    # -------------------------------------------------------- Stored Procedure

    def _stored_procedure(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        lines: list[str] = []
        lines.append(f"# Stored Procedure: {tx.name}")
        lines.append("# WARNING: requires manual review — stored procedures cannot")
        lines.append("# be directly called from Spark. Port the logic to PySpark or use JDBC.")

        proc_name = get_ci(
            tx.properties, "stored procedure name", "procedure_name", default=tx.name
        )
        lines.append(f"# Original procedure: {proc_name}")

        # Add placeholder columns for OUTPUT ports so downstream transforms don't fail
        output_fields = [
            f for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction == DataFlowDirection.OUTPUT
        ]
        if output_fields:
            lines.append(
                "# REVIEW REQUIRED: the stored procedure's output ports are NULL "
                "placeholders below -- they used to be the literal 'N', which "
                "flowed into the target as if the procedure had returned it."
            )
            for f in output_fields:
                lines.append(
                    f'{in_df} = {in_df}.withColumn("{f.name}", {_null_of(f)})  '
                    f'# REVIEW REQUIRED: stored procedure output'
                )

        lines.append(f"{out} = {in_df}")
        return lines

    # ----------------------------------------------------- SQL Transformation

    def _sql_transformation(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        import re as _re
        lines: list[str] = []
        lines.append(f"# SQL Transformation: {tx.name}")

        # SQL Transformation stores SQL in properties["Sql Query"] or sql_override
        sql = (tx.properties.get("Sql Query", "") or tx.sql_override or "").strip()

        if sql:
            # Convert Informatica ~column~ bind syntax to PySpark
            bind_cols = _re.findall(r'~(\w+)~', sql)

            if bind_cols:
                lines.append(f"# SQL Transformation with bind variables: {', '.join(bind_cols)}")
                lines.extend(_comment_lines(sql[:400], "Original SQL"))

                # Detect external table JOINs in the SQL
                # Example: LEFT JOIN REF.REGIONAL_TAX_RATES t ON t.CURRENCY_CODE = ~CURRENCY_CODE~
                join_match = _re.search(
                    r'(?:LEFT\s+)?JOIN\s+([\w.]+)\s+(\w+)\s+ON\s+\2\.(\w+)\s*=\s*~(\w+)~',
                    sql, _re.IGNORECASE
                )
                if join_match:
                    ext_table = join_match.group(1)
                    ext_alias = join_match.group(2)
                    ext_col = join_match.group(3)
                    bind_col = join_match.group(4)
                    lines.append(f"# External table join: {ext_table}")
                    lines.append(f'_ext_{ext_alias} = spark.table("{ext_table}")')
                    lines.append(
                        f'{in_df} = {in_df}.join(F.broadcast(_ext_{ext_alias}), '
                        f'{in_df}["{bind_col}"] == _ext_{ext_alias}["{ext_col}"], "left")'
                    )
                    lines.append(f'{in_df} = {in_df}.drop(_ext_{ext_alias}["{ext_col}"])')

                # Translate the SQL CASE logic to PySpark F.when() chains
                case_match = _re.search(
                    r'SELECT\s+CASE\s+(.+?)\s+END\s+AS\s+(\w+)',
                    sql, _re.IGNORECASE | _re.DOTALL
                )
                if case_match:
                    case_body = case_match.group(1)
                    alias_name = case_match.group(2)

                    # Parse WHEN clauses — handle both literal rates and table column refs
                    when_clauses = _re.findall(
                        r"WHEN\s+~(\w+)~\s*=\s*'(\w+)'\s+THEN\s+~(\w+)~\s*\*\s*([\d.]+|\w+\.\w+)",
                        case_body, _re.IGNORECASE
                    )
                    else_match = _re.search(r"ELSE\s+~(\w+)~\s*\*\s*([\d.]+)", case_body, _re.IGNORECASE)

                    if when_clauses:
                        parts = []
                        for cond_col, cond_val, amt_col, rate in when_clauses:
                            # Distinguish table.column refs (t.TAX_RATE) from
                            # decimal literals (0.08) — table refs have alpha before dot
                            is_table_ref = "." in rate and not rate[0].isdigit()
                            if is_table_ref:
                                col_name = rate.split(".")[-1]
                                parts.append(
                                    f'F.when(F.col("{cond_col}") == "{cond_val}", '
                                    f'F.col("{amt_col}") * F.coalesce(F.col("{col_name}"), F.lit(0)))'
                                )
                            else:
                                parts.append(
                                    f'F.when(F.col("{cond_col}") == "{cond_val}", '
                                    f'F.col("{amt_col}") * {rate})'
                                )
                        # Build chained .when() — first F.when(), rest .when()
                        chain = parts[0]
                        for p in parts[1:]:
                            # Extract the when(...) part after "F."
                            when_content = p[p.index("when("):]
                            chain += f"\n     .{when_content}"

                        if else_match:
                            chain += f'.otherwise(F.col("{else_match.group(1)}") * {else_match.group(2)})'
                        else:
                            chain += ".otherwise(F.lit(0))"

                        lines.append(f'{out} = {in_df}.withColumn("{alias_name}", {chain})')
                    else:
                        # A bare F.lit(None) is NullType (VOID), which Delta
                        # refuses to create a column for
                        # (DELTA_MERGE_ADD_VOID_COLUMN), and a TODO comment
                        # left the column silently empty besides.
                        lines.extend(_refuse_lines(
                            f"SQL CASE in {tx.name!r} was not translated",
                            "REVIEW REQUIRED: could not convert the SQL CASE "
                            "expression"))
                else:
                    # No CASE found — register as temp view and run SQL
                    lines.append(f'{in_df}.createOrReplaceTempView("_input_{_safe_var(tx.name)}")')
                    spark_sql = sql
                    for col in bind_cols:
                        spark_sql = spark_sql.replace(f"~{col}~", f"_input_{_safe_var(tx.name)}.{col}")
                    # Remove DUAL references (Oracle-specific)
                    spark_sql = _re.sub(r'\bFROM\s+DUAL\b', '', spark_sql, flags=_re.IGNORECASE)
                    spark_sql = spark_sql.replace('"', '\\"').replace("\n", " ")
                    lines.append(f'{out} = spark.sql("{spark_sql}")')
            else:
                sql_escaped = sql.replace('"', '\\"').replace("\n", " ")
                lines.append(f'{in_df}.createOrReplaceTempView("_tmp_{_safe_var(tx.name)}")')
                lines.append(f'{out} = spark.sql("{sql_escaped}")')
        else:
            # No SQL found — check for output fields and generate TODO
            output_fields = [
                f for f in tx.fields
                if isinstance(f, TransformationField)
                and f.direction == DataFlowDirection.OUTPUT
            ]
            if output_fields:
                lines.append(f"# TODO: implement SQL Transformation logic")
                for of in output_fields:
                    lines.append(
                        f'{in_df} = {in_df}.withColumn("{of.name}", {_null_of(of)})  # TODO'
                    )
            lines.append(f"{out} = {in_df}")
        return lines

    # ------------------------------------------------------------ Normalizer

    @staticmethod
    def _normalizer_by_port_convention(tx: Transformation, in_df: str, out: str) -> list[str]:
        """A relational Normalizer as the Designer builds it, or [].

        For a column SALES that occurs N times the Designer generates input
        ports SALES_in1..SALES_inN, one output port SALES, and the output
        ports GK_SALES (generated key) and GCID_SALES (occurrence index);
        single-occurring ports are INPUT/OUTPUT and pass through. Each
        source row becomes N rows: SALES from SALES_ink, GCID_SALES = k,
        the pass-through ports repeated, and ONE GK_SALES value shared by
        the source row's N outputs. A NULL occurrence is still a row --
        Informatica does not drop it (dropping it needs a Filter).
        """
        fields = [f for f in tx.fields if isinstance(f, TransformationField)]
        by_upper = {f.name.upper(): f for f in fields}
        groups: dict[str, list[tuple[int, str]]] = {}
        for f in fields:
            if f.direction != DataFlowDirection.INPUT:
                continue
            m = re.fullmatch(r"(?P<col>.+)_in(?P<k>\d+)", f.name, re.IGNORECASE)
            col = (f.ref_field or (m.group("col") if m else "")).strip()
            if not col or col.upper() not in by_upper:
                continue
            k = int(m.group("k")) if m else len(groups.get(col, [])) + 1
            groups.setdefault(by_upper[col.upper()].name, []).append((k, f.name))
        groups = {c: sorted(v) for c, v in groups.items() if len(v) > 1}
        if not groups:
            return []
        counts = {len(v) for v in groups.values()}
        if len(counts) != 1:
            return []          # different OCCURS per column: not one row set
        n = counts.pop()
        passthrough = [f.name for f in fields if f.direction == DataFlowDirection.INPUT_OUTPUT]
        lines = [
            f"# Relational Normalizer: {', '.join(groups)} occur {n} times; each source row "
            f"becomes {n} rows, NULL occurrences included.",
            f"# GK_* is one value per SOURCE row (monotonically_increasing_id); Informatica "
            f"draws it from a repository sequence, so its start value differs.",
            f'_nrm = {in_df}.withColumn("__nrm_gk", F.monotonically_increasing_id())',
        ]
        structs = []
        for k in range(1, n + 1):
            members = [f'F.lit({k}).alias("__gcid")'] + [
                f'F.col("{dict(v)[k]}").alias("{col}")' for col, v in groups.items()
            ]
            structs.append(f"F.struct({', '.join(members)})")
        lines.append(
            f"_nrm = _nrm.select({', '.join(repr(c) for c in passthrough + ['__nrm_gk'])}, "
            f"F.explode(F.array({', '.join(structs)})).alias('__occ'))"
        )
        selects = [repr(c) for c in passthrough]
        for col in groups:
            selects.append(f'F.col("__occ.{col}").alias("{col}")')
            if f"GCID_{col}".upper() in by_upper:
                selects.append(f'F.col("__occ.__gcid").alias("{by_upper[f"GCID_{col}".upper()].name}")')
            if f"GK_{col}".upper() in by_upper:
                selects.append(f'F.col("__nrm_gk").alias("{by_upper[f"GK_{col}".upper()].name}")')
        lines.append(f"{out} = _nrm.select({', '.join(selects)})")
        return lines

    def _normalizer(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Convert Informatica Normalizer to PySpark explode().

        Informatica Normalizer handles:
        1. VSAM OCCURS (repeating groups) — explode array columns
        2. Multiple-input group normalization — unpivot
        3. Generated keys (GK) for the normalized output
        """
        lines: list[str] = []
        lines.append(f"# Normalizer: {tx.name}")

        port_exact = self._normalizer_by_port_convention(tx, in_df, out)
        if port_exact:
            return lines + port_exact

        # Find the array/repeating field(s) to normalize
        # In Informatica, OCCURS fields or fields with LEVEL > 0 are the repeating groups
        occurs_fields = [
            f for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.INPUT, DataFlowDirection.INPUT_OUTPUT)
        ]
        output_fields = [
            f for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)
        ]

        # Check if there's a specific occurs/repeat count in properties
        occurs_count = get_ci(tx.properties, "occurs")

        if occurs_count:
            # VSAM-style: fixed number of occurrences
            # Example: PHONE_1, PHONE_2, PHONE_3 → explode into PHONE column
            lines.append(f"# VSAM normalization: {occurs_count} occurrences")
            lines.append(f"# Unpivot repeating columns into rows")

            # Detect repeating field groups (fields ending in _1, _2, _3 etc.)
            base_fields = set()
            for f in occurs_fields:
                match = re.match(r'^(.+?)_?\d+$', f.name)
                if match:
                    base_fields.add(match.group(1))

            if base_fields:
                # Find the sequence/index output field (e.g., DIAG_SEQUENCE)
                seq_field = None
                for of in output_fields:
                    if "SEQUENCE" in of.name.upper() or "IDX" in of.name.upper() or "GK" in of.name.upper():
                        seq_field = of.name
                        break

                for base in sorted(base_fields):
                    repeat_cols = [f.name for f in occurs_fields if f.name.startswith(base)]
                    col_array = ", ".join(f'F.struct(F.lit({i+1}).alias("_idx"), F.col("{c}").alias("{base}"))'
                                         for i, c in enumerate(repeat_cols))
                    lines.append(
                        f'{out} = {in_df}.withColumn("_arr", F.array({col_array}))'
                    )
                    lines.append(
                        f'{out} = {out}.withColumn("_exploded", F.explode("_arr"))'
                    )
                    lines.append(
                        f'{out} = {out}.select("*", F.col("_exploded.{base}").alias("{base}"))'
                    )
                    # Extract the sequence index
                    if seq_field:
                        lines.append(
                            f'{out} = {out}.select("*", F.col("_exploded._idx").alias("{seq_field}"))'
                        )
                    lines.append(f'{out} = {out}.drop("_arr", "_exploded")')
            else:
                lines.append(f"{out} = {in_df}  # TODO: identify repeating columns for normalization")
        else:
            # General normalization: look for array-type columns to explode
            # or multi-value columns
            array_candidates = [
                f.name for f in occurs_fields
                if f.datatype.lower() in ("array", "struct", "text")
                or "ARRAY" in f.datatype.upper()
            ]

            if array_candidates:
                col = array_candidates[0]
                lines.append(f"# Explode array column: {col}")
                lines.append(f'{out} = {in_df}.withColumn("{col}", F.explode(F.col("{col}")))')
            else:
                # Fallback: use stack() for unpivot pattern
                lines.append("# Normalizer — converting to unpivot pattern")
                input_names = [f.name for f in occurs_fields]
                output_name = output_fields[0].name if output_fields else "normalized_value"

                # Without OCCURS metadata, the only safe repeating group is
                # an unambiguous numbered one (SALES_1..SALES_n or Q1..Qn, all
                # one type). Every other port -- a key such as PRODUCT_ID --
                # repeats unchanged on each output row. Stacking all input
                # ports unpivoted the key as if it were a value: the notebook
                # ran and wrote a ('PRODUCT_ID', 101) row per source row.
                groups: dict = {}
                for f in occurs_fields:
                    m = re.match(r"^(.*?[A-Za-z])_?(\d+)$", f.name)
                    if m:
                        groups.setdefault(m.group(1).upper(), []).append(f)
                repeating = [
                    g for g in groups.values()
                    if len(g) >= 2 and len({_port_spark_type(f) for f in g}) == 1
                ]
                if len(repeating) == 1:
                    rep = repeating[0]
                    # stack() is SQL: columns are backtick-quoted identifiers.
                    # This used to embed Python -- F.col('X') -- inside the SQL
                    # string, which no Spark version can parse (seen on AIDP
                    # 2026-09-25: UNRESOLVED_ROUTINE `F`.`col`).
                    stack_args = ", ".join(f"'{f.name}', `{f.name}`" for f in rep)
                    lines.append(
                        f'{out} = {in_df}.selectExpr("*", '
                        f'"stack({len(rep)}, {stack_args}) as (source_col, {output_name})")'
                    )
                    lines.append(
                        f'{out} = {out}.drop({", ".join(repr(f.name) for f in rep)})'
                        f'.filter(F.col("{output_name}").isNotNull())'
                    )
                elif len(input_names) > 1:
                    # No single numbered repeating group: the occurrences
                    # cannot be told apart from the ports that carry through.
                    # Say so instead of guessing.
                    lines.append(
                        f"# REVIEW REQUIRED: Normalizer '{tx.name}' has no OCCURS "
                        f"metadata and no single numbered repeating group among its input "
                        f"ports ({', '.join(f'{f.name}:{_port_spark_type(f)}' for f in occurs_fields)}), "
                        f"so the repeating group cannot be identified. Unpivot the "
                        f"repeating columns by hand (stack() or F.explode over an "
                        f"array); rows pass through unchanged until then."
                    )
                    lines.append(f"{out} = {in_df}")
                else:
                    lines.append(f"{out} = {in_df}  # Single field — no normalization needed")

        # Add generated key (GK) if normalizer has a GENERATED_KEY output
        gk_fields = [f for f in output_fields if "GK" in f.name.upper() or "GENERATED" in f.name.upper()]
        if gk_fields:
            gk_name = gk_fields[0].name
            lines.append(f'# Generated key for normalized rows')
            lines.append(
                f'{out} = {out}.withColumn("{gk_name}", F.monotonically_increasing_id())'
            )

        return lines

    # -------------------------------------------------------------- Mapplet

    def _mapplet(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Convert Informatica Mapplet to PySpark function.

        A Mapplet is a reusable transformation group — like a function.
        It contains internal transformations (Expression, Filter, Lookup, etc.)
        that are applied as a unit.

        Strategy:
        1. If the Mapplet has embedded transformations → process each one
        2. If only input/output ports are visible → generate a function skeleton
           that applies the output expressions
        """
        lines: list[str] = []
        lines.append(f"# Mapplet: {tx.name}")
        lines.append(f"# Reusable transformation group — converted to inline PySpark")

        # Get input and output fields
        input_fields = [
            f for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.INPUT, DataFlowDirection.INPUT_OUTPUT)
        ]
        output_fields = [
            f for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)
            and f.expression and f.expression.strip() != f.name
        ]

        if output_fields:
            # Has expressions — generate inline transformations
            lines.append(f"# Mapplet has {len(output_fields)} derived output(s)")
            current = in_df
            for field in output_fields:
                pyspark_expr, review = self._convert_or_flag(field.expression)
                if review and not _conversion_failed(pyspark_expr, review):
                    lines.append(f"# {review}")  # converted; divergence note only
                    review = None
                if review:
                    lines.append(f"# {review}")
                    lines.append(f"# {field.name}: {field.expression}")
                    lines.append(
                        f'{current} = {current}.withColumn("{field.name}", '
                        f'{_null_of(field)})  # REVIEW REQUIRED: manual conversion'
                    )
                else:
                    # Apply DecimalType cast if needed
                    needs_decimal = (
                        field.datatype.lower() in ("number", "decimal", "numeric")
                        and field.scale > 0
                        and any(op in field.expression for op in ("*", "/", "+", "-"))
                    )
                    if needs_decimal:
                        lines.append(
                            f'{current} = {current}.withColumn("{field.name}", '
                            f"({pyspark_expr}).cast(DecimalType({field.precision}, {field.scale})))"
                        )
                    else:
                        lines.append(
                            f'{current} = {current}.withColumn("{field.name}", {pyspark_expr})'
                        )

            if current != out:
                lines.append(f"{out} = {current}")
        else:
            # No expressions visible — generate function skeleton
            lines.append("# Mapplet internals not visible — generating function skeleton")
            input_cols = ", ".join(f'"{f.name}"' for f in input_fields[:5])

            lines.append(f"def mapplet_{tx.name.lower()}(df):")
            lines.append(f"    # TODO: implement mapplet logic")
            lines.append(f"    # Input columns: {input_cols}")
            lines.append(f"    return df")
            lines.append(f"")
            lines.append(f"{out} = mapplet_{tx.name.lower()}({in_df})")

        return lines

    # ----------------------------------------------------------- Unsupported

    # ------------------------------------------- code-carrying (Phase 4)

    # Transformations whose logic is embedded source code in another
    # language. There is nothing to *translate* -- the code is the logic --
    # so the faithful conversion preserves it verbatim and scaffolds around
    # it. Dropping the body would lose the only copy a migrator has; a
    # paraphrase would be a guess at semantics nobody can check.
    #
    # (type, language label, property spellings holding the body, guidance)
    _CODE_CARRYING: dict = {}   # populated below the class body

    def _code_carrying(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        label, props, guidance = self._CODE_CARRYING[tx.type]
        body = get_ci(tx.properties, *props, default="")
        lines = [f"# {label}: {tx.name}"]

        if body:
            lines.append(f"# --- original {label} body, preserved verbatim ---")
            lines.extend(f"#   {ln}" for ln in body.splitlines() or [""])
            lines.append("# --- end original body ---")
        else:
            lines.append(
                f"# The {label} body is not present in this export. It lives "
                "outside the mapping and must be retrieved from the source "
                "system before this can be reimplemented."
            )

        lines.extend([
            f"# REVIEW REQUIRED: {guidance} Rows pass through unchanged below, "
            "so this transformation's logic is NOT applied.",
            f"{out} = {in_df}",
        ])
        return lines

    # ------------------------------------- explicitly unconvertible (Phase 6)

    # Types with no Spark answer. Each entry states why and what to do
    # instead, because "unsupported" alone leaves a migrator with no next
    # step. A tested, specific refusal is coverage; a silent drop is not.
    _NO_EQUIVALENT: dict = {}   # populated below the class body

    def _report_no_equivalent(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        why, instead = self._NO_EQUIVALENT[tx.type]
        lines = [f"# {tx.type.value}: {tx.name}"]
        asset = get_ci(tx.properties, "asset", "rule name", "model name",
                       "dictionary", "reference data", default="")
        if asset:
            lines.append(f"# Referenced asset: {asset}")
        lines.extend([
            f"# REVIEW REQUIRED: {why} {instead} Rows pass through unchanged "
            "below, so this transformation's logic is NOT applied.",
            f"{out} = {in_df}",
        ])
        return lines

    # ------------------------------------------------- AIDP-native (Phase 5)

    def _chunking(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """CDI Chunking -> split text into chunks, one row per chunk.

        Genuinely convertible: chunking is string slicing plus an explode.
        The emitted form is fixed-size with overlap, which is the common
        default; the source's own size and strategy are used when present.
        """
        field = get_ci(tx.properties, "input field", "text field", "source field",
                       default="")
        size = get_ci(tx.properties, "chunk size", "size", default="1000")
        overlap = get_ci(tx.properties, "chunk overlap", "overlap", default="0")
        lines = [f"# Chunking: {tx.name}"]
        if not field:
            lines.extend([
                "# REVIEW REQUIRED: no input text field configured, so there is "
                "nothing to chunk. Rows pass through unchanged.",
                f"{out} = {in_df}",
            ])
            return lines
        lines.extend([
            f"# Fixed-size chunks of {size} characters, overlap {overlap}.",
            f'{out} = ({in_df}',
            f'    .withColumn("_chunk_starts", F.sequence(',
            f'        F.lit(0), F.length(F.col("{field}")), '
            f'F.lit(max(1, {size} - {overlap}))))',
            '    .withColumn("_chunk_start", F.explode_outer(F.col("_chunk_starts")))',
            f'    .withColumn("chunk", F.substring(F.col("{field}"), '
            f'F.col("_chunk_start") + 1, {size}))',
            '    .drop("_chunk_starts", "_chunk_start")',
            ')',
            "# REVIEW REQUIRED: Informatica's chunking strategy (sentence, "
            "paragraph, token-aware) is not recoverable from the export. This "
            "emits fixed-size character chunks -- confirm against the source "
            "if chunk boundaries matter to downstream retrieval quality.",
        ])
        return lines

    def _vector_embedding(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """CDI Vector Embedding -> AIDP embedding over a text column."""
        field = get_ci(tx.properties, "input field", "text field", default="")
        model = get_ci(tx.properties, "model", "embedding model", default="")
        lines = [f"# Vector Embedding: {tx.name}"]
        if model:
            lines.append(f"# Source embedding model: {model}")
        lines.extend([
            "# REVIEW REQUIRED: AIDP provides embeddings through its knowledge-"
            "base/embedding stack rather than a row-level transformation, and "
            "the model name above will not map 1:1 to an AIDP model. Pick the "
            "AIDP equivalent and confirm the vector dimension matches whatever "
            "consumes it downstream -- a dimension mismatch fails at query "
            "time, not here.",
        ])
        if field:
            lines.append(f'# Text column to embed: "{field}"')
        lines.append(f"{out} = {in_df}")
        return lines

    def _machine_learning(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """CDI Machine Learning -> a model served from the AIDP registry."""
        model = get_ci(tx.properties, "model name", "model", "deployment",
                       default="")
        lines = [f"# Machine Learning: {tx.name}"]
        if model:
            lines.append(f"# Source model / deployment: {model}")
        lines.extend([
            "# REVIEW REQUIRED: this scores rows against a model deployed in "
            "Informatica, which is not part of the mapping export. The model "
            "itself has to be migrated or retrained and registered in AIDP "
            "MLOps first; then score with an mlflow.pyfunc.spark_udf over the "
            "same input columns. Rows pass through unchanged, so no scoring "
            "happens.",
            f"{out} = {in_df}",
        ])
        return lines

    # ------------------------------------------------- partial CDQ (Phase 6)

    def _data_masking(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Data Masking -> hashing or redaction per field.

        Only irreversible masking converts. Format-preserving and
        reversible masking depend on Informatica's key management, which is
        not in the export, and a hash silently substituted for
        format-preserving masking breaks any downstream format check.
        """
        lines = [f"# Data Masking: {tx.name}"]
        targets = [getattr(f, "name", "") for f in tx.fields
                   if getattr(f, "name", "")]
        technique = get_ci(tx.properties, "masking technique", "technique",
                           default="").lower()

        if technique and ("format" in technique or "reversible" in technique):
            lines.extend([
                f"# Technique: {technique}",
                "# REVIEW REQUIRED: format-preserving and reversible masking "
                "depend on Informatica's key management, which is not in the "
                "export. A hash would satisfy the column type and break any "
                "downstream format validation, so none is emitted.",
                f"{out} = {in_df}",
            ])
            return lines

        if targets:
            expr = in_df
            for col in targets:
                expr += f'.withColumn("{col}", F.sha2(F.col("{col}").cast("string"), 256))'
            lines.append(f"{out} = {expr}")
            lines.append(
                "# REVIEW REQUIRED: irreversible SHA-256 masking. Confirm this "
                "matches the source technique -- if the mapping used "
                "format-preserving or reversible masking, this is not "
                "equivalent and downstream consumers will see different data."
            )
        else:
            lines.extend([
                "# REVIEW REQUIRED: no masked fields found on this "
                "transformation. Rows pass through unmasked.",
                f"{out} = {in_df}",
            ])
        return lines

    # ------------------------------------------------ hierarchy family

    # Informatica's hierarchical transformations move between relational rows
    # and nested documents. Spark can express both directions natively, so
    # these are genuinely convertible -- unlike Structure Parser below.
    #
    # The shape of these transformations in a real export is NOT yet known:
    # no real IDMC export has been parsed, and the property names below are
    # the plausible spellings, looked up spelling-tolerantly. When the
    # metadata is absent the converters emit a working scaffold plus a review
    # marker naming exactly what was missing, rather than inventing a schema.
    # That is the same rule the source/target readers follow.

    @staticmethod
    def _hier_paths(tx: Transformation) -> list[str]:
        """Output field paths, e.g. ``order.lines.sku``. Dots are the nesting."""
        paths = []
        for fld in tx.fields:
            direction = getattr(fld, "direction", None)
            if direction is DataFlowDirection.INPUT:
                continue
            path = getattr(fld, "expression", "") or getattr(fld, "name", "")
            if path:
                paths.append((getattr(fld, "name", path), path))
        return paths

    def _hierarchy_parser(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Hierarchy Parser: nested document -> relational rows.

        Arrays become ``explode_outer``, not ``explode``. Informatica's
        parser emits a row for a parent whose child array is empty; Spark's
        ``explode`` drops that row entirely. The difference is invisible
        until a row count comes up short, and it is silent -- no error, just
        fewer rows. ``explode_outer`` preserves the parent with nulls, which
        matches the source behaviour.
        """
        lines = [f"# Hierarchy Parser: {tx.name} (nested -> relational)"]
        root = get_ci(tx.properties, "input field", "root", "source field",
                      "input group", default="")
        paths = self._hier_paths(tx)

        if not root and not paths:
            lines.extend([
                "# REVIEW REQUIRED: no input field or output field paths found "
                "on this transformation, so the nested structure it flattens is "
                "unknown. Supply the hierarchical schema and re-run, or flatten "
                "by hand. Rows pass through unchanged below -- nothing is "
                "flattened.",
                f"{out} = {in_df}",
            ])
            return lines

        src = root or "value"
        lines.append(f"# Input field: {src}")
        arrays = [(n, p) for n, p in paths if "[" in p or p.endswith("[]")]
        scalars = [(n, p) for n, p in paths if (n, p) not in arrays]

        expr = in_df
        for _, path in arrays:
            clean = path.replace("[]", "").strip()
            expr += f'.withColumn("{clean.split(".")[-1]}", F.explode_outer(F.col("{clean}")))'
        if arrays:
            lines.append(
                "# explode_outer, not explode: Informatica emits a row for a "
                "parent whose child array is empty; explode drops it silently."
            )
        if scalars:
            sel = ", ".join(
                f'F.col("{p}").alias("{n}")' for n, p in scalars
            )
            expr += f".select({sel})"
        lines.append(f"{out} = {expr}")
        return lines

    def _hierarchy_builder(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Hierarchy Builder: relational rows -> nested document."""
        lines = [f"# Hierarchy Builder: {tx.name} (relational -> nested)"]
        out_field = get_ci(tx.properties, "output field", "root",
                           "target field", default="")
        paths = self._hier_paths(tx)

        if not paths:
            lines.extend([
                "# REVIEW REQUIRED: no output field paths found, so the "
                "document shape this builds is unknown. Rows pass through "
                "unchanged below -- nothing is nested.",
                f"{out} = {in_df}",
            ])
            return lines

        root = out_field or "document"
        cols = ", ".join(f'F.col("{p}").alias("{n}")' for n, p in paths)
        lines.extend([
            f'{out} = {in_df}.withColumn("{root}", F.struct({cols}))',
            "# REVIEW REQUIRED: this builds one document per input row. If the "
            "source transformation groups rows into a repeating element, add a "
            "groupBy with F.collect_list before the struct -- the grouping key "
            "is not recoverable from the export.",
        ])
        return lines

    def _hierarchy_processor(
        self, tx: Transformation, in_df: str, out: str, extra: dict
    ) -> list[str]:
        """Hierarchy Processor: the general relational<->hierarchical node.

        Unlike Parser and Builder it is bidirectional and can carry several
        output groups, each with its own shape. Direction is inferred from
        the configured properties; when it cannot be inferred the
        transformation is reported rather than guessed at, because
        flattening when the mapping meant to nest produces a valid notebook
        and wrong output.
        """
        lines = [f"# Hierarchy Processor: {tx.name}"]
        mode = get_ci(tx.properties, "output type", "mode", "direction",
                      default="").strip().lower()
        groups = tx.properties.get("output_groups") or tx.router_groups or []

        if len(groups) > 1:
            lines.append(
                f"# REVIEW REQUIRED: {len(groups)} output groups configured. "
                "Each output group is a separate downstream shape and needs "
                "its own DataFrame; only the first is emitted below."
            )

        if mode.startswith("relational") or "flatten" in mode:
            return lines + self._hierarchy_parser(tx, in_df, out, extra)[1:]
        if mode.startswith("hierarch") or "nest" in mode or "build" in mode:
            return lines + self._hierarchy_builder(tx, in_df, out, extra)[1:]

        lines.extend([
            "# REVIEW REQUIRED: could not determine whether this flattens or "
            "nests -- no output-type property found. The two directions are "
            "not interchangeable and guessing produces a notebook that runs "
            "and is wrong, so neither is emitted. Rows pass through unchanged.",
            f"{out} = {in_df}",
        ])
        return lines

    def _structure_parser(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Structure Parser has no Spark equivalent, and cannot have one.

        It parses unstructured input using an *intelligent structure model*
        -- a CLAIRE-derived asset that lives in IDMC and is not part of the
        mapping export. The parsing logic is in the model, not in the
        mapping, so there is nothing in the export to translate. This is a
        different situation from a missing converter, and it is reported as
        such.
        """
        model = get_ci(tx.properties, "structure model", "intelligent structure",
                       "model name", default="")
        lines = [f"# Structure Parser: {tx.name}"]
        if model:
            lines.append(f"# Intelligent structure model: {model}")
        lines.extend([
            "# REVIEW REQUIRED: Structure Parser has no Spark equivalent. Its "
            "parsing logic lives in an intelligent structure model held in "
            "IDMC, which is not part of the mapping export -- there is nothing "
            "here to translate. Replace with an explicit parser for this input "
            "format (from_json, regexp_extract, or a custom reader). Rows pass "
            "through unchanged, so the input is NOT parsed.",
            f"{out} = {in_df}",
        ])
        return lines

    # ------------------------------------------------- mapplet Input/Output

    def _mapplet_port(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Mapplet Input / Output transformation -- the mapplet's boundary.

        These define the mapplet's signature; they carry no logic of their
        own, so the DataFrame passes through. They are not no-ops to the
        reader, though: a renamed port changes the column name crossing the
        boundary, and dropping that rename silently loses the column
        downstream. So the pass-through is explicit and any rename is
        applied.

        Previously these fell to ``_unsupported`` and emitted a manual-
        conversion marker -- a gap inside a feature already listed as
        covered, since MAPPLET itself converts.
        """
        kind = "Input" if tx.type is TransformationType.INPUT else "Output"
        lines = [f"# Mapplet {kind} port: {tx.name} (boundary, no logic)"]

        renames: list[tuple[str, str]] = []
        for fld in tx.fields:
            src = getattr(fld, "source_field", "") or ""
            name = getattr(fld, "name", "") or ""
            if src and name and src != name:
                renames.append((src, name))

        if renames:
            expr = in_df
            for src, name in renames:
                expr += f'.withColumnRenamed("{src}", "{name}")'
            lines.append(f"{out} = {expr}")
        else:
            lines.append(f"{out} = {in_df}")
        return lines

    # ------------------------------------------------------------ Deduplicate

    def _deduplicate(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """CDI Deduplicate -> dropDuplicates on the configured key set.

        Informatica's Deduplicate keeps the *first* row per key group, where
        "first" follows the input order. Spark's ``dropDuplicates`` keeps an
        arbitrary row from each group -- there is no ordering contract, and
        the row kept can differ between runs of the same job. When the
        non-key columns differ within a group that is a real behavioural
        difference, so it is stated rather than left for someone to discover
        in a reconciliation.
        """
        lines = [f"# Deduplicate: {tx.name}"]

        keys = tx.group_by_fields or [
            getattr(f, "name", "") for f in tx.fields
            if getattr(f, "is_key", False)
        ]
        keys = [k for k in keys if k]

        if keys:
            cols = ", ".join(f'"{k}"' for k in keys)
            lines.append(f"{out} = {in_df}.dropDuplicates([{cols}])")
            lines.append(
                "# REVIEW REQUIRED: Informatica keeps the first row per group "
                "in input order; dropDuplicates keeps an arbitrary row and may "
                "pick a different one between runs. Add an orderBy + "
                "row_number filter if which row survives matters."
            )
        else:
            lines.append(f"{out} = {in_df}.dropDuplicates()")
            lines.append(
                "# REVIEW REQUIRED: no dedup key found on this transformation, "
                "so this deduplicates on ALL columns. Confirm against the "
                "source mapping."
            )
        return lines

    # ----------------------------------------------------- Transaction Control

    def _transaction_control(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        """Transaction Control: the rows of a rolled-back transaction do not
        land; everything else passes through.

        The condition returns TC_CONTINUE_TRANSACTION (0), TC_COMMIT_BEFORE
        (1: commit what is open, the row starts a new transaction),
        TC_COMMIT_AFTER (2: the row ends the transaction, committed),
        TC_ROLLBACK_BEFORE (3: roll back what is open, the row starts a new
        transaction) or TC_ROLLBACK_AFTER (4: the row ends the transaction,
        rolled back). Transactions are numbered over the row order on
        entry, and the rows of every rolled-back one are removed; the one
        open at end of file is committed unless "Commit On End Of File" is
        NO. It used to pass every row through, so rolled-back rows landed.

        What cannot be kept is visibility DURING the run: Informatica
        commits mid-stream, a Spark write commits once at the end.
        """
        lines = [f"# Transaction Control: {tx.name}"]
        expr = (tx.properties.get("tc_expression") or tx.properties.get("Transaction Control Condition")
                or tx.update_strategy_expression or "").strip()
        if not expr:
            lines.append("# No transaction control condition in the export: every row continues the transaction")
            lines.append(f"{out} = {in_df}")
            return lines
        code, review = self._convert_or_flag(expr)
        if review and _conversion_failed(code, review):
            lines.append(f"# {review}")
            lines.append(
                "# REVIEW REQUIRED: the Transaction Control condition could not be converted, so "
                "rolled-back rows are NOT removed below."
            )
            lines.append(f"{out} = {in_df}")
            return lines
        eof_commit = str(get_ci(tx.properties, "commit on end of file", default="YES")).upper() != "NO"
        lines += [
            f"# Condition: {expr}",
            "# Informatica commits mid-stream on this condition; this notebook writes once at the",
            "# end, so only the RESULT is kept: rows of rolled-back transactions are removed.",
            f'_tc = {in_df}.withColumn("__ord", F.monotonically_increasing_id())'
            f'.withColumn("__tc", F.coalesce(({_as_column(code)}).cast("int"), F.lit(0)))',
            '_tcw = Window.orderBy("__ord")',
            '_tcr = Window.orderBy("__ord").rowsBetween(Window.unboundedPreceding, Window.currentRow)',
            '_tcp = Window.orderBy("__ord").rowsBetween(Window.unboundedPreceding, -1)',
            '_tc = _tc.withColumn("__start", F.col("__tc").isin(1, 3).cast("int"))',
            '_tc = _tc.withColumn("__end", F.col("__tc").isin(2, 4).cast("int"))',
            '_tc = _tc.withColumn("__tx", F.sum("__start").over(_tcr) + F.coalesce(F.sum("__end").over(_tcp), F.lit(0)))',
            '_tc = _tc.withColumn("__rb", F.when(F.col("__tc") == 4, F.col("__tx"))'
            '.when((F.col("__tc") == 3) & ~F.coalesce(F.lag("__end").over(_tcw) == 1, F.lit(False)), '
            'F.lag("__tx").over(_tcw)))',
        ]
        if not eof_commit:
            lines.append(
                '_tc = _tc.withColumn("__rb", F.when(F.row_number().over(Window.orderBy(F.col("__ord").desc())) == 1, '
                'F.when(F.col("__end") == 0, F.col("__tx"))).otherwise(F.col("__rb")))'
                '  # Commit On End Of File = NO: the open transaction is rolled back'
            )
        lines += [
            '_tc_rolled = _tc.where(F.col("__rb").isNotNull()).select(F.col("__rb").alias("__tx")).distinct()',
            f'{out} = _tc.join(_tc_rolled, on="__tx", how="left_anti").orderBy("__ord")'
            f'.drop("__ord", "__tc", "__start", "__end", "__tx", "__rb")',
        ]
        return lines

    def _unsupported(
        self, tx: Transformation, in_df: str, out: str, _extra: dict
    ) -> list[str]:
        # Name what was actually in the export. ``tx.type.value`` is
        # "Unknown" for anything the resolver did not recognise, so reporting
        # only that told a migrator nothing about what it lost -- and the
        # pass-through below means the rows keep flowing as if nothing were
        # missing. The raw string is the one piece of evidence that
        # identifies the transformation, so it leads.
        if tx.type is TransformationType.UNKNOWN and tx.raw_type:
            label = f"{tx.raw_type} (unrecognised type)"
        elif tx.raw_type and tx.raw_type.strip().lower() != tx.type.value.lower():
            label = f"{tx.type.value} [export type: {tx.raw_type}]"
        else:
            label = tx.type.value
        return [
            f"# Unsupported transformation type: {label} ({tx.name})",
            "# TODO: manual conversion required -- rows pass through "
            "unchanged below, so this transformation's logic is NOT applied",
            f"{out} = {in_df}",
        ]


# ---------------------------------------------------------------------------
# Data tables for the generic Phase 4/6 handlers.
#
# Defined after the class so the enum members read plainly. Each entry is a
# decision about what a migrator should do next, which is the part that makes
# a refusal useful rather than merely honest.
# ---------------------------------------------------------------------------

TransformationConverter._CODE_CARRYING = {
    TransformationType.JAVA: (
        "Java transformation",
        ("java code", "code", "on input row", "java snippet"),
        "Java has no Spark translation -- the code IS the logic. Reimplement "
        "it as a PySpark UDF or, better, as DataFrame operations; a UDF "
        "blocks Catalyst optimisation and will be slower than the original.",
    ),
    TransformationType.PYTHON: (
        "Python transformation",
        ("python code", "code", "script"),
        "The body is already Python, but it is written against Informatica's "
        "row-at-a-time API, not a Spark DataFrame. Rework it as a "
        "pandas_udf for vectorised execution, or inline the logic as column "
        "expressions where it is simple enough.",
    ),
    TransformationType.VELOCITY: (
        "Velocity transformation",
        ("template", "velocity template", "script"),
        "Velocity is a template engine with no Spark equivalent. Rebuild the "
        "template with concat_ws/format_string, or render it outside Spark "
        "if it produces documents rather than column values.",
    ),
    TransformationType.CUSTOM: (
        "Custom transformation",
        ("module identifier", "function identifier", "class name", "library"),
        "A Custom transformation calls a compiled procedure that is not in "
        "the export. Obtain the procedure's source or specification from the "
        "source system and reimplement it -- there is no way to infer its "
        "behaviour from the mapping.",
    ),
    TransformationType.EXTERNAL_PROCEDURE: (
        "External Procedure",
        ("module identifier", "function identifier", "class name", "library"),
        "An External Procedure runs a compiled .so/.dll on the Integration "
        "Service host. The binary is not in the export and cannot be "
        "translated. Reimplement from its specification, or keep this step "
        "outside Spark.",
    ),
}

TransformationConverter._NO_EQUIVALENT = {
    TransformationType.CLEANSE: (
        "Cleanse applies Cloud Data Quality rules that live in CDQ assets, "
        "not in the mapping export.",
        "Simple operations (trim, case, padding) rebuild directly as column "
        "expressions; dictionary-driven standardisation needs the dictionary "
        "exported separately.",
    ),
    TransformationType.LABELER: (
        "Labeler classifies tokens against CDQ dictionaries and reference "
        "data held outside the mapping.",
        "Rebuild with regexp_extract or a broadcast join against the "
        "dictionary, once that dictionary has been exported.",
    ),
    TransformationType.PARSE: (
        "Parse splits values into fields using CDQ patterns and reference "
        "data that are not in the export.",
        "Rebuild with regexp_extract or split once the pattern set is known.",
    ),
    TransformationType.RULE_SPECIFICATION: (
        "A Rule Specification references a CDQ rule asset by name; the rule "
        "logic itself is not in the mapping export.",
        "Export the rule separately and reimplement it as column expressions.",
    ),
    TransformationType.VERIFIER: (
        "Verifier validates addresses against licensed reference data. There "
        "is no Spark equivalent and no way to approximate it -- an "
        "unverified address is not a verified one.",
        "Keep this step on Informatica, or call an address-verification "
        "service from the pipeline.",
    ),
    TransformationType.B2B: (
        "B2B parses and generates partner EDI/X12/EDIFACT documents using "
        "Informatica's B2B engine.",
        "No Spark equivalent exists. Keep it on Informatica or use a "
        "dedicated EDI library outside the Spark job.",
    ),
    TransformationType.DATA_SERVICES: (
        "Data Services exposes a mapping as a queryable virtual service -- a "
        "serving pattern, not a transformation.",
        "No Spark equivalent. Serve the equivalent data from an AIDP table or "
        "an API in front of it.",
    ),
    TransformationType.WEB_SERVICES: (
        "Web Services calls an external SOAP/REST endpoint per row.",
        "Rebuild as a batched API call -- per-row HTTP from Spark executors "
        "will rate-limit or overwhelm the endpoint. Collect the distinct "
        "inputs, call once, and join the results back.",
    ),
    TransformationType.WEB_SERVICES_CONSUMER: (
        "Web Services Consumer calls an external SOAP endpoint per row.",
        "Rebuild as a batched API call and join the results back rather than "
        "issuing one request per row from executors.",
    ),
    TransformationType.ACCESS_POLICY: (
        "Access Policy applies row and column entitlements at runtime.",
        "Express as AIDP roles and restricted views rather than as a "
        "pipeline step -- enforcement belongs in the catalog, not in the job "
        "that writes the data.",
    ),
    TransformationType.UNSTRUCTURED_DATA: (
        "Unstructured Data parses documents using an Informatica data "
        "transformation service that is not in the export.",
        "Replace with an explicit parser for the format concerned.",
    ),
    TransformationType.HTTP: (
        "HTTP calls an external endpoint per row.",
        "Rebuild as a batched call and join the results back; per-row HTTP "
        "from executors will rate-limit the endpoint.",
    ),
}


TransformationConverter._NO_EQUIVALENT.update({
    TransformationType.XML_PARSER: (
        "XML Parser shreds an XML document into relational rows. Spark has no "
        "built-in from_xml before Spark 4 -- it lives in the external "
        "spark-xml package, which may not be installed on the target cluster.",
        "Confirm the cluster's Spark version: on Spark 4 use from_xml with an "
        "explicit schema; otherwise add the spark-xml package, or convert the "
        "payload to JSON upstream and use from_json.",
    ),
    TransformationType.XML_GENERATOR: (
        "XML Generator builds an XML document from relational rows. Spark's "
        "to_xml has the same availability constraint as from_xml.",
        "Confirm the cluster's Spark version: on Spark 4 use to_xml; "
        "otherwise build the document with struct/to_json and transform, or "
        "generate it outside Spark if the output is a file rather than a "
        "column.",
    ),
})
