"""Convert Informatica expression language to PySpark equivalents.

Handles function mappings, operator translation, nested expressions,
date format conversion, and parameter variable references.
"""

import re
from typing import Optional

# datemask/decode are a shared seam: DECODE and
# TO_DATE/TO_CHAR's format-mask conversion are Class A (pure scalar
# expressions, no runtime state) so they stay emitted INLINE here -- but
# the token table and pair/fallthrough-splitting logic they depend on
# also has to be usable by Class C call sites in ``infa_compat`` without
# a second, driftable copy. Both modules live in ``infa_compat`` and are
# imported (not duplicated) here. See those modules' docstrings.
from infa2aidp.properties import is_truthy
from infa_compat.datemask import to_java_format as _convert_date_format
from infa_compat.decode import build_when_chain as _build_decode_chain
from infa_compat.decode import split_pairs_and_default as _split_decode_pairs


# Informatica date-unit tokens shared by ADD_TO_DATE / DATE_DIFF / TRUNC.
#
# One table instead of three independent per-function maps (previously each
# of the three converters carried its own partial unit list, and each one
# quietly fell back to day-granularity for anything it didn't recognize --
# e.g. 'Q' (quarter) silently became +1 *day*, an ~89-day error with no
# signal). Mirrors the upstream Rust `informatica_date_unit` table
# (the Rust reference implementation, src/recognize.rs ~:1410-1425): every unit maps to one of
# the eight canonical Spark ``date_trunc``-style families below, and an
# unrecognized unit must raise -- never silently default to another
# granularity. See ``ExpressionConverter._date_unit_family``.
_DATE_UNIT_FAMILY = {
    "D": "day", "DD": "day", "DDD": "day", "DAY": "day", "DY": "day", "J": "day",
    "HH": "hour", "HH12": "hour", "HH24": "hour",
    "MI": "minute",
    "MM": "month", "MON": "month", "MONTH": "month",
    "SS": "second", "SSSSS": "second",
    "W": "week", "WW": "week",
    "Q": "quarter",
    "Y": "year", "YY": "year", "YYY": "year", "YYYY": "year",
    "RR": "year", "SYYYY": "year",
}


class _Tokenizer:
    """Break an Informatica expression into tokens.

    Informatica string literals escape an embedded quote by doubling it
    (``'it''s'``), not with a backslash -- the STRING pattern below matches
    that form so the literal is one token, not two adjacent ones.

    Every character of the input must belong to some token. The previous
    ``finditer`` loop silently skipped anything no pattern matched, so a
    stray ``%``, ``:`` or a single ``$`` simply vanished and the rest of the
    expression was converted as if it had never been there (``A % 2``
    became ``F.col('A')``). An unmatched character now raises so the
    caller turns the whole expression into a review item.
    """

    _PATTERNS = [
        ("STRING", r"'(?:[^']|'')*'"),
        ("NUMBER", r"\d+(?:\.\d+)?"),
        ("BUILTIN", r"\$\$\$\w+"),
        ("PARAM", r"\$\$\w+"),
        ("SYSVAR", r"\$\w+"),
        ("IDENT", r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*"),
        ("OP2", r"<>|>=|<=|!=|\|\||:="),
        ("OP1", r"[+\-*/=<>(),]"),
        ("WS", r"\s+"),
        ("BAD", r"."),
    ]
    _RE = re.compile("|".join(f"(?P<{n}>{p})" for n, p in _PATTERNS), re.DOTALL)

    def __init__(self, text: str):
        self.tokens: list[tuple[str, str]] = []
        for m in self._RE.finditer(text):
            kind = m.lastgroup
            if kind == "WS":
                continue
            if kind == "BAD":
                raise UnconvertibleExpression(
                    f"unexpected character {m.group()!r} at offset {m.start()}"
                )
            self.tokens.append((kind, m.group()))
        self.pos = 0

    def peek(self) -> Optional[tuple[str, str]]:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def advance(self) -> tuple[str, str]:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def expect(self, value: str) -> None:
        tok = self.advance()
        if tok[1] != value:
            raise ValueError(f"Expected '{value}', got '{tok[1]}'")


def strip_comments(expr: str) -> str:
    """Remove Informatica expression comments.

    Informatica's expression editor accepts ``--`` and ``//`` comments that
    run to the end of the line, and real mappings use them freely. Left in
    place they tokenize as two ``-`` (or ``/``) operators and the whole
    expression is refused, so a mapping loses a correct expression over a
    remark. Comment markers inside a string literal are left alone.
    """
    out: list[str] = []
    i = 0
    n = len(expr)
    in_string = False
    while i < n:
        ch = expr[i]
        if in_string:
            out.append(ch)
            if ch == "'":
                # a doubled quote is an escaped quote, still inside the string
                if i + 1 < n and expr[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            out.append(ch)
            i += 1
            continue
        if expr.startswith("--", i) or expr.startswith("//", i):
            nl = expr.find("\n", i)
            if nl == -1:
                break
            i = nl
            continue
        out.append(ch)
        i += 1
    return "".join(out)


class UnconvertibleExpression(Exception):
    """An Informatica expression that ExpressionConverter cannot confidently
    translate to PySpark.

    Raised instead of returning a "# TODO ..." string. A comment fragment
    returned as an expression value silently corrupts the *enclosing*
    generated statement whenever it's nested inside another call (the
    trailing/leading "#" comments out everything after it on that line --
    see). Raising lets the failure abort the whole
    expression cleanly; the caller (typically transformation_converter.py)
    catches this and turns it into an explicit review item plus a safe
    ``F.lit(None)`` placeholder -- never a silent guess and never broken
    Python.
    """


# Scale used for TO_DECIMAL when the expression gives none. Informatica
# would keep the input's own scale; Spark must be told a number at cast
# time. Ten matches the scale in Oracle's published Spark-to-Oracle
# mapping for AIDP (NUMBER(38,10)), so a value cast here survives the
# eventual write without a second rounding step.
_DEFAULT_DECIMAL_SCALE = 10

_NUMERIC_LITERAL = re.compile(r"^\(?\s*-?\d+(?:\.\d+)?\s*\)?$")


def _as_column(expr: str) -> str:
    """Wrap a bare numeric literal so it is a Column, leave anything else.

    Several Spark builtins accept only Columns. The converter emits numeric
    literals bare (``1``) and column references already wrapped
    (``F.col('N')``), so only the former needs lifting -- wrapping a
    Column in ``F.lit`` again is an error of its own.
    """
    s = expr.strip()
    if _NUMERIC_LITERAL.match(s):
        return f"F.lit({s.strip('()').strip()})"
    return s


class ExpressionConverter:
    """Convert a single Informatica expression string to PySpark code."""

    # Simple 1:1 function name replacements (single argument)
    _SIMPLE_FUNCS = {
        "LENGTH": "F.length",
        "UPPER": "F.upper",
        "LOWER": "F.lower",
        "REVERSE": "F.reverse",
        "ABS": "F.abs",
        "MD5": "F.md5",
        "ISNULL": "F.isnull",
        "CEIL": "F.ceil",
        "FLOOR": "F.floor",
        "SQRT": "F.sqrt",
        "EXP": "F.exp",
        "LN": "F.log",
        "SIGN": "F.signum",
        "ASCII": "F.ascii",
        "LAST_DAY": "F.last_day",
        "SOUNDEX": "F.soundex",
    }

    # Informatica's Update Strategy constants. They are integer codes in
    # the expression language, so they must convert to integer literals --
    # as bare identifiers they came out as F.col('DD_INSERT'), a column
    # that does not exist, and the caller then had no way to tell which
    # branch of an IIF(...) meant insert and which meant update.
    _DD_CONSTANTS = {"DD_INSERT": 0, "DD_UPDATE": 1, "DD_DELETE": 2, "DD_REJECT": 3,
                     # Transaction Control constants, the same way
                     "TC_CONTINUE_TRANSACTION": 0, "TC_COMMIT_BEFORE": 1, "TC_COMMIT_AFTER": 2,
                     "TC_ROLLBACK_BEFORE": 3, "TC_ROLLBACK_AFTER": 4}

    # Session-start built-ins. Informatica evaluates these ONCE per session,
    # never per row; the generated notebook's setup cell binds
    # ``_SESSION_START_TIME`` once for the same reason.
    _SESSION_START_NAMES = {"SESSSTARTTIME", "$$$SESSSTARTTIME"}

    def convert(self, infa_expression: str) -> str:
        """Convert an Informatica expression to a PySpark expression string.

        Args:
            infa_expression: Raw Informatica expression text.

        Returns:
            PySpark code string using ``pyspark.sql.functions as F`` style.
        """
        if not infa_expression or not infa_expression.strip():
            return ""
        expr = strip_comments(infa_expression).strip()
        if not expr:
            return ""

        # Handle Informatica unconnected lookups (:LKP.LKP_NAME(args)) BEFORE
        # tokenizing — the leading colon is not a valid identifier start.
        # We rewrite the expression to a helper call that documents the lookup
        # intent and keeps the broadcast-join structure visible to a reviewer.
        expr = self._rewrite_unconnected_lookups(expr)

        tokenizer = _Tokenizer(expr)
        try:
            result = self._parse_or(tokenizer)
        except UnconvertibleExpression:
            # Already a well-formed "can't confidently convert this" signal
            # -- propagate as-is so the caller can turn it into a review
            # item (never swallow it into a "#..." string here; that's the
            # exact defect this exception type replaces).
            raise
        except Exception as exc:
            # ANY other failure -- a tokenizer/parser bug, a malformed
            # expression, whatever -- must become the same flagged-failure
            # signal, not an escaping crash (previously only IndexError/
            # ValueError were caught, so e.g. AttributeError from a typo'd
            # tokenizer attribute name reached the caller uncaught) and not
            # a silent pass (returning something that looks like valid code).
            raise UnconvertibleExpression(
                f"could not convert '{expr}': {type(exc).__name__}: {exc}"
            ) from exc
        return result

    @staticmethod
    def _rewrite_unconnected_lookups(expr: str) -> str:
        """Replace ':LKP.LKP_<NAME>(arg1, arg2, ...)' calls with a recognizable
        helper function reference. The transformation-level converter is
        expected to emit a corresponding helper definition (broadcast-join
        lookup) so the resulting notebook is runnable end-to-end.

        Pattern matched: :LKP.<IDENT>(...)  (case-sensitive on the :LKP. prefix
        per Informatica's syntax; lookup name itself can be any identifier).
        """
        # Match :LKP.NAME(  — capture name; argument list parsed by main parser
        return re.sub(
            r":LKP\.([A-Za-z_][A-Za-z0-9_]*)\(",
            lambda m: f"UNCONNECTED_LOOKUP_{m.group(1)}(",
            expr,
        )

    # ------------------------------------------------------------------ parser
    # Precedence (low to high): OR, AND, NOT, comparison, ||, add/sub, mul/div,
    # unary, atom (literal / function call / parenthesized expr)
    #
    # ``||`` sits BELOW arithmetic (Informatica: unary > */ > +- > || >
    # comparison > NOT > AND > OR), so it must split its operands before
    # add/sub ever runs -- not below mul/div as this chain previously had
    # it. ``'x' || A + B`` must convert to ``concat('x', A + B)`` (the whole
    # ``A + B`` is one concat operand), not ``concat('x', A) + B`` (
    # Step 2; matches the split-top-level-``||``-first approach in the
    # upstream Rust ``rewrite_concat``, the Rust reference implementation, src/recognize.rs
    # ~:955-1010).

    def _parse_or(self, t: _Tokenizer) -> str:
        left = self._parse_and(t)
        while t.peek() and t.peek()[1].upper() == "OR":
            t.advance()
            right = self._parse_and(t)
            left = f"({left}) | ({right})"
        return left

    def _parse_and(self, t: _Tokenizer) -> str:
        left = self._parse_not(t)
        while t.peek() and t.peek()[1].upper() == "AND":
            t.advance()
            right = self._parse_not(t)
            left = f"({left}) & ({right})"
        return left

    def _parse_not(self, t: _Tokenizer) -> str:
        if t.peek() and t.peek()[1].upper() == "NOT":
            t.advance()
            operand = self._parse_comparison(t)
            return f"~({operand})"
        return self._parse_comparison(t)

    def _parse_comparison(self, t: _Tokenizer) -> str:
        left = self._parse_concat(t)

        # Handle IN (...) — e.g., STATUS IN ('COMPLETED','SHIPPED')
        # .isin() expects plain Python values, not F.lit() wrapped Column objects
        if t.peek() and t.peek()[1].upper() == "IN":
            t.advance()  # consume IN
            t.expect("(")
            values = []
            values.append(self._parse_or(t))
            while t.peek() and t.peek()[1] == ",":
                t.advance()
                values.append(self._parse_or(t))
            t.expect(")")
            # Strip F.lit() wrappers — .isin() wants plain Python values
            plain_values = [self._strip_lit(v) for v in values]
            val_list = ", ".join(plain_values)
            return f"{left}.isin({val_list})"

        # Handle NOT IN (...)
        if (t.peek() and t.peek()[1].upper() == "NOT"
                and t.pos + 1 < len(t.tokens) and t.tokens[t.pos + 1][1].upper() == "IN"):
            t.advance()  # consume NOT
            t.advance()  # consume IN
            t.expect("(")
            values = []
            values.append(self._parse_or(t))
            while t.peek() and t.peek()[1] == ",":
                t.advance()
                values.append(self._parse_or(t))
            t.expect(")")
            plain_values = [self._strip_lit(v) for v in values]
            val_list = ", ".join(plain_values)
            return f"~{left}.isin({val_list})"

        ops = {"=": "==", "<>": "!=", "!=": "!=", ">": ">", "<": "<",
               ">=": ">=", "<=": "<="}
        if t.peek() and t.peek()[1] in ops:
            op_tok = t.advance()[1]
            right = self._parse_concat(t)
            # Plain three-valued comparison, on purpose. Informatica's
            # comparison operators return NULL when either side is NULL,
            # and IIF/Filter/Router then treat that NULL as FALSE -- which
            # is exactly what Spark's ``==``/``!=`` inside when()/filter()
            # do. The previous eqNullSafe() emission made ``NULL = NULL``
            # TRUE and ``NULL <> 'X'`` TRUE, so a Filter on
            # ``STATUS <> 'CLOSED'`` passed rows with a NULL status that
            # Informatica drops, and an SCD change-detect on
            # ``OLD_VAL <> NEW_VAL`` flagged a NULL->NULL row as changed.
            return f"({left} {ops[op_tok]} {right})"
        return left

    def _parse_concat(self, t: _Tokenizer) -> str:
        """Handle ``||`` concatenation operator.

        Binds looser than +/-/*/ -- each operand is parsed via
        ``_parse_add`` so a run of arithmetic on either side of a ``||``
        stays together as ONE concat operand.
        """
        left = self._parse_add(t)
        parts = [left]
        while t.peek() and t.peek()[1] == "||":
            t.advance()
            parts.append(self._parse_add(t))
        if len(parts) > 1:
            # Same NULL rule as the CONCAT function below: Informatica's
            # ``||`` ignores a NULL operand and returns the other side, so
            # ``FIRST || ' ' || MIDDLE || ' ' || LAST`` yields a name for a
            # row with no middle name. Spark's concat() returns NULL if
            # ANY operand is NULL; concat_ws skips NULLs.
            return self._null_skipping_concat(parts)
        return left

    @staticmethod
    def _null_skipping_concat(parts: list[str]) -> str:
        """Informatica CONCAT / ``||``: a NULL operand is skipped, and the
        result is NULL only when EVERY operand is NULL ("If both strings are
        NULL, CONCAT returns NULL"). concat_ws alone returned '' there."""
        joined = ", ".join(parts)
        if any(p.strip().startswith("F.lit(") and p.strip() != "F.lit(None)" for p in parts):
            # A non-NULL literal operand: the result is never NULL.
            return f"F.concat_ws('', {joined})"
        as_text = ", ".join(f"({p}).cast('string')" for p in parts)
        return (
            f"F.when(F.coalesce({as_text}).isNull(), F.lit(None).cast('string'))"
            f".otherwise(F.concat_ws('', {joined}))"
        )

    def _parse_add(self, t: _Tokenizer) -> str:
        left = self._parse_mul(t)
        while t.peek() and t.peek()[1] in ("+", "-"):
            op = t.advance()[1]
            right = self._parse_mul(t)
            left = f"{left} {op} {right}"
        return left

    def _parse_mul(self, t: _Tokenizer) -> str:
        left = self._parse_unary(t)
        while t.peek() and t.peek()[1] in ("*", "/"):
            op = t.advance()[1]
            right = self._parse_unary(t)
            left = f"{left} {op} {right}"
        return left

    def _parse_unary(self, t: _Tokenizer) -> str:
        if t.peek() and t.peek()[1] == "-":
            t.advance()
            operand = self._parse_atom(t)
            return f"-{operand}"
        return self._parse_atom(t)

    def _parse_atom(self, t: _Tokenizer) -> str:
        tok = t.peek()
        if tok is None:
            return ""

        kind, val = tok

        # Parenthesized sub-expression
        if val == "(":
            t.advance()
            inner = self._parse_or(t)
            t.expect(")")
            return f"({inner})"

        # Bare "*" -- only legal here as the COUNT(*) wildcard argument
        # (binary multiplication never reaches _parse_atom for a lone "*"
        # since it always has a preceding left operand). Emit it as a
        # quoted string so F.count(*) doesn't come out as invalid Python
        # syntax -- F.count("*") is the correct PySpark idiom.
        if val == "*":
            t.advance()
            return '"*"'

        # String literal -- emitted as a Python literal via repr(), after
        # un-doubling Informatica's '' quote escape. Splicing the raw
        # Informatica text into Python turned '\t' in a path into a tab.
        if kind == "STRING":
            t.advance()
            return f"F.lit({self._string_literal(val)!r})"

        # Numeric literal
        if kind == "NUMBER":
            t.advance()
            return val

        # Parameter variable
        if kind == "PARAM":
            t.advance()
            return self._convert_param(val)

        # $$$SessStartTime and the other triple-dollar built-ins
        if kind == "BUILTIN":
            t.advance()
            if val.upper() in self._SESSION_START_NAMES:
                return "F.lit(_SESSION_START_TIME)"
            raise UnconvertibleExpression(
                f"built-in variable {val} has no Spark equivalent"
            )

        # $PMSessionName, $PMWorkflowName, $Source, $Target ... -- session
        # and service variables with no per-row value in Spark. Refusing is
        # better than the previous behaviour, where the '$' was skipped and
        # 'PMSessionName' became a column reference.
        if kind == "SYSVAR":
            raise UnconvertibleExpression(
                f"session/service variable {val} has no Spark equivalent; "
                f"pass it in as a migration parameter"
            )

        # NULL keyword
        if kind == "IDENT" and val.upper() == "NULL":
            t.advance()
            return "F.lit(None)"

        # TRUE / FALSE
        if kind == "IDENT" and val.upper() in ("TRUE", "FALSE"):
            t.advance()
            return f"F.lit({val.upper() == 'TRUE'})"

        # SYSDATE
        if kind == "IDENT" and val.upper() == "SYSDATE":
            t.advance()
            return "F.current_timestamp()"

        # SESSSTARTTIME -- one value per session, not per row
        if kind == "IDENT" and val.upper() in self._SESSION_START_NAMES:
            t.advance()
            return "F.lit(_SESSION_START_TIME)"

        # DD_INSERT / DD_UPDATE / DD_DELETE / DD_REJECT
        if kind == "IDENT" and val.upper() in self._DD_CONSTANTS:
            t.advance()
            return f"F.lit({self._DD_CONSTANTS[val.upper()]})"

        # Function call or plain field reference
        if kind == "IDENT":
            t.advance()
            if t.peek() and t.peek()[1] == "(":
                return self._parse_function(val.upper(), t)
            return self._col_ref(val)

        # Fallback -- consume and return raw
        t.advance()
        return val

    # -------------------------------------------------------------- functions

    def _parse_args(self, t: _Tokenizer) -> list[str]:
        """Parse comma-separated argument list inside parentheses."""
        t.expect("(")
        args: list[str] = []
        if t.peek() and t.peek()[1] == ")":
            t.advance()
            return args
        args.append(self._parse_or(t))
        while t.peek() and t.peek()[1] == ",":
            t.advance()
            args.append(self._parse_or(t))
        t.expect(")")
        return args

    def _parse_function(self, name: str, t: _Tokenizer) -> str:
        args = self._parse_args(t)

        # Unconnected-lookup helper call (rewritten upstream from :LKP.<NAME>(...))
        # Produce a broadcast-join lookup expression that's syntactically valid
        # and documents the original Informatica intent. The reviewer/integrator
        # must wire the actual lookup table — but the emitted code structure
        # is correct and there are no silent NULLs.
        if name.startswith("UNCONNECTED_LOOKUP_"):
            lkp_name = name[len("UNCONNECTED_LOOKUP_"):]
            # Build a join-key expression — args correspond to the lookup's
            # condition ports, in declaration order. NOTE: this must be a
            # clean expression with no trailing comment -- a "# ..." suffix
            # here is only safe when this call happens to be the *entire*
            # top-level expression. When it's nested inside another function
            # (e.g. UPPER(:LKP.LKP_X(a))) the "#" comments out everything
            # after it, including the enclosing call's closing paren (Task
            # 21 Step 4). The reviewer note about wiring the lookup table
            # lives in the helper definition itself, not spliced in here.
            # An unconnected lookup is resolved into a join by the
            # Expression converter, which has the lookup's definition. Here
            # there is none (a Filter, an Aggregator, a lookup not in the
            # export): a helper call nothing defines would be a NameError on
            # the cluster, so it is refused instead.
            raise UnconvertibleExpression(
                f":LKP.{lkp_name}({', '.join(args)}) -- the unconnected lookup's "
                f"definition is not available here; convert the call by hand"
            )

        # Simple 1:1 mappings (single argument)
        if name in self._SIMPLE_FUNCS:
            if len(args) != 1:
                raise UnconvertibleExpression(
                    f"{name} takes exactly 1 argument, got {len(args)}: {args}"
                )
            fn = self._SIMPLE_FUNCS[name]
            return f"{fn}({args[0]})"

        if name == "INITCAP":
            if len(args) != 1:
                raise UnconvertibleExpression(f"INITCAP takes exactly 1 argument: {args}")
            return self._convert_initcap(args[0])
        if name in ("LTRIM", "RTRIM", "TRIM"):
            return self._convert_trim(name, args)
        if name == "IIF":
            return self._convert_iif(args)
        if name == "DECODE":
            return self._convert_decode(args)
        if name == "IN":
            return self._convert_in_function(args)
        if name == "NVL":
            return self._convert_nvl(args)
        if name == "NVL2":
            return self._convert_nvl2(args)
        if name == "TO_DATE":
            return self._convert_to_date(args)
        if name == "TO_CHAR":
            return self._convert_to_char(args)
        if name in ("LPAD", "RPAD"):
            return self._convert_pad(name, args)
        if name == "SUBSTR":
            return self._convert_substr(args)
        if name == "INSTR":
            return self._convert_instr(args)
        if name == "CONCAT":
            # NOT F.concat. Informatica's CONCAT ignores a NULL argument and
            # returns the other; Spark's concat() returns NULL if ANY
            # argument is NULL. A mapping that builds FULL_NAME from
            # FIRST || MIDDLE || LAST would produce NULL for every row with
            # no middle name -- a wrong result with no error.
            return self._null_skipping_concat(args)
        if name == "ROUND":
            return self._convert_round(args)
        if name == "TRUNC":
            return self._convert_trunc(args)
        if name == "MOD":
            if len(args) != 2:
                raise UnconvertibleExpression(f"MOD requires 2 arguments, got {len(args)}: {args}")
            # Informatica MOD keeps the sign of the dividend, like Spark's %.
            return f"({args[0]} % {args[1]})"
        if name == "LOG":
            # LOG(base, exponent) -- F.log(base, col) needs a Python float base
            if len(args) != 2:
                raise UnconvertibleExpression(f"LOG requires 2 arguments, got {len(args)}: {args}")
            base = self._strip_lit(args[0])
            if not re.fullmatch(r"\d+(?:\.\d+)?", base):
                raise UnconvertibleExpression(
                    f"LOG base must be a numeric literal for F.log(base, col); got {args[0]}"
                )
            return f"F.log({base}, {args[1]})"
        if name == "ADD_TO_DATE":
            return self._convert_add_to_date(args)
        if name == "ADD_MONTHS":
            # ADD_MONTHS(date, num_months) -- keep the time-of-day, which
            # F.add_months (DATE result) would drop.
            if len(args) >= 2:
                return f"({args[0]} + {self._interval('month', args[1])})"
            raise UnconvertibleExpression(
                f"ADD_MONTHS requires 2 arguments, got {len(args)}: {args}"
            )
        if name == "GET_DATE_PART":
            return self._convert_get_date_part(args)
        if name == "DATE_COMPARE":
            if len(args) != 2:
                raise UnconvertibleExpression(f"DATE_COMPARE requires 2 arguments, got {len(args)}")
            a, b = args
            return (
                f"F.when({a} < {b}, F.lit(-1)).when({a} > {b}, F.lit(1))"
                f".when({a} == {b}, F.lit(0)).otherwise(F.lit(None))"
            )
        if name == "POWER":
            return f"F.pow({', '.join(args)})"
        if name in ("SETMAXVARIABLE", "SETMINVARIABLE"):
            # Returns the higher (lower) of the variable's current value and
            # the argument: a running max/min in row order, starting from
            # the variable's start value. Persisting it at session end is
            # session-level and is reported by the caller's divergence note.
            if len(args) != 2:
                raise UnconvertibleExpression(f"{name} takes 2 arguments: {args}")
            agg, pick = ("max", "greatest") if name == "SETMAXVARIABLE" else ("min", "least")
            window = ("Window.orderBy(F.monotonically_increasing_id())"
                      ".rowsBetween(Window.unboundedPreceding, Window.currentRow)")
            return (f"F.when({args[1]}.isNull(), F.lit(None)).otherwise("
                    f"F.{pick}({args[0]}.cast('double'), F.{agg}({args[1]}).over({window})"
                    f".cast('double')))")
        if name == "SETVARIABLE":
            if len(args) != 2:
                raise UnconvertibleExpression(f"SETVARIABLE takes 2 arguments: {args}")
            return args[1]
        if name == "REG_MATCH":
            return self._convert_reg_match(args)
        if name in ("REPLACECHR", "REPLACESTR"):
            return self._convert_replace(name, args)
        if name == "IS_SPACES":
            return self._convert_is_spaces(args)
        if name == "IS_NUMBER":
            # NULL in, NULL out. `.isNotNull()` only ever returns True or
            # False, so a NULL input came back FALSE -- which reads as
            # "this is not a number" and collapses Informatica's
            # three-valued logic into two. IS_SPACES already propagated
            # NULL, so the three IS_* predicates disagreed with each other.
            #
            # The NULL rule is documented for IS_SPACES; applying it to
            # IS_NUMBER and IS_DATE is inference from the same family
            # rather than a separately documented fact. The internal
            # inconsistency was a defect whichever way it resolved.
            if not args:
                return ""
            return f"F.when({args[0]}.isNotNull(), {args[0]}.cast('double').isNotNull())"
        if name == "IS_DATE":
            if not args:
                return ""
            if len(args) > 1:
                fmt = _convert_date_format(self._extract_string_literal(args[1]))
                inner = f"F.to_timestamp({args[0]}, {fmt!r}).isNotNull()"
            else:
                inner = f"F.to_timestamp({args[0]}).isNotNull()"
            return f"F.when({args[0]}.isNotNull(), {inner})"  # same NULL rule
        if name == "DATE_DIFF":
            return self._convert_date_diff(args)
        if name in ("TO_INTEGER", "TO_BIGINT"):
            return self._convert_to_integer(name, args)
        if name == "TO_FLOAT":
            return f"{self._castable(args[0])}.cast('double')" if args else ""
        if name == "TO_DECIMAL":
            return self._convert_to_decimal(args)

        if name in ("GREATEST", "LEAST"):
            return self._convert_greatest_least(name, args)
        if name == "SHA256":
            # Same input-encoding caveat as MD5; flagged as divergent.
            return f"F.sha2({args[0]}, 256)" if args else ""
        if name == "REG_REPLACE":
            # Informatica's regex engine is PCRE-like, Spark's is
            # java.util.regex. Most patterns are portable; the difference is
            # reported rather than assumed away.
            if len(args) < 3:
                raise UnconvertibleExpression(
                    f"REG_REPLACE needs subject, pattern and replacement: {args}"
                )
            return f"F.regexp_replace({args[0]}, {args[1]}, {args[2]})"
        if name == "REG_EXTRACT":
            # REG_EXTRACT(subject, pattern, subPatternNum). Spark's
            # regexp_extract takes the group index as a plain int, not a
            # Column, so a non-literal group cannot be translated.
            if len(args) < 2:
                raise UnconvertibleExpression(
                    f"REG_EXTRACT needs at least a subject and pattern: {args}"
                )
            group = "1"
            if len(args) > 2:
                if not _NUMERIC_LITERAL.match(args[2].strip()):
                    raise UnconvertibleExpression(
                        f"REG_EXTRACT group number {args[2]} is not a literal; "
                        f"Spark's regexp_extract takes a plain integer index"
                    )
                group = args[2].strip().strip("()").strip()
            # regexp_extract's pattern must be a plain str, not a Column --
            # `F.regexp_extract(col, F.lit('p'), 1)` raises "Column is not
            # iterable" at run time. Same trap as locate(). regexp_replace
            # accepts either, which is why only this one needs unwrapping.
            if not self._is_string_literal(args[1]):
                raise UnconvertibleExpression(
                    f"REG_EXTRACT pattern {args[1]} is not a literal; Spark's "
                    f"regexp_extract takes a plain string pattern and cannot "
                    f"take it from a column"
                )
            pattern = repr(self._extract_string_literal(args[1]))
            return f"F.regexp_extract({args[0]}, {pattern}, {group})"
        if name == "SYSTIMESTAMP":
            # Informatica re-evaluates SYSTIMESTAMP per row; Spark's
            # current_timestamp is fixed for the whole query. Reported.
            return "F.current_timestamp()"
        if name == "UUID_STRING":
            # F.uuid() only exists in newer PySpark; the SQL function is
            # available on every runtime this targets.
            return "F.expr('uuid()')"
        if name == "RAND":
            # Seedable in both, but the PRNGs differ, so the same seed does
            # NOT reproduce Informatica's sequence. Reported.
            if args:
                if not _NUMERIC_LITERAL.match(args[0].strip()):
                    raise UnconvertibleExpression(
                        f"RAND seed {args[0]} is not a literal; Spark's rand "
                        f"takes a plain integer seed"
                    )
                return f"F.rand({args[0].strip().strip('()').strip()})"
            return "F.rand()"
        if name == "INDEXOF":
            # INDEXOF(valueToSearch, string1, string2, ...) -> 1-based
            # position, 0 when absent, NULL for a NULL search value.
            # array_position has those properties too, but on Spark 3.5
            # (what AIDP runs) pyspark's array_position accepts only a
            # Python literal as the value -- a Column raises "Column is not
            # iterable" -- so the array form ran only on Spark 4. A CASE
            # chain has the same semantics on every version.
            if len(args) < 2:
                raise UnconvertibleExpression(
                    f"INDEXOF needs a search value and at least one candidate: {args}"
                )
            value = args[0]
            chain = "".join(
                f".when({value} == {cand}, F.lit({i}))" for i, cand in enumerate(args[1:], 1)
            )
            return f"F.when(({value}).isNull(), F.lit(None)){chain}.otherwise(F.lit(0))"
        if name in ("ABORT", "ERROR"):
            # F.raise_error exists, but the semantics do not match and the
            # difference is not cosmetic. Informatica's ERROR skips the ROW
            # and writes it to the reject file, letting the session finish;
            # ABORT stops the session and rolls back. raise_error fails the
            # Spark TASK, which the cluster then retries before failing the
            # whole job. Emitting it would turn a row-level reject into an
            # aborted run.
            raise UnconvertibleExpression(
                f"{name} has no Spark equivalent with the same semantics: "
                f"Informatica {'skips the row and continues' if name == 'ERROR' else 'stops the session and rolls back'}, "
                f"while F.raise_error fails the Spark task and then the job. "
                f"Route rejected rows to a reject table instead"
            )
        if name in ("AES_ENCRYPT", "AES_DECRYPT", "AES_GCM_ENCRYPT", "AES_GCM_DECRYPT"):
            # Spark has aes_encrypt/aes_decrypt, but key derivation, mode and
            # padding differ from Informatica's, so ciphertext is not
            # interchangeable. Encrypting fresh data would work; DECRYPTING
            # data Informatica wrote would silently produce garbage or fail.
            # Not a mapping this converter can make safely.
            raise UnconvertibleExpression(
                f"{name} cannot be translated safely: Spark's AES functions "
                f"use different key derivation, mode and padding, so ciphertext "
                f"is not interchangeable with Informatica's. Re-encrypt the data "
                f"rather than trying to match the existing ciphertext"
            )

        # Aggregate functions (used inside Aggregator expressions)
        agg_map = {
            "SUM": "F.sum", "COUNT": "F.count", "AVG": "F.avg",
            "MIN": "F.min", "MAX": "F.max", "FIRST": "F.first",
            "LAST": "F.last", "MEDIAN": "F.percentile",
            "STDDEV": "F.stddev", "VARIANCE": "F.variance",
            "PERCENTILE": "F.percentile",
        }
        if name in agg_map:
            fn = agg_map[name]
            if name == "MEDIAN":
                return f"{fn}({args[0]}, 0.5)" if args else f"{fn}()"
            if name == "PERCENTILE":
                # PERCENTILE(value, percentile) -- Informatica's second
                # argument is 0..100, Spark's is 0..1.
                if len(args) < 2:
                    raise UnconvertibleExpression(f"PERCENTILE requires 2 arguments, got {len(args)}")
                pct = self._strip_lit(args[1])
                if not re.fullmatch(r"\d+(?:\.\d+)?", pct):
                    raise UnconvertibleExpression(
                        f"PERCENTILE needs a numeric literal percentile, got {args[1]}"
                    )
                return f"{fn}({args[0]}, {float(pct) / 100.0})"
            if len(args) > 1 and name not in ("COUNT",):
                # Every aggregate above takes an optional trailing filter
                # condition in Informatica (SUM(x, cond)); Spark has no such
                # argument, so the condition must become a when().
                value, cond = args[0], args[1]
                return f"{fn}(F.when({cond}, {value}))"
            return f"{fn}({', '.join(args)})"

        # Unknown function -- flag for manual review rather than emitting a
        # "# TODO ...\n<call>" placeholder: that string, once interpolated
        # into a generated withColumn(...)/filter(...) call, has the
        # comment marker land MID-LINE and silently comment out the rest of
        # the statement.
        raise UnconvertibleExpression(
            f"unmapped function {name}({', '.join(args)})"
        )

    # --------------------------------------------------- specific converters

    def _convert_iif(self, args: list[str]) -> str:
        if len(args) < 2:
            raise UnconvertibleExpression(f"IIF with insufficient args: {args}")
        cond, true_val = args[0], args[1]
        if len(args) > 2:
            false_val = args[2]
        else:
            # Omitted value2: "0 if value1 is a Numeric datatype, an empty
            # string if value1 is a String, NULL if value1 is a Date/Time"
            # (Transformation Language Reference). Decidable here only when
            # value1 is a literal; otherwise NULL, as before.
            v = self._strip_lit(true_val).strip()
            if re.fullmatch(r"-?\d+(?:\.\d+)?", v):
                false_val = "F.lit(0)"
            elif len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                false_val = "F.lit('')"
            else:
                false_val = "F.lit(None)"
        return f"F.when({cond}, {true_val}).otherwise({false_val})"

    def _convert_decode(self, args: list[str]) -> str:
        """Convert Informatica DECODE to a chained PySpark expression.

        The pair/default-splitting and chain-assembly logic lives in
        ``infa_compat.decode`` (a shared seam) -- this
        method only supplies the already-recursively-converted argument
        strings.
        """
        if len(args) < 3:
            raise UnconvertibleExpression(f"DECODE with insufficient args: {args}")
        val = args[0]
        pairs, default = _split_decode_pairs(args[1:])
        default_expr = default if default is not None else "F.lit(None)"
        return _build_decode_chain(val, pairs, default_expr)

    def _convert_in_function(self, args: list[str]) -> str:
        """``IN(value, v1, v2, ... [, CaseFlag])`` -- Informatica's IN is a
        function, not an operator. A trailing 0/1 CaseFlag is accepted only
        as 0 (case-sensitive, the default); 1 asks for case-insensitive
        matching which has no direct isin() form.
        """
        if len(args) < 2:
            raise UnconvertibleExpression(f"IN requires a value and at least one candidate: {args}")
        value, candidates = args[0], list(args[1:])
        if len(candidates) >= 2 and candidates[-1].strip() in ("0", "1"):
            flag = candidates.pop().strip()
            if flag == "1":
                raise UnconvertibleExpression(
                    "IN(..., CaseFlag=1) asks for case-insensitive matching; "
                    "wrap the value and candidates in UPPER() and rewrite by hand"
                )
        plain = [self._strip_lit(c) for c in candidates]
        return f"{value}.isin({', '.join(plain)})"

    def _convert_trim(self, name: str, args: list[str]) -> str:
        """LTRIM/RTRIM(string[, trim_set]) and TRIM(string).

        Spark's ``F.ltrim``/``F.rtrim`` take one argument; the two-argument
        Informatica form was previously passed straight through and failed
        at run time with a TypeError. A literal trim set becomes a regex
        character class; a non-literal one cannot be built here.
        """
        if not args:
            raise UnconvertibleExpression(f"{name} with no args")
        if len(args) == 1:
            return f"F.{name.lower()}({args[0]})"
        if len(args) != 2 or name == "TRIM":
            raise UnconvertibleExpression(
                f"{name} takes 1 or 2 arguments, got {len(args)}: {args}"
            )
        trim_set = args[1].strip()
        if not (trim_set.startswith("F.lit(") and trim_set.endswith(")")):
            raise UnconvertibleExpression(
                f"{name} with a non-literal trim set ({trim_set}) cannot be "
                f"translated; Spark trims a fixed character set only"
            )
        chars = self._extract_string_literal(trim_set)
        cls = "[" + "".join("\\" + c if c in "\\]^-[" else c for c in chars) + "]+"
        pattern = f"^{cls}" if name == "LTRIM" else f"{cls}$"
        return f"F.regexp_replace({args[0]}, {pattern!r}, '')"

    def _convert_pad(self, name: str, args: list[str]) -> str:
        """LPAD/RPAD(string, length[, pad_string]) -- pad_string defaults to
        a single space in Informatica; Spark's lpad/rpad require it and
        take it as a plain str, not a Column.
        """
        if len(args) < 2:
            raise UnconvertibleExpression(f"{name} requires at least 2 arguments, got {len(args)}: {args}")
        pad = self._strip_lit(args[2]) if len(args) > 2 else "' '"
        length = self._strip_lit(args[1])
        return f"F.{name.lower()}({args[0]}, {length}, {pad})"

    def _convert_round(self, args: list[str]) -> str:
        """ROUND(numeric[, precision]) -- the date form ROUND(date, 'MM')
        has no direct Spark equivalent and is refused rather than emitted
        as F.round(col, 'MM'), which fails at run time.
        """
        if not args:
            raise UnconvertibleExpression("ROUND with no args")
        if len(args) == 1:
            return f"F.round({args[0]})"
        precision = self._strip_lit(args[1])
        if self._is_string_literal(args[1]):
            return self._round_date(args[0], self._extract_string_literal(args[1]))
        if not re.fullmatch(r"-?\d+", precision):
            raise UnconvertibleExpression(
                f"ROUND with a non-integer precision {args[1]} (date ROUND?) "
                f"has no Spark equivalent; rewrite by hand"
            )
        return f"F.round({args[0]}, {precision})"

    @staticmethod
    def _round_date(d: str, fmt: str) -> str:
        """ROUND(date, format), per the Transformation Language Reference:
        DD rounds to the next day from 12:00:00; MM to the next month from
        the 16th; YY to the next year from July; HH to the next hour from
        minute 30; MI to the next minute from second 30."""
        f = fmt.strip().upper()
        if f in ("D", "DD", "DDD", "DY", "DAY", "J"):
            return f"F.date_trunc('day', {d} + F.expr('INTERVAL 12 HOURS'))"
        if f in ("MM", "MON", "MONTH", "RM"):
            return (f"F.when(F.dayofmonth({d}) >= 16, F.add_months(F.trunc({d}, 'month'), 1))"
                    f".otherwise(F.trunc({d}, 'month')).cast('timestamp')")
        if f in ("Y", "YY", "YYY", "YYYY", "SYYYY", "YEAR"):
            return (f"F.when(F.month({d}) >= 7, F.add_months(F.trunc({d}, 'year'), 12))"
                    f".otherwise(F.trunc({d}, 'year')).cast('timestamp')")
        if f in ("HH", "HH12", "HH24"):
            return f"F.date_trunc('hour', {d} + F.expr('INTERVAL 30 MINUTES'))"
        if f == "MI":
            return f"F.date_trunc('minute', {d} + F.expr('INTERVAL 30 SECONDS'))"
        raise UnconvertibleExpression(f"ROUND(date, {fmt!r}): unrecognised format")

    def _convert_to_integer(self, name: str, args: list[str]) -> str:
        """TO_INTEGER/TO_BIGINT(value[, flag]).

        Two things decide the emitted code. The width: TO_INTEGER is 32-bit
        and TO_BIGINT 64-bit, so ``cast('int')`` vs ``cast('long')`` -- a value
        past 2^31 that Informatica rejects must not flow through a long.
        And the flag, per the Transformation Language Reference:
        "Truncates the decimal portion when TRUE or a number other than 0.
        Rounds to the nearest integer if FALSE or 0 or is omitted." So a
        bare ``TO_INTEGER('3.7')`` is 4, and ``TO_INTEGER('3.7', TRUE)`` is 3;
        a bare cast always truncated. A non-literal flag (a port) cannot be
        decided at conversion time and is refused rather than guessed.
        """
        if not args:
            raise UnconvertibleExpression(f"{name} with no args")
        spark_type = "int" if name == "TO_INTEGER" else "long"
        truncate = False
        if len(args) > 1:
            flag = self._strip_lit(args[1]).strip().strip("\"'")
            if not re.fullmatch(r"(?i)true|false|-?\d+", flag):
                raise UnconvertibleExpression(
                    f"{name} flag {args[1]} is not a literal; whether it truncates or "
                    f"rounds is decided at run time and cannot be converted"
                )
            truncate = self._is_truthy_literal(args[1]) or (
                re.fullmatch(r"-?\d+", flag) is not None and int(flag) != 0
            )
        value = self._castable(args[0])
        if truncate:
            return f"{value}.cast('double').cast('{spark_type}')"
        return f"F.round({value}.cast('double')).cast('{spark_type}')"

    @staticmethod
    def _castable(arg: str) -> str:
        """``arg`` in a form a ``.cast()`` can hang off.

        A bare numeric literal is a Python number (``3.7.cast`` is an
        AttributeError) and a compound argument (``a + b``) would bind the
        cast to its last operand only, so both are wrapped."""
        s = arg.strip()
        if re.fullmatch(r"-?\d+(?:\.\d+)?", s) or (s[:1] in "'\"" and s[-1:] == s[:1]):
            return f"F.lit({s})"
        if re.fullmatch(r"[\w.]+(?:\([^()]*\))?", s):
            return s
        return f"({s})"

    def _convert_to_decimal(self, args: list[str]) -> str:
        """TO_DECIMAL(value[, scale]) -- the second argument is the SCALE
        (digits after the point), not the precision. It was read as the
        precision, so TO_DECIMAL(AMT, 2) became decimal(2,0): every amount
        of 100 or more overflowed to NULL and the cents were dropped.
        """
        if not args:
            raise UnconvertibleExpression("TO_DECIMAL with no args")
        if len(args) > 1:
            scale = self._strip_lit(args[1])
            if not re.fullmatch(r"\d+", scale):
                raise UnconvertibleExpression(
                    f"TO_DECIMAL scale must be an integer literal, got {args[1]}"
                )
            return f"{args[0]}.cast('decimal(38,{scale})')"
        # No scale given. Informatica returns a value with the SAME SCALE AS
        # THE INPUT; Spark requires a scale at cast time and has no way to
        # express "whatever the input had".
        #
        # This defaulted to 0, which is the one clearly wrong choice: it
        # truncates every fractional value, so an amount column silently
        # lost its cents. The default now preserves the fraction, and the
        # choice is reported as an assumption rather than passed off as a
        # translation -- nothing in the expression tells us the input's real
        # scale, which lives in the port definition.
        return f"{args[0]}.cast('decimal(38,{_DEFAULT_DECIMAL_SCALE})')"

    def _convert_get_date_part(self, args: list[str]) -> str:
        if len(args) < 2:
            raise UnconvertibleExpression(f"GET_DATE_PART requires 2 arguments, got {len(args)}")
        family = self._date_unit_family(args[1])
        fn = {
            "year": "F.year", "quarter": "F.quarter", "month": "F.month",
            "week": "F.weekofyear", "day": "F.dayofmonth", "hour": "F.hour",
            "minute": "F.minute", "second": "F.second",
        }[family]
        return f"{fn}({args[0]})"

    @staticmethod
    def _convert_greatest_least(name: str, args: list[str]) -> str:
        """GREATEST/LEAST with Informatica's NULL rule.

        Informatica propagates NULL: if ANY argument is NULL the result is
        NULL. Spark's greatest/least IGNORE nulls and return the largest or
        smallest of the rest, so a bare mapping returns a value where
        Informatica returns nothing -- a wrong answer, not an error.
        """
        if len(args) < 2:
            raise UnconvertibleExpression(f"{name} needs at least two arguments: {args}")
        fn = "F.greatest" if name == "GREATEST" else "F.least"
        inner = f"{fn}({', '.join(args)})"
        any_null = " | ".join(f"{a}.isNull()" for a in args)
        return f"F.when({any_null}, F.lit(None)).otherwise({inner})"

    def _convert_nvl(self, args: list[str]) -> str:
        if len(args) < 2:
            raise UnconvertibleExpression(f"NVL with insufficient args: {args}")
        return f"F.coalesce({', '.join(args)})"

    def _convert_nvl2(self, args: list[str]) -> str:
        if len(args) < 3:
            raise UnconvertibleExpression(f"NVL2 with insufficient args: {args}")
        return f"F.when({args[0]}.isNotNull(), {args[1]}).otherwise({args[2]})"

    def _convert_to_date(self, args: list[str]) -> str:
        """Convert Informatica TO_DATE to PySpark.

        Uses F.to_timestamp instead of F.to_date for consistency with
        F.current_timestamp() — avoids date/timestamp type mismatches
        in SCD2 patterns where EFF_START_DATE and EFF_END_DATE must
        be the same type.
        """
        col = args[0] if args else ""
        if len(args) > 1:
            fmt = self._extract_string_literal(args[1])
            spark_fmt = _convert_date_format(fmt)
            return f"F.to_timestamp({col}, {spark_fmt!r})"
        return f"F.to_timestamp({col})"

    def _convert_to_char(self, args: list[str]) -> str:
        """Convert Informatica TO_CHAR.

        TO_CHAR(date_col, 'YYYY-MM-DD') → F.date_format(col, 'yyyy-MM-dd')
        TO_CHAR(number_col) → col.cast('string')  (no format = numeric to string)
        """
        col = args[0] if args else ""
        if len(args) > 1:
            fmt = self._extract_string_literal(args[1])
            spark_fmt = _convert_date_format(fmt)
            return f"F.date_format({col}, {spark_fmt!r})"
        # Single arg: numeric or generic to-string conversion
        # _castable, not the bare operand: TO_CHAR(FLOOR(y/10)*10) ended in a
        # numeric literal, so `* 10.cast('string')` parsed as the float `10.`
        # followed by `cast` -- "invalid decimal literal", and the notebook
        # did not compile at all.
        return f"{self._castable(col)}.cast('string')"

    @staticmethod
    def _is_truthy_literal(arg: str) -> bool:
        """Whether an already-converted argument is Informatica's TRUE.

        Several functions take an optional boolean flag -- TO_INTEGER's
        round-vs-truncate, IN's CaseFlag -- and by the time it reaches a
        converter it has been through literal conversion, so it may arrive
        as ``F.lit('TRUE')``, ``F.lit(1)``, or a bare ``TRUE``. Unwrap
        whatever wrapper is present and defer to the shared spelling rules
        rather than re-deriving which spellings mean yes.

        A non-literal flag (a port reference, say) is NOT truthy here: its
        value is unknown at conversion time, and guessing would pick one
        branch of a behaviour the mapping left to runtime.
        """
        s = arg.strip()
        if s.startswith("F.lit(") and s.endswith(")"):
            s = s[len("F.lit("):-1].strip()
        s = s.strip("\"'")
        return is_truthy(s)

    def _convert_date_diff(self, args: list[str]) -> str:
        """Convert Informatica DATE_DIFF(date1, date2, 'unit').

        DATE_DIFF(FILED_DATE, INCIDENT_DATE, 'DD') → F.datediff(FILED_DATE, INCIDENT_DATE)

        No unit argument at all defaults to 'DD' (Informatica's own
        documented default for this function) -- that is NOT the same
        thing as an explicit-but-unrecognized unit, which raises via
        ``_date_unit_family`` instead of guessing.
        """
        if len(args) < 2:
            raise UnconvertibleExpression(f"DATE_DIFF with insufficient args: {args}")
        col1, col2 = args[0], args[1]
        family = self._date_unit_family(args[2]) if len(args) > 2 else "day"

        # Informatica DATE_DIFF returns a FRACTIONAL difference in the
        # requested unit: two and a half months is 2.5, not 2. Every branch
        # here used to end in .cast('int'), so a mapping computing tenure in
        # months, or an age in years, silently lost the remainder and every
        # downstream average was wrong by up to a whole unit. No cast now --
        # the caller gets a double, which is what the source expression
        # meant.
        if family == "day":
            # datediff() is whole days by construction; the fractional part
            # needs the timestamp difference.
            return (
                f"((F.unix_timestamp({col1}) - F.unix_timestamp({col2})) / 86400.0)"
            )
        if family == "week":
            return (
                f"((F.unix_timestamp({col1}) - F.unix_timestamp({col2})) / 604800.0)"
            )
        if family == "month":
            return f"F.months_between({col1}, {col2})"
        if family == "quarter":
            return f"(F.months_between({col1}, {col2}) / 3)"
        if family == "year":
            return f"(F.months_between({col1}, {col2}) / 12)"
        if family == "hour":
            return f"((F.unix_timestamp({col1}) - F.unix_timestamp({col2})) / 3600.0)"
        if family == "minute":
            return f"((F.unix_timestamp({col1}) - F.unix_timestamp({col2})) / 60.0)"
        # family == "second"
        return f"(F.unix_timestamp({col1}) - F.unix_timestamp({col2}))"

    @staticmethod
    def _convert_initcap(arg: str) -> str:
        """Informatica INITCAP: lower-case everything, then capitalise the
        first character and every character that follows a non-alphanumeric
        one ("o'brien-smith" -> "O'Brien-Smith"). Spark's initcap only
        capitalises after whitespace, so it is not used. Built from array
        functions available on Spark 3.5: split into characters, upper-case
        index 0 and any character whose predecessor is not [A-Za-z0-9]."""
        chars = f"F.split(F.lower({arg}), '')"
        return (
            f"F.array_join(F.transform({chars}, lambda _c, _i: "
            f"F.when((_i == 0) | ~F.element_at({chars}, F.greatest(_i, F.lit(1))).rlike('[A-Za-z0-9]'), "
            f"F.upper(_c)).otherwise(_c)), '')"
        )

    def _convert_substr(self, args: list[str]) -> str:
        if len(args) < 2:
            raise UnconvertibleExpression(f"SUBSTR with insufficient args: {args}")
        col, start = args[0], args[1]
        length = args[2] if len(args) > 2 else "2147483647"
        # Informatica treats a start of 0 as 1 and still returns `length`
        # characters; Spark's substring(x, 0, n) returns only n-1. Only 0
        # differs (negative starts count from the end on both sides).
        if start.strip() == "0":
            start = "1"
        elif not re.fullmatch(r"-?\d+", start.strip()):
            start = f"F.when({start} == 0, F.lit(1)).otherwise({start})"
        int_lit = re.compile(r"-?\d+")
        if int_lit.fullmatch(start.strip()) and int_lit.fullmatch(length.strip()):
            return f"F.substring({col}, {start}, {length})"
        # A computed start or length. pyspark 3.5's F.substring(col, pos, len)
        # takes Python ints only -- a Column raises "Column is not iterable"
        # (seen on AIDP Spark 3.5.0, 2026-09-25); only pyspark 4 accepts
        # Columns there. Column.substr(start, length) takes Columns on every
        # version, provided BOTH arguments are Columns.
        as_col = lambda a: f"F.lit({a.strip()})" if int_lit.fullmatch(a.strip()) else a  # noqa: E731
        return f"({col}).substr({as_col(start)}, {as_col(length)})"

    def _convert_instr(self, args: list[str]) -> str:
        """INSTR(string, search[, start[, occurrence[, comparison_type]]]).

        Spark's ``locate(substr, str, pos)`` covers the first three
        arguments -- note the reversed first two. It has no Nth-occurrence
        form and no case-insensitivity flag, so those cannot be translated.

        Previously every argument past the second was dropped silently: a
        mapping asking for the SECOND occurrence got the position of the
        first, with no marker and no error. Refusing is the only honest
        option -- the caller turns this into a REVIEW REQUIRED marker.
        """
        if len(args) < 2:
            raise UnconvertibleExpression(f"INSTR with insufficient args: {args}")

        if len(args) >= 5:
            raise UnconvertibleExpression(
                "INSTR comparison_type (case-insensitive match) has no Spark "
                "equivalent; wrap both arguments in UPPER() and rewrite by hand"
            )
        if not self._is_string_literal(args[1]):
            raise UnconvertibleExpression(
                f"INSTR search argument {args[1]} is not a literal; PySpark's "
                f"locate() takes a plain string and cannot search for a "
                f"column value"
            )
        search = self._extract_string_literal(args[1])
        quoted = repr(search)

        def lit_int(a: str):
            s = self._strip_lit(a).strip()
            return int(s) if re.fullmatch(r"-?\d+", s) else None

        start = lit_int(args[2]) if len(args) >= 3 else 1
        occurrence = lit_int(args[3]) if len(args) >= 4 else 1
        if len(args) >= 4 and (occurrence is None or occurrence < 1):
            raise UnconvertibleExpression(
                f"INSTR occurrence {args[3]} must be a positive integer literal"
            )
        if start is None:
            if occurrence != 1:
                raise UnconvertibleExpression("INSTR with a non-literal start and an occurrence")
            # Spark locate's pos is 1-based, same as Informatica's start.
            return f"F.locate({quoted}, {args[0]}, {args[2]})"
        if start == 0:
            start = 1   # "If start is 0, INSTR searches from the first character"
        if start > 0 and occurrence == 1:
            return (f"F.locate({quoted}, {args[0]}, {start})" if len(args) >= 3
                    else f"F.locate({quoted}, {args[0]})")
        # Nth occurrence and/or a backward search (negative start: counting
        # from the end, searching toward the start). Spark's locate has
        # neither; the position is computed from the pieces between matches,
        # and a backward search is a forward one over the reversed string.
        src, needle, from_pos = args[0], search, start
        if start < 0:
            src, needle, from_pos = f"F.reverse({args[0]})", search[::-1], -start
        pattern = repr(re.escape(needle))
        n = occurrence
        parts = f"F.split({src}.substr(F.lit({from_pos}), F.length({src})), {pattern}, -1)"
        forward = (
            f"F.when(F.size({parts}) > {n}, F.lit({from_pos - 1 + (n - 1) * len(needle) + 1}) + "
            f"F.aggregate(F.slice({parts}, 1, {n}), F.lit(0), lambda _a, _x: _a + F.length(_x)))"
            f".otherwise(F.lit(0))"
        )
        if start > 0:
            return f"F.when({args[0]}.isNull(), F.lit(None)).otherwise({forward})"
        return (f"F.when({args[0]}.isNull(), F.lit(None)).otherwise("
                f"F.when({forward} > 0, F.length({args[0]}) - ({forward}) - {len(needle)} + 2)"
                f".otherwise(F.lit(0)))")

    def _convert_trunc(self, args: list[str]) -> str:
        """Convert Informatica TRUNC(date[, 'unit']) to F.date_trunc.

        ``F.date_trunc``'s format argument accepts exactly the family
        names ``_date_unit_family`` returns ('year', 'quarter', 'month',
        'week', 'day', 'hour', 'minute', 'second'), so no extra mapping
        table is needed here. No unit argument at all defaults to 'day'
        (TRUNC's own documented default) -- an explicit-but-unrecognized
        unit raises instead.
        """
        col = args[0] if args else ""
        if len(args) > 1:
            # TRUNC(numeric, precision): Informatica's numeric TRUNC drops
            # digits toward zero. An integer literal second argument is the
            # numeric form; a date-unit literal is the date form.
            precision = self._strip_lit(args[1])
            if re.fullmatch(r"-?\d+", precision):
                scale = 10 ** int(precision) if int(precision) >= 0 else 1
                if int(precision) < 0:
                    scale = f"(1 / {10 ** (-int(precision))})"
                return (
                    f"F.when({col} >= 0, F.floor({col} * {scale}) / {scale})"
                    f".otherwise(F.ceil({col} * {scale}) / {scale})"
                )
            family = self._date_unit_family(args[1])
            return f"F.date_trunc('{family}', {col})"
        return f"F.date_trunc('day', {col})"

    def _convert_add_to_date(self, args: list[str]) -> str:
        """Convert Informatica ADD_TO_DATE(date, 'unit', amount).

        An unrecognized unit raises ``UnconvertibleExpression`` via
        ``_date_unit_family`` rather than defaulting to day granularity --
        silently adding days where the mapping said e.g. quarters is a
        wrong number in every row with no signal (this
        replaces the previous per-function unit list that only covered
        DD/MM/YY/HH/MI/SS and fell through to day arithmetic for anything
        else, including 'W', 'Q', and 'J').
        """
        if len(args) < 3:
            raise UnconvertibleExpression(f"ADD_TO_DATE with insufficient args: {args}")
        col, unit_raw, amount = args[0], args[1], args[2]
        family = self._date_unit_family(unit_raw)
        # Informatica's Date/Time carries a time of day and ADD_TO_DATE
        # keeps it. F.date_add / F.add_months return a DATE, so every
        # timestamp lost its time component; and the previous hour/minute/
        # second branch emitted an f-string into the notebook that was a
        # SyntaxError whenever the amount was a column rather than a
        # literal. timestamp + interval keeps the type and takes a Column.
        return f"({col} + {self._interval(family, amount)})"

    @staticmethod
    def _interval(family: str, amount: str) -> str:
        """A Spark interval Column of ``amount`` units of ``family``.

        ``F.make_interval`` takes Column arguments, so ``amount`` may be a
        literal or a converted column expression. Weeks/quarters are
        expressed as days/months because make_interval has no separate
        quarter slot and its week slot is fine either way.
        """
        slots = {"year": 0, "month": 1, "week": 2, "day": 3, "hour": 4, "minute": 5, "second": 6}
        # make_interval takes COLUMN arguments. A numeric literal amount
        # reaches here as a bare "1", and `F.make_interval(F.lit(0), (1),
        # ...)` raises NOT_EXPECTED_TYPE at run time -- "Argument `col`
        # should be Column or str, got int" -- for every unit, so
        # ADD_TO_DATE did not work at all. A column amount is already a
        # Column and must not be wrapped again.
        amount_col = _as_column(amount)
        parts = ["F.lit(0)"] * 7
        if family == "quarter":
            # No quarter slot in make_interval; three months is exact.
            parts[1] = f"({amount_col} * F.lit(3))"
        else:
            parts[slots[family]] = amount_col
        return f"F.make_interval({', '.join(parts)})"

    def _convert_reg_match(self, args: list[str]) -> str:
        """REG_MATCH(subject, pattern) is a WHOLE-VALUE match in Informatica
        (like Java's ``matches()``); Spark's ``rlike`` is a substring search.
        The pattern is anchored so ``REG_MATCH(ZIP, '\\d{5}')`` is false for
        '123456', as it is in Informatica. ``rlike`` also takes a plain str,
        not a Column -- a literal pattern is unwrapped.
        """
        if len(args) < 2:
            raise UnconvertibleExpression(f"REG_MATCH with insufficient args: {args}")
        pattern = args[1].strip()
        if not (pattern.startswith("F.lit(") and pattern.endswith(")")):
            raise UnconvertibleExpression(
                "REG_MATCH with a non-literal pattern cannot be anchored here; "
                "Spark's rlike needs a plain str pattern"
            )
        raw = self._extract_string_literal(pattern)
        return f"{args[0]}.rlike({'^(?:' + raw + ')$'!r})"

    def _convert_replace(self, name: str, args: list[str]) -> str:
        """REPLACECHR(CaseFlag, Input, OldCharSet, NewChar) and
        REPLACESTR(CaseFlag, Input, OldString1[, ... OldStringN], NewString).

        Per the Transformation Language Reference:

        - CaseFlag **0 or NULL is case-INsensitive**, any other number is
          case-sensitive. (This was inverted: 0 was translated as
          case-sensitive and a non-zero flag refused.) A non-literal flag is
          decided per row and is refused.
        - REPLACECHR replaces every character of OldCharSet with the FIRST
          character of NewChar; a NULL or empty NewChar removes them.
        - REPLACESTR tries the OldStrings in argument order at each position;
          a NULL or empty NewString removes the match.
        - NULL Input returns NULL; a NULL or empty Old returns Input.

        Old/New must be literals: the replacement is compiled into
        ``translate`` / ``regexp_replace`` arguments at conversion time.
        """
        min_args = 4
        if len(args) < min_args or (name == "REPLACECHR" and len(args) != 4):
            raise UnconvertibleExpression(
                f"{name} requires {'4' if name == 'REPLACECHR' else 'at least 4'} arguments "
                f"(CaseFlag, Input, Old..., New), got {len(args)}: {args}"
            )
        case_flag, input_col, olds, new = args[0], args[1], args[2:-1], args[-1]

        flag = self._strip_lit(case_flag).strip()
        if flag.upper() in ("NONE", "F.LIT(NONE)", "NULL"):
            flag = "0"
        if self._is_truthy_literal(case_flag):
            flag = "1"
        if flag.upper() == "FALSE":
            flag = "0"
        if not re.fullmatch(r"-?\d+", flag):
            raise UnconvertibleExpression(
                f"{name} CaseFlag {case_flag!r} is not a literal; case sensitivity "
                "is decided per row and cannot be converted"
            )
        case_sensitive = int(flag) != 0

        def literal(a: str) -> "Optional[str]":
            """The Python string a literal argument holds, '' for NULL,
            None for a non-literal."""
            s = a.strip()
            if s in ("F.lit(None)", "None"):
                return ""
            inner = self._strip_lit(s)
            if len(inner) >= 2 and inner[0] == inner[-1] and inner[0] in "'\"":
                import ast
                return ast.literal_eval(inner)
            return None

        old_values = [literal(o) for o in olds]
        new_value = literal(new)
        if new_value is None or any(o is None for o in old_values):
            raise UnconvertibleExpression(
                f"{name} with a non-literal old/new value cannot be compiled into a "
                f"replacement: {args}"
            )
        old_values = [o for o in old_values if o]
        if not old_values:
            return input_col

        if name == "REPLACECHR":
            chars = old_values[0]
            if not case_sensitive:
                chars = "".join(dict.fromkeys(
                    c for ch in chars for c in (ch.lower(), ch.upper())
                ))
            else:
                chars = "".join(dict.fromkeys(chars))
            replacement = (new_value[:1] * len(chars)) if new_value else ""
            return f"F.translate({input_col}, {chars!r}, {replacement!r})"

        pattern = "|".join(re.escape(o) for o in old_values)
        if not case_sensitive:
            pattern = "(?i)" + pattern
        # Java regex replacement: a literal $ or \ in NewString must be escaped.
        replacement = new_value.replace("\\", "\\\\").replace("$", "\\$")
        return f"F.regexp_replace({input_col}, {pattern!r}, {replacement!r})"

    def _convert_is_spaces(self, args: list[str]) -> str:
        if not args:
            raise UnconvertibleExpression("IS_SPACES with no args")
        return f"(F.trim({args[0]}) == F.lit(''))"

    # --------------------------------------------------------------- helpers

    @staticmethod
    def _col_ref(name: str) -> str:
        """Convert a plain field name to a PySpark column reference."""
        return f"F.col('{name}')"

    @staticmethod
    def _convert_param(param: str) -> str:
        """Convert ``$$PARAM_NAME`` to a reference the generated notebook
        can resolve.

        The notebook's parameters cell defines ``_param(name)`` (reads
        ``spark.conf`` under the ``migration.`` namespace, falling back to
        the mapping's own DEFAULTVALUE). Emitting a bare
        ``spark.conf.get('NAME')`` here, as before, read a key nobody set
        (the parameters cell used ``migration.name``) and produced a Python
        str, so ``$$THRESHOLD > 100`` was a str-vs-int comparison at run time.
        """
        var_name = param.lstrip("$")
        return f"F.lit(_param({var_name!r}))"

    @staticmethod
    def _string_literal(token: str) -> str:
        """The Python string value of an Informatica STRING token
        (``'it''s'`` -> ``it's``)."""
        return token[1:-1].replace("''", "'")

    @staticmethod
    def _strip_lit(value: str) -> str:
        """Strip F.lit() wrapper for use in .isin() — returns plain Python value.

        F.lit('COMPLETED') → 'COMPLETED'
        F.lit(42) → 42
        """
        s = value.strip()
        if s.startswith("F.lit(") and s.endswith(")"):
            return s[6:-1]
        return s

    @staticmethod
    def _is_string_literal(token: str) -> bool:
        """Whether an already-converted token is a string literal.

        Needed because :meth:`_extract_string_literal` returns its input
        unchanged for anything else, so it cannot distinguish
        ``F.lit('a')`` from ``F.col('A')`` -- and a caller that assumes it
        can will happily search for the text of a column reference.
        """
        s = token.strip()
        if s.startswith("F.lit(") and s.endswith(")"):
            s = s[6:-1].strip()
        return len(s) >= 2 and s[0] in "'\"" and s[-1] == s[0]

    @staticmethod
    def _extract_string_literal(token: str) -> str:
        """Strip F.lit() wrapper and quotes to get raw string value.

        The inner literal is a Python repr (see ``_parse_atom``), so it is
        decoded with ``ast.literal_eval`` rather than by stripping quotes --
        stripping would leave escape sequences in place.
        """
        import ast

        s = token.strip()
        if s.startswith("F.lit(") and s.endswith(")"):
            s = s[6:-1]
        s = s.strip()
        if len(s) >= 2 and s[0] in "'\"" and s[-1] == s[0]:
            try:
                return str(ast.literal_eval(s))
            except (ValueError, SyntaxError):
                return s[1:-1]
        return s.strip("'\"")

    def _date_unit_family(self, unit_raw: str) -> str:
        """Resolve an Informatica date-unit literal to its canonical family.

        Shared by ``_convert_add_to_date``, ``_convert_date_diff`` and
        ``_convert_trunc`` via ``_DATE_UNIT_FAMILY``.
        Raises ``UnconvertibleExpression`` for anything not in that table --
        never returns a guessed default. Adding a day where the mapping
        said a quarter is an ~89-day error on every row with no signal;
        a loud failure here is strictly better.
        """
        unit = self._extract_string_literal(unit_raw).upper()
        family = _DATE_UNIT_FAMILY.get(unit)
        if family is None:
            raise UnconvertibleExpression(
                f"unrecognized date unit {unit!r} -- expected one of "
                f"{sorted(_DATE_UNIT_FAMILY)}"
            )
        return family
