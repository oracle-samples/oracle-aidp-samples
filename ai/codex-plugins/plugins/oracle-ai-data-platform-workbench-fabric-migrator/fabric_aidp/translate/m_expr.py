"""Translate an M scalar expression into PySpark Column source text.

`Table.AddColumn(prev, "c", each <expr>)` is 367 of the 956 steps in the
reference corpus. Step rules without expression coverage buy nothing, so this
module is where most of the real translation happens.

It is a small recursive-descent parser, not a regex pass. M's `&` is string
concatenation and its operators have their own precedence; pattern-matching
over the text produces silently wrong answers, which is the specific failure
mode this whole project exists to avoid.

Anything outside the grammar below raises `Untranslatable`, naming the
construct. The caller blocks that step and keeps the rest of the query.
"""
from __future__ import annotations

import re

from fabric_aidp.naming import spark_column_ref
from fabric_aidp.translate.m_parser import unquote_identifier


class Untranslatable(Exception):
    """This expression has no proven PySpark equivalent."""


# `column` is a field access, `[Name]`. M spells a name that is not a plain
# identifier -- anything holding a space, a hyphen, a `#` -- with its
# quoted-identifier syntax, `[#"order-id"]`, and that form is matched as its
# own alternative here. It has to be: a quoted identifier may itself contain
# `]` (`[#"data]1"]`), which the general `[^\]]` branch would end the token on.
_TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<number>\d+\.\d+|\.\d+|\d+)
  | (?P<string>"(?:[^"]|"")*")
  | (?P<column>\[\s*(?:\#"(?:[^"]|"")*"|[^\]]*?)\s*\])
  | (?P<name>\#?[A-Za-z_][A-Za-z0-9_.]*)
  | (?P<op><>|<=|>=|=|<|>|&|\+|-|\*|/|\(|\)|,|\{|\})
""", re.X)

_KEYWORDS = {"if", "then", "else", "and", "or", "not", "true", "false", "null",
             "each", "let", "in", "try", "otherwise", "meta"}

# M function -> a formatter taking already-translated argument strings.
_FUNCTIONS = {
    "Date.Year":        lambda a: "F.year(%s)" % a[0],
    "Date.Month":       lambda a: "F.month(%s)" % a[0],
    "Date.Day":         lambda a: "F.dayofmonth(%s)" % a[0],
    "Date.QuarterOfYear": lambda a: "F.quarter(%s)" % a[0],
    "Date.DayOfYear":   lambda a: "F.dayofyear(%s)" % a[0],
    "Date.MonthName":   lambda a: "F.date_format(%s, 'MMMM')" % a[0],
    "Date.DayOfWeekName": lambda a: "F.date_format(%s, 'EEEE')" % a[0],
    "Text.Upper":       lambda a: "F.upper(%s)" % a[0],
    "Text.Lower":       lambda a: "F.lower(%s)" % a[0],
    "Text.Proper":      lambda a: "F.initcap(%s)" % a[0],
    # Wrapped in a lambda (not a bare reference) because _FUNCTIONS is built
    # before _text_trim is defined further down the file; the lambda defers
    # the name lookup to call time, same as the List.* entries below.
    "Text.Trim":        lambda a: _text_trim(a),
    "Text.Length":      lambda a: "F.length(%s)" % a[0],
    "Text.Start":       lambda a: "F.substring(%s, 1, %s)" % (a[0], _int_literal(a[1], "Text.Start")),
    "Text.Contains":    lambda a: "%s.contains(%s)" % (a[0], a[1]),
    "Text.Repeat":      lambda a: "F.repeat(%s, %s)" % (a[0], _int_literal(a[1], "Text.Repeat")),
    "Text.Replace":     lambda a: "F.replace(%s, %s, %s)" % (a[0], a[1], a[2]),
    "Number.Round":     lambda a: "F.bround(%s, %s)" % (
        a[0], _int_literal(a[1], "Number.Round") if len(a) > 1 else "0"),
    "Number.RoundUp":   lambda a: _round_away("F.ceil", a, "Number.RoundUp"),
    "Number.RoundDown": lambda a: _round_away("F.floor", a, "Number.RoundDown"),
    "Number.Abs":       lambda a: "F.abs(%s)" % a[0],
    "List.Sum":         lambda a: _no_list("List.Sum"),
    "List.Max":         lambda a: _no_list("List.Max"),
    "List.Min":         lambda a: _no_list("List.Min"),
    "Date.ToText":      lambda a: "F.date_format(%s, %s)" % (a[0], _format_literal(a[1])),
    # M numbers weekdays from the given start day; Spark's dayofweek is 1=Sunday.
    "Date.DayOfWeek":   lambda a: _day_of_week(a),
    "Text.End":         lambda a: "F.substring(%s, -%s, %s)" % (
        a[0], _int_literal(a[1], "Text.End"), _int_literal(a[1], "Text.End")),
    "#date":            lambda a: "F.make_date(%s, %s, %s)" % (a[0], a[1], a[2]),
}

# Functions whose correct expression depends on the argument's *type*, which
# only the generated file can see. Each returns (source, helper name).
# Lambdas, not bare references, for the same reason _FUNCTIONS uses them: this
# is a module-level literal, evaluated at import.
_FRAME_FUNCTIONS = {
    "Text.From": lambda a, f: ("_m_text(%s, %s)" % (f, a[0]), "_m_text"),
    # `.cast('double')` was in _FUNCTIONS, whose entries do not see the
    # column's type. It is right for a number and wrong otherwise: on a
    # timestamp Spark's cast gives epoch seconds (1710510330.0) where M gives
    # a date serial (~45366.57) -- a silently wrong answer eight orders of
    # magnitude out. Measured on Spark 4.2.0.
    "Number.From": lambda a, f: ("_m_number(%s, %s)" % (f, a[0]), "_m_number"),
    # F.date_add returns a date whatever it is given, so a datetime loses its
    # time of day. Which expression preserves it depends on the column's type.
    "Date.AddDays": lambda a, f: (
        "_m_add_days(%s, %s, %s)" % (f, a[0], a[1]), "_m_add_days"),
    "Date.AddMonths": lambda a, f: (
        "_m_add_months(%s, %s, %s)" % (f, a[0], a[1]), "_m_add_months"),
    "Date.AddYears": lambda a, f: (
        "_m_add_months(%s, %s, (%s) * 12)" % (f, a[0], a[1]), "_m_add_months"),
    "Date.StartOfMonth": lambda a, f: (
        "_m_start_of_month(%s, %s)" % (f, a[0]), "_m_start_of_month"),
    "Date.EndOfMonth": lambda a, f: (
        "_m_end_of_month(%s, %s)" % (f, a[0]), "_m_end_of_month"),
    "Date.StartOfYear": lambda a, f: (
        "_m_start_of_year(%s, %s)" % (f, a[0]), "_m_start_of_year"),
    "Date.EndOfYear": lambda a, f: (
        "_m_end_of_year(%s, %s)" % (f, a[0]), "_m_end_of_year"),
    "Date.StartOfWeek": lambda a, f: (
        "_m_start_of_week(%s, %s%s)" % (f, a[0], _week_start(a)),
        "_m_start_of_week"),
}

# M's Day enumeration is a plain integer constant; Monday = 1.
_CONSTANTS = {
    "Day.Sunday": "F.lit(0)", "Day.Monday": "F.lit(1)", "Day.Tuesday": "F.lit(2)",
    "Day.Wednesday": "F.lit(3)", "Day.Thursday": "F.lit(4)",
    "Day.Friday": "F.lit(5)", "Day.Saturday": "F.lit(6)",
}
# name -> (minimum, maximum). Default (1, 1). Checking only the minimum let
# Text.Trim(s, "0") drop the trim characters and Text.Upper(s, "en-US") drop
# the culture, both silently.
_ARITY = {
    "Date.ToText": (2, 2), "#date": (3, 3), "Text.End": (2, 2),
    "Date.AddDays": (2, 2), "Date.AddMonths": (2, 2), "Date.AddYears": (2, 2),
    "Text.Start": (2, 2), "Text.Contains": (2, 2), "Text.Repeat": (2, 2),
    "Text.Replace": (3, 3), "Text.Trim": (1, 2), "Number.Round": (1, 2),
    "Number.RoundUp": (1, 2), "Number.RoundDown": (1, 2),
    "Date.StartOfWeek": (1, 2), "Date.DayOfWeek": (1, 2),
}

_INT_LITERAL = re.compile(r"^F\.lit\((-?\d+)\)$")


_STRING_LITERAL = re.compile(r"^F\.lit\(('.*'|\".*\")\)$", re.S)


_FORMAT_RUN = re.compile(r"([A-Za-z])\1*|[/\-.:,_ ]")
# Runs .NET and Spark read identically. Everything else either raises at run
# time (tt, fff) or means something different (ddd, dddd, F, g, K, z).
_SAFE_RUNS = frozenset({"yyyy", "yy", "MMMM", "MMM", "MM", "M", "dd", "d",
                        "HH", "H", "hh", "h", "mm", "m", "ss", "s"})


def _format_literal(arg):
    """Date.ToText's format must be a literal Spark reads the way .NET does.

    A one-character format is a .NET *standard* format, not a pattern: "d"
    means the short-date pattern there and day-of-month here, so M writes
    "3/15/2024" and Spark writes "15". Measured.

    The record form `[Format=..., Culture=...]` tokenises as a column
    reference, so it fails the literal check and refuses -- which is right:
    a culture is not reproducible here.
    """
    match = _STRING_LITERAL.match(arg)
    if match is None:
        raise Untranslatable("Date.ToText needs a literal format string")
    literal = match.group(1)
    pattern = literal[1:-1]
    if len(pattern) < 2:
        raise Untranslatable("Date.ToText %r is a .NET standard format, not a "
                             "pattern Spark reproduces" % pattern)
    position = 0
    while position < len(pattern):
        run = _FORMAT_RUN.match(pattern, position)
        if run is None:
            raise Untranslatable("Date.ToText: %r in %r has no Spark equivalent"
                                 % (pattern[position], pattern))
        if run.group()[0].isalpha() and run.group() not in _SAFE_RUNS:
            raise Untranslatable("Date.ToText: %r in %r means something else "
                                 "in Spark" % (run.group(), pattern))
        position = run.end()
    return literal


def _week_start(args):
    """M's default firstDayOfWeek is Day.Sunday -- and this module's own
    Date.DayOfWeek already treats the one-argument form as Sunday, so the
    Monday default here contradicted its neighbour.

    Returns the keyword argument Task 7's helper call needs: "" for Sunday.
    """
    if len(args) == 1 or args[1] == "F.lit(0)":
        return ""
    if args[1] == "F.lit(1)":
        return ", monday=True"
    raise Untranslatable("Date.StartOfWeek supports Day.Sunday and Day.Monday only")


def _day_of_week(args):
    """Date.DayOfWeek(d) is Sunday=0; with Day.Monday it is Monday=0."""
    if len(args) == 1 or args[1] == "F.lit(0)":
        return "(F.dayofweek(%s) - 1)" % args[0]
    if args[1] == "F.lit(1)":
        return "((F.dayofweek(%s) + 5) %% 7)" % args[0]
    raise Untranslatable("Date.DayOfWeek supports Day.Sunday and Day.Monday only")


def _int_literal(arg, function):
    """PySpark's substring/repeat/round want Python ints, not Columns."""
    match = _INT_LITERAL.match(arg)
    if match is None:
        raise Untranslatable("%s needs a literal integer, got %s" % (function, arg))
    return match.group(1)


def _round_away(function, args, name):
    """M's RoundUp/RoundDown take a digit count; F.ceil/F.floor do not.

    The obvious `x * 100` is wrong: 0.07 * 100 is 7.000000000000001 in binary
    floating point, so Number.RoundUp(0.07, 2) would answer 0.08. Scaling
    through decimal answers 0.07. Both measured.
    """
    if len(args) == 1:
        return "%s(%s)" % (function, args[0])
    digits = int(_int_literal(args[1], name))
    if digits == 0:
        return "%s(%s)" % (function, args[0])
    if digits < 0:
        raise Untranslatable("%s with %d digits" % (name, digits))
    scale = 10 ** digits
    return ("(%s((%s).cast('decimal(38,18)') * F.lit(%d)) / F.lit(%d))"
            ".cast('double')" % (function, args[0], scale, scale))


def _text_trim(args):
    """M removes all whitespace; F.trim removes ASCII spaces only, so a tab
    survives it. The two-argument form trims a given set of characters."""
    if len(args) == 1:
        return r"F.regexp_replace(%s, r'^\s+|\s+$', '')" % args[0]
    return "F.btrim(%s, %s)" % (args[0], args[1])


def _no_list(name):
    """`List.Sum(x)` used to emit `x` -- the argument, unaggregated.

    This parser has no list literal, so the argument is always a single
    column and there is nothing to sum. Refusing is the honest answer.
    """
    raise Untranslatable("%s needs an M list, which this parser does not read"
                         % name)


_BINARY = {"+": "+", "-": "-", "*": "*", "/": "/",
           "=": "==", "<>": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">="}


def _tokenize(text):
    tokens, pos = [], 0
    while pos < len(text):
        match = _TOKEN.match(text, pos)
        if match is None:
            raise Untranslatable("unexpected character %r" % text[pos])
        pos = match.end()
        kind = match.lastgroup
        if kind != "ws":
            tokens.append((kind, match.group()))
    return tokens


class _Parser:
    def __init__(self, tokens, scope=None, frame=None, helpers=None):
        self.tokens, self.i = tokens, 0
        self.scope = scope or {}
        self.frame = frame
        self.helpers = helpers if helpers is not None else set()

    def peek(self):
        return self.tokens[self.i] if self.i < len(self.tokens) else (None, None)

    def take(self):
        kind, value = self.peek()
        self.i += 1
        return kind, value

    def accept(self, value):
        if self.peek()[1] == value:
            self.i += 1
            return True
        return False

    def expect(self, value):
        if not self.accept(value):
            raise Untranslatable("expected %r near %r" % (value, self.peek()[1]))

    # --- grammar, loosest binding first -------------------------------------
    def expr(self):
        if self.accept("if"):
            cond = self.expr()
            self.expect("then")
            then = self.expr()
            self.expect("else")
            other = self.expr()
            return "F.when(%s, %s).otherwise(%s)" % (cond, then, other)
        return self.or_()

    def or_(self):
        left = self.and_()
        while self.accept("or"):
            left = "((%s) | (%s))" % (left, self.and_())
        return left

    def and_(self):
        left = self.compare()
        while self.accept("and"):
            left = "((%s) & (%s))" % (left, self.compare())
        return left

    def compare(self):
        left = self.concat()
        seen = 0
        while self.peek()[1] in ("=", "<>", "<", ">", "<=", ">="):
            seen += 1
            if seen > 1:
                # M's grammar is right-recursive at both comparison levels, so
                # `[a] = null = null` means `[a] = (null = null)` -- `[a] = true`.
                # This loop is left-associative and emitted
                # `F.col('a').isNull().isNull()` for it: always false, under a
                # PASS. No Power BI here to confirm the implementation matches
                # the published grammar, and a guess either way is a silently
                # wrong answer, so refuse and let the caller block the step.
                raise Untranslatable(
                    "chained comparison; M groups these right to left and this "
                    "parser does not, so the result would differ")
            op = self.take()[1]
            right = self.concat()
            if op in ("=", "<>"):
                left = _equality(left, right, negate=op == "<>")
            else:
                left = "((%s) %s (%s))" % (left, _BINARY[op], right)
        return left

    def concat(self):
        left = self.add()
        parts = [left]
        while self.accept("&"):
            parts.append(self.add())
        if len(parts) == 1:
            return left
        # M's `&` is string concatenation. Spark's concat propagates null the
        # way M does; concat_ws does not -- it drops nulls and yields a short
        # string instead of null. Same class of silent wrong answer as the
        # T-SQL `+` trap, so: concat.
        return "F.concat(%s)" % ", ".join(parts)

    def add(self):
        left = self.mul()
        while self.peek()[1] in ("+", "-"):
            op = self.take()[1]
            left = "((%s) %s (%s))" % (left, op, self.mul())
        return left

    def mul(self):
        left = self.unary()
        while self.peek()[1] in ("*", "/"):
            op = self.take()[1]
            left = "((%s) %s (%s))" % (left, op, self.unary())
        return left

    def unary(self):
        if self.accept("-"):
            return "(-(%s))" % self.unary()
        if self.accept("not"):
            return "(~(%s))" % self.unary()
        return self.primary()

    def primary(self):
        kind, value = self.take()
        if kind == "number":
            return "F.lit(%s)" % value
        if kind == "string":
            return "F.lit(%s)" % _py_string(value)
        if kind == "column":
            # `spark_column_ref`, not the bare name: `F.col` parses its
            # argument as a multipart identifier, so a column literally
            # called `Customer.Name` -- which is what Power Query's
            # Table.ExpandRecordColumn names its output -- resolves as the
            # field `Name` of a column `Customer` and raises
            # UNRESOLVED_COLUMN. Measured on Spark 4.2.0.
            return "F.col(%r)" % spark_column_ref(_field_name(value))
        if value == "(":
            inner = self.expr()
            self.expect(")")
            return "(%s)" % inner
        if kind == "name":
            if value == "true":
                return "F.lit(True)"
            if value == "false":
                return "F.lit(False)"
            if value == "null":
                return "F.lit(None)"
            if value in _KEYWORDS:
                raise Untranslatable("unsupported M construct %r" % value)
            if self.peek()[1] == "(":
                return self.call(value)
            if value in _CONSTANTS:
                return _CONSTANTS[value]
            bound = self.scope.get(value)
            if bound is not None:
                return bound
            raise Untranslatable("unbound identifier %r" % value)
        raise Untranslatable("unsupported token %r" % value)

    def call(self, name):
        self.expect("(")
        args = []
        if not self.accept(")"):
            args.append(self.expr())
            while self.accept(","):
                args.append(self.expr())
            self.expect(")")
        handler = _FUNCTIONS.get(name) or _FRAME_FUNCTIONS.get(name)
        if handler is None:
            raise Untranslatable("unsupported M function %r" % name)
        low, high = _ARITY.get(name, (1, 1))
        if not low <= len(args) <= high:
            raise Untranslatable("%s takes %s argument(s), got %d" % (
                name, low if low == high else "%d to %d" % (low, high), len(args)))
        if name in _FRAME_FUNCTIONS:
            if self.frame is None:
                raise Untranslatable(
                    "%s depends on the column's type, so it needs the frame it "
                    "reads; this expression is not evaluated against one" % name)
            source, helper = _FRAME_FUNCTIONS[name](args, self.frame)
            self.helpers.add(helper)
            return source
        return handler(args)


def _wrap(text):
    return text if text.startswith("F.col(") else "(%s)" % text


def _equality(left, right, *, negate):
    """M `=` / `<>` -> a null-safe Spark comparison.

    M equality is two-valued: null = null is true and null <> "x" is true.
    Spark's == and != return NULL for both, which filter() drops and F.when()
    sends to otherwise() -- so `each [Status] <> "Closed"` silently lost every
    row whose Status is null. Only the ordering operators propagate null in M.
    """
    null = "F.lit(None)"
    if left == null and right == null:
        return "F.lit(%s)" % (not negate)
    if null in (left, right):
        column = left if right == null else right
        return "%s.%s()" % (_wrap(column), "isNotNull" if negate else "isNull")
    same = "%s.eqNullSafe(%s)" % (_wrap(left), right)
    return "(~%s)" % same if negate else same


_ESCAPE = re.compile(r"#\(([^)]*)\)")
_NAMED_ESCAPES = {"tab": "\t", "lf": "\n", "cr": "\r", "#": "#"}
_CODE_POINT = re.compile(r"[0-9A-Fa-f]{4}|[0-9A-Fa-f]{8}")


def decode_escapes(text) -> str:
    """M writes a tab as `#(tab)`.

    Nothing decoded these, so `Delimiter = "#(tab)"` -- the ordinary way to
    say tab-separated -- reached Spark as six literal characters. An
    unrecognised escape refuses rather than passing through, because `#(` is
    always an escape in M and a passed-through one is a wrong string.
    """
    def replace(match):
        out = []
        for code in match.group(1).split(","):
            code = code.strip()
            if code in _NAMED_ESCAPES:
                out.append(_NAMED_ESCAPES[code])
            elif _CODE_POINT.fullmatch(code):
                value = int(code, 16)
                if value > 0x10FFFF:
                    raise Untranslatable(
                        "M escape #(%s) is not a Unicode code point" % code)
                out.append(chr(value))
            else:
                raise Untranslatable("unsupported M escape #(%s)" % code)
        return "".join(out)
    decoded = _ESCAPE.sub(replace, text)
    # An unterminated `#(` is not a literal -- `#(` always starts an escape in
    # M. Check the *source*: the decoded text can legitimately contain "#(",
    # via `#(#)(`.
    opened = {m.start() for m in _ESCAPE.finditer(text)}
    if any(text.startswith("#(", i) and i not in opened
           for i in range(len(text) - 1)):
        raise Untranslatable("unterminated M escape in %r" % text)
    return decoded


def _py_string(literal):
    return repr(decode_escapes(literal[1:-1].replace('""', '"')))


def _field_name(token) -> str:
    '''`[...]` token text -> the column name it accesses.

    This used to be `token[1:-1].strip()`, which carried M's *syntax* into
    the name: `[#"order-id"]` became `F.col('#"order-id"')` -- a reference to
    a column that does not exist, in a step graded a rewrite. `#"..."` is not
    decoration. It is mandatory in M for any name that is not a plain
    identifier, which is to say every column holding a space or a hyphen, so
    the names most likely to be wrong were the only ones that could be.

    Measured before the fix, on this worktree:

        [plain]         -> F.col('plain')            correct
        [#"order-id"]   -> F.col('#"order-id"')      wrong
        [#"a""b"]       -> F.col('#"a""b"')          wrong

    Three forms, one rule each:

      [Order ID]      a generalized identifier. M reads it verbatim -- no
                      escape processing -- so it is taken verbatim, with
                      surrounding whitespace stripped the way M's grammar
                      allows it between the brackets and the name.
      [#"order-id"]   a quoted identifier. The `#"` and `"` are syntax;
                      an inner `""` is one `"`. `unquote_identifier` is the
                      one place that is written down, shared with step names
                      and record keys so they cannot drift apart.
      [#"a#(tab)b"]   M's text escapes are legal inside a quoted identifier,
                      so they are decoded with the same function a string
                      literal uses. An escape with no mapping raises rather
                      than passing through, because a passed-through escape
                      is a wrong column name and this whole module exists to
                      refuse those rather than emit them.
    '''
    inner = token[1:-1].strip()
    if inner.startswith('#"') and inner.endswith('"') and len(inner) >= 3:
        name = decode_escapes(unquote_identifier(inner))
    else:
        name = inner
    if not name:
        # `[]` is M's empty record, not a field access, and `F.col('')` is a
        # reference to a column no frame can have. Named rather than emitted.
        raise Untranslatable("%s is not a field access" % token)
    return name


def translate_expression(text, scope=None, *, frame=None, helpers=None) -> str:
    """M scalar expression -> PySpark Column source. Raises Untranslatable.

    `scope` maps M identifiers the caller has *proven* hold scalars -- a
    parameter query, or an earlier scalar binding -- to the Python source that
    reproduces them. An identifier not in scope is untranslatable rather than
    guessed: emitting a bare name that happens to be a DataFrame would compile
    and then be wrong at runtime.

    `frame` is the bare Python identifier of the DataFrame the expression
    reads -- the one a `_FRAME_FUNCTIONS` entry needs to inspect the actual
    column type at run time (a lakehouse column carries no declared M type
    here). None when the expression is not evaluated against a DataFrame, in
    which case a function that requires a frame refuses.

    `helpers` is a set the caller owns; this parser only adds to it, one name
    per `_FRAME_FUNCTIONS` call used, so the caller knows which generated
    helper definitions the emitted file needs.
    """
    source = str(text or "").strip()
    if source.startswith("each "):
        source = source[5:].strip()
    elif source == "each":
        raise Untranslatable("empty each-expression")
    if not source:
        raise Untranslatable("empty expression")
    parser = _Parser(_tokenize(source), scope, frame, helpers)
    result = parser.expr()
    if parser.i != len(parser.tokens):
        raise Untranslatable("trailing input near %r" % parser.peek()[1])
    return result
