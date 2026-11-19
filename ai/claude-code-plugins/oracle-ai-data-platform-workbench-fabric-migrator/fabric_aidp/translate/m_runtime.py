"""The helpers a generated PySpark file carries with it.

Spark's casts and date functions do not return M's values, and which
expression does depends on the column's type -- which is not knowable at
migration time, because a lakehouse column carries no declared M type. So the
decision moves into the generated file, where the schema exists.

Held as source text rather than imported: a generated notebook has to run on a
cluster that has never heard of this tool.
"""
from __future__ import annotations

PREAMBLE = '''from pyspark.sql import types as T

# TimestampNTZType arrived in Spark 3.4. Build the tuple defensively so a
# generated file still runs on an older cluster.
_M_TIMESTAMP_TYPES = tuple(t for t in (getattr(T, "TimestampType", None),
                                       getattr(T, "TimestampNTZType", None)) if t)
'''

HELPERS = {
    "_m_is_timestamp": '''def _m_is_timestamp(df, column):
    """True when the column carries a time of day that M would preserve."""
    return isinstance(df.select(column.alias("v")).schema[0].dataType,
                      _M_TIMESTAMP_TYPES)
''',

    # Raw: the body holds r"\.$", and a non-raw outer literal makes that an
    # invalid escape sequence -- a warning today, a SyntaxError later.
    "_m_text": r'''def _m_text(df, column):
    """Power Query `Text.From` / `type text`.

    Spark's own cast writes 3.0 and 1.0E7 where M writes 3 and 10000000, so a
    cast is not a translation. A date has no culture-free rendering, so this
    stops rather than inventing one.

    Four deliberate divergences, none of them verifiable here against Power
    BI, all recorded rather than guessed at:

    * NaN and the infinities come out the way Spark spells them -- 'NaN',
      'Infinity', '-Infinity'.
    * A decimal column is rendered as a decimal, so it never goes
      scientific at any magnitude. That is .NET's decimal.ToString, and the
      honest reading of a decimal column; M might instead widen it to a
      double and apply "G".
    * -0.0 renders '0'. The zero branch wins, which matches .NET Framework;
      .NET Core 3.0 and later write "-0".
    * The scientific mantissa is whatever the JVM's Double.toString
      produces. That only became shortest-round-trip in JDK 19, and most
      clusters run JDK 8, 11 or 17, where the agreement with .NET comes
      from a different algorithm and has not been measured.
    """
    kind = df.select(column.alias("v")).schema[0].dataType
    if isinstance(kind, (T.DateType,) + _M_TIMESTAMP_TYPES):
        raise TypeError("Text.From on a %s is culture-dependent in M; "
                        "no faithful Spark rendering" % kind.simpleString())
    if not isinstance(kind, (T.DoubleType, T.FloatType, T.DecimalType)):
        return column.cast("string")
    text = column.cast("string")
    if isinstance(kind, T.DecimalType):
        # Spark renders a decimal exactly and never in scientific notation, so
        # the digits are already right: neither the decimal(38,20) round-trip
        # below nor the scientific branch applies. Routing a decimal through
        # that intermediate aborted the job on anything wider than 18 integer
        # digits -- [NUMERIC_VALUE_OUT_OF_RANGE] on decimal(38,0) holding
        # 9223372036854775807, which a plain cast renders correctly -- and
        # silently flattened decimal(38,25) 1E-25 to '0'. Measured.
        # Strip zeros ONLY after a decimal point: '1000' must not become '1'.
        return F.when(column.isNull(), F.lit(None)).otherwise(
            F.when(text.contains("."),
                   F.regexp_replace(F.regexp_replace(text, "0+$", ""), r"\.$", "")
                  ).otherwise(text))
    # Plain: re-render through decimal so 1.0E7 becomes 10000000, then drop
    # the zeros the fixed scale padded on. `0+$` cannot eat the integer part
    # -- the '.' is not a zero, so the run stops there.
    # decimal(38,20): 18 integer digits clears the fixed band's 1e15 ceiling,
    # and 20 decimal places carry a double's ~17 significant digits at the
    # band's 1e-4 floor. decimal(38,18) dropped digits there. Doubles only:
    # 1e15 is the branch's own ceiling, so 18 digits is always enough here.
    plain = F.regexp_replace(F.regexp_replace(
        text.cast("decimal(38,20)").cast("string"), "0+$", ""), r"\.$", "")
    # Scientific: Spark writes 1.0E-7, .NET's "G" -- which M uses -- writes
    # 1E-07. Same number, different spelling.
    mantissa = F.regexp_replace(F.split(text, "E").getItem(0), r"\.0$", "")
    raw = F.split(text, "E").getItem(1)
    # lpad TRUNCATES a too-long input -- lpad('300', 2, '0') is '30' -- so pad
    # only what is short, or 1e300 renders as 1E+30.
    digits = F.regexp_replace(raw, r"^[+-]", "")
    padded = F.when(F.length(digits) < 2, F.lpad(digits, 2, "0")).otherwise(digits)
    sci = F.concat(mantissa, F.lit("E"),
                   F.when(raw.startswith("-"), F.lit("-")).otherwise(F.lit("+")),
                   padded)
    magnitude = F.abs(column)
    return (F.when(column.isNull(), F.lit(None))
             # NaN and the infinities sort above every finite value, so they
             # would reach the scientific branch, where their string has no
             # "E" to split and the job dies on an array index.
             .when(F.isnan(column) | (magnitude == F.lit(float("inf"))), text)
             # Zero is special: the fixed-scale decimal renders it "0E-20".
             .when(column == F.lit(0), F.lit("0"))
             # .NET "G" is fixed-point when the exponent is > -5, i.e. from
             # 1e-4 upward. 1e-5 itself is scientific.
             .when((magnitude >= F.lit(1e15)) | (magnitude < F.lit(1e-4)), sci)
             .otherwise(plain))
''',

    "_m_text_columns": '''def _m_text_columns(df, *names):
    """`type text` for several columns at once.

    One call per column would nest: casting N columns would mean N levels of
    _m_text wrapped inside each other on one line, which stops being
    reviewable well before N is large. This hoists a step's whole set of
    text columns into one call instead.

    `F.col` parses its argument as a multipart identifier and
    `withColumn`'s first argument is a plain name, so the same column is
    spelled two ways here. A column literally called `Customer.Name` --
    Power Query's Table.ExpandRecordColumn names its output that by
    default -- resolves as the field `Name` of a column `Customer` and
    raises UNRESOLVED_COLUMN unless it is back-quoted. Measured on Spark
    4.2.0. Quoted here rather than by the caller because the name is
    needed both ways on one line, and quoted only where the grammar asks
    for it -- the same rule the rest of the tool follows, so a hyphen and
    a space stay readable.
    """
    for name in names:
        reference = name
        if "." in name or "`" in name:
            reference = "`" + name.replace("`", "``") + "`"
        df = df.withColumn(name, _m_text(df, F.col(reference)))
    return df
''',

    "_m_number": '''def _m_number(df, column):
    """Power Query `Number.From`. A bare cast is right for a number only.

    Measured on Spark 4.2.0. cast('double') on the timestamp 2024-03-15
    13:45:30 gives 1710510330.0 -- epoch seconds -- where M gives the OLE
    automation serial 45366.573... The same instant, eight orders of
    magnitude apart, and nothing about the number looks wrong. On a date the
    cast is not epoch seconds either: Spark 4 rejects it outright
    (DATATYPE_MISMATCH, "cannot cast DATE to DOUBLE"), and with ANSI off it
    is NULL.

    A string has no culture-free answer: M's text-to-number reads the current
    culture, where "1,5" is 1.5, while Spark reads that as invalid input.
    Same reason Date.ToText refuses a Culture argument.

    Booleans are left to the cast -- M's Number.From(true) is 1 and Spark's
    cast agrees. Measured.
    """
    kind = df.select(column.alias("v")).schema[0].dataType
    if isinstance(kind, (T.DateType,) + _M_TIMESTAMP_TYPES):
        raise TypeError("Number.From on a %s: M returns a date serial, Spark's "
                        "cast returns epoch seconds" % kind.simpleString())
    if isinstance(kind, T.StringType):
        raise TypeError("Number.From on a string is culture-dependent in M; "
                        "no faithful Spark equivalent")
    return column.cast("double")
''',

    "_m_int64": '''def _m_int64(column):
    """Power Query `Int64.Type` in a `Table.TransformColumnTypes`.

    `Int64.Type` there is `Int64.From`, and Spark's `cast("bigint")` is
    neither of the two things it does.

    ROUNDING. `Int64.From` rounds half to even (RoundingMode.ToEven, its
    default); the cast truncates toward zero. MEASURED on Spark 3.5.0 (AIDP)
    and reproduced on 4.2.0, ANSI off:

        doubles [1.6, 2.5, -1.5]        cast [1, 2, -1]   M [2, 2, -2]
        strings ["1.5", "2.5", "-1.5"]  cast [1, 2, -1]   M [2, 2, -2]

    So `F.bround` -- the function `Number.Round` already translates to --
    runs first, over `decimal(38,18)` rather than `double`, so a text column
    is read as the digits it holds and not as the nearest double. The
    migration cannot see the source column's type, so the rounding is
    emitted for every `Int64.Type`; over an already-integral column it is a
    no-op and not a different value.

    RANGE. `decimal(38,18)` holds twenty integer digits and `bigint`
    nineteen, so the intermediate is wider than the result, and a
    decimal->bigint overflow WRAPS where double->bigint saturated and
    text->bigint gave NULL. MEASURED on 4.2.0, ANSI off:

        input                      cast("bigint")        bround(dec)->bigint
        9.3e18                      9223372036854775807  -9146744073709551616
        1e19                        9223372036854775807  -8446744073709551616
        1e25                        9223372036854775807  NULL
        -9.3e18                    -9223372036854775808   9146744073709551616
        -1e19                      -9223372036854775808   8446744073709551616
        "9223372036854775808"      NULL                  -9223372036854775808
        "9300000000000000000"      NULL                  -9146744073709551616
        decimal 9223372036854775807.50
                                    9223372036854775807  -9223372036854775808

    The sign flips both ways; one past the maximum becomes the minimum; and
    on a text or decimal column it replaced a NULL or a saturated maximum
    with a wrong number -- on the very path the decimal intermediate exists
    to serve.

    M answers none of these: `Int64.From` out of range is an error value.
    Spark has no error value, so the choices are a wrong number, nothing, or
    stopping the job. This returns NULL, and the range test is why this is a
    function and not an expression spliced twice into a `withColumn`.

    NULL rather than the saturated maximum because a saturated maximum and a
    wrapped negative are both numbers a reader takes for data, and NULL is
    not; and because NULL is what Spark itself already returned for two of
    the eight rows above, so guarding makes one column's behaviour
    consistent instead of introducing a ninth. A row-level raise would match
    M more closely and is deliberately not done: `Int64.Type` is emitted on
    every integer cast in every migrated query, and a guard that can stop a
    job is a decision about the estate rather than about this function.

    Every value inside the range is untouched, including both bounds --
    measured over double, string, bigint and decimal(38,2) columns, the
    guarded and unguarded expressions agree on every in-range row.
    """
    rounded = F.bround(column.cast("decimal(38,18)"), 0)
    # Inclusive, and compared in decimal: the bound is the last value
    # `bigint` can hold, and a `bigint` comparison cannot represent the
    # out-of-range values this test exists to exclude.
    return F.when(rounded.between(-9223372036854775808,
                                  9223372036854775807), rounded).cast("bigint")
''',

    "_m_add_days": '''def _m_add_days(df, column, days):
    """M keeps the time of day. F.date_add returns a date and drops it."""
    if _m_is_timestamp(df, column):
        return column + F.expr("INTERVAL 1 DAY") * days
    return F.date_add(column, days)
''',

    "_m_add_months": '''def _m_add_months(df, column, months):
    """M keeps the time of day. F.add_months returns a date and drops it."""
    if _m_is_timestamp(df, column):
        return column + F.expr("INTERVAL 1 MONTH") * months
    return F.add_months(column, months)
''',

    "_m_start_of_month": '''def _m_start_of_month(df, column):
    if _m_is_timestamp(df, column):
        return F.date_trunc("month", column)
    return F.trunc(column, "month")
''',

    "_m_end_of_month": '''def _m_end_of_month(df, column):
    """M's datetime answer is the last instant of the month. Spark timestamps
    are microsecond-precision, so this is .999999 where M has .9999999."""
    if _m_is_timestamp(df, column):
        return (F.date_trunc("month", column + F.expr("INTERVAL 1 MONTH"))
                - F.expr("INTERVAL 1 MICROSECOND"))
    return F.last_day(column)
''',

    "_m_start_of_year": '''def _m_start_of_year(df, column):
    if _m_is_timestamp(df, column):
        return F.date_trunc("year", column)
    return F.trunc(column, "year")
''',

    "_m_end_of_year": '''def _m_end_of_year(df, column):
    if _m_is_timestamp(df, column):
        return (F.date_trunc("year", column + F.expr("INTERVAL 1 YEAR"))
                - F.expr("INTERVAL 1 MICROSECOND"))
    return F.last_day(F.add_months(F.trunc(column, "year"), 11))
''',

    "_m_start_of_week": '''def _m_start_of_week(df, column, monday=False):
    """M's default first day of the week is Sunday. Spark's trunc('week') is
    Monday-based, which is the wrong default."""
    if _m_is_timestamp(df, column):
        if monday:
            return F.date_trunc("week", column)
        return (F.date_trunc("week", column + F.expr("INTERVAL 1 DAY"))
                - F.expr("INTERVAL 1 DAY"))
    if monday:
        return F.trunc(column, "week")
    return F.date_sub(F.next_day(column, "SUN"), 7)
''',

    "_m_drop": '''def _m_drop(df, *names):
    """Power Query `Table.RemoveColumns`, which raises on a missing column.

    Measured on Spark 4.2.0: df.drop('nope') returns the frame unchanged and
    raises nothing, where df.select('nope') raises. So a typo, or a column an
    upstream change renamed, turned a hard M error into a table that is
    quietly a different shape. The schema is not knowable when the migration
    runs; it is knowable here.

    Compared exactly rather than case-insensitively: Spark's own resolution
    ignores case by default, so df.drop('ID') would take 'id', while M column
    names are case-sensitive and M raises.
    """
    missing = [name for name in names if name not in df.columns]
    if missing:
        raise ValueError(
            "Table.RemoveColumns: this frame has no column %s; it has %s"
            % (missing, df.columns))
    return df.drop(*names)
''',

    "_m_add_column": '''def _m_add_column(df, name, column):
    """Power Query `Table.AddColumn`, which never replaces a column.

    Measured on the AIDP cluster (Spark 3.5.0, spark.sql.caseSensitive
    false, the default): on a frame with column x = [1, 3],
    df.withColumn("X", F.col("x") * 2) returned columns ['X'], rows
    [(2,), (6,)] -- x and its values were replaced. M column names are
    case-sensitive, so M keeps both x and X; and an AddColumn whose name
    the table already has, exactly, is an error in M, where withColumn
    silently overwrites it. The schema is not knowable when the migration
    runs; it is knowable here.

    Compared casefolded, because that is the comparison Spark's default
    session makes when it decides withColumn is a replacement.

    Spark CAN produce M's shape: with spark.sql.caseSensitive=true the same
    withColumn returns columns ['x', 'X'] (measured in review on pyspark
    4.2.0; not measured on the AIDP cluster). That option is rejected, not
    missing. It is a session setting, so flipping it changes name
    resolution for every other step in this file and for every spark.table()
    read, where a reference that matched only case-insensitively would stop
    resolving; and a generated file runs in a session it shares, which it
    must not reconfigure. So this refuses rather than adds.
    """
    clash = [c for c in df.columns if c.casefold() == name.casefold()]
    if clash:
        raise ValueError(
            "Table.AddColumn %r: this frame already has column %s, which "
            "withColumn would replace (Spark compares names case-"
            "insensitively); M keeps both or raises" % (name, clash))
    return df.withColumn(name, column)
''',

    "_m_rename": '''def _m_rename(df, *pairs):
    """Power Query `Table.RenameColumns`: simultaneous, and it raises.

    SIMULTANEOUS is the whole reason this is not a loop. M applies every
    pair to the *original* column list at once, so {{"a","b"},{"b","a"}} is
    a swap. Measured on Spark 4.2.0, frame [(1, 2)] with columns a, b:

        withColumnRenamed('a','b').withColumnRenamed('b','a')
                                    -> columns ['a', 'a'], Row(a=1, a=2)
        df.withColumnsRenamed({'a': 'b', 'b': 'a'})
                                    -> columns ['a', 'a'], Row(a=1, a=2)
        df.toDF('b', 'a')           -> columns ['b', 'a'], Row(b=1, a=2)
        M's answer                  -> columns ['b', 'a'], values (1, 2)

    Not garbage loosely: two columns with the same name, and neither holds
    the name M gives it. Spark's own bulk API is not the fix -- it produces
    the identical wrong result.

    `toDF`, not a select of aliased columns, because `toDF` renames by
    position and never parses a name. Measured on Spark 4.2.0: on a frame
    whose column is literally called `a.b`, `F.col("a.b")` raises
    UNRESOLVED_COLUMN -- Spark reads the dot as a nested-field access -- so
    a select-with-aliases rendering would break every dotted column name
    while fixing the swap. `toDF` renames it correctly.

    Three refusals, all of them things M refuses too, and all of them
    checked before anything is applied -- half a rename would leave a frame
    neither M nor Spark would produce:

      a source column the frame does not have. Measured on Spark 4.2.0:
        df.withColumnRenamed('nope', 'x') returns the frame unchanged and
        raises nothing -- the same silent divergence _m_drop closes.
      the same source column renamed twice, which has no single answer.
      a result with two columns of one name -- {{"a","b"}} on a frame that
        already has a `b`. The swap above also gives every new name to a
        column that exists, so the check is on the *result*, not on the
        original: checking against the original would refuse the swap this
        function exists to get right.
    """
    columns = df.columns
    missing = [old for old, _ in pairs if old not in columns]
    if missing:
        raise ValueError(
            "Table.RenameColumns: this frame has no column %s; it has %s"
            % (missing, columns))
    sources = [old for old, _ in pairs]
    twice = sorted({old for old in sources if sources.count(old) > 1})
    if twice:
        raise ValueError(
            "Table.RenameColumns renames %s more than once; a column cannot "
            "take two names and which one wins is not read from the export"
            % twice)
    wanted = dict(pairs)
    renamed = [wanted.get(name, name) for name in columns]
    clash = sorted({name for name in renamed if renamed.count(name) > 1})
    if clash:
        raise ValueError(
            "Table.RenameColumns would leave two columns named %s; it renames "
            "%s and this frame has %s" % (clash, list(pairs), columns))
    return df.toDF(*renamed)
''',
}

REQUIRES = {
    "_m_text_columns": ("_m_text",),
    "_m_add_days": ("_m_is_timestamp",),
    "_m_add_months": ("_m_is_timestamp",),
    "_m_start_of_month": ("_m_is_timestamp",),
    "_m_end_of_month": ("_m_is_timestamp",),
    "_m_start_of_year": ("_m_is_timestamp",),
    "_m_end_of_year": ("_m_is_timestamp",),
    "_m_start_of_week": ("_m_is_timestamp",),
}

# The helpers whose bodies read what PREAMBLE binds (`T`,
# `_M_TIMESTAMP_TYPES`). Every other helper reads only `F` and its own
# arguments, and emitting PREAMBLE for those left an import and a tuple in
# the generated file that nothing in it used. Declared, not inferred at
# emit time, like REQUIRES; tests/test_m_runtime.py reads the bodies and
# fails if this set and what they read disagree.
USES_PREAMBLE = frozenset({"_m_is_timestamp", "_m_text", "_m_number"})


def prelude_for(names) -> str:
    """The definitions `names` needs, dependencies first. "" when none."""
    wanted: set = set()
    queue = list(names)
    while queue:
        name = queue.pop()
        if name in wanted:
            continue
        if name not in HELPERS:
            raise KeyError("no such generated helper: %r" % name)
        wanted.add(name)
        queue.extend(REQUIRES.get(name, ()))
    if not wanted:
        return ""
    # HELPERS is in dependency order, so filtering it keeps that order.
    bodies = [HELPERS[name] for name in HELPERS if name in wanted]
    # Checked on the closure: _m_add_days reads neither name, but the
    # _m_is_timestamp it pulls in does. The two newlines stay either way:
    # they are the blank lines between the header's imports and the first
    # definition, which PREAMBLE's absence does not remove.
    head = (PREAMBLE if wanted & USES_PREAMBLE else "") + "\n\n"
    # One trailing newline, not two: each body already ends in "\n", and
    # SESSION (m_to_pyspark.py) opens with its own "\n" before `spark = ...`,
    # so this plus that gives the two blank lines PEP 8 wants after a
    # function definition -- three would land here without this trim.
    return head + "\n\n".join(bodies) + "\n"
