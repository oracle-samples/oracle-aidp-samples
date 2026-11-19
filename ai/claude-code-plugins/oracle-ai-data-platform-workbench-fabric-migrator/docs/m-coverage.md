# M → PySpark coverage

Measured over 39 real Dataflow Gen2 exports (referenced, not vendored — see the spec, §10.1).

| Bucket | Count |
|---|---|
| clean (emitted, no flags) | 1 |
| flagged (emitted, needs a human) | 19 |
| blocked (in scope, no mapping) | 17 |
| out of scope (connector named) | 24 |
| destination helpers (suppressed) | 38 |
| parameter queries (constants) | 4 |
| members not read (no `let`, not a value) | 8 |

**In-scope pipelines: 37. Emitted: 20 (54%).**

Emitted files are `ast.parse`d before they are written. None has been
executed on a cluster; this table measures translation, not correctness.

<!-- everything below this line is hand-written and preserved across regeneration -->

## Known divergences

Eight places where Spark cannot return M's answer, or where this parser
refuses rather than guess at one, measured and left alone rather than
papered over. (This said "Six" over a list of five for several revisions.
The count is the number of bolded entries below; adding one means changing
it.)

**Division by zero and integer overflow.** M follows IEEE and yields
`#infinity`. Spark has the *value* — `cast('Infinity' as double)` is `inf`,
and it is reachable on demand
(`case when x = 0 then double('Infinity') else n / x end` → `inf`) — what it
lacks is division *producing* that value on its own: measured on 4.2.0, with
`spark.sql.ansi.enabled=false`, `x / 0` is `null`, and with Spark 4's default
`true`, division raises `DIVIDE_BY_ZERO`; `bigint` overflow wraps to the
negative extreme under ANSI off. So the outcome depends on the cluster's ANSI
setting as well as on Spark. Wrapping every division in a `CASE` to reach the
value is disproportionate to the benefit, so this stays a divergence rather
than a fix, and refusing every division would block essentially every
Dataflow.

This entry is about *arithmetic* in a translated expression, which is
unguarded. One conversion is not: `Table.TransformColumnTypes`' `Int64.Type`
goes through the generated `_m_int64`, which returns `NULL` outside
`bigint`'s range rather than a wrapped value — measured, `9.3e18` through a
plain rounded cast gave `-9146744073709551616`, and the text
`"9223372036854775808"` gave `-9223372036854775808`, the minimum, from one
past the maximum. M raises on all of these and Spark has no error value, so
`NULL` is the answer that cannot be read as data. See `_m_int64`'s docstring
in `fabric_aidp/translate/m_runtime.py` for the whole measured table.

An earlier version of this note said Spark has no such value at all. That was
wrong, and worth naming: the false premise is plausibly why `_m_text` (the
`Text.From` / `type text` renderer, below) went untested against an infinity
until a whole-branch review found it aborted the job on one — believing the
value did not exist meant nobody asked what the renderer would do if it ever
saw one.

**`NaN` and the infinities render as Spark spells them.** `_m_text`
(`Text.From` / `type text`) writes `'NaN'`, `'Infinity'` and `'-Infinity'`
using Spark's own spelling of each. What M writes for these could not be
verified in this environment — there is no Power BI here to run it against —
so this is a recorded, deliberate divergence rather than a silent guess.

**A decimal column never goes scientific.** `_m_text` renders a
`DecimalType` column as a decimal at every magnitude, because Spark renders
a decimal exactly and never in scientific notation — the digits are already
M's, and the only thing missing is M's dropping of trailing zeros. That is
.NET's `decimal.ToString` behaviour and the honest reading of a decimal
column, but M might instead widen the value to a double and apply `"G"`,
which would go scientific at or above `1e15`. Which of the two M picks could
not be verified here. Measured: `decimal(38,0)` holding `9223372036854775807`
renders `'9223372036854775807'`, not `'9.223372036854776E+18'`.

The alternative — routing a decimal through a fixed intermediate so the
double rules could apply — was tried and reverted. `decimal(38,20)` holds
only eighteen integer digits, so it aborted the job with
`[NUMERIC_VALUE_OUT_OF_RANGE]` on that same value, where a plain
`cast('string')` renders it correctly, and it silently flattened
`decimal(38,25)` holding `1E-25` to `'0'`.

**`-0.0` renders `'0'`.** `_m_text`'s zero branch matches `column == 0`,
which is true for negative zero, so the sign is lost. .NET Framework agrees;
.NET Core 3.0 and later write `"-0"`. Which one M follows could not be
verified here, so this is recorded rather than changed, and
`tests/test_m_runtime.py` asserts the current answer so that changing it
means facing the choice rather than drifting.

**The scientific mantissa comes from the JVM.** `_m_text`'s scientific
branch re-spells Spark's own `cast('string')` output, whose mantissa is
`java.lang.Double.toString`. That only became shortest-round-trip in JDK 19;
before it, the algorithm could emit a longer decimal than necessary.
Everything measured here ran on JDK 21, so the agreement with .NET's `"G"`
is established on JDK 19+ only — on the JDK 8, 11 or 17 that most clusters
run, the match is inherited from a different algorithm and has not been
checked.

**Month and weekday names.** `Date.MonthName`, `Date.DayOfWeekName` and
`MMM`/`MMMM` inside `Date.ToText` render in US English, always: Spark's
`date_format` hardcodes `Locale.US` internally, so the cluster's default
locale has no effect on it. Measured: forcing the JVM default locale to
`fr_FR` and calling `date_format(date'2024-03-15','MMMM')` still returns
`'March'`, and `'EEEE'` still returns `'Friday'`. M renders these in the
document's culture, which for most Fabric Dataflows is also en-US, so the
common case matches; a document authored under a different culture will not.
A `Culture=` argument is refused; its absence is not.

**`Date.EndOfMonth` and `Date.EndOfYear` on a datetime.** M's answer ends
`:59.9999999`. Spark timestamps are microsecond-precision, so the emitted
expression ends `:59.999999` — the last 100 ns cannot be represented.

**A chained comparison (`[a] = [b] = [c]`).** Refused rather than translated.
M's published grammar groups `=`, `<>` and the four ordering operators
right-to-left; this parser's `compare()` is a flat left-associative loop, and
the two groupings disagree for shapes such as `[a] = null = null` (M:
`[a] = (null = null)`, always `[a]`; left-associative: `([a] = null) = null`,
always false). There is no Power BI in this environment, so whether the
shipped implementation actually follows the published right-recursive
grammar could not be confirmed — that is an argument for refusing the
ambiguous shape, not for emitting a guess. Investigated across 86 shapes;
zero occurrences in the 39-export corpus.

## Runtime floor

Three of the emitted functions need more than Spark 3.0, per the installed
PySpark's own `versionadded`: `Text.Trim`'s two-argument form emits
`F.btrim`, and `Text.Replace` emits `F.replace` — both need PySpark ≥ 3.5.0;
`#date` emits `F.make_date`, which needs PySpark ≥ 3.3.0. The floor for this
tool's output is therefore PySpark ≥ 3.5 — `F.make_date`'s lower requirement
does not relax it. Running a notebook that emits `Text.Trim(text, chars)` or
`Text.Replace` on an older cluster fails at run time with `AttributeError`,
not at migration time.

## Testing note

`tests/test_m_runtime.py`'s `HelperExecutionTests` runs every generated
helper — `_m_text` included — against a real Spark session; the `NaN` /
infinity behaviour and the `EndOfMonth`/`EndOfYear` precision divergence
above are exercised by it directly. (The locale and chained-comparison
divergences above were established differently: the locale claim by a
one-off Spark session with the JVM default locale forced, the
chained-comparison one by inspecting the parser rather than running Spark at
all — neither goes through this test class.) `HelperExecutionTests` alone is guarded by
`unittest.skipUnless(spark_available())`. pyspark is deliberately not a
dependency of this project, so on a machine with no pyspark installed, which
is the default here, that class is skipped rather than run: it is not a CI
ratchet, and it protects whoever installs pyspark and runs it deliberately,
not every change automatically.

`HelperWiringTests` is a different matter, and this note used to get it
wrong by lumping the two together. It carries no `skipUnless` and needs no
Spark — it resolves names with `ast`, against the prelude `prelude_for`
assembles — so it runs on every change, everywhere. It checks that each
helper's transitive `REQUIRES` closure actually defines every `_m_`/`_M_`
name that helper's own prelude reads. Measured: deleting one entry from
`REQUIRES` is the sole failure in the full suite with no pyspark installed.
It is a real CI ratchet.
