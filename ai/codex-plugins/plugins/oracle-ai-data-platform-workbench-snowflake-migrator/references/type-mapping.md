# Snowflake → Spark/Delta type mapping

Precision and scale always come from `INFORMATION_SCHEMA.COLUMNS`. They are never
inferred from sampled data: `NUMBER` is Snowflake's default numeric type, and
getting its scale wrong does not raise — it silently changes values.

| Snowflake | Spark / Delta | Note |
|---|---|---|
| `NUMBER(p,s)`, `DECIMAL`, `NUMERIC` | `DECIMAL(p,s)` | Highest-consequence mapping. `NUMBER(38,0)` stays `DECIMAL(38,0)` — 38 digits do not fit in a `BIGINT` |
| `NUMBER` with no precision | **blocked** | Refuses to guess |
| `TIMESTAMP_NTZ` | `TIMESTAMP` by default; `TIMESTAMP_NTZ` with `--timestamp-ntz preserve` | The target refuses `TIMESTAMP_NTZ` at CREATE TABLE, so the default carries it as `TIMESTAMP` and records the timezone caveat on every affected column: Spark's `TIMESTAMP` is read through the session timezone, so keep sessions on UTC. With `preserve`, `ddl` halts (exit 3) |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `TIMESTAMP` | `TIMESTAMP` | Timezone semantics differ; recorded as a warning |
| `TEXT`, `VARCHAR(n)`, `CHAR` | `STRING` | Declared length is not enforced by Delta; recorded |
| `BOOLEAN`, `DATE`, `BINARY` | `BOOLEAN`, `DATE`, `BINARY` | Direct |
| `FLOAT`, `DOUBLE`, `REAL` | `DOUBLE` | |
| `TIME(p)` | `STRING` | No Spark TIME type. Read as `TO_VARCHAR(.., 'HH24:MI:SS.FFp')`, so every fractional digit arrives. **Warned**, not silent: ordering, comparison and time arithmetic become string operations |
| `TIMESTAMP_TZ` | `TIMESTAMP` | Also warned that the source's UTC offset is not stored; the instant is exact |
| `VECTOR(FLOAT, n)` / `VECTOR(INT, n)` | `ARRAY<FLOAT>` / `ARRAY<INT>` | Typed (both 32-bit, as in Snowflake). Warned: Delta does not enforce the dimension `n` |
| `MAP(K, V)` | `MAP<STRING, v>` | Typed. A `NUMBER` key is carried as its exact decimal text, and warned |
| `OBJECT(f T, ...)` (structured) | `STRUCT<f: t, ...>` | Typed, nests. An inner type a typed column cannot hold (a timestamp, a `VARIANT`) makes the column the semi-structured case below, with the reason |
| `ARRAY(T)` (structured) | `ARRAY<t>` | Typed; same fallback |
| `VECTOR` / `MAP` whose full type was not read | **blocked** | The element type is not in `INFORMATION_SCHEMA`; re-run `assess` (DESCRIBE TABLE) or discovery (GET_DDL) |
| `VARIANT`, `OBJECT`, `ARRAY` (untyped) | `STRING` (JSON text) by default; **blocked** with `--semi-structured block` | Semi-structured; warned on every affected column. A typed struct/map/array design is a separate decision |
| `GEOGRAPHY`, `GEOMETRY` | **blocked** by default; GeoJSON `STRING` with `--geospatial string`, WKT `STRING` with `--geospatial wkt` | No spatial target type; WKT drops a `GEOMETRY`'s SRID, and says so |
| anything else | **blocked** | Unmapped types are never approximated |

Structured types (`VECTOR`, `MAP`, structured `OBJECT`/`ARRAY`) are typed, so
`--semi-structured` does not apply to them. `INFORMATION_SCHEMA` reports only
their first word; the full type comes from `DESCRIBE TABLE` in `assess` and
`GET_DDL` in the in-AIDP discovery, read only for the tables that hold one.

## How each column is read

The mapping above decides the target type; `ddl` also records, per column, how
the copy reads it from Snowflake and converts it on AIDP (`columns` on every
table statement in `ddl_plan.json`, rule `R04_EXACT_READ` in `DDL_PLAN.md`).
The connector's own table read loses digits, fractions and offsets on these
types and cannot open a table holding `VECTOR`, `MAP` or a structured
`OBJECT`, so each is read as text in one pushdown and converted on AIDP:

| Source | Read in Snowflake | Converted on AIDP |
|---|---|---|
| `NUMBER(p,s)` | `"C"::VARCHAR` | ``CAST(`C` AS DECIMAL(p,s))`` |
| `FLOAT` | `TO_VARCHAR("C", 'TME')` | ``CAST(`C` AS DOUBLE)`` |
| `TIME(p)` | `TO_VARCHAR("C", 'HH24:MI:SS.FFp')` | as read |
| `TIMESTAMP_NTZ` | `TO_VARCHAR("C", 'YYYY-MM-DD HH24:MI:SS.FF9')` | `CAST` to the planned type |
| `TIMESTAMP_TZ` / `_LTZ` | ISO-8601 text with `TZH:TZM` | `CAST(.. AS TIMESTAMP)` |
| `VECTOR` | `"C"::ARRAY::VARCHAR` | ``from_json(`C`, 'array<float>')`` |
| `MAP`, structured `OBJECT` / `ARRAY` | `"C"::VARIANT::VARCHAR` | `from_json` into the planned type |
| untyped `VARIANT` / `OBJECT` / `ARRAY` | `TO_JSON("C"::VARIANT)` | as read (JSON text, keeping `"1"` and `1` apart) |
| `GEOGRAPHY` / `GEOMETRY` | `ST_ASWKT("C")` (`wkt`) or `ST_ASGEOJSON("C")::VARCHAR` (`string`) | as read |
| everything else | `"C"` | as read |

Sub-microsecond digits are still truncated by Spark's microsecond timestamps,
and warned.

## Table properties

- Carried into the CREATE TABLE (`carried_properties`, rule `R12`): a plain
  clustering key as `CLUSTER BY`, retention as the Delta retention
  properties, change tracking or a stream as `delta.enableChangeDataFeed`
  ([maintenance-and-layout.md](maintenance-and-layout.md)).
- Deferred with the reason (`deferred_properties`, rule `R11`): a clustering
  key Delta cannot take, listed per object in the DDL plan under
  *"Maintenance and layout — decisions, NOT applied"*.
- Recorded in `omitted_properties`, never emitted:
  `MAX_DATA_EXTENSION_TIME_IN_DAYS`, which has no equivalent, and the
  Iceberg, dynamic and secure flags.
- Tags, masking policies and row-access policies are not carried;
  `snowmig security` reports them in `SECURITY.md`.

## Constraints

Snowflake `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` are unenforced metadata — only
`NOT NULL` is enforced. They are captured in the inventory and reported, not
emitted as DDL: AIDP refuses `PRIMARY KEY` in `CREATE TABLE`. `NOT NULL` is
carried.

Column `DEFAULT` and `IDENTITY` / `AUTOINCREMENT` are read and warned, never
emitted, for the same reason: AIDP refuses a column default and
`GENERATED ... AS IDENTITY` in `CREATE TABLE`. After cutover the writer
supplies both.

---

# View SQL portability

A view migrates only if every Snowflake-only construct in its body has an exact
rewrite. A construct with one is **translated** and the rule id is recorded; a
construct without one **blocks** the view with the construct named — it is never
rewritten on a guess, because a subtly wrong translation still returns numbers,
just not the right ones. The authoritative list is `translate.RULES`, documented in
[dialect-translation.md](dialect-translation.md); the table below summarises it.

Object references inside a view body are identity in the default Bronze mirror
(`R40_VIEW_REFS_IDENTITY`) and are rewritten to the planned target names under
`--bronze-catalog-prefix` or a schema-style option (`R41_VIEW_REFS_REWRITTEN`).
The rewrite matches whole three-part names on code and identifier segments only
— never inside a string literal (that is data the view returns) or a comment —
a quoted part must match exactly, an unquoted one case-insensitively, and the
plan lists only the references that were actually rewritten.

One- and two-part names are qualified too, where Snowflake resolves them: a
bare `NAME` against the view's own schema, `SCHEMA.NAME` against its own
database. They are read only where a relation stands -- every item of a FROM
list (`from ORDERS o, CUSTOMERS c` has two) and after JOIN -- never the
column in `EXTRACT(YEAR FROM o.D)`, `TRIM(... FROM c.X)`, `SUBSTRING(s FROM
n)` or `a IS DISTINCT FROM c.Y`. A quoted part keeps its case (`"Orders"` is
not `ORDERS`). One that names nothing in the migration is left as written and
reported (`R42_VIEW_REFS_UNRESOLVED` for a bare name,
`R45_VIEW_REFS_UNRESOLVED` for every one- or two-part name).

A view's header column list (`create view V(CUSTOMER, TOTAL) as select
CUST_ID, SUM(AMT) ...`) renames the body's output columns, so it is carried
(`R44_VIEW_COLUMN_LIST`) by aliasing the body: `CREATE VIEW <fqn> AS SELECT *
FROM (<body>) AS named_columns(CUSTOMER, TOTAL)` in the SQL, and the same
query as the catalog API's viewText, which has no column-list field. Never as
a view column list (`CREATE VIEW <fqn> (CUSTOMER, TOTAL) AS ...`): AIDP
creates such a view, and then every read of it fails
`INCOMPATIBLE_VIEW_SCHEMA_CHANGE`. A list that cannot be read blocks the view.

| Construct | What happens |
|---|---|
| `IFF(` | **Translated** to `IF()` (`T01`) |
| `::` cast shorthand | **Translated** to `CAST(x AS <mapped type>)` through the type mapper (`T02`); an unmappable type blocks with the mapper's reason. `::TIME` blocks (the result depends on the operand's type); a `::TIMESTAMP` cast carries the timezone warning as an `R43` caveat |
| `ARRAY_CONSTRUCT(` | **Translated** to `array()` (`T03`) |
| `OBJECT_CONSTRUCT(` | **Translated** to `named_struct()` (`T04`) |
| `DATEADD(unit, n, col)` | **Translated** per unit (`T05`) when `n` is an integer literal or a column; any other form blocks. Exact for `DATE` operands only, and the plan says so |
| `LISTAGG(x, sep)` | **Translated** to `concat_ws(sep, collect_list(x))` (`T06`); `WITHIN GROUP` or a window `OVER (…)` blocks |
| `"quoted identifier"` | **Translated** to `` `backticked` `` (`T07`): Spark reads `"..."` as a string literal. Case is kept |
| `'it''s'` doubled quote in a literal | **Translated** to `'it\'s'` (`T08`): Spark reads `''` as two adjacent literals and concatenates them |
| `// note` line comment | **Translated** to `-- note` (`T21`): Spark has no `//` comment |
| `$$...$$` dollar-quoted string | Blocks: Spark has no dollar quoting (`T09`) |
| `QUALIFY` | Blocks: no Spark equivalent; needs a subquery with `WHERE` on the window result |
| `LATERAL FLATTEN` / `FLATTEN(` | Blocks: maps to `explode` / `LATERAL VIEW`, but the mapping depends on the VARIANT shape |
| `GENERATOR(` / `SEQ4(` / `SEQ8(` | Blocks: Spark uses `range()`, and `SEQ4()` has no gapless equivalent |
| `PIVOT` / `UNPIVOT` | Blocks: Spark syntax differs materially |
| `SYSTEM$…` | Blocks: Snowflake-internal, no target |
| `AT(TIMESTAMP…)` / `BEFORE(` | Blocks: Time Travel has no Delta equivalent in this form |
| `col:field` | Blocks: VARIANT path access; needs an explicit struct design |
| `DECODE(` | Blocks: must become `CASE` |
| `NVL2(` | Blocks: no Spark equivalent; must become `CASE` |
| `DATEDIFF` / `TIMESTAMPDIFF` | Blocks: Snowflake counts unit-boundary crossings, Spark truncates, and `dd`/`yy`/`mm` are not Spark units |
| `TIMESTAMPADD` / `TIMEADD` | Blocks: `DATEADD` aliases whose operands are `TIMESTAMP`/`TIME` by construction, where the `DATEADD` rewrite is not exact |

`MAX_BY` / `MIN_BY` are **not** blocked — Spark supports them.

Also blocked, regardless of body: **secure views** (row-visibility rules have no
equivalent). A **materialized view** is not created as a view either: it
migrates as a table snapshot, and its query is translated only for the
generated refresh job (`refresh generated` / `refresh NOT generated: <why>` in
`PLANNED_OBJECTS.md`).

## Object mapping

| Snowflake | AIDP |
|---|---|
| Database | In the S1–S12 runbook: an INTERNAL target catalog for the migrated tables, plus an EXTERNAL catalog (source type SNOWFLAKE) registered over the live source. The stand-alone catalog commands register the EXTERNAL catalog by default and create a Standard catalog on explicit request |
| Schema | Schema |
| Table | Table (managed Delta) |
| View | View |
| Warehouse | Spark compute cluster — see the compute proposal |


## Integer columns become `DECIMAL(38,0)`

Every Snowflake integer alias — `INT`, `INTEGER`, `BIGINT`, `SMALLINT`,
`TINYINT`, `BYTEINT` — *is* `NUMBER(38,0)`, so `DECIMAL(38,0)` is the faithful
mapping and `BIGINT` would silently narrow the declared range. Fidelity was
chosen over familiarity.

Expect it to surprise people: Spark schemas normally show `BIGINT` for an ID
column, and downstream casts and joins will see `DECIMAL`. It is reported as an
informational **note**, not a warning, precisely so it does not inflate every
table's risk level — a warning on every integer column would drown the warnings
that matter.

## Semi-structured, geospatial and timestamp modes

| Flag | Config key | Default | The other mode |
|---|---|---|---|
| `--semi-structured` | `mapping.semi_structured` | `string`: carry the JSON as text, with a warning on every affected column | `block`: the table is blocked until a typed design exists |
| `--geospatial` | `mapping.geospatial` | `block`: the table is blocked | `string`: carry the value as GeoJSON text; `wkt`: as WKT text (`ST_ASWKT`). Either way no spatial type, index or predicate support |
| `--timestamp-ntz` | `mapping.timestamp_ntz` | `timestamp`: carry `TIMESTAMP_NTZ` as `TIMESTAMP`, with the timezone caveat recorded | `preserve`: keep `TIMESTAMP_NTZ`; `ddl` halts on it (exit 3) |

`--mapping-defaults off` (or `mapping.enabled: false` in the config) restores
the strict modes for a run — `VARIANT` blocks and `TIMESTAMP_NTZ` is preserved
— and an explicit flag always wins.

`--semi-structured` and `--geospatial` are **separate flags on purpose**.
Carrying JSON as text is not the same decision as carrying a geography as
text, and one switch for both would make a customer accept a call they were
not asked about.

Text defers the design rather than completing it. With JSON carried as text,
nothing on the target can address a field inside the value (`from_json` /
`get_json_object` read it), and a view using Snowflake path syntax is blocked
until a struct/map design is agreed. Tell the consumers of those tables.
