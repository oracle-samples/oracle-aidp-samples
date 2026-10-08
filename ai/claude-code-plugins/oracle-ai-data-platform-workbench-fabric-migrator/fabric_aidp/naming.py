"""The one place a Fabric object's AIDP name is built.

Fabric nests a table under an *item* (a Lakehouse or a Warehouse) and
sometimes a schema; AIDP nests it under a catalog and a schema. Both are two
levels deep, so the only real question was which maps to which, and the answer
is recorded in docs/superpowers/specs/2026-09-28-table-naming-design.md:

    <catalog> . <item>[_<schema, unless it is Fabric's default>] . <table>

This module exists because the answer used to live in four places that
disagreed. A notebook said `default.SalesLake.claim`, the T-SQL rules said
`default.dbo.claim` -- a *different table*, since SQ11 dropped the item and
kept the schema -- and a Dataflow said `<oci-namespace>.SalesLake.claim`,
using the object-storage namespace as a SQL catalog. All three graded PASS.

It lives at the top level, not under `translate/`, because `plan/planner.py`
needs it too and a planner importing a translator would be backwards.
"""
from __future__ import annotations

import re

DEFAULT_CATALOG = "default"
# `dbo` is Fabric's default Warehouse schema. 42 of the 44 objects in the real
# Warehouse corpus use it, so carrying it into every name would add noise to
# 95% of them to preserve something that matters in 5%. A Lakehouse usually has
# no schema at all, so dropping the default makes the two symmetrical.
DEFAULT_SCHEMAS = frozenset({"dbo"})

# What Spark reads as a name without being told. Anything else has to be
# quoted; see `quote_part`.
_PLAIN_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# A catalog is the first part of every table name this tool emits, so a dot
# or a space in it does not make an odd-looking name, it makes a different
# name -- `--catalog a.b` would silently produce a four-part reference. Kept
# to one plain token; a hyphen is allowed because real Fabric-adjacent names
# use them and `quote_part` can carry them safely.
_CATALOG_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,127}")


class NamingError(ValueError):
    """The parts given cannot make a well-formed AIDP name."""


def _clean(value) -> str:
    return str(value or "").strip().strip("`[]\"")


def quote_part(part) -> str:
    """One name part, backticked if Spark would not read it as a name.

    `aidp_table` output is used in two places -- `spark.table("<name>")` and
    raw Spark SQL -- and Spark parses both with the same multipart-identifier
    parser, so a backtick means the same thing in each. That is why backticks
    are the quoting character here rather than brackets (an array subscript to
    Spark) or double quotes (a string literal under default settings).

    Quoting only the parts that need it, rather than every part, is
    deliberate: `default.SalesLake.claim` stays readable and stays stable
    against the names already in this repo's fixtures, while the real corpus
    items `fabric-data-engineering-ws_on-prem-warehouse-test-wh` and
    `Sales Lake` come out legal instead of being read as subtraction and as a
    syntax error respectively.
    """
    part = str(part or "")
    if _PLAIN_IDENT_RE.fullmatch(part):
        return part
    return "`" + part.replace("`", "``") + "`"


# A column *reference* -- the string `F.col(...)` and `DataFrame.select(str)`
# are handed -- is parsed as a multipart identifier, so the two characters of
# that grammar have to be quoted and nothing else does. MEASURED on Spark
# 4.2.0, frame with a flat column literally named `a.b`:
#
#     df.select("a.b")            FAIL UNRESOLVED_COLUMN.WITH_SUGGESTION
#     F.col("a.b")                FAIL UNRESOLVED_COLUMN.WITH_SUGGESTION
#     df["a.b"]                   FAIL UNRESOLVED_COLUMN.WITH_SUGGESTION
#     F.col("`a.b`")              OK   ['a.b']
#     df.select("`a.b`")          OK   ['a.b']
#     F.col("`a``b`")             OK   ['a`b']     (column named a`b)
#
# A hyphen and a space are NOT in that grammar and need no quoting:
# `df.select("order-id")`, `F.col("order-id")`, `F.col("line total")` all
# resolve. Measured on the same session.
_COLUMN_METACHARACTERS = ".`"


def spark_column_ref(name) -> str:
    '''One column name, ready for a position Spark *parses* as an identifier.

    For `F.col(<here>)` and `DataFrame.select(<here>)` -- and for nothing
    else. `DataFrame.drop`, `DataFrame.toDF` and `withColumn`'s first
    argument take a name verbatim and never parse it, so quoting there
    would look for a column whose name really does start with a backtick.
    Measured: `df.drop("a.b")` succeeds on the flat column `a.b`, where
    `df.select("a.b")` fails.

    Quoted only where the grammar requires it, the same rule `quote_part`
    follows for table names and for the same reason: `F.col('order-id')` is
    correct and readable, and backticking every name would rewrite every
    generated file in this repo to no effect.

    Power Query makes dotted column names routinely --
    `Table.ExpandRecordColumn` names its output `Customer.Name` by default
    -- so this is not an exotic shape.
    '''
    name = str(name)
    if not any(char in name for char in _COLUMN_METACHARACTERS):
        return name
    return "`" + name.replace("`", "``") + "`"


# What a catalog accepts as a schema or table name.
#
# MEASURED on the AIDP cluster (Spark 3.5.0), 2026-09-30 -- the
# metastore the migrated names are created in:
#
#     CREATE SCHEMA IF NOT EXISTS default.`fabric-data-engineering-ws_on-prem-warehouse-test-wh`
#         FAIL MetaException(message:name: fabric-data-engineering-ws_on-prem-
#              warehouse-test-wh. Only lower-case characters, numbers and
#              underscores are allowed.)
#     CREATE SCHEMA IF NOT EXISTS default.`R2 Sales Lake`     FAIL, same message
#     CREATE SCHEMA IF NOT EXISTS default.`r2Ünï`             FAIL, same message
#     CREATE SCHEMA IF NOT EXISTS default.R2MixedCase         OK
#         SHOW SCHEMAS then lists `r2mixedcase`, and a following
#         CREATE SCHEMA default.r2mixedcase fails [SCHEMA_ALREADY_EXISTS].
#         Errors name schemas as `hive.<lowercased>`, e.g. "There is no
#         database hive.saleslake".
#
# So the AIDP schema-name alphabet is [a-z0-9_] *after case-folding*, and the
# folding is silent: `SalesLake` is accepted and becomes `saleslake`, while a
# hyphen, a space or a non-ASCII letter is refused however it is quoted.
#
# The earlier measurement, on pyspark 4.2.0 with its built-in Hive-compatible
# catalog, reached the same alphabet for tables as well --
#
#     saveAsTable("pdb.plain")            OK
#     saveAsTable("pdb.`my-table`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     saveAsTable("pdb.`my table`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     saveAsTable("pdb.`my.table`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     saveAsTable("pdb.`myÜmlaut`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     CREATE DATABASE `my-lake`           FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     CREATE DATABASE my_lake             OK
#
# -- while a *read* of such a name parses and merely finds nothing, because
# no such object can have been created.
_ADDRESSABLE_RE = re.compile(r"[a-z0-9_]+")


def addressable(part) -> bool:
    """Whether AIDP's metastore accepts `part` as a schema (or table) name.

    Lower-cased first, because the metastore folds case silently -- see the
    measurement above -- and ASCII-only, because `str.lower` maps a few
    non-ASCII letters (the Kelvin sign) onto ASCII ones the metastore was
    never shown.
    """
    part = str(part or "")
    return part.isascii() and _ADDRESSABLE_RE.fullmatch(part.lower()) is not None


def unaddressable_parts(name) -> list:
    """The parts of an `aidp_table` name AIDP's metastore would reject.

    Takes the name `aidp_table` built, back-quotes and all, and gives back
    the parts that are not `[a-z0-9_]+` once lower-cased (`addressable`).
    Empty when every part is one, which is the case for every table in the
    synthetic fixtures.

    Reported by the caller rather than refused here, and that is a
    deliberate choice: which name an object gets on AIDP is the naming
    convention's decision (and, past it, the operator's), and this repo's
    own real corpus holds `fabric-data-engineering-ws_on-prem-warehouse-
    test-wh` as an item name, so the shape is common rather than
    pathological. A finding carrying the measurement lets the operator
    rename; a refusal would take the choice away, and silence hides a
    CREATE that fails.
    """
    parts, rest = [], str(name or "")
    for raw in re.findall(r"`((?:[^`]|``)*)`|([^.`]+)", rest):
        part = raw[0].replace("``", "`") if raw[0] else raw[1]
        if part and not addressable(part):
            parts.append(part)
    return parts


def unaddressable_schema(item, schema="") -> str:
    """The AIDP schema `aidp_schema(item, schema)` builds, if AIDP refuses it.

    "" when the schema is one AIDP can create, or when there is no item (a
    two-part name has no schema part to refuse). This is the check every
    translator runs where it emits a name: the schema is the part AIDP has to
    CREATE before any table in it exists, and it is built from a Fabric item
    display name, which Fabric lets contain hyphens and spaces.
    """
    built = aidp_schema(item, schema)
    return "" if not built or addressable(built) else built


def unaddressable_schema_detail(schemas) -> str:
    """The sentence every translator's schema finding says, one wording.

    `schemas` is the distinct AIDP schema names one object emitted that
    AIDP will not create. Shared so the T-SQL, notebook and Dataflow
    findings cannot drift into three accounts of one measurement.
    """
    schemas = list(schemas)
    named = ", ".join(repr(s) for s in schemas)
    return (
        f"AIDP schema {named} cannot be created on AIDP"
        if len(schemas) == 1 else
        f"AIDP schemas {named} cannot be created on AIDP") + (
        ": its metastore accepts only lower-case letters, digits and "
        "underscores in a schema name (upper case is folded silently; a "
        "hyphen, a space or a non-ASCII letter is refused even back-quoted). "
        "MEASURED on an AIDP cluster (Spark 3.5.0), 2026-09-30: CREATE "
        "SCHEMA IF NOT EXISTS default.`fabric-data-engineering-ws_on-prem-"
        "warehouse-test-wh` was refused with \"MetaException(message:name: "
        "... Only lower-case characters, numbers and underscores are "
        "allowed.)\", while default.R2MixedCase was created as r2mixedcase. The "
        "name was emitted as the naming convention builds it, so no table "
        "under it can be created or read; rename the Fabric item or "
        "schema, or choose an AIDP schema by hand, before running")


def validated_catalog(catalog) -> str:
    """The AIDP catalog, or a NamingError naming what is wrong with it.

    `--namespace` has been validated since the first release; `--catalog`
    reached the first position of every table name unchecked.
    """
    catalog = _clean(catalog)
    if not catalog:
        return DEFAULT_CATALOG
    if _CATALOG_RE.fullmatch(catalog) is None:
        raise NamingError(
            f"AIDP catalog {catalog!r} is not a single identifier: it must "
            f"start with a letter or underscore and contain only letters, "
            f"digits, underscores or hyphens. The catalog is the first part "
            f"of every table name, so a dot or a space in it changes which "
            f"table the name refers to rather than how it looks")
    return catalog


def aidp_schema(item, schema="") -> str:
    """Fabric item (+ schema) -> the AIDP schema name, unquoted.

    `SalesLake` + `dbo`   -> `SalesLake`      (Fabric's default, dropped)
    `SalesLake` + `sales` -> `SalesLake_sales`
    `AcmeDW`    + ``      -> `AcmeDW`

    Unquoted because the callers that want this on its own -- the plan's
    `target.schema` field -- want a name, not a SQL fragment. `aidp_table`
    quotes it when it lands in SQL.
    """
    item, schema = _clean(item), _clean(schema)
    if schema and not item:
        # Cannot occur in a Fabric export, and the alternative is emitting
        # `<catalog>._<schema>.<table>`, which looks deliberate and is not.
        raise NamingError(
            f"schema {schema!r} with no item; a Fabric schema always sits "
            f"inside a Lakehouse or Warehouse")
    if not schema or schema.casefold() in DEFAULT_SCHEMAS:
        return item
    return f"{item}_{schema}"


def aidp_table(item, table, schema="", catalog=DEFAULT_CATALOG) -> str:
    """The one place a three-part AIDP table name is built.

    With no item the name is two parts, `<catalog>.<table>`, rather than a
    fabricated schema: NB11 already flags a missing lakehouse, and inventing a
    placeholder would turn a flagged unknown into a confident wrong name.

    The result is ready to paste into Spark SQL or into `spark.table("...")`:
    every part that is not a plain identifier is backticked. See `quote_part`
    for why the two uses can share one form.
    """
    table = _clean(table)
    if not table:
        raise NamingError("a table name is required")
    catalog = _clean(catalog) or DEFAULT_CATALOG
    middle = aidp_schema(item, schema)
    parts = [catalog, middle, table] if middle else [catalog, table]
    return ".".join(quote_part(part) for part in parts)


def note_unaddressable_schema(findings, rule, item, schema=""):
    """Flag, once per object, an emitted AIDP schema AIDP will not create.

    Called by a translator at the point it emits `aidp_table(item, ...,
    schema=schema)`. The first bad schema appends one `flag` finding under
    `rule`; every further one -- a view reading two such items -- is added
    to that same finding rather than to a second one, so an object carries
    one finding naming every refused schema. A repeat of a schema already
    named adds nothing.

    The schemas so far ride on the finding as `aidp_schemas` so the detail
    can be rebuilt; nothing serialises that attribute, and the detail says
    the same thing in words.
    """
    bad = unaddressable_schema(item, schema)
    if not bad:
        return
    from fabric_aidp.translate.types import Finding

    for finding in findings:
        if finding.rule == rule:
            named = getattr(finding, "aidp_schemas", [])
            if bad not in named:
                named.append(bad)
                finding.aidp_schemas = named
                finding.detail = unaddressable_schema_detail(named)
            return
    finding = Finding(rule, unaddressable_schema_detail([bad]), "flag")
    finding.aidp_schemas = [bad]
    findings.append(finding)
