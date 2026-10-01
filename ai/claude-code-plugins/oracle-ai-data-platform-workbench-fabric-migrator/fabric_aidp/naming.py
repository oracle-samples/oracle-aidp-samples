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


# What Spark's own catalog accepts as a schema or table name. MEASURED on
# Spark 4.2.0 with its built-in Hive-compatible catalog: everything outside
# this is rejected by CREATE and by saveAsTable *even when back-quoted* --
#
#     saveAsTable("pdb.plain")            OK
#     saveAsTable("pdb.`my-table`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     saveAsTable("pdb.`my table`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     saveAsTable("pdb.`my.table`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     saveAsTable("pdb.`myÜmlaut`")       FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     CREATE DATABASE `my-lake`           FAIL INVALID_SCHEMA_OR_RELATION_NAME
#     CREATE DATABASE my_lake             OK
#
# -- while a *read* of the same name parses and merely fails to find
# anything: spark.table("pdb.`my-table`") gives TABLE_OR_VIEW_NOT_FOUND,
# not a parse error. Which is the same conclusion by a longer road: no
# such table can be created, so none can be read.
_CATALOG_NAME_RE = re.compile(r"[A-Za-z0-9_]+")


def unaddressable_parts(name) -> list:
    """The parts of an `aidp_table` name Spark's catalog would reject.

    Takes the name `aidp_table` built, back-quotes and all, and gives back
    the parts that are not `[A-Za-z0-9_]+`. Empty when every part is one,
    which is the case for every table in this repo's fixtures.

    Reported by the caller rather than refused here, and that is a
    deliberate choice: the restriction belongs to the *catalog*, it was
    measured against Spark's built-in one, and AIDP's is not this. A
    refusal on the strength of a measurement taken somewhere else would
    block a migration that may be fine -- and this repo's own real corpus
    holds `fabric-data-engineering-ws_on-prem-warehouse-test-wh` as an item
    name, so the shape is common rather than pathological. A finding
    carrying the measurement lets the operator decide; silence does not.
    """
    parts, rest = [], str(name or "")
    for raw in re.findall(r"`((?:[^`]|``)*)`|([^.`]+)", rest):
        part = raw[0].replace("``", "`") if raw[0] else raw[1]
        if part and _CATALOG_NAME_RE.fullmatch(part) is None:
            parts.append(part)
    return parts


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
