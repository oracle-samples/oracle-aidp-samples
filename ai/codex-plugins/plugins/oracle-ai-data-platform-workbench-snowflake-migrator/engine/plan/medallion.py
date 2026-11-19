"""Medallion mapping. Pure functions, zero I/O.

BRONZE IS A MIRROR, NOT AN ASSIGNMENT. The required mapping is structural:

    Snowflake database  ->  AIDP Standard Catalog
    Snowflake schema    ->  AIDP schema
    Snowflake table     ->  AIDP table
    Snowflake view      ->  AIDP view

So Bronze is an identity transform on the three-part name. AIDP supports
three-level namespaces, so nothing has to be flattened and nothing can collide --
which is why there is no layer-assignment heuristic here any more. An earlier
design assigned each object a layer by name matching; that was wrong for this
requirement and produced a `bronze.<schema>.<table>` shape that silently dropped
the source database.

SILVER AND GOLD ARE REQUIREMENT-DRIVEN. Their content depends on transformations
nobody has specified yet, so this module does not invent any. It emits one job
per layer per source schema, created but **never triggered**, as the place that
logic will later live. Fabricating transformation SQL would ship logic no one
asked for and no one can review.
"""
from __future__ import annotations

import collections
import re

__all__ = ["LAYERS", "SCHEMA_STYLES", "TARGET_KEY_MAX", "UnknownStrategy",
           "bronze_target", "layer_jobs", "detect_target_collisions",
           "target_key_overage"]

SCHEMA_STYLES = ("db_schema", "db")

LAYERS = ("BRONZE", "SILVER", "GOLD")

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# Measured against a live DataLake on 2026-09-24, not taken from a doc: a
# `catalog.schema.name` key of 255 characters is created, 256 returns 202
# Accepted and never appears. Confirmed under schema names of two different
# lengths, so it is the whole key that is bounded and not the object name.
TARGET_KEY_MAX = 255


def target_key_overage(target_fqn: str) -> int:
    """How many characters the whole target key is over the limit, or 0.

    The key is the three-part name the catalog stores, so a long catalog or
    schema leaves a short table name no room -- which is why the caller
    reports the parts and not just the object name.
    """
    return max(0, len(str(target_fqn)) - TARGET_KEY_MAX)


class UnknownStrategy(ValueError):
    """An identifier or option is not usable as an AIDP catalog/schema name."""


# What the destination accepts for a schema, table or view name. Live
# 2026-09-22, AIDP answered a create of `mixed case table` with 400
# `InvalidParameter: Invalid name: mixed case table. Only lower-case
# characters, numbers and underscores are allowed.`, and a view named
# `v quoted` with `Should start with a letter, no spaces or special
# characters except for underscore`. A Snowflake identifier is far wider than
# that -- anything inside double quotes -- so the difference is a migration
# fact, not an edge case.
TARGET_NAME_RULE = re.compile(r"^[a-z][a-z0-9_]*$")

TARGET_NAME_RULE_TEXT = (
    "the destination accepts a name of lower-case letters, digits and "
    "underscores, starting with a letter")


# Measured against a live DataLake on 2026-09-24, not taken from a doc: a
# `catalog.schema.name` key of 255 characters is created, 256 returns 202
# Accepted and never appears. Confirmed twice, under schema names of two
# different lengths, so it is the key that is bounded and not the name.
TARGET_KEY_MAX = 255


def target_key_overage(target_fqn: str) -> int:
    """How many characters the whole target key is over the limit, or 0.

    The key is the three-part name the catalog API stores, so a long
    catalog or schema leaves a short table name no room -- which is why the
    caller reports the parts and not just the object name.
    """
    return max(0, len(str(target_fqn)) - TARGET_KEY_MAX)


def unacceptable_target_names(target_fqn: str) -> list[str]:
    """The parts of a planned FQN the destination will refuse, if any.

    Checked at plan time because the alternative is what happened live: the
    plan shows the name, the DDL is generated, the create returns 400, and
    the name is burned for the rest of the run.
    """
    return [part for part in str(target_fqn).split(".")
            if not TARGET_NAME_RULE.match(part)]


def bronze_target(source_db: str, source_schema: str, object_name: str, *,
                  catalog_prefix: str | None = None,
                  schema_style: str = "db_schema",
                  fold_case: bool = True) -> str:
    """The Bronze target FQN for one source object. Names are never rewritten.

    Without a prefix, Bronze mirrors the source exactly: the Snowflake database
    becomes its own AIDP Standard Catalog.

    With a prefix, everything lands inside one existing catalog and the target
    schema encodes the source. Two styles:

      db_schema (default) -- `prefix.DB_SCHEMA.NAME`. Keeps two same-named
        schemas apart inside one catalog, which is why it is the default.
      db                  -- `prefix.DB.NAME`, giving `lake.<database>.<table>`.
        Closer to a 1:1 read of the source, and it CAN collide: two schemas in
        the same database that share a table name map to one target. That is
        caught by detect_target_collisions, which halts the run rather than
        merging two different tables.

    The object name itself is carried over verbatim in every style -- only its
    CASE changes.

    `fold_case` (default on) lower-cases the target name, because **AIDP
    lower-cases identifiers**: a schema created as `SNOWMIG_TESTDB` comes
    back as `snowmig_testdb`. Planning the folded name means the plan
    shows the name the destination will really use, rather than one that then
    silently differs. It also makes the fold visible to
    detect_target_collisions, which is what stops Snowflake's `ORDERS` and
    `"orders"` -- two different tables -- from quietly becoming one.
    """
    if schema_style not in SCHEMA_STYLES:
        raise ValueError(
            f"unknown schema_style {schema_style!r}; expected one of "
            f"{list(SCHEMA_STYLES)}")
    for part in (source_db, source_schema, object_name):
        if not part or not str(part).strip():
            raise UnknownStrategy(f"empty name component in "
                                  f"{source_db!r}.{source_schema!r}.{object_name!r}")
    def cased(value: str) -> str:
        return str(value).lower() if fold_case else str(value)

    if catalog_prefix is None:
        return f"{cased(source_db)}.{cased(source_schema)}.{cased(object_name)}"
    if not _SAFE_IDENT.match(catalog_prefix):
        raise UnknownStrategy(
            f"catalog_prefix {catalog_prefix!r} is not a valid identifier")
    schema = source_db if schema_style == "db" else f"{source_db}_{source_schema}"
    return f"{cased(catalog_prefix)}.{cased(schema)}.{cased(object_name)}"


def layer_jobs(scopes: list[tuple[str, str]]) -> list[dict]:
    """One SILVER and one GOLD job per (database, schema) scope.

    Created, disabled, and never triggered. The body is an explicit placeholder:
    what belongs there is a requirement, not something to guess.
    """
    jobs: list[dict] = []
    for db, schema in scopes:
        for layer in ("SILVER", "GOLD"):
            jobs.append({
                "name": f"{layer.lower()}_{db}_{schema}",
                "layer": layer,
                "source_database": db,
                "source_schema": schema,
                "reads_from": f"{db}.{schema}",
                "enabled": False,
                "schedule": None,
                "trigger": "MANUAL_NEVER_TRIGGERED",
                "body_status": "placeholder",
                "body_note": (
                    f"{layer} transformation logic is a requirement to be defined "
                    "with the customer. This job is created as the place it will "
                    "live; it is disabled and is never triggered by the migrator."),
            })
    return jobs


def detect_target_collisions(mapping: dict[str, str]) -> dict[str, list[str]]:
    """Two source objects mapping to one target name. Empty result means safe.

    Bronze cannot collide by construction, but this still runs: a
    `catalog_prefix` deployment, or a source estate with case-variant names, can
    produce one. A collision halts rather than being resolved by guessing.
    """
    buckets: dict[str, list[str]] = collections.defaultdict(list)
    for source, target in mapping.items():
        buckets[target.upper()].append(source)
    return {mapping[v[0]]: sorted(v) for v in buckets.values() if len(v) > 1}
