"""Medallion mapping. Bronze mirrors Snowflake 1:1; Silver/Gold are job stubs."""
import pytest

from plan.medallion import (
    LAYERS, UnknownStrategy, bronze_target, layer_jobs, detect_target_collisions,
)


# --- Bronze: a structural mirror, not a layer assignment -------------------

def test_database_becomes_the_catalog():
    # Database -> Standard Catalog, Schemas -> Schemas, Tables -> tables.
    assert bronze_target("MYDB", "SALES", "ORDERS") == "mydb.sales.orders"


def test_bronze_preserves_case_exactly():
    # Snowflake unquoted folds to UPPER; the mirror must not re-fold it.
    assert bronze_target("MyDb", "Sales", "Orders") == "mydb.sales.orders"


def test_bronze_is_identity_so_it_cannot_collide():
    mapping = {
        "DB1.PUBLIC.ORDERS": bronze_target("DB1", "PUBLIC", "ORDERS"),
        "DB2.PUBLIC.ORDERS": bronze_target("DB2", "PUBLIC", "ORDERS"),
    }
    assert detect_target_collisions(mapping) == {}


def test_collision_detector_still_catches_case_variants():
    assert detect_target_collisions({"A": "db.s.t", "B": "DB.S.T"})


def test_optional_catalog_prefix_for_a_shared_bronze_catalog():
    # Some deployments want one bronze catalog rather than catalog-per-database.
    assert bronze_target("MYDB", "SALES", "ORDERS",
                         catalog_prefix="bronze") == "bronze.mydb_sales.orders"


def test_prefix_mode_can_collide_and_is_detected():
    mapping = {
        "DB1.PUBLIC.T": bronze_target("DB1", "PUBLIC", "T", catalog_prefix="bronze"),
        "DB2.PUBLIC.T": bronze_target("DB2", "PUBLIC", "T", catalog_prefix="bronze"),
    }
    assert detect_target_collisions(mapping) == {}, "db is folded in, so distinct"


def test_bad_identifier_rejected():
    with pytest.raises(UnknownStrategy):
        bronze_target("DB", "S", "T", catalog_prefix="not a catalog")


# --- Silver / Gold: jobs created, never triggered --------------------------

def test_silver_and_gold_jobs_are_created_for_each_source_schema():
    jobs = layer_jobs([("MYDB", "SALES"), ("MYDB", "OPS")])
    names = sorted(j["name"] for j in jobs)
    assert names == ["gold_MYDB_OPS", "gold_MYDB_SALES",
                     "silver_MYDB_OPS", "silver_MYDB_SALES"]


def test_jobs_are_never_scheduled_or_triggered():
    for job in layer_jobs([("D", "S")]):
        assert job["trigger"] == "MANUAL_NEVER_TRIGGERED"
        assert job["schedule"] is None
        assert job["enabled"] is False


def test_jobs_declare_their_layer_and_source_scope():
    jobs = layer_jobs([("D", "S")])
    silver = next(j for j in jobs if j["layer"] == "SILVER")
    assert silver["source_database"] == "D"
    assert silver["source_schema"] == "S"
    assert silver["reads_from"] == "D.S"


def test_job_body_is_an_explicit_placeholder_not_fabricated_logic():
    # Silver/Gold transformations are requirement-driven. Inventing SQL here
    # would ship logic nobody specified.
    silver = next(j for j in layer_jobs([("D", "S")]) if j["layer"] == "SILVER")
    assert silver["body_status"] == "placeholder"
    assert "requirement" in silver["body_note"].lower()


def test_no_jobs_for_an_empty_scope():
    assert layer_jobs([]) == []


def test_layers_constant():
    assert LAYERS == ("BRONZE", "SILVER", "GOLD")


# ==========================================================================
# Schema naming style. The default concatenates DB_SCHEMA to keep two
# same-named schemas apart inside one catalog; `db` style names the target
# schema after the Snowflake database alone, which is what a 1:1
# "lake.<snowflake database>.<table>" layout asks for.
# ==========================================================================

def test_db_style_names_the_schema_after_the_database():
    assert bronze_target("TEST_DB", "PUBLIC", "ORDERS", catalog_prefix="lake",
                         schema_style="db") == "lake.test_db.orders"


def test_default_style_is_unchanged():
    assert bronze_target("TEST_DB", "PUBLIC", "ORDERS",
                         catalog_prefix="lake") == "lake.test_db_public.orders"


def test_table_names_are_preserved_apart_from_case_in_both_styles():
    # AIDP folds case, so the name is carried over character-for-character
    # EXCEPT its case. Nothing else about it is rewritten.
    for style in ("db", "db_schema"):
        target = bronze_target("D", "S", "Weird_Name$1", catalog_prefix="lake",
                               schema_style=style)
        assert target.endswith(".weird_name$1")
        assert bronze_target("D", "S", "Weird_Name$1", catalog_prefix="lake",
                             schema_style=style,
                             fold_case=False).endswith(".Weird_Name$1")


def test_db_style_collides_when_two_schemas_share_a_table_name():
    # The reason the default concatenates. Allowed, but the collision must be
    # CAUGHT rather than silently merging two different tables.
    a = bronze_target("D", "PUBLIC", "ORDERS", catalog_prefix="lake",
                      schema_style="db")
    b = bronze_target("D", "STAGING", "ORDERS", catalog_prefix="lake",
                      schema_style="db")
    assert a == b, "this is the hazard the collision detector exists for"
    collisions = detect_target_collisions({"D.PUBLIC.ORDERS": a,
                                           "D.STAGING.ORDERS": b})
    assert collisions, "a two-into-one mapping must be reported"


def test_an_unknown_style_is_refused():
    with pytest.raises(ValueError):
        bronze_target("D", "S", "T", catalog_prefix="lake", schema_style="guess")


# ==========================================================================
# AIDP lower-cases identifiers (verified live). The PLAN must therefore show
# the name the destination will really use, not the name we asked for.
# ==========================================================================

def test_target_names_are_folded_to_lower_case_by_default():
    assert bronze_target("TEST_DB", "PUBLIC", "ORDER_ITEMS",
                         catalog_prefix="lake", schema_style="db") == \
        "lake.test_db.order_items"


def test_folding_applies_without_a_prefix_too():
    assert bronze_target("TEST_DB", "PUBLIC", "ORDERS") == \
        "test_db.public.orders"


def test_folding_can_be_turned_off():
    assert bronze_target("TEST_DB", "PUBLIC", "ORDERS", fold_case=False) == \
        "TEST_DB.PUBLIC.ORDERS"


def test_the_catalog_prefix_is_folded_as_well():
    assert bronze_target("D", "S", "T", catalog_prefix="Lake",
                         schema_style="db") == "lake.d.t"


def test_two_source_names_differing_only_by_case_collide_after_folding():
    # THE hazard folding creates, and the reason detect_target_collisions
    # exists: Snowflake keeps ORDERS and "orders" apart, AIDP cannot.
    a = bronze_target("D", "S", "ORDERS")
    b = bronze_target("D", "S", "orders")
    assert a == b
    collisions = detect_target_collisions({"D.S.ORDERS": a, "D.S.orders": b})
    assert collisions, "folding two distinct tables into one must HALT"


# --------------------------------------------- names the destination refuses
#
# Live 2026-09-22: AIDP answered `create table mixed case table` with 400
# `InvalidParameter: Invalid name: mixed case table. Only lower-case
# characters, numbers and underscores are allowed.`, and a view named
# `v quoted` with `Should start with a letter, no spaces or special
# characters except for underscore`. Both came from double-quoted Snowflake
# identifiers, which are far wider than that.

def test_a_plain_folded_name_is_acceptable():
    from plan.medallion import unacceptable_target_names
    assert unacceptable_target_names("db.sales.orders") == []
    assert unacceptable_target_names("db_1.s_2.t_3") == []


def test_a_name_with_a_space_is_named_as_unacceptable():
    from plan.medallion import unacceptable_target_names
    assert unacceptable_target_names("db.edge.mixed case table") == [
        "mixed case table"]


def test_every_offending_part_is_returned_not_just_the_first():
    from plan.medallion import unacceptable_target_names
    assert unacceptable_target_names("db.my schema.my table") == [
        "my schema", "my table"]


def test_a_name_starting_with_a_digit_is_unacceptable():
    from plan.medallion import unacceptable_target_names
    assert unacceptable_target_names("db.s.2023_orders") == ["2023_orders"]


def test_an_upper_case_name_is_unacceptable_because_the_plan_folds_first():
    from plan.medallion import unacceptable_target_names
    assert unacceptable_target_names("db.s.ORDERS") == ["ORDERS"]


# ------------------------------- the 255-character key, measured not guessed
#
# Live 2026-09-24 against the DataLake. The limit is NOT on the object name:
# it is 255 characters on the whole key `catalog.schema.name`.
#
#   catalog snowmig_coverage_v2 + schema snowmig_coverage_edge (prefix 42)
#       213-char name accepted, 214 refused        42 + 213 = 255
#   same catalog + schema default (prefix 28)
#       227-char name accepted, 228 refused        28 + 227 = 255
#
# The second was predicted from the first and hit exactly, so the rule is
# the key length and nothing else.
#
# That matters more than a plain name limit, because the migrator chooses
# two thirds of the key: `--bronze-catalog-prefix` and the `db_schema`
# style, which concatenates database + "_" + schema, both eat the budget a
# table name has left. And the failure mode is the bad one: 202 Accepted,
# the object never appears, and the name is then burned in that schema.

def test_a_short_key_is_fine():
    from plan.medallion import target_key_overage
    assert target_key_overage("db.sales.orders") == 0


def test_a_key_of_exactly_255_is_accepted():
    from plan.medallion import target_key_overage, TARGET_KEY_MAX
    assert TARGET_KEY_MAX == 255
    fqn = "c.s." + "x" * (255 - 4)
    assert len(fqn) == 255
    assert target_key_overage(fqn) == 0


def test_one_character_over_is_reported_as_one_over():
    from plan.medallion import target_key_overage
    assert target_key_overage("c.s." + "x" * (256 - 4)) == 1


def test_the_overage_counts_the_whole_key_not_the_name():
    """The live shape: the same name passes under a short schema and fails
    under a long one."""
    from plan.medallion import target_key_overage
    name = "x" * 227
    assert target_key_overage(f"snowmig_coverage_v2.default.{name}") == 0
    assert target_key_overage(
        f"snowmig_coverage_v2.snowmig_coverage_edge.{name}") == 14


def test_a_too_long_target_is_refused_at_plan_time():
    from plan.build import build_plan
    long_name = "T" + "X" * 250
    plan = build_plan({"inventory": [rec_for_key(long_name)]}, {"edges": []})
    assert plan["can_migrate"] == []
    entry = plan["cannot_migrate"][0]
    assert entry["category"] == "target_key_too_long"
    assert "255" in entry["reason"]


def test_the_refusal_says_what_is_eating_the_budget():
    """An operator who only sees "name too long" shortens the table name.
    The catalog and schema are usually where the room actually is, and they
    are the parts this tool chose."""
    from plan.build import build_plan
    plan = build_plan({"inventory": [rec_for_key("T" + "X" * 250)]},
                      {"edges": []}, bronze_catalog_prefix="a_long_prefix")
    reason = plan["cannot_migrate"][0]["reason"]
    assert "catalog" in reason.lower() and "schema" in reason.lower()
    assert "bronze-catalog-prefix" in reason


def test_an_ordinary_name_still_plans_under_a_prefix():
    from plan.build import build_plan
    plan = build_plan({"inventory": [rec_for_key("ORDERS")]}, {"edges": []},
                      bronze_catalog_prefix="bronze")
    assert [c["source_identifier"] for c in plan["can_migrate"]] == [
        "D.SALES.ORDERS"]


def rec_for_key(name):
    return {"source_identifier": f"D.SALES.{name}", "object_type": "TABLE",
            "source_database": "D", "source_schema": "SALES",
            "compatibility_status": "supported", "blocked_reasons": [],
            "row_count_exact": 0, "source_metadata": {}}
