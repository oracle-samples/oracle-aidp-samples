"""User-supplied restrictions applied to the estate. Pure."""
import pytest

from plan.restrictions import (
    InvalidRestriction, apply_restrictions, validate_restrictions,
)


def rec(ident, kind="TABLE", rows=10, byts=100):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": kind,
            "source_database": db, "source_schema": schema,
            "row_count_exact": rows, "source_metadata": {"bytes": byts}}


RECS = [rec("D1.PUBLIC.ORDERS"), rec("D1.STAGE.TMP_X"),
        rec("D2.PUBLIC.BIG", rows=5_000_000), rec("D1.PUBLIC.V", kind="VIEW")]


def test_no_restrictions_keeps_everything():
    kept, excluded = apply_restrictions(RECS, None)
    assert len(kept) == 4 and excluded == []


def test_exclude_databases():
    kept, excluded = apply_restrictions(RECS, {"exclude_databases": ["D2"]})
    assert [e["source_identifier"] for e in excluded] == ["D2.PUBLIC.BIG"]
    assert "database D2 excluded" in excluded[0]["reason"]


def test_include_databases_is_an_allowlist():
    kept, excluded = apply_restrictions(RECS, {"include_databases": ["D2"]})
    assert [k["source_identifier"] for k in kept] == ["D2.PUBLIC.BIG"]
    assert len(excluded) == 3


def test_exclude_schemas():
    kept, _ = apply_restrictions(RECS, {"exclude_schemas": ["STAGE"]})
    assert "D1.STAGE.TMP_X" not in [k["source_identifier"] for k in kept]


def test_exclude_object_types():
    kept, excluded = apply_restrictions(RECS, {"exclude_object_types": ["VIEW"]})
    assert all(k["object_type"] == "TABLE" for k in kept)
    assert "object type VIEW excluded" in excluded[0]["reason"]


def test_max_rows():
    kept, excluded = apply_restrictions(RECS, {"max_rows": 1000})
    assert "D2.PUBLIC.BIG" in [e["source_identifier"] for e in excluded]
    assert "5000000 rows exceeds max_rows 1000" in excluded[0]["reason"]


def test_max_bytes():
    _, excluded = apply_restrictions(RECS, {"max_bytes": 50})
    assert len(excluded) == 4


def test_exclude_name_pattern():
    kept, excluded = apply_restrictions(RECS, {"exclude_name_patterns": ["^TMP_"]})
    assert "D1.STAGE.TMP_X" in [e["source_identifier"] for e in excluded]
    assert "^TMP_" in excluded[0]["reason"]


def test_explicit_object_exclusion_wins():
    _, excluded = apply_restrictions(RECS, {"exclude_objects": ["D1.PUBLIC.ORDERS"]})
    assert excluded[0]["source_identifier"] == "D1.PUBLIC.ORDERS"
    # An unquoted entry folds, so the reason names the entry and the fold; only
    # a double-quoted, exact hit is reported as the operator's explicit choice.
    assert "exclude_objects entry 'D1.PUBLIC.ORDERS'" in excluded[0]["reason"]
    assert "explicitly" not in excluded[0]["reason"]


def test_object_exclusion_is_case_insensitive():
    _, excluded = apply_restrictions(RECS, {"exclude_objects": ["d1.public.orders"]})
    assert len(excluded) == 1


def test_multiple_restrictions_combine():
    kept, excluded = apply_restrictions(
        RECS, {"exclude_object_types": ["VIEW"], "max_rows": 1000})
    assert [k["source_identifier"] for k in kept] == ["D1.PUBLIC.ORDERS",
                                                      "D1.STAGE.TMP_X"]
    assert len(excluded) == 2


def test_every_exclusion_carries_a_reason():
    _, excluded = apply_restrictions(RECS, {"exclude_databases": ["D1", "D2"]})
    assert all(e["reason"] for e in excluded)
    assert all(e["restriction"] for e in excluded)


def test_unknown_restriction_key_is_rejected_not_ignored():
    # Silently ignoring a typo'd key would apply nothing and look like success.
    with pytest.raises(InvalidRestriction, match="exclude_datbases"):
        validate_restrictions({"exclude_datbases": ["D1"]})


def test_wrong_type_rejected():
    with pytest.raises(InvalidRestriction, match="list"):
        validate_restrictions({"exclude_databases": "D1"})


def test_bad_regex_rejected_early():
    with pytest.raises(InvalidRestriction, match="regex"):
        validate_restrictions({"exclude_name_patterns": ["([unclosed"]})


def test_valid_restrictions_pass_validation():
    validate_restrictions({"exclude_databases": ["A"], "max_rows": 10,
                           "exclude_name_patterns": ["^TMP_"]})


# --- case-collision twins ---------------------------------------------------
#
# `customers` and `"customers"` are DIFFERENT Snowflake objects, and a case
# collision HALTs the plan rather than being guessed away. The only in-tool
# remedy is a restriction, so a restriction must be able to name exactly one
# twin: a double-quoted part is matched case-sensitively, as Snowflake itself
# resolves it; an unquoted part folds to upper, as before.

TWINS = [rec("D1.PUBLIC.ORDERS"), rec("D1.PUBLIC.orders"), rec("D2.PUBLIC.X")]


def test_quoted_exclude_entry_drops_only_the_twin_it_spells():
    kept, excluded = apply_restrictions(
        TWINS, {"exclude_objects": ['"D1"."PUBLIC"."orders"']})
    assert [e["source_identifier"] for e in excluded] == ["D1.PUBLIC.orders"]
    assert "D1.PUBLIC.ORDERS" in [k["source_identifier"] for k in kept]
    assert "explicitly excluded" in excluded[0]["reason"]


def test_quoted_include_entry_admits_exactly_one_twin():
    kept, _ = apply_restrictions(
        TWINS, {"include_objects": ['"D1"."PUBLIC"."ORDERS"']})
    assert [k["source_identifier"] for k in kept] == ["D1.PUBLIC.ORDERS"]


def test_quoting_only_the_last_part_folds_the_others():
    _, excluded = apply_restrictions(
        TWINS, {"exclude_objects": ['d1.public."orders"']})
    assert [e["source_identifier"] for e in excluded] == ["D1.PUBLIC.orders"]


def test_unquoted_entry_still_folds_and_the_reason_says_so():
    # Unchanged behaviour, made honest: both twins go, and each exclusion says
    # it was a case-insensitive match rather than "explicitly excluded".
    _, excluded = apply_restrictions(TWINS, {"exclude_objects": ["d1.public.orders"]})
    assert sorted(e["source_identifier"] for e in excluded) == [
        "D1.PUBLIC.ORDERS", "D1.PUBLIC.orders"]
    assert all("case-insensitively" in e["reason"] for e in excluded)
    assert all("'d1.public.orders'" in e["reason"] for e in excluded)
    # The twin the operator did not name must not be blamed on the operator.
    assert all("explicitly" not in e["reason"] for e in excluded)


# --- degenerate values are errors, not silent no-ops ----------------------
#
# An empty allowlist admitted the whole estate while PLANNED_OBJECTS.md said
# the restriction was "in force"; an empty pattern excluded everything; a
# negative cap excluded every counted table; JSON true passed as an integer.
# The module's own rule -- a typo'd key must not apply nothing while appearing
# to succeed -- applies to values as much as keys.

INCLUDE_KEYS = ("include_databases", "include_schemas", "include_object_types",
                "include_objects", "include_name_patterns")
EXCLUDE_KEYS = ("exclude_databases", "exclude_schemas", "exclude_object_types",
                "exclude_objects", "exclude_name_patterns")


@pytest.mark.parametrize("key", INCLUDE_KEYS)
def test_empty_include_list_is_rejected(key):
    with pytest.raises(InvalidRestriction, match="empty"):
        validate_restrictions({key: []})


def test_empty_include_objects_does_not_admit_the_whole_estate():
    with pytest.raises(InvalidRestriction, match="include_objects"):
        apply_restrictions(RECS, {"include_objects": []})


@pytest.mark.parametrize("key", EXCLUDE_KEYS)
def test_empty_exclude_list_is_rejected(key):
    with pytest.raises(InvalidRestriction, match="empty"):
        validate_restrictions({key: []})


@pytest.mark.parametrize("pattern", ["", "  "])
def test_empty_or_blank_pattern_is_rejected(pattern):
    with pytest.raises(InvalidRestriction, match="exclude_name_patterns"):
        validate_restrictions({"exclude_name_patterns": [pattern]})


def test_non_string_list_entries_are_rejected():
    with pytest.raises(InvalidRestriction, match="include_objects"):
        validate_restrictions({"include_objects": [None, 123]})


@pytest.mark.parametrize("bad", [{"max_rows": -1}, {"max_bytes": -1},
                                 {"max_rows": True}, {"max_bytes": False}])
def test_negative_or_bool_caps_are_rejected(bad):
    with pytest.raises(InvalidRestriction, match="integer|>= 0"):
        validate_restrictions(bad)


def test_zero_cap_is_a_legitimate_cap():
    assert validate_restrictions({"max_rows": 0}) == {"max_rows": 0}


def test_unknown_object_type_is_rejected():
    # `VIEWS` excluded nothing, silently. The vocabulary is TABLE and VIEW.
    with pytest.raises(InvalidRestriction, match="VIEWS"):
        validate_restrictions({"exclude_object_types": ["VIEWS"]})
    assert validate_restrictions({"exclude_object_types": ["view"]})


# --- a cap that cannot be evaluated is not a pass -------------------------

def test_unknown_row_count_under_max_rows_is_excluded_with_the_reason():
    big = rec("D.S.BIG", rows=None)
    big["row_count_note"] = "not counted: --row-counts none"
    kept, excluded = apply_restrictions([big, rec("D.S.SMALL", rows=5)],
                                        {"max_rows": 10})
    assert [k["source_identifier"] for k in kept] == ["D.S.SMALL"]
    assert excluded[0]["source_identifier"] == "D.S.BIG"
    assert excluded[0]["restriction"] == "max_rows"
    assert "unknown" in excluded[0]["reason"]
    assert "cannot be evaluated" in excluded[0]["reason"]
    assert "--row-counts none" in excluded[0]["reason"]


def test_unknown_size_under_max_bytes_is_excluded_with_the_reason():
    nosize = rec("D.S.T")
    nosize["source_metadata"] = {}
    _, excluded = apply_restrictions([nosize], {"max_bytes": 10})
    assert excluded[0]["restriction"] == "max_bytes"
    assert "unknown" in excluded[0]["reason"]
    assert "cannot be evaluated" in excluded[0]["reason"]


def test_upper_case_BYTES_metadata_is_still_capped():
    # The in-AIDP discovery bridge writes the key upper-case; the cap must
    # read it, or max_bytes is a no-op on every manifest-sourced inventory.
    r = rec("D.S.T")
    r["source_metadata"] = {"BYTES": 10**12}
    _, excluded = apply_restrictions([r], {"max_bytes": 1000})
    assert excluded and "exceeds max_bytes" in excluded[0]["reason"]


# ------------------------------- name patterns fold case, like every sibling
#
# Snowflake upper-cases every
# unquoted identifier, so a hand-written `^tmp_` matched nothing in a real
# estate -- the one restriction that could look right and silently do
# nothing, while `exclude_databases: [sales]` has always matched SALES.

def _rec(ident, kind="TABLE"):
    db, schema, name = ident.split(".")
    return {"source_identifier": ident, "object_type": kind,
            "source_database": db, "source_schema": schema,
            "compatibility_status": "supported", "source_metadata": {}}


def test_an_exclude_pattern_matches_an_upper_cased_name():
    kept, excluded = apply_restrictions(
        [_rec("D.S.TMP_ORDERS"), _rec("D.S.ORDERS")],
        {"exclude_name_patterns": ["^tmp_"]})
    assert [r["source_identifier"] for r in kept] == ["D.S.ORDERS"]
    assert excluded[0]["restriction"] == "exclude_name_patterns"


def test_an_include_pattern_matches_an_upper_cased_name():
    kept, _ = apply_restrictions(
        [_rec("D.S.DIM_DATE"), _rec("D.S.FACT_SALES")],
        {"include_name_patterns": ["^dim_"]})
    assert [r["source_identifier"] for r in kept] == ["D.S.DIM_DATE"]


def test_a_pattern_written_in_upper_case_still_matches_a_lower_name():
    kept, _ = apply_restrictions(
        [_rec("D.S.tmp_x"), _rec("D.S.keep")],
        {"exclude_name_patterns": ["^TMP_"]})
    assert [r["source_identifier"] for r in kept] == ["D.S.keep"]
