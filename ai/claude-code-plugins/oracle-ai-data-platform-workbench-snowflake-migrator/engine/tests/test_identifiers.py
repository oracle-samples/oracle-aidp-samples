"""Identifier case policy and collision detection. Pure; no connection."""
import pytest

from snowflake_source.dialect.identifiers import (
    UnsafeIdentifier, assert_safe_identifier, case_form, detect_collisions,
)


def test_upper_is_unquoted_form():
    assert case_form("CUSTOMERS") == "UPPER_UNQUOTED"


@pytest.mark.parametrize("name", ["customers", "Customers", "cUSTOMERS"])
def test_non_upper_is_case_sensitive_form(name):
    assert case_form(name) == "MIXED_QUOTED_CASE_SENSITIVE"


def test_digits_and_underscores_are_upper_form():
    assert case_form("ORDER_ITEMS_2024") == "UPPER_UNQUOTED"


def test_no_collision_returns_empty():
    assert detect_collisions(["DB.S.A", "DB.S.B"]) == {}


def test_same_name_twice_is_not_a_collision():
    # The same object listed twice is a duplicate, not two colliding objects.
    assert detect_collisions(["DB.S.A", "DB.S.A"]) == {}


def test_case_variants_collide():
    got = detect_collisions(["DB.S.customers", "DB.S.CUSTOMERS"])
    assert list(got) == ["DB.S.CUSTOMERS"]
    assert sorted(got["DB.S.CUSTOMERS"]) == ["DB.S.CUSTOMERS", "DB.S.customers"]


def test_three_way_collision_lists_all():
    got = detect_collisions(["A.B.c", "A.B.C", "A.B.C "])
    assert len(got["A.B.C"]) >= 2


@pytest.mark.parametrize("bad", ["", "   ", "a`b", "a\tb", "a\nb", "a\x00b", None])
def test_unsafe_identifiers_rejected(bad):
    with pytest.raises(UnsafeIdentifier):
        assert_safe_identifier(bad)


def test_safe_identifier_returned_unchanged():
    assert assert_safe_identifier("ORDER_DIMENSIONS") == "ORDER_DIMENSIONS"
