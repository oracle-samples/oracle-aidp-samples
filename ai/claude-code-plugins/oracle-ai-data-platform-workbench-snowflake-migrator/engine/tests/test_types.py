"""Snowflake -> Spark/Delta type mapping. Pure; no connection."""
import pytest

from snowflake_source.dialect.types import TypeMapping, map_type


@pytest.mark.parametrize("dt", ["NUMBER", "DECIMAL", "NUMERIC"])
def test_number_preserves_precision_and_scale(dt):
    m = map_type(dt, precision=18, scale=2)
    assert m.spark_type == "DECIMAL(18,2)"
    assert m.blocked is False


def test_number_scale_zero_is_still_decimal():
    # NUMBER(38,0) is Snowflake's default integer. Mapping it to BIGINT would
    # overflow: 38 digits does not fit in 64 bits.
    assert map_type("NUMBER", precision=38, scale=0).spark_type == "DECIMAL(38,0)"


def test_number_without_precision_is_blocked_not_guessed():
    m = map_type("NUMBER")
    assert m.blocked is True
    assert "precision" in m.reason.lower()


def test_timestamp_ntz_maps_to_ntz_not_timestamp():
    # Spark's bare TIMESTAMP is session-timezone-dependent; NTZ is not.
    m = map_type("TIMESTAMP_NTZ")
    assert m.spark_type == "TIMESTAMP_NTZ"
    assert m.warning is None


@pytest.mark.parametrize("dt", ["TIMESTAMP_LTZ", "TIMESTAMP_TZ"])
def test_zoned_timestamps_map_with_a_warning(dt):
    m = map_type(dt)
    assert m.spark_type == "TIMESTAMP"
    assert m.warning is not None and "timezone" in m.warning.lower()


@pytest.mark.parametrize("dt,expected", [
    ("TEXT", "STRING"), ("VARCHAR", "STRING"), ("CHAR", "STRING"),
    ("BOOLEAN", "BOOLEAN"), ("DATE", "DATE"), ("BINARY", "BINARY"),
    ("FLOAT", "DOUBLE"), ("DOUBLE", "DOUBLE"),
])
def test_direct_mappings(dt, expected):
    assert map_type(dt).spark_type == expected


def test_varchar_length_is_recorded_as_a_warning():
    m = map_type("TEXT", char_length=100)
    assert m.spark_type == "STRING"
    assert "100" in m.warning


@pytest.mark.parametrize("dt", ["VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY"])
def test_semistructured_and_geo_are_blocked(dt):
    m = map_type(dt)
    assert m.blocked is True
    assert m.spark_type is None
    assert m.reason


def test_unknown_type_is_blocked_never_defaulted():
    m = map_type("SOME_FUTURE_TYPE")
    assert m.blocked is True
    assert "unmapped" in m.reason.lower()


def test_case_and_whitespace_insensitive():
    assert map_type("  number ", precision=5, scale=2).spark_type == "DECIMAL(5,2)"


def test_mapping_is_immutable():
    with pytest.raises(Exception):
        map_type("DATE").spark_type = "STRING"


# ==========================================================================
# Semi-structured escape hatch and silent lossy mappings.
# ==========================================================================

def test_variant_is_still_blocked_by_default():
    # Default deny stays the default: an unexamined VARIANT becomes a typed
    # design decision, not a guess.
    assert map_type("VARIANT").blocked


def test_variant_can_be_carried_as_a_string_when_asked():
    # Without this, ONE VARIANT column blocks its whole table and there is no
    # way to proceed -- which on a real Snowflake estate blocks most tables.
    m = map_type("VARIANT", semi_structured="string")
    assert not m.blocked
    assert m.spark_type == "STRING"
    assert "json" in m.warning.lower()
    assert "not a typed" in m.warning.lower() or "defer" in m.warning.lower()


def test_object_and_array_follow_the_same_switch():
    for t in ("OBJECT", "ARRAY"):
        assert map_type(t).blocked
        assert map_type(t, semi_structured="string").spark_type == "STRING"


def test_geospatial_has_its_own_switch_and_is_blocked_by_default():
    assert map_type("GEOGRAPHY").blocked
    assert map_type("GEOMETRY").blocked
    m = map_type("GEOGRAPHY", geospatial="string")
    assert m.spark_type == "STRING"
    assert "wkt" in m.warning.lower() or "text" in m.warning.lower()


def test_semi_structured_switch_does_not_unblock_geospatial():
    # Two separate decisions. Carrying JSON as text is not the same call as
    # carrying a geography as text.
    assert map_type("GEOGRAPHY", semi_structured="string").blocked


def test_unknown_switch_values_are_refused():
    with pytest.raises(ValueError):
        map_type("VARIANT", semi_structured="maybe")
    with pytest.raises(ValueError):
        map_type("GEOGRAPHY", geospatial="maybe")


def test_an_unmapped_type_is_still_blocked_whatever_the_switches():
    assert map_type("VECTOR", semi_structured="string", geospatial="string").blocked


def test_time_to_string_carries_a_warning():
    # It used to map silently. TIME -> STRING changes ordering and comparison
    # semantics, which is exactly the kind of thing that must not be silent.
    m = map_type("TIME")
    assert m.spark_type == "STRING"
    assert m.warning and "TIME" in m.warning


def test_integer_to_decimal_is_noted_but_does_not_raise_risk():
    # Every Snowflake integer is NUMBER(38,0), so DECIMAL(38,0) is faithful --
    # but it is a surprise in Spark and deserves saying once. As a NOTE, not a
    # warning: a warning on every integer column would mark every table MEDIUM
    # and drown the warnings that matter.
    m = map_type("BIGINT", precision=38, scale=0)
    assert m.spark_type == "DECIMAL(38,0)"
    assert m.warning is None
    assert m.note and "DECIMAL" in m.note


def test_number_with_a_scale_gets_no_integer_note():
    assert map_type("NUMBER", precision=10, scale=2).note is None


# ==========================================================================
# TIMESTAMP_NTZ is a TRANSLATION decision, so it belongs here.
#
# It was first handled in the catalog transport, which was the wrong place:
# the translator owns what a Snowflake type becomes, and the transport should
# only ever refuse what it genuinely cannot express. Deciding it here means
# the PLAN shows the type that will really be created.
# ==========================================================================

def test_timestamp_ntz_is_preserved_by_default():
    m = map_type("TIMESTAMP_NTZ")
    assert m.spark_type == "TIMESTAMP_NTZ"
    assert m.warning is None


def test_timestamp_ntz_can_be_downgraded_by_the_translator():
    m = map_type("TIMESTAMP_NTZ", timestamp_ntz="timestamp")
    assert m.spark_type == "TIMESTAMP"
    assert "timezone" in m.warning.lower()
    assert "session" in m.warning.lower()


def test_the_downgrade_says_why_it_was_needed():
    m = map_type("TIMESTAMP_NTZ", timestamp_ntz="timestamp")
    assert "catalog" in m.warning.lower() or "target" in m.warning.lower()


def test_an_unknown_timestamp_mode_is_refused():
    with pytest.raises(ValueError):
        map_type("TIMESTAMP_NTZ", timestamp_ntz="maybe")


def test_the_mode_does_not_touch_other_timestamp_types():
    for t in ("TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"):
        assert map_type(t, timestamp_ntz="timestamp").spark_type == "TIMESTAMP"
