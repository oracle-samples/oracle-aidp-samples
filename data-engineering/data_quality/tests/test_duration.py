"""Tests for duration parsing — ISO 8601 only."""

from datetime import timedelta

import pytest

from qualifire.core.duration import format_duration_iso, parse_duration
from qualifire.core.exceptions import QualifireConfigError


class TestParseDurationIso:
    def test_hours(self):
        assert parse_duration("PT4H") == timedelta(hours=4)

    def test_days(self):
        assert parse_duration("P2D") == timedelta(days=2)

    def test_minutes(self):
        assert parse_duration("PT30M") == timedelta(minutes=30)

    def test_weeks(self):
        assert parse_duration("P1W") == timedelta(weeks=1)

    def test_seconds(self):
        assert parse_duration("PT90S") == timedelta(seconds=90)

    def test_compound_days_hours(self):
        assert parse_duration("P1DT4H") == timedelta(days=1, hours=4)

    def test_compound_hours_minutes(self):
        assert parse_duration("PT2H30M") == timedelta(hours=2, minutes=30)

    def test_case_insensitive(self):
        assert parse_duration("pt4h") == timedelta(hours=4)
        assert parse_duration("p2d") == timedelta(days=2)

    def test_passthrough_timedelta(self):
        td = timedelta(hours=3)
        assert parse_duration(td) is td


class TestParseDurationRejects:
    def test_empty_raises(self):
        with pytest.raises(QualifireConfigError, match="cannot be empty"):
            parse_duration("")

    @pytest.mark.parametrize(
        "value",
        # NOTE: these are the rejected legacy literals — keep them
        # without the leading 'P' so the test exercises the rejection
        # path. The repo-wide ISO sweep skips this list because the
        # values aren't quoted in a way the matcher considers
        # convertible.
        ["1D", "7D", "4H", "30M", "1W", "1D4H", "2H30M", "90S"],
    )
    def test_legacy_compact_form_rejected(self, value):
        """Legacy compact form is no longer supported. Operators must
        use ISO 8601. The error message must point at the right form
        so the migration path is obvious."""
        with pytest.raises(QualifireConfigError, match="ISO 8601"):
            parse_duration(value)

    def test_invalid_format(self):
        with pytest.raises(QualifireConfigError, match="ISO 8601"):
            parse_duration("abc")

    def test_iso_with_no_components(self):
        with pytest.raises(QualifireConfigError, match="no components"):
            parse_duration("P")

    def test_iso_months_rejected(self):
        with pytest.raises(QualifireConfigError, match="Invalid ISO 8601"):
            parse_duration("P3M")  # ambiguous: P3M has no T, parses as months

    def test_iso_years_rejected(self):
        with pytest.raises(QualifireConfigError, match="Invalid ISO 8601"):
            parse_duration("P1Y")


class TestFormatDurationIso:
    def test_hours(self):
        assert format_duration_iso(timedelta(hours=4)) == "PT4H"

    def test_days_and_hours(self):
        assert format_duration_iso(timedelta(days=1, hours=4)) == "P1DT4H"

    def test_zero(self):
        assert format_duration_iso(timedelta(0)) == "PT0S"

    def test_weeks_collapse_to_days(self):
        # ISO format flattens weeks to days (14D, not 2W) — by design
        # so the output is unambiguous and trivially comparable.
        assert format_duration_iso(timedelta(weeks=2)) == "P14D"

    def test_complex(self):
        assert (
            format_duration_iso(timedelta(weeks=1, days=2, hours=3, minutes=30))
            == "P9DT3H30M"
        )

    def test_negative(self):
        assert format_duration_iso(timedelta(hours=-2)) == "-PT2H"
