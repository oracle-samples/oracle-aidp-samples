"""Duration parsing — ISO 8601 only.

Supported forms: ``P7D``, ``PT1H``, ``P1W``, ``P2DT12H``,
``PT30M``. Months and years (``PnM`` / ``PnY``) are intentionally
not supported because they're variable-width and would force
calendar arithmetic into history-read paths.

The legacy compact form (``"7D"`` / ``"1H"`` / ``"1D4H"``) is no
longer accepted. Operators must use ISO 8601 throughout. This is
deliberate: the library is under active development and we don't
keep two parsers when one suffices.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta  # noqa: F401  (datetime used in helper)

from qualifire.core.exceptions import QualifireConfigError

# ISO 8601 duration: P[nW][nD][T[nH][nM][nS]]. Months/years deliberately not
# supported — they're variable-width and would force calendar arithmetic into
# every history-read path. Use weeks/days/hours instead.
_ISO_DURATION_PATTERN = re.compile(
    r"^P"
    r"(?:(\d+)W)?"
    r"(?:(\d+)D)?"
    r"(?:T"
    r"(?:(\d+)H)?"
    r"(?:(\d+)M)?"
    r"(?:(\d+)S)?"
    r")?$",
    re.IGNORECASE,
)


def parse_duration(value: str | timedelta) -> timedelta:
    """Parse an ISO 8601 duration string into a timedelta.

    Supported forms: ``P7D``, ``PT1H``, ``P1W``, ``PT30M``,
    ``P2DT12H``. Months / years (``PnM`` / ``PnY``) are rejected.

    If already a timedelta, returns it unchanged.

    Raises:
        QualifireConfigError: empty, missing leading ``P``, or any
            other invalid form (including the legacy compact form
            ``"7D"`` / ``"1H"`` — operators must use ISO 8601).
    """
    if isinstance(value, timedelta):
        return value

    if not isinstance(value, str):
        raise QualifireConfigError(
            f"Duration must be a string or timedelta, got "
            f"{type(value).__name__}: {value!r}"
        )

    value = value.strip()
    if not value:
        raise QualifireConfigError("Duration string cannot be empty")

    if value[:1].upper() != "P":
        raise QualifireConfigError(
            f"Duration '{value}' must use ISO 8601 form starting with "
            "'P' — e.g. 'P7D', 'PT1H', 'P1W', 'P2DT12H'. The legacy "
            "compact form is no longer supported; convert by prefixing "
            "with 'P' for date components or 'PT' for time components."
        )

    m = _ISO_DURATION_PATTERN.match(value)
    if not m:
        raise QualifireConfigError(
            f"Invalid ISO 8601 duration format: '{value}'. Expected "
            "forms like 'P7D', 'PT1H', 'P1W', 'P2DT12H'. Months / "
            "years (PnM / PnY) are not supported — use weeks / days."
        )
    weeks_s, days_s, hours_s, minutes_s, seconds_s = m.groups()
    if not any((weeks_s, days_s, hours_s, minutes_s, seconds_s)):
        raise QualifireConfigError(
            f"ISO 8601 duration '{value}' has no components. "
            "Specify at least one unit, e.g. 'P7D' or 'PT1H'."
        )
    return timedelta(
        weeks=int(weeks_s or 0),
        days=int(days_s or 0),
        hours=int(hours_s or 0),
        minutes=int(minutes_s or 0),
        seconds=int(seconds_s or 0),
    )


def partition_lookback_anchors(
    anchor: "datetime", count: int, step: str | timedelta,
) -> list["datetime"]:
    """Return the list of partition timestamps to look up for a partition-
    anchored historical read.

    For ``anchor=2026-04-28`` (datetime), ``count=3``, ``step="P7D"`` →
    ``[2026-04-21, 2026-04-14, 2026-04-07]``.

    Returns datetimes ordered most-recent-first. Caller is responsible
    for serializing them to ISO-8601 if persisted as strings.
    """
    from datetime import datetime as _dt  # local to avoid circular at module load

    if not isinstance(anchor, _dt):
        raise QualifireConfigError(
            f"partition_lookback_anchors: anchor must be a datetime, got {type(anchor).__name__}"
        )
    if count <= 0:
        return []
    td = parse_duration(step)
    if td.total_seconds() <= 0:
        raise QualifireConfigError(
            f"step must be positive, got '{step}' ({td.total_seconds()}s)"
        )
    return [anchor - td * k for k in range(1, count + 1)]


def parse_step_seconds(value: str | timedelta | None) -> int | None:
    """Parse a ``step`` value into positive integer seconds.

    Returns ``None`` when ``value`` is ``None``. Notebook authors
    calling ``read_metric_history(step=None)`` directly use this to
    request raw, un-bucketed history (one row per persisted record)
    — the validators always pass an explicit step so they never hit
    this path. Any other invalid input (empty string, ``0``,
    negative duration) raises ``QualifireConfigError``; storage-layer
    bucketing SQL assumes the bucket width is a strictly positive
    integer.
    """
    if value is None:
        return None

    td = parse_duration(value)
    total = int(td.total_seconds())
    if total <= 0:
        raise QualifireConfigError(
            f"Duration must be positive for step bucketing, got '{value}' "
            f"({total}s). Use a non-zero ISO 8601 interval like 'P1D' or 'P7D'."
        )
    return total


def format_duration_iso(td: timedelta) -> str:
    """Format a timedelta as an ISO 8601 duration string ("P1DT2H30M15S").

    Used by SLO/recency to surface freshness in a column-friendly form
    (`actual_value_text`) alongside the numeric seconds in `metric_value`.
    Returns ``"PT0S"`` for a zero delta. Negative durations get a leading
    ``-`` (rarely needed — `now - recency_ts` is normally positive).
    """
    total_seconds = int(td.total_seconds())
    if total_seconds == 0:
        return "PT0S"
    if total_seconds < 0:
        return "-" + format_duration_iso(-td)

    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    date_part = f"{days}D" if days else ""
    time_parts = []
    if hours:
        time_parts.append(f"{hours}H")
    if minutes:
        time_parts.append(f"{minutes}M")
    if seconds:
        time_parts.append(f"{seconds}S")
    time_part = ("T" + "".join(time_parts)) if time_parts else ""
    return f"P{date_part}{time_part}"
