"""Informatica date-mask -> Spark/Java date-pattern conversion.

Shared seam: the deterministic compiler
(``engine/infa2aidp/converters/expression_converter.py``) needs this table
to emit ``TO_DATE``/``TO_CHAR`` inline as pure Spark expressions (Class A --
those two functions are pure scalar expressions with no runtime state, so
they stay inlined, never wrapped in a library call). Class C call sites in
``infa_compat`` (``sequence.py``'s cache-restart bookkeeping, generated
notebook cells that stamp SCD2 effective-dates, etc.) need the exact same
mapping when they format a date driver-side rather than emitting a Spark
expression string.

Rather than keep two copies of the token table -- the classic way these
things drift (a unit gets added on one side, forgotten on the other, and
years later someone hits a mapping that formats dates one way in the
inline path and a subtly different way in the library path) -- the table
lives here, once, and ``expression_converter.py`` imports
:func:`to_java_format` ( relocates it there; see that
module's history for the pre-relocation copy).

Spark's ``date_format``/``to_date``/``to_timestamp`` format strings ARE
Java ``DateTimeFormatter`` pattern strings (Spark 3+ uses
``java.time`` under the hood, not the legacy ``SimpleDateFormat``/
``FastDateFormat`` engine) -- so one conversion serves both "give me a
Spark format string" and "give me a Java format string" call sites. That
is why this function is named for the Java pattern rather than
"to_spark_format": it is honest about what the string actually is.
"""
from __future__ import annotations

# Informatica date-format tokens -> Java/Spark DateTimeFormatter tokens.
# Order does not matter for iteration correctness -- ``to_java_format``
# sorts by token length (longest first) so e.g. "MONTH" is replaced before
# "MON" and "HH24" before "HH" ever get a chance to partially match.
DATE_FORMAT_MAP: dict[str, str] = {
    "YYYY": "yyyy",
    "YY": "yy",
    "MM": "MM",
    "DD": "dd",
    "HH24": "HH",
    "HH12": "hh",
    "HH": "HH",
    "MI": "mm",
    "SS": "ss",
    "MS": "SSS",
    "US": "SSSSSS",
    "AM": "a",
    "PM": "a",
    "MON": "MMM",
    "MONTH": "MMMM",
    "DY": "EEE",
    "DAY": "EEEE",
    "D": "u",
    "J": "D",
}


def to_java_format(infa_mask: str) -> str:
    """Convert an Informatica date-mask string to its Java/Spark
    ``DateTimeFormatter`` pattern equivalent.

    Strips a single pair of surrounding quotes (Informatica expression
    literals arrive quoted, e.g. ``'YYYY-MM-DD'``) and then replaces every
    recognized token, longest first, so multi-character tokens are never
    shadowed by a shorter one that is also a substring of it (``"MONTH"``
    vs ``"MON"``, ``"HH24"`` vs ``"HH"``).

    Unrecognized characters (literal punctuation like ``-``/``/``/``:``,
    or a token this table does not know) pass through unchanged -- this
    mirrors the pre-Task-31 inline behavior in
    ``expression_converter.py`` exactly, so relocating it here changes
    nothing observable for any existing mapping.
    """
    fmt = infa_mask.strip("'\"")
    for infa_tok, java_tok in sorted(DATE_FORMAT_MAP.items(), key=lambda kv: -len(kv[0])):
        fmt = fmt.replace(infa_tok, java_tok)
    return fmt
