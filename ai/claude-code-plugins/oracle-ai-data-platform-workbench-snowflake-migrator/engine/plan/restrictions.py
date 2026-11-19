"""User-supplied migration restrictions. Pure, zero I/O.

Restrictions narrow the estate before planning: exclude a database, cap object
size, skip a naming convention, drop a whole object type. Every exclusion records
WHICH restriction fired and a brief reason, because the plan has to explain what
cannot be migrated and why -- an object that silently vanished from the plan is
indistinguishable from one that was never there.

An unrecognised restriction key is an ERROR, not an ignored line. A typo'd key
would apply nothing while appearing to succeed. The same goes for values: an
empty list, a blank pattern, a non-string entry, a negative cap, a JSON boolean
where an integer belongs, or an object type outside TABLE/VIEW is rejected
rather than applied as a no-op that the report then lists as "in force".

A cap that cannot be evaluated excludes the object, with that as the reason.
A table whose count was never captured (`--row-counts none`, a count that
errored, a view under the default metadata mode, a manifest without sizes) is
not known to be under `max_rows`, and keeping it would be a guess; the
exclusion is listed in PLANNED_OBJECTS.md like any other, so the operator can
drop the cap or count exactly and re-plan.

`include_objects` / `exclude_objects` entries follow Snowflake's own case rule:
an unquoted part folds to upper (`d.s.orders` is D.S.ORDERS), a double-quoted
part is case-sensitive (`"D"."S"."orders"` is only the lower-case object). That
is what lets a restriction resolve an identifier-case collision -- the plan
HALTs on ORDERS vs "orders" rather than guessing, and the operator defers one
twin by spelling it exactly. An unquoted entry matches every case-variant, as
it always did; its exclusion reason names the entry and says the match was
case-insensitive, and only a quoted, exact hit is reported as the operator's
explicit choice, so the report never blames a twin the operator did not name
on the operator. Database and schema entries follow the same rule: they
upper-cased the entry WITH its quote characters, so `"sales_eu"` -- the
quoted form the collision remedy teaches -- could never match anything.

A value can be well-formed and still match nothing -- a typo in an object
name. That is not rejected (another estate may hold it), but it is not
silent either: `restriction_matches` counts what each list entry matched,
the plan records the counts, and PLANNED_OBJECTS.md flags a zero instead of
listing the entry as in force like any other.
"""
from __future__ import annotations

import re

__all__ = ["InvalidRestriction", "OBJECT_TYPES", "SCHEMA", "apply_restrictions",
           "restriction_matches", "validate_restrictions"]


class InvalidRestriction(ValueError):
    """A restriction key is unknown, or its value has the wrong type."""


# key -> expected python type
SCHEMA: dict[str, type] = {
    "include_databases": list,
    "exclude_databases": list,
    "include_schemas": list,
    "exclude_schemas": list,
    "include_object_types": list,
    "exclude_object_types": list,
    "include_objects": list,
    "exclude_objects": list,
    "include_name_patterns": list,
    "exclude_name_patterns": list,
    "max_rows": int,
    "max_bytes": int,
}

# The only kinds the inventory produces and the plan decides on.
OBJECT_TYPES = ("TABLE", "VIEW")


def validate_restrictions(restrictions: dict | None) -> dict:
    if not restrictions:
        return {}
    for key, value in restrictions.items():
        if key not in SCHEMA:
            raise InvalidRestriction(
                f"unknown restriction {key!r}; expected one of {sorted(SCHEMA)}")
        expected = SCHEMA[key]
        if expected is list:
            if not isinstance(value, list):
                raise InvalidRestriction(f"{key!r} must be a list, got "
                                         f"{type(value).__name__}")
            if not value:
                what = ("an empty allowlist admits nothing or everything -- "
                        "say which" if key.startswith("include_")
                        else "an empty denylist excludes nothing")
                raise InvalidRestriction(f"{key!r} is an empty list; {what}. "
                                         "Omit the key or list entries")
            for entry in value:
                if not isinstance(entry, str) or not entry.strip():
                    raise InvalidRestriction(f"{key!r} entries must be non-empty "
                                             f"strings, got {entry!r}")
        if expected is int:
            # bool is an int subclass, so JSON true would otherwise pass and
            # then compare as 1.
            if isinstance(value, bool) or not isinstance(value, int):
                raise InvalidRestriction(f"{key!r} must be an integer, got "
                                         f"{type(value).__name__}")
            if value < 0:
                raise InvalidRestriction(f"{key!r} must be >= 0, got {value}")
    for key in ("include_object_types", "exclude_object_types"):
        for entry in restrictions.get(key) or []:
            if entry.upper() not in OBJECT_TYPES:
                raise InvalidRestriction(
                    f"{key} entry {entry!r} is not an object type this plan "
                    f"knows; expected one of {list(OBJECT_TYPES)}")
    for key in ("include_name_patterns", "exclude_name_patterns"):
        for pattern in restrictions.get(key) or []:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise InvalidRestriction(
                    f"{key} entry {pattern!r} is not a valid regex: {exc}") from exc
    return restrictions


def _upper(values) -> set[str]:
    return {str(v).upper() for v in values or []}


def _name_matcher(entries):
    """value -> the entry that matches it, or None, for one name part.

    Snowflake's rule, as for exclude_objects: `"sales_eu"` is exactly
    sales_eu, an unquoted entry folds to upper on both sides.
    """
    exact: dict[str, str] = {}
    folded: dict[str, str] = {}
    for entry in entries or []:
        text = str(entry)
        stripped = text.strip()
        if len(stripped) >= 2 and stripped[0] == stripped[-1] == '"':
            exact[stripped[1:-1]] = text
        else:
            folded[text.upper()] = text

    def match(value: str):
        return exact.get(value) or folded.get(str(value).upper())

    return match


def _split_quoted(entry: str) -> list[tuple[str, bool]]:
    """`d.s."Orders"` -> [("d", False), ("s", False), ("Orders", True)].

    A doubled quote inside a quoted part is not unescaped: assert_safe_identifier
    forbids `"` in a name, so no inventory object needs one.
    """
    parts: list[tuple[str, bool]] = []
    buf: list[str] = []
    in_quotes = was_quoted = False
    for ch in entry:
        if ch == '"':
            in_quotes = not in_quotes
            was_quoted = True
        elif ch == "." and not in_quotes:
            parts.append(("".join(buf), was_quoted))
            buf, was_quoted = [], False
        else:
            buf.append(ch)
    parts.append(("".join(buf), was_quoted))
    return parts


def _object_matcher(entries):
    """Build ident -> (entry, how) | None for include_objects/exclude_objects.

    An entry with no double quote is folded to upper on both sides, as before.
    An entry with quotes is compared part by part: a quoted part must match
    exactly, an unquoted part folds. `how` is the word the exclusion reason
    uses, so a case-insensitive hit on a collision twin is never reported as
    the operator's explicit choice.
    """
    folded: dict[str, str] = {}
    quoted: list[tuple[str, list[tuple[str, bool]]]] = []
    for entry in entries or []:
        text = str(entry)
        if '"' in text:
            quoted.append((text, _split_quoted(text)))
        else:
            folded[text.upper()] = text

    def match(ident: str):
        hit = folded.get(ident.upper())
        if hit is not None:
            return hit, "case-insensitively"
        parts = ident.split(".")
        for text, spec in quoted:
            if len(spec) == len(parts) and all(
                    (have == want) if exact else (have.upper() == want.upper())
                    for (want, exact), have in zip(spec, parts)):
                return text, "exactly"
        return None

    return match


def _exclusion(rec: dict, restriction: str, reason: str) -> dict:
    return {"source_identifier": rec["source_identifier"],
            "object_type": rec.get("object_type"),
            "restriction": restriction, "reason": reason}


def apply_restrictions(records: list[dict],
                       restrictions: dict | None) -> tuple[list[dict], list[dict]]:
    """Split records into (kept, excluded). Each exclusion carries its reason."""
    r = validate_restrictions(restrictions)
    if not r:
        return list(records), []

    inc_db = _name_matcher(r.get("include_databases"))
    exc_db = _name_matcher(r.get("exclude_databases"))
    inc_sc = _name_matcher(r.get("include_schemas"))
    exc_sc = _name_matcher(r.get("exclude_schemas"))
    has_inc_db, has_inc_sc = bool(r.get("include_databases")), bool(r.get("include_schemas"))
    inc_ty, exc_ty = _upper(r.get("include_object_types")), _upper(r.get("exclude_object_types"))
    match_inc = _object_matcher(r.get("include_objects"))
    match_exc = _object_matcher(r.get("exclude_objects"))
    has_inc_ob = bool(r.get("include_objects"))
    # Case-insensitive, like every sibling restriction. Snowflake
    # upper-cases every unquoted identifier, so `^tmp_` written by hand
    # matched nothing at all in a real estate -- the one restriction that
    # could look right and silently do nothing.
    inc_pat = [re.compile(p, re.IGNORECASE)
               for p in r.get("include_name_patterns") or []]
    exc_pat = [re.compile(p, re.IGNORECASE)
               for p in r.get("exclude_name_patterns") or []]
    max_rows, max_bytes = r.get("max_rows"), r.get("max_bytes")

    kept, excluded = [], []
    for rec in records:
        ident = rec["source_identifier"]
        db = str(rec.get("source_database", ""))
        schema = str(rec.get("source_schema", ""))
        kind = str(rec.get("object_type", "")).upper()
        name = ident.rsplit(".", 1)[-1]

        hit = match_exc(ident)
        if hit:
            entry, how = hit
            # Only a quoted, exact hit is the operator's explicit choice; a
            # folded hit may be a case twin the operator never named.
            reason = (f"explicitly excluded by the user (exclude_objects entry "
                      f"{entry!r})" if how == "exactly" else
                      f"excluded by exclude_objects entry {entry!r}, which "
                      f"matched case-insensitively")
            excluded.append(_exclusion(rec, "exclude_objects", reason))
            continue
        if has_inc_ob and not match_inc(ident):
            excluded.append(_exclusion(rec, "include_objects",
                                       "not in the user's include_objects list"))
            continue
        if exc_db(db):
            excluded.append(_exclusion(rec, "exclude_databases",
                                       f"database {db} excluded by the user"))
            continue
        if has_inc_db and not inc_db(db):
            excluded.append(_exclusion(rec, "include_databases",
                                       f"database {db} is not in the include list"))
            continue
        if exc_sc(schema):
            excluded.append(_exclusion(rec, "exclude_schemas",
                                       f"schema {schema} excluded by the user"))
            continue
        if has_inc_sc and not inc_sc(schema):
            excluded.append(_exclusion(rec, "include_schemas",
                                       f"schema {schema} is not in the include list"))
            continue
        if exc_ty and kind in exc_ty:
            excluded.append(_exclusion(rec, "exclude_object_types",
                                       f"object type {kind} excluded by the user"))
            continue
        if inc_ty and kind not in inc_ty:
            excluded.append(_exclusion(rec, "include_object_types",
                                       f"object type {kind} is not in the include list"))
            continue

        hit = next((p for p in exc_pat if p.search(name)), None)
        if hit:
            excluded.append(_exclusion(rec, "exclude_name_patterns",
                                       f"name matches excluded pattern {hit.pattern!r}"))
            continue
        if inc_pat and not any(p.search(name) for p in inc_pat):
            excluded.append(_exclusion(rec, "include_name_patterns",
                                       "name matches no include pattern"))
            continue

        if max_rows is not None:
            rows = rec.get("row_count_exact")
            if rows is None:
                note = rec.get("row_count_note")
                excluded.append(_exclusion(
                    rec, "max_rows",
                    "row count unknown; max_rows cannot be evaluated"
                    + (f" ({note})" if note else "")))
                continue
            if rows > max_rows:
                excluded.append(_exclusion(
                    rec, "max_rows", f"{rows} rows exceeds max_rows {max_rows}"))
                continue
        if max_bytes is not None:
            # The in-AIDP discovery bridge writes the key upper-case.
            meta = rec.get("source_metadata") or {}
            byts = meta.get("bytes", meta.get("BYTES"))
            if byts is None:
                excluded.append(_exclusion(
                    rec, "max_bytes",
                    "byte size unknown; max_bytes cannot be evaluated"))
                continue
            if byts > max_bytes:
                excluded.append(_exclusion(
                    rec, "max_bytes",
                    f"{byts} bytes exceeds max_bytes {max_bytes}"))
                continue

        kept.append(rec)
    return kept, excluded


def restriction_matches(records: list[dict],
                        restrictions: dict | None) -> dict[str, dict[str, int]]:
    """{key: {entry: objects it matched}} for every list-valued restriction.

    Counted against every record, independent of the order the rules fire
    in, so an entry shadowed by an earlier rule still counts what it names.
    The caps are left out: a cap that excludes nothing is a result, not a
    spelling mistake.
    """
    r = validate_restrictions(restrictions)
    out: dict[str, dict[str, int]] = {}
    for key, value in r.items():
        if SCHEMA[key] is not list:
            continue
        counts = {}
        for entry in value:
            if key.endswith("_databases"):
                m = _name_matcher([entry])
                hit = lambda rec: m(str(rec.get("source_database", "")))
            elif key.endswith("_schemas"):
                m = _name_matcher([entry])
                hit = lambda rec: m(str(rec.get("source_schema", "")))
            elif key.endswith("_object_types"):
                hit = lambda rec: (str(rec.get("object_type", "")).upper()
                                   == entry.upper())
            elif key.endswith("_objects"):
                m = _object_matcher([entry])
                hit = lambda rec: m(rec["source_identifier"])
            else:
                pattern = re.compile(entry, re.IGNORECASE)
                hit = lambda rec: pattern.search(
                    rec["source_identifier"].rsplit(".", 1)[-1])
            counts[entry] = sum(1 for rec in records if hit(rec))
        out[key] = counts
    return out
