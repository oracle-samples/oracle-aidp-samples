"""Pure core for per-column bronze read-schema patches (feature
bronze-extract-schema-patch).

A PVO column whose connector-produced runtime value type mismatches the
connector's own declared schema (live case: ItemExtractPVO's
``ItemBasePEOMaterialCost`` declared ``decimal(38,0)``, values
``java.lang.Long``) makes EVERY encode-forcing operation fail. The repair
is a READ-side schema patch (the connector honors user-supplied schemas —
live-verified) followed by a cast back to the declared type, guarded
against silent cast corruption.

This module is the schema-layer pure core: no Spark import, laptop-safe
(dispatch import boundary), 100% unit-testable. The cluster-side adapter
converts ``StructType`` ⇄ :class:`FieldDescriptor` at its boundary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

AIDPF_2093_CONNECTOR_TYPE_MISMATCH = "AIDPF-2093"
"""The connector produced row values that mismatch its OWN declared column
type (live case: ItemExtractPVO ``ItemBasePEOMaterialCost`` declared
decimal(38,0), values java.lang.Long) — every encode-forcing operation
(cache/write) fails while pruned counts pass. Remediation: a per-column
``schemaPatches`` entry (read-side repair)."""

AIDPF_2094_CAST_INTEGRITY = "AIDPF-2094"
"""A schemaPatches cast-back changed data (null introduced / round-trip
mismatch) — Spark ``cast`` is permissive (overflow → NULL, fractional →
rounding, malformed → NULL; live-verified), so a wrong-but-scannable patch
type MUST fail the node before the write, never land silently."""

ENCODE_FAILURE_SIGNATURE = "is not a valid external type for schema of"
"""The Spark encode-failure marker (RowEncoder external-type validation)."""


def full_exception_text(exc: BaseException, *, max_depth: int = 8) -> str:
    """Assemble the FULL exception text from the cause chain.

    ``str(exc)`` alone can carry only ``An error occurred while calling
    o<N>.count.`` while the encode signature lives in the Java cause —
    so this walks ``__cause__``/``__context__`` (bounded, cycle-safe) and
    includes Py4J's ``java_exception`` string when the attribute exists.
    """
    parts: list[str] = []
    seen: set[int] = set()
    cur: BaseException | None = exc
    depth = 0
    while cur is not None and id(cur) not in seen and depth < max_depth:
        seen.add(id(cur))
        depth += 1
        try:
            parts.append(str(cur))
        except Exception:  # noqa: BLE001 — a broken __str__ must not mask
            parts.append(repr(cur))
        java_exc = getattr(cur, "java_exception", None)
        if java_exc is not None:
            try:
                parts.append(str(java_exc))
            except Exception:  # noqa: BLE001
                pass
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return "\n".join(parts)


def classify_bronze_extract_error(text: str) -> bool:
    """True when the assembled exception text carries the encode-failure
    signature (AIDPF-2093 class)."""
    return ENCODE_FAILURE_SIGNATURE in (text or "")


def encode_failure_hint(dataset_id: str) -> str:
    """The operator-facing AIDPF-2093 hint. PREPENDED to the failure
    message — the RUN VERDICT renders only the first line, so an appended
    hint would never surface."""
    return (
        f"{AIDPF_2093_CONNECTOR_TYPE_MISMATCH}: connector value/type "
        f"mismatch — a produced value does not match the connector's own "
        f"declared column type. Run `bronze diagnose-encode --dataset "
        f"{dataset_id}` to name the column, then add a `schemaPatches` "
        f"entry for it on the bundle dataset (read-side repair; declared "
        f"types restored + integrity-guarded at landing)."
    )


_SIMPLE_PATCH_TYPES = frozenset(
    (
        "bigint",
        "long",
        "int",
        "integer",
        "double",
        "float",
        "string",
        "boolean",
        "date",
        "timestamp",
    )
)
_DECIMAL_RE = re.compile(r"^decimal\((\d{1,2}),\s?(\d{1,2})\)$")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class SchemaPatchError(ValueError):
    """Fail-closed validation / application error for schemaPatches."""


class CastIntegrityError(RuntimeError):
    """§5a guard violation (AIDPF-2094): a cast-back changed data. Raised
    BEFORE the write — a patched landing never persists corrupted values."""


def validate_patch_type(type_str: str) -> str:
    """Validate one patch type string; returns the normalized (lowercased)
    form. Decimal is validated SEMANTICALLY: precision 1–38 and
    0 <= scale <= precision — ``decimal(99,99)`` / ``decimal(10,11)`` must
    fail at bundle load, never on the cluster."""
    norm = type_str.strip().lower()
    if norm in _SIMPLE_PATCH_TYPES:
        return norm
    m = _DECIMAL_RE.match(norm)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        if not 1 <= precision <= 38:
            raise SchemaPatchError(
                f"schemaPatches type {type_str!r}: decimal precision must be "
                f"1–38, got {precision}."
            )
        if not 0 <= scale <= precision:
            raise SchemaPatchError(
                f"schemaPatches type {type_str!r}: decimal scale must be "
                f"0–precision ({precision}), got {scale}."
            )
        return f"decimal({precision},{scale})"
    raise SchemaPatchError(
        f"schemaPatches type {type_str!r} is not an allowed Spark type. "
        f"Allowed: {sorted(_SIMPLE_PATCH_TYPES)} or decimal(p,s) with "
        f"1<=p<=38, 0<=s<=p."
    )


def validate_schema_patches(patches: Mapping[str, str]) -> dict[str, str]:
    """Validate a full ``schemaPatches`` map (bundle-load, fail-closed).

    - keys must satisfy the SQL-identifier allowlist;
    - keys duplicated after case-folding are rejected (both would target
      one Spark field case-insensitively — order-dependent otherwise);
    - values go through :func:`validate_patch_type`.

    Returns a new dict with normalized type strings (keys verbatim).
    """
    seen_folded: dict[str, str] = {}
    out: dict[str, str] = {}
    for col, type_str in patches.items():
        if not isinstance(col, str) or not _IDENT_RE.match(col):
            raise SchemaPatchError(
                f"schemaPatches column {col!r} fails the SQL-identifier "
                f"allowlist `^[A-Za-z_][A-Za-z0-9_]{{0,62}}$`."
            )
        folded = col.casefold()
        if folded in seen_folded:
            raise SchemaPatchError(
                f"schemaPatches keys {seen_folded[folded]!r} and {col!r} "
                f"collide case-insensitively — they would target the same "
                f"Spark field; keep exactly one."
            )
        seen_folded[folded] = col
        out[col] = validate_patch_type(str(type_str))
    return out


@dataclass(frozen=True)
class FieldDescriptor:
    """Complete StructField mirror — dataType as DDL, everything else
    verbatim. Rebuilding from (name, type) pairs alone would DROP
    ``nullable`` and ``metadata``; only ``ddl_type`` may ever change."""

    name: str
    ddl_type: str
    nullable: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CastBackEntry:
    """One patched column: read as ``patch_type``, cast back to
    ``declared_type`` before landing; both drive the integrity guard."""

    column: str
    patch_type: str
    declared_type: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    """The base field's metadata, restored in-flight via ``withMetadata``
    (live-verified to survive the atomic overwrite write)."""


def apply_schema_patches(
    base_fields: Sequence[FieldDescriptor],
    patches: Mapping[str, str],
) -> tuple[list[FieldDescriptor], list[CastBackEntry]]:
    """Return ``(patched field list, cast-back plan)``.

    - ONLY ``ddl_type`` of matched fields changes; ``name`` casing,
      ``nullable`` and ``metadata`` pass through untouched;
    - a patch naming a column absent from ``base_fields`` raises
      :class:`SchemaPatchError` (never silently ignored);
    - a patch equal to the declared type is a NO-OP dropped from BOTH
      outputs (idempotent config; also keeps provenance honest — no-ops
      must never be reported as applied);
    - name matching is case-insensitive (Spark semantics); output keeps
      the connector's original casing.
    """
    normalized = validate_schema_patches(patches)
    by_folded = {c.casefold(): (c, t) for c, t in normalized.items()}

    patched: list[FieldDescriptor] = []
    cast_back: list[CastBackEntry] = []
    matched: set[str] = set()
    for fd in base_fields:
        hit = by_folded.get(fd.name.casefold())
        if hit is None:
            patched.append(fd)
            continue
        matched.add(hit[0])
        patch_type = hit[1]
        declared = fd.ddl_type.strip().lower()
        if patch_type == declared:
            patched.append(fd)  # no-op patch: dropped from the plan
            continue
        patched.append(
            FieldDescriptor(
                name=fd.name,
                ddl_type=patch_type,
                nullable=fd.nullable,
                metadata=fd.metadata,
            )
        )
        cast_back.append(
            CastBackEntry(
                column=fd.name,
                patch_type=patch_type,
                declared_type=fd.ddl_type,
                metadata=fd.metadata,
            )
        )
    unknown = sorted(set(normalized) - matched)
    if unknown:
        raise SchemaPatchError(
            f"schemaPatches name column(s) {unknown!r} that the connector "
            f"schema does not contain — fix the patch (column names are "
            f"matched case-insensitively against the live PVO schema)."
        )
    return patched, cast_back


def collision_free_temp_names(
    base_field_names: Sequence[str],
    count: int,
    *,
    prefix: str = "__patch_raw_",
) -> list[str]:
    """Generate ``count`` temp column names PROVEN absent (case-insensitive)
    from ``base_field_names`` — a PVO owning a ``__patch_raw_0``-style
    column must never produce ambiguous resolution."""
    taken = {n.casefold() for n in base_field_names}
    out: list[str] = []
    i = 0
    while len(out) < count:
        candidate = f"{prefix}{i}"
        i += 1
        if candidate.casefold() in taken:
            continue
        taken.add(candidate.casefold())
        out.append(candidate)
    return out


__all__ = [
    "AIDPF_2093_CONNECTOR_TYPE_MISMATCH",
    "AIDPF_2094_CAST_INTEGRITY",
    "ENCODE_FAILURE_SIGNATURE",
    "CastBackEntry",
    "CastIntegrityError",
    "FieldDescriptor",
    "SchemaPatchError",
    "apply_schema_patches",
    "classify_bronze_extract_error",
    "collision_free_temp_names",
    "encode_failure_hint",
    "full_exception_text",
    "validate_patch_type",
    "validate_schema_patches",
]
