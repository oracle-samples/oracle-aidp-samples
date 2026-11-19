"""Spark ``DESCRIBE TABLE`` wrapper for bootstrap's variation phase.

Probes each declared bronze dataset once, returning a
``{dataset_id: [ColumnInfo, ...]}`` mapping the walker
(:mod:`variation_resolver`) and the fingerprint helper
(:mod:`schema.bronze_fingerprint`) both consume.

The probe is the only Spark-touching seam in this feature — the walker
and the fingerprint algorithm are pure-Python. Tests inject a mock
Spark session whose ``sql("DESCRIBE TABLE ...")`` returns fixture rows.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ..schema.bronze_fingerprint import ColumnInfo

if TYPE_CHECKING:  # pragma: no cover — Spark import-guard
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from ..orchestrator.content_pack import ResolvedPack
    from ..schema.bundle import Bundle


# SQL identifier allowlist. Standard unquoted Spark SQL identifier
# shape: alphanumeric / underscore start, alphanumeric / underscore
# body. No dots (each part of the three-part name is validated
# separately), no whitespace, no shell metacharacters, no quoting
# tokens. Rejects semicolons, backticks, single/double quotes,
# parentheses, dashes — anything that could alter the SQL text
# bootstrap issues. Matches the spirit of the renderer's identifier
# allowlist (``schema.identifier_validation``) — the bootstrap path
# enforces the same invariant before any Spark call.
_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class UnsafeIdentifierError(ValueError):
    """Raised when an SQL identifier (catalog / schema / dataset)
    fails the allowlist regex.

    Without this check the f-string interpolation in
    :func:`describe_bronze` would allow a malformed or malicious bundle
    to inject arbitrary SQL into the DESCRIBE TABLE statement
    (semicolons, backticks, whitespace, dotted IDs). Validation
    fails closed BEFORE any Spark call.
    """

    def __init__(self, *, value: str, field: str) -> None:
        self.value = value
        self.field = field
        super().__init__(
            f"unsafe SQL identifier for {field}={value!r}; must match "
            f"^[A-Za-z_][A-Za-z0-9_]*$. No semicolons, backticks, "
            f"whitespace, or dotted IDs."
        )


def _validate_identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise UnsafeIdentifierError(value=str(value), field=field)
    if not _SQL_IDENT_RE.match(value):
        raise UnsafeIdentifierError(value=value, field=field)
    return value


def describe_bronze(
    spark: "SparkSession",
    *,
    catalog: str,
    bronze_schema: str,
    dataset_ids: list[str],
    table_names: "dict[str, str] | None" = None,
) -> dict[str, list[ColumnInfo]]:
    """Run ``DESCRIBE TABLE`` against each bronze dataset and return the
    parsed column metadata.

    Args:
        spark: an active Spark session.
        catalog: e.g. ``"fusion_catalog"``. Validated against the
            SQL-identifier allowlist; rejected with
            :class:`UnsafeIdentifierError` on failure.
        bronze_schema: e.g. ``"bronze"``. Same allowlist.
        dataset_ids: bronze dataset ids declared in the pack's
            ``bronze.yaml`` (e.g. ``["erp_suppliers", "ap_invoices"]``).
            Each id is validated.

    Returns:
        ``{dataset_id: [ColumnInfo, ...]}``. The walker takes a
        ``set[str]`` of column names per dataset; the fingerprint helper
        takes the full ColumnInfo list. Bootstrap (Step 8) feeds both
        from this single probe.

    Raises:
        UnsafeIdentifierError: any of ``catalog`` / ``bronze_schema`` /
            one of ``dataset_ids`` failed the SQL-identifier allowlist.
            Bootstrap fails closed BEFORE issuing the Spark query.
        BronzeProbeFailure: a ``DESCRIBE`` query failed for any dataset.
            Wraps the underlying Spark exception with the dataset id so
            the operator knows which bronze table was unreachable.
    """
    # The physical bronze table is the node's ``target``, which the pack
    # contract permits to differ from the dataset ``id`` (e.g. node
    # ``gl_journal_lines`` targets table ``gl_journal_headers``). Callers pass
    # ``table_names`` (id -> target); absent an entry we fall back to the id
    # (id == target for most nodes), preserving existing callers' behaviour.
    table_names = table_names or {}

    # Fail-closed identifier validation BEFORE any Spark call.
    _validate_identifier(catalog, field="aidp.catalog")
    _validate_identifier(bronze_schema, field="aidp.bronzeSchema")
    for dataset_id in dataset_ids:
        _validate_identifier(dataset_id, field="bronze.dataset.id")
        _validate_identifier(table_names.get(dataset_id, dataset_id), field="bronze.dataset.target")

    out: dict[str, list[ColumnInfo]] = {}
    for dataset_id in dataset_ids:
        table = table_names.get(dataset_id, dataset_id)
        fully_qualified = f"{catalog}.{bronze_schema}.{table}"
        try:
            rows = spark.sql(f"DESCRIBE TABLE {fully_qualified}").collect()
        except Exception as exc:  # noqa: BLE001 — Spark raises a variety of types
            raise BronzeProbeFailure(
                dataset_id=dataset_id,
                fully_qualified=fully_qualified,
                cause=exc,
            ) from exc
        out[dataset_id] = _parse_describe_rows(rows)  # keyed by id, not target
    return out


def bronze_table_absent(
    spark: "SparkSession",
    *,
    catalog: str,
    bronze_schema: str,
    table: str,
) -> bool:
    """Strict, fail-closed check for "this bronze table is not landed yet".

    Returns ``True`` ONLY when the metadata probe fails with a recognized
    *table/view not found* condition. Any other failure — permission
    denied, catalog/metastore unreachable, connector error — is
    **re-raised** (fail closed), so a transient outage is never silently
    misread as "fresh tenant → source-probe fallback".

    Deliberately does NOT use ``spark.catalog.tableExists`` /
    ``bronze_extract_adapter._table_exists``: those swallow *all*
    exceptions and return ``False``, conflating a missing table with an
    auth/catalog failure (Risk R6).

    Identifiers are validated against the same allowlist as
    :func:`describe_bronze` before any Spark call.

    Args:
        spark: an active Spark session.
        catalog / bronze_schema / table: three-part name parts; ``table``
            is the physical ``node.target`` (NOT the dataset id).

    Returns:
        ``True`` if the table is absent (not-found), ``False`` if it
        exists.

    Raises:
        UnsafeIdentifierError: an identifier failed the allowlist.
        Exception: re-raised verbatim for any non-"not found" Spark error.
    """
    _validate_identifier(catalog, field="aidp.catalog")
    _validate_identifier(bronze_schema, field="aidp.bronzeSchema")
    _validate_identifier(table, field="bronze.dataset.target")

    fully_qualified = f"{catalog}.{bronze_schema}.{table}"
    try:
        spark.sql(f"DESCRIBE TABLE {fully_qualified}").take(1)
        return False
    except Exception as exc:  # noqa: BLE001 — classify, then re-raise non-NOTFOUND
        if _is_table_or_view_not_found(exc):
            return True
        raise


def _is_table_or_view_not_found(exc: Exception) -> bool:
    """Classify a Spark exception as a "table or view not found" condition.

    Recognizes (in order of precision):

    * the structured ``AnalysisException`` error class
      ``TABLE_OR_VIEW_NOT_FOUND`` (Spark 3.4+ via ``getErrorClass()``),
    * the bracketed ``[TABLE_OR_VIEW_NOT_FOUND]`` message prefix Spark
      emits for that class,
    * the legacy phrase ``Table or view not found``.

    Anything else returns ``False`` so the caller fails closed. Pure
    classification — no Spark import, so it is unit-testable with fakes.
    """
    get_error_class = getattr(exc, "getErrorClass", None)
    if callable(get_error_class):
        try:
            if get_error_class() == "TABLE_OR_VIEW_NOT_FOUND":
                return True
        except Exception:  # noqa: BLE001 — defensive; fall through to message match
            pass
    message = str(exc).upper()
    return (
        "[TABLE_OR_VIEW_NOT_FOUND]" in message
        or "TABLE OR VIEW NOT FOUND" in message
    )


def describe_bronze_present(
    spark: "SparkSession",
    *,
    catalog: str,
    bronze_schema: str,
    dataset_ids: list[str],
    table_names: dict[str, str] | None = None,
) -> tuple[dict[str, "list[ColumnInfo]"], list[str]]:
    """Absence-tolerant :func:`describe_bronze` (feature:
    bronze-fingerprint-gate-scope).

    Splits ``dataset_ids`` into PRESENT tables (DESCRIBEd, returned in
    ``observed``) and ABSENT tables (TABLE_OR_VIEW_NOT_FOUND, returned as
    ``absent_ids`` — e.g. a dataset whose extract never succeeded, so its
    bronze table was never materialised). One unmaterialised dataset no longer
    aborts the whole probe.

    Security boundary — validation-first: ALL identifiers are validated
    upfront, BEFORE any Spark call or exception-translation boundary exists,
    so :class:`UnsafeIdentifierError` is raised pre-Spark and can never be
    wrapped into a (``--force-fingerprint-skip``-bypassable)
    :class:`BronzeProbeFailure`. Exception taxonomy:

    * :class:`UnsafeIdentifierError` — injection guard; NEVER translated,
      never bypassable.
    * TABLE_OR_VIEW_NOT_FOUND — absence; tolerated (collected).
    * :class:`BronzeProbeFailure` — any OTHER Spark-call failure
      (permissions, broken catalog, transport); fail-closed here, break-glass
      bypassable at the gate exactly as today.

    Returns:
        ``(observed, absent_ids)`` — ``observed`` keyed by dataset id for the
        present tables; ``absent_ids`` in ``dataset_ids`` order.
    """
    table_names = table_names or {}

    # Validation-first: no loop / translation boundary until every identifier
    # has passed the allowlist (same triple describe_bronze enforces).
    _validate_identifier(catalog, field="aidp.catalog")
    _validate_identifier(bronze_schema, field="aidp.bronzeSchema")
    for dataset_id in dataset_ids:
        _validate_identifier(dataset_id, field="bronze.dataset.id")
        _validate_identifier(
            table_names.get(dataset_id, dataset_id), field="bronze.dataset.target"
        )

    absent_ids: list[str] = []
    present_ids: list[str] = []
    for dataset_id in dataset_ids:
        table = table_names.get(dataset_id, dataset_id)
        try:
            is_absent = bronze_table_absent(
                spark, catalog=catalog, bronze_schema=bronze_schema, table=table
            )
        except UnsafeIdentifierError:
            # Defense in depth: unreachable (validated above), but the guard
            # must never enter the BronzeProbeFailure translation below.
            raise
        except Exception as exc:  # noqa: BLE001 — Spark-call failures only
            raise BronzeProbeFailure(
                dataset_id=dataset_id,
                fully_qualified=f"{catalog}.{bronze_schema}.{table}",
                cause=exc,
            ) from exc
        (absent_ids if is_absent else present_ids).append(dataset_id)

    observed: dict[str, list[ColumnInfo]] = {}
    if present_ids:
        observed = describe_bronze(
            spark,
            catalog=catalog,
            bronze_schema=bronze_schema,
            dataset_ids=present_ids,
            table_names=table_names,
        )
    return observed, absent_ids


def describe_bronze_from_source(
    spark: "SparkSession",
    *,
    pack: "ResolvedPack",
    bundle: "Bundle",
    resolved_password: str,
    dataset_ids: "list[str] | None" = None,
) -> dict[str, list[ColumnInfo]]:
    """Build the same ``{dataset_id: [ColumnInfo, ...]}`` mapping as
    :func:`describe_bronze`, but from the **live BICC PVO source schema**
    via a metadata-only ``inferSchema`` probe — for the fresh-tenant case
    where the lakehouse bronze table is not yet landed.

    Delegates the BICC roundtrip to
    :func:`orchestrator.builtins.bronze_extract_adapter.probe_bronze_schemas`,
    which already keys by node id and builds its descriptor from
    ``node.target`` — so the pack-id-vs-physical-table divergence (node
    ``gl_journal_lines`` targets table ``gl_journal_headers``) is honored
    natively, identically to the landed ``table_names`` path.

    Args:
        spark: an active Spark session (cluster-side for default dispatch).
        pack: assembled ``ResolvedPack``; scope is ``pack.bronze``.
        bundle: bundle supplying ``fusion.service_url`` / ``username`` /
            ``external_storage`` (rendered) for the BICC connector.
        resolved_password: BICC password value, already resolved by the
            caller via ``_resolve_password`` (NOT a ``${...}`` placeholder).
        dataset_ids: optional subset filter; ``None`` probes every bronze
            node in ``pack.bronze``.

    Returns:
        ``{dataset_id: [ColumnInfo, ...]}`` keyed by node id. Audit columns
        are NOT present in the source PVO; the fingerprint helper strips
        them on the landed side, so the two producers agree (Risk R1 parity
        is asserted by a dedicated test).
    """
    # Lazy import — bronze_extract_adapter pulls in the BICC extractor and
    # a wide orchestrator surface; keep bronze_probe import-light for the
    # cluster cell and variation_phase.
    from ..orchestrator.builtins.bronze_extract_adapter import (
        probe_bronze_schemas,
    )

    live_schemas = probe_bronze_schemas(
        spark,
        pack=pack,
        bundle=bundle,
        resolved_password=resolved_password,
        dataset_ids=dataset_ids,
    )
    return {
        dataset_id: _struct_type_to_columns(schema)
        for dataset_id, schema in live_schemas.items()
    }


def _struct_type_to_columns(schema: "StructType") -> list[ColumnInfo]:
    """Convert a Spark ``StructType`` to ``[ColumnInfo]``.

    Uses ``dataType.simpleString()`` so the type string matches the
    ``data_type`` column ``DESCRIBE TABLE`` emits on the landed side
    (e.g. ``"string"`` / ``"bigint"`` / ``"decimal(38,0)"``), keeping the
    canonical fingerprint identical across producers. ``str(dataType)``
    is deliberately NOT used — it yields ``"StringType()"``.
    """
    return [
        ColumnInfo(name=field.name, type=field.dataType.simpleString())
        for field in schema.fields
    ]


def _bronze_scope_and_targets(
    pack: "ResolvedPack",
) -> tuple[list[str], dict[str, str]]:
    """Return ``(dataset_ids, table_names)`` for the pack's bronze scope.

    Mirrors ``variation_phase._bronze_dataset_ids`` + its id→target map
    (replicated here, not imported, to avoid a ``variation_phase`` ↔
    ``bronze_probe`` import cycle). ``pack.bronze`` is the source of truth;
    legacy ``pack.bronze_yaml`` ids are appended with an id==target
    fallback. ``table_names`` carries the **physical** ``node.target`` so a
    node whose id differs from its table (``gl_journal_lines`` →
    ``gl_journal_headers``) is probed correctly and keyed by id.
    """
    dataset_ids: list[str] = list(pack.bronze.keys())
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for entry in bronze_yaml.get("datasets", []) or []:
        if isinstance(entry, dict) and "id" in entry:
            entry_id = str(entry["id"])
            if entry_id not in dataset_ids:
                dataset_ids.append(entry_id)
    table_names = {
        nid: pack.bronze[nid].target
        for nid in dataset_ids
        if nid in pack.bronze and getattr(pack.bronze[nid], "target", None)
    }
    return dataset_ids, table_names


def resolve_observed(
    spark: "SparkSession",
    *,
    catalog: str,
    bronze_schema: str,
    pack: "ResolvedPack",
    bundle: "Bundle",
    resolved_password: str,
) -> dict[str, list[ColumnInfo]]:
    """Build the ``{dataset_id: [ColumnInfo]}`` map both dispatch paths
    consume, selecting per node between the landed and source producers.

    For each in-scope bronze node: if its physical target table is absent
    (strict :func:`bronze_table_absent`), resolve its schema from the live
    BICC source (:func:`describe_bronze_from_source`); otherwise
    ``DESCRIBE`` the landed table (:func:`describe_bronze`). Results merge
    into one map keyed by node id — so a partially-landed tenant gets a
    consistent observation (Risk R2). Both branches honor the physical
    ``node.target`` (Risk R8).

    Only nodes present in ``pack.bronze`` are source-probable; a legacy
    ``bronze_yaml``-only id (no node descriptor) always takes the landed
    path. ``resolved_password`` is the already-resolved BICC secret (NOT a
    ``${...}`` placeholder) — the caller resolves it via
    ``_resolve_password`` (local) or the creds cell (cluster).

    Raises:
        Any non-"not found" Spark error from :func:`bronze_table_absent`
        (fail-closed), or :class:`BronzeProbeFailure` from a landed probe.
    """
    dataset_ids, table_names = _bronze_scope_and_targets(pack)

    present_ids: list[str] = []
    absent_ids: list[str] = []
    for nid in dataset_ids:
        target = table_names.get(nid, nid)
        if nid in pack.bronze and bronze_table_absent(
            spark, catalog=catalog, bronze_schema=bronze_schema, table=target
        ):
            absent_ids.append(nid)
        else:
            present_ids.append(nid)

    observed: dict[str, list[ColumnInfo]] = {}
    if present_ids:
        observed.update(
            describe_bronze(
                spark,
                catalog=catalog,
                bronze_schema=bronze_schema,
                dataset_ids=present_ids,
                table_names=table_names,
            )
        )
    if absent_ids:
        observed.update(
            describe_bronze_from_source(
                spark,
                pack=pack,
                bundle=bundle,
                resolved_password=resolved_password,
                dataset_ids=absent_ids,
            )
        )
    return observed


class BronzeProbeFailure(Exception):
    """Raised when ``DESCRIBE TABLE`` cannot reach a bronze dataset.

    Bootstrap maps this to a remediation message naming the offending
    dataset; the operator's typical cause is a missing bronze schema
    (the pre-onboarding probes should catch this, but the variation
    phase runs after them).
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        fully_qualified: str,
        cause: Exception,
    ) -> None:
        self.dataset_id = dataset_id
        self.fully_qualified = fully_qualified
        self.cause = cause
        super().__init__(
            f"DESCRIBE TABLE failed for {fully_qualified} ({dataset_id}): "
            f"{type(cause).__name__}: {cause}"
        )


def _parse_describe_rows(rows: list) -> list[ColumnInfo]:
    """Convert Spark DESCRIBE TABLE Row objects to ColumnInfo.

    Spark emits these output shapes for DESCRIBE:

    * Standard (Spark 3.x): ``col_name``, ``data_type``, ``comment``.
    * Extended: additional partition / detailed-info rows after a
      ``# col_name`` header. We stop reading at the first ``#``-prefixed
      ``col_name`` so partition / detailed-info rows don't pollute the
      column list.
    * Empty / null ``col_name`` rows separate sections; drop them.
    """
    columns: list[ColumnInfo] = []
    for row in rows:
        col_name = _row_field(row, "col_name", 0)
        data_type = _row_field(row, "data_type", 1)
        if col_name is None or not str(col_name).strip():
            continue
        name = str(col_name)
        if name.startswith("#"):
            # Detailed-info / partition header — everything after this
            # is metadata, not column rows.
            break
        if data_type is None:
            continue
        columns.append(ColumnInfo(name=name, type=str(data_type)))
    return columns


def _row_field(row, name: str, index: int):
    """Read a Row attribute by name (preferred) or positionally.

    Mocked rows in tests are often plain tuples; Spark's real Rows
    expose ``asDict()`` or attribute access. Try attribute first, then
    fall back to positional indexing.
    """
    # Attribute / dict access.
    try:
        return row[name]  # works for Spark Row + dict
    except (KeyError, TypeError, IndexError, AttributeError):
        pass
    try:
        return getattr(row, name)
    except AttributeError:
        pass
    # Positional fallback.
    try:
        return row[index]
    except (IndexError, TypeError, KeyError):
        return None


__all__ = [
    "BronzeProbeFailure",
    "UnsafeIdentifierError",
    "bronze_table_absent",
    "describe_bronze",
    "describe_bronze_from_source",
    "describe_bronze_present",
    "resolve_observed",
]
