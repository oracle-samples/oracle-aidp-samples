"""Qualifire exception hierarchy."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qualifire.core.models import QualifireResult


class QualifireReinitWarning(UserWarning):
    """Emitted when a `Qualifire` instance reinitializes its
    system-table storage because a subsequent `run_config` /
    `backfill` / `deactivate_metric` call's YAML disagrees with
    the construction-time configuration on `system_table` /
    `system_table_backend` / `jdbc`.

    The warning fires when the instance was constructed with
    `warn_on_reinit=True` — the default for both
    `Qualifire.from_config(...)` and direct `Qualifire(...)`
    construction (single contract; one consistent default).
    Pass `warn_on_reinit=False` to silence when the swap is
    intentional (e.g. CLI paths where construction and
    execution use the same YAML).
    """


class QualifireError(Exception):
    """Base exception for all Qualifire errors."""


class QualifireConfigError(QualifireError):
    """Raised when configuration is invalid."""


class MissingPartitionAnchorError(QualifireConfigError):
    """Raised by collectors / validators when a partition-anchored
    operation can't proceed because ``partition_ts``, ``step``, or a
    necessary shiftable filter is missing.

    The engine catches this subclass specifically and converts it
    into a structured ``ValidationResult`` with
    ``details.missing_partition_anchor=True``, so shape / pattern /
    drift / forecast all surface the misconfiguration the same way
    in dashboards and alert filters.
    """


class QualifireValidationError(QualifireError):
    """Raised when validation fails with ERROR severity due to
    **data-quality findings** — the operator's data violated one
    or more validator rules (threshold breach, drift outside the
    band, forecast residual, anomaly, pattern shift, etc.).

    Carries the full QualifireResult so callers can inspect
    individual findings.

    Sibling exception:
        :class:`QualifireInternalError` — distinct error category
        for library / infrastructure failures (storage outage,
        validator code threw an unhandled exception, etc.). The
        two errors are deliberately separated so callers can route
        them to different on-call queues:

        .. code-block:: python

            try:
                qf.run_config(yaml_path)
            except QualifireValidationError as e:
                notify_data_team(e)            # data is broken
            except QualifireInternalError as e:
                page_engineering_oncall(e)     # qualifire is broken

        Both inherit from :class:`QualifireError` for catch-all
        purposes, but neither subclasses the other. ``except
        QualifireValidationError`` does NOT catch internal
        failures, and vice versa — that is the contract.
    """

    def __init__(self, message: str, result: QualifireResult | None = None):
        super().__init__(message)
        self.result = result


class QualifireInternalError(QualifireError):
    """Raised when a qualifire run fails due to a **library or
    infrastructure issue** rather than a data-quality finding.

    Two distinct categories surface this exception:

    1. **Persistence-infrastructure outage** — the system table
       backend (SQLite / Delta / ExternalCatalog / JDBC) is
       unreachable or rejected the write. The original storage
       exception is preserved as ``__cause__`` (PEP 3134
       chaining).

    2. **Validator-execution exception** — code inside a
       validator threw an unhandled exception (a library bug,
       a sklearn / pandas / pyspark integration issue, etc.).
       The synthesized "<type>_error" ValidationResult on the
       run carries ``details['qualifire_internal_failure']=True``
       for forensic trail.

    A run with BOTH internal failures AND real data-quality
    findings raises ``QualifireValidationError`` (the data
    findings take precedence — they're what the operator's
    monitoring is paged on for). To check the mixed case
    explicitly, walk ``e.result.datasets[*].validation_results[*]``
    and inspect ``vr.details.get('qualifire_internal_failure')``.

    Carries the full QualifireResult on ``self.result`` so
    callers can inspect ``engine_warnings`` (where the
    ``qualifire.persistence`` warning row lives for the infra
    case) and the marked validation rows (for the validator-
    exception case).

    Sibling exception:
        :class:`QualifireValidationError` — distinct error
        category for data-quality findings. See its docstring
        for the routing pattern.
    """

    def __init__(self, message: str, result: QualifireResult | None = None):
        super().__init__(message)
        self.result = result


class QualifireNotificationError(QualifireError):
    """Raised when notification delivery fails."""


class MissingSecretResolverError(QualifireConfigError):
    """Raised when a config contains ``secret://...`` references but
    no ``secret_resolver`` was supplied to ``Qualifire(...)``.
    """


class SecretResolutionError(QualifireConfigError):
    """Raised when a ``secret://...`` reference cannot be resolved.

    Covers: malformed references, resolver-side failures (wrapped
    with original-exception type-name only — never the value, see
    feature plan Decision Pin 8), resolved-value type guard
    failures (resolver returned a non-``str`` / non-``dict``),
    and ``from_secret`` directive misuse (overlap with literals,
    overlap with per-field refs, missing required keys, unexpected
    keys).
    """


class QualifireSystemTableError(QualifireConfigError):
    """Raised when the system table cannot be initialised.

    Surfaces at ``ExternalCatalogStorage.initialize()`` for four
    distinct catalog-side failure modes, each with a structured
    remediation hint operators can route directly:

    1. **Namespace not found** — qualifire does not create catalog
       schemas; ask DBA to create the namespace and re-run.
    2. **Read-only catalog** — switch to a writable catalog or ask
       DBA to pre-create the table on a writable mirror.
    3. **CREATE privilege denied** — ask DBA to pre-create the
       table; qualifire only needs INSERT from then on.
    4. **Format-rejection / generic** — verify the catalog accepts
       ``CREATE TABLE IF NOT EXISTS <3-part-name> (...)`` DDL with
       no ``USING`` / ``LOCATION`` / ``TBLPROPERTIES``.

    Subclass of ``QualifireConfigError`` so existing callers
    catching the latter for "config-time problems" stay correct.
    """
