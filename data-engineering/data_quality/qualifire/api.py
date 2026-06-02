"""Programmatic API: the main Qualifire class for library usage."""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from qualifire.backends.base import Backend
from qualifire.core.config import (
    AggregationCollectionConfig,
    AnomalyDetectionValidationConfig,
    AnomalyModelConfig,
    AnomalyThresholdConfig,
    DatasetConfig,
    ForecastModelConfig,
    ForecastRuleConfig,
    ForecastValidationConfig,
    HistoricalCompareConfig,
    HistoricalRuleConfig,
    HistoricalThresholds,
    HistoricalValidationConfig,
    JDBCConfig,
    NotifyConfig,
    PatternModelConfig,
    PatternThresholdConfig,
    PatternValidationConfig,
    QualifireConfig,
    RecencyCollectionConfig,
    SampleCollectionConfig,
    SampleHistoryConfig,
    SLOValidationConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
    load_config,
)
from qualifire.core.backfill_report import BackfillReport
from qualifire.core.context import QualifireContext
from qualifire.core.engine import QualifireEngine
from qualifire.core.exceptions import QualifireConfigError
from qualifire.core.models import QualifireResult, ValidationResult
from qualifire.core.secrets import SecretResolver, resolve_secrets
from qualifire.notification.base import Notifier, get_notifier

# Ensure built-in notifiers are registered
import qualifire.notification.console_notifier  # noqa: F401
import qualifire.notification.email_notifier  # noqa: F401
import qualifire.notification.slack_notifier  # noqa: F401
import qualifire.notification.webhook_notifier  # noqa: F401

logger = logging.getLogger(__name__)


# Validator types whose correctness depends on a stable per-dataset
# history key. If any of these is attached, ``validate_query()`` must
# refuse to default the dataset name (history would land under a
# different key each run and drift / forecast lookups would miss).
_HISTORY_BACKED_VALIDATION_TYPES: tuple[type, ...] = (
    HistoricalValidationConfig,
    ForecastValidationConfig,
    AnomalyDetectionValidationConfig,
    PatternValidationConfig,
)


def _has_history_backed_validator(validations: list[Any]) -> bool:
    return any(isinstance(v, _HISTORY_BACKED_VALIDATION_TYPES) for v in validations or [])


class Qualifire:
    """Main entry point for the Qualifire library.

    Supports both config-driven (YAML/JSON) and programmatic usage.

    Args:
        backend: Data backend instance (SparkBackend or PandasBackend).
        system_table: Fully-qualified system table name.
        system_table_backend: Storage backend type ("external_catalog", "delta", "sqlite").
        owner: Owner identifier (team name).
        bu: Business unit identifier.

    Thread-safety:
        ``Qualifire`` is **not** thread-safe. The following instance
        attributes are read or mutated without locking, and concurrent
        calls on the same instance can race on any of them:

        - ``self.system_table`` / ``self.system_table_backend`` /
          ``self.jdbc`` / ``self._storage`` — mutated by the storage
          swap inside ``run_config_parsed`` and by constructor-time
          JDBC resolution.
        - ``self._notifiers`` — mutated by ``register_notifier`` and
          read by every ``run_config_parsed`` / ``validate`` /
          ``validate_query`` / ``write_audit_publish`` call.
        - ``self._secret_resolver`` — read by every API entry; only
          mutated at construction time, but a partially-constructed
          instance racing with a call elsewhere is undefined.

        Operators running parallel validations should construct one
        ``Qualifire`` per worker / thread. Per-call secret resolution
        itself is concurrency-safe (the resolver cache is local to a
        single ``resolve_secrets()`` invocation), but the surrounding
        instance-level mutations are not.
    """

    def __init__(
        self,
        backend: Backend | None,
        system_table: str | None = None,
        system_table_backend: str = "external_catalog",
        owner: str = "",
        bu: str = "",
        notifiers: dict[str, Notifier] | None = None,
        jdbc: JDBCConfig | dict[str, Any] | None = None,
        *,
        secret_resolver: SecretResolver | None = None,
        warn_on_reinit: bool = True,
        redacted_columns: list[str] | None = None,
        allowlist_columns: list[str] | None = None,
    ):
        self.backend = backend
        self.system_table = system_table
        self.system_table_backend = system_table_backend
        self.owner = owner
        self.bu = bu
        # Instance-level redaction policy. Combined additively with
        # any per-dataset list at validator-instantiation time. See
        # ``qualifire/core/_redaction.py`` for the resolution rules.
        self.redacted_columns: list[str] | None = redacted_columns
        self.allowlist_columns: list[str] | None = allowlist_columns
        self._secret_resolver = secret_resolver
        # When True (default), storage-swap dimensions disagreeing
        # between construction and a later run/backfill/deactivate
        # call surface a ``QualifireReinitWarning``. Pass False to
        # silence the warning when the swap is intentional (e.g. CLI
        # paths where construction and execution use the same YAML).
        self._warn_on_reinit: bool = warn_on_reinit
        # Normalize ``jdbc`` so callers can pass either a JDBCConfig or
        # a dict (useful when lifted straight from parsed YAML). ``None``
        # is valid whenever the backend isn't jdbc; ``_init_storage`` is
        # where the matching-backend requirement is enforced.
        if isinstance(jdbc, dict):
            jdbc = JDBCConfig(**jdbc)
        # Resolve constructor-supplied JDBC secrets before storage
        # opens. Scoped to the JDBC backend (per plan P2.2): a
        # templated jdbc block sitting alongside a non-jdbc backend
        # is left untouched — non-credential backends never read it.
        if (
            jdbc is not None
            and system_table is not None
            and system_table_backend == "jdbc"
        ):
            _, jdbc = resolve_secrets(cfg=None, resolver=self._secret_resolver, jdbc=jdbc)
        self.jdbc: JDBCConfig | None = jdbc
        self._storage = self._init_storage() if system_table else None
        self._notifiers: dict[str, Notifier] = dict(notifiers or {})
        # Default ``console`` channel — print-to-stdout notifier so
        # development / notebook workflows can route alerts via
        # ``notify: ["console"]`` without registering a programmatic
        # capturing notifier in a setup cell. Skipped when the caller
        # already supplied a ``console`` instance via ``notifiers={}``
        # (programmatic always wins over defaults), and overridable
        # later via ``register_notifier("console", ...)`` or a YAML
        # ``notifications.console:`` block.
        if "console" not in self._notifiers:
            from qualifire.notification.console_notifier import ConsoleNotifier

            self._notifiers["console"] = ConsoleNotifier()

    @classmethod
    def from_config(
        cls,
        config_path: str | Path,
        *,
        backend: Backend | None,
        notifiers: dict[str, Notifier] | None = None,
        secret_resolver: SecretResolver | None = None,
        warn_on_reinit: bool = True,
        **overrides: Any,
    ) -> Qualifire:
        """Construct a ``Qualifire`` instance from a YAML config + a
        runtime ``backend``.

        Lifts ``owner``, ``bu``, ``system_table``,
        ``system_table_backend``, and ``jdbc`` from the loaded
        config onto the instance. Per-config fields
        (``notifications``, ``engine_notify``, ``partition_ts``,
        ``partition_step``, ``dataset_parallelism``, ``context``,
        ``datasets``) stay attached to each run's config and apply
        per ``run_config_parsed`` / ``backfill`` / ``validate``
        call when YAML-backed; they are NOT lifted onto the
        instance. To make a notifier visible to programmatic
        paths (``validate_query``, ``write_audit_publish``,
        ``validate(...)`` without a config), pass it as a runtime
        instance via the ``notifiers={}`` kwarg — runtime
        instances flow into ``self._notifiers`` and win over any
        YAML ``notifications:`` block of the same name on every
        engine call.

        Any of ``owner``, ``bu``, ``system_table``,
        ``system_table_backend``, ``jdbc`` can be passed as
        ``**overrides`` to win over the YAML value.
        ``notifications`` is **not** an accepted override key
        (its merge semantics would be ambiguous) — use the
        explicit ``notifiers={}`` kwarg instead. Unknown override
        keys raise ``TypeError`` so typos surface immediately.

        ``backend=None`` is accepted for the SQLite-only path
        where storage operations don't require a Spark session
        (mirrors the existing ``Qualifire(backend=None,
        system_table_backend="sqlite", ...)`` pattern).

        Notifier merge precedence: the runtime instances passed
        via ``notifiers={}`` flow into ``self._notifiers`` and
        win over any YAML ``notifications:`` block on every
        notifier resolution (``run_config_parsed``,
        ``backfill``, ``validate``, ``validate_query``,
        ``write_audit_publish``). ``from_config`` does NOT
        pre-build YAML notifiers, so ``secret://`` references
        inside the YAML ``notifications:`` block stay deferred
        and resolve per-call.

        ``warn_on_reinit=True`` (default) emits
        ``QualifireReinitWarning`` when a subsequent
        ``run_config_parsed`` / ``backfill`` /
        ``deactivate_metric`` call's YAML disagrees with the
        construction-time YAML on ``system_table`` /
        ``system_table_backend`` / ``jdbc``. Pass ``False`` to
        silence the warning when the swap is intentional. The
        default applies to every construction path (one
        consistent contract across ``from_config`` and direct
        ``Qualifire(...)``).

        Secret resolution timing matches existing semantics:
        - JDBC connection secrets resolve eagerly during
          construction (the storage handle opens in
          ``__init__`` for ``system_table_backend == "jdbc"``).
        - YAML ``notifications:`` block secrets resolve lazily
          per-call (because YAML notifiers are not pre-built).

        Raises:
            QualifireConfigError: if the file is missing,
                malformed, or carries the deprecated ``backend:``
                field.
            TypeError: if an unknown ``**overrides`` key is passed.

        See Also:
            :meth:`_from_parsed_config` — module-private
            classmethod for callers (e.g. CLI) that already hold
            a parsed ``QualifireConfig`` and must not re-read
            the file (TOCTOU).
        """
        config = load_config(config_path)
        return cls._from_parsed_config(
            config,
            backend=backend,
            notifiers=notifiers,
            secret_resolver=secret_resolver,
            warn_on_reinit=warn_on_reinit,
            **overrides,
        )

    @classmethod
    def _from_parsed_config(
        cls,
        config: QualifireConfig,
        *,
        backend: Backend | None,
        notifiers: dict[str, Notifier] | None = None,
        secret_resolver: SecretResolver | None = None,
        warn_on_reinit: bool = True,
        **overrides: Any,
    ) -> Qualifire:
        """Internal classmethod that constructs from an already-
        parsed ``QualifireConfig``. Used by the CLI to avoid a
        second ``load_config()`` read after preflight (TOCTOU
        guard). ``from_config(path, ...)`` is the public one-line
        wrapper that loads then delegates here.

        Single leading underscore signals "stable enough to test;
        not part of the documented public API surface."
        """
        accepted_overrides = {
            "owner", "bu", "system_table", "system_table_backend", "jdbc",
            "redacted_columns", "allowlist_columns",
        }
        unknown = set(overrides) - accepted_overrides
        if unknown:
            raise TypeError(
                f"Qualifire.from_config got unexpected keyword "
                f"argument(s): {sorted(unknown)!r}. Accepted "
                f"overrides: {sorted(accepted_overrides)!r}. "
                f"Pass runtime notifiers via notifiers={{}}."
            )

        return cls(
            backend=backend,
            system_table=overrides.get("system_table", config.system_table),
            system_table_backend=overrides.get(
                "system_table_backend", config.system_table_backend,
            ),
            owner=overrides.get("owner", config.owner),
            bu=overrides.get("bu", config.bu),
            jdbc=overrides.get("jdbc", config.jdbc),
            # Runtime notifier instances flow straight into
            # ``self._notifiers`` and win over YAML on every merge
            # via ``_effective_notifiers``. YAML
            # ``notifications:`` config is NOT pre-built — it
            # remains deferred so ``secret://`` references resolve
            # at run time.
            notifiers=notifiers,
            secret_resolver=secret_resolver,
            warn_on_reinit=warn_on_reinit,
            redacted_columns=overrides.get("redacted_columns"),
            allowlist_columns=overrides.get("allowlist_columns"),
        )

    def register_notifier(self, name: str, notifier: Notifier) -> None:
        """Register a notifier instance by name for programmatic usage.

        Once registered, the name can be used in `notify` params on
        builder methods. The registration wins over any YAML
        ``notifications:`` block of the same name on every
        subsequent ``run_config_parsed`` / ``backfill`` /
        ``validate`` call (programmatic always wins over YAML —
        see ``_effective_notifiers``).
        """
        self._notifiers[name] = notifier

    def _effective_notifiers(
        self,
        yaml_notifications: Any = None,
    ) -> dict[str, Notifier]:
        """Single source of truth for notifier resolution.

        Merge order (later wins):
          1. YAML ``notifications:`` block — when a config is in
             scope (``yaml_notifications`` is non-None / truthy).
             Built lazily via ``_build_notifiers`` so ``secret://``
             references inside the YAML resolve per-call rather
             than at construction time.
          2. ``self._notifiers`` — runtime notifier instances passed
             via the ``notifiers={}`` constructor kwarg or registered
             post-construction via ``register_notifier``.
             Programmatic always wins over YAML.

        Used by ``run_config_parsed``, ``backfill``, ``validate``,
        ``validate_query``, and ``write_audit_publish`` so every
        engine entrypoint honors the same precedence.
        """
        merged: dict[str, Notifier] = {}
        if yaml_notifications:
            merged.update(self._build_notifiers(yaml_notifications))
        merged.update(self._notifiers)
        return merged

    def _swap_storage_if_needed(self, config: QualifireConfig) -> Any:
        """Atomic storage-swap helper shared by ``run_config_parsed``
        and ``_resolve_storage_for_op``.

        Computes ``table_changed`` / ``backend_changed`` /
        ``jdbc_changed``, emits ``QualifireReinitWarning`` when any
        dimension flips and ``self._warn_on_reinit`` is True, then
        performs the candidate-locals + commit pattern (instance
        state stays unchanged if ``_init_storage`` raises).

        Centralizing here ensures the warning fires from every
        storage-swap path — direct ``run_config`` calls and the
        ``backfill`` / ``deactivate_metric`` paths that go through
        ``_resolve_storage_for_op``.
        """
        import warnings

        from qualifire.core.exceptions import QualifireReinitWarning

        table_changed = (
            bool(config.system_table)
            and config.system_table != self.system_table
        )
        backend_explicit = "system_table_backend" in config.model_fields_set
        backend_changed = (
            backend_explicit
            and config.system_table_backend != self.system_table_backend
        )
        jdbc_explicit = "jdbc" in config.model_fields_set
        jdbc_changed = jdbc_explicit and config.jdbc != self.jdbc

        if not (table_changed or backend_changed or jdbc_changed):
            return self._storage

        candidate_table = config.system_table
        candidate_backend = (
            config.system_table_backend
            if backend_explicit
            else self.system_table_backend
        )
        candidate_jdbc = config.jdbc if jdbc_explicit else self.jdbc

        if self._warn_on_reinit:
            changes: list[str] = []
            if table_changed:
                changes.append(
                    f"system_table: {self.system_table!r} -> {candidate_table!r}"
                )
            if backend_changed:
                changes.append(
                    f"system_table_backend: "
                    f"{self.system_table_backend!r} -> {candidate_backend!r}"
                )
            if jdbc_changed:
                changes.append("jdbc: <changed>")
            warnings.warn(
                QualifireReinitWarning(
                    f"reinitializing storage: {'; '.join(changes)}. "
                    f"Pass warn_on_reinit=False to "
                    f"Qualifire.from_config(...) or Qualifire(...) "
                    f"if this storage swap is intentional."
                ),
                # +1 frame because of this helper hop; callers'
                # frames remain at stacklevel 3.
                stacklevel=3,
            )

        storage = self._init_storage(
            system_table=candidate_table,
            system_table_backend=candidate_backend,
            jdbc=candidate_jdbc,
        )
        self.system_table = candidate_table
        self.system_table_backend = candidate_backend
        self.jdbc = candidate_jdbc
        self._storage = storage
        return storage

    def _resolve_secrets_or_raise(
        self, config: QualifireConfig
    ) -> QualifireConfig:
        """Walk credential allowlist and return a resolved copy.

        The caller's ``config`` is never mutated (see
        ``qualifire.core.secrets.resolve_secrets`` selective-copy
        contract). When no resolver is configured and the config
        contains no references, the call is a structural no-op.

        ``effective_backend`` mirrors ``run_config_parsed``'s
        candidate-backend resolution: when YAML does not override
        ``system_table_backend``, the instance backend wins. Without
        this, a templated YAML carrying ``secret://...`` refs in
        ``jdbc:`` paired with an instance-backend of ``"jdbc"``
        would skip resolution and storage would open with literal
        secret-reference strings.
        """
        backend_explicit = "system_table_backend" in config.model_fields_set
        effective_backend = (
            config.system_table_backend if backend_explicit else self.system_table_backend
        )
        resolved, _ = resolve_secrets(
            config,
            self._secret_resolver,
            jdbc=None,
            effective_backend=effective_backend,
        )
        return resolved

    def _init_storage(
        self,
        *,
        system_table: str | None = None,
        system_table_backend: str | None = None,
        jdbc: JDBCConfig | None = None,
    ) -> Any:
        """Initialize the system table storage backend.

        Accepts optional overrides so callers (notably
        ``run_config_parsed``) can probe a *candidate* table/backend
        without first mutating ``self``. Defaults fall back to the
        instance fields so existing zero-arg call sites behave
        unchanged. This lets storage migrations be all-or-nothing:
        instance state is committed only after ``_init_storage``
        returns successfully.

        Delegates the per-backend wiring to
        :func:`qualifire.storage.factory.open_storage` so the
        notebook-facing :func:`qualifire.reporting.make_storage` and
        this engine-facing path share a single implementation.
        """
        from qualifire.storage.factory import open_storage

        table = system_table if system_table is not None else self.system_table
        backend = (
            system_table_backend
            if system_table_backend is not None
            else self.system_table_backend
        )
        jdbc_cfg = jdbc if jdbc is not None else self.jdbc
        # Centralized JDBC secret resolution at the storage-open
        # boundary. Covers the path where ``Qualifire(jdbc=...)``
        # was given without a ``system_table`` (constructor doesn't
        # call ``_init_storage``); the table is added later by the
        # caller and ``_init_storage`` is the next layer that would
        # otherwise hand a literal ``secret://...`` straight into
        # Spark JDBC.
        if backend == "jdbc" and jdbc_cfg is not None and self._has_secret_refs_or_directive(jdbc_cfg):
            _, jdbc_cfg = resolve_secrets(
                cfg=None, resolver=self._secret_resolver, jdbc=jdbc_cfg
            )
        spark = getattr(self.backend, "spark", None)
        return open_storage(backend, table, jdbc=jdbc_cfg, spark=spark)

    @staticmethod
    def _has_secret_refs_or_directive(jdbc_cfg: JDBCConfig) -> bool:
        """Cheap pre-check so non-secret JDBCConfigs skip the
        deep-copy / walker overhead in ``_init_storage``.
        """
        from qualifire.core.secrets import is_secret_ref

        if jdbc_cfg.from_secret is not None:
            return True
        for fname in ("url", "user", "password", "driver"):
            if is_secret_ref(getattr(jdbc_cfg, fname, None)):
                return True
        for v in (jdbc_cfg.properties or {}).values():
            if is_secret_ref(v):
                return True
        return False

    def run_config(
        self,
        config_path: str,
        context: dict[str, str] | None = None,
        *,
        skip_recollection: bool = False,
        skip_renotification: bool = False,
        skip_revalidation: bool = False,
    ) -> QualifireResult:
        """Run validations from a YAML/JSON config file.

        Thin wrapper over :meth:`run_config_parsed` that loads the
        config from disk. Callers that already hold a parsed
        ``QualifireConfig`` (notably the CLI, which reads the file
        once in the preflight validator and must not re-read it —
        see ``_cmd_run``) should call :meth:`run_config_parsed`
        directly to avoid a file-swap TOCTOU between preflight and
        execution.

        Args:
            config_path: Path to the config file.
            context: Extra Jinja2 context variables.

        Returns:
            QualifireResult with all dataset results and notifications.

        Raises:
            QualifireValidationError: If any dataset has ERROR
                severity due to a data-quality finding (validator
                threshold breach, drift outside bounds, etc.).
                Mixed runs (data findings + qualifire-internal
                failures) also raise this class — the data
                findings take precedence in routing.
            QualifireInternalError: If the run failed entirely
                due to qualifire-internal issues — system-table
                persistence outage, or all ERROR rows came from
                validator-execution exceptions (no real data
                findings). Sibling class to
                ``QualifireValidationError``: ``except
                QualifireValidationError`` does NOT catch this.
                Persistence-infrastructure failures preserve the
                original storage exception via ``__cause__``
                (PEP 3134 chaining).
        """
        config = load_config(config_path)
        return self.run_config_parsed(
            config, context=context,
            skip_recollection=skip_recollection,
            skip_renotification=skip_renotification,
            skip_revalidation=skip_revalidation,
        )

    def run_config_parsed(
        self,
        config: QualifireConfig,
        context: dict[str, str] | None = None,
        *,
        skip_recollection: bool = False,
        skip_renotification: bool = False,
        skip_revalidation: bool = False,
    ) -> QualifireResult:
        """Run validations from an already-parsed config object.

        This is the single source of truth for config-driven
        execution. ``run_config(path)`` exists as a convenience
        wrapper. Taking a parsed object here lets CLI callers load
        the file exactly once (for preflight validation) and hand
        the resulting object through to execution, eliminating a
        TOCTOU window where the file could be swapped between
        preflight and run.

        Storage reinitialization rules:
          - ``system_table`` change → always reinit. Required field
            on ``QualifireConfig`` so ``model_fields_set`` always
            contains it; comparison is direct.
          - ``system_table_backend`` change → reinit *only* when the
            caller explicitly set the field. The Pydantic default
            (``"external_catalog"``) cannot be distinguished from an
            explicit value at the attribute level, so a programmatic
            caller who built ``Qualifire(system_table_backend=
            "sqlite")`` and passes a parsed config without the field
            would otherwise be silently flipped to
            ``external_catalog``. Detected via
            ``config.model_fields_set`` (Pydantic v2 records exactly
            which fields the caller supplied).

        Reinitialized storage is persisted to ``self._storage`` so
        subsequent calls (``validate``, ``health_report``, a later
        ``run_config_parsed``) use the new backend. Before this fix
        the new handle lived only in a local variable and the next
        API call silently reverted to the stale one — a
        write/read-split disaster for any migration workflow.

        Reinit is all-or-nothing: candidate table/backend stay in
        locals until ``_init_storage`` succeeds, then
        ``self.system_table``, ``self.system_table_backend``, and
        ``self._storage`` are assigned together. If ``_init_storage``
        raises (bad catalog permissions, missing path, transient
        backend error), instance state is unchanged — a retry with
        the same config still sees a pending migration and reinits
        again, rather than falsely concluding the swap already
        completed.

        Args:
            config: A validated ``QualifireConfig`` instance
                (typically from ``load_config()``).
            context: Extra Jinja2 context variables, merged on top
                of ``config.context``.

        Returns:
            QualifireResult with all dataset results and notifications.

        Raises:
            QualifireValidationError: If any dataset has ERROR
                severity due to a data-quality finding (validator
                threshold breach, drift outside bounds, etc.).
                Mixed runs (data findings + qualifire-internal
                failures) also raise this class — the data
                findings take precedence in routing.
            QualifireInternalError: If the run failed entirely
                due to qualifire-internal issues — system-table
                persistence outage, or all ERROR rows came from
                validator-execution exceptions (no real data
                findings). Sibling class to
                ``QualifireValidationError``: ``except
                QualifireValidationError`` does NOT catch this.
                Persistence-infrastructure failures preserve the
                original storage exception via ``__cause__``
                (PEP 3134 chaining).
        """
        # Resolve secret references (notifications, config-level jdbc)
        # BEFORE any storage swap. The caller's ``config`` is unchanged;
        # the engine sees the resolved copy.
        config = self._resolve_secrets_or_raise(config)
        ctx = QualifireContext(extra_context={**(config.context or {}), **(context or {})})
        ctx.skip_recollection = skip_recollection
        ctx.skip_renotification = skip_renotification
        ctx.skip_revalidation = skip_revalidation
        ctx.instance_redacted_columns = self.redacted_columns
        ctx.instance_allowlist_columns = self.allowlist_columns

        # Notifier merge: instance + YAML notifications + always-wins
        # programmatic overrides. See ``_effective_notifiers``.
        notifiers = self._effective_notifiers(config.notifications)

        # Atomic storage swap (with reinit warning when enabled).
        # See ``_swap_storage_if_needed``.
        storage = self._swap_storage_if_needed(config)

        engine = QualifireEngine(
            backend=self.backend,
            storage=storage,
            context=ctx,
            config=config,
            notifiers=notifiers,
        )
        return engine.run()

    def backfill(
        self,
        config: QualifireConfig | str | None = None,
        *,
        partition_ts: Any,
        selector: str | None = None,
        data: bool = False,
        skip_recollection: bool = False,
        skip_renotification: bool = False,
        skip_revalidation: bool = False,
        soft_delete_prior: bool = False,
        parallelism: int = 1,
        max_partitions: int = 10000,
    ) -> BackfillReport:
        """Run a metrics-only backfill against past partition(s).

        For WAP-backed datasets, reads from ``wap.target_table``
        filtered to ``wap.partition_column = P`` rather than
        re-running ``wap.sql`` (the cost / source-retention /
        idempotency story in the idea). Non-WAP datasets read from
        ``dataset.table`` filtered to the partition value.

        Args:
            config: Optional ``QualifireConfig`` or path to a config
                file. When provided, storage is (re)initialized to
                match. When ``None``, the instance's existing storage
                is used.
            partition_ts: Single partition (string / datetime), list
                of partitions, or ``(start, end)`` tuple expanded by
                each dataset's effective ``partition_step``. Range
                is inclusive on both ends.
            selector: Optional ``<dataset>:<validation>[:<metric>]``
                selector restricting scope. Default = every
                validation in the config.
            data: ``False`` (default) = metrics-only refresh
                (no WRITE / PUBLISH). ``True`` = full WAP cycle on
                the past partition; only valid for WAP datasets.
            skip_recollection: Pass-through to the engine's pre-pass
                (Step 6) — short-circuits cache-eligible collectors
                when matching values exist.
            skip_renotification: When True, the engine's notification
                dispatch skips any (dataset, validation, metric, dim)
                key whose prior run already carried the same severity.
                Default ``False`` so backfills re-page on every replay
                unless the operator opts in.
            soft_delete_prior: When True, write a tombstone INSERT
                for every metric the backfill touches BEFORE the
                new collection INSERTs. Default False (newer
                ``run_timestamp`` supersession is sufficient for
                most operator workflows).
            parallelism: Number of concurrent worker threads
                processing distinct ``(scope, anchor)`` units.
                Default ``1`` (serial; today's behavior).
                ``parallelism > 1`` forces ``notifiers={}`` for
                the inner engine call to avoid suppression races
                — `BackfillReport.notifications_suppressed` flags
                this. Cap: 64.
            max_partitions: Upper bound on the number of partitions
                produced by ``(start, end)`` range expansion.
                Default ``10000``. Operators backfilling 50k+
                partitions in one shot can lift this; smaller values
                surface runaway expansions earlier.

        Returns:
            ``BackfillReport`` with per-partition diff
            (``original_value`` → ``backfilled_value``, severity
            flip if any) and aggregate counts (``refreshed`` /
            ``unchanged`` / ``skipped`` / ``errored``).

        Raises:
            QualifireConfigError: For backfill-misconfiguration
                (missing ``partition_column`` on WAP, ``data=True``
                on non-WAP, malformed ``partition_ts``, selector
                no-match, etc.).
            QualifireValidationError: When ``report.has_errors`` —
                at least one partition errored. Sibling partitions
                still completed; the report is attached.
        """
        from qualifire.core.backfill import run_backfill

        # Normalize ``config`` exactly once (TOCTOU guard). Two reads
        # of the same YAML path would let a file-swap racer execute
        # against snapshot B while storage was opened from snapshot
        # A. Backfill needs the parsed config for both the storage
        # swap and the engine driver, so we resolve it here, then
        # hand the same object to both call sites below.
        if config is None:
            raise QualifireConfigError(
                "Qualifire.backfill requires a config argument so the "
                "engine can resolve datasets / validations to run."
            )
        if isinstance(config, str):
            config = load_config(config)
        config = self._resolve_secrets_or_raise(config)

        # ``_resolve_storage_for_op`` accepts the already-parsed
        # config — no second ``load_config`` call. Hand the resolved
        # config object straight in.
        storage = self._resolve_storage_for_op(config)

        # Notifier merge: instance + YAML notifications + always-wins
        # programmatic overrides. See ``_effective_notifiers``.
        notifiers = self._effective_notifiers(config.notifications)

        # Validate new kwargs at the API boundary before any work.
        if parallelism < 1 or parallelism > 64:
            raise QualifireConfigError(
                f"parallelism must be in [1, 64]; got {parallelism}"
            )
        if max_partitions < 1:
            raise QualifireConfigError(
                f"max_partitions must be >= 1; got {max_partitions}"
            )

        report = run_backfill(
            config=config,
            storage=storage,
            backend=self.backend,
            notifiers=notifiers,
            partition_ts=partition_ts,
            selector=selector,
            data=data,
            skip_recollection=skip_recollection,
            skip_renotification=skip_renotification,
            skip_revalidation=skip_revalidation,
            soft_delete_prior=soft_delete_prior,
            parallelism=parallelism,
            max_partitions=max_partitions,
            owner=self.owner,
            bu=self.bu,
            instance_redacted_columns=self.redacted_columns,
            instance_allowlist_columns=self.allowlist_columns,
        )
        if report.has_errors:
            # Plan N18: per-partition error isolation runs each
            # partition independently, then surfaces the aggregate
            # via an exception so non-zero-exit-driven monitoring
            # fires. The report is attached for forensic inspection.
            from qualifire.core.exceptions import QualifireValidationError
            err = QualifireValidationError(
                f"backfill: {report.errored} partition(s) errored "
                f"(refreshed={report.refreshed}, "
                f"unchanged={report.unchanged}, "
                f"skipped={report.skipped}, "
                f"errored={report.errored}, "
                f"total={report.total}). "
                f"See exception.report for the full BackfillReport."
            )
            err.report = report  # type: ignore[attr-defined]
            raise err
        return report

    def deactivate_metric(
        self,
        config: QualifireConfig | str | None = None,
        *,
        dataset_name: str,
        metric_name: str,
        dimension_value: str | None = None,
        partition_ts: Any = None,
        note: str | None = None,
    ) -> int:
        """Tombstone every active row matching the filter.

        Operator-facing soft-delete API. Writes a tombstone INSERT
        per matched active row (H1 read-back-and-bump). Subsequent
        reads honour the tombstone via the H2 read-side enforcement
        on every backend (SQLite + Delta + ExternalCatalog + JDBC,
        as of the K storage-parity work in
        ``backfill-api-and-wap-target-mode``).

        Args:
            config: Optional ``QualifireConfig`` or path to a config
                file. When provided, storage is (re)initialized to
                match the config's ``system_table`` / backend /
                ``jdbc`` block before the deactivate runs. When
                ``None``, the instance's existing storage handle is
                used (operator constructed ``Qualifire(system_table=
                ...)`` already).
            dataset_name: Identifies the dataset whose metric to
                deactivate.
            metric_name: Logical metric name.
            dimension_value: Optional encoded dimension. ``None``
                matches rows persisted with SQL NULL (non-dimensioned
                metrics). To deactivate a specific dimension, pass
                its encoded form.
            partition_ts: ``None`` (default) tombstones every active
                partition for the matched (dataset, metric, dim)
                tuple. A specific datetime / ISO string narrows to
                that partition.
            note: Free-form string captured in the tombstone row's
                ``details_json.deactivated_by`` (e.g. ops ticket
                number, human rationale).

        Returns:
            Number of tombstone rows written.

        Raises:
            QualifireConfigError: When ``config`` cannot be loaded
                or storage cannot be initialized.
            Backend-native column-not-found / table-missing errors
                when invoked against a legacy system table that
                pre-dates the ``is_active`` column (loud-fail per
                D9 — operators see the schema gap immediately).

        Notes:
            ``deactivate_metric`` is non-WAP-aware: it tombstones
            rows in the qualifire system table only; it does not
            modify ``wap.target_table`` or any user-facing data
            warehouse table.
        """
        from qualifire.core.deactivate import deactivate_metric as _do

        storage = self._resolve_storage_for_op(config)
        return _do(
            storage,
            dataset_name=dataset_name,
            metric_name=metric_name,
            dimension_value=dimension_value,
            partition_ts=partition_ts,
            note=note,
            owner=self.owner,
            bu=self.bu,
        )

    def _resolve_storage_for_op(
        self,
        config: QualifireConfig | str | None,
    ) -> Any:
        """Resolve the storage handle for an out-of-engine operation.

        Shared helper between ``deactivate_metric`` and the future
        ``backfill`` API: both need a resolved storage handle but
        don't run the engine. The caller may pass a config path /
        parsed config to (re)initialize storage, or ``None`` to use
        the instance's existing handle.

        Mirrors ``run_config_parsed``'s storage-swap rules so a
        config-driven deactivate uses exactly the same backend the
        engine would use for the same config.
        """
        if config is None:
            if self._storage is None:
                raise QualifireConfigError(
                    "Qualifire.deactivate_metric requires either an "
                    "instance-level system_table (Qualifire(system_table=...)) "
                    "or a config argument naming the system table."
                )
            return self._storage

        if isinstance(config, str):
            config = load_config(config)
        config = self._resolve_secrets_or_raise(config)

        # Atomic storage swap (with reinit warning when enabled).
        # Centralised in ``_swap_storage_if_needed`` so the warning
        # also fires from ``backfill`` and ``deactivate_metric``,
        # not just ``run_config_parsed``.
        return self._swap_storage_if_needed(config)

    def validate(
        self,
        table: str | None = None,
        validations: list[Any] | None = None,
        df: Any = None,
        name: str | None = None,
        filter_expr: str | None = None,
        notify: dict[str, list[str]] | None = None,
        context: dict[str, str] | None = None,
        partition_ts: str | None = None,
        partition_step: str | None = None,
        *,
        skip_recollection: bool = False,
        skip_renotification: bool = False,
        skip_revalidation: bool = False,
    ) -> QualifireResult:
        """Run programmatic validations on a table or DataFrame.

        At least one of ``table`` or ``df`` must be provided:

        - ``table=`` only: validate a registered table/view by name.
        - ``df=`` only: validate an in-memory DataFrame. An explicit
          ``name`` is REQUIRED in this mode — it becomes the logical
          identity used for system-table persistence and drift/forecast
          history lookups. Omitting it would share history and alert
          suppression state across unrelated df-only calls.
        - ``table=`` + ``df=``: df supplies the data, table supplies the
          logical identity — useful when validating a fresh snapshot of
          a known table name.

        Args:
            table: Table name. Optional when ``df`` is given (and
                ``name`` is supplied to take over the identity role).
            validations: List of validation configs (from builder methods).
            df: Optional DataFrame (registered as temp view if provided).
            name: Dataset name for reporting and history keying.
                Required when ``table`` is omitted.
            filter_expr: Filter expression applied to all validations.
            notify: Notification routing {"warning": [...], "error": [...]}.
            context: Extra Jinja2 variables.
        """
        if table is None and df is None:
            raise ValueError(
                "validate() requires either 'table' (a registered table/view name) "
                "or 'df' (an in-memory DataFrame). Got neither."
            )
        if validations is None:
            raise ValueError("validate() requires 'validations' to be supplied.")
        # For df-only callers (no table), an explicit ``name`` is
        # REQUIRED — we do not silently default to a shared identifier.
        # The dataset name becomes the ``logical_table`` for
        # system-table persistence and drift/forecast history keying.
        # Without it, repeated df-only runs would collapse onto the
        # same key, and one caller's drift history / suppression state
        # would leak into an unrelated caller's validations. Force the
        # caller to own the identity choice.
        if table is None and name is None:
            raise ValueError(
                "validate(df=..., name=...) requires an explicit 'name' when no "
                "'table' is supplied. The dataset name becomes the persistence / "
                "drift-history key; without one, history and repeat-alert "
                "suppression would share state across unrelated df-only calls."
            )
        dataset_name = name or table  # guaranteed non-None by the checks above
        ctx = QualifireContext(extra_context=context or {})
        ctx.skip_recollection = skip_recollection
        ctx.skip_renotification = skip_renotification
        ctx.skip_revalidation = skip_revalidation
        ctx.instance_redacted_columns = self.redacted_columns
        ctx.instance_allowlist_columns = self.allowlist_columns

        # Dataset-level notify fanout: when caller passed `notify=`, fan it
        # out onto every entry in `validations` whose own `notify` is the
        # default empty value. Per-validation `notify` wins. Use
        # `model_copy(update=...)` so caller-owned config objects are not
        # mutated — reusing the same builder config in a later call
        # without `notify=` must not carry over the previous routing.
        validations = self._fan_out_dataset_notify(validations, notify)

        # Build minimal config. When df is supplied, it flows through
        # DatasetConfig and is materialized by the engine's backend-agnostic
        # temp-view lifecycle (see engine._materialize_df), so it works for
        # both Spark and Pandas backends and cleans up on success and error.
        config = QualifireConfig(
            owner=self.owner,
            bu=self.bu,
            system_table=self.system_table or "",
            datasets=[
                DatasetConfig(
                    name=dataset_name,
                    table=table,
                    df=df,
                    filter=filter_expr,
                    partition_ts=partition_ts,
                    partition_step=partition_step,
                    validations=validations,
                )
            ],
        )
        config = self._resolve_secrets_or_raise(config)

        engine = QualifireEngine(
            backend=self.backend,
            storage=self._storage,
            context=ctx,
            config=config,
            notifiers=self._effective_notifiers(),
        )
        return engine.run()

    def validate_query(
        self,
        query: str,
        validations: list[Any],
        name: str | None = None,
        dimensions: list[str] | None = None,
        measures: list[str] | None = None,
        cache: bool = False,
        cache_storage_level: str = "MEMORY_AND_DISK",
        context: dict[str, str] | None = None,
        partition_ts: str | None = None,
        partition_step: str | None = None,
        *,
        skip_renotification: bool = False,
    ) -> QualifireResult:
        """Run validations on the result of a custom SQL query.

        The query is materialized as a temp view and all validations run
        against it, just like a table-based dataset.

        Args:
            query: SQL query (supports Jinja2 templating).
            validations: List of validation configs (from builder methods).
            name: Dataset name for reporting and history keying. REQUIRED
                when any history-backed validator (historical / forecast /
                anomaly_detection) is attached, because the dataset name is
                the history key. The default ``"custom_query"`` is fine for
                stateless checks, but two different SQL bodies both using
                the default would share drift/forecast baselines — a silent
                correctness bug. See also the equivalent gate on
                ``validate(df=..., name=...)``.
            dimensions: Declared dimension columns (validated against result schema).
            measures: Declared measure columns (validated against result schema).
            cache: Cache the query result for faster multi-validation reads.
            cache_storage_level: Spark StorageLevel name.
            context: Extra Jinja2 variables.
        """
        # History-backed validators key lookups on the dataset name. If
        # the caller didn't pick one, refuse to default — otherwise
        # multiple ``validate_query()`` calls with different SQL silently
        # read and write the same ``"custom_query"`` history, producing
        # false drift/forecast verdicts across unrelated checks.
        # Stateless validators (SLO, threshold) are fine with the default.
        if name is None and _has_history_backed_validator(validations):
            raise ValueError(
                "validate_query(..., name=...) requires an explicit 'name' "
                "when history-backed validators (historical / forecast / "
                "anomaly_detection) are attached. The dataset name is the "
                "drift-history key — without one, repeated validate_query() "
                "calls with different SQL would silently share baselines "
                "under the default 'custom_query' identifier and emit "
                "false drift/forecast verdicts."
            )
        dataset_name = name or "custom_query"
        ctx = QualifireContext(extra_context=context or {})
        ctx.skip_renotification = skip_renotification
        ctx.instance_redacted_columns = self.redacted_columns
        ctx.instance_allowlist_columns = self.allowlist_columns

        config = QualifireConfig(
            owner=self.owner,
            bu=self.bu,
            system_table=self.system_table or "",
            datasets=[
                DatasetConfig(
                    name=dataset_name,
                    query=query,
                    dimensions=dimensions,
                    measures=measures,
                    cache=cache,
                    cache_storage_level=cache_storage_level,
                    partition_ts=partition_ts,
                    partition_step=partition_step,
                    validations=validations,
                )
            ],
        )
        config = self._resolve_secrets_or_raise(config)

        engine = QualifireEngine(
            backend=self.backend,
            storage=self._storage,
            context=ctx,
            config=config,
            notifiers=self._effective_notifiers(),
        )
        return engine.run()

    def write_audit_publish(
        self,
        target_table: str,
        validations: list[Any],
        df: Any = None,
        sql: str | None = None,
        sql_file: str | None = None,
        write_options: dict[str, Any] | None = None,
        name: str | None = None,
        notify: dict[str, list[str]] | None = None,
        context: dict[str, str] | None = None,
        cache: bool = False,
        cache_storage_level: str = "MEMORY_AND_DISK",
        *,
        skip_renotification: bool = False,
    ) -> QualifireResult:
        """Run WAP pattern: write to staging, validate, publish or rollback.

        Args:
            target_table: Final destination table.
            validations: List of validation configs.
            df: DataFrame to write (for DataFrame-driven WAP).
            sql: SELECT query (for SQL-driven WAP). Staging table is managed internally.
            sql_file: Path to a .sql file with the SELECT query
                (alternative to inline ``sql=``). Read at config-load
                time. Mutually exclusive with ``df`` and ``sql``.
            write_options: Spark write options (mode, partitionBy, etc.).
            name: Dataset name for reporting.
            notify: Notification routing.
            context: Extra Jinja2 variables.
            cache: Cache staging data for faster multi-validation reads.
            cache_storage_level: Spark StorageLevel name (default: MEMORY_AND_DISK).
        """
        from qualifire.core.config import WAPConfig

        dataset_name = name or target_table
        ctx = QualifireContext(extra_context=context or {})
        ctx.skip_renotification = skip_renotification
        ctx.instance_redacted_columns = self.redacted_columns
        ctx.instance_allowlist_columns = self.allowlist_columns

        # P3.2: WAP DataFrame is plumbed through ``WAPConfig.df`` —
        # no engine monkey-patch. Three-way mutual exclusion
        # (``df`` × ``sql`` × ``sql_file``) is enforced by
        # WAPConfig's ``@model_validator`` (§R9 + sub-feature B).
        wap_cfg = WAPConfig(
            target_table=target_table,
            sql=sql,
            sql_file=sql_file,
            write_options=write_options or {},
            df=df,
        )

        # Dataset-level notify fanout for WAP datasets too.
        validations = self._fan_out_dataset_notify(validations, notify)

        config = QualifireConfig(
            owner=self.owner,
            bu=self.bu,
            system_table=self.system_table or "",
            datasets=[
                DatasetConfig(
                    name=dataset_name,
                    wap=wap_cfg,
                    validations=validations,
                    cache=cache,
                    cache_storage_level=cache_storage_level,
                )
            ],
        )
        config = self._resolve_secrets_or_raise(config)

        engine = QualifireEngine(
            backend=self.backend,
            storage=self._storage,
            context=ctx,
            config=config,
            notifiers=self._effective_notifiers(),
        )
        return engine.run()

    # --- Convenience builders for programmatic validations ---

    @staticmethod
    def slo_check(
        column: str | None = None,
        strategy: str = "max_column",
        sql: str | None = None,
        warning: str | None = None,
        error: str | None = None,
        notify: dict[str, list[str]] | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> SLOValidationConfig:
        """Build an SLO validation config."""
        thresholds = {}
        if warning:
            thresholds["warning"] = warning
        if error:
            thresholds["error"] = error
        kwargs: dict[str, Any] = {
            "recency": RecencyCollectionConfig(strategy=strategy, column=column, sql=sql),
            "thresholds": thresholds,
        }
        if notify is not None:
            kwargs["notify"] = NotifyConfig(**notify)
        if name is not None:
            kwargs["name"] = name
        if description is not None:
            kwargs["description"] = description
        return SLOValidationConfig(**kwargs)

    @staticmethod
    def threshold_check(
        aggregations: dict[str, str] | None = None,
        metrics: list[str] | None = None,
        rules: list[dict[str, Any]] | None = None,
        notify: dict[str, list[str]] | None = None,
        name: str | None = None,
        description: str | None = None,
        dimensions: list[str] | None = None,
    ) -> ThresholdValidationConfig:
        """Build a threshold validation config.

        Choosing how to feed metrics into the validator
        ===============================================

        Three call shapes, picked based on what the source data
        looks like at the point where the validator reads it:

        1. **Source already aggregated** (typical
           ``validate_query()`` flow — the inner SELECT computes
           ``COUNT(*) AS row_count, AVG(amount) AS avg_amount``
           and returns a 1-row table)::

               qf.validate_query(
                   query="SELECT COUNT(*) AS row_count, "
                         "AVG(amount) AS avg_amount "
                         "FROM sales WHERE sale_date='{ds}'",
                   validations=[
                       Qualifire.threshold_check(
                           rules=[{'metric': 'row_count',
                                   'thresholds': {'error': {'min': 100}}},
                                  {'metric': 'avg_amount',
                                   'thresholds': {'warning': {'max': 1000}}}],
                       )
                   ],
               )

           Neither ``aggregations`` nor ``metrics`` is needed —
           the builder reads metric names off ``rules`` and wraps
           each in ``MAX(<col>) AS <col>``. ``MAX`` over a 1-row
           input is a no-op; it's there so the validator's
           collection contract (one numeric value per metric) is
           uniform with the dimensional case below.

        2. **Source is raw rows; reduce with explicit
           expressions** (e.g. validating a fact table directly via
           ``qf.validate(table='retail.sales')``)::

               Qualifire.threshold_check(
                   aggregations={
                       'row_count':  'COUNT(*)',
                       'avg_amount': 'AVG(amount)',
                       'null_pid_pct': (
                           "SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) "
                           "* 100.0 / NULLIF(COUNT(*), 0)"
                       ),
                   },
                   rules=[{'metric': 'row_count',  ...},
                          {'metric': 'null_pid_pct', ...}],
               )

           ``aggregations`` is required — without it the validator
           has no idea how to reduce the raw rows to scalars.
           Custom expressions matter here: ``COUNT(*)``, weighted
           averages, NULL-aware ratios, etc.

        3. **Per-segment evaluation** with ``dimensions=[...]`` —
           the aggregation collector adds a ``GROUP BY`` over those
           columns and emits one result row per (metric,
           dimension-combo). The threshold rule fires
           independently on each segment::

               Qualifire.threshold_check(
                   dimensions=['region'],
                   aggregations={'rows': 'COUNT(*)'},
                   rules=[{'metric': 'rows',
                           'thresholds': {'error': {'min': 100}}}],
               )

           Here the source query MUST be raw / un-aggregated. If
           the inner SELECT already has its own ``GROUP BY``, the
           outer ``GROUP BY region`` is added on top and the
           combination depends on whether ``region`` is in the
           inner GROUP BY:

           - **Inner has ``GROUP BY region`` + outer dimensional
             threshold groups by ``region``**: the outer
             ``COUNT(*)`` becomes per-region row count of the
             inner result (= number of inner groups per region).
             Useful when validating a metric ABOUT pre-aggregated
             segments — e.g. "every region must have at least 50
             distinct product groups in the daily summary."
           - **Inner has ``GROUP BY product`` (different column)
             + outer groups by ``region``**: the outer aggregator
             reduces the inner pre-aggregated rows to one value
             per region. ``MAX(row_count)`` becomes "max product
             group size per region" — ``COUNT(*)`` becomes "number
             of distinct products per region." Compose carefully;
             the engine doesn't push down or rewrite, so what you
             write is what runs.

        ``metrics`` (``list[str]``) is the explicit passthrough
        shorthand — equivalent to writing ``aggregations={n:
        f'MAX({n})'}``. Useful when you want the metric names to
        differ from the rule references but still passthrough.
        Mutually exclusive with ``aggregations``.
        """
        if aggregations and metrics:
            raise ValueError(
                "threshold_check: pass either `aggregations` (explicit "
                "expressions) or `metrics` (passthrough column names), "
                "not both."
            )
        # Auto-derive ``metrics`` from the rules when neither
        # ``aggregations`` nor ``metrics`` was supplied. The common
        # ``validate_query`` pattern looks like
        # ``rules=[{'metric': 'rows', ...}, {'metric': 'null_pid_pct', ...}]``;
        # repeating those names in ``metrics=['rows', 'null_pid_pct']``
        # is just ceremony. Auto-derive removes the duplication while
        # keeping the explicit forms available for the cases that need
        # them (mismatched names, custom SQL).
        if aggregations is None and metrics is None and rules:
            metrics = [r["metric"] for r in rules]
        if metrics:
            # Wrap each source column in a no-op aggregation so the
            # collector contract is uniform and Spark is happy when
            # `dimensions` is set (GROUP BY needs aggregates in SELECT).
            aggregations = {nm: f"MAX({nm})" for nm in metrics}
        expressions = dict(aggregations or {})
        parsed_rules = []
        for r in (rules or []):
            thresholds = r.get("thresholds", {})
            parsed_rules.append(
                ThresholdRuleConfig(
                    metric=r["metric"],
                    thresholds=ThresholdLevels(**thresholds),
                )
            )
        kwargs: dict[str, Any] = {
            "collection": AggregationCollectionConfig(
                expressions=expressions,
                dimensions=dimensions,
            ),
            "rules": parsed_rules,
        }
        if notify is not None:
            kwargs["notify"] = NotifyConfig(**notify)
        if name is not None:
            kwargs["name"] = name
        if description is not None:
            kwargs["description"] = description
        return ThresholdValidationConfig(**kwargs)

    @staticmethod
    def drift_check(
        aggregations: dict[str, str] | None = None,
        metrics: list[str] | None = None,
        rules: list[dict[str, Any]] | None = None,
        notify: dict[str, list[str]] | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> HistoricalValidationConfig:
        """Build a historical comparison validation config.

        ``metrics`` is the passthrough shorthand (see
        ``threshold_check`` for full semantics) — pass column names
        when the source query has already aggregated, so you don't
        have to wrap each column in ``MAX(...)`` ceremony.
        """
        if aggregations and metrics:
            raise ValueError(
                "drift_check: pass either `aggregations` or `metrics`, "
                "not both."
            )
        # Auto-derive metrics from rules when neither aggregations nor
        # metrics is given (see ``threshold_check`` for rationale).
        if aggregations is None and metrics is None and rules:
            metrics = [r["metric"] for r in rules]
        if metrics:
            aggregations = {nm: f"MAX({nm})" for nm in metrics}
        expressions = dict(aggregations or {})
        parsed_rules = []
        for r in (rules or []):
            compare = r.get("compare", {})
            thresholds = r.get("thresholds", {})
            parsed_rules.append(
                HistoricalRuleConfig(
                    metric=r["metric"],
                    compare=HistoricalCompareConfig(**compare),
                    thresholds=HistoricalThresholds(**thresholds),
                )
            )
        kwargs: dict[str, Any] = {
            "collection": AggregationCollectionConfig(expressions=expressions),
            "rules": parsed_rules,
        }
        if notify is not None:
            kwargs["notify"] = NotifyConfig(**notify)
        if name is not None:
            kwargs["name"] = name
        if description is not None:
            kwargs["description"] = description
        return HistoricalValidationConfig(**kwargs)

    @staticmethod
    def trend_check(
        metric: str,
        aggregation: str | None = None,
        history_count: int = 90,
        step: str | None = None,
        notify: dict[str, list[str]] | None = None,
        name: str | None = None,
        description: str | None = None,
        **model_kwargs: Any,
    ) -> ForecastValidationConfig:
        """Build a forecast validation config.

        ``step`` is the partition cadence — required, ISO 8601
        (``"P1D"``, ``"P7D"``, ``"PT1H"``). Forecast reads history
        via ``cr.partition_ts − k·step`` and Prophet's ``ds`` axis is
        ``partition_ts``. Pydantic rejects an omitted ``step`` at
        config-construction time so the misconfiguration is loud.

        ``aggregation`` is optional in the common ``validate_query``
        flow where the inner SELECT already produces ``metric`` as a
        column. When omitted, the builder defaults to ``MAX(metric)``
        — a no-op on a single-row aggregate query but keeps the
        collector contract uniform. Pass an explicit expression
        (e.g. ``aggregation="AVG(amount)"``) when running over raw,
        un-aggregated source data.
        """
        if aggregation is None:
            # Passthrough default: source query is expected to expose
            # ``metric`` as a column already (typical
            # ``validate_query`` shape). MAX is a no-op on a 1-row
            # aggregate; explicit form is still available for raw
            # sources.
            aggregation = f"MAX({metric})"
        expressions = {metric: aggregation}
        model_fields: dict[str, Any] = {"history_count": history_count, **model_kwargs}
        if step is not None:
            model_fields["step"] = step
        kwargs: dict[str, Any] = {
            "collection": AggregationCollectionConfig(expressions=expressions),
            "rules": [
                ForecastRuleConfig(
                    metric=metric,
                    model=ForecastModelConfig(**model_fields),
                )
            ],
        }
        if notify is not None:
            kwargs["notify"] = NotifyConfig(**notify)
        if name is not None:
            kwargs["name"] = name
        if description is not None:
            kwargs["description"] = description
        return ForecastValidationConfig(**kwargs)

    @staticmethod
    def shape_check(
        n_records: int = 10000,
        past_dates: int = 3,
        step: str | None = None,
        slice_column: str | None = None,
        slice_value: str | None = None,
        notify: dict[str, list[str]] | None = None,
        name: str | None = None,
        description: str | None = None,
        **model_kwargs: Any,
    ) -> AnomalyDetectionValidationConfig:
        """Build an anomaly detection validation config.

        ``step`` is the partition cadence used to anchor history
        slices via ``slice_value − k·step``. **Required**; pass
        an ISO 8601 duration like ``"P7D"`` or ``"PT1H"``. Omitting
        it raises a first-party ``ValueError`` here so the failure
        surfaces with operator-friendly migration text rather than
        as a deeper Pydantic stack trace.

        ``slice_column`` + ``slice_value`` form the slicing predicate.
        Pass both for the common date-partition case
        (``slice_column="event_dt", slice_value="{{ ds }}"``); the
        sampler auto-generates ``<slice_column> = '<rendered>'`` for
        the current slice and shifts by ``k·step`` per past slice.
        """
        if step is None:
            raise ValueError(
                "shape_check requires step=... (ISO 8601 duration, "
                "e.g. step='P7D' or step='PT1H'). Step anchors past "
                "history slices at slice_value − k·step."
            )
        kwargs: dict[str, Any] = {
            "collection": SampleCollectionConfig(
                n_records=n_records,
                slice_column=slice_column,
                slice_value=slice_value,
                history=SampleHistoryConfig(past_dates=past_dates, step=step),
            ),
            "model": AnomalyModelConfig(
                **{k: v for k, v in model_kwargs.items() if k in AnomalyModelConfig.model_fields}
            ),
        }
        if notify is not None:
            kwargs["notify"] = NotifyConfig(**notify)
        if name is not None:
            kwargs["name"] = name
        if description is not None:
            kwargs["description"] = description
        return AnomalyDetectionValidationConfig(**kwargs)

    @staticmethod
    def pattern_check(
        n_records: int = 10000,
        past_dates: int | None = None,
        step: str | None = None,
        slice_column: str | None = None,
        slice_value: str | None = None,
        history_filters: list[str] | None = None,
        thresholds: dict[str, dict[str, float]] | None = None,
        notify: dict[str, list[str]] | None = None,
        name: str | None = None,
        description: str | None = None,
        **model_kwargs: Any,
    ) -> PatternValidationConfig:
        """Build a pattern-check (Random Forest two-sample) validation config.

        ``step`` is the partition cadence and is **required** for
        both anchor mode (``past_dates`` + ``step``) and explicit
        ``history_filters`` mode. In anchor mode it drives the
        ``cr.partition_ts − k·step`` lookbacks; in filters mode it's
        not consulted at runtime, but config-load enforces presence
        so every rule is reproducible from the YAML alone.

        The history window is defined by either ``past_dates`` (with
        step-based slicing) or ``history_filters`` (explicit SQL
        predicates). If both are supplied, their lengths must match —
        otherwise ``ValueError`` is raised. If neither is supplied,
        ``past_dates`` defaults to 3.
        """
        if step is None:
            raise ValueError(
                "pattern_check requires step=... (ISO 8601 duration, "
                "e.g. step='P7D'). Required even when supplying "
                "history_filters — the field is part of the rule's "
                "config-level identity."
            )
        if history_filters is not None:
            if past_dates is not None and past_dates != len(history_filters):
                # Don't silently rewrite the caller's intent. A caller
                # who passes past_dates=5 + 2 filters has a
                # contradictory spec; emitting a 2-slice config would
                # change class balance, CV feasibility, and alert
                # behavior while appearing to succeed.
                raise ValueError(
                    f"pattern_check: past_dates ({past_dates}) does not match "
                    f"history_filters length ({len(history_filters)}). Omit "
                    f"past_dates when supplying explicit history_filters."
                )
            history = SampleHistoryConfig(
                past_dates=len(history_filters),
                step=step,
                filters=history_filters,
            )
        else:
            history = SampleHistoryConfig(
                past_dates=past_dates if past_dates is not None else 3,
                step=step,
            )

        model_fields = {
            k: v for k, v in model_kwargs.items()
            if k in PatternModelConfig.model_fields
        }
        kwargs: dict[str, Any] = {
            "collection": SampleCollectionConfig(
                n_records=n_records,
                slice_column=slice_column,
                slice_value=slice_value,
                history=history,
            ),
            "model": PatternModelConfig(**model_fields),
            "thresholds": PatternThresholdConfig(**(thresholds or {})),
        }
        if name is not None:
            kwargs["name"] = name
        if notify is not None:
            kwargs["notify"] = NotifyConfig(**notify)
        if description is not None:
            kwargs["description"] = description
        return PatternValidationConfig(**kwargs)

    def health_report(
        self,
        days: int = 30,
        output_path: str | None = None,
    ) -> Any:
        """Generate a data quality health report from system table history.

        Args:
            days: Number of days to include in the report.
            output_path: If provided, write an HTML report to this file.

        Returns:
            HealthReport dataclass with aggregated metrics.
        """
        from qualifire.reporting.health import HealthReporter
        from qualifire.reporting.html_report import generate_health_html

        if not self._storage:
            raise ValueError("System table is required for health reports. Set system_table on init.")

        reporter = HealthReporter(self._storage)
        report = reporter.generate(days=days)

        if output_path:
            generate_health_html(report, output_path=output_path)

        return report

    def interactive_dashboard(
        self,
        output_path: str | None = None,
        days: int = 90,
        inline_plotly_js: bool = False,
        as_iframe: bool = False,
        iframe_height: int = 900,
    ) -> str:
        """Render an interactive Plotly-based dashboard from system table history.

        Drill-down view: pick a dataset → its validations → a single
        validation's per-partition history. Time range filter (default 90
        days) applies across every chart and table. Adapts to OS dark mode.

        Args:
            output_path: If provided, write the self-contained HTML to this
                path.
            days: Default window in days; users can adjust at runtime via the
                in-page input.
            inline_plotly_js: When True, bundle the Plotly JS library inline
                (~3.5 MB heavier) instead of referencing the CDN. Required
                for inline rendering in sandboxed contexts like VS Code's
                notebook webview, where external `<script src="cdn...">`
                loads are blocked. Default False (CDN) keeps standalone
                files tiny — the right choice when serving the file
                via a real browser or any non-sandboxed renderer.
            as_iframe: When True, wrap the dashboard in an
                ``<iframe srcdoc="...">`` so VS Code / JupyterLab notebook
                webviews execute the dashboard JS (which populates the
                dataset/validation dropdowns and renders the charts).
                Without this, ``display(HTML(qf.interactive_dashboard(...)))``
                shows empty dropdowns because the inline ``<script>`` is
                stripped or sandboxed. Pair with ``inline_plotly_js=True``
                for full notebook interactivity.
            iframe_height: Height (px) of the iframe wrapper when
                ``as_iframe=True``. Default 900.

        Returns:
            The full HTML document as a string.
        """
        from qualifire.reporting.html_report import generate_interactive_html

        if not self._storage:
            raise ValueError(
                "System table is required for the interactive dashboard. "
                "Set system_table on Qualifire init."
            )
        return generate_interactive_html(
            self._storage, output_path=output_path, days=days,
            inline_plotly_js=inline_plotly_js,
            as_iframe=as_iframe, iframe_height=iframe_height,
        )

    def _build_notifiers(self, configs: dict[str, Any]) -> dict[str, Notifier]:
        """Instantiate notifiers from config channel definitions."""
        notifiers = {}
        for name, cfg in configs.items():
            if hasattr(cfg, "model_dump"):
                cfg_dict = cfg.model_dump()
            else:
                cfg_dict = dict(cfg)
            channel_type = cfg_dict.pop("type", name)
            cls = get_notifier(channel_type)
            notifiers[name] = cls(**cfg_dict)
        return notifiers

    @staticmethod
    def _fan_out_dataset_notify(
        validations: list[Any],
        notify: dict[str, list[str]] | None,
    ) -> list[Any]:
        """Fan a dataset-level `notify` dict onto every validation
        whose own `notify` was not explicitly set.

        Returns a new list of validation configs (caller's originals
        are not mutated). When ``notify`` is None, returns the
        input list unchanged.

        "Not explicitly set" is detected via Pydantic's
        ``model_fields_set``: builders pass ``notify`` to the config
        constructor only when the caller supplied a value
        (``notify is not None``). Configs built without ``notify=``
        therefore have ``"notify" not in model_fields_set`` and
        inherit the dataset-level default. Callers that want to
        explicitly opt out of inheritance pass ``notify={}`` to the
        builder, which sets the field.
        """
        if notify is None:
            return validations
        dataset_notify_cfg = NotifyConfig(**notify)
        out: list[Any] = []
        for vc in validations:
            try:
                fields_set = getattr(vc, "model_fields_set", set())
                if "notify" in fields_set:
                    out.append(vc)
                else:
                    out.append(vc.model_copy(update={"notify": dataset_notify_cfg}))
            except Exception:
                out.append(vc)
        return out
