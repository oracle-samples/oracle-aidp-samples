"""Write-Audit-Publish pattern executor."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from qualifire.backends.base import Backend
from qualifire.core.context import QualifireContext

logger = logging.getLogger(__name__)


class WAPPublishError(RuntimeError):
    """Raised when WAP publish fails for a configuration reason
    (e.g. ``allow_create=False`` and target doesn't exist).

    Distinct from runtime errors raised by the backend (catalog
    permissions, schema mismatches) — those propagate as the
    backend's native exceptions.
    """


class WAPExecutor:
    """Orchestrates the Write-Audit-Publish pattern.

    SQL-driven: User provides a SELECT query. The executor wraps it as
        CREATE TABLE <staging> AS <select_sql>
    and manages the staging table lifecycle internally.

    DataFrame-driven: Writes DataFrame to an auto-generated staging table.

    When cache=True (DataFrame-driven):
        - Caches the DataFrame with the configured storage level
        - Registers it as a temp view (createOrReplaceTempView)
        - Validations run against the cached temp view (fast repeated reads)
        - Publish reads from the temp view into the target table
        - Rollback/cleanup unpersists the cache and drops the temp view

    AUDIT: Caller runs validations against the staging table/view.
    PUBLISH: On success, INSERT INTO target FROM staging, drop staging.
    ROLLBACK: On failure, drop staging.

    Args:
        backend: Data backend.
        target_table: Final destination table.
        write_sql: SELECT query for SQL-driven WAP.
        write_options: Spark write options (mode, partitionBy, etc.).
        context: Jinja context for rendering SQL.
        df: DataFrame for DataFrame-driven WAP.
        cache: Whether to cache the staging data for faster validation.
        cache_storage_level: Spark StorageLevel name (default: MEMORY_AND_DISK).
    """

    def __init__(
        self,
        backend: Backend,
        target_table: str,
        sql: str | None = None,
        write_options: dict[str, Any] | None = None,
        context: QualifireContext | None = None,
        df: Any = None,
        cache: bool = False,
        cache_storage_level: str = "MEMORY_AND_DISK",
        allow_create: bool = True,
    ):
        self.backend = backend
        self.target_table = target_table
        self.staging_table = self._generate_staging_name(target_table)
        self.sql = sql
        self.write_options = write_options or {}
        self.context = context
        self.df = df
        self.cache = cache
        self.cache_storage_level = cache_storage_level
        self.allow_create = allow_create
        self._cached_df: Any = None
        self._already_cleaned: bool = False
        # Engine drains this list into ``QualifireResult.engine_warnings``
        # after publish/rollback returns. `qualifire.wap_cleanup` rows
        # land here when cleanup fails after a successful publish.
        from qualifire.core.models import ValidationResult
        self.cleanup_warnings: list[ValidationResult] = []

    @staticmethod
    def _generate_staging_name(target_table: str) -> str:
        suffix = uuid.uuid4().hex[:8]
        parts = target_table.rsplit(".", 1)
        if len(parts) == 2:
            return f"{parts[0]}._qf_staging_{parts[1]}_{suffix}"
        return f"_qf_staging_{target_table}_{suffix}"

    def write(self) -> None:
        """WRITE phase: populate the staging table."""
        if self.sql:
            rendered_select = self.context.render(self.sql)
            if self.cache:
                self._write_sql_cached(rendered_select)
            else:
                sql = f"CREATE TABLE {self.staging_table} AS {rendered_select}"
                logger.info("WAP WRITE (SQL): %s", sql[:200])
                self.backend.execute_sql(sql)
        elif self.df is not None:
            if self.cache:
                self._write_df_cached(self.df)
            else:
                logger.info("WAP WRITE (DataFrame): -> %s", self.staging_table)
                self.backend.write_df(self.df, self.staging_table, self.write_options)
        else:
            raise ValueError(
                "WAP requires one of: sql=, sql_file= (via WAPConfig), or df="
            )

    def _write_sql_cached(self, rendered_select: str) -> None:
        """Execute the SELECT, cache result, register as temp view."""
        df = self.backend.execute_sql(rendered_select)
        self._write_df_cached(df)

    def _write_df_cached(self, df: Any) -> None:
        """Cache a DataFrame and register as a temp view for staging.

        Two paths:
          - Spark-capable backend (`backend.spark` present): persist
            the DataFrame and register through the backend Protocol.
            If PySpark is missing entirely, raise — the caller
            explicitly asked for caching on a Spark-capable backend
            and silently downgrading would be misleading.
          - Pandas-style backend (no `.spark`): caching is a semantic
            no-op (data is already in memory). Register the
            DataFrame as a temp view and proceed without persist().
        """
        view_name = self.staging_table.replace(".", "_")
        spark = getattr(self.backend, "spark", None)
        if spark is None:
            # PandasBackend / equivalent: register as a temp view, no persist.
            self.backend.register_table(view_name, df)
            self.staging_table = view_name
            self._cached_df = df  # used as a non-None marker for cleanup
            logger.info("WAP WRITE (cached, in-memory): %s", self.staging_table)
            return

        try:
            from pyspark import StorageLevel
        except ImportError as e:
            raise RuntimeError(
                "PySpark is required for cache=True on a Spark-capable "
                "WAP path; install with: pip install 'qualifire[spark]'"
            ) from e

        level = getattr(StorageLevel, self.cache_storage_level, StorageLevel.MEMORY_AND_DISK)
        self._cached_df = df.persist(level)
        self.backend.register_table(view_name, self._cached_df)
        self.staging_table = view_name
        self._cached_df.count()
        logger.info(
            "WAP WRITE (cached): %s (%s)", self.staging_table, self.cache_storage_level
        )

    def publish(self) -> None:
        """PUBLISH phase: move data from staging to target, drop staging.

        Spark-capable backends route the publish through the
        DataFrame writer (sub-feature D): ``df.write.format(...).
        options(...).saveAsTable(target)``. This honours
        ``write_options['format']`` (Delta / Parquet / ...),
        ``partitionBy``, format-specific options
        (``mergeSchema=true`` etc.) — none of which the prior
        ``INSERT INTO/OVERWRITE TABLE`` path consumed. Pandas-style
        backends keep the SQL fallback (no ``.spark`` attribute).

        ``allow_create`` (default True) gates target auto-creation.
        Operators in catalog/RBAC-controlled environments set
        ``allow_create=False`` to restore the strict "target must
        pre-exist" contract; missing target then raises
        ``WAPPublishError`` rather than silently creating a table.

        ``_cleanup`` runs in a ``finally`` so a publish failure
        still releases staging. If both the publish and cleanup
        raise, the publish exception propagates and the cleanup
        error is logged + recorded as a ``qualifire.wap_cleanup``
        engine warning so operators see the orphan-staging signal
        alongside the primary failure.
        """
        logger.info("WAP PUBLISH: %s -> %s", self.staging_table, self.target_table)
        try:
            spark = getattr(self.backend, "spark", None)
            if spark is not None:
                self._publish_spark()
            else:
                self._publish_sql()
        finally:
            self._cleanup()
        logger.info("WAP PUBLISH complete, staging cleaned up")

    def _publish_spark(self) -> None:
        """DataFrame-writer publish path (Spark-capable backends).

        Honours every key in ``write_options``: ``format``, ``mode``,
        ``partitionBy`` (string or list), and any vendor-specific
        option (Delta ``mergeSchema``, Parquet ``compression``,
        etc.) via ``writer.option(k, v)``.
        """
        mode = self.write_options.get("mode", "append")
        target_exists = False
        try:
            target_exists = bool(self.backend.table_exists(self.target_table))
        except Exception:
            # If the existence probe itself fails, fall through to
            # the writer attempt — saveAsTable will surface a clean
            # error if the catalog actually rejects.
            target_exists = False
        if not target_exists and not self.allow_create:
            raise WAPPublishError(
                f"WAP publish: target table {self.target_table!r} does "
                f"not exist and WAPConfig.allow_create=False. Either "
                f"create the target table or set allow_create=True."
            )
        df = self.backend.spark.table(self.staging_table)
        writer = df.write
        fmt = self.write_options.get("format")
        if fmt:
            writer = writer.format(fmt)
        writer = writer.mode(mode)
        partition_by = self.write_options.get("partitionBy")
        if partition_by is not None:
            cols = (
                list(partition_by)
                if isinstance(partition_by, (list, tuple))
                else [partition_by]
            )
            writer = writer.partitionBy(*cols)
        for k, v in self.write_options.items():
            if k in ("format", "mode", "partitionBy"):
                continue
            writer = writer.option(k, v)
        writer.saveAsTable(self.target_table)

    def _publish_sql(self) -> None:
        """Pandas-style backend publish path (SQL fallback)."""
        mode = self.write_options.get("mode", "append")
        if mode == "overwrite":
            sql = f"INSERT OVERWRITE TABLE {self.target_table} SELECT * FROM {self.staging_table}"
        else:
            sql = f"INSERT INTO {self.target_table} SELECT * FROM {self.staging_table}"
        self.backend.execute_sql(sql)

    def rollback(self) -> None:
        """ROLLBACK phase: drop the staging table/view."""
        logger.info("WAP ROLLBACK: dropping %s", self.staging_table)
        self._cleanup()

    def _cleanup(self) -> None:
        """Unpersist cached DataFrame and drop staging table/view.

        Idempotent: a second call (e.g. if both the outer
        ``_run_wap`` rollback and an inner publish ``finally`` fire)
        is a no-op. Implemented by clearing the cached-df marker on
        first call. Independent try blocks ensure unpersist failure
        doesn't skip the drop.
        """
        if self._already_cleaned:
            return
        self._already_cleaned = True

        if self._cached_df is not None:
            try:
                # Pandas-style cached marker has no `.unpersist`; skip.
                if hasattr(self._cached_df, "unpersist"):
                    self._cached_df.unpersist()
                    logger.debug("Unpersisted cached DataFrame")
            except Exception as e:
                # Surface as a cleanup warning but keep going to drop.
                self._record_cleanup_warning(
                    "qualifire.wap_cleanup",
                    f"Unpersist failed: {e}",
                )
            try:
                self.backend.drop_temp_view(self.staging_table)
            except Exception as e:
                self._record_cleanup_warning(
                    "qualifire.wap_cleanup",
                    f"drop_temp_view failed for {self.staging_table}: {e}",
                )
            self._cached_df = None
        else:
            try:
                self.backend.drop_table(self.staging_table)
            except Exception as e:
                self._record_cleanup_warning(
                    "qualifire.wap_cleanup",
                    f"drop_table failed for {self.staging_table}: {e}",
                )

    def _record_cleanup_warning(self, base_name: str, message: str) -> None:
        """Append a cleanup warning the engine will drain into
        ``QualifireResult.engine_warnings``."""
        from qualifire.core.models import Severity, ValidationResult

        self.cleanup_warnings.append(ValidationResult(
            validation_name=base_name,
            validation_type="engine",
            severity=Severity.WARNING,
            message=message,
            validation_base_name=base_name,
            details={"staging_table": self.staging_table},
        ))
