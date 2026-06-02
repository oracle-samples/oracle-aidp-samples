"""Tests for WAP pattern executor."""

from unittest.mock import MagicMock, call

import pytest

from qualifire.core.context import QualifireContext
from qualifire.wap.pattern import WAPExecutor


class TestWAPExecutor:
    def test_staging_name_generation(self):
        name = WAPExecutor._generate_staging_name("catalog.schema.sales")
        assert name.startswith("catalog.schema._qf_staging_sales_")

    def test_sql_driven_write_wraps_select(self):
        """write_sql is a SELECT; executor wraps it as CREATE TABLE staging AS ..."""
        backend = MagicMock()
        ctx = QualifireContext()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            sql="SELECT * FROM raw.t WHERE date = '{{ ds }}'",
            context=ctx,
        )
        executor.write()
        backend.execute_sql.assert_called_once()
        sql_arg = backend.execute_sql.call_args[0][0]
        assert sql_arg.startswith(f"CREATE TABLE {executor.staging_table} AS ")
        assert "SELECT * FROM raw.t" in sql_arg

    def test_df_driven_write(self):
        backend = MagicMock()
        mock_df = MagicMock()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            df=mock_df,
            write_options={"mode": "overwrite"},
        )
        executor.write()
        backend.write_df.assert_called_once_with(
            mock_df, executor.staging_table, {"mode": "overwrite"}
        )

    def test_staging_table_auto_generated(self):
        """staging_table is always derived from target_table."""
        backend = MagicMock()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
        )
        assert executor.staging_table.startswith("cat.schema._qf_staging_prod_")

    def test_publish_append(self):
        backend = MagicMock()
        # SQL fallback path — Pandas-style backend (no spark).
        backend.spark = None
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
        )
        staging = executor.staging_table
        executor.publish()
        sql_arg = backend.execute_sql.call_args[0][0]
        assert "INSERT INTO cat.schema.prod" in sql_arg
        assert staging in sql_arg
        backend.drop_table.assert_called_with(staging)

    def test_publish_overwrite(self):
        backend = MagicMock()
        backend.spark = None
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            write_options={"mode": "overwrite"},
        )
        executor.publish()
        sql_arg = backend.execute_sql.call_args[0][0]
        assert "INSERT OVERWRITE" in sql_arg

    def test_publish_spark_uses_dataframe_writer(self):
        """Spark-capable backends route publish through the DataFrame
        writer (sub-feature D). Honours ``format`` / ``partitionBy`` /
        format-specific options that the SQL path silently dropped."""
        backend = MagicMock()
        # Spark-capable backend: configure the .table().write... chain
        spark = MagicMock()
        backend.spark = spark
        backend.table_exists.return_value = True
        df = MagicMock()
        spark.table.return_value = df
        writer = MagicMock()
        df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        writer.option.return_value = writer

        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            write_options={
                "format": "delta",
                "mode": "overwrite",
                "partitionBy": ["region", "month"],
                "mergeSchema": "true",
            },
        )
        executor.publish()

        spark.table.assert_called_once_with(executor.staging_table)
        writer.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("overwrite")
        writer.partitionBy.assert_called_once_with("region", "month")
        # mergeSchema option was forwarded.
        writer.option.assert_any_call("mergeSchema", "true")
        writer.saveAsTable.assert_called_once_with("cat.schema.prod")

    def test_publish_allow_create_false_missing_target_raises(self):
        from qualifire.wap.pattern import WAPPublishError

        backend = MagicMock()
        backend.spark = MagicMock()
        backend.table_exists.return_value = False  # target missing

        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            allow_create=False,
        )
        with pytest.raises(WAPPublishError, match="does not exist"):
            executor.publish()

    def test_rollback(self):
        backend = MagicMock()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
        )
        staging = executor.staging_table
        executor.rollback()
        backend.drop_table.assert_called_with(staging)

    def test_write_requires_sql_or_df(self):
        backend = MagicMock()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
        )
        with pytest.raises(ValueError, match="sql.*or df"):
            executor.write()


class TestWAPCaching:
    def test_df_cached_and_registered_as_temp_view(self):
        """With cache=True on a Spark-capable backend, df is persisted and
        registered via the Backend Protocol method (P4.5)."""
        backend = MagicMock()
        backend.spark = MagicMock()  # Spark-capable
        mock_df = MagicMock()
        mock_df.persist.return_value = mock_df
        mock_df.count.return_value = 100

        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            df=mock_df,
            cache=True,
            cache_storage_level="MEMORY_ONLY",
        )
        executor.write()

        mock_df.persist.assert_called_once()
        # P4.5: registration goes through backend.register_table(name, df).
        backend.register_table.assert_called_once()
        registered_name, registered_df = backend.register_table.call_args[0]
        assert registered_name == executor.staging_table
        assert registered_df is mock_df  # the persisted df
        mock_df.count.assert_called_once()
        assert "." not in executor.staging_table

    def test_sql_cached_and_registered(self):
        """SQL-driven WAP with cache=True executes SELECT, caches, registers via Protocol."""
        backend = MagicMock()
        backend.spark = MagicMock()  # Spark-capable
        mock_result_df = MagicMock()
        mock_result_df.persist.return_value = mock_result_df
        mock_result_df.count.return_value = 50
        backend.execute_sql.return_value = mock_result_df

        ctx = QualifireContext()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            sql="SELECT * FROM raw.t",
            context=ctx,
            cache=True,
        )
        executor.write()

        backend.execute_sql.assert_called_once_with("SELECT * FROM raw.t")
        mock_result_df.persist.assert_called_once()
        backend.register_table.assert_called_once()

    def test_cached_rollback_unpersists(self):
        """Rollback with cache=True unpersists the DataFrame and drops the view."""
        backend = MagicMock()
        backend.spark = MagicMock()
        mock_df = MagicMock()
        mock_df.persist.return_value = mock_df
        mock_df.count.return_value = 10

        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            df=mock_df,
            cache=True,
        )
        executor.write()
        executor.rollback()

        mock_df.unpersist.assert_called_once()
        # P4.5: cleanup goes through drop_temp_view (not drop_table).
        backend.drop_table.assert_not_called()
        backend.drop_temp_view.assert_called_once_with(executor.staging_table)

    def test_cached_publish_unpersists(self):
        """Publish with cache=True writes from view via DataFrame
        writer (sub-feature D), then unpersists and drops."""
        backend = MagicMock()
        spark = MagicMock()
        backend.spark = spark
        backend.table_exists.return_value = True
        mock_df = MagicMock()
        mock_df.persist.return_value = mock_df
        mock_df.count.return_value = 10

        # Set up the publish-side df that spark.table returns.
        publish_df = MagicMock()
        spark.table.return_value = publish_df
        writer = MagicMock()
        publish_df.write = writer
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        writer.option.return_value = writer

        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            df=mock_df,
            cache=True,
        )
        executor.write()
        view_name = executor.staging_table

        executor.publish()

        # Publish ran via DataFrame writer (sub-feature D), not SQL.
        spark.table.assert_called_with(view_name)
        writer.saveAsTable.assert_called_once_with("cat.schema.prod")
        mock_df.unpersist.assert_called_once()
        # P4.5: cleanup uses backend.drop_temp_view, not raw SQL.
        backend.drop_temp_view.assert_called_once_with(view_name)

    def test_non_cached_wap_unchanged(self):
        """cache=False (default) uses original CREATE TABLE behavior."""
        backend = MagicMock()
        ctx = QualifireContext()
        executor = WAPExecutor(
            backend=backend,
            target_table="cat.schema.prod",
            sql="SELECT 1",
            context=ctx,
            cache=False,
        )
        executor.write()
        sql_arg = backend.execute_sql.call_args[0][0]
        assert sql_arg.startswith("CREATE TABLE")
