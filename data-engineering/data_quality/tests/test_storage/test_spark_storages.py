"""Tests for Delta, ExternalCatalog, and JDBC storage backends (mocked Spark)."""

from unittest.mock import MagicMock, call

import pytest

from tests.conftest import MockDataFrame, MockRow


# ======================================================================
# ExternalCatalogStorage
# ======================================================================


class TestExternalCatalogStorage:
    def _make(self):
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()
        spark.sql.return_value = MockDataFrame()
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        return storage, spark

    @staticmethod
    def _save_as_table_mock(spark):
        """Wire createDataFrame → .write.format().mode().saveAsTable
        on the supplied ``spark`` mock and return the leaf
        ``saveAsTable`` mock for assertion / side-effect setup.

        The eager-creation path is:
            empty = spark.createDataFrame([], schema)
            empty.write.format("aidataplatform").mode("ignore") \\
                       .saveAsTable(table)
        """
        mock_empty_df = MagicMock()
        spark.createDataFrame.return_value = mock_empty_df
        return (
            mock_empty_df
            .write
            .format.return_value
            .mode.return_value
            .saveAsTable
        )

    def test_initialize_creates_table_via_aidp_format_save_as_table_no_create_schema(self):
        """``initialize()`` eagerly creates the system table via the
        AIDP ``aidataplatform`` data source explicitly
        (``empty.write.format("aidataplatform").mode("ignore")
        .saveAsTable(<3-part-name>)``).

        The explicit format is required because the AIDP external-
        catalog feature wires the Spark catalog plugin for **read**
        paths only — CREATE TABLE DDL (raw ``spark.sql`` or
        V2-native ``writeTo().create()``) falls through to Spark's
        session-default source (Delta on AIDP Workbench) and
        lands in Object Storage instead of the underlying ADW.
        Forcing the writer's format to ``aidataplatform``
        bypasses Spark's catalog routing and goes straight
        through the AIDP connector, which honors the 3-part
        name's resolved catalog connection and issues native
        Oracle ``CREATE TABLE``.

        ``initialize()`` **does not** issue ``CREATE SCHEMA`` —
        operators provision schemas out-of-band on every catalog
        this backend targets.
        """
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                # Probe fails with a non-permission exception → falls
                # through to aidataplatform saveAsTable.
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        storage.initialize()

        all_sqls = [c[0][0] for c in spark.sql.call_args_list]
        # DESCRIBE TABLE fired (probe).
        assert any("DESCRIBE TABLE" in s for s in all_sqls), \
            f"Expected DESCRIBE TABLE probe; got {all_sqls}"
        # No CREATE TABLE / CREATE SCHEMA SQL was emitted — eager
        # creation goes through the DataFrame writer chain, not
        # ``spark.sql``.
        assert not any("CREATE TABLE" in s for s in all_sqls), \
            f"CREATE TABLE DDL must not fire; got {all_sqls}"
        assert not any("CREATE SCHEMA" in s for s in all_sqls), \
            f"CREATE SCHEMA should not be issued; got {all_sqls}"
        # The writer chain pinned end-to-end so a future regression
        # that drops ``.format("aidataplatform")`` fails here —
        # that's the load-bearing piece that routes to ADW.
        mock_empty_df = spark.createDataFrame.return_value
        mock_empty_df.write.format.assert_called_with("aidataplatform")
        mock_empty_df.write.format.return_value.mode.assert_called_with("ignore")
        save_as_table.assert_called_once_with("cat.schema.history")
        # createDataFrame was called with an explicit StructType so
        # the AIDP connector sees the full column set + declared
        # types up-front.
        spark.createDataFrame.assert_called_once()
        ca = spark.createDataFrame.call_args
        rows_arg = ca[0][0] if ca[0] else ca.kwargs.get("data")
        assert rows_arg == [], "eager creation must pass an empty row list"
        from pyspark.sql.types import StructType
        schema_arg = ca[0][1] if len(ca[0]) > 1 else ca.kwargs.get("schema")
        assert isinstance(schema_arg, StructType), \
            "createDataFrame must receive an explicit StructType"

    def test_initialize_rejects_non_three_part_identifier(self):
        """The external_catalog backend requires
        ``catalog.schema.table`` (exactly 3 parts). 1-part /
        2-part / 4+ part identifiers don't fit the AIDP / Unity
        Catalog / Iceberg-REST shape this backend targets and
        raise at ``initialize()`` with a clear migration
        pointer.

        Leading / trailing / double dots collapse to 4-part
        names with empty segments and are caught here (the
        count check runs before the per-segment regex), giving
        the operator a clearer "shape is wrong" diagnostic than
        the segment-level error would."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()
        spark.sql.return_value = MockDataFrame()
        for bad in (
            "just_a_name",                 # 1-part
            "schema.table",                # 2-part
            "server.cat.schema.tbl",       # 4-part
            "a.b.c.d.e",                   # 5-part
            ".cat.schema.tbl",             # leading dot → 4-part
            "cat..schema.tbl",             # double dot → 4-part
            "cat.schema.tbl.",             # trailing dot → 4-part
            # SQL-clause injection that collapses to 2 parts
            # because there's no dot between the LOCATION clause
            # and the table-segment dot.
            "default LOCATION '/tmp/x'.tbl",
        ):
            storage = ExternalCatalogStorage(spark=spark, table=bad)
            with pytest.raises(
                ValueError, match="not a 3-part identifier"
            ):
                storage.initialize()

    def test_write_results(self):
        storage, spark = self._make()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        storage.write_results([{
            "run_id": "r1", "run_timestamp": "2024-01-01T00:00:00",
            "owner": "o", "bu": "b", "dataset_name": "ds",
            "table_name": "t", "metric_name": "m",
            "metric_value": 42.0, "collection_type": "agg",
            "validation_name": "v", "validation_type": "threshold",
            "validation_status": "PASS", "validation_message": "ok",
            "notification_channel": None, "notification_status": None,
            "details_json": {"key": "val"},
        }])
        # write_results now goes through the DataFrame writer (saveAsTable)
        # rather than SQL ``INSERT INTO ... VALUES`` — external catalogs
        # like AIDP / Iceberg REST reject the SQL VALUES form.
        spark.createDataFrame.assert_called_once()
        rows_arg = spark.createDataFrame.call_args[0][0]
        assert any(r.run_id == "r1" for r in rows_arg)
        # An explicit StructType must be passed — without it,
        # all-None optional columns crash inference and fresh
        # tables drift from COLUMN_DEFINITIONS types (R6 review fix).
        schema_arg = spark.createDataFrame.call_args.kwargs.get("schema")
        assert schema_arg is not None, "createDataFrame must receive an explicit schema"
        from pyspark.sql.types import (
            DoubleType,
            StringType,
            StructType,
            TimestampType,
        )
        assert isinstance(schema_arg, StructType)
        field_types = {f.name: type(f.dataType) for f in schema_arg.fields}
        # Spot-check the load-bearing types: metric_value (DOUBLE),
        # the timestamp columns (TIMESTAMP), and any STRING column.
        assert field_types["metric_value"] is DoubleType
        assert field_types["run_timestamp"] is TimestampType
        assert field_types["partition_ts"] is TimestampType
        assert field_types["validation_status"] is StringType
        mock_df.write.mode.assert_called_with("append")
        # Phase 1 contract: writes go through ``insertInto`` with
        # ``skip.oos.staging=true`` baked in via the
        # ``_SYSTEM_TABLE_WRITE_OPTIONS`` dict. ``saveAsTable``
        # would update table metadata which insert-only credentials
        # can't do; ``insertInto`` is schema-strict and works with
        # the lowest-privilege identity (DBA pre-created the table,
        # qualifire only has INSERT).
        mode_writer = mock_df.write.mode.return_value
        # ``.options(**dict)`` was called with skip.oos.staging.
        mode_writer.options.assert_called_with(**{"skip.oos.staging": "true"})
        options_writer = mode_writer.options.return_value
        options_writer.insertInto.assert_called_with("cat.schema.history")
        # ``saveAsTable`` is NOT called any more — pin so a future
        # regression doesn't quietly re-introduce it.
        assert not mode_writer.saveAsTable.called, \
            "saveAsTable must not be called; use insertInto instead"

    def test_write_results_handles_all_null_optional_columns(self):
        """A row where every optional column is ``None`` must succeed
        — without an explicit schema, ``createDataFrame`` raises
        'cannot determine type' on inference. R6 review blocking
        finding."""
        storage, spark = self._make()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        storage.write_results([{
            "run_id": "r1", "run_timestamp": "2024-01-01T00:00:00",
            "owner": "o", "bu": "b", "dataset_name": "ds",
            "table_name": "t", "metric_name": "count",
            "metric_value": 100.0, "collection_type": "aggregation",
            # Every optional column None — would crash without explicit schema.
            "validation_name": None, "validation_type": None,
            "validation_status": None, "validation_message": None,
            "notification_channel": None, "notification_status": None,
            "details_json": None, "expected_value": None,
            "actual_value_text": None,
            "dataset_description": None, "validation_description": None,
        }])
        spark.createDataFrame.assert_called_once()
        # Schema is supplied — no crash on None-typed fields.
        assert spark.createDataFrame.call_args.kwargs.get("schema") is not None

    def test_write_empty_is_noop(self):
        storage, spark = self._make()
        storage.write_results([])
        spark.createDataFrame.assert_not_called()

    def test_initialize_skips_create_when_table_exists(self):
        """Insert-only operator path — DBA pre-created the table and
        qualifire's identity only has INSERT privilege. The
        DESCRIBE TABLE probe succeeds; eager creation
        (``saveAsTable``) is NOT invoked.

        Round-3 codex plan-review BLOCKER fix (low-privilege regression).
        """
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()
        # DESCRIBE returns successfully (table exists), so probe
        # confirms existence and the saveAsTable path is skipped.
        spark.sql.return_value = MockDataFrame()
        save_as_table = self._save_as_table_mock(spark)
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        storage.initialize()

        all_sqls = [c[0][0] for c in spark.sql.call_args_list]
        assert any("DESCRIBE TABLE" in s for s in all_sqls)
        assert not any("CREATE TABLE" in s for s in all_sqls), \
            f"CREATE TABLE must not fire when DESCRIBE confirms existence; got {all_sqls}"
        save_as_table.assert_not_called()

    def test_write_results_raises_on_malformed_timestamp(self):
        """A non-ISO ``run_timestamp`` / ``partition_ts`` is a
        producer-side data-contract bug — silently coercing it to
        NULL would corrupt drift / forecast lookbacks that key on
        these fields. R6 round-2 review fix: raise ValueError
        with column + value context."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()
        spark.sql.return_value = MockDataFrame()
        spark.createDataFrame.return_value = MagicMock()
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")

        with pytest.raises(ValueError, match=r"run_timestamp.*non-ISO"):
            storage.write_results([{
                "run_id": "r1",
                "run_timestamp": "yesterday at noon",  # not ISO
                "owner": "o", "bu": "b", "dataset_name": "ds",
                "table_name": "t", "metric_name": "m",
                "metric_value": 1.0, "collection_type": "agg",
            }])

    def test_initialize_create_permission_denied_raises_with_dba_hint(self):
        """When DESCRIBE confirms missing AND ``saveAsTable`` fails
        with ``PERMISSION DENIED`` / ``INSUFFICIENT PRIVILEGES`` /
        ``ORA-01031``, raise ``QualifireSystemTableError`` pointing
        at the DBA-pre-create remediation. Operators with insert-
        only credentials and a not-yet-pre-created table see the
        right fix."""
        from qualifire.core.exceptions import QualifireSystemTableError
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        save_as_table.side_effect = RuntimeError("ORA-01031: insufficient privileges")
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        with pytest.raises(QualifireSystemTableError, match="Ask your DBA"):
            storage.initialize()

    def test_initialize_oracle_table_or_view_does_not_exist_falls_through_to_create(self):
        """Round-4 codex plan-review test gap: Oracle / AIDP surfaces
        ``ORA-00942: table or view does not exist`` — note no
        ``NOT FOUND`` substring after uppercase. The probe must NOT
        treat this as a privilege error and must fall through to
        eager saveAsTable creation."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("ORA-00942: table or view does not exist")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        storage.initialize()
        save_as_table.assert_called_once_with(
            "cat.schema.history"
        )
        # (save_as_table called above is the leaf)

    def test_initialize_unsupported_describe_falls_through_to_create(self):
        """Round-4 codex plan-review RISK fix: REST DSV2 catalogs
        without DESCRIBE support raise ``UNSUPPORTED_OPERATION``
        (no NOT FOUND, no privilege phrase). Falls through to
        eager saveAsTable creation."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("UNSUPPORTED_OPERATION: DESCRIBE not implemented")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        storage.initialize()
        save_as_table.assert_called_once_with(
            "cat.schema.history"
        )
        # (save_as_table called above is the leaf)

    def test_initialize_describe_permission_denied_skips_create(self):
        """Round-4 codex plan-review BLOCKER reinforcement:
        DESCRIBE rejected with explicit privilege phrase →
        skip eager creation (table exists, restricted DESCRIBE)."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("INSUFFICIENT PRIVILEGES on cat.schema.history")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        storage.initialize()
        save_as_table.assert_not_called()

    def test_initialize_describe_resource_error_falls_through_to_create(self):
        """Round-5 codex plan-review BLOCKER fix (token over-match
        guard): bare tokens like ``DENIED`` / ``INSUFFICIENT`` would
        flip ``table_exists=True`` for unrelated transient errors.
        Tightened phrase matcher must NOT fire on resource errors
        — falls through to eager saveAsTable creation."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                # Contains "denied" and "insufficient" as bare tokens but
                # no matching phrase — must fall through to create.
                raise RuntimeError("connection denied by gateway: insufficient resources")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        storage.initialize()
        save_as_table.assert_called_once_with(
            "cat.schema.history"
        )
        # (save_as_table called above is the leaf)

    def test_initialize_create_namespace_not_found_raises_with_namespace_hint(self):
        """Round-5 codex plan-review RISK fix: when ``saveAsTable``
        fails with ``SCHEMA_NOT_FOUND`` / ``NAMESPACE NOT FOUND`` /
        ``ORA-00959`` / ``ORA-04043``, raise the namespace-creation
        hint."""
        from qualifire.core.exceptions import QualifireSystemTableError
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        save_as_table.side_effect = RuntimeError("SCHEMA_NOT_FOUND: cat.schema")
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        with pytest.raises(
            QualifireSystemTableError,
            match=r"Namespace 'cat\.schema'.*does not exist.*ask your DBA",
        ):
            storage.initialize()

    def test_initialize_create_read_only_catalog_raises_with_dba_hint(self):
        """Round-5 codex plan-review RISK fix: ``saveAsTable`` against
        a read-only catalog raises with the read-only-mirror hint."""
        from qualifire.core.exceptions import QualifireSystemTableError
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        save_as_table.side_effect = RuntimeError(
            "catalog is READ ONLY: cannot create table"
        )
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        with pytest.raises(
            QualifireSystemTableError,
            match=r"catalog is read-only.*writable",
        ):
            storage.initialize()

    def test_initialize_create_dual_phrase_namespace_plus_privilege(self):
        """Round-6 codex plan-review BLOCKER fix (classifier order):
        Glue / Ranger errors carry BOTH 'DATABASE NOT FOUND' AND
        'INSUFFICIENT PRIVILEGES' in the same message. Namespace
        bucket wins; operator gets the upstream-cause remediation."""
        from qualifire.core.exceptions import QualifireSystemTableError
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        save_as_table.side_effect = RuntimeError(
            "DATABASE_NOT_FOUND: cat.schema "
            "(also: INSUFFICIENT PRIVILEGES upstream)"
        )
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        with pytest.raises(QualifireSystemTableError) as excinfo:
            storage.initialize()
        # Namespace hint, not privilege hint:
        assert "ask your DBA to create" in str(excinfo.value)
        assert "Ask your DBA to pre-create the table" not in str(excinfo.value)

    def test_initialize_create_race_table_already_exists_treated_as_success(self):
        """V2 ``DataFrameWriterV2.create()`` raises
        ``TableAlreadyExistsException`` (fail-if-exists semantics)
        if the table appears between the DESCRIBE probe and the
        create. The narrow race window must not surface as an
        error — the table now exists, the next ``write_results``
        will use it, the run continues. Probe-then-create is the
        primary guard against this case; this is the belt-and-
        suspenders fallback."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        # Simulate Spark's TableAlreadyExistsException via message substring —
        # Spark wraps Java exceptions through Py4J, so the operator-visible
        # form is the message text not the Python exception class.
        save_as_table.side_effect = RuntimeError(
            "[TABLE_ALREADY_EXISTS] Cannot create table cat.schema.history "
            "because it already exists."
        )
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        # Must NOT raise — race-window resolution is a no-op success.
        storage.initialize()
        # Probe + create were both attempted (proof we hit the race-window
        # branch, not the success-path-with-create-skipped branch).
        save_as_table.assert_called_once_with("cat.schema.history")

    def test_initialize_create_dual_phrase_readonly_plus_privilege(self):
        """Round-6 codex plan-review BLOCKER fix: dual-phrase
        (READ ONLY + PERMISSION DENIED) routes to read-only bucket
        not privilege bucket."""
        from qualifire.core.exceptions import QualifireSystemTableError
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()

        def _sql(stmt):
            if "DESCRIBE TABLE" in stmt:
                raise RuntimeError("table not found")
            return MockDataFrame()

        spark.sql.side_effect = _sql
        save_as_table = self._save_as_table_mock(spark)
        save_as_table.side_effect = RuntimeError(
            "READ ONLY catalog: PERMISSION DENIED on writes"
        )
        storage = ExternalCatalogStorage(spark=spark, table="cat.schema.history")
        with pytest.raises(QualifireSystemTableError) as excinfo:
            storage.initialize()
        assert "catalog is read-only" in str(excinfo.value)

    def test_permission_pattern_does_not_match_embedded_substrings(self):
        """Round-6 codex plan-review RISK fix (regex word boundaries):
        the matcher uses ``\\b`` anchors so embedded substrings like
        ``12-ORA-010312`` don't trigger the privilege bucket."""
        from qualifire.storage.external_catalog import _PERMISSION_PATTERN

        # Genuine matches:
        assert _PERMISSION_PATTERN.search("ORA-01031: insufficient privileges")
        assert _PERMISSION_PATTERN.search("PERMISSION DENIED on table")
        assert _PERMISSION_PATTERN.search("INSUFFICIENT PRIVILEGE on schema")

        # Embedded substrings — must NOT match (would falsely flip
        # `table_exists=True` for unrelated errors):
        assert not _PERMISSION_PATTERN.search("12-ORA-010312 is a different code")
        assert not _PERMISSION_PATTERN.search("NORA-01031Z is not the privilege code")

    def test_no_create_schema_calls_in_source(self):
        """Round-2 self-review RISK fix: the durable replacement
        for the brittle CI grep. Reads
        ``qualifire/storage/external_catalog.py`` as text and
        asserts no live ``CREATE SCHEMA`` substring remains."""
        from pathlib import Path

        import qualifire.storage.external_catalog as mod
        source = Path(mod.__file__).read_text(encoding="utf-8")
        # Strip the docstring / comments that mention CREATE SCHEMA
        # historically — only operator-visible references should
        # remain (none in source code that issues SQL). Tighten by
        # asserting no ``self._spark.sql(... CREATE SCHEMA ...)``
        # call exists.
        # Substring check is intentionally loose on doc/comment
        # mentions but tight on actual SQL emission (the prefix
        # '"CREATE SCHEMA' or "spark.sql(.*CREATE SCHEMA").
        import re
        sql_call_pattern = re.compile(
            r"\.sql\s*\(\s*[fr]?[\"'].*CREATE\s+SCHEMA",
            re.IGNORECASE | re.DOTALL,
        )
        match = sql_call_pattern.search(source)
        assert match is None, (
            f"Found a live CREATE SCHEMA SQL emission in "
            f"external_catalog.py — Phase 1 contract is no CREATE "
            f"SCHEMA: {match.group()!r}"
        )

    def test_system_table_spark_schema_full_coverage(self):
        """Lock the schema contract end-to-end: every
        ``SYSTEM_TABLE_COLUMNS`` entry appears in the StructType,
        every field is nullable, every COLUMN_DEFINITIONS type
        maps to the expected Spark type. R6 round-2 review fix —
        spot-checks let regressions slip past."""
        from pyspark.sql.types import (
            DoubleType,
            StringType,
            StructType,
            TimestampType,
        )

        from qualifire.storage.base import (
            COLUMN_DEFINITIONS,
            SYSTEM_TABLE_COLUMNS,
        )
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        storage = ExternalCatalogStorage(
            spark=MagicMock(), table="cat.schema.history"
        )
        schema = storage._system_table_spark_schema()

        assert isinstance(schema, StructType)

        # Names match exactly, in order.
        actual_names = [f.name for f in schema.fields]
        assert actual_names == list(SYSTEM_TABLE_COLUMNS), (
            "schema fields must mirror SYSTEM_TABLE_COLUMNS order"
        )

        # Every field nullable — write_results passes None for
        # optional columns and NULL is the no-dimension form.
        for f in schema.fields:
            assert f.nullable, f"{f.name} must be nullable"

        # Type-by-type assertion — catches a future
        # COLUMN_DEFINITIONS addition that misses the type_map.
        type_for = {
            "STRING": StringType,
            "DOUBLE": DoubleType,
            "TIMESTAMP": TimestampType,
        }
        for f in schema.fields:
            generic = COLUMN_DEFINITIONS[f.name][0]
            expected = type_for[generic]
            assert isinstance(f.dataType, expected), (
                f"{f.name} should be {expected.__name__} per "
                f"COLUMN_DEFINITIONS, got {type(f.dataType).__name__}"
            )

    def test_initialize_rejects_malformed_segment_in_3_part_identifier(self):
        """A 3-part name where any segment fails the Spark
        unquoted-identifier regex raises with the segment named.
        Empty / leading-dot / trailing-dot / double-dot cases
        change the part count so they're rejected by the count
        check (see ``test_initialize_rejects_non_three_part
        _identifier``); this test covers the remaining vectors:
        embedded whitespace, leading digits, hyphens, semicolons,
        and SQL comment markers — all of which would interpolate
        directly into the raw CREATE SCHEMA DDL."""
        from qualifire.storage.external_catalog import ExternalCatalogStorage

        spark = MagicMock()
        spark.sql.return_value = MockDataFrame()
        bad_cases = (
            # Whitespace-only / embedded whitespace
            "cat. .tbl",
            " cat.schema.tbl",
            "cat.schema .tbl",
            "cat.\tschema.tbl",
            # Leading digit / non-identifier characters
            "1cat.schema.tbl",
            "cat.schema-name.tbl",
            # SQL-clause injection vectors (3-part, bad chars)
            "cat.schema.tbl; DROP TABLE x",
            "cat.schema.tbl --comment",
        )
        for bad in bad_cases:
            storage = ExternalCatalogStorage(spark=spark, table=bad)
            with pytest.raises(ValueError, match="malformed identifier segment"):
                storage.initialize()

    def test_read_metric_history(self):
        storage, spark = self._make()
        spark.sql.return_value = MockDataFrame([
            {"metric_name": "m", "metric_value": 100.0,
             "run_timestamp": "2024-01-01", "validation_status": "PASS"},
        ])
        rows = storage.read_metric_history(table_name="t", metric_name="m")
        assert len(rows) == 1
        assert rows[0]["metric_value"] == 100.0

    def test_read_latest_run(self):
        storage, spark = self._make()
        spark.sql.return_value = MockDataFrame([{"run_id": "r1", "dataset_name": "ds"}])
        row = storage.read_latest_run("ds")
        assert row["run_id"] == "r1"

    def test_read_latest_run_empty(self):
        storage, spark = self._make()
        spark.sql.return_value = MockDataFrame([])
        assert storage.read_latest_run("missing") is None

    def test_read_validation_history(self):
        storage, spark = self._make()
        spark.sql.return_value = MockDataFrame([
            {"validation_name": "v", "validation_type": "t",
             "validation_status": "PASS", "validation_message": "ok",
             "run_timestamp": "2024-01-01"},
        ])
        rows = storage.read_validation_history("ds", "v")
        assert len(rows) == 1


# ======================================================================
# DeltaStorage
# ======================================================================


class TestDeltaStorage:
    def _make(self):
        from qualifire.storage.delta_storage import DeltaStorage

        spark = MagicMock()
        spark.sql.return_value = MockDataFrame()
        spark.createDataFrame.return_value = MagicMock()
        storage = DeltaStorage(spark=spark, table="cat.delta.history")
        return storage, spark

    def test_initialize_creates_delta_table(self):
        storage, spark = self._make()
        storage.initialize()
        first_sql = spark.sql.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS cat.delta.history" in first_sql
        assert "USING DELTA" in first_sql

    def test_write_results(self):
        storage, spark = self._make()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        storage.write_results([{
            "run_id": "r1", "run_timestamp": "2024-01-01",
            "owner": "o", "bu": "b", "dataset_name": "ds",
            "table_name": "t", "metric_name": "m",
            "metric_value": 42.0, "collection_type": "agg",
            "validation_name": "v", "validation_type": "threshold",
            "validation_status": "PASS", "validation_message": "ok",
            "notification_channel": None, "notification_status": None,
            "details_json": {"key": "val"},
        }])
        spark.createDataFrame.assert_called_once()
        mock_df.write.mode.assert_called_with("append")

    def test_compact(self):
        storage, spark = self._make()
        storage.compact()
        sql = spark.sql.call_args[0][0]
        assert "OPTIMIZE cat.delta.history" in sql

    def test_read_metric_history(self):
        storage, spark = self._make()
        spark.sql.return_value = MockDataFrame([
            {"metric_name": "m", "metric_value": 50.0,
             "run_timestamp": "2024-01-01", "validation_status": "PASS"},
        ])
        rows = storage.read_metric_history(table_name="t", metric_name="m")
        assert len(rows) == 1


# ======================================================================
# JDBCStorage
# ======================================================================


class TestJDBCStorage:
    @staticmethod
    def _full_schema_fields():
        """Build a Spark-schema-compatible stand-in (StructField-like)
        with every SYSTEM_TABLE column so ``_ensure_table``'s
        post-round-3 name check AND post-round-6 type check are
        satisfied. The round-6 validator uses real ``isinstance`` on
        ``DataType`` subclasses, so we hand it real ``StringType`` /
        ``DoubleType`` instances rather than MagicMocks."""
        from unittest.mock import MagicMock as _MM

        from pyspark.sql.types import DoubleType, StringType

        from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS

        fields = []
        for col in SYSTEM_TABLE_COLUMNS:
            f = _MM()
            f.name = col
            # ``metric_value`` is declared DOUBLE; every other column
            # (including the ISO-8601 ``*_timestamp``/``*_at`` fields)
            # is STRING by design — see _ensure_table docstring.
            generic_type = COLUMN_DEFINITIONS[col][0]
            f.dataType = DoubleType() if generic_type == "DOUBLE" else StringType()
            fields.append(f)
        return fields

    def _make(self):
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        # Default: simulate an existing, fully-schema-compatible table
        # so the ping + existence check + schema validation all pass.
        spark.read.jdbc.return_value = MockDataFrame(schema_fields=self._full_schema_fields())
        spark.createDataFrame.return_value = MagicMock()
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:oracle:thin:@host:1521/svc",
            connection_properties={"user": "u", "password": "p"},
        )
        return storage, spark

    def test_initialize_connectivity_check(self):
        storage, spark = self._make()
        storage.initialize()
        # First call is connectivity ping, second is table existence check
        assert spark.read.jdbc.call_count >= 1

    def test_initialize_rejects_table_with_missing_columns(self):
        """Round-3 review: DBA-created tables must be schema-validated,
        not blindly accepted. A table missing newer columns would have
        passed the existence check and later fail during insert/select
        with opaque driver errors. initialize() now introspects the
        Spark-inferred schema and rejects incompatible tables up front."""
        from unittest.mock import MagicMock as _MM

        from qualifire.storage.jdbc_storage import JDBCStorage

        # Existing table with only a subset of required columns.
        partial_fields = []
        for col in ("run_id", "metric_name", "metric_value"):
            f = _MM()
            f.name = col
            partial_fields.append(f)

        spark = MagicMock()
        call_counter = {"n": 0}

        def read_side_effect(**kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                # ping succeeds
                return MockDataFrame([{"ping": 1}])
            # existence check returns a DF with missing columns
            return MockDataFrame(schema_fields=partial_fields)

        spark.read.jdbc.side_effect = read_side_effect

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        with pytest.raises(RuntimeError, match="missing required column"):
            storage.initialize()

    def test_write_results(self):
        storage, spark = self._make()
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df

        storage.write_results([{
            "run_id": "r1", "run_timestamp": "2024-01-01",
            "owner": "o", "bu": "b", "dataset_name": "ds",
            "table_name": "t", "metric_name": "m",
            "metric_value": 10.0, "collection_type": "agg",
            "validation_name": "v", "validation_type": "t",
            "validation_status": "PASS", "validation_message": "ok",
            "notification_channel": None, "notification_status": None,
            "details_json": {},
        }])
        mock_df.write.jdbc.assert_called_once()

    def test_read_metric_history(self):
        storage, spark = self._make()
        spark.read.jdbc.return_value = MockDataFrame([
            {"metric_name": "m", "metric_value": 75.0,
             "run_timestamp": "2024-01-01", "validation_status": "PASS"},
        ])
        rows = storage.read_metric_history(table_name="t", metric_name="m")
        assert len(rows) == 1

    def test_read_latest_run_empty(self):
        storage, spark = self._make()
        spark.read.jdbc.return_value = MockDataFrame([])
        assert storage.read_latest_run("missing") is None

    def test_initialize_raises_on_connectivity_failure(self):
        """Regression: a broken JDBC URL / missing driver / bad creds
        must fail the run, not let the engine proceed with a storage
        backend that silently swallows every write later. See the
        Phase-2 adversarial review: the prior code logged a warning
        and returned, which hid misconfiguration behind 'PASS' runs
        that wrote nothing to the system table."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        spark.read.jdbc.side_effect = RuntimeError("Connection refused")

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://bad-host/db",
            connection_properties={"user": "u", "password": "p"},
        )
        with pytest.raises(RuntimeError, match="JDBC connectivity check failed"):
            storage.initialize()

    def test_initialize_raises_on_table_creation_failure(self):
        """If the ping succeeds but auto-creation fails (e.g. no CREATE
        privilege), initialize() must raise. Otherwise the first
        ``write_results`` call fails after the run has already been
        reported as successful."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        # Ping succeeds; table-exists check raises (table missing);
        # createDataFrame().write.jdbc raises (no create perms).
        ping_df = MockDataFrame([{"ping": 1}])
        call_counter = {"n": 0}

        def read_side_effect(**kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                return ping_df
            raise RuntimeError("Table 'qf_history' does not exist")

        spark.read.jdbc.side_effect = read_side_effect
        mock_write = MagicMock()
        mock_write.jdbc.side_effect = PermissionError("INSUFFICIENT_PRIVILEGE")
        mock_df = MagicMock()
        mock_df.write = mock_write
        spark.createDataFrame.return_value = mock_df

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        with pytest.raises(RuntimeError, match="Could not create JDBC table"):
            storage.initialize()

    def test_read_health_data_builds_cutoff_predicate(self):
        """``qualifire report`` must work on JDBC too. The original
        Phase-2 branch was missing ``read_health_data`` on JDBCStorage,
        so every JDBC-backed report call would have hit AttributeError
        at runtime. Verify the predicate shape — portable string
        comparison on ``run_timestamp`` — rather than a dialect-
        specific date-math function."""
        storage, spark = self._make()
        spark.read.jdbc.return_value = MockDataFrame([
            {"dataset_name": "ds", "validation_type": "threshold",
             "validation_status": "PASS", "run_timestamp": "2024-06-10",
             "validation_name": "v"},
        ])

        rows = storage.read_health_data(days=7)

        assert len(rows) == 1
        called_query = spark.read.jdbc.call_args.kwargs["table"]
        assert "record_type = 'validation'" in called_query
        assert "run_timestamp >=" in called_query
        # No DB-specific date arithmetic leaked through.
        assert "DATEADD" not in called_query.upper()
        assert "INTERVAL" not in called_query.upper()

    def test_ensure_table_declares_timestamps_as_string(self):
        """Round-2 review: the prior schema declared TIMESTAMP columns
        but ``write_results`` only ever ships ISO-8601 strings, so
        insert behavior relied on each driver's implicit string→
        timestamp cast (lenient on MySQL/PostgreSQL, strict/mis-
        storing on Oracle/SQL Server). Declaring these columns as
        STRING keeps the schema honest about what's actually written
        and mirrors how the SQLite backend stores the same fields as
        TEXT. This test pins the decision."""
        from pyspark.sql.types import DoubleType, StringType

        from qualifire.storage.jdbc_storage import JDBCStorage

        captured = {}

        def capture_create(rows, schema):
            captured["schema"] = schema
            mock_df = MagicMock()
            # The ping succeeds, the existence check fails, then
            # createDataFrame is called for the create path.
            mock_df.write.jdbc = MagicMock()
            return mock_df

        spark = MagicMock()
        # ping OK, existence check fails -> table creation path
        call_counter = {"n": 0}

        def read_jdbc(**kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                return MockDataFrame([{"ping": 1}])
            raise RuntimeError("does not exist")

        spark.read.jdbc.side_effect = read_jdbc
        spark.createDataFrame.side_effect = capture_create

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        storage.initialize()

        schema = captured["schema"]
        field_by_name = {f.name: f.dataType for f in schema.fields}
        for col in ("run_timestamp", "collected_at", "validated_at", "notified_at"):
            assert isinstance(field_by_name[col], StringType), (
                f"{col} should be StringType to match what write_results sends"
            )
        assert isinstance(field_by_name["metric_value"], DoubleType)

    def test_ensure_table_falls_through_to_create_when_lazy_probe_defers_error(
        self,
    ):
        """Round-9 adversarial review: Spark's JDBC reader is lazy —
        ``spark.read.jdbc(...)`` returns a DataFrame without
        executing the subquery, so a truly missing table raises only
        when the caller first touches ``.schema`` or calls an action.
        The prior ``_ensure_table`` wrapped just the reader call in
        try/except, so on a real Spark driver the "table missing"
        error surfaced at ``existing_df.schema.fields`` *outside*
        the guard, short-circuiting the "fall through to create
        path" logic. Result: first-time JDBC deployments would
        crash in the bootstrap step this code is supposed to
        support.

        The fix forces schema resolution inside the try. Pin the
        behavior by simulating Spark's laziness: ``spark.read.jdbc``
        returns a stub DataFrame whose ``.schema`` property raises —
        matching what a real driver does for a missing table. The
        guard must swallow that error and reach the create path
        (``createDataFrame().write.jdbc``)."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        # Track what the code does: ping returns a normal df; the
        # existence-check returns a LAZY df that only errors when
        # ``.schema`` is touched (mimicking real Spark JDBC).
        create_called = {"n": 0}

        class _LazyMissingTableDF:
            @property
            def schema(self):
                raise Exception("relation does not exist (lazy parse)")

        ping_df = MockDataFrame([{"ping": 1}])
        call_counter = {"n": 0}

        def read_jdbc(**kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                return ping_df
            return _LazyMissingTableDF()

        def capture_create(*args, **kwargs):
            create_called["n"] += 1
            mock_df = MagicMock()
            mock_df.write.jdbc = MagicMock()
            return mock_df

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        spark.createDataFrame.side_effect = capture_create

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        # Must NOT raise — the guard must absorb the lazy-parse
        # error and create the table.
        storage.initialize()

        assert create_called["n"] == 1, (
            "lazy-parse error on the existence probe must fall through "
            "to the create path, got createDataFrame calls: "
            f"{create_called['n']}"
        )

    def test_read_latest_run_escapes_quotes_in_dataset_name(self):
        """Round-9 adversarial review: ``read_latest_run`` spliced
        ``dataset_name`` directly into SQL, so a name with an
        apostrophe (legitimate, like ``O'Reilly``) would break the
        predicate, and a hostile name could alter the query because
        Spark's JDBC subquery interface is not parameterized. Apply
        the same quote-escape hardening that ``read_metric_history``
        already uses."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        spark.read.jdbc.return_value = MockDataFrame([])
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        storage.read_latest_run(dataset_name="O'Reilly")
        called_query = spark.read.jdbc.call_args.kwargs["table"]
        # Quote must be doubled, not left raw (which would break the
        # predicate) and not stripped (which would silently change
        # the name).
        assert "dataset_name = 'O''Reilly'" in called_query, called_query
        # Guard against naive removal.
        assert "O'Reilly" not in called_query.replace("O''Reilly", "")

    def test_read_validation_history_escapes_quotes_in_both_names(self):
        """Same hardening as ``read_latest_run`` but for both
        ``dataset_name`` and ``validation_name`` — either can
        contain apostrophes."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        spark.read.jdbc.return_value = MockDataFrame([])
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        storage.read_validation_history(
            dataset_name="O'Reilly",
            validation_name="user's-check",
            limit=5,
        )
        called_query = spark.read.jdbc.call_args.kwargs["table"]
        assert "dataset_name = 'O''Reilly'" in called_query, called_query
        assert "validation_name = 'user''s-check'" in called_query, called_query

    def test_read_metric_history_no_step_pushes_limit_down(self):
        """Round-2 review: without LIMIT pushdown, asking for
        ``past_values=3`` on a long-lived system table would scan
        the entire history through JDBC. Verify the subquery the
        backend generates for the non-bucketed path includes both
        ORDER BY and FETCH FIRST so the DB caps remote work."""
        storage, spark = self._make()
        spark.read.jdbc.return_value = MockDataFrame([
            {"metric_name": "m", "metric_value": 10.0,
             "run_timestamp": "2024-06-10", "validation_status": "PASS",
             "record_type": "collection", "is_active": "true"},
        ])

        # Spark-side precedence reorder needs a real SparkContext
        # (see the pre-existing test_read_metric_history limitation);
        # we only care about the JDBC subquery that reached the DB.
        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=3, step=None
            )
        except Exception:
            pass

        called_query = spark.read.jdbc.call_args.kwargs["table"]
        assert "ORDER BY" in called_query
        assert "FETCH FIRST 3 ROWS ONLY" in called_query
        # Dedupe rule pushed down: latest run_timestamp wins, with
        # validation as the record_type tiebreak. Same rule the
        # dashboard JS uses, so SQL-fed history and the dashboard's
        # rendered history agree on the same survivor row.
        assert "run_timestamp DESC" in called_query
        assert "record_type = 'validation'" in called_query

    def test_read_metric_history_with_step_anchors_cutoff_to_max_row(self):
        """Round-3 review: the bucketed path cannot anchor its cutoff
        to ``datetime.now()`` because after long idle periods the real
        baselines are older than ``now - window`` and silently drop
        out, flipping the validator into cold-start mode. The pushdown
        must instead be anchored to MAX(run_timestamp) for the
        (table, metric). This test pins that behavior: given a
        MAX-row from 2024-01-15, the cutoff in the main subquery
        must be relative to that date, not today."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        # Two calls to the JDBC reader: first the MAX query, then the
        # main bucketed subquery. Record what each one was asked.
        calls = []

        def read_jdbc(**kwargs):
            calls.append(kwargs["table"])
            if "MAX(run_timestamp)" in kwargs["table"]:
                return MockDataFrame([{"max_ts": "2024-01-15T12:00:00"}])
            return MockDataFrame(
                [{"metric_name": "m", "metric_value": 10.0,
                  "run_timestamp": "2024-01-14T00:00:00",
                  "validation_status": "PASS",
                  "record_type": "collection",
                  "is_active": "true",
                  "collector_name": "c", "dimension_value": None}]
            )

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )

        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=3, step="P7D"
            )
        except Exception:
            pass  # bucketing step may fail without SparkContext

        assert len(calls) >= 2, "expected MAX query then main query"
        main_query = calls[1]
        # Cutoff must be relative to MAX=2024-01-15, not today's date.
        # 3 * 7D * 3x safety = 63 days earlier ≈ late Nov 2023.
        assert "run_timestamp >=" in main_query
        import re
        m = re.search(r"run_timestamp >= '(\d{4})-(\d{2})-\d{2}T", main_query)
        assert m is not None, f"cutoff not found in: {main_query}"
        year = int(m.group(1))
        # The anchor is 2024-01-15 and we subtract ~63 days, which lands
        # in late Nov 2023. If the code regressed to using ``now()``
        # instead, the year would be the current wall-clock year.
        assert year == 2023, (
            f"cutoff year {year} — expected 2023 (anchored to MAX=2024-01-15), "
            f"a later year means the code regressed to wall-clock anchoring"
        )

    def test_read_metric_history_with_step_and_no_history_short_circuits(self):
        """When MAX(run_timestamp) is NULL (no history yet), avoid the
        second round-trip and return an empty result cheaply."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        calls = []

        def read_jdbc(**kwargs):
            calls.append(kwargs["table"])
            if "MAX(run_timestamp)" in kwargs["table"]:
                return MockDataFrame([{"max_ts": None}])
            # This must be the empty-fallback subquery (``AND 1=0``),
            # not a real bucketed read.
            assert "AND 1=0" in kwargs["table"]
            return MockDataFrame([])

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )

        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=3, step="P7D"
            )
        except Exception:
            pass
        # MAX query + empty-fallback query, no "main" bucketed scan.
        assert any("MAX(run_timestamp)" in c for c in calls)

    def test_initialize_uses_oracle_probe_for_oracle_url(self):
        """Round-4 review: ``SELECT 1 AS ping`` is not valid on Oracle,
        which requires ``FROM DUAL``. Since the JDBC block is
        advertised as Oracle-compatible, the connectivity probe must
        match. Without this, every Oracle deployment would fail at
        initialize() with a parser error before even reaching the
        table-existence check."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        probes = []
        call_counter = {"n": 0}

        def read_side_effect(**kwargs):
            probes.append(kwargs["table"])
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                # probe: succeed so we can observe its shape
                return MockDataFrame([{"ping": 1}])
            # existence check: say "table exists with correct schema"
            return MockDataFrame(schema_fields=self._full_schema_fields())

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_side_effect

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:oracle:thin:@host:1521/svc",
            connection_properties={"user": "u", "password": "p"},
        )
        storage.initialize()
        assert "FROM DUAL" in probes[0], (
            f"Oracle connectivity probe must use FROM DUAL, got: {probes[0]}"
        )

    def test_initialize_uses_bare_select_for_non_oracle_url(self):
        """Paired with the Oracle test: Postgres and friends do NOT
        need ``FROM DUAL`` and typically don't provide a compatible
        ``DUAL`` pseudo-table. Keep the bare-SELECT form for them."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        spark = MagicMock()
        # Default fixture is Oracle-URL; build a Postgres one directly.
        spark.read.jdbc.return_value = MockDataFrame(
            schema_fields=self._full_schema_fields()
        )
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://localhost:5432/qf",
            connection_properties={"user": "u", "password": "p"},
        )
        storage.initialize()
        first_probe = spark.read.jdbc.call_args_list[0].kwargs["table"]
        assert "FROM DUAL" not in first_probe
        assert "SELECT 1 AS ping" in first_probe

    def test_bucketed_read_falls_back_to_unbounded_on_sparse_history(self):
        """Round-4 review: the ``limit * step * 3`` safety window only
        tolerates ~2/3 empty buckets. On sparser schedules (weekly
        metric with ``step='P7D'`` and long freezes), the bounded read
        can return fewer than ``limit`` rows, and bucketing then
        produces fewer than ``limit`` distinct buckets — silently
        flipping the validator into cold-start mode despite valid
        older history existing beyond the cutoff. This test pins the
        fallback: if bounded returns < limit rows, re-read unbounded."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        calls = []

        def read_jdbc(**kwargs):
            query = kwargs["table"]
            calls.append(query)
            if "MAX(run_timestamp)" in query:
                return MockDataFrame([{"max_ts": "2024-06-15T00:00:00"}])
            if "run_timestamp >=" in query:
                # Bounded read: only one row — Spark-side distinct
                # bucket count resolves to 1 (< limit=5).
                return MockDataFrame(
                    [
                        {"metric_name": "m", "metric_value": 10.0,
                         "run_timestamp": "2024-06-14T00:00:00",
                         "validation_status": "PASS",
                         "record_type": "collection",
                         "is_active": "true",
                         "collector_name": "c", "dimension_value": None},
                    ]
                )
            # Unbounded fallback: full history
            return MockDataFrame([
                {"metric_name": "m", "metric_value": 10.0,
                 "run_timestamp": f"2023-{mo:02d}-01T00:00:00",
                 "validation_status": "PASS",
                 "record_type": "collection",
                 "is_active": "true",
                 "collector_name": "c", "dimension_value": None}
                for mo in range(1, 8)
            ])

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        # Stub the Spark-side bucket count since pyspark.sql.functions
        # needs an active SparkContext to build the expression. The
        # scenario under test: the bounded read yielded only 1 distinct
        # bucket, which is less than limit=5 — guard should fall back.
        storage._count_distinct_buckets = lambda df, step_seconds: 1

        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=5, step="P7D"
            )
        except Exception:
            pass

        # Expect: MAX, bounded (with cutoff), unbounded fallback.
        assert len(calls) >= 3, f"expected three queries, got {len(calls)}: {calls}"
        assert "MAX(run_timestamp)" in calls[0]
        assert "run_timestamp >=" in calls[1]
        # Third call is the unbounded fallback — no cutoff, no 1=0.
        assert "run_timestamp >=" not in calls[2]
        assert "1=0" not in calls[2]

    def test_bucketed_read_keeps_bounded_when_sufficient(self):
        """Complement to the sparse-history test: when the bounded read
        already yields >= ``limit`` distinct buckets, skip the unbounded
        fallback. The fast path must stay fast on normal data.

        Five rows spaced 30 days apart produce 5 distinct 7D buckets
        (== limit), so no fallback fires. The bucket-count check runs
        against the Spark DataFrame (``distinct_count_after_select``),
        and the same DataFrame is returned to the caller — no second
        JDBC round-trip needed since Spark DataFrames are lazy and
        re-executed on demand."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        calls = []

        def read_jdbc(**kwargs):
            query = kwargs["table"]
            calls.append(query)
            if "MAX(run_timestamp)" in query:
                return MockDataFrame([{"max_ts": "2024-06-15T00:00:00"}])
            # Bounded read returns 5 rows — the stubbed Spark-side
            # bucket count (see below) resolves to 5 distinct buckets
            # (== limit), so no fallback fires.
            return MockDataFrame(
                [
                    {"metric_name": "m", "metric_value": 10.0,
                     "run_timestamp": f"2024-{mo:02d}-01T00:00:00",
                     "validation_status": "PASS",
                     "record_type": "collection",
                     "is_active": "true",
                     "collector_name": "c", "dimension_value": None}
                    for mo in range(2, 7)
                ]
            )

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        # Stub Spark-side count: 5 distinct buckets satisfy limit=5.
        storage._count_distinct_buckets = lambda df, step_seconds: 5

        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=5, step="P7D"
            )
        except Exception:
            pass

        # MAX + bounded — no unbounded fallback. Spark's bucket count
        # is computed against the lazy bounded DataFrame in-place, so
        # the storage hands the SAME frame back to the caller without
        # re-issuing the subquery.
        assert len(calls) == 2, f"expected two queries, got {len(calls)}: {calls}"
        assert "MAX(run_timestamp)" in calls[0]
        assert "run_timestamp >=" in calls[1]

    def test_bucketed_read_distinct_bucket_check_catches_burst_clustering(self):
        """Round-5 review: raw row count is the wrong invariant. If a
        metric has many runs inside one or two recent buckets (e.g.
        50 runs on the last day of a 7D step) and older valid
        buckets only exist before the cutoff, the bounded read's
        row count easily exceeds ``limit`` while the downstream
        bucketing collapses to too few distinct buckets — silently
        handing the validator a truncated baseline.

        Pin the new distinct-bucket guard: 20 rows, all within 3
        days, form only 1 distinct 7D bucket — triggering the
        unbounded fallback even though 20 >> limit=5."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        calls = []

        def read_jdbc(**kwargs):
            query = kwargs["table"]
            calls.append(query)
            if "MAX(run_timestamp)" in query:
                return MockDataFrame([{"max_ts": "2024-06-15T00:00:00"}])
            if "run_timestamp >=" in query:
                # 20 rows, all within 3 days — exceeds row-count limit
                # but collapses to 1 distinct 7D bucket (Spark-side).
                return MockDataFrame(
                    [
                        {"metric_name": "m", "metric_value": 10.0 + i,
                         "run_timestamp": f"2024-06-{13 + (i % 3):02d}T{i:02d}:00:00",
                         "validation_status": "PASS",
                         "record_type": "collection",
                         "is_active": "true",
                         "collector_name": "c", "dimension_value": None}
                        for i in range(20)
                    ]
                )
            # Unbounded fallback: full history with 5 older buckets.
            return MockDataFrame([
                {"metric_name": "m", "metric_value": 10.0,
                 "run_timestamp": f"2024-0{mo}-01T00:00:00",
                 "validation_status": "PASS",
                 "record_type": "collection",
                 "is_active": "true",
                 "collector_name": "c", "dimension_value": None}
                for mo in range(1, 6)
            ])

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        # Stub Spark-side count: 20 clustered rows produce only 1
        # distinct 7D bucket — Spark would collapse them. Row count
        # alone (20 >> limit=5) would have skipped the fallback.
        storage._count_distinct_buckets = lambda df, step_seconds: 1

        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=5, step="P7D"
            )
        except Exception:
            pass

        # Expect MAX + bounded + unbounded (3). If the old row-count
        # check were still in place, we'd stop after 2 (20 >> 5).
        assert len(calls) >= 3, (
            f"expected fallback to unbounded on clustered-burst data; "
            f"got {len(calls)} queries: {calls}"
        )
        # Last call is the unbounded fallback — no cutoff, no 1=0.
        last = calls[-1]
        assert "run_timestamp >=" not in last, (
            f"final query should be unbounded, got: {last}"
        )
        assert "1=0" not in last

    def test_initialize_rejects_native_timestamp_column_types(self):
        """Round-6 review: ``_ensure_table`` previously only validated
        column *names*, so a DBA-pre-created table with the right names
        but native TIMESTAMP / DATE types would pass init and then
        silently fail (or mis-store) the first real write —
        ``write_results`` sends every non-metric_value column as
        ``str(value)``, and strict JDBC drivers (Oracle, SQL Server)
        reject implicit string→timestamp casts while lenient ones
        (PostgreSQL, MySQL) truncate fractional seconds without
        warning. The round-6 fix extends the validator to reject
        native timestamp column types up front, so the failure is
        immediate and actionable rather than mid-run."""
        from unittest.mock import MagicMock as _MM

        from pyspark.sql.types import DoubleType, StringType, TimestampType

        from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS
        from qualifire.storage.jdbc_storage import JDBCStorage

        # Mimic a DBA-created table: all columns present by name, but
        # ``run_timestamp`` is TIMESTAMP (not STRING as qualifire
        # declares).
        fields = []
        for col in SYSTEM_TABLE_COLUMNS:
            f = _MM()
            f.name = col
            if col == "run_timestamp":
                f.dataType = TimestampType()
            elif COLUMN_DEFINITIONS[col][0] == "DOUBLE":
                f.dataType = DoubleType()
            else:
                f.dataType = StringType()
            fields.append(f)

        call_counter = {"n": 0}

        def read_side_effect(**kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                return MockDataFrame([{"ping": 1}])
            return MockDataFrame(schema_fields=fields)

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_side_effect

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        with pytest.raises(RuntimeError, match="incompatible column type"):
            storage.initialize()

    def test_initialize_rejects_non_numeric_metric_value_column(self):
        """Round-6 review (type-check complement): ``metric_value`` is
        written via ``float(v)`` — the column must be a numeric type.
        A pre-created table with ``metric_value STRING`` would silently
        round-trip through lenient drivers as a stringified number,
        breaking every downstream aggregation and range predicate.
        Reject it at init."""
        from unittest.mock import MagicMock as _MM

        from pyspark.sql.types import DoubleType, StringType

        from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS
        from qualifire.storage.jdbc_storage import JDBCStorage

        fields = []
        for col in SYSTEM_TABLE_COLUMNS:
            f = _MM()
            f.name = col
            if col == "metric_value":
                # Wrong: string instead of numeric
                f.dataType = StringType()
            elif COLUMN_DEFINITIONS[col][0] == "DOUBLE":
                f.dataType = DoubleType()
            else:
                f.dataType = StringType()
            fields.append(f)

        call_counter = {"n": 0}

        def read_side_effect(**kwargs):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                return MockDataFrame([{"ping": 1}])
            return MockDataFrame(schema_fields=fields)

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_side_effect

        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        with pytest.raises(RuntimeError, match="incompatible column type"):
            storage.initialize()

    def test_initialize_accepts_numeric_variants_for_metric_value(self):
        """Not every backend surfaces ``metric_value`` as DOUBLE — Spark
        may upcast NUMERIC/DECIMAL/FLOAT/INT columns. The round-6 type
        check should tolerate any numeric Spark DataType on
        ``metric_value``, since the write side narrows via ``float(v)``
        before binding. Pin the tolerance explicitly so future
        tightening doesn't regress real-world schemas."""
        from unittest.mock import MagicMock as _MM

        from pyspark.sql.types import (
            DecimalType,
            FloatType,
            IntegerType,
            LongType,
            StringType,
        )

        from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS
        from qualifire.storage.jdbc_storage import JDBCStorage

        for variant in (FloatType(), IntegerType(), LongType(), DecimalType(38, 6)):
            fields = []
            for col in SYSTEM_TABLE_COLUMNS:
                f = _MM()
                f.name = col
                if col == "metric_value":
                    f.dataType = variant
                elif COLUMN_DEFINITIONS[col][0] == "DOUBLE":
                    f.dataType = variant
                else:
                    f.dataType = StringType()
                fields.append(f)

            call_counter = {"n": 0}

            def read_side_effect(**kwargs):
                call_counter["n"] += 1
                if call_counter["n"] == 1:
                    return MockDataFrame([{"ping": 1}])
                return MockDataFrame(schema_fields=fields)

            spark = MagicMock()
            spark.read.jdbc.side_effect = read_side_effect
            storage = JDBCStorage(
                spark=spark, table="qf_history",
                jdbc_url="jdbc:postgresql://ok/db",
                connection_properties={"user": "u", "password": "p"},
            )
            # Should not raise — the numeric variant is acceptable.
            storage.initialize()

    def test_bucketed_read_distinct_bucket_check_uses_spark_side_count(self):
        """Round-6 review: the bucket-count guard previously computed
        distinct buckets Python-side via ``datetime.fromisoformat(x).
        timestamp()``, which uses the *Python process timezone* for
        naive datetimes. The downstream bucketing uses Spark's
        ``unix_timestamp()``, which uses the *Spark session timezone*.
        If those disagree (common in ops setups: process TZ = UTC,
        Spark session TZ = America/Los_Angeles, or vice versa), the
        Python-side guard could declare "enough buckets" while Spark
        would actually collapse them into fewer — silently truncating
        the baseline.

        The fix computes the distinct count on the Spark DataFrame
        itself (``select(...).distinct().count()``), so the guard and
        the downstream bucketing use exactly the same TZ basis.

        Pin the behavior structurally: the guard reads its bucket
        count from ``_count_distinct_buckets`` (which goes through
        Spark's ``unix_timestamp``), *not* from Python parsing of
        collected ``run_timestamp`` strings. We stub that method to
        return a Spark-determined count that disagrees with what
        Python would compute from the same rows, and verify the guard
        trusts the Spark-side number.
        """
        from qualifire.storage.jdbc_storage import JDBCStorage

        # Scenario: 5 rows that look Python-side like 5 distinct
        # buckets (30 days apart, way beyond one 7D step). A
        # Python-side bucket count would return 5 and skip the
        # fallback. But Spark — with a session TZ that disagrees
        # with the Python process TZ — returns 1. The guard must
        # defer to Spark and fall back.
        calls = []

        def read_jdbc(**kwargs):
            query = kwargs["table"]
            calls.append(query)
            if "MAX(run_timestamp)" in query:
                return MockDataFrame([{"max_ts": "2024-06-15T00:00:00"}])
            if "run_timestamp >=" in query:
                return MockDataFrame(
                    [
                        {"metric_name": "m", "metric_value": 10.0,
                         "run_timestamp": f"2024-{mo:02d}-01T00:00:00",
                         "validation_status": "PASS",
                         "record_type": "collection",
                         "is_active": "true",
                         "collector_name": "c", "dimension_value": None}
                        for mo in range(2, 7)
                    ]
                )
            # Unbounded fallback
            return MockDataFrame([])

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:postgresql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )
        # Inject the Spark-side count: 1 bucket, disagreeing with the
        # Python parsing of the same rows (which would see 5). Guard
        # must trust Spark.
        storage._count_distinct_buckets = lambda df, step_seconds: 1

        try:
            storage.read_metric_history(
                table_name="t", metric_name="m", limit=5, step="P7D"
            )
        except Exception:
            pass

        # Spark-side count disagrees with what Python would compute,
        # but the guard must trust Spark — expect the unbounded
        # fallback to fire.
        assert len(calls) >= 3, (
            f"guard must trust Spark-side count and fall back, got "
            f"{len(calls)} queries: {calls}"
        )
        assert "run_timestamp >=" not in calls[-1], (
            f"final call should be the unbounded fallback, got: {calls[-1]}"
        )

    def test_bounded_history_read_forces_execution_before_fetch_first_fallback(
        self,
    ):
        """Round-8 review: the ``FETCH FIRST → LIMIT`` fallback in
        ``_bounded_history_read`` relied on ``self._read_jdbc`` raising
        inside the try/except. ``_read_jdbc`` is lazy — it builds a
        Spark DataFrame but doesn't execute the SQL until an action
        runs — so on MySQL and older-PostgreSQL, the ``FETCH FIRST``
        subquery was returned, and the SQL error only surfaced at the
        caller's ``.collect()``, outside the try/except. Result: the
        ``LIMIT`` fallback was dead code and every non-bucketed
        history read on those dialects crashed.
        Pin the behavior: we must trigger a remote parse inside the
        loop so the first attempt actually fails fast and the fallback
        fires. The simplest proxy is ``df.schema``."""
        from qualifire.storage.jdbc_storage import JDBCStorage

        schema_accesses: list[str] = []

        class _LazyDF:
            """A fake Spark DataFrame whose ``.schema`` fails for
            FETCH FIRST subqueries and succeeds for LIMIT ones."""

            def __init__(self, subquery: str):
                self._sq = subquery

            @property
            def schema(self):
                schema_accesses.append(self._sq)
                if "FETCH FIRST" in self._sq:
                    raise Exception("simulated: FETCH FIRST not supported")
                return MagicMock(fields=[])

            def collect(self):
                return []

        read_calls: list[str] = []

        def read_jdbc(**kwargs):
            read_calls.append(kwargs["table"])
            return _LazyDF(kwargs["table"])

        spark = MagicMock()
        spark.read.jdbc.side_effect = read_jdbc
        storage = JDBCStorage(
            spark=spark, table="qf_history",
            jdbc_url="jdbc:mysql://ok/db",
            connection_properties={"user": "u", "password": "p"},
        )

        df = storage._bounded_history_read("table_name='t'", limit=5)

        # We must have probed BOTH subqueries — the FETCH FIRST one
        # (fails) and the LIMIT one (succeeds). If the first attempt's
        # lazy DataFrame had been returned without forcing parse,
        # only one read_jdbc call would appear here.
        assert len(read_calls) == 2, (
            f"fallback must fire inside the loop, got reads: {read_calls}"
        )
        assert "FETCH FIRST" in read_calls[0]
        assert "LIMIT" in read_calls[1]
        assert "FETCH FIRST" in schema_accesses[0]
        # And the caller gets the LIMIT DataFrame back, not the failed
        # FETCH FIRST one.
        assert isinstance(df, _LazyDF)
        assert "LIMIT" in df._sq

    def test_read_health_data_cutoff_matches_naive_writer_basis(self):
        """Round-4 review: ``write_results`` receives ``run_timestamp``
        values the engine produced via naive ``datetime.now().isoformat()``
        (no ``Z``, no ``+00:00``). If the health-report cutoff carried
        a UTC offset, the lexicographic string comparison with stored
        naive strings would be broken around the boundary (offset
        suffix changes sort order, and the underlying wall-clock
        reference is off by the process timezone). The cutoff must
        therefore be a naive ISO string to match the write path."""
        storage, spark = self._make()
        spark.read.jdbc.return_value = MockDataFrame([])

        storage.read_health_data(days=7)
        cutoff_query = spark.read.jdbc.call_args.kwargs["table"]
        import re
        m = re.search(r"run_timestamp >= '([^']+)'", cutoff_query)
        assert m is not None, cutoff_query
        cutoff_iso = m.group(1)
        # Neither trailing 'Z' (UTC shorthand) nor '+HH:MM' (offset);
        # must match what naive isoformat() emits.
        assert not cutoff_iso.endswith("Z"), cutoff_iso
        assert "+" not in cutoff_iso.split("T", 1)[-1], cutoff_iso
