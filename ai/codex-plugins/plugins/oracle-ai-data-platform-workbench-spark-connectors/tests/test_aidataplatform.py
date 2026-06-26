"""Unit tests for the AIDP `aidataplatform` Spark format option builder."""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT,
    aidataplatform_options,
)


class TestAidpFormatConstant:
    def test_format_name(self):
        assert AIDP_FORMAT == "aidataplatform"


class TestAidataplatformOptionsBasic:
    def test_minimum_options_is_just_type(self):
        out = aidataplatform_options(type="POSTGRESQL")
        assert out == {"type": "POSTGRESQL"}

    def test_type_is_required(self):
        with pytest.raises(ValueError):
            aidataplatform_options(type="")
        with pytest.raises(TypeError):
            aidataplatform_options()  # type: ignore[call-arg]


class TestAidataplatformOptionsRdbms:
    """The five RDBMS connectors share an identical option shape."""

    @pytest.mark.parametrize(
        "type_const",
        ["POSTGRESQL", "MYSQL", "MYSQL_HEATWAVE", "SQLSERVER", "ORACLE_DB"],
    )
    def test_rdbms_shape(self, type_const):
        out = aidataplatform_options(
            type=type_const,
            host="db.example.com",
            port=5432,
            user="alice",
            password="s3cret",
            schema="analytics",
            table="orders",
        )
        assert out == {
            "type": type_const,
            "host": "db.example.com",
            "port": "5432",  # ports are strings — Spark options are stringly typed
            "user.name": "alice",
            "password": "s3cret",
            "schema": "analytics",
            "table": "orders",
        }

    def test_database_name_when_set(self):
        out = aidataplatform_options(
            type="ORACLE_DB",
            host="oradb.example.com",
            port=1521,
            database_name="ORCLPDB1",
            user="hr_app",
            password="pw",
            schema="HR",
            table="EMPLOYEES",
        )
        assert out["database.name"] == "ORCLPDB1"

    def test_database_name_absent_when_unset(self):
        out = aidataplatform_options(type="POSTGRESQL", host="x", port=5432)
        assert "database.name" not in out


class TestAidataplatformOptionsExtras:
    def test_extras_merge(self):
        out = aidataplatform_options(
            type="FUSION_BICC",
            user="svc",
            password="pw",
            schema="ERP",
            extra={
                "fusion.service.url": "https://pod.example.com",
                "fusion.external.storage": "BICC_AIDP_DEMO",
                "datastore": "FscmTopModelAM.SomePvo",
            },
        )
        assert out["fusion.service.url"] == "https://pod.example.com"
        assert out["fusion.external.storage"] == "BICC_AIDP_DEMO"
        assert out["datastore"] == "FscmTopModelAM.SomePvo"

    def test_extras_override_main_keys(self):
        # If callers pass an `extra` that conflicts with a named arg, the extra wins.
        # This is intentional — it's the escape hatch for connector-specific overrides.
        out = aidataplatform_options(
            type="POSTGRESQL",
            host="originally-this",
            extra={"host": "overridden"},
        )
        assert out["host"] == "overridden"

    def test_no_extra_means_no_extra(self):
        out = aidataplatform_options(type="POSTGRESQL", host="h", port=5432)
        # No mystery keys leak in
        assert set(out.keys()) == {"type", "host", "port"}


class TestAidataplatformOptionsKafka:
    def test_kafka_aidataplatform_shape(self):
        out = aidataplatform_options(
            type="KAFKA",
            user="svc",
            password="pw",
            schema="raw",
            extra={
                "bootstrap.servers": "kafka.example.com:9092",
                "ssl.enabled": "true",
                "host.name.verification": "true",
                "message": "orders:0",
            },
        )
        assert out["type"] == "KAFKA"
        assert out["bootstrap.servers"] == "kafka.example.com:9092"
        assert out["message"] == "orders:0"
        # No host/port/table for the kafka shape
        assert "host" not in out
        assert "port" not in out


class TestAidataplatformOptionsGenericRest:
    def test_generic_rest_shape(self):
        out = aidataplatform_options(
            type="GENERIC_REST",
            user="alice",
            password="pw",
            schema="default",
            extra={
                "base.url": "http://api.internal/v1",
                "manifest.url": "http://api.internal/v1/manifest",
                "auth.type": "basic",
                "api": "getOrdersByOrderID",
                "derived.property.orderNo": "12345",
            },
        )
        assert out["type"] == "GENERIC_REST"
        assert out["api"] == "getOrdersByOrderID"
        assert out["derived.property.orderNo"] == "12345"
        # No host/port/table for the rest shape
        assert "host" not in out


class TestAidataplatformOptionsV050NewTypes:
    """Tests for the 5 new connector types added in v0.5.0 (oracle-samples PR #46)."""

    def test_peoplesoft_shape(self):
        out = aidataplatform_options(
            type="ORACLE_PEOPLESOFT",
            host="psft-db.example.com",
            port=1521,
            database_name="HCMDB",
            user="PS_USER",
            password="pw",
            schema="SYSADM",
            table="PS_JOB",
        )
        assert out["type"] == "ORACLE_PEOPLESOFT"
        assert out["host"] == "psft-db.example.com"
        assert out["port"] == "1521"
        assert out["database.name"] == "HCMDB"
        assert out["schema"] == "SYSADM"
        assert out["table"] == "PS_JOB"

    def test_siebel_shape(self):
        out = aidataplatform_options(
            type="ORACLE_SIEBEL",
            host="siebel-db.example.com",
            port=1521,
            database_name="SIEBELDB",
            user="SIEBEL_USER",
            password="pw",
            schema="SIEBEL",
            table="S_CONTACT",
        )
        assert out["type"] == "ORACLE_SIEBEL"
        assert out["schema"] == "SIEBEL"
        assert out["table"] == "S_CONTACT"

    def test_salesforce_uses_sforce_type(self):
        """Salesforce connector type literal is ``SFORCE``, not ``SALESFORCE``."""
        out = aidataplatform_options(
            type="SFORCE",
            host="login.salesforce.com",
            port=443,
            database_name="myorg",
            user="user@example.com",
            password="pwd+token",
            schema="SFORCE",
            table="Account",
        )
        assert out["type"] == "SFORCE"
        assert out["table"] == "Account"

    def test_hive_no_database_name(self):
        """Hive connector uses ``schema`` directly (Hive database name)."""
        out = aidataplatform_options(
            type="HIVE",
            host="hs2.example.com",
            port=10000,
            user="hive",
            password="hivepw",
            schema="sales_db",
            table="transactions",
        )
        assert out["type"] == "HIVE"
        assert out["schema"] == "sales_db"
        assert "database.name" not in out  # Hive doesn't use database.name

    def test_oracle_db_with_catalog_id(self):
        """v0.5.0 pattern: use catalog.id and skip host/port/user/password."""
        out = aidataplatform_options(
            type="ORACLE_DB",
            schema="HR",
            table="EMPLOYEES",
            extra={"catalog.id": "my-oracle-catalog"},
        )
        assert out == {
            "type": "ORACLE_DB",
            "schema": "HR",
            "table": "EMPLOYEES",
            "catalog.id": "my-oracle-catalog",
        }

    def test_pushdown_sql_extra(self):
        """v0.5.0 pattern: pushdown.sql replaces schema/table option building."""
        out = aidataplatform_options(
            type="ORACLE_PEOPLESOFT",
            host="h",
            port=1521,
            database_name="HCMDB",
            user="u",
            password="p",
            extra={"pushdown.sql": "SELECT 1 FROM DUAL"},
        )
        assert out["pushdown.sql"] == "SELECT 1 FROM DUAL"
        # When pushdown.sql is used, schema/table aren't in the dict
        assert "schema" not in out
        assert "table" not in out

    def test_write_mode_merge_with_keys(self):
        """v0.5.0 pattern: write.mode=MERGE requires write.merge.keys."""
        out = aidataplatform_options(
            type="ORACLE_DB",
            host="h", port=1521, database_name="DB",
            user="u", password="p", schema="HR", table="EMPLOYEES",
            extra={"write.mode": "MERGE", "write.merge.keys": "EMPLOYEE_ID"},
        )
        assert out["write.mode"] == "MERGE"
        assert out["write.merge.keys"] == "EMPLOYEE_ID"

    def test_manifest_path_for_rest(self):
        """v0.5.0 pattern: REST connector accepts manifest.path (workspace/volume)."""
        out = aidataplatform_options(
            type="GENERIC_REST",
            extra={"manifest.path": "/Volumes/myvol/manifests/my_api.json"},
        )
        assert out["type"] == "GENERIC_REST"
        assert out["manifest.path"] == "/Volumes/myvol/manifests/my_api.json"
