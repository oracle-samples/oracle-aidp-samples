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
