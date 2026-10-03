"""Tests for the Informatica PowerCenter crawler."""

import os
import tempfile

from infa2aidp.crawlers.informatica_crawler import (
    InformaticaCrawler,
    InfaConnectionConfig,
    InfaCrawlResult,
    InfaObject,
)


# ── Informatica Crawler Tests ──


class TestInfaConnectionConfig:
    def test_defaults(self):
        config = InfaConnectionConfig(host="infa-server")
        assert config.host == "infa-server"
        assert config.port == 7333  # WSH default
        assert config.security_domain == "Native"
        assert config.pmrep_path == "pmrep"

    def test_custom_config(self):
        config = InfaConnectionConfig(
            host="infa-server",
            port=7343,  # HTTPS
            username="admin",
            password="secret",
            repository="DEV_REPO",
            domain="Domain_infa",
            wsh_url="https://infa-server:7343/wsh/services",
        )
        assert config.repository == "DEV_REPO"
        assert config.wsh_url == "https://infa-server:7343/wsh/services"


class TestInfaCrawlResult:
    def test_empty_result(self):
        result = InfaCrawlResult()
        assert result.folders == []
        assert result.mappings == []
        assert result.errors == []

    def test_result_with_data(self):
        result = InfaCrawlResult(
            folders=["SDE", "SIL"],
            mappings=[
                InfaObject(name="m_extract_orders", folder="STAGING", object_type="mapping"),
                InfaObject(name="m_load_orders", folder="WAREHOUSE", object_type="mapping"),
            ],
        )
        assert len(result.folders) == 2
        assert len(result.mappings) == 2


class TestInformaticaCrawler:
    def setup_method(self):
        self.config = InfaConnectionConfig(
            host="infa-server",
            username="admin",
            password="secret",
            repository="DEV_REPO",
        )
        self.crawler = InformaticaCrawler(self.config)

    def test_init(self):
        assert self.crawler.config.host == "infa-server"
        assert self.crawler._access_method is None
        assert self.crawler._pmrep_connected is False

    def test_disconnect_noop(self):
        self.crawler.disconnect()

    def test_pmrep_output_parsing(self):
        output = """Informatica(r) PMREP, version [10.5.0]
Copyright (c) Informatica LLC

mapping m_Extract_Customer
mapping m_Load_Orders
mapping m_Transform_Products

listobjects completed successfully."""
        lines = self.crawler._parse_pmrep_output(output)
        assert len(lines) == 3
        assert "mapping m_Extract_Customer" in lines[0]

    def test_generate_inventory_report(self):
        result = InfaCrawlResult(
            folders=["SDE", "SIL"],
            mappings=[
                InfaObject(name="m_extract_customers", folder="STAGING"),
                InfaObject(name="m_extract_orders", folder="STAGING"),
                InfaObject(name="m_load_customer_dim", folder="WAREHOUSE"),
            ],
            workflows=[
                InfaObject(name="wf_SDE_Daily", folder="SDE"),
            ],
            sessions=[
                InfaObject(name="s_SDE_Customers", folder="SDE"),
            ],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            path = f.name

        try:
            self.crawler._access_method = "pmrep"
            self.crawler.generate_inventory_report(result, path)
            with open(path, encoding="utf-8") as f:
                content = f.read()

            assert "Informatica Repository Inventory" in content
            assert "SDE" in content
        finally:
            os.unlink(path)
