"""Unit tests for JDBC URL and Spark-options builders."""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_connectors.jdbc import (
    build_oracle_jdbc_url,
    spark_jdbc_options_dbtoken,
    spark_jdbc_options_password,
    spark_jdbc_options_wallet,
)


# --- Oracle URL builder -----------------------------------------------------


def test_oracle_tns_alias_url():
    url = build_oracle_jdbc_url(tns_alias="alh_high", tns_admin="/tmp/wallet")
    assert url == "jdbc:oracle:thin:@alh_high?TNS_ADMIN=/tmp/wallet"


def test_oracle_tns_alias_without_admin():
    url = build_oracle_jdbc_url(tns_alias="atp_high")
    assert url == "jdbc:oracle:thin:@atp_high"


def test_oracle_direct_tcps():
    url = build_oracle_jdbc_url(
        host="exacs.priv.subnet.oraclevcn.com",
        port=1522,
        service_name="orcl_pdb1",
    )
    assert url == "jdbc:oracle:thin:@tcps://exacs.priv.subnet.oraclevcn.com:1522/orcl_pdb1"


def test_oracle_direct_plain_tcp():
    url = build_oracle_jdbc_url(
        host="onprem.example.com",
        port=1521,
        service_name="orcl",
        use_tcps=False,
    )
    assert "tcp://" in url
    assert "tcps://" not in url


def test_oracle_url_requires_alias_or_host_and_service():
    with pytest.raises(ValueError):
        build_oracle_jdbc_url()
    with pytest.raises(ValueError):
        build_oracle_jdbc_url(host="x")  # no service_name


# --- Oracle Spark options ---------------------------------------------------


def test_wallet_options_carry_user_and_password():
    opts = spark_jdbc_options_wallet(
        url="jdbc:oracle:thin:@atp_high",
        user="ADMIN",
        password="secret",
    )
    assert opts["driver"] == "oracle.jdbc.OracleDriver"
    assert opts["user"] == "ADMIN"
    assert opts["password"] == "secret"
    assert opts["fetchsize"] == "10000"
    assert opts["oracle.jdbc.timezoneAsRegion"] == "false"


def test_dbtoken_options_no_user_password():
    opts = spark_jdbc_options_dbtoken(
        url="jdbc:oracle:thin:@atp_high",
        token_dir="/tmp/dbcred_atp",
    )
    assert "user" not in opts
    assert "password" not in opts
    assert opts["oracle.jdbc.tokenAuthentication"] == "OCI_TOKEN"
    assert opts["oracle.jdbc.tokenLocation"] == "/tmp/dbcred_atp"


def test_password_options_basic():
    opts = spark_jdbc_options_password(
        url="jdbc:oracle:thin:@tcps://h:1522/svc",
        user="legacy",
        password="pw",
    )
    assert opts["user"] == "legacy"
    assert opts["password"] == "pw"
    assert "oracle.jdbc.tokenAuthentication" not in opts


