"""The EXTERNAL catalog's connectionDetails, from a YAML/JSON config file.

The rule the source side already follows: a secret is a PATH in the config,
read at call time, never an inline value. A config file that ends up in a
ticket or a commit then carries no credential.
"""
import json

import pytest

from target.snowflake_catalog_connection import (
    ConnectionConfigError, build_snowflake_connection_details,
    load_connection_config,
)


def _config(tmp_path, **overrides):
    key = tmp_path / "rsa.p8"
    key.write_text("-----BEGIN PRIVATE KEY-----\nabc\n", encoding="utf-8")
    base = {"account": "ORG-ACC", "warehouse": "WH", "database": "SALES_DB",
            "user": "SVC", "auth": "keypair", "key_path": str(key)}
    base.update(overrides)
    return base


def test_json_config_loads_without_pyyaml(tmp_path):
    path = tmp_path / "conn.json"
    path.write_text(json.dumps({"account": "A"}), encoding="utf-8")
    assert load_connection_config(path) == {"account": "A"}


def test_yaml_config_loads(tmp_path):
    pytest.importorskip("yaml")
    path = tmp_path / "conn.yaml"
    path.write_text("account: A\nwarehouse: WH\n", encoding="utf-8")
    assert load_connection_config(path)["warehouse"] == "WH"


def test_a_missing_config_file_names_the_path(tmp_path):
    with pytest.raises(ConnectionConfigError, match="not readable"):
        load_connection_config(tmp_path / "absent.json")


def test_keypair_credential_is_read_from_the_file_not_the_config(tmp_path):
    config = _config(tmp_path)
    details = build_snowflake_connection_details(config)
    # Enum live-enumerated by the API: "Basic" | "KeyPair".
    assert details["SNOWFLAKE_AUTHENTICATION_METHOD"] == "KeyPair"
    assert details["SNOWFLAKE_PRIVATE_KEY_CONTENT"].startswith(
        "-----BEGIN PRIVATE KEY-----")
    # The path itself is not what gets sent.
    assert config["key_path"] not in json.dumps(details)


def test_the_live_verified_key_names_are_used(tmp_path):
    # The API enumerated the allowed connectionProperties itself (2026-09-16);
    # these names are that list, not a guess.
    details = build_snowflake_connection_details(_config(tmp_path))
    assert details["SNOWFLAKE_HOST"] == "org-acc.snowflakecomputing.com"
    assert details["SNOWFLAKE_PORT"] == "443"
    assert details["SNOWFLAKE_WAREHOUSE"] == "WH"
    assert details["SNOWFLAKE_DATABASE_NAME"] == "SALES_DB"
    assert details["SNOWFLAKE_USERNAME"] == "SVC"


def test_an_explicit_host_overrides_the_account_derivation(tmp_path):
    details = build_snowflake_connection_details(
        _config(tmp_path, host="sf.private.example.com"))
    assert details["SNOWFLAKE_HOST"] == "sf.private.example.com"


def test_optional_role_is_omitted_when_absent(tmp_path):
    details = build_snowflake_connection_details(_config(tmp_path))
    assert "SNOWFLAKE_ROLE" not in details


def test_a_schema_key_is_accepted_and_left_out_of_the_body(tmp_path):
    """It used to be REFUSED, which made one config file for both ends
    impossible: the source side needs a real `schema` to scope the
    connector's pushdown session, and the catalog contract has no schema
    property at all (an EXTERNAL catalog registers the whole database). So it
    is ignored here rather than fatal -- and nothing is smuggled into the
    body, which the API validates key by key."""
    details = build_snowflake_connection_details(_config(tmp_path, schema="S"))
    assert not any("SCHEMA" in k.upper() for k in details), details
    assert all(not k.startswith("_") for k in details), \
        "the body is the API request; an unknown key earns a 400"
    assert details["SNOWFLAKE_DATABASE_NAME"]


def test_the_credential_may_be_inline_in_the_one_config_file(tmp_path):
    """One file, everything in it -- the documented default."""
    config = {"account": "ORG-ACC", "user": "SVC", "warehouse": "WH",
              "database": "DB", "auth": "keypair",
              "private_key": "-----BEGIN PRIVATE KEY-----\nINLINE\n"}
    details = build_snowflake_connection_details(config)
    assert details["SNOWFLAKE_AUTHENTICATION_METHOD"] == "KeyPair"
    assert "INLINE" in details["SNOWFLAKE_PRIVATE_KEY_CONTENT"]

    pw = build_snowflake_connection_details(
        {**config, "auth": "password", "private_key": None,
         "password": "inline-pw"})
    assert pw["SNOWFLAKE_AUTHENTICATION_METHOD"] == "Basic"
    assert pw["SNOWFLAKE_PASSWORD"] == "inline-pw"


def test_keypair_with_neither_inline_nor_path_is_refused(tmp_path):
    with pytest.raises(ConnectionConfigError, match="private_key"):
        build_snowflake_connection_details(
            {"account": "A", "user": "U", "warehouse": "W", "database": "D",
             "auth": "keypair"})


def test_password_auth_reads_the_password_file(tmp_path):
    secret = tmp_path / "pw"
    secret.write_text("hunter2\n", encoding="utf-8")
    details = build_snowflake_connection_details(
        _config(tmp_path, auth="password", password_path=str(secret)))
    assert details["SNOWFLAKE_AUTHENTICATION_METHOD"] == "Basic"
    assert details["SNOWFLAKE_PASSWORD"] == "hunter2"


def test_pat_auth_is_refused_because_the_contract_has_no_token(tmp_path):
    # The live-enumerated property list carries no PAT/token field.
    with pytest.raises(ConnectionConfigError, match="pat is not supported"):
        build_snowflake_connection_details(
            _config(tmp_path, auth="pat", pat_path="x"))


@pytest.mark.parametrize("field",
                         ["account", "warehouse", "database", "user", "auth"])
def test_a_missing_required_field_names_itself(tmp_path, field):
    config = _config(tmp_path)
    del config[field]
    with pytest.raises(ConnectionConfigError, match=field):
        build_snowflake_connection_details(config)


def test_an_unknown_auth_mode_is_refused(tmp_path):
    with pytest.raises(ConnectionConfigError, match="auth must be"):
        build_snowflake_connection_details(_config(tmp_path, auth="oauth2"))


def test_keypair_without_a_key_path_is_refused(tmp_path):
    config = _config(tmp_path)
    del config["key_path"]
    with pytest.raises(ConnectionConfigError, match="key_path"):
        build_snowflake_connection_details(config)


def test_an_unreadable_credential_file_names_the_path(tmp_path):
    with pytest.raises(ConnectionConfigError, match="private key not readable"):
        build_snowflake_connection_details(
            _config(tmp_path, key_path=str(tmp_path / "gone.p8")))
