"""Auth kwarg construction. Pure; opens no socket."""
import pytest

from snowflake_source.conn import AuthError, build_connect_kwargs

ACC = "example-org-account"


def test_keypair_requires_a_key_path():
    with pytest.raises(AuthError, match="key_path"):
        build_connect_kwargs("keypair", account=ACC, user="u")


def test_pat_requires_a_token_file():
    with pytest.raises(AuthError, match="pat_path"):
        build_connect_kwargs("pat", account=ACC, user="u")


def test_password_requires_a_password_file():
    with pytest.raises(AuthError, match="password_path"):
        build_connect_kwargs("password", account=ACC, user="u")


def test_account_is_always_required():
    with pytest.raises(AuthError, match="account"):
        build_connect_kwargs("keypair", account=None, user="u", key_path="/k")


def test_user_required_except_for_externalbrowser():
    with pytest.raises(AuthError, match="user"):
        build_connect_kwargs("keypair", account=ACC, key_path="/k")
    kw = build_connect_kwargs("externalbrowser", account=ACC)
    assert "user" not in kw


def test_externalbrowser_sets_the_authenticator():
    kw = build_connect_kwargs("externalbrowser", account=ACC, user="u")
    assert kw["authenticator"] == "externalbrowser"


def test_pat_reads_the_token_from_a_file(tmp_path):
    f = tmp_path / "pat"
    f.write_text("  tok-abc123  \n", encoding="utf-8")
    kw = build_connect_kwargs("pat", account=ACC, user="u", pat_path=str(f))
    assert kw["authenticator"] == "PROGRAMMATIC_ACCESS_TOKEN"
    # The connector's PAT authenticator reads `token`, never `password`. A PAT
    # handed over as `password` reaches the wire as `TOKEN: null`.
    assert kw["token"] == "tok-abc123", "token must be stripped"
    assert "password" not in kw


def test_pat_lands_where_the_connector_actually_reads_it(tmp_path):
    # Pins the plugin's kwargs to the connector's own consumption, so a rename
    # on either side cannot silently regress PAT login again. No socket: this
    # runs the connector's kwarg mapping and its PAT auth class, nothing else.
    connection = pytest.importorskip("snowflake.connector.connection")
    from snowflake.connector.auth.pat import AuthByPAT
    f = tmp_path / "pat"
    f.write_text("tok-abc123\n", encoding="utf-8")
    kw = build_connect_kwargs("pat", account=ACC, user="u", pat_path=str(f))

    c = connection.SnowflakeConnection.__new__(connection.SnowflakeConnection)
    for name, (value, _type) in connection.DEFAULT_CONFIGURATION.items():
        setattr(c, "_" + name, value)          # what __init__ does before __config
    c._SnowflakeConnection__config(**kw)
    assert c._authenticator == "PROGRAMMATIC_ACCESS_TOKEN"
    assert c._token == "tok-abc123"

    body = {"data": {}}
    AuthByPAT(c._token).update_body(body)      # what connect() puts in the login body
    assert body["data"]["TOKEN"] == "tok-abc123"


def test_password_read_from_file_not_taken_inline(tmp_path):
    f = tmp_path / "pw"
    f.write_text("s3cret\n", encoding="utf-8")
    kw = build_connect_kwargs("password", account=ACC, user="u", password_path=str(f))
    assert kw["password"] == "s3cret"


def test_optional_session_context_passed_through():
    kw = build_connect_kwargs("externalbrowser", account=ACC, user="u",
                              role="R", warehouse="W", database="D")
    assert (kw["role"], kw["warehouse"], kw["database"]) == ("R", "W", "D")


def test_unset_session_context_is_omitted_not_none():
    kw = build_connect_kwargs("externalbrowser", account=ACC, user="u")
    assert "role" not in kw and "warehouse" not in kw and "database" not in kw


def test_unknown_auth_mode_rejected():
    with pytest.raises(AuthError, match="unknown"):
        build_connect_kwargs("magic", account=ACC, user="u")


def test_missing_secret_file_is_a_clear_error(tmp_path):
    with pytest.raises(AuthError, match="not readable"):
        build_connect_kwargs("pat", account=ACC, user="u",
                             pat_path=str(tmp_path / "nope"))



# --- host: the laptop connects where the catalog registration points -------

def test_host_is_passed_to_the_driver_when_set():
    kw = build_connect_kwargs("externalbrowser", account=ACC,
                              host=" x.us-east-2.aws.snowflakecomputing.com ")
    assert kw["host"] == "x.us-east-2.aws.snowflakecomputing.com"


def test_host_is_omitted_when_absent_so_the_driver_derives_it():
    kw = build_connect_kwargs("externalbrowser", account=ACC)
    assert "host" not in kw
    kw = build_connect_kwargs("externalbrowser", account=ACC, host="")
    assert "host" not in kw
