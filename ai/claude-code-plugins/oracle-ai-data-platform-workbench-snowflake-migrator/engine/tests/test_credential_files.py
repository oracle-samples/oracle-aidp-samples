"""SEC-AIDP-SAMPLES-001: a credential is a FILE the config names, never a value
in it, and the file is readable by its owner alone.

Three sinks read credential files -- the parser (`migration_config`), the
laptop transport (`snowflake_source.conn`) and the data-plane connector
(`dataplane.snowmig_source`) -- and each is pinned here to the same rule:
inline refused with the `*_path` to use, group/world-readable refused on
POSIX, the check skipped and said so on Windows, and only a basename in any
message. The role gate at the laptop-side connection is pinned here too.
"""
import os
import pathlib

import pytest

from migration_config import (
    MODE_CHECK_SKIPPED_NOTE, ConfigError, check_secret_file,
    credential_sources, describe_credential_sources, read_secret_file,
    refuse_inline_secrets, resolve_secret, snowflake_block)
from secret_files import temp_secret, world_readable, write_secret

POSIX_ONLY = pytest.mark.skipif(
    os.name == "nt", reason="POSIX mode bits; Windows files inherit the "
                            "profile ACL and st_mode is meaningless")


# --- the parser --------------------------------------------------------------

@pytest.mark.parametrize("field,path_field", [
    ("password", "password_path"),
    ("private_key", "key_path"),
    ("key_passphrase", "key_passphrase_path"),
    ("token", "pat_path"),
])
def test_every_inline_credential_field_is_refused_with_its_path_field(
        field, path_field):
    block = {"account": "A", "user": "U", "auth": "password",
             field: "THE-SECRET-VALUE"}
    with pytest.raises(ConfigError) as caught:
        resolve_secret(block, field, path_field)
    assert f"`{path_field}`" in str(caught.value)
    assert "THE-SECRET-VALUE" not in str(caught.value)

    with pytest.raises(ConfigError) as caught:
        snowflake_block({"snowflake": block})
    message = str(caught.value)
    assert f"`{field}`" in message and f"`{path_field}`" in message
    assert "THE-SECRET-VALUE" not in message


def test_the_parser_refuses_a_flat_config_with_an_inline_secret_too():
    with pytest.raises(ConfigError, match="password_path"):
        snowflake_block({"account": "A", "password": "x"})


def test_every_inline_field_is_named_in_one_refusal():
    with pytest.raises(ConfigError) as caught:
        refuse_inline_secrets({"password": "a", "token": "b"})
    message = str(caught.value)
    assert "`password` -> `password_path`" in message
    assert "`token` -> `pat_path`" in message


def test_a_block_without_inline_secrets_passes_the_parser():
    block = {"account": "A", "auth": "keypair", "key_path": "~/k.p8"}
    assert snowflake_block({"snowflake": block}) == block
    refuse_inline_secrets(block)


# --- the file check ----------------------------------------------------------

def test_an_owner_only_file_is_read_and_its_note_says_so(tmp_path):
    path = write_secret(tmp_path / "pw", "  s3cret \n")
    note = check_secret_file(path, field="password_path")
    assert note == (MODE_CHECK_SKIPPED_NOTE if os.name == "nt"
                    else "owner-only (mode 0600)")
    assert read_secret_file(path, field="password_path") == "s3cret"
    assert resolve_secret({"password_path": path}, "password",
                          "password_path") == "s3cret"


@POSIX_ONLY
@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o660, 0o666])
def test_a_file_others_can_read_is_refused_with_the_chmod(tmp_path, mode):
    path = write_secret(tmp_path / "pw", "s3cret")
    os.chmod(path, mode)
    with pytest.raises(ConfigError) as caught:
        check_secret_file(path, field="password_path")
    message = str(caught.value)
    assert "readable by others" in message and "chmod 600 pw" in message
    assert f"{mode:04o}" in message
    with pytest.raises(ConfigError):
        read_secret_file(path, field="password_path")


def test_on_windows_the_mode_check_is_skipped_and_said_not_refused(
        tmp_path, monkeypatch):
    """st_mode is 0o666 for every Windows file, so enforcing the check there
    would refuse every credential file. It is skipped, with one fixed note."""
    path = write_secret(tmp_path / "pw", "s3cret")
    world_readable(path)
    monkeypatch.setattr(os, "name", "nt")
    assert check_secret_file(path, field="password_path") == \
        MODE_CHECK_SKIPPED_NOTE
    assert "Windows" in MODE_CHECK_SKIPPED_NOTE
    assert read_secret_file(path, field="password_path") == "s3cret"


def test_a_missing_file_is_named_by_basename_only(tmp_path):
    with pytest.raises(ConfigError) as caught:
        check_secret_file(tmp_path / "deep" / "gone.p8", field="key_path")
    message = str(caught.value)
    assert "gone.p8" in message and "not readable" in message
    assert str(tmp_path) not in message


def test_a_directory_is_not_a_credential_file(tmp_path):
    with pytest.raises(ConfigError, match="regular file"):
        check_secret_file(tmp_path, field="key_path")


# --- the credential source, as reported ------------------------------------

def test_credential_sources_name_the_file_never_its_directory_or_content(
        tmp_path):
    key = write_secret(tmp_path / "secrets" / "rsa_key.p8", "PEM-BODY")
    pw = write_secret(tmp_path / "secrets" / "pw", "PW-BODY")
    block = {"auth": "keypair", "key_path": key, "password_path": pw}
    sources = credential_sources(block)
    assert [s["field"] for s in sources] == ["key_path", "password_path"]
    assert [s["name"] for s in sources] == ["rsa_key.p8", "pw"]
    assert all(s["source"] == "file" and s["ok"] for s in sources)
    blob = repr(sources) + " ".join(describe_credential_sources(block))
    assert "PEM-BODY" not in blob and "PW-BODY" not in blob
    assert "secrets" not in blob and str(tmp_path) not in blob
    assert "rsa_key.p8" in blob


@POSIX_ONLY
def test_a_credential_source_that_fails_the_check_is_reported_not_raised(
        tmp_path):
    pw = write_secret(tmp_path / "pw", "x")
    os.chmod(pw, 0o644)
    [source] = credential_sources({"password_path": pw})
    assert source["ok"] is False and "readable by others" in source["protection"]


# --- the laptop transport (snowflake_source.conn) ---------------------------

@POSIX_ONLY
@pytest.mark.parametrize("auth,field", [("pat", "pat_path"),
                                         ("password", "password_path")])
def test_the_transport_refuses_a_credential_file_others_can_read(tmp_path,
                                                                 auth, field):
    from snowflake_source.conn import AuthError, build_connect_kwargs
    path = write_secret(tmp_path / "secret", "tok")
    os.chmod(path, 0o644)
    with pytest.raises(AuthError, match="readable by others"):
        build_connect_kwargs(auth, account="acc", user="u", **{field: path})


@POSIX_ONLY
def test_the_transport_refuses_a_private_key_others_can_read(tmp_path):
    from snowflake_source.conn import AuthError, load_private_key_der
    path = write_secret(tmp_path / "rsa.p8", "not even a pem")
    os.chmod(path, 0o644)
    with pytest.raises(AuthError, match="readable by others"):
        load_private_key_der(path)


def test_the_transport_names_an_unreadable_file_by_basename(tmp_path):
    from snowflake_source.conn import AuthError, build_connect_kwargs
    with pytest.raises(AuthError) as caught:
        build_connect_kwargs("pat", account="acc", user="u",
                             pat_path=str(tmp_path / "nowhere" / "pat"))
    assert "pat" in str(caught.value) and str(tmp_path) not in str(caught.value)


# --- the data-plane connector (dataplane.snowmig_source) --------------------

def _stub_spark():
    class _Spark:
        read = None
    return _Spark()


def _source(config):
    from dataplane.snowmig_source import SnowflakeSource
    base = {"account": "acct", "warehouse": "WH", "database": "DB",
            "user": "svc", "schema": "S"}
    return SnowflakeSource(_stub_spark(), config={**base, **config})


@pytest.mark.parametrize("auth,field,path_field", [
    ("password", "password", "password_path"),
    ("keypair", "private_key", "key_path"),
    ("keypair", "key_passphrase", "key_passphrase_path"),
])
def test_the_connector_refuses_an_inline_credential(auth, field, path_field):
    from dataplane.snowmig_source import SourceConfigError
    config = {"auth": auth, field: "INLINE-VALUE"}
    if field == "key_passphrase":
        config["key_path"] = temp_secret("PEM")
    with pytest.raises(SourceConfigError) as caught:
        _source(config)
    assert f"`{path_field}`" in str(caught.value)
    assert "INLINE-VALUE" not in str(caught.value)


def test_the_connector_reads_each_credential_from_its_file():
    key, passphrase = temp_secret("PEM-TEXT\n"), temp_secret("pass\n")
    opts = _source({"auth": "keypair", "key_path": key,
                    "key_passphrase_path": passphrase})._options
    assert opts["private.key.content"] == "PEM-TEXT"
    assert opts["private.key.pass.phrase"] == "pass"
    pw = _source({"auth": "password", "password_path": temp_secret("pw")})
    assert pw._options["password"] == "pw"


def test_the_connector_says_which_path_field_is_missing():
    from dataplane.snowmig_source import SourceConfigError
    with pytest.raises(SourceConfigError, match="key_path"):
        _source({"auth": "keypair"})
    with pytest.raises(SourceConfigError, match="password_path"):
        _source({"auth": "password"})


@POSIX_ONLY
def test_the_connector_refuses_a_file_others_can_read_off_the_mount(tmp_path):
    from dataplane.snowmig_source import SourceConfigError
    pw = write_secret(tmp_path / "pw", "pw")
    os.chmod(pw, 0o644)
    with pytest.raises(SourceConfigError, match="readable by others"):
        _source({"auth": "password", "password_path": pw})


def test_on_the_workspace_mount_the_mode_is_the_mounts_and_is_not_checked(
        tmp_path, monkeypatch):
    """`provision` places the file on /Workspace; the mount decides its mode
    bits and workspace membership who can read it, so a 0644 there is not
    the operator's doing and is not refused."""
    import dataplane.snowmig_source as src
    pw = write_secret(tmp_path / "pw", "pw")
    world_readable(pw)
    monkeypatch.setattr(src, "WORKSPACE_MOUNT", tmp_path.as_posix() + "/")
    monkeypatch.setattr(os, "name", "posix")
    assert _source({"auth": "password", "password_path": pw}
                   )._options["password"] == "pw"


def test_the_connector_names_a_missing_file_by_basename(tmp_path):
    from dataplane.snowmig_source import SourceConfigError
    with pytest.raises(SourceConfigError) as caught:
        _source({"auth": "password",
                 "password_path": str(tmp_path / "vault" / "pw")})
    assert "pw" in str(caught.value) and "vault" not in str(caught.value)


# --- the laptop-side connection: credential source said, role gated --------

def _session(grants):
    def run_sql(sql, params=None):
        low = " ".join(sql.split()).lower()
        if "current_role()" in low:
            return [{"R": "READER", "S": '{"roles":"","value":""}'}]
        if low.startswith("show grants to role"):
            return grants
        return []
    return run_sql


def _wire(monkeypatch, run_sql):
    import snowmig
    built = {}
    monkeypatch.setattr(snowmig, "build_connect_kwargs",
                        lambda auth, **kw: built.update(kw) or {})
    monkeypatch.setattr(snowmig, "connect", lambda **kw: "CONN")
    monkeypatch.setattr(snowmig, "make_run_sql", lambda conn: run_sql)
    return built


def _config(tmp_path):
    pw = write_secret(tmp_path / "secrets" / "pw.txt", "not-a-real-password")
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text("snowflake:\n  account: ORG-ACC\n  user: SVC\n"
                   "  warehouse: WH\n  database: SALES_DB\n  auth: password\n"
                   f"  password_path: {pw}\n", encoding="utf-8")
    return cfg


def test_the_connection_announces_the_credential_source_by_basename(
        tmp_path, monkeypatch, capsys):
    import snowmig
    built = _wire(monkeypatch, _session(
        [{"privilege": "SELECT", "granted_on": "TABLE", "name": "SALES_DB.S.T"}]))
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config", str(_config(tmp_path))])
    snowmig._run_sql_from_args(args)
    out, err = capsys.readouterr()
    assert "credential: password_path: file pw.txt" in out
    assert "secrets" not in out and "not-a-real-password" not in out + err
    assert built["password_path"].endswith("pw.txt"), "a PATH reaches conn.py"
    assert "role: READER is read-only on SALES_DB" in err


def test_the_connection_is_refused_when_the_role_can_write_the_source(
        tmp_path, monkeypatch, capsys):
    import snowmig
    from snowflake_source.role_guard import RoleNotReadOnly
    _wire(monkeypatch, _session([
        {"privilege": "SELECT", "granted_on": "TABLE", "name": "SALES_DB.S.T"},
        {"privilege": "DROP", "granted_on": "TABLE", "name": "SALES_DB.S.T"}]))
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config", str(_config(tmp_path))])
    with pytest.raises(RoleNotReadOnly, match="DROP"):
        snowmig._run_sql_from_args(args)
    # Through main(): one `error:` line, no traceback, exit 1.
    rc = snowmig.main(["assess", "--out-dir", str(tmp_path),
                       "--config", str(_config(tmp_path))])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error:" in err and "DROP" in err and "Traceback" not in err


def test_preflight_runs_the_role_check_once_as_a_reported_step(
        tmp_path, monkeypatch, capsys):
    """`preflight --test-source` reports the grants in PREFLIGHT_CONFIG.md
    and does not run the connection-time gate a second time."""
    import snowmig
    calls = []
    inner = _session([{"privilege": "INSERT", "granted_on": "TABLE",
                       "name": "SALES_DB.S.T"}])

    def counting(sql, params=None):
        calls.append(sql)
        low = sql.lower()
        if "current_user" in low:
            return [{"U": "SVC", "R": "READER", "W": "WH", "D": "SALES_DB"}]
        if "show schemas" in low:
            return []
        if "information_schema" in low:
            return [{"N": 1}]
        return inner(sql, params)

    _wire(monkeypatch, counting)
    rc = snowmig.main(["preflight", "--test-source", "--out-dir",
                       str(tmp_path / "out"), "--config",
                       str(_config(tmp_path))])
    assert rc == 1, "a role that can INSERT fails preflight"
    grants_reads = [c for c in calls if c.lower().startswith("show grants")]
    assert len(grants_reads) == 1
    report = (tmp_path / "out" / "PREFLIGHT_CONFIG.md").read_text(encoding="utf-8")
    assert "source role is read-only | **FAIL**" in report
    assert "`INSERT` on TABLE `SALES_DB.S.T`" in report
    assert "pw.txt" in report and str(tmp_path) not in report
    assert "not-a-real-password" not in report
    assert "not-a-real-password" not in capsys.readouterr().out


def test_nothing_is_spooled_to_a_temp_file_any_more(tmp_path, monkeypatch):
    import tempfile
    import snowmig
    _wire(monkeypatch, _session([]))
    before = set(pathlib.Path(tempfile.gettempdir()).glob("snowmig_secret_*"))
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config", str(_config(tmp_path))])
    snowmig._run_sql_from_args(args)
    after = set(pathlib.Path(tempfile.gettempdir()).glob("snowmig_secret_*"))
    assert after == before
