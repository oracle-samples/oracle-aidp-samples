"""The `preflight` STAGE: echo the connection config back, then test it.

(The deploy-time pre-flight summary -- what would happen before a write -- is
`test_preflight.py`. Two different pre-flights, both load-bearing: this one
is about the config the user filled in, that one about the plan they approve.)

The echo is the deliverable. These tests pin two properties above all: a
credential's CONTENT never appears, and a check that was not run reads as
skipped rather than as a pass.
"""
import json
import os

import pytest

from plan.preflight import (
    describe_config, render_preflight_report, run_preflight)
from secret_files import write_secret


def _config(tmp_path, **overrides):
    key = tmp_path / "rsa.p8"
    write_secret(key, "-----BEGIN PRIVATE KEY-----\nSUPERSECRET\n")
    base = {"account": "ORG-ACC", "user": "SVC", "warehouse": "WH",
            "database": "SALES_DB", "role": "READER", "auth": "keypair",
            "key_path": str(key)}
    base.update(overrides)
    return base


READ_ONLY_GRANTS = [
    {"privilege": "USAGE", "granted_on": "DATABASE", "name": "SALES_DB"},
    {"privilege": "SELECT", "granted_on": "TABLE",
     "name": "SALES_DB.PUBLIC.ORDERS"},
]


def _run_sql(sql, params=None):
    low = " ".join(sql.split()).lower()
    if "current_user" in low:
        return [{"U": "SVC", "R": "READER", "W": "WH", "D": "SALES_DB"}]
    if "current_role()" in low:
        return [{"R": "READER", "S": '{"roles":"","value":""}'}]
    if low.startswith("show grants to role"):
        return list(READ_ONLY_GRANTS)
    if "show schemas" in low:
        return [{"name": "A"}, {"name": "B"}]
    raise AssertionError(f"unexpected sql: {sql}")


def _writer_sql(sql, params=None):
    """The same session, but its role can also INSERT into the source."""
    low = " ".join(sql.split()).lower()
    if low.startswith("show grants to role"):
        return READ_ONLY_GRANTS + [{"privilege": "INSERT", "granted_on":
                                    "TABLE", "name": "SALES_DB.PUBLIC.ORDERS"}]
    return _run_sql(sql, params)


def _call(operation, **kw):
    if operation == "list_catalogs":
        return {"items": [{"displayName": "lake", "catalogType": "INTERNAL"}]}
    raise AssertionError(operation)


def test_every_field_is_echoed_with_its_purpose(tmp_path):
    described = describe_config(_config(tmp_path))
    fields = {f["field"]: f for f in described["fields"]}
    assert fields["role"]["value"] == "READER"
    assert fields["warehouse"]["purpose"], "a field with no purpose is noise"
    assert described["missing"] == []


def test_the_host_is_reported_as_derived_when_absent(tmp_path):
    described = describe_config(_config(tmp_path))
    host = next(f for f in described["fields"] if f["field"] == "host")
    assert host["derived"] is True
    assert described["host_effective"] == "ORG-ACC.snowflakecomputing.com"


def test_an_explicit_host_is_not_marked_derived(tmp_path):
    described = describe_config(_config(tmp_path, host="h.example.com"))
    host = next(f for f in described["fields"] if f["field"] == "host")
    assert host["derived"] is False
    assert described["host_effective"] == "h.example.com"


def test_the_credential_is_echoed_by_its_file_name_never_as_content(tmp_path):
    """SEC-AIDP-SAMPLES-001: the report says the credential's SOURCE -- the
    file's basename and that it is owner-only -- and neither its content
    nor its directory, which can name a user, a host or a secret store."""
    config = _config(tmp_path)
    result = run_preflight(config, run_sql=_run_sql)
    report = render_preflight_report(result)
    assert "SUPERSECRET" not in report
    assert "SUPERSECRET" not in repr(result)
    assert "rsa.p8" in report, "the basename is the source the user confirms"
    assert str(tmp_path) not in report, "the directory is not echoed"
    assert config["key_path"] not in repr(result)
    check = next(c for c in result["checks"] if "credential" in c["name"])
    assert check["ok"] is True
    if os.name == "nt":
        assert "mode check skipped on Windows" in check["detail"]
    else:
        assert "owner-only" in check["detail"]


@pytest.mark.skipif(os.name == "nt",
                    reason="POSIX mode bits; Windows files inherit the profile ACL")
def test_a_credential_file_others_can_read_fails_the_check(tmp_path):
    config = _config(tmp_path)
    os.chmod(config["key_path"], 0o644)
    result = run_preflight(config, run_sql=_run_sql)
    bad = next(c for c in result["checks"] if "credential" in c["name"])
    assert bad["ok"] is False
    assert "readable by others" in bad["detail"] and "chmod 600" in bad["detail"]
    assert result["ok"] is False


def test_a_missing_credential_file_fails_the_check(tmp_path):
    result = run_preflight(_config(tmp_path, key_path=str(tmp_path / "gone")),
                           run_sql=_run_sql)
    bad = next(c for c in result["checks"] if "credential" in c["name"])
    assert bad["ok"] is False
    assert "not readable" in bad["detail"]
    assert result["ok"] is False


# --- the role gate: grants read back before anything is discovered ----------

def test_a_read_only_role_passes_and_its_grants_are_the_evidence(tmp_path):
    result = run_preflight(_config(tmp_path), run_sql=_run_sql)
    check = next(c for c in result["checks"]
                 if c["name"] == "source role is read-only")
    assert check["ok"] is True and "READER" in check["detail"]
    assert result["role_grants"]["read_only"] is True
    report = render_preflight_report(result)
    assert "## Source role grants" in report
    assert "`SELECT` | 1" in report and "SHOW GRANTS TO ROLE" in report


def test_a_role_that_can_write_the_source_fails_preflight(tmp_path):
    result = run_preflight(_config(tmp_path), run_sql=_writer_sql)
    check = next(c for c in result["checks"]
                 if c["name"] == "source role is read-only")
    assert check["ok"] is False
    assert "INSERT" in check["detail"] and "SALES_DB.PUBLIC.ORDERS" in check["detail"]
    assert result["ok"] is False
    report = render_preflight_report(result)
    assert "Write privileges on the source" in report
    assert "`INSERT` on TABLE `SALES_DB.PUBLIC.ORDERS`" in report


def test_unreadable_grants_fail_the_role_check_closed(tmp_path):
    def denied(sql, params=None):
        if sql.lower().startswith("show grants"):
            raise RuntimeError("Insufficient privileges")
        return _run_sql(sql, params)

    result = run_preflight(_config(tmp_path), run_sql=denied)
    check = next(c for c in result["checks"]
                 if c["name"] == "source role is read-only")
    assert check["ok"] is False and "could not be read" in check["detail"]
    assert result["ok"] is False


def test_a_missing_required_field_is_named(tmp_path):
    config = _config(tmp_path)
    del config["warehouse"]
    result = run_preflight(config, run_sql=_run_sql)
    assert "warehouse" in result["config"]["missing"]
    assert result["ok"] is False
    assert "warehouse" in render_preflight_report(result)


def test_an_unchecked_end_reads_as_skipped_not_as_pass(tmp_path):
    result = run_preflight(_config(tmp_path))          # no transports at all
    statuses = {c["name"]: c["ok"] for c in result["checks"]}
    assert statuses["source"] is None
    assert statuses["destination"] is None
    assert result["skipped"] == 2
    assert result["ok"] is True, "a skip is not a failure either"
    assert "SKIPPED" in render_preflight_report(result)


def test_the_source_identity_is_reported(tmp_path):
    result = run_preflight(_config(tmp_path), run_sql=_run_sql)
    check = next(c for c in result["checks"] if c["name"] == "source identity")
    assert check["ok"] is True
    assert "role=READER" in check["detail"]


def test_a_source_failure_is_captured_not_raised(tmp_path):
    def broken(sql, params=None):
        raise RuntimeError("390100 incorrect username or password")

    result = run_preflight(_config(tmp_path), run_sql=broken)
    check = next(c for c in result["checks"] if c["name"] == "source identity")
    assert check["ok"] is False
    assert "390100" in check["detail"]


def test_the_destination_catalog_type_is_reported(tmp_path):
    result = run_preflight(_config(tmp_path), call=_call, catalog="lake")
    check = next(c for c in result["checks"]
                 if c["name"] == "destination catalog")
    assert check["ok"] is True and "INTERNAL" in check["detail"]


def test_an_absent_destination_catalog_is_a_failure(tmp_path):
    result = run_preflight(_config(tmp_path), call=_call, catalog="missing")
    check = next(c for c in result["checks"]
                 if c["name"] == "destination catalog")
    assert check["ok"] is False
    assert "not found" in check["detail"]


def test_unrecognised_keys_are_reported_rather_than_ignored(tmp_path):
    described = describe_config(_config(tmp_path, typo_field="x"))
    assert described["unknown"] == ["typo_field"]


def test_a_config_with_no_credential_at_all_is_called_out(tmp_path):
    """No `*_path` at all: the auth mode cannot work, and the report has to
    say so. (A credential sitting INLINE is refused -- see below.)"""
    config = _config(tmp_path)
    del config["key_path"]
    report = render_preflight_report(run_preflight(config))
    assert "No credential in the config at all" in report


def test_the_report_tells_the_reader_to_confirm_with_the_user(tmp_path):
    report = render_preflight_report(run_preflight(_config(tmp_path)))
    assert "confirm" in report.lower()


# --- the config file itself ------------------------------------------------

@pytest.mark.skipif(os.name == "nt",
                    reason="POSIX mode bits; Windows files inherit the profile ACL")
def test_a_new_config_is_created_unreadable_to_others(tmp_path):
    """It is about to hold a password in plain text; 0644 from the default
    umask would make that world-readable on a shared host."""
    import stat
    from migration_config import write_template
    template = tmp_path / "template.yaml"
    template.write_text("snowflake:\n  account: X\n", encoding="utf-8")
    written = write_template(tmp_path / "snowmig-config.yaml",
                             template=template)
    mode = stat.S_IMODE(written.stat().st_mode)
    assert mode == 0o600, oct(mode)


def test_it_refuses_to_overwrite_a_config_that_holds_credentials(tmp_path):
    from migration_config import ConfigError, write_template
    template = tmp_path / "template.yaml"
    template.write_text("snowflake: {}\n", encoding="utf-8")
    target = tmp_path / "snowmig-config.yaml"
    target.write_text("snowflake:\n  password: real-one\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="refusing to overwrite"):
        write_template(target, template=template)
    assert "real-one" in target.read_text(encoding="utf-8"), "the existing file is untouched"
    # --force is the explicit way through.
    write_template(target, template=template, overwrite=True)
    assert "real-one" not in target.read_text(encoding="utf-8")


def test_discovery_prefers_the_working_directory_over_the_plugin(tmp_path):
    from migration_config import discover_config
    cwd, plugin = tmp_path / "work", tmp_path / "plugin"
    cwd.mkdir(), plugin.mkdir()
    (plugin / "snowmig-config.yaml").write_text("snowflake: {}\n", encoding="utf-8")
    # With only the plugin's copy, that is what is found.
    assert discover_config(cwd=cwd, plugin_root=plugin).parent == plugin
    # The operator's own file wins as soon as it exists.
    (cwd / "snowmig-config.yaml").write_text("snowflake: {}\n", encoding="utf-8")
    assert discover_config(cwd=cwd, plugin_root=plugin).parent == cwd


def test_a_missing_config_says_how_to_make_one(tmp_path):
    from migration_config import ConfigError, discover_config
    with pytest.raises(ConfigError) as caught:
        discover_config(cwd=tmp_path, plugin_root=tmp_path / "nope")
    message = str(caught.value)
    assert "init-config" in message
    assert "snowmig-config.example.yaml" in message


def test_a_secret_is_never_rendered(tmp_path):
    from migration_config import redact
    config = {"snowflake": {"user": "svc", "password": "hunter2",
                            "private_key": "-----BEGIN", "role": "READER"},
              "aidp": {"datalake_ocid": "ocid1.x"}}
    shown = redact(config)
    blob = json.dumps(shown)
    assert "hunter2" not in blob and "BEGIN" not in blob
    # Everything that is NOT a secret stays readable: the point is to show
    # the config to the user, not to hide it.
    assert shown["snowflake"]["user"] == "svc"
    assert shown["snowflake"]["role"] == "READER"
    assert shown["aidp"]["datalake_ocid"] == "ocid1.x"


def test_an_inline_secret_is_refused_with_or_without_the_path_beside_it(
        tmp_path):
    """SEC-AIDP-SAMPLES-001: `resolve_secret` reads a credential from the
    owner-only file at `*_path` and from nowhere else."""
    from migration_config import ConfigError, resolve_secret
    secret = write_secret(tmp_path / "pw", "from-file")
    with pytest.raises(ConfigError, match="password_path"):
        resolve_secret({"password": "inline", "password_path": secret},
                       "password", "password_path")
    with pytest.raises(ConfigError, match="password_path") as caught:
        resolve_secret({"password": "inline-value"}, "password",
                       "password_path")
    assert "inline-value" not in str(caught.value)
    assert resolve_secret({"password_path": secret}, "password",
                          "password_path") == "from-file"
    assert resolve_secret({}, "password", "password_path") is None


def test_an_unknown_aidp_key_is_reported_not_ignored():
    from migration_config import ConfigError, aidp_block
    with pytest.raises(ConfigError, match="datalake_ocid"):
        aidp_block({"aidp": {"datalake_ocd": "typo"}})


# --- a config the parser rejects must not be quoted back ---------------------

_BROKEN_SECRET = "SECRET-XYZ-123"

# Each line is what a real password looks like once a YAML-special character
# lands in it unquoted, and each drives PyYAML into a different error class.
# A parser error quotes the offending source line; on the password line that
# IS the password.
_BROKEN_PASSWORD_LINES = [
    pytest.param("password: {" + _BROKEN_SECRET, id="brace-parser-error"),
    pytest.param("password: Pa: " + _BROKEN_SECRET, id="colon-scanner-error"),
    pytest.param("password: [" + _BROKEN_SECRET, id="bracket-parser-error"),
    pytest.param("password: *" + _BROKEN_SECRET, id="star-composer-error"),
    pytest.param('password: "' + _BROKEN_SECRET, id="quote-unterminated"),
]


def _broken_config(tmp_path, line):
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text("snowflake:\n  account: acme\n  auth: password\n"
                   f"  {line}\naidp:\n  workspace: ws\n", encoding="utf-8")
    return cfg


@pytest.mark.parametrize("line", _BROKEN_PASSWORD_LINES)
def test_a_malformed_yaml_config_is_refused_without_echoing_the_secret(
        tmp_path, line):
    """`yaml.safe_load` was unguarded, and a YAMLError is not a ValueError, so
    a password containing `{`, `: `, `[`, `*` or a leading quote escaped
    `main()` as a traceback -- with PyYAML's snippet of the offending line,
    i.e. the password, in it."""
    import traceback
    from migration_config import ConfigError, load_config
    with pytest.raises(ConfigError) as caught:
        load_config(_broken_config(tmp_path, line))
    message = str(caught.value)
    assert "not valid YAML" in message
    assert "line 4" in message, "the operator needs to know WHERE, not what"
    assert _BROKEN_SECRET not in message
    # Nor may the chained cause carry it: a traceback printer walks the chain.
    rendered = "".join(traceback.format_exception(caught.value))
    assert _BROKEN_SECRET not in rendered


@pytest.mark.parametrize("line", _BROKEN_PASSWORD_LINES)
def test_preflight_on_a_malformed_config_exits_1_with_a_redacted_message(
        tmp_path, line, capsys):
    """`preflight` is the documented first command, and it is where a strong
    password first meets the YAML parser."""
    from snowmig import main
    cfg = _broken_config(tmp_path, line)
    rc = main(["preflight", "--config", str(cfg),
               "--out-dir", str(tmp_path / "out")])
    captured = capsys.readouterr()
    assert rc == 1
    assert "error:" in captured.err
    assert "Traceback" not in captured.err
    assert _BROKEN_SECRET not in captured.out
    assert _BROKEN_SECRET not in captured.err


def test_a_malformed_json_config_is_refused_without_echoing_the_secret(
        tmp_path):
    import traceback
    from migration_config import ConfigError, load_config
    cfg = tmp_path / "snowmig-config.json"
    cfg.write_text('{"snowflake": {"account": "acme", "password": '
                   + _BROKEN_SECRET + '}}', encoding="utf-8")
    with pytest.raises(ConfigError) as caught:
        load_config(cfg)
    assert "not valid JSON" in str(caught.value)
    rendered = "".join(traceback.format_exception(caught.value))
    assert _BROKEN_SECRET not in rendered


# --- the first-run failure everyone hits -----------------------------------

def test_a_bad_account_is_explained_not_tracebacked(monkeypatch):
    """A mistyped `account` is the most common first-run mistake, and the
    driver reports it as a 404 on a URL. That must not reach the user as a
    traceback: it sends them looking for a bug in the tool."""
    from snowflake_source.conn import AuthError, connect
    from snowflake.connector.errors import HttpError

    def explode(**kwargs):
        raise HttpError(msg="404 Not Found: post NOPE.snowflakecomputing.com"
                            ":443/session/v1/login-request",
                        errno=290404)

    import snowflake.connector
    monkeypatch.setattr(snowflake.connector, "connect", explode)
    with pytest.raises(AuthError) as caught:
        connect(account="NOPE", user="U", password="s3cr3t-never-print")
    message = str(caught.value)
    assert "NOPE" in message
    assert "could not be reached" in message
    assert "migration config" in message, "it must say WHERE to fix it"
    assert "s3cr3t-never-print" not in message, "the secret must not be echoed"


def test_an_unrecognised_driver_error_still_says_what_to_check(monkeypatch):
    """No hint is not a reason to leak a traceback; the advice still applies."""
    from snowflake_source.conn import AuthError, connect
    from snowflake.connector.errors import DatabaseError

    def explode(**kwargs):
        raise DatabaseError(msg="something new", errno=999999)

    import snowflake.connector
    monkeypatch.setattr(snowflake.connector, "connect", explode)
    with pytest.raises(AuthError, match="preflight --test-source"):
        connect(account="A", user="U")


# --- inline secrets are refused, with the path field that replaces them ----

def test_an_inline_secret_is_reported_refused_and_names_the_path_field():
    """SEC-AIDP-SAMPLES-001. The field is still recognised (not an unknown
    key), its value is never rendered, and the check FAILS: the config
    travels, so a credential in it is not accepted."""
    from plan.preflight import describe_config, render_preflight_report, \
        run_preflight
    config = {"account": "ORG-ACCT", "user": "SVC", "warehouse": "WH",
              "database": "DB", "auth": "keypair",
              "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n"}
    described = describe_config(config)
    assert described["unknown"] == [], described["unknown"]
    assert described["missing"] == []
    inline = [s for s in described["secrets"] if s["field"] == "private_key"]
    assert inline and inline[0]["inline"] is True
    assert inline[0]["exists"] is False, "an inline credential is refused"
    assert "key_path" in inline[0]["note"]

    result = run_preflight(config)
    assert result["ok"] is False
    check = next(c for c in result["checks"] if "private_key" in c["name"])
    assert check["ok"] is False and "refused" in check["name"]
    report = render_preflight_report(result)
    assert "not accepted" in report and "key_path" in report
    assert "abc" not in report, "the key itself must never be rendered"
    assert "BEGIN PRIVATE KEY" not in report


def test_preflight_refuses_an_inline_password_in_the_config_file(tmp_path,
                                                                 capsys):
    """Through the CLI: the parser refuses before any check runs, the
    message names `password_path`, and the value stays out of the output."""
    from snowmig import main
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text("snowflake:\n  account: acme\n  user: u\n  warehouse: w\n"
                   "  database: d\n  auth: password\n"
                   "  password: INLINE-SECRET-VALUE\n", encoding="utf-8")
    rc = main(["preflight", "--config", str(cfg),
               "--out-dir", str(tmp_path / "out")])
    captured = capsys.readouterr()
    assert rc == 1
    assert "error:" in captured.err and "password_path" in captured.err
    assert "INLINE-SECRET-VALUE" not in captured.err + captured.out


def test_no_credential_at_all_is_still_called_out():
    from plan.preflight import render_preflight_report, run_preflight
    config = {"account": "ORG-ACCT", "user": "SVC", "warehouse": "WH",
              "database": "DB", "auth": "password"}
    report = render_preflight_report(run_preflight(config))
    assert "No credential in the config at all" in report
