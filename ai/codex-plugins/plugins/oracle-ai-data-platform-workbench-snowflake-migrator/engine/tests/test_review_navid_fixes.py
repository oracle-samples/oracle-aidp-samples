"""Review round (navid-heydari) on the SEC-AIDP-SAMPLES-001 remediation.

Each test here failed against the reviewed head and pins the fix:

  1. `--database` handed the role guard a LIST, which it read as the text
     "['SALES_DB']": every real database was "outside the migration" and a
     write on the source was reported, never refused. The scope is now a
     set of names, exactly the databases the run reads, and any other
     shape refuses the session.
  2. The guard upper-cased every unquoted `database:`; discovery folds only
     a plain identifier. `my-db` was read as MY-DB by the guard and my-db
     by discovery, so a write on `"my-db"` passed. Both now resolve with
     `lexer.config_name`.
  3. A secret under a non-canonical key (`Password:`, `passcode:`, a nested
     `extra: {password: ...}`) passed the parser and was copied to the
     workspace mount. Secret-like keys are matched case-insensitively and
     recursively, and the uploaded block is built from an allowlist.
  4. A test patched `os.name`, which pathlib consults; on Python 3.11 the
     whole run INTERNALERRORed. Platform flags are module-level now.
  5. The data-plane connector tested the mount prefix on an unresolved
     path and read a FIFO. Resolved first; regular files only.
  6. A re-push could inherit a credential FILE as the notebooks'
     `--source-config`. The config object is tracked on its own.
  7. Credential files were stat'ed then reopened by name, and an empty one
     read as ''. Opened once, judged on the descriptor, empty refused.
  8. A procedure row the guard could not place got the inside-scope
     allowance. Refused.
  Nits: the preflight "inline" row that could never appear is gone, the
  template wording no longer says the config holds a password, a 0640 file
  is refused as readable by its "group or others".
"""
import json
import os
import pathlib
import re

import pytest
import yaml

import snowmig
import migration_config
from migration_config import (
    ConfigError, check_secret_file, read_secret_file, redact,
    refuse_inline_secrets, snowflake_block, write_template)
from snowflake_source.conn import AuthError
from snowflake_source.dialect import lexer
from snowflake_source.role_guard import (
    RoleNotReadOnly, assert_role_read_only, read_role_grants)
from target import provisioning
from target.provisioning import (
    PLAN_FOLDER, SCRIPTS_FOLDER, provision, source_config_objects)
from secret_files import temp_secret, world_readable, write_secret
from test_credential_files import _config, _session, _wire
from test_provisioning import Fake
from test_role_guard import FakeCursor, _grant
from test_teardown_provenance import World

POSIX_ONLY = pytest.mark.skipif(
    os.name == "nt", reason="POSIX mode bits or FIFOs; neither exists here")

TESTS_DIR = pathlib.Path(__file__).resolve().parent

# The reviewer's repro: a read-only-looking role that holds INSERT on the
# source table. `database="SALES_DB"` refuses it; the list form did not.
REVIEWER_GRANTS = [
    _grant("USAGE", "DATABASE", "SALES_DB"),
    _grant("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
]


# --- 1 · a list of databases is a set of names, not a string ----------------

def test_the_reviewers_repro_a_list_scope_refuses_the_write_too():
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(FakeCursor(REVIEWER_GRANTS), database="SALES_DB")
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(FakeCursor(REVIEWER_GRANTS),
                              database=["SALES_DB"])


@pytest.mark.parametrize("scope", [
    ["SALES_DB"], ("SALES_DB",), {"SALES_DB"}, ["sales_db"],
    ["OTHER_DB", "SALES_DB"], ["SALES_DB", "SALES_DB"],
])
def test_every_database_in_the_list_is_inside_the_migration(scope):
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(FakeCursor(REVIEWER_GRANTS), database=scope)
    evidence = read_role_grants(FakeCursor(REVIEWER_GRANTS), database=scope)
    assert evidence["read_only"] is False
    assert evidence["out_of_scope_writes"] == 0
    assert "SALES_DB" in evidence["databases"]
    assert "SALES_DB" in evidence["database"]
    assert "[" not in evidence["database"], "the scope is names, not a repr"


def test_a_write_on_a_database_the_list_does_not_name_is_reported():
    cur = FakeCursor(REVIEWER_GRANTS + [_grant("INSERT", "TABLE",
                                              "SCRATCH.PUBLIC.T")])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database=["SALES_DB", "ARCHIVE_DB"])
    assert "SALES_DB.PUBLIC.ORDERS" in str(caught.value)
    assert "SCRATCH" not in str(caught.value)
    evidence = read_role_grants(cur, database=["SALES_DB", "ARCHIVE_DB"])
    assert evidence["out_of_scope_writes"] == 1
    assert evidence["databases"] == ["ARCHIVE_DB", "SALES_DB"]
    assert evidence["database"] == "ARCHIVE_DB, SALES_DB"


@pytest.mark.parametrize("scope", [
    [], (), [None], [1], ["SALES_DB", None], ["SALES_DB", ""], [" "],
    {"database": "SALES_DB"}, 7, b"SALES_DB", ["SALES_DB", b"X"],
])
def test_a_scope_the_guard_cannot_read_refuses_the_session(scope):
    """Fail closed: a shape that is not a name or a list of names would be
    judged against the wrong thing, which is exactly how the list bug
    turned every write into a report."""
    read_only = [_grant("SELECT", "TABLE", "SALES_DB.PUBLIC.ORDERS")]
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(FakeCursor(read_only), database=scope)
    message = str(caught.value)
    assert "scope" in message and "`database:`" in message
    assert "--database" in message


def test_a_blank_database_is_no_scope_and_everything_is_inside():
    """`database: ""` is not a scope named "" (which would place every
    real database outside); it is no scope, as None is."""
    cur = FakeCursor(REVIEWER_GRANTS)
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database="")
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database="   ")
    assert read_role_grants(cur, database="")["database"] is None
    assert read_role_grants(cur, database="")["databases"] == []


def _reviewer_session():
    return _session([
        {"privilege": "USAGE", "granted_on": "DATABASE", "name": "SALES_DB"},
        {"privilege": "INSERT", "granted_on": "TABLE",
         "name": "SALES_DB.PUBLIC.ORDERS"}])


def test_assess_with_a_database_flag_is_refused_through_the_real_parser(
        tmp_path, monkeypatch, capsys):
    """The reviewer's command: `assess --database SALES_DB` with a role
    that can INSERT on SALES_DB. The flag built a list, the guard read its
    text, and the run went on."""
    _wire(monkeypatch, _reviewer_session())
    argv = ["assess", "--out-dir", str(tmp_path / "out"),
            "--config", str(_config(tmp_path)), "--database", "SALES_DB"]
    args = snowmig.build_parser().parse_args(argv)
    assert args.database == ["SALES_DB"], "argparse builds a list"
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        snowmig._run_sql_from_args(args)
    assert snowmig.main(argv) == 1
    err = capsys.readouterr().err
    assert "error:" in err and "INSERT" in err and "Traceback" not in err
    assert not (tmp_path / "out" / "inventory.json").exists(), \
        "nothing was discovered"


def test_smoke_with_a_database_flag_is_refused_through_the_real_parser(
        tmp_path, monkeypatch, capsys):
    _wire(monkeypatch, _reviewer_session())
    argv = ["smoke", "--out-dir", str(tmp_path / "out"),
            "--config", str(_config(tmp_path)), "--database", "SALES_DB"]
    assert snowmig.main(argv) == 1
    err = capsys.readouterr().err
    assert "error:" in err and "INSERT" in err
    assert not (tmp_path / "out" / "smoke.json").exists()


def test_the_guard_is_scoped_to_the_databases_the_flag_names_not_the_configs(
        tmp_path, monkeypatch, capsys):
    """`--database OTHER_DB` with `database: SALES_DB` in the config:
    discovery reads OTHER_DB, so the guard judges OTHER_DB -- a write there
    refuses the run, a write on SALES_DB (not read) is reported."""
    other_write = _session([
        {"privilege": "INSERT", "granted_on": "TABLE",
         "name": "OTHER_DB.PUBLIC.T"}])
    _wire(monkeypatch, other_write)
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config",
         str(_config(tmp_path)), "--database", "OTHER_DB"])
    with pytest.raises(RoleNotReadOnly, match="OTHER_DB.PUBLIC.T"):
        snowmig._run_sql_from_args(args)

    sales_write = _session([
        {"privilege": "INSERT", "granted_on": "TABLE",
         "name": "SALES_DB.PUBLIC.T"}])
    _wire(monkeypatch, sales_write)
    snowmig._run_sql_from_args(args)
    err = capsys.readouterr().err
    assert "READER is read-only on OTHER_DB" in err

    # Two flags, two databases in the scope; the config's is still not it.
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config",
         str(_config(tmp_path)), "--database", "OTHER_DB",
         "--database", "THIRD_DB"])
    snowmig._run_sql_from_args(args)
    assert "read-only on OTHER_DB, THIRD_DB" in capsys.readouterr().err
    assert snowmig._snowflake_coords(args)["database"] == "SALES_DB", \
        "the coordinate is the config's; the flag is a filter"


def test_without_the_flag_the_guard_is_scoped_to_the_configs_database(
        tmp_path, monkeypatch, capsys):
    _wire(monkeypatch, _reviewer_session())
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config",
         str(_config(tmp_path))])
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        snowmig._run_sql_from_args(args)
    assert snowmig._guard_scope(args, {"database": "SALES_DB"}) == "SALES_DB"
    assert snowmig._guard_scope(args, {"database": None}) is None
    args.database = ["A", "B"]
    assert snowmig._guard_scope(args, {"database": "SALES_DB"}) == ["A", "B"]


# --- 2 · the guard and discovery fold `database:` the same way --------------

@pytest.mark.parametrize("configured,printed", [
    ("my-db", '"my-db".PUBLIC.T'),
    ("Sales DB", '"Sales DB".PUBLIC.T'),
    ("db-x", '"db-x"."PUBLIC"."T"'),
    ('"MyDb"', '"MyDb".PUBLIC.T'),
    ("sales_db", "SALES_DB.PUBLIC.T"),
    ("SALES_DB", "SALES_DB.PUBLIC.T"),
    ("  SALES_DB  ", "SALES_DB.PUBLIC.T"),
    ("sales_db$1", "SALES_DB$1.PUBLIC.T"),
])
def test_a_write_on_the_database_discovery_reads_is_refused_however_spelled(
        configured, printed):
    """`database: my-db` is read verbatim by discovery (not a plain
    identifier) and SHOW GRANTS quotes it the same way; the guard used to
    upper-case it to MY-DB and excuse the write as another database's."""
    cur = FakeCursor([_grant("INSERT", "TABLE", printed)])
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database=configured)
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database=[configured])
    evidence = read_role_grants(cur, database=configured)
    assert evidence["databases"] == [lexer.config_name(configured)], \
        "the scope IS what discovery resolves the config value to"
    assert evidence["out_of_scope_writes"] == 0


@pytest.mark.parametrize("configured,elsewhere", [
    ("my-db", '"MY-DB".PUBLIC.T'),        # a different quoted name
    ("Sales DB", '"SALES DB".PUBLIC.T'),
    ('"MyDb"', '"MYDB".PUBLIC.T'),
    ("sales_db", '"sales_db".PUBLIC.T'),  # quoted lower case is another db
])
def test_a_different_spelling_of_a_quoted_name_is_another_database(
        configured, elsewhere):
    evidence = read_role_grants(
        FakeCursor([_grant("INSERT", "TABLE", elsewhere)]),
        database=configured)
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 1


def test_the_guard_has_no_folding_rule_of_its_own():
    import snowflake_source.role_guard as guard
    assert not hasattr(guard, "_unquote"), \
        "the scope is resolved with lexer.config_name, nothing else"
    assert guard.lexer is lexer


# --- 3 · a secret under any key name is refused and never uploaded ----------

@pytest.mark.parametrize("key,alternative", [
    ("Password", "password_path"), ("PASSWORD", "password_path"),
    ("passwd", "password_path"), ("snowflake_password", "password_path"),
    ("passcode", None), ("pat", "pat_path"), ("oauth_token", "pat_path"),
    ("token", "pat_path"), ("access_token", "pat_path"),
    ("private_key", "key_path"), ("private_key_file", "key_path"),
    ("PRIVATE_KEY_CONTENT", "key_path"),
    ("key_passphrase", "key_passphrase_path"),
    ("passphrase", "key_passphrase_path"),
    ("private_key_passphrase", "key_passphrase_path"),
    ("secret", None), ("client_secret", None),
])
def test_a_secret_like_key_is_refused_with_the_path_field_or_remove_it(
        key, alternative):
    block = {"account": "A", "user": "U", "auth": "password", key: "HUNTER2"}
    with pytest.raises(ConfigError) as caught:
        snowflake_block({"snowflake": block})
    message = str(caught.value)
    assert f"`{key}`" in message and "HUNTER2" not in message
    if alternative:
        assert f"`{key}` -> `{alternative}`" in message
    else:
        assert f"`{key}` -> remove it" in message
        assert "`password_path`" in message
    assert migration_config.inline_secret_keys(block) == [(key, alternative)]
    assert redact(block)[key] == "<redacted>"
    assert "HUNTER2" not in json.dumps(redact(block))


def test_a_nested_secret_is_refused_by_its_dotted_key():
    block = {"account": "A", "extra": {"password": "HUNTER2",
                                       "note": "fine"},
             "more": [{"Token": "T0K"}]}
    with pytest.raises(ConfigError) as caught:
        refuse_inline_secrets(block)
    message = str(caught.value)
    assert "`extra.password` -> `password_path`" in message
    assert "`more.0.Token` -> `pat_path`" in message
    assert "HUNTER2" not in message and "T0K" not in message
    redacted = redact(block)
    assert redacted["extra"] == {"password": "<redacted>", "note": "fine"}
    assert redacted["more"] == [{"Token": "<redacted>"}]


@pytest.mark.parametrize("key", ["password_path", "key_path", "pat_path",
                                 "key_passphrase_path", "Password_Path",
                                 "token_path", "account", "user", "role"])
def test_a_path_field_or_a_coordinate_is_not_a_secret(key):
    assert migration_config.is_secret_key(key) is False
    block = {"account": "A", key: "/some/where"}
    refuse_inline_secrets(block)
    assert redact(block)[key] == "/some/where"


def test_an_unset_secret_like_key_is_not_refused():
    refuse_inline_secrets({"account": "A", "Password": None, "token": ""})


def _mount_config(tmp_path, **extra):
    block = {"account": "ACME-TEST", "user": "READER", "warehouse": "WH",
             "database": "DB", "auth": "password",
             "password_path": write_secret(tmp_path / "pw", "FAKE-PW-123")}
    block.update(extra)
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(yaml.safe_dump({"snowflake": block}), encoding="utf-8")
    return cfg


@pytest.mark.parametrize("extra", [
    {"Password": "HUNTER2"}, {"PASSWORD": "HUNTER2"}, {"passcode": "HUNTER2"},
    {"pat": "HUNTER2"}, {"oauth_token": "HUNTER2"},
    {"private_key_file": "HUNTER2"}, {"extra": {"password": "HUNTER2"}},
])
def test_the_reviewers_repro_a_secret_under_another_key_never_hits_the_mount(
        tmp_path, extra):
    """`Password: HUNTER2` beside `password_path:` was copied, as part of
    the whole block, into the JSON `provision --source-config` uploads."""
    cfg = _mount_config(tmp_path, **extra)
    with pytest.raises(ConfigError) as caught:
        source_config_objects(cfg)
    assert "HUNTER2" not in str(caught.value)
    fake = Fake()
    with pytest.raises(ConfigError):
        provision(call=fake, workspace_name="acme", scripts=[],
                  source_config=cfg, execute=True, delays=())
    assert fake.ops == [], "refused before anything reached AIDP"
    blob = json.dumps(fake.contents)
    assert "HUNTER2" not in blob and "Password" not in blob


def test_the_uploaded_block_is_built_from_the_allowlist_not_the_file(
        tmp_path, monkeypatch):
    """Even a key the parser does not refuse does not travel: the copy on
    the mount carries the coordinates the readers there use and the
    `*_path` fields, and nothing else."""
    cfg = _mount_config(tmp_path, vendor_field="x", note="keep off",
                        extra={"anything": 1}, port=443, role="R",
                        schema="PUBLIC", host="h")
    payload, files = source_config_objects(cfg)
    block = payload["snowflake"]
    assert set(block) <= set(provisioning.MOUNT_CONFIG_FIELDS)
    assert "vendor_field" not in block and "note" not in block
    assert "extra" not in block
    assert block["password_path"].startswith("/Workspace/")
    assert block["role"] == "R" and block["schema"] == "PUBLIC"
    assert [remote for _, remote in files] == [
        f"{PLAN_FOLDER}/snowmig-config.password_path"]
    # ...and the parser is not what keeps an unknown key off the mount.
    monkeypatch.setattr(provisioning, "snowflake_block",
                        lambda config: dict(config["snowflake"]))
    block = source_config_objects(cfg)[0]["snowflake"]
    assert "vendor_field" not in block and "extra" not in block
    assert set(block) <= set(provisioning.MOUNT_CONFIG_FIELDS)
    assert set(provisioning.MOUNT_CONFIG_FIELDS) >= {
        "account", "host", "port", "user", "role", "warehouse", "database",
        "schema", "auth", "key_path", "key_passphrase_path",
        "password_path", "pat_path"}


def test_the_mount_allowlist_covers_what_the_readers_on_the_mount_read():
    """Every key `dataplane/snowmig_source.SnowflakeSource`,
    `diagnose_environment` and the catalog registration read from the
    source config is on the allowlist, so the copy still works."""
    engine = TESTS_DIR.parent
    read_keys = set()
    for rel in ("dataplane/snowmig_source.py",
                "dataplane/diagnose_environment.py",
                "target/snowflake_catalog_connection.py"):
        text = (engine / rel).read_text(encoding="utf-8")
        read_keys |= set(re.findall(r'\bcfg\.get\("([a-z_]+)"', text))
        read_keys |= set(re.findall(r'\bcfg\["([a-z_]+)"\]', text))
        read_keys |= set(re.findall(r'\bconfig\.get\("([a-z_]+)"', text))
        read_keys |= set(re.findall(r'\bconfig\["([a-z_]+)"\]', text))
    # The inline names are read only to REFUSE them; they are not carried.
    read_keys -= {"private_key", "password", "key_passphrase"}
    assert read_keys, "the scan found the readers"
    assert read_keys <= set(provisioning.MOUNT_CONFIG_FIELDS), \
        read_keys - set(provisioning.MOUNT_CONFIG_FIELDS)


# --- 4 · no test patches os.name or sys.platform ----------------------------

def test_no_test_in_this_suite_patches_os_name_or_sys_platform():
    """`monkeypatch.setattr(os, "name", ...)` lies to pathlib too: on
    Python 3.11 every Path(...) then raises NotImplementedError and pytest
    INTERNALERRORs, so a full run reports nothing. The platform is a
    module flag (`_IS_WINDOWS`) that tests patch instead."""
    offenders = []
    for path in TESTS_DIR.glob("*.py"):
        if path.name == pathlib.Path(__file__).name:
            continue  # this test quotes the patterns it looks for
        text = path.read_text(encoding="utf-8")
        if re.search(r'setattr\(\s*os\s*,\s*["\']name["\']', text):
            offenders.append(f"{path.name}: os.name")
        if re.search(r'setattr\(\s*sys\s*,\s*["\']platform["\']', text):
            offenders.append(f"{path.name}: sys.platform")
        if re.search(r'os\.name\s*=\s*["\']', text):
            offenders.append(f"{path.name}: os.name =")
    assert offenders == []


def test_the_platform_is_a_module_flag_the_check_consults(tmp_path,
                                                          monkeypatch):
    import migration_config
    import dataplane.snowmig_source as src
    assert migration_config._IS_WINDOWS == (os.name == "nt")
    assert src._IS_WINDOWS == (os.name == "nt")
    path = write_secret(tmp_path / "pw", "s3cret")
    world_readable(path)
    monkeypatch.setattr(migration_config, "_IS_WINDOWS", True)
    assert "Windows" in check_secret_file(path, field="password_path")
    assert read_secret_file(path, field="password_path") == "s3cret"
    assert os.name == ("nt" if os.name == "nt" else os.name), \
        "os.name itself was not touched"
    # The other platform's path, on any host: a 0644 (or, on Windows, the
    # 0666 every file reports) is refused with the chmod that fixes it.
    monkeypatch.setattr(migration_config, "_IS_WINDOWS", False)
    with pytest.raises(ConfigError, match="group or others"):
        check_secret_file(path, field="password_path")
    with pytest.raises(ConfigError, match="group or others"):
        read_secret_file(path, field="password_path")
    monkeypatch.setattr(src, "_IS_WINDOWS", False)
    with pytest.raises(src.SourceConfigError, match="group or others"):
        src.read_secret_file(path, "password_path")


# --- 5 · the connector resolves the path first, and reads regular files -----

def _real(path) -> str:
    return pathlib.Path(os.path.realpath(path)).as_posix()


def test_a_dot_dot_through_the_mount_prefix_does_not_skip_the_mode_check(
        tmp_path, monkeypatch):
    import dataplane.snowmig_source as src
    mount = tmp_path / "mount"
    mount.mkdir()
    outside = write_secret(tmp_path / "outside" / "pw", "pw")
    world_readable(outside)
    monkeypatch.setattr(src, "WORKSPACE_MOUNT", _real(mount) + "/")
    monkeypatch.setattr(src, "_IS_WINDOWS", False)
    sneaky = f"{_real(mount)}/../outside/pw"
    assert sneaky.startswith(src.WORKSPACE_MOUNT), "the raw prefix matches"
    with pytest.raises(src.SourceConfigError, match="group or others"):
        src.read_secret_file(sneaky, "password_path")
    # A file really on the mount is still read whatever its mode.
    on_mount = write_secret(mount / "pw", "mounted")
    world_readable(on_mount)
    assert src.read_secret_file(on_mount, "password_path") == "mounted"


@POSIX_ONLY
def test_a_symlink_under_the_mount_to_a_file_outside_it_is_checked(
        tmp_path, monkeypatch):
    import dataplane.snowmig_source as src
    mount = tmp_path / "mount"
    mount.mkdir()
    outside = write_secret(tmp_path / "outside" / "pw", "pw")
    os.chmod(outside, 0o644)
    (mount / "pw").symlink_to(outside)
    monkeypatch.setattr(src, "WORKSPACE_MOUNT", _real(mount) + "/")
    monkeypatch.setattr(src, "_IS_WINDOWS", False)
    with pytest.raises(src.SourceConfigError, match="group or others"):
        src.read_secret_file(mount / "pw", "password_path")


@POSIX_ONLY
def test_a_fifo_is_refused_without_blocking(tmp_path):
    """A FIFO opened for reading waits for a writer for ever; the connector
    used to stat it, see no mode problem, and hang in read_text."""
    import dataplane.snowmig_source as src
    fifo = tmp_path / "pw"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(src.SourceConfigError, match="regular file"):
        src.read_secret_file(fifo, "password_path")
    with pytest.raises(ConfigError, match="regular file"):
        read_secret_file(fifo, field="password_path")
    with pytest.raises(ConfigError, match="regular file"):
        check_secret_file(fifo, field="password_path")


def test_a_directory_is_not_a_credential_file_for_the_connector(tmp_path):
    import dataplane.snowmig_source as src
    with pytest.raises(src.SourceConfigError,
                       match="regular file|not readable"):
        src.read_secret_file(tmp_path, "password_path")


# --- 6 · a re-push never bakes a credential FILE in as --source-config ------

def _cfg(tmp_path, stem):
    path = tmp_path / f"{stem}.yaml"
    path.write_text(yaml.safe_dump({"snowflake": {
        "account": "ACME-TEST", "user": "READER", "warehouse": "WH",
        "database": "DB", "auth": "password",
        "password_path": write_secret(tmp_path / "pw", "FAKE-PW-456")}}),
        encoding="utf-8")
    return path


def _cli(tmp_path, monkeypatch, world):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)

    def run(*extra):
        return snowmig.main(["provision", "--datalake-ocid",
                             "ocid1.aidataplatform.oc1.iad.aaaafake",
                             "--workspace-name", "acme", "--skip-libraries",
                             "--out-dir", str(tmp_path / "out"), "--execute",
                             *extra])
    return run


def _record(tmp_path):
    return json.loads((tmp_path / "out" / "provision_result.json")
                      .read_text(encoding="utf-8"))


def _params(world, notebook="00_discover_snowflake.ipynb"):
    nb = json.loads(world.contents[f"{SCRIPTS_FOLDER}/{notebook}"]["body"])
    return "".join(nb["cells"][1]["source"])


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def test_the_reviewers_repro_the_key_file_is_never_the_source_config(
        tmp_path, monkeypatch, capsys):
    """Push with --source-config; the JSON is removed from the mount; a
    re-push without --source-config records it missing and keeps only
    plan/a.password_path in `credential_objects`; the NEXT re-push used to
    inherit that file and write source-config=/Workspace/plan/
    a.password_path into every notebook."""
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert run("--source-config", str(_cfg(tmp_path, "a"))) == 0
    config_obj = f"{PLAN_FOLDER}/a.json"
    file_obj = f"{PLAN_FOLDER}/a.password_path"
    first = _record(tmp_path)
    assert first["credential_config"] == config_obj
    assert set(first["credential_objects"]) == {config_obj, file_obj}
    del world.contents[config_obj]            # removed by hand on the mount
    capsys.readouterr()

    assert run("--reuse-existing", "--refresh-notebooks") == 1
    second = _record(tmp_path)
    assert second["credential_missing"] == [config_obj]
    assert second["credential_objects"] == [file_obj]
    assert second["credential_config"] is None
    assert "source-config': '/Workspace" not in _params(world)
    capsys.readouterr()

    rc = run("--reuse-existing", "--refresh-notebooks")
    err = capsys.readouterr().err
    third = _record(tmp_path)
    params = _params(world)
    assert f"/Workspace/{file_obj}" not in params, \
        "the credential FILE is not the notebooks' source config"
    assert "source-config': '/Workspace" not in params
    assert ".password_path" not in (third.get("credential_config") or "")
    assert third["credential_config"] is None
    # The config is still named as missing, so the operator is told again
    # rather than left with notebooks that point at nothing.
    assert third["credential_missing"] == [config_obj]
    assert rc == 1, "a push whose notebooks point at nothing is not a pass"
    step = next(s for s in third["steps"] if s["step"] == "credential")
    assert step["verified"] is False
    assert "a.json" in step["detail"] and "--source-config" in step["detail"]
    assert f"{PLAN_FOLDER}/a.json holds" not in err
    for nb in ("00_discover_snowflake.ipynb", "01_create_structure.ipynb",
               "02_copy_schema.ipynb"):
        assert ".password_path'" not in _params(world, nb)


def test_a_re_push_with_the_config_still_there_inherits_the_config(
        tmp_path, monkeypatch, capsys):
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert run("--source-config", str(_cfg(tmp_path, "a"))) == 0
    capsys.readouterr()
    assert run("--reuse-existing", "--refresh-notebooks") == 0
    rec = _record(tmp_path)
    assert rec["credential_config"] == f"{PLAN_FOLDER}/a.json"
    assert f"'source-config': '/Workspace/{PLAN_FOLDER}/a.json'" in \
        _params(world)


@pytest.mark.parametrize("prior,expected", [
    ({"credential_objects": ["plan/a.password_path"]}, None),
    ({"credential_objects": ["plan/a.key_path", "plan/a.pat_path",
                             "plan/a.key_passphrase_path"]}, None),
    ({"credential_objects": ["plan/a.password_path", "plan/a.json"]},
     "plan/a.json"),
    ({"credential_config": "plan/a.json",
      "credential_objects": ["plan/a.password_path"]}, "plan/a.json"),
    ({"credential_requested": "plan/b.json",
      "credential_config": "plan/a.json"}, "plan/b.json"),
    ({"credential_unconfirmed": ["plan/a.password_path", "plan/a.json"]},
     "plan/a.json"),
    ({"credential_missing": ["plan/a.json"],
      "credential_objects": ["plan/a.password_path"]}, "plan/a.json"),
    ({"credential_objects": ["plan/a.json"],
      "credential_superseded": ["plan/a.json"]}, None),
    ({}, None),
])
def test_only_a_config_object_is_ever_inherited(prior, expected):
    assert provisioning._current_credential(prior) == expected


def test_the_dry_run_record_names_the_config_object_too(tmp_path):
    cfg = _cfg(tmp_path, "a")
    res = provision(call=None, workspace_name="acme", scripts=[],
                    source_config=cfg, execute=False)
    assert res["credential_config"] == f"{PLAN_FOLDER}/a.json"
    res = provision(call=None, workspace_name="acme", scripts=[],
                    execute=False)
    assert res["credential_config"] is None


# --- 7 · the credential file is judged on the descriptor that is read -------

def test_the_read_goes_through_the_opened_descriptor_not_a_second_open(
        tmp_path, monkeypatch):
    """No stat-then-reopen: `Path.read_text` / `read_bytes` are never
    called, so there is no window in which the name could be re-pointed
    between the check and the read."""
    import dataplane.snowmig_source as src

    def boom(*_a, **_k):
        raise AssertionError("reopened the credential file by name")

    monkeypatch.setattr(pathlib.Path, "read_text", boom)
    monkeypatch.setattr(pathlib.Path, "read_bytes", boom)
    path = write_secret(tmp_path / "pw", "  s3cret \n")
    assert read_secret_file(path, field="password_path") == "s3cret"
    assert migration_config.read_secret_bytes(
        path, field="password_path").strip() == b"s3cret"
    assert src.read_secret_file(path, "password_path") == "s3cret"
    from snowflake_source.conn import _read_secret_file, build_connect_kwargs
    assert _read_secret_file(path, "password file") == "s3cret"
    assert build_connect_kwargs("password", account="a", user="u",
                                password_path=path)["password"] == "s3cret"


@pytest.mark.parametrize("content", ["", "   ", "\n\n", "\r\n"])
def test_an_empty_credential_file_is_refused_as_such(tmp_path, content):
    """Read as '' it surfaced later as a confusing "needs password_path"
    although the path was set; now the message says the FILE is empty."""
    import dataplane.snowmig_source as src
    from snowflake_source.conn import (build_connect_kwargs,
                                       load_private_key_der)
    path = write_secret(tmp_path / "pw", content)
    with pytest.raises(ConfigError) as caught:
        read_secret_file(path, field="password_path")
    message = str(caught.value)
    assert "is empty" in message and "`password_path`" in message
    assert str(tmp_path) not in str(caught.value)
    with pytest.raises(src.SourceConfigError, match="is empty"):
        src.read_secret_file(path, "password_path")
    with pytest.raises(AuthError, match="is empty"):
        build_connect_kwargs("password", account="a", user="u",
                             password_path=path)
    with pytest.raises(AuthError, match="is empty"):
        build_connect_kwargs("pat", account="a", user="u", pat_path=path)
    with pytest.raises(AuthError, match="is empty"):
        load_private_key_der(path)


def test_an_empty_credential_file_on_the_mount_is_refused_too(tmp_path,
                                                              monkeypatch):
    import dataplane.snowmig_source as src
    monkeypatch.setattr(src, "WORKSPACE_MOUNT", _real(tmp_path) + "/")
    path = write_secret(tmp_path / "pw", "")
    with pytest.raises(src.SourceConfigError, match="is empty"):
        src.read_secret_file(path, "password_path")
    with pytest.raises(src.SourceConfigError, match="is empty"):
        src.SnowflakeSource(object(), config={
            "account": "a", "warehouse": "w", "database": "d", "user": "u",
            "auth": "password", "password_path": path})


def test_the_connector_says_empty_not_missing_path(tmp_path):
    import dataplane.snowmig_source as src
    with pytest.raises(src.SourceConfigError) as caught:
        src.SnowflakeSource(object(), config={
            "account": "a", "warehouse": "w", "database": "d", "user": "u",
            "auth": "password", "password_path": temp_secret("")})
    assert "is empty" in str(caught.value)
    assert "needs `password_path`" not in str(caught.value)


def test_the_private_key_is_read_once_through_the_shared_rule(tmp_path,
                                                              monkeypatch):
    from snowflake_source import conn
    monkeypatch.setattr(pathlib.Path, "read_bytes",
                        lambda *_a, **_k: (_ for _ in ()).throw(
                            AssertionError("reopened by name")))
    seen = {}

    def fake_load(raw, password=None):
        seen["raw"] = raw

        class Key:
            def private_bytes(self, **_k):
                return b"DER"
        return Key()

    import cryptography.hazmat.primitives.serialization as ser
    monkeypatch.setattr(ser, "load_pem_private_key", fake_load)
    path = write_secret(tmp_path / "k.p8", "PEM-BODY")
    assert conn.load_private_key_der(path) == b"DER"
    assert seen["raw"] == b"PEM-BODY"


# --- 8 · an unplaceable procedure row is refused ----------------------------

LEGITIMATE = [
    _grant("USAGE", "DATABASE", "SALES_DB"),
    _grant("USAGE", "SCHEMA", "SALES_DB.PUBLIC"),
    _grant("SELECT", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
]


@pytest.mark.parametrize("granted_on,name", [
    ("PROCEDURE", "SALES_DB.LOAD(VARCHAR)"),              # two parts
    ("PROCEDURE", "SALES_DB.PUBLIC.X.LOAD(VARCHAR)"),     # four parts
    ("PROCEDURE", "SALES_DB .PUBLIC.LOAD(VARCHAR)"),      # padded part
    ("PROCEDURE", "LOAD(VARCHAR)"),                       # one part
    ("PROCEDURE", ""),                                    # blank
    ("NOTEBOOK", "SALES_DB.NB"),
    ("STREAMLIT", "SALES_DB.PUBLIC.X.APP"),
])
def test_a_procedure_row_that_cannot_be_placed_gets_no_allowance(granted_on,
                                                                  name):
    """USAGE of a procedure passes INSIDE `database:` because discovery
    reads it there. A row with two or four name parts is not shown to be
    inside -- it cannot be placed at all -- and used to pass by default."""
    cur = FakeCursor(LEGITIMATE + [_grant("USAGE", granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert f"USAGE on {granted_on}" in message
    assert "cannot be placed" in message and "allowance" in message
    evidence = read_role_grants(cur, database=["SALES_DB"])
    assert evidence["read_only"] is False
    assert evidence["out_of_scope_writes"] == 0


def test_a_well_formed_procedure_row_inside_the_scope_still_passes():
    cur = FakeCursor(LEGITIMATE + [
        _grant("USAGE", "PROCEDURE", "SALES_DB.PUBLIC.LOAD(VARCHAR, NUMBER)"),
        _grant("USAGE", "PROCEDURE", '"SALES_DB"."PUBLIC"."Load"(VARCHAR)'),
        _grant("USAGE", "NOTEBOOK", "SALES_DB.PUBLIC.NB")])
    assert assert_role_read_only(cur, database="SALES_DB")["read_only"]
    assert assert_role_read_only(cur, database=["SALES_DB"])["read_only"]


def test_without_a_scope_the_allowance_stands_and_the_readme_says_to_set_one():
    """With no `database:` nothing can be placed, so the documented
    allowance applies everywhere; the README therefore asks that
    `database:` always be set, and says why."""
    cur = FakeCursor(LEGITIMATE + [
        _grant("USAGE", "PROCEDURE", "SALES_DB.LOAD(VARCHAR)")])
    assert assert_role_read_only(cur, database=None)["read_only"]
    readme = (TESTS_DIR.parent.parent / "README.md").read_text(
        encoding="utf-8")
    marker = "`database:` should always be set"
    assert marker in readme
    assert "procedure" in readme.split(marker, 1)[1][:900]


# --- nits -------------------------------------------------------------------

def test_the_template_wording_does_not_say_the_config_holds_a_password(
        tmp_path):
    template = tmp_path / "template.yaml"
    template.write_text("snowflake: {}\n", encoding="utf-8")
    dest = tmp_path / "snowmig-config.yaml"
    write_template(dest, template=template)
    with pytest.raises(ConfigError) as caught:
        write_template(dest, template=template)
    message = str(caught.value)
    assert "holds credentials" not in message
    assert "credential file" in message
    import migration_config
    assert "plain text" not in (write_template.__doc__ or "")
    assert "holds the password" not in (migration_config._yaml_refusal.__doc__
                                        or "")
    bad = tmp_path / "bad.yaml"
    bad.write_text("snowflake: {account: [}\n", encoding="utf-8")
    from migration_config import load_config
    with pytest.raises(ConfigError) as caught:
        load_config(bad)
    assert "holds a credential" not in str(caught.value)
    assert "by mistake" in str(caught.value)


@POSIX_ONLY
@pytest.mark.parametrize("mode,who", [(0o640, "group"), (0o604, "others"),
                                      (0o660, "group"), (0o644, "both")])
def test_a_group_readable_file_is_refused_as_group_or_others(tmp_path, mode,
                                                             who):
    import dataplane.snowmig_source as src
    path = write_secret(tmp_path / "pw", "pw")
    os.chmod(path, mode)
    with pytest.raises(ConfigError) as caught:
        check_secret_file(path, field="password_path")
    assert "readable by its group or others" in str(caught.value)
    assert f"{mode:04o}" in str(caught.value)
    with pytest.raises(src.SourceConfigError,
                       match="readable by its group or others"):
        src.read_secret_file(path, "password_path")
