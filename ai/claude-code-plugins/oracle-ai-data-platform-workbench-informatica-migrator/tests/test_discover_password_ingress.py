"""How the repository password reaches `discover`, pinned (SEC-AIDP-SAMPLES-002).

`discover` is the only command that authenticates to a live system, and its
password used to be an ordinary `--password` flag: visible in `ps` to every
user on the host, kept in shell history, copied into CI transcripts and
into the service requests a failing command gets pasted into. The crawler
was already careful on its own side (`pmrep -X`, TLS on by default -- see
test_crawler_security.py); these tests pin the ingress to the same posture:

  - `--password <value>` is refused with the remediation text, exit 2, and
    the value is never echoed.
  - `--password-file` is the file-based alternative; on POSIX a file that
    anyone but its owner can read is refused. Windows has no such mode bits,
    so the rule is pinned there through the seam `secret_ingress._file_mode`.
  - `INFA_PASSWORD` in the environment still works.
  - Once the password is in memory it is redacted from every log record,
    verbose or not -- including a library exception that echoes a request
    URL or the XML-escaped request body -- and the connection config's
    repr never shows it. `-v` logs a redacted traceback instead of
    re-raising (a re-raise is printed by the interpreter past every
    logging filter), pinned through a real subprocess.
  - A password file written by Notepad or PowerShell (UTF-8 BOM, UTF-16)
    is read correctly; a file that is not text is a clear error.
  - A host of the form `user:secret@host` -- or one carrying a path, query
    or fragment -- is refused, since it would put the credential into
    every request URL and exception message.
  - Option prefixes are not accepted (`--password-fil <secret>` must not
    become `--password-file`), and the values after an unrecognised option
    are masked in argparse's error.
  - The SOAP LoginRequest XML-escapes every field, so a password with `&`
    or `<` cannot break or rewrite the request.

These assert behaviour, not wording, except for the one string a user is
meant to read: the remediation text itself.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import textwrap
import types

import pytest

from infa2aidp import secret_ingress
from infa2aidp.cli import build_parser, main
from infa2aidp.secret_ingress import (
    PASSWORD_ARGV_REFUSAL,
    REDACTED,
    RedactingFilter,
    read_secret_file,
    resolve_password,
)
from infa2aidp.crawlers.informatica_crawler import (
    InfaConnectionConfig,
    InformaticaCrawler,
)

SECRET = "s3cr3t-do-not-leak"
POSIX_ONLY = pytest.mark.skipif(os.name == "nt", reason="st_mode permission bits are meaningless on Windows")


@pytest.fixture
def fake_crawler(monkeypatch):
    """Stand-in for InformaticaCrawler that records the config it was built
    with. `connect_error`, when set, is raised from connect() -- the shape
    of a library error that echoes connection details."""
    import infa2aidp.crawlers.informatica_crawler as crawler_mod

    seen: dict = {}

    class FakeCrawler:
        connect_error: "Exception | None" = None

        def __init__(self, cfg):
            seen["cfg"] = cfg

        def connect(self, method):
            if FakeCrawler.connect_error is not None:
                raise FakeCrawler.connect_error
            return "soap"

        def crawl_repository(self, out_dir, folders=None, export_xml=False):
            return types.SimpleNamespace(mappings=[1], workflows=[], exported_xml_files=["a.xml"], errors=[])

        def generate_inventory_report(self, result, path):
            seen["report"] = path

        def disconnect(self):
            seen["closed"] = True

    monkeypatch.setattr(crawler_mod, "InformaticaCrawler", FakeCrawler)
    monkeypatch.delenv("INFA_PASSWORD", raising=False)
    return seen, FakeCrawler


def _password_file(tmp_path, content=SECRET):
    p = tmp_path / "infa.pw"
    p.write_text(content + "\n", encoding="utf-8")
    if os.name != "nt":
        os.chmod(p, 0o600)
    return str(p)


# ── --password is refused ──────────────────────────────────────────────

@pytest.mark.parametrize("argv_tail", [
    ["--password", SECRET],
    [f"--password={SECRET}"],
    ["--password"],                       # bare flag: still the wrong habit
    ["--password", SECRET, "--repo", "R"],
])
def test_password_on_argv_is_refused_with_remediation_text(argv_tail, capsys, fake_crawler):
    with pytest.raises(SystemExit) as exc:
        main(["discover", "--host", "pc.example", *argv_tail])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert PASSWORD_ARGV_REFUSAL in err
    assert SECRET not in err, "the refused value must not be echoed back"
    assert "cfg" not in fake_crawler[0], "refused before any connection config was built"


def test_the_remediation_text_names_both_alternatives():
    assert "--password-file" in PASSWORD_ARGV_REFUSAL
    assert "INFA_PASSWORD" in PASSWORD_ARGV_REFUSAL


def test_password_is_not_a_plain_store_action():
    """`--password x` must not be parsed as a boolean (leaving `x` to become
    a stray positional) nor stored anywhere -- it has to error out."""
    p = build_parser()
    with pytest.raises(SystemExit):
        p.parse_args(["discover", "--host", "h", "--password", "x"])


# ── --password-file and INFA_PASSWORD ──────────────────────────────────

def test_password_file_is_read_and_reaches_the_crawler(tmp_path, fake_crawler, capsys):
    seen, _ = fake_crawler
    rc = main(["discover", "--host", "pc.example", "--password-file", _password_file(tmp_path), "-o", str(tmp_path / "out")])
    assert rc == 0
    assert seen["cfg"].password == SECRET
    assert SECRET not in capsys.readouterr().out


def test_password_file_wins_over_the_environment(tmp_path, fake_crawler, monkeypatch):
    seen, _ = fake_crawler
    monkeypatch.setenv("INFA_PASSWORD", "from-env")
    main(["discover", "--host", "pc.example", "--password-file", _password_file(tmp_path), "-o", str(tmp_path / "out")])
    assert seen["cfg"].password == SECRET


def test_environment_password_is_still_supported(tmp_path, fake_crawler, monkeypatch):
    seen, _ = fake_crawler
    monkeypatch.setenv("INFA_PASSWORD", "from-env")
    assert main(["discover", "--host", "pc.example", "-o", str(tmp_path / "out")]) == 0
    assert seen["cfg"].password == "from-env"


def test_resolve_password_reports_the_source_not_the_value(tmp_path, monkeypatch):
    monkeypatch.delenv("INFA_PASSWORD", raising=False)
    assert resolve_password(None) == ("", "none")
    monkeypatch.setenv("INFA_PASSWORD", "from-env")
    assert resolve_password(None) == ("from-env", "INFA_PASSWORD")
    assert resolve_password(_password_file(tmp_path)) == (SECRET, "--password-file")


def test_password_file_takes_the_first_line_only(tmp_path):
    p = tmp_path / "pw"
    p.write_text(f"  {SECRET}  \nsecond line is ignored\n", encoding="utf-8")
    if os.name != "nt":
        os.chmod(p, 0o600)
    assert read_secret_file(str(p)) == SECRET


@pytest.mark.parametrize("make", [
    lambda d: str(d / "missing.pw"),
    lambda d: (d.mkdir(exist_ok=True), str(d))[1],                       # a directory
    lambda d: (d.joinpath("empty.pw").write_text("\n"), str(d / "empty.pw"))[1],
])
def test_unusable_password_file_is_a_clear_error_without_content(tmp_path, make):
    path = make(tmp_path)
    with pytest.raises(ValueError) as exc:
        read_secret_file(path, enforce_mode=False)
    assert "password file" in str(exc.value)


@pytest.mark.parametrize("encoding, newline", [
    ("utf-8-sig", "\n"),      # Notepad "UTF-8 with BOM", PowerShell 5.1 Out-File -Encoding utf8
    ("utf-16", "\r\n"),       # PowerShell 5.1 `echo pw > file` (UTF-16 LE with BOM)
    ("utf-16-be", None),      # big-endian with an explicit BOM
    ("utf-8", "\r\n"),        # CRLF, no BOM
])
def test_password_file_as_windows_tools_write_it_is_read_whole(tmp_path, encoding, newline):
    """A BOM is not part of the password, and UTF-16 is text: before this a
    BOM-prefixed file logged in with U+FEFF glued to the password and a
    UTF-16 file raised an undocumented UnicodeDecodeError."""
    p = tmp_path / "pw"
    if encoding == "utf-16-be":
        p.write_bytes(b"\xfe\xff" + (SECRET + "\nsecond\n").encode("utf-16-be"))
    else:
        p.write_bytes((SECRET + newline + "second line" + newline).encode(encoding))
    assert read_secret_file(str(p), enforce_mode=False) == SECRET


@pytest.mark.parametrize("data", [
    b"\x80\x81\x82\n",                                  # not UTF-8 at all
    (SECRET + "\n").encode("utf-16-le"),                # UTF-16 without a BOM: NULs, not text
])
def test_password_file_that_is_not_text_is_a_value_error_naming_the_path_only(tmp_path, data):
    p = tmp_path / "pw"
    p.write_bytes(data)
    with pytest.raises(ValueError) as exc:
        read_secret_file(str(p), enforce_mode=False)
    msg = str(exc.value)
    assert "password file" in msg and "UTF-8" in msg and str(p) in msg
    assert SECRET not in msg and "\\x" not in msg


# ── permission check: owner-only on POSIX, skipped on Windows ─────────

@POSIX_ONLY
@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o660, 0o666])
def test_group_or_world_readable_password_file_is_refused(tmp_path, mode):
    p = tmp_path / "pw"
    p.write_text(SECRET, encoding="utf-8")
    os.chmod(p, mode)
    with pytest.raises(ValueError) as exc:
        read_secret_file(str(p))
    msg = str(exc.value)
    assert f"{mode:04o}" in msg and "600" in msg
    assert SECRET not in msg


@POSIX_ONLY
@pytest.mark.parametrize("mode", [0o600, 0o400])
def test_owner_only_password_file_is_accepted(tmp_path, mode):
    p = tmp_path / "pw"
    p.write_text(SECRET, encoding="utf-8")
    os.chmod(p, mode)
    assert read_secret_file(str(p)) == SECRET


@POSIX_ONLY
def test_cli_refuses_a_world_readable_password_file(tmp_path, fake_crawler, caplog):
    p = tmp_path / "pw"
    p.write_text(SECRET, encoding="utf-8")
    os.chmod(p, 0o644)
    with caplog.at_level(logging.DEBUG):
        rc = main(["discover", "--host", "pc.example", "--password-file", str(p), "-o", str(tmp_path / "out")])
    assert rc == 1
    assert "cfg" not in fake_crawler[0], "no connection may be attempted with a refused file"
    assert SECRET not in caplog.text


def test_the_mode_rule_is_pinned_independently_of_the_filesystem(tmp_path, monkeypatch):
    """Windows cannot express 0600, so the POSIX rule is pinned through the
    seam the check reads the mode from -- on every platform."""
    p = tmp_path / "pw"
    p.write_text(SECRET, encoding="utf-8")
    monkeypatch.setattr(secret_ingress, "_file_mode", lambda path: 0o644)
    with pytest.raises(ValueError):
        read_secret_file(str(p), enforce_mode=True)
    monkeypatch.setattr(secret_ingress, "_file_mode", lambda path: 0o600)
    assert read_secret_file(str(p), enforce_mode=True) == SECRET


def test_windows_skips_the_mode_check_rather_than_refusing_every_file(tmp_path, monkeypatch, caplog):
    p = tmp_path / "pw"
    p.write_text(SECRET, encoding="utf-8")
    monkeypatch.setattr(secret_ingress, "_file_mode", lambda path: 0o666)   # what Windows reports
    with caplog.at_level(logging.DEBUG, logger="infa2aidp.secret_ingress"):
        # enforce_mode=False is exactly what the default resolves to on nt.
        assert read_secret_file(str(p), enforce_mode=False) == SECRET
    if os.name == "nt":
        assert read_secret_file(str(p)) == SECRET
    assert "permission check skipped" in caplog.text
    assert SECRET not in caplog.text


# ── diagnostics never carry the password ──────────────────────────────

def test_verbose_diagnostics_redact_the_password(tmp_path, fake_crawler, caplog):
    """A library error that echoes the request URL -- the realistic leak --
    reaches the log with the password replaced, in verbose mode too."""
    _, FakeCrawler = fake_crawler
    FakeCrawler.connect_error = ConnectionError(
        f"login failed: http://admin:{SECRET}@pc.example:6005/wsh/services/MetadataService"
    )
    try:
        with caplog.at_level(logging.DEBUG):
            rc = main(["discover", "--host", "pc.example", "--password-file", _password_file(tmp_path), "-o", str(tmp_path / "out")])
    finally:
        FakeCrawler.connect_error = None
    assert rc == 1
    assert SECRET not in caplog.text
    assert REDACTED in caplog.text
    assert "pc.example" in caplog.text, "host stays visible -- it is the password that is secret"


def test_verbose_failure_is_exit_1_with_a_redacted_traceback_not_a_reraise(tmp_path, fake_crawler, caplog):
    """`-v` used to re-raise. A re-raised exception is printed by the
    interpreter through sys.excepthook, which no logging filter sees, so
    the traceback carried the raw password. Verbose mode now gets the
    traceback through logging, where it is redacted, and exits 1."""
    _, FakeCrawler = fake_crawler
    FakeCrawler.connect_error = ConnectionError(f"PMREP connect failed: pmrep connect -x {SECRET}")
    try:
        with caplog.at_level(logging.DEBUG):
            rc = main(["discover", "-v", "--host", "pc.example", "--password-file", _password_file(tmp_path), "-o", str(tmp_path / "out")])
    finally:
        FakeCrawler.connect_error = None
    assert rc == 1
    assert "Traceback" in caplog.text and "ConnectionError" in caplog.text, "verbose still gets the traceback"
    assert SECRET not in caplog.text
    assert REDACTED in caplog.text


def test_verbose_failure_stderr_of_a_real_process_carries_no_password(tmp_path):
    """Through a real interpreter, because that is where the leak was: the
    in-process test above only sees what logging captured, while a re-raise
    reaches stderr through a path logging never touches."""
    import infa2aidp
    engine = os.path.dirname(os.path.dirname(os.path.abspath(infa2aidp.__file__)))
    out = str(tmp_path / "out")
    code = textwrap.dedent("""
        import os, sys
        import infa2aidp.crawlers.informatica_crawler as m
        from infa2aidp.cli import main
        class Fake:
            def __init__(self, cfg): pass
            def connect(self, method):
                raise ConnectionError("login failed: body=<Password>" + os.environ["INFA_PASSWORD"] + "</Password>")
        m.InformaticaCrawler = Fake
        sys.exit(main(["discover", "-v", "--host", "pc.example", "-o", sys.argv[1]]))
    """)
    env = {**os.environ, "PYTHONPATH": engine, "INFA_PASSWORD": SECRET,
           "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.run([sys.executable, "-c", code, out], capture_output=True, text=True,
                          env=env, cwd=str(tmp_path), timeout=120)
    assert proc.returncode == 1, proc.stderr
    assert SECRET not in proc.stderr and SECRET not in proc.stdout
    assert REDACTED in proc.stderr
    assert "Traceback" in proc.stderr, "verbose still shows where it failed"


# ── argv near-misses of the refused flag ──────────────────────────────

def test_option_prefixes_are_not_accepted(fake_crawler, capsys):
    """argparse prefix matching turned `--password-fil <secret>` into
    `--password-file <secret>`: the secret became a path, the file reader
    failed, and its error named the path. Exact spellings only."""
    with pytest.raises(SystemExit) as exc:
        main(["discover", "--host", "pc.example", "--password-fil", SECRET])
    assert exc.value.code == 2
    assert SECRET not in capsys.readouterr().err
    assert "cfg" not in fake_crawler[0]


@pytest.mark.parametrize("argv_tail", [
    ["--pasword", SECRET],                 # typo of the refused flag
    [f"--pasword={SECRET}"],
    ["--passwo", SECRET],                  # would have been "ambiguous", now plainly unknown
    ["--", "--password", SECRET],
])
def test_values_after_an_unrecognised_option_are_masked(argv_tail, capsys):
    with pytest.raises(SystemExit) as exc:
        main(["discover", "--host", "pc.example", *argv_tail])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "unrecognized arguments" in err
    assert SECRET not in err
    assert REDACTED in err


def test_successful_discovery_logs_the_source_not_the_secret(tmp_path, fake_crawler, caplog, monkeypatch):
    monkeypatch.setenv("INFA_REPO", "REP")
    with caplog.at_level(logging.DEBUG):
        main(["discover", "-v", "--host", "pc.example", "--password-file", _password_file(tmp_path), "-o", str(tmp_path / "out")])
    assert "password from --password-file" in caplog.text
    assert "REP" in caplog.text
    assert SECRET not in caplog.text


def test_redacting_filter_covers_message_args_and_exceptions():
    flt = RedactingFilter([SECRET, "short"])
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, f"url=http://u:{SECRET}@h %s %s",
                            (ValueError(f"bad {SECRET}"), {"k": SECRET}), None)
    assert flt.filter(rec)
    assert SECRET not in rec.getMessage() and REDACTED in rec.getMessage()
    try:
        raise RuntimeError(f"boom {SECRET}")
    except RuntimeError:
        import sys
        rec2 = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    flt.filter(rec2)
    assert rec2.exc_info is None and SECRET not in (rec2.exc_text or "")
    assert RedactingFilter([""]).filter(rec2), "an empty secret must not redact everything"


ESCAPABLE = 'p&ss<w0rd>"/+ é'   # every character some transport re-spells


def test_redaction_covers_the_escaped_spellings_of_the_password():
    """The crawler XML-escapes the password into the LoginRequest, so a hub
    or proxy that echoes the rejected body shows `p&amp;ss&lt;w0rd&gt;`;
    a URL carries it percent-encoded and a JSON payload backslash-escaped.
    Redacting only the raw form left each of those readable."""
    from json import dumps
    from urllib.parse import quote, quote_plus
    from xml.sax.saxutils import escape

    flt = RedactingFilter([ESCAPABLE])
    for spelled in (ESCAPABLE, escape(ESCAPABLE), escape(ESCAPABLE, {'"': "&quot;"}),
                    quote(ESCAPABLE, safe=""), quote_plus(ESCAPABLE), dumps(ESCAPABLE)[1:-1]):
        assert spelled, "a spelling must not be empty"
        redacted = flt.redact(f"body=<Password>{spelled}</Password>")
        assert spelled not in redacted and REDACTED in redacted, spelled
    # the ordinary text around it survives
    assert flt.redact("pc.example rejected the login") == "pc.example rejected the login"


def test_an_error_echoing_the_escaped_request_body_is_redacted(tmp_path, fake_crawler, caplog, monkeypatch):
    from xml.sax.saxutils import escape
    _, FakeCrawler = fake_crawler
    monkeypatch.setenv("INFA_PASSWORD", ESCAPABLE)
    FakeCrawler.connect_error = ConnectionError(
        f"WSH rejected body: <Password>{escape(ESCAPABLE)}</Password>")
    try:
        with caplog.at_level(logging.DEBUG):
            rc = main(["discover", "--host", "pc.example", "-o", str(tmp_path / "out")])
    finally:
        FakeCrawler.connect_error = None
    assert rc == 1
    assert ESCAPABLE not in caplog.text and escape(ESCAPABLE) not in caplog.text
    assert "<Password>***</Password>" in caplog.text


def test_connection_config_repr_hides_the_password():
    cfg = InfaConnectionConfig(host="h", username="u", password=SECRET, repository="r")
    assert SECRET not in repr(cfg)
    assert SECRET not in str(cfg)
    assert cfg.password == SECRET, "hidden from repr, still usable"


# ── no credential ingress through the host / URL ──────────────────────

@pytest.mark.parametrize("host", [f"admin:{SECRET}@pc.example", f"http://admin:{SECRET}@pc.example", "pc.example/wsh"])
def test_credentials_in_the_host_are_refused_and_not_echoed(host):
    with pytest.raises(ValueError) as exc:
        InfaConnectionConfig(host=host)
    assert SECRET not in str(exc.value)
    assert PASSWORD_ARGV_REFUSAL in str(exc.value)


@pytest.mark.parametrize("host", [
    f"pc.example?u=admin:{SECRET}",     # would become the URL's query string
    f"pc.example#admin:{SECRET}",       # ... or its fragment
    f"pc.example\\{SECRET}",
    f"pc.example {SECRET}",
])
def test_a_host_that_is_not_a_bare_name_is_refused(host):
    """`--host` is interpolated straight into `http://<host>:<port>/wsh/...`;
    a `?` or `#` smuggles the rest of the value into every request URL."""
    with pytest.raises(ValueError) as exc:
        InfaConnectionConfig(host=host)
    assert SECRET not in str(exc.value)


@pytest.mark.parametrize("host", ["pc.example", "10.0.0.7", "[::1]", "pc.example:7333", "pc-01.corp.example"])
def test_bare_hosts_and_addresses_are_accepted(host):
    assert InfaConnectionConfig(host=host).host == host


def test_credentials_in_an_explicit_wsh_url_are_refused():
    with pytest.raises(ValueError):
        InfaConnectionConfig(host="pc", wsh_url=f"https://admin:{SECRET}@pc:7343/wsh/services")
    InfaConnectionConfig(host="pc", wsh_url="https://pc:7343/wsh/services")   # fine


def test_cli_refuses_a_host_carrying_credentials(tmp_path, fake_crawler, caplog):
    with caplog.at_level(logging.DEBUG):
        rc = main(["discover", "--host", f"admin:{SECRET}@pc.example", "-o", str(tmp_path / "out")])
    assert rc == 1
    assert "cfg" not in fake_crawler[0]
    assert SECRET not in caplog.text


# ── the SOAP login request ────────────────────────────────────────────

def test_soap_login_escapes_every_field_and_does_not_log_the_session(monkeypatch, caplog):
    pw = 'p&ss<word>"'
    crawler = InformaticaCrawler(InfaConnectionConfig(
        host="h", username="u<1>", password=pw, domain="d&d", repository="r"))
    seen = {}

    def fake_call(service, operation, body_xml):
        seen["body"] = body_xml
        return "<Login><SessionId>sess-ABCDEFGHIJKLMNOPQRSTUVWXYZ</SessionId></Login>"

    monkeypatch.setattr(crawler, "_soap_raw_call", fake_call)
    with caplog.at_level(logging.DEBUG):
        crawler._connect_soap()
    body = seen["body"]
    assert "<Password>p&amp;ss&lt;word&gt;&quot;</Password>" in body or \
           "<Password>p&amp;ss&lt;word&gt;\"</Password>" in body
    assert "<word>" not in body and "d&d" not in body and "u<1>" not in body
    assert crawler._session_id == "sess-ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    assert "sess-ABCDEFGHIJKLMNOP" not in caplog.text, "a session id is a bearer credential"
    assert pw not in caplog.text
