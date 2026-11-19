"""SEC-NEW-DATABRICKS-03: the Databricks PAT never travels on argv.

``extract_catalog_databricks.py`` used to accept ``--token <value>``. Now:

- ``--token`` (with a value, ``--token=value`` or bare) exits 2 with the
  remediation text and never stores or echoes the value;
- ``--token-file`` reads an owner-only file (mode & 0o077 refused on POSIX,
  check skipped on Windows); ``DATABRICKS_TOKEN`` keeps working and the file
  wins when both are present;
- output names the token's *source*, never the token, and error text that
  happens to carry the token is redacted before it is printed or stored.
"""
import json
import os
import sys

import pytest

pytest.importorskip("requests")

SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

import extract_catalog_databricks as ex  # noqa: E402

TOKEN = "dapiPLANTEDTOKENVALUE0123456789abcdefPLANTED"
HOST = "https://ws.example.cloud.databricks.com"
REFUSAL = "Do not pass tokens in argv. Use DATABRICKS_TOKEN or --token-file."


def _pack(host):
    return {"format_version": 1, "source_workspace": host, "catalogs": [], "schemas": [],
            "tables": [], "volumes": [], "errors": [],
            "stats": {"catalogs": 0, "schemas": 0, "tables_listed": 0,
                      "tables_detailed": 0, "tables_failed": 0, "volumes": 0}}


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """No developer token leaks in, no network goes out, redaction state is fresh."""
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", HOST)
    monkeypatch.setattr(ex.requests, "get",
                        lambda *a, **k: pytest.fail("network call attempted"))
    if hasattr(ex, "_REDACT_SECRETS"):
        ex._REDACT_SECRETS.clear()


@pytest.fixture
def fake_extract(monkeypatch):
    """Replace the REST walk with a recorder of the token it was handed."""
    seen = {}

    def _extract(host, token, *args, **kwargs):
        seen["host"], seen["token"] = host, token
        return _pack(host)

    monkeypatch.setattr(ex, "extract", _extract)
    return seen


def _run(argv, monkeypatch, capsys, tmp_path):
    """Run main() with *argv*; return (exit_code, combined stdout+stderr)."""
    out_path = tmp_path / "pack.json"
    monkeypatch.setattr(sys, "argv", ["extract_catalog_databricks.py", "--out", str(out_path)] + argv)
    code, message = 0, ""
    try:
        ex.main()
    except SystemExit as exc:  # argparse exits 2; sys.exit(str) exits 1
        if isinstance(exc.code, int):
            code = exc.code
        else:  # the interpreter would print this string to stderr
            code, message = 1, str(exc.code) + "\n"
    captured = capsys.readouterr()
    return code, captured.out + captured.err + message


def _token_file(tmp_path, content=TOKEN + "\n"):
    p = tmp_path / "token"
    p.write_text(content, encoding="utf-8")
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass
    return p


# ── --token is refused ─────────────────────────────────────────────────
@pytest.mark.parametrize("argv", [
    ["--token", TOKEN],
    ["--token=" + TOKEN],
    ["--token"],
    ["--token", TOKEN, "--catalogs", "main"],
    # mistyped forms: argparse's own "ambiguous option: --tok=<value> could
    # match ..." / "unrecognized arguments: <value>" diagnostics would echo
    # the token into stderr and the CI log
    ["--tok=" + TOKEN],
    ["--toke", TOKEN],
    ["-t", TOKEN],
    [TOKEN],
    ["--token-fil=" + TOKEN],
])
def test_token_on_argv_is_refused_with_exit_2(argv, monkeypatch, capsys, tmp_path, fake_extract):
    code, text = _run(argv, monkeypatch, capsys, tmp_path)
    assert code == 2
    assert REFUSAL in text
    assert TOKEN not in text
    assert "token" not in fake_extract, "the token was handed to extract()"


def test_parser_error_text_is_scrubbed_of_argv_values(monkeypatch, capsys):
    ap = ex.ArgvSafeParser(prog="x")
    ap.add_argument("--out")
    monkeypatch.setattr(sys, "argv", ["x", "--out", "pack.json", "--bogus=" + TOKEN, TOKEN])
    with pytest.raises(SystemExit) as exc:
        ap.parse_args()
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert TOKEN not in err
    assert "2 unrecognized arguments (not shown)" in err and REFUSAL in err
    # defence in depth: whatever argparse puts in a message, the argv values come out
    assert ap.redact_argv(f"ambiguous option: --bogus={TOKEN} could match --out") == \
        f"ambiguous option: {ex.REDACTED} could match --out"
    assert ap.redact_argv("argument --out: expected one argument") == \
        "argument --out: expected one argument"


def test_help_hides_token_and_offers_token_file(monkeypatch, capsys, tmp_path):
    code, text = _run(["--help"], monkeypatch, capsys, tmp_path)
    assert code == 0
    assert "--token-file" in text
    assert "--token TOKEN" not in text and "--token [" not in text


# ── accepted ingress paths ─────────────────────────────────────────────
def test_token_file_is_read_and_only_its_source_is_reported(monkeypatch, capsys, tmp_path, fake_extract):
    p = _token_file(tmp_path)
    code, text = _run(["--token-file", str(p)], monkeypatch, capsys, tmp_path)
    assert code == 0
    assert fake_extract["token"] == TOKEN
    assert "token source: --token-file" in text
    assert TOKEN not in text
    assert json.loads((tmp_path / "pack.json").read_text(encoding="utf-8"))["source_workspace"] == HOST


def test_env_token_is_still_supported(monkeypatch, capsys, tmp_path, fake_extract):
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    code, text = _run([], monkeypatch, capsys, tmp_path)
    assert code == 0
    assert fake_extract["token"] == TOKEN
    assert "token source: DATABRICKS_TOKEN" in text
    assert TOKEN not in text


def test_token_file_wins_over_env(monkeypatch, capsys, tmp_path, fake_extract):
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-env-token-should-lose")
    p = _token_file(tmp_path)
    code, _ = _run(["--token-file", str(p)], monkeypatch, capsys, tmp_path)
    assert code == 0
    assert fake_extract["token"] == TOKEN


def test_missing_token_fails_with_remediation(monkeypatch, capsys, tmp_path, fake_extract):
    code, text = _run([], monkeypatch, capsys, tmp_path)
    assert code != 0
    assert REFUSAL in text
    assert "token" not in fake_extract


# ── token file hygiene ─────────────────────────────────────────────────
def test_group_or_world_readable_token_file_is_refused_on_posix(monkeypatch, tmp_path):
    p = _token_file(tmp_path)
    monkeypatch.setattr(ex, "_file_mode", lambda path: 0o644)
    with pytest.raises(ValueError) as exc:
        ex.read_token_file(str(p), enforce_mode=True)
    assert "chmod 600" in str(exc.value)
    assert TOKEN not in str(exc.value)
    monkeypatch.setattr(ex, "_file_mode", lambda path: 0o600)
    assert ex.read_token_file(str(p), enforce_mode=True) == TOKEN


def test_mode_check_follows_platform_default(monkeypatch, tmp_path, capsys):
    p = _token_file(tmp_path)
    monkeypatch.setattr(ex, "_file_mode", lambda path: 0o644)
    monkeypatch.setattr(os, "name", "nt")
    assert ex.read_token_file(str(p)) == TOKEN  # skipped on Windows
    assert "permission check skipped" in capsys.readouterr().err
    monkeypatch.setattr(os, "name", "posix")
    with pytest.raises(ValueError):
        ex.read_token_file(str(p))


def test_missing_or_empty_token_file_names_the_path_not_content(tmp_path):
    with pytest.raises(ValueError, match="not found"):
        ex.read_token_file(str(tmp_path / "absent"), enforce_mode=False)
    empty = tmp_path / "empty"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        ex.read_token_file(str(empty), enforce_mode=False)
    assert ex.read_token_file(None) == ""


def test_token_file_takes_first_line_only(tmp_path):
    p = _token_file(tmp_path, TOKEN + "\nsecond line ignored\n")
    assert ex.read_token_file(str(p), enforce_mode=False) == TOKEN


# ── nothing echoes the token ───────────────────────────────────────────
def test_host_with_embedded_credentials_is_refused(monkeypatch, capsys, tmp_path, fake_extract):
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    code, text = _run(["--host", f"https://user:{TOKEN}@ws.example.com"], monkeypatch, capsys, tmp_path)
    assert code != 0
    assert REFUSAL in text
    assert TOKEN not in text
    assert "token" not in fake_extract


def test_library_error_is_redacted_before_it_is_printed(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)

    def _boom(*a, **k):
        raise RuntimeError(f"401 Client Error for Authorization: Bearer {TOKEN}")

    monkeypatch.setattr(ex, "extract", _boom)
    code, text = _run([], monkeypatch, capsys, tmp_path)
    assert code != 0
    assert "RuntimeError" in text and "401" in text
    assert TOKEN not in text
    assert ex.REDACTED in text


def test_pack_errors_are_redacted(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    token, source = ex.resolve_token(None)
    assert (token, source) == (TOKEN, "DATABRICKS_TOKEN")
    ex._REDACT_SECRETS.append(token)
    assert ex._redact(f"GET failed with Bearer {TOKEN}") == f"GET failed with Bearer {ex.REDACTED}"
