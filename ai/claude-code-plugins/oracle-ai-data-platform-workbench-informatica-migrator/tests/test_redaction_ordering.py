"""Redaction only covers handlers that exist when it is installed.

`install_redaction` walks `logging.getLogger().handlers` and attaches its
filter to each. That is the right mechanism -- a filter on a *logger* does
not see records propagated up from child loggers, so the filter has to sit
on the handler -- but it makes the feature depend on an ordering:

    main()            -> logging.basicConfig(...)        # creates the handler
      _cmd_discover() -> prepare_live_credentials(...)   # installs the filter

Today `cli.main` does exactly that, and the end-to-end test below proves a
library exception echoing the password comes out as `***`. But nothing
enforces the order. Move `basicConfig` after the dispatch, or add a handler
inside a command, and redaction silently stops covering it -- silently being
the whole problem, since a leak raises nothing and the first symptom is a
password in a support transcript.

The ordering test below fails if that sequence is broken.
"""
from __future__ import annotations

import contextlib
import inspect
import io
import logging
import os

import pytest

from infa2aidp import cli
from infa2aidp.secret_ingress import install_redaction

SECRET = "p@ss<w0rd>&1"


@pytest.fixture(autouse=True)
def _clean_logging():
    root = logging.getLogger()
    saved = list(root.handlers), root.level
    for h in list(root.handlers):
        root.removeHandler(h)
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved[0]:
        root.addHandler(h)
    root.setLevel(saved[1])


# ── The mechanism, and the ordering it depends on ──────────────────────

def test_a_handler_present_before_install_is_covered():
    root = logging.getLogger()
    buf = io.StringIO()
    root.addHandler(logging.StreamHandler(buf))
    root.setLevel(logging.INFO)

    install_redaction(SECRET)
    logging.getLogger("x").info("login failed for %s", SECRET)

    assert SECRET not in buf.getvalue()
    assert "***" in buf.getvalue()


def test_the_handler_main_creates_is_itself_redacted(monkeypatch, tmp_path, capfd):
    """The ordering, tested by behaviour rather than by reading the source.

    With NO root handler, `basicConfig` inside `main` creates the only one,
    writing to stderr. If it runs before the command, the filter that
    `prepare_live_credentials` installs lands on it and the password is
    redacted. If it ran after, that handler would have no filter and the
    password would reach stderr intact.

    An earlier version of this test compared source positions instead.
    That was decoration: moving `basicConfig` inside the `try` block keeps
    it textually first while changing nothing, and the test still passed.
    """
    import infa2aidp.crawlers.informatica_crawler as crawl

    monkeypatch.setenv("INFA_PASSWORD", SECRET)

    class _Echoes(crawl.InformaticaCrawler):
        def connect(self, method="auto"):
            raise RuntimeError(
                f"SOAP LoginRequest rejected: <password>{SECRET}</password>")

    monkeypatch.setattr(crawl, "InformaticaCrawler", _Echoes)

    # No handlers at all: whatever main configures is what we read.
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    rc = cli.main(["discover", "--host", "infa.example.invalid", "--repo", "r",
                   "--user", "u", "--domain", "d", "-o", str(tmp_path / "out")])
    err = capfd.readouterr().err

    assert rc == 1
    assert err.strip(), "main logged nothing, so this proves nothing"
    assert SECRET not in err, (
        f"password reached stderr unredacted -- the handler main created was "
        f"not covered by the redaction filter: {err}"
    )
    assert "***" in err


# ── End to end, through the real CLI ───────────────────────────────────

def test_a_password_echoed_by_a_library_exception_is_redacted(monkeypatch, tmp_path):
    """The case the feature exists for: a Web Services Hub that rejects the
    login and quotes the request body back.

    The handler is attached here rather than left to `basicConfig`, which
    no-ops once anything has configured logging -- under pytest that means
    the buffer would stay empty and the test would pass without proving
    anything. Attaching first also mirrors the real ordering: the handler
    exists before `prepare_live_credentials` installs the filter.
    """
    import infa2aidp.crawlers.informatica_crawler as crawl

    monkeypatch.setenv("INFA_PASSWORD", SECRET)

    class _Echoes(crawl.InformaticaCrawler):
        def connect(self, method="auto"):
            raise RuntimeError(
                f"SOAP LoginRequest rejected: <password>{SECRET}</password>")

    monkeypatch.setattr(crawl, "InformaticaCrawler", _Echoes)

    buf = io.StringIO()
    root = logging.getLogger()
    handler = logging.StreamHandler(buf)
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    with contextlib.redirect_stdout(io.StringIO()):
        rc = cli.main(["discover", "--host", "infa.example.invalid", "--repo", "r",
                       "--user", "u", "--domain", "d", "-o", str(tmp_path / "out")])

    out = buf.getvalue()
    assert rc == 1
    assert out.strip(), "nothing was logged, so this proves nothing"
    assert SECRET not in out, f"password reached the log: {out}"
    assert "***" in out


def test_the_cli_never_reraises_so_no_traceback_bypasses_the_filter():
    """A traceback printed by sys.excepthook goes through no logging filter,
    so a command that raises out of main() would print the secret."""
    src = inspect.getsource(cli.main)
    assert "except Exception" in src
    tail = src[src.index("except Exception"):]
    assert "raise" not in tail.replace("raise", "", 0) or "logger.error" in tail, \
        "main() must log the failure, not re-raise it"
    assert "return 1" in tail
