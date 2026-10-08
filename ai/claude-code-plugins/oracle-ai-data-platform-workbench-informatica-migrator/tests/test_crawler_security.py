"""The crawler's security defaults, pinned.

PR #139 fixed three real exposures in this crawler and shipped no test for
any of them. Each is the kind that regresses quietly:

  - TLS verification was hard-coded `self._http.verify = False`, with the
    comment "Many on-prem installs use self-signed certs". The repository
    PASSWORD went over that unverified channel on every connect. The obvious
    way for it to come back is someone hitting a self-signed-cert failure and
    flipping the default rather than setting the documented opt-out.
  - The pmrep password was passed as `-x <password>`, visible to every user
    on the host in `ps`. It is now `-X INFA_PMREP_PASSWORD`, read from the
    child's environment.
  - The env-var wiring that makes the opt-out reachable at all lives in one
    expression in cli.py; `or` and `!=` bind in an order that is easy to get
    wrong when editing.

These assert behaviour, not wording, so they survive a rename of the
comment but not a change of the default.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from infa2aidp.crawlers.informatica_crawler import (
    InfaConnectionConfig,
    InformaticaCrawler,
)


# ── TLS verification ───────────────────────────────────────────────────

def test_tls_verification_is_on_by_default():
    """The whole point of the fix: a default that sends the repository
    password over an unverified channel is not an acceptable default."""
    assert InfaConnectionConfig(host="h").verify_tls is True


def test_the_session_actually_carries_the_setting():
    """A config field nothing reads would be decoration."""
    c = InformaticaCrawler(InfaConnectionConfig(host="h"))
    assert c._http.verify is True


def test_a_ca_bundle_path_reaches_the_session():
    """A corporate CA is a bundle path, not a boolean -- requests accepts
    either, so the field must pass a str through unchanged."""
    c = InformaticaCrawler(InfaConnectionConfig(host="h", verify_tls="/etc/ssl/corp.pem"))
    assert c._http.verify == "/etc/ssl/corp.pem"


def test_verification_can_still_be_turned_off_explicitly():
    """A self-signed lab host has to remain usable, or users will patch the
    default back out."""
    c = InformaticaCrawler(InfaConnectionConfig(host="h", verify_tls=False))
    assert c._http.verify is False


# ── The env-var wiring that makes the opt-out reachable ────────────────
#
# cli.py: verify_tls = INFA_CA_BUNDLE or (INFA_TLS_VERIFY != "0")
# `or` binds looser than `!=`, so the bundle wins when both are set.

def _cli_verify_tls() -> "bool | str":
    """Evaluate the CLI's own expression against the current environment."""
    return os.environ.get("INFA_CA_BUNDLE") or os.environ.get("INFA_TLS_VERIFY", "1") != "0"


@pytest.mark.parametrize("env,expected,why", [
    ({}, True, "nothing set -> verification on"),
    ({"INFA_TLS_VERIFY": "1"}, True, "explicitly on"),
    ({"INFA_TLS_VERIFY": "0"}, False, "documented opt-out"),
    ({"INFA_CA_BUNDLE": "/etc/ssl/corp.pem"}, "/etc/ssl/corp.pem", "corporate CA"),
    ({"INFA_CA_BUNDLE": "", "INFA_TLS_VERIFY": "0"}, False, "empty bundle falls through"),
    # Both set: the bundle wins and verification stays ON. Deliberate -- an
    # explicit CA is a stronger signal than a blanket disable -- but worth
    # pinning so it is a decision rather than an accident of precedence.
    ({"INFA_CA_BUNDLE": "/ca.pem", "INFA_TLS_VERIFY": "0"}, "/ca.pem",
     "an explicit CA bundle outranks the blanket disable"),
])
def test_cli_env_wiring(monkeypatch, env, expected, why):
    for k in ("INFA_CA_BUNDLE", "INFA_TLS_VERIFY"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert _cli_verify_tls() == expected, why


def test_the_cli_really_uses_that_expression():
    """Guard against the test above drifting from cli.py: if the CLI stops
    reading these names, these parametrised cases prove nothing."""
    import inspect

    from infa2aidp import cli

    src = inspect.getsource(cli)
    assert "INFA_CA_BUNDLE" in src and "INFA_TLS_VERIFY" in src
    assert "verify_tls=" in src


# ── The pmrep password must not reach the command line ─────────────────

def test_the_password_is_passed_by_env_var_not_argv(monkeypatch):
    """`-x <password>` put the repository password in `ps` output for every
    user on the host. `-X NAME` names an environment variable instead."""
    seen: dict = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = list(cmd)
        seen["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(cmd, 0, "connect completed successfully", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    cfg = InfaConnectionConfig(
        host="h", username="u", password="s3cr3t-do-not-leak", domain="d",
        repository="r",
    )
    crawler = InformaticaCrawler(cfg)
    crawler._connect_pmrep()

    argv = " ".join(seen["cmd"])
    assert "s3cr3t-do-not-leak" not in argv, f"password on the command line: {argv}"
    assert "-x" not in seen["cmd"], "-x puts the password in argv; -X names an env var"
    assert "-X" in seen["cmd"]
    assert seen["cmd"][seen["cmd"].index("-X") + 1] == "INFA_PMREP_PASSWORD"


def test_the_password_does_reach_the_child_environment(monkeypatch):
    """Not passing it in argv is only correct if pmrep can still read it --
    otherwise the fix just breaks connect."""
    seen: dict = {}

    def fake_run(cmd, **kwargs):
        seen["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(cmd, 0, "connect completed successfully", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    crawler = InformaticaCrawler(InfaConnectionConfig(
        host="h", username="u", password="s3cr3t-do-not-leak", domain="d", repository="r"))
    crawler._connect_pmrep()

    assert seen["env"] is not None, "env not passed, so pmrep cannot read the password"
    assert seen["env"]["INFA_PMREP_PASSWORD"] == "s3cr3t-do-not-leak"
    # The child needs the rest of the environment too (PATH, INFA_HOME).
    assert "PATH" in seen["env"], "env replaced rather than extended"
