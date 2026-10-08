"""Snowflake connection and auth. The only module here that opens a socket.

Secrets are read from FILES, never taken as inline arguments, so they cannot end
up in shell history, process listings, or an artifact. Nothing in this module
logs or returns a credential.

Auth modes:
  keypair          -- key_path (+ key_passphrase). Preferred; also what AIDP's
                      native Snowflake connector uses, so it is not throwaway setup.
  pat              -- pat_path. Scoped, expiring, revocable.
  password         -- password_path.
  externalbrowser  -- SSO. Needs a SAML IdP on the account; a plain Snowflake
                      account returns 390190.
"""
from __future__ import annotations

import pathlib
from typing import Any, Callable

from .dialect import lexer

__all__ = ["AuthError", "SourceWriteRefused", "READ_ONLY_VERBS",
           "drop_secondary_roles",
           "build_connect_kwargs", "load_private_key_der", "connect",
           "make_run_sql"]

# The ONLY statements this plugin may send to Snowflake. Default deny: an
# unrecognised verb is refused rather than assumed safe. WITH is on the list
# for the CTE-SELECT and only for it: assert_read_only looks past the CTE
# list, because `WITH x AS (...) INSERT ...` leads with WITH too.
#
# This is enforced at the transport, not by convention: any statement not led
# by one of these verbs (or a CTE ending in SELECT) is refused, whatever the
# credential allows. It is a verb gate -- a SELECT can still call a function
# with side effects -- so the read-only role remains what prevents writes.
READ_ONLY_VERBS = ("SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "EXPLAIN")

_MODES = {"keypair", "pat", "password", "externalbrowser"}


class AuthError(RuntimeError):
    """Auth arguments are missing, contradictory, or unreadable."""


class SourceWriteRefused(PermissionError):
    """A statement that is not a read was aimed at Snowflake. Refused."""


def assert_read_only(sql: str) -> None:
    """Refuse anything that is not a read. Raises SourceWriteRefused.

    Statement boundaries and the leading keyword come from the scanner in
    `dialect.lexer`, not from a regex, because a regex cannot tell code from
    the inside of a string literal. Three consequences:

      * a `;` inside a literal does not fabricate a second statement, so
        `select 'a;drop table t'` is correctly one read and is allowed
      * a comment marker inside a literal cannot hide a real separator
      * a statement whose first content is a literal has no verb at all and is
        refused rather than having a word read out of the literal

    A leading WITH is a read only when the statement after the CTE list is a
    SELECT: `WITH x AS (...) INSERT ...` is refused, naming INSERT, and so is
    a CTE whose body the walker cannot identify (a parenthesised body).

    Fails closed: SQL the scanner cannot make sense of is refused.
    """
    try:
        statements = lexer.split_statements(sql or "")
    except lexer.UnterminatedLiteral as exc:
        raise SourceWriteRefused(
            f"statement could not be scanned ({exc}); refused. This plugin "
            f"only sends Snowflake statements it can positively identify as "
            f"reads.") from exc

    if not statements:
        raise SourceWriteRefused(
            f"empty statement refused; this plugin is read-only against "
            f"Snowflake (allowed: {', '.join(READ_ONLY_VERBS)})")

    for part in statements:
        # `->>` (Snowflake's flow operator) chains a second statement into
        # the same request, so a write could ride behind an accepted read.
        if lexer.find_code(r"->>", part):
            raise SourceWriteRefused(
                "the ->> flow operator chains statements; refused. This "
                "plugin only sends single reads.")
        verb = lexer.leading_verb(part)
        if verb is None:
            raise SourceWriteRefused(
                f"statement has no leading SQL keyword; refused. This plugin "
                f"only sends statements it can positively identify as reads. "
                f"Allowed: {', '.join(READ_ONLY_VERBS)}.")
        if verb not in READ_ONLY_VERBS:
            raise SourceWriteRefused(
                f"{verb}: not a recognised read verb. This plugin is strictly "
                f"read-only against Snowflake and never writes to or drops "
                f"from the source, regardless of what the credential permits. "
                f"Allowed: {', '.join(READ_ONLY_VERBS)}.")
        if verb == "WITH":
            body = lexer.cte_body_verb(part)
            if body != "SELECT":
                raise SourceWriteRefused(
                    f"WITH ... {body or '<no keyword>'}: a common table "
                    f"expression is only a read when the statement after the "
                    f"CTE list is a SELECT; refused. This plugin is strictly "
                    f"read-only against Snowflake and never writes to or drops "
                    f"from the source, regardless of what the credential "
                    f"permits. Allowed: {', '.join(READ_ONLY_VERBS)}.")


def _read_secret_file(path: str, label: str) -> str:
    p = pathlib.Path(path).expanduser()
    try:
        return p.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise AuthError(f"{label} not readable at {path}: {exc.strerror}") from exc


def load_private_key_der(path: str, passphrase: str | None = None) -> bytes:
    from cryptography.hazmat.primitives import serialization
    p = pathlib.Path(path).expanduser()
    try:
        raw = p.read_bytes()
    except OSError as exc:
        raise AuthError(f"private key not readable at {path}: {exc.strerror}") from exc
    key = serialization.load_pem_private_key(
        raw, password=passphrase.encode() if passphrase else None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())


def build_connect_kwargs(auth: str, *, account: str, user: str | None = None,
                         role: str | None = None, warehouse: str | None = None,
                         database: str | None = None, key_path: str | None = None,
                         key_passphrase: str | None = None,
                         pat_path: str | None = None,
                         password_path: str | None = None,
                         host: str | None = None) -> dict[str, Any]:
    if auth not in _MODES:
        raise AuthError(f"unknown auth mode {auth!r}; expected one of {sorted(_MODES)}")
    if not account:
        raise AuthError("account is required")
    if not user and auth != "externalbrowser":
        raise AuthError(f"user is required for auth mode {auth!r}")

    kw: dict[str, Any] = {"account": account, "client_session_keep_alive": False}
    # An explicit host (locator form, PrivateLink) is what the EXTERNAL
    # catalog registers; without it the driver derives
    # <account>.snowflakecomputing.com, which is right only for an
    # org-account identifier. Passed through so preflight tests the same
    # endpoint AIDP will use.
    if host and host.strip():
        kw["host"] = host.strip()
    if user:
        kw["user"] = user
    for value, key in ((role, "role"), (warehouse, "warehouse"), (database, "database")):
        if value:
            kw[key] = value

    if auth == "externalbrowser":
        kw["authenticator"] = "externalbrowser"
    elif auth == "keypair":
        if not key_path:
            raise AuthError("keypair auth requires key_path")
        kw["private_key"] = load_private_key_der(key_path, key_passphrase)
    elif auth == "pat":
        if not pat_path:
            raise AuthError("pat auth requires pat_path")
        kw["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
        # The connector builds its PAT authenticator from `token`. A PAT
        # handed over as `password` goes to the wire as TOKEN: null.
        kw["token"] = _read_secret_file(pat_path, "PAT file")
    elif auth == "password":
        if not password_path:
            raise AuthError("password auth requires password_path")
        kw["password"] = _read_secret_file(password_path, "password file")
    return kw


# A driver error code -> the config field that is actually wrong. The first
# thing anyone gets wrong is the account identifier, and the driver's own
# message for that is a 404 on a URL, which reads like a tool failure.
_CONNECT_HINTS = {
    "250001": "the account/host could not be reached at all",
    "251001": "`account` must be the account identifier, not the URL "
              "(e.g. ORG-ACCOUNT, without https:// or "
              ".snowflakecomputing.com)",
    "290404": "the account/host could not be reached at all",
    "390100": "the user or the password/key was rejected",
    "390190": "this account has no SAML IdP, so `auth: externalbrowser` "
              "cannot work here",
    "390201": "the role or warehouse named does not exist, or the user has "
              "no grant on it",
    "002003": "the database, schema or warehouse named does not exist for "
              "this role",
}

# Driver errnos that mean "could not reach it this time", by the driver's
# own names (snowflake.connector.errorcode): ER_CONNECTION_TIMEOUT 251011,
# ER_RETRYABLE_CODE 251012, ER_FAILED_TO_REQUEST 250003. Auth, config and
# object errors are absent on purpose -- repeating them only delays the
# message. That excludes 251001 (ER_NO_ACCOUNT_NAME: an invalid account
# identifier, raised locally before any socket), 253003 (a stage upload)
# and 290400 (HTTP 400), which all used to be here. Literals, not imports:
# this module is imported where the driver is not installed.
_NETWORK_ERRNOS = {"250003", "251011", "251012"}

_CONNECT_ADVICE = (
    "Check `account`/`host`, `user`, `role`, `warehouse` and `database` in the "
    "migration config, then re-run `preflight --test-source`. Do not paste the "
    "credential here — fix it in the file.")


def connect(**kwargs):
    """Open the source connection, or fail with something a human can act on.

    The driver reports a mistyped account as `404 Not Found: post
    <account>.snowflakecomputing.com/session/v1/login-request` and lets the
    traceback escape. That is the single most common first-run mistake, and a
    traceback sends people looking for a bug in this tool instead of at the
    one field they need to fix -- so every driver-level failure is re-raised
    as an AuthError naming the likely field.

    The secret is never in the message: only the error code, the driver's own
    text (which carries the host, not the credential) and what to check.
    """
    import snowflake.connector
    from snowflake.connector.errors import Error as SnowflakeError
    from retry import retry_call

    def network_blip(exc: BaseException) -> bool:
        # Only a failure to REACH Snowflake is repeated. A rejected user,
        # password, role or warehouse is permanent and fails on the first try.
        code = str(getattr(exc, "errno", "") or "")
        # A TLS failure the driver itself marks as unable to succeed on a
        # retry carries 250003 too (a certificate, a hostname, a protocol
        # floor); it is not a blip.
        tls = getattr(snowflake.connector.errors, "NonRetryableTlsError",
                      None)
        if tls is not None and isinstance(exc, tls):
            return False
        return code in _NETWORK_ERRNOS

    try:
        return retry_call(lambda: snowflake.connector.connect(**kwargs),
                          label="snowflake connect", retryable=network_blip)
    except SnowflakeError as exc:
        code = str(getattr(exc, "errno", "") or "")
        hint = _CONNECT_HINTS.get(code)
        account = kwargs.get("account") or "<unset>"
        head = (f"could not connect to Snowflake account {account}"
                + (f": {hint}" if hint else ""))
        raise AuthError(
            f"{head}.\n  driver said ({code or 'no code'}): {exc}\n  "
            f"{_CONNECT_ADVICE}") from exc


def drop_secondary_roles(conn) -> None:
    """Scope the session to its primary role alone.

    With secondary roles active -- the default on many accounts -- every role
    granted to the user is in effect, so a read attributed to a restricted
    role can have been served by ACCOUNTADMIN. A migration rehearsal meant to
    prove a least-privilege role is sufficient has to turn them off, or it
    proves nothing. Live-verified 2026-09-23: the same session read a
    database it had no grant on until this ran.

    A session setting, not a write, so it bypasses the read-only verb gate
    deliberately: `make_run_sql` never sends it.
    """
    with conn.cursor() as cur:
        cur.execute("use secondary roles none")


def make_run_sql(conn) -> Callable[..., list[dict]]:
    """Return the injected-I/O callable every extract module consumes.

    Signature: run_sql(sql, params=None) -> list[dict]

    Every statement passes assert_read_only first. A write never reaches
    Snowflake, whatever the credential allows.
    """
    def run_sql(sql: str, params: dict | None = None) -> list[dict]:
        assert_read_only(sql)
        cur = conn.cursor()
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    return run_sql
