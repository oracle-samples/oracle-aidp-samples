"""Credential ingress for the one command that talks to a live system.

``discover`` is the only infa2aidp command that authenticates to anything
(a PowerCenter repository, over SOAP or ``pmrep``). Its password used to be
an ordinary ``--password`` flag, which is the one place a secret must never
travel: argv is readable by every user on the host in ``ps``, it lands in
shell history, in CI transcripts, in terminal recordings and in the service
requests people paste a failing command into. The crawler was already
careful on its own side (``pmrep -X <env var>`` rather than ``-x <pwd>``,
TLS verification on by default), so the ingress is brought up to the same
posture here:

- ``--password`` is still declared, so an old command fails loudly with
  the remediation text instead of being parsed as something else, but it
  exits 2 before the value is ever stored anywhere.
- ``--password-file`` reads the secret from a file that only its owner can
  read. On POSIX a group- or world-readable file is refused outright; on
  Windows ``st_mode`` carries no such bits, so the check is skipped with a
  debug note rather than refusing every file.
- ``INFA_PASSWORD`` in the environment remains supported (it is what the
  skills recommend, and what ``.env`` loading provides).
- :class:`CredentialSafeParser` is the argparse class the CLI is built
  from: no option-prefix matching (so ``--password-fil <secret>`` is not
  quietly accepted as a file path) and the values after an unrecognised
  option are masked in the error message.

The second half of the module keeps the secret out of diagnostics once it
is in memory: :func:`install_redaction` puts a filter on every logging
handler that replaces the secret with ``***`` in anything logged, which
covers the crawler's own messages as well as the CLI's catch-all
``"<command> failed: <exc>"`` -- a library exception that happens to echo a
request body or connection string is redacted before it reaches a terminal
or a log file. The CLI never re-raises for the same reason: a traceback
printed by the interpreter goes through no logging filter. This lives
beside, not in, ``cli.py`` so the CLI stays the thin dispatcher the release
gate holds under 500 lines.
"""
from __future__ import annotations

import argparse
import codecs
import html
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote, quote_plus
from xml.sax.saxutils import escape as _xml_escape

logger = logging.getLogger(__name__)

PASSWORD_ARGV_REFUSAL = (
    "Do not pass passwords in argv. Use --password-file or INFA_PASSWORD."
)

#: What a redacted secret is replaced with in logs and messages.
REDACTED = "***"

#: Permission bits a secret file must NOT have on POSIX: anything that
#: lets the group or the world read, write or execute it.
_GROUP_OR_WORLD = 0o077

#: A password file is a line of text; anything beyond this is not one.
_MAX_SECRET_FILE_BYTES = 64 * 1024

#: Characters a bare hostname or address never contains. ``@`` and ``/``
#: carry credentials and paths; ``?`` and ``#`` would smuggle a query or
#: fragment into the URL the host is interpolated into; ``\`` and
#: whitespace are never part of a host.
_NOT_A_BARE_HOST = re.compile(r"[@/\\?#\s]")


class RejectPasswordArgv(argparse.Action):
    """``--password`` is declared only so that it can be refused.

    Declared with ``nargs="?"`` so both ``--password secret`` and a bare
    ``--password`` are consumed by this action rather than falling through
    as an unknown argument or a positional. The action never stores the
    value: it calls ``parser.error``, which prints the usage line plus the
    remediation text to stderr and exits 2, exactly like any other
    argument error -- and the value itself is not echoed.
    """

    def __init__(self, option_strings, dest, **kwargs):
        kwargs["nargs"] = "?"
        kwargs.setdefault("help", argparse.SUPPRESS)
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        parser.error(PASSWORD_ARGV_REFUSAL)


class CredentialSafeParser(argparse.ArgumentParser):
    """``argparse.ArgumentParser`` with two credential-hygiene changes.

    No prefix matching: with it, ``--password-fil <secret>`` resolves to
    ``--password-file`` and hands the secret to the file reader, whose
    "not found" error then names it. Only the exact option spellings are
    accepted -- and ``add_subparsers`` builds the sub-parsers from the
    parent's class, so the rule holds for every command.

    Masked echo: the token after a mistyped ``--pasword`` is the password,
    and argparse's ``unrecognized arguments: ...`` would print it. Every
    value following an unrecognised option is replaced with
    :data:`REDACTED` before the message is written.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)

    def error(self, message: str):
        prefix = "unrecognized arguments: "
        if message.startswith(prefix):
            masked = []
            for tok in message[len(prefix):].split():
                if not tok.startswith("-"):
                    tok = REDACTED
                elif "=" in tok:
                    tok = tok.split("=", 1)[0] + "=" + REDACTED
                masked.append(tok)
            message = prefix + " ".join(masked)
        super().error(message)


def _file_mode(path: Path) -> int:
    """Permission bits of *path*; split out so tests can pin the POSIX rule
    on a platform whose filesystem cannot express it."""
    return path.stat().st_mode & 0o777


def _decode_secret_file(data: bytes, path: Path) -> str:
    """The text of a password file as the usual editors write it.

    UTF-8 with or without a byte-order mark (Notepad's "UTF-8 with BOM",
    PowerShell 5.1's ``Out-File -Encoding utf8``) and UTF-16 with a BOM
    (what PowerShell 5.1's ``>`` redirection writes) are all accepted: a
    BOM left in the text would be passed to the repository as part of the
    password and the login would fail with no hint why. Anything else --
    including BOM-less UTF-16, which ``utf-8`` would silently decode into
    a password full of NULs -- is a ``ValueError`` naming the path, never
    the bytes.
    """
    if data.startswith(codecs.BOM_UTF8):
        encoding = "utf-8-sig"
    elif data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        encoding = "utf-16"
    else:
        encoding = "utf-8"
    try:
        if encoding == "utf-8" and b"\x00" in data:
            raise ValueError("NUL byte in a text file")
        return data.decode(encoding)
    except ValueError:   # UnicodeDecodeError is one
        raise ValueError(
            f"password file {path} is not UTF-8 text (save it as UTF-8, or UTF-16 with a BOM)"
        ) from None


def read_secret_file(path: Optional[str], *, enforce_mode: Optional[bool] = None) -> str:
    """Return the secret held in *path* (first line, stripped), or ``""``
    when no path was given.

    Refuses a file that is readable by anyone but its owner (``st_mode &
    0o077`` must be 0) -- a 0644 password file is the shell-history problem
    in a different place. *enforce_mode* defaults to "on POSIX only":
    Windows reports 0666 for every ordinary file regardless of its ACL, so
    the check would refuse every file there and is skipped with a debug
    note instead. The file may be UTF-8 (with or without a BOM) or UTF-16
    with a BOM -- see :func:`_decode_secret_file`. A missing, unreadable or
    non-text file raises ``ValueError`` naming the path, never the content.
    """
    if not path:
        return ""
    p = Path(path).expanduser()
    if enforce_mode is None:
        enforce_mode = os.name != "nt"
    try:
        if enforce_mode:
            mode = _file_mode(p)
            if mode & _GROUP_OR_WORLD:
                raise ValueError(
                    f"password file {p} is mode {mode:04o}; it must be readable "
                    f"by its owner only (chmod 600 {p})"
                )
        else:
            logger.debug("password file %s: permission check skipped on this platform", p)
        with open(p, "rb") as fh:
            data = fh.read(_MAX_SECRET_FILE_BYTES)
    except FileNotFoundError:
        raise ValueError(f"password file not found: {p}") from None
    except (IsADirectoryError, PermissionError) as exc:
        raise ValueError(f"password file {p} could not be read: {type(exc).__name__}") from None
    first_line = _decode_secret_file(data, p).split("\n", 1)[0]
    secret = first_line.strip()
    if not secret:
        raise ValueError(f"password file {p} is empty")
    return secret


def resolve_password(password_file: Optional[str], env_var: str = "INFA_PASSWORD") -> "tuple[str, str]":
    """The password for a live connection and where it came from.

    Returns ``(password, source)`` where *source* is ``"--password-file"``,
    the environment variable's name, or ``"none"`` -- the source is what
    verbose diagnostics may print; the password is not.
    """
    secret = read_secret_file(password_file)
    if secret:
        return secret, "--password-file"
    secret = os.environ.get(env_var, "")
    if secret:
        return secret, env_var
    return "", "none"


def reject_url_credentials(host: str, url: str = "") -> None:
    """Refuse a host or Web Services Hub URL that smuggles ``user:pass@``.

    The CLI has no URL flag, but ``--host`` is interpolated straight into
    ``http://<host>:<port>/wsh/services`` and a host of the form
    ``user:secret@pc.example`` would put the credential in every request
    URL, every requests exception message and every proxy log -- and so
    would ``pc.example?u=admin:secret`` or ``pc.example#...``, as a query
    or fragment of the same URL. Only a bare hostname or address (an
    optional ``:port`` included) is accepted. The message deliberately
    does not echo the offending value.
    """
    if host and _NOT_A_BARE_HOST.search(host):
        raise ValueError(
            "the PowerCenter host must be a bare hostname or address -- "
            "credentials in the host/URL are not accepted. " + PASSWORD_ARGV_REFUSAL
        )
    if url:
        netloc = url.split("://", 1)[-1].split("/", 1)[0]
        if "@" in netloc:
            raise ValueError(
                "credentials in the Web Services Hub URL are not accepted. "
                + PASSWORD_ARGV_REFUSAL
            )


def _secret_spellings(secret: str) -> "set[str]":
    """Every spelling of *secret* a request or its error might carry.

    The crawler XML-escapes the password into the SOAP LoginRequest, so a
    Web Services Hub or proxy that echoes the rejected body shows
    ``p&amp;ss&lt;w0rd&gt;``, not ``p&ss<w0rd>``; a URL carries it
    percent-encoded, a JSON payload backslash-escaped. Redacting only the
    raw form would leave each of those readable.
    """
    spellings = {
        secret,
        _xml_escape(secret),
        _xml_escape(secret, {'"': "&quot;", "'": "&apos;"}),
        html.escape(secret),              # &#x27; for the apostrophe
        quote(secret, safe=""),
        quote_plus(secret),
        json.dumps(secret)[1:-1],
    }
    return {s for s in spellings if s}


class RedactingFilter(logging.Filter):
    """Replace every occurrence of the configured secrets in a log record
    with :data:`REDACTED` -- in the message, its arguments and any attached
    exception text -- before a handler formats it. Each secret is redacted
    in every spelling :func:`_secret_spellings` lists."""

    def __init__(self, secrets=()) -> None:
        super().__init__()
        self._secrets: list = []
        self.add(*secrets)

    def add(self, *secrets: str) -> None:
        """Redact *secrets* too, from now on."""
        spellings = set(self._secrets)
        spellings.update(v for s in secrets if s for v in _secret_spellings(s))
        # Longest first, so a secret that contains another is redacted whole.
        self._secrets = sorted(spellings, key=len, reverse=True)

    def redact(self, text: str) -> str:
        for s in self._secrets:
            text = text.replace(s, REDACTED)
        return text

    def _scrub(self, value):
        if isinstance(value, str):
            return self.redact(value)
        if isinstance(value, BaseException):
            return self.redact(str(value))
        if isinstance(value, dict):
            return {k: self._scrub(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return type(value)(self._scrub(v) for v in value)
        if isinstance(value, (int, float, bool, type(None))):
            return value
        # Anything else (a config object, a requests.Response, ...) is
        # formatted through str() by %s anyway; only rewrite it when its
        # text actually carries a secret, so numeric formats stay numeric.
        text = str(value)
        return self.redact(text) if text != self.redact(text) else value

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._secrets:
            return True
        record.msg = self._scrub(record.msg)
        if isinstance(record.args, dict):
            record.args = {k: self._scrub(v) for k, v in record.args.items()}
        elif isinstance(record.args, tuple):
            record.args = tuple(self._scrub(a) for a in record.args)
        if record.exc_info and record.exc_info[1] is not None:
            # The traceback text is rendered from exc_info by the formatter;
            # render it here instead so it can be redacted below.
            import traceback
            record.exc_text = "".join(traceback.format_exception(*record.exc_info))
            record.exc_info = None
        if record.exc_text:
            # Also covers a traceback another handler or filter rendered
            # first -- the formatter caches it on the record, so an
            # unredacted rendering would otherwise be reused as-is.
            record.exc_text = self.redact(record.exc_text)
        return True


#: The one filter :func:`install_redaction` maintains per process. Secrets
#: accumulate in it: installing twice must not stack two filters on a
#: handler, since the first would render the traceback with its own
#: secrets and the second would find nothing left to render.
_REDACTOR = RedactingFilter()


def install_redaction(*secrets: str) -> RedactingFilter:
    """Redact *secrets* from every handler on the root logger
    (``logging.basicConfig`` installs exactly one) from now on, and return
    the filter so callers can redact free text with the same rules."""
    _REDACTOR.add(*secrets)
    for handler in logging.getLogger().handlers:
        if _REDACTOR not in handler.filters:
            handler.addFilter(_REDACTOR)
    return _REDACTOR


def prepare_live_credentials(host: str, password_file: Optional[str],
                             env_var: str = "INFA_PASSWORD") -> "tuple[str, str]":
    """Everything the CLI needs before it opens a live connection, in order:
    refuse credentials smuggled into the host, resolve the password from
    ``--password-file`` then *env_var*, and redact it from all logging for
    the rest of the process. Returns ``(password, source)``."""
    reject_url_credentials(host)
    password, source = resolve_password(password_file, env_var)
    install_redaction(password)
    return password, source
