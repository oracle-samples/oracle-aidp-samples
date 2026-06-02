"""URL / JDBC URL redaction for logs and persisted result messages.

Resolved Slack/webhook URLs typically embed a bearer-grade secret
in the path or query (Slack incoming webhooks are exactly this
shape). Resolved JDBC URLs may carry user / password / token
inline (`jdbc:...//user:pwd@host`). Logging or persisting the
full URL turns those secrets into a durable log/audit-row leak.

These helpers preserve the operationally useful prefix (scheme +
host or sub-protocol) and replace the rest with a sentinel.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse


_REDACTED_SUFFIX = "/<redacted>"

# JDBC URLs use a non-standard shape `jdbc:<sub-protocol>:...`.
# Capture only the leading `jdbc:<sub-protocol>` so log lines stay
# operationally useful (operator can see "it's postgres", "it's
# oracle") without exposing host / credentials.
_JDBC_HEAD_RE = re.compile(r"^(jdbc:[A-Za-z0-9_+\-]+):")


def redact_url(url: str | None) -> str:
    """Return a host-only form of an HTTP-style ``url`` safe for logs.

    Rebuilds the safe form from ``hostname`` + optional ``port`` only —
    never from raw ``netloc``, which would include any HTTP basic-auth
    ``user:password@`` prefix.

    - ``None`` / empty / non-str → ``"<unparseable-url>"``
    - well-formed URL → ``"{scheme}://{host[:port]}/<redacted>"``
    - missing scheme or host → ``"<unparseable-url>"``
    """
    if not isinstance(url, str) or not url:
        return "<unparseable-url>"
    try:
        parsed = urlparse(url)
    except Exception:
        return "<unparseable-url>"
    if not parsed.scheme or not parsed.hostname:
        return "<unparseable-url>"
    netloc = parsed.hostname
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    return f"{parsed.scheme}://{netloc}{_REDACTED_SUFFIX}"


_CRED_KEYS = (
    "password", "pwd", "user", "userid", "uid",
    "secret", "token", "apikey", "api_key",
)


def _scrub_userinfo(out: str) -> str:
    """Conservatively strip every ``//...@`` userinfo segment.

    JDBC passwords are not URL-encoded by convention and may contain
    raw ``/`` and ``@`` characters. Greedy-match the *last* ``@``
    that still falls inside the authority section so password chars
    can't escape the scrub. Authority ends at the first
    ``?`` / ``#`` / ``;`` (SQL Server property delimiter) or
    end-of-string; ``/`` is **not** a boundary because passwords
    contain literal slashes in practice.

    Examples:
      - ``//user:pwd@h:5432/db`` → ``//<credentials>@h:5432/db``
      - ``//user:p@ss@h:5432/db`` → ``//<credentials>@h:5432/db``
      - ``//user:p/ss@h:5432/db`` → ``//<credentials>@h:5432/db``
      - ``//h:5432/db?email=x@y`` (`@` in query) → unchanged
    """
    sentinel = "//"
    idx = out.find(sentinel)
    if idx < 0:
        return out
    rest_start = idx + len(sentinel)
    authority_end = len(out)
    for delim in "?#;":
        di = out.find(delim, rest_start)
        if di >= 0 and di < authority_end:
            authority_end = di
    last_at = out.rfind("@", rest_start, authority_end)
    if last_at < 0:
        return out
    return out[:rest_start] + "<credentials>@" + out[last_at + 1 :]


def _scrub_param_creds(out: str) -> str:
    """Replace credential-bearing query/property values with
    ``<redacted>``. Walks the string to handle quoted values,
    ``{...}`` SQL-Server-style values, and mixed ``&`` / ``;``
    delimiters."""
    lower = out.lower()
    n = len(out)
    pieces: list[str] = []
    i = 0
    while i < n:
        # Find the next credential key boundary.
        next_match: tuple[int, str] | None = None
        for key in _CRED_KEYS:
            j = lower.find(key + "=", i)
            if j < 0:
                continue
            # Boundary check — the byte before key must be a delimiter
            # or start of string, so we don't match `cluster_user=...`.
            if j > 0 and lower[j - 1] not in "?&;":
                continue
            if next_match is None or j < next_match[0]:
                next_match = (j, key)
        if next_match is None:
            pieces.append(out[i:])
            break
        j, key = next_match
        # Emit everything up to and including ``key=``.
        pieces.append(out[i : j + len(key) + 1])
        # Determine the value extent.
        v_start = j + len(key) + 1
        if v_start < n and out[v_start] in "{":
            # SQL Server `{...}` form: read until matching `}`.
            close = out.find("}", v_start + 1)
            if close < 0:
                # Unmatched brace — redact the rest, conservative.
                pieces.append("<redacted>")
                i = n
                continue
            v_end = close + 1
        elif v_start < n and out[v_start] in '"':
            close = out.find('"', v_start + 1)
            v_end = (close + 1) if close >= 0 else n
        else:
            # Bare value: ends at the next delimiter.
            v_end = n
            for delim in "&;":
                di = out.find(delim, v_start)
                if di >= 0 and di < v_end:
                    v_end = di
        pieces.append("<redacted>")
        i = v_end
    return "".join(pieces)


def redact_jdbc_url(url: str | None) -> str:
    """Return a credential-stripped form of a ``jdbc:`` URL.

    Preserves operationally useful routing information (sub-protocol,
    host, port, database name) while scrubbing every known credential
    carrier:
      - ``//user:pwd@host`` → ``//<credentials>@host``
      - ``?password=...`` / ``?user=...`` / SQL-Server-style
        ``;password=...`` → key replaced with ``<redacted>``

    Falls back to ``"<unparseable-jdbc-url>"`` when the URL doesn't
    match the expected ``jdbc:<sub-protocol>:`` head.
    """
    if not isinstance(url, str) or not url:
        return "<unparseable-jdbc-url>"
    m = _JDBC_HEAD_RE.match(url)
    if not m:
        return "<unparseable-jdbc-url>"
    out = url
    out = _scrub_userinfo(out)
    out = _scrub_param_creds(out)
    return out
