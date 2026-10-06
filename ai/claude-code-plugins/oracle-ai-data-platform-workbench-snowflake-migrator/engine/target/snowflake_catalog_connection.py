"""Build `connectionDetails` for an EXTERNAL/SNOWFLAKE AIDP catalog.

Read from the ONE migration config file, never from inline arguments or
environment variables -- the same rule `snowflake_source/conn.py` applies to
the source side. The credential itself may be inline in that file (the
documented default: one file, everything in it) or a path to a separate file.
Either way it is never rendered: only the FIELD NAMES reach a report.

LIVE-VERIFIED KEY NAMES (2026-09-16). The API itself enumerated the allowed
`connectionProperties` for SNOWFLAKE when handed a bogus key -- observed
against a real deployment:

    SNOWFLAKE_HOST, SNOWFLAKE_PORT, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE_NAME, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
    SNOWFLAKE_AUTHENTICATION_METHOD, SNOWFLAKE_PRIVATE_KEY_FILE,
    SNOWFLAKE_PRIVATE_KEY_CONTENT, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
    WORKSPACE_KEY, WORKSPACE_NAME

Notes the list itself settles: the key travels as CONTENT (the PEM text, read
from the config's key_path at call time), there is NO PAT/token property (so
`auth: pat` is refused here), and there is no schema property. The
AUTHENTICATION_METHOD enum is "Basic" | "KeyPair" -- also live-enumerated
by the API when handed a wrong value.
"""
from __future__ import annotations

import json
import pathlib

__all__ = ["ConnectionConfigError", "load_connection_config",
           "build_snowflake_connection_details"]

_AUTH_MODES = ("keypair", "password", "pat")


class ConnectionConfigError(ValueError):
    """The connection config file is missing, unreadable, or incomplete."""


def load_connection_config(path: str | pathlib.Path) -> dict:
    """Parse a YAML or JSON connection config file. Never guesses a location."""
    p = pathlib.Path(path).expanduser()
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConnectionConfigError(
            f"connection config not readable at {p}: {exc.strerror}") from exc

    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as exc:
            raise ConnectionConfigError(
                "PyYAML is not installed; either `pip install pyyaml` or "
                "write the config as JSON instead") from exc
        data = yaml.safe_load(text) or {}
    else:
        try:
            data = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError as exc:
            raise ConnectionConfigError(f"{p}: not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ConnectionConfigError(f"{p}: expected a mapping at the top level")
    return data


def _read_secret_file(path: str, label: str) -> str:
    p = pathlib.Path(path).expanduser()
    try:
        return p.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ConnectionConfigError(
            f"{label} not readable at {path}: {exc.strerror}") from exc


def build_snowflake_connection_details(config: dict) -> dict:
    """`connectionDetails` for a SNOWFLAKE EXTERNAL catalog, from a loaded config.

    Required: account, warehouse, database, auth (one of "keypair", "password",
    "pat"), user. The credential may sit INLINE in the one config file, which
    is the documented default, or in a file the config points at:

      keypair  -- private_key   (or key_path,      + optional key_passphrase)
      password -- password      (or password_path)
      pat      -- token         (or pat_path)

    `schema` is accepted and ignored: an EXTERNAL catalog registers the whole
    database, and the same file's `schema` belongs to the source side.
    """
    missing = [k for k in ("account", "warehouse", "database", "auth", "user")
               if not config.get(k)]
    if missing:
        raise ConnectionConfigError(
            "connection config is missing required field(s): "
            + ", ".join(sorted(missing)))

    auth = str(config["auth"]).strip().lower()
    if auth not in _AUTH_MODES:
        raise ConnectionConfigError(
            f"auth must be one of {_AUTH_MODES}, got {config['auth']!r}")

    account = str(config["account"]).strip()
    # The API takes a HOST, not an account locator; the standard host form is
    # <account>.snowflakecomputing.com, overridable with an explicit `host`.
    host = str(config.get("host")
               or f"{account.lower()}.snowflakecomputing.com").strip()
    details = {
        "SNOWFLAKE_HOST": host,
        "SNOWFLAKE_PORT": str(config.get("port") or 443),
        "SNOWFLAKE_USERNAME": str(config["user"]).strip(),
        "SNOWFLAKE_DATABASE_NAME": str(config["database"]).strip(),
        "SNOWFLAKE_WAREHOUSE": str(config["warehouse"]).strip(),
    }
    if config.get("role"):
        details["SNOWFLAKE_ROLE"] = str(config["role"]).strip()
    # `schema` is deliberately IGNORED rather than refused. One config file
    # serves both ends: the source side needs a real schema to scope the
    # connector's pushdown session, and the live catalog contract has no
    # schema property at all -- an EXTERNAL catalog registers the whole
    # database. Refusing it (as this did while the config was catalog-only)
    # makes the one-file design impossible: the same field would have to be
    # present for `assess` and absent for `catalog`.
    # (Nothing is added to `details` for it: that dict IS the API body, whose
    # allowed keys the live contract enumerates and whose unknown keys it
    # rejects with a 400. The caller reports the omission instead.)

    # A secret may be inline in the one config file, or in a file the config
    # points at. Both are supported; inline is the documented default.
    def secret(inline: str, path_field: str, label: str) -> str | None:
        if config.get(inline):
            return str(config[inline])
        if config.get(path_field):
            return _read_secret_file(config[path_field], label)
        return None

    if auth == "keypair":
        key = secret("private_key", "key_path", "private key")
        if not key:
            raise ConnectionConfigError(
                "auth: keypair needs `private_key` (inline) or `key_path`")
        details["SNOWFLAKE_AUTHENTICATION_METHOD"] = "KeyPair"
        details["SNOWFLAKE_PRIVATE_KEY_CONTENT"] = key
        passphrase = secret("key_passphrase", "key_passphrase_path",
                            "private key passphrase")
        if passphrase:
            details["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"] = passphrase
    elif auth == "password":
        password = secret("password", "password_path", "password")
        if not password:
            raise ConnectionConfigError(
                "auth: password needs `password` (inline) or `password_path`")
        details["SNOWFLAKE_AUTHENTICATION_METHOD"] = "Basic"
        details["SNOWFLAKE_PASSWORD"] = password
    else:  # pat
        raise ConnectionConfigError(
            "auth: pat is not supported for an AIDP Snowflake catalog "
            "connection — its allowed connection properties have no token "
            "field. Use keypair (preferred) or password.")

    return details
