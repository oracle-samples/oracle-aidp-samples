"""ONE config file for both ends of a migration. Pure parsing, no network.

The operating assumption, chosen deliberately: the person running a migration
is an engineer with full access to both environments, so making them juggle
several files and repeat coordinates on every command buys nothing. One file
holds the Snowflake connection AND the AIDP destination, and a secret may sit
inline rather than in a companion file.

That trades away one protection, so the ones that remain have to be explicit:

  * **The file is gitignored and must stay out of tickets, commits and chat.**
    A secret inline is a secret that leaks if the file travels.
  * **Secrets are never echoed.** `redact()` is the only way this plugin
    renders a config, and `preflight` uses it.
  * **A destination read from a file is announced, not assumed.** The CLI
    prints which AIDP coordinates it took from the config, and writing still
    needs `--execute`. `target/coords.py` itself still performs no I/O at
    all -- it cannot discover a destination, only be handed one -- so a
    stale config cannot silently redirect a write.

Two shapes are accepted. The documented one is nested:

    snowflake:
      account: ORG-ACCOUNT
      ...
    aidp:
      datalake_ocid: ocid1.aidataplatform...
      ...

A flat mapping (no `snowflake:` key) is read as the Snowflake block alone,
which is what older configs look like.
"""
from __future__ import annotations

import json
import os
import pathlib
import re

__all__ = ["ConfigError", "CONFIG_NAMES", "SECRET_FIELDS", "TEMPLATE_NAME",
           "discover_config", "load_config", "redact", "resolve_secret",
           "snowflake_block", "aidp_block", "write_template",
           "MAPPING_DEFAULTS", "MAPPING_MODES", "MAPPING_STRICT", "mapping_block",
           "COMPUTE_MODES", "compute_block", "decisions_block",
           "RETRY_DEFAULTS", "retry_block", "reporting_block",
           "teardown_block"]

# The names looked for, in order, when no --config is given. One file, one
# expected name, so a second conversation finds what the first one made.
CONFIG_NAMES = ("snowmig-config.yaml", "snowmig-config.yml",
                "snowmig-config.json")

TEMPLATE_NAME = "snowmig-config.example.yaml"

# Fields whose VALUE is a credential, whether inline or as a `*_path`.
SECRET_FIELDS = ("password", "private_key", "key_content", "token",
                 "key_passphrase")

_REDACTED = "<redacted>"

# What `aidp:` may carry. An unknown key is reported rather than ignored, so a
# typo cannot silently leave a destination unset.
AIDP_FIELDS = ("datalake_ocid", "workspace", "cluster_id", "catalog",
               "external_catalog", "target_catalog", "oci_profile",
               "oci_auth", "subnet_id")


# Type-mapping decisions, under `mapping:`. These are the defaults of the
# config/CLI path -- the answers every real migration has given -- and each
# stricter mode stays selectable. The pure mapper (dialect/types.py) keeps
# its own refuse-rather-than-guess defaults for callers that pass nothing.
MAPPING_MODES = {
    "semi_structured": ("block", "string"),
    "timestamp_ntz": ("preserve", "timestamp"),
    "geospatial": ("block", "string", "wkt"),
    "source_type_drift": ("refuse", "convert"),
}
MAPPING_DEFAULTS = {
    # VARIANT/OBJECT/ARRAY carried as JSON text, warned on every column,
    # rather than blocking every table that has one.
    "semi_structured": "string",
    # The AIDP metastore refuses TIMESTAMP_NTZ at CREATE TABLE, so `preserve`
    # halts `ddl` on any table that has one. `timestamp` is a semantic
    # downgrade (read through the session timezone) and is warned as such.
    "timestamp_ntz": "timestamp",
    # GEOGRAPHY/GEOMETRY block their table, as they always have: carrying a
    # geography as text is a decision. `string` carries GeoJSON, `wkt` WKT
    # (ST_ASWKT, live-verified 2026-09-29).
    "geospatial": "block",
    # A source column whose type changed after the plan was approved. The
    # other defaults convert types that were reviewed when the plan was
    # approved; this type never was, so the table is refused (`type_drift`)
    # until assess and plan are re-run. `convert` copies the column under
    # the mapping rules for its NEW type and records the table
    # `verified_with_conversion`, never plain `verified`.
    "source_type_drift": "refuse",
}

# Top-level blocks that are neither end's connection settings.
_NON_CONNECTION_BLOCKS = ("aidp", "mapping", "compute", "decisions",
                          "retry", "reporting", "teardown")

# Warehouse-equivalent compute, under `compute:`. `new` (default) proposes
# one new cluster per Snowflake warehouse, named from its base name; `existing`
# points every warehouse at one cluster that already exists instead.
COMPUTE_MODES = ("new", "existing")


class ConfigError(ValueError):
    """The config is missing, unreadable, or says something contradictory."""


def discover_config(explicit: str | pathlib.Path | None = None, *,
                    cwd: pathlib.Path | None = None,
                    plugin_root: pathlib.Path | None = None) -> pathlib.Path:
    """Where the migration config is, in a fixed order of preference.

    Search order, and why:

      1. `--config <path>`, when given. An explicit path always wins.
      2. `./snowmig-config.yaml` in the working directory. This is where a
         config belongs when the plugin is INSTALLED rather than cloned: the
         plugin directory may be read-only, and the config is the operator's
         file, not the plugin's.
      3. the same name beside the plugin itself, which is the convenient spot
         while working inside a checkout of this repo.

    Raises with the copy-paste fix when there is none, rather than running
    against coordinates nobody confirmed.
    """
    if explicit:
        p = pathlib.Path(explicit).expanduser()
        if not p.is_file():
            raise ConfigError(
                f"--config {p} does not exist. Create it from "
                f"{TEMPLATE_NAME}, or drop the flag to look for "
                f"{CONFIG_NAMES[0]} in the current directory.")
        return p

    roots = [cwd or pathlib.Path.cwd()]
    if plugin_root:
        roots.append(pathlib.Path(plugin_root))
    for root in roots:
        for name in CONFIG_NAMES:
            candidate = root / name
            if candidate.is_file():
                return candidate

    looked = ", ".join(str(r / CONFIG_NAMES[0]) for r in roots)
    raise ConfigError(
        f"no migration config found (looked for: {looked}). Create one with "
        f"`snowmig.py init-config`, or copy {TEMPLATE_NAME} to "
        f"./{CONFIG_NAMES[0]} and fill it in — it holds the Snowflake "
        f"connection and the AIDP destination, and nothing else needs "
        f"passing on the command line.")


def write_template(destination: pathlib.Path, *,
                   template: pathlib.Path,
                   overwrite: bool = False) -> pathlib.Path:
    """Put a fill-me-in config where the operator works, mode 0600.

    Two deliberate choices:

      * it refuses to overwrite -- the file it would clobber is the one
        carrying live credentials;
      * it is created 0600 before anything is written to it, because this
        file is about to hold a password in plain text and a default-umask
        644 would make it world-readable on a shared host.
    """
    destination = pathlib.Path(destination).expanduser()
    if destination.exists() and not overwrite:
        raise ConfigError(
            f"{destination} already exists and holds credentials; refusing to "
            f"overwrite it. Edit it, or pass --force if you really mean to "
            f"replace it.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Create it closed, THEN fill it: a chmod after the write leaves a window
    # where the secret-bearing file is readable by everyone.
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(pathlib.Path(template).read_text(encoding="utf-8"))
    os.chmod(destination, 0o600)
    return destination


def load_config(path: str | pathlib.Path) -> dict:
    """Parse the migration config. Never guesses a location."""
    p = pathlib.Path(path).expanduser()
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            f"config not readable at {p}: {exc.strerror}") from exc

    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as exc:
            raise ConfigError(
                f"{p} is YAML but PyYAML is not installed; `pip install "
                f"pyyaml` or write the config as JSON") from exc
        try:
            data = yaml.safe_load(text) or {}
        except yaml.YAMLError as exc:
            # `from None`, deliberately: the parser's own exception carries a
            # snippet of the offending line, and a traceback printer would
            # render the chained cause along with this one.
            raise ConfigError(_yaml_refusal(p, exc)) from None
    else:
        try:
            data = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{p}: not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"{p}: expected a mapping at the top level")
    return data


# What PyYAML quotes inside its problem text. A token kind reads `'<scalar>'`
# or is one character (`':'`); anything longer is lifted from the file.
_QUOTED = re.compile(r"'([^']*)'|\"([^\"]*)\"")


def _quotes_the_file(text: str) -> bool:
    for match in _QUOTED.finditer(text):
        quoted = match.group(1) if match.group(1) is not None else match.group(2)
        if len(quoted) > 1 and not (quoted.startswith("<")
                                    and quoted.endswith(">")):
            return True
    return False


def _yaml_refusal(path: pathlib.Path, exc: Exception) -> str:
    """Say WHERE the YAML broke, never WHAT was there.

    This file holds the password in plain text, and a password containing
    `{`, `[`, `*`, `: ` or a leading quote is exactly what breaks the parser
    -- so the line PyYAML would quote back is the password. Only the position
    travels. The parser's problem text is kept when it names token kinds
    (`expected ',' or '}', but got ':'`) and withheld when it quotes the file
    (a composer error names the alias it could not resolve, and that alias is
    the value on the password line).
    """
    marks = [m for m in (getattr(exc, "context_mark", None),
                         getattr(exc, "problem_mark", None)) if m is not None]
    lines = sorted({m.line + 1 for m in marks})
    if not lines:
        where = ""
    elif len(lines) == 1:
        where = f" at line {lines[0]}"
    else:
        where = f" between line {lines[0]} and line {lines[-1]}"
    parts = [str(getattr(exc, attr, None) or "")
             for attr in ("context", "problem")]
    said = ", ".join(p for p in parts if p)
    detail = (f" ({said})" if said and not _quotes_the_file(said)
              else " (the parser's message is withheld: it quotes the file)")
    return (f"{path}: not valid YAML{where}{detail}. The offending line is "
            f"not shown because this file holds a credential. A value that "
            f"contains ':', '#', '{{', '[', '*' or '&', or starts with a "
            f"quote, must be wrapped in single quotes.")


def snowflake_block(config: dict) -> dict:
    """The Snowflake half, from either shape."""
    if "snowflake" in config:
        block = config.get("snowflake") or {}
        if not isinstance(block, dict):
            raise ConfigError("`snowflake:` must be a mapping")
        return block
    # A flat config predates the `aidp:` half; everything is Snowflake's.
    return {k: v for k, v in config.items()
            if k not in _NON_CONNECTION_BLOCKS}


# `mapping.enabled: false` switches the whole config-default logic off: the
# modes fall back to these -- the pure mapper's refuse-rather-than-guess
# behaviour -- and the per-field values in `mapping:` are ignored.
MAPPING_STRICT = {"semi_structured": "block", "timestamp_ntz": "preserve",
                  "geospatial": "block", "source_type_drift": "refuse"}


def mapping_block(config: dict, *, enabled: bool | None = None) -> dict:
    """The mapping decisions, defaults filled in, every value validated.

    `enabled` (the CLI's --mapping-defaults) overrides `mapping.enabled`.
    """
    block = config.get("mapping") or {}
    if not isinstance(block, dict):
        raise ConfigError("`mapping:` must be a mapping")
    toggle = block.get("enabled", True)
    if not isinstance(toggle, bool):
        raise ConfigError("mapping.enabled must be true or false")
    if enabled is not None:
        toggle = enabled
    block = {k: v for k, v in block.items() if k != "enabled"}
    unknown = sorted(k for k in block if k not in MAPPING_MODES)
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `mapping:`: {', '.join(unknown)}. Expected "
            f"any of: {', '.join(MAPPING_MODES)}")
    out = dict(MAPPING_DEFAULTS)
    for key, value in block.items():
        if value not in MAPPING_MODES[key]:
            raise ConfigError(
                f"mapping.{key}: {value!r} is not one of "
                f"{', '.join(MAPPING_MODES[key])}")
        out[key] = value
    if not toggle:
        out = dict(MAPPING_STRICT)
    return {"enabled": toggle, **out}


def aidp_block(config: dict) -> dict:
    """The AIDP half, or {} when the config carries none."""
    block = config.get("aidp") or {}
    if not isinstance(block, dict):
        raise ConfigError("`aidp:` must be a mapping")
    unknown = sorted(k for k in block if k not in AIDP_FIELDS)
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `aidp:`: {', '.join(unknown)}. Expected "
            f"any of: {', '.join(AIDP_FIELDS)}")
    return block


def resolve_secret(block: dict, inline: str, path_field: str) -> str | None:
    """A credential, from `inline` or from the file at `path_field`.

    Inline is the documented default now (one file, one place to look). A
    path still works, and is the better choice for a PEM key. Both set at
    once is a contradiction, not a precedence question: refuse rather than
    pick, because the wrong guess is an auth failure nobody can explain.
    """
    value, path = block.get(inline), block.get(path_field)
    if value and path:
        raise ConfigError(
            f"both `{inline}` and `{path_field}` are set; keep one. Inline "
            f"is simplest; a path keeps the secret out of this file.")
    if value:
        return str(value)
    if not path:
        return None
    p = pathlib.Path(str(path)).expanduser()
    try:
        return p.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ConfigError(
            f"`{path_field}` points at {p}, which is not readable: "
            f"{exc.strerror}") from exc


def redact(value):
    """The config with every credential replaced. The ONLY way to render one.

    Recurses, so a nested block cannot smuggle a secret past it, and reports
    a `*_path` as the path itself -- the path is useful to a reader and is
    not the secret.
    """
    if isinstance(value, dict):
        out = {}
        for key, inner in value.items():
            if key in SECRET_FIELDS:
                out[key] = _REDACTED if inner else inner
            else:
                out[key] = redact(inner)
        return out
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def compute_block(config: dict) -> dict:
    """The warehouse-cluster decision, validated.

    `existing` needs a cluster to point at: `compute.cluster_id`, else the
    `aidp.cluster_id` the rest of the config already names. Refused without
    one rather than silently falling back to creating clusters.
    """
    block = config.get("compute") or {}
    if not isinstance(block, dict):
        raise ConfigError("`compute:` must be a mapping")
    unknown = sorted(k for k in block if k not in ("warehouse_clusters",
                                                   "cluster_id"))
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `compute:`: {', '.join(unknown)}. Expected "
            f"warehouse_clusters, cluster_id")
    mode = block.get("warehouse_clusters", "new")
    if mode not in COMPUTE_MODES:
        raise ConfigError(
            f"compute.warehouse_clusters: {mode!r} is not one of "
            f"{', '.join(COMPUTE_MODES)}")
    cluster_id = block.get("cluster_id") or (
        (config.get("aidp") or {}).get("cluster_id") if mode == "existing"
        else None)
    if mode == "existing" and not cluster_id:
        raise ConfigError(
            "compute.warehouse_clusters: existing needs a cluster to point "
            "at -- set compute.cluster_id (or aidp.cluster_id)")
    return {"warehouse_clusters": mode,
            "cluster_id": cluster_id if mode == "existing" else None}


# Decisions the operator approved, recorded as engine inputs rather than
# carried in a conversation. The defaults are today's behaviour.
DECISION_DEFAULTS = {
    # false: provision and catalog refuse to CREATE anything (dry runs and
    # reuse of this migration's own objects still work).
    "allow_new_objects": True,
    # proposal_only: S12 proposes warehouse clusters and never creates them;
    # `provision --warehouse-clusters --execute` is refused.
    "warehouse_clusters": "create_on_confirmation",
}
_WAREHOUSE_DECISIONS = ("create_on_confirmation", "proposal_only")


def decisions_block(config: dict) -> dict:
    block = config.get("decisions") or {}
    if not isinstance(block, dict):
        raise ConfigError("`decisions:` must be a mapping")
    unknown = sorted(k for k in block if k not in DECISION_DEFAULTS)
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `decisions:`: {', '.join(unknown)}. "
            f"Expected any of: {', '.join(DECISION_DEFAULTS)}")
    out = dict(DECISION_DEFAULTS)
    out.update(block)
    if not isinstance(out["allow_new_objects"], bool):
        raise ConfigError("decisions.allow_new_objects must be true or false")
    if out["warehouse_clusters"] not in _WAREHOUSE_DECISIONS:
        raise ConfigError(
            f"decisions.warehouse_clusters: {out['warehouse_clusters']!r} is "
            f"not one of {', '.join(_WAREHOUSE_DECISIONS)}")
    return out


# Backoff for every network call (engine/retry.py):
# delay = base_delay * multiplier ** (attempt - 1), capped at max_delay.
RETRY_DEFAULTS = {"base_delay": 2.0, "multiplier": 2.0, "max_attempts": 4,
                  "max_delay": 60.0}


def retry_block(config: dict) -> dict:
    block = config.get("retry") or {}
    if not isinstance(block, dict):
        raise ConfigError("`retry:` must be a mapping")
    unknown = sorted(k for k in block if k not in RETRY_DEFAULTS)
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `retry:`: {', '.join(unknown)}. Expected "
            f"any of: {', '.join(RETRY_DEFAULTS)}")
    out = dict(RETRY_DEFAULTS)
    try:
        for key, value in block.items():
            out[key] = int(value) if key == "max_attempts" else float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"retry: every value must be a number ({exc})")
    if out["base_delay"] < 0 or out["max_delay"] < 0:
        raise ConfigError("retry: delays cannot be negative")
    if out["multiplier"] < 1:
        raise ConfigError("retry.multiplier must be >= 1 (backoff never "
                          "shrinks)")
    if out["max_attempts"] < 1:
        raise ConfigError("retry.max_attempts must be >= 1")
    return out


REPORTING_DEFAULTS = {"publish_each_stage": False,
                      "workspace_dir": "report/output"}


def reporting_block(config: dict) -> dict:
    """Where, and whether, the accumulated report is written after every
    stage. Off by default: it writes to AIDP, so it is asked for."""
    block = config.get("reporting") or {}
    if not isinstance(block, dict):
        raise ConfigError("`reporting:` must be a mapping")
    unknown = sorted(k for k in block if k not in REPORTING_DEFAULTS)
    if unknown:
        raise ConfigError(
            f"unknown key(s) under `reporting:`: {', '.join(unknown)}. "
            f"Expected any of: {', '.join(REPORTING_DEFAULTS)}")
    out = {**REPORTING_DEFAULTS, **block}
    if not isinstance(out["publish_each_stage"], bool):
        raise ConfigError("reporting.publish_each_stage must be true or false")
    folder = str(out["workspace_dir"]).strip().strip("/")
    if not folder or ".." in folder.split("/"):
        raise ConfigError("reporting.workspace_dir must be a relative "
                          "workspace path without `..`")
    out["workspace_dir"] = folder
    return out


def teardown_block(config: dict) -> dict:
    """`teardown.action`: stop (default, reversible) or delete (final)."""
    block = config.get("teardown") or {}
    if not isinstance(block, dict):
        raise ConfigError("`teardown:` must be a mapping")
    unknown = sorted(k for k in block if k != "action")
    if unknown:
        raise ConfigError(f"unknown key(s) under `teardown:`: "
                          f"{', '.join(unknown)}. Expected: action")
    action = block.get("action", "stop")
    if action not in ("stop", "delete"):
        raise ConfigError(f"teardown.action: {action!r} is not stop or delete")
    return {"action": action}
