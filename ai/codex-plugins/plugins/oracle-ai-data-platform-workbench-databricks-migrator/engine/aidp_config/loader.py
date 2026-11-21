"""Loader for the AIDP Migrator customer YAML config.

Discovery order (first match wins):
  1. cli_config_path argument (from --config)
  2. AIDP_CONFIG_PATH env var
  3. ./aidp_migrator.yaml
  4. ~/aidp_migrator.yaml

Value precedence (highest wins):
  1. cli_overrides dict passed by the caller
  2. Environment variables (documented whitelist below)
  3. Customer YAML — sole source of truth (no merged defaults file)

The customer YAML is self-contained. Copy default_aidp_migrator.yaml (at repo
root) as your starting point; every field the tool needs is documented there.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional

import yaml
from jsonschema import Draft7Validator


_HERE = Path(__file__).resolve().parent
_SCHEMA_PATH = _HERE / "schema.json"
# Canonical template at repo root, tracked in git. Customer copies this file,
# fills in placeholders, and points the loader at their copy. There is NO
# hidden defaults.yaml — the customer YAML is the sole source of truth.
_EXAMPLE_PATH = _HERE.parent / "default_aidp_migrator.yaml"

_CONFIG_FILENAME = "aidp_migrator.yaml"

# Env var -> dotted-path in the merged config. Keep in sync with the table at
# the bottom of example.customer.yaml and the README.
_ENV_OVERRIDES: Dict[str, str] = {
    "AIDP_LAKE_OCID":     "aidp.lake_ocid",
    "AIDP_WORKSPACE_ID":  "aidp.workspace_id",
    "AIDP_REGION":        "aidp.region",
    "AIDP_OCI_PROFILE":   "aidp.oci_profile",
    "AIDP_MIGRATE_OUTPUT":   "paths.migrate_output",
    "AIDP_EXPORT_OUTPUT":   "paths.export_output",
    "AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE": "paths.target_runtime_requirements_file",
    "AIDP_COMPATIBILITY_POLICY_FILE": "paths.compatibility_policy_file",
    "AIDP_READINESS_REPORT": "runtime.readiness_report_path",
    "AIDP_MLFLOW_URI":    "secrets.mlflow_tracking_uri",
    "AIDP_ENTITY_CRYPTO_ARN":    "secrets.entity_crypto_arn",
    "AIDP_DBX_HOST":      "dbx_source.host",
    "AIDP_DBX_PROXY":     "dbx_source.proxy",
}

# Keys whose values must stay as dicts. Their child keys contain dots
# (spark.foo.bar) which don't work as attribute names on SimpleNamespace.
_DICT_KEEP = frozenset({"spark_config", "autoscale_config"})


class ConfigError(RuntimeError):
    """Raised on any config discovery, parse, or validation failure."""


def discover_config_path(cli_config_path: Optional[str] = None) -> Path:
    candidates = []
    if cli_config_path:
        candidates.append(("--config", Path(cli_config_path).expanduser()))
    env = os.environ.get("AIDP_CONFIG_PATH")
    if env:
        candidates.append(("$AIDP_CONFIG_PATH", Path(env).expanduser()))
    candidates.append(("./" + _CONFIG_FILENAME, Path.cwd() / _CONFIG_FILENAME))
    candidates.append(("~/" + _CONFIG_FILENAME, Path.home() / _CONFIG_FILENAME))

    for _source, p in candidates:
        if p.is_file():
            return p

    tried = "\n".join(f"  - {src}: {p}" for src, p in candidates)
    raise ConfigError(
        "No customer configuration file found. Tried (in order):\n"
        f"{tried}\n\n"
        f"Copy the template at {_EXAMPLE_PATH} to one of the above locations "
        "and fill in placeholders. See README.md for details."
    )


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse YAML at {path}: {e}") from e
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(
            f"Top-level YAML at {path} must be a mapping, got {type(data).__name__}"
        )
    return data


def _set_dotted(d: Dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    cur = d
    for p in parts[:-1]:
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _apply_env_overrides(merged: Dict[str, Any]) -> None:
    for env_name, dotted in _ENV_OVERRIDES.items():
        val = os.environ.get(env_name)
        if val is None or val == "":
            continue
        # Skip env override when the YAML already declares the field as a
        # credential-store ref (dict shape). Overwriting a ref with a plaintext
        # string from a stale shell export would silently downgrade Phase 2's
        # cluster-fetched secret back to a laptop-injected plaintext — the
        # exact regression Phase 2 aimed to eliminate.
        existing = _get_dotted(merged, dotted)
        if isinstance(existing, dict) and existing.get("from") == "aidp_credential_store":
            continue
        _set_dotted(merged, dotted, val)


def _apply_cli_overrides(merged: Dict[str, Any], overrides: Dict[str, Any]) -> None:
    for dotted, val in overrides.items():
        if val is None:
            continue
        _set_dotted(merged, dotted, val)


def _get_dotted(d: Dict[str, Any], dotted: str) -> Any:
    """Return d[a][b][c] for 'a.b.c' or None if any segment is missing/not a dict."""
    cur: Any = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _check_assert_present(merged: Dict[str, Any]) -> None:
    """Enforce customer-declared 'must be set' assertions.

    Runs AFTER schema validation. `assert_present` is a per-customer list of
    dotted paths that must resolve to a non-empty value for THIS deployment.
    Distinct from schema-level `required` (tool-wide infra fields).
    """
    paths = merged.get("assert_present") or []
    if not paths:
        return
    missing = []
    for p in paths:
        val = _get_dotted(merged, p)
        if val is None or val == "" or val == {} or val == []:
            missing.append(p)
    if missing:
        lines = [f"  - {p}" for p in missing]
        raise ConfigError(
            "assert_present listed these fields but they are missing / empty:\n"
            + "\n".join(lines)
            + "\nEither supply values for these paths, or remove them from "
              "assert_present if optional for this setup."
        )


# Dotted config paths that reference files/directories on disk. The loader
# fails fast if a referenced path is set but does not exist, so we never
# silently degrade (e.g. bucket_mapping missing = empty dict = wrong OCI paths).
_FILE_REFERENCES = (
    ("bucket_mapping.path", "Bucket-mapping CSV (S3 -> OCI translation)"),
    ("paths.target_runtime_requirements_file", "Target runtime requirements baseline"),
    ("paths.compatibility_policy_file", "AIDP package compatibility policy"),
)


def _check_referenced_files_exist(merged: Dict[str, Any]) -> None:
    """Fail loudly if any config-referenced file does not exist on disk.

    Runs AFTER schema + assert_present validation. Catches typos in
    `bucket_mapping.path` and friends that would otherwise silently degrade
    into empty mappings and wrong migration output.
    """
    missing = []
    for dotted, label in _FILE_REFERENCES:
        p = _get_dotted(merged, dotted)
        if not p:
            continue
        if not Path(p).expanduser().is_file():
            missing.append((dotted, p, label))
    if not missing:
        return
    lines = [f"  - {dotted} = {p!r}   ({label})" for dotted, p, label in missing]
    raise ConfigError(
        "Config references files that do not exist on disk:\n"
        + "\n".join(lines)
        + "\nFix the path(s) or create the file(s) before running."
    )


def _resolve_secrets(merged: Dict[str, Any]) -> None:
    """Runs AFTER schema validation. Both shapes (plaintext string, credential-
    store ref dict) pass through unchanged.

    - Plaintext string: value stays a string; laptop and cluster both use it as-is.
    - Credential-store ref (`{from, name, key?}`): stays a dict. Laptop-side does
      NOT fetch these values — they are consumed cluster-side by the migrator's
      injected runtime via `scripts/oci_credential_helper.py::get_secret(key)`,
      which calls `aidputils.secrets.get(name, key=...)` at each point of use.

    Schema already validated the shape. This function is a documented no-op
    kept as the extension seam if a future provider (e.g. a laptop-side vault
    fetch) needs to materialise refs into strings before downstream consumption.
    """
    return


def _validate(merged: Dict[str, Any]) -> None:
    with _SCHEMA_PATH.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(merged), key=lambda e: list(e.absolute_path))
    if errors:
        lines = ["Configuration validation failed:"]
        for e in errors:
            loc = ".".join(str(p) for p in e.absolute_path) or "<root>"
            msg = e.message
            if loc.startswith("secrets"):
                # jsonschema echoes the full offending value into the message —
                # for secrets.* that would print credentials to console/CI logs.
                msg = f"invalid value (redacted) — {e.validator} constraint failed"
            lines.append(f"  - {loc}: {msg}")
        raise ConfigError("\n".join(lines))

    # dbx_source: `host` and `proxy` are documented as mutually exclusive
    # (exactly one). The schema's anyOf only enforces "at least one" —
    # enforce the "not both" half here with a precise message.
    dbx = merged.get("dbx_source") or {}
    if isinstance(dbx, dict) and dbx.get("host") and dbx.get("proxy"):
        raise ConfigError(
            "dbx_source: `host` and `proxy` are mutually exclusive — set "
            "exactly ONE (direct Databricks host, or the OCI-side proxy "
            "that fronts it). Both are currently set."
        )


def _to_namespace(value: Any, _parent_key: str = "") -> Any:
    if isinstance(value, dict):
        if _parent_key in _DICT_KEEP:
            return dict(value)
        ns = SimpleNamespace()
        for k, v in value.items():
            setattr(ns, k, _to_namespace(v, _parent_key=k))
        return ns
    if isinstance(value, list):
        return [_to_namespace(v, _parent_key=_parent_key) for v in value]
    return value


def load_config(
    cli_config_path: Optional[str] = None,
    cli_overrides: Optional[Dict[str, Any]] = None,
) -> SimpleNamespace:
    """Discover, load, validate, and return the customer config.

    Precedence (highest wins): cli_overrides > env > customer YAML.

    The customer YAML is self-contained — every field the tool needs must be
    present. Copy default_aidp_migrator.yaml as your starting template.

    `spark_config` and `autoscale_config` stay as dicts because their keys
    contain dots (spark.foo.bar) which don't work as attribute names.
    """
    path = discover_config_path(cli_config_path)
    merged = _load_yaml(path)
    _apply_env_overrides(merged)
    if cli_overrides:
        _apply_cli_overrides(merged, cli_overrides)
    _validate(merged)
    _check_assert_present(merged)
    _check_referenced_files_exist(merged)
    _resolve_secrets(merged)
    ns = _to_namespace(merged)
    setattr(ns, "_source_path", str(path))
    return ns
