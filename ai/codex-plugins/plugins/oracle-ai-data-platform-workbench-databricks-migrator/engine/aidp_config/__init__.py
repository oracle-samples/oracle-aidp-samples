"""AIDP Migrator customer configuration.

Public API:
    load_config(cli_config_path=None, cli_overrides=None) -> SimpleNamespace
    discover_config_path(cli_config_path=None) -> Path
    ConfigError

Usage:
    from aidp_config import load_config
    cfg = load_config()
    session = AIDPSession(
        lake_ocid=cfg.aidp.lake_ocid,
        workspace_id=cfg.aidp.workspace_id,
        oci_profile=cfg.aidp.oci_profile,
        ...,
    )

`spark_config_overrides` and `autoscale_overrides` are exposed as dicts (their
keys contain dots). Everything else is a SimpleNamespace: attribute access
(cfg.aidp.lake_ocid) fails loudly on typos.
"""

from .cli import add_config_args, load_config_from_cli
from .loader import ConfigError, discover_config_path, load_config

__all__ = [
    "ConfigError",
    "add_config_args",
    "discover_config_path",
    "load_config",
    "load_config_from_cli",
]
