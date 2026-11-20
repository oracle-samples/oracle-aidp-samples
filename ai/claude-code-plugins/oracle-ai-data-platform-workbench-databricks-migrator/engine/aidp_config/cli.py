"""CLI helpers for entry-point scripts.

Every CLI entry point that consumes customer values should:
  1. call `add_config_args(parser)` after building its own argparse parser
  2. call `load_config_from_cli(args)` to get a Config namespace with CLI
     overrides applied

Keeps the customer-value plumbing in ONE place and makes every entry point
consistent.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Optional

from .loader import load_config


# argparse arg name -> dotted path in the merged config.
# Used both to build the argparse parser (add_config_args) and to translate
# argparse.Namespace back into cli_overrides for load_config.
_ARG_TO_DOTTED: Dict[str, str] = {
    "lake_ocid":    "aidp.lake_ocid",
    "workspace_id": "aidp.workspace_id",
    "oci_profile":  "aidp.oci_profile",
    "region":       "aidp.region",
    "migrate_output":  "paths.migrate_output",
    "export_output":  "paths.export_output",
}


def add_config_args(parser: argparse.ArgumentParser) -> None:
    """Add standard config-related CLI args to a parser.

    Every value defaults to None so it does NOT override the YAML unless the
    user actually passed it. Consumers should NOT read the argparse attribute
    directly — call load_config_from_cli(args) instead and read cfg.*.
    """
    parser.add_argument(
        "--config",
        default=None,
        help="Path to aidp_migrator.yaml. If omitted, discovers via "
             "$AIDP_CONFIG_PATH, then ./aidp_migrator.yaml, then ~/aidp_migrator.yaml.",
    )
    parser.add_argument(
        "--lake-ocid",
        default=None,
        help="Override cfg.aidp.lake_ocid for this run only.",
    )
    parser.add_argument(
        "--workspace-id",
        default=None,
        help="Override cfg.aidp.workspace_id for this run only.",
    )
    parser.add_argument(
        "--oci-profile",
        default=None,
        help="Override cfg.aidp.oci_profile for this run only.",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Override cfg.aidp.region for this run only.",
    )
    parser.add_argument(
        "--migrate-output",
        default=None,
        help="Override cfg.paths.migrate_output for this run only.",
    )
    parser.add_argument(
        "--export-output",
        default=None,
        help="Override cfg.paths.export_output for this run only.",
    )


def load_config_from_cli(args: argparse.Namespace,
                          extra_overrides: Optional[Dict[str, Any]] = None):
    """Load config using --config path + all --<field> CLI overrides."""
    overrides: Dict[str, Any] = {}
    for attr, dotted in _ARG_TO_DOTTED.items():
        val = getattr(args, attr, None)
        if val is not None:
            overrides[dotted] = val
    if extra_overrides:
        overrides.update(extra_overrides)
    return load_config(cli_config_path=getattr(args, "config", None),
                       cli_overrides=overrides)


__all__ = ["add_config_args", "load_config_from_cli"]
