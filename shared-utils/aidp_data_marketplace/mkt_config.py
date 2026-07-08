"""Configuration for the AIDP Marketplace UI.

Loads/saves the editable instance settings in ``config.yaml`` that live next to
this module. The UI lets the user change the AIDP instance details (endpoint,
OCID, workspace, cluster, catalogs, OCI profile) at any time; changes are
persisted back to ``config.yaml``.
"""

import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")

# Fields the UI is allowed to edit, in display order. (key, label, help)
EDITABLE_FIELDS = [
    ("name", "Instance name", "A label for this AIDP instance."),
    ("region", "OCI region", "e.g. us-ashburn-1"),
    ("endpoint", "AIDP endpoint", "https://aidp.<region>.oci.oraclecloud.com"),
    ("platform_id", "AI Data Platform OCID", "ocid1.aidataplatform.oc1..."),
    ("oci_profile", "OCI config profile", "Profile in ~/.oci/config used to sign requests."),
    ("workspace", "Workspace key", "Workspace whose cluster runs the SQL."),
    ("cluster_key", "Cluster key", "Compute cluster that executes the Spark jobs."),
    ("cluster_name", "Cluster name", "Display name of the compute cluster."),
    ("marketplace_catalog", "Marketplace catalog", "Catalog the table is published into (e.g. marketplace)."),
    ("marketplace_schema", "Marketplace schema", "Schema the table is published into (e.g. default)."),
    ("default_table_format", "Default table format", "PARQUET, CSV, ORC, DELTA, JSON ..."),
    ("job_timeout_seconds", "Job timeout (s)", "Max seconds to wait for a Spark job run."),
]

_DEFAULTS = {
    "name": "AR_AIDP_V0",
    "region": "us-ashburn-1",
    "endpoint": "https://aidp.us-ashburn-1.oci.oraclecloud.com",
    "platform_id": "",
    "oci_profile": "DEFAULT",
    "workspace": "",
    "cluster_key": "",
    "cluster_name": "",
    "table_catalog": "std_catalog",
    "table_schema": "bronze",
    "marketplace_catalog": "marketplace",
    "marketplace_schema": "default",
    "default_table_format": "PARQUET",
    "job_timeout_seconds": 420,
}

_INT_FIELDS = {"job_timeout_seconds"}


def load_config():
    """Return the current config as a dict (defaults merged with config.yaml)."""
    cfg = dict(_DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as fh:
            stored = yaml.safe_load(fh) or {}
        cfg.update({k: v for k, v in stored.items() if v is not None})
    cfg["job_timeout_seconds"] = int(cfg.get("job_timeout_seconds") or 420)
    return cfg


def save_config(updates):
    """Merge ``updates`` into the stored config and write config.yaml.

    Only keys in EDITABLE_FIELDS are accepted. Returns the new full config.
    """
    cfg = load_config()
    allowed = {k for k, _, _ in EDITABLE_FIELDS}
    for key, value in (updates or {}).items():
        if key not in allowed:
            continue
        if value is None:
            continue
        value = str(value).strip()
        if key in _INT_FIELDS:
            try:
                cfg[key] = int(value)
            except ValueError:
                continue
        else:
            cfg[key] = value
    with open(CONFIG_PATH, "w") as fh:
        yaml.safe_dump(cfg, fh, default_flow_style=False, sort_keys=False)
    return cfg
