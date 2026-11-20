"""
AIDP Compatibility Layer for Databricks
=========================================
Drop-in replacement for the parts of Databricks that AIDP does NOT provide
natively. Modules covered here:

- dbutils.fs         -> OCI Object Storage / HDFS operations (no AIDP native)
- dbutils.notebook.entry_point.getContext() mock  (no AIDP native)
- translate_path()   -> DBFS/S3/mount path translation to OCI
- oci_throttle       -> 429 hardening for large parallel migrations
- bucket_shard       -> S3→OCI bucket routing
- entity_crypto             -> entity-crypto entity_decrypt/encrypt ARN registration
- budget, cost_observability -> Migration-run cost tracking
- notebook_policy    -> Mandatory sandbox (AIDP_SANDBOX_CATALOG/SCHEMA/PREFIX) + AST gate + policy log

Parts of Databricks that AIDP DOES provide natively (and hence NOT wrapped
here anymore):
- display()          -> AIDP-native for Spark DataFrames. For pandas DataFrames
                        use IPython.display.display (already available as
                        `Idisplay` in the migration bootstrap) or leave the
                        DataFrame as the last expression (Jupyter auto-renders).
                        aidp_compat does NOT re-export display — the migrator
                        emits IPython-native patterns instead.
- %sql cell magic    -> native Jupyter kernel magic
- dbutils.widgets    -> aidputils.widgets
- dbutils.secrets.get-> aidputils.secrets.get
- dbutils.notebook.run/exit -> aidputils.notebook.run/exit
  (Opus prompts rewrite call sites to use aidputils.* directly.)

Usage in migrated notebooks (as injected by the migration bootstrap):
    from aidp_compat import dbutils, translate_path
"""

from aidp_compat.dbutils_shim import DBUtils
from aidp_compat.path_translator import translate_path, PathTranslator
from aidp_compat import dbfs_open_hook as _dbfs_open_hook
_dbfs_open_hook.install()  # idempotent — /dbfs/* open() fallback to /Volumes
from aidp_compat.optuna_compat import safe_optuna_create_study, finalize_optuna_study
from aidp_compat.notebook_policy import (
    SandboxPolicy,
    PolicyViolation,
    SandboxUndeclaredError,
    set_sandbox_policy,
    get_policy_log,
    policy_log_markdown,
)
from aidp_compat.oci_throttle import (
    apply_object_storage_hardening,
    tune_for_parallel_migration,
)
from aidp_compat.bucket_shard import BucketRouter, parse_oci_uri
from aidp_compat.entity_crypto import ensure_entity_crypto_arn
# NOTE: set_notebook_dir is a utility for the forthcoming artifact
# bootstrap — the migrator does NOT yet inject it into saved artifacts;
# it only takes effect when a notebook calls it manually.
from aidp_compat.notebook import set_notebook_dir
from aidp_compat import budget, cost_observability

# Global dbutils instance - mimics Databricks behavior
dbutils = DBUtils()

# Single source of truth for the aidp_compat version. Both setup.py (build)
# and scripts/cluster_lifecycle.py (target wheel path resolution) read this.
__version__ = "0.5.13"
__all__ = [
    "dbutils",
    "DBUtils",
    "translate_path",
    "PathTranslator",
    "safe_optuna_create_study",
    "finalize_optuna_study",
    "SandboxPolicy",
    "PolicyViolation",
    "SandboxUndeclaredError",
    "set_sandbox_policy",
    "get_policy_log",
    "policy_log_markdown",
    "apply_object_storage_hardening",
    "tune_for_parallel_migration",
    "BucketRouter",
    "parse_oci_uri",
    "ensure_entity_crypto_arn",
    "set_notebook_dir",
    "budget",
    "cost_observability",
]
