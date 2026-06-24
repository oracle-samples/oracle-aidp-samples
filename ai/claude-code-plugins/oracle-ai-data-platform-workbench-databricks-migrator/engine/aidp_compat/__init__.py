"""
AIDP Compatibility Layer for Databricks
=========================================
Complete drop-in replacement for Databricks-specific APIs on OCI AIDP (Spark 3.5).

Provides:
- dbutils.fs       -> OCI Object Storage / HDFS operations
- dbutils.secrets  -> OCI Vault secrets
- dbutils.widgets  -> Environment variables / notebook parameters
- dbutils.notebook -> Notebook orchestration
- dbutils.jobs     -> Job parameters via environment variables
- display()        -> DataFrame/visualization display
- sql()            -> %sql magic replacement
- translate_path() -> DBFS/S3/mount path translation to OCI

Usage in migrated notebooks:
    # Full import (recommended at top of every migrated notebook)
    from aidp_compat import dbutils, display, displayHTML, sql, translate_path

    # The above makes these globally available, matching Databricks behavior
"""

from aidp_compat.dbutils_shim import DBUtils
from aidp_compat.display import display, displayHTML
from aidp_compat.sql_magic import sql
from aidp_compat.path_translator import translate_path, PathTranslator
from aidp_compat.safe_io import (
    safe_pickle_dump, safe_pickle_load,
    safe_write_parquet, safe_save_as_table,
    safe_read_modify_write_parquet, safe_pandas_to_csv,
    safe_materialize, safe_unpersist, safe_read_file,
    load_saved_model_from_volumes,
    safe_joblib_dump, safe_joblib_load,
    safe_write_parquet_coalesced, safe_save_as_table_coalesced,
)
from aidp_compat.optuna_compat import safe_optuna_create_study, finalize_optuna_study
from aidp_compat.glue_compat import get_glue_table_s3_location
from aidp_compat.s3_compat import read_s3_object, write_s3_object
from aidp_compat.notebook import set_notebook_dir
from aidp_compat.oci_throttle import (
    apply_object_storage_hardening,
    tune_for_parallel_migration,
)
from aidp_compat.bucket_shard import BucketRouter, parse_oci_uri

# Global dbutils instance - mimics Databricks behavior
dbutils = DBUtils()

__version__ = "0.5.0"
__all__ = [
    "dbutils",
    "DBUtils",
    "display",
    "displayHTML",
    "sql",
    "translate_path",
    "PathTranslator",
    "safe_pickle_dump",
    "safe_pickle_load",
    "safe_write_parquet",
    "safe_save_as_table",
    "safe_write_parquet_coalesced",
    "safe_save_as_table_coalesced",
    "safe_read_modify_write_parquet",
    "safe_pandas_to_csv",
    "safe_materialize",
    "safe_unpersist",
    "safe_read_file",
    "load_saved_model_from_volumes",
    "safe_joblib_dump",
    "safe_joblib_load",
    "safe_optuna_create_study",
    "finalize_optuna_study",
    "get_glue_table_s3_location",
    "read_s3_object",
    "write_s3_object",
    "set_notebook_dir",
    "apply_object_storage_hardening",
    "tune_for_parallel_migration",
    "BucketRouter",
    "parse_oci_uri",
]
