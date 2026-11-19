"""Oracle AI Data Platform Spark connectors helper package.

Importable from an AIDP notebook after the user adds the plugin's scripts/
directory to sys.path. Public surface is intentionally small; each connector
skill points users at one or two helpers below.

Submodules:
    auth            - wallet, dbtoken, oci_config, user_principal, secrets
    jdbc            - oracle (ALH/ATP/ExaCS),
                      runtime_load (load custom JDBC JARs without restart)
    rest            - fusion, epm, essbase
    streaming       - kafka
    aidataplatform  - builder for the AIDP `aidataplatform` Spark format
                      (ORACLE_DB, ORACLE_EXADATA, ORACLE_ALH, ORACLE_ATP,
                      POSTGRESQL, MYSQL, MYSQL_HEATWAVE, SQLSERVER,
                      KAFKA, FUSION_BICC, GENERIC_REST)
    excel           - stdlib-only .xlsx parser for AIDP clusters that have
                      neither openpyxl nor com.crealytics.spark.excel
"""

__version__ = "0.3.0"

from .aidataplatform import AIDP_FORMAT, aidataplatform_options
from .excel import read_xlsx_stdlib

__all__ = [
    "auth",
    "jdbc",
    "rest",
    "streaming",
    "AIDP_FORMAT",
    "aidataplatform_options",
    "read_xlsx_stdlib",
]
