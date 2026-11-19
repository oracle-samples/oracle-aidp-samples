"""Deterministic source-side translators.

- notebook_format            Fabric "# CELL" source format, parse + serialize
- onelake_to_oci             abfss:// OneLake and /lakehouse FUSE → oci://
- fabric_notebook_to_spark   Fabric notebook → AIDP Spark
- tsql_to_spark_sql          Warehouse T-SQL → Spark SQL
"""
from fabric_aidp.translate.types import Finding, TranslationResult

__all__ = ["Finding", "TranslationResult"]
