"""Informatica to Oracle AI Data Platform migration tool.

Converts current Informatica ETL metadata - IDMC/IICS cloud mappings and
taskflows, and PowerCenter 10.5 repository exports - into executable
PySpark notebooks, AIDP Job DAGs, and target table DDL on Oracle AI Data
Platform. Source and target are addressed as AIDP catalogs (standard or
external); no credentials are written into generated notebooks.
"""

__version__ = "0.1.0"
