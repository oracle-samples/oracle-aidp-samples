"""
DBUtils Shim - Main entry point that wires up all sub-modules.
Provides a drop-in replacement for Databricks dbutils on AIDP Spark 3.5.
"""

import os


class DBUtils:
    """Drop-in replacement for Databricks dbutils."""

    def __init__(self, spark=None):
        """Initialize DBUtils with optional SparkSession.

        If spark is None, attempts to get the active session.
        """
        self._spark = spark

    @property
    def spark(self):
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession
                self._spark = SparkSession.builder.getOrCreate()
            except Exception:
                pass
        return self._spark

    @property
    def fs(self):
        if not hasattr(self, '_fs'):
            from aidp_compat.fs import AIDPFileSystemUtils
            self._fs = AIDPFileSystemUtils(self.spark)
        return self._fs

    @property
    def secrets(self):
        if not hasattr(self, '_secrets'):
            from aidp_compat.secrets import AIDPSecretsUtils
            self._secrets = AIDPSecretsUtils()
        return self._secrets

    @property
    def widgets(self):
        if not hasattr(self, '_widgets'):
            from aidp_compat.widgets import AIDPWidgetUtils
            self._widgets = AIDPWidgetUtils()
        return self._widgets

    @property
    def notebook(self):
        if not hasattr(self, '_notebook'):
            from aidp_compat.notebook import AIDPNotebookUtils
            self._notebook = AIDPNotebookUtils(self.spark)
        return self._notebook

    @property
    def jobs(self):
        if not hasattr(self, '_jobs'):
            from aidp_compat.jobs import AIDPJobUtils
            self._jobs = AIDPJobUtils()
        return self._jobs

    @property
    def library(self):
        if not hasattr(self, '_library'):
            from aidp_compat.library import AIDPLibraryUtils
            self._library = AIDPLibraryUtils()
        return self._library

    @property
    def credentials(self):
        if not hasattr(self, '_credentials'):
            from aidp_compat.credentials import AIDPCredentialsUtils
            self._credentials = AIDPCredentialsUtils()
        return self._credentials

    @property
    def data(self):
        if not hasattr(self, '_data'):
            from aidp_compat.data import AIDPDataUtils
            self._data = AIDPDataUtils(self.spark)
        return self._data

    def help(self):
        """Print help information about available utilities."""
        print("AIDP Compatibility Layer for Databricks DBUtils")
        print("=" * 50)
        print("Available modules:")
        print("  dbutils.fs       - File system operations (OCI Object Storage)")
        print("  dbutils.secrets  - Secret management (OCI Vault)")
        print("  dbutils.widgets  - Widget/parameter management")
        print("  dbutils.notebook - Notebook orchestration")
        print("  dbutils.jobs     - Job task values")
        print("  dbutils.library  - Library management")
        print("  dbutils.credentials - Credential management")
        print("  dbutils.data     - Data utilities")
        print()
        print("Use dbutils.<module>.help() for module-specific help.")
