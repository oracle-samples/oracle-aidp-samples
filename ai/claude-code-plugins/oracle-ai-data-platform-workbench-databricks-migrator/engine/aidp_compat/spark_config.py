"""
AIDP Spark Configuration Handler
=================================
Handles Databricks-specific Spark configurations and translates them
for AIDP Spark 3.5. Addresses the 88 spark_config issues.

Databricks configs that need translation:
- spark.databricks.* -> remove or find AIDP equivalent
- spark.sql.catalog.* -> AIDP catalog configuration
- AWS-specific Hadoop configs -> OCI equivalents
- Delta Lake configs -> verify availability
"""

# Databricks-specific configs to REMOVE (no AIDP equivalent)
REMOVE_CONFIGS = {
    "spark.databricks.delta.preview.enabled",
    "spark.databricks.delta.retentionDurationCheck.enabled",
    "spark.databricks.delta.schema.autoMerge.enabled",
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
    "spark.databricks.photon.enabled",
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled",
    "spark.databricks.io.cache.enabled",
    "spark.databricks.io.cache.maxDiskUsage",
    "spark.databricks.io.cache.maxMetaDataCache",
    "spark.databricks.io.cache.compression.enabled",
    "spark.databricks.optimizer.dynamicPartitionPruning",
    "spark.databricks.repl.allowedLanguages",
    "spark.databricks.cluster.profile",
    "spark.databricks.passthrough.enabled",
    "spark.databricks.pyspark.enableProcessIsolation",
}

# Databricks configs to TRANSLATE to AIDP equivalents
TRANSLATE_CONFIGS = {
    # Delta Lake
    "spark.databricks.delta.optimizeWrite.enabled": ("spark.delta.optimizeWrite.enabled", None),
    "spark.databricks.delta.autoCompact.enabled": ("spark.delta.autoCompact.enabled", None),

    # AWS -> OCI storage
    "fs.s3a.access.key": ("fs.oci.client.auth.tenantId", None),
    "fs.s3a.secret.key": (None, "Remove - configured via OCI API key (config file at /Workspace/<oci-config-workspace-path>)"),
    "fs.s3a.endpoint": (None, "Remove - use OCI Object Storage endpoint"),
    "fs.s3a.impl": ("fs.oci.client.custom.client", "com.oracle.bmc.hdfs.BmcFilesystem"),

    # Hive metastore
    "spark.sql.catalog.spark_catalog": ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
}

# Configs to ADD for AIDP.
# OCI BmcFilesystem auth is configured at cluster level using API key (config file
# at /Workspace/<oci-config-workspace-path>, DEFAULT profile). Resource principal
# auth is NOT used on AIDP. The values below are populated from that config file.
AIDP_REQUIRED_CONFIGS = {
    # OCI HDFS connector
    "fs.oci.client.auth.tenantId": "Set from OCI config file (API key auth)",
    "fs.oci.client.auth.userId": "Set from OCI config file (API key auth)",
    "fs.oci.client.auth.fingerprint": "Set from OCI config file (API key auth)",
    "fs.oci.client.auth.pemfilepath": "Set from OCI config file (key_file path)",
    "fs.oci.client.hostname": "https://objectstorage.{region}.oraclecloud.com",
}


def filter_spark_configs(configs: dict) -> dict:
    """Filter and translate Spark configs for AIDP.

    Args:
        configs: Dict of spark config key -> value

    Returns:
        Dict with Databricks configs removed/translated for AIDP
    """
    result = {}
    warnings = []

    for key, value in configs.items():
        # Skip configs that should be removed
        if key in REMOVE_CONFIGS:
            warnings.append(f"Removed Databricks-specific config: {key}")
            continue

        # Translate configs that have AIDP equivalents
        if key in TRANSLATE_CONFIGS:
            new_key, new_val = TRANSLATE_CONFIGS[key]
            if new_key:
                result[new_key] = new_val if new_val else value
                warnings.append(f"Translated: {key} -> {new_key}")
            else:
                warnings.append(f"Removed (no equivalent): {key} - {new_val}")
            continue

        # Pass through all other configs
        result[key] = value

    if warnings:
        for w in warnings:
            print(f"[AIDP Config] {w}")

    return result


def apply_aidp_configs(spark):
    """Apply AIDP-specific configurations to a SparkSession."""
    for key, value in AIDP_REQUIRED_CONFIGS.items():
        if not value.startswith("Set via"):
            try:
                spark.conf.set(key, value)
            except Exception:
                pass
