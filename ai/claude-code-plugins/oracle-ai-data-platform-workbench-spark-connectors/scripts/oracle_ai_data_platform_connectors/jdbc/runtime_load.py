"""Load JDBC drivers and Spark DataSource JARs into a running AIDP Spark session at runtime.

Spark's normal mechanism for adding a driver / DataSource JAR is to set
``spark.jars`` at session creation time, which means stopping the kernel and
re-bootstrapping the notebook context. That's awkward in AIDP because the
notebook itself owns the SparkSession lifecycle.

This module does it without restarting:

* The **driver** JVM gets a new ``URLClassLoader`` rooted at the existing
  thread context CL, with the JARs added. The loader is set as the thread
  context CL so ``ServiceLoader`` (used by Spark to look up DataSources) and
  ``Utils.classForName`` (used by JDBC) resolve the classes. For JDBC drivers
  specifically, the driver class is also registered with
  ``java.sql.DriverManager`` so any code path that goes through
  ``DriverManager.getConnection`` finds it.

* The **executors** receive the JARs via ``SparkContext.addJar()``. Without
  this step, Spark can serialize task code that references the new classes
  but executors can't deserialize it (``ClassNotFoundException`` deep in
  ``ObjectInputStream.resolveClass``). Add-jar uploads each JAR to the
  driver's HTTP file-server and adds it to each executor's
  ``ExecutorClassLoader`` chain.

Use ``add_spark_connector_at_runtime`` for Spark DataSource JARs (Snowflake,
spark-excel, custom format providers). Use ``add_jdbc_jar_at_runtime`` for
plain JDBC drivers (SQLite, ClickHouse, DuckDB, IBM DB2 ...).

Two patterns covered:
* SQLite JDBC and similar (no executor distribution needed — driver-only).
* Snowflake-style Spark connectors (needs both driver and executor distribution).

Maven Central is reachable from AIDP clusters; PyPI is not.
"""

from __future__ import annotations

from typing import Iterable, Optional


# Internal helper: shared classloader-rewire logic
def _install_urlclassloader(spark, jar_paths: Iterable[str]):
    """Build a URLClassLoader rooted at the current thread CL covering ``jar_paths``,
    set it as the thread context class loader, and return it.
    """
    jar_paths = list(jar_paths)
    jvm = spark._jvm
    gw = spark.sparkContext._gateway

    urls = gw.new_array(jvm.java.net.URL, len(jar_paths))
    for i, p in enumerate(jar_paths):
        urls[i] = jvm.java.io.File(p).toURI().toURL()

    parent = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    loader = jvm.java.net.URLClassLoader(urls, parent)
    jvm.java.lang.Thread.currentThread().setContextClassLoader(loader)
    return loader


def _distribute_to_executors(spark, jar_paths: Iterable[str]) -> None:
    """Push JARs to executors via the SparkContext's HTTP file-server."""
    for p in jar_paths:
        spark._jsc.addJar(p)


def add_jdbc_jar_at_runtime(
    spark,
    *,
    jar_path: str,
    driver_class: str,
    distribute_to_executors: bool = True,
) -> None:
    """Make a JDBC driver class loadable in the current Spark session.

    Args:
        spark: The active ``SparkSession``.
        jar_path: Filesystem path to the JDBC driver JAR. Must be visible to
            the driver JVM — typically ``/tmp/...`` (after a download) or
            ``/Volumes/...``.
        driver_class: The JDBC driver class name, e.g. ``org.sqlite.JDBC``.
        distribute_to_executors: If True (default), also call ``addJar`` so
            executors fetch the JAR. Required when the JDBC read partitions
            execute on multiple executors. Pass False only for fully
            driver-local cases like in-memory SQLite.

    Example:
        >>> jar = download_jdbc_jar(
        ...     maven_url="https://repo1.maven.org/maven2/org/xerial/"
        ...               "sqlite-jdbc/3.46.0.0/sqlite-jdbc-3.46.0.0.jar",
        ...     target_path="/tmp/sqlite-jdbc-3.46.0.0.jar")
        >>> add_jdbc_jar_at_runtime(spark, jar_path=jar,
        ...                         driver_class="org.sqlite.JDBC")
    """
    jvm = spark._jvm
    loader = _install_urlclassloader(spark, [jar_path])

    cls = loader.loadClass(driver_class)
    driver = cls.newInstance()
    jvm.java.sql.DriverManager.registerDriver(driver)

    if distribute_to_executors:
        _distribute_to_executors(spark, [jar_path])


def add_spark_connector_at_runtime(
    spark,
    *,
    jar_paths: Iterable[str],
    verify_classes: Optional[Iterable[str]] = None,
    register_jdbc_driver_class: Optional[str] = None,
) -> None:
    """Install a Spark DataSource (and optionally a JDBC driver) at runtime.

    Use this for connectors that register a Spark format via
    ``META-INF/services/org.apache.spark.sql.sources.DataSourceRegister``.
    Examples:

    * Snowflake (``net.snowflake.spark.snowflake.DefaultSource`` registered as
      ``snowflake``).
    * Crealytics Spark Excel
      (``com.crealytics.spark.excel.WorkbookReader`` and the v2 source).
    * Any third-party DataSource the user has on a Volume.

    Args:
        spark: The active ``SparkSession``.
        jar_paths: All JARs the connector needs (typically the connector JAR
            plus its JDBC driver JAR; e.g. for Snowflake pass both
            ``spark-snowflake_2.12-X.Y.Z.jar`` and ``snowflake-jdbc-X.Y.Z.jar``).
        verify_classes: Optional list of class names to load through the new
            class loader as a sanity check before returning. If any class is
            missing, ``ClassNotFoundException`` propagates.
        register_jdbc_driver_class: If provided, also register an instance of
            this JDBC driver with ``DriverManager`` (needed for connectors
            that fall through to JDBC for some operations — e.g. Snowflake).

    Example (Snowflake):
        >>> add_spark_connector_at_runtime(
        ...     spark,
        ...     jar_paths=[
        ...         "/tmp/spark-snowflake_2.12-3.1.1.jar",
        ...         "/tmp/snowflake-jdbc-3.19.0.jar",
        ...     ],
        ...     verify_classes=[
        ...         "net.snowflake.spark.snowflake.DefaultSource",
        ...         "net.snowflake.client.jdbc.SnowflakeDriver",
        ...     ],
        ...     register_jdbc_driver_class="net.snowflake.client.jdbc.SnowflakeDriver",
        ... )
        >>> df = (spark.read.format("snowflake")
        ...           .options(**snow_opts).option("query", "SELECT 1").load())
    """
    jvm = spark._jvm
    loader = _install_urlclassloader(spark, jar_paths)

    if verify_classes:
        for cn in verify_classes:
            loader.loadClass(cn)  # raises if missing

    if register_jdbc_driver_class:
        cls = loader.loadClass(register_jdbc_driver_class)
        driver = cls.newInstance()
        jvm.java.sql.DriverManager.registerDriver(driver)

    _distribute_to_executors(spark, jar_paths)


def download_jdbc_jar(
    *,
    maven_url: str,
    target_path: str,
    overwrite: bool = False,
) -> str:
    """Convenience wrapper around urllib to fetch a driver JAR from Maven Central.

    Args:
        maven_url: Full URL to the JAR.
        target_path: Where to write it — must be JVM-readable (``/tmp/...`` is
            recommended).
        overwrite: If False (default) and the target already exists, skip the
            download.

    Returns:
        ``target_path`` for chaining.
    """
    import os
    import urllib.request

    if overwrite or not os.path.exists(target_path):
        urllib.request.urlretrieve(maven_url, target_path)
    return target_path
