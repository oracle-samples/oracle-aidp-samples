"""
AIDP Safe I/O Helpers
======================
Wrappers for file operations that handle AIDP's /Volumes FUSE mount
consistency issues. The FUSE layer (backed by OCI Object Storage) has
write-then-read delays - a file written may not be immediately visible.

Usage in migrated notebooks:
    from aidp_compat.safe_io import safe_pickle_dump, safe_pickle_load, safe_write_parquet

These are also available as:
    from aidp_compat import safe_pickle_dump, safe_pickle_load, safe_write_parquet
"""

import os
import time
import pickle
import shutil
import tempfile
from typing import Any, Optional

# Default delay after write before read on /Volumes FUSE mount
FUSE_WRITE_DELAY = 3  # seconds


def safe_pickle_dump(obj: Any, filepath: str, delay: float = FUSE_WRITE_DELAY) -> str:
    """Write a pickle file with FUSE consistency handling.

    Writes to a temp file first, flushes, fsyncs, then renames to final path.
    Adds a delay after write to allow FUSE propagation.

    Args:
        obj: Object to pickle
        filepath: Target file path
        delay: Seconds to wait after write for FUSE consistency

    Returns:
        The filepath written to
    """
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)

    # Write to temp file in same directory (same filesystem for atomic rename)
    tmp_path = filepath + ".tmp"
    try:
        with open(tmp_path, "wb") as f:
            pickle.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())

        # Delete existing file first — FUSE /Volumes can't atomically replace
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError:
                pass
        # Rename into place
        shutil.move(tmp_path, filepath)
    except Exception:
        # Cleanup temp on failure
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    # FUSE consistency delay
    if delay > 0:
        time.sleep(delay)

    return filepath


def safe_pickle_load(filepath: str, retries: int = 3, delay: float = FUSE_WRITE_DELAY) -> Any:
    """Load a pickle file with retry for FUSE consistency.

    Retries if the file is not yet visible after a write.

    Args:
        filepath: File to load
        retries: Number of retry attempts
        delay: Seconds between retries

    Returns:
        The unpickled object
    """
    for attempt in range(retries):
        try:
            with open(filepath, "rb") as f:
                return pickle.load(f)
        except (FileNotFoundError, EOFError, PermissionError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise FileNotFoundError(
                    f"Failed to load after {retries} attempts ({delay}s each): {filepath}. "
                    f"Last error: {e}. This may be a FUSE mount consistency issue."
                ) from e


def safe_write_parquet(df, path: str, mode: str = "overwrite", **kwargs):
    """Write a Spark DataFrame to parquet with copy-on-write safety.

    If mode is 'overwrite' and the DataFrame was read from the SAME path,
    this uses a temp path to avoid the read-write-same-path issue on AIDP
    (no ACID guarantees on OCI Object Storage for non-Delta tables).

    Pattern:
    1. Write to {path}_tmp/
    2. Wait for FUSE consistency
    3. Remove original {path}/
    4. Rename {path}_tmp/ to {path}/

    Args:
        df: Spark DataFrame to write
        path: Target path (OCI or /Volumes)
        mode: Write mode ('overwrite', 'append', etc.)
        **kwargs: Additional args passed to df.write.parquet()
    """
    if mode == "overwrite":
        tmp_path = path.rstrip("/") + "_aidp_tmp"

        # Write to temp location
        df.write.mode("overwrite").parquet(tmp_path, **kwargs)

        # FUSE consistency delay
        time.sleep(FUSE_WRITE_DELAY)

        # Remove original and rename
        try:
            _remove_path(path)
        except Exception:
            pass  # Original may not exist yet

        _rename_path(tmp_path, path)
        time.sleep(FUSE_WRITE_DELAY)
    else:
        # For append mode, write directly (no conflict)
        df.write.mode(mode).parquet(path, **kwargs)


def safe_save_as_table(df, table_name: str, mode: str = "overwrite",
                       format: str = "parquet", **kwargs):
    """Save DataFrame as a Spark table with consistency handling.

    For overwrite mode, caches the DataFrame first to break the
    read-write dependency chain.

    Args:
        df: Spark DataFrame
        table_name: Target table name (schema.table)
        mode: Write mode
        format: Table format (parquet, delta, etc.)
        **kwargs: Additional args
    """
    if mode == "overwrite":
        # Cache to break read-write dependency
        df = df.cache()
        df.count()  # Force materialization

    df.write.mode(mode).format(format).saveAsTable(table_name, **kwargs)

    if mode == "overwrite":
        df.unpersist()


def safe_read_modify_write_parquet(spark, path: str, transform_fn, **write_kwargs):
    """Read parquet from a path, transform, write back to the SAME path safely.

    This is the most dangerous pattern on AIDP - reading and writing to the
    same parquet path without ACID guarantees. This function handles it:

    1. Read from original path
    2. Cache the DataFrame (breaks lazy dependency on source files)
    3. Apply transform function
    4. Write to {path}_aidp_tmp/
    5. Wait for FUSE consistency
    6. Remove original path
    7. Rename tmp to original path

    Usage:
        from aidp_compat import safe_read_modify_write_parquet

        def my_transform(df):
            return df.filter(df.status == 'active').withColumn('processed', lit(True))

        safe_read_modify_write_parquet(spark, 'oci://bucket@ns/data/', my_transform)

    Args:
        spark: SparkSession
        path: Parquet path to read from AND write back to
        transform_fn: Function that takes a DataFrame and returns a transformed DataFrame
        **write_kwargs: Additional args for df.write.parquet() (partitionBy, etc.)
    """
    from pyspark import StorageLevel

    # Read and cache to break dependency on source files
    df = spark.read.parquet(path)
    df = df.cache()
    df.count()  # Force full materialization into memory/disk

    # Apply transformation
    result_df = transform_fn(df)

    # If the transform returns a different DataFrame, cache that too
    if result_df is not df:
        result_df = result_df.cache()
        result_df.count()

    # Write to temp path
    tmp_path = path.rstrip("/") + "_aidp_tmp"
    result_df.write.mode("overwrite").parquet(tmp_path, **write_kwargs)
    time.sleep(FUSE_WRITE_DELAY)

    # Swap: remove original, rename tmp
    try:
        _remove_path(path)
    except Exception:
        pass
    _rename_path(tmp_path, path)
    time.sleep(FUSE_WRITE_DELAY)

    # Cleanup caches
    df.unpersist()
    if result_df is not df:
        result_df.unpersist()

    return spark.read.parquet(path)


# Default target Parquet file size in MB. 256 MB matches Delta's
# tuneFileSizesForRewrites guidance and is large enough that a large-scale
# wave does not flood Object Storage with thousands of <1 MB part files.
DEFAULT_TARGET_FILE_MB = 256


def _estimate_target_partitions(df, target_mb: int = DEFAULT_TARGET_FILE_MB) -> int:
    """Estimate the partition count that yields ~``target_mb`` per output file.

    Uses Spark's logical-plan stats when available (Catalyst's
    ``optimizedPlan().stats().sizeInBytes()``). Falls back to the current
    partition count if stats are unavailable. Always returns at least 1.

    This is a heuristic -- compressed Parquet output is typically 5-10x
    smaller than the in-memory size Catalyst reports, so the result is a
    safe upper bound on file count, not an exact match. The goal is to
    eliminate the 100s-of-tiny-files pattern, not hit a precise size.
    """
    target_bytes = max(1, target_mb) * 1024 * 1024
    try:
        plan = df._jdf.queryExecution().optimizedPlan()  # type: ignore[attr-defined]
        size_bytes = plan.stats().sizeInBytes()
        if size_bytes and size_bytes > 0:
            est = max(1, int((size_bytes + target_bytes - 1) // target_bytes))
            # Clamp to current partition count -- coalesce can shrink but
            # not safely grow without a full repartition shuffle, which is
            # the wrong tool here.
            current = df.rdd.getNumPartitions()
            return min(est, current) if current > 0 else est
    except Exception:
        pass
    try:
        current = df.rdd.getNumPartitions()
        return max(1, current)
    except Exception:
        return 1


def safe_write_parquet_coalesced(
    df,
    path: str,
    mode: str = "append",
    target_file_mb: int = DEFAULT_TARGET_FILE_MB,
    partition_by: Optional[list] = None,
    **kwargs,
):
    """Write a Spark DataFrame to Parquet, coalescing first to control file count.

    Designed to prevent the small-file write storm that triggers OCI
    Object Storage 429s + CircuitBreaker trips during parallel migration.

    Coalesce is preferred over repartition because it avoids a shuffle.
    For small DataFrames (1-2 partitions already) this is effectively a
    no-op. For DataFrames partitioned hundreds-of-ways from upstream
    operators it collapses to ~``target_file_mb`` per output file.

    Args:
        df: Spark DataFrame.
        path: Target path (oci://, /Volumes, etc.).
        mode: Write mode -- ``append`` (default) or ``overwrite``. For
            ``overwrite`` against the SAME source path, prefer
            ``safe_write_parquet`` which has the temp-swap dance.
        target_file_mb: Target file size in MB. Default 256.
        partition_by: Optional list of column names for partitioned writes.
        **kwargs: Forwarded to ``df.write.parquet(...)``.

    Returns:
        The path written to.
    """
    n = _estimate_target_partitions(df, target_mb=target_file_mb)
    out = df.coalesce(n) if n >= 1 else df

    writer = out.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path, **kwargs)
    return path


def safe_save_as_table_coalesced(
    df,
    table_name: str,
    mode: str = "append",
    format: str = "parquet",
    target_file_mb: int = DEFAULT_TARGET_FILE_MB,
    partition_by: Optional[list] = None,
    **kwargs,
):
    """Save a DataFrame as a Spark table with coalesce-before-write.

    Same intent as :func:`safe_write_parquet_coalesced` but routes through
    ``saveAsTable`` so the catalog (HMS / Unity Catalog) is updated.

    Args:
        df: Spark DataFrame.
        table_name: Target table (``schema.table`` or 3-part).
        mode: Write mode. For ``overwrite``, the DataFrame is cached and
            counted to break read-write dependency on the same files.
        format: Table format (``parquet``, ``delta``, ``orc``).
        target_file_mb: Target output file size in MB.
        partition_by: Optional list of partition columns.
        **kwargs: Forwarded to ``saveAsTable``.
    """
    if mode == "overwrite":
        df = df.cache()
        df.count()

    n = _estimate_target_partitions(df, target_mb=target_file_mb)
    out = df.coalesce(n) if n >= 1 else df

    writer = out.write.mode(mode).format(format)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    try:
        writer.saveAsTable(table_name, **kwargs)
    finally:
        if mode == "overwrite":
            try:
                df.unpersist()
            except Exception:
                pass


def safe_pandas_to_csv(df_pandas, filepath: str, delay: float = FUSE_WRITE_DELAY, **kwargs):
    """Write a pandas DataFrame to CSV with FUSE consistency handling.

    Writes to temp file, fsyncs, renames, then waits.

    Args:
        df_pandas: pandas DataFrame
        filepath: Target CSV path
        delay: FUSE consistency delay
        **kwargs: Additional args for df.to_csv()
    """
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    tmp_path = filepath + ".tmp"
    try:
        df_pandas.to_csv(tmp_path, **kwargs)
        # fsync
        with open(tmp_path, "rb") as f:
            os.fsync(f.fileno())
        shutil.move(tmp_path, filepath)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

    if delay > 0:
        time.sleep(delay)

    return filepath


def safe_materialize(df, large: bool = False, checkpoint_dir: Optional[str] = None):
    """Force Spark lazy evaluation plan materialization before a write operation.

    Solves the classic stale lazy eval problem:
        df = spark.read.parquet("oci://bucket/path/")   # captures file list lazily
        other_df.write.mode("overwrite").parquet("oci://bucket/path/")  # overwrites source
        df.write.mode("overwrite")...                   # BOOM — df still has stale file list

    Spark's execution plan is lazily evaluated: it captures the file list at read() time
    but only reads actual data at write() time. If the source files are overwritten between
    those two points, the plan is stale and the job will fail with AnalysisException or
    silently write wrong data.

    Usage:
        from aidp_compat import safe_materialize

        # Before writing a DataFrame whose source may have been overwritten upstream:
        df = safe_materialize(df)
        df.write.mode("overwrite").format("parquet").saveAsTable("schema.table")

        # For large DataFrames (>memory), use checkpoint to spill to disk:
        df = safe_materialize(df, large=True, checkpoint_dir="/Volumes/default/default/checkpoints/")
        df.write.mode("overwrite").format("parquet").saveAsTable("schema.table")

    Args:
        df: Spark DataFrame with a potentially stale execution plan
        large: If True, uses checkpoint() instead of cache() — writes to disk,
               breaks lineage entirely, safe for DataFrames that don't fit in memory
        checkpoint_dir: Required when large=True. Path for checkpoint storage
                        (e.g. "/Volumes/default/default/dbfs/checkpoints/")

    Returns:
        Materialized DataFrame (use the returned reference, not the original)
    """
    if large:
        if not checkpoint_dir:
            raise ValueError(
                "checkpoint_dir is required when large=True. "
                "Use a /Volumes path, e.g. '/Volumes/default/default/checkpoints/'"
            )
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "No active SparkSession — cannot set checkpoint directory. "
                "Call safe_materialize() from within a running Spark job."
            )
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        return df.checkpoint()
    else:
        df = df.cache()
        df.count()  # Forces actual read NOW — materializes into memory/disk
        return df


def safe_unpersist(df):
    """Unpersist a cached DataFrame after its write is complete.

    Call after safe_materialize() + write() to release cluster memory:
        df = safe_materialize(df)
        df.write.mode("overwrite").format("parquet").saveAsTable("schema.table")
        safe_unpersist(df)

    Args:
        df: DataFrame previously passed to safe_materialize()
    """
    try:
        df.unpersist()
    except Exception:
        pass


def safe_read_file(filepath: str, retries: int = 3, delay: float = 1.0,
                   mode: str = "r", encoding: str = "utf-8") -> Any:
    """Read a file with retry for AIDP /Volumes FUSE cache inconsistency.

    Reading the same /Volumes path twice in rapid succession can trigger
    FileNotFoundError on the second read due to FUSE cache invalidation.
    This helper retries with a short delay to work around this.

    Args:
        filepath: File to read
        retries: Number of retry attempts
        delay: Seconds between retries
        mode: File open mode ('r', 'rb', etc.)
        encoding: Text encoding (ignored for binary mode)

    Returns:
        File contents (str for text mode, bytes for binary mode)
    """
    kwargs = {} if "b" in mode else {"encoding": encoding}
    for attempt in range(retries):
        try:
            with open(filepath, mode, **kwargs) as f:
                return f.read()
        except (FileNotFoundError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise FileNotFoundError(
                    f"Failed to read after {retries} attempts ({delay}s each): {filepath}. "
                    f"Last error: {e}. This may be a FUSE mount cache invalidation issue."
                ) from e


def load_saved_model_from_volumes(volumes_path: str, custom_objects=None):
    """Load a TensorFlow/Keras SavedModel from /Volumes with FUSE consistency handling.

    Loading a TF SavedModel directly from /Volumes intermittently fails with:
        Input/output error [Op:RestoreV2]
    because TensorFlow's C++ RestoreV2 op reads multiple files atomically
    (variables.index + variables.data-00000-of-00001) and FUSE may not have
    propagated all shards from OCI Object Storage to the kernel cache yet.

    This helper copies the entire model directory to a local /tmp path first,
    then loads from local disk — no FUSE involved in the actual model load.

    Usage:
        from aidp_compat import load_saved_model_from_volumes

        # Replace:
        model = tf.keras.models.load_model("/Volumes/default/default/dbfs/.../model.savedmodel")

        # With:
        model = load_saved_model_from_volumes("/Volumes/default/default/dbfs/.../model.savedmodel")

        # With custom objects:
        model = load_saved_model_from_volumes(path, custom_objects={"MyLayer": MyLayer})

    Args:
        volumes_path: Path to the SavedModel directory on /Volumes
        custom_objects: Optional dict of custom objects for tf.keras.models.load_model()

    Returns:
        Loaded Keras model

    Raises:
        ImportError: If TensorFlow is not installed
        FileNotFoundError: If the model path does not exist on /Volumes
        OSError: If the model copy or load fails after retries
    """
    try:
        import tensorflow as tf
    except ImportError as e:
        raise ImportError(
            "TensorFlow is not installed. Install via cluster libraries: tensorflow>=2.12.0"
        ) from e

    if not os.path.exists(volumes_path):
        raise FileNotFoundError(
            f"SavedModel not found at {volumes_path}. "
            f"If it was just written, add time.sleep(5) before loading to allow FUSE flush."
        )

    tmp_dir = tempfile.mkdtemp(prefix="aidp_tf_model_")
    local_model_path = os.path.join(tmp_dir, "model")
    try:
        # Wait for FUSE to flush all variable shards before copying
        time.sleep(FUSE_WRITE_DELAY)

        shutil.copytree(volumes_path, local_model_path)

        kwargs = {}
        if custom_objects is not None:
            kwargs["custom_objects"] = custom_objects

        return tf.keras.models.load_model(local_model_path, **kwargs)
    finally:
        # Clean up temp dir (model is loaded into memory, temp files no longer needed)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _remove_path(path: str):
    """Remove a path (file or directory) using Spark's Hadoop filesystem if available,
    otherwise use os operations."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            jvm = spark._jvm
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs_path = jvm.org.apache.hadoop.fs.Path(path)
            uri = jvm.java.net.URI(path)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            if fs.exists(fs_path):
                fs.delete(fs_path, True)
                return
    except Exception:
        pass

    # Fallback to os operations
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def _rename_path(src: str, dst: str):
    """Rename/move a path using Spark's Hadoop filesystem if available."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            jvm = spark._jvm
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            src_path = jvm.org.apache.hadoop.fs.Path(src)
            dst_path = jvm.org.apache.hadoop.fs.Path(dst)
            uri = jvm.java.net.URI(src)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            fs.rename(src_path, dst_path)
            return
    except Exception:
        pass

    # Fallback
    shutil.move(src, dst)


def safe_joblib_dump(obj: Any, filepath: str, delay: float = FUSE_WRITE_DELAY, **kwargs) -> str:
    """Write an object using joblib with FUSE consistency handling.

    Writes to a /tmp temp file first, then moves to the target /Volumes path.
    /tmp is a local disk (not FUSE-backed), so the dump is always reliable.
    The move is atomic on most filesystems, and the delay allows FUSE propagation.

    Drop-in for:
        joblib.dump(obj, "/Volumes/.../model.joblib")
    Replace with:
        from aidp_compat import safe_joblib_dump
        safe_joblib_dump(obj, "/Volumes/.../model.joblib")

    Also handles /dbfs/ paths (translates to /Volumes/ automatically).
    """
    import joblib

    # Translate any legacy /dbfs/ paths transparently
    if filepath.startswith("/dbfs/"):
        filepath = "/Volumes/default/default/dbfs" + filepath[5:]

    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)

    # Dump to /tmp (local disk, no FUSE), then move to target
    with tempfile.NamedTemporaryFile(delete=False, suffix=".joblib") as tmp:
        tmp_path = tmp.name
    try:
        joblib.dump(obj, tmp_path, **kwargs)
        shutil.move(tmp_path, filepath)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    time.sleep(delay)
    return filepath


def safe_joblib_load(filepath: str, retries: int = 3, delay: float = FUSE_WRITE_DELAY, **kwargs):
    """Load an object using joblib with retry for FUSE cache invalidation.

    FUSE may not make a file visible immediately after a write. This helper
    retries with a delay on FileNotFoundError or EOFError (truncated file).

    Drop-in for:
        joblib.load("/Volumes/.../model.joblib")
    Replace with:
        from aidp_compat import safe_joblib_load
        safe_joblib_load("/Volumes/.../model.joblib")
    """
    import joblib

    # Translate any legacy /dbfs/ paths transparently
    if filepath.startswith("/dbfs/"):
        filepath = "/Volumes/default/default/dbfs" + filepath[5:]

    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            return joblib.load(filepath, **kwargs)
        except (FileNotFoundError, EOFError) as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(delay)
        except Exception as e:
            # Non-retriable error (e.g. corrupted file) — raise immediately
            raise
    raise last_exc  # type: ignore[misc]
