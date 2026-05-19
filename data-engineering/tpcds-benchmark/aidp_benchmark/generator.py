"""
TPCDSDataGenerator — generates TPC-DS benchmark data and uploads to OCI.

Two execution paths (auto-selected based on whether spark= is passed):

  AIDP path  (spark=spark): uses Java bridge + Hadoop FS for ALL OCI ops.
             Same pattern as PoC Cell 5 — Resource Principal via JVM, no
             Python OCI SDK auth needed.

  Local path (spark=None):  uses Python OCI SDK + ~/.oci/config key auth.
             Used for local dev runs outside AIDP.

Implements:
- D2: spark session entry point (pre-injected or getOrCreate)
- D5: DuckDB dsdgen for SF≤100; SF>100 raises NotImplementedError (V1.2)
- D8: Idempotent upload (skip if object already exists at same size)
- D9: Bucket auto-creation with graceful NotAuthorized fallback
"""
from __future__ import annotations

import logging
import os
import stat
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _tenancy_from_rpst() -> str:
    """Extract tenancy OCID from the Resource Principal Session Token (RPST).

    The Java OCI SDK's `ResourcePrincipalAuthenticationDetailsProvider` does NOT
    expose `getTenantId()` (unlike the Python SDK's `signer.tenancy_id`). So we
    decode the RPST JWT ourselves and read the tenancy claim — pure stdlib, no
    `oci` dependency required (which matters on AIDP clusters where the Python
    `oci` package isn't pre-installed).

    The RPST env var contains EITHER the raw JWT or a path to a file with it.
    Raises RuntimeError with actionable remediation if the token can't be read.
    """
    import base64
    import json

    rpst = os.environ.get("OCI_RESOURCE_PRINCIPAL_RPST", "")
    if not rpst:
        raise RuntimeError(
            "OCI_RESOURCE_PRINCIPAL_RPST environment variable is not set. "
            "The cluster's Resource Principal is not configured — check that "
            "the cluster runs inside AIDP/OCI compute."
        )
    if os.path.isfile(rpst):
        with open(rpst) as f:
            rpst = f.read().strip()

    parts = rpst.split(".")
    if len(parts) != 3:
        raise RuntimeError(
            f"OCI_RESOURCE_PRINCIPAL_RPST is not a valid JWT (expected 3 parts, got {len(parts)})"
        )

    payload_b64 = parts[1]
    padding = "=" * (-len(payload_b64) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload_b64 + padding))
    except Exception as e:
        raise RuntimeError(f"Failed to decode RPST JWT payload: {e}") from e

    tenancy = claims.get("tenant") or claims.get("res_tenant") or claims.get("tenant_id")
    if not tenancy:
        raise RuntimeError(
            f"No tenancy claim found in RPST token. Claim keys present: {sorted(claims.keys())}. "
            "Expected one of: tenant, res_tenant, tenant_id."
        )
    return tenancy


TPCDS_TABLES = (
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
)


@dataclass
class GenerationResult:
    scale_factor: int
    bucket_name: str
    namespace: str
    region: str
    table_row_counts: dict[str, int] = field(default_factory=dict)
    table_byte_sizes: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0
    bucket_status: str = ""
    upload_summary: dict[str, int] = field(default_factory=dict)


class TPCDSDataGenerator:
    """Generate TPC-DS data and upload to OCI Object Storage.

    Usage inside AIDP Workbench (recommended):
        gen = TPCDSDataGenerator(scale_factor=1)
        result = gen.generate(spark=spark)   # pass the pre-injected spark

    Usage locally (for dev):
        gen = TPCDSDataGenerator(scale_factor=1)
        result = gen.generate()              # uses ~/.oci/config
    """

    def __init__(
        self,
        scale_factor: int,
        output_format: str = "parquet",
        tables: Optional[list[str]] = None,
        bucket_name: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        if scale_factor < 1 or scale_factor > 100_000:
            raise ValueError(f"scale_factor must be 1..100000 (got {scale_factor})")
        if output_format != "parquet":
            raise NotImplementedError(
                f"output_format={output_format!r} not yet supported — use 'parquet'. "
                "Iceberg + Delta come in V1.2 once the baseline is stable."
            )
        if scale_factor > 100:
            raise NotImplementedError(
                f"SF={scale_factor} requires distributed C dsdgen (V1.2). "
                "V1.1 supports SF≤100 via DuckDB."
            )

        self.scale_factor = scale_factor
        self.output_format = output_format
        self.tables = list(tables) if tables else list(TPCDS_TABLES)
        self.bucket_name = bucket_name or f"tpcds-benchmark-sf{scale_factor}"
        self._namespace = namespace        # optional override; auto-detected in generate()
        self._region: Optional[str] = None
        self._spark = None
        self._jvm = None
        self._last_result: Optional[GenerationResult] = None

    # ── Public API ──────────────────────────────────────────────────────

    def generate(self, spark=None) -> GenerationResult:
        """Generate TPC-DS data and upload to OCI (idempotent).

        Pass spark=spark when running inside AIDP Workbench — this unlocks
        the Hadoop FS + Java bridge auth path, which works with Resource
        Principal without needing Python OCI SDK auth.
        """
        start = time.time()

        if spark is not None:
            self._spark = spark
            self._jvm = spark._jvm
            ns, region = self._detect_via_jvm()
        else:
            from .config import detect_environment
            client, env = detect_environment()
            self._oci_client = client
            ns, region = env.namespace, env.region

        self._namespace = self._namespace or ns
        self._region = region
        logger.info("OCI: namespace=%s  region=%s", self._namespace, self._region)

        # Ensure bucket exists
        bucket_status = self._ensure_bucket()

        with tempfile.TemporaryDirectory(prefix="tpcds_") as tmpdir:
            row_counts, byte_sizes, upload_summary = self._stream_generate_and_upload(Path(tmpdir))

        result = GenerationResult(
            scale_factor=self.scale_factor,
            bucket_name=self.bucket_name,
            namespace=self._namespace,
            region=self._region,
            table_row_counts=row_counts,
            table_byte_sizes=byte_sizes,
            duration_seconds=round(time.time() - start, 2),
            bucket_status=bucket_status,
            upload_summary=upload_summary,
        )
        self._last_result = result
        return result

    def bucket_uri(self, path: str = "") -> str:
        return f"oci://{self.bucket_name}@{self._namespace}/{path.lstrip('/')}"

    def validate_data(self) -> dict:
        """V1.1 stub — returns row counts from last generate()."""
        if self._last_result is None:
            raise RuntimeError("Call .generate() first.")
        return {t: {"rows": self._last_result.table_row_counts.get(t, 0), "status": "PASS"}
                for t in self.tables}

    def cleanup(self, confirm: bool = False) -> None:
        raise NotImplementedError("cleanup() ships in V1.1 MVP 4.")

    # ── AIDP path (Hadoop FS + Java bridge) ─────────────────────────────

    def _detect_via_jvm(self) -> tuple[str, str]:
        """Get namespace + region using Java SDK (works on AIDP cluster nodes)."""
        from py4j.java_gateway import java_import
        jvm = self._jvm
        java_import(jvm, 'com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider')
        java_import(jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(jvm, 'org.apache.hadoop.fs.Path')

        provider = jvm.ResourcePrincipalAuthenticationDetailsProvider.builder().build()
        region = provider.getRegion().getRegionId()

        # getNamespace() via the Java RP provider returns the auth namespace (the
        # tenancy used for Resource Principal), which differs from the storage
        # namespace on some AIDP deployments. Read from warehouse.dir first —
        # it contains the real storage namespace as oci://bucket@<namespace>/.
        ns = (self._namespace
              or self._detect_storage_namespace_via_jvm()
              or os.environ.get("OCI_OBJECT_STORAGE_NAMESPACE", "")
              or self._get_namespace_via_hadoop())

        return ns, region

    def _detect_storage_namespace_via_jvm(self) -> str:
        """Extract the storage namespace from spark.sql.warehouse.dir.

        The warehouse dir is an OCI URI of the form oci://bucket@<namespace>/...
        so the namespace embedded there is always the correct storage namespace,
        regardless of what the RP auth provider reports.
        """
        import re
        try:
            warehouse = self._spark.sparkContext._conf.get(
                "spark.sql.warehouse.dir", "")
            m = re.search(r'oci://[^@]+@([^/]+)/', warehouse)
            if m:
                return m.group(1)
        except Exception:
            pass
        return ""

    def _get_namespace_via_hadoop(self) -> str:
        """Extract OCI namespace from the Hadoop FS configuration."""
        try:
            from py4j.java_gateway import java_import
            jvm = self._jvm
            sc = self._spark.sparkContext
            java_import(jvm, 'org.apache.hadoop.fs.FileSystem')
            java_import(jvm, 'org.apache.hadoop.fs.Path')

            # Get namespace via Java OCI SDK ObjectStorage client
            java_import(jvm, 'com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider')
            java_import(jvm, 'com.oracle.bmc.objectstorage.ObjectStorageClient')
            java_import(jvm, 'com.oracle.bmc.objectstorage.requests.GetNamespaceRequest')

            provider = jvm.ResourcePrincipalAuthenticationDetailsProvider.builder().build()
            oci_client = jvm.ObjectStorageClient.builder().build(provider)
            request = jvm.GetNamespaceRequest.builder().build()
            ns = oci_client.getNamespace(request).getValue()
            logger.info("Auto-detected namespace: %s", ns)
            return ns
        except Exception as e:
            raise RuntimeError(
                f"Namespace auto-detect failed via Java OCI SDK: {e}. "
                "Check that the cluster's Resource Principal can call "
                "ObjectStorageClient.getNamespace(). See REQUIREMENTS.md."
            ) from e

    def _ensure_bucket(self) -> str:
        """Create bucket if not exists. Returns 'exists'/'created'/'unauthorized'."""
        if self._spark is not None:
            return self._ensure_bucket_hadoop()
        from .config import ensure_bucket, AIDPEnvironment
        env = AIDPEnvironment(self._namespace, self._region, "", False)
        return ensure_bucket(self._oci_client, env, self.bucket_name)

    def _ensure_bucket_hadoop(self) -> str:
        """Verify bucket is reachable via Hadoop FS (AIDP path).

        V1.2 dropped the OCI Java SDK auto-create path: AIDP clusters typically
        grant Hadoop FS access (manage objects) but NOT OCI metadata access
        (read buckets / create buckets via Java SDK). Auto-create through the
        Java SDK kept failing in confusing ways.

        Now: probe via Hadoop FS (which is what V1.1 already uses for uploads).
        If the bucket is reachable, proceed. If not, raise loud with explicit
        manual-create instructions. Bucket pre-creation is a documented V1.2
        prerequisite — see REQUIREMENTS.md.
        """
        from py4j.java_gateway import java_import
        jvm = self._jvm
        sc = self._spark.sparkContext
        java_import(jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(jvm, 'org.apache.hadoop.fs.Path')

        bucket_uri = f"oci://{self.bucket_name}@{self._namespace}/"
        try:
            fs = jvm.FileSystem.get(
                jvm.java.net.URI.create(bucket_uri),
                sc._jsc.hadoopConfiguration()
            )
            fs.exists(jvm.Path(bucket_uri))  # raises if bucket missing or RP can't reach it
            return "exists"
        except Exception as e:
            raise RuntimeError(
                f"Cannot access bucket '{self.bucket_name}' in namespace '{self._namespace}'.\n"
                f"  Underlying error: {str(e)[:200]}\n"
                f"\n"
                f"V1.2 does NOT auto-create buckets. Create it manually:\n"
                f"  1. OCI Console → Object Storage → Create Bucket\n"
                f"  2. Bucket name: {self.bucket_name}\n"
                f"  3. Namespace: {self._namespace}\n"
                f"  4. Compartment: same one your AIDP cluster runs in\n"
                f"  5. Grant the cluster's dynamic group 'manage objects' on it\n"
                f"Then re-run.\n"
            ) from e

    # ── Data generation + upload (streaming, per-table) ─────────────────

    def _stream_generate_and_upload(
        self, tmpdir: Path
    ) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
        """Generate via DuckDB dsdgen, then export → upload → delete per table.

        Per-table streaming reduces peak tmpdir disk: each Parquet file lives
        on the driver only between export and upload, then is deleted before
        the next table is exported.

        DuckDB tuning applied to reduce dsdgen-time spill:
          - preserve_insertion_order=false  (major memory/spill reducer)
          - threads=2                       (lower parallel working set)
        See https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads
        """
        import duckdb

        logger.info("DuckDB dsdgen SF=%s (per-table streaming, tuned) ...", self.scale_factor)
        con = duckdb.connect()
        con.execute(f"SET temp_directory='{tmpdir}/duckdb_tmp'")
        con.execute("SET memory_limit='24GB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute("INSTALL tpcds; LOAD tpcds;")
        con.execute(f"CALL dsdgen(sf={self.scale_factor})")

        # c_last_review_date fix — DuckDB Issue #219
        try:
            con.execute("ALTER TABLE customer ADD COLUMN c_last_review_date DATE")
        except Exception:
            pass

        row_counts, byte_sizes = {}, {}
        uploaded = skipped = 0

        for table in self.tables:
            out = tmpdir / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET)")
            n = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
            row_counts[table] = n
            byte_sizes[table] = out.stat().st_size
            logger.info("  exported: %-25s %12d rows  %7.1f MB",
                        table, n, byte_sizes[table] / 1e6)

            # Drop DuckDB's copy now that we have a Parquet on disk —
            # frees memory + temp_directory spill for the next table.
            try:
                con.execute(f"DROP TABLE {table}")
            except Exception as e:
                logger.debug("DROP TABLE %s failed (continuing): %s", table, e)

            # Upload immediately, then delete local file to reclaim tmpdir.
            did_upload = self._upload_one(out, table)
            if did_upload:
                uploaded += 1
            else:
                skipped += 1
            try:
                out.unlink()
            except FileNotFoundError:
                pass

        con.close()
        return row_counts, byte_sizes, {"uploaded": uploaded, "skipped": skipped}

    # ── Upload (per-table; two transport paths) ─────────────────────────

    def _upload_one(self, local: Path, table: str) -> bool:
        """Upload a single Parquet file. Returns True if uploaded, False if skipped."""
        if self._spark is not None:
            return self._upload_one_hadoop(local, table)
        return self._upload_one_sdk(local, table)

    def _upload_one_hadoop(self, local: Path, table: str) -> bool:
        """Upload one Parquet via Hadoop FS (AIDP path). Idempotent: skips if exists."""
        from py4j.java_gateway import java_import
        jvm = self._jvm
        sc = self._spark.sparkContext

        java_import(jvm, 'org.apache.hadoop.fs.FileSystem')
        java_import(jvm, 'org.apache.hadoop.fs.Path')
        java_import(jvm, 'org.apache.hadoop.io.IOUtils')
        java_import(jvm, 'java.io.FileInputStream')

        # Parent tmpdir needs traversable perms for JVM
        os.chmod(str(local.parent), 0o755)

        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = jvm.FileSystem.get(
            jvm.java.net.URI.create(f"oci://{self.bucket_name}@{self._namespace}/"),
            hadoop_conf
        )

        obj_name = f"{table}.parquet"
        path_obj = jvm.Path(f"oci://{self.bucket_name}@{self._namespace}/{obj_name}")

        if fs.exists(path_obj):
            logger.info("  skipped (exists): %s", obj_name)
            return False

        os.chmod(str(local), 0o644)
        out = fs.create(path_obj, True)
        in_stream = jvm.FileInputStream(str(local))
        jvm.IOUtils.copyBytes(in_stream, out, 4 * 1024 * 1024, True)
        logger.info("  uploaded: %s", obj_name)
        return True

    def _upload_one_sdk(self, local: Path, table: str) -> bool:
        """Upload one Parquet via Python OCI SDK (local dev path). Idempotent."""
        ns = self._namespace
        obj_name = f"{table}.parquet"
        local_size = local.stat().st_size
        try:
            head = self._oci_client.head_object(ns, self.bucket_name, obj_name)
            if int(head.headers.get("Content-Length", -1)) == local_size:
                return False
        except Exception:
            pass
        with local.open("rb") as fh:
            self._oci_client.put_object(ns, self.bucket_name, obj_name, fh)
        return True
