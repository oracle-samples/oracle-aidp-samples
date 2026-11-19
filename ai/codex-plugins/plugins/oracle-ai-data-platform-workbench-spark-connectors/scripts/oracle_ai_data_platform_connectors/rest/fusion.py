"""Oracle Fusion REST + BICC helpers.

Two distinct flows:
- ``fetch_paged()`` for live REST calls (≤ 499 rows/page hard cap per MOS 2429019.1).
- ``trigger_bicc_extract()`` + ``read_bicc_csv_from_object_storage()`` for bulk
  extracts that land as gzipped CSV in OCI Object Storage.
"""

from __future__ import annotations

import io
import time
from typing import Any, Iterator, Optional

# Per Oracle MOS Doc ID 2429019.1
FUSION_PAGE_LIMIT_HARD_CAP = 499


def fetch_paged(
    session: Any,
    base_url: str,
    path: str,
    *,
    limit: int = FUSION_PAGE_LIMIT_HARD_CAP,
    fields: Optional[str] = None,
    extra_params: Optional[dict] = None,
) -> Iterator[dict]:
    """Yield rows from a Fusion REST resource, page by page.

    Args:
        session: A ``requests.Session`` from
            ``oracle_ai_data_platform_connectors.auth.user_principal.http_basic_session``.
        base_url: Fusion pod base URL (e.g.
            ``https://my-pod.fa.us-phoenix-1.oraclecloud.com``).
        path: Resource path beneath base_url (e.g.
            ``/fscmRestApi/resources/11.13.18.05/invoices``).
        limit: Page size. **Hard-capped at 499 by Fusion.** Anything higher is
            silently truncated server-side.
        fields: Comma-separated list of field names to project. Lets you avoid
            pulling unneeded columns.
        extra_params: Additional query params (e.g. ``q=...`` filters).

    Yields:
        One Python dict per row.
    """
    if limit > FUSION_PAGE_LIMIT_HARD_CAP:
        limit = FUSION_PAGE_LIMIT_HARD_CAP

    base_url = base_url.rstrip("/")
    offset = 0
    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "onlyData": "true",
        }
        if fields:
            params["fields"] = fields
        if extra_params:
            params.update(extra_params)

        url = f"{base_url}{path}"
        response = session.get(url, params=params, timeout=120)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items", [])
        if not items:
            return
        yield from items

        if not payload.get("hasMore", False):
            return
        offset += limit


def trigger_bicc_extract(
    session: Any,
    base_url: str,
    offering: str,
    *,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 3600,
) -> str:
    """Trigger a BICC extract job and wait for completion.

    Args:
        session: ``requests.Session`` with HTTP Basic auth (BICC trigger side).
        base_url: Fusion pod base URL (same as REST).
        offering: BICC offering ID (e.g. ``"FscmTopModelAM.AnalyticsServiceAM"``).
        poll_interval_seconds: How often to poll status. Default 30s.
        timeout_seconds: Give up after this many seconds. Default 1h.

    Returns:
        The OCI Object Storage prefix (relative to your BICC bucket) where
        the extract's gzipped CSV files landed. Pass this to
        ``read_bicc_csv_from_object_storage()`` to materialize as a Spark
        DataFrame.
    """
    base_url = base_url.rstrip("/")
    submit_path = "/biacm/api/v2/extracts/run"
    response = session.post(
        f"{base_url}{submit_path}",
        json={"offeringName": offering},
        timeout=60,
    )
    response.raise_for_status()
    job_id = response.json()["jobId"]

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status_response = session.get(
            f"{base_url}/biacm/api/v2/extracts/{job_id}",
            timeout=60,
        )
        status_response.raise_for_status()
        status = status_response.json()
        state = status.get("status", "").upper()
        if state in ("SUCCEEDED", "COMPLETED"):
            return status["outputPrefix"]
        if state in ("FAILED", "CANCELLED", "ERROR"):
            raise RuntimeError(f"BICC extract failed: {status}")
        time.sleep(poll_interval_seconds)

    raise TimeoutError(f"BICC extract did not finish in {timeout_seconds}s")


def read_bicc_csv_from_object_storage(
    spark: Any,
    namespace: str,
    bucket: str,
    prefix: str,
    *,
    schema: Optional[Any] = None,
):
    """Read all gzipped CSV files under an OCI Object Storage prefix into Spark.

    The AIDP cluster's Spark configuration must already have the OCI Object
    Storage connector attached (`oci://` scheme) and credentials configured —
    typically inherited from the cluster's API key profile.

    Args:
        spark: The active SparkSession.
        namespace: OCI Object Storage namespace.
        bucket: Bucket name where BICC dropped the extract.
        prefix: Output prefix from ``trigger_bicc_extract``.
        schema: Optional StructType to enforce; otherwise schema is inferred
            (slower for large files but fine for one-shot extracts).

    Returns:
        A Spark DataFrame.
    """
    path = f"oci://{bucket}@{namespace}/{prefix.lstrip('/')}/*.csv.gz"
    reader = spark.read.format("csv").option("header", "true").option(
        "compression", "gzip"
    )
    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    return reader.load(path)


def read_bicc_via_aidp_format(
    spark: Any,
    fusion_service_url: str,
    username: str,
    password: str,
    schema: str,
    datastore: str,
    fusion_external_storage: str,
):
    """Read a Fusion BICC PVO via AIDP's built-in `aidataplatform` Spark format.

    This mirrors the official Oracle AIDP sample
    (oracle-samples/oracle-aidp-samples →
    ``data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb``):

        spark.read.format("aidataplatform")
            .option("type", "FUSION_BICC")
            .option("fusion.service.url", "<URL>")
            .option("user.name", "<user>")
            .option("password", "<pwd>")
            .option("schema", "<SCHEMA>")
            .option("fusion.external.storage", "<storage-name>")
            .option("datastore", "<PVO name>")
            .load()

    The format handler internally:
    1. Triggers the BICC extract via Fusion REST.
    2. Waits for completion.
    3. Reads the extracted CSVs from the OCI Object Storage location
       backing ``fusion.external.storage`` (which is a pre-configured
       AIDP external-storage profile).
    4. Returns the result as a Spark DataFrame with the requested schema.

    Args:
        spark: Active SparkSession.
        fusion_service_url: Fusion pod base URL
            (e.g. ``https://my-pod.fa.us-phoenix-1.oraclecloud.com``).
        username: Fusion user with BICC privileges
            (e.g. ``BIA_ADMINISTRATOR_DUTY`` or equivalent role).
        password: Fusion user password.
        schema: Target schema (BICC offering schema name, e.g. ``ERP``).
        datastore: PVO (Public View Object) name to extract.
        fusion_external_storage: Name of a pre-configured AIDP external
            storage profile pointing at the OCI Object Storage bucket
            BICC writes to. This is set up once in the AIDP catalog by
            an administrator; users just reference it by name.

    Returns:
        Spark DataFrame.
    """
    return (
        spark.read.format("aidataplatform")
            .option("type", "FUSION_BICC")
            .option("fusion.service.url", fusion_service_url)
            .option("user.name", username)
            .option("password", password)
            .option("schema", schema)
            .option("fusion.external.storage", fusion_external_storage)
            .option("datastore", datastore)
            .load()
    )


def rows_to_spark_dataframe(spark: Any, rows: Iterator[dict], *, mode: str = "json_string"):
    """Materialize an iterator of dicts as a Spark DataFrame.

    Fusion REST responses commonly contain nested objects (HATEOAS links,
    addresses, classifications) plus all-null columns. PySpark schema
    inference can't merge ``StringType`` and ``StructType`` for the same
    column across rows, so the default ``mode="json_string"`` packs every
    row into a single ``row_json`` STRING column. Caller can then ``from_json``
    selectively for the fields they need.

    Args:
        spark: SparkSession.
        rows: iterator of dicts (e.g. from ``fetch_paged``).
        mode: ``"json_string"`` (recommended; deterministic, robust)
            or ``"infer"`` (legacy; may fail on nested data).
    """
    import json as _json

    import pandas as pd

    rows_list = list(rows)
    if not rows_list:
        # Spark needs SOMETHING — return a 0-row DataFrame with a placeholder col.
        return spark.createDataFrame([], "placeholder STRING")

    if mode == "json_string":
        # Single-column, deterministic, type-safe across nested + null shapes.
        json_rows = [(_json.dumps(r),) for r in rows_list]
        return spark.createDataFrame(json_rows, schema="row_json STRING")

    if mode == "infer":
        # Legacy: try pandas-based inference. Fails on nested struct / all-null cols.
        pdf = pd.DataFrame(rows_list)
        return spark.createDataFrame(pdf)

    raise ValueError(f"unknown mode {mode!r}; use 'json_string' or 'infer'")
