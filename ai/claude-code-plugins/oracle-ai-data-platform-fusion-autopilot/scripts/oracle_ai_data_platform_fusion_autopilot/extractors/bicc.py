"""BICC bulk extractor — mirrors the official Oracle AIDP sample.

Single canonical entry point: :func:`extract_pvo`, which composes the
``aidataplatform`` Spark format options exactly as documented in
``oracle-aidp-samples/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb``
and pdf1 Step 3:

.. code-block:: python

    spark.read.format("aidataplatform")
        .option("type", "FUSION_BICC")
        .option("fusion.service.url", FUSION_URL)
        .option("user.name", FUSION_USER)        # MUST hold BIA_ADMINISTRATOR_DUTY
        .option("password", FUSION_PASSWORD)
        .option("schema", "Financial")           # offering schema
        .option("fusion.external.storage", "<storage-name>")  # BICC console profile name
        .option("datastore", "FscmTopModelAM.SupplierExtractPVO")
        .load()

Incremental support: pass ``watermark="<ISO-8601-Z>"`` to set the
``fusion.initial.extract-date`` option (pdf2 p2-3).

The format handler internally:
1. Triggers the BICC extract via Fusion REST
2. Polls for completion
3. Reads zipped CSV + manifest from the OCI Object Storage location
   backing ``fusion.external.storage`` (a BICC console External Storage profile)
4. Returns a Spark DataFrame with the requested schema

The bundle does NOT re-implement that flow; it defers entirely to the
``aidataplatform`` connector (which is what makes this code minimal).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

    from ..schema.fusion_catalog import PvoEntry


def extract_pvo(
    spark: "SparkSession",
    pvo: "PvoEntry",
    *,
    fusion_service_url: str,
    username: str,
    password: str,
    fusion_external_storage: str,
    schema: str | None = None,
    watermark: str | None = None,
    extra_options: dict[str, str] | None = None,
) -> "DataFrame":
    """Extract one PVO via the ``aidataplatform`` Spark format with ``type=FUSION_BICC``.

    Args:
        spark: Active SparkSession.
        pvo: Curated PVO entry from :mod:`schema.fusion_catalog`. Provides
            the datastore name and offering schema.
        fusion_service_url: Fusion pod base URL
            (e.g. ``https://my-pod.fa.us-phoenix-1.oraclecloud.com``).
        username: Fusion user with BICC privileges
            (``BIA_ADMINISTRATOR_DUTY`` or ``ORA_ASM_APPLICATION_IMPLEMENTATION_ADMIN_ABSTRACT``).
        password: Fusion user password (resolved from OCI Vault upstream;
            never logged here).
        fusion_external_storage: Name of a pre-configured **BICC console** External Storage
            profile pointing at the OCI Object Storage bucket BICC writes to.
            Set up once by an admin; users reference by name (pdf1 Step 1).
        schema: Override the offering schema. Default uses ``pvo.schema``.
        watermark: ISO-8601 UTC timestamp (e.g. ``"2026-04-01T00:00:00Z"``) for
            incremental extraction via the ``fusion.initial.extract-date``
            option (pdf2 p2-3). When ``None``, performs a full extract.
        extra_options: Any additional ``aidataplatform`` Spark options to pass
            through (use sparingly; the format handler typically picks the
            right defaults).

    Returns:
        A Spark DataFrame with one row per PVO record.

    Raises:
        ValueError: If the PVO is annotated as :attr:`PvoKind.OTBI` (pdf1 Pro Tip:
            do not use OTBI reporting PVOs for bulk extraction).
    """
    from ..schema.fusion_catalog import PvoKind

    if pvo.kind == PvoKind.OTBI:
        raise ValueError(
            f"PVO {pvo.id!r} ({pvo.datastore}) is annotated as OTBI — pdf1 Pro Tip says "
            "do not use OTBI reporting PVOs for bulk extracts (unnecessarily large files, "
            "non-optimized). Pick a dedicated ExtractPVO instead, or override at your own risk."
        )

    reader = (
        spark.read.format("aidataplatform")
        .option("type", "FUSION_BICC")
        .option("fusion.service.url", fusion_service_url)
        .option("user.name", username)
        .option("password", password)
        .option("schema", schema or pvo.schema)
        .option("fusion.external.storage", fusion_external_storage)
        .option("datastore", pvo.datastore)
    )

    if watermark is not None:
        reader = reader.option("fusion.initial.extract-date", watermark)

    if extra_options:
        for key, value in extra_options.items():
            reader = reader.option(key, value)

    return reader.load()


def build_options_dict(
    pvo: "PvoEntry",
    *,
    fusion_service_url: str,
    username: str,
    password: str,
    fusion_external_storage: str,
    schema: str | None = None,
    watermark: str | None = None,
) -> dict[str, str]:
    """Construct the ``aidataplatform`` option dict without invoking Spark.

    Useful for unit tests and for users who want to pass the options dict
    to ``spark.read.format("aidataplatform").options(**opts).load()`` directly.
    """
    opts: dict[str, str] = {
        "type": "FUSION_BICC",
        "fusion.service.url": fusion_service_url,
        "user.name": username,
        "password": password,
        "schema": schema or pvo.schema,
        "fusion.external.storage": fusion_external_storage,
        "datastore": pvo.datastore,
    }
    if watermark is not None:
        opts["fusion.initial.extract-date"] = watermark
    return opts
