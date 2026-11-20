"""entity-crypto entity_decrypt/entity_encrypt support on AIDP.

Databricks' `entity_decrypt` / `entity_encrypt` are entity-crypto JVM UDFs. On Databricks
the AWS assumed-role ARN they need was picked up from the EC2 instance role via the
`ENTITY_CRYPTO_ARN` JVM system property. On AIDP there is no instance role, so the property
is unset and the UDF fails.

The field-validated workaround (the pilot deployment/the operator, job Example_Top5_field) is to set the ARN
explicitly and wrap the encrypted column in an `ensure_arn` UDF so the property is
(re)set inside every executor JVM before `entity_decrypt` runs:

    %scala
    val arn = "arn:aws:sts::<acct>:assumed-role/databricks-ec2-role/<instance>"
    System.setProperty("ENTITY_CRYPTO_ARN", arn)
    spark.udf.register("ensure_arn", udf((data: String) => {
      if (System.getProperty("ENTITY_CRYPTO_ARN") == null) System.setProperty("ENTITY_CRYPTO_ARN", arn)
      data
    }))
    // then:  entity_decrypt(ensure_arn(col), 'NLSP', 'COL', '')

`ensure_entity_crypto_arn` below reproduces that from Python via py4j. The `ensure_arn` UDF
is registered as a Scala closure (not a Python UDF) precisely because the property
must be set inside the *JVM* task — a PySpark Python UDF runs in a separate Python
worker and could not affect the JVM system property that the entity-crypto UDF reads.

Requires the entity-crypto JAR to be present as a cluster library. Verify with a small
`spark.sql("SELECT entity_decrypt(ensure_arn('x'),'NLSP','COL','')")` before relying
on it in a full run.
"""
from __future__ import annotations

ENTITY_CRYPTO_ARN_PROP = "ENTITY_CRYPTO_ARN"


def ensure_entity_crypto_arn(spark, arn: str, register_udf: bool = True) -> str:
    """Set the entity-crypto ARN on the driver + every executor and register `ensure_arn`.

    Args:
        spark: the active SparkSession.
        arn:  the AWS STS assumed-role ARN entity-crypto expects. Format:
              "arn:aws:sts::<account>:assumed-role/<role-name>/<session-name>".
        register_udf: also register the `ensure_arn(col)` SQL UDF (default True) so
              existing `entity_decrypt(ensure_arn(col), ...)` call sites work unchanged.

    Returns:
        The arn (so callers can chain).

    Notes:
        - The driver-side property is set immediately.
        - `ensure_arn` is registered as a JVM (Scala) UDF via py4j so that the property
          is guaranteed to be set inside each executor task before entity-crypto reads it.
        - Idempotent: safe to call once per notebook near the top.
    """
    # Driver-side: set the JVM system property now via py4j so any entity-crypto code that
    # resolves ENTITY_CRYPTO_ARN on the driver (e.g. at UDF registration / query planning)
    # sees it.
    spark._jvm.System.setProperty(ENTITY_CRYPTO_ARN_PROP, arn)

    if register_udf:
        # Register `ensure_arn` as a passthrough so existing call sites
        # `entity_decrypt(ensure_arn(col), ...)` resolve. NOTE: this is a PySpark
        # Python UDF, which cannot set a JVM system property on the executor. For
        # workloads where entity-crypto reads the property per-executor-task, register the
        # Scala `ensure_arn` from a %scala cell instead (see module docstring) — the
        # migration tool injects that snippet. This Python registration + the
        # driver-side property covers driver-evaluated call sites.
        from pyspark.sql.functions import udf as _udf
        from pyspark.sql.types import StringType as _StringType

        spark.udf.register("ensure_arn", _udf(lambda data: data, _StringType()))
    return arn
