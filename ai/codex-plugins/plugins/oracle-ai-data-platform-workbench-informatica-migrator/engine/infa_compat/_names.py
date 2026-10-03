"""Table names that ``DeltaTable.forName`` resolves on AIDP.

On AIDP, after ``USE <catalog>.<schema>`` (the current catalog is then an
AIDP catalog, not ``spark_catalog``), ``DeltaTable.forName(spark, "t")``
raises ``IndexOutOfBoundsException: 1`` for a ONE-part name, while
``spark.catalog.tableExists("t")`` finds the table and ``schema.t`` /
``catalog.schema.t`` both work (verified on an AIDP Spark 3.5.0 cluster,
2026-09-29). A sequence counter table, or a target the export names
without an owner, is exactly such a name.
"""
from __future__ import annotations

from typing import Any


def delta_name(spark: Any, name: str) -> str:
    """``name`` qualified with the current catalog and schema when it is a
    single part; any dotted name is returned unchanged."""
    if not name or "." in name.replace("`", ""):
        return name
    try:
        return f"{spark.catalog.currentCatalog()}.{spark.catalog.currentDatabase()}.{name}"
    except Exception:  # an older Spark without currentCatalog(): leave it
        return name


def align_to_table(spark: Any, df: Any, name: str) -> Any:
    """``df`` with every column the table ``name`` also has cast to the
    table's type, as Informatica converts a value to the target port's
    type on write.

    A Delta MERGE refuses a source column whose type differs from the
    target's (``DELTA_FAILED_TO_MERGE_FIELDS`` -- a Sequence Generator's
    BIGINT into a NUMBER(10,0) key, seen on AIDP 2026-09-29), and an
    append would silently widen the table. A table that does not exist
    yet leaves ``df`` as it is."""
    try:
        if not spark.catalog.tableExists(name):
            return df
        target = {f.name.lower(): f.dataType for f in spark.table(name).schema.fields}
    except Exception:
        return df
    from pyspark.sql import functions as F

    cols = []
    for f in df.schema.fields:
        t = target.get(f.name.lower())
        cols.append(F.col(f"`{f.name}`").cast(t).alias(f.name) if t is not None and t != f.dataType
                    else F.col(f"`{f.name}`"))
    return df.select(*cols)
