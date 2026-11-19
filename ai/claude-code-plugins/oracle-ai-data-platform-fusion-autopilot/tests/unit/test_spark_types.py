"""Unit tests for the Spark-free type-normalisation helper.

Confirms the synonym table is the single source of truth shared by the runtime
AIDPF-4070 gate (re-exported from sql_runner) and the design-time AIDPF-2045
column-contract gate (imported from spark_types) — so the two gates agree.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.orchestrator import spark_types
from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner


def test_normalise_maps_synonyms() -> None:
    assert spark_types._normalise_spark_type("INT") == "integer"
    assert spark_types._normalise_spark_type("int") == spark_types._normalise_spark_type(
        "integer"
    )
    assert spark_types._normalise_spark_type("long") == "bigint"
    # Unknown / parameterised types pass through lowercased + stripped.
    assert spark_types._normalise_spark_type("  Decimal(18,0) ") == "decimal(18,0)"


def test_sql_runner_reexports_same_objects() -> None:
    # The 4070 gate must use the exact same function + table, not a copy.
    assert sql_runner._normalise_spark_type is spark_types._normalise_spark_type
    assert sql_runner._SPARK_TYPE_SYNONYMS is spark_types._SPARK_TYPE_SYNONYMS
