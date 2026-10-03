"""infa_compat -- cluster-side runtime library for Informatica semantics
that need real state, not just an inline scalar expression (
Class C).

Installed ONTO the AIDP cluster (see ``engine/setup.py``), distinct from
``infa2aidp`` -- the workstation-side generator/migrator package. Every
Class A construct (``IIF``, ``DECODE``, ``TO_DATE``/``TO_CHAR`` format
masks, ``NVL``, casts, aggregates, ...) stays inlined by
``infa2aidp.converters.expression_converter``; this package owns exactly
the constructs that need a persisted counter, a multi-row policy
decision, a file-scoped precedence merge, or a MERGE-shaped write that
differs by target catalog type. See ``SUPPORTED_OPERATIONS.md`` for the
closed contract the generator is constrained to call.

Importing this package (or any of its submodules) never requires
PySpark to be installed -- every ``pyspark``/``delta``/``oracledb``
import in this package is deferred into a function body, checked by
``tests/test_infa_compat.py::test_import_without_pyspark`` and exercised
by ``demo.sh``, which runs with no cluster and no Spark.
"""
from __future__ import annotations

__version__ = "0.1.0"

from .datemask import DATE_FORMAT_MAP, to_java_format
from .decode import build_when_chain, decode_fallthrough, split_pairs_and_default
from ._names import align_to_table, delta_name
from .lookup import VALID_POLICIES, LookupMultipleMatchError, cached_lookup, lookup_join
from .params import (
    ParameterNotFoundError,
    ParameterScope,
    SCOPE_PRECEDENCE,
    activate,
    load_parameter_file,
    param,
    sess_start_time,
)
from .scd2 import Scd2ConfigError, Scd2MergeResult, scd2_merge
from .sequence import (
    AdwSequenceBackend,
    DeltaSequenceBackend,
    Sequence,
    SequenceBackend,
    SequenceExhausted,
    get_sequence_backend,
    sequence,
)
from .update_strategy import (
    NonIntegerStrategyColumnError,
    UpdateStrategyCode,
    UpdateStrategyResult,
    apply_update_strategy,
    write_update_strategy,
)

__all__ = [
    "__version__",
    # datemask
    "DATE_FORMAT_MAP",
    "to_java_format",
    # decode
    "build_when_chain",
    "decode_fallthrough",
    "split_pairs_and_default",
    # lookup
    "VALID_POLICIES",
    "LookupMultipleMatchError",
    "cached_lookup",
    "align_to_table",
    "delta_name",
    "lookup_join",
    # params
    "ParameterNotFoundError",
    "ParameterScope",
    "SCOPE_PRECEDENCE",
    "activate",
    "load_parameter_file",
    "param",
    "sess_start_time",
    # scd2
    "Scd2ConfigError",
    "Scd2MergeResult",
    "scd2_merge",
    # sequence
    "AdwSequenceBackend",
    "DeltaSequenceBackend",
    "Sequence",
    "SequenceBackend",
    "SequenceExhausted",
    "get_sequence_backend",
    "sequence",
    # update_strategy
    "NonIntegerStrategyColumnError",
    "UpdateStrategyCode",
    "UpdateStrategyResult",
    "apply_update_strategy",
    "write_update_strategy",
]
