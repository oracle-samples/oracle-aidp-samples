"""The Spark version generated notebooks are emitted for.

One constant, because three places need to agree and had no way to:

* the API-surface guard (``tests/test_spark_api_compatibility.py``) refuses
  to emit a function newer than this;
* the generated notebook declares it, so the artifact carries its own
  requirement rather than relying on whoever deploys it to remember;
* the deployer refuses a cluster older than it.

**A floor, not a match.** The generator emits the *intersection* of what
3.5 and 4.x both accept, and pins the behavioural flags
(``spark.sql.ansi.enabled``, ``spark.sql.storeAssignmentPolicy``) rather
than inheriting cluster defaults. A notebook is therefore valid on this
version and on anything newer, which is what makes it a portable artifact:
the same file runs on a 3.5 dev cluster and a 4.x prod cluster during a
staggered upgrade, and two runs of the migrator over the same export
produce the same notebook regardless of which cluster happened to be
queried.

That is why there is no per-version code generation. Emitting one form for
3.5 and another for 4.x would couple each notebook to the cluster it was
generated against and make the generator non-deterministic, in exchange for
nothing: every construct the converter emits has a single form both
versions accept. If Spark ever *removes* something we emit -- as opposed to
widening a signature, which is backward compatible -- that case would need
a runtime branch inside the notebook, and the API-surface guard is what
would surface it.

**Raise this deliberately.** It should move when the oldest cluster in the
fleet moves, not as a side effect of upgrading a laptop. AIDP ran 3.5.0 on
every cluster when this was last checked (2026-10-01).
"""
from __future__ import annotations

# (major, minor, patch) -- the oldest Spark a generated notebook supports.
TARGET_SPARK: tuple[int, int, int] = (3, 5, 0)

# What the notebook writes into its own setup cell, and what the deployer
# compares a cluster against.
TARGET_SPARK_STR = ".".join(str(p) for p in TARGET_SPARK)


def parse_spark_version(raw: str | None) -> tuple[int, ...] | None:
    """``"3.5.0"`` -> ``(3, 5, 0)``; ``None`` when it cannot be read.

    Always at least three parts: ``"3.5"`` and ``"3.5.x"`` are both
    ``(3, 5, 0)``. Python orders a tuple after its own prefix, so an
    unpadded ``(3, 5)`` sorted *below* ``(3, 5, 0)`` and the deployer
    refused a 3.5 cluster as older than 3.5.0. A non-numeric part after
    the major (``x``, ``*``) counts as 0 -- the lowest version it could
    stand for, the only reading a floor check can safely assume.

    Returns None rather than guessing when not even the major is numeric:
    a version string we do not understand is not evidence that a cluster
    is too old, and refusing a deploy on an unparseable string would be
    worse than letting it through with a warning.
    """
    if not raw:
        return None
    parts: list[int] = []
    for chunk in str(raw).strip().split("."):
        digits = ""
        for ch in chunk:
            if ch.isdigit():
                digits += ch
            else:
                break
        if not digits and not parts:
            return None
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)
