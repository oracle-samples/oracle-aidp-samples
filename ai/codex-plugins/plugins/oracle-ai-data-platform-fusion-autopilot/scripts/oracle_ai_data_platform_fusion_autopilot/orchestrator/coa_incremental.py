"""Spark-side orchestration for the additive-COA incremental fast path.

Feature: incremental-coa-chart-onboarding. The pure classification lives in
``coa_change``; this module is the thin Spark/IO glue that the orchestrator
composes:

* :func:`read_protected_charts` — the materialised charts an incremental will
  NOT revisit (``DISTINCT chart_of_accounts_id`` across dims that carry it).
* :func:`manifest_for_run` — read + parse a specific run's ``__run_manifest__``
  row, so each node's prior plan-hash is paired with the COA baseline that
  actually produced it (Round-2/3 Finding 4 — per-run pairing).
* :class:`CoaIncrementalContext` — per-run state threaded (optionally) into the
  node execution paths; its :meth:`coa_accept_reason` is the per-node AIDPF-4040
  acceptance decision, with the render-time prior-equivalent hash supplied by the
  caller (which owns the renderer).
* :func:`decide_coa_accept` — the pure decision core (unit-tested without Spark).

Default-inactive: when the context is absent or ``active`` is False, the node
paths behave exactly as before this feature.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from .coa_change import CoaVerdict, classify_coa_change

if TYPE_CHECKING:  # pragma: no cover
    from ..config.paths import TablePaths
    from ..schema.medallion_pack import ResolvedPack


def _coa_dim_targets(pack: "ResolvedPack", paths: "TablePaths") -> list[str]:
    """Fully-qualified tables that materialise per-chart rows — every silver/gold
    node whose ``outputSchema`` declares a ``chart_of_accounts_id`` column. These
    are the authority on which charts are materialised (NOT ``gl_coa`` active
    charts — Round-2 Finding 1)."""
    targets: list[str] = []
    for layer, nodes, schema in (
        ("silver", pack.silver, paths.silver_schema),
        ("gold", pack.gold, paths.gold_schema),
    ):
        for node in nodes.values():
            cols = getattr(getattr(node, "output_schema", None), "columns", None) or []
            if any(getattr(c, "name", None) == "chart_of_accounts_id" for c in cols):
                targets.append(f"{paths.catalog}.{schema}.{node.target}")
    return targets


def read_protected_charts(
    spark: Any, pack: "ResolvedPack", paths: "TablePaths"
) -> "frozenset[str] | None":
    """Return the set of ``chart_of_accounts_id`` values already materialised in
    the COA dimension(s).

    Returns ``None`` (fail-closed) if a target table EXISTS but cannot be read —
    the caller must then block the additive path. A target that does not exist
    yet (first run) contributes nothing (empty result), which correctly makes any
    chart freely mappable when no dimension rows exist.
    """
    protected: set[str] = set()
    for table in _coa_dim_targets(pack, paths):
        try:
            exists = bool(spark.catalog.tableExists(table))
        except Exception:  # noqa: BLE001 — catalog probe unavailable → be safe
            exists = True  # assume it might exist; a failed read below fails closed
        if not exists:
            continue
        try:
            rows = spark.sql(
                f"SELECT DISTINCT chart_of_accounts_id FROM {table} "
                f"WHERE chart_of_accounts_id IS NOT NULL"
            ).collect()
        except Exception:  # noqa: BLE001 — existing table unreadable → fail closed
            return None
        protected.update(str(r[0]) for r in rows if r[0] is not None)
    return frozenset(protected)


def manifest_for_run(
    spark: Any, paths: "TablePaths", run_id: str
) -> "dict[str, Any] | None":
    """Read + parse the ``__run_manifest__`` row for ``run_id``. Returns ``None``
    when the row is absent (a pre-feature run) or unparseable, or the
    ``run_manifest`` column does not exist (a pre-feature state table)."""
    from . import state as v1_state
    from .run_manifest import RUN_MANIFEST_DATASET_ID, parse_manifest

    table_path = v1_state._state_table_path(paths)
    try:
        if "run_manifest" not in v1_state._existing_state_columns(spark, table_path):
            return None
        rows = spark.sql(
            f"SELECT run_manifest FROM {table_path} "
            f"WHERE run_id = '{run_id}' "
            f"AND dataset_id = '{RUN_MANIFEST_DATASET_ID}' "
            f"AND run_manifest IS NOT NULL LIMIT 1"
        ).collect()
    except Exception:  # noqa: BLE001 — no baseline recoverable
        return None
    if not rows:
        return None
    raw = rows[0][0]
    try:
        return parse_manifest(raw)
    except Exception:  # noqa: BLE001 — malformed baseline → no additive path
        return None


def decide_coa_accept(
    *,
    verdict: CoaVerdict | None,
    is_coa_source: bool,
    coa_checkpoint_passed: bool,
    prior_equivalent_hash: str | None,
    stored_prior_hash: str,
) -> bool:
    """Pure AIDPF-4040 additive-acceptance decision (unit-tested without Spark).

    Accept the plan-hash advance for this node IFF ALL hold:

    * ``verdict == "additive"`` — the COA change is a proven additive arm;
    * a downstream node additionally requires the post-land COA data checkpoint
      to have passed this run; the COA-SOURCE node (``gl_coa``) does NOT — it
      produces the data the checkpoint reads, so requiring it would deadlock
      (Round-2 Finding 3);
    * the per-node prior-equivalent hash (this node re-rendered under the prior
      COA baseline + the stored prior ``profile_hash``) equals the stored prior
      plan-hash — i.e. the node's ENTIRE delta is the accepted COA change; a
      non-COA edit riding along fails this and blocks.
    """
    if verdict != "additive":
        return False
    if not is_coa_source and not coa_checkpoint_passed:
        return False
    if prior_equivalent_hash is None:
        return False
    return prior_equivalent_hash == stored_prior_hash


@dataclass
class CoaIncrementalContext:
    """Per-run state for the additive-COA fast path, threaded (optionally) into
    the node execution paths. Inactive by default — a ``None`` context or
    ``active=False`` leaves every gate at its pre-feature behaviour."""

    active: bool
    incoming_coa: dict[str, Any]
    protected_charts: "frozenset[str] | None"
    coa_source_ids: "frozenset[str]"
    coa_checkpoint_passed: bool
    # run_id -> parsed manifest dict (or None). Injected so the decision stays
    # testable and the Spark read lives in one place.
    manifest_by_run_id: Callable[[str], "dict[str, Any] | None"]

    def coa_accept_reason(
        self,
        *,
        node: Any,
        prior_run_id: "str | None",
        stored_prior_hash: str,
        recompute_hash: Callable[[dict[str, Any], str], str],
    ) -> "str | None":
        """Return an acceptance reason string if this node's AIDPF-4040 drift is a
        proven additive-COA change, else ``None`` (block / normal gate).

        ``recompute_hash(prior_coa_projection, prior_profile_hash)`` is supplied by
        the caller (it owns the renderer): it re-renders THIS node under the prior
        COA baseline and returns the prior-equivalent plan-hash.
        """
        if not self.active or self.protected_charts is None or not prior_run_id:
            return None  # inactive, fail-closed read, or no paired baseline
        baseline = self.manifest_by_run_id(prior_run_id)
        if not baseline:
            return None
        prior_coa = baseline.get("coa_projection")
        prior_profile_hash = baseline.get("profile_hash")
        if not isinstance(prior_coa, dict) or not isinstance(prior_profile_hash, str):
            return None  # v1 baseline (no COA projection) → conservative block
        verdict = classify_coa_change(
            prior_coa, self.incoming_coa, self.protected_charts
        )
        prior_equiv: str | None
        try:
            prior_equiv = recompute_hash(prior_coa, prior_profile_hash)
        except Exception:  # noqa: BLE001 — recompute failed → fail closed
            prior_equiv = None
        accepted = decide_coa_accept(
            verdict=verdict,
            is_coa_source=node.id in self.coa_source_ids,
            coa_checkpoint_passed=self.coa_checkpoint_passed,
            prior_equivalent_hash=prior_equiv,
            stored_prior_hash=stored_prior_hash,
        )
        if not accepted:
            return None
        return (
            f"coa-additive-arm: plan-hash advance accepted "
            f"(prior_run={prior_run_id}, verdict=additive)"
        )


__all__ = [
    "CoaIncrementalContext",
    "decide_coa_accept",
    "manifest_for_run",
    "read_protected_charts",
]
