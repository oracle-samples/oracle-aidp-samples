"""Execution-identity hashing + content-pack plan-hash for resume drift detection.

A resume operation needs to prove that the bundle being resumed against is
materially the same as the bundle that started the run. "Same" splits across
two axes:

1. **Execution identity** — non-plan environmental knobs that change the
   semantic meaning of "same plan": Fusion pod (`serviceUrl`), BICC storage
   profile (`externalStorage`), Fusion principal (`username`), AIDP target
   paths (`catalog` / `bronzeSchema` / `silverSchema` / `goldSchema`), and
   plugin code version. Secrets (`password`, vault OCIDs) are deliberately
   excluded — the hash is persisted to ``fusion_autopilot_state`` and surfaced
   in error messages; identity ≠ credentials. Surfaced by
   :func:`_identity_dict`, consumed by :func:`orchestrator.resume.check_identity_drift`.

2. **Content-pack plan-hash** — per-node SHA256 over pack/node identity +
   refresh strategy + rendered SQL hash + output-schema hash + profile hash
   + tenant fingerprint. Computed inside :func:`compute_content_pack_plan_hash`
   and stored on every state-row by ``sql_runner.execute_node``. The
   resume drift gate (AIDPF-4040) compares the prior row's hash against
   the freshly-computed one.

The v1 plan-hash entrypoints (``hash_resolved_plan``,
``serialize_plan_snapshot``, ``build_current_diagnostics``, ``_node_tuple``,
``_canonical_payload``) were retired with the old ``_execute_node`` dispatcher.
Legacy snapshots stored under the v1 shape remain readable by
:func:`orchestrator.resume.check_identity_drift` — that helper only reads
the ``identity`` half of the JSON; the ``nodes`` half is fed back into
:func:`orchestrator.resume.render_drift_error` as a sequence of dicts.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
    from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle


def _identity_dict(
    bundle: "Bundle",
    paths: "TablePaths",
    plugin_version: str,
) -> dict[str, str]:
    """Extract the 8-field execution identity from bundle + paths + version.

    Secrets (``bundle.fusion.password``, vault OCIDs) are excluded;
    ``fusion.username`` is non-secret by Oracle convention and pins the
    principal (mixed-authorization guard).
    """
    return {
        "fusion.serviceUrl": bundle.fusion.service_url,
        "fusion.externalStorage": bundle.fusion.external_storage,
        "fusion.username": bundle.fusion.username,
        "aidp.catalog": paths.catalog,
        "aidp.bronzeSchema": paths.bronze_schema,
        "aidp.silverSchema": paths.silver_schema,
        "aidp.goldSchema": paths.gold_schema,
        "plugin_version": plugin_version,
    }


__all__ = [
    "compute_output_schema_hash",
    "compute_content_pack_plan_hash",
]


# ---------------------------------------------------------------------------
# Content-pack plan-hash
# ---------------------------------------------------------------------------
#
# Mixes everything that can semantically change "what this node would
# produce":
#
#   * pack identity (pack_id, pack_version)
#   * node identity (node_id, node_version, node_implementation_type)
#   * rendered SQL hash (template + profile-resolved params; from
#     orchestrator/sql_renderer.py::compute_rendered_sql_hash)
#   * declared output_schema hash (this module)
#   * profile_hash (from schema/tenant_profile.py::compute_profile_hash)
#   * tenant fingerprint + bronze-schema fingerprint (from profile)
#   * refresh strategy + natural key + partition columns + watermark spec
#
# Drift on any of these blocks the resume (AIDPF-4040).


def compute_output_schema_hash(node: "NodeYaml") -> str:  # noqa: F821 — forward
    """Deterministic hash of a node's declared ``outputSchema``.

    Canonical serialisation: ``name|type|nullable|pii`` per column in
    declared order (column reorder shifts the hash — declared order is
    significant; downstream consumers rely on it). Joined with newlines,
    sha256.

    Computed pre-dispatch from ``node.outputSchema`` — NOT from a
    Spark probe. The materialised-schema assertion in Step 11
    (``_assert_materialized_matches_declared``) is a separate post-
    execution check that catches SQL-induced drift; this hash catches
    YAML-author-induced drift.

    Cosmetic YAML whitespace doesn't shift the hash; semantic flips
    (add a column / change a type / flip nullable / change pii level)
    do.

    Args:
        node: the validated NodeYaml whose outputSchema to hash.

    Returns:
        Hex sha256 string.
    """
    columns = []
    for col in node.output_schema.columns:
        columns.append(
            f"{col.name}|{col.type}|{int(col.nullable)}|{col.pii}"
        )
    canonical = "\n".join(columns)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_content_pack_plan_hash(
    *,
    pack: "ResolvedPack",  # noqa: F821 — forward
    node: "NodeYaml",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    rendered_sql_hash: str,
    output_schema_hash: str,
    profile_hash: str,
) -> str:
    """Assemble the content-pack plan-hash from its constituent inputs.

    The content-pack plan-hash includes every input that can semantically
    change "what this node would produce":

    * Pack identity (id + version) — catches "we re-resolved to a
      different pack".
    * Node identity (id + implementation type + declared version).
    * Refresh strategy + natural key + partition columns + watermark
      spec — catches a node-yaml edit that didn't change the SQL but
      changed the refresh shape.
    * Rendered SQL hash — catches a SQL template edit (or a profile-
      value flip the template substituted).
    * Output schema hash — catches a YAML outputSchema edit.
    * Profile hash — catches a variation-point pick flip or any
      tenant-profile value change.
    * Tenant fingerprint + bronze schema fingerprint — catches "tenant
      identity or bronze schema drifted since the last successful run".

    Args:
        pack: assembled ResolvedPack (provides pack.id + pack.version).
        node: validated NodeYaml.
        profile: validated TenantProfile (provides tenant fingerprint +
            bronze schema fingerprint).
        rendered_sql_hash: from ``compute_rendered_sql_hash(rendered)``.
        output_schema_hash: from :func:`compute_output_schema_hash`.
        profile_hash: from ``compute_profile_hash(profile)``.

    Returns:
        Hex sha256 string. Comparing this against the prior successful
        state row's ``plan_hash`` is the AIDPF-4040 resume drift gate.
    """
    inc = node.refresh.incremental
    payload = {
        # Pack identity.
        "pack_id": pack.pack.id,
        "pack_version": pack.pack.version,
        # Node identity.
        "node_id": node.id,
        "node_implementation_type": node.implementation.type,
        # Refresh shape.
        "refresh_seed_strategy": node.refresh.seed.strategy,
        "refresh_incremental_strategy": inc.strategy if inc else None,
        "natural_key": list(inc.natural_key) if inc and inc.natural_key else [],
        "partition_columns": list(inc.partition_columns) if inc and inc.partition_columns else [],
        "watermark_source": inc.watermark.source if inc and inc.watermark else None,
        "watermark_column": inc.watermark.column if inc and inc.watermark else None,
        # Hashes (computed by other modules; mixed in by reference here).
        "rendered_sql_hash": rendered_sql_hash,
        "output_schema_hash": output_schema_hash,
        "profile_hash": profile_hash,
        # Identity fingerprints.
        "tenant": profile.tenant,
        "bronze_schema_fingerprint": profile.bronze_schema_fingerprint,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()
