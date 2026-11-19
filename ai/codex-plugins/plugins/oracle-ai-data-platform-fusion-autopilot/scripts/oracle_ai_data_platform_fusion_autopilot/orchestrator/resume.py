"""Resume helpers — pure functions used when ``--resume`` is set.

Three pure functions used by ``orchestrator.run`` when ``--resume`` is set:

  * :func:`reconstruct_resume_scope` — derive the ``datasets`` / ``layers``
    filter from the stored ``plan_snapshot`` so bare ``--resume <run_id>``
    (no CLI filters) re-resolves the original scope.
  * :func:`render_drift_error` — produce the operator-facing
    ``ResumeBundleMismatchError`` message: identity diff first, dataset
    diff second, hash echo last.
  * :func:`compute_reattempt_extra_deps` — augment external-dep preflight
    so reattempted downstream nodes catch a manually-dropped upstream
    table BEFORE dispatch (clean exit-2, not a mid-flight crash).

These live in a dedicated module to keep ``orchestrator/__init__.py``
focused on the main dispatch loop. They are pure (no Spark, no I/O) so
unit-testable without fixtures.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Mapping, Sequence
    from typing import Any

    from .runtime import ExternalDep


def reconstruct_resume_scope(
    plan_snapshot: str,
) -> tuple[list[str], list[str]]:
    """Parse ``plan_snapshot`` JSON and return
    ``(datasets, layers)`` reproducing the original run's scope.

    Both lists are deduplicated and sorted for determinism. Callers
    only consult them when the CLI didn't supply explicit
    ``--datasets`` / ``--layers``; an explicit filter wins (and gets
    checked by the hash compare for divergence).

    Raises:
        ResumeRunNotResumableError: if the snapshot is unparseable.
            (``read_resumable_state`` should have already rejected
            this case; defense-in-depth here so a corrupt snapshot
            doesn't crash with an opaque JSON error.)
    """
    from .errors import ResumeRunNotResumableError

    try:
        snapshot = json.loads(plan_snapshot)
    except (ValueError, TypeError) as exc:  # pragma: no cover
        raise ResumeRunNotResumableError(
            f"plan_snapshot is not valid JSON: {exc!r}. Re-run from scratch."
        ) from exc

    nodes = snapshot.get("nodes", [])
    datasets = sorted({n["dataset_id"] for n in nodes})
    layers = sorted({n["layer"] for n in nodes})
    return datasets, layers


# The content-pack runner's per-node atomic-commit model handles resume
# natively: each execute_node is a full preflight → render → drift → execute →
# quality → state commit cycle, and is the resume unit.


def render_drift_error(
    stored_snapshot_json: str,
    current_identity: "Mapping[str, str]",
    current_node_tuples: "Sequence[Mapping[str, str]]",
    stored_hash: str,
    current_hash: str,
    run_id: str,
) -> str:
    """Build the operator-facing ``ResumeBundleMismatchError`` message.

    Three sections, in order:
      1. **Identity diff** — one line per changed field, named explicitly
         (``aidp.silverSchema: "silver_v1" → "silver_v2"``).
      2. **Dataset diff** — added / removed dataset_ids, and for nodes
         present on both sides with diverging
         ``(layer, mode, effective_schema)``, name the per-field delta.
      3. **Hash echo** — ``stored_hash`` + ``current_hash`` truncated to
         12 hex chars for readability (collision-resistant enough for
         operator correlation; full hashes are in the state-table row).

    All three render even if a given section finds zero differences —
    catches the (unlikely) case where the canonical-payload code path
    differs from the hash compute path (would manifest as "hash mismatch
    but nothing diffs"). In that case we still print the hashes so the
    operator has something to file a bug with.
    """
    snapshot = json.loads(stored_snapshot_json)
    stored_identity = snapshot.get("identity", {})
    stored_nodes = snapshot.get("nodes", [])

    lines: list[str] = [
        f"--resume: bundle drift detected against run_id={run_id!r}. "
        f"Either re-run from scratch with the current bundle, or revert "
        f"the bundle to match the original run.",
        "",
    ]

    # 1. Identity diff.
    identity_changes: list[str] = []
    all_identity_keys = sorted(set(stored_identity) | set(current_identity))
    for key in all_identity_keys:
        old = stored_identity.get(key)
        new = current_identity.get(key)
        if old != new:
            identity_changes.append(f"  {key}: {old!r} → {new!r}")
    if identity_changes:
        lines.append("Identity changes:")
        lines.extend(identity_changes)
        lines.append("")

    # 2. Dataset diff.
    stored_by_id = {n["dataset_id"]: n for n in stored_nodes}
    current_by_id = {n["dataset_id"]: n for n in current_node_tuples}
    added = sorted(set(current_by_id) - set(stored_by_id))
    removed = sorted(set(stored_by_id) - set(current_by_id))
    common = sorted(set(stored_by_id) & set(current_by_id))

    per_dataset_changes: list[str] = []
    for ds_id in common:
        s = stored_by_id[ds_id]
        c = current_by_id[ds_id]
        deltas: list[str] = []
        for field in ("layer", "mode", "effective_schema"):
            if s.get(field) != c.get(field):
                deltas.append(f"{field}: {s.get(field)!r} → {c.get(field)!r}")
        if deltas:
            per_dataset_changes.append(
                f"  {ds_id}: " + ", ".join(deltas)
            )

    if added or removed or per_dataset_changes:
        lines.append("Dataset changes:")
        if added:
            lines.append(f"  added:   {added}")
        if removed:
            lines.append(f"  removed: {removed}")
        if per_dataset_changes:
            lines.append("  per-dataset deltas:")
            lines.extend(per_dataset_changes)
        lines.append("")

    # 3. Hash echo.
    lines.append(
        f"Hashes: stored={stored_hash[:12]}…  current={current_hash[:12]}…  "
        f"(full hashes in fusion_autopilot_state.plan_hash)"
    )

    return "\n".join(lines)


def check_identity_drift(
    plan_snapshot_json: str,
    *,
    bundle: "Any",
    paths: "Any",
    plugin_version: str,
    run_id: str,
) -> None:
    """Raise ``ResumeBundleMismatchError`` if the current bundle's
    8-field execution identity diverges from the snapshot's stored
    identity.

    Runs BEFORE any preflight / BICC call / password unwrap so a
    drifted ``fusion.serviceUrl`` / ``fusion.username`` never sends
    credentials to the wrong endpoint, and a drifted
    ``aidp.{catalog,bronzeSchema}`` is detected before downstream
    state-write side effects on the drifted path.

    Identity-only check — does NOT compare plan shape or per-node
    ``effective_schema`` (those need preflight to compute and can't
    be checked here). The full hash compare after preflight catches
    shape/schema drift; this one catches identity drift early.

    Pure: no spark, no I/O. Parses ``plan_snapshot_json``, builds the
    current identity dict via ``_identity_dict`` from ``plan_hash``,
    diffs them, raises on any difference with an identity-only diff
    rendered by ``render_drift_error`` (with empty current/stored
    node tuples so only the identity section shows).
    """
    from .errors import ResumeBundleMismatchError, ResumeRunNotResumableError
    from .plan_hash import _identity_dict

    try:
        snapshot = json.loads(plan_snapshot_json)
    except (ValueError, TypeError) as exc:
        # Unparseable snapshot is a non-resumable condition. This gate runs
        # BEFORE any credential unwrap / BICC call, so it MUST fail closed —
        # returning here would silently skip the identity-drift check and let
        # a resume proceed (e.g. send credentials to a drifted endpoint).
        # ``read_content_pack_resumable_state`` does NOT validate snapshot
        # parseability, so a corrupt-but-non-NULL snapshot reaches here.
        raise ResumeRunNotResumableError(
            f"plan_snapshot is not valid JSON: {exc!r}. Cannot verify "
            "identity drift before resume — re-run from scratch."
        ) from exc

    stored_identity = snapshot.get("identity", {})
    current_identity = _identity_dict(bundle, paths, plugin_version)
    if stored_identity == current_identity:
        return

    # Identity-only diff — pass empty node lists so the dataset-diff
    # section renders nothing, and a placeholder for the hashes
    # (full hash isn't computed yet — preflight would be needed).
    msg = render_drift_error(
        stored_snapshot_json=plan_snapshot_json,
        current_identity=current_identity,
        current_node_tuples=snapshot.get("nodes", []),  # same as stored
        stored_hash="<identity-only check, full hash not computed>",
        current_hash="<identity-only check, full hash not computed>",
        run_id=run_id,
    )
    raise ResumeBundleMismatchError(msg)


__all__ = [
    "reconstruct_resume_scope",
    "render_drift_error",
    "check_identity_drift",
]
