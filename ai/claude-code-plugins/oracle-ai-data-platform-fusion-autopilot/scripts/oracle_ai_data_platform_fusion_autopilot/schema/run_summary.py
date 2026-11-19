"""Cross-boundary RunStep + RunSummary dataclasses.

The orchestrator produces these objects cluster-side; the CLI renderer
consumes them laptop-side. Under the `--inline` execution surface both sides
live in the same Python process. Under the REST-dispatch surface
the orchestrator emits a JSON marker that the dispatch package deserializes
back into a ``RunSummary`` for the same renderer to format.

The dispatch package cannot import ``orchestrator/*`` because that pulls
extractors, dimensions, transforms, and the full registry into ``sys.modules``.
So the dataclass definitions live here, in the neutral ``schema/`` namespace;
``orchestrator/runtime.py`` re-exports them for back-compat so every existing
in-package import path keeps working.

Identity is preserved:
``orchestrator.runtime.RunStep is schema.run_summary.RunStep``
``orchestrator.runtime.RunSummary is schema.run_summary.RunSummary``

Spec-typed factory classmethods (``RunStep.success`` /
``.failed`` / ``.skipped_cascade`` / ``.skipped_aborted`` / ``.deferred`` /
``.resumed_skip``) were deleted along with the v1 execution path. The live
content-pack dispatcher constructs ``RunStep`` directly with positional args
(see ``orchestrator/__init__.py:_run_content_pack_backend``); the dispatch
package deserializes via ``RunSummary.from_marker_dict`` (pure JSON →
dataclass; no factory, no spec, no engine import). ``RunStep.gate_failed``
survives because it takes no spec object and is still used by
``_dispatch_content_pack_run`` for AIDPF-207x medallion gate failures.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import Final, Literal
from uuid import uuid4


# Marker payload schema version. Bump when ``from_marker_dict`` /
# ``to_marker_dict`` gain or rename fields in a non-back-compat way.
MARKER_SCHEMA_VERSION: Final[int] = 1


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso_utc(dt: datetime | None) -> str | None:
    """Serialize a datetime to ISO 8601 with ``Z`` suffix. NULL-preserving."""
    if dt is None:
        return None
    # Normalize to UTC and emit with ``Z`` (not ``+00:00``) so the marker is
    # cross-platform-readable. Naïve datetimes are assumed UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_iso(value: str | None) -> datetime | None:
    """Inverse of :func:`_iso_utc`. NULL-preserving."""
    if value is None:
        return None
    # ``fromisoformat`` accepts ``+00:00`` but not bare ``Z`` until 3.11+.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


# ---------------------------------------------------------------------------
# PlanNode — neutral DTO for dry-run plan rendering
# ---------------------------------------------------------------------------
# Used by the dispatch package's dry-run path to package up plan rows without
# importing the engine. Carries ``dataset_id`` + ``layer`` directly; the
# renderer reads ``.layer`` without any spec/registry round-trip.


@dataclass(frozen=True)
class PlanNode:
    """Cross-boundary representation of one plan entry rendered by the
    CLI's dry-run path. Carries everything the table needs without depending
    on orchestrator-side spec dataclasses.
    """

    dataset_id: str
    layer: Literal["bronze", "silver", "gold"]
    status: Literal["eligible", "deferred", "skipped-filter"] = "eligible"
    reason: str | None = None


@dataclass(frozen=True)
class PrereqNode:
    """Cross-boundary representation of an extra-plan prerequisite — an
    in-plan consumer whose upstream is filtered out via ``--datasets`` /
    ``--layers``. Mirrors :class:`orchestrator.runtime.ExternalDep` but
    lives in the neutral schema namespace so the dispatch package can
    populate prereqs laptop-side under ``--dry-run`` without importing the
    engine.

    The engine-side ``--inline`` path still constructs ``ExternalDep``
    instances for ``_preflight_external_deps`` (which calls
    ``spark.catalog.tableExists(...)``). The two DTOs have identical
    shape; the dispatch dry-run path does not check existence — it just
    declares which tables WOULD be required.
    """

    dataset_id: str
    layer: Literal["bronze", "silver", "gold"]
    consumer: str
    table_path: str


# ---------------------------------------------------------------------------
# RunStep + RunSummary
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunStep:
    """One row of orchestrator telemetry.

    Mirrors the ``fusion_autopilot_state`` schema and can be constructed
    directly engine-side or via :meth:`RunSummary.from_marker_dict`
    dispatch-side.
    """

    run_id: str
    dataset_id: str
    layer: Literal["bronze", "silver", "gold"]
    mode: Literal["seed", "incremental"]
    status: Literal["success", "failed", "skipped", "deferred", "resumed_skipped"]
    row_count: int | None
    duration_seconds: float
    error_message: str | None
    watermark_used: datetime | None
    last_watermark: datetime | None = None
    skip_reason: Literal["cascade", "aborted", "resume-skip"] | None = None
    plan_hash: str | None = None
    plan_snapshot: str | None = None

    @classmethod
    def gate_failed(
        cls,
        *,
        run_id: str,
        mode: str,
        layer: str,
        gate_dataset_id: str,
        aidpf_code: str,
        error_message: str,
    ) -> "RunStep":
        """Synthetic ``RunStep`` for a medallion gate failure.

        Used by the top-level dispatcher when ``assert_bronze_readiness``
        (AIDPF-2071) or ``assert_fusion_pvo_compatibility`` (AIDPF-2072)
        raises. The dispatcher catches the gate error, appends one of
        these steps to the merged :class:`RunSummary`, and returns
        normally — the CLI translates ``summary.has_failures()`` to a
        non-zero exit code.

        Reserved ``__<name>__`` ``dataset_id`` convention identifies the
        synthetic step (e.g. ``__bronze_readiness_gate__`` /
        ``__fusion_pvo_drift_gate__``) so downstream filters can
        distinguish gate-failure steps from real node failures.
        """
        return cls(
            run_id=run_id,
            dataset_id=gate_dataset_id,
            layer=layer,  # type: ignore[arg-type]
            mode=mode,  # type: ignore[arg-type]
            status="failed",
            row_count=None,
            duration_seconds=0.0,
            error_message=f"[{aidpf_code}] {error_message}",
            watermark_used=None,
            last_watermark=None,
            plan_hash=None,
            plan_snapshot=None,
        )

    # ---------------------- Marker (de)serialization ----------------------

    def to_marker_dict(self) -> dict:
        """Serialize to the marker-payload shape. Used by the run-cell that
        the dispatch package's notebook builder generates."""
        return {
            "run_id": self.run_id,
            "dataset_id": self.dataset_id,
            "layer": self.layer,
            "mode": self.mode,
            "status": self.status,
            "row_count": self.row_count,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "watermark_used": _iso_utc(self.watermark_used),
            "last_watermark": _iso_utc(self.last_watermark),
            "skip_reason": self.skip_reason,
            "plan_hash": self.plan_hash,
            "plan_snapshot": self.plan_snapshot,
        }

    @classmethod
    def from_marker_dict(cls, payload: dict) -> "RunStep":
        """Inverse of :meth:`to_marker_dict`. Raises ``ValueError`` if any
        required field is missing."""
        try:
            return cls(
                run_id=payload["run_id"],
                dataset_id=payload["dataset_id"],
                layer=payload["layer"],
                mode=payload["mode"],
                status=payload["status"],
                row_count=payload["row_count"],
                duration_seconds=payload["duration_seconds"],
                error_message=payload["error_message"],
                watermark_used=_parse_iso(payload.get("watermark_used")),
                last_watermark=_parse_iso(payload.get("last_watermark")),
                skip_reason=payload.get("skip_reason"),
                plan_hash=payload.get("plan_hash"),
                plan_snapshot=payload.get("plan_snapshot"),
            )
        except KeyError as exc:
            raise ValueError(
                f"RunStep.from_marker_dict: missing required field {exc.args[0]!r} "
                f"in payload (keys present: {sorted(payload.keys())!r})"
            ) from exc


@dataclass(frozen=True)
class RunSummary:
    """Aggregate result of one ``orchestrator.run(...)`` invocation."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    bundle_project: str
    mode: str
    steps: tuple[RunStep, ...]
    plan: tuple[object, ...] | None = None
    prereqs: tuple[object, ...] | None = None
    recommendations: tuple[str, ...] = ()
    diagnostics: tuple[dict, ...] = ()
    """Structured per-node failure payloads (JSON-able dicts) the laptop
    dispatcher persists under ``.aidp/diagnostics/<run_id>/`` for skill
    consumption — e.g. AIDPF-4071 source-column-missing diagnostics."""

    @property
    def succeeded(self) -> int:
        return sum(1 for s in self.steps if s.status == "success")

    @property
    def failed(self) -> int:
        return sum(1 for s in self.steps if s.status == "failed")

    @property
    def skipped(self) -> int:
        return sum(1 for s in self.steps if s.status == "skipped")

    @property
    def deferred(self) -> int:
        return sum(1 for s in self.steps if s.status == "deferred")

    @property
    def resumed_skipped(self) -> int:
        return sum(1 for s in self.steps if s.status == "resumed_skipped")

    @property
    def total_duration_seconds(self) -> float:
        return sum(s.duration_seconds for s in self.steps)

    @classmethod
    def empty(
        cls,
        bundle_project: str,
        mode: str,
        *,
        plan: tuple[object, ...] | None = None,
        prereqs: tuple[object, ...] | None = None,
    ) -> "RunSummary":
        """Construct a zero-step RunSummary for paths that didn't dispatch
        (empty bundle or ``dry_run=True``)."""
        now = _utc_now()
        return cls(
            run_id=f"empty-{uuid4()}",
            started_at=now,
            finished_at=now,
            bundle_project=bundle_project,
            mode=mode,
            steps=(),
            plan=plan,
            prereqs=prereqs,
        )

    # ---------------------- Marker (de)serialization ----------------------

    def to_marker_dict(self) -> dict:
        """Serialize the run summary to the marker JSON shape. The dispatch
        package's notebook builder embeds this between
        ``AIDP_LIVE_TEST_RESULT_BEGIN`` and ``AIDP_LIVE_TEST_RESULT_END`` so
        the laptop-side dispatcher can deserialize via
        :meth:`from_marker_dict`.

        ``plan`` and ``prereqs`` are omitted from the marker payload — they
        carry engine spec objects on the in-process ``--inline`` path and
        would not round-trip cleanly through JSON. The dispatch dry-run path
        resolves the plan laptop-side instead (see plan §Step 8c).
        """
        return {
            "schema_version": MARKER_SCHEMA_VERSION,
            "run_id": self.run_id,
            "started_at": _iso_utc(self.started_at),
            "finished_at": _iso_utc(self.finished_at),
            "bundle_project": self.bundle_project,
            "mode": self.mode,
            "recommendations": list(self.recommendations),
            "steps": [step.to_marker_dict() for step in self.steps],
            "diagnostics": [dict(d) for d in self.diagnostics],
        }

    @classmethod
    def from_marker_dict(cls, payload: dict) -> "RunSummary":
        """Inverse of :meth:`to_marker_dict`. Validates ``schema_version``."""
        version = payload.get("schema_version")
        if version != MARKER_SCHEMA_VERSION:
            raise ValueError(
                f"RunSummary.from_marker_dict: unsupported schema_version "
                f"{version!r} (this build expects {MARKER_SCHEMA_VERSION!r}). "
                "The notebook that produced this marker was built from a "
                "different plugin version — rebuild the wheel and re-dispatch."
            )
        try:
            started_at = _parse_iso(payload["started_at"])
            finished_at = _parse_iso(payload["finished_at"])
        except KeyError as exc:
            raise ValueError(
                f"RunSummary.from_marker_dict: missing required field "
                f"{exc.args[0]!r} in payload"
            ) from exc
        assert started_at is not None
        assert finished_at is not None
        return cls(
            run_id=payload["run_id"],
            started_at=started_at,
            finished_at=finished_at,
            bundle_project=payload["bundle_project"],
            mode=payload["mode"],
            steps=tuple(RunStep.from_marker_dict(s) for s in payload["steps"]),
            plan=None,
            prereqs=None,
            recommendations=tuple(payload.get("recommendations", ())),
            diagnostics=tuple(payload.get("diagnostics", ())),
        )


__all__ = [
    "MARKER_SCHEMA_VERSION",
    "PlanNode",
    "PrereqNode",
    "RunStep",
    "RunSummary",
    "_utc_now",
]
