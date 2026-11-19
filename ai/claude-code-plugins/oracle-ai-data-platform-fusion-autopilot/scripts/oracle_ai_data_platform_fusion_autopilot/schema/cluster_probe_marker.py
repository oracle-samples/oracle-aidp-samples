"""Cluster-side bootstrap probe marker schema.

The cluster-side notebook the laptop dispatches emits a base64-wrapped
JSON marker carrying the variation-phase probe + walker results back
to the laptop. The laptop unwraps the envelope, validates the inner
marker, and proceeds with multi-match resolution + filesystem writes.

Two models, NOT one:

* :class:`ClusterProbeEnvelope` — outer envelope with an ``ok`` flag
  that discriminates between success (``marker`` set) and the
  in-cell ``try``/``except`` error path (``error_type`` /
  ``error_message`` / ``traceback`` set).
* :class:`ClusterProbeMarker` — inner happy-path payload: observed
  bronze schema + bronze fingerprint + per-variation-point walker
  outcomes + the cluster's wall-clock at dispatch time.

The two-model split exists because the cluster cell catches its own
exceptions and emits a single marker line either way (Jupyter cell N
exceptions cannot be caught by cell N+1). The envelope discriminates
the laptop-side branching.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .bronze_fingerprint import ColumnInfo


# ---------------------------------------------------------------------------
# Wire-format mirrors for canonical dataclasses
# ---------------------------------------------------------------------------
#
# ``ColumnInfo`` (in ``bronze_fingerprint``) and ``CandidateAttempt`` (in
# ``commands.variation_resolver``) are plain dataclasses — they pre-date
# this feature and stay as-is. Pydantic models below mirror their fields
# so the marker round-trips cleanly through JSON without forcing a
# wholesale dataclass→model migration on the canonical types. Conversion
# helpers (``from_column_info`` / ``to_column_info``) live with the
# mirror.


class ColumnInfoMarker(BaseModel):
    """Pydantic wire-format mirror of
    :class:`schema.bronze_fingerprint.ColumnInfo`. Same three fields,
    same semantics — only the serialisation layer differs."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    type: str
    nullable: bool = True

    @classmethod
    def from_column_info(cls, ci: ColumnInfo) -> "ColumnInfoMarker":
        return cls(name=ci.name, type=ci.type, nullable=ci.nullable)

    def to_column_info(self) -> ColumnInfo:
        return ColumnInfo(name=self.name, type=self.type, nullable=self.nullable)


class CandidateAttemptMarker(BaseModel):
    """Wire-format mirror of
    :class:`commands.variation_resolver.CandidateAttempt`. Carries a
    failed candidate's id + the walker's reason for skipping it.
    Bootstrap uses this list when writing AIDPF-2010/2011 artifacts."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    candidate: str
    outcome: Literal["column_not_found", "detect_clause_failed"]
    detail: str | None = None


# ---------------------------------------------------------------------------
# Walker outcome per variation point
# ---------------------------------------------------------------------------


class WalkerOutcomeMarker(BaseModel):
    """One variation point's walker result, in a wire-format that
    preserves the same outcome shapes the laptop-side
    :mod:`commands.variation_resolver` exposes
    (:class:`AutoResolved` / :class:`MultiMatch` / :class:`NoMatch`).

    The discriminator is ``outcome``:

    * ``auto_resolved`` → ``chosen`` set; ``matched`` empty;
      ``candidates_tried`` empty.
    * ``multi_match`` → ``chosen`` ``None``; ``matched`` is the
      priority-ordered list of matching candidate ids; ``candidates_tried``
      empty.
    * ``no_match`` → ``chosen`` ``None``; ``matched`` empty;
      ``candidates_tried`` carries one entry per attempted candidate
      with its skip reason.

    The laptop-side resolver translates back to the dataclass outcome
    types after envelope validation.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    """Variation point name as declared in ``pack.yaml``
    (e.g. ``invoice_currency_code``)."""

    kind: Literal["columnAliases", "semanticVariants"]
    """Which pack-level map the variation point lives under."""

    outcome: Literal["auto_resolved", "multi_match", "no_match"]
    """Discriminator for the three walker outcome shapes."""

    chosen: str | None = None
    """Set iff ``outcome == "auto_resolved"`` — the chosen candidate id."""

    matched: list[str] = Field(default_factory=list)
    """Priority-ordered list of matching candidate ids — non-empty iff
    ``outcome == "multi_match"``."""

    candidates_tried: list[CandidateAttemptMarker] = Field(
        default_factory=list, alias="candidatesTried"
    )
    """Per-candidate skip reasons — non-empty iff
    ``outcome == "no_match"``. The laptop writes this list into the
    AIDPF-2010 / AIDPF-2011 diagnostic artifact when the VP is
    ``required: true``."""

    @model_validator(mode="after")
    def _outcome_shape(self) -> "WalkerOutcomeMarker":
        if self.outcome == "auto_resolved":
            if self.chosen is None:
                raise ValueError(
                    "outcome=auto_resolved requires `chosen` to be set"
                )
            if self.matched or self.candidates_tried:
                raise ValueError(
                    "outcome=auto_resolved must not populate `matched` "
                    "or `candidates_tried`"
                )
        elif self.outcome == "multi_match":
            if len(self.matched) < 2:
                raise ValueError(
                    "outcome=multi_match requires at least 2 entries in "
                    "`matched`"
                )
            if self.chosen is not None or self.candidates_tried:
                raise ValueError(
                    "outcome=multi_match must not populate `chosen` or "
                    "`candidates_tried`"
                )
        else:  # no_match
            if not self.candidates_tried:
                raise ValueError(
                    "outcome=no_match requires `candidates_tried` "
                    "(at least one attempt)"
                )
            if self.chosen is not None or self.matched:
                raise ValueError(
                    "outcome=no_match must not populate `chosen` or "
                    "`matched`"
                )
        return self


# ---------------------------------------------------------------------------
# Inner marker — what the cluster emits on success
# ---------------------------------------------------------------------------


class ClusterProbeMarker(BaseModel):
    """Inner happy-path payload — the cluster-side cell's success output.

    The laptop receives this nested inside :class:`ClusterProbeEnvelope`;
    direct construction is rare outside tests.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    marker_version: Literal[1] = Field(default=1, alias="markerVersion")
    """Bootstrap-probe marker schema version. ``Literal[1]`` (not
    ``int = 1``) is what makes a future cluster emitting
    ``markerVersion: 2`` raise a Pydantic validation error on the
    laptop instead of silently coercing — same pattern as
    :class:`schema.diagnostic_artifact.DiagnosticArtifactBase`."""

    tenant: str
    """Tenant identifier — usually the Fusion pod short name."""

    bronze_fingerprint: str = Field(alias="bronzeFingerprint")
    """``sha256:<hex>`` fingerprint computed cluster-side via
    :func:`schema.bronze_fingerprint.compute_bronze_fingerprint`.
    The laptop pins this into ``profile.bronzeSchemaFingerprint`` after
    multi-match resolution succeeds."""

    observed_schema: dict[str, list[ColumnInfoMarker]] = Field(
        alias="observedSchema"
    )
    """``{dataset_id: [ColumnInfoMarker, ...]}`` — the cluster's
    DESCRIBE TABLE observation. The laptop converts back to
    :class:`ColumnInfo` for the resolver + snapshot writes."""

    walker_results: list[WalkerOutcomeMarker] = Field(alias="walkerResults")
    """One entry per declared variation point. Order: ``columnAliases``
    first (pack-yaml order), then ``semanticVariants`` (pack-yaml order)."""

    dispatched_at: datetime = Field(alias="dispatchedAt")
    """Cluster-side wall-clock when the marker was emitted. Advisory
    (operator audit); not load-bearing — the laptop uses its own
    ``profile.pinned_at`` timestamp for the SOX trail."""


# ---------------------------------------------------------------------------
# Outer envelope — discriminates success vs in-cell error
# ---------------------------------------------------------------------------


class ClusterProbeEnvelope(BaseModel):
    """Outer envelope the cluster cell emits inside the
    ``AIDP_BOOTSTRAP_PROBE_MARKER_BEGIN ... END`` delimiters.

    Wraps either a successful :class:`ClusterProbeMarker` (``ok=True``)
    or the in-cell ``try``/``except`` failure payload (``ok=False`` —
    ``error_type`` / ``error_message`` / ``traceback`` set). The laptop
    unwraps via :meth:`model_validate` and branches on ``ok``.

    Why two models, not one: the cluster cell catches its own exceptions
    and emits a single marker line either way. The envelope is the laptop's
    branching seam.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ok: bool
    """``True`` ⇒ ``marker`` carries the validated payload.
    ``False`` ⇒ the error fields below are populated."""

    marker: ClusterProbeMarker | None = None
    """Set iff ``ok=True``. Nested Pydantic validation fires on
    :class:`ClusterProbeMarker`'s ``markerVersion`` ``Literal[1]`` etc."""

    error_type: str | None = Field(default=None, alias="errorType")
    """Set iff ``ok=False``. ``type(exc).__name__`` from the cluster
    cell's ``except`` block."""

    error_message: str | None = Field(default=None, alias="errorMessage")
    """Set iff ``ok=False``. ``str(exc)[:4000]`` from the cluster cell."""

    traceback: str | None = None
    """Set iff ``ok=False``. ``traceback.format_exc()[:8000]`` —
    truncated to fit the marker envelope budget."""

    @model_validator(mode="after")
    def _consistency(self) -> "ClusterProbeEnvelope":
        if self.ok:
            if self.marker is None:
                raise ValueError("ok=True requires `marker` payload")
            if self.error_type is not None or self.error_message is not None:
                raise ValueError(
                    "ok=True must not populate `errorType` / `errorMessage`"
                )
        else:
            if self.marker is not None:
                raise ValueError("ok=False must not populate `marker`")
            if self.error_type is None:
                raise ValueError(
                    "ok=False requires `errorType` (the cluster cell's "
                    "exception class name)"
                )
        return self


__all__ = [
    "CandidateAttemptMarker",
    "ClusterProbeEnvelope",
    "ClusterProbeMarker",
    "ColumnInfoMarker",
    "WalkerOutcomeMarker",
]
