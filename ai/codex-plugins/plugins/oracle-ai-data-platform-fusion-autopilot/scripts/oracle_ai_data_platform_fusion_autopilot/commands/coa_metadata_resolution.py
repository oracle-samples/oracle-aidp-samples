"""COA metadata resolution — READ-ONLY slice (P1): derive candidate
``chartOfAccounts.byChart`` arms from Fusion key-flexfield segment qualifiers.

The multi-chart COA gate (AIDPF-2018/2017) fails closed because *which*
``CodeCombinationSegment<N>`` carries balancing / cost-center / natural-account
is per-chart tenant configuration that column existence cannot prove. Fusion's
own flexfield setup DOES know: each segment carries a qualifier
(``GL_BALANCING`` / ``FA_COST_CTR`` / ``GL_ACCOUNT``). This module reads that
metadata and derives candidate arms — and NOTHING more in P1: verification,
persistence, the ladder merge, and bootstrap integration are the P2 slice of
this same module (round-8: the ``coa metadata-probe`` smoke must exercise the
exact derivation production later uses, so the pure core ships here and the
probe calls it directly).

Structure mirrors ``coa_advisory.py``: live-established constants → frozen
dataclasses → PURE core (unit-tested with literals) → fail-soft I/O shell
(every failure a labelled ``_Skip``, never an exception out).

The segment-qualifier REST resource is **UNVERIFIED** until the live smoke
(`coa metadata-probe`, FR-16) pins it: every built-in candidate must pass a
live SHAPE ASSERTION before it is used, and no candidate passing means a
labelled AIDPF-2021 skip — never a guess.

Path safety (NFR-9, design §6.2): the fetchers build URLs by raw
``base_url + path`` concatenation on a Basic-auth session, so a crafted
configurable path is a credential-exfiltration vector
(``https://fusion.example`` + ``@attacker.example/x`` parses with the Fusion
host as USERINFO and the attacker as the connection host). Two layers, both
fail-closed to AIDPF-2021 with ZERO network I/O: static relative-path
validation in :func:`resolve_resource_plan`, plus a post-construction
``(scheme, host, port)`` origin assertion against the normalized
``serviceUrl`` before every request.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Sequence
from urllib.parse import urlsplit

from ..schema.coa_roles import COA_SEGMENT_RE, coa_segment_column
from .coa_advisory import IDENTITY_ATTR, _env_creds, _Skip

AIDPF_2021_COA_METADATA_UNREACHABLE = "AIDPF-2021"
"""COA metadata resolution could not run: unverified/unsafe resource, missing
credentials/privilege, or budget exhaustion before the chart list completed.
(P2 registers the error-codes doc row; the constant ships with the probe so
its skips are labelled consistently from day one.)"""

# ── budgets (NFR-1, design §6.4) ─────────────────────────────────────────────
METADATA_BUDGET_S = 60.0
PER_REQUEST_TIMEOUT_S = 10
MAX_CHARTS_DERIVED = 200

# ── qualifier defaults (design §6.2; overridable per tenant/locale) ─────────
DEFAULT_QUALIFIER_VALUES: Mapping[str, str] = {
    "balancing": "GL_BALANCING",
    "costCenter": "FA_COST_CTR",
    "naturalAccount": "GL_ACCOUNT",
}
DEFAULT_QUALIFIER_ATTR = "SegmentQualifier"
DEFAULT_SEGMENT_COLUMN_ATTR = "ApplicationColumnName"

_ATTR_RE = re.compile(r"^[A-Za-z0-9_]+$")
"""Configurable attribute names are identifier-shaped (NFR-9 defense in depth
for the ``fields=`` / ``q=`` request params)."""

_CHART_ID_RE = re.compile(r"^[0-9]{1,18}$")
"""Mirrors ``node_preflight._COA_CHART_ID_RE`` (and ``sql_renderer``'s copy):
a derived key must be a valid numeric chart_of_accounts_id BEFORE it can
become a ``byChart`` key — a non-numeric key would only be caught at render
time otherwise."""

_SEGMENT_POSITION_RE = re.compile(r"^(?:SEGMENT|Segment|segment)(\d{1,2})$")

_API = "/fscmRestApi/resources/11.13.18.05"


# ── resource candidates (UNVERIFIED until the smoke pins one) ────────────────


@dataclass(frozen=True)
class ResourceCandidate:
    """One segment-qualifier resource hypothesis. ``verified=False`` entries
    MUST pass :func:`shape_assertion` on live data before any full fetch."""

    path: str
    qualifier_style: Literal["attribute", "flags"] = "attribute"
    identity_attr: str = IDENTITY_ATTR
    qualifier_attr: str = DEFAULT_QUALIFIER_ATTR
    segment_column_attr: str = DEFAULT_SEGMENT_COLUMN_ATTR
    qualifier_values: Mapping[str, str] = field(
        default_factory=lambda: dict(DEFAULT_QUALIFIER_VALUES)
    )
    flag_attrs: Mapping[str, str] = field(default_factory=dict)
    """flags-style only: role key → boolean attribute name."""
    verified: bool = False
    note: str = ""


SEGMENT_QUALIFIER_CANDIDATES: tuple[ResourceCandidate, ...] = (
    # UNVERIFIED — every entry must pass shape_assertion() on live data
    # before it is used (FR-16: the smoke records the pinned path into
    # bundle.yaml `fusion.coaMetadata.resourcePath`). Ordered most→least
    # likely; the ladder stops at the first candidate whose shape asserts.
    ResourceCandidate(
        path=f"{_API}/keyFlexfieldStructureInstances/GL%23GL%23Accounting%20Flexfield/child/segments",
        note="doc-plausible KFF structure-instance segments child (UNVERIFIED)",
    ),
    ResourceCandidate(
        path=f"{_API}/flexFndKfSegmentInstances",
        note="doc-plausible flattened segment-instances resource (UNVERIFIED)",
    ),
)


# ── path safety (NFR-9) ──────────────────────────────────────────────────────


def validate_resource_path(path: str, *, field_name: str) -> str | None:
    """Static relative-Fusion-path validation. Returns a reason string when
    INVALID (→ AIDPF-2021, zero network I/O), ``None`` when safe."""
    if not path:
        return f"{field_name} is empty"
    if not path.startswith("/"):
        return f"{field_name} must start with '/' (relative Fusion path)"
    if path.startswith("//"):
        return f"{field_name} must not be protocol-relative ('//…')"
    if "://" in path:
        return f"{field_name} must not carry a scheme"
    first_slashless = path.lstrip("/").split("/", 1)[0]
    if "@" in first_slashless:
        return (
            f"{field_name} must not carry userinfo ('@' in the first path "
            f"segment redirects the authenticated request off-origin)"
        )
    if any(ch in path for ch in ("?", "#", "\\")) or any(
        ch.isspace() for ch in path
    ) or any(ord(ch) < 0x20 for ch in path):
        return f"{field_name} must not contain '?', '#', whitespace or control characters"
    return None


def assert_same_origin(composed_url: str, service_url: str) -> str | None:
    """Post-construction origin pin: the composed URL's
    ``(scheme, host, port)`` must equal the normalized ``serviceUrl`` origin.
    Returns a reason when they differ (→ AIDPF-2021, request NOT issued)."""
    got = urlsplit(composed_url)
    want = urlsplit(service_url)
    if (got.scheme, got.hostname, got.port) != (
        want.scheme, want.hostname, want.port,
    ):
        return (
            f"composed URL origin ({got.scheme}://{got.hostname}:{got.port}) "
            f"differs from serviceUrl origin "
            f"({want.scheme}://{want.hostname}:{want.port}) — refusing to "
            f"send credentials off-origin"
        )
    return None


# ── config resolution (precedence: CLI > env > bundle > ladder) ─────────────

ENV_RESOURCE_PATH = "AIDPF_COA_METADATA_RESOURCE_PATH"


@dataclass(frozen=True)
class ResourcePlan:
    candidates: tuple[ResourceCandidate, ...]
    source: Literal["cli", "env", "bundle", "ladder"]


def resolve_resource_plan(
    coa_metadata: Any | None,
    *,
    cli_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> ResourcePlan:
    """The candidate ladder + static NFR-9 validation. An explicitly
    configured path SKIPS the ladder but still runs the live shape assertion
    (a typo must fail loudly, not silently return an empty page). Raises
    :class:`_Skip` (→ AIDPF-2021) on any statically-invalid configuration —
    before any I/O."""
    env = os.environ if env is None else env

    def _spec(name: str, default: str) -> str:
        value = getattr(coa_metadata, name, None) if coa_metadata else None
        return value or default

    identity_attr = _spec("identity_attr", IDENTITY_ATTR)
    qualifier_attr = _spec("qualifier_attr", DEFAULT_QUALIFIER_ATTR)
    segment_column_attr = _spec(
        "segment_column_attr", DEFAULT_SEGMENT_COLUMN_ATTR
    )
    for attr_field, value in (
        ("identityAttr", identity_attr),
        ("qualifierAttr", qualifier_attr),
        ("segmentColumnAttr", segment_column_attr),
    ):
        if not _ATTR_RE.match(value):
            raise _Skip(
                f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: {attr_field} "
                f"{value!r} is not identifier-shaped ([A-Za-z0-9_]+)"
            )
    qualifier_values = dict(DEFAULT_QUALIFIER_VALUES)
    qualifier_values.update(
        getattr(coa_metadata, "qualifier_values", None) or {}
    )

    explicit = (
        ("cli", cli_path)
        if cli_path
        else ("env", env.get(ENV_RESOURCE_PATH))
        if env.get(ENV_RESOURCE_PATH)
        else ("bundle", getattr(coa_metadata, "resource_path", None))
        if coa_metadata is not None
        and getattr(coa_metadata, "resource_path", None)
        else None
    )
    child_path = (
        getattr(coa_metadata, "segments_child_path", None)
        if coa_metadata is not None else None
    )
    if child_path:
        reason = validate_resource_path(
            child_path, field_name="segmentsChildPath"
        )
        if reason:
            raise _Skip(f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: {reason}")

    if explicit is not None:
        source, path = explicit
        assert path is not None
        reason = validate_resource_path(path, field_name="resourcePath")
        if reason:
            raise _Skip(f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: {reason}")
        full = path + child_path if child_path else path
        return ResourcePlan(
            candidates=(
                ResourceCandidate(
                    path=full,
                    identity_attr=identity_attr,
                    qualifier_attr=qualifier_attr,
                    segment_column_attr=segment_column_attr,
                    qualifier_values=qualifier_values,
                    note=f"explicitly configured ({source})",
                ),
            ),
            source=source,  # type: ignore[arg-type]
        )

    ladder = tuple(
        ResourceCandidate(
            path=c.path,
            qualifier_style=c.qualifier_style,
            identity_attr=identity_attr,
            qualifier_attr=qualifier_attr,
            segment_column_attr=segment_column_attr,
            qualifier_values=qualifier_values,
            flag_attrs=c.flag_attrs,
            note=c.note,
        )
        for c in SEGMENT_QUALIFIER_CANDIDATES
        if validate_resource_path(c.path, field_name="candidate") is None
    )
    return ResourcePlan(candidates=ladder, source="ladder")


# ── PURE derivation core (the code the smoke AND production share) ──────────


@dataclass(frozen=True)
class CandidateArm:
    chart_id: str
    balancing_segment: str
    cost_center_segment: str
    natural_account_segment: str

    def as_by_chart_block(self) -> dict[str, str]:
        return {
            "balancingSegment": self.balancing_segment,
            "costCenterSegment": self.cost_center_segment,
            "naturalAccountSegment": self.natural_account_segment,
        }


@dataclass(frozen=True)
class ArmReject:
    chart_id: str
    reason: Literal[
        "incomplete", "ambiguous", "invalid_segment",
        "duplicate_column", "invalid_chart_id",
    ]
    detail: str


def segment_column(app_col: str | None) -> str | None:
    """``"SEGMENT7"`` / ``"Segment7"`` / ``"CodeCombinationSegment7"`` →
    ``"CodeCombinationSegment7"``; anything else (incl. positions > 30) →
    ``None`` — the caller rejects the arm rather than inventing a column."""
    if not app_col:
        return None
    value = str(app_col).strip()
    if COA_SEGMENT_RE.match(value):
        # Normalise the canonical casing.
        position = int(re.sub(r"(?i)^CodeCombinationSegment", "", value))
        return coa_segment_column(position)
    m = _SEGMENT_POSITION_RE.match(value)
    if not m:
        return None
    return coa_segment_column(int(m.group(1)))


_ROLE_KEYS = ("balancing", "costCenter", "naturalAccount")


def arm_from_qualifiers(
    chart_id: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate: ResourceCandidate,
) -> CandidateArm | ArmReject:
    """Derive ONE chart's arm from its qualifier rows.

    Fail-soft per chart: a missing role → ``incomplete``; the same role
    qualified on two segments → ``ambiguous``; an unparseable/out-of-domain
    segment → ``invalid_segment``; and the AIDPF-2016 distinctness pre-check
    (three roles must bind three DISTINCT columns) → ``duplicate_column`` —
    auto-derivation can never introduce a 2016."""
    token_to_role = {
        str(candidate.qualifier_values.get(role, "")).upper(): role
        for role in _ROLE_KEYS
    }
    found: dict[str, str] = {}
    for row in rows:
        col = segment_column(row.get(candidate.segment_column_attr))
        matched_roles: list[str] = []
        if candidate.qualifier_style == "attribute":
            token = str(row.get(candidate.qualifier_attr) or "").upper()
            if token in token_to_role:
                matched_roles.append(token_to_role[token])
        else:  # flags style — strict truthiness (True or "Y"), never bool(str)
            for role in _ROLE_KEYS:
                flag_attr = candidate.flag_attrs.get(role)
                if not flag_attr:
                    continue
                raw = row.get(flag_attr)
                if raw is True or (
                    isinstance(raw, str) and raw.strip().upper() == "Y"
                ):
                    matched_roles.append(role)
        for role in matched_roles:
            if col is None:
                return ArmReject(
                    chart_id, "invalid_segment",
                    f"role {role!r} qualified on segment column "
                    f"{row.get(candidate.segment_column_attr)!r}, outside "
                    f"CodeCombinationSegment1–30",
                )
            if role in found and found[role] != col:
                return ArmReject(
                    chart_id, "ambiguous",
                    f"role {role!r} qualified on BOTH {found[role]!r} and "
                    f"{col!r}",
                )
            found[role] = col
    missing = [r for r in _ROLE_KEYS if r not in found]
    if missing:
        return ArmReject(
            chart_id, "incomplete",
            f"no segment qualified for role(s) {missing!r}",
        )
    columns = [found[r] for r in _ROLE_KEYS]
    if len({c.lower() for c in columns}) != len(columns):
        return ArmReject(
            chart_id, "duplicate_column",
            f"two roles bind the same physical column ({found!r}) — "
            f"AIDPF-2016 must never be introduced by auto-derivation",
        )
    return CandidateArm(
        chart_id=chart_id,
        balancing_segment=found["balancing"],
        cost_center_segment=found["costCenter"],
        natural_account_segment=found["naturalAccount"],
    )


def derive_arms(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate: ResourceCandidate,
) -> tuple[dict[str, CandidateArm], tuple[ArmReject, ...]]:
    """Group qualifier rows by chart identity and derive every chart's arm.

    Chart identity joins on the empirically-pinned attribute contract
    (``IDENTITY_ATTR`` — 41/41 on the Step-1 smoke) and every derived key is
    re-validated as numeric BEFORE it can become a ``byChart`` key."""
    by_chart: dict[str, list[Mapping[str, Any]]] = {}
    rejects: list[ArmReject] = []
    invalid_ids: set[str] = set()
    for row in rows:
        raw_id = row.get(candidate.identity_attr)
        if raw_id is None:
            continue
        chart_id = str(raw_id)
        if not _CHART_ID_RE.match(chart_id):
            if chart_id not in invalid_ids:
                invalid_ids.add(chart_id)
                rejects.append(ArmReject(
                    chart_id, "invalid_chart_id",
                    f"{candidate.identity_attr}={chart_id!r} is not a valid "
                    f"numeric chart_of_accounts_id",
                ))
            continue
        by_chart.setdefault(chart_id, []).append(row)
    arms: dict[str, CandidateArm] = {}
    for chart_id in sorted(by_chart):
        outcome = arm_from_qualifiers(
            chart_id, by_chart[chart_id], candidate=candidate,
        )
        if isinstance(outcome, CandidateArm):
            arms[chart_id] = outcome
        else:
            rejects.append(outcome)
    return arms, tuple(rejects)


# ── shape assertion (the smoke's honesty rule, design §6.1) ─────────────────


@dataclass(frozen=True)
class ShapeReport:
    ok: bool
    attribute_names_seen: tuple[str, ...]
    identity_attr_present: bool
    qualifier_style_detected: Literal["attribute", "flags", "none"]
    reason: str = ""


def shape_assertion(
    first_page: Sequence[Mapping[str, Any]],
    candidate: ResourceCandidate,
) -> ShapeReport:
    """A candidate is usable ONLY when its first page carries the identity
    attribute, a qualifier signal, and a segment-column attribute
    :func:`segment_column` can parse — a wrong path/typo must fail loudly,
    never silently return an empty derivation."""
    names = sorted({k for row in first_page for k in row})
    if not first_page:
        return ShapeReport(False, (), False, "none", "first page is empty")
    identity_present = any(
        candidate.identity_attr in row for row in first_page
    )
    style: Literal["attribute", "flags", "none"] = "none"
    if any(candidate.qualifier_attr in row for row in first_page):
        style = "attribute"
    elif candidate.flag_attrs and all(
        any(attr in row for row in first_page)
        for attr in candidate.flag_attrs.values()
    ):
        style = "flags"
    segment_parseable = any(
        segment_column(row.get(candidate.segment_column_attr)) is not None
        for row in first_page
    )
    ok = identity_present and style != "none" and segment_parseable
    reason = "" if ok else (
        f"shape assertion failed: identity_present={identity_present}, "
        f"qualifier_style={style}, segment_parseable={segment_parseable}"
    )
    return ShapeReport(ok, tuple(names), identity_present, style, reason)


# ── fail-soft I/O shell ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class QualifierFetch:
    """Outcome of one metadata fetch. ``skip_reason`` set → nothing usable
    (labelled AIDPF-2021 skip); otherwise ``rows`` came from ``candidate``
    whose shape asserted on live data."""

    rows: tuple[Mapping[str, Any], ...] = ()
    coverage: Literal["complete", "partial"] = "complete"
    candidate: ResourceCandidate | None = None
    shape: ShapeReport | None = None
    skip_reason: str | None = None
    candidates_tried: tuple[tuple[str, str], ...] = ()
    """(path, outcome) per candidate — the probe's per-candidate report."""


def fetch_segment_qualifiers(
    *,
    service_url: str,
    plan: ResourcePlan,
    session: Any | None = None,
    budget_s: float = METADATA_BUDGET_S,
) -> QualifierFetch:
    """Fetch qualifier rows from the first candidate whose shape asserts.

    Fail-soft: every failure mode degrades to a labelled skip
    (:class:`QualifierFetch` with ``skip_reason``), never an exception out.
    Origin-pinned (NFR-9): the composed URL's origin is asserted against
    ``serviceUrl`` BEFORE any request — a violating candidate records zero
    network calls."""
    from ..extractors.rest import DeadlineExceeded, fetch_paged

    tried: list[tuple[str, str]] = []
    try:
        if session is None:
            import requests

            user, pwd = _env_creds()
            session = requests.Session()
            session.auth = (user, pwd)

        deadline = time.monotonic() + budget_s
        base = service_url.rstrip("/")

        for candidate in plan.candidates:
            reason = validate_resource_path(
                candidate.path, field_name="resourcePath"
            ) or assert_same_origin(f"{base}{candidate.path}", service_url)
            if reason:
                tried.append((candidate.path, f"blocked: {reason}"))
                continue
            rows: list[Mapping[str, Any]] = []
            try:
                for item in fetch_paged(
                    session, base, candidate.path,
                    timeout=PER_REQUEST_TIMEOUT_S,
                    deadline=deadline,
                ):
                    rows.append(item)
                    if len(rows) >= MAX_CHARTS_DERIVED * 40:
                        break
            except DeadlineExceeded:
                if not rows:
                    tried.append((candidate.path, "deadline before first page"))
                    raise _Skip(
                        f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: time budget "
                        f"expired before the qualifier list completed"
                    ) from None
                shape = shape_assertion(rows[:25], candidate)
                if shape.ok:
                    tried.append((candidate.path, "partial (deadline)"))
                    return QualifierFetch(
                        rows=tuple(rows), coverage="partial",
                        candidate=candidate, shape=shape,
                        candidates_tried=tuple(tried),
                    )
                tried.append((candidate.path, f"skipped: {shape.reason}"))
                continue
            except Exception as exc:  # noqa: BLE001 — per-candidate fail-soft
                status = getattr(
                    getattr(exc, "response", None), "status_code", None
                )
                if status in (401, 403):
                    raise _Skip(
                        f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: Fusion REST "
                        f"returned {status} — the configured user lacks the "
                        f"GL REST privilege "
                        f"(FUN_GET_ENTERPRISE_STRUCTURES_REST_SERVICE_PRIV)"
                    ) from None
                tried.append(
                    (candidate.path, f"error: {type(exc).__name__}")
                )
                continue
            shape = shape_assertion(rows[:25], candidate)
            if not shape.ok:
                tried.append((candidate.path, f"skipped: {shape.reason}"))
                continue
            tried.append((candidate.path, "shape asserted"))
            return QualifierFetch(
                rows=tuple(rows), coverage="complete", candidate=candidate,
                shape=shape, candidates_tried=tuple(tried),
            )

        raise _Skip(
            f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: no segment-qualifier "
            f"resource passed the shape assertion "
            f"({len(plan.candidates)} candidate(s) tried) — pin one with "
            f"`coa metadata-probe` + `fusion.coaMetadata.resourcePath`"
        )
    except _Skip as skip:
        return QualifierFetch(
            skip_reason=skip.reason, candidates_tried=tuple(tried),
        )
    except Exception as exc:  # noqa: BLE001 — the shell NEVER raises out
        return QualifierFetch(
            skip_reason=(
                f"{AIDPF_2021_COA_METADATA_UNREACHABLE}: "
                f"{type(exc).__name__}: {str(exc)[:120]}"
            ),
            candidates_tried=tuple(tried),
        )


# ── the read-only probe (the P1 smoke's engine) ─────────────────────────────


def run_metadata_probe(
    *,
    service_url: str,
    coa_metadata: Any | None,
    cli_resource_path: str | None = None,
    chart: str | None = None,
    limit: int = 3,
    budget_s: float = METADATA_BUDGET_S,
    session: Any | None = None,
) -> dict:
    """Read-only smoke report (design §6.3) — writes NOTHING.

    Derivation happens through the SAME shared core production uses
    (:func:`derive_arms`; round-8 fixture invariant), so the smoke validates
    the exact interpretation that ships."""
    try:
        plan = resolve_resource_plan(coa_metadata, cli_path=cli_resource_path)
    except _Skip as skip:
        return {"ok": False, "skipReason": skip.reason, "candidates": []}

    fetch = fetch_segment_qualifiers(
        service_url=service_url, plan=plan, session=session,
        budget_s=budget_s,
    )
    report: dict = {
        "ok": fetch.skip_reason is None,
        "planSource": plan.source,
        "candidates": [
            {"path": path, "outcome": outcome}
            for path, outcome in fetch.candidates_tried
        ],
    }
    if fetch.skip_reason is not None:
        report["skipReason"] = fetch.skip_reason
        return report

    assert fetch.candidate is not None and fetch.shape is not None
    arms, rejects = derive_arms(fetch.rows, candidate=fetch.candidate)
    if chart is not None:
        arms = {k: v for k, v in arms.items() if k == chart}
    shown = dict(sorted(arms.items())[: max(limit, 0)])
    report.update({
        "resourcePath": fetch.candidate.path,
        "coverage": fetch.coverage,
        "identityAttr": fetch.candidate.identity_attr,
        "attribute_names_seen": list(fetch.shape.attribute_names_seen),
        "identity_attr_present": fetch.shape.identity_attr_present,
        "qualifier_style_detected": fetch.shape.qualifier_style_detected,
        "chartsDerived": len(arms),
        "chartsRejected": [
            {"chart": r.chart_id, "reason": r.reason, "detail": r.detail}
            for r in rejects
        ],
        "derivedArms": {
            chart_id: arm.as_by_chart_block()
            for chart_id, arm in shown.items()
        },
        "byChartYamlFragment": _by_chart_yaml(shown),
    })
    return report


def _by_chart_yaml(arms: Mapping[str, CandidateArm]) -> str:
    lines = ["byChart:"]
    for chart_id, arm in sorted(arms.items()):
        lines.append(f'  "{chart_id}":')
        for key, value in arm.as_by_chart_block().items():
            lines.append(f"    {key}: {value}")
    return "\n".join(lines) + ("\n" if arms else "")


# ═════════════════════════════════════════════════════════════════════════
# P2 surface — BICC row source, Tier-B verification, default selection.
# The FR-16 smoke (see docs/features/coa-mapping-auto-remediation/
# smoke-record.md) pinned the transport: this tenant class exposes the
# qualifier metadata through BICC PVOs, not transactional REST. The PVOs are
# read CLUSTER-SIDE (the `aidataplatform` Spark connector is cluster-only)
# inside the existing bootstrap probe dispatch — which also runs the Tier-B
# probes, collapsing D-4's laptop/cluster split. Everything below is PURE.
# ═════════════════════════════════════════════════════════════════════════

AIDPF_2022_COA_ARM_REJECTED = "AIDPF-2022"
"""A metadata-derived arm was REJECTED and not persisted (failed Tier-B, or
malformed at derivation). Warn-only detail accompanying AIDPF-2023."""

AIDPF_2023_COA_CHARTS_UNRESOLVED = "AIDPF-2023"
"""After metadata resolution, active in-scope charts remain unmapped —
verified arms ARE persisted (monotonic, FR-14a S2/S4); the phase exits
non-zero; the post-extraction AIDPF-2018 gate remains the enforcer."""

GL_KEY_FLEXFIELD_CODE = "GL#"
"""The Accounting Flexfield's KeyFlexfieldCode — the structure-instance
filter (smoke-pinned column on KeyFlexStructureInstancesBPVO)."""

#: The smoke-pinned PVO triplet (datastore names verified live via
#: /biacm/rest/meta/datastores on 2026-08-06; columns recorded in the smoke
#: record). Offering schema follows the gl_coa precedent ("Financial").
KFF_METADATA_PVOS: Mapping[str, str] = {
    "structure_instances": (
        "FscmTopModelAM.FinExtractAM.AnalyticsExtractServiceAM."
        "KeyFlexStructureInstancesBPVO"
    ),
    "segments": (
        "FscmTopModelAM.FinExtractAM.AnalyticsExtractServiceAM."
        "KeyFlexSegmentsBPVO"
    ),
    "labeled_segments": (
        "FscmTopModelAM.FinExtractAM.AnalyticsExtractServiceAM."
        "KeyFlexLabeledSegmentsPVO"
    ),
}
KFF_PVO_SCHEMA = "Financial"


def bicc_rows_to_qualifier_rows(
    structure_instances: Sequence[Mapping[str, Any]],
    segments: Sequence[Mapping[str, Any]],
    labeled_segments: Sequence[Mapping[str, Any]],
    *,
    key_flexfield_code: str = GL_KEY_FLEXFIELD_CODE,
) -> list[dict[str, Any]]:
    """Join the three KFF PVOs into the qualifier-row shape the P1 core
    consumes — ``{IDENTITY_ATTR, SegmentQualifier, ApplicationColumnName}``
    — so the round-8 invariant holds by construction: BICC-sourced rows go
    through the exact same ``derive_arms`` as every other source.

    Labels bind at the STRUCTURE level; every enabled structure INSTANCE of
    that structure inherits them (smoke record). Pure; the cluster probe
    cell calls it over collected DataFrames (setup tables — tiny)."""
    col_by_struct_seg: dict[tuple[str, str], Any] = {
        (str(s.get("StructureId")), str(s.get("SegmentCode"))):
            s.get("ColumnName")
        for s in segments
        if s.get("StructureId") is not None and s.get("SegmentCode") is not None
    }
    labels_by_struct: dict[str, list[Mapping[str, Any]]] = {}
    for lab in labeled_segments:
        sid = lab.get("StructureId")
        if sid is not None:
            labels_by_struct.setdefault(str(sid), []).append(lab)
    rows: list[dict[str, Any]] = []
    for inst in structure_instances:
        if str(inst.get("KeyFlexfieldCode") or "") != key_flexfield_code:
            continue
        flag = inst.get("EnabledFlag")
        if flag is False or (isinstance(flag, str) and flag.strip().upper() == "N"):
            continue
        identity = inst.get(IDENTITY_ATTR)
        struct_id = inst.get("StructureId")
        if identity is None or struct_id is None:
            continue
        for lab in labels_by_struct.get(str(struct_id), ()):
            seg_code = lab.get("SegmentCode")
            if seg_code is None:
                continue
            rows.append({
                IDENTITY_ATTR: identity,
                DEFAULT_QUALIFIER_ATTR: lab.get("SegmentLabelCode"),
                DEFAULT_SEGMENT_COLUMN_ATTR: col_by_struct_seg.get(
                    (str(struct_id), str(seg_code))
                ),
            })
    return rows


# ── Tier-B verification (verdicts from the SHARED gate, never re-derived) ──

ArmVerdictKind = Literal["verified", "verified_weak", "rejected", "unverified"]


@dataclass(frozen=True)
class VerifiedArm:
    """One derived arm plus its verification evidence (→ provenance)."""

    arm: CandidateArm
    verdict: ArmVerdictKind
    active_rows: int | None = None
    na_distinct: int | None = None
    na_ambiguous: int | None = None
    detail: str = ""

    @property
    def persistable(self) -> bool:
        return self.verdict in ("verified", "verified_weak")


@dataclass(frozen=True)
class VerificationOutcome:
    persistable: Mapping[str, VerifiedArm]
    rejected: tuple[VerifiedArm, ...]
    unverified: tuple[tuple[str, str], ...]
    """(chart_id, reason) — never persisted, never an error by itself."""


def verify_arms(
    candidates: Mapping[str, CandidateArm],
    probes: Mapping[str, Any],
    *,
    role_domains: Mapping[str, frozenset[str]] | None = None,
) -> VerificationOutcome:
    """Map each candidate arm to a verdict using EXCLUSIVELY the shared gate
    (:func:`coa_gate.check_natural_account` over the same
    :class:`coa_gate.ChartProbe` the runtime gate uses) plus the probe's own
    zero-evidence fields — thresholds are never re-derived here.

    Order per chart (design §7.2, extended per §7.3):
      outside the contract-backed
      segment domain           → REJECTED (AIDPF-2042 detail; the metadata
                                  may truthfully bind e.g. Segment9 while the
                                  pack contract + silver projections carry
                                  only Segment1-6 — persisting the arm would
                                  make the renderer fail with
                                  UNRESOLVED_COLUMN mid-run; the shared
                                  :func:`coa_gate.check_role_domain` rule,
                                  same as the runtime checkpoint's step 1b)
      probe missing            → UNVERIFIED ("probe_not_run") — not an error
      zero evidence (n==0 or
      active_rows==0)          → UNVERIFIED ("zero_evidence") — an arm is
                                  never persisted on zero observations,
                                  checked BEFORE below-floor
      gate errors (2017)       → REJECTED (AIDPF-2022 detail; not persisted)
      below_sample_floor       → VERIFIED_WEAK (persisted, labelled)
      warnings (above floor)   → VERIFIED_WEAK
      clean + sufficient       → VERIFIED
    """
    from ..orchestrator import coa_gate

    persistable: dict[str, VerifiedArm] = {}
    rejected: list[VerifiedArm] = []
    unverified: list[tuple[str, str]] = []
    for chart_id in sorted(candidates):
        arm = candidates[chart_id]
        domain_errors = coa_gate.check_role_domain(
            {
                chart_id: {
                    "coa.balancing": arm.balancing_segment,
                    "coa.cost_center": arm.cost_center_segment,
                    "coa.natural_account": arm.natural_account_segment,
                }
            },
            dict(role_domains) if role_domains else None,
        )
        if domain_errors:
            rejected.append(VerifiedArm(
                arm=arm, verdict="rejected",
                detail="; ".join(f"{code}: {msg}" for code, msg in domain_errors),
            ))
            continue
        probe = probes.get(chart_id)
        if probe is None:
            unverified.append((chart_id, "probe_not_run"))
            continue
        stats = dict(
            active_rows=probe.active_row_count,
            na_distinct=probe.natural_account_distinct,
            na_ambiguous=probe.natural_account_ambiguous,
        )
        if (
            probe.natural_account_distinct == 0
            or probe.active_row_count == 0
        ):
            unverified.append((chart_id, "zero_evidence"))
            continue
        res = coa_gate.check_natural_account(probe)
        if res.errors:
            rejected.append(VerifiedArm(
                arm=arm, verdict="rejected",
                detail="; ".join(f"{code}: {msg}" for code, msg in res.errors),
                **stats,
            ))
            continue
        if res.below_sample_floor or res.warnings:
            persistable[chart_id] = VerifiedArm(
                arm=arm, verdict="verified_weak",
                detail="; ".join(res.warnings), **stats,
            )
            continue
        persistable[chart_id] = VerifiedArm(
            arm=arm, verdict="verified", **stats,
        )
    return VerificationOutcome(
        persistable=persistable,
        rejected=tuple(rejected),
        unverified=tuple(unverified),
    )


def select_default_arm(
    persistable: Mapping[str, VerifiedArm],
) -> tuple[str, CandidateArm] | None:
    """Deterministic `default` fallback (design §7.4) — the arm of the
    highest-active-row chart, strictly-verified preferred over weak,
    tie-break lowest numeric chart id. Never a silent shared layout: §7.6's
    completeness verdict still requires an arm per active chart."""
    if not persistable:
        return None

    def _rank(item: tuple[str, VerifiedArm]):
        chart_id, va = item
        return (
            0 if va.verdict == "verified" else 1,
            -(va.active_rows or 0),
            int(chart_id),
        )

    chart_id, va = min(persistable.items(), key=_rank)
    return chart_id, va.arm


def render_ledger(
    outcome: VerificationOutcome,
    derivation_rejects: Sequence[ArmReject] = (),
    *,
    unresolved_active: Sequence[str] = (),
) -> tuple[str, ...]:
    """The operator-facing per-chart summary (FR-11)."""
    lines: list[str] = []
    strong = [c for c, v in outcome.persistable.items() if v.verdict == "verified"]
    weak = [c for c, v in outcome.persistable.items() if v.verdict == "verified_weak"]
    lines.append(
        f"COA metadata resolution: {len(strong)} chart(s) verified"
        + (f", {len(weak)} verified-weakly (below sample floor)" if weak else "")
    )
    for va in outcome.rejected:
        lines.append(
            f"  {AIDPF_2022_COA_ARM_REJECTED} chart {va.arm.chart_id!r} REJECTED "
            f"(not persisted): {va.detail}"
        )
    for reject in derivation_rejects:
        lines.append(
            f"  {AIDPF_2022_COA_ARM_REJECTED} chart {reject.chart_id!r} "
            f"{reject.reason}: {reject.detail}"
        )
    for chart_id, reason in outcome.unverified:
        lines.append(f"  chart {chart_id!r} UNVERIFIED ({reason}) — not persisted")
    if unresolved_active:
        lines.append(
            f"  {AIDPF_2023_COA_CHARTS_UNRESOLVED} {len(unresolved_active)} active "
            f"chart(s) remain unmapped: {', '.join(sorted(unresolved_active)[:10])}"
            + (" …" if len(unresolved_active) > 10 else "")
        )
    return tuple(lines)



# ── cluster-side helpers (the probe cell calls these from the wheel) ────────

#: The candidate config the BICC row source derives under — rows from
#: :func:`bicc_rows_to_qualifier_rows` already carry the default attribute
#: names, so only the defaults matter; the path is a label, never fetched.
BICC_ROW_CANDIDATE = ResourceCandidate(
    path="/bicc/kff-metadata",
    note="BICC KFF PVO join (smoke-pinned transport)",
    verified=True,
)


def kff_pvo_entries() -> "dict[str, Any]":
    """The smoke-pinned PVO triplet as catalog entries for
    :func:`extractors.bicc.extract_pvo`. Lazy import keeps the laptop-side
    probe free of the catalog module."""
    from ..schema.fusion_catalog import PvoEntry

    return {
        key: PvoEntry(
            id=f"__coa_metadata_{key}__",
            datastore=datastore,
            schema=KFF_PVO_SCHEMA,
            bronze_table_name=f"__coa_metadata_{key}__",
            description=(
                "COA metadata-resolution source (ephemeral — never landed "
                "as bronze); smoke-pinned 2026-08-06."
            ),
            confirmed=True,
            incremental_capable=False,
        )
        for key, datastore in KFF_METADATA_PVOS.items()
    }


def cluster_fetch_kff_rows(
    spark: "Any",
    *,
    service_url: str,
    username: str,
    password: str,
    external_storage: str,
) -> list[dict[str, Any]]:
    """CLUSTER-SIDE: extract the three KFF PVOs (tiny setup tables, full
    extract, never persisted) and join them into qualifier rows via the
    shared pure join. Raises on failure — the generated probe cell wraps
    this fail-soft into the marker's ``skipReason`` (→ AIDPF-2021 on the
    laptop). The `aidataplatform` Spark connector is cluster-only, which is
    why this cannot run on the laptop (FR-16 smoke, transport pivot)."""
    from ..extractors.bicc import extract_pvo

    entries = kff_pvo_entries()

    def _rows(key: str) -> list[dict[str, Any]]:
        df = extract_pvo(
            spark, entries[key],
            fusion_service_url=service_url,
            username=username,
            password=password,
            fusion_external_storage=external_storage,
        )
        return [row.asDict() for row in df.collect()]

    return bicc_rows_to_qualifier_rows(
        _rows("structure_instances"), _rows("segments"),
        _rows("labeled_segments"),
    )


def arms_probe_map(
    candidates: "Mapping[str, CandidateArm]",
) -> dict[str, dict[str, str]]:
    """Adapter: candidate arms → the ``arms`` mapping
    :func:`orchestrator.coa_probe.probe_charts` consumes
    (``chart_id -> {"coa.natural_account": column}``)."""
    return {
        chart_id: {"coa.natural_account": arm.natural_account_segment}
        for chart_id, arm in candidates.items()
    }


__all__ = [
    "kff_pvo_entries",
    "cluster_fetch_kff_rows",
    "arms_probe_map",
    "BICC_ROW_CANDIDATE",
    "AIDPF_2021_COA_METADATA_UNREACHABLE",
    "AIDPF_2022_COA_ARM_REJECTED",
    "AIDPF_2023_COA_CHARTS_UNRESOLVED",
    "ArmReject",
    "ArmVerdictKind",
    "CandidateArm",
    "DEFAULT_QUALIFIER_VALUES",
    "ENV_RESOURCE_PATH",
    "GL_KEY_FLEXFIELD_CODE",
    "KFF_METADATA_PVOS",
    "KFF_PVO_SCHEMA",
    "MAX_CHARTS_DERIVED",
    "METADATA_BUDGET_S",
    "PER_REQUEST_TIMEOUT_S",
    "QualifierFetch",
    "ResourceCandidate",
    "ResourcePlan",
    "SEGMENT_QUALIFIER_CANDIDATES",
    "ShapeReport",
    "VerificationOutcome",
    "VerifiedArm",
    "arm_from_qualifiers",
    "assert_same_origin",
    "bicc_rows_to_qualifier_rows",
    "derive_arms",
    "fetch_segment_qualifiers",
    "render_ledger",
    "resolve_resource_plan",
    "run_metadata_probe",
    "segment_column",
    "select_default_arm",
    "shape_assertion",
    "validate_resource_path",
    "verify_arms",
]

