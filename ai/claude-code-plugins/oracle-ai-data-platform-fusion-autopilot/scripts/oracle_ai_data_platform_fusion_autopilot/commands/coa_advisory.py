"""Bootstrap COA advisory (feature: metadata-driven-coa-resolution, Phase 1).

Pre-extraction, NON-BLOCKING advisory: compare the charts of accounts
**visible to the configured Fusion user** (transactional REST LOVs) against
the profile's ``chartOfAccounts`` mapping, and surface active-but-unmapped
charts BEFORE any extract — with the additive-flow remediation
(`bootstrap --refresh` then an ordinary incremental) spelled out.

Contract (hardened across five plan-review rounds):

* **Never a gate** — no exit-code impact, no AIDPF error code; every failure
  mode degrades to a labelled skip/partial (fail-soft ladder).
* **Gate-consistent activity** — a chart is ACTIVE iff it has >= 1
  combination with ``EnabledFlag=Y`` (matching ``_coa_chart_active``'s
  enabled-only definition over landed ``gl_coa``; dates are NOT part of
  activity in this codebase).
* **Coverage-aware monotonicity** — FINDINGS may be emitted from partial
  evidence; OK verdicts / counts / "mapped but inactive" notes require
  complete relevant coverage. Partial renders as "probed N of M", never OK.
* **Visibility honesty** — the REST surface proves API-visible knowledge for
  the calling principal only; copy says "visible to the configured Fusion
  user" and never claims tenant-wide truth. The authoritative,
  tenant-complete backstop remains the post-extraction AIDPF-2018 gate.
* **Budget-bounded** — one shared absolute monotonic deadline enforced by the
  fetchers themselves (checked before every request; per-request timeout
  clamped to the remainder).

Live-established constants (Step-1 smoke, 2026-07-17, dev tenant):
identity attribute = ``StructureInstanceNumber`` (matched 41/41 known
``byChart`` ids; ``StructureInstanceId`` matched 6); combinations filter
grammar = ``q=EnabledFlag=Y;_CHART_OF_ACCOUNTS_ID=<id>`` with the SEMICOLON
separator — the `` and `` form returns 200 with 0 rows (silent
false-inactive hazard, pinned in tests).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

# ── REST surface (Step-1-validated) ─────────────────────────────────────────
_API = "/fscmRestApi/resources/11.13.18.05"
CHART_LOV_PATH = f"{_API}/chartOfAccountsLOV"
COMBINATIONS_LOV_PATH = f"{_API}/accountCombinationsLOV"

IDENTITY_ATTR = "StructureInstanceNumber"
"""The chartOfAccountsLOV attribute whose values equal
``gl_coa.CodeCombinationChartOfAccountsId`` / the numeric ``byChart`` keys.
Established EMPIRICALLY (Step-1 smoke): 41/41 known ids matched; the
doc-plausible ``StructureInstanceId`` matched only 6."""

CHART_LOV_FIELDS = f"{IDENTITY_ATTR},EnabledFlag"
"""Field projection for the chart list — keep the page tiny."""

_ACTIVE_Q = "EnabledFlag=Y;_CHART_OF_ACCOUNTS_ID={chart_id}"
"""Server-side ACTIVE filter for the per-chart existence probe. SEMICOLON is
load-bearing: `` and `` silently matches nothing (Step-1 smoke) — using it
would classify every chart inactive."""

PER_REQUEST_TIMEOUT_S = 10
ADVISORY_BUDGET_S = 30.0

Coverage = Literal["complete", "partial"]
Activity = Literal["active", "inactive", "unprobed"]


# ── structured result ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class CoaAdvisoryResult:
    """Outcome of one advisory evaluation. The caller renders ``lines``
    verbatim (yellow, non-blocking); ``kind`` is for tests/telemetry."""

    kind: Literal["ok", "findings", "partial", "skipped"]
    lines: tuple[str, ...] = ()
    skip_reason: str | None = None
    coverage: Coverage | None = None
    findings: tuple[str, ...] = ()  # chart ids that are active-but-unmapped


# ── pure comparison core (coverage-aware) ───────────────────────────────────


def compare_charts(
    *,
    visible_enabled: dict[str, bool],
    activity: dict[str, Activity],
    by_chart_keys: frozenset[str],
    singleton_accepted: bool,
) -> CoaAdvisoryResult:
    """Compare visible charts vs the profile mapping. Pure; unit-tested.

    Monotonicity: findings emit from partial evidence; OK / counts /
    informational notes require complete relevant coverage.
    """
    enabled_ids = {c for c, en in visible_enabled.items() if en}

    # ── No byChart at all: shared-layout semantics govern ────────────────
    if not by_chart_keys:
        act = {c: activity.get(c, "unprobed") for c in sorted(enabled_ids)}
        actives = [c for c, a in act.items() if a == "active"]
        unprobed = [c for c, a in act.items() if a == "unprobed"]
        if unprobed:
            lines = []
            if len(actives) > 1 and not singleton_accepted:
                lines.append(_shared_layout_finding(len(actives)))
            lines.append(_partial_line(len(act) - len(unprobed), len(act), unprobed))
            return CoaAdvisoryResult(
                kind="partial", lines=tuple(lines), coverage="partial",
                findings=tuple(actives) if len(actives) > 1 and not singleton_accepted else (),
            )
        if len(actives) > 1 and not singleton_accepted:
            return CoaAdvisoryResult(
                kind="findings",
                lines=(_shared_layout_finding(len(actives)),),
                coverage="complete",
                findings=tuple(actives),
            )
        # 0/1 active (singleton OK), or explicitly accepted shared layout
        # (matches the AIDPF-2018 gate's acceptance) → silent.
        return CoaAdvisoryResult(kind="ok", coverage="complete")

    # ── byChart exists: candidate = enabled-and-unmapped ──────────────────
    unmapped = sorted(enabled_ids - by_chart_keys)
    candidate_activity = {c: activity.get(c, "unprobed") for c in unmapped}
    confirmed = tuple(c for c in unmapped if candidate_activity[c] == "active")
    unprobed_candidates = [c for c in unmapped if candidate_activity[c] == "unprobed"]

    lines: list[str] = []

    # Findings — valid from partial evidence.
    if confirmed:
        lines.append(
            f"COA advisory: chart(s) {', '.join(confirmed)} are ACTIVE (have "
            f"enabled combinations) and visible to the configured Fusion user, "
            f"but have no `chartOfAccounts.byChart` arm. Add each arm via "
            f"`bootstrap --refresh`; an ordinary `run --mode incremental` will "
            f"absorb a new chart (additive fast path)."
        )

    if unprobed_candidates:
        lines.append(
            _partial_line(
                len(unmapped) - len(unprobed_candidates), len(unmapped),
                unprobed_candidates,
            )
        )
        return CoaAdvisoryResult(
            kind="partial", lines=tuple(lines), coverage="partial",
            findings=confirmed,
        )

    # Complete coverage of candidates from here on.
    # Informational: mapped ids not visible to this user (never a finding —
    # could be privilege / data-security scope).
    not_visible = sorted(by_chart_keys - set(visible_enabled))
    if not_visible:
        lines.append(
            f"COA advisory (info): mapped chart(s) {', '.join(not_visible)} "
            f"are not visible to the configured Fusion user (privilege or "
            f"data-security scope?). The post-extraction COA gate remains the "
            f"authoritative check."
        )

    # Informational: mapped-but-inactive — only where the probe actually ran.
    mapped_inactive = sorted(
        c for c in by_chart_keys
        if activity.get(c) == "inactive" and c in visible_enabled
    )
    if mapped_inactive:
        lines.append(
            f"COA advisory (info): mapped chart(s) {', '.join(mapped_inactive)} "
            f"currently have no enabled combinations visible to the configured "
            f"Fusion user."
        )

    if not lines:
        # Silent when everything matches — no operator noise.
        return CoaAdvisoryResult(kind="ok", coverage="complete")
    return CoaAdvisoryResult(
        kind="findings" if confirmed else "ok",
        lines=tuple(lines), coverage="complete", findings=confirmed,
    )


def _shared_layout_finding(n_active: int) -> str:
    return (
        f"COA advisory: {n_active} active charts are visible to the "
        f"configured Fusion user but the profile has no `byChart` mapping and "
        f"the shared layout is not accepted. Map them via "
        f"`chartOfAccounts.byChart`, or explicitly accept the shared layout "
        f"with `--accept-singleton-coa`."
    )


def _partial_line(probed: int, total: int, unprobed: list[str]) -> str:
    return (
        f"COA advisory: partial — probed {probed} of {total} candidate "
        f"chart(s) within the time budget; unprobed: {', '.join(unprobed)}. "
        f"Findings above are confirmed; no all-clear is implied."
    )


# ── fetch orchestration (fail-soft, budget-bounded) ─────────────────────────


def _is_aidp_secret_ref(value: str) -> bool:
    return value.startswith("${aidp:secret:")


def _enabled_flag(value: Any) -> bool:
    """Parse the chart LOV's ``EnabledFlag`` — never ``bool()``.

    Fusion serialises flag attributes as JSON booleans on some resources and
    as ``"Y"`` / ``"N"`` strings on others; ``bool("N")`` is ``True``, which
    would admit disabled charts into the candidate probe list and let them
    burn the shared budget ahead of a genuinely active unmapped chart.
    Policy is gate-consistent with ``_coa_chart_active``'s
    ``COALESCE(flag, 'Y') <> 'N'``: disabled iff explicitly ``False`` or
    ``"N"``; absent/null → enabled (an extra probe is safe; silently
    dropping a chart is not).
    """
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() != "N"


@dataclass
class _Skip(Exception):
    reason: str


def _env_creds() -> tuple[str, str]:
    """The `_probe_bicc` credential contract: env vars only; a missing value
    or an ``${aidp:secret:…}`` ref (cluster-only resolvable) → skip."""
    user = os.environ.get("FUSION_BICC_USER") or ""
    pwd = os.environ.get("FUSION_BICC_PASSWORD") or ""
    if not user or not pwd:
        raise _Skip("FUSION_BICC_USER / FUSION_BICC_PASSWORD not set")
    if _is_aidp_secret_ref(pwd):
        raise _Skip(
            "FUSION_BICC_PASSWORD is an ${aidp:secret:…} ref (cluster-only)"
        )
    return user, pwd


def run_coa_advisory(
    *,
    service_url: str,
    chart_of_accounts: dict[str, Any] | None,
    session: Any | None = None,
    budget_s: float = ADVISORY_BUDGET_S,
) -> CoaAdvisoryResult:
    """Fetch + compare, fail-soft. Returns a result the caller renders.

    Args:
        service_url: the Fusion pod base URL (``bundle.fusion.service_url``).
        chart_of_accounts: the profile's raw ``chartOfAccounts`` dict (prior
            profile on the no-drift refresh path; freshly resolved on the full
            path). ``None``/empty → treated as no mapping.
        session: injected authenticated session (tests); ``None`` → build one
            from the env-credential contract.
        budget_s: overall wall-clock budget (monotonic).
    """
    from ..extractors.rest import DeadlineExceeded, fetch_first, fetch_paged

    coa = chart_of_accounts or {}
    by_chart_keys = frozenset(str(k) for k in (coa.get("byChart") or {}))
    singleton_accepted = bool(coa.get("singletonAccepted"))

    try:
        if session is None:
            import requests

            user, pwd = _env_creds()
            session = requests.Session()
            session.auth = (user, pwd)

        deadline = time.monotonic() + budget_s
        base = service_url.rstrip("/")

        # 1) Chart list — must COMPLETE or the advisory skips (an incomplete
        #    list can justify nothing).
        try:
            visible_enabled: dict[str, bool] = {}
            for item in fetch_paged(
                session, base, CHART_LOV_PATH,
                fields=CHART_LOV_FIELDS,
                timeout=PER_REQUEST_TIMEOUT_S,
                deadline=deadline,
            ):
                cid = item.get(IDENTITY_ATTR)
                if cid is not None:
                    visible_enabled[str(cid)] = _enabled_flag(
                        item.get("EnabledFlag")
                    )
        except DeadlineExceeded:
            raise _Skip("time budget expired while listing charts") from None

        # 2) Activity probes — candidates FIRST (potential findings), then the
        #    informational mapped sweep; each probe is one request by
        #    construction; deadline enforced by the fetcher.
        enabled_ids = {c for c, en in visible_enabled.items() if en}
        candidates = sorted(enabled_ids - by_chart_keys)
        mapped_visible = sorted(by_chart_keys & set(visible_enabled))
        activity: dict[str, Activity] = {}
        for cid in candidates + mapped_visible:
            try:
                row = fetch_first(
                    session, base, COMBINATIONS_LOV_PATH,
                    extra_params={"q": _ACTIVE_Q.format(chart_id=cid)},
                    timeout=PER_REQUEST_TIMEOUT_S,
                    deadline=deadline,
                )
                activity[cid] = "active" if row is not None else "inactive"
            except DeadlineExceeded:
                break  # remaining charts stay "unprobed" → partial handling

        return compare_charts(
            visible_enabled=visible_enabled,
            activity=activity,
            by_chart_keys=by_chart_keys,
            singleton_accepted=singleton_accepted,
        )

    except _Skip as skip:
        return CoaAdvisoryResult(kind="skipped", skip_reason=skip.reason)
    except Exception as exc:  # noqa: BLE001 — advisory NEVER raises out
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in (401, 403):
            reason = (
                f"Fusion REST returned {status} — the configured user lacks "
                f"the GL REST privilege "
                f"(FUN_GET_ENTERPRISE_STRUCTURES_REST_SERVICE_PRIV)"
            )
        else:
            reason = f"{type(exc).__name__}: {str(exc)[:120]}"
        return CoaAdvisoryResult(kind="skipped", skip_reason=reason)


__all__ = [
    "ADVISORY_BUDGET_S",
    "CHART_LOV_FIELDS",
    "CHART_LOV_PATH",
    "COMBINATIONS_LOV_PATH",
    "CoaAdvisoryResult",
    "IDENTITY_ATTR",
    "PER_REQUEST_TIMEOUT_S",
    "compare_charts",
    "run_coa_advisory",
]
