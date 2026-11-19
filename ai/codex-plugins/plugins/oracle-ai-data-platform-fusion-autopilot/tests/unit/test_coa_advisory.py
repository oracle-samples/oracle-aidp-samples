"""Unit tests for the bootstrap COA advisory (metadata-driven-coa-resolution).

Pins every plan-review contract: coverage-aware monotonicity (round 2),
singletonAccepted shared-layout semantics (round 2), gate-consistent
enabled-only activity + semicolon grammar (rounds 1-2), fetch_first
single-request-by-construction (round 1), deadline-aware pagers with
fake-clock tests (round 4), fail-soft ladder incl. the named privilege
(round 4), and visibility-honest copy (rounds 4-5).
"""

from __future__ import annotations

import time as _time
from types import SimpleNamespace

import pytest
import requests

from oracle_ai_data_platform_fusion_autopilot.commands.coa_advisory import (
    CoaAdvisoryResult,
    _enabled_flag,
    compare_charts,
    run_coa_advisory,
)
from oracle_ai_data_platform_fusion_autopilot.extractors.rest import (
    DeadlineExceeded,
    fetch_first,
    fetch_paged,
)


# ── fakes ────────────────────────────────────────────────────────────────


class _Resp:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code}")
            err.response = self
            raise err


class _Session:
    """Scripted fake session: routes by substring of URL + q param."""

    def __init__(self):
        self.calls: list[dict] = []
        self.chart_pages: list[dict] = [{"items": [], "hasMore": False}]
        self.combo_router = {}  # chart_id -> n rows (or "error"/int status)
        self.fail_all_status: int | None = None

    def get(self, url, params=None, timeout=None):
        params = params or {}
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        if self.fail_all_status:
            return _Resp({}, status=self.fail_all_status)
        if "chartOfAccountsLOV" in url:
            page = int(params.get("offset", 0)) // max(int(params.get("limit", 499)), 1)
            idx = min(page, len(self.chart_pages) - 1)
            return _Resp(self.chart_pages[idx])
        if "accountCombinationsLOV" in url:
            q = params.get("q", "")
            chart_id = q.split("_CHART_OF_ACCOUNTS_ID=")[-1]
            n = self.combo_router.get(chart_id, 0)
            return _Resp({"items": [{"_CHART_OF_ACCOUNTS_ID": chart_id}] * n,
                          "hasMore": False})
        return _Resp({}, status=404)


# ── compare_charts: pure comparison core ─────────────────────────────────


class TestCompareCharts:
    def test_singleton_ok_silent(self):
        r = compare_charts(
            visible_enabled={"1": True},
            activity={"1": "active"},
            by_chart_keys=frozenset(),
            singleton_accepted=False,
        )
        assert r.kind == "ok" and r.lines == () and r.coverage == "complete"

    def test_multi_active_accepted_shared_layout_silent(self):
        """Round-2 Finding 3: singletonAccepted → no warning (gate parity)."""
        r = compare_charts(
            visible_enabled={"1": True, "2": True},
            activity={"1": "active", "2": "active"},
            by_chart_keys=frozenset(),
            singleton_accepted=True,
        )
        assert r.kind == "ok" and r.lines == ()

    def test_multi_active_unaccepted_names_both_remediations(self):
        r = compare_charts(
            visible_enabled={"1": True, "2": True},
            activity={"1": "active", "2": "active"},
            by_chart_keys=frozenset(),
            singleton_accepted=False,
        )
        assert r.kind == "findings"
        assert "byChart" in r.lines[0] and "--accept-singleton-coa" in r.lines[0]

    def test_full_mapping_all_active_silent(self):
        r = compare_charts(
            visible_enabled={"1": True, "2": True},
            activity={"1": "active", "2": "active"},
            by_chart_keys=frozenset({"1", "2"}),
            singleton_accepted=False,
        )
        assert r.kind == "ok" and r.lines == ()

    def test_active_unmapped_finding_with_additive_remediation(self):
        r = compare_charts(
            visible_enabled={"1": True, "9": True},
            activity={"9": "active"},
            by_chart_keys=frozenset({"1"}),
            singleton_accepted=False,
        )
        assert r.kind == "findings" and r.findings == ("9",)
        assert "bootstrap --refresh" in r.lines[0]
        assert "additive fast path" in r.lines[0]
        assert "visible to the configured Fusion user" in r.lines[0]

    def test_coverage_monotonicity_partial_never_ok(self):
        """Round-2 Finding 2: confirmed active + unprobed candidate → partial,
        finding still emitted, no all-clear."""
        r = compare_charts(
            visible_enabled={"1": True, "9": True, "8": True},
            activity={"9": "active"},  # "8" unprobed
            by_chart_keys=frozenset({"1"}),
            singleton_accepted=False,
        )
        assert r.kind == "partial" and r.coverage == "partial"
        assert r.findings == ("9",)
        assert any("no all-clear" in l for l in r.lines)
        assert any("8" in l for l in r.lines)

    def test_enabled_but_inactive_unmapped_is_not_a_finding(self):
        """The live dev-tenant case (34626/67627): enabled, no active
        combinations → gate-consistently NOT a finding."""
        r = compare_charts(
            visible_enabled={"1": True, "34626": True},
            activity={"34626": "inactive", "1": "active"},
            by_chart_keys=frozenset({"1"}),
            singleton_accepted=False,
        )
        assert r.kind == "ok" and r.findings == ()

    def test_mapped_not_visible_is_info_not_finding(self):
        """Round-4 visibility honesty."""
        r = compare_charts(
            visible_enabled={"1": True},
            activity={"1": "active"},
            by_chart_keys=frozenset({"1", "777"}),
            singleton_accepted=False,
        )
        assert r.kind == "ok"
        assert any("not visible to the configured Fusion user" in l for l in r.lines)

    def test_mapped_inactive_info_only_when_probed(self):
        probed = compare_charts(
            visible_enabled={"1": True, "2": True},
            activity={"1": "active", "2": "inactive"},
            by_chart_keys=frozenset({"1", "2"}),
            singleton_accepted=False,
        )
        assert any("no enabled combinations" in l for l in probed.lines)
        unprobed = compare_charts(
            visible_enabled={"1": True, "2": True},
            activity={"1": "active"},  # 2 never probed
            by_chart_keys=frozenset({"1", "2"}),
            singleton_accepted=False,
        )
        assert not any("no enabled combinations" in l for l in unprobed.lines)


# ── EnabledFlag parsing (post-ship review, Blocking) ─────────────────────


class TestEnabledFlagParsing:
    """``bool("N")`` is ``True`` — the chart LOV's flag must be parsed
    explicitly. Policy is gate-consistent with ``_coa_chart_active``'s
    ``COALESCE(flag, 'Y') <> 'N'``: disabled iff explicitly False/"N";
    absent/null → enabled. Both live wire shapes (JSON boolean, "Y"/"N"
    string) are accepted."""

    @pytest.mark.parametrize(
        ("wire", "expected"),
        [
            ("Y", True),
            ("N", False),
            ("y", True),
            ("n", False),
            (" N ", False),   # whitespace-tolerant
            (True, True),
            (False, False),
            (None, True),     # null policy: COALESCE(flag, 'Y')
            ("", True),       # SQL '' <> 'N' → enabled
        ],
    )
    def test_wire_shapes(self, wire, expected):
        assert _enabled_flag(wire) is expected


# ── fetchers: single-request + deadline (rounds 1 & 4) ───────────────────


class TestFetchers:
    def test_fetch_first_exactly_one_request_despite_hasmore(self):
        s = _Session()
        s.chart_pages = [{"items": [{"A": 1}], "hasMore": True}]
        row = fetch_first(s, "https://x", "/y/chartOfAccountsLOV")
        assert row == {"A": 1}
        assert len(s.calls) == 1  # hasMore=true must NOT trigger a second call

    def test_fetch_first_none_on_empty(self):
        s = _Session()
        s.chart_pages = [{"items": [], "hasMore": False}]
        assert fetch_first(s, "https://x", "/y/chartOfAccountsLOV") is None

    def test_fetch_paged_deadline_stops_multipage(self, monkeypatch):
        """Fake clock: each request costs 20s; a 30s budget allows one page."""
        clock = SimpleNamespace(t=1000.0)
        monkeypatch.setattr(_time, "monotonic", lambda: clock.t)

        class _SlowSession(_Session):
            def get(self, url, params=None, timeout=None):
                clock.t += 20.0
                return super().get(url, params=params, timeout=timeout)

        s = _SlowSession()
        s.chart_pages = [
            {"items": [{"A": i}], "hasMore": True} for i in range(10)
        ]
        it = fetch_paged(
            s, "https://x", "/y/chartOfAccountsLOV",
            limit=1, timeout=10, deadline=1030.0,
        )
        got = [next(it)]           # page 1 ok (t: 1000 -> 1020)
        got.append(next(it))       # page 2 ok (t: 1020 -> 1040 > deadline after)
        with pytest.raises(DeadlineExceeded):
            next(it)               # page 3 request refused pre-flight
        assert len(got) == 2

    def test_near_expiry_request_gets_clamped_timeout(self, monkeypatch):
        clock = SimpleNamespace(t=0.0)
        monkeypatch.setattr(_time, "monotonic", lambda: clock.t)
        s = _Session()
        s.chart_pages = [{"items": [{"A": 1}], "hasMore": False}]
        clock.t = 27.5  # 2.5s left of a 30s deadline
        fetch_first(s, "https://x", "/y/chartOfAccountsLOV",
                    timeout=10, deadline=30.0)
        assert s.calls[0]["timeout"] == pytest.approx(2.5)

    def test_expired_deadline_raises_before_any_request(self, monkeypatch):
        clock = SimpleNamespace(t=100.0)
        monkeypatch.setattr(_time, "monotonic", lambda: clock.t)
        s = _Session()
        with pytest.raises(DeadlineExceeded):
            fetch_first(s, "https://x", "/y", timeout=10, deadline=99.0)
        assert s.calls == []


# ── run_coa_advisory: orchestration + fail-soft ladder ───────────────────


def _coa(by_chart: dict | None = None, singleton: bool = False) -> dict:
    coa: dict = {"default": {"naturalAccountSegment": "SEGMENT3"}}
    if by_chart is not None:
        coa["byChart"] = by_chart
    if singleton:
        coa["singletonAccepted"] = True
    return coa


class TestRunCoaAdvisory:
    def test_happy_path_finding(self):
        s = _Session()
        s.chart_pages = [{
            "items": [
                {"StructureInstanceNumber": 1, "EnabledFlag": True},
                {"StructureInstanceNumber": 9, "EnabledFlag": True},
            ],
            "hasMore": False,
        }]
        s.combo_router = {"9": 1, "1": 1}
        r = run_coa_advisory(
            service_url="https://x",
            chart_of_accounts=_coa(by_chart={"1": {}}),
            session=s,
        )
        assert r.kind == "findings" and r.findings == ("9",)
        # Semicolon grammar (Step-1: ' and ' silently matches nothing).
        probe_qs = [c["params"].get("q", "") for c in s.calls if "accountCombinations" in c["url"]]
        assert all("EnabledFlag=Y;_CHART_OF_ACCOUNTS_ID=" in q for q in probe_qs)
        # Candidate (9) probed BEFORE the mapped sweep (1).
        assert "9" in probe_qs[0]

    def test_disabled_n_chart_never_probed_never_displaces_candidates(self):
        """Post-ship review (Blocking): with ``bool()``, a string-"N"
        (disabled) unmapped chart entered the candidate probe list and could
        exhaust the shared budget before a genuinely active unmapped chart
        was reached. Pin: an "N" chart is never probed at all — the probe
        count equals the enabled charts exactly — and the active unmapped
        chart still yields its finding."""
        s = _Session()
        s.chart_pages = [{
            "items": [
                {"StructureInstanceNumber": 5, "EnabledFlag": "N"},  # disabled, unmapped
                {"StructureInstanceNumber": 9, "EnabledFlag": "Y"},  # active, unmapped
                {"StructureInstanceNumber": 1, "EnabledFlag": "Y"},  # mapped
            ],
            "hasMore": False,
        }]
        s.combo_router = {"9": 1, "1": 1}
        r = run_coa_advisory(
            service_url="https://x",
            chart_of_accounts=_coa(by_chart={"1": {}}),
            session=s,
        )
        assert r.kind == "findings" and r.findings == ("9",)
        assert r.coverage == "complete"  # nothing left unprobed by budget
        probe_qs = [c["params"].get("q", "")
                    for c in s.calls if "accountCombinationsLOV" in c["url"]]
        # The disabled chart is never probed…
        assert not any(q.endswith("_CHART_OF_ACCOUNTS_ID=5") for q in probe_qs)
        # …and cannot displace anyone: probes = candidate 9 + mapped 1 only,
        # with the candidate still first in budget order.
        assert len(probe_qs) == 2
        assert probe_qs[0].endswith("_CHART_OF_ACCOUNTS_ID=9")

    def test_dev_tenant_shape_silent_ok(self):
        """43 visible / 41 mapped / 2 enabled-but-inactive → silent OK."""
        items = [{"StructureInstanceNumber": i, "EnabledFlag": True} for i in range(41)]
        items += [{"StructureInstanceNumber": 34626, "EnabledFlag": True},
                  {"StructureInstanceNumber": 67627, "EnabledFlag": True}]
        s = _Session()
        s.chart_pages = [{"items": items, "hasMore": False}]
        s.combo_router = {str(i): 1 for i in range(41)}  # mapped all active
        # 34626/67627 default to 0 rows → inactive
        r = run_coa_advisory(
            service_url="https://x",
            chart_of_accounts=_coa(by_chart={str(i): {} for i in range(41)}),
            session=s,
        )
        assert r.kind == "ok" and r.findings == ()

    def test_missing_creds_skips(self, monkeypatch):
        monkeypatch.delenv("FUSION_BICC_USER", raising=False)
        monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)
        r = run_coa_advisory(service_url="https://x", chart_of_accounts=_coa())
        assert r.kind == "skipped" and "not set" in r.skip_reason

    def test_secret_ref_password_skips(self, monkeypatch):
        monkeypatch.setenv("FUSION_BICC_USER", "u")
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "${aidp:secret:x}")
        r = run_coa_advisory(service_url="https://x", chart_of_accounts=_coa())
        assert r.kind == "skipped" and "cluster-only" in r.skip_reason

    def test_401_skips_naming_the_privilege(self):
        s = _Session()
        s.fail_all_status = 401
        r = run_coa_advisory(
            service_url="https://x", chart_of_accounts=_coa(), session=s,
        )
        assert r.kind == "skipped"
        assert "FUN_GET_ENTERPRISE_STRUCTURES_REST_SERVICE_PRIV" in r.skip_reason

    def test_budget_expiry_mid_lov_skips(self, monkeypatch):
        clock = SimpleNamespace(t=0.0)
        monkeypatch.setattr(_time, "monotonic", lambda: clock.t)

        class _SlowSession(_Session):
            def get(self, url, params=None, timeout=None):
                clock.t += 31.0  # single page blows the whole budget
                return super().get(url, params=params, timeout=timeout)

        s = _SlowSession()
        s.chart_pages = [{"items": [{"StructureInstanceNumber": 1, "EnabledFlag": True}],
                          "hasMore": True}]
        r = run_coa_advisory(
            service_url="https://x", chart_of_accounts=_coa(by_chart={"1": {}}),
            session=s,
        )
        assert r.kind == "skipped" and "listing charts" in r.skip_reason

    def test_budget_expiry_mid_probes_partial(self, monkeypatch):
        clock = SimpleNamespace(t=0.0)
        monkeypatch.setattr(_time, "monotonic", lambda: clock.t)

        class _ProbeSlowSession(_Session):
            def get(self, url, params=None, timeout=None):
                if "accountCombinationsLOV" in url:
                    clock.t += 31.0  # first probe alone blows the 30s budget
                return super().get(url, params=params, timeout=timeout)

        s = _ProbeSlowSession()
        s.chart_pages = [{
            "items": [
                {"StructureInstanceNumber": 8, "EnabledFlag": True},
                {"StructureInstanceNumber": 9, "EnabledFlag": True},
            ],
            "hasMore": False,
        }]
        s.combo_router = {"8": 1, "9": 1}
        r = run_coa_advisory(
            service_url="https://x",
            chart_of_accounts=_coa(by_chart={}),  # byChart empty dict → no keys
            session=s,
        )
        # Two candidates; the second probe is refused at the deadline.
        assert r.kind == "partial"
        assert any("no all-clear" in l for l in r.lines)