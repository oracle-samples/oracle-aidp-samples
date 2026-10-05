"""Unit tests for ``commands.bootstrap._probe_aidp``.

D4 regression (docs/v2-phase-4-live-defects.md): the probe must use
``AidpRestClient`` (canonical ``datalake.<region>.oci.oraclecloud.com``
host) rather than the obsolete ``<workspace>.aidataplatform.<region>...``
URL pattern. These tests pin the canonical-client reuse so the
regression cannot recur silently.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from oracle_ai_data_platform_fusion_autopilot.commands import bootstrap as bs


def _env(**overrides) -> SimpleNamespace:
    """Minimal env shape with the fields ``_probe_aidp`` reads."""
    defaults = dict(
        workspace_key="ws-key-123",
        ai_data_platform_id="ocid1.aidataplatform.oc1.iad.aaa",
        region="us-ashburn-1",
        oci_profile="DEFAULT",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _ws(key: str, display_name: str = "") -> SimpleNamespace:
    return SimpleNamespace(key=key, display_name=display_name)


class TestProbeAidpReusesCanonicalClient:
    def test_pass_when_workspace_visible(self) -> None:
        results: list = []
        client = MagicMock()
        client.list_workspaces.return_value = [
            _ws("ws-other"),
            _ws("ws-key-123", "demo-pod"),
        ]
        with patch.object(
            bs, "_probe_aidp", wraps=bs._probe_aidp
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient",
            return_value=client,
        ):
            bs._probe_aidp(_env(), results)
        assert len(results) == 1
        assert results[0].status == "PASS"
        assert "ws-key-123" in results[0].detail

    def test_fail_when_workspace_not_in_list(self) -> None:
        results: list = []
        client = MagicMock()
        client.list_workspaces.return_value = [_ws("ws-other"), _ws("ws-third")]
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient",
            return_value=client,
        ):
            bs._probe_aidp(_env(), results)
        assert results[0].status == "FAIL"
        # Diagnostic must surface what WAS visible — operator's clue.
        assert "ws-other" in results[0].detail

    def test_fail_when_aidp_id_unset(self) -> None:
        results: list = []
        bs._probe_aidp(_env(ai_data_platform_id=None), results)
        assert results[0].status == "FAIL"
        assert "aiDataPlatformId" in results[0].detail

    def test_fail_on_rest_error(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
            AidpRestError,
        )

        results: list = []
        client = MagicMock()
        client.list_workspaces.side_effect = AidpRestError("HTTP 401 — unauthenticated")
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient",
            return_value=client,
        ):
            bs._probe_aidp(_env(), results)
        assert results[0].status == "FAIL"
        assert "401" in results[0].detail

    def test_fail_on_signer_construction_exception(self) -> None:
        # Generic Exception (e.g., OCI config load failure) is caught and
        # surfaced as FAIL, not crashed out of the probe phase.
        results: list = []
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient",
            side_effect=RuntimeError("oci config missing"),
        ):
            bs._probe_aidp(_env(), results)
        assert results[0].status == "FAIL"
        assert "client init/request error" in results[0].detail


class TestProbeAidpUrlAntiRegression:
    """Lock the D4 fix: the probe must NOT call requests.get against the
    obsolete `<ws>.aidataplatform.<region>...` URL pattern.
    """

    def test_does_not_call_requests_get(self) -> None:
        results: list = []
        client = MagicMock()
        client.list_workspaces.return_value = [_ws("ws-key-123")]
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client.AidpRestClient",
            return_value=client,
        ), patch.object(bs, "requests") as requests_mock:
            bs._probe_aidp(_env(), results)
        # The probe used AidpRestClient.list_workspaces — `requests.get`
        # was never invoked from this code path.
        requests_mock.get.assert_not_called()
