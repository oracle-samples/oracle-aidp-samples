"""Unit tests for OacOauthFlow (Auth Code + PKCE + Refresh Token)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.oac.rest.oauth import (
    IdcsTokenFetcher,
    OacOauthFlow,
    TokenBundle,
    derive_oac_scope,
    discover_oac_audience,
)


def _ok_response(*, access_token: str = "a.b.c", refresh_token: str = "rt-123",
                 expires_in: int = 3600) -> MagicMock:
    r = MagicMock(status_code=200)
    r.json.return_value = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    }
    return r


class TestRefreshTokenFlow:
    """The cached path: load persisted refresh token, exchange for fresh access token."""

    def test_refreshes_silently_when_cached_token_expired(self, tmp_path: Path) -> None:
        # Pre-populate a cached bundle whose access token is expired but with a refresh token
        cache = tmp_path / "oac-token.json"
        cache.write_text(json.dumps({
            "access_token": "stale-access",
            "refresh_token": "valid-refresh",
            "expires_at": time.time() - 60,
        }))

        s = MagicMock()
        s.post.return_value = _ok_response(access_token="fresh-access")
        flow = OacOauthFlow(
            "https://idcs-x.identity.oraclecloud.com",
            "cid", "csec",
            scope="https://oac.example.comurn:opc:resource:consumer::all",
            token_path=cache,
            session=s,
        )
        assert flow.get_token() == "fresh-access"

        # Verify it called refresh_token grant (not authorization_code)
        call = s.post.call_args
        assert call.kwargs["data"]["grant_type"] == "refresh_token"
        assert call.kwargs["data"]["refresh_token"] == "valid-refresh"

    def test_returns_cached_token_when_still_valid(self, tmp_path: Path) -> None:
        cache = tmp_path / "oac-token.json"
        cache.write_text(json.dumps({
            "access_token": "still-good",
            "refresh_token": "rt",
            "expires_at": time.time() + 3600,
        }))
        s = MagicMock()
        flow = OacOauthFlow(
            "https://idcs-x.identity.oraclecloud.com",
            "cid", "csec",
            scope="https://oac.example.comurn:opc:resource:consumer::all",
            token_path=cache,
            session=s,
        )
        assert flow.get_token() == "still-good"
        s.post.assert_not_called()

    def test_persists_token_after_refresh(self, tmp_path: Path) -> None:
        cache = tmp_path / "oac-token.json"
        cache.write_text(json.dumps({
            "access_token": "stale",
            "refresh_token": "valid-rt",
            "expires_at": time.time() - 60,
        }))
        s = MagicMock()
        s.post.return_value = _ok_response(access_token="new", refresh_token="new-rt")
        flow = OacOauthFlow(
            "https://idcs-x.identity.oraclecloud.com",
            "cid", "csec",
            scope="https://oac.example.comurn:opc:resource:consumer::all",
            token_path=cache,
            session=s,
        )
        flow.get_token()
        persisted = json.loads(cache.read_text())
        assert persisted["access_token"] == "new"
        assert persisted["refresh_token"] == "new-rt"


class TestUrlValidation:
    def test_requires_url_scheme(self) -> None:
        with pytest.raises(ValueError, match="must include scheme"):
            OacOauthFlow(
                "idcs-x.identity.oraclecloud.com",
                "cid", "csec",
                scope="https://oac.example.comurn:opc:resource:consumer::all",
            )


class TestDeriveOacScope:
    def test_default_uses_oac_url_when_no_audience(self) -> None:
        scope = derive_oac_scope("https://oac.example.com/")
        assert scope == "https://oac.example.comurn:opc:resource:consumer::all offline_access"

    def test_audience_override_replaces_oac_url(self) -> None:
        scope = derive_oac_scope(
            "https://oac.example.com/",
            audience="https://prefix.analytics.ocp.oraclecloud.com",
        )
        assert scope == (
            "https://prefix.analytics.ocp.oraclecloud.com"
            "urn:opc:resource:consumer::all offline_access"
        )


class TestDiscoverOacAudience:
    def test_extracts_audience_from_login_redirect(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verified live 2026-05-01 — OAC's /ui/ redirects to IDCS authorize URL with
        ``idcs_app_name=<24-char>_APPID`` query param; the prefix is the audience host."""
        from oracle_ai_data_platform_fusion_bundle.oac.rest import oauth as oauth_mod

        redirect_loc = (
            "https://idcs-abc.identity.oraclecloud.com/oauth2/v1/authorize?"
            "idcs_app_name=tqa3fnvu5u6mjswcuphhqeorojsrl5wq_APPID&"
            "X-HOST-IDENTIFIER-NAME=oac-host"
        )
        fake_response = MagicMock()
        fake_response.headers = {"Location": redirect_loc}
        fake_response.status_code = 302

        captured: dict = {}
        def fake_get(url, **kwargs):
            captured["url"] = url
            return fake_response
        monkeypatch.setattr(oauth_mod.requests, "get", fake_get)

        audience = discover_oac_audience("https://oac.example.com/")
        assert audience == "https://tqa3fnvu5u6mjswcuphhqeorojsrl5wq.analytics.ocp.oraclecloud.com"
        assert captured["url"].endswith("/ui/")

    def test_raises_when_no_idcs_app_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from oracle_ai_data_platform_fusion_bundle.oac.rest import oauth as oauth_mod
        fake_response = MagicMock(status_code=200, headers={})
        monkeypatch.setattr(oauth_mod.requests, "get", lambda *a, **k: fake_response)
        with pytest.raises(RuntimeError, match="audience discovery failed"):
            discover_oac_audience("https://oac.example.com/")


class TestTokenBundle:
    def test_is_valid_when_far_from_expiry(self) -> None:
        b = TokenBundle("t", refresh_token="rt", expires_at=time.time() + 3600)
        assert b.is_valid() is True

    def test_is_invalid_when_within_leeway(self) -> None:
        b = TokenBundle("t", refresh_token="rt", expires_at=time.time() + 5)
        assert b.is_valid(leeway_seconds=30) is False

    def test_is_invalid_after_expiry(self) -> None:
        b = TokenBundle("t", refresh_token=None, expires_at=time.time() - 1)
        assert b.is_valid() is False

    def test_roundtrip_to_and_from_dict(self) -> None:
        original = TokenBundle("at", refresh_token="rt", expires_at=12345.0)
        roundtripped = TokenBundle.from_dict(original.to_dict())
        assert roundtripped.access_token == "at"
        assert roundtripped.refresh_token == "rt"
        assert roundtripped.expires_at == 12345.0


class TestBackwardsCompatAlias:
    def test_idcs_token_fetcher_is_alias(self) -> None:
        """Existing callers using ``IdcsTokenFetcher`` still resolve to ``OacOauthFlow``."""
        assert IdcsTokenFetcher is OacOauthFlow
