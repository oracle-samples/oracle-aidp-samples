"""Unit tests for :mod:`oracle_ai_data_platform_fusion_autopilot.commands.operator_identity`.

Pins the §9.5.9 precedence:

* ``--operator`` wins over both env vars.
* ``AIDP_OPERATOR`` wins over ``USER`` when ``--operator`` is unset.
* ``USER`` is the floor.
* All three empty / whitespace / unset → ``AIDPF-1020``.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.operator_identity import (
    OperatorIdentityUnresolved,
    resolve_operator,
)


class TestPrecedence:
    def test_cli_flag_beats_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIDP_OPERATOR", "from-env")
        monkeypatch.setenv("USER", "from-user")
        assert resolve_operator("from-flag") == "from-flag"

    def test_aidp_operator_beats_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIDP_OPERATOR", "from-env")
        monkeypatch.setenv("USER", "from-user")
        assert resolve_operator(None) == "from-env"

    def test_user_is_the_floor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.setenv("USER", "from-user")
        assert resolve_operator(None) == "from-user"


class TestNormalisation:
    def test_empty_cli_flag_falls_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # AIDP_OPERATOR is checked before USER in the precedence chain; clear it
        # so the CI runner's environment can't leak a value into this assertion.
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.setenv("USER", "fallback")
        # Empty string is treated as "not set"; fall through to USER.
        assert resolve_operator("") == "fallback"

    def test_whitespace_cli_flag_falls_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.setenv("USER", "fallback")
        assert resolve_operator("   \t  ") == "fallback"

    def test_whitespace_env_var_falls_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AIDP_OPERATOR", "   ")
        monkeypatch.setenv("USER", "fallback")
        assert resolve_operator(None) == "fallback"

    def test_returned_value_is_trimmed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.delenv("USER", raising=False)
        assert resolve_operator("  alice@example.com  ") == "alice@example.com"


class TestUnresolved:
    def test_all_unset_raises_aidpf_1020(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.delenv("USER", raising=False)
        with pytest.raises(OperatorIdentityUnresolved) as excinfo:
            resolve_operator(None)
        assert "AIDPF-1020" in str(excinfo.value)
        assert excinfo.value.probed_sources == ["--operator", "AIDP_OPERATOR", "USER"]

    def test_all_empty_raises_aidpf_1020(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AIDP_OPERATOR", "")
        monkeypatch.setenv("USER", "")
        with pytest.raises(OperatorIdentityUnresolved):
            resolve_operator("")

    def test_all_whitespace_raises_aidpf_1020(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AIDP_OPERATOR", " \t ")
        monkeypatch.setenv("USER", "  ")
        with pytest.raises(OperatorIdentityUnresolved):
            resolve_operator("    ")
