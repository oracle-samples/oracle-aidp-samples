"""Tests for the ``Qualifire.from_config`` classmethod factory and
related machinery: the ``_from_parsed_config`` private classmethod,
the ``_effective_notifiers`` helper, the ``_swap_storage_if_needed``
helper, and the ``QualifireReinitWarning`` emission policy.

Notifier precedence is single-layer: ``self._notifiers`` (constructor
+ ``register_notifier``) always wins over a YAML ``notifications:``
block of the same name. ``warn_on_reinit=True`` is the default for
every construction path (no legacy silent mode).

Coverage maps 1:1 to the plan's Step 5 acceptance list.
"""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import pytest

from qualifire.api import Qualifire
from qualifire.core.config import QualifireConfig, load_config
from qualifire.core.exceptions import (
    QualifireConfigError,
    QualifireReinitWarning,
)


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _write_yaml(tmp_path, name: str, body: str) -> str:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return str(p)


@pytest.fixture
def basic_yaml(tmp_path):
    """Minimal SQLite-backed YAML — no Spark required."""
    return _write_yaml(
        tmp_path,
        "basic.yaml",
        "owner: demo_owner\n"
        "bu: demo_bu\n"
        f"system_table: {tmp_path / 'qf_history.sqlite'}\n"
        "system_table_backend: sqlite\n"
        "datasets: []\n",
    )


@pytest.fixture
def alt_yaml(tmp_path):
    """Same shape, different system_table — used for reinit tests."""
    return _write_yaml(
        tmp_path,
        "alt.yaml",
        "owner: demo_owner\n"
        "bu: demo_bu\n"
        f"system_table: {tmp_path / 'qf_history_alt.sqlite'}\n"
        "system_table_backend: sqlite\n"
        "datasets: []\n",
    )


@pytest.fixture
def yaml_with_inbox_slack(tmp_path):
    """YAML declaring an ``inbox`` notifier with a Slack-typed config —
    used to verify programmatic-overrides win over YAML notifications."""
    return _write_yaml(
        tmp_path,
        "with_inbox.yaml",
        "owner: demo_owner\n"
        "bu: demo_bu\n"
        f"system_table: {tmp_path / 'qf_history_notif.sqlite'}\n"
        "system_table_backend: sqlite\n"
        "notifications:\n"
        "  inbox:\n"
        "    type: slack\n"
        "    webhook_url: https://hooks.slack.example/X/Y/Z\n"
        "datasets: []\n",
    )


# ---------------------------------------------------------------------
# 1. Minimum signature — only (path, backend)
# ---------------------------------------------------------------------


class TestMinimum:
    def test_from_config_minimum(self, basic_yaml):
        qf = Qualifire.from_config(basic_yaml, backend=None)
        assert qf.owner == "demo_owner"
        assert qf.bu == "demo_bu"
        assert qf.system_table_backend == "sqlite"
        assert qf.system_table.endswith("qf_history.sqlite")
        assert qf.jdbc is None
        # ``warn_on_reinit`` defaults True for every construction
        # path (no legacy support for silent reinit).
        assert qf._warn_on_reinit is True
        # ``console`` is the only auto-registered default notifier —
        # operators routing ``notify: ["console"]`` get a
        # print-to-stdout channel without registering anything
        # explicitly. No other channels (slack/email/webhook)
        # auto-instantiate; those still require a YAML
        # ``notifications:`` block or a ``register_notifier`` call.
        from qualifire.notification.console_notifier import ConsoleNotifier
        assert set(qf._notifiers) == {"console"}
        assert isinstance(qf._notifiers["console"], ConsoleNotifier)

    def test_direct_init_without_yaml(self, tmp_path):
        """Pure-programmatic construction (no YAML, no
        ``from_config``) — operator supplies all instance fields
        explicitly. This path is the no-YAML escape hatch and must
        keep working without invoking the factory."""
        qf = Qualifire(
            backend=None,
            system_table=str(tmp_path / "qf.sqlite"),
            system_table_backend="sqlite",
            owner="prog_owner",
            bu="prog_bu",
        )
        assert qf.owner == "prog_owner"
        assert qf.bu == "prog_bu"
        assert qf.system_table_backend == "sqlite"
        # ``console`` is the only auto-registered default notifier —
        # operators routing ``notify: ["console"]`` get a
        # print-to-stdout channel without registering anything
        # explicitly. No other channels (slack/email/webhook)
        # auto-instantiate; those still require a YAML
        # ``notifications:`` block or a ``register_notifier`` call.
        from qualifire.notification.console_notifier import ConsoleNotifier
        assert set(qf._notifiers) == {"console"}
        assert isinstance(qf._notifiers["console"], ConsoleNotifier)
        # Default warn_on_reinit also True for direct __init__
        # (single contract everywhere).
        assert qf._warn_on_reinit is True


# ---------------------------------------------------------------------
# 2. Overrides win over YAML
# ---------------------------------------------------------------------


class TestOverrides:
    def test_overrides_win(self, basic_yaml):
        qf = Qualifire.from_config(
            basic_yaml,
            backend=None,
            owner="override_owner",
            bu="override_bu",
        )
        assert qf.owner == "override_owner"
        assert qf.bu == "override_bu"

    def test_unknown_override_raises_typeerror(self, basic_yaml):
        with pytest.raises(TypeError, match="bogus"):
            Qualifire.from_config(basic_yaml, backend=None, bogus="x")

    def test_notifications_not_an_override_key(self, basic_yaml):
        """`notifications` is a dict-typed YAML field whose merge
        semantics would be ambiguous as an override. The factory
        rejects it explicitly so operators learn to use
        ``notifiers={}`` for runtime instances."""
        with pytest.raises(TypeError, match="notifications"):
            Qualifire.from_config(
                basic_yaml, backend=None,
                notifications={"inbox": {"type": "slack"}},
            )


# ---------------------------------------------------------------------
# 3. Notifier merge — programmatic always wins
# ---------------------------------------------------------------------


class TestNotifierProgrammaticAlwaysWins:
    def test_runtime_notifier_lands_in_self_notifiers(self, yaml_with_inbox_slack):
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            yaml_with_inbox_slack,
            backend=None,
            notifiers={"inbox": captured},
        )
        assert qf._notifiers["inbox"] is captured

    def test_effective_notifiers_no_yaml_path(self, yaml_with_inbox_slack):
        """`validate_query` / `write_audit_publish` / no-config
        `validate` resolve via `_effective_notifiers(None)`. Only
        ``self._notifiers`` is in scope."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            yaml_with_inbox_slack,
            backend=None,
            notifiers={"inbox": captured},
        )
        merged = qf._effective_notifiers()
        assert merged["inbox"] is captured

    def test_effective_notifiers_yaml_path_programmatic_wins(
        self, yaml_with_inbox_slack
    ):
        """Codex R1 MUST-FIX 1 + R2 SHOULD-FIX 3 regression guard.
        Even when YAML notifications are present, programmatic
        instances win — they're applied LAST in the merge."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            yaml_with_inbox_slack,
            backend=None,
            notifiers={"inbox": captured},
        )
        config = load_config(yaml_with_inbox_slack)
        merged = qf._effective_notifiers(config.notifications)
        # YAML-built Slack notifier filled first, then programmatic
        # capture wins.
        assert merged["inbox"] is captured

    def test_register_notifier_post_construction_wins(
        self, yaml_with_inbox_slack,
    ):
        """`register_notifier` writes to ``self._notifiers`` and
        thus participates in the always-wins layer. Operators can
        swap notifiers post-construction and the swap survives
        every subsequent ``run_config`` call."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(yaml_with_inbox_slack, backend=None)
        # Initially, no programmatic notifiers — YAML provides inbox
        # at run time.
        assert "inbox" not in qf._notifiers
        qf.register_notifier("inbox", captured)
        assert qf._notifiers["inbox"] is captured

        config = load_config(yaml_with_inbox_slack)
        merged = qf._effective_notifiers(config.notifications)
        assert merged["inbox"] is captured

    def test_no_raw_self_notifiers_left_in_engine_paths(self):
        """Static guard: every engine handoff in api.py must use
        `_effective_notifiers(...)`, not raw `self._notifiers`.
        If a future change adds a new engine entrypoint that bypasses
        the helper, this test fails before runtime."""
        import qualifire.api as api_mod
        src = open(api_mod.__file__, encoding="utf-8").read()
        # No `notifiers=self._notifiers` (raw kwarg) should remain.
        assert "notifiers=self._notifiers" not in src, (
            "Found a raw `notifiers=self._notifiers` handoff to the "
            "engine — bypasses YAML-merge. Use "
            "`notifiers=self._effective_notifiers(...)` instead."
        )


class TestProgrammaticAlwaysWinsAtRunConfig:
    def test_programmatic_wins_at_engine_handoff_run_config(
        self, yaml_with_inbox_slack
    ):
        """End-to-end at the engine handoff: ``qf.run_config(yaml)``
        sees the captured notifier for ``inbox`` even though YAML
        declares it as ``type: slack``."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            yaml_with_inbox_slack,
            backend=None,
            notifiers={"inbox": captured},
        )
        config = load_config(yaml_with_inbox_slack)
        with patch("qualifire.api.QualifireEngine") as engine_cls:
            engine_inst = engine_cls.return_value
            engine_inst.run.return_value = MagicMock(
                overall_severity=MagicMock(value="PASS"),
                datasets=[],
            )
            qf.run_config_parsed(config)

        engine_kwargs = engine_cls.call_args.kwargs
        assert engine_kwargs["notifiers"]["inbox"] is captured

    def test_programmatic_wins_at_engine_handoff_validate(
        self, basic_yaml,
    ):
        """Codex impl-R2 SHOULD-FIX 2: real engine-handoff coverage
        for programmatic paths. ``qf.validate(...)`` calls the
        engine with ``notifiers=self._effective_notifiers()`` (no
        YAML in scope). The override notifier must reach the
        engine kwargs."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            basic_yaml, backend=None, notifiers={"inbox": captured},
        )
        with patch("qualifire.api.QualifireEngine") as engine_cls:
            engine_cls.return_value.run.return_value = MagicMock(
                overall_severity=MagicMock(value="PASS"),
                datasets=[],
            )
            try:
                qf.validate(
                    table="t", validations=[],
                    name="ds", partition_ts="2026-01-01",
                )
            except Exception:
                pass

        engine_kwargs = engine_cls.call_args.kwargs
        assert engine_kwargs["notifiers"]["inbox"] is captured

    def test_programmatic_wins_at_engine_handoff_validate_query(
        self, basic_yaml,
    ):
        """Codex impl-R2 SHOULD-FIX 2: real engine-handoff coverage
        for ``qf.validate_query``."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            basic_yaml, backend=None, notifiers={"inbox": captured},
        )
        with patch("qualifire.api.QualifireEngine") as engine_cls:
            engine_cls.return_value.run.return_value = MagicMock(
                overall_severity=MagicMock(value="PASS"),
                datasets=[],
            )
            try:
                qf.validate_query(
                    query="SELECT 1", validations=[], name="q",
                )
            except Exception:
                pass

        engine_kwargs = engine_cls.call_args.kwargs
        assert engine_kwargs["notifiers"]["inbox"] is captured

    def test_programmatic_wins_at_engine_handoff_wap(
        self, basic_yaml,
    ):
        """Codex impl-R2 SHOULD-FIX 2: real engine-handoff coverage
        for ``qf.write_audit_publish``."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            basic_yaml, backend=None, notifiers={"inbox": captured},
        )
        with patch("qualifire.api.QualifireEngine") as engine_cls:
            engine_cls.return_value.run.return_value = MagicMock(
                overall_severity=MagicMock(value="PASS"),
                datasets=[],
            )
            try:
                qf.write_audit_publish(
                    target_table="cat.t",
                    sql="SELECT 1",
                    validations=[],
                )
            except Exception:
                pass

        engine_kwargs = engine_cls.call_args.kwargs
        assert engine_kwargs["notifiers"]["inbox"] is captured

    def test_programmatic_wins_at_backfill_handoff(
        self, basic_yaml,
    ):
        """Codex impl-R2 SHOULD-FIX 2: real handoff coverage for
        ``qf.backfill(config=...)``. The driver receives the
        merged ``notifiers`` dict including the captured override."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire.from_config(
            basic_yaml, backend=None, notifiers={"inbox": captured},
        )
        with patch("qualifire.core.backfill.run_backfill") as run_bf:
            run_bf.return_value = MagicMock(
                has_errors=False,
                refreshed=0, unchanged=0, skipped=0, errored=0,
            )
            try:
                qf.backfill(
                    config=basic_yaml, partition_ts="2026-01-01",
                )
            except Exception:
                pass

        bf_kwargs = run_bf.call_args.kwargs
        assert bf_kwargs["notifiers"]["inbox"] is captured


# ---------------------------------------------------------------------
# 4. Direct __init__ backward compat
# ---------------------------------------------------------------------


class TestDirectInitProgrammaticAlsoWins:
    def test_direct_init_with_notifiers_kwarg_wins_over_yaml(
        self, yaml_with_inbox_slack
    ):
        """``Qualifire(notifiers={...})`` flows into
        ``self._notifiers``. YAML's ``notifications:`` block with
        the same name does NOT clobber — programmatic always wins
        (single contract; no legacy mode)."""
        captured = MagicMock(name="CapturingNotifier")
        qf = Qualifire(
            backend=None,
            system_table="x",  # placeholder; overridden by run_config
            system_table_backend="sqlite",
            notifiers={"inbox": captured},
        )
        assert qf._notifiers["inbox"] is captured
        config = load_config(yaml_with_inbox_slack)
        merged = qf._effective_notifiers(config.notifications)
        assert merged["inbox"] is captured


# ---------------------------------------------------------------------
# 5. Reinit warnings
# ---------------------------------------------------------------------


class TestReinitWarnings:
    def test_reinit_warns_on_run_config(self, basic_yaml, alt_yaml):
        qf = Qualifire.from_config(basic_yaml, backend=None)
        config = load_config(alt_yaml)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                qf._swap_storage_if_needed(config)
            except Exception:
                # Storage init may raise on missing SQLite path; the
                # warning is what we're asserting on, not the swap
                # itself.
                pass
            reinit = [
                w for w in caught
                if issubclass(w.category, QualifireReinitWarning)
            ]
            assert len(reinit) == 1
            msg = str(reinit[0].message)
            assert "system_table" in msg

    def test_reinit_silent_when_warn_off(self, basic_yaml, alt_yaml):
        qf = Qualifire.from_config(
            basic_yaml, backend=None, warn_on_reinit=False,
        )
        config = load_config(alt_yaml)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                qf._swap_storage_if_needed(config)
            except Exception:
                pass
            reinit = [
                w for w in caught
                if issubclass(w.category, QualifireReinitWarning)
            ]
            assert len(reinit) == 0

    def test_direct_init_warns_on_reinit_by_default(
        self, alt_yaml, tmp_path,
    ):
        """Single contract: every construction path defaults to
        ``warn_on_reinit=True``. Direct ``Qualifire(...)`` callers
        also see the warning when storage swaps. No legacy silent
        mode — opt out per call by passing
        ``warn_on_reinit=False``."""
        qf = Qualifire(
            backend=None,
            system_table=str(tmp_path / "decoy.sqlite"),
            system_table_backend="sqlite",
            owner="x", bu="y",
        )
        config = load_config(alt_yaml)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                qf._swap_storage_if_needed(config)
            except Exception:
                pass
            reinit = [
                w for w in caught
                if issubclass(w.category, QualifireReinitWarning)
            ]
            assert len(reinit) == 1

    def test_reinit_warns_via_resolve_storage_for_op(
        self, basic_yaml, alt_yaml
    ):
        """Codex R2 MUST-FIX 2 regression guard. ``backfill`` and
        ``deactivate_metric`` go through ``_resolve_storage_for_op``,
        which MUST also fire the reinit warning. We can't run the
        full backfill (no datasets), so we exercise the helper
        directly."""
        qf = Qualifire.from_config(basic_yaml, backend=None)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                qf._resolve_storage_for_op(alt_yaml)
            except Exception:
                pass
            reinit = [
                w for w in caught
                if issubclass(w.category, QualifireReinitWarning)
            ]
            assert len(reinit) == 1

    def test_reinit_warns_via_public_run_config(
        self, basic_yaml, alt_yaml
    ):
        """Codex impl-R1 SHOULD-FIX 2: public-path coverage.
        ``qf.run_config(other_yaml)`` exercises the full path
        (``run_config`` → ``run_config_parsed`` →
        ``_swap_storage_if_needed``). The warning must fire end-
        to-end. Engine.run is stubbed so we don't need real
        validations."""
        qf = Qualifire.from_config(basic_yaml, backend=None)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with patch("qualifire.api.QualifireEngine") as engine_cls:
                engine_cls.return_value.run.return_value = MagicMock(
                    overall_severity=MagicMock(value="PASS"),
                    datasets=[],
                )
                # Storage init may fail (alt_yaml's path may not
                # exist yet); we only care about the warning.
                try:
                    qf.run_config(alt_yaml)
                except Exception:
                    pass
            reinit = [
                w for w in caught
                if issubclass(w.category, QualifireReinitWarning)
            ]
            assert len(reinit) == 1, (
                f"expected 1 reinit warning via public run_config; "
                f"got {len(reinit)}"
            )

    def test_backfill_loads_config_path_only_once(
        self, basic_yaml,
    ):
        """Codex impl-R2 MUST-FIX 1: ``Qualifire.backfill(config="path")``
        previously read the YAML twice — once inside
        ``_resolve_storage_for_op`` and again before ``run_backfill``.
        After the fix the path is normalized once at the top of
        ``backfill``; ``_resolve_storage_for_op`` receives the
        parsed object."""
        from qualifire.core import config as config_module

        qf = Qualifire.from_config(basic_yaml, backend=None)

        calls = {"load_config": 0}
        real = config_module.load_config

        def counting(p):
            calls["load_config"] += 1
            return real(p)

        # Patch both the public binding and the alias used inside
        # api.py (the module imports it as ``load_config`` directly).
        with patch.object(config_module, "load_config", counting), \
             patch("qualifire.api.load_config", counting), \
             patch("qualifire.core.backfill.run_backfill") as run_bf:
            run_bf.return_value = MagicMock(
                has_errors=False,
                refreshed=0, unchanged=0, skipped=0, errored=0,
            )
            try:
                qf.backfill(
                    config=basic_yaml,
                    partition_ts="2026-01-01",
                )
            except Exception:
                # Empty datasets list may surface a different error
                # path; we only care about the file-read count.
                pass

        assert calls["load_config"] == 1, (
            f"Qualifire.backfill loaded the YAML "
            f"{calls['load_config']} times; expected exactly 1 "
            f"(TOCTOU guard)."
        )

    def test_reinit_warns_via_public_deactivate_metric(
        self, basic_yaml, alt_yaml
    ):
        """Codex impl-R1 SHOULD-FIX 2: public-path coverage.
        ``qf.deactivate_metric(other_yaml, ...)`` exercises the
        ``_resolve_storage_for_op`` path. Stub the deactivate
        worker so the test focuses on the warning."""
        qf = Qualifire.from_config(basic_yaml, backend=None)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with patch("qualifire.core.deactivate.deactivate_metric") as do_deact:
                do_deact.return_value = 1
                try:
                    qf.deactivate_metric(
                        config=alt_yaml,
                        dataset_name="d",
                        metric_name="m",
                    )
                except Exception:
                    pass
            reinit = [
                w for w in caught
                if issubclass(w.category, QualifireReinitWarning)
            ]
            assert len(reinit) == 1, (
                f"expected 1 reinit warning via public "
                f"deactivate_metric; got {len(reinit)}"
            )


# ---------------------------------------------------------------------
# 6. Zero double-init
# ---------------------------------------------------------------------


class TestZeroDoubleInit:
    def test_from_config_then_run_config_initializes_once(
        self, basic_yaml,
    ):
        """Codex R1 SHOULD-FIX 4 + R2 SHOULD-FIX 2 lock-in.
        Construct first, then wrap the bound method (binding-safe).
        Two-phase assertion: 1 call after construction, 1 call still
        after first run."""
        qf = Qualifire.from_config(basic_yaml, backend=None)

        original = qf._init_storage
        spy = MagicMock(wraps=original)
        qf._init_storage = spy

        # Phase A: nothing happened yet — construction already
        # completed before we wrapped.
        assert spy.call_count == 0

        # Spying after construction only lets us measure the *re*-init
        # path. Run the same YAML; with matching system_table /
        # backend / jdbc, _swap_storage_if_needed must NOT call
        # _init_storage.
        config = load_config(basic_yaml)
        with patch("qualifire.api.QualifireEngine") as engine_cls:
            engine_cls.return_value.run.return_value = MagicMock(
                overall_severity=MagicMock(value="PASS"), datasets=[],
            )
            qf.run_config_parsed(config)

        assert spy.call_count == 0, (
            f"Expected zero re-init calls; got {spy.call_count}: "
            f"{spy.call_args_list}"
        )


# ---------------------------------------------------------------------
# 8. Secret-resolver timing (split per codex R2 SHOULD-FIX 1)
# ---------------------------------------------------------------------


class TestSecretResolverTiming:
    def test_notification_secrets_lazy(self, tmp_path):
        """YAML's ``notifications:`` block carries ``secret://...``;
        ``from_config`` does NOT pre-build YAML notifiers, so the
        resolver is not called at construction time. Resolution
        happens lazily on the first ``run_config_parsed`` call.
        ``SecretResolver`` is a Protocol with ``.get(name, key=...)``
        — see ``qualifire.core.secrets``."""
        yaml = _write_yaml(
            tmp_path,
            "lazy.yaml",
            "owner: o\nbu: b\n"
            f"system_table: {tmp_path / 'qf.sqlite'}\n"
            "system_table_backend: sqlite\n"
            "notifications:\n"
            "  inbox:\n"
            "    type: webhook\n"
            "    url: secret://prod/webhook\n"
            "datasets: []\n",
        )

        class TrackingResolver:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | None]] = []

            def get(self, name: str, key: str | None = None):
                self.calls.append((name, key))
                return "https://resolved.example/hook"

        resolver = TrackingResolver()
        qf = Qualifire.from_config(
            yaml, backend=None, secret_resolver=resolver,
        )
        # ``from_config`` must NOT have triggered notification secret
        # resolution — YAML notifiers stay deferred.
        assert resolver.calls == [], (
            f"resolver.get called eagerly: {resolver.calls}"
        )
        # Stash on the instance for the run-time path
        assert qf._secret_resolver is resolver

    def test_jdbc_secrets_eager(self, tmp_path):
        """For ``system_table_backend == "jdbc"`` with a
        ``secret://`` reference inside the ``jdbc:`` block, secret
        resolution must happen eagerly during construction (storage
        opens during ``__init__``). Existing behaviour preserved."""
        yaml = _write_yaml(
            tmp_path,
            "jdbc.yaml",
            "owner: o\nbu: b\n"
            "system_table: qf_history\n"
            "system_table_backend: jdbc\n"
            "jdbc:\n"
            "  url: jdbc:postgresql://unused/db\n"
            "  user: u\n"
            "  password: secret://prod/pw\n"
            "datasets: []\n",
        )

        class TrackingResolver:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | None]] = []

            def get(self, name: str, key: str | None = None):
                self.calls.append((name, key))
                return "resolved-pw"

        resolver = TrackingResolver()
        # Storage open may fail (no real DB); we only care that the
        # resolver was invoked before that failure.
        with pytest.raises(Exception):
            Qualifire.from_config(
                yaml, backend=None, secret_resolver=resolver,
            )
        assert ("prod", "pw") in resolver.calls, (
            f"JDBC password secret not resolved eagerly: "
            f"{resolver.calls}"
        )


# ---------------------------------------------------------------------
# 9. ``_from_parsed_config`` parity
# ---------------------------------------------------------------------


class TestFromParsedConfig:
    def test_from_parsed_config_parity_with_from_config(
        self, basic_yaml,
    ):
        """The two factories must produce instances with the same
        instance-level fields. ``_from_parsed_config`` is the
        TOCTOU-safe internal CLI path; ``from_config`` is the
        public wrapper that calls ``load_config(path)`` first."""
        qf_path = Qualifire.from_config(basic_yaml, backend=None)
        config = load_config(basic_yaml)
        qf_parsed = Qualifire._from_parsed_config(config, backend=None)
        for field in (
            "owner", "bu", "system_table", "system_table_backend",
            "jdbc",
        ):
            assert getattr(qf_path, field) == getattr(qf_parsed, field)
        assert qf_path._warn_on_reinit == qf_parsed._warn_on_reinit


# ---------------------------------------------------------------------
# 10. ``register_notifier`` post-construction semantics
# ---------------------------------------------------------------------


class TestRegisterNotifierPostConstruction:
    def test_register_replaces_constructor_notifier(
        self, yaml_with_inbox_slack,
    ):
        """Single-layer simplification: ``register_notifier`` writes
        to ``self._notifiers`` (the always-wins layer), so calling
        ``register_notifier('inbox', replacement)`` after
        ``from_config(yaml, notifiers={'inbox': original})`` REPLACES
        ``original``. The next ``run_config`` resolves ``inbox`` to
        ``replacement``."""
        original = MagicMock(name="OriginalNotifier")
        replacement = MagicMock(name="ReplacementNotifier")
        qf = Qualifire.from_config(
            yaml_with_inbox_slack,
            backend=None,
            notifiers={"inbox": original},
        )
        qf.register_notifier("inbox", replacement)
        assert qf._notifiers["inbox"] is replacement

        merged = qf._effective_notifiers()
        assert merged["inbox"] is replacement
