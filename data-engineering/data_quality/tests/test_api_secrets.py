"""Integration tests for ``Qualifire`` × ``secret_resolver``.

Covers Phase 2/3 wiring: constructor-time JDBC resolution,
post-validation resolution before storage swap, and that the
caller's ``QualifireConfig`` is not mutated across calls.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from qualifire.api import Qualifire
from qualifire.core.config import (
    EmailNotificationConfig,
    JDBCConfig,
    QualifireConfig,
    SlackNotificationConfig,
    WebhookNotificationConfig,
)
from qualifire.core.exceptions import (
    MissingSecretResolverError,
    QualifireConfigError,
)
from tests.conftest import MockDataFrame


class _StubResolver:
    def __init__(self, values):
        self.values = values
        self.calls = 0

    def get(self, name, key=None):
        self.calls += 1
        return self.values[(name, key)]


def _backend():
    b = MagicMock()
    b.spark = MagicMock()
    b.execute_sql.return_value = MockDataFrame(
        [{"max_ts": datetime(2024, 6, 15, 10, 0, 0)}]
    )
    b.get_aggregations.return_value = {"row_count": 1}
    b.sample_records.return_value = MockDataFrame([{"a": 1}])
    return b


def test_qualifire_stores_secret_resolver():
    resolver = _StubResolver({})
    qf = Qualifire(backend=_backend(), secret_resolver=resolver)
    assert qf._secret_resolver is resolver


def test_qualifire_default_resolver_is_none():
    qf = Qualifire(backend=_backend())
    assert qf._secret_resolver is None


def test_constructor_jdbc_secret_resolved_for_jdbc_backend(monkeypatch):
    """Storage open must see the resolved url, not the literal."""
    captured = {}

    def fake_open_storage(backend, table, *, jdbc=None, spark=None):
        captured["jdbc"] = jdbc
        return MagicMock()

    monkeypatch.setattr("qualifire.storage.factory.open_storage", fake_open_storage)
    resolver = _StubResolver({
        ("prod_db", "url"): "jdbc:postgresql://h/db",
        ("prod_db", "user"): "qf",
        ("prod_db", "pwd"): "PWD",
    })
    Qualifire(
        backend=_backend(),
        system_table="qf.history",
        system_table_backend="jdbc",
        jdbc={
            "url": "secret://prod_db/url",
            "user": "secret://prod_db/user",
            "password": "secret://prod_db/pwd",
        },
        secret_resolver=resolver,
    )
    seen = captured["jdbc"]
    assert seen.url == "jdbc:postgresql://h/db"
    assert seen.user == "qf"
    assert seen.password == "PWD"


def test_constructor_jdbc_with_secret_no_resolver_raises():
    with pytest.raises(MissingSecretResolverError):
        Qualifire(
            backend=_backend(),
            system_table="qf.history",
            system_table_backend="jdbc",
            jdbc={"url": "secret://prod_db/url"},
            # no secret_resolver
        )


def test_constructor_jdbc_skipped_for_non_jdbc_backend(monkeypatch):
    """A templated jdbc block on a non-jdbc backend is left untouched —
    matches the plan's ``_require_jdbc_when_needed`` tolerance."""
    monkeypatch.setattr(
        "qualifire.storage.factory.open_storage",
        lambda *a, **kw: MagicMock(),
    )
    qf = Qualifire(
        backend=_backend(),
        system_table=":memory:",
        system_table_backend="sqlite",
        jdbc={"url": "secret://x/y"},  # would fail to resolve
    )
    # Original literal is preserved — no resolver called, no raise.
    assert qf.jdbc.url == "secret://x/y"


def test_run_config_resolves_notifications_secrets(monkeypatch):
    """End-to-end: YAML-style config with secret refs in notifications;
    `qf.run_config_parsed` resolves before the engine runs."""
    resolver = _StubResolver({
        ("smtp_creds", "pwd"): "RESOLVED-PWD",
        ("slack", "hook"): "https://hooks.slack.com/abc",
        ("api", "tok"): "Bearer xyz",
    })
    qf = Qualifire(
        backend=_backend(),
        system_table=":memory:",
        system_table_backend="sqlite",
        secret_resolver=resolver,
    )
    cfg = QualifireConfig(
        owner="t", bu="b", system_table=":memory:",
        system_table_backend="sqlite",
        notifications={
            "email_alert": EmailNotificationConfig(
                smtp_host="m.x",
                smtp_password="secret://smtp_creds/pwd",
                recipients=["a@x"],
            ),
            "slack_alert": SlackNotificationConfig(
                webhook_url="secret://slack/hook"
            ),
            "wh": WebhookNotificationConfig(
                url="https://w.x",
                headers={"Authorization": "secret://api/tok"},
            ),
        },
        datasets=[],  # no datasets — engine returns empty result
    )

    captured_engine_config = {}

    def fake_engine_init(self, **kw):
        captured_engine_config["config"] = kw["config"]

    def fake_engine_run(self):
        from qualifire.core.models import QualifireResult
        return QualifireResult(owner="t", bu="b", run_id="r")

    monkeypatch.setattr(
        "qualifire.core.engine.QualifireEngine.__init__", fake_engine_init
    )
    monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_engine_run)

    qf.run_config_parsed(cfg)
    seen = captured_engine_config["config"]
    assert seen.notifications["email_alert"].smtp_password == "RESOLVED-PWD"
    assert seen.notifications["slack_alert"].webhook_url == "https://hooks.slack.com/abc"
    assert seen.notifications["wh"].headers == {"Authorization": "Bearer xyz"}


def test_run_config_no_resolver_with_secrets_raises():
    qf = Qualifire(
        backend=_backend(),
        system_table=":memory:",
        system_table_backend="sqlite",
        # no secret_resolver
    )
    cfg = QualifireConfig(
        owner="t", bu="b", system_table=":memory:",
        system_table_backend="sqlite",
        notifications={
            "p": EmailNotificationConfig(
                smtp_host="m", smtp_password="secret://x/y", recipients=["a@x"]
            )
        },
        datasets=[],
    )
    with pytest.raises(MissingSecretResolverError):
        qf.run_config_parsed(cfg)


def test_caller_config_not_mutated_across_calls(monkeypatch):
    """Same parsed config passed twice resolves cleanly both times,
    and the caller's literal references are preserved."""
    resolver = _StubResolver({("x", "y"): "PWD"})
    qf = Qualifire(
        backend=_backend(),
        system_table=":memory:",
        system_table_backend="sqlite",
        secret_resolver=resolver,
    )
    cfg = QualifireConfig(
        owner="t", bu="b", system_table=":memory:",
        system_table_backend="sqlite",
        notifications={
            "p": EmailNotificationConfig(
                smtp_host="m", smtp_password="secret://x/y", recipients=["a@x"]
            )
        },
        datasets=[],
    )

    def fake_init(self, **kw):
        pass

    def fake_run(self):
        from qualifire.core.models import QualifireResult
        return QualifireResult(owner="t", bu="b", run_id="r")

    monkeypatch.setattr(
        "qualifire.core.engine.QualifireEngine.__init__", fake_init
    )
    monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)

    qf.run_config_parsed(cfg)
    qf.run_config_parsed(cfg)
    # Caller's config still carries the literal reference.
    assert cfg.notifications["p"].smtp_password == "secret://x/y"
    # Resolver called twice (once per call) — cache is per-call.
    assert resolver.calls == 2


def test_jdbc_from_secret_pydantic_validator_accepts(monkeypatch):
    """``_require_jdbc_when_needed`` accepts ``from_secret`` as a
    substitute for ``url`` (Codex round-1 finding 4)."""
    monkeypatch.setattr(
        "qualifire.storage.factory.open_storage",
        lambda *a, **kw: MagicMock(),
    )
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="qf.history",
        system_table_backend="jdbc",
        jdbc=JDBCConfig(from_secret="prod_db"),
        datasets=[],
    )
    # No raise from Pydantic — `from_secret` satisfies the gate even
    # though `url` is None.
    assert cfg.jdbc.from_secret == "prod_db"


def test_jdbc_validator_still_rejects_when_neither_url_nor_from_secret():
    with pytest.raises(Exception):  # pydantic ValidationError or QualifireConfigError
        QualifireConfig(
            owner="t", bu="b", system_table="qf.history",
            system_table_backend="jdbc",
            jdbc=JDBCConfig(user="qf"),  # no url, no from_secret
            datasets=[],
        )


def test_templated_jdbc_block_tolerated_on_non_jdbc_backend(monkeypatch):
    """A templated jdbc block carrying secret://... refs alongside a
    non-jdbc backend MUST NOT trigger resolution — operators may keep
    such blocks in YAML for cross-environment templating."""
    qf = Qualifire(
        backend=_backend(),
        system_table=":memory:",
        system_table_backend="sqlite",
        # no secret_resolver
    )
    cfg = QualifireConfig(
        owner="t", bu="b", system_table=":memory:",
        system_table_backend="sqlite",
        jdbc=JDBCConfig(url="secret://prod_db/url"),  # templated, unused
        datasets=[],
    )

    def fake_init(self, **kw):
        pass

    def fake_run(self):
        from qualifire.core.models import QualifireResult
        return QualifireResult(owner="t", bu="b", run_id="r")

    monkeypatch.setattr(
        "qualifire.core.engine.QualifireEngine.__init__", fake_init
    )
    monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)

    # No raise — backend is sqlite, jdbc block is left untouched.
    qf.run_config_parsed(cfg)
    # The original literal is preserved.
    assert cfg.jdbc.url == "secret://prod_db/url"


def test_jdbc_resolves_when_init_storage_called_late(monkeypatch):
    """Codex impl-review round 3 finding 3: when system_table is
    None at construction, the constructor doesn't call _init_storage,
    so JDBC resolution must happen at the next storage-open boundary
    (run_config_parsed). Storage MUST see resolved JDBC."""
    captured: dict = {}

    def fake_open_storage(backend, table, *, jdbc=None, spark=None):
        captured["jdbc"] = jdbc
        return MagicMock()

    monkeypatch.setattr("qualifire.storage.factory.open_storage", fake_open_storage)
    resolver = _StubResolver({
        ("prod_db", "url"): "jdbc:postgresql://h/db",
        ("prod_db", "pwd"): "PWD",
    })
    qf = Qualifire(
        backend=_backend(),
        # NOTE: no system_table at construction → constructor skips
        # _init_storage entirely. Jdbc references stay literal.
        system_table_backend="jdbc",
        jdbc={"url": "secret://prod_db/url", "password": "secret://prod_db/pwd"},
        secret_resolver=resolver,
    )
    # Constructor did not resolve (no system_table), so qf.jdbc still
    # carries the literal ref.
    assert qf.jdbc.url == "secret://prod_db/url"

    # Now run a config that supplies a system_table → triggers
    # _init_storage on the jdbc backend, which must resolve.
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="qf.history",
        # Don't override backend — let instance backend "jdbc" win.
        datasets=[],
    )

    def fake_init(self, **kw):
        pass

    def fake_run(self):
        from qualifire.core.models import QualifireResult
        return QualifireResult(owner="t", bu="b", run_id="r")

    monkeypatch.setattr(
        "qualifire.core.engine.QualifireEngine.__init__", fake_init
    )
    monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)

    qf.run_config_parsed(cfg)
    seen = captured["jdbc"]
    assert seen.url == "jdbc:postgresql://h/db"
    assert seen.password == "PWD"


def test_jdbc_secret_resolves_when_yaml_omits_backend_but_instance_is_jdbc(monkeypatch):
    """Templated YAML (no ``system_table_backend``) + jdbc instance →
    jdbc block must still resolve. Otherwise storage opens with
    `secret://...` literals (Adversarial Round 2 finding 1)."""

    captured = {}

    def fake_open_storage(backend, table, *, jdbc=None, spark=None):
        captured["jdbc"] = jdbc
        return MagicMock()

    monkeypatch.setattr("qualifire.storage.factory.open_storage", fake_open_storage)
    resolver = _StubResolver({
        ("prod_db", "url"): "jdbc:postgresql://h/db",
        ("prod_db", "pwd"): "PWD",
    })
    qf = Qualifire(
        backend=_backend(),
        system_table="qf.history",
        system_table_backend="jdbc",
        jdbc={"url": "jdbc:placeholder://h/db"},  # placeholder; resolved below
        secret_resolver=resolver,
    )
    captured.clear()
    cfg = QualifireConfig(
        owner="t", bu="b",
        system_table="qf.new_history",  # forces a storage swap
        # NOTE: no system_table_backend → instance "jdbc" wins
        jdbc=JDBCConfig(
            url="secret://prod_db/url",
            password="secret://prod_db/pwd",
        ),
        datasets=[],
    )

    def fake_init(self, **kw):
        pass

    def fake_run(self):
        from qualifire.core.models import QualifireResult
        return QualifireResult(owner="t", bu="b", run_id="r")

    monkeypatch.setattr(
        "qualifire.core.engine.QualifireEngine.__init__", fake_init
    )
    monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)
    qf.run_config_parsed(cfg)
    seen = captured["jdbc"]
    assert seen.url == "jdbc:postgresql://h/db"
    assert seen.password == "PWD"


class TestAidpSmokeShape:
    """CI substitute for the manual AIDP smoke (acceptance gate 2.b).

    Cannot fully replace the manual smoke — that requires real
    `aidputils.secrets`, real Slack/SMTP/JDBC backends, and the
    AIDP runtime. But it can verify every end-to-end shape that
    `aidp_smoke.md` lists: the duck-typed resolver path, the
    `from_secret` directive on a JDBC backend, mixed
    `secret://...` references in email/slack/webhook sections, and
    the missing-resolver failure mode at construction time.

    Each test below maps to one bullet in
    docs/features/secrets-resolver-aidputils/aidp_smoke.md.
    """

    def test_full_shape_with_stub_resolver(self, monkeypatch):
        """Mirrors the smoke fixture verbatim: constructor receives no
        `system_table` / `jdbc` (so `_init_storage` does not fire
        during construction); the YAML supplies system_table, backend,
        `jdbc.from_secret`, and per-field refs in email/slack."""
        captured: dict = {}

        def fake_open_storage(backend, table, *, jdbc=None, spark=None):
            captured["jdbc"] = jdbc
            captured["table"] = table
            captured["backend"] = backend
            return MagicMock()

        monkeypatch.setattr("qualifire.storage.factory.open_storage", fake_open_storage)
        resolver = _StubResolver({
            ("qf_smoke_db", None): {
                "url": "jdbc:postgresql://h/qf",
                "user": "qf",
                "password": "PWD",
            },
            ("qf_smoke_smtp", "password"): "SMTP-PWD",
            ("qf_smoke_slack", "webhook_url"): "https://hooks.slack.com/services/T/B/abc",
        })
        # Constructor matches aidp_smoke.md verbatim — no system_table,
        # no jdbc kwarg. Storage opens inside run_config_parsed only.
        qf = Qualifire(
            backend=_backend(),
            secret_resolver=resolver,
            owner="smoke", bu="smoke",
        )
        assert qf._storage is None  # constructor did not open storage

        cfg = QualifireConfig(
            owner="smoke", bu="smoke",
            system_table="qf_smoke_history",
            system_table_backend="jdbc",
            jdbc=JDBCConfig(from_secret="qf_smoke_db"),
            notifications={
                "smoke_email": EmailNotificationConfig(
                    smtp_host="smtp.invalid.example",
                    smtp_password="secret://qf_smoke_smtp/password",
                    sender="smoke@example.com",
                    recipients=["smoke-team@example.com"],
                ),
                "smoke_slack": SlackNotificationConfig(
                    webhook_url="secret://qf_smoke_slack/webhook_url"
                ),
            },
            datasets=[],
        )

        captured_cfg: dict = {}

        def fake_init(self, **kw):
            captured_cfg["config"] = kw["config"]

        def fake_run(self):
            from qualifire.core.models import QualifireResult
            return QualifireResult(owner="smoke", bu="smoke", run_id="r")

        monkeypatch.setattr(
            "qualifire.core.engine.QualifireEngine.__init__", fake_init
        )
        monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)

        qf.run_config_parsed(cfg)
        resolved = captured_cfg["config"]
        # Storage opened with resolved JDBC (proves _init_storage
        # fired with the config-supplied jdbc + from_secret resolved).
        seen_jdbc = captured["jdbc"]
        assert seen_jdbc.url == "jdbc:postgresql://h/qf"
        assert seen_jdbc.user == "qf"
        assert seen_jdbc.password == "PWD"
        assert seen_jdbc.from_secret is None
        assert captured["backend"] == "jdbc"
        # Per-field refs resolved.
        assert resolved.notifications["smoke_email"].smtp_password == "SMTP-PWD"
        assert (
            resolved.notifications["smoke_slack"].webhook_url
            == "https://hooks.slack.com/services/T/B/abc"
        )

    def test_no_secret_literals_remain_in_resolved_config(self, monkeypatch):
        """First smoke acceptance bullet: no `secret://` substrings
        survive in the resolved config the engine sees."""
        monkeypatch.setattr(
            "qualifire.storage.factory.open_storage",
            lambda *a, **kw: MagicMock(),
        )
        resolver = _StubResolver({
            ("smtp", "pwd"): "real-pwd",
            ("slack", "url"): "https://hooks.slack.com/services/T/B/secret-token",
        })
        qf = Qualifire(
            backend=_backend(),
            system_table=":memory:",
            system_table_backend="sqlite",
            secret_resolver=resolver,
        )
        cfg = QualifireConfig(
            owner="t", bu="b", system_table=":memory:",
            system_table_backend="sqlite",
            notifications={
                "e": EmailNotificationConfig(
                    smtp_host="m", smtp_password="secret://smtp/pwd",
                    recipients=["a@b"],
                ),
                "s": SlackNotificationConfig(webhook_url="secret://slack/url"),
            },
            datasets=[],
        )

        captured_cfg: dict = {}

        def fake_init(self, **kw):
            captured_cfg["config"] = kw["config"]

        def fake_run(self):
            from qualifire.core.models import QualifireResult
            return QualifireResult(owner="t", bu="b", run_id="r")

        monkeypatch.setattr(
            "qualifire.core.engine.QualifireEngine.__init__", fake_init
        )
        monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)
        qf.run_config_parsed(cfg)
        # No `secret://` substring anywhere in any walked field.
        resolved = captured_cfg["config"]
        for entry in resolved.notifications.values():
            for field in ("smtp_password", "webhook_url"):
                v = getattr(entry, field, None)
                if v is not None:
                    assert "secret://" not in v

    def test_construction_without_resolver_raises_with_clear_message(self):
        """Smoke checklist bullet 4: same fixture without
        `secret_resolver=` raises MissingSecretResolverError with the
        offending reference and field path."""
        with pytest.raises(MissingSecretResolverError) as excinfo:
            Qualifire(
                backend=_backend(),
                system_table="qf.history",
                system_table_backend="jdbc",
                jdbc=JDBCConfig(from_secret="qf_smoke_db"),
                # no secret_resolver
            )
        msg = str(excinfo.value)
        assert "qf_smoke_db" in msg
        assert "secret_resolver" in msg

    def test_resolved_jdbc_url_is_real_url_not_directive(self, monkeypatch):
        """Smoke checklist bullet 5: `qf.jdbc.url` after construction
        is a real `jdbc:` URL, not `from_secret`."""
        monkeypatch.setattr(
            "qualifire.storage.factory.open_storage",
            lambda *a, **kw: MagicMock(),
        )
        resolver = _StubResolver({
            ("prod_db", None): {"url": "jdbc:postgresql://h/db", "user": "u"},
        })
        qf = Qualifire(
            backend=_backend(),
            system_table="qf.history",
            system_table_backend="jdbc",
            jdbc=JDBCConfig(from_secret="prod_db"),
            secret_resolver=resolver,
        )
        assert qf.jdbc.url == "jdbc:postgresql://h/db"
        assert qf.jdbc.from_secret is None

    def test_duck_typed_resolver_no_inheritance(self, monkeypatch):
        """Acceptance criterion 2.a / 7: a 5-line resolver class
        works without inheriting from Qualifire."""
        monkeypatch.setattr(
            "qualifire.storage.factory.open_storage",
            lambda *a, **kw: MagicMock(),
        )

        class FiveLineResolver:
            def get(self, name, key=None):
                return {"x": "RESOLVED"}[key] if key else None

        qf = Qualifire(
            backend=_backend(),
            system_table=":memory:",
            system_table_backend="sqlite",
            secret_resolver=FiveLineResolver(),
        )
        cfg = QualifireConfig(
            owner="t", bu="b", system_table=":memory:",
            notifications={
                "p": EmailNotificationConfig(
                    smtp_host="m", smtp_password="secret://x/x", recipients=["a@b"]
                )
            },
            datasets=[],
        )

        captured: dict = {}

        def fake_init(self, **kw):
            captured["config"] = kw["config"]

        def fake_run(self):
            from qualifire.core.models import QualifireResult
            return QualifireResult(owner="t", bu="b", run_id="r")

        monkeypatch.setattr(
            "qualifire.core.engine.QualifireEngine.__init__", fake_init
        )
        monkeypatch.setattr("qualifire.core.engine.QualifireEngine.run", fake_run)
        qf.run_config_parsed(cfg)
        assert captured["config"].notifications["p"].smtp_password == "RESOLVED"


def test_malformed_secret_ref_in_yaml_raises_secret_resolution_error():
    """A malformed `secret://` literal must surface as
    `SecretResolutionError` (a `QualifireConfigError` subclass), not
    as a bare `ValueError` deep in the parser."""
    from qualifire.core.exceptions import SecretResolutionError

    qf = Qualifire(
        backend=_backend(),
        system_table=":memory:",
        system_table_backend="sqlite",
        secret_resolver=_StubResolver({}),
    )
    cfg = QualifireConfig(
        owner="t", bu="b", system_table=":memory:",
        system_table_backend="sqlite",
        notifications={
            "p": EmailNotificationConfig(
                smtp_host="m",
                smtp_password="secret://name",  # missing key — invalid
                recipients=["a@x"],
            )
        },
        datasets=[],
    )
    with pytest.raises(SecretResolutionError, match="secret_password|smtp_password"):
        qf.run_config_parsed(cfg)
