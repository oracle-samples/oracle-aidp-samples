"""Unit tests for ``qualifire.core.secrets``.

Covers Phase 1–4 of the secrets-resolver-aidputils feature:
parser, allowlist walker, type guards, idempotency,
caller-config preservation, ``from_secret`` directive,
and the resolver-failure no-leak contract.
"""

from __future__ import annotations

import traceback

import pytest

from qualifire.core.config import (
    EmailNotificationConfig,
    JDBCConfig,
    QualifireConfig,
    SlackNotificationConfig,
    WebhookNotificationConfig,
)
from qualifire.core.exceptions import (
    MissingSecretResolverError,
    SecretResolutionError,
)
from qualifire.core.secrets import (
    _build_walkers,
    is_secret_ref,
    parse_secret_ref,
    resolve_secrets,
)


class _StubResolver:
    """Five-line duck-typed resolver — also serves as the
    "resolver Protocol is duck-typed" acceptance test."""

    def __init__(self, values: dict[tuple[str, str | None], object]):
        self.values = values
        self.calls = 0

    def get(self, name, key=None):
        self.calls += 1
        return self.values[(name, key)]


def _config_with_email(**kwargs) -> QualifireConfig:
    return QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={"prod": EmailNotificationConfig(smtp_host="mail.x", **kwargs)},
        datasets=[],
    )


# --- parse_secret_ref ---


class TestParseSecretRef:
    def test_valid_with_key(self):
        assert parse_secret_ref("secret://prod_db/password") == ("prod_db", "password")

    def test_valid_key_with_slash(self):
        # Decision Pin 1: key may contain '/'.
        assert parse_secret_ref("secret://prod_db/team/api/key") == (
            "prod_db",
            "team/api/key",
        )

    def test_not_a_ref_returns_none(self):
        assert parse_secret_ref("plain-string") is None
        assert parse_secret_ref("https://example.com") is None

    @pytest.mark.parametrize(
        "bad",
        [
            "secret://name",  # no key — only `from_secret` is whole-secret
            "secret://",
            "secret:///key",  # empty name
            "secret://name/",  # empty key
            "secret://na me/key",  # space in name
            "secret://name/k ey",  # space in key
        ],
    )
    def test_malformed_raises(self, bad):
        with pytest.raises(ValueError):
            parse_secret_ref(bad)

    def test_split_on_first_slash(self):
        """`secret://name/path/with/slash` splits into name + path-key."""
        assert parse_secret_ref("secret://team/prod_db/password") == (
            "team",
            "prod_db/password",
        )


def test_is_secret_ref():
    assert is_secret_ref("secret://a/b") is True
    assert is_secret_ref("plain") is False
    assert is_secret_ref(None) is False
    assert is_secret_ref(123) is False


# --- Walker registry exhaustiveness (Codex r3 finding 5) ---


def test_walker_registry_covers_every_notification_type():
    walkers = _build_walkers()
    assert EmailNotificationConfig in walkers
    assert SlackNotificationConfig in walkers
    assert WebhookNotificationConfig in walkers


def test_unknown_notification_type_fails_fast(monkeypatch):
    class FakeNotifier:
        def model_copy(self, deep=False, update=None):
            return self

    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={"prod": EmailNotificationConfig(smtp_host="mail.x", recipients=["a@b"])},
        datasets=[],
    )
    # Inject a non-registered notifier instance after construction.
    cfg.notifications["weird"] = FakeNotifier()  # type: ignore[assignment]
    with pytest.raises(SecretResolutionError, match="no secret walker registered"):
        resolve_secrets(cfg, _StubResolver({}))


# --- Walker basics ---


def test_walker_email_per_field_resolution():
    cfg = _config_with_email(
        smtp_user="qf",
        smtp_password="secret://smtp_creds/pwd",
        sender="secret://smtp_creds/from_addr",
        recipients=["data@x"],
    )
    resolver = _StubResolver({
        ("smtp_creds", "pwd"): "RESOLVED-PWD",
        ("smtp_creds", "from_addr"): "qualifire@x",
    })

    resolved, _ = resolve_secrets(cfg, resolver)
    new_email = resolved.notifications["prod"]
    assert new_email.smtp_password == "RESOLVED-PWD"
    assert new_email.sender == "qualifire@x"
    # smtp_user wasn't a ref — passes through.
    assert new_email.smtp_user == "qf"
    # recipients deliberately NOT walked (Decision Pin 6).
    assert new_email.recipients == ["data@x"]


def test_walker_recipients_not_walked():
    cfg = _config_with_email(
        recipients=["secret://smtp_creds/to"],
    )
    resolver = _StubResolver({("smtp_creds", "to"): "should-not-be-used"})
    resolved, _ = resolve_secrets(cfg, resolver)
    # The literal `secret://...` is preserved verbatim because
    # recipients is not in the allowlist.
    assert resolved.notifications["prod"].recipients == ["secret://smtp_creds/to"]
    assert resolver.calls == 0


def test_walker_webhook_headers_dict_values():
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={
            "wh": WebhookNotificationConfig(
                url="secret://api_creds/url",
                headers={"Authorization": "secret://api_creds/token"},
            )
        },
        datasets=[],
    )
    resolver = _StubResolver({
        ("api_creds", "url"): "https://hooks.x/abc",
        ("api_creds", "token"): "Bearer xyz",
    })
    resolved, _ = resolve_secrets(cfg, resolver)
    new = resolved.notifications["wh"]
    assert new.url == "https://hooks.x/abc"
    assert new.headers == {"Authorization": "Bearer xyz"}


def test_walker_dict_keys_not_inspected():
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={
            "wh": WebhookNotificationConfig(
                url="https://x",
                headers={"secret://k/v": "literal-value"},
            )
        },
        datasets=[],
    )
    resolver = _StubResolver({})
    resolved, _ = resolve_secrets(cfg, resolver)
    # Key remains literal; resolver is never called.
    assert "secret://k/v" in resolved.notifications["wh"].headers
    assert resolver.calls == 0


def test_walker_slack():
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={"sl": SlackNotificationConfig(webhook_url="secret://slack/hook")},
        datasets=[],
    )
    resolver = _StubResolver({("slack", "hook"): "https://hooks.slack.com/abc"})
    resolved, _ = resolve_secrets(cfg, resolver)
    assert resolved.notifications["sl"].webhook_url == "https://hooks.slack.com/abc"


# --- No-resolver paths ---


def test_no_resolver_no_refs_zero_calls():
    cfg = _config_with_email(smtp_password="literal-pwd", recipients=["a@b"])
    resolved, _ = resolve_secrets(cfg, None)
    # No exception, no walking necessary.
    assert resolved.notifications["prod"].smtp_password == "literal-pwd"


def test_no_resolver_with_refs_raises():
    cfg = _config_with_email(smtp_password="secret://x/y", recipients=["a@b"])
    with pytest.raises(MissingSecretResolverError, match="secret://x/y"):
        resolve_secrets(cfg, None)


# --- Cache + idempotency ---


def test_cache_dedupes_within_one_call():
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={
            "wh": WebhookNotificationConfig(
                url="secret://api_creds/url",
                headers={"a": "secret://api_creds/url", "b": "secret://api_creds/url"},
            )
        },
        datasets=[],
    )
    resolver = _StubResolver({("api_creds", "url"): "https://x/abc"})
    resolve_secrets(cfg, resolver)
    # Three references, all (api_creds, url) — exactly one resolver call.
    assert resolver.calls == 1


def test_idempotent_second_call_zero_resolver_calls():
    cfg = _config_with_email(
        smtp_password="secret://smtp_creds/pwd", recipients=["a@b"]
    )
    resolver = _StubResolver({("smtp_creds", "pwd"): "PWD1"})
    once, _ = resolve_secrets(cfg, resolver)
    assert resolver.calls == 1
    twice, _ = resolve_secrets(once, resolver)
    # Once resolved, no `secret://...` literals remain.
    assert resolver.calls == 1
    assert twice.notifications["prod"].smtp_password == "PWD1"


# --- Caller-config preservation (Codex r1 finding 2) ---


def test_caller_config_is_not_mutated():
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={
            "wh": WebhookNotificationConfig(
                url="secret://api/u",
                headers={"k": "secret://api/h"},
            )
        },
        datasets=[],
    )
    original_headers = cfg.notifications["wh"].headers
    resolver = _StubResolver({
        ("api", "u"): "https://x/y",
        ("api", "h"): "v",
    })
    resolve_secrets(cfg, resolver)
    # Original config still carries the literal references.
    assert cfg.notifications["wh"].url == "secret://api/u"
    # The original headers dict is unchanged in identity AND content.
    assert cfg.notifications["wh"].headers is original_headers
    assert original_headers == {"k": "secret://api/h"}


def test_dataframe_branch_preserves_object_identity():
    """`copy.deepcopy` would fail/copy a sentinel df; selective-copy
    must leave DataFrame branches alone."""
    from qualifire.core.config import DatasetConfig

    class _UncopyableDF:
        def __deepcopy__(self, memo):  # pragma: no cover - assertion below
            raise AssertionError("DataFrame branch must not be deepcopied")

    sentinel = _UncopyableDF()
    cfg = QualifireConfig(
        owner="t", bu="b", system_table="cat.sch.t",
        notifications={
            "p": EmailNotificationConfig(
                smtp_host="m", smtp_password="secret://x/y", recipients=["a@b"]
            )
        },
        datasets=[DatasetConfig(name="d", df=sentinel, validations=[])],
    )
    resolver = _StubResolver({("x", "y"): "PWD"})
    resolved, _ = resolve_secrets(cfg, resolver)
    # The dataset's df is the same object — never copied.
    assert resolved.datasets[0].df is sentinel


# --- Type guard (Codex r2 finding 8) ---


def test_resolver_returning_non_str_raises_with_type_only():
    cfg = _config_with_email(smtp_password="secret://x/y", recipients=["a@b"])
    resolver = _StubResolver({("x", "y"): {"sentinel-key": "sentinel-value"}})
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_secrets(cfg, resolver)
    msg = str(excinfo.value)
    # Type name appears, value content does not.
    assert "dict" in msg
    assert "sentinel-key" not in msg
    assert "sentinel-value" not in msg


# --- Resolver-side failure: no leak (gate 7 + Codex r2 finding 4) ---


def test_resolver_exception_no_cause_no_value_leak():
    SECRET_VALUE = "hunter2-leaked-via-vault-error"

    class _FailingResolver:
        def get(self, name, key=None):
            raise RuntimeError(f"vault denied access for {SECRET_VALUE}")

    cfg = _config_with_email(smtp_password="secret://x/y", recipients=["a@b"])
    with pytest.raises(SecretResolutionError) as excinfo:
        resolve_secrets(cfg, _FailingResolver())

    # __cause__ must be None — no traceback chain leaks the original.
    assert excinfo.value.__cause__ is None
    # Type name appears; value does not.
    err_text = str(excinfo.value)
    assert "RuntimeError" in err_text
    assert SECRET_VALUE not in err_text
    # Even traceback rendering must not surface the original.
    tb = "".join(traceback.format_exception(excinfo.value))
    assert SECRET_VALUE not in tb


# --- from_secret directive (Phase 4) ---


class TestFromSecret:
    def test_populates_direct_fields(self):
        jdbc = JDBCConfig(from_secret="prod_db")
        resolver = _StubResolver({
            ("prod_db", None): {
                "url": "jdbc:postgresql://h/db",
                "user": "qf",
                "password": "p",
            }
        })
        _, resolved = resolve_secrets(None, resolver, jdbc=jdbc)
        assert resolved.url == "jdbc:postgresql://h/db"
        assert resolved.user == "qf"
        assert resolved.password == "p"
        # Idempotency: from_secret cleared on the resolved copy.
        assert resolved.from_secret is None
        # Caller's original is untouched.
        assert jdbc.from_secret == "prod_db"
        assert jdbc.url is None

    def test_missing_url_raises(self):
        jdbc = JDBCConfig(from_secret="prod_db")
        resolver = _StubResolver({("prod_db", None): {"user": "qf"}})
        with pytest.raises(SecretResolutionError, match="did not.*supply 'url'"):
            resolve_secrets(None, resolver, jdbc=jdbc)

    def test_unexpected_key_raises(self):
        jdbc = JDBCConfig(from_secret="prod_db")
        resolver = _StubResolver({
            ("prod_db", None): {"url": "u", "unexpected": "v"}
        })
        with pytest.raises(SecretResolutionError, match="unexpected keys"):
            resolve_secrets(None, resolver, jdbc=jdbc)

    def test_non_dict_resolver_value_raises(self):
        jdbc = JDBCConfig(from_secret="prod_db")
        resolver = _StubResolver({("prod_db", None): "just-a-string"})
        with pytest.raises(SecretResolutionError, match="dict\\[str, str\\]"):
            resolve_secrets(None, resolver, jdbc=jdbc)

    def test_mutual_exclusion_with_per_field_ref(self):
        jdbc = JDBCConfig(from_secret="prod_db", url="secret://x/y")
        resolver = _StubResolver({})
        with pytest.raises(SecretResolutionError, match="set together with"):
            resolve_secrets(None, resolver, jdbc=jdbc)

    def test_mutual_exclusion_with_literal_field(self):
        jdbc = JDBCConfig(from_secret="prod_db", user="literal-user")
        resolver = _StubResolver({("prod_db", None): {"url": "u"}})
        with pytest.raises(SecretResolutionError, match="precludes inline"):
            resolve_secrets(None, resolver, jdbc=jdbc)

    def test_no_resolver_raises(self):
        jdbc = JDBCConfig(from_secret="prod_db")
        with pytest.raises(MissingSecretResolverError):
            resolve_secrets(None, None, jdbc=jdbc)

    def test_invalid_name_raises(self):
        jdbc = JDBCConfig(from_secret="bad/name")  # '/' not allowed in names
        with pytest.raises(SecretResolutionError, match="from_secret"):
            resolve_secrets(None, _StubResolver({}), jdbc=jdbc)


def test_jdbc_per_field_secret_resolution():
    jdbc = JDBCConfig(
        url="secret://prod_db/url",
        user="literal-user",
        password="secret://prod_db/pwd",
    )
    resolver = _StubResolver({
        ("prod_db", "url"): "jdbc:postgresql://h/db",
        ("prod_db", "pwd"): "PWD",
    })
    _, resolved = resolve_secrets(None, resolver, jdbc=jdbc)
    assert resolved.url == "jdbc:postgresql://h/db"
    assert resolved.user == "literal-user"
    assert resolved.password == "PWD"


def test_jdbc_properties_dict_values_walked():
    jdbc = JDBCConfig(
        url="jdbc:postgresql://h/db",
        properties={"fetchsize": "1000", "cert": "secret://prod_db/cert"},
    )
    resolver = _StubResolver({("prod_db", "cert"): "<pem>"})
    _, resolved = resolve_secrets(None, resolver, jdbc=jdbc)
    assert resolved.properties == {"fetchsize": "1000", "cert": "<pem>"}


def test_resolve_with_only_jdbc_kwarg():
    """``cfg=None`` is the constructor-time form (P2.2)."""
    jdbc = JDBCConfig(url="secret://x/u", password="secret://x/p")
    resolver = _StubResolver({("x", "u"): "u", ("x", "p"): "p"})
    new_cfg, new_jdbc = resolve_secrets(None, resolver, jdbc=jdbc)
    assert new_cfg is None
    assert new_jdbc.url == "u"
    assert new_jdbc.password == "p"


def test_resolved_copy_preserves_caller_model_fields_set():
    """Codex impl-review r1 finding 3: resolution must not mark
    `jdbc` (or any field) as explicit when the caller didn't set it.
    Otherwise `run_config_parsed` would mistakenly treat a resolved
    no-jdbc config as a jdbc override and clobber the instance's
    programmatic JDBCConfig."""
    cfg = QualifireConfig(
        owner="t", bu="b", system_table=":memory:",
        # NOTE: no jdbc, no system_table_backend → these fields stay
        # out of model_fields_set per Pydantic v2 semantics.
        notifications={
            "p": EmailNotificationConfig(
                smtp_host="m", smtp_password="secret://x/y", recipients=["a@b"]
            )
        },
        datasets=[],
    )
    # Sanity: the caller's config does not mark these explicit.
    assert "jdbc" not in cfg.model_fields_set
    assert "system_table_backend" not in cfg.model_fields_set

    resolver = _StubResolver({("x", "y"): "PWD"})
    resolved, _ = resolve_secrets(cfg, resolver)
    # Resolution must NOT add `jdbc` or `system_table_backend` to the
    # resolved copy's model_fields_set.
    assert "jdbc" not in resolved.model_fields_set
    assert "system_table_backend" not in resolved.model_fields_set
    # `notifications` was already explicit on the caller's config; it
    # remains explicit on the resolved copy.
    assert "notifications" in resolved.model_fields_set


def test_no_op_no_refs_no_resolver_no_calls():
    """Acceptance criterion 4 — no refs, no resolver = zero calls."""
    cfg = _config_with_email(smtp_password=None, recipients=["a@b"])
    counting = _StubResolver({})
    resolved, _ = resolve_secrets(cfg, counting)
    assert counting.calls == 0
    # Output structurally equivalent.
    assert resolved.notifications["prod"].smtp_password is None
