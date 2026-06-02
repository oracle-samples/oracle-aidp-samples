"""Secret-reference resolution for Qualifire configs.

Operators on AIDP (or any platform with a managed secret backend)
inject a ``SecretResolver`` into ``Qualifire(secret_resolver=...)``
and reference credentials in YAML or programmatic configs as
``secret://name/key`` strings instead of inline values. This module
parses those references and walks the credential-bearing
allowlist on a freshly-validated ``QualifireConfig`` (and any
constructor-supplied ``JDBCConfig``), returning resolved copies
without mutating the caller's input.

See ``docs/features/secrets-resolver-aidputils/plan.md`` for the
full design — Decision Pins, scope gates, and phase boundaries.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Protocol

from pydantic import BaseModel

from qualifire.core.exceptions import (
    MissingSecretResolverError,
    SecretResolutionError,
)


_SECRET_PREFIX = "secret://"
# Decision Pin 1 — name excludes ``/`` (split delimiter); key allows
# ``/`` because some vault backends use slash-delimited paths.
_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_KEY_RE = re.compile(r"^[A-Za-z0-9_./\-]+$")


class SecretResolver(Protocol):
    """Duck-typed contract for any secret backend.

    AIDP's ``aidputils.secrets`` already conforms. Operators on
    other backends write a 5-line wrapper class and pass it in.
    """

    def get(self, name: str, key: str | None = None) -> Any:  # pragma: no cover - protocol
        ...


def parse_secret_ref(value: str) -> tuple[str, str] | None:
    """Parse a ``secret://name/key`` string.

    Returns ``(name, key)`` for valid references; returns ``None``
    when the input is not a reference at all (so callers can treat
    plain literals as passthrough). Raises ``ValueError`` for
    malformed references — operators get a clear parse-time
    rejection rather than a confusing runtime auth failure.
    """
    if not isinstance(value, str) or not value.startswith(_SECRET_PREFIX):
        return None
    rest = value[len(_SECRET_PREFIX) :]
    if "/" not in rest:
        # Per Decision Pin 1: per-field refs MUST include both name
        # and key. ``secret://name`` (no key) is not a valid form;
        # the only whole-secret path is ``from_secret`` on JDBCConfig.
        raise ValueError(
            f"Malformed secret reference {value!r}: per-field references "
            "must take the form 'secret://name/key' (both name and key "
            "required). Use 'from_secret:' on JDBCConfig for whole-secret "
            "lookups."
        )
    name, key = rest.split("/", 1)
    if not name or not _NAME_RE.fullmatch(name):
        raise ValueError(
            f"Malformed secret reference {value!r}: name must match "
            f"{_NAME_RE.pattern} (no slashes; alphanumeric, '_', '.', '-')."
        )
    if not key or not _KEY_RE.fullmatch(key):
        raise ValueError(
            f"Malformed secret reference {value!r}: key must match "
            f"{_KEY_RE.pattern} (alphanumeric, '_', '.', '-', '/')."
        )
    return name, key


def is_secret_ref(value: Any) -> bool:
    """Cheap check used by mutual-exclusion validators —
    ``str.startswith(secret://)`` without raising on garbage."""
    return isinstance(value, str) and value.startswith(_SECRET_PREFIX)


class _ResolutionContext:
    """Per-call state passed through the dispatch table.

    Owns the resolver and the (name, key) cache. The cache exists
    strictly inside one ``resolve_secrets()`` call — never stored
    on ``Qualifire``, ``QualifireContext``, the resolver, or a
    module-level global (see Decision Pin 5). It dies when the
    call returns; rotated secrets stay fresh on the next call.
    """

    def __init__(self, resolver: SecretResolver | None, *, allow_from_secret: bool):
        self.resolver = resolver
        self.cache: dict[tuple[str, str | None], Any] = {}
        # Phase-1 walker rejects ``from_secret`` even with a resolver
        # present; flipped on by Phase 4 (``_walk_jdbc`` honors the
        # directive). Lets Phase 1 land independently.
        self.allow_from_secret = allow_from_secret

    def fetch(self, name: str, key: str | None, *, field_path: str) -> Any:
        if self.resolver is None:
            raise MissingSecretResolverError(
                f"Config contains 'secret://{name}/{key or ''}' at "
                f"'{field_path}' but Qualifire(secret_resolver=...) was "
                "not supplied. Pass a resolver or remove the reference."
            )
        cache_key = (name, key)
        if cache_key in self.cache:
            return self.cache[cache_key]
        try:
            value = self.resolver.get(name, key=key) if key is not None else self.resolver.get(name)
        except Exception as e:
            # Decision Pin 8: re-raise ``from None`` with the type
            # name only. Vault SDK error messages occasionally embed
            # the value; we never echo whatever they say.
            type_name = type(e).__name__
            raise SecretResolutionError(
                f"Resolver failed for 'secret://{name}/{key or ''}' at "
                f"'{field_path}' ({type_name})."
            ) from None
        self.cache[cache_key] = value
        return value


def _resolve_str_field(value: Any, ctx: _ResolutionContext, *, field_path: str) -> Any:
    """Resolve a single string-typed credential field.

    Returns ``value`` unchanged if it isn't a reference. Raises
    ``SecretResolutionError`` (the unified config-error family) for
    both parse-time errors and resolved-value type guard failures —
    callers inside ``api.py`` always get the same exception type
    regardless of failure kind.
    """
    if not isinstance(value, str):
        return value
    try:
        parsed = parse_secret_ref(value)
    except ValueError as e:
        raise SecretResolutionError(f"{field_path}: {e}") from None
    if parsed is None:
        return value
    name, key = parsed
    resolved = ctx.fetch(name, key, field_path=field_path)
    if not isinstance(resolved, str):
        raise SecretResolutionError(
            f"Resolver returned non-str for 'secret://{name}/{key}' at "
            f"'{field_path}': expected 'str', got '{type(resolved).__name__}'."
        )
    return resolved


# --- Per-class walkers (allowlist dispatch — Decision Pin 6) ---


def _walk_email(cfg: BaseModel, ctx: _ResolutionContext, *, base_path: str) -> BaseModel:
    new = cfg.model_copy(deep=False)
    new.smtp_user = _resolve_str_field(new.smtp_user, ctx, field_path=f"{base_path}.smtp_user")
    new.smtp_password = _resolve_str_field(
        new.smtp_password, ctx, field_path=f"{base_path}.smtp_password"
    )
    new.sender = _resolve_str_field(new.sender, ctx, field_path=f"{base_path}.sender")
    # ``recipients`` deliberately NOT walked — Decision Pin 6.
    return new


def _walk_slack(cfg: BaseModel, ctx: _ResolutionContext, *, base_path: str) -> BaseModel:
    new = cfg.model_copy(deep=False)
    new.webhook_url = _resolve_str_field(
        new.webhook_url, ctx, field_path=f"{base_path}.webhook_url"
    )
    return new


def _walk_webhook(cfg: BaseModel, ctx: _ResolutionContext, *, base_path: str) -> BaseModel:
    new = cfg.model_copy(deep=False)
    new.url = _resolve_str_field(new.url, ctx, field_path=f"{base_path}.url")
    # Clone the headers dict before mutating; the caller's original
    # container reference must remain untouched.
    new.headers = {
        k: _resolve_str_field(v, ctx, field_path=f"{base_path}.headers[{k!r}]")
        for k, v in (new.headers or {}).items()
    }
    return new


_JDBC_DIRECT_FIELDS = ("url", "user", "password", "driver")


def _walk_jdbc(cfg: BaseModel, ctx: _ResolutionContext, *, base_path: str) -> BaseModel:
    new = cfg.model_copy(deep=False)
    from_secret = getattr(new, "from_secret", None)
    if from_secret is not None:
        if not ctx.allow_from_secret:
            # Phase 1: directive parse-rejects until Phase 4 wires it
            # in. Lets the runtime walker land independently.
            raise SecretResolutionError(
                f"'{base_path}.from_secret' is not yet supported "
                "(Phase 4 of the secrets-resolver feature)."
            )
        if not _NAME_RE.fullmatch(from_secret):
            raise SecretResolutionError(
                f"'{base_path}.from_secret' must match {_NAME_RE.pattern}; "
                f"got {from_secret!r}."
            )
        # Mutual exclusion #1: from_secret + per-field secret://...
        # references on the same JDBCConfig (Decision Pin 9).
        for fname in _JDBC_DIRECT_FIELDS:
            if is_secret_ref(getattr(new, fname, None)):
                raise SecretResolutionError(
                    f"'{base_path}.from_secret' set together with a "
                    f"per-field secret reference at '{base_path}.{fname}'. "
                    "Use one or the other, not both."
                )
        # Mutual exclusion #2: from_secret + non-None literal field values.
        for fname in _JDBC_DIRECT_FIELDS:
            v = getattr(new, fname, None)
            if v is not None:
                raise SecretResolutionError(
                    f"'{base_path}.from_secret' precludes inline credential "
                    f"values; remove '{base_path}.{fname}'."
                )
        if ctx.resolver is None:
            raise MissingSecretResolverError(
                f"'{base_path}.from_secret={from_secret!r}' requires "
                "Qualifire(secret_resolver=...) to be supplied."
            )
        try:
            secret = ctx.resolver.get(from_secret)
        except Exception as e:
            raise SecretResolutionError(
                f"Resolver failed for '{base_path}.from_secret="
                f"{from_secret!r}' ({type(e).__name__})."
            ) from None
        if not isinstance(secret, dict):
            raise SecretResolutionError(
                f"'{base_path}.from_secret={from_secret!r}' expected the "
                "resolver to return a dict[str, str]; got "
                f"'{type(secret).__name__}'."
            )
        unexpected = sorted(set(secret) - set(_JDBC_DIRECT_FIELDS))
        if unexpected:
            raise SecretResolutionError(
                f"'{base_path}.from_secret={from_secret!r}' returned "
                f"unexpected keys {unexpected}; allowed keys: "
                f"{list(_JDBC_DIRECT_FIELDS)}."
            )
        for fname in _JDBC_DIRECT_FIELDS:
            if fname in secret:
                v = secret[fname]
                if not isinstance(v, str):
                    raise SecretResolutionError(
                        f"'{base_path}.from_secret={from_secret!r}' value "
                        f"for {fname!r} must be 'str'; got "
                        f"'{type(v).__name__}'."
                    )
                setattr(new, fname, v)
        # ``url`` is the one direct field the rest of the system
        # genuinely needs (``_require_jdbc_when_needed``); make the
        # missing-key error surface here, not at storage-open time.
        if not getattr(new, "url", None):
            raise SecretResolutionError(
                f"'{base_path}.from_secret={from_secret!r}' did not "
                "supply 'url'; resolver must return a dict containing "
                "at least 'url'."
            )
        # Idempotency: clear the directive on the resolved copy so a
        # second walk is a no-op (gate 5).
        new.from_secret = None
        # Walk ``properties`` values too — operators may put
        # ``secret://...`` refs there for driver-specific options
        # alongside ``from_secret``.
        new.properties = {
            k: _resolve_str_field(v, ctx, field_path=f"{base_path}.properties[{k!r}]")
            for k, v in (new.properties or {}).items()
        }
        return new

    # No from_secret directive — walk the per-field refs.
    for fname in _JDBC_DIRECT_FIELDS:
        v = getattr(new, fname, None)
        setattr(new, fname, _resolve_str_field(v, ctx, field_path=f"{base_path}.{fname}"))
    new.properties = {
        k: _resolve_str_field(v, ctx, field_path=f"{base_path}.properties[{k!r}]")
        for k, v in (new.properties or {}).items()
    }
    return new


def _build_walkers() -> dict[type, Callable[..., BaseModel]]:
    """Build the per-class walker registry.

    Imports are deferred so this module stays importable even when
    the surrounding config module is partially loaded (avoids a
    cycle during module bootstrap in tests).
    """
    from qualifire.core.config import (
        EmailNotificationConfig,
        SlackNotificationConfig,
        WebhookNotificationConfig,
    )

    return {
        EmailNotificationConfig: _walk_email,
        SlackNotificationConfig: _walk_slack,
        WebhookNotificationConfig: _walk_webhook,
    }


def _walkers() -> dict[type, Callable[..., BaseModel]]:
    """Lazy-cached registry. Tests patch ``_build_walkers`` to inject
    synthetic notifier types and call ``_walkers.cache_clear()`` to
    reset; mirrors ``functools.cache``'s API."""
    if _walkers._cache is None:  # type: ignore[attr-defined]
        _walkers._cache = _build_walkers()  # type: ignore[attr-defined]
    return _walkers._cache  # type: ignore[attr-defined]


def _walkers_cache_clear() -> None:
    _walkers._cache = None  # type: ignore[attr-defined]


_walkers._cache = None  # type: ignore[attr-defined]
# Expose ``.cache_clear()`` on the function object so callers don't
# need to know the internal ``_cache`` attribute. Matches the stdlib
# ``functools.cache.cache_clear`` ergonomics.
_walkers.cache_clear = _walkers_cache_clear  # type: ignore[attr-defined]


def resolve_secrets(
    cfg: Any | None,
    resolver: SecretResolver | None,
    *,
    jdbc: Any | None = None,
    allow_from_secret: bool = True,
    effective_backend: str | None = None,
) -> tuple[Any | None, Any | None]:
    """Walk the credential allowlist and return resolved copies.

    Args:
        cfg: A ``QualifireConfig`` instance (or ``None`` if only
            ``jdbc`` should be walked — used by the constructor-time
            JDBC path).
        resolver: Operator-supplied resolver implementing
            ``SecretResolver``. ``None`` is allowed when the config
            contains no references; raises
            ``MissingSecretResolverError`` if references are present.
        jdbc: Optional standalone ``JDBCConfig`` (constructor path).
        allow_from_secret: Phase-1 escape valve — set ``False`` until
            Phase 4 lands the directive. Default ``True`` for the
            shipped path.
        effective_backend: Override the ``cfg.system_table_backend``
            value used to decide whether to walk ``cfg.jdbc``. The
            api.py layer uses this to honor the
            ``run_config_parsed`` policy of falling back to the
            instance backend when YAML does not override it; without
            this knob, a templated YAML carrying ``secret://`` refs
            in ``jdbc:`` plus an instance-backend of ``"jdbc"``
            would skip resolution and storage would open with
            literals.

    Returns:
        ``(resolved_cfg, resolved_jdbc)``. Both are selectively
        cloned; the caller's originals are not mutated.
    """
    ctx = _ResolutionContext(resolver, allow_from_secret=allow_from_secret)

    new_jdbc = jdbc
    if jdbc is not None:
        new_jdbc = _walk_jdbc(jdbc, ctx, base_path="jdbc")

    new_cfg = cfg
    if cfg is not None:
        # Walk notifications dict via the registry.
        notifications = getattr(cfg, "notifications", None) or {}
        new_notifications: dict[str, Any] = {}
        registry = _walkers()
        for chan_name, entry in notifications.items():
            walker = registry.get(type(entry))
            if walker is None:
                # Fail-fast on an unknown type rather than silently
                # preserving credential refs.
                raise SecretResolutionError(
                    f"notifications[{chan_name!r}]: no secret walker "
                    f"registered for type {type(entry).__name__!r}; "
                    "extend qualifire/core/secrets.py:_build_walkers."
                )
            new_notifications[chan_name] = walker(
                entry, ctx, base_path=f"notifications[{chan_name!r}]"
            )

        # Walk the config-level ``jdbc`` only when the active backend
        # actually uses it. Preserves the existing
        # ``_require_jdbc_when_needed`` tolerance for a templated
        # jdbc block sitting alongside a non-jdbc backend (e.g.,
        # sqlite for tests, external_catalog for prod) — those
        # operators may keep a ``url:``-only block in YAML and never
        # intended secret resolution.
        config_jdbc = getattr(cfg, "jdbc", None)
        new_config_jdbc = config_jdbc
        active_backend = (
            effective_backend
            if effective_backend is not None
            else getattr(cfg, "system_table_backend", None)
        )
        if config_jdbc is not None and active_backend == "jdbc":
            new_config_jdbc = _walk_jdbc(config_jdbc, ctx, base_path="jdbc")

        # Pydantic v2 marks every key in ``update`` as explicit on the
        # copy's ``model_fields_set``. Downstream
        # ``run_config_parsed`` reads ``model_fields_set`` to decide
        # whether the caller actually overrode ``jdbc`` /
        # ``system_table_backend`` (Codex impl-review round 1
        # finding 3). Avoid wrongly marking either field explicit
        # by only including them in the update when the original was
        # explicit, or when the value actually changed.
        update: dict[str, Any] = {"notifications": new_notifications}
        original_set = cfg.model_fields_set
        if "jdbc" in original_set or new_config_jdbc is not config_jdbc:
            update["jdbc"] = new_config_jdbc
        new_cfg = cfg.model_copy(update=update)
        # ``model_copy(update=...)`` also adds the updated keys to
        # the new instance's ``model_fields_set``. Restore exactly
        # the caller's original set so downstream ``jdbc_explicit``
        # / ``backend_explicit`` checks see the same shape they
        # would have without secret resolution.
        #
        # NOTE: ``__pydantic_fields_set__`` is documented as a
        # Pydantic-v2 instance attribute (see Pydantic 2.x release
        # notes; ``model_construct(_fields_set=...)`` is the public
        # constructor that also writes through to it). Direct slot
        # mutation is the narrow alternative that keeps the
        # existing ``model_copy`` path — round-trips through
        # ``model_construct`` would re-run no validators but would
        # need to enumerate ``__dict__`` keys explicitly. Verified
        # against pydantic >= 2.5; if a future Pydantic release
        # promotes this to a managed attribute, switch to
        # ``model_construct``.
        new_cfg.__pydantic_fields_set__ = set(original_set)

    return new_cfg, new_jdbc
