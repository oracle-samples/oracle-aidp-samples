"""Tests for notification base, registry, and deduplication."""

from qualifire.core.models import Severity
from qualifire.notification.base import (
    get_notifier,
    get_registered_notifiers,
    should_suppress,
)


class TestNotifierRegistry:
    def test_builtin_notifiers_registered(self):
        # Import to trigger registration
        import qualifire.notification.email_notifier  # noqa: F401
        import qualifire.notification.slack_notifier  # noqa: F401
        import qualifire.notification.webhook_notifier  # noqa: F401

        registry = get_registered_notifiers()
        assert "email" in registry
        assert "slack" in registry
        assert "webhook" in registry

    def test_get_unknown_notifier(self):
        import pytest

        with pytest.raises(KeyError, match="Unknown notifier"):
            get_notifier("nonexistent")


class MockStorage:
    def __init__(self, history):
        self._history = history

    def read_validation_history(self, **kwargs):
        return self._history


class TestAlertDeduplication:
    def test_suppress_duplicate(self):
        storage = MockStorage([{"validation_status": "WARNING"}])
        assert should_suppress(storage, "ds", "v1", Severity.WARNING) is True

    def test_no_suppress_different_severity(self):
        storage = MockStorage([{"validation_status": "PASS"}])
        assert should_suppress(storage, "ds", "v1", Severity.WARNING) is False

    def test_no_suppress_no_history(self):
        storage = MockStorage([])
        assert should_suppress(storage, "ds", "v1", Severity.WARNING) is False

    def test_no_suppress_no_storage(self):
        assert should_suppress(None, "ds", "v1", Severity.WARNING) is False
