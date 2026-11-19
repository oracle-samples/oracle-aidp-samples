"""Version support policy: detect everything, accept only 10.x+.

Per spec section 15, detection of pre-10.x PowerCenter releases is kept
(``version_detector._VERSION_MAP``) so the tool can refuse clearly rather
than silently mis-migrate a 9.x/8.x export. This module tests the gate
sitting on top of that detection: ``is_supported`` (a pure predicate) and
``require_supported`` (the enforcement point, with a test-only override).
"""
from __future__ import annotations

import pytest

from infa2aidp.models import InfaVersion
from infa2aidp.parsers.version_detector import (
    detect_version,
    is_supported,
    require_supported,
)


def test_modern_powercenter_is_supported():
    assert is_supported(InfaVersion.V10) is True


def test_nine_x_is_detected_but_refused():
    assert is_supported(InfaVersion.V9) is False


def test_eight_x_is_refused():
    assert is_supported(InfaVersion.V8) is False


def test_unknown_is_refused():
    assert is_supported(InfaVersion.UNKNOWN) is False


def test_require_supported_is_a_noop_for_a_supported_version():
    require_supported(InfaVersion.V10, "10.2+ (repository version 189.x)")  # must not raise


def test_require_supported_raises_with_an_actionable_message_by_default(monkeypatch):
    monkeypatch.delenv("INFA_ALLOW_UNSUPPORTED_VERSION", raising=False)
    with pytest.raises(ValueError) as exc_info:
        require_supported(InfaVersion.V9, "9.x (repository version 186.x)")
    message = str(exc_info.value)
    assert "9.x (repository version 186.x)" in message
    assert "10.x" in message


def test_override_env_var_bypasses_the_gate_but_is_not_default(monkeypatch):
    """Test-only escape hatch, off unless explicitly set to "1"."""
    # Default: refused.
    monkeypatch.delenv("INFA_ALLOW_UNSUPPORTED_VERSION", raising=False)
    with pytest.raises(ValueError):
        require_supported(InfaVersion.V9, "9.x (repository version 186.x)")

    # Explicit opt-in: allowed.
    monkeypatch.setenv("INFA_ALLOW_UNSUPPORTED_VERSION", "1")
    require_supported(InfaVersion.V9, "9.x (repository version 186.x)")  # must not raise

    # Any other truthy-looking value must NOT bypass.
    monkeypatch.setenv("INFA_ALLOW_UNSUPPORTED_VERSION", "true")
    with pytest.raises(ValueError):
        require_supported(InfaVersion.V9, "9.x (repository version 186.x)")


def test_override_env_var_logs_a_warning_naming_the_version(monkeypatch, caplog):
    import logging

    monkeypatch.setenv("INFA_ALLOW_UNSUPPORTED_VERSION", "1")
    with caplog.at_level(logging.WARNING):
        require_supported(InfaVersion.V9, "9.x (repository version 186.x)")
    assert any(
        "9.x (repository version 186.x)" in r.message and "BYPASSED" in r.message
        for r in caplog.records
    )


# ── Developer / Model repository exports (KB 516223) ──────────────────

def _detect(tmp_path, xml: str):
    f = tmp_path / "export.xml"
    f.write_text(xml)
    return detect_version(str(f))


def test_a_developer_export_is_recognised_and_named(tmp_path):
    """A Developer export carries SerializationSpecVersion, not
    REPOSITORY_VERSION.

    Unlike the REPOSITORY_VERSION table -- which Informatica publishes
    nowhere, and which is still disputed for 186/187 -- this mapping has
    a documented source, so it can be asserted rather than inferred.
    """
    _, detail = _detect(
        tmp_path, '<?xml version="1.0"?><IMX SerializationSpecVersion="14.0"><c/></IMX>'
    )
    assert "Developer" in detail
    assert "10.5.x" in detail


def test_a_developer_export_is_not_reported_as_supported(tmp_path):
    """Knowing the release does not make the file migratable.

    This parser reads POWERMART. Returning V10_5 here would pass the
    version gate and then fail obscurely inside the parser, which is a
    worse outcome than a clear refusal naming the format.
    """
    version, _ = _detect(
        tmp_path, '<?xml version="1.0"?><IMX SerializationSpecVersion="14.0"><c/></IMX>'
    )
    assert version is InfaVersion.UNKNOWN


def test_an_older_developer_release_is_named(tmp_path):
    _, detail = _detect(
        tmp_path, '<?xml version="1.0"?><IMX SerializationSpecVersion="6.0"><c/></IMX>'
    )
    assert "9.6.x" in detail


def test_an_unmapped_spec_version_still_identifies_the_format(tmp_path):
    """A value the KB does not list must not be silently ignored."""
    _, detail = _detect(
        tmp_path, '<?xml version="1.0"?><IMX SerializationSpecVersion="99.0"><c/></IMX>'
    )
    assert "Developer" in detail
    assert "99.0" in detail


def test_a_powercenter_export_is_unaffected(tmp_path):
    """REPOSITORY_VERSION still wins; the new check runs only when absent."""
    version, detail = _detect(
        tmp_path, '<?xml version="1.0"?><POWERMART REPOSITORY_VERSION="189.96"/>'
    )
    assert version is InfaVersion.V10_5
    assert "Developer" not in detail
