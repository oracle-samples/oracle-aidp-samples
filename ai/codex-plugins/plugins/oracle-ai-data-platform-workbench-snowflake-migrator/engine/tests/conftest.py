"""Suite-wide safety.

The CLI discovers `snowmig-config.yaml` from the plugin folder, so a test that
runs `snowmig.main()` can pick up a developer's real config. Per-stage report
publishing would then upload to a real workspace from a test. It is switched
off for the whole suite; the tests that exercise it inject a fake transport.
"""
import pytest


@pytest.fixture(autouse=True)
def _no_live_stage_publish(monkeypatch):
    monkeypatch.setenv("SNOWMIG_NO_STAGE_PUBLISH", "1")
