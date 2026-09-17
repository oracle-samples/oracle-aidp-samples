"""Local pytest configuration for the lineage conformance suite.

This is a conftest.py rather than a pytest.ini on purpose: an ini file is only read
when it is the rootdir's config, so running `pytest data-engineering/lineage/` from the
repository root silently ignored it and every marker below came back as an unknown-mark
warning. conftest.py is collected relative to the test file, so the markers register
wherever pytest is invoked from.
"""


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "existence: proves the AIDP lineage API is released and live"
    )
    config.addinivalue_line(
        "markers", "population: probes whether the lineage graph is populated"
    )
    config.addinivalue_line(
        "markers", "legacy: probes the previous API generation (aidp.{region}/20240831)"
    )
