import os

import pytest

# Set before any PySpark import; suppresses the PyArrow timezone warning.
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")


def pytest_configure(config):
    """Register markers and quieten PySpark's own deprecation noise.

    Mirrors data-engineering/transformation/merge/conftest.py: PySpark 3.5 uses
    distutils.version.LooseVersion internally, deprecated on Python 3.12+.
    """
    config.addinivalue_line(
        "markers",
        "delta: needs delta-spark and its jars (downloaded from Maven on first run)",
    )
    config.addinivalue_line("filterwarnings", "ignore::DeprecationWarning:pyspark.*")
