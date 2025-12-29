import os
import pytest

# Set PyArrow environment variable before any PySpark imports
# This suppresses the timezone warning for tests
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'


def pytest_configure(config):
    """
    Configure pytest to suppress PySpark-related warnings.

    PySpark 3.5.0 uses distutils.version.LooseVersion internally,
    which is deprecated in Python 3.12+. This suppresses those warnings
    to keep test output clean.
    """
    # Suppress distutils deprecation warnings from PySpark modules
    config.addinivalue_line(
        "filterwarnings", "ignore::DeprecationWarning:pyspark.*"
    )
