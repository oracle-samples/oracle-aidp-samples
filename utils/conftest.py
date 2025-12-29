import os

# Set PyArrow environment variable before any PySpark imports
# This suppresses the timezone warning for tests
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
