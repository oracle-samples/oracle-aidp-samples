.PHONY: install install-aidp install-dev install-all test test-precheck test-cov lint clean wheel

# Use python3 throughout — macOS dev boxes ship without a `python`
# shim (the spark-primary plan-review fix flagged this).
PYTHON ?= python3

# Local-dev install: full set of optional validator extras.
# ``requirements-local.txt`` is the historical "everything except
# pyspark" file. AIDP installs use ``install-aidp`` instead, which
# only delta-installs jinja2 / prophet / shap / plotly / matplotlib.
install:
	pip install -r requirements-local.txt
	pip install -e .

# AIDP Workbench install — the minimum delta on top of AIDP's
# pre-provisioned compute. ``requirements.txt`` is intentionally
# tiny (5 packages) so that the install does NOT overwrite AIDP's
# pre-shipped pandas / numpy / pyarrow / pyspark / scikit-learn /
# pydantic etc.
install-aidp:
	pip install -r requirements.txt
	pip install -e .

install-dev:
	pip install -r requirements-dev.txt
	pip install -e .

install-all:
	pip install -r requirements-local.txt
	pip install -r requirements-dev.txt
	pip install -e .

# `test-precheck` errors out with a clear, copy-safe message if
# optional deps are missing — the test suite exercises
# pandas/forecast/anomaly paths and fails confusingly without them.
# Quoted `'pyspark>=3.5'` in the message so a contributor pasting the
# command into a POSIX shell does not get `>` interpreted as redirect.
test-precheck:
	@$(PYTHON) scripts/test_precheck.py

test: test-precheck
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=qualifire --cov-report=term-missing --cov-report=html

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .coverage htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

wheel:
	$(PYTHON) -m build --wheel
