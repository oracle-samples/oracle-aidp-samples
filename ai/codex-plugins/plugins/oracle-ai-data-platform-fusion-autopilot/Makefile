# Makefile — aidp-fusion-autopilot
#
# Override the Python interpreter to bypass $PATH:
#   make test PYTHON=python3.11
#   make test PYTHON=.venv/bin/python
PYTHON ?= python

.PHONY: test test-all test-delta docs-check

# Unit tests only (acceptance target for BACKLOG P2.4).
# Live tests are gated by AIDP_FUSION_AUTOPILOT_INTEGRATION=1 and live under tests/live.
test:
	$(PYTHON) -m pytest tests/unit -q

# Unit + non-live integration. Live tests still no-op without the env gate.
test-all:
	$(PYTHON) -m pytest tests -q

# REQUIRED Delta leg for bronze-extract-schema-patch (design 5d): builds an
# isolated venv with cluster-matched pins and runs the Delta-capable tests
# with AIDPF_REQUIRE_DELTA_TESTS=1 (an unavailable pyspark/delta import is a
# FAILURE there, never a silent skip). Needs Java 11/17 + Python <= 3.12:
#   make test-delta DELTA_PYTHON=python3.11
DELTA_PYTHON ?= python3.11

test-delta:
	$(DELTA_PYTHON) -m venv .venv-delta
	.venv-delta/bin/python -m pip install -q -U pip
	.venv-delta/bin/python -m pip install -q -r scripts/requirements-delta-test.txt pytest
	.venv-delta/bin/python -m pip install -q -e . --no-deps
	AIDPF_REQUIRE_DELTA_TESTS=1 .venv-delta/bin/python -m pytest tests/real_spark/test_bronze_schema_patch_delta.py -q

docs-check:
	$(PYTHON) scripts/docs_check.py
