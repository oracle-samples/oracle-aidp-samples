# Makefile — aidp-fusion-autopilot
#
# Override the Python interpreter to bypass $PATH:
#   make test PYTHON=python3.11
#   make test PYTHON=.venv/bin/python
PYTHON ?= python

.PHONY: test test-all docs-check

# Unit tests only (acceptance target for BACKLOG P2.4).
# Live tests are gated by AIDP_FUSION_AUTOPILOT_INTEGRATION=1 and live under tests/live.
test:
	$(PYTHON) -m pytest tests/unit -q

# Unit + non-live integration. Live tests still no-op without the env gate.
test-all:
	$(PYTHON) -m pytest tests -q

docs-check:
	$(PYTHON) scripts/docs_check.py
