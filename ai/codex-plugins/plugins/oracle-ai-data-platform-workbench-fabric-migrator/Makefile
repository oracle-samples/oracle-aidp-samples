# fabric-aidp -- Microsoft Fabric workspace export -> Oracle AIDP artifacts
#
# From a fresh clone:
#
#   make setup     once: virtualenv, the CLI, and the Power Query parser
#   make demo      migrate the bundled estate and print the verdict
#
# Or `make check` to do both plus the test suite in one go -- that is the
# command to hand someone who is evaluating the tool.
#
# Nothing here contacts AIDP, Azure, or any network after `make setup`.
.PHONY: help setup demo test check clean needs-setup

PY     := .venv/bin/python
PYBOOT := $(shell command -v python3 2>/dev/null)

help:
	@echo ""
	@echo "  make setup   create .venv, install the CLI and the Power Query parser"
	@echo "  make demo    migrate every bundled input (offline, no credentials)"
	@echo "  make test    run the test suite"
	@echo "  make check   setup + demo + test -- one command for a reviewer"
	@echo "  make clean   remove .venv, demo output and caches"
	@echo ""

setup:
	@test -n "$(PYBOOT)" || { \
	  echo ""; \
	  echo "  no python3 on PATH. Install Python 3.9 or newer:"; \
	  echo ""; \
	  echo "      brew install python3          # macOS"; \
	  echo "      sudo apt install python3-venv # Debian/Ubuntu"; \
	  echo ""; \
	  exit 1; }
	@echo "  using $(PYBOOT) ($$($(PYBOOT) --version))"
	$(PYBOOT) -m venv .venv
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -e .
	@# Power Query has no Python parser -- Microsoft's own is TypeScript -- so
	@# translating Dataflows needs Node. Everything else works without it, and
	@# the tool reports Dataflows as counted-but-not-translated when it is
	@# absent. Failing here is deliberate: a half-working demo reads as a bug.
	@command -v npm >/dev/null || { \
	  echo ""; \
	  echo "  npm not found. Dataflow translation needs Node 18+:"; \
	  echo ""; \
	  echo "      brew install node             # macOS"; \
	  echo "      sudo apt install nodejs npm   # Debian/Ubuntu"; \
	  echo ""; \
	  echo "  Everything else works without it; rerun 'make setup' after."; \
	  exit 1; }
	cd fabric_aidp/mparse && npm install --silent
	@echo ""
	@echo "  ready. next:  make demo"
	@echo ""

demo: needs-setup
	@$(PY) scripts/stage_demo_input.py
	@PATH="$(CURDIR)/.venv/bin:$$PATH" ./demo.sh

test: needs-setup
	@PYTHONPATH=. $(PY) scripts/run_stress_tests.py --quiet

# One command for someone evaluating the tool. Ends on the two numbers that
# matter: FAIL from the migration, and the test result.
check: setup
	@$(MAKE) --no-print-directory demo
	@echo ""
	@$(MAKE) --no-print-directory test
	@echo ""
	@echo "  Read the two blocks above:"
	@echo "    FAIL: 0    the migration found nothing broken."
	@echo "    OK         the tests pass. A 'skipped' count is expected. Every"
	@echo "               skip here means a tool or a corpus is absent from"
	@echo "               this machine -- pyspark and a JVM for most of them,"
	@echo "               Node, setuptools, or a fuller private corpus. None"
	@echo "               is a problem, and the third-party corpora vendored"
	@echo "               in this repo run for everyone. README, 'Testing',"
	@echo "               lists every reason a test here is allowed to skip."
	@echo ""
	@echo "  The translated Dataflow -- Power Query M to PySpark:"
	@echo "    cat demo-output/migrated/dataflows/Agents_Dim.dim_agent.py"
	@echo ""

# `make demo` before `make setup` used to fail with
# "./demo.sh: fabric-aidp: command not found", which blames the demo. A
# parse-time $(error) gives one clean line rather than a friendly message
# followed by make's own "*** [.venv] Error 1".
needs-setup:
ifeq (,$(wildcard .venv/bin/python))
	$(error no .venv yet -- run 'make setup' first)
endif
	@true

clean:
	rm -rf .venv demo-output demo-input fabric_aidp/mparse/node_modules .pytest_cache build *.egg-info
	find . -name __pycache__ -type d -exec rm -rf {} +
