---
started: 2026-04-01
---

# Implementation Plan: Consolidate Requirements Files

## Overview
Remove `-r` flag usage from all requirements files and make `requirements.txt` the full dependency list. Each file is self-contained with explicit package names.

## Implementation Steps
- [ ] Step 1: Rewrite `requirements.txt` with all dependencies (core + pandas + forecast + anomaly)
- [ ] Step 2: Create `requirements-core.txt` with core-only deps (for lightweight/minimal installs)
- [ ] Step 3: Rewrite `requirements-dev.txt` with all explicit deps (no `-r`)
- [ ] Step 4: Remove `requirements-all.txt` (superseded by new `requirements.txt`)
- [ ] Step 5: Verify `requirements-forecast.txt` and `requirements-anomaly.txt` remain standalone
- [ ] Step 6: Run tests to verify nothing breaks

## Technical Decisions
- `requirements.txt` becomes the "install everything" file (matches AIDP expectation)
- `requirements-core.txt` preserves the old minimal install path
- No `-r` flags anywhere — every file lists full package names
- `pyproject.toml` remains the source of truth for package metadata; requirements files are for AIDP/pip convenience

## Testing Strategy
- Run `pytest tests/` to verify no import breakage
- Verify each requirements file is parseable: `pip install --dry-run -r <file>`
