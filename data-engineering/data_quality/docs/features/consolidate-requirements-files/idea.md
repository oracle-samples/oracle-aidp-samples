---
id: consolidate-requirements-files
name: Consolidate Requirements Files
type: Enhancement
priority: P1
effort: Small
impact: Medium
created: 2026-04-01
---

# Consolidate Requirements Files

## Problem Statement
The current requirements files use the `-r` flag to include other files (e.g., `requirements-all.txt` has `-r requirements.txt`). The AIDP platform does not support the `-r` flag inside requirements files, so each file must list all package names explicitly.

The main `requirements.txt` currently only contains core dependencies (4 packages). It should contain all dependencies needed for the full library, including forecast, anomaly, and pandas extras. Separate `requirements-*.txt` files should exist for minimal/test variants but must duplicate package names rather than using `-r` references.

## Affected Areas
- requirements.txt (main — should include all deps)
- requirements-all.txt (remove -r, list all packages)
- requirements-dev.txt (remove -r, list all packages)
- requirements-forecast.txt (standalone)
- requirements-anomaly.txt (standalone)
