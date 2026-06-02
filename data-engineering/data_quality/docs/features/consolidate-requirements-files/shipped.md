---
shipped: 2026-04-01
---

# Shipped: Consolidate Requirements Files

## Summary
Removed all `-r` flag usage from requirements files for AIDP platform compatibility. Made `requirements.txt` the full dependency list with all packages. Each file is now self-contained.

## Key Changes
- `requirements.txt` now lists all 8 dependencies (core + pandas + forecast + anomaly)
- Created `requirements-core.txt` with 4 core-only deps for lightweight installs
- Rewrote `requirements-dev.txt` with explicit package names (no `-r`)
- Removed `requirements-all.txt` (superseded by `requirements.txt`)
- `requirements-forecast.txt` and `requirements-anomaly.txt` unchanged (already standalone)

## Files Changed
- `requirements.txt` — expanded to all deps
- `requirements-core.txt` — new, core-only
- `requirements-dev.txt` — rewritten without `-r`
- `requirements-all.txt` — deleted

## Testing
- 385 tests pass
- Verified no `-r` flags in any requirements file
- No code changes, only dependency file reorganization

## Notes
- `pyproject.toml` remains the source of truth for package metadata
- Requirements files are for AIDP/pip convenience only
