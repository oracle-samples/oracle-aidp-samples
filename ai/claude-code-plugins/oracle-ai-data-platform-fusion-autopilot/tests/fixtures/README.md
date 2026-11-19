# Test fixtures

Read-only test data used by v2 unit/integration tests.

## `v1_registry_snapshot.yaml`

Transcribed from the v1 reference branch via `scripts/dev/transcribe_v1_registry.py`.

**Provenance** (recorded inside the YAML's `provenance:` block on every regeneration):

- **Source branch**: `P1.5ε-fix5`
- **Branch head at transcription time**: `650d6909655fd30618f56edbbded6e4b81d6cc3b`
- **Source file blob hash**: `02ec45a7fae7c1fa5b94a3940144727da69dcc13`
- **Source path on v1**: `scripts/oracle_ai_data_platform_fusion_autopilot/schema/registry_metadata.py`

**Purpose**: parity baseline for v2 Phase 4's dual-runner gate. When Phase 4 ships, it compares content-pack-backend output against the v1 registry's declared dependencies, natural keys, and `incremental_capable` flags. The snapshot is the single source of truth for "what v1 promised."

**Status**: checked-in test data, **not runtime code**. The engine never imports this file. `transcribe_v1_registry.py` may be re-run at any time, but produces byte-identical output until the v1 branch's blob hash changes.

## Regenerating

```bash
python scripts/dev/transcribe_v1_registry.py > tests/fixtures/v1_registry_snapshot.yaml
```

The script asserts:

- `P1.5ε-fix5` branch is reachable from this checkout.
- The branch head matches the expected commit (warning on mismatch — v1 maintenance is allowed).
- The source file blob hash matches `02ec45a7…` (**hard fail** on mismatch — content has changed; review before regenerating).

## Snapshot test

`tests/unit/test_v1_registry_snapshot.py` re-runs the transcription in a subprocess and asserts the output matches this committed YAML byte-for-byte. CI fails on drift.
