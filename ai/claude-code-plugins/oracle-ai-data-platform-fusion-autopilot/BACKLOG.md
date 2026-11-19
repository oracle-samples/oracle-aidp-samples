# Backlog - `oracle-ai-data-platform-fusion-autopilot`

This is the active backlog for the current content-pack plugin. Historical
v1-transition notes are intentionally excluded from the published docs.

Use this file for current work only. Before starting a task, verify the details
against:

- [`workflow.md`](workflow.md)
- [`docs/project_setup.md`](docs/project_setup.md)
- [`docs/mart_overlay_authoring.md`](docs/mart_overlay_authoring.md)
- [`docs/aidpf-error-codes.md`](docs/aidpf-error-codes.md)
- [`docs/adr/`](docs/adr/)

## Priority Legend

| Class | Meaning |
|---|---|
| **P0** | Prevent users or maintainers from following wrong instructions |
| **P1** | Improve the default content-pack workflow |
| **P2** | Quality, validation, and live-evidence coverage |
| **P3** | Roadmap or upstream dependency |

## P0 - Documentation And Workflow Safety

### `[ ]` P0.1 - Finish top-level historical-doc cleanup

**Why**: `CLAUDE.md` and `STATUS.md` still mix current guidance with historical
phase notes. `BACKLOG.md` is now split, but those files still need clear
current-vs-historical boundaries.

**Accept**:
- `CLAUDE.md` points current contributors to the content-pack workflow and
  moves/archive-marks old v1/v2 coexistence narrative.
- `STATUS.md` is labeled as a historical status snapshot, or replaced by a
  small current status page plus archive link.
- No top-level doc tells users to follow deleted v1 module or old phase tasks.

### `[ ]` P0.2 - Add stale-reference hygiene check

**Why**: Old `PLAN §`, `Phase N`, `P1.*`, `TC*`, and demo-tenant references can
quietly re-enter current docs or source comments.

**Accept**:
- A lightweight docs/source check fails when stale project-history markers
  appear in current workflow docs, source comments, skills, or pack YAML.
- Historical/archive paths are excluded or explicitly allowed.
- The check is wired into `make docs-check` or a clearly named docs-check
  subcommand.

### `[ ]` P0.3 - Decide untracked working files

**Why**: Current working tree has untracked paths that need an explicit fate.

**Accept**:
- `docs/features/` is either tracked with intentional documentation, moved,
  or removed if scratch output.
- `tests/live/dispatch_bicc_smoke.py` is either tracked as a supported live
  smoke test, moved to the right live-test pattern, or removed if scratch.

## P1 - Content-Pack Workflow Improvements

### `[ ]` P1.1 - Strengthen starter project examples

**Why**: New users need concrete, copyable examples for bundle setup, profile
setup, overlays, and one-mart overrides.

**Accept**:
- `examples/minimal-bundle/` stays aligned with `aidp-fusion-autopilot init`.
- `examples/overlay-pack/` documents default `use-pack` alignment and
  `--no-align` for narrow bundles.
- Docs link to examples from `docs/project_setup.md` and
  `docs/mart_overlay_authoring.md`.

### `[ ]` P1.2 - Improve current backlog/roadmap granularity

**Why**: This new backlog intentionally starts small. After the historical docs
are cleaned, active known limitations should be moved here from `LIMITS.md`,
current docs, and open implementation gaps.

**Accept**:
- Active tasks are content-pack/OAC-MCP oriented.
- Obsolete v1 work stays only in archive.
- Each active task has owner-facing acceptance criteria.

### `[ ]` P1.3 - Expand mart and overlay authoring guidance

**Why**: Users can now override a mart SQL/YAML or create a new mart overlay,
but the safest path should be obvious before they edit pack files.

**Accept**:
- `docs/mart_overlay_authoring.md` includes examples for:
  - new gold mart
  - SQL-only override
  - YAML refresh-strategy override
  - `use-pack --no-align`
- `skills/mart-author/SKILL.md` matches the same rules.

## P2 - Validation And Evidence

### `[ ]` P2.1 - Keep AIDPF error-code docs complete

**Why**: Users need one place to understand `AIDPF-*` failures and recovery.

**Accept**:
- A docs check verifies every `AIDPF-*` code referenced in source, skills, and
  starter packs exists in `docs/aidpf-error-codes.md`.
- The check ignores archived historical docs unless explicitly requested.

### `[ ]` P2.2 - Strengthen link validation

**Why**: Docs now link across setup, workflow, overlays, MCP, ADRs, examples,
and archive. Broken links quickly make onboarding unreliable.

**Accept**:
- `make docs-check` verifies local Markdown links for current docs.
- Archive docs can either be checked separately or exempted with a clear rule.

### `[ ]` P2.3 - Content-pack live evidence refresh

**Why**: The current product path is content-pack execution plus OAC MCP-assisted
dashboard authoring. Live evidence should track that path, not old v1 parity.

**Accept**:
- A current live evidence template exists for content-pack seed/incremental,
  overlay `use-pack`, and OAC MCP workbook authoring.
- Existing live tests/results are labeled as historical when they validate old
  transitional behavior.

## P3 - Roadmap

### `[ ]` P3.1 - OAC dataset creation automation research

**Why**: Users still create OAC datasets manually because the MCP/tooling does
not currently expose a reliable create-dataset path.

**Accept**:
- Document the current manual dataset boundary.
- Track any supported OAC API/MCP capability that could safely create datasets.
- Do not remove the manual workflow until the replacement is live-validated.

### `[ ]` P3.2 - Non-demo tenant portability evidence

**Why**: The plugin must work across customer Fusion tenants, not only the demo
or known internal environments.

**Accept**:
- At least one non-demo tenant run validates bootstrap, seed, one overlay path,
  and OAC handoff.
- Evidence is redacted and stored under the live-evidence convention.
