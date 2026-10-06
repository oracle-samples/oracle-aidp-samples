# Contributing — `oracle-ai-data-platform-fusion-autopilot`

> Mechanical PR-grading checklist. The working principles ("how to think about a change") live in [`AGENTS.md`](CODEX.md). Read that first.

---

## Quick start

```bash
# Clone + install in dev mode
git clone <this-repository-url> oracle-ai-data-platform-fusion-autopilot
cd oracle-ai-data-platform-fusion-autopilot

# Editable install (quoted extras — zsh treats `.[dev,test]` as a glob otherwise)
pip install -e '.[dev,test]'

# Run the test suite
make test   # or: pytest -x -q

# Lint + format
ruff check .
ruff format --check .
```

`make test` is the canonical entry point — works regardless of which shell PATH `pytest` lives on (P2.4).

---

## Claiming a backlog item (coordination norm)

Before you start coding a backlog item, **record the claim on the item's issue or your draft PR** — never commit directly to `main`. This is a small commit that flips `[ ]` → `[~]` and names you as the owner. The point is coordination: other contributors checking `BACKLOG.md` on `main` see what's in flight without having to scan open PRs or feature branches.

### When you pick an item up

1. On your feature branch, edit the item's heading in `BACKLOG.md`:
   - `### [ ] P1.X — <title>` → `### [~] P1.X — <title> (in progress — <your-handle>, <YYYY-MM-DD>)`
2. Commit on your branch:
   ```
   git commit -m "fusion-autopilot: BACKLOG — claim P1.X (<your-handle>)"
   ```
3. Push `main` immediately. **Don't batch claims with code** — the claim must land before others start their work, not after yours merges.
4. *Then* branch off `main` for the implementation.

### When you finish

When the implementation PR merges, flip the marker again — same direct-to-`main` discipline:
   - `### [~] P1.X — <title> (in progress — …)` → `### [x] P1.X — <title> (commit <SHA>, live <evidence-SHA>)`

This matches the existing "Closes: BACKLOG.md P<N>" entry in the PR template (§"Commit + PR conventions"). The PR-merge commit and the `[~]` → `[x]` flip can be the same commit; the **claim** at start is the one that must be standalone.

### If you stop working on an item

Flip `[~]` → `[ ]` and remove the owner annotation in a one-line commit on `main`. Releases the lock so someone else can pick it up. Don't leave stale `[~]` markers — they're worse than `[ ]` because they hide availability.

### Why direct-to-main (and not via PR)

The claim is a coordination signal, not a code change. Running it through a PR adds review overhead for a single-line edit that's not technically reviewable. The downside (someone could fight over an item) is unlikely in practice and self-correcting (whoever pushes second sees the conflict and backs off).

---

## Module checklist (new dim or mart)

Use this as the PR template when shipping a new module.

### Code shape

- [ ] Module exposes `build_<name>_sql(*, ...) → str` — pure-string builder, no Spark required. This is the contract unit tests assert against.
- [ ] Module exposes `build(spark, *, ...) → DataFrame` — Spark wrapper that executes the SQL and returns the freshly-written table.
- [ ] All source/target table paths are module-level `Final[str]` **defaults**, overridable via `bronze_table=` / `silver_table=` / `gold_table=` kwargs. The orchestrator threads in the configured paths from `bundle.yaml.aidp.*`.
- [ ] Module-level `__all__` exports the public surface (defaults, builder, `build()`, any `detect_*` helpers).
- [ ] Module docstring documents: bronze column convention (UPPERCASE / PascalCase+prefix), join shape, currency-in-grain rationale, the empty-source behavior.

### Plugin-portability (see [`AGENTS.md`](CODEX.md) §"What varies per tenant")

- [ ] Required columns gated via `KNOWN_*_ALIASES` priority list + `detect_*(spark) → str | None` + `ValueError` on `None` naming the aliases tried.
- [ ] Optional dim attributes use `COALESCE` through alternates, emit NULL when absent. No hard-gate on optionals.
- [ ] Explicit `kwarg=` always wins over detection (use the `auto_detect: bool = True` pattern from `ap_aging.build`).
- [ ] Policy values (bucket boundaries, NET-N residual, fiscal-start-month, FY naming) come from `bundle.yaml`, never probed.
- [ ] Configured SQL identifiers (alias names, segment maps) validated against `^[A-Za-z_][A-Za-z0-9_]*$`. Template: `dim_account._validate_segment_map`.
- [ ] Currency-in-grain enforced on every amount aggregate. Cross-currency rollup is the consumer's concern.

### Medallion correctness

- [ ] `CREATE OR REPLACE` SQL emitted for `refresh_mode="seed"`. `MERGE INTO target USING <filtered-by-watermark> ON target.<natural_key> = src.<natural_key>` emitted for `refresh_mode="incremental"`. Exception: `dim_calendar` is `CREATE OR REPLACE` only.
- [ ] Surrogate keys use `xxhash64(<natural_key>)`, never `monotonically_increasing_id()`.
- [ ] Every arithmetic operation on Fusion amount columns wrapped in `COALESCE(amount, 0)`. NULL-propagation is a known live-evidence bug class.
- [ ] Single financially-correct SQL shape (single LEFT JOIN, fact preserved). No runtime path selection for join topology. Runtime decisions are for data-quality gates only.
- [ ] Audit columns populated: bronze → `_extract_ts` / `_source_pvo` / `_run_id` / `_watermark_used`; silver → `bronze_extract_ts` / `bronze_source_pvo` / `silver_built_at`; gold → `gold_built_at`.

### Performance

- [ ] Large facts (>1M rows projected) declare `PARTITIONED BY` on the dominant slice column (period_year, _extract_date).
- [ ] Gold marts the dashboard reads declare `OPTIMIZE … ZORDER BY (currency_code, <other-filter-cols>)` after each write.
- [ ] Bronze + silver tables set `TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')`. Gold sets `optimizeWrite` only.
- [ ] Decimal casts hoisted into a CTE — don't repeat `CAST(... AS DECIMAL(28,2))` across SELECT and aggregates. Template: `ap_aging`'s `open_invoices` CTE.
- [ ] Normalized columns (e.g. `UPPER(CAST(... AS STRING)) AS currency_code`) projected once in a CTE, referenced by alias afterward. No identical-expression repetition in SELECT + GROUP BY.
- [ ] One bronze scan per build. Probes that share a WHERE clause with the materialization either cache the filtered DataFrame or fold into a single CTE.

### SQL correctness

- [ ] ANSI-safe: `NULLIF(COUNT(*), 0)` for any divisor; explicit casts on overflow-prone aggregates; no implicit string-to-int.
- [ ] `current_timestamp()` only in audit columns — never in WHERE/JOIN/aggregate keys (non-deterministic across stages).
- [ ] Empty-source case unit-tested: zero-row bronze → zero-row dim/mart, correct schema, audit columns populated, no crash.

### Wiring (CLI is the contract — see [`AGENTS.md`](CODEX.md) §"Architecture")

- [ ] Module added to the orchestrator DAG (`scripts/oracle_ai_data_platform_fusion_autopilot/orchestrator/`) in dependency order in the **same PR** as the module itself. Leaf modules without a caller are not accepted.
- [ ] Orchestrator passes resolved 3-part paths from `bundle.yaml.aidp.*` (not the module's `Final[str]` defaults).
- [ ] Orchestrator advances the watermark in `fusion_autopilot_state` after a successful build. Modules never touch `fusion_autopilot_state` directly.
- [ ] `aidp-fusion-autopilot run --mode seed` (from a clean checkout) materializes the new dim/mart end-to-end. If it doesn't, the PR is incomplete.

---

## Tests

### Unit tests

Live at `tests/unit/test_<module>.py`. Use the fake-Spark stub pattern (see `tests/unit/test_ap_aging.py` and `tests/unit/test_dim_account.py` for the template — no real Spark required).

**Required coverage**:
- The pure-SQL builder for every kwarg permutation (default, every variant, edge cases).
- Validation errors: out-of-range positions, invalid SQL identifiers, duplicate aliases, missing required cols.
- Empty-source case (asserted at the SQL-shape level, not just runtime).

**Run**:
```bash
make test                                 # everything
pytest tests/unit/test_<module>.py -v     # one module
pytest -k "test_currency" -v              # filter by name
```

### Live tests (gated)

Live tests sit at `tests/live/test_<feature>_live.py` and the evidence narrative at `tests/live/TC<N>_<feature>_results.md`. They're gated behind `AIDP_FUSION_AUTOPILOT_INTEGRATION=1` so they don't fire in normal CI:

```bash
AIDP_FUSION_AUTOPILOT_INTEGRATION=1 pytest -m live -v
```

**TC numbering** is sequential across the bundle (TC1, TC2, …, TC10h-4, TC22, TC23, TC24, …). Pick the next unused number when adding evidence. Sub-variants append a letter (TC10h, TC10h-2, TC10h-3, TC10h-4). The TC ID goes in:
- The `tests/live/TC<N>_<feature>_results.md` filename.
- The `# TC<N> — <one-line title>` H1 of that file.
- The commit message: `fusion-autopilot: TC<N> — <one-line title>`.
- The `[ ]` → `[x]` transition line in [`BACKLOG.md`](BACKLOG.md) (if applicable).

### Plugin-portability evidence

Per [`AGENTS.md`](CODEX.md): any "portable" claim needs a live run on at least one non-`demo-pod` tenant. The tracked-blocker entries in [`BACKLOG.md`](BACKLOG.md) (P3.7, P3.9) gate this — until a customer / dedicated CI pod is provisioned, the portability claim is provisional and so noted.

---

## Versioning & backwards compatibility

- **New columns in existing dims/marts are non-breaking** — add them at the end of the SELECT projection. Customers' OAC workbooks, downstream notebooks, and dashboards join on column names.
- **Renames and removals require a new module.** `dim_supplier_v2` ships alongside `dim_supplier` until the deprecation window closes. Don't break Type-1 consumers.
- **Type-2 SCD is opt-in via a separate variant module** — `dim_account_history` is a sibling that depends on `dim_account`, not a flag on `dim_account.build()`.
- **PVO names use the full AM-hierarchy from live BICC.** The curated catalog in [`schema/fusion_catalog.py`](scripts/oracle_ai_data_platform_fusion_autopilot/schema/fusion_catalog.py) is the source of truth. Don't paste blog abbreviations.

---

## Security

- **Secrets never inline in `bundle.yaml`.** Always `${vault:OCID}` references; the bundle schema validates and the CLI prints clear errors on plaintext that looks credential-shaped.
- **Mask sensitive values in `debug()` and rich console output.** Truncate tokens, fingerprints, IDCS client secrets. The same rule from the workspace-level `AGENTS.md` AIDP guidance.
- **No credentials in commits, ever.** `.gitignore` blocks the common patterns; if you suspect a leak, rotate immediately and rewrite history.

---

## Commit + PR conventions

### Commit messages

```
fusion-autopilot: <P-id or TC-id> — <one-line summary in imperative voice>

<optional body explaining the why, not the what>
<live-evidence link if applicable>
```

Examples (from `git log`):
- `fusion-autopilot: P1.9 — gold.ap_aging + TC24 live verification`
- `fusion-autopilot: TC23b — live verify of dim_account + gl_balance refactor`
- `fusion-autopilot: plugin-portability — supplier_spend currency detect + ap_aging cancel-date alias`

Atomic commits preferred — one P-id / TC-id per commit so backlog cross-refs are clean.

### Branches

- `main` — release-track. Protected.
- `<contributor>-dev` — long-running feature branches. PRs merge here first for integration, then up to `main` once green.

### Pull-request template

```markdown
## Summary
- <P-id> — <one-line>

## Changes
- <module> — <what changed and why>

## Tests
- Unit: <N> new / <N> total, all passing
- Live: TC<N> — <result summary> (or "deferred — see BACKLOG.md")

## Plugin-portability claims
- [ ] Hardcoded values reviewed against AGENTS.md §"What varies per tenant"
- [ ] Live evidence on non-demo-pod tenant (or noted as deferred)

## Backlog
- Closes: BACKLOG.md P<N> → mark [x] with this commit SHA
- New items added: P<N> — <one-line>
```

---

## Live-test conventions

- **Evidence file**: `tests/live/TC<N>_<feature>_results.md`. Markdown, narrative-first. Capture: tenant identity (pod URL, OAC instance, date), exact commands run, row counts, sample outputs, any anomalies. Pin every claim to a query you actually ran.
- **Tenant identification**: name the pod (e.g. `<pod-name>` / `fusion_autopilot_dev`) at the top of every TC file. The portability story depends on knowing what was tested where.
- **Anomaly handling**: when a live run surfaces something unexpected (NULL-propagation bug, schema variant, performance cliff), file the finding in the TC file AND open a backlog entry. Don't patch silently.
- **Re-verification after refactors**: any code change to a module with an existing TC needs a TC<N>b suffix run before merge. The "I didn't change the SQL" hand-wave isn't sufficient — Catalyst plans shift on adjacent changes.

---

## Useful files for new contributors

| File | Purpose |
|---|---|
| [`AGENTS.md`](CODEX.md) | Working principles — how to think about a change |
| [`README.md`](README.md) | Customer-facing pitch, architecture, quickstart |
| [`STATUS.md`](STATUS.md) | Current-state audit (untracked working notes) |
| [`BACKLOG.md`](BACKLOG.md) | Open items, priorities, decisions (untracked working notes) |
| [`LIMITS.md`](LIMITS.md) | Registry of known L1/L2 caveats per Fusion / AIDP / OAC |
| [`CHANGELOG.md`](CHANGELOG.md) | Per-release decision history |
| [`docs/oac_rest_api_setup.md`](docs/oac_rest_api_setup.md) | One-time IDCS confidential-app setup |
| [`docs/oac_mcp_setup.md`](docs/oac_mcp_setup.md) | Per-user OAC MCP setup |
| [`scripts/oracle_ai_data_platform_fusion_autopilot/schema/fusion_catalog.py`](scripts/oracle_ai_data_platform_fusion_autopilot/schema/fusion_catalog.py) | Curated PVO catalog (source of truth for datastore paths) |
| [`tests/live/`](tests/live/) | Live-evidence trail (TC1..TC24…) |

## Cross-references

- Workspace-level AIDP rules: your workspace `AGENTS.md` (if present)
- Sibling plugin (single-PVO connector skills): `oracle-ai-data-platform-workbench-spark-connectors` in the original source monorepo
