---
started: 2026-05-08
---

# Implementation Plan: AND-combine collector filter with dataset filter

## Overview

Replace the engine's three `collection.filter or filter_expr`
override sites (`QualifireEngine._collect` method,
`qualifire/core/engine.py:1186`; the override calls land at
lines 1231, 1247, 1255 — anchor verification per scope gate 8)
with a single helper that AND-combines the dataset-level and
collector-level filter clauses. The sampler already has this
helper
(`_and_combine` at `qualifire/collection/sampler.py:278`); this
plan promotes it to a shared module so engine + sampler share
one implementation. After the change, the same composition rule
holds across every validator type — `aggregation`, `profiling`,
`metrics`, *and* `sample` — and the asymmetry is gone.

### Stated outcome (from `idea.md`)

```yaml
datasets:
  - name: sales
    table: catalog.schema.sales
    filter: "region = 'US'"           # dataset-level — soft-delete / PII / tenant guard
    validations:
      - type: threshold
        collection:
          type: aggregation
          expressions:
            row_count: "COUNT(*)"     # current dict[str, str] shape
          filter: "status = 'completed'"  # collector-level — narrows for THIS check
```

Today's effective scope: `status = 'completed'` (region filter
silently dropped). Post-change effective scope:
`(region = 'US') AND (status = 'completed')`.

### What this plan is NOT

- **Not** a refactor of `SampleCollectionConfig` —
  `slice_value` / `history.filters` already AND-combine via
  `_and_combine` (sampler.py:278). This plan promotes that
  helper to a shared module so the engine's three collector
  call sites can use the same implementation. Sampler call
  signatures don't change. Sampler *behaviour* changes only
  for whitespace-only operands (the existing helper composed
  `(   ) AND (...)`; the new helper normalises whitespace to
  `None` — Pin 7 fix). Non-whitespace inputs see byte-equal
  output.
- **Not** a change to `custom_query` —
  `CustomQueryCollectionConfig._reject_nonempty_filter` already
  hard-fails for non-empty filter values and silently strips
  empty/whitespace-only ones (config.py:344-388). Operators
  put the predicate in the `WHERE` clause of `sql`. Out of
  scope here.
- **Not** a change to `DatasetConfig.filter` shape, semantics, or
  Jinja rendering. The dataset filter is preserved verbatim;
  it's the *composition* that changes.

## Decision Pins

These resolve the open questions from `idea.md`. The
implementation treats these as inputs.

1. **Direct behaviour change, no deprecation cycle.**
   - Why: today's override behaviour is silently wrong (drops
     dataset-level safety guards). A deprecation cycle that emits
     warnings while keeping the wrong behaviour prolongs the
     incorrectness for one or two minor versions. Operators with
     dual filters today are already authoring an ambiguous
     intent — fix-and-document-the-fix is cleaner than
     warn-without-fix.
   - Migration cost is bounded but not always one-line: in the
     simple case (single validator with both filters set) it's
     a one-line edit. When several validators on the same
     dataset share a `DatasetConfig.filter` and the operator
     wants the *broader* scope on one of them, the migration
     means deleting the dataset-level filter and pushing the
     guard predicate into each remaining validator's
     collector-level `filter`. Documented as a worked example
     in `idea.md` § Backwards-compatibility risk and in the
     CHANGELOG entry (P3.4).
   - The repo's release-note convention (commit body + `shipped.md`
     "Notes / Follow-ups") is the place this lands.

2. **Empty-string filter coerced to `None` at config-load.**
   - Why: `f"({a}) AND ({b})"` with `a = ""` produces
     `"() AND (...)"` — invalid SQL. Today `collection.filter or
     filter_expr` swallows the empty case via Python truthiness;
     the AND form needs explicit handling.
   - Cleanest fix: a Pydantic field validator on the three
     affected models that coerces empty / whitespace-only strings
     to `None`. Single source of truth; the engine helper then
     only deals with `str | None`.
   - Mirrors the `custom_query.filter` empty-tolerance carved out
     in `_reject_nonempty_filter` (round-9 history).

3. **No engine-side WARNING log on first dual-filter run.**
   - Why: noise for operators who *want* the new behaviour (most
     of them). The release note + commit message + `shipped.md`
     entry are the surface for this; the doc updates in
     `docs/validators/validator_collector_matrix.md` carry the
     forever explanation.
   - Operators who want a runtime log get one DEBUG-level line
     per call site (added in P2.2). The line names the dataset
     and collection type only — never the filter strings, which
     can carry tenant identifiers or PII predicates that don't
     belong in default DEBUG output.

4. **The combined filter expression is parenthesised, not
   bare-concatenated.**
   - `f"({dataset}) AND ({collector})"` not `f"{dataset} AND
     {collector}"`. SQL operator-precedence on the left or right
     side could otherwise change the truth value
     (`a OR b AND c` ≠ `(a OR b) AND c`). Operators write filter
     clauses including `OR`; the parenthesisation is non-negotiable.
   - Operands already parenthesised on input get a second pair
     (`and_combine_filters("(a)", "b")` → `"((a)) AND (b)"`).
     Double-parens are SQL-equivalent and not optimised here —
     adding "smart" stripping would mean parsing SQL fragments,
     which is out of scope and risky. A unit test pins the
     literal output (P1.4) so this stays predictable.

7. **Compose post-render, not pre-render.**
   - The sampler's existing `_and_combine` is post-render
     (sampler.py:107-122 — `rendered_filter_expr =
     context.render(self.filter_expr) if self.filter_expr else
     None` then combine). The engine's three call sites
     currently store the *unrendered* filter on the collector
     and the collector renders inside `collect()`. The plan
     follows the sampler pattern: render dataset and collector
     filters separately at the engine call site, then combine.
   - Why this matters (Codex round 1 critical finding): a
     pre-render compose like
     `f"({dataset_template}) AND ({collector_template})"`
     followed by a single Jinja render breaks when one template
     resolves to empty. `filter: "{{ optional_predicate }}"`
     with `optional_predicate=""` ends up as
     `"(rendered_ds) AND ()"` — invalid SQL. Post-render
     compose lets us normalise empties (None / `""` /
     whitespace) on each side BEFORE composing, so the
     combined string is always well-formed.
   - The engine renders each side via `self.context.render(...)`
     before calling `and_combine_filters`. The combined string
     then flows into the collector's `filter_expr` kwarg.
     Inside `collect()`, the collector's existing render call
     (`context.render(self.filter_expr)`) becomes idempotent —
     resolved SQL contains no `{{ }}` markers, so Jinja
     output equals input. No collector signature change.

5. **Helper lives in a shared `qualifire/collection/_filters.py`
   module, exposed as `and_combine_filters` (no leading
   underscore for cross-module use).**
   - Pure string operation, no engine state. Module-level
     placement makes it trivially testable without instantiating
     `QualifireEngine`.
   - The sampler currently owns `_and_combine` at
     `qualifire/collection/sampler.py:278` with identical
     semantics (parenthesised AND, None-tolerant). Promoting it
     to a shared module keeps one implementation; the sampler
     re-imports under its existing private alias to preserve
     backwards-compatible imports inside the package.
   - Why `qualifire/collection/_filters.py` rather than
     `qualifire/core/`: the helper is a post-render string
     operation (Pin 7) conceptually owned by the collection
     layer (where filters are composed), not by orchestration
     logic. Mirrors the existing internal modules under
     `qualifire/collection/` (`_partition.py`,
     `profiling/engine.py` helpers).
   - Module name uses a leading underscore on the file
     (`_filters.py`) to mark it package-private; the function
     name does NOT have a leading underscore so callers across
     `qualifire/` can import it cleanly.
   - **No public re-export.** `qualifire/collection/__init__.py`
     stays empty; callers import from the leaf module
     explicitly (`from qualifire.collection._filters import
     and_combine_filters`). Mirrors how
     `qualifire/storage/_predicates.py` is imported.
   - **No circular-import risk.** `_filters.py` is pure string
     manipulation — no imports from `qualifire.core.*`,
     `qualifire.collection.*`, or any peer module. The engine
     imports `_filters` (one-way); the sampler imports
     `_filters` (one-way); `_filters` imports nothing from
     either. P1.1 explicitly verifies the empty top-level
     imports of the new file.

6. **Reuse the existing `_and_combine` semantic — don't fork.**
   - The sampler's helper is the source of truth for the
     parenthesised AND form (the same form Decision Pin 4
     mandates). Adding a second, near-identical helper in
     `engine.py` would create two implementations that could
     drift. The plan moves the existing function and keeps the
     private alias `_and_combine` inside `sampler.py` pointing
     at the moved function (P1.2). The sampler's two call
     sites at lines 122 and 249 stay byte-equal.
   - One deliberate semantic divergence: the moved helper
     normalises whitespace-only operands to `None` (Pin 7
     defence). The pre-move sampler `_and_combine`
     truthiness-skipped empty strings only — `"   "` stayed
     truthy and would compose to `(   ) AND (...)`. Sampler
     callers never relied on that broken case (`grep -rn
     _and_combine` returns only the call sites and definition,
     no tests). The whitespace regression test added in P1.2
     pins the new behaviour from the sampler's side too.
   - No tests existed for `_and_combine` directly. The truth
     table that P1.4 adds is the first direct test coverage.

## Scope gates (hard-stop conditions)

Implementation MUST stop and escalate to the user if any of the
following emerge:

1. **Scope gate — `sample` *behaviour* unchanged for non-empty
   operands; whitespace-only normalisation explicitly accepted.**
   The sampler's existing `_and_combine` (sampler.py:278-288)
   already truthiness-skips empty strings (`if a and b:`), so
   the plan's helper preserves identical output for any
   non-empty / non-whitespace operand pair. Whitespace-only
   operands (`"   "`) DO change semantically — the new helper
   normalises them to `None` instead of composing
   `(   ) AND (...)`. This is a deliberate fix to the same
   bug class Pin 7 catches. A sampler regression test (added
   in P1.2) asserts `_and_combine("region = 'us'", "  ")`
   returns `"region = 'us'"` rather than the
   parenthesised-with-whitespace form. *Other* sampler
   semantics — slice-anchor math, history-filter override,
   per-slice predicate composition — are NOT touched. If a
   design pressure appears to restructure those, stop and
   escalate.
2. **Scope gate — `custom_query` semantics unchanged.** The
   field is already config-load-rejected; this feature does not
   touch that decision. Out of scope.
3. **Scope gate — no new validator/collector types.** Filter
   composition is a runtime change; no new model classes.
4. **Correctness gate — empty-filter coercion is in TWO places:
   Pydantic at config-load, AND the helper at runtime.** The
   Pydantic layer normalises static YAML (`filter: ""` →
   `None`). The helper additionally treats empty / whitespace-
   only operands as `None` so a Jinja template like
   `filter: "{{ optional_predicate }}"` that renders to `""`
   at runtime composes correctly (Pin 7). Both layers exist
   because rendered-empty cannot be caught at config-load.
5. **Correctness gate — parentheses on both sides.** A test
   asserts that `and_combine_filters("a OR b", "c OR d")`
   produces `"(a OR b) AND (c OR d)"`, not
   `"a OR b AND c OR d"` (which would change the truth value).
6. **Correctness gate — compose post-render, not pre-render.**
   The dataset filter and collector filter are rendered
   *separately* in the engine call site (`self.context.render(...)`
   on each), then handed to `and_combine_filters` which
   normalises empty / whitespace operands to `None` before
   composing. The combined string is then handed to the
   collector via the existing `filter_expr` kwarg; the
   collector's internal `context.render(self.filter_expr)`
   becomes idempotent on resolved SQL. This is the only correct
   shape — pre-render compose collapses operand boundaries too
   early and produces `() AND (...)` when one Jinja template
   resolves to empty (Codex round 1 critical finding).
7. **Hard-stop — no behaviour change in the single-filter case.**
   When only one of dataset / collector filter is set, the
   resulting filter must be byte-equal to that single filter.
   Otherwise we silently break every existing config that used
   only one layer. Test asserts both single-side cases.
8. **Anchor verification before edit.** Line numbers in this
   plan (engine.py:1231/1247/1255, sampler.py:278,
   config.py:165/190/228/233/242/245/267/344) are anchors as of
   the rebase against main on 2026-05-09. Before each edit,
   re-locate the target via `grep -n` so the implementation
   doesn't land on the wrong line. The anchor lines have been
   stable for weeks but unrelated PRs can still shift them.

## Implementation Phases

Phase 1 is independently shippable. It is mostly additive —
new helper, new Pydantic coercion — with a *small* deliberate
behaviour change for whitespace-only filter values: today they
pass through to the backend as `WHERE   `-shaped clauses
(parser-rejected on most engines); after P1.3 they coerce to
`None` at config load. P1.2 adds the same normalisation to the
sampler's helper alias (whitespace-only operand → None instead
of composing `(   ) AND (...)`). The runtime override-vs-AND
composition rule is unchanged in Phase 1 — that's Phase 2.

**Phase 2 and Phase 3 must ship together** (Codex round 1
finding): Phase 2 is the breaking override-to-AND behaviour
change; Phase 3 is the operator-facing documentation that
makes the change discoverable. Splitting them would land a
silent breaking change with no migration surface. Each phase
is reviewable / revertible in isolation, but the release that
ships Phase 2 must include Phase 3 in the same commit / PR.

### Phase 1 — Promote helper + empty-string Pydantic coercion

Goal: move the existing `_and_combine` to a shared module under
the public name `and_combine_filters`, ship the empty-string →
None coercion at config load. No engine call sites changed yet
— so the runtime composition rule (still override) is identical
to today. Note: the Pydantic coercion in P1.3 is a small
config-load behaviour change for whitespace-only filters
(today: passes through verbatim and overrides the dataset
filter; post-P1.3: coerced to `None` so the dataset filter
applies). The pre-fix shape was already buggy — a filter of
`"   "` would push `WHERE` followed by whitespace to the
backend, which most parsers reject. Calling out explicitly in
the P1.3 change-log entry.

- [ ] **P1.1** — Create
      `qualifire/collection/_filters.py`:
    - `def and_combine_filters(dataset_filter: str | None,
      collector_filter: str | None) -> str | None`
    - **Normalise each operand first**: `None`, `""`, or
      whitespace-only → treated as None for composition. This is
      a slight superset of the current sampler `_and_combine`
      semantics (which truthiness-checks via `if a and b:` —
      empty string is already falsy, but whitespace-only `"  "`
      is *truthy* and would compose to `(   ) AND (...)`).
      The new helper explicitly strips and tests, fixing the
      whitespace-only edge case.
    - Composition: both empty → None; one set → that side
      verbatim (no extra parens); both set →
      `f"({dataset_filter}) AND ({collector_filter})"` with the
      *original* (pre-strip) text on each side preserved.
    - Docstring names the pre-2026-05-09 engine override
      behaviour, points at Decision Pins 4 and 7 for the
      parenthesisation + post-render contract, and notes that
      runtime callers should pass already-rendered strings
      (Pin 7).
- [ ] **P1.2** — `qualifire/collection/sampler.py`:
    - Replace the local `_and_combine` definition with
      `from qualifire.collection._filters import and_combine_filters as _and_combine`
      (private alias keeps existing call sites at lines 122 and
      249 byte-equal).
    - Add a one-line whitespace-regression test in
      `tests/test_collection/test_sampler.py`:
      ```python
      def test_and_combine_treats_whitespace_as_empty():
          from qualifire.collection.sampler import _and_combine
          assert _and_combine("region = 'us'", "   ") == "region = 'us'"
      ```
      Pins the deliberate semantic divergence called out in
      Scope gate 1 — whitespace-only filter no longer composes
      to `(region = 'us') AND (   )`.
    - Verify the sampler's two call sites and the existing
      sample / pattern / shape integration tests still pass
      without further edits.
- [ ] **P1.3** — `qualifire/core/config.py` Pydantic field
      validator on `AggregationCollectionConfig.filter`
      (line ~190), `ProfilingCollectionConfig.filter`
      (line ~233), `MetricsCollectionConfig.filter` (line ~245):
    - Define one module-level normaliser
      (`def _normalize_filter(v: str | None) -> str | None`)
      that returns `None` for `None`, `""`, or whitespace-only;
      otherwise returns `v` unchanged. Reuse it across all
      three model validators — the implementer chooses whether
      to attach via the existing
      `_check_dims = field_validator(...)(staticmethod(...))`
      precedent (config.py:151, 172, 181) or a stacked decorator
      shape, as long as the three models share one normaliser
      function.
    - Mirrors the empty-tolerance carved out in
      `CustomQueryCollectionConfig._reject_nonempty_filter`
      (config.py:344-388). The precedent uses
      `model_validator(mode="before")`; this plan stays at the
      field level because the logic targets a single field.
- [ ] **P1.4** — Tests in `tests/test_collection/test_filters.py`
      (new):
    - `and_combine_filters` truth table: None/None → None,
      set/None → set verbatim, None/set → set verbatim, set/set
      → `(a) AND (b)`, OR-on-each-side parenthesisation
      (`a OR b` and `c OR d` → `(a OR b) AND (c OR d)`).
    - **Empty-string operand** —
      `and_combine_filters("", "b")` and
      `and_combine_filters("a", "")` and
      `and_combine_filters("", "")` all behave as if the empty
      side were `None`. Pinned literally so the helper is safe
      to call with rendered-empty Jinja outputs.
    - **Whitespace-only operand** —
      `and_combine_filters("   ", "b")`,
      `and_combine_filters("a", "  \t  ")`, and
      `and_combine_filters(" ", "\n")` behave the same as the
      empty-string case (whitespace normalises to `None`).
      Whitespace can leak in via Jinja outputs that emit a
      space-only string (`"{{ '' if optional is none else
      optional }}"`-shaped templates) and would otherwise
      compose to `(   ) AND (...)`.
    - **Double-parenthesisation case** —
      `and_combine_filters("(a)", "b")` returns
      `"((a)) AND (b)"`. Pinned literally because Decision Pin 4
      explicitly accepts the double-parens cost over SQL-fragment
      parsing.
    - Pydantic empty-coercion: `filter=""`, `filter=None`,
      `filter="   "` all land as `None` on
      `AggregationCollectionConfig`, `ProfilingCollectionConfig`,
      `MetricsCollectionConfig`. `filter="x = 1"` passes through
      unchanged.
    - Sampler import-alias regression (one line): assert
      `qualifire.collection.sampler._and_combine
      is qualifire.collection._filters.and_combine_filters`.
      Pins Pin 6's "no fork" guarantee. *Lives in*
      `tests/test_collection/test_filters.py` alongside the
      truth-table tests so the helper-side guarantees are
      collocated; the whitespace-regression assertion
      added in P1.2 stays in `tests/test_collection/test_sampler.py`
      because it tests sampler-side wiring, not helper-side
      identity.
- [ ] **Exit:** `pytest -q tests/test_collection/test_filters.py
      tests/test_collection/test_sampler.py` passes; engine
      collector call sites at lines 1231, 1247, 1255 still use
      the old override-form composition (the override-to-AND
      switch is Phase 2). The only behaviour change in Phase 1
      is the deliberate whitespace-only normalisation called
      out at the top of this section.

### Phase 2 — Engine call site swap

Goal: replace the three override call sites with the shared
helper. This is the phase where behaviour changes.

- [ ] **P2.1** — `qualifire/core/engine.py` post-render compose
      at the three call sites (lines 1231, 1247, 1255 inside
      `QualifireEngine._collect`). Add the import
      `from qualifire.collection._filters import
      and_combine_filters` near the existing collector imports.
      Replace each call site's
      `filter_expr=collection.filter or filter_expr` with:
      ```python
      _ds_rendered = self.context.render(filter_expr) if filter_expr else None
      _co_rendered = self.context.render(collection.filter) if collection.filter else None
      _combined = and_combine_filters(_ds_rendered, _co_rendered)
      collector = AggregationCollector(   # or Profiling/Metrics
          ...,
          filter_expr=_combined,
          ...
      )
      ```
      The collector's existing `context.render(self.filter_expr)`
      becomes idempotent on resolved SQL (no `{{ }}` markers
      left to substitute). No collector signature changes.
- [ ] **P2.2** — `qualifire/core/engine.py` add a single
      `logger.debug` line per call site capturing
      *non-sensitive presence flags only* — never the filter
      strings themselves (Decision Pin 3). Example:
      ```python
      logger.debug(
          "filter compose: dataset=%s collection=%s "
          "ds_filter_present=%s co_filter_present=%s",
          ds_config.name, collection.type,
          filter_expr is not None,
          collection.filter is not None,
      )
      ```
      A test (P2.3) uses `pytest`'s `caplog` to assert the log
      record contains the dataset name + collection type but
      does **not** contain any filter literal substring (e.g.
      the test's filter `"region = 'US'"` must not appear in
      any DEBUG record).
- [ ] **P2.3** — End-to-end integration tests in
      `tests/test_collection/test_filters.py` against `PandasBackend`:
    - dataset filter only → row count matches dataset scope
    - collector filter only → row count matches collector scope
    - both filters → row count matches the **AND** scope (key
      regression — proves we changed behaviour from override to
      AND)
    - same shape replicated for `aggregation`, `metrics`,
      `profiling` collectors (three call sites = three test
      paths).
    - Jinja-rendered each side: `filter: "region =
      '{{ tenant }}'"` on dataset side, `filter: "ds =
      '{{ ds }}'"` on collector side, both render then AND.
    - **Rendered-empty Jinja** — dataset filter set,
      collector `filter: "{{ optional_predicate }}"` with the
      context variable resolving to `""`. Expected: combined
      filter equals the rendered dataset filter; no
      `() AND (...)` SQL leaks to backend. Symmetric test with
      empty side flipped. Three collector types ⇒ three
      symmetric pairs ⇒ six tests total. This is the regression
      that pre-render compose would silently break (Pin 7).
    - **Single-rendered-empty operand** — both filters set,
      one renders to `""` at runtime. Expected: combined
      filter equals the non-empty side verbatim, no parens
      around an empty operand.
    - Edge case: `DatasetConfig.filter=None` +
      `collection.filter` set → byte-equal to
      `collection.filter` (no parenthesisation, no `AND`).
    - Edge case: `DatasetConfig.filter` set +
      `collection.filter=None` → byte-equal to
      `DatasetConfig.filter`.
    - **Logging redaction** — caplog assert that DEBUG log
      record does NOT contain any operand's literal SQL
      (`"region = 'US'"`, `"status = 'returned'"`, etc.) for
      one chosen call site (aggregation is enough; the helper
      log line is the same for all three).
- [ ] **P2.4** — `skip_if_cached` interaction. Cached
      collection rows persisted under the pre-change override
      semantics could be replayed under the new AND semantics
      and produce a metric value computed against the wrong
      scope.
      - **Decision (Codex round 2 pin)**: bypass
        `_try_skip_if_cached` for any collector whose
        resolved-combined filter is non-`None` — i.e. any
        collector that uses `filter_expr` at all. The cache
        short-circuit was always a per-metric optimisation;
        suppressing it for filtered collectors is a no-op for
        unfiltered datasets and a correctness win for filtered
        ones. The runtime cost is one extra collection per
        filtered metric per cached run; acceptable given the
        alternative is a system-table schema migration.
      - **Why not "stable hash in cache key"**: that path
        requires changes to `read_collection_metric_at_partition`
        across SQLite, Delta, JDBC, and external-catalog
        backends, plus a new system-table column for
        `effective_filter_hash`, plus migration handling for
        existing rows. That's a separate feature; capture it
        as `cache-key-filter-identity` in the backlog (idea-only)
        as a follow-up.
      - **Implementation site**: add the bypass condition
        inside `_try_skip_if_cached` (engine.py:1298+) — return
        `None` early when the resolved combined filter for the
        collector is non-`None`. Keep the existing return paths
        (rule-already-fired, no-history, etc.) intact.
      - **Test (in `tests/test_collection/test_filters.py`)**:
        seed the system table with a collection row that would
        match the metric, run with `skip_if_cached=True` and a
        dual-filter config, assert
        `_try_skip_if_cached` returns `None` so the engine
        re-collects under the new combined scope. A second
        test exercises the *unfiltered* path to confirm the
        bypass doesn't trigger for collectors with no filter.
- [ ] **Exit:** `pytest -q` passes (full sweep), including the
      new rendered-empty Jinja, logging-redaction, and
      `skip_if_cached` tests.

### Phase 3 — Documentation

- [ ] **P3.1** — `docs/validators/validator_collector_matrix.md`:
      new "Filter precedence" section after "What happens when
      you pair them anyway" (the last existing top-level
      section). Spells out the AND-combine rule, contrasts with
      the pre-change override behaviour, includes a worked YAML
      example using current dict-form expressions, and points at
      `qualifire/collection/_filters.py:and_combine_filters`
      for the authoritative definition.
- [ ] **P3.1b** — Stale collector-doc cleanup (Codex round 1
      finding). Update the four docs that explicitly state the
      old override contract — they're the most-likely place an
      operator reads after upgrade and gets the wrong answer:
      - `docs/collectors/aggregation.md:27` —
        `| filter | ... | overrides DatasetConfig.filter for this validation |`
        → rewrite to "AND-combined with `DatasetConfig.filter`"
      - `docs/collectors/profiling.md:32` — same fix.
      - `docs/collectors/metrics.md:34` — same fix.
      - `docs/collectors/README.md:66` — sentence currently
        reads "DatasetConfig.filter is a SQL fragment AND-applied
        to every …"; verify wording stays accurate post-change
        and add a one-line cross-reference to the validator
        matrix's Filter Precedence section.
      - Also `docs/configuration.md` and `docs/jinja_rendering.md` — grep
        for `DatasetConfig.filter` and update any sentences that
        describe the override semantic.
      Acceptance grep:
      `grep -rn "overrides DatasetConfig.filter" docs/` returns
      zero hits after this task.
- [ ] **P3.2** — `docs/validators/README.md`: one-paragraph
      "Filters" subsection cross-referencing P3.1, mentioning the
      AND-combine rule by name.
- [ ] **P3.3** — Inline docstring updates on
      `AggregationCollectionConfig` (config.py:165),
      `ProfilingCollectionConfig` (config.py:228),
      `MetricsCollectionConfig` (config.py:242). One-paragraph
      each, naming the AND-combine rule and pointing at the
      matrix doc. Mirrors the precedent set by
      `SampleCollectionConfig` docstring at config.py:267-269.
- [ ] **P3.4** — `docs/CHANGELOG.md`: add entry under the
      existing `## Unreleased` › `### Breaking` heading.
      Concrete migration shape from `idea.md`'s
      "Backwards-compatibility risk" lifted into the changelog,
      including the multi-validator worst case from Pin 1.
- [ ] **P3.5** — Manual-notebook audit. Run
      `grep -nE 'filter:' tests/manual/*.ipynb`. For each hit
      that uses both a dataset-level and collector-level filter,
      either (a) update the YAML cell to the new AND-combine
      semantic and re-render the seeded outputs, or (b) leave
      unchanged but add a markdown cell above noting the cell
      pre-dates the AND-combine change. Manual notebooks are
      not in CI; this is best-effort documentation hygiene.
- [ ] **Exit:** `grep -r "AND-combine" docs/validators/` returns
      hits in both `README.md` and `validator_collector_matrix.md`;
      `grep -r "or filter_expr" qualifire/` returns no hits in
      production code (only in test/doc strings explaining the
      old behaviour); `docs/CHANGELOG.md` lists the breaking
      change under "Unreleased › Breaking"; `tests/manual/`
      audit complete.

## Testing Strategy

| Layer | What's tested | File |
|---|---|---|
| Unit | `and_combine_filters` truth table + parenthesisation | `tests/test_collection/test_filters.py` (new) |
| Unit | Empty + whitespace-only operand → None composition | `tests/test_collection/test_filters.py` |
| Unit | Pydantic empty-string coercion on three models | `tests/test_collection/test_filters.py` |
| Unit | Sampler import-alias regression (`sampler._and_combine is _filters.and_combine_filters`) | `tests/test_collection/test_filters.py` (lives with the helper-side tests) |
| Unit | Sampler whitespace divergence regression (`_and_combine("region = 'us'", "   ") == "region = 'us'"`) | `tests/test_collection/test_sampler.py` (sampler-side wiring) |
| Integration | Three collector paths × three filter combos (dataset-only / collector-only / both) | `tests/test_collection/test_filters.py` |
| Integration | Jinja-rendered filters on each side, including rendered-empty regression | `tests/test_collection/test_filters.py` |
| Integration | `skip_if_cached` runtime-bypass: filtered collector forces re-collection, unfiltered collector still cache-hits | `tests/test_collection/test_filters.py` |
| Integration | DEBUG log redaction (caplog asserts no filter literal SQL appears) | `tests/test_collection/test_filters.py` |
| Doc | `validator_collector_matrix.md` + `README.md` + `CHANGELOG.md` + four collector docs reference AND-combine | manual review |
| Regression | Existing 1,170+ tests still pass; codebase scan (`grep -rn "filter:.*\nfilter:" tests/`) confirmed no test relies on dual-filter override semantics | full pytest sweep |

## Acceptance / Exit criteria

The feature is shippable when **all** of these hold:

1. ✅ `pytest -q` passes; new tests cover every Decision Pin.
2. ✅ `and_combine_filters` produces correct results for the
   four truth-table combinations, the OR-on-each-side
   parenthesisation case, the empty-string-operand case (Pin 7
   defence), and the double-parens preservation case.
3. ✅ All three engine collector call sites (engine.py:1231,
   1247, 1255) use post-render compose via the helper; zero
   `or filter_expr` strings remain in `engine.py`.
4. ✅ The sampler's call signatures and non-whitespace
   semantics are unchanged. `_and_combine` in sampler.py is a
   re-export alias of
   `qualifire/collection/_filters.py:and_combine_filters`. The
   one deliberate divergence (whitespace-only operands now
   normalise to `None`) is pinned by the regression test
   added in P1.2; existing sample / pattern integration tests
   still pass.
5. ✅ Empty / whitespace-only filter on any of the three models
   coerces to `None` at config-load (Pydantic round-trip test).
6. ✅ End-to-end integration test on `PandasBackend` confirms
   AND-scope row count when both filters are set, matching the
   `idea.md` worked example.
7. ✅ **Rendered-empty Jinja regression test** —
   `filter: "{{ optional_predicate }}"` with the variable
   resolving to `""` does NOT produce `() AND (...)` SQL on the
   backend. Verified for all three collector types.
8. ✅ **`skip_if_cached` runtime-bypass handled** —
   `_try_skip_if_cached` returns `None` (i.e. forces
   re-collection) for any collector whose resolved combined
   filter is non-`None`, regardless of whether the dataset or
   collector contributed the filter. This prevents override-
   era cached rows from silently replaying under the new AND
   scope. Two tests pin the behaviour: filtered-collector path
   bypasses the cache; unfiltered-collector path still hits
   the cache. Cache-key filter identity is captured as a
   separate backlog item (`cache-key-filter-identity`); not
   shipped with this feature.
9. ✅ **Logging redaction verified** — caplog assertion confirms
   no DEBUG record contains a filter literal SQL fragment.
10. ✅ `docs/validators/validator_collector_matrix.md` documents
    the rule with an example; `docs/validators/README.md`
    cross-references it; `docs/collectors/aggregation.md`,
    `metrics.md`, `profiling.md`, and `README.md` no longer say
    "overrides DatasetConfig.filter" (verified via
    `grep -rn "overrides DatasetConfig.filter" docs/` returning
    zero hits); the three Pydantic models' docstrings match;
    `docs/CHANGELOG.md` lists the breaking change under
    "Unreleased › Breaking".
11. ✅ Commit message ends the implementation phase clearly
    marking it as a **breaking behaviour change** with the
    migration shape from `idea.md`. (`shipped.md` itself is
    written by the `/feature-ship` skill at merge time, not by
    the implementation; it lifts the migration shape from the
    final commit body.)

## Out of Scope (explicit non-goals)

- `sample` collector — already AND-combines via the helper this
  plan promotes. Behaviour identical for non-whitespace
  operands; whitespace-only operands now normalise to `None`
  (intentional fix per Pin 7). Pinned by the P1.2 regression
  test.
- `custom_query` collector — `filter` is already config-load
  rejected.
- `recency` collector — receives only the dataset filter via
  engine plumbing; no collector-level `filter` field exists.
  Unchanged.
- Cross-validator filter sharing (one filter per dataset block
  applied to multiple validators). Operators can already do this
  by setting `DatasetConfig.filter` and *not* setting collector
  filters; not a new feature.
- Filter expression validation / SQL parsing. The current
  pass-through-to-backend behaviour is preserved — Spark / pandas
  reject malformed filters at execution time, which is the
  pre-change contract.
- A deprecation-cycle path (one minor version of warnings before
  the change). Decision Pin 1 — direct change.

## Risks

- **Operators with existing dual-filter configs see narrower row
  scopes on first run after upgrade.** Mitigation: clear
  release-note language, the worked example in `idea.md`, and the
  one-line migration shape. Quantitative risk: low — the codebase
  scan in pre-flight confirmed zero existing tests assume the
  override semantics, suggesting the dual-filter case is uncommon
  in practice.
- **Surprise on `OR` parenthesisation.** Operators writing filters
  with `OR` clauses see the engine inject parentheses. Could
  produce a different SQL plan than they expected (rare —
  optimisers fold parens). Mitigation: the parenthesisation is
  documented in `validator_collector_matrix.md`'s Filter
  Precedence section; the helper docstring explains it.
- **Manual notebooks may reference the old override semantics.**
  `tests/manual/notebook_end_to_end.ipynb` and
  `tests/manual/notebook_overall.ipynb` showed up in the
  audit grep (`grep -rln "filter:" tests/manual/`). They're
  manual / not part of CI, so test failures won't break the
  build, but operators copy from them. P3.5 (added below)
  audits and either updates or annotates them.
- **Config-load behaviour change for empty-string filters.**
  Today `filter: ""` is silently treated as no filter (Python
  truthiness). Post-change, the Pydantic coercion explicitly
  normalises to `None`. Same observable behaviour, slightly
  different model state. No operator-visible regression expected.

## Effort estimate

Small-to-medium. After Codex round 1 expanded scope (rendered-
empty Jinja, `skip_if_cached` interaction, log redaction, stale
collector docs), realistic estimate is **~6–8 hours** of code +
tests + docs, broken across 3 phases. Approximate breakdown:

- ~50 lines runtime code (helper + three engine post-render
  blocks + field validator + `_try_skip_if_cached` runtime
  bypass in P2.4).
- ~300 lines tests (truth table + Pydantic coercion + three
  collector × three filter combos + rendered-empty Jinja six
  tests + caplog redaction + skip_if_cached regression).
- ~80 lines documentation prose (matrix Filter Precedence
  section, four collector doc updates, three model docstrings,
  CHANGELOG entry).
- Manual notebook audit (P3.5) — 30 minutes if there are no
  dual-filter cells, longer if any need re-rendering.
