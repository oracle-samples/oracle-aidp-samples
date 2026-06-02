# Plan: webhook-payload-redaction

## Goal

Stop SHAP-named columns from leaking into any egress channel
(notifications, system table, anywhere `details_json` lands) by
redacting them at compute time, not egress time. Operators in
regulated industries (HIPAA / GDPR / PCI) can list sensitive
column names; the redacted value never enters
`details["top_contributing_features"]`,
`details["value_drift_explainer"]`, or anywhere downstream.

## Locked Decisions (operator's recommendation)

1. **Compute-time redaction**, not egress-time. Apply inside
   `pattern_check.py` and `isolation_forest.py`, AFTER
   `explain_value_drift(...)` returns — see "Source-column
   lookup + ordering" below for the strict sequence. The
   `_drift_explainer.py` itself is NOT modified; it needs real
   names to look up the encoding_map. Defense in depth: the
   redacted name never enters details, so any future egress path
   (system table, dashboard, programmatic API, third-party
   webhook) automatically inherits the protection.

2. **Both allowlist + denylist** — denylist takes precedence.
   Denylist is the ergonomic default ("redact these N columns
   from outbound"); allowlist is the locked-down posture
   ("only these K columns can leave"). Both ship; denylist's
   semantics override the allowlist where they overlap.

3. **Two attachment points: global + dataset-level.** Skip
   validation-level (premature granularity — operators don't
   ask for "redact column X for this one check"). Effective set
   per dataset = global denylist ∪ dataset denylist; effective
   allowlist = global allowlist ∩ dataset allowlist (i.e.,
   strictest scope wins). When the allowlist is set, columns
   not in it are redacted.

4. **Placeholder `<redacted>`** for redacted output. Cheaper
   than hashing; no reversibility risk; alert-grouping value
   below the cost. The redacted feature appears in
   `top_contributing_features` as
   `{"feature": "<redacted>", "importance": 0.42}` so the count
   stays meaningful. Drift-explainer entries get their
   identifying fields (`feature`, `source_column`, `kind`,
   `category`, `summary`) replaced with `<redacted>`; the
   numeric `current` / `past` / `delta` blocks stay intact —
   they don't reveal column identity, only frequency stats.

5. **System-table writes inherit the redaction.** Compute-time
   means the redaction lives at the source; the system-table
   write is downstream of `details`. No special-casing.

6. **Retroactive cleanup is out of scope.** Operators adding a
   column to the denylist after rows have shipped: the
   already-persisted rows stay as-is. Future runs honor the
   list. Deferred to a separate `redacted-history-scrub`
   backlog item if anyone asks.

## What Changes

### Public API

- **`qualifire/api.py:Qualifire.__init__(...,
  redacted_columns: list[str] | None = None,
  allowlist_columns: list[str] | None = None)`** — new instance-
  level options. Read from any/all `Qualifire(...)` constructor
  call paths (`Qualifire(...)` direct / `Qualifire.from_config`).
- **`qualifire/core/config.py:DatasetConfig`** — add
  `redacted_columns: list[str] = []` and
  `allowlist_columns: list[str] | None = None` fields. Pydantic
  loaded from YAML. Empty list / `None` = unset (no policy).

### Effective-set helper

- New `qualifire/core/_redaction.py`:
  - `def redacted(column: str, *, denylist: set[str], allowlist: set[str] | None) -> bool`
  - Single source of truth for the policy decision; called
    everywhere a column name is about to enter `details`.
  - `denylist` precedence: a column in the denylist is always
    redacted regardless of allowlist membership.
  - When `allowlist is None`, every column passes the allowlist
    check (legacy behavior). When `allowlist` is set, columns
    NOT in it are redacted.

### Validation-side wiring

The validators receive a `RedactionPolicy` via `__init__` from
the engine. Apply it AFTER `explain_value_drift(...)` returns —
see "Source-column lookup + ordering" below for the strict
sequencing.

- **`qualifire/validation/pattern_check.py`** —
  - SHAP top-features list (line 419-425): build with REAL names,
    feed to `explain_value_drift`, then post-hoc walk both lists
    and replace per-entry identifying fields with `<redacted>`.
  - Schema-change list (line 208 area): pre-explainer site —
    direct denylist lookup on the raw source column names.
    Redact both the `details["new_columns"]` /
    `dropped_columns` / `inconsistent_past_columns` lists AND
    the `message` interpolation (line 202 area) — codex R1
    MAJOR. The message must show
    `Schema change: new=['<redacted>'], dropped=[...]`, etc.
- **`qualifire/validation/isolation_forest.py`** — same shape.
  - SHAP top-features list (line 293-296): post-explainer redaction.
  - Schema-change list (line 196 area, message at line 190 area):
    same as pattern_check (codex R1 MAJOR — both validators leak
    identically; both must redact identically).
- **`qualifire/validation/_drift_explainer.py`** — explainer
  itself stays unchanged (no policy threading needed).
  Redaction happens by the validator AFTER the explainer
  returns. Each entry's `source_column` is read directly; if
  redacted, the validator replaces identifying fields:
  `{"feature": "<redacted>", "source_column": "<redacted>",
   "kind": "<redacted>", "summary": "<redacted>"}`. Numeric
  `current` / `past` / `delta` blocks stay intact.

### Engine — redaction-set assembly

- **`qualifire/core/engine.py`** — at validator instantiation
  (lines 1630, 1651), compute the effective `RedactionPolicy`
  per dataset and pass to the validator's `__init__`:

  **Denylist** (additive, strictest-wins):
  ```
  effective_denylist = (instance.redacted_columns or set())
                     ∪ (ds_config.redacted_columns or set())
  ```

  **Allowlist** (codex R1 MEDIUM — pinned semantics for one-
  sided configuration):
  ```
  if instance.allowlist is None and ds.allowlist is None:
      effective_allowlist = None     # no allowlist policy
  elif instance.allowlist is None:
      effective_allowlist = ds.allowlist     # dataset-only
  elif ds.allowlist is None:
      effective_allowlist = instance.allowlist     # instance-only
  else:
      effective_allowlist = instance.allowlist ∩ ds.allowlist
  ```
  Rationale: `None` = "no allowlist policy at this scope, so
  the other scope governs". Set on both = strictest scope wins
  (intersection). This avoids accidentally widening the
  allowlist by adding a dataset-level scope.

  The `RedactionPolicy` exposes a single
  `.redacted(source_column) -> bool` method.

### Source-column lookup + ordering (R1 MEDIUM + R2 MEDIUM)

SHAP `feature_names` carry POST-encoded names (`channel_mobile`).
The denylist is on SOURCE columns (`channel`). Pattern_check has
`encoding_map: dict[encoded_name, EncodedFeatureSpec]`; the
spec carries `.source_column`.

**Critical ordering** (codex impl-review trap): the redaction
runs AFTER `explain_value_drift(...)` returns. The explainer
relies on the un-redacted `feature` field to look up
`encoding_map.get(feat_name)` (see
`qualifire/validation/_drift_explainer.py:74`); redacting
upstream would feed `<redacted>` into the lookup and trip the
mapping_errors path. Sequence:

1. Build `top_features` with REAL names (existing code path).
2. Call `explain_value_drift(top_features, ...)` → drift_entries.
3. Walk both lists post-hoc and apply redaction in place:
   - For each `top_features[i]`: look up source column via
     `encoding_map[name].source_column`; if redacted, replace
     `feature` with `"<redacted>"`.
   - For each `drift_entries[i]`: read entry's existing
     `source_column` field directly; if redacted, replace
     identifying fields with `"<redacted>"` (numeric stats stay).
4. Emit redacted lists into `details`.

### Schema-change keys also redact

Captured in the per-validator wiring section above
(both `pattern_check.py` and `isolation_forest.py`).
Same denylist lookup; replace list entries with `"<redacted>"`
AND redact the message string interpolation
(codex R1 MAJOR — message body leaks the same names).

### Tests

- New `tests/test_validation/test_pattern_check_redaction.py`:
  - T1: column in denylist → SHAP top entry shows
    `feature="<redacted>"`, `importance` unchanged.
  - T2: column NOT in denylist → SHAP top entry shows the real
    name (regression for non-redacted columns).
  - T3: allowlist set, column NOT in allowlist → redacted.
  - T4: column in BOTH allowlist AND denylist → redacted
    (denylist precedence).
  - T5: drift explainer entry for a redacted column → the
    identifying fields are `<redacted>`, the numeric stats
    are intact.
  - T6: dataset-level + global denylists merge (union).
  - T7 (codex R1 MEDIUM): allowlist scope semantics —
    instance-only allowlist, dataset-only allowlist, both
    set (intersection). Three sub-cases.
  - T8: schema-change emission — column in denylist surfaces
    as `<redacted>` in `details["new_columns"]` /
    `dropped_columns` / `inconsistent_past_columns` AND in
    the validation message interpolation.
- New `tests/test_validation/test_isolation_forest_redaction.py`:
  - mirror of T1–T8.
- Extend `tests/test_storage/test_details_json_roundtrip.py`?
  No — the round-trip test pins a fixture, redaction is upstream
  of the system table.

### Documentation

- `docs/notifications.md` — add a "Column-name redaction" section
  near the alert deduplication section.
- `docs/configuration.md` — document the dataset-level
  `redacted_columns` / `allowlist_columns` fields.
- `docs/programmatic_api.md` — document the
  `Qualifire(redacted_columns=...)` instance-level option with
  example.
- `docs/CHANGELOG.md` — Enhancement entry under Unreleased.

## Acceptance Criteria

- AC1: A column listed in a dataset's `redacted_columns`
  produces `feature="<redacted>"` in
  `details["top_contributing_features"]` and a redacted-shape
  entry in `details["value_drift_explainer"]`.
- AC2: A column in the global `Qualifire(redacted_columns=...)`
  list is redacted on every dataset (no per-dataset opt-in
  required).
- AC3: When `allowlist_columns` is set, columns not in the
  allowlist are redacted; columns in the allowlist BUT also in
  the denylist are still redacted (denylist precedence).
- AC4: The numeric `current` / `past` / `delta` blocks of a
  redacted drift entry are preserved (they don't reveal column
  identity).
- AC5: System-table `details_json` write carries the same
  redaction (compute-time means no extra wiring).
- AC6: Tests T1–T8 pass for both pattern_check and
  isolation_forest (T7 covers allowlist scope semantics; T8
  covers schema-change emission redaction including the
  message-string interpolation).
- AC7: Pattern_check's message-string `top3` summary (built at
  the validator's message construction site, line 467 area)
  inherits redacted feature names — the message reads
  `<redacted>(0.42), <redacted>(0.32), ...`. This is intentional;
  the message lands in the same downstream channels as
  `details`, so consistent redaction is the right behavior.
- AC8: Schema-change `details["new_columns"]`,
  `details["dropped_columns"]`,
  `details["inconsistent_past_columns"]` lists carry
  `<redacted>` strings for redacted source columns; non-
  redacted columns pass through unchanged.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Operators set a typo'd column name; nothing gets redacted | Plan-phase: log a WARN at engine-init time when a denylist column is not present in any dataset's known schema. Stretch goal — not blocking. |
| `<redacted>` as a magic string collides with a real column named `<redacted>` | Vanishingly small; a literal `<redacted>` column would already break the dashboard's display logic. Document and move on. |
| Drift explainer's numeric blocks STILL leak through statistics that imply column identity (e.g., `min=0, max=999, p50=100` is suspicious for a known integer-bound column) | Out of scope — this feature is about column name redaction, not statistical redaction. Statistical redaction is a separate (much harder) feature. |
| `feature_names` upstream of pattern_check is the post-encoding name (`channel_mobile`, not `channel`) — denylist lookup needs to map post-encoding names back to source columns | Use the existing `encoding_map` in `_drift_explainer.py` and the `source_column` field that already disambiguates. The redaction lookup is on `source_column`, not `feature` (the post-encoding name). |

## Out-of-Band Reviews

- 2 adversarial plan reviews.
- 2 codex plan reviews → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Small. Estimated diffs: ~250 net LOC across `core/_redaction.py`
(new), `core/config.py`, `api.py`, `validation/pattern_check.py`,
`validation/isolation_forest.py`, `validation/_drift_explainer.py`,
`engine.py`, plus ~150 LOC of tests across two new test files.

## Plan Iteration Log

- v1: initial draft.
- v2: addressed adversarial round 1 — pinned encoding_map
  source-column lookup pattern; added schema-change keys
  (`new_columns`, `dropped_columns`,
  `inconsistent_past_columns`) to the redaction surface;
  dropped stale AC7 reference to `_REMOVED_FIELDS_GUIDANCE`;
  pinned message-string redaction via new AC7.
- v3: addressed adversarial round 2 — moved redaction AFTER
  `explain_value_drift(...)` returns (the explainer relies on
  real feature names to look up `encoding_map`; redacting
  upstream would feed `<redacted>` into the lookup and trip
  the mapping_errors path).
- v5: addressed codex plan-review round 2 — Locked Decision
  #1 still said "redact at SHAP-naming step ... and at the
  drift-explainer entry construction in _drift_explainer.py";
  rewrote to match the v3/v4 body text (POST-explainer
  redaction in the validators; _drift_explainer.py untouched).
  Updated AC6 to include T7 and T8.
- v4: addressed codex plan-review round 1 —
  (a) BLOCKER ×2: cleaned up the v2-leftover language
  describing pre-explainer redaction in the validator-wiring
  section (contradicted v3's post-explainer ordering);
  (b) MAJOR: added isolation_forest.py:196 to the schema-
  change redaction surface (was only pattern_check.py:208
  before);
  (c) MAJOR: added message-string redaction at
  pattern_check.py:202 and isolation_forest.py:190 (the
  `Schema change: new=...` interpolation also leaks);
  (d) MEDIUM: pinned allowlist semantics for one-sided
  configuration (None = "no policy at this scope, other scope
  governs"; both set = strictest-wins intersection); added T7
  test covering the three sub-cases.
