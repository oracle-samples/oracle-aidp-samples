---
id: dashboard-rich-detail-panel
name: Interactive Dashboard — Rich Detail Panel for Validation Rows
status: planned
created: 2026-05-10
---

# Interactive Dashboard — Rich Detail Panel — Plan

## Goal & Hard Scope

The interactive dashboard's per-validation table row currently shows
severity + message + a couple of scalars. Every validator already
populates a structured `details` dict (SHAP top features, the new
`value_drift_explainer` block from PR #15, drift's `mean_past` /
`z_score` / `rate_of_change_pct`, forecast's prediction band,
threshold's `expected_value`, SLO's age vs target). This plan
ships a **click-to-expand row detail panel** that surfaces those
fields without forcing the operator to drop into raw storage.

In scope:

- Project the persisted `details_json` into the dashboard's
  embedded `SNAPSHOT` array as a parsed object (with size budget).
- New JS: a click-to-expand detail panel under each table row that
  dispatches to a per-validator-type renderer.
- Per-validator-type renderers (`shape`, `pattern`, `drift`,
  `trend`, `threshold`, `slo`, plus generic JSON fallback for
  unknown types and `qualifire.engine` warnings).
- Special-case rendering for the `value_drift_explainer` block
  (parallel list with `top_contributing_features`).
- Per-row size budget on the projected `details` blob so the
  embedded SNAPSHOT JSON stays bounded.
- Tests: snapshot serialization tests for `details` projection
  + size budget + redaction; JS-side rendering exercised via a
  golden HTML smoke (no JS execution required, just `grep` for
  the rendered structural strings).
- Docs: `docs/notifications.md` cross-link, dashboard rendering
  notes in `docs/validators/*.md`, CHANGELOG.

Out of scope (hard-stop — defer to follow-up):

- Static `generate_html_report` — keep it script-free. The
  detail panel ships only on the interactive dashboard.
- Plotly band rendering for forecast (a static actual-vs-predicted
  table is fine for v1; a Plotly band is its own iteration).
- Server-side filtering of `details` (privacy / PII redaction
  beyond a simple known-key allowlist).
- Per-slice past breakdown for `value_drift_explainer` —
  separate backlog item `drift-explainer-per-slice-breakdown`.
- Any change to the persisted system-table schema or to
  `read_health_data` signatures (`details_json` is already
  projected by all four backends today; verified).

## Design

### Snapshot projection

`generate_interactive_html` (`qualifire/reporting/html_report.py:313+`)
already builds each snapshot row as a dict. Today the only field it
extracts from `details_json` is the boolean `qualifire_internal_failure`
marker. Today's interpolation
`f"<script>\nconst SNAPSHOT = {snapshot_json};\n"`
is **not** safe against script-breakout content
(e.g. a category value of `"</script><script>alert(1)</script>"`
in the persisted details would terminate the script tag inside
the embedded JSON literal). The serializer must escape the JSON
output before embedding (see "Embedding helper" below).

Extension:

1. Add a helper `_snapshot_details(raw_details, *, max_bytes=8192)`:
   - Accept either a `dict` (in-memory) or a JSON string
     (re-read from the persisted TEXT column) — same dual-shape
     tolerance as `_row_is_internal_failure` in
     `qualifire/reporting/health.py:11+`.
   - On parse failure → return `None` and log at DEBUG.
   - Drop the `qualifire_internal_failure` key (already surfaced
     as a top-level snapshot field; no point duplicating).
   - **Path-aware truncation**:
     - The `value_drift_explainer` key is a *known* schema with a
       parallel-length / parallel-order invariant against
       `top_contributing_features` (PR #15's contract — up to
       16 KB serialized). It is NEVER dropped wholesale. Tail
       entries are replaced with `kind="truncated"` placeholders
       (mirroring PR #15's own internal truncation contract) until
       the per-row 8 KB budget is met. The validator's own 16 KB
       cap is honored upstream; this dashboard cap is the
       client-side fallback for backends that may have stored
       larger payloads pre-cap.
     - Other dict / list values larger than **3 KB** when JSON-
       serialized are replaced with `"[truncated dict size=N bytes]"`
       (or `"[truncated list len=N]"`) — preserves the key shape.
   - Replace any string value longer than **2 KB** with
     `"[truncated]"`. Pinned: REPLACE (not drop), so renderers'
     branch logic still fires.
   - **Sensitive-key redaction** (path-aware, NOT recursive on
     `value_drift_explainer.current.top[*].value` /
     `.past.top[*].value` / `.category`, which are legitimate
     schema fields per PR #15). Redact a key iff:
     - the key matches whole-key regex
       `^(password|secret|token|api[_-]?key|credential)$`
       (case-insensitive), AND
     - the JSON path does NOT match the explainer's known
       schema fields listed above.
     Replace the value with `"[redacted]"`.
   - If the resulting dict still exceeds the per-row cap after
     all the above, return `{"_truncated": True, "_reason":
     "row payload exceeded 8 KB"}` as a last resort.
   - Return the surviving dict.

2. Add `"details": _snapshot_details(r.get("details_json"), max_bytes=8192)`
   to the snapshot row.

3. Total SNAPSHOT JSON size guardrail (multi-stage):
   - **Stage 1**: after building the full snapshot list, if
     `len(snapshot_json) > 8 MB`, mutate each row's `details` to
     `{"_truncated": True, "_reason": "snapshot payload exceeded 8 MB"}`
     for the lower-importance rows (oldest first by
     `run_timestamp`) until the total fits.
   - **Stage 2**: if Stage 1 strips ALL details and the total is
     still > 8 MB (metadata-only overflow on extreme time windows),
     drop the oldest rows entirely from the snapshot using the
     **same stable sort key as Stage 1**:
     `(run_timestamp ASC, dataset_name ASC, validation_name ASC,
     dimension_value ASC)`. A top-level marker
     `SNAPSHOT_TRUNCATED = {dropped: N, reason: "metadata overflow"}`
     is embedded so the dashboard banner can warn the operator
     ("Showing N of M rows; older rows omitted").
   - This protects long time-window dashboards (`days=365`) from
     emitting multi-GB HTML.

### Embedding helper

A tiny `_safe_json_for_html_script(obj) -> str` helper wraps
`json.dumps(obj)` and additionally escapes the four sequences that
can break out of a `<script>` block: `</`, U+2028, U+2029, plus
`<!--` (HTML comment opener):

```python
def _safe_json_for_html_script(obj):
    return (
        json.dumps(obj)
        .replace("</", "<\\/")
        .replace(" ", "\\u2028")
        .replace(" ", "\\u2029")
        .replace("<!--", "<\\u0021--")
    )
```

The interactive dashboard MUST use this helper everywhere it
embeds JSON inside a `<script>` block (today: `SNAPSHOT`,
`DEFAULT_DAYS`). A category value or message containing
`"</script>"` no longer breaks the page.

Why these caps: today's snapshot is unbounded. Adding the full
`value_drift_explainer` (16 KB cap) × `top_contributing_features`
× thousands of validation rows would balloon the embedded JSON.
The 8 KB per-row + 8 MB total ceilings keep the page renderable
in any browser while preserving useful information for the
"recent" rows operators actually click.

### Per-validator-type renderers (JS)

A registry `DETAIL_RENDERERS` keyed by `validation_type`:

```javascript
const DETAIL_RENDERERS = {
  shape:     renderShapePattern,
  pattern:   renderShapePattern,
  drift:     renderDrift,
  trend:     renderForecast,
  threshold: renderThreshold,
  slo:       renderSLO,
};

function renderDetailPanel(row) {
  const renderer = DETAIL_RENDERERS[row.validation_type] || renderGeneric;
  if (isInternalFailure(row)) return renderInternalFailure(row);
  if (!row.details) return renderGeneric(row);
  return renderer(row);
}
```

Each renderer returns an HTML string (already escaped) the click
handler injects into a `<tr class="detail-row">` slid in below
the parent row.

#### `renderShapePattern(row)`

- Mirror `format_validation_details` in
  `qualifire/notification/base.py`'s `shape | pattern` branch:
  - SHAP top contributors as a `<table>` with two columns
    (feature, importance).
  - Below each row, if `row.details.value_drift_explainer[i]`
    exists, render the per-feature shift summary as a nested
    block. Per-kind:
    - `numeric` → `current vs past` mini-table with
      `mean / p50 / p99 / null_pct` and the `delta_pct` cell
      coloured red if it crosses ±10%.
    - `boolean` → `true_rate cur / past / Δpp`.
    - `datetime` → ISO range row.
    - `onehot` → `category=… rate cur / past / Δpp` (use
      `category` if `is_null_bin=false`, else "&lt;null&gt;").
    - `label_encoded` → aligned top-N category mix (zip the
      two parallel lists).
    - `unknown` / `truncated` → flat summary line.
- AUC/anomaly_ratio + sample sizes + `auc_std` shown above the
  feature table.
- Fall back to a "value drift unavailable: {error}" line if the
  explainer key is absent / errored.

#### `renderDrift(row)`

- `current` vs `past mean` + the per-key signed details
  (`deviation_pct`, `deviation_abs`, `z_score`,
  `rate_of_change_pct`, `rate_of_change_abs`).
- Per-cell red border when the value violates the
  `expected_value` block (already a JSON string on the
  snapshot row).

#### `renderForecast(row)`

- `observed` vs `predicted (yhat)` + `[yhat_lower, yhat_upper]`
  range. Static text — no Plotly band in v1 (deferred).

#### `renderThreshold(row)`

- `actual: …` + threshold breakdown (warning / error min/max),
  highlighting the bound that tripped.

#### `renderSLO(row)`

- `age` vs `threshold` (already a duration). Highlight when
  `age > threshold`.

#### `renderGeneric(row)`

- Pretty-printed JSON of `row.details` inside `<pre>` tags.
- HTML-escaped to avoid script injection from a hostile
  validator's `details` value (defense-in-depth — the
  validators in-tree never put HTML in their details, but
  the renderer must escape unconditionally).

#### `renderInternalFailure(row)`

- Distinct styling (`qf-internal-failure` class — already
  defined). Show `error_class`, truncated stack-trace
  excerpt, and the originating component (engine / dispatcher
  / persistence). Operators reading the panel know this row
  is a qualifire bug, not a data finding.

### Click-to-expand UX

- Click on a non-pager table row → toggle a `<tr class="detail-row"
  data-row-id="…">` directly below it. Re-clicking collapses.
- Esc key → close all open detail rows.
- Detail row spans all parent columns (`<td colspan="N">`).
- CSS animation: fade-in over 100 ms; no layout-jank.
- `details-row` is excluded from sort / pagination logic —
  ordering is already locked by the parent's row-level sort.

### Privacy / PII

The dashboard renders persisted validator details. Two layers of
defense:

1. **HTML escape at render time.** Every leaf value passed into
   the rendered DOM is HTML-escaped via the JS
   `escapeHtml(...)` helper (already present in the dashboard
   JS — used by today's `renderTable`). A details value of
   `"<script>alert(1)</script>"` is rendered as
   `&lt;script&gt;alert(1)&lt;/script&gt;` text, never executed.
2. **Script-breakout protection at embed time.** SNAPSHOT JSON
   is escaped via `_safe_json_for_html_script` before
   interpolation into the `<script>` block. A details string of
   `"</script><script>alert(1)</script>"` cannot terminate the
   surrounding script tag.

**Sensitive-key redaction.** `_snapshot_details` replaces values
with `"[redacted]"` when:
- the key is a whole-key match of
  `^(password|secret|token|api[_-]?key|credential)$` (case-
  insensitive), AND
- the JSON path is NOT one of the known-legitimate explainer
  schema paths (`value_drift_explainer[*].current.top[*].value`,
  `.past.top[*].value`, `.category`).

**Acknowledged disclosure path.** The exemption is load-bearing:
`value_drift_explainer`'s top-N category mix IS the point of
the explainer, including when category values are user-
meaningful. Redacting categories silently would mask the
drift the explainer is meant to surface. **However**, this
also means a shared dashboard rendering an explainer for a
PII-bearing column (email, phone, payment-card-like values)
would expose those raw values verbatim to anyone with
dashboard access. The plan accepts this trade-off and
**requires operator-side controls**, documented per AC#26:

- `model.explain_value_drift: false` — disable the block
  entirely for the affected validation.
- `model.exclude_columns: [<col>]` — keep the explainer on,
  but bar SHAP from picking that column as a top contributor.
- Upstream filtering / hashing — strip / mask the column
  before it reaches qualifire.

The shape and pattern validator pages MUST carry a "Shared
dashboards / PII" subsection naming this path. The keyword
denylist is *secondary* defense for unintended
`password`/`token`/etc. keys in non-explainer paths, NOT
the primary privacy guard for the explainer itself.

**Validator-side audit.** A guard test walks every in-tree
validator's `details` payload (one fixture per validator type)
and asserts no top-level dict key matches the sensitive-key
regex. This catches future validators that accidentally
introduce a `details["password"]` or similar.

## Files Touched

- `qualifire/reporting/html_report.py` — extend `generate_interactive_html`'s
  snapshot serializer + add the JS renderer registry + click
  handler + CSS for the detail row.
- `qualifire/reporting/_snapshot_details.py` (NEW) — `_snapshot_details`
  helper + `_redact_sensitive_keys` + size budget enforcement.
  Kept separate so the helper is unit-testable without the
  ~1300-line html_report.
- `tests/test_reporting/test_snapshot_details.py` (NEW) — per-key
  redaction, per-row cap, total-snapshot cap, dual-shape
  (dict / JSON string) tolerance, parse-failure path.
- `tests/test_reporting/test_html.py` — extend the existing
  smoke to assert the rendered HTML contains the structural
  markers (`detail-row`, `DETAIL_RENDERERS`, the per-validator
  `dataset` attribute, etc.).
- `tests/test_reporting/test_interactive_dashboard.py` (or
  `tests/test_interactive_dashboard.py` — wherever the existing
  interactive tests live) — assert `details` shows up in
  `SNAPSHOT[i]` for at least one validator-type fixture row.
- `tests/test_reporting/test_dashboard_renderers_node.py` (NEW)
  — executable JS test of the renderer registry via Node's
  built-in `node:test` runner. Skips if `node` is not on PATH.
  Exercises: click toggle, click-again-to-collapse,
  `renderGeneric` HTML escape, `renderShapePattern` zip with
  `value_drift_explainer`, parallel-list mismatch fallback,
  `renderInternalFailure` `error` field surfacing, and the
  script-breakout fixture (`"</script>"` in a category value).
  See "Executable JS test" below for the harness shape.
- `qualifire/storage/base.py` — update the `read_health_data`
  protocol docstring to list `details_json` as an optional
  projected column (every concrete backend already projects
  it — codex round 1 LOW).
- `docs/validators/{shape,pattern,drift,forecast,threshold,slo}.md`
  — short note in each validator page that the new dashboard
  detail panel renders the validator's `details` block.
  Additionally, `shape.md` and `pattern.md` get a "Shared
  dashboards / PII" subsection naming the explainer category-
  disclosure path and the three operator-side controls
  (`explain_value_drift=False`, `exclude_columns`, upstream
  filtering / hashing) — codex R2 HIGH-2.
- `docs/CHANGELOG.md` — Enhancement entry.

## Acceptance Criteria (verifiable from a diff)

1. Every snapshot row carries a `"details"` key — either a
   dict (parsed from persisted JSON) or `null` (parse failed
   or no details).
2. `_snapshot_details(...)` redacts any key matching the
   sensitive-key regex with `"[redacted]"` (test injects a
   `details["api_key"]` and asserts redaction).
3. `_snapshot_details(...)` returns
   `{"_truncated": True, "_reason": "row payload exceeded 8 KB"}`
   when the input dict serializes above the 8 KB cap.
4. Per-row cap: a single dict whose value is a string >2 KB
   has that string dropped (replaced by `"[truncated]"`).
   Test asserts the surviving dict.
5. Total snapshot cap: a fixture with 5000 rows × ~6 KB
   `details` each (~30 MB raw) compresses to under 8 MB
   serialized JSON, with the oldest rows' `details` replaced
   by the truncation marker.
6. Snapshot accepts both dict and JSON-string forms of
   `details_json` (mirrors `_row_is_internal_failure`).
7. Generated HTML contains the literal strings:
   - `DETAIL_RENDERERS = {`
   - `class="detail-row"`
   - per-renderer function names (`renderShapePattern`,
     `renderDrift`, `renderForecast`, `renderThreshold`,
     `renderSLO`, `renderGeneric`, `renderInternalFailure`).
8. JS click handler is wired on the **toggle button** (first
   column of the validation-selected view's per-partition
   table — NOT on `tr.row-clickable`). Pressing the toggle
   creates exactly one `<tr class="detail-row">` directly
   below the parent row; pressing it again removes the
   detail row. The grouped / dataset-selected views' existing
   `tr.row-clickable` drill-down handlers stay unchanged
   (asserted by grepping the rendered HTML for the literal
   button class `qf-detail-toggle` AND for the unchanged
   row-clickable navigation handlers).
9. The shape/pattern renderer branches on
   `row.details.value_drift_explainer` and renders the
   parallel `top_contributing_features` × explainer table.
   Asserted by including the literal substring
   `value_drift_explainer` and `top_contributing_features` in
   the embedded JS source (since the renderer reads them).
10. The generic renderer HTML-escapes its input — a fixture
    `details["msg"] = "<script>alert(1)</script>"` produces
    rendered DOM containing `&lt;script&gt;…` (not raw
    `<script>`) when the renderer is invoked via the Node
    DOM harness. SNAPSHOT-side breakout safety is the separate
    AC#14 (`</script>` round-trip).
11. PII denylist test: walk every in-tree validator's
    `details` payload (one fixture per validator type) and
    assert no key is in the denylist set. This is a guard
    against future validators leaking PII through the
    dashboard.
12. Static `generate_html_report` does not gain the detail-panel
    surface. The static report has historically carried a tiny
    inline `filterTable()` script (6 lines for the severity
    dropdown); that one stays. The assertion is **markers-
    absent**: the static HTML for a fixed fixture contains none
    of the new feature's markers (`DETAIL_RENDERERS`,
    `qf-detail-toggle`, `wireDetailToggles`,
    `renderShapePattern`, `value_drift_explainer`,
    `snapshot-truncation-banner`, `SNAPSHOT_TRUNCATED`). This
    survives CSS/formatting noise and is unambiguous about
    what's out of scope.
13. CHANGELOG Enhancement entry mentions the detail panel,
    the per-validator-type renderers, and the per-row +
    total-snapshot size budgets.

14. **Script-breakout safety**: a fixture detail value
    `"</script><script>alert(1)</script>"` round-trips into
    the embedded SNAPSHOT JSON without producing a raw
    `</script>` substring inside the JSON literal. Test
    asserts via raw-string match against the generated HTML.
    The rendered DOM (when JS is executed) emits the value as
    escaped text via `escapeHtml`.

15. **Path-aware `value_drift_explainer` preservation**: a
    fixture explainer payload exceeding 8 KB serialized
    survives `_snapshot_details` with parallel length / order
    against `top_contributing_features` (tail entries become
    `kind="truncated"` placeholders), NOT dropped wholesale.

16. **Path-aware PII exemption**: a fixture explainer with
    category value `"alice@example.com"` and another with
    `"token-like-12345"` round-trip into the embedded
    SNAPSHOT under `value_drift_explainer[*].current.top[*].value`
    without redaction. A non-explainer `details["api_key"]`
    field is replaced with `"[redacted]"`. Both assertions in
    the same test for clarity.

17. **Multi-stage 8 MB cap**: a fixture with 50,000 metadata-
    only validation rows (no details) generates HTML where
    the embedded SNAPSHOT either fits under 8 MB OR carries a
    `SNAPSHOT_TRUNCATED = {dropped: N, ...}` marker and the
    surviving row count keeps the page under 8 MB.

18. **Executable JS test (HIGH from codex)**: when `node` is
    on PATH, the test asserts via Node:
    - click toggle adds + removes the detail row,
    - `renderGeneric({...,"msg":"<script>x</script>"})`
      emits HTML-escaped output,
    - `renderShapePattern` zips parallel lists correctly,
    - parallel-list length mismatch emits a warning row,
    - `renderInternalFailure(row)` surfaces `details.error`.
    Skips with a clear message when `node` is unavailable so
    the suite still runs in minimal CI environments.

19. **Storage protocol docs (LOW from codex)**:
    `qualifire/storage/base.py:read_health_data` docstring
    lists `details_json` among the optional projected
    columns. Verified by grep.

20. **`expected_value` parsing contract (MEDIUM from codex)**:
    a fixture row with
    `expected_value = "{'warning': {'min': 100}}"`
    (Python-style stringified dict) renders the threshold
    breakdown correctly. The renderer's permissive parser
    (JSON first, then single-to-double-quote rewrite, then
    raw fallback) is exercised end-to-end via the Node
    test.

21. **PASS pattern/shape row clean render** (R2.4): a fixture
    PASS-severity pattern row with no
    `top_contributing_features` and no `value_drift_explainer`
    renders the AUC + sample-size lines, with NO empty "top
    features" table and NO "value drift unavailable" message.

22. **Deterministic Stage 1 stripping** (R2.2): a fixture with
    two rows at the same `run_timestamp` and different
    `(dataset_name, validation_name, dimension_value)` triggers
    Stage 1; the surviving rows are deterministic
    (test asserts ordering twice across re-runs).

23. **Unified truncation banner** (R2.7): when any of
    `_truncated`, `kind="truncated"`, or `SNAPSHOT_TRUNCATED`
    is embedded, the rendered HTML contains the literal banner
    string `"Some details were truncated"`. When all three are
    absent, the banner is absent.

24. **Detail-row preservation key includes `dimension_value`**
    (R2.1): a fixture with two dimension rows
    (validation_name=v, partition_ts=p, run_timestamp=t,
    dimension_value ∈ {"a","b"}) opened simultaneously stays
    open after a re-render. (Asserted via Node DOM test.)

25. **Stage 2 deterministic ordering** (codex R2 MEDIUM-1): a
    fixture with two metadata-only rows sharing
    `run_timestamp` triggers Stage 2; the surviving rows are
    deterministic across two regenerations of the same
    snapshot. Asserts the same `(run_timestamp, dataset_name,
    validation_name, dimension_value)` stable sort applies to
    Stage 2 as to Stage 1.

26. **Operator-facing PII guidance** (codex R2 HIGH-2): both
    `docs/validators/shape.md` and `docs/validators/pattern.md`
    contain a "Shared dashboards / PII" subsection that
    states explicitly:
    - The new dashboard detail panel surfaces
      `value_drift_explainer` category values verbatim from
      the source column (no redaction at the rendering
      layer).
    - For shared dashboards where the source column may
      contain PII (e.g. email, phone, payment-card values),
      operators MUST control disclosure at the source: set
      `model.explain_value_drift: false` to disable the
      block, OR add the column to `model.exclude_columns`
      so SHAP doesn't pick it as a top contributor, OR
      filter / hash the column upstream of qualifire.
    - The `[redacted]` keyword redaction is intentionally
      NOT applied to explainer category fields because doing
      so would mask the very drift the explainer is meant
      to surface.

    AC verified by grep — both docs contain the literal
    strings `"Shared dashboards / PII"` and
    `"explain_value_drift"` and `"exclude_columns"`.

## Hard Stops

- No new Python dependencies. `json` (stdlib) and `html`
  (stdlib for escape) only.
- No change to `read_health_data` signatures or to any storage
  backend write path. `details_json` projection is already in
  place across all four backends.
- No JS frameworks (React / Vue / etc). Plain DOM + Plotly
  (already loaded). The detail row is built as an HTML string
  and injected via `innerHTML` after server-side escaping.
- The total snapshot ceiling MUST be enforced — long
  time-window dashboards (`days=365`) without it would embed
  multi-GB JSON.
- Static health report (`generate_html_report`) is OUT OF
  SCOPE. Adding JS to the script-free static report is its own
  iteration.
- The renderer MUST handle `row.details === null` (parse
  failure / absent column) without throwing. Detail panel
  shows `"No details available."`

## Open Questions Resolved

| Question (from idea.md) | Pin |
|-------------------------|-----|
| Snapshot size cap | 8 KB per row, 8 MB total. Truncation markers preserve the schema. |
| Renderer registry vs generic | Registry by `validation_type` with a generic JSON fallback. The fallback covers unknown future types without dashboard-side changes. |
| Privacy / PII | Sensitive-key redaction (regex denylist) + a test that asserts no in-tree validator emits keys in the denylist. Dashboard rendering escapes HTML. |

## Why this is shippable in one PR

- Touches one Python file (html_report.py) + one new helper
  module + tests/docs. No storage changes, no validator
  changes, no API changes.
- Per-validator-type rendering is dispatched in JS by
  `validation_type` — already a snapshot column; no new
  serialization plumbing.
- The 8 KB / 8 MB caps are conservative enough to land
  without operator surprises; can be relaxed in a follow-up
  if needed.

## Iteration Notes

### Adversarial round 1 (applied below)

R1.1 **Detail-row column count.** Detail row injects
`<td colspan="N">`; N is read from `thead.querySelectorAll('th').length`
at render time so the three table layouts (grouped / dataset-selected /
validation-selected) all stay aligned without hardcoding column counts.

R1.2 **Click handler conflict with `tr.row-clickable`.** Today the
all-datasets view treats a row click as drill-down; the
dataset-selected / validation-selected views also navigate. The
detail panel must NOT hijack those. Resolution:

- The detail panel ships only on the **validation-selected** view
  (the per-partition history table at the bottom). Drill-down rows
  in the grouped / dataset-selected views remain navigation-only.
- A small `▸` toggle button is added as the first column of that
  table; clicking the toggle opens / closes the detail row.
  Clicking the rest of the row is a no-op (it's already the
  innermost view).

This keeps the new UX additive and avoids ambiguity with existing
drill-down behavior.

R1.3 **Sensitive-key regex scope.** Match the **whole** key, not
substring — `^.*(password|secret|token|api[_-]?key|credential).*$`
becomes `^(password|secret|token|api[_-]?key|credential)$` (case-
insensitive). Avoids redacting innocent keys like
`password_check_threshold`. The regex applies only to dict keys
under `details`, never to leaf string values.

R1.4 **Mismatched parallel-list defensive render.** If
`top_contributing_features.length !== value_drift_explainer.length`
(should never happen given PR #15's invariants, but defensive),
render the SHAP table in full and append a single
`"value drift list length mismatch"` row instead of zip-rendering.

R1.5 **Internal-failure renderer payload.** Today the engine
populates internal-failure rows with `details = {"qualifire_internal_failure": True, "error": "<exception str>"}`
(see `qualifire/core/engine.py:243+`). No `error_class` /
stack-trace keys exist. The renderer surfaces just `error` as an
escaped string — no fabricated stack-trace UI.

R1.6 **Detail-row preservation across re-render.** The dashboard's
table rebuilds `tbody.innerHTML` on filter / pager change,
which would clobber any open detail row. State is preserved in
a module-level `Set<rowKey>` keyed by `(validation_name, partition_ts, run_timestamp)`;
after each `body.innerHTML = …` rebuild, the click handler
re-opens any row whose key is still in the set.

R1.7 **Static-report regression check via "no `<script>` tag added"
binary assertion** rather than full snapshot comparison —
brittle to CSS / formatting noise.

R1.8 **Per-row 8 KB cap behavior under nested oversize.** When a
nested dict / list value is > 3 KB, it's replaced with the
literal string `"[truncated dict size=N bytes]"` (or
`"[truncated list len=N]"`) — preserves the key shape so the
renderer's branch logic still fires; just the rich content is
gone. Avoids the renderer crashing on missing keys.

R1.9 **`details === null` UX.** Detail panel for `null` details
shows `"No details persisted for this row."` with no further
content. Distinguishes "validator didn't emit details" from
"details parse failed" — the former is normal (e.g. SLO with
no extras), the latter would log at DEBUG server-side and the
panel says `"Details unavailable: parse error."`.

### Adversarial round 2 (applied above)

R2.1 **Detail-row preservation key includes `dimension_value`.**
The (validation_name, partition_ts, run_timestamp) triple is
NOT unique for dimensional validators. Key extends to
(validation_name, partition_ts, run_timestamp, dimension_value).

R2.2 **Deterministic Stage 1 ordering.** Stripping rows by
"oldest first" needs a tie-breaker for rows with the same
`run_timestamp`. Pinned: sort by `(run_timestamp ASC,
dataset_name ASC, validation_name ASC, dimension_value ASC)`.
Test exercises a fixture with two rows at the same
run_timestamp and asserts deterministic Stage 1 stripping.

R2.3 **`_safe_json_for_html_script` source-of-truth chars.** The
plan's snippet uses literal U+2028/U+2029 chars (invisible
in source). Implementation MUST use the explicit Python
escape `" "` / `" "` for grep-friendliness:

```python
.replace(" ", "\\u2028")
.replace(" ", "\\u2029")
```

R2.4 **Renderer dispatch for PASS rows.** A PASS pattern/shape
row has neither `top_contributing_features` nor
`value_drift_explainer` (the validator's `if severity != PASS`
gate skips both). The renderer must show just AUC /
anomaly_ratio + sample sizes, NOT an empty "top features"
table or a "value drift unavailable" line. New AC.

R2.5 **AC#12 reformulation.** Replaced "snapshot-compare static
report" with "static report contains no `<script>` tag" — a
binary check that survives CSS / formatting noise.

R2.6 **Uniform script embedding.** Every JSON literal embedded
in a `<script>` block (`SNAPSHOT`, `DEFAULT_DAYS`,
`SNAPSHOT_TRUNCATED` if present) goes through
`_safe_json_for_html_script` — even simple int / boolean
embeds, for grep-uniformity. Pinned in implementation.

R2.7 **Truncation marker reconciliation.** Three internal
markers (`_truncated`, `kind="truncated"`,
`SNAPSHOT_TRUNCATED`) translate to ONE operator-facing
banner: `"Some details were truncated to keep the dashboard
fast — see the system table for full payloads."` The
banner's render condition is "any of the three markers is
present in the embedded data". Test asserts the banner
appears when any marker is set.

R2.8 **Test files pinned to existing paths.**
`tests/test_reporting/test_html.py` (existing) and
`tests/test_interactive_dashboard.py` (existing) are the
extension points. The new `test_dashboard_renderers_node.py`
lands under `tests/test_reporting/`. The new
`test_snapshot_details.py` lands under `tests/test_reporting/`.

### Codex round 2 fixes (applied above)

C2.1 [HIGH] **Click contract pinned to toggle button.** AC#8
no longer says `tr.row-clickable` toggles the detail row;
instead, a dedicated `qf-detail-toggle` button in the first
column of the validation-selected view is the sole entry
point. Grouped / dataset-selected views' navigation behavior
is preserved. AC#8 / AC#10 / AC#18 / AC#24 / executable JS
test all updated.

C2.2 [HIGH] **PII docs guard added.** AC#26 requires both
`shape.md` and `pattern.md` to carry a "Shared dashboards /
PII" subsection that names the disclosure path and the three
operator-side controls (`explain_value_drift=False`,
`exclude_columns`, upstream filter / hash). The "Privacy /
PII" section of this plan also restated to call the
exemption out as a known disclosure path with operator-side
controls — not "resolved by denylist + escaping alone."

C2.3 [MEDIUM] **Stage 2 ordering** uses the same stable sort
key as Stage 1: `(run_timestamp ASC, dataset_name ASC,
validation_name ASC, dimension_value ASC)`. AC#25 asserts.

C2.4 [MEDIUM] **AC#10 reformulated.** The DOM-side escape
property is verified via the Node renderer test (no raw
`<script>` in the rendered DOM HTML). The embedded-SNAPSHOT
breakout safety remains AC#14's separate `</script>`
round-trip assertion. The two are now non-overlapping.

### Codex round 1 fixes (applied above)

C1.1 [BLOCKER] **Script-breakout-safe SNAPSHOT embedding.** A new
`_safe_json_for_html_script(obj)` helper escapes `</`, U+2028,
U+2029, and `<!--` before interpolation into the `<script>`
block. New AC asserts a fixture detail value of
`"</script><script>alert(1)</script>"` round-trips into the
embedded SNAPSHOT without closing the surrounding `<script>` tag
(checked via raw string match: no `</script>` substring inside
the embedded JSON literal except the parser-state-final one).

C1.2 [BLOCKER] **`value_drift_explainer` is path-aware-preserved.**
The 3 KB per-key cap NEVER drops the explainer wholesale.
Instead, tail entries are replaced with `kind="truncated"`
placeholders (mirroring PR #15's own internal contract) until
the per-row 8 KB budget is met. Parallel-length / parallel-
order with `top_contributing_features` is preserved.

C1.3 [BLOCKER] **PII denylist is path-aware.** The
`value_drift_explainer.current.top[*].value` /
`.past.top[*].value` / `.category` paths are explicitly
exempted from sensitive-key redaction. The denylist applies
**only** to dict keys at non-explainer paths. New AC asserts
a fixture explainer with category value `"alice@example.com"`
round-trips intact (categories are user-data semantically
already, and operator awareness is the whole point of the
explainer — redacting them silently would hide the drift the
explainer is meant to surface). Operators concerned about
PII in the dashboard control PII at the source-column /
sample level, not at the rendering layer. The plan's
"Privacy / PII" section is updated accordingly: the regex
denylist guards against unintended `password`/`token`/etc.
keys in non-schema paths, NOT against legitimate data
emitted by the explainer.

C1.4 [HIGH] **Executable JS test added.** `tests/test_reporting/test_dashboard_renderers_node.py`
runs the embedded JS via Node and asserts behavior end-to-end
(click toggle, escape, value_drift_explainer rendering,
parallel-list mismatch, internal-failure render, script-
breakout fixture). The test skips with a clear message if
`node` is not on PATH. See "Executable JS test" below.

C1.5 [HIGH] **8 MB total cap is multi-stage.** Stage 1 strips
details only; Stage 2 drops oldest rows entirely if metadata-
only overflow remains. A top-level
`SNAPSHOT_TRUNCATED = {dropped, reason}` marker tells the
dashboard banner to warn the operator. New AC fixture
exercises a synthetic 50 K validation rows with no details
to trigger Stage 2.

C1.6 [MEDIUM] **`expected_value` parsing contract.** The
existing `expected_value` snapshot field is a stringified
`str(...)` of the original dict. The dashboard renderer
parses it into a JSON-compatible dict via a permissive
parser: try `JSON.parse(s)` first; if it fails (Python
`str(dict)` uses single quotes which JSON rejects), parse
with a tiny single-to-double-quote rewrite. If both fail,
render the raw string. Forecast / threshold / SLO renderers
all follow this contract. Test asserts a Python-side
`expected_value = {"warning": {"min": 100}}` round-trips
into a parseable dict the renderer can branch on.

C1.7 [MEDIUM] **String truncation contract** — pinned: REPLACE
with `"[truncated]"`, never drop. Plan text updated.

C1.8 [LOW] **Storage protocol docs** — `qualifire/storage/base.py`'s
`read_health_data` docstring updated to list `details_json`
as an optional projected column.

### Executable JS test

The renderer JS is factored as a self-contained string constant
(`_DASHBOARD_RENDERERS_JS` in `html_report.py`) so the same
constant gets embedded into the HTML AND fed to Node from a
Python test:

```python
# tests/test_reporting/test_dashboard_renderers_node.py
import shutil, subprocess, json, pytest

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node not on PATH")

def _run_in_node(harness_js: str) -> dict:
    proc = subprocess.run(
        [NODE, "--input-type=module", "-e", harness_js],
        capture_output=True, text=True, check=True, timeout=30,
    )
    return json.loads(proc.stdout)
```

Each test composes a tiny harness (jsdom-free; the renderers
return HTML strings, not DOM nodes) that imports the
`_DASHBOARD_RENDERERS_JS` constant via a string template,
calls one renderer with a fixture row, and `console.log`s a
JSON envelope the Python side parses. No npm install required.

Test cases (per AC #14a–f):
- Click toggle adds + removes a `<tr class="detail-row">` once.
- `renderGeneric({...,"msg":"<script>x</script>"})` emits
  `&lt;script&gt;x&lt;/script&gt;` (escaped).
- `renderShapePattern(row)` zips `top_contributing_features`
  and `value_drift_explainer` and emits both as a table.
- Parallel-list mismatch (`length` differs) emits the SHAP
  table and a `"length mismatch"` warning row.
- `renderInternalFailure(row)` shows `details.error` only.
- Script-breakout: a row with category value
  `"</script><script>alert(1)</script>"` round-trips through
  `_safe_json_for_html_script` → embedded literal does NOT
  contain a raw `</script>` substring. (Python-side test
  using `_safe_json_for_html_script` directly; Node not
  needed for this one.)
