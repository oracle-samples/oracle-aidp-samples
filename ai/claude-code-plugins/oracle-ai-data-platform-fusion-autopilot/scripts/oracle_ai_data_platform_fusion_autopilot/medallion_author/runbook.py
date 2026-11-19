"""Remediation runbook drafter.

* **Option A — No action**: emit a `remediation.md` explaining the
  rename-only rationale.
* **Option B — Surgical backfill MERGE**: emit `remediation.md` +
  `remediation.sql` (operator-reviewed surgical SQL).
* **Option C — Watermark rewind**: **DEFERRED to v0.4**. Drafter
  raises :class:`OptionDeferredError` with an operator-facing message
  redirecting to Option D / Option B.
* **Option D — Targeted re-seed (v0.3 default)**:
  ``aidp-fusion-autopilot run --mode seed --datasets <silver/gold-node-ids>``.
* **Option E — Full re-seed**: bare
  ``aidp-fusion-autopilot run --mode seed``.

The drafter NEVER executes the remediation — emits files only.

Only one execution path ships; the drafter emits the unflagged CLI
invocation.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..schema.incremental_impact import RemediationOption


class OptionDeferredError(Exception):
    """Raised when the operator requests an option that's deferred to
    a future release. Carries an operator-facing redirect message.

    Currently fires for Option C (watermark rewind), which requires
    an ``aidp-fusion-autopilot rewind`` verb that doesn't exist yet.
    Use Option D (targeted re-seed) or Option B (surgical MERGE)
    instead.
    """


@dataclass(frozen=True)
class RemediationArtifacts:
    """The skill's emitted runbook for one variation-point change."""

    option: RemediationOption
    runbook_markdown: str
    """Always populated — operator-facing runbook for the chosen option."""

    sql: str | None = None
    """Populated ONLY for Option B (surgical backfill MERGE).
    Options A / D / E have no SQL — their runbook is markdown-only."""


# ---------------------------------------------------------------------------
# Drafter entry point
# ---------------------------------------------------------------------------


def draft_remediation(
    *,
    option: RemediationOption,
    vp_name: str,
    prior_pinned: str | None,
    new_candidate: str,
    affected_silver_ids: set[str],
    affected_gold_ids: set[str],
    risk_label: str,
    rationale: str,
) -> RemediationArtifacts:
    """Draft the runbook + (Option B only) SQL for the chosen option.

    Args:
        option: A | B | C | D | E. C raises :class:`OptionDeferredError`.
        vp_name: variation-point name (e.g. ``invoice_currency_code``).
        prior_pinned: previously-pinned candidate (``None`` for
            initial onboarding).
        new_candidate: the operator-approved candidate.
        affected_silver_ids: bare silver node ids consuming the VP.
        affected_gold_ids: bare gold node ids consuming the VP.
        risk_label: ``likely-rename`` / ``likely-different-semantics``
            / ``unknown`` (drives the runbook narrative).
        rationale: operator-facing one-paragraph explanation.

    Returns:
        :class:`RemediationArtifacts`. ``sql`` is ``None`` for every
        option except B.
    """
    if option == "C":
        raise OptionDeferredError(
            "Option C (watermark rewind) is deferred to v0.4 — requires "
            "the `aidp-fusion-autopilot rewind` verb that knows the "
            "content-pack state contract. Use Option D (targeted "
            "re-seed) or Option B (advanced surgical MERGE) instead."
        )

    if option == "A":
        return _draft_option_a(
            vp_name=vp_name,
            prior_pinned=prior_pinned,
            new_candidate=new_candidate,
            risk_label=risk_label,
            rationale=rationale,
        )
    if option == "B":
        return _draft_option_b(
            vp_name=vp_name,
            prior_pinned=prior_pinned,
            new_candidate=new_candidate,
            affected_silver_ids=affected_silver_ids,
            affected_gold_ids=affected_gold_ids,
            rationale=rationale,
        )
    if option == "D":
        return _draft_option_d(
            vp_name=vp_name,
            new_candidate=new_candidate,
            affected_silver_ids=affected_silver_ids,
            affected_gold_ids=affected_gold_ids,
            rationale=rationale,
        )
    # Option E
    return _draft_option_e(rationale=rationale, vp_name=vp_name, new_candidate=new_candidate)


# ---------------------------------------------------------------------------
# Per-option drafters
# ---------------------------------------------------------------------------


def _draft_option_a(
    *, vp_name, prior_pinned, new_candidate, risk_label, rationale
) -> RemediationArtifacts:
    md = f"""# Remediation — Option A (no action)

**Variation point**: `{vp_name}`
**Risk label**: `{risk_label}`
**Change**: `{prior_pinned}` → `{new_candidate}`

## Decision

No silver/gold re-seed needed. The operator confirmed (via the skill's
review prompt) that the prior and new candidate hold semantically
identical data — typically a Fusion-release rename. Future incremental
runs will MERGE updated rows using the new column; historical rows
remain accurate because the old + new columns held the same values.

## Rationale

{rationale}

## Verification (optional)

Spot-check on a handful of rows that the silver values look correct
after the next incremental cycle:

```sql
SELECT * FROM <silver_schema>.<affected_node> WHERE <natural_key> = '<sample>'
ORDER BY silver_built_at DESC LIMIT 5;
```
"""
    return RemediationArtifacts(option="A", runbook_markdown=md)


def _draft_option_b(
    *,
    vp_name,
    prior_pinned,
    new_candidate,
    affected_silver_ids,
    affected_gold_ids,
    rationale,
) -> RemediationArtifacts:
    affected = sorted(affected_silver_ids) + sorted(affected_gold_ids)
    affected_list = "\n".join(f"- `{name}`" for name in affected)
    sql_blocks: list[str] = []
    for node_id in sorted(affected_silver_ids):
        sql_blocks.append(
            f"""-- Backfill {vp_name} on silver.{node_id} from the new candidate column.
-- REVIEW: confirm the column expression matches the silver template's semantics.
MERGE INTO {{catalog}}.{{silver_schema}}.{node_id} AS target
USING (
    SELECT
        <natural_key> AS nk,
        {new_candidate} AS {vp_name}
    FROM {{catalog}}.{{bronze_schema}}.<affected_bronze_table>
) AS src
ON target.<natural_key> = src.nk
WHEN MATCHED THEN UPDATE SET target.{vp_name} = src.{vp_name};
"""
        )
    for node_id in sorted(affected_gold_ids):
        sql_blocks.append(
            f"""-- Backfill {vp_name} on gold.{node_id} from the new candidate column.
-- REVIEW: confirm the column expression matches the gold template's semantics.
MERGE INTO {{catalog}}.{{gold_schema}}.{node_id} AS target
USING (
    SELECT
        <natural_key> AS nk,
        {new_candidate} AS {vp_name}
    FROM {{catalog}}.{{bronze_schema}}.<affected_bronze_table>
) AS src
ON target.<natural_key> = src.nk
WHEN MATCHED THEN UPDATE SET target.{vp_name} = src.{vp_name};
"""
        )
    sql = "\n".join(sql_blocks)

    md = f"""# Remediation — Option B (surgical backfill MERGE)

**Variation point**: `{vp_name}`
**Change**: `{prior_pinned}` → `{new_candidate}`

## When to use

Choose Option B over Option D when the affected silver/gold tables are
too large to re-seed cheaply AND the column substitution is
genuinely surgical (no derived columns / joins / filters reference the
VP). Option D (the v0.3 default) is safer because it reuses the
engine's tested seed path.

## Affected nodes

{affected_list}

## 5-point operator review checklist

**Before running the SQL in `remediation.sql`, verify:**

1. **Column dependency review**: confirm `{vp_name}` is the ONLY
   column the affected silver SQL template derives from the
   variation-point token. If the template uses the column in a
   CASE expression, JOIN clause, or filter, Option B is unsafe —
   switch to Option D.
2. **Derived columns**: check for downstream columns whose values
   depend on `{vp_name}`. If any exist, Option D is safer.
3. **Join keys**: confirm `{vp_name}` is not used in any JOIN
   condition. Joins with the new column may produce different
   matches than the historical column.
4. **Type coercion**: confirm the prior and new columns have
   identical types. A `string` → `decimal(18,2)` change would
   require explicit casting.
5. **Audit-trail attestation**: record the operator's name +
   timestamp + this commit's SHA in the run-log alongside the SQL
   execution.

## Rationale

{rationale}

## SQL

See `remediation.sql`. Review the placeholders (`<natural_key>`,
`<affected_bronze_table>`) and substitute the actual values from the
affected node's silver template BEFORE executing.
"""
    return RemediationArtifacts(option="B", runbook_markdown=md, sql=sql)


def _draft_option_d(
    *,
    vp_name,
    new_candidate,
    affected_silver_ids,
    affected_gold_ids,
    rationale,
) -> RemediationArtifacts:
    all_bare_ids = affected_silver_ids | affected_gold_ids
    if not all_bare_ids:
        raise ValueError(
            f"Option D requested for {vp_name!r} but no affected silver/gold "
            f"nodes were identified. Skill should fall back to Option B or E."
        )

    # Content-pack ``--datasets`` validates against pack node IDs — pack
    # IDs ARE the filter contract.
    sorted_ids = ",".join(sorted(all_bare_ids))
    command = (
        "aidp-fusion-autopilot run --mode seed \\\n"
        f"  --datasets {sorted_ids}"
    )

    affected = sorted(affected_silver_ids) + sorted(affected_gold_ids)
    affected_list = "\n".join(f"- `{name}`" for name in affected)
    md = f"""# Remediation — Option D (targeted re-seed of affected nodes)

**Variation point**: `{vp_name}` → `{new_candidate}`

## Affected nodes

{affected_list}

## Decision

Re-seed just the silver/gold nodes that consume the affected bronze
column. The engine's existing tested seed path rebuilds the rows from
the new column; unaffected dimensions and marts are not touched.

## Command

```
{command}
```

## Rationale

{rationale}

## Verification

After re-seed completes, spot-check that the affected silver tables
carry the new column's values:

```sql
SELECT {vp_name}, COUNT(*) FROM <silver_schema>.<affected_node>
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

The distribution should match what's expected for the new candidate
(no NULLs from the old-column path, no stale values).
"""
    return RemediationArtifacts(option="D", runbook_markdown=md)


def _draft_option_e(*, vp_name, new_candidate, rationale) -> RemediationArtifacts:
    md = f"""# Remediation — Option E (full re-seed)

**Variation point**: `{vp_name}` → `{new_candidate}`

## When to use

Rare. Option D (targeted) is the v0.3 default; choose Option E only
when the operator wants a clean audit baseline across all marts
(post-major-Fusion-upgrade, post-incident audit, etc.). Cost is
hours-to-days depending on tenant size.

## Command

```
aidp-fusion-autopilot run --mode seed
```

(No `--datasets` filter — every silver/gold node rebuilds.)

## Rationale

{rationale}

## Verification

`fusion_autopilot_state_latest` shows `status=success` for every dataset
after completion. Affected node values match the new candidate.
"""
    return RemediationArtifacts(option="E", runbook_markdown=md)


__all__ = [
    "OptionDeferredError",
    "RemediationArtifacts",
    "draft_remediation",
]
