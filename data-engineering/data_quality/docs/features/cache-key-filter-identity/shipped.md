---
shipped: 2026-05-10
status: superseded
---

# Superseded: cache-key-filter-identity

This idea proposed adding a filter-identity column to the
system table so `skip_if_cached` could safely short-circuit
filtered collectors by including the filter in the natural
key.

## Why superseded

`skip-recollection` (PR #25, merged 2026-05-10) shipped with
the explicit operator decision **NOT** to include filter in
the natural key. Quote from the planning sweep:

> Since we are checking on (dataset, table, metric, partition,
> dim), changing filter would still skip collection if metric
> is already present. Do include any check on the filter.
> However this needs to be documented similar to
> skip_revalidation related behaviour.

The trade-off (filter NOT in natural key; cross-filter replay
permitted) is documented in `docs/CHANGELOG.md` Unreleased
Breaking entry for skip-recollection. The "filter-aware
short-circuit" this idea proposed is therefore a non-goal —
the broader feature shipped a different (simpler) contract.

## What if an operator ever wants filter-scoped caching?

A future feature could revisit this. It would need:
- New `effective_filter_hash` column on the system table.
- Migration logic for existing rows (default to NULL hash →
  match only NULL-hash queries).
- Engine: include the hash in the natural-key lookup.
- Per-backend changes across all 4 storage backends.

That's a substantial rework that should ship under its own
adversarial review cycle, not via this superseded idea.

## No action required

No code changes; this file marks the idea as superseded so the
backlog dashboard reflects accurate state.
