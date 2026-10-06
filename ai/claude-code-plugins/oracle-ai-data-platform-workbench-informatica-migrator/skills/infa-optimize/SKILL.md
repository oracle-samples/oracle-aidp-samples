---
name: infa-optimize
description: Spark performance suggestions over already-generated notebooks via `infa2aidp cli optimize`, with an optional --auto-apply for the subset of suggestions safe to apply mechanically. Use after infa-migrate-mapping to check for Spark anti-patterns in the generated code — this is a code-quality pass, not a correctness check (that's infa-reconcile) and not a cluster-execution check (nothing here touches a live cluster).
---

# `infa-optimize` — Spark performance suggestions

Runs `infa2aidp cli optimize` over already-generated `.ipynb`/`.py` files
and reports Spark performance anti-patterns, with a subset auto-fixable.

## When to use

- After [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) (unless
  `--skip-optimize` was passed, in which case this already ran as part of
  that command).
- The user asks "any performance issues", "optimize these notebooks".

## Canonical invocation

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli optimize \
  -i <migrate-output-dir> \
  -o ./optimization_report
```

Add `--auto-apply` to mechanically rewrite the subset of suggestions the
optimizer considers safe to apply without a human decision — the notebook
files under `<input>/` are modified in place when this flag is set.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-i, --input` | required | Must be a directory; recurses for `.ipynb` and `.py` files |
| `-o, --output` | `./optimization_report` | |
| `--auto-apply` | off | Rewrites files in place for auto-applicable suggestions only |

## Output

Console: `<N> notebook(s): <T> suggestion(s), <A> auto-applicable ->
<output dir>`. Writes `optimization_report.md`.

## What this is not

- Not a correctness check. A notebook can be Spark-performant and still be
  wrong — use [`infa-reconcile`](../infa-reconcile/SKILL.md) for
  correctness against real data.
- Not a live-cluster profiling tool. All analysis is static, over the
  generated source — nothing here runs on AIDP.

## After this

If `--auto-apply` was used, re-run
[`infa-review`](../infa-review/SKILL.md) or at least diff the modified
notebooks before [`infa-deploy`](../infa-deploy/SKILL.md) — an automatic
rewrite is still a code change worth a look.
