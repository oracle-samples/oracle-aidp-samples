---
id: pydantic-docstring-audit
name: Pydantic Config Class Docstring Audit
type: Documentation
priority: P2
effort: Small
impact: Medium
created: 2026-05-10
---

# Pydantic Config Class Docstring Audit

## Problem Statement

Pydantic config classes in `qualifire/core/config.py` carry the
truth (types, defaults, validators) but the docstrings are tuned
for engineering review, not for the data engineer asking
"what does `compare.step` do and what does it default to?"
The configuration reference (`docs/configuration.md`) should be
able to quote the docstrings as primary source — today it can't,
because some are too terse and some assume context.

## Scope

- Audit each public config class in `qualifire/core/config.py`.
- Tighten docstrings so they read as the operator-facing
  authoritative answer.
- Each docstring should include: one-paragraph "what this is",
  field-by-field "what it does + default + accepted forms",
  common pitfalls.

## Why It Matters

- The configuration reference (`configuration-reference-audit`
  backlog) should be quotable from the docstrings; if the
  docstring is wrong the reference is wrong.
- Hover-help in IDEs picks up the docstrings — improving them
  helps every author who edits a YAML config.

## Affected Areas

- `qualifire/core/config.py` — every Pydantic class.
- `docs/configuration.md` — should quote / cross-link to the
  audited docstrings.

## Why Deferred

Captured during `comprehensive-config-and-architecture-docs`
v2 scope cut. Audit is mechanical but tedious (~70 classes);
deserves its own focused pass.
