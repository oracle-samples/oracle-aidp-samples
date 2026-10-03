# PowerCenter fixtures

## Provenance
The fixtures dated before 2026-09 came from the test set of an earlier Rust
Informatica parser, ported together with four of its parser modules. They were
written to exercise a *different* parser with different structural assumptions.

## Known limitation, now being fixed
They originally carried **no** `<SOURCE>`, `<TARGET>` or `<INSTANCE>` elements, so they
exercised transformation bodies only - never source binding or target write. A real
`pmrep` export always has them. See `multi_stage_invoice_dw.xml` (6/3/9) and
`../corpus/orders_transform.xml` (1/1/4) for the correct shape.
