# Mart Overlay Authoring

Use this guide when the shipped gold layer does not cover a business request,
or when a customer needs to customize the SQL for a shipped mart.

`aidp-fusion-autopilot init` creates only:

```text
bundle.yaml
aidp.config.yaml
```

It does not copy bronze, silver, or gold node YAML files into the customer
project. Those node definitions live in the active content pack. Customer
changes belong in an overlay pack under the customer bundle directory.

## Choose The Right Path

| Need | Path |
|---|---|
| Use a shipped mart such as `gl_balance` | Select it in `bundle.yaml`; do not create an overlay. |
| Add a new customer mart | Add a new gold node YAML and SQL under `overlays/<name>/`. |
| Override shipped mart SQL | Add an overlay `overrides:` entry that replaces the SQL path. |
| Change dependencies, grain, natural key, target table, or output schema of a shipped mart | Create a new mart id instead of overriding the shipped mart in place. |
| Resolve tenant column variation | Use `/medallion-author`, not `/mart-author`. |

The overlay is not active until the bundle points at it. Use
`aidp-fusion-autopilot use-pack`; do not hand-edit `contentPack.path` for the
normal workflow. Editing the bundle scope lists (`datasets`, `dimensions.build`,
and `gold.marts`) is normal when you want a narrow customer bundle.

By default, `use-pack` aligns `dimensions.build` and `gold.marts` to every
silver and gold node in the resolved pack chain. That is useful for a full
starter pack run, but it can broaden a narrow customer bundle. Use `--no-align`
when applying an overlay to a narrow bundle or when overriding one shipped mart:

```bash
aidp-fusion-autopilot use-pack overlays/<name> --profile <tenant> --no-align
```

With `--no-align`, `use-pack` still updates `contentPack`, but it preserves the
existing `dimensions.build` and `gold.marts` lists. If the overlay adds a new
mart, add only that mart to `gold.marts` before running it.

## Use A Shipped Mart

For a shipped mart such as `gl_balance`, edit `bundle.yaml` so the dependency
chain is selected:

```yaml
datasets:
  - id: gl_period_balances
    mode: full
  - id: gl_coa
    mode: full

dimensions:
  build:
    - dim_account

gold:
  marts:
    - gl_balance
```

Then validate and run:

```bash
aidp-fusion-autopilot validate
aidp-fusion-autopilot bootstrap --check-iam
aidp-fusion-autopilot run --mode seed
```

No custom mart YAML is needed for this path.

## Add A New Gold Mart

Create an overlay beside `bundle.yaml`:

```text
overlays/supplier-currency-summary/
  pack.yaml
  gold/supplier_spend_by_currency.yaml
  gold/supplier_spend_by_currency.sql
```

`pack.yaml` identifies the overlay and declares which shipped pack it extends:

```yaml
id: supplier-currency-summary
version: 0.1.0
extends: fusion-finance-starter@0.1.0
description: Customer mart for supplier spend by currency.
compatibility:
  pluginMinVersion: 0.3.0
  fusionFamilies:
    - ERP
  aidp:
    requiresDelta: true
```

The gold node YAML declares the new mart:

```yaml
id: supplier_spend_by_currency
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend_by_currency.sql
target: supplier_spend_by_currency

dependsOn:
  bronze:
    - id: ap_invoices
      role: primary
      watermark:
        column: _extract_ts
  silver:
    - id: dim_supplier
      role: lookup

refresh:
  seed:
    strategy: replace
  incremental:
    strategy: replace
    reason: Replace aggregate until aggregate_merge is available.

outputSchema:
  columns:
    - name: currency_code
      type: string
      nullable: false
      pii: none
    - name: total_invoice_amount
      type: decimal(20,2)
      nullable: true
      pii: none
    - name: gold_built_at
      type: timestamp
      nullable: false
      pii: none
    - name: gold_run_id
      type: string
      nullable: true
      pii: none

quality:
  tests:
    - type: row_count_min
      min: 1
      whenSourceNonEmpty: bronze.ap_invoices
```

The SQL file uses content-pack renderer tokens:

```sql
SELECT
  COALESCE(inv.{{ column.invoice_currency_code }}, 'UNKNOWN') AS currency_code,
  SUM(COALESCE(inv.ApInvoicesInvoiceAmount, 0)) AS total_invoice_amount,
  current_timestamp() AS gold_built_at,
  {{ run_id_literal }} AS gold_run_id
FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv
GROUP BY COALESCE(inv.{{ column.invoice_currency_code }}, 'UNKNOWN')
```

Validate, wire, and seed:

```bash
aidp-fusion-autopilot content-pack validate overlays/supplier-currency-summary
aidp-fusion-autopilot use-pack overlays/supplier-currency-summary --profile finance-default --no-align
aidp-fusion-autopilot validate
aidp-fusion-autopilot bootstrap --check-iam
aidp-fusion-autopilot run --mode seed --datasets supplier_spend_by_currency --layers gold
```

Because `--no-align` preserves the existing bundle scope, make sure
`bundle.yaml` selects the new mart:

```yaml
gold:
  marts:
    - supplier_spend
    - supplier_spend_by_currency
```

After the table exists in AIDP, run `oac-dataset-advisor` again. It must use
the live AIDP gold catalog before recommending an OAC dataset.

## Override Existing Mart SQL

Use this path only when the shipped mart identity and table contract stay the
same, but the SQL implementation needs customer-specific logic.

Create an overlay:

```text
overlays/gl-balance-custom/
  pack.yaml
  gold/gl_balance_custom.sql
```

`pack.yaml` replaces only the SQL path for the shipped `gold/gl_balance` node:

```yaml
id: gl-balance-custom
version: 0.1.0
extends: fusion-finance-starter@0.1.0
description: Customer SQL override for shipped gl_balance.
compatibility:
  pluginMinVersion: 0.3.0
  fusionFamilies:
    - ERP
  aidp:
    requiresDelta: true

overrides:
  gold/gl_balance:
    sql: gold/gl_balance_custom.sql
    quality:
      tests:
        - type: not_null
          columns: [ledger_id, account_id, period_year, period_num]
```

Start `gold/gl_balance_custom.sql` from the shipped `gl_balance.sql` for the
same plugin version, then edit the customer-specific logic. Keep the same
target table contract unless you intentionally want to rebuild all downstream
OAC datasets and workbooks that depend on `gold.gl_balance`.

Validate and wire:

```bash
aidp-fusion-autopilot content-pack validate overlays/gl-balance-custom
aidp-fusion-autopilot use-pack overlays/gl-balance-custom --profile finance-default --no-align
aidp-fusion-autopilot validate
aidp-fusion-autopilot run --mode seed --datasets gl_balance --layers gold
```

Use `--no-align` here so an override for `gl_balance` does not expand a narrow
bundle to every shipped gold mart. The existing `gold.marts: [gl_balance]`
selection remains intact.

## Full YAML Replacement

Full same-id YAML replacement for a shipped mart is not the normal supported
workflow. The overlay `overrides:` path is for SQL replacement and quality-test
extension.

If the customer needs to change any of these fields, create a new mart id
instead:

- `dependsOn`
- `target`
- `refresh.incremental.naturalKey`
- grain
- output columns or data types
- semantics that would break existing OAC datasets

Example: use `gl_balance_customer` instead of replacing `gl_balance`. That keeps
the shipped mart stable and makes the customer contract explicit.

## Skill Routing

Use `/mart-author` when a new customer mart or additive analytical content is
needed. The skill writes overlay YAML/SQL, validates it, and wires the bundle
with `use-pack`.

Use `/medallion-author` only for tenant variation recovery such as
`AIDPF-2010`, `AIDPF-2011`, or column alias candidate updates. It does not
author new silver/gold nodes.
