# Cross-Dataset Notification Grouping

When one Qualifire run validates many datasets, each channel gets
**one** message that aggregates every dataset's matching alerts —
not N messages for N datasets. The grouping key is the channel name
(since each channel name maps to one notifier instance / one
recipient set).

## Why grouping is the default

A nightly run validating 50 tables with 5 alerts each used to send
250 Slack messages and bury the on-call channel. With grouping:

- `oncall_slack` channel: 1 message containing all 250 alerts.
- `team_email` channel: 1 message containing only the team's tables.

Grouping is automatic — there's no flag to enable it. It follows from
the fact that each channel name maps to one notifier with one
delivery target.

## Routing patterns

### Single oncall channel + per-team mailing lists

```yaml
notifications:
  oncall:
    type: slack
    webhook_url: "secret://slack_creds/oncall_webhook"
  sales_team:
    type: email
    recipients: ["sales-data@co.com"]
  inventory_team:
    type: email
    recipients: ["inventory-data@co.com"]

datasets:
  - name: sales
    validations:
      - notify: { error: ["oncall", "sales_team"] }
  - name: inventory
    validations:
      - notify: { error: ["oncall", "inventory_team"] }
  - name: orders
    validations:
      - notify: { error: ["oncall"] }
```

Result on a run with errors in all three datasets:

| Channel | Datasets in message |
|---------|---------------------|
| oncall | sales, inventory, orders |
| sales_team | sales |
| inventory_team | inventory |

### Severity escalation

```yaml
datasets:
  - name: sales
    validations:
      - notify:
          warning: ["sales_team"]
          error: ["sales_team", "oncall"]
```

Warnings stay with the team. Errors page on-call.

## Message body

For grouped messages (multiple datasets, same channel), the body
shifts to a per-dataset section list:

```
Qualifire Alert — ERROR
Owner: data-eng | BU: finance
Run: e2e_run_42
Datasets: 3

--- sales_daily (catalog.schema.sales) ---
  [ERROR] pattern_check: Pattern AUC: 0.91 (+/-0.03)
    - top contributing features:
        • amount (0.4200)
        • merchant_id (0.0910)
    - AUC: 0.910 ± 0.030
    - sample sizes: current=500, past=1500

--- inventory_daily (catalog.schema.inventory) ---
  [ERROR] threshold_check.row_count: row_count = 5 (violates error threshold {"min": 1000})
    - actual: 5
    - threshold: {"min": 1000}

--- orders_daily (catalog.schema.orders) ---
  [WARNING] drift_check.order_value: order_value deviates 32% from past mean
    - current: 47.5 | past mean: 70.0
    - deviation_pct: -32.1
```

Single-dataset alerts skip the dataset header.

## Webhook grouping

The webhook payload includes a top-level `datasets` array when more
than one dataset routes to the same channel:

```json
{
  "severity": "ERROR",
  "dataset_name": "sales_daily",
  ...
  "validations": [...],            // first dataset's validations
  "datasets": [
    {"dataset_name": "sales_daily", "table": "...", "validations": [...]},
    {"dataset_name": "inventory_daily", "table": "...", "validations": [...]},
    {"dataset_name": "orders_daily", "table": "...", "validations": [...]}
  ]
}
```

Adapters that already handle the single-dataset shape work unchanged
— they just see the first dataset. Adapters that want the full set
read `datasets[]`.

## Common pitfalls

- **Channel name drift** — typos like `on_call` vs `oncall` create two
  separate channels and break grouping. Validate channel names at
  config-load (Qualifire does, but stays case-sensitive).
- **One channel name → multiple notifiers** — not supported. If you
  need redundant delivery (Slack + Teams), declare them as separate
  channel names in `notify:`.
- **Per-validation `notify` overrides dataset `notify`** —
  inheritance is per-validation, not per-dataset. If you set
  dataset-level routing but one validation overrides, only that
  validation uses the override.
