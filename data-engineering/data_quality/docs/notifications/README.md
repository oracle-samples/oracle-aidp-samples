# Notifications

Qualifire routes alerts by severity to one or more channels. This
folder documents each channel in depth (config, payload, retries,
secret handling), grouping behavior across datasets, and how to
read a Qualifire alert.

The single-page legacy reference at [`../notifications.md`](../notifications.md)
remains for quick lookups; the per-channel pages here go deeper.

| Channel | Page | Use it for |
|---------|------|------------|
| Email (SMTP) | [email.md](email.md) | Audit trails, distribution lists |
| Slack | [slack.md](slack.md) | On-call channels, oncall escalation |
| Generic Webhook | [webhook.md](webhook.md) | PagerDuty, Opsgenie, Teams, custom ingest |
| Cross-dataset grouping | [grouping.md](grouping.md) | Bulk daily runs, multi-dataset configs |

## What's in every alert

After the R3 notifier-detail enrichment, every notification body
includes the per-validator diagnostic block:

```
Qualifire Alert — ERROR
Dataset: sales_daily
Table: catalog.schema.sales
Owner: data-eng | BU: finance
Run: e2e_run_42

Validation Results:
  [ERROR] pattern_check: Pattern AUC: 0.91 (+/-0.03)
    - top contributing features:
        • amount (0.4200)
        • channel_mobile (0.1800)
        • merchant_id (0.0910)
    - AUC: 0.910 ± 0.030
    - sample sizes: current=500, past=1500
  [WARNING] drift_check.avg_amount: avg_amount = 142.5 deviates 28.4% from past mean
    - current: 142.5 | past mean: 110.0
    - deviation_pct: 28.4
    - z_score: 2.1
```

The block is computed by `format_validation_details(vr)` in
`notification/base.py`. Per-type behavior:

| Type | Diagnostic fields |
|------|-------------------|
| `pattern` | top_contributing_features (all 5), AUC ± std, current / past sample sizes |
| `shape` | top_contributing_features, anomaly_ratio |
| `drift` | current vs past mean, deviation_pct, deviation_abs, z_score, rate_of_change_pct, rate_of_change_abs |
| `trend` | observed, predicted (yhat), prediction interval [yhat_lower, yhat_upper] |
| `threshold` | actual, threshold dict |
| `slo` | data age, threshold |
| any | dimension_value (if present), partition_ts (if present) |

If SHAP is unavailable or the explanation pipeline failed, the
diagnostic block surfaces `- explanation unavailable: <reason>`
rather than silently dropping the section.

## Severity routing

```yaml
notify:
  on_success: ["email"]    # opt-in; default: no PASS notifications
  warning: ["email"]
  error: ["email", "slack"]
```

The engine sends one message per channel per severity, after
deduplication. See [`../notifications.md`](../notifications.md#alert-deduplication)
for the dedup contract.
