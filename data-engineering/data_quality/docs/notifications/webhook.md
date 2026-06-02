# Webhook Notifier

Generic HTTP webhook for any endpoint that accepts JSON: PagerDuty,
Opsgenie, Microsoft Teams (incoming webhook), Datadog incident, custom
ingest services.

## Configuration

```yaml
notifications:
  pagerduty:
    type: webhook
    url: "https://events.pagerduty.com/v2/enqueue"
    method: POST
    headers:
      Authorization: "secret://pagerduty/integration_key"
      Content-Type: "application/json"
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"webhook"` | yes | discriminator |
| `url` | `str` | yes | endpoint URL |
| `method` | `"POST"` \| `"PUT"` | no (default POST) | uppercased automatically |
| `headers` | `dict[str, str]` | no | merged with `Content-Type: application/json` |

## Programmatic shape

```python
from qualifire.notification.webhook_notifier import WebhookNotifier

notifier = WebhookNotifier(
    url="https://events.pagerduty.com/v2/enqueue",
    method="POST",
    headers={"Authorization": "Token token=..."},
)
```

## Payload

After the R3 enrichment, the JSON body includes per-validation
`details` and `dimension_value` so downstream pipelines don't need to
round-trip the system table:

```json
{
  "severity": "ERROR",
  "dataset_name": "sales_daily",
  "table": "catalog.schema.sales",
  "owner": "data-eng",
  "bu": "finance",
  "run_id": "e2e_run_42",
  "validations": [
    {
      "name": "pattern_check",
      "type": "pattern",
      "severity": "ERROR",
      "message": "Pattern AUC: 0.91 (+/-0.03)",
      "metric_name": "auc",
      "actual_value": 0.91,
      "expected_value": null,
      "dimension_value": null,
      "details": {
        "auc": 0.91,
        "auc_std": 0.03,
        "n_current": 500,
        "n_past": 1500,
        "top_contributing_features": [
          {"feature": "amount",         "importance": 0.42},
          {"feature": "channel_mobile", "importance": 0.18},
          {"feature": "merchant_id",    "importance": 0.09}
        ]
      }
    }
  ]
}
```

For grouped notifications (multiple datasets routed to the same
webhook), the payload also includes a top-level `datasets` array with
one entry per dataset. The first dataset's `validations` mirror at the
top level for back-compat with single-dataset consumers.

## Targeting specific services

### PagerDuty Events API v2

```yaml
notifications:
  pagerduty:
    type: webhook
    url: "https://events.pagerduty.com/v2/enqueue"
    headers:
      Authorization: "secret://pagerduty/integration_key"
```

PagerDuty expects a particular payload shape (`event_action`,
`payload.summary`, etc.). The current Qualifire webhook payload
**does not** match PagerDuty's contract directly — wire in an
intermediary (e.g., a small Lambda) that reshapes the body.
This is on the backlog as a templating feature.

### Microsoft Teams

Teams accepts the simple webhook payload as-is in incoming webhooks
that show JSON content. For richer adaptive cards, route via an Azure
Function that reformats.

### Custom ingest

Most teams running Qualifire long-term build a small adapter:

```python
@app.post("/qualifire/ingest")
def ingest(payload: dict):
    for v in payload["validations"]:
        emit_metric(v["name"], v["actual_value"], severity=v["severity"])
        if v["details"].get("top_contributing_features"):
            log_features(v["name"], v["details"]["top_contributing_features"])
```

## Common pitfalls

- **Tokens in YAML** — never commit. Use a `secret://...`
  reference paired with `Qualifire(secret_resolver=...)`, or
  construct `WebhookNotifier(headers={...})` programmatically with
  resolved values. Plain `${ENV_VAR}` substitution is **not**
  supported by Qualifire — the literal string is sent on the wire.
- **30-second timeout, no retries** — the notifier issues a single
  request with `timeout=30`. Slow endpoints cause delivery failures
  that surface as `notification_status=failed` rows in the system
  table. If your endpoint is occasionally slow, put a queue in front.
- **HTTP errors don't fail the run** — the engine logs and persists
  the failure but continues. Monitor `notification_status` in the
  system table to catch silent delivery drops.
