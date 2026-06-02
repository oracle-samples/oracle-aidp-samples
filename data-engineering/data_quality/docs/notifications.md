# Notifications

Qualifire routes alerts by severity level to configurable notification channels.

## Per-Severity Routing

Each validation can route pass, warnings, and errors to different channels:

```yaml
notify:
  on_success: ["email"]              # optional — notify on success
  warning: ["email"]
  error: ["email", "slack"]
```

This means:
- PASS → email (only when explicitly configured; disabled by default)
- WARNING → email only
- ERROR → email + slack

### Success Notifications

By default, passing validations do **not** trigger notifications. To opt in:

```yaml
notify:
  on_success: ["email"]              # send email when ALL validations pass
```

This is useful for audit trails or confirmation that a pipeline completed cleanly.

## Cross-Dataset Grouping

When a YAML config validates multiple datasets, notifications are **grouped by (channel, severity)** across all datasets. The grouping key is the channel name — since each channel name maps to one notifier instance (one webhook URL, one set of recipients), datasets routing to the same channel are naturally grouped.

```yaml
notifications:
  oncall_slack:
    type: slack
    webhook_url: "https://hooks.slack.com/services/oncall"
  sales_email:
    type: email
    recipients: ["sales-team@co.com"]
  infra_email:
    type: email
    recipients: ["infra-team@co.com"]

datasets:
  - name: "sales"
    validations:
      - notify: { error: ["oncall_slack", "sales_email"] }
  - name: "inventory"
    validations:
      - notify: { error: ["oncall_slack", "infra_email"] }
  - name: "orders"
    validations:
      - notify: { error: ["oncall_slack"] }
```

If all three datasets have errors:
- **oncall_slack** receives **ONE** message containing all 3 datasets (same channel)
- **sales_email** receives **ONE** message with sales only
- **infra_email** receives **ONE** message with inventory only

To control which teams get which datasets, use separate channel names with different recipients. No grouping config flag needed — it follows from the channel definitions.

## Built-In Channels

### Email (SMTP)

```yaml
notifications:
  email:
    type: email
    smtp_host: "smtp.company.com"
    smtp_port: 587
    smtp_user: "qualifire@company.com"
    smtp_password: "secret"
    use_tls: true
    sender: "qualifire@company.com"
    recipients:
      - "team@company.com"
```

### Slack (Webhook)

```yaml
notifications:
  slack:
    type: slack
    webhook_url: "https://hooks.slack.com/services/T.../B.../xxx"
```

### Generic Webhook

Works with PagerDuty, OpsGenie, Teams, or any HTTP endpoint:

```yaml
notifications:
  pagerduty:
    type: webhook
    url: "https://events.pagerduty.com/v2/enqueue"
    method: POST
    headers:
      Authorization: "Token token=xxx"
      Content-Type: "application/json"
```

The webhook sends a JSON payload with:
```json
{
  "severity": "ERROR",
  "dataset_name": "sales_daily",
  "table": "catalog.schema.sales",
  "owner": "team-x",
  "bu": "finance",
  "run_id": "uuid",
  "validations": [
    {
      "name": "threshold_check.row_count",
      "type": "threshold",
      "severity": "ERROR",
      "message": "row_count = 50 (violates error threshold: {'min': 100})",
      "metric_name": "row_count",
      "actual_value": 50
    }
  ]
}
```

## Custom Notification Plugins

Register your own notifier:

```python
from qualifire.notification.base import Notifier, register_notifier
from qualifire.core.models import DatasetResult, NotificationResult, Severity

class TeamsNotifier(Notifier):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @property
    def channel_name(self) -> str:
        return "teams"

    def send(self, dataset_result, severity, owner, bu):
        # your implementation
        return NotificationResult(
            channel="teams", severity=severity, status="sent"
        )

register_notifier("teams", TeamsNotifier)
```

Then use it in config:
```yaml
notifications:
  teams:
    type: teams
    webhook_url: "https://..."
```

## Alert Deduplication — `skip_renotification`

Qualifire dispatches every matching alert by default. Operators can opt into a runtime kill switch — `skip_renotification` — that skips dispatch for any (dataset, validation, metric, dim) key whose prior run already paged at the same severity.

This is a **runtime flag**, not a YAML field. Use it on retries / replays / CI rehearsals to avoid duplicate paging:

```bash
# CLI — single run
qualifire run --config qf.yml --skip-renotification

# CLI — backfill replay (90-day backfill won't fire 90 days of pages)
qualifire backfill --config qf.yml --partition 2026-04-01..2026-06-30 --skip-renotification
```

```python
# Programmatic
from qualifire.api import Qualifire
qf = Qualifire(...)
result = qf.run_config("qf.yml", skip_renotification=True)
report = qf.backfill(config="qf.yml", partition_ts=("2026-04-01", "2026-06-30"), skip_renotification=True)
```

**How it works**: When `skip_renotification=True`, Qualifire pre-fetches the validation history snapshot via `read_validation_history_bulk` and, for each `(channel, severity, base_name)` group, drops any (dataset, validation, metric, dim) key whose most-recent persisted row carries the same severity. If every key in the group is dropped, the engine emits a synthetic `notification_status='skipped'` row for forensic visibility instead of dispatching.

**Default is False** so retries / replays re-page by default — operators opt in explicitly. The previous per-validation `suppress_repeat_alerts` YAML field has been removed; YAMLs carrying the field will fail loud with guidance pointing at this runtime flag.

## Notification Grouping

Notifications are grouped per dataset. One notification per dataset per severity level, containing all relevant validation results — not one notification per individual validation.

## Message Format

All built-in notifiers use a standardized format:

```
Qualifire Alert — ERROR
Dataset: sales_daily
Table: catalog.schema.sales
Owner: data-engineering | BU: finance
Run: a1b2c3d4-...

Validation Results:
  [ERROR] threshold_check.row_count: row_count = 50 (violates error threshold: {'min': 100})
  [ERROR] slo_check: Data freshness: 10H since last update (threshold: error < 8H)
```

## Qualifire-Internal Failures

Two engine paths produce `ValidationResult` rows that look like
data-quality findings but represent qualifire-internal failures:

1. **Persistence-infrastructure outages.** When the system-table
   write fails, the engine creates a `qualifire.persistence`
   warning row capturing the underlying exception.
2. **Validator-execution exceptions.** When validator code throws
   an unhandled exception, the engine wraps it as a
   `<type>_error` ValidationResult so the run fails loudly.

Both classes carry `details['qualifire_internal_failure'] = True`.

### Distinct exception class

Qualifire raises **two sibling exception classes** so callers can
route data-team alerts and engineering on-call to different queues:

| Failure category | Exception | Inherits from |
|---|---|---|
| Data-quality finding (validator threshold breach, drift, etc.) | `QualifireValidationError` | `QualifireError` |
| Library / infrastructure failure (storage outage, validator code threw) | `QualifireInternalError` | `QualifireError` |

The two are **siblings, not parent/child**. `except
QualifireValidationError` does NOT catch internal failures and
vice versa — that is the contract. Both inherit from the
top-level `QualifireError` for catch-all purposes.

```python
try:
    qf.run_config(yaml_path)
except QualifireValidationError as e:
    notify_data_team(e)            # data findings — operator's data is bad
except QualifireInternalError as e:
    page_engineering_oncall(e)     # qualifire is broken — library/infra
```

Mixed runs (real data findings AND internal failures alongside)
raise `QualifireValidationError`. The data findings take
precedence — they're the actionable result. Internal failures in
the same run surface in `e.result.datasets[*].validation_results[*]`
with `details['qualifire_internal_failure']=True` for forensic
inspection. The exception message includes the count of internal
failures so operators routing to engineering on-call can
introspect.

For persistence-infrastructure outages specifically, the original
storage exception is preserved as `__cause__` (PEP 3134
chaining):

```python
try:
    qf.run_config(yaml_path)
except QualifireInternalError as e:
    if e.__cause__ is not None:
        # Storage backend exception (Spark / JDBC / catalog).
        log_storage_outage(e.__cause__)
    else:
        # Validator code threw — library bug.
        log_library_bug(e)
```

### Notification suppression

The notification dispatcher **filters these rows out of every
configured channel's payload**, both at severity bucketing and at
message-body building. Operators don't get false-positive
Slack/email pages when:

- A storage outage prevents qualifire from persisting validation
  results (the validations themselves may have succeeded — paging
  about a library / infra failure as if it were a finding is the
  wrong contract).
- A validator throws because of a library bug (e.g. an unexpected
  pandas extension dtype).

The rows are still **persisted to the system table** for forensic
trail. The run still **raises `QualifireValidationError`** so
non-zero-exit-code-driven monitoring fires. On INFRA_FAILURE the
exception message includes a summary of any data-quality ERROR
findings that were also present so operators see both axes of
the failure even though notifications were suppressed.

The dashboard renders marked rows with a wrench-icon (🔧) prefix
and a muted color, segregated into a separate "Internal" bucket
on the severity pie + summary cards. The pass-rate trend
explicitly excludes them so a storage outage doesn't tank the
displayed pass rate.
