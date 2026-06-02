# Slack Notifier

Posts alerts to a Slack channel via incoming webhook URL.

## Configuration

```yaml
notifications:
  oncall_slack:
    type: slack
    webhook_url: "secret://slack_creds/oncall_webhook"
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"slack"` | yes | discriminator |
| `webhook_url` | `str` | yes | Slack incoming-webhook URL; accepts `secret://name/key` (see [secrets](../jinja_rendering.md#secrets)) |

## Programmatic shape

```python
from qualifire.notification.slack_notifier import SlackNotifier

notifier = SlackNotifier(webhook_url="https://hooks.slack.com/...")
```

## Message format

Plain text — Slack will auto-render code-fence-style indentation for
the diagnostic block. See [README.md](README.md#whats-in-every-alert)
for the canonical body shape.

The R3 enrichment puts the SHAP top contributors / drift signed
measures inline so on-call doesn't have to chase the system table.

## Common pitfalls

- **Webhook URL in YAML** — never commit. Use a `secret://...`
  reference paired with `Qualifire(secret_resolver=...)`, or
  construct `SlackNotifier(webhook_url=...)` programmatically with
  a resolved value. **Plain `${ENV_VAR}` substitution is not
  supported by Qualifire** — a literal `${SLACK_WEBHOOK_URL}` in
  YAML is sent as the URL and the post fails.
- **Rate limits** — Slack incoming webhooks cap at ~1 msg/sec per
  webhook. Big multi-dataset runs benefit from grouping (one message
  per channel, not per dataset).
- **Channel rotation** — incoming webhooks are tied to one channel.
  To route the same alert to multiple Slack channels, declare each
  one as a separate notification entry.
- **No threading / interactive components** — the notifier sends a
  single text post. Use a webhook → bot proxy if you need threads or
  buttons.

## Example: separate oncall vs team channels

```yaml
notifications:
  oncall:
    type: slack
    webhook_url: "secret://slack_creds/oncall_webhook"
  data_team:
    type: slack
    webhook_url: "secret://slack_creds/data_team_webhook"

datasets:
  - name: sales
    validations:
      - type: drift
        notify:
          warning: ["data_team"]            # team gets warnings
          error: ["data_team", "oncall"]    # oncall pages on errors
```
