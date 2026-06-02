# Email Notifier (SMTP)

Send alerts as plaintext emails via SMTP. Best for audit trails,
distribution lists, and channels that already aggregate operational
mail.

## Configuration

```yaml
notifications:
  email:
    type: email
    smtp_host: "smtp.company.com"
    smtp_port: 587
    smtp_user: "qualifire@company.com"
    smtp_password: "secret://smtp_creds/password"
    use_tls: true
    sender: "qualifire@company.com"
    recipients:
      - "data-team@company.com"
      - "oncall@company.com"
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | `"email"` | yes | discriminator |
| `smtp_host` | `str` | yes | SMTP server hostname |
| `smtp_port` | `int` | yes | typical: 587 (STARTTLS), 465 (TLS), 25 (plain) |
| `smtp_user` | `str` | no | omit for unauthenticated relays |
| `smtp_password` | `str` | no | accepts `secret://name/key` (see [secrets](../jinja_rendering.md#secrets)) |
| `use_tls` | `bool` | no (default true) | STARTTLS upgrade |
| `sender` | `str` | yes | From: header |
| `recipients` | `list[str]` | yes | To: header (one message, all recipients on To:) |

## Programmatic shape

```python
from qualifire.notification.email_notifier import EmailNotifier

notifier = EmailNotifier(
    smtp_host="smtp.company.com",
    smtp_port=587,
    smtp_user="qualifire@company.com",
    smtp_password="...",
    sender="qualifire@company.com",
    recipients=["data-team@company.com"],
)
```

## Subject line

- Single dataset: `Qualifire Alert — <severity> — <dataset_name>`
- Grouped (multiple datasets, same channel): `Qualifire Alert — <severity> — <N> datasets`

## Body

Plain text — see [README.md](README.md#whats-in-every-alert) for the
full format including the per-validator diagnostic block.

## Common pitfalls

- **Password in YAML** — never commit. Use a `secret://...`
  reference paired with `Qualifire(secret_resolver=...)`, or
  construct `EmailNotifier(...)` directly via the Python API and
  pass the resolved password as a kwarg. **Note:** plain
  `${ENV_VAR}` substitution is **not** performed by Qualifire —
  if you put `${SMTP_PASSWORD}` in YAML, the literal string is
  sent as the password and authentication fails.
- **Corporate SMTP rate limits** — large multi-dataset configs can
  hit "too many messages" caps. Use grouped notifications (the engine
  already collapses per channel) or split across multiple SMTP
  accounts.
- **Hop limits / spam filters** — alerts often get flagged by greylists
  on the first delivery. Whitelist `sender` upstream.
- **Plaintext only** — Qualifire does not emit HTML email today. If
  rich rendering matters, route through a webhook to a service that
  formats it (or generate the HTML report and link to it).
