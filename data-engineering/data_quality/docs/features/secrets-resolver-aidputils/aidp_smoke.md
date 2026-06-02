# AIDP Smoke Checklist — secrets-resolver-aidputils

Acceptance criterion 2.b — manual smoke against a real AIDP
notebook before tagging the feature as shipped.

This is **not** a CI test. It runs once per release on a real AIDP
workbench, against a real `aidputils.secrets` backend.

## CI substitute (already running)

`tests/test_api_secrets.py::TestAidpSmokeShape` exercises every
shape on this checklist *except* the parts that require a real
AIDP workbench:

| Smoke bullet | CI test |
|---|---|
| All-in-one fixture (jdbc.from_secret + email/slack refs) | `test_full_shape_with_stub_resolver` |
| No `secret://` literals in resolved config | `test_no_secret_literals_remain_in_resolved_config` |
| Construction without resolver fails with clear message | `test_construction_without_resolver_raises_with_clear_message` |
| `qf.jdbc.url` is a real URL post-resolution | `test_resolved_jdbc_url_is_real_url_not_directive` |
| Duck-typed resolver works (no Qualifire inheritance) | `test_duck_typed_resolver_no_inheritance` |
| No resolved-secret leakage in logs / persisted rows | `tests/test_notification/test_redact.py` (Phase 5 suite) |

The CI substitute test runs the **same constructor shape** as the
manual procedure below — `Qualifire(backend=..., secret_resolver=...,
owner=..., bu=...)` with no `system_table` / `jdbc` kwargs — and
relies on `run_config_parsed` to open storage after the YAML supplies
the JDBC block. If the manual fixture is later extended (e.g., to add
a webhook channel), update both `aidp_smoke_fixture.yaml` AND the CI
test so the parity claim stays honest.

What the CI substitute **cannot** verify (AIDP-only):
- `aidputils.secrets` actually conforms to the duck-typed Protocol
  (it does today, but a future AIDP-side change to `get(...)` would
  not be caught by CI).
- A live Slack channel actually receives the test webhook delivery.
- AIDP-side audit trail of "which secrets were read by this run."

The manual procedure below stays in force for those.

## Owner

Whoever is shipping this feature on the qualifire repo (commit
author of the `feature-ship` step).

## Target environment

- AIDP Workbench (any tier with `aidputils` auto-import enabled).
- A running, reachable PySpark + Delta runtime (the AIDP default
  is fine).
- Three test secrets pre-provisioned in the workbench's secret
  store:
  1. `qf_smoke_smtp` with a `password` key — any literal value;
     does not need to authenticate against a real SMTP server.
  2. `qf_smoke_slack` with a `webhook_url` key pointing at a
     **test** Slack channel webhook, not production.
  3. `qf_smoke_db` with `url` / `user` / `password` keys —
     pointing at a sandbox JDBC database, not production.

## Fixture

`docs/features/secrets-resolver-aidputils/aidp_smoke_fixture.yaml`
(create at smoke time; not committed because it carries
deployment-specific table names):

```yaml
owner: smoke
bu: smoke
system_table: catalog.schema.qf_smoke_history
system_table_backend: jdbc

jdbc:
  from_secret: qf_smoke_db

notifications:
  smoke_email:
    type: email
    smtp_host: smtp.invalid.example
    smtp_password: "secret://qf_smoke_smtp/password"
    sender: smoke@example.com
    recipients: [smoke-team@example.com]
  smoke_slack:
    type: slack
    webhook_url: "secret://qf_smoke_slack/webhook_url"

datasets: []
```

## Procedure

1. In an AIDP notebook cell. The smoke runs the Qualifire
   constructor **without** `system_table` / `jdbc` so
   `_init_storage()` does not fire at construction time;
   storage opens later inside `run_config_parsed` after the
   YAML's `jdbc:` block has been read and resolved:

   ```python
   import aidputils
   from qualifire import Qualifire

   qf = Qualifire(
       backend=...,    # AIDP-provided Spark backend
       secret_resolver=aidputils.secrets,
       owner="smoke", bu="smoke",
   )
   qf.run_config("docs/features/secrets-resolver-aidputils/aidp_smoke_fixture.yaml")
   ```

2. Capture the cell output.

3. Inspect the workbench's notebook log stream (or
   `qf_notifications` system-table rows for this run).

## Expected evidence

- [ ] **No `secret://` literals** appear anywhere in the cell
      output, log stream, or `qf_notifications.message` column for
      this run.
- [ ] **No resolved secret values** (the literal contents of the
      three test secrets) appear in any log line at INFO / DEBUG /
      ERROR level. Grep the stream.
- [ ] The Slack test channel receives a notification (datasets
      list is empty, so no validation runs — but the notifier
      *registration* succeeded with a resolved URL, which is what
      we're verifying).
- [ ] Calling the same fixture in a second cell with
      `Qualifire(...)` constructed **without**
      `secret_resolver=aidputils.secrets` raises
      `MissingSecretResolverError` naming the first offending
      reference and the field path.
- [ ] `qf.jdbc.url` after construction is a real `jdbc:` URL, not
      the literal `from_secret` directive.

## Sign-off

Once all five boxes above are checked, paste the cell output into
the shipped.md notes for this feature and tag the smoke as
complete.
