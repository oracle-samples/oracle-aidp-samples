# Fixtures

Field-name-only snapshots of what a live Snowflake account returns, read by
`tests/test_enterprise_estate.py` to keep the offline emulation in line with the
real response shape. `tests/fake_sql.py` takes canned responses in code and
loads nothing from this folder.

To refresh after a Snowflake behaviour change, run the statements against a
live account and record the field names only, as in
`snowflake_live_field_names.json`.

**Fixtures hold field names or synthetic values only. Never paste payloads from
`inventory.json` or any other live run, and never commit an account identifier,
user name, credential, or customer object name.**
