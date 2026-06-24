---
description: Read from Salesforce into a Spark DataFrame in an AIDP notebook via the AIDP `aidataplatform` Spark format handler. Use when the user mentions Salesforce, SFDC, Sales Cloud, Service Cloud, Account, Opportunity, Lead, sObject, SOQL. Auth is host/port + user/password. Read-only.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-salesforce` — Salesforce via AIDP `aidataplatform`

## When to use
- User wants to ingest Salesforce data (Account, Opportunity, Lead, Contact, custom sObjects) into a Spark DataFrame from an AIDP notebook.
- User mentions: "Salesforce", "SFDC", "Sales Cloud", "Service Cloud", "sObject", "SOQL", a Salesforce object name (Account, Opportunity, etc.).

## When NOT to use
- For arbitrary REST APIs with a custom manifest → [`aidp-rest-generic`](../aidp-rest-generic/SKILL.md).
- For Oracle CX/CRM → [`aidp-siebel`](../aidp-siebel/SKILL.md) (Siebel) or Fusion CX via [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md).

## Prerequisites in the AIDP notebook
1. Helpers on `sys.path` (run `aidp-connectors-bootstrap` first).
2. Env vars / OCI Vault secrets:
   - `SFDC_HOST` (Salesforce login host, e.g. `login.salesforce.com` or `<my-domain>.my.salesforce.com`)
   - `SFDC_PORT` (typically `443`)
   - `SFDC_DATABASE_NAME` (org name / database identifier; for most tenants this is just the org name)
   - `SFDC_USER` (Salesforce username — typically `<email>`)
   - `SFDC_PASSWORD` (password concatenated with security token: `<password><security-token>`)
   - `SFDC_SCHEMA` (typically `SFORCE` for the connector)
   - `SFDC_TABLE` (sObject API name, e.g. `Account`, `Opportunity`, `Custom_Object__c`)

## Read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="SFORCE",
    host=os.environ["SFDC_HOST"],
    port=int(os.environ.get("SFDC_PORT", "443")),
    database_name=os.environ["SFDC_DATABASE_NAME"],
    user=os.environ["SFDC_USER"],
    password=os.environ["SFDC_PASSWORD"],
    schema=os.environ.get("SFDC_SCHEMA", "SFORCE"),
    table=os.environ["SFDC_TABLE"],   # e.g. "Account", "Opportunity"
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Pushdown SQL

Use `pushdown.sql` to push a SOQL-like query at the source — useful for filtering on indexed fields, joins via relationship paths, or LIMIT semantics.

```python
opts = aidataplatform_options(
    type="SFORCE",
    host=os.environ["SFDC_HOST"],
    port=int(os.environ.get("SFDC_PORT", "443")),
    database_name=os.environ["SFDC_DATABASE_NAME"],
    user=os.environ["SFDC_USER"],
    password=os.environ["SFDC_PASSWORD"],
    extra={
        "pushdown.sql": (
            "SELECT Id, Name, AnnualRevenue, BillingCountry "
            "FROM Account "
            "WHERE AnnualRevenue > 1000000 AND BillingCountry = 'United States'"
        ),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(10)
```

## Gotchas
- **`type` is `SFORCE`, not `SALESFORCE`.** Easy to get wrong if you're following the human-readable name. The connector type literally says `SFORCE`.
- **Password requires the security token appended.** Salesforce username/password auth needs `<password><security-token>` concatenated as a single string. The security token is reset each time the user changes their password — emailed to the user.
- **API limits.** Salesforce enforces per-org daily API call quotas. Bulk reads count against the quota. Use `pushdown.sql` with selective filters and field projection to minimize calls.
- **Connector is read-only.** Salesforce writes (create/update sObjects) need to go through the Salesforce REST/Bulk API or the Composite API directly.
- **Custom objects** end in `__c` (e.g. `Project__c`). Custom fields end in `__c` too — always include the trailing `__c` in `pushdown.sql`.
- **Field-level security.** The connector user inherits Salesforce profile + permission sets. Fields hidden by FLS won't appear in the result.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Only_Ingestion_Connectors/Salesforce.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors/Salesforce.ipynb)
