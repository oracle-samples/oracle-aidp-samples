---
description: Run a Planning data-slice export against Oracle EPM Cloud (Planning / EPBCS) and materialize as a Spark DataFrame in an AIDP notebook. Use when the user mentions EPM Cloud, EPBCS, Hyperion Planning, planning app, MDX export, or wants Planning data in Spark. HTTP Basic auth with identity-domain-prefixed username.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-epm-cloud` — EPM Cloud Planning REST → Spark

## When to use
- User wants to pull a Planning data slice (POV × columns × rows) from EPM Cloud into Spark.
- User mentions: "EPM Cloud", "EPBCS", "Hyperion Planning", "planning app", "exportdataslice", "MDX export".

## When NOT to use
- For Essbase 21c on-prem MDX → use [`aidp-essbase`](../aidp-essbase/SKILL.md).
- For Fusion ERP / HCM → use [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md).

## Prerequisites in the AIDP notebook
1. `pip install requests pandas` (usually pre-installed).
2. Helpers on `sys.path`.
3. EPM pod URL + credentials in the form ``tenancy.user@domain``.

## Auth: HTTP Basic

```python
import os
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.epm import (
    list_applications, export_data_slice, slice_to_long_dataframe,
)

# EPM_USERNAME MUST be in identity-domain form: tenancy.user@domain
# e.g. epmloaner622.first.last@oracle.com
session = http_basic_session(
    username=os.environ["EPM_USERNAME"],
    password=os.environ["EPM_PASSWORD"],
    base_url=os.environ["EPM_BASE_URL"],
)

# Pre-flight: confirm credentials work and the app is reachable
apps = list_applications(session, os.environ["EPM_BASE_URL"])
print("applications:", [a["name"] for a in apps])

# Run the export
slice_response = export_data_slice(
    session=session,
    base_url=os.environ["EPM_BASE_URL"],
    application=os.environ["EPM_APPLICATION"],
    plan_type=os.environ["EPM_PLAN_TYPE"],
    grid_definition={
        "suppressMissingBlocks": True,
        "suppressMissingRows": True,
        "pov": {
            "dimensions": ["HSP_View", "Year", "Scenario", "Version", "Entity", "Product"],
            "members": [["BaseData"], ["FY26"], ["Actual"], ["Working"], ["Total Entity"], ["P_TP"]]
        },
        "columns": [{"dimensions": ["Period"], "members": [["Jan", "Feb", "Mar", "Apr", "May", "Jun"]]}],
        "rows":    [{"dimensions": ["Account"], "members": [["IChildren(PL)"]]}],
    },
)

df = slice_to_long_dataframe(spark, slice_response)
df.show(10)
print("cells:", df.count())
```

## Gotchas
- **Username MUST include the identity-domain prefix.** `tenancy.user@domain` (e.g. `epmloaner622.first.last@oracle.com`). The bare `first.last@oracle.com` returns 401.
- **POV members must be leaf-level.** EPM returns 400 / empty if you pass a parent member without `IChildren()` / `ILvl0Descendants()`.
- **`#Missing` cells** — empty Planning blocks come back as the literal string `"#Missing"`. Helper preserves this in the `value` column; cast to numeric and filter as needed.
- **401 vs 403** — 401 = auth fail (re-check Basic creds). 403 = permission denied (different code path; don't retry).

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/epm.py](../../scripts/oracle_ai_data_platform_connectors/rest/epm.py)
- EPM Planning REST docs: https://docs.oracle.com/en/cloud/saas/planning-budgeting-cloud/pbcrr/
