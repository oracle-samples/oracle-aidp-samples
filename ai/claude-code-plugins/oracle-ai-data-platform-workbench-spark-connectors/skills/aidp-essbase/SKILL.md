---
description: Run an MDX query against an Oracle Essbase 21c cube and materialize the result as a Spark DataFrame in an AIDP notebook. Use when the user mentions Essbase, MDX, Essbase 21c, OLAP cube, or wants to read cube data into Spark. Auth is HTTP Basic.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-essbase` — Essbase 21c MDX → Spark

## When to use
- User wants to run an MDX SELECT against an Essbase 21c cube and load the result into Spark.
- User mentions: "Essbase", "MDX", "OLAP cube", "Essbase REST", "21c cube".

## When NOT to use
- For EPM Cloud Planning (cloud-hosted) → use [`aidp-epm-cloud`](../aidp-epm-cloud/SKILL.md).

## Prerequisites in the AIDP notebook
1. `pip install requests pandas`.
2. Helpers on `sys.path`.
3. Essbase REST URL + Basic credentials.
4. Network reachability AIDP → Essbase host (often on a private subnet — confirm `nc -zv <host> 9000`).

## Auth: HTTP Basic

Essbase 21c REST API expects `Authorization: Basic base64(user:pass)`. **The JET UI redirects to IDCS OAuth, but the REST surface is Basic-only** — passing an OCI session JWT as Bearer fails (different identity provider). Username is the Essbase service-admin (e.g. `Oacadmin1`).

If your Essbase host uses an internal CA chain not trusted by the AIDP cluster (e.g. `cealinfra.com`), pass `verify_tls=False` to the session helper. Live-validated against `https://ess21c.cealinfra.com/`.

```python
import os, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # internal CA
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.essbase import (
    execute_mdx, mdx_result_to_spark_dataframe,
)

session = http_basic_session(
    username=os.environ["ESSBASE_USER"],
    password=os.environ["ESSBASE_PASSWORD"],
    base_url=os.environ["ESSBASE_BASE_URL"],
    verify_tls=False,   # set True for trusted public CAs
)

mdx_query = """
SELECT
  {[Measures].[Sales]} ON COLUMNS,
  {[Product].[Product Family].Members} ON ROWS
FROM [Sample.Basic]
WHERE ([Year].[2026], [Scenario].[Actual])
"""

response = execute_mdx(
    session=session,
    base_url=os.environ["ESSBASE_BASE_URL"],
    application=os.environ["ESSBASE_APPLICATION"],
    cube=os.environ["ESSBASE_CUBE"],
    mdx_query=mdx_query,
)

df = mdx_result_to_spark_dataframe(spark, response)
df.show(20)
print("cells:", df.count())
```

## Gotchas
- **MDX braces `{...}` are required** around member sets — bare `[Product].[Product Family].Members` returns 400.
- **WHERE slicer** for POV — if you skip dimensions in `WHERE`, Essbase uses dimension defaults, which may be parents (returns aggregated/empty data depending on aggregation).
- **Network** — Essbase 21c is typically deployed on a private host (port 9000 / 9001). Confirm reachability before chasing MDX errors.
- **`#Missing` cells** — empty cube intersections return the literal `"#Missing"`. Helper preserves; cast as needed.
- **HTTP 200 with empty result** — Essbase returns 200 even when MDX matches nothing. Always check `df.count() == 0` rather than relying on the HTTP status.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/essbase.py](../../scripts/oracle_ai_data_platform_connectors/rest/essbase.py)
- Essbase 21c REST docs: https://docs.oracle.com/en/database/other-databases/essbase/21/essoa/
