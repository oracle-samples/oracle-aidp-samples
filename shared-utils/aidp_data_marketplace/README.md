# AIDP Marketplace UI

A small Flask + HTML/JS app that drives an Oracle AI Data Platform (AIDP)
instance through a two-step data-product workflow:

1. **Publish to Marketplace** — create an external table directly in
   `marketplace.default` over a directory (object-storage) location, then
   preview 5 sample rows.
2. **Grant access to users** — grant `SELECT` on the published object to one or
   more users, with an **access expiry date**. Recipients must be existing AIDP
   platform users (validated against OCI/IDCS identities); non-members get a
   clear error. Each grant is recorded in an Oracle ADW tracking table and the
   object's full access list (user / granted date / expiry date) is displayed.

Defaults target **AR_AIDP_V0** (`us-ashburn-1`). All instance details are
editable from the UI's **AIDP Instance** tab and persist to
[`config.yaml`](config.yaml). Requests are OCI-signed with the profile named in
`config.yaml` (`DEFAULT`) from `~/.oci/config`.

## Run

```bash
./run.sh                 # http://127.0.0.1:8090/
PORT=9000 ./run.sh       # custom port
```

`run.sh` uses the project venv at `../../venv` if present (it already has
`oci`, `requests`, `flask`, `pyyaml`), otherwise installs `requirements.txt`.

## Files

| File | Purpose |
|---|---|
| `app.py` | Flask backend + REST endpoints. |
| `aidp_engine.py` | Executes Spark SQL on AIDP via the job-submission engine. |
| `mkt_config.py` / `config.yaml` | Editable instance settings (load/save). |
| `db_access.py` / `db_config.yaml` | ADW access-validity tracking (`coder.aidp_marketplace_access`). |
| `static/index.html` | Single-page UI for the workflow. |

## How SQL actually runs (the engine)

AR_AIDP_V0 exposes **no synchronous "run SQL" REST endpoint**, and the Jupyter
kernel websocket is **not reachable with an OCI API key** (only
`/notebook/api/sessions` is proxied; `/notebook/api/kernels/.../channels`
returns `NotAuthorizedOrNotFound`). The only fully REST/api-key path that runs
Spark SQL is to **submit a notebook as a job and poll**. `aidp_engine.py` wraps
that into one synchronous call (verified live against AR_AIDP_V0):

1. Generate a notebook whose Spark code runs the statements and writes a JSON
   result file into the workspace (`/Workspace/marketplace_ui/_out/<token>.json`).
2. Upload it via the Workbench objects API (`POST /objects`).
3. Create a job (`POST /jobs`), trigger a run (`POST /jobRuns`), poll
   `GET /jobRuns/{key}` until `SUCCESS`/`FAILED` (~16–25 s).
4. Download + parse the result file via the objects API.
5. Best-effort cleanup of the generated job, notebook and result file.

Stdout/`print` from the job is **not** retrievable via `searchLogs` (those
return only framework logs), which is why results are returned through the
workspace result file.

## REST endpoints

| Method/Path | Action |
|---|---|
| `GET /api/config` / `POST /api/config` | Read / save instance settings. |
| `POST /api/test-connection` | Read-only connectivity + cluster check. |
| `POST /api/create-table` | `{table_name, location, format}` → publish external table into `marketplace.default` + 5-row preview. |
| `POST /api/share` | `{view_name, users, expiry_date}` → validate AIDP membership, `GRANT SELECT`, record validity in ADW, return tracked accesses. |
| `POST /api/permissions` | `{view_name}` → read the object's live catalog ACL. |
| `POST /api/access` | `{view_name}` → list tracked access records (user / granted / expiry / revoked) from ADW. |
| `POST /api/review` | `{scope: "all"}` or `{scope: "object", view_name}` → revoke SELECT for any grant whose access expired (expiry ≤ today, not already revoked) and stamp `PERMISSION_REVOKED_DATE`. |

## Important findings / limitations (AR_AIDP_V0)

- **External tables need an object-storage `oci://` location**, not a managed
  `/Volumes/...` path (the catalog rejects `/Volumes` for `CREATE EXTERNAL
  TABLE`). The location must not already be claimed by another table.
- **Schema is inferred**: `CREATE EXTERNAL TABLE ... USING <fmt> LOCATION` does
  not auto-discover columns via the catalog, so the engine reads the data files
  first and emits an explicit column list (so the preview has real columns). For
  CSV the DDL also carries `OPTIONS ('header'='true')` so the header line is not
  shown as a data row.
- **Membership check**: recipients are validated against OCI/IDCS users
  (`oci.identity.list_users`); names are domain-prefixed
  (`oracleidentitycloudservice/<email>`), so matching strips the domain prefix.
  Non-members are rejected with *"… is not part of the AIDP platform."*
- **Granting `SELECT` uses the AIDP REST API** — applied instantly (no Spark
  job; Spark SQL `GRANT` is disabled on the catalog). The grant is:
  `POST {platform}/tables/{tableKey}/actions/managePermission` with
  `tableKey = <catalog>.<schema>.<table>` and body
  `{"assignTablePermissionDetails": {"assignees": {"type": "USER", "targets": [<emails>]},
  "permissions": ["SELECT"], "includeColumns": ["*"], "excludeColumns": []}}`
  (HTTP 204 on success). The access list is read back from
  `GET {platform}/tables/{tableKey}/permissions`. Resolving the table key needs
  `GET /schemas?catalogKey=<cat>` and
  `GET /tables?catalogKey=<cat>&schemaKey=<cat.schema>`. To revoke, the same
  endpoint accepts `revokeTablePermissionDetails`. Equivalent endpoints exist
  for catalogs / schemas / views / volumes (`.../actions/managePermission`).

## Access-validity tracking (Oracle ADW)

Grants are recorded with a validity window in
`coder.aidp_marketplace_access` (columns: `OBJECT_NAME`, `GRANTEE`,
`PERMISSION_GRANTED_DATE`, `PERMISSION_EXPIRY_DATE`, `PERMISSION_REVOKED_DATE`)
on **ADW4IDL**.

**Review & revoke:** a standalone **Review & Revoke Access** section
(`/api/review`) that can be run any time, with a scope selector:
*All objects* or *Specific object*. It reads the tracking table (all rows, or
just the chosen object), and for every record whose `PERMISSION_EXPIRY_DATE` is
today or earlier and is not already revoked, it revokes the `SELECT` grant on
that object via the AIDP REST API (`revokeTablePermissionDetails` on
`.../tables/{key}/actions/managePermission`, by the user's OCID) and stamps
`PERMISSION_REVOKED_DATE = today`. The UI shows each access with Object / User /
dates and a status of Active / Expired / Revoked. (Re-granting a previously
revoked user clears the revoked date.)

ADW4IDL is a **private-endpoint** Autonomous Database — its host does not
resolve on the public internet, so it is unreachable from this machine. It *is*
reachable from the AIDP compute cluster (it is the same DB exposed as the
`ext_private_catalog_adw4idl` catalog). So `db_access.py` runs the table
work (create-if-absent, `MERGE` upsert, select) **as a cluster job** via the
engine, connecting with `python-oracledb` (pre-installed on the cluster, thin
mode) using the wallet uploaded into the workspace
(`marketplace_ui/adw_wallet/`). Connection details live in `db_config.yaml`
(and are mirrored into `.mcp.json` env so the agent can reuse them). The grant
itself is upserted per `(object, grantee)`, so re-granting updates the dates.

## Changing the target instance

Open the **AIDP Instance** tab and edit any field (instance name, region,
endpoint, AI Data Platform OCID, OCI profile, workspace, cluster, catalog,
schema, table format, job timeout) — they are pre-filled with the current
configuration — then click **Save** (or edit `config.yaml` directly). Use
**Test connection** to confirm the profile + cluster resolve before running
operations.
