# OAC Workbook Authoring — End-to-End (generate → bind → save → open)

> **What this documents:** how to take a natural-language intent + a gold-mart schema and
> produce a **live OAC dashboard** — using the vendored `skills/workbook-authoring` skill for
> generation and OAC's project REST API for delivery. Proven end-to-end on a live OAC instance
> on 2026-06-13.
>
> **Redaction:** the live OAC host, dataset UUID, admin user, and session tokens are replaced
> with `<placeholders>` below per the repo's redaction rule. Substitute your own.

For the complete operator workflow, start with [../workflow.md](../workflow.md).
For MCP setup, including Claude Code non-interactive auth, see
[oac_mcp_setup.md](oac_mcp_setup.md).

---

## TL;DR — the chain that works

```
intent + gold schema
   │  (1) skills/workbook-authoring  →  regenerate-workbook.mjs        [pure local, no OAC]
   ▼
workbook JSON  (criteria / datasources / layouts / views / reportConfig …)
   │  (2) rebind datasources+expressions to the real dataset XSA token
   ▼
bound workbook JSON
   │  (3) POST /dv/ui/api/v1/projects/<path>   { json, name, type:"JSON" }
   ▼
live workbook in OAC  →  open via /dv/ui/project.jsp?...&reportpath=<path>
```

Generation (step 1) needs **no OAC at all**. Only delivery (step 3) touches OAC.

---

## Key findings

1. **The skill's generation engine is a pure local tool.** `regenerate-workbook.mjs` takes a
   `request.json` and emits a runtime-valid OAC workbook JSON with **no MCP / no network**. Dataset
   columns are *embedded in the request*; live `describe_data` is only how the agent *learns* them.

2. **The stored project format == the skill's output format.** A `GET` of a real workbook returns
   exactly the keys the skill emits: `criteria, datasources, layouts, views, reportConfig,
   projectVersion, parameters`. So the skill's output is directly saveable.

3. **OAC project REST API (private, `/dv/ui/api/v1/projects/...`)** — discovered from the
   `bi-tech` functional test `ProjectAPITest` / `ApiTestCase.testSaveProject`:
   | Op | Call |
   |----|------|
   | **Save** | `POST /dv/ui/api/v1/projects/<path>` body `{ "json": "<project JSON string>", "name": "<name>", "type": "JSON" }` → `{ "success": true }` |
   | **Read** | `GET /dv/ui/api/v1/projects/<path>` → bare project JSON |
   | **Delete** | `DELETE /dv/ui/api/v1/projects/<path>` |
   | **Make folder** | `PUT /dv/ui/api/v1/items/<folder>` body `{ "type": "folder" }` |
   - The browser UI uses `v2/projects/json` (read) + `v2/projects/blobs` (save); the **v1**
     endpoints above are simpler and test-backed, and accept the **bare** project JSON.

4. **Dataset binding uses an XSA token.** A workbook references its dataset as
   `XSA('<dataset-uuid>'.'<dataset-name>')` in both `datasources[].subjectArea` and every column
   `expression` (`XSA('<dataset-uuid>'.'<name>')."<column>"`). The skill, given that token as the
   `subjectArea`/expressions, composes correct bindings. The `<dataset-uuid>` is what
   `describe_data` (or an existing workbook on that dataset) provides.

5. **Auth: two systems, pick by environment.**
   - **Cookie session** (`JSESSIONID` + `ORA_BI_SESSTOK` + `x-csrf-token`) — what the UI uses;
     works against the project REST API. Short-lived; good for dev/eng pods.
   - **Bearer token** — what the MCP connector + public REST require. On **release-mode OAC** this
     is the product path (`dashboard mcp-token` + MCP `save_catalog_content`).

6. **Delivery-path scorecard** (which ways get a workbook into OAC):
   | Path | Mechanism | Verdict |
   |------|-----------|---------|
   | MCP `save_catalog_content` | Bearer | ✅ **product path** on release-mode OAC; ⛔ blocked on NotReleaseMode eng pods (dummy tokens) |
   | `.dva` hand-package | UI import | ❌ dead — `.dva` content (`*.arc`) is **AES-encrypted** |
   | **Project REST `v1/projects`** | cookie session | ✅ **works on eng pods**; the dev/test delivery used here |

---

## What's required (prerequisites)

| # | Requirement | Notes |
|---|-------------|-------|
| 1 | **Node.js** | skill tools are self-contained (Node built-ins only, **no `npm install`**) |
| 2 | **`skills/workbook-authoring`** | vendored in this repo; auto-discovered |
| 3 | **A dataset already in OAC** | the workbook *binds* to it — OAC has **no dataset-create REST/MCP**, so create it once in the UI on your AIDP JDBC connection |
| 4 | **The dataset's XSA reference** | `XSA('<uuid>'.'<name>')` — from `describe_data`, an existing workbook's `datasources`, or the catalog |
| 5 | **Auth to OAC** | release-mode → Bearer (MCP/product path); dev/eng pod → cookie session (`JSESSIONID`+`ORA_BI_SESSTOK`+`x-csrf-token`) |
| 6 | **Network reachability** | private-VCN pods require the corporate VPN |

---

## Step-by-step

### Step 1 — Generate the workbook JSON (offline, no OAC)
Author your `request.json` under `workbooks/<name>/` **beside `bundle.yaml`**
(the convention — mirrors `overlays/<name>/` / `profiles/`; raw output is
gitignored, only `*.redacted.json` is committable), then run the skill:
```bash
node skills/workbook-authoring/tools/regenerate-workbook.mjs \
  --request workbooks/<name>/request.json --target-version 26.07 \
  --output workbooks/<name>/workbook.json
# expect: status: ok, validationStatus.valid: true, compositionCoverage: ootb_composed
```
**request.json essentials** (see `tests/live/workbook_authoring_T1_offline/` for a full example):
- top level: `capabilities {discoveryMethod, saveAvailable, exportAvailable}` + `adapterPayload {discovery, describe, profiling}` + an **approved** `analysisRequirements`.
- view `bindings.<role>` must be a **direct expression string** (e.g. `XSA('<uuid>'.'supplier_spend')."supplier_name"`) — **not** a `{columnID}` object.
- `profiling.filterDecisionTrace` needs: `required, contractVersion, selectedFilterMode, fallbackUsed, fallbackReason, queryIntents, probeResults, derivedDecisions`.

### Step 2 — Get the dataset's XSA reference
Create the dataset once in OAC on your AIDP JDBC connection, then obtain its token:
- from an existing workbook: `GET /dv/ui/api/v1/projects/<that-workbook>` → read `datasources[].subjectArea`;
- it looks like `XSA('<dataset-uuid>'.'<dataset-name>')`.

Dataset creation is manual today because OAC exposes no dataset-create REST/MCP
tool. The advisor should provide the table, column, and join spec; the OAC UI
is where the governed dataset is created.

### Step 3 — Bind the generated workbook to the real dataset
In the `request.json`, set `adapterPayload.discovery.selectedDataModel`, every
`describe.columns[].expression`, and the view `bindings` to use the **XSA token** (not a
placeholder), then re-run step 1. Verify the dataset id is present in the output:
```bash
grep -c "<dataset-uuid>" workbook.json   # > 0
```

### Step 4 — Save into OAC
**Product path (release-mode OAC):** the skill saves via MCP `save_catalog_content` and returns a
`viewUrl` — fully automated; set `capabilities.saveAvailable=true`.

**Dev/eng-pod path (cookie session):**
```bash
# body = { "json": <stringified bare workbook>, "name": "<name>", "type": "JSON" }
python3 -c "import json;p=json.load(open('workbook.json'));\
json.dump({'json':json.dumps(p),'name':'<name>','type':'JSON'},open('save_body.json','w'))"

curl -s -X POST \
  -H "Cookie: JSESSIONID=<...>; ORA_BI_SESSTOK=<...>; ORA_BI_SESSPARAM=<...>" \
  -H "x-csrf-token: <csrf>" -H "x-requested-with: XMLHttpRequest" \
  -H "Content-Type: application/json; charset=UTF-8" \
  --data @save_body.json \
  "http://<oac-host>/dv/ui/api/v1/projects/users/<oac-user>/<name>"
# expect: {"success":true, ...}
```

### Step 5 — Verify & open
```bash
# read back
curl -s -H "Cookie: <...>" -H "x-csrf-token: <csrf>" -H "x-requested-with: XMLHttpRequest" \
  "http://<oac-host>/dv/ui/api/v1/projects/users/<oac-user>/<name>"
# open in browser (on VPN)
http://<oac-host>/dv/ui/project.jsp?pageid=visualAnalyzer&reportmode=full&reportpath=%2F%40Catalog%2Fusers%2F<oac-user>%2F<name>&viewermode=false
```

---

## Proven run (2026-06-13)
- **Generated** `gold.supplier_spend` bar workbook offline (`valid:true`, `oracle.bi.tech.chart.bar`).
- **Bound** to the live dataset `XSA('<dataset-uuid>'.'supplier_spend')`.
- **Saved** via `POST /dv/ui/api/v1/projects/users/<oac-user>/aidp_supplier_spend_gen` → `{"success":true}` (HTTP 200).
- **Read back** HTTP 200 (7831 bytes), datasources bound to the XSA token, 2 views.
- Evidence: `tests/live/workbook_authoring_T1_offline_results.md`, `tests/live/workbook_authoring_e2e/`.

---

## Caveats / limitations
- **Cookie session is short-lived and dev-only.** Not a production mechanism — for shippable use,
  the **MCP/Bearer path on a release-mode OAC** is the supported automation.
- **The dataset must pre-exist.** OAC exposes no dataset-create REST/MCP; create it in the UI once.
- **`v1/projects` is a private/internal API** (not Oracle's public documented REST surface) —
  fine for tooling, but no stability guarantee across releases.
- **`.dva` import is not a packaging option** — its content is AES-encrypted; you cannot inject
  generated JSON into one.
- **NotReleaseMode `oaceng*` pods cannot issue working Bearer tokens** — so MCP delivery is only
  testable on a real release-mode OAC. (Background: `project_oac_phoenix_pod_dummy_tokens`.)
