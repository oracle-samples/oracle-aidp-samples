---
name: aidp-notebooks
description: Create, edit, and execute AIDP notebooks and manage their kernel sessions. Use when the user wants to author a notebook, run cells/code on a cluster, attach/detach a kernel session, or build an interactive notebook (e.g. with widgets + Spark SQL + Plotly). Notebook FILE ops run via the Notebook contents REST API; CELL EXECUTION runs via scripts/aidp_sql.py.
---

# `aidp-notebooks` — notebook authoring & kernel execution

Author AIDP notebooks via the Notebook contents REST API, and run code on a Spark cluster via the
bundled SQL/cell helper. No MCP server is required — the plugin is self-contained.

## When to use
- "Create a notebook", "run this on the cluster", "execute these cells", "build an interactive notebook".

> **Connecting to an external / non-lakehouse source in the notebook** (Fusion, EPM, Essbase, Oracle
> ADB/ExaCS, Snowflake, S3, Kafka, …)? **Do NOT hand-roll the connection here.** Use the
> **`oracle-ai-data-platform-workbench-spark-connectors`** plugin's `aidp-<source>` skill for the connection
> recipe (e.g. `aidp-fusion-rest`/`aidp-fusion-bicc`, `aidp-oracle-db`, `aidp-snowflake`, `aidp-object-storage`).
> **Check it's installed** (`claude plugin list`); if not, tell the user to install it. Its
> `oracle_ai_data_platform_connectors` helper package is installed once via that plugin's own
> **`aidp-connectors-bootstrap`** skill (it pushes the package to `/Workspace/Shared` via the AIDP MCP +
> runs a sanity import; if the MCP can't reach your instance, upload it manually). This skill then just
> authors + runs the notebook; for multi-source joins see `aidp-federate`.

## Two engines
- **Notebook FILE + SESSION ops** (create / read / rename / save the `.ipynb`, manage kernel sessions) →
  the official **`aidp notebook …`** CLI (preferred); `oci raw-request` against the AIDP **Notebook
  contents API** is the fallback.
- **CELL EXECUTION** (run python/Spark, persist kernel state) → the bundled `scripts/aidp_sql.py`,
  which creates the kernel session and runs the cell over the WebSocket for you. **Cell execution is NOT
  a CLI command** — the official `aidp` CLI/SDK Notebook group is files + sessions only and cannot exec
  cells (running a notebook end-to-end is job-based). Interactive Spark-SQL stays on `scripts/aidp_sql.py`.

> **Live-verified 2026-06-10 on de-agent:** the full WebSocket path via `scripts/aidp_sql.py` — auto-create
> notebook, attach kernel, execute cells, and list/delete at `/Workspace/Shared` — is proven working end-to-end.
> The bare HTTP `…/notebook/api/contents/<path>` contents-CRUD path, by contrast, 500/404s for `api_key` raw-request
> on `20240831` instances, so **prefer the WebSocket helper over the HTTP contents path for notebook file ops** (see
> `aidp-workspace-files`).

## Notebook FILE + SESSION ops
**CLI (preferred):** `aidp notebook <command> …` (Oracle-supported, versioned — see
[references/aidp-cli-map.md](../../references/aidp-cli-map.md)).
- Files: `aidp notebook create-content | get-content | update-content | modify-content |
  delete-content | export-contents`.
- Sessions: `aidp notebook create-session | get-session | list-sessions | patch-session |
  delete-session`.

**Fallback (`oci raw-request` — Notebook contents API)** when the CLI isn't installed or doesn't expose
the op. Base (see [references/oci-raw-request.md](../../references/oci-raw-request.md) for host/version/auth):
`…/20240831/dataLakes/<OCID>/workspaces/<WS>/notebook/api/contents/<url-encoded-path>`

- **Create / save** an `.ipynb` — `PUT …/contents/<enc-path>` with body
  `{"type":"notebook","format":"json","path":"<path>","content":{"cells":[…],"metadata":{},"nbformat":4,"nbformat_minor":5}}`
  (an empty notebook uses `"cells":[]`).
- **Read** — `GET …/contents/<enc-path>` (add `?content=0` to fetch metadata only / probe existence).
- **Rename / move** — `PATCH …/contents/<enc-path>` with body `{"path":"<new-path>"}`.
- **Delete** — `DELETE …/contents/<enc-path>`.

URL-encode the notebook path (e.g. `Shared/my_nb.ipynb` → `Shared%2Fmy_nb.ipynb`). Use the auth ladder
in [references/oci-raw-request.md](../../references/oci-raw-request.md) (`--profile DEFAULT` api_key first).

For mutating ops (create/update/modify/delete content, patch/delete session), persist the request body to
`.aidp/payloads/` and confirm first — see [references/payloads.md](../../references/payloads.md).

## CELL EXECUTION (the core) — scripts/aidp_sql.py
`aidp_sql.py` is the plugin's one bundled helper. It mints a UPST from the api_key DEFAULT profile,
auto-creates a scratch notebook if needed, opens the kernel session, runs the cell over the WebSocket,
and returns JSON. **No `AIDP_SESSION` required** (`--session-profile` is optional).

```bash
python "$PLUGIN_DIR/scripts/aidp_sql.py" \
  --region us-ashburn-1 --datalake <OCID> --workspace <WS> --cluster <cluster-key> \
  --code "df = spark.sql('SELECT 1'); df.show()"
# optional: --notebook "Shared/_aidp_sql_scratch.ipynb" --profile DEFAULT \
#           --session-profile AIDP_SESSION --timeout 180
```

Returns JSON: `{"status":"ok|error","execution_count":N,"outputs":[…],"spark_job_ids":[…],"error":{…}}`.

1. Ensure the target cluster is RUNNING first (cluster start/status via the cluster skill / REST —
   see [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md)).
2. Run cells with the helper. Kernel state (DataFrames, imports) persists within a single helper run.
   - Spark SQL: `--code "spark.sql('…').show()"`.
   - Interactive UIs (ipywidgets), Plotly charts, OCI SDK calls, custom auth/token refresh — all run here.
3. Persist results into the `.ipynb` by writing the cells back via the contents `PUT` above.
4. Smoke test: a `SELECT 1` cell (`--code "spark.sql('SELECT 1').show()"`).

## Auth note
- FILE ops follow the REST auth ladder (api_key `--profile DEFAULT`; on 401/403 fall back to
  `AIDP_SESSION` — see [references/oci-raw-request.md](../../references/oci-raw-request.md)).
- The kernel runs over WebSocket and needs a security token. `aidp_sql.py` mints a short-lived UPST from
  the api_key DEFAULT profile automatically; pass `--session-profile AIDP_SESSION` only if you want to
  use an existing session token instead.

## Composing notebooks (`%run`, `oidlUtils`, terminal) — runs inside a cell via the helper
These are **cell** constructs — run them through `scripts/aidp_sql.py --code "…"`, not the CLI.
- **`%run`** — inline another notebook's code: `%run /Workspace/folder/called.ipynb`. Runs immediately with
  the **caller's user principal + attached cluster**; the callee's functions/variables become available in
  the caller.
- **`oidlUtils.notebook.run/exit`** — value passing between notebooks:
  ```python
  result = oidlUtils.notebook.run("NotebookB", timeout_seconds=0, parameters={"key": "value"})  # caller
  oidlUtils.notebook.exit(json.dumps(payload))                                                  # callee returns
  ```
- **Job task output** — in a notebook *task*, `oidlUtils.notebook.exit(json.dumps(payload))` sets the task
  output; downstream tasks read it via `{{tasks.[name].…}}` system params (`aidp-pipelines`) or the
  `jobs/runs/get-output` API (`response["notebook_output"]["result"]`).
- **Terminal / shell** — prefix `!` (e.g. `!pip install …`, `!unzip …`) or use the `subprocess` module.
  Notebook-scoped `!pip install` works only in `.ipynb` and applies to that notebook + its job tasks.

## AIDP notebook gotchas (authoring + execution rules)
- **NEVER call `spark.stop()` in an AIDP notebook.** AIDP manages the kernel's Spark lifecycle; `spark.stop()`
  kills the context, breaks every later cell, and forces a session restart. Omit it everywhere.
- **Markdown cell rendering quirks** (the AIDP UI renderer):
  - **No spaces inside parentheses** — the renderer URL-encodes them to `%20` (e.g. `(85 groupBy + 33 slots)`
    renders as `85%20groupBy…`). Rephrase with dashes — `-- 85 groupBy + 33 slots --`. Empty parens like
    `processNext()` are fine.
  - **Stick to ASCII** — use `--` not an em-dash, `-->` not `→`; avoid non-ASCII characters in markdown cells.
- **Spark OUTPUT paths** (`fs.defaultFS` is `compute:///`, FUSE-backed):
  - **Never `Path.resolve()` / write to the driver's local FS** for distributed output — executors can't reach it.
  - **`compute:///` reports size 0** (`getContentSummary().getLength()` → 0); to *measure* output size, write/read
    via `oci://<bucket>@<namespace>/…` instead. Workspace-relative paths work for read/write but not sizing.
  - **Never derive paths from `spark.sql.warehouse.dir`** — it points at AIDP-managed internal storage.
  - Credentials/scratch belong under `/tmp/`, not `/Workspace/` (FUSE, intermittent + `chmod` no-op); streaming
    checkpoints under `/Volumes/<catalog>/<schema>/<volume>/…`.
- **Session debugging** (two surfaces — debug independently): if REST session create/list works but cell
  execution fails → it's WebSocket auth or a stale kernel, **not** the REST path. List sessions, confirm the
  attached cluster, check kernel `idle`/`busy`, and isolate signer-path validation from end-to-end exec. A stale
  `busy` session may just need a restart (don't assume the auth fix failed). For deep perf tuning see
  `aidp-spark-optimization`; for failure triage see `aidp-spark-debugging`.

## Structured Streaming (code) — runs inside a cell via the helper (platform-ref §21)
Spark Structured Streaming runs as **cell** code via `scripts/aidp_sql.py --code "…"` (cluster RUNNING).
Canonical Delta pattern (platform-ref §21, lines 937–946):
```python
# Read stream — table source/sink is Delta format only (3-part name)
streaming_df = spark.readStream.format("delta").table("catalog.schema.deltatable")
# Write stream
streaming_df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/checkpoints1/") \
    .toTable("catalog.schema.deltatable")
```
- **Checkpoint must live on a Volume** (`/Volume/…`, recommended) or workspace path; OCI Object Storage
  (`oci://`) is **not supported** as a checkpoint location (platform-ref §21, lines 930–933, 927).
- Supported sources/sinks: Volume / Workspace paths (all formats), 3-part **tables (Delta only)**, Kafka,
  OCI Streaming Service. `oci://` and Oracle ALH/ATP/AI DB are **not** supported for streaming
  (platform-ref §21, lines 918–928).
- For a **continuous/scheduled** stream, run it as a job *Streaming task* (Max Concurrent Runs = 1, no
  timeout/dependencies, runs until stopped) — configure via `aidp-pipelines` (platform-ref §21, lines 948–954).

## Reliability rules
- For large outputs, bound with `.show(n)` / `LIMIT` to protect context.
- Markdown/widget rendering quirks: prefer simple cell content; verify a cell ran (`status:ok`) before
  assuming output.
- Idempotent create: `GET …/contents/<path>?content=0` first; only `PUT` an empty notebook if it 404s.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) — skill → official `aidp` CLI command map (primary engine)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) — REST host/version/auth + invocation shapes (fallback)
- [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) — control-plane endpoints (clusters, jobs)
- [references/payloads.md](../../references/payloads.md) — persist + confirm request bodies for mutating ops
- `scripts/aidp_sql.py` — the bundled cell/SQL executor
- Pairs with `aidp-analyzing-data`, `aidp-ai-sql`, `aidp-pipelines`
