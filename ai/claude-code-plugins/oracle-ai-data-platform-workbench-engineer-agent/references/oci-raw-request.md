# `oci raw-request` — the AIDP REST fallback pattern

> **Precedence:** the **preferred** control-plane engine is the official `aidp` CLI
> (see [`aidp-cli-map.md`](aidp-cli-map.md)). `oci raw-request` is the **fallback** — used when the `aidp`
> CLI isn't installed, or for operations the CLI v1.0.0 doesn't expose (full native Git, agent-flow
> authoring). Both hit the same `aidp.<region>` REST API with the same auth, so they are interchangeable.

This pattern (no bundled code) backs every skill when the CLI isn't used. The `oci` CLI signs the request
(RSA-SHA256 + `opc-security-token` for session profiles); skills never hand-roll the signing string.

## Base URL (verified from `ai-data-engineer-agent/src/aidp_agent/base_client.py`)

```
https://aidp.<region>.oci.oraclecloud.com/<API_VERSION>/<PATH_PREFIX>/<dataLakeOcid>/<resource…>
```

- **Region host (us-ashburn-1):** `https://aidp.us-ashburn-1.oci.oraclecloud.com`
  - Live DNS resolves via `overlay.us-ashburn-1.oci.oraclecloud.com`. **Do NOT** use the swagger's dev host
    `aidpdev2.us-phoenix-1.oci.oc-test.com` — it is a non-production artifact.
- **`<API_VERSION>`** — resolved per endpoint (see `rest-endpoint-map.md`):
  - **LIVE-VERIFIED 2026-06-09 (tenancy `oaseceal`/`idseylbmv0mm`, us-ashburn-1): this env serves
    `20240831`. GA `20260430` returns 404 here.** So default to **`20240831`**; treat `20260430` as the
    future GA target and only try it after a tenancy upgrade (probe, don't assume).
  - LA categories (agent flows, models catalog): **`20240831`**.
- **`<PATH_PREFIX>`** — **LIVE-VERIFIED `dataLakes`** in this env (the `aiDataPlatforms` prefix 404s here).
  Use `dataLakes`; only revisit if a future env says otherwise.
- **`<dataLakeOcid>`** — the AIDP DataLake OCID for the environment.

> **Official Oracle docs (provenance — reviewed 2026-06-11):** the path shape above is confirmed by Oracle's
> AIDP docs: [Use the APIs, SDK & CLI](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/use-apis-sdk-cli.html)
> → [REST APIs](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/rest-apis.html) ·
> [SDK & CLI](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/sdk-cli.html). The **GA** REST
> reference ([aiwap/rest-endpoints.html](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/rest-endpoints.html))
> lists endpoints as `/20260430/aiDataPlatforms/<id>/…` — **version-first, NO `/api/` segment**, prefix
> `aiDataPlatforms`. Same shape as ours, so an LA→GA move flips **both** `<API_VERSION>` 20240831→20260430
> **and** `<PATH_PREFIX>` dataLakes→aiDataPlatforms (they travel together — never pair `20260430` with
> `dataLakes`). Host `aidp.<region>.oci.oraclecloud.com` and the official CLI/SDK repo
> `oracle-samples/aidataplatform-sdk` (Python/TS/Java) are confirmed there too.

## Auth ladder (MANDATORY in every REST skill)

```
1. oci raw-request … --profile DEFAULT                       # api_key (preferred when it works)
2. on 401 / 403 / "NotAuthenticated" / "Security Token":
     oci session refresh --profile AIDP_SESSION
     retry:  oci raw-request … --auth security_token --profile AIDP_SESSION
3. if refresh fails:
     oci session authenticate --profile AIDP_SESSION --region us-ashburn-1
```

> Some AIDP tenancies reject IAD API keys outright (session-token only). Never trust a local `.env` for
> region/OCID/profile — resolve them explicitly (the active `.env` may point at a different region/tenancy).

## Session-token auth (no api_key) — full parity

Customers without an api_key use an `oci session authenticate` **session-token profile** (it carries a
`security_token_file`, not a fingerprint). **Every operation works with it** — control-plane AND interactive
Spark-SQL — with no api_key anywhere:
- **Control-plane (`oci raw-request` / `aidp` CLI):** add `--auth security_token --profile <SESSION>` to any
  call — identical calls to the api_key path, just a different signer. The `aidp` CLI takes `--auth security_token`.
- **Interactive Spark-SQL (`scripts/aidp_sql.py`):** pass the session profile as the **base** `--profile`
  (e.g. `--profile MYSESSION`). The helper auto-detects the `security_token_file`, signs REST with a
  `SecurityTokenSigner`, and **reuses the session token directly for the WebSocket — no UPST mint** (LIVE-VERIFIED
  2026-06-12: a session profile is detected, signed, and fails only on token *expiry*, never the old api_key
  `KeyError`). api_key profiles still mint a UPST automatically; `--session-profile` remains an explicit
  WS-only override.
- **Expiry (~1h):** session tokens expire; the helper prints an `oci session refresh --profile <p>` hint. When
  refresh fails, re-create with `oci session authenticate --profile <p> --region <r>` — keep the region
  consistent with the call region. Mirrors the reference SDK's `auth.py` / `notebook.py` security-token path.

## Invocation shapes

```bash
# GET (list)
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces" \
  --profile DEFAULT

# GET with session token (Preview category)
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/credentials" \
  --auth security_token --profile AIDP_SESSION

# POST with body
oci raw-request --http-method POST \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/roles/<ROLE>/actions/addMember" \
  --request-body '{"principals":["ocid1.user.oc1..xxxx"]}' \
  --request-headers '{"content-type":"application/json"}' \
  --auth security_token --profile AIDP_SESSION
```

Output is JSON: `{ "data": …, "headers": …, "status": <int> }`.

## Cross-cutting REST conventions

- **Async (202)** — long-running actions return `202` with an operation key (header
  `datalake-async-operation-key` or a body field). Poll the async-ops endpoint (or the MCP
  `get_async_operation_status` / `wait_for_async_operation_completion`) until terminal.
- **Pagination** — pass `limit` + `page`; follow the `opc-next-page` response header until absent.
- **Concurrency** — supply `if-match: <etag>` on `PUT`/`DELETE` when the resource returns an etag.
- **Idempotency** — set `opc-retry-token` on retried `POST`s.
- **Errors** — `400` bad request · `401` not authenticated (→ auth ladder) · `403` not authorized ·
  `404` wrong version/prefix (→ try the other) · `409` conflict · `412` etag mismatch · `429` throttle
  (back off) · `5xx` server (retry with `opc-retry-token`).

## Listing reliability (don't manufacture a false "empty") — LIVE-LESSONS 2026-06-12

- **Status-check before "empty".** Treat a list as empty only after confirming the response is **2xx with a
  parsed JSON body**. A non-2xx (auth/network/CLI-flag error) or non-JSON output rendered as `len(items)==0`
  is a silent false-negative — it once reported a 100+-job workspace as "0 jobs". When parsing, branch on the
  `status` field; never `json.loads(out[out.find('{'):]) or {}` and print the count.
- **Paginate.** List endpoints cap a page (e.g. jobs return 100); pass `limit` + follow the `opc-next-page`
  header until absent before concluding a total.
- **Shell quoting (zsh).** Pass multi-flag args as an **array** — `G=(--instance-id "$OCID" --auth api_key
  --profile DEFAULT --region "$R"); aidp <cmd> "${G[@]}"` — not an unquoted variable. zsh does **not**
  word-split unquoted vars, so `$G` becomes one bad argument (`Unknown option '--instance-id …'`).
- **Backgrounded commands are network-sandboxed.** Multi-call loops can get auto-backgrounded and then fail
  every request with `ConnectionError`. Issue network calls as **single foreground** calls (or several single
  calls in parallel), not one long shell loop.
- **Two path roots.** Workspace objects use `/Workspace/<folder>/...` (`aidp workspace-object list --path
  /Workspace/Shared/x`); the Jupyter contents API uses a **relative** path (`aidp notebook get-content <ws>
  Shared/x.ipynb`). Using the wrong root lists empty.

## No-fabrication gate

A REST skill must NOT present an endpoint/version/prefix as confirmed until it has returned a **live 2xx**
(or a documented 4xx for a deliberately-invalid request) and the result is recorded in
`rest-endpoint-map.md`. Until then, the skill states the call is **UNVERIFIED (Preview/LA)** and asks the
user to confirm before any destructive action.
