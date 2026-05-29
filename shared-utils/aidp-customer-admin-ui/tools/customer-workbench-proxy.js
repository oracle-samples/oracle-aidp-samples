#!/usr/bin/env node
"use strict";

const fs = require("fs");
const http = require("http");
const path = require("path");
const { randomUUID } = require("crypto");
const { execFile } = require("child_process");
const { URL } = require("url");

const RAW_SCHEMA_VERSION = "aidp.customer-workbench.raw.v1";
const DEFAULT_AIDP_WORKBENCH_API_VERSION = "/20260430";
const DEFAULT_OCI_AUTH = "security_token";
const DEFAULT_OCI_COMMAND = "oci";
const DEFAULT_OCI_PROFILE = "DEFAULT";
const DEFAULT_PORT = 8083;
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_STATIC_PAGE = "index.html";
const DEFAULT_UPSTREAM_TIMEOUT_MS = 30_000;
const CUSTOMER_WORKBENCH_API_PATH = "/api/aidp-customer/workbench";
const OCI_SESSION_STATUS_API_PATH = "/api/oci-session/status";
const REPO_ROOT = path.resolve(__dirname, "..");
const APP_DIR = path.join(REPO_ROOT, "app");
const OCI_SHORT_REGION_ENDPOINTS = {
  iad: "us-ashburn-1",
  phx: "us-phoenix-1",
  r2: "us-phoenix-1"
};

if (require.main === module) {
  main(process.argv.slice(2));
}

module.exports = {
  createServer,
  fetchAidpCustomerWorkbenchRaw,
  ociSessionStatus
};

function main(args) {
  const options = parseOptions(args);
  const port = parsePort(options.port || process.env.PORT || DEFAULT_PORT);
  const host = parseHost(options.host || process.env.HOST || process.env.AIDP_PROXY_HOST || DEFAULT_HOST);
  const mode = options.mode || process.env.AIDP_PROXY_MODE || "fixture";
  const server = createServer({
    mode,
    aidpWorkbenchApiBaseUrl: options["aidp-workbench-api-base-url"] || process.env.AIDP_WORKBENCH_API_BASE_URL,
    aidpWorkbenchApiVersion: options["aidp-workbench-api-version"] ||
      process.env.AIDP_WORKBENCH_API_VERSION ||
      DEFAULT_AIDP_WORKBENCH_API_VERSION,
    serviceApiAuthHeader: options["service-api-auth-header"] || process.env.AIDP_SERVICE_API_AUTH_HEADER,
    serviceApiBearerToken: options["service-api-bearer-token"] || process.env.AIDP_SERVICE_API_BEARER_TOKEN,
    ociCommand: options["oci-command"] || process.env.AIDP_OCI_COMMAND || DEFAULT_OCI_COMMAND,
    ociProfile: options["oci-profile"] || process.env.AIDP_OCI_PROFILE || process.env.OCI_CLI_PROFILE || DEFAULT_OCI_PROFILE,
    ociAuth: options["oci-auth"] || process.env.AIDP_OCI_AUTH || DEFAULT_OCI_AUTH,
    upstreamTimeoutMs: options["upstream-timeout-ms"] || process.env.AIDP_UPSTREAM_TIMEOUT_MS,
    defaultStaticPage: options["default-page"] || process.env.AIDP_DEFAULT_PAGE || DEFAULT_STATIC_PAGE
  });

  server.on("error", (error) => {
    if (error.code === "EADDRINUSE") {
      console.error(`Port ${port} is already in use. Stop the existing proxy or choose another port with --port.`);
      process.exitCode = 1;
      return;
    }
    console.error(error.message || error);
    process.exitCode = 1;
  });

  server.listen(port, host, () => {
    const displayHost = host === "0.0.0.0" || host === "::" ? "localhost" : host;
    const baseUrl = `http://${displayHost}:${port}`;
    console.log(`AIDP customer Workbench proxy listening on ${baseUrl}/`);
    console.log(`Open ${baseUrl}/ for fixture mode or ${baseUrl}/?liveApi=1 for live REST lookup.`);
    console.log(`Proxy mode: ${normalizeProxyMode(mode)}`);
  });
}

function createServer({
  appDir = APP_DIR,
  mode = "fixture",
  aidpWorkbenchApiBaseUrl,
  aidpWorkbenchApiVersion = DEFAULT_AIDP_WORKBENCH_API_VERSION,
  serviceApiAuthHeader,
  serviceApiBearerToken,
  fetchImpl = globalThis.fetch,
  execFileImpl = execFile,
  ociCommand = DEFAULT_OCI_COMMAND,
  ociProfile = DEFAULT_OCI_PROFILE,
  ociAuth = DEFAULT_OCI_AUTH,
  upstreamTimeoutMs = DEFAULT_UPSTREAM_TIMEOUT_MS,
  defaultStaticPage = DEFAULT_STATIC_PAGE
} = {}) {
  const proxyMode = normalizeProxyMode(mode);
  const config = {
    aidpWorkbenchBaseUrl: aidpWorkbenchApiBaseUrl,
    aidpWorkbenchVersion: aidpWorkbenchApiVersion,
    aidpAuthHeader: serviceApiAuthHeader || bearerAuthHeader(serviceApiBearerToken),
    fetchImpl,
    execFileImpl,
    ociCommand,
    ociProfile,
    ociAuth,
    timeoutMs: parseTimeoutMs(upstreamTimeoutMs)
  };

  return http.createServer(async (request, response) => {
    try {
      const requestUrl = new URL(request.url, "http://localhost");

      if (request.method !== "GET") {
        sendJson(response, 405, errorBody("METHOD_NOT_ALLOWED", "Only read-only GET requests are supported."));
        return;
      }

      if (requestUrl.pathname === OCI_SESSION_STATUS_API_PATH) {
        await handleGetOciSessionStatus(requestUrl, response, { proxyMode, config });
        return;
      }

      if (requestUrl.pathname === CUSTOMER_WORKBENCH_API_PATH) {
        await handleGetCustomerWorkbench(requestUrl, response, { proxyMode, config });
        return;
      }

      serveStaticFile(requestUrl.pathname, response, appDir, defaultStaticPage);
    } catch (error) {
      if (!response.headersSent && !response.writableEnded) {
        sendJson(response, 500, errorBody(
          "INTERNAL_SERVER_ERROR",
          "Customer Workbench proxy request failed.",
          false
        ));
        return;
      }
      response.destroy(error);
    }
  });
}

async function handleGetOciSessionStatus(requestUrl, response, { proxyMode, config }) {
  const requestedRegion = String(requestUrl.searchParams.get("region") || "").trim();
  const status = await ociSessionStatus(config, requestedRegion, { proxyMode });
  sendJson(response, status.signedIn === false ? 401 : 200, status);
}

async function handleGetCustomerWorkbench(requestUrl, response, { proxyMode, config }) {
  const aiDataPlatformId = firstSearchParam(requestUrl.searchParams, [
    "aiDataPlatformId",
    "aidpOcid",
    "aidpId"
  ]);
  const requestedRegion = String(requestUrl.searchParams.get("region") || "").trim();

  if (!aiDataPlatformId || !requestedRegion) {
    sendJson(response, 400, errorBody(
      "REQUEST_INCOMPLETE",
      "AIDP OCID and region are required for customer workbench lookup."
    ));
    return;
  }

  if (proxyMode === "service-api") {
    await handleCustomerWorkbenchWithFetcher(response, config, {
      aiDataPlatformId,
      requestedRegion
    }, fetchServiceApiAidpWorkbenchGet);
    return;
  }

  if (proxyMode === "oci-raw-request") {
    const sessionStatus = await ociSessionStatus(config, requestedRegion, { proxyMode });
    if (!sessionStatus.signedIn) {
      sendJson(response, 401, {
        ...errorBody(
          "OCI_SESSION_REQUIRED",
          sessionStatus.message || "OCI CLI session authentication is required.",
          false
        ),
        session: sessionStatus
      });
      return;
    }
    await handleCustomerWorkbenchWithFetcher(response, config, {
      aiDataPlatformId,
      requestedRegion
    }, fetchOciRawRequestAidpWorkbenchGet);
    return;
  }

  sendJson(response, 404, errorBody(
    "LIVE_MODE_REQUIRED",
    "Customer workbench REST lookup is available only in service-api or oci-raw-request proxy mode.",
    false
  ));
}

async function handleCustomerWorkbenchWithFetcher(response, config, request, fetcher) {
  try {
    const snapshot = await fetchAidpCustomerWorkbenchRaw(config, request, fetcher);
    sendJson(response, 200, snapshot);
  } catch (error) {
    sendJson(response, error.statusCode || 502, errorBody(
      error.code || "UPSTREAM_UNAVAILABLE",
      error.message || "AIDP Workbench REST lookup failed.",
      error.retryable !== false
    ));
  }
}

async function fetchAidpCustomerWorkbenchRaw(config, request, fetcher) {
  const warnings = [];
  const workspacesPayload = await fetcher(
    config,
    request,
    aidpWorkbenchPath(request, "/workspaces")
  );
  const workspaceItems = collectionItems(workspacesPayload);
  const workspaces = await Promise.all(workspaceItems.map((workspace) =>
    fetchAidpCustomerWorkspaceEntry(config, request, fetcher, workspace, warnings)
  ));

  return {
    schemaVersion: RAW_SCHEMA_VERSION,
    aiDataPlatformId: request.aiDataPlatformId,
    region: request.requestedRegion,
    fetchedAt: new Date().toISOString(),
    source: {
      adapter: "aidp-workbench-rest-proxy",
      freshness: "Live AIDP Workbench REST API",
      partial: warnings.length > 0,
      warnings: uniqueStrings(warnings)
    },
    workspaces
  };
}

async function fetchAidpCustomerWorkspaceEntry(config, request, fetcher, workspace, warnings) {
  const workspaceKey = workbenchKey(workspace, ["workspaceKey", "key", "id"]);
  if (!workspaceKey) {
    warnings.push("A workspace was skipped because the Workbench REST response did not include a workspace key.");
    return { workspace, clusters: [], sessions: [], jobs: [], recentRuns: [], clusterLibraries: {} };
  }

  const [clustersPayload, sessionsPayload, jobsPayload] = await Promise.all([
    fetchOptionalCustomerCollection(
      config,
      request,
      fetcher,
      aidpWorkspacePath(request, workspaceKey, "/clusters"),
      `clusters for workspace ${workspaceKey}`,
      warnings
    ),
    fetchOptionalCustomerCollection(
      config,
      request,
      fetcher,
      aidpWorkspacePath(request, workspaceKey, "/notebook/api/sessions"),
      `notebook sessions for workspace ${workspaceKey}`,
      warnings
    ),
    fetchOptionalCustomerCollection(
      config,
      request,
      fetcher,
      aidpWorkspacePath(request, workspaceKey, "/jobs"),
      `workflows for workspace ${workspaceKey}`,
      warnings
    )
  ]);
  const jobs = collectionItems(jobsPayload);
  const recentRuns = await fetchAidpCustomerRecentJobRuns(
    config,
    request,
    fetcher,
    workspaceKey,
    jobs,
    warnings
  );

  const clusterLibraries = {};
  const clusters = await Promise.all(collectionItems(clustersPayload).map(async (cluster) => {
    const clusterKey = workbenchKey(cluster, ["clusterKey", "key", "id"]);
    if (!clusterKey) {
      warnings.push(`A cluster in workspace ${workspaceKey} was missing a cluster key.`);
      return cluster;
    }

    const [clusterDetailsPayload, librariesPayload] = await Promise.all([
      fetchOptionalCustomerCollection(
        config,
        request,
        fetcher,
        aidpClusterPath(request, workspaceKey, clusterKey),
        `details for cluster ${clusterKey}`,
        warnings
      ),
      fetchOptionalCustomerCollection(
        config,
        request,
        fetcher,
        aidpClusterPath(request, workspaceKey, clusterKey, "/libraries"),
        `libraries for cluster ${clusterKey}`,
        warnings
      )
    ]);
    const clusterDetails = objectPayload(clusterDetailsPayload);
    const normalizedClusterKey = workbenchKey(clusterDetails, ["clusterKey", "key", "id"]) || clusterKey;
    clusterLibraries[normalizedClusterKey] = collectionItems(librariesPayload);
    return {
      ...cluster,
      ...clusterDetails,
      key: normalizedClusterKey,
      clusterKey: normalizedClusterKey
    };
  }));

  return {
    workspace: {
      ...workspace,
      key: workspaceKey,
      workspaceKey
    },
    clusters,
    sessions: collectionItems(sessionsPayload),
    jobs,
    recentRuns,
    clusterLibraries
  };
}

async function fetchAidpCustomerRecentJobRuns(config, request, fetcher, workspaceKey, jobs, warnings) {
  const jobKeys = uniqueStrings(jobs.map((job) => workbenchKey(job, ["jobKey", "key", "id"])));
  if (jobKeys.length === 0) {
    return [];
  }

  const settledRuns = await Promise.all(jobKeys.map(async (jobKey) => {
    try {
      const payload = await fetcher(
        config,
        request,
        aidpWorkspaceJobRunsPath(request, workspaceKey, jobKey)
      );
      return collectionItems(payload);
    } catch (error) {
      warnings.push(`Latest job run for job ${jobKey} in workspace ${workspaceKey} unavailable: ${error.message || "lookup failed."}`);
      return [];
    }
  }));
  return settledRuns.flat();
}

async function fetchOptionalCustomerCollection(config, request, fetcher, pathName, label, warnings) {
  try {
    return await fetcher(config, request, pathName);
  } catch (error) {
    warnings.push(`${titleCase(label)} unavailable: ${error.message || "lookup failed."}`);
    return { items: [] };
  }
}

async function fetchServiceApiAidpWorkbenchGet(config, request, pathName) {
  if (!config.fetchImpl) {
    throw proxyError("FETCH_UNAVAILABLE", "No fetch implementation is available.", 500, false);
  }

  const url = aidpWorkbenchUrl(config, request, pathName);
  const headers = {
    Accept: "application/json",
    "opc-request-id": `aidp-customer-workbench-${randomUUID()}`
  };
  if (config.aidpAuthHeader) {
    headers.Authorization = config.aidpAuthHeader;
  }

  const upstreamResponse = await fetchWithTimeout(config.fetchImpl, url, {
    method: "GET",
    headers
  }, config.timeoutMs).catch((error) => {
    throw proxyError("UPSTREAM_UNAVAILABLE", error.message, 502, true);
  });

  const payload = await upstreamResponse.json().catch(() => {
    throw proxyError("INVALID_UPSTREAM_RESPONSE", "AIDP Workbench REST API returned invalid JSON.", 502, true);
  });

  if (!upstreamResponse.ok) {
    throw proxyError(
      payload?.code || codeFromStatus(upstreamResponse.status),
      payload?.message || `AIDP Workbench REST API returned HTTP ${upstreamResponse.status}.`,
      mapUpstreamStatus(upstreamResponse.status),
      isRetryableStatus(upstreamResponse.status)
    );
  }

  return payload;
}

async function fetchOciRawRequestAidpWorkbenchGet(config, request, pathName) {
  if (!config.execFileImpl) {
    throw proxyError("OCI_COMMAND_UNAVAILABLE", "No execFile implementation is available.", 500, false);
  }

  const args = ociRawRequestArgsForUrl(
    config,
    aidpWorkbenchUrl(config, request, pathName),
    request.requestedRegion
  );
  const payload = await execFileJson(config.execFileImpl, config.ociCommand, args, config.timeoutMs);
  const statusCode = rawRequestStatusCode(payload?.status);

  if (statusCode >= 400) {
    const errorPayload = payload?.data || {};
    throw proxyError(
      errorPayload.code || codeFromStatus(statusCode),
      errorPayload.message || `OCI raw-request AIDP Workbench lookup returned HTTP ${statusCode}.`,
      mapUpstreamStatus(statusCode),
      isRetryableStatus(statusCode)
    );
  }

  return payload?.data || payload;
}

async function ociSessionStatus(config, requestedRegion, { proxyMode } = {}) {
  const profile = config.ociProfile || DEFAULT_OCI_PROFILE;
  const auth = config.ociAuth || DEFAULT_OCI_AUTH;
  const canonicalRegion = canonicalRegionForEndpoint(requestedRegion);
  const command = ociSessionAuthenticateCommand(profile, canonicalRegion);
  const baseStatus = {
    schemaVersion: "aidp.oci-session.status.v1",
    required: proxyMode === "oci-raw-request",
    mode: proxyMode || "unknown",
    signedIn: proxyMode === "oci-raw-request" ? false : null,
    profile,
    auth,
    region: canonicalRegion || null,
    command,
    validateCommand: ociSessionValidateCommand(profile, auth, canonicalRegion),
    expiresAt: null,
    message: proxyMode === "oci-raw-request"
      ? "OCI CLI session status has not been checked."
      : "This proxy mode does not use OCI CLI session authentication."
  };

  if (proxyMode !== "oci-raw-request") {
    return baseStatus;
  }

  if (auth !== "security_token") {
    return {
      ...baseStatus,
      signedIn: false,
      message: "The local proxy must use --oci-auth security_token for customer OCI session sign-in."
    };
  }

  if (!config.execFileImpl) {
    return {
      ...baseStatus,
      signedIn: false,
      message: "OCI CLI command execution is unavailable in this proxy."
    };
  }

  try {
    const output = await execFileText(
      config.execFileImpl,
      config.ociCommand || DEFAULT_OCI_COMMAND,
      ociSessionValidateArgs(profile, auth, canonicalRegion),
      config.timeoutMs,
      "OCI session validation"
    );
    const text = [output.stdout, output.stderr].filter(Boolean).join("\n");
    const expiresAt = parseOciSessionExpiration(text);
    return {
      ...baseStatus,
      signedIn: true,
      expiresAt,
      message: expiresAt
        ? `OCI CLI session is valid until ${expiresAt}.`
        : "OCI CLI session is valid."
    };
  } catch (error) {
    return {
      ...baseStatus,
      signedIn: false,
      message: error.message || "OCI CLI session validation failed."
    };
  }
}

function ociRawRequestArgsForUrl(config, targetUrl, requestedRegion) {
  return [
    "raw-request",
    "--profile", config.ociProfile || DEFAULT_OCI_PROFILE,
    "--auth", config.ociAuth || DEFAULT_OCI_AUTH,
    "--region", canonicalRegionForEndpoint(requestedRegion),
    "--http-method", "GET",
    "--target-uri", targetUrl
  ];
}

function ociSessionValidateArgs(profile, auth, requestedRegion) {
  const args = [
    "session",
    "validate",
    "--profile", profile || DEFAULT_OCI_PROFILE,
    "--auth", auth || DEFAULT_OCI_AUTH
  ];
  if (requestedRegion) {
    args.push("--region", requestedRegion);
  }
  return args;
}

function ociSessionAuthenticateCommand(profile, requestedRegion) {
  return [
    "oci session authenticate",
    requestedRegion ? `--region ${requestedRegion}` : "",
    `--profile-name ${profile || DEFAULT_OCI_PROFILE}`
  ].filter(Boolean).join(" ");
}

function ociSessionValidateCommand(profile, auth, requestedRegion) {
  return [
    "oci session validate",
    `--profile ${profile || DEFAULT_OCI_PROFILE}`,
    `--auth ${auth || DEFAULT_OCI_AUTH}`,
    requestedRegion ? `--region ${requestedRegion}` : ""
  ].filter(Boolean).join(" ");
}

function aidpWorkbenchUrl(config, request, pathName) {
  const baseUrl = config.aidpWorkbenchBaseUrl || defaultAidpEndpoint(request.requestedRegion);
  return [
    trimTrailingSlash(baseUrl),
    normalizeApiVersion(config.aidpWorkbenchVersion || DEFAULT_AIDP_WORKBENCH_API_VERSION),
    pathName
  ].join("");
}

function aidpWorkbenchPath(request, suffix = "") {
  const normalizedSuffix = suffix ? (suffix.startsWith("/") ? suffix : `/${suffix}`) : "";
  return `/aiDataPlatforms/${encodeURIComponent(request.aiDataPlatformId)}${normalizedSuffix}`;
}

function aidpWorkspacePath(request, workspaceKey, suffix = "") {
  const normalizedSuffix = suffix ? (suffix.startsWith("/") ? suffix : `/${suffix}`) : "";
  return aidpWorkbenchPath(
    request,
    `/workspaces/${encodeURIComponent(workspaceKey)}${normalizedSuffix}`
  );
}

function aidpClusterPath(request, workspaceKey, clusterKey, suffix = "") {
  const normalizedSuffix = suffix ? (suffix.startsWith("/") ? suffix : `/${suffix}`) : "";
  return aidpWorkspacePath(
    request,
    workspaceKey,
    `/clusters/${encodeURIComponent(clusterKey)}${normalizedSuffix}`
  );
}

function aidpWorkspaceJobRunsPath(request, workspaceKey, jobKey) {
  const params = new URLSearchParams({
    jobKey,
    limit: "1",
    sortBy: "timeCreated",
    sortOrder: "DESC"
  });
  return `${aidpWorkspacePath(request, workspaceKey, "/jobRuns")}?${params.toString()}`;
}

function execFileJson(execFileImpl, command, args, timeoutMs, commandLabel = "OCI command") {
  return new Promise((resolve, reject) => {
    execFileImpl(command, args, {
      timeout: timeoutMs,
      maxBuffer: 4 * 1024 * 1024
    }, (error, stdout, stderr) => {
      const parsed = parseJson(stdout);
      if (error) {
        // A timeout-killed process may have written partial JSON to stdout; treat it
        // as a failure rather than resolving with a truncated/incomplete response.
        if (error.killed) {
          reject(proxyError(
            "TIMEOUT",
            summarizeCommandError(stderr || stdout || error.message, { timedOut: true, commandLabel }),
            504,
            true
          ));
          return;
        }
        if (parsed) {
          resolve(parsed);
          return;
        }
        reject(proxyError(
          "UPSTREAM_UNAVAILABLE",
          summarizeCommandError(stderr || stdout || error.message, { timedOut: false, commandLabel }),
          502,
          true
        ));
        return;
      }
      if (!parsed) {
        reject(proxyError("INVALID_UPSTREAM_RESPONSE", `${commandLabel} returned invalid JSON.`, 502, true));
        return;
      }
      resolve(parsed);
    });
  });
}

function execFileText(execFileImpl, command, args, timeoutMs, commandLabel = "OCI command") {
  return new Promise((resolve, reject) => {
    execFileImpl(command, args, {
      timeout: timeoutMs,
      maxBuffer: 1024 * 1024
    }, (error, stdout, stderr) => {
      if (error) {
        reject(proxyError(
          error.killed ? "TIMEOUT" : "UPSTREAM_UNAVAILABLE",
          summarizeCommandError(stderr || stdout || error.message, { timedOut: error.killed, commandLabel }),
          error.killed ? 504 : 502,
          true
        ));
        return;
      }
      resolve({ stdout: String(stdout || ""), stderr: String(stderr || "") });
    });
  });
}

async function fetchWithTimeout(fetchImpl, url, options, timeoutMs) {
  if (!timeoutMs || typeof AbortController === "undefined") {
    return fetchImpl(url, options);
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetchImpl(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function serveStaticFile(pathname, response, appDir, defaultStaticPage = DEFAULT_STATIC_PAGE) {
  const defaultPage = String(defaultStaticPage || DEFAULT_STATIC_PAGE).replace(/^\/+/, "") || DEFAULT_STATIC_PAGE;
  const relativePath = pathname === "/" ? defaultPage : pathname.slice(1);
  const safeRelativePath = path.normalize(relativePath).replace(/^(\.\.(\/|\\|$))+/, "");
  const filePath = path.resolve(appDir, safeRelativePath);

  if (!filePath.startsWith(`${appDir}${path.sep}`) || !fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    sendText(response, 404, "Not found\n", "text/plain; charset=utf-8");
    return;
  }

  sendText(response, 200, fs.readFileSync(filePath), contentTypeFor(filePath));
}

function sendJson(response, statusCode, value) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  response.end(`${JSON.stringify(value, null, 2)}\n`);
}

function sendText(response, statusCode, value, contentType) {
  response.writeHead(statusCode, {
    "Content-Type": contentType,
    "Cache-Control": "no-store"
  });
  response.end(value);
}

function errorBody(code, message, retryable = false) {
  return { code, message, retryable };
}

function proxyError(code, message, statusCode, retryable) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  error.retryable = retryable;
  return error;
}

function codeFromStatus(statusCode) {
  if (statusCode === 400) return "INVALID_REQUEST";
  if (statusCode === 401 || statusCode === 403) return "PERMISSION_DENIED";
  if (statusCode === 404) return "NOT_FOUND";
  if (statusCode === 409) return "REGION_MISMATCH";
  if (statusCode === 504) return "TIMEOUT";
  if (statusCode === 502 || statusCode === 503) return "UPSTREAM_UNAVAILABLE";
  return `HTTP_${statusCode}`;
}

function mapUpstreamStatus(statusCode) {
  if (statusCode === 401 || statusCode === 403) return 403;
  if (statusCode === 404) return 404;
  if (statusCode === 409) return 409;
  if (statusCode === 504) return 504;
  if (statusCode >= 400 && statusCode < 500) return 502;
  return statusCode || 502;
}

function isRetryableStatus(statusCode) {
  return statusCode === 408 || statusCode === 429 || statusCode >= 500;
}

function parseOptions(args) {
  const options = {};
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected argument '${arg}'`);
    }
    const key = arg.slice(2);
    const value = args[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for '${arg}'`);
    }
    options[key] = value;
    index += 1;
  }
  return options;
}

function parsePort(value) {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("--port must be an integer between 1 and 65535");
  }
  return port;
}

function parseHost(value) {
  const host = String(value || DEFAULT_HOST).trim();
  if (!host || /[\s/\\]/.test(host)) {
    throw new Error("--host must be a hostname or IP address");
  }
  return host;
}

function normalizeProxyMode(mode) {
  if (mode === "fixture" || mode === "service-api" || mode === "oci-raw-request") {
    return mode;
  }
  throw new Error("--mode must be fixture, service-api, or oci-raw-request");
}

function parseTimeoutMs(value) {
  const timeoutMs = Number(value || DEFAULT_UPSTREAM_TIMEOUT_MS);
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error("--upstream-timeout-ms must be a positive integer");
  }
  return timeoutMs;
}

function trimTrailingSlash(value) {
  return String(value).replace(/\/+$/, "");
}

function normalizeApiVersion(value) {
  const apiVersion = String(value || DEFAULT_AIDP_WORKBENCH_API_VERSION);
  return apiVersion.startsWith("/") ? apiVersion : `/${apiVersion}`;
}

function defaultAidpEndpoint(region) {
  return `https://aidp.${canonicalRegionForEndpoint(region)}.oci.oraclecloud.com`;
}

function canonicalRegionForEndpoint(region) {
  const normalized = String(region || "").trim().toLowerCase();
  return OCI_SHORT_REGION_ENDPOINTS[normalized] || normalized;
}

function bearerAuthHeader(token) {
  return token ? `Bearer ${token}` : "";
}

function collectionItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }
  return [
    payload?.items,
    payload?.data?.items,
    payload?.collection?.items,
    payload?.data?.collection?.items,
    payload?.data,
    payload?.resources
  ].find(Array.isArray) || [];
}

function objectPayload(payload) {
  const value = payload?.data || payload?.cluster || payload?.workspace || payload;
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function workbenchKey(value, keys) {
  return stringOrNull(pick(value, keys));
}

function pick(value, keys) {
  if (!value || typeof value !== "object") {
    return null;
  }
  return keys.map((key) => value[key]).find((item) => item !== undefined && item !== null && item !== "") || null;
}

function stringOrNull(value) {
  return value === undefined || value === null || value === "" ? null : String(value);
}

function uniqueStrings(values) {
  return [...new Set((Array.isArray(values) ? values : [])
    .filter((item) => typeof item === "string" && item.trim())
    .map((item) => item.trim()))];
}

function titleCase(value) {
  return String(value || "").replace(/^\w/, (letter) => letter.toUpperCase());
}

function rawRequestStatusCode(value) {
  if (!value) {
    return 200;
  }
  const match = String(value).match(/^(\d{3})/);
  return match ? Number(match[1]) : 200;
}

function parseOciSessionExpiration(value) {
  const text = String(value || "");
  const match = text.match(/\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?/);
  if (!match) {
    return null;
  }
  const normalized = match[0].replace(" ", "T");
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? normalized : date.toISOString();
}

function firstSearchParam(searchParams, keys) {
  return keys
    .map((key) => searchParams.get(key))
    .find((value) => typeof value === "string" && value.trim()) || null;
}

function parseJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function summarizeCommandError(value, { timedOut = false, commandLabel = "OCI command" } = {}) {
  const text = String(value || "").trim();
  if (timedOut) {
    return `${commandLabel} timed out.`;
  }
  return text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)[0] ||
    `${commandLabel} failed.`;
}

function contentTypeFor(filePath) {
  const extension = path.extname(filePath);
  if (extension === ".html") return "text/html; charset=utf-8";
  if (extension === ".css") return "text/css; charset=utf-8";
  if (extension === ".js") return "text/javascript; charset=utf-8";
  if (extension === ".json") return "application/json; charset=utf-8";
  if (extension === ".svg") return "image/svg+xml";
  return "application/octet-stream";
}
