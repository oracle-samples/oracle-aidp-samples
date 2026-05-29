#!/usr/bin/env node
"use strict";

const assert = require("assert");
const http = require("http");
const { createServer } = require("./customer-workbench-proxy");

const AIDP_ID = "ocid1.aidataplatform.oc1.phx.example";
const ENCODED_AIDP_ID = encodeURIComponent(AIDP_ID);

testServiceApiCustomerWorkbenchRoute().then(() => {
  return testOciSessionStatusRoute();
}).then(() => {
  return testOciRawWorkbenchRequiresSession();
}).then(() => {
  return testCustomerWorkbenchDefaultPage();
}).then(() => {
  return testRequestHandlerReturns500OnUnexpectedError();
}).then(() => {
  return testOciRawRequestTimeoutIsNotResolvedAsSuccess();
}).then(() => {
  console.log("customer workbench proxy tests passed");
}).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

async function testServiceApiCustomerWorkbenchRoute() {
  const upstreamRequests = [];
  const upstream = http.createServer((request, response) => {
    upstreamRequests.push({
      method: request.method,
      url: request.url,
      authorization: request.headers.authorization,
      opcRequestId: request.headers["opc-request-id"]
    });
    const payload = upstreamPayloadFor(request.url);
    response.writeHead(payload.statusCode || 200, { "Content-Type": "application/json" });
    response.end(`${JSON.stringify(payload.body)}\n`);
  });

  await listen(upstream);
  const upstreamBaseUrl = `http://127.0.0.1:${upstream.address().port}`;
  const proxy = createServer({
    mode: "service-api",
    aidpWorkbenchApiBaseUrl: upstreamBaseUrl,
    serviceApiAuthHeader: "Bearer test-token",
    fixtures: []
  });
  await listen(proxy);

  try {
    const baseUrl = `http://127.0.0.1:${proxy.address().port}`;
    const success = await getJson(`${baseUrl}/api/aidp-customer/workbench?region=phx&aiDataPlatformId=${ENCODED_AIDP_ID}`);

    assert.strictEqual(success.statusCode, 200);
    assert.strictEqual(success.body.schemaVersion, "aidp.customer-workbench.raw.v1");
    assert.strictEqual(success.body.aiDataPlatformId, AIDP_ID);
    assert.strictEqual(success.body.region, "phx");
    assert.strictEqual(success.body.workspaces.length, 1);
    assert.strictEqual(success.body.workspaces[0].workspace.key, "workspace-key");
    assert.strictEqual(success.body.workspaces[0].clusters.length, 1);
    assert.strictEqual(success.body.workspaces[0].clusters[0].clusterKey, "cluster-key");
    assert.strictEqual(success.body.workspaces[0].sessions.length, 1);
    assert.strictEqual(success.body.workspaces[0].jobs.length, 1);
    assert.strictEqual(success.body.workspaces[0].recentRuns.length, 1);
    assert.strictEqual(success.body.workspaces[0].clusterLibraries["cluster-key"][0].name, "xgboost");

    const paths = upstreamRequests.map((request) => request.url.split("?")[0]);
    assert.deepStrictEqual(new Set(paths), new Set([
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces`,
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces/workspace-key/clusters`,
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces/workspace-key/notebook/api/sessions`,
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces/workspace-key/jobs`,
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces/workspace-key/jobRuns`,
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces/workspace-key/clusters/cluster-key`,
      `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}/workspaces/workspace-key/clusters/cluster-key/libraries`
    ]));
    const jobRunsRequest = upstreamRequests.find((request) => request.url.includes("/jobRuns?"));
    const jobRunsParams = new URL(jobRunsRequest.url, "http://upstream.local").searchParams;
    assert.strictEqual(jobRunsParams.get("jobKey"), "job-1");
    assert.strictEqual(jobRunsParams.get("limit"), "1");
    assert.strictEqual(jobRunsParams.get("sortBy"), "timeCreated");
    assert.strictEqual(jobRunsParams.get("sortOrder"), "DESC");
    assert.ok(upstreamRequests.every((request) => request.method === "GET"));
    assert.ok(upstreamRequests.every((request) => request.authorization === "Bearer test-token"));
    assert.ok(upstreamRequests.every((request) => request.opcRequestId?.startsWith("aidp-customer-workbench-")));
    assert.strictEqual(new Set(upstreamRequests.map((request) => request.opcRequestId)).size, upstreamRequests.length);
  } finally {
    await close(proxy);
    await close(upstream);
  }
}

async function testOciSessionStatusRoute() {
  const execCalls = [];
  const proxy = createServer({
    mode: "oci-raw-request",
    ociProfile: "AIDP_CUSTOMER",
    fixtures: [],
    execFileImpl: (command, args, options, callback) => {
      execCalls.push({ command, args, options });
      callback(null, "Session is valid until 2026-05-15T20:00:00Z\n", "");
    }
  });
  await listen(proxy);

  try {
    const baseUrl = `http://127.0.0.1:${proxy.address().port}`;
    const success = await getJson(`${baseUrl}/api/oci-session/status?region=phx`);

    assert.strictEqual(success.statusCode, 200);
    assert.strictEqual(success.body.required, true);
    assert.strictEqual(success.body.signedIn, true);
    assert.strictEqual(success.body.profile, "AIDP_CUSTOMER");
    assert.strictEqual(success.body.auth, "security_token");
    assert.strictEqual(success.body.region, "us-phoenix-1");
    assert.strictEqual(success.body.command, "oci session authenticate --region us-phoenix-1 --profile-name AIDP_CUSTOMER");
    assert.strictEqual(success.body.expiresAt, "2026-05-15T20:00:00.000Z");
    assert.deepStrictEqual(execCalls[0].args, [
      "session",
      "validate",
      "--profile", "AIDP_CUSTOMER",
      "--auth", "security_token",
      "--region", "us-phoenix-1"
    ]);
  } finally {
    await close(proxy);
  }
}

async function testOciRawWorkbenchRequiresSession() {
  const execCalls = [];
  const proxy = createServer({
    mode: "oci-raw-request",
    ociProfile: "AIDP_CUSTOMER",
    fixtures: [],
    execFileImpl: (command, args, options, callback) => {
      execCalls.push({ command, args, options });
      callback(new Error("NotAuthenticated"), "", "NotAuthenticated");
    }
  });
  await listen(proxy);

  try {
    const baseUrl = `http://127.0.0.1:${proxy.address().port}`;
    const response = await getJson(`${baseUrl}/api/aidp-customer/workbench?region=phx&aiDataPlatformId=${ENCODED_AIDP_ID}`);

    assert.strictEqual(response.statusCode, 401);
    assert.strictEqual(response.body.code, "OCI_SESSION_REQUIRED");
    assert.strictEqual(response.body.session.signedIn, false);
    assert.strictEqual(response.body.session.command, "oci session authenticate --region us-phoenix-1 --profile-name AIDP_CUSTOMER");
    assert.strictEqual(execCalls.length, 1);
    assert.deepStrictEqual(execCalls[0].args.slice(0, 2), ["session", "validate"]);
  } finally {
    await close(proxy);
  }
}

async function testCustomerWorkbenchDefaultPage() {
  const proxy = createServer({
    fixtures: [],
    defaultStaticPage: "index.html"
  });
  await listen(proxy);

  try {
    const baseUrl = `http://127.0.0.1:${proxy.address().port}`;
    const response = await getText(`${baseUrl}/`);

    assert.strictEqual(response.statusCode, 200);
    assert.ok(response.body.includes("<title>AIDP Customer Workbench Usage</title>"));
  } finally {
    await close(proxy);
  }
}

async function testRequestHandlerReturns500OnUnexpectedError() {
  const proxy = createServer({
    appDir: null
  });
  await listen(proxy);

  try {
    const baseUrl = `http://127.0.0.1:${proxy.address().port}`;
    const response = await getJson(`${baseUrl}/`);

    assert.strictEqual(response.statusCode, 500);
    assert.strictEqual(response.body.code, "INTERNAL_SERVER_ERROR");
    assert.strictEqual(response.body.retryable, false);
  } finally {
    await close(proxy);
  }
}

async function testOciRawRequestTimeoutIsNotResolvedAsSuccess() {
  const proxy = createServer({
    mode: "oci-raw-request",
    ociProfile: "AIDP_CUSTOMER",
    fixtures: [],
    execFileImpl: (command, args, options, callback) => {
      if (args[0] === "session" && args[1] === "validate") {
        callback(null, "Session is valid until 2026-05-15T20:00:00Z\n", "");
        return;
      }
      // Simulate a timeout-killed OCI CLI process that nonetheless flushed a
      // complete, parseable JSON document to stdout before being terminated.
      // The timeout must win: a killed process is a failure even when its output
      // happens to parse, so this must surface as TIMEOUT and not a 200 success.
      const killError = new Error("Command timed out");
      killError.killed = true;
      callback(killError, '{"status":"200 OK","data":{"items":[]}}', "");
    }
  });
  await listen(proxy);

  try {
    const baseUrl = `http://127.0.0.1:${proxy.address().port}`;
    const response = await getJson(`${baseUrl}/api/aidp-customer/workbench?region=phx&aiDataPlatformId=${ENCODED_AIDP_ID}`);

    assert.strictEqual(response.statusCode, 504);
    assert.strictEqual(response.body.code, "TIMEOUT");
  } finally {
    await close(proxy);
  }
}

function upstreamPayloadFor(url) {
  const pathname = new URL(url, "http://upstream.local").pathname;
  const routePrefix = `/20260430/aiDataPlatforms/${ENCODED_AIDP_ID}`;
  const routes = {
    [`${routePrefix}/workspaces`]: {
      items: [
        {
          key: "workspace-key",
          displayName: "Workspace"
        }
      ]
    },
    [`${routePrefix}/workspaces/workspace-key/clusters`]: {
      items: [
        {
          key: "cluster-key",
          displayName: "Cluster"
        }
      ]
    },
    [`${routePrefix}/workspaces/workspace-key/notebook/api/sessions`]: {
      items: [
        {
          id: "session-1",
          name: "Notebook",
          computeClusterKey: "cluster-key",
          state: "ACTIVE"
        }
      ]
    },
    [`${routePrefix}/workspaces/workspace-key/jobs`]: {
      items: [
        {
          key: "job-1",
          displayName: "Workflow",
          clusterKey: "cluster-key"
        }
      ]
    },
    [`${routePrefix}/workspaces/workspace-key/jobRuns`]: {
      items: [
        {
          jobKey: "job-1",
          status: "SUCCEEDED"
        }
      ]
    },
    [`${routePrefix}/workspaces/workspace-key/clusters/cluster-key`]: {
      key: "cluster-key",
      state: "ACTIVE",
      nodeType: "VM.Standard.E5.Flex",
      driverConfig: {
        driverShape: "VM.Standard.E5.Flex",
        driverShapeConfig: {
          ocpus: 2,
          memoryInGBs: 32
        }
      },
      activeClusterResources: {
        activeExecutorCount: 2,
        activeCores: 8,
        activeMemoryInGBs: 64
      }
    },
    [`${routePrefix}/workspaces/workspace-key/clusters/cluster-key/libraries`]: {
      items: [
        {
          name: "xgboost",
          version: "2.0.3"
        }
      ]
    }
  };
  if (!routes[pathname]) {
    return {
      statusCode: 404,
      body: {
        code: "NOT_FOUND",
        message: `Unexpected path ${pathname}`
      }
    };
  }
  return {
    statusCode: 200,
    body: routes[pathname]
  };
}

function listen(server) {
  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
}

function close(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (response) => {
      let text = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        text += chunk;
      });
      response.on("end", () => {
        resolve({
          statusCode: response.statusCode,
          body: JSON.parse(text)
        });
      });
    }).on("error", reject);
  });
}

function getText(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (response) => {
      let text = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        text += chunk;
      });
      response.on("end", () => {
        resolve({
          statusCode: response.statusCode,
          body: text
        });
      });
    }).on("error", reject);
  });
}
