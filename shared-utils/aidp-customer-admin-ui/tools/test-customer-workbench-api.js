#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const context = {
  console,
  Date,
  Set,
  URLSearchParams,
  window: {}
};
vm.createContext(context);

[
  "../app/data/customer-workbench-fixture.js",
  "../app/scripts/customer-workbench-api.js"
].forEach((relativePath) => {
  const filePath = path.join(__dirname, relativePath);
  vm.runInContext(fs.readFileSync(filePath, "utf8"), context, {
    filename: filePath
  });
});

const api = context.window.AIDP.customerWorkbench;
const fixture = context.window.AIDP_CUSTOMER_WORKBENCH_FIXTURE;

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

async function run() {
  testFixtureNormalization();
  testRawRestAggregation();
  await testOciSessionAdapter();
  console.log("customer workbench API tests passed");
}

function testFixtureNormalization() {
  const snapshot = api.normalizeWorkbenchSnapshot(fixture);

  assert.strictEqual(snapshot.schemaVersion, "aidp.customer-workbench.view-model.v1");
  assert.strictEqual(snapshot.totals.workspaces, 2);
  assert.strictEqual(snapshot.totals.clusters, 3);
  assert.strictEqual(snapshot.totals.activeClusters, 2);
  assert.strictEqual(snapshot.totals.notebooks, 3);
  assert.strictEqual(snapshot.totals.workflows, 4);
  assert.strictEqual(snapshot.totals.specialLibraries, 6);
  assert.strictEqual(snapshot.totals.ocpus, 32);
  assert.strictEqual(snapshot.totals.memoryInGBs, 512);

  const productionCluster = snapshot.workspaces[0].clusters[0];
  assert.strictEqual(productionCluster.key, "e185eb24-7022-4ade-ad2c-048850970332");
  assert.strictEqual(productionCluster.active, true);
  assert.strictEqual(productionCluster.notebookSessions.length, 2);
  assert.ok(productionCluster.specialLibraries.some((library) => library.name === "xgboost"));
}

function testRawRestAggregation() {
  const snapshot = api.normalizeWorkbenchSnapshot({
    schemaVersion: "aidp.customer-workbench.raw.v1",
    aiDataPlatformId: "ocid1.aidataplatform.oc1.phx.example",
    region: "phx",
    fetchedAt: "2026-05-15T18:30:00.000Z",
    source: {
      adapter: "aidp-workbench-rest-proxy",
      freshness: "Live AIDP Workbench REST API"
    },
    workspaces: [
      {
        workspace: {
          key: "workspace-key",
          displayName: "Workspace"
        },
        clusters: [
          {
            key: "cluster-key",
            displayName: "Cluster",
            state: "ACTIVE",
            nodeType: "VM.Standard.E5.Flex",
            activeClusterResources: {
              activeExecutorCount: 2,
              activeCores: 8,
              activeMemoryInGBs: 64
            }
          }
        ],
        sessions: [
          {
            id: "session-1",
            name: "Notebook",
            kernel: {
              computeClusterKey: "cluster-key"
            },
            state: "ACTIVE"
          }
        ],
        jobs: [
          {
            key: "job-1",
            displayName: "Workflow",
            settings: {
              clusterKey: "cluster-key"
            },
            isScheduled: true
          }
        ],
        recentRuns: [
          {
            jobKey: "job-1",
            status: "RUNNING",
            timeStarted: "2026-05-15T18:00:00.000Z"
          }
        ],
        clusterLibraries: {
          "cluster-key": [
            {
              name: "spark",
              version: "3.5.0"
            },
            {
              name: "xgboost",
              version: "2.0.3"
            }
          ]
        }
      }
    ]
  });

  assert.strictEqual(snapshot.totals.workspaces, 1);
  assert.strictEqual(snapshot.totals.clusters, 1);
  assert.strictEqual(snapshot.totals.activeClusters, 1);
  assert.strictEqual(snapshot.totals.notebooks, 1);
  assert.strictEqual(snapshot.totals.workflows, 1);
  assert.strictEqual(snapshot.totals.specialLibraries, 1);
  assert.strictEqual(snapshot.totals.ocpus, 8);
  assert.strictEqual(snapshot.totals.memoryInGBs, 64);

  const cluster = snapshot.workspaces[0].clusters[0];
  assert.strictEqual(cluster.notebookSessions[0].name, "Notebook");
  assert.strictEqual(cluster.workflows[0].latestRunState, "RUNNING");
  assert.deepStrictEqual(cluster.specialLibraries.map((library) => library.name), ["xgboost"]);
}

async function testOciSessionAdapter() {
  const adapter = api.createOciSessionAdapter({
    fetchImpl: async (url, options) => {
      assert.strictEqual(String(url), "/api/oci-session/status?region=phx");
      assert.strictEqual(options.method, "GET");
      return {
        ok: true,
        async json() {
          return {
            schemaVersion: "aidp.oci-session.status.v1",
            required: true,
            signedIn: true,
            profile: "AIDP_CUSTOMER",
            auth: "security_token",
            region: "us-phoenix-1",
            message: "OCI CLI session is valid."
          };
        }
      };
    }
  });

  const status = await adapter.getStatus({ region: "phx" });
  assert.strictEqual(status.signedIn, true);
  assert.strictEqual(status.profile, "AIDP_CUSTOMER");

  // A non-OK response that is itself a session status (e.g. 401 signedIn:false)
  // is still returned as the status.
  const signedOutAdapter = api.createOciSessionAdapter({
    fetchImpl: async () => ({
      ok: false,
      status: 401,
      async json() {
        return { required: true, signedIn: false, message: "Sign-in required." };
      }
    })
  });
  const signedOut = await signedOutAdapter.getStatus({ region: "phx" });
  assert.strictEqual(signedOut.signedIn, false);
  assert.strictEqual(signedOut.required, true);

  // A bare error body (no session / no signedIn) must surface as an error rather
  // than masquerade as a status object with undefined fields.
  const erroringAdapter = api.createOciSessionAdapter({
    fetchImpl: async () => ({
      ok: false,
      status: 500,
      async json() {
        return { code: "INTERNAL_SERVER_ERROR", message: "boom", retryable: false };
      }
    })
  });
  await assert.rejects(
    () => erroringAdapter.getStatus({ region: "phx" }),
    (error) => error && error.code === "INTERNAL_SERVER_ERROR"
  );
}
