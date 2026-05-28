(function () {
  "use strict";

  window.AIDP_CUSTOMER_WORKBENCH_FIXTURE = {
    schemaVersion: "aidp.customer-workbench.view-model.v1",
    aiDataPlatformId: "ocid1.aidataplatform.oc1.phx.example",
    region: "phx",
    fetchedAt: "2026-05-15T18:00:00.000Z",
    source: {
      adapter: "fixture",
      freshness: "Local fixture",
      warnings: [
        "Fixture data mirrors the AIDP Workbench REST resource shape and contains no customer data."
      ]
    },
    workspaces: [
      {
        key: "2b3e6c3e-a2b9-4f2c-9127-4f69ea4166b1",
        displayName: "Fraud Analytics",
        description: "Shared notebook and workflow environment for fraud feature generation.",
        state: "ACTIVE",
        ownerDisplayName: "Analytics team",
        timeUpdated: "2026-05-15T16:42:00.000Z",
        clusters: [
          {
            key: "e185eb24-7022-4ade-ad2c-048850970332",
            displayName: "feature-engineering-prod",
            state: "ACTIVE",
            stateDetails: "Running notebooks and scheduled jobs",
            type: "USER",
            nodeType: "VM.Standard.E5.Flex",
            driverConfig: {
              driverShape: "VM.Standard.E5.Flex",
              driverShapeConfig: {
                ocpus: 2,
                memoryInGBs: 32
              }
            },
            activeClusterResources: {
              activeExecutorCount: 5,
              activeCores: 22,
              activeMemoryInGBs: 352
            },
            notebookSessions: [
              {
                id: "session-fraud-risk-model",
                name: "risk-model.ipynb",
                path: "/Users/analysts/risk-model.ipynb",
                state: "active"
              },
              {
                id: "session-score-audit",
                name: "score-audit.ipynb",
                path: "/Shared/fraud/score-audit.ipynb",
                state: "idle"
              }
            ],
            workflows: [
              {
                key: "job-nightly-features",
                displayName: "Nightly feature refresh",
                state: "ACTIVE",
                schedule: "0 3 * * *",
                latestRunState: "SUCCEEDED",
                latestRunAt: "2026-05-15T10:04:00.000Z"
              },
              {
                key: "job-model-validation",
                displayName: "Model validation pack",
                state: "ACTIVE",
                schedule: "Manual",
                latestRunState: "RUNNING",
                latestRunAt: "2026-05-15T17:21:00.000Z"
              }
            ],
            libraries: [
              {
                name: "oracle-ads",
                version: "2.12.1",
                type: "PYPI",
                scope: "cluster",
                status: "INSTALLED"
              },
              {
                name: "xgboost",
                version: "2.0.3",
                type: "PYPI",
                scope: "cluster",
                status: "INSTALLED"
              },
              {
                name: "oci://customer-libs/fraud-features-0.9.4.whl",
                version: "0.9.4",
                type: "WHEEL",
                scope: "cluster",
                status: "INSTALLED"
              }
            ]
          },
          {
            key: "8f6b5047-32a7-4b32-9a13-7b6ecf344610",
            displayName: "ad-hoc-sandbox",
            state: "STOPPED",
            stateDetails: "Stopped by user",
            type: "USER",
            nodeType: "VM.Standard.E4.Flex",
            driverConfig: {
              driverShape: "VM.Standard.E4.Flex",
              driverShapeConfig: {
                ocpus: 1,
                memoryInGBs: 16
              }
            },
            activeClusterResources: {
              activeExecutorCount: 0,
              activeCores: 0,
              activeMemoryInGBs: 0
            },
            notebookSessions: [],
            workflows: [
              {
                key: "job-exploration-template",
                displayName: "Exploration template",
                state: "INACTIVE",
                schedule: "Manual",
                latestRunState: "SUCCEEDED",
                latestRunAt: "2026-05-11T21:18:00.000Z"
              }
            ],
            libraries: [
              {
                name: "geopandas",
                version: "0.14.3",
                type: "PYPI",
                scope: "cluster",
                status: "PENDING_RESTART"
              }
            ]
          }
        ]
      },
      {
        key: "7f24f395-1e98-45ff-a644-e64ced2730d7",
        displayName: "Marketing Science",
        description: "Campaign analytics, SQL exploration, and automated audience scoring.",
        state: "ACTIVE",
        ownerDisplayName: "Marketing data science",
        timeUpdated: "2026-05-14T23:09:00.000Z",
        clusters: [
          {
            key: "c4c57eb5-f483-4f45-b86b-9b058cd6de2b",
            displayName: "audience-scoring",
            state: "STARTING",
            stateDetails: "Provisioning executors",
            type: "USER",
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
              activeCores: 10,
              activeMemoryInGBs: 160
            },
            notebookSessions: [
              {
                id: "session-audience-model",
                name: "audience-model.ipynb",
                path: "/Shared/marketing/audience-model.ipynb",
                state: "starting"
              }
            ],
            workflows: [
              {
                key: "job-audience-score",
                displayName: "Audience score publish",
                state: "ACTIVE",
                schedule: "0 */6 * * *",
                latestRunState: "FAILED",
                latestRunAt: "2026-05-15T12:03:00.000Z"
              }
            ],
            libraries: [
              {
                name: "prophet",
                version: "1.1.5",
                type: "PYPI",
                scope: "cluster",
                status: "INSTALLED"
              },
              {
                name: "spark-nlp",
                version: "5.3.2",
                type: "PYPI",
                scope: "cluster",
                status: "INSTALLED"
              }
            ]
          }
        ]
      }
    ]
  };
})();
