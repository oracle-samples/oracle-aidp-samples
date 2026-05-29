(function () {
  "use strict";

  const SCHEMA_VERSION = "aidp.customer-workbench.view-model.v1";
  const RAW_SCHEMA_VERSION = "aidp.customer-workbench.raw.v1";
  const DEFAULT_PROXY_PATH = "/api/aidp-customer/workbench";
  const DEFAULT_SESSION_STATUS_PATH = "/api/oci-session/status";
  const ACTIVE_STATES = new Set(["ACTIVE", "STARTING", "RESTARTING", "UPDATING"]);

  window.AIDP = window.AIDP || {};
  window.AIDP.customerWorkbench = {
    ACTIVE_STATES,
    SCHEMA_VERSION,
    aggregateRawWorkbench,
    createCustomerWorkbenchAdapter,
    createOciSessionAdapter,
    normalizeWorkbenchSnapshot
  };

  function createCustomerWorkbenchAdapter(options = {}) {
    const fetchImpl = options.fetchImpl || window.fetch?.bind(window);
    const proxyPath = options.proxyPath || DEFAULT_PROXY_PATH;
    return {
      async getWorkbenchSnapshot(request = {}) {
        if (!fetchImpl) {
          throw adapterError("FETCH_UNAVAILABLE", "No fetch implementation is available.");
        }
        const aiDataPlatformId = stringOrNull(request.aiDataPlatformId);
        const region = stringOrNull(request.region);
        if (!aiDataPlatformId || !region) {
          throw adapterError("REQUEST_INCOMPLETE", "AIDP OCID and region are required.");
        }

        const params = new URLSearchParams({ aiDataPlatformId, region });
        const response = await fetchImpl(`${proxyPath}?${params.toString()}`, {
          method: "GET",
          headers: { Accept: "application/json" },
          signal: request.signal
        }).catch((error) => {
          throw adapterError("NETWORK_ERROR", error.message || "Workbench lookup failed.");
        });

        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          throw adapterError(
            payload.code || `HTTP_${response.status}`,
            payload.message || `Workbench lookup failed with HTTP ${response.status}.`
          );
        }

        const payload = await response.json().catch(() => {
          throw adapterError("INVALID_RESPONSE", "Workbench lookup returned invalid JSON.");
        });
        return normalizeWorkbenchSnapshot(payload, { aiDataPlatformId, region });
      }
    };
  }

  function createOciSessionAdapter(options = {}) {
    const fetchImpl = options.fetchImpl || window.fetch?.bind(window);
    const statusPath = options.statusPath || DEFAULT_SESSION_STATUS_PATH;
    return {
      async getStatus(request = {}) {
        if (!fetchImpl) {
          throw adapterError("FETCH_UNAVAILABLE", "No fetch implementation is available.");
        }
        const params = new URLSearchParams();
        const region = stringOrNull(request.region);
        if (region) {
          params.set("region", region);
        }

        const response = await fetchImpl(`${statusPath}?${params.toString()}`, {
          method: "GET",
          headers: { Accept: "application/json" },
          signal: request.signal
        }).catch((error) => {
          throw adapterError("NETWORK_ERROR", error.message || "OCI session status lookup failed.");
        });
        const payload = await response.json().catch(() => {
          throw adapterError("INVALID_RESPONSE", "OCI session status lookup returned invalid JSON.");
        });
        if (!response.ok) {
          if (payload?.session) {
            return payload.session;
          }
          // Only treat the body as a session status when it actually carries one
          // (e.g. a 401 with signedIn:false). A bare error body ({code,message,...})
          // must surface as an error, not masquerade as an unknown-but-OK status.
          if (payload && typeof payload === "object" && "signedIn" in payload) {
            return payload;
          }
          throw adapterError(
            payload?.code || "SESSION_STATUS_ERROR",
            payload?.message || `OCI session status lookup failed (${response.status}).`
          );
        }
        return payload;
      }
    };
  }

  function normalizeWorkbenchSnapshot(payload, context = {}) {
    const value = payload?.data || payload?.workbench || payload || {};
    if (value.schemaVersion === RAW_SCHEMA_VERSION) {
      return withDerivedStats(aggregateRawWorkbench(value));
    }

    const aiDataPlatformId = stringOrNull(value.aiDataPlatformId || context.aiDataPlatformId);
    const region = stringOrNull(value.region || context.region);
    const workspaces = collectionItems(value.workspaces).map(normalizeWorkspace).filter(Boolean);
    return withDerivedStats({
      schemaVersion: SCHEMA_VERSION,
      aiDataPlatformId,
      region,
      fetchedAt: value.fetchedAt || new Date().toISOString(),
      source: {
        adapter: value.source?.adapter || "fixture",
        freshness: value.source?.freshness || "Local fixture",
        warnings: safeStrings(value.source?.warnings)
      },
      workspaces
    });
  }

  function aggregateRawWorkbench(raw) {
    const aiDataPlatformId = stringOrNull(raw.aiDataPlatformId);
    const region = stringOrNull(raw.region);
    const workspaces = collectionItems(raw.workspaces).map((entry) => {
      const workspace = entry.workspace || entry;
      const workspaceKey = keyOf(workspace);
      const sessions = collectionItems(entry.sessions || raw.sessionsByWorkspace?.[workspaceKey]);
      const jobs = collectionItems(entry.jobs || raw.jobsByWorkspace?.[workspaceKey]);
      const recentRuns = collectionItems(entry.recentRuns || raw.recentRunsByWorkspace?.[workspaceKey]);
      const clusters = collectionItems(entry.clusters || raw.clustersByWorkspace?.[workspaceKey]).map((cluster) => {
        const clusterKey = keyOf(cluster);
        const libraries = collectionItems(
          entry.clusterLibraries?.[clusterKey] ||
          raw.librariesByCluster?.[clusterKey] ||
          cluster.libraries
        );
        return normalizeCluster({
          ...cluster,
          libraries: libraries.map(normalizeLibrary).filter(Boolean),
          notebookSessions: sessions.filter((session) => sessionMatchesCluster(session, clusterKey)),
          workflows: jobs
            .filter((job) => jobMatchesCluster(job, clusterKey))
            .map((job) => normalizeWorkflow(job, recentRuns))
            .filter(Boolean)
        });
      }).filter(Boolean);
      return normalizeWorkspace({ ...workspace, clusters });
    }).filter(Boolean);

    return {
      schemaVersion: SCHEMA_VERSION,
      aiDataPlatformId,
      region,
      fetchedAt: raw.fetchedAt || new Date().toISOString(),
      source: {
        adapter: raw.source?.adapter || "aidp-rest-proxy",
        freshness: raw.source?.freshness || "Live AIDP Workbench REST API",
        warnings: safeStrings(raw.source?.warnings)
      },
      workspaces
    };
  }

  function normalizeWorkspace(value) {
    const key = keyOf(value);
    if (!key) {
      return null;
    }
    const clusters = collectionItems(value.clusters).map(normalizeCluster).filter(Boolean);
    return {
      key,
      displayName: stringOrNull(value.displayName || value.name) || key,
      description: stringOrNull(value.description),
      state: normalizeState(value.state || value.lifecycleState),
      ownerDisplayName: stringOrNull(value.ownerDisplayName || value.createdByName || value.createdBy),
      timeUpdated: normalizeTimestamp(value.timeUpdated || value.updatedAt),
      clusters
    };
  }

  function normalizeCluster(value) {
    const key = keyOf(value);
    if (!key) {
      return null;
    }
    const activeResources = value.activeClusterResources || {};
    const existingCapacity = value.capacity || {};
    const libraries = collectionItems(value.libraries).map(normalizeLibrary).filter(Boolean);
    const notebookSessions = collectionItems(value.notebookSessions).map(normalizeNotebookSession).filter(Boolean);
    const workflows = collectionItems(value.workflows || value.jobs).map(normalizeWorkflow).filter(Boolean);
    const state = normalizeState(value.state || value.lifecycleState);
    return {
      key,
      displayName: stringOrNull(value.displayName || value.name) || key,
      description: stringOrNull(value.description),
      state,
      stateDetails: stringOrNull(value.stateDetails || value.lifecycleDetails),
      type: stringOrNull(value.type || value.sourceApi) || "USER",
      nodeType: stringOrNull(value.nodeType || value.shape),
      driverShape: stringOrNull(value.driverConfig?.driverShape || value.driverShape),
      driverShapeConfig: sanitizeShapeConfig(value.driverConfig?.driverShapeConfig || value.driverShapeConfig),
      active: ACTIVE_STATES.has(state),
      capacity: {
        nodes: numberOrNull(value.nodeCount ?? value.activeNodeCount ?? activeResources.activeExecutorCount ?? existingCapacity.nodes),
        executors: numberOrNull(activeResources.activeExecutorCount ?? existingCapacity.executors),
        ocpus: numberOrNull(activeResources.activeCores ?? value.ocpus ?? existingCapacity.ocpus),
        memoryInGBs: numberOrNull(activeResources.activeMemoryInGBs ?? value.memoryInGBs ?? existingCapacity.memoryInGBs),
        gpus: numberOrNull(activeResources.activeGpuCores ?? existingCapacity.gpus)
      },
      notebookSessions,
      workflows,
      libraries,
      specialLibraries: libraries.filter(isSpecialLibrary),
      timeUpdated: normalizeTimestamp(value.timeUpdated || value.updatedAt)
    };
  }

  function normalizeLibrary(value) {
    if (!value || typeof value !== "object") {
      return null;
    }
    const name = stringOrNull(value.name || value.displayName || value.libraryName || value.packageName ||
      value.pypi?.package || value.maven?.coordinates || value.uri || value.path);
    if (!name) {
      return null;
    }
    return {
      name,
      version: stringOrNull(value.version || value.pypi?.version),
      type: stringOrNull(value.type || value.libraryType || value.kind || value.source) || "LIBRARY",
      scope: stringOrNull(value.scope) || "cluster",
      status: normalizeState(value.status || value.state || value.lifecycleState || "INSTALLED")
    };
  }

  function normalizeNotebookSession(value) {
    if (!value || typeof value !== "object") {
      return null;
    }
    return {
      id: stringOrNull(value.id || value.sessionId || value.key),
      name: stringOrNull(value.name || value.notebookName || value.path || value.notebookPath) || "Notebook session",
      path: stringOrNull(value.path || value.notebookPath),
      state: normalizeState(value.state || value.status || "UNKNOWN")
    };
  }

  function normalizeWorkflow(value, recentRuns = []) {
    if (!value || typeof value !== "object") {
      return null;
    }
    const key = keyOf(value) || stringOrNull(value.id);
    const latestRun = latestRunForJob(key, recentRuns) || value.latestRun || {};
    return {
      key,
      displayName: stringOrNull(value.displayName || value.name) || key || "Workflow",
      state: normalizeState(value.state || value.status || value.lifecycleState || "UNKNOWN"),
      schedule: scheduleLabel(value),
      latestRunState: normalizeState(latestRun.state || latestRun.status || value.latestRunState || "UNKNOWN"),
      latestRunAt: normalizeTimestamp(latestRun.timeStarted || latestRun.timeCreated || value.latestRunAt)
    };
  }

  function withDerivedStats(snapshot) {
    const workspaces = collectionItems(snapshot.workspaces);
    const clusters = workspaces.flatMap((workspace) => collectionItems(workspace.clusters));
    const activeClusters = clusters.filter((cluster) => cluster.active || ACTIVE_STATES.has(normalizeState(cluster.state)));
    const totals = clusters.reduce((acc, cluster) => {
      acc.notebooks += collectionItems(cluster.notebookSessions).length;
      acc.workflows += collectionItems(cluster.workflows).length;
      acc.specialLibraries += collectionItems(cluster.specialLibraries || cluster.libraries).filter(isSpecialLibrary).length;
      acc.ocpus += Number(cluster.capacity?.ocpus || 0);
      acc.memoryInGBs += Number(cluster.capacity?.memoryInGBs || 0);
      return acc;
    }, {
      workspaces: workspaces.length,
      clusters: clusters.length,
      activeClusters: activeClusters.length,
      notebooks: 0,
      workflows: 0,
      specialLibraries: 0,
      ocpus: 0,
      memoryInGBs: 0
    });

    return {
      ...snapshot,
      totals
    };
  }

  function sessionMatchesCluster(session, clusterKey) {
    const candidates = [
      session.clusterKey,
      session.computeClusterKey,
      session.kernel?.clusterKey,
      session.kernel?.computeClusterKey,
      session.cluster?.key,
      session.computeCluster?.key,
      session.metadata?.clusterKey,
      session.metadata?.computeClusterKey
    ].map(stringOrNull);
    return candidates.includes(clusterKey);
  }

  function jobMatchesCluster(job, clusterKey) {
    const direct = [
      job.clusterKey,
      job.computeClusterKey,
      job.cluster?.key,
      job.computeCluster?.key,
      job.compute?.clusterKey,
      job.settings?.clusterKey,
      job.settings?.computeClusterKey
    ].map(stringOrNull).includes(clusterKey);
    const jobClusters = collectionItems(job.jobClusterConfigs || job.jobClusters || job.clusters || job.tasks);
    return direct || jobClusters.some((item) => [
      item.clusterKey,
      item.computeClusterKey,
      item.existingClusterKey,
      item.cluster?.key,
      item.computeCluster?.key,
      item.newCluster?.clusterKey
    ].map(stringOrNull).includes(clusterKey));
  }

  function latestRunForJob(jobKey, recentRuns) {
    if (!jobKey) {
      return null;
    }
    return collectionItems(recentRuns).find((run) =>
      [run.jobKey, run.job?.key, run.parentJobKey].map(stringOrNull).includes(jobKey)
    ) || null;
  }

  function scheduleLabel(value) {
    if (value.schedule) return String(value.schedule);
    if (value.scheduleExpression) return String(value.scheduleExpression);
    if (value.trigger?.scheduleExpression) return String(value.trigger.scheduleExpression);
    return value.isScheduled ? "Scheduled" : "Manual";
  }

  function isSpecialLibrary(library) {
    if (!library) {
      return false;
    }
    const name = String(library.name || "").toLowerCase();
    if (!name) {
      return false;
    }
    return !["pyspark", "spark", "python", "scala"].includes(name);
  }

  function sanitizeShapeConfig(value) {
    if (!value || typeof value !== "object") {
      return null;
    }
    return {
      ocpus: numberOrNull(value.ocpus ?? value.ocpuCount),
      memoryInGBs: numberOrNull(value.memoryInGBs ?? value.memoryInGbs ?? value.memoryGb),
      gpus: numberOrNull(value.gpus)
    };
  }

  function keyOf(value) {
    return stringOrNull(value?.key || value?.workspaceKey || value?.clusterKey || value?.jobKey || value?.id);
  }

  function collectionItems(value) {
    if (Array.isArray(value)) {
      return value;
    }
    return [
      value?.items,
      value?.data?.items,
      value?.collection?.items,
      value?.data?.collection?.items,
      value?.data,
      value?.resources
    ].find(Array.isArray) || [];
  }

  function normalizeState(value) {
    return String(value || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";
  }

  function normalizeTimestamp(value) {
    if (!value) {
      return null;
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  function numberOrNull(value) {
    const numberValue = Number(value);
    return Number.isFinite(numberValue) ? numberValue : null;
  }

  function stringOrNull(value) {
    const normalized = String(value || "").trim();
    return normalized || null;
  }

  function safeStrings(value) {
    return (Array.isArray(value) ? value : [])
      .filter((item) => typeof item === "string" && item.trim())
      .map((item) => item.trim());
  }

  function adapterError(code, message) {
    const error = new Error(message);
    error.name = "AIDPCustomerWorkbenchError";
    error.code = code;
    return error;
  }
})();
