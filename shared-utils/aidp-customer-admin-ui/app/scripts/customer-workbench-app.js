(function () {
  "use strict";

  const workbenchApi = window.AIDP.customerWorkbench;
  const searchParams = new URLSearchParams(window.location.search);
  const liveApiEnabled = searchParams.get("liveApi") === "1";
  const adapter = liveApiEnabled ? workbenchApi.createCustomerWorkbenchAdapter() : null;
  const sessionAdapter = liveApiEnabled ? workbenchApi.createOciSessionAdapter() : null;
  const fixture = window.AIDP_CUSTOMER_WORKBENCH_FIXTURE;

  const state = {
    snapshot: workbenchApi.normalizeWorkbenchSnapshot(fixture),
    sessionStatus: null,
    noticeMessage: null,
    selectedWorkspaceKey: null,
    filters: {
      query: "",
      status: "all"
    },
    loading: false
  };

  const els = {
    aiDataPlatformId: document.querySelector("#aiDataPlatformId"),
    checkOciSession: document.querySelector("#checkOciSession"),
    clusterGrid: document.querySelector("#clusterGrid"),
    customerNotice: document.querySelector("#customerNotice"),
    emptyState: document.querySelector("#emptyState"),
    form: document.querySelector("#customerWorkbenchForm"),
    lastUpdated: document.querySelector("#lastUpdated"),
    ociSessionCommand: document.querySelector("#ociSessionCommand"),
    ociSessionMessage: document.querySelector("#ociSessionMessage"),
    ociSessionPanel: document.querySelector("#ociSessionPanel"),
    ociSessionState: document.querySelector("#ociSessionState"),
    queryFilter: document.querySelector("#queryFilter"),
    region: document.querySelector("#region"),
    sourceBadge: document.querySelector("#sourceBadge"),
    statusFilter: document.querySelector("#statusFilter"),
    totals: document.querySelector("#totals"),
    workspaceList: document.querySelector("#workspaceList"),
    workspaceTitle: document.querySelector("#workspaceTitle")
  };

  init();

  function init() {
    els.aiDataPlatformId.value = searchParams.get("aiDataPlatformId") || state.snapshot.aiDataPlatformId || "";
    els.region.value = searchParams.get("region") || state.snapshot.region || "phx";
    state.selectedWorkspaceKey = state.snapshot.workspaces[0]?.key || null;
    bindEvents();
    render();
    if (sessionAdapter) {
      checkOciSession({ silent: true });
    }
  }

  function bindEvents() {
    els.form.addEventListener("submit", async (event) => {
      event.preventDefault();
      await refresh();
    });
    els.checkOciSession.addEventListener("click", async () => {
      await checkOciSession();
    });
    els.region.addEventListener("change", () => {
      if (sessionAdapter) {
        checkOciSession({ silent: true });
      }
    });
    els.queryFilter.addEventListener("input", () => {
      state.filters.query = els.queryFilter.value.trim().toLowerCase();
      renderWorkspaceDetails();
    });
    els.statusFilter.addEventListener("change", () => {
      state.filters.status = els.statusFilter.value;
      renderWorkspaceDetails();
    });
  }

  async function refresh() {
    const aiDataPlatformId = els.aiDataPlatformId.value.trim();
    const region = els.region.value.trim();
    if (!adapter) {
      showNotice("Live REST lookup is disabled. Open this page with ?liveApi=1 through the local read-only proxy to query AIDP.");
      return;
    }
    if (!aiDataPlatformId || !region) {
      showNotice("AIDP OCID and region are required for live lookup.");
      return;
    }

    showNotice("Checking OCI CLI session...");
    const sessionStatus = await checkOciSession({ silent: true });
    if (sessionStatus?.required && !sessionStatus.signedIn) {
      showNotice(sessionStatus.message || "OCI CLI session authentication is required.");
      return;
    }

    state.loading = true;
    showNotice("Loading workspaces and compute usage...");
    renderLoading();
    try {
      state.snapshot = await adapter.getWorkbenchSnapshot({ aiDataPlatformId, region });
      state.selectedWorkspaceKey = state.snapshot.workspaces[0]?.key || null;
      state.loading = false;
      state.noticeMessage = null;
      render();
    } catch (error) {
      state.loading = false;
      showNotice(error.message || "Workbench lookup failed.");
      render();
    }
  }

  function render() {
    renderHeader();
    renderTotals();
    renderOciSessionPanel();
    renderWorkspaceList();
    renderWorkspaceDetails();
    renderNotice();
  }

  function renderHeader() {
    els.sourceBadge.textContent = state.snapshot.source?.adapter === "fixture"
      ? "Fixture"
      : "Live REST";
    els.lastUpdated.textContent = state.snapshot.fetchedAt
      ? `Updated ${formatDateTime(state.snapshot.fetchedAt)}`
      : "Updated -";
  }

  function renderNotice(message) {
    if (message !== undefined) {
      state.noticeMessage = message;
    }
    const warnings = state.noticeMessage ? [state.noticeMessage] : state.snapshot.source?.warnings || [];
    els.customerNotice.hidden = warnings.length === 0;
    els.customerNotice.textContent = warnings.join(" ");
  }

  function showNotice(message) {
    state.noticeMessage = message;
    renderNotice();
  }

  async function checkOciSession({ silent = false } = {}) {
    if (!sessionAdapter) {
      state.sessionStatus = null;
      renderOciSessionPanel();
      return null;
    }

    state.sessionStatus = {
      required: true,
      signedIn: null,
      message: "Checking OCI CLI session...",
      command: ""
    };
    renderOciSessionPanel();
    try {
      state.sessionStatus = await sessionAdapter.getStatus({
        region: els.region.value.trim()
      });
    } catch (error) {
      state.sessionStatus = {
        required: true,
        signedIn: false,
        message: error.message || "OCI CLI session status lookup failed.",
        command: ""
      };
    }
    renderOciSessionPanel();
    if (!silent && state.sessionStatus?.message) {
      showNotice(state.sessionStatus.message);
    }
    return state.sessionStatus;
  }

  function renderOciSessionPanel() {
    if (!liveApiEnabled) {
      els.ociSessionPanel.hidden = true;
      return;
    }

    const status = state.sessionStatus || {
      signedIn: null,
      message: "OCI CLI session status has not been checked.",
      command: ""
    };
    const signedIn = status.signedIn === true;
    const pending = status.signedIn === null || status.signedIn === undefined;
    els.ociSessionPanel.hidden = false;
    els.ociSessionState.className = `session-state ${signedIn ? "success" : pending ? "neutral" : "warning"}`;
    els.ociSessionState.textContent = signedIn ? "Signed in" : pending ? "Checking" : "Sign-in required";
    els.ociSessionMessage.textContent = status.message || "";
    els.ociSessionCommand.hidden = signedIn || !status.command;
    els.ociSessionCommand.textContent = status.command || "";
  }

  function renderTotals() {
    const totals = state.snapshot.totals || {};
    els.totals.replaceChildren(
      metric("Workspaces", totals.workspaces),
      metric("Clusters", totals.clusters),
      metric("Active", totals.activeClusters),
      metric("Notebooks", totals.notebooks),
      metric("Workflows", totals.workflows),
      metric("Special libraries", totals.specialLibraries),
      metric("Active OCPUs", totals.ocpus),
      metric("Active memory", `${totals.memoryInGBs || 0} GB`)
    );
  }

  function renderWorkspaceList() {
    els.workspaceList.replaceChildren(...state.snapshot.workspaces.map((workspace) => {
      const button = document.createElement("button");
      const isSelected = workspace.key === state.selectedWorkspaceKey;
      button.type = "button";
      button.className = `workspace-item${isSelected ? " active" : ""}`;
      button.setAttribute("aria-pressed", String(isSelected));
      button.append(
        textElement("strong", workspace.displayName),
        textElement("span", `${workspace.clusters.length} clusters · ${activeClusterCount(workspace)} active`),
        textElement("em", workspace.state)
      );
      button.addEventListener("click", () => {
        state.selectedWorkspaceKey = workspace.key;
        renderWorkspaceList();
        renderWorkspaceDetails();
      });
      return button;
    }));
  }

  function renderWorkspaceDetails() {
    const workspace = selectedWorkspace();
    if (!workspace) {
      els.workspaceTitle.textContent = "No workspace selected";
      els.clusterGrid.replaceChildren();
      els.emptyState.hidden = false;
      return;
    }

    els.workspaceTitle.textContent = workspace.displayName;
    const clusters = filteredClusters(workspace);
    els.emptyState.hidden = clusters.length > 0;
    els.clusterGrid.replaceChildren(...clusters.map(clusterPanel));
  }

  function renderLoading() {
    els.clusterGrid.replaceChildren(emptyState("Loading AIDP workspace usage..."));
  }

  function selectedWorkspace() {
    return state.snapshot.workspaces.find((workspace) => workspace.key === state.selectedWorkspaceKey) ||
      state.snapshot.workspaces[0] ||
      null;
  }

  function filteredClusters(workspace) {
    return workspace.clusters.filter((cluster) => {
      const matchesQuery = !state.filters.query ||
        [cluster.displayName, cluster.key, cluster.nodeType, cluster.state]
          .some((value) => String(value || "").toLowerCase().includes(state.filters.query));
      const matchesStatus = state.filters.status === "all" ||
        (state.filters.status === "active" && cluster.active) ||
        (state.filters.status === "inactive" && !cluster.active);
      return matchesQuery && matchesStatus;
    });
  }

  function clusterPanel(cluster) {
    const panel = document.createElement("article");
    panel.className = "cluster-panel";
    panel.append(
      clusterHeader(cluster),
      clusterCapacity(cluster),
      usageGrid(cluster),
      librarySection(cluster)
    );
    return panel;
  }

  function clusterHeader(cluster) {
    const header = document.createElement("div");
    header.className = "cluster-header";
    header.append(
      textGroup(cluster.displayName, cluster.key),
      badge(cluster.state, stateTone(cluster.state))
    );
    return header;
  }

  function clusterCapacity(cluster) {
    const capacity = cluster.capacity || {};
    const grid = document.createElement("div");
    grid.className = "capacity-grid";
    grid.append(
      field("Node type", cluster.nodeType || "-"),
      field("Executors", valueOrDash(capacity.executors ?? capacity.nodes)),
      field("OCPUs", valueOrDash(capacity.ocpus)),
      field("Memory", capacity.memoryInGBs === null || capacity.memoryInGBs === undefined ? "-" : `${capacity.memoryInGBs} GB`),
      field("Driver", driverLabel(cluster))
    );
    return grid;
  }

  function usageGrid(cluster) {
    const grid = document.createElement("div");
    grid.className = "usage-grid";
    grid.append(
      usageBlock("Attached notebooks", cluster.notebookSessions, notebookLabel),
      usageBlock("Workflows", cluster.workflows, workflowLabel)
    );
    return grid;
  }

  function librarySection(cluster) {
    const section = document.createElement("section");
    section.className = "library-section";
    const libraries = cluster.specialLibraries || [];
    section.append(
      textElement("h4", "Special libraries"),
      libraries.length === 0
        ? emptyState("No cluster scoped libraries reported.")
        : tagList(libraries.map(libraryLabel))
    );
    return section;
  }

  function usageBlock(title, items, labeler) {
    const block = document.createElement("section");
    block.className = "usage-block";
    block.append(textElement("h4", `${title} (${items.length})`));
    block.append(items.length === 0
      ? emptyState("None reported.")
      : compactList(items.map(labeler)));
    return block;
  }

  function textGroup(title, subtitle) {
    const group = document.createElement("div");
    group.className = "text-group";
    group.append(textElement("h3", title), textElement("span", subtitle));
    return group;
  }

  function metric(label, value) {
    const item = document.createElement("div");
    item.className = "metric";
    item.append(textElement("span", label), textElement("strong", valueOrDash(value)));
    return item;
  }

  function field(label, value) {
    const item = document.createElement("div");
    item.className = "field";
    item.append(textElement("span", label), textElement("strong", valueOrDash(value)));
    return item;
  }

  function badge(label, tone) {
    const item = document.createElement("span");
    item.className = `badge ${tone}`;
    item.textContent = label || "UNKNOWN";
    return item;
  }

  function compactList(items) {
    const list = document.createElement("ul");
    list.className = "compact-list";
    items.forEach((item) => {
      const li = document.createElement("li");
      li.textContent = item;
      list.appendChild(li);
    });
    return list;
  }

  function tagList(items) {
    const list = document.createElement("div");
    list.className = "tag-list";
    items.forEach((item) => list.appendChild(textElement("span", item)));
    return list;
  }

  function notebookLabel(session) {
    return [session.name, session.state].filter(Boolean).join(" · ");
  }

  function workflowLabel(workflow) {
    return [workflow.displayName, workflow.latestRunState, workflow.schedule].filter(Boolean).join(" · ");
  }

  function libraryLabel(library) {
    return [library.name, library.version, library.status].filter(Boolean).join(" · ");
  }

  function driverLabel(cluster) {
    const config = cluster.driverShapeConfig || {};
    const shape = cluster.driverShape || "-";
    const size = [config.ocpus ? `${config.ocpus} OCPU` : null, config.memoryInGBs ? `${config.memoryInGBs} GB` : null]
      .filter(Boolean)
      .join(" / ");
    return size ? `${shape} · ${size}` : shape;
  }

  function activeClusterCount(workspace) {
    return workspace.clusters.filter((cluster) => cluster.active).length;
  }

  function stateTone(state) {
    const normalized = String(state || "").toUpperCase();
    if (normalized === "ACTIVE") return "success";
    if (normalized === "FAILED") return "danger";
    if (["STARTING", "STOPPING", "UPDATING", "RESTARTING"].includes(normalized)) return "warning";
    return "neutral";
  }

  function formatDateTime(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? "-"
      : new Intl.DateTimeFormat(undefined, {
          dateStyle: "medium",
          timeStyle: "short"
        }).format(date);
  }

  function valueOrDash(value) {
    return value === undefined || value === null || value === "" ? "-" : String(value);
  }

  function emptyState(message) {
    return textElement("p", "empty", message);
  }

  function textElement(tag, classNameOrText, maybeText) {
    const element = document.createElement(tag);
    if (maybeText === undefined) {
      element.textContent = classNameOrText;
    } else {
      element.className = classNameOrText;
      element.textContent = maybeText;
    }
    return element;
  }
})();
