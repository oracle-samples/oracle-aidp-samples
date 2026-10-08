#!/usr/bin/env node
// Static QA for the Ask AIDP Claude Code plugin.
//
// Checks (no live AIDP / OCI calls):
//   1. MCP stdio smoke test: initialize -> tools/list returns 43 tools,
//      server name is "ask-aidp", and nothing non-JSON is printed to stdout.
//   2. .mcp.json uses the wrapped mcpServers format with ${CLAUDE_PLUGIN_ROOT}.
//   3. No broken or absolute symlinks anywhere in the plugin tree.
//   4. `claude plugin validate --strict` on the plugin and the marketplace
//      manifest (skipped with a warning when the claude CLI is absent).
//   5. tools/call dry-run suite for the SDK-backed and inline-data tools
//      (upload, table-with-data, schema, git commit/push) so dry-run
//      regressions cannot pass QA.
//   6. CLI root request-model selection and Compute export REST transport
//      (mocked HTTP), plus CLI/REST lookup and REST planning for every
//      August/September 2026 addition.
//
// Exit code is non-zero if any check fails.
import { spawnSync } from 'node:child_process';
import { deepEqual } from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync, readlinkSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { computeExportTransportTest, exportFields, exportModel, requestBodyReferenceTest } from './qa-compute-export.mjs';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const MARKETPLACE_ROOT = path.resolve(PLUGIN_ROOT, '..', '..');
const SERVER = path.join(PLUGIN_ROOT, 'mcp', 'ask-aidp-server.mjs');
const EXPECTED_VERSION = '0.10.0';
const EXPECTED_TOOLS = 43;
const MAX_STDIO_BUFFER = 16 * 1024 * 1024;
const EXPECTED_TOOL_NAMES = [
  'aidp_cli',
  'aidp_check_connection',
  'aidp_notebook_workflow',
  'aidp_three_notebook_workflow',
  'aidp_upload_workspace_code',
  'aidp_create_git_folder',
  'aidp_git_commit_push',
  'aidp_git_pull',
  'aidp_git_get_repository',
  'aidp_git_operation_state',
  'aidp_git_list_branches',
  'aidp_git_create_branch',
  'aidp_git_checkout_branch',
  'aidp_git_list_diffs',
  'aidp_git_diff_detail',
  'aidp_git_merge',
  'aidp_git_rebase',
  'aidp_git_reset',
  'aidp_create_agent',
  'aidp_deploy_agent',
  'aidp_list_agents',
  'aidp_get_agent_session_trace',
  'aidp_create_ai_compute',
  'aidp_list_ai_computes',
  'aidp_update_ai_compute',
  'aidp_collect_logs',
  'aidp_track_runs',
  'aidp_create_schema',
  'aidp_create_delta_table',
  'aidp_create_table_with_data',
  'aidp_generate_csv_table_sql',
  'aidp_list_catalogs',
  'aidp_get_catalog',
  'aidp_create_catalog',
  'aidp_create_external_catalog',
  'aidp_auto_heal_workflow',
  'aidp_create_medallion_architecture',
  'aidp_create_bundle',
  'aidp_deploy_bundle',
  'aidp_command_help',
  'aidp_rest',
  'aidp_rest_api_reference',
  'aidp_cli_reference'
];

const results = [];
const record = (name, ok, detail) => { results.push({ name, ok, detail }); };

function pluginManifest() {
  const manifest = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, '.claude-plugin', 'plugin.json'), 'utf8'));
  record('plugin-name', manifest.name === 'ask-aidp', `plugin name=${manifest.name}`);
  record('plugin-version', manifest.version === EXPECTED_VERSION, `plugin version=${manifest.version} (expected ${EXPECTED_VERSION})`);
}

// 1 + ensure stdout is pure JSON-RPC.
function smokeTest() {
  const messages = [
    { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'qa-claude', version: '0' } } },
    { jsonrpc: '2.0', method: 'notifications/initialized', params: {} },
    { jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} }
  ].map((m) => JSON.stringify(m)).join('\n') + '\n';
  const proc = spawnSync(process.execPath, [SERVER], { input: messages, encoding: 'utf8', maxBuffer: MAX_STDIO_BUFFER });
  if (proc.status !== 0 && proc.status !== null) {
    record('mcp-smoke', false, `server exited with status ${proc.status}: ${proc.stderr?.slice(0, 500)}`);
    return;
  }
  const lines = (proc.stdout || '').trim().split(/\r?\n/).filter(Boolean);
  const parsed = [];
  for (const line of lines) {
    try { parsed.push(JSON.parse(line)); }
    catch { record('mcp-stdout-json', false, `non-JSON line on stdout: ${line.slice(0, 120)}`); return; }
  }
  record('mcp-stdout-json', true, 'all stdout lines are JSON');
  const init = parsed.find((m) => m.id === 1);
  const list = parsed.find((m) => m.id === 2);
  const serverName = init?.result?.serverInfo?.name;
  const serverVersion = init?.result?.serverInfo?.version;
  const tools = list?.result?.tools || [];
  const toolCount = tools.length;
  const toolNames = tools.map((tool) => tool.name);
  record('mcp-server-name', serverName === 'ask-aidp', `serverInfo.name=${serverName}`);
  record('mcp-server-version', serverVersion === EXPECTED_VERSION, `serverInfo.version=${serverVersion}`);
  record('mcp-tool-count', toolCount === EXPECTED_TOOLS, `tools/list returned ${toolCount} (expected ${EXPECTED_TOOLS})`);
  const missingTools = EXPECTED_TOOL_NAMES.filter((name) => !toolNames.includes(name));
  const unexpectedTools = toolNames.filter((name) => !EXPECTED_TOOL_NAMES.includes(name));
  record(
    'mcp-tool-set',
    missingTools.length === 0 && unexpectedTools.length === 0,
    missingTools.length || unexpectedTools.length
      ? `missing=${missingTools.join(',') || 'none'} unexpected=${unexpectedTools.join(',') || 'none'}`
      : 'all 43 expected tools present'
  );
  const tableTool = tools.find((tool) => tool.name === 'aidp_create_table_with_data');
  const headerSchemaIsSafe = tableTool?.inputSchema?.properties?.csvIncludeHeader?.const === false;
  record('table-header-schema', headerSchemaIsSafe, headerSchemaIsSafe ? 'csvIncludeHeader is fixed to false' : 'csvIncludeHeader must be const false');
}

function referenceCatalogs() {
  const cli = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, 'assets', 'aidp-cli-command-reference.json'), 'utf8'));
  record('cli-reference-counts', cli.groupCount === 18 && cli.commandCount === 256, `groups=${cli.groupCount}, commands=${cli.commandCount}`);
  const agentDeploy = cli.commands?.find((command) => command.fullName === 'aidp agent deploy');
  record('cli-agent-reference', Boolean(agentDeploy), agentDeploy ? 'aidp agent deploy present' : 'aidp agent deploy missing');

  const rest = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, 'assets', 'aidp-rest-api-reference.json'), 'utf8'));
  record('rest-reference-counts', rest.categoryCount === 19 && rest.operationCount === 271, `categories=${rest.categoryCount}, operations=${rest.operationCount}`);
}

// 2
function mcpConfig() {
  const cfg = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, '.mcp.json'), 'utf8'));
  const server = cfg?.mcpServers?.['ask-aidp'];
  const wrapped = Boolean(server);
  record('mcp-config-wrapped', wrapped, wrapped ? 'mcpServers.ask-aidp present' : 'missing mcpServers.ask-aidp');
  const usesRoot = JSON.stringify(server?.args || []).includes('${CLAUDE_PLUGIN_ROOT}');
  record('mcp-config-portable-path', usesRoot, usesRoot ? 'args use ${CLAUDE_PLUGIN_ROOT}' : 'args do not use ${CLAUDE_PLUGIN_ROOT}');
}

// 3
function symlinks() {
  const problems = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isSymbolicLink()) {
        const target = readlinkSync(full);
        const rel = path.relative(PLUGIN_ROOT, full);
        if (path.isAbsolute(target)) problems.push(`absolute: ${rel} -> ${target}`);
        else if (!existsSync(path.resolve(path.dirname(full), target))) problems.push(`broken: ${rel} -> ${target}`);
      } else if (entry.isDirectory()) {
        walk(full);
      }
    }
  };
  walk(PLUGIN_ROOT);
  record('symlinks-portable', problems.length === 0, problems.length ? problems.slice(0, 10).join('; ') : 'no broken or absolute symlinks');
}

// 4
function claudeValidate() {
  const runClaude = (args) => {
    const direct = spawnSync('claude', args, { encoding: 'utf8', maxBuffer: MAX_STDIO_BUFFER });
    if (direct.error?.code !== 'ENOEXEC' || process.platform === 'win32') return direct;
    return spawnSync(process.env.SHELL || '/bin/sh', ['-lc', 'exec claude "$@"', 'claude', ...args], {
      encoding: 'utf8',
      maxBuffer: MAX_STDIO_BUFFER
    });
  };

  const probe = runClaude(['--version']);
  if (probe.error?.code === 'ENOENT') {
    record('claude-validate', true, 'claude CLI not found on PATH; skipped (warning)');
    return;
  }
  if (probe.status !== 0) {
    record('claude-validate', false, `claude --version failed: ${(probe.stderr || probe.error?.message || '').trim()}`);
    return;
  }
  const plugin = runClaude(['plugin', 'validate', '--strict', PLUGIN_ROOT]);
  record('claude-validate-plugin', plugin.status === 0, (plugin.stdout || plugin.stderr || '').trim().split(/\r?\n/).pop());
  const marketplaceManifest = [
    path.join(PLUGIN_ROOT, '.claude-plugin', 'marketplace.json'),
    path.join(MARKETPLACE_ROOT, '.claude-plugin', 'marketplace.json')
  ].find(existsSync);
  if (marketplaceManifest) {
    const market = runClaude(['plugin', 'validate', marketplaceManifest]);
    record('claude-validate-marketplace', market.status === 0, (market.stdout || market.stderr || '').trim().split(/\r?\n/).pop());
  } else {
    record('claude-validate-marketplace', true, 'marketplace manifest not present in installed plugin cache; skipped');
  }
}

// 5
function callServer(calls) {
  const messages = [
    { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'qa-claude', version: '0' } } },
    { jsonrpc: '2.0', method: 'notifications/initialized', params: {} },
    ...calls
  ].map((m) => JSON.stringify(m)).join('\n') + '\n';
  const proc = spawnSync(process.execPath, [SERVER], { input: messages, encoding: 'utf8', maxBuffer: MAX_STDIO_BUFFER });
  return (proc.stdout || '').trim().split(/\r?\n/).filter(Boolean)
    .map((line) => { try { return JSON.parse(line); } catch { return null; } })
    .filter(Boolean);
}

function planFor(responses, id) {
  const response = responses.find((m) => m.id === id);
  if (!response) return { missing: true, detail: 'no response' };
  if (response.error) return { isError: true, detail: JSON.stringify(response.error).slice(0, 300) };
  const isError = response.result?.isError === true;
  const text = response.result?.content?.[0]?.text || '';
  let plan = null;
  try { plan = JSON.parse(text); } catch { plan = null; }
  return { isError, plan, detail: text.slice(0, 300) };
}

function toolDryRuns() {
  const responses = callServer([
    { jsonrpc: '2.0', id: 10, method: 'tools/call', params: { name: 'aidp_upload_workspace_code', arguments: { localPath: path.join(PLUGIN_ROOT, 'README.md'), workspacePath: '/Workspace/qa/README.md', config: { workspaceKey: 'ws-key-only' }, dryRun: true } } },
    { jsonrpc: '2.0', id: 11, method: 'tools/call', params: { name: 'aidp_create_table_with_data', arguments: { catalogKey: 'catalog-key', schemaKey: 'catalog.schema', displayName: 'qa_table_with_data', rows: [{ id: 1, amount: 10.5, status: 'new' }, { id: 2, amount: 22.0, status: 'posted' }], dryRun: true } } },
    { jsonrpc: '2.0', id: 12, method: 'tools/call', params: { name: 'aidp_create_schema', arguments: { catalogName: 'qa_catalog', displayName: 'qa_schema', dryRun: true } } },
    { jsonrpc: '2.0', id: 13, method: 'tools/call', params: { name: 'aidp_git_commit_push', arguments: { gitRepositoryKey: 'git-repository-key', gitFolderPath: '/Workspace/git/sample', branchName: 'main', files: ['notebooks/01_ingest.ipynb'], commitMessage: 'QA commit', config: { endpoint: 'https://aidp.example.com', instanceId: 'ocid1.aidataplatform.oc1..example', workspaceKey: 'workspace-key' }, dryRun: true } } },
    { jsonrpc: '2.0', id: 14, method: 'tools/call', params: { name: 'aidp_create_agent', arguments: { displayName: 'qa_agent', pathInfo: '/Workspace/agents/qa_agent', agentType: 'CANVAS', entryFilePath: 'agent.py', config: { workspaceKey: 'workspace-key' }, dryRun: true } } },
    { jsonrpc: '2.0', id: 15, method: 'tools/call', params: { name: 'aidp_create_ai_compute', arguments: { displayName: 'qa_ai_compute', driverShape: 'VM.Standard.E5.Flex', ocpus: 2, memoryInGBs: 32, minReplicas: 1, maxReplicas: 2, config: { workspaceKey: 'workspace-key' }, dryRun: true } } },
    { jsonrpc: '2.0', id: 16, method: 'tools/call', params: { name: 'aidp_rest_api_reference', arguments: {} } },
    { jsonrpc: '2.0', id: 17, method: 'tools/call', params: { name: 'aidp_rest', arguments: { method: 'GET', path: '/20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/clusters', query: { type: 'AI_COMPUTE' }, config: { endpoint: 'https://aidp.example.com', instanceId: 'ocid1.aidataplatform.oc1..example', workspaceKey: 'workspace-key' }, dryRun: true } } }
  ]);

  const upload = planFor(responses, 10);
  const uploadOk = !upload.isError && upload.plan?.entryCount === 1
    && upload.plan?.entries?.[0]?.request?.aiDataPlatformId === '<aiDataPlatformId>'
    && String(upload.plan?.entries?.[0]?.request?.createWorkspaceObjectDetails || '').startsWith('<base64:');
  record('dryrun-upload-workspace-code', uploadOk, uploadOk ? 'workspaceKey-only dry-run returns placeholder request without reading files' : `upload dry-run mismatch: ${upload.detail}`);

  const table = planFor(responses, 11);
  const tableOk = !table.isError
    && JSON.stringify(table.plan?.body?.selectedColumns) === JSON.stringify(['_c0', '_c1', '_c2'])
    && table.plan?.body?.tableFields?.some((field) => field.fieldName === 'amount')
    && /(^|\n)1,10\.5,new(\n|$)/.test(table.plan?.dataPreview || '')
    && !/id,amount,status/.test(table.plan?.dataPreview || '');
  record('dryrun-table-with-data', tableOk, tableOk ? 'inline rows use positional selectedColumns + headerless CSV; tableFields carry names' : `table dry-run mismatch: ${table.detail}`);

  const schema = planFor(responses, 12);
  const schemaOk = !schema.isError && String(schema.plan?.command || '').includes('schema create');
  record('dryrun-create-schema', schemaOk, schemaOk ? 'schema create dry-run returns command preview' : `schema dry-run mismatch: ${schema.detail}`);

  const git = planFor(responses, 13);
  const gitOk = !git.isError && git.plan?.implementation === 'aidp-typescript-client GitClient'
    && git.plan?.request?.commitPushDetails?.commitMessage === 'QA commit';
  record('dryrun-git-commit-push', gitOk, gitOk ? 'git commit/push dry-run uses SDK GitClient with commit message' : `git dry-run mismatch: ${git.detail}`);

  const agent = planFor(responses, 14);
  const agentOk = !agent.isError && String(agent.plan?.command || '').includes('agent create workspace-key')
    && agent.plan?.body?.type === 'CANVAS' && agent.plan?.body?.entryFilePath === 'agent.py';
  record('dryrun-create-agent', agentOk, agentOk ? 'agent create dry-run returns typed command and body' : `agent dry-run mismatch: ${agent.detail}`);

  const aiCompute = planFor(responses, 15);
  const aiComputeOk = !aiCompute.isError && String(aiCompute.plan?.command || '').includes('cluster create workspace-key')
    && aiCompute.plan?.body?.type === 'AI_COMPUTE' && aiCompute.plan?.body?.replicaConfig?.maxReplica === 2;
  record('dryrun-create-ai-compute', aiComputeOk, aiComputeOk ? 'AI Compute dry-run returns typed cluster body' : `AI Compute dry-run mismatch: ${aiCompute.detail}`);

  const restReference = planFor(responses, 16);
  const restReferenceOk = !restReference.isError && restReference.plan?.categoryCount === 19
    && restReference.plan?.operationCount === 271 && restReference.plan?.apiVersion === '20260430';
  record('rest-reference-summary', restReferenceOk, restReferenceOk ? 'REST reference exposes 19 categories and 271 operations' : `REST reference mismatch: ${restReference.detail}`);

  const rest = planFor(responses, 17);
  const restOk = !rest.isError && String(rest.plan?.url || '').includes('/20260430/aiDataPlatforms/ocid1.aidataplatform.oc1..example/workspaces/workspace-key/clusters?type=AI_COMPUTE');
  record('dryrun-rest', restOk, restOk ? 'REST dry-run expands configured identifiers and query' : `REST dry-run mismatch: ${rest.detail}`);
}

// 6
async function computeExportChecks() {
  try {
    requestBodyReferenceTest();
    record('cli-root-request-model', true, 'root model selected from manifest regardless of README model order');
  } catch (error) {
    record('cli-root-request-model', false, error.message);
  }
  try {
    await computeExportTransportTest();
    record('rest-compute-export-transport', true, 'export defaults to Accept: application/x-yaml; dry-run and sent headers match');
  } catch (error) {
    record('rest-compute-export-transport', false, error.message);
  }
}

function latestOperationChecks() {
  const basePath = '/20260430/aiDataPlatforms/{aiDataPlatformId}';
  const workspacePath = `${basePath}/workspaces/{workspaceKey}`;
  const clusterPath = `${workspacePath}/clusters/{clusterKey}`;
  const config = {
    endpoint: 'https://aidp.example.com',
    instanceId: 'ocid1.aidataplatform.oc1..example',
    workspaceKey: 'workspace-key', clusterKey: 'cluster-key'
  };
  const latestOperations = [
    ['bundle', 'publish-bundle-action', 'Bundle', 'POST', `${workspacePath}/actions/publishBundle`],
    ['bundle', 'fetch-publish-status-action', 'Bundle', 'POST', `${workspacePath}/actions/getBundlePublishStatus`],
    ['cluster', 'clone-compute', 'Cluster', 'POST', `${clusterPath}/actions/cloneCompute`],
    ['cluster', 'export-compute-configuration', 'Cluster', 'POST', `${clusterPath}/actions/exportComputeConfiguration`],
    ['cluster', 'get-compute-configuration', 'Cluster', 'GET', `${clusterPath}/actions/getComputeConfiguration`],
    ['cluster', 'import-compute-configuration', 'Cluster', 'POST', `${clusterPath}/actions/importComputeConfiguration`],
    ['cluster', 'search-maven-packages', 'Cluster', 'GET', `${clusterPath}/mavenPackages`],
    ['data-lineage', 'export', 'DataLineage', 'POST', `${basePath}/actions/exportLineage`],
    ['data-lineage', 'fetch-entity-lineage', 'DataLineage', 'POST', `${basePath}/actions/fetchLineage`],
    ['volume', 'upload-and-extract-volume-zip', 'Volume', 'POST', `${basePath}/volumes/{volumeKey}/actions/uploadAndExtractZip`],
    ['volume', 'zip-and-download-volume-folder', 'Volume', 'POST', `${basePath}/volumes/{volumeKey}/actions/zipAndDownloadFolder`],
    ['workflow', 'list-task-run-retries', 'Workflow', 'GET', `${workspacePath}/taskRuns/{taskRunKey}/retries`],
    ['workspace-object', 'upload-and-extract-workspace-zip', 'WorkspaceObject', 'POST', `${workspacePath}/actions/uploadAndExtractZip`],
    ['workspace-object', 'zip-and-download-workspace-folder', 'WorkspaceObject', 'POST', `${workspacePath}/actions/zipAndDownloadFolder`]
  ];
  // Three calls per operation: CLI reference, REST reference, REST dry-run plan.
  const calls = latestOperations.flatMap(([group, command, category, method, endpointPath], index) => {
    const requestPath = endpointPath.replace('{volumeKey}', 'volume-key').replace('{taskRunKey}', 'task-run-key');
    const id = 100 + index * 3;
    return [
      { jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'aidp_cli_reference', arguments: { group, command } } },
      { jsonrpc: '2.0', id: id + 1, method: 'tools/call', params: { name: 'aidp_rest_api_reference', arguments: { category, search: endpointPath } } },
      { jsonrpc: '2.0', id: id + 2, method: 'tools/call', params: { name: 'aidp_rest', arguments: { method, path: requestPath, dryRun: true, config } } }
    ];
  });
  const wrongMethodId = 99;
  calls.push({ jsonrpc: '2.0', id: wrongMethodId, method: 'tools/call', params: { name: 'aidp_rest', arguments: { method: 'GET', path: `${workspacePath}/actions/getBundlePublishStatus`, dryRun: true, config } } });
  const responses = callServer(calls);

  const failures = [];
  latestOperations.forEach(([group, command, , method, endpointPath], index) => {
    const id = 100 + index * 3;
    const fullName = `aidp ${group} ${command}`;
    const reference = planFor(responses, id);
    const commandRef = reference.plan?.command;
    if (reference.isError || commandRef?.fullName !== fullName || !String(commandRef?.usage || '').startsWith(fullName)) {
      failures.push(`CLI reference: ${fullName}`);
    } else if (command === 'export-compute-configuration') {
      try {
        deepEqual(commandRef.bodyModel, exportModel);
        deepEqual(commandRef.bodyFields.map((field) => field.name).sort(), [...exportFields].sort());
      } catch {
        failures.push('export default lookup must expose the root request model');
      }
    }
    const lookup = planFor(responses, id + 1);
    if (lookup.isError || !lookup.plan?.matches?.some((op) => op.method === method && op.path === endpointPath)) {
      failures.push(`REST reference: ${method} ${endpointPath}`);
    }
    const plan = planFor(responses, id + 2);
    const expectedPath = endpointPath.replace('{volumeKey}', 'volume-key').replace('{taskRunKey}', 'task-run-key')
      .replace('{aiDataPlatformId}', config.instanceId).replace('{workspaceKey}', config.workspaceKey).replace('{clusterKey}', config.clusterKey);
    const expectedAccept = command === 'export-compute-configuration' ? 'application/x-yaml' : 'application/json';
    let planPath = '';
    try { planPath = new URL(plan.plan?.url).pathname; } catch { planPath = ''; }
    if (plan.isError || plan.plan?.method !== method || planPath !== expectedPath || plan.plan?.headers?.accept !== expectedAccept) {
      failures.push(`REST dry-run: ${method} ${endpointPath}`);
    }
  });
  record('latest-operations', failures.length === 0, failures.length ? failures.join('; ') : `${latestOperations.length} additions resolve through CLI reference, REST reference, and REST planning`);

  const wrongMethod = planFor(responses, wrongMethodId);
  const wrongMethodRejected = (wrongMethod.isError || Boolean(responses.find((m) => m.id === wrongMethodId)?.error))
    && /method and path do not match a documented AIDP REST operation/.test(wrongMethod.detail || '');
  record('rest-bundle-status-method', wrongMethodRejected, wrongMethodRejected ? 'GET getBundlePublishStatus rejected; operation reference requires POST' : `expected rejection, got: ${wrongMethod.detail}`);
}

pluginManifest();
smokeTest();
mcpConfig();
symlinks();
claudeValidate();
referenceCatalogs();
toolDryRuns();
await computeExportChecks();
latestOperationChecks();

const failed = results.filter((r) => !r.ok);
for (const r of results) console.log(`${r.ok ? 'PASS' : 'FAIL'}  ${r.name}: ${r.detail}`);
console.log(JSON.stringify({ ok: failed.length === 0, passed: results.length - failed.length, failed: failed.length }, null, 2));
process.exit(failed.length === 0 ? 0 : 1);
