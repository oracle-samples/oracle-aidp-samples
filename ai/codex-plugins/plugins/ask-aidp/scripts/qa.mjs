#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { deepEqual, rejects } from 'node:assert/strict';
import { chmodSync, existsSync, mkdirSync, readFileSync, readdirSync, readlinkSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { computeExportTransportTest, exportFields, exportModel, requestBodyReferenceTest } from './qa-compute-export.mjs';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const SERVER = path.join(PLUGIN_ROOT, 'mcp', 'ask-aidp-server.mjs');
const EXPECTED_VERSION = '0.10.0';
const EXPECTED_TOOLS = 43;
const args = new Set(process.argv.slice(2));
const live = args.has('--live');
const workflow = args.has('--workflow');

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function pluginManifestTest() {
  const manifestPath = path.join(PLUGIN_ROOT, '.codex-plugin', 'plugin.json');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  assert(manifest.name === 'ask-aidp', 'plugin name mismatch');
  assert(manifest.version === EXPECTED_VERSION, `plugin version mismatch: expected ${EXPECTED_VERSION}, got ${manifest.version}`);
  assert(manifest.mcpServers === './.mcp.json', 'missing mcpServers manifest entry');
  assert(existsSync(path.join(PLUGIN_ROOT, '.mcp.json')), 'missing .mcp.json');
  assert(existsSync(SERVER), 'missing MCP server');
  assert(existsSync(path.join(PLUGIN_ROOT, 'skills', 'ask-aidp', 'SKILL.md')), 'missing skill');
}

function mcpConfigTest() {
  const config = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, '.mcp.json'), 'utf8'));
  const server = config?.mcpServers?.['ask-aidp'];
  assert(server, '.mcp.json must use the wrapped mcpServers.ask-aidp format');
  assert(server.command === 'node', `.mcp.json command should be node, got ${server.command}`);
  assert(server.cwd === '.', `.mcp.json cwd should be ".", got ${server.cwd}`);
  const serverArgs = server.args || [];
  assert(
    serverArgs.some((arg) => arg.includes('mcp/ask-aidp-server.mjs')),
    '.mcp.json args must point at mcp/ask-aidp-server.mjs'
  );
  for (const arg of serverArgs) {
    assert(!path.isAbsolute(arg), `.mcp.json args must stay relative, got absolute ${arg}`);
  }
}

function marketplaceManifestTest() {
  const nestedRoot = path.resolve(PLUGIN_ROOT, '..', '..');
  const repositoryRoot = path.resolve(PLUGIN_ROOT, '..', '..', '..', '..');
  const nestedPath = path.join(nestedRoot, '.agents', 'plugins', 'marketplace.json');
  const repositoryPath = path.join(repositoryRoot, '.agents', 'plugins', 'marketplace.json');
  const hasNestedManifest = existsSync(nestedPath);
  const hasRepositoryManifest = existsSync(repositoryPath);

  // Packaged and installed plugin copies do not include either marketplace root.
  if (!hasNestedManifest && !hasRepositoryManifest) return;
  assert(hasNestedManifest, 'repository checkout is missing the nested ai/codex-plugins marketplace manifest');
  assert(hasRepositoryManifest, 'repository checkout is missing the repository-root marketplace manifest');

  const loadManifest = (manifestPath, marketplaceRoot, label) => {
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    assert(manifest.name === 'oracle-aidp-codex', `unexpected ${label} marketplace name: ${manifest.name}`);
    assert(Array.isArray(manifest.plugins), `${label} marketplace plugins must be an array`);

    const resolvedPlugins = new Map();
    for (const entry of manifest.plugins) {
      assert(entry?.name, `${label} marketplace contains a plugin without a name`);
      assert(!resolvedPlugins.has(entry.name), `${label} marketplace contains duplicate plugin ${entry.name}`);
      assert(entry.source?.source === 'local', `${label} marketplace plugin ${entry.name} should use a local source`);
      const resolved = path.resolve(marketplaceRoot, entry.source.path || '.');
      assert(
        existsSync(path.join(resolved, '.codex-plugin', 'plugin.json')),
        `${label} marketplace plugin ${entry.name} path ${entry.source.path} does not resolve to a Codex plugin`
      );
      resolvedPlugins.set(entry.name, resolved);
    }
    return { manifest, resolvedPlugins };
  };

  const nested = loadManifest(nestedPath, nestedRoot, 'nested');
  const repository = loadManifest(repositoryPath, repositoryRoot, 'repository-root');
  const nestedPlugins = nested.resolvedPlugins;
  const repositoryPlugins = repository.resolvedPlugins;
  const nestedNames = [...nestedPlugins.keys()].sort();
  const repositoryNames = [...repositoryPlugins.keys()].sort();
  assert(
    JSON.stringify(nestedNames) === JSON.stringify(repositoryNames),
    'repository-root and nested marketplace manifests must list the same plugins'
  );
  assert(
    JSON.stringify(nested.manifest.interface) === JSON.stringify(repository.manifest.interface),
    'repository-root and nested marketplace interfaces must match'
  );
  for (const pluginName of nestedNames) {
    const nestedEntry = nested.manifest.plugins.find((entry) => entry.name === pluginName);
    const repositoryEntry = repository.manifest.plugins.find((entry) => entry.name === pluginName);
    assert(
      nestedPlugins.get(pluginName) === repositoryPlugins.get(pluginName),
      `repository-root and nested marketplace entries for ${pluginName} must resolve to the same plugin`
    );
    assert(
      nestedEntry.category === repositoryEntry.category &&
        JSON.stringify(nestedEntry.policy) === JSON.stringify(repositoryEntry.policy),
      `repository-root and nested marketplace metadata for ${pluginName} must match`
    );
  }
  assert(nestedPlugins.get('ask-aidp') === PLUGIN_ROOT, 'marketplace ask-aidp entries must resolve to this plugin');
}

function symlinkTest() {
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
  assert(problems.length === 0, `non-portable symlinks: ${problems.slice(0, 10).join('; ')}`);
}

function startServer(extraEnv = {}) {
  const child = spawn(process.execPath, [SERVER], {
    cwd: PLUGIN_ROOT,
    env: { ...process.env, ...extraEnv },
    stdio: ['pipe', 'pipe', 'pipe']
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
  child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
  let nextId = 1;
  const pending = new Map();
  const nonJsonLines = [];
  const timer = setInterval(() => {
    let index;
    while ((index = stdout.indexOf('\n')) >= 0) {
      const line = stdout.slice(0, index);
      stdout = stdout.slice(index + 1);
      if (!line.trim()) continue;
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        nonJsonLines.push(line.slice(0, 200));
        continue;
      }
      if (pending.has(message.id)) {
        const { resolve, reject } = pending.get(message.id);
        pending.delete(message.id);
        if (message.error) reject(new Error(`${message.error.message}\n${JSON.stringify(message.error.data || {})}`));
        else resolve(message.result);
      }
    }
  }, 10);

  return {
    async request(method, params = {}) {
      const id = nextId++;
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
      return await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Timed out waiting for ${method}. stderr=${stderr}`));
        }, method === 'tools/call' ? 2_000_000 : 30_000);
        pending.set(id, {
          resolve: (value) => { clearTimeout(timeout); resolve(value); },
          reject: (error) => { clearTimeout(timeout); reject(error); }
        });
      });
    },
    nonJsonLines() {
      return nonJsonLines;
    },
    stop() {
      clearInterval(timer);
      child.stdin.end();
      child.kill('SIGTERM');
    }
  };
}

function toolText(result) {
  return (result.content || []).map((item) => item.text || '').join('\n');
}

async function main() {
  pluginManifestTest();
  mcpConfigTest();
  marketplaceManifestTest();
  symlinkTest();
  requestBodyReferenceTest();
  await computeExportTransportTest();
  const env = {};
  const localAidp = path.resolve(PLUGIN_ROOT, '..', '..', 'samples', 'npm-cli', 'node_modules', '.bin', process.platform === 'win32' ? 'aidp.cmd' : 'aidp');
  if (!process.env.AIDP_CLI_BIN && existsSync(localAidp)) env.AIDP_CLI_BIN = localAidp;

  const server = startServer(env);
  try {
    const init = await server.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'ask-aidp-qa', version: EXPECTED_VERSION } });
    assert(init.serverInfo.name === 'ask-aidp', 'server did not initialize as ask-aidp');
    assert(init.serverInfo.version === EXPECTED_VERSION, `server version mismatch: expected ${EXPECTED_VERSION}, got ${init.serverInfo.version}`);

    const list = await server.request('tools/list');
    assert(
      list.tools.length === EXPECTED_TOOLS,
      `tools/list returned ${list.tools.length} tools, expected ${EXPECTED_TOOLS}`
    );
    const tableWithDataTool = list.tools.find((tool) => tool.name === 'aidp_create_table_with_data');
    assert(
      tableWithDataTool?.inputSchema?.properties?.csvIncludeHeader?.const === false,
      'aidp_create_table_with_data must reject CSV headers because create-data-table reads positionally'
    );
    const names = list.tools.map((tool) => tool.name);
    for (const name of [
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
    ]) {
      assert(names.includes(name), `missing tool: ${name}`);
    }

    const version = await server.request('tools/call', { name: 'aidp_cli', arguments: { args: ['version'], addCommonFlags: false } });
    assert(!version.isError, `aidp version failed: ${toolText(version)}`);
    assert(/aidp/i.test(toolText(version)), 'version output did not mention aidp');

    const help = await server.request('tools/call', { name: 'aidp_command_help', arguments: { group: 'workflow' } });
    assert(!help.isError, `workflow help failed: ${toolText(help)}`);
    assert(/create-job-run/.test(toolText(help)), 'workflow help missing create-job-run');

    const cliReferenceSummary = await server.request('tools/call', { name: 'aidp_cli_reference', arguments: {} });
    assert(!cliReferenceSummary.isError, `CLI reference summary failed: ${toolText(cliReferenceSummary)}`);
    const cliReferencePlan = JSON.parse(toolText(cliReferenceSummary));
    assert(cliReferencePlan.groupCount === 18, 'CLI reference expected 18 command groups');
    assert(cliReferencePlan.commandCount === 256, 'CLI reference expected 256 commands');

    const agentReference = await server.request('tools/call', {
      name: 'aidp_cli_reference',
      arguments: { group: 'agent', command: 'deploy' }
    });
    assert(!agentReference.isError, `agent deploy reference failed: ${toolText(agentReference)}`);
    const agentReferencePlan = JSON.parse(toolText(agentReference));
    assert(agentReferencePlan.command.fullName === 'aidp agent deploy', 'agent reference command mismatch');
    assert(agentReferencePlan.command.usage.includes('agent deploy'), 'agent reference usage mismatch');

    const restReference = await server.request('tools/call', {
      name: 'aidp_rest_api_reference',
      arguments: { category: 'Agent' }
    });
    assert(!restReference.isError, `REST agent reference failed: ${toolText(restReference)}`);
    const restReferencePlan = JSON.parse(toolText(restReference));
    assert(restReferencePlan.apiVersion === '20260430', 'REST API version mismatch');
    assert(restReferencePlan.category.id === 'agent', 'REST Agent category mismatch');
    assert(restReferencePlan.category.operationCount === 21, 'REST Agent operation count mismatch');

    const restReferenceSummary = await server.request('tools/call', { name: 'aidp_rest_api_reference', arguments: {} });
    assert(!restReferenceSummary.isError, `REST reference summary failed: ${toolText(restReferenceSummary)}`);
    const restReferenceSummaryPlan = JSON.parse(toolText(restReferenceSummary));
    assert(restReferenceSummaryPlan.categoryCount === 19, 'REST reference expected 19 categories');
    assert(restReferenceSummaryPlan.operationCount === 271, 'REST reference expected 271 operations');

    // Exercise every August/September addition through the public MCP tools.
    const basePath = '/20260430/aiDataPlatforms/{aiDataPlatformId}';
    const workspacePath = `${basePath}/workspaces/{workspaceKey}`;
    const clusterPath = `${workspacePath}/clusters/{clusterKey}`;
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
    for (const [group, command, category, method, endpointPath] of latestOperations) {
      const referenceResult = await server.request('tools/call', {
        name: 'aidp_cli_reference', arguments: { group, command }
      });
      assert(!referenceResult.isError, `missing CLI reference: ${group} ${command}`);
      const reference = JSON.parse(toolText(referenceResult)).command;
      assert(reference.fullName === `aidp ${group} ${command}`, `wrong CLI reference: ${group} ${command}`);
      assert(reference.usage.startsWith(reference.fullName), `missing usage: ${reference.fullName}`);
      if (command === 'export-compute-configuration') {
        assert(reference.bodyModel === exportModel, 'export default lookup must expose the root request model');
        deepEqual(reference.bodyFields.map((field) => field.name).sort(), [...exportFields].sort());
      }

      const endpointResult = await server.request('tools/call', {
        name: 'aidp_rest_api_reference', arguments: { category, search: endpointPath }
      });
      assert(!endpointResult.isError, `REST lookup failed: ${endpointPath}`);
      const matches = JSON.parse(toolText(endpointResult)).matches;
      assert(matches.some((op) => op.method === method && op.path === endpointPath), `missing REST method/path: ${method} ${endpointPath}`);

      const requestPath = endpointPath.replace('{volumeKey}', 'volume-key').replace('{taskRunKey}', 'task-run-key');
      const planResult = await server.request('tools/call', {
        name: 'aidp_rest',
        arguments: {
          method, path: requestPath, dryRun: true,
          config: {
            endpoint: 'https://aidp.example.com',
            instanceId: 'ocid1.aidataplatform.oc1..example',
            workspaceKey: 'workspace-key', clusterKey: 'cluster-key'
          }
        }
      });
      assert(!planResult.isError, `REST dry run failed: ${method} ${endpointPath}: ${toolText(planResult)}`);
      const plan = JSON.parse(toolText(planResult));
      const expectedPath = requestPath.replace('{aiDataPlatformId}', 'ocid1.aidataplatform.oc1..example')
        .replace('{workspaceKey}', 'workspace-key').replace('{clusterKey}', 'cluster-key');
      assert(plan.method === method && new URL(plan.url).pathname === expectedPath, `REST request plan mismatch: ${endpointPath}`);
      assert(plan.headers.accept === (command === 'export-compute-configuration' ? 'application/x-yaml' : 'application/json'), `wrong effective Accept header: ${endpointPath}`);
    }

    await rejects(server.request('tools/call', {
      name: 'aidp_rest',
      arguments: {
        method: 'GET', path: `${workspacePath}/actions/getBundlePublishStatus`, dryRun: true,
        config: { endpoint: 'https://aidp.example.com', instanceId: 'ocid1.aidataplatform.oc1..example', workspaceKey: 'workspace-key' }
      }
    }), /method and path do not match a documented AIDP REST operation/,
    'bundle publish status must use POST as specified by its operation reference');

    const schemaReference = await server.request('tools/call', {
      name: 'aidp_cli_reference',
      arguments: { group: 'schema', command: 'create-table' }
    });
    assert(!schemaReference.isError, `schema create-table reference failed: ${toolText(schemaReference)}`);
    const schemaReferencePlan = JSON.parse(toolText(schemaReference));
    assert(schemaReferencePlan.command.fullName === 'aidp schema create-table', 'schema reference command mismatch');
    assert(schemaReferencePlan.command.usage.includes('schema create-table'), 'schema reference usage mismatch');

    const mlopsReference = await server.request('tools/call', {
      name: 'aidp_cli_reference',
      arguments: { search: 'registered model', limit: 5 }
    });
    assert(!mlopsReference.isError, `CLI reference search failed: ${toolText(mlopsReference)}`);
    const mlopsReferencePlan = JSON.parse(toolText(mlopsReference));
    assert(mlopsReferencePlan.count > 0, 'CLI reference search returned no matches');

    const notebookWorkflowDryRun = await server.request('tools/call', {
      name: 'aidp_notebook_workflow',
      arguments: {
        notebookCount: 4,
        config: { workspaceKey: 'workspace-key', clusterKey: 'cluster-key' },
        dryRun: true
      }
    });
    assert(!notebookWorkflowDryRun.isError, `notebook workflow dry run failed: ${toolText(notebookWorkflowDryRun)}`);
    const notebookWorkflowPlan = JSON.parse(toolText(notebookWorkflowDryRun));
    assert(notebookWorkflowPlan.notebookCount === 4, 'notebook workflow dry run did not plan 4 notebooks');
    const generatedNotebook = JSON.parse(readFileSync(path.join(notebookWorkflowPlan.evidenceDir, 'requests', 'update-notebook-1.json'), 'utf8')).content;
    assert(generatedNotebook.cells.length === 3, 'generated notebook should have setup/work/validation cells');
    assert(generatedNotebook.cells[0].source[0].includes('# Setup:'), 'generated notebook setup cell missing comment');
    assert(generatedNotebook.cells[1].source[0].includes('# Work:'), 'generated notebook work cell missing comment');
    assert(generatedNotebook.cells[2].source[0].includes('# Validation:'), 'generated notebook validation cell missing comment');

    const uploadDryRun = await server.request('tools/call', {
      name: 'aidp_upload_workspace_code',
      arguments: {
        localPath: path.join(PLUGIN_ROOT, 'README.md'),
        workspacePath: '/Workspace/qa/README.md',
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!uploadDryRun.isError, `upload dry run failed: ${toolText(uploadDryRun)}`);
    const uploadPlan = JSON.parse(toolText(uploadDryRun));
    assert(uploadPlan.entryCount === 1, 'upload dry run expected one file');
    assert(
      uploadPlan.implementation === 'aidp-typescript-client WorkspaceObjectClient#createWorkspaceObject',
      'upload dry run should use the WorkspaceObjectClient implementation'
    );
    assert(
      uploadPlan.entries[0].request.aiDataPlatformId === '<aiDataPlatformId>',
      'upload dry run should use an instance ID placeholder when only workspaceKey is configured'
    );
    assert(
      uploadPlan.entries[0].request.createWorkspaceObjectDetails.startsWith('<base64:'),
      'upload dry run should return a base64 placeholder instead of file contents'
    );

    const globDir = path.join(PLUGIN_ROOT, 'qa-runs', 'glob-top-level');
    rmSync(globDir, { recursive: true, force: true });
    mkdirSync(globDir, { recursive: true });
    writeFileSync(path.join(globDir, 'flat.py'), 'print("flat")\n');
    const globUploadDryRun = await server.request('tools/call', {
      name: 'aidp_upload_workspace_code',
      arguments: {
        localPath: globDir,
        workspacePath: '/Workspace/qa/glob',
        includeGlobs: ['**/*.py'],
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!globUploadDryRun.isError, `glob upload dry run failed: ${toolText(globUploadDryRun)}`);
    const globUploadPlan = JSON.parse(toolText(globUploadDryRun));
    assert(globUploadPlan.entries.some((entry) => entry.localPath.endsWith('flat.py')), '**/*.py should match top-level files');

    const fakeAidp = path.join(PLUGIN_ROOT, 'qa-runs', 'fake-aidp-json-warning.mjs');
    writeFileSync(fakeAidp, [
      '#!/usr/bin/env node',
      'console.log(JSON.stringify({ data: { key: "workspace-key", displayName: "Workspace" } }));',
      'console.error("warning: benign {non-json} notice");',
      ''
    ].join('\n'));
    chmodSync(fakeAidp, 0o755);
    const warningJson = await server.request('tools/call', {
      name: 'aidp_check_connection',
      arguments: {
        config: { aidpBin: fakeAidp, workspaceKey: 'workspace-key' },
        includeCluster: false
      }
    });
    assert(!warningJson.isError, `CLI JSON parse should tolerate stderr warnings: ${toolText(warningJson)}`);

    const gitFolderDryRun = await server.request('tools/call', {
      name: 'aidp_create_git_folder',
      arguments: {
        folderPath: '/Workspace/git/sample',
        gitRepositoryUrl: 'https://github.com/example/repo.git',
        branchName: 'main',
        credentialKey: 'credential-key',
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!gitFolderDryRun.isError, `git folder dry run failed: ${toolText(gitFolderDryRun)}`);
    const gitFolderPlan = JSON.parse(toolText(gitFolderDryRun));
    assert(gitFolderPlan.body.gitRepositoryUrl.includes('github.com'), 'git folder dry run missing repository URL');

    const gitPushDryRun = await server.request('tools/call', {
      name: 'aidp_git_commit_push',
      arguments: {
        gitRepositoryKey: 'git-repository-key',
        gitFolderPath: '/Workspace/git/sample',
        branchName: 'main',
        files: ['notebooks/01_ingest.ipynb'],
        commitMessage: 'QA commit',
        config: {
          endpoint: 'https://aidp.example.com',
          instanceId: 'ocid1.aidataplatform.oc1..example',
          workspaceKey: 'workspace-key'
        },
        dryRun: true
      }
    });
    assert(!gitPushDryRun.isError, `git commit/push dry run failed: ${toolText(gitPushDryRun)}`);
    const gitPushPlan = JSON.parse(toolText(gitPushDryRun));
    assert(gitPushPlan.implementation === 'aidp-typescript-client GitClient', 'git push dry run did not use SDK implementation');
    assert(gitPushPlan.request.commitPushDetails.commitMessage === 'QA commit', 'git push dry run missing commit message');

    const gitPullDryRun = await server.request('tools/call', {
      name: 'aidp_git_pull',
      arguments: {
        gitFolderPath: '/Workspace/git/sample',
        branchName: 'main',
        config: {
          endpoint: 'https://aidp.example.com',
          instanceId: 'ocid1.aidataplatform.oc1..example',
          workspaceKey: 'workspace-key'
        },
        dryRun: true
      }
    });
    assert(!gitPullDryRun.isError, `git pull dry run failed: ${toolText(gitPullDryRun)}`);
    const gitPullPlan = JSON.parse(toolText(gitPullDryRun));
    assert(gitPullPlan.gitRepositoryKeyResolution.operation === 'WorkspaceObjectClient#listWorkspaceObjects', 'git pull dry run did not plan workspace-object repository key resolution');
    assert(gitPullPlan.gitRepositoryKeyResolution.request.path === '/Workspace/git/sample', 'git pull dry run lookup path mismatch');
    assert(gitPullPlan.request.gitPullDetails.pullAction === 'PULL', 'git pull dry run default action mismatch');

    const gitBranchDryRun = await server.request('tools/call', {
      name: 'aidp_git_create_branch',
      arguments: {
        gitRepositoryKey: 'git-repository-key',
        gitBranchName: 'feature/qa',
        config: {
          endpoint: 'https://aidp.example.com',
          instanceId: 'ocid1.aidataplatform.oc1..example',
          workspaceKey: 'workspace-key'
        },
        dryRun: true
      }
    });
    assert(!gitBranchDryRun.isError, `git branch dry run failed: ${toolText(gitBranchDryRun)}`);
    const gitBranchPlan = JSON.parse(toolText(gitBranchDryRun));
    assert(gitBranchPlan.request.createGitBranchDetails.gitBranchName === 'feature/qa', 'git branch dry run missing branch name');

    const createAgentDryRun = await server.request('tools/call', {
      name: 'aidp_create_agent',
      arguments: {
        displayName: 'qa_agent',
        pathInfo: '/Workspace/agents/qa_agent',
        agentType: 'CANVAS',
        entryFilePath: 'agent.py',
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!createAgentDryRun.isError, `create agent dry run failed: ${toolText(createAgentDryRun)}`);
    const createAgentPlan = JSON.parse(toolText(createAgentDryRun));
    assert(createAgentPlan.command.includes('agent create workspace-key'), 'create agent command mismatch');
    assert(createAgentPlan.body.type === 'CANVAS', 'create agent type mismatch');
    assert(createAgentPlan.body.entryFilePath === 'agent.py', 'create agent entry file mismatch');

    const deployAgentDryRun = await server.request('tools/call', {
      name: 'aidp_deploy_agent',
      arguments: {
        agentKey: 'agent-key',
        agentComputeKey: 'agent-compute-key',
        sessionRetentionConfig: { retentionPeriodInDays: 30 },
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!deployAgentDryRun.isError, `deploy agent dry run failed: ${toolText(deployAgentDryRun)}`);
    const deployAgentPlan = JSON.parse(toolText(deployAgentDryRun));
    assert(deployAgentPlan.command.includes('agent deploy workspace-key agent-key'), 'deploy agent command mismatch');
    assert(deployAgentPlan.body.agentComputeKey === 'agent-compute-key', 'deploy agent compute mismatch');

    const listAgentsDryRun = await server.request('tools/call', {
      name: 'aidp_list_agents',
      arguments: {
        displayNameContains: 'qa',
        sortBy: 'displayName',
        sortOrder: 'ASC',
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!listAgentsDryRun.isError, `list agents dry run failed: ${toolText(listAgentsDryRun)}`);
    const listAgentsPlan = JSON.parse(toolText(listAgentsDryRun));
    assert(listAgentsPlan.command.includes('agent list workspace-key'), 'list agents command mismatch');
    assert(listAgentsPlan.command.includes('--display-name-contains qa'), 'list agents filter mismatch');

    const agentTraceDryRun = await server.request('tools/call', {
      name: 'aidp_get_agent_session_trace',
      arguments: {
        agentKey: 'agent-key',
        sessionId: 'session-id',
        traceKey: 'trace-key',
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!agentTraceDryRun.isError, `agent trace dry run failed: ${toolText(agentTraceDryRun)}`);
    const agentTracePlan = JSON.parse(toolText(agentTraceDryRun));
    assert(agentTracePlan.command.includes('agent get-session-trace workspace-key agent-key session-id trace-key'), 'agent trace command mismatch');

    const aiComputeDryRun = await server.request('tools/call', {
      name: 'aidp_create_ai_compute',
      arguments: {
        displayName: 'qa_ai_compute',
        driverShape: 'VM.Standard.E5.Flex',
        ocpus: 2,
        memoryInGBs: 32,
        minReplicas: 1,
        maxReplicas: 2,
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!aiComputeDryRun.isError, `AI Compute create dry run failed: ${toolText(aiComputeDryRun)}`);
    const aiComputePlan = JSON.parse(toolText(aiComputeDryRun));
    assert(aiComputePlan.command.includes('cluster create workspace-key'), 'AI Compute create command mismatch');
    assert(aiComputePlan.body.type === 'AI_COMPUTE', 'AI Compute type mismatch');
    assert(aiComputePlan.body.replicaConfig.maxReplica === 2, 'AI Compute replica configuration mismatch');

    const listAiComputesDryRun = await server.request('tools/call', {
      name: 'aidp_list_ai_computes',
      arguments: { displayNameContains: 'qa', config: { workspaceKey: 'workspace-key' }, dryRun: true }
    });
    assert(!listAiComputesDryRun.isError, `AI Compute list dry run failed: ${toolText(listAiComputesDryRun)}`);
    const listAiComputesPlan = JSON.parse(toolText(listAiComputesDryRun));
    assert(listAiComputesPlan.command.includes('cluster list workspace-key --type AI_COMPUTE'), 'AI Compute list type filter mismatch');

    const updateAiComputeDryRun = await server.request('tools/call', {
      name: 'aidp_update_ai_compute',
      arguments: {
        clusterKey: 'ai-compute-key',
        ocpus: 4,
        memoryInGBs: 64,
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!updateAiComputeDryRun.isError, `AI Compute update dry run failed: ${toolText(updateAiComputeDryRun)}`);
    const updateAiComputePlan = JSON.parse(toolText(updateAiComputeDryRun));
    assert(updateAiComputePlan.command.includes('cluster update workspace-key ai-compute-key'), 'AI Compute update command mismatch');
    assert(updateAiComputePlan.body.driverConfig.driverShapeConfig.ocpus === 4, 'AI Compute update shape mismatch');

    const restDryRun = await server.request('tools/call', {
      name: 'aidp_rest',
      arguments: {
        method: 'GET',
        path: '/20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/clusters',
        query: { type: 'AI_COMPUTE' },
        config: {
          endpoint: 'https://aidp.example.com',
          instanceId: 'ocid1.aidataplatform.oc1..example',
          workspaceKey: 'workspace-key'
        },
        dryRun: true
      }
    });
    assert(!restDryRun.isError, `REST dry run failed: ${toolText(restDryRun)}`);
    const restPlan = JSON.parse(toolText(restDryRun));
    assert(restPlan.url.includes('/20260430/aiDataPlatforms/ocid1.aidataplatform.oc1..example/workspaces/workspace-key/clusters?type=AI_COMPUTE'), 'REST URL expansion mismatch');

    const schemaDryRun = await server.request('tools/call', {
      name: 'aidp_create_schema',
      arguments: {
        catalogName: 'qa_catalog',
        displayName: 'qa_schema',
        description: 'QA dry run',
        properties: { owner: 'codex' },
        dryRun: true
      }
    });
    assert(!schemaDryRun.isError, `schema dry run failed: ${toolText(schemaDryRun)}`);
    const schemaPlan = JSON.parse(toolText(schemaDryRun));
    assert(schemaPlan.command.includes('schema create'), 'schema dry run command mismatch');
    assert(schemaPlan.body.catalogName === 'qa_catalog', 'schema dry run body missing catalogName');

    const managedDeltaDryRun = await server.request('tools/call', {
      name: 'aidp_create_delta_table',
      arguments: {
        catalogKey: 'catalog-key',
        schemaKey: 'schema-key',
        displayName: 'qa_delta_managed',
        columns: [{ fieldName: 'id', fieldType: 'INT' }],
        tableProperties: { deltaFeature: 'true' },
        dryRun: true
      }
    });
    assert(!managedDeltaDryRun.isError, `managed Delta dry run failed: ${toolText(managedDeltaDryRun)}`);
    const managedDeltaPlan = JSON.parse(toolText(managedDeltaDryRun));
    assert(managedDeltaPlan.body.managedTableDefinition.managedTableDataFormat === 'DELTA', 'managed Delta format mismatch');

    const externalDeltaDryRun = await server.request('tools/call', {
      name: 'aidp_create_delta_table',
      arguments: {
        catalogKey: 'catalog-key',
        schemaKey: 'schema-key',
        displayName: 'qa_delta_external',
        tableType: 'EXTERNAL',
        objectStorageLocationPath: 'oci://bucket@namespace/path/to/delta',
        columns: [{ fieldName: 'id', fieldType: 'INT' }],
        dryRun: true
      }
    });
    assert(!externalDeltaDryRun.isError, `external Delta dry run failed: ${toolText(externalDeltaDryRun)}`);
    const externalDeltaPlan = JSON.parse(toolText(externalDeltaDryRun));
    assert(externalDeltaPlan.body.externalTableDefinition.externalTableDataFormat === 'DELTA', 'external Delta format mismatch');

    const tableWithDataDryRun = await server.request('tools/call', {
      name: 'aidp_create_table_with_data',
      arguments: {
        catalogKey: 'catalog-key',
        schemaKey: 'catalog.schema',
        displayName: 'qa_table_with_data',
        rows: [
          { id: 1, amount: 10.5, status: 'new' },
          { id: 2, amount: 22.0, status: 'posted' }
        ],
        dryRun: true
      }
    });
    assert(!tableWithDataDryRun.isError, `table with data dry run failed: ${toolText(tableWithDataDryRun)}`);
    const tableWithDataPlan = JSON.parse(toolText(tableWithDataDryRun));
    assert(tableWithDataPlan.command.includes('schema create-data-table'), 'table with data command mismatch');
    assert(tableWithDataPlan.body.managedTableDefinition.managedTableDataFormat === 'DELTA', 'table with data managed format mismatch');
    assert(tableWithDataPlan.body.fileFormat === 'CSV', 'table with data file format mismatch');
    assert(
      JSON.stringify(tableWithDataPlan.body.selectedColumns) === JSON.stringify(['_c0', '_c1', '_c2']),
      'table with data selected columns should be positional (_c0/_c1/_c2) for headerless inline rows'
    );
    assert(
      tableWithDataPlan.body.tableFields.some((field) => field.fieldName === 'amount'),
      'table with data tableFields should carry the amount column name'
    );
    assert(
      /(^|\n)1,10\.5,new(\n|$)/.test(tableWithDataPlan.dataPreview) && !/id,amount,status/.test(tableWithDataPlan.dataPreview),
      'table with data CSV preview should be headerless data rows'
    );

    const csvTableSql = await server.request('tools/call', {
      name: 'aidp_generate_csv_table_sql',
      arguments: {
        fullTableName: 'bronze_layer.erp.customers',
        path: 'oci://revenue-leakage-demo@idlhizlfs5zd/erp/customers/',
        columns: [
          { name: 'customer_id', type: 'STRING' },
          { name: 'customer_name', type: 'STRING' }
        ]
      }
    });
    assert(!csvTableSql.isError, `CSV table SQL generation failed: ${toolText(csvTableSql)}`);
    const csvTableSqlPlan = JSON.parse(toolText(csvTableSql));
    assert(csvTableSqlPlan.sql.includes('USING CSV'), 'CSV table SQL missing USING CSV');
    assert(csvTableSqlPlan.sql.includes("header 'true'"), 'CSV table SQL missing header true option');

    const listCatalogsDryRun = await server.request('tools/call', {
      name: 'aidp_list_catalogs',
      arguments: {
        catalogType: 'EXTERNAL',
        catalogState: 'ACTIVE',
        sortBy: 'displayName',
        sortOrder: 'ASC',
        limit: 20,
        dryRun: true
      }
    });
    assert(!listCatalogsDryRun.isError, `list catalogs dry run failed: ${toolText(listCatalogsDryRun)}`);
    const listCatalogsPlan = JSON.parse(toolText(listCatalogsDryRun));
    assert(listCatalogsPlan.command.includes('catalog list'), 'list catalogs command mismatch');
    assert(listCatalogsPlan.command.includes('--catalog-type EXTERNAL'), 'list catalogs catalog type missing');

    const getCatalogDryRun = await server.request('tools/call', {
      name: 'aidp_get_catalog',
      arguments: {
        catalogKey: 'catalog-key',
        isCatalogGuid: true,
        dryRun: true
      }
    });
    assert(!getCatalogDryRun.isError, `get catalog dry run failed: ${toolText(getCatalogDryRun)}`);
    const getCatalogPlan = JSON.parse(toolText(getCatalogDryRun));
    assert(getCatalogPlan.command.includes('catalog get catalog-key'), 'get catalog command mismatch');
    assert(getCatalogPlan.command.includes('--is-catalog-guid true'), 'get catalog GUID flag missing');

    const catalogCreateDryRun = await server.request('tools/call', {
      name: 'aidp_create_catalog',
      arguments: {
        displayName: 'qa_catalog_general',
        catalogType: 'EXTERNAL',
        sourceType: 'ADW',
        connectionProperties: { connectionId: 'placeholder' },
        properties: { owner: 'codex' },
        dryRun: true
      }
    });
    assert(!catalogCreateDryRun.isError, `catalog create dry run failed: ${toolText(catalogCreateDryRun)}`);
    const catalogCreatePlan = JSON.parse(toolText(catalogCreateDryRun));
    assert(catalogCreatePlan.command.includes('catalog create'), 'catalog create command mismatch');
    assert(catalogCreatePlan.body.catalogType === 'EXTERNAL', 'catalog create type mismatch');
    assert(catalogCreatePlan.body.sourceType === 'ADW', 'catalog create source type mismatch');
    assert(catalogCreatePlan.body.connectionDetails.connectionProperties.connectionId === 'placeholder', 'catalog create connection properties mismatch');

    const catalogDryRun = await server.request('tools/call', {
      name: 'aidp_create_external_catalog',
      arguments: {
        displayName: 'qa_external_catalog',
        sourceType: 'ADW',
        connectionProperties: { connectionId: 'placeholder' },
        dryRun: true
      }
    });
    assert(!catalogDryRun.isError, `external catalog dry run failed: ${toolText(catalogDryRun)}`);
    const catalogPlan = JSON.parse(toolText(catalogDryRun));
    assert(catalogPlan.body.catalogType === 'EXTERNAL', 'external catalog type mismatch');
    assert(catalogPlan.body.sourceType === 'ADW', 'external catalog source type mismatch');

    const autoHealDryRun = await server.request('tools/call', {
      name: 'aidp_auto_heal_workflow',
      arguments: {
        jobRunKey: 'job-run-key',
        taskKeys: ['failed_task'],
        parameters: [{ name: 'retry_reason', value: 'qa' }],
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!autoHealDryRun.isError, `auto-heal dry run failed: ${toolText(autoHealDryRun)}`);
    const autoHealPlan = JSON.parse(toolText(autoHealDryRun));
    assert(autoHealPlan.command.includes('workflow repair-job-run'), 'auto-heal command mismatch');
    assert(autoHealPlan.body.taskKeys[0] === 'failed_task', 'auto-heal task key mismatch');
    assert(autoHealPlan.body.parameters[0].name === 'retry_reason', 'auto-heal parameter mismatch');

    const medallionDryRun = await server.request('tools/call', {
      name: 'aidp_create_medallion_architecture',
      arguments: {
        catalogName: 'qa_catalog',
        schemaPrefix: 'lakehouse',
        dryRun: true
      }
    });
    assert(!medallionDryRun.isError, `medallion dry run failed: ${toolText(medallionDryRun)}`);
    const medallionPlan = JSON.parse(toolText(medallionDryRun));
    assert(medallionPlan.schemaCount === 3, 'medallion dry run expected three schemas');
    assert(medallionPlan.schemas[0].body.displayName === 'lakehouse_bronze', 'medallion bronze schema mismatch');

    const bundleCreateDryRun = await server.request('tools/call', {
      name: 'aidp_create_bundle',
      arguments: {
        path: '/Workspace/bundles',
        name: 'qa_bundle',
        bundledResources: [{ resourceKey: 'job-key', resourceType: 'JOB' }],
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!bundleCreateDryRun.isError, `bundle create dry run failed: ${toolText(bundleCreateDryRun)}`);
    const bundleCreatePlan = JSON.parse(toolText(bundleCreateDryRun));
    assert(bundleCreatePlan.body.bundledResources[0].resourceType === 'JOB', 'bundle resource type mismatch');

    const bundleDeployDryRun = await server.request('tools/call', {
      name: 'aidp_deploy_bundle',
      arguments: {
        path: '/Workspace/bundles/qa_bundle',
        config: { workspaceKey: 'workspace-key' },
        dryRun: true
      }
    });
    assert(!bundleDeployDryRun.isError, `bundle deploy dry run failed: ${toolText(bundleDeployDryRun)}`);
    const bundleDeployPlan = JSON.parse(toolText(bundleDeployDryRun));
    assert(bundleDeployPlan.deploy.command.includes('bundle deploy'), 'bundle deploy command mismatch');

    if (live) {
      const connection = await server.request('tools/call', { name: 'aidp_check_connection', arguments: { config: { auth: process.env.AIDP_AUTH || 'api_key' } } });
      assert(!connection.isError, `live connection failed: ${toolText(connection)}`);
    }

    if (workflow) {
      const workflowResult = await server.request('tools/call', {
        name: 'aidp_three_notebook_workflow',
        arguments: {
          config: { auth: process.env.AIDP_AUTH || 'api_key' },
          clusterName: process.env.AIDP_CLUSTER_NAME || 'VishalFirstAIDPCluster',
          pollIntervalSeconds: 20,
          pollTimeoutSeconds: 1800,
          logLookbackMinutes: 60
        }
      });
      assert(!workflowResult.isError, `workflow tool failed: ${toolText(workflowResult)}`);
      const summary = JSON.parse(toolText(workflowResult));
      assert(summary.jobRunStatus === 'SUCCESS', `workflow status was ${summary.jobRunStatus}`);
      assert(summary.taskRuns.length === 3, 'workflow did not return three task runs');
      writeFileSync(path.join(PLUGIN_ROOT, 'qa-live-result.json'), JSON.stringify(summary, null, 2));
    }

    const stray = server.nonJsonLines();
    assert(stray.length === 0, `non-JSON output on stdout: ${stray.slice(0, 5).join(' | ')}`);
  } finally {
    server.stop();
  }
}

main()
  .then(() => console.log(JSON.stringify({ ok: true, live, workflow }, null, 2)))
  .catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
