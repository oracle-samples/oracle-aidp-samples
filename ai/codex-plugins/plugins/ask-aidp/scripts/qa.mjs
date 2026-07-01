#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const SERVER = path.join(PLUGIN_ROOT, 'mcp', 'ask-aidp-server.mjs');
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
  assert(manifest.mcpServers === './.mcp.json', 'missing mcpServers manifest entry');
  assert(existsSync(path.join(PLUGIN_ROOT, '.mcp.json')), 'missing .mcp.json');
  assert(existsSync(SERVER), 'missing MCP server');
  assert(existsSync(path.join(PLUGIN_ROOT, 'skills', 'ask-aidp', 'SKILL.md')), 'missing skill');
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
  const timer = setInterval(() => {
    let index;
    while ((index = stdout.indexOf('\n')) >= 0) {
      const line = stdout.slice(0, index);
      stdout = stdout.slice(index + 1);
      if (!line.trim()) continue;
      const message = JSON.parse(line);
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
  const env = {};
  const localAidp = path.resolve(PLUGIN_ROOT, '..', '..', 'samples', 'npm-cli', 'node_modules', '.bin', process.platform === 'win32' ? 'aidp.cmd' : 'aidp');
  if (!process.env.AIDP_CLI_BIN && existsSync(localAidp)) env.AIDP_CLI_BIN = localAidp;

  const server = startServer(env);
  try {
    const init = await server.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'ask-aidp-qa', version: '0.7.2' } });
    assert(init.serverInfo.name === 'ask-aidp', 'server did not initialize as ask-aidp');

    const list = await server.request('tools/list');
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
    assert(cliReferencePlan.groupCount === 16, 'CLI reference expected 16 command groups');
    assert(cliReferencePlan.commandCount === 215, 'CLI reference expected 215 commands');

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
    assert(tableWithDataPlan.body.selectedColumns.includes('amount'), 'table with data selected columns mismatch');
    assert(/id,amount,status/.test(tableWithDataPlan.dataPreview), 'table with data CSV preview mismatch');

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
