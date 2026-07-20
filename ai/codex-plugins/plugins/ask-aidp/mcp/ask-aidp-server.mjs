#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const SERVER_VERSION = '0.9.0';
const SERVER_NAME = 'ask-aidp';
const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const CLI_REFERENCE_PATH = path.join(PLUGIN_ROOT, 'assets', 'aidp-cli-command-reference.json');
const REST_API_REFERENCE_PATH = path.join(PLUGIN_ROOT, 'assets', 'aidp-rest-api-reference.json');
const requireFromPlugin = createRequire(import.meta.url);

const TOOLS = [
  {
    name: 'aidp_cli',
    description: 'Run any aidp-cli command using an argument array. Supports all aidp-cli command groups and commands.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        args: {
          type: 'array',
          items: { type: 'string' },
          description: 'Arguments after the aidp executable, for example ["workspace","get","<workspaceKey>"]. Do not include "aidp".'
        },
        body: {
          description: 'Optional JSON-serializable request body. The tool writes it to a temporary file and appends --body @file.'
        },
        addCommonFlags: {
          type: 'boolean',
          description: 'Append common endpoint/instance/auth/profile flags from config/environment.',
          default: true
        },
        config: { $ref: '#/$defs/config' },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 },
        maxOutputChars: { type: 'integer', minimum: 1000, maximum: 200000, default: 40000 }
      },
      required: ['args'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_check_connection',
    description: 'Verify AIDP workspace connectivity and optional cluster access using aidp-cli.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        config: { $ref: '#/$defs/config' },
        includeCluster: { type: 'boolean', default: true }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_notebook_workflow',
    description: 'Create any N number of notebooks, upload them to AIDP Workbench, create a sequential workflow, run it, export task outputs, and collect logs.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        config: { $ref: '#/$defs/config' },
        runId: { type: 'string', description: 'Optional stable run ID. Defaults to UTC timestamp.' },
        notebookCount: { type: 'integer', minimum: 1, maximum: 100, default: 3 },
        notebooks: {
          type: 'array',
          minItems: 1,
          maxItems: 100,
          items: { $ref: '#/$defs/notebookSpec' },
          description: 'Optional explicit notebook definitions. If omitted, notebookCount generated sample notebooks are used.'
        },
        notebookFolder: { type: 'string', description: 'Optional target folder. Defaults to /Workspace/aidp_codex_notebook_workflow_<runId>.' },
        jobPath: { type: 'string', default: '/Workspace/jobs' },
        clusterName: { type: 'string', description: 'Cluster display name. Defaults to AIDP_CLUSTER_NAME or cluster key.' },
        evidenceDir: { type: 'string', description: 'Local evidence directory. Defaults to ./qa-runs/<runId>/ask-aidp-plugin.' },
        pollIntervalSeconds: { type: 'integer', minimum: 1, maximum: 300, default: 20 },
        pollTimeoutSeconds: { type: 'integer', minimum: 60, maximum: 14400, default: 1800 },
        logLookbackMinutes: { type: 'integer', minimum: 1, maximum: 1440, default: 60 },
        outputFormat: { type: 'string', enum: ['IPYNB', 'HTML'], default: 'IPYNB' },
        dryRun: { type: 'boolean', default: false }
      },
      $defs: { config: configSchema(), notebookSpec: notebookSpecSchema() }
    }
  },
  {
    name: 'aidp_three_notebook_workflow',
    description: 'Compatibility alias for aidp_notebook_workflow with notebookCount=3.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        config: { $ref: '#/$defs/config' },
        runId: { type: 'string', description: 'Optional stable run ID. Defaults to UTC timestamp.' },
        notebookFolder: { type: 'string', description: 'Optional target folder. Defaults to /Workspace/aidp_codex_three_notebook_<runId>.' },
        jobPath: { type: 'string', default: '/Workspace/jobs' },
        clusterName: { type: 'string', description: 'Cluster display name. Defaults to AIDP_CLUSTER_NAME or cluster key.' },
        evidenceDir: { type: 'string', description: 'Local evidence directory. Defaults to ./qa-runs/<runId>/ask-aidp-plugin.' },
        pollIntervalSeconds: { type: 'integer', minimum: 1, maximum: 300, default: 20 },
        pollTimeoutSeconds: { type: 'integer', minimum: 60, maximum: 14400, default: 1800 },
        logLookbackMinutes: { type: 'integer', minimum: 1, maximum: 1440, default: 60 },
        outputFormat: { type: 'string', enum: ['IPYNB', 'HTML'], default: 'IPYNB' }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_upload_workspace_code',
    description: 'Upload local code files or a directory tree into an AIDP workspace path using workspace-object create.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        localPath: { type: 'string', description: 'Local file or directory to upload.' },
        workspacePath: { type: 'string', description: 'Target workspace file/folder path.' },
        recursive: { type: 'boolean', default: true },
        overwrite: { type: 'boolean', default: true },
        includeGlobs: { type: 'array', items: { type: 'string' }, default: [] },
        excludeGlobs: { type: 'array', items: { type: 'string' }, default: ['.git/**', 'node_modules/**', '.venv/**', '__pycache__/**'] },
        maxFiles: { type: 'integer', minimum: 1, maximum: 10000, default: 500 },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['localPath', 'workspacePath'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_git_folder',
    description: 'Connect an AIDP workspace folder to Git and fetch code by creating a Git-backed folder.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        folderPath: { type: 'string' },
        gitRepositoryUrl: { type: 'string' },
        branchName: { type: 'string' },
        credentialKey: { type: 'string' },
        gitProviderKey: { type: 'string' },
        description: { type: 'string' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['folderPath', 'gitRepositoryUrl', 'branchName', 'credentialKey'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_commit_push',
    description: 'Use the native AIDP TypeScript SDK GitClient to stage selected workspace files, commit, and push to the connected Git repository.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        branchName: { type: 'string' },
        files: { type: 'array', items: { type: 'string' }, description: 'Repo-relative file paths to stage before commit.' },
        commitMessage: { type: 'string' },
        commitDescription: { type: 'string' },
        ifMatch: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      required: ['commitMessage'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_pull',
    description: 'Use the native AIDP TypeScript SDK GitClient to pull remote changes into a Git-backed workspace folder.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        branchName: { type: 'string' },
        remoteBranchName: { type: 'string' },
        pullAction: { type: 'string', enum: ['PULL', 'MERGE_CONTINUE', 'MERGE_ABORT'], default: 'PULL' },
        commitMessage: { type: 'string', description: 'Used when pullAction is MERGE_CONTINUE.' },
        ifMatch: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_get_repository',
    description: 'Use the native AIDP TypeScript SDK GitClient to get metadata for a connected workspace Git repository.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        shouldIncludeCredentialKey: { type: 'boolean', default: false },
        opcRequestId: { type: 'string' },
        config: { $ref: '#/$defs/config' }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_operation_state',
    description: 'Use the native AIDP TypeScript SDK GitClient to inspect current Git worktree status or an in-progress Git operation.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        operationName: { type: 'string' },
        branchName: { type: 'string' },
        opcRequestId: { type: 'string' },
        config: { $ref: '#/$defs/config' }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_list_branches',
    description: 'Use the native AIDP TypeScript SDK GitClient to list branches for a connected workspace Git repository.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        displayName: { type: 'string' },
        displayNameContains: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 1000 },
        page: { type: 'string' },
        sortOrder: { type: 'string', enum: ['ASC', 'DESC'] },
        sortBy: { type: 'string' },
        opcRequestId: { type: 'string' },
        config: { $ref: '#/$defs/config' }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_create_branch',
    description: 'Use the native AIDP TypeScript SDK GitClient to create a branch in a connected workspace Git repository.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitBranchName: { type: 'string' },
        gitFolderPath: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      required: ['gitBranchName'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_checkout_branch',
    description: 'Use the native AIDP TypeScript SDK GitClient to check out a remote branch into a Git-backed workspace folder.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        branchName: { type: 'string' },
        gitFolderPath: { type: 'string' },
        ifMatch: { type: 'string' },
        dhUserPrincipal: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      required: ['branchName'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_list_diffs',
    description: 'Use the native AIDP TypeScript SDK GitClient to list file-level Git diffs for a workspace branch.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string' },
        branchName: { type: 'string' },
        compareTo: { type: 'string' },
        filter: { type: 'string' },
        displayName: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 1000 },
        page: { type: 'string' },
        sortOrder: { type: 'string', enum: ['ASC', 'DESC'] },
        sortBy: { type: 'string' },
        opcRequestId: { type: 'string' },
        config: { $ref: '#/$defs/config' }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_diff_detail',
    description: 'Use the native AIDP TypeScript SDK GitClient to get a unified diff patch for a specific workspace Git file.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string' },
        branchName: { type: 'string' },
        gitFilePath: { type: 'string' },
        contextLines: { type: 'integer', minimum: 0, maximum: 1000 },
        maxPatchBytes: { type: 'integer', minimum: 1, maximum: 10000000 },
        opcRequestId: { type: 'string' },
        config: { $ref: '#/$defs/config' }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_merge',
    description: 'Use the native AIDP TypeScript SDK GitClient to merge a branch or commit into the workspace branch.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from details.gitFolderPath or details.sourceGitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        details: { type: 'object', additionalProperties: true, description: 'Raw GitMergeDetails object accepted by the SDK.' },
        ifMatch: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      required: ['details'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_rebase',
    description: 'Use the native AIDP TypeScript SDK GitClient to rebase the workspace branch on another branch or commit.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from details.gitFolderPath or details.sourceGitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        details: { type: 'object', additionalProperties: true, description: 'Raw GitRebaseDetails object accepted by the SDK.' },
        ifMatch: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      required: ['details'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_git_reset',
    description: 'Use the native AIDP TypeScript SDK GitClient to reset a workspace Git repository or reset Git folder state.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        gitRepositoryKey: { type: 'string', description: 'Optional. If omitted, the tool resolves it from details.gitFolderPath using WorkspaceObjectClient#listWorkspaceObjects.' },
        gitFolderPath: { type: 'string', description: 'Git-backed workspace folder path. Used to resolve gitRepositoryKey when omitted.' },
        mode: { type: 'string', enum: ['repository', 'folder_state'], default: 'repository' },
        details: { type: 'object', additionalProperties: true, description: 'Raw GitResetDetails or ResetGitFolderStateDetails object accepted by the SDK.' },
        ifMatch: { type: 'string' },
        opcRetryToken: { type: 'string' },
        opcRequestId: { type: 'string' },
        shouldUpdateRecent: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false }
      },
      required: ['details'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_agent',
    description: 'Create an AIDP agent with typed common inputs. Wraps aidp agent create; advanced agent-card, diagram, and guardrail fields are supported as JSON objects.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        displayName: { type: 'string', description: 'Agent display name.' },
        pathInfo: { type: 'string', description: 'Workspace path that contains the agent source or definition.' },
        agentType: { type: 'string', description: 'Agent type supported by the target service, for example CANVAS.' },
        description: { type: 'string' },
        computeKey: { type: 'string', description: 'Optional agent compute key.' },
        entryFilePath: { type: 'string', description: 'Optional entry file path relative to pathInfo.' },
        dependenciesFilePath: { type: 'string', description: 'Optional dependency file path relative to pathInfo.' },
        agentCardConfig: { type: 'object', additionalProperties: true, description: 'Optional AgentCardConfigDetail payload.' },
        diagram: { type: 'object', additionalProperties: true, description: 'Optional AgentDiagram payload.' },
        guardrails: { type: 'object', additionalProperties: true, description: 'Optional GuardrailsConfiguration payload.' },
        sessionConfig: { type: 'object', additionalProperties: true, description: 'Optional AgentSessionConfig payload.' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['displayName', 'pathInfo'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_deploy_agent',
    description: 'Deploy an existing AIDP agent with typed deployment, OAuth, and session-retention inputs. Wraps aidp agent deploy.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        agentKey: { type: 'string' },
        displayName: { type: 'string' },
        description: { type: 'string' },
        agentComputeKey: { type: 'string' },
        oAuthConfig: { type: 'object', additionalProperties: true, description: 'Optional OAuthConfiguration payload.' },
        sessionRetentionConfig: { type: 'object', additionalProperties: true, description: 'Optional SessionRetentionConfiguration payload.' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['agentKey'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_list_agents',
    description: 'List AIDP agents with typed compute, display-name, pagination, and sort filters. Wraps aidp agent list.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        computeKey: { type: 'string' },
        displayName: { type: 'string' },
        displayNameContains: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 1000 },
        page: { type: 'string' },
        sortOrder: { type: 'string', enum: ['ASC', 'DESC'] },
        sortBy: { type: 'string', enum: ['timeCreated', 'displayName'] },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_get_agent_session_trace',
    description: 'Get trace details for an AIDP agent session message. Wraps aidp agent get-session-trace.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        agentKey: { type: 'string' },
        sessionId: { type: 'string' },
        traceKey: { type: 'string' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['agentKey', 'sessionId', 'traceKey'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_ai_compute',
    description: 'Create an AIDP AI Compute cluster for agent playground and deployment workloads. Wraps aidp cluster create with type AI_COMPUTE.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        displayName: { type: 'string' },
        driverShape: { type: 'string', description: 'OCI compute shape for each AI Compute replica.' },
        ocpus: { type: 'integer', minimum: 1, maximum: 64 },
        memoryInGBs: { type: 'integer', minimum: 1, maximum: 1024 },
        gpus: { type: 'integer', minimum: 0 },
        minReplicas: { type: 'integer', minimum: 1, maximum: 10, default: 1 },
        maxReplicas: { type: 'integer', minimum: 1, maximum: 10, default: 1 },
        driverNodeType: { type: 'string' },
        nodeType: { type: 'string' },
        description: { type: 'string' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['displayName', 'driverShape', 'ocpus', 'memoryInGBs'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_list_ai_computes',
    description: 'List AIDP AI Compute clusters. Wraps aidp cluster list with type AI_COMPUTE.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        state: { type: 'string' },
        displayName: { type: 'string' },
        displayNameContains: { type: 'string' },
        limit: { type: 'integer', minimum: 1, maximum: 1000 },
        page: { type: 'string' },
        sortOrder: { type: 'string', enum: ['ASC', 'DESC'] },
        sortBy: { type: 'string', enum: ['timeCreated', 'displayName'] },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_update_ai_compute',
    description: 'Update an AIDP AI Compute cluster configuration. Wraps aidp cluster update with type AI_COMPUTE.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        clusterKey: { type: 'string' },
        displayName: { type: 'string' },
        driverShape: { type: 'string' },
        ocpus: { type: 'integer', minimum: 1, maximum: 64 },
        memoryInGBs: { type: 'integer', minimum: 1, maximum: 1024 },
        gpus: { type: 'integer', minimum: 0 },
        minReplicas: { type: 'integer', minimum: 1, maximum: 10 },
        maxReplicas: { type: 'integer', minimum: 1, maximum: 10 },
        driverNodeType: { type: 'string' },
        nodeType: { type: 'string' },
        description: { type: 'string' },
        ifMatch: { type: 'string' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['clusterKey'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_collect_logs',
    description: 'Collect cluster logs for an existing AIDP workflow job run.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        jobRunKey: { type: 'string' },
        config: { $ref: '#/$defs/config' },
        evidenceDir: { type: 'string' },
        logLookbackMinutes: { type: 'integer', minimum: 1, maximum: 1440, default: 60 },
        limit: { type: 'integer', minimum: 1, maximum: 10000, default: 1000 }
      },
      required: ['jobRunKey'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_track_runs',
    description: 'Track workflow job runs, task runs, notebook sessions, task outputs, and optionally download cluster logs.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        jobRunKey: { type: 'string' },
        taskRunKey: { type: 'string' },
        sessionId: { type: 'string' },
        notebookPath: { type: 'string' },
        clusterId: { type: 'string' },
        includeTaskRuns: { type: 'boolean', default: true },
        includeTaskOutputs: { type: 'boolean', default: false },
        includeNotebookSessions: { type: 'boolean', default: false },
        downloadLogs: { type: 'boolean', default: false },
        evidenceDir: { type: 'string' },
        logLookbackMinutes: { type: 'integer', minimum: 1, maximum: 1440, default: 60 },
        config: { $ref: '#/$defs/config' },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_schema',
    description: 'Create an AIDP schema with simplified typed inputs. Wraps aidp schema create.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        catalogName: { type: 'string', description: 'Catalog display/name value required by aidp-cli schema create.' },
        displayName: { type: 'string', description: 'Schema display name.' },
        description: { type: 'string' },
        properties: { type: 'object', additionalProperties: true, default: {} },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['catalogName', 'displayName'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_delta_table',
    description: 'Create a managed or external Delta table with simplified typed inputs. Wraps aidp schema create-table.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        catalogKey: { type: 'string' },
        schemaKey: { type: 'string' },
        displayName: { type: 'string' },
        description: { type: 'string' },
        tableType: { type: 'string', enum: ['MANAGED', 'EXTERNAL'], default: 'MANAGED' },
        columns: {
          type: 'array',
          minItems: 1,
          items: { $ref: '#/$defs/field' },
          description: 'Table columns. fieldName and fieldType are required.'
        },
        partitionKeys: {
          type: 'array',
          items: { $ref: '#/$defs/field' },
          default: []
        },
        objectStorageLocationPath: { type: 'string', description: 'Required for EXTERNAL Delta tables.' },
        externalTableLocationType: { type: 'string', enum: ['OBJECT_STORAGE', 'MOUNT'], default: 'OBJECT_STORAGE' },
        tableProperties: {
          description: 'Either an object map or an array of {propertyName, propertyValue}.',
          oneOf: [
            { type: 'object', additionalProperties: { type: ['string', 'number', 'boolean'] } },
            {
              type: 'array',
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  propertyName: { type: 'string' },
                  propertyValue: { type: 'string' }
                },
                required: ['propertyName', 'propertyValue']
              }
            }
          ]
        },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['catalogKey', 'schemaKey', 'displayName', 'columns'],
      $defs: { config: configSchema(), field: fieldSchema() }
    }
  },
  {
    name: 'aidp_create_table_with_data',
    description: 'Create a managed table and load data into it from an object storage path, local data file, or inline rows. Wraps aidp schema create-data-table.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        catalogKey: { type: 'string' },
        schemaKey: { type: 'string', description: 'Schema key. For temp-file staging, use the fully-qualified schema key expected by aidp-cli, such as catalog.schema.' },
        displayName: { type: 'string' },
        description: { type: 'string' },
        columns: {
          type: 'array',
          minItems: 1,
          items: { $ref: '#/$defs/dataTableField' },
          description: 'Table columns. fieldName is required; fieldType is optional and can be inferred for inline rows.'
        },
        rows: {
          type: 'array',
          minItems: 1,
          maxItems: 10000,
          items: {
            type: 'object',
            additionalProperties: { type: ['string', 'number', 'boolean', 'null'] }
          },
          description: 'Inline rows to stage as CSV before creating the table.'
        },
        localDataFile: { type: 'string', description: 'Local data file to stage before creating the table.' },
        objectStorageLocationPath: { type: 'string', description: 'Existing object storage path to load from. When set, no staging upload is performed.' },
        fileFormat: { type: 'string', enum: dataFormatValues(), default: 'CSV' },
        managedTableDataFormat: { type: 'string', enum: dataFormatValues(), default: 'DELTA' },
        selectedColumns: { type: 'array', items: { type: 'string' }, default: [] },
        partitionKeys: { type: 'array', items: { $ref: '#/$defs/dataTableField' }, default: [] },
        tableProperties: {
          description: 'Either an object map or an array of {propertyName, propertyValue}.',
          oneOf: [
            { type: 'object', additionalProperties: { type: ['string', 'number', 'boolean'] } },
            {
              type: 'array',
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  propertyName: { type: 'string' },
                  propertyValue: { type: 'string' }
                },
                required: ['propertyName']
              }
            }
          ]
        },
        csvIncludeHeader: { type: 'boolean', default: true },
        uploadMethod: { type: 'string', enum: ['PUT', 'POST'], default: 'PUT' },
        uploadContentType: { type: 'string', description: 'Optional content type for staged local data upload.' },
        uploadTargetBaseUrl: { type: 'string', description: 'Optional base object storage URL if AIDP returns a relative tempFileUploadTarget.' },
        maxUploadBytes: { type: 'integer', minimum: 1, maximum: 52428800, default: 10485760 },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['catalogKey', 'schemaKey', 'displayName'],
      $defs: { config: configSchema(), dataTableField: dataTableFieldSchema() }
    }
  },
  {
    name: 'aidp_generate_csv_table_sql',
    description: 'Generate AIDP notebook SQL to create a CSV-backed table with header row handling using USING CSV OPTIONS header true.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        catalogName: { type: 'string', description: 'Catalog name used in the SQL identifier.' },
        schemaName: { type: 'string', description: 'Schema/database name used in the SQL identifier.' },
        tableName: { type: 'string', description: 'Table name used in the SQL identifier.' },
        fullTableName: { type: 'string', description: 'Optional fully qualified table name. Overrides catalogName/schemaName/tableName.' },
        columns: {
          type: 'array',
          minItems: 1,
          items: { $ref: '#/$defs/sqlColumn' },
          description: 'SQL columns with name and type.'
        },
        path: { type: 'string', description: 'CSV folder or file path, such as oci://bucket@namespace/path/.' },
        header: { type: 'boolean', default: true },
        includeSqlMagic: { type: 'boolean', default: true },
        comments: { type: 'boolean', default: true }
      },
      required: ['columns', 'path'],
      $defs: { sqlColumn: sqlColumnSchema() }
    }
  },
  {
    name: 'aidp_list_catalogs',
    description: 'List AIDP catalogs with typed filters. Wraps aidp catalog list.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        displayName: { type: 'string', description: 'Exact catalog display-name filter.' },
        catalogState: { type: 'string', enum: ['ACTIVE', 'CREATING', 'DELETING'] },
        catalogType: { type: 'string', enum: ['INTERNAL', 'EXTERNAL'] },
        limit: { type: 'integer', minimum: 1, maximum: 1000 },
        page: { type: 'string' },
        sortOrder: { type: 'string', enum: ['ASC', 'DESC'] },
        sortBy: { type: 'string', enum: ['timeCreated', 'displayName'] },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_get_catalog',
    description: 'Get detailed information for one AIDP catalog by key or GUID. Wraps aidp catalog get.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        catalogKey: { type: 'string' },
        isCatalogGuid: { type: 'boolean', default: false },
        shouldUpdateRecent: { type: 'boolean' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['catalogKey'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_catalog',
    description: 'Create an AIDP catalog with simplified typed inputs. Wraps aidp catalog create.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        displayName: { type: 'string' },
        catalogType: { type: 'string', enum: ['EXTERNAL', 'INTERNAL', 'UNKNOWN_VALUE'] },
        sourceType: { type: 'string', enum: ['ADW', 'ALH', 'KAFKA', 'ATP', 'ORACLE', 'EXADATA', 'ORACLE_ANALYTICS', 'UNKNOWN_VALUE'] },
        connectionProperties: { type: 'object', additionalProperties: true },
        description: { type: 'string' },
        properties: { type: 'object', additionalProperties: true, default: {} },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['displayName'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_external_catalog',
    description: 'Create an external AIDP catalog with simplified typed inputs. Wraps aidp catalog create.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        displayName: { type: 'string' },
        sourceType: { type: 'string', enum: ['ADW', 'ALH', 'KAFKA', 'ATP', 'ORACLE', 'EXADATA', 'ORACLE_ANALYTICS', 'UNKNOWN_VALUE'] },
        connectionProperties: { type: 'object', additionalProperties: true, default: {} },
        description: { type: 'string' },
        properties: { type: 'object', additionalProperties: true, default: {} },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['displayName', 'sourceType', 'connectionProperties'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_auto_heal_workflow',
    description: 'Auto-heal a failed workflow job run by selecting failed task keys and calling aidp workflow repair-job-run.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        jobRunKey: { type: 'string', description: 'Original job run key to inspect and repair.' },
        taskKeys: {
          type: 'array',
          items: { type: 'string' },
          description: 'Optional explicit task keys to repair. If omitted, the tool lists task runs and selects failed/error task statuses.'
        },
        statuses: {
          type: 'array',
          items: {
            type: 'string',
            enum: ['FAILED', 'INTERNAL_ERROR', 'TIMED_OUT', 'CANCELED', 'UPSTREAM_FAILED', 'UPSTREAM_CANCELED', 'BLOCKED', 'EXCLUDED']
          },
          description: 'Task statuses to auto-select when taskKeys is omitted.'
        },
        parameters: {
          type: 'array',
          items: { $ref: '#/$defs/parameter' },
          default: []
        },
        pollToCompletion: { type: 'boolean', default: false },
        pollIntervalSeconds: { type: 'integer', minimum: 1, maximum: 300, default: 20 },
        pollTimeoutSeconds: { type: 'integer', minimum: 60, maximum: 14400, default: 1800 },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['jobRunKey'],
      $defs: { config: configSchema(), parameter: parameterSchema() }
    }
  },
  {
    name: 'aidp_create_medallion_architecture',
    description: 'Create medallion schemas from scratch, typically bronze, silver, and gold, in an AIDP catalog.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        catalogName: { type: 'string' },
        schemaPrefix: { type: 'string', description: 'Optional prefix applied to layer schema names, for example project_a creates project_a_bronze.' },
        layers: {
          type: 'array',
          items: { type: 'string' },
          default: ['bronze', 'silver', 'gold']
        },
        descriptions: { type: 'object', additionalProperties: { type: 'string' }, default: {} },
        properties: { type: 'object', additionalProperties: true, default: {} },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['catalogName'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_create_bundle',
    description: 'Create an AIDP bundle from selected workspace resources.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        path: { type: 'string', description: 'Parent workspace folder where the bundle should be created.' },
        name: { type: 'string', description: 'Bundle folder name.' },
        bundledResources: {
          type: 'array',
          minItems: 1,
          items: { $ref: '#/$defs/bundledResource' }
        },
        description: { type: 'string' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['path', 'name', 'bundledResources'],
      $defs: { config: configSchema(), bundledResource: bundledResourceSchema() }
    }
  },
  {
    name: 'aidp_deploy_bundle',
    description: 'Deploy an AIDP bundle and optionally fetch deployment status.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        path: { type: 'string', description: 'Bundle root folder path in the workspace.' },
        fetchStatus: { type: 'boolean', default: true },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 }
      },
      required: ['path'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_command_help',
    description: 'Inspect aidp-cli top-level help, command group help, command help, or search results.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        group: { type: 'string', description: 'Optional command group such as workflow, notebook, cluster, catalog, schema, or mlops.' },
        command: { type: 'string', description: 'Optional command name within the group.' },
        search: { type: 'string', description: 'Optional search text. Uses aidp search <text>.' },
        config: { $ref: '#/$defs/config' },
        maxOutputChars: { type: 'integer', minimum: 1000, maximum: 100000, default: 30000 }
      },
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_rest',
    description: 'Call any documented AIDP Workbench REST endpoint with OCI request signing. Use aidp_rest_api_reference to find the canonical method and path.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        method: { type: 'string', enum: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'] },
        path: { type: 'string', description: 'Documented endpoint path. May use {aiDataPlatformId}, {workspaceKey}, and {clusterKey} placeholders from config.' },
        query: { type: 'object', additionalProperties: { type: ['string', 'number', 'boolean', 'array'] }, default: {} },
        headers: { type: 'object', additionalProperties: { type: 'string' }, default: {} },
        body: { description: 'Optional JSON-serializable request body.' },
        config: { $ref: '#/$defs/config' },
        dryRun: { type: 'boolean', default: false },
        timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200, default: 120 },
        maxOutputChars: { type: 'integer', minimum: 1000, maximum: 200000, default: 40000 }
      },
      required: ['method', 'path'],
      $defs: { config: configSchema() }
    }
  },
  {
    name: 'aidp_rest_api_reference',
    description: 'Look up the current Oracle AIDP Workbench REST API version, category coverage, and official reference links used for REST fallback decisions.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        category: { type: 'string', description: 'Optional REST category, for example Agent, Git, Workflow, or Schema.' },
        operation: { type: 'string', description: 'Optional exact REST operation name.' },
        search: { type: 'string', description: 'Optional search text matched against operation names, methods, and paths.' },
        limit: { type: 'integer', minimum: 1, maximum: 100, default: 20 }
      }
    }
  },
  {
    name: 'aidp_cli_reference',
    description: 'Look up the documented AIDP CLI command reference generated from the aidataplatform-sdk CLI README.',
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        group: { type: 'string', description: 'Optional command group, such as schema, workflow, mlops, or workspace-object.' },
        command: { type: 'string', description: 'Optional command name within a group, such as create-table or list-job-runs.' },
        search: { type: 'string', description: 'Optional search text matched against command names, summaries, usages, and reference markdown.' },
        includeMarkdown: { type: 'boolean', default: false, description: 'Include the full generated Markdown section for matched commands.' },
        limit: { type: 'integer', minimum: 1, maximum: 100, default: 20 }
      }
    }
  }
];

function configSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      aidpBin: { type: 'string', description: 'Path to aidp executable. Defaults to AIDP_CLI_BIN, bundled vendor CLI, or aidp on PATH.' },
      endpoint: { type: 'string', description: 'AIDP data plane endpoint.' },
      instanceId: { type: 'string', description: 'AIDP instance OCID.' },
      workspaceKey: { type: 'string', description: 'AIDP workspace key.' },
      clusterKey: { type: 'string', description: 'AIDP cluster key.' },
      profile: { type: 'string', description: 'OCI config profile.' },
      auth: { type: 'string', enum: ['api_key', 'security_token', 'instance_principal', 'resource_principal'] },
      configFile: { type: 'string', description: 'OCI config file path.' },
      region: { type: 'string', description: 'OCI region.' },
      timeoutSeconds: { type: 'integer', minimum: 1, maximum: 7200 }
    }
  };
}

function fieldSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      fieldName: { type: 'string' },
      fieldType: { type: 'string' },
      fieldDescription: { type: 'string' },
      fieldPrecision: { type: 'string' },
      fieldScale: { type: 'string' }
    },
    required: ['fieldName', 'fieldType']
  };
}

function dataTableFieldSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      fieldName: { type: 'string' },
      fieldType: { type: 'string' },
      fieldDescription: { type: 'string' },
      fieldPrecision: { type: 'string' },
      fieldScale: { type: 'string' }
    },
    required: ['fieldName']
  };
}

function dataFormatValues() {
  return ['AVRO', 'ORC', 'PARQUET', 'TEXTFILE', 'JSON', 'CSV', 'DELTA', 'ICEBERG', 'UNKNOWN_VALUE'];
}

function parameterSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      name: { type: 'string' },
      value: { type: 'string' }
    },
    required: ['name']
  };
}

function sqlColumnSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      name: { type: 'string' },
      type: { type: 'string', description: 'SQL data type such as STRING, INT, DOUBLE, DATE, or TIMESTAMP.' },
      comment: { type: 'string' }
    },
    required: ['name', 'type']
  };
}

function notebookSpecSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      name: { type: 'string', description: 'Notebook filename, for example 01_ingest.ipynb.' },
      taskKey: { type: 'string' },
      source: { type: 'string', description: 'Python source for the first code cell.' },
      dependsOn: {
        type: 'array',
        items: {
          type: 'object',
          additionalProperties: false,
          properties: { taskKey: { type: 'string' } },
          required: ['taskKey']
        }
      }
    },
    required: ['name']
  };
}

function bundledResourceSchema() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      resourceKey: { type: 'string' },
      resourceType: { type: 'string', enum: ['JOB', 'AGENTFLOW'] }
    },
    required: ['resourceKey', 'resourceType']
  };
}

function jsonResponse(id, result) {
  process.stdout.write(`${JSON.stringify({ jsonrpc: '2.0', id, result })}\n`);
}

function jsonError(id, code, message, data) {
  process.stdout.write(`${JSON.stringify({ jsonrpc: '2.0', id, error: { code, message, data } })}\n`);
}

function toolText(text, isError = false) {
  return { content: [{ type: 'text', text }], isError };
}

function utcRunId() {
  return new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
}

function mergedConfig(input = {}) {
  return {
    aidpBin: input.aidpBin || process.env.AIDP_CLI_BIN || '',
    endpoint: input.endpoint || process.env.AIDP_ENDPOINT || '',
    instanceId: input.instanceId || process.env.AIDP_OCID || process.env.AIDP_INSTANCE_ID || '',
    workspaceKey: input.workspaceKey || process.env.AIDP_WORKSPACE_KEY || '',
    clusterKey: input.clusterKey || process.env.AIDP_CLUSTER_KEY || '',
    profile: input.profile || process.env.OCI_PROFILE || '',
    auth: input.auth || process.env.AIDP_AUTH || '',
    configFile: input.configFile || process.env.OCI_CONFIG_FILE || '',
    region: input.region || process.env.OCI_REGION || '',
    timeoutSeconds: input.timeoutSeconds || Number(process.env.AIDP_TIMEOUT_SECONDS || 0) || undefined
  };
}

function workflowConfig(input = {}) {
  const config = mergedConfig(input);
  if (!config.auth) config.auth = 'api_key';
  return config;
}

function requireValue(value, name) {
  if (!value) throw new Error(`Missing required AIDP configuration: ${name}`);
  return value;
}

function resolveAidp(config = {}) {
  const explicit = config.aidpBin || process.env.AIDP_CLI_BIN;
  if (explicit) return { command: explicit, prefixArgs: [] };

  const vendoredJs = path.join(PLUGIN_ROOT, 'vendor', 'node_modules', 'aidp-cli', 'dist', 'bin', 'aidp.js');
  if (existsSync(vendoredJs)) return { command: process.execPath, prefixArgs: [vendoredJs] };

  const vendored = path.join(PLUGIN_ROOT, 'vendor', 'node_modules', '.bin', process.platform === 'win32' ? 'aidp.cmd' : 'aidp');
  if (existsSync(vendored)) return { command: vendored, prefixArgs: [] };

  return { command: 'aidp', prefixArgs: [] };
}

function commonFlags(config = {}) {
  const flags = [];
  if (config.endpoint) flags.push('--endpoint', config.endpoint);
  if (config.instanceId) flags.push('--instance-id', config.instanceId);
  if (config.profile) flags.push('--profile', config.profile);
  if (config.auth) flags.push('--auth', config.auth);
  if (config.configFile) flags.push('--config-file', config.configFile);
  if (config.region) flags.push('--region', config.region);
  if (config.timeoutSeconds) flags.push('--timeout', String(config.timeoutSeconds));
  return flags;
}

function sdkResolvePaths() {
  return [
    path.join(PLUGIN_ROOT, 'vendor', 'node_modules'),
    path.join(PLUGIN_ROOT, 'node_modules'),
    path.join(PLUGIN_ROOT, '..', '..', 'samples', 'npm-cli', 'node_modules'),
    path.join(PLUGIN_ROOT, '..', '..', 'samples', 'node-sdk', 'node_modules'),
    process.cwd()
  ];
}

function requireSdkModule(packageName) {
  const resolved = requireFromPlugin.resolve(packageName, { paths: sdkResolvePaths() });
  return requireFromPlugin(resolved);
}

function normalizeCommonModule(module) {
  return module.ConfigFileAuthenticationDetailsProvider ? module : module.default;
}

async function createSdkAuthProvider(config) {
  const common = normalizeCommonModule(requireSdkModule('oci-common'));
  const auth = config.auth || 'api_key';
  if (auth === 'instance_principal') {
    const builder = new common.InstancePrincipalsAuthenticationDetailsProviderBuilder();
    return { common, authProvider: await builder.build() };
  }
  if (auth === 'resource_principal') {
    return { common, authProvider: await common.ResourcePrincipalAuthenticationDetailsProvider.builder() };
  }
  if (!['api_key', 'security_token'].includes(auth)) {
    throw new Error(`Native SDK Git operations support auth values api_key, security_token, instance_principal, and resource_principal. Received: ${auth}`);
  }
  return {
    common,
    authProvider: new common.ConfigFileAuthenticationDetailsProvider(config.configFile || undefined, config.profile || undefined)
  };
}

async function createGitClient(config) {
  return await createSdkClient(config, 'GitClient');
}

async function createWorkspaceObjectClient(config) {
  return await createSdkClient(config, 'WorkspaceObjectClient');
}

async function createSdkClient(config, clientName) {
  const endpoint = requireValue(config.endpoint, 'endpoint or AIDP_ENDPOINT');
  const { authProvider } = await createSdkAuthProvider(config);
  const sdk = requireSdkModule('aidp-typescript-client');
  if (typeof sdk[clientName] !== 'function') {
    throw new Error(`Installed aidp-typescript-client does not export ${clientName}. Install a newer SDK package.`);
  }
  const client = new sdk[clientName]({ authenticationDetailsProvider: authProvider });
  client.endpoint = endpoint.replace(/\/+$/, '');
  return client;
}

function gitBaseRequest(input, config) {
  return {
    aiDataPlatformId: requireValue(config.instanceId, 'instanceId, AIDP_OCID, or AIDP_INSTANCE_ID'),
    workspaceKey: requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY'),
    gitRepositoryKey: input.gitRepositoryKey || '<resolved-from-workspace-object>'
  };
}

function gitRepositoryLookupPath(input = {}) {
  return input.gitFolderPath
    || input.workspaceObjectPath
    || input.details?.gitFolderPath
    || input.details?.sourceGitFolderPath
    || input.details?.folderPath
    || '';
}

function workspaceObjectLookupRequest(input, config) {
  return addOptionalFields({
    aiDataPlatformId: requireValue(config.instanceId, 'instanceId, AIDP_OCID, or AIDP_INSTANCE_ID'),
    workspaceKey: requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY'),
    path: requireValue(gitRepositoryLookupPath(input), 'gitFolderPath when gitRepositoryKey is omitted'),
    limit: 25,
    shouldUpdateRecent: false
  }, input, ['opcRequestId']);
}

async function resolveGitRepositoryKey(input, config) {
  if (input.gitRepositoryKey) return { gitRepositoryKey: input.gitRepositoryKey };
  const request = workspaceObjectLookupRequest(input, config);
  const client = await createWorkspaceObjectClient(config);
  try {
    const response = await client.listWorkspaceObjects(request);
    const items = response.workspaceObjectCollection?.items || [];
    const exact = items.filter((item) => normalizeGitLookupPath(item.path) === normalizeGitLookupPath(request.path));
    const candidates = exact.length ? exact : items;
    const matches = candidates
      .map((item) => ({ item, gitRepositoryKey: extractGitRepositoryKey(item) }))
      .filter((match) => match.gitRepositoryKey);
    const distinct = [...new Set(matches.map((match) => match.gitRepositoryKey))];
    if (distinct.length === 1) {
      return {
        gitRepositoryKey: distinct[0],
        lookup: {
          operation: 'WorkspaceObjectClient#listWorkspaceObjects',
          request,
          matchedPath: matches[0].item.path,
          matchedType: matches[0].item.type
        }
      };
    }
    if (distinct.length > 1) {
      throw new Error(`Workspace object lookup found multiple Git repository keys for ${request.path}: ${distinct.join(', ')}`);
    }
    throw new Error(`Workspace object lookup did not find Git repository metadata for ${request.path}. Returned ${items.length} item(s).`);
  } finally {
    client.close?.();
  }
}

function normalizeGitLookupPath(value = '') {
  return String(value).replace(/\/+$/, '') || '/';
}

function extractGitRepositoryKey(value) {
  const seen = new Set();
  const stack = [value];
  const keyNames = new Set(['gitRepositoryKey', 'gitRepoKey', 'repositoryKey', 'repoKey']);
  while (stack.length) {
    const current = stack.pop();
    if (current === null || current === undefined) continue;
    if (typeof current === 'string') {
      const trimmed = current.trim();
      if (!trimmed) continue;
      if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
        try {
          stack.push(JSON.parse(trimmed));
        } catch {
          // Non-JSON metadata values are ignored.
        }
      }
      continue;
    }
    if (typeof current !== 'object') continue;
    if (seen.has(current)) continue;
    seen.add(current);
    for (const [key, nested] of Object.entries(current)) {
      if (keyNames.has(key) && typeof nested === 'string' && nested.trim()) return nested.trim();
      stack.push(nested);
    }
  }
  return '';
}

function addOptionalFields(target, source, fields) {
  for (const field of fields) {
    if (source[field] !== undefined) target[field] = source[field];
  }
  return target;
}

function sdkResponseToJson(response) {
  return JSON.stringify(response, (key, value) => {
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'function') return undefined;
    return value;
  }, 2);
}

function sdkDryRun(operation, request) {
  return toolText(JSON.stringify({
    dryRun: true,
    implementation: 'aidp-typescript-client GitClient',
    operation,
    request
  }, null, 2));
}

function gitDryRun(operation, request, input, config) {
  const lookupPath = gitRepositoryLookupPath(input);
  const lookup = !input.gitRepositoryKey && lookupPath
    ? {
        operation: 'WorkspaceObjectClient#listWorkspaceObjects',
        request: workspaceObjectLookupRequest(input, config)
      }
    : undefined;
  return toolText(JSON.stringify({
    dryRun: true,
    implementation: 'aidp-typescript-client GitClient',
    operation,
    gitRepositoryKeyResolution: lookup || (input.gitRepositoryKey ? { provided: true } : { required: 'gitFolderPath' }),
    request
  }, null, 2));
}

function scrubCommand(args) {
  return `aidp ${args.map((arg) => quoteArg(arg)).join(' ')}`;
}

function quoteArg(arg) {
  if (/^[A-Za-z0-9_./:=@+-]+$/.test(arg)) return arg;
  return JSON.stringify(arg);
}

function truncate(text, maxOutputChars) {
  if (!maxOutputChars || text.length <= maxOutputChars) return text;
  return `${text.slice(0, maxOutputChars)}\n... truncated ${text.length - maxOutputChars} chars ...`;
}

async function runAidp(args, options = {}) {
  const config = options.config || {};
  const resolved = resolveAidp(config);
  const finalArgs = [...resolved.prefixArgs, ...args];
  if (options.addCommonFlags !== false) finalArgs.push(...commonFlags(config));

  const commandForDisplay = scrubCommand([...args, ...(options.addCommonFlags === false ? [] : commonFlags(config))]);
  const timeoutMs = (options.timeoutSeconds || config.timeoutSeconds || 120) * 1000;
  const spawnTarget = normalizeSpawnTarget(resolved.command, finalArgs);

  return await new Promise((resolve) => {
    const child = spawn(spawnTarget.command, spawnTarget.args, {
      cwd: options.cwd || process.cwd(),
      env: { ...process.env, ...(options.env || {}) },
      shell: false
    });

    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      setTimeout(() => child.kill('SIGKILL'), 3000).unref();
    }, timeoutMs);

    child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
    child.on('error', (error) => {
      clearTimeout(timer);
      resolve({ exitCode: 127, stdout, stderr: `${stderr}${error.message}\n`, command: commandForDisplay, timedOut });
    });
    child.on('close', (code, signal) => {
      clearTimeout(timer);
      resolve({ exitCode: timedOut ? 124 : (code ?? 1), signal, stdout, stderr, command: commandForDisplay, timedOut });
    });
  });
}

function normalizeSpawnTarget(command, args) {
  if (process.platform !== 'win32') return { command, args };
  const lower = command.toLowerCase();
  if (!lower.endsWith('.cmd') && !lower.endsWith('.bat')) return { command, args };
  return {
    command: process.env.ComSpec || 'cmd.exe',
    args: ['/d', '/s', '/c', [quoteWindowsCmdArg(command), ...args.map(quoteWindowsCmdArg)].join(' ')]
  };
}

function quoteWindowsCmdArg(value) {
  const text = String(value);
  if (text === '') return '""';
  if (!/[\s"&()<>^|%]/.test(text)) return text;
  return `"${text.replace(/(["^&|<>()%])/g, '^$1')}"`;
}

function parseCliJson(result) {
  const text = `${result.stdout}\n${result.stderr}`;
  for (let idx = 0; idx < text.length; idx += 1) {
    const char = text[idx];
    if (char !== '{' && char !== '[') continue;
    const end = balancedJsonEnd(text, idx);
    if (end < 0) continue;
    try {
      return JSON.parse(text.slice(idx, end));
    } catch {
      // Continue scanning. CLI warnings can contain braces before the JSON payload.
    }
  }
  throw new Error(`AIDP CLI response did not contain parseable JSON. Command: ${result.command}`);
}

function responseRoot(result) {
  const json = parseCliJson(result);
  return json.data || json;
}

function balancedJsonEnd(text, start) {
  const stack = [text[start]];
  let inString = false;
  let escaped = false;
  for (let index = start + 1; index < text.length; index += 1) {
    const char = text[index];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === '\\') {
        escaped = true;
      } else if (char === '"') {
        inString = false;
      }
      continue;
    }
    if (char === '"') {
      inString = true;
      continue;
    }
    if (char === '{' || char === '[') {
      stack.push(char);
      continue;
    }
    if (char === '}' || char === ']') {
      const open = stack.pop();
      if ((open === '{' && char !== '}') || (open === '[' && char !== ']')) return -1;
      if (stack.length === 0) return index + 1;
    }
  }
  return -1;
}

function resultToText(result, maxOutputChars = 40000) {
  const parts = [
    `Command: ${result.command}`,
    `Exit code: ${result.exitCode}`
  ];
  if (result.signal) parts.push(`Signal: ${result.signal}`);
  if (result.timedOut) parts.push('Timed out: true');
  if (result.stdout.trim()) parts.push(`Stdout:\n${truncate(result.stdout.trim(), maxOutputChars)}`);
  if (result.stderr.trim()) parts.push(`Stderr:\n${truncate(result.stderr.trim(), maxOutputChars)}`);
  if (result.exitCode !== 0 && looksLikeOciAuthFailure(result)) {
    parts.push(`OCI auth fallback:\n${ociApiKeySetupHint()}`);
  }
  return parts.join('\n\n');
}

function looksLikeOciAuthFailure(result) {
  const text = `${result.stdout}\n${result.stderr}`.toLowerCase();
  return [
    'notauthenticated',
    'not authorized',
    'unauthorized',
    '401',
    'oci login',
    'oci config',
    'config file',
    'fingerprint',
    'key_file',
    'api key',
    'security token'
  ].some((needle) => text.includes(needle));
}

function ociApiKeySetupHint() {
  return [
    'If browser-based `oci login` or security-token auth is not available, configure OCI API key authentication instead:',
    '1. Create or use an IAM user with the required AIDP permissions.',
    '2. Generate an RSA PEM API signing key pair, minimum 2048 bits.',
    '3. Upload the public key in OCI Console: User settings > API Keys > Add API Key.',
    '4. Copy the generated config snippet into ~/.oci/config and set key_file to the private key path.',
    '5. Restrict the private key permissions, for example: chmod go-rwx ~/.oci/oci_api_key.pem.',
    '6. Set AIDP_AUTH=api_key, OCI_PROFILE=DEFAULT or your profile, and OCI_CONFIG_FILE if the config is not at ~/.oci/config.',
    'Oracle reference: https://docs.oracle.com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#two'
  ].join('\n');
}

async function runWithOptionalBody(input) {
  const args = [...input.args];
  let tempDir = '';
  try {
    if (Object.prototype.hasOwnProperty.call(input, 'body')) {
      if (args.includes('--body')) throw new Error('Do not pass --body when using the body input.');
      tempDir = await mkdtemp(path.join(tmpdir(), 'ask-aidp-'));
      const bodyPath = path.join(tempDir, 'body.json');
      writeFileSync(bodyPath, typeof input.body === 'string' ? input.body : JSON.stringify(input.body, null, 2));
      args.push('--body', `@${bodyPath}`);
    }
    const result = await runAidp(args, {
      config: mergedConfig(input.config || {}),
      addCommonFlags: input.addCommonFlags !== false,
      timeoutSeconds: input.timeoutSeconds || 120
    });
    return toolText(resultToText(result, input.maxOutputChars || 40000), result.exitCode !== 0);
  } finally {
    if (tempDir) await rm(tempDir, { recursive: true, force: true });
  }
}

async function checkConnection(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const summary = [];

  const workspace = await runAidp(['workspace', 'get', workspaceKey], { config, timeoutSeconds: 120 });
  summary.push({ label: 'workspace', exitCode: workspace.exitCode, command: workspace.command, data: safeData(workspace) });
  if (workspace.exitCode !== 0) {
    if (looksLikeOciAuthFailure(workspace)) summary.push({ label: 'ociAuthFallback', guidance: ociApiKeySetupHint() });
    return toolText(JSON.stringify(summary, null, 2), true);
  }

  if (input.includeCluster !== false) {
    const clusterKey = requireValue(config.clusterKey, 'clusterKey or AIDP_CLUSTER_KEY');
    const cluster = await runAidp(['cluster', 'get', workspaceKey, clusterKey], { config, timeoutSeconds: 120 });
    summary.push({ label: 'cluster', exitCode: cluster.exitCode, command: cluster.command, data: safeData(cluster) });
    if (cluster.exitCode !== 0) {
      if (looksLikeOciAuthFailure(cluster)) summary.push({ label: 'ociAuthFallback', guidance: ociApiKeySetupHint() });
      return toolText(JSON.stringify(summary, null, 2), true);
    }
  }

  return toolText(JSON.stringify(summary, null, 2));
}

function safeData(result) {
  try {
    return responseRoot(result);
  } catch {
    return { stdout: result.stdout.trim(), stderr: result.stderr.trim() };
  }
}

async function commandHelp(input) {
  const args = [];
  if (input.search) {
    args.push('search', input.search);
  } else if (input.group && input.command) {
    args.push(input.group, input.command, '--help');
  } else if (input.group) {
    args.push(input.group, '--help');
  } else {
    args.push('--help');
  }
  const result = await runAidp(args, {
    config: mergedConfig(input.config || {}),
    addCommonFlags: false,
    timeoutSeconds: 60
  });
  return toolText(resultToText(result, input.maxOutputChars || 30000), result.exitCode !== 0);
}

function loadCliReference() {
  return JSON.parse(readFileSync(CLI_REFERENCE_PATH, 'utf8'));
}

function normalizeLookup(value) {
  return String(value || '').trim().toLowerCase();
}

function compactCommandReference(command, includeMarkdown = false) {
  const result = {
    fullName: command.fullName,
    group: command.group,
    command: command.command,
    summary: command.summary,
    usage: command.usage,
    bodyModel: command.bodyModel || undefined,
    bodyFields: command.bodyFields?.length ? command.bodyFields : undefined
  };
  if (includeMarkdown) result.markdown = command.markdown;
  return result;
}

async function cliReference(input) {
  const reference = loadCliReference();
  const includeMarkdown = input.includeMarkdown === true;
  const limit = input.limit || 20;
  const groupName = normalizeLookup(input.group);
  const commandName = normalizeLookup(input.command);

  if (input.search) {
    const terms = normalizeLookup(input.search).split(/\s+/).filter(Boolean);
    const matches = reference.commands
      .map((command) => {
        const haystack = [
          command.fullName,
          command.summary,
          command.usage,
          command.bodyModel,
          command.markdown
        ].filter(Boolean).join('\n').toLowerCase();
        const score = terms.reduce((total, term) => total + (haystack.includes(term) ? 1 : 0), 0);
        return { command, score };
      })
      .filter((item) => item.score > 0)
      .sort((left, right) => right.score - left.score || left.command.fullName.localeCompare(right.command.fullName))
      .slice(0, limit)
      .map((item) => compactCommandReference(item.command, includeMarkdown));

    return toolText(JSON.stringify({
      source: reference.source,
      generatedAt: reference.generatedAt,
      search: input.search,
      count: matches.length,
      matches
    }, null, 2));
  }

  if (groupName && commandName) {
    const command = reference.commands.find((candidate) =>
      normalizeLookup(candidate.group) === groupName &&
      (normalizeLookup(candidate.command) === commandName || normalizeLookup(candidate.fullName) === `aidp ${groupName} ${commandName}`)
    );
    if (!command) {
      return toolText(JSON.stringify({
        found: false,
        message: `No documented command found for group=${input.group} command=${input.command}.`,
        source: reference.source
      }, null, 2), true);
    }
    return toolText(JSON.stringify({
      source: reference.source,
      generatedAt: reference.generatedAt,
      command: compactCommandReference(command, includeMarkdown)
    }, null, 2));
  }

  if (groupName) {
    const group = reference.groups.find((candidate) => normalizeLookup(candidate.id) === groupName || normalizeLookup(candidate.title) === groupName);
    if (!group) {
      return toolText(JSON.stringify({
        found: false,
        message: `No documented command group found for ${input.group}.`,
        source: reference.source
      }, null, 2), true);
    }
    return toolText(JSON.stringify({
      source: reference.source,
      generatedAt: reference.generatedAt,
      group: {
        id: group.id,
        title: group.title,
        description: group.description,
        commandCount: group.commands.length,
        commands: group.commands.slice(0, limit).map((command) => ({
          fullName: command.fullName,
          command: command.command,
          summary: command.summary,
          usage: command.usage,
          bodyModel: command.bodyModel || undefined
        })),
        truncated: group.commands.length > limit
      }
    }, null, 2));
  }

  return toolText(JSON.stringify({
    source: reference.source,
    generatedAt: reference.generatedAt,
    groupCount: reference.groupCount,
    commandCount: reference.commandCount,
    groups: reference.groups.map((group) => ({
      id: group.id,
      title: group.title,
      description: group.description,
      commandCount: group.commands.length
    }))
  }, null, 2));
}

function loadRestApiReference() {
  return JSON.parse(readFileSync(REST_API_REFERENCE_PATH, 'utf8'));
}

async function restApiReference(input) {
  const reference = loadRestApiReference();
  const category = normalizeLookup(input.category);
  const operation = normalizeLookup(input.operation);
  const limit = input.limit || 20;
  const categories = category
    ? reference.categories.filter((item) => normalizeLookup(item.name) === category || normalizeLookup(item.id) === category)
    : reference.categories;

  if (category && !categories.length) {
    return toolText(JSON.stringify({
      found: false,
      message: `No REST API category found for ${input.category}.`,
      source: reference.referenceUrl,
      availableCategories: reference.categories.map((item) => item.name)
    }, null, 2), true);
  }

  const allOperations = categories.flatMap((item) => item.operations.map((entry) => ({ ...entry, category: item.name, categoryId: item.id })));
  if (input.search) {
    const terms = normalizeLookup(input.search).split(/\s+/).filter(Boolean);
    const matches = allOperations
      .filter((entry) => {
        const haystack = `${entry.category} ${entry.name} ${entry.method} ${entry.path}`.toLowerCase();
        return terms.every((term) => haystack.includes(term));
      })
      .slice(0, limit);
    return toolText(JSON.stringify({
      source: reference.source,
      generatedAt: reference.generatedAt,
      apiVersion: reference.apiVersion,
      search: input.search,
      count: matches.length,
      matches
    }, null, 2));
  }

  if (operation) {
    const matches = allOperations.filter((entry) => normalizeLookup(entry.name) === operation);
    if (!matches.length) {
      return toolText(JSON.stringify({
        found: false,
        message: `No REST operation found for ${input.operation}.`,
        source: reference.source
      }, null, 2), true);
    }
    return toolText(JSON.stringify({
      source: reference.source,
      generatedAt: reference.generatedAt,
      apiVersion: reference.apiVersion,
      operations: matches
    }, null, 2));
  }

  if (category) {
    const selected = categories[0];
    return toolText(JSON.stringify({
      source: reference.source,
      generatedAt: reference.generatedAt,
      apiVersion: reference.apiVersion,
      category: {
        id: selected.id,
        name: selected.name,
        description: selected.description,
        operationCount: selected.operationCount,
        operations: selected.operations.slice(0, limit),
        truncated: selected.operationCount > limit
      }
    }, null, 2));
  }

  return toolText(JSON.stringify({
    apiVersion: reference.apiVersion,
    endpointTemplate: reference.endpointTemplate,
    source: reference.source,
    whatsNewUrl: reference.whatsNewUrl,
    categoryCount: reference.categoryCount,
    operationCount: reference.operationCount,
    categories: reference.categories.map((item) => ({
      id: item.id,
      name: item.name,
      description: item.description,
      operationCount: item.operationCount
    }))
  }, null, 2));
}

function addDefinedProperties(target, source, fields) {
  for (const field of fields) {
    if (source[field] !== undefined && source[field] !== '') target[field] = source[field];
  }
  return target;
}

function aiComputeBody(input, { requireShape = false } = {}) {
  const body = addDefinedProperties({ type: 'AI_COMPUTE' }, input, [
    'displayName',
    'description',
    'nodeType'
  ]);
  const driverConfig = addDefinedProperties({}, input, ['driverNodeType']);
  if (input.driverShape) driverConfig.driverShape = input.driverShape;
  if (input.ocpus !== undefined || input.memoryInGBs !== undefined || input.gpus !== undefined) {
    driverConfig.driverShapeConfig = addDefinedProperties({}, input, ['ocpus', 'memoryInGBs', 'gpus']);
  }
  if (requireShape) {
    body.displayName = requireValue(input.displayName, 'displayName');
    driverConfig.driverShape = requireValue(input.driverShape, 'driverShape');
    if (input.ocpus === undefined) throw new Error('Missing required AI Compute setting: ocpus');
    if (input.memoryInGBs === undefined) throw new Error('Missing required AI Compute setting: memoryInGBs');
  }
  if (Object.keys(driverConfig).length) body.driverConfig = driverConfig;

  const minReplica = requireShape ? (input.minReplicas ?? 1) : input.minReplicas;
  const maxReplica = requireShape ? (input.maxReplicas ?? minReplica) : input.maxReplicas;
  if (minReplica !== undefined || maxReplica !== undefined) {
    const min = minReplica ?? maxReplica;
    const max = maxReplica ?? minReplica;
    if (min > max) throw new Error('minReplicas cannot be greater than maxReplicas.');
    body.replicaConfig = { minReplica: min, maxReplica: max };
  }
  return body;
}

async function createAiCompute(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  return await runJsonBodyCommand({
    label: 'Create AI Compute',
    args: ['cluster', 'create', workspaceKey],
    body: aiComputeBody(input, { requireShape: true }),
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function listAiComputes(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const args = ['cluster', 'list', workspaceKey, '--type', 'AI_COMPUTE'];
  if (input.state) args.push('--state', input.state);
  if (input.displayName) args.push('--display-name', input.displayName);
  if (input.displayNameContains) args.push('--display-name-contains', input.displayNameContains);
  if (input.limit) args.push('--limit', String(input.limit));
  if (input.page) args.push('--page', input.page);
  if (input.sortOrder) args.push('--sort-order', input.sortOrder);
  if (input.sortBy) args.push('--sort-by', input.sortBy);
  if (input.dryRun === true) return toolText(JSON.stringify({ dryRun: true, command: scrubCommand([...args, ...commonFlags(config)]) }, null, 2));

  const result = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
  return toolText(JSON.stringify({ command: result.command, exitCode: result.exitCode, response: safeData(result) }, null, 2), result.exitCode !== 0);
}

async function updateAiCompute(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const args = ['cluster', 'update', workspaceKey, requireValue(input.clusterKey, 'clusterKey')];
  if (input.ifMatch) args.push('--if-match', input.ifMatch);
  const body = aiComputeBody(input);
  if (Object.keys(body).length === 1) throw new Error('Specify at least one AI Compute property to update.');
  return await runJsonBodyCommand({
    label: 'Update AI Compute',
    args,
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

function expandRestPath(value, config, apiVersion) {
  if (!String(value).startsWith('/') || String(value).includes('?') || String(value).includes('://') || String(value).split('/').includes('..')) {
    throw new Error('path must be a documented absolute API path without a query string, protocol, or parent-directory segments.');
  }
  const replacements = {
    aiDataPlatformId: requireValue(config.instanceId, 'instanceId, AIDP_OCID, or AIDP_INSTANCE_ID'),
    workspaceKey: config.workspaceKey,
    clusterKey: config.clusterKey
  };
  const expanded = String(value).replace(/\{([^}]+)\}/g, (_match, key) => {
    const replacement = replacements[key];
    if (!replacement) throw new Error(`Replace {${key}} in path with the target resource key before calling aidp_rest.`);
    return encodeURIComponent(replacement);
  });
  if (!expanded.startsWith(`/${apiVersion}/aiDataPlatforms/`)) {
    throw new Error(`path must start with /${apiVersion}/aiDataPlatforms/{aiDataPlatformId}/.`);
  }
  return expanded;
}

function restPathMatchesOperation(path, method, reference) {
  return reference.categories
    .flatMap((category) => category.operations)
    .some((operation) => {
      if (operation.method !== method) return false;
      const pattern = `^${operation.path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\\{[^}]+\\\}/g, '[^/]+')}$`;
      return new RegExp(pattern).test(path);
    });
}

function restSafeHeaders(headers = {}) {
  const blocked = new Set(['authorization', 'host', 'x-date', 'content-length', 'x-content-sha256']);
  const result = {};
  for (const [key, value] of Object.entries(headers)) {
    if (blocked.has(key.toLowerCase())) throw new Error(`Do not set signed REST header: ${key}`);
    result[key] = value;
  }
  return result;
}

async function runRest(input) {
  const config = workflowConfig(input.config || {});
  const reference = loadRestApiReference();
  const method = String(input.method || '').toUpperCase();
  const pathValue = expandRestPath(input.path, config, reference.apiVersion);
  if (!restPathMatchesOperation(pathValue, method, reference)) {
    throw new Error('method and path do not match a documented AIDP REST operation. Use aidp_rest_api_reference to find a supported endpoint.');
  }
  const endpoint = requireValue(config.endpoint, 'endpoint or AIDP_ENDPOINT').replace(/\/+$/, '');
  const headers = restSafeHeaders(input.headers || {});
  const query = input.query || {};
  const body = input.body === undefined ? undefined : JSON.stringify(input.body);
  const url = new URL(`${endpoint}${pathValue}`);
  for (const [key, value] of Object.entries(query)) {
    for (const item of Array.isArray(value) ? value : [value]) url.searchParams.append(key, String(item));
  }

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      method,
      url: url.toString(),
      headers,
      body: input.body,
      implementation: 'oci-common DefaultRequestSigner + FetchHttpClient'
    }, null, 2));
  }

  const { common, authProvider } = await createSdkAuthProvider(config);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), (input.timeoutSeconds || 120) * 1000);
  try {
    const request = await common.composeRequest({
      baseEndpoint: endpoint,
      path: pathValue,
      defaultHeaders: { accept: 'application/json' },
      headerParams: headers,
      queryParams: query,
      method,
      bodyContent: body
    });
    const client = new common.FetchHttpClient(new common.DefaultRequestSigner(authProvider), null, { signal: controller.signal });
    const response = await client.send(request);
    const responseText = await response.text();
    let responseBody = responseText;
    try { responseBody = responseText ? JSON.parse(responseText) : null; } catch { /* Non-JSON responses are returned as text. */ }
    return toolText(JSON.stringify({
      method,
      url: url.toString(),
      status: response.status,
      ok: response.ok,
      headers: Object.fromEntries(response.headers.entries()),
      body: typeof responseBody === 'string' ? truncate(responseBody, input.maxOutputChars || 40000) : responseBody
    }, null, 2), !response.ok);
  } finally {
    clearTimeout(timer);
  }
}

async function createAgent(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const body = addDefinedProperties({
    displayName: requireValue(input.displayName, 'displayName'),
    pathInfo: requireValue(input.pathInfo, 'pathInfo')
  }, {
    ...input,
    type: input.agentType
  }, [
    'type',
    'description',
    'computeKey',
    'entryFilePath',
    'dependenciesFilePath',
    'agentCardConfig',
    'diagram',
    'guardrails',
    'sessionConfig'
  ]);

  return await runJsonBodyCommand({
    label: 'Create agent',
    args: ['agent', 'create', workspaceKey],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function deployAgent(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const agentKey = requireValue(input.agentKey, 'agentKey');
  const body = addDefinedProperties({ agentKey }, input, [
    'displayName',
    'description',
    'agentComputeKey',
    'oAuthConfig',
    'sessionRetentionConfig'
  ]);

  return await runJsonBodyCommand({
    label: 'Deploy agent',
    args: ['agent', 'deploy', workspaceKey, agentKey],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function listAgents(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const args = ['agent', 'list', workspaceKey];
  if (input.computeKey) args.push('--compute-key', input.computeKey);
  if (input.displayName) args.push('--display-name', input.displayName);
  if (input.displayNameContains) args.push('--display-name-contains', input.displayNameContains);
  if (input.limit) args.push('--limit', String(input.limit));
  if (input.page) args.push('--page', input.page);
  if (input.sortOrder) args.push('--sort-order', input.sortOrder);
  if (input.sortBy) args.push('--sort-by', input.sortBy);

  if (input.dryRun === true) {
    return toolText(JSON.stringify({ dryRun: true, command: scrubCommand([...args, ...commonFlags(config)]) }, null, 2));
  }

  const result = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
  return toolText(JSON.stringify({
    command: result.command,
    exitCode: result.exitCode,
    response: safeData(result)
  }, null, 2), result.exitCode !== 0);
}

async function getAgentSessionTrace(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const args = [
    'agent',
    'get-session-trace',
    workspaceKey,
    requireValue(input.agentKey, 'agentKey'),
    requireValue(input.sessionId, 'sessionId'),
    requireValue(input.traceKey, 'traceKey')
  ];
  if (input.dryRun === true) {
    return toolText(JSON.stringify({ dryRun: true, command: scrubCommand([...args, ...commonFlags(config)]) }, null, 2));
  }

  const result = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
  return toolText(JSON.stringify({
    command: result.command,
    exitCode: result.exitCode,
    response: safeData(result)
  }, null, 2), result.exitCode !== 0);
}

async function createSchema(input) {
  const config = workflowConfig(input.config || {});
  const body = {
    catalogName: requireValue(input.catalogName, 'catalogName'),
    displayName: requireValue(input.displayName, 'displayName'),
    properties: input.properties || {}
  };
  if (input.description) body.description = input.description;

  return await runJsonBodyCommand({
    label: 'Create schema',
    args: ['schema', 'create'],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function createDeltaTable(input) {
  const config = workflowConfig(input.config || {});
  const tableType = input.tableType || 'MANAGED';
  const columns = (input.columns || []).map(normalizeField);
  if (!columns.length) throw new Error('columns must include at least one field.');

  const body = {
    catalogKey: requireValue(input.catalogKey, 'catalogKey'),
    schemaKey: requireValue(input.schemaKey, 'schemaKey'),
    displayName: requireValue(input.displayName, 'displayName'),
    tableType,
    tableFields: columns
  };
  if (input.description) body.description = input.description;
  if (input.partitionKeys?.length) body.partitionKeys = input.partitionKeys.map(normalizeField);
  const tableProperties = normalizeTableProperties(input.tableProperties);
  if (tableProperties.length) body.tableProperties = tableProperties;

  if (tableType === 'EXTERNAL') {
    body.externalTableDefinition = {
      externalTableDataFormat: 'DELTA',
      externalTableLocationType: input.externalTableLocationType || 'OBJECT_STORAGE',
      objectStorageLocationPath: requireValue(input.objectStorageLocationPath, 'objectStorageLocationPath for EXTERNAL Delta tables')
    };
  } else {
    body.managedTableDefinition = {
      managedTableDataFormat: 'DELTA'
    };
  }

  return await runJsonBodyCommand({
    label: 'Create Delta table',
    args: ['schema', 'create-table'],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function createTableWithData(input) {
  const config = workflowConfig(input.config || {});
  const source = dataTableSource(input);
  const fileFormat = input.fileFormat || inferFileFormat(input);
  if (source.kind === 'rows' && fileFormat !== 'CSV') {
    throw new Error('Inline rows are staged as CSV. Use localDataFile or objectStorageLocationPath for non-CSV fileFormat values.');
  }

  const body = buildCreateDataTableBody(input, source.objectStorageLocationPath || '<generated-oci-file-path>', fileFormat);
  const command = scrubCommand(['schema', 'create-data-table', '--body', '@<generated-json>', ...commonFlags(config)]);

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      label: 'Create table with data',
      source,
      staging: source.kind === 'objectStorage'
        ? null
        : {
            generateUploadTargetCommand: scrubCommand(['schema', 'generate-temp-file-upload-target', input.schemaKey, ...commonFlags(config)]),
            uploadMethod: input.uploadMethod || 'PUT',
            uploadContentType: input.uploadContentType || contentTypeForFormat(fileFormat),
            uploadTarget: '<tempFileUploadTarget>',
            objectStorageLocationPath: '<generated-oci-file-path>'
          },
      command,
      body,
      dataPreview: source.kind === 'rows' ? truncate(rowsToCsv(input.rows, body.selectedColumns, input.csvIncludeHeader !== false), 2000) : undefined
    }, null, 2));
  }

  let tempDir = '';
  try {
    let objectStorageLocationPath = source.objectStorageLocationPath;
    let uploadSummary = null;

    if (!objectStorageLocationPath) {
      tempDir = await mkdtemp(path.join(tmpdir(), 'ask-aidp-data-'));
      const dataFile = source.kind === 'rows'
        ? writeRowsDataFile(input, body.selectedColumns, tempDir)
        : validateLocalDataFile(input.localDataFile, input.maxUploadBytes || 10485760);

      const uploadTarget = await runAidp(['schema', 'generate-temp-file-upload-target', input.schemaKey], {
        config,
        timeoutSeconds: input.timeoutSeconds || 120
      });
      if (uploadTarget.exitCode !== 0) {
        return toolText(JSON.stringify({
          created: false,
          stage: 'generate-temp-file-upload-target',
          command: uploadTarget.command,
          exitCode: uploadTarget.exitCode,
          response: safeData(uploadTarget)
        }, null, 2), true);
      }

      const uploadDetails = extractTempUploadDetails(safeData(uploadTarget));
      const uploadUrl = resolveUploadUrl(uploadDetails.tempFileUploadTarget, input.uploadTargetBaseUrl);
      const upload = await uploadFileToUrl(dataFile.path, uploadUrl, {
        method: input.uploadMethod || 'PUT',
        contentType: input.uploadContentType || contentTypeForFormat(fileFormat),
        timeoutSeconds: input.timeoutSeconds || 120
      });
      objectStorageLocationPath = requireValue(uploadDetails.ociFilePath, 'ociFilePath from generate-temp-file-upload-target response');
      uploadSummary = {
        generateUploadTargetCommand: uploadTarget.command,
        uploadKey: uploadDetails.uploadKey,
        ociFilePath: uploadDetails.ociFilePath,
        localPath: dataFile.path,
        bytes: dataFile.bytes,
        method: upload.method,
        statusCode: upload.statusCode,
        responseBody: upload.responseBody
      };
    }

    const createBody = buildCreateDataTableBody(input, objectStorageLocationPath, fileFormat);
    const create = await runGeneratedJsonCommand(['schema', 'create-data-table'], createBody, config, input.timeoutSeconds || 120);
    const response = {
      created: create.exitCode === 0,
      command: create.command,
      exitCode: create.exitCode,
      upload: uploadSummary,
      body: createBody,
      response: safeData(create)
    };
    return toolText(JSON.stringify(response, null, 2), create.exitCode !== 0);
  } finally {
    if (tempDir) await rm(tempDir, { recursive: true, force: true });
  }
}

async function generateCsvTableSql(input) {
  const fullTableName = input.fullTableName
    ? normalizeSqlIdentifierPath(input.fullTableName)
    : [
        requireValue(input.catalogName, 'catalogName or fullTableName'),
        requireValue(input.schemaName, 'schemaName or fullTableName'),
        requireValue(input.tableName, 'tableName or fullTableName')
      ].map(sqlIdentifier).join('.');
  const columns = (input.columns || []).map((column) => {
    const suffix = column.comment ? ` COMMENT ${sqlString(column.comment)}` : '';
    return `    ${sqlIdentifier(requireValue(column.name, 'columns[].name')).padEnd(20)} ${requireValue(column.type, 'columns[].type')}${suffix}`;
  });
  if (!columns.length) throw new Error('columns must include at least one SQL column.');

  const lines = [];
  if (input.includeSqlMagic !== false) lines.push('%sql');
  if (input.comments !== false) {
    lines.push('-- Create a CSV-backed table and treat the first row as column headers.');
    lines.push('-- Keep the source path pointed at the CSV folder or file location.');
  }
  lines.push(`CREATE TABLE ${fullTableName} (`);
  lines.push(columns.join(',\n'));
  lines.push(')');
  lines.push('USING CSV');
  lines.push('OPTIONS (');
  lines.push(`  path ${sqlString(requireValue(input.path, 'path'))},`);
  lines.push(`  header ${sqlString(String(input.header !== false))}`);
  lines.push(')');
  return toolText(JSON.stringify({ sql: lines.join('\n') }, null, 2));
}

function normalizeSqlIdentifierPath(value) {
  return String(value).split('.').map((part) => sqlIdentifier(part.trim())).join('.');
}

function sqlIdentifier(value) {
  const text = requireValue(value, 'SQL identifier');
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(text)) return text;
  return `\`${String(text).replace(/`/g, '``')}\``;
}

function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function listCatalogs(input) {
  const config = workflowConfig(input.config || {});
  const args = ['catalog', 'list'];
  if (input.displayName) args.push('--display-name', input.displayName);
  if (input.catalogState) args.push('--catalog-state', input.catalogState);
  if (input.catalogType) args.push('--catalog-type', input.catalogType);
  if (input.limit) args.push('--limit', String(input.limit));
  if (input.page) args.push('--page', input.page);
  if (input.sortOrder) args.push('--sort-order', input.sortOrder);
  if (input.sortBy) args.push('--sort-by', input.sortBy);

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      command: scrubCommand([...args, ...commonFlags(config)])
    }, null, 2));
  }

  const result = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
  return toolText(JSON.stringify({
    command: result.command,
    exitCode: result.exitCode,
    response: safeData(result)
  }, null, 2), result.exitCode !== 0);
}

async function getCatalog(input) {
  const config = workflowConfig(input.config || {});
  const args = ['catalog', 'get', requireValue(input.catalogKey, 'catalogKey')];
  if (input.isCatalogGuid === true) args.push('--is-catalog-guid', 'true');
  if (input.shouldUpdateRecent !== undefined) args.push('--should-update-recent', String(input.shouldUpdateRecent));

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      command: scrubCommand([...args, ...commonFlags(config)])
    }, null, 2));
  }

  const result = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
  return toolText(JSON.stringify({
    command: result.command,
    exitCode: result.exitCode,
    response: safeData(result)
  }, null, 2), result.exitCode !== 0);
}

async function createCatalog(input) {
  const config = workflowConfig(input.config || {});
  const body = buildCreateCatalogBody(input);

  return await runJsonBodyCommand({
    label: 'Create catalog',
    args: ['catalog', 'create'],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function createExternalCatalog(input) {
  const config = workflowConfig(input.config || {});
  const body = buildCreateCatalogBody({
    ...input,
    catalogType: 'EXTERNAL',
    sourceType: requireValue(input.sourceType, 'sourceType'),
    connectionProperties: input.connectionProperties || {}
  });

  return await runJsonBodyCommand({
    label: 'Create external catalog',
    args: ['catalog', 'create'],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function autoHealWorkflow(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const jobRunKey = requireValue(input.jobRunKey, 'jobRunKey');
  const statuses = new Set((input.statuses || ['FAILED', 'INTERNAL_ERROR', 'TIMED_OUT', 'CANCELED', 'UPSTREAM_FAILED', 'UPSTREAM_CANCELED']).map((status) => String(status).toUpperCase()));
  const parameters = (input.parameters || []).map(normalizeParameter);
  let selectedTaskKeys = Array.from(new Set(input.taskKeys || []));
  const inspection = {};

  if (!selectedTaskKeys.length) {
    const jobRun = await runAidp(['workflow', 'get-job-run', workspaceKey, jobRunKey], { config, timeoutSeconds: input.timeoutSeconds || 120 });
    inspection.jobRun = {
      exitCode: jobRun.exitCode,
      command: jobRun.command,
      data: safeData(jobRun)
    };
    if (jobRun.exitCode !== 0) return toolText(JSON.stringify(inspection, null, 2), true);

    const jobStatus = inspection.jobRun.data?.state?.status || inspection.jobRun.data?.status || '';
    if (jobStatus && !isTerminal(jobStatus)) {
      return toolText(JSON.stringify({
        dryRun: input.dryRun === true,
        repaired: false,
        noAction: true,
        reason: `Job run is not terminal: ${jobStatus}`,
        jobRunKey,
        inspection
      }, null, 2));
    }

    const taskRuns = await runAidp(['workflow', 'list-task-runs', workspaceKey, '--job-run-key', jobRunKey, '--limit', '100', '--sort-by', 'timeCreated', '--sort-order', 'ASC'], { config, timeoutSeconds: input.timeoutSeconds || 120 });
    inspection.taskRuns = {
      exitCode: taskRuns.exitCode,
      command: taskRuns.command,
      data: safeData(taskRuns)
    };
    if (taskRuns.exitCode !== 0) return toolText(JSON.stringify(inspection, null, 2), true);

    const items = inspection.taskRuns.data?.items || inspection.taskRuns.data?.taskRunCollection?.items || [];
    selectedTaskKeys = Array.from(new Set(items
      .filter((task) => statuses.has(taskStatus(task)))
      .map((task) => task.taskKey)
      .filter(Boolean)));
  }

  const body = { taskKeys: selectedTaskKeys };
  if (parameters.length) body.parameters = parameters;
  const command = scrubCommand(['workflow', 'repair-job-run', workspaceKey, jobRunKey, '--body', '@<generated-json>', ...commonFlags(config)]);

  if (!selectedTaskKeys.length) {
    return toolText(JSON.stringify({
      dryRun: input.dryRun === true,
      repaired: false,
      noAction: true,
      reason: `No task runs matched statuses: ${Array.from(statuses).join(', ')}`,
      jobRunKey,
      body,
      command,
      inspection
    }, null, 2));
  }

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      repaired: false,
      wouldRepair: true,
      jobRunKey,
      selectedTaskKeys,
      command,
      body,
      inspection
    }, null, 2));
  }

  const repair = await runGeneratedJsonCommand(['workflow', 'repair-job-run', workspaceKey, jobRunKey], body, config, input.timeoutSeconds || 120);
  const repairData = safeData(repair);
  const response = {
    repaired: repair.exitCode === 0,
    jobRunKey,
    selectedTaskKeys,
    command: repair.command,
    exitCode: repair.exitCode,
    repairResponse: repairData,
    inspection
  };
  if (repair.exitCode !== 0) return toolText(JSON.stringify(response, null, 2), true);

  const repairedJobRunKey = repairData?.key || repairData?.jobRun?.key || jobRunKey;
  if (input.pollToCompletion === true) {
    response.polls = [];
    const deadline = Date.now() + (input.pollTimeoutSeconds || 1800) * 1000;
    while (Date.now() < deadline) {
      const poll = await runAidp(['workflow', 'get-job-run', workspaceKey, repairedJobRunKey], { config, timeoutSeconds: input.timeoutSeconds || 120 });
      const pollData = safeData(poll);
      const status = pollData?.state?.status || pollData?.status || '';
      response.polls.push({ exitCode: poll.exitCode, status, data: pollData });
      if (poll.exitCode !== 0 || isTerminal(status)) break;
      await sleep((input.pollIntervalSeconds || 20) * 1000);
    }
    response.finalStatus = response.polls.at(-1)?.status || '';
  }

  return toolText(JSON.stringify(response, null, 2));
}

async function runJsonBodyCommand({ label, args, body, config, dryRun, timeoutSeconds }) {
  const displayArgs = [...args, '--body', '@<generated-json>', ...commonFlags(config)];
  if (dryRun) {
    return toolText(JSON.stringify({
      dryRun: true,
      label,
      command: scrubCommand(displayArgs),
      body
    }, null, 2));
  }

  const result = await runGeneratedJsonCommand(args, body, config, timeoutSeconds);
  const response = {
    label,
    command: result.command,
    exitCode: result.exitCode,
    response: safeData(result)
  };
  return toolText(JSON.stringify(response, null, 2), result.exitCode !== 0);
}

async function runGeneratedJsonCommand(args, body, config, timeoutSeconds) {
  let tempDir = '';
  try {
    tempDir = await mkdtemp(path.join(tmpdir(), 'ask-aidp-'));
    const bodyPath = path.join(tempDir, 'body.json');
    writeJson(bodyPath, body);
    return await runAidp([...args, '--body', `@${bodyPath}`], { config, timeoutSeconds });
  } finally {
    if (tempDir) await rm(tempDir, { recursive: true, force: true });
  }
}

function normalizeField(field) {
  const normalized = {
    fieldName: requireValue(field?.fieldName, 'fieldName'),
    fieldType: requireValue(field?.fieldType, 'fieldType')
  };
  if (field.fieldDescription) normalized.fieldDescription = field.fieldDescription;
  if (field.fieldPrecision) normalized.fieldPrecision = field.fieldPrecision;
  if (field.fieldScale) normalized.fieldScale = field.fieldScale;
  return normalized;
}

function normalizeDataTableField(field, rows = []) {
  const normalized = {
    fieldName: requireValue(field?.fieldName, 'fieldName')
  };
  const fieldType = field.fieldType || inferFieldType(rows, field.fieldName);
  if (fieldType) normalized.fieldType = fieldType;
  if (field.fieldDescription) normalized.fieldDescription = field.fieldDescription;
  if (field.fieldPrecision) normalized.fieldPrecision = field.fieldPrecision;
  if (field.fieldScale) normalized.fieldScale = field.fieldScale;
  return normalized;
}

function buildCreateDataTableBody(input, objectStorageLocationPath, fileFormat) {
  const rows = Array.isArray(input.rows) ? input.rows : [];
  const tableFields = buildDataTableFields(input, rows);
  const selectedColumns = input.selectedColumns?.length
    ? input.selectedColumns
    : tableFields.map((field) => field.fieldName);
  const body = {
    catalogKey: requireValue(input.catalogKey, 'catalogKey'),
    schemaKey: requireValue(input.schemaKey, 'schemaKey'),
    displayName: requireValue(input.displayName, 'displayName'),
    fileFormat,
    managedTableDefinition: {
      managedTableDataFormat: input.managedTableDataFormat || 'DELTA'
    },
    objectStorageLocationPath: requireValue(objectStorageLocationPath, 'objectStorageLocationPath'),
    selectedColumns,
    tableFields
  };
  if (input.description) body.description = input.description;
  if (input.partitionKeys?.length) body.partitionKeys = input.partitionKeys.map((field) => normalizeDataTableField(field, rows));
  const tableProperties = normalizeTableProperties(input.tableProperties);
  if (tableProperties.length) body.tableProperties = tableProperties;
  return body;
}

function buildDataTableFields(input, rows) {
  if (input.columns?.length) return input.columns.map((field) => normalizeDataTableField(field, rows));
  if (rows.length) {
    const fieldNames = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
    if (!fieldNames.length) throw new Error('rows must include at least one property when columns are omitted.');
    return fieldNames.map((fieldName) => normalizeDataTableField({ fieldName }, rows));
  }
  throw new Error('columns are required when rows are not provided.');
}

function inferFieldType(rows, fieldName) {
  const values = rows.map((row) => row[fieldName]).filter((value) => value !== null && value !== undefined);
  if (!values.length) return 'STRING';
  if (values.every((value) => typeof value === 'boolean')) return 'BOOLEAN';
  if (values.every((value) => typeof value === 'number' && Number.isInteger(value))) return 'INT';
  if (values.every((value) => typeof value === 'number')) return 'DOUBLE';
  return 'STRING';
}

function dataTableSource(input) {
  const sources = [
    input.objectStorageLocationPath ? 'objectStorage' : '',
    input.localDataFile ? 'localDataFile' : '',
    Array.isArray(input.rows) && input.rows.length ? 'rows' : ''
  ].filter(Boolean);
  if (sources.length !== 1) {
    throw new Error('Provide exactly one data source: objectStorageLocationPath, localDataFile, or rows.');
  }
  if (sources[0] === 'objectStorage') {
    return { kind: 'objectStorage', objectStorageLocationPath: input.objectStorageLocationPath };
  }
  if (sources[0] === 'localDataFile') {
    return { kind: 'localDataFile', localDataFile: path.resolve(input.localDataFile) };
  }
  return { kind: 'rows', rowCount: input.rows.length };
}

function inferFileFormat(input) {
  if (input.fileFormat) return input.fileFormat;
  if (input.localDataFile) {
    const ext = path.extname(input.localDataFile).toLowerCase();
    if (ext === '.json' || ext === '.jsonl') return 'JSON';
    if (ext === '.parquet') return 'PARQUET';
    if (ext === '.orc') return 'ORC';
    if (ext === '.avro') return 'AVRO';
    if (ext === '.delta') return 'DELTA';
  }
  return 'CSV';
}

function rowsToCsv(rows, selectedColumns, includeHeader = true) {
  const lines = [];
  if (includeHeader) lines.push(selectedColumns.map(csvCell).join(','));
  for (const row of rows) {
    lines.push(selectedColumns.map((column) => csvCell(row[column])).join(','));
  }
  return `${lines.join('\n')}\n`;
}

function csvCell(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function writeRowsDataFile(input, selectedColumns, tempDir) {
  const csv = rowsToCsv(input.rows, selectedColumns, input.csvIncludeHeader !== false);
  const bytes = Buffer.byteLength(csv);
  const maxUploadBytes = input.maxUploadBytes || 10485760;
  if (bytes > maxUploadBytes) throw new Error(`Generated CSV is ${bytes} bytes, exceeding maxUploadBytes=${maxUploadBytes}.`);
  const dataPath = path.join(tempDir, `${safeName(input.displayName || 'aidp_table_data')}.csv`);
  writeFileSync(dataPath, csv);
  return { path: dataPath, bytes };
}

function validateLocalDataFile(localDataFile, maxUploadBytes) {
  const dataPath = path.resolve(requireValue(localDataFile, 'localDataFile'));
  const stats = statSync(dataPath);
  if (!stats.isFile()) throw new Error(`localDataFile is not a file: ${dataPath}`);
  if (stats.size > maxUploadBytes) throw new Error(`localDataFile is ${stats.size} bytes, exceeding maxUploadBytes=${maxUploadBytes}.`);
  return { path: dataPath, bytes: stats.size };
}

function extractTempUploadDetails(data) {
  const details = findObjectWithKeys(data, ['tempFileUploadTarget', 'ociFilePath']);
  if (!details) throw new Error('generate-temp-file-upload-target response did not include tempFileUploadTarget and ociFilePath.');
  return details;
}

function findObjectWithKeys(value, keys, depth = 0) {
  if (!value || typeof value !== 'object' || depth > 6) return null;
  if (keys.every((key) => Object.prototype.hasOwnProperty.call(value, key))) return value;
  for (const child of Object.values(value)) {
    const found = findObjectWithKeys(child, keys, depth + 1);
    if (found) return found;
  }
  return null;
}

function resolveUploadUrl(tempFileUploadTarget, uploadTargetBaseUrl) {
  const target = requireValue(tempFileUploadTarget, 'tempFileUploadTarget');
  if (/^https?:\/\//i.test(target)) return target;
  const base = requireValue(uploadTargetBaseUrl, 'uploadTargetBaseUrl because tempFileUploadTarget is relative');
  return `${base.replace(/\/+$/, '')}/${target.replace(/^\/+/, '')}`;
}

function contentTypeForFormat(fileFormat) {
  switch (fileFormat) {
    case 'CSV':
      return 'text/csv';
    case 'JSON':
      return 'application/json';
    case 'TEXTFILE':
      return 'text/plain';
    default:
      return 'application/octet-stream';
  }
}

async function uploadFileToUrl(filePath, uploadUrl, options) {
  const buffer = readFileSync(filePath);
  const parsed = new URL(uploadUrl);
  const request = parsed.protocol === 'http:' ? httpRequest : httpsRequest;
  const method = options.method || 'PUT';
  const timeoutMs = (options.timeoutSeconds || 120) * 1000;

  return await new Promise((resolve, reject) => {
    const req = request(parsed, {
      method,
      headers: {
        'content-length': String(buffer.length),
        'content-type': options.contentType || 'application/octet-stream'
      }
    }, (res) => {
      let responseBody = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { responseBody += chunk; });
      res.on('end', () => {
        const summary = {
          method,
          statusCode: res.statusCode || 0,
          responseBody: truncate(responseBody.trim(), 2000)
        };
        if ((res.statusCode || 0) < 200 || (res.statusCode || 0) >= 300) {
          reject(new Error(`Temp file upload failed with HTTP ${res.statusCode}: ${summary.responseBody}`));
          return;
        }
        resolve(summary);
      });
    });
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Temp file upload timed out after ${options.timeoutSeconds || 120}s`));
    });
    req.on('error', reject);
    req.end(buffer);
  });
}

function buildCreateCatalogBody(input) {
  const body = {
    displayName: requireValue(input.displayName, 'displayName')
  };
  const hasConnectionProperties = input.connectionProperties !== undefined;
  if (input.catalogType) body.catalogType = input.catalogType;
  if (!body.catalogType && (input.sourceType || hasConnectionProperties)) body.catalogType = 'EXTERNAL';
  if (input.sourceType) body.sourceType = input.sourceType;
  if (hasConnectionProperties) {
    body.connectionDetails = {
      connectionProperties: input.connectionProperties || {}
    };
  }
  if (input.description) body.description = input.description;
  if (input.properties) body.properties = input.properties;
  return body;
}

function normalizeTableProperties(input) {
  if (!input) return [];
  if (Array.isArray(input)) {
    return input.map((property) => ({
      propertyName: requireValue(property.propertyName, 'tableProperties[].propertyName'),
      propertyValue: String(requireValue(property.propertyValue, 'tableProperties[].propertyValue'))
    }));
  }
  if (typeof input === 'object') {
    return Object.entries(input).map(([propertyName, propertyValue]) => ({
      propertyName,
      propertyValue: String(propertyValue)
    }));
  }
  throw new Error('tableProperties must be an object map or an array of {propertyName, propertyValue}.');
}

function normalizeParameter(parameter) {
  const normalized = {
    name: requireValue(parameter?.name, 'parameters[].name')
  };
  if (parameter.value !== undefined) normalized.value = String(parameter.value);
  return normalized;
}

function taskStatus(task) {
  return String(task?.state?.status || task?.status || '').toUpperCase();
}

function buildUploadEntries(input) {
  const localRoot = path.resolve(requireValue(input.localPath, 'localPath'));
  const workspaceRoot = normalizeWorkspacePath(requireValue(input.workspacePath, 'workspacePath'));
  const maxFiles = input.maxFiles || 500;
  const includeGlobs = input.includeGlobs || [];
  const excludeGlobs = input.excludeGlobs || ['.git/**', 'node_modules/**', '.venv/**', '__pycache__/**'];
  const rootStat = statSync(localRoot);
  const entries = [];

  if (rootStat.isFile()) {
    entries.push({ kind: 'file', localPath: localRoot, relativePath: path.basename(localRoot), workspacePath: workspaceRoot });
    return entries;
  }

  if (!rootStat.isDirectory()) throw new Error(`localPath is neither a file nor directory: ${localRoot}`);
  entries.push({ kind: 'folder', localPath: localRoot, relativePath: '.', workspacePath: workspaceRoot });
  walkUploadDirectory(localRoot, localRoot, workspaceRoot, entries, {
    recursive: input.recursive !== false,
    includeGlobs,
    excludeGlobs,
    maxFiles
  });
  const fileCount = entries.filter((entry) => entry.kind === 'file').length;
  if (fileCount > maxFiles) throw new Error(`Upload plan has ${fileCount} files, exceeding maxFiles=${maxFiles}.`);
  return entries;
}

function walkUploadDirectory(root, current, workspaceRoot, entries, options) {
  for (const name of readdirSync(current)) {
    const fullPath = path.join(current, name);
    const rel = toPosix(path.relative(root, fullPath));
    if (matchesAnyGlob(rel, options.excludeGlobs)) continue;
    const stat = statSync(fullPath);
    const workspacePath = joinWorkspacePath(workspaceRoot, rel);
    if (stat.isDirectory()) {
      entries.push({ kind: 'folder', localPath: fullPath, relativePath: rel, workspacePath });
      if (options.recursive) walkUploadDirectory(root, fullPath, workspaceRoot, entries, options);
    } else if (stat.isFile() && (!options.includeGlobs.length || matchesAnyGlob(rel, options.includeGlobs))) {
      entries.push({ kind: 'file', localPath: fullPath, relativePath: rel, workspacePath });
    }
  }
}

function uploadEntryCommand(workspaceKey, entry, overwrite, config) {
  const args = entry.kind === 'folder'
    ? ['workspace-object', 'create', workspaceKey, '--path', entry.workspacePath, '--type', 'FOLDER', '--is-overwrite', String(overwrite), '--body', '{}']
    : ['workspace-object', 'create', workspaceKey, '--path', entry.workspacePath, '--type', 'FILE', '--is-overwrite', String(overwrite), '--body', `@${entry.localPath}`];
  return scrubCommand([...args, ...commonFlags(config)]);
}

function normalizeWorkspacePath(value) {
  return value.startsWith('/') ? value : `/${value}`;
}

function joinWorkspacePath(root, rel) {
  if (!rel || rel === '.') return root;
  return `${root.replace(/\/+$/, '')}/${toPosix(rel).replace(/^\/+/, '')}`;
}

function toPosix(value) {
  return value.split(path.sep).join('/');
}

function matchesAnyGlob(value, patterns) {
  return patterns.some((pattern) => globToRegExp(pattern).test(value));
}

function globToRegExp(pattern) {
  let source = '';
  for (let index = 0; index < pattern.length; index += 1) {
    const char = pattern[index];
    const next = pattern[index + 1];
    const afterNext = pattern[index + 2];
    if (char === '*' && next === '*' && afterNext === '/') {
      source += '(?:.*/)?';
      index += 2;
      continue;
    }
    if (char === '*' && next === '*') {
      source += '.*';
      index += 1;
      continue;
    }
    if (char === '*') {
      source += '[^/]*';
      continue;
    }
    if (char === '?') {
      source += '[^/]';
      continue;
    }
    source += char.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`^${source}$`);
}

async function notebookWorkflow(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const clusterKey = requireValue(config.clusterKey, 'clusterKey or AIDP_CLUSTER_KEY');
  const runId = input.runId || utcRunId();
  const notebookFolder = input.notebookFolder || `/Workspace/aidp_codex_notebook_workflow_${runId}`;
  const jobPath = input.jobPath || '/Workspace/jobs';
  const clusterName = input.clusterName || process.env.AIDP_CLUSTER_NAME || clusterKey;
  const outputFormat = input.outputFormat || 'IPYNB';
  const evidenceDir = path.resolve(input.evidenceDir || path.join(process.cwd(), 'qa-runs', runId, 'ask-aidp-plugin'));
  const reqDir = path.join(evidenceDir, 'requests');
  const respDir = path.join(evidenceDir, 'responses');
  const logsDir = path.join(evidenceDir, 'logs');
  const transcript = path.join(evidenceDir, 'commands-and-responses.md');
  mkdirSync(reqDir, { recursive: true });
  mkdirSync(respDir, { recursive: true });
  mkdirSync(logsDir, { recursive: true });

  writeFileSync(transcript, [
    '# Ask AIDP Plugin request-response transcript',
    '',
    `runId=${runId}`,
    `evidenceDir=${evidenceDir}`,
    ''
  ].join('\n'));

  const notebooks = buildWorkflowNotebookSpecs(input, runId, notebookFolder);

  writeJson(path.join(reqDir, 'create-content.json'), { ext: '.ipynb', type: 'notebook' });
  for (const notebook of notebooks) {
    const notebookPath = `${notebookFolder}/${notebook.name}`;
    writeJson(path.join(reqDir, `rename-notebook-${notebook.stage}.json`), { path: notebookPath });
    writeJson(path.join(reqDir, `update-notebook-${notebook.stage}.json`), {
      name: notebook.name,
      path: notebookPath,
      type: 'notebook',
      format: 'json',
      content: notebookContent(notebook.stage, notebook.source, notebook.taskKey)
    });
  }
  const createJobBody = {
    name: `aidp_codex_notebook_workflow_${notebooks.length}_${runId}.job`,
    description: `Codex Ask AIDP plugin workflow that runs ${notebooks.length} notebooks in sequence.`,
    path: jobPath,
    maxConcurrentRuns: 1,
    jobClusters: [{ clusterKey, clusterName }],
    tasks: notebooks.map((notebook) => ({
      type: 'NOTEBOOK_TASK',
      taskKey: notebook.taskKey,
      dependsOn: notebook.dependsOn,
      runIf: 'ALL_SUCCESS',
      maxRetries: 0,
      isRetryOnTimeout: false,
      notebookPath: `${notebookFolder}/${notebook.name}`,
      cluster: { clusterKey, clusterName },
      parameters: [],
      source: 'WORKSPACE'
    })),
    parameters: []
  };
  writeJson(path.join(reqDir, 'create-job.json'), createJobBody);
  writeJson(path.join(reqDir, 'export-task-output.json'), { format: outputFormat });

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      runId,
      notebookFolder,
      notebookCount: notebooks.length,
      notebooks: notebooks.map((notebook) => ({
        name: notebook.name,
        taskKey: notebook.taskKey,
        dependsOn: notebook.dependsOn,
        path: `${notebookFolder}/${notebook.name}`
      })),
      createJobBody,
      requestDir: reqDir,
      evidenceDir
    }, null, 2));
  }

  const runStep = async (label, args, outputFile, options = {}) => {
    const result = await runAidp(args, { config, timeoutSeconds: options.timeoutSeconds || 120 });
    const rawPath = path.join(options.rawDir || respDir, outputFile);
    writeFileSync(rawPath, `Response:\n${result.stdout}${result.stderr}`);
    appendTranscript(transcript, label, result.command, result, rawPath, evidenceDir);
    if (result.exitCode !== 0 && options.throwOnError !== false) throw new Error(`AIDP CLI step failed: ${label}\n${resultToText(result, 12000)}`);
    return result;
  };

  await runStep('Get workspace', ['workspace', 'get', workspaceKey], '00-workspace-get.txt');
  await runStep('Get cluster', ['cluster', 'get', workspaceKey, clusterKey], '01-cluster-get.txt');
  await runStep('Create notebook folder', ['workspace-object', 'create', workspaceKey, '--path', notebookFolder, '--type', 'FOLDER', '--is-overwrite', 'true', '--body', '{}'], '02-create-notebook-folder.txt');

  for (const notebook of notebooks) {
    const createFile = `1${notebook.stage}-notebook-create-content.txt`;
    const create = await runStep(`Create notebook ${notebook.stage} shell`, ['notebook', 'create-content', workspaceKey, notebookFolder, '--body', `@${path.join(reqDir, 'create-content.json')}`], createFile);
    const createdPath = responseRoot(create).path || responseRoot(create).content?.path;
    await runStep(`Rename notebook ${notebook.stage}`, ['notebook', 'modify-content', workspaceKey, createdPath, '--body', `@${path.join(reqDir, `rename-notebook-${notebook.stage}.json`)}`], `2${notebook.stage}-notebook-modify-content.txt`);
    await runStep(`Upload notebook ${notebook.stage} JSON`, ['notebook', 'update-content', workspaceKey, `${notebookFolder}/${notebook.name}`, '--body', `@${path.join(reqDir, `update-notebook-${notebook.stage}.json`)}`], `3${notebook.stage}-notebook-update-content.txt`);
  }

  const job = await runStep('Create workflow job', ['workflow', 'create-job', workspaceKey, '--body', `@${path.join(reqDir, 'create-job.json')}`], '40-workflow-create-job.txt');
  const jobKey = requireValue(responseRoot(job).key || responseRoot(job).job?.key, 'created job key');
  writeJson(path.join(reqDir, 'create-job-run.json'), { jobKey, parameters: [] });

  const jobRun = await runStep('Create workflow job run', ['workflow', 'create-job-run', workspaceKey, '--body', `@${path.join(reqDir, 'create-job-run.json')}`], '41-workflow-create-job-run.txt');
  const jobRunKey = requireValue(responseRoot(jobRun).key || responseRoot(jobRun).jobRun?.key, 'created job run key');

  let finalJobRun = null;
  const deadline = Date.now() + (input.pollTimeoutSeconds || 1800) * 1000;
  let poll = 0;
  while (Date.now() < deadline) {
    poll += 1;
    const result = await runStep(`Poll workflow job run ${poll}`, ['workflow', 'get-job-run', workspaceKey, jobRunKey], `50-workflow-get-job-run-poll-${String(poll).padStart(2, '0')}.txt`, { throwOnError: false });
    if (result.exitCode !== 0) {
      writeFileSync(path.join(evidenceDir, 'last-poll-error.txt'), resultToText(result, 12000));
      await sleep((input.pollIntervalSeconds || 20) * 1000);
      continue;
    }
    let root;
    try {
      root = responseRoot(result);
    } catch (error) {
      writeFileSync(path.join(evidenceDir, 'last-poll-parse-error.txt'), `${error.message}\n\n${resultToText(result, 12000)}`);
      await sleep((input.pollIntervalSeconds || 20) * 1000);
      continue;
    }
    const status = root.state?.status || root.status;
    if (isTerminal(status)) {
      finalJobRun = root;
      writeFileSync(path.join(evidenceDir, 'job-run-final.json'), JSON.stringify(root, null, 2));
      break;
    }
    await sleep((input.pollIntervalSeconds || 20) * 1000);
  }
  if (!finalJobRun) {
    const summary = {
      runId,
      notebookFolder,
      jobKey,
      jobRunKey,
      jobRunStatus: 'POLL_TIMEOUT',
      notebookCount: notebooks.length,
      evidenceDir,
      transcript,
      message: `Timed out waiting for workflow job run ${jobRunKey}`
    };
    writeFileSync(path.join(evidenceDir, 'summary.json'), JSON.stringify(summary, null, 2));
    return toolText(JSON.stringify(summary, null, 2), true);
  }

  const list = await runStep('List task runs', ['workflow', 'list-task-runs', workspaceKey, '--job-run-key', jobRunKey, '--limit', '100', '--sort-by', 'timeCreated', '--sort-order', 'ASC'], '60-workflow-list-task-runs.txt');
  const taskRuns = responseRoot(list).items || responseRoot(list).taskRunCollection?.items || [];
  const taskOrder = new Map(notebooks.map((notebook, index) => [notebook.taskKey, index]));
  taskRuns.sort((left, right) => {
    const leftOrder = taskOrder.has(left.taskKey) ? taskOrder.get(left.taskKey) : Number.MAX_SAFE_INTEGER;
    const rightOrder = taskOrder.has(right.taskKey) ? taskOrder.get(right.taskKey) : Number.MAX_SAFE_INTEGER;
    return leftOrder - rightOrder;
  });
  writeFileSync(path.join(evidenceDir, 'task-runs.json'), JSON.stringify(taskRuns, null, 2));

  for (let index = 0; index < taskRuns.length; index += 1) {
    const task = taskRuns[index];
    const taskKey = task.taskKey || `task-${index + 1}`;
    const safe = safeName(taskKey);
    const detail = await runStep(`Get task run ${index + 1} (${taskKey})`, ['workflow', 'get-task-run', workspaceKey, task.key], `70-task-run-${safe}.txt`);
    const detailRoot = responseRoot(detail);
    if (detailRoot.outputKey) {
      await runStep(`Export task output ${index + 1} (${taskKey})`, ['workflow', 'export-task-run-output', workspaceKey, task.key, detailRoot.outputKey, '--body', `@${path.join(reqDir, 'export-task-output.json')}`], `80-task-output-${safe}.txt`);
    }
  }

  await collectLogsInternal({
    config,
    workspaceKey,
    clusterKey,
    jobRunKey,
    evidenceDir,
    reqDir,
    logsDir,
    transcript,
    logLookbackMinutes: input.logLookbackMinutes || 60,
    limit: 1000
  });

  const status = finalJobRun.state?.status || finalJobRun.status || '';
  const summary = {
    runId,
    notebookFolder,
    jobKey,
    jobRunKey,
    jobRunStatus: status,
    notebookCount: notebooks.length,
    taskRuns: taskRuns.map((task) => ({ key: task.key, taskKey: task.taskKey, status: task.state?.status || task.status })),
    evidenceDir,
    transcript
  };
  writeFileSync(path.join(evidenceDir, 'summary.json'), JSON.stringify(summary, null, 2));
  if (String(status).toUpperCase() !== 'SUCCESS') return toolText(JSON.stringify({
    ...summary,
    message: `Workflow completed with non-success status: ${status}`
  }, null, 2), true);
  return toolText(JSON.stringify(summary, null, 2));
}

async function threeNotebookWorkflow(input) {
  return await notebookWorkflow({ ...input, notebookCount: 3 });
}

async function collectLogs(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const clusterKey = requireValue(config.clusterKey, 'clusterKey or AIDP_CLUSTER_KEY');
  const runId = utcRunId();
  const evidenceDir = path.resolve(input.evidenceDir || path.join(process.cwd(), 'qa-runs', runId, 'ask-aidp-plugin-logs'));
  const reqDir = path.join(evidenceDir, 'requests');
  const logsDir = path.join(evidenceDir, 'logs');
  const transcript = path.join(evidenceDir, 'commands-and-responses.md');
  mkdirSync(reqDir, { recursive: true });
  mkdirSync(logsDir, { recursive: true });
  writeFileSync(transcript, ['# AIDP log collection transcript', '', `jobRunKey=${input.jobRunKey}`, ''].join('\n'));

  await collectLogsInternal({
    config,
    workspaceKey,
    clusterKey,
    jobRunKey: input.jobRunKey,
    evidenceDir,
    reqDir,
    logsDir,
    transcript,
    logLookbackMinutes: input.logLookbackMinutes || 60,
    limit: input.limit || 1000
  });

  return toolText(JSON.stringify({ jobRunKey: input.jobRunKey, evidenceDir, transcript }, null, 2));
}

async function uploadWorkspaceCode(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const entries = buildUploadEntries(input);
  const commands = entries.map((entry) => uploadEntryCommand(workspaceKey, entry, input.overwrite !== false, config));

  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      localPath: path.resolve(input.localPath),
      workspacePath: input.workspacePath,
      entryCount: entries.length,
      entries: entries.map((entry, index) => ({ ...entry, command: commands[index] }))
    }, null, 2));
  }

  const results = [];
  for (const entry of entries) {
    const args = entry.kind === 'folder'
      ? ['workspace-object', 'create', workspaceKey, '--path', entry.workspacePath, '--type', 'FOLDER', '--is-overwrite', String(input.overwrite !== false), '--body', '{}']
      : ['workspace-object', 'create', workspaceKey, '--path', entry.workspacePath, '--type', 'FILE', '--is-overwrite', String(input.overwrite !== false), '--body', `@${entry.localPath}`];
    const result = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
    results.push({ entry, command: result.command, exitCode: result.exitCode, response: safeData(result) });
    if (result.exitCode !== 0) return toolText(JSON.stringify({ uploaded: false, results }, null, 2), true);
  }

  return toolText(JSON.stringify({ uploaded: true, entryCount: entries.length, results }, null, 2));
}

async function createGitFolder(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const body = {
    folderPath: requireValue(input.folderPath, 'folderPath'),
    gitRepositoryUrl: requireValue(input.gitRepositoryUrl, 'gitRepositoryUrl'),
    branchName: requireValue(input.branchName, 'branchName'),
    credentialKey: requireValue(input.credentialKey, 'credentialKey')
  };
  if (input.gitProviderKey) body.gitProviderKey = input.gitProviderKey;
  if (input.description) body.description = input.description;

  return await runJsonBodyCommand({
    label: 'Create Git folder',
    args: ['workspace', 'create-git-folder', workspaceKey],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function gitCommitPush(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    commitPushDetails: addOptionalFields({
      commitMessage: requireValue(input.commitMessage, 'commitMessage')
    }, input, ['gitFolderPath', 'branchName', 'files', 'commitDescription'])
  }, input, ['ifMatch', 'opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  if (input.dryRun === true) return gitDryRun('GitClient#commitPushGitRepository', request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.commitPushGitRepository(request)));
  } finally {
    client.close?.();
  }
}

async function gitPull(input) {
  const config = workflowConfig(input.config || {});
  const details = addOptionalFields({}, input, ['gitFolderPath', 'branchName', 'remoteBranchName', 'pullAction', 'commitMessage']);
  if (!details.pullAction) details.pullAction = 'PULL';
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    gitPullDetails: details
  }, input, ['ifMatch', 'opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  if (input.dryRun === true) return gitDryRun('GitClient#pullGitRepository', request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.pullGitRepository(request)));
  } finally {
    client.close?.();
  }
}

async function gitGetRepository(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields(gitBaseRequest(input, config), input, ['shouldIncludeCredentialKey', 'opcRequestId']);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.getGitRepository(request)));
  } finally {
    client.close?.();
  }
}

async function gitOperationState(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields(gitBaseRequest(input, config), input, ['operationName', 'branchName', 'opcRequestId']);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.getGitOperationState(request)));
  } finally {
    client.close?.();
  }
}

async function gitListBranches(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields(gitBaseRequest(input, config), input, ['displayName', 'displayNameContains', 'limit', 'page', 'sortOrder', 'sortBy', 'opcRequestId']);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.listGitBranches(request)));
  } finally {
    client.close?.();
  }
}

async function gitCreateBranch(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    createGitBranchDetails: addOptionalFields({
      gitBranchName: requireValue(input.gitBranchName, 'gitBranchName')
    }, input, ['gitFolderPath'])
  }, input, ['opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  if (input.dryRun === true) return gitDryRun('GitClient#createGitBranch', request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.createGitBranch(request)));
  } finally {
    client.close?.();
  }
}

async function gitCheckoutBranch(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    checkoutBranchDetails: addOptionalFields({
      branchName: requireValue(input.branchName, 'branchName')
    }, input, ['gitFolderPath'])
  }, input, ['ifMatch', 'dhUserPrincipal', 'opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  if (input.dryRun === true) return gitDryRun('GitClient#checkoutBranch', request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.checkoutBranch(request)));
  } finally {
    client.close?.();
  }
}

async function gitListDiffs(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields(gitBaseRequest(input, config), input, ['gitFolderPath', 'branchName', 'compareTo', 'filter', 'limit', 'page', 'sortOrder', 'sortBy', 'displayName', 'opcRequestId']);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.listGitDiffs(request)));
  } finally {
    client.close?.();
  }
}

async function gitDiffDetail(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields(gitBaseRequest(input, config), input, ['gitFolderPath', 'branchName', 'gitFilePath', 'contextLines', 'maxPatchBytes', 'opcRequestId']);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.getGitDiffDetail(request)));
  } finally {
    client.close?.();
  }
}

async function gitMerge(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    gitMergeDetails: requireValue(input.details, 'details')
  }, input, ['ifMatch', 'opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  if (input.dryRun === true) return gitDryRun('GitClient#mergeGitRepository', request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.mergeGitRepository(request)));
  } finally {
    client.close?.();
  }
}

async function gitRebase(input) {
  const config = workflowConfig(input.config || {});
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    gitRebaseDetails: requireValue(input.details, 'details')
  }, input, ['ifMatch', 'opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  if (input.dryRun === true) return gitDryRun('GitClient#rebaseGitRepository', request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(await client.rebaseGitRepository(request)));
  } finally {
    client.close?.();
  }
}

async function gitReset(input) {
  const config = workflowConfig(input.config || {});
  const folderState = input.mode === 'folder_state';
  const request = addOptionalFields({
    ...gitBaseRequest(input, config),
    [folderState ? 'resetGitFolderStateDetails' : 'gitResetDetails']: requireValue(input.details, 'details')
  }, input, folderState ? ['ifMatch', 'opcRetryToken', 'opcRequestId'] : ['ifMatch', 'opcRetryToken', 'opcRequestId', 'shouldUpdateRecent']);
  const operation = folderState ? 'GitClient#resetGitFolderState' : 'GitClient#resetGitRepository';
  if (input.dryRun === true) return gitDryRun(operation, request, input, config);
  request.gitRepositoryKey = (await resolveGitRepositoryKey(input, config)).gitRepositoryKey;
  const client = await createGitClient(config);
  try {
    return toolText(sdkResponseToJson(folderState
      ? await client.resetGitFolderState(request)
      : await client.resetGitRepository(request)));
  } finally {
    client.close?.();
  }
}

async function trackRuns(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const summary = {};
  const evidenceDir = input.evidenceDir ? path.resolve(input.evidenceDir) : path.join(process.cwd(), 'qa-runs', utcRunId(), 'ask-aidp-run-tracking');
  const respDir = path.join(evidenceDir, 'responses');
  const reqDir = path.join(evidenceDir, 'requests');
  const logsDir = path.join(evidenceDir, 'logs');
  const transcript = path.join(evidenceDir, 'commands-and-responses.md');
  if (input.includeTaskOutputs || input.downloadLogs) {
    mkdirSync(respDir, { recursive: true });
    mkdirSync(reqDir, { recursive: true });
    mkdirSync(logsDir, { recursive: true });
    writeFileSync(transcript, ['# AIDP run tracking transcript', '', `created=${new Date().toISOString()}`, ''].join('\n'));
    summary.evidenceDir = evidenceDir;
    summary.transcript = transcript;
  }

  if (input.jobRunKey) {
    const jobRun = await runAidp(['workflow', 'get-job-run', workspaceKey, input.jobRunKey], { config, timeoutSeconds: input.timeoutSeconds || 120 });
    summary.jobRun = { command: jobRun.command, exitCode: jobRun.exitCode, data: safeData(jobRun) };
    if (jobRun.exitCode !== 0) return toolText(JSON.stringify(summary, null, 2), true);

    if (input.includeTaskRuns !== false) {
      const taskRuns = await runAidp(['workflow', 'list-task-runs', workspaceKey, '--job-run-key', input.jobRunKey, '--limit', '100', '--sort-by', 'timeCreated', '--sort-order', 'ASC'], { config, timeoutSeconds: input.timeoutSeconds || 120 });
      const taskRunData = safeData(taskRuns);
      summary.taskRuns = { command: taskRuns.command, exitCode: taskRuns.exitCode, data: taskRunData };
      if (taskRuns.exitCode !== 0) return toolText(JSON.stringify(summary, null, 2), true);

      if (input.includeTaskOutputs === true) {
        summary.taskOutputs = [];
        const items = taskRunData?.items || taskRunData?.taskRunCollection?.items || [];
        for (const task of items) {
          const detail = await runAidp(['workflow', 'get-task-run', workspaceKey, task.key], { config, timeoutSeconds: input.timeoutSeconds || 120 });
          const detailData = safeData(detail);
          if (detail.exitCode === 0 && detailData?.outputKey) {
            const output = await runAidp(['workflow', 'export-task-run-output', workspaceKey, task.key, detailData.outputKey, '--body', JSON.stringify({ format: 'IPYNB' })], { config, timeoutSeconds: input.timeoutSeconds || 120 });
            const outputPath = path.join(respDir, `task-output-${safeName(task.taskKey || task.key)}.txt`);
            writeFileSync(outputPath, `Response:\n${output.stdout}${output.stderr}`);
            appendTranscript(transcript, `Export task output (${task.taskKey || task.key})`, output.command, output, outputPath, evidenceDir);
            summary.taskOutputs.push({ taskKey: task.taskKey, taskRunKey: task.key, outputKey: detailData.outputKey, exitCode: output.exitCode, outputPath });
          }
        }
      }
    }
  }

  if (input.taskRunKey) {
    const taskRun = await runAidp(['workflow', 'get-task-run', workspaceKey, input.taskRunKey], { config, timeoutSeconds: input.timeoutSeconds || 120 });
    summary.taskRun = { command: taskRun.command, exitCode: taskRun.exitCode, data: safeData(taskRun) };
    if (taskRun.exitCode !== 0) return toolText(JSON.stringify(summary, null, 2), true);
  }

  if (input.sessionId) {
    const session = await runAidp(['notebook', 'get-session', workspaceKey, input.sessionId], { config, timeoutSeconds: input.timeoutSeconds || 120 });
    summary.notebookSession = { command: session.command, exitCode: session.exitCode, data: safeData(session) };
    if (session.exitCode !== 0) return toolText(JSON.stringify(summary, null, 2), true);
  }

  if (input.includeNotebookSessions || input.notebookPath || input.clusterId) {
    const args = ['notebook', 'list-sessions', workspaceKey];
    if (input.notebookPath) args.push('--path', input.notebookPath);
    if (input.clusterId) args.push('--cluster-id', input.clusterId);
    const sessions = await runAidp(args, { config, timeoutSeconds: input.timeoutSeconds || 120 });
    summary.notebookSessions = { command: sessions.command, exitCode: sessions.exitCode, data: safeData(sessions) };
    if (sessions.exitCode !== 0) return toolText(JSON.stringify(summary, null, 2), true);
  }

  if (input.downloadLogs === true) {
    const clusterKey = requireValue(config.clusterKey, 'clusterKey or AIDP_CLUSTER_KEY for log download');
    const jobRunKey = requireValue(input.jobRunKey, 'jobRunKey for log download');
    await collectLogsInternal({
      config,
      workspaceKey,
      clusterKey,
      jobRunKey,
      evidenceDir,
      reqDir,
      logsDir,
      transcript,
      logLookbackMinutes: input.logLookbackMinutes || 60,
      limit: 1000
    });
    summary.logs = { logsDir, transcript };
  }

  return toolText(JSON.stringify(summary, null, 2));
}

async function createMedallionArchitecture(input) {
  const config = workflowConfig(input.config || {});
  const layers = input.layers?.length ? input.layers : ['bronze', 'silver', 'gold'];
  const descriptions = input.descriptions || {};
  const bodies = layers.map((layer) => {
    const displayName = input.schemaPrefix ? `${input.schemaPrefix}_${layer}` : layer;
    return {
      catalogName: requireValue(input.catalogName, 'catalogName'),
      displayName,
      description: descriptions[layer] || `${layer} medallion schema`,
      properties: {
        ...(input.properties || {}),
        medallionLayer: layer
      }
    };
  });
  const commands = bodies.map(() => scrubCommand(['schema', 'create', '--body', '@<generated-json>', ...commonFlags(config)]));
  if (input.dryRun === true) {
    return toolText(JSON.stringify({ dryRun: true, schemaCount: bodies.length, schemas: bodies.map((body, index) => ({ command: commands[index], body })) }, null, 2));
  }

  const results = [];
  for (const body of bodies) {
    const result = await runGeneratedJsonCommand(['schema', 'create'], body, config, input.timeoutSeconds || 120);
    results.push({ displayName: body.displayName, command: result.command, exitCode: result.exitCode, response: safeData(result) });
    if (result.exitCode !== 0) return toolText(JSON.stringify({ created: false, results }, null, 2), true);
  }
  return toolText(JSON.stringify({ created: true, results }, null, 2));
}

async function createBundle(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const body = {
    path: requireValue(input.path, 'path'),
    name: requireValue(input.name, 'name'),
    bundledResources: (input.bundledResources || []).map((resource) => ({
      resourceKey: requireValue(resource.resourceKey, 'bundledResources[].resourceKey'),
      resourceType: requireValue(resource.resourceType, 'bundledResources[].resourceType')
    }))
  };
  if (!body.bundledResources.length) throw new Error('bundledResources must include at least one resource.');
  if (input.description) body.description = input.description;
  return await runJsonBodyCommand({
    label: 'Create bundle',
    args: ['bundle', 'create', workspaceKey],
    body,
    config,
    dryRun: input.dryRun === true,
    timeoutSeconds: input.timeoutSeconds || 120
  });
}

async function deployBundle(input) {
  const config = workflowConfig(input.config || {});
  const workspaceKey = requireValue(config.workspaceKey, 'workspaceKey or AIDP_WORKSPACE_KEY');
  const body = { path: requireValue(input.path, 'path') };
  if (input.dryRun === true) {
    return toolText(JSON.stringify({
      dryRun: true,
      deploy: {
        command: scrubCommand(['bundle', 'deploy', workspaceKey, '--body', '@<generated-json>', ...commonFlags(config)]),
        body
      },
      fetchStatus: input.fetchStatus !== false ? {
        command: scrubCommand(['bundle', 'fetch-deployment-status', workspaceKey, '--body', '@<generated-json>', ...commonFlags(config)]),
        body
      } : null
    }, null, 2));
  }

  const deploy = await runGeneratedJsonCommand(['bundle', 'deploy', workspaceKey], body, config, input.timeoutSeconds || 120);
  const response = { deploy: { command: deploy.command, exitCode: deploy.exitCode, response: safeData(deploy) } };
  if (deploy.exitCode !== 0) return toolText(JSON.stringify(response, null, 2), true);
  if (input.fetchStatus !== false) {
    const status = await runGeneratedJsonCommand(['bundle', 'fetch-deployment-status', workspaceKey], body, config, input.timeoutSeconds || 120);
    response.status = { command: status.command, exitCode: status.exitCode, response: safeData(status) };
    if (status.exitCode !== 0) return toolText(JSON.stringify(response, null, 2), true);
  }
  return toolText(JSON.stringify(response, null, 2));
}

async function collectLogsInternal({ config, workspaceKey, clusterKey, jobRunKey, evidenceDir, reqDir, logsDir, transcript, logLookbackMinutes, limit }) {
  const end = new Date();
  const begin = new Date(end.getTime() - logLookbackMinutes * 60 * 1000);
  const queries = [
    ['events', 'events', null],
    ['driver-stdout', 'driver', 'stdout'],
    ['driver-stderr', 'driver', 'stderr'],
    ['executor-stdout', 'executor', 'stdout'],
    ['executor-stderr', 'executor', 'stderr']
  ];

  for (const [label, contentType, streamType] of queries) {
    const body = {
      timeBegin: begin.toISOString(),
      timeEnd: end.toISOString(),
      logContentTypeContains: contentType,
      advancedFilter: `jobRunKey = ${jobRunKey}`
    };
    if (streamType) body.logStreamTypeContains = streamType;
    const bodyPath = path.join(reqDir, `search-logs-${label}.json`);
    writeJson(bodyPath, body);
    const result = await runAidp(['cluster', 'search-logs', workspaceKey, clusterKey, '--limit', String(limit), '--body', `@${bodyPath}`], { config, timeoutSeconds: 120 });
    const rawPath = path.join(logsDir, `cluster-logs-${label}.txt`);
    writeFileSync(rawPath, `Response:\n${result.stdout}${result.stderr}`);
    appendTranscript(transcript, `Search cluster logs (${label})`, result.command, result, rawPath, evidenceDir);
    if (result.exitCode !== 0) throw new Error(`AIDP log search failed: ${label}\n${resultToText(result, 12000)}`);
  }
}

function buildWorkflowNotebookSpecs(input, runId) {
  if (Array.isArray(input.notebooks) && input.notebooks.length) {
    return input.notebooks.map((notebook, index) => {
      const stage = index + 1;
      const taskKey = notebook.taskKey || `codex_notebook_${stage}`;
      const previousTaskKey = index === 0 ? null : (input.notebooks[index - 1].taskKey || `codex_notebook_${index}`);
      return {
        stage,
        name: ensureNotebookName(notebook.name || `${String(stage).padStart(2, '0')}_notebook.ipynb`),
        taskKey,
        dependsOn: notebook.dependsOn || (previousTaskKey ? [{ taskKey: previousTaskKey }] : []),
        source: notebook.source || ''
      };
    });
  }

  const count = input.notebookCount || 3;
  return Array.from({ length: count }, (_, index) => {
    const stage = index + 1;
    const taskKey = `codex_stage_${stage}`;
    return {
      stage,
      name: `${String(stage).padStart(2, '0')}_stage_${stage}.ipynb`,
      taskKey,
      dependsOn: index === 0 ? [] : [{ taskKey: `codex_stage_${stage - 1}` }],
      source: ''
    };
  });
}

function ensureNotebookName(name) {
  return name.endsWith('.ipynb') ? name : `${name}.ipynb`;
}

function generatedNotebookSource(stage, taskKey) {
  return [
    '# Setup: identify this workflow stage and task.',
    `stage = ${stage}`,
    `task_key = "${taskKey}"`,
    'print(f"AIDP Codex plugin workflow stage {stage} task {task_key} starting")',
    '',
    '# Work: keep related computation together in this cell.',
    'total = sum(range(stage + 4))',
    '',
    '# Validation: fail fast if the stage output is invalid.',
    'assert total >= stage',
    `print(f"AIDP_CODEX_STAGE_${stage}_SUCCESS total={total}")`
  ].join('\n');
}

function notebookContent(stage, source, taskKey = '') {
  const cells = source
    ? customNotebookCells(stage, source, taskKey || `codex_stage_${stage}`)
    : generatedNotebookCells(stage, taskKey || `codex_stage_${stage}`);
  return {
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {
      kernelspec: { name: 'python3', display_name: 'Python 3', language: 'python' },
      language_info: { name: 'python' }
    },
    cells
  };
}

function generatedNotebookCells(stage, taskKey) {
  return [
    notebookCodeCell([
      '# Setup: identify this workflow stage and task.',
      `stage = ${stage}`,
      `task_key = "${taskKey}"`,
      'print(f"AIDP Codex plugin workflow stage {stage} task {task_key} starting")'
    ].join('\n')),
    notebookCodeCell([
      '# Work: keep related computation together in this cell.',
      'total = sum(range(stage + 4))'
    ].join('\n')),
    notebookCodeCell([
      '# Validation: fail fast if the stage output is invalid.',
      'assert total >= stage',
      `print(f"AIDP_CODEX_STAGE_${stage}_SUCCESS total={total}")`
    ].join('\n'))
  ];
}

function customNotebookCells(stage, source, taskKey) {
  return [
    notebookCodeCell([
      '# Setup: common values for the provided notebook code.',
      `stage = ${stage}`,
      `task_key = "${taskKey}"`
    ].join('\n')),
    notebookCodeCell(commentedSourceCell(source)),
    notebookCodeCell([
      '# Completion marker: keep workflow evidence easy to search in logs.',
      `print("AIDP_CODEX_STAGE_${stage}_CUSTOM_SOURCE_COMPLETE")`
    ].join('\n'))
  ];
}

function commentedSourceCell(source) {
  const text = String(source || '').trimEnd();
  if (/^\s*%sql\b/i.test(text)) {
    return text.replace(/^\s*%sql\b/i, '%sql\n-- Work: provided SQL kept together in one cell.');
  }
  return [
    '# Work: provided notebook source kept together in one cell.',
    text
  ].join('\n');
}

function notebookCodeCell(source) {
  return {
    cell_type: 'code',
    execution_count: null,
    metadata: { trusted: true },
    outputs: [],
    source: [`${source}\n`]
  };
}

function writeJson(file, value) {
  writeFileSync(file, JSON.stringify(value, null, 2));
}

function appendTranscript(transcript, label, command, result, rawPath, evidenceDir) {
  const summary = responseSummary(result);
  const rel = path.relative(evidenceDir, rawPath);
  const text = [
    `## ${label}`,
    '',
    'Request:',
    '```sh',
    command,
    '```',
    '',
    `Response (exit ${result.exitCode}, raw: ${rel}):`,
    '```json',
    summary,
    '```',
    ''
  ].join('\n');
  writeFileSync(transcript, text, { flag: 'a' });
}

function responseSummary(result) {
  try {
    const root = responseRoot(result);
    const pick = {};
    for (const key of ['key', 'displayName', 'name', 'path', 'type', 'lifecycleState']) {
      if (root?.[key] !== undefined) pick[key] = root[key];
    }
    if (root?.state?.status) pick.status = root.state.status;
    if (root?.state?.stateMessage) pick.stateMessage = root.state.stateMessage;
    if (root?.items) pick.itemCount = root.items.length;
    if (root?.taskRunCollection?.items) pick.itemCount = root.taskRunCollection.items.length;
    if (root?.clusterLogCollection?.items) pick.itemCount = root.clusterLogCollection.items.length;
    if (root?.outputKey) pick.outputKey = root.outputKey;
    return Object.keys(pick).length ? JSON.stringify(pick, null, 2) : JSON.stringify(root, null, 2).slice(0, 1600);
  } catch {
    return truncate(`${result.stdout}${result.stderr}`.trim(), 1600);
  }
}

function isTerminal(status = '') {
  return ['SUCCESS', 'FAILED', 'CANCELED', 'CANCELLED', 'TIMED_OUT', 'TIMEDOUT', 'INTERNAL_ERROR', 'UPSTREAM_CANCELED', 'UPSTREAM_FAILED'].includes(String(status).toUpperCase());
}

function safeName(value) {
  return String(value).replace(/[^A-Za-z0-9_.-]/g, '_');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function handleToolCall(name, args = {}) {
  switch (name) {
    case 'aidp_cli':
      return await runWithOptionalBody(args);
    case 'aidp_check_connection':
      return await checkConnection(args);
    case 'aidp_notebook_workflow':
      return await notebookWorkflow(args);
    case 'aidp_three_notebook_workflow':
      return await threeNotebookWorkflow(args);
    case 'aidp_upload_workspace_code':
      return await uploadWorkspaceCode(args);
    case 'aidp_create_git_folder':
      return await createGitFolder(args);
    case 'aidp_git_commit_push':
      return await gitCommitPush(args);
    case 'aidp_git_pull':
      return await gitPull(args);
    case 'aidp_git_get_repository':
      return await gitGetRepository(args);
    case 'aidp_git_operation_state':
      return await gitOperationState(args);
    case 'aidp_git_list_branches':
      return await gitListBranches(args);
    case 'aidp_git_create_branch':
      return await gitCreateBranch(args);
    case 'aidp_git_checkout_branch':
      return await gitCheckoutBranch(args);
    case 'aidp_git_list_diffs':
      return await gitListDiffs(args);
    case 'aidp_git_diff_detail':
      return await gitDiffDetail(args);
    case 'aidp_git_merge':
      return await gitMerge(args);
    case 'aidp_git_rebase':
      return await gitRebase(args);
    case 'aidp_git_reset':
      return await gitReset(args);
    case 'aidp_create_agent':
      return await createAgent(args);
    case 'aidp_deploy_agent':
      return await deployAgent(args);
    case 'aidp_list_agents':
      return await listAgents(args);
    case 'aidp_get_agent_session_trace':
      return await getAgentSessionTrace(args);
    case 'aidp_create_ai_compute':
      return await createAiCompute(args);
    case 'aidp_list_ai_computes':
      return await listAiComputes(args);
    case 'aidp_update_ai_compute':
      return await updateAiCompute(args);
    case 'aidp_collect_logs':
      return await collectLogs(args);
    case 'aidp_track_runs':
      return await trackRuns(args);
    case 'aidp_create_schema':
      return await createSchema(args);
    case 'aidp_create_delta_table':
      return await createDeltaTable(args);
    case 'aidp_create_table_with_data':
      return await createTableWithData(args);
    case 'aidp_generate_csv_table_sql':
      return await generateCsvTableSql(args);
    case 'aidp_list_catalogs':
      return await listCatalogs(args);
    case 'aidp_get_catalog':
      return await getCatalog(args);
    case 'aidp_create_catalog':
      return await createCatalog(args);
    case 'aidp_create_external_catalog':
      return await createExternalCatalog(args);
    case 'aidp_auto_heal_workflow':
      return await autoHealWorkflow(args);
    case 'aidp_create_medallion_architecture':
      return await createMedallionArchitecture(args);
    case 'aidp_create_bundle':
      return await createBundle(args);
    case 'aidp_deploy_bundle':
      return await deployBundle(args);
    case 'aidp_command_help':
      return await commandHelp(args);
    case 'aidp_rest':
      return await runRest(args);
    case 'aidp_rest_api_reference':
      return await restApiReference(args);
    case 'aidp_cli_reference':
      return await cliReference(args);
    default:
      return toolText(`Unknown tool: ${name}`, true);
  }
}

async function handleMessage(message) {
  if (message.method === 'initialize') {
    jsonResponse(message.id, {
      protocolVersion: message.params?.protocolVersion || '2024-11-05',
      capabilities: { tools: {} },
      serverInfo: { name: SERVER_NAME, version: SERVER_VERSION }
    });
    return;
  }

  if (message.method === 'tools/list') {
    jsonResponse(message.id, { tools: TOOLS });
    return;
  }

  if (message.method === 'tools/call') {
    const result = await handleToolCall(message.params?.name, message.params?.arguments || {});
    jsonResponse(message.id, result);
    return;
  }

  if (message.method === 'ping') {
    jsonResponse(message.id, {});
    return;
  }

  if (message.id !== undefined) {
    jsonError(message.id, -32601, `Method not found: ${message.method}`);
  }
}

let buffer = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
  buffer += chunk;
  let index;
  while ((index = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, index).trim();
    buffer = buffer.slice(index + 1);
    if (!line) continue;
    let message;
    try {
      message = JSON.parse(line);
    } catch (error) {
      jsonError(null, -32700, `Parse error: ${error.message}`);
      continue;
    }
    Promise.resolve(handleMessage(message)).catch((error) => {
      if (message.id !== undefined) jsonError(message.id, -32000, error.message, { stack: error.stack });
    });
  }
});

process.stdin.on('end', () => process.exit(0));

if (process.argv.includes('--self-test')) {
  const fingerprint = createHash('sha256').update(readFileSync(__filename)).digest('hex').slice(0, 12);
  process.stdout.write(JSON.stringify({ name: SERVER_NAME, version: SERVER_VERSION, tools: TOOLS.length, fingerprint, id: randomUUID() }, null, 2));
  process.exit(0);
}
