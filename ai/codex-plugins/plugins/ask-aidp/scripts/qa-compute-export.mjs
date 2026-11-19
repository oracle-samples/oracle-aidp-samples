import { deepEqual, equal, rejects, throws } from 'node:assert/strict';
import { requestBodyReference } from './cli-body-reference.mjs';
import { runRest } from '../mcp/ask-aidp-server.mjs';

export const exportModel = 'ExportComputeConfigurationDetails';
export const exportFields = ['clusterScopedLibraries', 'environmentVariables', 'destinationPath', 'fileName'];
const decode = (result) => JSON.parse(result.content.map((item) => item.text || '').join('\n'));

export function requestBodyReferenceTest() {
  const nested = '**Request Body (`ComputeConfigurationLibraryEntry`):**\n- `pip` (string, optional) — Package\n';
  const root = `**Request Body (\`${exportModel}\`):**\n` + exportFields.map((name, index) =>
    `- \`${name}\` (string, optional) —${index ? ' A hyphen-separated description.' : ''}\n`).join('');
  const operation = { bodyModel: exportModel, bodyFields: exportFields.map((name) => ({ name })) };
  for (const section of [nested + root, root + nested, nested + '**Example:**\n{}\n' + root]) {
    const result = requestBodyReference(section, operation, 'export');
    equal(result.bodyModel, exportModel);
    deepEqual(result.bodyFields.map((field) => field.name), exportFields);
    equal(result.bodyFields[0].description, '');
    equal(result.bodyFields[1].description, 'A hyphen-separated description.');
  }
  throws(() => requestBodyReference(nested + root, undefined, 'export'), /ambiguous request models/);
  throws(() => requestBodyReference(root, { bodyModel: '' }, 'export'), /no root in the CLI manifest/);
  throws(() => requestBodyReference(nested, operation, 'export'), /root model .* missing/);
  throws(() => requestBodyReference(root, { ...operation, bodyFields: [{ name: 'unknown' }] }, 'export'), /root fields differ/);
  equal(requestBodyReference(root, undefined, 'export').bodyModel, exportModel);
  deepEqual(requestBodyReference('', undefined, 'get'), { bodyModel: '', bodyFields: [] });
  deepEqual(requestBodyReference('', { bodyModel: 'RestartClusterDetails', bodyFields: [] }, 'restart'), {
    bodyModel: 'RestartClusterDetails', bodyFields: []
  });
  const mlflow = requestBodyReference('**Request Body (`CreateExperimentDetails`):**\n- `artifact_location` (string, optional) —\n', {
    bodyModel: 'CreateExperimentDetails', bodyFields: [{ name: 'artifactLocation' }]
  }, 'create-experiment');
  equal(mlflow.bodyFields[0].name, 'artifact_location');
}

export async function computeExportTransportTest() {
  const input = {
    method: 'POST',
    path: '/20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/clusters/{clusterKey}/actions/exportComputeConfiguration',
    config: { endpoint: 'https://aidp.example.com', instanceId: 'instance-id', workspaceKey: 'workspace-key', clusterKey: 'cluster-key' },
    body: {
      clusterScopedLibraries: [{ pip: 'example-package==1.0.0' }],
      environmentVariables: { EXAMPLE: 'test' },
      destinationPath: '/Workspace/configurations/', fileName: 'qa-export.yaml'
    }
  };
  const yaml = 'clusterScopedLibraries:\n  - pip: example-package==1.0.0\nenvironmentVariables:\n  EXAMPLE: test\n';
  let composed;
  let sent;
  // Match OCI's append semantics: separate default/caller Accept headers combine.
  const factory = async () => ({
    authProvider: {},
    common: {
      composeRequest: async (options) => {
        composed = options;
        const headers = new Headers();
        for (const entries of [options.defaultHeaders, options.headerParams]) {
          for (const [key, value] of Object.entries(entries)) headers.append(key, value);
        }
        return { headers, body: options.bodyContent };
      },
      DefaultRequestSigner: class {},
      FetchHttpClient: class {
        async send(request) {
          sent = request;
          return { status: 200, ok: true, headers: new Headers({
            'content-type': 'application/x-yaml', path: '/Workspace/configurations/qa-export.yaml'
          }), text: async () => yaml };
        }
      }
    }
  });
  const noAuth = async () => { throw new Error('Dry runs must not authenticate'); };
  for (const headers of [undefined, { Accept: 'application/x-yaml' }, { aCcEpT: 'application/octet-stream', 'Content-Type': 'application/json' }]) {
    const request = { ...input, headers };
    const plan = decode(await runRest({ ...request, dryRun: true }, noAuth));
    const response = decode(await runRest(request, factory));
    deepEqual(composed.defaultHeaders, {});
    deepEqual(Object.fromEntries(sent.headers), plan.headers);
    equal(sent.headers.get('accept'), headers?.aCcEpT || 'application/x-yaml');
    equal(sent.headers.get('content-type'), 'application/json');
    deepEqual(JSON.parse(sent.body), input.body);
    deepEqual(plan.body, input.body);
    equal(response.status, 200);
    equal(response.body, yaml);
    equal(response.headers['content-type'], 'application/x-yaml');
    equal(response.headers.path, '/Workspace/configurations/qa-export.yaml');
  }
  await rejects(runRest({ ...input, headers: { Accept: 'application/x-yaml', accept: 'application/json' }, dryRun: true }, noAuth), /Duplicate REST header/);
  await rejects(runRest({ ...input, headers: { Authorization: 'example' }, dryRun: true }, noAuth), /Do not set signed REST header/);
  const get = { ...input, method: 'GET', path: input.path.replace('exportComputeConfiguration', 'getComputeConfiguration'), body: undefined };
  const plan = decode(await runRest({ ...get, dryRun: true }, noAuth));
  await runRest(get, factory);
  deepEqual(plan.headers, { accept: 'application/json' });
  deepEqual(Object.fromEntries(sent.headers), plan.headers);
  equal(sent.body, undefined);
}
