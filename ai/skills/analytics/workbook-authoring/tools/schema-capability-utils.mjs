import fs from 'node:fs';
import path from 'node:path';

const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const version = (value) => typeof value === 'string' && /^\d+(?:\.\d+)*$/.test(value);

export function normalizeSchemaCapabilities(raw) {
   if (!object(raw) || raw.contractVersion !== 1 || !Array.isArray(raw.components) || !raw.components.length) {
      throw new Error('INVALID_SERVER_INFO: Expected schemaCapabilities contractVersion=1 and a nonempty components array.');
   }
   const patterns = new Set();
   const components = raw.components.map((component) => {
      if (!object(component) || typeof component.schemaIdPattern !== 'string' ||
         !component.schemaIdPattern.startsWith('http://oracle.com/bi/') || !component.schemaIdPattern.includes('{VERSION}') ||
         patterns.has(component.schemaIdPattern) || typeof component.versionField !== 'string' ||
         !/^[A-Za-z_][A-Za-z0-9_]*$/.test(component.versionField) ||
         !Array.isArray(component.supportedVersions) || !component.supportedVersions.length ||
         !component.supportedVersions.every(version) || new Set(component.supportedVersions).size !== component.supportedVersions.length) {
         throw new Error('INVALID_SERVER_INFO: Malformed or duplicate component schema capability.');
      }
      patterns.add(component.schemaIdPattern);
      return { schemaIdPattern: component.schemaIdPattern, versionField: component.versionField,
         supportedVersions: [...component.supportedVersions] };
   });
   return { contractVersion: 1, components };
}

function decodeServerInfoResponse(input) {
   if (!object(input)) return input;
   if (own(input, 'isError') && input.isError !== false) {
      throw new Error('INVALID_SERVER_INFO: Server-info tool response must not report an error.');
   }
   if (own(input, 'structuredContent')) {
      throw new Error('INVALID_SERVER_INFO: Supply the decoded oracleAnalytics payload or a single-text MCP result, not structuredContent or mixed representations.');
   }
   if (!own(input, 'content')) return input;
   if (own(input, 'oracleAnalytics') || !Array.isArray(input.content) || input.content.length !== 1
      || input.content[0]?.type !== 'text' || typeof input.content[0].text !== 'string') {
      throw new Error('INVALID_SERVER_INFO: Expected exactly one MCP text block containing the complete oracleAnalytics JSON payload.');
   }
   let payload;
   try { payload = JSON.parse(input.content[0].text); } catch {
      throw new Error('INVALID_SERVER_INFO: MCP server-info text must contain valid JSON, without prose or code fences.');
   }
   if (!object(payload) || ['content', 'structuredContent', 'isError'].some((key) => own(payload, key))) {
      throw new Error('INVALID_SERVER_INFO: MCP server-info text must contain the decoded oracleAnalytics payload, not another envelope.');
   }
   return payload;
}

/** Accept the complete payload or its single-text MCP envelope; never infer missing capabilities. */
export function resolveServerInfo({ serverInfo, serverInfoFile, projectVersions = [] } = {}) {
   const inputs = [serverInfo, serverInfoFile ? JSON.parse(fs.readFileSync(serverInfoFile, 'utf8')) : undefined]
      .filter((entry) => entry !== undefined && entry !== null);
   const versions = projectVersions.filter((entry) => entry !== undefined && entry !== null && entry !== '');
   let capabilities;
   for (const input of inputs) {
      const workbook = decodeServerInfoResponse(input)?.oracleAnalytics?.workbook;
      if (!object(workbook)) throw new Error('INVALID_SERVER_INFO: Expected oracleAnalytics.workbook.');
      versions.push(workbook.latestProjectVersion);
      if (own(workbook, 'schemaCapabilities')) {
         const next = normalizeSchemaCapabilities(workbook.schemaCapabilities);
         const canonical = (value) => JSON.stringify(value.components.map((c) => ({ ...c,
            supportedVersions: [...c.supportedVersions].sort() })).sort((a, b) => a.schemaIdPattern.localeCompare(b.schemaIdPattern)));
         if (capabilities && canonical(capabilities) !== canonical(next)) {
            throw new Error('CONFLICTING_SERVER_INFO: Capability inputs disagree.');
         }
         capabilities = next;
      }
   }
   if (versions.some((entry) => !/^[1-9]\d*$/.test(String(entry)) || !Number.isSafeInteger(Number(entry)))) {
      throw new Error('INVALID_SERVER_INFO: Server project version must be a positive integer.');
   }
   if (new Set(versions.map(Number)).size > 1) throw new Error('CONFLICTING_SERVER_INFO: Project version inputs disagree.');
   return { latestProjectVersion: versions.length ? Number(versions[0]) : null, schemaCapabilities: capabilities };
}

/** Connected CLI executions must distinguish unavailable discovery from a lost handoff. */
export function resolveServerInfoHandoff({ connected = false, unavailableReasons = [], ...inputs } = {}) {
   // Parse first: an unavailable declaration must never hide malformed/conflicting input.
   const server = resolveServerInfo(inputs);
   const reasons = unavailableReasons.filter((reason) => reason !== undefined && reason !== null);
   if (reasons.some((reason) => typeof reason !== 'string' || !reason.trim())) {
      throw new Error('INVALID_SERVER_INFO: serverInfoUnavailableReason must be a nonempty reason for unavailable discovery.');
   }
   const distinctReasons = [...new Set(reasons.map((reason) => reason.trim()))];
   if (distinctReasons.length > 1 || (distinctReasons.length && server.latestProjectVersion !== null)) {
      throw new Error('CONFLICTING_SERVER_INFO: Do not combine unavailable discovery with server info/version or conflicting reasons.');
   }
   const unavailableReason = distinctReasons[0] || null;
   if (connected && server.latestProjectVersion === null && !unavailableReason) {
      throw new Error('MISSING_SERVER_INFO_HANDOFF: Connected authoring/validation requires the complete get_server_info response in request serverInfo or --server-info-file. If discovery is genuinely unavailable, supply serverInfoUnavailableReason or --server-info-unavailable-reason. Do not change compatibility target or omit connection flags to bypass this check.');
   }
   const mode = server.schemaCapabilities ? 'server_capabilities' : server.latestProjectVersion !== null
      ? 'project_version_only' : unavailableReason ? 'explicit_fallback' : 'offline';
   return { server, unavailableReason, summary: { mode,
      serverComponentValidation: Boolean(server.schemaCapabilities),
      fallbackReason: unavailableReason,
      serverInfoFile: inputs.serverInfoFile ? path.resolve(inputs.serverInfoFile) : null } };
}

export function readSchemaRegistry(profileDir) {
   return JSON.parse(fs.readFileSync(path.join(profileDir, 'model/validation/component-schema-registry.json'), 'utf8'));
}

/** Follow the product registry's logical paths (array indexes do not form part of those paths). */
export function collectVersionedNodes(workbook, registries, { requireVersions = false } = {}) {
   const nodes = [];
   const applied = new Set();
   function visitRegistry(type, value, prefix) {
      const registry = registries[type];
      if (!registry) throw new Error(`MISSING_COMPONENT_REGISTRY: ${type}`);
      const definitions = new Map(registry.schemaDefinitions.map((d) => [d.schemaDefName, d]));
      function apply(name, node, pointer) {
         const key = `${type}:${name}:${pointer}`;
         if (applied.has(key)) return;
         applied.add(key);
         const definition = definitions.get(name);
         if (!definition) throw new Error(`MISSING_COMPONENT_REGISTRY: ${type}/${name}`);
         if (definition.relativeSchemaType) visitRegistry(definition.relativeSchemaType, node, pointer);
         else if (definition.versionField && object(node) && (requireVersions || own(node, definition.versionField))) {
            const declaredVersion = String(node[definition.versionField]);
            const mapped = new Map(definition.versionMapping || []).get(declaredVersion) || declaredVersion;
            nodes.push({ path: pointer || '/', value: node, schemaIdPattern: definition.schema$IdPattern,
               versionField: definition.versionField, declaredVersion,
               schemaId: definition.schema$IdPattern.replace('{VERSION}', mapped),
               locallySupportedVersions: [...(definition.versions || []), ...(definition.versionMapping || []).map(([v]) => v)],
               locallySupported: (definition.versions || []).map(String).includes(mapped) });
         }
      }
      function walk(node, logical, pointer, parent, property) {
         if (Array.isArray(node)) {
            node.forEach((child, index) => walk(child, logical, `${pointer}/${index}`, parent, property));
         } else if (object(node)) {
            for (const mapping of registry.parsedSchemaDefinitions || []) {
               if (mapping.jsonPath === logical) {
                  if (mapping.noConditionSchemaDefName) apply(mapping.noConditionSchemaDefName, node, pointer);
                  for (const conditional of mapping.propertyConditions || []) {
                     if (conditional.conditions.every((c) => own(node, c.name) && String(node[c.name]) === String(c.value))) {
                        apply(conditional.schemaDefName, node, pointer);
                     }
                  }
               }
               if (mapping.jsonPath === parent) {
                  for (const child of mapping.childPropertyRegExStrings || []) {
                     if (new RegExp(`^(?:${child.regExpString})$`).test(property)) apply(child.schemaDefName, node, pointer);
                  }
               }
            }
            for (const [name, child] of Object.entries(node)) {
               walk(child, `${logical === '/' ? '' : logical}/${name}`,
                  `${pointer}/${name.replace(/~/g, '~0').replace(/\//g, '~1')}`, logical, name);
            }
         }
      }
      walk(value, '/', prefix, '/', '');
   }
   visitRegistry('workbook', workbook, '');
   return nodes;
}

export function componentCompatibilityIssues(nodes, capabilities, profileID) {
   const supported = new Map((capabilities?.components || []).map((component) => [component.schemaIdPattern, component]));
   const issues = [];
   for (const node of nodes) {
      const component = supported.get(node.schemaIdPattern);
      if (!node.locallySupported || (capabilities && (!component || component.versionField !== node.versionField ||
         !component.supportedVersions.includes(node.declaredVersion)))) {
         issues.push({ path: node.path, message: `UNSUPPORTED_COMPONENT_SCHEMA: profile=${profileID}, component=${node.schemaIdPattern}, ` +
            `emitted=${node.declaredVersion}, supported=${capabilities ? JSON.stringify(component?.supportedVersions || []) : 'not in local registry'}.` });
         if (!node.locallySupported) issues[issues.length - 1].message += ` Local profile accepts ${JSON.stringify(node.locallySupportedVersions)}.`;
      }
   }
   return issues;
}

export function profileRequirementsSupported(profile, capabilities) {
   const filename = path.join(profile.bundleRootDir, 'model/schema-requirements.json');
   if (!fs.existsSync(filename)) throw new Error(`MISSING_PROFILE_SCHEMA_CONTRACT: ${profile.id}; rebuild the installed skill.`);
   const required = normalizeSchemaCapabilities(JSON.parse(fs.readFileSync(filename, 'utf8')));
   const supported = new Map(capabilities.components.map((c) => [c.schemaIdPattern, c]));
   return required.components.every((r) => {
      const actual = supported.get(r.schemaIdPattern);
      return actual?.versionField === r.versionField && r.supportedVersions.every((v) => actual.supportedVersions.includes(v));
   });
}

/** Canonical repairs must not erase or lower a declaration on a retained component. */
export function assertNoVersionDowngrade(before, after, registries, profileID) {
   const original = collectVersionedNodes(before, registries);
   const updated = new Map(collectVersionedNodes(after, registries, { requireVersions: true })
      .map((node) => [`${node.path}:${node.schemaIdPattern}`, node]));
   for (const node of original) {
      const next = updated.get(`${node.path}:${node.schemaIdPattern}`);
      if (next && (!version(next.declaredVersion) || next.declaredVersion.localeCompare(node.declaredVersion, undefined, { numeric: true }) < 0)) {
         throw new Error(`SCHEMA_VERSION_DOWNGRADE: path=${node.path}, component=${node.schemaIdPattern}, ` +
            `original=${node.declaredVersion}, emitted=${next.declaredVersion}, profile=${profileID}; preserve the existing declaration.`);
      }
   }
}
