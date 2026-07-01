#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { get } from 'node:https';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const source = 'https://github.com/oracle-samples/aidataplatform-sdk/blob/main/docs/cli/README.md';
const rawSource = 'https://raw.githubusercontent.com/oracle-samples/aidataplatform-sdk/main/docs/cli/README.md';
const input = process.argv[2] || '';

async function readMarkdown() {
  if (input) return readFileSync(input, 'utf8');
  return await fetchText(rawSource);
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    get(url, (response) => {
      if (response.statusCode && response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
        response.resume();
        fetchText(new URL(response.headers.location, url).toString()).then(resolve, reject);
        return;
      }
      if (response.statusCode !== 200) {
        response.resume();
        reject(new Error(`Failed to fetch ${url}: HTTP ${response.statusCode}`));
        return;
      }
      response.setEncoding('utf8');
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => resolve(body));
    }).on('error', reject);
  });
}

const markdown = await readMarkdown();

const groups = [];
let current = null;
for (const line of markdown.split(/\r?\n/)) {
  const groupMatch = line.match(/^## <a id="([^"]+)"><\/a>(.+)$/);
  if (groupMatch) {
    current = { id: groupMatch[1], title: groupMatch[2].trim(), description: '', commands: [] };
    groups.push(current);
    continue;
  }

  if (
    current &&
    !current.description &&
    line.trim() &&
    !line.startsWith('**') &&
    !line.startsWith('- ') &&
    !line.startsWith('###') &&
    !line.startsWith('####') &&
    !line.startsWith('<a ')
  ) {
    current.description = line.trim();
  }

  const commandMatch = line.match(/^- \[([^\]]+)\]\(#([^)]+)\)/);
  if (commandMatch && current) {
    current.commands.push({ displayName: commandMatch[1], anchor: commandMatch[2] });
  }
}

const commands = [];
const sectionPattern = /#### `([^`]+)`\s+([\s\S]*?)(?=#### `|^## (?!Global Options|Utility Commands|Command Index)|(?![\s\S]))/gm;
let sectionMatch;
while ((sectionMatch = sectionPattern.exec(markdown))) {
  const fullName = sectionMatch[1].trim();
  const section = sectionMatch[2].trim();
  const parts = fullName.split(/\s+/);
  const group = parts[1] || '';
  const command = parts.slice(2).join(' ');
  const anchor = section.match(/<a id="([^"]+)"><\/a>/)?.[1] || `${group}-${command}`.replace(/\s+/g, '-');
  const usage = section.match(/\*\*Usage:\*\*\s*`([^`]+)`/)?.[1] || '';
  const bodyModel = section.match(/\*\*Request Body \(`([^`]+)`\):\*\*/)?.[1] || '';
  const bodyFields = [];
  const bodyStart = section.indexOf('**Request Body');

  if (bodyStart >= 0) {
    const bodyPart = section.slice(bodyStart).split('**Example:**')[0].split('---')[0];
    const fieldPattern = /- `([^`]+)` \(([^)]+)\) —\s*([^-]*)/g;
    let fieldMatch;
    while ((fieldMatch = fieldPattern.exec(bodyPart))) {
      bodyFields.push({
        name: fieldMatch[1],
        type: fieldMatch[2],
        description: fieldMatch[3].trim()
      });
    }
  }

  const summarySection = section
    .split('**Usage:**')[0]
    .replace(/<a id="[^"]+"><\/a>/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  commands.push({
    fullName,
    group,
    command,
    anchor,
    summary: summarySection.slice(0, 600).trim(),
    usage,
    bodyModel,
    bodyFields,
    markdown: `#### \`${fullName}\`\n${section}`
  });
}

const byFullName = new Map(commands.map((command) => [command.fullName, command]));
for (const group of groups) {
  for (const command of group.commands) {
    const fullName = `aidp ${command.displayName}`;
    const detail = byFullName.get(fullName);
    if (detail) {
      Object.assign(command, {
        fullName,
        command: detail.command,
        summary: detail.summary,
        usage: detail.usage,
        bodyModel: detail.bodyModel
      });
    } else {
      command.fullName = fullName;
    }
  }
}

const reference = {
  generatedAt: new Date().toISOString(),
  source,
  groupCount: groups.length,
  commandCount: commands.length,
  groups,
  commands
};

mkdirSync(path.join(PLUGIN_ROOT, 'assets'), { recursive: true });
writeFileSync(
  path.join(PLUGIN_ROOT, 'assets', 'aidp-cli-command-reference.json'),
  JSON.stringify(reference, null, 2)
);

const catalog = [
  '# AIDP CLI Command Catalog',
  '',
  `Generated from ${source}.`,
  '',
  `This plugin supports all ${reference.commandCount} documented AIDP CLI commands through \`aidp_cli\`, with lookup support through \`aidp_cli_reference\`.`,
  ''
];

for (const group of groups) {
  catalog.push(`## ${group.title} (${group.id})`);
  if (group.description) catalog.push(group.description);
  catalog.push('');
  for (const command of group.commands) {
    catalog.push(`- \`${command.fullName}\`${command.summary ? ` - ${command.summary}` : ''}`);
  }
  catalog.push('');
}

writeFileSync(path.join(PLUGIN_ROOT, 'skills', 'ask-aidp', 'CLI_COMMANDS.md'), catalog.join('\n'));

console.log(JSON.stringify({
  ok: true,
  source,
  groups: reference.groupCount,
  commands: reference.commandCount,
  json: path.relative(process.cwd(), path.join(PLUGIN_ROOT, 'assets', 'aidp-cli-command-reference.json')),
  markdown: path.relative(process.cwd(), path.join(PLUGIN_ROOT, 'skills', 'ask-aidp', 'CLI_COMMANDS.md'))
}, null, 2));
