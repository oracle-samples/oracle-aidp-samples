#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const source = 'https://github.com/oracle-samples/aidataplatform-sdk/blob/main/docs/cli/README.md';
const input = process.argv[2] || '/private/tmp/aidp-cli-readme.md';
const markdown = readFileSync(input, 'utf8');

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
const sectionPattern = /^#### `([^`]+)`\n([\s\S]*?)(?=^#### `|^## <a id=|\z)/gm;
let sectionMatch;
while ((sectionMatch = sectionPattern.exec(markdown))) {
  const fullName = sectionMatch[1].trim();
  const section = sectionMatch[2].trim();
  const parts = fullName.split(/\s+/);
  const group = parts[1] || '';
  const command = parts.slice(2).join(' ');
  const anchor = section.match(/<a id="([^"]+)"><\/a>/)?.[1] || `${group}-${command}`.replace(/\s+/g, '-');
  const usage = section.match(/\*\*Usage:\*\*\n\n`([^`]+)`/)?.[1] || '';
  const bodyModel = section.match(/\*\*Request Body \(`([^`]+)`\):\*\*/)?.[1] || '';
  const bodyFields = [];
  const bodyStart = section.indexOf('**Request Body');

  if (bodyStart >= 0) {
    const bodyPart = section.slice(bodyStart).split('**Example:**')[0].split('---')[0];
    for (const line of bodyPart.split(/\r?\n/)) {
      const fieldMatch = line.match(/^- `([^`]+)` \(([^)]+)\) —(.*)$/);
      if (fieldMatch) {
        bodyFields.push({
          name: fieldMatch[1],
          type: fieldMatch[2],
          description: fieldMatch[3].trim()
        });
      }
    }
  }

  const summaryLines = [];
  for (const line of section.split(/\r?\n/)) {
    const text = line.trim();
    if (!text || text.startsWith('<a ') || text.startsWith('**Usage:**')) continue;
    if (
      text.startsWith('`aidp ') ||
      text.startsWith('**Path Arguments:**') ||
      text.startsWith('**Options:**') ||
      text.startsWith('**Request Body')
    ) {
      break;
    }
    if (text.startsWith('---')) break;
    summaryLines.push(text);
    if (summaryLines.join(' ').length > 500) break;
  }

  commands.push({
    fullName,
    group,
    command,
    anchor,
    summary: summaryLines.join(' ').replace(/\s+/g, ' ').trim(),
    usage,
    bodyModel,
    bodyFields,
    markdown: `#### ${fullName}\n${section}`
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
