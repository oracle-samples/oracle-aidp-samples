#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { get } from 'node:https';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const source = 'https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/rest-endpoints.html';
const whatsNewUrl = 'https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/whatsnew.html';
const rawSource = source;
const input = process.argv[2] || '';

async function readHtml() {
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

function decodeHtml(value) {
  return value
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function stripHtml(value) {
  return decodeHtml(value.replace(/<[^>]+>/g, ' '));
}

const html = await readHtml();
const categories = [];
const panelPattern = /<div class="panel panel-default">\s*<div class="panel-heading">[\s\S]*?<div>\s*([^<]+?)\s*<\/div>[\s\S]*?<div class="panel-body">\s*<div>([\s\S]*?)<\/div>\s*<dl class="list-group schema-properties">([\s\S]*?)<\/dl>/g;
let panel;
while ((panel = panelPattern.exec(html))) {
  const name = stripHtml(panel[1]);
  const description = stripHtml(panel[2]);
  const operations = [];
  const operationPattern = /<dt><a href="([^"]+)">([\s\S]*?)<\/a><\/dt><dd><div>Method:\s*<span[^>]*>([^<]+)<\/span><\/div><div>Path:\s*<code>([^<]+)<\/code><\/div><\/dd>/g;
  let operation;
  while ((operation = operationPattern.exec(panel[3]))) {
    operations.push({
      name: stripHtml(operation[2]),
      method: operation[3].trim().toUpperCase(),
      path: decodeHtml(operation[4]),
      referenceUrl: new URL(operation[1], source).toString()
    });
  }
  if (name && operations.length) {
    categories.push({
      id: name.replace(/([a-z])([A-Z])/g, '$1-$2').replace(/\s+/g, '-').toLowerCase(),
      name,
      description,
      operationCount: operations.length,
      operations
    });
  }
}

const apiVersion = categories
  .flatMap((category) => category.operations)
  .map((operation) => operation.path.match(/^\/(\d+)\//)?.[1])
  .find(Boolean);

const reference = {
  generatedAt: new Date().toISOString(),
  source,
  whatsNewUrl,
  apiVersion,
  endpointTemplate: `/${apiVersion}/aiDataPlatforms/{aiDataPlatformId}/...`,
  categoryCount: categories.length,
  operationCount: categories.reduce((total, category) => total + category.operations.length, 0),
  categories
};

if (!reference.apiVersion || !reference.operationCount) {
  throw new Error('Could not parse the AIDP REST endpoint catalog.');
}

mkdirSync(path.join(PLUGIN_ROOT, 'assets'), { recursive: true });
writeFileSync(
  path.join(PLUGIN_ROOT, 'assets', 'aidp-rest-api-reference.json'),
  JSON.stringify(reference, null, 2)
);

console.log(JSON.stringify({
  ok: true,
  source,
  categories: reference.categoryCount,
  operations: reference.operationCount,
  apiVersion: reference.apiVersion,
  json: path.relative(process.cwd(), path.join(PLUGIN_ROOT, 'assets', 'aidp-rest-api-reference.json'))
}, null, 2));
