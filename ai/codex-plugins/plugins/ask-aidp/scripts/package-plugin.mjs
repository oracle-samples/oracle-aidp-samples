#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const WORKSPACE_ROOT = path.resolve(PLUGIN_ROOT, '..', '..');
const DIST_DIR = path.join(PLUGIN_ROOT, 'dist');
const STAGING_ROOT = path.join(WORKSPACE_ROOT, '.plugin-build');
const STAGING_PLUGIN = path.join(STAGING_ROOT, 'ask-aidp');
const version = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, '.codex-plugin', 'plugin.json'), 'utf8')).version;
const baseName = `ask-aidp-codex-plugin-${version}`;

function run(command, args, cwd = WORKSPACE_ROOT) {
  const result = spawnSync(command, args, { cwd, stdio: 'inherit' });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed with ${result.status}`);
}

rmSync(STAGING_ROOT, { recursive: true, force: true });
mkdirSync(STAGING_ROOT, { recursive: true });
mkdirSync(DIST_DIR, { recursive: true });
cpSync(PLUGIN_ROOT, STAGING_PLUGIN, {
  recursive: true,
  filter: (src) => {
    const rel = path.relative(PLUGIN_ROOT, src);
    return !rel.startsWith('qa-live-result.json')
      && !rel.startsWith('qa-runs')
      && !rel.startsWith('vendor')
      && !rel.startsWith('dist');
  }
});

const vendorSource = process.env.AIDP_VENDOR_NODE_MODULES || path.join(WORKSPACE_ROOT, 'samples', 'npm-cli', 'node_modules');
if (existsSync(vendorSource)) {
  const vendorTarget = path.join(STAGING_PLUGIN, 'vendor', 'node_modules');
  mkdirSync(path.dirname(vendorTarget), { recursive: true });
  cpSync(vendorSource, vendorTarget, { recursive: true });
}

const tarPath = path.join(DIST_DIR, `${baseName}.tar.gz`);
const zipPath = path.join(DIST_DIR, `${baseName}.zip`);
const sampleEnvPath = path.join(DIST_DIR, 'aidp.env.sample');
rmSync(tarPath, { force: true });
rmSync(zipPath, { force: true });
run('tar', ['-czf', tarPath, '-C', STAGING_ROOT, 'ask-aidp']);
if (spawnSync('zip', ['--version'], { stdio: 'ignore' }).status === 0) {
  run('zip', ['-qr', zipPath, 'ask-aidp'], STAGING_ROOT);
}
cpSync(path.join(PLUGIN_ROOT, 'examples', 'aidp.env.sample'), sampleEnvPath);

const files = [tarPath, zipPath, sampleEnvPath].filter(existsSync);
const checksums = files.map((file) => {
  const hash = createHash('sha256').update(readFileSync(file)).digest('hex');
  return `${hash}  ${path.basename(file)}`;
}).join('\n') + '\n';
writeFileSync(path.join(DIST_DIR, `${baseName}.sha256`), checksums);

console.log(JSON.stringify({
  ok: true,
  distDir: DIST_DIR,
  files: files.map((file) => path.relative(WORKSPACE_ROOT, file)),
  checksumFile: path.relative(WORKSPACE_ROOT, path.join(DIST_DIR, `${baseName}.sha256`)),
  vendoredAidpCli: existsSync(vendorSource)
}, null, 2));
