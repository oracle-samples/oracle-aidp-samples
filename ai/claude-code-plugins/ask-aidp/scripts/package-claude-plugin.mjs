#!/usr/bin/env node
// Package the Ask AIDP Claude Code plugin into release archives.
//
// Produces under <plugin-root>/dist:
//   ask-aidp-claude-plugin-<version>.tar.gz
//   ask-aidp-claude-plugin-<version>.zip            (when `zip` is available)
//   ask-aidp-claude-plugin-<version>.sha256
//   aidp.env.sample
//   ASK_AIDP_CLAUDE_INSTALL.md                       (when present)
//
// The archives extract to a self-contained plugin and marketplace root:
//   ask-aidp-claude/
//   ├── .claude-plugin/plugin.json
//   ├── .claude-plugin/marketplace.json
//   └── ...
// so `claude plugin marketplace add <extracted-dir>` works straight from the archive.
//
// The staged tree includes the vendored node_modules. Broken symlinks are
// dropped and any absolute / escaping symlink target fails the build so no
// machine-specific path leaks into the archive.
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cpSync, existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, readlinkSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const PLUGIN_ROOT = path.resolve(path.dirname(__filename), '..');
const DIST_DIR = path.join(PLUGIN_ROOT, 'dist');
const STAGING_ROOT = path.join(tmpdir(), 'ask-aidp-claude-plugin-build');
const ARCHIVE_DIR_NAME = 'ask-aidp-claude';
const STAGING_PLUGIN = path.join(STAGING_ROOT, ARCHIVE_DIR_NAME);
const version = JSON.parse(readFileSync(path.join(PLUGIN_ROOT, '.claude-plugin', 'plugin.json'), 'utf8')).version;
const baseName = `ask-aidp-claude-plugin-${version}`;

const EXCLUDES = new Set(['aidp.env', 'qa-live-result.json', 'qa-runs', '.DS_Store', '.plugin-build', 'dist', 'vendor']);

function run(command, args, cwd = PLUGIN_ROOT) {
  const result = spawnSync(command, args, { cwd, stdio: 'inherit' });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed with ${result.status}`);
}

// Walk the staged tree: drop broken symlinks, fail on absolute / escaping ones.
function auditSymlinks(dir) {
  const dropped = [];
  const walk = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isSymbolicLink()) {
        const target = readlinkSync(full);
        if (path.isAbsolute(target)) {
          throw new Error(`Absolute symlink target not portable: ${path.relative(STAGING_PLUGIN, full)} -> ${target}`);
        }
        const resolved = path.resolve(path.dirname(full), target);
        if (!resolved.startsWith(STAGING_PLUGIN + path.sep)) {
          throw new Error(`Symlink escapes plugin tree: ${path.relative(STAGING_PLUGIN, full)} -> ${target}`);
        }
        if (!existsSync(resolved)) {
          unlinkSync(full);
          dropped.push(path.relative(STAGING_PLUGIN, full));
        }
      } else if (entry.isDirectory()) {
        walk(full);
      }
    }
  };
  walk(dir);
  return dropped;
}

rmSync(STAGING_ROOT, { recursive: true, force: true });
mkdirSync(STAGING_ROOT, { recursive: true });
mkdirSync(DIST_DIR, { recursive: true });

const REQUIRED_VENDOR_DEPS = ['aidp-cli', 'aidp-typescript-client', 'oci-common'];
const pluginVendorSource = path.join(PLUGIN_ROOT, 'vendor', 'node_modules');
const vendorSource = process.env.AIDP_VENDOR_NODE_MODULES || pluginVendorSource;
if (!existsSync(vendorSource)) {
  throw new Error(
    `Vendor node_modules not found. Set AIDP_VENDOR_NODE_MODULES, or provide ${pluginVendorSource}. `
    + `The packaged plugin must bundle ${REQUIRED_VENDOR_DEPS.join(', ')}.`
  );
}
const missingVendorDeps = REQUIRED_VENDOR_DEPS.filter((dep) => !existsSync(path.join(vendorSource, dep)));
if (missingVendorDeps.length) {
  throw new Error(`Vendor source ${vendorSource} is missing required dependencies: ${missingVendorDeps.join(', ')}.`);
}

cpSync(PLUGIN_ROOT, STAGING_PLUGIN, {
  recursive: true,
  verbatimSymlinks: true,
  filter: (src) => {
    const rel = path.relative(PLUGIN_ROOT, src);
    if (rel === '') return true;
    const top = rel.split(path.sep)[0];
    return !EXCLUDES.has(top) && !rel.endsWith('/.DS_Store') && path.basename(rel) !== '.DS_Store';
  }
});

const vendorTarget = path.join(STAGING_PLUGIN, 'vendor', 'node_modules');
mkdirSync(path.dirname(vendorTarget), { recursive: true });
cpSync(vendorSource, vendorTarget, { recursive: true, verbatimSymlinks: true });

const droppedSymlinks = auditSymlinks(STAGING_PLUGIN);

// The staged marketplace must resolve its source back to this self-contained plugin.
const stagedMarketplaceManifest = path.join(STAGING_PLUGIN, '.claude-plugin', 'marketplace.json');
if (!existsSync(stagedMarketplaceManifest)) {
  throw new Error(`Missing ${path.join(PLUGIN_ROOT, '.claude-plugin', 'marketplace.json')}.`);
}
const stagedEntry = JSON.parse(readFileSync(stagedMarketplaceManifest, 'utf8'))
  .plugins?.find((plugin) => plugin.name === 'ask-aidp');
if (!stagedEntry) throw new Error('Staged marketplace.json has no ask-aidp plugin entry.');
const stagedPluginPath = path.resolve(STAGING_PLUGIN, stagedEntry.source);
if (!existsSync(path.join(stagedPluginPath, '.claude-plugin', 'plugin.json'))) {
  throw new Error(`Staged marketplace entry source ${stagedEntry.source} does not resolve to the staged plugin.`);
}

const tarPath = path.join(DIST_DIR, `${baseName}.tar.gz`);
const zipPath = path.join(DIST_DIR, `${baseName}.zip`);
const sampleEnvPath = path.join(DIST_DIR, 'aidp.env.sample');
const installGuideDist = path.join(DIST_DIR, 'ASK_AIDP_CLAUDE_INSTALL.md');
rmSync(tarPath, { force: true });
rmSync(zipPath, { force: true });

run('tar', ['-czf', tarPath, '-C', STAGING_ROOT, ARCHIVE_DIR_NAME]);
if (spawnSync('zip', ['--version'], { stdio: 'ignore' }).status === 0) {
  run('zip', ['-qr', zipPath, ARCHIVE_DIR_NAME], STAGING_ROOT);
}
cpSync(path.join(PLUGIN_ROOT, 'examples', 'aidp.env.sample'), sampleEnvPath);
cpSync(path.join(PLUGIN_ROOT, 'README.md'), installGuideDist);

const files = [tarPath, zipPath, sampleEnvPath, installGuideDist].filter(existsSync);
const checksums = files.map((file) => {
  const hash = createHash('sha256').update(readFileSync(file)).digest('hex');
  return `${hash}  ${path.basename(file)}`;
}).join('\n') + '\n';
writeFileSync(path.join(DIST_DIR, `${baseName}.sha256`), checksums);

console.log(JSON.stringify({
  ok: true,
  version,
  distDir: DIST_DIR,
  files: files.map((file) => path.relative(PLUGIN_ROOT, file)),
  checksumFile: path.relative(PLUGIN_ROOT, path.join(DIST_DIR, `${baseName}.sha256`)),
  vendoredAidpCli: true,
  droppedBrokenSymlinks: droppedSymlinks
}, null, 2));
