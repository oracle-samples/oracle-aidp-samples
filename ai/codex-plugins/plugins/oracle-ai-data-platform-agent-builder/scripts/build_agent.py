#!/usr/bin/env python3
"""Offline A2App source validation, deterministic packaging and public publish plans."""
import argparse
import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import sys
import zipfile

PLUGIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN / 'runtime'))
from a2app_protocol import validate_definition

MAX_FILE = 2 * 1024 * 1024
MAX_SOURCE = 8 * 1024 * 1024
MAX_PACKAGE = 10 * 1024 * 1024
ID = re.compile(r'[a-z][a-z0-9-]{0,63}')
BUILD = re.compile(r'build-[0-9]{3,}')
PRIVATE = {'credentials.json', 'secrets.json', 'oci-config.json', 'config.local.json', 'settings.local.json'}


def relative(name):
    if not isinstance(name, str) or not name or '\\' in name:
        raise ValueError('Use explicit POSIX relative source paths')
    path = PurePosixPath(name)
    if path.is_absolute() or str(path) != name or any(p.startswith('.') for p in path.parts):
        raise ValueError('Source paths cannot be absolute, hidden or traverse parents')
    if len(path.parts) < 2 or path.parts[0] not in {'agent', 'contracts'}:
        raise ValueError('Source files must be under agent/ or contracts/')
    allowed = {'.py', '.json', '.txt'} if path.parts[0] == 'agent' else {'.md'}
    if path.suffix not in allowed or path.name.lower() in PRIVATE:
        raise ValueError('Unsupported or private source file: ' + name)
    if path.parts[0] == 'agent' and (path.parts[1] == 'versions' or path.name == 'build-info.json'):
        raise ValueError('Build history and metadata are publisher-owned')
    return path


def read_file(root, name):
    path = root / name
    if any(root.joinpath(*Path(name).parts[:i]).is_symlink() for i in range(1, len(Path(name).parts) + 1)):
        raise ValueError('Symbolic links are not permitted in source paths')
    if not path.is_file() or not path.resolve().is_relative_to(root.resolve()) or path.stat().st_size > MAX_FILE:
        raise ValueError('Missing, unsafe or oversized source file: ' + name)
    data = path.read_bytes()
    data.decode('utf-8')
    if b'-----BEGIN ' in data and b'PRIVATE KEY-----' in data:
        raise ValueError('Private key material cannot be packaged')
    return data


def validate(source):
    root = Path(source).expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise ValueError('SOURCE must be a real directory')
    root = root.resolve()
    raw = read_file(root, 'app.json')
    manifest = json.loads(raw)
    fields = {'schemaVersion', 'id', 'title', 'entrypoint', 'requirements', 'definition', 'contracts', 'files'}
    if not isinstance(manifest, dict) or set(manifest) != fields or type(manifest['schemaVersion']) is not int or manifest['schemaVersion'] != 1:
        raise ValueError('app.json must use the documented schemaVersion 1 fields')
    if not isinstance(manifest['id'], str) or not ID.fullmatch(manifest['id']) or not isinstance(manifest['title'], str) or not 1 <= len(manifest['title']) <= 120:
        raise ValueError('Invalid source id or title')
    names = manifest['files']
    if not isinstance(names, list) or not 1 <= len(names) <= 200 or any(not isinstance(n, str) for n in names) or len(set(names)) != len(names):
        raise ValueError('files must be a bounded, unique explicit source allowlist')
    for name in names:
        relative(name)
    contracts = manifest['contracts']
    if not isinstance(contracts, list) or not contracts or any(not isinstance(n, str) or not re.fullmatch(r'contracts/[a-z0-9-]+\.md', n) for n in contracts):
        raise ValueError('List English contracts under contracts/<stable-id>.md')
    required = [manifest[k] for k in ('entrypoint', 'requirements', 'definition')] + contracts
    if any(not isinstance(n, str) or n not in names for n in required):
        raise ValueError('Every entrypoint, dependency, definition and contract file must be explicitly listed')
    for key, suffix in [('entrypoint', '.py'), ('requirements', '.txt'), ('definition', '.json')]:
        path = relative(manifest[key])
        if path.parts[0] != 'agent' or path.suffix != suffix:
            raise ValueError('Invalid ' + key + ' source path')
    files = {name: read_file(root, name) for name in names}
    if sum(map(len, files.values())) + len(raw) > MAX_SOURCE:
        raise ValueError('Source exceeds the bounded package input size')
    for name, data in files.items():
        if name.endswith('.py'):
            ast.parse(data, filename=name)
        elif name.endswith('.json'):
            json.loads(data)
    definition = validate_definition(json.loads(files[manifest['definition']]))
    if any(c['contract'] not in contracts for c in definition['components']):
        raise ValueError('Component contracts must be declared and packaged')
    for name in contracts:
        if not files[name].strip():
            raise ValueError('English contracts cannot be empty')
    hashed = {'app.json': raw.decode(), **{n: b.decode() for n, b in files.items()}}
    revision = hashlib.sha256(json.dumps(hashed, sort_keys=True, separators=(',', ':')).encode()).hexdigest()[:16]
    return manifest, files, revision


def zipped(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(files.items()):
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
    return buffer.getvalue()


def write(path, data):
    path = Path(path)
    if path.is_symlink() or any(p.is_symlink() for p in path.parents):
        raise ValueError('Output paths cannot be symbolic links')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(data)


def encoded(value):
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False).encode()


def package(source, output, config=None):
    manifest, files, revision = validate(source)
    config = config or {}
    runtime = hashlib.sha256((revision + str(config.get('modelId', ''))).encode()).hexdigest()[:16]
    output = Path(output).expanduser().absolute()
    state_path = output / 'builds.json'
    if output.is_symlink() or state_path.is_symlink():
        raise ValueError('Package output/state cannot be symbolic links')
    output = output.resolve()
    state_path = output / 'builds.json'
    if state_path.exists() and state_path.stat().st_size > MAX_FILE:
        raise ValueError('Saved build state exceeds the size limit')
    builds = json.loads(state_path.read_text()) if state_path.exists() else {}
    if (not isinstance(builds, dict) or any(not isinstance(n, str) or not BUILD.fullmatch(n) or int(n[6:]) < 1 for n in builds.values())
            or len(set(builds.values())) != len(builds)):
        raise ValueError('Invalid saved build names')
    if runtime not in builds:
        builds[runtime] = f'build-{max((int(n[6:]) for n in builds.values()), default=0) + 1:03d}'
    build = builds[runtime]
    archive = 'versions/' + build
    flat = {n.removeprefix('agent/'): data for n, data in files.items()}
    settings = json.loads(flat.get('config.json', b'{}'))
    if not isinstance(settings, dict):
        raise ValueError('agent/config.json must be an object')
    settings.update(applicationId=manifest['id'], sourceRevision=revision, applicationRevision=runtime)
    if config.get('modelId'):
        settings['modelId'] = config['modelId']
    flat['config.json'] = encoded(settings)
    tools = {name: data for name, data in flat.items()
        if name not in {manifest['entrypoint'].removeprefix('agent/'), 'tool-package.json'}}
    tool_zip = zipped(tools)
    tool_config = json.loads(flat.get('tool-package.json', b'{"toolType":"CUSTOM","toolConfig":{"tools":[]}}'))
    if not isinstance(tool_config, dict) or not isinstance(tool_config.get('toolConfig'), dict):
        raise ValueError('tool-package.json requires toolConfig')
    home = config.get('workspacePath', '').rstrip('/')
    tool_config['toolConfig']['packagePath'] = (home + '/' if home else '') + 'components.zip'
    flat['tool-package.json'] = encoded(tool_config)
    metadata = {'buildName': build, 'sourceRevision': revision, 'runtimeRevision': runtime,
        'toolPackageSha256': hashlib.sha256(tool_zip).hexdigest(), 'archivePath': archive,
        'contracts': manifest['contracts'],
        'files': sorted(set(flat) | {'components.zip', 'build-info.json', 'README.md'})}
    flat.update({'components.zip': tool_zip, 'build-info.json': encoded(metadata),
        'README.md': ('# Current generated agent\n\nCurrent source is at this level, with English contracts in contracts/.\n'
            'AIDP snapshots this source at deployment; Python imports provide the CODE tool implementations.\n'
            'components.zip is a matching build artifact, not a per-call code download.\n'
            'Saved copies are under versions/. Current saved build: ' + build + '.\n').encode()})
    snapshot = dict(flat)
    saved_tools = json.loads(encoded(tool_config))
    saved_tools['toolConfig']['packagePath'] = (home + '/' if home else '') + archive + '/components.zip'
    snapshot['tool-package.json'] = encoded(saved_tools)
    snapshot['README.md'] = ('# Saved ' + build + '\n\nSee build-info.json for exact source/runtime revision and tool ZIP hash.\n').encode()
    workspace = output / 'workspace'
    # Only publisher-owned files may be reused; an output folder is not a source scan.
    if workspace.is_symlink() or any(p.is_symlink() for p in workspace.rglob('*')):
        raise ValueError('Output workspace cannot contain symbolic links')
    known = set()
    for folder, prefix in [(workspace, ''), *[(workspace / 'versions' / name, 'versions/' + name + '/') for name in builds.values()]]:
        info = folder / 'build-info.json'
        if info.exists():
            if info.is_symlink() or info.stat().st_size > MAX_FILE:
                raise ValueError('Unsafe saved build metadata')
            saved = json.loads(info.read_text())
            if not isinstance(saved, dict) or not isinstance(saved.get('files'), list):
                raise ValueError('Invalid saved build metadata')
            for name in saved['files']:
                if not isinstance(name, str) or not name or '\\' in name:
                    raise ValueError('Unsafe saved build file list')
                path = PurePosixPath(name)
                if (path.is_absolute() or any(p.startswith('.') for p in path.parts) or str(path) != name
                        or path.parts[0] == 'versions' or path.name.lower() in PRIVATE
                        or path.suffix not in {'.py', '.json', '.txt', '.md', '.zip'}):
                    raise ValueError('Unsafe saved build file list')
                known.add(prefix + name)
    for path in workspace.rglob('*'):
        if path.is_symlink() or path.is_file() and path.relative_to(workspace).as_posix() not in known:
            raise ValueError('Output workspace contains unexpected files; choose a fresh output directory')
    current = set(flat)
    previous = {n for n in known if not n.startswith('versions/')}
    bundle = {}
    for name in sorted(n for n in known if n.startswith('versions/')):
        path = workspace / name
        if not path.is_file() or path.stat().st_size > (MAX_PACKAGE if path.suffix == '.zip' else MAX_FILE):
            raise ValueError('Missing or oversized saved build file')
        bundle[name] = path.read_bytes()
    bundle.update(flat)
    bundle.update({archive + '/' + name: data for name, data in snapshot.items()})
    source_zip = zipped(bundle)
    if len(source_zip) > MAX_PACKAGE:
        raise ValueError('Retained build history exceeds the 10 MiB deployment ZIP limit')
    for name, data in snapshot.items():
        write(workspace / archive / name, data)
    for name, data in flat.items():
        write(workspace / name, data)
    for name in previous - current:
        (workspace / name).unlink(missing_ok=True)
    write(state_path, encoded(builds))
    write(output / 'agent-source.zip', source_zip)
    return metadata | {'workspace': str(workspace), 'id': manifest['id'], 'title': manifest['title'],
        'uploadFiles': sorted(bundle),
        'entryFilePath': manifest['entrypoint'].removeprefix('agent/'),
        'dependenciesFilePath': manifest['requirements'].removeprefix('agent/')}


def plan(source, config_path, output):
    config = json.loads(Path(config_path).read_text())
    required = {'aiDataPlatformId', 'workspaceKey', 'agentComputeKey', 'workspacePath'}
    if not isinstance(config, dict) or not required <= config.keys() or set(config) - required - {'modelId'}:
        raise ValueError('Publish config requires instance/workspace/compute/path and optional modelId only; no credentials')
    if any(not isinstance(v, str) or not v.strip() for v in config.values()):
        raise ValueError('Publish configuration values must be nonempty strings')
    home = PurePosixPath(config['workspacePath'])
    if not str(home).startswith('/Workspace/') or str(home) != config['workspacePath'] or '..' in home.parts:
        raise ValueError('workspacePath must be an absolute /Workspace/ agent folder')
    result = package(source, output, config)
    output = Path(output).expanduser().resolve()
    common = {'instanceId': config['aiDataPlatformId'], 'workspaceKey': config['workspaceKey']}
    create = {'displayName': result['title'], 'type': 'CODE', 'pathInfo': str(home),
        'entryFilePath': result['entryFilePath'], 'dependenciesFilePath': result['dependenciesFilePath'], 'computeKey': config['agentComputeKey']}
    deploy = {'agentKey': '<created-agent-key>', 'agentComputeKey': config['agentComputeKey']}
    upload = {'localPath': result['workspace'], 'workspacePath': str(home), 'recursive': True, 'overwrite': True,
        'includeGlobs': result['uploadFiles'], 'maxFiles': len(result['uploadFiles']), 'config': common, 'dryRun': True}
    operations = [{'id': 'upload', 'tool': 'aidp_upload_workspace_code', 'arguments': upload},
        {'id': 'create-agent', 'dependsOn': ['upload'], 'tool': 'aidp_create_agent',
         'arguments': {k: v for k, v in create.items() if k != 'type'} | {'agentType': 'CODE', 'config': common, 'dryRun': True}},
        {'id': 'deploy-agent', 'dependsOn': ['create-agent'], 'resolve': {'agentKey': 'create-agent.key'},
         'tool': 'aidp_deploy_agent', 'arguments': deploy | {'config': common, 'dryRun': True}}]
    for filename, value in [('create-agent.json', create), ('deploy-agent.json', deploy), ('upload-request.json', upload)]:
        write(output / filename, encoded(value))
    prepared = {'status': 'plan-only', 'experimentalProfile': 'native-a2app', 'result': result,
        'unresolvedInputs': ['created-agent-key'], 'operations': operations,
        'instructions': 'No requests sent. Inspect dry runs, verify native profile support, then use public AIDP tools with authorized live arguments. Resolve agentKey from the create response.'}
    write(output / 'plan.json', encoded(prepared))
    return prepared


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('validate', 'package', 'plan'):
        item = commands.add_parser(name)
        item.add_argument('source', type=Path)
        if name != 'validate':
            item.add_argument('--output', type=Path, required=True)
        if name == 'plan':
            item.add_argument('--config', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == 'validate':
        manifest, files, revision = validate(args.source)
        result = {'status': 'valid', 'id': manifest['id'], 'sourceRevision': revision, 'files': len(files)}
    elif args.command == 'package':
        result = package(args.source, args.output)
    else:
        result = plan(args.source, args.config, args.output)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, SyntaxError) as error:
        raise SystemExit(str(error)) from None
