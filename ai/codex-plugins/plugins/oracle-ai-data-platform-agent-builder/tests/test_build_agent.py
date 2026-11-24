"""Offline packager tests: source is inspected, never imported or executed."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location('build_agent', Path(__file__).resolve().parents[1] / 'scripts' / 'build_agent.py')
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


class BuilderTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.output = self.root / 'built'
        definition = {
            'protocol': 'aidp.a2app', 'protocolVersion': 1,
            'catalogId': 'https://aidp.local/catalogs/a2app-v2',
            'components': [{'id': 'overview', 'label': 'Overview',
                'contract': 'contracts/overview.md',
                'actions': {'display': {'label': 'Display', 'handler': 'display', 'effect': 'read',
                    'inputSchema': {'type': 'object', 'properties': {}, 'additionalProperties': False}}}}],
        }
        self.files = {
            'agent/main.py': b'raise RuntimeError("Generated code must not execute during packaging")\n',
            'agent/helpers/overview.py': b'def display():\n    return {"items": []}\n',
            'agent/requirements.txt': b'langgraph\n',
            'agent/component-definitions.json': json.dumps(definition).encode(),
            'contracts/overview.md': b'# Overview\n\nDisplay the selected business overview.\n',
        }
        self.manifest = {'schemaVersion': 1, 'id': 'example-agent', 'title': 'Example agent',
            'entrypoint': 'agent/main.py', 'requirements': 'agent/requirements.txt',
            'definition': 'agent/component-definitions.json',
            'contracts': ['contracts/overview.md'], 'files': list(self.files)}
        for name, data in self.files.items():
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        self.save_manifest()

    def save_manifest(self):
        (self.source / 'app.json').write_text(json.dumps(self.manifest))

    def test_validate_does_not_execute_or_scan_unlisted_files(self):
        (self.source / '.env').write_text('DO_NOT_PACKAGE=secret')
        (self.source / 'agent' / 'credentials.json').write_text('{"password":"private"}')
        manifest, files, revision = builder.validate(self.source)
        self.assertEqual(manifest['id'], 'example-agent')
        self.assertEqual(set(files), set(self.files))
        self.assertRegex(revision, r'^[0-9a-f]{16}$')
        builder.package(self.source, self.output)
        with zipfile.ZipFile(self.output / 'agent-source.zip') as archive:
            self.assertFalse(any('.env' in p or 'credentials' in p for p in archive.namelist()))

    def test_rejects_unsafe_paths_and_symlinks(self):
        for name in ['agent/../private.json', '/agent/private.json', 'agent/.secrets.json',
                     'agent/credentials.json', 'agent/helpers\\private.py', 'agent/versions/old.py']:
            with self.subTest(path=name), self.assertRaises(ValueError):
                builder.relative(name)
        linked = self.source / 'agent/helpers/overview.py'
        linked.unlink()
        linked.symlink_to(self.source / 'agent/main.py')
        with self.assertRaisesRegex(ValueError, 'Symbolic'):
            builder.validate(self.source)

    def test_rejects_invalid_code_missing_contract_and_oversized_input(self):
        helper = self.source / 'agent/helpers/overview.py'
        helper.write_text('def broken(\n')
        with self.assertRaises(SyntaxError):
            builder.validate(self.source)
        helper.write_bytes(self.files['agent/helpers/overview.py'])
        self.manifest['files'].remove('contracts/overview.md')
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, 'explicitly listed'):
            builder.validate(self.source)
        self.manifest['files'].append('contracts/overview.md')
        self.save_manifest()
        with patch.object(builder, 'MAX_SOURCE', 10), self.assertRaisesRegex(ValueError, 'input size'):
            builder.validate(self.source)

    def test_rejects_unsupported_component_definition(self):
        path = self.source / 'agent/component-definitions.json'
        value = json.loads(path.read_text())
        value['components'][0]['actions']['display']['inputSchema']['additionalProperties'] = True
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'additional properties'):
            builder.validate(self.source)

    def test_flattened_current_code_and_matching_deterministic_tool_zip(self):
        first = builder.package(self.source, self.output)
        first_bundle = (self.output / 'agent-source.zip').read_bytes()
        workspace = self.output / 'workspace'
        self.assertEqual((workspace / 'main.py').read_bytes(), self.files['agent/main.py'])
        self.assertEqual((workspace / 'helpers/overview.py').read_bytes(), self.files['agent/helpers/overview.py'])
        self.assertTrue((workspace / 'contracts/overview.md').is_file())
        self.assertFalse((workspace / 'agent').exists())
        with zipfile.ZipFile(workspace / 'components.zip') as archive:
            self.assertEqual(set(archive.namelist()), {'helpers/overview.py', 'requirements.txt',
                'component-definitions.json', 'contracts/overview.md', 'config.json'})
            self.assertEqual(archive.read('helpers/overview.py'), self.files['agent/helpers/overview.py'])
            self.assertEqual(archive.read('config.json'), (workspace / 'config.json').read_bytes())
        settings = json.loads((workspace / 'config.json').read_text())
        self.assertEqual(settings['applicationRevision'], first['runtimeRevision'])
        self.assertEqual(settings['sourceRevision'], first['sourceRevision'])
        current = json.loads((workspace / 'tool-package.json').read_text())
        saved = json.loads((workspace / 'versions/build-001/tool-package.json').read_text())
        self.assertEqual(current['toolConfig']['packagePath'], 'components.zip')
        self.assertEqual(saved['toolConfig']['packagePath'], 'versions/build-001/components.zip')
        again = builder.package(self.source, self.output)
        self.assertEqual(again['buildName'], 'build-001')
        self.assertEqual(first_bundle, (self.output / 'agent-source.zip').read_bytes())

    def test_new_revision_retains_readable_snapshot_without_nested_history(self):
        first = builder.package(self.source, self.output)
        previous = (self.output / 'workspace/versions/build-001/contracts/overview.md').read_bytes()
        (self.source / 'contracts/overview.md').write_text('# Revised overview\n\nShow the latest period.\n')
        second = builder.package(self.source, self.output)
        self.assertEqual(second['buildName'], 'build-002')
        self.assertNotEqual(first['runtimeRevision'], second['runtimeRevision'])
        self.assertEqual((self.output / 'workspace/versions/build-001/contracts/overview.md').read_bytes(), previous)
        with zipfile.ZipFile(self.output / 'agent-source.zip') as archive:
            self.assertIn('versions/build-002/main.py', archive.namelist())
            self.assertFalse(any(p.count('versions/') > 1 for p in archive.namelist()))

    def test_output_workspace_refuses_unmanaged_files_and_symlinks(self):
        builder.package(self.source, self.output)
        private = self.output / 'workspace/notes.txt'
        private.write_text('not an explicit package input')
        with self.assertRaisesRegex(ValueError, 'unexpected files'):
            builder.package(self.source, self.output)
        private.unlink()
        private.symlink_to(self.source / 'contracts/overview.md')
        with self.assertRaisesRegex(ValueError, 'symbolic'):
            builder.package(self.source, self.output)

    def test_failed_size_check_leaves_saved_build_unchanged(self):
        builder.package(self.source, self.output)
        state = (self.output / 'builds.json').read_bytes()
        current = (self.output / 'workspace/contracts/overview.md').read_bytes()
        (self.source / 'contracts/overview.md').write_text('# Revised contract\n')
        with patch.object(builder, 'MAX_PACKAGE', 1), self.assertRaises(ValueError):
            builder.package(self.source, self.output)
        self.assertEqual((self.output / 'builds.json').read_bytes(), state)
        self.assertEqual((self.output / 'workspace/contracts/overview.md').read_bytes(), current)
        self.assertFalse((self.output / 'workspace/versions/build-002').exists())

    def test_plan_uses_public_tool_payloads_without_claiming_deployment(self):
        config = {'aiDataPlatformId': 'ocid1.aidataplatform.example', 'workspaceKey': 'workspace-key',
            'agentComputeKey': 'compute-key', 'workspacePath': '/Workspace/Agents/example-agent',
            'modelId': 'example-model'}
        path = self.root / 'publish.json'
        path.write_text(json.dumps(config))
        result = builder.plan(self.source, path, self.output)
        self.assertEqual(result['status'], 'plan-only')
        self.assertEqual(result['unresolvedInputs'], ['created-agent-key'])
        operations = result['operations']
        self.assertEqual([op['tool'] for op in operations],
            ['aidp_upload_workspace_code', 'aidp_create_agent', 'aidp_deploy_agent'])
        self.assertTrue(all(op['arguments']['dryRun'] for op in operations))
        self.assertEqual(operations[1]['arguments']['agentType'], 'CODE')
        self.assertEqual(operations[1]['arguments']['computeKey'], 'compute-key')
        self.assertEqual(operations[1]['arguments']['entryFilePath'], 'main.py')
        self.assertEqual(operations[0]['arguments']['config']['instanceId'], config['aiDataPlatformId'])
        self.assertEqual(operations[2]['resolve']['agentKey'], 'create-agent.key')
        self.assertEqual(operations[2]['arguments']['agentKey'], '<created-agent-key>')
        for name in ['create-agent.json', 'deploy-agent.json', 'upload-request.json', 'plan.json']:
            self.assertTrue((self.output / name).is_file())
        tool = json.loads((self.output / 'workspace/tool-package.json').read_text())
        self.assertEqual(tool['toolConfig']['packagePath'], config['workspacePath'] + '/components.zip')
        config['password'] = 'not-allowed'
        path.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, 'no credentials'):
            builder.plan(self.source, path, self.output)


if __name__ == '__main__':
    unittest.main()
