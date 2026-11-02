# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Offline checks for customer-ready, independent notebook artifacts."""
import ast
import base64
import json
import re
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import connectivity_checks as shared


class NotebookChecks(unittest.TestCase):
    def items(self):
        for item in json.loads((ROOT / 'case-index.json').read_text()):
            yield item, json.loads((ROOT / item['notebook']).read_text())

    def environment(self, nb):
        env = {}
        helpers = next(c for c in nb['cells'] if c['cell_type'] == 'code' and
                       ''.join(c['source']).startswith('CASE_NAMES = '))
        exec(''.join(helpers['source']), env)
        config = next(c for c in nb['cells'] if c['cell_type'] == 'code' and
                      ''.join(c['source']).startswith('CASE_ID = '))
        exec(''.join(config['source']), env)
        return env

    def test_seven_self_contained_notebooks_with_empty_outputs(self):
        items = list(self.items())
        self.assertEqual({i['case'] for i, _ in items}, set(shared.CASE_NAMES))
        self.assertEqual(len(items), 7)
        self.assertEqual({p.name for p in ROOT.glob('V*.ipynb')},
                         {item['notebook'] for item, _ in items})
        for item, nb in items:
            with self.subTest(case=item['case']):
                for cell in nb['cells']:
                    if cell['cell_type'] == 'code':
                        ast.parse(''.join(cell['source']))
                        self.assertEqual(cell['outputs'], [])
                        self.assertIsNone(cell['execution_count'])
                env = self.environment(nb)
                self.assertEqual(list(env['CASE_NAMES']), [item['case']])
                self.assertEqual(env['CASE_ID'], item['case'])
                self.assertNotIn('SELECTED_CASES', env)
                self.assertFalse(env['RUN_LIVE'])
                self.assertFalse(env['RUN_EVIDENCE_CHECKS'])
                for field in ('workspace_label', 'compute_label', 'hostname'):
                    self.assertEqual(env['CONFIG'][field], '')
                for field in ('expected_instance', 'wallet_zip', 'catalog_id', 'user', 'tns', 'region', 'workspace_region', 'endpoint', 'uri'):
                    if field in env['CONFIG']:
                        self.assertEqual(env['CONFIG'][field], '')

    def test_embedded_network_diagrams_and_setup_in_every_notebook(self):
        for item, nb in self.items():
            markdown = '\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type'] == 'markdown')
            self.assertIn('## Network setup', markdown)
            self.assertIn('### Example network topology', markdown)
            self.assertIn('attachment:network.png', markdown)
            cell = next(c for c in nb['cells'] if 'network.png' in c.get('attachments', {}))
            image = base64.b64decode(cell['attachments']['network.png']['image/png'], validate=True)
            self.assertTrue(image.startswith(b'\x89PNG\r\n\x1a\n'))
            self.assertEqual(image, (ROOT / 'assets' / (item['case'].lower() + '-network.png')).read_bytes())

    def test_no_environment_specific_or_internal_material(self):
        files = [ROOT / 'README.md', ROOT / 'connectivity_checks.py', ROOT / 'build_notebooks.py']
        files += [ROOT / item['notebook'] for item, _ in self.items()]
        forbidden = [r'ocid1\.', r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',
                     r'\.oraclecorp\.', r'confluence/', r'Oracle Internal', r'/Users/']
        for path in files:
            content = path.read_text()
            for pattern in forbidden:
                self.assertIsNone(re.search(pattern, content, re.I), (path.name, pattern))

    def test_specialized_checks_preserve_shared_behavior(self):
        for item, nb in self.items():
            env = self.environment(nb)
            case = item['case']
            if 'run_case' in env:
                self.assertEqual(env['run_case'](case, env['CONFIG'])['checks'],
                                 shared.run_case(case, env['CONFIG'])['checks'])
            if 'evaluate_evidence' in env:
                self.assertEqual(env['evaluate_evidence'](case, {})['checks'],
                                 shared.evaluate_evidence(case, {})['checks'])

    def test_guide_catalog_and_local_links(self):
        guide = (ROOT / 'README.md').read_text()
        catalog = (ROOT.parents[1] / 'README.md').read_text()
        for item, _ in self.items():
            self.assertIn('](' + item['notebook'] + ')', guide)
            self.assertIn('private-workspace-connectivity/' + item['notebook'], catalog)
        for text, parent in [(guide, ROOT)] + [('\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type'] == 'markdown'), ROOT) for _, nb in self.items()]:
            for target in re.findall(r'\]\(([^)]+)\)', text):
                if target.startswith(('https:', 'attachment:', '#')):
                    continue
                self.assertTrue((parent / target.split('#')[0]).exists(), target)

    def test_regeneration_is_deterministic(self):
        files = [ROOT / item['notebook'] for item, _ in self.items()] + [ROOT / 'case-index.json']
        before = {p: p.read_bytes() for p in files}
        subprocess.run([sys.executable, str(ROOT / 'build_notebooks.py')], check=True, capture_output=True)
        self.assertEqual(before, {p: p.read_bytes() for p in files})

    def test_cross_region_requires_two_distinct_regions(self):
        class Spark:
            class Context:
                applicationId = 'test-application'
            sparkContext = Context()
            def sql(self, query):
                return self
            def collect(self):
                return [(1,)]
        for home, target in [('', ''), ('region-a', 'region-a')]:
            cfg = {'workspace_label': 'test', 'compute_label': 'test', 'workspace_region': home, 'region': target}
            result = shared.run_case('V03', cfg, spark=Spark(), live=True)
            self.assertEqual(result['checks']['fixture_region']['status'], 'BLOCKED')
            self.assertNotIn('dns', result['checks'])

    def test_service_coverage_is_selected_endpoint_specific(self):
        empty = shared.evaluate_evidence('V05', {'evidence_ref': 'test', 'required_services': [], 'services': []})
        self.assertEqual(empty['checks']['service_inventory']['status'], 'BLOCKED')
        ev = {'evidence_ref': 'test', 'required_services': ['selected-service'], 'services': [
            {'service': 'selected-service', 'hostname': 'owned.example', 'configuration_accepted': True,
             'saved_config_ref': 'readback.json', 'functional': 'PASS', 'operation_evidence_ref': 'operation.json'}]}
        result = shared.evaluate_evidence('V05', ev)
        self.assertEqual(result['checks']['selected-service']['status'], 'PASS')
        self.assertEqual(shared.summarize(result), 'REVIEW_REQUIRED')
        ev['services'][0]['functional'] = 'FAIL'
        self.assertEqual(shared.evaluate_evidence('V05', ev)['checks']['selected-service']['status'], 'FAIL')


if __name__ == '__main__':
    unittest.main()
