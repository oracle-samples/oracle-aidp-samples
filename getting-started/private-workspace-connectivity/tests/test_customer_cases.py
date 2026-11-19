# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Safety and customer-result checks for the three standalone walkthroughs."""
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
FILES = {'V01': 'V01_Amazon_S3.ipynb', 'V03': 'V03_Cross_Region_Object_Storage.ipynb',
         'V04': 'V04_Service_Private_Endpoint.ipynb'}


class CustomerCases(unittest.TestCase):
    def notebook(self, case):
        return json.loads((ROOT / FILES[case]).read_text())

    def source(self, case, suffix):
        cell = next(c for c in self.notebook(case)['cells'] if c['id'] == case.lower() + '-' + str(suffix))
        return ''.join(cell['source'])

    def environment(self, case):
        env = {}
        with patch('builtins.print'):
            exec(self.source(case, 5), env)
        exec(self.source(case, 7), env)
        return env

    def test_session_guard_explains_recovery(self):
        for case, cell in [('V01', 11), ('V03', 13), ('V04', 11)]:
            with self.subTest(case=case), self.assertRaisesRegex(RuntimeError, 'Initialize ' + case):
                exec(self.source(case, cell), {})

    def test_reinitialization_preserves_customer_state(self):
        for case in FILES:
            env = self.environment(case)
            cfg, results = env['CONFIG'], [{'phase': 'before'}]
            env['RESULTS'] = results
            with patch('builtins.print'):
                exec(self.source(case, 5), env)
            self.assertIs(env['CONFIG'], cfg)
            self.assertIs(env['RESULTS'], results)

    def test_dry_run_never_resolves_or_reads(self):
        for case in FILES:
            env = self.environment(case)
            env['run_case'] = Mock(side_effect=AssertionError('unexpected workload'))
            # Diagnostic dry run is independent of Spark and DNS.
            with patch.object(env['socket'], 'getaddrinfo', side_effect=AssertionError('unexpected DNS')):
                result = env['diagnose_customer_target'](env['CONFIG'], live=False, case=case)
                self.assertEqual(result['checks']['execution']['status'], 'NOT_RUN')
            with self.assertRaisesRegex(ValueError, 'without quotes'):
                env['run_customer_case'](case, env['CONFIG'], live='False')
            env['run_case'].assert_not_called()

    def test_private_descriptor_confirmation_precedes_workload(self):
        env = self.environment('V04')
        env['run_case'] = Mock()
        for confirmed in (False, None, 'True'):
            cfg = {**env['CONFIG'], 'private_endpoint_confirmed': confirmed}
            result = env['run_customer_case']('V04', cfg, live=True)
            self.assertEqual(result['acceptance'], 'SETUP_REQUIRED')
        env['run_case'].assert_not_called()
        env['run_case'].return_value = {'checks': {'functional': {'status': 'PASS'}}}
        result = env['run_customer_case']('V04', {**env['CONFIG'], 'private_endpoint_confirmed': True}, live=True)
        self.assertEqual(result['acceptance'], 'READ_PASSED')

    def test_missing_evidence_does_not_relabel_a_successful_read_as_blocked(self):
        env = self.environment('V04')
        result = env['customer_labels']({'checks': {'functional': {'status': 'PASS'},
            'source_path': {'status': 'BLOCKED'}, 'mtls': {'status': 'BLOCKED'}}})
        self.assertEqual(result['acceptance'], 'READ_PASSED')
        self.assertEqual(result['checks']['source_path']['status'], 'NOT_ASSESSED')
        self.assertEqual(result['checks']['mtls']['status'], 'NOT_ASSESSED')
        result['checks']['functional']['status'] = 'FAIL'
        self.assertEqual(env['customer_summary'](result), 'FAIL')

    def test_local_control_rejects_remote_fixture_before_reading(self):
        env = self.environment('V03')
        env.update(RUN_LIVE=True, PHASE='before', CONTROL_CONFIG={**env['CONFIG'], 'region': 'region-b'})
        env['CONFIG'].update(workspace_region='region-a')
        env['run_customer_case'] = Mock()
        with self.assertRaisesRegex(ValueError, 'separate fixture'):
            exec(self.source('V03', 15), env)
        env['run_customer_case'].assert_not_called()

    def test_case_specific_authorization_and_logging_are_before_execution(self):
        for case in FILES:
            nb = self.notebook(case)
            ids = [c['id'] for c in nb['cells']]
            self.assertEqual(len(ids), len(set(ids)))
            self.assertLess(ids.index(case.lower() + '-logging'), ids.index(case.lower() + '-5'))
            text = '\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type'] == 'markdown')
            self.assertNotIn('lab-readiness', text)
            self.assertNotIn('source_path: BLOCKED', text)
            for other in set(FILES) - {case}:
                self.assertNotIn(other, text)
            self.assertIn('## Troubleshooting', text)


if __name__ == '__main__':
    unittest.main()
