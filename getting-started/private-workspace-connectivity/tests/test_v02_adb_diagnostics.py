# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Troubleshooting guidance must not mistake DNS/TCP success for source proof."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import v02_adb_diagnostics as diag
from v02_adb_check import run_adb_check
from test_v02_adb_check import Reader


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.cfg = dict(hostname='adb.example.com', port=1522, region='region-a',
                        workspace_region='region-a', expected_service='expected_service',
                        workspace_label='test', compute_label='test', catalog_id='approved_catalog',
                        public_endpoint_confirmed=True)
        self.dns = {'python': {'status': 'RESOLVED', 'addresses': ['192.0.2.8']},
                    'jvm': {'status': 'RESOLVED', 'addresses': ['192.0.2.8']}}

    def diagnose(self, sql=None, query=True, **kwargs):
        reader = Mock(return_value={'checks': {'functional': sql or {'status': 'PASS'}}})
        with patch.object(diag, 'v02_adb_resolve', return_value=self.dns), \
             patch.object(diag, 'v02_adb_tcp', return_value={'status': 'REACHABLE'}):
            result = diag.v02_adb_diagnose(self.cfg, live=True, query=query, adb_reader=reader, **kwargs)
        return result, reader

    def test_disabled_or_invalid_connection_fields_make_no_network_calls(self):
        with patch.object(diag, 'v02_adb_resolve') as resolve:
            self.assertEqual(diag.v02_adb_diagnose({})['assessment'], 'NOT_RUN')
            for host in ('https://adb.example.com', 'db_high', '*.adb.example.com', '192.0.2.8', 'expected_service'):
                cfg = dict(self.cfg, hostname=host)
                self.assertEqual(diag.v02_adb_diagnose(cfg, live=True)['assessment'], 'CHECK_CONNECTION_FIELDS')
            self.assertFalse(resolve.called)

    def test_query_requires_separate_opt_in_and_dns_tcp_are_not_source_proof(self):
        result, reader = self.diagnose(query=False)
        reader.assert_not_called()
        self.assertEqual(result['assessment'], 'READY_FOR_SQL_CHECK')
        self.assertEqual(result['sql']['status'], 'NOT_RUN')
        self.assertIn('do not establish', result['scope'])

    def test_filter_error_directs_customer_to_actual_descriptor_acl_and_route(self):
        result, _ = self.diagnose({'status': 'FAIL', 'error_codes': ['ORA-12529']})
        self.assertEqual(result['assessment'], 'CONNECTION_FILTER_REJECTED')
        self.assertTrue(any('PE DNS proxy' in a for a in result['next_actions']))
        self.assertNotIn('SOURCE_CONTEXT_VERIFIED', json.dumps(result))

    def test_sql_success_still_does_not_verify_source_or_mtls(self):
        result, _ = self.diagnose({'status': 'PASS'})
        self.assertEqual(result['assessment'], 'SQL_ACCEPTED_SOURCE_NOT_VERIFIED')

    def test_missing_sql_setup_is_not_reported_as_ready(self):
        result, _ = self.diagnose({'status': 'NEEDS_SETUP', 'reason': 'Existing Spark session required'})
        self.assertEqual(result['assessment'], 'VERIFY_SQL_SETUP')
        self.assertEqual(result['next_actions'], ['Existing Spark session required'])

    def test_dns_difference_is_observation_not_a_restart_instruction(self):
        self.dns['jvm']['addresses'] = ['192.0.2.9']
        result, _ = self.diagnose(query=False)
        self.assertEqual(result['dns_comparison'], 'DIFFERENT')
        self.assertEqual(result['assessment'], 'COMPARE_PYTHON_AND_JVM_DNS')
        self.assertIn('rotation', result['next_actions'][0])

    def test_failure_classification_and_redaction(self):
        for code, expected in [('ORA-01017', 'CHECK_DATABASE_LOGIN'), ('ORA-29024', 'CHECK_TLS_OR_WALLET'),
                               ('ORA-12514', 'CHECK_DATABASE_SERVICE'), ('ORA-12170', 'SQL_FAILED')]:
            with self.subTest(code=code):
                result, _ = self.diagnose({'status': 'FAIL', 'error_codes': [code]})
                self.assertEqual(result['assessment'], expected)
        with patch.object(diag, 'v02_adb_resolve', return_value=self.dns), \
             patch.object(diag, 'v02_adb_tcp', return_value={'status': 'REACHABLE'}), \
             patch('v02_adb_check.socket.getaddrinfo', return_value=[]):
            result = diag.v02_adb_diagnose(self.cfg, spark=Reader(error='ORA-01017 password=do-not-export'),
                live=True, query=True, adb_reader=run_adb_check)
        self.assertNotIn('do-not-export', json.dumps(result))

    def test_saved_configuration_remains_timestamped_snapshot(self):
        result, _ = self.diagnose(query=False, workspace_snapshot={
            'captured_utc': '2026-01-01T00:00:00Z', 'workspace': {'dns_names': ['adb.example.com']}})
        self.assertEqual(result['saved_dns_entry']['status'], 'EXACT_NAME_LISTED')
        self.assertEqual(result['saved_dns_entry']['captured_utc'], '2026-01-01T00:00:00Z')
        self.assertIn('not a live workspace read', result['saved_dns_entry']['scope'])


if __name__ == '__main__':
    unittest.main()
