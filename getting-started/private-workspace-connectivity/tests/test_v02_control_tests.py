# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Safety and evidence checks for source-control switching, without cloud access."""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v02_control_tests import (V02BucketControl, V02AdbControl, run_v02_control_cycle,
    restore_v02_control_from_journal, v02_error_codes, v02_summary)


class SwitchingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.journal = str(Path(self.directory.name) / 'restore.json')
        self.allow = "Allow dynamic-group reader to read objects in compartment test where all {target.bucket.name='test-bucket', request.networkSource.name='customer-source'}"
        self.policy = {'statements': ['unrelated statement', self.allow], 'etag': '1'}
        self.original = copy.deepcopy(self.policy['statements'])
        self.source = {'name': 'customer-source', 'public_source_list': [],
                       'virtual_source_list': [{'vcn_id': 'customer-vcn', 'ip_ranges': ['10.1.0.0/24']}]}
        self.writes = []
        def put(statements, etag):
            self.assertEqual(etag, self.policy['etag'])
            self.writes.append(statements)
            self.policy = {'statements': statements, 'etag': str(int(etag) + 1)}
        self.control = V02BucketControl(lambda: copy.deepcopy(self.policy), put,
            lambda: copy.deepcopy(self.source), 'policy', self.allow, 'test-bucket',
            'customer-source', 'customer-vcn', '10.1.0.0/24')

    def probe(self, outcome=None):
        return {'outcome': outcome or ('ALLOWED' if self.allow in self.policy['statements'] else 'DENIED'),
                'runtime_id': 'same-spark', 'target_id': 'same-object', 'connection_id': 'same-endpoint'}

    def run_cycle(self, probe=None):
        return run_v02_control_cycle(self.control, probe or self.probe, self.journal,
                                    apply=True, max_wait_seconds=0, interval_seconds=0)

    def test_allow_deny_restore_verifies_and_preserves_statement_count(self):
        result = self.run_cycle()
        self.assertEqual(result['status'], 'SOURCE_CONTEXT_VERIFIED')
        self.assertEqual(self.policy['statements'], self.original)
        self.assertEqual([len(s) for s in self.writes], [2, 2])
        self.assertEqual([s[0] for s in self.writes], ['unrelated statement'] * 2)
        self.assertEqual(json.loads(Path(self.journal).read_text())['result']['restoration'], 'RESTORED')

    def test_baseline_failure_does_not_change_permissions(self):
        result = self.run_cycle(lambda: self.probe('ERROR'))
        self.assertEqual(result['status'], 'NEEDS_SETUP')
        self.assertEqual(self.writes, [])

    def test_bypass_and_timeout_cannot_count_as_expected_denial(self):
        for negative in ('ALLOWED', 'ERROR'):
            with self.subTest(outcome=negative):
                result = self.run_cycle(lambda: self.probe('ALLOWED' if self.allow in self.policy['statements'] else negative))
                self.assertEqual(result['status'], 'INCONCLUSIVE')
                self.assertEqual(self.policy['statements'], self.original)

    def test_exception_during_probe_restores_permissions(self):
        def probe():
            if self.allow not in self.policy['statements']:
                raise RuntimeError('simulated interruption')
            return self.probe()
        result = self.run_cycle(probe)
        self.assertEqual(result['restoration'], 'RESTORED')
        self.assertEqual(self.policy['statements'], self.original)

    def test_uncertain_update_outcome_still_restores(self):
        original_put = self.control.put_policy
        def put(statements, etag):
            original_put(statements, etag)
            if self.allow not in statements:
                raise TimeoutError('server accepted the update but response was lost')
        self.control.put_policy = put
        result = self.run_cycle()
        self.assertEqual(result['restoration'], 'RESTORED')
        self.assertEqual(self.policy['statements'], self.original)

    def test_concurrent_unrelated_edit_is_preserved_and_invalidates_proof(self):
        def probe():
            if self.allow not in self.policy['statements']:
                self.policy['statements'][0] = 'another administrators change'
            return self.probe()
        result = self.run_cycle(probe)
        self.assertEqual(result['status'], 'INCONCLUSIVE')
        self.assertEqual(self.policy['statements'], ['another administrators change', self.allow])

    def test_failed_restoration_retains_journal_and_prevents_new_cycle(self):
        restore = self.control.restore
        self.control.restore = lambda saved: (_ for _ in ()).throw(TimeoutError())
        result = self.run_cycle()
        self.assertEqual(result['status'], 'RESTORE_REQUIRED')
        original_journal = Path(self.journal).read_bytes()
        with self.assertRaises(ValueError):
            self.run_cycle()
        self.assertEqual(Path(self.journal).read_bytes(), original_journal)
        self.control.restore = restore
        self.assertTrue(restore_v02_control_from_journal(self.control, self.journal)['configuration_restored'])
        self.assertEqual(self.policy['statements'], self.original)

    def test_recovery_rejects_different_grant_in_same_policy(self):
        self.run_cycle()
        other = V02BucketControl(self.control.get_policy, self.control.put_policy, self.control.get_source,
            'policy', self.allow.replace('dynamic-group reader', 'dynamic-group another_reader'),
            'test-bucket', 'customer-source', 'customer-vcn', '10.1.0.0/24')
        before = len(self.writes)
        with self.assertRaises(ValueError):
            restore_v02_control_from_journal(other, self.journal)
        self.assertEqual(len(self.writes), before)

    def test_shared_or_broader_grants_and_public_sources_are_rejected(self):
        with self.assertRaises(ValueError):
            V02BucketControl(None, None, None, 'policy', self.allow.replace('where all', 'where any'),
                'test-bucket', 'customer-source', 'customer-vcn', '10.1.0.0/24')
        self.source['public_source_list'] = ['0.0.0.0/0']
        with self.assertRaises(ValueError):
            self.run_cycle()
        self.assertEqual(self.writes, [])

    def test_adb_changes_only_acl_and_requires_fresh_sessions(self):
        database = {'acl': ['customer-vcn'], 'state': 'AVAILABLE', 'etag': 'x',
                    'network': {'private_endpoint': 'unchanged', 'mtls': False}}
        def put_acl(acl, etag):
            database['acl'] = acl
        control = V02AdbControl(lambda: copy.deepcopy(database), put_acl, 'db',
                                'customer-vcn', '10.1.0.0/24', '10.2.0.0/24')
        count = [0]
        def probe():
            count[0] += 1
            return {**self.probe('ALLOWED' if database['acl'] == ['customer-vcn'] else 'DENIED'),
                    'session_id': str(count[0])}
        result = run_v02_control_cycle(control, probe, self.journal, apply=True, max_wait_seconds=0)
        self.assertEqual(result['status'], 'SOURCE_CONTEXT_VERIFIED')
        self.assertEqual(database['acl'], ['customer-vcn'])
        self.assertEqual(database['network'], {'private_endpoint': 'unchanged', 'mtls': False})
        result = run_v02_control_cycle(control, lambda: {**probe(), 'session_id': 'reused'}, self.journal,
                                      apply=True, max_wait_seconds=0)
        self.assertEqual(result['status'], 'INCONCLUSIVE')

    def test_adb_overlapping_negative_subnet_or_broad_acl_rejected(self):
        with self.assertRaises(ValueError):
            V02AdbControl(None, None, 'db', 'vcn', '10.1.0.0/24', '10.1.0.8/32')
        c = V02AdbControl(lambda: {'state': 'AVAILABLE', 'acl': ['0.0.0.0/0']}, None,
                          'db', 'vcn', '10.1.0.0/24', '10.2.0.0/24')
        with self.assertRaises(ValueError):
            c.prepare()

    def test_error_redaction_and_access_success_status(self):
        self.assertEqual(v02_error_codes(RuntimeError('ORA-12529 secret-password=do-not-export')), ['ORA-12529'])
        result = {'checks': {'functional': {'status': 'PASS'}, 'source_path': {'status': 'BLOCKED'}}}
        self.assertEqual(v02_summary(result), 'ACCESS_SUCCEEDED_SOURCE_NOT_VERIFIED')
        self.assertEqual(result['checks']['source_path']['status'], 'NOT_VERIFIED')


if __name__ == '__main__':
    unittest.main()
