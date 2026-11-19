# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Guard against mistaking DNS/configuration or unrelated reads for route proof."""
import copy
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v02_route_check import v02_route_report


class RouteEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.cfg = {'hostname': 'objectstorage.region-a.example',
                    'endpoint': 'https://objectstorage.region-a.example',
                    'uri': 'oci://bucket@namespace/v02/sample.csv',
                    'region': 'region-a', 'workspace_region': 'region-a'}
        self.read = {'target': 'bucket', 'phase': 'before', 'object_uri': self.cfg['uri'],
                     'utc': '2026-01-01T12:00:00+00:00', 'completed_utc': '2026-01-01T12:00:10+00:00',
                     'checks': {'dns': {'status': 'PASS', 'detail': {'addresses': ['255.1.2.3']}},
                                'functional': {'status': 'PASS', 'detail': {'configured_endpoint': self.cfg['endpoint']}}}}
        self.snapshot = {
            'captured_utc': '2026-01-01T11:59:00+00:00', 'region': 'region-a',
            'workspace': {'subnet_id': 'subnet', 'dns_names': [self.cfg['hostname']]},
            'subnet': {'id': 'subnet', 'vcn_id': 'customer-vcn', 'cidr_block': '10.20.1.0/24', 'route_table_id': 'routes'},
            'route_table': {'id': 'routes', 'route_rules': [
                {'destination_type': 'SERVICE_CIDR_BLOCK', 'destination': 'all-region-a-services', 'network_entity_id': 'sgw'}]},
            'service_gateway': {'id': 'sgw', 'vcn_id': 'customer-vcn', 'block_traffic': False,
                                'lifecycle_state': 'AVAILABLE', 'services': [{'service_id': 'all-services'}]},
            'service': {'id': 'all-services', 'cidr_block': 'all-region-a-services'},
            'network_source': {'name': 'source', 'public_source_list': [], 'services': ['all'],
                               'virtual_source_list': [{'vcn_id': 'customer-vcn', 'ip_ranges': ['10.20.1.0/24']}]}}
        self.event = {'bucketName': 'bucket', 'namespace': 'namespace', 'objectName': 'v02/sample.csv',
                      'requestAction': 'GET', 'statusCode': 200, 'apiType': 'native', 'isPar': False,
                      'opcRequestId': 'request-one', 'principalId': 'reader-one', 'clientIpAddress': '10.20.1.8',
                      'startTime': '2026-01-01T12:00:05Z', 'credentials': 'never-print-this'}
        self.review = {'principal_id': 'reader-one', 'object_uri': self.cfg['uri'],
                       'network_source_name': 'source', 'no_bypass_grants': True,
                       'reviewed_utc': '2026-01-01T11:59:30+00:00', 'evidence_ref': 'review-record'}

    def report(self, **kwargs):
        values = dict(cfg=self.cfg, results=[self.read], snapshot=self.snapshot,
                      event=self.event, review=self.review)
        values.update(kwargs)
        return v02_route_report(**values)

    def test_dns_and_route_configuration_do_not_confirm_runtime_path(self):
        result = self.report(event={}, review={})
        self.assertEqual(result['dns_addresses'], ['255.1.2.3'])
        self.assertEqual(result['configuration'], 'CONFIGURED_IN_SNAPSHOT')
        self.assertEqual(result['customer_vcn_source'], 'NOT_CONFIRMED')

    def test_correlated_source_control_evidence_and_secret_redaction(self):
        result = self.report(event={'logContent': {'data': self.event}})
        self.assertEqual(result['customer_vcn_source'], 'CONFIRMED_BY_REVIEWED_SOURCE_CONTROL')
        self.assertEqual(result['missing_checks'], [])
        self.assertNotIn('never-print-this', json.dumps(result))
        self.assertNotIn('credentials', result['request_evidence'])

    def test_wrong_request_or_source_never_confirms(self):
        mismatches = {'principalId': 'another-reader', 'objectName': 'another-object',
                      'namespace': 'another-namespace', 'clientIpAddress': '255.1.2.3',
                      'startTime': '2026-01-01T11:00:00Z', 'requestAction': 'HEAD',
                      'statusCode': 403, 'isPar': True, 'opcRequestId': ''}
        for field, value in mismatches.items():
            with self.subTest(field=field):
                result = self.report(event={**self.event, field: value})
                self.assertEqual(result['customer_vcn_source'], 'NOT_CONFIRMED')

    def test_wrong_route_or_disabled_gateway_never_confirms(self):
        for target, field, value in [('workspace', 'subnet_id', 'other-subnet'),
                                     ('service_gateway', 'block_traffic', True),
                                     ('service_gateway', 'vcn_id', 'service-vcn'),
                                     ('service', 'cidr_block', 'another-service'),
                                     ('route_table', 'id', 'unattached-table')]:
            with self.subTest(target=target, field=field):
                snapshot = copy.deepcopy(self.snapshot)
                snapshot[target][field] = value
                self.assertEqual(self.report(snapshot=snapshot)['customer_vcn_source'], 'NOT_CONFIRMED')

    def test_bypass_or_multiple_vcns_or_stale_review_never_confirms(self):
        for field, value in [('public_source_list', ['0.0.0.0/0']),
                             ('virtual_source_list', self.snapshot['network_source']['virtual_source_list'] +
                              [{'vcn_id': 'other-vcn', 'ip_ranges': ['10.20.1.0/24']}])]:
            with self.subTest(field=field):
                snapshot = copy.deepcopy(self.snapshot)
                snapshot['network_source'][field] = value
                self.assertEqual(self.report(snapshot=snapshot)['customer_vcn_source'], 'NOT_CONFIRMED')
        for updates in ({'no_bypass_grants': False}, {'evidence_ref': ''},
                        {'reviewed_utc': '2026-01-01T11:00:00Z'}, {'object_uri': 'oci://other@namespace/object'}):
            with self.subTest(updates=updates):
                self.assertEqual(self.report(review={**self.review, **updates})['customer_vcn_source'], 'NOT_CONFIRMED')

    def test_wrong_phase_missing_run_window_or_failed_read_never_confirms(self):
        self.assertEqual(self.report(phase='after_add')['customer_vcn_source'], 'NOT_CONFIRMED')
        read = copy.deepcopy(self.read)
        del read['completed_utc']
        self.assertEqual(self.report(results=[read])['customer_vcn_source'], 'NOT_CONFIRMED')
        read['completed_utc'] = self.read['completed_utc']
        read['checks']['functional']['status'] = 'FAIL'
        self.assertEqual(self.report(results=[read])['customer_vcn_source'], 'NOT_CONFIRMED')


if __name__ == '__main__':
    unittest.main()
