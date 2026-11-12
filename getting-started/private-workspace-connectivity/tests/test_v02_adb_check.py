# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Offline checks for the ADB half of V02; no cloud calls."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import v02_adb_check as adb


class Reader:
    def __init__(self, service='expected_service', error=None):
        self.options = {}
        self.service = service
        self.error = error
        self.read = self

    def format(self, value):
        self.options['format'] = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        if self.error:
            raise RuntimeError(self.error)
        return self

    def limit(self, n):
        self.limit_value = n
        return self

    def collect(self):
        class Row:
            def asDict(row):
                return {'SERVICE_NAME': self.service, 'SESSION_USER': 'TEST_READER', 'CLIENT_IP': '10.20.1.5'}
        return [Row()]


class AdbChecks(unittest.TestCase):
    def config(self):
        return dict(workspace_label='test', compute_label='test', hostname='adb.example',
                    workspace_region='region-a', region='region-a', catalog_id='approved_catalog',
                    expected_service='expected_service', public_endpoint_confirmed=True)

    def run_check(self, cfg, reader):
        with patch.object(adb.socket, 'getaddrinfo', return_value=[(0, 0, 0, '', ('192.0.2.10', 1522))]):
            return adb.run_adb_check(cfg, spark=reader, live=True)

    def test_disabled_and_unverified_connection_make_no_calls(self):
        with patch.object(adb.socket, 'getaddrinfo') as dns:
            self.assertEqual(adb.run_adb_check({})['checks']['execution']['status'], 'NOT_RUN')
            cfg = self.config();cfg['public_endpoint_confirmed'] = False
            reader = Reader();result = adb.run_adb_check(cfg, spark=reader, live=True)
            self.assertEqual(result['checks']['execution']['status'], 'NEEDS_SETUP')
            self.assertEqual(reader.options, {})
            dns.assert_not_called()

    def test_catalog_query_is_bounded_and_cannot_prove_source_restriction(self):
        reader = Reader();result = self.run_check(self.config(), reader)
        self.assertEqual(result['checks']['functional']['status'], 'PASS')
        self.assertEqual(result['checks']['source_path']['status'], 'NOT_VERIFIED')
        self.assertEqual(reader.options['catalog.id'], 'approved_catalog')
        self.assertNotIn('password', reader.options)
        self.assertEqual(reader.limit_value, 2)
        self.assertTrue(reader.options['pushdown.sql'].startswith('SELECT '))
        self.assertTrue(reader.options['pushdown.sql'].endswith(' FROM dual'))

    def test_wrong_database_service_fails(self):
        result = self.run_check(self.config(), Reader(service='unexpected_service'))
        self.assertEqual(result['checks']['functional']['status'], 'FAIL')

    def test_driver_error_does_not_export_credentials(self):
        reader = Reader(error='password=do-not-export; jdbc-url=private-value')
        result = self.run_check(self.config(), reader)
        self.assertEqual(result['checks']['functional']['status'], 'FAIL')
        self.assertNotIn('do-not-export', json.dumps(result))
        self.assertNotIn('private-value', json.dumps(result))

    def test_other_region_blocks_before_connection(self):
        cfg = self.config();cfg['region'] = 'region-b'
        reader = Reader();result = self.run_check(cfg, reader)
        self.assertEqual(result['checks']['execution']['status'], 'NEEDS_SETUP')
        self.assertEqual(reader.options, {})
