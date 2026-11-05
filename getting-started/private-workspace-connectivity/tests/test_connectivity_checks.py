"""Offline acceptance regressions; no network, cloud or Spark runtime required."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import connectivity_checks as h


class FakeRow(dict):
    def asDict(self):
        return dict(self)


class Reader:
    def __init__(self, rows):
        self.rows = rows
    def option(self, *args):
        return self
    def csv(self, *args):
        return self
    def limit(self, n):
        self.rows = self.rows[:n]
        return self
    def collect(self):
        return self.rows


class AcceptanceRegressions(unittest.TestCase):
    def test_plan_cannot_pass(self):
        for case in h.CASE_NAMES:
            result = h.run_case(case, {})
            self.assertEqual(h.summarize(result), 'INCOMPLETE')

    def test_missing_spark_blocks_live(self):
        self.assertEqual(h.summarize(h.run_case('V01', {}, live=True)), 'BLOCKED')

    def test_signed_fixture_is_rejected(self):
        with self.assertRaises(h.Prerequisite):
            h.fixture_read(None, {'uri': 's3a://b/f?token=secret',
                                 'endpoint': 's3.us-east-1.amazonaws.com',
                                 'expected_rows': [{'id': '1'}]})

    def test_embedded_endpoint_credentials_are_rejected(self):
        with self.assertRaises(h.Prerequisite):
            h.fixture_read(None, {'uri': 's3a://fixture.example/sample.csv',
                                 'endpoint': 'https://user:secret@service.example',
                                 'region': 'test-region', 'fixture_size_bytes': 32,
                                 'expected_rows': [{'id': '1'}]})

    def test_unverified_fixture_size_is_blocked(self):
        with self.assertRaises(h.Prerequisite):
            h.fixture_read(None, {'uri': 'oci://fixture@namespace/sample.csv',
                                 'endpoint': 'https://service.example',
                                 'fixture_size_bytes': 0,
                                 'expected_rows': [{'id': '1'}]})

    def test_extra_and_duplicate_fixture_rows_fail(self):
        cfg = {'uri': 'oci://b@n/f.csv', 'endpoint': 'https://objectstorage.us-phoenix-1.oraclecloud.com',
               'fixture_size_bytes': 32, 'expected_rows': [{'id': '1'}, {'id': '2'}]}
        for actual in ([{'id': '1'}, {'id': '1'}],
                       [{'id': '1'}, {'id': '2'}, {'id': '3'}]):
            with self.assertRaises(AssertionError):
                h.fixture_read(SimpleNamespace(read=Reader([FakeRow(r) for r in actual])), cfg)

    def test_row_order_is_not_significant(self):
        cfg = {'uri': 'oci://b@n/f.csv', 'endpoint': 'https://objectstorage.us-phoenix-1.oraclecloud.com',
               'fixture_size_bytes': 32, 'expected_rows': [{'id': '1'}, {'id': '2'}]}
        output = h.fixture_read(SimpleNamespace(read=Reader([FakeRow(id='2'), FakeRow(id='1')])), cfg)
        self.assertEqual(output['rows'], 2)

    def test_zone_suffix_boundary(self):
        zone = 'adb.us-ashburn-1.oraclecloud.com'
        self.assertTrue(h.zone_match('x.' + zone, zone))
        self.assertFalse(h.zone_match('evil' + zone, zone))
        self.assertFalse(h.zone_match(zone + '.attacker.example', zone))

    def test_missing_evidence_does_not_pass(self):
        for case in h.CASE_NAMES:
            self.assertEqual(h.summarize(h.evaluate_evidence(case, {})), 'BLOCKED')

    def private_database_phases(self):
        baseline = {'dns_zones': ['legacy.example'], 'scan': []}
        snapshots = [baseline, {'dns_zones': ['legacy.example', 'a.example'], 'scan': []},
                     {'dns_zones': ['legacy.example', 'b.example'], 'scan': []}, baseline, baseline]
        return {'evidence_ref': 'observations.json', 'before': baseline, 'restored': baseline,
                'phases': [{'phase': name, 'functional': 'PASS',
                            'operation_evidence_ref': name + '.json', 'configuration_snapshot': snap}
                           for name, snap in zip(['before', 'after_add', 'after_edit', 'after_delete', 'restored'], snapshots)]}





    def test_private_database_preserved_across_dns_edits(self):
        e = self.private_database_phases()
        e['expected_instance'] = 'original-database'
        for phase in e['phases']:
            phase['instance'] = 'original-database'
        r = h.evaluate_evidence('V04', e)
        self.assertTrue(all(v['status'] == 'PASS' for v in r['checks'].values()))
        self.assertNotIn('no_restart_events', r['checks'])
        self.assertEqual(h.summarize(r), 'REVIEW_REQUIRED')
        e['phases'][2]['instance'] = 'different-database'
        self.assertEqual(h.evaluate_evidence('V04', e)['checks']['same_database_identity']['status'], 'FAIL')

    def test_private_database_baseline_alone_is_incomplete(self):
        e = self.private_database_phases()
        e['expected_instance'] = 'original-database'
        e['phases'] = e['phases'][:1]
        e['phases'][0]['instance'] = 'original-database'
        r = h.evaluate_evidence('V04', e)
        self.assertEqual(r['checks']['complete_phases']['status'], 'FAIL')
        self.assertEqual(r['checks']['configuration_change_exercised']['status'], 'FAIL')

    def test_private_database_unchanged_dns_does_not_validate_preservation(self):
        e = self.private_database_phases()
        e['expected_instance'] = 'original-database'
        for phase in e['phases']:
            phase['instance'] = 'original-database'
            phase['configuration_snapshot'] = e['before']
        r = h.evaluate_evidence('V04', e)
        self.assertEqual(r['checks']['configuration_change_exercised']['status'], 'FAIL')


    def test_driver_secret_not_written_to_results(self):
        class BadSpark:
            def sql(self, query):
                raise RuntimeError('password=DO_NOT_PERSIST_ME')
        r = h.run_case('V01', {'workspace_label': 'test-workspace', 'compute_label': 'test-compute'},
                       spark=BadSpark(), live=True)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'result.json'
            h.write_results(p, [r])
            self.assertNotIn('DO_NOT_PERSIST_ME', p.read_text())
            self.assertEqual(json.loads(p.read_text())['results'][0]['checks']['spark_control']['error_type'], 'RuntimeError')


if __name__ == '__main__':
    unittest.main()
