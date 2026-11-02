# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Read-only AIDP DNS acceptance harness; standard library outside Spark.

Run the CLI locally to inspect prerequisites without network calls. Live checks run
inside an already-running AIDP Spark notebook. No infrastructure/DNS mutations,
compute lifecycle changes, package installation, or credential persistence.
"""
import argparse
import collections
import datetime
import hashlib
import json
import socket
import time
from pathlib import Path
from urllib.parse import urlsplit

CASE_NAMES = {
    'V01': 'Amazon S3', 'V02': 'Customer Network Source / service gateway',
    'V03': 'Cross-region OCI Object Storage', 'V04': 'Private database baseline and preservation',
    'V05': 'OCI service hostname coverage', 'V06': 'DNS CRUD and validation',
    'V07': 'DNS changes without Spark restart',
}



class Prerequisite(Exception):
    pass


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def same(a, b):
    return canonical(a) == canonical(b)


def zone_match(host, zone):
    host, zone = host.lower().rstrip('.'), zone.lower().rstrip('.')
    return host == zone or host.endswith('.' + zone)


def endpoint_host(value):
    return urlsplit(value if '://' in value else 'https://' + value).hostname


def fixture_read(spark, cfg):
    """Bounded CSV check; connector options apply to this reader, not globally."""
    for key in ('uri', 'endpoint', 'expected_rows'):
        if not cfg.get(key):
            raise Prerequisite('Missing fixture ' + key)
    uri, endpoint = cfg['uri'], cfg['endpoint']
    if '?' in uri or '#' in uri or '?' in endpoint or '#' in endpoint:
        raise Prerequisite('Use unsigned fixture and endpoint references')
    parsed_endpoint = urlsplit(endpoint if '://' in endpoint else 'https://' + endpoint)
    if parsed_endpoint.username is not None or parsed_endpoint.password is not None:
        raise Prerequisite('Endpoint must not contain embedded credentials')
    if not parsed_endpoint.hostname:
        raise Prerequisite('Missing endpoint hostname')
    if uri.startswith('s3a://') and '@' in uri.split('://', 1)[1].split('/', 1)[0]:
        raise Prerequisite('S3 URI must not contain embedded credentials')
    expected = cfg['expected_rows']
    if not isinstance(expected, list) or not 1 <= len(expected) <= 100:
        raise Prerequisite('Supply 1 to 100 expected synthetic CSV rows')
    size = cfg.get('fixture_size_bytes')
    if not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= 65536:
        raise Prerequisite('Verify fixture size is between 1 byte and 64 KiB')
    reader = spark.read.option('header', 'true').option('inferSchema', 'false')
    if uri.startswith('s3a://'):
        if not cfg.get('region'):
            raise Prerequisite('Missing AWS region')
        reader = reader.option('fs.s3a.endpoint', endpoint).option(
            'fs.s3a.endpoint.region', cfg['region'])
    elif uri.startswith('oci://'):
        authority = uri.split('://', 1)[1].split('/', 1)[0]
        if authority.count('@') != 1:
            raise Prerequisite('OCI URI must specify bucket@namespace')
        bucket, namespace = authority.split('@')
        reader = reader.option('fs.oci.client.hostname.' + bucket + '.' + namespace,
                               endpoint)
    else:
        raise Prerequisite('Use s3a:// or oci://')
    rows = reader.csv(uri).limit(len(expected) + 1).collect()
    actual = [r.asDict() for r in rows]
    assert collections.Counter(map(canonical, actual)) == collections.Counter(
        map(canonical, expected)), 'Fixture content mismatch'
    return {'rows': len(actual), 'fixture_assertion': 'exact row multiset',
            'configured_endpoint': endpoint}


def adb_read(spark, cfg, runtime):
    """Use an existing catalog connection or an existing wallet ZIP.

    runtime carries secrets in memory only and is never included in results.
    No wallet is generated/copied; provide a supported existing ZIP or catalog.
    """
    if not cfg.get('expected_instance'):
        raise Prerequisite('Missing expected database identity')
    reader = (spark.read.format('aidataplatform').option('type', 'ORACLE_ALH')
              .option('schema', 'SYS').option('table', 'DUAL')
              .option('pushdown.sql',
                      "SELECT sys_context('userenv','instance_name') AS INSTANCE_NAME FROM dual")
              .option('login.timeout.seconds', '20'))
    if cfg.get('catalog_id'):
        reader = reader.option('catalog.id', cfg['catalog_id'])
    else:
        if not cfg.get('wallet_zip') or not cfg.get('tns') or not cfg.get('user'):
            raise Prerequisite('Existing catalog or wallet ZIP/TNS/user required')
        password = runtime.get('adb_password')
        if not password:
            raise Prerequisite('ADB password is missing from runtime context')
        reader = (reader.option('wallet.path', cfg['wallet_zip'])
                  .option('tns', cfg['tns']).option('user.name', cfg['user'])
                  .option('password', password))
    rows = reader.load().limit(2).collect()
    assert len(rows) == 1, 'Identity query must return one row'
    assert str(rows[0][0]) == cfg['expected_instance'], 'Unexpected database identity'
    return {'rows': 1, 'identity_matches': True,
            'transport_claim': 'Authenticated query; mTLS not independently established'}


def run_case(case, cfg, spark=None, runtime=None, live=False, phase='baseline'):
    if case not in CASE_NAMES:
        raise ValueError('Unknown case')
    result = {'case': case, 'name': CASE_NAMES[case], 'utc': utc(), 'phase': phase,
              'workspace_label': cfg.get('workspace_label'), 'compute_label': cfg.get('compute_label'),
              'checks': {}, 'origin': 'harness'}
    checks = result['checks']
    def check(name, action):
        started = time.monotonic()
        try:
            checks[name] = {'status': 'PASS', 'detail': action()}
        except Prerequisite as exc:
            checks[name] = {'status': 'BLOCKED', 'reason': str(exc)}
        except Exception as exc:
            # Arbitrary driver errors may echo credentials/JDBC URLs. Never persist them.
            checks[name] = {'status': 'FAIL', 'error_type': type(exc).__name__}
        checks[name]['elapsed_seconds'] = round(time.monotonic() - started, 3)
    if not live:
        checks['execution'] = {'status': 'NOT_RUN', 'reason': 'Live execution disabled'}
        return result
    if spark is None:
        checks['execution'] = {'status': 'BLOCKED', 'reason': 'Existing Spark session required'}
        return result
    if not cfg.get('workspace_label') or not cfg.get('compute_label'):
        checks['execution'] = {'status': 'BLOCKED', 'reason': 'Record workspace and compute labels'}
        return result
    def spark_control():
        value = spark.sql('SELECT 1').collect()[0][0]
        assert value == 1, 'Spark control query returned unexpected result'
        return {'select_1': value, 'application_id': spark.sparkContext.applicationId}
    check('spark_control', spark_control)
    if case in ('V05', 'V06'):
        checks['configuration'] = {'status': 'BLOCKED', 'reason': 'Supply Workbench evidence for evaluate_evidence()'}
        return result
    if case == 'V03' and (not cfg.get('region') or not cfg.get('workspace_region') or
                          cfg['region'] == cfg['workspace_region']):
        checks['fixture_region'] = {'status': 'BLOCKED', 'reason': 'Select verified fixture in a different region'}
        return result
    host = cfg.get('hostname') or endpoint_host(cfg.get('endpoint', ''))
    if not host:
        checks['dns'] = {'status': 'BLOCKED', 'reason': 'Missing exact hostname'}
    else:
        check('dns', lambda: {'addresses': sorted({a[4][0] for a in socket.getaddrinfo(
            host, cfg.get('port', 443), type=socket.SOCK_STREAM)})})
        if cfg.get('probe_tcp', False):
            def tcp():
                with socket.create_connection((host, cfg.get('port', 443)), timeout=5):
                    return {'connected': True, 'tls_verified': False}
            check('tcp', tcp)
        else:
            checks['tcp'] = {'status': 'NOT_RUN'}
    if cfg.get('kind') == 'csv':
        check('functional', lambda: fixture_read(spark, cfg))
    elif cfg.get('kind') == 'adb':
        check('functional', lambda: adb_read(spark, cfg, runtime or {}))
        checks['mtls'] = {'status': 'BLOCKED', 'reason': 'Requires separate effective-descriptor and client-certificate evidence'}
    else:
        checks['functional'] = {'status': 'BLOCKED', 'reason': 'Configure kind=csv or adb'}
    checks['source_path'] = {'status': 'BLOCKED', 'reason': 'Independent path evidence required; successful read is insufficient'}
    return result


def evaluate_evidence(case, evidence):
    """Validate explicitly supplied Workbench snapshots; never modify Workbench.

    These assertions check recorded observations. External source-path, private
    endpoint and mTLS claims always require human evidence review.
    """
    result = {'case': case, 'utc': utc(), 'origin': 'supplied evidence', 'checks': {}}
    checks = result['checks']
    if not evidence.get('evidence_ref'):
        checks['evidence'] = {'status': 'BLOCKED', 'reason': 'Missing evidence artifact reference'}
        return result
    def test(name, condition):
        checks[name] = {'status': 'PASS' if condition else 'FAIL'}
    before = evidence.get('before')
    restored = evidence.get('restored')
    if case == 'V06':
        if not isinstance(before, dict) or not isinstance(restored, dict):
            checks['snapshots'] = {'status': 'BLOCKED'}
            return result
        test('restored_baseline', same(before, restored))
        steps = {s.get('name'): s for s in evidence.get('steps', [])}
        for name in ('add', 'edit', 'delete', 'cancel'):
            step = steps.get(name, {})
            valid = bool(step.get('work_request_ref') or (name == 'cancel' and step.get('ui_evidence_ref')))
            valid = valid and isinstance(step.get('actual_snapshot'), dict) and isinstance(step.get('expected_snapshot'), dict)
            test(name, valid and same(step.get('actual_snapshot'), step.get('expected_snapshot')))
        for name in ('blank', 'underscore', 'path', 'query', 'fragment', 'malformed'):
            step = steps.get(name, {})
            test('reject_' + name, step.get('rejected') is True and
                 bool(step.get('ui_evidence_ref')) and same(before, step.get('actual_snapshot')))
        partial = steps.get('partial', {})
        if partial.get('performed'):
            test('partial_readback_recorded', bool(partial.get('work_request_ref')) and
                 partial.get('work_request_status') in ('FAILED', 'SUCCEEDED') and
                 isinstance(partial.get('saved_hostnames'), list) and
                 bool(partial.get('readback_evidence_ref')))
        else:
            checks['partial_observation'] = {'status': 'NOT_RUN',
                'reason': 'Optional partial-provisioning observation not performed'}
    elif case in ('V04', 'V07'):
        phases = evidence.get('phases', [])
        expected_phases = {'before', 'after_add', 'after_edit', 'after_delete', 'restored'}
        test('complete_phases', {p.get('phase') for p in phases} == expected_phases and len(phases) == 5)
        test('functional_each_phase', bool(phases) and all(p.get('functional') == 'PASS' and
             p.get('operation_evidence_ref') for p in phases))
        test('restored_baseline', isinstance(before, dict) and same(before, restored))
        snapshots = {p.get('phase'): p.get('configuration_snapshot') for p in phases}
        test('configuration_change_exercised', all(isinstance(v, dict) for v in snapshots.values()) and
             all(k in snapshots for k in expected_phases) and
             not same(snapshots.get('before'), snapshots.get('after_add')) and
             not same(snapshots.get('after_add'), snapshots.get('after_edit')) and
             same(snapshots.get('before'), snapshots.get('after_delete')) and
             same(snapshots.get('before'), snapshots.get('restored')))
        if case == 'V07':
            ids = {p.get('application_id') for p in phases}
            test('same_spark_application', len(ids) == 1 and None not in ids and '' not in ids)
            test('no_restart_events', evidence.get('restart_count') == 0 and
                 bool(evidence.get('lifecycle_evidence_ref')))
            test('dns_each_phase', bool(phases) and all(p.get('dns_addresses') and
                 p.get('expected_dns_addresses') and p.get('mapping_evidence_ref') and
                 set(p['dns_addresses']) == set(p['expected_dns_addresses']) for p in phases))
        else:
            test('same_database_identity', bool(evidence.get('expected_instance')) and bool(phases) and
                 all(p.get('instance') == evidence['expected_instance'] for p in phases))
    elif case == 'V05':
        services = evidence.get('services', [])
        required = evidence.get('required_services', [])
        if not required or not services:
            checks['service_inventory'] = {'status': 'BLOCKED',
                'reason': 'Select services and their actual resource hostnames'}
            return result
        for service in required:
            matches = [s for s in services if s.get('service') == service]
            test(service, len(matches) == 1 and bool(matches[0].get('hostname')) and
                 matches[0].get('configuration_accepted') is True and
                 matches[0].get('functional') == 'PASS' and
                 bool(matches[0].get('saved_config_ref')) and
                 bool(matches[0].get('operation_evidence_ref')))
        checks['coverage_review'] = {'status': 'REVIEW_REQUIRED',
            'reason': 'Review endpoint scope and observed configuration; no automatic service-wide claim'}
    else:
        checks['external_evidence'] = {'status': 'REVIEW_REQUIRED',
                                      'reason': 'Review correlated route/source, endpoint or transport evidence'}
    return result


def summarize(result):
    """No overall acceptance PASS: case-specific evidence review remains explicit."""
    states = [v['status'] for v in result['checks'].values()]
    if 'FAIL' in states:
        return 'FAIL'
    if 'BLOCKED' in states:
        return 'BLOCKED'
    if 'NOT_RUN' in states:
        return 'INCOMPLETE'
    return 'REVIEW_REQUIRED'


def write_results(path, results):
    payload = {'generated_utc': utc(), 'results': results}
    # Do not persist configuration, passwords, client exception text, or row contents.
    content = json.dumps(payload, indent=2) + '\n'
    Path(path).write_text(content)
    return hashlib.sha256(content.encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--evidence')
    parser.add_argument('--output', default='results.json')
    args = parser.parse_args()
    cfg = json.loads(Path(args.config).read_text())
    results = [run_case(c, cfg.get(c, {})) for c in CASE_NAMES]
    if args.evidence:
        ev = json.loads(Path(args.evidence).read_text())
        results += [evaluate_evidence(c, v) for c, v in ev.items() if c in CASE_NAMES]
    for r in results:
        r['acceptance'] = summarize(r)
    print('Result artifact SHA-256:', write_results(args.output, results))


if __name__ == '__main__':
    main()
