# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Explain V02 routing without treating translated DNS answers as route proof.

The administrator supplies OCI configuration records and, optionally, an Object
Storage read event plus a review of effective grants. No cloud calls or policy
changes are made by these functions.
"""
import datetime
import ipaddress
from urllib.parse import urlsplit


def v02_route_report(cfg, results, snapshot=None, event=None, review=None, phase='before'):
    snapshot, event, review = snapshot or {}, event or {}, review or {}
    report = {'phase': phase, 'dns_addresses': [], 'configuration': 'NOT_CHECKED',
              'customer_vcn_source': 'NOT_CONFIRMED', 'checks': {},
              'scope': 'This bucket request only; database routing is checked separately.'}
    checks = report['checks']
    reads = [r for r in results if r.get('target') == 'bucket' and r.get('phase') == phase]
    read = reads[-1] if reads else {}
    report['dns_addresses'] = read.get('checks', {}).get('dns', {}).get('detail', {}).get('addresses', [])
    report['dns_meaning'] = ('DNS PASS means the notebook driver resolved the hostname. '
        'AIDP mapped destination addresses are service-side addresses for the private connection; '
        'they need not be in the customer subnet. DNS alone does not establish the request path.')
    report['bucket_read'] = read.get('checks', {}).get('functional', {}).get('status', 'NOT_RUN')

    def timestamp(value):
        try:
            parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            return parsed.timestamp() if parsed.tzinfo else None
        except (AttributeError, TypeError, ValueError):
            return None

    def in_cidr(address, cidr):
        try:
            return ipaddress.ip_address(address) in ipaddress.ip_network(cidr)
        except (TypeError, ValueError):
            return False

    subnet = snapshot.get('subnet', {})
    workspace = snapshot.get('workspace', {})
    table = snapshot.get('route_table', {})
    gateway = snapshot.get('service_gateway', {})
    service = snapshot.get('service', {})
    source = snapshot.get('network_source', {})
    expected_host = urlsplit(cfg.get('endpoint', '')).hostname
    checks['same_region'] = bool(cfg.get('region') and
        cfg['region'] == cfg.get('workspace_region') == snapshot.get('region'))
    checks['workspace_destination'] = bool(expected_host and
        expected_host == cfg.get('hostname') and expected_host in workspace.get('dns_names', []))
    checks['workspace_subnet'] = bool(subnet.get('id') and workspace.get('subnet_id') == subnet['id'])
    checks['subnet_route_table'] = bool(table.get('id') and subnet.get('route_table_id') == table['id'])
    checks['gateway_in_customer_vcn'] = bool(subnet.get('vcn_id') and
        subnet['vcn_id'] == gateway.get('vcn_id'))
    checks['gateway_available'] = gateway.get('lifecycle_state') == 'AVAILABLE' and gateway.get('block_traffic') is False
    checks['regional_service_enabled'] = bool(service.get('id') and service.get('cidr_block') and
        any(s.get('service_id') == service['id'] for s in gateway.get('services', [])))
    checks['service_route'] = bool(gateway.get('id') and service.get('cidr_block') and any(
        r.get('destination_type') == 'SERVICE_CIDR_BLOCK' and
        r.get('destination') == service['cidr_block'] and
        r.get('network_entity_id') == gateway['id'] for r in table.get('route_rules', [])))
    configuration_checks = list(checks)
    if all(checks[k] for k in configuration_checks):
        report['configuration'] = 'CONFIGURED_IN_SNAPSHOT'
    elif snapshot:
        report['configuration'] = 'INCOMPLETE_OR_DIFFERENT'
    report['snapshot_utc'] = snapshot.get('captured_utc')
    report['configured_path'] = ('AIDP mapped destination -> workspace private connection -> '
        + subnet.get('display_name', 'customer subnet') + ' [' + subnet.get('cidr_block', 'unknown CIDR')
        + '] -> ' + gateway.get('display_name', 'customer service gateway')
        + ' -> ' + (expected_host or 'Object Storage hostname'))

    # Accept a single Console service-log event, or its data object. Do not copy
    # the event wholesale: Object Storage logs can contain a credentials field.
    event = event.get('logContent', event)
    event = event.get('data', event)
    uri = urlsplit(cfg.get('uri', ''))
    parts = uri.netloc.split('@')
    bucket, namespace = parts if len(parts) == 2 else ('', '')
    object_name = uri.path.lstrip('/')
    start = timestamp(read.get('utc'))
    end = timestamp(read.get('completed_utc'))
    event_time = timestamp(event.get('startTime'))
    captured = timestamp(snapshot.get('captured_utc'))
    reviewed = timestamp(review.get('reviewed_utc'))
    checks['same_fixture_read'] = bool(bucket and namespace and object_name and
        read.get('object_uri') == cfg.get('uri') and report['bucket_read'] == 'PASS' and
        read.get('checks', {}).get('functional', {}).get('detail', {}).get('configured_endpoint') == cfg.get('endpoint'))
    checks['request_time_matches_read'] = bool(start is not None and end is not None and
        event_time is not None and start <= end and start - 2 <= event_time <= end + 2)
    # This is an explicit freshness limit for this diagnostic, not an OCI SLA.
    checks['recent_configuration_and_review'] = bool(start is not None and end is not None and
        captured is not None and reviewed is not None and
        start - 900 <= captured <= end + 900 and start - 900 <= reviewed <= end + 900)
    checks['matching_object_get'] = bool(event.get('bucketName') == bucket and
        event.get('namespace', event.get('namespaceName')) == namespace and
        event.get('objectName') == object_name and event.get('requestAction') == 'GET' and
        str(event.get('statusCode')) in ('200', '206') and event.get('isPar') is False and
        event.get('apiType') == 'native' and event.get('opcRequestId'))
    checks['reviewed_principal'] = bool(review.get('principal_id') and
        event.get('principalId') == review['principal_id'])
    checks['reviewed_source_restriction'] = bool(review.get('evidence_ref') and
        review.get('no_bypass_grants') is True and review.get('object_uri') == cfg.get('uri') and
        source.get('name') and review.get('network_source_name') == source['name'] and
        source.get('public_source_list') == [])
    # NetworkSources.services is reserved and has no effect (OCI SDK/API).
    checks['exclusive_customer_vcn'] = bool(source.get('virtual_source_list') and all(
        v.get('vcn_id') == subnet.get('vcn_id') and v.get('ip_ranges')
        for v in source.get('virtual_source_list', [])))
    ip = event.get('clientIpAddress')
    checks['customer_source_address'] = bool(in_cidr(ip, subnet.get('cidr_block')) and any(
        v.get('vcn_id') == subnet.get('vcn_id') and any(in_cidr(ip, cidr) for cidr in v.get('ip_ranges', []))
        for v in source.get('virtual_source_list', [])))
    if all(checks.values()):
        report['customer_vcn_source'] = 'CONFIRMED_BY_REVIEWED_SOURCE_CONTROL'
        report['confirmation_basis'] = ('A matching successful object GET has the reviewed principal and '
            'customer source address. The administrator reviewed all applicable grants and confirmed '
            'that this reader can read this object only through the named VCN Network Source. '
            'This is source-control evidence, not a packet trace.')
    report['request_evidence'] = {k: event[k] for k in
        ('opcRequestId', 'principalId', 'clientIpAddress', 'startTime') if k in event}
    report['missing_checks'] = [k for k, passed in checks.items() if not passed]
    return report


def print_v02_route_report(report):
    print('DNS answers seen by the driver: ' + (', '.join(report['dns_addresses']) or 'No recorded lookup'))
    print(report['dns_meaning'])
    print('\nConfigured customer path: ' + report['configuration'])
    print(report['configured_path'])
    print('Configuration captured: ' + str(report.get('snapshot_utc') or 'Not supplied'))
    print('\nBucket read: ' + report['bucket_read'])
    print('Customer VCN source: ' + report['customer_vcn_source'])
    if report.get('request_evidence'):
        print('Object Storage observed: ' + str(report['request_evidence']))
    if report.get('confirmation_basis'):
        print(report['confirmation_basis'])
    if report['missing_checks']:
        checks = report['checks']
        print('Next actions:')
        if report['configuration'] != 'CONFIGURED_IN_SNAPSHOT':
            print('- Supply or correct the administrator snapshot: workspace destination, subnet and service-gateway route must match.')
        if not checks['same_fixture_read']:
            print('- Rerun the updated bucket cell for this phase; it must return the expected rows and record the request interval.')
        if not all(checks[k] for k in ('matching_object_get', 'request_time_matches_read', 'reviewed_principal', 'customer_source_address')):
            print('- Supply the matching Object Storage GET event: same object, read interval, reviewed principal and customer source address.')
        if not all(checks[k] for k in ('reviewed_source_restriction', 'exclusive_customer_vcn')):
            print('- Complete the IAM review: the logged reader must have no grant bypassing the customer-VCN-only Network Source.')
        if not checks['recent_configuration_and_review']:
            print('- Collect configuration and IAM review records within 15 minutes of the read; keep settings unchanged.')
    print(report['scope'])
