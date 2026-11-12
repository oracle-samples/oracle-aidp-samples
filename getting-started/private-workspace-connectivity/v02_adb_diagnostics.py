# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Read-only ADB troubleshooting; no DNS, ACL, IAM or credential changes."""
import datetime
import ipaddress
import re
import socket


def v02_adb_resolve(host, port, spark=None):
    records = {}
    for scope in ('python', 'jvm'):
        if scope == 'jvm' and spark is None:
            records[scope] = {'status': 'NOT_RUN', 'reason': 'Attach Spark compute to check JVM resolution'}
            continue
        try:
            addresses = (sorted({a[4][0] for a in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
                if scope == 'python' else sorted({a.getHostAddress() for a in
                    spark.sparkContext._jvm.java.net.InetAddress.getAllByName(host)}))
            records[scope] = {'status': 'RESOLVED' if addresses else 'NO_ADDRESSES', 'addresses': addresses}
        except Exception as exc:
            records[scope] = {'status': 'FAILED', 'error_type': type(exc).__name__}
    return records


def v02_adb_tcp(addresses, port, spark=None):
    """Bounded TCP handshakes only. No HTTP or TLS payload is sent to the listener."""
    rows = []
    for address in addresses[:4]:
        connection = None
        try:
            if spark is None:
                connection = socket.create_connection((address, port), timeout=3)
            else:
                jvm = spark.sparkContext._jvm
                connection = jvm.java.net.Socket()
                connection.connect(jvm.java.net.InetSocketAddress(address, port), 3000)
            rows.append({'address': address, 'status': 'REACHABLE'})
        except Exception as exc:
            rows.append({'address': address, 'status': 'NOT_REACHED', 'error_type': type(exc).__name__})
        finally:
            if connection is not None:
                connection.close()
    return {'scope': 'Spark JVM driver' if spark is not None else 'Python kernel',
            'status': ('REACHABLE' if rows and all(r['status'] == 'REACHABLE' for r in rows) else
                       'PARTIAL' if any(r['status'] == 'REACHABLE' for r in rows) else
                       'NOT_REACHED' if rows else 'NOT_RUN'),
            'attempts': rows, 'addresses_not_tested': max(0, len(addresses) - 4)}


def v02_adb_diagnose(cfg, spark=None, runtime=None, live=False, query=False,
                     workspace_snapshot=None, adb_reader=None):
    report = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'assessment': 'NOT_RUN', 'next_actions': [],
              'scope': 'Driver diagnostics; DNS and TCP do not establish the customer VCN route or mTLS'}
    if not live:
        report['next_actions'] = ['Set RUN_LIVE=True after selecting the target hostname and port.']
        return report
    host, port = cfg.get('hostname', ''), cfg.get('port')
    valid_host = isinstance(host, str) and len(host) <= 253 and len(host.split('.')) >= 2 and all(
        re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?', label) for label in host.split('.'))
    try:
        ipaddress.ip_address(host)
        valid_host = False
    except ValueError:
        pass
    if (not valid_host or host == cfg.get('expected_service') or not isinstance(port, int)
            or isinstance(port, bool) or not 1 <= port <= 65535):
        report.update(assessment='CHECK_CONNECTION_FIELDS', next_actions=[
            'Copy HOST and PORT from the selected public connection descriptor. Use a plain FQDN, not a URL, IP, TNS alias, wildcard or SERVICE_NAME.'])
        return report
    report['target'] = {'hostname': host, 'port': port, 'intended_endpoint': 'PUBLIC',
        'descriptor_review': 'CONFIRMED_BY_USER' if cfg.get('public_endpoint_confirmed') is True else 'REQUIRED'}
    report['runtime'] = {'workspace_label': cfg.get('workspace_label'), 'compute_label': cfg.get('compute_label')}
    if spark is not None:
        try:
            report['runtime']['spark_application_id'] = spark.sparkContext.applicationId
        except AttributeError:
            pass
    if cfg.get('region') != cfg.get('workspace_region'):
        report.update(assessment='CHECK_REGIONS', next_actions=['V02 requires the ADB and workspace in the same region.'])
        return report
    snapshot = workspace_snapshot or {}
    recorded_names = snapshot.get('workspace', {}).get('dns_names')
    report['saved_dns_entry'] = {'status': 'NOT_CHECKED' if recorded_names is None else
        'EXACT_NAME_LISTED' if host in recorded_names else 'EXACT_NAME_NOT_LISTED',
        'captured_utc': snapshot.get('captured_utc'),
        'workspace_label': snapshot.get('workspace', {}).get('display_name'),
        'scope': 'Optional saved snapshot; not a live workspace read or proof of effective DNS forwarding'}
    report['dns'] = v02_adb_resolve(host, port, spark)
    py = report['dns']['python'].get('addresses', [])
    jvm = report['dns']['jvm'].get('addresses', [])
    report['dns_comparison'] = ('SAME' if py and jvm and py == jvm else
                                'DIFFERENT' if py and jvm else 'NOT_COMPARABLE')
    # JDBC uses the JVM. Do not substitute Python answers when JVM resolution fails.
    report['tcp'] = v02_adb_tcp(jvm if spark is not None else py, port, spark)
    report['sql'] = {'status': 'NOT_RUN'}
    if query:
        if adb_reader is None:
            raise ValueError('Initialize V02 before running the diagnostic query')
        result = adb_reader(cfg, spark=spark, runtime=runtime, live=True, phase='diagnostic')
        report['sql'] = result.get('checks', {}).get('functional') or result.get('checks', {}).get('execution', {'status': 'NOT_RUN'})
    sql, tcp = report['sql'], report['tcp']
    codes = set(sql.get('error_codes', []))
    if sql.get('status') == 'PASS':
        report.update(assessment='SQL_ACCEPTED_SOURCE_NOT_VERIFIED', next_actions=[
            'The selected connection returned the expected service. Use the separate ACL allow/deny/restore test to verify its source restriction.'])
    elif codes & {'ORA-12529', 'ORA-12506'}:
        report.update(assessment='CONNECTION_FILTER_REJECTED', next_actions=[
            'Verify the catalog or wallet uses this public HOST and PORT; this diagnostic cannot inspect a managed catalog wallet.',
            'Check the ADB VCN ACL against the customer VCN and actual source subnet. Keep the existing restriction while investigating.',
            'Have the AIDP administrator verify effective forwarding of this exact hostname to the workspace PE DNS proxy and the customer Service Gateway path. An accepted DNS update or an IP answer alone is insufficient.'])
    elif codes & {'ORA-01017', 'ORA-28000', 'ORA-28001'}:
        report.update(assessment='CHECK_DATABASE_LOGIN', next_actions=[
            'Have the database owner verify the selected account, password and account state using the approved credential store.'])
    elif codes & {'ORA-29024', 'ORA-28759', 'ORA-28860'}:
        report.update(assessment='CHECK_TLS_OR_WALLET', next_actions=[
            'Check the selected TLS/mTLS descriptor, wallet availability and certificate validation. Do not disable certificate checks.'])
    elif codes & {'ORA-12514', 'ORA-12505'}:
        report.update(assessment='CHECK_DATABASE_SERVICE', next_actions=[
            'Compare SERVICE_NAME and the selected TNS alias with current Database connection information.'])
    elif sql.get('status') == 'FAIL':
        report.update(assessment='SQL_FAILED', next_actions=[
            'Review the redacted Oracle codes and the detailed error privately. A timeout or generic connection error does not establish an ACL denial.'])
    elif sql.get('status') == 'NEEDS_SETUP':
        report.update(assessment='VERIFY_SQL_SETUP', next_actions=[
            sql.get('reason', 'Complete the public catalog/wallet connection setup before retrying SQL.')])
    elif not (jvm if spark is not None else py):
        report.update(assessment='CHECK_DNS_RESOLUTION', next_actions=[
            'Verify HOST spelling and the resolver used by this compute. Have the AIDP administrator inspect the effective workspace DNS forwarding.'])
    elif report['dns_comparison'] == 'DIFFERENT':
        report.update(assessment='COMPARE_PYTHON_AND_JVM_DNS', next_actions=[
            'Python and JVM returned different sets. DNS rotation, caching or resolver differences may explain this; compare repeated observations before restarting compute. JDBC uses the JVM.'])
    elif tcp['status'] != 'REACHABLE':
        report.update(assessment='CHECK_LISTENER_PATH', next_actions=[
            'Verify the descriptor port, customer subnet egress and Service Gateway route, and the workspace private connection. TCP failure alone does not identify a specific firewall or ACL.'])
    elif cfg.get('public_endpoint_confirmed') is not True:
        report.update(assessment='VERIFY_PUBLIC_DESCRIPTOR', next_actions=[
            'DNS and TCP succeeded. Verify the effective public descriptor before setting public_endpoint_confirmed=True and enabling the query.'])
    else:
        report.update(assessment='READY_FOR_SQL_CHECK', next_actions=[
            'Enable RUN_ADB_DIAGNOSTIC_QUERY to test the existing catalog or wallet. TCP success does not verify SQL login or the VCN route.'])
    return report


def print_v02_adb_diagnostic(report):
    target = report.get('target', {})
    if target:
        print('Target: {}:{} — public-endpoint test; descriptor review: {}'.format(
            target['hostname'], target['port'], target['descriptor_review']))
    for label, check in [('Saved DNS entry', report.get('saved_dns_entry', {})),
                          ('Python DNS', report.get('dns', {}).get('python', {})),
                          ('JVM DNS', report.get('dns', {}).get('jvm', {})),
                          ('TCP listener', report.get('tcp', {})), ('SQL query', report.get('sql', {}))]:
        if check:
            detail = ', '.join(check.get('addresses', []) or check.get('error_codes', []))
            if label == 'Saved DNS entry':
                detail = 'snapshot: ' + str(check.get('captured_utc') or 'not supplied')
            print('{:<16} {:<24} {}'.format(label, check['status'], detail))
    print('Assessment: ' + report['assessment'])
    for index, action in enumerate(report['next_actions'], 1):
        print('{}. {}'.format(index, action))
    print(report['scope'])
