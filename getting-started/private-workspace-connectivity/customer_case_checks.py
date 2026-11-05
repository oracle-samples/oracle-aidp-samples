# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Case-specific presentation and read-only diagnostics, embedded in notebooks."""


def customer_summary(result):
    checks = result.get('checks', {})
    states = [v.get('status') for v in checks.values()]
    if 'FAIL' in states:
        return 'FAIL'
    if 'SETUP_REQUIRED' in states:
        return 'SETUP_REQUIRED'
    if checks.get('execution', {}).get('status') == 'NOT_RUN':
        return 'NOT_RUN'
    if checks.get('functional', {}).get('status') == 'PASS':
        return 'READ_PASSED'
    return 'REVIEW_REQUIRED'


def customer_labels(result):
    """Keep missing evidence separate from a failed connection."""
    for name, check in result.get('checks', {}).items():
        if check.get('status') == 'BLOCKED':
            check['status'] = 'NOT_ASSESSED' if name in ('source_path', 'mtls') else 'SETUP_REQUIRED'
    result['acceptance'] = customer_summary(result)
    return result


def run_customer_case(case, cfg, spark=None, runtime=None, live=False,
                      phase='before', fixture_role='primary'):
    """Use the existing connector checks with explicit per-case prerequisites."""
    if type(live) is not bool:
        raise ValueError('RUN_LIVE must be True or False, without quotes')
    allowed = {'before', 'after_add', 'restored'}
    if case == 'V04':
        allowed |= {'after_edit', 'after_delete'}
    if fixture_role == 'local_control':
        allowed = {p + '_local_control' for p in allowed}
    if live and phase not in allowed:
        raise ValueError('Select a documented phase in Choose phase')
    problem = None
    if live:
        if case == 'V01' and (cfg.get('kind') != 'csv' or not cfg.get('uri', '').startswith('s3a://')):
            problem = 'Use kind=csv and an s3a:// bucket/object URI'
        elif case == 'V03' and (cfg.get('kind') != 'csv' or not cfg.get('uri', '').startswith('oci://')):
            problem = 'Use kind=csv and an oci://bucket@namespace/object URI'
        elif case == 'V04' and (cfg.get('kind') != 'adb' or cfg.get('private_endpoint_confirmed') is not True):
            problem = 'Verify the catalog/wallet private HOST and PORT, then set private_endpoint_confirmed=True'
    if problem:
        return {'case': case, 'phase': phase, 'utc': utc(), 'end_utc': utc(),
                'checks': {'execution': {'status': 'SETUP_REQUIRED', 'reason': problem}},
                'acceptance': 'SETUP_REQUIRED'}
    result = run_case(case, cfg, spark=spark, runtime=runtime, live=live,
                      phase=phase, fixture_role=fixture_role)
    result['end_utc'] = utc()
    result['fixture_role'] = fixture_role
    if 'source_path' in result['checks']:
        result['checks']['source_path']['reason'] = (
            'Correlate this UTC interval with the customer-side VNIC and destination evidence in Logging setup')
    return customer_labels(result)


def diagnose_customer_target(cfg, spark=None, live=False, case=None):
    """Driver-side DNS/TCP checks; no IAM, database, routing or DNS mutations."""
    if type(live) is not bool:
        raise ValueError('RUN_LIVE must be True or False, without quotes')
    report = {'utc': utc(), 'scope': 'current notebook driver', 'checks': {}}
    if not live:
        report['checks']['execution'] = {'status': 'NOT_RUN', 'reason': 'RUN_LIVE=False'}
        return report
    host = cfg.get('hostname') or endpoint_host(cfg.get('endpoint', ''))
    port = cfg.get('port', 443)
    if not host or '://' in host or '/' in host or type(port) is not int or not 1 <= port <= 65535:
        report['checks']['configuration'] = {'status': 'SETUP_REQUIRED', 'reason': 'Supply an exact HOST and integer PORT'}
        return report
    report.update(hostname=host, port=port)
    checks = report['checks']
    try:
        checks['python_dns'] = {'status': 'PASS', 'addresses': sorted({
            a[4][0] for a in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})}
    except Exception as exc:
        checks['python_dns'] = {'status': 'FAIL', 'error_type': type(exc).__name__}
    if spark is None:
        checks['jvm'] = {'status': 'SETUP_REQUIRED', 'reason': 'Attach the intended compute'}
    else:
        try:
            addresses = spark._jvm.java.net.InetAddress.getAllByName(host)
            checks['jvm_dns'] = {'status': 'PASS', 'addresses': sorted({a.getHostAddress() for a in addresses})}
        except Exception as exc:
            checks['jvm_dns'] = {'status': 'FAIL', 'error_type': type(exc).__name__}
        connection = None
        try:
            connection = spark._jvm.java.net.Socket()
            connection.connect(spark._jvm.java.net.InetSocketAddress(host, port), 5000)
            checks['jvm_tcp'] = {'status': 'PASS', 'detail': 'TCP connected; TLS and service authorization not tested'}
        except Exception as exc:
            checks['jvm_tcp'] = {'status': 'FAIL', 'error_type': type(exc).__name__}
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    checks['tcp_cleanup'] = {'status': 'REVIEW_REQUIRED', 'reason': 'Socket close failed'}
        if case == 'V01':
            try:
                loader = spark._jsc.hadoopConfiguration().getClassLoader()
                loader.loadClass('org.apache.hadoop.fs.s3a.S3AFileSystem')
                checks['s3a_class'] = {'status': 'PASS', 'hadoop_version': spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
            except Exception as exc:
                checks['s3a_class'] = {'status': 'SETUP_REQUIRED', 'error_type': type(exc).__name__,
                                     'reason': 'Administrator must provide compatible Hadoop S3A and AWS libraries'}
    if all(checks.get(k, {}).get('status') == 'PASS' for k in ('python_dns', 'jvm_dns')):
        report['driver_answer_sets_match'] = checks['python_dns']['addresses'] == checks['jvm_dns']['addresses']
    report['end_utc'] = utc()
    report['interpretation'] = 'Driver diagnostics only. Confirm executor behavior and the actual client descriptor separately.'
    return report
