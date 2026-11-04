# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Bounded public-endpoint ADB check embedded in the standalone V02 notebook."""
import datetime
import re
import socket


def run_adb_check(cfg, spark=None, runtime=None, live=False, phase='before'):
    """Query an existing connection; never change ACLs, IAM, credentials or data."""
    result = {'case': 'V02', 'target': 'adb', 'phase': phase,
              'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'checks': {}}
    checks = result['checks']
    if not live:
        checks['execution'] = {'status': 'NOT_RUN', 'reason': 'Live execution disabled'}
        return result
    missing = [k for k in ('workspace_label', 'compute_label', 'hostname', 'expected_service',
                           'workspace_region', 'region') if not cfg.get(k)]
    reason = ('Complete ADB configuration: ' + ', '.join(missing)) if missing else ''
    if cfg.get('region') != cfg.get('workspace_region'):
        reason = 'The database and workspace must be in the same region'
    if cfg.get('public_endpoint_confirmed') is not True:
        reason = 'Verify the catalog or wallet descriptor uses the intended public endpoint'
    if spark is None:
        reason = 'Existing Spark session required'
    if reason:
        checks['execution'] = {'status': 'NEEDS_SETUP', 'reason': reason}
        return result
    catalog = cfg.get('catalog_id')
    if not catalog and (not all(cfg.get(k) for k in ('wallet_zip', 'tns', 'user')) or
                        not (runtime or {}).get('adb_password')):
        checks['execution'] = {'status': 'NEEDS_SETUP', 'reason':
            'Use an existing catalog, or wallet ZIP/TNS/user and a private runtime password'}
        return result
    try:
        addresses = sorted({a[4][0] for a in socket.getaddrinfo(
            cfg['hostname'], cfg.get('port', 1522), type=socket.SOCK_STREAM)})
        checks['dns'] = {'status': 'PASS', 'addresses': addresses}
    except Exception as exc:
        checks['dns'] = {'status': 'FAIL', 'error_type': type(exc).__name__}
    query = ("SELECT sys_context('USERENV','SERVICE_NAME') AS SERVICE_NAME, "
             "sys_context('USERENV','SESSION_USER') AS SESSION_USER, "
             "sys_context('USERENV','SESSIONID') AS SESSION_ID, "
             "sys_context('USERENV','IP_ADDRESS') AS CLIENT_IP FROM dual")
    try:
        reader = (spark.read.format('aidataplatform').option('type', 'ORACLE_ALH')
                  .option('schema', 'SYS').option('table', 'DUAL')
                  .option('pushdown.sql', query).option('login.timeout.seconds', '20'))
        if catalog:
            reader = reader.option('catalog.id', catalog)
        else:
            reader = (reader.option('wallet.path', cfg['wallet_zip']).option('tns', cfg['tns'])
                      .option('user.name', cfg['user'])
                      .option('password', runtime['adb_password']))
        rows = reader.load().limit(2).collect()
        assert len(rows) == 1, 'Expected one database identity row'
        row = {k.upper(): v for k, v in rows[0].asDict().items()}
        matches = str(row.get('SERVICE_NAME', '')).casefold() == cfg['expected_service'].casefold()
        checks['functional'] = {'status': 'PASS' if matches else 'FAIL',
            'service_matches': matches, 'service_name': row.get('SERVICE_NAME'),
            'database_user': row.get('SESSION_USER'), 'session_id': row.get('SESSION_ID'),
            'client_ip_seen_by_database': row.get('CLIENT_IP')}
    except Exception as exc:
        # Driver exceptions can include passwords and connection strings.
        checks['functional'] = {'status': 'FAIL', 'error_type': type(exc).__name__,
            'error_codes': sorted(set(re.findall(r'\bORA-\d{5}\b', str(exc))))}
    checks['source_path'] = {'status': 'NOT_VERIFIED', 'reason':
        'Run the public-ACL allow/deny/restore test to verify the source context.'}
    result['authorization_control'] = 'ADB VCN ACL plus database login; not an IAM Network Source condition'
    result['transport_claim'] = 'An authenticated query does not independently establish mTLS'
    return result
