# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Reversible V02 permission tests. Operator credentials stay outside results."""
import base64
import datetime
import hashlib
import ipaddress
import json
import os
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit


def v02_normalize(result):
    for name, check in result.get('checks', {}).items():
        if name == 'source_path' and check.get('status') == 'BLOCKED':
            check.update(status='NOT_VERIFIED', reason='Run the allow/deny/restore source-control test below.')
        elif check.get('status') == 'BLOCKED':
            check['status'] = 'NEEDS_SETUP'
    return result


def v02_summary(result):
    checks = v02_normalize(result).get('checks', {})
    if checks.get('source_path', {}).get('status') == 'PASS':
        return 'SOURCE_CONTEXT_VERIFIED'
    if checks.get('functional', {}).get('status') == 'PASS':
        return 'ACCESS_SUCCEEDED_SOURCE_NOT_VERIFIED'
    states = [c.get('status') for c in checks.values()]
    return 'FAILED' if 'FAIL' in states else 'NEEDS_SETUP' if 'NEEDS_SETUP' in states else 'NOT_RUN'


def v02_admin_clients(config_file, profile):
    """Use a separately provisioned private operator profile, never Spark's reader identity."""
    if not config_file or not profile:
        raise ValueError('Set an approved private OCI config file and operator profile first')
    import oci
    cfg = oci.config.from_file(file_location=config_file, profile_name=profile)
    if cfg.get('security_token_file'):
        token = Path(cfg['security_token_file']).expanduser().read_text().strip()
        part = token.split('.')[1]
        expires = json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))['exp']
        if expires - time.time() < 1200:
            raise ValueError('Renew the operator session first: at least 20 minutes are required for test and restoration')
        auth = oci.auth.signers.SecurityTokenSigner(token,
            oci.signer.load_private_key_from_file(cfg['key_file'], cfg.get('pass_phrase')))
    else:
        auth = oci.signer.Signer(cfg['tenancy'], cfg['user'], cfg['fingerprint'],
                                 cfg.get('key_file'), pass_phrase=cfg.get('pass_phrase'))
    options = dict(signer=auth, timeout=(10, 30), retry_strategy=oci.retry.NoneRetryStrategy())
    return oci.identity.IdentityClient(cfg, **options), oci.database.DatabaseClient(cfg, **options)


def v02_error_codes(exc):
    # Never export the exception itself: driver messages may contain credentials.
    message = str(exc)
    codes = sorted(set(re.findall(r'\bORA-\d{5}\b', message)))
    codes += [name for name in ('NotAuthorizedOrNotFound', 'NotAuthorized', 'AccessDenied')
              if re.search(r'\b' + name + r'\b', message)]
    return codes


def v02_bucket_probe(cfg, spark, fixture_reader):
    record = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        record['runtime_id'] = spark.sparkContext.applicationId
        record['target_id'] = cfg['uri']
        record['connection_id'] = cfg['endpoint']
        # A fresh stream guarantees an object GET rather than a cached Spark result.
        fresh = v02_fresh_object_get(cfg, spark)
        record['detail'] = {'fresh_driver_get': fresh, 'spark_fixture': fixture_reader(spark, cfg)}
        record['scope'] = 'Fresh object GET from the driver; Spark fixture checked separately'
        record['outcome'] = 'ALLOWED'
    except Exception as exc:
        record['error_codes'] = v02_error_codes(exc)
        record['error_type'] = type(exc).__name__
        record['outcome'] = ('DENIED' if any(c in record['error_codes'] for c in
            ('NotAuthorizedOrNotFound', 'NotAuthorized', 'AccessDenied')) else 'ERROR')
    record['completed_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return record


def v02_fresh_object_get(cfg, spark):
    size = cfg.get('fixture_size_bytes')
    if not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= 65536:
        raise ValueError('Verify the small fixture size before reading')
    uri = cfg['uri']
    if not uri.startswith('oci://') or '?' in uri or '#' in uri:
        raise ValueError('Use the unsigned OCI fixture URI')
    endpoint = urlsplit(cfg['endpoint'])
    if endpoint.scheme != 'https' or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
        raise ValueError('Use the unsigned HTTPS Object Storage endpoint')
    bucket, namespace = uri.split('://', 1)[1].split('/', 1)[0].split('@')
    jvm = spark.sparkContext._jvm
    conf = jvm.org.apache.hadoop.conf.Configuration(spark.sparkContext._jsc.hadoopConfiguration())
    conf.set('fs.oci.client.hostname.' + bucket + '.' + namespace, cfg['endpoint'])
    filesystem = jvm.org.apache.hadoop.fs.FileSystem.newInstance(jvm.java.net.URI(uri), conf)
    try:
        stream = filesystem.open(jvm.org.apache.hadoop.fs.Path(uri))
        try:
            body = bytes(stream.readNBytes(size + 1))
            if len(body) != size:
                raise ValueError('Fixture size changed')
            return {'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
        finally:
            stream.close()
    finally:
        filesystem.close()


def v02_adb_probe(cfg, spark, adb_reader, runtime=None):
    result = adb_reader(cfg, spark=spark, runtime=runtime, live=True, phase='source_control')
    functional = result.get('checks', {}).get('functional', {})
    codes = functional.get('error_codes', [])
    outcome = ('ALLOWED' if functional.get('status') == 'PASS' else
               'DENIED' if any(c in codes for c in ('ORA-12529', 'ORA-12506')) else 'ERROR')
    return {'utc': result['utc'], 'outcome': outcome, 'error_codes': codes,
            'runtime_id': spark.sparkContext.applicationId,
            'target_id': cfg['expected_service'],
            'connection_id': cfg.get('catalog_id') or cfg.get('tns'),
            'session_id': functional.get('session_id'),
            'client_ip_seen_by_database': functional.get('client_ip_seen_by_database'),
            'detail': functional, 'setup': result.get('checks', {}).get('execution')}


class V02BucketControl:
    kind = 'bucket'

    def __init__(self, get_policy, put_policy, get_source, policy_id, grant, bucket, source_name, vcn_id, source_cidr):
        self.get_policy, self.put_policy, self.get_source = get_policy, put_policy, get_source
        self.resource_id, self.allow = policy_id, grant
        self.bucket, self.source_name, self.vcn_id, self.source_cidr = bucket, source_name, vcn_id, source_cidr
        pair = [r"target\.bucket\.name\s*=\s*'" + re.escape(bucket) + "'",
                r"request\.networkSource\.name\s*=\s*'" + re.escape(source_name) + "'"]
        parts = re.split(r'\bwhere\b', grant, maxsplit=1, flags=re.I)
        if len(parts) != 2 or not any(re.fullmatch(r'\s*all\s*\{\s*' + a + r'\s*,\s*' + b + r'\s*\}\s*',
            parts[1], re.I) for a, b in (pair, pair[::-1])):
            raise ValueError('Use one grant restricted only to the exact test bucket and named Network Source')
        head, count = re.subn(r'\bto\s+read\s+objects\b', 'to inspect objects', parts[0], flags=re.I)
        if count != 1 or not re.match(r'\s*allow\b', parts[0], re.I):
            raise ValueError('Expected an Allow ... to read objects grant')
        self.deny = head + 'where' + parts[1]

    def prepare(self):
        p, source = self.get_policy(), self.get_source()
        if p['statements'].count(self.allow) != 1 or self.deny in p['statements']:
            raise ValueError('The exact allowed grant must occur once in the selected policy')
        virtual = source.get('virtual_source_list', [])
        if (source.get('name') != self.source_name or source.get('public_source_list') != [] or
            len(virtual) != 1 or virtual[0].get('vcn_id') != self.vcn_id or
            virtual[0].get('ip_ranges') != [self.source_cidr]):
            raise ValueError('Network Source must allow only the selected customer VCN and source subnet')
        return {'kind': self.kind, 'resource_id': self.resource_id, 'before': p['statements'],
                'allow': self.allow, 'deny': self.deny, 'source_before': source}

    def deny_access(self, saved):
        p = self.get_policy()
        if p['statements'] != saved['before']:
            raise RuntimeError('Policy changed before the test; no write performed')
        statements = [self.deny if s == self.allow else s for s in p['statements']]
        self.put_policy(statements, p['etag'])
        if self.get_policy()['statements'] != statements:
            raise RuntimeError('Denied grant readback did not match')

    def restore(self, saved):
        p = self.get_policy()
        if p['statements'].count(self.allow) == 1 and self.deny not in p['statements']:
            return
        if p['statements'].count(self.deny) != 1 or self.allow in p['statements']:
            raise RuntimeError('Concurrent grant change: use the journal for manual restoration')
        # Preserve concurrent edits to unrelated statements.
        self.put_policy([self.allow if s == self.deny else s for s in p['statements']], p['etag'])
        p = self.get_policy()
        if p['statements'].count(self.allow) != 1 or self.deny in p['statements']:
            raise RuntimeError('Restoration readback did not match')

    def unchanged(self, saved):
        return self.get_policy()['statements'] == saved['before'] and self.get_source() == saved['source_before']


class V02AdbControl:
    kind = 'adb'

    def __init__(self, get_database, put_acl, database_id, vcn_id, source_cidr, denied_cidr):
        self.get_database, self.put_acl = get_database, put_acl
        self.resource_id, self.vcn_id = database_id, vcn_id
        self.source_cidr, self.denied_cidr = source_cidr, denied_cidr
        if ipaddress.ip_network(source_cidr).overlaps(ipaddress.ip_network(denied_cidr)):
            raise ValueError('The denied-phase CIDR must not overlap the workspace source subnet')
        self.deny = vcn_id + ';' + denied_cidr

    def ready(self):
        deadline = time.monotonic() + 180
        while True:
            d = self.get_database()
            if d['state'] == 'AVAILABLE':
                return d
            if time.monotonic() >= deadline:
                raise RuntimeError('Database update did not finish; restore from the journal when AVAILABLE')
            time.sleep(5)

    def prepare(self):
        d = self.ready()
        # Keep this sample intentionally strict: no other ACL may bypass the negative test.
        allowed = d['acl']
        if allowed not in ([self.vcn_id], [self.vcn_id + ';' + self.source_cidr]):
            raise ValueError('Use a database whose public ACL contains only the intended customer VCN/source subnet')
        return {'kind': self.kind, 'resource_id': self.resource_id, 'before': allowed,
                'deny': [self.deny], 'network_before': d['network']}

    def deny_access(self, saved):
        d = self.ready()
        if d['acl'] != saved['before'] or d['network'] != saved['network_before']:
            raise RuntimeError('Database network configuration changed; no write performed')
        self.put_acl(saved['deny'], d['etag'])
        if self.ready()['acl'] != saved['deny']:
            raise RuntimeError('Denied ACL readback did not match')

    def restore(self, saved):
        d = self.ready()
        if d['acl'] == saved['before']:
            return
        if d['acl'] != saved['deny']:
            raise RuntimeError('Concurrent ACL edit: use the journal for manual restoration')
        self.put_acl(saved['before'], d['etag'])
        if self.ready()['acl'] != saved['before']:
            raise RuntimeError('Restored ACL readback did not match')

    def unchanged(self, saved):
        d = self.ready()
        return d['acl'] == saved['before'] and d['network'] == saved['network_before']


def v02_bucket_control(client, settings):
    import oci
    def get_policy():
        r = client.get_policy(settings['policy_id'])
        return {'statements': r.data.statements, 'etag': r.headers['etag']}
    def put_policy(statements, etag):
        client.update_policy(settings['policy_id'], oci.identity.models.UpdatePolicyDetails(statements=statements), if_match=etag)
    def get_source():
        d = oci.util.to_dict(client.get_network_source(settings['network_source_id']).data)
        return {k: d.get(k) for k in ('name', 'public_source_list', 'virtual_source_list')}
    return V02BucketControl(get_policy, put_policy, get_source, settings['policy_id'], settings['allow_statement'],
        settings['bucket_name'], settings['network_source_name'], settings['vcn_id'], settings['source_subnet_cidr'])


def v02_adb_control(client, settings):
    import oci
    def get_database():
        r = client.get_autonomous_database(settings['database_id'])
        d = r.data
        return {'acl': d.whitelisted_ips, 'state': d.lifecycle_state, 'etag': r.headers['etag'],
                'network': {k: getattr(d, k, None) for k in ('subnet_id', 'nsg_ids', 'private_endpoint',
                    'public_endpoint', 'is_access_control_enabled', 'is_mtls_connection_required')}}
    def put_acl(acl, etag):
        client.update_autonomous_database(settings['database_id'],
            oci.database.models.UpdateAutonomousDatabaseDetails(whitelisted_ips=acl), if_match=etag)
    return V02AdbControl(get_database, put_acl, settings['database_id'], settings['vcn_id'],
        settings['source_subnet_cidr'], settings['denied_subnet_cidr'])


def run_v02_control_cycle(control, probe, journal_path, apply=False, max_wait_seconds=120, interval_seconds=10):
    """Require ALLOWED / explicit DENIED / ALLOWED and exact restoration.

    A killed kernel cannot run finally: retain the recovery journal outside the
    notebook session and keep operator credentials valid throughout the test.
    """
    if not apply:
        return {'status': 'NOT_RUN', 'reason': 'Permission switching is disabled'}
    if Path(journal_path).exists():
        previous = json.loads(Path(journal_path).read_text())
        if previous.get('result', {}).get('restoration') in ('REQUIRED', 'MANUAL_ACTION_REQUIRED'):
            raise ValueError('Restore the previous cycle from this journal before starting another test')
    result = {'target': control.kind, 'status': 'INCONCLUSIVE', 'observations': [], 'restoration': 'NOT_NEEDED'}
    saved = control.prepare()
    def journal():
        body = {'saved_control': saved, 'result': result}
        v02_write_journal(journal_path, body)
    def observe(phase, expected, wait=False):
        deadline = time.monotonic() + max_wait_seconds
        while True:
            row = probe()
            row.update(phase=phase, expected=expected)
            result['observations'].append(row)
            journal()
            if row.get('outcome') == expected or not wait or time.monotonic() >= deadline:
                return row
            time.sleep(interval_seconds)
    journal()
    before = observe('allow', 'ALLOWED')
    if before.get('outcome') != 'ALLOWED':
        result.update(status='NEEDS_SETUP', reason='The baseline read must succeed before changing permissions')
        journal()
        return result
    denied = restored = {}
    try:
        # Save recovery instructions before the first mutation, including an uncertain HTTP outcome.
        result['restoration'] = 'REQUIRED'
        journal()
        control.deny_access(saved)
        denied = observe('deny', 'DENIED', wait=True)
    except Exception as exc:
        result['test_error_type'] = type(exc).__name__
    finally:
        try:
            control.restore(saved)
            result['restoration'] = 'RESTORED'
        except Exception as exc:
            result.update(status='RESTORE_REQUIRED', restoration='MANUAL_ACTION_REQUIRED', restore_error_type=type(exc).__name__)
        journal()
    if result['restoration'] != 'RESTORED':
        return result
    restored = observe('restored', 'ALLOWED', wait=True)
    stable = all(before.get(k) and before.get(k) == denied.get(k) == restored.get(k)
                 for k in ('runtime_id', 'target_id', 'connection_id'))
    fresh = control.kind != 'adb' or bool(before.get('session_id') and restored.get('session_id')
                                         and before['session_id'] != restored['session_id'])
    result['same_reader_and_target'] = stable
    result['fresh_database_sessions'] = fresh if control.kind == 'adb' else None
    result['configuration_restored'] = control.unchanged(saved)
    if (before.get('outcome') == restored.get('outcome') == 'ALLOWED' and
        denied.get('outcome') == 'DENIED' and stable and fresh and result['configuration_restored'] and
        'test_error_type' not in result):
        result['status'] = 'SOURCE_CONTEXT_VERIFIED'
        result['basis'] = ('The driver needs the source-conditioned object-read grant; '
            'inspection-only permission denies a fresh GET and restoring the grant restores access. '
            'This does not independently verify executor request sources.' if control.kind == 'bucket' else
            'The public VCN ACL rejects the same connection when the source subnet is excluded; '
            'restoring the ACL permits a fresh database session.')
    else:
        if denied.get('outcome') == 'ALLOWED':
            result['reason'] = 'The negative read still succeeded. Review the effective reader, alternate grants, connection caching, and propagation.'
        elif denied.get('outcome') != 'DENIED':
            result['reason'] = 'No explicit authorization/ACL denial was observed; a timeout or other error cannot verify the source control.'
        elif restored.get('outcome') != 'ALLOWED':
            result['reason'] = 'The original setting was restored, but read recovery was not observed within the retry window.'
        elif not fresh:
            result['reason'] = 'The database session was reused or its ID is missing. Verify fresh connections before claiming the ACL test passed.'
        else:
            result['reason'] = 'The reader, target, or saved configuration changed during the comparison. Review the journal before repeating.'
    journal()
    return result


def print_v02_control_result(result):
    print('Phase       Expected  Observed  Attempts')
    for phase in ('allow', 'deny', 'restored'):
        rows = [r for r in result.get('observations', []) if r['phase'] == phase]
        if rows:
            row = rows[-1]
            print('{:<11} {:<9} {:<9} {}'.format(phase, row['expected'], row.get('outcome', 'ERROR'), len(rows)))
    print('Result: ' + result['status'])
    print('Restoration: ' + result.get('restoration', 'NOT_NEEDED'))
    if result.get('basis') or result.get('reason'):
        print(result.get('basis') or result['reason'])


def restore_v02_control_from_journal(control, journal_path):
    """Recovery after a terminated session; restore only the recorded target control."""
    body = json.loads(Path(journal_path).read_text())
    saved = body['saved_control']
    if saved['kind'] != control.kind or saved['resource_id'] != control.resource_id:
        raise ValueError('The journal belongs to another target; no write performed')
    if control.kind == 'bucket' and (control.allow != saved['allow'] or control.deny != saved['deny']):
        raise ValueError('The controller selects another grant; use the journal’s original settings')
    control.restore(saved)
    result = {'restoration': 'RESTORED', 'configuration_restored': control.unchanged(saved)}
    body['result'].update(result)
    v02_write_journal(journal_path, body)
    return result


def v02_write_journal(path, body):
    # Atomic replacement retains the last complete recovery record if interrupted.
    with tempfile.NamedTemporaryFile(mode='w', dir=str(Path(path).parent), delete=False) as stream:
        temporary = stream.name
        try:
            json.dump(body, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            os.unlink(temporary)
            raise
    os.replace(temporary, path)
