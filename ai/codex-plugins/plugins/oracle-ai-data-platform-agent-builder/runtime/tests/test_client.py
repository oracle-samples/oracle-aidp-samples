"""Offline boundary and HTTP tests; no OCI credentials or cloud calls."""
from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from a2app_gateway import A2AppApplication
from a2app_protocol import APP_MIME, CATALOG, PROTOCOL, public_definition, response
from config import load_configuration
from client import local_sender, main as client_main
from oci_transport import OciTransport
from server import make_server


def definition():
    action = lambda effect: {'label': effect, 'handler': 'handle', 'effect': effect,
        'inputSchema': {'type': 'object', 'properties': {}, 'additionalProperties': False}}
    return {'protocol': PROTOCOL, 'protocolVersion': 1, 'catalogId': CATALOG, 'components': [
        {'id': 'tasks', 'label': 'Tasks', 'contract': 'contracts/tasks.md',
         'actions': {'display': action('read'), 'status': action('read'), 'start': action('process')}}]}


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'definition.json').write_text(json.dumps(definition()))
        self.values = {'endpoint': 'https://gateway.aidp.region.oraclecloud.com/agentendpoint/test-agent/a2a',
            'agentKey': 'test-agent', 'workspaceKey': 'test-workspace', 'runtimeRevision': 'revision-one',
            'definitionFile': 'definition.json', 'stateDirectory': 'state', 'ociProfile': 'DEFAULT', 'region': 'test-region'}

    def config(self, **changes):
        path = self.root / 'config.json'
        path.write_text(json.dumps(self.values | changes))
        return load_configuration(path)

    def request(self, component='@app', action='describe', **extra):
        return {'protocol': PROTOCOL, 'protocolVersion': 1, 'type': 'command',
            'requestId': 'request-1', 'channel': 'cli', 'componentId': component, 'actionId': action,
            'arguments': {}, 'definitionRevision': 'revision-one', **extra}

    def test_configuration_is_explicit_and_endpoint_is_constrained(self):
        self.assertEqual(self.config().values['authType'], 'security_token')
        for endpoint in ('http://example.test/a2a', 'https://evil.example/a2a',
                self.values['endpoint'] + '?token=secret', 'https://user:password@gateway.aidp.region.oraclecloud.com/a2a'):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                self.config(endpoint=endpoint)
        custom = 'https://gateway.sovereign.example/a2a'
        self.assertEqual(self.config(endpoint=custom, expectedHostname='gateway.sovereign.example').endpoint, custom)

    def test_transport_only_signs_the_configured_endpoint_and_hides_cli_diagnostics(self):
        config = self.config()
        calls = []
        def run(command, **_kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, json.dumps({'status': '200 OK', 'data': {'ok': True}}).encode(), b'')
        transport = OciTransport(config, run=run)
        packet = {'jsonrpc': '2.0', 'method': 'tasks/get', 'params': {'id': 'task-1'}}
        self.assertEqual(transport('POST', config.endpoint, packet), (200, {'ok': True}))
        self.assertIn('--request-body', calls[0])
        with self.assertRaises(ValueError):
            transport('POST', 'https://another.oraclecloud.com/a2a', packet)
        with self.assertRaises(ValueError):
            transport('POST', config.endpoint, packet | {'method': 'unknown/mutation'})
        def fail(command, **_kwargs):
            return subprocess.CompletedProcess(command, 1, b'', b'Authorization: private-test-value')
        with self.assertRaisesRegex(ValueError, 'OCI CLI agent request failed') as error:
            OciTransport(config, run=fail)('POST', config.endpoint, packet)
        self.assertNotIn('private-test-value', str(error.exception))

    def test_cli_rejects_a_running_gateway_for_another_revision(self):
        config = self.config()
        for name, value in [('api-token', 'private-test-token'), ('server.json', json.dumps({
                'url': 'http://127.0.0.1:1', 'pid': 1, 'bindingFingerprint': config.fingerprint}))]:
            path = config.state_directory / name
            path.write_text(value)
            path.chmod(0o600)
        self.assertTrue(callable(local_sender(config)))
        with self.assertRaisesRegex(ValueError, 'different agent binding or revision'):
            local_sender(self.config(runtimeRevision='revision-two'))

    def test_cli_read_does_not_require_process_state(self):
        sent = []
        def send(packet):
            sent.append(packet)
            result = {'status': 'ok', 'definitionRevision': 'revision-one'}
            if packet['actionId'] == 'describe':
                result['result'] = {'definition': public_definition(definition())}
            return result
        with patch('client.load_configuration'), patch('client.local_sender', return_value=send), redirect_stdout(io.StringIO()):
            client_main(['--config', 'unused.json', 'invoke', 'tasks', 'display'])
        self.assertEqual(sent[-1]['actionId'], 'display')
        self.assertNotIn('process', sent[-1])
        with patch('client.load_configuration'), patch('client.local_sender', return_value=send):
            with self.assertRaisesRegex(ValueError, 'requires process state'):
                client_main(['--config', 'unused.json', 'invoke', 'tasks', 'start'])
        self.assertEqual(sent[-1]['actionId'], 'initialize')

    def test_cli_preserves_an_explicit_expected_version(self):
        sent = []
        def send(packet):
            sent.append(packet)
            result = {'status': 'ok', 'definitionRevision': 'revision-one'}
            if packet['actionId'] == 'describe':
                result['result'] = {'definition': public_definition(definition())}
            elif packet['actionId'] == 'initialize':
                result['process'] = {'runId': 'run-one', 'stateVersion': 2}
            return result
        with patch('client.load_configuration'), patch('client.local_sender', return_value=send), redirect_stdout(io.StringIO()):
            client_main(['--config', 'unused.json', 'invoke', 'tasks', 'start', '--expected-version', '0'])
        self.assertEqual(sent[-1]['process'], {'runId': 'run-one', 'expectedStateVersion': 0})

    def test_native_parts_idempotency_recovery_and_no_business_state_in_gateway(self):
        config = self.config()
        calls, pending = [], {}
        def task(request, context_id, state='completed'):
            result = response(request, config.revision,
                result={'definition': public_definition(config.definition)} if request['actionId'] == 'describe' else {'count': 2},
                process={'runId': 'run-1', 'stateVersion': 2, 'phase': 'ready'},
                telemetry={'modelCalls': 0, 'pendingInterruptId': 'a' * 32})
            return {'kind': 'task', 'id': 'task-1', 'contextId': context_id, 'status': {'state': state},
                'artifacts': [] if state == 'working' else [{'parts': [{'kind': 'data', 'data': result,
                    'metadata': {'mimeType': APP_MIME}}]}]}
        def send(_method, _url, packet, **_kwargs):
            calls.append(deepcopy(packet))
            if packet['method'] == 'tasks/get':
                return 200, {'result': task(pending['request'], pending['context'])}
            message = packet['params']['message']
            part = message['parts'][0]
            self.assertEqual(part['kind'], 'data')
            request = part['data']
            self.assertNotIn('state', request)
            if request['actionId'] == 'start':
                self.assertEqual(part['metadata']['aidp.a2app']['interruptId'], 'a' * 32)
                pending.update(request=request, context=message['contextId'])
                return 200, {'result': task(request, message['contextId'], 'working')}
            return 200, {'result': task(request, message['contextId'])}
        app = A2AppApplication(config, call=send)
        self.addCleanup(app.close)
        self.assertEqual(app.invoke(self.request())['status'], 'ok')
        start = self.request('tasks', 'start', idempotencyKey='command-001',
            process={'runId': 'run-1', 'expectedStateVersion': 2})
        self.assertEqual(app.invoke(start)['status'], 'accepted')
        count = len(calls)
        self.assertTrue(app.invoke(start | {'requestId': 'retry-1'})['replayed'])
        self.assertEqual(len(calls), count)
        done = app.invoke(self.request('tasks', 'status', requestId='status-1'))
        self.assertEqual(done['status'], 'ok')
        self.assertEqual(calls[-2]['method'], 'tasks/get')
        tables = {row[0] for row in app.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn('app_state', tables)
        other = self.config(runtimeRevision='different-revision')
        with self.assertRaisesRegex(ValueError, 'another agent, revision'):
            A2AppApplication(other, call=send)

    def test_text_result_is_not_silently_treated_as_structured_a2app(self):
        config = self.config()
        def send(_method, _url, packet, **_kwargs):
            context = packet['params']['message']['contextId']
            return 200, {'result': {'kind': 'task', 'id': 'task-1', 'contextId': context,
                'status': {'state': 'completed'}, 'artifacts': [{'parts': [{'kind': 'text', 'text': '{}'}]}]}}
        app = A2AppApplication(config, call=send)
        self.addCleanup(app.close)
        with self.assertRaisesRegex(ValueError, 'DataPart'):
            app.invoke(self.request())

    def test_loopback_http_cookie_origin_and_host_boundaries(self):
        config = self.config()
        class App:
            app_id = 'test-agent'
            def invoke(self, body):
                return response(body, config.revision, result={'ok': True})
        server = make_server(config, application=App())
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        opener = build_opener(ProxyHandler({}))
        with opener.open(server.client_url) as result:
            cookie = result.headers['Set-Cookie']
            self.assertIn('HttpOnly', cookie)
            self.assertIn('SameSite=Strict', cookie)
            self.assertNotIn(server.client_token.encode(), result.read())
        endpoint = server.client_url + '/api/apps/test-agent/a2app'
        data = json.dumps(self.request()).encode()
        headers = {'Cookie': cookie.split(';')[0], 'Content-Type': 'application/json'}
        with self.assertRaises(HTTPError) as failure:
            opener.open(Request(endpoint, data=data, headers=headers | {'Origin': 'https://evil.example'}))
        self.assertEqual(failure.exception.code, 403)
        failure.exception.close()
        with opener.open(Request(endpoint, data=data, headers=headers | {'Origin': server.client_url})) as result:
            self.assertEqual(json.load(result)['status'], 'ok')
        with self.assertRaises(HTTPError) as failure:
            opener.open(Request(server.client_url + '/api/config', headers={'Host': 'evil.example'}))
        failure.exception.close()
        with opener.open(Request(server.client_url + '/api/config', headers={'Cookie': headers['Cookie']})) as result:
            public = json.load(result)
        self.assertEqual(set(public), {'agentKey', 'catalogId', 'title'})


if __name__ == '__main__':
    unittest.main()
