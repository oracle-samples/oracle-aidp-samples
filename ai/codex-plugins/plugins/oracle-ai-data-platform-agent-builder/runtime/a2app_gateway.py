"""A2A transport receipts only; business-process state remains in the native agent."""
import json
import re
import sqlite3
import threading
import time
import uuid

from a2app_protocol import (APP_MIME, A2UI_MIME, A2UI_EXTENSION, PROTOCOL, ProtocolError,
    encoded, error_response, parse_request, public_definition, request_part, resolve, response)
from config import private_file
from oci_transport import OciTransport


class A2AppApplication:
    def __init__(self, config, call=None):
        self.config, self.definition = config, config.definition
        self.app_id = config.values['agentKey']
        self.call = call or OciTransport(config)
        self.lock = threading.Lock()
        self.db = sqlite3.connect(private_file(config.state_directory / 'client.sqlite'), check_same_thread=False)
        self.db.execute('CREATE TABLE IF NOT EXISTS client_binding (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
        saved = self.db.execute('SELECT value FROM client_binding WHERE id=1').fetchone()
        if saved and saved[0] != encoded(config.binding):
            self.db.close()
            raise ValueError('stateDirectory belongs to another agent, revision, definition or OCI identity; choose its matching configuration')
        self.db.execute('INSERT OR IGNORE INTO client_binding VALUES (1,?)', (encoded(config.binding),))
        self.db.execute('CREATE TABLE IF NOT EXISTS app_commands '
            '(app TEXT, revision TEXT, id TEXT, request TEXT NOT NULL, response TEXT NOT NULL, PRIMARY KEY(app,revision,id))')
        self.db.execute('CREATE TABLE IF NOT EXISTS a2app_contexts '
            '(app TEXT, revision TEXT, value TEXT NOT NULL, PRIMARY KEY(app,revision))')
        self.db.commit()

    def close(self):
        self.db.close()

    def _context(self):
        row = self.db.execute('SELECT value FROM a2app_contexts WHERE app=? AND revision=?',
            (self.app_id, self.config.revision)).fetchone()
        return json.loads(row[0]) if row else {'contextId': str(uuid.uuid4())}

    def _save_context(self, context):
        self.db.execute('INSERT OR REPLACE INTO a2app_contexts VALUES (?,?,?)',
            (self.app_id, self.config.revision, encoded(context)))
        self.db.commit()

    def _task_value(self, task, request, context):
        if task.get('kind') != 'task' or task.get('contextId') != context['contextId']:
            raise ValueError('A2A returned an unrelated application context')
        parts = [part for artifact in task.get('artifacts', []) for part in artifact.get('parts', [])]
        if not parts:
            return None
        business = [p['data'] for p in parts if p.get('kind') == 'data'
                    and (p.get('metadata') or {}).get('mimeType') == APP_MIME]
        if len(business) != 1:
            raise ValueError('Expected one native A2App result DataPart; no text/JSON fallback is supported')
        value = business[0]
        views = [p['data'] for p in parts if p.get('kind') == 'data'
                 and (p.get('metadata') or {}).get('mimeType') == A2UI_MIME]
        if views:
            value = {**value, 'presentation': {'mode': 'snapshot', 'a2ui': views}}
        if (value.get('protocol') != PROTOCOL or value.get('protocolVersion') != 1
                or value.get('requestId') != request['requestId']
                or value.get('definitionRevision') != self.config.revision
                or value.get('status') not in {'ok', 'accepted', 'input_required', 'error'}):
            raise ValueError('Native A2App response does not match this request/release')
        if len(encoded(value).encode()) > 2 * 1024 * 1024:
            raise ValueError('Native A2App result exceeds the client byte limit')
        if ('definition' in value.get('result', {}) and
                value['result']['definition'] != public_definition(self.definition)):
            raise ValueError('Native discovery differs from the configured component definition')
        receipt = value.get('telemetry', {}).get('pendingInterruptId')
        if receipt is not None:
            if (not isinstance(receipt, str) or not re.fullmatch(r'[a-f0-9]{32}', receipt)
                    or value.get('process', {}).get('phase') == 'analyzing'):
                raise ValueError('Invalid native pause receipt')
            context['interruptId'] = receipt
        value.setdefault('telemetry', {}).update(transport='a2a-parts', a2uiDataParts=len(views))
        if task.get('status', {}).get('state') == 'failed':
            value['telemetry']['taskFinalizationWarning'] = (
                'A2A reported task failure after returning this matching result; inspect native diagnostics.')
        return value

    def invoke(self, raw):
        started = time.monotonic()
        request = {}
        revision = self.config.revision
        try:
            request = parse_request(raw)
            action = resolve(request, self.definition, revision)
        except ProtocolError as error:
            return error_response(request, revision, error)
        with self.lock:
            context = self._context()
            semantic = {k: v for k, v in request.items() if k not in {'requestId', 'presentation', 'channel'}}
            key = request.get('idempotencyKey')
            if key:
                previous = self.db.execute('SELECT request,response FROM app_commands WHERE app=? AND revision=? AND id=?',
                    (self.app_id, revision, key)).fetchone()
                if previous:
                    if previous[0] != encoded(semantic):
                        return error_response(request, revision, ProtocolError('IDEMPOTENCY_CONFLICT',
                            'Idempotency key was used for different input'))
                    return json.loads(previous[1]) | {'requestId': request['requestId'], 'replayed': True}
            if context.get('taskState') in {'submitted', 'working'} and context.get('taskId'):
                _, polled = self.call('POST', self.config.endpoint, {'jsonrpc': '2.0', 'id': str(uuid.uuid4()),
                    'method': 'tasks/get', 'params': {'id': context['taskId']}},
                    headers={'x-aidp-a2a-interrupts': 'true'})
                task = polled.get('result', {})
                if (polled.get('error') or task.get('id') != context['taskId']
                        or task.get('contextId') != context['contextId']):
                    raise ValueError('Cannot verify the current A2App task; retry status')
                context['taskState'] = task.get('status', {}).get('state')
                if context['taskState'] not in {'submitted', 'working'} and context.get('pendingRequest'):
                    finished = self._task_value(task, context['pendingRequest'], context)
                    if finished is None:
                        raise ValueError('The native task ended without an A2App result')
                    terminal_key = context['pendingRequest'].get('idempotencyKey')
                    if terminal_key:
                        self.db.execute('UPDATE app_commands SET response=? WHERE app=? AND revision=? AND id=?',
                            (encoded(finished), self.app_id, revision, terminal_key))
                    if finished['status'] == 'error':
                        context['actionError'] = finished['error']
                    elif context['taskState'] == 'failed':
                        context['transportWarning'] = finished['telemetry']['taskFinalizationWarning']
                    context.pop('pendingRequest', None)
            mutation = action is not None and action['effect'] == 'process'
            if mutation and context.get('taskState') in {'submitted', 'working'}:
                self._save_context(context)
                return error_response(request, revision, ProtocolError('PROCESS_BUSY',
                    'An agent action is running; read status before another action'))
            self._save_context(context)
            if mutation:
                if not context.get('interruptId'):
                    raise ValueError('Native pause receipt is missing; initialize this agent context before changing state')
                context.pop('actionError', None)
            event = raw['data'] if isinstance(raw, dict) and (raw.get('metadata') or {}).get('mimeType') == A2UI_MIME else None
            part = request_part(request, event=event,
                interrupt_id=context.get('interruptId') if mutation else None)
            message = {'messageId': str(uuid.uuid4()), 'role': 'user',
                'contextId': context['contextId'], 'parts': [part],
                'metadata': {'a2uiClientCapabilities': {'supportedCatalogIds': [self.definition['catalogId']]}}}
            _, packet = self.call('POST', self.config.endpoint, {'jsonrpc': '2.0', 'id': str(uuid.uuid4()),
                'method': 'message/send', 'params': {'configuration': {'blocking': not mutation}, 'message': message}},
                headers={'x-aidp-a2a-interrupts': 'true', 'X-A2A-Extensions': A2UI_EXTENSION})
            if packet.get('error'):
                raise ValueError('Native agent returned an A2A error')
            task = packet.get('result', {})
            task_state = task.get('status', {}).get('state')
            value = self._task_value(task, request, context)
            if mutation:
                context.update(taskId=task['id'], taskState=task_state)
                if task_state in {'submitted', 'working'}:
                    context['pendingRequest'] = request
            if value is None:
                if task_state not in {'submitted', 'working'}:
                    raise ValueError('Native agent returned no A2App result (task ' + str(task_state) + ')')
                value = response(request, revision, status='accepted', message='Agent action is running. Read status to reconnect.')
            elif value['status'] != 'error' and context.get('taskState') in {'submitted', 'working'}:
                value['status'] = 'accepted'
            if not mutation and context.get('actionError') and value['status'] != 'error':
                error = context.pop('actionError')
                value.update(status='error', error=error, message=error['message'])
            if context.get('transportWarning'):
                value.setdefault('telemetry', {})['taskFinalizationWarning'] = context['transportWarning']
            value.setdefault('telemetry', {}).update(gatewayElapsedMs=round((time.monotonic() - started) * 1000, 1),
                agentKey=self.app_id, a2aContextId=context['contextId'], taskId=task['id'],
                taskState=context.get('taskState', task_state), stateAuthority='native-checkpointer')
            self._save_context(context)
            if key:
                self.db.execute('INSERT INTO app_commands VALUES (?,?,?,?,?)',
                    (self.app_id, revision, key, encoded(semantic), encoded(value)))
                self.db.commit()
            return value
