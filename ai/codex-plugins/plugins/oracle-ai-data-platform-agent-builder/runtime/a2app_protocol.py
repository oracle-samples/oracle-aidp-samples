"""A2App v1 fixed command boundary, shared by packaging and native agents.

The initial schema profile is deliberately bounded: closed objects and primitive
fields. Unsupported schema keywords are rejected rather than partly enforced.
"""
from copy import deepcopy
from difflib import get_close_matches
import hashlib
import json
import math
import re

PROTOCOL = 'aidp.a2app'
CATALOG = 'https://aidp.local/catalogs/a2app-v2'
CATALOGS = {CATALOG, 'https://aidp.local/catalogs/a2app-v1'}
A2UI_MIME = 'application/json+a2ui'
APP_MIME = 'application/json+aidp-a2app'
A2UI_EXTENSION = 'https://a2ui.org/a2a-extension/a2ui/v0.8'
APP_METADATA = 'aidp.a2app'
ID = re.compile(r'[a-z][a-z0-9-]{0,63}')
APP_ACTIONS = {'describe', 'initialize', 'display_all'}


class ProtocolError(ValueError):
    def __init__(self, code, message, suggestions=None):
        super().__init__(message)
        self.code, self.suggestions = code, suggestions or []


def encoded(value):
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':'))


def schema_valid(schema):
    allowed = {'type', 'properties', 'required', 'additionalProperties', 'enum',
               'minimum', 'maximum', 'minLength', 'maxLength', 'description'}
    if (not isinstance(schema, dict) or set(schema) - allowed
            or schema.get('type') not in {'object', 'string', 'integer', 'number', 'boolean'}):
        raise ValueError('Unsupported A2App input schema; use the documented bounded profile')
    if schema['type'] == 'object':
        if schema.get('additionalProperties') is not False:
            raise ValueError('A2App input objects must reject additional properties')
        props = schema.get('properties', {})
        if not isinstance(props, dict) or len(props) > 20 or not set(schema.get('required', [])) <= props.keys():
            raise ValueError('Invalid action properties or required fields')
        for item in props.values():
            schema_valid(item)


def validate_value(value, schema, path='arguments'):
    kind = schema['type']
    valid = {'object': isinstance(value, dict), 'string': isinstance(value, str),
             'integer': type(value) is int, 'number': type(value) in {int, float},
             'boolean': type(value) is bool}[kind]
    if not valid or (kind == 'number' and not math.isfinite(value)):
        raise ProtocolError('INVALID_ARGUMENTS', path + ' must be ' + kind)
    if kind == 'object':
        props = schema.get('properties', {})
        if set(value) - props.keys() or not set(schema.get('required', [])) <= value.keys():
            raise ProtocolError('INVALID_ARGUMENTS', path + ' has missing or unknown fields')
        for key, item in value.items():
            validate_value(item, props[key], path + '.' + key)
    if 'enum' in schema and value not in schema['enum']:
        raise ProtocolError('INVALID_ARGUMENTS', path + ' is not an allowed choice')
    if kind == 'string' and not schema.get('minLength', 0) <= len(value) <= schema.get('maxLength', 4000):
        raise ProtocolError('INVALID_ARGUMENTS', path + ' has invalid length')
    if kind in {'integer', 'number'} and not schema.get('minimum', -math.inf) <= value <= schema.get('maximum', math.inf):
        raise ProtocolError('INVALID_ARGUMENTS', path + ' is outside its bounds')


def validate_definition(value):
    if (not isinstance(value, dict) or set(value) != {'protocol', 'protocolVersion', 'catalogId', 'components'}
            or value['protocol'] != PROTOCOL or value['protocolVersion'] != 1 or value['catalogId'] not in CATALOGS
            or not isinstance(value['components'], list) or not 1 <= len(value['components']) <= 20):
        raise ValueError('Invalid A2App definition')
    ids = set()
    for component in value['components']:
        if (set(component) != {'id', 'label', 'contract', 'actions'} or not ID.fullmatch(component['id'])
                or component['id'] in ids or not isinstance(component['label'], str)
                or not 1 <= len(component['label']) <= 120
                or not re.fullmatch(r'contracts/[a-z0-9-]+\.md', component['contract'])
                or not isinstance(component['actions'], dict) or not 1 <= len(component['actions']) <= 20
                or 'display' not in component['actions']):
            raise ValueError('Invalid or duplicate A2App component')
        ids.add(component['id'])
        for name, action in component['actions'].items():
            if (not ID.fullmatch(name) or set(action) - {'label', 'handler', 'effect', 'inputSchema', 'parentAction'}
                    or not {'label', 'handler', 'effect', 'inputSchema'} <= action.keys()
                    or not isinstance(action['label'], str) or not 1 <= len(action['label']) <= 120
                    or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', action['handler'])
                    or action['effect'] not in {'read', 'process', 'artifact'}):
                raise ValueError('Invalid action definition')
            schema_valid(action['inputSchema'])
            seen = {name}; current = action
            while 'parentAction' in current:
                parent = current['parentAction']
                if not isinstance(parent, str) or parent not in component['actions'] or parent in seen:
                    raise ValueError('Action hierarchy requires existing parents and no cycles')
                seen.add(parent); current = component['actions'][parent]
    return value


def public_definition(definition):
    result = deepcopy(definition)
    for component in result['components']:
        for action in component['actions'].values():
            action.pop('handler')
    return result


def parse_request(raw):
    if isinstance(raw, dict) and raw.get('kind') == 'data':
        raw = request_from_part(raw)
    if isinstance(raw, str):
        if len(raw) > 32000:
            raise ProtocolError('INVALID_REQUEST', 'Request exceeds the A2App size limit')
        try:
            value = json.loads(raw)
        except ValueError:
            if raw.lstrip().startswith(('{', 'Component:')):
                raise ProtocolError('INVALID_REQUEST', 'Use an A2App JSON command or ordinary chat text') from None
            value = {'protocol': PROTOCOL, 'protocolVersion': 1, 'type': 'chat',
                     'requestId': 'native-chat', 'channel': 'chat', 'text': raw}
    else:
        value = raw
    required = {'protocol', 'protocolVersion', 'type', 'requestId', 'channel'}
    if not isinstance(value, dict) or not required <= value.keys():
        raise ProtocolError('INVALID_REQUEST', 'Missing A2App envelope fields')
    if value['protocol'] != PROTOCOL or type(value['protocolVersion']) is not int or value['protocolVersion'] != 1:
        raise ProtocolError('UNSUPPORTED_VERSION', 'This agent supports aidp.a2app version 1')
    if (value['channel'] not in {'ui', 'cli', 'chat'} or not isinstance(value['requestId'], str)
            or not 1 <= len(value['requestId']) <= 100):
        raise ProtocolError('INVALID_REQUEST', 'Invalid channel or request ID')
    if value['type'] == 'chat':
        allowed = required | {'text', 'context', 'definitionRevision'}
        if set(value) - allowed or not isinstance(value.get('text'), str) or not 1 <= len(value['text'].strip()) <= 4000:
            raise ProtocolError('INVALID_REQUEST', 'Chat requires bounded text')
        if 'context' in value and (not isinstance(value['context'], dict)
                or set(value['context']) - {'componentId', 'runId'}):
            raise ProtocolError('INVALID_REQUEST', 'Invalid chat context')
        return value
    required |= {'componentId', 'actionId', 'arguments'}
    if (value['type'] != 'command' or not required <= value.keys()
            or set(value) - (required | {'definitionRevision', 'idempotencyKey', 'process', 'presentation'})
            or not isinstance(value['componentId'], str) or not isinstance(value['actionId'], str)
            or not isinstance(value['arguments'], dict)):
        raise ProtocolError('INVALID_REQUEST', 'Use a typed component/action command')
    if 'presentation' in value and value['presentation'] not in [{'a2uiVersion':'0.8','catalogId':c} for c in CATALOGS]:
        raise ProtocolError('UNSUPPORTED_PRESENTATION', 'Use the advertised A2UI 0.8 catalog')
    if 'process' in value and (not isinstance(value['process'], dict)
            or set(value['process']) - {'runId', 'expectedStateVersion'}
            or ('expectedStateVersion' in value['process'] and
                (type(value['process']['expectedStateVersion']) is not int or value['process']['expectedStateVersion'] < 1))):
        raise ProtocolError('INVALID_REQUEST', 'Invalid process reference/version')
    return value


def request_from_part(part):
    """Standard A2UI events carry business arguments; only app policy is extra."""
    metadata = part.get('metadata') or {}
    if not isinstance(metadata, dict):
        raise ProtocolError('INVALID_REQUEST', 'Part metadata must be an object')
    if metadata.get('mimeType') == APP_MIME:
        return part.get('data')
    if metadata.get('mimeType') != A2UI_MIME:
        raise ProtocolError('INVALID_REQUEST', 'Unsupported structured content type')
    value = part.get('data')
    if not isinstance(value, dict) or set(value) != {'userAction'}:
        raise ProtocolError('INVALID_REQUEST', 'Expected an A2UI userAction')
    event = value['userAction']
    if (not isinstance(event, dict) or set(event) != {'name', 'surfaceId', 'sourceComponentId', 'timestamp', 'context'}
            or any(not isinstance(event[k], str) or not 1 <= len(event[k]) <= 200 for k in ('name','surfaceId','sourceComponentId','timestamp'))
            or not isinstance(event['context'], dict)):
        raise ProtocolError('INVALID_REQUEST', 'Invalid A2UI userAction fields')
    from datetime import datetime
    try:
        if datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).tzinfo is None:
            raise ValueError()
    except ValueError:
        raise ProtocolError('INVALID_REQUEST', 'A2UI timestamp requires an ISO date and timezone') from None
    component, separator, action = event['name'].rpartition('.')
    if not separator or component != event['surfaceId']:
        raise ProtocolError('INVALID_REQUEST', 'Action name must identify its business surface')
    extra = metadata.get(APP_METADATA)
    allowed = {'protocolVersion','requestId','channel','definitionRevision','idempotencyKey','process','presentation','interruptId'}
    if not isinstance(extra, dict) or set(extra) - allowed:
        raise ProtocolError('INVALID_REQUEST', 'Missing or unknown application action metadata')
    return {'protocol':PROTOCOL, 'type':'command', 'componentId':component,
            'actionId':action, 'arguments':event['context'], **{k:v for k,v in extra.items() if k!='interruptId'}}


def request_part(request, *, event=None, interrupt_id=None):
    """A2A DataPart encoding; no JSON hidden inside a text prompt."""
    if event is not None:
        meta = {k:v for k,v in request.items() if k not in {'protocol','type','componentId','actionId','arguments'}}
        if interrupt_id is not None: meta['interruptId'] = interrupt_id
        return {'kind':'data','data':event,'metadata':{'mimeType':A2UI_MIME,APP_METADATA:meta}}
    metadata = {'mimeType':APP_MIME}
    if interrupt_id is not None: metadata[APP_METADATA] = {'interruptId':interrupt_id}
    return {'kind':'data','data':request,'metadata':metadata}


def native_input(raw, parts):
    """Preserve the native runtime's original Parts and reject ambiguous inputs."""
    values = [p.model_dump() if hasattr(p,'model_dump') else p for p in (parts or [])]
    data = [p for p in values if p.get('kind') == 'data']
    if not data:
        return raw, False
    if len(data) != 1 or any(p.get('kind') != 'data' for p in values):
        raise ProtocolError('INVALID_REQUEST', 'Send one structured command per turn')
    request = parse_request(data[0])
    extra = (data[0].get('metadata') or {}).get(APP_METADATA) or {}
    if not isinstance(extra,dict):
        raise ProtocolError('INVALID_REQUEST', 'Application metadata must be an object')
    receipt = extra.get('interruptId')
    return ({'request':request,'interruptId':receipt} if receipt else request), True


def response_parts(value):
    """Business results and A2UI messages remain separate typed A2A parts."""
    business = {k:v for k,v in value.items() if k!='presentation'}
    return [{'kind':'data','data':business,'metadata':{'mimeType':APP_MIME}},
        *[{'kind':'data','data':message,'metadata':{'mimeType':A2UI_MIME}}
          for message in value.get('presentation',{}).get('a2ui',[])]]


def resolve(request, definition, revision):
    if request.get('presentation',{}).get('catalogId',definition['catalogId']) != definition['catalogId']:
        raise ProtocolError('UNSUPPORTED_PRESENTATION', 'Requested catalog differs from the agent definition')
    if request.get('definitionRevision', revision) != revision:
        raise ProtocolError('REVISION_MISMATCH', 'Reload the agent definition before issuing commands')
    if request['type'] == 'chat':
        return None
    component_id, action_id = request['componentId'], request['actionId']
    if component_id == '@app':
        if action_id not in APP_ACTIONS:
            raise ProtocolError('UNKNOWN_ACTION', 'Unknown application action', sorted(APP_ACTIONS))
        validate_value(request['arguments'], {'type': 'object', 'properties': {}, 'additionalProperties': False})
        return None
    if request.get('definitionRevision') != revision:
        raise ProtocolError('REVISION_MISMATCH', 'Component commands require the current definitionRevision')
    components = {c['id']: c for c in definition['components']}
    if component_id not in components:
        raise ProtocolError('UNKNOWN_COMPONENT', 'Unknown component: ' + component_id,
                            get_close_matches(component_id, components, n=3, cutoff=.4))
    actions = components[component_id]['actions']
    if action_id not in actions:
        raise ProtocolError('UNKNOWN_ACTION', 'Unknown action: ' + action_id,
                            get_close_matches(action_id, actions, n=3, cutoff=.4))
    action = actions[action_id]
    validate_value(request['arguments'], action['inputSchema'])
    if action['effect'] != 'read':
        key = request.get('idempotencyKey')
        if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9-]{8,100}', key):
            raise ProtocolError('INVALID_REQUEST', 'State changes require an idempotency key')
        if type(request.get('process', {}).get('expectedStateVersion')) is not int:
            raise ProtocolError('STALE_STATE', 'Refresh and supply the current process version')
    return action


def response(request, revision, *, status='ok', result=None, message='', **fields):
    value = {'protocol': PROTOCOL, 'protocolVersion': 1, 'requestId': request.get('requestId', 'invalid-request'),
             'definitionRevision': revision, 'status': status, 'message': message, **fields}
    if request.get('type') == 'command':
        value.update(componentId=request['componentId'], actionId=request['actionId'])
    if result is not None:
        value['result'] = result
    encoded(value)
    return value


def error_response(request, revision, error):
    return response(request, revision, status='error', message=str(error),
                    error={'code': error.code, 'message': str(error), 'suggestions': error.suggestions})


def command_id(request):
    """Receipts bind semantic content, not the per-attempt transport request ID."""
    return request.get('idempotencyKey') or hashlib.sha256(encoded(request).encode()).hexdigest()[:32]
