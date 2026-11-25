"""Standard UI/CLI client for a deployed A2App agent. Start serve before CLI calls."""
import argparse
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
import uuid

from a2app_protocol import PROTOCOL
from config import load_configuration, private_file


def local_sender(config):
    address_file = config.state_directory / 'server.json'
    token_file = config.state_directory / 'api-token'
    if not address_file.exists() or not token_file.exists():
        raise ValueError('Start this configuration with the serve command in another terminal first')
    private_file(address_file)
    private_file(token_file)
    saved = json.loads(address_file.read_text())
    if saved.get('bindingFingerprint') != config.fingerprint:
        raise ValueError('Running gateway has a different agent binding or revision; restart serve with the matching configuration')
    address = saved['url']
    target = urlsplit(address)
    if target.scheme != 'http' or target.hostname != '127.0.0.1' or target.path or target.query or target.fragment or target.username or target.password:
        raise ValueError('Invalid saved loopback server address')
    endpoint = address + '/api/apps/' + quote(config.values['agentKey'], safe='') + '/a2app'
    token = token_file.read_text().strip()
    opener = build_opener(ProxyHandler({}))

    def send(packet):
        body = {'protocol': PROTOCOL, 'protocolVersion': 1, 'requestId': str(uuid.uuid4()), 'channel': 'cli', **packet}
        request = Request(endpoint, data=json.dumps(body, allow_nan=False).encode(),
            headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
        try:
            with opener.open(request, timeout=240) as response:
                value = json.load(response)
        except HTTPError as error:
            raise ValueError(json.load(error).get('error', 'Local agent request failed')) from None
        except URLError:
            raise ValueError('Local gateway is unavailable; restart serve before retrying') from None
        if value['status'] == 'error':
            raise ValueError(value['error']['code'] + ': ' + value['message'])
        return value
    return send


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    sub = parser.add_subparsers(dest='command', required=True)
    host = sub.add_parser('serve')
    host.add_argument('--host', choices=['127.0.0.1'], default='127.0.0.1')
    host.add_argument('--port', type=int, default=0, help='0 selects a free loopback port')
    sub.add_parser('describe')
    call = sub.add_parser('invoke')
    call.add_argument('component')
    call.add_argument('action')
    call.add_argument('--arguments', default='{}')
    call.add_argument('--idempotency-key')
    call.add_argument('--expected-version', type=int)
    call.add_argument('--wait', action='store_true')
    call.add_argument('--output', type=Path, help='Save an action-provided Markdown report; refuses overwrites')
    chat = sub.add_parser('chat')
    chat.add_argument('text')
    args = parser.parse_args(argv)
    config = load_configuration(args.config)
    if args.command == 'serve':
        if not 0 <= args.port <= 65535:
            parser.error('port must be between 0 and 65535')
        from server import serve
        serve(config, args.port)
        return
    send = local_sender(config)
    description = send({'type': 'command', 'componentId': '@app', 'actionId': 'describe', 'arguments': {}})
    revision = description['definitionRevision']
    if args.command == 'describe':
        result = description
    elif args.command == 'chat':
        result = send({'type': 'chat', 'definitionRevision': revision, 'text': args.text})
    else:
        initial = send({'type': 'command', 'componentId': '@app', 'actionId': 'initialize', 'arguments': {}})
        process = initial.get('process')
        component = next((c for c in description['result']['definition']['components'] if c['id'] == args.component), None)
        action = component['actions'].get(args.action) if component else None
        packet = {'type': 'command', 'definitionRevision': revision, 'componentId': args.component,
            'actionId': args.action, 'arguments': json.loads(args.arguments),
            'idempotencyKey': args.idempotency_key or str(uuid.uuid4())}
        if process is not None:
            if not isinstance(process, dict) or not {'runId', 'stateVersion'} <= process.keys():
                raise ValueError('Agent initialization returned incomplete process state')
            packet['process'] = {'runId': process['runId'],
                'expectedStateVersion': args.expected_version if args.expected_version is not None else process['stateVersion']}
        elif action and action['effect'] != 'read':
            raise ValueError('This action requires process state, but agent initialization returned none')
        result = send(packet)
        deadline = time.monotonic() + 900
        while args.wait and result['status'] == 'accepted':
            if time.monotonic() >= deadline:
                raise TimeoutError('Agent remains active; use its declared status action to reconnect')
            time.sleep(2)
            component = next(c for c in description['result']['definition']['components'] if c['id'] == args.component)
            result = send({'type': 'command', 'definitionRevision': revision, 'componentId': args.component,
                'actionId': 'status' if 'status' in component['actions'] else 'display', 'arguments': {}})
        if args.output:
            report = result.get('result', {}).get('markdown')
            if not isinstance(report, str):
                raise ValueError('This action returned no Markdown report')
            with args.output.expanduser().open('x', encoding='utf-8') as output:
                output.write(report)
            result['savedFile'] = str(args.output.expanduser().resolve())
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, TimeoutError) as error:
        raise SystemExit(str(error)) from None
