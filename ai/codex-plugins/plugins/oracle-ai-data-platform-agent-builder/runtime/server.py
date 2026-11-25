"""Loopback-only browser/CLI gateway. No generated business Python executes here."""
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import html
import json
import os
from pathlib import Path
import secrets
from urllib.parse import quote, urlsplit

from a2app_gateway import A2AppApplication
from config import private_file

WEB = Path(__file__).parent / 'web'


def make_server(config, port=0, application=None):
    app = application or A2AppApplication(config)
    token = secrets.token_urlsafe(32)
    cookie_name = 'aidp_agent_' + config.fingerprint[:16]
    title = str(config.values.get('title', 'AIDP agent application'))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def trusted_host(self):
            return self.headers.get('Host') == '127.0.0.1:' + str(self.server.server_address[1])

        def authorized(self, mutation=False):
            if not self.trusted_host():
                return False
            bearer = self.headers.get('Authorization', '')
            if bearer.startswith('Bearer ') and hmac.compare_digest(bearer[7:], token):
                return True
            cookie = SimpleCookie()
            try:
                cookie.load(self.headers.get('Cookie', ''))
            except Exception:
                return False
            value = cookie.get(cookie_name)
            if not value or not hmac.compare_digest(value.value, token):
                return False
            expected = 'http://' + self.headers['Host']
            return not mutation or self.headers.get('Origin') == expected

        def reply(self, status, value, mime='application/json', cookie=False):
            raw = value if isinstance(value, bytes) else json.dumps(value, allow_nan=False).encode()
            self.send_response(status)
            for name, content in {'Content-Type': mime, 'Content-Length': str(len(raw)),
                    'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
                    'Referrer-Policy': 'no-referrer',
                    'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"}.items():
                self.send_header(name, content)
            if cookie:
                self.send_header('Set-Cookie', cookie_name + '=' + token + '; HttpOnly; SameSite=Strict; Path=/')
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            path = urlsplit(self.path)
            if not self.trusted_host() or path.query or path.fragment:
                self.reply(403, {'error': 'Unexpected local gateway host or URL'})
                return
            origin = self.headers.get('Origin')
            if origin and origin != 'http://' + self.headers['Host']:
                self.reply(403, {'error': 'Cross-origin gateway access is not permitted'})
                return
            static = {'/': ('index.html', 'text/html;charset=utf-8'),
                '/main.mjs': ('main.mjs', 'text/javascript'), '/a2app.mjs': ('a2app.mjs', 'text/javascript'),
                '/charts.mjs': ('charts.mjs', 'text/javascript'), '/style.css': ('style.css', 'text/css')}
            if path.path in static:
                filename, mime = static[path.path]
                self.reply(200, (WEB / filename).read_bytes(), mime, cookie=path.path == '/')
            elif not self.authorized():
                self.reply(403, {'error': 'Open the local gateway page first'})
            elif path.path == '/api/config':
                self.reply(200, {'agentKey': app.app_id, 'catalogId': config.definition['catalogId'], 'title': title})
            elif path.path == '/explain':
                sections = ['<h1>' + html.escape(title) + '</h1>',
                    '<p>Saved local definition; inspect native traces to verify execution.</p>',
                    '<pre>' + html.escape(json.dumps(config.definition, indent=2)) + '</pre>']
                root = Path(config.values['definitionFile']).parent.resolve()
                for component in config.definition['components']:
                    contract = root / component['contract']
                    if (contract.is_file() and not contract.is_symlink() and
                            contract.resolve().is_relative_to(root) and contract.stat().st_size <= 2 * 1024 * 1024):
                        sections += ['<h2>' + html.escape(component['contract']) + '</h2>',
                            '<pre>' + html.escape(contract.read_text(encoding='utf-8')) + '</pre>']
                page = '<!doctype html><meta charset="utf-8"><title>Explain agent</title><link rel="stylesheet" href="/style.css"><main>' + ''.join(sections) + '</main>'
                self.reply(200, page.encode(), 'text/html;charset=utf-8')
            else:
                self.reply(404, {'error': 'Unknown gateway path'})

        def do_POST(self):
            path = urlsplit(self.path)
            expected = '/api/apps/' + quote(app.app_id, safe='') + '/a2app'
            if path.path != expected or path.query or not self.authorized(mutation=True):
                self.reply(403, {'error': 'Unauthorized local agent request'})
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 65536 or self.headers.get_content_type() != 'application/json':
                    raise ValueError('Send a bounded application/json request')
                self.connection.settimeout(30)
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError('Incomplete request')
                value = app.invoke(json.loads(raw))
                self.reply(200, value)
            except (ValueError, TypeError, KeyError) as error:
                self.reply(400, {'error': str(error)[:500]})
            except Exception:
                self.reply(502, {'error': 'Native agent request failed; inspect the target runtime and local configuration'})

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    server.daemon_threads = True
    server.application = app
    server.client_token = token
    server.client_url = 'http://127.0.0.1:' + str(server.server_address[1])
    return server


def serve(config, port):
    server = make_server(config, port)
    token_file = private_file(config.state_directory / 'api-token')
    address_file = private_file(config.state_directory / 'server.json')
    token_file.write_text(server.client_token, encoding='utf-8')
    address_file.write_text(json.dumps({'url': server.client_url, 'pid': os.getpid(),
        'bindingFingerprint': config.fingerprint}), encoding='utf-8')
    print('Preview: ' + server.client_url, flush=True)
    print('Explain: ' + server.client_url + '/explain', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        server.application.close()
        if address_file.exists() and json.loads(address_file.read_text()).get('pid') == os.getpid():
            address_file.unlink()
            token_file.unlink(missing_ok=True)
