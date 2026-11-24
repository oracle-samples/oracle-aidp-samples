"""OCI CLI signs only the configured agent endpoint; credentials stay in OCI config."""
import json
from pathlib import Path
import re
import subprocess
import tempfile

MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class OciTransport:
    def __init__(self, config, run=subprocess.run):
        self.config, self.run = config, run

    def __call__(self, method, url, body=None, *, headers=None):
        endpoint = self.config.endpoint
        card = self.config.values.get('agentCardUrl')
        if method == 'GET':
            if not card or url != card or body is not None:
                raise ValueError('Only the configured advertised Agent Card may be retrieved')
        elif method == 'POST':
            if (url != endpoint or not isinstance(body, dict) or body.get('jsonrpc') != '2.0'
                    or body.get('method') not in {'message/send', 'tasks/get'}):
                raise ValueError('Only configured A2A message/send and tasks/get requests are permitted')
        else:
            raise ValueError('Unsupported agent transport method')
        allowed = {'x-aidp-a2a-interrupts': 'true',
                   'X-A2A-Extensions': 'https://a2ui.org/a2a-extension/a2ui/v0.8'}
        if headers and any(key not in allowed or allowed[key] != value for key, value in headers.items()):
            raise ValueError('Unsupported agent capability header')
        values = self.config.values
        command = ['oci', '--profile', values['ociProfile'], '--auth', values['authType'],
                   '--region', values['region'], 'raw-request', '--http-method', method,
                   '--target-uri', url, '--output', 'json', '--connection-timeout', '10',
                   '--read-timeout', '180', '--max-retries', '0']
        if values.get('ociConfigFile'):
            command[1:1] = ['--config-file', str(Path(values['ociConfigFile']).expanduser())]
        if headers:
            command += ['--request-headers', json.dumps(headers)]
        with tempfile.TemporaryDirectory(prefix='aidp-agent-request-') as directory:
            if body is not None:
                request = Path(directory) / 'request.json'
                request.write_text(json.dumps(body, allow_nan=False), encoding='utf-8')
                request.chmod(0o600)
                command += ['--request-body', 'file://' + str(request)]
            try:
                result = self.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                  timeout=200, check=False)
            except FileNotFoundError:
                raise ValueError('OCI CLI is required; install it and configure the selected profile') from None
            except (OSError, subprocess.TimeoutExpired):
                raise ValueError('Agent request timed out; inspect status before retrying a process action') from None
        if result.returncode:
            # CLI diagnostics can contain signed headers; never relay them.
            diagnostic = result.stderr[:MAX_RESPONSE_BYTES] + result.stdout[:MAX_RESPONSE_BYTES]
            if re.search(rb'(?i)(expired|notauthenticated|security.token)', diagnostic):
                raise ValueError('OCI authentication is unavailable or expired; renew the configured profile')
            raise ValueError('OCI CLI agent request failed; inspect OCI configuration and service status')
        if len(result.stdout) > MAX_RESPONSE_BYTES:
            raise ValueError('Agent response exceeds the client byte limit')
        try:
            packet = json.loads(result.stdout)
            status = int(str(packet.get('status', '')).split()[0])
            data = packet.get('data')
            if isinstance(data, str):
                data = json.loads(data)
        except (ValueError, TypeError, IndexError):
            raise ValueError('OCI CLI returned an invalid agent response') from None
        if not 200 <= status < 300:
            raise ValueError('Agent endpoint returned HTTP ' + str(status))
        if not isinstance(data, dict):
            raise ValueError('Agent response must be a JSON object')
        return status, data
