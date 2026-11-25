"""Explicit, non-secret client configuration; no repository or tenancy discovery."""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from a2app_protocol import validate_definition


def private_directory(path):
    path = Path(path).expanduser().absolute()
    if path.is_symlink():
        raise ValueError('stateDirectory must not be a symbolic link')
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.is_dir() or (os.name == 'posix' and
            (path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077)):
        raise ValueError('stateDirectory must be a private directory owned by this user (mode 700)')
    return path


def private_file(path):
    path = Path(path)
    if not path.exists():
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
    if path.is_symlink() or not path.is_file() or (os.name == 'posix' and
            (path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077)):
        raise ValueError('Client state files must be private regular files owned by this user (mode 600)')
    return path


@dataclass
class Configuration:
    values: dict
    definition: dict
    state_directory: Path

    @property
    def endpoint(self):
        return self.values['endpoint'].rstrip('/')

    @property
    def revision(self):
        return self.values['runtimeRevision']

    @property
    def binding(self):
        return {key: self.values.get(key) for key in
                ('endpoint', 'agentKey', 'workspaceKey', 'runtimeRevision', 'ociProfile',
                 'region', 'authType', 'ociConfigFile')} | {'definition': self.definition}

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.binding, sort_keys=True).encode()).hexdigest()


def load_configuration(filename):
    filename = Path(filename).expanduser().resolve()
    values = json.loads(filename.read_text(encoding='utf-8'))
    required = {'endpoint', 'agentKey', 'workspaceKey', 'runtimeRevision', 'definitionFile',
                'stateDirectory', 'ociProfile', 'region'}
    optional = {'authType', 'ociConfigFile', 'expectedHostname', 'agentCardUrl', 'title'}
    if not isinstance(values, dict) or not required <= values.keys() or set(values) - required - optional:
        raise ValueError('Configuration requires exactly the documented target and client fields')
    for key in required:
        if not isinstance(values[key], str) or not values[key].strip():
            raise ValueError(key + ' must be a nonempty string')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', values['agentKey']):
        raise ValueError('agentKey must be an identifier, not a path')
    endpoint = urlsplit(values['endpoint'])
    expected = values.get('expectedHostname')
    if expected is not None and (not isinstance(expected, str) or
            not re.fullmatch(r'[A-Za-z0-9.-]+', expected)):
        raise ValueError('expectedHostname must be an exact hostname')
    hostname_ok = bool(endpoint.hostname and
        (endpoint.hostname.endswith('.oraclecloud.com') or
         expected and endpoint.hostname == expected.lower()))
    if (endpoint.scheme != 'https' or not hostname_ok or endpoint.port not in (None, 443)
            or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment
            or not endpoint.path.rstrip('/').endswith('/a2a')):
        raise ValueError('endpoint must be an advertised HTTPS A2A endpoint on the approved OCI hostname')
    if 'agentCardUrl' in values:
        card = urlsplit(values['agentCardUrl'])
        if (card.scheme != 'https' or card.netloc != endpoint.netloc or card.username or
                card.password or card.query or card.fragment or not card.path.startswith(endpoint.path.rstrip('/') + '/')):
            raise ValueError('agentCardUrl must be advertised under the configured A2A endpoint')
    values.setdefault('authType', 'security_token')
    if values['authType'] not in {'api_key', 'security_token', 'instance_principal', 'resource_principal'}:
        raise ValueError('Unsupported OCI authentication type')
    definition_file = Path(values['definitionFile']).expanduser()
    if not definition_file.is_absolute():
        definition_file = filename.parent / definition_file
    if definition_file.is_symlink() or not definition_file.is_file() or definition_file.stat().st_size > 2 * 1024 * 1024:
        raise ValueError('definitionFile must be a bounded regular JSON file')
    definition = validate_definition(json.loads(definition_file.read_text(encoding='utf-8')))
    values['definitionFile'] = str(definition_file.resolve())
    state_directory = Path(values['stateDirectory']).expanduser()
    if not state_directory.is_absolute():
        state_directory = filename.parent / state_directory
    return Configuration(values, definition, private_directory(state_directory))
