# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Read V02 network configuration from an administrator's OCI CLI environment.

Run outside the notebook. Uses an existing API-key or security-token profile;
does not create resources, edit routes, or read credential values into output.
Dependencies: oci and requests.
"""
import argparse
import datetime
import json
import os
from pathlib import Path


class SnapshotError(Exception):
    """An actionable, credential-free configuration error."""


def collect(args):
    import oci
    import requests
    cfg = oci.config.from_file(profile_name=args.profile)
    cfg['region'] = args.region
    if cfg.get('security_token_file'):
        auth = oci.auth.signers.SecurityTokenSigner(
            Path(cfg['security_token_file']).expanduser().read_text().strip(),
            oci.signer.load_private_key_from_file(cfg['key_file'], cfg.get('pass_phrase')))
    else:
        auth = oci.signer.Signer(cfg['tenancy'], cfg['user'], cfg['fingerprint'],
                                 cfg.get('key_file'), pass_phrase=cfg.get('pass_phrase'))
    base = oci.regions.endpoint_for('aidp', args.region,
        service_endpoint_template='https://aidp.{region}.oci.{secondLevelDomain}')
    url = base + '/20240831/dataLakes/' + args.instance_id + '/workspaces/' + args.workspace_id
    response = requests.get(url, auth=auth, timeout=40)
    if response.status_code != 200:
        raise SnapshotError('Workspace read failed: HTTP ' + str(response.status_code))
    workspace = response.json()
    network = workspace['networkConfigurationDetails']
    client = oci.core.VirtualNetworkClient(cfg, signer=auth)
    subnet = oci.util.to_dict(client.get_subnet(network['subnetId']).data)
    table = oci.util.to_dict(client.get_route_table(subnet['route_table_id']).data)
    routes = [r for r in table['route_rules'] if r['destination_type'] == 'SERVICE_CIDR_BLOCK'
              and r['network_entity_id'].startswith('ocid1.servicegateway.')]
    if len(routes) != 1:
        raise SnapshotError('Expected one direct service-gateway route; review this topology manually')
    gateway = oci.util.to_dict(client.get_service_gateway(routes[0]['network_entity_id']).data)
    services = oci.pagination.list_call_get_all_results(client.list_services).data
    service = next((oci.util.to_dict(s) for s in services if s.cidr_block == routes[0]['destination']), None)
    if not service:
        raise SnapshotError('Route does not match a service CIDR label in the selected region')
    identity = oci.identity.IdentityClient(cfg, signer=auth)
    source = oci.util.to_dict(identity.get_network_source(args.network_source_id).data)
    fields = lambda item, names: {name: item.get(name) for name in names}
    return {
        'captured_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'region': args.region,
        'workspace': {'id': args.workspace_id, 'display_name': workspace.get('displayName'),
                      'subnet_id': network['subnetId'], 'dns_names': network.get('dnsZones', [])},
        'subnet': fields(subnet, ('id', 'display_name', 'vcn_id', 'cidr_block', 'route_table_id')),
        'route_table': fields(table, ('id', 'display_name', 'route_rules')),
        'service_gateway': fields(gateway, ('id', 'display_name', 'vcn_id', 'block_traffic', 'lifecycle_state', 'services')),
        'service': fields(service, ('id', 'name', 'cidr_block')),
        'network_source': fields(source, ('id', 'name', 'public_source_list', 'virtual_source_list', 'services')),
        'scope': 'Configuration snapshot only. No traffic, effective IAM grants, or runtime source were verified.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', default='DEFAULT')
    for name in ('region', 'instance-id', 'workspace-id', 'network-source-id', 'output'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    try:
        snapshot = collect(args)
    except SnapshotError as exc:
        print(str(exc))
        raise SystemExit(1)
    except Exception as exc:
        # Do not echo arbitrary SDK/HTTP messages or credential material.
        print('Snapshot failed: ' + type(exc).__name__ + '; HTTP status: ' + str(getattr(exc, 'status', 'not available')))
        raise SystemExit(1)
    with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(snapshot, stream, indent=2)
        stream.write('\n')
    print('Saved configuration snapshot to ' + args.output + '. This does not prove runtime routing.')


if __name__ == '__main__':
    main()
