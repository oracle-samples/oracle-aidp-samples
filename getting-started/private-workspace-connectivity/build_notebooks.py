# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Build seven independent customer notebooks with embedded network diagrams.

Uses only the Python standard library. Generate PNGs with build_diagrams.py
first if a diagram changes. No cloud calls or customer configuration required.
"""
import ast
import base64
import copy
import json
import pprint
from pathlib import Path

ROOT = Path(__file__).parent


def documentation_section(filename, heading):
    """Embed the maintained Markdown section so single-notebook imports work."""
    text = (ROOT / 'docs' / filename).read_text()
    section = text.split('\n## ' + heading + '\n', 1)[1].split('\n## ', 1)[0]
    return section.strip() + '\n'


CASE_HEADINGS = {
    'V01': 'Amazon S3 through customer egress',
    'V02': 'Customer Network Source and service gateway',
    'V03': 'Cross-region Object Storage',
    'V04': 'Database service private endpoint',
    'V05': 'Service endpoint coverage',
    'V06': 'DNS configuration operations',
    'V07': 'Workload continuity',
}
VALIDATION_HEADINGS = {
    'V04': 'Private database baseline and preservation',
    'V05': 'Endpoint coverage and default resolution',
    'V06': 'Self-service configuration lifecycle',
    'V07': 'Apply changes without a Spark restart',
}

DOCS = {
    'workspaces': 'https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html',
    'sgw': 'https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/servicegateway.htm',
    'nat': 'https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/NATgateway.htm',
    'sources': 'https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingnetworksources.htm',
    'adb': 'https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/connect-introduction.html',
    'hdfs': 'https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/hdfsconnector.htm',
}
CASES = {
 'V01': {
  'title': 'Amazon S3 through customer egress', 'file': 'V01_Amazon_S3.ipynb', 'kind': 'csv',
  'purpose': 'Read a small private S3 object from an AIDP workspace through your approved outbound network path.',
  'setup': [
   ('Workspace', 'Enable private network access and select the customer VCN, subnet and applicable NSGs. Confirm the service-required IAM and network prerequisites in the AIDP workspace documentation.'),
   ('Route', 'For the illustrated IPv4 NAT design, the customer subnet has a default route to an existing NAT gateway. Use your approved equivalent egress path if different. This example uses the S3 public regional endpoint, not a private OCI-to-AWS interconnect.'),
   ('DNS', 'Copy the actual regional and bucket-specific hostnames used by your S3 client. Add the required exact destinations in advanced DNS when appropriate. Include redirect destinations observed by the client; do not use a broad domain wildcard.'),
   ('Security', 'Permit outbound HTTPS to the chosen S3 endpoint through subnet rules, NSGs and any firewall; allow return traffic for the connection. No unsolicited inbound internet rule or AWS VPC is required for this example.'),
   ('Service and fixture', 'Use an existing private bucket with a known CSV object no larger than 64 KiB. Configure a supported S3A credential provider and runtime-matched Hadoop/AWS libraries before the comparison; do not embed access keys or change libraries during the test.')],
  'steps': ['Record baseline saved DNS/SCAN settings and the actual S3 client endpoint.', 'Run the baseline fixture read with the same runtime identity you will use afterward.', 'If the destination needs advanced DNS, add only its exact FQDN through Workbench; capture the operation result and reopened saved configuration.', 'Rerun the same reader and compare exact rows. Restore only the entries added for this test.'],
  'acceptance': 'The authenticated S3 read returns the expected rows. Configuration, DNS and optional TCP results are separate. Review route evidence before claiming a specific outbound path.',
  'refs': ['workspaces', 'nat']},
 'V02': {
  'title': 'Bucket and ADB access from the customer VCN', 'file': 'V02_Customer_Network_Source.ipynb', 'kind': 'csv',
  'purpose': 'Read a bucket under an IAM Network Source policy and query an ADB public endpoint under its VCN ACL, through the same customer network.',
  'setup': [
   ('Workspace', 'Attach the private-network-enabled workspace to the VCN/subnet intended by the source restriction.'),
   ('Route', 'Use an existing service gateway in that VCN and enable the supported regional service. Add/verify the regional service-CIDR route in the customer subnet route table and the applicable security rules. A more specific service route must take precedence over unrelated default egress.'),
   ('DNS', 'Use the exact service endpoint your client calls. Configure advanced DNS for the destination when customer-VCN source context is required; verify the saved result and runtime resolution.'),
   ('IAM first', 'Identify the actual Spark reader, create the VCN Network Source and grant read access to the test bucket with request.networkSource.name before executing. Confirm no broader grant bypasses the condition. Keep the policy fixed during DNS comparisons.'),
   ('Evidence', 'Arrange access to policy/route records and correlated target request or network telemetry before testing. A successful read, translated destination IP, or public egress-IP check does not independently establish the service-gateway path.')],
  'steps': ['Capture the existing restriction, intended source network, route and identity label.', 'Read the known small in-region Object Storage CSV before and after the scoped FQDN change, keeping identity and client unchanged.', 'Correlate the operation with source/network evidence while the restriction remains unchanged.', 'Restore the saved DNS baseline and repeat the original control.'],
  'acceptance': 'The fixture read passes under the unchanged restriction AND reviewed independent evidence establishes the intended customer source/path. Keep an ADB ACL variant separately labeled.',
  'refs': ['workspaces', 'sgw', 'sources']},
 'V03': {
  'title': 'Cross-region OCI Object Storage', 'file': 'V03_Cross_Region_Object_Storage.ipynb', 'kind': 'csv',
  'purpose': 'Read a dataset in one OCI region while your AIDP workspace runs in another.',
  'setup': [
   ('Workspace and regions', 'Record your workspace region A and the fixture region B. Choose different regions; the notebook does not prescribe a tenancy or a specific region pair.'),
   ('Route', 'For this public-endpoint example, use an existing customer NAT/approved egress path from region A to the region B endpoint. A region A service gateway only serves supported services in region A; it is not the region B route. A private peering design would need its own verified topology.'),
   ('DNS and client', 'Copy the actual region B Object Storage endpoint and configure its exact FQDN if required. Set the bucket-specific HDFS endpoint property, not an unrelated global endpoint. Confirm the effective endpoint with a fresh filesystem client/session when cached clients could retain earlier values.'),
   ('Security', 'Permit the destination HTTPS path through customer security rules/firewalls. Keep bucket IAM and source restrictions appropriate to the selected identity and route.'),
   ('Fixtures', 'Use an existing region B CSV object no larger than 64 KiB with known rows, and a separate region A control object. Supply each object\'s own namespace, endpoint and expected values.')],
  'steps': ['Fill both regions and the region B fixture; record effective endpoint and baseline configuration.', 'Run the remote read before and after a scoped destination FQDN change.', 'Record a separate region A control read using its own fixture settings.', 'Review effective endpoint/path evidence, then restore only this run\'s FQDN entries.'],
  'acceptance': 'The region B fixture matches the expected rows and the region A control still passes. Configuring a region B endpoint does not by itself prove the client used it.',
  'refs': ['workspaces', 'sgw', 'nat', 'hdfs']},
 'V04': {
  'title': 'Private database connectivity before and after DNS edits', 'file': 'V04_Service_Private_Endpoint.ipynb', 'kind': 'adb',
  'purpose': 'Establish existing private database connectivity, then verify that unrelated Advanced DNS edits preserve the same database connection.',
  'setup': [
   ('Destination', 'Select an existing Autonomous Database private endpoint. Copy its private connection descriptor, hostname, listener port and service name from Database Connection. A regional public hostname protected by an ACL is a different case.'),
   ('Workspace and routing', 'Attach the workspace to the required customer private network. Verify the destination subnet route and return path. For a different VCN, arrange supported local/remote peering or DRG connectivity with non-overlapping ranges and the appropriate route tables.'),
   ('DNS', 'For a private ADB hostname that the service resolver cannot resolve, use default private-endpoint DNS proxy resolution through customer DNS. Do not add an Advanced DNS entry solely because the hostname is private. Verify runtime resolution reaches the intended target. Configure customer resolver/forwarding rules if your topology needs them. Stop on a private/split-DNS mismatch; do not substitute a public descriptor to pass the case.'),
   ('Security', 'Allow the actual listener port through the relevant subnet/NSG/firewall rules and target access configuration, including any service-required workspace rules. NAT/internet routing is not needed for the illustrated private destination path.'),
   ('Authentication', 'Prefer an existing workspace external catalog connection, or provide an existing supported wallet ZIP/TNS plus a private runtime secret. Preserve certificate/server identity checks. A wallet-configured query or port 1522 alone does not prove client-certificate presentation.'),
   ('Unrelated DNS edits', 'Prepare owned A/B hostnames resolvable from the service environment for explicit Advanced DNS provisioning. Follow supported workspace-edit prerequisites. Preserve the database hostname, original DNS/SCAN entries, route, policy and authentication; do not stop another user\'s compute.')],
  'steps': ['Capture the verified private descriptor, network path, expected database identity and complete original DNS/SCAN configuration. Establish override provenance; use a clean approved workspace if existing overrides prevent default-path attribution.', 'Set PHASE="before" and run the read-only identity query without adding an Advanced DNS entry for the private database. Preserve RESULTS for the rest of the run.', 'Add only unrelated hostname A; reopen the saved settings, set PHASE="after_add" and rerun the same database query. Record its configuration snapshot and operation evidence.', 'Replace only A with B, then remove B. Repeat the query with PHASE="after_edit" and PHASE="after_delete", preserving the original database configuration.', 'Restore the original DNS/SCAN configuration, set PHASE="restored" and repeat the query. Complete the five-phase evidence template with the same expected database identity.', 'Review default DNS proxy, private endpoint and any mTLS evidence separately. Use a fresh connection where practical. This case permits supported lifecycle prerequisites; use V07 for the distinct no-restart objective.'],
  'acceptance': 'The intended private database is reachable before and after actual unrelated add/edit/delete operations, all five phases return the same expected database identity, and the original DNS/SCAN settings are restored. The database receives no new explicit DNS entry. Default proxy attribution, private endpoint type and mTLS still require separate evidence review.',
  'refs': ['workspaces', 'adb']},
 'V05': {
  'title': 'OCI service hostname coverage', 'file': 'V05_Service_Hostname_Coverage.ipynb', 'kind': None,
  'purpose': 'Record which of your selected OCI service endpoints can be configured and accessed through the workspace.',
  'setup': [
   ('Inventory', 'Select actual resources you own. Possible coverage includes Autonomous Database, Streaming, OpenSearch, PostgreSQL, Data Flow interactive endpoints and Kafka endpoints. This is a test inventory, not a guarantee of automatic DNS insertion or a service-wide exception policy.'),
   ('Route per service', 'Use each resource\'s real connection descriptor. Verify whether it needs an in-region service gateway, customer egress or a private route. Do not assume all selected services share a gateway or listener port.'),
   ('DNS', 'Use actual resource FQDNs rather than probing only a service-zone root. Classify explicit mappings, regional service-zone allowances and default private DNS proxy behavior separately. Record saved configuration, override provenance and runtime results.'),
   ('Security and service operation', 'Verify each service\'s firewall/NSG and authorization prerequisites and use a small supported read/query operation. Record unavailable fixtures as blocked coverage, not success.'),
   ('Change control', 'Use owned test endpoints and restore only entries introduced for this coverage run. Record any additional support/configuration action required rather than assuming none.')],
  'steps': ['Fill required_services and one actual endpoint record for each selected service.', 'Capture baseline and override provenance per host; classify default private proxy, service-zone permission and explicit mapping separately. Record accepted/rejected operations only where an explicit entry is required.', 'Perform each service\'s existing authenticated operation and reference its evidence.', 'Evaluate the recorded coverage, review endpoint scope, and restore the baseline.'],
  'acceptance': 'Each selected endpoint has configuration and authenticated-operation evidence. Do not generalize an endpoint result to an entire DNS zone or all service instances.',
  'refs': ['workspaces', 'sgw']},
 'V06': {
  'title': 'DNS edits and input validation', 'file': 'V06_DNS_CRUD_And_Validation.ipynb', 'kind': None,
  'purpose': 'Validate scoped add/edit/delete/cancel behavior and record input-validation outcomes.',
  'setup': [
   ('Owned endpoints', 'Prepare two owned disposable hostnames A/B that are resolvable from the service environment for explicit Advanced DNS provisioning and an approved existing route to their small fixtures. For an optional partial-provisioning observation, use an owned deliberately unresolved name U.'),
   ('Route and security', 'Verify A/B are reachable through the workspace\'s existing customer path and permitted ports. DNS edits do not create routes, gateways or authorization. Keep those controls fixed during the comparison.'),
   ('Workspace editing', 'Capture the complete DNS list and SCAN settings first. Follow the deployment\'s edit prerequisites; current public documentation says most network changes require stopped compute. Do not stop another user\'s resource to perform this sample.'),
   ('Input behavior', 'Use the FQDN field\'s published format and current UI validation. Record duplicate/normalization and partially failed-request behavior from actual readback rather than assuming atomic rollback or a universal normalization rule.')],
  'steps': ['Capture the full original configuration and approved A/B test hostnames.', 'Add A; replace only A with B; remove B; try an unsaved edit and cancel. Reopen after each step.', 'Test invalid FQDN forms in the matrix below without saving changes to unrelated settings. Record normalization/duplicate behavior separately.', 'Optionally submit A with unresolved U, inspect operation/readback, and restore the complete baseline regardless of retained entries.'],
  'acceptance': 'Saved snapshots match the expected scoped edits, invalid FQDN forms do not persist, cancel preserves baseline, and final state is restored. Record partial/normalization observations separately.',
  'refs': ['workspaces']},
 'V07': {
  'title': 'DNS changes without a Spark restart', 'file': 'V07_DNS_Without_Restart.ipynb', 'kind': 'csv',
  'purpose': 'Measure workload continuity for a DNS edit that your deployment explicitly supports while compute is running.',
  'setup': [
   ('Deployment capability', 'Confirm this particular DNS operation is supported without stopping compute in your deployment. Public documentation says most network changes require stopped clusters. If the UI requires a stop or the operation is unsupported, mark this case BLOCKED; do not bypass that requirement.'),
   ('Network baseline', 'Use owned, service-resolvable public A/B hostnames suitable for explicit Advanced DNS provisioning and an existing working customer route, security rules and authenticated fixture. Private-only names exercise the default proxy path instead. Keep gateways, listener ports, identity and service policy unchanged across phases.'),
   ('DNS evidence', 'Capture authoritative workspace mapping/readback for each phase. Record convergence time as observed; no universal propagation deadline is assumed. Do not copy a translated address from another workspace.'),
   ('Runtime evidence', 'Use the same dedicated Spark session. Record application ID, SELECT 1, workload result and independent compute lifecycle evidence before/add/edit/delete/restored. A stable display name alone is insufficient.')],
  'steps': ['First verify the capability prerequisite and capture the before phase.', 'For each permitted add/edit/delete operation, keep the same session and record the reader, DNS mapping and application identity.', 'Capture lifecycle events across the entire run, including any stop/start/replacement.', 'Restore the original configuration. If a prerequisite adoption restart is needed, handle it outside this measured run and start a separate test afterward.'],
  'acceptance': 'Actual DNS changes converge, every workload phase passes, the Spark application is unchanged and lifecycle evidence shows no restart. Otherwise record the observed failure or prerequisite.',
  'refs': ['workspaces']},
}

UNKNOWN = object()
def static(node, case):
    if isinstance(node, ast.Name) and node.id == 'case': return case
    try: return ast.literal_eval(node)
    except (ValueError, TypeError): pass
    if isinstance(node, ast.Compare) and len(node.ops) == 1:
        a, b = static(node.left, case), static(node.comparators[0], case)
        if a is UNKNOWN or b is UNKNOWN: return UNKNOWN
        op = node.ops[0]
        if isinstance(op, ast.Eq): return a == b
        if isinstance(op, ast.NotEq): return a != b
        if isinstance(op, ast.In): return a in b
        if isinstance(op, ast.NotIn): return a not in b
    if isinstance(node, ast.BoolOp):
        values = [static(v, case) for v in node.values]
        if isinstance(node.op, ast.And) and any(v is False for v in values): return False
        if isinstance(node.op, ast.Or) and any(v is True for v in values): return True
        if all(v is not UNKNOWN for v in values):
            return all(values) if isinstance(node.op, ast.And) else any(values)
    return UNKNOWN

class Specialize(ast.NodeTransformer):
    def __init__(self, case): self.case = case
    def visit_If(self, node):
        node = self.generic_visit(node)
        value = static(node.test, self.case)
        return node.body if value is True else node.orelse if value is False else node

class V02Specialize(Specialize):
    """Keep only the OCI CSV path in the standalone Network Source notebook."""
    def visit_Name(self, node):
        if node.id == 'fixture_role' and isinstance(node.ctx, ast.Load):
            return ast.Constant('primary')
        return node

    def visit_Call(self, node):
        node = self.generic_visit(node)
        if (isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
                and node.args and isinstance(node.args[0], ast.Constant)):
            if (node.func.value.id, node.func.attr, node.args[0].value) == ('cfg', 'get', 'kind'):
                return ast.Constant('csv')
            if (node.func.value.id, node.func.attr, node.args[0].value) == ('uri', 'startswith', 's3a://'):
                return ast.Constant(False)
        return node

    def visit_Constant(self, node):
        replacements = {
            'Use s3a:// or oci://': 'Use an oci:// bucket@namespace/object URI',
            'V02 requires a bucket in the workspace region; use V03 for a remote bucket':
                'The bucket must be in the workspace region',
        }
        if isinstance(node.value, str) and node.value in replacements:
            return ast.Constant(replacements[node.value])
        return node

def engine(case, title):
    source = ast.parse((ROOT / 'connectivity_checks.py').read_text())
    nodes = []
    for item in source.body:
        if isinstance(item, (ast.Import, ast.ImportFrom, ast.ClassDef)):
            nodes.append(copy.deepcopy(item))
        elif isinstance(item, ast.FunctionDef) and item.name != 'main':
            if case == 'V02':
                if item.name not in ('utc', 'canonical', 'endpoint_host', 'fixture_read', 'run_case', 'summarize'):
                    continue
                nodes.append(V02Specialize(case).visit(copy.deepcopy(item)))
                continue
            if item.name in ('run_case', 'fixture_read', 'adb_read') and case in ('V05', 'V06'): continue
            if item.name == 'evaluate_evidence' and case in ('V01', 'V02', 'V03'): continue
            nodes.append(Specialize(case).visit(copy.deepcopy(item)))
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    return 'CASE_NAMES = ' + repr({case: title}) + '\n\n' + ast.unparse(module) + '\n'

def configuration(kind):
    cfg = {'workspace_label': '', 'compute_label': '', 'hostname': '', 'probe_tcp': False, 'port': 443}
    if kind == 'csv':
        cfg.update(kind='csv', uri='', endpoint='', region='', workspace_region='', fixture_size_bytes=0,
                   expected_rows=[{'id': '1', 'label': 'alpha'}, {'id': '2', 'label': 'beta'}])
    elif kind == 'adb':
        cfg.update(kind='adb', port=1522, catalog_id='', wallet_zip='', tns='', user='', expected_instance='')
    return cfg

def evidence(case):
    ev = {'evidence_ref': None}
    if case == 'V02':
        ev.update(runtime_principal_id='', dynamic_group_rule_ref='', network_source_name='',
                  network_source_snapshot_ref='', bucket_policy_snapshot_ref='',
                  effective_grants_review_ref='', route_snapshot_ref='',
                  dns_before_ref='', dns_after_ref='', dns_restored_ref='',
                  read_request_refs=[], adb_public_descriptor_ref='', adb_acl_snapshot_ref='',
                  adb_session_refs=[], outside_source_comparison_refs=[])
    if case == 'V05':
        ev.update(required_services=[], services=[])
    if case == 'V06':
        ev.update(before=None, restored=None, steps=[{'name': name, 'actual_snapshot': None, 'expected_snapshot': None, 'work_request_ref': None, 'ui_evidence_ref': None} for name in ('add', 'edit', 'delete', 'cancel')])
        ev['steps'] += [{'name': name, 'rejected': None, 'actual_snapshot': None, 'ui_evidence_ref': None} for name in ('blank', 'underscore', 'path', 'query', 'fragment', 'malformed')]
        ev['steps'] += [{'name': 'partial', 'performed': False, 'work_request_ref': None, 'work_request_status': None, 'saved_hostnames': [], 'readback_evidence_ref': None}]
        ev['normalization_observations'] = []
    if case in ('V04', 'V07'):
        ev.update(before=None, restored=None, phases=[{'phase': phase, 'functional': None, 'operation_evidence_ref': None, 'application_id': None, 'configuration_snapshot': None, 'instance': None, 'dns_addresses': [], 'expected_dns_addresses': [], 'mapping_evidence_ref': None} for phase in ('before', 'after_add', 'after_edit', 'after_delete', 'restored')])
    if case == 'V07': ev.update(restart_count=None, lifecycle_evidence_ref=None)
    if case == 'V04': ev['expected_instance'] = ''
    return ev

def md(text, attachments=None):
    cell = {'cell_type': 'markdown', 'metadata': {}, 'source': text.splitlines(True)}
    if attachments: cell['attachments'] = attachments
    return cell

def code(text, hidden=False):
    return {'cell_type': 'code', 'execution_count': None, 'outputs': [], 'metadata': {'jupyter': {'source_hidden': True}} if hidden else {}, 'source': text.splitlines(True)}

def v02_session_guard(helper, config):
    """Explain how to recover when cells run out of order or the session restarts."""
    return (
        'if not all(callable(globals().get(name)) for name in ' + repr((helper, 'summarize')) + '):\n'
        '    raise RuntimeError(\'V02 is not initialized in this session. Run the code cell under '
        '"Initialize V02" and wait for "V02 initialized". Then rerun this cell. '
        'After a session restart, also rerun "Your configuration" and "Choose phase".\')\n'
        'if any(name not in globals() for name in ' + repr(('CASE_ID', config, 'RUN_LIVE', 'PHASE', 'RESULTS')) + '):\n'
        '    raise RuntimeError(\'V02 configuration is missing. Complete and run "Your configuration", '
        'then "Choose phase", before running this cell.\')\n\n'
    )

def v02_walkthrough(diagram):
    """Build this use case without the multi-case reference appendices."""
    guide = 'v02-network-source.md'
    section = lambda heading: documentation_section(guide, heading)
    intro = (ROOT / 'docs' / guide).read_text().split('\n## ', 1)[0]
    cells = [md(intro + '\n\nThis notebook is an unexecuted sample.\n')]
    for heading in ('How the source restrictions work', 'Before you begin', 'IAM and database setup', 'Network setup'):
        cells.append(md('## ' + heading + '\n\n' + section(heading)))
    cells += [diagram, md('## Customer steps\n\n' + section('Customer steps')),
        md('## Initialize V02\n\n**Required:** attach your compute and run the code cell immediately below. '
           'Wait for **V02 initialized** before continuing to **Your configuration**. '
           'This cell defines the bucket and database checks; it makes no network calls. '
           'Run it again after a session restart or compute reattachment. '
           'Rerunning initialization preserves your configuration and collected results.\n'),
        code(engine('V02', CASES['V02']['title']) + '\n' + (ROOT / 'v02_adb_check.py').read_text() + '\n'
             'print("V02 initialized. Next: Your configuration, Choose phase, then the bucket and database cells.")\n'),
        md('## Your configuration\n\nComplete the setup first. CONFIG selects the new bucket fixture; '
           'ADB_CONFIG selects the existing database public-endpoint connection. Set each target region '
           'to the workspace region. The expected CSV rows match sample.csv. Copy expected_service '
           'from the selected ADB descriptor. Keep credentials out of both dictionaries.\n'),
        code("CASE_ID = 'V02'\nCONFIG = " + pprint.pformat(configuration('csv'), sort_dicts=False) + '\n'
             'ADB_CONFIG = ' + pprint.pformat(dict(workspace_label='', compute_label='',
                 hostname='', port=1522, region='', workspace_region='', catalog_id='',
                 wallet_zip='', tns='', user='', expected_service='', public_endpoint_confirmed=False),
                 sort_dicts=False) + '\n'
             'RUN_LIVE = False\nRUN_EVIDENCE_CHECKS = False\nPHASE = "before"\nRESULTS = []\n'),
        md('## Choose phase\n\nChange only the phase and live flag between attempts. Keep earlier RESULTS.\n'),
        code('RUN_LIVE = False  # Set True after completing IAM and network setup.\n'
             'PHASE = "before"  # Then after_add and restored, following Customer steps.\n'),
        md('## Read the bucket object\n\nRun this cell once per phase. The same configured runtime identity '
           'must read the same object under the unchanged policy and Network Source.\n'),
        code(v02_session_guard('run_case', 'CONFIG') +
             'result = run_case(CASE_ID, CONFIG, spark=globals().get("spark"), live=RUN_LIVE, phase=PHASE)\n'
             'result["target"] = "bucket"\n'
             'result["acceptance"] = summarize(result)\nRESULTS.append(result)\nprint(json.dumps(result, indent=2))\n'),
        md('## Query the database\n\nRun once per phase using the same approved public-endpoint connection. '
           'The query reads DUAL and returns service, database user and the client address observed by ADB. '
           'A catalog supplies its own credentials; the wallet alternative reads ADB_PASSWORD privately '
           'from the runtime environment. This checks the ADB VCN ACL path, not an IAM Network Source condition.\n'),
        code(v02_session_guard('run_adb_check', 'ADB_CONFIG') + 'import os\n'
             'adb_result = run_adb_check(ADB_CONFIG, spark=globals().get("spark"), '
             'runtime={"adb_password": os.environ.get("ADB_PASSWORD")}, live=RUN_LIVE, phase=PHASE)\n'
             'adb_result["acceptance"] = summarize(adb_result)\nRESULTS.append(adb_result)\n'
             'print(json.dumps(adb_result, indent=2))\n'),
        md('## Review the result\n\n' + section('Review the result') + '\n'
           '`functional: PASS` confirms the CSV rows or expected database service for the named target. '
           '`source_path: BLOCKED` means that the policy/ACL and path evidence still needs human review. '
           'The following references document that review; '
           'they do not automatically query or validate IAM.\n'),
        code('EVIDENCE = ' + pprint.pformat(evidence('V02'), sort_dicts=False) + '\n'),
        md('## Export\n\nSave this output privately with your configuration snapshots and request records.\n'),
        code('print(json.dumps({"case": CASE_ID, "generated_utc": utc(), "results": RESULTS, '
             '"evidence": EVIDENCE}, indent=2))\n'),
        md('## References\n\n' + section('References').replace('../../../LICENSE.txt', '../../LICENSE.txt'))]
    return cells

def customer_walkthrough(case, cells):
    """Put customer actions and executable cells before reference appendices."""
    if case == 'V02':
        diagram = next(c for c in cells if 'network.png' in c.get('attachments', {}))
        return v02_walkthrough(diagram)
    headings = {
        'V01': 'V01 - Amazon S3',
        'V02': 'V02 - Customer Network Source',
        'V03': 'V03 - Cross-region Object Storage',
        'V04': 'V04 - Private database baseline and preservation',
    }
    if case not in headings:
        return cells
    source = lambda c: ''.join(c['source'])
    title = cells[0]
    title['source'] = (source(title).replace('[Choose another use case](README.md)',
                                            '[Start here](00_START_HERE.ipynb)')).splitlines(True)
    # Keep setup/reference material available, without interrupting cell execution.
    appendix, executable = [], []
    for cell in cells[1:]:
        text = source(cell)
        if text.startswith(('## Advanced DNS objective', '## When do you need',
                            '## Network setup', '## IAM and network examples')):
            appendix.append(cell)
        elif not text.startswith('## Test procedure'):
            executable.append(cell)
    diagram = next(c for c in executable if 'network.png' in c.get('attachments', {}))
    executable.remove(diagram)
    procedure = md('## Customer steps\n\n' + documentation_section(
        'customer-walkthrough.md', headings[case]))
    common = md('## Workbench actions and output\n\n' + documentation_section(
        'customer-walkthrough.md', 'Common Workbench actions'))
    # Phase changes must not reset the accumulated observations.
    run_index = next(i for i, c in enumerate(executable)
                     if source(c).startswith('## Run a workload phase'))
    phase = [md('## Choose phase\n\nRun this cell before each workload attempt. '
                'Change only the phase and live flag here; keep earlier RESULTS. '
                'Do not use Run all across manual workspace changes.\n'),
             code('RUN_LIVE = False  # Set True only after completing the prerequisites.\n'
                  'PHASE = "before"  # Follow the phase sequence in Customer steps.\n')]
    if case == 'V03':
        phase = [md('## In-region control configuration\n\nFill this independent fixture once. '
                    'Its region must equal the workspace region. Do not reuse the remote URI.\n'),
                 code('CONTROL_CONFIG = ' + pprint.pformat(configuration('csv'), sort_dicts=False) + '\n')] + phase
    executable[run_index:run_index] = phase
    if case == 'V03':
        run_index = next(i for i, c in enumerate(executable)
                         if source(c).startswith('import os\nRUNTIME'))
        executable[run_index + 1:run_index + 1] = [
            md('## Run the in-region control\n\nRun after the remote workload for every phase. '
               'This is an independent V03 control read, '
               'not a claim that an IAM Network Source was tested.\n'),
            code('if RUN_LIVE and (not CONTROL_CONFIG.get("region") or\n'
                 '                 CONTROL_CONFIG["region"] != CONFIG.get("workspace_region") or\n'
                 '                 CONTROL_CONFIG.get("workspace_region") != CONFIG.get("workspace_region") or\n'
                 '                 CONTROL_CONFIG["uri"] == CONFIG.get("uri")):\n'
                 '    raise ValueError("Choose a separate fixture in the workspace region")\n'
                 'control = run_case(CASE_ID, CONTROL_CONFIG, spark=globals().get("spark"),\n'
                 '                   live=RUN_LIVE, phase=PHASE + "_local_control", fixture_role="local_control")\n'
                 'control["acceptance"] = summarize(control)\n'
                 'RESULTS.append(control)\n'
                 'print(json.dumps(control, indent=2))\n')]
    return [title, procedure, diagram, common] + executable + [
        md('## Reference appendix\n\nUse the following network, IAM and DNS details when preparing the environment.\n')] + appendix

def build():
    index = []
    for case, item in CASES.items():
        network = '\n'.join(f'| {topic} | {detail} |' for topic, detail in item['setup'])
        image = base64.b64encode((ROOT / 'assets' / (case.lower() + '-network.png')).read_bytes()).decode()
        cells = [md(f'# {case}: {item["title"]}\n\n{item["purpose"]}\n\n[Choose another use case](README.md). This notebook is an unexecuted sample, not a live validation report.\n'),
                 md('## Network setup\n\nComplete and verify these prerequisites with your network/service administrator before the workload comparison. The notebook does not create routes, change policies, configure DNS, or start compute.\n\n| Area | Required setup and verification |\n| --- | --- |\n' + network + '\n'),
                 md('### Example network topology\n\n![Conceptual network path for ' + case + '](attachment:network.png)\n\nThe diagram is illustrative; it does not prove the route taken by a request.\n', {'network.png': {'image/png': image}}),
                 md('## Test procedure\n\n' + '\n'.join(f'{i+1}. {step}' for i, step in enumerate(item['steps'])) + '\n\n**Acceptance:** ' + item['acceptance'] + '\n'),
                 md('## Test helpers\n\nRun once to define this case\'s checks. Definitions make no network calls. AIDP uses Spark for the data-operation cells; no separate helper file is required.\n'), code(engine(case, item['title']), True),
                 md('## Your configuration\n\nFill the empty values from your own environment. Labels identify your evidence; they do not attach this notebook to a workspace or compute. The example CSV rows match fixtures/sample.csv. For a different small fixture, supply its known size and exact rows. Copy the database port from its actual descriptor; 1522 below is only a starting value.\n\nUse an existing catalog connection for ADB when available. Otherwise provide an existing supported wallet ZIP/TNS and a private runtime password; a wallet directory is not a ZIP file. Do not put passwords or access keys in CONFIG.\n'),
                 code('CASE_ID = ' + repr(case) + '\nCONFIG = ' + pprint.pformat(configuration(item['kind']), sort_dicts=False) + '\nRUN_LIVE = False\nRUN_EVIDENCE_CHECKS = False\nPHASE = "' + ('before' if case == 'V04' else 'baseline') + '"\nRESULTS = []\n')]
        preparation = ('## IAM and network examples\n\n'
                       'Preparation templates only: replace every angle-bracket placeholder privately. '
                       'An administrator prepares the approved resources; this notebook applies no policies or network changes.\n\n'
                       + documentation_section('network-and-iam-examples.md', 'Shared prerequisites')
                       + ('' if case == 'V02' else '\n' + documentation_section(
                           'network-and-iam-examples.md', case + ' — ' + CASE_HEADINGS[case])))
        # Include referenced preparation patterns in notebooks imported on their own.
        related = {'V04': ('V06',), 'V05': ('V02', 'V03', 'V04'),
                   'V06': ('V02',), 'V07': ('V02', 'V03')}
        for reference in related.get(case, ()):
            preparation += ('\n### Supporting pattern: ' + reference + '\n\n'
                            + documentation_section('network-and-iam-examples.md',
                                                    reference + ' — ' + CASE_HEADINGS[reference]))
        resolution = documentation_section('dns-resolution-paths.md', 'Resolution paths')
        cells.insert(1, md('## When do you need an Advanced DNS entry?\n\n' + resolution))
        cells.insert(4, md(preparation))
        if case in VALIDATION_HEADINGS:
            cells.insert(1, md('## Advanced DNS objective and coverage limits\n\n'
                              + documentation_section('advanced-dns-validation.md',
                                                      case + ' — ' + VALIDATION_HEADINGS[case])))
        if item['kind']:
            cells += [md('## Run a workload phase\n\nUse the same identity and client before and after a supported change. Only set RUN_LIVE=True when prerequisites are ready. For subsequent phases, change PHASE and rerun this cell without reinitializing RESULTS. The runtime password hook reads an already-provisioned environment variable; it never prints its value. The sample does not configure the S3A credential provider or install connector libraries.\n'),
                      code('import os\nRUNTIME = {}\nif os.environ.get("ADB_PASSWORD"):\n    RUNTIME["adb_password"] = os.environ["ADB_PASSWORD"]\nresult = run_case(CASE_ID, CONFIG, spark=globals().get("spark"), runtime=RUNTIME, live=RUN_LIVE, phase=PHASE)\nresult["acceptance"] = summarize(result)\nRESULTS.append(result)\nprint(json.dumps(result, indent=2))\n')]
        if case == 'V06':
            cells += [md('### Input/CRUD observations\n\nUse your owned A/B hostnames. For add/edit/delete, record actual and expected complete snapshots including DNS list, SCAN settings and workspace label. For cancel, record UI evidence and unchanged baseline.\n\n| Input | Expected check |\n| --- | --- |\n| Blank entry | Rejected with baseline unchanged |\n| Underscore in a hostname label | Rejected with baseline unchanged |\n| URL containing a path, query or fragment | Rejected as a non-FQDN input; baseline unchanged |\n| Malformed name containing an empty middle label | Rejected with baseline unchanged |\n| Duplicate, case, trailing dot or host-only URL | Record actual UI normalization/rejection; do not assume a universal outcome |\n| Optional valid and unresolved names together | Record operation result and actual retained entries; do not assume atomic rollback |\n')]
        if case == 'V05':
            cells += [md('### Service observation format\n\nSelect your own required_services labels. Add one service record per selected resource using this shape. A missing fixture is blocked coverage. Accepted DNS configuration is not an authenticated read.\n'),
                      code('SERVICE_RECORD_EXAMPLE = {"service": "", "hostname": "", "configuration_accepted": None, "saved_config_ref": None, "functional": None, "operation_evidence_ref": None, "additional_configuration_notes": ""}\n')]
        cells += [md('## Evidence and review\n\nKeep each earlier phase and its UTC timestamp. Reference saved configuration, work requests, mappings and independent source/lifecycle records. Evidence snapshots contain the complete dns_zones, scan and workspace_label fields. Do not share credentials, signed URLs, wallet contents or customer identifiers in repository issues. Raw connector exception messages are omitted because they can contain secrets.\n\nDNS, TCP, authenticated access, private endpoint type, source path, mTLS and application continuity are separate results. A successful wallet-configured query does not independently establish client-certificate presentation. Review the applicable external evidence before accepting the complete case.\n'), code('EVIDENCE = ' + pprint.pformat(evidence(case), sort_dicts=False) + '\n')]
        if case in ('V04', 'V05', 'V06', 'V07'):
            cells += [code('if RUN_EVIDENCE_CHECKS:\n    observed = evaluate_evidence(CASE_ID, EVIDENCE)\n    observed["acceptance"] = summarize(observed)\n    RESULTS.append(observed)\n    print(json.dumps(observed, indent=2))\nelse:\n    print("Evidence assertions NOT_RUN; capture real observations first.")\n')]
        cells += [md('## Restore and export\n\nRestore only settings introduced by this run, reopen the saved baseline and repeat the original control operation. Preserve original routes, security policies, wallet/authentication and unrelated DNS/SCAN entries. Manage resource lifecycle according to your own test plan; do not stop compute that was already in use.\n\nExport the JSON below under a new run-specific name and review it privately. No automatic overall PASS is produced: a successful check does not replace the remaining case evidence.\n'),
                  code('print(json.dumps({"case": CASE_ID, "generated_utc": utc(), "results": RESULTS}, indent=2))\n'),
                  md('## Public references\n\n' + '\n'.join('- [' + key + '](' + DOCS[key] + ')' for key in item['refs']) + '\n\nCopyright (c) 2026 Oracle and/or its affiliates. Licensed under the [Universal Permissive License](../../LICENSE.txt), Version 1.0.\n')]
        cells = customer_walkthrough(case, cells)
        for i, cell in enumerate(cells): cell['id'] = case.lower() + '-' + str(i)
        nb = {'cells': cells, 'metadata': {'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}, 'language_info': {'name': 'python'}}, 'nbformat': 4, 'nbformat_minor': 5}
        (ROOT / item['file']).write_text(json.dumps(nb, indent=2) + '\n')
        index.append({'case': case, 'title': item['title'], 'notebook': item['file'], 'objective': item['purpose'], 'kind': item['kind']})
    (ROOT / 'case-index.json').write_text(json.dumps(index, indent=2) + '\n')
    intro = (ROOT / 'docs' / 'customer-walkthrough.md').read_text().split('\n## V01 -', 1)[0]
    for item in index[:4]:
        intro = intro.replace('| ' + item['notebook'] + ' |', '| [' + item['notebook'] + '](' + item['notebook'] + ') |')
    guide = {'cells': [md(intro + '\n\n[Full tester guide](README.md).\n')],
             'metadata': {'language_info': {'name': 'python'}}, 'nbformat': 4, 'nbformat_minor': 5}
    guide['cells'][0]['id'] = 'start-here'
    (ROOT / '00_START_HERE.ipynb').write_text(json.dumps(guide, indent=2) + '\n')
    print('Built seven customer notebooks with embedded network diagrams.')

if __name__ == '__main__':
    build()
