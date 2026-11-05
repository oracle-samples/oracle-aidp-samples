# Copyright (c) 2026 Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Build four independent customer notebooks with embedded network diagrams.

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


CASES = {'V01': {'title': 'Amazon S3 through customer egress',
         'file': 'V01_Amazon_S3.ipynb',
         'kind': 'csv',
         'purpose': 'Read a small private S3 object from an AIDP workspace through your approved '
                    'outbound network path.'},
 'V02': {'title': 'Bucket and ADB access from the customer VCN',
         'file': 'V02_Customer_Network_Source.ipynb',
         'kind': 'csv',
         'purpose': 'Read a bucket under an IAM Network Source policy and query an ADB public '
                    'endpoint under its VCN ACL, through the same customer network.'},
 'V03': {'title': 'Cross-region OCI Object Storage',
         'file': 'V03_Cross_Region_Object_Storage.ipynb',
         'kind': 'csv',
         'purpose': 'Read a dataset in one OCI region while your AIDP workspace runs in another.'},
 'V04': {'title': 'Private database connectivity before and after DNS edits',
         'file': 'V04_Service_Private_Endpoint.ipynb',
         'kind': 'adb',
         'purpose': 'Establish existing private database connectivity, then verify that unrelated '
                    'Advanced DNS edits preserve the same database connection.'}}

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
    if case == 'V04':
        ev.update(before=None, restored=None, phases=[{'phase': phase, 'functional': None,
                  'operation_evidence_ref': None, 'application_id': None, 'configuration_snapshot': None,
                  'instance': None, 'dns_addresses': [], 'expected_dns_addresses': [], 'mapping_evidence_ref': None}
                  for phase in ('before', 'after_add', 'after_edit', 'after_delete', 'restored')])
        ev['expected_instance'] = ''
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
    """Build customer instructions; keep dated lab history in separate reports."""
    guide = 'v02-network-source.md'
    section = lambda heading: documentation_section(guide, heading)
    intro = (ROOT / 'docs' / guide).read_text().split('\n## ', 1)[0]
    cells = [md(intro + '\n')]
    for heading in ('How the source restrictions work', 'Before you begin', 'IAM and database setup', 'Network setup'):
        cells.append(md('## ' + heading + '\n\n' + section(heading)))
    cells += [diagram, md('## Customer steps\n\n' + section('Customer steps')),
        md('## Initialize V02\n\n**Required:** attach your compute and run the code cell immediately below. '
           'Wait for **V02 initialized** before continuing to **Your configuration**. '
           'This cell defines the bucket and database checks; it makes no network calls. '
           'Run it again after a session restart or compute reattachment. '
           'Rerunning initialization preserves your configuration and collected results.\n'),
        code(engine('V02', CASES['V02']['title']) + '\n' + (ROOT / 'v02_adb_check.py').read_text() + '\n'
             + (ROOT / 'v02_route_check.py').read_text() + '\n'
             + (ROOT / 'v02_control_tests.py').read_text() + '\n'
             + (ROOT / 'v02_adb_diagnostics.py').read_text() + '\n'
             'print("V02 initialized. Next: Your configuration, Choose phase, then the bucket and database cells.")\n'),
        md('## Your configuration\n\nComplete the setup first. CONFIG selects the new bucket fixture; '
           'ADB_CONFIG selects the existing database public-endpoint connection. Set each target region '
           'to the workspace region. The expected CSV rows match sample.csv. Copy expected_service '
           'from the selected ADB descriptor. Keep credentials out of both dictionaries.\n\n'
           + section('Which ADB name belongs in the configuration?')),
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
        code(v02_session_guard('v02_summary', 'CONFIG') +
             'result = run_case(CASE_ID, CONFIG, spark=globals().get("spark"), live=RUN_LIVE, phase=PHASE)\n'
             'result["completed_utc"] = utc()\nresult["object_uri"] = CONFIG.get("uri")\n'
             'if "dns" in result["checks"]:\n'
             '    result["checks"]["dns"]["scope"] = "Driver hostname resolution only; customer routing is checked separately below."\n'
             'result["target"] = "bucket"\n'
             'result["acceptance"] = v02_summary(result)\nRESULTS.append(result)\nprint(json.dumps(result, indent=2))\n'),
        md('## Query the database\n\nRun once per phase using the same approved public-endpoint connection. '
           'The query reads DUAL and returns service, database user and the client address observed by ADB. '
           'A catalog supplies its own credentials; the wallet alternative reads ADB_PASSWORD privately '
           'from the runtime environment. This checks the ADB VCN ACL path, not an IAM Network Source condition.\n'),
        code(v02_session_guard('v02_summary', 'ADB_CONFIG') + 'import os\n'
             'adb_result = run_adb_check(ADB_CONFIG, spark=globals().get("spark"), '
             'runtime={"adb_password": os.environ.get("ADB_PASSWORD")}, live=RUN_LIVE, phase=PHASE)\n'
             'adb_result["acceptance"] = v02_summary(adb_result)\nRESULTS.append(adb_result)\n'
             'print(json.dumps(adb_result, indent=2))\n'),
        md('## Expected results\n\n' + section('Expected results') + '\n'
           '`functional: PASS` confirms the CSV rows or expected database service for the named target. '
           '`source_path: NOT_VERIFIED` identifies a read awaiting the source-control test. '
           'The following references document that review; '
           'they do not automatically query or validate IAM.\n'),
        code('EVIDENCE = ' + pprint.pformat(evidence('V02'), sort_dicts=False) + '\n'),
        md('## Export\n\nSave this output privately with your configuration snapshots and request records.\n'),
        code('print(json.dumps({"case": CASE_ID, "generated_utc": utc(), "results": RESULTS, '
             '"evidence": EVIDENCE, "route_reports": globals().get("ROUTE_REPORTS", []), "control_tests": globals().get("CONTROL_RESULTS", []), "adb_diagnostics": globals().get("ADB_DIAGNOSTICS", [])}, indent=2))\n'),
        md('## References\n\n' + section('References').replace('../../../LICENSE.txt', '../../LICENSE.txt'))]
    route_guide = md('## Verify the customer VCN path\n\n' + section('Verify the customer VCN path'))
    route_guide['id'] = 'v02-route-guide'
    route_cell = code(v02_session_guard('v02_route_report', 'CONFIG') +
        '# Optional administrator evidence. Leave empty to see what is still needed.\n'
        'ROUTE_SNAPSHOT_FILE = ""  # Upload v02-route-snapshot.json and use its /Workspace/... path.\n'
        'OBJECT_STORAGE_EVENT = {}  # Sanitized Read Access Event; fields are listed above.\n'
        'SOURCE_REVIEW = {"principal_id": "", "object_uri": CONFIG.get("uri", ""),\n'
        '                 "network_source_name": "", "no_bypass_grants": False,\n'
        '                 "reviewed_utc": "", "evidence_ref": ""}\n'
        'route_snapshot = json.loads(Path(ROUTE_SNAPSHOT_FILE).read_text()) if ROUTE_SNAPSHOT_FILE else {}\n'
        'route_report = v02_route_report(CONFIG, RESULTS, route_snapshot, OBJECT_STORAGE_EVENT, SOURCE_REVIEW, phase=PHASE)\n'
        'ROUTE_REPORTS = globals().get("ROUTE_REPORTS", [])\nROUTE_REPORTS.append(route_report)\n'
        'print_v02_route_report(route_report)\n')
    route_cell['id'] = 'v02-route-check'
    position = next(i for i, c in enumerate(cells) if ''.join(c['source']).startswith('## Expected results'))
    control_guide = md('## Test the source controls: allow, deny, restore\n\n' + section('Test the source controls: allow, deny, restore'))
    control_guide['id'] = 'v02-control-guide'
    control_setup = code('ADMIN_CONFIG_FILE = ""  # Existing approved private operator profile; never a shared upload.\n'
        'ADMIN_PROFILE = ""\n'
        'RUN_BUCKET_PERMISSION_TEST = False\nRUN_ADB_ACL_TEST = False\nADB_INTERRUPTION_APPROVED = False\n'
        'JOURNAL_DIRECTORY = ""  # Existing private persistent directory; keep a copy outside this session.\n'
        'BUCKET_CONTROL = ' + pprint.pformat(dict(policy_id='', network_source_id='', allow_statement='',
            bucket_name='', network_source_name='', vcn_id='', source_subnet_cidr=''), sort_dicts=False) + '\n'
        'ADB_CONTROL = ' + pprint.pformat(dict(database_id='', vcn_id='', source_subnet_cidr='',
            denied_subnet_cidr=''), sort_dicts=False) + '\n'
        'CONTROL_RESULTS = globals().get("CONTROL_RESULTS", [])\n')
    control_setup['id'] = 'v02-control-setup'
    bucket_guide = md('## Bucket: allow, deny, restore\n\nRun after the normal bucket read succeeds. '
        'This changes only the exact scoped object-read grant, keeps the Network Source fixed, and restores the grant. '
        'Allow up to a few minutes for policy propagation. Expected denial is a successful test step.\n')
    bucket_guide['id'] = 'v02-bucket-control-guide'
    bucket_cell = code(v02_session_guard('run_v02_control_cycle', 'CONFIG') + 'if RUN_BUCKET_PERMISSION_TEST:\n'
        '    if not JOURNAL_DIRECTORY or not Path(JOURNAL_DIRECTORY).is_dir():\n'
        '        raise ValueError("Choose an existing private journal directory first")\n'
        '    iam_admin, _ = v02_admin_clients(ADMIN_CONFIG_FILE, ADMIN_PROFILE)\n'
        '    bucket_control = v02_bucket_control(iam_admin, BUCKET_CONTROL)\n'
        '    bucket_control_result = run_v02_control_cycle(bucket_control,\n'
        '        lambda: v02_bucket_probe(CONFIG, spark, fixture_read),\n'
        '        str(Path(JOURNAL_DIRECTORY) / "v02-bucket-restore.json"), apply=True)\n'
        'else:\n'
        '    bucket_control_result = {"target": "bucket", "status": "NOT_RUN", "reason": "Permission switching is disabled"}\n'
        'CONTROL_RESULTS.append(bucket_control_result)\nprint_v02_control_result(bucket_control_result)\n')
    bucket_cell['id'] = 'v02-bucket-control'
    adb_guide = md('## ADB: allow, deny, restore\n\nThe normal query must succeed through the public endpoint. '
        'The temporary ACL excludes the source subnet and can interrupt new public connections from other consumers. '
        'Run only in the agreed maintenance window. ADB restores its original ACL even if the negative probe fails.\n')
    adb_guide['id'] = 'v02-adb-control-guide'
    adb_cell = code(v02_session_guard('run_v02_control_cycle', 'ADB_CONFIG') + 'if RUN_ADB_ACL_TEST:\n'
        '    if not ADB_INTERRUPTION_APPROVED:\n'
        '        raise ValueError("Confirm the database maintenance window before changing its ACL")\n'
        '    if not JOURNAL_DIRECTORY or not Path(JOURNAL_DIRECTORY).is_dir():\n'
        '        raise ValueError("Choose an existing private journal directory first")\n'
        '    _, db_admin = v02_admin_clients(ADMIN_CONFIG_FILE, ADMIN_PROFILE)\n'
        '    adb_control = v02_adb_control(db_admin, ADB_CONTROL)\n'
        '    adb_control_result = run_v02_control_cycle(adb_control,\n'
        '        lambda: v02_adb_probe(ADB_CONFIG, spark, run_adb_check, {"adb_password": os.environ.get("ADB_PASSWORD")}),\n'
        '        str(Path(JOURNAL_DIRECTORY) / "v02-adb-restore.json"), apply=True)\n'
        'else:\n'
        '    adb_control_result = {"target": "adb", "status": "NOT_RUN", "reason": "ACL switching is disabled"}\n'
        'CONTROL_RESULTS.append(adb_control_result)\nprint_v02_control_result(adb_control_result)\n')
    adb_cell['id'] = 'v02-adb-control'
    recovery_guide = md('## Restore from journal\n\nUse only to recover an interrupted cycle. '
        'Select the affected target and its saved journal; renew the operator session first if needed. '
        'The helper checks the recorded resource and preserves unrelated edits.\n')
    recovery_guide['id'] = 'v02-recovery-guide'
    recovery_cell = code('RUN_RECOVERY = False\nRECOVERY_TARGET = "bucket"  # Or "adb".\nRECOVERY_JOURNAL = ""\n'
        'if RUN_RECOVERY:\n'
        '    iam_admin, db_admin = v02_admin_clients(ADMIN_CONFIG_FILE, ADMIN_PROFILE)\n'
        '    if RECOVERY_TARGET not in ("bucket", "adb"):\n'
        '        raise ValueError("Select bucket or adb")\n'
        '    recovery_control = (v02_bucket_control(iam_admin, BUCKET_CONTROL) if RECOVERY_TARGET == "bucket"\n'
        '                        else v02_adb_control(db_admin, ADB_CONTROL))\n'
        '    print(restore_v02_control_from_journal(recovery_control, RECOVERY_JOURNAL))\n'
        'else:\n'
        '    print("Recovery disabled; no changes made")\n')
    recovery_cell['id'] = 'v02-recovery'
    diagnostic_guide = md('## Troubleshoot ADB\n\n' + section('Troubleshoot the ADB connection'))
    diagnostic_guide['id'] = 'v02-adb-diagnostic-guide'
    diagnostic_cell = code(v02_session_guard('v02_adb_diagnose', 'ADB_CONFIG') +
        'RUN_ADB_DIAGNOSTIC_QUERY = False  # Enable after verifying the public catalog/wallet descriptor.\n'
        'ADB_WORKSPACE_SNAPSHOT_FILE = ""  # Optional private v02-route-snapshot.json path.\n'
        'adb_snapshot = json.loads(Path(ADB_WORKSPACE_SNAPSHOT_FILE).read_text()) if ADB_WORKSPACE_SNAPSHOT_FILE else {}\n'
        'adb_diagnostic = v02_adb_diagnose(ADB_CONFIG, spark=globals().get("spark"),\n'
        '    runtime={"adb_password": os.environ.get("ADB_PASSWORD")}, live=RUN_LIVE,\n'
        '    query=RUN_ADB_DIAGNOSTIC_QUERY, workspace_snapshot=adb_snapshot, adb_reader=run_adb_check)\n'
        'ADB_DIAGNOSTICS = globals().get("ADB_DIAGNOSTICS", [])\nADB_DIAGNOSTICS.append(adb_diagnostic)\n'
        'print_v02_adb_diagnostic(adb_diagnostic)\n')
    diagnostic_cell['id'] = 'v02-adb-diagnostic'
    cells[position:position] = [diagnostic_guide, diagnostic_cell, control_guide, control_setup, bucket_guide, bucket_cell,
        adb_guide, adb_cell, recovery_guide, recovery_cell, route_guide, route_cell]
    return cells


def focused_walkthrough(case, diagram):
    """Give each customer case its own setup and preserve deployed cell IDs."""
    diagram['id'] = case.lower() + '-2'
    guide = {'V01': 'v01-s3.md', 'V03': 'v03-cross-region.md', 'V04': 'v04-private-adb.md'}[case]
    section = lambda heading: documentation_section(guide, heading)
    def markdown(heading, cell_id):
        cell = md('## ' + heading + '\n\n' + section(heading).replace('../../../LICENSE.txt', '../../LICENSE.txt'))
        cell['id'] = case.lower() + '-' + str(cell_id)
        return cell
    def executable(text, cell_id):
        cell = code(text)
        cell['id'] = case.lower() + '-' + str(cell_id)
        return cell
    def guard(config='CONFIG'):
        return (
            'if not callable(globals().get("run_customer_case")):\n'
            '    raise RuntimeError(\'Run "Initialize ' + case + '" first; rerun initialization after a session restart.\')\n'
            'if any(name not in globals() for name in ' + repr(('CASE_ID', config, 'RUN_LIVE', 'PHASE', 'RESULTS')) + '):\n'
            '    raise RuntimeError(\'Run "Your configuration" and "Choose phase" first.\')\n\n')
    ids = ({'phase': 11, 'run': 13, 'expected': 16, 'evidence': 17, 'restore': 18, 'export': 19, 'refs': 20,
            'network': 23, 'iam': 24} if case == 'V03' else
           {'phase': 9, 'run': 11, 'expected': 12, 'evidence': 13,
            'restore': 15 if case == 'V04' else 14, 'export': 16 if case == 'V04' else 15,
            'refs': 17 if case == 'V04' else 16, 'network': 21 if case == 'V04' else 19,
            'iam': 22 if case == 'V04' else 20})
    intro = md((ROOT / 'docs' / guide).read_text().split('\n## ', 1)[0] + '\n')
    intro['id'] = case.lower() + '-0'
    init = md('## Initialize ' + case + '\n\nAttach the intended compute. Run the next cell and wait for **'
              + case + ' initialized**. It defines checks without making network calls and preserves your '
              'configuration and RESULTS. Rerun it after reattaching compute or restarting the session.\n')
    init['id'] = case.lower() + '-4'
    helpers = engine(case, CASES[case]['title']) + '\n' + (ROOT / 'customer_case_checks.py').read_text()
    helpers += '\nprint("' + case + ' initialized. Next: Your configuration, then Choose phase.")\n'
    text = ('CASE_ID = ' + repr(case) + '\nCONFIG = ' +
            pprint.pformat(configuration(CASES[case]['kind']), sort_dicts=False) +
            '\nRUN_LIVE = False\nRUN_EVIDENCE_CHECKS = False\nPHASE = "before"\nRESULTS = []\n')
    if case == 'V04':
        text += '\nCONFIG["private_endpoint_confirmed"] = False  # Verify the actual catalog/wallet HOST and PORT first.\n'
    config = executable(text, 7)
    config_intro = md('## Your configuration\n\n' + section('Configuration'))
    config_intro['id'] = case.lower() + '-6'
    phase_intro = md('## Choose phase\n\nRun this cell before each attempt. Change only the phase and live flag '
                     'here; keep earlier RESULTS. Do not use Run all across manual workspace changes.\n')
    phase_intro['id'] = case.lower() + '-' + str(ids['phase'] - 1)
    phase = executable('RUN_LIVE = False  # Set True only after completing the prerequisites.\n'
                       'PHASE = "before"  # Follow the phase sequence in Customer steps.\n', ids['phase'])
    diagnostic_intro = md('## Check DNS and the listener\n\nOptional read-only driver checks: compare Python and '
        'JVM resolution, then attempt a five-second TCP connection to the configured hostname and port. '
        'RUN_LIVE controls execution. This does not prove the customer route, executor connectivity, TLS '
        'authentication or service authorization. Correlate the UTC interval with the logs below.\n')
    diagnostic_intro['id'] = case.lower() + '-diagnostic-guide'
    diagnostic = executable(guard() +
        'diagnostic = diagnose_customer_target(CONFIG, spark=globals().get("spark"), live=RUN_LIVE, case=CASE_ID)\n'
        'DIAGNOSTICS = globals().get("DIAGNOSTICS", [])\nDIAGNOSTICS.append(diagnostic)\n'
        'print(json.dumps(diagnostic, indent=2))\n', 'diagnostic')
    run_intro = md('## ' + ('Query the private database' if case == 'V04' else 'Read the test object') + '\n\n'
        'Run once per phase with the same target, identity and client. ' +
        ('The query reads DUAL and verifies the expected database identity. Use the reviewed private catalog '
         'or wallet connection; the password is read privately from ADB_PASSWORD.\n' if case == 'V04' else
         'The read is bounded to the declared small fixture and compares the exact expected CSV rows.\n'))
    run_intro['id'] = case.lower() + '-' + str(ids['run'] - 1)
    runtime = ('import os\nRUNTIME = {"adb_password": os.environ.get("ADB_PASSWORD")}\n' if case == 'V04' else
               'RUNTIME = {}\n')
    run = executable(guard() + runtime +
        'result = run_customer_case(CASE_ID, CONFIG, spark=globals().get("spark"), runtime=RUNTIME, live=RUN_LIVE, phase=PHASE)\n'
        'RESULTS.append(result)\nprint(json.dumps(result, indent=2))\n', ids['run'])
    cells = [intro, markdown('Prerequisites', 'prerequisites'), markdown('IAM and database setup' if case == 'V04' else 'IAM setup', ids['iam']),
        markdown('Network setup', ids['network']), diagram, markdown('Logging setup', 'logging'),
        markdown('Customer steps', 1), init, executable(helpers, 5), config_intro, config]
    if case == 'V03':
        control_intro = md('## In-region control configuration\n\nFill this independent fixture once. '
                           'Its region must equal the workspace region. Do not reuse the remote URI.\n')
        control_intro['id'] = 'v03-8'
        cells += [control_intro, executable('CONTROL_CONFIG = ' +
                  pprint.pformat(configuration('csv'), sort_dicts=False) + '\n', 9)]
    cells += [phase_intro, phase, diagnostic_intro, diagnostic, run_intro, run]
    if case == 'V03':
        control_intro = md('## Run the in-region control\n\nRun after the remote workload for every phase. '
                           'This is an independent V03 control read, '
                           'not a claim that an IAM Network Source was tested.\n')
        control_intro['id'] = 'v03-14'
        control = ('if RUN_LIVE and (not CONTROL_CONFIG.get("region") or\n'
                   '                 CONTROL_CONFIG["region"] != CONFIG.get("workspace_region") or\n'
                   '                 CONTROL_CONFIG.get("workspace_region") != CONFIG.get("workspace_region") or\n'
                   '                 CONTROL_CONFIG["uri"] == CONFIG.get("uri")):\n'
                   '    raise ValueError("Choose a separate fixture in the workspace region")\n'
                   'control = run_customer_case(CASE_ID, CONTROL_CONFIG, spark=globals().get("spark"),\n'
                   '                   live=RUN_LIVE, phase=PHASE + "_local_control", fixture_role="local_control")\n'
                   'RESULTS.append(control)\nprint(json.dumps(control, indent=2))\n')
        cells += [control_intro, executable(guard('CONTROL_CONFIG') + control, 15)]
    cells += [markdown('Expected results', ids['expected']), executable('EVIDENCE = ' + pprint.pformat(evidence(case), sort_dicts=False) + '\n', ids['evidence'])]
    if case == 'V04':
        evaluate = ('if RUN_EVIDENCE_CHECKS:\n    observed = evaluate_evidence(CASE_ID, EVIDENCE)\n'
                    '    observed = customer_labels(observed)\n    RESULTS.append(observed)\n'
                    '    print(json.dumps(observed, indent=2))\nelse:\n'
                    '    print("Evidence assertions NOT_RUN; capture real observations first.")\n')
        cells += [executable(guard() + evaluate, 14)]
    cells += [markdown('Troubleshooting', 'troubleshooting'), markdown('Restore and export', ids['restore']),
        executable(guard() + 'print(json.dumps({"case": CASE_ID, "generated_utc": utc(), "results": RESULTS, '
            '"evidence": globals().get("EVIDENCE", {}), "diagnostics": globals().get("DIAGNOSTICS", [])}, indent=2))\n', ids['export']),
        markdown('References', ids['refs'])]
    return cells

def build():
    index = []
    for case, item in CASES.items():
        image = base64.b64encode((ROOT / 'assets' / (case.lower() + '-network.png')).read_bytes()).decode()
        diagram = md('### Example network topology\n\n![Conceptual network path for ' + case +
                     '](attachment:network.png)\n\nThe diagram is illustrative; it does not prove the route taken by a request.\n',
                     {'network.png': {'image/png': image}})
        cells = v02_walkthrough(diagram) if case == 'V02' else focused_walkthrough(case, diagram)
        # Explicit IDs keep new diagnostic cells from renumbering existing cells.
        legacy_index = 0
        for cell in cells:
            if 'id' not in cell:
                cell['id'] = case.lower() + '-' + str(legacy_index)
                legacy_index += 1
        nb = {'cells': cells, 'metadata': {'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}, 'language_info': {'name': 'python'}}, 'nbformat': 4, 'nbformat_minor': 5}
        (ROOT / item['file']).write_text(json.dumps(nb, indent=2) + '\n')
        index.append({'case': case, 'title': item['title'], 'notebook': item['file'], 'objective': item['purpose'], 'kind': item['kind']})
    (ROOT / 'case-index.json').write_text(json.dumps(index, indent=2) + '\n')
    intro = (ROOT / 'docs' / 'customer-walkthrough.md').read_text().split('\n## Common Workbench actions', 1)[0]
    for item in index:
        intro = intro.replace('| ' + item['notebook'] + ' |', '| [' + item['notebook'] + '](' + item['notebook'] + ') |')
    guide = {'cells': [md(intro + '\n\n[Full tester guide](README.md).\n')],
             'metadata': {'language_info': {'name': 'python'}}, 'nbformat': 4, 'nbformat_minor': 5}
    guide['cells'][0]['id'] = 'start-here'
    (ROOT / '00_START_HERE.ipynb').write_text(json.dumps(guide, indent=2) + '\n')
    print('Built four customer notebooks with embedded network diagrams.')

if __name__ == '__main__':
    build()
