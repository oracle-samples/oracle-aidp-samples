"""Execution-only notebook copies for native validation.

Never replace a delivered artifact. Every reachable literal child is copied
before dispatch; an unresolvable child aborts the operation, never falls back
onto the unprotected notebook. The migrator supplies its existing transforms.
"""
import ast
import base64
import hashlib
import json
import posixpath
import re
import uuid

from nested_run_dispatch import UnsupportedDispatch, plan


def _receipt(result, marker, render):
    if not isinstance(result, dict) or result.get('status') != 'ok' or any(
            item.get('type') == 'error' for item in result.get('outputs', [])):
        raise UnsupportedDispatch('Protected native notebook I/O failed')
    values = {line.split(marker, 1)[1] for line in render(result).splitlines()
              if line.startswith(marker)}
    if len(values) != 1:
        raise UnsupportedDispatch('Missing or conflicting protected notebook receipt')
    return values.pop()


async def prepare(target, session, *, migrated_base, redirect, language,
                  canonical, parse_run, quote_run, render):
    """Read durable children, protect copies, and verify bytes before dispatch."""
    root = posixpath.normpath(migrated_base or '') + '/'
    invocation = uuid.uuid4().hex
    copies = {}
    documents = {}

    async def visit(path):
        path = posixpath.normpath(path)
        if not path.startswith(root):
            raise UnsupportedDispatch('Protected native target is outside migrated output')
        if path in copies:
            return copies[path]
        copy_path = path.removesuffix('.ipynb') + f'.__aidp_validation_{invocation}.ipynb'
        copies[path] = copy_path
        marker = '__AIDP_VALIDATION_SOURCE__'
        result = await session.execute(
            'import base64, builtins\n'
            f'with builtins.open({path!r}, "rb") as _aidp_validation_file:\n'
            f'    print({marker!r} + base64.b64encode(_aidp_validation_file.read()).decode())',
            timeout=30)
        try:
            notebook = json.loads(base64.b64decode(_receipt(result, marker, render), validate=True))
        except (ValueError, TypeError) as exc:
            raise UnsupportedDispatch('Invalid durable child notebook for protected validation') from exc
        if not isinstance(notebook, dict) or not isinstance(notebook.get('cells'), list):
            raise UnsupportedDispatch('Protected validation requires notebook cells')
        default_language = language(notebook) or 'python'
        for index, cell in enumerate(notebook['cells']):
            if cell.get('cell_type') != 'code':
                continue
            source = cell.get('source', '')
            source = ''.join(source) if isinstance(source, list) else source
            if not source.strip():
                continue
            # Materialize native metadata as a directive before using the same
            # execution-only redirect path as preparation.
            if default_language in ('scala', 'sql') and not source.lstrip().startswith('%'):
                source = '%' + default_language + '\n' + source
            run = re.fullmatch(r'\s*%run\s+(.+?)\s*', source, re.S)
            if run:
                parsed = parse_run(run[1])
                if not parsed:
                    raise UnsupportedDispatch('Cannot protect nested %run target')
                child, trailing = parsed
                protected = await visit(canonical(child, path))
                source = '%run ' + quote_run(protected) + trailing
            elif re.search(r'\b(?:dbutils|oidlUtils|aidputils)\s*\.notebook\s*\.run\s*\(', source):
                lang = 'scala' if source.lstrip().startswith('%scala') else 'python'
                prefix, body, calls = plan(source, lang)
                edits = []
                for call in calls:
                    try:
                        child = ast.literal_eval(call.arguments[0])
                    except (ValueError, SyntaxError) as exc:
                        raise UnsupportedDispatch(
                            'Cannot protect a runtime-computed call inside a native validation copy; '
                            'no unprotected native dispatch was performed') from exc
                    if not isinstance(child, str):
                        raise UnsupportedDispatch('Protected child target must be a string')
                    protected = await visit(canonical(child, path))
                    start = body.index('(', call.call_start, call.call_end) + 1
                    start += len(body[start:]) - len(body[start:].lstrip())
                    edits.append((start, start + len(call.arguments[0]), json.dumps(protected)))
                for start, end, replacement in reversed(edits):
                    body = body[:start] + replacement + body[end:]
                source = prefix + body
            cell['source'] = [redirect(source, source_op_hint=f'native-child-cell-{index}')]
            cell['outputs'] = []
            cell['execution_count'] = None
        documents[copy_path] = notebook
        return copy_path

    protected_target = await visit(target)
    # Build the entire reachable graph before writing anything. No native
    # dispatch is allowed until every copy has a verified content receipt.
    for path, notebook in documents.items():
        data = json.dumps(notebook, ensure_ascii=False).encode()
        digest = hashlib.sha256(data).hexdigest()
        encoded = base64.b64encode(data).decode()
        marker = '__AIDP_VALIDATION_SAVED__'
        result = await session.execute(
            'import base64, builtins, hashlib\n'
            f'_aidp_validation_bytes = base64.b64decode({encoded!r})\n'
            f'with builtins.open({path!r}, "xb") as _aidp_validation_file:\n'
            '    _aidp_validation_file.write(_aidp_validation_bytes)\n'
            f'with builtins.open({path!r}, "rb") as _aidp_validation_file:\n'
            f'    print({marker!r} + hashlib.sha256(_aidp_validation_file.read()).hexdigest())',
            timeout=60)
        if _receipt(result, marker, render) != digest:
            raise UnsupportedDispatch('Protected notebook readback hash mismatch')
    return protected_target
