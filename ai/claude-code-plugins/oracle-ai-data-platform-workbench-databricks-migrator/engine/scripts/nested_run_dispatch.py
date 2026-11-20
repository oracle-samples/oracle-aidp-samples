"""Execution planning for nested notebook calls; never rewrites saved source.

Only unconditional statement-level calls can be split without moving work
across a control-flow boundary. Unsupported shapes are rejected before any
part of the cell executes, rather than eagerly dispatching a conditional call.
"""
import ast
import re
from dataclasses import dataclass


class UnsupportedDispatch(ValueError):
    """The inline executor cannot preserve this invocation safely."""


@dataclass(frozen=True)
class Call:
    start: int
    end: int
    call_start: int
    call_end: int
    arguments: tuple[str, ...]


def _mask_scala(source: str) -> str:
    """Blank comments and literals, preserving offsets and physical newlines."""
    out = list(source)
    i = 0
    while i < len(source):
        start = i
        if source.startswith('//', i):
            j = source.find('\n', i)
            i = len(source) if j < 0 else j
        elif source.startswith('/*', i):
            depth = 1
            i += 2
            while i < len(source) and depth:
                if source.startswith('/*', i):
                    depth += 1
                    i += 2
                elif source.startswith('*/', i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise UnsupportedDispatch('Unclosed Scala block comment')
        elif source.startswith('"""', i):
            j = source.find('"""', i + 3)
            if j < 0:
                raise UnsupportedDispatch('Unclosed Scala string')
            i = j + 3
        elif source[i] in ('"', "'"):
            quote = source[i]
            i += 1
            while i < len(source):
                if source[i] == '\\':
                    i += 2
                elif source[i] == quote:
                    i += 1
                    break
                else:
                    i += 1
            else:
                raise UnsupportedDispatch('Unclosed Scala literal')
        else:
            i += 1
            continue
        if (source[start] == '"' and start > 0
                and (source[start - 1].isalnum() or source[start - 1] == '_')
                and 'notebook.run' in source[start:i]):
            raise UnsupportedDispatch('Nested notebook.run in a Scala interpolated string cannot be eagerly inlined')
        for j in range(start, min(i, len(out))):
            if out[j] != '\n':
                out[j] = ' '
    return ''.join(out)


def _python_plan(source: str) -> list[Call]:
    tree = ast.parse(source)
    lines = source.splitlines(keepends=True)

    def offset(node, end=False):
        row = node.end_lineno if end else node.lineno
        col = node.end_col_offset if end else node.col_offset
        # AST columns are UTF-8 byte offsets, not character offsets.
        return sum(map(len, lines[:row - 1])) + len(lines[row - 1].encode()[:col].decode())

    def is_run(node):
        return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'run' and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == 'notebook'
                and isinstance(node.func.value.value, ast.Name)
                and node.func.value.value.id in ('dbutils', 'oidlUtils', 'aidputils'))

    calls = [n for n in ast.walk(tree) if is_run(n)]
    planned = []
    for statement in tree.body:
        value = getattr(statement, 'value', None)
        if isinstance(statement, (ast.Expr, ast.Assign, ast.AnnAssign)) and is_run(value):
            if value.keywords or len(value.args) not in (2, 3) or any(
                    isinstance(n, ast.Starred) for n in value.args):
                raise UnsupportedDispatch('Nested notebook.run requires path, timeout and optional argument map')
            planned.append(Call(offset(statement), offset(statement, True),
                                offset(value), offset(value, True),
                                tuple(ast.get_source_segment(source, n) for n in value.args)))
    if len(planned) != len(calls):
        raise UnsupportedDispatch('Nested notebook.run inside control flow or an expression cannot be eagerly inlined')
    return planned


def _scala_plan(source: str) -> list[Call]:
    mask = _mask_scala(source)
    call_re = re.compile(r'\b(?:dbutils|oidlUtils|aidputils)\.notebook\.run\s*\(')
    depth = 0
    boundaries = [0]
    depths = []
    for i, char in enumerate(mask):
        depths.append(depth)
        if char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        if depth < 0:
            raise UnsupportedDispatch('Unbalanced Scala cell')
        if char in '\n;' and depth == 0:
            boundaries.append(i + 1)
    if depth:
        raise UnsupportedDispatch('Unbalanced Scala cell')
    boundaries.append(len(source))
    planned = []
    for match in call_re.finditer(mask):
        if depths[match.start()] != 0:
            raise UnsupportedDispatch('Nested notebook.run inside Scala control flow cannot be eagerly inlined')
        start = max(b for b in boundaries if b <= match.start())
        # A bare call or a simple val/var assignment is a complete statement.
        prefix = mask[start:match.start()].strip()
        if prefix and not re.fullmatch(r'(?:val|var)\s+[A-Za-z_]\w*\s*(?::\s*String\s*)?=', prefix):
            raise UnsupportedDispatch('Unsupported Scala notebook.run statement prefix')
        opening = match.end() - 1
        closing = opening + 1
        while closing < len(mask) and not (mask[closing] == ')' and depths[closing] == 1):
            closing += 1
        if closing == len(mask):
            raise UnsupportedDispatch('Unclosed notebook.run call')
        end = next(b for b in boundaries if b >= closing + 1)
        if mask[closing + 1:end].strip(' \r\n\t;'):
            raise UnsupportedDispatch('Unsupported Scala expression after notebook.run')
        commas = [i for i in range(opening + 1, closing) if mask[i] == ',' and depths[i] == 1]
        cuts = [opening] + commas + [closing]
        args = tuple(source[a + 1:b].strip() for a, b in zip(cuts, cuts[1:]))
        if len(args) not in (2, 3):
            raise UnsupportedDispatch('Nested notebook.run requires path, timeout and optional argument map')
        planned.append(Call(start, end, match.start(), closing + 1, args))
    return planned


def plan(source: str, language: str) -> tuple[str, str, list[Call]]:
    """Return the language prefix, body, and ordered complete call statements."""
    prefix = ''
    if source.lstrip().startswith('%'):
        prefix, separator, source = source.partition('\n')
        if not separator:
            return prefix + '\n', '', []
        prefix += '\n'
    if language == 'scala':
        calls = _scala_plan(source)
    elif language in ('python', 'python3', 'pyspark'):
        calls = _python_plan(source)
    else:
        raise UnsupportedDispatch(f'Nested notebook.run is unsupported in {language}')
    return prefix, source, calls
