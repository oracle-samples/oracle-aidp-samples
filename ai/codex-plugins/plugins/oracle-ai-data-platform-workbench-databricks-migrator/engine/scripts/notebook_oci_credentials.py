"""YAML credential references for generated Python cells; never secret values."""
import ast
import os


OCI_ENV_KEYS = ("AIDP_CRED_OCI_NAME",) + tuple(
    "AIDP_CRED_OCI_KEY_" + field
    for field in ("USER", "FINGERPRINT", "TENANCY", "REGION", "PRIVATE_KEY_CONTENT")
)
_KEY_FIELDS = {
    "user": "USER", "fingerprint": "FINGERPRINT", "tenancy": "TENANCY",
    "region": "REGION", "key_file_content": "PRIVATE_KEY_CONTENT",
}


def credential_env_snippet() -> str:
    """Use the effective YAML refs, including on a reused kernel with stale env."""
    lines = ["# Oracle tool modification: OCI credential references from migration YAML",
             "import os"]
    for key in OCI_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            lines.append(f"os.environ[{key!r}] = {value!r}")
        else:
            lines.append(f"os.environ.pop({key!r}, None)")
    return "\n".join(lines)


def _python_source(source):
    if source.lstrip().startswith("%python\n"):
        offset = source.index("\n") + 1
        return source[:offset], source[offset:]
    return "", source


def rewrite_oci_credential_reads(source: str) -> str:
    """Remove defaults only from the emitted OCI-env credential pattern.

    AST matching leaves comments, strings, unrelated secrets and other languages
    untouched. Original affected lines are retained as tagged comments. Unknown
    credential key names are refused rather than mapped to a guessed field.
    """
    if "AIDP_CRED_OCI_" not in source:
        return source
    magic, body = _python_source(source)
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return source  # Native/magic/invalid cells follow the existing execution path.
    lines = body.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line.encode("utf-8")))
    edits = {}
    originals = set()

    def replace(node, replacement):
        start = offsets[node.lineno - 1] + node.col_offset
        end = offsets[node.end_lineno - 1] + node.end_col_offset
        edits[(start, end)] = replacement.encode("utf-8")
        originals.update(range(node.lineno - 1, node.end_lineno))

    def env_key(node):
        if isinstance(node, ast.Subscript) and ast.unparse(node.value) == "os.environ":
            key = node.slice
        elif (isinstance(node, ast.Call)
              and ast.unparse(node.func) in ("os.environ.get", "os.getenv")
              and node.args):
            key = node.args[0]
        else:
            return None
        return key.value if isinstance(key, ast.Constant) and key.value in OCI_ENV_KEYS else None

    # The generated pattern binds a credential variable then reads fields from
    # that credential in the same lexical scope. Do not cross function scopes.
    def visit_scope(scope):
        nodes = []
        def collect(node):
            if node is not scope and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                visit_scope(node)
                return
            nodes.append(node)
            for child in ast.iter_child_nodes(node):
                collect(child)
        collect(scope)
        bindings = {}
        for node in nodes:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        bindings.setdefault(target.id, []).append(env_key(node.value))
        credentials = {name for name, values in bindings.items()
                       if values and all(v == "AIDP_CRED_OCI_NAME" for v in values)}
        if any("AIDP_CRED_OCI_NAME" in values and name not in credentials
               for name, values in bindings.items()):
            raise ValueError("Ambiguous OCI credential binding; use the YAML credential reference consistently")
        for node in nodes:
            key = env_key(node)
            if key and isinstance(node, ast.Call):
                replace(node, f"os.environ[{key!r}]")
            if not (isinstance(node, ast.Call)
                    and ast.unparse(node.func) == "aidputils.secrets.get"
                    and node.args and isinstance(node.args[0], ast.Name)
                    and node.args[0].id in credentials):
                continue
            field = next((kw.value for kw in node.keywords if kw.arg == "key"), None)
            if field is None:
                raise ValueError("OCI credential read requires a YAML-mapped key= field")
            if isinstance(field, ast.Constant) and isinstance(field.value, str):
                if field.value not in _KEY_FIELDS:
                    raise ValueError("Unmapped OCI credential field; use aidp.oci_credentials.keys")
                replace(field, f"os.environ['AIDP_CRED_OCI_KEY_{_KEY_FIELDS[field.value]}']")
    visit_scope(tree)
    if not edits:
        return source
    encoded = body.encode("utf-8")
    for (start, end), value in sorted(edits.items(), reverse=True):
        encoded = encoded[:start] + value + encoded[end:]
    comments = "# Oracle tool modification: use YAML OCI credential name and field mappings\n"
    comments += "".join("# Original: " + lines[i].rstrip("\r\n") + "\n" for i in sorted(originals))
    return magic + comments + encoded.decode("utf-8")


def ensure_artifact_oci_credentials(cells, native_language="python"):
    """Persist refs for standalone jobs; no YAML parser/tool checkout is needed."""
    needs_auth = False
    for cell in cells:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        magic, body = _python_source(source)
        if native_language not in ("", "python") and not magic:
            continue
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            key = None
            if isinstance(node, ast.Subscript) and ast.unparse(node.value) == "os.environ":
                key = node.slice
            elif (isinstance(node, ast.Call) and node.args
                  and ast.unparse(node.func) in ("os.environ.get", "os.getenv")):
                key = node.args[0]
            if isinstance(key, ast.Constant) and key.value in OCI_ENV_KEYS:
                needs_auth = True
    if not needs_auth:
        return cells
    missing = [key for key in OCI_ENV_KEYS if not os.environ.get(key, "").strip()]
    if missing:
        raise ValueError("Configure aidp.oci_credentials.name and keys in the migration YAML "
                         "before exporting a notebook using OCI authentication")
    marker = "aidp_yaml_oci_credentials"
    # Regeneration replaces only our own bootstrap, keeping repeated passes idempotent.
    cells = [cell for cell in cells if not cell.get("metadata", {}).get(marker)]
    snippet = credential_env_snippet()
    if native_language not in ("", "python"):
        snippet = "%python\n" + snippet
    return [{"cell_type": "code", "source": [snippet], "outputs": [],
             "execution_count": None, "metadata": {marker: True}}] + cells
