"""Translate an AWS Glue ETL (PySpark) script to a plain Spark/PySpark script.

Deterministic-first, same philosophy as the Athena translator: every rule is a
small, explainable transform. The DynamicFrame-specific transforms that have no
1:1 Spark equivalent (ApplyMapping, ResolveChoice, Relationalize, ...) are
FLAGGED for manual review — left in place + reported — never silently dropped.

Reuses the `Finding` / `TranslationResult` shapes from the Athena translator so
the migrate report renders identically for both source types.

Returns a TranslationResult; call `translate(script, oci_namespace=...)`.
"""
from __future__ import annotations

import ast
import io
import json
import re
import tokenize

from aws_aidp.translate.athena_to_spark_sql import Finding, TranslationResult


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_OCI_NAMESPACE_PLACEHOLDER = "<your-oci-namespace>"
_S3_URI_PATTERN = re.compile(r"s3a?://", re.IGNORECASE)


def _valid_oci_namespace(namespace: str) -> bool:
    return bool(
        isinstance(namespace, str)
        and re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,254}", namespace)
    )

def _s3_to_oci(uri: str, ns: str) -> str:
    """s3://bucket/key  →  oci://bucket@<namespace>/key  (s3a:// too)."""
    m = re.fullmatch(
        r"s3a?://([^/:@?#\[\]\s'\"\\<>]+)(/.*)?", uri,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m or (ns != _OCI_NAMESPACE_PLACEHOLDER and not _valid_oci_namespace(ns)):
        return uri
    bucket, key = m.group(1), m.group(2) or ""
    return f"oci://{bucket}@{ns}{key}"


def _spark_table_identifier(*parts: str) -> str:
    """Build a Spark SQL multipart identifier without changing its parts."""
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def _docstring_node_ids(tree: ast.AST) -> set[int]:
    """Identify module, class, and function docstring constants."""
    identities: set[int] = set()
    scopes = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, scopes) or not node.body:
            continue
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) \
                and isinstance(first.value.value, str):
            identities.add(id(first.value))
    return identities


def _s3_literal_count(tree: ast.AST) -> int:
    """Count static string fragments containing an S3 URI in a parsed module.

    Docstrings are documentation rather than data paths, so they are excluded;
    every other string constant (including f-string segments) is counted.
    """
    docstrings = _docstring_node_ids(tree)
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and _S3_URI_PATTERN.search(node.value)
    )


def _balanced_args(text: str, open_idx: int) -> tuple[str, int] | None:
    """Given index of a '(', return (inner_args_str, index_after_closing_paren).

    Python's tokenizer handles triple-quoted/raw strings, escaped quotes,
    comments, and nested containers for us.  Returning ``None`` for an
    incomplete call is deliberate: consuming the rest of a script as an
    argument list is more dangerous than leaving one Glue call for review.
    """
    fragment = text[open_idx:]
    lines = fragment.splitlines(keepends=True)
    offsets: list[int] = []
    total = 0
    for line in lines:
        offsets.append(total)
        total += len(line)
    offsets.append(total)

    def absolute(position: tuple[int, int]) -> int:
        row, column = position
        return open_idx + offsets[min(max(row - 1, 0), len(offsets) - 1)] + column

    depth = 0
    try:
        tokens = tokenize.generate_tokens(io.StringIO(fragment).readline)
        for token in tokens:
            if token.type != tokenize.OP:
                continue
            if token.string in "([{":
                depth += 1
            elif token.string in ")]}":
                depth -= 1
                if depth == 0:
                    close = absolute(token.start)
                    return text[open_idx + 1:close], absolute(token.end)
    except (IndentationError, SyntaxError, tokenize.TokenError):
        pass
    return None


def _first_top_level_arg(args: str) -> str:
    depth = 0
    quote: str | None = None
    i = 0
    while i < len(args):
        char = args[i]
        if quote:
            if char == quote and (i == 0 or args[i - 1] != "\\"):
                quote = None
        elif char in "\"'":
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}" and depth:
            depth -= 1
        elif char == "," and depth == 0:
            return args[:i].strip()
        i += 1
    return args.strip()


def _parsed_call_args(args: str) -> tuple[list[str], dict[str, str]] | None:
    """Return exact top-level positional and keyword expressions.

    Regexes cannot safely extract ``frame=make_df(a, b)`` or expressions with
    lambdas/comprehensions.  Parsing a synthetic call gives us Python's real
    grammar while ``ast.get_source_segment`` preserves the original expression.
    """
    wrapped = f"__aws_aidp_call__({args})"
    try:
        expression = ast.parse(wrapped, mode="eval").body
    except SyntaxError:
        return None
    if not isinstance(expression, ast.Call):
        return None
    positional = [ast.get_source_segment(wrapped, arg) or "" for arg in expression.args]
    keywords = {
        keyword.arg: ast.get_source_segment(wrapped, keyword.value) or ""
        for keyword in expression.keywords
        if keyword.arg is not None
    }
    return positional, keywords


def _kwarg_expr(args: str, key: str) -> str | None:
    parsed = _parsed_call_args(args)
    return parsed[1].get(key) if parsed else None


def _literal_string(args: str, key: str) -> str | None:
    expression = _kwarg_expr(args, key)
    if expression is None:
        return None
    try:
        value = ast.literal_eval(expression)
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, str) else None


def _literal_connection_options(args: str) -> dict[object, object] | None:
    expression = _kwarg_expr(args, "connection_options")
    if expression is None:
        return None
    try:
        options = ast.literal_eval(expression)
    except (SyntaxError, ValueError):
        return None
    return options if isinstance(options, dict) else None


def _kwarg_paths(args: str) -> list[str]:
    """Extract only literal S3 paths from Glue ``connection_options``."""
    options = _literal_connection_options(args)
    if options is None:
        return []
    values: list[object] = []
    if "paths" in options and isinstance(options["paths"], (list, tuple)):
        values.extend(options["paths"])
    if "path" in options:
        values.append(options["path"])
    return [value for value in values if isinstance(value, str) and re.match(r"s3a?://", value)]


def _raw_connection_paths(options: dict[object, object] | None) -> list[object]:
    if options is None:
        return []
    values: list[object] = []
    raw_paths = options.get("paths")
    if isinstance(raw_paths, (list, tuple)):
        values.extend(raw_paths)
    elif raw_paths is not None:
        values.append(raw_paths)
    if "path" in options:
        values.append(options["path"])
    return values


def _dict_string_list(args: str, key: str) -> list[str]:
    """Extract a literal string-list entry from ``connection_options``."""
    options = _literal_connection_options(args)
    if options is None:
        return []
    value = options.get(key)
    if not isinstance(value, (list, tuple)) or not all(isinstance(item, str) for item in value):
        return []
    return list(value)


def _token_spans(text: str, token_types: set[int]) -> list[tuple[int, int]]:
    """Return absolute spans for selected Python token types, best effort."""
    lines = text.splitlines(keepends=True)
    offsets = []
    total = 0
    for line in lines:
        offsets.append(total)
        total += len(line)
    if not lines or not text.endswith(("\n", "\r")):
        offsets.append(total)

    def absolute(position: tuple[int, int]) -> int:
        row, column = position
        row_index = min(max(row - 1, 0), len(offsets) - 1)
        return offsets[row_index] + column

    spans: list[tuple[int, int]] = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            if token.type in token_types:
                spans.append((absolute(token.start), absolute(token.end)))
    except (IndentationError, SyntaxError, tokenize.TokenError):
        # Keep spans yielded before the tokenizer reached malformed input.  The
        # public translator leaves syntactically invalid sources untouched, but
        # this also makes the helpers conservative when called independently.
        pass
    return spans


def _inside_spans(index: int, spans: list[tuple[int, int]]) -> bool:
    return any(start <= index < end for start, end in spans)


def _search_code(pattern: re.Pattern, text: str) -> re.Match | None:
    """Find the next pattern occurrence outside Python comments and strings."""
    spans = _token_spans(text, {tokenize.COMMENT, tokenize.STRING})
    position = 0
    while match := pattern.search(text, position):
        if not _inside_spans(match.start(), spans):
            return match
        position = max(match.end(), match.start() + 1)
    return None


def _sub_code(
    pattern: re.Pattern,
    replacement: str,
    text: str,
) -> tuple[str, int]:
    """Apply a regex replacement only to executable Python token spans."""
    spans = _token_spans(text, {tokenize.COMMENT, tokenize.STRING})
    count = 0

    def sub(match: re.Match) -> str:
        nonlocal count
        if _inside_spans(match.start(), spans):
            return match.group(0)
        count += 1
        return match.expand(replacement)

    return pattern.sub(sub, text), count


def _insert_module_preamble(text: str, block: str) -> str:
    """Insert module code without invalidating shebangs, docs, or future imports."""
    try:
        module = ast.parse(text)
    except SyntaxError:
        return block + text

    insert_after = 0
    body = list(module.body)
    if body and isinstance(body[0], ast.Expr) \
            and isinstance(body[0].value, ast.Constant) \
            and isinstance(body[0].value.value, str):
        insert_after = getattr(body.pop(0), "end_lineno", 0)
    for statement in body:
        if isinstance(statement, ast.ImportFrom) and statement.module == "__future__":
            insert_after = getattr(statement, "end_lineno", statement.lineno)
        else:
            break

    lines = text.splitlines(keepends=True)
    # A shebang or encoding cookie must remain on the first/second physical line.
    if insert_after == 0:
        for index, line in enumerate(lines[:2]):
            if (index == 0 and line.startswith("#!")) or re.search(r"coding[:=]\s*[-\w.]+", line):
                insert_after = index + 1
    if insert_after and not lines[insert_after - 1].endswith(("\n", "\r")):
        lines[insert_after - 1] += "\n"
    lines.insert(insert_after, block)
    return "".join(lines)


def _spark_bootstrap(text: str) -> str:
    """Insert SparkSession initialization without invalidating future imports."""
    return _insert_module_preamble(
        text,
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.getOrCreate()  # [glue→spark] injected\n",
    )


# ---------------------------------------------------------------------------
# rules — each takes (text, findings, ns) and returns rewritten text
# ---------------------------------------------------------------------------

def _rule_flag_glue_aliases(text: str, findings: list[Finding], ns: str) -> str:
    """Prevent imports hidden behind aliases from producing false PASS output."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return text
    used_names = {
        node.id for node in ast.walk(tree)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    directly_supported = {
        "GlueContext", "Job", "DynamicFrame", "getResolvedOptions",
        *(name for name, _hint in _FLAG_TRANSFORMS),
    }
    unresolved: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("awsglue"):
            for imported in node.names:
                local_name = imported.asname or imported.name
                if local_name not in used_names:
                    continue
                if imported.asname or imported.name not in directly_supported:
                    unresolved.add(local_name)
        elif isinstance(node, ast.Import):
            for imported in node.names:
                if not imported.name.startswith("awsglue"):
                    continue
                local_name = imported.asname or imported.name.split(".", 1)[0]
                if imported.asname and local_name in used_names:
                    unresolved.add(local_name)
    if unresolved:
        findings.append(Finding(
            "glue_alias_unhandled",
            "Glue import alias or unsupported imported API remains in use: "
            + ", ".join(sorted(unresolved)),
            "flag",
        ))
    return text


def _rule_glue_imports(text: str, findings: list[Finding], ns: str) -> str:
    """Replace ``awsglue`` imports with ``pass`` while preserving suites."""
    spans = _token_spans(text, {tokenize.COMMENT, tokenize.STRING})
    out_lines = []
    n = 0
    offset = 0
    for line in text.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        newline = line[len(content):]
        match = re.match(r"(?P<indent>\s*)(from|import)\s+awsglue", content)
        if match and ";" not in content and not _inside_spans(offset + match.start(), spans):
            out_lines.append(
                f"{match.group('indent')}pass  # [glue→spark] removed Glue-only import{newline}"
            )
            n += 1
        else:
            out_lines.append(line)
        offset += len(line)
    if n:
        findings.append(Finding("glue_imports",
                                f"commented out {n} awsglue import line(s)", "rewrite"))
    return "".join(out_lines)


def _rule_context_bootstrap(text: str, findings: list[Finding], ns: str) -> str:
    """SparkContext()/GlueContext(sc)/Job(...) boilerplate → SparkSession."""
    changed = False

    # Receivers that genuinely hold a GlueContext, so the `.spark_session` rewrite
    # below can be scoped to them instead of every attribute with that name.
    glue_aliases = {
        match.group(1)
        for match in re.finditer(
            r"^[ \t]*(\w+)\s*=\s*GlueContext\s*\(", text, re.MULTILINE,
        )
    }

    # glueContext = GlueContext(sc)   → glueContext = spark   (kept as alias so
    # later `.spark_session` / `.create_dynamic_frame` rewrites still resolve)
    new, k = _sub_code(
        re.compile(
            r"^([ \t]*)(\w+)\s*=\s*GlueContext\s*\([^()\n]*\)[ \t]*(?:#.*)?$",
            re.MULTILINE,
        ),
        r"\1\2 = spark  # [glue→spark] GlueContext → SparkSession",
        text,
    )
    if k:
        changed = True
        text = new

    # sc = SparkContext() / SparkContext.getOrCreate()
    new, k = _sub_code(
        re.compile(
            r"^([ \t]*)(\w+)\s*=\s*SparkContext(\.\w+)?\s*\([^)]*\)[ \t]*$",
            re.MULTILINE,
        ),
        r"\1\2 = spark.sparkContext  # [glue→spark]",
        text,
    )
    if k:
        changed = True
        text = new

    # `spark = glueContext.spark_session` is redundant after GlueContext is
    # replaced, but removing a suite's only statement would break its syntax.
    # Replace only this generated self-alias with `pass`; never delete an
    # unrelated `spark = spark` assignment that was already in the source.
    new, k = _sub_code(
        re.compile(
            r"^([ \t]*)spark\s*=\s*\w+\.spark_session[ \t]*(?:#.*)?$",
            re.MULTILINE,
        ),
        r"\1pass  # [glue→spark] removed redundant SparkSession alias",
        text,
    )
    if k:
        changed = True
        text = new

    # Non-self aliases such as `session = glueContext.spark_session` remain
    # useful and become `session = spark`.
    #
    # Restricted to receivers that are actually GlueContext aliases. A bare
    # `\w+\.spark_session` also matches unrelated attributes such as
    # `self.spark_session`, where rewriting an attribute STORE into `spark = ...`
    # silently drops the assignment and makes every later read resolve to the
    # injected module global instead of the session that was passed in.
    if glue_aliases:
        alternatives = "|".join(sorted(re.escape(alias) for alias in glue_aliases))
        new, k = _sub_code(
            # The lookahead keeps an assignment target intact; only reads are rewritten.
            re.compile(rf"\b(?:{alternatives})\.spark_session\b(?!\s*=(?!=))"),
            "spark",
            text,
        )
        if k:
            changed = True
            text = new

    # ensure a SparkSession exists — inject a builder near the top if the script
    # now uses `spark` but never assigns it (bootstrap was dissolved above)
    has_module_spark = False
    try:
        module = ast.parse(text)
    except SyntaxError:
        module = None
    if module is not None:
        for statement in module.body:
            targets = statement.targets if isinstance(statement, ast.Assign) else []
            if isinstance(statement, ast.AnnAssign):
                targets = [statement.target]
            if any(isinstance(target, ast.Name) and target.id == "spark" for target in targets):
                has_module_spark = True
                break
    if changed and not has_module_spark \
            and _search_code(re.compile(r"SparkSession\.builder"), text) is None:
        text = _spark_bootstrap(text)

    if changed:
        findings.append(Finding("context_bootstrap",
                                "GlueContext/SparkContext bootstrap → SparkSession", "rewrite"))
    return text


def _rule_job_lifecycle(text: str, findings: list[Finding], ns: str) -> str:
    """job = Job(glueContext) / job.init(...) / job.commit() → comment out.

    AIDP jobs have no bookmark/commit lifecycle, so these calls are dropped.
    """
    n = 0
    job_variables: set[str] = set()
    reassigned_variables: set[str] = set()
    try:
        tree = ast.parse(text)
    except SyntaxError:
        tree = None
    if tree is not None:
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            target_names = {target.id for target in targets if isinstance(target, ast.Name)}
            function = value.func if isinstance(value, ast.Call) else None
            is_job = isinstance(function, ast.Name) and function.id == "Job"
            if is_job:
                job_variables.update(target_names)
            else:
                reassigned_variables.update(target_names)
        job_variables.difference_update(reassigned_variables)

    assignment_pattern = None
    lifecycle_pattern = None
    if job_variables:
        names = "|".join(re.escape(name) for name in sorted(job_variables))
        assignment_pattern = re.compile(rf"(?m)^[ \t]*(?:{names})\s*=\s*Job\s*\(")
        lifecycle_pattern = re.compile(rf"(?m)^[ \t]*(?:{names})\.(?:init|commit)\s*\(")
    patterns = (
        assignment_pattern,
        lifecycle_pattern,
    )
    while True:
        matches = [
            match for pattern in patterns if pattern is not None
            if (match := _search_code(pattern, text))
        ]
        if not matches:
            break
        match = min(matches, key=lambda item: item.start())
        balanced = _balanced_args(text, match.end() - 1)
        if balanced is None:
            findings.append(Finding(
                "job_lifecycle_unparsed",
                "incomplete Glue Job lifecycle call was left unchanged",
                "flag",
            ))
            break
        _args, end = balanced
        line_end = text.find("\n", end)
        if line_end < 0:
            line_end = len(text)
            newline = ""
        elif not text[end:line_end].strip():
            line_end += 1
            newline = "\n"
        else:
            findings.append(Finding(
                "job_lifecycle_compound_statement",
                "Glue Job lifecycle call in a compound statement was left unchanged",
                "flag",
            ))
            break
        indent_match = re.match(r"[ \t]*", match.group(0))
        indent = indent_match.group(0) if indent_match else ""
        replacement = f"{indent}pass  # [glue→spark] removed Glue Job lifecycle{newline}"
        text = text[:match.start()] + replacement + text[line_end:]
        n += 1
    if n:
        findings.append(Finding("job_lifecycle",
                                f"removed {n} Glue Job lifecycle call(s) (init/commit)", "rewrite"))
    return text


def _rule_resolved_options(text: str, findings: list[Finding], ns: str) -> str:
    """Map required Glue arguments to the documented AIDP parameter API."""
    pat = re.compile(r"(?:awsglue\.utils\.)?getResolvedOptions\s*\(")
    out = text
    replacements = 0
    while True:
        m = _search_code(pat, out)
        if not m:
            break
        balanced = _balanced_args(out, m.end() - 1)
        if balanced is None:
            findings.append(Finding(
                "resolved_options_unparsed",
                "incomplete getResolvedOptions call was left unchanged",
                "flag",
            ))
            break
        args, end = balanced
        parsed = _parsed_call_args(args)
        keys: list[str] = []
        if parsed and len(parsed[0]) >= 2:
            try:
                raw_keys = ast.literal_eval(parsed[0][1])
            except (SyntaxError, ValueError):
                raw_keys = None
            if isinstance(raw_keys, (list, tuple)) and all(isinstance(key, str) for key in raw_keys):
                keys = list(raw_keys)
        if not keys:
            findings.append(Finding(
                "resolved_options_dynamic",
                "getResolvedOptions uses non-literal parameter names; configure AIDP parameters manually",
                "flag",
            ))
            break
        findings.append(Finding(
            "resolved_options",
            "getResolvedOptions → required AIDP parameters via "
            "oidlUtils.parameters.getParameter; "
            f"configure: {', '.join(keys)}",
            "rewrite"))
        body = ", ".join(
            f"{json.dumps(key)}: _aws_aidp_required_parameter({json.dumps(key)})"
            for key in keys
        )
        replacement = "{" + body + "}"
        out = out[: m.start()] + replacement + out[end:]
        replacements += 1
    if replacements and _search_code(
        re.compile(r"^def\s+_aws_aidp_required_parameter\s*\(", re.MULTILINE), out
    ) is None:
        helper = (
            "def _aws_aidp_required_parameter(name):\n"
            "    value = oidlUtils.parameters.getParameter(name, None)\n"
            "    if value is None:\n"
            "        raise ValueError(f\"missing required AIDP job parameter: {name}\")\n"
            "    return value\n\n"
        )
        out = _insert_module_preamble(out, helper)
    return out


def _rule_read_from_catalog(text: str, findings: list[Finding], ns: str) -> str:
    """glueContext.create_dynamic_frame.from_catalog(database=, table_name=) → spark.table."""
    pat = re.compile(r"\.create_dynamic_frame(?:\.|_)from_catalog\s*\(")
    out = text
    while True:
        m = _search_code(pat, out)
        if not m:
            break
        balanced = _balanced_args(out, m.end() - 1)
        if balanced is None:
            findings.append(Finding(
                "read_from_catalog_unparsed",
                "incomplete Glue catalog read was left unchanged",
                "flag",
            ))
            break
        args, end = balanced
        db = _literal_string(args, "database")
        tbl = _literal_string(args, "table_name")
        parsed_args = _parsed_call_args(args)
        # back up to the receiver (glueContext) start of the attribute chain
        recv = re.search(r"[\w.]+$", out[: m.start()])
        start = recv.start() if recv else m.start()
        if db and tbl:
            table_identifier = _spark_table_identifier(db, tbl)
            replacement = f"spark.table({json.dumps(table_identifier, ensure_ascii=False)})"
            findings.append(Finding(
                "read_from_catalog",
                f"create_dynamic_frame.from_catalog({table_identifier}) → spark.table",
                "rewrite",
            ))
        else:
            findings.append(Finding(
                "read_from_catalog",
                "from_catalog with non-literal db/table — verify manually", "flag"))
            # Keep the original operation. A placeholder table is executable
            # but can silently point at the wrong target data.
            break
        if re.search(r"\b(?:push_down_predicate|additional_options|catalog_id)\s*=", args):
            findings.append(Finding(
                "catalog_options_unhandled",
                "Glue catalog predicates or additional options require manual conversion",
                "flag",
            ))
        if parsed_args:
            unknown = set(parsed_args[1]) - {
                "database", "table_name", "transformation_ctx",
                "push_down_predicate", "additional_options", "catalog_id",
            }
            if unknown:
                findings.append(Finding(
                    "catalog_arguments_unhandled",
                    "Glue catalog argument(s) require manual conversion: "
                    + ", ".join(sorted(unknown)),
                    "flag",
                ))
        out = out[:start] + replacement + out[end:]
    return out


def _rule_read_from_options(text: str, findings: list[Finding], ns: str) -> str:
    """create_dynamic_frame.from_options(connection_type='s3', ...) → spark.read...load()."""
    pat = re.compile(r"\.create_dynamic_frame(?:\.|_)from_options\s*\(")
    out = text
    while True:
        m = _search_code(pat, out)
        if not m:
            break
        balanced = _balanced_args(out, m.end() - 1)
        if balanced is None:
            findings.append(Finding(
                "read_from_options_unparsed",
                "incomplete Glue options read was left unchanged",
                "flag",
            ))
            break
        args, end = balanced
        connection_expression = _kwarg_expr(args, "connection_type")
        format_expression = _kwarg_expr(args, "format")
        connection_value = _literal_string(args, "connection_type")
        format_value = _literal_string(args, "format")
        if connection_expression is not None and connection_value is None:
            findings.append(Finding(
                "read_connection_dynamic",
                "dynamic Glue connection type was left unchanged",
                "flag",
            ))
            break
        if format_expression is not None and format_value is None:
            findings.append(Finding(
                "read_format_dynamic", "dynamic Glue input format was left unchanged", "flag"
            ))
            break
        connection_type = (connection_value or "").lower()
        fmt = format_value or "parquet"
        options_expression = _kwarg_expr(args, "connection_options")
        connection_options = _literal_connection_options(args)
        if options_expression is not None and connection_options is None:
            findings.append(Finding(
                "connection_options_dynamic",
                "dynamic Glue S3 connection options were left unchanged",
                "flag",
            ))
            break
        raw_paths = _raw_connection_paths(connection_options)
        if raw_paths and not all(
            isinstance(path, str) and re.match(r"s3a?://[^/]+", path)
            for path in raw_paths
        ):
            findings.append(Finding(
                "read_paths_unhandled",
                "Glue input contains a non-S3, non-string, or invalid literal path and was left unchanged",
                "flag",
            ))
            break
        paths = _kwarg_paths(args)
        recv = re.search(r"[\w.]+$", out[: m.start()])
        start = recv.start() if recv else m.start()
        if connection_type not in ("", "s3"):
            findings.append(Finding(
                "read_connection_unhandled",
                f"Glue {connection_type or 'dynamic'} connection was left unchanged; "
                "migrate its credentials and connector explicitly",
                "flag",
            ))
            break
        if paths:
            locations = [_s3_to_oci(path, ns) for path in paths]
            if len(locations) == 1:
                load_arg = json.dumps(locations[0], ensure_ascii=False)
            else:
                load_arg = json.dumps(locations, ensure_ascii=False)
            replacement = f"spark.read.format({json.dumps(fmt, ensure_ascii=False)}).load({load_arg})"
            findings.append(Finding(
                "read_from_options",
                f'create_dynamic_frame.from_options(s3 {fmt}) → spark.read.load ({len(locations)} path(s))',
                "rewrite"))
        else:
            findings.append(Finding(
                "read_from_options", "from_options with no literal path — verify manually", "flag"))
            break
        if re.search(r"\bformat_options\s*=", args):
            findings.append(Finding(
                "read_options_unhandled",
                "Glue format_options require manual verification after conversion",
                "flag",
            ))
        if fmt.lower() not in {"parquet", "json", "csv", "orc", "text", "avro"}:
            findings.append(Finding(
                "read_format_unverified",
                f"Spark provider {fmt!r} may require an AIDP-compatible package",
                "flag",
            ))
        unhandled_options = set(connection_options or {}) - {"path", "paths"}
        if unhandled_options:
            findings.append(Finding(
                "connection_options_unhandled",
                "Glue S3 connection option(s) require manual conversion: "
                + ", ".join(sorted(map(str, unhandled_options))),
                "flag",
            ))
        parsed_args = _parsed_call_args(args)
        if parsed_args:
            unknown = set(parsed_args[1]) - {
                "connection_type", "connection_options", "format",
                "format_options", "transformation_ctx",
            }
            if unknown:
                findings.append(Finding(
                    "read_arguments_unhandled",
                    "Glue read argument(s) require manual conversion: "
                    + ", ".join(sorted(unknown)),
                    "flag",
                ))
        out = out[:start] + replacement + out[end:]
    return out


def _rule_write_from_options(text: str, findings: list[Finding], ns: str) -> str:
    """write_dynamic_frame.from_options(frame=df, connection_type='s3', ...) → df.write...save()."""
    pat = re.compile(r"[\w.]*\.write_dynamic_frame(?:\.|_)from_options\s*\(")
    out = text
    while True:
        m = _search_code(pat, out)
        if not m:
            break
        balanced = _balanced_args(out, m.end() - 1)
        if balanced is None:
            findings.append(Finding(
                "write_from_options_unparsed",
                "incomplete Glue options write was left unchanged",
                "flag",
            ))
            break
        args, end = balanced
        frame = _kwarg_expr(args, "frame")
        connection_expression = _kwarg_expr(args, "connection_type")
        format_expression = _kwarg_expr(args, "format")
        connection_value = _literal_string(args, "connection_type")
        format_value = _literal_string(args, "format")
        if connection_expression is not None and connection_value is None:
            findings.append(Finding(
                "write_connection_dynamic",
                "dynamic Glue destination type was left unchanged",
                "flag",
            ))
            break
        if format_expression is not None and format_value is None:
            findings.append(Finding(
                "write_format_dynamic", "dynamic Glue output format was left unchanged", "flag"
            ))
            break
        fmt = format_value or "parquet"
        connection_type = (connection_value or "").lower()
        options_expression = _kwarg_expr(args, "connection_options")
        connection_options = _literal_connection_options(args)
        if options_expression is not None and connection_options is None:
            findings.append(Finding(
                "connection_options_dynamic",
                "dynamic Glue destination options were left unchanged",
                "flag",
            ))
            break
        raw_paths = _raw_connection_paths(connection_options)
        if raw_paths and not all(
            isinstance(path, str) and re.match(r"s3a?://[^/]+", path)
            for path in raw_paths
        ):
            findings.append(Finding(
                "write_paths_unhandled",
                "Glue destination contains a non-S3, non-string, or invalid literal path and was left unchanged",
                "flag",
            ))
            break
        paths = _kwarg_paths(args)
        partition_keys = _dict_string_list(args, "partitionKeys")
        start = m.start()
        if connection_type not in ("", "s3"):
            findings.append(Finding(
                "write_connection_unhandled",
                f"Glue {connection_type or 'dynamic'} destination was left unchanged; "
                "migrate its credentials and connector explicitly",
                "flag",
            ))
            break
        if frame is None:
            findings.append(Finding(
                "write_frame_unhandled",
                "Glue write frame expression could not be parsed and was left unchanged",
                "flag",
            ))
            break
        if connection_options and "partitionKeys" in connection_options and not (
            isinstance(connection_options["partitionKeys"], (list, tuple))
            and all(isinstance(key, str) for key in connection_options["partitionKeys"])
        ):
            findings.append(Finding(
                "partition_keys_unhandled",
                "dynamic or invalid Glue partition keys were left unchanged",
                "flag",
            ))
            break
        if len(paths) > 1:
            findings.append(Finding(
                "write_paths_unhandled",
                "Glue write contains multiple S3 paths and was left unchanged to avoid "
                "selecting the wrong destination",
                "flag",
            ))
            break
        if paths:
            loc = _s3_to_oci(paths[0], ns)
            partition = ""
            if partition_keys:
                quoted = ", ".join(json.dumps(key, ensure_ascii=False) for key in partition_keys)
                partition = f".partitionBy({quoted})"
            replacement = (
                f"{frame}.write.format({json.dumps(fmt, ensure_ascii=False)}){partition}"
                f".save({json.dumps(loc, ensure_ascii=False)})"
            )
            findings.append(Finding(
                "write_from_options",
                f'write_dynamic_frame.from_options(s3 {fmt}) → df.write.save ({loc})', "rewrite"))
            findings.append(Finding(
                "write_disposition",
                "Glue does not encode a Spark save mode here; choose "
                "error/append/overwrite/ignore for the target explicitly",
                "flag",
            ))
        else:
            findings.append(Finding(
                "write_from_options", "write_dynamic_frame with no literal path — verify manually", "flag"))
            break
        if re.search(r"\bformat_options\s*=", args):
            findings.append(Finding(
                "write_options_unhandled",
                "Glue format_options require manual verification after conversion",
                "flag",
            ))
        unhandled_options = set(connection_options or {}) - {"path", "paths", "partitionKeys"}
        if unhandled_options:
            findings.append(Finding(
                "write_connection_options_unhandled",
                "Glue destination option(s) require manual conversion: "
                + ", ".join(sorted(map(str, unhandled_options))),
                "flag",
            ))
        if fmt.lower() not in {"parquet", "json", "csv", "orc", "text", "avro"}:
            findings.append(Finding(
                "write_format_unverified",
                f"Spark provider {fmt!r} may require an AIDP-compatible package",
                "flag",
            ))
        parsed_args = _parsed_call_args(args)
        if parsed_args:
            unknown = set(parsed_args[1]) - {
                "frame", "connection_type", "connection_options", "format",
                "format_options", "transformation_ctx",
            }
            if unknown:
                findings.append(Finding(
                    "write_arguments_unhandled",
                    "Glue write argument(s) require manual conversion: "
                    + ", ".join(sorted(unknown)),
                    "flag",
                ))
        out = out[:start] + replacement + out[end:]
    return out


def _rule_dynamicframe_fromdf(text: str, findings: list[Finding], ns: str) -> str:
    """DynamicFrame.fromDF(df, glueContext, 'name') → df  (already a DataFrame)."""
    pat = re.compile(r"DynamicFrame\.fromDF\s*\(")
    out = text
    n = 0
    while True:
        m = _search_code(pat, out)
        if not m:
            break
        balanced = _balanced_args(out, m.end() - 1)
        if balanced is None:
            findings.append(Finding(
                "dynamicframe_fromdf_unparsed",
                "incomplete DynamicFrame.fromDF call was left unchanged",
                "flag",
            ))
            break
        args, end = balanced
        parsed = _parsed_call_args(args)
        first = parsed[0][0] if parsed and parsed[0] else _first_top_level_arg(args)
        if not first:
            findings.append(Finding(
                "dynamicframe_fromdf_unparsed",
                "DynamicFrame.fromDF without a DataFrame argument was left unchanged",
                "flag",
            ))
            break
        out = out[: m.start()] + first + out[end:]
        n += 1
    if n:
        findings.append(Finding("dynamicframe_fromdf",
                                f"DynamicFrame.fromDF(df, ...) → df ({n}x)", "rewrite"))
    return out


# DynamicFrame transforms with no clean 1:1 — FLAG, leave in place.
_FLAG_TRANSFORMS = [
    (
        "ApplyMapping",
        "ApplyMapping.apply / DynamicFrame.apply_mapping → rewrite as "
        "df.select(col(...).cast(...).alias(...))",
    ),
    (
        "ResolveChoice",
        "ResolveChoice.apply / DynamicFrame.resolveChoice → resolve column "
        "type ambiguity manually (cast/withColumn)",
    ),
    (
        "DropNullFields",
        "DropNullFields.apply / DynamicFrame.drop_null_fields → identify and drop "
        "NullType columns explicitly",
    ),
    ("Relationalize",  "Relationalize.apply / DynamicFrame.relationalize → flatten nested structs manually"),
    ("Unbox",          "Unbox.apply / DynamicFrame.unbox → parse with from_json manually"),
    ("SelectFields",   "SelectFields.apply / DynamicFrame.select_fields → df.select(...)"),
    ("DropFields",     "DropFields.apply / DynamicFrame.drop_fields → df.drop(...)"),
    (
        "RenameField",
        "RenameField.apply / DynamicFrame.rename_field → df.withColumnRenamed(...)",
    ),
    ("Join",           "Join.apply / DynamicFrame.join → df1.join(df2, on=..., how=...)"),
    ("Filter",         "Filter.apply / DynamicFrame.filter(f=...) → df.filter(<condition>)"),
    ("Map",            "Map.apply / DynamicFrame.map → df.withColumn(...) / a UDF"),
    ("SplitFields",    "SplitFields.apply / DynamicFrame.split_fields → split columns manually"),
    (
        "SplitRows",
        "DynamicFrame.split_rows returns a DynamicFrameCollection; rebuild both "
        "predicate branches explicitly",
    ),
    (
        "MergeDynamicFrame",
        "DynamicFrame.mergeDynamicFrame has Glue-specific upsert and duplicate "
        "semantics; implement and validate an explicit merge",
    ),
    ("Spigot", "DynamicFrame.spigot samples to a side output; replace it explicitly"),
    (
        "Unnest",
        "DynamicFrame.unnest has Glue-specific nested-field naming; flatten the "
        "Spark schema explicitly",
    ),
    (
        "UnnestDDBJson",
        "DynamicFrame.unnest_ddb_json has Glue DynamoDB JSON semantics; replace it "
        "with an explicitly validated Spark projection",
    ),
    (
        "SimplifyDDBJson",
        "DynamicFrame.simplify_ddb_json has Glue DynamoDB JSON semantics; replace "
        "it with an explicitly validated Spark projection",
    ),
    (
        "ErrorsAsDynamicFrame",
        "DynamicFrame.errorsAsDynamicFrame depends on Glue's record-level error "
        "channel, which a Spark DataFrame does not preserve",
    ),
    (
        "AssertErrorThreshold",
        "DynamicFrame.assertErrorThreshold depends on Glue transformation error "
        "state; replace it with explicit validation and failure policy",
    ),
    (
        "ErrorsCount",
        "DynamicFrame.errorsCount depends on Glue transformation error state; "
        "replace it with explicit data-quality metrics",
    ),
    (
        "StageErrorsCount",
        "DynamicFrame.stageErrorsCount depends on Glue stage error state; replace "
        "it with explicit data-quality metrics",
    ),
    (
        "GetNumPartitions",
        "DynamicFrame.getNumPartitions has no DataFrame method equivalent; use "
        "df.rdd.getNumPartitions() after validating the execution contract",
    ),
    (
        "RecomputeSchema",
        "DynamicFrame.recomputeSchema has Glue choice-type semantics with no "
        "DataFrame equivalent",
    ),
    (
        "Schema",
        "DynamicFrame.schema() becomes the non-callable DataFrame.schema property; "
        "remove the call and validate type differences",
    ),
    (
        "Union",
        "DynamicFrame.union can reconcile Glue choice types differently from "
        "DataFrame.union; validate schemas and choose union/unionByName explicitly",
    ),
    (
        "Write",
        "DynamicFrame.write(...) is not a DataFrame API; migrate the connection, "
        "format, options, and save mode explicitly",
    ),
    (
        "ToDFOptions",
        "DynamicFrame.toDF(resolve_options) options would be interpreted as "
        "DataFrame column names; resolve choice types explicitly before conversion",
    ),
    (
        "ShowOptions",
        "DynamicFrame.show(num_rows=...) uses a Glue-only keyword; use Spark's "
        "show(n=...) contract explicitly",
    ),
]

_METHOD_TRANSFORMS = {
    "apply_mapping": "ApplyMapping",
    "drop_fields": "DropFields",
    "join": "Join",
    "map": "Map",
    "relationalize": "Relationalize",
    "resolveChoice": "ResolveChoice",
    "rename_field": "RenameField",
    "select_fields": "SelectFields",
    "split_fields": "SplitFields",
    "unbox": "Unbox",
    "filter": "Filter",
    "drop_null_fields": "DropNullFields",
    "split_rows": "SplitRows",
    "mergeDynamicFrame": "MergeDynamicFrame",
    "spigot": "Spigot",
    "unnest": "Unnest",
    "unnest_ddb_json": "UnnestDDBJson",
    "simplify_ddb_json": "SimplifyDDBJson",
    "errorsAsDynamicFrame": "ErrorsAsDynamicFrame",
    "assertErrorThreshold": "AssertErrorThreshold",
    "errorsCount": "ErrorsCount",
    "stageErrorsCount": "StageErrorsCount",
    "getNumPartitions": "GetNumPartitions",
    "recomputeSchema": "RecomputeSchema",
    "schema": "Schema",
    "union": "Union",
    "write": "Write",
    "toDF": "ToDFOptions",
    "show": "ShowOptions",
}

# Names that also exist on Spark DataFrames, RDDs, or ordinary Python objects.
# Only a proven DynamicFrame receiver may flag these; every other name in
# _METHOD_TRANSFORMS is DynamicFrame-only and cannot be correct on any receiver
# once the Glue runtime is gone, so a Glue script flags it without provenance.
_DATAFRAME_COMPATIBLE_METHOD_NAMES = {
    "filter", "getNumPartitions", "join", "map", "schema", "show", "toDF",
    "union", "write",
}
_GLUE_SCRIPT_MARKER = re.compile(
    r"awsglue|\bGlueContext\b|\bDynamicFrame\b|\.(?:create|write)_dynamic_frame|"
    r"getResolvedOptions\s*\(|\btransformation_ctx\s*=|\.(?:getSource|getSink)\s*\("
)
_GLUE_CONTEXT_METHOD_NAMES = {"create_dynamic_frame", "write_dynamic_frame", "getSource", "getSink"}
_TRY_STATEMENTS: tuple[type, ...] = (ast.Try,) + (
    (ast.TryStar,) if hasattr(ast, "TryStar") else ()
)


def _is_glue_context_method(attribute: str) -> bool:
    """Return whether an attribute name is a GlueContext reader/writer entry point."""
    return attribute in _GLUE_CONTEXT_METHOD_NAMES or attribute.startswith(
        ("create_dynamic_frame_from_", "write_dynamic_frame_from_"),
    )


def _is_provenance_free_transform(node: ast.Call) -> bool:
    """Return whether a method call is DynamicFrame-only by name or signature."""
    if not isinstance(node.func, ast.Attribute):
        return False
    attribute = node.func.attr
    if attribute not in _DATAFRAME_COMPATIBLE_METHOD_NAMES:
        return True
    # DataFrame.filter has no `f=` keyword and DataFrame.show has no `num_rows=`.
    glue_only_keyword = {"filter": "f", "show": "num_rows"}.get(attribute)
    return glue_only_keyword is not None and any(
        keyword.arg == glue_only_keyword for keyword in node.keywords
    )


_DYNAMICFRAME_LINEAGE_METHODS = {"coalesce", "repartition"}
_DYNAMICFRAME_RETURNING_METHODS = {
    "apply_mapping",
    "coalesce",
    "drop_fields",
    "drop_null_fields",
    "errorsAsDynamicFrame",
    "filter",
    "join",
    "map",
    "mergeDynamicFrame",
    "rename_field",
    "repartition",
    "resolveChoice",
    "select_fields",
    "simplify_ddb_json",
    "spigot",
    "unbox",
    "union",
    "unnest",
    "unnest_ddb_json",
}
_DATAFRAME_RETURNING_METHODS = {
    "agg",
    "alias",
    "cache",
    "checkpoint",
    "coalesce",
    "crossJoin",
    "distinct",
    "drop",
    "dropDuplicates",
    "dropDuplicatesWithinWatermark",
    "dropna",
    "exceptAll",
    "fillna",
    "filter",
    "hint",
    "intersect",
    "intersectAll",
    "join",
    "limit",
    "localCheckpoint",
    "melt",
    "observe",
    "orderBy",
    "persist",
    "repartition",
    "repartitionByRange",
    "replace",
    "sample",
    "sampleBy",
    "select",
    "selectExpr",
    "sort",
    "sortWithinPartitions",
    "subtract",
    "summary",
    "transform",
    "union",
    "unionAll",
    "unionByName",
    "unpivot",
    "where",
    "withColumn",
    "withColumnRenamed",
    "withColumns",
    "withColumnsRenamed",
    "withMetadata",
}
_BATCH_WRITER_BUILDERS = {
    "bucketBy", "clusterBy", "format", "mode", "option", "options",
    "partitionBy", "sortBy",
}
_STREAM_WRITER_BUILDERS = {
    "foreach", "foreachBatch", "format", "option", "options", "outputMode",
    "partitionBy", "queryName", "trigger",
}
_V2_WRITER_BUILDERS = {"option", "options", "partitionedBy", "tableProperty", "using"}
_GROUPED_DATAFRAME_BUILDERS = {"cube", "groupBy", "rollup"}


class _LocalBindingCollector(ast.NodeVisitor):
    """Collect function-local bindings without entering nested lexical scopes."""

    def __init__(self) -> None:
        self.names: set[str] = set()
        self.global_names: set[str] = set()
        self.nonlocal_names: set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.names.add(node.id)

    def visit_Global(self, node: ast.Global) -> None:  # noqa: N802
        self.global_names.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:  # noqa: N802
        self.nonlocal_names.update(node.names)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self.names.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self.names.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802
        return

    visit_SetComp = visit_ListComp
    visit_DictComp = visit_ListComp
    visit_GeneratorExp = visit_ListComp

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        self.names.update(alias.asname or alias.name.split(".", 1)[0] for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        self.names.update(alias.asname or alias.name for alias in node.names if alias.name != "*")


def _function_local_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    collector = _LocalBindingCollector()
    for statement in node.body:
        collector.visit(statement)
    arguments = (
        list(node.args.posonlyargs) + list(node.args.args) + list(node.args.kwonlyargs)
    )
    collector.names.update(argument.arg for argument in arguments)
    if node.args.vararg:
        collector.names.add(node.args.vararg.arg)
    if node.args.kwarg:
        collector.names.add(node.args.kwarg.arg)
    return collector.names - collector.global_names - collector.nonlocal_names


def _method_transform_name(node: ast.Call) -> str | None:
    """Return the class-style name for a supported DynamicFrame method call."""
    if not isinstance(node.func, ast.Attribute):
        return None
    name = _METHOD_TRANSFORMS.get(node.func.attr)
    if name == "Filter" and not (
        node.args or any(keyword.arg == "f" for keyword in node.keywords)
    ):
        return None
    if name == "ToDFOptions" and not (node.args or node.keywords):
        return None
    if name == "ShowOptions" and not any(
        keyword.arg == "num_rows" for keyword in node.keywords
    ):
        return None
    return name


def _transform_aliases(tree: ast.AST, hints: dict[str, str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "awsglue.transforms":
            for imported in node.names:
                if imported.name in hints:
                    aliases[imported.asname or imported.name] = imported.name
    return aliases


def _class_transform_name(
    node: ast.Call,
    aliases: dict[str, str],
    hints: dict[str, str],
) -> str | None:
    if not isinstance(node.func, ast.Attribute) or node.func.attr != "apply":
        return None
    owner = node.func.value
    if isinstance(owner, ast.Name):
        return aliases.get(owner.id, owner.id if owner.id in hints else None)
    if isinstance(owner, ast.Attribute) and owner.attr in hints:
        return owner.attr
    return None


def _is_dynamicframe_expression(
    expression: ast.AST,
    dynamic_names: set[str],
    aliases: dict[str, str],
    hints: dict[str, str],
) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id in dynamic_names
    if not isinstance(expression, ast.Call):
        return False
    function = expression.func
    if isinstance(function, ast.Attribute):
        if function.attr == "fromDF" and (
            isinstance(function.value, ast.Name) and function.value.id == "DynamicFrame"
            or isinstance(function.value, ast.Attribute)
            and function.value.attr == "DynamicFrame"
        ):
            return True
        if isinstance(function.value, ast.Attribute) \
                and function.value.attr == "create_dynamic_frame":
            return True
        if function.attr.startswith("create_dynamic_frame_from_"):
            return True
        if function.attr == "getFrame" and any(
            isinstance(child, ast.Attribute) and child.attr == "getSource"
            for child in ast.walk(function.value)
        ):
            return True
        if _class_transform_name(expression, aliases, hints):
            return True
        if function.attr in _DYNAMICFRAME_RETURNING_METHODS \
                and _is_dynamicframe_expression(
                    function.value, dynamic_names, aliases, hints,
                ):
            return True
    return False


def _is_dataframe_expression(
    expression: ast.AST,
    dataframe_names: set[str],
    dynamic_names: set[str],
    aliases: dict[str, str],
    hints: dict[str, str],
    spark_names: set[str] | None = None,
) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id in dataframe_names
    if not isinstance(expression, ast.Call) or not isinstance(
        expression.func, ast.Attribute,
    ):
        return False
    function = expression.func
    if function.attr == "toDF" and _is_dynamicframe_expression(
        function.value, dynamic_names, aliases, hints,
    ):
        return True
    known_spark_names = spark_names or {"spark"}
    if isinstance(function.value, ast.Name) and function.value.id in known_spark_names \
            and function.attr in {"createDataFrame", "range", "sql", "table"}:
        return True
    if any(
        isinstance(child, ast.Attribute)
        and child.attr in {"read", "readStream"}
        and isinstance(child.value, ast.Name)
        and child.value.id in known_spark_names
        for child in ast.walk(function.value)
    ):
        return True
    if function.attr == "agg" and _is_inline_grouped_dataframe_expression(
        function.value,
        dataframe_names,
        dynamic_names,
        aliases,
        hints,
        known_spark_names,
    ):
        return True
    return function.attr in _DATAFRAME_RETURNING_METHODS \
        and _is_dataframe_expression(
            function.value,
            dataframe_names,
            dynamic_names,
            aliases,
            hints,
            known_spark_names,
        )


def _is_inline_grouped_dataframe_expression(
    expression: ast.AST,
    dataframe_names: set[str],
    dynamic_names: set[str],
    aliases: dict[str, str],
    hints: dict[str, str],
    spark_names: set[str],
) -> bool:
    if not isinstance(expression, ast.Call) or not isinstance(
        expression.func, ast.Attribute,
    ):
        return False
    function = expression.func
    if function.attr in _GROUPED_DATAFRAME_BUILDERS:
        return _is_dataframe_expression(
            function.value,
            dataframe_names,
            dynamic_names,
            aliases,
            hints,
            spark_names,
        )
    return function.attr == "pivot" and _is_inline_grouped_dataframe_expression(
        function.value,
        dataframe_names,
        dynamic_names,
        aliases,
        hints,
        spark_names,
    )


def _match_bound_names(pattern: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(pattern):
        if node.__class__.__name__ in {"MatchAs", "MatchStar"}:
            name = getattr(node, "name", None)
            if name:
                names.add(name)
        elif node.__class__.__name__ == "MatchMapping":
            rest = getattr(node, "rest", None)
            if rest:
                names.add(rest)
    return names


def _match_pattern_is_irrefutable(pattern: ast.AST) -> bool:
    if pattern.__class__.__name__ == "MatchAs" \
            and getattr(pattern, "pattern", None) is None:
        return True
    if pattern.__class__.__name__ == "MatchOr":
        return any(
            _match_pattern_is_irrefutable(child)
            for child in getattr(pattern, "patterns", [])
        )
    return False


class _LineageAnalysis:
    """Record definite DynamicFrame/DataFrame provenance at each call site.

    RHS calls are inspected before an assignment updates the environment. This
    models straight-line Python execution, so self-reassignment and later
    resets neither erase earlier findings nor contaminate later receivers.
    Branch joins retain a type only when every reachable branch agrees.
    """

    _DYNAMIC = "dynamicframe"
    _DATAFRAME = "dataframe"
    _SPARK = "spark_session"
    _GLUE_SINK = "glue_sink"
    _GLUE_CONTEXT = "glue_context"
    _BATCH_WRITER = "batch_writer"
    _STREAM_WRITER = "stream_writer"
    _V2_WRITER = "v2_writer"
    _GROUPED_DATAFRAME = "grouped_dataframe"
    _STATIC_STRING = "static_string"
    _TRANSFORM_CLASS = "transform_class"

    def __init__(
        self,
        tree: ast.Module,
        aliases: dict[str, str],
        hints: dict[str, str],
    ) -> None:
        self.aliases = aliases
        self.hints = hints
        self.call_states: dict[
            int,
            tuple[
                frozenset[str],
                frozenset[str],
                frozenset[str],
                frozenset[str],
                frozenset[str],
                frozenset[str],
                frozenset[str],
                frozenset[str],
            ],
        ] = {}
        self.call_static_strings: dict[int, dict[str, str]] = {}
        self.call_transform_aliases: dict[int, dict[str, str]] = {}
        self.call_order: dict[int, int] = {}
        self.call_scope: dict[int, int] = {}
        self._next_order = 0
        self._current_scope = 0
        self._next_scope = 1
        self._analyze_block(tree.body, {"spark": self._SPARK})

    @staticmethod
    def _names(
        environment: dict[str, object], kind: str,
    ) -> set[str]:
        return {name for name, value in environment.items() if value == kind}

    def state_for(
        self, node: ast.Call,
    ) -> tuple[
        set[str], set[str], set[str], set[str], set[str], set[str], set[str], set[str],
    ]:
        empty = frozenset()
        dynamic, dataframe, sinks, contexts, spark, batch, stream, v2 = \
            self.call_states.get(
                id(node),
                (empty, empty, empty, empty, empty, empty, empty, empty),
        )
        return (
            set(dynamic),
            set(dataframe),
            set(sinks),
            set(contexts),
            set(spark),
            set(batch),
            set(stream),
            set(v2),
        )

    def static_strings_for(self, node: ast.Call) -> dict[str, str]:
        return self.call_static_strings.get(id(node), {})

    def transform_aliases_for(self, node: ast.Call) -> dict[str, str]:
        return self.call_transform_aliases.get(id(node), {})

    def _snapshot_calls(
        self, expression: ast.AST | None, environment: dict[str, object],
    ) -> None:
        if expression is None:
            return
        if isinstance(expression, ast.Lambda):
            for default in list(expression.args.defaults) + [
                value for value in expression.args.kw_defaults if value is not None
            ]:
                self._snapshot_calls(default, environment)
            local_names = {
                argument.arg
                for argument in (
                    list(expression.args.posonlyargs)
                    + list(expression.args.args)
                    + list(expression.args.kwonlyargs)
                )
            }
            if expression.args.vararg:
                local_names.add(expression.args.vararg.arg)
            if expression.args.kwarg:
                local_names.add(expression.args.kwarg.arg)
            outer_scope = self._current_scope
            self._current_scope = self._next_scope
            self._next_scope += 1
            try:
                self._snapshot_calls(
                    expression.body,
                    self._new_scope_environment(environment, local_names),
                )
            finally:
                self._current_scope = outer_scope
            return
        if isinstance(
            expression, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp),
        ):
            comprehension_env = environment.copy()
            for generator in expression.generators:
                self._snapshot_calls(generator.iter, comprehension_env)
                self._assign_target(generator.target, None, comprehension_env)
                for condition in generator.ifs:
                    self._snapshot_calls(condition, comprehension_env)
            if isinstance(expression, ast.DictComp):
                self._snapshot_calls(expression.key, comprehension_env)
                self._snapshot_calls(expression.value, comprehension_env)
            else:
                self._snapshot_calls(expression.elt, comprehension_env)
            return
        if isinstance(expression, ast.Call):
            self._snapshot_calls(expression.func, environment)
            for argument in expression.args:
                self._snapshot_calls(argument, environment)
            for keyword in expression.keywords:
                self._snapshot_calls(keyword.value, environment)
            self._record_call(expression, environment)
            return
        for child in ast.iter_child_nodes(expression):
            self._snapshot_calls(child, environment)

    def _record_call(
        self, node: ast.Call, environment: dict[str, object],
    ) -> None:
        if id(node) in self.call_states:
            return
        dynamic = frozenset(self._names(environment, self._DYNAMIC))
        dataframe = frozenset(self._names(environment, self._DATAFRAME))
        sinks = frozenset(self._names(environment, self._GLUE_SINK))
        contexts = frozenset(self._names(environment, self._GLUE_CONTEXT))
        spark = frozenset(self._names(environment, self._SPARK))
        batch = frozenset(self._names(environment, self._BATCH_WRITER))
        stream = frozenset(self._names(environment, self._STREAM_WRITER))
        v2 = frozenset(self._names(environment, self._V2_WRITER))
        static_strings = {
            name: value[1]
            for name, value in environment.items()
            if isinstance(value, tuple)
            and len(value) == 2
            and value[0] == self._STATIC_STRING
            and isinstance(value[1], str)
        }
        transform_aliases = {
            name: value[1]
            for name, value in environment.items()
            if isinstance(value, tuple)
            and len(value) == 2
            and value[0] == self._TRANSFORM_CLASS
            and isinstance(value[1], str)
        }
        self.call_states[id(node)] = (
            dynamic, dataframe, sinks, contexts, spark, batch, stream, v2,
        )
        self.call_static_strings[id(node)] = static_strings
        self.call_transform_aliases[id(node)] = transform_aliases
        self.call_order[id(node)] = self._next_order
        self.call_scope[id(node)] = self._current_scope
        self._next_order += 1

    def _new_scope_environment(
        self, environment: dict[str, object], local_names: set[str],
    ) -> dict[str, object]:
        scoped = {
            name: value for name, value in environment.items()
            if name not in local_names
        }
        if "spark" not in local_names:
            scoped["spark"] = self._SPARK
        return scoped

    def _expression_kind(
        self, expression: ast.AST, environment: dict[str, object],
    ) -> object | None:
        if isinstance(expression, ast.Name):
            return environment.get(expression.id)
        static_string = self._static_string(expression, environment)
        if static_string is not None:
            return (self._STATIC_STRING, static_string)
        dynamic = self._names(environment, self._DYNAMIC)
        dataframe = self._names(environment, self._DATAFRAME)
        spark = self._names(environment, self._SPARK)
        aliases = {
            name: value[1]
            for name, value in environment.items()
            if isinstance(value, tuple)
            and len(value) == 2
            and value[0] == self._TRANSFORM_CLASS
            and isinstance(value[1], str)
        }
        if _is_dynamicframe_expression(expression, dynamic, aliases, self.hints):
            return self._DYNAMIC
        if _is_dataframe_expression(
            expression, dataframe, dynamic, aliases, self.hints, spark,
        ):
            return self._DATAFRAME
        if isinstance(expression, ast.Attribute):
            base_kind = self._expression_kind(expression.value, environment)
            if expression.attr == "write" and base_kind == self._DATAFRAME:
                return self._BATCH_WRITER
            if expression.attr == "writeStream" and base_kind == self._DATAFRAME:
                return self._STREAM_WRITER
            if expression.attr == "spark_session" \
                    and base_kind == self._GLUE_CONTEXT:
                return self._SPARK
        if isinstance(expression, ast.Call) and isinstance(
            expression.func, ast.Attribute,
        ):
            function = expression.func
            receiver_kind = self._expression_kind(function.value, environment)
            if function.attr in _GROUPED_DATAFRAME_BUILDERS \
                    and receiver_kind == self._DATAFRAME:
                return self._GROUPED_DATAFRAME
            if function.attr == "pivot" \
                    and receiver_kind == self._GROUPED_DATAFRAME:
                return self._GROUPED_DATAFRAME
            if function.attr == "agg" \
                    and receiver_kind == self._GROUPED_DATAFRAME:
                return self._DATAFRAME
            if function.attr in _DATAFRAME_RETURNING_METHODS \
                    and receiver_kind == self._DATAFRAME:
                return self._DATAFRAME
            if function.attr == "getSink" and receiver_kind == self._GLUE_CONTEXT:
                return self._GLUE_SINK
            if function.attr == "writeTo" and receiver_kind == self._DATAFRAME:
                return self._V2_WRITER
            if receiver_kind == self._BATCH_WRITER \
                    and function.attr in _BATCH_WRITER_BUILDERS:
                return self._BATCH_WRITER
            if receiver_kind == self._STREAM_WRITER \
                    and function.attr in _STREAM_WRITER_BUILDERS:
                return self._STREAM_WRITER
            if receiver_kind == self._V2_WRITER \
                    and function.attr in _V2_WRITER_BUILDERS:
                return self._V2_WRITER
            if function.attr == "GlueContext":
                return self._GLUE_CONTEXT
            if function.attr == "getOrCreate" and any(
                isinstance(child, ast.Name) and child.id == "SparkSession"
                for child in ast.walk(function.value)
            ):
                return self._SPARK
        if isinstance(expression, ast.Call) and isinstance(expression.func, ast.Name) \
                and expression.func.id == "GlueContext":
            return self._GLUE_CONTEXT
        return None

    def _learn_glue_context_names(
        self, expression: ast.AST | None, environment: dict[str, object],
    ) -> None:
        if expression is None:
            return
        for node in ast.walk(expression):
            if isinstance(node, ast.Attribute) \
                    and _is_glue_context_method(node.attr) \
                    and isinstance(node.value, ast.Name):
                environment[node.value.id] = self._GLUE_CONTEXT

    def _static_string(
        self, expression: ast.AST, environment: dict[str, object],
    ) -> str | None:
        if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
            return expression.value
        if isinstance(expression, ast.Name):
            value = environment.get(expression.id)
            if isinstance(value, tuple) and len(value) == 2 \
                    and value[0] == self._STATIC_STRING \
                    and isinstance(value[1], str):
                return value[1]
            return None
        if isinstance(expression, ast.JoinedStr):
            parts: list[str] = []
            for value in expression.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                elif isinstance(value, ast.FormattedValue):
                    parts.append("?")
                else:
                    return None
            return "".join(parts)
        if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
            left = self._static_string(expression.left, environment)
            right = self._static_string(expression.right, environment)
            return left + right if left is not None and right is not None else None
        return None

    @staticmethod
    def _assign_target(
        target: ast.AST, kind: object | None, environment: dict[str, object],
    ) -> None:
        if isinstance(target, ast.Name):
            if kind is None:
                environment.pop(target.id, None)
            else:
                environment[target.id] = kind
        elif isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                _LineageAnalysis._assign_target(element, None, environment)
        elif isinstance(target, ast.Starred):
            _LineageAnalysis._assign_target(target.value, None, environment)

    @staticmethod
    def _merge_environments(
        environments: list[dict[str, object]],
    ) -> dict[str, object]:
        if not environments:
            return {}
        common = set(environments[0])
        for environment in environments[1:]:
            common.intersection_update(environment)
        return {
            name: environments[0][name]
            for name in common
            if all(environment[name] == environments[0][name]
                   for environment in environments[1:])
        }

    def _analyze_block(
        self, statements: list[ast.stmt], environment: dict[str, object],
    ) -> dict[str, object]:
        for statement in statements:
            environment = self._analyze_statement(statement, environment)
        return environment

    def _analyze_statement(
        self, statement: ast.stmt, environment: dict[str, object],
    ) -> dict[str, object]:
        if isinstance(statement, ast.Assign):
            self._snapshot_calls(statement.value, environment)
            self._learn_glue_context_names(statement.value, environment)
            kind = self._expression_kind(statement.value, environment)
            for target in statement.targets:
                self._assign_target(target, kind, environment)
            return environment
        if isinstance(statement, ast.AnnAssign):
            self._snapshot_calls(statement.value, environment)
            self._learn_glue_context_names(statement.value, environment)
            kind = self._expression_kind(statement.value, environment) \
                if statement.value is not None else None
            self._assign_target(statement.target, kind, environment)
            return environment
        if isinstance(statement, ast.AugAssign):
            self._snapshot_calls(statement.value, environment)
            self._assign_target(statement.target, None, environment)
            return environment
        if isinstance(statement, ast.Expr):
            self._snapshot_calls(statement.value, environment)
            self._learn_glue_context_names(statement.value, environment)
            return environment
        if isinstance(statement, ast.ImportFrom):
            for imported in statement.names:
                if imported.name == "*":
                    continue
                target = imported.asname or imported.name
                if statement.module == "awsglue.transforms" \
                        and imported.name in self.hints:
                    environment[target] = (self._TRANSFORM_CLASS, imported.name)
                else:
                    environment.pop(target, None)
            return environment
        if isinstance(statement, ast.Import):
            for imported in statement.names:
                environment.pop(imported.asname or imported.name.split(".", 1)[0], None)
            return environment
        if isinstance(statement, ast.If):
            self._snapshot_calls(statement.test, environment)
            before = environment.copy()
            body = self._analyze_block(statement.body, before.copy())
            otherwise = self._analyze_block(statement.orelse, before.copy()) \
                if statement.orelse else before
            return self._merge_environments([body, otherwise])
        if isinstance(statement, (ast.For, ast.AsyncFor)):
            self._snapshot_calls(statement.iter, environment)
            before = environment.copy()
            loop = before.copy()
            self._assign_target(statement.target, None, loop)
            loop = self._analyze_block(statement.body, loop)
            otherwise = self._analyze_block(statement.orelse, before.copy()) \
                if statement.orelse else before
            return self._merge_environments([before, loop, otherwise])
        if isinstance(statement, ast.While):
            self._snapshot_calls(statement.test, environment)
            before = environment.copy()
            loop = self._analyze_block(statement.body, before.copy())
            otherwise = self._analyze_block(statement.orelse, before.copy()) \
                if statement.orelse else before
            return self._merge_environments([before, loop, otherwise])
        if isinstance(statement, (ast.With, ast.AsyncWith)):
            for item in statement.items:
                self._snapshot_calls(item.context_expr, environment)
                if item.optional_vars is not None:
                    self._assign_target(item.optional_vars, None, environment)
            return self._analyze_block(statement.body, environment)
        if hasattr(ast, "Match") and isinstance(statement, ast.Match):
            self._snapshot_calls(statement.subject, environment)
            before = environment.copy()
            paths: list[dict[str, object]] = []
            exhaustive = False
            for case in statement.cases:
                case_environment = before.copy()
                for name in _match_bound_names(case.pattern):
                    case_environment.pop(name, None)
                self._snapshot_calls(case.guard, case_environment)
                paths.append(self._analyze_block(case.body, case_environment))
                if case.guard is None and _match_pattern_is_irrefutable(case.pattern):
                    exhaustive = True
            if not exhaustive:
                paths.append(before)
            return self._merge_environments(paths)
        if isinstance(statement, _TRY_STATEMENTS):
            before = environment.copy()
            body = self._analyze_block(statement.body, before.copy())
            if statement.orelse:
                body = self._analyze_block(statement.orelse, body)
            paths = [body]
            for handler in statement.handlers:
                handler_env = before.copy()
                if handler.name:
                    handler_env.pop(handler.name, None)
                paths.append(self._analyze_block(handler.body, handler_env))
            merged = self._merge_environments(paths)
            return self._analyze_block(statement.finalbody, merged)
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for expression in (
                list(statement.decorator_list)
                + list(statement.args.defaults)
                + [value for value in statement.args.kw_defaults if value is not None]
            ):
                self._snapshot_calls(expression, environment)
            function_env = self._new_scope_environment(
                environment, _function_local_names(statement),
            )
            outer_scope = self._current_scope
            self._current_scope = self._next_scope
            self._next_scope += 1
            try:
                self._analyze_block(statement.body, function_env)
            finally:
                self._current_scope = outer_scope
            environment.pop(statement.name, None)
            return environment
        if isinstance(statement, ast.ClassDef):
            for expression in list(statement.decorator_list) + list(statement.bases):
                self._snapshot_calls(expression, environment)
            outer_scope = self._current_scope
            self._current_scope = self._next_scope
            self._next_scope += 1
            try:
                self._analyze_block(
                    statement.body,
                    self._new_scope_environment(environment, set()),
                )
            finally:
                self._current_scope = outer_scope
            environment.pop(statement.name, None)
            return environment
        if isinstance(statement, ast.Delete):
            for target in statement.targets:
                self._assign_target(target, None, environment)
            return environment

        # Return, raise, assert, imports, and version-specific simple statements.
        for _field, value in ast.iter_fields(statement):
            if isinstance(value, ast.expr):
                self._snapshot_calls(value, environment)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.expr):
                        self._snapshot_calls(item, environment)
        return environment


def _lineage_analysis(
    tree: ast.Module,
    aliases: dict[str, str],
    hints: dict[str, str],
) -> _LineageAnalysis:
    return _LineageAnalysis(tree, aliases, hints)


def _rule_flag_transforms(text: str, findings: list[Finding], ns: str) -> str:
    hints = dict(_FLAG_TRANSFORMS)
    detected: set[str] = set()
    try:
        tree = ast.parse(text)
    except SyntaxError:
        tree = None
    if tree is not None:
        aliases = _transform_aliases(tree, hints)
        lineage = _lineage_analysis(tree, aliases, hints)
        glue_script = _GLUE_SCRIPT_MARKER.search(text) is not None
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            dynamic_names, *_other_names = lineage.state_for(node)
            call_aliases = lineage.transform_aliases_for(node)
            method_name = _method_transform_name(node)
            if method_name and isinstance(node.func, ast.Attribute) and (
                (glue_script and _is_provenance_free_transform(node))
                or _is_dynamicframe_expression(
                    node.func.value, dynamic_names, call_aliases, hints,
                )
            ):
                detected.add(method_name)
            class_name = _class_transform_name(node, call_aliases, hints)
            if class_name:
                detected.add(class_name)
    for name, hint in _FLAG_TRANSFORMS:
        if name in detected or _search_code(re.compile(rf"\b{name}\.apply\s*\("), text):
            findings.append(Finding(f"transform_{name.lower()}", hint, "flag"))
    return text


_SPARK_PATH_CALL = re.compile(
    r"(?P<prefix>\.(?:load|save|parquet|json|csv|orc|text|textFile)\s*\(\s*)"
    r"(?P<quote>['\"])(?P<uri>s3a?://[^'\"]*)(?P=quote)"
)


def _rule_spark_io_paths(text: str, findings: list[Finding], ns: str) -> str:
    """Rewrite S3 literals only in recognized Spark filesystem operations."""
    spans = _token_spans(text, {tokenize.COMMENT, tokenize.STRING})
    rewritten: set[str] = set()

    def replace(match: re.Match) -> str:
        # The match begins at the executable method call.  Its URI is a string
        # by design, so shielding the whole match would suppress the safe rule.
        if _inside_spans(match.start(), spans):
            return match.group(0)
        uri = match.group("uri")
        translated = _s3_to_oci(uri, ns)
        if translated == uri:
            return match.group(0)
        rewritten.add(uri)
        quote = match.group("quote")
        return f"{match.group('prefix')}{quote}{translated}{quote}"

    out = _SPARK_PATH_CALL.sub(replace, text)
    if rewritten:
        findings.append(Finding(
            "spark_io_paths",
            f"rewrote {len(rewritten)} Spark I/O S3 path(s) → OCI",
            "rewrite",
        ))
    return out


_SPARK_WRITE_ACTIONS = {
    "csv",
    "insertInto",
    "jdbc",
    "json",
    "orc",
    "parquet",
    "save",
    "saveAsTable",
    "text",
}
_SPARK_STREAM_WRITE_ACTIONS = {"start", "toTable"}
_SPARK_V2_WRITE_ACTIONS = {
    "append",
    "create",
    "createOrReplace",
    "overwrite",
    "overwritePartitions",
    "replace",
}
_SPARK_SQL_WRITE = re.compile(
    r"\s*(?:INSERT|MERGE|UPDATE|DELETE|TRUNCATE|"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|REPLACE\s+TABLE|DROP\s+TABLE|"
    r"ALTER\s+TABLE)\b",
    re.IGNORECASE,
)


def _contains_attribute(node: ast.AST, names: set[str]) -> bool:
    return any(
        isinstance(child, ast.Attribute) and child.attr in names
        for child in ast.walk(node)
    )


def _is_stored_writer_expression(
    expression: ast.AST,
    writer_names: set[str],
    builder_names: set[str],
) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id in writer_names
    return (
        isinstance(expression, ast.Call)
        and isinstance(expression.func, ast.Attribute)
        and expression.func.attr in builder_names
        and _is_stored_writer_expression(
            expression.func.value, writer_names, builder_names,
        )
    )


def _is_glue_sink_expression(
    expression: ast.AST,
    sink_names: set[str],
) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id in sink_names
    # getSink exists only on GlueContext, so the spelling alone identifies a sink.
    return (
        isinstance(expression, ast.Call)
        and isinstance(expression.func, ast.Attribute)
        and expression.func.attr == "getSink"
    )


def _static_string_value(
    expression: ast.AST, static_strings: dict[str, str],
) -> str | None:
    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return expression.value
    if isinstance(expression, ast.Name):
        return static_strings.get(expression.id)
    if isinstance(expression, ast.JoinedStr):
        parts: list[str] = []
        for value in expression.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                parts.append("?")
            else:
                return None
        return "".join(parts)
    if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
        left = _static_string_value(expression.left, static_strings)
        right = _static_string_value(expression.right, static_strings)
        return left + right if left is not None and right is not None else None
    return None


def _strip_leading_sql_comments(sql: str) -> str:
    remainder = sql
    while True:
        remainder = remainder.lstrip()
        if remainder.startswith("--"):
            newline = remainder.find("\n")
            return "" if newline < 0 else _strip_leading_sql_comments(
                remainder[newline + 1:],
            )
        if remainder.startswith("/*"):
            close = remainder.find("*/", 2)
            return "" if close < 0 else _strip_leading_sql_comments(
                remainder[close + 2:],
            )
        return remainder


def _spark_sql_write_risk(
    node: ast.Call, static_strings: dict[str, str],
) -> bool:
    expression: ast.AST | None = node.args[0] if node.args else None
    if expression is None:
        expression = next(
            (
                keyword.value
                for keyword in node.keywords
                if keyword.arg == "sqlQuery"
            ),
            None,
        )
    if expression is None:
        return True
    sql = _static_string_value(expression, static_strings)
    if sql is None:
        # With a bookmark-bearing source, an unresolved SQL statement cannot be
        # proven read-only. Fail closed instead of silently approving DML.
        return True
    sql = _strip_leading_sql_comments(sql)
    if sql.startswith("?"):
        # The leading interpolated value may itself be INSERT/MERGE/etc.; its
        # statement kind is not statically knowable.
        return True
    return _SPARK_SQL_WRITE.match(sql) is not None


def _is_output_call(
    node: ast.Call,
    dataframe_names: set[str],
    dynamic_names: set[str],
    sink_names: set[str],
    context_names: set[str],
    spark_names: set[str],
    batch_writer_names: set[str],
    stream_writer_names: set[str],
    v2_writer_names: set[str],
    static_strings: dict[str, str],
    aliases: dict[str, str],
    hints: dict[str, str],
) -> bool:
    if not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr == "write" and _is_dynamicframe_expression(
        node.func.value, dynamic_names, aliases, hints,
    ):
        return True
    if node.func.attr == "writeFrame":
        return _is_glue_sink_expression(node.func.value, sink_names)
    # write_dynamic_frame is Glue-only in every spelling, so no receiver proof is
    # needed: a bookmarked job that writes through it must never pass silently.
    if node.func.attr.startswith("write_dynamic_frame_from_") \
            or _contains_attribute(node.func, {"write_dynamic_frame"}):
        return True
    if node.func.attr == "sql" and isinstance(node.func.value, ast.Name) \
            and node.func.value.id in spark_names:
        return _spark_sql_write_risk(node, static_strings)
    # This classifier only runs once a bookmark-bearing Glue read exists, so a
    # writer chain (`x.write.parquet(...)`, `x.writeStream...start()`,
    # `x.writeTo(...).append()`) counts as output even when `x` came through a
    # helper, a container, or a parameter that lineage cannot type.
    if node.func.attr in _SPARK_WRITE_ACTIONS:
        return _is_stored_writer_expression(
            node.func.value, batch_writer_names, _BATCH_WRITER_BUILDERS,
        ) or _contains_attribute(node.func, {"write", "writeStream"})
    if node.func.attr in _SPARK_STREAM_WRITE_ACTIONS:
        return _is_stored_writer_expression(
            node.func.value, stream_writer_names, _STREAM_WRITER_BUILDERS,
        ) or _contains_attribute(node.func, {"writeStream"})
    if node.func.attr in _SPARK_V2_WRITE_ACTIONS:
        return _is_stored_writer_expression(
            node.func.value, v2_writer_names, _V2_WRITER_BUILDERS,
        ) or _contains_attribute(node.func, {"writeTo"})
    return False


def _rule_flag_job_bookmark(text: str, findings: list[Finding], ns: str) -> str:
    """Flag stateful Glue inputs when the script also produces output.

    A non-empty or dynamic ``transformation_ctx`` can identify bookmark state.
    The translator cannot see the job-level bookmark setting, so a write makes
    both incremental append and full-table replacement semantics unsafe to
    approve automatically.  An explicit empty string is Glue's stateless form.

    Output detection is deliberately fail-closed: once a bookmarked read exists,
    any Spark writer chain or Glue-only write spelling counts as output even when
    lineage cannot type its receiver (helpers, containers, parameters).  A
    bookmarked job that only writes an unrelated report is therefore reviewed
    rather than risking a silent full-table append.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return text

    hints = dict(_FLAG_TRANSFORMS)
    aliases = _transform_aliases(tree, hints)
    lineage = _lineage_analysis(tree, aliases, hints)
    bookmark_events: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not any(
            keyword.arg == "transformation_ctx"
            and not (
                isinstance(keyword.value, ast.Constant)
                and keyword.value.value == ""
            )
            for keyword in node.keywords
        ):
            continue
        (
            dynamic_names,
            _dataframe_names,
            _sink_names,
            context_names,
            *_other_names,
        ) = lineage.state_for(node)
        call_aliases = lineage.transform_aliases_for(node)
        # transformation_ctx is itself Glue-only, so any reader/writer spelling
        # carrying it is bookmark state regardless of receiver provenance.
        if _is_dynamicframe_expression(node, dynamic_names, call_aliases, hints) \
                or (
                    isinstance(node.func, ast.Attribute)
                    and _is_glue_context_method(node.func.attr)
                ) \
                or _contains_attribute(node.func, _GLUE_CONTEXT_METHOD_NAMES):
            bookmark_events.append((
                lineage.call_scope.get(id(node), 0),
                lineage.call_order.get(id(node), 0),
            ))
    output_events: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        (
            dynamic_names,
            dataframe_names,
            sink_names,
            context_names,
            spark_names,
            batch_writer_names,
            stream_writer_names,
            v2_writer_names,
        ) = lineage.state_for(node)
        call_aliases = lineage.transform_aliases_for(node)
        if _is_output_call(
            node,
            dataframe_names,
            dynamic_names,
            sink_names,
            context_names,
            spark_names,
            batch_writer_names,
            stream_writer_names,
            v2_writer_names,
            lineage.static_strings_for(node),
            call_aliases,
            hints,
        ):
            output_events.append((
                lineage.call_scope.get(id(node)),
                lineage.call_order.get(id(node)),
            ))
    # An output call the lineage pass never reached (unrecorded order) cannot be
    # proven to precede the bookmarked read, so it counts as a later write.
    if any(
        output_scope is None
        or bookmark_scope != output_scope
        or bookmark_order <= output_order
        for bookmark_scope, bookmark_order in bookmark_events
        for output_scope, output_order in output_events
    ):
        findings.append(Finding(
            "job_bookmark",
            "transformation_ctx is present in a writing job, but Glue bookmark state "
            "is not migrated; define an explicit AIDP watermark/checkpoint and verify "
            "append deduplication or full-table replacement semantics",
            "flag",
        ))
    return text


# Methods that exist on GlueContext and nowhere on SparkSession. The translator
# rewrites the GlueContext receiver to a SparkSession, so any of these left in the
# output resolve against SparkSession at runtime and raise AttributeError on the
# job's first executable line -- while a status-only verdict still reads PASS.
# Previously only getSource/getSink/create_data_frame were listed, so streaming,
# purge, transition and transaction jobs translated to a silent PASS.
_GLUECONTEXT_ONLY_METHODS = (
    "getSource", "getSink", "getSourceWithFormat", "getSinkWithFormat",
    "getSampleStreamingDynamicFrame",
    "create_data_frame", "create_data_frame_from_catalog",
    "create_data_frame_from_options",
    "forEachBatch",
    "purge_table", "purge_s3_path",
    "transition_table", "transition_s3_path",
    "start_transaction", "commit_transaction", "cancel_transaction",
    "extract_jdbc_conf", "write_from_options", "add_ingestion_time_columns",
)

_RESIDUAL_GLUE_API_PATTERN = re.compile(
    r"\b(?:awsglue|GlueContext|DynamicFrame|Job)\b|"
    r"\.(?:create|write)_dynamic_frame(?:_from_\w+)?\b|"
    # longest-first so create_data_frame does not shadow its _from_* variants
    r"\.(?:" + "|".join(sorted(_GLUECONTEXT_ONLY_METHODS, key=len, reverse=True))
    + r")\s*\(|"
    r"getResolvedOptions\s*\("
)


def _rule_flag_residual_aws(text: str, findings: list[Finding], ns: str) -> str:
    """A PASS artifact must not retain executable Glue APIs or unknown S3 literals."""
    masked = list(text)
    spans = _token_spans(text, {tokenize.COMMENT, tokenize.STRING})
    for start, end in spans:
        for index in range(start, end):
            if masked[index] not in "\r\n":
                masked[index] = " "
    code = "".join(masked)
    if _RESIDUAL_GLUE_API_PATTERN.search(code):
        findings.append(Finding(
            "residual_glue_api",
            "translated script still contains executable Glue-only API usage",
            "flag",
        ))

    unresolved_paths = 0
    try:
        tree = ast.parse(text)
        # Constant string nodes include ordinary/raw/bytes-adjacent string
        # literals and the literal segments of JoinedStr f-strings on every
        # supported Python version.  This avoids relying on the tokenizer's
        # version-specific STRING versus FSTRING_MIDDLE representation.
        unresolved_paths = _s3_literal_count(tree)
    except (SyntaxError, ValueError):
        pass
    if unresolved_paths:
        findings.append(Finding(
            "s3_path_unhandled",
            f"left {unresolved_paths} S3 literal(s) unchanged outside recognized Spark I/O calls",
            "flag",
        ))
    return text


_RULES = [
    # Capture aliases while their import statements are still available.
    _rule_flag_glue_aliases,
    _rule_flag_transforms,
    # Inspect bookmark/write coupling before transformation_ctx and Glue sink
    # calls are removed by the deterministic rewrites below.
    _rule_flag_job_bookmark,
    _rule_glue_imports,
    _rule_context_bootstrap,
    _rule_job_lifecycle,
    _rule_resolved_options,
    _rule_read_from_catalog,
    # Collapse DynamicFrame.fromDF(df, ...) → df BEFORE the from_options rules,
    # so an inline frame=DynamicFrame.fromDF(df, ...) resolves to frame=df
    # instead of the write rule grabbing the literal "DynamicFrame" token.
    _rule_dynamicframe_fromdf,
    _rule_read_from_options,
    _rule_write_from_options,
    _rule_spark_io_paths,
    _rule_flag_residual_aws,
]

# Every translation or review rule is gated by one of these markers. Checking
# them once lets ordinary PySpark/Python inputs avoid the repeated tokenization
# and AST walks used by Glue-specific rules. Transform names are matched as
# complete identifiers: this covers Python comments and explicit line
# continuations around attribute access while excluding names such as MapType.
_GLUE_MIGRATION_LITERALS = (
    "awsglue",
)
_GLUE_MIGRATION_CODE_PATTERN = re.compile(
    r"\b(?:GlueContext|SparkContext|DynamicFrame|Job)\b|\.spark_session\b|"
    r"\.(?:create|write)_dynamic_frame(?:_from_\w+)?\b|"
    r"\.(?:getSource|getSink|create_data_frame)\s*\(|getResolvedOptions\s*\(|"
    r"\btransformation_ctx\s*="
)
_GLUE_TRANSFORM_NAME_PATTERN = re.compile(
    rf"\b(?:{'|'.join(re.escape(name) for name, _hint in _FLAG_TRANSFORMS)})\b"
)
_AMBIGUOUS_METHOD_MARKERS = {
    "filter", "schema", "show", "toDF", "union", "write",
}
_GLUE_METHOD_TRANSFORM_NAME_PATTERN = re.compile(
    rf"\b(?:{'|'.join(re.escape(name) for name in _METHOD_TRANSFORMS if name not in _AMBIGUOUS_METHOD_MARKERS)})\b"
)


def _requires_glue_migration(script: str, tree: ast.AST | None = None) -> bool:
    if (
        any(marker in script for marker in _GLUE_MIGRATION_LITERALS)
        or _S3_URI_PATTERN.search(script) is not None
        or _GLUE_MIGRATION_CODE_PATTERN.search(script) is not None
        or _GLUE_TRANSFORM_NAME_PATTERN.search(script) is not None
        or _GLUE_METHOD_TRANSFORM_NAME_PATTERN.search(script) is not None
    ):
        return True
    # Python folds adjacent string tokens in its AST.  Avoid an AST walk on the
    # ordinary fast path, but catch spellings such as "s3" "://bucket/key".
    if "s3" in script.lower() and "://" in script:
        if tree is None:
            try:
                tree = ast.parse(script)
            except (SyntaxError, ValueError):
                return True
        if _s3_literal_count(tree):
            return True
    # `filter` is common native Spark syntax, so only route it through the Glue
    # rules when its AST has Glue's method-style `f=` callback signature.
    if "filter" not in script or "f" not in script:
        return False
    if tree is None:
        try:
            tree = ast.parse(script)
        except (SyntaxError, ValueError):
            return True
    return any(
        isinstance(node, ast.Call) and _method_transform_name(node) == "Filter"
        for node in ast.walk(tree)
    )


def _add_namespace_findings(
    translated_script: str,
    oci_namespace: str,
    findings: list[Finding],
) -> None:
    if oci_namespace != _OCI_NAMESPACE_PLACEHOLDER and not _valid_oci_namespace(oci_namespace):
        findings.append(Finding(
            "oci_namespace_invalid",
            "OCI namespace must contain only lowercase letters, digits, underscores, or hyphens; S3 paths were left unchanged",
            "flag",
        ))
    if f"@{_OCI_NAMESPACE_PLACEHOLDER}" in translated_script:
        findings.append(Finding(
            "oci_namespace_missing",
            "OCI paths contain a namespace placeholder; provide the target tenancy namespace",
            "flag",
        ))


def translate(script: str, *, oci_namespace: str = "<your-oci-namespace>") -> TranslationResult:
    findings: list[Finding] = []
    try:
        source_tree = ast.parse(script)
    except (SyntaxError, ValueError) as error:
        findings.append(Finding(
            "source_python_invalid",
            f"source is not valid Python and was left unchanged: {error.msg if isinstance(error, SyntaxError) else error}",
            "flag",
        ))
        return TranslationResult(source_sql=script, translated_sql=script, findings=findings)
    if not _requires_glue_migration(script, source_tree):
        _add_namespace_findings(script, oci_namespace, findings)
        return TranslationResult(source_sql=script, translated_sql=script, findings=findings)
    out = script
    for rule in _RULES:
        out = rule(out, findings, oci_namespace)
    _add_namespace_findings(out, oci_namespace, findings)
    try:
        ast.parse(out)
    except SyntaxError as error:
        # Never emit a newly broken artifact.  Retaining the valid source with a
        # review gate is safer than handing downstream execution partial code.
        findings = [finding for finding in findings if finding.severity == "flag"]
        findings.append(Finding(
            "translation_syntax_guard",
            f"automatic rewrites were rolled back because generated Python was invalid: {error.msg}",
            "flag",
        ))
        out = script
    return TranslationResult(source_sql=script, translated_sql=out, findings=findings)
