"""Remove literal Slack tokens from delivered artifacts, never execution code.

The migration kernel still needs its original credentials for read-only Slack
calls. This pass therefore runs on a COPY at save time.

A saved notebook has each credential literal replaced IN PLACE with an inert
placeholder string (`'<redacted>'`). It deliberately does NOT emit a runtime
`os.environ[...]` lookup: the Slack CALL that would consume the credential is
separately commented out by the execution-boundary neutraliser, so the value is
dead code, and an env lookup in an EAGER position (a default parameter value, a
module-level or class-body assignment) raised KeyError at import/def time and
bricked notebooks that never call Slack (AutoRefit derived_raw, 2026-09-22). A
placeholder is evaluation-safe in every position; the customer restores their
own token if they re-enable the call. There is nothing for an operator to
configure, so `configured_env` is empty by construction.

Scope discipline: artifact redaction is an OPTIONAL feature
sitting on the save-back path that migrates every notebook. It must therefore
be a provable no-op for a notebook with no Slack credential. `build_redactor`
returns an inert `NULL_REDACTOR` unless a credential is actually discovered,
so the common path keeps byte-identical artifacts, its existing cache
semantics, and no new failure surface. Recovery is limited to full-line comments
containing prose. Existing live-code and inline-comment behavior is preserved;
unresolved authentication in recovered comments refuses delivery rather than
saving an unverifiable credential.
"""
from __future__ import annotations

import ast
import copy
import hashlib
import io
import re
import textwrap
import tokenize

from slack_ast_neutralize import (
    SLACK_CTORS, _MAGIC_WITH_CODE_RE, _evolve, _logical_lines, _strip_magics,
    new_bindings,
    _constructor_roles, _tracked_constructor_function, _union_states,
)


class SlackRedactionError(ValueError):
    """Artifact cannot be safely redacted; message must never contain source."""


class SourcePreview(str):
    """Bounded result text with its full source retained only for artifact rendering."""

    def __new__(cls, source, limit):
        preview = super().__new__(cls, source[:limit])
        preview.source = source
        preview.limit = limit
        return preview

    def __getnewargs__(self):
        return self.source, self.limit

    def redacted(self, redactor):
        return redactor.preview_text(self.source)[:self.limit]


# Candidate shape of a Slack credential in FREE TEXT (markdown, outputs,
# reports, logs) where no constructor proves intent. Broad on purpose -- it
# gates discovery, `_looks_like_slack_token` decides.
_TOKEN = re.compile(r"\b(?:xox[baprse]|xapp)-[A-Za-z0-9-]+")

# A real token is `<prefix>-<team>-<id>-<secret>`: long, multi-segment and
# digit-bearing. The original prefix-only catch-all rewrote ordinary customer
# strings -- anything merely STARTING with a Slack prefix, e.g. a legacy
# hyphenated name -- to `<redacted>` in notebooks with no Slack code at all,
# silently mutating unrelated artifacts.
_MIN_TOKEN_LEN = 24

# Blind substring replacement is only safe for a value distinctive enough that
# it cannot occur incidentally. A short or wordlike constructor literal is a
# placeholder, not a credential: rewriting every occurrence of `'a'` or
# `'test'` across code, markdown, outputs and four reports corrupts the
# artifact. Such a literal is still removed from the constructor itself, which
# is AST-precise and cannot touch anything else.
_MIN_SCRUB_LEN = 8
_PLACEHOLDERS = frozenset({
    "none", "null", "nil", "true", "false", "token", "secret", "password",
    "changeme", "change-me", "test", "testing", "dummy", "fake", "sample",
    "example", "placeholder", "your-token", "xxx", "todo", "tbd", "n/a", "na",
})

_ANNOTATION = "# Oracle tool modification: literal Slack token removed; configure "
_PLACEHOLDER = "<redacted>"
# Cell-level note prepended when a Slack credential literal was replaced IN PLACE
# with the inert placeholder. There is deliberately nothing here for the operator
# to "configure": the Slack call that would read the credential is separately
# commented out by the execution-boundary neutraliser (slack_ast_neutralize), so
# the placeholder is never evaluated as a token. This pass no longer injects a
# runtime `os.environ[...]` lookup -- that lookup added nothing runnable and, in an
# eager position (a default parameter value, a module-level or class-body
# assignment), raised KeyError at import/def time and broke notebooks that never
# call Slack (AutoRefit derived_raw, run c4fe5757, KeyError SLACK_TOKEN_EXAMPLE01,
# 2026-09-22). The customer restores their own token if they re-enable the call.
_ANNOTATION_REMOVED = (
    "# Oracle tool modification: Slack credential literal(s) removed "
    "(replaced with '<redacted>'); the Slack call is neutralised -- restore "
    "your own token if you re-enable it")

# A COMPLETE string literal. Used to tell an unparseable line that may hide a
# credential from prose that merely mentions Slack.
_SINGLE = r"'''(?:[^\\]|\\.)*?'''|'(?:[^'\\\n]|\\.)*'"
_DOUBLE = r'"""(?:[^\\]|\\.)*?"""|"(?:[^"\\\n]|\\.)*"'
_STRING_LITERAL_RE = re.compile("(?:" + _SINGLE + "|" + _DOUBLE + ")", re.S)
# Paired intraword apostrophes (client's / isn't) are prose. A literal may
# still touch a label on one side, as in prose'backup-token'.
_COMMENT_LITERAL_RE = re.compile(
    r"(?<!\w)(?:[rbuft]{0,2})" + _STRING_LITERAL_RE.pattern
    + "|" + _STRING_LITERAL_RE.pattern + r"(?!\w)", re.I | re.S)

_CTOR_WORD_RE = re.compile(r"\b(?:" + "|".join(sorted(SLACK_CTORS)) + r")\b")
_SLACK_WORD_RE = re.compile(r"slack", re.I)


def _looks_like_slack_token(value):
    """Is this free-text string actually shaped like a Slack credential?"""
    return (len(value) >= _MIN_TOKEN_LEN
            and value.count("-") >= 2
            and any(character.isdigit() for character in value)
            and _TOKEN.fullmatch(value) is not None)


def _is_scrubbable(value):
    """Is this value distinctive enough to replace everywhere it appears?"""
    stripped = value.strip()
    if len(stripped) < _MIN_SCRUB_LEN or stripped.lower() in _PLACEHOLDERS:
        return False
    # A bare word occurs in prose; a separator or digit makes it identifying.
    return any(c.isdigit() or not c.isalnum() for c in stripped)


def _names_re(names):
    """Word-boundary matcher for a set of Slack client / module names."""
    return re.compile(r"\b(?:" + "|".join(re.escape(n) for n in sorted(names)) + r")\b")


def scrub_text(text):
    """Remove anything token-SHAPED from free text, with no learned state.

    For the refusal path, where no redactor could be built but the reports
    still have to reach the operator.
    """
    return _TOKEN.sub(
        lambda m: _PLACEHOLDER if _looks_like_slack_token(m.group()) else m.group(),
        text)


def _credential_risk(text, names):
    """True when an unparseable line could be hiding a hardcoded credential.

    Requires a Slack client name used as a CONSTRUCTOR CALL with a complete
    string literal among its arguments, on one logical line. Merely sharing a
    line is not enough: `print 'WebClient failed'` (a Python-2 cell in a
    notebook that imports slack_sdk) has both a name and a literal, and
    failing a whole task over it was the over-eager behaviour.

    Conservative on purpose: a call appearing inside a string or a comment on
    an unparseable line is matched textually and refuses. A mention that does
    NOT refuse is still surfaced through `unverified`, so no unparseable cell
    passes silently either way.

    A chunk carrying this module's own placeholder is skipped outright. The
    preserved comment it writes -- `# client = WebClient(token=<redacted>,
    base_url='https://…')` -- keeps any OTHER argument's literal, so on a
    cold-cache resume it would otherwise look exactly like a hidden
    credential and fail every re-run of an already-redacted notebook.
    """
    if not names:
        return False
    name_re = _names_re(names)
    lines = text.splitlines()
    for group in _logical_lines(text):
        chunk = "\n".join(lines[i] for i in group if i < len(lines))
        if _PLACEHOLDER in chunk:
            continue  # already redacted by this pass
        for match in name_re.finditer(chunk):
            arguments = chunk[match.end():]
            if not arguments.lstrip().startswith("("):
                continue  # a mention, not a construction
            if _STRING_LITERAL_RE.search(arguments):
                return True
    return False


def _comment_call_end(source):
    """Return the end offset of a balanced call, or None for incomplete syntax."""
    starts = [0]
    for line in source.splitlines(keepends=True):
        starts.append(starts[-1] + len(line))
    stack = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type == tokenize.ERRORTOKEN and not token.string.isspace():
                return None
            if token.type != tokenize.OP:
                continue
            if token.string in "([{":
                stack.append(token.string)
            elif token.string in ")]}":
                if not stack or stack.pop() != {")": "(", "]": "[", "}": "{"}[token.string]:
                    return None
                if not stack:
                    return starts[token.end[0] - 1] + token.end[1]
    except (tokenize.TokenError, IndentationError):
        return None
    return None


def _comment_fragments(source):
    """Yield legacy full-line comment spans and whether recovery is allowed."""
    lines = source.splitlines(keepends=True)
    comments, locations = [], []
    offset = 0
    for line in lines:
        match = re.match(r"\s*#(.*)", line)
        comments.append(match.group(1) if match else "")
        locations.append(offset + match.start(1) if match else offset)
        offset += len(line)
    # The old line scan also saw # lines inside multiline strings. Preserve
    # its full-statement checks there, but never recover prose as string code.
    comment_rows = set()
    try:
        for token in tokenize.generate_tokens(io.StringIO(_strip_magics(source)).readline):
            if token.type == tokenize.COMMENT:
                comment_rows.add(token.start[0] - 1)
    except (tokenize.TokenError, IndentationError):
        pass  # Legacy full-line scanning does not require a valid token stream.
    for group in _logical_lines("\n".join(comments)):
        fragment = textwrap.dedent("\n".join(comments[i] for i in group))
        mapping = []
        for row, i in zip(fragment.split("\n"), group):
            start = locations[i] + len(comments[i]) - len(row)
            mapping.extend(range(start, start + len(row)))
            mapping.append(start + len(row))
        yield fragment, mapping, all(i in comment_rows for i in group if comments[i])


def _comment_strings(fragment, names, quoted):
    """String spans proved by a parseable prefix, not by quotes in prose."""
    starts = [0]
    for line in fragment.splitlines(keepends=True):
        starts.append(starts[-1] + len(line))
    proven = set()
    formatted = []
    string_nodes = (ast.Constant, ast.JoinedStr, getattr(ast, "TemplateStr", ast.JoinedStr))
    try:
        for token in tokenize.generate_tokens(io.StringIO(fragment).readline):
            hi = starts[token.end[0] - 1] + token.end[1]
            kind = tokenize.tok_name[token.type]
            lo = starts[token.start[0] - 1] + token.start[1]
            if kind in {"FSTRING_START", "TSTRING_START"}:
                formatted.append(lo)
            elif kind in {"FSTRING_END", "TSTRING_END"}:
                quoted.append((formatted.pop(), hi))
            elif token.type == tokenize.STRING:
                quoted.append((lo, hi))
            if not (token.type == tokenize.STRING
                    or kind in {"FSTRING_END", "TSTRING_END"}
                    or (token.type == tokenize.OP and token.string in ")]};")):
                continue
            prefix = fragment[:hi].lstrip()
            try:
                tree = ast.parse(prefix)
            except SyntaxError:
                continue
            rows = prefix.splitlines(keepends=True)
            row_starts = [0]
            for row in rows:
                row_starts.append(row_starts[-1] + len(row))

            def offset(row, column):
                return (hi - len(prefix) + row_starts[row - 1]
                        + len(rows[row - 1].encode("utf-8")[:column].decode("utf-8")))

            for node in ast.walk(tree):
                if not isinstance(node, string_nodes):
                    continue
                if isinstance(node, ast.Constant) and not isinstance(node.value, (str, bytes)):
                    continue
                if any(isinstance(child, ast.Call)
                       and _names_re(names).search(ast.unparse(child.func))
                       for child in ast.walk(node)):
                    raise SyntaxError("cannot verify a Slack call inside a formatted string")
                span = (offset(node.lineno, node.col_offset),
                        offset(node.end_lineno, node.end_col_offset))
                if span not in proven:
                    proven.add(span)
                    yield span
    except (tokenize.TokenError, IndentationError):
        # Only already-proven prefix spans are trusted. The residual check
        # below still sees every unproved constructor and its raw arguments.
        return


def _residual_credential_risk(raw, names):
    """Check every remaining name against raw arguments, one call at a time."""
    for match in _names_re(names).finditer(raw):
        arguments = raw[match.end():]
        if not arguments.lstrip().startswith("("):
            continue
        end = _comment_call_end(raw[match.start():])
        if end is None:
            if _STRING_LITERAL_RE.search(arguments):
                return True
        elif _credential_risk(raw[match.start():match.start() + end], names):
            return True
    return False


def _comment_code(fragment, names, recover=True, authenticated=None, verified_calls=None):
    """Return (offset, source, AST, recovered) for statements or calls amid prose.

    Parse the full comment first to preserve assignments and scope. Refuse
    any unparsed remainder that could still contain a Slack credential.
    Discovery supplies authentication/call spans on the second pass; parsing
    a candidate alone is not proof that its literals are safe to exclude.
    """
    try:
        tree = ast.parse(fragment)
    except SyntaxError:
        pass
    else:
        return [(0, fragment, tree, False)]

    if not recover:
        if _credential_risk(fragment, names):
            raise SyntaxError("unresolved commented Slack credential")
        return []
    if not names:
        return []
    names = names | {"slack", "slack_sdk"}
    # AST binding analysis resolves the aliases found by this candidate scan.
    candidate = re.compile(
        r"(?<![\w.])(?:" + "|".join(re.escape(n) for n in sorted(names))
        + r")(?:\s*\.\s*[A-Za-z_]\w*)*\s*\(")
    # A Python string is documentation; an English apostrophe is not evidence
    # of a string. Mask only spans supported by a parseable statement prefix.
    searchable = list(fragment)
    quoted = []
    strings = list(_comment_strings(fragment, names, quoted))
    for lo, hi in strings:
        searchable[lo:hi] = ["\n" if c == "\n" else " " for c in fragment[lo:hi]]
    candidates = list(candidate.finditer("".join(searchable)))
    recovered = []
    recovered_calls = []
    verified_strings = []
    remainder = list(fragment)
    previous_end = 0
    for match in candidates:
        if match.start() < previous_end:
            continue  # a nested call is inspected through its outer AST
        call_end = _comment_call_end(fragment[match.start():])
        if call_end is None:
            continue
        end = match.start() + call_end
        # An unproved quoted example must reach the refusal backstop, never
        # be rewritten as a real constructor. Partial spans from apostrophes
        # in prose do not contain the whole call and cannot hide it.
        if any(lo <= match.start() and end <= hi for lo, hi in quoted):
            continue
        # Keep assignments before the call when they form valid Python.
        for start in (previous_end, match.start()):
            snippet = fragment[start:end]
            stripped = snippet.lstrip(" \t\r\n;")
            start += len(snippet) - len(stripped)
            try:
                tree = ast.parse(stripped)
            except SyntaxError:
                continue
            recovered.append((start, stripped, tree, True))
            if verified_calls is None:
                recovered_calls.append((match.start(), end))
            if authenticated is not None:
                proven = authenticated.get(stripped.rstrip(), set())
                rows = stripped.splitlines(keepends=True)
                offsets = [0]
                for row in rows:
                    offsets.append(offsets[-1] + len(row))
                def offset(row, column):
                    return start + offsets[row - 1] + len(
                        rows[row - 1].encode('utf-8')[:column].decode('utf-8'))
                for row, col, end_row, end_col in verified_calls.get(stripped.rstrip(), set()):
                    recovered_calls.append((offset(row, col), offset(end_row, end_col)))
                # Only the token field of a proved authentication mapping is
                # structural; unrelated dictionary strings remain unverified.
                keys = {id(key) for node in ast.walk(tree) if isinstance(node, ast.Dict)
                        for key, value in zip(node.keys, node.values)
                        if _literal(key) == "token" and (
                            _literal(value) in ("", _PLACEHOLDER) or (
                                value.lineno, value.col_offset,
                                value.end_lineno, value.end_col_offset) in proven)}
                for node in ast.walk(tree):
                    if not isinstance(node, (ast.Constant, ast.BinOp)):
                        continue
                    span = (node.lineno, node.col_offset, node.end_lineno, node.end_col_offset)
                    if span in proven or id(node) in keys or _literal(node) in ("", _PLACEHOLDER):
                        verified_strings.append((offset(span[0], span[1]), offset(span[2], span[3])))
            remainder[start:end] = ["\n" if c == "\n" else " " for c in fragment[start:end]]
            previous_end = end
            break
    # Recovered assignments can contain quoted examples too. Keep them in
    # the refusal check unless discovery proved their authentication spans.
    for lo, hi in strings:
        if not any(start <= lo and hi <= end for start, end in recovered_calls + verified_strings):
            remainder[lo:hi] = fragment[lo:hi]
    residual = "".join(remainder)
    # Validate after discovery: a parsed prefix assignment is not automatically
    # safe. Preserve legacy no-literal calls, including quoted explanatory prose.
    unhandled_literal = (authenticated is not None and recovered
                         and _credential_risk(fragment.replace(_PLACEHOLDER, ""), names)
                         and _COMMENT_LITERAL_RE.search(residual))
    # A legacy <redacted> marker must not exempt a second malformed call.
    # Search the entire remainder, including qualified names with unknown
    # roots and quoted examples. Strings are excluded from recovery, not safety checks.
    if unhandled_literal or _residual_credential_risk(residual, names):
        raise SyntaxError("unresolved commented Slack credential")
    return recovered


def _slack_ctor_token_args(node, bindings):
    """Expressions passed as a Slack constructor's token, or None if not one.

    Shared by discovery and rewriting so the two cannot drift about what
    counts as an authentication site. `**mapping` is returned unresolved --
    discovery follows it through variables, the rewriter only handles a
    literal dict, which is all it can see in a single cell.
    """
    if not isinstance(node, ast.Call):
        return None
    roles = _constructor_roles(node.func, bindings)
    # WebhookClient's first argument is a URL, not a token. URL migration is
    # separate from this token-only change.
    if not roles & {"token", "webhook"}:
        if isinstance(node.func, ast.Name) and node.func.id in bindings.ctor_functions:
            result = _constructor_roles(node, bindings)
            if "instance" in result and len(result) != 1:
                raise SlackRedactionError(
                    "UNRESOLVED Slack constructor identity: factory has mixed client results")
            tracked = _tracked_constructor_function(node.func, bindings)
            if (node.args or node.keywords) and (
                    "instance" in result or "unknown" in result and tracked):
                # The inner constructor's parameter is not the caller's
                # credential expression. Until that provenance is established,
                # do not publish caller arguments as though they were safe.
                raise SlackRedactionError(
                    "UNRESOLVED Slack constructor identity: factory argument credential "
                    "provenance requires a direct constructor or manual review")
        return None
    if len(roles) != 1:
        raise SlackRedactionError(
            "UNRESOLVED Slack constructor identity: cannot safely classify authentication arguments")
    if "webhook" in roles:
        return [], []
    args = list(node.args[:1])
    unresolved = []
    for kw in node.keywords:
        if kw.arg == "token":
            args.append(kw.value)
        elif kw.arg is None:
            unresolved.append(kw.value)
    return args, unresolved


def _literal(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _literal(node.left), _literal(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _source(cell):
    src = cell.get("source", "")
    return "".join(src) if isinstance(src, list) else src


def _has_slack_candidate(cells):
    """Cheap pre-check: could this notebook contain a Slack credential at all?

    Deliberately broader than the AST analysis below -- a hit only costs one
    parse, a miss must be impossible. A miss means the caller skips redaction
    entirely and the artifact is byte-identical to the unredacted pipeline.
    """
    for cell in cells:
        src = _source(cell)
        if not src:
            continue
        if _TOKEN.search(src) or _SLACK_WORD_RE.search(src):
            return True
        if cell.get("cell_type") == "code" and _CTOR_WORD_RE.search(src):
            return True
    return False


class _NullRedactor:
    """Inert stand-in used when no Slack credential exists.

    Every method is identity, so a non-Slack notebook takes exactly the
    pre-redaction path: same bytes, same cache content, no new raise sites.
    """

    # Mirrors the full SlackArtifactRedactor read surface: a caller must never
    # have to branch on which one it holds.
    active = False
    required_env = ()
    configured_env = ()
    unverified = ()
    manual_action = ()
    tokens = {}

    def text(self, text):
        return text

    def preview_text(self, source):
        return source

    def source(self, source):
        return source

    def data(self, value):
        return value

    def notebook(self, notebook):
        return notebook


NULL_REDACTOR = _NullRedactor()


def build_redactor(*cell_groups, artifact_path=None):
    """Redactor for these cells, or `NULL_REDACTOR` when there is nothing to do.

    Pass every view of the notebook (original cells AND migrated cells): a
    model rewrite can move a credential from code into a comment, an output or
    the migration report, and discovery must see both.
    """
    groups = [list(group) for group in cell_groups]
    if not any(_has_slack_candidate(group) for group in groups):
        return NULL_REDACTOR
    redactor = SlackArtifactRedactor(groups[0] if groups else [],
                                     artifact_path=artifact_path)
    for extra in groups[1:]:
        redactor.learn(extra)
    if redactor.active or redactor.unverified:
        return redactor
    return NULL_REDACTOR


class SlackArtifactRedactor:
    """Notebook-local discovery and artifact rewriting (no shared secret state).

    Learn from original AND final cells, so a model rewrite cannot leave the
    original token in a comment, output, report or notebook metadata. Imports
    and lexical scopes come from the same analysis used by the Slack guard.
    Literal token variables are followed across cells and within functions.
    Runtime secret/environment calls and unknown external variables stay intact.

    Two tiers of discovered value:
      * `tokens`   -- constructor-confirmed credentials. These authenticate, so
                      they earn a required environment variable and an
                      AST-precise rewrite of the constructor argument.
      * `scrub`    -- values safe to replace everywhere they appear. Includes
                      the distinctive `tokens` plus token-shaped strings found
                      in free text. A free-text match authenticates nothing we
                      can see, so it is removed but never manufactures a
                      required environment variable the saved notebook does
                      not read.
    """

    def __init__(self, cells, artifact_path=None):
        # Names must not collide when several saved notebooks run in one
        # kernel with different Slack credentials. Hash only the PUBLIC path,
        # never the secret; variable names reveal no credential fingerprint.
        suffix = ("_" + hashlib.sha256(artifact_path.encode()).hexdigest()[:12].upper()
                  if artifact_path else "")
        self.env_prefix = "SLACK_TOKEN" + suffix
        self.tokens = {}  # value -> required environment variable (never logged)
        self.scrub = set()  # values safe to replace in free text
        self.spellings = {}  # source spelling -> the value it encodes
        # Names (variables, parameters) that discovery traced to a credential.
        # A literal is only rewritten into a required lookup where it is
        # actually used to authenticate; see `source`.
        self.credential_names = set()
        self.bindings = new_bindings()  # import aliases, for ctor detection
        self._source_bindings = {}
        self._comment_bindings = new_bindings()
        self._comment_spans = {}
        self._comment_calls = {}
        self._comment_sources = set()
        self.unverified = []  # residual risk to REPORT, never silently drop
        self.manual_action = []  # credential removed from code we cannot rewrite
        self.emitted_env = set()  # env names actually written INTO saved code
        self.learn(cells)

    @property
    def active(self):
        """Did discovery actually find anything to remove?

        False means this instance would be a no-op, and the caller should keep
        the pre-redaction path rather than run a deepcopy + rewrite that can
        only reproduce its input. Deliberately excludes `unverified`, which is
        a report-only signal and must not pull an unmodified notebook onto the
        redacting path.
        """
        return bool(self.tokens or self.scrub or self.spellings)

    @property
    def proven(self):
        """Values whose SHAPE proves they are credentials wherever they appear.

        A constructor literal only proves the value authenticates AT THAT
        SITE. A value matching Slack's documented token shape proves the
        string ITSELF is a secret, so it can be removed from anywhere without
        asking what the code meant by it.
        """
        return {v for v in set(self.tokens) | self.scrub
                if _looks_like_slack_token(v)}

    @property
    def required_env(self):
        """Every credential discovered, whether or not the artifact uses it."""
        return tuple(self.tokens.values())

    @property
    def configured_env(self):
        """Names the SAVED artifact actually reads -- what to tell an operator.

        `required_env` counts every discovered credential, including ones the
        migration model had already rewritten to its own lookup and ones that
        only ever appeared in a commented-out constructor. Instructing someone
        to configure those sends them after a variable nothing reads.
        """
        return tuple(sorted(self.emitted_env))

    def _env_for(self, value):
        """Allocate (once) the required-environment name for a credential."""
        if value not in self.tokens:
            self.tokens[value] = (self.env_prefix if not self.tokens
                                  else f"{self.env_prefix}_{len(self.tokens) + 1}")
        return self.tokens[value]

    def _remember(self, value):
        """A constructor-confirmed credential."""
        if not value or value == "<redacted>":
            return
        self._env_for(value)
        if _is_scrubbable(value):
            self.scrub.add(value)

    def _remember_free_text(self, value):
        """A token-shaped string with no constructor behind it."""
        if _looks_like_slack_token(value):
            self.scrub.add(value)

    def learn(self, cells):
        cells = list(cells)
        # Disabled code can refer to an alias imported later in the notebook.
        # Discover that vocabulary before inspecting comments so a malformed
        # aliased call refuses during discovery, where reports are retained.
        for cell in cells:
            if cell.get("cell_type") != "code":
                continue
            try:
                tree = ast.parse(_strip_magics(_source(cell)))
            except SyntaxError:
                continue
            _, bindings, _, _ = _evolve(tree, new_bindings())
            self._comment_bindings.ctors.update(
                (name, value) for name, value in bindings.ctors.items() if value)
            self._comment_bindings.ctor_values.update(
                (name, bindings.ctor_values[name]) for name, value in bindings.ctors.items()
                if value and name in bindings.ctor_values)
            self._comment_bindings.ctor_values.update(
                (name, bindings.ctor_values[name]) for name in bindings.modules
                if name in bindings.ctor_values)
            self._comment_bindings.modules.update(bindings.modules)
        state = new_bindings()
        parsed = []
        scopes = {None: {}}
        parents = {}
        recovered_comments = {}
        recovered_sources = {}
        comment_trees = set()
        authentication_literals = set()
        verified_calls = set()
        strict_comment = None
        # Index lexical assignments first: functions may reference a token
        # assigned in a later cell. Parameters shadow outer names.
        def index(node, scope, conditional=False):
            node_order[id(node)] = len(node_order)
            conditional = conditional or isinstance(node, (ast.If, ast.Try, ast.For, ast.While))
            conditional_nodes[id(node)] = conditional
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                parents[node] = scope
                scope = node
                scopes[scope] = {}
                if hasattr(node, "args"):
                    for arg in ast.walk(node.args):
                        if isinstance(arg, ast.arg):
                            scopes[scope][arg.arg] = []
                    positional = node.args.posonlyargs + node.args.args
                    for arg, default in zip(positional[-len(node.args.defaults):], node.args.defaults):
                        scopes[scope][arg.arg].append(default)
                    for arg, default in zip(node.args.kwonlyargs, node.args.kw_defaults):
                        if default is not None:
                            scopes[scope][arg.arg].append(default)
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if isinstance(target, ast.Name) and node.value is not None:
                        scopes[scope].setdefault(target.id, []).append(node.value)
            node_scopes[id(node)] = scope
            for child in ast.iter_child_nodes(node):
                index(child, scope, conditional)

        node_scopes = {}
        node_order = {}
        conditional_nodes = {}
        for cell_index, cell in enumerate(cells):
            src = _source(cell)
            for match in _TOKEN.finditer(src):
                self._remember_free_text(match.group())
            if cell.get("cell_type") != "code":
                continue
            try:
                tree = ast.parse(_strip_magics(src))
            except SyntaxError:
                # Do not echo SyntaxError: it includes the offending source.
                names = SLACK_CTORS | set(state.ctors) | state.modules
                if _credential_risk(src, names):
                    raise SlackRedactionError(
                        f"cell {cell_index}: cannot parse Slack code for token redaction") from None
                if _names_re(names).search(src):
                    # Mentions a client but shows no literal on the same logical
                    # line. Usually prose or a non-Python cell, so failing the
                    # task here was the over-eager behaviour. But static
                    # analysis cannot see into a cell it cannot parse: a
                    # credential in a format this pass does not recognise, bound
                    # on a separate line, would pass through unredacted. Report
                    # the residual risk rather than drop it silently.
                    self.unverified.append(
                        f"cell {cell_index}: unparseable code references a Slack client; "
                        "any literal credential inside it could not be verified or removed")
                continue
            previous = self._source_bindings.get(src)
            self._source_bindings[src] = (_union_states([previous, state])
                                          if previous is not None else state._child())
            snapshots, state, _, _ = _evolve(tree, state)
            index(tree, None)
            parsed.append((src, tree, snapshots, state))
            # Old exports can already contain commented constructors. Inspect
            # comment text separately; it must never change live bindings.
            self._comment_bindings.ctors.update(state.ctors)
            self._comment_bindings.ctor_values.update(
                (name, state.ctor_values[name]) for name in state.ctors
                if name in state.ctor_values)
            self._comment_bindings.ctor_values.update(
                (name, state.ctor_values[name]) for name in state.modules
                if name in state.ctor_values)
            self._comment_bindings.modules.update(state.modules)
            comment_bindings = self._comment_bindings._child()
            for fragment, _, recover in _comment_fragments(src):
                if fragment.strip():
                    self._comment_sources.add(src.rstrip())
                try:
                    comment_parts = _comment_code(
                        fragment, SLACK_CTORS | set(comment_bindings.ctors) | comment_bindings.modules, recover)
                except SyntaxError:
                    raise SlackRedactionError(
                        f"cell {cell_index}: cannot parse commented Slack token") from None
                for _, comment_src, comment_tree, recovered in comment_parts:
                    comment_snapshots, comment_state, _, _ = _evolve(comment_tree, comment_bindings)
                    if recovered:
                        recovered_comments[comment_tree] = fragment
                        recovered_sources[src] = cell_index
                    comment_trees.add(comment_tree)
                    # A commented assignment cannot overwrite the live variable
                    # environment consulted by later cells.
                    parents[comment_tree] = None
                    scopes[comment_tree] = {}
                    index(comment_tree, comment_tree)
                    parsed.append((comment_src, comment_tree, comment_snapshots, comment_state))

        def dicts(expr, scope, seen):
            """Candidate dict literals an expression can evaluate to."""
            if isinstance(expr, ast.Dict):
                return [expr]
            if not isinstance(expr, ast.Name):
                return []
            key = (scope, expr.id, "dict")
            if key in seen:
                return []
            while expr.id not in scopes[scope] and scope is not None:
                scope = parents[scope]
            found = []
            for rhs in scopes[scope].get(expr.id, []):
                found.extend(dicts(rhs, scope, seen | {key}))
            return found

        def fragments(expr, scope, seen):
            """String pieces that would be CONCATENATED into a token value.

            Only the operands of the composition count. `os.environ['X']`
            contributes nothing even though its subscript holds a constant --
            a pure runtime composition hides no credential in the source.
            """
            if isinstance(expr, ast.JoinedStr):
                found = []
                for part in expr.values:
                    if isinstance(part, ast.Constant) and isinstance(part.value, str):
                        found.append(part.value)
                    elif isinstance(part, ast.FormattedValue):
                        found.extend(fragments(part.value, scope, seen))
                return found
            if isinstance(expr, ast.BinOp):
                return (fragments(expr.left, scope, seen)
                        + fragments(expr.right, scope, seen))
            if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
                return [expr.value]
            if isinstance(expr, ast.Name):
                return values(expr, scope, seen)
            return []

        def unresolved(expr, scope, seen):
            if strict_comment is None:
                return []  # Preserve existing handling outside recovered prose comments.
            if isinstance(expr, ast.Subscript) and ast.unparse(expr.value) == "os.environ":
                return []
            if isinstance(expr, ast.Call):
                name = ast.unparse(expr.func)
                if (name in {"os.getenv", "os.environ.get"}
                        and len(expr.args) == 1 and not expr.keywords):
                    return []
                if (name in {"os.getenv", "os.environ.get"} and len(expr.args) == 2
                        and not expr.keywords
                        and (_literal(expr.args[1]) in self.proven
                             or _literal(expr.args[1]) == _PLACEHOLDER)):
                    return []  # the existing shape-proven scrub covers this fallback
                if name in {"dbutils.secrets.get", "aidputils.secrets.get"}:
                    return []
            assignments = (re.finditer(
                rf"\b{re.escape(expr.id)}\s*(?::[^=;\n]+)?=(?!=)([^;\n]*)", strict_comment or "")
                if isinstance(expr, ast.Name) else ())
            if any(_STRING_LITERAL_RE.search(match.group(1)) for match in assignments) or any(
                    isinstance(node, ast.Constant) and isinstance(node.value, (str, bytes))
                    and node.value not in ("", b"", _PLACEHOLDER)
                    for node in ast.walk(expr)):
                raise SlackRedactionError(
                    "UNRESOLVED computed Slack token: cannot verify authentication expression")
            if not isinstance(expr, ast.Name) and any(
                    values(node, scope, seen) for node in ast.walk(expr)
                    if isinstance(node, ast.Name)):
                raise SlackRedactionError(
                    "UNRESOLVED computed Slack token: cannot verify authentication expression")
            return []

        def values(expr, scope, seen):
            literal = _literal(expr)
            if literal is not None:
                authentication_literals.add(id(expr))
                return [literal]
            if isinstance(expr, ast.Name):
                expression_scope = scope
                while expr.id not in scopes[scope] and scope is not None:
                    scope = parents[scope]
                key = (scope, expr.id)
                if key in seen:
                    return unresolved(expr, scope, seen)
                candidates = scopes[scope].get(expr.id, [])
                if scope is expression_scope or isinstance(expression_scope, ast.Module):
                    # In a straight-line scope a reference sees only earlier
                    # assignments. Follow aliases at THEIR assignment time too.
                    candidates = [rhs for rhs in candidates
                                  if node_order[id(rhs)] < node_order[id(expr)]]
                    possible = []
                    for rhs in reversed(candidates):
                        possible.append(rhs)
                        if not conditional_nodes[id(rhs)]:
                            break
                    candidates = possible
                if not candidates:
                    return unresolved(expr, scope, seen)
                # Free variables in a function resolve at call time, which
                # need not be visible here; retain possible outer definitions.
                return [v for rhs in candidates
                        for v in values(rhs, scope, seen | {key})]
            if isinstance(expr, ast.IfExp):
                return values(expr.body, scope, seen) + values(expr.orelse, scope, seen)
            if isinstance(expr, ast.Subscript):
                # `WebClient(token=cfg['token'])` where cfg is a dict literal.
                index_key = expr.slice
                if not (isinstance(index_key, ast.Constant)
                        and isinstance(index_key.value, str)):
                    return unresolved(expr, scope, seen)
                found = []
                for mapping in dicts(expr.value, scope, seen):
                    for k, v in zip(mapping.keys, mapping.values):
                        if isinstance(k, ast.Constant) and k.value == index_key.value:
                            found.extend(values(v, scope, seen))
                return found or unresolved(expr, scope, seen)
            if isinstance(expr, (ast.JoinedStr, ast.BinOp)):
                # Refuse only when part of the credential is genuinely in the
                # source. `f"{os.environ['SLACK_BOT_TOKEN']}"` and
                # `prefix + os.environ['X']` hold no literal: they are runtime
                # lookups and must not fail a task that has no hardcoded
                # credential at all.
                if any(fragments(expr, scope, seen)):
                    raise SlackRedactionError(
                        "UNRESOLVED computed Slack token: move it to a runtime secret lookup")
                return []
            return unresolved(expr, scope, seen)

        def note_names(expr, scope, seen):
            """Record every name a credential reaches the constructor through.

            `tok = 'secret'; alias = tok; WebClient(token=alias)` makes both
            `alias` and `tok` authentication-bearing, so the assignment that
            holds the literal is rewritten even though it is cells away from
            any constructor.
            """
            if isinstance(expr, ast.IfExp):
                note_names(expr.body, scope, seen)
                note_names(expr.orelse, scope, seen)
                return
            if isinstance(expr, ast.Subscript):
                note_names(expr.value, scope, seen)   # cfg['token']
                return
            if not isinstance(expr, ast.Name):
                return
            target = scope
            while expr.id not in scopes[target] and target is not None:
                target = parents[target]
            key = (target, expr.id)
            if key in seen:
                return
            # The name is credential-bearing if it resolves to a credential
            # directly, or holds a mapping whose 'token' entry does.
            mappings = dicts(expr, scope, set())
            carries = (any(values(entry, scope, set())
                           for mapping in mappings
                           for key_node, entry in zip(mapping.keys, mapping.values)
                           if isinstance(key_node, ast.Constant) and key_node.value == "token")
                       if mappings else bool(values(expr, scope, set())))
            if carries:
                self.credential_names.add(expr.id)
            for rhs in scopes[target].get(expr.id, []):
                note_names(rhs, target, seen | {key})

        for src, tree, snapshots, final_state in parsed:
            strict_comment = recovered_comments.get(tree)
            def visit(node, binding):
                binding = snapshots.get(id(node), binding)
                try:
                    found = _slack_ctor_token_args(node, binding)
                except SlackRedactionError as exc:
                    if strict_comment is not None and str(exc).startswith(
                            'UNRESOLVED Slack constructor identity'):
                        raise SlackRedactionError('cannot parse commented Slack token') from None
                    raise
                if found is not None:
                    args, unresolved = found
                    scope = node_scopes[id(node)]
                    unknown_mappings = []
                    for mapping_expr in unresolved:
                        # `WebClient(**cfg)` where cfg carries 'token'.
                        mappings = dicts(mapping_expr, scope, set())
                        if not mappings:
                            unknown_mappings.append(mapping_expr)
                        for mapping in mappings:
                            args.extend(
                                v for k, v in zip(mapping.keys, mapping.values)
                                if isinstance(k, ast.Constant) and k.value == "token")
                        note_names(mapping_expr, scope, set())
                    for arg in args + unknown_mappings:
                        for value in values(arg, scope, set()):
                            self._remember(value)
                        note_names(arg, scope, set())
                    verified_calls.add(id(node))
                for child in ast.iter_child_nodes(node):
                    visit(child, binding)
            visit(tree, final_state)
        self.bindings = state

        # Capture encoded spellings as well as decoded values; a preserved
        # comment may contain '\\x61...' instead of the decoded token. Only for
        # scrubbable values -- a placeholder's spelling is no safer to blind
        # replace than the placeholder itself.
        for src, tree, _, _ in parsed:
            if tree in comment_trees:
                spans = self._comment_spans.setdefault(src.rstrip(), set())
                calls = self._comment_calls.setdefault(src.rstrip(), set())
                for node in ast.walk(tree):
                    if id(node) in verified_calls:
                        calls.add((node.lineno, node.col_offset, node.end_lineno, node.end_col_offset))
                    if id(node) in authentication_literals and _literal(node) in self.tokens:
                        spans.add((node.lineno, node.col_offset, node.end_lineno, node.end_col_offset))
            for node in ast.walk(tree):
                literal = _literal(node)
                if literal in self.scrub:
                    segment = ast.get_source_segment(_strip_magics(src), node)
                    if segment:
                        self.spellings[segment] = literal

        # Validate before returning even when no token was learned: an unsafe
        # recovered candidate must not escape through the NULL_REDACTOR path.
        for src, cell_index in recovered_sources.items():
            try:
                self._comment_literals(src, validate=True)
            except SlackRedactionError as exc:
                raise SlackRedactionError(f"cell {cell_index}: {exc}") from None

    def _apply(self, text, values, quote_spellings=False):
        """Replace these credential values, and the spellings that encode them."""
        spellings = sorted((spelling for spelling, value in self.spellings.items()
                            if value in values), key=len, reverse=True)
        for spelling in spellings:
            text = text.replace(spelling, "'<redacted>'" if quote_spellings
                                else _PLACEHOLDER)
        for value in sorted(values, key=len, reverse=True):
            text = text.replace(value, _PLACEHOLDER)
        return _TOKEN.sub(
            lambda m: _PLACEHOLDER if _looks_like_slack_token(m.group()) else m.group(),
            text)

    def text(self, text):
        """Scrub NON-EXECUTABLE artifact text: reports, markdown, outputs, metadata.

        Nothing here can break, so every suspected credential goes.
        """
        # Reports embed original cell source. Rewrite those proven comment
        # sites without globally replacing short words in surrounding prose.
        for source in sorted(self._comment_sources, key=len, reverse=True):
            if source in text:
                text = text.replace(source, self._comment_literals(source))
        return self._apply(text, self.scrub)

    def code_text(self, text):
        """Scrub a fragment that must stay valid CODE.

        A spelling is a complete source literal (`'fixture-secret'`), so the
        plain `text()` replacement would strip its quotes and break the
        expression. Keep the quoting and redact what is inside it.
        """
        return self._apply(text, self.scrub, quote_spellings=True)

    def preview_text(self, source):
        """Scrub actual preview source without requiring a learned cell-text match."""
        return self._apply(self._comment_literals(source), self.scrub)

    def proven_code_text(self, text):
        """Scrub only SHAPE-PROVEN credentials from a code fragment.

        A constructor literal proves the value authenticates at that site, not
        that every other occurrence of the same string is a secret -- rewriting
        those corrupts unrelated code. A shape-matching value needs no
        such care.
        """
        return self._apply(text, self.proven, quote_spellings=True)

    def _authentication_nodes(self, tree, bindings=None):
        """Node ids of literals actually used to authenticate in this cell.

        A credential is never a subscript index, a dict key or a comparison
        operand; rewriting those into `os.environ[...]` produced nonsense like
        `os.environ[__import__('os').environ['SLACK_TOKEN_…']]`. Only these
        positions are authentication: a Slack constructor's token argument,
        an assignment to a name discovery traced to one, and the default of a
        parameter it traced to one.
        """
        marked = set()

        def mark(expr):
            # Mark the node itself as well as its parts: `_literal` folds a
            # constant concatenation, so `WebClient(token='a' + 'b')` is
            # rewritten at the BinOp, never at either operand.
            marked.add(id(expr))
            if isinstance(expr, ast.BinOp):            # constant concatenation
                mark(expr.left)
                mark(expr.right)
            elif isinstance(expr, ast.IfExp):
                mark(expr.body)
                mark(expr.orelse)
            elif isinstance(expr, ast.Dict):           # {'token': <literal>}
                for key, value in zip(expr.keys, expr.values):
                    if isinstance(key, ast.Constant) and key.value == "token":
                        mark(value)

        initial = self.bindings if bindings is None else bindings
        snapshots, final, _, _ = _evolve(tree, initial)

        def visit(node, state):
            state = snapshots.get(id(node), state)
            found = _slack_ctor_token_args(node, state)
            if found is not None:
                for expr in found[0] + found[1]:
                    mark(expr)
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = (node.targets if isinstance(node, ast.Assign)
                           else [node.target])
                if node.value is not None and any(
                        isinstance(t, ast.Name) and t.id in self.credential_names
                        for t in targets):
                    mark(node.value)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                positional = node.args.posonlyargs + node.args.args
                for arg, default in zip(positional[len(positional)
                                                   - len(node.args.defaults):],
                                        node.args.defaults):
                    if arg.arg in self.credential_names:
                        mark(default)
                for arg, default in zip(node.args.kwonlyargs, node.args.kw_defaults):
                    if default is not None and arg.arg in self.credential_names:
                        mark(default)
            for child in ast.iter_child_nodes(node):
                visit(child, state)
        visit(tree, final)
        return marked

    def source(self, source, bindings=None):
        """Rewrite complete literal expressions; keep formatting and magics."""
        if not self.tokens and not self.scrub and not self.spellings:
            return source
        if bindings is None:
            bindings = self._source_bindings.get(source, self.bindings)
        source = self._comment_literals(source)
        lines = source.splitlines(keepends=True)
        starts = [0]
        for line in lines:
            starts.append(starts[-1] + len(line))
        def offset(pos):
            return starts[pos[0] - 1] + pos[1]
        edits = []
        # Truthy once any credential literal was replaced with the inert
        # placeholder; drives the cell-level removal note below. Replaces the old
        # `used`/`promoted` env-name sets, which only existed to build the
        # now-removed `os.environ[...]` "configure X" annotation.
        redacted = []
        proven, left_in_place = self.proven, set()
        stripped = _strip_magics(source)
        stripped_lines = stripped.splitlines(keepends=True)

        def node_offset(row, byte_col):
            line = stripped_lines[row - 1]
            col = len(line.encode("utf-8")[:byte_col].decode("utf-8"))
            # An expression magic (`%time expr`) is blanked by moving the
            # Python tail left to the original indent and padding the end.
            # Measure that distance from the magic token itself: a substring
            # search finds nothing for an INDENTED magic, because the indent
            # is present on both sides, and the whole cell was then refused.
            shift = 0
            if line != lines[row - 1]:
                magic = _MAGIC_WITH_CODE_RE.match(lines[row - 1])
                if magic:
                    shift = len(magic.group(2)) + len(magic.group(3))
            return offset((row, col + shift))

        try:
            tree = ast.parse(stripped)
        except SyntaxError:
            tree = None
        authentication = self._authentication_nodes(tree, bindings) if tree is not None else set()

        def span(node):
            return (node_offset(node.lineno, node.col_offset),
                    node_offset(node.end_lineno, node.end_col_offset))

        def visit(node):
            # An `os.environ.get(KEY, <token>)` fallback token is handled by the
            # generic literal path below: the <token> Constant is scrubbed to the
            # inert placeholder, leaving `os.environ.get(KEY, '<redacted>')`, which
            # is evaluation-safe whether or not KEY is set. We deliberately do NOT
            # promote it to a bare `os.environ[KEY]`: that made the saved notebook
            # require KEY and raise KeyError when it was unset -- the same eager-
            # evaluation failure class as the def-time KeyError this pass no longer
            # emits (AutoRefit derived_raw, 2026-09-22).
            if isinstance(node, ast.JoinedStr):
                # Before Python 3.12 every node inside an f-string reports the
                # span of the WHOLE f-string, so a node-level rewrite replaced
                # `f"... {name}"` with a plain string and silently dropped the
                # interpolation. Treat the f-string as one text unit: scrubbing
                # the token substring keeps both the prefix and the braces.
                segment = ast.get_source_segment(stripped, node)
                if segment and self.proven_code_text(segment) != segment:
                    lo, hi = span(node)
                    edits.append((lo, hi, self.proven_code_text(segment)))
                return
            value = _literal(node)
            replacement = None
            if value is not None and (value in self.tokens or value in self.scrub):
                if value in proven or id(node) in authentication:
                    # A proven Slack credential position: either the string is
                    # token-shaped (a credential wherever it appears) or the
                    # discovery pass traced this exact position to a Slack
                    # constructor.
                    #
                    # Replace it with an INERT placeholder string, never a runtime
                    # `os.environ[...]` lookup. The Slack CALL that would consume
                    # the credential is already commented out by the execution-
                    # boundary neutraliser, so the value is dead code here: an
                    # env lookup added nothing runnable and, in an EAGER position
                    # -- a default parameter value (`def post(token=<here>)`), a
                    # module-level or class-body assignment -- it was evaluated at
                    # import/def time and raised KeyError, bricking notebooks that
                    # never call Slack (AutoRefit derived_raw, KeyError
                    # SLACK_TOKEN_EXAMPLE01, 2026-09-22). A string literal is
                    # evaluation-safe in every position.
                    replacement = repr(_PLACEHOLDER)
                    redacted.append(True)
                else:
                    # Same string, but here it is a dict key, a lookup name or
                    # a comparison operand -- not an authentication. A
                    # constructor literal only proves the value authenticates
                    # WHERE IT WAS FOUND; rewriting it here would change code
                    # that has nothing to do with Slack.
                    left_in_place.add(value)
            elif value is not None and self._apply(value, proven) != value:
                # Text that embeds a PROVEN token is not itself an auth token.
                # Re-quote the scrubbed VALUE rather than patching the source
                # span: an implicit concatenation (`'a' 'b'`) is one Constant
                # node, so a textual edit there would leave a dangling operand.
                replacement = repr(self._apply(value, proven))
            if replacement is not None:
                lo, hi = span(node)
                edits.append((lo, hi, replacement))
                return  # never rewrite half of an implicit concatenation
            for child in ast.iter_child_nodes(node):
                visit(child)
        if tree is not None:
            visit(tree)

        try:
            for tok in tokenize.generate_tokens(io.StringIO(source).readline):
                if tok.type == tokenize.COMMENT:
                    replacement = self._apply(tok.string, self.scrub)
                    if replacement != tok.string and not any(
                            lo <= offset(tok.start) < hi for lo, hi, _ in edits):
                        edits.append((offset(tok.start), offset(tok.end), replacement))
                elif tok.type == tokenize.STRING and tree is None and self.text(tok.string) != tok.string:
                    raise SlackRedactionError("cannot parse credential-bearing artifact cell")
        except (tokenize.TokenError, IndentationError):
            if self.text(source) != source:
                raise SlackRedactionError("cannot tokenize credential-bearing artifact cell") from None
            return source
        for lo, hi, replacement in sorted(edits, reverse=True):
            source = source[:lo] + replacement + source[hi:]

        # SAFETY NET. `visit()` can only reach a credential that is an AST
        # string constant. A magic line is blanked to `pass` by _strip_magics
        # before parsing, and a bytes literal is not a str node -- both are
        # invisible to it, and both live in cells that PARSE, so the
        # `tree is None` refusal never fires either. Without this, a value we
        # already know is a secret shipped in the delivered notebook with
        # nothing reported: the silent-miss class the write guard exists to
        # prevent. Removing the secret is not optional, so scrub what is left
        # and tell the operator that cell needs manual rework.
        if self.proven_code_text(source) != source:
            scrubbed = self.proven_code_text(source)
            if tree is not None:
                try:
                    ast.parse(_strip_magics(scrubbed), feature_version=(3, 11))
                except SyntaxError:
                    raise SlackRedactionError(
                        "cannot remove a credential from this cell without "
                        "changing its syntax") from None
            source = scrubbed
            self.manual_action.append(
                "a credential appeared in code this pass cannot rewrite "
                "(shell/SQL magic line, bytes literal or similar); it was "
                "removed and the cell needs manual rework before the saved "
                "notebook will run")
        if left_in_place:
            self.manual_action.append(
                f"{len(left_in_place)} suspected credential value(s) also occur "
                "here outside any authentication site (a lookup name, dict key "
                "or comparison); left unchanged so unrelated code keeps working "
                "-- confirm by hand whether they are really secrets")

        if tree is not None and edits:
            try:
                # Match the minimum supported Python grammar, not the host's
                # relaxed f-string grammar (Python 3.12+).
                ast.parse(_strip_magics(source), feature_version=(3, 11))
            except SyntaxError:
                raise SlackRedactionError("token redaction would change artifact syntax") from None
        if redacted:
            newline = "\r\n" if "\r\n" in source else "\n"
            # A short cell-level marker. Unlike the old env-lookup rewrite there is
            # nothing to "configure", and no need to preserve the original line as
            # a comment: the credential was replaced IN PLACE with '<redacted>', so
            # the touched line is still present and evaluation-safe. Keep a %% cell
            # magic on its required first line.
            note = _ANNOTATION_REMOVED + newline
            if source.startswith("%%"):
                head, separator, tail = source.partition("\n")
                source = head + separator + note + tail
            else:
                source = note + source
        return source

    def _comment_literals(self, source, validate=False):
        """Redact short authentication literals at their spans, preserving prose."""
        short_tokens = set(self.tokens) - self.scrub
        if not short_tokens and not validate:
            return source
        edits = []
        names = SLACK_CTORS | set(self._comment_bindings.ctors) | self._comment_bindings.modules
        for fragment, mapping, recover in _comment_fragments(source):
            try:
                parts = _comment_code(fragment, names, recover, self._comment_spans, self._comment_calls)
            except SyntaxError:
                raise SlackRedactionError("cannot parse commented Slack token") from None
            for base, snippet, tree, _ in parts:
                rows = snippet.splitlines(keepends=True)
                starts = [0]
                for row in rows:
                    starts.append(starts[-1] + len(row))
                def original_offset(row, byte_column):
                    column = len(rows[row - 1].encode("utf-8")[:byte_column].decode("utf-8"))
                    return mapping[base + starts[row - 1] + column]

                # Discovery-time spans retain the exact alias/scope evidence
                # even when another notebook view drops or rebinds an import.
                if snippet.rstrip() in self._comment_spans:
                    for row, col, end_row, end_col in self._comment_spans[snippet.rstrip()]:
                        edits.append((original_offset(row, col), original_offset(end_row, end_col),
                                      repr(_PLACEHOLDER)))
                    continue

                authentication = self._authentication_nodes(tree, self._comment_bindings)

                def visit(node):
                    if id(node) in authentication and _literal(node) in short_tokens:
                        edits.append((original_offset(node.lineno, node.col_offset),
                                      original_offset(node.end_lineno, node.end_col_offset),
                                      repr(_PLACEHOLDER)))
                        return
                    for child in ast.iter_child_nodes(node):
                        visit(child)
                visit(tree)
        for lo, hi, replacement in sorted(edits, reverse=True):
            source = source[:lo] + replacement + source[hi:]
        return source

    def notebook(self, notebook):
        """Return an independent redacted artifact; leave replay input untouched."""
        result = copy.deepcopy(notebook)
        bindings = new_bindings()
        for index, cell in enumerate(result.get("cells", [])):
            src = _source(cell)
            reported = len(self.manual_action)
            if cell.get("cell_type") == "code":
                clean = self.source(src, bindings)
                try:
                    _, bindings, _, _ = _evolve(ast.parse(_strip_magics(src)), bindings)
                except SyntaxError:
                    pass  # discovery already handles unparseable source
            else:
                clean = self.text(src)
            for position in range(reported, len(self.manual_action)):
                self.manual_action[position] = (
                    f"cell {index}: " + self.manual_action[position])
            was_list = isinstance(cell.get("source"), list)
            for key in list(cell):
                if key != "source":
                    cell[key] = self.data(cell[key])
            if clean != src:
                cell["source"] = clean.splitlines(keepends=True) if was_list else clean
        for key in result:
            if key != "cells":
                result[key] = self.data(result[key])
        return result

    def data(self, value):
        """Scrub strings before JSON encoding; never replace JSON syntax."""
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, list):
            return [self.data(v) for v in value]
        if isinstance(value, dict):
            return {k: self.data(v) for k, v in value.items()}
        return value
