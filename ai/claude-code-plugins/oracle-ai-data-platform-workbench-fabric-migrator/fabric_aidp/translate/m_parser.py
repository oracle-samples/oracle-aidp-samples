"""Run the optional Node M parser and return its JSON contract.

Power Query has no Python parser. `@microsoft/powerquery-parser` is Microsoft's
own, is TypeScript, and has no PyPI equivalent -- so translation needs Node.

Node is therefore *optional*: it is discovered at runtime, never declared as a
dependency. Without it this module raises `MParserUnavailable`, callers report
Dataflows as counted-but-not-translated, and nothing pretends to a result it
does not have.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

# Inside the package, not beside it. At `parents[2] / "mparse"` this resolved
# to `site-packages/mparse` once installed -- a directory pip has no reason to
# create, so a `pip install` could never translate a Dataflow.
MPARSE_DIR = Path(__file__).resolve().parents[1] / "mparse"
PARSE_JS = MPARSE_DIR / "parse.js"
TIMEOUT_SECONDS = 60
_STEP_KEYS = {"name", "fn", "args", "nav", "inputs", "raw"}


def unquote_identifier(name) -> str:
    '''Strip M's quoted-identifier syntax: #"Promoted headers" -> Promoted headers.

    24% of corpus step names are quoted (234 of 956), and they appear quoted
    in `inputs` and in DataDestinations `QueryName` too. Comparing a quoted name against an
    unquoted one silently fails to find a step that is right there.
    '''
    text = str(name or "")
    if text.startswith('#"') and text.endswith('"') and len(text) >= 3:
        return text[2:-1].replace('""', '"')
    return text


class MParserUnavailable(Exception):
    """Node, parse.js, or its node_modules is not installed."""


class MParseError(Exception):
    """Node ran, but the M did not parse or the contract was malformed."""


def _node_path(node=None):
    return node if node is not None else "node"


def parser_available(node=None) -> bool:
    if not PARSE_JS.is_file() or not (MPARSE_DIR / "node_modules").is_dir():
        return False
    return shutil.which(_node_path(node)) is not None


def _validate(payload) -> dict:
    if not isinstance(payload, dict) or "ok" not in payload:
        raise MParseError("parser returned something that is not the contract")
    if not payload["ok"]:
        raise MParseError(str(payload.get("error", "parse failed")))
    queries = payload.get("queries")
    if not isinstance(queries, list):
        raise MParseError("contract has no 'queries' list")
    # Required, not defaulted. The section attribute is where a Dataflow's
    # default destination is written down, so a parse.js too old to report it
    # would turn every `[BindToDefaultDestination = true]` query into a
    # DataFrame nobody stores -- which is the defect, arriving silently.
    # `None` is a legitimate value (most sections carry no attributes); the
    # key being absent is not.
    if "section_attrs" not in payload:
        raise MParseError(
            "contract has no 'section_attrs' key; this parse.js predates the "
            "default-destination contract -- reinstall the package")
    for query in queries:
        for step in query.get("steps") or []:
            missing = _STEP_KEYS - set(step)
            if missing:
                raise MParseError(f"step is missing contract keys: {sorted(missing)}")
    return payload


def parse_file(path, *, node=None) -> dict:
    path = Path(path)
    executable = shutil.which(_node_path(node))
    if executable is None:
        raise MParserUnavailable(
            f"{_node_path(node)!r} not found on PATH; install Node 18+ to translate Dataflows")
    if not PARSE_JS.is_file():
        raise MParserUnavailable(f"{PARSE_JS} is missing")
    if not (MPARSE_DIR / "node_modules").is_dir():
        raise MParserUnavailable(f"run `npm install` in {MPARSE_DIR} to translate Dataflows")
    try:
        # Node writes UTF-8. Without an explicit encoding Python decodes with
        # the locale code page (cp1252 on Windows) and mangles every
        # non-ASCII step, column, and table name.
        done = subprocess.run([executable, str(PARSE_JS), str(path)],
                              capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as exc:
        raise MParseError(f"parser timed out after {TIMEOUT_SECONDS}s on {path}") from exc
    if done.returncode != 0:
        raise MParseError(f"parser exited {done.returncode}: {done.stderr.strip()[:400]}")
    try:
        payload = json.loads(done.stdout)
    except json.JSONDecodeError as exc:
        raise MParseError(f"parser printed non-JSON: {done.stdout.strip()[:200]!r}") from exc
    return _validate(payload)


def parse_text(text, *, node=None) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp) / "mashup.pq"
        scratch.write_text(text, encoding="utf-8")
        return parse_file(scratch, node=node)
