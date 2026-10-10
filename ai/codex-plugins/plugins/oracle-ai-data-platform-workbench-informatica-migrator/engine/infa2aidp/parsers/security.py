"""Security validation for Informatica input files.

Defenses:
1. Input size limit (10 MB)
2. XXE prevention — reject DOCTYPE/ENTITY declarations before XML parsing
3. Path traversal validation
4. Expression size sanitization
5. Output path containment -- export-supplied object names become single
   path components and every write is checked to land under the output
   directory (SEC-AIDP-SAMPLES-INFA-H1)

Reference: the Rust reference implementation, src/security.rs
"""

import logging
import os
import re
import unicodedata

logger = logging.getLogger(__name__)

MAX_INPUT_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_EXPR_SIZE = 50_000  # 50 KB per expression

# Maximum XML element nesting depth accepted before parsing.
#
# ElementTree's C accelerator (like upstream roxmltree) recurses once per
# nesting level and can overflow the interpreter's stack on pathologically
# deep input such as "<a><a>...<a/>...</a></a>". MAX_INPUT_SIZE alone does
# NOT bound this: three-byte "<a>" tags permit well over a million nesting
# levels within the 10 MB cap. Real Informatica PowerCenter mappings nest
# only a handful of levels (POWERMART > REPOSITORY > FOLDER > MAPPING >
# TRANSFORMATION > field), so 256 sits orders of magnitude above any
# legitimate document. Matches the Rust reference implementation, src/security.rs::MAX_XML_DEPTH.
MAX_XML_DEPTH = 256


class SecurityError(Exception):
    """Raised when security validation fails."""
    pass


def validate_input_size(content: str, max_size: int = MAX_INPUT_SIZE):
    """Reject inputs larger than max_size bytes."""
    if len(content) > max_size:
        raise SecurityError(
            f"Input too large: {len(content):,} bytes exceeds "
            f"{max_size:,} byte limit"
        )


def validate_no_xxe(xml_content: str):
    """Reject XML with dangerous ENTITY declarations (XXE prevention).

    Allows Informatica's standard DOCTYPE (powrmart.dtd) but blocks
    ENTITY declarations that could exploit the parser.

    Informatica XML exports legitimately contain:
      <!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
    This is safe — it's a local DTD reference, not an external entity.
    """
    upper = xml_content.upper()
    # Block ENTITY declarations (the actual XXE attack vector)
    if "<!ENTITY" in upper:
        raise SecurityError(
            "ENTITY declarations are not allowed in Informatica XML input. "
            "This is a security measure to prevent XML External Entity (XXE) attacks."
        )
    # Block any DOCTYPE other than PowerCenter's own. The previous check
    # only asked whether the string "POWRMART" appeared ANYWHERE in the
    # file, so a foreign <!DOCTYPE ... SYSTEM "evil.dtd"> passed as long as
    # the document also mentioned powrmart.dtd somewhere.
    import re as _re

    for m in _re.finditer(r"<!DOCTYPE\b[^>]*>", xml_content, _re.IGNORECASE | _re.DOTALL):
        if not _re.fullmatch(
            r"<!DOCTYPE\s+POWERMART\s+SYSTEM\s+[\"']powrmart\.dtd[\"']\s*>",
            m.group(0), _re.IGNORECASE,
        ):
            raise SecurityError(
                "Non-Informatica DOCTYPE declarations are not allowed. "
                "Only <!DOCTYPE POWERMART SYSTEM 'powrmart.dtd'> is permitted."
            )


def validate_path(path: str):
    """Reject path traversal attempts."""
    if "../" in path or "/.." in path or "..\\" in path:
        raise SecurityError(f"Path traversal detected: {path}")
    if path.startswith("/"):
        raise SecurityError(f"Absolute path not allowed: {path}")
    if len(path) >= 3 and path[1] == ":":
        raise SecurityError(f"Windows absolute path not allowed: {path}")


# Characters an export-supplied name may keep when it becomes a file or
# directory name: Unicode word characters (letters and digits of any
# script, underscore), dot and hyphen. Everything else -- path separators,
# drive colons, NUL and other control characters, whitespace, quotes, shell
# metacharacters, format characters such as a right-to-left override -- is
# replaced. ``\w`` rather than ``A-Za-z0-9_``: PowerCenter Unicode-mode
# repositories and IDMC allow names such as ``m_顧客``, and an ASCII-only
# class collapsed every such name to ``m___`` -- distinct mappings became
# NAME CLASH suffixes and spurious RENAMED warnings.
_UNSAFE_PATH_CHARS = re.compile(r"[^\w.\-]")

# Names Windows refuses as a file name regardless of extension. A FOLDER
# called "CON" would otherwise make every write under it fail.
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10)),
}


def safe_path_component(name: str, fallback: str = "unnamed") -> str:
    """Reduce an export-supplied object name to ONE safe path component.

    PowerCenter ``<FOLDER NAME>`` / ``<MAPPING NAME>`` / ``<WORKFLOW NAME>``
    and IICS ``name`` / ``project`` / ``folder`` are free text under the
    export's control, and the migrator builds output paths from them. Used
    verbatim, an absolute folder name makes ``os.path.join`` discard the
    output directory entirely and a ``..`` segment climbs out of it -- so a
    tampered export could overwrite a ``CLAUDE.md``, a ``settings.json`` or
    another engagement's reviewed notebook wherever it pointed.

    The result never contains a path separator, a drive colon, NUL, or a
    ``..`` run; it never starts with a dot (no hidden files, no relative
    climb); it is never empty; and it is never a Windows device name. Legal
    Informatica names (word characters of any script, the usual
    ``m_``/``wf_`` forms) come back unchanged, so output layout for real
    exports -- ASCII or Unicode-mode -- is identical. The name is
    NFKC-normalised first, so a fullwidth solidus (``／``) or a fullwidth
    ``ＣＯＮ`` is judged as the ``/`` or ``CON`` it is displayed as.
    Callers that care about collisions should compare the result with the
    original and report a change -- see ``run_migration``.
    """
    cleaned = unicodedata.normalize("NFKC", name or "")
    cleaned = _UNSAFE_PATH_CHARS.sub("_", cleaned)
    # ".." (and longer dot runs) are the one remaining way to name a parent
    # directory; a single "." is harmless inside a name (m_v1.2).
    cleaned = re.sub(r"\.{2,}", "_", cleaned)
    cleaned = cleaned.lstrip(".").rstrip(". ")
    if not cleaned:
        return fallback
    if cleaned.split(".")[0].upper() in _WINDOWS_RESERVED:
        cleaned = f"_{cleaned}"
    return cleaned


def ensure_within(base_dir: str, path: str) -> str:
    """Return *path* if it resolves to *base_dir* or below; else raise.

    The last line of defence before an ``open()``: even a name that slipped
    past :func:`safe_path_component` (or a caller that forgot to apply it)
    cannot write outside the directory the operator passed as ``-o``.
    Compares the ``realpath`` of both (symlinks resolved, ``..`` collapsed,
    case folded on Windows) so neither a lexical trick nor a planted
    symlink inside the output directory can redirect the write.
    """
    base = os.path.normcase(os.path.realpath(base_dir))
    target = os.path.normcase(os.path.realpath(path))
    if target != base and not target.startswith(base.rstrip(os.sep) + os.sep):
        raise SecurityError(
            f"Refusing to write outside the output directory: {path!r} "
            f"resolves to {target!r}, which is not under {base_dir!r}"
        )
    return path


def validate_xml_nesting_depth(xml: str, max_depth: int = MAX_XML_DEPTH) -> None:
    """Reject XML whose element nesting depth exceeds ``max_depth``.

    Runs as a linear, quote-aware, **non-recursive** pre-parse scan -- before
    ``ET.parse``/``ET.fromstring`` ever sees the content. This must happen
    BEFORE parsing because the crash it guards against (a stack overflow in
    the parser's own recursive descent) happens *during* parsing; a depth
    check written as a recursive tree-walk after the fact would never get a
    chance to run against adversarial input, and a naive recursive scanner
    written for this guard would have the exact same problem. Counts nesting
    without building a tree: a start tag increases depth, an end tag
    decreases it, and self-closing tags / comments / CDATA / processing
    instructions / declarations do not change depth. Quoted attribute values
    are skipped so a ``>`` inside an attribute is never mistaken for a tag
    terminator.

    Ported from the Rust reference implementation, src/security.rs::validate_xml_nesting_depth
    (~:92-150) -- a single linear pass with no recursion of its own, so it
    adds negligible cost to valid input and short-circuits the instant the
    bound is crossed.
    """
    n = len(xml)
    i = 0
    depth = 0

    while i < n:
        if xml[i] != "<":
            i += 1
            continue

        # Comments and CDATA can contain '<'/'>' freely -- skip their whole
        # span rather than mis-tokenizing their contents as tags.
        if xml.startswith("<!--", i):
            j = xml.find("-->", i + 4)
            if j == -1:
                break  # unterminated -- let the real parser report the error
            i = j + 3
            continue
        if xml.startswith("<![CDATA[", i):
            j = xml.find("]]>", i + 9)
            if j == -1:
                break
            i = j + 3
            continue

        nxt = xml[i + 1] if i + 1 < n else ""
        # Processing instructions ("<?...?>") and declarations ("<!...>",
        # e.g. DOCTYPE -- already rejected by validate_no_xxe) do not open
        # an element; skip to the closing '>'.
        if nxt in ("?", "!"):
            gt = xml.find(">", i + 1)
            i = (gt + 1) if gt != -1 else n
            continue

        is_end_tag = nxt == "/"
        gt, self_closing = _scan_tag(xml, i + 1)
        if is_end_tag:
            depth = max(depth - 1, 0)
        elif not self_closing:
            depth += 1
            if depth > max_depth:
                raise SecurityError(
                    f"XML element nesting exceeds the maximum supported "
                    f"depth of {max_depth} levels"
                )
        i = (gt + 1) if gt is not None else n


def _scan_tag(s: str, start: int) -> tuple:
    """Scan a tag body from just after '<' to its closing '>'.

    Honors single/double-quoted attribute values so a '>' inside a value is
    never treated as the tag terminator. Returns ``(index_of_gt_or_None,
    is_self_closing)``.
    """
    n = len(s)
    i = start
    quote = ""
    last_non_ws = ""
    while i < n:
        c = s[i]
        if quote:
            if c == quote:
                quote = ""
        elif c in ("'", '"'):
            quote = c
        elif c == ">":
            return i, last_non_ws == "/"
        elif not c.isspace():
            last_non_ws = c
        i += 1
    return None, False


def sanitize_expression(expr: str) -> str:
    """Truncate oversized expressions to prevent context overflow."""
    if len(expr) > MAX_EXPR_SIZE:
        logger.warning("Expression truncated: %d chars > %d limit",
                       len(expr), MAX_EXPR_SIZE)
        return expr[:MAX_EXPR_SIZE] + "/* TRUNCATED */"
    return expr
