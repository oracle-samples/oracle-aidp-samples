"""Shared Python text handling: masking literals and comments for scanning.

Every rule that scans Python source with a regular expression can read
inside a `display(x)` that is a user's prose rather than a call, so the
source is tokenized once and each scan runs on a copy with the *body* of
each literal and each comment blanked. The delimiters stay visible,
because a rule like NB10 legitimately matches a quoted argument and needs
to see its quotes; the name it rewrites is then read back out of the real
source. Every pass here preserves length and newlines, so an offset found
in the masked copy is valid in the original.

This lived inside `fabric_notebook_to_spark` until the inventory catalog
needed it too -- `extract_written_tables` was counting a `saveAsTable`
that is commented out -- and the notebook translator imports the catalog,
so a second copy was the only alternative to moving it here.
"""
from __future__ import annotations

import io
import re
import tokenize
from dataclasses import dataclass, field

STRING_RE = re.compile(
    r"(?P<prefix>[rRbBuUfF]{0,2})(?P<q>'''|\"\"\"|'|\")(?P<val>.*?)(?P=q)", re.S)


# --- masking Python string literals and comments --------------------------
#
# Every rule below scans Python source with a regular expression, and a
# regular expression cannot tell a `display(x)` that is code from one that is
# a user's prose. Measured on this tree before the fix:
#
#   in:  note = "we call display(x) in the docs"
#   out: note = "we call x.show() in the docs"
#
# and the same for a table call, a OneLake path, a `notebookutils` import and
# a `%%python` line sitting inside a triple-quoted docstring -- the cell is
# processed line by line, so line 2 of a multi-line literal read as ordinary
# code. So the cell is tokenized once and every scan runs on a copy with the
# *body* of each literal blanked. The delimiters stay visible, because a rule
# like NB10 legitimately matches a quoted argument and needs to see its
# quotes; the name it rewrites is then read back out of the real source.
#
# Comments are masked for the same reason: they are the user's text, and none
# of these rules is about text. Measured on the bundled estate,
# `#display(dfDataChanged)` in edkreuk_FMD_FRAMEWORK_007 was being rewritten
# to `#dfDataChanged.show()` -- an edit to a line that does not run.
#
# Python 3.12 re-tokenized f-strings: the literal text between the `{...}`
# parts became FSTRING_MIDDLE and is no longer a STRING token. Measured on
# 3.14: `f"display({name}) and more"` yields FSTRING_MIDDLE 'display(' and
# FSTRING_MIDDLE ') and more', with `{name}` as ordinary OP/NAME tokens. So
# both token kinds are masked and the `{...}` parts are left visible, which is
# right -- a display() call in there is real code.
#
# None of the three FSTRING_* names exists before 3.12 and this project
# supports 3.9+ (`requires-python = ">=3.9"`), where the whole f-string is one
# STRING token. Masking that token whole would hide the `{...}` expressions,
# so on those versions `_fstring_parts` does the tokenizer's job. Measured,
# 3.9.6 against 3.14.6 before the fix:
#
#   x = f"{spark.table('dbo.claim')}"   3.14 rewritten, 3.9 not rewritten
#
# which is the defect this module exists to close, invisible on 3.14. The
# names are looked up with the same defensive `getattr` as
# `m_runtime._M_TIMESTAMP_TYPES`, for the same reason.
_FSTRING_MIDDLE = getattr(tokenize, "FSTRING_MIDDLE", None)
_FSTRING_START = getattr(tokenize, "FSTRING_START", None)
_FSTRING_END = getattr(tokenize, "FSTRING_END", None)
# NUL: it cannot occur in Python source, is not a word character, is not
# whitespace to `re`, and is not a quote or a bracket -- so no pattern here
# can match across a masked run or mistake one for an identifier.
MASK_CHAR = "\x00"


class Unmaskable(ValueError):
    """The cell does not tokenize, so its string literals cannot be located."""


@dataclass(frozen=True)
class MaskedPython:
    """A copy of some Python source with the literals and comments blanked.

    `text` is the same length as the source, newline for newline, so a span
    found in it indexes the real source unchanged.

    `literals` holds `(start, end, value_start, value_end)` for every real
    literal: the whole token including prefix and delimiters, and its value.
    A rule whose target is legitimately *inside* a literal -- a path, a table
    name -- reads the real source and uses these to check that the run it
    matched is a literal's value and not a slice of someone's prose.
    """

    text: str
    literals: tuple = ()
    values: frozenset = field(default=frozenset(), init=False, compare=False)

    def __post_init__(self):
        object.__setattr__(
            self, "values",
            frozenset((literal[2], literal[3]) for literal in self.literals))

    def is_literal_value(self, start: int, end: int) -> bool:
        """Whether source[start:end] is exactly one literal's value."""
        return (start, end) in self.values


def _fstring_parts(value: str, offset: int, fields: list) -> list:
    """Spans of the *literal* text in an f-string body, `{...}` excluded.

    Pre-3.12 only; from 3.12 the tokenizer reports the same split itself as
    FSTRING_MIDDLE. Appends `(absolute start, text)` for each replacement
    field to `fields`, so the caller can scan the expressions as code.

    `{{` and `}}` are literal braces. A field runs to its matching `}` with
    nested braces counted, so neither a dict display nor a nested format
    spec -- `f"{x!r:>{w}}"` -- ends it early.
    """
    spans, literal_start, depth, index, field_start = [], 0, 0, 0, 0
    while index < len(value):
        char = value[index]
        if depth == 0:
            if char in "{}" and value[index + 1:index + 2] == char:
                index += 2                       # `{{` / `}}`: a real brace
                continue
            if char == "{":
                spans.append((offset + literal_start, offset + index))
                depth, field_start, index = 1, index + 1, index + 1
                continue
            index += 1
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                fields.append((offset + field_start, value[field_start:index]))
                literal_start = index + 1
        index += 1
    if depth:
        # Pre-3.12 the tokenizer does not check an f-string's braces, so an
        # unbalanced one gets here. A scan that cannot see where the
        # expression ends must not rewrite inside it: mask the rest.
        spans.append((offset + field_start - 1, offset + len(value)))
    else:
        spans.append((offset + literal_start, offset + len(value)))
    return spans


def _scan_literals(source: str, base: int = 0, strict: bool = True):
    """`(blanks, literals)` for `source`, every offset shifted by `base`.

    `strict` raises `Unmaskable` when the tokenizer cannot get through.
    Recursion into an f-string's `{...}` passes `strict=False` instead: a
    field carries `!r` conversions and `:spec` format specs that are not
    valid Python on their own, so a field that will not tokenize must
    contribute nothing rather than fail the whole cell.
    """
    line_start = [0]
    for line in source.split("\n"):
        line_start.append(line_start[-1] + len(line) + 1)

    def offset(position) -> int:
        return base + line_start[position[0] - 1] + position[1]

    blanks, literals, open_fstrings = [], [], []
    try:
        # A source without a trailing newline makes tokenize raise, and a cell
        # body routinely has none.
        for token in tokenize.generate_tokens(
                io.StringIO(source + "\n").readline):
            start, end = offset(token.start), offset(token.end)
            if token.type == tokenize.STRING:
                match = STRING_RE.fullmatch(token.string)
                if match is None:          # unreachable for a real STRING
                    continue
                value = (start + match.start("val"), start + match.end("val"))
                literals.append((start, end) + value)
                if "f" not in match.group("prefix").lower():
                    blanks.append(value)
                    continue
                # Pre-3.12: one STRING token for the whole f-string. Split it
                # so the `{...}` expressions stay visible, then scan each one
                # as code -- a literal inside a field is a literal, and on
                # 3.12+ the tokenizer reports it as its own STRING token.
                fields: list = []
                blanks.extend(_fstring_parts(match.group("val"), value[0],
                                             fields))
                for field_start, text in fields:
                    if "\n" in text:
                        continue   # a field spanning lines: rare, not mapped
                    nested = _scan_literals(text, field_start, strict=False)
                    blanks.extend(nested[0])
                    literals.extend(nested[1])
            elif token.type == tokenize.COMMENT:
                # `#` stays visible so a rule that keys off it still can; only
                # the text after it is blanked.
                blanks.append((start + 1, end))
            elif token.type == tokenize.ERRORTOKEN and token.string in ("\"", "'"):
                # 3.9-3.11 do not raise on an unterminated single-quoted
                # string: they emit the quote as an ERRORTOKEN and carry on,
                # so the safety net never fired and the cell was scanned with
                # a literal boundary unknown. Measured on 3.9.6:
                # `x = "unterminated` / `display(y)` was rewritten blind.
                # Only a quote qualifies -- `!pip install x` also yields an
                # ERRORTOKEN on 3.9 and none on 3.12+, so refusing every
                # ERRORTOKEN would trade one version-dependent behaviour for
                # another.
                if strict:
                    raise Unmaskable("unterminated string literal")
            elif _FSTRING_MIDDLE is not None and token.type == _FSTRING_MIDDLE:
                blanks.append((start, end))
            elif _FSTRING_START is not None and token.type == _FSTRING_START:
                # f-strings nest from 3.12, so this is a stack.
                open_fstrings.append((start, end))
            elif _FSTRING_END is not None and token.type == _FSTRING_END:
                if open_fstrings:
                    opener, value_start = open_fstrings.pop()
                    literals.append((opener, end, value_start, start))
    except (tokenize.TokenError, SyntaxError, IndentationError) as exc:
        if strict:
            raise Unmaskable(str(exc)) from exc
        return [], []
    return blanks, literals


def masked_python(source: str) -> MaskedPython:
    """Mask the string literals and comments in one run of Python source.

    Raises `Unmaskable` when `tokenize` cannot get through the source. A
    partial token stream is discarded rather than used: the tokens after the
    failure point are exactly the ones a blind scan would get wrong.
    """
    blanks, literals = _scan_literals(source)
    chars = list(source)
    for start, end in blanks:
        for index in range(max(start, 0), min(end, len(chars))):
            if chars[index] != "\n":
                chars[index] = MASK_CHAR
    return MaskedPython("".join(chars), tuple(sorted(literals)))


# `\n`, `\r` and `\t` written as two source characters, and what they are
# once Python has decoded them -- padded back to two characters so the
# substitution preserves every offset.
#
# A backslash pair is matched whole and left alone, which is what keeps
# `"a\\nb"` -- one backslash then the letter n -- from being read as a line
# break. The rest of Python's escapes are not here: `\'` and `\"` already
# reach a SQL scan as the quote they are, and `\x41` and friends decode to
# text a SQL scan reads the same way on either side of the decoding.
_ESCAPE_RE = re.compile(r"\\(.)", re.S)
_ESCAPE_AS_WHITESPACE = {"n": "\n ", "r": "\n ", "t": "  "}


def sql_scan_text(source: str) -> str:
    r"""`source` with its whitespace escapes decoded, same length, same offsets.

    The SQL inside `spark.sql("...")` is read out of the notebook's *source
    text*, where a line break is the two characters `\` and `n`. Every SQL
    pass -- the comment scanner, the literal masker, the FROM pattern --
    reads those as two ordinary characters, and both of the things a line
    break does in SQL are lost:

        spark.sql("SELECT 1 -- note\nFROM claim")   the comment never ends
        spark.sql("SELECT *\nFROM claim")           `nFROM` is one word, so
                                                    `\bFROM` cannot match

    Measured on this tree with the demo catalog, `claim` being a real table
    that resolves from the same call written with a space: both came back
    unchanged with no finding at all. Suppression rather than corruption,
    and silent either way.

    Length is preserved so a span found in the result indexes the real
    source unchanged -- which is what lets the rewrite be applied to the
    source with its `\n` still spelt `\n`. Nothing is decoded on the rewrite
    path; this is a scanning view, exactly like `masked_python`.

    Not for an `r"..."` literal: there the two characters really are two
    characters, and the SQL is whatever Spark makes of them.
    """
    def replace(match):
        return _ESCAPE_AS_WHITESPACE.get(match.group(1), match.group(0))
    return _ESCAPE_RE.sub(replace, source)


def untokenizable_reason(exc) -> str:
    """Why the tokenizer stopped, in words that do not vary by interpreter.

    The tokenizer's own message and coordinates differ between Pythons --
    3.9.6 says "EOF in multi-line string" where 3.14.6 says "unterminated
    triple-quoted f-string literal", at different columns -- and a finding
    whose text changes with the interpreter is not one a reviewer can
    compare between runs.
    """
    text = str(exc).lower()
    if "string" in text:
        return "an unterminated string literal"
    if "statement" in text:
        return "an unclosed bracket"
    return "a token it could not read"


def cell_views(lines) -> list:
    """One `MaskedPython` per line of a cell, computed from the whole cell.

    Tokenizing a single line cannot work -- line 2 of a triple-quoted literal
    is not valid Python on its own -- so the cell is tokenized whole and the
    result sliced back to lines. A literal that spans lines therefore lands in
    no line's `values`, which is what the per-line rules want: they have never
    rewritten across a line break and must not start now.
    """
    view = masked_python("\n".join(lines))
    out, position = [], 0
    for line in lines:
        end = position + len(line)
        out.append(MaskedPython(
            view.text[position:end],
            tuple(tuple(offset - position for offset in literal)
                  for literal in view.literals
                  if position <= literal[0] and literal[1] <= end)))
        position = end + 1
    return out


def remasked(line: str, original: str, view):
    """`(line, mask)` with the mask valid for `line`. See the call site."""
    return line, view if line == original else view_of(line, None)


def view_of(source: str, view):
    """`view` when the caller built one, else a mask for `source` alone.

    The rules below are called per line by `translate`, which masks the whole
    cell; they are also called directly with one string, by tests and by
    anything outside this module, and must mask it themselves rather than scan
    raw source.

    Source the tokenizer cannot read at all is masked *entirely*: a rule that
    cannot see where the literals are must not rewrite anything, and the
    unmasked scan is the defect. `translate` raises the flag, once per cell,
    so the refusal is visible; this keeps the individual rules honest when
    they are called on their own.
    """
    if view is not None:
        return view
    try:
        return masked_python(source)
    except Unmaskable:
        return MaskedPython(MASK_CHAR * len(source))


__all__ = [
    "MASK_CHAR", "MaskedPython", "STRING_RE", "Unmaskable", "cell_views",
    "masked_python", "remasked", "sql_scan_text", "untokenizable_reason",
    "view_of",
]
