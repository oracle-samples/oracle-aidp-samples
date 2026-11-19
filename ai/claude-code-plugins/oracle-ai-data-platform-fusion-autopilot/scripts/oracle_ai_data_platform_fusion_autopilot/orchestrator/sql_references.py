"""Conservative, block-scoped extraction of a silver/gold node's upstream column
references from its (pre-render) SQL — the engine behind the declared-inputs gate
(AIDPF-2084 / AIDPF-2085).

Design (see docs/features/declared-inputs-contract-gate/plan.md):

* **Symbol-level, profile-independent.** Each demand is emitted in the SAME
  vocabulary the author declares ``requiredColumns`` in — a literal physical
  name, ``$column.<key>`` (from a ``{{ column.<key> }}`` token), or ``$coa.<role>``
  (from a ``{{ coa.<role> }}`` token). The gate then matches symbol-to-symbol with
  no profile, so it works on the profile-less run-start validation path.
* **Block-scoped.** SQL is split into nested query blocks (the top-level query,
  each ``WITH <name> AS ( … )`` CTE body, and each parenthesised subquery). Each
  block builds its OWN ``{alias: upstream_id}`` map from its FROM/JOIN clauses, so
  an alias that means an upstream table in one block and a CTE/derived table in
  another is never confused (e.g. ``inv`` in supplier_spend).
* **Conservative.** Only references the extractor can confidently attribute to an
  upstream table — ``<alias>.<Col>`` where ``<alias>`` maps to an upstream in the
  *same* block — become hard demands. A wildcard (``*`` / ``<alias>.*``) over an
  upstream is a provably-unverifiable read → hard violation. Bare unqualified
  identifiers are surfaced separately as warn-only candidates (they may be
  CTE-derived). Anything else is ignored — no false positives.

Out of scope (documented v1 gaps): columns hidden inside ``{{ semantic.<key> }}``
predicates (profile-resolved; backstopped by the live gates), and full SQL
semantics. Upgrade path is sqlglot (plan Risks) — the result dataclass stays the
same.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Schema tokens collapse to short sentinel identifiers so a FROM/JOIN target like
# ``{{ catalog }}.{{ bronze_schema }}.gl_coa`` becomes ``cat.br.gl_coa`` and the
# trailing segment (the node id) is recoverable by the FROM scanner.
_SCHEMA_TOKEN_SENTINEL = {
    "catalog": "cat",
    "bronze_schema": "br",
    "silver_schema": "sv",
    "gold_schema": "gd",
}
_SCHEMA_SENTINELS = set(_SCHEMA_TOKEN_SENTINEL.values())

_COLUMN_TOKEN_RE = re.compile(r"\{\{\s*column\.([A-Za-z0-9_]+)\s*\}\}")
_COA_TOKEN_RE = re.compile(r"\{\{\s*coa\.([A-Za-z0-9_]+)\s*\}\}")
_SEMANTIC_TOKEN_RE = re.compile(r"\{\{\s*semantic\.([A-Za-z0-9_]+)\s*\}\}")
_SCHEMA_TOKEN_RE = re.compile(r"\{\{\s*(catalog|bronze_schema|silver_schema|gold_schema)\s*\}\}")
_ANY_TOKEN_RE = re.compile(r"\{\{[^}]*\}\}")

# A ``{{ column.<key> }}`` token is rewritten to this sentinel identifier so it
# survives as a normal ``<alias>.<ident>`` / bare ``<ident>`` reference; the
# extractor maps the sentinel back to the ``$column.<key>`` symbol.
_COL_SENTINEL_PREFIX = "coltok_x_"
# ``{{ coa.<role> }}`` and ``{{ semantic.<key> }}`` are "role-like" tokens that
# render to column reads but carry no alias. Each is rewritten to a sentinel that
# survives block splitting, so the extractor can attribute the role/variant to
# the upstream source(s) of the block it is read from (like a column read), and
# the gate requires it declared (`$coa.<role>` / `$semantic.<key>`).
_COA_SENTINEL_PREFIX = "coarole_x_"
_SEM_SENTINEL_PREFIX = "semvar_x_"
_ROLE_SENTINEL_RE = re.compile(
    r"\b(?:" + _COA_SENTINEL_PREFIX + r"|" + _SEM_SENTINEL_PREFIX + r")[A-Za-z0-9_]+\b"
)

_SQL_KEYWORDS = {
    "on", "where", "group", "order", "by", "left", "right", "inner", "outer",
    "full", "cross", "join", "using", "having", "qualify", "window", "union",
    "all", "select", "from", "as", "and", "or", "where", "lateral", "limit",
    "distinct", "partition", "over", "when", "then", "else", "end", "case",
}

# Identifier (table or column). Allows the schema-sentinel dotted form.
_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"


@dataclass
class UpstreamReads:
    """Result of extracting one node's SQL.

    * ``demands`` — confidently-attributed reads, keyed by upstream source id,
      each a set of *symbols* (literal column name, ``$column.<key>``, or
      ``$coa.<role>``). These are gated as hard AIDPF-2084 demands.
    * ``role_sources`` — role-like reads via a standalone ``{{ coa.<role> }}``
      (``$coa.<role>``) or ``{{ semantic.<key> }}`` (``$semantic.<key>``) token,
      mapped to the **candidate upstream source ids** it is read from: the direct
      upstream(s) of the block the token appears in, or (for a token in a
      derived/CTE block with no direct upstream) the set of all upstreams
      referenced in the SQL. The gate requires the symbol declared in
      ``requiredColumns`` of one of those sources.
    * ``wildcard_sources`` — upstream ids read via ``*`` / ``<alias>.*`` (hard
      AIDPF-2084: unverifiable).
    * ``bare_identifiers`` — unqualified identifiers seen in a block that has an
      upstream source (warn-only AIDPF-2085 candidates; the validator filters
      them against the upstream ``outputSchema``).
    """

    demands: dict[str, set[str]] = field(default_factory=dict)
    role_sources: dict[str, set[str]] = field(default_factory=dict)
    wildcard_sources: set[str] = field(default_factory=set)
    bare_identifiers: set[str] = field(default_factory=set)

    def _add(self, source_id: str, symbol: str) -> None:
        self.demands.setdefault(source_id, set()).add(symbol)


# ---------------------------------------------------------------------------
# Comment / string-literal stripping (run before any scanning)
# ---------------------------------------------------------------------------


def _strip_comments_and_strings(sql: str) -> str:
    """Blank SQL line comments (``-- …``), block comments (``/* … */``), and
    single-quoted string literals to spaces, preserving length/newlines.

    The reference scanner must never read a column-looking token out of a comment
    or a quoted literal (e.g. ``-- s.B`` or ``'s.B'``) — that would be a
    false-positive demand and break the zero-false-positive guarantee.
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        two = sql[i : i + 2]
        if two == "--":
            j = sql.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif two == "/*":
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(" " * (j - i))
            i = j
        elif sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and sql[j + 1 : j + 2] == "'":
                    j += 2  # escaped '' inside the string
                    continue
                if sql[j] == "'":
                    j += 1
                    break
                j += 1
            out.append(" " * (j - i))
            i = j
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# Token neutralization (Step 1)
# ---------------------------------------------------------------------------


def _neutralize_tokens(sql: str) -> tuple[str, dict[str, str], dict[str, str]]:
    """Replace ``{{ … }}`` tokens with parseable sentinels, preserving demand.

    Returns ``(neutralized_sql, col_symbol_by_sentinel, role_symbol_by_sentinel)``:
    column-token sentinels map to ``$column.<key>``; COA and semantic sentinels
    map to ``$coa.<role>`` / ``$semantic.<key>``. All survive into the per-block
    text so the extractor can attribute them to the block they're read from.
    Inert tokens (watermark/run_id/snapshot) become harmless literals.
    """
    col_symbol: dict[str, str] = {}
    role_symbol: dict[str, str] = {}

    def _col(m: "re.Match[str]") -> str:
        key = m.group(1)
        sentinel = f"{_COL_SENTINEL_PREFIX}{key}"
        col_symbol[sentinel.lower()] = f"$column.{key}"
        return sentinel

    def _coa(m: "re.Match[str]") -> str:
        role = m.group(1)
        sentinel = f"{_COA_SENTINEL_PREFIX}{role}"
        role_symbol[sentinel.lower()] = f"$coa.{role}"
        return sentinel

    def _sem(m: "re.Match[str]") -> str:
        key = m.group(1)
        sentinel = f"{_SEM_SENTINEL_PREFIX}{key}"
        role_symbol[sentinel.lower()] = f"$semantic.{key}"
        return sentinel

    sql = _COLUMN_TOKEN_RE.sub(_col, sql)
    sql = _COA_TOKEN_RE.sub(_coa, sql)
    sql = _SEMANTIC_TOKEN_RE.sub(_sem, sql)
    # Schema tokens → sentinel idents so FROM targets are dotted, parseable.
    sql = _SCHEMA_TOKEN_RE.sub(lambda m: _SCHEMA_TOKEN_SENTINEL[m.group(1)], sql)
    # Everything else ({{ watermark_predicate }}, {{ run_id_literal }},
    # {{ snapshot_date }}) → an inert literal.
    sql = _ANY_TOKEN_RE.sub("NULL", sql)
    return sql, col_symbol, role_symbol


# ---------------------------------------------------------------------------
# Block splitting (Step 2)
# ---------------------------------------------------------------------------


_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)


def _paren_spans(sql: str) -> list[tuple[int, int]]:
    """All balanced ``(start, end)`` paren spans (inclusive indices)."""
    spans: list[tuple[int, int]] = []
    stack: list[int] = []
    for i, ch in enumerate(sql):
        if ch == "(":
            stack.append(i)
        elif ch == ")" and stack:
            spans.append((stack.pop(), i))
    return spans


def _split_blocks(sql: str) -> list[str]:
    """Return each query block's own-level text.

    A **subquery** paren group (one whose inner text contains ``SELECT`` — a CTE
    body or a derived table) starts a new block; a **function-call** paren group
    (``CAST(…)``, ``OVER(…)``, ``COALESCE(…)`` — no ``SELECT``) does NOT and is
    kept inline so the column refs inside it stay visible. For each block we blank
    only its **direct child subqueries** (not function parens), so:

    * a reused alias (``b``/``inv`` defined inside a CTE) can't leak into the
      parent block, AND
    * ``CAST(b.BalanceDr AS …)`` column refs at the block's own level survive.

    Blocks = the whole SQL (top-level) + each subquery group's inner text.
    """
    spans = _paren_spans(sql)
    subqueries = [sp for sp in spans if _SELECT_RE.search(sql[sp[0] + 1 : sp[1]])]
    ranges = [(0, len(sql))] + [(st + 1, en) for (st, en) in subqueries]

    def _scannable(lo: int, hi: int) -> str:
        # Direct child subqueries of this block = subquery spans inside (lo,hi)
        # not nested within another subquery that is itself inside (lo,hi).
        inside = [sp for sp in subqueries if lo <= sp[0] and sp[1] < hi]
        direct = [
            sp
            for sp in inside
            if not any(o != sp and o[0] <= sp[0] and sp[1] <= o[1] for o in inside)
        ]
        chars = list(sql[lo:hi])
        for st, en in direct:
            for k in range(st, en + 1):
                chars[k - lo] = " "
        return "".join(chars)

    return [_scannable(lo, hi) for lo, hi in ranges]


# ---------------------------------------------------------------------------
# Per-block source + reference scanning (Steps 2–3, 3b)
# ---------------------------------------------------------------------------

# A schema-qualified upstream source in FROM/JOIN: ``<schemaparts>.<id> [AS] <alias>``
# where the part before the id is one/two schema sentinels. The alias is optional.
_SOURCE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+"
    r"(?:(?P<s1>" + _IDENT + r")\.)?"
    r"(?:(?P<s2>" + _IDENT + r")\.)?"
    r"(?P<id>" + _IDENT + r")"
    r"(?:\s+(?:AS\s+)?(?P<alias>" + _IDENT + r"))?",
    re.IGNORECASE,
)


def _block_upstream_aliases(block: str, depends_on_ids: set[str]) -> dict[str, str]:
    """``{alias_or_id_lower: canonical_source_id}`` for upstream tables in THIS
    block's FROM/JOIN.

    A source counts as upstream only if its trailing id matches one of
    ``depends_on_ids`` AND it is schema-qualified (preceded by a schema sentinel)
    — so a bare ``FROM some_cte`` (CTE/derived) is never treated as an upstream.

    Spark unquoted identifiers are case-insensitive, so both the source-id match
    and the returned keys are normalised to lowercase (``FROM …src S`` →
    ``s.SecretCol`` must still resolve). The *value* preserves the canonical
    source id exactly as declared in ``depends_on_ids`` so downstream demand
    attribution keys against the contract correctly. The key is the SQL alias
    when present, else the id itself (covers an unaliased upstream).
    """
    dep_canonical = {d.lower(): d for d in depends_on_ids}
    out: dict[str, str] = {}
    for m in _SOURCE_RE.finditer(block):
        sid = m.group("id")
        schema_qualified = (m.group("s1") in _SCHEMA_SENTINELS) or (
            m.group("s2") in _SCHEMA_SENTINELS
        )
        canonical = dep_canonical.get(sid.lower())
        if not schema_qualified or canonical is None:
            continue
        alias = m.group("alias")
        if alias and alias.lower() not in _SQL_KEYWORDS:
            out[alias.lower()] = canonical
        else:
            out[canonical.lower()] = canonical  # unaliased — reference by bare id/column
    return out


_QUALIFIED_REF_RE = re.compile(r"\b(?P<alias>" + _IDENT + r")\.(?P<col>" + _IDENT + r"|\*)")
# A bare `*` projection ITEM (delimited by SELECT / `,` / end) anywhere in the
# SELECT list — e.g. `SELECT *`, `SELECT a, *`, `SELECT *, b`. Bounded so it does
# not match multiplication (`a * b`) or a qualified `t.*` (handled separately).
_BARE_STAR_ITEM_RE = re.compile(r"(?:^|,)\s*\*\s*(?:,|$)")
_PAREN_GROUP_RE = re.compile(r"\([^()]*\)")


def _has_bare_star_projection(block: str) -> bool:
    """True if the block's SELECT list contains a bare ``*`` item.

    Isolates the projection (between this block's ``SELECT`` and its ``FROM``),
    strips a leading set quantifier (``DISTINCT`` / ``ALL``) so ``SELECT DISTINCT
    *`` is caught, blanks parenthesised groups so ``COUNT(*)`` / function args
    don't count, collapses a Spark/Databricks ``* EXCEPT (...)`` star clause to a
    bare ``*`` (it still reads an unbounded upstream column set), and looks for a
    ``*`` that stands as its own select-list item. A qualified ``<alias>.*`` is
    NOT matched here (the qualified-ref scan handles it).
    """
    m = re.search(r"\bSELECT\b(?P<proj>.*?)\bFROM\b", block, re.IGNORECASE | re.DOTALL)
    proj = m.group("proj") if m else ""
    if not proj:
        return False
    # `SELECT DISTINCT *` / `SELECT ALL *` — the quantifier sits between SELECT
    # and the first projection item, so strip it before the bare-`*` scan.
    proj = re.sub(r"^\s*(?:DISTINCT|ALL)\s+", " ", proj, flags=re.IGNORECASE)
    prev = None
    while prev != proj:  # strip nested parens (function args, COUNT(*), EXCEPT list)
        prev = proj
        proj = _PAREN_GROUP_RE.sub(" ", proj)
    # `* EXCEPT (col, …)` (Spark 3.4+/Databricks 11.3+ star clause) — the EXCEPT
    # list parens are already blanked above; drop the trailing EXCEPT keyword so
    # the leading `*` reads as a bare wildcard item (it reads every column except
    # an explicit few — still an unbounded, unverifiable upstream read).
    proj = re.sub(r"\*\s*EXCEPT\b", "*", proj, flags=re.IGNORECASE)
    return _BARE_STAR_ITEM_RE.search(proj) is not None


def extract_upstream_reads(sql: str, *, depends_on_ids: set[str]) -> UpstreamReads:
    """Extract a node's upstream column demands from its pre-render SQL.

    ``depends_on_ids`` is the set of the node's declared upstream ids
    (``dependsOn.bronze`` + ``dependsOn.silver``); only sources in this set are
    considered upstream.
    """
    result = UpstreamReads()
    sql = _strip_comments_and_strings(sql)  # never scan comments / string literals
    neutral, col_symbol, role_symbol = _neutralize_tokens(sql)
    blocks = _split_blocks(neutral)

    # Role-token attribution pass: a standalone `{{ coa.<role> }}` /
    # `{{ semantic.<key> }}` is attributed to the direct upstream(s) of the block
    # it appears in. Tokens in a derived/CTE block (no direct upstream) are
    # deferred and resolved to the union of all upstreams referenced anywhere in
    # the SQL (covers dim_account, whose COA tokens live in the outer block over a
    # `(SELECT … FROM gl_coa)` subquery).
    referenced_upstreams: set[str] = set()
    role_block_attr: dict[str, set[str]] = {}
    role_deferred: set[str] = set()
    for block in blocks:
        aliases = _block_upstream_aliases(block, depends_on_ids)
        referenced_upstreams |= set(aliases.values())
        for sentinel in _ROLE_SENTINEL_RE.findall(block):
            sym = role_symbol.get(sentinel.lower())
            if sym is None:
                continue
            if aliases:
                role_block_attr.setdefault(sym, set()).update(aliases.values())
            else:
                role_deferred.add(sym)
    for sym in set(role_block_attr) | role_deferred:
        cands = set(role_block_attr.get(sym, set()))
        if not cands:  # only seen in derived blocks → fall back to all referenced
            cands = set(referenced_upstreams)
        result.role_sources[sym] = cands

    for block in blocks:
        aliases = _block_upstream_aliases(block, depends_on_ids)
        if not aliases:
            continue
        # Map a bare-id "alias" (unaliased upstream) so qualified refs to it work,
        # and remember the set of upstream aliases for wildcard / bare handling.
        upstream_alias_set = set(aliases)

        # Qualified references <alias>.<col> / <alias>.*
        for m in _QUALIFIED_REF_RE.finditer(block):
            alias, col = m.group("alias"), m.group("col")
            sid = aliases.get(alias.lower())  # Spark identifiers are case-insensitive
            if sid is None:
                continue  # alias not an upstream in this block → ignore
            if col == "*":
                result.wildcard_sources.add(sid)
                continue
            sym = col_symbol.get(col.lower(), col)  # sentinel → $column.x, else literal
            result._add(sid, sym)

        # A bare ``*`` projection item (anywhere in the SELECT list, not just
        # first) in a block with an upstream source → wildcard read of every
        # upstream in the block (can't prove which columns). Catches
        # `SELECT *`, `SELECT a, *`, `SELECT *, b` alike.
        if _has_bare_star_projection(block):
            result.wildcard_sources.update(aliases.values())

        # Bare identifiers (no qualifier) for the warn-only check. Collect simple
        # identifiers that are not keywords, not schema sentinels, not aliases,
        # and not column-token sentinels (those are real $column demands handled
        # below). The validator filters these against the upstream outputSchema.
        # Only meaningful when the block has exactly one upstream (else ambiguous).
        if len(set(aliases.values())) == 1:
            (only_sid,) = set(aliases.values())
            only_sid_low = only_sid.lower()
            # Residual = block with qualified refs (<alias>.<col>) and AS-targets
            # removed, so only *truly unqualified* column-position identifiers
            # remain (a qualified ref's column part and an output alias are NOT
            # bare reads).
            residual = _QUALIFIED_REF_RE.sub(" ", block)
            residual = re.sub(r"\bAS\s+" + _IDENT, " ", residual, flags=re.IGNORECASE)
            for tok in re.findall(r"\b" + _IDENT + r"\b", residual):
                low = tok.lower()
                if (
                    low in _SQL_KEYWORDS
                    or tok in _SCHEMA_SENTINELS
                    or low in upstream_alias_set
                    or low == only_sid_low
                    or low.startswith(_COL_SENTINEL_PREFIX)
                    or low.startswith(_COA_SENTINEL_PREFIX)
                    or low.startswith(_SEM_SENTINEL_PREFIX)
                ):
                    continue
                result.bare_identifiers.add(tok)
            # A bare column-token read in a single-upstream block is a real
            # $column demand on that upstream.
            for sentinel, sym in col_symbol.items():
                if re.search(r"\b" + re.escape(sentinel) + r"\b", block, re.IGNORECASE):
                    result._add(only_sid, sym)

    return result


__all__ = ["UpstreamReads", "extract_upstream_reads"]
