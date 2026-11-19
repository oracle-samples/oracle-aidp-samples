"""SQL template renderer for the content-pack execution backend.

The renderer is the **security boundary** between untrusted profile/run-id
values and Spark SQL execution. Profile-string values flow
through Spark parameter markers (``spark.sql(sql, args=rendered.params)``);
identifier substitutions (catalog/schema/column names) flow through an
allowlist regex (``^[A-Za-z_][A-Za-z0-9_]{0,62}$``) and are inlined as
quoted identifiers only if they match.

The renderer returns a typed :class:`RenderedSql` (NOT a raw string).
Strategy executors call ``spark.sql(rendered.sql, args=rendered.params)`` —
never ``spark.sql(rendered.sql)`` alone, never string concatenation.

Token contract:

* ``{{ catalog }}`` / ``{{ bronze_schema }}`` / ``{{ silver_schema }}``
  / ``{{ gold_schema }}`` — identifier substitution from ``RunContext``.
* ``{{ run_id_literal }}`` — parameter marker ``:run_id``.
* ``{{ watermark_predicate }}`` — composed from ``node.refresh.incremental.
  watermark.column`` + ``ctx.prior_watermark``; column identifier is
  allowlist-checked; watermark value is a parameter marker
  ``:watermark_<source>``.
* ``{{ profile.<dotted.key> }}`` — dotted lookup into the tenant profile;
  always emitted as a parameter marker ``:profile_<sanitised_key>``.
* ``{{ column.<name> }}`` — looked up in ``profile.resolved.column[<name>]``;
  identifier-substituted (the resolved value is a column name).
* ``{{ semantic.<name> }}`` — picks the candidate from the pack's
  ``semanticVariants.<name>.candidates`` whose ``id`` matches
  ``profile.resolved.semantic[<name>]``; renders the candidate's
  ``fragment`` with ``{table}`` replaced by the source bronze table
  identifier.

Failure modes:

* ``AIDPF-5001`` — identifier substitution fails the allowlist regex
  (catalog/schema name has unsafe chars; resolved column name is
  ``"evil"; DROP TABLE"`` etc.).
* ``AIDPF-5002`` — unknown ``{{ token }}`` not in the renderer's vocabulary.
* ``AIDPF-5003`` — variation-point reference (``{{ column.X }}`` or
  ``{{ semantic.X }}``) not resolved in the active profile / pack.
* ``AIDPF-5010`` — post-render check fails (comment markers ``--`` or
  ``/*``, multiple top-level statements, etc.).
* ``AIDPF-5011`` — profile value referenced by ``{{ profile.<key> }}``
  has a disallowed Python type (only str/int/float/bool/date/datetime).

Render is deterministic; no LLM or operator interaction is allowed during
seed/incremental execution.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from ..schema.medallion_pack import NodeYaml
from ..schema.tenant_profile import TenantProfile

# ResolvedPack is imported lazily inside functions where needed because
# orchestrator/content_pack.py also imports this file indirectly via
# preflight-style callers; keeping the cycle broken with lazy imports.


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_5001_IDENTIFIER_ALLOWLIST = "AIDPF-5001"
AIDPF_5002_UNKNOWN_TOKEN = "AIDPF-5002"
AIDPF_5003_UNRESOLVED_VARIATION = "AIDPF-5003"
AIDPF_5010_POST_RENDER_REJECTED = "AIDPF-5010"
AIDPF_5011_DISALLOWED_PARAM_TYPE = "AIDPF-5011"
AIDPF_5013_INVALID_SNAPSHOT_DATE = "AIDPF-5013"


class SqlRendererError(Exception):
    """Base error for SQL renderer failures. Subclasses carry the AIDPF code."""


class IdentifierAllowlistError(SqlRendererError):
    """Identifier substitution failed the allowlist regex (AIDPF-5001)."""


class UnknownTokenError(SqlRendererError):
    """Renderer encountered a ``{{ token }}`` not in its vocabulary (AIDPF-5002)."""


class UnresolvedVariationPointError(SqlRendererError):
    """Variation-point reference not found in active profile/pack (AIDPF-5003)."""


class PostRenderRejectedError(SqlRendererError):
    """Rendered SQL contains disallowed shape (comment markers, ``;`` middle,
    multiple statements). AIDPF-5010."""


class DisallowedParamTypeError(SqlRendererError):
    """Profile value's Python type isn't in the allowed set (AIDPF-5011)."""


class InvalidSnapshotDateError(SqlRendererError):
    """``profile.profile.snapshotDate`` is present but not an ISO-8601 date.

    The ``{{ snapshot_date }}`` token requires either an absent / empty
    value (falls back to literal ``CURRENT_DATE()``) or an ISO-8601
    date string (``YYYY-MM-DD``) that binds as a parameter. Any other
    shape — non-string types, malformed dates, or strings containing
    SQL like ``CURRENT_DATE()`` — fails with this error (AIDPF-5013).
    """


# ---------------------------------------------------------------------------
# Regexes (compiled once)
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"\{\{\s*([^{}\n]+?)\s*\}\}")
"""Match ``{{ token }}``. Inner content cannot contain ``{``, ``}`` or
newlines — keeps the parser greedy-resistant on malformed templates."""

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
"""SQL identifier allowlist: ASCII letters / digits / underscore;
must start with a letter or underscore; max 63 chars (Postgres-compatible)."""

_DISALLOWED_AFTER_RENDER = (
    "--",   # SQL line comment — injection vector
    "/*",   # SQL block comment open
    "*/",   # SQL block comment close
)
"""Substrings rejected by the post-render check (AIDPF-5010). Semicolons are
checked separately (a trailing newline-only ``;`` is acceptable, mid-string ``;``
is not — but the renderer enforces "no ``;`` at all" for simplicity)."""


# Allowed Python types for profile values flowing through parameter markers.
# Anything else (list, dict, set, None, custom classes) → AIDPF-5011.
_ALLOWED_PARAM_TYPES: tuple[type, ...] = (str, int, float, bool, date, datetime)


# Maximum depth for ``{{ profile.<a.b.c.d> }}`` dotted lookups.
_PROFILE_DOTTED_MAX_DEPTH = 4


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RenderedSql:
    """A rendered SQL template plus its parameter bindings.

    Attributes:
        sql: parameter-marker-bearing SQL string. Profile / run-id /
            watermark values appear as ``:param_name`` placeholders, NOT
            inline literals.
        params: dict of ``name -> value`` bindings for
            ``spark.sql(rendered.sql, args=rendered.params)``. Values are
            plain Python types from the allowed set
            (str / int / float / bool / date / datetime).
        hash_input: canonical-whitespace-normalised ``sql`` PLUS a sorted,
            deterministic serialisation of ``params``. Used by
            :func:`compute_rendered_sql_hash` so plan-hash drift detection
            catches "same template, different profile-resolved values."
    """

    sql: str
    params: Mapping[str, Any] = field(default_factory=dict)
    hash_input: str = ""


@dataclass(frozen=True)
class RunContext:
    """Render-time context provided by the orchestrator.

    Attributes:
        catalog: Spark catalog identifier (e.g. ``fusion_catalog``).
        bronze_schema / silver_schema / gold_schema: medallion-layer schema
            identifiers (e.g. ``bronze``, ``silver``, ``gold``).
        run_id: orchestrator-generated run identifier; emitted via the
            ``:run_id`` parameter marker (never inlined).
        prior_watermark: per-source prior watermark value for incremental
            mode. Keys are source ids (matching
            ``node.refresh.incremental.watermark.source``); values are the
            prior ``output_watermark`` from ``fusion_autopilot_state``. Empty
            dict on seed runs.
        mode: ``"seed"`` or ``"incremental"`` — drives whether
            ``{{ watermark_predicate }}`` emits an always-true predicate
            (seed) or a watermark-gated one (incremental).
        bronze_table_for_source: maps source id to the fully-qualified
            bronze table identifier used inside semantic-fragment
            ``{table}`` substitutions.
        active_profile_name: the ``contentPack.profile`` name declared
            on the bundle (e.g. ``"finance-default"``). Required —
            builtin adapters (Step 3) key off ``pack.pack.profiles``
            on this name; renderer extensions may need it for
            pack-default lookups. The tenant id on
            :class:`TenantProfile` is NOT a substitute (tenants can
            run any profile the pack ships).
    """

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    run_id: str
    active_profile_name: str
    prior_watermark: Mapping[str, Any] = field(default_factory=dict)
    mode: str = "seed"
    bronze_table_for_source: Mapping[str, str] = field(default_factory=dict)
    # bronze_extract_adapter needs the bundle for
    # bundle.fusion.{service_url, username, password, external_storage}
    # + bundle.fusion.schemaOverrides.<id> at extract time. Defaults to
    # None so silver/gold adapters that don't need it stay unaffected
    # (they should not depend on the bundle at the renderer layer).
    bundle: Any = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def render_node_sql(
    node: NodeYaml,
    pack: "ResolvedPack",  # noqa: F821 — forward ref, broken cycle
    profile: TenantProfile,
    ctx: RunContext,
) -> RenderedSql:
    """Render a node's SQL template into a :class:`RenderedSql`.

    Args:
        node: the validated ``NodeYaml`` whose ``implementation.sql`` path
            is the template to render.
        pack: the assembled ``ResolvedPack`` (used for the per-node source
            root via ``pack.root_for(qualified_id)`` and for
            ``semanticVariants`` candidate lookup).
        profile: validated ``TenantProfile`` for ``{{ profile.<key> }}``,
            ``{{ column.<name> }}``, ``{{ semantic.<name> }}`` substitution.
        ctx: render-time context (catalog, schemas, run_id, prior
            watermark, mode, bronze-table-for-source map).

    Returns:
        :class:`RenderedSql` with ``sql`` carrying parameter markers and
        ``params`` carrying the bindings. ``hash_input`` deterministic for
        a given (template, profile, ctx).

    Raises:
        IdentifierAllowlistError: AIDPF-5001 — identifier substitution
            failed the regex.
        UnknownTokenError: AIDPF-5002 — ``{{ X }}`` not in vocabulary.
        UnresolvedVariationPointError: AIDPF-5003 — ``column.X`` /
            ``semantic.X`` not declared in profile or pack.
        PostRenderRejectedError: AIDPF-5010 — rendered SQL contains
            disallowed shape (``--``, ``/*``, ``*/``, ``;``).
        DisallowedParamTypeError: AIDPF-5011 — profile value referenced
            via ``{{ profile.<key> }}`` has a disallowed Python type.
    """
    qualified_id = _qualified_id_for_node(pack, node)
    sql_path = pack.root_for(qualified_id) / node.implementation.sql  # type: ignore[union-attr]
    template_text = Path(sql_path).read_text(encoding="utf-8")

    params: dict[str, Any] = {}
    pack_variants = _build_semantic_variants_index(pack)
    primary_source = _resolve_primary_source(node, ctx)

    def replace(match: re.Match[str]) -> str:
        token = match.group(1).strip()
        return _substitute_token(
            token=token,
            node=node,
            ctx=ctx,
            profile=profile,
            primary_source=primary_source,
            pack_variants=pack_variants,
            params=params,
        )

    rendered = _TOKEN_RE.sub(replace, template_text)
    _check_post_render(rendered)

    # ----- Hash-normalized render pass -------------------------------
    # The plan-hash must be mode-independent: a MERGE node seeded then
    # run --mode incremental must produce the same plan-hash, or the
    # AIDPF-4040 continuity gate fires a false positive. The
    # only per-mode-varying token is {{ watermark_predicate }} (1=1 on seed,
    # `<col> > :watermark_<source>` on incremental). We compute hash_input
    # from a SECOND render pass where that token is forced to its canonical
    # `1=1` form (for_hash=True) and writes NO watermark_* param — so the
    # hash is identical across modes. Every OTHER token renders identically
    # in both passes, so a genuine template / profile / variation edit still
    # shifts the hash. Watermark column/source changes are caught
    # independently via dedicated fields in compute_content_pack_plan_hash,
    # so normalizing the predicate text away is lossless.
    hash_params: dict[str, Any] = {}

    def replace_for_hash(match: re.Match[str]) -> str:
        token = match.group(1).strip()
        return _substitute_token(
            token=token,
            node=node,
            ctx=ctx,
            profile=profile,
            primary_source=primary_source,
            pack_variants=pack_variants,
            params=hash_params,
            for_hash=True,
        )

    hash_rendered = _TOKEN_RE.sub(replace_for_hash, template_text)
    hash_input = _build_hash_input(hash_rendered, hash_params)
    return RenderedSql(sql=rendered, params=params, hash_input=hash_input)


def compute_rendered_sql_hash(rendered: RenderedSql) -> str:
    """Deterministic sha256 over a :class:`RenderedSql`'s ``hash_input``.

    Used by ``compute_content_pack_plan_hash`` so plan-hash drift detection
    catches "same template, different profile-resolved values" — cosmetic
    template whitespace doesn't shift the hash; profile-value flips do.
    """
    return hashlib.sha256(rendered.hash_input.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Fork fingerprints (guarded same-id replacement, AIDPF-2064)
#
# Two PROFILE-INDEPENDENT, validate-time-computable fingerprints over a base
# node, used by the ``replaceNode`` staleness gate and ``content-pack
# refresh-fork``. These are deliberately NOT ``compute_rendered_sql_hash``:
# that folds profile-resolved param values into ``hash_input`` and needs an
# execution context (TenantProfile/RunContext), so it is neither stable across
# tenant profiles nor computable at validate time.
# ---------------------------------------------------------------------------


def compute_fork_fingerprint(node: NodeYaml, pack: "ResolvedPack") -> str:  # noqa: F821
    """SHA-256 over a base SQL mart's *logic* — raw ``.sql`` template text plus
    the referenced ``{{ semantic.* }}`` candidate lists from the base pack.

    Profile-independent by construction: tokens are left **unresolved** (no
    ``{{ column.* }}`` / ``{{ coa.* }}`` / profile lookups), and for each
    referenced semantic variant the **full** candidate list (every id+fragment,
    order-stable) is folded in — not the profile-selected one. So a base fix to a
    candidate ``fragment`` in ``pack.yaml`` shifts the hash even though the
    ``.sql`` file is untouched (closing the pack-owned-render-input hole).

    Caller contract: ``node.implementation.type == "sql"`` (the merge-time guard
    fails closed on builtin/non-SQL before calling this).
    """
    impl = node.implementation
    sql_rel = getattr(impl, "sql", None)
    if sql_rel is None:
        raise ValueError(
            f"compute_fork_fingerprint requires a SQL node; node {node.id!r} has "
            f"implementation.type={getattr(impl, 'type', '?')!r}."
        )
    qualified_id = _qualified_id_for_node(pack, node)
    sql_path = pack.root_for(qualified_id) / sql_rel
    template_text = Path(sql_path).read_text(encoding="utf-8")
    canonical_sql = re.sub(r"\s+", " ", template_text).strip()

    # Collect referenced semantic-variant names from the raw template.
    referenced: list[str] = []
    for m in _TOKEN_RE.finditer(template_text):
        token = m.group(1).strip()
        if token.startswith("semantic."):
            name = token[len("semantic.") :].strip()
            if name and name not in referenced:
                referenced.append(name)

    variants = _build_semantic_variants_index(pack)
    semantic_material: list[str] = []
    for name in sorted(referenced):
        variant = variants.get(name)
        candidates = getattr(variant, "candidates", None) or []
        # Order-stable serialization of every candidate (id + fragment).
        cand_repr = [
            {"id": getattr(c, "id", None), "fragment": getattr(c, "fragment", None)}
            for c in candidates
        ]
        semantic_material.append(
            f"{name}:" + json.dumps(cand_repr, sort_keys=True, ensure_ascii=False)
        )

    material = "SQL:" + canonical_sql + "\nSEMANTIC:" + "|".join(semantic_material)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def compute_contract_fingerprint(node: NodeYaml) -> str:
    """SHA-256 over a base node's *YAML contract* — ``outputSchema`` (incl.
    ``pii``), ``requiredColumns``, and ``quality.tests``.

    Profile-independent and computable from the base node object alone. A base
    change that touches only the YAML (e.g. reclassifying a column to
    ``pii: high``, tightening a type, or adding a quality test) shifts this hash
    so the ``replaceNode`` staleness gate (AIDPF-2064) fires — even though the
    ``.sql`` and its fork fingerprint are unchanged. The PII case is
    security-relevant: dashboard enforcement reads the merged ``outputSchema``.
    """
    # outputSchema: declared order matters (it is the materialized column order).
    output_cols = [
        {"name": c.name, "type": c.type, "nullable": c.nullable, "pii": str(c.pii)}
        for c in node.output_schema.columns
    ]
    # requiredColumns: order-independent — sort sources and each column list.
    required = {
        src: sorted(cols) for src, cols in (node.required_columns or {}).items()
    }
    # quality.tests: order-independent — canonicalize each test, sort the set.
    tests = sorted(
        json.dumps(t.model_dump(by_alias=True), sort_keys=True, ensure_ascii=False)
        for t in node.quality.tests
    )
    material = json.dumps(
        {"outputSchema": output_cols, "requiredColumns": required, "qualityTests": tests},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _format_profile_value_for_params(value: Any) -> Any:
    """Validate that ``value`` is in the allowed parameter-type set.

    Returns the value unchanged when allowed (Spark accepts these directly
    via ``args=``). Raises :class:`DisallowedParamTypeError` (AIDPF-5011)
    otherwise.

    The booleans-check uses ``isinstance(value, bool)`` BEFORE
    ``isinstance(value, int)`` because ``bool`` subclasses ``int`` and we
    want explicit bool-vs-int diagnostics, not silent coercion.
    """
    # Order matters: bool before int (bool subclasses int in Python).
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str, date, datetime)):
        return value
    raise DisallowedParamTypeError(
        f"{AIDPF_5011_DISALLOWED_PARAM_TYPE}: profile value of type "
        f"{type(value).__name__!r} is not allowed for parameter-marker "
        f"substitution. Allowed types: str, int, float, bool, date, datetime."
    )


# ---------------------------------------------------------------------------
# Internals — token substitution
# ---------------------------------------------------------------------------


def _substitute_token(
    *,
    token: str,
    node: NodeYaml,
    ctx: RunContext,
    profile: TenantProfile,
    primary_source: str | None,
    pack_variants: dict[str, "Any"],
    params: dict[str, Any],
    for_hash: bool = False,
) -> str:
    """Dispatch a single ``{{ token }}`` to its substitution handler.

    Returns the string to splice into the rendered SQL. Mutates ``params``
    in place for parameter-marker substitutions.

    When ``for_hash`` is True this is the plan-hash normalization pass:
    ``{{ watermark_predicate }}`` is forced to its canonical ``1=1`` form
    (and writes no ``watermark_*`` param) so seed and incremental renders of
    the same node hash identically (Approach 3 / P-incr-L1). Every other
    token renders identically to the executable pass.
    """
    # 1. Simple identifier substitutions from RunContext.
    if token == "catalog":
        return _check_identifier(ctx.catalog, source=f"{{{{ catalog }}}}")
    if token == "bronze_schema":
        return _check_identifier(ctx.bronze_schema, source=f"{{{{ bronze_schema }}}}")
    if token == "silver_schema":
        return _check_identifier(ctx.silver_schema, source=f"{{{{ silver_schema }}}}")
    if token == "gold_schema":
        return _check_identifier(ctx.gold_schema, source=f"{{{{ gold_schema }}}}")

    # 2. Parameter-marker substitutions.
    if token == "run_id_literal":
        params["run_id"] = ctx.run_id
        return ":run_id"

    if token == "watermark_predicate":
        return _render_watermark_predicate(
            node, ctx, primary_source, params, for_hash=for_hash
        )

    if token == "snapshot_date":
        return _render_snapshot_date(profile, params)

    # 3. Dotted-prefix substitutions.
    if token.startswith("profile."):
        return _render_profile_lookup(token[len("profile."):], profile, params)

    if token.startswith("column."):
        return _render_column_lookup(token[len("column."):], profile)

    if token.startswith("semantic."):
        return _render_semantic_lookup(
            token[len("semantic."):],
            profile=profile,
            pack_variants=pack_variants,
            ctx=ctx,
            primary_source=primary_source,
        )

    if token.startswith("coa."):
        return _render_coa_role(token[len("coa."):], profile=profile, params=params)

    # 4. Unknown token.
    raise UnknownTokenError(
        f"{AIDPF_5002_UNKNOWN_TOKEN}: unknown template token {{{{ {token} }}}}. "
        f"Allowed: catalog, bronze_schema, silver_schema, gold_schema, "
        f"run_id_literal, watermark_predicate, snapshot_date, "
        f"profile.<key>, column.<name>, semantic.<name>, coa.<role>."
    )


def _check_identifier(value: str, *, source: str) -> str:
    """Validate ``value`` against the SQL identifier allowlist.

    Returns the value unchanged on match; raises
    :class:`IdentifierAllowlistError` (AIDPF-5001) on miss.
    """
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise IdentifierAllowlistError(
            f"{AIDPF_5001_IDENTIFIER_ALLOWLIST}: identifier substitution for "
            f"{source} resolved to {value!r}, which fails the allowlist "
            f"`^[A-Za-z_][A-Za-z0-9_]{{0,62}}$`."
        )
    return value


def _render_watermark_predicate(
    node: NodeYaml,
    ctx: RunContext,
    primary_source: str | None,
    params: dict[str, Any],
    *,
    for_hash: bool = False,
) -> str:
    """Render the ``{{ watermark_predicate }}`` token.

    On seed runs, returns an always-true predicate so the same template
    runs for both modes.

    On incremental runs, emits ``<watermark_column> > :watermark_<source>``
    where the column is identifier-checked and the prior watermark value
    flows through a parameter marker. Multi-source nodes emit the predicate
    for the primary source only; the primary is what advances the cursor.

    When ``for_hash`` is True (the plan-hash normalization pass), the
    predicate is forced to the canonical ``1=1`` form regardless of mode and
    writes NO ``watermark_*`` param — so a node's seed and incremental
    renders hash identically. The watermark column/source are still mixed
    into the plan-hash via dedicated fields in
    ``compute_content_pack_plan_hash``, so this normalization loses no
    drift-detection power.
    """
    if for_hash:
        return "1=1"

    if ctx.mode == "seed":
        # Seed mode: no watermark filter. Always-true predicate keeps the
        # template single-shape across modes.
        return "1=1"

    inc = node.refresh.incremental
    if inc is None or inc.watermark is None:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: node {node.id!r} uses "
            f"{{{{ watermark_predicate }}}} in incremental mode but declares no "
            f"`refresh.incremental.watermark` block."
        )

    column = _check_identifier(inc.watermark.column, source=f"watermark column for node {node.id}")
    source = primary_source or inc.watermark.source
    param_name = f"watermark_{_sanitise_param_segment(source)}"
    prior_value = ctx.prior_watermark.get(source)
    if prior_value is None:
        # No prior watermark — first incremental after a seed, or no rows yet.
        # Renderer cannot fabricate a sentinel; the caller (execute_node) is
        # responsible for switching the run to seed shape or supplying a
        # tenant-min watermark via ctx.prior_watermark. Treat absence as an
        # always-true predicate so the template still renders deterministically.
        return "1=1"
    params[param_name] = _format_profile_value_for_params(prior_value)
    return f"{column} > :{param_name}"


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
"""Strict ISO-8601 date shape for ``{{ snapshot_date }}`` profile values.

Used by :func:`_render_snapshot_date`. Anything that doesn't match this
regex (including ``CURRENT_DATE()`` strings, slashes, timestamps) is
rejected with AIDPF-5013; a separate ``date.fromisoformat`` round-trip
catches things like ``9999-99-99`` that match the regex but are not
real dates.
"""


def _render_snapshot_date(
    profile: TenantProfile,
    params: dict[str, Any],
) -> str:
    """Render the ``{{ snapshot_date }}`` token.

    Resolution order:

    * ``profile.profile.snapshotDate`` absent / ``None`` / empty string →
      emit the literal SQL expression ``CURRENT_DATE()`` (no parameter
      binding). This is the only place the renderer emits a raw SQL
      function call from a token — documented exception so production
      runs default to "today" without forcing every tenant to author a
      profile value.
    * Present, type ``str``, matches ``^\\d{4}-\\d{2}-\\d{2}$`` AND
      round-trips through ``datetime.date.fromisoformat`` → bind as
      ``:snapshot_date`` parameter. Test determinism path.
    * Anything else (non-string, malformed, contains SQL, etc.) → raise
      :class:`InvalidSnapshotDateError` (AIDPF-5013).

    Notes:

    * Empty string is treated as absent so a customer who clears their
      profile's ``snapshotDate`` doesn't accidentally break rendering
      — they get the same production default as if the key was never
      authored.
    * The value flows through ``params`` as a plain ``str`` (Spark
      accepts ISO-date strings for ``DATE`` comparisons).
    """
    value = profile.profile.get("snapshotDate") if isinstance(profile.profile, dict) else None

    # Absent / explicit-None / empty-string → fall back to raw SQL.
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return "CURRENT_DATE()"

    # Must be a string with the exact ISO-date shape.
    if not isinstance(value, str) or not _ISO_DATE_RE.match(value):
        raise InvalidSnapshotDateError(
            f"{AIDPF_5013_INVALID_SNAPSHOT_DATE}: profile.snapshotDate must be "
            f"absent or an ISO-8601 date string (YYYY-MM-DD); got "
            f"{value!r} of type {type(value).__name__}. Embedding SQL "
            f"functions (e.g. 'CURRENT_DATE()') in this profile field is "
            f"rejected — clear the value to fall back to CURRENT_DATE() at "
            f"render time, or pass a literal date string."
        )

    # Round-trip through fromisoformat to catch nonsense like '9999-99-99'.
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidSnapshotDateError(
            f"{AIDPF_5013_INVALID_SNAPSHOT_DATE}: profile.snapshotDate "
            f"{value!r} matches the YYYY-MM-DD shape but is not a real "
            f"calendar date: {exc}."
        ) from exc

    params["snapshot_date"] = value
    return ":snapshot_date"


def _render_profile_lookup(
    dotted_key: str,
    profile: TenantProfile,
    params: dict[str, Any],
) -> str:
    """Render ``{{ profile.<dotted.key> }}`` as a parameter marker.

    Looks up the value in ``profile.profile`` (the free-form block) via
    a depth-limited dotted walk. The value MUST have an allowed parameter
    type (AIDPF-5011); the lookup MUST resolve to a leaf (AIDPF-5003).

    The emitted parameter marker name sanitises the dotted key by
    replacing ``.`` with ``__`` so Spark accepts it as an identifier.
    """
    segments = dotted_key.split(".")
    if not segments or len(segments) > _PROFILE_DOTTED_MAX_DEPTH:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: profile lookup "
            f"{{{{ profile.{dotted_key} }}}} exceeds max depth "
            f"{_PROFILE_DOTTED_MAX_DEPTH} (or is empty)."
        )

    # Prefer the free-form profile.profile block; fall back to dotted
    # attributes on the validated model so `{{ profile.tenant }}` works.
    payload: Any = profile.profile
    for seg in segments:
        if isinstance(payload, dict) and seg in payload:
            payload = payload[seg]
            continue
        # Fall back to model attribute for the very first segment only —
        # e.g. ``{{ profile.tenant }}`` reaches profile.tenant on the model.
        if payload is profile.profile and hasattr(profile, seg):
            payload = getattr(profile, seg)
            continue
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: profile lookup "
            f"{{{{ profile.{dotted_key} }}}} unresolved at segment {seg!r}."
        )

    if isinstance(payload, (dict, list)):
        raise DisallowedParamTypeError(
            f"{AIDPF_5011_DISALLOWED_PARAM_TYPE}: profile lookup "
            f"{{{{ profile.{dotted_key} }}}} resolves to a {type(payload).__name__}; "
            f"only leaf scalar values are allowed for parameter-marker substitution."
        )

    value = _format_profile_value_for_params(payload)
    param_name = f"profile_{_sanitise_param_segment(dotted_key)}"
    params[param_name] = value
    return f":{param_name}"


_COA_ROLE_FIELD = {
    "balancing": "balancingSegment",
    "cost_center": "costCenterSegment",
    "natural_account": "naturalAccountSegment",
}

# Fusion chart_of_accounts_id values are numeric surrogate ids. We require keys
# to match this shape AND reject any quote/comment/control char defensively.
_COA_CHART_ID_RE = re.compile(r"^[0-9]{1,18}$")
_COA_DISCRIMINANT_COLUMN = "CodeCombinationChartOfAccountsId"


def _render_coa_role(role: str, *, profile: TenantProfile, params: dict[str, Any]) -> str:
    """Render ``{{ coa.<role> }}`` from ``profile.profile['chartOfAccounts']``.

    Single-COA (``default`` only) renders a bare, identifier-checked column.
    Multi-COA (``byChart`` present) renders a per-row CASE keyed by the BRONZE
    discriminant ``CodeCombinationChartOfAccountsId``; the WHEN chart-id values
    are PARAMETERIZED (``:coa_chart_<i>``), never inlined — the renderer's
    untrusted-profile-value contract. THEN columns are identifier-checked; the
    ELSE arm is a static ``raise_error`` literal. Arms are sorted by chart-id
    so the rendered text + params are deterministic (stable plan-hash).
    """
    field = _COA_ROLE_FIELD.get(role)
    if field is None:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: unknown COA role {{{{ coa.{role} }}}}. "
            f"Known roles: {sorted(_COA_ROLE_FIELD)!r}."
        )
    coa = (profile.profile or {}).get("chartOfAccounts")
    if not isinstance(coa, dict):
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: {{{{ coa.{role} }}}} referenced but the "
            f"active profile has no `chartOfAccounts`. Run bootstrap to resolve COA roles."
        )

    default_block = coa.get("default", coa)
    by_chart = coa.get("byChart") or {}

    def _col(block: dict) -> str:
        val = block.get(field)
        if not isinstance(val, str):
            raise UnresolvedVariationPointError(
                f"{AIDPF_5003_UNRESOLVED_VARIATION}: chartOfAccounts mapping is missing "
                f"{field!r} for role {role!r}."
            )
        return _check_identifier(val, source=f"{{{{ coa.{role} }}}}")

    if not by_chart:
        # Single-COA: bare column (identical to {{ column.coa_* }} behaviour).
        return _col(default_block)

    # Multi-COA: deterministic CASE with parameterized chart-id WHEN values.
    default_col = _col(default_block)
    whens: list[str] = []
    for i, chart_id in enumerate(sorted(by_chart.keys())):
        if not _COA_CHART_ID_RE.match(str(chart_id)):
            raise UnresolvedVariationPointError(
                f"{AIDPF_5003_UNRESOLVED_VARIATION}: byChart key {chart_id!r} is not a "
                f"valid numeric chart_of_accounts_id; refusing to render."
            )
        col = _col(by_chart[chart_id])
        param = f"coa_{role}_chart_{i}"
        params[param] = str(chart_id)
        whens.append(f"WHEN :{param} THEN {col}")
    whens_sql = " ".join(whens)
    return (
        f"CASE CAST({_COA_DISCRIMINANT_COLUMN} AS STRING) {whens_sql} "
        f"ELSE raise_error('unmapped chart_of_accounts_id for role {role}') END"
    )


def _render_column_lookup(name: str, profile: TenantProfile) -> str:
    """Render ``{{ column.<name> }}`` as an identifier substitution.

    Looks up ``profile.resolved.column[<name>]``; raises AIDPF-5003 if
    undeclared. Identifier-checks the resolved value (AIDPF-5001).
    """
    resolved = profile.resolved.column.get(name)
    if resolved is None:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: variation point "
            f"{{{{ column.{name} }}}} has no resolution in the active profile's "
            f"`resolved.column` map. Either author the profile to include this "
            f"key (e.g. `resolved: {{ column: {{ {name}: <chosen_col> }} }}`) or "
            f"remove the reference from the template."
        )
    return _check_identifier(resolved, source=f"{{{{ column.{name} }}}}")


def _render_semantic_lookup(
    name: str,
    *,
    profile: TenantProfile,
    pack_variants: dict[str, Any],
    ctx: RunContext,
    primary_source: str | None,
) -> str:
    """Render ``{{ semantic.<name> }}`` by selecting the matching candidate's fragment.

    The candidate id comes from ``profile.resolved.semantic[<name>]``;
    the fragment lives in ``pack.pack.semantic_variants.<name>.candidates``.
    The fragment may contain ``{table}`` which is substituted with the
    primary source's bronze table identifier.
    """
    candidate_id = profile.resolved.semantic.get(name)
    if candidate_id is None:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: variation point "
            f"{{{{ semantic.{name} }}}} has no resolution in the active profile's "
            f"`resolved.semantic` map."
        )

    variant = pack_variants.get(name)
    if variant is None:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: variation point "
            f"{{{{ semantic.{name} }}}} is referenced by the template but not "
            f"declared in `pack.yaml`'s `semanticVariants` block."
        )

    matching = [c for c in variant.candidates if c.id == candidate_id]
    if not matching:
        raise UnresolvedVariationPointError(
            f"{AIDPF_5003_UNRESOLVED_VARIATION}: profile picked semantic "
            f"candidate {candidate_id!r} for variant {name!r}, but that candidate "
            f"is not declared in `pack.yaml`'s semanticVariants.{name}.candidates."
        )

    fragment = matching[0].fragment
    # Semantic-fragment grammar: reject the same disallowed shapes as the
    # post-render check would (so the renderer fails fast on a malicious
    # pack instead of letting an injection slip through).
    _check_semantic_fragment_grammar(fragment, variant_name=name)

    if "{table}" in fragment:
        if primary_source is None:
            raise UnresolvedVariationPointError(
                f"{AIDPF_5003_UNRESOLVED_VARIATION}: semantic fragment for "
                f"variant {name!r} references {{table}} but node has no primary "
                f"source declared."
            )
        table = ctx.bronze_table_for_source.get(primary_source)
        if table is None:
            raise UnresolvedVariationPointError(
                f"{AIDPF_5003_UNRESOLVED_VARIATION}: semantic fragment references "
                f"{{table}} but ctx.bronze_table_for_source has no entry for "
                f"source {primary_source!r}."
            )
        # `table` may be a dotted identifier (catalog.schema.table). Validate
        # each segment against the identifier allowlist.
        for segment in table.split("."):
            _check_identifier(segment, source=f"bronze table for source {primary_source}")
        fragment = fragment.replace("{table}", table)

    return fragment


def _check_semantic_fragment_grammar(fragment: str, *, variant_name: str) -> None:
    """Enforce the semantic-fragment grammar.

    Forbids comment markers, semicolons, and SQL keywords that smell like
    subqueries (``SELECT``, ``UNION``, etc.). Allows column refs +
    comparison ops + ``COALESCE``/``NULLIF``/``IS NULL``/``IS NOT NULL``
    + standard arithmetic.
    """
    for marker in _DISALLOWED_AFTER_RENDER:
        if marker in fragment:
            raise PostRenderRejectedError(
                f"{AIDPF_5010_POST_RENDER_REJECTED}: semantic fragment for variant "
                f"{variant_name!r} contains disallowed substring {marker!r}."
            )
    if ";" in fragment:
        raise PostRenderRejectedError(
            f"{AIDPF_5010_POST_RENDER_REJECTED}: semantic fragment for variant "
            f"{variant_name!r} contains a semicolon."
        )
    # Block obvious subquery / DDL patterns by keyword (case-insensitive).
    forbidden_keywords = (
        "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP",
        "ALTER", "TRUNCATE", "GRANT", "REVOKE", "UNION", "INTERSECT", "EXCEPT",
    )
    upper = fragment.upper()
    for kw in forbidden_keywords:
        # Word-boundary match so column names containing these substrings
        # (e.g. `selected_amount`) don't false-positive.
        if re.search(rf"\b{kw}\b", upper):
            raise PostRenderRejectedError(
                f"{AIDPF_5010_POST_RENDER_REJECTED}: semantic fragment for variant "
                f"{variant_name!r} contains forbidden keyword {kw!r}."
            )


# ---------------------------------------------------------------------------
# Internals — post-render check
# ---------------------------------------------------------------------------


def _check_post_render(rendered: str) -> None:
    """Reject rendered SQL containing comment markers or semicolons.

    A rendered SQL string must be exactly one statement with no inline
    comments. Trailing whitespace is tolerated.
    """
    for marker in _DISALLOWED_AFTER_RENDER:
        if marker in rendered:
            raise PostRenderRejectedError(
                f"{AIDPF_5010_POST_RENDER_REJECTED}: rendered SQL contains "
                f"disallowed substring {marker!r}. The renderer forbids inline "
                f"SQL comments (``--`` / ``/* */``) so a malicious profile value "
                f"cannot smuggle one in via string substitution."
            )
    # Semicolons are forbidden entirely (strip trailing whitespace first).
    if ";" in rendered.rstrip():
        raise PostRenderRejectedError(
            f"{AIDPF_5010_POST_RENDER_REJECTED}: rendered SQL contains a semicolon. "
            f"Each template MUST render to exactly one statement with no terminator."
        )


# ---------------------------------------------------------------------------
# Internals — hash input
# ---------------------------------------------------------------------------


def _build_hash_input(sql: str, params: Mapping[str, Any]) -> str:
    """Canonicalise (sql, params) into a deterministic string for hashing.

    Whitespace normalisation: collapse runs of whitespace, strip leading/
    trailing. Param normalisation: sorted by key, values stringified with
    their type tag so ``1`` (int) and ``"1"`` (str) hash differently.
    """
    canonical_sql = re.sub(r"\s+", " ", sql).strip()
    # Exclude PER-RUN param VALUES from the plan-hash. ``run_id`` (run identity)
    # and ``watermark_<source>`` (the cursor, which advances every run) are not
    # part of the plan shape; the hash is meant to catch SQL-template /
    # outputSchema / variation-point / schema-fingerprint changes, not run
    # identity. Including them made the hash run-dependent, so the AIDPF-4040
    # continuity gate fired on every incremental after a seed (the run_id and
    # cursor always differ). The marker (``:run_id`` / ``:watermark_*``) still
    # appears in ``canonical_sql``, so a template change is still caught.
    plan_params = {
        k: v for k, v in params.items()
        if k != "run_id" and not k.startswith("watermark_")
    }
    sorted_params = sorted(plan_params.items())
    params_repr = "|".join(f"{k}:{type(v).__name__}:{v!r}" for k, v in sorted_params)
    return f"SQL:{canonical_sql}\nPARAMS:{params_repr}"


# ---------------------------------------------------------------------------
# Internals — helpers
# ---------------------------------------------------------------------------


def _qualified_id_for_node(pack: "ResolvedPack", node: NodeYaml) -> str:  # noqa: F821
    """Find the qualified id (``silver/<id>`` or ``gold/<id>``) for a node.

    Used by ``pack.root_for(...)`` lookups. Falls back to a bare id if the
    pack has no record (shouldn't happen for a validated pack, but a
    defensive default keeps unit tests trivial to set up).
    """
    if node.id in pack.silver:
        return f"silver/{node.id}"
    if node.id in pack.gold:
        return f"gold/{node.id}"
    return node.id


def _build_semantic_variants_index(pack: "ResolvedPack") -> dict[str, Any]:  # noqa: F821
    """Return a name → SemanticVariant index from the pack.

    Looks up ``pack.pack.semantic_variants`` (Pydantic field on PackYaml).
    Returns an empty dict if the pack declares no semanticVariants block.
    """
    variants = getattr(pack.pack, "semantic_variants", None) or {}
    if isinstance(variants, dict):
        return dict(variants)
    return {}


def _resolve_primary_source(node: NodeYaml, ctx: RunContext) -> str | None:
    """Return the primary source id for a node, if declared.

    Used for the watermark predicate's source-suffixed parameter marker
    and the semantic-fragment ``{table}`` substitution. Multi-source nodes
    follow the multi-source contract: exactly one primary source per node.
    """
    inc = node.refresh.incremental
    if inc is not None and inc.watermark is not None:
        return inc.watermark.source
    # Fall back to dependsOn.bronze[0] when no explicit watermark source.
    bronze = getattr(node.depends_on, "bronze", None) if node.depends_on else None
    if bronze:
        return bronze[0].id
    return None


_PARAM_SEGMENT_RE = re.compile(r"[^A-Za-z0-9_]")


def _sanitise_param_segment(segment: str) -> str:
    """Make a token segment safe for use inside a Spark parameter marker name.

    Dots become double underscores; any other non-identifier char becomes
    a single underscore. The original segment is round-trip-able for
    debugging because dots are the only common case and use ``__``.
    """
    return _PARAM_SEGMENT_RE.sub("_", segment.replace(".", "__"))
