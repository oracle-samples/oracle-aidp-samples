"""Implementation of ``aidp-fusion-autopilot use-pack``.

Wires ``bundle.yaml`` to run on a content pack (or an overlay) in ONE command,
so operators — and the skills (``mart-author`` / ``aidp-fusion-seed``) — don't
hand-edit YAML. It is the codified form of the live-proven overlay-seed recipe
(see LIMITS.md L-overlay-seed):

  1. set ``contentPack: {name, path, profile}``;
  2. align ``dimensions.build`` / ``gold.marts`` to the resolved pack's real
     nodes (stale v1 entries like ``dim_org`` break the plan resolver);
  3. normalize ``fusion.password`` to ``${FUSION_BICC_PASSWORD}`` when it's a
     placeholder vault OCID (the cluster loads it from the AIDP credential
     store; a placeholder vault ref fails with CredentialResolutionError).

Comment-preserving: edits are text surgery (top-level block replace/append +
one targeted password-line rewrite), not a ``yaml.dump`` round-trip — the
customer's hand-authored bundle.yaml keeps its comments and ordering.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml
from rich.console import Console

# A placeholder vault password ref that fails cluster-side. Matched loosely so
# any ``...placeholder...fusion_password`` vault OCID is normalized.
_PLACEHOLDER_PW = re.compile(
    r"^(?P<indent>\s*)password:\s*\$\{vault:[^}]*placeholder[^}]*\}.*$",
    re.MULTILINE,
)
_ENV_PW = "${FUSION_BICC_PASSWORD}"


def _replace_or_append_top_level_block(text: str, key: str, block: str) -> str:
    """Replace a top-level ``<key>:`` block (through the line before the next
    top-level key / EOF), or append ``block`` if the key is absent.

    ``block`` must be the full block text including the ``<key>:`` line and a
    trailing newline. Preserves everything outside the block.
    """
    lines = text.splitlines(keepends=True)
    start = None
    for i, ln in enumerate(lines):
        if re.match(rf"^{re.escape(key)}:\s*(#.*)?$", ln):
            start = i
            break
    if start is None:
        # Append (ensure a blank line separator).
        sep = "" if text.endswith("\n\n") else ("\n" if text.endswith("\n") else "\n\n")
        return text + sep + block
    # Find end: next line that starts a top-level key (no indent, not blank/comment).
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if re.match(r"^[^\s#]", lines[j]):
            end = j
            break
    return "".join(lines[:start]) + block + "".join(lines[end:])


def _yaml_list_block(key: str, sub: str, items: list[str], note: str) -> str:
    body = "\n".join(f"    - {it}" for it in items)
    return f"{key}:\n  # {note}\n  {sub}:\n{body}\n"


def use_pack(
    bundle_path: Path,
    pack_spec: str,
    profile: str,
    *,
    align: bool = True,
    fix_credentials: bool = True,
    console: Console | None = None,
) -> int:
    console = console or Console()
    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1

    # Resolve the pack + its node ids (overlay-chain aware).
    try:
        from .content_pack import _load_full_chain, resolve_pack_path
        pack_path = resolve_pack_path(pack_spec)
        resolved = _load_full_chain(pack_path)
    except Exception as exc:
        console.print(f"[red]could not resolve pack {pack_spec!r}:[/red] {str(exc).splitlines()[0]}")
        return 2

    # Node ids come from the MERGED chain; the contentPack.name must be the
    # pack at pack_path itself (the overlay id), not the merged chain's id
    # (which is the base pack's id after merge).
    own = yaml.safe_load((pack_path / "pack.yaml").read_text(encoding="utf-8")) or {}
    pack_id = own.get("id") or resolved.pack.id
    silver_ids = sorted(resolved.silver)
    gold_ids = sorted(resolved.gold)

    # contentPack.path: relative to bundle parent when the pack lives beside the
    # bundle (the overlays/<name> case); omit for installed Oracle-shipped packs
    # (name-based lookup).
    bundle_root = bundle_path.parent.resolve()
    try:
        rel = pack_path.resolve().relative_to(bundle_root)
        path_line = f"  path: {rel}\n"
    except ValueError:
        path_line = ""  # installed pack — resolved by name

    text = bundle_path.read_text(encoding="utf-8")

    cp_block = (
        "contentPack:\n"
        f"  # wired by `aidp-fusion-autopilot use-pack {pack_spec}`\n"
        f"  name: {pack_id}\n"
        f"{path_line}"
        f"  profile: {profile}\n"
    )
    text = _replace_or_append_top_level_block(text, "contentPack", cp_block)

    changed = [f"contentPack -> {pack_id} (profile={profile})"]

    if align:
        text = _replace_or_append_top_level_block(
            text, "dimensions",
            _yaml_list_block("dimensions", "build", silver_ids,
                             f"aligned to {pack_id} silver nodes by use-pack"),
        )
        text = _replace_or_append_top_level_block(
            text, "gold",
            _yaml_list_block("gold", "marts", gold_ids,
                             f"aligned to {pack_id} gold nodes by use-pack"),
        )
        changed.append(f"dimensions.build={silver_ids}")
        changed.append(f"gold.marts={gold_ids}")

    if fix_credentials:
        new_text, n = _PLACEHOLDER_PW.subn(lambda m: f"{m['indent']}password: {_ENV_PW}", text)
        if n:
            text = new_text
            changed.append("fusion.password -> ${FUSION_BICC_PASSWORD} (credential-store env)")

    bundle_path.write_text(text, encoding="utf-8")
    for c in changed:
        console.print(f"[green]✓[/green] {c}")

    # Validate the result loads + the contentPack profile resolves.
    try:
        from ..schema.tenant_profile import resolve_profile_path
        raw = yaml.safe_load(text)
        assert (raw.get("contentPack") or {}).get("name") == pack_id
        prof_path = resolve_profile_path(bundle_path, profile)
        if not prof_path.exists():
            console.print(
                f"[yellow]profile {profile!r} not found at {prof_path}[/yellow] — "
                "run `aidp-fusion-autopilot bootstrap` to create it before seeding."
            )
    except Exception as exc:
        console.print(f"[red]post-write validation failed:[/red] {str(exc).splitlines()[0]}")
        return 1

    console.print(
        f"\n[bold]Wired[/bold] {bundle_path} -> pack [cyan]{pack_id}[/cyan]. Next:\n"
        "  1. [cyan]aidp-fusion-autopilot validate[/cyan]\n"
        "  2. [cyan]aidp-fusion-autopilot run --mode seed --datasets <node> --layers gold[/cyan]"
        "  (or /aidp-fusion-seed)\n"
    )
    return 0


__all__ = ["use_pack"]
