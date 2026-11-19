"""Transcribe v1 registry metadata into a checked-in test fixture.

Reads ``tests/fixtures/v1_registry_metadata_source.py`` — a byte-identical,
committed copy of ``schema/registry_metadata.py`` as it existed on the v1
reference branch ``P1.5ε-fix5`` — and emits
``tests/fixtures/v1_registry_snapshot.yaml`` as deterministic, byte-stable
YAML.

The v1 ``registry_metadata.py`` was deleted from the live tree in Phase 9,
and ``P1.5ε-fix5`` is a local-only branch absent from clean clones / CI.
Reading the committed fixture instead of shelling out to git keeps the
snapshot test self-contained — it runs identically anywhere.

Used as the parity baseline for v2's Phase 4 dual-runner gate. Both the
source fixture and the snapshot are **test data**, not runtime code — the
engine never imports them.

Provenance (recorded on every run):

    branch:        P1.5ε-fix5  (historical; source now committed as a fixture)
    branch head:   650d6909655fd30618f56edbbded6e4b81d6cc3b
    file blob:     02ec45a7fae7c1fa5b94a3940144727da69dcc13

The script verifies the committed fixture's git blob hash on every run.
If the blob hash diverges from 02ec45a7, the script hard fails — the v1
registry source has been modified and the snapshot must be re-reviewed
before regenerating. Regenerate the source fixture (only with explicit
re-review) via:

    git show P1.5ε-fix5:./scripts/oracle_ai_data_platform_fusion_autopilot/schema/registry_metadata.py \\
        > tests/fixtures/v1_registry_metadata_source.py

Usage:
    python scripts/dev/transcribe_v1_registry.py > tests/fixtures/v1_registry_snapshot.yaml

The script writes to stdout; redirect into the fixture path.

Re-running with the same input must produce byte-identical output (tested
in ``tests/unit/test_v1_registry_snapshot.py``).
"""

from __future__ import annotations

import ast
import hashlib
import sys
from pathlib import Path

import yaml

V1_BRANCH = "P1.5ε-fix5"
V1_FILE_REL = "./scripts/oracle_ai_data_platform_fusion_autopilot/schema/registry_metadata.py"
EXPECTED_HEAD = "650d6909655fd30618f56edbbded6e4b81d6cc3b"
EXPECTED_BLOB = "02ec45a7fae7c1fa5b94a3940144727da69dcc13"

# Committed, byte-identical copy of the v1 registry source. Self-contained
# replacement for `git show P1.5ε-fix5:<path>` — that branch is local-only
# and absent from clean clones / CI.
FIXTURE_SOURCE = (
    Path(__file__).resolve().parent.parent.parent
    / "tests"
    / "fixtures"
    / "v1_registry_metadata_source.py"
)


def _git_blob_hash(data: bytes) -> str:
    """Compute the git blob SHA-1 for a byte string (matches ``git hash-object``).

    Normalize CRLF -> LF first so the hash matches git's stored (LF) blob
    regardless of the checkout's line endings. Without this, a Windows
    checkout (autocrlf) reads CRLF bytes and the hash diverges from the
    LF-based pin even though the committed content is unchanged.
    """
    data = data.replace(b"\r\n", b"\n")
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()  # noqa: S324 — git uses SHA-1


def fetch_v1_registry_source() -> str:
    """Read the v1 registry_metadata.py source from the committed fixture."""
    if not FIXTURE_SOURCE.exists():
        print(
            f"ERROR: v1 source fixture missing: {FIXTURE_SOURCE}. Regenerate "
            f"(with re-review) via: git show {V1_BRANCH}:{V1_FILE_REL} > "
            f"{FIXTURE_SOURCE}",
            file=sys.stderr,
        )
        sys.exit(2)
    return FIXTURE_SOURCE.read_bytes().decode("utf-8")


def assert_provenance() -> tuple[str, str]:
    """Verify the committed fixture's blob hash matches recorded provenance.

    No git access — the historical branch head is reported verbatim for the
    snapshot's provenance block; integrity is enforced by the blob hash of
    the committed source fixture. Returns ``(branch_head, file_blob)``.
    """
    if not FIXTURE_SOURCE.exists():
        print(
            f"ERROR: v1 source fixture missing: {FIXTURE_SOURCE}.",
            file=sys.stderr,
        )
        sys.exit(2)

    current_blob = _git_blob_hash(FIXTURE_SOURCE.read_bytes())
    if current_blob != EXPECTED_BLOB:
        print(
            f"ERROR: {FIXTURE_SOURCE} blob has changed from {EXPECTED_BLOB} "
            f"to {current_blob}. The committed v1 registry source fixture has "
            "been modified. Re-review and update the snapshot manually.",
            file=sys.stderr,
        )
        sys.exit(2)

    return EXPECTED_HEAD, current_blob


def parse_v1_registry(source: str) -> dict:
    """Parse the v1 registry source via Python AST.

    Extracts the three top-level dict literals (BRONZE_EXTRACT_METADATA,
    SILVER_DIM_METADATA, GOLD_MART_METADATA) and the three deferred maps.
    """
    tree = ast.parse(source)
    out: dict = {
        "bronze_extract_metadata": {},
        "silver_dim_metadata": {},
        "gold_mart_metadata": {},
        "deferred": {
            "datasets": {},
            "dims": {},
            "marts": {},
        },
    }

    def _extract_dict_assignment(target_name: str) -> ast.Dict | None:
        for node in tree.body:
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == target_name
                and isinstance(node.value, ast.Dict)
            ):
                return node.value
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == target_name
                and isinstance(node.value, ast.Dict)
            ):
                return node.value
        return None

    def _ast_to_python(node: ast.AST) -> object:
        """Convert literal AST nodes (Call, Tuple, Constant, etc.) to plain Python."""
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, (ast.Tuple, ast.List)):
            return [_ast_to_python(e) for e in node.elts]
        if isinstance(node, ast.Call):
            # Dataclass invocation; map keyword args + positionals to dict.
            cls_name = getattr(node.func, "id", None) or "?"
            obj: dict = {"_class": cls_name}
            # Positional args (e.g., BronzeExtractMetadata("erp_suppliers", "erp_suppliers")).
            for i, arg in enumerate(node.args):
                obj[f"_pos{i}"] = _ast_to_python(arg)
            for kw in node.keywords:
                obj[kw.arg] = _ast_to_python(kw.value)
            return obj
        if isinstance(node, ast.Name):
            return f"<name:{node.id}>"
        raise NotImplementedError(f"unhandled AST node: {ast.dump(node)}")

    # ---- BRONZE ----
    bronze_dict = _extract_dict_assignment("BRONZE_EXTRACT_METADATA")
    if bronze_dict is not None:
        for k_node, v_node in zip(bronze_dict.keys, bronze_dict.values):
            assert isinstance(k_node, ast.Constant)
            key = k_node.value
            v_obj = _ast_to_python(v_node)
            assert isinstance(v_obj, dict)
            # BronzeExtractMetadata(dataset_id, pvo_id) — two positional args.
            entry = {
                "dataset_id": v_obj.get("dataset_id") or v_obj.get("_pos0"),
                "pvo_id": v_obj.get("pvo_id") or v_obj.get("_pos1"),
            }
            out["bronze_extract_metadata"][key] = entry

    # ---- SILVER ----
    silver_dict = _extract_dict_assignment("SILVER_DIM_METADATA")
    if silver_dict is not None:
        for k_node, v_node in zip(silver_dict.keys, silver_dict.values):
            assert isinstance(k_node, ast.Constant)
            key = k_node.value
            v_obj = _ast_to_python(v_node)
            assert isinstance(v_obj, dict)
            entry = {
                "dataset_id": v_obj.get("dataset_id") or v_obj.get("_pos0"),
                "depends_on_bronze": list(
                    v_obj.get("depends_on_bronze") or v_obj.get("_pos1") or []
                ),
                "natural_key": v_obj.get("natural_key", ""),
            }
            out["silver_dim_metadata"][key] = entry

    # ---- GOLD ----
    gold_dict = _extract_dict_assignment("GOLD_MART_METADATA")
    if gold_dict is not None:
        for k_node, v_node in zip(gold_dict.keys, gold_dict.values):
            assert isinstance(k_node, ast.Constant)
            key = k_node.value
            v_obj = _ast_to_python(v_node)
            assert isinstance(v_obj, dict)
            natural_key = v_obj.get("natural_key", "")
            # Normalise tuple/list -> list; bare string stays string.
            if isinstance(natural_key, list):
                natural_key_norm: object = list(natural_key)
            else:
                natural_key_norm = natural_key
            entry = {
                "dataset_id": v_obj.get("dataset_id") or v_obj.get("_pos0"),
                "depends_on_bronze": list(v_obj.get("depends_on_bronze") or []),
                "depends_on_silver": list(v_obj.get("depends_on_silver") or []),
                "natural_key": natural_key_norm,
                "incremental_capable": bool(
                    v_obj.get("incremental_capable", True)
                ),
            }
            out["gold_mart_metadata"][key] = entry

    # ---- DEFERRED (simple str-keyed dicts) ----
    for ast_name, out_key in [
        ("KNOWN_DEFERRED_DATASETS", "datasets"),
        ("KNOWN_DEFERRED_DIMS", "dims"),
        ("KNOWN_DEFERRED_MARTS", "marts"),
    ]:
        deferred_dict = _extract_dict_assignment(ast_name)
        if deferred_dict is None:
            continue
        for k_node, v_node in zip(deferred_dict.keys, deferred_dict.values):
            assert isinstance(k_node, ast.Constant)
            # Reason strings may be ast.Constant or a `Foo + Bar` Concat;
            # for our two-arg parenthesised strings, ast.parse already
            # collapses adjacent string literals.
            if isinstance(v_node, ast.Constant):
                out["deferred"][out_key][k_node.value] = v_node.value
            else:
                # Joined string (parenthesised concat). Walk and concat.
                parts: list[str] = []
                for sub in ast.walk(v_node):
                    if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                        parts.append(sub.value)
                out["deferred"][out_key][k_node.value] = "".join(parts)

    return out


def main() -> None:
    head, blob = assert_provenance()
    source = fetch_v1_registry_source()
    parsed = parse_v1_registry(source)

    payload = {
        "provenance": {
            "branch": V1_BRANCH,
            "branch_head": head,
            "file_blob": blob,
            "source_path_on_v1": V1_FILE_REL,
            "note": (
                "Test fixture transcribed from v1 reference branch. See "
                "tests/fixtures/README.md. Engine never imports this file."
            ),
        },
        **parsed,
    }

    yaml.safe_dump(
        payload,
        sys.stdout,
        sort_keys=True,
        default_flow_style=False,
        allow_unicode=True,
    )


if __name__ == "__main__":
    # The transcribed YAML contains non-ASCII identifiers (e.g. "P1.5ε"), so
    # force UTF-8 stdout or printing crashes on a non-UTF-8 console / pipe
    # (Windows cp1252). Safe no-op when already UTF-8.
    try:
        if (getattr(sys.stdout, "encoding", "") or "").lower().replace("-", "") != "utf8":
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except (AttributeError, ValueError, OSError):
        pass
    main()
