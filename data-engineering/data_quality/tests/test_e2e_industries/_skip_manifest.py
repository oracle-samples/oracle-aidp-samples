"""Skip-and-coverage manifest for the industry packs.

Single source of truth for:
1. **Which test cases must exist** per industry (coverage enforcement).
2. **Which Spark parametrizations are sanctioned skips** vs. must-run
   (skip-reason enforcement).

Used by:
- An in-process ``pytest_collection_modifyitems`` hook in
  ``tests/test_e2e_industries/conftest.py`` that asserts the collected
  node IDs contain every required tuple. Runs inside the single
  ``pytest tests/ -v --junitxml=artifacts/junit.xml`` invocation from
  plan Step 6 — no second pytest run.
- A CLI verifier ``python -m tests.test_e2e_industries._skip_manifest
  --verify-skips --junit artifacts/junit.xml`` that consumes the JUnit
  XML produced by that same run and asserts:
  (a) every ``skip_pandas_only`` row skipped with the sanctioned reason,
  (b) every ``required`` row executed (no skip, no xfail).

Editing this manifest is required when a coverage-matrix row is
added/removed in the plan — the PR review diffs this file line-for-line
against plan §Coverage matrices.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import NamedTuple


class Entry(NamedTuple):
    industry: str
    scenario_key: str
    backend: str  # "pandas" | "spark"
    status: str  # "required" | "skip_pandas_only"


# ---------------------------------------------------------------------------
# Manifest — derived line-for-line from plan.md §Coverage matrices.
# ---------------------------------------------------------------------------
# scenario_key naming: matches the pytest test function's ``scenario``
# marker keyword so collection and skip-message matching both work off
# the same identifier.
#
# Status rules (from plan §Backend contract per industry):
#   - All validator families (SLO / Threshold / Drift / Trend /
#     Shape / Pattern / Query JOIN) run on both backends. The
#     previously-sanctioned Pandas-only skips for Shape, Pattern,
#     and Query JOIN were lifted once those Spark paths were
#     verified end-to-end per spark-primary-industry-packs plan §1.
#   - The ``skip_pandas_only`` status value is retained in the
#     public API (and exported via ``sanctioned_skip_reason``) as
#     a sanctioned escape hatch for future regressions — see plan
#     Step 5 (negative step: retain escape hatch).

_ROWS_PER_INDUSTRY = (
    # (scenario_key, pandas_required, spark_required_or_skip)
    ("slo_pass",             "required", "required"),
    ("slo_fail",             "required", "required"),
    ("threshold_pass",       "required", "required"),
    ("threshold_fail",       "required", "required"),
    ("drift_pass",           "required", "required"),
    ("drift_fail",           "required", "required"),
    ("trend_pass",           "required", "required"),
    ("trend_fail",           "required", "required"),
    ("shape_pass",           "required", "required"),
    ("shape_fail",           "required", "required"),
    ("query_join",           "required", "required"),
    ("pattern_pass",         "required", "required"),
    ("pattern_fail",         "required", "required"),
)


INDUSTRIES = ("retail", "life_sciences", "financial_services")


def _build_manifest() -> list[Entry]:
    manifest = []
    for industry in INDUSTRIES:
        for scenario_key, pandas_status, spark_status in _ROWS_PER_INDUSTRY:
            manifest.append(
                Entry(industry, scenario_key, "pandas", pandas_status)
            )
            manifest.append(
                Entry(industry, scenario_key, "spark", spark_status)
            )
    return manifest


MANIFEST: list[Entry] = _build_manifest()


def sanctioned_skip_reason(scenario_key: str) -> str:
    """The only skip reason that counts as a Pandas-only skip."""
    return (
        f"SparkBackend not supported for {scenario_key} — "
        f"see shipped E2E notes"
    )


# ---------------------------------------------------------------------------
# Collection-time enforcement (in-process, via conftest hook).
# ---------------------------------------------------------------------------


def expected_node_id_fragments() -> set[tuple[str, str, str]]:
    """Return the set of (industry, scenario_key, backend) that must be
    collected. Every entry in the manifest — including sanctioned
    Pandas-only skips — must appear as a collected node, because a
    skip must still exist to be skipped.
    """
    return {(e.industry, e.scenario_key, e.backend) for e in MANIFEST}


def industries_present(collected_node_ids: list[str]) -> set[str]:
    """Return the set of industries actually present in this collection.

    The in-process hook uses this to scope enforcement to industries the
    current pytest invocation is targeting. A developer running only
    ``pytest tests/test_e2e_industries/retail/`` should not be forced to
    also have Life Sciences + Financial Services collected. CI's final
    authority on cross-industry coverage is the JUnit-based
    ``verify_skips`` step, which requires a testcase for every manifest
    row regardless of scope.
    """
    present: set[str] = set()
    for industry in INDUSTRIES:
        needle = f"tests/test_e2e_industries/{industry}/"
        if any(needle in nid for nid in collected_node_ids):
            present.add(industry)
    return present


def missing_from_collection(collected_node_ids: list[str]) -> list[tuple[str, str, str]]:
    """Return tuples that are expected but not present in collection.

    Scoped to industries whose tests are in the current collection — see
    ``industries_present``. Returns ``[]`` if no industry is targeted.
    """
    present = industries_present(collected_node_ids)
    if not present:
        return []
    missing = []
    for industry, scenario_key, backend in expected_node_id_fragments():
        if industry not in present:
            continue
        # Node IDs look like:
        #   tests/test_e2e_industries/retail/test_retail_e2e.py::
        #   test_slo_pass[pandas]
        # Match on industry path segment + scenario suffix + backend id.
        needle_path = f"tests/test_e2e_industries/{industry}/"
        needle_scenario = f"test_{scenario_key}"
        needle_backend = f"[{backend}]"
        matched = any(
            needle_path in nid
            and needle_scenario in nid
            and needle_backend in nid
            for nid in collected_node_ids
        )
        if not matched:
            missing.append((industry, scenario_key, backend))
    return missing


# ---------------------------------------------------------------------------
# Runtime enforcement (CLI, reads artifacts/junit.xml from single run).
# ---------------------------------------------------------------------------


def _classify_testcase(case: ET.Element) -> tuple[str, str, str] | None:
    """Extract (industry, scenario_key, backend) from a JUnit testcase.

    Uses ``file`` attribute first (pytest >=6), falls back to
    ``classname`` (always emitted). Returns ``None`` if the testcase
    is not from the industry packs.
    """
    path = case.attrib.get("file") or case.attrib.get("classname", "")
    # classname uses dots; normalize to slashes for prefix matching.
    path_slash = path.replace(".", "/")
    industry = None
    for candidate in INDUSTRIES:
        if f"tests/test_e2e_industries/{candidate}" in path_slash:
            industry = candidate
            break
    if industry is None:
        return None
    name = case.attrib.get("name", "")
    # name looks like: test_slo_pass[pandas] or test_slo_pass[spark]
    if "[" not in name or not name.endswith("]"):
        return None
    base_name, bracket = name.rsplit("[", 1)
    backend = bracket.rstrip("]").strip()
    if backend not in ("pandas", "spark"):
        return None
    if not base_name.startswith("test_"):
        return None
    scenario_key = base_name[len("test_"):]
    return (industry, scenario_key, backend)


def verify_skips(junit_path: str) -> list[str]:
    """Check skip reasons and required-row execution. Returns error
    messages (empty list = all good).
    """
    errors: list[str] = []
    if not Path(junit_path).exists():
        return [f"junit xml not found at {junit_path}"]
    tree = ET.parse(junit_path)
    by_key: dict[tuple[str, str, str], dict] = {}
    for case in tree.iter("testcase"):
        classified = _classify_testcase(case)
        if classified is None:
            continue
        skipped_el = case.find("skipped")
        by_key[classified] = {
            "skipped": skipped_el is not None,
            "skip_reason": (
                skipped_el.attrib.get("message", "")
                if skipped_el is not None
                else None
            ),
        }

    expected = {(e.industry, e.scenario_key, e.backend): e for e in MANIFEST}
    for key, entry in expected.items():
        actual = by_key.get(key)
        if actual is None:
            errors.append(
                f"missing testcase in junit.xml: {key[0]}.{key[1]}[{key[2]}]"
            )
            continue
        if entry.status == "required":
            if actual["skipped"]:
                errors.append(
                    f"required row {key[0]}.{key[1]}[{key[2]}] was "
                    f"skipped with reason {actual['skip_reason']!r} — "
                    f"mandatory rows cannot be skipped"
                )
        elif entry.status == "skip_pandas_only":
            if not actual["skipped"]:
                errors.append(
                    f"expected skip_pandas_only for {key[0]}.{key[1]}"
                    f"[{key[2]}] but it executed — Spark path is not "
                    f"supported for this scenario per plan §Backend "
                    f"contract"
                )
                continue
            expected_reason = sanctioned_skip_reason(key[1])
            actual_reason = (actual["skip_reason"] or "").strip()
            if actual_reason != expected_reason:
                errors.append(
                    f"{key[0]}.{key[1]}[{key[2]}] skip reason mismatch: "
                    f"got {actual_reason!r}, expected {expected_reason!r}"
                )
    return errors


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Industry pack manifest verifier")
    parser.add_argument(
        "--verify-skips",
        action="store_true",
        help="Verify skip reasons and required-row execution from JUnit XML",
    )
    parser.add_argument(
        "--junit",
        default="artifacts/junit.xml",
        help="Path to JUnit XML produced by the single pytest run",
    )
    args = parser.parse_args(argv)

    if args.verify_skips:
        errors = verify_skips(args.junit)
        if errors:
            for err in errors:
                print(f"::error::{err}", file=sys.stderr)
            return 1
        print(
            f"manifest verification passed: {len(MANIFEST)} entries, "
            f"{len(INDUSTRIES)} industries"
        )
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
