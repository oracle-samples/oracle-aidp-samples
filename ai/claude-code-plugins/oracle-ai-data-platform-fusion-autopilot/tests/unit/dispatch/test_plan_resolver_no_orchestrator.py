"""Regression: ``schema.plan_resolver.resolve_dry_run_plan`` MUST NOT pull
``orchestrator/*`` into ``sys.modules`` — not even when *called*.

The sibling ``test_imports.py`` boundary tests only cover *import time*. The
COA-first dry-run ordering block in ``resolve_dry_run_plan`` used to do a
**lazy** ``from ..orchestrator.node_preflight import _coa_role_aliases`` inside
the function body, so an import-time boundary test could not see it — the leak
only happened when the resolver actually ran on the REST dry-run path
(``dispatch.dispatch_via_rest(dry_run=True)``). This test closes that gap by
building a pack + bundle and *invoking* the resolver in a clean subprocess,
then asserting no orchestrator module was loaded.

The COA role filter now lives in the neutral ``schema.coa_roles`` module, which
both the engine-side COA gate and this dry-run resolver share.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


_BUNDLE_YAML = """\
apiVersion: aidp-fusion-autopilot/v1
project: boundary-probe
fusion:
  serviceUrl: https://example.com
  username: test
  password: test
  externalStorage: test-storage
aidp:
  catalog: probe_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
datasets:
  - id: erp_suppliers
  - id: dim_supplier
dimensions:
  build: []
gold:
  marts: []
contentPack:
  name: boundary-probe-pack
  path: ./pack
  profile: probe-tenant
"""

# Subprocess body: build a ResolvedPack in-code from schema-layer models ONLY
# (no orchestrator loader), load the bundle, call resolve_dry_run_plan, and
# print every orchestrator.* module that ended up in sys.modules. A COA
# semanticRole alias is declared so the COA-first ordering block (the formerly
# leaking code path) actually executes.
_SUBPROCESS = r'''
import sys
from pathlib import Path

from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
    NodeYaml, PackYaml, ResolvedPack,
)
from oracle_ai_data_platform_fusion_autopilot.schema.plan_resolver import (
    resolve_dry_run_plan,
)

bundle_path = Path(sys.argv[1])

pack = PackYaml.model_validate({
    "id": "boundary-probe-pack",
    "version": "1.0.0",
    "description": "d",
    "compatibility": {"pluginMinVersion": "0.3.0"},
    # A COA semanticRole alias so resolve_dry_run_plan's COA-first block runs.
    "columnAliases": {
        "coa_bal": {
            "appliesTo": "bronze.erp_suppliers",
            "resolution": "semanticRole",
            "role": "coa.balancing",
            "candidates": ["SEGMENT1"],
        },
    },
})

bronze = NodeYaml.model_validate({
    "id": "erp_suppliers", "layer": "bronze",
    "implementation": {"type": "bronze_extract", "datastore": "X", "biccSchema": "F"},
    "target": "erp_suppliers",
    "dependsOn": {"bronze": [], "silver": []},
    "refresh": {"seed": {"strategy": "replace"}},
    "outputSchema": {"columns": [
        {"name": "SEGMENT1", "type": "string", "nullable": True, "pii": "low"},
    ]},
})
silver = NodeYaml.model_validate({
    "id": "dim_supplier", "layer": "silver",
    "implementation": {"type": "sql", "sql": "silver/dim_supplier.sql"},
    "target": "dim_supplier",
    "dependsOn": {"bronze": [{"id": "erp_suppliers"}]},
    "refresh": {"seed": {"strategy": "replace"}},
    "outputSchema": {"columns": [
        {"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"},
    ]},
})
resolved = ResolvedPack(
    root=Path("."), pack=pack,
    bronze={bronze.id: bronze}, silver={silver.id: silver},
)

bundle, paths = load_bundle(bundle_path)
plan, prereqs = resolve_dry_run_plan(
    resolved, bundle, paths, datasets=None, layers=None,
)
# Sanity: the COA-first block should have hoisted the bronze COA source ahead.
assert plan and plan[0].dataset_id == "erp_suppliers", [p.dataset_id for p in plan]

for m in sorted(sys.modules):
    if m.startswith("oracle_ai_data_platform_fusion_autopilot.orchestrator"):
        print(m)
'''


def test_resolve_dry_run_plan_does_not_load_orchestrator(tmp_path: Path) -> None:
    bundle_path = tmp_path / "bundle.yaml"
    bundle_path.write_text(_BUNDLE_YAML, encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, "-c", _SUBPROCESS, str(bundle_path)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        f"subprocess failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"
    )
    leaked = [ln for ln in proc.stdout.split() if ln]
    assert not leaked, (
        "resolve_dry_run_plan() pulled orchestrator module(s) into "
        f"sys.modules when called: {leaked}. The COA-first ordering block "
        "must use schema.coa_roles, NOT a lazy import of "
        "orchestrator.node_preflight — that breaks the dispatch import "
        "boundary on the REST dry-run path."
    )
