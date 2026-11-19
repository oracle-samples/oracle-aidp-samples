"""Operator-runnable live E2E for the ``aidp-fusion-seed`` skill.

Drives the skill's real decision chain against a live tenant — intent parse →
preconditions → fail-closed guard → ``run --mode seed --dry-run`` → (gated)
real dispatch — using the SAME helper modules the SKILL.md invokes. Produces
the live evidence the AGENTS.md testing discipline requires for any
plugin-portability claim (``tests/live/TC<N>_seed_skill_results.md``).

NOT collected by CI (filename is not ``test_*.py``). Operator-driven only:
requires real identifiers (via flags or ``AIDP_*`` env vars) and the
``aidp-fusion-autopilot`` CLI installed. A real seed runs ONLY with ``--execute``;
without it the harness stops after the dry-run + guard decision (safe to run
against any tenant).

Usage:
  .venv/bin/python tests/live/seed_skill_e2e.py \\
      --bundle bundle.yaml --config aidp.config.yaml --env dev \\
      --phrase "seed supplier_spend"            # dry-run + guard only
  .venv/bin/python tests/live/seed_skill_e2e.py ... --execute   # real seed
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "skills/aidp-fusion-seed"))

import guard  # type: ignore[import-not-found]  # noqa: E402
import intent  # type: ignore[import-not-found]  # noqa: E402
import preconditions as pre  # type: ignore[import-not-found]  # noqa: E402


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    print(f"\n$ {' '.join(cmd)}")
    cp = subprocess.run(cmd, capture_output=True, text=True)
    sys.stdout.write(cp.stdout)
    sys.stderr.write(cp.stderr)
    return cp


def _known_nodes(cli: str, bundle: Path, config: Path, env: str) -> list[str]:
    """Assemble known node ids = bundle datasets + pack silver + gold."""
    import yaml

    raw = yaml.safe_load(bundle.read_text(encoding="utf-8"))
    nodes = [d["id"] for d in raw.get("datasets", []) if isinstance(d, dict) and d.get("id")]
    pack = (raw.get("contentPack") or {}).get("name")
    if pack:
        cp = _run([cli, "content-pack", "info", pack, "--json"])
        if cp.returncode == 0:
            try:
                info = json.loads(cp.stdout)
                nodes += info.get("nodes", {}).get("silver", [])
                nodes += info.get("nodes", {}).get("gold", [])
            except json.JSONDecodeError:
                pass
    return list(dict.fromkeys(nodes))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cli", default="aidp-fusion-autopilot", help="CLI entrypoint.")
    ap.add_argument("--bundle", type=Path, default=Path("bundle.yaml"))
    ap.add_argument("--config", type=Path, default=Path("aidp.config.yaml"))
    ap.add_argument("--env", default="dev")
    ap.add_argument("--phrase", default="seed supplier_spend")
    ap.add_argument("--poll-timeout", type=int, default=14400)
    ap.add_argument("--execute", action="store_true",
                    help="Dispatch the REAL seed. Without it: dry-run + guard only.")
    ns = ap.parse_args(argv)

    # 1 — intent
    known = _known_nodes(ns.cli, ns.bundle, ns.config, ns.env)
    parsed = intent.parse(ns.phrase, known)
    print("\n=== intent ===")
    print(json.dumps({"argv": parsed.argv, "ambiguous": parsed.ambiguous,
                      "unknown_tokens": parsed.unknown_tokens,
                      "needs_run_id": parsed.needs_run_id}, indent=2))
    if parsed.ambiguous or parsed.unknown_tokens or parsed.needs_run_id:
        raise SystemExit("intent unresolved — the skill would ask the user here; "
                         "supply an unambiguous --phrase for the live harness.")

    # 2 — preconditions (live cluster probe)
    print("\n=== preconditions ===")
    pc = pre.check_preconditions(bundle_path=ns.bundle, config_path=ns.config, env_name=ns.env)
    print(json.dumps({"ok": pc.ok, "missing": pc.missing, "tenant": pc.tenant,
                      "cluster_state": pc.cluster_state,
                      "config_placeholders": pc.config_placeholders}, indent=2))
    if not pc.ok:
        raise SystemExit(f"preconditions not satisfied: missing={pc.missing} — "
                         "run the auto-fix ladder (bootstrap / init-config / start cluster).")

    scope_flags = parsed.argv[3:]  # everything after ["run","--mode","seed"]

    # 3 — guard (fail closed: today's CLI has no status --json -> confirm)
    decision = guard.classify_guard([], status_json_supported=False)
    print("\n=== destructive guard ===")
    print(json.dumps(decision, indent=2))

    # 4 — dry-run plan
    _run([ns.cli, "run", "--mode", "seed", *scope_flags, "--dry-run"])

    if not ns.execute:
        print("\n[dry-run only] re-run with --execute to dispatch the real seed "
              "(operator confirms the guard decision first).")
        return 0

    # 5 — real dispatch (operator opted in)
    cp = _run([ns.cli, "run", "--mode", "seed", *scope_flags,
               "--poll-timeout", str(ns.poll_timeout)])
    print(f"\n=== seed exit={cp.returncode} ===")
    print("Capture the per-step table + run_id into tests/live/TC<N>_seed_skill_results.md")
    return cp.returncode


if __name__ == "__main__":  # pragma: no cover — operator entry point
    sys.exit(main())
