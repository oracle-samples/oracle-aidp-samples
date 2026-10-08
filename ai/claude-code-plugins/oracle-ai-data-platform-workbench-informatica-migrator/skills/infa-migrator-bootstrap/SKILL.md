---
name: infa-migrator-bootstrap
description: One-shot environment readiness check for the Informatica→AIDP migrator engine. Verifies Python + engine dependencies, which `infa2aidp` actually resolves, ANTHROPIC_API_KEY, and (only if the user needs them) OCI/AIDP reachability for deploy and reconcile. Use the first time the migrator is invoked on a workstation, or when any other infa-* skill fails with an import, auth, or connectivity error.
---

# `infa-migrator-bootstrap` — environment readiness check

Confirms the engine can actually run before any other skill attempts real
work. Idempotent — re-run whenever something unexpected fails.

## When to use

- The user is running this migrator for the first time on this workstation.
- Any other skill fails with an `ImportError`, a stale-version symptom, or an
  auth/connectivity error.
- The user is about to run `infa-deploy` or `infa-reconcile` against a real
  AIDP instance or Oracle database for the first time.

## Step-by-step

### 1. Python and engine dependencies

```bash
python3 --version                 # 3.9+
cd <repo-root>
pip install -e .                  # base deps: pyyaml, requests
```

Optional extras, only install what the user's next step actually needs:

```bash
pip install -e ".[llm]"           # anthropic — needed for migrate --use-llm/--agentic
pip install -e ".[reconcile]"     # jaydebeapi, oracledb — needed for infa-reconcile against Oracle
pip install oci                   # OCI SDK — needed for infa-deploy's request signing
pip install -e ".[dev]"           # pytest, pytest-cov — only for running the test suite
```

### 2. Confirm which `infa2aidp` actually resolves

This matters because an unrelated `infa2aidp` package installed elsewhere
(another editable install, a stale global install) can shadow this repo's
engine silently:

```bash
which infa2aidp
pip show infa2aidp | grep -i location
```

If `Editable project location` (or `Location`) does not point at *this*
repo's `engine/`, do not trust the bare `infa2aidp` command. Use the
unambiguous form instead — every skill in this plugin uses this by default:

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli version
```

Expected output:

```
infa2aidp 0.1.0 (python 3.13.x)
Claude API: configured (model: claude-opus-5)
```

(or `Claude API: ANTHROPIC_API_KEY not set -- rule-based only` if the key
isn't set — that's fine for the rule-based path.)

### 3. `ANTHROPIC_API_KEY` (only if the user wants LLM-assisted migration)

```bash
echo ${ANTHROPIC_API_KEY:+set}${ANTHROPIC_API_KEY:-unset}
```

Not required for `infa-analyze`, `infa-review`, `infa-reconcile`,
`infa-deploy`, `infa-optimize`, `infa-lineage`, `infa-rag`, or a rule-based
`infa-migrate-mapping` run. Required for `migrate --use-llm` or `--agentic`.
Without it, complex transformations either fall back to rule-based (only if
`RULE_BASED_FALLBACK=true` is set) or the run errors out loudly rather than
guessing — that is the tool's deliberate default (`RULE_BASED_FALLBACK=false`).

### 4. AIDP reachability (only before `infa-deploy`)

```bash
echo "${AIDP_REGION:?not set}" "${AIDP_INSTANCE_ID:?not set}" "${AIDP_WORKSPACE_KEY:?not set}"
echo "${AIDP_CLUSTER_KEY:-no cluster key -- jobs cannot be created}"
oci --profile "${OCI_PROFILE:-DEFAULT}" iam region list --query 'data[0].name' 2>&1 | head -1
```

`infa-deploy` uploads notebooks and creates AIDP jobs over OCI-signed REST
— it does **not** need a running cluster to do that upload, but it does
need an OCI credential (`~/.oci/config`, profile `OCI_PROFILE`), the
DataLake OCID (`AIDP_INSTANCE_ID`), region (`AIDP_REGION`), workspace key
(`AIDP_WORKSPACE_KEY`) and, for job creation, the cluster key
(`AIDP_CLUSTER_KEY`). A 401 means the profile has no valid credential
(for a session-token profile: `oci session authenticate --profile
<profile> --region <region>`). Run with `--dry-run` first if the user just
wants to see what would upload without needing live credentials at all.
The cluster the jobs run on also needs the `infa_compat` wheel installed
(see [`infa-deploy`](../infa-deploy/SKILL.md), Prerequisites).

### 5. Live Informatica repository reachability (only before `infa-discover`)

```bash
echo "${INFA_REPO:?not set}"
```

`infa-discover` is the one command that talks to a live Informatica
repository (SOAP or `pmrep`). Confirm `--host`, `--user`/`INFA_USER`,
`--password`/`INFA_PASSWORD`, `--repo`/`INFA_REPO`, `--domain`/`INFA_DOMAIN`
are set, and that the host is reachable from this workstation.

### 6. The confidence check: `demo.sh`

```bash
./demo.sh
```

Runs `analyze` + `migrate` (rule-based) over the committed fixture corpus
and verifies the output — no cloud account, no `ANTHROPIC_API_KEY`, no OCI
profile. Expect the final line `notebooks=11 error=0`. If this fails on a
clean checkout, something is wrong with the installation, not with a
customer's data — fix this before anything else.

## Output template

Report back as a single table:

```
| Check                        | Status | Notes |
|---|---|---|
| Python + base deps            |  OK    | 3.13.x, pyyaml/requests present |
| `infa2aidp` resolution         |  OK    | PYTHONPATH=engine resolves to this repo's engine/ |
| ANTHROPIC_API_KEY              |  n/a   | not needed for the requested command |
| demo.sh                        |  OK    | notebooks=11 error=0 |
```

Any FAIL row → give the exact command to fix it. Do not proceed to a
long-running skill (`infa-migrate-mapping` with `--use-llm`, `infa-deploy`,
`infa-reconcile` against a live Oracle source) until the relevant rows are
clean.

## Notes

- This skill never modifies anything and never talks to a live AIDP cluster
  — it only checks reachability and local install state.
- Re-run after any change of workstation, Python environment, or credential
  set.
