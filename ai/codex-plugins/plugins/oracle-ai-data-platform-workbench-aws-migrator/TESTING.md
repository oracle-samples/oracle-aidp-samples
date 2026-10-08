# Testing guide

Thanks for helping test **aws-aidp-migrator** — a CLI that migrates an AWS data
stack (S3 / Glue / Athena / EMR / SageMaker) to Oracle AIDP.

You do **not** need an AWS or OCI account for the core test pass — everything
runs offline against a bundled fixture. Budget ~30–45 min for a full pass.

---

## 1. Setup (5 min)

```bash
git clone https://github.com/oracle-samples/oracle-aidp-samples.git
cd oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-workbench-aws-migrator
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

Requirements: **Python 3.9+**. That's it — the only runtime dependency is `boto3`.

---

## 2. Smoke test — the 5-minute demo

```bash
./demo.sh
```

**Expected:** it runs `inventory → plan → migrate → verify` twice (Athena path and
Glue path) and finishes with two summary lines ending in `error=0`, then prints
paths to two reports under `/tmp/aws-aidp-demo/`.

✅ **Pass** if `demo.sh` exits 0 and both reports are generated.
❌ **File a bug** if it crashes, throws a traceback, or `error=` is not 0.

Open the reports and eyeball the SQL/PySpark diffs:
- `/tmp/aws-aidp-demo/migrated/report.md`      (Athena → Spark SQL)
- `/tmp/aws-aidp-demo/migrated-glue/report.md` (Glue → PySpark)

---

## 3. Unit tests

```bash
PYTHONPATH=. python3 tests/test_glue_to_spark.py
PYTHONPATH=. python3 tests/test_athena_to_spark_sql.py
# or, if you have pytest:  pytest -q

# dependency-free offline stress suite
PYTHONPATH=. python3 scripts/run_stress_tests.py
# include release-scale cases
PYTHONPATH=. python3 scripts/run_stress_tests.py --release
```

✅ **Pass** if the commands print their pass totals with no failures.

The development repository runs a CI matrix over Python 3.9–3.14 plus a native
Windows job. That workflow is not part of the published plugin, so run the
commands above locally.
Optional runtime-contract checks use MCP 1.x and Spark 3.5 / Python 3.11 /
Java 17. To run them locally:

```bash
pip install -e '.[mcp]'
PYTHONPATH=. python3 -m unittest -v tests.stress.test_mcp_parity

pip install 'pyspark~=3.5.0'
AIDP_SPARK_VERSION=3.5 PYTHONPATH=. \
  python3 -m unittest -v tests.stress.test_spark_runtime
```

---

## 4. Run the 4 verbs by hand (fixture mode — no AWS)

```bash
python3 -m aws_aidp.cli inventory --fixture demo -o /tmp/inv.json
python3 -m aws_aidp.cli plan     /tmp/inv.json   -o /tmp/plan.json
python3 -m aws_aidp.cli migrate  /tmp/plan.json  --demo -o /tmp/out
python3 -m aws_aidp.cli verify   /tmp/out
```

Try the flags: `migrate --filter athena` / `--filter glue`, `verify --filter athena`,
`inventory --sources glue,athena`. Note anything confusing or any flag that errors.

---

## 5. Translator testing — the part we most want stressed

The heart of the tool is the two dialect translators. **Try to break them.**

**Athena → Spark SQL** — write your own Athena/Presto queries and translate them:

```python
from aws_aidp.translate.athena_to_spark_sql import translate
r = translate("SELECT array_agg(x), cardinality(tags) FROM t "
              "CROSS JOIN UNNEST(split(tags, ',')) AS u(tag)")
print("changes:", r.changes, "flags:", r.flags)
print(r.translated_sql)
for f in r.findings: print(" -", f)
```

**Glue PySpark → Spark** — paste a real (or realistic) Glue ETL script:

```python
from aws_aidp.translate.glue_to_spark import translate
r = translate(open("some_glue_job.py").read(), oci_namespace="myns")
print(r.translated_sql)
```

**What to look for (report any of these):**
- A translated query/script that **won't actually run on Spark** (invalid syntax
  left behind). This is the most valuable bug type.
- Something translated **silently but wrongly** (a "rewrite" that changes meaning,
  or an unsupported construct passed through as OK with **0 flags** — it should be
  flagged, not silently accepted).
- A construct we **flag** that actually *does* have a clean Spark equivalent we
  could auto-rewrite (a missed rule).
- Crashes / tracebacks on any input.

Good source material: real AWS Glue sample scripts and Athena/Presto docs.

---

## 6. (Optional) Live AWS mode

Only if you have your own throwaway AWS account. See `README.md` → "Real AWS".

> The `scripts/live_*.py` helpers below live in the development repository and are
> **not** shipped with the published plugin. Skip this section if `scripts/` only
> contains the test runner.

There's a seeder that creates a tiny test stack:

```bash
AWS_PROFILE=<your-profile> python3 scripts/live_seed.py     # create test resources
AWS_PROFILE=<your-profile> python3 -m aws_aidp.cli inventory --region us-east-1
AWS_PROFILE=<your-profile> python3 scripts/live_teardown.py  # clean up (do this!)
```

⚠️ Always run `live_teardown.py` when done so you don't leave billable resources.

---

## 7. How to report a bug

Open a **GitHub Issue** on the repo with:

1. **Title** — one line, e.g. `Athena UNNEST with map arg passes with 0 flags`
2. **Command / input** — the exact command or the SQL/PySpark you fed in
3. **Expected** vs **Actual** output (paste both; include tracebacks in full)
4. **Environment** — `python3 --version`, OS
5. Label it `bug`, `translator`, or `question`

**One issue per problem.** Small, reproducible cases are gold.

---

## Known limitations — please DON'T file these

These are already known / by design — no need to report:

- **EMR and SageMaker translators are stubs** — `verify` reports them as `SKIP`
  ("translator not yet implemented"). That's expected; they're on the roadmap.
- **`migrate` has no live AIDP write-side yet** — `--demo` writes artifacts
  locally; a call without `--demo` fails clearly instead of claiming success.
  The live push remains roadmap v0.5.
- **AWS free-plan accounts can't create Glue *jobs*** (`AccessDenied` on
  `CreateJob`). That's an AWS account-tier limitation, not a tool bug. The
  translator doesn't need a live Glue job — feed it a script directly (§5).
- DynamicFrame-only transforms (`ApplyMapping`, `ResolveChoice`, `Join`, …) are
  **flagged and left in place** on purpose — they have no clean 1:1 in Spark.

---

Questions? Open a GitHub issue. Happy breaking. 🧪
