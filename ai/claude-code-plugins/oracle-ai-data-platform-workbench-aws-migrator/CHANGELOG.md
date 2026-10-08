# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow semver.

## [0.3.0] — 2026-09-15

### Changed
- **Athena → Spark SQL translator reworked off `sqlglot`** onto deterministic
  rules with explicit semantic-gap guards. The transpiler was silently changing
  meaning (`array_agg ORDER BY` dropped ordering, sub-day `date_add` added days,
  `to_hex(sha256(…))` double-hexed, multi-statement input truncated) and drifted
  between library versions. `sqlglot` is no longer a dependency; `boto3` is again
  the only runtime requirement.
- **`PASS` no longer claims to be runnable.** It now reads "translated, no known
  issue detected — not execution-verified" in the CLI summary, `report.md`,
  `report.html`, README and `verbs.md`. Nothing parses or executes an artifact,
  so a construct no rule covers is reported clean; the wording now says so.

### Added
- **Output-validation gates** in the Athena translator, restoring the parse check
  that was lost with `sqlglot`: multi-statement input, unrecognised or unbalanced
  statements, a dangling operator or `ORDER BY`, ten Presto-only constructs Spark
  cannot parse, a Spark-3.5 built-in allowlist, and residual `s3://` paths.
  A 20-query adversarial manifest that previously returned PASS 20/20 — including
  literal non-SQL and DDL still pointing at S3 — now flags correctly.
- **Generated Spark vocabulary** (`aws_aidp/translate/spark_builtins.py`): 418
  functions from `SHOW FUNCTIONS` and 326 keywords from the `SqlBaseLexer`
  vocabulary, produced by `scripts/generate_spark_builtins.py` and held to the
  live engine by CI. Hand-maintaining these lists produced both false PASSes and
  false flags.
- Offline stress suite grown to **369 tests**, including Spark 3.5 runtime checks
  that execute translated output, plugin frontmatter validation, and verdict
  wording checks.

### Fixed
- Three translator defects found against Spark 3.5.3: a `JOIN` after a rewritten
  `UNNEST` emitted unparseable SQL; bare and qualified array subscripts silently
  returned the wrong element (Athena is 1-based, Spark 0-based); the
  `.spark_session` rewrite hit unrelated attributes.
- Five false flags the gates themselves introduced: `GROUPING SETS`, `CASE … THEN
  (…) ELSE (…)`, parenthesised set operations, and AS-less table aliases.
- `UnicodeEncodeError` writing `report.html` under Windows cp1252 — the caller
  swallowed the error, so Windows users silently got no HTML report.
- Plugin frontmatter in `SKILL.md` and `commands/inventory.md` was invalid YAML,
  so Claude Code loaded both with empty metadata and the skill never triggered.
- `scripts/live_teardown.py` reported success when every deletion failed.

## [0.2.0] — 2026-09-11

### Added
- **Glue ETL → PySpark translator** (`translate/glue_to_spark.py`) — 11 deterministic
  rules; DynamicFrame-only transforms (ApplyMapping, ResolveChoice, Join, …) are
  flagged for manual review, never silently dropped.
- **Live AWS testing path** — `scripts/live_seed.py` / `live_teardown.py` seed and
  remove a small, tagged, idempotent AWS test stack (S3 + Glue catalog + Athena).
- **Claude Code plugin** — `.claude-plugin/plugin.json` + `marketplace.json`,
  `skills/aws-aidp-migrator/SKILL.md`, and `commands/{inventory,plan,migrate,verify}.md`.
- **MCP server** (`aws_aidp/mcp_server.py`, `aws-aidp-mcp` entry point) exposing the
  four verbs to Codex / Cursor / Claude Desktop. Requires Python 3.10+.
- Docs: `TESTING.md`, `docs/FDE_PRESENTATION.md` (+ generated deck), `docs/MCP.md`,
  `docs/DEMO_VIDEO_SCRIPT.md`.

### Fixed (found via live testing against a real AWS account)
- Athena inventory crashed on `list_work_groups` — it is not a boto3 paginator;
  replaced with a manual `NextToken` loop.
- `CROSS JOIN UNNEST(split(col, ';'))` fell through unrewritten **and** unflagged
  (false "OK"). Array-expression regex now allows one level of nested parens, plus a
  safety-net flag so any residual `CROSS JOIN UNNEST` is always flagged.
- Inline `frame=DynamicFrame.fromDF(df, …)` in `write_dynamic_frame.from_options`
  produced a broken `DynamicFrame.write…`; the `fromDF` collapse now runs before the
  from_options rules so it resolves to `df.write…`.

### Tests
- 15 standalone tests (8 Glue + 7 Athena), including regressions for all three bugs.

## [0.1.0] — 2026-06-28

### Added
- Initial POC: `inventory`, `plan`, `migrate --demo`, `verify` verbs.
- Athena (Presto/Trino) → Spark SQL translator with deterministic rewrite rules.
- Fixture-driven demo (Acme Insurance) via `demo.sh`.
