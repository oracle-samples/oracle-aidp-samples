# Oracle AIDP Migrator for AWS (Codex plugin)

Migrate an AWS data stack (Amazon S3, AWS Glue, Amazon Athena) to Oracle AI Data Platform. Same
deterministic engine as the Claude Code plugin, exposed to Codex / Cursor /
Claude Desktop through an **MCP server** with four tools: `inventory`, `plan`,
`migrate`, `verify`.

## Install (Codex)
```
codex plugin marketplace add oracle-samples/oracle-aidp-samples
codex plugin add oracle-ai-data-platform-workbench-aws-migrator@oracle-aidp-codex
```
Then install the Python engine once so the MCP server can run:
```
pip install -e "<plugin dir>[mcp]"     # requires Python 3.10+
```
The plugin auto-registers the `aws-aidp` MCP server via `.codex-plugin/plugin.json`
→ `.mcp.json`. You do **not** need to hand-edit `~/.codex/config.toml`.

## Tools
| Tool | Args | Does |
|---|---|---|
| `inventory` | `region`, `fixture`, `sources`, `output` | read-only AWS scan → manifest |
| `plan` | `manifest`, `output`, `namespace` | manifest → AIDP mapping plan |
| `migrate` | `plan_path`, `demo`, `filter`, `out_dir` | translate (offline by default) |
| `verify` | `report_or_dir`, `filter` | classify PASS / REVIEW / SKIP / FAIL |

No AWS account? `inventory(fixture="demo")` runs on a bundled sample.

See `TESTING.md` for the offline test pass and `MCP.md` for non-plugin MCP
clients (Cursor, Claude Desktop). Deterministic translation, MIT licensed.
