# Using aws-aidp-migrator over MCP (Codex, Cursor, Claude Desktop, …)

OpenAI's Codex CLI has **no plugin marketplace** like Claude Code. The portable way
to get the same four verbs in Codex / Cursor / Claude Desktop / any MCP client is the
bundled **MCP server**, which exposes `inventory`, `plan`, `migrate`, and `verify` as
MCP tools.

> **Requires Python 3.10+** — the MCP SDK does not support 3.9. The core `aws-aidp`
> CLI itself still runs on 3.9+; only the MCP server needs 3.10+.

## Install

```bash
pip install -e '.[mcp]'      # installs the mcp SDK + the aws-aidp-mcp entry point
aws-aidp-mcp                  # starts the stdio MCP server (Ctrl-C to stop)
```

## Client config

The server speaks stdio. Point your client at the `aws-aidp-mcp` command.

### OpenAI Codex CLI (`~/.codex/config.toml`)
```toml
[mcp_servers.aws-aidp]
command = "aws-aidp-mcp"
args = []
```

### Cursor / Claude Desktop (`mcp.json` / `claude_desktop_config.json`)
```json
{
  "mcpServers": {
    "aws-aidp": { "command": "aws-aidp-mcp", "args": [] }
  }
}
```

### Claude Code
Already wired via the plugin (`.mcp.json`) — no extra config needed once the plugin
is installed.

## Tools exposed

| Tool | Args | Does |
|---|---|---|
| `inventory` | `region`, `fixture`, `sources`, `output` | read-only AWS scan → manifest |
| `plan` | `manifest`, `output`, `namespace` | manifest → AIDP mapping plan |
| `migrate` | `plan_path`, `demo`, `filter`, `out_dir` | translate (default `demo=true`, offline) |
| `verify` | `report_or_dir`, `filter` | classify PASS / REVIEW / SKIP / FAIL |

Each tool shells out to the same tested `aws_aidp.cli` pipeline, so behavior is
identical to the CLI and the Claude Code plugin.

## AWS credentials
The server inherits the environment it's launched in — standard boto3 auth chain
(`AWS_PROFILE`, `~/.aws/credentials`, IAM role). For a no-AWS trial, call `inventory`
with `fixture="demo"`.
