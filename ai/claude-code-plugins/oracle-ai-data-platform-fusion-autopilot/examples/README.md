# Examples

Current examples focus on the Phase 9 content-pack workflow.

| Example | Purpose |
|---|---|
| [full-finance-starter/](full-finance-starter/) | Default customer bundle scaffold used by `aidp-fusion-autopilot init`. Enables the full shipped finance starter scope and includes `bundle.yaml` plus `aidp.config.yaml`. |
| [minimal-bundle/](minimal-bundle/) | Narrow scaffold used by `aidp-fusion-autopilot init --template minimal-bundle`. Builds only the `supplier_spend` chain. |
| [overlay-pack/](overlay-pack/) | Additive content-pack overlay that adds a new gold mart without changing the shipped starter pack. See [../docs/mart_overlay_authoring.md](../docs/mart_overlay_authoring.md). |
| [fusion-finance-starter.yaml](fusion-finance-starter.yaml) | Older single-file content-pack bundle example kept for compatibility. Prefer `full-finance-starter/` for new users. |
| [aidp.config.example.yaml](aidp.config.example.yaml) | Older standalone AIDP config example kept for compatibility. Prefer `full-finance-starter/aidp.config.yaml` for new users. |

The old `minimal_gl_only.yaml` and `full_finance.yaml` examples are legacy
fixtures from the pre-Phase-9 runner. They remain in the repository for tests
and reference, but new projects should start from `aidp-fusion-autopilot init`.

## Recommended New-User Paths

Most users should install the Claude Code plugin and let autopilot drive the
setup from a clean sibling customer directory:

```text
/plugin marketplace add repo/oracle-ai-data-platform-fusion-autopilot
/plugin install oracle-ai-data-platform-fusion-autopilot@aidp-fusion-autopilot
/aidp-fusion-autopilot Build a CFO dashboard for supplier spend, AP aging, and GL balance using this Fusion tenant.
```

Use the manual CLI path when you want each command explicitly:

```bash
mkdir demo-fusion-cfo
cd demo-fusion-cfo
aidp-fusion-autopilot init
aidp-fusion-autopilot validate
aidp-fusion-autopilot dashboard mcp-setup --connector-js <path-to-oac-mcp-connect.js>
aidp-fusion-autopilot bootstrap --check-iam
aidp-fusion-autopilot run --mode seed --dry-run
```

For the full setup guide, see [../docs/project_setup.md](../docs/project_setup.md).
