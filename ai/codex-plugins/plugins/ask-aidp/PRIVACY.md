# Privacy Policy

**Plugin:** `ask-aidp`

**Effective:** 2026-07-01

## Summary

This plugin does not collect, store, transmit, or share user data with the plugin author. It runs locally in Codex and operates against the user's Oracle AI Data Platform tenancy using the user's configured OCI credentials.

## What the plugin does

The plugin exposes Oracle AI Data Platform Workbench operations through a local MCP server. Most operations call the official `aidp-cli`; workspace Git operations that are not available in the CLI use the native AIDP TypeScript SDK.

The plugin can create or update AIDP resources when the user asks it to do so, including notebooks, workflows, catalogs, schemas, tables, bundles, workspace files, and Git-backed workspace folders.

## What the plugin does not do

- No telemetry is sent to Oracle Samples maintainers or third parties.
- No analytics, usage metrics, or error reporting are collected by the plugin.
- No OCI private keys, tokens, fingerprints, or passwords are collected or persisted by the plugin.
- No completed `aidp.env` file should be distributed with the plugin.

## Data flow

```text
User in Codex
  -> Ask AIDP plugin MCP server
  -> aidp-cli or AIDP TypeScript SDK
  -> user's Oracle AI Data Platform tenancy
```

When installed from GitHub, Codex clones the Oracle Samples repository. That repository access is governed by GitHub's terms and privacy policy.

## Local artifacts

The plugin may write local run evidence, request transcripts, generated notebooks, exported task outputs, and downloaded logs into user-selected or plugin-created local folders when the user asks for operations that require evidence collection.

## Credentials

The plugin uses the user's local OCI configuration, such as `~/.oci/config`, `OCI_PROFILE`, `OCI_CONFIG_FILE`, and `AIDP_AUTH`. If browser-based `oci login` is unavailable, the plugin documentation guides users to configure OCI API key authentication.

## Contact

For questions, open an issue at <https://github.com/oracle-samples/oracle-aidp-samples/issues>.
