# Changelog

Release notes for the Oracle AI Data Platform (AIDP) Workbench Snowflake
migrator Codex plugin, newest first. The format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

## [0.29.0] — 2026-10-10

### Security
- SEC-AIDP-SAMPLES-001: the migration config no longer accepts a credential
  value. An inline `password:`, `private_key:`, `key_passphrase:` or `token:`
  is refused by every stage with the `*_path` field to use instead, and the
  value is never echoed. Each credential lives in its own file, named by
  `password_path:`, `key_path:`, `key_passphrase_path:` or `pat_path:`.
- A credential file must be readable by its owner alone: a group- or
  world-readable file (mode & 0o077) is refused on POSIX with the `chmod 600`
  that fixes it. On Windows, where `st_mode` does not describe who can read a
  file, the check is skipped and the report says so.
- Before any discovery, the session role's grants are read back with `SHOW
  GRANTS TO ROLE` (primary and secondary roles) and the run stops if the role
  holds CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, MERGE, TRUNCATE or
  OWNERSHIP on the source database, or if the grants cannot be read.
  `preflight --test-source` reports the check and lists the grants found.
- The role check walks the role hierarchy: every role and database role a
  listing names (`USAGE on ROLE SYSADMIN`, `USAGE on DATABASE_ROLE DB.X`) is
  listed with `SHOW GRANTS TO ROLE` / `SHOW GRANTS TO DATABASE ROLE` in turn,
  with a visited set so a cycle terminates, and a write inherited through a
  granted role refuses the session with the carrying role named (`INSERT on
  TABLE ... via SYSADMIN`). A `current_secondary_roles()` of `ALL` that names
  no roles is resolved from `SHOW GRANTS TO USER`, or refused when that
  cannot be read; a write grant with no object name is judged inside the
  source rather than out of scope.
- `init-config` no longer says the config "will hold live credentials": it
  names the credential files, and those are what to keep out of git and chat.
- `preflight`, every stage and `PROVISION.md` print the credential's source
  — the file's basename and whether it is owner-only — never its value or
  its directory. Nothing is spooled to a temp file any more.

### Changed
- `provision --source-config` now accepts a `*_path` config (previously
  refused) and places the credential file(s) on the workspace mount at
  `plan/<config stem>.<field>` beside the `snowflake:` block, whose paths are
  rewritten to the mount's. Each file is checked before anything is uploaded;
  every placed object is tracked for `teardown --scope credential`.
- The data-plane connector and the EXTERNAL catalog registration read the
  credential from the file the config names and refuse an inline value.
- README, `snowmig-config.example.yaml`, PRIVACY.md and the skills describe
  the credential files, the read-only role, SSO/MFA, network policy, key
  rotation and query-history review. Same engine as the Claude Code plugin
  0.29.0.

## [0.28.0] — 2026-10-01

### Added
- First Codex release. Same engine, data-plane notebooks, skills, slash
  commands and twelve-step runbook as the Claude Code plugin 0.28.0
  (`ai/claude-code-plugins/oracle-ai-data-platform-workbench-snowflake-migrator`).
  Skills address the plugin's files as `<plugin-root>`, resolved from each
  `SKILL.md`.
