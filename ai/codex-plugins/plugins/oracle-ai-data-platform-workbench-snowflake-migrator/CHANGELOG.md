# Changelog

Release notes for the Oracle AI Data Platform (AIDP) Workbench Snowflake
migrator Codex plugin, newest first. The format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- `snowflake-connector-python` is pinned below 5 (`>=4.7.0,<5`) in
  `engine/requirements.txt` and in the commented fallback line of
  `data-migration-scripts/requirements-aidp.txt`. Connector 5.x removed
  `snowflake.connector.auth.pat` and `errors.NonRetryableTlsError`, so a fresh
  install picked up 5.x and two connector tests failed on import.

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
- A grant that is not on source data is judged too, instead of being
  skipped: `MANAGE GRANTS` on the account (the role could grant itself any
  privilege, INSERT on every table included), `OWNERSHIP` of a role, user,
  warehouse or integration, `CREATE ROLE` / `CREATE USER` / `CREATE
  INTEGRATION` / `CREATE SHARE`, the `APPLY ... POLICY` and `APPLY TAG`
  privileges, `EXECUTE TASK` / `EXECUTE MANAGED TASK` and `MANAGE WAREHOUSES`
  each refuse the session with the escalation named. Each non-source class
  has an allowlist of what passes (USAGE/OPERATE/MONITOR/MODIFY on a
  warehouse; USAGE on an integration; MONITOR USAGE, MONITOR EXECUTION,
  MONITOR SECURITY, IMPORTED PRIVILEGES, CREATE DATABASE and CREATE WAREHOUSE
  on the account; USAGE/MONITOR on a resource monitor, network policy,
  compute pool, connection, replication or failover group, external volume,
  application or share); any other privilege, on any class, is refused as
  unknown rather than assumed harmless.
- A system role (`ACCOUNTADMIN`, `SECURITYADMIN`, `USERADMIN`, `SYSADMIN`,
  `ORGADMIN`, `GLOBALORGADMIN`) as the session's role, as a secondary role or
  anywhere in the walked hierarchy refuses the session before its grants are
  listed.
- Future grants are read with `SHOW FUTURE GRANTS TO ROLE` / `TO DATABASE
  ROLE` for every walked role and judged by the same rules: a future INSERT
  on a schema is a write on every table created there later. A future grants
  listing that cannot be read, or that reaches the 10,000-row cap, refuses
  the session like any other.
- The role check also lists `PUBLIC`, which every role holds and no `SHOW
  GRANTS TO ROLE` row names, and every other role granted to the session's
  user (`SHOW GRANTS TO USER`, for every session, not only under `USE
  SECONDARY ROLES ALL`): a write held by any of them, or a system role among
  them, refuses the session, since a leaked credential is one `USE ROLE`
  away from each. A user listing that cannot be read, a missing or empty
  `current_secondary_roles()`, or a grants row that is not a mapping of
  columns refuses the session instead of passing or crashing. The user
  listing is believed only when it is one the session could have produced:
  a listing that does not name the role the session runs as and each
  secondary role in effect (an empty listing included), or whose rows name
  no role at all (a null or blank `role`, a row without the column, a None
  row), is refused as cut or filtered rather than read as "the user holds
  nothing else". PUBLIC, which no user listing names, is exempt.
- The `database:` scope excuses only a write the check can place on another
  database: a refused privilege on a database-contained source class
  (table, view, schema, stage, function, ...) whose name carries a database
  other than `database:` is reported, not refused; a refused privilege on
  any other class — the account, the organisation, a listing, a class
  Snowflake adds later — or on a row whose class or name is blank is refused
  whatever the scope. Nor does the scope excuse an escalation: `MANAGE
  GRANTS`, `CREATE ROLE`, `CREATE USER`, `EXECUTE TASK`, the `APPLY ...
  POLICY` privileges and any privilege spelled `MANAGE ...`, `APPLY ...` or
  `EXECUTE ...` reach past the object a row reports them on, so one reported
  on a database, schema or table of another database is refused like one on
  the account. Class and privilege names are read with stray
  whitespace removed, so a padded or null `granted_on` is not a loophole: a
  blank class refuses the row, a padded `ROLE` is still walked, and a
  role-like class the walk does not follow (`APPLICATION_ROLE`,
  `INSTANCE_ROLE`, ...) is refused unwalked.
- Owning, altering or applying a masking, row access, aggregation,
  projection, join or privacy policy, a tag, an authentication, password,
  session or packages policy or a network rule is a write on the source in
  whatever database the object lives: a policy created elsewhere is applied
  to the source all the same, and its owner can change what the source
  shows. `MONITOR` on a user (its query history) is allowed.
- When the role check fails, `preflight` no longer reads the source
  afterwards: the schema listing and the connector session schema checks
  are reported as skipped ("not checked: the source role is not provably
  read-only"), and a skip is not a pass.
- Two allowances are documented as such: `CREATE DATABASE` on the account
  passes because a created database, even a `CLONE` of the source, is not a
  write on the source (a clone is still a copy of the estate under the
  role's control, so the README asks that it be withheld); `USAGE` on a
  procedure in the migration's database passes because discovery reads
  procedure DDL (an owner's-rights procedure is a write the credential could
  call, so the README asks that none be granted that is not needed).
- `OPERATE` on a task, pipe, alert or dynamic table and `APPLY` on a policy
  or tag count as writes on the source (OPERATE stays a read on a warehouse).
- The privilege that runs an owner's-rights object is refused in any
  database: `OPERATE` on a task, pipe, alert, dynamic table or service and
  `USAGE` of a procedure, notebook or Streamlit app outside `database:`
  each run a body with its owner's privileges, which the check never reads
  and which may write the source, so the scope no
  longer excuses them as writes on another database (and discovery never
  reads a procedure there). `OWNERSHIP` of such an object in another
  database stays reported only: it would then run as the read-only role.
- Placing a grant on another database is strict: the name must have exactly
  the parts `SHOW GRANTS` prints for its class (`DB`, `DB.SCHEMA`,
  `DB.SCHEMA.OBJECT`; a future row names its container, two or three) and
  no whitespace in an unquoted database or schema part; a padded
  (`SALES_DB .PUBLIC.ORDERS`), two-part or four-part name on a current grant
  is judged inside the scope rather than excused. An unquoted database part
  is compared with its whitespace removed.
- A role grant that names no role -- a null or blank name, a blank part
  (`SALES_DB.""`), or not the parts its class prints (`NAME` for a role,
  `DB.NAME` for a database role) -- is refused rather than listed as
  `show grants to role ""`.
- System-role names are compared with the stored name, case-sensitively:
  `current_role()`, the user listing and `SHOW GRANTS` print a stored name
  unquoted, and Snowflake stores an unquoted identifier in upper case, so
  exactly `SYSADMIN` is the system role and a distinct role created as
  `"sysadmin"` is no longer refused as one -- it is walked and judged by
  its grants like any other role (a false refusal before this change).
  The same holds for `PUBLIC`: a role created as `"public"` is a role the
  user was granted, and the user listing has to name it.
- `init-config` no longer says the config "will hold live credentials": it
  names the credential files, and those are what to keep out of git and chat.
- `preflight`, every stage and `PROVISION.md` print the credential's source
  — the file's basename and whether it is owner-only — never its value or
  its directory. Nothing is spooled to a temp file any more.
- Review round: `assess --database X` and `smoke --database X` handed the
  role check a list, which it read as the text `['X']`, so every real
  database was "outside the migration" and a write on the source was
  reported rather than refused. The scope is now a set of names — exactly
  the databases the run reads: the `--database` values when given, else
  the config's `database:` — and a scope of any other shape (an empty
  list, a member that is not a name) refuses the session.
- The role check resolves `database:` the way discovery does
  (`lexer.config_name`): `my-db`, `Sales DB` or `db-x` is taken as written
  — which is how `SHOW GRANTS` prints it, quoted — instead of being
  upper-cased to a name no grant carries, so a write on `"my-db"` is
  refused rather than excused as another database's. The evidence carries
  the resolved scope (`databases`).
- A procedure, notebook or Streamlit row the check cannot place (a name
  that is not `DB.SCHEMA.OBJECT`) no longer gets the inside-scope `USAGE`
  allowance; it is refused. The README says `database:` should always be
  set, and why.
- An inline credential is refused under any key spelling and inside nested
  mappings: `Password:`, `PASSWORD:`, `passcode:`, `pat:`, `oauth_token:`,
  `private_key_file:`, `passphrase:`, `secret:`, anything with `password`
  in it, or `extra: {password: ...}` is refused with the `*_path` field to
  use (or "remove it"), and `redact()` masks the same keys. The
  `snowflake:` block that `provision --source-config` uploads is built from
  an allowlist of the connection fields the in-AIDP readers use plus the
  `*_path` fields, never from the whole block, so a key the parser does not
  know never reaches the workspace mount.
- A credential file is opened once and judged through the descriptor that
  is read (`fstat`: a regular, owner-only file), on the laptop, in the
  transport and in the data-plane connector, so the path cannot be swapped
  between the check and the read; a FIFO is refused without blocking; an
  empty file is refused as empty rather than read as `''` and reported
  later as a missing `*_path`. The connector decides whether a file is on
  the workspace mount from its resolved path, so `/Workspace/../x` or a
  symlink out of the mount gets the owner-only check.
- A re-push without `--source-config` inherits only the config object
  (`plan/<stem>.json`, now recorded as `credential_config`), never a
  credential file placed beside it: after a push that found the config
  missing, the next one used to bake `plan/<stem>.key_path` into the
  notebooks as `--source-config`.
- Tests take the platform from a module flag (`_IS_WINDOWS`) instead of
  patching `os.name`, which pathlib consults and which made every
  `Path(...)` raise on Python 3.11 (the whole run INTERNALERRORed).
- Wording: a 0640 file is refused as readable by its "group or others";
  `init-config` and the YAML refusal no longer say the config holds a
  password; `preflight`'s "credential (inline, refused)" check, which the
  parser's refusal made unreachable, is gone.

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
