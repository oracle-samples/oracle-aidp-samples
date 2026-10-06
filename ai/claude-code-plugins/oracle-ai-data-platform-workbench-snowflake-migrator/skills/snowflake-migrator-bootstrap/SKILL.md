---
name: snowflake-migrator-bootstrap
description: First-run setup for the Snowflake to AIDP migrator. Finds or creates the one migration config file (snowmig-config.yaml), reads every field back with secrets masked, verifies Snowflake authentication by password, key-pair, programmatic access token or SSO, verifies the AIDP end, then smoke-tests the connection and reports the account, region, role and warehouse. Use the first time the migrator runs on a machine, or when any other stage fails with an authentication, connection or "no config found" error.
---

# Bootstrap

Getting from "nothing set up" to "both ends verified". Five steps, in order.

## 1. Dependencies — nothing to install, nothing left behind

`bin/snowmig` needs no bootstrap step and creates nothing that persists. It
runs `engine/snowmig.py` on the first interpreter on `PATH` that already
imports `yaml` and `snowflake.connector` (`SNOWMIG_PYTHON` is tried first,
then `python3`, `python3.13`, `python3.12`, `python3.11`, `python`). If none
does, it builds a throwaway venv under `$TMPDIR` for that one invocation,
prints `building a throwaway environment (removed on exit)` to stderr — a
notice, not an error — and deletes the venv on exit, failure or interrupt.
Nothing lands in your home or beside the plugin.

**Every stage is run through the launcher**, so there is no interpreter path
to remember:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" preflight --test-source
```

To avoid paying the install on every run, put the dependencies
(`snowflake-connector-python`, `cryptography`, `pyyaml`) into the interpreter
you use, and the launcher picks it up:

```bash
python3 -m pip install -r "${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt"
```

**Do not hand-roll a venv and do not pass `--break-system-packages`.** If no
Python 3.10+ is on `PATH`, the launcher says so and stops — that is a machine
to fix, not a step to improvise around.

## 2. Find the plugin, and the config — do not assume either

**Do not assume the user is inside this repo, or that any folder is open.** The
plugin may be installed rather than cloned, and the working directory may be
anywhere at all.

- **The engine is always at `${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py`**, and the
  in-AIDP scripts at `${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`. Build
  every path from `${CLAUDE_PLUGIN_ROOT}`, never from the user's current
  directory, and never ask them to `cd` anywhere.
- **If the engine or the scripts cannot be found, STOP and say so.** Never
  substitute your own SQL, your own API calls or your own translation for a
  stage that is missing. The engine is the method; there is no hand-made
  fallback.
- **The config is the OPERATOR's file, not the plugin's.** The CLI looks for it
  in this order — `--config <path>` → `./snowmig-config.yaml` in the working
  directory → the same name beside the plugin — and **every stage prints the
  file it used**. Repeat that line to the user; it is how they catch a run
  pointed at last month's environment.

If there is no config, create one where the user is working:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" init-config
```

That writes `./snowmig-config.yaml` from the template, mode `0600` on POSIX
(Windows has no mode bits), and refuses
to overwrite an existing one (that file holds live credentials). An installed
plugin's own directory may be read-only, which is exactly why the config
belongs in the working directory.

Then ask the user to fill it in and tell you when it is ready.

## 3. One file, both ends — what goes where

**Say this explicitly, because it is the question users actually ask.** There is
one file to fill in, and the secret goes *in that file*, not into the chat:

| What | Where | Note |
|---|---|---|
| Snowflake account/host, user, warehouse, database, role, schema | `snowmig-config.yaml`, under `snowflake:` | created from the template; `0600` on POSIX; gitignored only inside the plugin folder — tell the user to add it to their own `.gitignore` when it lives elsewhere |
| The Snowflake **password or private key** | the same file — `password:` or `private_key: |` inline | inline is the default; `*_path` variants exist but are not what you propose first |
| Which AIDP resources to use (DataLake OCID, workspace, cluster, catalog) | the same file, under `aidp:` | any of them can also be passed as a flag, and a flag wins |
| AIDP **authentication** | `~/.oci/config` (`oci setup config`) | never a value in the config file. `aidp.oci_profile`, when set, is announced on stdout and passed as `--profile` to every `oci` and `aidp` call; otherwise each CLI uses `OCI_CLI_PROFILE`, else `DEFAULT`. Both CLIs always get an explicit `--auth`: `aidp.oci_auth`, else `OCI_CLI_AUTH`, else `security_token` for a profile with a `security_token_file` and `api_key` for any other. Every `aidp` call also gets `--region <from the OCID>`. On an auth error, check that profile and the mode the run announced |

Rules that come with an inline secret, and they are not optional:

- **Ask the user before reading the config**, and say why you need it.
- **Never print, echo, quote or summarise a secret value** — not in chat, not in
  a report, not in a commit message. Render a config only through
  `migration_config.redact()`, which is what `preflight` uses.
- **Never ask the user to paste a password or a private key into the
  conversation.** They put it in the file, on their own machine. If one does end
  up in the chat or in a committed file, say so plainly and tell them to rotate
  it.
- The file is gitignored only inside the plugin folder; in the user's working
  directory nothing ignores it until they add it to that repo's `.gitignore`
  — say so when you create it. Keep it out of tickets and commits too — an inline
  secret is a secret that leaks the moment the file travels.

Key-pair setup, if the user wants one instead of a password:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out ./sf_key.p8
openssl rsa -in ./sf_key.p8 -pubout | grep -v '^-----' | tr -d '\n'
# then in Snowsight:  ALTER USER <user> SET RSA_PUBLIC_KEY='<that string>';
```

Then set `auth: keypair` and paste the PEM **into the same config file**, under
`private_key: |`. A key pair is also what AIDP's own Snowflake connector uses,
so it is not throwaway setup. (`key_path:` also works if they would rather keep
the PEM on disk — but do not send them to a second file by default.) `pat`
works for the engine but **not** for the EXTERNAL catalog registration, whose
Snowflake connection properties have no token field.

SSO (`authenticator: externalbrowser`) needs a SAML IdP configured on the
account; without one, the login fails with `390190`.

## 4. Read the config back, then actually connect

This step catches a wrong account, role or destination before anything runs:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" preflight --test-source
```

No `--config` needed once the file is in place — the CLI discovers it and prints
which one. `PREFLIGHT_CONFIG.md` lists every field with what it is for, secrets
masked, then the results of the live checks:

- `--test-source` opts into the Snowflake connection. It is opt-in because it
  resumes the warehouse; without it the source is reported *skipped*.
- The **AIDP end is checked automatically** whenever the config carries both
  `aidp.datalake_ocid` and `aidp.catalog` — it lists the catalogs and reports
  whether that one exists and whether it is `INTERNAL` or `EXTERNAL`. Read-only,
  so it needs no flag. Without those two fields it is reported *skipped*.

Walk the table with the user and ask them to confirm, especially:

- **`host`** — the *Account/Server URL* from the Snowflake console
  (`Account → Account/Server URL`), e.g. `ORG-ACCOUNT.snowflakecomputing.com`.
  Derived from `account` when absent, which is right for the org-account form;
  set it explicitly for the account-locator form or PrivateLink.
- **`role`** — what the migration can see is *exactly* this role's grants. A
  missing `role` means the user's default role, which is rarely what they meant.
- **`warehouse`** — must be resumable; a suspended one auto-resumes on the first
  query, so the first call can be slow.
- **`schema`** — any real schema. It only scopes the connector's pushdown
  session inside AIDP, and that option rejects `INFORMATION_SCHEMA`.
- **`datalake_ocid`** — the destination. Say it out loud and have the user
  confirm it is the right environment.

**A skipped check is not a pass.** If only one end was configured, say which end
was never tested rather than reporting "preflight OK".

## 5. Smoke-test the source

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" assess --database <one small database>
```

Report the account, region, role and warehouse back to the user. Then hand off
to [`snowflake-migrator-overview`](../snowflake-migrator-overview/SKILL.md) for
the stage sequence.
