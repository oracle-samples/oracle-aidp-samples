# Iceberg external table sync: AIDP to Oracle Autonomous Database

Serves an AIDP Delta lakehouse to N Autonomous Data Warehouses as **read-only Iceberg external
tables**. No data copy, no catalog service in the read path, and schema drift handled
automatically.

The sync is **incremental**: only tables whose schema changed are recreated. In steady state it
issues **zero DDL** against the ADWs (a run still performs a few lightweight reads per ADW — a
connectivity probe, `ALTER SESSION DISABLE PARALLEL DML`, and the registry lookups).

- **`ARCHITECTURE.md`** - design, diagrams, measured scale, test evidence and references. Read it
  if you are going to change the code or need to explain the solution.
- **`Architecture-EXT-TABLE-Sync.drawio.png`** - the component view, editable in draw.io.
- **This file** - onboarding, day-two operations and how to add an ADW.

---

## Is this the sample you want?

This repository carries the same AIDP -> ADW Iceberg pattern at two very different sizes. They
are complements, not alternatives: the other one teaches the mechanism, this one operates it.

| | [ADW External Table on Delta UniForm](../adw-ext-table-on-uniform/README.md) | **This sample** |
|---|---|---|
| Scope | one table you name | every eligible table in a catalog |
| Consumers | one ADW | a fleet of N ADWs, provisioned in parallel |
| Where the logic lives | a PL/SQL procedure inside the ADW | an AIDP notebook; the ADWs stay pure consumers |
| How it runs | by hand, one call per table | an AIDP job with a `CATALOG` parameter, on a schedule |
| What it does per run | always drops and recreates | fingerprints the Iceberg metadata and recreates **only** what drifted; steady state issues zero DDL |
| Credentials | placeholders edited into the SQL and the notebook | OCI Vault through the AIDP Credential Store; nothing in code or config |
| Consumer grants | lost on every recreate | recaptured before the drop and reapplied |
| State | none | per-catalog registry table in each ADW |
| Scale proven | a demo table | 4,777 tables x 2 ADWs, discovery in ~30s |

**Read the other one first if the pattern is new to you.** It is short, it shows the raw
`DBMS_CLOUD.CREATE_EXTERNAL_TABLE` call this sample generates, and its manual verification
section is the best tool available for debugging a credential or ACL problem - which is where
most first-time failures actually live, in either sample.

**Come here when the manual pattern stops scaling**: more tables than a person can track, more
than one warehouse to keep in sync, a schedule to meet, or an auditor asking where the passwords
are.

---

## Scope: Autonomous Database on OCI

This automation targets **Oracle Autonomous Database - ADW or ATP - on commercial OCI**. Other
Oracle Database variants or OCI realms may work but require code changes.

| Item | Impact outside ADB or commercial OCI |
|---|---|
| `DBMS_CLOUD.CREATE_EXTERNAL_TABLE` with Iceberg | this is the product mechanism itself; it does not exist on non-Autonomous Oracle |
| `GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR` | ADB's default directory. Required for both CREATING and READING the external table |
| `ALTER USER ... QUOTA UNLIMITED ON DATA` | `DATA` is ADB's tablespace name; ExaCS, Base DB and on-premises differ |
| `.oraclecloud.com` in the Object Storage endpoint | commercial realm. Gov and sovereign realms use another domain |
| mTLS wallet containing `ewallet.pem` | ADB wallet format |
| `ALTER SESSION DISABLE PARALLEL DML` | works around the parallel DML ADB enables by default, the cause of `ORA-12838` |

Changing only the tablespace or only the domain would not "port" this: the real dependency is
`DBMS_CLOUD`, which exists only on Autonomous. Outside it the answer is a different product, not
a configuration change.

---

## Prerequisites

**On the AIDP side**

- A catalog whose tables are written with Delta **UniForm**: `delta.columnMapping.mode = name`,
  `delta.enableIcebergCompatV2 = true`,
  `delta.universalFormat.enabledFormats = iceberg`.
- **`oracledb`, `oci` and `pyyaml` installed as cluster libraries** on the compute cluster the
  job runs on - see [Dependencies](#dependencies). `oracledb` is the one that is *not* present
  by default: without it cell 1 fails immediately with `ModuleNotFoundError: No module named
  'oracledb'`. A `%pip install` inside the notebook does not survive a scheduled job.
- Permission to create jobs and read the Credential Store.

**On the OCI side**

- An IAM user - the service account - with an API key. This single identity is used by the
  notebook to read Object Storage and by every ADW at query time.
- A Vault with a master encryption key.
- A private bucket for the wallets, if you use mTLS.

**On the ADW side**

- `ADMIN` credentials for each ADW in the fleet.
- The wallet, if mTLS is required, or a TLS connection string if it is not.

---

## Onboarding, step by step

Everything below is done once per environment, by hand, through the OCI Console and the AIDP
Workbench. After that, adding an ADW is four secrets and one line of YAML.

### Step 1 - Create the wallet bucket

Only needed if your ADWs require mTLS. In the OCI Console: **Object Storage -> Buckets ->
Create Bucket**, in the compartment where you keep this deployment.

| Field | Value |
|---|---|
| Name | `aidp-adw-wallets` |
| Public access | **No public access** |
| Versioning | Disabled |

Or by CLI:

```bash
oci os bucket create --compartment-id <COMPARTMENT_OCID> \
  --name aidp-adw-wallets --public-access-type NoPublicAccess --versioning Disabled
```

`No public access` is not a detail: **a wallet plus its password grants full database access.**
This bucket holds credentials, not data. Treat it accordingly, and consider encrypting it with
your own key.

Upload one wallet per ADW, in a folder named after the ADW prefix you will use:

```bash
oci os object put --bucket-name aidp-adw-wallets \
  --name demo_adw1/Wallet_adw1.zip --file ./Wallet_adw1.zip
```

Note the namespace of your tenancy, you will need it: `oci os ns get`.

### Step 2 - Grant the service account access to the bucket

The bucket is read by the **IAM user whose API key is in the Vault** - the service account - not
by the AIDP service principal. They are different subjects and need different statements.

In **Identity -> Policies**, in the bucket compartment:

```
allow group <SERVICE_ACCOUNT_GROUP> to read objects in compartment id <COMPARTMENT_OCID> where target.bucket.name = 'aidp-adw-wallets'
```

`where target.bucket.name` limits the grant to this bucket alone, so the service account gains
no access anywhere else.

If you prefer to scope to the user instead of a group - note that `user` is **not** a valid
policy subject, so it has to be expressed as a condition:

```
allow any-user to read objects in compartment id <COMPARTMENT_OCID> where all { request.user.id = '<USER_OCID>', target.bucket.name = 'aidp-adw-wallets' }
```

### Step 3 - Create the secrets in OCI Vault

You need a Vault with a master encryption key. In **Identity & Security -> Vault -> your vault
-> Secrets -> Create Secret**, create one secret per value below.

**One secret holds one value. Never a JSON document with several fields.** The AIDP masking
layer redacts exactly the string that `secrets.get()` returned, so a pure value shows up as
`[REDACTED]` if it is ever printed by accident. A value parsed out of a JSON blob would leak
into the notebook output in clear.

**Service account secrets** - four, sharing a prefix of your choice. `demo_oci` is used throughout
this documentation:

| Secret name | Contents | Where to get it |
|---|---|---|
| `demo_oci_user_id` | OCID of the IAM user | Identity -> Users -> the user, "OCID" field |
| `demo_oci_tenancy_id` | OCID of the tenancy | Profile menu -> Tenancy, "OCID" field |
| `demo_oci_fingerprint` | API key fingerprint | Identity -> Users -> the user -> API Keys |
| `demo_oci_privkey` | the private key contents | the `.pem` file you downloaded when creating the API key |

For `demo_oci_privkey`, two accepted shapes:

- the **base64 body only**, on a single line, headers stripped - assumed to be PKCS#1;
- the **full PEM including headers** - used verbatim, which covers PKCS#8
  (`-----BEGIN PRIVATE KEY-----`), the format of many OCI Console generated keys.

If your key is PKCS#8, store it **with the headers**. Without them it would be wrapped in the
wrong header and the client fails with an invalid-key error that is hard to trace back.

**Per-ADW secrets** - four for each ADW, sharing a prefix. One prefix per ADW; it also becomes
that ADW's name in every log line, so pick something recognisable:

| Secret name | Contents | Where to get it |
|---|---|---|
| `demo_adw1_dsn` | the full connection descriptor | ADW Console -> Database connection -> Connection strings; pick a service such as `_tpurgent` |
| `demo_adw1_wallet_zip` | `oci://aidp-adw-wallets@<namespace>/demo_adw1/Wallet_adw1.zip` | the URI of the object you uploaded in Step 1. An AIDP Volume path also works |
| `demo_adw1_pwd` | password of the ADW administrative user | whoever provisioned the ADW |
| `demo_adw1_wallet_pwd` | password set when the wallet was downloaded | whoever downloaded the wallet |

Repeat for `demo_adw2`, `demo_adw3` and so on.

At two ADWs that is **12 secrets**: 4 for the service account plus 4 per ADW.

**What must NOT become a secret.** The masking layer redacts any exact occurrence of a value
that passed through `secrets.get()`. Storing short, common values poisons the entire log: with
`ADMIN` in the vault, `SELECT user FROM dual`, error messages and `all_users` listings all print
as `[REDACTED]` and the notebook becomes impossible to debug. So the ADW administrative user
name lives in `adw_sync.yaml` under `adw_user`, and the ADW display name is derived from the
prefix. Rule of thumb: **a short, common value, or one that appears in logs, does not belong in
the vault.**

The wallet **files** do not belong there either - they exceed the 25 KB secret limit. Only the
path and the password go to the Vault.

### Step 4 - Register the secrets in the AIDP Credential Store

For **each** secret created in Step 3, in AIDP Workbench: **Credential Store -> Create ->
Credentials**.

| Field | Value |
|---|---|
| Name | **exactly the secret name**, e.g. `demo_adw1_pwd` |
| Credential type | **Vault Reference** |
| Vault OCID | the OCID of the vault holding the secrets, the same for all of them |

The name must match the secret name, because the notebook derives every credential name from the
prefixes in `adw_sync.yaml`.

This is a **one-off cost**: a Vault Reference stores the secret OCID and always reads the
`CURRENT` version, so rotating a value in the Vault never requires touching the Credential Store
again.

### Step 5 - Grant AIDP access to the secrets. Nothing works without this.

In **Identity -> Policies**, in the compartment holding the secrets:

```
allow any-user to use secrets         in compartment id <COMPARTMENT_OCID> where all { request.principal.type = 'aidataplatform' }
allow any-user to read secret-bundles in compartment id <COMPARTMENT_OCID> where all { request.principal.type = 'aidataplatform' }
```

Three traps that cost real time:

- **`secret` singular is not a valid resource-type** and returns `Invalid parameter`. The valid
  ones are plural: `secrets`, `secret-bundles`, `secret-versions`, `secret-family`.
- **`in tenancy` is only valid in a policy created in the root compartment.** Anywhere else use
  `in compartment id <ocid>`, or you get
  `Compartment ... does not exist or is not part of the policy compartment subtree`.
- **Do not add a condition on `target.resource.tag.orcl-aidp.governingAidpId`.** That system tag
  is applied to resources AIDP creates, not to secrets it merely references, so the predicate
  never matches and the read fails. Consequence worth raising with a security team: today the
  grant can only be scoped by compartment, so every AIDP instance in that compartment can read
  the secrets.

Symptom of a missing policy: cell 1 fails with `404 NotAuthorizedOrNotFound`. Note the same 404
appears for a **non-existent** credential, so also check that the name matches the prefix in the
YAML exactly.

### Step 6 - Create `adw_sync.yaml`

This folder ships only the commented template. Copy it to the name the notebook looks for:

```bash
cp adw_sync.sample.yaml adw_sync.yaml
```

**The name matters** - `adw_sync.yaml` is the distinctive name the notebook resolves on its own
under `/Workspace`. Keep the template untouched as a reference.

Then point `oci_credential_prefix` and `adw_prefixes` at the prefixes you chose, and set `region`.
Nothing else is required; the banner in cell 1 lists which absent keys fell back to defaults.

The YAML holds **no secret** - only prefixes, region and knobs. Version it freely.

Objects created in ADW are named `<catalog>_<schema>.<table_prefix><table>`. With
`CATALOG=sales`, the AIDP table `analytics.orders` becomes `SALES_ANALYTICS.ORDERS`.

### Step 7 - Upload the folder and create the job

Upload `adw_external_table_sync.ipynb` and `adw_sync.yaml` into the same workspace folder, then
create a job pointing at the notebook:

| Job parameter | Required | Purpose |
|---|---|---|
| `CATALOG` | yes | the AIDP catalog to sync |
| `CONFIG_PATH` | no | path to the YAML; without it the notebook finds `adw_sync.yaml` on its own |

Both are accepted in upper or lower case, since parameter names are case-sensitive on the
platform.

One job per catalog, all pointing at the same notebook and the same YAML.

### Step 8 - First run: read the dry run

Run sections 1 through 6 and **stop at the dry run**. It reports create / recreate / skip / drop
per ADW with examples. Then run section 7 to apply.

Sections 3.5 (teardown) and 8 (validate) ship **disabled**: their code sits inside a triple-quoted
string. Section 8 is read-only, so removing the surrounding `'''` is safe and is the recommended
way to confirm the objects are queryable. Section 3.5 is destructive - read it before enabling.

On a first run everything is `CREATE`. After a naming change, expect `DROP` of the old names plus
`CREATE` of the new ones - a full re-registration, not data loss.

## Where the notebook looks for the configuration

The path is **not fixed**: install the folder anywhere under `/Workspace`. Interactively the
working directory is the notebook folder, but in a **scheduled job** it is the session home -
`/home/lcu-...` - so a relative path never resolves. The search covers both:

| Order | How | Resolves when |
|---|---|---|
| 1 | `CONFIG_PATH` job parameter | always; wins over the rest |
| 2 | `adw_sync.yaml`, `config.yaml` or `config.yml` in the working directory | interactive run |
| 3 | a config next to `adw_external_table_sync.ipynb` under `/Workspace`, up to 10 levels | scheduled job |
| 4 | a single `adw_sync.yaml` under `/Workspace` | scheduled job, even if the notebook was renamed |

The banner prints **how** it was found, which avoids guesswork in a job log:

```
Config      : /Workspace/<your-folder>/adw_sync.yaml   (distinctive name adw_sync.yaml)
```

Two situations where it **stops instead of choosing**, both deliberately:

- **Notebook renamed and the config called `config.yaml`.** With no anchor and a generic name
  there is no way to tell it apart from another project's `config.yaml` - a real workspace holds
  several. Keep the name `adw_sync.yaml`, or set `CONFIG_PATH`.
- **More than one deployment in the same workspace.** It lists the candidates and asks for
  `CONFIG_PATH`. That case genuinely is ambiguous.

---

## Adding a new ADW

Four secrets, four credentials, one line of YAML. No code change.

**1. Choose a prefix.** Say `demo_adw3`. It becomes the ADW name in every log line, so pick
something you will recognise.

**2. Upload the wallet** into the bucket, in a folder matching the prefix:

```bash
oci os object put --bucket-name aidp-adw-wallets \
  --name demo_adw3/Wallet_adw3.zip --file ./Wallet_adw3.zip
```

**3. Create four secrets** in the Vault, following the same pattern as the existing ones:

| Secret | Contents |
|---|---|
| `demo_adw3_dsn` | the connection descriptor from the ADW Console |
| `demo_adw3_wallet_zip` | `oci://aidp-adw-wallets@<namespace>/demo_adw3/Wallet_adw3.zip` |
| `demo_adw3_pwd` | password of the administrative user |
| `demo_adw3_wallet_pwd` | password of the wallet |

**4. Register the four in the Credential Store** as **Vault Reference**, each named exactly like
its secret, all pointing at the same vault OCID.

**5. Add one line to `adw_sync.yaml`:**

```yaml
adw_prefixes:
  - demo_adw1
  - demo_adw2
  - demo_adw3      # new
```

**6. Run the job.** The new ADW starts empty, so everything is `CREATE` there while the existing
ones report `SKIP`. Nothing else to do.

Two things worth checking as the fleet grows:

- **Connections.** `min(fleet, adw_workers_cap) x workers`. With the defaults that is 4 x 8 = 32,
  and it does not grow with fleet size - `adw_workers_cap` is a deliberate cap.
- **`adw_user`.** If the new ADW uses a different administrative user, `adw_user` accepts a list
  with one value per ADW in the order of `adw_prefixes`. The count is validated.

### Removing an ADW

Delete the line from `adw_prefixes`. The notebook stops touching it; existing external tables keep
working until you drop them. To clean up, point `CATALOG` at it and run the teardown cell before
removing the line.

## Day-two operations

### Rotating a secret

Create a new version of the secret in OCI Vault. Nothing else. The AIDP credential stores the
**secret OCID** and always reads the `CURRENT` version, so no code, YAML or Credential Store
change is needed. Verified: the next read in the same session already returns the new value.

Distinguish the two rotations a security team may mean:

| | What changes | Impact |
|---|---|---|
| Master encryption key rotation | a new key version; the secret value is unchanged | none, invisible |
| Secret value rotation | a new secret version with a different value | picked up on the next run |

Vault **auto-rotation** is not useful here: `rotationConfig.target_system_details` is required and
accepts only `ADB` or `FUNCTION`. The target is the system that OWNS the credential, never the
AIDP, which is a reader. Rotate externally.

### Handling a misaligned table

Iceberg keeps a `current-schema-id` on the table and a `schema-id` on every snapshot. After a
**metadata-only** change - adding, dropping or renaming a column under column mapping - the
current schema advances, but the tip snapshot still references the schema that was current when
it was written.

Consumers that resolve the table shape **through the snapshot** rather than through
`current-schema-id` will therefore keep seeing the previous set of columns, even though the
external table was recreated correctly. Discovery reports these as `MISALIGNED` so the condition
is visible instead of silent.

Any real data commit on the table realigns the two. Three options:

1. **Do nothing.** The next normal ETL write clears it. This is the right choice most of the time.
2. **Land a commit yourself** on the affected table.
3. **Set `flags.force_snapshot: true`.** The job writes one dummy row and deletes it, only on the
   misaligned tables, then polls until the metadata regenerates.

**Option 3 is off by default and should stay off unless the new shape must be visible
immediately.** It writes to the source lakehouse, and each table then requires waiting for the
asynchronous Iceberg metadata regeneration - several seconds per table. On a run with many
misaligned tables that wait dominates the total execution time, turning a sync of seconds into
one of minutes. Enable it deliberately, for a specific run, rather than leaving it on.

Do **not** reach for `OPTIMIZE` instead: it can take hours and may not produce a new snapshot at
all.

### Reading the summary

```
demo_adw1: {'create': 12, 'recreate': 3, 'skip': 4762, 'drop': 0, 'ok': 15, 'err': 0, 'grants': 8}
```

`skip` dominating is the healthy steady state. `err > 0` prints the first twenty errors with the
real Oracle cause, which is often on the second line of the message.

### Teardown

Cell 3.5, disabled by default and dry-run by default. Scope comes from `CATALOG`, the current
`PLAN` and the registry filtered by catalog - never a hardcoded list. If an owner is shared with
another catalog the user is **not** dropped; only this catalog's objects are.

---

## Multiple catalogs against the same fleet

One job per catalog, several pointing at the same ADWs. **Run them in sequence, not in
parallel** - an operational recommendation, not a code lock. Reasons and the two in-code
protections are in `ARCHITECTURE.md`, section 7.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `404 NotAuthorizedOrNotFound` on a credential | missing IAM policy, or a name that does not match the YAML prefix. The same 404 covers both |
| `Configuration file not found` | scheduled job with the config not next to the notebook. Set `CONFIG_PATH` |
| `CATALOG not provided` | the job parameter is missing or spelled differently. `CATALOG` and `catalog` both work |
| `CROSS-CATALOG COLLISION` | another catalog already owns objects this job would create. Check the schema prefix |
| `ORA-00942` on a consumer query | the query started inside the ~0.8s drop-and-recreate window. Schedule syncs off-peak |
| a consumer does not see a newly added column | misaligned metadata: the tip snapshot references an older schema. See "Handling a misaligned table" |
| `ORA-01017` intermittently | two jobs with work in the same schema rotating the password under each other. Run catalogs sequentially |
| `ORA-06564: DATA_PUMP_DIR` | the `GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR` was removed. It is required for reads too |
| `ORA-12838` | `ALTER SESSION DISABLE PARALLEL DML` did not run. Check `_prep` |
| Everything prints as `[REDACTED]` | a short, common value was stored in the Vault. Never put `ADMIN` or similar there |

---

## Configuration reference

Full commented template in `adw_sync.sample.yaml`.

| Key | Default | Purpose |
|---|---|---|
| `region` | **required** | Object Storage / lakehouse region, not the ADW's |
| `oci_credential_prefix` | **required** | prefix of the API key credentials |
| `adw_prefixes` | **required** | one prefix per ADW |
| `adw_user` | `ADMIN` | one value for the fleet, or one per ADW |
| `naming.table_prefix` | empty | optional prefix on the ADW table name |
| `naming.cred_name` | `OCI_CRED_<CATALOG>` | credential name inside the ADW |
| `naming.raw_suffix` | `__RAW` | suffix of the raw table when `create_views` is on |
| `flags.create_views` | `false` | `true` = `<T>__RAW` plus view `<T>` |
| `flags.preserve_grants` | `true` | recapture and reapply grants on recreate |
| `flags.force_snapshot` | `false` | realign misaligned tables. Writes to the source and adds significant run time; leave off unless needed |
| `flags.retry_failed` | `true` | one retry before writing the registry |
| `flags.bulk_discovery` | `true` | bulk listing through the OCI SDK |
| `parallelism.workers` | `8` | connections per ADW |
| `parallelism.adw_workers_cap` | `4` | cap on ADWs in parallel |
| `parallelism.intra_schema_workers` | `1` | slices per schema; raise when few schemas hold many tables |
| `parallelism.read_workers` | `32` | parallel `metadata.json` reads |
| `discovery.list_page` | `1000` | objects per listing request; also the API maximum |
| `discovery.fallback_max_per_schema` | `20` | cap on individual `DESCRIBE` calls per schema |
| `discovery.exclude_schemas` | see sample | schemas never synced |
| `registry_table` | `ADMIN.EXT_REGISTRY_V4` | sync state, catalog-scoped |
| `acl_privileges` | `[connect]` | network ACL privileges |
| `vault_key` | `VaultSecretReference` | fixed literal for a Vault Reference |
| `catalog` | `null` | interactive-testing fallback only; the banner warns when it is used |

**`naming.schema_prefix` is not configurable.** It is always `<catalog>_`. A YAML that still
carries the key is rejected rather than silently ignored. Reasoning in `ARCHITECTURE.md`.

---

## Contents

| Path | What it is |
|---|---|
| `adw_external_table_sync.ipynb` | the notebook |
| `adw_sync.sample.yaml` | the commented template. Copy it to `adw_sync.yaml` - see Step 6 |
| `ARCHITECTURE.md` | design, diagrams, measured scale, test evidence, references |
| `Architecture-EXT-TABLE-Sync.drawio.png` | component diagram, editable in draw.io |
| `requirements.txt` | `oracledb`, `oci`, `pyyaml` - install as cluster libraries |
| `README.md` | this file |

## Dependencies

`oracledb`, `oci` and `pyyaml`, listed in `requirements.txt`.

**Install them as cluster libraries**, on the compute cluster attached to the notebook and to the
job. In AIDP Workbench: **Compute -> your cluster -> Libraries -> Install new -> PyPI**, one entry
per package, then restart the cluster.

`oci` and `pyyaml` are usually already present; **`oracledb` is not**, and it is the whole database
driver - cell 1 fails with `ModuleNotFoundError: No module named 'oracledb'` without it.

Cell 1 also carries a `%pip install` line, commented out. It is fine for a quick interactive test,
but it installs into the session only: a **scheduled job runs in a fresh session and will fail**.
For anything you schedule, the cluster library is the only option.
