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
| Credentials | placeholders edited into the SQL and the notebook | the AIDP Credential Store: the service account as a native Service account credential, the per-ADW secrets as Vault References to OCI Vault. Nothing in code or config |
| Consumer grants | lost on every recreate | recaptured before the drop and reapplied |
| State | none | per-catalog registry and credential-state tables in each ADW |
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
- Permission to create jobs, and to create and read Credential Store entries.

**On the OCI side**

- An IAM user - the service account - with an API key. This single identity is used by the
  notebook to read Object Storage and by every ADW at query time. Its key is held in the AIDP
  Credential Store, not in OCI Vault.
- A Vault with a master encryption key, for the per-ADW secrets. Needed in **both** connection
  modes.
- A private bucket for the wallets - **mTLS only**.

**On the ADW side**

- `ADMIN` credentials for each ADW in the fleet, or a dedicated user - see
  [Using a dedicated user instead of ADMIN](#using-a-dedicated-user-instead-of-admin).
- Either the **wallet** (mTLS, the default), or a **private endpoint** with mutual TLS set to not
  required (walletless TLS). See [Connection mode](#connection-mode-mtls-or-walletless-tls).

---

## Credentials, policies and connection mode

Two decisions shape every onboarding step: where each credential lives, and whether the fleet
connects with a wallet. Both are settled here, so the steps below only have to say which mode
they apply to.

### Where each credential lives - two different places

This deployment reads from **two** stores, and they are not interchangeable:

| What | Where it lives | Credential type | Needed | Kept current by |
|---|---|---|---|---|
| OCI service account - User OCID, Tenancy OCID, fingerprint, private key | **inside AIDP**, in the Credential Store | **Service account** | always | updated in place, ideally by an **OCI Function** on the rotation event |
| Per-ADW `_dsn` and `_pwd` | OCI Vault, referenced from AIDP | **Vault Reference** | always | whatever syncs your Vault, for example Terraform |
| Per-ADW `_wallet_zip` and `_wallet_pwd` | OCI Vault, referenced from AIDP | **Vault Reference** | mTLS only | same as above |
| The wallet files themselves | the wallet bucket, or an AIDP Volume | not a credential - an object | mTLS only | re-uploaded when the wallet is rotated |

The service account entry is **not** a Vault Reference. Its values are stored in the Credential
Store itself, so there is no Vault secret behind it and no `read secret-bundles` policy is needed
for it. The consequence is that a Vault sync will **not** update it - whatever rotates the API key
has to write the new values into this credential as well. See
[Rotating the service account API key](#rotating-the-service-account-api-key).

The per-ADW entries are the opposite case: they are Vault References, so the Vault stays the
source of truth and AIDP always reads the `CURRENT` version. Nothing to do in AIDP on rotation.

### Connection mode: mTLS or walletless TLS

`flags.use_wallet` picks the mode for the **whole fleet**. It is a global switch and not inferred
from whether a wallet secret happens to exist - a missing secret is a mistake worth seeing, not an
instruction to silently change how the job connects.

| | mTLS - `use_wallet: true` (default) | Walletless TLS - `use_wallet: false` |
|---|---|---|
| ADB "Mutual TLS authentication" | **required** - the ADB default | **not required** |
| ADB network access | public or private endpoint | **private endpoint** - see below |
| `<prefix>_dsn` holds | the mTLS connection descriptor (port 1522) | the TLS connection string (port 1521) |
| Wallet bucket - [Step 1](#step-1---create-the-wallet-bucket-mtls-only) | yes, or an AIDP Volume | no |
| Bucket policy for the service account - [Step 2](#step-2---grant-the-service-account-access-to-the-wallet-bucket-mtls-only) | yes, when `_wallet_zip` is an `oci://` URI | no |
| Service account credential - [Step 3](#step-3---create-the-service-account-credential-in-aidp) | yes | yes |
| Vault secrets per ADW - [Step 4](#step-4---create-the-per-adw-secrets-in-oci-vault) | 4: `_dsn`, `_pwd`, `_wallet_zip`, `_wallet_pwd` | 2: `_dsn`, `_pwd` |
| Vault References per ADW - [Step 5](#step-5---register-the-secrets-as-vault-references) | 4 | 2 |
| Secrets policy for AIDP - [Step 6](#step-6---grant-aidp-access-to-the-secrets-nothing-works-without-this) | yes | **yes** - `_dsn` and `_pwd` are still Vault References |
| Cell 3 | downloads each wallet into a temporary directory unique to the run, removed at the end | skips the wallets; only the connectivity probe runs |
| What a caller needs besides the password | the wallet | a network path to the private endpoint |

Two things do **not** depend on the mode. The Vault References and their IAM policy stay, because
walletless removes two secrets per ADW, not the Vault. And the service account stays: the ADWs
read Object Storage with a `DBMS_CLOUD` credential built from its API key, which has nothing to do
with how the job connects to the database.

What that adds up to, counting both stores separately:

| | Vault secrets | Credential Store entries |
|---|---|---|
| Service account, either mode | **0** - it is a native Service account credential | 1 |
| Per ADW, walletless | 2 | 2 Vault References |
| Per ADW, mTLS | 4 | 4 Vault References |
| **Two-ADW fleet, walletless** | **4** | **5** |
| **Two-ADW fleet, mTLS** | **8** | **9** |

#### Walletless from AIDP needs a private endpoint

On a **public** endpoint, OCI only lets you set mutual TLS to not required once a network ACL is
in place - and an active ACL refused connections from AIDP in every configuration tried here: the
client address the database reports for the AIDP session, AIDP's egress IP measured from a
notebook, and all three RFC 1918 ranges together. Each attempt failed with `ORA-12529`. AIDP
compute has no VCN, subnet or NSG of yours, so an ACL entry by VCN OCID is not available either.

So on a public endpoint, keep the wallet. Walletless TLS is for ADBs on a **private endpoint**,
where no ACL is involved and setting mutual TLS to not required is a single change. AIDP still has
to reach that private endpoint, which is a networking prerequisite of its own.

A corollary worth remembering: `ORA-12529`, or `DPY-6000: Listener refused connection`, from AIDP
points at the ACL - not at TLS, the wallet or the password.

### IAM policies, by mode

Two subjects, and they need different statements:

| Grant | Principal | mTLS | Walletless | What it is for |
|---|---|---|---|---|
| `use secrets` + `read secret-bundles`, in the secrets' compartment - [Step 6](#step-6---grant-aidp-access-to-the-secrets-nothing-works-without-this) | the **AIDP service** (`aidataplatform`) | yes | yes | resolving the per-ADW Vault References: `_dsn` and `_pwd` always, the two wallet secrets with mTLS |
| `read objects` on the wallet bucket - [Step 2](#step-2---grant-the-service-account-access-to-the-wallet-bucket-mtls-only) | the **service account** | yes, with an `oci://` wallet path | no | cell 3 downloads each wallet with the service account's API key |
| read access to the lakehouse bucket | the **service account** | yes | yes | discovery from the notebook, and every ADW at query time. Mode-independent, and specific to how your lakehouse is laid out, so not spelled out here |
| anything for the Service account credential itself | - | no | no | it is a native credential: AIDP's own RBAC guards it, not IAM |

### What holding the service account natively buys: Run As becomes usable

A Vault Reference needs an IAM policy - `read secret-bundles` in the secret's compartment, for the
AIDP service principal. That grant can only be scoped by compartment, so every AIDP instance in
that compartment satisfies it. A native Service account credential needs **no IAM policy at all**,
because the values never sit in Vault. What guards it is AIDP's own RBAC instead, which is per
credential and per identity.

That is what opens up [**Run As**](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/run-identity.html)
for a scheduled job. Run As sets the execution identity explicitly - your own, or a service
account - and only service accounts **stored in the Credential Store** can be selected. With the
service account held there, a job can run as a non-person identity whose authority over OCI is a
permission on that one credential entry, rather than a compartment-wide IAM grant.

Selecting it still takes a grant: whoever configures the job needs at least **USE** permission on
the service account's credential. This deployment does not depend on Run As - it is optional -
and we have not exercised it here. The point is that the credential no longer stands in the way of
using it.

---

## Onboarding, step by step

Everything below is done once per environment, by hand, through the OCI Console and the AIDP
Workbench. **Steps 1 and 2 apply only with mTLS**; with `flags.use_wallet: false`, start at
Step 3. After that, adding an ADW is two secrets and one line of YAML - four secrets with mTLS.

### Step 1 - Create the wallet bucket (mTLS only)

**Skip this step entirely if you set `flags.use_wallet: false`** - see
[Connection mode](#connection-mode-mtls-or-walletless-tls). Only needed if your ADWs require
mTLS. In the OCI Console: **Object Storage -> Buckets -> Create Bucket**, in the compartment where
you keep this deployment.

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

An AIDP Volume path also works for `_wallet_zip`, and then neither this bucket nor Step 2 is
needed. The bucket is preferred because uploading a wallet becomes a CLI or Terraform call, and
several AIDP instances can share one fleet.

### Step 2 - Grant the service account access to the wallet bucket (mTLS only)

**Skip this step with `flags.use_wallet: false`, or when the wallets live in an AIDP Volume.**

The bucket is read by the **service account** - the IAM user whose API key you register in
Step 3 - not by the AIDP service principal. Cell 3 downloads each wallet with that API key. They
are different subjects and need different statements.

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

### Step 3 - Create the service account credential in AIDP

**Both modes.** The service account comes from **one** credential of type **Service account** in
the Credential Store. That type carries the whole identity, so it is the only entry this notebook
needs for OCI, and nothing about it goes into OCI Vault.

Create it in AIDP Workbench under **Credential Store -> Create -> Credentials**, pick
**Service account** as the type, and fill the fields. The notebook reads them by the type's fixed
field names:

| Field in the form | Read as |
|---|---|
| User OCID | `key="userId"` |
| Tenancy OCID | `key="tenancyId"` |
| Fingerprint | `key="fingerprint"` |
| Private key | `key="privateKey"` |

Then put its name in `adw_sync.yaml` (Step 7):

```yaml
oci_credential_service_account: <exact credential name, as registered>
```

The name is used **verbatim**. Unlike `adw_prefixes`, which has suffixes appended to build the
real names, this value *is* the credential name - nothing is prefixed, suffixed or derived from it,
so a name emitted by external provisioning goes in unchanged.

`Region` is also a field on the form; the notebook ignores it and uses `region` from the
configuration, because that value is the Object Storage region rather than any ADW's.

**The private key, in any of its usual shapes.** Full PEM with headers - PKCS#1
(`-----BEGIN RSA PRIVATE KEY-----`) or PKCS#8 (`-----BEGIN PRIVATE KEY-----`) - or the bare base64
body of a PKCS#1 key all work. A bare PKCS#8 body does not: a value without a header is read as
PKCS#1, so store PKCS#8 keys with their headers. The notebook normalises the key for
each consumer, because the two want different shapes. The Object Storage client takes a PEM;
`DBMS_CLOUD.CREATE_CREDENTIAL` wants the **bare base64 body of a PKCS#1 key**, and a PKCS#8 body is
accepted at creation and then signs incorrectly - the failure surfaces much later as `ORA-20401`
on a read, or as `ORA-20000: Failed to generate column list` out of `CREATE_EXTERNAL_TABLE`.

If the field name and the credential type disagree, `secrets.get` returns an **empty string**
rather than an error - the notebook turns that into a message naming the credential and the field.

Nothing read from the Credential Store is printed. The banner reports names and counts only, and
the messages the notebook prints or raises are filtered so a value that came from the store is
replaced with `[REDACTED]`, including when it arrives inside a driver or SDK error.

### Step 4 - Create the per-ADW secrets in OCI Vault

**Both modes** - walletless needs two per ADW, mTLS four. You need a Vault with a master
encryption key. In **Identity & Security -> Vault -> your vault -> Secrets -> Create Secret**,
create one secret per value below.

**One secret holds one value. Never a JSON document with several fields.**

The secrets share a prefix per ADW. One prefix per ADW; it also becomes that ADW's name in every
log line, so pick something recognisable:

| Secret name | Mode | Contents | Where to get it |
|---|---|---|---|
| `demo_adw1_dsn` | both | the connection string - **which one depends on the mode**, see below | ADW Console -> Database connection -> Connection strings; pick a service such as `_tpurgent` |
| `demo_adw1_pwd` | both | password of the ADW administrative user | whoever provisioned the ADW |
| `demo_adw1_wallet_zip` | mTLS only | `oci://aidp-adw-wallets@<namespace>/demo_adw1/Wallet_adw1.zip` | the URI of the object you uploaded in Step 1. An AIDP Volume path also works |
| `demo_adw1_wallet_pwd` | mTLS only | password set when the wallet was downloaded | whoever downloaded the wallet |

**`_dsn` is the one secret whose content changes with the mode.** With mTLS it holds the mTLS
descriptor (port 1522), which only works together with the wallet. Walletless, it holds the
**TLS** connection string (port 1521) - in the console, switch the TLS authentication selector to
*TLS* before copying. An mTLS descriptor used without the wallet fails at the connectivity probe
in cell 3, and that probe is the first place the mismatch shows.

The two `wallet_*` secrets are **only read when `flags.use_wallet` is true**. With walletless
TLS, do not create them.

Repeat for `demo_adw2`, `demo_adw3` and so on. The totals per mode are in
[Connection mode](#connection-mode-mtls-or-walletless-tls).

**What must NOT become a secret.** The ADW administrative user name is not a secret: it lives
in `adw_sync.yaml` under `adw_user`, and the ADW display name is derived from the prefix. Rule of
thumb: **a short, common value, or one that appears in logs, does not belong in the vault.**

The wallet **files** do not belong there either - they exceed the 25 KB secret limit. Only the
path and the password go to the Vault.

### Step 5 - Register the secrets as Vault References

**Both modes.** For **each** secret created in Step 4, in AIDP Workbench: **Credential Store ->
Create -> Credentials**.

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

The Service account credential from Step 3 is the only entry in this deployment that is **not** a
Vault Reference.

### Step 6 - Grant AIDP access to the secrets. Nothing works without this.

**Both modes.** Walletless removes two secrets per ADW, not the Vault: `_dsn` and `_pwd` are still
Vault References, so this policy is needed with and without the wallet. It is **not** needed for
the service account, which is a native credential.

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
- **Scoping the grant to a single AIDP instance.** A condition on the generic defined-tag form
  `target.resource.tag.orcl-aidp.governingAidpId` was tried here and never matches, so the read
  fails. The Credential Store documentation prescribes a different predicate,
  `request.principal.id = target.secret.system-tag.orcl-aidp.governingAidpId` - a **secret
  system-tag** - which has not been exercised here. Testing it is low-risk: if the tag is absent
  the policy fails closed. Without such a condition the grant is scoped only by compartment, so
  every AIDP instance in that compartment can read these secrets - the argument for keeping them
  in a compartment of their own.

Symptom of a missing policy: cell 1 fails with `404 NotAuthorizedOrNotFound`. Note the same 404
appears for a **non-existent** credential, so also check that the name matches the prefix in the
YAML exactly.

### Step 7 - Create `adw_sync.yaml`

This folder ships one configuration file, and it is **not** the one the notebook reads:

| File | Role |
|---|---|
| `adw_sync.example.yaml` | the commented template. Every key documented, with the trade-offs |
| `adw_sync.yaml` | your deployment copy - the file the notebook reads. Not shipped; listed in this folder's `.gitignore` |

Copy the template and edit the copy:

```
cp adw_sync.example.yaml adw_sync.yaml
```

Replace the `CHANGE_ME_` values and the `demo_` prefixes; the comments beside each key explain the
alternatives.

**The name matters** - `adw_sync.yaml` is the distinctive name the notebook resolves on its own
under `/Workspace`, which is exactly why the repository does not ship a file with that name: a
committed copy would collide with a real deployment's configuration (the notebook stops when it
finds more than one), or, found alone, would run with its `CHANGE_ME_` values.

Then set `oci_credential_service_account` to the credential name from Step 3, point
`adw_prefixes` at the prefixes you chose, and set `region`. Set **`flags.use_wallet`** to the mode
you chose: the template keeps the default `true` (mTLS); set `false` for walletless TLS.
Nothing else is required; the banner in cell 1 lists which absent keys fell back to defaults.

The YAML holds **no secret** - only prefixes, region and knobs. Version it freely in your own
deployment repository.

Objects created in ADW are named `<catalog>_<schema>.<table_prefix><table>`. With
`CATALOG=sales`, the AIDP table `analytics.orders` becomes `SALES_ANALYTICS.ORDERS`.

### Step 8 - Upload the folder and create the job

Upload `adw_external_table_sync.ipynb` and `adw_sync.yaml` into the same workspace folder, then
create a job pointing at the notebook:

| Job parameter | Required | Purpose |
|---|---|---|
| `CATALOG` | yes | the AIDP catalog to sync |
| `CONFIG_PATH` | no | path to the YAML; without it the notebook finds `adw_sync.yaml` on its own |

Both are accepted in upper or lower case, since parameter names are case-sensitive on the
platform.

One job per catalog, all pointing at the same notebook and the same YAML.

### Step 9 - First run: read the dry run

Run sections 1 through 6 and **stop at the dry run**. It reports create / recreate / skip / drop
per ADW with examples. Then run section 7 to apply.

The end of section 3 is the first checkpoint: it connects once to every ADW, and is where a wrong
mode shows up - an mTLS descriptor with `use_wallet: false`, a wallet that does not match its
DSN, or an ACL in the way.

Sections 3.5 (teardown) and 8 (validate) ship **disabled**: their code sits inside a triple-quoted
string. Section 8 is read-only, so removing the surrounding `'''` is safe and is the recommended
way to confirm the objects are queryable. Section 3.5 is destructive - read it before enabling.

On a first run everything is `CREATE`, and the summary's `creds` counts every schema, since each
one gets its `DBMS_CLOUD` credential installed and recorded. After a naming change, expect `DROP`
of the old names plus `CREATE` of the new ones - a full re-registration, not data loss.

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

Two secrets, two credentials, one line of YAML. No code change. (Four and four if
`flags.use_wallet` is true - see [Connection mode](#connection-mode-mtls-or-walletless-tls).)

**Before you start, match the fleet's mode.** `flags.use_wallet` is fleet-wide, so the new ADB
has to fit it: with mTLS, mutual TLS left required and a wallet; walletless, a private endpoint
with mutual TLS set to not required.

**1. Choose a prefix.** Say `demo_adw3`. It becomes the ADW name in every log line, so pick
something you will recognise.

**2. Upload the wallet** into the bucket, in a folder matching the prefix. **Skip this step with
`flags.use_wallet: false`:**

```bash
oci os object put --bucket-name aidp-adw-wallets \
  --name demo_adw3/Wallet_adw3.zip --file ./Wallet_adw3.zip
```

**3. Create the secrets** in the Vault, following the same pattern as the existing ones. The two
`wallet_*` rows apply only when `flags.use_wallet` is true:

| Secret | Contents |
|---|---|
| `demo_adw3_dsn` | the connection string from the ADW Console - mTLS descriptor or TLS string, matching the fleet's mode |
| `demo_adw3_pwd` | password of the administrative user |
| `demo_adw3_wallet_zip` | `oci://aidp-adw-wallets@<namespace>/demo_adw3/Wallet_adw3.zip` |
| `demo_adw3_wallet_pwd` | password of the wallet |

**4. Register them in the Credential Store** as **Vault Reference**, each named exactly like its
secret, all pointing at the same vault OCID.

**5. Add one line to `adw_sync.yaml`:**

```yaml
adw_prefixes:
  - demo_adw1
  - demo_adw2
  - demo_adw3      # new
```

**6. Run the job.** The new ADW starts empty, so everything is `CREATE` there while the existing
ones report `SKIP`. Nothing else to do.

Nothing changes for the service account: the same Service account credential serves the whole
fleet, and the first run installs the `DBMS_CLOUD` credential built from it in each of the new
ADW's schemas. No IAM change either - the existing policies already cover the new secrets, as long
as they sit in the same compartment and, with mTLS, the wallet sits in the same bucket.

Two things worth checking as the fleet grows:

- **Connections.** `min(fleet, adw_workers_cap) x workers`. With the defaults that is 4 x 8 = 32,
  and it does not grow with fleet size - `adw_workers_cap` is a deliberate cap.
- **`adw_user`.** If the new ADW uses a different administrative user, turn `adw_user` into a
  mapping keyed by prefix (see [Configuration reference](#configuration-reference)). Prefixes
  you leave out fall back to `ADMIN`; an unknown key is rejected rather than ignored.

### Removing an ADW

Delete the line from `adw_prefixes`. The notebook stops touching it; existing external tables keep
working until you drop them. To clean up, point `CATALOG` at it and run the teardown cell before
removing the line. Then delete its secrets and their Credential Store entries - and, with mTLS, its
wallet object.

## Switching the fleet between mTLS and walletless

The mode is a fleet-wide switch, so a change of mode is a change for every ADB at once.

**From mTLS to walletless:**

1. On **every** ADB in the fleet, set **Mutual TLS authentication** to *not required*
   (Console -> your ADB -> Network -> Edit). From AIDP this needs a **private endpoint** - see
   [Walletless from AIDP needs a private endpoint](#walletless-from-aidp-needs-a-private-endpoint).
   It is a security decision, not just a convenience: the wallet stops being part of what a
   caller needs.
2. Change each `<prefix>_dsn` secret to the **TLS** connection string from the console. The mTLS
   descriptor will not work without the wallet.
3. Set `flags.use_wallet: false`.

Then delete the `<prefix>_wallet_zip` and `<prefix>_wallet_pwd` secrets and their Credential Store
entries, the wallet bucket and the bucket policy from Step 2. Per ADW that is 4 secrets down to 2.
The secrets policy from Step 6 stays.

**From walletless back to mTLS:** set mutual TLS back to *required* on every ADB, recreate the
wallet bucket and its policy (Steps 1 and 2), add the two wallet secrets per ADW and register them
(Steps 4 and 5), change each `<prefix>_dsn` back to the mTLS descriptor, and set
`flags.use_wallet: true`. On a public endpoint where an ACL was set, set mutual TLS back to
required **before** clearing the ACL: OCI refuses to open the network while mutual TLS is off.

Either way, the connectivity probe at the end of section 3 is what proves the switch worked: it is
the only place a TLS/mTLS mismatch surfaces before the apply step.

---

## Using a dedicated user instead of ADMIN

**The default is `ADMIN`, and it is the recommended setting.** A dedicated user is worth creating,
but be clear about what it buys: **not less privilege.**

### Why a dedicated user is still worth it

`adw_user: AIDP_SYNC_ADMIN` (any name) buys separation, not containment:

- the job's credential can be rotated or revoked without touching `ADMIN`, which the whole fleet
  and every human operator also use;
- database audit distinguishes what the sync did from what a person did;
- if the credential leaks, you drop one user instead of rotating `ADMIN` across the fleet.

### Why it is not a privilege reduction

The job **creates one ADW user per source schema and rotates its password on every run**, so it
needs `CREATE USER` and `ALTER USER`. `ALTER USER` has no scope: whoever holds it can set
`ADMIN`'s password and connect as `ADMIN` on the next statement. Verified on ADB 23ai - a user
with no roles at all, holding only the individual grants this job needs, still changes another
user's password successfully.

So any user that can drive this sync is admin-equivalent. Treat the credential accordingly:
Vault, no reuse, rotate on staff changes. Do not present it to a security review as a restricted
account.

The only change that would genuinely reduce authority is architectural - pre-provision the
schemas out of band so the job never needs `CREATE USER` / `ALTER USER`. That is the same
direction as the proxy-authentication item in `ARCHITECTURE.md`, and it is not implemented.

### Setting one up

`PDB_DBA` covers almost everything. Two grants are missing and both must be issued by `ADMIN`:

```sql
CREATE USER AIDP_SYNC_ADMIN IDENTIFIED BY "<password>";
GRANT CREATE SESSION TO AIDP_SYNC_ADMIN;
GRANT PDB_DBA        TO AIDP_SYNC_ADMIN;

GRANT EXECUTE ON DBMS_CLOUD TO AIDP_SYNC_ADMIN WITH GRANT OPTION;
ALTER USER AIDP_SYNC_ADMIN QUOTA UNLIMITED ON DATA;
```

| Missing grant | How it fails |
|---|---|
| `EXECUTE ON DBMS_CLOUD` **with grant option** | `ORA-01031` when the job runs `GRANT EXECUTE ON DBMS_CLOUD TO <schema>`. `PDB_DBA` can *use* `DBMS_CLOUD` but not pass it on, and the job passes it to every schema it provisions |
| `QUOTA UNLIMITED ON DATA` | `ORA-01950` on the first registry `MERGE`. `CREATE TABLE` succeeds without it - deferred segment creation means the quota only bites on the first insert, so a create-only smoke test misses this |

No role avoids these two. `DBMS_CLOUD` is a public synonym for a package owned by the **common**
user `C##CLOUD$SERVICE`, and `GRANT ANY OBJECT PRIVILEGE` does not reach a common object from
inside the PDB - so even a user granted plain `DBA` fails the same check. `ADMIN` works only
because Oracle grants it directly, per versioned package, with `GRANTABLE = YES`. Quota is a
per-user attribute, so no role can carry it either.

Two operational notes:

- **After an ADB patch**, the versioned package name changes
  (`C##CLOUD$SERVICE.DBMS_CLOUD$PDBCS_<version>`). Grant through the synonym, as above, so it
  re-resolves; if `CREATE_EXTERNAL_TABLE` starts returning `ORA-01031` after a patch window,
  re-issue the grant.
- The ADB mandatory password profile **rejects a password containing the user name**
  (`ORA-28219` / `ORA-20002`). This affects only the user you create by hand; the per-schema
  passwords the job generates are random.

### Web access is separate: this user cannot sign in to Database Actions

A user created this way can connect through any SQL client - the job uses `python-oracledb` - but
it cannot sign in to **Database Actions** (SQL Developer Web) in the console. That interface is
served by **ORDS**, the Oracle REST Data Services layer in front of the database, and ORDS only
accepts a sign-in from a schema that has been explicitly REST-enabled. On a fresh Autonomous
Database only `ADMIN` is, which you can confirm with:

```sql
SELECT parsing_schema, status FROM user_ords_schemas;
```

This is deliberate: a database schema is not exposed over HTTP until someone decides it should be.
`PDB_DBA` does not change it, because REST enablement is a per-schema action rather than a
privilege a role can carry.

The sync needs none of this, so leaving it disabled is the smaller surface. If you do want web
access for the user, `ADMIN` can enable it:

```sql
BEGIN
  ORDS_ADMIN.ENABLE_SCHEMA(
    p_enabled             => TRUE,
    p_schema              => 'AIDP_SYNC_ADMIN',
    p_url_mapping_type    => 'BASE_PATH',
    p_url_mapping_pattern => 'aidp_sync_admin',
    p_auto_rest_auth      => TRUE);
  COMMIT;
END;
/
```

Alternatively, inspect this schema's objects while signed in as `ADMIN` and qualify the names
(`AIDP_SYNC_ADMIN.EXT_REGISTRY_V4`), which needs no enablement at all.

### Point the job's own tables at the user's own schema

```yaml
adw_user: AIDP_SYNC_ADMIN
registry_table: EXT_REGISTRY_V4          # unqualified
credential_state_table: EXT_CRED_STATE_V1 # same rule
```

Both tables are created and written by the **administrative** connection, so the schema qualifier
decides which privileges that user needs. Unqualified, they resolve to each ADW's own `adw_user`
schema: no `ANY TABLE` privilege required, and a fleet using different users per ADW keeps its
state separated.

Qualified into another schema - the historical `ADMIN.EXT_REGISTRY_V4` default - also works for a
`PDB_DBA` user, since it carries the `ANY TABLE` privileges. But there is no reason for the job to
depend on privileges it does not otherwise need.

---

## Day-two operations

### Rotating credentials

Four things can rotate, and each one reaches the job by a different route:

| What rotates | Where you change it | What the next run does |
|---|---|---|
| A per-ADW Vault secret - `_dsn`, `_pwd`, and with mTLS `_wallet_zip`, `_wallet_pwd` | a new secret version in OCI Vault | nothing special: the Vault Reference already reads `CURRENT` |
| The service account API key | the Service account credential in AIDP | detects the new fingerprint and reinstalls the `DBMS_CLOUD` credential in every schema |
| An ADB wallet - **mTLS only** | the wallet object, plus `_wallet_pwd` if the password changed | downloads the wallet again, as on every run |
| The Vault master encryption key | OCI Vault | nothing: the secret values are unchanged |

#### Rotating a Vault secret

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

#### Rotating the service account API key

Rotating the key in OCI is only half of it. The Service account credential is **not** a Vault
Reference, so the new `fingerprint` and `privateKey` (and `userId`, if the IAM user changed) have
to be written into it. **Recommended: drive that from an OCI Function** on the rotation event.
Doing it by hand works but leaves a window in which the ADWs hold a key that no longer exists.

The ADWs do not read Object Storage with the credential in AIDP - they read it with a `DBMS_CLOUD`
credential **built from it**, one per schema, installed by this job. So the copy inside each ADW
has to be rebuilt too, or every consumer query starts failing (`ORA-20401`, or
`ORA-20000: Failed to generate column list`) while the sync still reports everything in sync -
nothing about the *tables* changed.

The job handles that part. It records, per schema, the **fingerprint** of the key each credential
was built from:

```yaml
credential_state_table: EXT_CRED_STATE_V1
```

One row per `(catalog, schema)` holding `cred_name`, `key_fingerprint` and `updated_at`. A
fingerprint identifies a key; it is not the key, and nothing secret is stored there. Same
schema-qualifier rule as `registry_table` - leave it unqualified and it lands in the `adw_user`
schema.

Each run compares the recorded fingerprint against the one just read from the credential:

| Situation | What happens |
|---|---|
| Fingerprints match, nothing to sync | nothing. Still zero DDL |
| Fingerprints differ | the credential is reinstalled in every affected schema, even with no table work |
| No row recorded yet (first run, or a new schema) | treated as differing, so the credential is installed and recorded |
| Dry run | the mismatch is reported and nothing is written |

So the rotation propagates on the next scheduled run with no flag to remember and no manual step.
The summary carries a `creds` count of how many schemas were reinstalled:

```
demo_adw1: {'create': 0, 'recreate': 0, 'skip': 4762, 'drop': 0, 'ok': 0, 'err': 0, 'creds': 10, 'cred_err': 0}
```

No table is touched on that path, so consumers keep reading straight through it.

A reinstall that **fails** - a wrong fingerprint, a key the database cannot parse, a missing
privilege - is counted in `cred_err`, and the apply cell raises when any schema has `cred_err > 0`.
The job therefore goes red instead of reporting success over a schema whose consumers can no longer
read: `DROP_CREDENTIAL` has already run by the time `CREATE_CREDENTIAL` fails, so that schema has no
credential until the next successful run.

The detection compares the **fingerprint field** of the credential, as typed into the form. Update
the fingerprint together with the private key: a new key under the old fingerprint is not seen as
a rotation.

This is the same in both connection modes. The `DBMS_CLOUD` credential is how the ADWs reach
Object Storage, not how the job reaches the ADWs.

#### Rotating a wallet (mTLS only)

Rotating a wallet in the ADB console invalidates the previous one, so the job stops at the
connectivity probe in cell 3 until the new wallet is in place. Download the new wallet, upload it
over the same object, and update `<prefix>_wallet_pwd` if you set a new password. Uploading under
a new object name also works; then point `<prefix>_wallet_zip` at the new URI.

Nothing else: each run downloads the wallets into a temporary directory of its own and removes it
at the end, so the next run uses the new file - no restart, no Credential Store change.

Walletless TLS has no wallet to rotate. Its only per-ADW secrets are `_dsn` and `_pwd`.

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
demo_adw1: {'create': 12, 'recreate': 3, 'skip': 4762, 'drop': 0, 'ok': 15, 'creds': 2, 'cred_err': 0, 'err': 0, 'grants': 8}
```

`skip` dominating is the healthy steady state. `creds` is the number of schemas whose `DBMS_CLOUD`
credential was installed this run - those with table work, plus any whose recorded key fingerprint
no longer matches. `cred_err` is the number of schemas where that install **failed**; any value
above zero fails the job, since the schema is left without a `DBMS_CLOUD` credential. `err > 0`
prints the first twenty errors with the real Oracle cause, which is often on the second line of
the message.

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
| `404 NotAuthorizedOrNotFound` on a credential | a Vault Reference without the Step 6 policy, or a name that does not match the YAML prefix. The same 404 covers both. The Service account credential needs no policy |
| `Credential '...': field '...' returned empty` | the credential exists but its type does not carry that field - `oci_credential_service_account` must name a **Service account** credential |
| `could not connect` at the end of section 3, with `use_wallet: false` | `<prefix>_dsn` still holds the mTLS descriptor, or the ADB still requires mutual TLS |
| `could not connect` at the end of section 3, with `use_wallet: true` | the wallet does not match the DSN, or was rotated in the console and not re-uploaded |
| `ORA-12529`, or `DPY-6000: Listener refused connection` | the ADB network ACL. From AIDP, an active ACL on a public endpoint refuses the connection whatever it lists; keep the wallet, or use a private endpoint. Not a TLS, wallet or password problem |
| consumer queries fail with `ORA-20401` or `ORA-20000: Failed to generate column list` while the sync reports SKIP | the `DBMS_CLOUD` credential is stale after an API key rotation. The next run reinstalls it once the Service account credential holds the new key and fingerprint |
| `Configuration file not found` | scheduled job with the config not next to the notebook. Set `CONFIG_PATH` |
| `CATALOG not provided` | the job parameter is missing or spelled differently. `CATALOG` and `catalog` both work |
| `CROSS-CATALOG COLLISION` | another catalog already owns objects this job would create. Check the schema prefix |
| `ORA-00942` on a consumer query | the query started inside the ~0.8s drop-and-recreate window. Schedule syncs off-peak |
| a consumer does not see a newly added column | misaligned metadata: the tip snapshot references an older schema. See "Handling a misaligned table" |
| `ORA-01017` intermittently | two jobs with work in the same schema rotating the password under each other. Run catalogs sequentially |
| `ORA-06564: DATA_PUMP_DIR` | the `GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR` was removed. It is required for reads too |
| `ORA-12838` | `ALTER SESSION DISABLE PARALLEL DML` did not run. Check `_prep` |

---

## Configuration reference

Full commented template in `adw_sync.example.yaml`.

| Key | Default | Purpose |
|---|---|---|
| `region` | **required** | Object Storage / lakehouse region, not the ADW's |
| `oci_credential_service_account` | **required** | name of the Service account credential, used verbatim |
| `adw_prefixes` | **required** | one prefix per ADW |
| `adw_user` | `ADMIN` | a scalar for the whole fleet, or a mapping keyed by ADW prefix. A positional list is rejected |
| `flags.use_wallet` | `true` | fleet-wide connection mode. `true` = mTLS with a wallet per ADW; `false` = walletless TLS, the two `wallet_*` secrets are not read and no wallet bucket is needed. See [Connection mode](#connection-mode-mtls-or-walletless-tls) |
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
| `discovery.exclude_schemas` | see the example file | schemas never synced |
| `registry_table` | `ADMIN.EXT_REGISTRY_V4` | sync state, catalog-scoped. Leave **unqualified** when `adw_user` is not `ADMIN` |
| `credential_state_table` | `EXT_CRED_STATE_V1` | API key fingerprint per `(catalog, schema)`, used to detect a rotation. Same schema-qualifier rule as `registry_table` |
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
| `adw_sync.example.yaml` | the commented template, documenting every key. Copy it to `adw_sync.yaml` and replace the `CHANGE_ME_` values |
| `.gitignore` | keeps `adw_sync.yaml`, the deployment copy, out of version control |
| `ARCHITECTURE.md` | design, diagrams, measured scale, test evidence, references |
| `Architecture-EXT-TABLE-Sync.drawio.png` | component diagram, editable in draw.io |
| `requirements.txt` | `oracledb`, `oci`, `pyyaml`, `cryptography` - install as cluster libraries |
| `README.md` | this file |

## Dependencies

`oracledb`, `oci`, `pyyaml` and `cryptography`, listed in `requirements.txt`. `cryptography` converts the
service account key to the PKCS#1 form `DBMS_CLOUD.CREATE_CREDENTIAL` needs; it is a dependency of `oci`, so
it is normally present wherever `oci` is.

**Install them as cluster libraries**, on the compute cluster attached to the notebook and to the
job. In AIDP Workbench: **Compute -> your cluster -> Libraries -> Install new -> PyPI**, one entry
per package, then restart the cluster.

`oci` and `pyyaml` are usually already present; **`oracledb` is not**, and it is the whole database
driver - cell 1 fails with `ModuleNotFoundError: No module named 'oracledb'` without it.

Cell 1 also carries a `%pip install` line, commented out. It is fine for a quick interactive test,
but it installs into the session only: a **scheduled job runs in a fresh session and will fail**.
For anything you schedule, the cluster library is the only option.
