# APEX Chat Plugin

The APEX Chat plugin is a region plugin that adds a UI for chatting with AIDP agents. The plugin provides persistent conversation history, async Oracle AQ-backed response processing, conversation summarization, and more.

## Prerequisites

Before installation, confirm the following:

1. AIDP Instance with Deployed Agent
   - You need an AIDP instance with an agent already deployed.
   - The agent endpoint URL will be used when configuring the plugin.

2. Oracle Autonomous Database
   - Use an Oracle Autonomous Database environment for this plugin.
   - You can use the one provisioned as part of your AIDP instance setup.

3. Database Schema
   - You need a database schema for the plugin objects.
   - While schema creation/updates are covered in the installation steps below, this is listed as a prerequisite because the APEX workspace requirement that follows depends on this schema.

4. APEX Workspace and APEX App
   - Ensure an APEX workspace exists and is mapped to the schema used for installation.
   - You can deploy the plugin into an existing APEX application, or create a basic/bare-bones application and deploy it there, or install the included application which already has the plugin installed.

5. IAM Domain User for AIDP Agent & Select AI Authentication
   - A domain user account will drive authentication for AIDP and Select AI.
   - The account must have the required permissions in place.

## Contents

- `install/01_create_user.sql`
  - Creates (or reuses) the application schema user.
  - Grants required system and package privileges.
- `install/02_install_objects.sql`
  - Installs all plugin database objects in dependency order.
- `install/sql/`
  - SQL source files needed for installation.
- `install/plugin/region_type_plugin_apex_chat_plugin.sql`
  - APEX region plugin export to import into your workspace/application.
- `install/app/f116.sql`
  - APEX application with the plugin already installed.

## Main Install Steps

There are three primary steps:

1. Create/prepare the schema user.
2. Install the database objects.
3. Import the APEX app or plugin.

### SQLcl Requirement for Steps 1 and 2

Run `install/01_create_user.sql` and `install/02_install_objects.sql` with SQLcl.
These scripts are tested with SQLcl `25.4.2` and should also work in earlier SQLcl releases.

### Step 1: Create/Prepare Schema User

Connect as an admin user (typically `ADMIN` or equivalent) and run:

```sql
@install/01_create_user.sql
```
If you are not connected, SQLcl will return `SP2-0640` errors and the script will not execute.

The script will prompt for:
   - `APP_SCHEMA` - defaults to `APEX_CHAT_PLUGIN`
   - `APP_PASSWORD`
   - `APP_DEFAULT_TABLESPACE` - defaults to `DATA`
   - `APP_TEMP_TABLESPACE` - defaults to `TEMP`

Press Enter at each prompt to accept the script defaults.
If `APP_PASSWORD` is left blank for a new user, the script auto-generates a 16-character strong password and prints it after user creation.
If the schema already exists, the script does not change that user's password.

### Step 2: Install Database Objects

Connect as the app schema user (`APP_SCHEMA`) and run:

```sql
@install/02_install_objects.sql
```
The script prompts for the target schema and defaults to `APEX_CHAT_PLUGIN`.
Part 2 is owner-only by design: connect as that schema before running it.
Cross-schema/admin-driven object installation is not supported.
The installer also prompts for required credential inputs and creates `ACP_CHAT_CRED` and `ACP_CHAT_PROFILE`:
- `Credential User OCID`
- `Credential Tenancy OCID`
- `Credential Private Key` (either a one-line key value or a local PEM file path)
- `Credential Fingerprint`

### Step 3: Import APEX App and Configure It

You can install the APEX app by uploading it into APEX or by running this script:

```sql
@install/app/f116.sql
```

Alternatively, you can import just the plugin into an existing application (via APEX UI) using this script:

```
install/plugin/region_type_plugin_apex_chat_plugin.sql
```

Then in your application:

1. If you imported a plugin, create a region of the imported plugin type.
2. Configure plugin attributes:
   - **Endpoint URL** - The URL for the AIDP agent.
   - **Session Key Item** - The name of a hidden, unprotected page item that identifies the current conversation for the APEX session.
3. Ensure supporting objects are present for runtime:
   - Credential `ACP_CHAT_CRED`
   - Profile `ACP_CHAT_PROFILE`
