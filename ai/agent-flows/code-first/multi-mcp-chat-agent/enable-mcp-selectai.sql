-- ============================================================
-- SELECT AI + MCP SETUP TEMPLATE
-- Oracle Autonomous Database (ADB)
-- Run each block sequentially in SQL Worksheet (or SQL Developer Web)
-- ============================================================
--
-- FILL IN BEFORE RUNNING:
--   PROFILE_NAME  → e.g. 'NL2SQL_PROFILE' (line 53)
--   SCHEMA_OWNER  → e.g. 'ADMIN'           (line 54)
--   TOOL_NAME     → e.g. 'NL2SQL_TOOL'     (lines 99 / 107)
--   SMOKE_TEST    → replace with a question about your data (line 121)
--
-- PREREQUISITES:
--   - ADB instance running (19c v19.29+ or 26ai v23.26+)
--   - MCP tag added to the ADB instance via OCI Console (Step 0 below)
--   - Tables already loaded into the schema you point Select AI at
-- ============================================================


-- ============================================================
-- STEP 0: ADD MCP TAG IN OCI CONSOLE (do this BEFORE running SQL)
-- ============================================================
-- OCI Console → Autonomous Database → your ADB instance
-- Click Tags → Add tag:
--   Tag namespace : (free-form)
--   Tag key       : adb$feature
--   Tag value     : {"name":"mcp_server","enable":true}
-- Click Save.
--
-- This exposes the MCP endpoint at:
--   https://dataaccess.adb.<region>.oraclecloudapps.com/adb/mcp/v1/databases/<adb_ocid>
-- ============================================================


-- STEP 1: Reset any manually created credential (safe to re-run)
BEGIN
  DBMS_CLOUD.DROP_CREDENTIAL(credential_name => 'OCI_CRED');
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

-- STEP 2: Enable resource principal authentication
-- This lets ADB call OCI Generative AI using the database's own identity
EXEC DBMS_CLOUD_ADMIN.ENABLE_RESOURCE_PRINCIPAL();

-- STEP 3: Confirm credential was created automatically
SELECT credential_name FROM user_credentials;
-- You should see OCI$RESOURCE_PRINCIPAL in the results


-- STEP 4: Create and enable Select AI profile
-- Auto-discovers all tables in the schema — no hardcoding needed.
-- Edit the profile name and schema owner below.
DECLARE
  l_profile_name CONSTANT VARCHAR2(100) := 'NL2SQL_PROFILE'; -- CHANGE
  l_schema_owner CONSTANT VARCHAR2(100) := 'ADMIN';          -- CHANGE IF NEEDED
  l_object_list  CLOB    := '[';
  l_first        BOOLEAN := TRUE;
BEGIN
  BEGIN
    DBMS_CLOUD_AI.DROP_PROFILE(profile_name => l_profile_name);
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;

  FOR r IN (
    SELECT table_name FROM all_tables
    WHERE owner = l_schema_owner
    ORDER BY table_name
  ) LOOP
    IF NOT l_first THEN
      l_object_list := l_object_list || ',';
    END IF;
    l_object_list := l_object_list || '{"owner":"' || l_schema_owner || '","name":"' || r.table_name || '"}';
    l_first := FALSE;
  END LOOP;

  l_object_list := l_object_list || ']';

  DBMS_CLOUD_AI.CREATE_PROFILE(
    profile_name => l_profile_name,
    attributes   => '{"provider":"oci","credential_name":"OCI$RESOURCE_PRINCIPAL","object_list":' || l_object_list || '}'
  );

  BEGIN
    DBMS_CLOUD_AI.ENABLE_PROFILE(profile_name => l_profile_name);
  EXCEPTION
    WHEN OTHERS THEN NULL; -- profile may already be enabled on newer DB versions
  END;

  DBMS_OUTPUT.PUT_LINE('Profile created: ' || l_profile_name);
  DBMS_OUTPUT.PUT_LINE('Tables included: ' || l_object_list);
END;
/


-- STEP 5: Register MCP tool
-- Change tool name and profile name to match Step 4.
BEGIN
  DBMS_CLOUD_AI_AGENT.DROP_TOOL(tool_name => 'NL2SQL_TOOL'); -- CHANGE
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  DBMS_CLOUD_AI_AGENT.CREATE_TOOL(
    tool_name   => 'NL2SQL_TOOL',                                                       -- CHANGE
    attributes  => '{"tool_type":"SQL","tool_params":{"profile_name":"NL2SQL_PROFILE"}}', -- CHANGE PROFILE NAME
    status      => 'ENABLED',
    description => 'Query the database using natural language.'                          -- OPTIONAL
  );
  COMMIT;  -- IMPORTANT: without this, tool registration silently fails to persist on some ADB versions.
END;
/

-- STEP 6: Confirm tool registered
-- View name varies across ADB versions. Try in order until one returns rows:
SELECT * FROM user_cloud_ai_tools;        -- some versions
-- SELECT * FROM user_cloud_ai_agent_tools;  -- other versions
-- SELECT * FROM user_ai_agent_tools;        -- newest


-- STEP 7: Smoke test (replace with a question about your data)
SELECT DBMS_CLOUD_AI.GENERATE(
  prompt       => 'How many account transactions do we have?', -- CHANGE
  profile_name => 'NL2SQL_PROFILE',                            -- MATCH STEP 4
  action       => 'runsql'
) AS result FROM dual;


-- ============================================================
-- TROUBLESHOOTING
-- ============================================================
-- If `curl <mcp_url> tools/list` returns an empty list ({"tools":[]})
-- even though Step 6 shows the tool is registered:
--   → MCP server caches the tool registry at startup. Force a refresh:
--     OCI Console → Autonomous Database → your ADB → More actions →
--     Stop, wait ~30s, then Start. The MCP sidecar re-reads the
--     registry on boot and the tool will show up in tools/list.
-- ============================================================
