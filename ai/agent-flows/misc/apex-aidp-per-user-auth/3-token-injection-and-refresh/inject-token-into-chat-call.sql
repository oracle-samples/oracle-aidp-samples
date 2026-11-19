-- Inject the per-user token into the APEX chat plugin's /chat request.
--
-- Context: the "invoke agent flows from APEX" chat plugin builds the /chat body with
-- input/sessionKey/etc. but no `metadata`, so the agent's OAC MCP tool never receives a token.
-- This adds the token as the agent's MCP session variable.
--
-- Where: in the plugin's request-builder, AFTER the input array is added to the body JSON and
-- BEFORE the body is serialized to a CLOB, e.g.:
--     l_body_json.put('input', l_input_arr_json);
--     <-- insert the block below here -->
--     l_request_payload := l_body_json.to_clob();
--
-- Placeholders:
--   <TOKEN_STORE>       your per-user token store package (e.g. from phase 2)
--   <TOOL_DISPLAY_NAME> the agent's OAC MCP tool display name (must match exactly)
--   'oac'               the token key phase 2 mints/stores under (get_token's 2nd arg)

declare
  l_meta  json_object_t := json_object_t();
  l_token clob := <TOKEN_STORE>.get_token(:APP_USER, 'oac');   -- per-user cached token (null if expired)
begin
  if l_token is not null and dbms_lob.getlength(l_token) > 0 then
    -- The key MUST match the agent's MCP tool display name, or the token is silently ignored.
    l_meta.put('sessionvariables.cred.mcp.<TOOL_DISPLAY_NAME>.bearer', l_token);
    l_body_json.put('metadata', l_meta);
  end if;
end;
