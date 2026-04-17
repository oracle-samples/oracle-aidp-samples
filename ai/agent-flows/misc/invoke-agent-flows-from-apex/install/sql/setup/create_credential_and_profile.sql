prompt Creating DBMS_CLOUD credential and DBMS_CLOUD_AI profile...

-- Resolve private key input:
-- 1) If input is an existing local file path, SQLcl JavaScript reads file contents.
-- 2) Otherwise, treat the input as the private key text itself.
-- Key text is normalized to single-line text with \n markers for safe substitution.
define RESOLVED_CRED_PRIVATE_KEY = '^CRED_PRIVATE_KEY'
define RESOLVED_CRED_PRIVATE_KEY_SOURCE = 'INLINE'
script sql/setup/resolve_private_key.js "^CRED_PRIVATE_KEY"

declare
  l_user_ocid       varchar2(4000) := trim('^CRED_USER_OCID');
  l_tenancy_ocid    varchar2(4000) := trim('^CRED_TENANCY_OCID');
  l_private_key_raw varchar2(32767) := trim('^RESOLVED_CRED_PRIVATE_KEY');
  l_private_key     clob;
  l_fingerprint     varchar2(4000) := trim('^CRED_FINGERPRINT');
begin
  l_private_key := replace(l_private_key_raw, '\n', chr(10));

  if l_user_ocid is null then
    raise_application_error(-20010, 'Credential User OCID is required.');
  end if;

  if l_tenancy_ocid is null then
    raise_application_error(-20011, 'Credential Tenancy OCID is required.');
  end if;

  if trim(l_private_key_raw) is null then
    raise_application_error(-20012, 'Credential Private Key is required.');
  end if;

  if l_fingerprint is null then
    raise_application_error(-20013, 'Credential Fingerprint is required.');
  end if;

  dbms_output.put_line(
    'Credential private key source=' || '^RESOLVED_CRED_PRIVATE_KEY_SOURCE'
    || ', length=' || dbms_lob.getlength(l_private_key)
  );

  dbms_cloud.create_credential(
    credential_name => 'ACP_CHAT_CRED',
    user_ocid       => l_user_ocid,
    tenancy_ocid    => l_tenancy_ocid,
    private_key     => l_private_key,
    fingerprint     => l_fingerprint
  );

  dbms_cloud_ai.create_profile(
    profile_name => 'ACP_CHAT_PROFILE',
    attributes   => '{"provider":"oci","credential_name":"ACP_CHAT_CRED","model":"meta.llama-3.1-405b-instruct"}'
  );

  dbms_output.put_line('Created ACP_CHAT_CRED and ACP_CHAT_PROFILE.');
end;
/
