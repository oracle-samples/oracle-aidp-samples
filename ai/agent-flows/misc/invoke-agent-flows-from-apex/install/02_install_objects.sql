-- AIDP installer: step 2 (run as app schema / object owner)

set define '^' verify off feedback on serveroutput on
whenever sqlerror exit sql.sqlcode rollback

define DEF_APP_SCHEMA = 'APEX_CHAT_PLUGIN'

prompt ==============================================
prompt AIDP - Install ACP objects
prompt ==============================================

accept APP_SCHEMA char default '^DEF_APP_SCHEMA' prompt 'Target schema for object install [^DEF_APP_SCHEMA]: '
accept CRED_USER_OCID char prompt 'Credential User OCID (required): '
accept CRED_TENANCY_OCID char prompt 'Credential Tenancy OCID (required): '
accept CRED_PRIVATE_KEY char prompt 'Credential Private Key or local PEM file path (required): '
accept CRED_FINGERPRINT char prompt 'Credential Fingerprint (required): '

declare
  l_schema_count number := 0;
  l_session_user varchar2(128) := upper(sys_context('USERENV', 'SESSION_USER'));
  l_user_ocid    varchar2(4000) := trim('^CRED_USER_OCID');
  l_tenancy_ocid varchar2(4000) := trim('^CRED_TENANCY_OCID');
  l_private_key  clob := trim('^CRED_PRIVATE_KEY');
  l_fingerprint  varchar2(4000) := trim('^CRED_FINGERPRINT');
begin
  select count(*)
    into l_schema_count
    from all_users
   where username = upper('^APP_SCHEMA');

  if l_schema_count = 0 then
    raise_application_error(-20001, 'Schema ' || upper('^APP_SCHEMA') || ' was not found.');
  end if;
  
  if l_session_user <> upper('^APP_SCHEMA') then
    raise_application_error(
      -20002,
      'Owner-only install: connect as '
      || upper('^APP_SCHEMA')
      || ' and rerun 02_install_objects.sql.'
    );
  end if;

  if l_user_ocid is null then
    raise_application_error(-20010, 'Credential User OCID is required.');
  end if;

  if l_tenancy_ocid is null then
    raise_application_error(-20011, 'Credential Tenancy OCID is required.');
  end if;

  if l_private_key is null then
    raise_application_error(-20012, 'Credential Private Key is required.');
  end if;

  if l_fingerprint is null then
    raise_application_error(-20013, 'Credential Fingerprint is required.');
  end if;
end;
/

prompt Creating tables...
@@sql/tables/acp_conversations.sql
@@sql/tables/acp_messages.sql

prompt Creating constraints and indexes...
@@sql/ref_constraints/acp_msg_conversation_id_fk.sql
@@sql/indexes/acp_conv_summary_stat_ix.sql
@@sql/indexes/acp_conv_active_ix.sql
@@sql/indexes/acp_msg_resp_stat_ix.sql
@@sql/indexes/acp_msg_conv_resp_ix.sql

prompt Creating package spec/body...
@@sql/package_specs/acp_chat.sql
@@sql/package_bodies/acp_chat.sql

prompt Creating AQ objects...
@@sql/aq_queues/acp_aq_tab.sql
@@sql/aq_queues/acp_queue.sql
@@sql/aq_queues/acp_aq_subscriber.sql

prompt Creating scheduler program/job...
@@sql/programs/acp_queue_worker_pgm.sql
@@sql/jobs/acp_queue_sweeper.sql

prompt Creating credential and profile...
@@sql/setup/create_credential_and_profile.sql

prompt --------------------------------------------------

prompt Optional: import sample APEX application
prompt @@app/f116.sql
prompt --------------------------------------------------

begin
  dbms_output.put_line('Step 2 complete.');
end;
/
