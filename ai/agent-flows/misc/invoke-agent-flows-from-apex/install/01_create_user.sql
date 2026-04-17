-- AIDP installer: step 1 (run as SYS or another admin user)
-- Provide values at prompts, or press Enter to use defaults.

set define on verify off feedback on serveroutput on
whenever sqlerror exit sql.sqlcode rollback

prompt ==============================================
prompt AIDP - Create/prepare application schema user
prompt ==============================================
prompt NOTE: You must be connected as an admin user before running this script.

-- Defaults (press Enter at each prompt to accept default values)
define DEF_APP_SCHEMA = 'APEX_CHAT_PLUGIN'
define DEF_APP_DEFAULT_TABLESPACE = 'DATA'
define DEF_APP_TEMP_TABLESPACE = 'TEMP'

accept APP_SCHEMA char default '&DEF_APP_SCHEMA' prompt 'Application schema username [&DEF_APP_SCHEMA]: '
accept APP_PASSWORD char hide prompt 'Application schema password (leave blank to auto-generate for new user): '
accept APP_DEFAULT_TABLESPACE char default '&DEF_APP_DEFAULT_TABLESPACE' prompt 'Default tablespace [&DEF_APP_DEFAULT_TABLESPACE]: '
accept APP_TEMP_TABLESPACE char default '&DEF_APP_TEMP_TABLESPACE' prompt 'Temporary tablespace [&DEF_APP_TEMP_TABLESPACE]: '

declare
  l_exists number := 0;
  l_app_schema varchar2(128) := upper(trim('&APP_SCHEMA'));
  l_password varchar2(32767) := trim('&APP_PASSWORD');
  l_generated_password boolean := false;

  function random_char(p_chars varchar2) return varchar2 is
    l_pos pls_integer;
  begin
    l_pos := trunc(dbms_random.value(1, length(p_chars) + 1));
    return substr(p_chars, l_pos, 1);
  end random_char;

  function random_password(p_length pls_integer default 16) return varchar2 is
    l_upper varchar2(26) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    l_lower varchar2(26) := 'abcdefghijklmnopqrstuvwxyz';
    l_digit varchar2(10) := '0123456789';
    l_special varchar2(30) := '!@#$%^*()-_=+[]{}:,.?';
    l_all varchar2(200);
    l_pool varchar2(32767);
    l_out varchar2(32767) := '';
    l_pick pls_integer;
  begin
    l_all := l_upper || l_lower || l_digit || l_special;

    -- Ensure at least one character from each class.
    l_pool := random_char(l_upper)
              || random_char(l_lower)
              || random_char(l_digit)
              || random_char(l_special);

    for i in 5 .. p_length loop
      l_pool := l_pool || random_char(l_all);
    end loop;

    -- Shuffle characters for better distribution.
    while length(l_pool) > 0 loop
      l_pick := trunc(dbms_random.value(1, length(l_pool) + 1));
      l_out := l_out || substr(l_pool, l_pick, 1);
      l_pool := substr(l_pool, 1, l_pick - 1) || substr(l_pool, l_pick + 1);
    end loop;

    return l_out;
  end random_password;
begin
  select count(*)
    into l_exists
    from dba_users
   where username = l_app_schema;

  if l_exists = 0 then
    if l_password is null then
      l_password := random_password(16);
      l_generated_password := true;
    end if;

    execute immediate 'create user '
                      || dbms_assert.enquote_name(
      l_app_schema,
      false
    )
                      || ' identified by "'
                      || replace(l_password, '"', '""')
                      || '"'
                      || ' default tablespace '
                      || dbms_assert.enquote_name(
      upper('&APP_DEFAULT_TABLESPACE'),
      false
    )
                      || ' temporary tablespace '
                      || dbms_assert.enquote_name(
      upper('&APP_TEMP_TABLESPACE'),
      false
    );
    dbms_output.put_line('Created user ' || l_app_schema);
    if l_generated_password then
      dbms_output.put_line('Generated password: ' || l_password);
    end if;
  else
    dbms_output.put_line('User already exists: ' || l_app_schema);
    dbms_output.put_line('Password was left unchanged.');
  end if;
end;
/

prompt Granting core privileges...

grant
  create session
to &app_schema;
grant
  create procedure
to &app_schema;
grant
  create job
to &app_schema;
grant
  create table
to &app_schema;
grant
  unlimited tablespace
to &app_schema;

prompt Granting required package execute privileges...

grant execute on sys.dbms_aqadm to &app_schema;
grant execute on sys.dbms_aq to &app_schema;

-- For environments where DBMS_CLOUD / DBMS_CLOUD_AI are directly grantable from your admin account.
-- Public synonyms are expected for DBMS_CLOUD and DBMS_CLOUD_AI.
whenever sqlerror continue
grant execute on dbms_cloud to &app_schema;
grant execute on dbms_cloud_ai to &app_schema;
whenever sqlerror exit sql.sqlcode rollback

begin
  dbms_output.put_line('Step 1 complete.');
end;
/
