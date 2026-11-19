# Step 7 - Database objects

Run these as your **workspace's parsing schema** (SQL Workshop -> SQL Commands). This
sample mints only one token (OAC's), but the table/package structure supports adding
more token keys later without a schema change.

## 7.1 `jwt_util` - base64url encoding and KMS-backed signing

```plsql
create or replace package jwt_util as
  function base64url(p_raw raw) return varchar2;
  function sign_and_build(p_signing_input varchar2) return varchar2;
  function build_jwt(p_payload_json varchar2, p_kid varchar2 default null) return varchar2;
end jwt_util;
/

create or replace package body jwt_util as

  function base64url(p_raw raw) return varchar2 is
    l_b64 varchar2(32767);
  begin
    l_b64 := utl_raw.cast_to_varchar2(utl_encode.base64_encode(p_raw));
    l_b64 := replace(l_b64, chr(13));
    l_b64 := replace(l_b64, chr(10));
    l_b64 := replace(l_b64, '+', '-');
    l_b64 := replace(l_b64, '/', '_');
    l_b64 := rtrim(l_b64, '=');
    return l_b64;
  end base64url;

  function sign_and_build(p_signing_input varchar2) return varchar2 is
    l_digest        raw(32);
    l_response      clob;
    l_signature_b64 varchar2(4000);
    l_signature_raw raw(2000);
  begin
    l_digest := dbms_crypto.hash(
                  src => utl_raw.cast_to_raw(p_signing_input),
                  typ => dbms_crypto.hash_sh256);

    apex_web_service.g_request_headers.delete;
    apex_web_service.g_request_headers(1).name  := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';

    l_response := apex_web_service.make_rest_request(
      p_url                  => 'https://<vault-id>-crypto.kms.<region>.oraclecloud.com/20180608/sign',
      p_http_method          => 'POST',
      p_credential_static_id => 'OCI_USER_CRED',
      p_body                 => json_object(
                                   key 'keyId' value '<key OCID>',
                                   key 'keyVersionId' value '<key version OCID>',
                                   key 'message' value utl_raw.cast_to_varchar2(utl_encode.base64_encode(l_digest)),
                                   key 'messageType' value 'DIGEST',
                                   key 'signingAlgorithm' value 'SHA_256_RSA_PKCS1_V1_5'));

    apex_json.parse(l_response);
    l_signature_b64 := apex_json.get_varchar2(p_path => 'signature');
    l_signature_raw := utl_encode.base64_decode(utl_raw.cast_to_raw(l_signature_b64));

    return base64url(l_signature_raw);
  end sign_and_build;

  function build_jwt(p_payload_json varchar2, p_kid varchar2 default null) return varchar2 is
    l_header_json   varchar2(200);
    l_header_b64    varchar2(200);
    l_payload_b64   varchar2(32767);
    l_signing_input varchar2(32767);
    l_signature     varchar2(500);
  begin
    if p_kid is not null then
      l_header_json := json_object(key 'alg' value 'RS256', key 'typ' value 'JWT', key 'kid' value p_kid);
    else
      l_header_json := '{"alg":"RS256","typ":"JWT"}';
    end if;

    l_header_b64    := base64url(utl_raw.cast_to_raw(l_header_json));
    l_payload_b64   := base64url(utl_raw.cast_to_raw(p_payload_json));
    l_signing_input := l_header_b64 || '.' || l_payload_b64;
    l_signature     := sign_and_build(l_signing_input);
    return l_signing_input || '.' || l_signature;
  end build_jwt;

end jwt_util;
/
```

**Smoke test** (confirms ACL + credential + KMS plumbing before building anything on
top):

```sql
select jwt_util.sign_and_build('test') from dual;
```

## 7.2 `token_store` - per-user, per-key token cache

```sql
create table agent_user_tokens (
  username     varchar2(255),
  token_key    varchar2(50),
  access_token clob,
  expires_at   timestamp,
  updated      timestamp default sys_extract_utc(systimestamp),
  constraint agent_user_tokens_pk primary key (username, token_key)
);
```

```plsql
create or replace package token_store as
  procedure set_token(p_username in varchar2, p_token_key in varchar2, p_access_token in clob, p_expires_in in number);
  function get_token(p_username in varchar2, p_token_key in varchar2) return clob;
end token_store;
/

create or replace package body token_store as

  procedure set_token(p_username in varchar2, p_token_key in varchar2, p_access_token in clob, p_expires_in in number) is
  begin
    merge into agent_user_tokens t
    using (select p_username as username, p_token_key as token_key from dual) s
    on (t.username = s.username and t.token_key = s.token_key)
    when matched then update set
      access_token = p_access_token,
      expires_at   = sys_extract_utc(systimestamp) + numtodsinterval(p_expires_in, 'second'),
      updated      = sys_extract_utc(systimestamp)
    when not matched then insert (username, token_key, access_token, expires_at, updated)
    values (p_username, p_token_key, p_access_token,
            sys_extract_utc(systimestamp) + numtodsinterval(p_expires_in, 'second'),
            sys_extract_utc(systimestamp));
    commit;
  end set_token;

  function get_token(p_username in varchar2, p_token_key in varchar2) return clob is
    l_token   clob;
    l_expires timestamp;
  begin
    select access_token, expires_at into l_token, l_expires
    from   agent_user_tokens where username = p_username and token_key = p_token_key;

    if sys_extract_utc(systimestamp) >= l_expires then
      return null;  -- expired; caller re-mints
    end if;
    return l_token;
  exception
    when no_data_found then return null;
  end get_token;

end token_store;
/
```

**Why UTC everywhere:** comparing `expires_at` in the session's local time zone against
a UTC-written value makes a fresh token look already expired, and calls fail downstream
in a way that looks like a token problem rather than an obvious time-zone bug.

## 7.3 `mint_oac_token` - mints the per-user OAC token

```plsql
create or replace procedure mint_oac_token(p_username in varchar2) as
  g_client_id       constant varchar2(100) := '<confidential-app-client-id>';    -- step 4
  g_token_endpoint  constant varchar2(200) := 'https://<identity-domain-host>/oauth2/v1/token';
  g_scope           constant varchar2(200) := '<OAC downstream API scope>';       -- step 4
  g_kid             constant varchar2(100) := 'oac_chat_jwt_signing_cert';        -- must match step 4's registered Alias

  l_username         varchar2(255);
  l_now_date         date := cast(sys_extract_utc(systimestamp) as date);
  l_iat              number;
  l_client_claims    varchar2(1000);
  l_user_claims      varchar2(1000);
  l_client_assertion varchar2(32767);
  l_user_assertion   varchar2(32767);
  l_body             varchar2(32767);
  l_response         clob;
  l_access_token     varchar2(32767);
  l_expires_in       number;
begin
  -- :APP_USER at the "After Authentication" process point carries a " (SchemeName)"
  -- suffix that doesn't appear anywhere else (e.g. on a page). Strip it.
  l_username := regexp_replace(p_username, ' \([^)]*\)$', '');

  l_iat := round((l_now_date - date '1970-01-01') * 86400);

  l_client_claims := json_object(
    key 'iss' value g_client_id,
    key 'sub' value g_client_id,
    key 'aud' value 'https://identity.oraclecloud.com/',  -- fixed IDCS audience, NOT your token endpoint URL
    key 'jti' value lower(rawtohex(sys_guid())),
    key 'iat' value l_iat,
    key 'exp' value l_iat + 300
  );

  l_user_claims := json_object(
    key 'iss' value g_client_id,
    key 'sub' value l_username,
    key 'aud' value 'https://identity.oraclecloud.com/',
    key 'jti' value lower(rawtohex(sys_guid())),
    key 'iat' value l_iat,
    key 'exp' value l_iat + 3600
  );

  -- Two assertions, not one duplicated: client_assertion authenticates the CLIENT
  -- (sub = client_id, proving this confidential app is a legitimate caller); assertion
  -- (the user claims) is the actual jwt-bearer grant payload, whose sub is who the
  -- issued token should represent. Drop either one and the request fails differently -
  -- missing client auth vs. no grant at all.
  l_client_assertion := jwt_util.build_jwt(l_client_claims, g_kid);
  l_user_assertion   := jwt_util.build_jwt(l_user_claims, g_kid);

  l_body := 'grant_type=' || utl_url.escape('urn:ietf:params:oauth:grant-type:jwt-bearer', true)
          || '&assertion=' || utl_url.escape(l_user_assertion, true)
          || '&client_assertion_type=' || utl_url.escape('urn:ietf:params:oauth:client-assertion-type:jwt-bearer', true)
          || '&client_assertion=' || utl_url.escape(l_client_assertion, true)
          || '&scope=' || utl_url.escape(g_scope, true);

  apex_web_service.g_request_headers.delete;
  apex_web_service.g_request_headers(1).name  := 'Content-Type';
  apex_web_service.g_request_headers(1).value := 'application/x-www-form-urlencoded';

  -- Must be a form body, not query-string params on the URL -- the token endpoint
  -- 500s otherwise.
  l_response := apex_web_service.make_rest_request(
    p_url         => g_token_endpoint,
    p_http_method => 'POST',
    p_body        => l_body);

  if apex_web_service.g_status_code != 200 then
    raise_application_error(-20001,
      'OAC token endpoint returned ' || apex_web_service.g_status_code || ': ' ||
      dbms_lob.substr(l_response, 4000, 1));
  end if;

  apex_json.parse(l_response);
  l_access_token := apex_json.get_varchar2(p_path => 'access_token');
  l_expires_in   := apex_json.get_number(p_path => 'expires_in');

  token_store.set_token(l_username, 'oac', l_access_token, l_expires_in);
end mint_oac_token;
/
```

**Manual test** (no live APEX session needed - use a literal email):

```plsql
begin
  mint_oac_token('you@example.com');
end;
/

select username, token_key, expires_at, updated from agent_user_tokens;
```

**Token claims check** - `sub` should be the user's email/identity, `aud` should match
the OAC scope from step 4:

```plsql
declare
  l_token clob; l_payload varchar2(4000);
begin
  select access_token into l_token from agent_user_tokens
  where username = 'you@example.com' and token_key = 'oac';
  l_payload := regexp_substr(l_token, '[^.]+', 1, 2);
  l_payload := replace(replace(l_payload,'-','+'),'_','/');
  l_payload := rpad(l_payload, length(l_payload) + mod(4 - mod(length(l_payload),4), 4), '=');
  dbms_output.put_line(utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(l_payload))));
end;
/
```

## Gotchas worth remembering

- `DBMS_CRYPTO` and `UTL_URL` execute privileges aren't always granted to `PUBLIC` on
  Autonomous Database - grant explicitly (step 6) if you hit `PLS-00201`.
- The `aud` claim on both assertions is the **fixed** string
  `https://identity.oraclecloud.com/`, not your tenant's token endpoint URL - using the
  token endpoint URL fails with `invalid_client` / "Invalid audience."
- `g_kid` must exactly match the certificate's Alias from step 4, or verification fails
  with a generic `invalid_client` "system error" that doesn't point at the mismatch.
- `SQLERRM` cannot be used directly inside a SQL statement (even one embedded in a
  PL/SQL block) - assign it to a variable first, then use that variable, or you get
  `ORA-00984: column not allowed here`.
