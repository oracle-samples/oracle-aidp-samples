-- -----------------------------------------------------------------------------
-- ADW setup for querying a Delta UniForm table through Iceberg-compatible
-- metadata generated in OCI Object Storage.
--
-- Run this file in ADW before running the notebook.
--
-- What this script does:
-- 1. Creates an OCI API signing-key credential for DBMS_CLOUD.
-- 2. Grants outbound network ACL access to OCI Object Storage.
-- 3. Creates a stored procedure that:
--    - parses an oci://bucket@namespace/table-path URI
--    - finds the highest vN.metadata.json under <table-path>/metadata/
--    - drops the existing ADW external table if present
--    - recreates the ADW external table against that latest Iceberg metadata
--
-- Notes:
-- - This procedure is intended for direct Iceberg metadata access in ADW.
-- - ADW direct metadata access is point-in-time, so refreshing to the latest
--   metadata file is implemented by drop + recreate.
-- - The Object Storage host ACL must be granted to the DB user that executes
--   DBMS_CLOUD operations. Replace <principal_name> accordingly.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- 1. Create the DBMS_CLOUD credential used to access OCI Object Storage.
-- -----------------------------------------------------------------------------
BEGIN
   BEGIN
      DBMS_CLOUD.DROP_CREDENTIAL('<name_of_credential>');
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END;

   DBMS_CLOUD.CREATE_CREDENTIAL(
      credential_name => '<name_of_credential>',
      user_ocid       => 'ocid1.user.oc1...',
      tenancy_ocid    => 'ocid1.tenancy.oc1...',
      private_key     => '...',
      fingerprint     => 'ab:...'
   );
END;
/

-- -----------------------------------------------------------------------------
-- 2. Grant network ACL access to OCI Object Storage.
--    principal_name should be the ADW schema that executes DBMS_CLOUD calls.
-- -----------------------------------------------------------------------------
BEGIN
  DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'objectstorage.<region>.oraclecloud.com',
    ace  => xs$ace_type(
      privilege_list => xs$name_list('connect', 'resolve'),
      principal_name => '<principal_name>',
      principal_type => xs_acl.ptype_db
    )
  );
END;
/

-- Verify the ACL grant.
SELECT host, lower_port, upper_port, privilege, status
FROM user_host_aces
WHERE host = 'objectstorage.<region>.oraclecloud.com';

/*
Expected output:
objectstorage.<region>.oraclecloud.com    RESOLVE    GRANTED
objectstorage.<region>.oraclecloud.com    CONNECT    GRANTED
*/

-- -----------------------------------------------------------------------------
-- 3. Procedure used by the demo notebook.
--
-- Parameters:
--   p_table_name      Name of the ADW external table to create/recreate.
--   p_credential_name Name of the DBMS_CLOUD credential for Object Storage.
--   p_region          OCI region, for example us-ashburn-1.
--   p_oci_table_uri   Base OCI table URI without /metadata/vN.metadata.json,
--                     for example:
--                     oci://bucket@namespace/path/to/uniform_table
--
-- The procedure builds the HTTPS Object Storage path expected by ADW,
-- scans the metadata directory, finds the latest vN.metadata.json, and
-- recreates the ADW external table against that metadata file.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE refresh_iceberg_external_table (
    p_table_name      IN VARCHAR2,
    p_credential_name IN VARCHAR2,
    p_region          IN VARCHAR2,
    p_oci_table_uri   IN VARCHAR2
) AS
    l_uri_no_scheme     VARCHAR2(4000);
    l_bucket_and_ns     VARCHAR2(1000);
    l_table_path        VARCHAR2(3000);
    l_bucket            VARCHAR2(1000);
    l_namespace         VARCHAR2(1000);
    l_bucket_base_url   VARCHAR2(4000);
    l_metadata_folder   VARCHAR2(4000);
    l_object_name       VARCHAR2(4000);
    l_metadata_file_url VARCHAR2(4000);
    l_format            VARCHAR2(4000);
BEGIN
    l_format := '{"access_protocol":{"protocol_type":"iceberg"}}';

    -- Validate the incoming OCI URI shape early.
    IF p_oci_table_uri IS NULL
       OR NOT REGEXP_LIKE(p_oci_table_uri, '^oci://[^@/]+@[^/]+/.+$')
    THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid OCI table URI: ' || p_oci_table_uri);
    END IF;

    -- Parse bucket@namespace and the object path from the OCI URI.
    l_uri_no_scheme := SUBSTR(p_oci_table_uri, LENGTH('oci://') + 1);
    l_bucket_and_ns := REGEXP_SUBSTR(l_uri_no_scheme, '^[^/]+');
    l_table_path    := REGEXP_SUBSTR(l_uri_no_scheme, '/(.+)$', 1, 1, NULL, 1);

    l_bucket    := REGEXP_SUBSTR(l_bucket_and_ns, '^[^@]+');
    l_namespace := REGEXP_SUBSTR(l_bucket_and_ns, '@(.+)$', 1, 1, NULL, 1);

    -- Build the HTTPS Object Storage endpoint expected by DBMS_CLOUD.
    l_bucket_base_url :=
           'https://objectstorage.'
        || p_region
        || '.oraclecloud.com/n/'
        || l_namespace
        || '/b/'
        || l_bucket
        || '/o/';

    l_metadata_folder := l_bucket_base_url || l_table_path || '/metadata/';

    -- Find the highest vN.metadata.json in the metadata folder.
    SELECT object_name
    INTO l_object_name
    FROM (
        SELECT object_name,
               TO_NUMBER(
                   REGEXP_SUBSTR(
                       object_name,
                       '^v([0-9]+)\.metadata\.json$',
                       1,
                       1,
                       NULL,
                       1
                   )
               ) AS version_no
        FROM TABLE(
            DBMS_CLOUD.LIST_OBJECTS(
                credential_name => p_credential_name,
                location_uri    => l_metadata_folder
            )
        )
        WHERE REGEXP_LIKE(object_name, '^v[0-9]+\.metadata\.json$')
        ORDER BY version_no DESC
    )
    WHERE ROWNUM = 1;

    l_metadata_file_url :=
           l_bucket_base_url
        || UTL_URL.ESCAPE(l_table_path || '/metadata/' || l_object_name, TRUE, 'UTF-8');

    DBMS_OUTPUT.PUT_LINE('Using metadata file: ' || l_metadata_file_url);

    -- Recreate the external table so ADW points at the latest metadata file.
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);
    EXCEPTION
        WHEN OTHERS THEN
            -- ORA-00942 means the table does not exist yet; ignore that case.
            IF SQLCODE != -942 THEN
                RAISE;
            END IF;
    END;

    DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
        table_name      => p_table_name,
        credential_name => p_credential_name,
        file_uri_list   => l_metadata_file_url,
        format          => l_format
    );
END;
/

/*
Example call:

BEGIN
    refresh_iceberg_external_table(
        p_table_name      => '<table_name_in_adw>',
        p_credential_name => '<name_of_credential>',
        p_region          => '<region_name>',
        p_oci_table_uri   => 'oci://<bucket>@<namespace>/<table_path>'
    );
END;
/
*/
