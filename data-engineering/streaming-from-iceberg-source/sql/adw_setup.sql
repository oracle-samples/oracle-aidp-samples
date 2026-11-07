-- One-time ADW setup for the streaming-from-iceberg-source sample.
-- Oracle AI Database 23ai or later (the notebooks use CREATE TABLE IF NOT EXISTS).

-- =================================================================================================
-- Part 1: run as ADMIN (or another user that can create users).
-- A dedicated schema that the notebooks log in as. Replace <STRONG_PASSWORD> and store the same
-- value in the AIDP Credential Store (README, "Store the ADW secrets").
-- =================================================================================================

CREATE USER iceberg_stream IDENTIFIED BY "<STRONG_PASSWORD>"
  QUOTA UNLIMITED ON data;

-- Log in, and create the tables the consumer stream merges into.
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO iceberg_stream;

-- DBMS_CLOUD: the Object Storage credential and the Iceberg external table.
GRANT EXECUTE ON DBMS_CLOUD TO iceberg_stream;

-- Autonomous Database needs this directory both to create and to query external tables.
GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO iceberg_stream;


-- =================================================================================================
-- Part 2: run as ICEBERG_STREAM.
-- A credential that lets ADW read the bucket holding the Iceberg table. It is an OCI API key of a
-- user who may read that bucket ("read objects" on it, "inspect buckets"). Paste the private
-- key without its BEGIN/END lines. Notebook 2 refers to it as ADW_OS_CREDENTIAL.
-- =================================================================================================

BEGIN
  DBMS_CLOUD.CREATE_CREDENTIAL(
    credential_name => 'OCI_ICEBERG_CRED',
    user_ocid       => '<user OCID>',
    tenancy_ocid    => '<tenancy OCID>',
    private_key     => '<private key, one line, without the BEGIN/END lines>',
    fingerprint     => '<key fingerprint>');
END;
/

-- Check that ADW can list the warehouse folder.
SELECT object_name
FROM   DBMS_CLOUD.LIST_OBJECTS('OCI_ICEBERG_CRED',
         'https://objectstorage.<region>.oraclecloud.com/n/<namespace>/b/<bucket>/o/iceberg-streaming/warehouse/')
FETCH FIRST 5 ROWS ONLY;


-- =================================================================================================
-- Queries to try in ADW once notebooks 1 and 2 run (as ICEBERG_STREAM).
-- =================================================================================================

-- The Iceberg source, read in place: every query sees the current snapshot, no refresh needed.
SELECT COUNT(*) AS events, MAX(event_id) AS last_event_id FROM order_events_iceberg;

-- The native table the stream maintains, and its batch log.
-- Deleted orders stay as tombstones (is_deleted = 'Y'), so that older events cannot bring them back.
SELECT is_deleted, status, COUNT(*) AS orders FROM orders GROUP BY is_deleted, status ORDER BY 1, 2;
SELECT batch_id, rows_in, rows_merged, applied_at
FROM   stream_batch_log WHERE query_name = 'orders_to_adw' ORDER BY batch_id DESC FETCH FIRST 10 ROWS ONLY;

-- Both together: orders whose state in ORDERS differs from the newest event in the Iceberg table.
-- Expect recent orders here while events are in flight; none once the stream has caught up.
WITH latest AS (
  SELECT order_id, region, status, amount, event_id
  FROM  (SELECT e.order_id, e.region, e.status, e.amount, e.event_id, e.op,
                ROW_NUMBER() OVER (PARTITION BY e.order_id ORDER BY e.event_id DESC) AS rn
         FROM order_events_iceberg e)
  WHERE rn = 1 AND op <> 'D')
SELECT l.order_id, l.status AS source_status, o.status AS adw_status, l.event_id, o.last_event_id
FROM   latest l FULL OUTER JOIN (SELECT * FROM orders WHERE is_deleted = 'N') o ON o.order_id = l.order_id
WHERE  o.order_id IS NULL OR l.order_id IS NULL
   OR  o.last_event_id <> l.event_id
   OR  DECODE(o.region, l.region, 0, 1) = 1
   OR  DECODE(o.status, l.status, 0, 1) = 1
   OR  DECODE(o.amount, l.amount, 0, 1) = 1
FETCH FIRST 20 ROWS ONLY;
