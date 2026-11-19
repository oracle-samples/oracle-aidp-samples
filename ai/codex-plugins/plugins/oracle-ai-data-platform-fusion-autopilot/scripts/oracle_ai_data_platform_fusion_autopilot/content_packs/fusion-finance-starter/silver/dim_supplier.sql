SELECT
  xxhash64(CAST({{ column.supplier_natural_key }} AS STRING))      AS supplier_key,
  {{ column.supplier_natural_key }}                                AS supplier_number,
  COALESCE(
    NULLIF(AlternateNamePartyName, ''),
    NULLIF(AliasPartyName,         ''),
    NULLIF(TaxReportingName,       ''),
    CAST(NULL AS STRING)
  )                                                                AS supplier_name,
  NULLIF(CAST({{ column.vendor_id }} AS BIGINT), 0)                AS vendor_id,
  NULLIF(CAST(PARTYID          AS BIGINT), 0)                      AS party_id,
  NULLIF(CAST(PARENTVENDORID   AS BIGINT), 0)                      AS parent_vendor_id,
  NULLIF(CAST(PARENTPARTYID    AS BIGINT), 0)                      AS parent_party_id,
  BUSINESSRELATIONSHIP                                             AS business_relationship,
  CAST(ENDDATEACTIVE     AS DATE)                                  AS inactive_date,
  CAST(CREATIONDATE      AS TIMESTAMP)                             AS creation_date,
  CAST(LASTUPDATEDATE    AS TIMESTAMP)                             AS last_update_date,
  _extract_ts                                                      AS bronze_extract_ts,
  _source_pvo                                                      AS bronze_source_pvo,
  current_timestamp()                                              AS silver_built_at,
  {{ run_id_literal }}                                             AS silver_run_id
FROM (
  SELECT
    es.{{ column.supplier_natural_key }}   AS {{ column.supplier_natural_key }},
    es.{{ column.vendor_id }}              AS {{ column.vendor_id }},
    es.AlternateNamePartyName              AS AlternateNamePartyName,
    es.AliasPartyName                      AS AliasPartyName,
    es.TaxReportingName                    AS TaxReportingName,
    es.PARTYID                             AS PARTYID,
    es.PARENTVENDORID                      AS PARENTVENDORID,
    es.PARENTPARTYID                       AS PARENTPARTYID,
    es.BUSINESSRELATIONSHIP                AS BUSINESSRELATIONSHIP,
    es.ENDDATEACTIVE                       AS ENDDATEACTIVE,
    es.CREATIONDATE                        AS CREATIONDATE,
    es.LASTUPDATEDATE                      AS LASTUPDATEDATE,
    es._extract_ts                         AS _extract_ts,
    es._source_pvo                         AS _source_pvo,
    ROW_NUMBER() OVER (
      PARTITION BY es.{{ column.supplier_natural_key }} ORDER BY es._extract_ts DESC
    ) AS _rn
  FROM {{ catalog }}.{{ bronze_schema }}.erp_suppliers es
  WHERE es.{{ column.supplier_natural_key }} IS NOT NULL
    AND {{ watermark_predicate }}
)
WHERE _rn = 1
