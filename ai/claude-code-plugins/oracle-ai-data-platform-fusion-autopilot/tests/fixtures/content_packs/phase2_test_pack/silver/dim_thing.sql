SELECT
  thing_id,
  thing_name,
  _extract_ts
FROM {{ catalog }}.{{ bronze_schema }}.erp_thing
WHERE {{ watermark_predicate }}
