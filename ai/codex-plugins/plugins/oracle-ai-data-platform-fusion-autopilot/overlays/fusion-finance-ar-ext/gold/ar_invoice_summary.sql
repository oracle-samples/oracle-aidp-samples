WITH ar AS (
  SELECT
    UPPER(CAST(t.RaCustomerTrxInvoiceCurrencyCode AS STRING))   AS currency_code,
    TRUNC(CAST(t.RaCustomerTrxBillingDate AS DATE), 'MM')       AS billing_month,
    CAST(t.RaCustomerTrxOrgId AS BIGINT)                        AS org_id,
    CAST(t.RaCustomerTrxBrAmount AS DECIMAL(20, 2))             AS br_amount,
    t._extract_ts                                               AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ar_invoices t
  WHERE t.RaCustomerTrxInvoiceCurrencyCode IS NOT NULL
)
SELECT
  ar.currency_code                                              AS currency_code,
  ar.billing_month                                              AS billing_month,
  ar.org_id                                                     AS org_id,
  COUNT(*)                                                      AS invoice_count,
  CAST(ROUND(SUM(COALESCE(ar.br_amount, 0)), 2) AS DECIMAL(20, 2)) AS total_br_amount,
  MAX(ar.bronze_extract_ts)                                     AS bronze_extract_ts,
  current_timestamp()                                           AS gold_built_at,
  {{ run_id_literal }}                                          AS gold_run_id
FROM ar
GROUP BY ar.currency_code, ar.billing_month, ar.org_id
