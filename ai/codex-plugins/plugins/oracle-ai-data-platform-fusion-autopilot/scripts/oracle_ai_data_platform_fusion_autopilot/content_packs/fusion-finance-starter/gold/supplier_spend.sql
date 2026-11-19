WITH invoices AS (
  SELECT
    CAST(inv.ApInvoicesVendorId AS BIGINT)                          AS vendor_id,
    UPPER(CAST(inv.{{ column.invoice_currency_code }} AS STRING))   AS currency_code,
    inv.ApInvoicesApprovalStatus,
    inv.ApInvoicesInvoiceAmount,
    inv.ApInvoicesAmountPaid,
    CAST(inv.ApInvoicesInvoiceDate AS DATE)                         AS invoice_date,
    inv._extract_ts                                                 AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv
  WHERE inv.ApInvoicesVendorId IS NOT NULL
    AND {{ watermark_predicate }}
),
supplier AS (
  SELECT vendor_id, supplier_number, supplier_name, business_relationship
  FROM (
    SELECT
      ds.vendor_id, ds.supplier_number, ds.supplier_name, ds.business_relationship,
      ROW_NUMBER() OVER (PARTITION BY ds.vendor_id ORDER BY ds.supplier_number) AS _rn
    FROM {{ catalog }}.{{ silver_schema }}.dim_supplier ds
    WHERE ds.vendor_id IS NOT NULL
  )
  WHERE _rn = 1
)
SELECT
  inv.vendor_id                                                     AS vendor_id,
  inv.currency_code                                                 AS currency_code,
  ds.supplier_number                                                AS supplier_number,
  ds.supplier_name                                                  AS supplier_name,
  ds.business_relationship                                          AS business_relationship,
  inv.ApInvoicesApprovalStatus                                      AS approval_status,
  COUNT(*)                                                          AS invoice_count,
  CAST(
    ROUND(SUM(COALESCE(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(20, 2)), 0)), 2)
    AS DECIMAL(20, 2)
  )                                                                 AS total_invoice_amount,
  CAST(
    ROUND(SUM(COALESCE(CAST(inv.ApInvoicesAmountPaid    AS DECIMAL(20, 2)), 0)), 2)
    AS DECIMAL(20, 2)
  )                                                                 AS total_paid,
  MAX(inv.invoice_date)                                             AS last_invoice_date,
  MAX(inv.bronze_extract_ts)                                        AS bronze_extract_ts,
  current_timestamp()                                               AS gold_built_at,
  {{ run_id_literal }}                                              AS gold_run_id
FROM invoices inv
LEFT JOIN supplier ds
  ON ds.vendor_id = inv.vendor_id
GROUP BY
  inv.vendor_id,
  inv.currency_code,
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  inv.ApInvoicesApprovalStatus
