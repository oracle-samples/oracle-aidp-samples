WITH invoices AS (
  SELECT
    UPPER(CAST(a.ApInvoicesInvoiceCurrencyCode AS STRING)) AS currency_code,
    CAST(a.ApInvoicesVendorId AS BIGINT) AS vendor_id,
    CAST(a.ApInvoicesInvoiceAmount AS DECIMAL(20, 2)) AS invoice_amount,
    CAST(a.ApInvoicesAmountPaid AS DECIMAL(20, 2)) AS amount_paid,
    a._extract_ts AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices a
  WHERE a.ApInvoicesInvoiceCurrencyCode IS NOT NULL
),
with_supplier AS (
  SELECT
    i.currency_code,
    s.vendor_id,
    i.invoice_amount,
    i.amount_paid,
    i.bronze_extract_ts
  FROM invoices i
  LEFT JOIN (
    SELECT vendor_id
    FROM (
      SELECT vendor_id,
             ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY supplier_number) AS _rn
      FROM {{ catalog }}.{{ silver_schema }}.dim_supplier
      WHERE vendor_id IS NOT NULL
    )
    WHERE _rn = 1
  ) s
    ON i.vendor_id = s.vendor_id
)
SELECT
  currency_code,
  COUNT(DISTINCT vendor_id) AS supplier_count,
  COUNT(*) AS invoice_count,
  CAST(ROUND(SUM(COALESCE(invoice_amount, 0)), 2) AS DECIMAL(20, 2)) AS total_invoice_amount,
  CAST(ROUND(SUM(COALESCE(amount_paid, 0)), 2) AS DECIMAL(20, 2)) AS total_paid,
  MAX(bronze_extract_ts) AS bronze_extract_ts,
  current_timestamp() AS gold_built_at,
  {{ run_id_literal }} AS gold_run_id
FROM with_supplier
GROUP BY currency_code
