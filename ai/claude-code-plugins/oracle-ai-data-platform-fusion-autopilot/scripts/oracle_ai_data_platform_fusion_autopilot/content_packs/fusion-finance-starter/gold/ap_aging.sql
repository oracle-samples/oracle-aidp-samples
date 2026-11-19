WITH open_invoices AS (
  SELECT
    CAST(ap_invoices.ApInvoicesVendorId AS BIGINT)                 AS vendor_id,
    UPPER(CAST(ap_invoices.{{ column.invoice_currency_code }} AS STRING)) AS currency_code,
    CAST(ap_invoices.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))    AS invoice_amount,
    CAST(COALESCE(ap_invoices.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2)) AS amount_paid,
    CAST(ap_invoices.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))
      - CAST(COALESCE(ap_invoices.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2)) AS open_amount,
    CAST(ap_invoices.ApInvoicesInvoiceDate AS DATE)               AS invoice_date,
    ap_invoices._extract_ts                                       AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices
  WHERE ap_invoices.ApInvoicesVendorId IS NOT NULL
    AND ap_invoices.ApInvoicesInvoiceDate IS NOT NULL
    AND {{ semantic.cancelled_status }}
    AND CAST(ap_invoices.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))
      - CAST(COALESCE(ap_invoices.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2)) <> 0
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
  o.vendor_id                                                       AS vendor_id,
  o.currency_code                                                   AS currency_code,
  ds.supplier_number                                                AS supplier_number,
  ds.supplier_name                                                  AS supplier_name,
  ds.business_relationship                                          AS business_relationship,
  CASE
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 0   THEN 'current'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 30  THEN '1-30'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 60  THEN '31-60'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 90  THEN '61-90'
    ELSE '91+'
  END                                                               AS aging_bucket,
  'invoice_date'                                                    AS bucket_basis,
  COUNT(*)                                                          AS open_invoice_count,
  CAST(ROUND(SUM(o.open_amount), 2)        AS DECIMAL(28, 2))       AS open_amount,
  CAST(ROUND(SUM(o.invoice_amount), 2)     AS DECIMAL(28, 2))       AS invoice_amount_total,
  CAST(ROUND(SUM(o.amount_paid), 2)        AS DECIMAL(28, 2))       AS amount_paid_total,
  CAST(
    ROUND(SUM(CASE WHEN o.open_amount < 0 THEN o.open_amount ELSE 0 END), 2)
    AS DECIMAL(28, 2)
  )                                                                 AS credit_open_amount,
  SUM(CASE WHEN o.open_amount < 0 THEN 1 ELSE 0 END)                AS credit_open_count,
  MIN(o.invoice_date)                                               AS oldest_invoice_date,
  MAX(DATEDIFF({{ snapshot_date }}, o.invoice_date))                AS max_days_outstanding,
  CAST({{ snapshot_date }} AS DATE)                                 AS as_of_date,
  MAX(o.bronze_extract_ts)                                          AS bronze_extract_ts,
  current_timestamp()                                               AS gold_built_at,
  {{ run_id_literal }}                                              AS gold_run_id
FROM open_invoices o
LEFT JOIN supplier ds
  ON ds.vendor_id = o.vendor_id
GROUP BY
  o.vendor_id,
  o.currency_code,
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  CASE
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 0   THEN 'current'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 30  THEN '1-30'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 60  THEN '31-60'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 90  THEN '61-90'
    ELSE '91+'
  END
