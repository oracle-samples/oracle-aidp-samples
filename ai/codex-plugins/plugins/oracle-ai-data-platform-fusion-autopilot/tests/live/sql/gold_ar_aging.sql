-- TC10h-7 prep — gold mart for AR aging.
-- Computed downstream from silver fact_ar_invoice + fact_ar_receipt
-- (Fusion has no direct AR-Aging PVO).
--
-- Approach: aged-receivables = invoices - applied receipts, bucketed by
-- days-since-due-date for invoices that still have outstanding amount.
-- This mirrors the standard AR aging report shape.

CREATE OR REPLACE TABLE fusion_catalog.gold.ar_aging
USING DELTA
AS
WITH applied_per_invoice AS (
    -- AR receipts in demo-pod don't always join 1:1 to invoices via
    -- TransactionId; we approximate by aggregating receipts per customer
    -- and netting against invoice totals. For pixel-perfect aging,
    -- ar_receipt_application_extract_pvo (if available) gives the exact
    -- application linkage.
    SELECT
        customer_id,
        SUM(amount_applied) AS total_applied
    FROM fusion_catalog.silver.fact_ar_receipt
    WHERE amount_applied IS NOT NULL
    GROUP BY customer_id
),
customer_invoice_totals AS (
    SELECT
        customer_id,
        SUM(invoice_amount) AS total_invoiced,
        MAX(due_date)       AS latest_due_date,
        MIN(due_date)       AS earliest_due_date
    FROM fusion_catalog.silver.fact_ar_invoice
    WHERE transaction_status NOT IN ('VOIDED', 'CANCELLED')
    GROUP BY customer_id
),
customer_outstanding AS (
    SELECT
        c.customer_id,
        c.total_invoiced,
        COALESCE(a.total_applied, 0) AS total_applied,
        c.total_invoiced - COALESCE(a.total_applied, 0) AS outstanding_amount,
        DATEDIFF(current_date(), c.latest_due_date) AS days_since_latest_due
    FROM customer_invoice_totals c
    LEFT JOIN applied_per_invoice a USING (customer_id)
    WHERE c.total_invoiced - COALESCE(a.total_applied, 0) > 0
),
aged AS (
    SELECT
        CASE
            WHEN days_since_latest_due IS NULL OR days_since_latest_due <= 0 THEN 'Current'
            WHEN days_since_latest_due <= 30  THEN '1-30 days'
            WHEN days_since_latest_due <= 60  THEN '31-60 days'
            WHEN days_since_latest_due <= 90  THEN '61-90 days'
            WHEN days_since_latest_due <= 180 THEN '91-180 days'
            WHEN days_since_latest_due <= 365 THEN '181-365 days'
            ELSE '365+ days'
        END AS age_bucket,
        customer_id,
        outstanding_amount
    FROM customer_outstanding
)
SELECT
    age_bucket,
    COUNT(*)                          AS customer_count,
    SUM(outstanding_amount)           AS total_outstanding,
    AVG(outstanding_amount)           AS avg_outstanding,
    MAX(outstanding_amount)           AS max_outstanding,
    current_timestamp()               AS _gold_built_ts
FROM aged
GROUP BY age_bucket;

-- Validation
SELECT * FROM fusion_catalog.gold.ar_aging
ORDER BY CASE age_bucket
    WHEN 'Current'      THEN 0
    WHEN '1-30 days'    THEN 1
    WHEN '31-60 days'   THEN 2
    WHEN '61-90 days'   THEN 3
    WHEN '91-180 days'  THEN 4
    WHEN '181-365 days' THEN 5
    ELSE 6
END;
