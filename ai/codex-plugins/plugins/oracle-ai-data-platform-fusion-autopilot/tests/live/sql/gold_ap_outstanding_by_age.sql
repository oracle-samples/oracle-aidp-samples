-- TC10h-6 (2026-05-03): gold mart bucketing UNPAID AP invoices by gl_date age
-- Source: fusion_catalog.silver.fact_ap_invoice
-- Output: 6 age buckets (0-30, 31-60, 61-90, 91-180, 181-365, 365+ days)
-- Real aging on real demo-pod data: $304M outstanding in 365+ bucket (9,556 invoices, 75 suppliers).

CREATE OR REPLACE TABLE fusion_catalog.gold.ap_outstanding_by_age
USING DELTA
AS
WITH outstanding AS (
    SELECT
        f.vendor_id,
        f.gl_date,
        f.approval_status,
        f.invoice_amount - COALESCE(f.amount_paid, 0) AS outstanding_amount,
        DATEDIFF(current_date(), f.gl_date)           AS days_since_gl
    FROM fusion_catalog.silver.fact_ap_invoice f
    WHERE f.invoice_amount > COALESCE(f.amount_paid, 0)
      AND f.approval_status NOT IN ('CANCELLED')
      AND f.gl_date IS NOT NULL
)
SELECT
    CASE
        WHEN days_since_gl <= 30  THEN '0-30 days'
        WHEN days_since_gl <= 60  THEN '31-60 days'
        WHEN days_since_gl <= 90  THEN '61-90 days'
        WHEN days_since_gl <= 180 THEN '91-180 days'
        WHEN days_since_gl <= 365 THEN '181-365 days'
        ELSE '365+ days'
    END                                       AS age_bucket,
    COUNT(*)                                  AS invoice_count,
    COUNT(DISTINCT vendor_id)                 AS supplier_count,
    SUM(outstanding_amount)                   AS total_outstanding,
    AVG(outstanding_amount)                   AS avg_outstanding,
    MAX(outstanding_amount)                   AS max_outstanding,
    current_timestamp()                       AS _gold_built_ts
FROM outstanding
GROUP BY 1;

-- Note: ages are based on gl_date because demo-pod's fact_ap_invoice does
-- not expose due_date. A true production aging would use due_date when available.
