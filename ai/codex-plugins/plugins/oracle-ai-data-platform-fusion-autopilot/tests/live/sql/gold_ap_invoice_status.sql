-- TC10h-5 (2026-05-03): gold mart for AP invoice approval status
-- Source: fusion_catalog.silver.fact_ap_invoice (49,985 rows from demo-pod BICC extract)
-- Output: 8 status buckets aggregated; APPROVED $3.19B, NEVER APPROVED $16.7M, etc.

CREATE OR REPLACE TABLE fusion_catalog.gold.ap_invoice_status
USING DELTA
AS
SELECT
    f.approval_status,
    COUNT(*)                              AS invoice_count,
    COUNT(DISTINCT f.vendor_id)           AS supplier_count,
    SUM(f.invoice_amount)                 AS total_invoice_amount,
    SUM(f.amount_paid)                    AS total_paid,
    SUM(f.invoice_amount - f.amount_paid) AS total_outstanding,
    AVG(f.invoice_amount)                 AS avg_invoice_amount,
    MIN(f.gl_date)                        AS first_gl_date,
    MAX(f.gl_date)                        AS last_gl_date,
    current_timestamp()                   AS _gold_built_ts
FROM fusion_catalog.silver.fact_ap_invoice f
GROUP BY f.approval_status;
