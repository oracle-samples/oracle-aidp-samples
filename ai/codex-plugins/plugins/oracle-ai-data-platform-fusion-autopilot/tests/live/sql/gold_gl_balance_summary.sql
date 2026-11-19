-- TC10h-7 prep — gold mart for GL balance summary.
-- Aggregates GL period-end balances by COA segment + period for the CFO view.

CREATE OR REPLACE TABLE fusion_catalog.gold.gl_balance_summary
USING DELTA
AS
SELECT
    b.period_name,
    b.currency_code,
    b.balance_type_code,
    a.company,
    a.cost_center,
    a.account,
    COUNT(*)                          AS combination_count,
    SUM(b.period_net_dr)              AS total_period_dr,
    SUM(b.period_net_cr)              AS total_period_cr,
    SUM(b.period_net_dr - b.period_net_cr) AS net_period_movement,
    SUM(b.end_balance)                AS total_end_balance,
    AVG(b.end_balance)                AS avg_end_balance,
    MAX(b.end_balance)                AS max_end_balance,
    current_timestamp()               AS _gold_built_ts
FROM fusion_catalog.silver.fact_gl_balance b
JOIN fusion_catalog.silver.dim_account   a USING (code_combination_id)
WHERE a.enabled_flag = 'Y'
GROUP BY b.period_name, b.currency_code, b.balance_type_code,
         a.company, a.cost_center, a.account;

-- Validation
SELECT period_name, account, total_end_balance
FROM fusion_catalog.gold.gl_balance_summary
ORDER BY total_end_balance DESC
LIMIT 20;
