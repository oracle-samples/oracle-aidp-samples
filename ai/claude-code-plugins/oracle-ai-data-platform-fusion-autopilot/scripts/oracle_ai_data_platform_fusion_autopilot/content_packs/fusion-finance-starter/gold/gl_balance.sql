WITH balances AS (
  SELECT
    b.BalanceLedgerId,
    b.BalanceCodeCombinationId,
    b.BalancePeriodYear,
    b.BalancePeriodNum,
    b.BalancePeriodName,
    b.BalanceCurrencyCode,
    b.BalanceActualFlag,
    b.BalanceTranslatedFlag,
    b._extract_ts                                                   AS bronze_extract_ts,
    CAST(b.BalanceBeginBalanceDr AS DECIMAL(28, 2))                 AS begin_balance_dr,
    CAST(b.BalanceBeginBalanceCr AS DECIMAL(28, 2))                 AS begin_balance_cr,
    CAST(b.BalancePeriodNetDr    AS DECIMAL(28, 2))                 AS period_net_dr,
    CAST(b.BalancePeriodNetCr    AS DECIMAL(28, 2))                 AS period_net_cr
  FROM {{ catalog }}.{{ bronze_schema }}.gl_period_balances b
  WHERE b.BalanceActualFlag = 'A'
    AND b.BalanceCodeCombinationId IS NOT NULL
    AND {{ watermark_predicate }}
)
SELECT
  CAST(b.BalanceLedgerId            AS BIGINT)                      AS ledger_id,
  CAST(b.BalanceCodeCombinationId   AS BIGINT)                      AS account_id,
  da.code_combination                                               AS code_combination,
  da.account_type                                                   AS account_type,
  da.company                                                        AS company,
  da.cost_center                                                    AS cost_center,
  da.account                                                        AS natural_account,
  da.subaccount                                                     AS subaccount,
  da.product                                                        AS product,
  da.intercompany                                                   AS intercompany,
  CAST(b.BalancePeriodYear          AS BIGINT)                      AS period_year,
  CAST(b.BalancePeriodNum           AS BIGINT)                       AS period_num,
  b.BalancePeriodName                                               AS period_name,
  b.BalanceCurrencyCode                                             AS currency_code,
  b.BalanceActualFlag                                               AS actual_flag,
  b.BalanceTranslatedFlag                                           AS translated_flag,
  b.begin_balance_dr                                                AS begin_balance_dr,
  b.begin_balance_cr                                                AS begin_balance_cr,
  b.period_net_dr                                                   AS period_net_dr,
  b.period_net_cr                                                   AS period_net_cr,
  CAST(
    ROUND(
        COALESCE(b.begin_balance_dr, 0)
      - COALESCE(b.begin_balance_cr, 0)
      + COALESCE(b.period_net_dr,    0)
      - COALESCE(b.period_net_cr,    0),
      2
    )
    AS DECIMAL(28, 2)
  )                                                                 AS closing_balance,
  b.bronze_extract_ts                                               AS bronze_extract_ts,
  current_timestamp()                                               AS gold_built_at,
  {{ run_id_literal }}                                              AS gold_run_id
FROM balances b
LEFT JOIN {{ catalog }}.{{ silver_schema }}.dim_account da
  ON da.account_id = CAST(b.BalanceCodeCombinationId AS BIGINT)
