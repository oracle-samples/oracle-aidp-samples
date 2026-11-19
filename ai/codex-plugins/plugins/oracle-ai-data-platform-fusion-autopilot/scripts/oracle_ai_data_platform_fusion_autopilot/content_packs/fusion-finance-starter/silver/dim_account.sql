SELECT
  xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))       AS account_key,
  CAST(CodeCombinationCodeCombinationId AS BIGINT)                 AS account_id,
  CAST(CodeCombinationChartOfAccountsId AS BIGINT)                 AS chart_of_accounts_id,
  CONCAT_WS('.',
    COALESCE(CodeCombinationSegment1, ''), COALESCE(CodeCombinationSegment2, ''),
    COALESCE(CodeCombinationSegment3, ''), COALESCE(CodeCombinationSegment4, ''),
    COALESCE(CodeCombinationSegment5, ''), COALESCE(CodeCombinationSegment6, '')
  )                                                                AS code_combination,
  CodeCombinationSegment1                                          AS segment_01,
  CodeCombinationSegment2                                          AS segment_02,
  CodeCombinationSegment3                                          AS segment_03,
  CodeCombinationSegment4                                          AS segment_04,
  CodeCombinationSegment5                                          AS segment_05,
  CodeCombinationSegment6                                          AS segment_06,
  {{ coa.balancing }}                                              AS company,
  {{ coa.cost_center }}                                            AS cost_center,
  {{ coa.natural_account }}                                        AS account,
  CodeCombinationSegment4                                          AS subaccount,
  CodeCombinationSegment5                                          AS product,
  CodeCombinationSegment6                                          AS intercompany,
  CodeCombinationAccountType                                       AS account_type,
  CodeCombinationEnabledFlag                                       AS enabled_flag,
  CodeCombinationSummaryFlag                                       AS summary_flag,
  CodeCombinationDetailPostingAllowedFlag                          AS detail_posting_allowed_flag,
  CodeCombinationFinancialCategory                                 AS financial_category,
  CodeCombinationStartDateActive                                   AS start_date_active,
  CodeCombinationEndDateActive                                     AS end_date_active,
  _extract_ts                                                      AS bronze_extract_ts,
  _source_pvo                                                      AS bronze_source_pvo,
  current_timestamp()                                              AS silver_built_at,
  {{ run_id_literal }}                                             AS silver_run_id
FROM (
  SELECT
    coa.CodeCombinationCodeCombinationId       AS CodeCombinationCodeCombinationId,
    coa.CodeCombinationChartOfAccountsId       AS CodeCombinationChartOfAccountsId,
    coa.CodeCombinationSegment1                AS CodeCombinationSegment1,
    coa.CodeCombinationSegment2                AS CodeCombinationSegment2,
    coa.CodeCombinationSegment3                AS CodeCombinationSegment3,
    coa.CodeCombinationSegment4                AS CodeCombinationSegment4,
    coa.CodeCombinationSegment5                AS CodeCombinationSegment5,
    coa.CodeCombinationSegment6                AS CodeCombinationSegment6,
    coa.CodeCombinationAccountType             AS CodeCombinationAccountType,
    coa.CodeCombinationEnabledFlag             AS CodeCombinationEnabledFlag,
    coa.CodeCombinationSummaryFlag             AS CodeCombinationSummaryFlag,
    coa.CodeCombinationDetailPostingAllowedFlag AS CodeCombinationDetailPostingAllowedFlag,
    coa.CodeCombinationFinancialCategory       AS CodeCombinationFinancialCategory,
    coa.CodeCombinationStartDateActive         AS CodeCombinationStartDateActive,
    coa.CodeCombinationEndDateActive           AS CodeCombinationEndDateActive,
    coa._extract_ts                            AS _extract_ts,
    coa._source_pvo                            AS _source_pvo,
    ROW_NUMBER() OVER (
      PARTITION BY coa.CodeCombinationCodeCombinationId
      ORDER BY coa._extract_ts DESC
    ) AS _rn
  FROM {{ catalog }}.{{ bronze_schema }}.gl_coa coa
  WHERE coa.CodeCombinationCodeCombinationId IS NOT NULL
    AND {{ watermark_predicate }}
)
WHERE _rn = 1
