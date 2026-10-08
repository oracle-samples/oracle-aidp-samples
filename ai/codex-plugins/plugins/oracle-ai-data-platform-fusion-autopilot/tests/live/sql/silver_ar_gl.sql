-- TC10h-7 (2026-05-05): silver fact tables for AR + GL.
-- Run AFTER BICC extracts have populated:
--   fusion_catalog.bronze.ar_invoices         (TransactionHeaderExtractPVO)
--   fusion_catalog.bronze.ar_receipts         (ReceiptHeaderExtractPVO)
--   fusion_catalog.bronze.gl_period_balances  (BalanceExtractPVO)
--   fusion_catalog.bronze.gl_coa              (CodeCombinationExtractPVO)
--
-- Column names below are the ACTUAL ones returned by demo-pod BICC
-- (<fusion-pod-2> pod, 2026-05-05) — the AM-prefix scheme: RaCustomerTrx*,
-- ArCashReceipt*, Balance*, CodeCombination*. Confirmed live.

------------------------------------------------------------------------------
-- silver.fact_ar_invoice
------------------------------------------------------------------------------
CREATE OR REPLACE TABLE fusion_catalog.silver.fact_ar_invoice
USING DELTA
AS
SELECT
    CAST(RaCustomerTrxCustomerTrxId       AS DECIMAL(18,0)) AS transaction_id,
    CAST(RaCustomerTrxBillToCustomerId    AS DECIMAL(18,0)) AS customer_id,
    CAST(RaCustomerTrxBillToSiteUseId     AS DECIMAL(18,0)) AS customer_site_id,
    CAST(RaCustomerTrxInvoiceCurrencyCode AS STRING)        AS currency,
    CAST(RaCustomerTrxStatusTrx           AS STRING)        AS status,
    CAST(RaCustomerTrxBillingDate         AS DATE)          AS billing_date,
    CAST(RaCustomerTrxTermDueDate         AS DATE)          AS due_date,
    CAST(RaCustomerTrxCreationDate        AS TIMESTAMP)     AS created_at,
    CAST(RaCustomerTrxCompleteFlag        AS STRING)        AS complete_flag,
    CAST(RaCustomerTrxBrAmount            AS DECIMAL(20,2)) AS br_amount
FROM fusion_catalog.bronze.ar_invoices
WHERE RaCustomerTrxCustomerTrxId IS NOT NULL;

-- Note on data quality (<fusion-pod-2> pod, 2026-05-05): of 187,970 AR transaction
-- headers, only 923 carry due_date and 270 carry billing_date — the bulk of
-- aging-driving timestamps live on `ReceivablesPaymentScheduleExtractPVO`
-- (per-installment), not the header. The bundle's gold.ar_aging therefore
-- buckets by `created_at` as a header-level age proxy. For pixel-perfect
-- aging, add the schedule PVO to the extract list.

------------------------------------------------------------------------------
-- silver.fact_ar_receipt
------------------------------------------------------------------------------
CREATE OR REPLACE TABLE fusion_catalog.silver.fact_ar_receipt
USING DELTA
AS
SELECT
    CAST(ArCashReceiptCashReceiptId       AS DECIMAL(18,0)) AS receipt_id,
    CAST(ArCashReceiptCustomerSiteUseId   AS DECIMAL(18,0)) AS customer_site_id,
    CAST(ArCashReceiptCurrencyCode        AS STRING)        AS currency,
    CAST(ArCashReceiptAmount              AS DECIMAL(20,2)) AS amount,
    CAST(ArCashReceiptStatus              AS STRING)        AS status,
    CAST(ArCashReceiptActualValueDate     AS DATE)          AS value_date,
    CAST(ArCashReceiptDepositDate         AS DATE)          AS deposit_date,
    CAST(ArCashReceiptCreationDate        AS TIMESTAMP)     AS created_at
FROM fusion_catalog.bronze.ar_receipts
WHERE ArCashReceiptCashReceiptId IS NOT NULL;

------------------------------------------------------------------------------
-- silver.fact_gl_balance — actual balances only (filter ActualFlag='A')
------------------------------------------------------------------------------
CREATE OR REPLACE TABLE fusion_catalog.silver.fact_gl_balance
USING DELTA
AS
SELECT
    CAST(BalanceLedgerId          AS DECIMAL(18,0)) AS ledger_id,
    CAST(BalanceCodeCombinationId AS DECIMAL(18,0)) AS code_combination_id,
    CAST(BalancePeriodName        AS STRING)        AS period_name,
    CAST(BalancePeriodNum         AS INT)           AS period_num,
    CAST(BalancePeriodYear        AS INT)           AS period_year,
    CAST(BalanceCurrencyCode      AS STRING)        AS currency,
    CAST(BalanceActualFlag        AS STRING)        AS actual_flag,
    CAST(BalancePeriodNetDr       AS DECIMAL(20,2)) AS period_net_dr,
    CAST(BalancePeriodNetCr       AS DECIMAL(20,2)) AS period_net_cr,
    CAST(BalanceBeginBalanceDr    AS DECIMAL(20,2)) AS begin_balance_dr,
    CAST(BalanceBeginBalanceCr    AS DECIMAL(20,2)) AS begin_balance_cr,
    CAST(BalanceProjectToDateDr   AS DECIMAL(20,2)) AS ptd_dr,
    CAST(BalanceProjectToDateCr   AS DECIMAL(20,2)) AS ptd_cr
FROM fusion_catalog.bronze.gl_period_balances
WHERE BalanceActualFlag = 'A'
  AND BalanceCodeCombinationId IS NOT NULL;

------------------------------------------------------------------------------
-- silver.dim_account (from gl_coa)
------------------------------------------------------------------------------
CREATE OR REPLACE TABLE fusion_catalog.silver.dim_account
USING DELTA
AS
SELECT
    CAST(CodeCombinationCodeCombinationId AS DECIMAL(18,0)) AS code_combination_id,
    CAST(CodeCombinationChartOfAccountsId AS DECIMAL(18,0)) AS coa_id,
    CAST(CodeCombinationSegment1          AS STRING)        AS company,
    CAST(CodeCombinationSegment2          AS STRING)        AS cost_center,
    CAST(CodeCombinationSegment3          AS STRING)        AS account,
    CAST(CodeCombinationSegment4          AS STRING)        AS sub_account,
    CAST(CodeCombinationSegment5          AS STRING)        AS product,
    CAST(CodeCombinationEnabledFlag       AS STRING)        AS enabled_flag,
    CAST(CodeCombinationStartDateActive   AS DATE)          AS start_date_active,
    CAST(CodeCombinationEndDateActive     AS DATE)          AS end_date_active
FROM fusion_catalog.bronze.gl_coa
WHERE CodeCombinationCodeCombinationId IS NOT NULL;
