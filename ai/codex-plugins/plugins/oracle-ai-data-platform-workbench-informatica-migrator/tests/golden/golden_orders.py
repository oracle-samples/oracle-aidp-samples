"""Golden expected output for m_ORDER_CONSOLIDATION mapping.

Source: /Users/veerao/Downloads/infa-test-2.xml
Mapping: m_ORDER_CONSOLIDATION
Flow: 3 parallel source branches (WEB, STORE, PARTNER) -> UNION -> LKP -> FIL
      -> SQL_TAX -> EXP_ENRICH -> SEQ -> RTR -> FACT_ORDERS / ORDER_ERRORS
"""

EXPECTED_CELLS = [
    # ── Cell 0: Imports ──
    """\
from pyspark.sql import functions as F
from pyspark.sql.window import Window""",

    # ── Cell 1: Read WEB_ORDERS source ──
    """\
# Source: WEB_ORDERS (Oracle: ECOMM_STG.WEB_ORDERS)
df_web_raw = spark.read.table("WEB_ORDERS")""",

    # ── Cell 2: SQ_WEB_ORDERS — source qualifier with SQL filter ──
    """\
# SQ_WEB_ORDERS: Source Qualifier
df_web_sq = df_web_raw.select(
    "ORDER_ID",
    "CUSTOMER_ID",
    "ORDER_DATE",
    "ORDER_STATUS",
    "ORDER_TOTAL",
    "CURRENCY_CODE",
    "PROMO_CODE",
    "SHIPPING_ADDR",
    "PAYMENT_METHOD",
)""",

    # ── Cell 3: EXP_NORM_WEB — normalize web orders to common schema ──
    """\
# EXP_NORM_WEB: Map web order fields to canonical schema
df_web = df_web_sq.select(
    F.lit("WEB").alias("SOURCE_SYSTEM"),
    F.col("ORDER_ID").cast("string").alias("SOURCE_ORDER_ID"),
    F.col("CUSTOMER_ID"),
    F.col("ORDER_DATE"),
    F.upper(F.col("ORDER_STATUS")).alias("ORDER_STATUS"),
    F.col("ORDER_TOTAL").alias("ORDER_AMOUNT"),
    F.col("CURRENCY_CODE"),
    F.col("PAYMENT_METHOD"),
    F.when(F.isnull(F.col("PROMO_CODE")), F.lit("WEB_DIRECT"))
     .otherwise(F.concat(F.lit("WEB_PROMO:"), F.col("PROMO_CODE")))
     .alias("CHANNEL_DETAIL"),
)""",

    # ── Cell 4: Read STORE_ORDERS source ──
    """\
# Source: STORE_ORDERS (Oracle: POS_STG.STORE_ORDERS)
df_store_raw = spark.read.table("STORE_ORDERS")""",

    # ── Cell 5: SQ_STORE_ORDERS — source qualifier ──
    """\
# SQ_STORE_ORDERS: Source Qualifier
df_store_sq = df_store_raw.select(
    "TXN_ID",
    "MEMBER_ID",
    "STORE_ID",
    "TXN_DATE",
    "TXN_STATUS",
    "TXN_AMOUNT",
    "CURRENCY_CODE",
    "REGISTER_ID",
    "PAYMENT_TYPE",
)""",

    # ── Cell 6: EXP_NORM_STORE — normalize store orders to common schema ──
    """\
# EXP_NORM_STORE: Map store transaction fields to canonical schema
df_store = df_store_sq.select(
    F.lit("STORE").alias("SOURCE_SYSTEM"),
    F.concat(F.lit("S-"), F.col("TXN_ID").cast("string")).alias("SOURCE_ORDER_ID"),
    F.col("MEMBER_ID").alias("CUSTOMER_ID"),
    F.col("TXN_DATE").alias("ORDER_DATE"),
    F.when(F.col("TXN_STATUS") == "COMPLETED", F.lit("COMPLETED"))
     .when(F.col("TXN_STATUS") == "VOIDED", F.lit("CANCELLED"))
     .when(F.col("TXN_STATUS") == "RETURNED", F.lit("RETURNED"))
     .otherwise(F.lit("PENDING"))
     .alias("ORDER_STATUS"),
    F.col("TXN_AMOUNT").alias("ORDER_AMOUNT"),
    F.col("CURRENCY_CODE"),
    F.col("PAYMENT_TYPE").alias("PAYMENT_METHOD"),
    F.concat(
        F.lit("STORE_"),
        F.col("STORE_ID").cast("string"),
        F.lit("_REG_"),
        F.col("REGISTER_ID").cast("string"),
    ).alias("CHANNEL_DETAIL"),
)""",

    # ── Cell 7: Read PARTNER_ORDERS source ──
    """\
# Source: PARTNER_ORDERS (Flat File)
df_partner_raw = spark.read.table("PARTNER_ORDERS")""",

    # ── Cell 8: SQ_PARTNER_ORDERS — source qualifier ──
    """\
# SQ_PARTNER_ORDERS: Source Qualifier
df_partner_sq = df_partner_raw.select(
    "PARTNER_ORDER_ID",
    "PARTNER_NAME",
    "CUSTOMER_ID",
    "ORDER_DATE",
    "STATUS",
    "TOTAL_AMOUNT",
    "CURRENCY",
    "ITEM_COUNT",
    "PAYMENT_METHOD",
    "COMMISSION_PCT",
)""",

    # ── Cell 9: EXP_NORM_PARTNER — normalize partner orders to common schema ──
    """\
# EXP_NORM_PARTNER: Map partner order fields to canonical schema
df_partner = df_partner_sq.select(
    F.lit("PARTNER").alias("SOURCE_SYSTEM"),
    F.col("PARTNER_ORDER_ID").alias("SOURCE_ORDER_ID"),
    F.col("CUSTOMER_ID"),
    F.col("ORDER_DATE"),
    F.upper(F.col("STATUS")).alias("ORDER_STATUS"),
    (F.col("TOTAL_AMOUNT") * (F.lit(1) - F.col("COMMISSION_PCT") / F.lit(100))).alias("ORDER_AMOUNT"),
    F.col("CURRENCY").alias("CURRENCY_CODE"),
    F.col("PAYMENT_METHOD"),
    F.concat(F.lit("PARTNER:"), F.col("PARTNER_NAME")).alias("CHANNEL_DETAIL"),
)""",

    # ── Cell 10: UNI_ALL_ORDERS — union all 3 branches ──
    """\
# UNI_ALL_ORDERS: Union all three channel streams into one
df = df_web.unionByName(df_store, allowMissingColumns=True).unionByName(df_partner, allowMissingColumns=True)""",

    # ── Cell 11: LKP_CURRENCY_RATE — lookup exchange rate ──
    """\
# LKP_CURRENCY_RATE: Lookup exchange rate to convert to USD
# SQL Override: SELECT FROM_CURRENCY, TO_CURRENCY, EXCHANGE_RATE, RATE_DATE
#   FROM REF.CURRENCY_RATES WHERE TO_CURRENCY = 'USD'
#   AND RATE_DATE = (SELECT MAX(RATE_DATE) FROM REF.CURRENCY_RATES CR2
#   WHERE CR2.FROM_CURRENCY = CURRENCY_RATES.FROM_CURRENCY)
df_lkp_currency = spark.read.table("CURRENCY_RATES").filter(
    F.col("TO_CURRENCY") == "USD"
)
# Keep only the latest rate per currency
w_latest = Window.partitionBy("FROM_CURRENCY").orderBy(F.col("RATE_DATE").desc())
df_lkp_currency = (
    df_lkp_currency
    .withColumn("_rn", F.row_number().over(w_latest))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        F.col("FROM_CURRENCY").alias("LKP_FROM_CURRENCY"),
        F.col("EXCHANGE_RATE").alias("LKP_EXCHANGE_RATE"),
    )
)
df = df.join(
    df_lkp_currency,
    df["CURRENCY_CODE"] == df_lkp_currency["LKP_FROM_CURRENCY"],
    "left",
).drop("LKP_FROM_CURRENCY").withColumnRenamed("LKP_EXCHANGE_RATE", "EXCHANGE_RATE")""",

    # ── Cell 12: FIL_VALID_ORDERS — filter cancelled / null / zero ──
    """\
# FIL_VALID_ORDERS: Remove cancelled and invalid orders
df = df.filter(
    (F.col("ORDER_STATUS") != "CANCELLED")
    & F.col("ORDER_AMOUNT").isNotNull()
    & (F.col("ORDER_AMOUNT") > 0)
)""",

    # ── Cell 13: SQL_CALC_TAX — calculate tax amount ──
    """\
# SQL_CALC_TAX: Calculate tax amount using regional tax rules
# TODO: Original SQL references REF.REGIONAL_TAX_RATES for STORE tax rate;
#       hard-coded fallback rates used below until lookup table is available.
df = df.withColumn(
    "TAX_AMOUNT",
    F.when(F.col("SOURCE_SYSTEM") == "WEB", F.col("ORDER_AMOUNT") * 0.08)
     .when(F.col("SOURCE_SYSTEM") == "STORE", F.col("ORDER_AMOUNT") * 0.07)
     .otherwise(F.col("ORDER_AMOUNT") * 0.05),
)""",

    # ── Cell 14: EXP_ENRICH — derive ORDER_AMOUNT_USD, ORDER_TIER, LOAD_DATE ──
    """\
# EXP_ENRICH: Calculate USD amount, assign order tier
df = df.withColumn(
    "ORDER_AMOUNT_USD",
    F.when(F.col("CURRENCY_CODE") == "USD", F.col("ORDER_AMOUNT"))
     .when(F.col("EXCHANGE_RATE").isNotNull() & (F.col("EXCHANGE_RATE") > 0),
           F.col("ORDER_AMOUNT") * F.col("EXCHANGE_RATE"))
     .otherwise(F.col("ORDER_AMOUNT")),
).withColumn(
    "ORDER_TIER",
    F.when(F.col("ORDER_AMOUNT_USD") >= 1000, F.lit("PREMIUM"))
     .when(F.col("ORDER_AMOUNT_USD") >= 250, F.lit("STANDARD"))
     .when(F.col("ORDER_AMOUNT_USD") >= 50, F.lit("BASIC"))
     .otherwise(F.lit("MICRO")),
).withColumn(
    "LOAD_DATE",
    F.current_timestamp(),
)""",

    # ── Cell 15: SEQ_ORDER_SK — surrogate key ──
    """\
# SEQ_ORDER_SK: Generate surrogate key
df = df.withColumn(
    "ORDER_SK",
    F.monotonically_increasing_id(),
)""",

    # ── Cell 16: RTR_VALID_ERROR — route valid vs error records ──
    """\
# RTR_VALID_ERROR: Route valid vs error records
df_valid = df.filter(
    F.col("ORDER_DATE").isNotNull() & F.col("CUSTOMER_ID").isNotNull()
)
df_error = df.filter(
    F.col("ORDER_DATE").isNull() | F.col("CUSTOMER_ID").isNull()
)""",

    # ── Cell 17: Write valid orders to FACT_ORDERS_CONSOLIDATED ──
    """\
# UPD_INSERT_VALID / TC_COMMIT -> FACT_ORDERS_CONSOLIDATED
df_valid.select(
    "ORDER_SK",
    "SOURCE_SYSTEM",
    "SOURCE_ORDER_ID",
    "CUSTOMER_ID",
    "ORDER_DATE",
    "ORDER_STATUS",
    F.col("ORDER_AMOUNT").alias("ORDER_AMOUNT_LOCAL"),
    F.col("CURRENCY_CODE").alias("ORIGINAL_CURRENCY"),
    "ORDER_AMOUNT_USD",
    "EXCHANGE_RATE",
    "PAYMENT_METHOD",
    "CHANNEL_DETAIL",
    "ORDER_TIER",
    "TAX_AMOUNT",
    "LOAD_DATE",
).write.mode("append").saveAsTable("FACT_ORDERS_CONSOLIDATED")""",

    # ── Cell 18: EXP_ERROR_REASON + write errors to ORDER_ERRORS ──
    """\
# EXP_ERROR_REASON: Build error reason
df_errors_out = df_error.select(
    "SOURCE_SYSTEM",
    "SOURCE_ORDER_ID",
    F.when(F.col("CUSTOMER_ID").isNull() & F.col("ORDER_DATE").isNull(),
           F.lit("MISSING_CUSTOMER_AND_DATE"))
     .when(F.col("CUSTOMER_ID").isNull(), F.lit("MISSING_CUSTOMER_ID"))
     .when(F.col("ORDER_DATE").isNull(), F.lit("MISSING_ORDER_DATE"))
     .otherwise(F.lit("UNKNOWN_ERROR"))
     .alias("ERROR_REASON"),
    F.current_timestamp().alias("ERROR_DATE"),
)
df_errors_out.write.mode("append").saveAsTable("ORDER_ERRORS")""",
]
