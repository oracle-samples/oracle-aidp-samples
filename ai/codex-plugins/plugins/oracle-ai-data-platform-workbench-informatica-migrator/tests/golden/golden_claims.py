"""Golden expected output for m_CLAIMS_PROCESSING Informatica mapping.

Each element in EXPECTED_CELLS is the code content of one notebook cell,
representing the PySpark translation of the Informatica pipeline:

CLAIMS -> SQ_CLAIMS -> JNR_CLAIMS_POLICY (detail)
POLICY -> SQ_POLICY -> JNR_CLAIMS_POLICY (master, Master Outer on POLICY_ID)
-> LKP_ADJUSTER (lookup ADJUSTERS on REGION_CODE + CLAIM_TYPE=SPECIALTY)
-> FIL_VALID_POLICY (NOT ISNULL(POLICY_NUMBER) AND CLAIM_AMOUNT > 0)
-> EXP_CALC (APPROVED_AMOUNT, NET_PAYOUT, DAYS_TO_FILE, CLAIM_PRIORITY)
-> RNK_REGION_CLAIMS (Rank by CLAIM_AMOUNT DESC, partition by REGION_CODE, top 100)
-> SEQ_CLAIM_SK (surrogate key -> CLAIM_SK)
-> SP_FRAUD_CHECK (stored proc -> FRAUD_FLAG)
-> RTR_VALID_REJECT (valid: NET_PAYOUT > 0 AND FRAUD_FLAG != 'Y')
-> Valid path:  UPD_INSERT_VALID -> FACT_CLAIMS
-> Reject path: EXP_REJECT_REASON -> UPD_INSERT_REJECT -> CLAIMS_REJECT
"""

EXPECTED_CELLS = [
    # Cell 0: Imports
    """\
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window""",

    # Cell 1: Read source CLAIMS (SQ_CLAIMS)
    """\
# SQ_CLAIMS: Source Qualifier for CLAIMS
df_source = spark.read.table("INS_STG.CLAIMS").select(
    "CLAIM_ID", "POLICY_ID", "CLAIMANT_ID", "CLAIM_TYPE", "CLAIM_AMOUNT",
    "INCIDENT_DATE", "FILED_DATE", "STATUS", "DESCRIPTION", "REGION_CODE"
)""",

    # Cell 2: Read source POLICY (SQ_POLICY)
    """\
# SQ_POLICY: Source Qualifier for POLICY
df_source_1 = spark.read.table("INS_STG.POLICY").select(
    "POLICY_ID", "POLICY_NUMBER", "POLICY_TYPE", "COVERAGE_LIMIT",
    "DEDUCTIBLE", "EFF_DATE", "EXP_DATE", "POLICY_STATUS"
).filter(F.col("POLICY_STATUS") == "ACTIVE")""",

    # Cell 3: JNR_CLAIMS_POLICY - Master Outer Join (left join keeping all detail/claims rows)
    """\
# JNR_CLAIMS_POLICY: Master Outer Join - CLAIMS (detail) LEFT JOIN POLICY (master) on POLICY_ID
df = df_source.join(
    df_source_1,
    df_source["POLICY_ID"] == df_source_1["POLICY_ID"],
    "left"
).select(
    df_source["CLAIM_ID"],
    df_source["POLICY_ID"],
    df_source["CLAIMANT_ID"],
    df_source["CLAIM_TYPE"],
    df_source["CLAIM_AMOUNT"],
    df_source["INCIDENT_DATE"],
    df_source["FILED_DATE"],
    df_source["STATUS"],
    df_source["DESCRIPTION"],
    df_source["REGION_CODE"],
    df_source_1["POLICY_NUMBER"],
    df_source_1["POLICY_TYPE"],
    df_source_1["COVERAGE_LIMIT"],
    df_source_1["DEDUCTIBLE"],
)""",

    # Cell 4: LKP_ADJUSTER - Lookup adjuster by REGION_CODE and CLAIM_TYPE = SPECIALTY
    """\
# LKP_ADJUSTER: Lookup ADJUSTERS on REGION_CODE = LKP_REGION_CODE AND CLAIM_TYPE = LKP_SPECIALTY
df_lkp_adjuster = spark.read.table("ADJUSTERS").select(
    F.col("ADJUSTER_ID").alias("LKP_ADJUSTER_ID"),
    F.col("ADJUSTER_NAME").alias("LKP_ADJUSTER_NAME"),
    F.col("REGION_CODE").alias("LKP_REGION_CODE"),
    F.col("SPECIALTY").alias("LKP_SPECIALTY"),
)

df = df.join(
    df_lkp_adjuster,
    (df["REGION_CODE"] == df_lkp_adjuster["LKP_REGION_CODE"])
    & (df["CLAIM_TYPE"] == df_lkp_adjuster["LKP_SPECIALTY"]),
    "left"
).withColumn("ADJUSTER_NAME", F.col("LKP_ADJUSTER_NAME")).drop(
    "LKP_ADJUSTER_ID", "LKP_ADJUSTER_NAME", "LKP_REGION_CODE", "LKP_SPECIALTY"
)""",

    # Cell 5: FIL_VALID_POLICY - Filter out claims with no matching policy or zero/negative amount
    """\
# FIL_VALID_POLICY: NOT ISNULL(POLICY_NUMBER) AND CLAIM_AMOUNT > 0
df = df.filter(F.col("POLICY_NUMBER").isNotNull() & (F.col("CLAIM_AMOUNT") > 0))""",

    # Cell 6: EXP_CALC - Expression: APPROVED_AMOUNT, NET_PAYOUT, DAYS_TO_FILE, CLAIM_PRIORITY
    """\
# EXP_CALC: Calculate APPROVED_AMOUNT, NET_PAYOUT, DAYS_TO_FILE, CLAIM_PRIORITY
df = df.withColumn(
    "APPROVED_AMOUNT",
    F.when(F.col("CLAIM_AMOUNT") > F.col("COVERAGE_LIMIT"), F.col("COVERAGE_LIMIT"))
    .otherwise(F.col("CLAIM_AMOUNT"))
).withColumn(
    "NET_PAYOUT",
    F.when(F.col("CLAIM_AMOUNT") > F.col("COVERAGE_LIMIT"), F.col("COVERAGE_LIMIT") - F.col("DEDUCTIBLE"))
    .otherwise(F.col("CLAIM_AMOUNT") - F.col("DEDUCTIBLE"))
).withColumn(
    "DAYS_TO_FILE",
    F.datediff(F.col("FILED_DATE"), F.col("INCIDENT_DATE"))
).withColumn(
    "CLAIM_PRIORITY",
    F.when(F.col("CLAIM_AMOUNT") >= 100000, F.lit("CRITICAL"))
    .when(F.col("CLAIM_AMOUNT") >= 50000, F.lit("HIGH"))
    .when(F.col("CLAIM_AMOUNT") >= 10000, F.lit("MEDIUM"))
    .otherwise(F.lit("LOW"))
)""",

    # Cell 7: RNK_REGION_CLAIMS - Rank by CLAIM_AMOUNT DESC partitioned by REGION_CODE, top 100
    """\
# RNK_REGION_CLAIMS: Rank by CLAIM_AMOUNT DESC, partition by REGION_CODE, top 100
w = Window.partitionBy("REGION_CODE").orderBy(F.col("CLAIM_AMOUNT").desc())
df = df.withColumn("RANKINDEX", F.row_number().over(w))
df = df.filter(F.col("RANKINDEX") <= 100)""",

    # Cell 8: SEQ_CLAIM_SK - Sequence Generator -> CLAIM_SK
    """\
# SEQ_CLAIM_SK: Generate surrogate key CLAIM_SK
df = df.withColumn("CLAIM_SK", F.monotonically_increasing_id())""",

    # Cell 9: SP_FRAUD_CHECK - Stored Procedure call (TODO: replace with actual fraud check logic)
    """\
# SP_FRAUD_CHECK: Call PKG_FRAUD.SP_CHECK_CLAIM
# TODO: Replace with actual fraud check logic (original calls Oracle stored procedure PKG_FRAUD.SP_CHECK_CLAIM)
df = df.withColumn("FRAUD_FLAG", F.lit("N"))""",

    # Cell 10: RTR_VALID_REJECT - Router: split valid vs rejected claims
    """\
# RTR_VALID_REJECT: Route valid claims vs rejected claims
df_valid_claims = df.filter((F.col("NET_PAYOUT") > 0) & (F.col("FRAUD_FLAG") != "Y"))
df_rejected_claims = df.filter((F.col("NET_PAYOUT") <= 0) | (F.col("FRAUD_FLAG") == "Y"))""",

    # Cell 11: EXP_REJECT_REASON - Add REJECT_REASON and REJECT_DATE to rejected claims
    """\
# EXP_REJECT_REASON: Build rejection reason for rejected claims
df_rejected_claims = df_rejected_claims.withColumn(
    "REJECT_REASON",
    F.when(F.col("FRAUD_FLAG") == "Y", F.lit("FRAUD_DETECTED"))
    .when(F.col("NET_PAYOUT") <= 0, F.lit("PAYOUT_BELOW_DEDUCTIBLE"))
    .otherwise(F.lit("UNKNOWN"))
).withColumn(
    "REJECT_DATE",
    F.current_timestamp()
)""",

    # Cell 12: Write valid claims to FACT_CLAIMS target
    """\
# UPD_INSERT_VALID -> FACT_CLAIMS: Write valid claims to fact table
df_valid_claims.withColumn("LOAD_DATE", F.current_timestamp()).select(
    "CLAIM_SK", "CLAIM_ID", "POLICY_ID", "POLICY_NUMBER", "CLAIM_TYPE",
    "POLICY_TYPE", "CLAIM_AMOUNT", "APPROVED_AMOUNT", "DEDUCTIBLE",
    "NET_PAYOUT", "INCIDENT_DATE", "FILED_DATE", "DAYS_TO_FILE",
    "CLAIM_PRIORITY", "ADJUSTER_NAME",
    F.col("RANKINDEX").alias("REGION_RANK"), "FRAUD_FLAG", "LOAD_DATE"
).write.mode("append").saveAsTable("DW.FACT_CLAIMS")""",

    # Cell 13: Write rejected claims to CLAIMS_REJECT target
    """\
# UPD_INSERT_REJECT -> CLAIMS_REJECT: Write rejected claims to reject table
df_rejected_claims.select(
    "CLAIM_ID", "POLICY_ID", "REJECT_REASON", "REJECT_DATE"
).write.mode("append").saveAsTable("DW.CLAIMS_REJECT")""",
]
