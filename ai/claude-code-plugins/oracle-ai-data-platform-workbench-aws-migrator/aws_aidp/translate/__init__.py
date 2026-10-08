"""Source-side dialect translators.

- athena_to_spark_sql      Athena/Presto SQL → Spark SQL (working)
- glue_to_spark            DynamicFrame → DataFrame (planned)
- emr_to_aidp_notebook     EMR .ipynb → AIDP notebook (planned)
- sagemaker_to_aidp        SageMaker → AIDP MLOps (planned)
"""
from aws_aidp.translate import athena_to_spark_sql

__all__ = ["athena_to_spark_sql"]
