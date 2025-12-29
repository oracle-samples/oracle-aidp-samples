def get_merged_df(base_df, incremental_df, primary_keys, partition_columns=[],
                  carry_base_values_when_column_missing_list=[], column_update_allow_list=["*"],
                  column_update_deny_list=[], allow_extra_columns=True, extra_columns_deny_list=[],
                  deletion_condition="false"):
    from pyspark.sql import functions as F
    from functools import reduce
    assert primary_keys is not None and len(primary_keys) >= 1
    for primary_key in primary_keys:
        assert primary_key in base_df.columns and primary_key in incremental_df.columns
    columns_to_select = []
    unchanged_record_condition = " AND ".join([f"incremental.{primary_key} IS NULL" for primary_key in primary_keys])
    inserted_record_condition = " AND ".join([f"base.{primary_key} IS NULL" for primary_key in primary_keys])
    # For columns present in both
    columns_in_both = [column_name for column_name in base_df.columns if column_name in incremental_df.columns]
    for column_name in columns_in_both:
        if (column_update_allow_list == [
            "*"] or column_name in column_update_allow_list) and column_name not in column_update_deny_list:
            columns_to_select.append(
                f"CASE WHEN {unchanged_record_condition} THEN base.{column_name} ELSE incremental.{column_name} END AS {column_name}")
        else:
            columns_to_select.append(
                f"CASE WHEN {inserted_record_condition} THEN incremental.{column_name} ELSE base.{column_name} END AS {column_name}")
    # For columns missing in the incremental
    extra_columns_in_base = [c for c in base_df.columns if c not in incremental_df.columns]
    for column_name in extra_columns_in_base:
        if column_name in carry_base_values_when_column_missing_list:
            columns_to_select.append(f"base.{column_name} AS {column_name}")
        else:
            columns_to_select.append(
                f"CASE WHEN {unchanged_record_condition} THEN base.{column_name} ELSE NULL END AS {column_name}")
    # For column names missing in the base
    extra_columns_in_incremental = [c for c in incremental_df.columns if c not in base_df.columns]
    if allow_extra_columns:
        for column_name in extra_columns_in_incremental:
            if column_name not in extra_columns_deny_list:
                columns_to_select.append(f"incremental.{column_name} AS {column_name}")
    distinct_partitions_list = incremental_df.select(*partition_columns).distinct().collect()
    conditions = []
    if partition_columns:
        for distinct_partition in distinct_partitions_list:
            single_partition_conditions = []
            for partition_column in partition_columns:
                single_partition_conditions.append(
                    F.col(partition_column) == getattr(distinct_partition, partition_column))
            # Create a filter condition to fetch for a specific partition
            single_partition_combined_condition = reduce(lambda cond_1, cond_2: cond_1 & cond_2,
                                                         single_partition_conditions)
            conditions.append(single_partition_combined_condition)
    # Based on the content of incremental table, fetch all the existing partitions from the base table
    combined_condition = reduce(lambda cond_1, cond_2: cond_1 | cond_2, conditions) if conditions else F.lit(True)
    filtered_base_df = base_df.filter(combined_condition)
    merged_df = (
        incremental_df.alias("incremental")
        .join(
            filtered_base_df.alias("base"),
            primary_keys,
            "full_outer"
        )
        .filter(~(F.expr(deletion_condition).isNotNull() & F.expr(
            deletion_condition)))  # Discard rows where the deletion_condition evaluates to not null and is true
        .selectExpr(*columns_to_select)
    )
    merged_df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
    return merged_df
