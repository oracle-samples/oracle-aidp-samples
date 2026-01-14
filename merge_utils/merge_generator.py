from functools import reduce
from typing import List, Optional
from pyspark.sql import DataFrame, functions as F


def get_merged_df(
    base_df: DataFrame,
    incremental_df: DataFrame,
    primary_keys: List[str],
    partition_columns: Optional[List[str]] = None,
    carry_base_values_when_column_missing_list: Optional[List[str]] = None,
    column_update_allow_list: Optional[List[str]] = None,
    column_update_deny_list: Optional[List[str]] = None,
    allow_extra_columns: bool = True,
    extra_columns_deny_list: Optional[List[str]] = None,
    deletion_condition: str = "false",
    set_dynamic_partition_mode: bool = True
) -> DataFrame:
    """
    Merge incremental data into a base DataFrame with configurable update behavior.

    This function performs a partition-aware merge operation that:
    - Updates existing records based on primary keys
    - Inserts new records
    - Handles column-level update policies
    - Supports deletes via deletion conditions

    Args:
        base_df: Base DataFrame containing existing data
        incremental_df: Incremental DataFrame with updates and new records
        primary_keys: List of column names that uniquely identify records
        partition_columns: Optional list of partition columns for optimized filtering
        carry_base_values_when_column_missing_list: Columns to preserve from base when missing in incremental
        column_update_allow_list: Columns allowed to be updated (["*"] = all)
        column_update_deny_list: Columns that should never be updated
        allow_extra_columns: Whether to include new columns from incremental data
        extra_columns_deny_list: New columns to exclude from the merge
        deletion_condition: SQL expression for filtering out deleted records
        set_dynamic_partition_mode: Whether to set Spark dynamic partition mode

    Returns:
        Merged DataFrame with combined data from base and incremental

    Raises:
        ValueError: If validation of input parameters fails
    """
    # Initialize default values for mutable parameters
    partition_columns = partition_columns or []
    carry_base_values_when_column_missing_list = carry_base_values_when_column_missing_list or []
    column_update_allow_list = column_update_allow_list or ["*"]
    column_update_deny_list = column_update_deny_list or []
    extra_columns_deny_list = extra_columns_deny_list or []

    # Validate inputs
    _validate_inputs(base_df, incremental_df, primary_keys, partition_columns)

    # Identify column categories
    common_columns = [col for col in base_df.columns if col in incremental_df.columns]
    base_only_columns = [col for col in base_df.columns if col not in incremental_df.columns]
    incremental_only_columns = [col for col in incremental_df.columns if col not in base_df.columns]

    # Filter base DataFrame by partitions present in incremental data
    filtered_base_df = _filter_base_by_partitions(base_df, incremental_df, partition_columns)

    # Perform full outer join
    joined_df = (
        incremental_df.alias("incremental")
        .join(filtered_base_df.alias("base"), primary_keys, "full_outer")
    )

    # Build column selection logic
    selected_columns = _build_column_selections(
        common_columns=common_columns,
        base_only_columns=base_only_columns,
        incremental_only_columns=incremental_only_columns,
        primary_keys=primary_keys,
        carry_base_values_when_column_missing_list=carry_base_values_when_column_missing_list,
        column_update_allow_list=column_update_allow_list,
        column_update_deny_list=column_update_deny_list,
        allow_extra_columns=allow_extra_columns,
        extra_columns_deny_list=extra_columns_deny_list
    )

    # Apply deletion condition filter and select columns
    merged_df = joined_df.filter(
        ~(F.expr(deletion_condition).isNotNull() & F.expr(deletion_condition))  # Filter out records marked for deletion
    ).selectExpr(*selected_columns)

    # Set dynamic partition mode if requested
    if set_dynamic_partition_mode:
        merged_df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    return merged_df


def _validate_inputs(
    base_df: DataFrame,
    incremental_df: DataFrame,
    primary_keys: List[str],
    partition_columns: List[str]
) -> None:
    """Validate input parameters for the merge operation."""
    if not primary_keys or len(primary_keys) < 1:
        raise ValueError("primary_keys must contain at least one column name")

    for primary_key in primary_keys:
        if primary_key not in base_df.columns:
            raise ValueError(f"Primary key '{primary_key}' not found in base_df columns")
        if primary_key not in incremental_df.columns:
            raise ValueError(f"Primary key '{primary_key}' not found in incremental_df columns")

    for partition_col in partition_columns:
        if partition_col not in base_df.columns:
            raise ValueError(f"Partition column '{partition_col}' not found in base_df columns")
        if partition_col not in incremental_df.columns:
            raise ValueError(f"Partition column '{partition_col}' not found in incremental_df columns")


def _filter_base_by_partitions(
    base_df: DataFrame,
    incremental_df: DataFrame,
    partition_columns: List[str]
) -> DataFrame:
    """Filter base DataFrame to only include partitions present in incremental data."""
    if not partition_columns:
        return base_df

    # Get distinct partition values from incremental data
    distinct_partitions = incremental_df.select(*partition_columns).distinct().collect()

    # Build filter conditions for each partition
    partition_conditions = []
    for partition_row in distinct_partitions:
        # Create condition for this specific partition combination
        single_partition_conditions = [
            F.col(col_name) == getattr(partition_row, col_name)
            for col_name in partition_columns
        ]
        # Combine conditions with AND
        combined_condition = reduce(lambda c1, c2: c1 & c2, single_partition_conditions)
        partition_conditions.append(combined_condition)

    # Combine all partition conditions with OR
    if partition_conditions:
        final_condition = reduce(lambda c1, c2: c1 | c2, partition_conditions)
        return base_df.filter(final_condition)

    return base_df


def _build_column_selections(
    common_columns: List[str],
    base_only_columns: List[str],
    incremental_only_columns: List[str],
    primary_keys: List[str],
    carry_base_values_when_column_missing_list: List[str],
    column_update_allow_list: List[str],
    column_update_deny_list: List[str],
    allow_extra_columns: bool,
    extra_columns_deny_list: List[str]
) -> List[str]:
    """Build the list of column selection expressions for the merge operation."""
    columns_to_select = []

    # Helper conditions for record type detection
    is_unchanged_record = " AND ".join([f"incremental.{pk} IS NULL" for pk in primary_keys])
    is_new_record = " AND ".join([f"base.{pk} IS NULL" for pk in primary_keys])

    # Handle columns present in both DataFrames
    for col_name in common_columns:
        is_updateable = (
            (column_update_allow_list == ["*"] or col_name in column_update_allow_list)
            and col_name not in column_update_deny_list
        )

        if is_updateable:
            # Use incremental value for updated/new records, base value for unchanged
            columns_to_select.append(
                f"CASE WHEN {is_unchanged_record} THEN base.{col_name} ELSE incremental.{col_name} END AS {col_name}"
            )
        else:
            # Preserve base value except for new records
            columns_to_select.append(
                f"CASE WHEN {is_new_record} THEN incremental.{col_name} ELSE base.{col_name} END AS {col_name}"
            )

    # Handle columns only in base DataFrame
    for col_name in base_only_columns:
        if col_name in carry_base_values_when_column_missing_list:
            # Always carry forward the base value
            columns_to_select.append(f"base.{col_name} AS {col_name}")
        else:
            # Set to NULL for updated/new records
            columns_to_select.append(
                f"CASE WHEN {is_unchanged_record} THEN base.{col_name} ELSE NULL END AS {col_name}"
            )

    # Handle columns only in incremental DataFrame
    if allow_extra_columns:
        for col_name in incremental_only_columns:
            if col_name not in extra_columns_deny_list:
                columns_to_select.append(f"incremental.{col_name} AS {col_name}")

    return columns_to_select
