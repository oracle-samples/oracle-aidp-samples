def mask_df(df, model='xai.grok-4', column_values_to_fetch=1000):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    masked_columns_dict = {}
    columns = df.columns
    _prompt = """Your job is to identify whether a column contains sensitive values. I have a column with name "{column_name}" and some of the values it contains are "{column_values}". Respond in a single word, "yes" if the column has sensitive data else "no" """
    for column_name in columns:
        _column_values = [
            getattr(row, column_name)
            for row in
            (
                df
                .filter(F.col(column_name).isNotNull())
                .select(F.col(column_name))
                .limit(column_values_to_fetch)
                .collect()
            )
        ]
        column_values = str(_column_values)
        prompt = _prompt.format(column_name=column_name, column_values=column_values).replace("'", '"').replace('"', '\\"')
        print(prompt)
        is_pii = (
            spark.range(1)
            .withColumn(
                'is_pii',
                F.expr(f"query_model('{model}', '{prompt}')"))
            .head()
            .is_pii
            .lower()
        )
        print(f"is_pii: {is_pii}")
        if is_pii == "yes":
            masked_columns_dict[column_name] = F.mask(F.col(column_name).cast(StringType()))
    return df.withColumns(masked_columns_dict) if masked_columns_dict else df


mask_df(spark.table("catalog.schema.table")).show()