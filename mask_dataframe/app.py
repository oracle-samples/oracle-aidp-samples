from abc import ABC, abstractmethod

class PIIChecker(ABC):
    def __init__(self, **kwargs):
        pass

    @staticmethod
    def get(model, **kwargs):
        if model == 'haiku':
            return AnthropicPIIChecker('claude-haiku-4-5-20251001', **kwargs)
        elif model == 'sonnet':
            return AnthropicPIIChecker('claude-sonnet-4-5-20250929', **kwargs)
        elif model == 'opus':
            return AnthropicPIIChecker('claude-opus-4-5-20251101', **kwargs)
        else:
            return OciPIIChecker(model, **kwargs)

    @abstractmethod
    def is_pii(self, column_name, column_values):
        pass

    def get_prompt(self, column_name, column_values):
        return f"""Your job is to identify whether a column contains sensitive values. I have a column with name "{column_name}" and some of the values it contains are "{column_values}". Respond in a single word, "yes" if the column has sensitive data else "no" """


class AnthropicPIIChecker(PIIChecker):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        import anthropic
        import os
        self.model = model
        self.client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    def is_pii(self, column_name, column_values):
        message = self.client.messages.create(
            model=self.model,
            max_tokens=50,
            messages=[
                {"role": "user", "content": self.get_prompt(column_name, column_values)}
            ]
        )
        return message.content[0].text


class OciPIIChecker(PIIChecker):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    def is_pii(self, column_name, column_values):
        from pyspark.sql import functions as F
        return (
            spark.range(1)
            .withColumn(
                'is_pii',
                F.expr(f"query_model('{self.model}', '{self.get_prompt(column_name, column_values)}')"))
            .head()
            .is_pii
        )

    def get_prompt(self, column_name, column_values):
        return super().get_prompt(column_name, column_values).replace("'", '"').replace('"', '\\"')


def mask_df(df, model='xai.grok-4', column_values_to_fetch=1000):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    masked_columns_dict = {}
    columns = df.columns
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
        pii_checker = PIIChecker.get(model)
        is_pii = pii_checker.is_pii(column_name, column_values)
        print(f"column: {column_name}, is_pii: {is_pii}, column_values: {column_values}")
        if is_pii.lower() == "yes":
            masked_columns_dict[column_name] = F.mask(F.col(column_name).cast(StringType()))
    return df.withColumns(masked_columns_dict) if masked_columns_dict else df


# Example call the `mask_df`
# mask_df(spark.table("catalog.schema.table")).show()