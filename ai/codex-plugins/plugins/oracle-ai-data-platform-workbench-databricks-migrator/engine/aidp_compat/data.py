"""AIDP Data Utils - Replacement for dbutils.data"""

class AIDPDataUtils:
    def __init__(self, spark=None):
        self._spark = spark

    def summarize(self, df, precise: bool = False):
        """Summarize a DataFrame. Uses pandas describe() or Spark summary()."""
        try:
            if hasattr(df, 'describe') and hasattr(df, 'toPandas'):
                # PySpark DataFrame
                df.summary().show()
            elif hasattr(df, 'describe'):
                # Pandas DataFrame
                print(df.describe())
        except Exception as e:
            print(f"[AIDP] summarize failed: {e}")
            if hasattr(df, 'show'):
                df.show()

    def help(self, method=None):
        print("dbutils.data - AIDP Data Utils")
        print("  summarize(df, precise=False) - Show DataFrame summary statistics")
