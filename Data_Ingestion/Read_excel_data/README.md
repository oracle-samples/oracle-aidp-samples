# Read Excel Files in Spark and Pandas

This module demonstrates two approaches to read Excel files within Spark environments like **OCI Data Flow**, **Databricks**, or **local Spark clusters**.

---

## 1. Using `com.crealytics.spark.excel`

This approach uses the **Spark Excel connector** developed by [Crealytics](https://github.com/crealytics/spark-excel).  
It supports `.xls` and `.xlsx` files directly within Spark DataFrames.

### Requirements

You must add the following JARs to your cluster classpath:

- poi-4.1.2.jar  
- poi-ooxml-4.1.2.jar  
- poi-ooxml-schemas-4.1.2.jar  
- xmlbeans-3.1.0.jar  
- curvesapi-1.06.jar  
- commons-collections4-4.4.jar  
- commons-compress-1.20.jar  
- spark-excel_2.12-0.13.5.jar  

Download them from [Maven Central Repository](https://mvnrepository.com/).

### Example

```python
excel_path = "/Volumes/test_data.xlsx"

df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(excel_path)

df.show()
```
# Excel to Spark using Pandas

This example demonstrates how to **read Excel files using Pandas**, optionally convert them to **CSV**, and then **load them into Spark** for further processing.  
It’s ideal for lightweight or pre-processing workflows before ingesting data into Spark.

---

## Requirements

Install the required dependencies via `requirements.txt`:
- `pandas`
- `openpyxl`
- `xlrd`

### Example

```python
import pandas as pd

# Path to Excel file
excel_path = "/Volumes/test_data.xlsx"

# Read Excel file using Pandas
df = pd.read_excel(excel_path)

# Convert to CSV if needed
csv_path = "/Volumes/test_data.csv"
df.to_csv(csv_path, index=False)

print(df.head())

# Load the CSV back into Spark
spark_df = spark.read.csv(csv_path, header=True, inferSchema=True)
spark_df.show()

```
