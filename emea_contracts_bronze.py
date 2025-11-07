"""
bronze_emea_contracts.py

Bronze layer ingestion script for EMEA vendor contracts.
This script pulls raw contract data from Smartsheet using its API, performs basic normalization,
and stores the results in a Delta Lake table for downstream processing.
"""

# Install required package
%pip install smartsheet_dataframe

import pandas as pd
from smartsheet_dataframe import get_sheet_as_df

# 🔐 Retrieve Smartsheet token securely (replace with your own secret manager or env variable)
smartsheet_token = "your-smartsheet-token"  # Replace with secure retrieval method

# 📥 Retrieve data from Smartsheet using the sheet ID
sheet_id = "your-emea-sheet-id"  # Replace with actual sheet ID
try:
    df = get_sheet_as_df(token=smartsheet_token, sheet_id=sheet_id)
except Exception as e:
    raise RuntimeError(f"Failed to retrieve sheet data: {e}")

# 🧹 Drop sensitive or unused columns
if "Funding_Cost_Center" in df.columns:
    df.drop(columns=["Funding_Cost_Center"], inplace=True)
    try:
        del df["Funding_Cost_Center"]
    except KeyError:
        pass

# 🕒 Add metadata
df["import_timestamp"] = pd.Timestamp.now()
df["contract_region"] = "EMEA"
df["sheet_id_used"] = str(sheet_id)

# 🔢 Normalize numeric columns
numeric_columns = ["FY26"]
for column in numeric_columns:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

# 🔤 Convert object columns to strings
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].astype(str)

# 🔤 Normalize specific string columns
string_columns = ["Cost Center", "Contract #", "BuyingHub"]
for column in string_columns:
    if column in df.columns:
        df[column] = df[column].astype(str)

# 🧼 Clean column names
df.columns = df.columns.str.replace(r"[^\w]", "_", regex=True)

# ⚡ Optional: Convert dtypes for Arrow compatibility
df = df.convert_dtypes()

# ⚙️ Enable Arrow-based data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 🔄 Convert to Spark DataFrame
df_spark = spark.createDataFrame(df)

# 💾 Save to Delta Lake (replace with your own table name)
df_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("your_database.your_table_name")  # Replace with actual table name

# 👀 Display latest data (optional for notebook use)
try:
    display(spark.sql("SELECT * FROM your_database.your_table_name"))  # Replace with actual table name
except Exception as e:
    print("Display failed:", e)
