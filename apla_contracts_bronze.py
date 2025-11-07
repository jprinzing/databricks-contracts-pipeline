"""
bronze_apla_contracts.py

Bronze layer ingestion script for APLA vendor contracts.
This script pulls raw contract data from Smartsheet using its API, performs basic normalization,
and stores the results in a Delta Lake table for downstream processing.
"""

# Install required package
%pip install smartsheet_dataframe

import pandas as pd
from smartsheet_dataframe import get_sheet_as_df

# 🔐 Replace with your own Smartsheet token or use a secure secret manager
smartsheet_token = "your-smartsheet-token"

# 📥 Retrieve data from Smartsheet using the sheet ID
# This sheet contains contracts specific to the APLA region
sheet_id = "your-sheet-id"
df = get_sheet_as_df(token=smartsheet_token, sheet_id=sheet_id)

# 🧹 Drop the "Funding_Cost_Center" column if present
if "Funding_Cost_Center" in df.columns:
    df.drop(columns=["Funding_Cost_Center"], inplace=True)
    try:
        del df["Funding_Cost_Center"]
    except KeyError:
        pass  # Already removed

# 🕒 Add import timestamp for tracking
df["import_timestamp"] = pd.Timestamp.now()

# 🔢 Normalize numeric columns
numeric_columns = [
    "Previous Contract Value", 
    "Total Contract Value", 
    "FY25 (Committed)", 
    "FY26 (Committed)", 
    "Annualized Cost", 
    "PO/NExT",
    "Contract Length (months)",
    "Days from End Date",
    "Helper Exp Value",
    "Renewal Total Value", 
    "Original - Renewal Total Value"
]
for column in numeric_columns:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

# 🔤 Convert object-type columns to strings for compatibility
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].astype(str)

# 🧾 Normalize specific string columns
string_columns = ["Cost Center", "Contract #", "BuyingHub"]
for column in string_columns:
    if column in df.columns:
        df[column] = df[column].astype(str)

# 🧼 Drop "Muge Present" column if it exists
if "Muge Present" in df.columns:
    df.drop(columns=["Muge Present"], inplace=True)

# 🧽 Clean column names: replace non-word characters with underscores
df.columns = df.columns.str.replace(r"[^\w]", "_", regex=True)

# ⚙️ Optional: Convert dtypes for better Arrow compatibility
df = df.convert_dtypes()

# ⚡ Enable Arrow-based data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 🔄 Convert to Spark DataFrame
df_spark = spark.createDataFrame(df)

# 💾 Save to Delta Lake (replace with your own table name)
df_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("contracts_pipeline.bronze_apla_contracts")

# 👀 Display table contents (safe for notebooks)
try:
    display(spark.sql("SELECT * FROM contracts_pipeline.bronze_apla_contracts"))
except Exception as e:
    print("Display failed:", e)
