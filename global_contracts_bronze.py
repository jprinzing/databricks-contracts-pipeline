"""
bronze_global_contracts.py

Bronze layer ingestion script for global vendor contracts.
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
# This sheet contains global contracts not tied to specific geographies
sheet_id = "your-sheet-id"
df = get_sheet_as_df(token=smartsheet_token, sheet_id=sheet_id)

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
    df[column] = pd.to_numeric(df[column], errors='coerce')

# 🔤 Ensure string consistency
df["Cost Center"] = df["Cost Center"].astype(str)
df["Contract #"] = df["Contract #"].astype(str)
df["BuyingHub"] = df["BuyingHub"].astype(str)

# 🧼 Rename and clean columns
df.rename(columns={"Contract #": "Contract_Number"}, inplace=True)
df.drop(columns=["Muge Present"], inplace=True)

df.columns = (
    pd.Series(df.columns)
    .astype(str)
    .str.replace(' ', '_')
    .str.replace('(', '')
    .str.replace(')', '')
    .str.replace('/', '_')
    .str.replace('-', '_')
    .str.replace('#', '')
)

# 🧾 Normalize dropdown values
df["VP_Reviewed"] = df["VP_Reviewed"].fillna("Not Reviewed").astype(str).str.strip()

# ⚡ Enable Arrow-based transfers for performance
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 🔄 Convert to Spark DataFrame
df_spark = spark.createDataFrame(df)

# 💾 Save to Delta Lake (replace with your own table name)
df_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("contracts_pipeline.bronze_global_contracts")

# 👀 Display latest import snapshot
latest_data = spark.sql("""
    SELECT *
    FROM contracts_pipeline.bronze_global_contracts
    WHERE import_timestamp = (
        SELECT MAX(import_timestamp)
        FROM contracts_pipeline.bronze_global_contracts
    )
""")
display(latest_data)
