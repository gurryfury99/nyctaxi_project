# Databricks notebook source
import sys
import os


project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql import functions as F
from modules.utils.date_utils import resolve_target_month
from modules.transformations.metadata import add_processed_timestamp
from modules.utils.delta_io import write_replace_where

# COMMAND ----------

dbutils.widgets.text("target_month", "")
dbutils.widgets.text("months_ago", "3")

formatted_date = resolve_target_month(
    dbutils.widgets.get("target_month"), int(dbutils.widgets.get("months_ago"))
)

# COMMAND ----------

# Read all Parquet files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(
    f"/Volumes/nyctaxi_workspace/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
)

# COMMAND ----------

# Add a column to capture when the data was processed, and which source file/month this
# row came from. source_month is what makes re-runs safe: we replace exactly this month.
df = add_processed_timestamp(df).withColumn("source_month", F.lit(formatted_date))

# COMMAND ----------

# Idempotent write: replaces this month's rows instead of appending, so re-running
# a month never creates duplicates.
write_replace_where(
    df,
    "nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw",
    f"source_month = '{formatted_date}'",
)
