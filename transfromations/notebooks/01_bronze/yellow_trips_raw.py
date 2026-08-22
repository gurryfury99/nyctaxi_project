# Databricks notebook source
import sys
import os


project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_target_yyyymm
from modules.transformations.metadata import add_processed_timestamp

# COMMAND ----------

formatted_date = get_target_yyyymm(4)

# Read all Parquet files for the specified month from the landing directory into a DataFrame
df = spark.read.format("parquet").load(f"/Volumes/nyctaxi_workspace/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

# Add a column to capture when the data was processed
df = add_processed_timestamp(df)

# COMMAND ----------

# Write the DataFrame to a Unity Catalog managed Delta table in the bronze schema, appending the new month's data
df.write.mode("append").saveAsTable("nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw")
