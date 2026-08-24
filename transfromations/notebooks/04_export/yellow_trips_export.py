# Databricks notebook source
import sys
import os
# Go three levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import get_month_start_n_months_ago
from pyspark.sql.functions import date_format

# COMMAND ----------

# Get the first day of the month three months ago
three_months_ago_start = get_month_start_n_months_ago(3)

# COMMAND ----------

# Read the 'yellow_trips_enriched' table from the 'nyctaxi_02_silver' schema
# and filter to only include trips with a pickup datetime
# later than the start date from three months ago

df = spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched").filter(
    f"tpep_pickup_datetime > '{three_months_ago_start}'"
)

# COMMAND ----------

# Add a year_month column, formated as yyyy-MM

df = df.withColumn("year_month", date_format("tpep_pickup_datetime", "yyyy-MM"))

# COMMAND ----------

# Write the yellow_trips data in JSON format to the External Table "yellow_trips_export"

df.write.\
    option("path", "abfss://nyctaxi-yellow@nyctaxistorage638.dfs.core.windows.net/yellow_trips_export/").\
    format("json").\
    mode("append").\
    partitionBy("vendor", "year_month").\
    saveAsTable("nyctaxi_workspace.nyctaxi_04_export.yellow_trips_export")
