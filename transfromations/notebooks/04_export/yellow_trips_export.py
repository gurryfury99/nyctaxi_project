# Databricks notebook source
import sys
import os
# Go three levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import resolve_target_month, month_window_for
from pyspark.sql.functions import date_format

# COMMAND ----------

dbutils.widgets.text("target_month", "")
dbutils.widgets.text("months_ago", "3")

target_month = resolve_target_month(
    dbutils.widgets.get("target_month"), int(dbutils.widgets.get("months_ago"))
)
month_start, month_end = month_window_for(target_month)

# COMMAND ----------

# Read the 'yellow_trips_enriched' table from the 'nyctaxi_02_silver' schema, limited to the
# target month. Same [start, end) window as the rest of the pipeline (the old filter used a
# bare ">" and had no upper bound, so it could skip midnight trips and pick up later months).

df = spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched").filter(
    f"tpep_pickup_datetime >= '{month_start}' AND tpep_pickup_datetime < '{month_end}'"
)

# COMMAND ----------

# Add a year_month column, formated as yyyy-MM

df = df.withColumn("year_month", date_format("tpep_pickup_datetime", "yyyy-MM"))

# COMMAND ----------

# Write the yellow_trips data in JSON format to the External Table "yellow_trips_export".
#
# Dynamic partition overwrite replaces only the (vendor, year_month) partitions present in
# this DataFrame, so re-running a month replaces its files instead of appending duplicates.
# NOTE: untested until the first run on Databricks - if the external table rejects
# overwrite, fall back to mode("append") and purge the month's partition folders first.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.write.\
    option("path", "abfss://nyctaxi-yellow@nyctaxistorage638.dfs.core.windows.net/yellow_trips_export/").\
    format("json").\
    mode("overwrite").\
    partitionBy("vendor", "year_month").\
    saveAsTable("nyctaxi_workspace.nyctaxi_04_export.yellow_trips_export")
