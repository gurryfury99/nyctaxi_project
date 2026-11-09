# Databricks notebook source
import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import resolve_target_month, month_window_for
from modules.transformations.gold import daily_trip_summary
from modules.utils.delta_io import write_replace_where

# COMMAND ----------

dbutils.widgets.text("target_month", "")
dbutils.widgets.text("months_ago", "3")

target_month = resolve_target_month(
    dbutils.widgets.get("target_month"), int(dbutils.widgets.get("months_ago"))
)
month_start, month_end = month_window_for(target_month)

# COMMAND ----------

# Load the enriched trips for the target month only
df = spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched").filter(
    f"tpep_pickup_datetime >= '{month_start}' AND tpep_pickup_datetime < '{month_end}'"
)

# COMMAND ----------

# Aggregate trip data by pickup date with key metrics (logic lives in modules/transformations/gold.py)
df_daily = daily_trip_summary(df)

display(df_daily)

# COMMAND ----------

write_replace_where(
    df_daily,
    "nyctaxi_workspace.nyctaxi_03_gold.daily_trip_summary",
    f"pickup_date >= '{month_start}' AND pickup_date < '{month_end}'",
)
