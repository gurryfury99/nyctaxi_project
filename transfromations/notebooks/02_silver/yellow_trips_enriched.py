# Databricks notebook source
import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import resolve_target_month, month_window_for
from modules.transformations.enrichment import enrich_with_zones
from modules.utils.delta_io import write_replace_where

# COMMAND ----------

dbutils.widgets.text("target_month", "")
dbutils.widgets.text("months_ago", "3")

target_month = resolve_target_month(
    dbutils.widgets.get("target_month"), int(dbutils.widgets.get("months_ago"))
)
month_start, month_end = month_window_for(target_month)

# COMMAND ----------

# Same [start, end) window as the cleansing step (the old version had no upper bound).
df_trips = spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_cleansed").filter(
    f"tpep_pickup_datetime >= '{month_start}' AND tpep_pickup_datetime < '{month_end}'"
)

# Zone dimension (SCD2). enrich_with_zones uses only the current version of each zone,
# so zones with history can no longer multiply trips.
df_zones = spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.taxi_zone_lookup")

# COMMAND ----------

df_enriched = enrich_with_zones(df_trips, df_zones)

display(df_enriched)

# COMMAND ----------

write_replace_where(
    df_enriched,
    "nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched",
    f"tpep_pickup_datetime >= '{month_start}' AND tpep_pickup_datetime < '{month_end}'",
)
