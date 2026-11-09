# Databricks notebook source
import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import resolve_target_month, month_window_for
from modules.transformations.cleansing import cleanse_trips
from modules.utils.delta_io import write_replace_where

# COMMAND ----------

dbutils.widgets.text("target_month", "")
dbutils.widgets.text("months_ago", "3")

target_month = resolve_target_month(
    dbutils.widgets.get("target_month"), int(dbutils.widgets.get("months_ago"))
)

# [start, end): first day of the month (inclusive) to first day of the next month (exclusive)
month_start, month_end = month_window_for(target_month)

# COMMAND ----------

# Only this month's source file. A few trips in each file fall outside its own month
# (some are dated as far back as 2008); those are quarantined below, not loaded.
raw = spark.read.table("nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw").filter(
    f"source_month = '{target_month}'"
)

# COMMAND ----------

# Validate every row. Clean rows continue to silver; rejected rows are kept, with the
# reason(s), in a quarantine table so nothing disappears silently.
clean, quarantine = cleanse_trips(raw, str(month_start), str(month_end))

# COMMAND ----------

write_replace_where(
    clean,
    "nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_cleansed",
    f"tpep_pickup_datetime >= '{month_start}' AND tpep_pickup_datetime < '{month_end}'",
)

write_replace_where(
    quarantine,
    "nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_quarantine",
    f"source_month = '{target_month}'",
)

# COMMAND ----------

# Quick reconciliation printed in the job output: clean + quarantined must equal raw.
n_raw, n_clean, n_quarantine = raw.count(), clean.count(), quarantine.count()
print(f"{target_month}: raw={n_raw:,} clean={n_clean:,} quarantined={n_quarantine:,}")
assert n_clean + n_quarantine == n_raw, "Rows were lost between bronze and silver"
