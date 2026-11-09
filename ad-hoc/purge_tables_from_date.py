# Databricks notebook source
# MAGIC %md
# MAGIC # Purge tables from date
# MAGIC
# MAGIC Maintenance notebook. Deletes records from the four pipeline tables from a
# MAGIC given date onwards, so `nyctaxi_job` can reload a clean slice and prove the
# MAGIC incremental path works end to end.
# MAGIC
# MAGIC **This deletes data.** `DRY_RUN` is `True` by default - it only counts.
# MAGIC Read the printed date and the row counts, then set `DRY_RUN = False`.

# COMMAND ----------

import sys
import os

# This notebook sits one level below the project root.
project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))

if project_root not in sys.path:
    sys.path.append(project_root)

from delta.tables import DeltaTable
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Set to False to actually delete. Leave True to preview the row counts first.
DRY_RUN = True

# The same offset every pipeline notebook uses. Today - 3 months, first of month,
# which is 2026-05 - the first month the backfill did not cover.
# Do not hardcode a date here; if the offset ever changes, this follows it.
MONTHS_AGO = 3

date_from = get_month_start_n_months_ago(MONTHS_AGO)

print(f"Target month : {date_from.strftime('%Y-%m')}")
print(f"date_from    : {date_from}")
print(f"Mode         : {'DRY RUN - nothing will be deleted' if DRY_RUN else 'LIVE - rows will be deleted'}")

# COMMAND ----------

# Every table in the pipeline, with the predicate that selects the rows to delete.
# yellow_trip_cleansed is singular - that is the actual table name in this project.
# Bronze and quarantine are keyed by source_month ('YYYY-MM'); the rest by trip date.
month_str = date_from.strftime("%Y-%m")
TABLES = [
    ("nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw",       f"source_month >= '{month_str}'"),
    ("nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_cleansed",   f"tpep_pickup_datetime >= '{date_from}'"),
    ("nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_quarantine", f"source_month >= '{month_str}'"),
    ("nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched",  f"tpep_pickup_datetime >= '{date_from}'"),
    ("nyctaxi_workspace.nyctaxi_03_gold.daily_trip_summary",       f"pickup_date >= '{date_from}'"),
]


def purge(table, predicate):
    rows = spark.read.table(table).filter(predicate).count()

    if DRY_RUN:
        print(f"[DRY RUN] {table:<58} {rows:>10,} rows would be deleted")
        return

    DeltaTable.forName(spark, table).delete(predicate)
    print(f"[DELETED] {table:<58} {rows:>10,} rows")

# COMMAND ----------

for table, predicate in TABLES:
    purge(table, predicate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete the landing file
# MAGIC
# MAGIC Deleting the landing folder is optional now. The pipeline loads each month with
# MAGIC `replaceWhere`, so it is safe to re-run, and the ingest notebook only skips a month
# MAGIC that is already in bronze. Delete the folder only if you want to re-download
# MAGIC the file from TLC as well.

# COMMAND ----------

# The landing volume schema. Confirm which one exists before running the cell below:
# the initial-load notebooks use nyctaxi_landing, the newer ones use 00_landing.
display(dbutils.fs.ls("/Volumes/nyctaxi_workspace/"))

# COMMAND ----------

# The live pipeline writes to 00_landing. nyctaxi_landing also exists in this
# catalog but only holds the initial-load data.
LANDING_SCHEMA = "00_landing"

landing_folder = (
    f"/Volumes/nyctaxi_workspace/{LANDING_SCHEMA}"
    f"/data_sources/nyctaxi_yellow/{date_from.strftime('%Y-%m')}"
)

try:
    files = dbutils.fs.ls(landing_folder)
except Exception:
    files = None

if files is None:
    print(f"Nothing to delete - {landing_folder} does not exist.")
    print("The job will download this month fresh.")
elif DRY_RUN:
    print(f"[DRY RUN] would delete {landing_folder}")
    display(files)
else:
    dbutils.fs.rm(landing_folder, recurse=True)
    print(f"[DELETED] {landing_folder}")
