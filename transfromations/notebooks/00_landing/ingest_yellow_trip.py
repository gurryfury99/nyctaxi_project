# Databricks notebook source
import sys
import os

# Go three levels up to reach the project root.
# The lecturer's layout is two levels; ours has an extra `transfromations`
# folder, so these notebooks sit one level deeper.
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from modules.utils.date_utils import resolve_target_month
from modules.data_loader.file_downloader import download_file

# COMMAND ----------

# Job parameters. Leave target_month blank for the normal monthly run
# (months_ago before today); set it to e.g. 2026-03 to backfill or re-run a specific month.
dbutils.widgets.text("target_month", "")
dbutils.widgets.text("months_ago", "3")

formatted_date = resolve_target_month(
    dbutils.widgets.get("target_month"), int(dbutils.widgets.get("months_ago"))
)

BRONZE_TABLE = "nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw"

# Define the local directory for this date's data
dir_path = f"/Volumes/nyctaxi_workspace/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

# Define the full path for the downloaded file
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# COMMAND ----------


def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def month_already_in_bronze(yyyy_mm):
    """True if this month has already been loaded into bronze (table or column may not exist yet)."""
    try:
        return (
            spark.read.table(BRONZE_TABLE).filter(f"source_month = '{yyyy_mm}'").limit(1).count() > 0
        )
    except Exception:
        return False


# COMMAND ----------

# Step 1: make sure the file is in the landing volume.
file_ready = file_exists(local_path)

if not file_ready:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"
        download_file(url, dir_path, local_path)
        file_ready = True
        print(f"File for {formatted_date} downloaded in this run")
    except Exception as e:
        print(f"File download failed: {str(e)}")

# Step 2: decide whether downstream tasks should run.
# Previously "file already exists" meant "skip", so a failure *after* the download
# left that month permanently unloaded. Now we skip only if the data is really in bronze.
if not file_ready:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
elif month_already_in_bronze(formatted_date):
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"{formatted_date} is already loaded in bronze, skipping downstream tasks")
else:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print(f"{formatted_date} is ready to load")
