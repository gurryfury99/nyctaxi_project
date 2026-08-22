# Databricks notebook source
import sys
import os

# Go three levels up to reach the project root.
# The lecturer's layout is two levels; ours has an extra `transfromations`
# folder, so these notebooks sit one level deeper.
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

import urllib.request
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_target_yyyymm
from modules.data_loader.file_downloader import download_file

# COMMAND ----------

formatted_date = get_target_yyyymm(4)

# Define the local directory for this date's data
dir_path = f"/Volumes/nyctaxi_workspace/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

# Define the full path for the downloaded file
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

# COMMAND ----------

try:
    # Check if the file already exists
    dbutils.fs.ls(local_path)

    # If the file already exists then set continue_downstream to no
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks")

except:
    try:
        # Construct the URL for the Parquet file corresponding to this month
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        # Download the file
        # Create the local directory for this date's data
        download_file(url, dir_path, local_path)

        # Set continue_downstream to yes if the file was loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File succesfully uploaded in current run")

    except Exception as e:
        # Set continue downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")
