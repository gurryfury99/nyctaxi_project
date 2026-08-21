# Databricks notebook source
import sys
import os

# Go three levels up to reach the project root.
# The lecturer's layout is two levels; ours has an extra `transfromations`
# folder, so these notebooks sit one level deeper.
project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import count, max, min, avg, sum, round
from dateutil.relativedelta import relativedelta
from datetime import date
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------

# Get the first day of the target month.
# The lecturer uses 2; our landing volume holds 2025-12 .. 2026-04, so 4
# selects 2026-04 - the newest month in our five-month scope.
two_months_ago_start = get_month_start_n_months_ago(4)

# COMMAND ----------

# Load the enriched trip dataset
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df = spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched").filter(
    f"tpep_pickup_datetime > '{two_months_ago_start}'"
)

# COMMAND ----------

# Aggregate trip data by pickup date with key metrics
# group records by calendar date
df = df.\
    groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(
        count("*").alias("total_trips"),
        round(avg("passenger_count"), 1).alias("average_passengers"),
        round(avg("trip_distance"), 1).alias("average_distance"),
        round(avg("fare_amount"), 2).alias("average_fare_per_trip"),
        max("fare_amount").alias("max_fare"),
        min("fare_amount").alias("min_fare"),
        round(sum("total_amount"), 2).alias("total_revenue")
    )

display(df)

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi_workspace.nyctaxi_03_gold.daily_trip_summary")
