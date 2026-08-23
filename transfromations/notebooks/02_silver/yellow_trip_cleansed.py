# Databricks notebook source
import sys
import os


project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from pyspark.sql.functions import col, when, timestamp_diff
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_month_start_n_months_ago

# COMMAND ----------


# The target month is the first month our backfill did not cover. The backfill
# ran 2025-12 .. 2026-04, so the month we do not have is 2026-05. The lecturer
# uses 2 (TLC's publishing lag); for us 2 would jump to 2026-06 and leave a gap.
target_month_start = get_month_start_n_months_ago(3)

# First day of the month after it - the upper bound of the window.
next_month_start = get_month_start_n_months_ago(2)

# COMMAND ----------


df = spark.read.table("nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw").filter(
    f"tpep_pickup_datetime >= '{target_month_start}' AND tpep_pickup_datetime < '{next_month_start}'"
)

# COMMAND ----------

# Select and transform fields, decoding codes and computing duration
df = df.select(
    # Map numeric VendorID to vendor names
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor"),

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    timestamp_diff(
        "MINUTE",
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime")
    ).alias("trip_duration"),

    "passenger_count",
    "trip_distance",

    when(col("RatecodeID") == 1, "Standard Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated Fare")
        .when(col("RatecodeID") == 6, "Group Ride")
        .otherwise("Unknown")
        .alias("rate_type"),

    "store_and_fwd_flag",

    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Unknown")
        .alias("payment_type"),

    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"
)

display(df)

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_cleansed")
