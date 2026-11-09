# Databricks notebook source
# MAGIC %md
# MAGIC # Rebuild bronze with `source_month`
# MAGIC
# MAGIC The live pipeline replaces one month at a time using a `source_month` column
# MAGIC (`replaceWhere source_month = 'YYYY-MM'`). A bronze table created by the original
# MAGIC initial-load notebook does not have that column.
# MAGIC
# MAGIC Run this **once** to rebuild `yellow_trips_raw` from the landing files, tagging each
# MAGIC row with the month of the file it came from. After that, the monthly job can load
# MAGIC (or re-load) any month safely, including all the historical ones, with
# MAGIC `target_month` set - no separate backfill code is needed.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Use whichever landing schema actually holds the backfill files (the initial load used
# nyctaxi_landing, the live pipeline uses 00_landing).
LANDING = "/Volumes/nyctaxi_workspace/nyctaxi_landing/data_sources/nyctaxi_yellow/*"

df = (
    spark.read.format("parquet")
    .load(LANDING)
    .withColumn("processed_timestamp", F.current_timestamp())
    # the month comes from the file name: .../yellow_tripdata_2026-03.parquet -> 2026-03
    .withColumn(
        "source_month",
        F.regexp_extract(F.col("_metadata.file_path"), r"yellow_tripdata_(\d{4}-\d{2})", 1),
    )
)

# COMMAND ----------

display(df.groupBy("source_month").count().orderBy("source_month"))

# COMMAND ----------

# Replaces the whole table once. Check the counts above first.
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw"
)
