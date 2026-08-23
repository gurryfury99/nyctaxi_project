# Databricks notebook source
# MAGIC %md
# MAGIC # Yellow taxi EDA 2
# MAGIC
# MAGIC Row counts per month for every table in the pipeline. Run this before and
# MAGIC after `nyctaxi_job` to confirm the target month was purged and reloaded.
# MAGIC
# MAGIC Backfill loaded five months, 2025-12 through 2026-04. The job adds the
# MAGIC sixth, 2026-05, so after a successful run this should show six months.

# COMMAND ----------

from pyspark.sql.functions import date_format, count, sum

# COMMAND ----------

# MAGIC %md ### 01_bronze - yellow_trips_raw

# COMMAND ----------

spark.read.table("nyctaxi_workspace.nyctaxi_01_bronze.yellow_trips_raw") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

# MAGIC %md ### 02_silver - yellow_trip_cleansed

# COMMAND ----------

spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trip_cleansed") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

# MAGIC %md ### 02_silver - yellow_trips_enriched

# COMMAND ----------

spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.yellow_trips_enriched") \
    .groupBy(date_format("tpep_pickup_datetime", "yyyy-MM").alias("year_month")) \
    .agg(count("*").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

# MAGIC %md ### 03_gold - daily_trip_summary
# MAGIC
# MAGIC Gold is already aggregated, so sum `total_trips` rather than counting rows.

# COMMAND ----------

spark.read.table("nyctaxi_workspace.nyctaxi_03_gold.daily_trip_summary") \
    .groupBy(date_format("pickup_date", "yyyy-MM").alias("year_month")) \
    .agg(sum("total_trips").alias("total_records")) \
    .orderBy("year_month") \
    .display()

# COMMAND ----------

# MAGIC %md ### 02_silver - taxi_zone_lookup
# MAGIC
# MAGIC No date column. Open records are the current versions; closed records have
# MAGIC an `end_date`, written by the SCD2 merge in the lookup notebook.

# COMMAND ----------

spark.sql("""
    SELECT
        CASE WHEN end_date IS NULL THEN 'open' ELSE 'closed' END AS record_state,
        COUNT(*) AS total_records
    FROM nyctaxi_workspace.nyctaxi_02_silver.taxi_zone_lookup
    GROUP BY 1
""").display()
