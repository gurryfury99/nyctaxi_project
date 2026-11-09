# Databricks notebook source
import sys
import os

project_root = os.path.abspath(os.path.join(os.getcwd(), "../../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

from datetime import datetime

from modules.transformations.zones import (
    ZONE_SCHEMA,
    empty_zone_table,
    prepare_zone_source,
    scd2_next_state,
)

# COMMAND ----------

ZONE_TABLE = "nyctaxi_workspace.nyctaxi_02_silver.taxi_zone_lookup"

# Read from 00_landing, the same volume ingest_lookup writes the CSV to.
raw = (
    spark.read.format("csv")
    .option("header", True)
    .load("/Volumes/nyctaxi_workspace/00_landing/data_sources/lookup/taxi_zone_lookup.csv")
)

display(raw)

# COMMAND ----------

as_of = datetime.now()
incoming = prepare_zone_source(raw, as_of)

# COMMAND ----------

# The table as it is now. First run: it does not exist yet, so start from an empty one.
# (The previous version overwrote the table with the fresh CSV here, which threw away
# the very history this table exists to keep.)
if spark.catalog.tableExists(ZONE_TABLE):
    current = spark.read.table(ZONE_TABLE).select(ZONE_SCHEMA.fieldNames())
else:
    current = empty_zone_table(spark)

# COMMAND ----------

# SCD2: close changed rows, add their new versions, add brand-new locations, keep history.
# Re-running with an unchanged CSV leaves the table exactly as it was.
next_state = scd2_next_state(current, incoming, as_of)

# COMMAND ----------

# The table is ~265 rows, so materialise the new state before writing. That avoids
# reading and overwriting the same table in a single Spark plan.
# Overwrite is safe here because next_state already contains the full history.
(
    spark.createDataFrame(next_state.collect(), ZONE_SCHEMA)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ZONE_TABLE)
)

# COMMAND ----------

display(spark.read.table(ZONE_TABLE).orderBy("location_id", "effective_date"))
