# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/Volumes/nyctaxi_workspace/nyctaxi_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date"))

# COMMAND ----------

display(df)

# COMMAND ----------

spark.sql("USE CATALOG nyctaxi_workspace")
spark.sql("USE SCHEMA nyctaxi_02_silver")

df.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("taxi_zone_lookup")

# COMMAND ----------

display(spark.read.table("nyctaxi_workspace.nyctaxi_02_silver.taxi_zone_lookup"))

# COMMAND ----------

from delta.tables import DeltaTable
from datetime import datetime

dt = DeltaTable.forName(
    spark,
    "nyctaxi_workspace.nyctaxi_02_silver.taxi_zone_lookup"
)

end_timestamp = datetime.now()

# COMMAND ----------

# PASS 1: Close active records whose attributes changed

dt.alias("t") \
    .merge(
        source=df.alias("s"),
        condition="""
            t.location_id = s.location_id
            AND t.end_date IS NULL
            AND (
                t.borough != s.borough
                OR t.zone != s.zone
                OR t.service_zone != s.service_zone
            )
        """
    ) \
    .whenMatchedUpdate(
        set={
            "end_date": lit(end_timestamp).cast(TimestampType())
        }
    ) \
    .execute()

# COMMAND ----------

# PASS 2: Insert the new current versions of changed records

closed_ids = [
    row.location_id
    for row in dt.toDF()
        .where(f"end_date = '{end_timestamp}'")
        .select("location_id")
        .collect()
]

if len(closed_ids) == 0:
    print("No updated records to insert")
else:
    dt.alias("t") \
        .merge(
            source=df.alias("s"),
            condition=f"s.location_id IN ({','.join(map(str, closed_ids))})"
        ) \
        .whenNotMatchedInsert(
            values={
                "location_id": "s.location_id",
                "borough": "s.borough",
                "zone": "s.zone",
                "service_zone": "s.service_zone",
                "effective_date": "s.effective_date",
                "end_date": "NULL"
            }
        ) \
        .execute()

# COMMAND ----------

# PASS 3: Insert completely new locations

dt.alias("t") \
    .merge(
        source=df.alias("s"),
        condition="t.location_id = s.location_id"
    ) \
    .whenNotMatchedInsert(
        values={
            "location_id": "s.location_id",
            "borough": "s.borough",
            "zone": "s.zone",
            "service_zone": "s.service_zone",
            "effective_date": "s.effective_date",
            "end_date": "NULL"
        }
    ) \
    .execute()
