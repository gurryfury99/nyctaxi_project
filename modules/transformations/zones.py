"""Taxi zone dimension with Slowly Changing Dimension Type 2 history.

The original notebook overwrote ``taxi_zone_lookup`` with the fresh CSV on every run and
then merged the CSV against itself, so changes were never detected and history was lost.

Here the SCD2 logic is a pure function: given the *current table state* and the *incoming
snapshot*, it returns the *new table state*. The notebook reads the table, calls this, and
writes the result back. Running it twice with the same snapshot is a no-op (tested).
"""

from functools import reduce
from typing import Union
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

TRACKED_COLUMNS = ["borough", "zone", "service_zone"]

ZONE_SCHEMA = StructType(
    [
        StructField("location_id", IntegerType()),
        StructField("borough", StringType()),
        StructField("zone", StringType()),
        StructField("service_zone", StringType()),
        StructField("effective_date", TimestampType()),
        StructField("end_date", TimestampType()),
    ]
)


def empty_zone_table(spark: SparkSession) -> DataFrame:
    """An empty table with the zone schema, for the very first load."""
    return spark.createDataFrame([], ZONE_SCHEMA)


def prepare_zone_source(raw: DataFrame, as_of: Union[datetime, str]) -> DataFrame:
    """Shape the raw taxi_zone_lookup.csv into the zone schema (one row per location_id)."""
    return (
        raw.select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"),
        )
        .dropDuplicates(["location_id"])
        .withColumn("effective_date", F.lit(as_of).cast("timestamp"))
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .select(ZONE_SCHEMA.fieldNames())
    )


def scd2_next_state(current: DataFrame, incoming: DataFrame, as_of: Union[datetime, str]) -> DataFrame:
    """Compute the next full state of the SCD2 zone table.

    - Active rows (end_date IS NULL) whose tracked attributes changed are closed
      (end_date = as_of) and a new active version is added (effective_date = as_of).
    - Unchanged active rows and all history rows are kept as they are.
    - Locations never seen before are added as new active rows.
    - Locations missing from the incoming snapshot stay active (not silently closed).
    - Comparison is null-safe, so NULL -> value counts as a change and NULL = NULL does not.
    """
    cols = ZONE_SCHEMA.fieldNames()
    incoming = incoming.dropDuplicates(["location_id"])

    active = current.filter(F.col("end_date").isNull())
    history = current.filter(F.col("end_date").isNotNull())

    a, i = active.alias("a"), incoming.alias("i")
    changed_cond = reduce(
        lambda x, y: x | y,
        [~F.col(f"a.{c}").eqNullSafe(F.col(f"i.{c}")) for c in TRACKED_COLUMNS],
    )
    changed_ids = (
        a.join(i, F.col("a.location_id") == F.col("i.location_id"))
        .filter(changed_cond)
        .select(F.col("a.location_id").alias("location_id"))
        .distinct()
    )

    unchanged_active = active.join(changed_ids, "location_id", "left_anti")
    closed = active.join(changed_ids, "location_id", "left_semi").withColumn(
        "end_date", F.lit(as_of).cast("timestamp")
    )
    new_versions = incoming.join(changed_ids, "location_id", "left_semi")
    known_ids = current.select("location_id").distinct()
    brand_new = incoming.join(known_ids, "location_id", "left_anti")

    parts = [history, unchanged_active, closed, new_versions, brand_new]
    return reduce(lambda x, y: x.unionByName(y), [p.select(cols) for p in parts])
