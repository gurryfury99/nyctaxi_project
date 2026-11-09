"""Gold-layer aggregations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_trip_summary(enriched: DataFrame) -> DataFrame:
    """One row per pickup date with the key trip and revenue metrics.

    Same metrics as the original notebook, so existing consumers keep working.
    """
    return enriched.groupBy(F.col("tpep_pickup_datetime").cast("date").alias("pickup_date")).agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("passenger_count"), 1).alias("average_passengers"),
        F.round(F.avg("trip_distance"), 1).alias("average_distance"),
        F.round(F.avg("fare_amount"), 2).alias("average_fare_per_trip"),
        F.max("fare_amount").alias("max_fare"),
        F.min("fare_amount").alias("min_fare"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )
