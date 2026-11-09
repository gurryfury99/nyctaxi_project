"""Join cleansed trips to the zone dimension (silver -> enriched)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ENRICHED_COLUMNS = [
    "vendor",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_duration",
    "passenger_count",
    "trip_distance",
    "rate_type",
    "pu_location_id",
    "do_location_id",
    "pu_borough",
    "do_borough",
    "pu_zone",
    "do_zone",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "processed_timestamp",
]


def current_zones(zones: DataFrame) -> DataFrame:
    """Only the active SCD2 version of each zone (end_date IS NULL).

    The original join used the whole table, so any zone with history would have
    multiplied the trips that matched it.
    """
    return zones.filter(F.col("end_date").isNull()).dropDuplicates(["location_id"])


def enrich_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Add pickup/dropoff borough and zone. One output row per input trip (tested)."""
    z = current_zones(zones).select("location_id", "borough", "zone")

    pickup = F.broadcast(
        z.select(
            F.col("location_id").alias("pu_location_id"),
            F.col("borough").alias("pu_borough"),
            F.col("zone").alias("pu_zone"),
        )
    )
    dropoff = F.broadcast(
        z.select(
            F.col("location_id").alias("do_location_id"),
            F.col("borough").alias("do_borough"),
            F.col("zone").alias("do_zone"),
        )
    )

    return (
        trips.join(pickup, "pu_location_id", "left")
        .join(dropoff, "do_location_id", "left")
        .select(ENRICHED_COLUMNS)
    )
