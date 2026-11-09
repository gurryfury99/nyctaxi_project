"""Cleansing rules for Yellow Taxi trips (bronze -> silver).

Design:
  * Rows are never silently dropped. Each raw row gets a ``reject_reasons`` array;
    rows with at least one reason go to a quarantine table, the rest continue to silver.
  * Rules were chosen from profiling real TLC files (2026-01 and 2026-05):
    out-of-month pickups (some from 2008), zero/negative durations, zero-distance trips,
    negative fares (refunds/disputes), and a few extreme distances/durations/fares.
  * Nulls in passenger_count, RatecodeID and the surcharge columns are NOT errors:
    roughly a quarter of rows are Flex Fare trips (payment_type 0) where those fields
    are legitimately empty. They are kept.

All functions are pure DataFrame -> DataFrame so they can be tested on local Spark.
"""

from typing import Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# --- code lookups (from the TLC data dictionary) -------------------------------------

VENDORS = {
    1: "Creative Mobile Technologies, LLC",
    2: "Curb Mobility, LLC",
    6: "Myle Technologies Inc",
    7: "Helix",
}

RATE_TYPES = {
    1: "Standard Rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated Fare",
    6: "Group Ride",
}

PAYMENT_TYPES = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    6: "Voided trip",
}

# --- validation thresholds (tune here, in one place) ---------------------------------

MAX_TRIP_DISTANCE_MILES = 100
MAX_TRIP_DURATION_MINUTES = 360  # 6 hours
MAX_FARE_AMOUNT = 500
MAX_PASSENGERS = 6
MAX_LOCATION_ID = 265  # taxi_zone_lookup has IDs 1..265

SILVER_COLUMNS = [
    "vendor",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_duration",
    "passenger_count",
    "trip_distance",
    "rate_type",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
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


def decode(col: Column, mapping: dict, default: str = "Unknown") -> Column:
    """Map numeric codes to labels. Nulls and unmapped codes become ``default``."""
    expr = None
    for code, label in mapping.items():
        expr = F.when(col == code, label) if expr is None else expr.when(col == code, label)
    return expr.otherwise(default)


def _ci(df: DataFrame, name: str) -> Column:
    """Column lookup ignoring case. TLC has shipped both 'airport_fee' and 'Airport_fee'."""
    lookup = {c.lower(): c for c in df.columns}
    return F.col(lookup[name.lower()])


def add_reject_reasons(
    df: DataFrame,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
) -> DataFrame:
    """Add ``reject_reasons`` (array<string>) to raw bronze rows. Empty array = valid.

    window_start / window_end (inclusive / exclusive, 'YYYY-MM-DD') restrict pickups to the
    month being processed; anything outside is flagged ``outside_window``.
    """
    pickup = _ci(df, "tpep_pickup_datetime")
    dropoff = _ci(df, "tpep_dropoff_datetime")
    duration_sec = F.timestamp_diff("SECOND", pickup, dropoff)

    pu = _ci(df, "PULocationID")
    do = _ci(df, "DOLocationID")

    checks = [
        ("missing_timestamp", pickup.isNull() | dropoff.isNull()),
        ("dropoff_before_pickup", duration_sec < 0),
        ("zero_duration", duration_sec == 0),
        ("excessive_duration", duration_sec > MAX_TRIP_DURATION_MINUTES * 60),
        ("non_positive_distance", _ci(df, "trip_distance") <= 0),
        ("excessive_distance", _ci(df, "trip_distance") > MAX_TRIP_DISTANCE_MILES),
        ("negative_amount", (_ci(df, "fare_amount") < 0) | (_ci(df, "total_amount") < 0)),
        ("excessive_fare", _ci(df, "fare_amount") > MAX_FARE_AMOUNT),
        # null passenger_count is allowed (Flex Fare trips); only impossible counts are rejected
        ("invalid_passenger_count", _ci(df, "passenger_count") > MAX_PASSENGERS),
        (
            "invalid_location",
            pu.isNull() | do.isNull() | ~pu.between(1, MAX_LOCATION_ID) | ~do.between(1, MAX_LOCATION_ID),
        ),
    ]

    if window_start is not None and window_end is not None:
        checks.append(
            (
                "outside_window",
                ~((pickup >= F.lit(str(window_start))) & (pickup < F.lit(str(window_end)))),
            )
        )

    flagged = F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(reason)) for reason, cond in checks])
    return df.withColumn("reject_reasons", F.filter(flagged, lambda x: x.isNotNull()))


def to_silver_trips(df: DataFrame) -> DataFrame:
    """Decode codes, compute trip_duration (whole minutes) and rename to silver columns.

    Expects rows that already passed validation. Keeps tip_amount (the original silver
    table dropped it, which made tip analysis impossible).
    """
    return df.select(
        decode(_ci(df, "VendorID"), VENDORS).alias("vendor"),
        _ci(df, "tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        _ci(df, "tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        F.timestamp_diff(
            "MINUTE", _ci(df, "tpep_pickup_datetime"), _ci(df, "tpep_dropoff_datetime")
        ).alias("trip_duration"),
        _ci(df, "passenger_count").alias("passenger_count"),
        _ci(df, "trip_distance").alias("trip_distance"),
        decode(_ci(df, "RatecodeID"), RATE_TYPES).alias("rate_type"),
        _ci(df, "store_and_fwd_flag").alias("store_and_fwd_flag"),
        _ci(df, "PULocationID").alias("pu_location_id"),
        _ci(df, "DOLocationID").alias("do_location_id"),
        decode(_ci(df, "payment_type"), PAYMENT_TYPES).alias("payment_type"),
        _ci(df, "fare_amount").alias("fare_amount"),
        _ci(df, "extra").alias("extra"),
        _ci(df, "mta_tax").alias("mta_tax"),
        _ci(df, "tip_amount").alias("tip_amount"),
        _ci(df, "tolls_amount").alias("tolls_amount"),
        _ci(df, "improvement_surcharge").alias("improvement_surcharge"),
        _ci(df, "total_amount").alias("total_amount"),
        _ci(df, "congestion_surcharge").alias("congestion_surcharge"),
        _ci(df, "airport_fee").alias("airport_fee"),
        _ci(df, "cbd_congestion_fee").alias("cbd_congestion_fee"),
        _ci(df, "processed_timestamp").alias("processed_timestamp"),
    )


def cleanse_trips(
    raw: DataFrame,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Split raw bronze trips into (clean silver rows, quarantined raw rows + reasons).

    Invariant (tested): clean.count() + quarantine.count() == raw.count().
    """
    flagged = add_reject_reasons(raw, window_start, window_end)
    valid_raw = flagged.filter(F.size("reject_reasons") == 0).drop("reject_reasons")
    quarantine = flagged.filter(F.size("reject_reasons") > 0)
    return to_silver_trips(valid_raw), quarantine
