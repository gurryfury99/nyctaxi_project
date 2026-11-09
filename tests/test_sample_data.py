"""End-to-end check on a real TLC month. Slower (~1 min); run with: pytest -m sample_data"""

import os
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from modules.transformations.cleansing import cleanse_trips
from modules.transformations.enrichment import enrich_with_zones
from modules.transformations.gold import daily_trip_summary
from modules.transformations.zones import ZONE_SCHEMA
from modules.utils.date_utils import month_window_for

PARQUET = os.path.join(
    os.path.dirname(__file__), "..", "one_off", "nyctaxi_data", "2026-05", "yellow_tripdata_2026-05.parquet"
)

pytestmark = [
    pytest.mark.sample_data,
    pytest.mark.skipif(not os.path.exists(PARQUET), reason="sample parquet not present"),
]


@pytest.fixture(scope="module")
def pipeline(spark):
    start, end = month_window_for("2026-05")
    raw = spark.read.parquet(PARQUET).withColumn("processed_timestamp", F.current_timestamp())
    clean, quarantine = cleanse_trips(raw, str(start), str(end))
    clean = clean.cache()
    quarantine = quarantine.cache()

    zones = spark.createDataFrame(
        [(i, "Borough", f"Zone {i}", "Boro Zone", datetime(2026, 1, 1), None) for i in range(1, 266)],
        ZONE_SCHEMA,
    )
    enriched = enrich_with_zones(clean, zones)
    return {"raw": raw, "clean": clean, "quarantine": quarantine, "enriched": enriched,
            "start": str(start), "end": str(end)}


def test_no_rows_lost(pipeline):
    total = pipeline["raw"].count()
    assert pipeline["clean"].count() + pipeline["quarantine"].count() == total


def test_clean_rows_satisfy_every_rule(pipeline):
    bad = pipeline["clean"].filter(
        (F.col("trip_distance") <= 0)
        | (F.col("fare_amount") < 0)
        | (F.col("total_amount") < 0)
        | (F.col("trip_duration") < 0)
        | (F.col("tpep_pickup_datetime") < F.lit(pipeline["start"]))
        | (F.col("tpep_pickup_datetime") >= F.lit(pipeline["end"]))
    )
    assert bad.count() == 0


def test_flex_fare_trips_survive(pipeline):
    """~25% of real rows have null passenger_count/RatecodeID; they must not be dropped."""
    assert pipeline["clean"].filter(F.col("payment_type") == "Flex Fare trip").count() > 500_000


def test_quarantine_explains_every_rejection(pipeline):
    q = pipeline["quarantine"]
    assert q.filter(F.size("reject_reasons") == 0).count() == 0
    assert q.filter(F.array_contains("reject_reasons", "outside_window")).count() > 0


def test_enrichment_is_one_row_per_trip(pipeline):
    assert pipeline["enriched"].count() == pipeline["clean"].count()


def test_gold_dates_stay_inside_the_month(pipeline):
    dates = daily_trip_summary(pipeline["enriched"]).agg(F.min("pickup_date"), F.max("pickup_date")).collect()[0]
    assert str(dates[0]) >= pipeline["start"]
    assert str(dates[1]) < pipeline["end"]
