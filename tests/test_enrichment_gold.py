from datetime import datetime

from modules.transformations.cleansing import SILVER_COLUMNS
from modules.transformations.enrichment import ENRICHED_COLUMNS, enrich_with_zones
from modules.transformations.gold import daily_trip_summary
from modules.transformations.zones import ZONE_SCHEMA

T0 = datetime(2026, 1, 1)
T1 = datetime(2026, 6, 1)


def trips(spark, rows):
    """rows: (pu, do, pickup, fare, total, passengers, distance)"""
    data = []
    for pu, do, pickup, fare, total, pax, dist in rows:
        values = {c: None for c in SILVER_COLUMNS}
        values.update(
            vendor="Curb Mobility, LLC", tpep_pickup_datetime=pickup,
            tpep_dropoff_datetime=pickup, trip_duration=10, passenger_count=pax,
            trip_distance=dist, rate_type="Standard Rate", pu_location_id=pu,
            do_location_id=do, payment_type="Credit card", fare_amount=fare,
            total_amount=total, tip_amount=1.0,
        )
        data.append(tuple(values[c] for c in SILVER_COLUMNS))
    schema = (
        "vendor string, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, trip_duration int,"
        " passenger_count long, trip_distance double, rate_type string, store_and_fwd_flag string,"
        " pu_location_id int, do_location_id int, payment_type string, fare_amount double, extra double,"
        " mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double,"
        " total_amount double, congestion_surcharge double, airport_fee double, cbd_congestion_fee double,"
        " processed_timestamp timestamp"
    )
    return spark.createDataFrame(data, schema)


def zones(spark, rows):
    return spark.createDataFrame(rows, ZONE_SCHEMA)


def test_enrichment_adds_borough_and_zone(spark):
    z = zones(spark, [
        (1, "Manhattan", "Midtown", "Yellow Zone", T0, None),
        (2, "Queens", "JFK", "Airports", T0, None),
    ])
    t = trips(spark, [(1, 2, datetime(2026, 5, 2, 9), 10.0, 12.0, 1, 2.0)])
    row = enrich_with_zones(t, z).collect()[0]
    assert (row["pu_borough"], row["pu_zone"]) == ("Manhattan", "Midtown")
    assert (row["do_borough"], row["do_zone"]) == ("Queens", "JFK")


def test_enrichment_schema(spark):
    z = zones(spark, [(1, "Manhattan", "Midtown", "Yellow Zone", T0, None)])
    t = trips(spark, [(1, 1, datetime(2026, 5, 2, 9), 10.0, 12.0, 1, 2.0)])
    assert enrich_with_zones(t, z).columns == ENRICHED_COLUMNS


def test_zone_history_does_not_duplicate_trips(spark):
    """Regression: joining the whole SCD2 table (old + current rows) multiplied matching trips."""
    z = zones(spark, [
        (1, "Manhattan", "Midtown", "Yellow Zone", T0, T1),           # old version, closed
        (1, "Manhattan", "Midtown East", "Yellow Zone", T1, None),    # current version
        (2, "Queens", "JFK", "Airports", T0, None),
    ])
    t = trips(spark, [
        (1, 2, datetime(2026, 5, 2, 9), 10.0, 12.0, 1, 2.0),
        (2, 1, datetime(2026, 5, 2, 10), 20.0, 24.0, 2, 5.0),
    ])
    out = enrich_with_zones(t, z)
    assert out.count() == t.count() == 2
    rows = {r["pu_location_id"]: r for r in out.collect()}
    assert rows[1]["pu_zone"] == "Midtown East"  # current version, not the closed one
    assert rows[2]["do_zone"] == "Midtown East"


def test_unknown_location_keeps_the_trip_with_null_zone(spark):
    z = zones(spark, [(1, "Manhattan", "Midtown", "Yellow Zone", T0, None)])
    t = trips(spark, [(1, 77, datetime(2026, 5, 2, 9), 10.0, 12.0, 1, 2.0)])
    out = enrich_with_zones(t, z)
    assert out.count() == 1
    assert out.collect()[0]["do_borough"] is None


def test_daily_summary(spark):
    z = zones(spark, [(1, "Manhattan", "Midtown", "Yellow Zone", T0, None)])
    t = trips(spark, [
        (1, 1, datetime(2026, 5, 2, 9), 10.0, 12.0, 1, 2.0),
        (1, 1, datetime(2026, 5, 2, 23), 30.0, 36.0, 3, 6.0),
        (1, 1, datetime(2026, 5, 3, 0, 5), 8.0, 9.5, 1, 1.0),
    ])
    out = {str(r["pickup_date"]): r for r in daily_trip_summary(enrich_with_zones(t, z)).collect()}
    may2 = out["2026-05-02"]
    assert may2["total_trips"] == 2
    assert may2["average_passengers"] == 2.0
    assert may2["average_distance"] == 4.0
    assert may2["average_fare_per_trip"] == 20.0
    assert (may2["max_fare"], may2["min_fare"]) == (30.0, 10.0)
    assert may2["total_revenue"] == 48.0
    assert out["2026-05-03"]["total_trips"] == 1
