from datetime import datetime

import pytest

from modules.transformations.cleansing import (
    SILVER_COLUMNS,
    add_reject_reasons,
    cleanse_trips,
)

RAW_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee", "processed_timestamp",
]


def make_trip(**overrides):
    """A valid trip; override single fields to build an invalid one."""
    row = {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime(2026, 5, 10, 8, 0, 0),
        "tpep_dropoff_datetime": datetime(2026, 5, 10, 8, 20, 0),
        "passenger_count": 1,
        "trip_distance": 3.2,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 161,
        "DOLocationID": 237,
        "payment_type": 1,
        "fare_amount": 18.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 27.0,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.75,
        "processed_timestamp": datetime(2026, 11, 9, 0, 0, 0),
    }
    row.update(overrides)
    return tuple(row[c] for c in RAW_COLUMNS)


RAW_SCHEMA = (
    "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp,"
    " passenger_count long, trip_distance double, RatecodeID long, store_and_fwd_flag string,"
    " PULocationID int, DOLocationID int, payment_type long, fare_amount double, extra double,"
    " mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double,"
    " total_amount double, congestion_surcharge double, Airport_fee double,"
    " cbd_congestion_fee double, processed_timestamp timestamp"
)


def frame(spark, *trips):
    # explicit schema: single-row frames with an all-null column can't be type-inferred
    return spark.createDataFrame(list(trips), RAW_SCHEMA)


def reasons(spark, trip, **window):
    df = add_reject_reasons(frame(spark, trip), **window)
    return df.collect()[0]["reject_reasons"]


def test_valid_trip_has_no_reasons(spark):
    assert reasons(spark, make_trip()) == []


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"tpep_dropoff_datetime": datetime(2026, 5, 10, 7, 0, 0)}, "dropoff_before_pickup"),
        ({"tpep_dropoff_datetime": datetime(2026, 5, 10, 8, 0, 0)}, "zero_duration"),
        ({"tpep_dropoff_datetime": datetime(2026, 5, 10, 15, 0, 0)}, "excessive_duration"),
        ({"trip_distance": 0.0}, "non_positive_distance"),
        ({"trip_distance": 250.0}, "excessive_distance"),
        ({"fare_amount": -5.0, "total_amount": -6.0}, "negative_amount"),
        ({"fare_amount": 900.0, "total_amount": 905.0}, "excessive_fare"),
        ({"passenger_count": 9}, "invalid_passenger_count"),
        ({"PULocationID": 0}, "invalid_location"),
        ({"DOLocationID": 999}, "invalid_location"),
        ({"DOLocationID": None}, "invalid_location"),
        ({"tpep_pickup_datetime": None}, "missing_timestamp"),
    ],
)
def test_each_rule_flags_its_reason(spark, overrides, expected):
    assert expected in reasons(spark, make_trip(**overrides))


def test_flex_fare_nulls_are_not_rejected(spark):
    """~25% of real rows are Flex Fare trips with null passenger_count / RatecodeID / surcharges."""
    flex = make_trip(
        payment_type=0, passenger_count=None, RatecodeID=None,
        congestion_surcharge=None, Airport_fee=None,
    )
    assert reasons(spark, flex) == []


def test_zero_passengers_is_kept(spark):
    assert reasons(spark, make_trip(passenger_count=0)) == []


def test_multiple_reasons_are_all_reported(spark):
    bad = make_trip(trip_distance=0.0, fare_amount=-3.0, total_amount=-3.0)
    assert set(reasons(spark, bad)) == {"non_positive_distance", "negative_amount"}


def test_window_flags_out_of_month_pickups(spark):
    window = {"window_start": "2026-05-01", "window_end": "2026-06-01"}
    in_window = make_trip()
    old = make_trip(tpep_pickup_datetime=datetime(2008, 12, 31, 23, 5, 53),
                    tpep_dropoff_datetime=datetime(2008, 12, 31, 23, 30, 0))
    next_month = make_trip(tpep_pickup_datetime=datetime(2026, 6, 1, 0, 20, 35),
                           tpep_dropoff_datetime=datetime(2026, 6, 1, 0, 40, 0))
    first_instant = make_trip(tpep_pickup_datetime=datetime(2026, 5, 1, 0, 0, 0),
                              tpep_dropoff_datetime=datetime(2026, 5, 1, 0, 10, 0))

    assert reasons(spark, in_window, **window) == []
    assert "outside_window" in reasons(spark, old, **window)
    assert "outside_window" in reasons(spark, next_month, **window)  # end is exclusive
    assert reasons(spark, first_instant, **window) == []  # start is inclusive


def test_no_rows_are_lost_between_clean_and_quarantine(spark):
    df = frame(
        spark,
        make_trip(),
        make_trip(trip_distance=0.0),
        make_trip(fare_amount=-1.0, total_amount=-1.0),
        make_trip(payment_type=0, passenger_count=None, RatecodeID=None),
    )
    clean, quarantine = cleanse_trips(df)
    assert clean.count() + quarantine.count() == df.count() == 4
    assert clean.count() == 2
    assert quarantine.count() == 2


def test_clean_output_schema_and_decoding(spark):
    df = frame(
        spark,
        make_trip(),
        make_trip(VendorID=99, RatecodeID=None, payment_type=0, passenger_count=None),
    )
    clean, _ = cleanse_trips(df)
    assert clean.columns == SILVER_COLUMNS
    assert "tip_amount" in clean.columns  # the original silver table dropped it

    rows = {r["vendor"]: r for r in clean.collect()}
    assert rows["Curb Mobility, LLC"]["rate_type"] == "Standard Rate"
    assert rows["Curb Mobility, LLC"]["payment_type"] == "Credit card"
    assert rows["Curb Mobility, LLC"]["trip_duration"] == 20
    assert rows["Curb Mobility, LLC"]["tip_amount"] == 4.0
    assert rows["Unknown"]["rate_type"] == "Unknown"
    assert rows["Unknown"]["payment_type"] == "Flex Fare trip"
    assert rows["Unknown"]["passenger_count"] is None


def test_airport_fee_column_name_case_is_tolerated(spark):
    """TLC has shipped both 'airport_fee' and 'Airport_fee'."""
    df = frame(spark, make_trip()).withColumnRenamed("Airport_fee", "airport_fee")
    clean, _ = cleanse_trips(df)
    assert clean.count() == 1
