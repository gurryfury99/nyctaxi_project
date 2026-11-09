from datetime import datetime

from modules.transformations.zones import (
    ZONE_SCHEMA,
    empty_zone_table,
    prepare_zone_source,
    scd2_next_state,
)

T0 = datetime(2026, 1, 1, 0, 0, 0)
T1 = datetime(2026, 6, 1, 0, 0, 0)
T2 = datetime(2026, 9, 1, 0, 0, 0)


def raw_zones(spark, rows):
    # explicit schema: an all-null column (e.g. service_zone) can't be type-inferred
    return spark.createDataFrame(
        rows, "LocationID string, Borough string, Zone string, service_zone string"
    )


def state(df):
    """Table state as sorted tuples for easy comparison."""
    return sorted(
        (r["location_id"], r["borough"], r["zone"], r["service_zone"], r["effective_date"], r["end_date"])
        for r in df.collect()
    )


def initial_load(spark):
    raw = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Jamaica Bay", "Boro Zone")])
    return scd2_next_state(empty_zone_table(spark), prepare_zone_source(raw, T0), T0)


def test_prepare_zone_source_shapes_the_csv(spark):
    raw = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("1", "EWR", "Newark Airport", "EWR")])
    out = prepare_zone_source(raw, T0)
    assert out.columns == ZONE_SCHEMA.fieldNames()
    assert out.count() == 1  # duplicates removed
    assert out.collect()[0]["location_id"] == 1  # cast to int
    assert out.collect()[0]["end_date"] is None


def test_first_load_inserts_everything_as_active(spark):
    first = initial_load(spark)
    assert state(first) == [
        (1, "EWR", "Newark Airport", "EWR", T0, None),
        (2, "Queens", "Jamaica Bay", "Boro Zone", T0, None),
    ]


def test_rerunning_the_same_snapshot_changes_nothing(spark):
    """The original notebook overwrote the table each run; this must be a no-op."""
    first = spark.createDataFrame(initial_load(spark).collect(), ZONE_SCHEMA)
    same = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Jamaica Bay", "Boro Zone")])
    second = scd2_next_state(first, prepare_zone_source(same, T1), T1)
    assert state(second) == state(first)


def test_a_change_closes_the_old_row_and_adds_a_new_version(spark):
    first = spark.createDataFrame(initial_load(spark).collect(), ZONE_SCHEMA)
    changed = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Jamaica Bay Park", "Boro Zone")])
    second = scd2_next_state(first, prepare_zone_source(changed, T1), T1)
    assert state(second) == [
        (1, "EWR", "Newark Airport", "EWR", T0, None),
        (2, "Queens", "Jamaica Bay", "Boro Zone", T0, T1),            # closed
        (2, "Queens", "Jamaica Bay Park", "Boro Zone", T1, None),     # new current version
    ]


def test_history_survives_later_runs(spark):
    first = spark.createDataFrame(initial_load(spark).collect(), ZONE_SCHEMA)
    v2 = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Park v2", "Boro Zone")])
    second = spark.createDataFrame(
        scd2_next_state(first, prepare_zone_source(v2, T1), T1).collect(), ZONE_SCHEMA
    )
    v3 = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Park v3", "Boro Zone")])
    third = scd2_next_state(second, prepare_zone_source(v3, T2), T2)
    zone2 = [row for row in state(third) if row[0] == 2]
    assert [(r[2], r[4], r[5]) for r in zone2] == [
        ("Jamaica Bay", T0, T1),
        ("Park v2", T1, T2),
        ("Park v3", T2, None),
    ]
    assert sum(1 for r in zone2 if r[5] is None) == 1  # exactly one active version


def test_new_location_is_inserted(spark):
    first = spark.createDataFrame(initial_load(spark).collect(), ZONE_SCHEMA)
    more = raw_zones(spark, [("1", "EWR", "Newark Airport", "EWR"), ("2", "Queens", "Jamaica Bay", "Boro Zone"),
                             ("3", "Bronx", "Allerton", "Boro Zone")])
    second = scd2_next_state(first, prepare_zone_source(more, T1), T1)
    assert (3, "Bronx", "Allerton", "Boro Zone", T1, None) in state(second)
    assert second.count() == 3


def test_null_to_value_counts_as_a_change(spark):
    """The original used != which ignores NULL comparisons, so these changes were missed."""
    first = spark.createDataFrame(
        scd2_next_state(
            empty_zone_table(spark),
            prepare_zone_source(raw_zones(spark, [("264", "Unknown", "N/A", None)]), T0),
            T0,
        ).collect(),
        ZONE_SCHEMA,
    )
    now_known = raw_zones(spark, [("264", "Unknown", "N/A", "Boro Zone")])
    second = scd2_next_state(first, prepare_zone_source(now_known, T1), T1)
    assert second.count() == 2
    assert second.filter("end_date IS NULL").collect()[0]["service_zone"] == "Boro Zone"
