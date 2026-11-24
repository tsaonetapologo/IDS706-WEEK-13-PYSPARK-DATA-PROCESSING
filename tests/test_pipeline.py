from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline import build_valid_trips


def test_build_valid_trips_filters_invalid_rows_and_derives_features(tmp_path):
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("NYC Taxi Pipeline Test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    schema = StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
        ]
    )
    pickup = datetime(2024, 1, 1, 12)
    rows = [
        (pickup, pickup + timedelta(minutes=10), 2.5, 100, 10.0, 12.0),
        (pickup, pickup, 2.5, 100, 10.0, 12.0),
        (
            datetime(2022, 1, 1, 12),
            datetime(2022, 1, 1, 12, 10),
            2.5,
            100,
            10.0,
            12.0,
        ),
    ]

    try:
        input_path = tmp_path / "trips.parquet"
        spark.createDataFrame(rows, schema).write.parquet(str(input_path))
        trips = build_valid_trips(spark, input_path, 2023, 2024)

        assert trips.count() == 1
        result = trips.select(
            "trip_duration_min", "pickup_date", "pickup_month", "pickup_zone_id"
        ).first()
        assert result.trip_duration_min == 10.0
        assert result.pickup_date.isoformat() == "2024-01-01"
        assert result.pickup_month == "2024-01"
        assert result.pickup_zone_id == 100
    finally:
        spark.stop()
