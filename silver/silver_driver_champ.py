from pyspark import pipelines as dp

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DoubleType, TimestampType


# ============================================================
# Silver championship driver data
# ============================================================

@dp.expect_all_or_drop({

    "valid_session_key":
        "session_key IS NOT NULL",

    "valid_meeting_key":
        "meeting_key IS NOT NULL",

    "valid_driver_number":
        "driver_number IS NOT NULL",

    "valid_position_current":
        "position_current IS NOT NULL AND position_current > 0",

    "valid_points_current":
        "points_current IS NOT NULL AND points_current >= 0"

})


@dp.table(
    name="silver_championship_drivers"
)


def silver_championship_drivers():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.bronze_championship_drivers"
        )

        # ====================================================
        # Standardize column types
        # ====================================================

        .withColumn("meeting_key", col("meeting_key").cast(LongType()))
        .withColumn("session_key", col("session_key").cast(LongType()))
        .withColumn("driver_number", col("driver_number").cast(LongType()))
        .withColumn("position_start", col("position_start").cast(LongType()))
        .withColumn("position_current", col("position_current").cast(LongType()))
        .withColumn("points_start", col("points_start").cast(DoubleType()))
        .withColumn("points_current", col("points_current").cast(DoubleType()))
        .withColumn("ingestion_ts", col("ingestion_ts").cast(TimestampType()))

    )