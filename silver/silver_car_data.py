from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    when
)
from pyspark.sql.types import LongType


# ============================================================
# Data quality expectations
# ============================================================

@dp.expect_all_or_drop({
    "valid_session_key":
        "session_key IS NOT NULL",

    "valid_driver_number":
        "driver_number IS NOT NULL",

    "valid_date":
        "date IS NOT NULL",

    "valid_speed":
        "speed >= 0",

    "valid_rpm":
        "rpm >= 0",

    "valid_throttle":
        "throttle >= 0 AND throttle <= 100",

    "valid_brake":
        "brake >= 0 AND brake <= 100",

    "valid_gear":
        "n_gear >= 0 AND n_gear <= 8"
})
@dp.table(
    name="silver_car_data"
)
def silver_car_data():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.car_data"
        )

        # ====================================================
        # Cast all integer columns to LongType for schema consistency
        # ====================================================

        .withColumn("brake", col("brake").cast(LongType()))
        .withColumn("driver_number", col("driver_number").cast(LongType()))
        .withColumn("drs", col("drs").cast(LongType()))
        .withColumn("meeting_key", col("meeting_key").cast(LongType()))
        .withColumn("n_gear", col("n_gear").cast(LongType()))
        .withColumn("rpm", col("rpm").cast(LongType()))
        .withColumn("session_key", col("session_key").cast(LongType()))
        .withColumn("speed", col("speed").cast(LongType()))
        .withColumn("throttle", col("throttle").cast(LongType()))

        # ====================================================
        # Convert DRS code into business-friendly status
        # ====================================================

        .withColumn(
            "drs_status",

            when(
                col("drs").isin(0, 1),
                "OFF"
            )

            .when(
                col("drs").isin(10, 12, 14),
                "ON"
            )

            .otherwise(
                "UNKNOWN"
            )
        )

        # ====================================================
        # Remove API-specific DRS code
        # ====================================================

        .drop("drs")
    )