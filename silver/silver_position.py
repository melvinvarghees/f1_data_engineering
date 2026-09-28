from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    to_timestamp
)


# ============================================================
# Data quality expectations
# ============================================================

@dp.expect_all_or_drop({

    "valid_session_key":
        "session_key IS NOT NULL",

    "valid_meeting_key":
        "meeting_key IS NOT NULL",

    "valid_driver_number":
        "driver_number IS NOT NULL",

    "valid_date":
        "date IS NOT NULL",

    "valid_position":
        "position IS NOT NULL AND position > 0"

})
@dp.table(
    name="silver_position"
)
def silver_position():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.position"
        )

        # ====================================================
        # Standardize timestamp
        # ====================================================

        .withColumn(
            "date",
            to_timestamp(col("date"))
        )

        # ====================================================
        # Remove ingestion-specific metadata
        # ====================================================

        .drop("source")
    )