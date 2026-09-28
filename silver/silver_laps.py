from pyspark import pipelines as dp

from pyspark.sql.functions import col, to_timestamp


# ============================================================
# Silver data quality expectations
# ============================================================

@dp.expect_all_or_drop({

    "valid_session_key":
        "session_key IS NOT NULL",

    "valid_meeting_key":
        "meeting_key IS NOT NULL",

    "valid_driver_number":
        "driver_number IS NOT NULL",

    "valid_lap_number":
        "lap_number IS NOT NULL AND lap_number > 0",

    "valid_date_start":
        "date_start IS NOT NULL",

    "valid_lap_duration":
        "lap_duration IS NOT NULL AND lap_duration >= 0",

    "valid_sector_1":
        "duration_sector_1 IS NOT NULL AND duration_sector_1 >= 0",

    "valid_sector_2":
        "duration_sector_2 IS NOT NULL AND duration_sector_2 >= 0",

    "valid_sector_3":
        "duration_sector_3 IS NOT NULL AND duration_sector_3 >= 0"

})
@dp.table(
    name="silver_laps"
)
def silver_laps():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.laps"
        )

        # ====================================================
        # Standardize timestamp
        # ====================================================

        .withColumn(
            "date_start",
            to_timestamp(col("date_start"))
        )

        # ====================================================
        # Remove ingestion-specific metadata
        # ====================================================

        .drop("source")

    )