from pyspark import pipelines as dp

from pyspark.sql.functions import col


# ============================================================
# Silver driver data
# ============================================================

@dp.expect_all_or_drop({

    "valid_session_key":
        "session_key IS NOT NULL",

    "valid_meeting_key":
        "meeting_key IS NOT NULL",

    "valid_driver_number":
        "driver_number IS NOT NULL",

    "valid_driver_name":
        "full_name IS NOT NULL",

    "valid_team":
        "team_name IS NOT NULL",

    "valid_headshot_url":
        "headshot_url IS NOT NULL"

})


@dp.table(
    name="silver_drivers"
)


def silver_drivers():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.drivers"
        )

        # ====================================================
        # Standardize column names / values
        # ====================================================

        .withColumn(
            "full_name",
            col("full_name").cast("string")
        )

        .withColumn(
            "team_name",
            col("team_name").cast("string")
        )

        # ====================================================
        # Remove unnecessary technical metadata
        # ====================================================

        .drop("source")

    )