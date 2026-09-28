from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    to_timestamp
)


# ============================================================
# Data quality expectations
# ============================================================

@dp.expect_all_or_drop({

    "valid_meeting_key":
        "meeting_key IS NOT NULL",

    "valid_circuit_key":
        "circuit_key IS NOT NULL",

    "valid_meeting_name":
        "meeting_name IS NOT NULL",

    "valid_year":
        "year IS NOT NULL AND year >= 1950",

    "valid_date_start":
        "date_start IS NOT NULL",

    "valid_date_end":
        "date_end IS NOT NULL"

})
@dp.table(
    name="silver_meeting"
)
def silver_meeting():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.meetings"
        )

        # ====================================================
        # Standardize timestamps
        # ====================================================

        .withColumn(
            "date_start",
            to_timestamp(col("date_start"))
        )

        .withColumn(
            "date_end",
            to_timestamp(col("date_end"))
        )

        # ====================================================
        # Add useful derived fields
        # ====================================================

        .withColumn(
            "meeting_duration_hours",
            (
                (
                    col("date_end").cast("long")
                    -
                    col("date_start").cast("long")
                ) / 3600
            )
        )

        # ====================================================
        # Remove technical metadata only
        # ====================================================

        .drop("source")
    )