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

    "valid_session_name":
        "session_name IS NOT NULL",

    "valid_session_type":
        "session_type IS NOT NULL",

    "valid_date_start":
        "date_start IS NOT NULL",

    "valid_date_end":
        "date_end IS NOT NULL",

    "valid_year":
        "year IS NOT NULL AND year >= 1950"

})
@dp.table(
    name="silver_sessions"
)
def silver_sessions():

    return (

        spark.readStream
        .table(
            "f1.bronze_sch.sessions"
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
        # Derived session duration
        # ====================================================

        .withColumn(
            "session_duration_minutes",
            (
                (
                    col("date_end").cast("long")
                    -
                    col("date_start").cast("long")
                ) / 60
            )
        )

        # ====================================================
        # Keep gmt_offset for local-time analysis
        # ====================================================

        .drop("source")
    )