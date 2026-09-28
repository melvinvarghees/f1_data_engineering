import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType
)
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession.builder.getOrCreate()


# ============================================================
# Configuration
# ============================================================

BRONZE_TABLE = "f1.bronze_sch.sessions"
OPENF1_URL = "https://api.openf1.org/v1/sessions"


# ============================================================
# Schema
# ============================================================

sessions_schema = StructType([
    StructField("circuit_key", IntegerType(), True),
    StructField("circuit_short_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country_key", IntegerType(), True),
    StructField("country_name", StringType(), True),
    StructField("date_end", TimestampType(), True),
    StructField("date_start", TimestampType(), True),
    StructField("gmt_offset", StringType(), True),
    StructField("location", StringType(), True),
    StructField("meeting_key", IntegerType(), True),
    StructField("session_key", IntegerType(), True),
    StructField("session_name", StringType(), True),
    StructField("session_type", StringType(), True),
    StructField("year", IntegerType(), True)
])


# ============================================================
# Create Bronze table
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    circuit_key INT,
    circuit_short_name STRING,
    country_code STRING,
    country_key INT,
    country_name STRING,
    date_end TIMESTAMP,
    date_start TIMESTAMP,
    gmt_offset STRING,
    location STRING,
    meeting_key INT,
    session_key INT,
    session_name STRING,
    session_type STRING,
    year INT,
    ingestion_ts TIMESTAMP,
    source STRING
)
USING DELTA
""")


# ============================================================
# Request sessions from OpenF1
# ============================================================

try:

    print("Loading sessions from OpenF1...")

    response = requests.get(
        OPENF1_URL,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()


    # --------------------------------------------------------
    # Validate response
    # --------------------------------------------------------

    if not isinstance(data, list):

        raise ValueError(
            "OpenF1 returned an unexpected response format."
        )


    if not data:

        print("No session data returned from OpenF1.")

    else:

        # ----------------------------------------------------
        # Create DataFrame
        # ----------------------------------------------------

        df = (
            spark.createDataFrame(
                data,
                schema=sessions_schema
            )
            .withColumn(
                "ingestion_ts",
                current_timestamp()
            )
            .withColumn(
                "source",
                lit("openf1")
            )
        )


        # ----------------------------------------------------
        # Temporary view
        # ----------------------------------------------------

        df.createOrReplaceTempView(
            "sessions_stg"
        )


        # ----------------------------------------------------
        # Merge into Bronze
        # ----------------------------------------------------

        spark.sql(f"""
        MERGE INTO {BRONZE_TABLE} AS target

        USING sessions_stg AS source

        ON target.session_key = source.session_key

        WHEN MATCHED THEN UPDATE SET
            target.circuit_key = source.circuit_key,
            target.circuit_short_name = source.circuit_short_name,
            target.country_code = source.country_code,
            target.country_key = source.country_key,
            target.country_name = source.country_name,
            target.date_end = source.date_end,
            target.date_start = source.date_start,
            target.gmt_offset = source.gmt_offset,
            target.location = source.location,
            target.meeting_key = source.meeting_key,
            target.session_name = source.session_name,
            target.session_type = source.session_type,
            target.year = source.year,
            target.ingestion_ts = source.ingestion_ts,
            target.source = source.source

        WHEN NOT MATCHED THEN INSERT (
            circuit_key,
            circuit_short_name,
            country_code,
            country_key,
            country_name,
            date_end,
            date_start,
            gmt_offset,
            location,
            meeting_key,
            session_key,
            session_name,
            session_type,
            year,
            ingestion_ts,
            source
        )
        VALUES (
            source.circuit_key,
            source.circuit_short_name,
            source.country_code,
            source.country_key,
            source.country_name,
            source.date_end,
            source.date_start,
            source.gmt_offset,
            source.location,
            source.meeting_key,
            source.session_key,
            source.session_name,
            source.session_type,
            source.year,
            source.ingestion_ts,
            source.source
        )
        """)


        print(
            f"Successfully processed "
            f"{df.count()} sessions."
        )


except requests.exceptions.Timeout:

    print("OpenF1 request timed out.")


except requests.exceptions.HTTPError as e:

    print(f"OpenF1 HTTP error: {e}")


except Exception as e:

    print(f"Failed loading sessions: {e}")


print("Sessions Bronze ingestion completed.")