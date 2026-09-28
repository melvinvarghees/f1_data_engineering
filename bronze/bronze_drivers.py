import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession.builder.getOrCreate()


# ============================================================
# Configuration
# ============================================================

BRONZE_TABLE = "f1.bronze_sch.drivers"
OPENF1_URL = "https://api.openf1.org/v1/drivers"


# ============================================================
# Schema
# ============================================================

drivers_schema = StructType([
    StructField("broadcast_name", StringType(), True),
    StructField("driver_number", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("headshot_url", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("meeting_key", IntegerType(), True),
    StructField("name_acronym", StringType(), True),
    StructField("session_key", IntegerType(), True),
    StructField("team_colour", StringType(), True),
    StructField("team_name", StringType(), True)
])


# ============================================================
# Create Bronze table
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    broadcast_name STRING,
    driver_number INT,
    first_name STRING,
    full_name STRING,
    headshot_url STRING,
    last_name STRING,
    meeting_key INT,
    name_acronym STRING,
    session_key INT,
    team_colour STRING,
    team_name STRING,
    ingestion_ts TIMESTAMP,
    source STRING
)
USING DELTA
""")


# ============================================================
# Find Race sessions not already loaded
# ============================================================

sql_query = f"""
SELECT DISTINCT s.session_key
FROM f1.bronze_sch.sessions s

LEFT JOIN (
    SELECT DISTINCT session_key
    FROM {BRONZE_TABLE}
) d

ON s.session_key = d.session_key

WHERE s.session_name = 'Race'
AND d.session_key IS NULL
AND s.session_key IS NOT NULL
"""


session_keys = [
    row["session_key"]
    for row in spark.sql(sql_query).collect()
]


print(
    f"Found {len(session_keys)} Race sessions "
    f"to load driver data for"
)


# ============================================================
# HTTP session
# ============================================================

http = requests.Session()


# ============================================================
# Load driver data
# ============================================================

for session_key in session_keys:

    params = {
        "session_key": session_key
    }

    try:

        print(
            f"Loading drivers for session {session_key}..."
        )

        response = http.get(
            OPENF1_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()


        # ----------------------------------------------------
        # Handle empty response
        # ----------------------------------------------------

        if not data:

            print(
                f"No driver data returned "
                f"for session {session_key}"
            )

            continue


        # ----------------------------------------------------
        # Create DataFrame
        # ----------------------------------------------------

        df = (
            spark.createDataFrame(
                data,
                schema=drivers_schema
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
        # Write to Bronze
        # ----------------------------------------------------

        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(BRONZE_TABLE)
        )


        record_count = df.count()

        print(
            f"Successfully loaded {record_count} "
            f"drivers for session {session_key}"
        )


    except requests.exceptions.Timeout:

        print(
            f"Request timeout for session "
            f"{session_key}"
        )


    except requests.exceptions.HTTPError as e:

        print(
            f"HTTP error for session "
            f"{session_key}: {e}"
        )


    except Exception as e:

        print(
            f"Failed loading session "
            f"{session_key}: {e}"
        )


print("Drivers Bronze ingestion completed")