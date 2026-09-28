import requests
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    TimestampType
)
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    to_timestamp
)

# ============================================================
# Spark Session
# ============================================================

spark = SparkSession.builder.getOrCreate()

# ============================================================
# Configuration
# ============================================================

BRONZE_TABLE = "f1.bronze_sch.position"
OPENF1_URL = "https://api.openf1.org/v1/position"

# ============================================================
# Schema
# ============================================================

position_schema = StructType([
    StructField("date", StringType(), True),
    StructField("driver_number", LongType(), True),
    StructField("meeting_key", LongType(), True),
    StructField("position", IntegerType(), True),
    StructField("session_key", LongType(), True)
])

# ============================================================
# Create table if not exists
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
(
    date TIMESTAMP,
    driver_number BIGINT,
    meeting_key BIGINT,
    position INT,
    session_key BIGINT,
    ingestion_ts TIMESTAMP,
    source STRING
)
USING DELTA
""")

# ============================================================
# Determine sessions not yet loaded
# ============================================================

session_sql = f"""
SELECT DISTINCT s.session_key
FROM f1.bronze_sch.sessions s

LEFT JOIN (
    SELECT DISTINCT session_key
    FROM {BRONZE_TABLE}
) p
ON s.session_key = p.session_key

WHERE p.session_key IS NULL
AND s.session_key IS NOT NULL
ORDER BY s.session_key
"""

session_keys = [
    row["session_key"]
    for row in spark.sql(session_sql).collect()
]

print(f"Found {len(session_keys)} sessions to process")

# ============================================================
# HTTP session
# ============================================================

http = requests.Session()

# ============================================================
# Process each session
# ============================================================

for session_key in session_keys:

    try:

        print(f"Processing session {session_key}")

        # Add delay to respect rate limits (adjust as needed)
        time.sleep(2)  # 2 second delay between requests

        response = http.get(
            OPENF1_URL,
            params={"session_key": session_key},
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        # ----------------------------------------------------
        # Validation
        # ----------------------------------------------------

        if not isinstance(data, list):
            print(
                f"Unexpected API response for "
                f"session {session_key}"
            )
            continue

        if len(data) == 0:
            print(
                f"No position data found for "
                f"session {session_key}"
            )
            continue

        # ----------------------------------------------------
        # Create dataframe
        # ----------------------------------------------------

        df = (
            spark.createDataFrame(
                data,
                schema=position_schema
            )
            .withColumn(
                "date",
                to_timestamp("date")
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

        record_count = df.count()

        # ----------------------------------------------------
        # Write to Delta
        # ----------------------------------------------------

        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(BRONZE_TABLE)
        )

        print(
            f"Loaded {record_count} records "
            f"for session {session_key}"
        )

    except requests.exceptions.Timeout:

        print(
            f"Timeout while loading "
            f"session {session_key}"
        )

    except requests.exceptions.HTTPError as e:

        # Handle rate limiting specifically
        if e.response is not None and e.response.status_code == 429:
            print(
                f"Rate limit hit for session {session_key}. "
                f"Waiting 60 seconds before continuing..."
            )
            time.sleep(60)  # Wait longer on rate limit
        else:
            print(
                f"HTTP error for "
                f"session {session_key}: {e}"
            )

    except Exception as e:

        print(
            f"Failed for "
            f"session {session_key}: {str(e)}"
        )

print("Position Bronze ingestion completed")