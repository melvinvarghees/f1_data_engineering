import requests
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    BooleanType,
    ArrayType,
    IntegerType
)

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

# ============================================================
# Spark Session
# ============================================================

spark = SparkSession.builder.getOrCreate()

# ============================================================
# Configuration
# ============================================================

BRONZE_TABLE = "f1.bronze_sch.laps"

OPENF1_URL = "https://api.openf1.org/v1/laps"

REQUEST_TIMEOUT = 60
REQUEST_DELAY = 1

# ============================================================
# Schema
# ============================================================

laps_schema = StructType([
    StructField("date_start", StringType(), True),
    StructField("driver_number", LongType(), True),
    StructField("duration_sector_1", DoubleType(), True),
    StructField("duration_sector_2", DoubleType(), True),
    StructField("duration_sector_3", DoubleType(), True),
    StructField("i1_speed", LongType(), True),
    StructField("i2_speed", LongType(), True),
    StructField("is_pit_out_lap", BooleanType(), True),
    StructField("lap_duration", DoubleType(), True),
    StructField("lap_number", LongType(), True),
    StructField("meeting_key", LongType(), True),
    StructField("segments_sector_1", ArrayType(IntegerType()), True),
    StructField("segments_sector_2", ArrayType(IntegerType()), True),
    StructField("segments_sector_3", ArrayType(IntegerType()), True),
    StructField("session_key", LongType(), True),
    StructField("st_speed", LongType(), True)
])

# ============================================================
# Get Unloaded Race Sessions
# ============================================================

sql_query = f"""
SELECT DISTINCT s.session_key

FROM f1.bronze_sch.sessions s

LEFT JOIN
(
    SELECT DISTINCT session_key
    FROM {BRONZE_TABLE}
) l
ON s.session_key = l.session_key

WHERE s.session_name = 'Race'
  AND s.session_key IS NOT NULL
  AND CAST(s.date_start AS TIMESTAMP) <= CURRENT_TIMESTAMP()
  AND l.session_key IS NULL

ORDER BY s.session_key
"""

session_keys = [
    row["session_key"]
    for row in spark.sql(sql_query).collect()
]

print(f"Found {len(session_keys)} race sessions to load")

# ============================================================
# HTTP Session
# ============================================================

http = requests.Session()

http.headers.update({
    "User-Agent": "OpenF1 Bronze Ingestion"
})

# ============================================================
# Load Data
# ============================================================

total_sessions = 0
total_rows = 0
failed_sessions = []

for session_key in session_keys:

    try:

        print(f"\nProcessing session_key={session_key}")

        response = http.get(
            OPENF1_URL,
            params={"session_key": session_key},
            timeout=REQUEST_TIMEOUT
        )

        # ----------------------------------------------------
        # Rate Limit Handling
        # ----------------------------------------------------

        if response.status_code == 429:

            print(
                f"Rate limited for session "
                f"{session_key}. Retrying later."
            )

            failed_sessions.append(session_key)

            time.sleep(10)

            continue

        response.raise_for_status()

        data = response.json()

        # ----------------------------------------------------
        # Validation
        # ----------------------------------------------------

        if not isinstance(data, list):

            print(
                f"Invalid API response for "
                f"session {session_key}"
            )

            failed_sessions.append(session_key)
            continue

        if len(data) == 0:

            print(
                f"No lap data found for "
                f"session {session_key}"
            )

            continue

        # ----------------------------------------------------
        # Create DataFrame
        # ----------------------------------------------------

        df = (
            spark.createDataFrame(
                data,
                schema=laps_schema
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

        row_count = df.count()

        # ----------------------------------------------------
        # Write Delta
        # ----------------------------------------------------

        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(BRONZE_TABLE)
        )

        total_sessions += 1
        total_rows += row_count

        print(
            f"Successfully loaded "
            f"{row_count} rows"
        )

        time.sleep(REQUEST_DELAY)

    except Exception as e:

        print(
            f"Failed session "
            f"{session_key}: {e}"
        )

        traceback.print_exc()

        failed_sessions.append(session_key)

# ============================================================
# Summary
# ============================================================

print("\n========================================")
print("LAPS INGESTION COMPLETE")
print("========================================")
print(f"Sessions Loaded : {total_sessions}")
print(f"Rows Loaded     : {total_rows}")
print(f"Sessions Failed : {len(failed_sessions)}")

if failed_sessions:
    print(
        f"Failed Session Keys: "
        f"{failed_sessions}"
    )