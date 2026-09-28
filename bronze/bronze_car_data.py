import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lit
)

# ============================================================
# Initialize Spark Session
# ============================================================

spark = SparkSession.builder.getOrCreate()

# ============================================================
# Define Schema
# ============================================================

car_data_schema = StructType([
    StructField("brake", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("driver_number", IntegerType(), True),
    StructField("drs", IntegerType(), True),
    StructField("meeting_key", IntegerType(), True),
    StructField("n_gear", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("session_key", IntegerType(), True),
    StructField("speed", IntegerType(), True),
    StructField("throttle", IntegerType(), True)
])

# ============================================================
# Check if table exists, create if it doesn't
# ============================================================

table_name = "f1.bronze_sch.car_data"

if not spark.catalog.tableExists(table_name):
    print(f"Table {table_name} does not exist. Creating...")
    
    # Create empty DataFrame with full schema (including metadata columns)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            brake INT,
            date TIMESTAMP,
            driver_number INT,
            drs INT,
            meeting_key INT,
            n_gear INT,
            rpm INT,
            session_key INT,
            speed INT,
            throttle INT,
            ingestion_ts TIMESTAMP,
            source STRING
        )
        USING DELTA
    """)
    
    print(f"✅ Table {table_name} created successfully")
else:
    print(f"Table {table_name} already exists")

# ============================================================
# Get only Race sessions NOT already loaded
# ============================================================

sql_query = """
WITH session_n AS
(
    SELECT DISTINCT
           s.session_key,
           DATE_FORMAT(
               CAST(s.date_start AS TIMESTAMP),
               'yyyy-MM-dd HH:mm:ss'
           ) AS normal_date
    FROM f1.bronze_sch.sessions s

    LEFT JOIN
    (
        SELECT DISTINCT session_key
        FROM f1.bronze_sch.car_data
    ) t
    ON s.session_key = t.session_key

    WHERE s.session_name = 'Race'
      AND s.date_start <= CURRENT_DATE
      AND t.session_key IS NULL
)

SELECT session_key
FROM session_n
ORDER BY normal_date ASC
"""

session_rows = spark.sql(sql_query).collect()

session_keys = [
    row["session_key"]
    for row in session_rows
]

print(f"Found {len(session_keys)} new Race sessions to process")

# ============================================================
# Process each Session
# ============================================================

for key in session_keys:

    try:

        # ====================================================
        # Additional duplicate check (safety check)
        # ====================================================

        existing_count = spark.sql(f"""
        SELECT COUNT(*)
        FROM f1.bronze_sch.car_data
        WHERE session_key = {key}
        """).collect()[0][0]

        if existing_count > 0:

            print(
                f"Session {key} already exists "
                f"({existing_count} rows). Skipping..."
            )
            continue

        # ====================================================
        # API URL
        # ====================================================

        url = (
            f"https://api.openf1.org/v1/car_data"
            f"?session_key={key}"
            f"&speed>=300"
        )

        print(f"Processing session {key}")

        response = requests.get(
            url,
            timeout=60
        )

        response.raise_for_status()

        data = response.json()

        # ====================================================
        # Validate response
        # ====================================================

        if (
            data
            and isinstance(data, list)
            and len(data) > 0
        ):

            first_record = data[0]

            if (
                isinstance(first_record, dict)
                and "session_key" in first_record
            ):

                df = spark.createDataFrame(
                    data,
                    schema=car_data_schema
                )

                df = (
                    df
                    .withColumn(
                        "drs",
                        coalesce(col("drs"), lit(0))
                    )
                    .withColumn(
                        "ingestion_ts",
                        current_timestamp()
                    )
                    .withColumn(
                        "source",
                        lit("openf1_api")
                    )
                )

                (
                    df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(
                        "f1.bronze_sch.car_data"
                    )
                )

                print(
                    f"✅ Successfully processed "
                    f"session {key} "
                    f"({len(data)} rows)"
                )

            else:

                print(
                    f"❌ Invalid structure "
                    f"for session {key}"
                )

        else:

            print(
                f"No data found for session {key}"
            )

    except requests.exceptions.HTTPError as e:

        print(
            f"HTTP Error for session "
            f"{key}: {e}"
        )

    except Exception as e:

        print(
            f"Error processing session "
            f"{key}: {e}"
        )

print("\n✅ Car data ingestion completed.")