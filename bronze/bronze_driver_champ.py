
import requests
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, TimestampType

print("="*60)
print("Bronze Layer: Championship Drivers Ingestion")
print("Fetches championship standings from the FINAL race of each year")
print("="*60)

# Get ONLY the latest (final) race session per year for championship standings
race_sessions_query = """
WITH ranked_sessions AS (
  SELECT
    session_key,
    year,
    location,
    session_name,
    date_end,
    ROW_NUMBER() OVER (PARTITION BY year ORDER BY date_end DESC) AS rn
  FROM f1.bronze_sch.sessions
  WHERE session_name = 'Race'
    AND date_end < CURRENT_DATE
)
SELECT
  session_key,
  year,
  location,
  session_name,
  date_end
FROM ranked_sessions
WHERE rn = 1
ORDER BY year DESC
"""

print("\n[Step 1] Getting LATEST race session per year from bronze.sessions...")
try:
    race_sessions_df = spark.sql(race_sessions_query)
    race_sessions = race_sessions_df.collect()
    print(f"Found {len(race_sessions)} championship-deciding races (1 per year)")
    for session in race_sessions:
        print(f"  • Year {session['year']}: {session['location']} (session_key: {session['session_key']})")
except Exception as e:
    print(f"Error fetching sessions: {e}")
    print("Make sure bronze.sessions table exists and has data.")
    raise

# Check what's already loaded
print("\n[Step 2] Checking existing bronze data...")
try:
    loaded_sessions = spark.sql("""
        SELECT DISTINCT session_key
        FROM f1.bronze_sch.bronze_championship_drivers
    """).collect()
    loaded_session_keys = {row['session_key'] for row in loaded_sessions}
    print(f"Already loaded: {len(loaded_session_keys)} sessions")
except Exception:
    loaded_session_keys = set()
    print("Bronze table is empty - will load all sessions")

# Schema for bronze data (raw API response)
schema = StructType([
    StructField("meeting_key", LongType(), True),
    StructField("session_key", LongType(), True),
    StructField("driver_number", LongType(), True),
    StructField("position_start", LongType(), True),
    StructField("position_current", LongType(), True),
    StructField("points_start", DoubleType(), True),
    StructField("points_current", DoubleType(), True),
    StructField("ingestion_ts", TimestampType(), True)
])

# Fetch data from API
print("\n[Step 3] Fetching championship data from OpenF1 API...")
all_records = []
ingestion_ts = datetime.now()
processed = 0
skipped = 0
no_data = 0
errors = 0

for session in race_sessions:
    session_key = session['session_key']
    year = session['year']
    location = session['location']
    
    if session_key in loaded_session_keys:
        skipped += 1
        if skipped <= 3:  # Show first few
            print(f"  ⊙ Skipping session {session_key} (Year {year})")
        continue
    
    api_url = f"https://api.openf1.org/v1/championship_drivers?session_key={session_key}"
    
    try:
        response = requests.get(api_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if data:
                print(f"  ✓ Final race {session_key} (Year {year}, {location}): {len(data)} drivers")
                
                for record in data:
                    all_records.append(Row(
                        meeting_key=record.get('meeting_key'),
                        session_key=record.get('session_key'),
                        driver_number=record.get('driver_number'),
                        position_start=record.get('position_start'),
                        position_current=record.get('position_current'),
                        points_start=record.get('points_start'),
                        points_current=record.get('points_current'),
                        ingestion_ts=ingestion_ts
                    ))
                processed += 1
            else:
                no_data += 1
                if no_data <= 3:
                    print(f"  • Session {session_key}: No championship data available")
        else:
            errors += 1
            if errors <= 3:
                print(f"  ✗ Session {session_key}: API error {response.status_code}")
    
    except Exception as e:
        errors += 1
        if errors <= 3:
            print(f"  ✗ Session {session_key}: {str(e)}")

if skipped > 3:
    print(f"  ... and {skipped - 3} more sessions skipped")
if no_data > 3:
    print(f"  ... and {no_data - 3} more sessions with no data")
if errors > 3:
    print(f"  ... and {errors - 3} more errors")

# Write to bronze table
print("\n[Step 4] Writing to bronze table...")
if all_records:
    df = spark.createDataFrame(all_records, schema)
    print(f"Inserting {len(all_records)} records from {processed} sessions...")
    df.write.mode("append").saveAsTable("f1.bronze_sch.bronze_championship_drivers")
    
    print("\n" + "="*60)
    print("✓ Bronze ingestion complete!")
    print("="*60)
    print(f"Summary:")
    print(f"  • Sessions processed: {processed}")
    print(f"  • Sessions skipped (already loaded): {skipped}")
    print(f"  • Sessions with no data: {no_data}")
    print(f"  • Errors: {errors}")
    print(f"  • Total records inserted: {len(all_records)}")
else:
    print("\n" + "="*60)
    print("✓ No new data to load")
    print("="*60)
    print(f"  • Skipped: {skipped} | No data: {no_data} | Errors: {errors}")