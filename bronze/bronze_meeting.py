import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

url = "https://api.openf1.org/v1/meetings"

data = requests.get(url).json()

df = spark.createDataFrame(data) \
          .withColumn("ingestion_ts", current_timestamp())

df.createOrReplaceTempView("meetings_stg")

spark.sql("""
CREATE TABLE IF NOT EXISTS f1.bronze_sch.meetings
USING DELTA
AS
SELECT * FROM meetings_stg WHERE 1=0
""")

spark.sql("""
MERGE INTO f1.bronze_sch.meetings t
USING meetings_stg s
ON t.meeting_key = s.meeting_key
WHEN NOT MATCHED THEN
INSERT *
""")

print("Meetings loaded successfully")