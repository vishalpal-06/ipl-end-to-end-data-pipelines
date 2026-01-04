from pyspark.sql.functions import max, col

# Extract max ingestion timestamps
max_match_ts = spark.table("bronze.match_loading").agg(max("ingestion_ts")).collect()[0][0]
max_delivery_ts = spark.table("bronze.delivery_loading").agg(max("ingestion_ts")).collect()[0][0]

# Pass these as task outputs
dbutils.jobs.taskValues.set(key="max_match_ts", value=str(max_match_ts))
dbutils.jobs.taskValues.set(key="max_delivery_ts", value=str(max_delivery_ts))
