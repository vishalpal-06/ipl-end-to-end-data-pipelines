from pyspark.sql import SparkSession

# Initialize SparkSession if not already
spark = SparkSession.builder.getOrCreate()

# 1️⃣ Read max_delivery_ts from Task 0
max_delivery_ts = dbutils.jobs.taskValues.get(
    taskKey="Extract_Max_ts",    # <-- name of your Task 0 in job
    key="max_delivery_ts",       # <-- key used in Task 0
    debugValue="2026-01-01 00:00:00"
)

print(f"Deleting records with ingestion_ts <= {max_delivery_ts}")

# 2️⃣ Delete records from delivery_loading
spark.sql(f"""
    DELETE FROM bronze.delivery_loading
    WHERE ingestion_ts <= '{max_delivery_ts}'
""")

# 3️⃣ Delete records from match_loading
spark.sql(f"""
    DELETE FROM bronze.match_loading
    WHERE ingestion_ts <= '{max_delivery_ts}'
""")

print("Deletion complete!")
