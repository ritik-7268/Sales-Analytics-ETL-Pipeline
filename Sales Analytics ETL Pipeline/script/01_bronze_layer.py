"""
Bronze Layer - Raw Data Ingestion
This script reads raw CSV data and saves it to Bronze layer without any transformation
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Create Spark session
spark = SparkSession.builder \
    .appName("Bronze Layer - Sales Data Ingestion") \
    .getOrCreate()

# Read raw CSV file
input_path = "../data/raw/sales_data.csv"
output_path = "../output/bronze/sales_raw"

print(f"\nReading raw data from: {input_path}")

# Load data with header and infer schema
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

# Add ingestion timestamp to track when data was loaded
df_bronze = df_raw.withColumn("ingestion_time", 
                               spark.sql.functions.current_timestamp())

# Show sample data
print("\nSample Bronze Data:")
df_bronze.show(5)

# Show data statistics
print(f"\nTotal Records in Bronze Layer: {df_bronze.count()}")
print("\nSchema:")
df_bronze.printSchema()

# Save to Bronze layer as parquet (better performance than CSV)
print(f"\nSaving data to Bronze layer: {output_path}")
df_bronze.write \
    .mode("overwrite") \
    .parquet(output_path)

# Stop Spark session
spark.stop()