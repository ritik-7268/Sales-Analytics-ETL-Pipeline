"""
Silver Layer - Data Cleaning and Transformation
This script cleans the raw data by removing duplicates, handling nulls, and standardizing formats
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, upper

# Create Spark session
spark = SparkSession.builder \
    .appName("Silver Layer - Data Cleaning") \
    .getOrCreate()

# Read data from Bronze layer
input_path = "../output/bronze/sales_raw"
output_path = "../output/silver/sales_cleaned"

print(f"\nReading data from Bronze layer: {input_path}")
df_bronze = spark.read.parquet(input_path)

print(f"\nRecords before cleaning: {df_bronze.count()}")

# Step 1: Remove duplicate records based on order_id
print("\nStep 1: Removing duplicate orders...")
df_deduplicated = df_bronze.dropDuplicates(["order_id"])

print(f"Records after removing duplicates: {df_deduplicated.count()}")

# Step 2: Handle missing customer_id (filter out or set default)
print("\nStep 2: Handling missing customer IDs...")
df_filtered = df_deduplicated.filter(col("customer_id").isNotNull())

print(f"Records after removing null customer_id: {df_filtered.count()}")

# Step 3: Standardize text columns (trim spaces, uppercase)
print("\nStep 3: Standardizing text columns...")
df_standardized = df_filtered \
    .withColumn("customer_id", trim(upper(col("customer_id")))) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", trim(upper(col("category")))) \
    .withColumn("region", trim(upper(col("region"))))

# Step 4: Convert order_date to proper date format
print("\nStep 4: Converting date format...")
df_cleaned = df_standardized \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    # It REPLACES the existing order_date column

# Step 5: Add calculated column - total_amount
print("\nStep 5: Adding calculated columns...")
df_silver = df_cleaned \
    .withColumn("total_amount", col("quantity") * col("price"))

# Show sample cleaned data
print("\nSample Silver Data:")
df_silver.select("order_id", "customer_id", "product_name", 
                 "quantity", "price", "total_amount", "order_date", "region").show(5)

print(f"\nFinal Record Count in Silver Layer: {df_silver.count()}")
print("\nSchema:")
df_silver.printSchema()

# Save to Silver layer
print(f"\nSaving cleaned data to Silver layer: {output_path}")
df_silver.write \
    .mode("overwrite") \
    .parquet(output_path)

# Stop Spark session
spark.stop()