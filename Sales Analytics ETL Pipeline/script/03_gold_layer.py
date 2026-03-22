"""
Gold Layer - Business Metrics and Aggregations
This script creates analytical datasets for business reporting
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, round, month, year

# Create Spark session
spark = SparkSession.builder \
    .appName("Gold Layer - Business Analytics") \
    .getOrCreate()


# Read data from Silver layer
input_path = "../output/silver/sales_cleaned"
print(f"\nReading cleaned data from Silver layer: {input_path}")
df_silver = spark.read.parquet(input_path)


# Analytics 1: Monthly Sales Summary
df_monthly = df_silver \
    .withColumn("year", year(col("order_date"))) \
    .withColumn("month", month(col("order_date"))) \
    .groupBy("year", "month") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    ) \
    .orderBy("year", "month")

print("\nMonthly Sales Summary:")
df_monthly.show()

# Save monthly summary
output_monthly = "../output/gold/monthly_sales_summary"
df_monthly.write.mode("overwrite").parquet(output_monthly)
print(f"Saved to: {output_monthly}")

# Analytics 2: Top 5 Products by Revenue
print("\n" + "=" * 50)
print("Creating Top Products Report")
print("=" * 50)

df_top_products = df_silver \
    .groupBy("product_id", "product_name") \
    .agg(
        sum("total_amount").alias("total_revenue"),
        sum("quantity").alias("total_quantity_sold"),
        count("order_id").alias("order_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

print("\nTop 5 Products by Revenue:")
df_top_products.show()

# Save top products
output_products = "../output/gold/top_products"
df_top_products.write.mode("overwrite").parquet(output_products)
print(f"Saved to: {output_products}")

# Analytics 3: Region-wise Performance
df_region = df_silver \
    .groupBy("region") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    ) \
    .orderBy(col("total_revenue").desc())

print("\nRegion-wise Performance:")
df_region.show()

# Save region performance
output_region = "../output/gold/region_performance"
df_region.write.mode("overwrite").parquet(output_region)
print(f"Saved to: {output_region}")


# Analytics 4: Top 5 Customers by Revenue
df_top_customers = df_silver \
    .groupBy("customer_id") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

print("\nTop 5 Customers by Revenue:")
df_top_customers.show()

# Save top customers
output_customers = "../output/gold/top_customers"
df_top_customers.write.mode("overwrite").parquet(output_customers)
print(f"Saved to: {output_customers}")

# Summary Statistics
total_revenue = df_silver.agg(sum("total_amount")).collect()[0][0] ## [0][0] - 1st row 1st col
total_orders = df_silver.count()
avg_order_val = df_silver.agg(avg("total_amount")).collect()[0][0]

print(f"\nTotal Revenue: ₹{total_revenue:,.2f}")
print(f"Total Orders: {total_orders}")
print(f"Average Order Value: ₹{avg_order_val:,.2f}")

# Stop Spark session
spark.stop()
