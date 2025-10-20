# Databricks notebook source
"""
SmartLogix Transport Optimization - Data Loading
This notebook loads and processes the transport optimization data.
"""

# COMMAND ----------

# Import required libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

# Add project root to path
sys.path.append('/Workspace/Repos/your-username/SmartLogix-Transport-Optimization')

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SmartLogixTransportOptimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data Files

# COMMAND ----------

# Define data paths
data_path = "/FileStore/shared_uploads/smartlogix/"
tables = {
    'delhivery_data': 'delhivery.csv',
    'distance_data': 'distance.csv',
    'order_data_small': 'order_small.csv',
    'order_data_large': 'order_large.csv',
    'supply_chain_data': 'supply_chain_data.csv'
}

# COMMAND ----------

# Load Delhivery data
delhivery_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{data_path}delhivery.csv")

# Display basic info
print(f"Delhivery data loaded: {delhivery_df.count()} records")
delhivery_df.printSchema()

# COMMAND ----------

# Load distance data
distance_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{data_path}distance.csv")

print(f"Distance data loaded: {distance_df.count()} records")
distance_df.show(5)

# COMMAND ----------

# Load order data
order_small_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{data_path}order_small.csv")

order_large_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{data_path}order_large.csv")

# Add order size column
order_small_df = order_small_df.withColumn("order_size", lit("small"))
order_large_df = order_large_df.withColumn("order_size", lit("large"))

# Combine order data
order_df = order_small_df.union(order_large_df)

print(f"Order data loaded: {order_df.count()} records")
order_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save as Delta Tables

# COMMAND ----------

# Save as Delta tables for better performance
delhivery_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.smartlogix.delhivery_data")

distance_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.smartlogix.distance_data")

order_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.smartlogix.order_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Check

# COMMAND ----------

# Check data quality
print("=== Data Quality Report ===")
print(f"Delhivery records: {spark.table('default.smartlogix.delhivery_data').count()}")
print(f"Distance records: {spark.table('default.smartlogix.distance_data').count()}")
print(f"Order records: {spark.table('default.smartlogix.order_data').count()}")

# Check for null values
delhivery_nulls = spark.table('default.smartlogix.delhivery_data').select(
    [count(when(col(c).isNull(), c)).alias(c) for c in spark.table('default.smartlogix.delhivery_data').columns]
).collect()[0]

print("\nDelhivery data null counts:")
for col_name, null_count in zip(spark.table('default.smartlogix.delhivery_data').columns, delhivery_nulls):
    if null_count > 0:
        print(f"  {col_name}: {null_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Analytics Views

# COMMAND ----------

# Create transport summary view
spark.sql("""
CREATE OR REPLACE VIEW default.smartlogix.transport_summary AS
SELECT 
    source_center,
    destination_center,
    COUNT(*) as trip_count,
    AVG(actual_time) as avg_actual_time,
    AVG(osrm_time) as avg_osrm_time,
    AVG(factor) as avg_factor,
    AVG(actual_distance_to_destination) as avg_distance
FROM default.smartlogix.delhivery_data 
GROUP BY source_center, destination_center
""")

# Create order summary view
spark.sql("""
CREATE OR REPLACE VIEW default.smartlogix.order_summary AS
SELECT 
    source,
    destination,
    order_size,
    COUNT(*) as order_count,
    SUM(weight) as total_weight,
    AVG(EXTRACT(EPOCH FROM (deadline - available_time))/3600) as avg_hours_to_deadline
FROM default.smartlogix.order_data 
GROUP BY source, destination, order_size
""")

print("Analytics views created successfully!")

# COMMAND ----------

# Display summary statistics
print("=== Transport Summary ===")
spark.table("default.smartlogix.transport_summary").show(10)

print("\n=== Order Summary ===")
spark.table("default.smartlogix.order_summary").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Complete!
# MAGIC 
# MAGIC The data has been successfully loaded into Databricks Delta tables:
# MAGIC - `default.smartlogix.delhivery_data`
# MAGIC - `default.smartlogix.distance_data` 
# MAGIC - `default.smartlogix.order_data`
# MAGIC 
# MAGIC Analytics views have been created for easy querying and dashboard integration.
