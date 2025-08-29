#!/usr/bin/env python3
"""
Advanced example: Using Iceberg's time travel and metadata features
"""

import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sys.path.append('/home/glue_functions')
from iceberg_glue_functions import (
    read_iceberg_table_at_timestamp,
    read_iceberg_table_at_snapshot,
    query_iceberg_table_history,
    query_iceberg_table_snapshots,
    get_iceberg_table_metadata
)

def create_spark_session():
    """Create Spark session for advanced Iceberg features"""
    
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']
    
    return SparkSession.builder \
        .appName("Advanced-Iceberg-Features") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token", session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .getOrCreate()

def time_travel_example():
    """Example: Time travel queries"""
    
    print("⏰ Time Travel Example")
    print("=" * 30)
    
    spark = create_spark_session()
    database_name = "analytics"
    table_name = "customer_transactions"
    
    try:
        # 1. Read current data
        current_df = spark.read.format("iceberg").load(f"glue_catalog.{database_name}.{table_name}")
        print(f"Current data count: {current_df.count()}")
        
        # 2. Read data as it was yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S.000')
        
        historical_df = spark.read.format("iceberg") \
            .option("as-of-timestamp", yesterday) \
            .load(f"glue_catalog.{database_name}.{table_name}")
        
        print(f"Yesterday's data count: {historical_df.count()}")
        
        # 3. Compare changes
        current_ids = set([row.customer_id for row in current_df.select("customer_id").collect()])
        historical_ids = set([row.customer_id for row in historical_df.select("customer_id").collect()])
        
        new_customers = current_ids - historical_ids
        print(f"New customers since yesterday: {len(new_customers)}")
        
        return current_df, historical_df
        
    finally:
        spark.stop()

def metadata_analysis_example():
    """Example: Analyzing table metadata and history"""
    
    print("📊 Metadata Analysis Example")
    print("=" * 35)
    
    spark = create_spark_session()
    database_name = "analytics"
    table_name = "customer_transactions"
    
    try:
        # 1. Get table history
        print("📚 Table History:")
        history_df = query_iceberg_table_history(spark, database_name, table_name)
        
        # 2. Get snapshots
        print("📸 Table Snapshots:")
        snapshots_df = query_iceberg_table_snapshots(spark, database_name, table_name)
        
        # 3. Analyze table evolution
        print("📈 Table Evolution Analysis:")
        
        # Count operations by type
        operation_counts = history_df.groupBy("operation").count().collect()
        for row in operation_counts:
            print(f"  {row.operation}: {row.count} times")
        
        # Show recent changes
        print("🕐 Recent Changes (last 5):")
        recent_changes = history_df.orderBy(desc("made_current_at")).limit(5)
        recent_changes.select("made_current_at", "operation", "snapshot_id").show(truncate=False)
        
        return history_df, snapshots_df
        
    finally:
        spark.stop()

def snapshot_comparison_example():
    """Example: Compare data between specific snapshots"""
    
    print("🔍 Snapshot Comparison Example")
    print("=" * 40)
    
    spark = create_spark_session()
    database_name = "analytics"
    table_name = "customer_transactions"
    
    try:
        # Get available snapshots
        snapshots_df = spark.read.format("iceberg") \
            .load(f"glue_catalog.{database_name}.{table_name}.snapshots")
        
        snapshots = snapshots_df.select("snapshot_id", "committed_at").orderBy(desc("committed_at")).collect()
        
        if len(snapshots) >= 2:
            # Compare latest two snapshots
            latest_snapshot = snapshots[0].snapshot_id
            previous_snapshot = snapshots[1].snapshot_id
            
            print(f"Comparing snapshots:")
            print(f"  Latest: {latest_snapshot}")
            print(f"  Previous: {previous_snapshot}")
            
            # Read data from both snapshots
            latest_df = spark.read.format("iceberg") \
                .option("snapshot-id", latest_snapshot) \
                .load(f"glue_catalog.{database_name}.{table_name}")
            
            previous_df = spark.read.format("iceberg") \
                .option("snapshot-id", previous_snapshot) \
                .load(f"glue_catalog.{database_name}.{table_name}")
            
            # Compare counts
            print(f"Latest snapshot count: {latest_df.count()}")
            print(f"Previous snapshot count: {previous_df.count()}")
            
            # Find differences (example for transactions table)
            if "transaction_id" in latest_df.columns:
                latest_ids = latest_df.select("transaction_id").distinct()
                previous_ids = previous_df.select("transaction_id").distinct()
                
                new_transactions = latest_ids.subtract(previous_ids)
                print(f"New transactions: {new_transactions.count()}")
        
        return snapshots_df
        
    finally:
        spark.stop()

if __name__ == "__main__":
    # Run advanced examples
    time_travel_example()
    metadata_analysis_example()
    snapshot_comparison_example()