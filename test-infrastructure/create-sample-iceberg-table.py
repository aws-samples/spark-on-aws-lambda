#!/usr/bin/env python3
"""
Script to create a sample Iceberg table with test data for Lambda testing.
This script should be run locally or on an EC2 instance with Spark and Iceberg configured.
"""

import os
import sys
from datetime import datetime, date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_spark_session(bucket_name, aws_region='us-east-1'):
    """Create Spark session configured for Iceberg with Glue Catalog"""
    
    spark = SparkSession.builder \
        .appName("CreateSampleIcebergTable") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3a://{bucket_name}/iceberg-warehouse/") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.catalog.glue_catalog.glue.region", aws_region) \
        .getOrCreate()
    
    return spark

def create_sample_data(spark):
    """Create sample customer data"""
    
    # Define schema
    schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("registration_date", DateType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("total_spent", DecimalType(10, 2), False),
        StructField("last_updated", TimestampType(), False)
    ])
    
    # Sample data
    sample_data = [
        (1, "John Doe", "john.doe@email.com", date(2023, 1, 15), 5, Decimal("299.99"), datetime(2024, 1, 15, 10, 30, 0)),
        (2, "Jane Smith", "jane.smith@email.com", date(2023, 2, 20), 8, Decimal("599.50"), datetime(2024, 1, 16, 14, 45, 0)),
        (3, "Bob Johnson", "bob.johnson@email.com", date(2023, 3, 10), 3, Decimal("149.75"), datetime(2024, 1, 17, 9, 15, 0)),
        (4, "Alice Brown", "alice.brown@email.com", date(2023, 4, 5), 12, Decimal("899.25"), datetime(2024, 1, 18, 16, 20, 0)),
        (5, "Charlie Wilson", "charlie.wilson@email.com", date(2023, 5, 12), 7, Decimal("449.80"), datetime(2024, 1, 19, 11, 10, 0)),
        (6, "Diana Davis", "diana.davis@email.com", date(2023, 6, 8), 15, Decimal("1299.99"), datetime(2024, 1, 20, 13, 25, 0)),
        (7, "Frank Miller", "frank.miller@email.com", date(2023, 7, 22), 4, Decimal("199.95"), datetime(2024, 1, 21, 8, 40, 0)),
        (8, "Grace Lee", "grace.lee@email.com", date(2023, 8, 18), 9, Decimal("679.30"), datetime(2024, 1, 22, 15, 55, 0)),
        (9, "Henry Taylor", "henry.taylor@email.com", date(2023, 9, 3), 6, Decimal("359.60"), datetime(2024, 1, 23, 12, 5, 0)),
        (10, "Ivy Anderson", "ivy.anderson@email.com", date(2023, 10, 14), 11, Decimal("799.85"), datetime(2024, 1, 24, 17, 30, 0))
    ]
    
    df = spark.createDataFrame(sample_data, schema)
    return df

def create_iceberg_table(spark, database_name, table_name, df):
    """Create Iceberg table in Glue Catalog"""
    
    table_identifier = f"glue_catalog.{database_name}.{table_name}"
    
    print(f"Creating Iceberg table: {table_identifier}")
    
    # Write DataFrame as Iceberg table
    df.writeTo(table_identifier) \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .create()
    
    print(f"Successfully created Iceberg table: {table_identifier}")
    
    # Verify the table
    verify_df = spark.read.format("iceberg").load(table_identifier)
    print(f"Table verification - Row count: {verify_df.count()}")
    verify_df.show()
    
    return table_identifier

def add_more_data(spark, table_identifier):
    """Add more data to demonstrate table evolution"""
    
    print(f"Adding more data to: {table_identifier}")
    
    # Additional data
    additional_schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("registration_date", DateType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("total_spent", DecimalType(10, 2), False),
        StructField("last_updated", TimestampType(), False)
    ])
    
    additional_data = [
        (11, "Kevin White", "kevin.white@email.com", date(2023, 11, 5), 2, Decimal("99.99"), datetime(2024, 1, 25, 10, 0, 0)),
        (12, "Laura Green", "laura.green@email.com", date(2023, 12, 1), 13, Decimal("999.75"), datetime(2024, 1, 26, 14, 30, 0)),
        (13, "Mike Black", "mike.black@email.com", date(2024, 1, 10), 1, Decimal("49.99"), datetime(2024, 1, 27, 9, 45, 0))
    ]
    
    additional_df = spark.createDataFrame(additional_data, additional_schema)
    
    # Append to existing table
    additional_df.writeTo(table_identifier).using("iceberg").append()
    
    print("Successfully added more data")
    
    # Verify updated table
    updated_df = spark.read.format("iceberg").load(table_identifier)
    print(f"Updated table - Row count: {updated_df.count()}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python create-sample-iceberg-table.py <bucket_name> <database_name>")
        print("Example: python create-sample-iceberg-table.py my-test-bucket-123456789 iceberg_test_db")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    database_name = sys.argv[2]
    table_name = "sample_customers"
    
    print(f"Creating sample Iceberg table in bucket: {bucket_name}")
    print(f"Database: {database_name}, Table: {table_name}")
    
    # Create Spark session
    spark = create_spark_session(bucket_name)
    
    try:
        # Create sample data
        df = create_sample_data(spark)
        
        # Create Iceberg table
        table_identifier = create_iceberg_table(spark, database_name, table_name, df)
        
        # Add more data to demonstrate table evolution
        add_more_data(spark, table_identifier)
        
        print("\n" + "="*50)
        print("SUCCESS: Sample Iceberg table created successfully!")
        print(f"Table: {table_identifier}")
        print(f"Location: s3://{bucket_name}/iceberg-warehouse/{database_name}/{table_name}/")
        print("="*50)
        
    except Exception as e:
        print(f"ERROR: Failed to create sample table: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()