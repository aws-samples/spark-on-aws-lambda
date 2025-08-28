#!/usr/bin/env python3
"""
Lambda Handler Templates for different Iceberg use cases
"""

import json
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sys.path.append('/home/glue_functions')
from iceberg_glue_functions import (
    read_iceberg_table_with_spark,
    read_iceberg_table_at_timestamp,
    query_iceberg_table_history
)

def create_spark_session():
    """Standard Spark session for Lambda"""
    
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']
    
    return SparkSession.builder \
        .appName("Lambda-Iceberg-Handler") \
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

# Template 1: Simple Data Reader
def simple_reader_handler(event, context):
    """
    Template for simple Iceberg table reading
    
    Event format:
    {
        "database": "your_database",
        "table": "your_table",
        "limit": 100
    }
    """
    
    database = event.get('database')
    table = event.get('table')
    limit = event.get('limit', 100)
    
    if not database or not table:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'database and table are required'})
        }
    
    spark = create_spark_session()
    
    try:
        # Read table
        df = spark.read.format("iceberg").load(f"glue_catalog.{database}.{table}")
        
        # Get sample data
        sample_data = df.limit(limit).collect()
        
        # Convert to JSON-serializable format
        result = []
        for row in sample_data:
            result.append(row.asDict())
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'database': database,
                'table': table,
                'row_count': df.count(),
                'sample_data': result
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        spark.stop()

# Template 2: Filtered Analytics
def analytics_handler(event, context):
    """
    Template for analytical queries on Iceberg tables
    
    Event format:
    {
        "database": "analytics",
        "table": "sales_data",
        "filters": ["date >= '2024-01-01'", "region = 'US'"],
        "aggregations": {
            "group_by": ["product_category"],
            "metrics": ["sum(sales_amount) as total_sales", "count(*) as transaction_count"]
        }
    }
    """
    
    database = event.get('database')
    table = event.get('table')
    filters = event.get('filters', [])
    aggregations = event.get('aggregations', {})
    
    spark = create_spark_session()
    
    try:
        # Read table
        df = spark.read.format("iceberg").load(f"glue_catalog.{database}.{table}")
        
        # Apply filters
        for filter_condition in filters:
            df = df.filter(filter_condition)
        
        # Apply aggregations if specified
        if aggregations:
            group_by = aggregations.get('group_by', [])
            metrics = aggregations.get('metrics', [])
            
            if group_by and metrics:
                df = df.groupBy(*group_by).agg(*[expr(metric) for metric in metrics])
        
        # Collect results
        results = df.collect()
        
        # Convert to JSON
        result_data = []
        for row in results:
            result_data.append(row.asDict())
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'database': database,
                'table': table,
                'filters_applied': filters,
                'result_count': len(result_data),
                'results': result_data
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        spark.stop()

# Template 3: Time Travel Query
def time_travel_handler(event, context):
    """
    Template for time travel queries
    
    Event format:
    {
        "database": "analytics",
        "table": "customer_data",
        "timestamp": "2024-01-15 10:00:00.000",
        "compare_with_current": true
    }
    """
    
    database = event.get('database')
    table = event.get('table')
    timestamp = event.get('timestamp')
    compare_with_current = event.get('compare_with_current', False)
    
    spark = create_spark_session()
    
    try:
        table_identifier = f"glue_catalog.{database}.{table}"
        
        # Read historical data
        historical_df = spark.read.format("iceberg") \
            .option("as-of-timestamp", timestamp) \
            .load(table_identifier)
        
        historical_count = historical_df.count()
        
        result = {
            'database': database,
            'table': table,
            'timestamp': timestamp,
            'historical_count': historical_count
        }
        
        # Compare with current if requested
        if compare_with_current:
            current_df = spark.read.format("iceberg").load(table_identifier)
            current_count = current_df.count()
            
            result.update({
                'current_count': current_count,
                'difference': current_count - historical_count
            })
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        spark.stop()

# Template 4: Data Quality Checker
def data_quality_handler(event, context):
    """
    Template for data quality checks on Iceberg tables
    
    Event format:
    {
        "database": "data_lake",
        "table": "customer_records",
        "checks": [
            {"type": "row_count", "min": 1000},
            {"type": "null_check", "columns": ["customer_id", "email"]},
            {"type": "duplicate_check", "columns": ["customer_id"]},
            {"type": "value_range", "column": "age", "min": 0, "max": 120}
        ]
    }
    """
    
    database = event.get('database')
    table = event.get('table')
    checks = event.get('checks', [])
    
    spark = create_spark_session()
    
    try:
        # Read table
        df = spark.read.format("iceberg").load(f"glue_catalog.{database}.{table}")
        
        quality_results = []
        
        for check in checks:
            check_result = {'type': check['type'], 'status': 'passed'}
            
            if check['type'] == 'row_count':
                count = df.count()
                min_count = check.get('min', 0)
                max_count = check.get('max', float('inf'))
                
                if not (min_count <= count <= max_count):
                    check_result['status'] = 'failed'
                    check_result['message'] = f"Row count {count} outside range [{min_count}, {max_count}]"
                else:
                    check_result['message'] = f"Row count: {count}"
            
            elif check['type'] == 'null_check':
                columns = check['columns']
                null_counts = {}
                
                for column in columns:
                    null_count = df.filter(col(column).isNull()).count()
                    null_counts[column] = null_count
                    
                    if null_count > 0:
                        check_result['status'] = 'failed'
                
                check_result['null_counts'] = null_counts
            
            elif check['type'] == 'duplicate_check':
                columns = check['columns']
                total_count = df.count()
                distinct_count = df.select(*columns).distinct().count()
                
                if total_count != distinct_count:
                    check_result['status'] = 'failed'
                    check_result['message'] = f"Found {total_count - distinct_count} duplicates"
                else:
                    check_result['message'] = "No duplicates found"
            
            elif check['type'] == 'value_range':
                column = check['column']
                min_val = check.get('min')
                max_val = check.get('max')
                
                out_of_range = df.filter(
                    (col(column) < min_val) | (col(column) > max_val)
                ).count()
                
                if out_of_range > 0:
                    check_result['status'] = 'failed'
                    check_result['message'] = f"{out_of_range} values outside range [{min_val}, {max_val}]"
                else:
                    check_result['message'] = f"All values within range [{min_val}, {max_val}]"
            
            quality_results.append(check_result)
        
        # Overall status
        overall_status = 'passed' if all(r['status'] == 'passed' for r in quality_results) else 'failed'
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'database': database,
                'table': table,
                'overall_status': overall_status,
                'checks': quality_results
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        spark.stop()

# Template 5: Event-Driven Processing
def event_driven_handler(event, context):
    """
    Template for event-driven processing (e.g., S3 trigger)
    
    Event format (S3 event):
    {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "my-bucket"},
                    "object": {"key": "data/new-file.parquet"}
                }
            }
        ],
        "processing_config": {
            "target_database": "processed_data",
            "target_table": "aggregated_metrics"
        }
    }
    """
    
    spark = create_spark_session()
    
    try:
        # Process S3 events
        if 'Records' in event:
            for record in event['Records']:
                if 's3' in record:
                    bucket = record['s3']['bucket']['name']
                    key = record['s3']['object']['key']
                    
                    print(f"Processing file: s3://{bucket}/{key}")
                    
                    # Read the new file
                    file_df = spark.read.parquet(f"s3a://{bucket}/{key}")
                    
                    # Process the data (example: simple aggregation)
                    processed_df = file_df.groupBy("category") \
                        .agg(
                            count("*").alias("record_count"),
                            sum("amount").alias("total_amount")
                        ) \
                        .withColumn("processed_at", current_timestamp())
                    
                    # Write to target Iceberg table
                    config = event.get('processing_config', {})
                    target_db = config.get('target_database', 'processed_data')
                    target_table = config.get('target_table', 'processed_metrics')
                    
                    processed_df.write \
                        .format("iceberg") \
                        .mode("append") \
                        .save(f"glue_catalog.{target_db}.{target_table}")
                    
                    print(f"Processed {processed_df.count()} records")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Event processing completed',
                'processed_files': len(event.get('Records', []))
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        spark.stop()

# Main handler router
def lambda_handler(event, context):
    """
    Main handler that routes to different templates based on event type
    """
    
    handler_type = event.get('handler_type', 'simple_reader')
    
    handlers = {
        'simple_reader': simple_reader_handler,
        'analytics': analytics_handler,
        'time_travel': time_travel_handler,
        'data_quality': data_quality_handler,
        'event_driven': event_driven_handler
    }
    
    if handler_type in handlers:
        return handlers[handler_type](event, context)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': f'Unknown handler type: {handler_type}',
                'available_types': list(handlers.keys())
            })
        }