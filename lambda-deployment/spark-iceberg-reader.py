#!/usr/bin/env python3
"""
Production Lambda function for reading Iceberg tables from Glue Catalog
This is the actual handler that will run in the Lambda container
"""

import json
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Add glue functions to path
sys.path.append('/home/glue_functions')

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_iceberg_spark_session():
    """Create Spark session optimized for Lambda with Iceberg support"""
    
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    
    logger.info("Creating Spark session with Iceberg configuration...")
    
    spark = SparkSession.builder \
        .appName("Lambda-Iceberg-Reader") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.glue.region", aws_region) \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token", session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    return spark

def read_iceberg_table(spark, database_name, table_name, limit=None, filters=None):
    """Read Iceberg table with optional filters and limit"""
    
    table_identifier = f"glue_catalog.{database_name}.{table_name}"
    logger.info(f"Reading Iceberg table: {table_identifier}")
    
    try:
        # Read the table
        df = spark.read.format("iceberg").load(table_identifier)
        
        # Apply filters if provided
        if filters:
            for filter_condition in filters:
                df = df.filter(filter_condition)
                logger.info(f"Applied filter: {filter_condition}")
        
        # Apply limit if provided
        if limit:
            df = df.limit(limit)
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading table {table_identifier}: {e}")
        raise

def get_table_analytics(df):
    """Get basic analytics from the DataFrame"""
    
    logger.info("Computing table analytics...")
    
    try:
        # Basic stats
        total_count = df.count()
        
        # Get numeric columns for analytics
        numeric_columns = []
        for field in df.schema.fields:
            if field.dataType.typeName() in ['integer', 'long', 'double', 'decimal', 'float']:
                numeric_columns.append(field.name)
        
        analytics = {
            'total_rows': total_count,
            'columns': len(df.columns),
            'column_names': df.columns
        }
        
        # Add numeric analytics if available
        if numeric_columns:
            for col_name in numeric_columns:
                try:
                    stats = df.agg(
                        avg(col_name).alias('avg'),
                        min(col_name).alias('min'),
                        max(col_name).alias('max')
                    ).collect()[0]
                    
                    analytics[f'{col_name}_stats'] = {
                        'avg': float(stats['avg']) if stats['avg'] else None,
                        'min': float(stats['min']) if stats['min'] else None,
                        'max': float(stats['max']) if stats['max'] else None
                    }
                except Exception as e:
                    logger.warning(f"Could not compute stats for {col_name}: {e}")
        
        return analytics
        
    except Exception as e:
        logger.error(f"Error computing analytics: {e}")
        return {'total_rows': 0, 'error': str(e)}

def convert_rows_to_json(rows):
    """Convert Spark rows to JSON-serializable format"""
    
    results = []
    for row in rows:
        row_dict = {}
        for field in row.__fields__:
            value = getattr(row, field)
            
            # Handle different data types
            if value is None:
                row_dict[field] = None
            elif hasattr(value, 'isoformat'):  # datetime
                row_dict[field] = value.isoformat()
            elif str(type(value)) == "<class 'decimal.Decimal'>":
                row_dict[field] = float(value)
            elif str(type(value)) == "<class 'datetime.date'>":
                row_dict[field] = value.isoformat()
            else:
                row_dict[field] = value
        
        results.append(row_dict)
    
    return results

def lambda_handler(event, context):
    """
    Main Lambda handler for Iceberg table operations
    
    Event format:
    {
        "operation": "read_table",
        "database": "iceberg_test_db",
        "table": "sample_customers",
        "limit": 10,
        "filters": ["total_spent > 300"],
        "include_analytics": true
    }
    """
    
    logger.info("🚀 Starting Iceberg Lambda handler")
    logger.info(f"Event: {json.dumps(event)}")
    
    # Parse event parameters
    operation = event.get('operation', 'read_table')
    database_name = event.get('database', os.environ.get('DATABASE_NAME', 'iceberg_test_db'))
    table_name = event.get('table', os.environ.get('TABLE_NAME', 'sample_customers'))
    limit = event.get('limit', 10)
    filters = event.get('filters', [])
    include_analytics = event.get('include_analytics', True)
    
    logger.info(f"Operation: {operation}")
    logger.info(f"Target table: {database_name}.{table_name}")
    
    spark = None
    
    try:
        # Create Spark session
        spark = create_iceberg_spark_session()
        
        if operation == 'read_table':
            # Read table data
            df = read_iceberg_table(spark, database_name, table_name, limit, filters)
            
            # Get sample data
            sample_rows = df.collect()
            sample_data = convert_rows_to_json(sample_rows)
            
            # Prepare response
            response_body = {
                'message': 'Successfully read Iceberg table',
                'database': database_name,
                'table': table_name,
                'operation': operation,
                'filters_applied': filters,
                'sample_data': sample_data,
                'sample_count': len(sample_data),
                'timestamp': datetime.now().isoformat()
            }
            
            # Add analytics if requested
            if include_analytics:
                # Read full table for analytics (without limit)
                full_df = read_iceberg_table(spark, database_name, table_name, filters=filters)
                analytics = get_table_analytics(full_df)
                response_body['analytics'] = analytics
            
            logger.info(f"✅ Successfully processed {len(sample_data)} rows")
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps(response_body)
            }
            
        elif operation == 'table_info':
            # Get table information only
            df = read_iceberg_table(spark, database_name, table_name, limit=1)
            
            schema_info = []
            for field in df.schema.fields:
                schema_info.append({
                    'name': field.name,
                    'type': str(field.dataType),
                    'nullable': field.nullable
                })
            
            # Get full count
            full_df = read_iceberg_table(spark, database_name, table_name)
            total_count = full_df.count()
            
            response_body = {
                'message': 'Table information retrieved',
                'database': database_name,
                'table': table_name,
                'total_rows': total_count,
                'schema': schema_info,
                'timestamp': datetime.now().isoformat()
            }
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps(response_body)
            }
            
        else:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'error': f'Unknown operation: {operation}',
                    'supported_operations': ['read_table', 'table_info']
                })
            }
            
    except Exception as e:
        logger.error(f"❌ Lambda execution failed: {str(e)}")
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': str(e),
                'message': 'Iceberg table operation failed',
                'timestamp': datetime.now().isoformat()
            })
        }
        
    finally:
        if spark:
            logger.info("🔧 Stopping Spark session")
            spark.stop()

# For testing locally
if __name__ == "__main__":
    # Test event
    test_event = {
        "operation": "read_table",
        "database": "iceberg_test_db",
        "table": "sample_customers",
        "limit": 5,
        "include_analytics": True
    }
    
    # Mock context
    class MockContext:
        def __init__(self):
            self.function_name = "test-iceberg-reader"
            self.memory_limit_in_mb = 3008
            self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
    
    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2))