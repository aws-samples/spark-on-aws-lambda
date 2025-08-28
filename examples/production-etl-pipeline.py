#!/usr/bin/env python3
"""
Production ETL Pipeline: Complete example for processing Iceberg tables in Lambda
"""

import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sys.path.append('/home/glue_functions')
from iceberg_glue_functions import (
    read_iceberg_table_with_spark,
    get_iceberg_table_metadata
)

def create_production_spark_session(app_name="Production-ETL"):
    """Production-ready Spark session configuration"""
    
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
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

class IcebergETLPipeline:
    """Production ETL Pipeline for Iceberg tables"""
    
    def __init__(self, config):
        self.config = config
        self.spark = create_production_spark_session(config.get('app_name', 'ETL-Pipeline'))
        
    def read_source_table(self, database, table, filters=None):
        """Read source Iceberg table with optional filters"""
        
        print(f"📖 Reading source table: {database}.{table}")
        
        try:
            df = self.spark.read.format("iceberg").load(f"glue_catalog.{database}.{table}")
            
            # Apply filters if provided
            if filters:
                for filter_condition in filters:
                    df = df.filter(filter_condition)
                    print(f"   Applied filter: {filter_condition}")
            
            row_count = df.count()
            print(f"   Loaded {row_count} rows")
            
            return df
            
        except Exception as e:
            print(f"❌ Error reading table {database}.{table}: {e}")
            raise
    
    def transform_data(self, df, transformations):
        """Apply data transformations"""
        
        print("🔄 Applying transformations...")
        
        for i, transform in enumerate(transformations, 1):
            print(f"   Transformation {i}: {transform['description']}")
            
            if transform['type'] == 'add_column':
                df = df.withColumn(transform['column'], expr(transform['expression']))
                
            elif transform['type'] == 'filter':
                df = df.filter(transform['condition'])
                
            elif transform['type'] == 'aggregate':
                df = df.groupBy(*transform['group_by']) \
                       .agg(*[expr(agg) for agg in transform['aggregations']])
                
            elif transform['type'] == 'join':
                other_df = self.read_source_table(
                    transform['join_table']['database'],
                    transform['join_table']['table']
                )
                df = df.join(other_df, transform['join_condition'], transform['join_type'])
                
            elif transform['type'] == 'custom':
                # Custom transformation function
                df = transform['function'](df)
        
        final_count = df.count()
        print(f"   Final row count: {final_count}")
        
        return df
    
    def write_to_target(self, df, target_config):
        """Write processed data to target location"""
        
        print(f"💾 Writing to target: {target_config['location']}")
        
        writer = df.write.mode(target_config.get('mode', 'overwrite'))
        
        # Configure output format
        if target_config['format'] == 'iceberg':
            # Write to Iceberg table
            table_identifier = f"glue_catalog.{target_config['database']}.{target_config['table']}"
            
            writer = writer.format("iceberg")
            
            # Add Iceberg-specific options
            if 'iceberg_options' in target_config:
                for key, value in target_config['iceberg_options'].items():
                    writer = writer.option(key, value)
            
            writer.save(table_identifier)
            
        elif target_config['format'] == 'parquet':
            writer.format("parquet").save(target_config['location'])
            
        elif target_config['format'] == 'delta':
            writer.format("delta").save(target_config['location'])
        
        print(f"   ✅ Data written successfully")
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        
        print("🚀 Starting ETL Pipeline")
        print("=" * 50)
        
        try:
            # Step 1: Read source data
            source_df = self.read_source_table(
                self.config['source']['database'],
                self.config['source']['table'],
                self.config['source'].get('filters')
            )
            
            # Step 2: Apply transformations
            if 'transformations' in self.config:
                transformed_df = self.transform_data(source_df, self.config['transformations'])
            else:
                transformed_df = source_df
            
            # Step 3: Data quality checks
            if 'quality_checks' in self.config:
                self.run_quality_checks(transformed_df, self.config['quality_checks'])
            
            # Step 4: Write to target
            self.write_to_target(transformed_df, self.config['target'])
            
            print("🎉 ETL Pipeline completed successfully!")
            
            return {
                'status': 'success',
                'rows_processed': transformed_df.count(),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ ETL Pipeline failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        
        finally:
            self.spark.stop()
    
    def run_quality_checks(self, df, checks):
        """Run data quality checks"""
        
        print("🔍 Running data quality checks...")
        
        for check in checks:
            if check['type'] == 'row_count':
                count = df.count()
                min_rows = check.get('min_rows', 0)
                max_rows = check.get('max_rows', float('inf'))
                
                if not (min_rows <= count <= max_rows):
                    raise ValueError(f"Row count {count} outside expected range [{min_rows}, {max_rows}]")
                
                print(f"   ✅ Row count check passed: {count} rows")
            
            elif check['type'] == 'null_check':
                for column in check['columns']:
                    null_count = df.filter(col(column).isNull()).count()
                    if null_count > 0:
                        raise ValueError(f"Found {null_count} null values in column {column}")
                
                print(f"   ✅ Null check passed for columns: {check['columns']}")
            
            elif check['type'] == 'custom':
                # Custom quality check function
                check['function'](df)

# Example usage in Lambda handler
def lambda_handler(event, context):
    """Lambda handler for ETL pipeline"""
    
    # Example configuration
    etl_config = {
        'app_name': 'Customer-Analytics-ETL',
        'source': {
            'database': 'raw_data',
            'table': 'customer_events',
            'filters': [
                "event_date >= '2024-01-01'",
                "event_type IN ('purchase', 'signup')"
            ]
        },
        'transformations': [
            {
                'type': 'add_column',
                'description': 'Add processing timestamp',
                'column': 'processed_at',
                'expression': 'current_timestamp()'
            },
            {
                'type': 'aggregate',
                'description': 'Aggregate by customer and event type',
                'group_by': ['customer_id', 'event_type'],
                'aggregations': [
                    'count(*) as event_count',
                    'sum(amount) as total_amount',
                    'max(event_date) as last_event_date'
                ]
            }
        ],
        'quality_checks': [
            {
                'type': 'row_count',
                'min_rows': 1
            },
            {
                'type': 'null_check',
                'columns': ['customer_id', 'event_type']
            }
        ],
        'target': {
            'format': 'iceberg',
            'database': 'analytics',
            'table': 'customer_summary',
            'mode': 'overwrite',
            'iceberg_options': {
                'write.format.default': 'parquet',
                'write.parquet.compression-codec': 'snappy'
            }
        }
    }
    
    # Override config with event parameters if provided
    if 'config' in event:
        etl_config.update(event['config'])
    
    # Run pipeline
    pipeline = IcebergETLPipeline(etl_config)
    result = pipeline.run_pipeline()
    
    return {
        'statusCode': 200 if result['status'] == 'success' else 500,
        'body': json.dumps(result)
    }

if __name__ == "__main__":
    # Test the pipeline locally
    test_event = {}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))