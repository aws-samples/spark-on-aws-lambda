# Iceberg Integration Usage Guide

This guide shows you how to use the Iceberg integration code in your Spark on AWS Lambda applications.

## 🚀 Quick Start

### 1. Basic Setup

```python
# In your Lambda function
import sys
sys.path.append('/home/glue_functions')

from iceberg_glue_functions import read_iceberg_table_with_spark
from pyspark.sql import SparkSession

# Create Spark session (use the provided helper)
spark = create_iceberg_spark_session()

# Read Iceberg table
df = spark.read.format("iceberg").load("glue_catalog.your_database.your_table")
```

### 2. Environment Variables

Set these in your Lambda function:

```bash
SCRIPT_BUCKET=your-s3-bucket
SPARK_SCRIPT=your-script.py
DATABASE_NAME=your_database
TABLE_NAME=your_table
AWS_REGION=us-east-1
```

## 📖 Usage Examples

### Example 1: Simple Data Reading

```python
def lambda_handler(event, context):
    spark = create_iceberg_spark_session()
    
    try:
        # Read table
        df = spark.read.format("iceberg").load("glue_catalog.analytics.customer_data")
        
        # Basic operations
        print(f"Row count: {df.count()}")
        df.show(10)
        
        # Filter data
        recent_data = df.filter(col("created_date") >= "2024-01-01")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'total_rows': df.count(),
                'recent_rows': recent_data.count()
            })
        }
    finally:
        spark.stop()
```

### Example 2: Time Travel Queries

```python
def lambda_handler(event, context):
    spark = create_iceberg_spark_session()
    
    try:
        # Current data
        current_df = spark.read.format("iceberg").load("glue_catalog.sales.transactions")
        
        # Historical data (yesterday)
        historical_df = spark.read.format("iceberg") \
            .option("as-of-timestamp", "2024-01-20 00:00:00.000") \
            .load("glue_catalog.sales.transactions")
        
        # Compare
        current_count = current_df.count()
        historical_count = historical_df.count()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'current_transactions': current_count,
                'historical_transactions': historical_count,
                'new_transactions': current_count - historical_count
            })
        }
    finally:
        spark.stop()
```

### Example 3: Data Processing Pipeline

```python
def lambda_handler(event, context):
    spark = create_iceberg_spark_session()
    
    try:
        # Read source data
        raw_df = spark.read.format("iceberg").load("glue_catalog.raw.events")
        
        # Process data
        processed_df = raw_df \
            .filter(col("event_type") == "purchase") \
            .withColumn("processing_date", current_date()) \
            .groupBy("customer_id", "product_category") \
            .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("purchase_count")
            )
        
        # Write to target table
        processed_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .save("glue_catalog.analytics.customer_purchases")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed_customers': processed_df.count(),
                'message': 'Processing completed successfully'
            })
        }
    finally:
        spark.stop()
```

## 🔧 Configuration Options

### Spark Session Configuration

```python
spark = SparkSession.builder \
    .appName("Your-App-Name") \
    .master("local[*]") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()
```

### Lambda Function Settings

- **Memory**: 3008 MB (recommended for Spark workloads)
- **Timeout**: 15 minutes (maximum)
- **Runtime**: Use container image with Iceberg support
- **Environment Variables**: Set database and table names

## 📊 Available Functions

### Basic Operations

```python
# Read table
df = read_iceberg_table_with_spark(spark, "database", "table")

# Get table metadata
metadata = get_iceberg_table_metadata("database", "table", "us-east-1")

# Get table location
location = get_iceberg_table_location(metadata)
```

### Advanced Operations

```python
# Time travel
historical_df = read_iceberg_table_at_timestamp(spark, "db", "table", "2024-01-01 00:00:00")

# Snapshot queries
snapshot_df = read_iceberg_table_at_snapshot(spark, "db", "table", "snapshot_id")

# Table history
history_df = query_iceberg_table_history(spark, "db", "table")

# Table snapshots
snapshots_df = query_iceberg_table_snapshots(spark, "db", "table")
```

## 🎯 Event Formats

### Simple Read Event

```json
{
  "handler_type": "simple_reader",
  "database": "analytics",
  "table": "customer_data",
  "limit": 100
}
```

### Analytics Event

```json
{
  "handler_type": "analytics",
  "database": "sales",
  "table": "transactions",
  "filters": ["date >= '2024-01-01'", "amount > 100"],
  "aggregations": {
    "group_by": ["product_category"],
    "metrics": ["sum(amount) as total_sales", "count(*) as transaction_count"]
  }
}
```

### Time Travel Event

```json
{
  "handler_type": "time_travel",
  "database": "analytics",
  "table": "customer_data",
  "timestamp": "2024-01-15 10:00:00.000",
  "compare_with_current": true
}
```

## 🔍 Error Handling

```python
def lambda_handler(event, context):
    spark = None
    
    try:
        spark = create_iceberg_spark_session()
        
        # Your processing logic here
        df = spark.read.format("iceberg").load("glue_catalog.db.table")
        
        return {'statusCode': 200, 'body': 'Success'}
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        if spark:
            spark.stop()
```

## 📈 Performance Tips

1. **Use appropriate filters** to reduce data volume
2. **Set proper memory allocation** (3008 MB recommended)
3. **Enable adaptive query execution**
4. **Use columnar operations** when possible
5. **Consider partitioning** for large tables

## 🔗 Integration Patterns

### Event-Driven Processing

```python
# Triggered by S3 events
def process_new_data(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Process new file and update Iceberg table
        process_file(f"s3a://{bucket}/{key}")
```

### Scheduled Processing

```python
# Triggered by CloudWatch Events
def daily_aggregation(event, context):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Process yesterday's data
    df = spark.read.format("iceberg") \
        .load("glue_catalog.raw.events") \
        .filter(f"date = '{yesterday}'")
    
    # Aggregate and save
    aggregated = df.groupBy("category").agg(sum("amount"))
    aggregated.write.format("iceberg").mode("append").save("glue_catalog.analytics.daily_summary")
```

## 🛠️ Troubleshooting

### Common Issues

1. **"Table not found"**
   - Check database and table names
   - Verify Glue Catalog permissions

2. **"Access Denied"**
   - Check S3 permissions for table location
   - Verify IAM role has Glue access

3. **Memory errors**
   - Increase Lambda memory allocation
   - Add filters to reduce data volume

4. **Timeout errors**
   - Optimize queries with filters
   - Consider breaking into smaller chunks

### Debug Commands

```python
# Check table exists
metadata = get_iceberg_table_metadata("db", "table", "us-east-1")
print(f"Table type: {metadata['Table']['Parameters'].get('table_type')}")

# Check table location
location = get_iceberg_table_location(metadata)
print(f"Location: {location}")

# Test S3 access
s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket='bucket', Prefix='prefix', MaxKeys=1)
print(f"S3 accessible: {response.get('KeyCount', 0) >= 0}")
```

## 📚 Next Steps

1. **Start with simple examples** and gradually add complexity
2. **Test with small datasets** before scaling up
3. **Monitor CloudWatch logs** for debugging
4. **Set up proper error handling** and retry logic
5. **Consider cost optimization** with appropriate filtering