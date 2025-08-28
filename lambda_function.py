import json
import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Simple Lambda function to test Glue Catalog access for Iceberg tables
    """
    
    logger.info("🚀 Starting Iceberg Glue Catalog test")
    
    # Get parameters from event or environment
    database_name = event.get('DATABASE_NAME', 'iceberg_test_db')
    table_name = event.get('TABLE_NAME', 'sample_customers')
    
    logger.info(f"📋 Testing table: {database_name}.{table_name}")
    
    try:
        # Initialize Glue client
        glue_client = boto3.client('glue')
        
        # Test 1: Get database
        logger.info("1️⃣ Testing database access...")
        db_response = glue_client.get_database(Name=database_name)
        logger.info(f"✅ Database found: {db_response['Database']['Name']}")
        
        # Test 2: Get table
        logger.info("2️⃣ Testing table access...")
        table_response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table = table_response['Table']
        
        logger.info(f"✅ Table found: {table['Name']}")
        
        # Test 3: Check if it's an Iceberg table
        logger.info("3️⃣ Validating Iceberg table...")
        table_type = table.get('Parameters', {}).get('table_type', '').upper()
        
        if table_type == 'ICEBERG':
            logger.info("✅ Confirmed: This is an Iceberg table")
        else:
            logger.warning(f"⚠️ Warning: Table type is '{table_type}', not 'ICEBERG'")
        
        # Test 4: Get table schema
        logger.info("4️⃣ Checking table schema...")
        storage_descriptor = table.get('StorageDescriptor', {})
        columns = storage_descriptor.get('Columns', [])
        location = storage_descriptor.get('Location', 'N/A')
        
        logger.info(f"📍 Location: {location}")
        logger.info(f"📊 Column count: {len(columns)}")
        
        # Test 5: Check S3 access
        logger.info("5️⃣ Testing S3 location access...")
        if location and location.startswith('s3://'):
            s3_client = boto3.client('s3')
            
            # Parse S3 location
            location_parts = location.replace('s3://', '').split('/', 1)
            bucket_name = location_parts[0]
            prefix = location_parts[1] if len(location_parts) > 1 else ''
            
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=10
                )
                
                object_count = response.get('KeyCount', 0)
                logger.info(f"📁 S3 bucket accessible: {bucket_name}")
                logger.info(f"📄 Objects found: {object_count}")
                
            except Exception as s3_error:
                logger.error(f"❌ S3 access failed: {s3_error}")
        
        # Prepare response
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Iceberg Glue Catalog test completed successfully',
                'database': database_name,
                'table': table_name,
                'table_type': table_type,
                'location': location,
                'column_count': len(columns),
                'columns': [{'name': col['Name'], 'type': col['Type']} for col in columns]
            })
        }
        
        logger.info("🎉 Test completed successfully!")
        return result
        
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        
        error_result = {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Iceberg Glue Catalog test failed'
            })
        }
        
        return error_result