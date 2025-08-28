import logging
import sys
import boto3
from typing import Dict, Optional, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_iceberg_table_metadata(db_name: str, table_name: str, aws_region: str) -> Optional[Dict[str, Any]]:
    """
    Fetches Iceberg table metadata from AWS Glue Catalog.
    
    Parameters:
    - db_name (str): The name of the database in Glue Catalog.
    - table_name (str): The name of the Iceberg table in Glue Database.
    - aws_region (str): AWS region for Glue client.
    
    Returns:
    - dict: The response from the Glue `get_table` API call, or None if error.
    """
    try:
        glue = boto3.client('glue', region_name=aws_region)
        response = glue.get_table(DatabaseName=db_name, Name=table_name)
        
        # Validate that this is an Iceberg table
        table_params = response.get('Table', {}).get('Parameters', {})
        table_type = table_params.get('table_type', '').upper()
        
        if table_type != 'ICEBERG':
            logger.warning(f"Table {table_name} is not an Iceberg table (type: {table_type})")
        
        return response
    except Exception as e:
        logger.error(f"Error fetching Iceberg table {table_name} from database {db_name}: {e}")
        return None


def get_iceberg_table_location(glue_table: Dict[str, Any]) -> Optional[str]:
    """
    Extracts the S3 location of an Iceberg table from Glue metadata.
    
    Parameters:
    - glue_table (dict): The table metadata from AWS Glue.
    
    Returns:
    - str: The S3 location of the Iceberg table, or None if not found.
    """
    try:
        if not glue_table or 'Table' not in glue_table:
            return None
            
        # For Iceberg tables, location is in StorageDescriptor
        storage_descriptor = glue_table['Table'].get('StorageDescriptor', {})
        location = storage_descriptor.get('Location', '')
        
        if location:
            # Convert s3:// to s3a:// for Spark compatibility
            if location.startswith("s3://"):
                location = location.replace("s3://", "s3a://")
            return location
        
        return None
    except Exception as e:
        logger.error(f"Error extracting table location: {e}")
        return None


def get_iceberg_table_properties(glue_table: Dict[str, Any]) -> Dict[str, str]:
    """
    Extracts Iceberg-specific table properties from Glue metadata.
    
    Parameters:
    - glue_table (dict): The table metadata from AWS Glue.
    
    Returns:
    - dict: Dictionary of Iceberg table properties.
    """
    try:
        if not glue_table or 'Table' not in glue_table:
            return {}
            
        table_params = glue_table['Table'].get('Parameters', {})
        
        # Extract Iceberg-specific properties
        iceberg_props = {}
        for key, value in table_params.items():
            if key.startswith('iceberg.') or key in ['table_type', 'metadata_location']:
                iceberg_props[key] = value
        
        return iceberg_props
    except Exception as e:
        logger.error(f"Error extracting Iceberg table properties: {e}")
        return {}


def read_iceberg_table_with_spark(spark: SparkSession, db_name: str, table_name: str, 
                                 catalog_name: str = "glue_catalog"):
    """
    Reads an Iceberg table using Spark with Glue Catalog integration.
    
    Parameters:
    - spark (SparkSession): The active SparkSession configured for Iceberg.
    - db_name (str): The database name in Glue Catalog.
    - table_name (str): The table name in Glue Catalog.
    - catalog_name (str): The catalog name configured in Spark (default: "glue_catalog").
    
    Returns:
    - DataFrame: Spark DataFrame containing the Iceberg table data.
    """
    try:
        table_identifier = f"{catalog_name}.{db_name}.{table_name}"
        logger.info(f"Reading Iceberg table: {table_identifier}")
        
        df = spark.read.format("iceberg").load(table_identifier)
        
        logger.info(f"Successfully loaded Iceberg table with {df.count()} rows")
        logger.info("Table schema:")
        df.printSchema()
        
        return df
    except Exception as e:
        logger.error(f"Error reading Iceberg table {table_identifier}: {e}")
        raise


def read_iceberg_table_by_location(spark: SparkSession, table_location: str):
    """
    Reads an Iceberg table directly from its S3 location.
    
    Parameters:
    - spark (SparkSession): The active SparkSession configured for Iceberg.
    - table_location (str): The S3 location of the Iceberg table.
    
    Returns:
    - DataFrame: Spark DataFrame containing the Iceberg table data.
    """
    try:
        # Ensure s3a:// protocol
        if table_location.startswith("s3://"):
            table_location = table_location.replace("s3://", "s3a://")
        
        logger.info(f"Reading Iceberg table from location: {table_location}")
        
        df = spark.read.format("iceberg").load(table_location)
        
        logger.info(f"Successfully loaded Iceberg table with {df.count()} rows")
        logger.info("Table schema:")
        df.printSchema()
        
        return df
    except Exception as e:
        logger.error(f"Error reading Iceberg table from location {table_location}: {e}")
        raise


def query_iceberg_table_history(spark: SparkSession, db_name: str, table_name: str, 
                               catalog_name: str = "glue_catalog"):
    """
    Queries the history of an Iceberg table to see snapshots and changes.
    
    Parameters:
    - spark (SparkSession): The active SparkSession configured for Iceberg.
    - db_name (str): The database name in Glue Catalog.
    - table_name (str): The table name in Glue Catalog.
    - catalog_name (str): The catalog name configured in Spark.
    
    Returns:
    - DataFrame: DataFrame containing the table history.
    """
    try:
        table_identifier = f"{catalog_name}.{db_name}.{table_name}"
        logger.info(f"Querying history for Iceberg table: {table_identifier}")
        
        history_df = spark.read.format("iceberg").load(f"{table_identifier}.history")
        
        logger.info("Table history:")
        history_df.show(truncate=False)
        
        return history_df
    except Exception as e:
        logger.error(f"Error querying table history for {table_identifier}: {e}")
        raise


def query_iceberg_table_snapshots(spark: SparkSession, db_name: str, table_name: str, 
                                 catalog_name: str = "glue_catalog"):
    """
    Queries the snapshots of an Iceberg table.
    
    Parameters:
    - spark (SparkSession): The active SparkSession configured for Iceberg.
    - db_name (str): The database name in Glue Catalog.
    - table_name (str): The table name in Glue Catalog.
    - catalog_name (str): The catalog name configured in Spark.
    
    Returns:
    - DataFrame: DataFrame containing the table snapshots.
    """
    try:
        table_identifier = f"{catalog_name}.{db_name}.{table_name}"
        logger.info(f"Querying snapshots for Iceberg table: {table_identifier}")
        
        snapshots_df = spark.read.format("iceberg").load(f"{table_identifier}.snapshots")
        
        logger.info("Table snapshots:")
        snapshots_df.show(truncate=False)
        
        return snapshots_df
    except Exception as e:
        logger.error(f"Error querying table snapshots for {table_identifier}: {e}")
        raise


def read_iceberg_table_at_timestamp(spark: SparkSession, db_name: str, table_name: str, 
                                   timestamp: str, catalog_name: str = "glue_catalog"):
    """
    Reads an Iceberg table as it existed at a specific timestamp (time travel).
    
    Parameters:
    - spark (SparkSession): The active SparkSession configured for Iceberg.
    - db_name (str): The database name in Glue Catalog.
    - table_name (str): The table name in Glue Catalog.
    - timestamp (str): Timestamp in format 'YYYY-MM-DD HH:MM:SS.SSS'
    - catalog_name (str): The catalog name configured in Spark.
    
    Returns:
    - DataFrame: Spark DataFrame containing the table data at the specified timestamp.
    """
    try:
        table_identifier = f"{catalog_name}.{db_name}.{table_name}"
        logger.info(f"Reading Iceberg table {table_identifier} at timestamp: {timestamp}")
        
        df = spark.read.format("iceberg") \
            .option("as-of-timestamp", timestamp) \
            .load(table_identifier)
        
        logger.info(f"Successfully loaded table at timestamp with {df.count()} rows")
        
        return df
    except Exception as e:
        logger.error(f"Error reading table at timestamp {timestamp}: {e}")
        raise


def read_iceberg_table_at_snapshot(spark: SparkSession, db_name: str, table_name: str, 
                                  snapshot_id: str, catalog_name: str = "glue_catalog"):
    """
    Reads an Iceberg table at a specific snapshot ID (time travel).
    
    Parameters:
    - spark (SparkSession): The active SparkSession configured for Iceberg.
    - db_name (str): The database name in Glue Catalog.
    - table_name (str): The table name in Glue Catalog.
    - snapshot_id (str): The snapshot ID to read from.
    - catalog_name (str): The catalog name configured in Spark.
    
    Returns:
    - DataFrame: Spark DataFrame containing the table data at the specified snapshot.
    """
    try:
        table_identifier = f"{catalog_name}.{db_name}.{table_name}"
        logger.info(f"Reading Iceberg table {table_identifier} at snapshot: {snapshot_id}")
        
        df = spark.read.format("iceberg") \
            .option("snapshot-id", snapshot_id) \
            .load(table_identifier)
        
        logger.info(f"Successfully loaded table at snapshot with {df.count()} rows")
        
        return df
    except Exception as e:
        logger.error(f"Error reading table at snapshot {snapshot_id}: {e}")
        raise