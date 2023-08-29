import logging
import sys

import boto3

from pyspark.sql.types import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_table(db_name, table_name, aws_region):
    """
    Fetches table metadata from AWS Glue Catalog.

    Parameters:
    - db_name (str): The name of the database in Glue Catalog.
    - table_name (str): The name of the table in Glue Database.

    Returns:
    - dict: The response from the Glue `get_table` API call.
    """
    try:
        glue = boto3.client('glue', region_name=aws_region)
        response = glue.get_table(DatabaseName=db_name, Name=table_name)
        return response
    except Exception as e:
        logger.error(f"Error fetching table {table_name} from database {db_name}: {e}")
        return None


def build_schema_for_table(glue_table):
    """
    Converts AWS Glue table schema to PySpark schema.

    Parameters:
    - glue_table (dict): The table metadata from AWS Glue.

    Returns:
    - StructType: The corresponding PySpark schema.
    """
    try:
        # Check if glue_table is valid and contains necessary keys
        if not glue_table \
                or 'Table' not in glue_table \
                or 'StorageDescriptor' not in glue_table['Table'] \
                or 'Columns' not in glue_table['Table']['StorageDescriptor']:
            return StructType()  # Return an empty schema

        # Extract columns and data types from the response
        columns = glue_table['Table']['StorageDescriptor']['Columns']

        # Mapping dictionary for Glue to PySpark data types
        dtype_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'bigint': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType(),
            'binary': BinaryType(),
            'decimal': DecimalType(),
            'array': ArrayType(StringType()),
            'map': MapType(StringType(), StringType()),
            'struct': StructType()
        }

        # Convert Glue schema to PySpark schema
        schema = [
            StructField(
                column['Name'],
                dtype_mapping.get(column.get('Type', 'string'), StringType())
            ) for column in columns
        ]
        return StructType(schema)
    except Exception as e:
        logger.error(f"Error building schema: {e}")
        return StructType()


def query_table(spark_session, s3_location, schema):
    """
    Queries a table stored in S3 using PySpark with the provided schema.

    Parameters:
    - spark_session (SparkSession): The active SparkSession.
    - s3_location (str): The S3 path to the table data.
    - schema (StructType): The PySpark schema for the table.

    Returns:
    - None: Prints the schema.
    """
    s3_location = _convert_s3_uri_to_s3a(s3_location)

    try:
        df = spark_session.read.schema(schema).format("delta").parquet(s3_location)
        df.printSchema()
    except Exception as e:
        logger.error(f"Error querying table from {s3_location}: {e}")


def _convert_s3_uri_to_s3a(s3_location):
    """
    Spark needs S3 location specified with the "s3a" protocol.
    This replaces "s3://" with "s3a://" in s3_location.

    Parameters:
    - s3_location (str): An S3 path.

    Returns:
    - str
    """
    if s3_location.startswith("s3://"):
        s3_location = s3_location.replace("s3://", "s3a://")

    return s3_location
