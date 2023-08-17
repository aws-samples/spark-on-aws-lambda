import argparse
import json
import logging
import os
import sys

import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

"""
 Function that gets triggered when AWS Lambda is running.
 We are using the example from Redshift documentation
 https://docs.aws.amazon.com/redshift/latest/dg/spatial-tutorial.html#spatial-tutorial-test-data

  Add the below parameters in the labmda function
  SCRIPT_BUCKET       BUCKER WHER YOU SAVE THIS SCRIPT
  SPARK_SCRIPT        THE SCRIPT NAME AND PATH
  INPUT_PATH          s3a://redshift-downloads/spatial-data/accommodations.csv
  OUTPUT_PATH         s3a://YOUR_BUCKET/YOUR_PATH
  DATABASE_NAME       AWS Glue Database name
  TABLE_NAME          AWS Glue Table name

  Create the below table in Athena

  CREATE EXTERNAL TABLE accommodations_delta
  LOCATION 's3://YOUR_BUCKET/YOUR_PATH' 
  TBLPROPERTIES (
      'table_type'='DELTA'
  );

"""


def get_table(db_name, table_name):
    glue = boto3.client('glue', region_name='us-east-1')

    response = glue.get_table(
        DatabaseName=db_name,
        Name=table_name
    )
    return response


def build_schema_for_table(glue_table):
    # Extract columns and data types from the response
    columns = glue_table['Table']['StorageDescriptor']['Columns']

    # Convert Glue schema to PySpark schema
    schema = StructType()
    for column in columns:
        dtype = column['Type']
        if dtype == 'string':
            spark_dtype = 'StringType()'
        elif dtype == 'int':
            spark_dtype = 'IntegerType()'
        elif dtype == "timestamp":
            spark_dtype = 'TimestampType()'
        # Add more data type mappings as needed
        else:
            spark_dtype = 'StringType()'  # default to StringType for simplicity
        schema.add(column['Name'], eval(spark_dtype))

    return schema


def query_table(spark_session, s3_location, schema):
    df = spark_session.read.schema(schema).format("delta").parquet(s3_location)
    df.printSchema()

    df.orderBy(col("price").desc()).show()


def main(db_name, table_name):
    logger.info("------> Building spark session...")
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']

    spark = SparkSession.builder.appName("GlueSchemaToSpark") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token", session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .getOrCreate()

    logger.info(f"------> Retrieving Glue table {table_name} from {db_name}")
    glue_table = get_table(db_name, table_name)

    table_location = glue_table['Table']['StorageDescriptor']['Location']
    table_location = table_location.replace("s3://", "s3a://")

    table_schema = build_schema_for_table(glue_table)

    query_table(spark_session=spark, s3_location=table_location, schema=table_schema)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--event",
                        help="event data from lambda")
    args = parser.parse_args()
    event_data = json.loads(args.event)
    params = json.loads(event_data["body"])
    main(db_name=params["DATABASE_NAME"], table_name=params["TABLE_NAME"])
