import argparse
import json
import logging
import os
import sys

from pyspark.sql import SparkSession

from glue_functions import get_table, build_schema_for_table, query_table

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

AWS_REGION = 'us-east-1'


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
    glue_table = get_table(db_name, table_name, aws_region=AWS_REGION)

    table_schema = build_schema_for_table(glue_table)

    table_location = glue_table['Table']['StorageDescriptor']['Location']
    query_table(spark_session=spark, s3_location=table_location, schema=table_schema)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--event",
                        help="event data from lambda")
    args = parser.parse_args()
    event_data = json.loads(args.event)
    params = json.loads(event_data["body"])
    main(db_name=params["DATABASE_NAME"], table_name=params["TABLE_NAME"])
