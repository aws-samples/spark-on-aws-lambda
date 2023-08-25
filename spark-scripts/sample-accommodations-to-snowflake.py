import logging
import argparse
import json
import os, os.path
import sys

from pyspark.sql import SparkSession

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
  SCRIPT_BUCKET         BUCKET WHERE YOU SAVE THIS SCRIPT
  SPARK_SCRIPT          THE SCRIPT NAME AND PATH
  INPUT_PATH            s3a://redshift-downloads/spatial-data/accommodations.csv
  SNOWFLAKE_URL         Snowflake account url (xxxxxxx-xxxxxxxx.snowflakecomputing.com)
  SNOWFLAKE_ACCOUNT     Snowflake account (XXXXXXX-XXXXXXXX)
  SNOWFLAKE_USER        Snowflake username
  SNOWFLAKE_PASSWORD    Snowflake password
  SNOWFLAKE_ROLE        Snowflake role to use
  SNOWFLAKE_DB          Snowflake DB to save data to
  SNOWFLAKE_SCHEMA      Snowflake schema to save data to 
  SNOWFLAKE_TABLE       Snowflake destination table

"""


def spark_script(input_path):
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']

    SNOWFLAKE_DB = os.environ['SNOWFLAKE_DB']
    SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
    SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
    SNOWFLAKE_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']
    SNOWFLAKE_URL = os.environ['SNOWFLAKE_URL']
    SNOWFLAKE_SCHEMA = os.environ['SNOWFLAKE_SCHEMA']
    SNOWFLAKE_ROLE = os.environ['SNOWFLAKE_ROLE']
    SNOWFLAKE_TABLE = os.environ['SNOWFLAKE_TABLE']

    logger.info(f" ******* Input path {input_path}")

    spark = SparkSession.builder \
        .appName("Spark-on-AWS-Lambda") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.jars.packages", "spark-snowflake_2.12-2.12.0-spark_3.3.jar,snowflake-jdbc-3.13.33.jar") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token", session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .getOrCreate()

    logger.info(f"Started Reading the CSV file from S3 location {input_path}")

    # Reading the csv file form input_path
    df = spark.read.option('header', 'true').option("delimiter", ";").csv(input_path)
    df.printSchema()

    # Save to snowflake
    logger.info(f"Started Writing the dataframe to Snowflake")

    sfOptions = {
        "sfURL": SNOWFLAKE_URL,
        "sfAccount": SNOWFLAKE_ACCOUNT,
        "sfUser": SNOWFLAKE_USER,
        "sfPassword": SNOWFLAKE_PASSWORD,
        "sfDatabase": SNOWFLAKE_DB,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfRole": SNOWFLAKE_ROLE,
        "dbtable": SNOWFLAKE_TABLE
    }

    df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .options(header=True) \
        .mode("overwrite") \
        .save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--event",
                        help="event data from lambda")
    args = parser.parse_args()
    event_data = json.loads(args.event)
    paths = json.loads(event_data["body"])

    spark_script(paths["INPUT_PATH"])
