from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
import sys
import os

"""
 Function that gets triggered when AWS Lambda is running.
 We are using the example from Redshift documentation
 https://docs.aws.amazon.com/redshift/latest/dg/spatial-tutorial.html#spatial-tutorial-test-data

  Add the below parameters in the labmda function
  SCRIPT_BUCKET       BUCKER WHER YOU SAVE THIS SCRIPT
  SPARK_SCRIPT        THE SCRIPT NAME AND PATH
  input_path          s3a://redshift-downloads/spatial-data/accommodations.csv
  output_path         s3a://YOUR_BUCKET/YOUR_PATH

  Create the below table in Athena

  CREATE EXTERNAL TABLE accommodations_delta
  LOCATION 's3://YOUR_BUCKET/YOUR_PATH' 
  TBLPROPERTIES (
      'table_type'='DELTA'
  );

"""

def spark_script():
    input_path = os.environ['input_path']
    output_path = os.environ['output_path']

    aws_region = os.environ['AWS_REGION']
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']

    print(" ******* Input path ", input_path)
    print(" ******* Output path ", output_path)

    spark = SparkSession.builder \
    .appName("Spark-on-AWS-Lambda") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12-2.2.0.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.session.token",session_token) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .enableHiveSupport().getOrCreate()

    print("Started Reading the CSV file from S3 location ",input_path)

    #Reading the csv file form input_path
    df=spark.read.option('header','true').option("delimiter", ";").csv(input_path)
    df = df.withColumn("last_upd_timestamp", current_timestamp())
    df.printSchema()

    print("Started Writing the dataframe file to Target delta table ", output_path)
    df.write.format("delta").mode("append").save(output_path)

if __name__ == '__main__':
    #Calling the Spark script method
    spark_script()
