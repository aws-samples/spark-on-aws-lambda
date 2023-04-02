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

  CREATE EXTERNAL TABLE  accommodations_hudi (
          _hoodie_commit_time  string, 
          _hoodie_commit_seqno  string, 
          _hoodie_record_key  string, 
          _hoodie_partition_path  string, 
          _hoodie_file_name  string, 
          id  string,
          shape  string,
          name  string,
          host_name  string,
          neighbourhood_group  string,
          neighbourhood  string,
          room_type  string,
          price  string,
          minimum_nights  string,
          number_of_reviews  string,
          last_review  string,
          reviews_per_month  string,
          calculated_host_listings_count  string,
          availability_365  string
  )
  ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
  STORED AS INPUTFORMAT 
    'org.apache.hudi.hadoop.HoodieParquetInputFormat' 
  OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
    's3://YOUR_BUCKET/YOUR_PATH' 

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
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("hoodie.meta.sync.client.tool.class", "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool") \
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

    # HUDI configuration for the table write
    hudi_options = {
       'hoodie.table.name': 'customer_table',
       'hoodie.datasource.write.recordkey.field': 'id',
       'hoodie.datasource.write.precombine.field': 'last_upd_timestamp',
       'hoodie.insert.shuffle.parallelism': 2,
       "hoodie.datasource.hive_sync.enable": "false",
       "hoodie.datasource.hive_sync.database": "default",
       "hoodie.datasource.hive_sync.table": "customer_table",
       "hoodie.datasource.hive_sync.use_jdbc": "false",
       "hoodie.datasource.hive_sync.mode": "hms",
       "hoodie.write.markers.type":"direct", # It's not advisable to use this configuration. Working on workaround without using this config.
       "hoodie.embed.timeline.server":"false" # It's not advisable to use this configuration. Working on workaround without using this config.
    }

    print("Started Writing the dataframe file to Target hudi table ", output_path)
    df.write.format("hudi").options(**hudi_options).mode("append").save(output_path)


if __name__ == '__main__':
    #Calling the Spark script method
    spark_script()
