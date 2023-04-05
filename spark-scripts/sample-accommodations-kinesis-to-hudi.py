from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import udf

import argparse
import base64
import json
import os
import sys
"""
 Function that gets triggered when AWS Lambda is running.
 We are using the example from Redshift documentation
 https://docs.aws.amazon.com/redshift/latest/dg/spatial-tutorial.html#spatial-tutorial-test-data

  1) Add the below parameters in the labmda function
  SCRIPT_BUCKET       BUCKER WHER YOU SAVE THIS SCRIPT
  SPARK_SCRIPT        THE SCRIPT NAME AND PATH
  output_path         s3a://YOUR_BUCKET/YOUR_PATH

  2) Create the below table in Athena

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

  3) Send data to your kinesis data stream, here we are using data from the accommodations dataset
     and the Hudi configuration is defined with recordkeyas id
     'hoodie.datasource.write.recordkey.field': 'id',

    Create a Kinisis Data Stream with only one shard, the function doesn't support yet concurrent writes

    import json
    import boto3
    kinesis = boto3.client('kinesis')

    record = {
    "id": "12312412",
    "shape": "0101000020E610000067728BA5A5D62A40EF255695FE3E4A40",
    "name": "großzuegige Altbauwhg. in Kreuzberg",
    "host_name": "Sabine",
    "neighbourhood_group": "Friedrichshain-Kreuzberg",
    "neighbourhood": "Tempelhofer Vorstadt",
    "room_type": "Entire home/apt",
    "price": "125",
    "minimum_nights": "5",
    "number_of_reviews": "3",
    "last_review": "2016-06-13",
    "reviews_per_month": "0.10",
    "calculated_host_listings_count": "1",
    "availability_365": "34",
    "last_upd_timestamp": "2023-04-03T23:57:11.802Z"
    }

    response = kinesis.put_record(
        StreamName="YOUR_STREAM",
        Data=json.dumps(record),
        PartitionKey="1")  


"""

def spark_script(json_array):
    output_path = os.environ['output_path']

    aws_region = os.environ['AWS_REGION']
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']

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

    # create dataframe form the lambda payload
    df = spark.createDataFrame(data=json_array)
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


#Decode base64 function
def decode_base64(encoded_str):
    return base64.b64decode(encoded_str).decode('utf-8')


if __name__ == '__main__':
    #Calling the Spark script method
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", help="events from lambda")
    args = parser.parse_args()

    # convert the events array into object and send to spark
    decode_base64_udf = udf(decode_base64, StringType())
    json_obj = json.loads(args.event)
    json_array = []
    for record in json_obj["Records"]:
        json_array.append(json.loads(base64.b64decode(record["kinesis"]["data"]).decode('utf-8')))

    # Calling the Spark script method
    spark_script(json_array)
