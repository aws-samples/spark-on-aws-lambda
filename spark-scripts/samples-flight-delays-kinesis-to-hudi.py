from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import udf

import argparse
import sys
import os
import json
import base64

def spark_script(json_array):
 """
 Function that gets triggered when AWS Lambda
 is running.
 """
 
 print("start...................")
 

 # Please use script to update it, try to avoid env variable through docker
 target_path = os.environ['output_path']
 s3_bucket  = os.environ['s3_bucket']

 aws_region = os.environ['AWS_REGION'] 
 aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] 
 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] 
 session_token = os.environ['AWS_SESSION_TOKEN']

 target_path ="s3a://"+s3_bucket+"/"+target_path
 
 print(" ******* Target path ",target_path)

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
 
 # create dataframe
 df = spark.createDataFrame(data=json_array)
 
 # adding column last_upd_timestamp
 df = df.withColumn("last_upd_timestamp", current_timestamp())
 
 # HUDI configuration for the table write
 hudi_options = {
    'hoodie.table.name': 'customer_table',
    'hoodie.datasource.write.recordkey.field': 'airlineid',
    'hoodie.datasource.write.precombine.field': 'last_upd_timestamp',
    'hoodie.insert.shuffle.parallelism': 2,
    "hoodie.datasource.hive_sync.enable": "false",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "customer_table",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.write.markers.type":"direct",
    "hoodie.embed.timeline.server":"false"
 }

 print("Started Writing the dataframe file to  Target hudi table ", target_path)
 df.write.format("hudi").options(**hudi_options).mode("append").save(target_path)

#Decode base64 function
def decode_base64(encoded_str):
    return base64.b64decode(encoded_str).decode('utf-8')

if __name__ == '__main__':
 # Setting the arguments
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
