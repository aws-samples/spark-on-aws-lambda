from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
import sys
import os

def spark_script():
 """
 Function that gets triggered when AWS Lambda
 is running.
 """
 
 print("start...................")
 
 # Please use script to update it, try to avoid env variable through docker
 input_path = os.environ['input_path'] 
 target_path = os.environ['output_path']
 s3_bucket  = os.environ['s3_bucket']

 aws_region = os.environ['AWS_REGION'] 
 aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] 
 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] 
 session_token = os.environ['AWS_SESSION_TOKEN']
 
 
 input_path = "s3a://"+s3_bucket+"/"+input_path
 target_path ="s3a://"+s3_bucket+"/"+target_path
 
 print(" ******* Input path ",input_path)
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
 
 
 print("Started Reading the CSV file from S3 location ",input_path)
 
 #Reading the csv file form input_path
 df=spark.read.option('header','true').csv(input_path)
 df = df.withColumn("last_upd_timestamp", current_timestamp())
 df.show()
 
 # HUDI configuration for the table write
 hudi_options = {
  'hoodie.table.name': 'customer_table',
    'hoodie.datasource.write.recordkey.field': 'Customer_ID',
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

 print("Started Writing the dataframe file to  Target hudi table ", target_path)
 df.write.format("hudi").options(**hudi_options).mode("overwrite").save(target_path)
 # df.write.format("csv").save(target_path)
 
if __name__ == '__main__':
 #Calling the Spark script method
 spark_script()
