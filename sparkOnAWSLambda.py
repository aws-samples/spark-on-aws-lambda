from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import os

def lambda_handler(event, context):
 print("start...................")

 input_path = os.environ['input_path'] 
 target_path = os.environ['output_path']

 aws_region = os.environ['AWS_REGION'] 
 aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] 
 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] 
 session_token = os.environ['AWS_SESSION_TOKEN']
 
 
 input_path = f's3://'
 output_path = f's3a://'
 
 print(" ******* Input path",input_path)
 print(" ******* Out path",output_path)
 
 # Change the input and target location for usage
 input_path ="s3a://sparkonlambda-1/inp/test.csv"
 target_path ="s3a://sparkonlambda-1/out/csv_table"
 
 print(" ******* Input path ",input_path)
 print(" ******* Target path ",target_path)

 spark = SparkSession.builder \
 .appName("Spark-on-AWS-Lambda") \
 .master("local[*]") \
 .config("spark.driver.bindAddress", "127.0.0.1") \
 .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
 .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
 .config("spark.hadoop.fs.s3a.session.token",session_token) \
 .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
 .getOrCreate()
 

 
 print("Started Reading the CSV file from S3 location ",input_path)
 
 df=spark.read.option('header','true').csv(input_path)
 df.show()
 

 print("Started Writing the CSV file to  Target S3 location ", target_path)
 df.write.format("csv").save(target_path)
