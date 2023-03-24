from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import os


print("start...................")
# arguments=os.environ.get('SPARK_ARGUMENTS', '')

input_path = os.environ['INPUT_PATH']
target_path = os.environ['OUTPUT_PATH']
# s3_bucket  = os.environ['s3_bucket']

aws_region = os.environ['AWS_REGION']
aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
session_token = os.environ['AWS_SESSION_TOKEN']


# Change the input and target location for usage
# input_path = "s3a://"+s3_bucket+"/"+input_path
# target_path ="s3a://"+s3_bucket+"/"+target_path

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
#df.write.format("csv").save(target_path)
df.write.format("hudi").save(target_path)



