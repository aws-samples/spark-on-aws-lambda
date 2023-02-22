import boto3
import sys
import os
import subprocess

def lambda_handler(event, context):
 
 """
 Lambda_handler is called when the AWS Lambda
 is triggered. The function is downloading file 
 from Amazon S3 location and spark submitting 
 the script in AWS Lambda
 """
 
 print("start AWS Lambda Handler ......")
 s3_bucket_script = os.environ['SCRIPT_BUCKET']
 input_script = os.environ['SPARK_SCRIPT']
 
 # Creating an S3 boto3 client
 s3_client = boto3.client("s3")
 s3_client.download_file(s3_bucket_script, input_script, "/tmp/spark_script.py")
 
 # Set the environment variables for the Spark application
 os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local pyspark-shell"

 
 # Run the spark-submit command on the local copy of teh script
 subprocess.run(["spark-submit", "/tmp/spark_script.py"])
