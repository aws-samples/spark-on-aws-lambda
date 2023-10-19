import boto3
import sys
import os
import subprocess
import logging
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def s3_script_download(s3_bucket_script: str,input_script: str)-> None:
    """
    """
    s3_client = boto3.resource("s3")

    try:
        logger.info(f'Now downloading script {input_script} in {s3_bucket_script} to /tmp')
        s3_client.Bucket(s3_bucket_script).download_file(input_script, "/tmp/spark_script.py")
      
    except Exception as e :
        logger.error(f'Error downloading the script {input_script} in {s3_bucket_script}: {e}')
    else:
        logger.info(f'Script {input_script} successfully downloaded to /tmp')



def spark_submit(s3_bucket_script: str,input_script: str, event: dict)-> None:
    """
    Submits a local Spark script using spark-submit.
    """
     # Set the environment variables for the Spark application
    # pyspark_submit_args = event.get('PYSPARK_SUBMIT_ARGS', '')
    # # Source input and output if available in event
    # input_path = event.get('INPUT_PATH','')
    # output_path = event.get('OUTPUT_PATH', '')

    for key,value in event.items():
        os.environ[key] = value
    # Run the spark-submit command on the local copy of teh script
    try:
        logger.info(f'Spark-Submitting the Spark script {input_script} from {s3_bucket_script}')
        subprocess.run(["spark-submit", "/tmp/spark_script.py", "--event", json.dumps(event)], check=True, env=os.environ)
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        raise e
    else:
        logger.info(f'Script {input_script} successfully submitted')

def lambda_handler(event, context):

    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """

    logger.info("******************Start AWS Lambda Handler************")
    s3_bucket_script = os.environ['SCRIPT_BUCKET']
    input_script = os.environ['SPARK_SCRIPT']
    os.environ['INPUT_PATH'] = event.get('INPUT_PATH','')
    os.environ['OUTPUT_PATH'] = event.get('OUTPUT_PATH', '')

    s3_script_download(s3_bucket_script,input_script)
    
    # Set the environment variables for the Spark application
    spark_submit(s3_bucket_script,input_script, event)
   
