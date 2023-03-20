# Import session

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
import sys
import os

# Method to create Spark Session and print Spark configuration

def spark_session():
 """
 Creating a spark session
 """
 print("************ Definining Spark Session ************")

 aws_region = os.environ['AWS_REGION'] 
 aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] 
 aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] 
 session_token = os.environ['AWS_SESSION_TOKEN']

 spark_session = SparkSession.builder \
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

 configurations = spark_session.sparkContext.getConf().getAll()
 for item in configurations: print(item)

 return spark_session

# Method to start Spark workload execution and load a Delta Lake on Amazon S3
 
def spark_execution(spark_session):
 """
 Spark execution script. It is using environemnt variable for
 input and output location
 """
 print("************ Executing Workload ************")
 input_path = os.environ['input_path'] 
 target_path = os.environ['output_path']
 s3_bucket  = os.environ['s3_bucket']
 input_path = "s3a://"+s3_bucket+"/"+input_path
 target_path ="s3a://"+s3_bucket+"/"+target_path
 
 print("Input path: ",input_path)
 print("Target path: ",target_path)
 print("Started Reading the CSV file from S3 location: ",input_path)
 
 df=spark_session.read.option('header','true').csv(input_path)
 df = df.withColumn("last_upd_timestamp", current_timestamp())
 print(df.printSchema())

 df.write.format("delta") \
    .mode("overwrite") \
    .save(target_path)

 print(f"Total of rows loaded: {str(df.count())}")

 return df

#MAIN function
 
if __name__ == '__main__':
 spark_session = spark_session()
 result = spark_execution(spark_session)
