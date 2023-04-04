from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark import SparkContext,SparkConf
import sys
import os

def spark_script():
    """
    Apache Iceberg Example
    """
    print("Starting the script...............")

    aws_region = os.environ['AWS_REGION']
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] 
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] 
    session_token = os.environ['AWS_SESSION_TOKEN']

    custom_sql="""MERGE INTO AwsDataCatalog.iceberg_database.my_iceberg_table t USING (SELECT customer_id,first_name,last_name,city,country, eff_start_date,eff_end_date,is_current,lastopflag  FROM source_table ) s ON t.first_name = s.first_name WHEN MATCHED THEN UPDATE SET t.first_name = s.first_name,t.last_name = s.last_name,t.city = s.city WHEN NOT MATCHED THEN INSERT * """
    
    #provide your input
    input_path="s3a://mnt-volume-jche/input_data/cust_sample_data.csv"
    target_path="s3a://mnt-volume-jche/output_iceberg/"

    '''
    # When using Glue catalog ....
    custom_sql_1 = """ CREATE TABLE AwsDataCatalog.default.my_spark_lambda_iceberg_table (customer_id string,first_name string,last_name string,city string,country string,eff_start_date string,eff_end_date string,is_current string,lastopflag string) PARTITIONED BY (customer_id) LOCATION '""" + target_path + """' TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet'); """
    custom_sql_2 = """ INSERT INTO my_spark_lambda_iceberg_table SELECT customer_id,first_name,last_name,city,country, eff_start_date,eff_end_date,is_current,lastopflag FROM  source_table ; """
    #custom_sql_3 = """ MERGE INTO AwsDataCatalog.default.my_spark_lambda_iceberg_table t USING (SELECT customer_id,first_name,last_name,city,country, eff_start_date,eff_end_date,is_current,lastopflag  FROM source_table ) s ON t.first_name = s.first_name WHEN MATCHED THEN UPDATE SET t.first_name = s.first_name,t.last_name = s.last_name,t.city = s.city WHEN NOT MATCHED THEN INSERT * ; """
    '''

    # When using Spark catalog ....
    custom_sql_1 = """ CREATE TABLE my_spark_lambda_iceberg_table (customer_id string,first_name string,last_name string,city string,country string,eff_start_date string,eff_end_date string,is_current string,lastopflag string) PARTITIONED BY (customer_id) LOCATION '""" + target_path + """' TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet'); """
    custom_sql_2 = """ INSERT INTO my_spark_lambda_iceberg_table SELECT customer_id,first_name,last_name,city,country, eff_start_date,eff_end_date,is_current,lastopflag FROM  source_table ; """
    #custom_sql_3 = """ MERGE INTO my_spark_lambda_iceberg_table t USING (SELECT customer_id,first_name,last_name,city,country, eff_start_date,eff_end_date,is_current,lastopflag  FROM source_table ) s ON t.first_name = s.first_name WHEN MATCHED THEN UPDATE SET t.first_name = s.first_name,t.last_name = s.last_name,t.city = s.city WHEN NOT MATCHED THEN INSERT * ; """
    
    print('Paramter passed ...for testing purposes')
    #print('Aws region : {}'.format(aws_region))
    #print('Aws access key : {}'.format(aws_access_key_id))
    #print('Aws secret key : {}'.format(aws_secret_access_key))
    #print('Aws token : {}'.format(session_token))
    #print('Input path : {}'.format(input_path))
    #print('S3 bucket : {}'.format(s3_bucket))
    #print('Custom sql : {}'.format(custom_sql))
    print('Source data path : {}'.format(input_path))
    print('Iceberg table location : {}'.format(target_path))

    '''
    # This configuration is used when Iceberg table is used with Glue catalog 
    spark = SparkSession.builder \
        .appName("Spark-on-AWS-Lambda") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token",session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.AwsDataCatalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.AwsDataCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.AwsDataCatalog.warehouse", target_path ) \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport().getOrCreate()
    '''

    # This configuration is used when Iceberg table is used with Spark catalog 
    spark = SparkSession.builder \
        .appName("Spark-on-AWS-Lambda") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token",session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog, org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local.type","hadoop") \
        .config("spark.sql.catalog.local.warehouse", target_path ) \
        .config("hive.exec.dynamic.partition","true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport().getOrCreate()

    ## Reading the data from S3 location
    sourceDF=spark.read.option('header','true').csv(input_path)
    sourceDF.createOrReplaceTempView("source_table")
    sourceData=spark.sql("SELECT * FROM source_table")
    sourceData.show()

    ## Write a dataFrame as a Iceberg dataset to the S3 location
    print("Started writing the data to Iceberg table ")
    spark.sql(custom_sql_1)
    spark.sql(custom_sql_2)

    ## Sample Ouput from the iceberg table
    #print("Showing the data from Iceberg Table in Glue catalog ...")
    #spark.sql("select * from AwsDataCatalog.default.my_spark_lambda_iceberg_table").show()
    print("Showing the data from Iceberg Table in Spark catalog ...")
    spark.sql("select * from my_spark_lambda_iceberg_table").show()

    print("Completed Loading the data in Iceberg table ")


if __name__ == '__main__':
    spark_script()
    
# Run as :
# docker build --build-arg FRAMEWORK=ICEBERG -t sparkonlambda .
# docker run -e SCRIPT_BUCKET=mnt-volume-jche -e SPARK_SCRIPT=input_data/sample-spark-script-csv-to-iceberg-table.py -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id) -e AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key) -e AWS_SESSION_TOKEN=$(aws configure get default.aws_session_token) -p 9000:8080 sparkonlambda
#
# curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'