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

  ICEBERG CREATES THE TABLE DIRECTLY IN GLUE DATA CATALOG

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
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token",session_token) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.jars.packages", "org.apache.iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.AwsDataCatalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.AwsDataCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.AwsDataCatalog.warehouse", output_path ) \
        .config("spark.sql.defaultCatalog", "AwsDataCatalog" ) \
        .enableHiveSupport().getOrCreate()

    print("Started Reading the CSV file from S3 location ",input_path)

    #Reading the csv file form input_path
    df=spark.read.option('header','true').option("delimiter", ";").csv(input_path)
    df.printSchema()

    print("Started Writing the dataframe file to Target iceberg table ", output_path)
    df.createOrReplaceTempView("source_table")
    sourceData=spark.sql("SELECT * FROM source_table")
    custom_sql_1 = """
        CREATE TABLE IF NOT EXISTS AwsDataCatalog.spark_on_lambda.accommodations_iceberg (
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
        LOCATION '""" + output_path + """' 
        TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet');
    """

    custom_sql_2 = """
        INSERT INTO AwsDataCatalog.spark_on_lambda.accommodations_iceberg
             SELECT * FROM  source_table ;
    """

    spark.sql(custom_sql_1)
    spark.sql(custom_sql_2)


if __name__ == '__main__':
    #Calling the Spark script method
    spark_script()
