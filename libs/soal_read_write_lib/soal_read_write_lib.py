def readLargeFiledf(spark, file_path):
    """
    Reads a large csv file using PySpark and returns a DataFrame
    partitioning by the spark.sql.files.maxPartitionBytes .
    deafult set to 400MB

    Parameters:
    - spark: An active SparkSession instance.
    - file_path: Path to the file.

    Returns:
    - DataFrame
    """
   
    # Increase the default number of partitions used during file reading.
    spark.conf.set("spark.sql.files.maxPartitionBytes", "400000000")  # 400 MB per partition

    # Read the file into a DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    return df