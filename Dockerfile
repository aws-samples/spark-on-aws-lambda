# Use AWS Lambda Python 3.8 image as base
FROM public.ecr.aws/lambda/python:3.8

# Setting the compatible versions of libraries
ARG HADOOP_VERSION=3.2.4
ARG AWS_SDK_VERSION=1.11.901
ARG PYSPARK_VERSION=3.3.0

#FRAMEWORK will passed during the Docker build 
ARG FRAMEWORK
ARG DELTA_FRAMEWORK_VERSION=2.2.0
ARG HUDI_FRAMEWORK_VERSION=0.12.2
ARG ICEBERG_FRAMEWORK_VERSION=2.2.0
ARG ICEBERG_FRAMEWORK_VERSION=3.2_2.12
ARG ICEBERG_FRAMEWORK_SUB_VERSION=1.1.0


# Perform system updates and install dependencies
RUN yum update -y && \
    yum -y update zlib && \
    yum -y install wget unzip tar && \
    yum -y install yum-plugin-versionlock && \
    yum -y versionlock add java-1.8.0-openjdk-1.8.0.362.b08-0.amzn2.0.1.x86_64 && \
    yum -y install java-1.8.0-openjdk && \
    pip install --upgrade pip && \
    #pip install pyspark==$PYSPARK_VERSION && \
    yum clean all

ENV BASE_DIR="/var/lang"

# Run below command to download and extract spark 
RUN mkdir ${BASE_DIR}/external && \
    wget -q https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-4.0/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0.tgz -P ${BASE_DIR}/external/ && \
    tar -xvf ${BASE_DIR}/external/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0.tgz -C ${BASE_DIR}/external/
#Run below command to download and extract glue lib
RUN wget -q https://github.com/awslabs/aws-glue-libs/archive/refs/heads/master.zip -P ${BASE_DIR}/external/ && \
    unzip ${BASE_DIR}/external/master.zip -d ${BASE_DIR}/external/    

# Set environment variables for PySpark
ENV SPARK_HOME="${BASE_DIR}/external/spark"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
#ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/python:$PATH


COPY download_jars.sh /tmp
RUN chmod +x /tmp/download_jars.sh && \
    /tmp/download_jars.sh $FRAMEWORK $SPARK_HOME $HADOOP_VERSION $AWS_SDK_VERSION $DELTA_FRAMEWORK_VERSION $HUDI_FRAMEWORK_VERSION $ICEBERG_FRAMEWORK_VERSION $ICEBERG_FRAMEWORK_SUB_VERSION


ENV PATH=${PATH}:${JAVA_HOME}/bin

# Setting  up the ENV vars for local code, in AWS LAmbda you have to set Input_path and Output_path
ENV INPUT_PATH=""
ENV OUTPUT_PATH=""
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_REGION=""
ENV AWS_SESSION_TOKEN=""
ENV CUSTOM_SQL=""

# spark-class file is setting the memory to 1 GB
COPY glue-setup.sh ${BASE_DIR}/external/aws-glue-libs-master/bin/
COPY libs/spark-jars/*.jar $SPARK_HOME/jars/

RUN chmod -R 755 $SPARK_HOME
RUN chmod -R 755 ${BASE_DIR}/external/aws-glue-libs-master

RUN mkdir ${BASE_DIR}/external/aws-glue-libs-master/jarsv1
COPY libs/glue-jars/*.jar  ${BASE_DIR}/external/aws-glue-libs-master/jarsv1/
# Copy the Pyspark script to container

# Copy the Pyspark script to container
COPY sparkLambdaHandler.py ${LAMBDA_TASK_ROOT}

# calling the Lambda handler
CMD [ "/var/task/sparkLambdaHandler.lambda_handler" ]
