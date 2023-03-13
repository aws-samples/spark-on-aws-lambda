FROM public.ecr.aws/lambda/python:3.8

# Setting the compatible versions of libraries

ARG HADOOP_VERSION=3.2.4
ARG AWS_SDK_VERSION=1.11.901
ARG PYSPARK_VERSION=3.3.0
ARG FRAMEWORK_VERSION=2.2.0
ARG FRAMEWORK='DELTA'


# yum updates, security updates for zlib, java installation and pyspark installation
RUN yum update -y && \
    yum -y update zlib && \
    yum -y install wget && \
    yum -y install yum-plugin-versionlock && \
    yum -y versionlock add java-1.8.0-openjdk-1.8.0.352.b08-0.amzn2.0.1.x86_64 && \
    yum -y install java-1.8.0-openjdk && \
    pip install --upgrade pip && \
    pip install pyspark==$PYSPARK_VERSION && \
    yum clean all


# setting the environment variable and Spark path
ENV SPARK_HOME="/var/lang/lib/python3.8/site-packages/pyspark"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/python:$PATH

# Setting the spark environment configuration and Copy all the jar files required and put it in jar folder

RUN mkdir $SPARK_HOME/conf && \
    echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh && \
    echo "JAVA_HOME=/usr/lib/jvm/$(ls /usr/lib/jvm |grep java)/jre" >> $SPARK_HOME/conf/spark-env.sh && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    if [ "$FRAMEWORK" = "HUDI" ]; then \
        echo "Downloading the HUDI Jar file"; \
        wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/${FRAMEWORK_VERSION}/hudi-spark3.3-bundle_2.12-${FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/; \
    elif [ "$FRAMEWORK" = "DELTA" ]; then \
        echo "Downloading the DELTA Jar file"; \
        wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${FRAMEWORK_VERSION}/delta-core_2.12-${FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/; \
        wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${FRAMEWORK_VERSION}/delta-storage-${FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/; \
    elif [ "$FRAMEWORK" = "ICEBERG" ]; then \
        echo "Downloading the ICEBERG Jar file"; \
    fi

# wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/${HUDI_VERSION}/hudi-spark3.3-bundle_2.12-${HUDI_VERSION}.jar -P ${SPARK_HOME}/jars/ && \

ENV PATH=${PATH}:${JAVA_HOME}/bin

# Setting  up the ENV vars for local code, in AWS LAmbda you have to set Input_path and Output_path
ENV input_path=""
ENV output_path=""
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_REGION=""
ENV AWS_SESSION_TOKEN=""

# spark-class file is setting the memory to 1 GB
COPY spark-class $SPARK_HOME/bin/
RUN chmod -R 755 $SPARK_HOME

# Copy the Pyspark script to container
COPY sparkLambdaHandler.py ${LAMBDA_TASK_ROOT}

# calling the Lambda handler
CMD [ "/var/task/sparkLambdaHandler.lambda_handler" ]
