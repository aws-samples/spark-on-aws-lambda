#!/usr/bin/env bash
FRAMEWORK=$1
SPARK_HOME=$2
HADOOP_VERSION=$3
AWS_SDK_VERSION=$4
DELTA_FRAMEWORK_VERSION=$5
HUDI_FRAMEWORK_VERSION=$6
ICEBERG_FRAMEWORK_VERSION=$7
ICEBERG_FRAMEWORK_SUB_VERSION=$8

mkdir $SPARK_HOME/conf
echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh
echo "JAVA_HOME=/usr/lib/jvm/$(ls /usr/lib/jvm |grep java)/jre" >> $SPARK_HOME/conf/spark-env.sh




#wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
#wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars/

case "$FRAMEWORK" in
    HUDI)
        wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/${HUDI_FRAMEWORK_VERSION}/hudi-spark3.3-bundle_2.12-${HUDI_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
        ;;
    DELTA)
        wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_FRAMEWORK_VERSION}/delta-core_2.12-${DELTA_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
        wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_FRAMEWORK_VERSION}/delta-storage-${DELTA_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
        ;;
    ICEBERG)
        wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_FRAMEWORK_VERSION}/${ICEBERG_FRAMEWORK_SUB_VERSION}/iceberg-spark-runtime-${ICEBERG_FRAMEWORK_VERSION}-${ICEBERG_FRAMEWORK_SUB_VERSION}.jar -P ${SPARK_HOME}/jars/
        wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.23/bundle-2.20.23.jar -P ${SPARK_HOME}/jars/
        wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.23/url-connection-client-2.20.23.jar -P ${SPARK_HOME}/jars/
        ;;
esac