#!/usr/bin/env bash
FRAMEWORK=$1
SPARK_HOME=$2
HADOOP_VERSION=$3
AWS_SDK_VERSION=$4
DELTA_FRAMEWORK_VERSION=$5
HUDI_FRAMEWORK_VERSION=$6
ICEBERG_FRAMEWORK_VERSION=$7
ICEBERG_FRAMEWORK_SUB_VERSION=$8
DEEQU_FRAMEWORK_VERSION=$9

mkdir $SPARK_HOME/conf
echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh
echo "JAVA_HOME=/usr/lib/jvm/$(ls /usr/lib/jvm |grep java)/jre" >> $SPARK_HOME/conf/spark-env.sh




wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars/
# jar files needed to conncet to Snowflake
#wget -q https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar -P ${SPARK_HOME}/jars/
#wget -q https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar -P ${SPARK_HOME}/jars/

echo 'Framework is:'
echo $FRAMEWORK

IFS=',' read -ra FRAMEWORKS <<< "$FRAMEWORK"

for fw in "${FRAMEWORKS[@]}"; do
echo $fw
    case "$fw" in
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
        SNOWFLAKE)
            wget -q https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar -P ${SPARK_HOME}/jars/
            ;;
        REDSHIFT)
            wget -q https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/4.1.1/spark-redshift_2.12-4.1.1.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.3.0/spark-avro_2.13-3.3.0.jar  -P ${SPARK_HOME}/jars/
            wget -q https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.18/redshift-jdbc42-2.1.0.18.zip -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.1/minimal-json-0.9.1.jar -P ${SPARK_HOME}/jars/
            # Unzip the Redshift JDBC driver
            unzip -o ${SPARK_HOME}/jars/redshift-jdbc42-2.1.0.18.zip -d ${SPARK_HOME}/jars/
            ;;
        DEEQU)
            wget -q https://repo1.maven.org/maven2/com/amazon/deequ/deequ/${DEEQU_FRAMEWORK_VERSION}/deequ-${DEEQU_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
            ;;
        *)
            echo "Unknown framework: $fw"
            ;;
    esac
done
