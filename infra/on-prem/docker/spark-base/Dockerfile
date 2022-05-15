FROM spark-standalone-cluster-base
LABEL maintainer="Daniel Faioli <dthereza.min@gmail.com>"
# -- Layer: Image metadata

LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Spark Base Image"
LABEL org.label-schema.description="Spark base image shipped with spark"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: Apache Spark

ARG spark_version=3.2.1
ARG hadoop_version=3.2

RUN curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    echo "alias pyspark=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/bin/pyspark" >> ~/.bashrc && \
    echo "alias spark-shell=/usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/bin/spark-shell" >> ~/.bashrc && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Add Hadoop Azure Jars

RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.2.1/hadoop-azure-2.7.4.jar \
    -o $SPARK_HOME/jars/hadoop-azure-3.2.1.jar && \
    curl -s https://repo1.maven.org/maven2/com/microsoft/azure/azure-data-lake-store-sdk/2.3.10/azure-data-lake-store-sdk-2.3.10.jar \
    -o $SPARK_HOME/jars/azure-data-lake-store-sdk-2.3.10.jar

RUN mv $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf

RUN echo "spark.driver.extraClassPath=$SPARK_HOME/jars/azure-data-lake-store-sdk-2.3.10.jar:$SPARK_HOME/jars/hadoop-azure-3.2.1.jar" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.driver.extraClassPath=$SPARK_HOME/jars/azure-data-lake-store-sdk-2.3.10.jar:$SPARK_HOME/jars/hadoop-azure-3.2.1.jar" >> $SPARK_HOME/conf/spark-defaults.conf

# -- Runtime

WORKDIR ${SPARK_HOME}