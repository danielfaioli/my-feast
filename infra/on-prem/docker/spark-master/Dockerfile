
FROM spark-standalone-spark-base
LABEL maintainer="Daniel Faioli <dthereza.min@gmail.com>"
# -- Layer: Image metadata

LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Spark Base Image"
LABEL org.label-schema.description="Spark master image"
LABEL org.label-schema.schema-version="1.0"

# -- Runtime

EXPOSE 8080 7077

CMD bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out