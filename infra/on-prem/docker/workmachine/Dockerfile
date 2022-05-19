FROM spark-standalone-cluster-base
LABEL maintainer="Daniel Faioli <dthereza.min@gmail.com>"
# -- Layer: Image metadata

LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Ingestion Work Machine Image"
LABEL org.label-schema.description="Ingestion Work Machine Image"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: Notebooks and Data

ADD ingest_scripts/ ${SHARED_WORKSPACE}/

# -- Layer: JupyterLab + Python kernel for PySpark

ARG spark_version=3.2.1
ARG scala_version=2.12.10

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev python3-venv && \
    pip3 install --upgrade pip && \
    pip3 install wget==3.2 pyspark==${spark_version}

# -- Runtime

WORKDIR ${SHARED_WORKSPACE}/

RUN apt-get install -y unixodbc-dev git 

RUN python3 -m venv venv && \
    . venv/bin/activate

RUN pip install sodapy tqdm

RUN apt-get install -y zsh iputils-ping telnet && \
    wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true

RUN pip install python-dateutil

CMD export SHELL=/bin/zsh ; spark-submit --master spark-master --deploy-mode client ${INGEST_SCRIPT} ${PARAMS}
