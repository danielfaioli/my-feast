FROM spark-standalone-cluster-base
LABEL maintainer="Daniel Faioli <dthereza.min@gmail.com>"
# -- Layer: Image metadata

LABEL org.label-schema.name="Apache Spark Standalone Cluster on Docker - Spark Base Image"
LABEL org.label-schema.description="Spark worker image"
LABEL org.label-schema.schema-version="1.0"

# -- Layer: Notebooks and Data

ADD workspace/ ${SHARED_WORKSPACE}/
ADD conf/jupyterlab/requirements.txt requirements.txt

# -- Layer: JupyterLab + Python kernel for PySpark

ARG spark_version=3.2.1
ARG jupyterlab_version=3.0.0
ARG scala_version=2.12.10
ARG scala_kernel_version=0.10.9

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev python3-venv && \
    pip3 install --upgrade pip && \
    pip3 install wget==3.2 pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# -- Layer: Scala kernel for Spark

RUN apt-get install -y ca-certificates-java --no-install-recommends && \
    curl -Lo coursier https://git.io/coursier-cli && \
    chmod +x coursier && \
    ./coursier launch --fork almond:${scala_kernel_version} --scala ${scala_version} -- --display-name "scala-kernel-${scala_version}" --install && \
    rm -f ./coursier

# -- Layer: R kernel for SparkR

RUN apt-get install -y r-base-dev && \
    R -e "install.packages('IRkernel')" && \
    R -e "IRkernel::installspec(displayname = 'R 3.5', user = FALSE)" && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/SparkR_${spark_version}.tar.gz -k -o sparkr.tar.gz && \
    R CMD INSTALL sparkr.tar.gz && \
    rm -f sparkr.tar.gz

# -- Runtime

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}

RUN apt-get install -y unixodbc-dev git 

RUN python3 -m venv venv && \
    . venv/bin/activate

RUN pip install git+https://github.com/danielfaioli/feast-azure.git#subdirectory=provider/sdk/ && \
    pip install sodapy tqdm

RUN python3 -m ipykernel install --user --name myfeast

RUN apt-get install -y zsh iputils-ping telnet && \
    wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true

CMD export SHELL=/bin/zsh ; jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
