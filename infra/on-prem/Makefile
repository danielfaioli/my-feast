.PHONY: build
.PHONY: cluster
.PHONY: interactive
.PHONY: ingestion

repository := danielfaioli
version := 0.1.0

build: 
	@docker build . -t=spark-standalone-cluster-base:latest -t=spark-standalone-cluster-base:${version}  --file=./docker/cluster-base/Dockerfile
	@docker build . -t=spark-standalone-spark-base:latest -t=spark-standalone-spark-base:${version} --file=./docker/spark-base/Dockerfile
	@docker build . -t=spark-standalone-spark-master:latest -t=spark-standalone-spark-master${version} --file=./docker/spark-master/Dockerfile
	@docker build . -t=spark-standalone-spark-worker:latest -t=spark-standalone-spark-worker:${version} --file=./docker/spark-worker/Dockerfile
	@docker build . -t=spark-standalone-jupyterlab:latest -t=spark-standalone-jupyterlab:${version} --file=./docker/jupyterlab/Dockerfile
	@docker build . -t=spark-standalone-workmachine:latest -t=spark-standalone-workmachine:${version} --file=./docker/workmachine/Dockerfile
# push:
# 	TODO: Add automation to push images to docker hub.

# starts the cluster only
make cluster:
	@docker compose up -d namenode datanode spark-master spark-worker-1  spark-worker-2

# starts cluster and interactive interface (Jupyter Hub)
make interactive:
	@docker compose up -d namenode datanode spark-master spark-worker-1  spark-worker-2 jupyterlab

# starts the cluster and the ingestion process 
# make cluster-ingestion:
	# @docker compose up -d namenode datanode spark-master spark-worker-1  spark-worker-2 workmachine

# starts only the jupyter hub service - should be used if the cluster is already running
# make jupyter:
	# @docker compose up -d jupyterlab

# runs the ingestion process
make ingestion:
	@docker compose up workmachine